//! Durable audit appender — HMAC-chained tab-separated log + fjall checkpoint.
//!
//! Spec IMPL §3.3 NORMATIVE — two storage layers:
//!
//! 1. **Fixed-format log** (`data_dir/audit.log`) — one line per [`AuditEntry`],
//!    fields TAB-separated in fixed order; byte fields as base64url. Append-only.
//! 2. **Checkpoint fjall** (`data_dir/audit_checkpoint`) — single-row
//!    `last_audit_hmac` keyspace with `(next_seq: u64, prev_hmac: [u8; 32])`.
//!    Always committed with `db.persist(PersistMode::SyncAll)` (fsync).
//!
//! ## fsync semantics
//!
//! - **Strict mode** ([`FjallAuditAppender::open_strict`]): every
//!   [`AuditAppender::append_entry`] writes + `sync_all`s the log file
//!   synchronously.
//! - **Batched mode** ([`FjallAuditAppender::open_batched`]): entries are
//!   buffered in memory and flushed by a background tokio task at the
//!   configured interval (≤5s per spec).
//!
//! Checkpoint writes are always durable regardless of mode.

use fjall::{Database, Keyspace, KeyspaceCreateOptions};
use parking_lot::Mutex;
use shamir_connect::server::audit_chain::{
    AuditAppender, AuditEntry, AuditError, MAX_RECORD_BYTES,
};
use std::ffi::OsStr;
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::{sync::Notify, task::JoinHandle};

mod checkpoint;
mod close_fence;
mod format;
mod recovery;
mod rotation;
mod shutdown;

use format::entry_to_line;
use rotation::RotationState;
pub use shutdown::{AuditFinalFlush, AuditShutdownOutcome, FlusherJoinFailure};

#[cfg(test)]
#[path = "tests/audit_appender_tests_manifest.rs"]
mod tests;

/// Filename of the fixed-format audit log inside `data_dir`.
const AUDIT_LOG_FILENAME: &str = "audit.log";
/// Directory name of the fjall checkpoint database inside `data_dir`.
const CHECKPOINT_DB_DIRNAME: &str = "audit_checkpoint";
/// fjall keyspace holding the single `(next_seq, prev_hmac)` row.
const CHECKPOINT_KEYSPACE: &str = "last_audit_hmac";
/// Single key used inside the checkpoint keyspace.
const CHECKPOINT_KEY: &[u8] = &[0u8];

/// Whether `name` lies in the reserved audit-log namespace inside `data_dir`:
/// the active `AUDIT_LOG_FILENAME` itself or an `audit.log.<digits>` rotation
/// archive (recovery's `ROTATED_PREFIX` names the archives with 20 digits; any
/// non-empty digit run is reserved here).
///
/// Matching is ASCII-case-insensitive on EVERY platform: over-refusal is
/// harmless and under-refusal is not. On a case-insensitive mount (Windows,
/// default macOS APFS, a case-insensitive Linux mount) `AUDIT.LOG` IS the
/// audit log, so refusing it is required; on a case-sensitive filesystem it
/// names a distinct entry, and refusing it only blocks a name nobody should
/// use. This helper is the appender's own namespace definition: its only
/// caller is the sink guard, `logging::sink_guard::validate_sink_path`, which
/// must therefore refuse a sink named `AUDIT.LOG` in `data_dir` everywhere.
pub fn is_reserved_audit_name(name: &OsStr) -> bool {
    let folded = name.as_encoded_bytes().to_ascii_lowercase();
    let Some(suffix) = folded.strip_prefix(AUDIT_LOG_FILENAME.as_bytes()) else {
        return false;
    };
    suffix.is_empty()
        || suffix
            .strip_prefix(b".")
            .is_some_and(|digits| !digits.is_empty() && digits.iter().all(u8::is_ascii_digit))
}

// Allow a full post-auth protocol frame of details (plus base64 expansion),
// maximum escaped text lengths from canonical_bytes, and the fixed columns
// (two decimal u64s, three base64 fields, ten tabs and LF/optional CR).
// Exact record-size validation is shared with recovery through
// `AuditEntry::validate_log_record_size()`; the ceiling itself is owned by
// `shamir-connect` (re-exported here for `recovery.rs`).
type Position = (u64, [u8; 32]);
/// Message for a rejected append at the end of the u64 sequence space.
const AUDIT_SEQ_EXHAUSTED: &str = "audit sequence space exhausted";
/// Optional server-metadata hook run under the persistence gate at every
/// rotation boundary and the final batched shutdown, AFTER the appender's
/// own anchor is persisted and BEFORE retention prunes.
type ServerMetaHook = Arc<dyn Fn(u64, [u8; 32]) -> io::Result<()> + Send + Sync>;
/// Private test seam around the actual parent-directory sync at rotation
/// (replaces the real [`rotation::sync_parent`] call when installed).
#[cfg(test)]
type DirSyncHook = Arc<dyn Fn(PathBuf) -> io::Result<()> + Send + Sync>;

/// Fail-closed errors from opening and verifying a durable audit history.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum AuditRecoveryError {
    /// Filesystem operation failed.
    #[error("audit I/O at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    /// Appender checkpoint database operation failed.
    #[error("audit checkpoint store at {path}: {source}")]
    CheckpointStore {
        path: PathBuf,
        #[source]
        source: AppenderError,
    },
    /// The Fjall checkpoint row has the wrong size.
    #[error("malformed audit checkpoint at {path}: expected 40 bytes, got {length}")]
    MalformedCheckpoint { path: PathBuf, length: usize },
    /// A checkpoint is not a valid chain position (zero or invalid genesis).
    #[error("invalid audit checkpoint at next sequence {next_seq}")]
    InvalidAnchor { next_seq: u64 },
    /// The two trusted stores disagree at the same chain position.
    #[error("audit checkpoints conflict at next sequence {next_seq}")]
    ConflictingAnchors { next_seq: u64 },
    /// A checkpoint disagrees with the authenticated history at its position.
    #[error("audit checkpoint digest mismatch at next sequence {next_seq}")]
    AnchorMismatch { next_seq: u64 },
    /// A trusted checkpoint lies beyond the verified tail.
    #[error("audit log truncation: checkpoint {next_seq} is past verified next sequence {verified_next_seq}")]
    AnchorPastTail {
        next_seq: u64,
        verified_next_seq: u64,
    },
    /// Retention removed genesis and neither anchor authenticates the suffix.
    #[error("retained audit history starts at {first_seq} without a usable checkpoint")]
    UnanchoredSuffix { first_seq: u64 },
    /// An audit log path is not a regular file (symlinks are not accepted).
    #[error("audit log is not a regular file: {path}")]
    NotRegularFile { path: PathBuf },
    /// A name in the reserved audit.log.* namespace is not a rotation name.
    #[error("invalid rotated audit log filename: {path}")]
    InvalidRotationName { path: PathBuf },
    /// A record exceeds the startup memory budget.
    #[error("audit record at {path}:{offset} exceeds {} bytes", MAX_RECORD_BYTES)]
    RecordTooLarge { path: PathBuf, offset: u64 },
    /// A rotated file has an unterminated final record.
    #[error("unterminated rotated audit record at {path}:{offset}")]
    ArchivedFragment { path: PathBuf, offset: u64 },
    /// An active record parses but lacks its final LF; recovery does not discard it.
    #[error("parseable active audit record lacks LF at {path}:{offset}")]
    UnterminatedActiveRecord { path: PathBuf, offset: u64 },
    /// A newline-terminated record cannot be decoded.
    #[error("malformed audit record at {path}:{offset}: {source}")]
    MalformedRecord {
        path: PathBuf,
        offset: u64,
        #[source]
        source: AppenderError,
    },
    /// A record's canonical representation is invalid.
    #[error("invalid canonical audit record at {path}:{offset}: {source}")]
    InvalidCanonicalRecord {
        path: PathBuf,
        offset: u64,
        #[source]
        source: AuditError,
    },
    /// A record is missing, duplicated or out of order.
    #[error("audit sequence gap at {path}:{offset}: expected {expected}, found {found}")]
    SequenceGap {
        path: PathBuf,
        offset: u64,
        expected: u64,
        found: u64,
    },
    /// A record does not point to its verified predecessor.
    #[error("audit chain link mismatch at {path}:{offset}")]
    LinkMismatch { path: PathBuf, offset: u64 },
    /// A record's HMAC does not verify.
    #[error("audit HMAC mismatch at {path}:{offset}")]
    HmacMismatch { path: PathBuf, offset: u64 },
    /// The recovered position leaves no sequence available for the writer.
    #[error("audit sequence exhausted at {path}:{offset}")]
    SequenceExhausted { path: PathBuf, offset: u64 },
}

/// Errors raised by [`FjallAuditAppender`] open + flush operations.
#[derive(Debug, thiserror::Error)]
pub enum AppenderError {
    /// Underlying I/O failure (file open, write, fsync).
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    /// fjall error while opening / committing the checkpoint keyspace.
    #[error("fjall: {0}")]
    Fjall(#[from] fjall::Error),
    /// A checkpoint row has the wrong byte length.
    #[error("malformed audit checkpoint row: expected 40 bytes, got {length}")]
    MalformedCheckpoint {
        /// Actual stored row length in bytes.
        length: usize,
    },
    /// A checkpoint tuple is not a valid chain position (zero next sequence
    /// or a genesis position carrying a non-genesis digest).
    #[error("invalid audit checkpoint at next sequence {next_seq}")]
    InvalidAnchor {
        /// Attempted next sequence of the refused tuple.
        next_seq: u64,
    },
    /// A checkpoint at an older chain position than the persisted anchor
    /// row was attempted (fail-closed anchor regression refusal).
    #[error(
        "audit anchor regression: persisted next sequence {persisted} is past attempted {attempted}"
    )]
    AnchorRegression { persisted: u64, attempted: u64 },
    /// A checkpoint at the same persisted position carries a
    /// different digest (fail-closed same-position conflict refusal).
    #[error("audit anchor conflict at next sequence {next_seq}")]
    AnchorConflict { next_seq: u64 },
}

/// Durability mode of the appender.
enum Durab {
    Strict,
    Batched {
        buffer: Mutex<Vec<AuditEntry>>,
        notify: Arc<Notify>,
        shutdown: Arc<std::sync::atomic::AtomicBool>,
        task: Mutex<Option<JoinHandle<Result<(), String>>>>,
    },
}

/// Optional log-rotation policy for the fixed-format audit file.
#[derive(Debug)]
pub struct RotationPolicy {
    pub max_size_bytes: u64,
    pub current_size: std::sync::atomic::AtomicU64,
    /// Delete rotated files older than this many days. `0` disables the
    /// retention sweep (operator manages retention out-of-band). The
    /// sweep runs piggyback on each rotation — see the free private
    /// `rotation::sweep_retention` helper above (in `audit_appender/rotation.rs`).
    pub retention_days: u32,
}

/// Durable [`AuditAppender`] backed by fixed-format tab-separated log + fjall checkpoint.
pub struct FjallAuditAppender {
    log_file: Mutex<File>,
    log_path: PathBuf,
    /// Open fjall database holding the checkpoint keyspace.
    checkpoint_db: Arc<Database>,
    /// Checkpoint keyspace (single-row table).
    checkpoint_ks: Keyspace,
    mode: Durab,
    /// Synchronous persistence gate — design (b): sync-only, never awaited
    /// by async tasks, no scoped-thread bridge, no join on a lock waiter.
    /// Lock order: persistence_gate -> buffer -> log_file -> rotation.
    /// Checkpoint flush + metadata run under the same gate.
    persistence_gate: Mutex<()>,
    failed: Mutex<Option<String>>,
    /// Latest fsynced chain position `(next_seq, prev_hmac)`: `Some` after
    /// a durable log fsync or the recovery baseline, `None` for raw legacy
    /// openers until their own first fsync. Checkpoint writes compare
    /// against the checked on-disk row instead; an empty raw close writes
    /// no invented genesis.
    latest_fsynced: Mutex<Option<Position>>,
    /// Optional server-metadata hook: run under the persistence gate at
    /// every actual rotation and the final batched shutdown, after the
    /// appender anchor is persisted and before retention prunes.
    meta_hook: Option<ServerMetaHook>,
    /// Internal rotation state; carries the policy and the precomputed
    /// legacy archive suffix order the retention sweep emits before
    /// suffix-ordered post-boot archives.
    rotation: Option<RotationState>,
    #[cfg(test)]
    after_extract_hook: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    #[cfg(any(test, feature = "test-hooks"))]
    fail_next_flush: std::sync::atomic::AtomicBool,
    #[cfg(test)]
    gate_contention_hook: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    #[cfg(any(test, feature = "test-hooks"))]
    gate_acquired_hook: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    #[cfg(any(test, feature = "test-hooks"))]
    final_close_attempts: std::sync::atomic::AtomicUsize,
    /// Public `checkpoint` calls the post-close fence refused (see `close_fence`).
    refused_checkpoint_attempts: std::sync::atomic::AtomicUsize,
    /// One-shot hook run at the END of a successful batched `append_entry`.
    #[cfg(test)]
    after_append_hook: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    /// One-shot hook run in `checkpoint` after the cheap pre-gate fence.
    #[cfg(test)]
    before_gate_checkpoint_hook: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    /// Test seam replacing [`rotation::sync_parent`] at dir sync.
    #[cfg(test)]
    dir_sync_hook: Mutex<Option<DirSyncHook>>,
}

impl core::fmt::Debug for FjallAuditAppender {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("FjallAuditAppender")
            .field("log_path", &self.log_path)
            .field(
                "mode",
                match &self.mode {
                    Durab::Strict => &"strict",
                    Durab::Batched { .. } => &"batched",
                },
            )
            .finish()
    }
}

impl FjallAuditAppender {
    fn paths(data_dir: &Path) -> (PathBuf, PathBuf) {
        (
            data_dir.join(AUDIT_LOG_FILENAME),
            data_dir.join(CHECKPOINT_DB_DIRNAME),
        )
    }

    fn open_log(path: &Path) -> std::io::Result<File> {
        OpenOptions::new().create(true).append(true).open(path)
    }

    fn open_db(path: &Path) -> Result<(Arc<Database>, Keyspace), AppenderError> {
        let db = Database::builder(path).open()?;
        let ks = db.keyspace(CHECKPOINT_KEYSPACE, KeyspaceCreateOptions::default)?;
        Ok((Arc::new(db), ks))
    }

    /// Open in **strict** mode — every entry is fsync'd synchronously.
    pub fn open_strict(data_dir: impl AsRef<Path>) -> Result<Arc<Self>, AppenderError> {
        Self::open_strict_with_rotation(data_dir, None, 0)
    }

    /// Same as [`Self::open_strict`] with optional rotation cap (in bytes).
    /// `retention_days` controls the age-based retention sweep that runs
    /// piggyback on each rotation (`0` disables it).
    pub fn open_strict_with_rotation(
        data_dir: impl AsRef<Path>,
        max_size_bytes: Option<u64>,
        retention_days: u32,
    ) -> Result<Arc<Self>, AppenderError> {
        let dir = data_dir.as_ref();
        std::fs::create_dir_all(dir)?;
        let (log_path, db_path) = Self::paths(dir);
        let log_file = Self::open_log(&log_path)?;
        let initial_size = log_file.metadata().map(|m| m.len()).unwrap_or(0);
        let (checkpoint_db, checkpoint_ks) = Self::open_db(&db_path)?;
        let rotation = RotationState::raw_policy(dir, max_size_bytes, initial_size, retention_days);
        Ok(Arc::new(Self::new_strict(
            log_path,
            log_file,
            (checkpoint_db, checkpoint_ks),
            rotation,
            None,
        )))
    }

    /// Shared strict-mode constructor (legacy openers and post-recovery
    /// boot both land here). No server-meta hook: raw legacy openers have
    /// none.
    fn new_strict(
        log_path: PathBuf,
        log_file: File,
        (checkpoint_db, checkpoint_ks): (Arc<Database>, Keyspace),
        rotation: Option<RotationState>,
        latest_fsynced: Option<Position>,
    ) -> Self {
        Self {
            log_file: Mutex::new(log_file),
            log_path,
            checkpoint_db,
            checkpoint_ks,
            mode: Durab::Strict,
            persistence_gate: Mutex::new(()),
            failed: Mutex::new(None),
            latest_fsynced: Mutex::new(latest_fsynced),
            meta_hook: None,
            #[cfg(test)]
            after_extract_hook: Mutex::new(None),
            #[cfg(any(test, feature = "test-hooks"))]
            fail_next_flush: std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)]
            gate_contention_hook: Mutex::new(None),
            #[cfg(any(test, feature = "test-hooks"))]
            gate_acquired_hook: Mutex::new(None),
            #[cfg(any(test, feature = "test-hooks"))]
            final_close_attempts: std::sync::atomic::AtomicUsize::new(0),
            refused_checkpoint_attempts: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(test)]
            after_append_hook: Mutex::new(None),
            #[cfg(test)]
            before_gate_checkpoint_hook: Mutex::new(None),
            #[cfg(test)]
            dir_sync_hook: Mutex::new(None),
            rotation,
        }
    }

    // Durable anchor row storage, strict decoding and anchor validation
    // live in the named sibling `checkpoint` module (round-3 item 4:
    // raw-path parity with recovery).

    /// Open in **batched** mode — entries buffered in memory, flushed every
    /// `flush_every` (≤5s per spec). Checkpoint writes remain immediate.
    pub fn open_batched(
        data_dir: impl AsRef<Path>,
        flush_every: Duration,
    ) -> Result<Arc<Self>, AppenderError> {
        Self::open_batched_with_rotation(data_dir, flush_every, None, 0)
    }

    /// Same as [`Self::open_batched`] with optional rotation cap (in bytes).
    /// `retention_days` controls the age-based retention sweep that runs
    /// piggyback on each rotation (`0` disables it).
    pub fn open_batched_with_rotation(
        data_dir: impl AsRef<Path>,
        flush_every: Duration,
        max_size_bytes: Option<u64>,
        retention_days: u32,
    ) -> Result<Arc<Self>, AppenderError> {
        let dir = data_dir.as_ref();
        std::fs::create_dir_all(dir)?;
        let (log_path, db_path) = Self::paths(dir);
        let log_file = Self::open_log(&log_path)?;
        let initial_size = log_file.metadata().map(|m| m.len()).unwrap_or(0);
        let (checkpoint_db, checkpoint_ks) = Self::open_db(&db_path)?;
        let rotation = RotationState::raw_policy(dir, max_size_bytes, initial_size, retention_days);
        Ok(Self::start_batched_with(
            log_path,
            log_file,
            (checkpoint_db, checkpoint_ks),
            flush_every,
            rotation,
            None,
            None,
        ))
    }

    fn start_batched_with(
        log_path: PathBuf,
        log_file: File,
        (checkpoint_db, checkpoint_ks): (Arc<Database>, Keyspace),
        flush_every: Duration,
        rotation: Option<RotationState>,
        latest_fsynced: Option<Position>,
        meta_hook: Option<ServerMetaHook>,
    ) -> Arc<Self> {
        let notify = Arc::new(Notify::new());
        let shutdown = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let me = Arc::new(Self {
            log_file: Mutex::new(log_file),
            log_path,
            checkpoint_db,
            checkpoint_ks,
            mode: Durab::Batched {
                buffer: Mutex::new(Vec::new()),
                notify: notify.clone(),
                shutdown: shutdown.clone(),
                task: Mutex::new(None),
            },
            persistence_gate: Mutex::new(()),
            failed: Mutex::new(None),
            latest_fsynced: Mutex::new(latest_fsynced),
            meta_hook,
            #[cfg(test)]
            after_extract_hook: Mutex::new(None),
            #[cfg(any(test, feature = "test-hooks"))]
            fail_next_flush: std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)]
            gate_contention_hook: Mutex::new(None),
            #[cfg(any(test, feature = "test-hooks"))]
            gate_acquired_hook: Mutex::new(None),
            #[cfg(any(test, feature = "test-hooks"))]
            final_close_attempts: std::sync::atomic::AtomicUsize::new(0),
            refused_checkpoint_attempts: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(test)]
            after_append_hook: Mutex::new(None),
            #[cfg(test)]
            before_gate_checkpoint_hook: Mutex::new(None),
            #[cfg(test)]
            dir_sync_hook: Mutex::new(None),
            rotation,
        });

        // Spawn the background flusher.
        //
        // The flush waits on the persistence gate, which a checkpoint can
        // hold across its own fsync, so the gate + drain run on a blocking
        // thread that this task awaits: the flusher's poll never sits inside
        // a `parking_lot` gate wait, which on a current-thread runtime would
        // stall the whole runtime (shutdown deadline timer included).
        let weak = Arc::downgrade(&me);
        let task = tokio::spawn(async move {
            loop {
                let sleep = tokio::time::sleep(flush_every);
                tokio::pin!(sleep);
                tokio::select! {
                    _ = &mut sleep => {}
                    _ = notify.notified() => {}
                }
                let Some(strong) = weak.upgrade() else {
                    break;
                };
                // A stop request wins over one more flush: the caller's
                // final flush drains the buffer anyway.
                if shutdown.load(std::sync::atomic::Ordering::SeqCst) {
                    break;
                }
                // Kept out of the job: the job owns `strong` and the error
                // branch must still reach the latch after the job panicked.
                let latch = Arc::clone(&strong);
                let result = tokio::task::spawn_blocking(move || {
                    let _guard = strong.acquire_persistence_gate();
                    strong.flush_buffer_locked()
                })
                .await;
                match result {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => {
                        // `flush_buffer_locked` latched this itself; the final
                        // drain reports it through `join_flusher`'s caller.
                        tracing::error!(%error, "audit background flush failed; appender latched fail-closed");
                    }
                    Err(error) => {
                        // The blocking job panicked or was cancelled after it
                        // took the buffer: those entries are lost and the chain
                        // may have a gap. Latch fail-closed AND return the
                        // failure, so the final drain cannot succeed and the
                        // shutdown report cannot miss the failed job.
                        let message = format!("audit background flush job failed: {error}");
                        *latch.failed.lock() = Some(message.clone());
                        tracing::error!(%error, "audit background flush job failed; appender latched fail-closed");
                        return Err(message);
                    }
                }
                if shutdown.load(std::sync::atomic::Ordering::SeqCst) {
                    break;
                }
            }
            Ok(())
        });

        if let Durab::Batched { task: t, .. } = &me.mode {
            *t.lock() = Some(task);
        }
        me
    }

    /// Authenticate the retained history, then open the batched appender at
    /// its verified tail. Does not update either checkpoint store.
    ///
    /// Hold the data-directory lock throughout recovery and subsequent use.
    /// Call from a Tokio runtime with time enabled. Only an unparseable
    /// final active fragment within the size limit may be discarded, and
    /// only after the complete prefix and both trusted anchors agree; a
    /// parseable active record without LF is refused even if it could
    /// authenticate; rotated fragments are never repaired; complete records
    /// beyond the protocol data-frame ceiling plus fixed-column overhead
    /// are rejected. Recovery verifies sequence continuity, predecessor
    /// links and record HMACs before returning the next sequence.
    pub fn open_recovered_batched(
        data_dir: impl AsRef<Path>,
        audit_chain_key: &[u8; 32],
        legacy_checkpoint: Option<(u64, [u8; 32])>,
        flush_every: Duration,
        max_size_bytes: Option<u64>,
        retention_days: u32,
    ) -> Result<(Arc<Self>, Position), AuditRecoveryError> {
        Self::open_recovered_impl(
            data_dir.as_ref(),
            audit_chain_key,
            legacy_checkpoint,
            flush_every,
            max_size_bytes,
            retention_days,
            None,
        )
    }

    /// Shared recovery constructor for [`Self::open_recovered_batched`] and
    /// [`Self::open_recovered_batched_with_meta`]: recover, then start the
    /// batched appender at the verified position (the fsynced baseline).
    fn open_recovered_impl(
        data_dir: &Path,
        audit_chain_key: &[u8; 32],
        legacy_checkpoint: Option<Position>,
        flush_every: Duration,
        max_size_bytes: Option<u64>,
        retention_days: u32,
        meta_hook: Option<ServerMetaHook>,
    ) -> Result<(Arc<Self>, Position), AuditRecoveryError> {
        let recovered = recovery::recover(data_dir, audit_chain_key, legacy_checkpoint)?;
        let position = recovered.position;
        let appender = Self::start_batched_with(
            recovered.log_path,
            recovered.log_file,
            recovered.checkpoint,
            flush_every,
            max_size_bytes.map(|max_size_bytes| RotationState {
                policy: RotationPolicy {
                    max_size_bytes,
                    current_size: std::sync::atomic::AtomicU64::new(recovered.initial_size),
                    retention_days,
                },
                // Recovered order truncated to `None` when retention is
                // disabled.
                legacy_archive_order: (retention_days != 0)
                    .then_some(recovered.legacy_archive_order)
                    .flatten(),
            }),
            Some(position),
            meta_hook,
        );
        Ok((appender, position))
    }

    /// Production boot path: [`Self::open_recovered_batched`] plus a final
    /// `impl Fn(u64, [u8; 32]) -> io::Result<()> + Send + Sync + 'static`
    /// hook run under the persistence gate at every actual rotation and the
    /// final batched shutdown, after the appender anchor is persisted and
    /// before retention prunes. Does not itself update either checkpoint
    /// store. Lock/runtime/fragment guarantees as
    /// [`Self::open_recovered_batched`].
    pub fn open_recovered_batched_with_meta(
        data_dir: impl AsRef<Path>,
        audit_chain_key: &[u8; 32],
        legacy_checkpoint: Option<Position>,
        flush_every: Duration,
        max_size_bytes: Option<u64>,
        retention_days: u32,
        server_meta: impl Fn(u64, [u8; 32]) -> io::Result<()> + Send + Sync + 'static,
    ) -> Result<(Arc<Self>, Position), AuditRecoveryError> {
        Self::open_recovered_impl(
            data_dir.as_ref(),
            audit_chain_key,
            legacy_checkpoint,
            flush_every,
            max_size_bytes,
            retention_days,
            Some(Arc::new(server_meta)),
        )
    }

    /// Strict-mode CLI path: authenticate the retained history, then return
    /// the verified appender and its resume position. No rotation policy
    /// and no server-meta hook; the recovery baseline is recorded as the
    /// latest fsynced anchor.
    ///
    /// Lock/runtime/fragment guarantees as [`Self::open_recovered_batched`].
    pub fn open_recovered_strict(
        data_dir: impl AsRef<Path>,
        audit_chain_key: &[u8; 32],
        meta_checkpoint: Option<Position>,
    ) -> Result<(Arc<Self>, Position), AuditRecoveryError> {
        let recovered = recovery::recover(data_dir.as_ref(), audit_chain_key, meta_checkpoint)?;
        let position = recovered.position;
        let appender = Arc::new(Self::new_strict(
            recovered.log_path,
            recovered.log_file,
            recovered.checkpoint,
            None,
            Some(position),
        ));
        Ok((appender, position))
    }

    /// Re-read the on-disk fixed-format log into [`AuditEntry`]s for
    /// startup verification ([`shamir_connect::server::audit_chain::AuditChain::verify_chain`]).
    pub fn read_log_for_verify(
        data_dir: impl AsRef<Path>,
    ) -> Result<Vec<AuditEntry>, AppenderError> {
        recovery::read_log_for_verify(data_dir.as_ref())
    }

    /// Force flush buffered entries (batched mode) and fsync.
    pub async fn flush_now(&self) -> Result<(), AppenderError> {
        if let Some(message) = self.failed.lock().as_ref() {
            return Err(AppenderError::Io(std::io::Error::other(message.clone())));
        }
        match &self.mode {
            Durab::Strict => Ok(()),
            Durab::Batched { notify, .. } => {
                blocking_io(|| {
                    let _guard = self.acquire_persistence_gate();
                    self.flush_buffer_locked()
                })?;
                notify.notify_one();
                Ok(())
            }
        }
    }

    /// Shutdown the batched flusher, draining any buffered entries.
    ///
    /// The three steps ([`Self::request_flusher_stop`],
    /// [`Self::join_flusher`], [`Self::final_flush_blocking`]) are also
    /// available separately so the server's bounded audit teardown can
    /// budget each one and report which failed; this wrapper keeps the
    /// historical stop-join-flush order and returns all three results.
    pub async fn shutdown(&self) -> AuditShutdownOutcome {
        self.request_flusher_stop();
        let flusher_join_error = self.join_flusher().await.err();
        // Strict mode has nothing to flush (`final_flush_blocking` returns
        // before taking the gate), and `blocking_io` is `block_in_place`,
        // which PANICS inside a `LocalSet`. Batched mode is unchanged.
        let final_flush = if matches!(&self.mode, Durab::Strict) {
            self.final_flush_blocking()
        } else {
            blocking_io(|| self.final_flush_blocking())
        };
        let reason = final_flush
            .flush_error
            .as_ref()
            .or(final_flush.anchor_error.as_ref());
        if let Some(error) = reason {
            tracing::error!(%error, "audit appender final shutdown flush failed; latched fail-closed");
        }
        AuditShutdownOutcome {
            flusher_join_error,
            flush_error: final_flush.flush_error,
            anchor_error: final_flush.anchor_error,
        }
    }

    /// Drain buffer while the caller owns the persistence gate.
    fn flush_buffer_locked(&self) -> Result<(), AppenderError> {
        let mut buffer = match &self.mode {
            Durab::Batched { buffer, .. } => buffer.lock(),
            Durab::Strict => return Ok(()),
        };
        if let Some(message) = self.failed.lock().as_ref() {
            return Err(AppenderError::Io(std::io::Error::other(message.clone())));
        }
        let entries: Vec<AuditEntry> = std::mem::take(&mut *buffer);
        drop(buffer);
        #[cfg(test)]
        let after_extract_hook = self.after_extract_hook.lock().take();
        #[cfg(test)]
        if let Some(hook) = after_extract_hook {
            hook();
        }
        if entries.is_empty() {
            return Ok(());
        }
        let result = (|| {
            #[cfg(any(test, feature = "test-hooks"))]
            if self
                .fail_next_flush
                .swap(false, std::sync::atomic::Ordering::SeqCst)
            {
                return Err(AppenderError::Io(std::io::Error::other(
                    "injected flush failure",
                )));
            }

            let mut f = self.log_file.lock();
            let mut bytes_written = 0u64;
            for e in &entries {
                let mut bytes = entry_to_line(e);
                bytes.push(b'\n');
                f.write_all(&bytes)?;
                bytes_written += bytes.len() as u64;
            }
            f.flush()?;
            f.sync_all()?;
            // Record the latest fsynced chain position (finding 1).
            if let Some(last) = entries.last() {
                let next_seq = last.seq.checked_add(1).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, AUDIT_SEQ_EXHAUSTED)
                })?;
                *self.latest_fsynced.lock() = Some((next_seq, last.hmac));
            }
            if let Some(rot) = &self.rotation {
                let new_size = rot
                    .policy
                    .current_size
                    .fetch_add(bytes_written, std::sync::atomic::Ordering::Relaxed)
                    + bytes_written;
                if new_size >= rot.policy.max_size_bytes {
                    // A rotation failure at or after the rename leaves an
                    // unanchored rotated file behind — latch fail-closed.
                    rotation::rotate_locked(self, &mut f, rot.legacy_archive_order.as_deref())?;
                }
            }
            Ok::<(), AppenderError>(())
        })();
        if let Err(error) = result {
            *self.failed.lock() = Some(error.to_string());
            return Err(error);
        }
        Ok(())
    }

    /// Write one entry synchronously + fsync. Used in strict mode.
    fn append_strict(&self, entry: &AuditEntry) -> Result<(), AppenderError> {
        let mut bytes = entry_to_line(entry);
        bytes.push(b'\n');
        let bytes_written = bytes.len() as u64;
        let mut f = self.log_file.lock();
        f.write_all(&bytes)?;
        f.flush()?;
        f.sync_all()?;
        // Record the latest fsynced chain position (finding 1).
        let next_seq = entry
            .seq
            .checked_add(1)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, AUDIT_SEQ_EXHAUSTED))?;
        *self.latest_fsynced.lock() = Some((next_seq, entry.hmac));
        if let Some(rot) = &self.rotation {
            let new_size = rot
                .policy
                .current_size
                .fetch_add(bytes_written, std::sync::atomic::Ordering::Relaxed)
                + bytes_written;
            if new_size >= rot.policy.max_size_bytes {
                // A rotation failure at or after the rename leaves an
                // unanchored rotated file behind; the error returned here
                // reaches `append_entry`, which latches it fail-closed.
                rotation::rotate_locked(self, &mut f, rot.legacy_archive_order.as_deref())?;
            }
        }
        Ok(())
    }

    /// Flush the freshly reopened writable ACTIVE handle (meaningful on
    /// Windows, where a read-only handle sync proves nothing), then the
    /// parent directory entry; `dir_sync_hook` overrides [`rotation::sync_parent`].
    fn dir_sync(&self, log_path: &Path, current: &File) -> io::Result<()> {
        #[cfg(test)]
        if let Some(hook) = self.dir_sync_hook.lock().clone() {
            return hook(log_path.to_path_buf());
        }
        current.sync_all()?;
        rotation::sync_parent(log_path)
    }

    // Checkpoint persistence, strict row decoding, anchor validation and
    // the fsynced-anchor + meta-hook path live in the named sibling
    // `checkpoint` module impl block (round-3 item 4).

    /// The message latched by the fail-closed path, if any. Exposed so the
    /// ordered-shutdown task can observe a failed final anchor write or
    /// server-meta hook (deferred shutdown-reporting item); `shutdown()`
    /// execution and reporting are unchanged, and the first failure still
    /// retains its type as the trait io error's source.
    pub fn last_failure(&self) -> Option<String> {
        self.failed.lock().clone()
    }

    /// Acquire the sync gate, exposing only actual contention to the tests.
    fn acquire_persistence_gate(&self) -> parking_lot::MutexGuard<'_, ()> {
        if let Some(guard) = self.persistence_gate.try_lock() {
            #[cfg(any(test, feature = "test-hooks"))]
            {
                let hook = self.gate_acquired_hook.lock().take();
                if let Some(hook) = hook {
                    hook();
                }
            }
            return guard;
        }
        #[cfg(test)]
        {
            let hook = self.gate_contention_hook.lock().take();
            if let Some(hook) = hook {
                hook();
            }
        }
        let guard = self.persistence_gate.lock();
        #[cfg(any(test, feature = "test-hooks"))]
        {
            let hook = self.gate_acquired_hook.lock().take();
            if let Some(hook) = hook {
                hook();
            }
        }
        guard
    }

    /// Run synchronous persistence under the persistence gate (design (b)):
    /// never awaits and never joins a lock waiter; checkpoint flush +
    /// metadata share this gate.
    fn persistence_section<T>(
        &self,
        f: impl FnOnce() -> Result<T, AppenderError>,
    ) -> Result<T, AppenderError> {
        let _guard = self.acquire_persistence_gate();
        f()
    }
}

/// Run blocking I/O without stalling a tokio worker thread, when doing so
/// is actually possible.
///
/// `block_in_place` panics outside a multi-thread tokio runtime (no
/// runtime at all, or a current-thread one), so this checks
/// `runtime_flavor()` explicitly, falling back to a direct call (correct:
/// there is no separate worker thread to protect).
fn blocking_io<T>(f: impl FnOnce() -> T) -> T {
    let is_multi_thread = tokio::runtime::Handle::try_current()
        .map(|h| h.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread)
        .unwrap_or(false);
    if is_multi_thread {
        tokio::task::block_in_place(f)
    } else {
        f()
    }
}

// `AuditAppender` is defined in `shamir-connect` and `Arc` is from `std`,
// so the orphan rule forbids `impl AuditAppender for Arc<FjallAuditAppender>`.
// Instead we implement on `FjallAuditAppender` directly — when callers wrap
// the inner type in `Arc<dyn AuditAppender>` (as `AuditChainWriter::new`
// requires) the `Arc::new` -> `Arc<dyn Trait>` coercion uses this impl.
impl AuditAppender for FjallAuditAppender {
    fn append_entry(&self, entry: &AuditEntry) -> Result<(), std::io::Error> {
        // Shared exact size validation (finding 5), before actual
        // enqueue/write. A caller's oversized record is refused WITHOUT
        // latching: only actual failed I/O latches fail-closed.
        if let Err(error) = entry.validate_log_record_size() {
            return Err(std::io::Error::new(io::ErrorKind::InvalidInput, error));
        }
        // Recovery's exclusive upper bound on `next_seq` is `u64::MAX`; a
        // record this close to the end would overflow the successor
        // position. Checked input refusal before enqueue/write.
        if entry.seq >= u64::MAX - 1 {
            return Err(std::io::Error::new(
                io::ErrorKind::InvalidInput,
                AUDIT_SEQ_EXHAUSTED,
            ));
        }
        if let Some(message) = self.failed.lock().as_ref() {
            return Err(std::io::Error::other(message.clone()));
        }
        let result = match &self.mode {
            Durab::Strict => blocking_io(|| {
                self.persistence_section(|| {
                    if let Some(message) = self.failed.lock().as_ref() {
                        return Err(AppenderError::Io(std::io::Error::other(message.clone())));
                    }
                    let result = self.append_strict(entry);
                    if let Err(error) = &result {
                        *self.failed.lock() = Some(error.to_string());
                    }
                    result
                })
            })
            .map_err(std::io::Error::other),
            Durab::Batched {
                buffer, shutdown, ..
            } => {
                let mut buffer = buffer.lock();
                // The final flush takes this same lock after setting shutdown.
                // A push already holding the lock is included in that flush.
                if shutdown.load(std::sync::atomic::Ordering::SeqCst) {
                    return Err(std::io::Error::other("audit appender is shut down"));
                }
                buffer.push(entry.clone());
                drop(buffer);
                #[cfg(test)]
                self.fire_after_append_hook();
                Ok(())
            }
        };
        if let Err(error) = result {
            *self.failed.lock() = Some(error.to_string());
            return Err(error);
        }
        Ok(())
    }

    fn checkpoint(&self, next_seq: u64, prev_hmac: &[u8; 32]) -> Result<(), std::io::Error> {
        // Post-close fence, cheap half: gate-free, store-free (Strict: no flag).
        self.refuse_closed_checkpoint()?;
        #[cfg(test)]
        self.fire_before_gate_checkpoint_hook();
        if let Some(message) = self.failed.lock().as_ref() {
            return Err(std::io::Error::other(message.clone()));
        }
        blocking_io(|| {
            self.persistence_section(|| {
                // DECISIVE half: same gate as `final_flush_blocking`.
                self.refuse_closed_checkpoint_under_gate()?;
                if let Some(message) = self.failed.lock().as_ref() {
                    return Err(AppenderError::Io(std::io::Error::other(message.clone())));
                }
                let result = self.checkpoint_locked(next_seq, prev_hmac);
                if let Err(error) = &result {
                    *self.failed.lock() = Some(error.to_string());
                }
                result
            })
        })
        .map_err(|error| self.latch_checkpoint_failure(std::io::Error::other(error)))
    }
}

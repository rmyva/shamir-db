//! Segmented WAL sink — a directory of numbered append-only segments.
//!
//! [`SegmentSet`] composes [`WalSegment`]s: at any moment there is exactly
//! one *active* segment (the append tail) plus zero or more *sealed*
//! segments (closed, replay/delete-only). When the active segment crosses
//! `max_bytes` it is sealed and a fresh active segment with the next seq is
//! rotated in. Truncation deletes whole sealed segments once every record
//! in them is durable in history (`max_version <= durable_watermark`).
//!
//! Files are named `NNNNNNNN.wal` (zero-padded 8-digit seq, lexical =
//! chronological). The active segment is never deleted nor rewritten — the
//! append path and the truncation path therefore never touch the same file
//! (zero writer↔truncator coordination, see `docs/dev-artifacts/perf/f6-subplan.md` §1).
//!
//! Wiring: production repository initialization constructs the set via
//! [`SegmentSet::open`] and installs it as [`crate::WalSink::File`] behind
//! [`crate::WalGroupCommit`] (see `shamir-engine`'s repo-instance
//! transaction bootstrap).

use std::fmt;
use std::fs::{metadata, remove_file, File};
use std::path::Path;
pub(super) use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
pub(super) use std::sync::{Arc, Mutex};

pub(super) use shamir_storage::error::{DbError, DbResult};
pub(super) use tokio::task::spawn_blocking;

use crate::segment::frame_reader::{FrameRead, FrameReader};
pub(super) use crate::segment::wal_sink::WalSink;
pub(super) use crate::wal_entry_v2::WalEntryV2;
pub(super) use crate::wal_segment::{wal_io_error, WalSegment};

/// A failure while reading an offline WAL without crash-tail tolerance.
#[derive(Debug)]
#[non_exhaustive]
pub enum WalStrictReadError {
    /// A partial frame header, payload, or checksum at the given frame start.
    IncompleteFrame { path: PathBuf, offset: u64 },
    /// A checksum-valid frame whose payload failed to decode.
    Decode {
        path: PathBuf,
        offset: u64,
        source: DbError,
    },
    /// Discovery, file I/O, frame size, checksum, or blocking task failure.
    Other(DbError),
}

impl fmt::Display for WalStrictReadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IncompleteFrame { path, offset } => {
                write!(
                    f,
                    "Incomplete WAL frame in {path:?} at byte offset {offset}"
                )
            }
            Self::Decode {
                path,
                offset,
                source,
            } => {
                write!(
                    f,
                    "WAL decode failed in {path:?} at byte offset {offset}: {source}"
                )
            }
            Self::Other(error) => fmt::Display::fmt(error, f),
        }
    }
}

impl std::error::Error for WalStrictReadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::IncompleteFrame { .. } => None,
            Self::Decode { source, .. } | Self::Other(source) => Some(source),
        }
    }
}

impl From<DbError> for WalStrictReadError {
    fn from(error: DbError) -> Self {
        Self::Other(error)
    }
}

/// Metadata for one sealed (closed) segment. Its file is fully written
/// (no torn tail) and may be replayed or deleted but never appended.
#[derive(Clone)]
pub(super) struct SealedMeta {
    pub(super) seq: u64,
    pub(super) path: PathBuf,
    /// Highest commit version, independent of retention eligibility.
    pub(super) max_version: u64,
    /// Any zero-version record pins the whole segment.
    pub(super) pinned: bool,
    pub(super) synced: bool,
    /// In-memory on-disk length of the sealed file — size-mirror only,
    /// never persisted (the sidecar format is untouched).
    pub(super) bytes: u64,
}

/// Mutable state guarded by the single coarse `Mutex`. Sealed-list mutation
/// happens only on rotation (the append leader) and truncation — both rare,
/// never on a per-record hot path.
pub(super) struct Inner {
    pub(super) sealed: Vec<SealedMeta>,
    pub(super) active: Arc<WalSegment>,
    pub(super) active_seq: u64,
}

#[cfg(test)]
type TruncatePause = (
    tokio::sync::oneshot::Sender<()>,
    std::sync::mpsc::Receiver<()>,
);

/// Segmented WAL sink over a directory of numbered segments.
///
/// Concurrency: the sealed list + active handle live behind one
/// **sanctioned** `std::sync::Mutex` (CLAUDE.md "Banned in hot paths").
/// It guards only O(1) metadata edits (clone the active `Arc`, push/scan a
/// short `Vec`, swap the active handle on rotation) — never an I/O syscall,
/// and is NEVER held across an `.await`: every `WalSegment` call takes a
/// cloned `Arc` after the lock is dropped. The single-writer model is the
/// group-commit leader (the sole appender) plus the truncator (rare); they
/// do not contend on a hot path. CAPSTONE replaces this with a
/// single-writer task.
pub struct SegmentSet {
    pub(super) dir: PathBuf,
    pub(super) max_bytes: u64,
    pub(super) inner: Arc<Mutex<Inner>>,
    truncate_gate: Arc<tokio::sync::Mutex<()>>,
    #[cfg(test)]
    truncate_fail_seq: Mutex<Option<u64>>,
    #[cfg(test)]
    truncate_pause: Mutex<Option<TruncatePause>>,
    // Cold sync/rotation exclusion only; ordinary appends never acquire it.
    pub(crate) sync_gate: Arc<tokio::sync::Mutex<()>>,
    // O(1) scrape mirrors (design §2.4): Relaxed RMWs only, never a lock.
    // `Arc`-wrapped so the blocking truncate job can decrement under the
    // sealed-list lock it already holds (precedent: `WalGroupCommit::flushing`).
    /// Mirror of retained segment FILES (sealed + the one active segment).
    segment_count: Arc<AtomicUsize>,
    /// Cumulative WAL payload bytes appended (one relaxed RMW per window).
    bytes_written_total: Arc<AtomicU64>,
    #[cfg(test)]
    pub(crate) sync_test: super::sync_test_seam::SyncTestSeam,
    #[cfg(test)]
    fail_retry_write: std::sync::atomic::AtomicBool,
    // Test-only one-shot seal-fsync fault, scoped to this WAL/repository.
    fail_next_seal_sync: std::sync::atomic::AtomicBool,
}

/// Format a segment file name from its seq: `NNNNNNNN.wal` (zero-padded 8).
pub(super) fn seg_file_name(seq: u64) -> String {
    format!("{seq:08}.wal")
}

/// Parse a segment seq from a `NNNNNNNN.wal` file stem; `None` if the name
/// is not a numeric `.wal` segment.
fn parse_seg_seq(name: &str) -> Option<u64> {
    let stem = name.strip_suffix(".wal")?;
    let seq = stem.parse::<u64>().ok()?;
    (seg_file_name(seq) == name).then_some(seq)
}

fn discover_segment_seqs(scan_dir: &Path) -> DbResult<Vec<u64>> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(scan_dir)
        .map_err(|e| wal_io_error(format!("SegmentSet read_dir {scan_dir:?}"), e))?
    {
        let entry = entry.map_err(|e| wal_io_error("SegmentSet dir entry".into(), e))?;
        if let Some(name) = entry.file_name().to_str() {
            if let Some(seq) = parse_seg_seq(name) {
                out.push(seq);
            } else if let Some(stem) = name.strip_suffix(".wal") {
                if stem.parse::<u64>().is_ok() {
                    return Err(DbError::Storage(format!(
                        "SegmentSet discovered non-canonical segment name {name:?} in {scan_dir:?}; refusing to open (possible sequence alias)"
                    )));
                }
            }
        }
    }
    out.sort_unstable();
    if out.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(DbError::Storage(
            "SegmentSet contains multiple filenames for the same segment sequence".into(),
        ));
    }
    Ok(out)
}

fn retention_summary(entries: &[WalEntryV2]) -> (u64, bool) {
    (
        entries.iter().map(|e| e.commit_version).max().unwrap_or(0),
        entries.iter().any(|e| e.commit_version == 0),
    )
}

impl SegmentSet {
    /// Open (creating if absent) the segment directory `dir`. `max_bytes`
    /// is the seal/rotate threshold (F6b passes
    /// `shamir_tunables::instance_defaults::WAL_SEGMENT_MAX_BYTES`).
    ///
    /// Scans `*.wal`, parses each seq, sorts ascending; the highest seq is
    /// the active segment, the rest are sealed. Each sealed segment's
    /// `max_version` is computed once here via a one-shot `replay` (a rare
    /// open-time cost). An empty directory gets a fresh active `00000000.wal`.
    pub async fn open(dir: PathBuf, max_bytes: u64) -> DbResult<Self> {
        let scan_dir = dir.clone();
        let mut seqs: Vec<u64> = spawn_blocking(move || -> DbResult<Vec<u64>> {
            std::fs::create_dir_all(&scan_dir)
                .map_err(|e| wal_io_error(format!("SegmentSet create_dir {scan_dir:?}"), e))?;
            discover_segment_seqs(&scan_dir)
        })
        .await
        .map_err(|e| DbError::Internal(format!("spawn_blocking join: {e}")))??;

        if seqs.is_empty() {
            // Fresh directory — create the first active segment.
            let active_seq = 0u64;
            let active_path = dir.join(seg_file_name(active_seq));
            let active = Arc::new(WalSegment::open(active_path).await?);
            // Mirror seed (design §2.4): one retained segment FILE, zero
            // payload bytes in a freshly created segment.
            let active_bytes = active.approx_len_bytes();
            return Ok(Self {
                dir,
                max_bytes,
                sync_gate: Arc::new(tokio::sync::Mutex::new(())),
                segment_count: Arc::new(AtomicUsize::new(1)),
                bytes_written_total: Arc::new(AtomicU64::new(active_bytes)),
                #[cfg(test)]
                sync_test: super::sync_test_seam::SyncTestSeam::default(),
                #[cfg(test)]
                fail_retry_write: std::sync::atomic::AtomicBool::new(false),
                fail_next_seal_sync: std::sync::atomic::AtomicBool::new(false),
                truncate_gate: Arc::new(tokio::sync::Mutex::new(())),
                #[cfg(test)]
                truncate_fail_seq: Mutex::new(None),
                #[cfg(test)]
                truncate_pause: Mutex::new(None),
                inner: Arc::new(Mutex::new(Inner {
                    sealed: Vec::new(),
                    active,
                    active_seq,
                })),
            });
        }

        // Highest seq = active — UNLESS it carries a valid sidecar.
        let mut active_seq = seqs.pop().expect("non-empty after is_empty check");
        let mut sealed = Vec::with_capacity(seqs.len());
        for seq in seqs {
            let path = dir.join(seg_file_name(seq));
            // Fast path (task #500): a sealed segment written by this build has
            // a `NNNNNNNN.meta` sidecar recording its `max_version`. Read it
            // (a ~17-byte read) and skip the full replay entirely. `None`
            // means absent OR corrupt/torn (both fall back identically — a
            // corrupt sidecar is never trusted, and never fatal); only a hard
            // I/O error surfaces.
            let (meta_probe, bytes) = {
                let probe_path = path.clone();
                spawn_blocking(move || {
                    // Cold open-time probe: on-disk length for the size mirror.
                    let bytes = metadata(&probe_path).map(|meta| meta.len()).unwrap_or(0);
                    crate::segment_meta::read_blocking(&probe_path).map(|probe| (probe, bytes))
                })
                .await
                .map_err(|e| DbError::Internal(format!("spawn_blocking join: {e}")))??
            };
            let (max_version, pinned) = match meta_probe {
                Some((mv, Some(pinned))) => (mv, pinned),
                _ => {
                    // Missing, corrupt, or legacy WMT2: replay to establish the pin.
                    let entries = WalSegment::replay_path(path.clone(), true, false, false).await?;
                    retention_summary(&entries)
                }
            };
            sealed.push(SealedMeta {
                seq,
                path,
                max_version,
                pinned,
                synced: false,
                bytes,
            });
        }
        // RV-WAL-A025886E — rule: "a segment that has a sidecar never changes."
        // If the highest segment has a VALID WMT2 or WMT3 sidecar it is sealed (its
        // data was fsync'd before the sidecar was written) and must never be
        // re-activated: create seq+1 as the new active instead. A WMT1
        // (legacy) sidecar reads as absent here, so the replay fallback runs
        // and a stale WMT1 value can never drive truncation.
        let mut active_path = dir.join(seg_file_name(active_seq));
        let (highest_meta, highest_bytes) = {
            let probe_path = active_path.clone();
            spawn_blocking(move || {
                // Same cold probe as the sealed loop above.
                let bytes = metadata(&probe_path).map(|meta| meta.len()).unwrap_or(0);
                crate::segment_meta::read_blocking(&probe_path).map(|probe| (probe, bytes))
            })
            .await
            .map_err(|e| DbError::Internal(format!("spawn_blocking join: {e}")))??
        };
        if let Some((cached_max, cached_pin)) = highest_meta {
            let (max_version, pinned) = match cached_pin {
                Some(pinned) => (cached_max, pinned),
                None => {
                    let entries =
                        WalSegment::replay_path(active_path.clone(), true, false, false).await?;
                    retention_summary(&entries)
                }
            };
            sealed.push(SealedMeta {
                seq: active_seq,
                path: active_path.clone(),
                max_version,
                pinned,
                synced: true,
                bytes: highest_bytes,
            });
            active_seq += 1;
            active_path = dir.join(seg_file_name(active_seq));
        }
        let active = Arc::new(WalSegment::open(active_path.clone()).await?);
        // Self-heal the active segment (audit §1.3): a prior process crash
        // mid-write (or a partial write that could not be rolled back) may
        // have left a torn tail. Sealed segments are fsync'd before sealing
        // (I4) so a torn tail is impossible there; the active segment is the
        // only one that can have one. Removing it now keeps the next
        // `append_batch` window from being stranded behind it on replay.
        active.repair_torn_tail().await?;
        let active_entries = active.replay_at_startup().await?;
        let (active_max, active_pin) = retention_summary(&active_entries);
        active.restore_retention(active_max, active_pin);
        // Mirror seed (design §2.4): retained FILES = sealed + 1; cumulative
        // bytes = recorded sealed lengths + the active segment's length.
        let sealed_bytes = sealed.iter().map(|m| m.bytes).sum::<u64>();
        let active_bytes = active.approx_len_bytes();

        // Cleanup only (RV-WAL-A025886E): with the sidecar-never-changes rule
        // the active segment can no longer carry a VALID sidecar (a highest
        // segment with one is sealed above), so this can only shed a
        // stray/corrupt/legacy sidecar. Correctness never depends on it —
        // failure is warned and swallowed.
        spawn_blocking(move || {
            crate::segment_meta::remove_blocking(
                &active_path,
                "re-activated segment kept a stale max_version sidecar; a future \
                 open may under-truncate on it until this is cleaned up",
            )
        })
        .await
        .map_err(|e| DbError::Internal(format!("spawn_blocking join: {e}")))?;

        Ok(Self {
            dir,
            max_bytes,
            sync_gate: Arc::new(tokio::sync::Mutex::new(())),
            segment_count: Arc::new(AtomicUsize::new(sealed.len() + 1)),
            bytes_written_total: Arc::new(AtomicU64::new(sealed_bytes + active_bytes)),
            #[cfg(test)]
            sync_test: super::sync_test_seam::SyncTestSeam::default(),
            #[cfg(test)]
            fail_retry_write: std::sync::atomic::AtomicBool::new(false),
            fail_next_seal_sync: std::sync::atomic::AtomicBool::new(false),
            truncate_gate: Arc::new(tokio::sync::Mutex::new(())),
            #[cfg(test)]
            truncate_fail_seq: Mutex::new(None),
            #[cfg(test)]
            truncate_pause: Mutex::new(None),
            inner: Arc::new(Mutex::new(Inner {
                sealed,
                active,
                active_seq,
            })),
        })
    }

    pub(crate) fn arm_fail_next_seal_sync_for_test(&self) {
        self.fail_next_seal_sync.store(true, Ordering::Release);
    }

    #[cfg(test)]
    pub(crate) fn inject_truncate_failure(&self, seq: u64) {
        *self.truncate_fail_seq.lock().unwrap() = Some(seq);
    }

    #[cfg(test)]
    pub(crate) fn pause_next_truncate(
        &self,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        std::sync::mpsc::Sender<()>,
    ) {
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (resume_tx, resume_rx) = std::sync::mpsc::channel();
        *self.truncate_pause.lock().unwrap() = Some((entered_tx, resume_rx));
        (entered_rx, resume_tx)
    }

    #[cfg(test)]
    pub(crate) fn inject_retry_write_failure(&self) {
        self.fail_retry_write
            .store(true, std::sync::atomic::Ordering::Release);
    }

    /// Append a batch to the active segment, then rotates if the active
    /// segment crossed `max_bytes`. Rotation is checked AFTER the whole
    /// batch is written so a batch never straddles two files (simplifies
    /// torn-tail handling and max_version accounting — see §4). Returns the
    /// seq assigned to the last entry in the active segment.
    ///
    /// **Poison / fault recovery (audit §1.3):** if the active segment is
    /// already poisoned (a prior write/sync failure) or the append fails
    /// mid-flight, the segment is sealed-as-poisoned and a brand-new active
    /// segment is rotated in — then the batch is retried ONCE on the fresh
    /// file. This breaks the "next leader writes to the same poisoned
    /// segment" bug: instead of retrying on the known-bad fd, we cut over to
    /// a fresh file. A second failure (the new segment also fails) is
    /// surfaced to the caller — we do not loop indefinitely.
    pub async fn append_batch(&self, payloads: Vec<Vec<u8>>, max_version: u64) -> DbResult<u64> {
        // Fast path: clone the active Arc under the lock, then release before awaiting.
        let mut active = {
            let g = self.inner.lock().expect("SegmentSet inner mutex poisoned");
            Arc::clone(&g.active)
        };
        // If the active segment is already poisoned, rotate BEFORE attempting
        // the append (writing to a poisoned segment always fails), and re-fetch
        // the freshly-rotated segment so the append below doesn't immediately
        // fail again on the stale (poisoned) Arc.
        if active.is_poisoned() {
            self.rotate_after_poison().await?;
            let g = self.inner.lock().expect("SegmentSet inner mutex poisoned");
            active = Arc::clone(&g.active);
        }
        // `payloads` is wrapped in an `Arc` ONCE here — the retry-after-poison
        // path (rare: only on a write/sync failure) then shares the SAME
        // allocation via a cheap refcount clone, instead of a full deep copy
        // of the batch on every append (the common, non-retry case pays only
        // one Arc::new, no data copy at all).
        let payloads = Arc::new(payloads);
        // One relaxed RMW per window: cumulative WAL payload bytes (§2.4).
        let batch_bytes = payloads.iter().map(|p| p.len()).sum::<usize>() as u64;
        match active
            .append_batch(Arc::clone(&payloads), max_version)
            .await
        {
            Ok(last_seq) => {
                self.bytes_written_total
                    .fetch_add(batch_bytes, Ordering::Relaxed);
                // Rotate iff this segment crossed the threshold. Read len
                // without holding the lock across the await above.
                if active.approx_len_bytes() >= self.max_bytes {
                    self.seal_and_rotate().await?;
                }
                Ok(last_seq)
            }
            Err(e) => {
                if !active.is_poisoned() {
                    return Err(e);
                }
                // A write or sync failure poisoned this segment. Rotate once
                // and retry on a fresh segment; non-poisoning errors returned above.
                log::error!(
                    "SegmentSet append failed on active segment; rotating and \
                     retrying once. Original error: {e}"
                );
                self.rotate_after_poison().await?;
                let active = {
                    let g = self.inner.lock().expect("SegmentSet inner mutex poisoned");
                    Arc::clone(&g.active)
                };
                #[cfg(test)]
                if self
                    .fail_retry_write
                    .swap(false, std::sync::atomic::Ordering::AcqRel)
                {
                    active.inject_partial_write_failure(7);
                }
                let last_seq = active.append_batch(payloads, max_version).await?;
                // The freshly-rotated segment is brand new and small — no
                // size-based rotation check needed here.
                // The first attempt failed and added nothing: this add counts
                // the batch exactly once.
                self.bytes_written_total
                    .fetch_add(batch_bytes, Ordering::Relaxed);
                Ok(last_seq)
            }
        }
    }

    /// Force a rotation because the current active segment is poisoned (a
    /// write/sync failure quarantined it — audit §1.3). Unlike
    /// `seal_and_rotate`, this does NOT fsync the segment being sealed (it is
    /// known-bad; an fsync would either fail again or falsely "succeed"
    /// without persisting the in-flight window) and does NOT require it to be
    /// over the size threshold. The poisoned segment is recorded in the
    /// sealed list with its current `max_version` so it remains replayable
    /// (its intact prefix is still valid data) and is reclaimable by
    /// truncation once durable in history — it simply never accepts new
    /// appends. A subsequent power-loss replays its intact prefix; the torn
    /// tail, if any, was already truncated by `WalSegment::append_batch`'s
    /// rollback (or will be healed by `repair_torn_tail` on next open).
    async fn rotate_after_poison(&self) -> DbResult<()> {
        let (sealed_seq, sealed_max, sealed_pin, sealed_bytes, next_seq) = {
            let g = self.inner.lock().expect("SegmentSet inner mutex poisoned");
            (
                g.active_seq,
                g.active.max_committed(),
                g.active.retention_pinned(),
                g.active.approx_len_bytes(),
                g.active_seq + 1,
            )
        };
        let sealed_path = self.dir.join(seg_file_name(sealed_seq));
        // Task #500 (found in @fl review): this segment may carry a STALE
        // on-disk max_version sidecar from a prior incarnation (e.g. it was
        // sealed-with-sidecar once, then re-activated after a crash before
        // the next segment's file existed — see the matching comment in
        // `open`). We record the correct `sealed_max` (read live from memory
        // above) in `SealedMeta` for THIS process's in-memory state, but a
        // stale on-disk sidecar would still mislead a FUTURE `open` into
        // trusting the old, too-low value instead of replaying. Drop it here
        // — a poisoned segment must never carry a sidecar (fresh or stale).
        {
            let stale_meta_path = sealed_path.clone();
            spawn_blocking(move || {
                crate::segment_meta::remove_blocking(
                    &stale_meta_path,
                    "poisoned segment must not keep a stale sidecar; a future open \
                     may under-truncate on it until this is cleaned up",
                )
            })
            .await
            .map_err(|e| DbError::Internal(format!("spawn_blocking join: {e}")))?;
        }
        let new_active = Arc::new(WalSegment::open(self.dir.join(seg_file_name(next_seq))).await?);
        let mut g = self.inner.lock().expect("SegmentSet inner mutex poisoned");
        // Re-check: another rotation may have advanced active past us.
        if g.active_seq != sealed_seq {
            return Ok(()); // someone else already rotated
        }
        g.sealed.push(SealedMeta {
            seq: sealed_seq,
            path: sealed_path,
            max_version: sealed_max,
            pinned: sealed_pin,
            synced: false,
            bytes: sealed_bytes,
        });
        g.active = new_active;
        g.active_seq = next_seq;
        Ok(())
    }

    /// Seal the current active segment (record its metadata in the sealed
    /// list) and open a fresh active segment with `active_seq + 1`.
    ///
    /// The new segment is opened OUTSIDE the lock (it does I/O); the lock is
    /// re-acquired only to swap the handles. If a concurrent caller already
    /// rotated past `active_seq` while we awaited the open, we abandon our
    /// freshly-opened segment rather than clobber the newer active (the
    /// single-writer leader model means this is not expected, but the guard
    /// keeps the invariant exact).
    ///
    /// Binding order (RV-WAL-A025886E): fsync → open the new active → swap it
    /// in under the lock → THEN write the WMT3 sidecar. The sidecar-bearing
    /// segment must never still be the live append tail when its sidecar lands
    /// on disk (cancellation/append hazard — see the inline comment).
    async fn seal_and_rotate(&self) -> DbResult<()> {
        // Snapshot the segment to seal under the lock.
        let (sealed_seq, sealed_path, sealed_max, sealed_pin, next_seq, sealing, sealed_bytes) = {
            let g = self.inner.lock().expect("SegmentSet inner mutex poisoned");
            // Guard against double-rotation: only the leader that still sees
            // an over-threshold active rotates it.
            if g.active.approx_len_bytes() < self.max_bytes {
                return Ok(());
            }
            (
                g.active_seq,
                self.dir.join(seg_file_name(g.active_seq)),
                g.active.max_committed(),
                g.active.retention_pinned(),
                g.active_seq + 1,
                Arc::clone(&g.active),
                g.active.approx_len_bytes(),
            )
        };

        // Make the segment being sealed durable on disk BEFORE it leaves the
        // active slot (I4: a sealed segment is fully written — including
        // fsync'd — so a later torn tail can only occur on the active segment,
        // and `SegmentSet::sync` need only fsync the active one). Done outside
        // the lock (it is an fsync syscall). This also underpins truncation
        // crash-safety (F6c): a sealed segment's records are crash-durable, so
        // a power-loss before they are drained to `history` still replays them.
        {
            let _sync = self.sync_gate.lock().await;
            // Test seam (RV-WAL-3B29232E): models the seal fsync failing after
            // the frames were written — exactly the outcome the WAL cannot
            // prove rolled back (tickets go UNKNOWN).
            if WalSink::take_fail_next_seal_sync_for_test(&self.fail_next_seal_sync) {
                return Err(DbError::Storage(
                    "SegmentSet injected seal fsync failure".into(),
                ));
            }
            self.sync_active(&sealing).await?;
        }

        let new_active = Arc::new(WalSegment::open(self.dir.join(seg_file_name(next_seq))).await?);

        // Scoped so the std Mutex guard is provably dropped before the
        // blocking sidecar I/O below (keeps the future Send).
        {
            let mut g = self.inner.lock().expect("SegmentSet inner mutex poisoned");
            // Re-check: another rotation may have advanced active past us.
            if g.active_seq != sealed_seq {
                // Already rotated — drop our freshly-opened (empty) segment. No
                // sidecar is written on this path: the winner did not seal this
                // segment, so leaving it sidecar-free keeps `open` on the (correct)
                // replay fallback.
                return Ok(());
            }
            g.sealed.push(SealedMeta {
                seq: sealed_seq,
                path: sealed_path.clone(),
                max_version: sealed_max,
                pinned: sealed_pin,
                synced: true,
                bytes: sealed_bytes,
            });
            // RV-WAL-A025886E binding order: the active-pointer swap happens BEFORE
            // the sidecar write. A sidecar means "this segment is sealed and its
            // bytes never change" — if it were written while the segment is still
            // the live active tail, a concurrent append or a cancellation of this
            // task between the two steps would mutate a sidecar-bearing segment,
            // breaking the rule. Only after the swap is the segment truly sealed.
            g.active = new_active;
            g.active_seq = next_seq;
        }

        // Best-effort sidecar write: data is already fsync'd (above), so the
        // sidecar never claims durability the data lacks; a write failure is
        // logged and swallowed (open falls back to replay).
        let meta_seg_path = sealed_path;
        let write_res = spawn_blocking(move || {
            crate::segment_meta::write_blocking(&meta_seg_path, sealed_max, sealed_pin)
        })
        .await
        .map_err(|e| DbError::Internal(format!("spawn_blocking join: {e}")))?;
        if let Err(e) = write_res {
            log::warn!(
                "SegmentSet seal: max_version sidecar write for seq {sealed_seq} failed \
                 ({e}); segment is intact, open will replay it (no fast path)"
            );
        }
        Ok(())
    }

    /// Replay every segment in seq order: all sealed (ascending) then the
    /// active segment last. Byte-identical to a single segment holding the
    /// same data in the same append order.
    ///
    /// Used for startup and live gap recovery. Sealed reads never create files;
    /// `NotFound` is tolerated for concurrent truncation, while `PermissionDenied`
    /// is retried a bounded number of times and then returned as an error.
    pub async fn replay(&self) -> DbResult<Vec<WalEntryV2>> {
        // Snapshot sealed metadata + active Arc under the lock; do I/O after.
        let (mut sealed, active) = {
            let g = self.inner.lock().expect("SegmentSet inner mutex poisoned");
            (g.sealed.clone(), Arc::clone(&g.active))
        };
        // Replay sealed strictly by seq (chronological). They are pushed in
        // ascending order at seal time, but sorting makes the seq-order
        // invariant explicit and independent of insertion order.
        sealed.sort_unstable_by_key(|m| m.seq);
        let mut out = Vec::new();
        for meta in &sealed {
            out.extend(WalSegment::replay_path(meta.path.clone(), true, false, false).await?);
        }
        out.extend(active.replay_at_startup().await?);
        Ok(out)
    }

    /// Delete every unpinned sealed segment wholly durable in history — i.e. whose
    /// `max_version` is in `(0, durable_version]`. Returns the count deleted.
    ///
    /// The active segment is NEVER touched (I3). A sealed segment with
    /// `max_version == 0` or any zero-version record pins the segment and it
    /// is never deleted by version. Each `remove_file` is an atomic FS op;
    /// replay of the survivors after truncation is idempotent (I6).
    /// Failed removals remain retryable; an I/O error reports the number
    /// actually deleted and the number of failures. Cancellation after dispatch
    /// detaches the blocking cleanup, which still reconciles its results.
    pub async fn truncate_below(&self, durable_version: u64) -> DbResult<usize> {
        // The blocking job owns the gate, even if its caller is cancelled.
        let gate = Arc::clone(&self.truncate_gate).lock_owned().await;
        let inner = Arc::clone(&self.inner);
        // The mirror decrements inside the job, under the same lock as the
        // sealed-list removal (both the removed and the NotFound paths leave).
        let segment_count = Arc::clone(&self.segment_count);
        #[cfg(test)]
        let fail_seq = self.truncate_fail_seq.lock().unwrap().take();
        #[cfg(test)]
        let pause = self.truncate_pause.lock().unwrap().take();
        spawn_blocking(move || -> DbResult<usize> {
            let _gate = gate;
            let candidates = {
                let g = inner.lock().expect("SegmentSet inner mutex poisoned");
                g.sealed
                    .iter()
                    .filter(|m| {
                        !m.pinned && m.max_version > 0 && m.max_version <= durable_version
                    })
                    .cloned()
                    .collect::<Vec<_>>()
            };
            #[cfg(test)]
            if let Some((entered, resume)) = pause {
                let _ = entered.send(());
                let _ = resume.recv();
            }
            let mut deleted = 0usize;
            let mut first_error = None;
            let mut failures = 0usize;
            for meta in candidates {
                #[cfg(test)]
                let result = if fail_seq == Some(meta.seq) {
                    Err(std::io::Error::new(
                        std::io::ErrorKind::PermissionDenied,
                        "injected WAL unlink failure",
                    ))
                } else {
                    remove_file(&meta.path)
                };
                #[cfg(not(test))]
                let result = remove_file(&meta.path);
                match result {
                    Ok(()) => deleted += 1,
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => {
                        failures += 1;
                        log::error!("SegmentSet truncate remove {:?}: {}", meta.path, e);
                        if first_error.is_none() {
                            first_error = Some((meta.path, e));
                        }
                        continue;
                    }
                }
                {
                    let mut g = inner.lock().expect("SegmentSet inner mutex poisoned");
                    g.sealed.retain(|m| m.seq != meta.seq);
                    segment_count.fetch_sub(1, Ordering::Relaxed);
                }
                crate::segment_meta::remove_blocking(
                    &meta.path,
                    "harmless leaked sidecar (its .wal is gone too, so open ignores it)",
                );
                // Preserve the F6c crash seam after a resolved unlink.
                #[cfg(debug_assertions)]
                if std::env::var("SHAMIR_TEST_CRASH_AFTER").as_deref() == Ok("wal_mid_delete") {
                    std::process::abort();
                }
            }
            if let Some((path, error)) = first_error {
                log::error!(
                    "SegmentSet truncate: {deleted} deleted, {failures} failed; first failure {path:?}: {error}"
                );
                return Err(wal_io_error(
                    format!(
                        "SegmentSet truncate: {deleted} deleted, {failures} failed; first failure {path:?}"
                    ),
                    error,
                ));
            }
            Ok(deleted)
        })
        .await
        .map_err(|e| DbError::Internal(format!("spawn_blocking join: {e}")))?
    }

    /// Cheap probe (under the lock, no I/O): is any sealed segment
    /// reclaimable at `durable_version` — i.e. has `0 < max_version <=
    /// durable_version`? The drainer gates the (more expensive)
    /// history-flush + `truncate_below` on this so truncation work fires
    /// only on a segment boundary, never per-commit (I2). The active
    /// segment is never considered (I3); a zero maximum or zero-version record
    /// pins the segment and makes it unreclaimable (I5).
    pub fn has_truncatable(&self, durable_version: u64) -> bool {
        let g = self.inner.lock().expect("SegmentSet inner mutex poisoned");
        g.sealed
            .iter()
            .any(|m| !m.pinned && m.max_version > 0 && m.max_version <= durable_version)
    }

    // ---- Sync scrape accessors (design §2.4; called by `WalSink` only,
    // never on the append path) ----

    /// Retained segment FILES (sealed + the one active segment): relaxed
    /// load of the mirror.
    pub(crate) fn segment_count(&self) -> u64 {
        self.segment_count.load(Ordering::Relaxed) as u64
    }

    /// Cumulative WAL payload bytes appended: relaxed load of the mirror.
    pub(crate) fn bytes_written_total(&self) -> u64 {
        self.bytes_written_total.load(Ordering::Relaxed)
    }

    /// Approximate on-disk WAL size: the active segment's length plus the
    /// recorded on-disk length of every sealed segment, under the same
    /// sanctioned lock `has_truncatable` takes (scrape-only cold path).
    pub(crate) fn approx_size_bytes(&self) -> u64 {
        let g = self.inner.lock().expect("SegmentSet inner mutex poisoned");
        g.sealed.iter().map(|m| m.bytes).sum::<u64>() + g.active.approx_len_bytes()
    }

    /// Truncation lag in versions: how far `durable_version` has advanced
    /// past the OLDEST reclaimable-but-retained sealed segment boundary;
    /// 0 when none is truncatable. Same lock and eligibility predicate as
    /// `has_truncatable` (`!pinned && 0 < max_version <= durable_version`).
    pub(crate) fn truncation_lag_versions(&self, durable_version: u64) -> u64 {
        let g = self.inner.lock().expect("SegmentSet inner mutex poisoned");
        g.sealed
            .iter()
            .filter(|m| !m.pinned && m.max_version > 0 && m.max_version <= durable_version)
            .map(|m| m.max_version)
            .min()
            .map_or(0, |oldest| durable_version.saturating_sub(oldest))
    }

    pub(super) async fn lock_truncation(&self) -> tokio::sync::OwnedMutexGuard<()> {
        Arc::clone(&self.truncate_gate).lock_owned().await
    }

    pub async fn sealed_retained_bytes(&self) -> DbResult<u64> {
        super::sealed_retained_bytes::measure(self).await
    }

    /// Test-only (task #531): clone the active segment's `Arc<WalSegment>` so
    /// a test can drive the poison path — e.g. `mark_poisoned()` it, then call
    /// `append_batch` to trigger `rotate_after_poison`. Not compiled into a
    /// production build.
    #[cfg(test)]
    pub(crate) fn active_segment_for_test(&self) -> Arc<WalSegment> {
        let g = self.inner.lock().expect("SegmentSet inner mutex poisoned");
        Arc::clone(&g.active)
    }

    /// Read every discovered segment in sequence and frame order, ignoring sidecars.
    ///
    /// The caller must keep the directory offline and stable during the read.
    /// A missing directory, no segments, or a missing discovered file is an error.
    /// This does not open an append handle or create, repair, or seal anything.
    /// Cancel-safe: yes; the blocking task may finish after cancellation, but only reads.
    pub async fn read_entries_strict(dir: &Path) -> Result<Vec<WalEntryV2>, WalStrictReadError> {
        let dir = dir.to_path_buf();
        spawn_blocking(move || {
            let seqs = discover_segment_seqs(&dir)?;
            if seqs.is_empty() {
                return Err(DbError::NotFound(format!("No WAL segments in {dir:?}")).into());
            }

            let mut entries = Vec::new();
            for seq in seqs {
                let path = dir.join(seg_file_name(seq));
                let file = File::open(&path)
                    .map_err(|e| wal_io_error(format!("Strict WAL read open {path:?}"), e))?;
                let meta = file
                    .metadata()
                    .map_err(|e| wal_io_error(format!("Strict WAL read metadata {path:?}"), e))?;
                if !meta.is_file() {
                    return Err(DbError::Storage(format!("Not a WAL file: {path:?}")).into());
                }
                let mut reader =
                    FrameReader::new(file, meta.len(), format!("Strict WAL read {path:?}"));
                loop {
                    match reader.next()? {
                        FrameRead::End => break,
                        FrameRead::Incomplete { offset, .. } => {
                            return Err(WalStrictReadError::IncompleteFrame { path, offset });
                        }
                        FrameRead::Frame {
                            offset,
                            payload,
                            crc_ok,
                            ..
                        } => {
                            if !crc_ok {
                                return Err(DbError::Storage(format!(
                                    "Strict WAL read {path:?}: CRC mismatch at byte offset {offset}"
                                ))
                                .into());
                            }
                            let entry = WalEntryV2::decode(&payload).map_err(|source| {
                                WalStrictReadError::Decode {
                                    path: path.clone(),
                                    offset,
                                    source,
                                }
                            })?;
                            entries.push(entry);
                        }
                    }
                }
            }
            Ok(entries)
        })
        .await
        .map_err(|e| DbError::Internal(format!("spawn_blocking join: {e}")))?
    }

    /// fsync the active segment (sealed segments are already fully on disk).
    pub async fn sync(&self) -> DbResult<()> {
        let _sync = self.sync_gate.lock().await;
        let active = {
            let g = self.inner.lock().expect("SegmentSet inner mutex poisoned");
            Arc::clone(&g.active)
        };
        self.sync_active(&active).await
    }

    pub(crate) async fn sync_active(&self, active: &WalSegment) -> DbResult<()> {
        #[cfg(test)]
        let _flight = self.sync_test.sync().await;
        active.sync().await
    }

    /// Highest `commit_version` across all segments (sealed + active) — for
    /// parity with [`WalSegment::max_committed`].
    ///
    /// Unlike a bare [`WalSegment::open`] handle (which starts at 0 on an
    /// existing non-empty segment, with the retention pin clear — see
    /// RV-WAL-48749F97), this value includes retained active frames restored
    /// at `open` time. Truncation eligibility is decided only by `SegmentSet`;
    /// never treat a handle-local `WalSegment::max_committed` as a durable
    /// cross-segment watermark.
    pub fn max_committed(&self) -> u64 {
        let g = self.inner.lock().expect("SegmentSet inner mutex poisoned");
        let sealed_max = g.sealed.iter().map(|m| m.max_version).max().unwrap_or(0);
        sealed_max.max(g.active.max_committed())
    }
}

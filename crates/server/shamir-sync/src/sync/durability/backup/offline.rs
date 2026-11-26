//! On-demand backup of the server's `data_dir` to a snapshot directory.
//!
//! v1: stop-and-copy. Operator stops the server, runs `shamir-server --config <ktav> backup
//! --to <dest>` which recursively copies every file under `data_dir` (fjall database
//! directories — each a directory tree of journal and LSM segment files — plus wire-table
//! registries, audit log, rotated audit files, and TLS PEMs). The destination is
//! `<dest>/<timestamp>/` so a single `--to` can collect daily snapshots. A `manifest.json`
//! (file list + sha256 + size per file) is written into the snapshot last, so `restore`
//! (`restore.rs`) has something durable to verify a snapshot against before trusting it.
//! Why stop-and-copy instead of an online backup: fjall exposes no live / online backup API on
//! `Database` (its public surface is `insert`/`get`/`range`/`persist`/`batch`; the closest
//! analogue, `Snapshot`, is just a seqno read snapshot, not a file-level backup). Copying the
//! database directory while the writer is paused (i.e. server stopped) is the simplest path.
//! Fjall is journal-based: every commit is appended to the journal as a *batch* (`Start → items
//! → End(xxh3 checksum)`), and the default recovery mode (`RecoveryMode::TolerateCorruptTail`)
//! discards a corrupt/torn tail batch and truncates the journal back to the last
//! fully-checksummed batch boundary. So a copy taken during a quiescent window between commits
//! recovers, on next open, to that last complete batch — a consistent pre-stop point. (A copy
//! that races an in-flight append merely loses the torn tail batch; it does not corrupt earlier
//! committed batches.) See `shamir-storage`'s `storage_fjall.rs` for how `flush()` maps to
//! `Database::persist(PersistMode::SyncAll)`.
//! Future enhancement (P2): an in-process / online snapshot triggered by an `admin` op, going
//! through the engine's own repo handles (not this offline CLI, which has no access to any live
//! handle). Not done here because it requires a wire-protocol change.

use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::path::{Component, Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use shamir_collections::new_fx_set_wc;

use super::online::manifest::{self as online_manifest, BackupKind};
use super::restore::fsync_dir;
use super::snapshot_lease;
use crate::format_stamp::{observe_stamp_for_backup, BackupStampObservation, StampError};

mod copy_gate;
mod plain_open;
mod source_lock;
#[cfg(test)]
mod tests;
mod timestamp;
mod v2_verify;
#[cfg(test)]
pub(super) use v2_verify::exact_spelling_mismatch;
/// The exact decoded manifest a successful verification already produced,
/// handed to the caller without a second parse of `manifest.json`.
/// `pub(in ...::backup)` — restore.rs is the intended consumer. Lives in the
/// v2_verify child module next to the verifiers that produce it.
pub(in crate::sync::durability::backup) use v2_verify::VerifiedManifest;

/// Size of the fixed, reused buffer streaming hash reads are chunked
/// through — chosen so a large fjall SST/journal file is hashed without
/// ever materializing the whole file in RAM at once.
const HASH_STREAM_BUFFER_SIZE: usize = 1024 * 1024;

/// Reserved lease-file name at the ROOT of a managed backup root (outside
/// every deletable `<timestamp>` snapshot child). Readers take a shared
/// advisory lock on it, retention takes a non-blocking exclusive one before
/// deleting a candidate. Defined here so the SERVER-facing re-export stays
/// inside an existing public file; the private `snapshot_lease` module
/// imports it inward.
pub const SNAPSHOT_LEASE_FILE_NAME: &str = ".shamir-snapshot.lock";

pub(crate) use copy_gate::{create_copy_output, stream_copy, CopyGateError, RestoreCopyLedger};
use plain_open::open_plain_file;
pub(crate) use plain_open::open_plain_snapshot_file;
pub use timestamp::{utc_timestamp, utc_timestamp_at};

/// Stream `path`'s full contents through a fixed-size buffer into a running SHA-256 hasher,
/// returning `(digest, size_bytes)`. Produces a byte-identical result to
/// `sha256(&fs::read(path)?)`, but never holds more than [`HASH_STREAM_BUFFER_SIZE`] bytes of file
/// content in memory at once — unlike `fs::read`, whose peak memory is proportional to the file
/// size. `cancel` is consulted once per chunk BEFORE that chunk is read; `true` aborts with
/// `Interrupted` rather than hashing a whole large file out.
fn hash_file_streaming(path: &Path, cancel: &dyn Fn() -> bool) -> std::io::Result<([u8; 32], u64)> {
    let mut file = open_plain_file(path)?;
    let mut buf = vec![0u8; HASH_STREAM_BUFFER_SIZE];
    let mut hasher = Sha256::new();
    let mut size_bytes = 0u64;
    loop {
        if cancel() {
            return Err(io::Error::new(io::ErrorKind::Interrupted, HashCancelled));
        }
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        size_bytes += n as u64;
    }
    Ok((hasher.finalize().into(), size_bytes))
}

/// Name of the manifest file written into every snapshot directory.
pub const MANIFEST_FILE_NAME: &str = "manifest.json";

/// The retention cancel hook aborted a streaming hash. A typed marker so the
/// caller can tell a deliberate cancellation from an OS `EINTR`
/// (`io::ErrorKind::Interrupted` is used for both on the wire).
#[derive(Debug)]
pub struct HashCancelled;

impl core::fmt::Display for HashCancelled {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("hashing cancelled by the retention budget")
    }
}

impl std::error::Error for HashCancelled {}

#[derive(Debug, Error)]
pub enum BackupError {
    #[error("backup io: {0}")]
    Io(#[from] std::io::Error),
    #[error("source dir does not exist: {0}")]
    SourceMissing(PathBuf),
    #[error("source is not a directory: {0}")]
    SourceNotDir(PathBuf),
    /// `verify_manifest`: `manifest.json` is absent from the snapshot dir.
    #[error("manifest missing: {0}")]
    ManifestMissing(PathBuf),
    /// `verify_manifest`: `manifest.json` exists but failed to parse.
    #[error("manifest invalid: {0}")]
    ManifestInvalid(String),
    /// `verify_manifest`: a file's actual sha256/size does not match what
    /// the manifest recorded — the snapshot must never be trusted/restored.
    #[error(
        "checksum mismatch for {path}: manifest says sha256={expected_sha256} \
         size={expected_size_bytes}, actual sha256={actual_sha256} size={actual_size_bytes}"
    )]
    ChecksumMismatch {
        path: PathBuf,
        expected_sha256: String,
        expected_size_bytes: u64,
        actual_sha256: String,
        actual_size_bytes: u64,
    },
    /// `verify_manifest`: a file is physically present under the snapshot
    /// directory but is not listed in the manifest — could indicate a
    /// tampered or hand-modified snapshot.
    #[error("file present on disk but not listed in manifest: {0}")]
    UnmanifestedFile(PathBuf),
    /// `verify_manifest`: `manifest.format_version` does not match the
    /// format this build understands — refuse rather than interpret
    /// entries under assumptions that may not hold for a different format.
    #[error("unsupported manifest format_version: found {found}, expected {expected}")]
    UnsupportedManifestFormatVersion { found: u32, expected: u32 },
    /// `verify_manifest`: an entry's `path` is absolute or contains a `..`
    /// (`ParentDir`) component — both can escape `snapshot_dir` when joined,
    /// so a tampered/malicious manifest could make `verify_manifest` read a
    /// file outside the snapshot directory.
    #[error("unsafe manifest entry path (absolute or contains '..'): {0}")]
    UnsafeManifestPath(String),
    /// `verify_exported_manifest`: the snapshot is a MANAGED scheduled
    /// child (a lease marker already exists in its canonical parent), so
    /// the no-lock exported-snapshot verifier refuses it instead of taking
    /// a lease it promises not to take. Managed snapshots verify through
    /// [`verify_manifest`], which holds the shared read lease.
    #[error(
        "snapshot {0} is a managed scheduled child (lease marker present); \
         verify_exported_manifest refuses to verify it without a lease"
    )]
    ManagedSnapshotNotExported(PathBuf),
    /// `verify_manifest`: a manifest lists a path inside the private cache subtree.
    #[error("snapshot manifest must not contain cache paths: {0}")]
    CacheManifestPath(String),
    /// `verify_manifest`: the same `path` appears more than once in
    /// `manifest.files` — a hand-crafted/tampered manifest could otherwise
    /// have one entry's checksum shadow another's expectations.
    #[error("duplicate manifest entry: {0}")]
    DuplicateManifestEntry(String),
    /// `verify_manifest`: WAL segment sequence numbers are not contiguous
    /// from zero for a repo (a copied history hole — never acceptable).
    #[error("wal sequence gap for repo {repo}: expected {expected}, found {found}")]
    WalSeqGap {
        repo: String,
        expected: u64,
        found: u64,
    },
    /// `verify_manifest`: an online manifest is missing its consistency or
    /// wal section.
    #[error("online manifest missing required section: {0}")]
    MissingOnlineSection(&'static str),
    /// `backup()`: the destination (`to`, or its `<timestamp>` subdir) is
    /// inside (or equal to) the source `data_dir` — backing up would
    /// recursively copy the backup's own destination into itself.
    #[error(
        "backup destination is inside (or equal to) the source data_dir: \
         from={from} to={to}"
    )]
    DestinationInsideSource { from: PathBuf, to: PathBuf },
    /// `backup()`: a lock artifact of the source `data_dir` is held by a
    /// running server — an offline stop-and-copy would race live writers
    /// (brief 47-r2a-3b2 §5, Q5). Stop the server, or use the online
    /// `StartBackup` admin operation instead.
    #[error(
        "source data_dir is held by a running server: {0} — stop it, or use \
         the online `StartBackup` admin operation"
    )]
    SourceRunning(PathBuf),
    /// `verify_manifest` (v2): the manifest's `format_epoch` and the
    /// snapshot's copied `format.stamp` disagree — the pair is treated as
    /// corrupt (design `online-backup.md` §5 "Stamp refusal").
    #[error(
        "manifest format_epoch {manifest_epoch} does not match the snapshot stamp epoch {stamp_epoch}"
    )]
    ManifestStampEpochMismatch {
        manifest_epoch: u64,
        stamp_epoch: u64,
    },
    /// `verify_manifest` (v2): the manifest omits `format_epoch` while the
    /// snapshot carries a VALID stamp — the producer fills both fields, so
    /// the absence means a tampered or foreign manifest.
    #[error("manifest omits format_epoch while the snapshot stamp has epoch {stamp_epoch}")]
    MissingManifestEpoch { stamp_epoch: u64 },
    /// `verify_manifest` (v2): the snapshot's copied `format.stamp` is
    /// corrupt or uses a newer layout — its epoch cannot be determined, so
    /// the manifest/stamp cross-check is impossible and the snapshot is
    /// refused (fail closed, independent of the restore-side gate).
    #[error("snapshot format stamp is invalid")]
    InvalidSnapshotStamp(#[source] StampError),
    /// `verify_manifest` (v1 and v2): a manifest-controlled path is refused by a
    /// lexical or metadata check before the entry is opened for content reads:
    /// an empty or `.` component, a symlink / non-directory component on the
    /// fixed snapshot, and (v2 on every host, v1 on Windows) a drive / device
    /// prefix, `:` or backslash. (`..` is `UnsafeManifestPath`.)
    #[error("manifest-controlled path is not confined to the snapshot directory: {0}")]
    UnconfinedManifestPath(String),
    /// `verify_manifest` (v1 and v2): a manifest-controlled path names something
    /// that is not a regular file (device, FIFO, socket).
    #[error("manifest path is not a regular file: {0}")]
    ManifestPathNotRegularFile(String),
    /// `verify_manifest` (v2): a WAL segment file or sidecar is not inside
    /// the wal directory its manifest entry names.
    #[error("non-canonical wal name: found {found}, expected {expected}")]
    NonCanonicalWalName { found: String, expected: String },
    /// `verify_manifest` (v2): a direct numeric `.wal` file sits under a
    /// declared wal directory but is absent from the `wal` listing — replay
    /// would read or diagnose it as a candidate segment while the manifest
    /// does not account for it. Non-`.wal` direct entries are not this
    /// error: they fall to the global file-set check.
    #[error("undeclared direct wal file {file} under manifest wal directory {dir}")]
    UndeclaredWalFile { dir: String, file: String },
    /// `verify_manifest` (v2): a WAL segment file or sidecar named in the
    /// `wal` section is absent from the manifest `files` array — the two
    /// listings must agree.
    #[error("wal file is absent from the manifest file list: {0}")]
    WalFileUnmanifested(String),
    /// `verify_manifest` (v2): a WAL segment file or sidecar is not inside
    /// the wal directory its manifest entry names.
    #[error("wal file {path} is outside its manifest wal directory {dir}")]
    WalFileOutsideDir { path: String, dir: String },
    /// `verify_manifest` (v2): the same wal directory or repo appears more
    /// than once in the `wal` section.
    #[error("duplicate wal directory in manifest: repo {repo} dir {dir}")]
    DuplicateWalDir { repo: String, dir: String },
    /// `verify_manifest` (v2): the `consistency.per_repo` map captures a
    /// repository the `wal` section has no directory listing for — replay
    /// could not run for it, so the listings must be a bijection.
    #[error("online manifest wal section has no directory listing for captured repo {repo}")]
    WalListingMissingForRepo { repo: String },
    /// `verify_manifest` (v2): the `wal` section lists a repository that is
    /// not captured in `consistency.per_repo` — the listings must be a
    /// bijection.
    #[error(
        "online manifest wal section lists repo {repo}, which is absent from \
         the consistency section"
    )]
    WalListingForUncapturedRepo { repo: String },
    /// `verify_manifest` (v2): a wal directory listing contains zero
    /// segments — even a `V*` of zero produces an empty segment file, so an
    /// empty listing is a truncated capture.
    #[error("wal directory listing for repo {repo} ({dir}) contains no segments")]
    WalListingEmpty { repo: String, dir: String },
    /// `verify_manifest` (v2): a listed wal directory is not the canonical
    /// `<store_dir>.shamirwal` sibling of that repo's `consistency.per_repo`
    /// `store_dir` — replay derives the WAL directory exactly this way and
    /// would refuse later; verify refuses typed up front.
    #[error("wal directory for repo {repo} is {found}, expected {expected}")]
    WalListingPathMismatch {
        repo: String,
        found: String,
        expected: String,
    },
    /// `verify_manifest` (v2): a per-repo `store_dir` in the consistency
    /// section is absolute or contains `..` — restore would join it under
    /// the snapshot, so refuse it at verify time.
    #[error("unsafe consistency store_dir for repo {repo}: {path}")]
    UnsafeStoreDir { repo: String, path: String },
}

/// Outcome of a successful backup.
#[derive(Debug)]
pub struct BackupReport {
    /// The actual destination directory created (`<to>/<timestamp>`).
    pub dest_dir: PathBuf,
    /// Total bytes copied.
    pub bytes_copied: u64,
    /// Number of files copied.
    pub files_copied: u64,
    /// Path to the `manifest.json` written into `dest_dir`.
    pub manifest_path: PathBuf,
    /// Read-only observation of the source stamp, taken before the copy
    /// (design 3.5). Backup never refuses and never stamps.
    pub format_stamp: BackupStampObservation,
}

/// One entry in `manifest.json`'s `files` array.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestFileEntry {
    /// Path relative to the snapshot directory, forward-slash separated
    /// (normalized even on Windows so a manifest written on one platform
    /// verifies correctly on another).
    pub path: String,
    /// Lowercase hex-encoded SHA-256 of the file's full contents.
    pub sha256: String,
    /// File size in bytes.
    pub size_bytes: u64,
}

/// On-disk shape of `manifest.json`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub format_version: u32,
    pub created_at_unix_ns: u128,
    pub files: Vec<ManifestFileEntry>,
}

/// Current manifest format version.
pub const MANIFEST_FORMAT_VERSION: u32 = 1;

/// Outcome of a successful [`verify_manifest`] call.
#[derive(Debug)]
pub struct ManifestVerifyReport {
    /// Number of files checked (equals `manifest.files.len()`).
    pub files_checked: u64,
    /// Sum of `size_bytes` across every checked file.
    pub total_bytes: u64,
    /// Manifest kind: [`BackupKind::Offline`] for every v1 manifest and for
    /// a v2 manifest with `kind = "offline"`.
    pub kind: BackupKind,
    /// v2 only: the manifest's recorded format epoch (`None` for v1 and
    /// for v2 manifests that omit the field — the current online producer
    /// still records `None`).
    pub format_epoch: Option<u64>,
    /// v2 only: the capturing binary's version, when recorded.
    pub binary_version: Option<String>,
    /// v2 only: the captured consistency section (per-repo `wal_position`
    /// / `store_dir` / `schema_version`, system-store version).
    pub consistency: Option<online_manifest::ConsistencySection>,
}

/// Recursively copy `from` → `to/<timestamp>/`. The timestamp is
/// `YYYYMMDD_HHMMSS` in UTC (sortable, human-readable).
///
/// `to` is created if missing. The timestamped subdirectory MUST NOT
/// already exist (we refuse to overwrite — operator should pick a new
/// `--to`).
///
/// After the recursive copy completes, a `manifest.json` (see [`Manifest`])
/// is written into `dest_dir`, hashing every JUST-COPIED file with SHA-256.
/// This is an extra full read of every backed-up file — an acceptable,
/// honest cost for a correctness-critical manifest. `manifest.json` itself
/// is not included in its own `files` list (it is written after the list is
/// computed).
///
/// F-19 (#812): after `write_manifest` returns, `dest_dir` itself is fsync'd
/// once via [`fsync_dir`] — a directory fsync flushes every entry added to it,
/// and `copy_dir_recursive` fsyncs each nested directory it writes into.
/// Log-only on failure, matching `fsync_dir`'s contract.
pub fn backup(from: &Path, to: &Path) -> Result<BackupReport, BackupError> {
    if !from.exists() {
        return Err(BackupError::SourceMissing(from.to_path_buf()));
    }
    if !from.is_dir() {
        return Err(BackupError::SourceNotDir(from.to_path_buf()));
    }
    reject_destination_inside_source(from, to)?;

    // Refuse a live source (brief 47-r2a-3b2 §5, Q5) BEFORE the stamp
    // observation, the destination creation or any copy. The shared locks
    // are retained for the whole copy: a server starting mid-backup blocks
    // on its exclusive acquisition (the stopped-source restart race), and
    // the lock artifacts themselves still copy as plain bytes.
    let _source_locks = source_lock::lock_backup_source(from)?;

    // Observe the stamp before the copy (design 3.5). Read-only; the byte
    // copy below brings the stamp itself along.
    let format_stamp = observe_stamp_for_backup(from);

    let stamp = utc_timestamp();
    let dest_dir = to.join(stamp);
    if dest_dir.exists() {
        return Err(BackupError::Io(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            format!("destination already exists: {}", dest_dir.display()),
        )));
    }
    fs::create_dir_all(&dest_dir)?;

    let mut bytes = 0u64;
    let mut files = 0u64;
    // No ledger: the walk WRITES the manifest afterwards, so there is
    // nothing to compare against yet and no unlisted-file refusal may fire.
    // The gate can therefore only produce `Io` here.
    copy_dir_recursive(from, &dest_dir, &mut bytes, &mut files, true, true, None).map_err(
        |error| match error {
            CopyGateError::Io(io) => BackupError::Io(io),
            // Unreachable while `ledger` is `None` (the gate is compiled
            // out on that path); refuse fail-closed rather than guessing.
            other => BackupError::ManifestInvalid(format!(
                "internal error: restore-only copy gate fired during backup: {other:?}"
            )),
        },
    )?;

    let manifest_path = write_manifest(&dest_dir)?;

    // F-19 (#812): fsync dest_dir itself so every directory entry added to
    // it (copied files, manifest.json, any subdirectories) is durable — see
    // this function's doc for why a single call here suffices.
    fsync_dir(&dest_dir);

    Ok(BackupReport {
        dest_dir,
        bytes_copied: bytes,
        files_copied: files,
        manifest_path,
        format_stamp,
    })
}

/// Liveness probe for the online restore path: report whether `root` is
/// held by a running server. The verdict is the typed refusal (or `Ok`);
/// the acquired shared locks are dropped immediately — the probe must not
/// keep artifacts locked, it must only classify the source.
pub(super) fn probe_source_stopped(root: &Path) -> Result<(), BackupError> {
    source_lock::lock_backup_source(root).map(drop)
}

/// Reject `to` (the backup destination root) when it is inside, or equal
/// to, `from` (the source `data_dir`) — otherwise a
/// `backup --to <data_dir>/somewhere` (or `backup --to <data_dir>`) would
/// recursively back up the destination into itself, corrupting the
/// operation.
///
/// `to`'s own `<timestamp>` subdirectory does not exist yet at call time
/// (created later in [`backup`]), so this canonicalizes `to`'s nearest
/// EXISTING ancestor (walking up via `Path::parent` until something
/// resolves) rather than `to` itself, and compares that against `from`'s
/// canonical path.
fn reject_destination_inside_source(from: &Path, to: &Path) -> Result<(), BackupError> {
    let from_canonical = from.canonicalize().map_err(|e| {
        BackupError::Io(std::io::Error::new(
            e.kind(),
            format!("canonicalize source {}: {e}", from.display()),
        ))
    })?;

    let to_canonical = canonicalize_nearest_existing_ancestor(to)?;

    if to_canonical.starts_with(&from_canonical) {
        return Err(BackupError::DestinationInsideSource {
            from: from_canonical,
            to: to.to_path_buf(),
        });
    }
    Ok(())
}

/// Canonicalize `path` if it exists, else walk up `Path::parent` until an
/// existing ancestor is found and canonicalize that instead. `path` itself
/// is appended (non-canonicalized) onto the canonicalized ancestor so the
/// result still reflects `path`'s full intended location relative to that
/// ancestor.
fn canonicalize_nearest_existing_ancestor(path: &Path) -> Result<PathBuf, BackupError> {
    let mut suffix: Vec<std::ffi::OsString> = Vec::new();
    let mut current = path;
    loop {
        if current.exists() {
            let mut canonical = current.canonicalize().map_err(|e| {
                BackupError::Io(std::io::Error::new(
                    e.kind(),
                    format!("canonicalize {}: {e}", current.display()),
                ))
            })?;
            for component in suffix.iter().rev() {
                canonical.push(component);
            }
            return Ok(canonical);
        }
        match current.parent() {
            Some(parent) => {
                if let Some(name) = current.file_name() {
                    suffix.push(name.to_os_string());
                }
                current = parent;
            }
            None => {
                // Reached the root without finding an existing ancestor —
                // treat the (non-canonicalized) original path as-is.
                return Ok(path.to_path_buf());
            }
        }
    }
}

/// Walk `dest_dir` (a just-written snapshot), hash every file, and write
/// `dest_dir/manifest.json`. Returns the manifest's path.
fn write_manifest(dest_dir: &Path) -> Result<PathBuf, BackupError> {
    let mut entries = Vec::new();
    collect_manifest_entries(dest_dir, dest_dir, &mut entries)?;

    let manifest = Manifest {
        format_version: MANIFEST_FORMAT_VERSION,
        created_at_unix_ns: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0),
        files: entries,
    };

    let manifest_path = dest_dir.join(MANIFEST_FILE_NAME);
    let json = serde_json::to_vec_pretty(&manifest)
        .map_err(|e| BackupError::ManifestInvalid(format!("encode: {e}")))?;
    // F-12 (#802): write + fsync so the manifest itself is durable before
    // `backup()` returns success — a bare `fs::write` leaves the bytes in the
    // OS page cache. Matches `segment_meta.rs`'s file-sync step (propagate —
    // the manifest's content correctness IS the point). This file is NOT
    // renamed into place (it's the final artifact, written AFTER the files it
    // lists), so there is no accompanying directory-fsync here (unlike
    // restore's swap in `restore.rs`, which renames and fsyncs the parent).
    {
        let mut f = fs::File::create(&manifest_path)?;
        f.write_all(&json)?;
        f.sync_all()?;
    }
    Ok(manifest_path)
}

/// Recursively walk `dir` (rooted at `root`), hashing every regular file and
/// pushing a [`ManifestFileEntry`] with a `root`-relative, forward-slash
/// path. Skips `manifest.json` itself at the root (it does not exist yet at
/// call time, but the check is defensive against a future caller re-running
/// this over an already-manifested directory).
fn collect_manifest_entries(
    root: &Path,
    dir: &Path,
    entries: &mut Vec<ManifestFileEntry>,
) -> Result<(), BackupError> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        let ft = entry.file_type()?;
        if ft.is_dir() {
            collect_manifest_entries(root, &path, entries)?;
        } else if ft.is_file() {
            let rel = relative_slash_path(root, &path);
            if rel == MANIFEST_FILE_NAME {
                continue;
            }
            let (digest, size_bytes) = hash_file_streaming(&path, &|| false)?;
            entries.push(ManifestFileEntry {
                path: rel,
                sha256: hex::encode(digest),
                size_bytes,
            });
        }
        // Symlinks / sockets / fifos: `copy_dir_recursive` already skipped
        // these with a warning; nothing to hash here either.
    }
    Ok(())
}

/// `path` relative to `root`, forward-slash separated regardless of
/// platform (so a manifest written on Windows verifies correctly on Linux
/// and vice versa).
fn relative_slash_path(root: &Path, path: &Path) -> String {
    let rel = path.strip_prefix(root).unwrap_or(path);
    rel.components()
        .map(|c| c.as_os_str().to_string_lossy().into_owned())
        .collect::<Vec<_>>()
        .join("/")
}

/// Verify `snapshot_dir` against its `manifest.json`, holding a shared read lease on the managed
/// root (when it is one) for the duration so retention cannot delete mid-verify. Snapshot paths
/// outside a managed root verify lock-free. The online backup writer self-verifies through the
/// module-private `verify_writer_snapshot` instead: it must not lease the snapshot it is still
/// publishing, and the old thread-scoped bypass that unleased EVERY direct child of the root on the
/// writer's thread is gone — an older sibling verified on that thread stays leased. The single
/// canonical-source decision happens at the start (`snapshot_lease::read_snapshot`) and every
/// subsequent read consumes only the resolved canonical path. A snapshot that is ABSENT at that
/// decision refuses with `BackupError::ManifestMissing` — a child that appears afterwards is never
/// read unleased by opening the original pathname. Readers never create or write the lease marker.
pub fn verify_manifest(snapshot_dir: &Path) -> Result<ManifestVerifyReport, BackupError> {
    verify_manifest_with_lookup(snapshot_dir, |path| fs::canonicalize(path))
}

/// Private test seam behind [`verify_manifest`]: the initial canonicalizing
/// lookup goes through `lookup`, so a test can make the snapshot
/// deterministically absent at the decision point while creating the
/// complete snapshot INSIDE the seam — before any manifest open. Production
/// passes the real `fs::canonicalize`.
pub(super) fn verify_manifest_with_lookup(
    snapshot_dir: &Path,
    lookup: impl FnOnce(&Path) -> io::Result<PathBuf>,
) -> Result<ManifestVerifyReport, BackupError> {
    match snapshot_lease::read_snapshot_with_lookup(snapshot_dir, lookup)
        .map_err(BackupError::Io)?
    {
        snapshot_lease::SourceSnapshot::Missing => Err(BackupError::ManifestMissing(
            snapshot_dir.join(MANIFEST_FILE_NAME),
        )),
        snapshot_lease::SourceSnapshot::Unmanaged { canonical } => {
            verify_manifest_impl(&canonical).map(|(report, _, _)| report)
        }
        snapshot_lease::SourceSnapshot::Managed { canonical, lease } => {
            let _source_lease = lease;
            verify_manifest_impl(&canonical).map(|(report, _, _)| report)
        }
    }
}

/// Verify an EXPORTED snapshot WITHOUT taking any lease and WITHOUT
/// creating or writing anything: the single canonical-source decision
/// classifies the path once, then a MANAGED child (lease marker already
/// present in its canonical parent) is refused as
/// [`BackupError::ManagedSnapshotNotExported`] rather than locked, a
/// missing path refuses as [`BackupError::ManifestMissing`], and only a
/// genuine unmanaged export (timestamped child without a marker, or a
/// non-timestamped directory) verifies — against its resolved canonical
/// path. For the server's no-lock `--plan --from-backup` gate; managed
/// snapshots verify through [`verify_manifest`], which holds the lease.
pub fn verify_exported_manifest(snapshot_dir: &Path) -> Result<ManifestVerifyReport, BackupError> {
    match snapshot_lease::resolve_source(snapshot_dir).map_err(BackupError::Io)? {
        snapshot_lease::Resolution::Missing => Err(BackupError::ManifestMissing(
            snapshot_dir.join(MANIFEST_FILE_NAME),
        )),
        snapshot_lease::Resolution::Managed(canonical) => {
            Err(BackupError::ManagedSnapshotNotExported(canonical))
        }
        snapshot_lease::Resolution::Unmanaged(canonical) => {
            verify_manifest_impl(&canonical).map(|(report, _, _)| report)
        }
    }
}

/// Internal full verify for restore: also returns the exact decoded
/// manifest so restore need not re-parse `manifest.json`. `pub(super)`.
/// Takes NO read lease: the caller holds the shared lease through the last
/// source read (a second handle would be a distinct lease; see history).
/// Verify-only callers should use [`verify_manifest`].
pub(super) fn verify_manifest_for_restore(
    snapshot_dir: &Path,
) -> Result<(ManifestVerifyReport, VerifiedManifest, String), BackupError> {
    verify_manifest_impl(snapshot_dir)
}

/// Retention preflight without cancellation (source-compatible entry point):
/// every hash runs to completion.
pub fn verify_retention_candidate(dir: &Path, raw: &[u8]) -> Result<(), BackupError> {
    verify_retention_candidate_cancellable(dir, raw, &|| false)
}

/// Retention preflight with the retention pass's cancellation probe threaded
/// into the per-chunk hash loop. Every other caller uses the two-argument
/// form above.
/// Run the v2 verifier core over a candidate snapshot WITHOUT taking the
/// blocking shared read lease (retention runs on a native scheduler worker and must never block)
/// and WITHOUT the v1 fallback: scheduled retention only manages v2 online copies, and its
/// classification refuses anything else before this call. `raw` is the candidate's `manifest.json`
/// exactly as the caller already read and size-bounded it — the verifier verifies against those
/// bytes and never re-reads the file. THE CALLER'S MANIFEST SIZE CAP IS A PRECONDITION and is not
/// re-checked here. An ONLINE-kind match is required FIRST, on the same bytes and BEFORE any
/// payload I/O: a v2 offline manifest refuses without walking or hashing the tree; the original v2
/// format-version check and error are kept. `cancel` is consulted once per chunk, so a shutdown
/// request interrupts a large payload hash instead of waiting for it to finish. Not used by any
/// reader path.
pub fn verify_retention_candidate_cancellable(
    dir: &Path,
    raw: &[u8],
    cancel: &dyn Fn() -> bool,
) -> Result<(), BackupError> {
    // Kind FIRST: decode and classify the bounded bytes without touching the
    // tree (`decode_manifest_v2` keeps the v2 format-version check; no v1 fallback).
    let decoded = v2_verify::decode_manifest_v2(raw)?;
    // A v2 manifest whose `kind` is offline is not a retention candidate:
    // refused BEFORE the tree walk, hash or any other payload I/O.
    if !matches!(decoded.kind, BackupKind::Online) {
        return Err(BackupError::ManifestInvalid(
            "retention candidate manifest kind is not online".to_owned(),
        ));
    }
    // Online-kind only: the payload half verifies the ALREADY-decoded manifest — one parse.
    v2_verify::verify_decoded_manifest_v2(dir, decoded, cancel)?;
    Ok(())
}

/// Crate-private unleased verify of the EXACT snapshot the online writer just published (the
/// protocol's self-verification of the child it wrote). The writer cannot lease the child it is
/// still writing; this entry takes that one path, so the self-check never waits behind
/// retention's exclusive lease and can never unlease an older sibling — any other verify stays
/// on the leased [`verify_manifest`]. Not exported past the backup module: only
/// `run_online_backup` may call it. Unit return — the protocol only needs the refusal, never
/// the report.
pub(super) fn verify_writer_snapshot(snapshot_dir: &Path) -> Result<(), BackupError> {
    verify_manifest_impl(snapshot_dir).map(drop)
}

/// Provision the lease marker ([`SNAPSHOT_LEASE_FILE_NAME`]) at the root of
/// a managed backup root, owner-private, without locking it. Idempotent;
/// called when scheduled retention is enabled. Precondition: `root` is a
/// private managed backup root the caller has already validated
/// (`open_or_create` re-checks only the leaf).
pub fn provision_snapshot_lease(root: &Path) -> io::Result<()> {
    snapshot_lease::provision(root)
}

/// Non-blocking EXCLUSIVE delete lease. `Ok(Some(handle))` means retention
/// may delete; `Ok(None)` means a read or delete lease is held — skip this
/// pass. Dropping the handle releases the lease. Never blocks, never holds
/// any registry permit.
pub fn try_snapshot_delete_lease(root: &Path) -> io::Result<Option<File>> {
    snapshot_lease::try_exclusive(root)
}

/// Shared implementation behind the entry points. Dispatches on the cheap
/// format-version peek: v2 snapshots route to the child module's
/// [`v2_verify::verify_manifest_v2`]; v1 adds only per-entry confinement.
/// Lease policy lives entirely in the callers ([`verify_manifest`] leases;
/// [`verify_manifest_for_restore`], [`verify_writer_snapshot`] and
/// [`verify_retention_candidate`] do not).
fn verify_manifest_impl(
    snapshot_dir: &Path,
) -> Result<(ManifestVerifyReport, VerifiedManifest, String), BackupError> {
    let manifest_path = snapshot_dir.join(MANIFEST_FILE_NAME);
    let mut file = open_plain_file(&manifest_path).map_err(|error| {
        if error.kind() == io::ErrorKind::NotFound {
            BackupError::ManifestMissing(manifest_path.clone())
        } else {
            BackupError::Io(error)
        }
    })?;
    let mut raw = Vec::new();
    file.read_to_end(&mut raw)?;
    // Round-2 finding 1c: the SHA-256 of the EXACT manifest bytes this
    // verification consumed, handed to the caller so a later reader of the
    // same pathname (restore's step-3 copy) can prove it copied those bytes
    // and not a re-read replacement. Crate-internal only.
    let manifest_digest = hex::encode(Sha256::digest(&raw));
    let (report, manifest) = verify_manifest_raw(snapshot_dir, &raw)?;
    Ok((report, manifest, manifest_digest))
}

/// The verifier core over already-read manifest bytes; [`verify_manifest`]
/// and [`verify_writer_snapshot`] read the bytes themselves,
/// [`verify_retention_candidate`] receives them from the caller. Dispatches
/// on the cheap format-version peek: v2 snapshots route to the child
/// module's [`v2_verify::verify_manifest_v2`]; the v1 path adds only
/// per-entry confinement.
fn verify_manifest_raw(
    snapshot_dir: &Path,
    raw: &[u8],
) -> Result<(ManifestVerifyReport, VerifiedManifest), BackupError> {
    tracing::trace!(target: "shamir_sync::snapshot_verify", "snapshot manifest loaded");
    if v2_verify::peek_manifest_format_version(raw)
        == Some(online_manifest::MANIFEST_FORMAT_VERSION_V2)
    {
        let (report, manifest) = v2_verify::verify_manifest_v2(snapshot_dir, raw)?;
        return Ok((report, VerifiedManifest::V2(manifest)));
    }
    let manifest: Manifest = serde_json::from_slice(raw).map_err(|e| {
        BackupError::ManifestInvalid(format!(
            "decode {}: {e}",
            snapshot_dir.join(MANIFEST_FILE_NAME).display()
        ))
    })?;

    if manifest.format_version != MANIFEST_FORMAT_VERSION {
        return Err(BackupError::UnsupportedManifestFormatVersion {
            found: manifest.format_version,
            expected: MANIFEST_FORMAT_VERSION,
        });
    }

    // A snapshot never carries the private cache subtree (backup skips it):
    // reject it before any file is hashed or restore touches the data dir.
    if let Some(entry) = manifest
        .files
        .iter()
        .find(|entry| is_cache_manifest_path(&entry.path))
    {
        return Err(BackupError::CacheManifestPath(entry.path.clone()));
    }

    let mut accounted: shamir_collections::TFxSet<String> = new_fx_set_wc(manifest.files.len());

    let mut total_bytes = 0u64;
    for entry in &manifest.files {
        reject_unsafe_manifest_path(&entry.path)?;
        if accounted.contains(&entry.path) {
            return Err(BackupError::DuplicateManifestEntry(entry.path.clone()));
        }

        // Shape rule, then per-component walk: a symlink refuses before the content read.
        let file_path = v2_verify::confined_v1_snapshot_path(snapshot_dir, &entry.path)?;
        let (digest, actual_size) = hash_file_streaming(&file_path, &|| false).map_err(|e| {
            BackupError::Io(std::io::Error::new(
                e.kind(),
                format!("manifest entry {}: {e}", entry.path),
            ))
        })?;
        let actual_sha256 = hex::encode(digest);

        if actual_size != entry.size_bytes || actual_sha256 != entry.sha256 {
            return Err(BackupError::ChecksumMismatch {
                path: file_path,
                expected_sha256: entry.sha256.clone(),
                expected_size_bytes: entry.size_bytes,
                actual_sha256,
                actual_size_bytes: actual_size,
            });
        }
        total_bytes += actual_size;
        accounted.insert(entry.path.clone());
    }

    // Every file physically present under snapshot_dir (except the
    // manifest itself) must be accounted for in the manifest. Walker is
    // shared with the v2 child module.
    let mut on_disk = Vec::new();
    v2_verify::collect_relative_file_paths(snapshot_dir, snapshot_dir, &mut on_disk)?;
    for rel in on_disk {
        if rel == MANIFEST_FILE_NAME {
            continue;
        }
        if !accounted.contains(&rel) {
            return Err(BackupError::UnmanifestedFile(snapshot_dir.join(&rel)));
        }
    }

    Ok((
        ManifestVerifyReport {
            files_checked: manifest.files.len() as u64,
            total_bytes,
            kind: BackupKind::Offline,
            format_epoch: None,
            binary_version: None,
            consistency: None,
        },
        VerifiedManifest::V1(manifest),
    ))
}

/// Return whether a manifest path addresses the top-level `cache` subtree,
/// accepting platform separators and harmless `.`/repeated-separator variants.
fn is_cache_manifest_path(entry_path: &str) -> bool {
    let normalized = entry_path.replace('\\', "/");
    normalized
        .split('/')
        .find(|component| !component.is_empty() && *component != ".")
        == Some("cache")
}

/// Reject a manifest entry `path` that is absolute or contains a `..`
/// (`ParentDir`) component — either would let `snapshot_dir.join(path)`
/// escape `snapshot_dir` (an absolute `PathBuf::join` argument REPLACES the
/// base entirely; a relative `..` component walks back out of it), letting
/// a tampered/malicious manifest make [`verify_manifest`] read a file
/// outside the snapshot directory.
fn reject_unsafe_manifest_path(entry_path: &str) -> Result<(), BackupError> {
    let path = Path::new(entry_path);
    if path.is_absolute() {
        return Err(BackupError::UnsafeManifestPath(entry_path.to_string()));
    }
    if path.components().any(|c| matches!(c, Component::ParentDir)) {
        return Err(BackupError::UnsafeManifestPath(entry_path.to_string()));
    }
    Ok(())
}

/// `pub(crate)` — reused by `restore.rs` to copy a snapshot into a
/// temporary sibling directory before the atomic swap into `data_dir`.
///
/// F-19 (#812): fsyncs `dst` once every entry this call added to it has been
/// written (see the `fsync_dir(dst)` call at the end of this function) — so
/// every directory this function writes into, at every recursion level,
/// gets its own directory-entry fsync, not just the top-level destination a
/// caller passes in. Cache exclusion applies only to the root invocation;
/// nested directories named `cache` remain ordinary user data.
///
/// `ledger` binds the walk to the verified manifest (round-2 finding 1,
/// `copy_gate.rs`): `Some` on the restore path (hash + entry comparison in
/// the copy pass; unlisted regular files refuse), `None` on the backup path
/// (backup WRITES the manifest after the walk — nothing to compare
/// against, nothing may refuse).
pub(crate) fn copy_dir_recursive(
    src: &Path,
    dst: &Path,
    bytes: &mut u64,
    files: &mut u64,
    exclude_cache: bool,
    top_level: bool,
    ledger: Option<&RestoreCopyLedger>,
) -> Result<(), CopyGateError> {
    for entry in fs::read_dir(src).map_err(CopyGateError::Io)? {
        let entry = entry.map_err(CopyGateError::Io)?;
        let path = entry.path();
        let file_name = entry.file_name();
        if exclude_cache && top_level && file_name == "cache" {
            continue;
        }
        // The verifiers reject non-Unicode names, and the ledger keys by the
        // lossy relative form: a name that is not valid Unicode could alias a
        // listed entry, so the restore walk refuses it before creating output.
        if let Some(ledger) = ledger {
            if file_name.to_str().is_none() {
                return Err(CopyGateError::UnmanifestedFile(ledger.relative(&path)));
            }
        }
        let target = dst.join(&file_name);
        let ft = entry.file_type().map_err(CopyGateError::Io)?;
        if ft.is_dir() {
            fs::create_dir_all(&target).map_err(CopyGateError::Io)?;
            copy_dir_recursive(&path, &target, bytes, files, exclude_cache, false, ledger)?;
        } else if ft.is_file() {
            // The opened source handle is the one copied and hashed.
            // RESIDUAL WINDOW (round-2 finding 1e): a parent component
            // swapped between this walk's `read_dir` and the leaf open
            // below needs an openat-class (dirfd-relative) primitive this
            // code base does not have — the leaf open is path-based, so the
            // per-file binding covers the leaf's bytes, not the directory
            // chain above it.
            let mut input = open_plain_file(&path).map_err(|e| match (&ledger, e.kind()) {
                // Truthful typed refusal where the ledger exists: the
                // no-follow open refused the leaf as a non-regular object
                // (symlink/reparse point/directory). The backup path has
                // no ledger and no manifest-relative path — generic Io.
                (Some(ledger), io::ErrorKind::InvalidData) => CopyGateError::NotARegularFile {
                    path: ledger.relative(&path),
                },
                _ => CopyGateError::Io(e),
            })?;
            let input_metadata = input.metadata().map_err(CopyGateError::Io)?;
            let opened_size = input_metadata.len();
            // `fs::copy` kept the source permissions; the streamed copy must
            // too (a 0600 key must not become 0644 under the umask).
            let source_permissions = input_metadata.permissions();
            let mut output =
                create_copy_output(&target, &source_permissions).map_err(CopyGateError::Io)?;
            // Finding 1a/1b: copy+hash in one pass, gate on the verified
            // ledger entry before this file counts as copied (`copy_gate.rs`).
            let gate = ledger.map(|ledger| (ledger, ledger.relative(&path)));
            let (n, _staged_digest) = stream_copy(
                &mut input,
                &mut output,
                opened_size,
                gate.as_ref().map(|(ledger, rel)| (*ledger, rel.as_str())),
            )?;
            output
                .set_permissions(source_permissions)
                .map_err(CopyGateError::Io)?;
            output.sync_all().map_err(CopyGateError::Io)?;
            // F-12 (#812): the copy's own handle is synced in place, so the
            // copied bytes are durable before this returns (both `backup()`
            // and `restore()`'s step-3 copy share this function).
            *bytes += n;
            *files += 1;
        } else {
            // Symlinks / sockets / fifos: skip with a warning. A fjall
            // database is a directory tree of journal (`*.jnl`) + LSM
            // segment files — none of those should normally be one of these.
            tracing::warn!(path = %path.display(), "backup: skipping non-regular file");
        }
    }
    // F-19 (#812): fsync `dst` once every entry this invocation added to it
    // (copied files, newly-created subdirectories) has been written — this
    // makes those DIRECTORY ENTRIES durable, not just the copied files'
    // CONTENT (handled above). A directory fsync flushes all of a
    // directory's current entry metadata, not just recently-added entries,
    // so one call after this loop is sufficient for `dst`'s own entries.
    // Symmetric across recursion levels: each nested subdirectory gets its
    // OWN entries (its children) made durable by ITS OWN invocation's fsync
    // here — the top-level `dst` (`backup()`'s `dest_dir`) additionally gets
    // a final fsync from `backup()` itself, after `write_manifest` adds
    // `manifest.json` (which this function never sees). Log-only on
    // failure, same contract as `fsync_dir`'s other call sites — does not
    // change this function's error surface. Benefits `restore()`'s step-3
    // copy too, since it shares this function.
    fsync_dir(dst);
    Ok(())
}

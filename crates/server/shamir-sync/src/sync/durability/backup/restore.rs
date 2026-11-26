//! Offline restore of a `backup::backup` snapshot back into a server's
//! `data_dir` (RI-11).
//!
//! Strictly offline (server-stopped) restore, mirroring the existing
//! stop-and-copy backup model — PITR / WAL-archive / logical dump are out
//! of scope (beta roadmap).
//!
//! Procedure:
//! 1. **Liveness probe** (LEGACY kind only, unless `force`; moved AFTER the
//!    staged presence re-check of step 3b — finding 6, R2d-FIX-C review): the
//!    supplied probe opens the server metadata store —
//!    the same store opened at boot. fjall already takes an exclusive OS-level
//!    advisory file lock per keyspace directory on open, so a lock-
//!    contention error (`MetaError::Fjall(fjall::Error::Locked)`) means a
//!    real server process is running against `data_dir` — refuse. ANY
//!    other open error (corruption, etc.) also refuses by default
//!    (fail-closed: a probe failure could equally mean genuine corruption,
//!    not liveness). `--force` skips this probe entirely. The probe is the
//!    ONLY supplied step that can MUTATE the target (`open_or_init`
//!    persists `last_started_at`), so on the legacy path it now runs only
//!    after verification, the stamp gate, the copy and the staged presence
//!    re-check have ALL succeeded — a missing staged file refuses before the
//!    probe can write anything, leaving the target untouched. A probe that
//!    RUNS (all earlier checks passed) and then refuses has already persisted
//!    `last_started_at` in the original `data_dir` (the swap never ran, so
//!    no rename moved it away); the restore itself cleans the staged copy
//!    and adds nothing to the target beyond that probe write.
//!    ONLINE snapshots bypass this probe in favor of the READ-ONLY
//!    `backup::probe_source_stopped` source-lock probes (early, after
//!    verify, and final, just before the swap).
//! 2. **Verify the SOURCE snapshot's manifest** (`backup::verify_manifest`)
//!    BEFORE touching `data_dir` at all. A checksum mismatch or missing
//!    manifest aborts the whole restore with no side effects.
//! 3. **Copy to a TEMPORARY sibling directory** (same filesystem as
//!    `data_dir`'s parent — required for the atomic rename in step 5). The
//!    temp dir is created with `fs::create_dir` (W-5(b), #790: NOT
//!    `create_dir_all` — a single atomic syscall that itself fails with
//!    `AlreadyExists` if the target is already there, closing the
//!    check-then-act TOCTOU a separate `exists()` probe used to leave open;
//!    safe here because `parent` is guaranteed to already exist). A copy
//!    failure here best-effort removes the staged temp dir
//!    ([`cleanup_staged_temp_dir`]) before propagating the original error.
//!
//! 3b. **Staged presence re-check** (staged_listing.rs): every file the
//!    VERIFIED manifest lists must exist as a regular file under the staged
//!    temp dir AND re-hash to its verified entry. The copy pass itself
//!    already hashed every copied file against the verified manifest
//!    (round-2 finding 1), so this pass is the second line of defense — a
//!    consistency check of the copy. Absence, a byte mismatch and a
//!    non-regular entry each refuse with their own typed variant
//!    ([`RestoreError::StagedFileMissing`],
//!    [`RestoreError::StagedFilePresentButDifferent`],
//!    [`RestoreError::StagedFileNotARegularFile`]), cleans up the staged
//!    copy and leaves `data_dir` untouched.
//! 4. **Invalidate sessions IN THE STAGED COPY, before anything touches the
//!    live `data_dir`**: open the `users` store INSIDE the temp dir (NOT
//!    `data_dir`'s) and call `invalidate_all_tickets(now_ns)` there, then
//!    release it. This validates that the staged snapshot's user store is
//!    structurally loadable AND invalidates pre-restore resumption tickets
//!    in the copy that is about to become live — a failure here (e.g. a
//!    corrupt/unopenable `users` store in the snapshot) aborts the restore
//!    with the CURRENT `data_dir` completely untouched, since the swap
//!    (step 5) has not run yet. Same best-effort temp-dir cleanup as step 3.
//! 5. **Atomic swap**: rename the CURRENT `data_dir` (if it exists) to a
//!    `.pre_restore_backup_<timestamp>` sibling (preserved, NOT deleted —
//!    the explicit rollback path), then rename the temp dir (tickets
//!    already invalidated in step 4) into place as the new `data_dir`. If
//!    the FIRST rename fails (W-5(a), #790: nothing has been staged for a
//!    rollback yet, the pre-existing `data_dir` is untouched), the staged
//!    `temp_dir` is best-effort cleaned up the same way steps 3/4 already
//!    do, then the original error propagates. A failure in the SECOND
//!    rename instead triggers a best-effort rollback of the first — this is
//!    a SEPARATE failure mode from the first rename's, and is unaffected by
//!    the step 4 reordering. The rollback's own outcome determines WHICH of
//!    two distinct errors is returned:
//!    [`RestoreError::SwapFailedRollbackSucceeded`] (rollback worked —
//!    `data_dir` is intact again, nothing to manually rename) or
//!    [`RestoreError::SwapPartialFailure`] (rollback ALSO failed —
//!    `data_dir` is genuinely missing, operator must manually intervene).
//!    `temp_dir` is deliberately left on disk in BOTH of those two cases
//!    (never cleaned up there) — each error message documents exactly why
//!    it's still there.

use std::fs;
use std::io::Read;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use sha2::{Digest, Sha256};
use shamir_db::engine::meta::repo_identity::RepoIdentity;
use shamir_db::ShamirDb;
use thiserror::Error;

use super::offline::{
    self as backup, copy_dir_recursive, BackupError, CopyGateError, RestoreCopyLedger,
    VerifiedManifest,
};
use super::online::manifest::{BackupKind, ManifestV2};
use super::snapshot_lease;
use crate::format_stamp::{restore_snapshot_gate, EpochPolicy};
use crate::meta_port::MetaError;
use crate::user_directory::FjallUserDirectory;

use classify::{classify_snapshot_kind, SnapshotKind};

mod catalogue;
mod classify;
mod fjall_locks;
mod online;
mod staged_listing;
#[cfg(test)]
mod tests;
mod wire_registry;

#[derive(Debug, Error)]
pub enum RestoreError {
    #[error("restore io: {0}")]
    Io(#[from] std::io::Error),
    #[error(
        "server appears to be running against this data_dir ({0}) — stop it first, \
         or pass --force if you are certain it is not"
    )]
    ServerRunning(PathBuf),
    #[error(
        "liveness probe of {0} failed (not a lock-contention error — could indicate \
         corruption): {1}; pass --force to bypass this check"
    )]
    ProbeFailed(PathBuf, MetaError),
    #[error("snapshot manifest verification failed: {0}")]
    ManifestVerification(#[from] BackupError),
    #[error("snapshot format stamp refused: {0}")]
    FormatStamp(#[from] crate::format_stamp::StampError),
    #[error(
        "atomic swap partially failed: pre-restore backup at {pre_restore_backup} \
         and NEW data at {temp_dir} both exist, but {data_dir} could not be \
         reconstructed automatically ({source}) — operator must manually rename \
         one of these two directories to {data_dir}"
    )]
    SwapPartialFailure {
        data_dir: PathBuf,
        pre_restore_backup: PathBuf,
        temp_dir: PathBuf,
        #[source]
        source: std::io::Error,
    },
    /// N-6: the swap's final rename (temp_dir -> data_dir) failed, but the
    /// best-effort ROLLBACK (pre_restore_backup -> data_dir) SUCCEEDED —
    /// `data_dir` is intact again, holding the ORIGINAL pre-restore data.
    /// Distinct from [`Self::SwapPartialFailure`] (rollback also failed,
    /// data_dir genuinely missing): here nothing needs manual renaming, so
    /// the message must NOT tell the operator to rename anything — the
    /// restored (but not swapped in) copy is left at `temp_dir` purely for
    /// inspection/retry.
    #[error(
        "restore's final swap step failed ({source}); the automatic rollback \
         SUCCEEDED — {data_dir} is intact and contains the ORIGINAL \
         pre-restore data. The restored copy (not swapped in) is left at \
         {temp_dir} for inspection or retry; no manual action is needed."
    )]
    SwapFailedRollbackSucceeded {
        data_dir: PathBuf,
        temp_dir: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("opening restored user directory failed: {0}")]
    UserDirectory(#[from] fjall::Error),
    #[error("restore identity: {0}")]
    Identity(#[from] shamir_db::DbError),
    #[error("invalidating tickets in restored user directory failed: {0}")]
    Invalidate(#[from] shamir_connect::common::error::Error),
    /// Online restore only: replaying one captured repository to its
    /// recorded point `V*` refused or failed. `data_dir` is untouched; the
    /// staged copy is cleaned up by the caller.
    #[error("replaying repository '{repo}' to its captured point failed: {source}")]
    Replay {
        repo: String,
        #[source]
        source: Box<shamir_db::engine::db_instance::replay_to::ReplayToError>,
    },
    /// Online restore only: the staged catalogue/consistency pass refused
    /// (captured set vs staged rows/directories, reserved or ambiguous
    /// destinations, boot-time set mismatch). Names the repository.
    #[error("staged online restore consistency check refused for '{repo}': {reason}")]
    Consistency { repo: String, reason: String },
    /// Online restore only: opening or mutating the STAGED system catalogue
    /// failed. Never implies anything about `data_dir`.
    #[error("staged system catalogue access failed: {0}")]
    Catalogue(shamir_db::DbError),
    /// Step 3b staged presence re-check: a file the VERIFIED manifest lists
    /// is ABSENT from the staged copy — the snapshot's source tree lost it
    /// between verification and enumeration. A present-but-different or
    /// present-but-non-regular file gets its own typed variant below; this
    /// one is only ever proof of genuine absence. Publishing would produce
    /// an INCOMPLETE target, so the restore refuses (staged copy cleaned
    /// up, `data_dir` untouched). Names ONLY the manifest-relative file
    /// (finding 6: never the joined staging path).
    #[error(
        "staged copy is missing a file listed in the verified manifest: {0} — \
         the snapshot changed between verification and the copy; refusing to \
         publish an incomplete restore"
    )]
    StagedFileMissing(PathBuf),
    /// Step-3 copy gate (round-2 finding 1b): a REGULAR file the snapshot
    /// walk found that the verified manifest does not list (outside the
    /// format's root control files). Manifest-relative path only.
    #[error(
        "snapshot contains a file the verified manifest does not list: {0} — \
         refusing to publish a restore that verification never covered"
    )]
    StagedFileUnmanifested(String),
    /// Finding 5 (round 2): the file IS present but its bytes differ from
    /// the verified manifest entry (size and/or sha256) — truthfully
    /// distinct from [`Self::StagedFileMissing`]. Manifest-relative only.
    #[error(
        "staged file differs from the verified manifest entry: {path} \
         (expected sha256 {expected_sha256}, {expected_size_bytes} bytes; \
         found sha256 {actual_sha256}, {actual_size_bytes} bytes)"
    )]
    StagedFilePresentButDifferent {
        path: String,
        expected_sha256: String,
        expected_size_bytes: u64,
        actual_sha256: String,
        actual_size_bytes: u64,
    },
    /// Finding 5 (round 2): the manifest-listed name exists under the
    /// staged copy but the no-follow open refused it as a link / directory
    /// / other non-regular object — truthfully distinct from absence.
    #[error(
        "staged copy holds a non-regular file where the verified manifest \
         lists a regular file: {0}"
    )]
    StagedFileNotARegularFile(String),
    /// Restore step 0 (kind classification): the classifier's no-follow
    /// open of the ROOT `manifest.json` refused it as a non-regular object
    /// (symlink / reparse point / directory). Distinct from the
    /// verification-stage refusal of the same shape
    /// ([`Self::ManifestVerification`]) so the control-file link
    /// regressions can attribute the refusal to the CLASSIFIER stage.
    /// Manifest-relative path only.
    #[error(
        "snapshot control file is not a regular file (link or reparse \
         point): {path} — refusing before any probe or copy runs"
    )]
    ControlFileNotRegular { path: String },
}

/// Outcome of a successful restore.
#[derive(Debug)]
pub struct RestoreReport {
    /// Number of files copied from the snapshot into the new `data_dir`.
    pub files_restored: u64,
    /// Total bytes copied.
    pub bytes_restored: u64,
    /// Number of user accounts whose `tickets_invalid_before_ns` was bumped.
    pub users_invalidated: usize,
    /// The `.pre_restore_backup_<timestamp>` sibling directory holding the
    /// PRE-restore `data_dir` contents (`None` if there was no pre-existing
    /// `data_dir` to preserve — e.g. restoring into a fresh target).
    pub pre_restore_backup: Option<PathBuf>,
    /// Online restores only: repositories (`db/repo`) that were part of the
    /// staged catalogue but NOT part of the manifest's captured set — their
    /// persisted rows were removed from the STAGED catalogue (files stay as
    /// inert orphans) and in-memory rows are reported without being opened
    /// (design Q8). Empty for offline/v1 restores.
    pub excluded_repositories: Vec<String>,
}

/// Run the canonical restore procedure using the supplied liveness probe.
///
/// See the module doc for the full step-by-step procedure. `force` skips
/// the supplied probe entirely — use only when certain no server process
/// holds `data_dir`.
///
/// Kind-aware (design `online-backup.md` §5): a v2 ONLINE manifest runs
/// the online pipeline (staged replay + consistency, `restore/online.rs`)
/// and BYPASSES the supplied probe in favor of the read-only
/// `backup::probe_source_stopped` source-lock probe, because the caller's
/// probe (the server's liveness wrapper) can MUTATE the target on refusal
/// and must not touch `data_dir` for a valid online snapshot. For a v1 or
/// v2-offline manifest the supplied probe still runs, but AFTER the staged
/// presence re-check succeeds (finding 6: the probe's `open_or_init`
/// persists `last_started_at` in the target, so a missing staged file must
/// refuse before the probe can write anything).
///
/// Operator prerequisites, holding for the WHOLE command: `data_dir`'s
/// server stays STOPPED (no handler is ever started against the live
/// target; the online path additionally re-probes the source lock just
/// before the swap) and the snapshot directory stays IMMUTABLE — the
/// verified manifest and the staged bytes are only trustworthy while the
/// source cannot change underneath the restore.
///
/// `from` and `data_dir` MUST be on the same filesystem as `data_dir`'s
/// parent directory, since step 3/5 relies on `fs::rename` being atomic
/// (a cross-filesystem rename would silently fall back to copy+delete on
/// some platforms, or fail outright — this function does not attempt to
/// paper over that; `fs::rename`'s own error surfaces as-is).
pub fn restore_with_probe<F>(
    from: &Path,
    data_dir: &Path,
    force: bool,
    probe: F,
) -> Result<RestoreReport, RestoreError>
where
    F: FnOnce(&Path) -> Result<(), RestoreError>,
{
    restore_impl(from, data_dir, force, probe, || {})
}

/// The exact [`restore_with_probe`] body, with one private test seam:
/// `before_copy` runs after verification and the format-stamp gate and
/// before the step-3 source walk. Production callers pass a no-op (the
/// delegating public wrapper above); the staged-listing regression test
/// uses it to delete a manifest-listed file from the snapshot between
/// verification and enumeration, deterministically reproducing a reader
/// racing a deleter. The seam inserts NO behavior of its own.
fn restore_impl<F, B>(
    from: &Path,
    data_dir: &Path,
    force: bool,
    probe: F,
    before_copy: B,
) -> Result<RestoreReport, RestoreError>
where
    F: FnOnce(&Path) -> Result<(), RestoreError>,
    B: FnOnce(),
{
    // Shared cross-process read lease on a MANAGED-root snapshot (no-op for
    // an exported copy): held from before the first read of the snapshot
    // through its last source read, so scheduled retention skips it. The
    // call makes the ONE canonical-source decision: a snapshot ABSENT at
    // this point refuses (a child that appears afterwards is never read
    // unleased by re-opening the original pathname), and every later read
    // consumes ONLY the resolved canonical path — no second lookup.
    let (source, source_lease) = match snapshot_lease::read_snapshot(from)
        .map_err(BackupError::Io)?
    {
        snapshot_lease::SourceSnapshot::Missing => {
            return Err(RestoreError::ManifestVerification(
                BackupError::ManifestMissing(from.join(backup::MANIFEST_FILE_NAME)),
            ));
        }
        snapshot_lease::SourceSnapshot::Unmanaged { canonical } => (canonical, None),
        snapshot_lease::SourceSnapshot::Managed { canonical, lease } => (canonical, Some(lease)),
    };

    // ---- Step 0: classify the snapshot kind from the raw manifest (pure
    // read, FAIL-CLOSED). A recognized v1 or v2-offline header stays on the
    // v1 path below — the supplied liveness probe moved AFTER the staged
    // presence re-check (finding 6, R2d-FIX-C review). An online v2 manifest
    // runs the online procedure: full verify FIRST
    // (read-only; includes the manifest/stamp epoch cross-check), then a
    // READ-ONLY source-lock probe — the caller-supplied probe is bypassed
    // because the server's liveness wrapper MUTATES the target on refusal
    // and must not touch `data_dir` for a valid online snapshot (see the
    // server-side integration note in `restore_with_probe`'s doc). Anything
    // UNCLASSIFIABLE — unreadable, unparseable, or an unknown
    // version/kind header — refuses with the verify-side error semantics
    // BEFORE any probe callback runs, so a corrupt snapshot can never
    // trigger the legacy probe's target mutation (P1, R2a 3b-2 review).
    let online_classified = matches!(classify_snapshot_kind(&source)?, SnapshotKind::Online);

    // ---- Step 1: verify the manifest consumed by the chosen path ----
    // ONLINE (classified from the raw manifest): verify FIRST and consume
    // ONLY the manifest this verification returns — the classifier's decode
    // is never trusted beyond the boolean probe-path choice. Verification
    // also returns the SHA-256 of the exact manifest BYTES it consumed
    // (round-2 finding 1c); the step-3a check below proves the staged copy
    // carries those bytes and not a re-read replacement. The source
    // manifest is assumed FIXED for the duration of the command (documented
    // operator prerequisite: a snapshot directory is immutable while being
    // restored); its file list drives the step-3 copy gate AND the staged
    // presence re-check, which now RE-HASHES every listed staged file as a
    // second line of defense.
    let (verified_manifest, manifest_digest): (VerifiedManifest, String) = if online_classified {
        let (report, manifest, manifest_digest) = backup::verify_manifest_for_restore(&source)?;
        if !matches!(report.kind, BackupKind::Online) {
            // Classified online but verified as something else: the
            // classification decode is stale — refuse as corrupt rather
            // than guessing which decode to trust.
            return Err(RestoreError::ManifestVerification(
                BackupError::ManifestInvalid(
                    "manifest kind changed between classification and verification".into(),
                ),
            ));
        }
        // Read-only target probe, exactly where the original online path ran
        // it: right after verify + kind check, before the stamp gate and the
        // copy. The caller-supplied (target-MUTATING) probe is bypassed.
        if !force {
            backup::probe_source_stopped(data_dir).map_err(|e| match e {
                BackupError::SourceRunning(_) => {
                    RestoreError::ServerRunning(data_dir.to_path_buf())
                }
                BackupError::Io(io) => RestoreError::Io(io),
                other => RestoreError::ManifestVerification(other),
            })?;
        }
        (manifest, manifest_digest)
    } else {
        // The legacy probe NO LONGER runs here (finding 6): it MUTATES the
        // target, so it now waits until the staged presence re-check below
        // has succeeded. Verification is the first source-consuming step.
        // ---- Step 2: verify the SOURCE snapshot's manifest, before touching
        // data_dir at all — and KEEP the exact verified manifest: the staged
        // presence re-check (step 3b) needs its file list, so the source is
        // verified exactly ONCE per kind, as before. The opposite flip — a
        // legacy-classified snapshot that verifies as ONLINE — refuses with
        // the same typed error instead of letting the replay pipeline run
        // against a target that was only guarded by the legacy probe. ----
        let (report, manifest, manifest_digest) = backup::verify_manifest_for_restore(&source)?;
        if matches!(report.kind, BackupKind::Online) {
            return Err(RestoreError::ManifestVerification(
                BackupError::ManifestInvalid(
                    "manifest kind changed between classification and verification".into(),
                ),
            ));
        }
        (manifest, manifest_digest)
    };

    // ---- Step 2b: verify the SOURCE snapshot's format stamp (design 3.5).
    // Newer, corrupt or newer-layout refuses; absent or older is allowed
    // (rollback must work). The copy brings its own stamp; nothing is
    // written here. ----
    restore_snapshot_gate(&source, &EpochPolicy::compiled())?;

    // ---- Test seam: runs after verification and the stamp gate, before the
    // step-3 source walk. Production no-op (see `restore_impl`'s doc). ----
    before_copy();

    // ---- Step 3: copy to a temporary sibling directory ----
    let stamp = restore_timestamp();
    // `parent` necessarily already exists: either it's `data_dir.parent()`
    // (a real, operator-configured `data_dir` sits inside an already-
    // existing directory), or `data_dir` has no parent at all (it IS a
    // filesystem root, e.g. `/` or `C:\`), in which case `parent` falls back
    // to `data_dir` itself — a root that trivially always exists. Either
    // way, `fs::create_dir` below (which, unlike `create_dir_all`, does NOT
    // create missing parents) is safe to use directly.
    let parent = data_dir.parent().unwrap_or(data_dir);
    let temp_dir = parent.join(format!(
        "{}.restore_tmp_{stamp}",
        dir_name(data_dir, "data_dir")
    ));
    // W-5(b) (#790, Wave D review): a separate `exists()` check followed by
    // `create_dir_all` was a check-then-act TOCTOU (a second concurrent
    // `restore()` call could create `temp_dir` in the gap between the check
    // and the create). `fs::create_dir` closes this atomically — it is a
    // single syscall that itself fails with `ErrorKind::AlreadyExists` if
    // the target already exists, so there is no window between "check" and
    // "create" at all.
    if let Err(e) = fs::create_dir(&temp_dir) {
        if e.kind() == std::io::ErrorKind::AlreadyExists {
            return Err(RestoreError::Io(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!(
                    "temporary restore dir already exists: {}",
                    temp_dir.display()
                ),
            )));
        }
        return Err(RestoreError::Io(e));
    }
    let mut bytes = 0u64;
    let mut files = 0u64;
    // Round-2 finding 1a/1b: the walk is BOUND to the verified manifest —
    // every copied regular file is hashed in the copy pass and compared to
    // its verified entry before it counts as copied, and an unlisted
    // regular file (outside the control-file union) refuses typed.
    let ledger = RestoreCopyLedger::new(
        &source,
        verified_manifest.files(),
        // Derived from the SAME verified manifest's variant — the ledger's
        // root control-file allow-set equals that verifier's (v1:
        // manifest.json only; v2: manifest.json + backup.meta.json).
        verified_manifest.allows_backup_meta(),
    );
    if let Err(e) = copy_dir_recursive(
        &source,
        &temp_dir,
        &mut bytes,
        &mut files,
        false,
        true,
        Some(&ledger),
    ) {
        // N-6: the error message for this failure gives the operator no
        // reference to `temp_dir` at all — clean it up best-effort (log,
        // don't let a cleanup failure mask the original error) rather than
        // leaving an orphaned `*.restore_tmp_*` directory behind.
        cleanup_staged_temp_dir(&temp_dir);
        return Err(match e {
            CopyGateError::Io(io) => RestoreError::Io(io),
            CopyGateError::NotARegularFile { path } => {
                RestoreError::StagedFileNotARegularFile(path)
            }
            CopyGateError::UnmanifestedFile(rel) => RestoreError::StagedFileUnmanifested(rel),
            CopyGateError::ListedFileMismatch {
                path,
                expected_sha256,
                expected_size_bytes,
                actual_sha256,
                actual_size_bytes,
            } => RestoreError::StagedFilePresentButDifferent {
                path,
                expected_sha256,
                expected_size_bytes,
                actual_sha256,
                actual_size_bytes,
            },
        });
    }

    // ---- Step 3a (round-2 finding 1c, digest-compare choice): re-hash the
    // staged `manifest.json` and require it to equal the digest
    // verification computed over the manifest bytes it consumed — a
    // mismatch means the copy did not carry the verified bytes. Refusal
    // cleans the staged copy; `data_dir` untouched. ----
    let staged_manifest = temp_dir.join(backup::MANIFEST_FILE_NAME);
    // Every error out of this read loop must run the same staged-copy
    // cleanup as the open-failure and digest-mismatch branches below:
    // compute the digest into a Result FIRST, clean up, then propagate.
    let staged_digest = match backup::open_plain_snapshot_file(&staged_manifest) {
        Ok(mut file) => {
            let mut hasher = Sha256::new();
            let mut buffer = [0u8; 64 * 1024];
            let mut read_result = Ok(());
            loop {
                match file.read(&mut buffer) {
                    Ok(0) => break,
                    Ok(n) => hasher.update(&buffer[..n]),
                    Err(e) => {
                        read_result = Err(e);
                        break;
                    }
                }
            }
            match read_result {
                Ok(()) => hex::encode(hasher.finalize()),
                Err(e) => {
                    cleanup_staged_temp_dir(&temp_dir);
                    return Err(RestoreError::Io(e));
                }
            }
        }
        // The copy pass just copied this exact name; absence or a
        // non-regular object here is an internal inconsistency.
        Err(e) => {
            cleanup_staged_temp_dir(&temp_dir);
            return Err(RestoreError::Io(e));
        }
    };
    if staged_digest != manifest_digest {
        cleanup_staged_temp_dir(&temp_dir);
        return Err(RestoreError::ManifestVerification(
            BackupError::ManifestInvalid(
                "staged manifest bytes differ from the manifest bytes verification consumed".into(),
            ),
        ));
    }
    // Last source read done — release the shared lease before any staging
    // mutation, so retention is never blocked by non-reading work.
    drop(source_lease);

    // The v2 payload for the online replay, extracted BEFORE the staged
    // presence re-check so a kind-change between classification and
    // verification fails closed before any further staging work.
    let replay_manifest: Option<&ManifestV2> = match (online_classified, &verified_manifest) {
        (true, VerifiedManifest::V2(manifest)) => Some(manifest),
        (true, VerifiedManifest::V1(_)) => {
            cleanup_staged_temp_dir(&temp_dir);
            return Err(RestoreError::ManifestVerification(
                BackupError::ManifestInvalid(
                    "manifest kind changed between classification and verification".into(),
                ),
            ));
        }
        (false, _) => None,
    };

    // ---- Step 3b: staged presence re-check — every file the verified
    // manifest lists must exist as a REGULAR file under the staged copy AND
    // re-hash to its verified entry (finding 1d: now the SECOND line of
    // defense behind the step-3 copy gate's in-pass hashing — a consistency
    // check of the copy itself). Refusals are typed and truthful: absence
    // is [`RestoreError::StagedFileMissing`], present-but-different is
    // [`RestoreError::StagedFilePresentButDifferent`], non-regular is
    // [`RestoreError::StagedFileNotARegularFile`]. Refusal cleans the
    // staged copy; `data_dir` is untouched. ----
    if let Err(e) = staged_listing::verify_staged_files(&temp_dir, verified_manifest.files()) {
        cleanup_staged_temp_dir(&temp_dir);
        return Err(e);
    }

    // ---- Step 3b' (LEGACY kind only, unless force): liveness probe, run
    // ONLY after the staged presence re-check succeeded (finding 6, R2d-FIX-C
    // review). The CLI probe's `server_meta::open_or_init` persists
    // `last_started_at` in the target, so a missing staged file now refuses
    // BEFORE the probe can write anything. A probe failure at this point
    // cleans the staged copy and leaves `data_dir` untouched by the restore
    // itself. The ONLINE path bypasses this probe entirely and keeps its
    // read-only `probe_source_stopped` probes (early, after verify; final,
    // before the swap). ----
    let probe_result = if !online_classified && !force {
        probe(data_dir)
    } else {
        Ok(())
    };
    if let Err(e) = probe_result {
        cleanup_staged_temp_dir(&temp_dir);
        return Err(e);
    }

    // ---- Step 3c (online kind only): replay + staged-catalogue consistency
    // inside the staged copy. Every refusal propagates after best-effort
    // cleanup of the staged directory; `data_dir` is untouched. ----
    let excluded_repositories = if let Some(manifest) = replay_manifest {
        match online::run(&temp_dir, manifest) {
            Ok(excluded) => excluded,
            Err(e) => {
                cleanup_staged_temp_dir(&temp_dir);
                return Err(e);
            }
        }
    } else {
        Vec::new()
    };

    // ---- Step 4: invalidate sessions IN THE STAGED COPY, before the swap.
    // A failure here (e.g. a corrupt/unopenable `users` store in the
    // snapshot) leaves `data_dir` completely untouched, since the swap
    // below has not run yet. ----
    let now_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let users_invalidated = match open_and_invalidate_users(&temp_dir, now_ns) {
        Ok(n) => n,
        Err(e) => {
            // N-6: same rationale as the step-3 copy failure above — this
            // error's message has no reference to `temp_dir` either, so it
            // would otherwise be orphaned on disk.
            cleanup_staged_temp_dir(&temp_dir);
            return Err(e);
        }
    };

    // Mark the unpublished copy BEFORE either swap rename. Never adopt a
    // marker copied from a backup: each restore has a new OS-random operation.
    // Failure leaves the live directory untouched and staging available.
    write_restore_marker(&temp_dir)?;

    // ---- Step 4b (online kind only, unless force): read-only re-probe of
    // the source lock just before the swap. The verification and staging
    // above take time; without this re-probe a server that started against
    // `data_dir` mid-restore would only hit fjall's OS lock at boot, after
    // the swapped-in copy is already live. Refusal cleans the staged copy —
    // `data_dir` is untouched. `force` skips this too, matching the caller's
    // explicit certainty. ----
    if online_classified && !force {
        if let Err(e) = backup::probe_source_stopped(data_dir) {
            cleanup_staged_temp_dir(&temp_dir);
            return Err(match e {
                BackupError::SourceRunning(_) => {
                    RestoreError::ServerRunning(data_dir.to_path_buf())
                }
                BackupError::Io(io) => RestoreError::Io(io),
                other => RestoreError::ManifestVerification(other),
            });
        }
    }

    // ---- Step 5: atomic swap ----
    let pre_restore_backup = if data_dir.exists() {
        let backup_sibling = parent.join(format!(
            "{}.pre_restore_backup_{stamp}",
            dir_name(data_dir, "data_dir")
        ));
        if let Err(e) = fs::rename(data_dir, &backup_sibling) {
            // W-5(a) (#790, Wave D review): this is the FIRST rename of the
            // swap, distinct from the second rename's failure paths below
            // (`SwapFailedRollbackSucceeded`/`SwapPartialFailure`) — nothing
            // has been staged for a rollback yet (the pre-existing
            // `data_dir` is still in place, untouched), and `temp_dir` is
            // the fully-staged (copied, tickets-invalidated) snapshot with
            // no further use once this error propagates. Left uncleaned,
            // this was the same orphaned-`temp_dir`-with-no-message-
            // reference gap N-6 already fixed for steps 3/4, just missed for
            // this specific step-5 sub-case.
            cleanup_staged_temp_dir(&temp_dir);
            return Err(RestoreError::Io(e));
        }
        // F-12 (#802): fsync the containing directory so this first rename's
        // directory-entry change (data_dir → backup_sibling) is durable — a
        // bare `fs::rename` can return before the directory-entry update
        // itself reaches stable storage. Log-only (see `fsync_dir`'s doc).
        fsync_dir(parent);
        match fs::rename(&temp_dir, data_dir) {
            Ok(()) => {
                // F-12 (#802): fsync for the second rename
                // (temp_dir → data_dir), same rationale as above.
                fsync_dir(parent);
                Some(backup_sibling)
            }
            Err(e) => {
                // Best-effort rollback: put the pre-restore backup back so
                // data_dir is not left missing.
                if fs::rename(&backup_sibling, data_dir).is_err() {
                    // Rollback ALSO failed: data_dir is genuinely missing —
                    // both `backup_sibling` and `temp_dir` exist, operator
                    // must manually choose and rename one to `data_dir`.
                    // `temp_dir` MUST survive on disk here — do NOT clean it
                    // up, the error message points the operator at it.
                    return Err(RestoreError::SwapPartialFailure {
                        data_dir: data_dir.to_path_buf(),
                        pre_restore_backup: backup_sibling,
                        temp_dir,
                        source: e,
                    });
                }
                // F-12 (#802): fsync for the rollback rename
                // (backup_sibling → data_dir), same rationale — even on the
                // rollback path, the directory-entry change should be durable.
                fsync_dir(parent);
                // Rollback SUCCEEDED: data_dir is intact again, holding the
                // ORIGINAL pre-restore data. `temp_dir` (the restored-but-
                // not-swapped-in copy) is deliberately left on disk for
                // inspection/retry — do NOT clean it up here, the error
                // message points the operator at it.
                return Err(RestoreError::SwapFailedRollbackSucceeded {
                    data_dir: data_dir.to_path_buf(),
                    temp_dir,
                    source: e,
                });
            }
        }
    } else {
        fs::rename(&temp_dir, data_dir)?;
        // F-12 (#802): fsync the containing directory so this fresh-target
        // rename (temp_dir → data_dir) is durable — same rationale as the
        // swap-path renames above. Log-only (see `fsync_dir`'s doc).
        fsync_dir(parent);
        None
    };

    let report = RestoreReport {
        files_restored: files,
        bytes_restored: bytes,
        users_invalidated,
        pre_restore_backup,
        excluded_repositories,
    };
    // Only AFTER the swap succeeded — before it, a refusal must stay silent
    // about an outcome that never happened; the v1/offline list is always
    // empty, so this logs nothing on the legacy path.
    if !report.excluded_repositories.is_empty() {
        tracing::warn!(
            repos = ?report.excluded_repositories,
            "restore: repositories created after capture were EXCLUDED from the \
             restored catalogue (their staged rows were removed; files remain \
             as inert orphans)"
        );
    }
    Ok(report)
}

const RESTORE_MARKER: &str = ".shamir_restore_epoch";
const RESTORE_MAGIC: &[u8; 4] = b"SRE1";

pub fn write_restore_marker(data_dir: &Path) -> Result<[u8; 16], RestoreError> {
    let operation = RepoIdentity::new_lineage_id()?;
    // Staging is exclusively owned by this offline restore. Truncation is
    // deliberate: a backup's old operation must not suppress this restore.
    let mut file = fs::File::create(data_dir.join(RESTORE_MARKER))?;
    file.write_all(RESTORE_MAGIC)?;
    file.write_all(&operation)?;
    file.sync_all()?;
    sync_restore_directory(data_dir)?;
    Ok(operation)
}

/// Called under the server data-directory ownership lock, before handlers or
/// supervisors exist, and before creating any new default repository. All
/// restored catalogue repos are processed before removing the global marker.
/// cancel-safe: NO — completed bumps remain, but their atomic receipts make a
/// subsequent open of the same marker idempotent.
pub async fn consume_restore_marker(data_dir: &Path, db: &ShamirDb) -> Result<(), RestoreError> {
    let path = data_dir.join(RESTORE_MARKER);
    let bytes = match tokio::fs::read(&path).await {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e.into()),
    };
    if bytes.len() != 20 || &bytes[..4] != RESTORE_MAGIC {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid restore operation marker",
        )
        .into());
    }
    let mut operation = [0; 16];
    operation.copy_from_slice(&bytes[4..]);
    for name in db.list_dbs() {
        let database = db.get_db(&name).ok_or_else(|| {
            std::io::Error::other("restore database disappeared during exclusive open")
        })?;
        for name in database.list_repos() {
            let repo = database.get_repo(&name).ok_or_else(|| {
                std::io::Error::other("restore repository disappeared during exclusive open")
            })?;
            repo.apply_restore_operation(operation).await?;
        }
    }
    tokio::fs::remove_file(path).await?;
    let directory = data_dir.to_path_buf();
    tokio::task::spawn_blocking(move || sync_restore_directory(&directory))
        .await
        .map_err(|e| std::io::Error::other(format!("restore directory sync task: {e}")))??;
    Ok(())
}

#[cfg(unix)]
fn sync_restore_directory(dir: &Path) -> std::io::Result<()> {
    fs::File::open(dir)?.sync_all()
}

#[cfg(not(unix))]
fn sync_restore_directory(_dir: &Path) -> std::io::Result<()> {
    // File contents are explicitly synced. The existing non-Unix rename
    // helper is NOT evidence of directory-entry durability on power loss.
    tracing::warn!("restore: directory-entry power-loss durability unverified on this platform");
    Ok(())
}

/// Step 4 body: open the staged copy's `users` store and invalidate every
/// ticket. Split out of [`restore`] so both callers (the real step and the
/// N-6 cleanup-on-failure path around it) share one `?`-propagating body.
///
/// N-9/N-6 note: `FjallUserDirectory::open` materializes an empty `users`
/// store when the snapshot lacks one — cosmetic (a restored server needs
/// SOME `users` store to open regardless), not a bug; documented here since
/// it's otherwise an undocumented side effect of this call.
fn open_and_invalidate_users(temp_dir: &Path, now_ns: u64) -> Result<usize, RestoreError> {
    let users = FjallUserDirectory::open(temp_dir.join("users"))?;
    let n = users.invalidate_all_tickets(now_ns)?;
    drop(users); // release fjall's lock before the swap renames this tree
    Ok(n)
}

/// N-6: best-effort removal of the staged `*.restore_tmp_*` directory after
/// a step-3 (copy) or step-4 (invalidate) failure — those error messages
/// give the operator no reference to `temp_dir` at all, so an orphaned copy
/// left behind would just be disk-space debt with no discoverable pointer
/// to it. Deliberately NOT used on the step-5 (swap) failure paths, where
/// `temp_dir` must survive exactly as documented in `RestoreError`'s
/// messages. A cleanup failure is logged, never propagated — it must not
/// mask the original error that triggered this cleanup attempt.
fn cleanup_staged_temp_dir(temp_dir: &Path) {
    if let Err(e) = fs::remove_dir_all(temp_dir) {
        tracing::warn!(
            temp_dir = %temp_dir.display(),
            error = %e,
            "restore: failed to clean up staged temp dir after an earlier failure"
        );
    }
}

/// Fsync the directory at `dir` so a rename's (or, since F-19/#812, a plain
/// `create`'s) directory-entry update is durable (F-12, #802). On ext4/xfs,
/// a successful `fs::rename` can return before the directory-entry change
/// itself is on stable storage — a power loss shortly after a "successful"
/// rename can leave the filesystem reflecting the PRE-rename state on next
/// boot (the classic "you fsync'd the file but not the directory" gap).
/// This makes `restore()`'s atomic swap actually crash-durable.
///
/// F-19 (#812): also reused by `backup.rs`'s `backup()`, which never renames
/// anything but does create brand-new directory entries (files and
/// subdirectories) under a freshly-created `dest_dir` — the same
/// "fsync'd the file, not the directory" gap applies to plain creates, not
/// just renames. Made `pub(crate)` for that reuse; behavior is unchanged
/// (log-only on failure, `#[cfg(not(unix))]` no-op).
///
/// Failures are logged but NOT propagated (matching `shamir-wal`'s
/// `wal_segment.rs::fsync_parent_dir` precedent): a missing directory fsync
/// degrades the power-loss window but does not corrupt data, and refusing
/// an otherwise-successful rename over a non-fatal dir-fsync would be worse
/// than the degraded window. This asymmetry — file `sync_all()` propagates
/// (content durability matters), directory fsync logs only (best-effort) —
/// is the established workspace convention (see `segment_meta.rs`'s file-sync
/// step, which propagates, vs `wal_segment.rs`'s directory fsync, which
/// logs). Since the directory fsync is log-only, it introduces NO new error
/// path into `restore()`'s return type (`RestoreError` needs no new variant).
///
/// **Windows / non-unix: directory fsync is a no-op here** — the specific
/// durability guarantee this targets (directory-entry durability after
/// rename) is provided by the filesystem's own rename semantics on those
/// platforms. This matches `wal_segment.rs`'s own `#[cfg(not(unix))]`
/// rationale exactly — this workspace already decided Windows doesn't need
/// this specific guarantee, and that decision carries into the backup/restore
/// path unchanged.
///
/// **Test scope note:** true power-loss crash injection (verifying a
/// post-rename directory entry survives an actual power failure) is outside
/// what a portable unit test can exercise — there is no way to truncate the
/// OS page cache mid-syscall from a `#[test]`. The regression guard for this
/// task is the interrupted-copy test in `tests/restore_tests.rs`
/// (`copy_step_failure_propagates_and_leaves_data_dir_untouched`), which
/// verifies the closest portable proxy: a failure during `restore()`'s copy
/// step propagates cleanly and leaves `data_dir` completely untouched.
#[cfg(unix)]
pub fn fsync_dir(dir: &Path) {
    match fs::File::open(dir) {
        Ok(dir_f) => {
            if let Err(e) = dir_f.sync_all() {
                tracing::warn!(
                    dir = %dir.display(),
                    error = %e,
                    "restore: directory fsync after rename failed; \
                     rename durability is not guaranteed on power loss \
                     (F-12, #802) — the rename itself succeeded"
                );
            }
        }
        Err(e) => {
            tracing::warn!(
                dir = %dir.display(),
                error = %e,
                "restore: directory open for fsync after rename failed; \
                 rename durability is not guaranteed on power loss (F-12, #802)"
            );
        }
    }
}

#[cfg(not(unix))]
pub fn fsync_dir(_dir: &Path) {
    // Windows / non-unix: directory fsync is not required for the durability
    // guarantee this targets (see the doc on the unix variant above). No-op —
    // matches wal_segment.rs's `#[cfg(not(unix))]` rationale exactly.
}

/// `YYYYMMDD_HHMMSS` UTC, matching `backup::utc_timestamp`'s format (kept
/// separate rather than reusing it directly since that helper is
/// `pub(crate)` in `backup.rs` and this needs the identical shape here).
fn restore_timestamp() -> String {
    backup::utc_timestamp()
}

/// Directory name (final path component) of `path`, falling back to
/// `default` if it cannot be determined (e.g. `path` is `/` or empty).
fn dir_name(path: &Path, default: &str) -> String {
    path.file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| default.to_string())
}

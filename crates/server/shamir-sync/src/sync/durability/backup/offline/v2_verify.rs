//! v2 manifest verify internals (design `online-backup.md` §5 "Offline
//! verify"): child module of `offline.rs`, one primary item
//! ([`verify_manifest_v2`]). Pure file inspection — no engine open.

#[cfg(test)]
use std::cell::Cell;
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use shamir_collections::new_fx_set_wc;

use super::super::online::manifest::{self as online_manifest, BackupKind};
use super::{
    hash_file_streaming, is_cache_manifest_path, BackupError, HashCancelled, ManifestVerifyReport,
    MANIFEST_FILE_NAME,
};

/// Full offline verify of a v2 snapshot. Returns the report AND the exact
/// decoded [`online_manifest::ManifestV2`] so restore can consume the
/// already-verified manifest without parsing `manifest.json` a second time
/// (the caller must only use it after this function returned `Ok`).
///
/// Gaps closed over the previous v2-only pass, per design §5:
/// * report carries `kind`, `format_epoch`, `binary_version` and the
///   consistency positions;
/// * manifest `format_epoch` is cross-checked against the snapshot's copied
///   stamp read through the real stamp codec: a valid stamp with a
///   mismatching or MISSING manifest epoch refuses typed; an ABSENT stamp
///   stays permitted (today's rule — the restore-side snapshot gate owns
///   absent-stamp policy); a corrupt / newer-layout stamp refuses typed
///   ([`BackupError::InvalidSnapshotStamp`]), independent of the restore gate;
/// * every manifest-controlled path (`files`, wal segment files, wal
///   sidecars) is confined to the snapshot directory by
///   [`reject_canonical_v2_manifest_path`] + [`confined_snapshot_path`] BEFORE any
///   read;
/// * wal segment files must be exactly the canonical writer names
///   `<wal-dir>/<seq:08>.wal` (sidecar `<seq:08>.meta`), each listing has at
///   least one segment (even `V*` = 0 produces an empty segment file), the
///   captured repos and listed wal dirs are a BIJECTION, each declared wal
///   directory is scanned for DIRECT numeric `.wal` files the listing omits
///   (other direct entries fall to the global file-set check), and
///   duplicate wal dirs / repos refuse.
pub(super) fn verify_manifest_v2(
    snapshot_dir: &Path,
    raw: &[u8],
) -> Result<(ManifestVerifyReport, online_manifest::ManifestV2), BackupError> {
    let decoded = decode_manifest_v2(raw)?;
    verify_decoded_manifest_v2(snapshot_dir, decoded, &|| false)
}

/// Decode-and-classify half of [`verify_manifest_v2`]: UTF-8 + JSON decode,
/// the ORIGINAL format-version behavior: decoded v1 manifests refuse with
/// [`BackupError::UnsupportedManifestFormatVersion`], while an unknown
/// version rejected by `manifest::decode` maps to `ManifestInvalid`; then
/// the online-section presence checks — ALL pure manifest-byte work, no
/// payload I/O, so a caller can refuse a candidate on `kind` (or format)
/// before any tree walk. Retention's kind-first preflight stops right here.
pub(super) fn decode_manifest_v2(raw: &[u8]) -> Result<online_manifest::ManifestV2, BackupError> {
    let text =
        std::str::from_utf8(raw).map_err(|e| BackupError::ManifestInvalid(format!("utf8: {e}")))?;
    let decoded =
        online_manifest::decode(text).map_err(|e| BackupError::ManifestInvalid(format!("{e}")))?;
    if decoded.format_version != online_manifest::MANIFEST_FORMAT_VERSION_V2 {
        return Err(BackupError::UnsupportedManifestFormatVersion {
            found: decoded.format_version,
            expected: online_manifest::MANIFEST_FORMAT_VERSION_V2,
        });
    }
    if matches!(decoded.kind, BackupKind::Online) {
        if decoded.consistency.is_none() {
            return Err(BackupError::MissingOnlineSection("consistency"));
        }
        if decoded.wal.is_none() {
            return Err(BackupError::MissingOnlineSection("wal"));
        }
    }
    Ok(decoded)
}

#[cfg(test)]
thread_local! {
    static WAL_REHASH_ACTIVE: Cell<bool> = const { Cell::new(false) };
}

#[cfg(test)]
struct WalRehashTestGuard;

#[cfg(test)]
impl WalRehashTestGuard {
    fn enter() -> Self {
        WAL_REHASH_ACTIVE.with(|active| active.set(true));
        Self
    }
}

#[cfg(test)]
impl Drop for WalRehashTestGuard {
    fn drop(&mut self) {
        WAL_REHASH_ACTIVE.with(|active| active.set(false));
    }
}

/// True only while the verifier is executing the WAL-loop re-hash. The
/// retention cancellation test uses this cfg(test) probe so the cancel fires
/// inside the exact re-hash site regardless of short non-EOF file reads.
#[cfg(test)]
pub(super) fn wal_rehash_active_for_test() -> bool {
    WAL_REHASH_ACTIVE.with(Cell::get)
}

/// Wrap a streaming-hash error with path context WITHOUT losing a typed
/// cancellation: `hash_file_streaming` marks the retention cancel hook's
/// abort with a [`HashCancelled`] payload inside the `io::Error`, and the
/// retention classifier (`retention/preflight.rs::classify_verifier_error`)
/// decides on exactly that payload. Re-wrapping the error into a fresh
/// string payload would turn a deliberate cancellation into an ordinary
/// incomplete read.
fn hash_error(context: String, error: std::io::Error) -> BackupError {
    if error
        .get_ref()
        .is_some_and(|inner| inner.is::<HashCancelled>())
    {
        return BackupError::Io(error);
    }
    BackupError::Io(std::io::Error::new(
        error.kind(),
        format!("{context}: {error}"),
    ))
}

/// Payload half of [`verify_manifest_v2`] over the ALREADY-decoded manifest
/// from [`decode_manifest_v2`] (one parse, no re-read): stamp agreement,
/// confinement, tree walk and re-hash, WAL rules, file-set check.
/// `cancel` is threaded into every streaming hash, which consults it once
/// per chunk; [`verify_manifest_v2`] passes `&|| false` (the public verify
/// paths are never cancelled), retention passes its own flag.
pub(super) fn verify_decoded_manifest_v2(
    snapshot_dir: &Path,
    decoded: online_manifest::ManifestV2,
    cancel: &dyn Fn() -> bool,
) -> Result<(ManifestVerifyReport, online_manifest::ManifestV2), BackupError> {
    check_stamp_agreement(snapshot_dir, decoded.format_epoch)?;

    // Per-repo store_dir paths are manifest-controlled: validate their
    // shape (relative, no `..`, no drive/UNC/ADS tricks) before anything
    // downstream (restore's replay) ever joins them under the snapshot.
    if let Some(consistency) = &decoded.consistency {
        for (repo, point) in &consistency.per_repo {
            reject_canonical_v2_manifest_path(&point.store_dir).map_err(|_| {
                BackupError::UnsafeStoreDir {
                    repo: repo.clone(),
                    path: point.store_dir.clone(),
                }
            })?;
        }
    }

    // File set: cache rule, duplicate rule, unsafe-path rule, per-entry
    // confinement and re-hash — the v1 rules, tightened with the strict
    // lexical name check and the per-component walk of
    // [`confined_snapshot_path`].
    if let Some(entry) = decoded
        .files
        .iter()
        .find(|entry| is_cache_manifest_path(&entry.path))
    {
        return Err(BackupError::CacheManifestPath(entry.path.clone()));
    }
    let mut accounted: shamir_collections::TFxSet<String> = new_fx_set_wc(decoded.files.len());
    let mut folded_paths = BTreeSet::new();
    let mut total_bytes = 0u64;
    for entry in &decoded.files {
        reject_canonical_v2_manifest_path(&entry.path)?;
        if accounted.contains(&entry.path) {
            return Err(BackupError::DuplicateManifestEntry(entry.path.clone()));
        }
        if !folded_paths.insert(entry.path.to_lowercase()) {
            return Err(BackupError::DuplicateManifestEntry(entry.path.clone()));
        }
        for control_name in [MANIFEST_FILE_NAME, online_manifest::BACKUP_META_FILE_NAME] {
            let folded = control_name.to_lowercase();
            if entry.path.to_lowercase() == folded && entry.path != control_name {
                return Err(BackupError::ManifestInvalid(format!(
                    "manifest entry \"{}\" reserves the root control-file name \"{}\" with a different spelling",
                    entry.path, folded
                )));
            }
        }
        let file_path = confined_snapshot_path(snapshot_dir, &entry.path)?;
        let (digest, actual_size) = hash_file_streaming(&file_path, cancel)
            .map_err(|e| hash_error(format!("manifest entry {}", entry.path), e))?;
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

    let mut on_disk = Vec::new();
    collect_relative_file_paths(snapshot_dir, snapshot_dir, &mut on_disk)?;
    let mut disk_identities = BTreeSet::new();
    for path in &on_disk {
        let folded = path.to_lowercase();
        if !disk_identities.insert(folded.clone()) {
            return Err(BackupError::DuplicateManifestEntry(folded));
        }
    }
    for entry in &decoded.files {
        if let Some(message) = exact_spelling_mismatch(&entry.path, &on_disk) {
            return Err(BackupError::ManifestInvalid(message));
        }
    }

    // WAL listing: canonical segment/sidecar names per repo, contiguity
    // from zero, per-segment re-hash, sidecar presence, bijection between
    // the captured repos and the listed wal dirs, no empty listing, no
    // omitted direct numeric `.wal` file on disk, no duplicate wal dir /
    // repo.
    if let Some(wal) = &decoded.wal {
        let mut seen_dirs = BTreeSet::new();
        let mut seen_repos = BTreeSet::new();
        for dir in &wal.dirs {
            reject_canonical_v2_manifest_path(&dir.dir)?;
            if !seen_dirs.insert(dir.dir.clone()) || !seen_repos.insert(dir.repo.clone()) {
                return Err(BackupError::DuplicateWalDir {
                    repo: dir.repo.clone(),
                    dir: dir.dir.clone(),
                });
            }
            if dir.segments.is_empty() {
                return Err(BackupError::WalListingEmpty {
                    repo: dir.repo.clone(),
                    dir: dir.dir.clone(),
                });
            }
            let mut declared_wal_names = BTreeSet::new();
            for (expected_seq, segment) in dir.segments.iter().enumerate() {
                let expected_seq = expected_seq as u64;
                if segment.seq != expected_seq {
                    return Err(BackupError::WalSeqGap {
                        repo: dir.repo.clone(),
                        expected: expected_seq,
                        found: segment.seq,
                    });
                }
                reject_canonical_v2_manifest_path(&segment.file)?;
                let expected_file = format!("{}/{:08}.wal", dir.dir, segment.seq);
                if segment.file != expected_file {
                    return Err(BackupError::NonCanonicalWalName {
                        found: segment.file.clone(),
                        expected: expected_file,
                    });
                }
                if !accounted.contains(&segment.file) {
                    return Err(BackupError::WalFileUnmanifested(segment.file.clone()));
                }
                declared_wal_names.insert(format!("{:08}.wal", segment.seq));
                let segment_path = confined_snapshot_path(snapshot_dir, &segment.file)?;
                // The test marker covers only the hash itself, not the digest
                // and sidecar checks that follow.
                let hashed = {
                    #[cfg(test)]
                    let _wal_rehash_test_guard = WalRehashTestGuard::enter();
                    hash_file_streaming(&segment_path, cancel)
                };
                let (digest, size) =
                    hashed.map_err(|e| hash_error(format!("wal segment {}", segment.file), e))?;
                let actual = hex::encode(digest);
                if actual != segment.sha256 || size != segment.size {
                    return Err(BackupError::ChecksumMismatch {
                        path: segment_path,
                        expected_sha256: segment.sha256.clone(),
                        expected_size_bytes: segment.size,
                        actual_sha256: actual,
                        actual_size_bytes: size,
                    });
                }
                if let Some(sidecar) = &segment.sidecar {
                    reject_canonical_v2_manifest_path(sidecar)?;
                    let expected_sidecar = format!("{}/{:08}.meta", dir.dir, segment.seq);
                    if *sidecar != expected_sidecar {
                        return Err(BackupError::NonCanonicalWalName {
                            found: sidecar.clone(),
                            expected: expected_sidecar,
                        });
                    }
                    if !accounted.contains(sidecar) {
                        return Err(BackupError::WalFileUnmanifested(sidecar.clone()));
                    }
                    declared_wal_names.insert(format!("{:08}.meta", segment.seq));
                    confined_snapshot_path(snapshot_dir, sidecar)?;
                }
            }
            // A direct undeclared file inside a declared wal directory
            // refuses ONLY when it is a numeric `.wal` file — exactly the
            // shape the engine accepts or diagnoses as a candidate
            // segment, so replay would diverge from the manifest listing.
            // Fully hashed ancillary files (`NNNNNNNN.meta.tmp`) and an
            // orphan `NNNNNNNN.meta` the WAL recognizes as harmless are
            // tolerated HERE; the global file-set check below still
            // refuses any of them the manifest does not account for. The
            // directory's components were symlink/dir-validated BEFORE
            // this listing; NotFound (fake-port fixtures without the
            // directory) means nothing on disk to scan.
            let wal_dir_abs = snapshot_dir.join(&dir.dir);
            if !wal_dir_scan_components(snapshot_dir, &dir.dir)? {
                continue;
            }
            let disk_entries = match fs::read_dir(&wal_dir_abs) {
                Ok(entries) => entries,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(BackupError::Io(e)),
            };
            for disk_entry in disk_entries {
                let disk_entry = disk_entry?;
                let name = disk_entry.file_name().to_string_lossy().into_owned();
                if !declared_wal_names.contains(&name) && is_numeric_wal_file_name(&name) {
                    return Err(BackupError::UndeclaredWalFile {
                        dir: dir.dir.clone(),
                        file: name,
                    });
                }
            }
        }
        // Bijection: every captured repo has exactly one wal dir listing
        // (checked above via `seen_repos`) and every listed dir belongs to
        // a captured repo whose wal directory is the canonical
        // `<store_dir>.shamirwal` sibling (replay derives exactly this
        // path, so a deviation must refuse here, not at replay). Applies
        // whenever both sections are present (an online manifest always
        // carries both).
        if let Some(consistency) = &decoded.consistency {
            for dir in &wal.dirs {
                let point = match consistency.per_repo.get(&dir.repo) {
                    Some(point) => point,
                    None => {
                        return Err(BackupError::WalListingForUncapturedRepo {
                            repo: dir.repo.clone(),
                        });
                    }
                };
                let expected = format!("{}.shamirwal", point.store_dir);
                if dir.dir != expected {
                    return Err(BackupError::WalListingPathMismatch {
                        repo: dir.repo.clone(),
                        found: dir.dir.clone(),
                        expected,
                    });
                }
            }
            for repo in consistency.per_repo.keys() {
                if !seen_repos.contains(repo) {
                    return Err(BackupError::WalListingMissingForRepo { repo: repo.clone() });
                }
            }
        }
    }

    // Every file physically present under snapshot_dir (except the
    // manifest itself and the backup-meta sidecar) must be accounted for.
    for rel in &on_disk {
        if rel == MANIFEST_FILE_NAME || rel == online_manifest::BACKUP_META_FILE_NAME {
            continue;
        }
        if !accounted.contains(rel) {
            return Err(BackupError::UnmanifestedFile(snapshot_dir.join(rel)));
        }
    }

    let report = ManifestVerifyReport {
        files_checked: decoded.files.len() as u64,
        total_bytes,
        kind: decoded.kind.clone(),
        format_epoch: decoded.format_epoch,
        binary_version: decoded.binary_version.clone(),
        consistency: decoded.consistency.clone(),
    };
    Ok((report, decoded))
}

/// Cross-check the manifest's `format_epoch` against the snapshot's copied
/// `format.stamp`, read through the real stamp codec (the same inspect path
/// the restore gate uses).
///
/// * valid stamp + manifest epoch: MUST match, else
///   [`BackupError::ManifestStampEpochMismatch`];
/// * valid stamp + manifest epoch `None`: refuses
///   ([`BackupError::MissingManifestEpoch`]) — the producer change wiring
///   the epoch into `build_v2` landed alongside this;
/// * ABSENT stamp: permitted regardless of the manifest epoch (today's
///   rule; the restore-side snapshot gate owns absent-stamp policy);
/// * corrupt / newer-layout stamp: refuses typed
///   ([`BackupError::InvalidSnapshotStamp`]), independent of the restore gate.
fn check_stamp_agreement(
    snapshot_dir: &Path,
    manifest_epoch: Option<u64>,
) -> Result<(), BackupError> {
    // The stamp location itself is fixed: only a REGULAR file is ever
    // inspected — a symlinked or special-file `format.stamp` refuses typed
    // (fail closed), a missing one falls through to the absent rule.
    let stamp_path = snapshot_dir.join(crate::format_stamp::STAMP_FILE_NAME);
    match fs::symlink_metadata(&stamp_path) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => {
            return Err(BackupError::InvalidSnapshotStamp(
                crate::format_stamp::StampError::Io {
                    dir: snapshot_dir.to_path_buf(),
                    source: e,
                },
            ));
        }
        Ok(meta) => {
            let ft = meta.file_type();
            if ft.is_symlink() || !ft.is_file() {
                return Err(BackupError::InvalidSnapshotStamp(
                    crate::format_stamp::StampError::Corrupt {
                        dir: snapshot_dir.to_path_buf(),
                        kind: crate::format_stamp::CorruptKind::BadBody,
                    },
                ));
            }
        }
    }
    match crate::format_stamp::inspect(snapshot_dir) {
        crate::format_stamp::StampState::Valid(stamp) => {
            let stamp_epoch = stamp.format_epoch();
            match manifest_epoch {
                Some(epoch) if epoch != stamp_epoch => {
                    Err(BackupError::ManifestStampEpochMismatch {
                        manifest_epoch: epoch,
                        stamp_epoch,
                    })
                }
                Some(_) => Ok(()),
                None => Err(BackupError::MissingManifestEpoch { stamp_epoch }),
            }
        }
        crate::format_stamp::StampState::Absent { .. } => Ok(()),
        crate::format_stamp::StampState::Corrupt { kind } => Err(
            BackupError::InvalidSnapshotStamp(crate::format_stamp::StampError::Corrupt {
                dir: snapshot_dir.to_path_buf(),
                kind,
            }),
        ),
        crate::format_stamp::StampState::NewerLayout { layout, epoch } => Err(
            BackupError::InvalidSnapshotStamp(crate::format_stamp::StampError::NewerLayout {
                dir: snapshot_dir.to_path_buf(),
                layout,
                epoch,
            }),
        ),
    }
}

/// Strict lexical check of a manifest-controlled path — the SINGLE v2
/// implementation, shared by the per-repo `store_dir` rule, the wal
/// dir/segment/sidecar rules and every `files` entry. Only canonical,
/// forward-slash separated, nonempty, normal component names are accepted.
/// Refuses (on every platform, so a manifest written for Windows cannot
/// smuggle anything past a Linux verifier and vice versa):
/// * any backslash `\` or colon `:` — kills Windows drive letters (`C:`),
///   `\\?\` / `\\.\` device prefixes and NTFS alternate-data streams
///   (`file.wal:stream`) in one rule;
/// * absolute / rooted paths (a leading `/` makes the first component
///   empty);
/// * empty, `.` and `..` components;
/// * components with a trailing `.` or trailing space (Windows strips
///   those, so `dir./x` and `dir /x` would silently resolve elsewhere).
///
/// Parent traversal keeps the existing [`BackupError::UnsafeManifestPath`]
/// typed refusals; every other noncanonical shape refuses as
/// [`BackupError::UnconfinedManifestPath`]. Callers proceed to
/// [`confined_snapshot_path`].
fn reject_canonical_v2_manifest_path(entry_path: &str) -> Result<(), BackupError> {
    let unconfined = || BackupError::UnconfinedManifestPath(entry_path.to_string());
    if entry_path.contains('\\') || entry_path.contains(':') {
        return Err(unconfined());
    }
    for component in entry_path.split('/') {
        if component == ".." {
            return Err(BackupError::UnsafeManifestPath(entry_path.to_string()));
        }
        let bad = component.is_empty()
            || component == "."
            || component.ends_with('.')
            || component.ends_with(' ');
        if bad {
            return Err(unconfined());
        }
    }
    Ok(())
}

/// `NNNNNNNN.wal`-style name: a nonempty all-digit stem with the `.wal`
/// extension — the only direct-entry shape the engine accepts or diagnoses
/// as a candidate segment.
fn is_numeric_wal_file_name(name: &str) -> bool {
    match name.strip_suffix(".wal") {
        Some(stem) => !stem.is_empty() && stem.bytes().all(|b| b.is_ascii_digit()),
        None => false,
    }
}

/// Before [`fs::read_dir`] on a declared wal directory: validate EVERY
/// component directory of the manifest path under the fixed snapshot with
/// `fs::symlink_metadata` — a symlinked or non-directory component refuses
/// BEFORE any listing ([`BackupError::UnconfinedManifestPath`]), other I/O
/// errors propagate. `Ok(false)` = the directory does not exist on this
/// snapshot (fake-port fixtures): nothing on disk to scan.
fn wal_dir_scan_components(snapshot_dir: &Path, dir: &str) -> Result<bool, BackupError> {
    let mut current = snapshot_dir.to_path_buf();
    for component in dir.split('/') {
        current.push(component);
        let meta = match fs::symlink_metadata(&current) {
            Ok(meta) => meta,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(e) => {
                return Err(BackupError::Io(std::io::Error::new(
                    e.kind(),
                    format!("manifest wal dir {}: {e}", dir),
                )))
            }
        };
        let ft = meta.file_type();
        if ft.is_symlink() || !ft.is_dir() {
            return Err(BackupError::UnconfinedManifestPath(dir.to_string()));
        }
    }
    Ok(true)
}

/// Resolve a v2 manifest-controlled path under `snapshot_dir`: the strict
/// lexical check ([`reject_canonical_v2_manifest_path`], every platform) and
/// then [`walk_confined`].
pub(super) fn confined_snapshot_path(
    snapshot_dir: &Path,
    entry_path: &str,
) -> Result<PathBuf, BackupError> {
    reject_canonical_v2_manifest_path(entry_path)?;
    walk_confined(snapshot_dir, entry_path)
}

/// Resolve a v1 manifest entry under `snapshot_dir`: the host-OS shape rule
/// ([`reject_v1_entry_shape`]) and then [`walk_confined`].
pub(super) fn confined_v1_snapshot_path(
    snapshot_dir: &Path,
    entry_path: &str,
) -> Result<PathBuf, BackupError> {
    reject_v1_entry_shape(entry_path)?;
    walk_confined(snapshot_dir, entry_path)
}

/// Lexical shape rule for a v1 entry, which a v1 backup writes WITHOUT the v2
/// naming rules. On Windows it is the strict v2 rule: a piece like `C:` would
/// reset, and a backslash bundle, the `PathBuf::push` walk. On Unix `:`, `\`
/// and trailing dots or spaces are literal name characters (a TLS file may be
/// named `tls:cert.pem` or `key.pem `), so only the component structure is
/// checked: no empty, `.` or `..` component.
fn reject_v1_entry_shape(entry_path: &str) -> Result<(), BackupError> {
    if cfg!(windows) {
        return reject_canonical_v2_manifest_path(entry_path);
    }
    for component in entry_path.split('/') {
        if component == ".." {
            return Err(BackupError::UnsafeManifestPath(entry_path.to_string()));
        }
        if component.is_empty() || component == "." {
            return Err(BackupError::UnconfinedManifestPath(entry_path.to_string()));
        }
    }
    Ok(())
}

/// Walk an already shape-checked manifest path under `snapshot_dir`, refusing
/// a symlink or a non-regular entry BEFORE the entry is opened for content
/// reads (check, then open: the window is not closed here). Every path
/// component is walked with `fs::symlink_metadata` on the fixed snapshot:
/// * a missing component is a plain io error (NotFound propagates);
/// * a SYMLINK component refuses ([`BackupError::UnconfinedManifestPath`]) —
///   a symlink planted inside the snapshot cannot retarget a later open;
/// * an intermediate component that is not a directory refuses;
/// * the final component must be a REGULAR file (devices, FIFOs, sockets
///   refuse).
///
/// For an unchanged snapshot, containment follows lexically: every component
/// is a normal name (no `..`, no separators; no drive or ADS pieces on
/// Windows), and every intermediate hop was verified to be a real directory
/// under the snapshot, so no per-file root canonicalize is needed.
fn walk_confined(snapshot_dir: &Path, entry_path: &str) -> Result<PathBuf, BackupError> {
    let mut current = snapshot_dir.to_path_buf();
    let components: Vec<&str> = entry_path.split('/').collect();
    let last = components.len().saturating_sub(1);
    for (index, component) in components.iter().enumerate() {
        current.push(component);
        let meta = fs::symlink_metadata(&current).map_err(|e| {
            BackupError::Io(std::io::Error::new(
                e.kind(),
                format!("manifest path {}: {e}", entry_path),
            ))
        })?;
        let ft = meta.file_type();
        if ft.is_symlink() {
            return Err(BackupError::UnconfinedManifestPath(entry_path.to_string()));
        }
        if index == last {
            if !ft.is_file() {
                return Err(BackupError::ManifestPathNotRegularFile(
                    entry_path.to_string(),
                ));
            }
        } else if !ft.is_dir() {
            return Err(BackupError::UnconfinedManifestPath(entry_path.to_string()));
        }
    }
    Ok(current)
}

/// Return a spelling diagnostic only when a case-folded disk identity exists.
pub(crate) fn exact_spelling_mismatch(entry_path: &str, on_disk: &[String]) -> Option<String> {
    if on_disk.iter().any(|path| path == entry_path) {
        return None;
    }
    on_disk
        .iter()
        .find(|path| path.to_lowercase() == entry_path.to_lowercase())
        .map(|actual| {
            format!(
                "manifest entry \"{}\" does not match its exact on-disk spelling \"{}\"",
                entry_path, actual
            )
        })
}

/// Recursively collect every regular file under `dir` (rooted at `root`) as
/// a `root`-relative, forward-slash path. THE shared walker: the parent
/// `offline.rs` v1 path reuses this instead of keeping its own duplicate.
pub(super) fn collect_relative_file_paths(
    root: &Path,
    dir: &Path,
    out: &mut Vec<String>,
) -> Result<(), BackupError> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        let ft = entry.file_type()?;
        if ft.is_dir() {
            collect_relative_file_paths(root, &path, out)?;
        } else if ft.is_file() {
            let mut rel = Vec::new();
            for component in path.strip_prefix(root).unwrap_or(&path).components() {
                let Some(name) = component.as_os_str().to_str() else {
                    // A JSON manifest cannot name this physically present file; refuse it as unmanifested.
                    return Err(BackupError::UnmanifestedFile(path.to_path_buf()));
                };
                rel.push(name.to_string());
            }
            out.push(rel.join("/"));
        }
    }
    Ok(())
}

/// Cheap format-version peek so v2 snapshots route to the v2 verifier
/// while v1 keeps its own manifest rules (design 4.5).
pub(super) fn peek_manifest_format_version(raw: &[u8]) -> Option<u32> {
    let value: serde_json::Value = serde_json::from_slice(raw).ok()?;
    value
        .get("format_version")?
        .as_u64()
        .map(|version| version as u32)
}

/// The exact decoded manifest a successful verification already produced,
/// handed to the caller without a second parse of `manifest.json`.
/// `pub(super)` — restore.rs is the intended consumer, via the offline
/// re-export. One enum per version so consumers derive the version's
/// rules (inventory exemptions) from the variant, never a second decode.
#[derive(Debug)]
pub(in crate::sync::durability::backup) enum VerifiedManifest {
    /// A v1 (offline stop-and-copy) manifest.
    V1(super::Manifest),
    /// A v2 (online) manifest.
    V2(online_manifest::ManifestV2),
}

impl VerifiedManifest {
    /// The manifest's file list, verbatim as verified.
    pub(in crate::sync::durability::backup) fn files(&self) -> &[super::ManifestFileEntry] {
        match self {
            VerifiedManifest::V1(manifest) => &manifest.files,
            VerifiedManifest::V2(manifest) => &manifest.files,
        }
    }

    /// Whether this manifest's VERSION exempts the root `backup.meta.json`
    /// sidecar from the on-disk inventory (v2: yes, v1: no) — the same
    /// rule each verifier applies, derived here from the variant itself
    /// so the restore-side ledger can mirror its verifier exactly.
    pub(in crate::sync::durability::backup) fn allows_backup_meta(&self) -> bool {
        matches!(self, VerifiedManifest::V2(_))
    }
}

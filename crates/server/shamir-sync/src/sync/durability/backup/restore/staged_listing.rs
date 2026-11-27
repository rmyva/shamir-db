//! Staged presence re-check (restore step 3b): after the source copy, before
//! replay / invalidation / swap, every verified-manifest entry must exist as
//! a regular file under the staged copy. The copy walks the SOURCE tree, not
//! the verified list, so a deletion that raced the copy (e.g. the snapshot
//! removed mid-restore) would otherwise publish an INCOMPLETE target with no
//! error. The staged file is opened once and its bytes are hashed against
//! the verified manifest before publication.
//! The typed refusals carry ONLY the manifest-relative file name — never
//! the joined staging path, which would leak the restore's absolute scratch
//! root into CLI output. Absence ([`RestoreError::StagedFileMissing`]),
//! present-but-different ([`RestoreError::StagedFilePresentButDifferent`])
//! and present-but-not-regular ([`RestoreError::StagedFileNotARegularFile`])
//! are truthfully distinct (round-2 finding 5).

use std::fs;
use std::io::{ErrorKind, Read};
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

use crate::sync::durability::backup::open_plain_snapshot_file;
use crate::sync::durability::backup::ManifestFileEntry;

use super::RestoreError;
pub(super) fn has_external_staging_location(data_dir: &Path) -> bool {
    let Ok(target) = canonicalize_target(data_dir) else {
        return false;
    };
    let Some(parent) = target.parent() else {
        return false;
    };
    if parent == target {
        return false;
    }
    let stage_sibling = parent.join(format!("{}.restore_tmp_probe", target.file_name().unwrap_or_default().to_string_lossy()));
    !stage_sibling.starts_with(&target)
}

pub(super) fn canonicalize_target(path: &Path) -> std::io::Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    let mut candidate = absolute.as_path();
    let mut suffix = Vec::new();
    let canonical = loop {
        match fs::canonicalize(candidate) {
            Ok(canonical) => break canonical,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let Some(name) = candidate.file_name() else {
                    return Err(error);
                };
                suffix.push(name.to_os_string());
                candidate = candidate.parent().ok_or(error)?;
            }
            Err(error) => return Err(error),
        }
    };
    let mut resolved = canonical;
    for component in suffix.iter().rev() {
        resolved.push(component);
    }
    #[cfg(windows)]
    let resolved = {
        let text = resolved.to_string_lossy();
        let stripped = text.strip_prefix("\\\\?\\").unwrap_or(&text);
        PathBuf::from(stripped)
    };
    Ok(resolved)
}


/// Verify that every manifest-listed file exists as a regular file under the
/// staged temp dir and re-hashes to its verified entry (the second line of
/// defense behind the copy gate's own in-pass hashing). First failure is
/// typed and truthful: [`RestoreError::StagedFileMissing`] ONLY for a
/// genuinely absent entry, [`RestoreError::StagedFileNotARegularFile`] for
/// a link/directory refused by the no-follow open,
/// [`RestoreError::StagedFilePresentButDifferent`] for a size/sha256
/// mismatch vs the manifest, [`RestoreError::ManifestVerification`] for a
/// hostile manifest-controlled path, [`RestoreError::Io`] for any other
/// metadata error (never claim "missing" without proof).
pub(super) fn verify_staged_files(
    temp_dir: &Path,
    files: &[ManifestFileEntry],
) -> Result<(), RestoreError> {
    for entry in files {
        let staged = staged_path(temp_dir, &entry.path)?;
        // Presence probe FIRST, so "not a regular file" (link, directory)
        // is truthfully distinguished from absence (round-2 finding 5).
        // The staged copy was written by this same restore and is not
        // concurrently mutated, so the probe-to-open gap here is not a
        // hostile TOCTOU surface — and the no-follow open below still
        // type-checks the object actually opened.
        let present = match fs::symlink_metadata(&staged) {
            Ok(meta) => meta.is_file(),
            // A vanished parent directory surfaces as NotADirectory on both
            // platforms; anything else is not proof of absence.
            Err(e) if e.kind() == ErrorKind::NotFound || e.kind() == ErrorKind::NotADirectory => {
                return Err(RestoreError::StagedFileMissing(PathBuf::from(&entry.path)))
            }
            Err(e) => return Err(RestoreError::Io(e)),
        };
        if !present {
            return Err(RestoreError::StagedFileNotARegularFile(entry.path.clone()));
        }
        // ONE no-follow open of the staged leaf: the handle whose bytes are
        // hashed is the handle whose metadata was type-checked, so a swap
        // between a metadata probe and a second open cannot widen what gets
        // published. The error names the MANIFEST-RELATIVE entry path only —
        // the joined staging path would expose the absolute scratch root.
        match open_plain_snapshot_file(&staged) {
            Ok(mut file) => {
                let mut hasher = Sha256::new();
                let mut size = 0u64;
                let mut buffer = [0u8; 64 * 1024];
                loop {
                    let n = file.read(&mut buffer)?;
                    if n == 0 {
                        break;
                    }
                    hasher.update(&buffer[..n]);
                    size += n as u64;
                }
                let actual_sha256 = hex::encode(hasher.finalize());
                if size != entry.size_bytes || actual_sha256 != entry.sha256 {
                    return Err(RestoreError::StagedFilePresentButDifferent {
                        path: entry.path.clone(),
                        expected_sha256: entry.sha256.clone(),
                        expected_size_bytes: entry.size_bytes,
                        actual_sha256,
                        actual_size_bytes: size,
                    });
                }
            }
            // Refused by the no-follow open (reparse point / non-regular
            // leaf that appeared after the probe — including a directory,
            // which opens successfully on Unix and is refused by the
            // regularity check inside the open).
            Err(e) if e.kind() == ErrorKind::InvalidData => {
                return Err(RestoreError::StagedFileNotARegularFile(entry.path.clone()))
            }
            Err(e) if e.kind() == ErrorKind::NotFound || e.kind() == ErrorKind::NotADirectory => {
                return Err(RestoreError::StagedFileMissing(PathBuf::from(&entry.path)))
            }
            Err(e) => return Err(RestoreError::Io(e)),
        }
    }
    Ok(())
}

/// Join a normalized manifest entry path under the staged temp dir,
/// re-deriving the safety the verifiers enforce: strict `/` split (empty and
/// `.` / `..` components refused, so absolute paths cannot slip through),
/// and on Windows additionally backslash separators, drive/UNC prefixes and
/// NTFS alternate-data-stream selectors (a `:` in a component). A literal
/// backslash is a legal Unix file name and stays allowed there.
fn staged_path(temp_dir: &Path, entry_path: &str) -> Result<PathBuf, RestoreError> {
    let refuse = || {
        RestoreError::ManifestVerification(super::super::BackupError::ManifestInvalid(format!(
            "unsafe manifest entry path: {entry_path}"
        )))
    };
    let mut staged = temp_dir.to_path_buf();
    for component in entry_path.split('/') {
        if component.is_empty() || component == "." || component == ".." {
            return Err(refuse());
        }
        if cfg!(windows) && (component.contains('\\') || component.contains(':')) {
            return Err(refuse());
        }
        staged.push(component);
    }
    Ok(staged)
}

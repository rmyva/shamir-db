//! Restore-side copy gate (round-2 finding 1): the ledger binding the step-3
//! walk to the VERIFIED manifest, and its typed refusals.

use shamir_collections::TMap;
use std::fs::{self, File};
use std::io::{self, Read, Write};
#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

use super::super::online::manifest::BACKUP_META_FILE_NAME;

use super::{ManifestFileEntry, MANIFEST_FILE_NAME};

// Mirrors offline.rs's hash-stream buffer.
const COPY_STREAM_BUFFER_SIZE: usize = 1024 * 1024;

/// Create the destination handle of one streamed copy. `fs::copy` carried the
/// source permissions over; the streamed copy keeps that behaviour: on Unix the
/// file is CREATED with the source's permission bits (so a private file is never
/// briefly readable under the process umask) and the caller re-applies the
/// exact permissions on the open handle after the bytes are written.
pub(crate) fn create_copy_output(path: &Path, source: &fs::Permissions) -> io::Result<File> {
    let mut options = fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    options.mode(source.mode());
    #[cfg(not(unix))]
    let _ = source;
    options.open(path)
}

/// Typed refusal of the RESTORE-side copy gate: the walk refuses any
/// regular file the verified manifest does not cover, and every copied
/// listed file is checked against its verified entry before it counts as
/// copied. `backup()` passes no ledger and can only ever see [`Self::Io`].
#[derive(Debug)]
pub(crate) enum CopyGateError {
    Io(std::io::Error),
    /// The no-follow open of a source leaf refused it as a symlink /
    /// reparse point / directory / other non-regular object. Only ever
    /// produced where a ledger exists (the restore path); the backup path
    /// maps the same open failure to [`Self::Io`]. Manifest-relative
    /// slash-separated path only — never the staging root.
    NotARegularFile {
        path: String,
    },
    /// A regular file present in the snapshot that neither the verified
    /// manifest lists nor is one of the format's root control files.
    /// Manifest-relative slash-separated path only — never the staging root.
    UnmanifestedFile(String),
    /// A manifest-listed file's copied bytes differ from its verified entry
    /// (size and/or sha256). Manifest-relative path only.
    ListedFileMismatch {
        path: String,
        expected_sha256: String,
        expected_size_bytes: u64,
        actual_sha256: String,
        actual_size_bytes: u64,
    },
}

/// What the verified manifest allows the restore walk to copy: the exact
/// set of manifest-listed relative paths, rooted at the snapshot directory
/// the verification consumed, PLUS the version's root control files.
/// Reuses the verifiers' inventory rule — every on-disk file must be
/// accounted for by `manifest.files` (which for v2 already includes the
/// wal segments and their `.meta` sidecars) EXCEPT the root control
/// files: `manifest.json` for EVERY version, plus `backup.meta.json` for
/// v2 only (v1's verifier refuses a root `backup.meta.json` as
/// unmanifested, so a v1 ledger must refuse it in the walk too). The
/// version flag is derived from the [`VerifiedManifest`] variant at the
/// call site — no second source of truth. Wal directories need no extra
/// allowance: the verifier refuses any unaccounted file in them, so
/// everything on disk there is listed.
pub(crate) struct RestoreCopyLedger {
    root: PathBuf,
    listed: TMap<String, ManifestFileEntry>,
    /// Whether the manifest version's verifier exempts the root
    /// `backup.meta.json` sidecar from its inventory (v2: yes, v1: no).
    allows_backup_meta: bool,
}

impl RestoreCopyLedger {
    /// Build the ledger from the VERIFIED manifest over `root` — the same
    /// snapshot directory verification consumed, so the walk and the
    /// verification see one path identity. `allows_backup_meta` must come
    /// from the SAME verified manifest's version (v2: `true`, v1:
    /// `false`) — the caller derives it from the variant it holds, so the
    /// ledger's allow-set always equals that verifier's.
    pub(crate) fn new(root: &Path, files: &[ManifestFileEntry], allows_backup_meta: bool) -> Self {
        RestoreCopyLedger {
            root: root.to_path_buf(),
            listed: files
                .iter()
                .map(|entry| (entry.path.clone(), entry.clone()))
                .collect(),
            allows_backup_meta,
        }
    }

    /// Manifest-relative, slash-normalized form of an absolute path this
    /// walk produced (every walked path is `root`-prefixed by
    /// construction). Exact spelling: the v2 verifier already refuses
    /// case-folded duplicates, so no folding here.
    pub(crate) fn relative(&self, path: &Path) -> String {
        path.strip_prefix(&self.root)
            .unwrap_or(path)
            .components()
            .map(|c| c.as_os_str().to_string_lossy().into_owned())
            .collect::<Vec<_>>()
            .join("/")
    }

    /// The version-aware root control-file set: `manifest.json` for every
    /// version, plus `backup.meta.json` only when the verified manifest is
    /// v2.
    fn is_root_control_file(&self, rel: &str) -> bool {
        rel == MANIFEST_FILE_NAME || (self.allows_backup_meta && rel == BACKUP_META_FILE_NAME)
    }
}

/// Stream `input` into `output`, hashing the bytes ACTUALLY WRITTEN in
/// the same pass (finding 1a — no second read a racing writer could
/// interleave with), then gate against the verified ledger entry BEFORE
/// returning success (finding 1a) or refuse an unlisted regular file
/// (finding 1b).
pub(crate) fn stream_copy(
    input: &mut File,
    output: &mut File,
    opened_size: u64,
    gate: Option<(&RestoreCopyLedger, &str)>,
) -> Result<(u64, String), CopyGateError> {
    let mut hasher = gate.is_some().then(Sha256::new);
    let mut buffer = vec![0u8; COPY_STREAM_BUFFER_SIZE];
    let mut n = 0u64;
    loop {
        let read = input.read(&mut buffer).map_err(CopyGateError::Io)?;
        if read == 0 {
            break;
        }
        if let Some(hasher) = hasher.as_mut() {
            hasher.update(&buffer[..read]);
        }
        output
            .write_all(&buffer[..read])
            .map_err(CopyGateError::Io)?;
        n += read as u64;
    }
    let actual_sha256 = hasher
        .map(|hasher| hex::encode(hasher.finalize()))
        .unwrap_or_default();
    if let Some((ledger, rel)) = gate {
        if let Some(entry) = ledger.listed.get(rel) {
            // Compare BEFORE the caller counts this file as copied (1a)
            // and BEFORE the generic opened-size check (finding: a listed
            // file whose LENGTH changed must report the truthful
            // ListedFileMismatch, not generic I/O) — the entry comparison
            // fully subsumes the length consistency check for listed
            // files.
            if n != entry.size_bytes || actual_sha256 != entry.sha256 {
                return Err(CopyGateError::ListedFileMismatch {
                    path: rel.to_string(),
                    expected_sha256: entry.sha256.clone(),
                    expected_size_bytes: entry.size_bytes,
                    actual_sha256,
                    actual_size_bytes: n,
                });
            }
        } else {
            // Unlisted (control or rogue): the opened-size check is the
            // only consistency check available here.
            if n != opened_size {
                return Err(CopyGateError::Io(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "snapshot file changed while copying",
                )));
            }
            if !ledger.is_root_control_file(rel) {
                // Finding 1b: an unlisted regular file is never published.
                return Err(CopyGateError::UnmanifestedFile(rel.to_string()));
            }
        }
    } else if n != opened_size {
        // Backup path (no ledger): same generic consistency check.
        return Err(CopyGateError::Io(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "snapshot file changed while copying",
        )));
    }
    Ok((n, actual_sha256))
}

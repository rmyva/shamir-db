//! Snapshot-kind classification (restore.rs step 0), split out of
//! `restore.rs` along its only seam: the raw-manifest header read that
//! chooses the online vs legacy pipeline BEFORE verification runs.

use std::io::Read;
use std::path::Path;

use super::super::online::manifest as online_manifest;
use super::super::{self as backup, BackupError, MANIFEST_FILE_NAME};
use super::RestoreError;

/// Snapshot kind as classified from the raw manifest header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SnapshotKind {
    /// v2 manifest with `kind = "online"`: run the online pipeline, bypass
    /// the supplied probe.
    Online,
    /// v1 manifest, or a v2 manifest with `kind = "offline"`: the legacy
    /// pipeline, with the supplied probe moved AFTER the staged presence
    /// re-check (finding 6).
    Legacy,
}

/// Fallible kind classification from the raw manifest header. Reuses the
/// verify-side error semantics ([`BackupError::ManifestMissing`],
/// [`BackupError::Io`], [`BackupError::ManifestInvalid`] wrapped in
/// [`RestoreError::ManifestVerification`]) so an unclassifiable or malformed
/// snapshot refuses BEFORE any probe callback runs — in particular before
/// the legacy server liveness probe, which MUTATES the target on refusal
/// and must never fire for a corrupt source (P1). Only a header that is
/// both readable and recognized (v1, or v2 with a known `kind`) yields a
/// classification; the manifest actually consumed later still comes
/// exclusively from `verify_manifest`/`verify_manifest_for_restore`.
///
/// The classifier is the ONLY place outside the verifiers that opens the
/// root manifest, so its no-follow open refusal carries its OWN typed
/// variant ([`RestoreError::ControlFileNotRegular`]) — distinguishable
/// from the verifier's `ManifestVerification` refusal of the same shape
/// (mutant isolation for the control-file link regressions).
pub(super) fn classify_snapshot_kind(from: &Path) -> Result<SnapshotKind, RestoreError> {
    let manifest_path = from.join(backup::MANIFEST_FILE_NAME);
    let refuse = |e| RestoreError::ManifestVerification(e);
    let mut file = backup::open_plain_snapshot_file(&manifest_path).map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            refuse(BackupError::ManifestMissing(manifest_path.clone()))
        } else if e.kind() == std::io::ErrorKind::InvalidData {
            // The no-follow open refused the link/object itself (not a
            // regular file, or a reparse point). Manifest-relative path.
            RestoreError::ControlFileNotRegular {
                path: MANIFEST_FILE_NAME.to_string(),
            }
        } else {
            refuse(BackupError::Io(e))
        }
    })?;
    let mut raw = String::new();
    file.read_to_string(&mut raw)
        .map_err(|e| refuse(BackupError::Io(e)))?;
    let value: serde_json::Value = serde_json::from_str(&raw).map_err(|e| {
        refuse(BackupError::ManifestInvalid(format!(
            "decode {}: {e}",
            manifest_path.display()
        )))
    })?;
    let version = value
        .get("format_version")
        .and_then(|v| v.as_u64())
        .ok_or_else(|| {
            refuse(BackupError::ManifestInvalid(
                "manifest is missing a numeric format_version".into(),
            ))
        })?;
    match version {
        1 => Ok(SnapshotKind::Legacy),
        v if v == online_manifest::MANIFEST_FORMAT_VERSION_V2 as u64 => {
            // Match the production `ManifestV2` decoder's compatibility rule:
            // an ABSENT `kind` field decodes there to `Offline` (serde
            // `#[serde(default = "offline_kind")]`), so classification must
            // treat it the same way and stay on the legacy path. A PRESENT
            // but non-string or unknown `kind` (including JSON null) is not
            // something the decoder tolerates for routing, so refuse —
            // fail-closed before any probe callback.
            match value.get("kind") {
                Some(serde_json::Value::String(s)) if s == "online" => Ok(SnapshotKind::Online),
                Some(serde_json::Value::String(s)) if s == "offline" => Ok(SnapshotKind::Legacy),
                None => Ok(SnapshotKind::Legacy),
                other => Err(refuse(BackupError::ManifestInvalid(format!(
                    "v2 manifest has no known kind (online|offline), found: {other:?}"
                )))),
            }
        }
        found => Err(refuse(BackupError::ManifestInvalid(format!(
            "decode {}: unsupported manifest format_version: found {found}",
            manifest_path.display()
        )))),
    }
}

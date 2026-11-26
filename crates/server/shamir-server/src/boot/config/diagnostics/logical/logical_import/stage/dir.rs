//! Attempt-owned sibling stage on the same volume as the target, mirroring
//! the `logical_dump` staging conventions with a distinct prefix. A stale
//! stage from a crashed attempt is removed only while the import lock is
//! held. Publication is ONE same-volume rename of the stage onto the absent
//! (or just-removed empty) target; a platform or volume that cannot do it
//! fails closed with a typed error.

use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use tempfile::Builder;
use tokio_util::sync::CancellationToken;

use super::super::error::LogicalImportError;

const STAGE_PREFIX: &str = ".logical-import-";

/// Rename-retry policy: the deterministic close (graceful engine shutdown,
/// store-release barrier, receipt sync) happens BEFORE the rename, so any
/// remaining `ERROR_ACCESS_DENIED` (5) / `ERROR_SHARING_VIOLATION` (32) is a
/// straggler closing its last handle. Ten attempts with a 250 ms backoff
/// bound the wait at ≈2.25 s; every other error and every non-Windows
/// platform fails on the first attempt, immediately.
pub(crate) const RENAME_ATTEMPTS: u32 = 10;
const RENAME_BACKOFF: Duration = Duration::from_millis(250);

pub(crate) struct ImportStage {
    path: PathBuf,
    published: bool,
}

impl ImportStage {
    /// Create a uniquely named stage directory beside the target. Requires
    /// the import lock to be held (so stale-stage cleanup is exclusive).
    pub(crate) fn prepare(target: &Path) -> Result<Self, LogicalImportError> {
        let parent = target
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        remove_stale(parent)?;
        let directory = Builder::new()
            .prefix(STAGE_PREFIX)
            .tempdir_in(parent)
            .map_err(|e| stage_err(format!("cannot create stage in {}: {e}", parent.display())))?;
        // Detach: the stage must survive until publish/discard; TempDir's
        // automatic cleanup would race the rename commit point.
        let path = directory.keep();
        Ok(Self {
            path,
            published: false,
        })
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    /// The rename is the publication commit point. On Windows the target
    /// must not exist; an existing empty target is rechecked and removed
    /// immediately before the rename while the caller holds the sibling lock.
    /// Transient Windows sharing errors are retried a bounded number of
    /// times (see [`RENAME_ATTEMPTS`]); the retry never replaces a target
    /// that appeared meanwhile — that fails immediately.
    pub(crate) async fn publish(
        mut self,
        target: &Path,
        cancel: &CancellationToken,
    ) -> Result<(), LogicalImportError> {
        let source = self.path.clone();
        remove_empty_target(target)?;
        let mut attempts = 0u32;
        let result = rename_with_retry(
            || {
                attempts += 1;
                fs::rename(&source, target)
            },
            target,
            cancel,
        )
        .await;
        match result {
            Ok(()) => {
                self.published = true;
                Ok(())
            }
            Err(e) => Err(LogicalImportError::Publish(format!(
                "same-volume rename of {} onto {} failed after {attempts} attempt(s): {e}",
                self.path.display(),
                target.display()
            ))),
        }
    }

    /// Remove the stage after a failed or cancelled attempt. Nothing of the
    /// attempt is left behind (the lock file is removed by `ImportLock`).
    pub(crate) fn discard(self) -> Result<(), LogicalImportError> {
        if self.published {
            return Ok(());
        }
        fs::remove_dir_all(&self.path)
            .map_err(|e| stage_err(format!("cannot discard stage {}: {e}", self.path.display())))
    }
}

/// Refuse an existing non-empty target (callers inspect the target only
/// after acquiring the lock). An existing empty directory remains intact
/// until publication, preserving it if pre-publication work fails.
pub(crate) fn prepare_target_name(target: &Path) -> Result<(), LogicalImportError> {
    match fs::symlink_metadata(target) {
        Err(_) => Ok(()),
        Ok(meta) if meta.is_dir() => {
            if has_entry(target)? {
                return Err(LogicalImportError::Target(format!(
                    "target {} exists and is not empty",
                    target.display()
                )));
            }
            Ok(())
        }
        Ok(_) => Err(LogicalImportError::Target(format!(
            "target {} exists and is not a directory",
            target.display()
        ))),
    }
}

fn remove_empty_target(target: &Path) -> Result<(), LogicalImportError> {
    match fs::symlink_metadata(target) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(LogicalImportError::Target(format!(
            "cannot inspect existing target {}: {error}",
            target.display()
        ))),
        Ok(meta) if meta.is_dir() => {
            if has_entry(target)? {
                return Err(LogicalImportError::Target(format!(
                    "target {} is no longer empty",
                    target.display()
                )));
            }
            fs::remove_dir(target).map_err(|e| {
                LogicalImportError::Target(format!(
                    "cannot remove empty target {}: {e}",
                    target.display()
                ))
            })
        }
        Ok(_) => Err(LogicalImportError::Target(format!(
            "target {} exists and is not a directory",
            target.display()
        ))),
    }
}

fn has_entry(target: &Path) -> Result<bool, LogicalImportError> {
    let mut entries = fs::read_dir(target).map_err(|e| {
        LogicalImportError::Target(format!(
            "cannot inspect existing target {}: {e}",
            target.display()
        ))
    })?;
    match entries.next() {
        Some(entry) => entry.map(|_| true).map_err(|e| {
            LogicalImportError::Target(format!(
                "cannot inspect existing target {}: {e}",
                target.display()
            ))
        }),
        None => Ok(false),
    }
}

fn remove_stale(parent: &Path) -> Result<(), LogicalImportError> {
    let entries = fs::read_dir(parent).map_err(|e| {
        stage_err(format!(
            "cannot read stage parent {}: {e}",
            parent.display()
        ))
    })?;
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with(STAGE_PREFIX) {
            let path = entry.path();
            if path.is_dir() {
                fs::remove_dir_all(&path).map_err(|e| {
                    stage_err(format!("cannot remove stale stage {}: {e}", path.display()))
                })?;
            }
        }
    }
    Ok(())
}

/// `true` only for the transient Windows sharing errors a directory rename
/// hits while a handle inside is still open: `ERROR_ACCESS_DENIED` (5) and
/// `ERROR_SHARING_VIOLATION` (32). Every other error and every non-Windows
/// platform fails immediately.
fn is_transient_sharing_error(e: &std::io::Error) -> bool {
    cfg!(windows) && matches!(e.raw_os_error(), Some(5) | Some(32))
}

/// Bounded retry of one same-volume rename, injectable for tests. Retries
/// ONLY transient Windows sharing errors, with a 250 ms backoff and the
/// cancellation token observed between attempts; a target that appeared
/// meanwhile and an exhausted budget fail with the LAST raw error (the
/// caller names the attempts). The rename is never re-run against an
/// existing target, so the no-replace guarantee is preserved.
pub(crate) async fn rename_with_retry<F>(
    mut rename: F,
    target: &Path,
    cancel: &CancellationToken,
) -> Result<(), std::io::Error>
where
    F: FnMut() -> std::io::Result<()>,
{
    let mut attempt = 1u32;
    loop {
        let error = match rename() {
            Ok(()) => return Ok(()),
            Err(e) => e,
        };
        if !is_transient_sharing_error(&error) {
            return Err(error);
        }
        // No-replace: if the target showed up, the next attempt must not
        // run — fail with the original error immediately.
        if target.symlink_metadata().is_ok() {
            return Err(error);
        }
        if attempt >= RENAME_ATTEMPTS {
            return Err(error);
        }
        attempt += 1;
        tokio::select! {
            () = tokio::time::sleep(RENAME_BACKOFF) => {}
            () = cancel.cancelled() => return Err(error),
        }
    }
}

fn stage_err(message: String) -> LogicalImportError {
    LogicalImportError::Stage(message)
}

//! Server-facing restore wrapper with a read-only liveness probe.

pub use shamir_sync::restore::*;

use std::fs::File;
use std::io::ErrorKind;
use std::path::Path;

/// Restore `from` into `data_dir` with a read-only liveness probe unless forced.
/// Pre-publication refusals preserve the target byte-for-byte; swap failure
/// variants are separate, and `SwapPartialFailure` may leave the path absent.
pub fn restore(from: &Path, data_dir: &Path, force: bool) -> Result<RestoreReport, RestoreError> {
    restore_with_probe(from, data_dir, force, probe_target_stopped_read_only)
}

/// Probe server lock artifacts using read-only handles; never initialize metadata.
fn probe_target_stopped_read_only(data_dir: &Path) -> Result<(), RestoreError> {
    const LOCK_FILES: [&str; 3] = [
        ".shamir.lock",
        "server_meta/lock",
        "shamir_db_meta.redb/lock",
    ];
    let mut held = Vec::new();
    for relative in LOCK_FILES {
        let path = data_dir.join(relative);
        let file = match File::open(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == ErrorKind::NotFound => continue,
            Err(error) => return Err(RestoreError::Io(error)),
        };
        match file.try_lock_shared() {
            Ok(()) => held.push(file),
            Err(std::fs::TryLockError::WouldBlock) => {
                return Err(RestoreError::ServerRunning(data_dir.to_path_buf()))
            }
            Err(std::fs::TryLockError::Error(error)) => return Err(RestoreError::Io(error)),
        }
    }
    drop(held);
    Ok(())
}

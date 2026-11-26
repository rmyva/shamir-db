//! Commit policy and typed WAL outcome classification.
use super::*;

use shamir_wal::WalAppendError;

use crate::repo::journal::backend::durability_handle::{wal_durability_for, DurabilityHandle};

#[cfg(any(test, feature = "test-util"))]
pub static TEST_SYNCED_WAL_REPO: std::sync::OnceLock<String> = std::sync::OnceLock::new();

pub(crate) fn commit_wal_durability(
    durability: &DurabilityHandle,
    _repo_name: &str,
) -> shamir_wal::WalDurability {
    #[cfg(test)]
    if TEST_SYNCED_WAL_REPO
        .get()
        .is_some_and(|name| name == _repo_name)
    {
        return shamir_wal::WalDurability::Synced;
    }
    wal_durability_for(durability)
}

/// `Ok(bool)` is the `durability_failed` flag (`WrittenNotDurable` = written
/// at level 2 but not fsynced). The second tuple element is TRUE only when
/// the ticket outcome PROVES the frame was never written (`NotWritten`) —
/// the caller settles the version for R-contiguity at that point (B3).
/// `Unknown`/`Indeterminate` report `WalOutcomeUnknown { commit_version }`:
/// the frame was queued, so the attempt may have survived — never aborted.
pub fn wal_completion(
    gate: &shamir_tx::RepoTxGate,
    commit_version: u64,
    result: Result<(), WalAppendError>,
) -> (Result<bool, TxError>, bool) {
    match result {
        Ok(()) => (Ok(false), false),
        Err(WalAppendError::WrittenNotDurable) => (Ok(true), false),
        Err(WalAppendError::NotWritten) => (
            Err(TxError::Storage(WalAppendError::NotWritten.into())),
            true,
        ),
        Err(WalAppendError::Unknown | WalAppendError::Indeterminate) => {
            gate.set_requires_reopen();
            (Err(TxError::WalOutcomeUnknown { commit_version }), false)
        }
        Err(_) => {
            gate.set_requires_reopen();
            (Err(TxError::WalRequiresReopen), false)
        }
    }
}

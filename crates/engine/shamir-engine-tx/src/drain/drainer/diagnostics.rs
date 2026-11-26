use std::sync::atomic::Ordering;
use std::sync::Arc;

use shamir_storage::error::DbError;
use shamir_wal::{WalEntryV2, WalOpV2};

use super::Drainer;
use crate::drainer::orphan_repair;

impl Drainer {
    /// Best-effort telemetry reads; never initializes repository state.
    pub fn enrich_diagnostics(&self, snapshot: &mut shamir_tx::GateDiagnostics) {
        snapshot.orphan_replay_attempts_total =
            self.orphan_replay_attempts_total.load(Ordering::Relaxed);
        snapshot.orphan_replay_failures_total =
            self.orphan_replay_failures_total.load(Ordering::Relaxed);
        snapshot.orphan_replay_consecutive_failures = self
            .orphan_replay_consecutive_failures
            .load(Ordering::Relaxed);
        snapshot.orphan_replay_last_error = self
            .orphan_replay_last_error
            .load_full()
            .map(|error| (*error).clone());
        let (repaired, gaps) = self.orphan_repair_metrics.snapshot();
        log::debug!("orphan journal repair totals: repaired={repaired}, gaps={gaps}");
    }

    pub(in crate::drain::drainer) fn record_orphan_failure(
        &self,
        failed: &mut bool,
        error: impl std::fmt::Display,
    ) {
        orphan_repair::record_orphan_failure(self, failed, error);
    }

    #[cfg(test)]
    pub(crate) fn record_orphan_failure_for_test(&self, failed: &mut bool, error: &str) {
        self.record_orphan_failure(failed, error);
    }

    pub(super) fn record_orphan_table_failure(
        &self,
        entries: &[Arc<WalEntryV2>],
        orphan: Option<u64>,
        table: u64,
        failed: &mut bool,
        error: &DbError,
    ) {
        if orphan.is_some_and(|version| {
            entries.iter().any(|entry| {
                entry.commit_version == version
                    && entry.ops.iter().any(|op| match op {
                        WalOpV2::Put {
                            table_id_interned, ..
                        }
                        | WalOpV2::Delete {
                            table_id_interned, ..
                        }
                        | WalOpV2::IndexPut {
                            table_id_interned, ..
                        }
                        | WalOpV2::IndexDel {
                            table_id_interned, ..
                        } => *table_id_interned == table,
                        _ => false,
                    })
            })
        }) {
            self.record_orphan_failure(failed, error);
        }
    }

    /// Total `wal.recover()` reseed calls fired from `drain_step`.
    pub fn recover_calls(&self) -> u64 {
        self.recover_calls.load(Ordering::Relaxed)
    }
}

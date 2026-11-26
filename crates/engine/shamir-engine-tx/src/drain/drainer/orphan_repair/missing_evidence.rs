use std::sync::atomic::Ordering;

use crate::commit::core::RepoCore;
use crate::drainer::Drainer;
use shamir_storage::error::DbResult;
use shamir_tx::RepoTxGate;

/// Settle registered orphans whose replay evidence disappeared from RAM and
/// WAL. Ordinary pending versions and any retained evidence are untouched.
pub(in crate::drain::drainer) async fn settle_missing_evidence(
    drainer: &Drainer,
    repo: &RepoCore,
    gate: &RepoTxGate,
) -> DbResult<()> {
    for version in gate.orphaned_post_wal_versions() {
        let next_version = gate.durable_watermark().checked_add(1);
        let window_has_entry = {
            let guard = scc::Guard::new();
            drainer
                .window
                .range(version..=version, &guard)
                .next()
                .is_some()
        };
        if window_has_entry
            || gate.unresolved_wal(version).is_some()
            || gate.is_active_post_wal(version)
            || gate.orphan_entry(version).is_some()
            || next_version != Some(version)
        {
            continue;
        }
        log::warn!("orphan {version}: WAL replay evidence unavailable; recording minimal GAP");
        drainer
            .orphan_replay_attempts_total
            .fetch_add(1, Ordering::Relaxed);
        let mut failed = false;
        repo.mark_changefeed_gap(version)
            .await
            .inspect_err(|error| {
                drainer.record_orphan_failure(&mut failed, error);
            })?;
        drainer
            .orphan_replay_consecutive_failures
            .store(0, Ordering::Relaxed);
        drainer.orphan_replay_last_error.store(None);
        gate.complete_orphaned_post_wal(version);
        gate.mark_durable(version);
        gate.forget_orphaned_post_wal(version);
        drainer.orphan_repair_metrics.gap();
    }
    Ok(())
}

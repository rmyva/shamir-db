use super::repair::{repair_event, Outcome};
use crate::commit::core::RepoCore;
use crate::drainer::{Drainer, WalEntryV2};
use shamir_storage::error::DbResult;

pub(in crate::drain::drainer) async fn settle_event(
    drainer: &Drainer,
    repo: &RepoCore,
    entry: &WalEntryV2,
    orphan_failed: &mut bool,
) -> DbResult<bool> {
    let version = entry.commit_version;
    let outcome = match repair_event(repo, entry).await {
        Ok(outcome) => outcome,
        Err(error) => {
            drainer.record_orphan_failure(orphan_failed, &error);
            log::warn!(
                "drain_step: orphan {version} event repair failed: {error}; leaving pending"
            );
            return Ok(false);
        }
    };
    match outcome {
        Outcome::Repaired => drainer.orphan_repair_metrics.repaired(),
        Outcome::EnqueuePending => {
            // Deferred by a full journal channel: the repair-pending drain
            // lands the event; the orphan itself stays unresolved so a later
            // pass retries it. No failure counters, no gap.
            log::debug!(
                "drain_step: orphan {version} journal channel full; event deferred to \
                 repair-pending; leaving orphan unresolved"
            );
            return Ok(false);
        }
        Outcome::Gap(reason) => {
            if let Err(error) = repo.mark_changefeed_gap(version).await {
                drainer.record_orphan_failure(orphan_failed, &error);
                return Err(error);
            }
            log::warn!("drain_step: orphan {version} remains a GAP: {reason}");
            drainer.orphan_repair_metrics.gap();
        }
    }
    Ok(true)
}

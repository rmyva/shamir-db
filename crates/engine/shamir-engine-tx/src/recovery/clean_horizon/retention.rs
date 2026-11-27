use shamir_storage::error::DbResult;

use crate::commit::core::RepoCore;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetentionAction {
    NoOverflow,
    Refused,
}

pub async fn enforce_retention(repo: &RepoCore, safe_ceiling: u64) -> DbResult<RetentionAction> {
    let group = repo.wal_group().await?;
    if group.is_mem_sink() {
        return Ok(RetentionAction::NoOverflow);
    }
    let policy = repo.durability_policy();
    let cap = policy.wal_retention_max_bytes();
    let mode = policy.on_wal_retention_exceeded();
    drop(policy);

    let gate = repo.wal_retention_gate();
    // Measure on each pass, including following a successful unlink.
    let retained = group.sealed_retained_bytes().await?;
    if retained <= cap {
        gate.clear_refuse_if_set();
        return Ok(RetentionAction::NoOverflow);
    }
    gate.record_overflow_event();
    if mode == shamir_query_types::RetentionOverflow::RefuseWrites {
        gate.record_refusal();
        return Ok(RetentionAction::Refused);
    }
    let feed = match repo.changefeed().await {
        Ok(handle) => handle.feed,
        Err(error) => {
            log::warn!("clean_horizon: changefeed unavailable; refusing eviction: {error}");
            gate.record_refusal();
            return Ok(RetentionAction::Refused);
        }
    };
    let first = feed.clean_horizon().saturating_add(1);
    let last = group.max_committed_version().min(safe_ceiling);
    if first <= last {
        // Persist a recovery allocation floor before certifying the durable GAP;
        // this makes the interval safe across restart before any WAL unlink.
        let info = match repo.tx_info_store().await {
            Ok(info) => info,
            Err(error) => {
                log::warn!(
                    "clean_horizon: recovery floor store unavailable; refusing eviction: {error}"
                );
                gate.record_refusal();
                return Ok(RetentionAction::Refused);
            }
        };
        if let Err(error) = super::recovery_floor::reserve_recovery_floor(repo, &info, last).await {
            log::warn!("clean_horizon: recovery floor flush failed; refusing eviction: {error}");
            gate.record_refusal();
            return Ok(RetentionAction::Refused);
        }
        if let Err(error) = feed.record_recovery_gap(first, last).await {
            log::warn!("clean_horizon: gap certification failed; refusing eviction: {error}");
            gate.record_refusal();
            return Ok(RetentionAction::Refused);
        }
        if let Err(error) = feed.record_clean_horizon(last).await {
            log::warn!(
                "clean_horizon: clean horizon persistence failed; refusing eviction: {error}"
            );
            gate.record_refusal();
            return Ok(RetentionAction::Refused);
        }
        gate.record_declare_gap();
    }
    // A previous pass may already have certified this range but retained its
    // segments for a live pin. Retry unlink even without a new certification.
    let delete_through = last.min(feed.clean_horizon());
    if delete_through > 0 {
        let wal = repo.repo_wal().await?;
        repo.flush_all_history().await?;
        #[cfg(any(test, feature = "test-util"))]
        super::checkpoint::test_seam::before_delete(repo).await;
        // Pin publication may have raced any of the certification/flush
        // awaits above. Sample and admit only here, never from `last` alone.
        if let Some(lease) = repo.try_begin_truncate(delete_through) {
            lease.truncate(wal).await?;
        }
        let remaining = group.sealed_retained_bytes().await?;
        if remaining <= cap {
            gate.clear_refuse_if_set();
            return Ok(RetentionAction::NoOverflow);
        }
    }
    // No covered retained range is currently safe to unlink; retry later.
    gate.record_refusal();
    Ok(RetentionAction::Refused)
}

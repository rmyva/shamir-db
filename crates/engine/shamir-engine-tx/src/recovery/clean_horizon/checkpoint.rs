use crate::commit::core::RepoCore;

#[cfg(any(test, feature = "test-util"))]
#[path = "checkpoint/test_seam.rs"]
pub mod test_seam;

pub async fn checkpoint_before_truncate(repo: &RepoCore) -> bool {
    if repo.initialized_mem_wal() {
        return true;
    }
    #[cfg(any(test, feature = "test-util"))]
    if test_seam::bypass(repo) {
        return true;
    }
    #[cfg(any(test, feature = "test-util"))]
    if test_seam::should_fail(repo) {
        repo.wal_retention_gate().record_checkpoint_failure();
        return false;
    }

    // Sample settled W before any awaited checkpoint I/O; the durable marker
    // also protects abort-only allocations absent from the WAL.
    let gate = match repo.tx_gate().await {
        Ok(gate) => gate,
        Err(error) => {
            log::warn!("clean_horizon: gate initialization failed: {error}");
            repo.wal_retention_gate().record_checkpoint_failure();
            return false;
        }
    };
    let allocation_floor = match repo.checkpoint_allocation_floor().await {
        Ok(floor) => floor,
        Err(error) => {
            log::warn!("clean_horizon: allocation floor read failed: {error}");
            repo.wal_retention_gate().record_checkpoint_failure();
            return false;
        }
    };
    let settled = gate.completion().watermark();
    #[cfg(any(test, feature = "test-util"))]
    test_seam::record(repo, "sampled_w");
    let recovery_floor = settled.max(allocation_floor);
    let info = match repo.tx_info_store().await {
        Ok(info) => info,
        Err(error) => {
            log::warn!("clean_horizon: recovery floor store unavailable: {error}");
            repo.wal_retention_gate().record_checkpoint_failure();
            return false;
        }
    };
    if let Err(error) =
        super::recovery_floor::reserve_recovery_floor(repo, &info, recovery_floor).await
    {
        log::warn!("clean_horizon: recovery floor flush through {recovery_floor} failed: {error}");
        repo.wal_retention_gate().record_checkpoint_failure();
        return false;
    }
    let feed = match repo.changefeed().await {
        Ok(handle) => handle.feed,
        Err(error) => {
            log::warn!("clean_horizon: changefeed initialization failed: {error}");
            repo.wal_retention_gate().record_checkpoint_failure();
            return false;
        }
    };
    if let Err(error) = feed.record_clean_horizon(settled).await {
        log::warn!("clean_horizon: checkpoint through {settled} failed: {error}");
        repo.wal_retention_gate().record_checkpoint_failure();
        return false;
    }
    #[cfg(any(test, feature = "test-util"))]
    test_seam::record(repo, "checkpoint");
    repo.wal_retention_gate().record_checkpoint_success();
    true
}

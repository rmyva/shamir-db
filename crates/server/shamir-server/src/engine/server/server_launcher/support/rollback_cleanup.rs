//! Reporting for a failed launch's cleanup.
//!
//! A launch rollback preserves the boot error that aborted it: cleanup
//! failures are logged here at error level and never replace the original
//! cause. The cleanup report itself comes from the same shared audit
//! teardown the graceful path uses, so a stalled checkpoint shows up as an
//! audit timeout in the log instead of an unbounded wait.

use std::sync::Arc;
use std::time::Duration;

use shamir_db::{ShutdownReport, ShutdownWaitStage};
use shamir_sync::durability::tx::tx_registry::ReaperTask;
use shamir_sync::durability::tx::tx_registry::TxRegistry;

use crate::server::server_handle::release_source_stop::stop_release_sources_and_drain_tails;
use crate::server::server_handle::release_source_stop::ReleaseStopBounds;
use crate::server::server_handle::{MetaSnapshotTask, Settlement};

/// Upper bound for joining the meta-snapshot task in a rollback. The task still
/// has to run one final snapshot on the blocking pool after the root token is
/// cancelled, so a saturated or stalled pool would keep the failed launch from
/// ever returning its boot error. Same value as the graceful shutdown's join.
const META_SNAPSHOT_JOIN_TIMEOUT: Duration = Duration::from_secs(5);

pub(super) const ROLLBACK_RELEASE_DRAIN: Duration = Duration::from_secs(10);

/// Bound for draining the detached mutation workers and the online-export
/// service in a rollback (moved here from `launch.rs` with the settlement).
pub(super) const ROLLBACK_WORKER_DRAIN: Duration = Duration::from_secs(10);

/// Bound for the rollback's store-release barrier: the SAME value as the
/// graceful shutdown's `STORE_RELEASE_TIMEOUT` (the barrier probes directory
/// locks with the same mechanism, so it needs the same budget).
pub(super) const STORE_RELEASE_TIMEOUT: Duration = Duration::from_secs(10);

/// The ONE ordered release-source stop for a launch rollback: stop the
/// release sources (the interactive-tx reaper), clear the registry so every
/// lingering transaction's `Drop` release is registered while tail admission
/// is still open, and wait for the release tails. Called from BOTH rollback
/// arms (listener bind failure; observability port failure — in the second
/// one connections may already have opened transactions): no release may run
/// untracked behind a boot that is being unwound. Expiries are recorded in
/// `cleanup` and logged by `log_rollback_cleanup`; the boot error still wins.
/// Bounded meta-snapshot join with a TYPED outcome: an expiry is NOT proof —
/// the task may still hold the `server_meta` store lock until it finishes.
pub(super) async fn stop_release_sources_settled(
    stage: &'static str,
    reaper: &mut Option<ReaperTask>,
    registry: &Arc<TxRegistry>,
    cleanup: &mut ShutdownReport,
) -> Settlement {
    let tails = registry.release_tails();
    let deadline = tokio::time::Instant::now() + ROLLBACK_RELEASE_DRAIN;
    let settled = stop_release_sources_and_drain_tails(
        reaper,
        Some(registry),
        tails.as_ref(),
        ReleaseStopBounds {
            reaper: Some((deadline, ROLLBACK_RELEASE_DRAIN)),
            tail_deadline: deadline,
            tail_budget: ROLLBACK_RELEASE_DRAIN,
        },
        stage,
        cleanup,
    )
    .await;
    if let Settlement::Unsettled(tail_stage) = settled {
        cleanup.record_expired_owner_wait(tail_stage, ROLLBACK_RELEASE_DRAIN);
    }
    settled
}

pub(super) async fn join_meta_snapshot_settled(
    stage: &'static str,
    task: Option<MetaSnapshotTask>,
) -> Settlement {
    let Some(task) = task else {
        return Settlement::Settled;
    };
    if tokio::time::timeout(META_SNAPSHOT_JOIN_TIMEOUT, task.handle)
        .await
        .is_err()
    {
        tracing::error!(
            stage,
            error = "meta snapshot task did not stop within 5s: it may still hold the \
                     server_meta store lock, so an immediate retry on the same data \
                     directory can fail",
            "launch rollback cleanup error"
        );
        return Settlement::Unsettled(ShutdownWaitStage::MetaSnapshotJoin);
    }
    Settlement::Settled
}

/// Join the cursor-reaper task of a failed launch. Both rollback arms do the
/// same unbounded await (the reaper is cooperative and its shutdown token is
/// already cancelled); kept here so the launcher's rollback arms stay one
/// line each.
pub(super) async fn join_cursor_reaper(reaper: Option<crate::cursor_registry::CursorReaperTask>) {
    if let Some(reaper) = reaper {
        let _ = reaper.handle.await;
    }
}

/// Log every cleanup failure of a rollback, naming the stage that failed.
pub(super) fn log_rollback_cleanup(stage: &'static str, cleanup: &ShutdownReport) {
    for error in cleanup.errors() {
        tracing::error!(stage, error, "launch rollback cleanup error");
    }
    for (repository, error) in cleanup.repository_failures() {
        tracing::error!(stage, repository, error, "launch rollback cleanup error");
    }
}

//! [`ShutdownOwner`] — the shutdown state that must OUTLIVE the bounded
//! shutdown wait (moved out of `core.rs`, which was at the 1000-line cap).

use std::sync::Arc;
use std::time::Duration;

use shamir_db::ShutdownReport;
use shamir_sync::durability::tx::ReleaseTails;
use tokio::task::JoinHandle;
use tokio_util::sync::DropGuard;

use super::release_tail_drain::drain_release_tails;
use super::{EXPORT_DRAIN_TIMEOUT, RELEASE_TAIL_DRAIN};
use crate::boot::config::diagnostics::logical::export_online::ExportDrainHandle;
use crate::db_handler::MutationTasks;

/// Shutdown budget for joining/draining a live connection task: the same cap
///  uses for its connection-drain stage.
pub(super) const CONN_DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

/// Shared budget for repository shutdown quiescing and checkpointing.
pub(super) const REPO_FLUSH_BUDGET: Duration = Duration::from_secs(15);

/// Bounds WAITs by the earlier of stage start + cap and outer deadline minus reserve; synchronous work is not interrupted, and timed-out joins may continue. `early_stage_bound` uses min(stage start + cap, early cutoff).
pub(super) fn stage_bound(
    deadline_at: Option<tokio::time::Instant>,
    cap: Duration,
    reserve_after: Duration,
) -> (tokio::time::Instant, Duration) {
    let now = tokio::time::Instant::now();
    let own = now + cap;
    match deadline_at {
        Some(at) => {
            let usable_until = at.checked_sub(reserve_after).unwrap_or(now);
            let until = own.min(usable_until);
            (until, until.saturating_duration_since(now))
        }
        None => (own, cap),
    }
}

pub(super) fn early_stage_bound(
    early_cutoff: Option<tokio::time::Instant>,
    cap: Duration,
) -> (tokio::time::Instant, Duration) {
    let now = tokio::time::Instant::now();
    let stage_cap = now + cap;
    match early_cutoff {
        Some(cutoff) => {
            let until = stage_cap.min(cutoff);
            (until, until.saturating_duration_since(now))
        }
        None => (stage_cap, cap),
    }
}

/// The shutdown state that must OUTLIVE the bounded shutdown wait.
///
/// `runtime::serve` wraps the shutdown future in a deadline; on expiry that
/// future is dropped, and with it anything it still owns. These two must not
/// be dropped there: the scheduled-backup worker is a native thread that keeps
/// doing filesystem work under the managed roots (copying, retention's tagging
/// and deletions) after its last cancellation checkpoint, and the
/// single-instance lock must not be released while it does — an embedding
/// process would otherwise see I/O and a free data dir that outlive `serve`.
///
/// The owner is therefore extracted BEFORE the deadline and BORROWED into the
/// shutdown future (`ServerHandle::shutdown_with_report` takes `&mut Self`
/// here), never moved into it: on a normal shutdown the worker is awaited in
/// place at its original teardown position (before the repository flush), and
/// on expiry the dropped future leaves the worker and the lock intact for
/// [`Self::drain`] to finish afterwards. The deadline still marks the report
/// unclean; it only bounds the audit/repository teardown.
pub(crate) struct ShutdownOwner {
    pub(super) schedule_task: Option<(DropGuard, JoinHandle<()>)>,
    pub(super) owned_mutation_tasks: Option<Arc<MutationTasks>>,
    /// Fallback ownership of the export DRAIN HANDLE only — never the
    /// service. The deadline dropping the shutdown future must not abandon
    /// a draining export worker either, but the service itself owns
    /// `Arc<ShamirDb>` + the durable user directory: an `Arc<OnlineExport>`
    /// retained here would hold the stores locked past the store-release
    /// probe (a false `StillLocked` on an idle export-enabled server).
    pub(super) online_export: Option<ExportDrainHandle>,
    pub(super) tx_release_tails: Option<Arc<ReleaseTails>>,
    /// The release-tail producers the fallback drain must stop before its
    /// tail drain: the interactive-tx reaper (moved out of the handle by
    /// [`ServerHandle::take_shutdown_owner`]) and the registry itself
    /// (emptied here so every lingering transaction's `Drop` release is
    /// registered while tail admission is still open).
    pub(super) tx_reaper: Option<crate::tx_registry::ReaperTask>,
    pub(super) tx_registry: Option<Arc<crate::tx_registry::TxRegistry>>,
    pub(super) data_dir_lock: Option<std::fs::File>,
}

impl ShutdownOwner {
    /// Await the scheduled worker in place and forget its handle.
    ///
    /// Called from INSIDE the bounded shutdown future with `&mut self`, so the
    /// owner itself is never moved into — or dropped with — that future. This
    /// keeps the original teardown position: the scheduled task and its
    /// in-flight run are awaited BEFORE the repository flush, not after it.
    ///
    /// Deliberately unbounded: a run already past its last cooperative
    /// cancellation checkpoint has to finish its own cleanup. The wait is a
    /// COMPLETION signal (the runner's per-run channel), not a join of the
    /// native runner thread.
    pub(crate) async fn join_schedule(&mut self, report: &mut ShutdownReport) {
        let Some((_, task)) = self.schedule_task.as_mut() else {
            return;
        };
        // Awaited THROUGH `&mut`: while the wait is pending the handle and its
        // drop guard stay in the owner, so the deadline dropping this future
        // mid-wait leaves them for `drain` instead of detaching the worker.
        let joined = task.await;
        // Finished: forget the handle (the guard drops with it).
        self.schedule_task = None;
        if let Err(error) = joined {
            report.add_error(format!("backup schedule task failed: {error}"));
        }
    }

    /// Await the interactive-tx reaper through `&mut` (a release-tail
    /// producer). Borrowed, so a deadline expiry leaves the reaper in the
    /// owner for [`Self::drain`]. `None` bound = the original unbounded join.
    ///
    /// On a timed-out bounded join the reaper task is ABORTED and its
    /// completion awaited: abort is cooperative — the task only awaits the
    /// sweep interval and the shutdown token — so the post-abort join is
    /// bounded by the sweep's current synchronous section (`reap_expired`),
    /// not by a clock. Without the abort the reaper would survive the join
    /// and keep reaping past [`Self::settle_tx_release_tails`]' admission
    /// close: each such release would land on the DETACHED refusal arm of
    /// `spawn_abort_release`, untracked and able to overlap the repository
    /// flush. The timeout is recorded BEFORE the post-abort wait (an outer
    /// deadline dropping this future mid-wait must not lose it), and a reaper
    /// task that panicked is reported too; the shutdown is unclean either way.
    pub(crate) async fn join_tx_reaper(
        &mut self,
        bound: Option<(tokio::time::Instant, Duration)>,
        report: &mut ShutdownReport,
    ) {
        let Some(reaper) = self.tx_reaper.as_mut() else {
            return;
        };
        match bound {
            Some((deadline, budget)) => {
                match tokio::time::timeout_at(deadline, &mut reaper.handle).await {
                    Ok(joined) => report_reaper_failure(joined, report),
                    Err(_) => {
                        // Recorded before the post-abort wait below.
                        report.add_error(format!(
                            "interactive transaction reaper join exceeded {budget:?}; the \
                             reaper was aborted and its final sweep awaited before tail \
                             admission closes"
                        ));
                        // Stop the producer before the tail admission closes
                        // below. The abort lands at the reaper's next await;
                        // the join after it only waits out one in-flight sweep.
                        reaper.handle.abort();
                        report_reaper_failure((&mut reaper.handle).await, report);
                    }
                }
            }
            None => {
                // Awaited THROUGH `&mut` (same borrow discipline as the
                // bounded branch above): the handle stays in the owner.
                report_reaper_failure((&mut reaper.handle).await, report);
            }
        }
        // Joined (or aborted and joined): the handle is consumed, and polling a
        // finished `JoinHandle` again panics. Forget it so the fallback drain
        // after a clean inline shutdown does not re-await it. A deadline that
        // drops this future mid-wait never reaches here and leaves the handle.
        self.tx_reaper = None;
    }

    /// Stop the tail producers and empty the registry so every lingering
    /// transaction's `Drop` release is REGISTERED before the tail admission
    /// closes, then close + drain the tails.
    ///
    /// Interleaving still open after the abort in [`Self::join_tx_reaper`]
    /// (stated exactly): an `Arc<InteractiveTx>` held OUTSIDE the registry —
    /// an in-flight request's clone, a caller-held handle — can drop its LAST
    /// reference after the drain below closes admission; `remove_all` drops
    /// only the REGISTRY's references, and `InteractiveTx::drop` releases
    /// only on the LAST one, so that release is refused by the closed owner
    /// and runs DETACHED via `spawn_abort_release`'s refusal arm: untracked,
    /// able to overlap the repository flush, and NOT lossless. Shutdown
    /// cannot name it in the report — the surviving reference lives outside
    /// shutdown's reach; the only signal is the `warn!` from that refusal
    /// arm.
    async fn settle_tx_release_tails(&mut self, report: &mut ShutdownReport) {
        let (reaper_deadline, reaper_budget) =
            stage_bound(None, CONN_DRAIN_TIMEOUT, Duration::ZERO);
        self.join_tx_reaper(Some((reaper_deadline, reaper_budget)), report)
            .await;
        if let Some(registry) = &self.tx_registry {
            // Dropping the returned `Arc`s is what triggers each RAII release
            // (registered into the still-open tail admission).
            drop(registry.remove_all());
        }
        let (deadline, budget) = stage_bound(None, RELEASE_TAIL_DRAIN, Duration::ZERO);
        drain_release_tails(
            self.tx_release_tails.as_deref(),
            true,
            deadline,
            budget,
            "fallback drain after the shutdown deadline",
            report,
        )
        .await;
    }

    /// Await whatever the deadline left unawaited, then release the instance
    /// lock by drop.
    ///
    /// On a normal shutdown [`Self::join_schedule`] already ran inside the
    /// deadline, so this only releases the lock. On expiry the shutdown future
    /// was dropped mid-teardown and the worker is still awaited here — the lock
    /// is released afterwards, never before.
    pub(crate) async fn drain(mut self, report: &mut ShutdownReport) {
        self.join_schedule(report).await;
        if let Some(tracker) = &self.owned_mutation_tasks {
            tracker.close();
            let (deadline, budget) = early_stage_bound(None, REPO_FLUSH_BUDGET);
            if tokio::time::timeout_at(deadline, tracker.wait())
                .await
                .is_err()
            {
                report.add_error(format!(
                    "owned mutation workers still running after the shutdown bound; repository flush skipped ({budget:?})"
                ));
            }
        }
        // The export drain HANDLE keeps its fallback ownership here (the
        // shutdown future that would normally wait for it may have been
        // dropped by the deadline). The service itself — and its
        // `Arc<ShamirDb>` — never enters the owner.
        if let Some(export) = &self.online_export {
            let (deadline, budget) = early_stage_bound(None, EXPORT_DRAIN_TIMEOUT);
            if !tokio::time::timeout_at(deadline, export.wait_idle())
                .await
                .unwrap_or(false)
            {
                report.add_error(format!(
                    "online export worker still draining after the fallback bound ({budget:?})"
                ));
            }
        }
        self.settle_tx_release_tails(report).await;
        // Released here, after the scheduled worker finished — never
        // explicitly unlocked (drop is the release).
        drop(self.data_dir_lock.take());
    }
}

/// A reaper task that ended by panicking is a shutdown failure; the
/// cancellation of our own `abort` is the expected end and is not.
fn report_reaper_failure(joined: Result<(), tokio::task::JoinError>, report: &mut ShutdownReport) {
    if let Err(error) = joined {
        if !error.is_cancelled() {
            report.add_error(format!(
                "interactive transaction reaper task failed: {error}"
            ));
        }
    }
}

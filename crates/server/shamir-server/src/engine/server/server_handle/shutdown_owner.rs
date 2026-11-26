//! [`ShutdownOwner`] — the shutdown state that must OUTLIVE the bounded
//! shutdown wait (moved out of `core.rs`, which was at the 1000-line cap).

use std::sync::Arc;
use std::time::Duration;

use shamir_db::{ShutdownReport, ShutdownWaitStage};
use shamir_sync::durability::tx::ReleaseTails;
use tokio::task::JoinHandle;
use tokio_util::sync::DropGuard;

use super::release_source_stop::{stop_release_sources_and_drain_tails, ReleaseStopBounds};
use super::{EXPORT_DRAIN_TIMEOUT, RELEASE_TAIL_DRAIN};
use crate::boot::config::diagnostics::logical::export_online::ExportDrainHandle;
use crate::db_handler::MutationTasks;

/// Shutdown budget for joining/draining a live connection task: the same cap
///  uses for its connection-drain stage.
pub(super) const CONN_DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

/// Shared budget for repository shutdown quiescing and checkpointing.
pub(super) const REPO_FLUSH_BUDGET: Duration = Duration::from_secs(15);

/// Generous bound for the post-deadline join of the scheduled backup worker
/// in [`ShutdownOwner::drain`] (and for the owner-side wait generally).
///
/// Why 120 s: it is FOUR times the whole outer shutdown deadline
/// (crate::boot::runtime::SHUTDOWN_DEADLINE, 30 s) and TWO times the longest
/// other owner wait (the online-export drain, 60 s), so a healthy backup run
/// that survived its last cooperative cancellation checkpoint — one that then
/// only has the store flush left (a blocking-pool job) plus its manifest
/// publication — finishes well inside it even on a starved blocking pool,
/// while a genuinely wedged runner can no longer hold the process hostage:
/// on expiry the wait stops, the report names the stage, and the
/// data-directory lock is KEPT until the process exits (see
/// [`report_expired_owner_wait`]).
pub(super) const BACKUP_JOIN_FALLBACK_TIMEOUT: Duration = Duration::from_secs(120);

/// Test-only seam for the drain-side backup join bound: a test cannot park a
/// worker for 120 s, so it shrinks the bound instead. `None` (production)
/// means [`BACKUP_JOIN_FALLBACK_TIMEOUT`].
#[cfg(test)]
static DRAIN_BACKUP_JOIN_BOUND_OVERRIDE: std::sync::Mutex<Option<Duration>> =
    std::sync::Mutex::new(None);

/// Set the drain-side backup join bound for ONE test; call
/// `set_drain_backup_join_bound_for_test(None)` to restore.
#[cfg(test)]
pub(crate) fn set_drain_backup_join_bound_for_test(bound: Option<Duration>) {
    *DRAIN_BACKUP_JOIN_BOUND_OVERRIDE
        .lock()
        .expect("drain backup join bound override") = bound;
}

#[cfg(test)]
fn drain_backup_join_budget() -> Duration {
    DRAIN_BACKUP_JOIN_BOUND_OVERRIDE
        .lock()
        .expect("drain backup join bound override")
        .unwrap_or(BACKUP_JOIN_FALLBACK_TIMEOUT)
}

#[cfg(not(test))]
fn drain_backup_join_budget() -> Duration {
    BACKUP_JOIN_FALLBACK_TIMEOUT
}

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

/// RAII owner of the data-directory instance lock (`<data_dir>/.shamir.lock`).
///
/// The lock is released ONLY by an explicit, completed, clean drain:
/// [`Self::release_clean`] is a CONSUMING method called at the end of a clean
/// owner drain, and it is the only place the inner `File` is dropped (the
/// drop of the file is what releases the advisory OS lock). EVERY other end
/// of the owner's life leaks the file ON PURPOSE — dropping or cancelling the
/// shutdown future, a panic unwinding through the owner, an early return: the
/// [`Drop`] impl `std::mem::forget`s whatever is still inside, so the OS keeps
/// the advisory lock until the process exits and no other process can open
/// the data directory over work that is still running. This is the same
/// mechanism the expired owner wait uses, expressed exactly ONCE, here.
pub(in crate::engine::server) struct DataDirLock(Option<std::fs::File>);

impl DataDirLock {
    pub(in crate::engine::server) fn new(file: std::fs::File) -> Self {
        Self(Some(file))
    }

    /// Release the advisory lock by dropping the inner `File`. Consuming:
    /// after this the [`Drop`] impl sees `None` and forgets nothing. Call it
    /// ONLY at the end of a completed, clean drain.
    pub(in crate::engine::server) fn release_clean(mut self) {
        drop(self.0.take());
    }
}

impl Drop for DataDirLock {
    fn drop(&mut self) {
        // Deliberate leak: the kernel releases the advisory lock only at
        // process death — never explicitly unlocked, and never handed to
        // another process over a stage that is still running.
        std::mem::forget(self.0.take());
    }
}

/// Record an expired owner-drain wait. The fixed prefix `shutdown owner wait
/// expired (` is the operator contract: on ANY of these expiries the
/// data-directory instance lock is NOT released before the process exits
/// (the `File` is leaked deliberately — the kernel releases the advisory
/// lock when the process dies), and the report stays unclean, so the exit
/// code is the abnormal-report code 70 as before. Nothing forces an exit.
pub(crate) fn report_expired_owner_wait(
    stage: ShutdownWaitStage,
    budget: Duration,
    report: &mut ShutdownReport,
) {
    report.set_expired_owner_wait(
        stage,
        format!(
            "shutdown owner wait expired ({}: waited {budget:?}): that stage is still running, \
             so the data-directory instance lock is KEPT until the process exits — check for a \
             running backup or export job, then restart the server",
            stage.as_str()
        ),
    );
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
/// shutdown future (`shutdown_with_budget` takes `&mut Self`
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
    /// Same discard discipline as the rest of the owner, wrapped in
    /// [`DataDirLock`]: the lock is released only by an explicit, completed,
    /// clean drain ([`DataDirLock::release_clean`]); any other end of this
    /// struct's life — a dropped or cancelled shutdown future, a panic that
    /// unwinds through it, an early return, an expired owner wait — LEAKS the
    /// lock file so the OS keeps it until the process exits.
    pub(super) data_dir_lock: Option<DataDirLock>,
}

impl ShutdownOwner {
    /// Await the scheduled worker in place and forget its handle.
    ///
    /// Called from INSIDE the bounded shutdown future with `&mut self`, so the
    /// owner itself is never moved into — or dropped with — that future. This
    /// keeps the original teardown position: the scheduled task and its
    /// in-flight run are awaited BEFORE the repository flush, not after it.
    ///
    /// The wait is a COMPLETION signal (the runner's per-run channel), not a
    /// join of the native runner thread. `bound` = `None` keeps the original
    /// unbounded join (cleanup order and owner lock make it deliberate); a
    /// `Some` bound stops WAITING on expiry — the run is not interrupted, the
    /// expiry is reported, and `false` is returned so the caller can keep the
    /// data-directory lock (see [`Self::drain`]).
    pub(crate) async fn join_schedule(
        &mut self,
        bound: Option<(tokio::time::Instant, Duration)>,
        report: &mut ShutdownReport,
    ) -> bool {
        let Some((_, task)) = self.schedule_task.as_mut() else {
            return true;
        };
        // Awaited THROUGH `&mut`: while the wait is pending the handle and its
        // drop guard stay in the owner, so the deadline dropping this future
        // mid-wait leaves them for `drain` instead of detaching the worker.
        match bound {
            Some((deadline, budget)) => match tokio::time::timeout_at(deadline, task).await {
                Ok(joined) => {
                    self.schedule_task = None;
                    if let Err(error) = joined {
                        report.add_error(format!("backup schedule task failed: {error}"));
                    }
                    true
                }
                Err(_) => {
                    report.add_error(format!(
                        "backup schedule join exceeded shutdown budget {budget:?}; the run \
                             is NOT interrupted and its completion is awaited no further here"
                    ));
                    false
                }
            },
            None => {
                let joined = task.await;
                self.schedule_task = None;
                if let Err(error) = joined {
                    report.add_error(format!("backup schedule task failed: {error}"));
                }
                true
            }
        }
    }

    /// Await whatever the deadline left unawaited, then release the instance
    /// lock — ONLY if every bounded owner wait settled.
    ///
    /// On a normal shutdown [`Self::join_schedule`] already ran inside the
    /// deadline, so this only releases the lock. On expiry of the shutdown
    /// future the worker is still awaited here — the lock is released
    /// afterwards, never before. If any of the bounded waits below expires,
    /// the stage that expired is recorded typed in the report
    /// ([`report_expired_owner_wait`]) and the lock is deliberately NOT
    /// released: the [`DataDirLock`] guard leaks the file, so the kernel holds
    /// the advisory data-directory lock until the process exits and no other
    /// process can open the directory over the still-running stage. The
    /// report stays unclean, so the process exit code is the abnormal 70 as
    /// before.
    pub(crate) async fn drain(mut self, report: &mut ShutdownReport) {
        let mut expired: Option<ShutdownWaitStage> = None;
        let budget = drain_backup_join_budget();
        let (deadline, _) = stage_bound(None, budget, Duration::ZERO);
        if !self.join_schedule(Some((deadline, budget)), report).await {
            expired = Some(ShutdownWaitStage::ScheduledBackupJoin);
            report_expired_owner_wait(ShutdownWaitStage::ScheduledBackupJoin, budget, report);
        }
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
                expired = Some(ShutdownWaitStage::MutationWorkersJoin);
                report_expired_owner_wait(ShutdownWaitStage::MutationWorkersJoin, budget, report);
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
                expired = Some(ShutdownWaitStage::OnlineExportDrain);
                report_expired_owner_wait(ShutdownWaitStage::OnlineExportDrain, budget, report);
            }
        }
        // Stop the release sources, clear the registry, wait for the release
        // tails (the ONE ordered stop, shared with the graceful shutdown and
        // the launch rollbacks). A reaper or tail expiry here is also an
        // in-memory producer that may still run past this function.
        {
            let (reaper_deadline, reaper_budget) =
                stage_bound(None, CONN_DRAIN_TIMEOUT, Duration::ZERO);
            let (tail_deadline, tail_budget) =
                stage_bound(None, RELEASE_TAIL_DRAIN, Duration::ZERO);
            let settled = stop_release_sources_and_drain_tails(
                &mut self.tx_reaper,
                self.tx_registry.as_ref(),
                self.tx_release_tails.as_ref(),
                ReleaseStopBounds {
                    reaper: Some((reaper_deadline, reaper_budget)),
                    tail_deadline,
                    tail_budget,
                },
                "the post-deadline owner drain",
                report,
            )
            .await;
            if !settled {
                expired = Some(ShutdownWaitStage::ReleaseTailDrain);
                report_expired_owner_wait(
                    ShutdownWaitStage::ReleaseTailDrain,
                    RELEASE_TAIL_DRAIN,
                    report,
                );
            }
        }
        match expired {
            // Keep the advisory lock until the process exits: leave the
            // `DataDirLock` in place — its `Drop` impl leaks the inner `File`
            // on purpose, so the kernel releases the lock only at process
            // death — never explicitly unlocked, and never handed to another
            // process over a stage that is still running.
            Some(_) => {}
            // Released here, after the scheduled worker finished — the ONE
            // clean end of the owner's life (never explicitly unlocked; the
            // drop of the inner `File` is the release).
            None => {
                if let Some(lock) = self.data_dir_lock.take() {
                    lock.release_clean();
                }
            }
        }
    }
}

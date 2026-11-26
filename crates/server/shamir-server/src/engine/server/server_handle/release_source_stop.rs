//! ONE helper for stopping the interactive-tx release SOURCES and settling
//! the release tails: "stop the release sources, clear the registry, wait
//! for the release tails".
//!
//! Every release producer (the periodic reaper and the transactions still
//! registered in the [`TxRegistry`]) is stopped and emptied BEFORE the tail
//! admission closes, so every lingering transaction's `Drop` release is
//! REGISTERED while admission is still open, and the tail drain can settle
//! them — all before the repository flush on the graceful path, and before a
//! launch rollback returns its boot error. The order of stopping is written
//! HERE and nowhere else.

use std::sync::Arc;
use std::time::Duration;

use shamir_db::ShutdownReport;
use shamir_sync::durability::tx::tx_registry::{ReaperTask, TxRegistry};
use shamir_sync::durability::tx::ReleaseTails;

#[cfg(test)]
use super::shutdown_probes::fire_release_sources_stopped_probe;

/// Join the interactive-tx reaper (a release-tail producer) — it must be
/// stopped before the tail admission closes.
///
/// The reaper is borrowed through `&mut Option`: an outer deadline that
/// drops the CALLER's future leaves the handle with the caller (the
/// shutdown owner) instead of detaching the task.
///
/// `None` bound = the original unbounded join. On a timed-out bounded join
/// the reaper task is ABORTED and its completion awaited: abort is
/// cooperative — the task only awaits the sweep interval and the shutdown
/// token — so the post-abort join is bounded by the sweep's current
/// synchronous section (`reap_expired`), not by a clock. The timeout is
/// recorded BEFORE the post-abort wait (an outer deadline dropping the
/// caller's future mid-wait must not lose it), and a reaper task that
/// panicked is reported too. Returns `false` only when the bounded join
/// expired; a joined reaper (or no reaper at all) returns `true`.
pub(in crate::engine::server) async fn join_tx_reaper(
    reaper: &mut Option<ReaperTask>,
    bound: Option<(tokio::time::Instant, Duration)>,
    report: &mut ShutdownReport,
) -> bool {
    let Some(task) = reaper.as_mut() else {
        return true;
    };
    let expired;
    match bound {
        Some((deadline, budget)) => {
            match tokio::time::timeout_at(deadline, &mut task.handle).await {
                Ok(joined) => {
                    expired = false;
                    report_reaper_failure(joined, report);
                }
                Err(_) => {
                    // Recorded before the post-abort wait below.
                    expired = true;
                    report.add_error(format!(
                        "interactive transaction reaper join exceeded {budget:?}; the \
                         reaper was aborted and its final sweep awaited before tail \
                         admission closes"
                    ));
                    // Stop the producer before the tail admission closes in
                    // the drain below. The abort lands at the reaper's next
                    // await; the join after it only waits out one in-flight
                    // sweep.
                    task.handle.abort();
                    report_reaper_failure((&mut task.handle).await, report);
                }
            }
        }
        None => {
            // Awaited THROUGH `&mut` (same borrow discipline as the bounded
            // branch above): the handle stays with the caller.
            expired = false;
            report_reaper_failure((&mut task.handle).await, report);
        }
    }
    // Joined (or aborted and joined): the handle is consumed, and polling a
    // finished `JoinHandle` again panics. Forget it so a later drain does
    // not re-await it. A deadline that drops the caller's future mid-wait
    // never reaches here and leaves the handle in place.
    *reaper = None;
    !expired
}

/// The reaper and tail bounds for one release-source stop, computed by the
/// caller (each call site bounds the waits with ITS stage deadline).
pub(in crate::engine::server) struct ReleaseStopBounds {
    /// The reaper join: `None` = the original unbounded join.
    pub reaper: Option<(tokio::time::Instant, Duration)>,
    /// The tail drain's absolute deadline and reported budget.
    pub tail_deadline: tokio::time::Instant,
    pub tail_budget: Duration,
}

/// THE ordered stop: stop the release sources (join the reaper), clear the
/// registry so every lingering transaction's `Drop` release is REGISTERED
/// while tail admission is still open, THEN close + drain the tails.
///
/// Called from the normal shutdown (before the repository flush) and from
/// BOTH launch-rollback arms (listener bind failure; observability port
/// failure — in the second one connections may already have opened
/// transactions). Returns `true` when the reaper AND the tail drain both
/// settled; either expiry is recorded in `report` with the caller's
/// `context` naming the stage.
pub(in crate::engine::server) async fn stop_release_sources_and_drain_tails(
    reaper: &mut Option<ReaperTask>,
    registry: Option<&Arc<TxRegistry>>,
    tails: Option<&Arc<ReleaseTails>>,
    bounds: ReleaseStopBounds,
    context: &str,
    report: &mut ShutdownReport,
) -> bool {
    let mut settled = join_tx_reaper(reaper, bounds.reaper, report).await;
    if let Some(registry) = registry {
        // Dropping the returned `Arc`s is what triggers each RAII release
        // (registered into the still-open tail admission below).
        drop(registry.remove_all());
        #[cfg(test)]
        super::shutdown_probes::fire_registry_cleared_probe();
    }
    // The release SOURCES are now stopped: fire the order probe (test-only)
    // before the drain closes admission, so a test can prove this point ran
    // before the repository flush.
    #[cfg(test)]
    fire_release_sources_stopped_probe();
    settled &= super::release_tail_drain::drain_release_tails(
        tails.map(|tails| tails.as_ref()),
        true,
        bounds.tail_deadline,
        bounds.tail_budget,
        context,
        report,
    )
    .await;
    settled
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

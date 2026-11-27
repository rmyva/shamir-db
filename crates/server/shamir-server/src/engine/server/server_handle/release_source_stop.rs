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

use shamir_db::{ShutdownReport, ShutdownWaitStage};
use shamir_sync::durability::tx::tx_registry::{ReaperTask, TxRegistry};
use shamir_sync::durability::tx::ReleaseTails;

use super::shutdown_owner::Settlement;

#[cfg(test)]
use super::shutdown_probes::fire_release_sources_stopped_probe;

/// Bound for the post-abort join of the interactive-tx reaper: after the
/// abort lands at the reaper's next await, only its current synchronous
/// section (`reap_expired`) remains. 2 s — the same value as the
/// connection-abort join cap (`CONN_ABORT_TIMEOUT` in `core.rs`), which
/// bounds the identical "one in-flight synchronous section after abort"
/// wait for connection tasks; a reaper that has not finished within it may
/// still be running and is NOT proof that the producer ended.
pub(in crate::engine::server) const REAPER_POST_ABORT_JOIN: Duration = Duration::from_secs(2);

/// Join the interactive-tx reaper (a release-tail producer) — it must be
/// stopped before the tail admission closes.
///
/// The reaper is borrowed through `&mut Option`: an outer deadline that
/// drops the CALLER's future leaves the handle with the caller (the
/// shutdown owner) instead of detaching the task.
///
/// `None` bound = the original unbounded join. On a timed-out bounded join
/// the reaper task is ABORTED and its completion awaited with its OWN named
/// bound ([`REAPER_POST_ABORT_JOIN`]): a post-abort join that misses it is
/// an in-flight producer that may still run past the caller — NOT proof.
/// The timeout is recorded BEFORE the post-abort wait (an outer deadline
/// dropping the caller's future mid-wait must not lose it); a reaper task
/// that panicked or was cancelled is likewise NOT proof. The handle is
/// forgotten only when it provably finished; on any unsettled outcome it
/// stays in place so a repeated call stays a safe no-op (a finished handle
/// is joined directly, polling a finished `JoinHandle` again is harmless).
pub(in crate::engine::server) async fn join_tx_reaper(
    reaper: &mut Option<ReaperTask>,
    bound: Option<(tokio::time::Instant, Duration)>,
    report: &mut ShutdownReport,
) -> Settlement {
    let Some(task) = reaper.as_mut() else {
        return Settlement::Settled;
    };
    let settled;
    match bound {
        Some((deadline, budget)) => {
            match tokio::time::timeout_at(deadline, &mut task.handle).await {
                Ok(joined) => {
                    // On a direct (non-abort) join, a cancelled result can only
                    // follow our own earlier abort (a re-entry after an unsettled
                    // post-abort wait) — the expected end of that abort.
                    settled = reaper_join_settlement(joined, true, report);
                }
                Err(_) => {
                    report.add_error(format!(
                        "interactive transaction reaper join exceeded {budget:?}; the \
                         reaper was aborted and its final sweep awaited before tail \
                         admission closes"
                    ));
                    // Stop the producer before the tail admission closes in
                    // the drain below. The abort lands at the reaper's next
                    // await; the join after it is bounded by
                    // [`REAPER_POST_ABORT_JOIN`], not by a clock.
                    task.handle.abort();
                    let post_abort = tokio::time::Instant::now() + REAPER_POST_ABORT_JOIN;
                    settled = match tokio::time::timeout_at(post_abort, &mut task.handle).await {
                        // A cancelled join right after OUR abort is the
                        // expected end of the abort — proof the task ended.
                        Ok(joined) => reaper_join_settlement(joined, true, report),
                        Err(_) => {
                            report.add_error(format!(
                                "the aborted interactive transaction reaper did not finish \
                                 within {REAPER_POST_ABORT_JOIN:?}: it may still be running, \
                                 so the release sources are NOT proven drained"
                            ));
                            Settlement::Unsettled(ShutdownWaitStage::ReleaseTailDrain)
                        }
                    };
                    if matches!(settled, Settlement::Unsettled(_)) {
                        // Not proven finished: leave the handle in place so a
                        // repeated call stays a safe no-op (a FINISHED handle
                        // is joined directly on re-entry).
                        return settled;
                    }
                }
            }
        }
        None => {
            // Awaited THROUGH `&mut` (same borrow discipline as the bounded
            // branch above): the handle stays with the caller.
            settled = reaper_join_settlement((&mut task.handle).await, false, report);
        }
    }
    // Joined (or aborted and joined): the handle is consumed, and polling a
    // finished `JoinHandle` again panics. Forget it so a later drain does
    // not re-await it. A deadline that drops the caller's future mid-wait
    // never reaches here and leaves the handle in place.
    *reaper = None;
    settled
}

/// The reaper and tail bounds for one release-source stop, computed by the
/// caller (each call site bounds the waits with ITS stage deadline).
pub(in crate::engine::server) struct ReleaseStopBounds {
    /// The reaper join: `None` = the original unbounded join.
    pub(in crate::engine::server) reaper: Option<(tokio::time::Instant, Duration)>,
    /// The tail drain's absolute deadline and reported budget.
    pub(in crate::engine::server) tail_deadline: tokio::time::Instant,
    pub(in crate::engine::server) tail_budget: Duration,
}

/// THE ordered stop: stop the release sources (join the reaper), clear the
/// registry so every lingering transaction's `Drop` release is REGISTERED
/// while tail admission is still open, THEN close + drain the tails.
///
/// Called from the normal shutdown (before the repository flush) and from
/// BOTH launch-rollback arms (listener bind failure; observability port
/// failure — in the second one connections may already have opened
/// transactions). Returns [`Settlement::Settled`] when the reaper AND the
/// tail drain both PROVED settled. When the reaper did not settle, the
/// registry is NOT cleared and the tail admission is NOT closed (a producer
/// may still register releases) — the caller keeps the lock and no log line
/// claims the sources were drained. The caller records the typed stage from
/// the returned [`Settlement::Unsettled`].
pub(in crate::engine::server) async fn stop_release_sources_and_drain_tails(
    reaper: &mut Option<ReaperTask>,
    registry: Option<&Arc<TxRegistry>>,
    tails: Option<&Arc<ReleaseTails>>,
    bounds: ReleaseStopBounds,
    context: &str,
    report: &mut ShutdownReport,
) -> Settlement {
    let settled = join_tx_reaper(reaper, bounds.reaper, report).await;
    if settled != Settlement::Settled {
        return settled;
    }
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
    settled.worst(
        super::release_tail_drain::drain_release_tails(
            tails.map(|tails| tails.as_ref()),
            true,
            bounds.tail_deadline,
            bounds.tail_budget,
            context,
            report,
        )
        .await,
    )
}

/// A reaper task that panicked, or was cancelled WITHOUT our abort, is NOT
/// proof that the producer ended; only a clean join (or the expected
/// cancelled end of our own abort) settles.
fn reaper_join_settlement(
    joined: Result<(), tokio::task::JoinError>,
    after_abort: bool,
    report: &mut ShutdownReport,
) -> Settlement {
    match joined {
        Ok(()) => Settlement::Settled,
        Err(error) if error.is_cancelled() && after_abort => Settlement::Settled,
        Err(error) => {
            if error.is_cancelled() {
                report.add_error(
                    "interactive transaction reaper task was cancelled without a clean join: \
                     the producer is NOT proven ended"
                        .to_string(),
                );
            } else {
                report.add_error(format!(
                    "interactive transaction reaper task failed: {error}"
                ));
            }
            Settlement::Unsettled(ShutdownWaitStage::ReleaseTailDrain)
        }
    }
}

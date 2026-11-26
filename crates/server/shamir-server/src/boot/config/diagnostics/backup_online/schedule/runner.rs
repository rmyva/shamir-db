use std::sync::Arc;
use std::time::Duration;

use shamir_types::time::local_minute_of_day;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::{sleep_until, Instant};
use tokio_util::sync::CancellationToken;

use super::super::metrics::{enable_schedule, note_scheduled, ScheduleInfo, ScheduledResult};
use super::super::progress_attribution::unix_nanos;
use super::super::registry::ScheduledRunSignals;
use super::super::{OnlineBackupRegistry, StartBackupError};
use crate::config::{DailyWindow, ResolvedSchedule};

/// Clears this scheduler's schedule cells on ANY exit of the task's future:
/// normal end, unwind, or future drop (task abort). Checked against the
/// scheduler's generation, so an older exit never clears a newer schedule.
/// The handle is the OWNING REGISTRY's `ScheduleState` (item 3): an exit
/// clears only its own registry's cell, never another registry's section.
struct ScheduleCellsGuard {
    state: Arc<super::super::metrics::ScheduleState>,
    generation: u64,
}

impl Drop for ScheduleCellsGuard {
    fn drop(&mut self) {
        self.state.clear_schedule_state_if(self.generation);
    }
}

pub(crate) fn spawn(
    schedule: ResolvedSchedule,
    registry: Arc<OnlineBackupRegistry>,
    owner: String,
    shutdown: CancellationToken,
) -> JoinHandle<()> {
    spawn_with_observer(schedule, registry, owner, shutdown, |_| {}, || {})
}

#[cfg(test)]
pub(crate) fn spawn_for_test(
    mut schedule: ResolvedSchedule,
    registry: Arc<OnlineBackupRegistry>,
    owner: String,
    shutdown: CancellationToken,
    interval: Duration,
    on_terminal: impl FnMut(bool) + Send + 'static,
    on_finished: impl FnMut() + Send + 'static,
) -> JoinHandle<()> {
    schedule.interval = interval;
    spawn_with_observer(
        schedule,
        registry,
        owner,
        shutdown,
        on_terminal,
        on_finished,
    )
}

fn spawn_with_observer(
    schedule: ResolvedSchedule,
    registry: Arc<OnlineBackupRegistry>,
    owner: String,
    shutdown: CancellationToken,
    mut on_terminal: impl FnMut(bool) + Send + 'static,
    mut on_finished: impl FnMut() + Send + 'static,
) -> JoinHandle<()> {
    let boot = Instant::now();
    let boot_unix_nanos = unix_nanos();
    let schedule_state = registry.schedule_cell();
    let generation = schedule_state.install_schedule_info(ScheduleInfo {
        interval_secs: schedule.interval.as_secs(),
        window: schedule.window.as_ref().map(DailyWindow::label),
        keep_last: schedule.retention.keep_last,
        run_on_start: schedule.run_on_start,
    });
    enable_schedule();
    let cells_guard = ScheduleCellsGuard {
        state: Arc::clone(&schedule_state),
        generation,
    };
    let boot_for_publish = boot;
    tokio::spawn(async move {
        let _cells_guard = cells_guard;
        run_with_anchor(
            (
                boot,
                schedule.interval,
                schedule.run_on_start,
                schedule.window,
            ),
            shutdown,
            local_minute_of_day,
            move || {
                registry.start_scheduled(
                    schedule.destination.clone(),
                    owner.clone(),
                    schedule.retention,
                )
            },
            move |result| {
                note_scheduled(result);
                match result {
                    ScheduledResult::Completed => on_terminal(true),
                    ScheduledResult::Failed => on_terminal(false),
                    _ => {}
                }
            },
            move |next| {
                // The adapter boundary is HERE: the wall-clock stamp stays
                // u128 into the cell, so an interval beyond u64::MAX nanos
                // keeps its real value (0 still means "no next run").
                let nanos = next
                    .map(|deadline| {
                        boot_unix_nanos.saturating_add(
                            deadline
                                .saturating_duration_since(boot_for_publish)
                                .as_nanos(),
                        )
                    })
                    .unwrap_or(0);
                schedule_state.note_next_run_unix_ns(generation, nanos);
            },
            &mut on_finished,
        )
        .await;
    })
}

/// One admitted run the scheduler is still tracking. The slot clears only on
/// the LATE `finished` signal, so a tick during retention is still busy.
struct InFlight {
    outcome: oneshot::Receiver<bool>,
    finished: oneshot::Receiver<()>,
    /// The COPY outcome was already noted; only `finished` is still awaited.
    outcome_received: bool,
}

/// Which of a run's two signals arrived first.
enum Signal {
    Outcome(Result<bool, oneshot::error::RecvError>),
    Finished,
}

/// Test-only entry: the runner tests call this wrapper. Unit tests own no global schedule cells, so publication is a no-op.
#[cfg(test)]
pub(super) async fn run(
    (boot, interval, run_on_start): (Instant, Duration, bool),
    window: Option<DailyWindow>,
    shutdown: CancellationToken,
    local_minute: impl FnMut() -> u32,
    start: impl FnMut() -> Result<ScheduledRunSignals, StartBackupError>,
    note: impl FnMut(ScheduledResult),
) {
    run_with_anchor(
        (boot, interval, run_on_start, window),
        shutdown,
        local_minute,
        start,
        note,
        |_: Option<Instant>| {},
        || {},
    )
    .await;
}

async fn run_with_anchor(
    (boot, interval, run_on_start, window): (Instant, Duration, bool, Option<DailyWindow>),
    shutdown: CancellationToken,
    mut local_minute: impl FnMut() -> u32,
    mut start: impl FnMut() -> Result<ScheduledRunSignals, StartBackupError>,
    mut note: impl FnMut(ScheduledResult),
    mut publish_next: impl FnMut(Option<Instant>),
    // Test-only completion handshake: fired when the loop's `Signal::Finished`
    // clears the in-flight slot, and again after the post-loop drain's
    // `run.finished.await` (at most one of the two fires per run, and a run
    // that never admits one fires only via the post-loop drain). Production
    // callers pass a no-op; tests await it instead of polling for completion.
    mut on_finished: impl FnMut(),
) {
    let mut next = if run_on_start {
        Some(boot)
    } else {
        boot.checked_add(interval)
    };
    publish_next(next);
    let mut in_flight: Option<InFlight> = None;
    while let Some(deadline) = next {
        tokio::select! {
            biased;
            () = shutdown.cancelled() => break,
            signal = advance(&mut in_flight) => match signal {
                Signal::Outcome(result) => {
                    if let Some(run) = in_flight.as_mut() {
                        run.outcome_received = true;
                    }
                    note(outcome_result(result));
                }
                Signal::Finished => {
                    in_flight = None;
                    on_finished();
                }
            },
            () = async {
                // Already-due ticks must not wait for Tokio's timer granularity.
                if deadline > Instant::now() {
                    sleep_until(deadline).await;
                }
            } => {
                if shutdown.is_cancelled() {
                    break;
                }
                if window.as_ref().is_some_and(|window| !window.contains(local_minute())) {
                    note(ScheduledResult::SkippedWindow);
                } else if in_flight.is_some() {
                    note(ScheduledResult::SkippedBusy);
                } else {
                    match start() {
                        Ok(signals) => {
                            in_flight = Some(InFlight {
                                outcome: signals.outcome,
                                finished: signals.finished,
                                outcome_received: false,
                            });
                            note(ScheduledResult::Started);
                        }
                        Err(StartBackupError::AlreadyRunning) => note(ScheduledResult::SkippedBusy),
                        Err(_) => note(ScheduledResult::Failed),
                    }
                }
                next = next_deadline(deadline, interval, Instant::now());
                publish_next(next);
            }
        }
    }
    if next.is_none() {
        tracing::warn!("scheduled backup stopped: next deadline is not representable");
    }
    if let Some(run) = in_flight {
        // Drain the admitted run: exactly one outcome note, then the late
        // signal once retention and the final publication are over.
        if !run.outcome_received {
            note(outcome_result(run.outcome.await));
        }
        let _ = run.finished.await;
        on_finished();
    }
}

/// cancel-safe: yes — polling borrows the run's receivers; a losing branch
/// leaves them available to drain.
async fn advance(in_flight: &mut Option<InFlight>) -> Signal {
    let Some(run) = in_flight.as_mut() else {
        return std::future::pending().await;
    };
    let outcome_received = run.outcome_received;
    let outcome = &mut run.outcome;
    let finished = &mut run.finished;
    // Precedence keeps one note per run: a FINISHED that races ahead of the
    // outcome (the unwind path) waits until the outcome has been noted.
    tokio::select! {
        result = async {
            if outcome_received {
                std::future::pending::<Result<bool, oneshot::error::RecvError>>().await
            } else {
                outcome.await
            }
        } => Signal::Outcome(result),
        result = async {
            if outcome_received {
                finished.await
            } else {
                std::future::pending::<Result<(), oneshot::error::RecvError>>().await
            }
        } => {
            let _ = result;
            Signal::Finished
        },
    }
}

fn terminal(result: Result<bool, oneshot::error::RecvError>) -> ScheduledResult {
    if matches!(result, Ok(true)) {
        ScheduledResult::Completed
    } else {
        ScheduledResult::Failed
    }
}

fn outcome_result(result: Result<bool, oneshot::error::RecvError>) -> ScheduledResult {
    // `false` means the protocol outcome was Err: registry.rs:693-700 derives
    // it from `outcome.is_err()` before publishing `!failed`; it is not a cancel signal.
    terminal(result)
}

pub(super) fn next_deadline(
    deadline: Instant,
    interval: Duration,
    now: Instant,
) -> Option<Instant> {
    const NANOS_PER_SECOND: u128 = 1_000_000_000;
    let period = interval.as_nanos();
    let elapsed = now.saturating_duration_since(deadline).as_nanos();
    let delay = period - elapsed.checked_rem(period)?;
    let secs = u64::try_from(delay / NANOS_PER_SECOND).ok()?;
    let nanos = u32::try_from(delay % NANOS_PER_SECOND).ok()?;
    now.checked_add(Duration::new(secs, nanos))
}

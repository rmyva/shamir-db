//! One bounded, truthful audit teardown shared by every shutdown caller.
//!
//! The order is the guarantee: admission is closed and the scheduler is
//! signalled first, the refusal-audit queue is drained, and only then is the
//! scheduler JOINED — before the appender's flusher is joined and before its
//! final drain, anchor write and fjall `SyncAll`. A scheduled audit
//! checkpoint therefore can no longer interleave with the final log sync and
//! anchor write. That final flush acquires the same synchronous persistence
//! gate a checkpoint takes, on its OWN dedicated thread (never a ticket in the
//! blocking pool a cancelled refusal job can saturate), and it takes it only
//! after the scheduler has stopped, so the last close runs under the same gate
//! discipline as a checkpoint.
//!
//! The audit stage owns the two things that can stall a stop: the
//! refusal-audit queue (accepted `spawn_blocking` append jobs) and the audit
//! appender (its background flusher plus that final drain, anchor write and
//! `SyncAll`). The scheduler join, the flusher join and the final flush are
//! all bounded by the one [`AUDIT_TEARDOWN_DEADLINE`], the blocking part runs
//! off the shutdown future's own poll (the final close on a dedicated thread,
//! the scheduler's checkpoint ticks on the blocking pool), and
//! every failure the appender used to discard — flusher join, final flush,
//! chain anchor, server-meta hook — reaches [`ShutdownReport`] once, at the
//! moment it is observed.
//!
//! A timeout stops waiting; it does not cancel an in-flight `fsync` on the
//! blocking thread. Teardown continues to the repository flush regardless:
//! the refusal reply and the refusal's own audit append never depend on this
//! stage.
//!
//! The one rule a deadline can break: the final close (final drain, anchor and
//! server-metadata write) is submitted only when BOTH the scheduler and the
//! flusher stopped inside the deadline. If either timed out, its task may still
//! be inside the persistence gate, so a final close submitted now could run
//! BEFORE it. In that case no close is submitted at all: the report says the
//! buffered events and the anchor were not persisted by this shutdown, and the
//! next start recovers the chain exactly as it does after a crash.

#[cfg(test)]
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use shamir_db::ShutdownReport;

use crate::audit_appender::{FjallAuditAppender, FlusherJoinFailure};
use crate::db_handler::CancelRefusalAudit;
use crate::scheduler::Scheduler;

/// One shared budget for the whole audit teardown: the refusal drain, the
/// scheduler join, the flusher join and the final flush + anchor write. 10 s
/// because the drain below may spend its own 3 s first and a loaded CI runner
/// fsyncs an order of magnitude slower than a workstation; the repository
/// flush that follows has its own budget, so this stage must never consume
/// the outer `runtime::serve` deadline on its own.
pub(crate) const AUDIT_TEARDOWN_DEADLINE: Duration = Duration::from_secs(10);

// One fixed prefix per distinct failure. `ShutdownReport` stores plain
// strings, so this prefix is the only thing an operator or a supervisor can
// grep for; each failure is reported once, at the moment it is observed, so a
// later timeout can never swallow an earlier error and no caller has to
// reinterpret a message.
const ERR_AUDIT_TIMEOUT: &str = "audit teardown exceeded";
const ERR_FLUSHER_JOIN: &str = "audit flusher task did not join cleanly";
const ERR_FLUSH: &str = "audit flush failed at shutdown";
const ERR_ANCHOR: &str = "audit anchor or metadata write failed at shutdown";
const ERR_FLUSH_TASK: &str = "audit flush task did not complete";
const ERR_SCHEDULER_STOP: &str = "scheduler did not stop within";
const ERR_FINAL_CLOSE_SKIPPED: &str = "audit final close not attempted";
const ERR_REFUSAL_DRAIN: &str = "refusal audit drain expired";

// Test-only one-shot probe fired at the ENTRY of [`teardown_audit`], before
// admission is closed. A rollback test arms it to prove the shared audit
// teardown actually ran on its path (mutant: the `teardown_audit` call removed
// from a launch-rollback branch would otherwise be invisible).
#[cfg(test)]
static TEARDOWN_PROBE: std::sync::Mutex<Option<Box<dyn FnOnce() + Send>>> =
    std::sync::Mutex::new(None);

/// Clears an unfired [`arm_teardown_probe`]; a probe that fires removes itself.
#[cfg(test)]
pub(crate) struct TeardownProbeGuard;

#[cfg(test)]
impl Drop for TeardownProbeGuard {
    fn drop(&mut self) {
        *TEARDOWN_PROBE.lock().expect("teardown probe") = None;
    }
}

/// Arm ONE teardown-entry probe for the next [`teardown_audit`] call.
#[cfg(test)]
pub(crate) fn arm_teardown_probe(probe: impl FnOnce() + Send + 'static) -> TeardownProbeGuard {
    *TEARDOWN_PROBE.lock().expect("teardown probe") = Some(Box::new(probe));
    TeardownProbeGuard
}

/// Fire and consume the current teardown-entry probe, if any.
#[cfg(test)]
fn fire_teardown_probe() {
    let probe = TEARDOWN_PROBE.lock().expect("teardown probe").take();
    if let Some(probe) = probe {
        probe();
    }
}

/// Test seam: ONE error injected INTO the real teardown's report just before
/// [`teardown_audit`] returns, so the rollback's log assertion pins the
/// teardown-produced report itself — a caller that discards teardown's entries
/// or logs a different report object misses the injected error and fails.
/// LIMIT: one injection per process — under nextest (one process per test) only
/// the intended test can arm it. Consumed exactly once; the guard clears a
/// leftover so a failed assertion cannot leak it into another test.
#[cfg(test)]
static INJECTED_TEARDOWN_ERROR: std::sync::Mutex<Option<String>> = std::sync::Mutex::new(None);

/// Arming guard: clears an unconsumed injection on drop.
#[cfg(test)]
pub(crate) struct InjectedTeardownErrorGuard;

#[cfg(test)]
pub(crate) fn inject_teardown_error(message: String) -> InjectedTeardownErrorGuard {
    *INJECTED_TEARDOWN_ERROR
        .lock()
        .expect("injected teardown error lock") = Some(message);
    InjectedTeardownErrorGuard
}

#[cfg(test)]
fn take_injected_teardown_error() -> Option<String> {
    INJECTED_TEARDOWN_ERROR
        .lock()
        .expect("injected teardown error lock")
        .take()
}

#[cfg(test)]
impl Drop for InjectedTeardownErrorGuard {
    fn drop(&mut self) {
        take_injected_teardown_error();
    }
}

/// The latched cause behind an appender error display.
///
/// The final close re-reads the latch as `AppenderError::Io`, whose display
/// adds `io: `, while the flusher's join error carries the latch text as it is:
/// the two strings name the SAME failure and must be compared on the cause.
fn latched_cause(message: &str) -> &str {
    message.strip_prefix("io: ").unwrap_or(message)
}

/// Whether `message` repeats a cause an earlier report already named.
fn already_reported(reported: &Option<String>, message: &str) -> bool {
    reported
        .as_deref()
        .is_some_and(|earlier| latched_cause(earlier) == latched_cause(message))
}

/// The three audit owners a shutdown must drain, in teardown order.
pub(crate) struct AuditTeardown<'a> {
    pub(crate) cancel_refusal_audit: &'a Arc<CancelRefusalAudit>,
    pub(crate) appender: &'a Arc<FjallAuditAppender>,
    pub(crate) scheduler: Scheduler,
    /// Test seam: fires the first time the REFUSAL DRAIN itself polls
    /// `Pending`, so the refusal-drain binding test can release its gate at
    /// the exact moment teardown waits for the queue.
    #[cfg(test)]
    pub(crate) drain_probe: Option<tokio::sync::oneshot::Sender<()>>,
}

/// Close admission, drain, join the scheduler, join the flusher and flush the
/// audit stage under one deadline.
///
/// Returns `true` when the stage completed inside the deadline and its final
/// close ran. Either way teardown is finished with the audit owners: the
/// scheduler has been told to stop and joined (or abandoned), the flusher has
/// been stopped and joined (or abandoned), and every failure has been recorded
/// in `report` once, at the moment it was observed. The final close runs only
/// after BOTH stoppers completed; if either timed out, [`ERR_FINAL_CLOSE_SKIPPED`]
/// is reported instead and nothing is submitted.
pub(crate) async fn teardown_audit(parts: AuditTeardown<'_>, report: &mut ShutdownReport) -> bool {
    let deadline = tokio::time::Instant::now() + AUDIT_TEARDOWN_DEADLINE;
    #[cfg(test)]
    fire_teardown_probe();
    parts.cancel_refusal_audit.close();
    // Signal before joining: the scheduler has its own broadcast, not the
    // root shutdown token, so a join alone would wait for the next tick.
    parts.scheduler.signal_shutdown();

    let mut complete = true;
    let mut drain_settled = true;

    // Cancel-safe: nothing is consumed by waiting. On expiry the queued jobs
    // are aborted below; the ones already inside the writer keep running.
    let refusal = parts.cancel_refusal_audit;
    #[cfg(test)]
    let drained = drain_refusal(refusal, parts.drain_probe).await;
    #[cfg(not(test))]
    let drained = drain_refusal(refusal).await;
    if !drained {
        // The drain could not establish that no admitted work remains: an append
        // already inside the writer may reach its own inline checkpoint after
        // any final close we submit now. Remember it for the close rule below.
        drain_settled = false;
        refusal.discard_queued();
        // Accepted refusal events may just have been discarded: a clean report
        // would hide it (the drop counter keeps the number).
        report.add_error(format!(
            "{ERR_REFUSAL_DRAIN}: accepted refusal audit events that had not started were \
             discarded and the ones already running are no longer awaited"
        ));
        complete = false;
    }

    // Join the scheduler BEFORE the flusher join and the final flush: a
    // scheduled audit checkpoint checked in after `close()` could otherwise
    // interleave with the final log sync and the anchor write.
    let mut scheduler_stopped = true;
    if tokio::time::timeout_at(deadline, parts.scheduler.join())
        .await
        .is_err()
    {
        report.add_error(format!("{ERR_SCHEDULER_STOP} {AUDIT_TEARDOWN_DEADLINE:?}"));
        complete = false;
        scheduler_stopped = false;
    }

    parts.appender.request_flusher_stop();
    let mut flusher_stopped = true;
    // ONE report per distinct failure: the flusher's join error text and the
    // final close's latched `flush_error` are the same latch, so the second
    // observation of that message is suppressed instead of being reported a
    // second time under another prefix.
    let mut reported_flusher: Option<String> = None;
    match tokio::time::timeout_at(deadline, parts.appender.join_flusher()).await {
        // A join that returned an error still stopped the task, so the final
        // close may run; its buffered entries are drained by that close.
        Ok(Ok(())) => {}
        Ok(Err(failure)) => {
            // The VARIANT decides whether the blocking work provably stopped,
            // never a string comparison with the latch: an inner job failure
            // was reported by a task that completed (the close may run), a
            // lost task proves nothing (the close must not).
            flusher_stopped = matches!(failure, FlusherJoinFailure::InnerBlockingJobFailed(_));
            let message = failure.into_message();
            report.add_error(format!("{ERR_FLUSHER_JOIN}: {message}"));
            reported_flusher = Some(message);
            complete = false;
        }
        Err(_) => {
            report.add_error(format!(
                "{ERR_AUDIT_TIMEOUT} {AUDIT_TEARDOWN_DEADLINE:?} waiting for the audit flusher"
            ));
            complete = false;
            flusher_stopped = false;
        }
    }

    // ONE rule: the final close is submitted only when every stopper settled —
    // the scheduler, the audit flusher AND the refusal drain. A timed-out task
    // can still be inside the persistence gate, and an admitted refusal append
    // can still reach its own inline checkpoint, so a close submitted now would
    // run before one of them — the ordering this stage exists for would be
    // inverted on exactly the path where it matters most.
    if !(scheduler_stopped && flusher_stopped && drain_settled) {
        report.add_error(format!(
            "{ERR_FINAL_CLOSE_SKIPPED}: the scheduler, the audit flusher or the refusal-audit \
             drain did not settle inside {AUDIT_TEARDOWN_DEADLINE:?}, so the final audit drain, \
             the chain anchor write and the server-metadata write were not started; the last \
             buffered audit events and the anchor are not persisted by this shutdown, and the \
             next start recovers the chain exactly as it does after a crash"
        ));
        // A background flush error that latched BEFORE this point would stay a
        // log line only: the step that reports it is the final close this
        // branch skips. Carry the latch into the report, once.
        if let Some(message) = parts.appender.last_failure() {
            if !already_reported(&reported_flusher, &message) {
                report.add_error(format!("{ERR_FLUSH}: {message}"));
            }
        }
        return complete;
    }

    // The final drain + anchor write is synchronous (gate + fsync) and it must
    // not depend on the shared blocking pool: cancelled refusal jobs still
    // queued there keep their writer references until a worker takes them out,
    // so a saturated pool would make THIS stage time out before the close even
    // started. It therefore runs on its own thread — one dedicated thread for
    // the process's last durable write, never a pool ticket — and stays bounded
    // by the shared deadline. Dropping the join handle detaches the thread: a
    // timed-out close keeps its `fsync` and its gate exactly like an in-flight
    // flush parked on the pool did.
    let appender = Arc::clone(parts.appender);
    let (flush_tx, flush_rx) = tokio::sync::oneshot::channel();
    let dedicated = std::thread::Builder::new()
        .name("audit-final-close".to_string())
        .spawn(move || {
            let outcome = appender.final_flush_blocking();
            // Release this thread's appender reference BEFORE publishing the
            // result: the appender owns the checkpoint store and the
            // `server_meta` hook, so a relaunch right after the reported
            // close must not race the thread's drop.
            drop(appender);
            let _ = flush_tx.send(outcome);
        });
    let flush = match dedicated {
        Ok(_thread) => tokio::time::timeout_at(deadline, flush_rx).await,
        Err(spawn_error) => {
            report.add_error(format!(
                "{ERR_FLUSH_TASK}: the dedicated final-close thread could not be spawned ({})",
                spawn_error.kind()
            ));
            return false;
        }
    };
    match flush {
        Ok(Ok(outcome)) => {
            if let Some(message) = outcome.flush_error {
                if !already_reported(&reported_flusher, &message) {
                    report.add_error(format!("{ERR_FLUSH}: {message}"));
                }
                complete = false;
            }
            if let Some(message) = outcome.anchor_error {
                report.add_error(format!("{ERR_ANCHOR}: {message}"));
                complete = false;
            }
        }
        Ok(Err(_cancelled)) => {
            // The dedicated thread panicked before it could report: its entries
            // are gone, so this must reach the report.
            report.add_error(format!(
                "{ERR_FLUSH_TASK}: the dedicated final-close thread ended without a result"
            ));
            complete = false;
        }
        Err(_) => {
            report.add_error(format!(
                "{ERR_AUDIT_TIMEOUT} {AUDIT_TEARDOWN_DEADLINE:?} waiting for the final audit \
                 flush — an in-flight flush is not cancelled"
            ));
            complete = false;
        }
    }

    // Test seam: the teardown's OWN report carries the injected error before
    // this function returns, so the rollback's log assertion pins the report
    // teardown produced, not one assembled after it.
    #[cfg(test)]
    if let Some(injected) = take_injected_teardown_error() {
        report.add_error(injected);
    }
    complete
}

/// Test seam: fire `probe` the first time the REFUSAL DRAIN itself polls
/// `Pending`, so the refusal-drain binding test releases its gate at the
/// exact moment teardown waits for the queue — never for a later stage.
#[cfg(test)]
async fn drain_refusal(
    queue: &Arc<CancelRefusalAudit>,
    probe: Option<tokio::sync::oneshot::Sender<()>>,
) -> bool {
    let drain = queue.drain();
    tokio::pin!(drain);
    let mut probe = probe;
    std::future::poll_fn(move |cx| {
        let result = drain.as_mut().poll(cx);
        if result.is_pending() {
            if let Some(signal) = probe.take() {
                let _ = signal.send(());
            }
        }
        result
    })
    .await
}

/// Cancel-safe refusal drain (no test seam in production builds).
#[cfg(not(test))]
async fn drain_refusal(queue: &Arc<CancelRefusalAudit>) -> bool {
    queue.drain().await
}

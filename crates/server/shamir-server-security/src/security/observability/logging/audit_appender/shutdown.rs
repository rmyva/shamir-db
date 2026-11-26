//! Ordered audit-appender shutdown: the stop request, the flusher join and
//! the final drain + anchor write as three separately observable steps.
//!
//! The persistence gate is a synchronous `parking_lot` mutex and the final
//! drain fsyncs, so a stalled checkpoint holds the gate for as long as its
//! own I/O takes. Splitting the steps lets the server put the blocking part
//! on a blocking thread, bound the whole stage with one deadline and report
//! which step failed instead of discarding the result. The gate rules
//! themselves, the anchor write and the latched fail-closed behaviour are
//! unchanged.

use super::{Durab, FjallAuditAppender};
use std::sync::atomic::Ordering;
#[cfg(any(test, feature = "test-hooks"))]
use std::sync::Arc;

mod flusher_join_failure;
pub use flusher_join_failure::FlusherJoinFailure;

/// Results of the appender's shutdown stage, in step order.
///
/// Deliberately not `#[must_use]`: boot-rollback and test callers that do
/// not own a shutdown report keep discarding it exactly as before.
#[derive(Clone, Debug, Default)]
pub struct AuditShutdownOutcome {
    /// The background flusher join failure: either its completed task reported
    /// an inner blocking-job failure (blocking work ended), or the outer task
    /// was lost to panic/cancellation (blocking work may still be running).
    pub flusher_join_error: Option<FlusherJoinFailure>,
    /// The final drain or its log sync failed. The appender is latched
    /// fail-closed either way.
    pub flush_error: Option<String>,
    /// The chain anchor write or the server-meta hook failed. The appender
    /// is latched fail-closed either way.
    pub anchor_error: Option<String>,
}

impl AuditShutdownOutcome {
    /// All three steps completed without an error.
    pub fn is_ok(&self) -> bool {
        self.flusher_join_error.is_none()
            && self.flush_error.is_none()
            && self.anchor_error.is_none()
    }
}

/// One final-close result, split by step so the caller reports an anchor or
/// metadata failure as such instead of as a plain flush failure.
#[derive(Clone, Debug, Default)]
pub struct AuditFinalFlush {
    /// The final drain of the buffer or its log sync failed.
    pub flush_error: Option<String>,
    /// The chain anchor row or the server-metadata hook failed.
    pub anchor_error: Option<String>,
}

impl AuditFinalFlush {
    /// Both steps completed without an error.
    pub fn is_ok(&self) -> bool {
        self.flush_error.is_none() && self.anchor_error.is_none()
    }
}

impl FjallAuditAppender {
    /// Ask the background flusher to stop and wake it.
    ///
    /// Synchronous and gate-free: it only flips the stop flag and notifies,
    /// so it is safe while another thread holds the persistence gate.
    pub fn request_flusher_stop(&self) {
        if let Durab::Batched {
            shutdown, notify, ..
        } = &self.mode
        {
            shutdown.store(true, Ordering::SeqCst);
            notify.notify_one();
        }
    }

    /// Await the background flusher's join handle.
    ///
    /// Pure async join: it takes no gate and blocks no worker thread, so a
    /// flusher parked on the persistence gate is bounded by the caller's
    /// deadline rather than by the gate holder's I/O.
    ///
    /// Distinguishes an inner blocking-job failure reported by a completed
    /// task from an outer task that was lost to panic or cancellation.
    pub async fn join_flusher(&self) -> Result<(), FlusherJoinFailure> {
        let handle = match &self.mode {
            Durab::Batched { task, .. } => task.lock().take(),
            Durab::Strict => None,
        };
        let Some(handle) = handle else {
            return Ok(());
        };
        match handle.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(message)) => Err(FlusherJoinFailure::InnerBlockingJobFailed(message)),
            Err(error) => Err(FlusherJoinFailure::TaskLost(error.to_string())),
        }
    }

    /// Final batched drain, anchor persistence and server-meta hook,
    /// reported per step.
    ///
    /// Synchronous by design: it takes the persistence gate itself and
    /// fsyncs, so the caller runs it on a blocking thread and the shutdown
    /// future's poll never waits for whoever holds the gate. Both helpers it
    /// calls require the caller to own the gate, so the gate is acquired
    /// here. Strict mode does nothing (entries are already durable) and
    /// touches no gate. A failed drain skips the anchor, so the anchor cannot
    /// advance past lost log records; retention sweeps stay rotation-only.
    /// Either failure latches fail-closed.
    pub fn final_flush_blocking(&self) -> AuditFinalFlush {
        #[cfg(any(test, feature = "test-hooks"))]
        self.final_close_attempts.fetch_add(1, Ordering::SeqCst);
        if let Durab::Strict = &self.mode {
            return AuditFinalFlush::default();
        }
        let _guard = self.acquire_persistence_gate();
        // `flush_buffer_locked` already latches its own failure; its error
        // also means the anchor must not move.
        let flush_error = match self.flush_buffer_locked() {
            Ok(()) => None,
            Err(error) => Some(error.to_string()),
        };
        if flush_error.is_some() {
            return AuditFinalFlush {
                flush_error,
                anchor_error: None,
            };
        }
        // The anchor/metadata step does not latch on its own (it also runs at
        // rotation), so latch it here exactly as the single result did.
        let anchor_error = match self.persist_anchor_and_meta() {
            Ok(()) => None,
            Err(error) => {
                let message = error.to_string();
                *self.failed.lock() = Some(message.clone());
                Some(message)
            }
        };
        AuditFinalFlush {
            flush_error: None,
            anchor_error,
        }
    }
}

/// Test-only arming of the persistence-gate hook across the crate
/// boundary: the hook runs while the caller already owns the gate, so a
/// test can hold a REAL checkpoint inside it and observe a bounded
/// teardown.
#[cfg(any(test, feature = "test-hooks"))]
impl FjallAuditAppender {
    /// Arm the one-shot hook run immediately after the persistence gate is
    /// acquired.
    pub fn arm_gate_acquired_hook(&self, hook: Arc<dyn Fn() + Send + Sync>) {
        *self.gate_acquired_hook.lock() = Some(hook);
    }

    /// Make the next actual flush fail (fault injection).
    pub fn arm_fail_next_flush(&self) {
        self.fail_next_flush.store(true, Ordering::SeqCst);
    }

    /// How many final closes were SUBMITTED by this appender.
    ///
    /// The ordered-shutdown rule is that neither a stalled scheduler nor a
    /// stalled flusher may be followed by a final close; this counter is the
    /// observable for that rule (0 means the close was never started).
    pub fn final_close_attempts(&self) -> usize {
        self.final_close_attempts.load(Ordering::SeqCst)
    }
}

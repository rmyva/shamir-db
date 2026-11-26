//! Admission-fenced owner of the handler's detached mutation workers.
//!
//! `tokio_util::TaskTracker::close` is NOT an admission fence: a worker
//! spawned into the tracker after `close()` (a write request admitted by an
//! accept loop in the same poll cycle as the shutdown cancel) is still
//! registered, and a `wait()` that already observed the empty tracker can
//! complete while that late worker keeps writing. Registration therefore
//! goes through a shutdown latch taken under the same mutex that admits the
//! task: after the latch closes, every further `spawn` is refused and the
//! request path answers the fixed `server_busy` wire text instead of
//! starting work the flush cannot wait for.

use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::task::JoinHandle;
use tokio_util::task::TaskTracker;

use shamir_tx::CancelHandle;

/// Explicit disconnect cancellation for a detached mutation worker that owns
/// the in-flight activity guard: the awaiting request future belongs to the
/// connection task, so if it is dropped mid-execution (disconnect, drain
/// abort) nobody else cancels the worker. Dropping an ARMED guard cancels the
/// worker through the shared [`CancelHandle`]; every refusal state
/// (`Committing`/`Done`/`Cancelled`) is already terminal and ignored.
///
/// The guard MUST be disarmed on normal worker completion: the engine does
/// not necessarily leave the handle terminal — a batch REJECTED during
/// authorization or planning (or one whose conditional mutations all skip)
/// returns while the handle is still `Running`, and a fire-on-drop there
/// would win the `Running → Cancelled` race against the waiter's
/// `finish()`, replacing the real response with `cancelled`. The guard fires
/// ONLY when the awaiting future is dropped mid-execution.
pub(crate) struct DisconnectCancel {
    cancel: Arc<CancelHandle>,
    /// Cleared by [`Self::disarm`] at the worker's completion boundary
    /// (after the awaiting future resumed from the join).
    armed: AtomicBool,
}

impl DisconnectCancel {
    pub(crate) fn new(cancel: Arc<CancelHandle>) -> Self {
        Self {
            cancel,
            armed: AtomicBool::new(true),
        }
    }

    /// Make the drop a no-op: the worker finished NORMALLY, so the awaiting
    /// future survived and nobody must cancel anything.
    pub(crate) fn disarm(&self) {
        self.armed.store(false, Ordering::Release);
    }
}

impl Drop for DisconnectCancel {
    fn drop(&mut self) {
        if self.armed.load(Ordering::Acquire) {
            let _ = self.cancel.try_cancel();
        }
    }
}

/// Owner of the detached mutation workers shared by every handler clone.
pub(crate) struct MutationTasks {
    tracker: TaskTracker,
    state: Mutex<MutationTasksState>,
    /// Test seam (per instance, consumed once): armed by a shutdown test,
    /// fired once when the shutdown join stage ENTERS `wait` — so the
    /// "shutdown stays pending while a worker is parked" claim is proved
    /// against THIS instance's join, never vacuously. Deliberately not a
    /// process-global static.
    #[cfg(test)]
    join_entry_probe: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}

#[derive(Default)]
struct MutationTasksState {
    closed: bool,
}

impl Default for MutationTasks {
    fn default() -> Self {
        Self::new()
    }
}

impl MutationTasks {
    pub(crate) fn new() -> Self {
        Self {
            tracker: TaskTracker::new(),
            state: Mutex::new(MutationTasksState::default()),
            #[cfg(test)]
            join_entry_probe: Mutex::new(None),
        }
    }

    /// Test seam: arm the one-shot join-entry acknowledgment for THIS
    /// instance (see the field docs).
    #[cfg(test)]
    pub(crate) fn arm_join_entry_probe(&self, probe: impl FnOnce() + Send + 'static) {
        *self.join_entry_probe.lock() = Some(Box::new(probe));
    }

    /// Admit one mutation worker. `None` after [`Self::close`]: shutdown is
    /// draining and the caller must refuse the request (no admission
    /// decision may depend on the tracker's own bookkeeping, which
    /// `close()` does not fence).
    pub(crate) fn spawn<F>(&self, future: F) -> Option<JoinHandle<F::Output>>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let state = self.state.lock();
        if state.closed {
            return None;
        }
        Some(self.tracker.spawn(future))
    }

    /// Close admission, then stop admitting forever. Idempotent; the same
    /// instance is closed by the inline shutdown stage and again by the
    /// post-deadline fallback drain.
    pub(crate) fn close(&self) {
        self.state.lock().closed = true;
        self.tracker.close();
    }

    /// Wait until every admitted worker finished. Only meaningful after
    /// [`Self::close`].
    pub(crate) async fn wait(&self) {
        // Fire (and consume) the instance's join-entry acknowledgment
        // BEFORE the wait, outside the lock: the probe may run arbitrary
        // test code.
        #[cfg(test)]
        {
            let probe = self.join_entry_probe.lock().take();
            if let Some(probe) = probe {
                probe();
            }
        }
        self.tracker.wait().await;
    }
}

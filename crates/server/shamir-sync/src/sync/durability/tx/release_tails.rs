//! Tracker for the detached record-lock release tails of interactive
//! transactions (`spawn_abort_release`): the operator-cancel release and the
//! registry `Drop` release of a transaction that ends outside the commit
//! pipeline.
//!
//! These tails used to be fully detached `tokio::spawn` tasks, so a shutdown
//! could flush the repositories and release the store directory locks while
//! one was still writing into a table's `MvccStore`. The tails are now
//! registered here; the server shutdown drains the owner before the
//! repository flush and again after the reaper teardown (whose `Drop`
//! releases can start late), and the post-deadline fallback drain in
//! `ShutdownOwner` waits for whatever is left.
//!
//! The shape mirrors `shamir-engine`'s cold-open owner and `ConnTasks`: an
//! atomic live count plus an idle `Notify`, with the close latch taken under
//! the same mutex that increments the count, so registering a tail is atomic
//! with shutdown admission (`tokio_util::TaskTracker::close` is not an
//! admission fence).

use std::future::Future;
use std::sync::Arc;

use parking_lot::Mutex;

use super::tx_registry::TxRegistry;

/// Owner of a transaction record-lock release tail. Cheap `Arc`-shared; one
/// instance per `TxRegistry`, shared by every parked transaction.
pub struct ReleaseTails {
    inner: Arc<ReleaseTailsInner>,
}

struct ReleaseTailsInner {
    state: Mutex<ReleaseTailsState>,
    idle: tokio::sync::Notify,
}

#[derive(Default)]
struct ReleaseTailsState {
    /// Set by [`ReleaseTails::close`]; refuses new admission so a drain can
    /// settle. Registration and the latch check run under one lock acquire.
    closed: bool,
    live: usize,
}

/// RAII live count; the drop notifies a drain waiting for zero.
struct LiveGuard(Arc<ReleaseTailsInner>);

impl Drop for LiveGuard {
    fn drop(&mut self) {
        let mut state = self.0.state.lock();
        state.live -= 1;
        if state.live == 0 {
            self.0.idle.notify_waiters();
        }
    }
}

impl Default for ReleaseTails {
    fn default() -> Self {
        Self::new()
    }
}

impl ReleaseTails {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(ReleaseTailsInner {
                state: Mutex::new(ReleaseTailsState::default()),
                idle: tokio::sync::Notify::new(),
            }),
        }
    }

    /// Admit one release tail as a detached task on the current runtime and
    /// return its join handle. `Err(work)` AFTER [`Self::close`]: shutdown is
    /// draining and the future is handed BACK to the caller un-consumed, so
    /// no lock release is ever silently dropped — the caller runs it inline
    /// or on its own fallback path. Registration and the latch check run
    /// under one lock acquire, so a close cannot land between them. Must be
    /// called inside a live tokio runtime.
    pub fn spawn_lossless<F>(&self, work: F) -> Result<tokio::task::JoinHandle<()>, F>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let mut state = self.inner.state.lock();
        if state.closed {
            return Err(work);
        }
        state.live += 1;
        drop(state);
        // Construct the RAII count BEFORE spawning and MOVE it into the task
        // body: a future the runtime drops before its first poll drops the
        // moved-in guard, so the live count cannot leak (a guard constructed
        // inside the body only exists after the first poll).
        let guard = LiveGuard(Arc::clone(&self.inner));
        Ok(tokio::spawn(async move {
            let _guard = guard;
            work.await;
        }))
    }

    /// Close admission. Idempotent; every drain follows a close.
    pub fn close(&self) {
        self.inner.state.lock().closed = true;
    }

    /// Wait until no registered tail is live, or `false` at `deadline`.
    pub async fn drain(&self, deadline: tokio::time::Instant) -> bool {
        loop {
            let notified = self.inner.idle.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.inner.state.lock().live == 0 {
                return true;
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return false;
            }
        }
    }
}

impl TxRegistry {
    /// Registry constructor: built with a release-tail owner so server
    /// shutdown can drain it.
    pub fn new() -> Self {
        let mut registry = Self::default();
        registry.release_tails = Some(Arc::new(ReleaseTails::new()));
        registry
    }

    /// The release-tail owner every transaction registered here hands its
    /// detached lock releases to (always `Some` on a registry built with
    /// [`TxRegistry::new`]). Shutdown drains it; REGISTRATION binds the
    /// same `Arc` into each `InteractiveTx` (see
    /// [`TxRegistry::register_activity`]) — there is no caller-managed
    /// binding, so a registered transaction can never miss its owner.
    pub fn release_tails(&self) -> Option<Arc<ReleaseTails>> {
        self.release_tails.clone()
    }
}

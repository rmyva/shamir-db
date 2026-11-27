//! Per-repository lifecycle owner for cold-table opens (`HandoffOnDrop`
//! handoff in `tx/commit/core/catalog_access.rs`).
//!
//! The handoff used to `tokio::spawn` the cold open fully detached, so a
//! shutdown could flush and close the repository's stores while a handed-off
//! open was still running recovery and publication under its admission
//! permit. The opens are now registered here BEFORE their first side effect;
//! repository shutdown (`RepoInstance::shutdown_clean`) closes admission at
//! shutdown ENTRY and drains the owner before commit quiescence and the
//! buffer flush.
//!
//! `tokio_util::TaskTracker` is unavailable in this crate (no dependency),
//! so the shape mirrors `shamir-sync`'s `ConnTasks`: an atomic live count
//! plus an idle `Notify`, and a close latch taken under the same mutex that
//! increments the count, so registering is atomic with shutdown admission.

use std::future::Future;
use std::sync::{Arc, Mutex};

/// Owner of one repository's detached cold opens. Cheap `Arc`-shared.
pub struct ColdOpenTasks {
    inner: Arc<Inner>,
}

struct Inner {
    state: Mutex<ColdOpenState>,
    idle: tokio::sync::Notify,
}

#[derive(Default)]
struct ColdOpenState {
    /// Set by [`ColdOpenTasks::close`]; refuses new admission so a drain can
    /// settle. Registration and the latch check run under one lock acquire.
    closed: bool,
    live: usize,
}

/// RAII live count for one admitted cold open. Taken by
/// [`ColdOpenTasks::register`] BEFORE the open's first side effect and moved
/// INTO the spawned task by [`ColdOpenTasks::spawn_registered`]: a future the
/// runtime drops before its first poll drops the moved-in registration, so
/// the count cannot leak. The drop notifies a drain waiting for zero.
pub struct ColdOpenRegistration(Arc<Inner>);

impl Drop for ColdOpenRegistration {
    fn drop(&mut self) {
        let mut state = self
            .0
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.live -= 1;
        if state.live == 0 {
            self.0.idle.notify_waiters();
        }
    }
}

impl Default for ColdOpenTasks {
    fn default() -> Self {
        Self::new()
    }
}

impl ColdOpenTasks {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                state: Mutex::new(ColdOpenState::default()),
                idle: tokio::sync::Notify::new(),
            }),
        }
    }

    /// Whether [`Self::close`] already closed admission. Fresh cold opens
    /// are refused with a typed error BEFORE they start (no admission permit,
    /// no store lookup) once shutdown is draining.
    pub fn is_closed(&self) -> bool {
        self.inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .closed
    }

    /// Admit one cold open BEFORE its first side effect (no store lookup, no
    /// recovery, no store open may precede registration). `None` after
    /// [`Self::close`]: shutdown is draining, the caller refuses. Ownership
    /// of the returned registration transfers with the open: pass it to
    /// [`Self::spawn_registered`] when the open is handed off, or drop it
    /// when the open completes inline.
    pub fn register(&self) -> Option<ColdOpenRegistration> {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.closed {
            return None;
        }
        state.live += 1;
        Some(ColdOpenRegistration(Arc::clone(&self.inner)))
    }

    /// Resume an ALREADY-REGISTERED cold open as a detached task, moving the
    /// registration INTO the task body. Registration happened before the
    /// open's first side effect, so this stays correct even after `close`
    /// (admission was granted atomically with the increment). The
    /// registration inside the queued future is dropped even when the
    /// runtime discards the task before its first poll, so the live count
    /// cannot leak. `false` only outside a live tokio runtime: the caller
    /// drops the registration together with the future.
    pub fn spawn_registered<F>(&self, registration: ColdOpenRegistration, work: F) -> bool
    where
        F: Future<Output = ()> + Send + 'static,
    {
        if tokio::runtime::Handle::try_current().is_err() {
            return false;
        }
        tokio::spawn(async move {
            let _registration = registration;
            work.await;
        });
        true
    }

    /// Close admission. Idempotent; every drain follows a close.
    pub fn close(&self) {
        self.inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .closed = true;
    }

    /// Reopen admission when a shutdown WINDOW has ended (the flush finished
    /// or failed) and the repository stays in use: embedders and tests flush
    /// mid-life and keep opening tables afterwards. A shutdown future dropped
    /// mid-window never reaches this call, so an abandoned shutdown stays
    /// closed.
    pub fn reopen(&self) {
        self.inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .closed = false;
    }

    /// Wait until no registered cold open is live, or `false` at `deadline`.
    pub async fn drain(&self, deadline: tokio::time::Instant) -> bool {
        loop {
            let notified = self.inner.idle.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self
                .inner
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .live
                == 0
            {
                return true;
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return false;
            }
        }
    }
}

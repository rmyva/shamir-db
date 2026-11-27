//! `DrainPauseState` — the owned RAII pause guard over the drainer's
//! `replaying` lock (design `online-backup.md` §2 (iii), R2a step 1d).
//!
//! While the pause is held, `drain_step`, `drain_all`, the background loop
//! and the truncation wait (all of which acquire `replaying` inside
//! `drain_step` / `settle_and_truncate`) are blocked on the same mutex.
//! Acquisition waits for any in-flight drain pass to finish and is
//! cancel-safe: nothing is mutated before the guard exists, and a cancelled
//! acquisition releases the busy flag it tentatively set (`PauseAttempt`'s
//! drop).
//!
//! Exclusivity beyond the mutex: the busy flag is checked-and-set BEFORE
//! the lock await, so a second acquisition while a seal hold owns the
//! pause fails immediately with the typed [`DrainPauseBusy`] error instead
//! of blocking unboundedly (the seal hold may live for minutes).
//!
//! Documented lock order (never reverse): commit-admission gate CLOSE
//! first, THEN the pause. Capture/seal always follow it; the background
//! drain takes no gate right at all.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use tokio::sync::{Mutex, OwnedMutexGuard};

/// Typed refusal when the pause is already owned (a live seal hold or a
/// concurrent barrier). Carries no side effect.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("the drain pause is already held (a seal hold or concurrent barrier owns it)")]
pub struct DrainPauseBusy;

/// One per drainer: the SAME `Arc<Mutex<()>>` the drain paths lock, plus
/// the long-holder busy flag. `Drainer` owns exactly one; see
/// [`crate::drainer::Drainer::pause`].
pub struct DrainPauseState {
    lock: Arc<Mutex<()>>,
    held: Arc<AtomicBool>,
}

impl DrainPauseState {
    pub(crate) fn new(lock: Arc<Mutex<()>>) -> Self {
        Self {
            lock,
            held: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Probe for the barrier entry busy check (a hold or transient pause
    /// exists). Advisory; the authoritative refusal is in
    /// [`acquire`](Self::acquire).
    pub(crate) fn is_held(&self) -> bool {
        self.held.load(Ordering::Acquire)
    }

    /// Acquire the pause: waits for an in-flight drain pass (the `replaying`
    /// mutex), then holds the lock until the guard drops. Refuses with
    /// [`DrainPauseBusy`] when the flag is already set.
    ///
    /// cancel-safe: yes — nothing is mutated before the guard exists; a
    /// cancelled wait clears the tentative busy flag (`PauseAttempt`).
    pub async fn acquire(&self) -> Result<DrainPauseGuard, DrainPauseBusy> {
        if self.held.swap(true, Ordering::AcqRel) {
            return Err(DrainPauseBusy);
        }
        let attempt = PauseAttempt {
            flag: Arc::clone(&self.held),
            lock: Box::pin(Arc::clone(&self.lock).lock_owned()),
            acquired: false,
        };
        let lock = attempt.await;
        Ok(DrainPauseGuard {
            flag: Arc::clone(&self.held),
            _lock: lock,
        })
    }
}

/// The in-flight lock wait with cleanup-on-cancel: if the future is dropped
/// before the lock guard exists, the tentative busy flag is cleared.
/// Every field is `Unpin`, so polling through `Pin<&mut Self>` is plain
/// `get_mut`.
struct PauseAttempt {
    flag: Arc<AtomicBool>,
    lock: Pin<Box<dyn Future<Output = OwnedMutexGuard<()>> + Send>>,
    acquired: bool,
}

impl Future for PauseAttempt {
    type Output = OwnedMutexGuard<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match this.lock.as_mut().poll(cx) {
            Poll::Ready(guard) => {
                this.acquired = true;
                Poll::Ready(guard)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for PauseAttempt {
    fn drop(&mut self) {
        if !self.acquired {
            self.flag.store(false, Ordering::Release);
        }
    }
}

/// Owned drain pause over the drainer's `replaying` lock. `Send + 'static`
/// so a seal hold can own it far beyond the call that created it. Drops the
/// pause exactly once: the lock guard is released and the busy flag is
/// cleared after it.
#[must_use = "dropping the guard releases the drain pause"]
pub struct DrainPauseGuard {
    flag: Arc<AtomicBool>,
    /// Never read — held for its Drop (the mutex release), which runs after
    /// the guard's own Drop body clears the busy flag (field order).
    _lock: OwnedMutexGuard<()>,
}

impl Drop for DrainPauseGuard {
    fn drop(&mut self) {
        // Clear the busy flag first so a waiter that has not yet reached the
        // lock can admit itself; the lock guard (dropped after this body,
        // fields in declaration order) releases the mutex.
        self.flag.store(false, Ordering::Release);
    }
}

//! `CommitAdmissionGate` — the per-repo commit admission primitive (design
//! `online-backup.md` section 2, settled). Lock-free: ONE `AtomicUsize`
//! state word + an armed-`Notify` wait loop (the
//! `CommitTailBarrier::wait_zero` pattern); no `Mutex`/`RwLock` anywhere.
//!
//! State word layout (usize, 64-bit targets):
//!
//! ```text
//! bit 63      CLOSE_TAKEN — exactly one RAII close owner holds the right
//!             to reopen; set before CLOSED, cleared after CLOSED
//! bit 62      CLOSED      — admission refused
//! bits 0..62  ACTIVE      — bounded active lease count (ACTIVE_MAX caps it
//!             well below the 62-bit ceiling)
//! ```
//!
//! Memory orderings:
//! * reads of the state word: `Acquire` (pairs with lease-release
//!   `fetch_sub` and with close/reopen `Release`/`AcqRel` RMWs);
//! * admission CAS: `AcqRel` success / `Acquire` failure — the lease's
//!   later `fetch_sub(1, Release)` publishes everything the leader did
//!   under the lease, and the zero-observer's `Acquire` load sees it;
//! * lease release: `fetch_sub(1, Release)` (design-mandated) — release
//!   semantics publish the leader's mutations to whoever next Acquires
//!   the word (the close owner's zero check, the next admission CAS);
//! * close: `fetch_or` RMWs with `AcqRel` (the CLOSED bit must not be
//!   reordered before CLOSE_TAKEN becomes visible);
//! * reopen: CAS clearing CLOSED with `AcqRel`, then clearing CLOSE_TAKEN
//!   with `AcqRel`, then `notify_waiters`.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Notify;

use super::close_owner::CommitAdmissionCloseOwner;
use super::error::CommitAdmissionError;
use super::lease::CommitAdmissionLease;

/// Bounded reopen wait for a leader that finds the gate CLOSED (design
/// "waiting policy"): past this cap the leader is refused with
/// [`CommitAdmissionError::Closed`] and NO side effect. Documented constant
/// (matches `COMMIT_QUIESCE_TIMEOUT` in shamir-db's db_management.rs in
/// spirit); lifted to `shamir-tunables` when a slot fits.
pub(crate) const ADMISSION_REOPEN_WAIT_CAP: Duration = Duration::from_secs(15);

/// The cap the commit entry passes: the documented constant in production.
#[cfg(not(test))]
pub(crate) const LEADER_REOPEN_WAIT_CAP: Duration = ADMISSION_REOPEN_WAIT_CAP;

/// Under `cfg(test)` the entry's cap is short so a refusal test does not wait
/// out the production value; it still leaves seconds of margin for
/// deterministic park-then-reopen handshakes on a loaded runner.
#[cfg(test)]
pub(crate) const LEADER_REOPEN_WAIT_CAP: Duration = Duration::from_secs(2);

/// CLOSED: gate refuses admission (bit 62).
const CLOSED: usize = 1 << 62;
/// CLOSE_TAKEN: an exclusive RAII close owner exists (bit 63).
/// Ungated since R2a step 1d: capture/seal consume the close API.
const CLOSE_TAKEN: usize = 1 << 63;
/// ACTIVE mask: bits 0..62.
const ACTIVE_MASK: usize = (1usize << 62) - 1;
/// Bounded active count, rejected well below the 62-bit ceiling.
const ACTIVE_MAX: usize = i32::MAX as usize;

/// One gate per repository, `Arc`-shared; owned next to the commit tail
/// barrier (`TxHorizon::commit_admission`).
#[derive(Default)]
pub struct CommitAdmissionGate {
    /// Single state word: CLOSED | CLOSE_TAKEN | ACTIVE count. All
    /// transitions are atomic RMWs on this word — the close-vs-acquire
    /// boundary is decided by the word itself (a lease is either counted
    /// before CLOSED lands or refused after; never uncounted).
    state: AtomicUsize,
    /// Waiters (leaders parked on CLOSED, the close owner parked on zero)
    /// re-arm `Notified` BEFORE rechecking the word, so a racing final
    /// decrement or reopen is never missed.
    notify: Notify,
}

impl CommitAdmissionGate {
    /// Follower entry (design Q13): refuse IMMEDIATELY when closed — no
    /// wait, no side effect. Cancel-safe: nothing is mutated before the
    /// lease exists; a cancelled call leaves the word untouched.
    pub(crate) fn try_acquire(
        self: &Arc<Self>,
    ) -> Result<CommitAdmissionLease, CommitAdmissionError> {
        let mut state = self.state.load(Ordering::Acquire);
        loop {
            if state & CLOSED != 0 {
                return Err(CommitAdmissionError::Closed);
            }
            if state & ACTIVE_MASK >= ACTIVE_MAX {
                return Err(CommitAdmissionError::Overflow);
            }
            match self.state.compare_exchange_weak(
                state,
                state + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(CommitAdmissionLease::new(Arc::clone(self))),
                Err(updated) => state = updated,
            }
        }
    }

    /// Leader entry: if CLOSED, WAIT for reopen on the gate's `Notify`
    /// (armed BEFORE the atomic recheck), bounded by `reopen_wait_cap`;
    /// past the cap, typed retryable [`CommitAdmissionError::Closed`] with
    /// no side effect. The bounded wait is cancel-safe: nothing is mutated
    /// before the lease is synchronously constructed from the successful
    /// CAS. Open fast path: one `load` + one CAS, no allocation.
    pub(crate) async fn acquire_leader(
        self: &Arc<Self>,
        reopen_wait_cap: Duration,
    ) -> Result<CommitAdmissionLease, CommitAdmissionError> {
        let deadline = tokio::time::Instant::now() + reopen_wait_cap;
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            // Arm before checking: a racing reopen/final-decrement is never
            // missed (CommitTailBarrier::wait_zero pattern).
            notified.as_mut().enable();
            let state = self.state.load(Ordering::Acquire);
            if state & CLOSED == 0 {
                return self.try_acquire();
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return Err(CommitAdmissionError::Closed);
            }
        }
    }

    /// Release one lease (called from the lease's `Drop`): `fetch_sub(1,
    /// Release)`. The wake keys on the ACTIVE count reaching zero, NOT on the
    /// whole word equalling 1: under a closed gate the CLOSED and CLOSE_TAKEN
    /// bits are set, and the close owner parked on zero must still be woken.
    pub(super) fn release(&self) {
        if self.state.fetch_sub(1, Ordering::Release) & ACTIVE_MASK == 1 {
            self.notify.notify_waiters();
        }
    }

    /// CAS-in CLOSE_TAKEN (single owner; a competing closer gets
    /// [`CommitAdmissionError::Busy`]), then set CLOSED, then return the
    /// RAII owner SYNCHRONOUSLY — no suspension point before the owner
    /// exists, so cancellation cannot leak a closed gate (fail-closed
    /// recovery: the owner reopens on drop, error or unwind). Zero-lease
    /// waiting is [`CommitAdmissionCloseOwner::wait_zero`] with the
    /// caller's cap; the old combined close-and-wait shape lives on as the
    /// cfg(test) helper in this module's tests.
    pub(crate) fn begin_close(
        self: &Arc<Self>,
    ) -> Result<CommitAdmissionCloseOwner, CommitAdmissionError> {
        let previous = self.state.fetch_or(CLOSE_TAKEN, Ordering::AcqRel);
        if previous & CLOSE_TAKEN != 0 {
            return Err(CommitAdmissionError::Busy);
        }
        self.state.fetch_or(CLOSED, Ordering::AcqRel);
        // Wake armed closed-waiters so they recheck (and keep waiting until
        // reopen, or hit their cap).
        self.notify.notify_waiters();
        Ok(CommitAdmissionCloseOwner::new(Arc::clone(self)))
    }

    /// Await zero ACTIVE leases through the armed-notify recheck
    /// (`CommitTailBarrier::wait_zero` pattern). Cancel-safe: nothing is
    /// mutated; the close right lives in the caller's owner. Past `cap`
    /// (`Duration::MAX` = unbounded) the typed
    /// [`CommitAdmissionError::CloseTimeout`] is returned with no side
    /// effect — the caller's owner drop reopens.
    pub(crate) async fn wait_zero(&self, cap: Duration) -> Result<(), CommitAdmissionError> {
        let deadline = if cap == Duration::MAX {
            None
        } else {
            Some(tokio::time::Instant::now() + cap)
        };
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.state.load(Ordering::Acquire) & ACTIVE_MASK == 0 {
                return Ok(());
            }
            match deadline {
                Some(deadline) => {
                    if tokio::time::timeout_at(deadline, notified).await.is_err() {
                        return Err(CommitAdmissionError::CloseTimeout);
                    }
                }
                None => notified.await,
            }
        }
    }

    /// Reopen — ONLY reachable by the close owner (CLOSE_TAKEN held):
    /// clear CLOSED (preserving ACTIVE), clear CLOSE_TAKEN, wake waiters.
    pub(crate) fn reopen(&self) {
        loop {
            let state = self.state.load(Ordering::Acquire);
            debug_assert!(state & CLOSED != 0, "reopen without a closed gate");
            match self.state.compare_exchange(
                state,
                state & !CLOSED,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(_) => continue, // ACTIVE raced; retry with fresh word
            }
        }
        self.state.fetch_and(!CLOSE_TAKEN, Ordering::AcqRel);
        self.notify.notify_waiters();
    }

    /// Diagnostic / test observation: current ACTIVE lease count.
    #[cfg(test)]
    pub(crate) fn active_count(&self) -> usize {
        self.state.load(Ordering::Acquire) & ACTIVE_MASK
    }

    /// Diagnostic / test observation: is CLOSED set?
    #[cfg(test)]
    pub(crate) fn is_closed(&self) -> bool {
        self.state.load(Ordering::Acquire) & CLOSED != 0
    }
}

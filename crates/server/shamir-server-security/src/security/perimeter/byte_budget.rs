//! Global byte-budget primitive — RI-15.
//!
//! `tokio::sync::Semaphore` counts *permits*, not bytes, and its permit
//! count is a `u32` — capping any byte-granular reuse of it at `u32::MAX`
//! and coupling byte accounting to permit accounting. [`ByteBudget`] is a
//! purpose-built async byte accountant: an `AtomicUsize` CAS-loop fast path
//! for acquire/release plus a [`tokio::sync::Notify`] to wake waiters when
//! bytes free up.
//!
//! # Why this exists
//!
//! The server already clamps the size of any ONE batch response
//! (`security.query_limits.max_result_size_bytes`,
//! `crates/server/shamir-server/src/db_handler/handler.rs`), but nothing bounded
//! the SUM of in-flight response bytes across every concurrently-executing
//! batch/connection. At `max_active_connections = 1000` × a 64 MiB
//! per-batch cap, worst case is ~64 GiB of simultaneously buffered response
//! memory — unbounded relative to a typical 4–8 GiB container. `ByteBudget`
//! closes that gap: a single server-wide cap on in-flight response bytes.
//!
//! # Usage
//!
//! CR-B2: `ShamirDbHandler::execute` acquires a [`ByteBudgetGuard`] UPFRONT,
//! before the batch executes, using a pessimistic estimate — the batch's
//! (server-clamped) `max_result_size` — so the budget actually gates
//! EXECUTION-time memory, not just how long a serialized response sits on
//! the write path. Once the final `DbResponse` is known, the reservation is
//! narrowed down to the real serialized size via [`ByteBudgetGuard::shrink_to`]
//! (a bounded few-byte overshoot past the estimate is absorbed via
//! [`ByteBudgetGuard::grow_unchecked`] rather than a second blocking
//! acquire). The guard is threaded through
//! `connection::request_loop::WriterMsg::{Reply,ReplyAndClose}` and dropped
//! only after the writer task finishes the socket write (success or
//! error) — so the accounted (now-shrunk) bytes stay "reserved" for exactly
//! as long as they occupy memory on the write path, and the release always
//! happens on the writer task, never the dispatch task. The cursor path
//! (`db_handler::cursor_handlers::enforce_page_budget`) mirrors this same
//! upfront-reserve-then-shrink shape whenever a per-page size cap is
//! actively configured.
//!
//! # Fairness
//!
//! Waiters are NOT served strict FIFO. Every waiter wakes on every
//! `notify_waiters()` call (broadcast) and independently retries the CAS;
//! whichever wins the race proceeds, the rest re-park. This gives
//! **at-least-one-progress** per release (a release that frees enough
//! bytes for the head-of-line waiter is guaranteed to unblock SOMEONE, but
//! not necessarily in arrival order). Acceptable here: the budget is a
//! coarse OOM shield, not a scheduler — starvation would require a
//! continuous stream of large releases each claimed by a newer waiter,
//! which does not happen under the realistic workload (a fixed pool of
//! `max_active_connections` each issuing one batch at a time).

use std::cell::RefCell;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use shamir_tx::{CancelHandle, WaitState};
use tokio::sync::Notify;

#[cfg(test)]
#[path = "tests/byte_budget_tests.rs"]
mod tests;

/// Shared state behind [`ByteBudget`]'s `Arc`. Split out so [`ByteBudget`]
/// itself can be a cheap `Clone` (just bumps the `Arc` refcount).
struct Inner {
    /// Bytes currently reserved by outstanding [`ByteBudgetGuard`]s.
    used: AtomicUsize,
    waiters: AtomicUsize,
    /// Hard cap in bytes. `None` = unbounded (preserves pre-RI-15
    /// behavior: `acquire` always succeeds immediately).
    cap: Option<usize>,
    /// Wakes every waiter parked in `acquire` when bytes are released.
    notify: Notify,
}

struct WaiterGuard<'a>(&'a AtomicUsize);

impl<'a> WaiterGuard<'a> {
    fn new(waiters: &'a AtomicUsize) -> Self {
        waiters.fetch_add(1, Ordering::Relaxed);
        Self(waiters)
    }
}

impl Drop for WaiterGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Server-wide async byte accountant. Cheaply `Clone` (wraps an `Arc`).
///
/// Construct once at boot (`server_launcher/launch.rs`, next to
/// `QueryLimitsCap`) from `security.query_limits.max_inflight_response_bytes`
/// and share the single instance across every connection/handler clone.
#[derive(Clone)]
pub struct ByteBudget {
    inner: Arc<Inner>,
}

impl ByteBudget {
    /// New budget with the given cap. `None` means unbounded: every
    /// `acquire` returns immediately regardless of `bytes`, matching the
    /// pre-RI-15 behavior (no global gate).
    pub fn new(cap: Option<usize>) -> Self {
        Self {
            inner: Arc::new(Inner {
                used: AtomicUsize::new(0),
                waiters: AtomicUsize::new(0),
                cap,
                notify: Notify::new(),
            }),
        }
    }

    /// Unbounded budget — every `acquire` succeeds immediately. Used as the
    /// default for handlers/tests that don't care about the global cap.
    pub fn unbounded() -> Self {
        Self::new(None)
    }

    /// Reserve `bytes` from the budget, waiting if the cap is currently
    /// exhausted. Returns a [`ByteBudgetGuard`] that releases the
    /// reservation on `Drop` (covers every early-return / error / panic
    /// unwind path on the caller's side).
    ///
    /// `bytes` larger than the configured cap is still admitted (once the
    /// budget is fully drained) rather than deadlocking forever — a single
    /// oversized-but-otherwise-valid response should not be able to wedge
    /// the whole gate. Validation at config-load time
    /// (`max_inflight_response_bytes >= max_result_size_bytes`) is what
    /// actually keeps this case from happening in practice.
    pub async fn acquire(&self, bytes: usize) -> ByteBudgetGuard {
        let Some(cap) = self.inner.cap else {
            // Unbounded — no accounting needed, no waiting.
            return ByteBudgetGuard {
                inner: None,
                bytes: 0,
            };
        };

        // Fast path FIRST: try the CAS loop before touching `Notify` at all.
        // CR-C1 (P-3): the `Notified` future used to be created AND
        // `enable()`d on every single call, including the common uncontended
        // case where this very first CAS succeeds and the future is
        // immediately dropped unused — two `Notify`-related ops on every
        // acquire, contended or not, which didn't match this module's own
        // "lock-free CAS-loop fast path" description. Restructured so the
        // `Notified` future is only constructed on the FIRST CAS failure,
        // right before actually needing to park.
        if let Some(guard) = self.try_cas(bytes, cap) {
            return guard;
        }

        // Count each contended acquire once, across retries and until return or drop.
        let _waiter = WaiterGuard::new(&self.inner.waiters);
        loop {
            // Not enough room right now — register interest in the next
            // notification BEFORE re-checking the CAS one more time. This is
            // `tokio::sync::Notify`'s documented race-free pattern:
            // `notified()` returns a future that, once polled, stores itself
            // as a waiter — so a `notify_waiters()` call that happens after
            // this line (even before we `.await` it below) is not lost. This
            // reasoning is unchanged from before the restructure — only WHEN
            // the `Notified` future is built moved (from "every call,
            // upfront" to "only once the fast path has already failed"), not
            // the invariant itself: without this ordering there would still
            // be a lost-wakeup window between "CAS failed" and "start
            // waiting" where a concurrent release could notify into the
            // void.
            let notified = self.inner.notify.notified();
            tokio::pin!(notified);
            // Polling once here (via `enable()`) commits this waiter into
            // Notify's internal list even before the first `.await` below —
            // required so a release racing right after this line still
            // wakes us instead of being missed.
            notified.as_mut().enable();

            // Re-check the CAS once more now that we're registered as a
            // waiter — state may have changed between the previous attempt
            // and this registration.
            if let Some(guard) = self.try_cas(bytes, cap) {
                return guard;
            }

            // Still no room — park until a release notifies us (the
            // `enable()` above guarantees we don't miss a release that
            // happened between the CAS failure and this await), then retry
            // the whole loop: state may have changed again by the time we
            // wake, so we re-check rather than assume we now have room.
            notified.await;
        }
    }

    /// Reserve bytes as in [`Self::acquire`], or return `None` if cancelled.
    /// Cancellation observed before returning a reservation takes precedence:
    /// any guard obtained in the race is dropped and its bytes released.
    ///
    /// cancel-safe: yes — the only suspension holds a waiter and a park
    /// marker, but no reservation; dropping the future clears both.
    pub async fn acquire_cancellable(
        &self,
        bytes: usize,
        handle: &CancelHandle,
    ) -> Option<ByteBudgetGuard> {
        if handle.is_cancelled() {
            return None;
        }
        let Some(cap) = self.inner.cap else {
            return Some(ByteBudgetGuard {
                inner: None,
                bytes: 0,
            });
        };
        if let Some(guard) = self.try_cas(bytes, cap) {
            return (!handle.is_cancelled()).then_some(guard);
        }

        let _waiter = WaiterGuard::new(&self.inner.waiters);
        loop {
            if handle.is_cancelled() {
                return None;
            }
            // Register before retrying so a racing release cannot be lost.
            let notified = self.inner.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(guard) = self.try_cas(bytes, cap) {
                return (!handle.is_cancelled()).then_some(guard);
            }

            let _park = handle.park(WaitState::Budget);
            tokio::select! {
                biased;
                _ = handle.wait_cancelled() => return None,
                _ = notified => {}
            }
        }
    }

    /// One pass of the lock-free CAS loop: `Some(guard)` if `bytes` was
    /// admitted (either there was room, or the budget was empty — see
    /// `acquire`'s doc comment on the oversized-request escape hatch),
    /// `None` if the budget is currently too full and the caller must park.
    /// Shared by both the pre-`Notify` fast-path attempt and the
    /// post-registration re-check in `acquire` so the CAS logic itself is
    /// not duplicated.
    fn try_cas(&self, bytes: usize, cap: usize) -> Option<ByteBudgetGuard> {
        let mut current = self.inner.used.load(Ordering::Acquire);
        loop {
            let after = current.saturating_add(bytes);
            // Admit if there's room, OR if the budget is currently empty
            // (current == 0) — guarantees an oversized request eventually
            // gets a turn instead of deadlocking forever when `bytes > cap`.
            if after <= cap || current == 0 {
                match self.inner.used.compare_exchange_weak(
                    current,
                    after,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => {
                        return Some(ByteBudgetGuard {
                            inner: Some(self.inner.clone()),
                            bytes,
                        });
                    }
                    Err(actual) => {
                        current = actual;
                        continue;
                    }
                }
            }
            return None;
        }
    }

    /// Current reserved-byte count (0 if unbounded/no outstanding guards).
    pub fn used(&self) -> usize {
        self.inner.used.load(Ordering::Acquire)
    }

    /// Number of acquires on the contended path, for observability only.
    pub(crate) fn waiters(&self) -> usize {
        self.inner.waiters.load(Ordering::Relaxed)
    }

    /// Configured cap, or `None` if unbounded.
    pub fn cap(&self) -> Option<usize> {
        self.inner.cap
    }
}

/// RAII reservation. Releases its `bytes` back to the [`ByteBudget`] on
/// `Drop` and wakes any parked waiters — fires on every path (normal
/// completion, write error, task abort/panic unwind).
///
/// Deliberately holds `Arc<Inner>` directly (not `ByteBudget`) so it has no
/// dependency on the public wrapper type — keeps the guard `Send + 'static`
/// with the smallest possible footprint, since it rides inside
/// `connection::request_loop::WriterMsg` across an `mpsc` channel.
pub struct ByteBudgetGuard {
    inner: Option<Arc<Inner>>,
    bytes: usize,
}

impl Drop for ByteBudgetGuard {
    fn drop(&mut self) {
        if let Some(inner) = &self.inner {
            if self.bytes > 0 {
                inner.used.fetch_sub(self.bytes, Ordering::AcqRel);
            }
            // Broadcast: every currently-parked `acquire` re-checks the CAS.
            // Cheap even with zero waiters (Notify short-circuits).
            inner.notify.notify_waiters();
        }
    }
}

impl ByteBudgetGuard {
    /// Bytes currently reserved by this guard (what `Drop`/`shrink_to`'s
    /// delta math is relative to). Used by callers that need to compare
    /// the reservation against an actual size before deciding whether to
    /// [`Self::shrink_to`] or [`Self::grow_unchecked`] (CR-B2's overshoot
    /// case).
    pub fn bytes_reserved(&self) -> usize {
        self.bytes
    }

    /// Narrow this reservation down from a pessimistic upfront estimate to
    /// the actual size, once known (CR-B2 — upfront-reserve-then-shrink).
    ///
    /// Shrink-only: a no-op when `self.inner` is `None` (unbounded budget)
    /// or when `new_bytes >= self.bytes` — this reservation scheme only
    /// ever estimates HIGH then narrows DOWN, so `new_bytes` should always
    /// be `<= self.bytes` at every intended call site, but treating the
    /// edge case as a no-op (rather than panicking or silently growing) is
    /// the defensive choice. Use [`Self::grow_unchecked`] for the rare
    /// legitimate overshoot case instead.
    ///
    /// Releases `delta = self.bytes - new_bytes` back to the budget and
    /// wakes any parked waiters (same release pattern `Drop` uses), then
    /// updates `self.bytes` so a later `Drop` releases only the remaining
    /// (already-shrunk) amount — never double-releasing the shrunk delta.
    pub fn shrink_to(&mut self, new_bytes: usize) {
        let Some(inner) = &self.inner else {
            return;
        };
        if new_bytes >= self.bytes {
            return;
        }
        let delta = self.bytes - new_bytes;
        inner.used.fetch_sub(delta, Ordering::AcqRel);
        inner.notify.notify_waiters();
        self.bytes = new_bytes;
    }

    /// Add `extra_bytes` to this reservation UNCONDITIONALLY — no waiting,
    /// no cap check (CR-B2 — the bounded, few-bytes-of-envelope-framing
    /// overshoot case: the final serialized `DbResponse` can be a handful
    /// of bytes larger than the raw pessimistic estimate it was reserved
    /// against, e.g. enum discriminator framing on top of the inner
    /// payload).
    ///
    /// Deliberately does NOT re-acquire via a blocking `acquire().await` —
    /// the response is already computed at this point, and blocking here
    /// risks deadlocking against the very budget this guard already holds
    /// a slice of. This is a documented, bounded overshoot (a few bytes of
    /// framing, never unbounded), not a general-purpose growth mechanism.
    ///
    /// A no-op when `self.inner` is `None` (unbounded budget).
    pub fn grow_unchecked(&mut self, extra_bytes: usize) {
        let Some(inner) = &self.inner else {
            return;
        };
        if extra_bytes == 0 {
            return;
        }
        inner.used.fetch_add(extra_bytes, Ordering::AcqRel);
        self.bytes += extra_bytes;
    }
}

tokio::task_local! {
    /// Side-channel that carries a just-acquired [`ByteBudgetGuard`] out of
    /// `db_handler::handler::ShamirDbHandler::execute` to the dispatch task
    /// in `connection::request_loop`, without changing
    /// `shamir_connect::server::dispatch::RequestHandler::handle`'s
    /// `Result<Vec<u8>, String>` signature (that trait lives in a different
    /// crate and carries no per-request resource context — see the RI-15
    /// brief).
    ///
    /// Both the writer (`execute`, via [`stash_guard`]) and the reader
    /// (`connection::request_loop`, via [`take_stashed_guard`]) run inside
    /// the SAME spawned per-request dispatch task
    /// (`request_loop.rs::join_set.spawn(async move { ... })`), so this
    /// task-local is never shared across concurrent requests — each
    /// request's task gets its own independent storage slot the moment it
    /// is `.scope()`d in `run_with_guard_slot`.
    pub(crate) static PENDING_RESPONSE_BUDGET_GUARD: RefCell<Option<ByteBudgetGuard>>;
}

/// Run `fut` with fresh, empty [`PENDING_RESPONSE_BUDGET_GUARD`] AND
/// [`PENDING_SERIALIZED_RESPONSE`] slots scoped to it. Call this once per
/// dispatched request, wrapping the same future that (transitively) calls
/// `ShamirDbHandler::execute`.
///
/// Both task-locals are scoped together here (nested `.scope()` calls) so
/// callers only need to remember one entry point — mirrors the existing
/// single-guard-slot pattern rather than introducing a second, divergent
/// "remember to scope this too" call site.
pub async fn run_with_guard_slot<F: std::future::Future>(fut: F) -> F::Output {
    PENDING_RESPONSE_BUDGET_GUARD
        .scope(
            RefCell::new(None),
            PENDING_SERIALIZED_RESPONSE.scope(RefCell::new(None), fut),
        )
        .await
}

/// FB3EC046 — guard-handoff seam. Runs `fut` inside a fresh
/// [`PENDING_RESPONSE_BUDGET_GUARD`] (and serialized-response) scope and, in
/// that SAME scoped task, takes ownership of any guard the future stashed
/// (mirroring `ShamirDbHandler::execute` via [`stash_guard`]) as soon as the
/// future resolves. Returns `(output, guard)` so the caller — in production,
/// the per-request dispatch task in `sync/connection/request_loop` — can
/// attach the guard to the `WriterMsg` and keep the reservation charged
/// until the socket write completes.
///
/// Production AND the regression test (`response_budget_scope_tests.rs`)
/// both go through this helper, so the handoff shape (take-inside-scope,
/// guard outlives the scope via the returned tuple) is encoded in exactly
/// one place: reintroducing an out-of-scope `take_stashed_guard()` in this
/// helper makes the test fail. HONEST LIMIT: a direct revert of
/// `request_loop.rs` back to calling `run_with_guard_slot` +
/// `take_stashed_guard` by hand would NOT be discriminated by the test —
/// the test pins the shared helper, not every call site.
pub async fn run_with_handoff<F: std::future::Future>(
    fut: F,
) -> (F::Output, Option<ByteBudgetGuard>) {
    run_with_guard_slot(async {
        let output = fut.await;
        (output, take_stashed_guard())
    })
    .await
}

/// Called from inside `ShamirDbHandler::execute` right after acquiring the
/// budget for the response about to be returned. Overwrites any previously
/// stashed guard for this request (there is at most one response per
/// dispatched request, so this is always a fresh slot).
///
/// Outside a [`run_with_guard_slot`] scope (e.g. a unit test that calls
/// `ShamirDbHandler::execute` directly without going through
/// `request_loop`) this is a silent no-op: the guard is simply dropped
/// immediately, releasing the budget right away instead of holding it
/// through a write that will never happen. That is a safe fallback — it
/// only under-holds the budget in a context that was never going to route
/// the response through the real writer anyway.
pub fn stash_guard(guard: ByteBudgetGuard) {
    let _ = PENDING_RESPONSE_BUDGET_GUARD.try_with(|cell| {
        *cell.borrow_mut() = Some(guard);
    });
}

/// Called from `connection::request_loop` immediately after the dispatch
/// future resolves, to retrieve (and take ownership of) the guard
/// `execute` stashed for this response — or `None` if the budget is
/// unbounded, the request never reached `execute` (e.g. `Ping`,
/// `CreateScramUser`), or `execute` short-circuited before running the
/// batch (version/permission/read-only gates).
pub fn take_stashed_guard() -> Option<ByteBudgetGuard> {
    PENDING_RESPONSE_BUDGET_GUARD
        .try_with(|cell| cell.borrow_mut().take())
        .ok()
        .flatten()
}

tokio::task_local! {
    /// CR-B2 — side-channel that carries the msgpack bytes
    /// `ShamirDbHandler::execute` already serialized (to shrink the RI-15
    /// reservation to the actual size) out to `RequestHandler::handle`, so
    /// the SAME response is never serialized a second time for the wire.
    ///
    /// Mirrors [`PENDING_RESPONSE_BUDGET_GUARD`]'s exact scoping: both live
    /// in the same per-request dispatch task, scoped together by
    /// [`run_with_guard_slot`], so this is never shared across concurrent
    /// requests.
    pub(crate) static PENDING_SERIALIZED_RESPONSE: RefCell<Option<Vec<u8>>>;
}

/// Called from inside `ShamirDbHandler::execute` right after serializing the
/// final `DbResponse` (whether success or error) to measure its actual size
/// for the RI-15 shrink step. Overwrites any previously stashed bytes for
/// this request (there is at most one response per dispatched request).
///
/// Outside a [`run_with_guard_slot`] scope this is a silent no-op — the
/// bytes are simply dropped, and the caller falls back to serializing fresh
/// (see [`take_stashed_serialized_response`]).
pub fn stash_serialized_response(bytes: Vec<u8>) {
    let _ = PENDING_SERIALIZED_RESPONSE.try_with(|cell| {
        *cell.borrow_mut() = Some(bytes);
    });
}

/// Called from `RequestHandler::handle` right before it would otherwise
/// serialize `response` fresh for the wire. Returns the bytes
/// `ShamirDbHandler::execute` already produced for this SAME response value
/// — or `None` when the request never went through `execute`'s stash point
/// (`Ping`, `CreateScramUser`, cursor ops, etc.), in which case the caller
/// falls through to its existing fresh-serialize path.
pub fn take_stashed_serialized_response() -> Option<Vec<u8>> {
    PENDING_SERIALIZED_RESPONSE
        .try_with(|cell| cell.borrow_mut().take())
        .ok()
        .flatten()
}

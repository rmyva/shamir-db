//! Global cap on simultaneously-active connections.
//!
//! A simple atomic counter incremented at TCP-accept and decremented when
//! the per-connection task ends. RAII guard means we can't forget to
//! decrement on early exits (TLS handshake failure, slow-loris timeout,
//! handler panic — all clean up via Drop).
//!
//! Why **before** the TLS handshake: TLS itself costs ~5–10 ms of CPU per
//! connection. If the server is already at the cap, accepting a TCP
//! connection just to throw it away after the TLS work is wasteful and
//! plays into a DoS attacker's hand. We close the socket immediately
//! (TCP RST) and burn a single accept syscall instead.
//!
//! `try_acquire()` is non-blocking — it returns `None` rather than
//! waiting. The accept loop logs and drops the socket; the attacker's
//! retries see the same RST until the cap clears.

use std::net::IpAddr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use dashmap::DashMap;
use shamir_collections::THasher;
use tokio::sync::Notify;

/// Cap-tracker. Cheaply cloneable — wraps an `Arc<AtomicUsize>` and a
/// fixed maximum.
#[derive(Debug, Clone)]
pub struct ConnLimiter {
    active: Arc<AtomicUsize>,
    cap: usize,
    /// Every connection that holds a guard, counted even when `cap == 0`
    /// (unlimited), so shutdown can wait for the last connection to end.
    live: Arc<LiveConnections>,
}

/// Always-on count of live connections plus the wake-up for
/// [`ConnLimiter::wait_idle`].
#[derive(Debug, Default)]
struct LiveConnections {
    count: AtomicUsize,
    rejected: AtomicU64,
    idle: Notify,
}

impl ConnLimiter {
    /// New limiter with the given hard cap. `cap == 0` is treated as
    /// "no limit" (every `try_acquire` returns `Some`).
    pub fn new(cap: usize) -> Self {
        Self {
            active: Arc::new(AtomicUsize::new(0)),
            cap,
            live: Arc::new(LiveConnections::default()),
        }
    }

    /// Try to reserve a slot for one new connection. Returns `Some(guard)`
    /// on success — drop the guard to release the slot. Returns `None`
    /// when the cap is reached.
    ///
    /// Implementation note: we use a CAS-free `fetch_add` + rollback
    /// pattern. Two callers racing into the cap-1 slot can both increment
    /// (transiently exceed by 1), then one rolls back. Worst case: the
    /// counter briefly shows `cap + N` for a few μs. Acceptable: the cap
    /// is a coarse DoS shield, not a precise resource accountant.
    pub fn try_acquire(&self) -> Option<ConnGuard> {
        if self.cap == 0 {
            // No cap configured — return a guard that only tracks liveness
            // so callers can pretend the limiter is always "on".
            self.live.count.fetch_add(1, Ordering::AcqRel);
            return Some(ConnGuard {
                limiter: self.clone(),
                active: false,
            });
        }
        let prev = self.active.fetch_add(1, Ordering::Relaxed);
        if prev >= self.cap {
            self.active.fetch_sub(1, Ordering::Relaxed);
            self.live.rejected.fetch_add(1, Ordering::Relaxed);
            None
        } else {
            self.live.count.fetch_add(1, Ordering::AcqRel);
            Some(ConnGuard {
                limiter: self.clone(),
                active: true,
            })
        }
    }

    /// Current count of active connections. Cheap atomic load.
    pub fn active(&self) -> usize {
        self.active.load(Ordering::Relaxed)
    }

    /// Configured hard cap (or 0 if unlimited).
    pub fn cap(&self) -> usize {
        self.cap
    }

    /// Connections currently holding a guard, regardless of the cap.
    pub fn live(&self) -> usize {
        self.live.count.load(Ordering::Acquire)
    }

    /// Total acquisitions refused by this limiter's global cap, shared across clones.
    pub fn rejected_total(&self) -> u64 {
        self.live.rejected.load(Ordering::Relaxed)
    }

    /// Resolve once no connection holds a guard. Used by shutdown, after the
    /// accept loops stopped and the connection tasks were told to end.
    pub async fn wait_idle(&self) {
        loop {
            let notified = self.live.idle.notified();
            tokio::pin!(notified);
            // Register for the wake-up BEFORE the check, so a guard dropped in
            // between cannot be missed.
            notified.as_mut().enable();
            if self.live.count.load(Ordering::Acquire) == 0 {
                return;
            }
            notified.await;
        }
    }
}

/// RAII guard for one occupied connection slot. `Drop` decrements the
/// counter — guaranteed to fire even on panic.
pub struct ConnGuard {
    limiter: ConnLimiter,
    active: bool,
}

impl Drop for ConnGuard {
    fn drop(&mut self) {
        if self.active {
            self.limiter.active.fetch_sub(1, Ordering::Relaxed);
        }
        if self.limiter.live.count.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.limiter.live.idle.notify_waiters();
        }
    }
}

// ============================================================================
// Per-IP connection cap — companion to the global `ConnLimiter`.
//
// Audit §2a / top-5 #4: the global cap alone lets a SINGLE attacker IP
// occupy an unbounded fraction of `max_active_connections` (default 10000)
// with slow-loris sockets, starving every legitimate client. This limiter
// bounds the number of in-flight (pre-handshake + post-handshake)
// connections from one source IP.
//
// Design (per CLAUDE.md concurrency ideology):
//   * Backing store: `DashMap<IpAddr, AtomicUsize, THasher>` — sharded
//     locks, workspace-default `FxHasher`. NOT `std::sync::Mutex`.
//   * Counter is an `AtomicUsize` INSIDE the value. `try_acquire`'s CAS
//     retry loop reads/writes it through the `entry()` `RefMut` borrow,
//     so — despite an earlier version of this comment claiming
//     otherwise (F-2, #1077) — the shard's write lock IS held for the
//     ENTIRE `try_acquire` critical section, not released early. This
//     is deliberate, not an oversight: it fully serializes `try_acquire`
//     against `release()`'s `get_mut` + remove-if-zero on the SAME IP
//     (same shard key), closing the exact TOCTOU race #1073 found in
//     `CursorRegistry::register` (an outstanding `Arc<AtomicUsize>`
//     clone taken AFTER dropping the shard guard can race a concurrent
//     removal's "is it still zero" check, since a raw CAS on an escaped
//     Arc is invisible to the map's locking). A future perf pass that
//     wants to shrink this critical section MUST re-verify against that
//     exact race before dropping the guard early — see `try_acquire`'s
//     doc comment.
//   * RAII guard with `Drop` — same pattern as `ConnGuard`, so every
//     early-exit path (panic, TLS failure, timeout) releases automatically.
//   * Map-bounded: the guard's `Drop` removes the entry when the count
//     falls to 0, so the map doesn't grow unboundedly across distinct
//     historical IPs. (Removing under the shard lock is safe and cheap —
//     a concurrent `try_acquire` for the same IP that missed this entry
//     simply re-inserts it.)
// ============================================================================

/// Per-IP cap-tracker. Cheaply cloneable — wraps an `Arc<DashMap<…>>` and
/// a fixed per-IP maximum. Constructed once at boot alongside
/// [`ConnLimiter`] and shared across every listener.
#[derive(Debug, Clone)]
pub struct PerIpLimiter {
    counts: Arc<DashMap<IpAddr, AtomicUsize, THasher>>,
    cap: usize,
}

impl PerIpLimiter {
    /// New limiter with the given per-IP hard cap. `cap == 0` is treated
    /// as "no per-IP limit" (every `try_acquire` returns `Some`, no map
    /// entries are ever created) — mirroring [`ConnLimiter::new`]'s
    /// convention.
    pub fn new(cap: usize) -> Self {
        Self {
            counts: Arc::new(DashMap::with_hasher(THasher::default())),
            cap,
        }
    }

    /// Try to reserve a per-IP slot for one new connection from `ip`.
    /// Returns `Some(guard)` on success — drop the guard to release the
    /// slot. Returns `None` when this IP is already at its cap.
    ///
    /// The counter lives in an `AtomicUsize` inside the map value. The CAS
    /// retry loop below borrows it from the `entry()` `RefMut`, so it DOES
    /// hold the DashMap shard's write lock for the whole loop, not just for
    /// `entry().or_insert_with()`'s slot resolution — a prior version of
    /// this comment claimed the lock was released early, which was false
    /// (F-2, #1077). Do NOT "fix" this by cloning the counter into an owned
    /// handle and dropping the `entry` guard early: that is the exact shape
    /// of the race #1073 found in `CursorRegistry::register` (see the
    /// module-level design comment above for why holding the guard here is
    /// what keeps `try_acquire` safely serialized against `release()`'s
    /// remove-if-zero on the same IP).
    pub fn try_acquire(&self, ip: IpAddr) -> Option<PerIpGuard> {
        if self.cap == 0 {
            // No per-IP cap configured — no-op guard (no map entry created).
            return Some(PerIpGuard {
                limiter: self.clone(),
                ip: None,
            });
        }

        // Materialise the per-IP atomic if this is the first connection
        // from `ip`. `entry()` always takes the shard's WRITE lock (it must
        // be able to insert), whether or not the key turns out to already
        // be present — and `entry` is held past this point, through the CAS
        // loop below, until `try_acquire` returns (see the doc comment
        // above and the module-level design comment for why).
        let entry = self.counts.entry(ip).or_insert_with(|| AtomicUsize::new(0));
        let counter = entry.value();

        // CAS loop on the atomic — bail out if at cap.
        let mut current = counter.load(Ordering::Relaxed);
        loop {
            if current >= self.cap {
                return None;
            }
            match counter.compare_exchange_weak(
                current,
                current + 1,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }

        Some(PerIpGuard {
            limiter: self.clone(),
            ip: Some(ip),
        })
    }

    /// Current count of active connections from `ip`. Cheap atomic load
    /// (0 if the IP has never been seen).
    pub fn active(&self, ip: IpAddr) -> usize {
        if self.cap == 0 {
            return 0;
        }
        self.counts
            .get(&ip)
            .map(|e| e.value().load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    /// Configured per-IP hard cap (or 0 if unlimited).
    pub fn cap(&self) -> usize {
        self.cap
    }

    /// Decrement the per-IP counter, and prune the map entry if it falls
    /// back to 0 — keeps the map bounded across distinct historical IPs.
    /// Called by `PerIpGuard::drop`.
    fn release(&self, ip: IpAddr) {
        // `entry()` gives us a write lock on the shard; we decrement and
        // remove-if-zero atomically with respect to other callers on the
        // SAME IP. A concurrent `try_acquire` for this IP that arrived
        // before this remove sees the decremented count; one that arrives
        // after the remove simply re-inserts a fresh `0` entry.
        if let Some(mut entry) = self.counts.get_mut(&ip) {
            let counter = entry.value_mut();
            let prev = counter.fetch_sub(1, Ordering::Relaxed);
            if prev <= 1 {
                // This decrement brings the count to 0 (or below —
                // defensive against double-release bugs). Drop the shard
                // entry so the map doesn't accumulate stale zero-count
                // IPs. We drop the `entry` guard first to avoid holding
                // the shard lock across `remove`.
                drop(entry);
                self.counts
                    .remove_if(&ip, |_, v| v.load(Ordering::Relaxed) == 0);
            }
        }
    }
}

/// RAII guard for one occupied per-IP connection slot. `Drop` decrements
/// the per-IP counter (and prunes the entry at 0) — guaranteed to fire
/// even on panic. `ip == None` means "no per-IP cap configured" and Drop
/// is a no-op.
pub struct PerIpGuard {
    limiter: PerIpLimiter,
    ip: Option<IpAddr>,
}

impl Drop for PerIpGuard {
    fn drop(&mut self) {
        if let Some(ip) = self.ip {
            self.limiter.release(ip);
        }
    }
}

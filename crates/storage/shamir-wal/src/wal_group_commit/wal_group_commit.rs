//! Two-tier group-commit coordinator over a [`WalSink`] (W2).
//!
//! [`WalGroupCommit`] coalesces many concurrent appends through one
//! rotating leader: a single `AtomicBool` CAS elects the leader, which
//! drains the pending queue and issues ONE
//! [`append_batch`](WalSink::append_batch) (`write()` → OS page cache,
//! level 2) and at most ONE [`sync`](WalSink::sync) (`fsync`, level 3)
//! per window. The rest park until their physical entry reaches the
//! requested durability tier.
//!
//! ## Durability tiers
//!
//! - [`WalDurability::Buffered`] → waiter acks after the batched `write()`
//!   (level 2): survives a process crash, lost only on power loss before a
//!   later `sync`.
//! - [`WalDurability::Synced`] → waiter acks after the batched `fsync()`
//!   (level 3): survives power loss.
//!
//! A window issues an `fsync` IFF it contains at least one `Synced`
//! waiter; a window of only `Buffered` appends needs no `fsync` at all.
//!
//! ## Correctness + liveness
//!
//! This reuses the verified leader/follower structure originally proved in
//! `shamir-tx`'s `group_fsync.rs` (D1b). That file was REMOVED in `e9e48c9`
//! ("remove dead GroupFsync — superseded by file-WAL WalGroupCommit"); the
//! verbatim liveness proof it carried lives on in git at
//! `f62fe18:crates/storage/shamir-tx/src/group_fsync.rs`, and the L1/L2/L3 arguments
//! it established are reproduced below and in
//! `docs/dev-artifacts/perf/capstone-subplan.md` §1:
//!   - **L1 (no stranded committer):** leadership is released under the SAME
//!     `pending` lock as `push`; after two windows a caller transfers reserved
//!     leadership to a continuation if work remains, including cancelled
//!     followers' frames. A late push is seen by the leader or elects itself;
//!   - **L2 (no lost wakeup):** the `enable()`-before-check `Notify` park
//!     loop closes the subscribe-window race;
//!   - **L3 (circuit-breaker):** on a write/sync error the leader releases
//!     leadership and returns, so the next append elects a fresh leader and
//!     no task spins on a dead segment;
//!   - completion is tied to the PHYSICAL entry: the leader that drains a
//!     `(payload, tier, waiter)` tuple is the task that calls
//!     `waiter.complete(..)` once that exact tuple has reached its tier.
//!
//! A single-writer-task replacement for the rotating leader (this `pending`
//! Mutex → bounded MPSC, the `flushing` CAS deleted) is designed in
//! `docs/dev-artifacts/perf/capstone-subplan.md`. It was PROTOTYPED and REVERTED: a
//! permanent writer task mandates a per-append cross-task `oneshot`
//! round-trip that suspends the executor on every append, whereas this
//! rotating leader drains the in-RAM `Mem` sink synchronously within one
//! poll. That regressed mem N=1 latency ~+22% (the subplan §0 GO/NO-GO
//! criterion) and broke an atomicity property the engine commit path relies
//! on (the non-yielding Mem append keeps a commit atomic on a current-thread
//! runtime; the mandatory yield let concurrent SSI committers all validate
//! before any published). The subtraction is therefore DEFERRED, not
//! rejected — see the subplan §5/§5-bis.
//!
//! ## Out of scope for W2
//!
//! The background fsync timer — which bounds the power-loss window for
//! `Buffered` entries on a quiet system or at shutdown by flushing the
//! page cache to disk on an interval — is NOT part of this primitive. It
//! belongs to the integration layer (W3). This coordinator only fsyncs
//! when a window carries a `Synced` waiter.
//!
//! **Wired in.** `shamir-tx`'s `RepoWalManager` (`repo_wal_manager.rs`) wraps
//! this coordinator in an `Arc<WalGroupCommit>` and funnels every WAL
//! append, replay, truncate, and sync call through it — it is the
//! production commit path (W3/W4 landed), not an unwired scaffold.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use shamir_storage::error::{DbError, DbResult};
use tokio::sync::{Mutex, Notify};

use super::recoverable_through::SyncCoordinator;
use crate::wal_sink::WalSink;
use crate::wal_ticket::{WalAppendError, WalTicket, WalTicketState};

/// Durability tier for a single WAL append.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum WalDurability {
    /// Ack after `write()` to the OS page cache (level 2): survives a
    /// process crash, lost only on power loss before a later `sync`.
    Buffered,
    /// Ack after `fsync` (level 3): survives power loss.
    Synced,
}

type Pending = (
    Vec<u8>,
    u64, /* commit_version */
    WalDurability,
    Arc<WalTicket>,
);

/// Lock-free-leader group commit over a [`WalSink`]. See the module-level
/// correctness argument (L1/L2/L3 — the structure originally proved in the
/// now-removed `group_fsync.rs`, preserved at `f62fe18`).
pub struct WalGroupCommit {
    pub(super) sink: Arc<WalSink>,
    // Sanctioned tokio::sync::Mutex (CLAUDE.md "Banned in hot paths"):
    // guards a tiny O(1) push / mem::take critical section, NEVER held
    // across `append_batch`/`sync` .await. Contention model: one push per
    // concurrent committer + one `mem::take` per drain window — sub-µs
    // under lock.
    //
    // SINGLE-WRITER BY CONSTRUCTION. The rotating leader (CAS on `flushing`)
    // makes exactly one task drain at a time, so this lock simulates a single
    // writer out of many equal committers. Historical bench observation
    // (`benches/wal_append.rs`, baseline `2e3bd51`): mem-sink append
    // throughput rose with concurrency (20.5K→91.2K, N=1→64). The historical
    // "4.4×" ratio is UNVERIFIED — no reproducible measurement exists in this
    // repo, so the ratio is withdrawn; group-commit coalescing makes each
    // added committer CHEAPER.
    //
    // A single-writer-task replacement (this `Mutex` → bounded MPSC, leader
    // CAS deleted) is designed in `docs/dev-artifacts/perf/capstone-subplan.md`. It was
    // PROTOTYPED and REVERTED (subplan §5/§5-bis): the permanent writer task
    // mandates a per-append cross-task `oneshot` round-trip that suspends the
    // executor on every append, whereas this leader drains the in-RAM `Mem`
    // sink synchronously within one poll. Measured cost: mem N=1 latency
    // ~+22% (the subplan §0 GO/NO-GO criterion) plus a broken commit-path
    // atomicity property (the non-yielding Mem append keeps a commit atomic
    // on a current-thread runtime; the mandatory yield let concurrent SSI
    // committers all validate before any published). Deferred, not rejected.
    pending: Arc<Mutex<Vec<Pending>>>,
    flushing: Arc<AtomicBool>,
    indeterminate: Arc<AtomicBool>,
    stopped: Arc<Notify>,
    // RF1: true when a Buffered append completed but no fsync has run since.
    // R = contiguous RECOVERABILITY prefix + coalesced-fsync state. All
    // logic in `wal_group_commit/recoverable_through.rs` (lock-free:
    // atomics + Notify). R advances only over contiguous covered runs —
    // never skips a hole (amendment B3 point 1).
    pub(super) sync: Arc<SyncCoordinator>,
}

impl WalGroupCommit {
    /// Test-only: arm the seal-fsync fault on this group's own WAL sink.
    #[doc(hidden)]
    pub fn arm_fail_next_seal_sync_for_test(&self) {
        self.sink.arm_fail_next_seal_sync_for_test();
    }

    pub fn new(sink: Arc<WalSink>) -> Self {
        Self {
            sink: Arc::clone(&sink),
            pending: Arc::new(Mutex::new(Vec::new())),
            flushing: Arc::new(AtomicBool::new(false)),
            indeterminate: Arc::new(AtomicBool::new(false)),
            stopped: Arc::new(Notify::new()),
            sync: Arc::new(SyncCoordinator::new(sink)),
        }
    }

    // Private shared handle for a continuation; all coordinator state is shared.
    fn shared_handle(&self) -> Self {
        Self {
            sink: Arc::clone(&self.sink),
            pending: Arc::clone(&self.pending),
            flushing: Arc::clone(&self.flushing),
            indeterminate: Arc::clone(&self.indeterminate),
            stopped: Arc::clone(&self.stopped),
            sync: Arc::clone(&self.sync),
        }
    }

    #[doc(hidden)]
    pub fn arm_fail_next_sync_for_test(&self) {
        self.sink.arm_fail_next_sync();
    }

    /// Test-only: arm the next Mem-sink `append_batch` to fail (Mem-only;
    /// panics on a File sink). Cross-crate test seam.
    #[doc(hidden)]
    pub fn arm_fail_next_append_for_test(&self) {
        self.sink.arm_fail_next_append_for_test();
    }

    pub fn requires_reopen(&self) -> bool {
        self.indeterminate.load(Ordering::Acquire)
    }

    /// Append one encoded payload at the given tier; returns once the
    /// entry has reached the requested durability level. `commit_version`
    /// is the MVCC version stamped on the entry — the leader folds the
    /// window's max into the sink's `max_committed` watermark (F6).
    ///
    /// Error fidelity (RV-WAL-C55C2D47): this legacy entry point converts
    /// every failure into [`DbError::Storage`] with a fixed, path-free
    /// outcome text, deliberately discarding the underlying sink cause.
    /// This preserves the legacy classification/match shape and keeps WAL
    /// file paths out of client-visible errors. Callers that need the
    /// underlying sink cause use [`Self::append_ticketed`] and
    /// [`WalTicket::failure_cause`](crate::WalTicket::failure_cause()),
    /// which retains the original `Arc<DbError>`.
    pub async fn append(
        &self,
        payload: Vec<u8>,
        commit_version: u64,
        durability: WalDurability,
    ) -> DbResult<()> {
        let (_, completion) = self.append_ticketed(payload, commit_version, durability);
        completion.await.map_err(DbError::from)
    }

    /// Return the frame's ticket and its lazy completion future without an
    /// extra allocation or task hop. Nothing is queued until the first poll.
    /// The future borrows this coordinator; the ticket is independently owned.
    /// cancel-safe: NO — a queued follower is still written; dropping a leader
    /// at append/sync awaits makes its window UNKNOWN and fail-stops the group.
    pub fn append_ticketed(
        &self,
        payload: Vec<u8>,
        commit_version: u64,
        durability: WalDurability,
    ) -> (
        Arc<WalTicket>,
        impl std::future::Future<Output = Result<(), WalAppendError>> + Send + '_,
    ) {
        let ticket = Arc::new(WalTicket::new());
        let waiter = Arc::clone(&ticket);
        (ticket, async move {
            {
                let mut pending = self.pending.lock().await;
                if self.indeterminate.load(Ordering::Acquire) {
                    waiter.complete(WalTicketState::Failed);
                    return Err(WalAppendError::Indeterminate);
                }
                pending.push((payload, commit_version, durability, Arc::clone(&waiter)));
            }
            self.drive_and_wait(&waiter).await
        })
    }

    async fn drive_and_wait(&self, waiter: &WalTicket) -> Result<(), WalAppendError> {
        if !self.indeterminate.load(Ordering::Acquire)
            && self
                .flushing
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        {
            let guard = LeaderGuard {
                group: self.shared_handle(),
                metas: Vec::new(),
                armed: true,
            };
            if let Some(guard) = self.lead_until_drained(guard, true).await {
                let group = guard.group.shared_handle();
                // fire-and-forget: reserved leadership owns queued work even if
                // every follower is cancelled. The guard exists before spawn,
                // so runtime shutdown before the first poll still fail-stops.
                drop(tokio::spawn(async move {
                    let _ = group.lead_until_drained(guard, false).await;
                }));
            }
        }
        loop {
            let notified = waiter.notify.notified();
            let stopped = self.stopped.notified();
            tokio::pin!(notified, stopped);
            notified.as_mut().enable();
            stopped.as_mut().enable();
            if let Some(outcome) = waiter.outcome() {
                return outcome;
            }
            if self.indeterminate.load(Ordering::Acquire) {
                // UNKNOWN is published before fail-stop. Recheck after the
                // acquire so a concurrent guard cannot be overwritten FAILED.
                if let Some(outcome) = waiter.outcome() {
                    return outcome;
                }
                // Still pending means this frame was never in the abandoned
                // window. It is known not written, but admission is fail-stop.
                waiter.complete(WalTicketState::Failed);
                return Err(WalAppendError::Indeterminate);
            }
            tokio::select! {
                _ = notified => {},
                _ = stopped => {},
            }
        }
    }

    /// Batched append — all payloads land in ONE leader window (one
    /// `append_batch` call to the sink), so the batch is atomic at the
    /// segment level: payloads are submitted together, and a partial write
    /// attempts to roll the batch back. File errors do not prove rollback
    /// and are UNKNOWN, fail-stopping the group; explicit group fsync errors
    /// mean WRITTEN, not absent. Legacy callers still receive DbError::Storage.
    ///
    /// `entries` is `(payload, commit_version)` pairs; `durability` applies to
    /// the whole batch. The window's `max_version` (folded into the sink's
    /// watermark) is the max of the entries' `commit_version`s.
    ///
    /// Atomicity argument: all entries are pushed to `pending` under ONE lock
    /// acquisition. The leader that drains the queue observes the full batch
    /// in one `mem::take` and issues a single `sink.append_batch(payloads, …)`
    /// — a partial write (ENOSPC mid-frame) quarantines the segment and
    /// attempts to roll back ALL frames to the pre-batch offset (§1.3).
    /// Only a successful rollback proves no frame survives. The File sink
    /// does not expose that distinction, so an append error is UNKNOWN;
    /// recovery may still observe frames from that indeterminate batch.
    ///
    /// Error fidelity (RV-WAL-C55C2D47): like [`Self::append`], this legacy
    /// entry point returns [`DbError::Storage`] with a fixed, path-free
    /// outcome text and discards the underlying sink cause — deliberate,
    /// to preserve classification and keep WAL file paths out of
    /// client-visible errors. Use [`Self::append_ticketed`] plus
    /// [`WalTicket::failure_cause`](crate::WalTicket::failure_cause())
    /// for the preserved `Arc<DbError>` cause.
    pub async fn append_many(
        &self,
        entries: Vec<(Vec<u8>, u64)>,
        durability: WalDurability,
    ) -> DbResult<()> {
        if self.indeterminate.load(Ordering::Acquire) {
            return Err(WalAppendError::Indeterminate.into());
        }
        if entries.is_empty() {
            return Ok(());
        }
        let waiter = Arc::new(WalTicket::new());
        {
            let mut pending = self.pending.lock().await;
            if self.indeterminate.load(Ordering::Acquire) {
                return Err(WalAppendError::Indeterminate.into());
            }
            for (payload, commit_version) in entries {
                pending.push((payload, commit_version, durability, Arc::clone(&waiter)));
            }
        }
        self.drive_and_wait(&waiter).await.map_err(DbError::from)
    }

    // The caller owns leadership for at most two windows. If more work is
    // queued, transfer its still-armed guard without releasing `flushing`.
    // The continuation drains to empty and releases under the pending lock.
    async fn lead_until_drained(
        &self,
        mut guard: LeaderGuard,
        bounded: bool,
    ) -> Option<LeaderGuard> {
        let mut windows = 0;
        loop {
            let batch = {
                let mut p = self.pending.lock().await;
                if self.indeterminate.load(Ordering::Acquire) || p.is_empty() {
                    guard.armed = false;
                    self.flushing.store(false, Ordering::Release);
                    return None;
                }
                if bounded && windows == 2 {
                    return Some(guard);
                }
                std::mem::take(&mut *p)
            };
            // Split without cloning payloads.
            let mut payloads = Vec::with_capacity(batch.len());
            guard.metas = Vec::with_capacity(batch.len());
            let mut batch_max_version = 0u64;
            for (p, version, tier, w) in batch {
                payloads.push(p);
                batch_max_version = batch_max_version.max(version);
                guard.metas.push((tier, w));
            }
            // One write() for the whole window (level 2). The window's max
            // commit_version is the watermark folded into the sink (F6).
            let write_result = self.sink.append_batch(payloads, batch_max_version).await;
            // Preserve the underlying failure cause for every waiter in this
            // window BEFORE any completion publishes `done` (plan P1.5).
            let write_cause: Option<Arc<DbError>> = write_result.err().map(Arc::new);
            if let Some(cause) = &write_cause {
                for (_, ticket) in &guard.metas {
                    ticket.set_cause(Arc::clone(cause));
                }
            }
            let write_ok = write_cause.is_none();
            // A File sink error does not prove rollback: SegmentSet can fail
            // rotating AFTER a successful write, and rollback failure is not
            // typed by WalSegment. Conservatively fail-stop those outcomes.
            if !write_ok && matches!(self.sink.as_ref(), WalSink::File(_)) {
                return None;
            }
            // Publish WRITTEN before this coordinator's explicit sync. File
            // auto-rotation can fsync inside append_batch before it returns;
            // that earlier boundary is not observable at this layer.
            // Wake Buffered waiters immediately — level 2 reached. (write()
            // success means bytes are in the OS page cache regardless of a
            // later fsync outcome.)
            if write_ok {
                for (_, ticket) in &guard.metas {
                    ticket.written();
                }
                self.sync.written_generation.fetch_add(1, Ordering::Release);
                self.sync.dirty_since_sync.store(true, Ordering::Release);
                self.sync.done.notify_waiters();
            }
            for (tier, w) in &guard.metas {
                if *tier == WalDurability::Buffered {
                    w.complete(if write_ok {
                        WalTicketState::Acked
                    } else {
                        WalTicketState::Failed
                    });
                }
            }
            // One fsync for the window iff any Synced waiter is present.
            let needs_fsync = guard.metas.iter().any(|(t, _)| *t == WalDurability::Synced);
            let sync_cause = if write_ok && needs_fsync {
                // Synced appends share the same cancellation-independent
                // worker as timer and pull demands; never call sink.sync here.
                // Already an Arc<DbError>: the ORIGINAL sink error, no
                // stringification — set_cause preserves variant/kind/source.
                self.sync_window().await.err()
            } else {
                None
            };
            if let Some(cause) = &sync_cause {
                for (tier, ticket) in &guard.metas {
                    if *tier == WalDurability::Synced {
                        ticket.set_cause(Arc::clone(cause));
                    }
                }
            }
            let sync_ok = if write_ok && needs_fsync {
                sync_cause.is_none()
            } else {
                write_ok
            };
            for (tier, w) in &guard.metas {
                if *tier == WalDurability::Synced {
                    w.complete(if !write_ok {
                        WalTicketState::Failed
                    } else if sync_ok {
                        WalTicketState::Acked
                    } else {
                        WalTicketState::Written
                    });
                }
            }
            // Circuit breaker: on write or sync error, release leadership
            // and return so the next append elects a fresh leader. Without
            // this, a dead segment causes infinite spin.
            guard.metas.clear();
            if !write_ok || (needs_fsync && !sync_ok) {
                // Fail queued (not yet written) frames too: no follower may
                // remain parked waiting for a future append to elect a leader.
                let mut pending = self.pending.lock().await;
                for (_, _, _, ticket) in pending.drain(..) {
                    ticket.complete(WalTicketState::Failed);
                }
                guard.armed = false;
                self.flushing.store(false, Ordering::Release);
                return None;
            }
            if bounded {
                windows += 1;
            }
        }
    }

    /// Replay all `WalEntryV2` records persisted in the underlying sink.
    pub async fn replay(&self) -> DbResult<Vec<crate::wal_entry_v2::WalEntryV2>> {
        self.sink.replay().await
    }

    /// F6b: delegate WAL truncation to the sink — reclaim every record fully
    /// durable in history (`commit_version` in `(0, durable]`). Returns the
    /// count reclaimed. The caller (drainer) is responsible for flushing
    /// history before this (I2).
    pub async fn truncate_below(&self, durable: u64) -> DbResult<usize> {
        super::history_coverage::truncate(self, durable).await
    }

    /// F6b: cheap probe gating the drainer's history-flush + truncate so it
    /// fires only on a segment/frame boundary, never per-commit (I2).
    pub fn has_truncatable(&self, durable: u64) -> bool {
        self.sink.has_truncatable(durable)
    }

    pub async fn sealed_retained_bytes(&self) -> DbResult<u64> {
        self.sink.sealed_retained_bytes().await
    }

    /// `shamir_wal_segments` source: retained segment FILES (sealed + active).
    pub fn segment_count(&self) -> u64 {
        self.sink.segment_count()
    }

    /// `shamir_wal_size_bytes` source: approximate on-disk WAL size
    /// (active + sealed; Mem reports 0).
    pub fn wal_size_bytes(&self) -> u64 {
        self.sink.wal_size_bytes()
    }

    /// `shamir_wal_bytes_total` source: cumulative WAL payload bytes appended.
    pub fn bytes_written_total(&self) -> u64 {
        self.sink.bytes_written_total()
    }

    /// `shamir_wal_truncation_lag_versions` source: distance from
    /// `durable_version` to the oldest reclaimable-but-retained sealed
    /// segment boundary (0 when nothing is truncatable).
    pub fn truncation_lag_versions(&self, durable_version: u64) -> u64 {
        self.sink.truncation_lag_versions(durable_version)
    }

    /// `shamir_wal_fsync_seconds` source: torn-free snapshot of the
    /// sync-worker round histogram (snapshot replay + sink sync, `ns`).
    pub fn fsync_histogram_snapshot(&self) -> shamir_metrics::HistogramSnapshot {
        self.sync.fsync_histogram_snapshot()
    }

    // `sync_now` (coalesced) lives in
    // `wal_group_commit/recoverable_through.rs`, alongside the `R` prefix it
    // publishes.

    /// Atomically read and clear the dirty flag. Returns `true` if a
    /// Buffered append happened since the last fsync.
    pub fn take_dirty(&self) -> bool {
        self.sync.dirty_since_sync.swap(false, Ordering::AcqRel)
    }

    /// Spawn a background task that fsyncs the WAL every `interval` IFF a
    /// Buffered append happened since the last sync. The timer is nominal,
    /// not a hard power-loss bound: scheduling, I/O and errors can extend it.
    /// Weak-ref lifecycle: the task
    /// exits when the last `Arc<WalGroupCommit>` is dropped (no leak —
    /// mirrors MemBufferStore's flusher). No-op sinks make `sync_now()` cheap.
    ///
    /// **Audit §1.5:** `take_dirty()` clears the flag BEFORE attempting the
    /// fsync. If the fsync fails, restoring the flag (`store(true)`) ensures
    /// the next tick retries instead of leaving the dirty state lost with no
    /// further attempt until a new append arrives — on a quiescent system
    /// that is an unbounded data-at-risk window. The fsync error is logged
    /// loudly (it was previously swallowed by `let _ =`). Repeated fsync
    /// failures cause the sink to quarantine the segment (see §1.3 — the
    /// `WalSegment::sync` poison path forces the leader to rotate), so this
    /// loop converges: either the fsync eventually succeeds (clearing the
    /// flag) or every append starts failing fast on a poisoned segment.
    pub fn spawn_background_fsync(self: &Arc<Self>, interval: Duration) {
        let weak = Arc::downgrade(self);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                match weak.upgrade() {
                    Some(g) => {
                        if g.take_dirty() {
                            if let Err(e) = g.sync_now().await {
                                // Restore the dirty flag so the next tick
                                // retries the fsync instead of silently
                                // abandoning the unflushed Buffered entries.
                                g.sync.dirty_since_sync.store(true, Ordering::Release);
                                log::error!(
                                    "WalGroupCommit background fsync failed: {e}; \
                                     dirty flag restored — will retry next tick"
                                );
                            }
                        }
                    }
                    None => break,
                }
            }
        });
    }

    #[cfg(test)]
    pub(crate) fn fsync_count(&self) -> u64 {
        self.sync.rounds.load(Ordering::Relaxed)
    }

    /// Test-only: is the dirty flag currently set? Used by the §1.5
    /// regression test to assert the background fsync restores it after a
    /// failed sync (rather than silently losing it).
    #[cfg(test)]
    pub(crate) fn is_dirty(&self) -> bool {
        self.sync.dirty_since_sync.load(Ordering::Acquire)
    }
}

// Owns the existing metadata allocation, not another per-window allocation.
// Drop performs only synchronous publication/wakes; it never waits or spawns.
struct LeaderGuard {
    group: WalGroupCommit,
    metas: Vec<(WalDurability, Arc<WalTicket>)>,
    armed: bool,
}

impl Drop for LeaderGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        // Complete the window before publishing fail-stop; observers acquiring
        // indeterminate must see UNKNOWN rather than misclassifying it FAILED.
        // A detached blocking write may still land, so another leader must
        // never touch the sink.
        for (_, ticket) in &self.metas {
            if ticket.outcome().is_none() {
                ticket.complete(WalTicketState::Unknown);
            }
        }
        self.group.indeterminate.store(true, Ordering::Release);
        // No async lock in Drop. Queued followers observe stopped even when
        // a concurrent producer currently owns the pending mutex.
        self.group.stopped.notify_waiters();
        self.group.flushing.store(false, Ordering::Release);
    }
}

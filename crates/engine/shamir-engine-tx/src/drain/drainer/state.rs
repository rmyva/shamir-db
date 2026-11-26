use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use arc_swap::ArcSwapOption;
use scc::TreeIndex;
use shamir_collections::THasher;
use shamir_wal::WalEntryV2;
use tokio::sync::{Mutex, Notify};

use crate::drain::drain_pause::DrainPauseState;

#[path = "diagnostics.rs"]
mod diagnostics;
#[path = "drain_step.rs"]
pub(super) mod drain_step;
#[path = "truncate.rs"]
mod truncate;
#[path = "window.rs"]
mod window;

/// Default high-watermark for the drain window. When `window.len() >=` this
/// value, `offer` drops the entry (backpressure). The gap-reseed path in
/// `drain_step` recovers dropped entries from the WAL on the next pass.
const DEFAULT_WINDOW_HIGH_WATERMARK: usize = 64 * 1024;

/// Repo-level single-owner drainer.
///
/// Holds a [`Notify`] so a producer (the commit path, in P1d-2b) can wake
/// the drain loop on every commit, plus a monotonic counter of versions
/// drained so far (telemetry / tests). One background task per repo owns
/// the loop; explicit drains serialize with it across awaited replay I/O.
/// Producers still offer entries and publish watermarks lock-free.
pub struct Drainer {
    /// Woken on every commit (P1d-2b) so the loop drains promptly without
    /// busy-polling. The interval in [`spawn`](Self::spawn) is a backstop.
    notify: Notify,
    /// Total versions drained across all `drain_step` calls — telemetry and
    /// a hook for tests to assert progress without reading the gate.
    drained_total: AtomicU64,
    /// pub(super): read directly by the `orphan_repair` modules
    /// (mounted in the parent `drainer`); audience identical to the
    /// pre-split private-in-`drainer` scope.
    pub(super) orphan_replay_attempts_total: AtomicU64,
    /// pub(super): read directly by the `orphan_repair` modules
    /// (mounted in the parent `drainer`); audience identical to the
    /// pre-split private-in-`drainer` scope.
    pub(super) orphan_replay_failures_total: AtomicU64,
    /// pub(super): read directly by the `orphan_repair` modules
    /// (mounted in the parent `drainer`); audience identical to the
    /// pre-split private-in-`drainer` scope.
    pub(super) orphan_replay_consecutive_failures: AtomicU64,
    /// pub(super): read/written directly by the `orphan_repair` modules
    /// (mounted in the parent `drainer`); audience identical to the
    /// pre-split private-in-`drainer` scope.
    pub(super) orphan_replay_last_error: ArcSwapOption<String>,
    /// pub(super): read/written directly by the `orphan_repair` modules
    /// (mounted in the parent `drainer`); audience identical to the
    /// pre-split private-in-`drainer` scope.
    pub(super) orphan_repair_metrics: super::orphan_repair::Metrics,
    /// Per-repo ordered window of inflight WAL entries already offered by
    /// the commit path. Replaces wal.recover() on the drain hot path —
    /// see Op #2 design.
    ///
    /// pub(super): read directly by the `orphan_repair` modules (mounted in
    /// the parent `drainer`); audience identical to the pre-split
    /// private-in-`drainer` scope.
    pub(super) window: TreeIndex<u64, Arc<WalEntryV2>>,
    /// Op #2 Stage 4: hard cap on the window depth. When `window.len() >=
    /// high_watermark`, `offer` becomes a no-op (the entry is dropped).
    /// Default 64K. Configurable via `set_window_high_watermark`.
    window_high_watermark: AtomicUsize,
    /// Op #2 Stage 4: total entries dropped by `offer` due to backpressure.
    /// Telemetry only — never affects correctness (drops are recovered by
    /// `drain_step`'s gap-reseed path).
    offer_dropped_total: AtomicU64,
    /// PT 2: total `wal.recover()` calls fired from `drain_step`'s
    /// gap-reseed path. Telemetry; load-bearing in regression tests that
    /// assert the new empty-prefix trigger does NOT fire spuriously on
    /// interior aborts.
    recover_calls: AtomicU64,
    /// PT 1: approximate depth of `window`. Atomic mirror so `offer`'s
    /// backpressure check is O(1) instead of `scc::TreeIndex::len()`'s
    /// O(N) full-tree walk (verified in scc-2.4.0 source). Updated at
    /// every site that mutates `window`. Approximate under contention
    /// (multiple offers racing the check + insert) — fine because
    /// `window_high_watermark` is a SOFT cap (Stage 4 contract: drops
    /// recovered via gap-reseed). Bounded slack ≤ concurrent committer
    /// count.
    window_depth: AtomicUsize,
    /// CRIT-2 (#436): every entry version whose A5 interner-hwm gate has
    /// EVER reported unsafe and has not yet been individually reconfirmed
    /// safe. Keyed by `commit_version`, value is the entry's interner-delta
    /// max id (`0` = none, matching `entry_interner_max_id`'s `None`;
    /// interner ids are 1-based — see
    /// `interner_manager.rs::persisted_high_water`).
    ///
    /// This is the SINGLE authoritative source for the truncation ceiling:
    /// `ceiling = min(durable_watermark, min_key(pending_unsafe) - 1)`,
    /// recomputed fresh on every call, never cached as a per-call local. Two
    /// earlier (rejected, @sh-caught) designs both failed under concurrent
    /// `drain_step` callers sharing one `Drainer` via `repo.drainer()` (the
    /// ambient background loop races any explicit `drain_all` caller):
    ///   1. A per-call `truncation_ceiling` local seeded from
    ///      `durable_watermark()` read at call start — poisoned by a
    ///      CONCURRENT call's `mark_durable` advancing the SAME watermark
    ///      for still-unsafe entries before this call computed its ceiling.
    ///   2. A single `unsafe_floor_version` "first-unsafe-only" latch —
    ///      only ever recorded the FIRST entry to trip unsafe; a LATER
    ///      still-unsafe entry in a subsequent pass silently had no floor
    ///      to latch onto (the single-slot floor was already occupied), so
    ///      once the FIRST entry's delta was confirmed safe, truncation
    ///      jumped straight to `durable_watermark` — past the second
    ///      entry's still-unpersisted delta.
    ///
    /// A concurrent map (lock-free, `scc::TreeIndex`) that tracks EVERY
    /// pending-unsafe version and re-derives the ceiling as "one below the
    /// smallest still-pending version" closes both gaps: it is always
    /// exhaustive (every unsafe entry is tracked, not just the first) and
    /// always current (no per-call snapshot to go stale).
    pending_unsafe: TreeIndex<u64, u64>,
    /// `(commit_version, table_token)` pairs whose WAL index ops (`IndexPut` /
    /// `IndexDel`) the commit path ALREADY applied inline (Phase 5c) before the
    /// version became visible. The drain must not replay those: replaying an
    /// older entry's `Set` posting after a newer inline commit removed it
    /// resurrects the posting until the newer entry is replayed too, and the
    /// raw `info_store` write never invalidated the posting cache, so a read
    /// that cached the resurrected list kept serving it after the removal
    /// (SET NULL left `dept_id = 100` matching a row whose FK was already
    /// null — CI flake, ~12% under load). Keys are captured before offer's
    /// backpressure check; cold recovery/async-index entries start unapplied.
    index_applied: scc::HashMap<(u64, u64), (bool, Vec<bytes::Bytes>), THasher>,
    /// Exact info-store key granularity (not table/index): unrelated postings
    /// must still replay. Retained only above the contiguous durable watermark.
    posting_applied: scc::HashMap<(u64, bytes::Bytes), u64, THasher>,
    counter_applied: scc::HashMap<(u64, u64), (), THasher>,
    // Sanctioned async mutex: bounded contention between the background pass
    // and explicit drains; the guard must span awaited replay/history I/O.
    // Lock order: replay -> per-table drain/marker locks. No callee re-enters
    // drain_step/drain_all; truncation uses flush_all_history, not flush_buffers.
    //
    // Arc-shared since R2a-1d: the capture/seal barriers take an owned
    // pause over this SAME lock (`DrainPauseState`) that can outlive the
    // creating call (`Send + 'static` seal hold).
    replaying: Arc<Mutex<()>>,
    /// R2a-1d: the owned pause facade over `replaying` (one per drainer).
    /// While its guard is held, `drain_step`, `drain_all`, the background
    /// loop and the truncation wait are all blocked (they acquire
    /// `replaying` through `drain_step` / `settle_and_truncate`).
    pause: DrainPauseState,
}

impl Default for Drainer {
    fn default() -> Self {
        Self::new()
    }
}

impl Drainer {
    /// Create a fresh, idle drainer.
    pub fn new() -> Self {
        let replaying_cell = Arc::new(Mutex::new(()));
        Self {
            notify: Notify::new(),
            drained_total: AtomicU64::new(0),
            orphan_replay_attempts_total: AtomicU64::new(0),
            orphan_replay_failures_total: AtomicU64::new(0),
            orphan_replay_consecutive_failures: AtomicU64::new(0),
            orphan_replay_last_error: ArcSwapOption::empty(),
            orphan_repair_metrics: super::orphan_repair::Metrics::default(),
            window: TreeIndex::new(),
            window_high_watermark: AtomicUsize::new(DEFAULT_WINDOW_HIGH_WATERMARK),
            offer_dropped_total: AtomicU64::new(0),
            recover_calls: AtomicU64::new(0),
            window_depth: AtomicUsize::new(0),
            pending_unsafe: TreeIndex::new(),
            index_applied: scc::HashMap::with_hasher(THasher::default()),
            posting_applied: scc::HashMap::with_hasher(THasher::default()),
            counter_applied: scc::HashMap::with_hasher(THasher::default()),
            replaying: Arc::clone(&replaying_cell),
            pause: DrainPauseState::new(Arc::clone(&replaying_cell)),
        }
    }

    /// R2a-1d: the pause facade over `replaying` (capture/seal barriers).
    pub(crate) fn pause(&self) -> &DrainPauseState {
        &self.pause
    }

    /// R2a-1d: busy probe for the barrier entry check — a seal hold or a
    /// transient pause currently owns `replaying`.
    pub(crate) fn pause_held(&self) -> bool {
        self.pause.is_held()
    }

    /// Test-only: yield the future that owns the `replaying` lock WITHOUT
    /// the pause busy flag — exactly the state an in-flight drain pass
    /// leaves behind. The cancel-safety tests need this to park a pause
    /// acquisition deterministically: `DrainPauseState::acquire` checks the
    /// busy flag FIRST (a second acquisition while a guard holds the pause
    /// is refused with `DrainPauseBusy` before it can ever park), so the
    /// flag-cleanup path of a cancelled acquisition is reachable only when
    /// someone else (a drain pass) holds the lock flag-free.
    #[cfg(test)]
    pub(crate) fn test_replaying_lock(
        &self,
    ) -> impl std::future::Future<Output = tokio::sync::OwnedMutexGuard<()>> {
        Arc::clone(&self.replaying).lock_owned()
    }

    /// R2a-1d: pending-work probe for the settle/recheck checks (atomic
    /// mirror of `window`; production-capable twin of the test-only
    /// `window_depth`).
    pub(crate) fn pending_window_len(&self) -> usize {
        self.window_depth.load(Ordering::Relaxed)
    }
}

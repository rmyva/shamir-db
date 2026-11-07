//! P0-3a (#1011) — reader-vs-DROP mutual exclusion gate.
//!
//! Closes the "sub-bug 3a" known gap documented on
//! [`base_index::index_manager::IndexManager::drop_index`]: a reader that
//! resolves an index definition BEFORE `drop_index` retires it can, with no
//! synchronization at all, go on to scan the shared posting store WHILE the
//! retire's physical sweep is running — observing a partially-swept
//! keyspace (an incomplete, never corrupted, result).
//!
//! # Design — flag + counter + reader back-off, not epoch-parity RCU
//!
//! Epoch-parity (the classic RCU grace-period pattern: readers pick a
//! parity, the writer flips it and drains the OLD parity while new readers
//! proceed on the NEW one) buys nothing here: there is only ONE physical
//! copy of the postings, not two generations to serve reads from — a
//! reader landing in the "new parity" would still scan the exact bytes the
//! sweep is about to erase. So this mirrors the SIMPLER, already-proven
//! shape this codebase uses for the mirror-image problem (drain in-flight
//! WRITERS before a DDL op proceeds):
//! [`shamir_engine::table::writer_drain_barrier::WriterDrainBarrier`] — same
//! flag-then-counter memory-model proof, roles swapped (there: writers
//! drain, a flag gates NEW writers onto the slow path; here: DROP drains
//! READERS, a flag makes NEW readers back off instead of proceeding).
//!
//! Memory-model proof (mirrors `WriterDrainBarrier`'s, verbatim reasoning,
//! roles swapped) — both cross-atomic operations are `SeqCst` so the single
//! total order settles which of the two orderings occurred:
//!
//! 1. Reader: `in_flight.fetch_add(1, SeqCst)` THEN `dropping.load(SeqCst)`.
//! 2. Drop: `dropping.store(true, SeqCst)` THEN
//!    `while in_flight.load(SeqCst) != 0 {}`.
//!
//! Two exhaustive cases:
//! - Reader's `fetch_add` precedes drop's `store`: drop's later
//!   `in_flight.load` observes the increment → **drop waits for this
//!   reader** (the reader's own `dropping.load`, happening-after the
//!   store since it comes later in the SAME total order at the reader's
//!   physical program point only if the store already happened — worst
//!   case the reader's check races the store and can go either way, but
//!   either way it either proceeds AND is counted (drop waits for it) or
//!   backs off AND decrements before drop's wait can conclude — no data
//!   race, no missed synchronization, exactly `WriterDrainBarrier`'s own
//!   proof shape).
//! - Drop's `store` precedes reader's `fetch_add`: reader's later
//!   `dropping.load` (after incrementing) observes `true` → **reader backs
//!   off** (decrements back out, returns `None`).
//!
//! Termination (the property epoch-parity exists to buy, and a naive single
//! counter without the flag lacks) — **not** proved via strict monotonicity
//! of `in_flight` after `dropping` goes true, despite an earlier version of
//! this doc claiming exactly that (F-10, 2026-08-09 review): `enter()`'s own
//! ordering (`fetch_add` BEFORE the flag check) means a reader arriving
//! AFTER the flag is already `true` still transiently bumps `in_flight` by 1
//! for the handful of instructions between its `fetch_add` and the
//! `fetch_sub` that undoes it once it observes `dropping == true` — so
//! `in_flight` is not monotonically non-increasing once `dropping` flips,
//! and `wait_for_drain`'s sampling loop can, in principle, keep re-observing
//! a nonzero count under a continuous stream of new readers even after
//! every PRE-flag reader has long since finished. What IS true, and is what
//! actually bounds the wait: every pre-flag reader is counted exactly once
//! and decrements exactly once (no reader can hold the count forever
//! without a bug elsewhere), and every post-flag reader's straddling window
//! is O(1) instructions — nanoseconds, not microseconds — so in practice the
//! sampling loop's `yield_now` gap is always eventually wide enough to land
//! on an instant with zero straddling readers. This is a practical
//! termination guarantee, not a formal starvation-freedom proof against an
//! adversarial unbroken stream of concurrent `enter()` calls — the same
//! class of guarantee this codebase already accepts for
//! `WriterDrainBarrier::drain`'s mirror-image unbounded wait.
//!
//! # Placement — INSIDE the read chokepoint, never at the earlier "resolve"
//! step
//!
//! The engine's planner resolves a candidate index NAME long before issuing
//! the physical read (`iter_indexes_ready()` → later, separately,
//! `lookup_by_index(name, ..)`) — no definition/token object crosses that
//! gap today. A guard acquired at resolve time would be held across
//! `unique_write_lock` acquisitions elsewhere in the engine, which is
//! exactly how a real ABBA deadlock gets constructed against
//! `TableManager::begin_write_barrier`'s own
//! `ddl_admission → bit → drain_writers → unique_write_lock` sequence.
//! Acquired INSIDE the chokepoint (the read method touches the posting
//! store and leaf caches while holding the guard). Do not acquire an outer DDL
//! or lifecycle lock while holding a [`ReadGuard`]. Leaf cache shard locks are
//! permitted: they do not wait for the drain gate and are released before await.
//!
//! # Granularity
//!
//! One gate per manager (regular `IndexManager`, `SortedIndexManager`),
//! not per-index — a per-index counter would need an extra map probe on
//! every hot-path read to shrink a DDL-rare window, not worth it without
//! measurement. Collateral effect: while index A is being dropped, reads of
//! sibling index B on the SAME table also back off (fall back to full
//! scan) for the drain+sweep window — bounded, and no worse than the
//! write-barrier bit already forcing every writer on that table onto the
//! slow path for the same window.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

/// Shared reader-vs-DROP exclusion gate. Cheap to clone — every clone
/// shares the same counters (mirrors `IndexManager`'s own
/// `generation: Arc<AtomicU64>` sharing pattern).
#[derive(Debug, Clone)]
pub struct ReaderDrainGate {
    in_flight: Arc<AtomicUsize>,
    dropping: Arc<AtomicBool>,
    /// Telemetry + test oracle: incremented exactly once per `begin_drop`
    /// call whose `wait_for_drain` actually observed a non-zero
    /// `in_flight` on its first check (i.e. genuinely had to wait, not a
    /// vacuous zero-cost pass). See the struct's own test module for why a
    /// lone "waits == 0" assertion is not sufficient proof by itself.
    drain_waits: Arc<AtomicUsize>,
}

impl ReaderDrainGate {
    pub fn new() -> Self {
        Self {
            in_flight: Arc::new(AtomicUsize::new(0)),
            dropping: Arc::new(AtomicBool::new(false)),
            drain_waits: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Reader entry point. Hot path: one `SeqCst` RMW plus, on the common
    /// (no concurrent DROP) case, one more `SeqCst` load — no allocation,
    /// no lock, no await.
    ///
    /// `Some(guard)` — safe to read the physical posting store for the
    /// guard's lifetime. `None` — a DROP is currently between raising its
    /// intent and finishing its sweep; the caller MUST NOT read the
    /// physical store and must fall back (e.g. full scan) instead.
    #[must_use]
    pub fn enter(&self) -> Option<ReadGuard> {
        // SeqCst: see the struct doc's memory-model proof — increment
        // BEFORE checking the flag, mirroring `WriterDrainBarrier::enter_writer`.
        self.in_flight.fetch_add(1, Ordering::SeqCst);
        if self.dropping.load(Ordering::SeqCst) {
            // A DROP is in its raise->sweep window (or raised after our
            // increment — either way, safe to just not proceed). Undo the
            // increment and back off.
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            return None;
        }
        Some(ReadGuard {
            in_flight: Arc::clone(&self.in_flight),
        })
    }

    /// DROP entry point. Raises the intent flag (`SeqCst`) so every NEW
    /// reader backs off, and returns an RAII guard whose
    /// [`wait_for_drain`](DropDrainGuard::wait_for_drain) waits for every
    /// reader that was ALREADY in flight to finish and drop its
    /// [`ReadGuard`]. Dropping the returned guard (even without calling
    /// `wait_for_drain`) clears the flag — callers that error out before
    /// reaching the sweep still leave the gate in a consistent state.
    #[must_use]
    pub fn begin_drop(&self) -> DropDrainGuard {
        self.dropping.store(true, Ordering::SeqCst);
        DropDrainGuard { gate: self.clone() }
    }

    /// Test-only: current in-flight reader count. Exposed for regression tests
    /// to verify a reader is counted mid-flight (e.g., to prove `in_flight_count()
    /// == 1` while a read is parked at `lookup_pause_hook`). Not `#[cfg(test)]`-gated
    /// — cross-crate test consumer.
    pub fn in_flight_count(&self) -> usize {
        self.in_flight.load(Ordering::Acquire)
    }

    /// Telemetry + test oracle: how many `begin_drop` drains genuinely had
    /// to wait for at least one in-flight reader (as opposed to observing
    /// zero on the first check).
    pub fn drain_waits(&self) -> usize {
        self.drain_waits.load(Ordering::Acquire)
    }
}

impl Default for ReaderDrainGate {
    fn default() -> Self {
        Self::new()
    }
}

/// RAII guard returned by [`ReaderDrainGate::enter`]. Decrements the
/// in-flight counter on drop (covers early `?` returns and panics inside
/// the guarded read — the counter can never leak stuck).
#[derive(Debug)]
pub struct ReadGuard {
    in_flight: Arc<AtomicUsize>,
}

impl Drop for ReadGuard {
    fn drop(&mut self) {
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
    }
}

/// RAII guard returned by [`ReaderDrainGate::begin_drop`]. Clears the
/// `dropping` flag on drop (covers early `?` returns inside the caller's
/// drop sequence) — always AFTER any `wait_for_drain` call has returned,
/// since `wait_for_drain` takes `&self` and the flag is only cleared when
/// the WHOLE guard drops.
#[derive(Debug)]
pub struct DropDrainGuard {
    gate: ReaderDrainGate,
}

impl DropDrainGuard {
    /// Wait until every reader that was in flight when this guard was
    /// created (i.e. every reader that entered before `dropping` went
    /// true) has finished and dropped its [`ReadGuard`].
    ///
    /// Mirrors `WriterDrainBarrier::drain`'s exact shape (yield-loop,
    /// `SeqCst` load, escalating diagnostic log past 1s of waiting) —
    /// deliberately unbounded, consistent with that sibling primitive's
    /// own precedent: this wait sits inside an already-held
    /// `ddl_admission`+`unique_write_lock` critical section
    /// (`TableManager::drop_index` → `begin_write_barrier` →
    /// `IndexManager::drop_index`), and `WriterDrainBarrier::drain` — used
    /// in the exact same critical section for the mirror-image problem —
    /// is itself unbounded. A stuck reader is already a latent hazard this
    /// codebase accepts for the writer-drain case; the escalating log is
    /// the same tripwire this uses to catch it in practice.
    ///
    /// When no reader is in flight, returns after a single `SeqCst` load.
    pub async fn wait_for_drain(&self) {
        let drain_started = std::time::Instant::now();
        let mut last_report = drain_started;
        let mut counted_wait = false;
        while self.gate.in_flight.load(Ordering::SeqCst) != 0 {
            if !counted_wait {
                self.gate.drain_waits.fetch_add(1, Ordering::Relaxed);
                counted_wait = true;
            }
            tokio::task::yield_now().await;
            if last_report.elapsed() >= std::time::Duration::from_secs(1) {
                log::warn!(
                    "ReaderDrainGate::wait_for_drain: still waiting after {:?}, \
                     in_flight={} (possible stuck reader, see task #1011)",
                    drain_started.elapsed(),
                    self.gate.in_flight.load(Ordering::SeqCst)
                );
                last_report = std::time::Instant::now();
            }
        }
    }
}

impl Drop for DropDrainGuard {
    fn drop(&mut self) {
        self.gate.dropping.store(false, Ordering::SeqCst);
    }
}

// ============================================================================
// F-1103 (#1103) — loom model of the reader-drain gate's interleaving contract.
//
// RUN (NOT part of `./scripts/test.sh` — loom is opt-in via the `loom` cargo
// feature, which the `build.rs` translates into a crate-local `cfg(loom)` so
// the dependency tree is not pulled into its own loom code paths; this module
// is compiled away from every normal build):
//
//   cargo test -p shamir-index --features loom --lib \
//       reader_drain_gate::loom_model -- --nocapture
//
// ## Honest scope — what this model does and does NOT prove
//
// This mirrors `shamir_engine::table::writer_drain_barrier`'s loom model (F-84,
// #912). The same limitation applies: loom 0.7.x's `Thread::seq_cst()` is a
// documented no-op ("the previous implementation ... was incorrect ... as a
// quick fix, just disable it ... may fail to model correct code, but will not
// silently allow bugs" — `loom::rt::thread`). Concretely: a plain
// `store(SeqCst)`/`load(SeqCst)` pair in this loom version behaves like
// `Release`/`Acquire`, NOT full SC — it can still exhibit the classic
// store-buffering (SB/Dekker) outcome that real SeqCst forbids. Only an
// explicit `loom::sync::atomic::fence(Ordering::SeqCst)` gets loom's real SC
// enforcement. The two `fence(SeqCst)` calls below exist SOLELY to compensate
// for this loom limitation — they have NO counterpart in the real (non-model)
// code, which uses plain SeqCst accesses throughout and needs no fence,
// because on a REAL C11/Rust implementation `store(SeqCst); load(SeqCst)` already
// forbids SB. Do not read the fences as implying production needs them; they
// are a modeling-only device.
//
// Because of this, the fenced model still does NOT red/green any ordering
// relaxation itself (an SC fence forbids SB regardless of what ordering the
// surrounding accesses use, so the model would "pass" whether or not the
// accesses below were SeqCst, Acquire/Release, or even Relaxed) — that
// argument continues to rest entirely on the worked proof in the
// [`ReaderDrainGate`] doc comment above, NOT on this model.
//
// What this model IS (and always was) good for: guarding the protocol's
// INTERLEAVING/structural contract against future regressions — a broken
// `wait_for_drain` loop (e.g. one that exits while `in_flight != 0`), a missing
// guard decrement, or a reordering of `enter` AFTER the flag read. Any of
// those shows up here as a violation of the invariant "after `wait_for_drain()`
// returns, every in-flight reader has completed its read" — PROVIDED the
// assertion actually samples state at drain-return time, not after the caller
// separately joins the reader thread (see `read_at_drop_return`'s doc below
// for why a post-`join` read of `reader_read` is tautological and cannot
// witness any violation, including the SB outcome above, until this model
// fixed both the sampling point and added the fences needed to make loom
// actually enforce the ordering the real code relies on).
// ============================================================================
#[cfg(loom)]
#[path = "../../tests/model/reader_drain_loom.rs"]
mod loom_model;

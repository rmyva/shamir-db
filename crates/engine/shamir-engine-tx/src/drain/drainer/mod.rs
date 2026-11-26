//! Repo-level background drainer — generalized inflight-WAL recovery run
//! as a continuous loop instead of only once on open (D2 P1d-2a).
//!
//! ## Why this exists
//!
//! P1d-2 moves the expensive `history.transact` (the version-log DATA
//! write) OFF the commit ack-path into a background task. The §8 refinement
//! of `docs/dev-artifacts/perf/d2-p1d2-subplan.md` observes that this background work is
//! EXACTLY [`recover_inflight_v2`](crate::recovery::recover_inflight_v2)
//! prowled in a loop: the source of truth is the inflight tail of the WAL
//! (`wal.recover()` → `Vec<WalEntryV2>`, each carrying `commit_version` +
//! ops), and [`replay_v2_entry`](crate::recovery::replay_v2_entry)
//! already routes those ops per-table into history. So the drain step is a
//! generalization of the recovery body — no separate `SegQueue<DrainJob>`
//! (a third copy of the ops) is needed.
//!
//! ## What [`drain_step`](Drainer::drain_step) does, vs recovery
//!
//! `recover_inflight_v2` replays EVERY inflight entry unconditionally (on
//! open, everything visible must be made durable). `drain_step` replays
//! only the entries in the window `durable_watermark < commit_version <=
//! last_committed` (visibility), in ascending `commit_version` order, then:
//!   1. `replay_v2_entry(entry, repo)` → history (idempotent, last-write-wins)
//!   2. `gate.mark_durable(commit_version)` — advance the durable watermark
//!   3. A5 interner-hwm gate, then `wal.commit(txn_id)` — truncate the
//!      inflight marker ONLY when the interner delta is durably covered.
//!
//! Both recovery (cold) and the drainer (warm) converge to the same state.
//! The shared "replay V → history" core is `replay_v2_entry`; the A5
//! truncation gate is shared via
//! `interner_delta_safe_to_truncate`.
//!
//! ## Current flow (P1d-2b cutover — DONE)
//!
//! The drainer is the SOLE history writer on the warm path. The commit
//! ack-path writes only the overlay (visibility) + the WAL entry, then
//! calls [`offer`](Drainer::offer) + [`wake`](Drainer::wake) to hand
//! the entry to the background drain task. That task runs
//! [`drain_step`](Drainer::drain_step) in a loop:
//!   - **Phase A** — for each entry in the window
//!     `durable_watermark < commit_version <= last_committed` (ascending),
//!     apply its interner delta in memory (A4 keystone) and replay
//!     non-MVCC ops; data ops (Put/Delete) are accumulated per table.
//!   - **Phase B** — ONE `write_committed_batch_to_history` per touched
//!     table (coalesces E entries × T tables down to T history-transact
//!     calls).
//!   - **Phase C** — for each finalized entry: `gate.mark_durable(v)`,
//!     then the A5 interner-hwm gate
//!     (`interner_delta_safe_to_truncate`),
//!     then `wal.commit(txn_id)` to advance the truncation watermark
//!     ONLY when the interner delta is durably covered.
//!
//! Cold recovery ([`recover_inflight_v2`](crate::recovery::recover_inflight_v2))
//! and the warm drainer converge to the same state. The shared
//! "replay V → history" core is `replay_v2_entry`; the A5 truncation
//! gate is shared via `interner_delta_safe_to_truncate`.
//!
//! ## Truncation liveness bound (A5)
//!
//! `drain_step` does NOT force an interner persist itself — the A5 gate's
//! `persisted_high_water()` only advances via the per-table background
//! checkpoint (fires every `INTERNER_CHECKPOINT_INTERVAL`, default 64,
//! commits — see `shamir-tunables`) or a graceful shutdown
//! (`RepoInstance::flush_buffers`, which persists every table's interner
//! synchronously). So a table that references a new interned field id and
//! then commits FEWER than `INTERNER_CHECKPOINT_INTERVAL` more times before
//! going idle forever has its covering WAL segments held un-truncatable
//! until either the checkpoint eventually fires or the process next
//! shuts down gracefully — a bounded, self-healing gap, not unconditional
//! growth. In practice this window is negligible: at the default
//! `WAL_SEGMENT_MAX_BYTES` (8 MiB), a segment holds tens of thousands of
//! records before sealing, so any table with real traffic clears the
//! 64-commit checkpoint interval long before a single segment could even
//! seal. (Previously this doc claimed `drain_step` runs an unconditional
//! forced persist as a "Phase B" step mirroring cold recovery — that never
//! shipped; this section corrects the record. Found during task #572's
//! review of task #571's crash_recovery.rs fix, where a test using a tiny
//! non-default segment cap hit exactly this window.)

use shamir_wal::WalEntryV2;

mod state;

pub mod orphan_repair;

pub use state::Drainer;

#[cfg(debug_assertions)]
#[doc(hidden)]
pub use state::drain_step::FAIL_INDEX_BATCH_TABLE_ID;

use std::sync::Arc;

use shamir_storage::types::RecordKey;
use shamir_tx::{CellReservationGuard, IsolationLevel, TxContext};
use shamir_wal::WalEntryV2;

use crate::commit::core::RepoCore;
use crate::commit::TxError;

// ── F-48b (#867) test-only pause seam ──────────────────────────────────────
//
// Mirrors F-46's `TEST_POST_VALIDATE_PRE_PUBLISH_HOOK` (commit.rs, #857),
// F-47's `TEST_POST_GENCHECK_PRE_PUBLISH_HOOK` (fk_reverse_cache.rs, #858),
// and F-48's `TEST_POST_BARRIER_PRE_WRITE_HOOK` (table_manager_crud.rs, #859):
// a `#[cfg(test)]` `OnceLock<Arc<Hook>>` global, zero cost when unset. The
// seam fires in `pre_commit_prelock` strictly AFTER Phase 2.5's
// `needs_write_barrier()` check has run for every table in `tx.write_set`
// (so the test knows which path each table took) and BEFORE Phase 5c's
// materialize write. A test installs the hook, spawns a tx commit,
// busy-polls `reached`, then drives a schema-activation DDL's
// raise+drain+count-proof while the tx is parked, then `resume`s it —
// deterministically reproducing the check-then-act interleaving the
// 2026-07-28 review's P0-3 finding describes for the tx-commit path.

/// Test-only pause/resume handshake installed via
/// [`TEST_POST_PRELOCK_PRE_MATERIALIZE_HOOK`]. `reached` lets the harness
/// detect (via polling, no sleeps) that the tx commit under test has
/// actually parked at the seam; `resume` is the release signal; `armed`
/// makes the pause one-shot (CAS true→false) so only the FIRST committer
/// to reach the seam parks — every later arrival (including the harness's
/// own concurrent DDL-side writer, if it also commits) passes straight
/// through. Shape mirrors `commit.rs::PostValidatePrePublishHook` (F-46)
/// and `table_manager_crud.rs::PostBarrierPreWriteHook` (F-48) exactly.
#[cfg(any(test, feature = "test-util"))]
#[derive(Debug)]
pub struct PostPrelockPreMaterializeHook {
    pub reached: std::sync::atomic::AtomicUsize,
    pub resume: tokio::sync::Notify,
    pub armed: std::sync::atomic::AtomicBool,
}

/// Test-only pause seam. Fires in `pre_commit_prelock` immediately AFTER
/// Phase 2.5's `needs_write_barrier()` check loop (so the writer has
/// committed to the fast or slow path per table) and BEFORE the function
/// returns into the commit pipeline's later phases (Phase 5c materialize
/// being the actual data write). See
/// [`PostPrelockPreMaterializeHook`]'s doc for the exact rationale.
///
/// `nextest` runs each test in its own process, so this global cannot leak
/// across test files. Uninstalled (`None`, the default for every other
/// test) this is a single `OnceLock::get()` read with no lock, no
/// allocation, no await point taken.
#[cfg(any(test, feature = "test-util"))]
pub static TEST_POST_PRELOCK_PRE_MATERIALIZE_HOOK: std::sync::OnceLock<
    std::sync::Arc<PostPrelockPreMaterializeHook>,
> = std::sync::OnceLock::new();

/// Parks on `TEST_POST_PRELOCK_PRE_MATERIALIZE_HOOK` if a test installed
/// one; a true no-op otherwise.
pub(super) async fn fire_post_prelock_pre_materialize_test_hook() {
    #[cfg(any(test, feature = "test-util"))]
    if let Some(hook) = TEST_POST_PRELOCK_PRE_MATERIALIZE_HOOK.get() {
        hook.reached
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        // One-shot: only the FIRST committer to reach this seam actually
        // parks (CAS true→false). Every later arrival passes straight
        // through — see `PostPrelockPreMaterializeHook::armed`'s doc for
        // why this is load-bearing (a concurrent committer / the harness's
        // own DDL-side writer must not deadlock here).
        let should_pause = hook
            .armed
            .compare_exchange(
                true,
                false,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
            )
            .is_ok();
        if should_pause {
            hook.resume.notified().await;
        }
    }
}

/// Stage I: the constant first `u64` of every `WalEntryV2.interner_delta`
/// triple. Pre-Stage-I this slot carried the per-table `table_token` so
/// recovery could route each delta to the right per-table interner. The
/// interner is now per-REPO (one id-namespace shared across tables), so the
/// slot is repurposed to a constant scope marker — recovery resolves the
/// single repo interner directly and ignores this value. The WAL wire-shape
/// (`Vec<(u64, String, u64)>`) is UNCHANGED; no version bump. The constant
/// `0` is chosen so a future reader can distinguish "repo scope" from any
/// nonzero per-table token if a hybrid scheme ever returns.
pub const REPO_INTERNER_SCOPE: u64 = 0;

/// Return the expected scope if the supplied WAL interner scope is not repository-wide.
pub fn unexpected_repo_interner_scope(scope: u64) -> Option<u64> {
    (scope != REPO_INTERNER_SCOPE).then_some(REPO_INTERNER_SCOPE)
}

/// SSI fix S2 — atomically CLAIM every data write-set key of `tx` on its
/// per-table `MvccStore` cell, BEFORE the version is assigned and the WAL
/// entry is written (Phase 4). This moves the "who won the write-write race"
/// decision OUT of the post-WAL publish (Phase 5a) and INTO an atomic
/// pre-WAL claim, so a loser aborts with `SsiConflict` having never touched
/// the WAL (invariant I-PreWAL).
///
/// Scope: **`Serializable` only**. Snapshot is documented last-writer-wins
/// (no read-set validation, no first-committer-wins), and Pessimistic
/// already serializes write-write via per-key Level-3 locks — claiming on
/// either would change documented semantics, so this returns an empty guard
/// vector (zero overhead) off Serializable, mirroring `build_footprint_from_tx`
/// and the read-set validation block, both of which are also Serializable-only.
///
/// One [`CellReservationGuard`] per touched table (each guard owns ONE
/// `Arc<MvccStore>` — the abort-path release target). A won key is `add`ed to
/// its table's guard IMMEDIATELY, so a conflict on a LATER key returns `Err`
/// and the guards (dropped on that `?`) release every already-won key — no
/// partial claim survives an abort.
///
/// `try_reserve` NEVER blocks (no-wait, invariant I-NoWait): a contended cell
/// returns `false` → we abort with `SsiConflict` immediately, so multi-key
/// claim in any order is deadlock-free.
///
/// Returns the guards on success (the caller disarms them once the publisher
/// has finalized every claim), or `Err(SsiConflict)` on the first contended /
/// stale cell (the partial guards drop here → release).
pub(super) async fn claim_write_set(
    tx: &TxContext,
    repo: &RepoCore,
) -> Result<Vec<CellReservationGuard>, TxError> {
    // Off Serializable: no claim (Snapshot last-writer-wins / Pessimistic
    // lock-serialized). Empty vec — zero allocation beyond the Vec header.
    if tx.isolation != IsolationLevel::Serializable {
        return Ok(Vec::new());
    }

    let txn_id = tx.tx_id.0;
    let snapshot = tx.snapshot_version;
    let mvcc_map = repo.per_table_mvcc();

    let mut guards: Vec<CellReservationGuard> = Vec::with_capacity(tx.write_set.len());
    for (table_id, staging) in &tx.write_set {
        if staging.is_empty() {
            continue;
        }
        // Per-table MvccStore: cells live per-table, and the publish-side
        // `finalize_reservation` (apply_committed_visible) uses the SAME store,
        // so claim and finalize meet on the same cell. A table absent from
        // `per_table_mvcc` (system / unattached table) has no cell to claim —
        // it also has no overlay/cell finalize, so it is correctly skipped.
        //
        // DEADLOCK FIX (same class as #589 / `cells` map commit `7a4abf62`,
        // H1+H2 commit `621776bd`): `read_sync`, NOT `read_async`. The
        // `per_table_mvcc` map is also touched SYNCHRONOUSLY — `read_sync`
        // (version_provider.rs, EVERY Serializable `validate_read_set` commit),
        // `get_sync` (commit.rs pessimistic-lock release; rename-table) and
        // `iter_sync` (flush_all_history; drainer F6a overlay GC) — and
        // EXCLUSIVELY by `insert_sync` (table attach) / `remove_sync` (drop
        // table). `read_async`'s wait is lock-HANDOFF: saa grants the shared
        // bucket lock to the suspended reader TASK, which then holds it while
        // unpolled in tokio's run queue. A DDL exclusive writer (attach/drop)
        // racing sustained commit/drain traffic can park every worker in
        // `read_sync`/`get_sync`/`insert_sync` behind that unpolled reader →
        // whole-runtime deadlock. `read_sync`'s bucket lock is held only by a
        // RUNNING thread for a few instructions (an `Arc::clone`), bounding
        // every wait. The fn stays `async`; this call no longer suspends.
        let Some(store) = mvcc_map.read_sync(table_id, |_, mvcc| std::sync::Arc::clone(mvcc))
        else {
            continue;
        };
        let mut guard = CellReservationGuard::new(store.clone(), txn_id);
        for key in staging.keys() {
            // task #532: `try_reserve` / the guard / the cell registry are all
            // `RecordKey`-keyed now — pass the staged `RecordKey` straight
            // through, no `Bytes` round-trip on the hot claim path.
            let key: RecordKey = key.clone();
            if store.try_reserve(key.clone(), snapshot, txn_id) {
                // Won — register immediately so an abort on a later key (this
                // table or a subsequent one) releases this claim on drop.
                guard.add(key);
            } else {
                // Contended or stale cell → this committer LOST the race.
                // Returning drops `guard` (releasing this table's won keys) and
                // every earlier table's guard in `guards`, then the tx aborts
                // BEFORE Phase 4 — no WAL is written for a loser. The
                // `SsiConflict` error carries `Bytes`; convert once here on the
                // cold abort path (a necessary boundary conversion).
                repo.tx_metrics().on_tx_aborted_ssi();
                return Err(TxError::SsiConflict { key: key.into() });
            }
        }
        guards.push(guard);
    }
    Ok(guards)
}

/// Outcome of `pre_commit`: the assigned MVCC commit version plus the
/// per-table `unique_write_lock` guards that must stay held through
/// Phase 5c (released inside `materialize`).
pub(in crate::tx) struct PreCommit {
    pub(in crate::tx) commit_version: u64,
    pub(in crate::tx) uwl_guards: Vec<tokio::sync::OwnedMutexGuard<()>>,
    /// F-48b (#867): kept-alive writer-drain guards (one per fast-path table
    /// in `tx.write_set` whose `needs_write_barrier()` read `false` in Phase
    /// 2.5). Threaded alongside `uwl_guards` through Phase 5c, dropped inside
    /// `materialize` after the data/index writes have landed.
    pub(in crate::tx) drain_guards: Vec<crate::table::writer_drain_barrier::WriterDrainGuard>,
    /// RAII owner of the version's terminal-mark obligation (P0a). Survives
    /// to the caller's success path (consumed via `materialize` →
    /// `guard.commit()` → Materialized). WAL begin already succeeded by the
    /// time this is returned, so the only remaining terminal state is
    /// Materialized — but a panic before `materialize` still drops the guard
    /// → Aborted, closing hole H1.
    pub(in crate::tx) version_guard: shamir_tx::VersionGuard,
    /// SSI fix S2 — RAII owners of this committer's pre-WAL cell-reservations
    /// (one guard per touched table). WAL begin has already succeeded by the
    /// time this is returned, so on the success path the caller `disarm`s these
    /// AFTER the publisher finalizes every claim; any panic before that drops
    /// them → release. Empty off Serializable.
    pub(in crate::tx) cell_guards: Vec<CellReservationGuard>,
    /// Op #2 Stage 2: the WAL entry that was just persisted via
    /// `begin_grouped`, wrapped in `Arc` so the caller can `offer` it to
    /// the drainer window without cloning the payload again.
    pub(in crate::tx) wal_entry_arc: Arc<WalEntryV2>,
    pub(in crate::tx) durability_failed: bool,
}

/// Outcome of [`pre_commit_prelock`](super::prelock::pre_commit_prelock): per-table uwl_guards acquired in
/// sorted token order OUTSIDE the commit_lock. These are passed into
/// [`pre_commit_locked`](super::locked::pre_commit_locked) and then through to `materialize`.
///
/// F-48b (#867): `drain_guards` carries one `WriterDrainGuard` per table in
/// `tx.write_set` whose Phase 2.5 `needs_write_barrier()` read `false` (the
/// fast path). These stay alive through Phase 5c materialize — the exact
/// window a schema-activation / index2-create DDL raising the barrier AFTER
/// the flag read must drain before stamping its proof. Tables that read
/// `true` (slow path) contribute to `uwl_guards` instead and are NOT in the
/// drain set (the lock serializes them — staying in the drain set while
/// blocking on the lock would deadlock).
pub(in crate::tx) struct PreLockResult {
    pub(in crate::tx) uwl_guards: Vec<tokio::sync::OwnedMutexGuard<()>>,
    pub(in crate::tx) drain_guards: Vec<crate::table::writer_drain_barrier::WriterDrainGuard>,
}

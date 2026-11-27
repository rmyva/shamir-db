#[cfg(any(test, feature = "test-util"))]
use std::sync::atomic::Ordering::SeqCst;

use shamir_collections::TFxMap;
use shamir_collections::THasher;
use shamir_storage::error::DbError;
use shamir_tunables::instance_defaults::INTERNER_CHECKPOINT_INTERVAL;
use shamir_tx::{IndexWriteOp, TxContext};

use super::materialize::{apply_vector_delta_batch, apply_vector_graph_batch};
use crate::commit::core::RepoCore;
use crate::meta::recovery_marker::{
    load_last_committed, load_next_tx_id_snapshot, save_last_committed, save_next_tx_id_snapshot,
};
use crate::tx_outcome::MaterializationState;

/// Bounded inline retry budget for a post-commit materialization
/// sub-phase. A few attempts absorb transient storage hiccups; a
/// persistent failure falls through to deferral (recovery re-applies the
/// WAL entry). Transient-vs-persistent is NOT perfectly classified —
/// idempotent re-application makes that unnecessary.
pub const MATERIALIZE_ATTEMPTS: u32 = 3;

#[cfg(any(test, feature = "test-util"))]
#[doc(hidden)]
#[path = "test_seams.rs"]
pub mod test_seams;
#[cfg(any(test, feature = "test-util"))]
#[doc(hidden)]
pub use test_seams::*;

/// Parks on `TEST_INDEX_BATCH_BUMP_APPLY_HOOK` if a test installed one; a
/// true no-op otherwise. Fires strictly between the bump and apply calls in
/// [`apply_index_batch`], regardless of which of the two orderings
/// (`TEST_LEGACY_APPLY_THEN_BUMP_ORDER`) is active — i.e. always at the exact
/// program point between "epoch bump" and "posting apply", whichever one ran
/// first.
async fn fire_index_batch_bump_apply_test_hook() {
    #[cfg(any(test, feature = "test-util"))]
    if let Some(hook) = TEST_INDEX_BATCH_BUMP_APPLY_HOOK.get() {
        hook.reached
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
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

/// Async-mode helper: run Phase 5a (data) inline on the client path.
///
/// Mirrors the per-table loop in `crate::commit::materialize` (drain → bounded retry →
/// log warn on persistent failure) but writes outcomes into a shared
/// `Arc<AtomicBool>` so the background tail can flip its final state to
/// `Deferred` if a data write actually failed. The DATA write must finish
/// before ack so read-your-own-writes on data holds.
pub(crate) async fn apply_data_phase(tx: &mut TxContext, repo: &RepoCore, commit_version: u64) {
    let tx_id = tx.tx_id.0;
    let data_batches = collect_data_batches(tx);
    for (table_id, base, ops) in data_batches {
        if let Err(e) = retry_materialize(MATERIALIZE_ATTEMPTS, || {
            apply_data_batch(repo, table_id, base.clone(), &ops, commit_version, tx_id)
        })
        .await
        {
            // A data-apply failure on the ack path is logged here and
            // RE-SURFACED via the inflight WAL marker — the tail will see
            // an empty staging (data already drained) but the recovery
            // contract still holds: the marker is left inflight, recovery
            // replays the WAL entry. Mark the tx as having an async-prefix
            // failure so the background tail forces `Deferred`.
            log::warn!(
                "commit_tx (async) Phase 5a (data) failed for tx {tx_id} \
                 commit_version {commit_version} table {table_id}: {e}; deferring to recovery"
            );
            tx.async_prefix_failed = true;
        }
    }
}

/// VR-4 (#426) Phase 5d durability — variant A: append the durable
/// delta-log chunk(s) for this tx's staged vectors + deletes BEFORE
/// `version_guard.commit()` (pre-publish), so `restore_on_open`'s
/// `replay_delta` re-materializes the vectors over the base snapshot on
/// the next open. Closes the W-2 window where a post-ack crash could
/// leave a committed vector WITHOUT a durable delta echo (the live graph
/// is RAM-only and dies with the process; the delta-log was the only
/// durable bridge, and before #426 it was appended POST-publish).
///
/// This runs the DELTA half only. The GRAPH half
/// (`apply_staged_vector_deletes` + `apply_staged_vectors`, which mutate
/// the in-RAM HNSW graph) stays in `promote_vectors` AFTER the lock — it
/// is a derived read-accelerator (III.5) and its unbounded per-vector work
/// must not stall other committers under `commit_lock`.
///
/// Failure semantics (§5.3 of the design): a persistent delta-append
/// failure flips the tx to `Deferred`. The WAL marker is left inflight so
/// recovery re-applies the data; BUT recovery does NOT replay the vector
/// mutation (vectors are not serialised in the WAL — they are derived).
/// So a delta failure is still a loss of the vector mutation from the
/// live graph — BUT now it is DETECTABLE pre-ack (`Deferred` +
/// warn-log), unlike the prior silent post-ack loss. The client can retry
/// the tx. Aborting before publish is impossible (Phase 4 already made
/// the data durable); the `Deferred` signal is the honest surface.
///
/// Idempotency: `replay_delta` applies `DeltaOp::Upsert`/`Delete` via
/// `adapter.upsert/delete` (last-write-wins), so a duplicate chunk on a
/// re-attempt after restart converges. `next_delta_idx.fetch_add` in
/// `append_vector_delta` may produce a duplicate chunk index on retry —
/// `Store::set` overwrites the same chunk key (last-writer-wins).
pub(crate) async fn apply_vector_delta_phase(
    tx: &mut TxContext,
    repo: &RepoCore,
    _commit_version: u64,
) {
    let tx_id = tx.tx_id.0;

    // P0 index promote order (round-2 finding 2): reservations must follow
    // the SAME global total order (ascending table token) that
    // `promote_vectors` uses for promotion, so every tx reserves its
    // (token, backend descriptor id) pairs in one deterministic order. The
    // insert tokens and the delete-only tokens are therefore collected into
    // ONE sorted Vec and visited in that order: a token with staged inserts
    // takes the insert path (its deletes ride the same chunk — gap#1), a
    // token with ONLY deletes takes the delete-only path. Unsorted map
    // iteration would let two txs reserve the same two tables in opposite
    // orders, breaking the deadlock-freedom induction in
    // `apply_vector_graph_batch`.
    let mut tokens: Vec<u64> = tx
        .staged_vectors
        .iter()
        .filter(|(_, v)| !v.is_empty())
        .map(|(t, _)| *t)
        .collect();
    for (t, dels) in tx.staged_vector_deletes.iter() {
        // gap#1: delete-only tables (no staged inserts) still need their
        // tombstones echoed durably. Tokens that also appear in
        // `staged_vectors` are handled by the single insert-path call below.
        if !dels.is_empty() && !tx.staged_vectors.contains_key(t) {
            tokens.push(*t);
        }
    }
    tokens.sort_unstable();
    tokens.dedup();
    for token in tokens {
        let has_inserts = tx.staged_vectors.get(&token).is_some_and(|v| !v.is_empty());
        // This table's staged vector deletes ride the SAME delta chunk as
        // the inserts (gap#1: a replace = delete old + insert new on the
        // same rid). `append_vector_delta` writes both `DeltaOp::Upsert`
        // and `DeltaOp::Delete` ops in one chunk so a restart replays
        // tombstone-then-upsert over the base snapshot.
        let deleted: Vec<shamir_types::types::record_id::RecordId> = tx
            .staged_vector_deletes_for(token)
            .map(|s| s.to_vec())
            .unwrap_or_default();
        let result = if has_inserts {
            retry_materialize(MATERIALIZE_ATTEMPTS, || {
                let vecs = tx.staged_vectors_for(token).unwrap_or(&[]);
                apply_vector_delta_batch(repo, token, vecs, &deleted, tx_id)
            })
            .await
        } else {
            // Delete-only table: empty vecs, tombstones only.
            retry_materialize(MATERIALIZE_ATTEMPTS, || {
                apply_vector_delta_batch(repo, token, &[], &deleted, tx_id)
            })
            .await
        };
        match result {
            // P0 delta-prune: park the per-backend chunk acknowledgements on
            // the tx; promote_vectors calls them only after the graph half
            // succeeded, which is what advances the applied prune boundary.
            Ok((acks, promote_tickets)) => {
                let _ = tx.vector_delta_acks.insert_sync(token, acks);
                // P0 index promote order: park the turnstile tickets on the
                // tx; their Drop releases the turn on EVERY outcome.
                let _ = tx
                    .vector_promote_tickets
                    .insert_sync(token, promote_tickets);
            }
            Err(e) => {
                log::warn!(
                    "commit_tx Phase 5d (delta-append, pre-publish) failed for tx {tx_id} \
                 commit_version {_commit_version} table {token}: {e}; tx will be reported \
                 Deferred — the vector mutation has no durable delta echo and recovery will \
                 NOT re-apply it (vectors are derived, not WAL-replayed); client should retry"
                );
                tx.async_prefix_failed = true;
            }
        }
    }
}

/// Async-mode helper: run the materialization TAIL on the background task.
///
/// Phases (in order): 5c (index) → drop `uwl_guards` → 6.5 (markers) →
/// 7 (`wal.commit`). NEVER aborts: a sub-phase failure flips the returned
/// state to `Deferred` and leaves the WAL marker inflight, exactly like the
/// sync deferral path. `recover_v2_inflight` is the eventual reconciler.
pub(crate) async fn materialize_async_tail(
    tx: &mut TxContext,
    repo: &RepoCore,
    commit_version: u64,
    uwl_guards: Vec<tokio::sync::OwnedMutexGuard<()>>,
    drain_guards: Vec<crate::table::writer_drain_barrier::WriterDrainGuard>,
) -> MaterializationState {
    let tx_id = tx.tx_id.0;
    let mut ok = !tx.async_prefix_failed;

    // Phase 5c: index postings.
    if !tx.index_write_set.is_empty() {
        let mut by_token: TFxMap<u64, Vec<IndexWriteOp>> =
            TFxMap::with_capacity_and_hasher(tx.index_write_set.len(), THasher::default());
        for (token, op) in std::mem::take(&mut tx.index_write_set) {
            by_token.entry(token).or_default().push(op);
        }
        for (token, ops) in by_token {
            if let Err(e) = retry_materialize(MATERIALIZE_ATTEMPTS, || {
                apply_index_batch(repo, token, &ops, tx_id, commit_version)
            })
            .await
            {
                log::warn!(
                    "commit_tx (async) Phase 5c (index) failed for tx {tx_id} \
                     commit_version {commit_version} table {token}: {e}; deferring to recovery"
                );
                ok = false;
            }
        }
    }

    // Release per-table unique_write_lock guards (HIGH-A): the Phase 2.6
    // guard window has closed (postings either published or deferred to
    // recovery; recovery is idempotent). Released even on a deferred 5c —
    // a stuck guard would only block non-tx writers.
    //
    // F-48b (#867): drop the kept-alive writer-drain guards at the SAME
    // point — Phase 5c has landed (or deferred to recovery), so the tx's
    // writes are observable. See `materialize`'s matching drop for the
    // memory-model rationale.
    drop(uwl_guards);
    drop(drain_guards);

    // Phase 6.5: persist recovery markers.
    match repo.tx_gate().await {
        Ok(gate) => {
            if let Err(e) = persist_markers(repo, gate.as_ref(), commit_version).await {
                log::warn!(
                    "commit_tx (async) Phase 6.5 (recovery markers) failed for tx {tx_id} \
                     commit_version {commit_version}: {e}; deferring to recovery"
                );
                ok = false;
            }
        }
        Err(e) => {
            log::warn!(
                "commit_tx (async) Phase 6.5 (recovery markers) could not acquire tx_gate for \
                 tx {tx_id} commit_version {commit_version}: {e}; deferring to recovery"
            );
            ok = false;
        }
    }

    // A5 + Stage I: capture the max interner id across the tx's per-repo
    // delta for the interner checkpoint below. Pre-Stage-I this was a
    // per-table `Vec<(token, max_id)>`; the interner is now per-repo so it
    // collapses to ONE max id (or `None` if the delta was empty).
    let interner_delta_max_id: Option<u64> = tx.interner_deltas.iter().map(|(_, id)| *id).max();

    // A5 + Stage I: background interner checkpoint on interval. One persist
    // covers the whole repo (the manager is Arc-shared across tables).
    if commit_version.is_multiple_of(INTERNER_CHECKPOINT_INTERVAL)
        && interner_delta_max_id.is_some()
    {
        let repo_ck = repo.clone();
        tokio::spawn(async move {
            match repo_ck.repo_interner().await {
                Ok(repo_interner) => {
                    if let Err(e) = repo_interner.persist().await {
                        log::warn!(
                            "A5 interner checkpoint (async) failed for repo {}: {e}",
                            repo_ck.name()
                        );
                    }
                }
                Err(e) => {
                    log::warn!(
                        "A5 interner checkpoint (async): repo_interner resolve for {}: {e}",
                        repo_ck.name()
                    );
                }
            }
        });
    }

    // D2 P1d-2b FIX: Phase 7 (WAL truncation) is NO LONGER done here. Post-
    // cutover the AsyncIndex ack-path writes ONLY the in-memory overlay (via
    // `apply_data_batch` → `apply_committed_visible`) — the value is durable in
    // `history` only after the background `Drainer` replays the inflight WAL
    // entry. Truncating the marker here would remove the WAL entry while the
    // data lives ONLY in the (volatile) overlay → DATA LOSS on crash, and would
    // wedge `durable_watermark` (the drainer could never mark this version
    // durable because its entry is gone). The drainer is the SOLE caller of
    // `wal.commit` now: it writes history → `mark_durable` → A5-gated truncate,
    // exactly as the lockfree path's `post_publish_cleanup` was changed to.
    if !ok {
        log::warn!(
            "commit_tx (async) tx {tx_id} commit_version {commit_version} COMMITTED but \
             materialization DEFERRED — recovery / drainer will reconcile main/info"
        );
    }

    if ok {
        MaterializationState::Complete
    } else {
        MaterializationState::Deferred
    }
}

/// cancel-safe: NO, and it does NOT need to be — by the time this runs the
/// tx is fully COMMITTED *and* materialized (Phases 5a/5c published the
/// data + index under `commit_lock`, Phase 7 removed the WAL marker). This
/// promotes the tx's staged HNSW vectors (`tx.staged_vectors`) into the
/// live graph OUTSIDE the commit critical section (III.5).
///
/// Why this runs after the lock is dropped:
///   The HNSW graph is a DERIVED read-accelerator, not a source of truth.
///   The vectors themselves are already durable in `main` (Phase 5a + the
///   Phase 4 WAL entry) AND their durable delta chunk was appended
///   PRE-publish by `apply_vector_delta_phase` (#426, VR-4 variant A); the
///   graph is re-derived from snapshot + delta by
///   `VectorBackend::restore_on_open` (`index2/vector/vector_backend.rs`)
///   on open. So the promote determines no visibility a committer must
///   serialise on, and — for bulk vector commits — its unbounded work (the
///   brute-force adapter's bounded-actor `submit().await`, the HNSW
///   adapter's per-vector `spawn_blocking`) must not run under
///   `commit_lock`, where it would stall every other committer (audit MED).
///
/// Why a failure here is NOT `MaterializationState::Deferred`:
///   The Deferred contract is "a visibility-bearing projection didn't land
///   inline, so the inflight WAL marker is the recovery guarantor." But
///   HNSW is NOT replayed from the WAL — vectors are not serialised as
///   `IndexPut`; they are derived. Phase 7 has ALREADY removed the marker
///   (data + index committed and materialized under the lock), so there is
///   no marker to lean on and nothing for recovery to replay for the
///   graph. A failed post-lock GRAPH promote simply means the in-memory
///   graph lags the data — but the DURABLE delta chunk was already
///   appended PRE-publish by `apply_vector_delta_phase` (#426, VR-4 variant
///   A), so `restore_on_open::replay_delta` applies it on the next open.
///   We therefore log a warning and leave `materialization` untouched
///   (`Complete`). The delta chunk is the durable bridge; the graph is
///   re-derived from snapshot + delta on open.
pub(crate) async fn promote_vectors(tx: &TxContext, repo: &RepoCore, commit_version: u64) {
    let tx_id = tx.tx_id.0;

    // We iterate exactly the tables the tx staged vectors into
    // (`tx.staged_vectors`), keyed by table token. `apply_staged_vectors`
    // is a no-op for every backend except `VectorBackend`. Phase 5a only
    // drained `tx.write_set`, so `tx.staged_vectors` is intact here.
    //
    // P2 perf fix (audit 4.26): borrow the embedding payloads straight out
    // of `tx.staged_vectors` instead of deep-cloning them a second time
    // (they were already cloned once in `apply_vector_delta_phase` — see
    // its own matching fix). `tx` is a plain shared reference here and
    // `staged_vectors` is never mutated by this function, so there is no
    // borrow-checker conflict to work around, unlike the delta-phase side.
    //
    // P0 index promote order (round-2 finding 2): the promotion pass must
    // follow the SAME global total order (ascending table token) that
    // `apply_vector_delta_phase` used for its reservations. The insert
    // tokens and the delete-only tokens are therefore merged into ONE
    // sorted Vec — running all inserts before all delete-onlys (or vice
    // versa) would let a tx promote table Y (insert) while still holding an
    // earlier RESERVATION on table X (delete-only) that another tx
    // processes before Y, breaking the identical-order premise of the
    // induction in `apply_vector_graph_batch`.
    let mut vector_tokens: Vec<u64> = tx
        .staged_vectors
        .iter()
        .filter(|(_, v)| !v.is_empty())
        .map(|(t, _)| *t)
        .collect();
    for (t, dels) in tx.staged_vector_deletes.iter() {
        if !dels.is_empty() && !tx.staged_vectors.contains_key(t) {
            vector_tokens.push(*t);
        }
    }
    vector_tokens.sort_unstable();
    vector_tokens.dedup();
    for token in vector_tokens {
        let has_inserts = tx.staged_vectors.get(&token).is_some_and(|v| !v.is_empty());
        // gap#1: gather this table's staged vector deletes so they ride
        // the SAME commit-ack promote pass as the inserts. A replace
        // (delete old + insert new on the same rid) lands BOTH here: the
        // old rid in `deleted` and the new embedding in `vecs`. Order is
        // apply-deletes-then-inserts inside `apply_vector_graph_batch`,
        // which is the inverse-direction-safe order for HNSW (a tombstone
        // followed by an upsert on the same rid leaves the NEW vector
        // live — `upsert` re-inserts the node; `delete` first is a no-op
        // on a not-yet-inserted node, so either order converges, but
        // delete-then-insert matches the replace intent literally). The
        // durable delta chunk (with BOTH ops) was already appended
        // PRE-publish by `apply_vector_delta_phase`.
        let deleted: Vec<shamir_types::types::record_id::RecordId> = tx
            .staged_vector_deletes_for(token)
            .map(|s| s.to_vec())
            .unwrap_or_default();
        let vecs = tx.staged_vectors_for(token).unwrap_or(&[]);
        // P0 index promote order: take the tickets BEFORE promoting — they
        // must drop (releasing the turn) on every outcome, including a
        // failed or cancelled promote. A failed promote never acks below.
        let tickets = tx
            .vector_promote_tickets
            .remove_sync(&token)
            .map(|(_, t)| t)
            .unwrap_or_default();
        let res = if has_inserts {
            retry_materialize(MATERIALIZE_ATTEMPTS, || {
                apply_vector_graph_batch(repo, token, vecs, &deleted, tx_id, &tickets)
            })
            .await
        } else {
            // gap#1 delete-only table: empty vecs, tombstones only.
            retry_materialize(MATERIALIZE_ATTEMPTS, || {
                apply_vector_graph_batch(repo, token, &[], &deleted, tx_id, &tickets)
            })
            .await
        };
        if res.is_ok() {
            // P0 delta-prune: the chunk's mutations are in the live graph;
            // acknowledge them so the backend's applied prefix advances and
            // the next snapshot may prune the chunk. A failed promote never
            // reaches this: its chunk stays replayable (never pruned).
            if let Some((_, acks)) = tx.vector_delta_acks.remove_sync(&token) {
                for ack in acks {
                    ack();
                }
            }
        } else {
            // NOT Deferred: data + index already committed and materialized
            // under the lock; the WAL marker is gone (Phase 7). The DURABLE
            // delta chunk was appended PRE-publish by
            // `apply_vector_delta_phase`, so `restore_on_open::replay_delta`
            // re-materializes the vectors over the base snapshot on the next
            // open. The live in-RAM graph lags until then — derived, not
            // WAL-replayed.
            log::warn!(
                "commit_tx Phase 5d (hnsw graph promote, post-lock) failed for tx {tx_id} \
                 commit_version {commit_version} table {token}; the live graph \
                 lags until restore_on_open replays the pre-publish durable delta \
                 (tx stays COMMITTED + materialized — NOT deferred)"
            );
        }
    }
}

/// Drain each table's `StagingStore` into an owned `(token, base, ops)`
/// batch. Done once up front so a bounded retry re-applies the SAME ops
/// (idempotent at the data layer). Empty batches are dropped.
#[allow(clippy::type_complexity)]
pub(crate) fn collect_data_batches(
    tx: &mut TxContext,
) -> Vec<(
    u64,
    std::sync::Arc<dyn shamir_storage::types::Store>,
    Vec<shamir_storage::types::KvOp>,
)> {
    let mut out = Vec::with_capacity(tx.write_set.len());
    for (table_id, staging) in std::mem::take(&mut tx.write_set) {
        let base = staging.base().clone();
        let ops = staging.drain();
        if ops.is_empty() {
            continue;
        }
        out.push((table_id, base, ops));
    }
    out
}

/// Apply one table's data ops (Phase 5a). Routes through MvccStore when
/// the table is registered in `per_table_mvcc`, else `base.transact`.
///
/// P2 perf fix (audit 4.7): takes `&[KvOp]`, not an owned `Vec`. The MVCC
/// arm (`apply_committed_visible`) only ever reads the slice, so the
/// common (attached-table) path needs zero clone at all — callers used to
/// pay a full deep-copy of the per-table op batch (record bodies
/// included) on EVERY commit, purely to satisfy this signature, when the
/// clone was almost never actually needed. Only the rare non-MVCC arm
/// (`base.transact`) needs ownership, since `Store::transact` takes
/// `Vec<KvOp>` by its trait signature — that clone is confined to that
/// cold path and paid only there.
pub(crate) async fn apply_data_batch(
    repo: &RepoCore,
    table_id: u64,
    base: std::sync::Arc<dyn shamir_storage::types::Store>,
    ops: &[shamir_storage::types::KvOp],
    commit_version: u64,
    _tx_id: u64,
) -> Result<(), DbError> {
    // Test-only failure injection (audit MED): simulate a persistent Phase
    // 5a storage error for a SPECIFIC `(table_token, tx_id)` so a tx writing
    // several tables can have exactly its second table's data write fail —
    // the partial cross-table materialization the audit flagged. Both
    // registers must match; armed via `FAIL_PHASE_5A_TX_ID` +
    // `FAIL_PHASE_5A_TABLE_TOKEN`.
    #[cfg(any(test, feature = "test-util"))]
    {
        let armed_tx = FAIL_PHASE_5A_TX_ID.load(SeqCst);
        let armed_token = FAIL_PHASE_5A_TABLE_TOKEN.load(SeqCst);
        if _tx_id != 0 && armed_tx == _tx_id && armed_token == table_id {
            return Err(DbError::Internal(format!(
                "injected Phase 5a (data) failure for tx {_tx_id} table {table_id}"
            )));
        }
    }

    // Test-only failure injection (P2 perf fix regression, audit 4.7): fail
    // exactly `FAIL_PHASE_5A_TRANSIENT_REMAINING` attempts for the matching
    // tx, then let the call through for real — proves `retry_materialize`
    // recovers (not just defers) with the borrowed `&[KvOp]` signature.
    #[cfg(any(test, feature = "test-util"))]
    {
        if _tx_id != 0 && FAIL_PHASE_5A_TRANSIENT_TX_ID.load(SeqCst) == _tx_id {
            let decremented = FAIL_PHASE_5A_TRANSIENT_REMAINING.fetch_update(SeqCst, SeqCst, |r| {
                if r > 0 {
                    Some(r - 1)
                } else {
                    None
                }
            });
            if decremented.is_ok() {
                return Err(DbError::Internal(format!(
                    "injected TRANSIENT Phase 5a (data) failure for tx {_tx_id} table {table_id}"
                )));
            }
        }
    }

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
    let mvcc_found = repo
        .per_table_mvcc()
        .read_sync(&table_id, |_, mvcc| std::sync::Arc::clone(mvcc));
    match mvcc_found {
        // D2 P1d-2b CUTOVER: the ack-path writes ONLY the in-memory visible
        // half (overlay + cell + floor). It no longer writes `history` inline
        // — the background `Drainer` (replaying the durable WAL entry via
        // `write_committed_to_history`) is now the SOLE history writer. The WAL
        // entry (Phase 4, already durable) is the source of truth; the overlay
        // holds the single RAM copy of the value until the drainer makes it
        // durable. Crash before drain ⇒ inflight WAL ⇒ recovery replays.
        Some(mvcc) => {
            mvcc.apply_committed_visible(ops, commit_version);
            Ok(())
        }
        // Non-MVCC tables (system / test) have NO overlay and are NOT drained
        // — they have no version-log. Keep the direct durable write inline:
        // this is the rare unattached-table path, and its data is replayed
        // from the WAL on recovery exactly as before the cutover. `transact`
        // needs ownership (`Store::transact(Vec<KvOp>)`); the clone is paid
        // ONLY on this cold, unattached-table path.
        None => base.transact(ops.to_vec()).await,
    }
}

/// Apply one table's staged index ops (Phase 5c).
///
/// F-53b Step 2 (#878) / F-67 (#893) / F-74 (#901): advance the PER-INDEX
/// mutation high-water (`last_mutation_version(name_interned)`) to
/// `commit_version`, for exactly the sorted index(es) `ops` actually touched
/// (`SortedIndexManager::bump_touched_indexes` decodes `name_interned` from
/// each op's physical key) — BEFORE the postings themselves are applied to
/// `info_store`. This is the LOAD-BEARING wire point for the tx path: the
/// AsOf cursor seek gate compares this against a cursor's pinned version, for
/// the SAME index the cursor's seek is planned against.
///
/// F-74 (#901): bump-BEFORE-apply mirrors the non-tx direct path
/// (`SortedIndexManager::on_record_created`/`on_record_updated`/
/// `on_record_deleted`, `sorted_index_manager.rs`) and closes a real TOCTOU
/// window the previous apply-then-bump order left open: a concurrent AsOf
/// read's entry gate + post-scan re-check (`read_asof_seek.rs`) both compare
/// `last_mutation_version(index_name) <= pinned` directly against this
/// counter, with NO `.await` between a scan and its own post-check on that
/// same tokio task — but tokio is a multi-threaded work-stealing runtime by
/// default here, so a genuinely concurrent reader on ANOTHER OS thread can
/// run its own gate-check → scan → post-check sequence anywhere inside the
/// window between these two calls. Under apply-then-bump, that reader could
/// observe postings already mutated by this commit while the counter still
/// read as unbumped, pass both its entry gate and its post-check, and return
/// a silently short/wrong AsOf page. Under bump-then-apply that window
/// cannot produce a wrong answer: if the bump is visible to a concurrent
/// reader at all, so is the fact that this index changed, and its gate (entry
/// or post-scan) declines the fast path in favour of the always-correct full
/// scan. The only possible failure mode left is the SAFE one: the bump lands
/// first and the apply has not yet landed, in which case a concurrent reader
/// unnecessarily falls back to a full scan for a mutation that has not
/// actually taken effect yet — conservative, never wrong.
///
/// `ops` is the flat per-table `Vec<IndexWriteOp>` assembled by
/// `materialize.rs`/this module's caller from `tx.index_write_set` — a
/// `Vec<(table_token, IndexWriteOp)>` grouped only by table, never by
/// index, and shared across every index family (hash/sorted/FTS/functional).
/// `bump_touched_indexes` skips any op whose key does not decode as a
/// sorted-index physical key (`SORTED_TAG`-prefixed) — those belong to a
/// different index family and are harmless to skip here. Note this function
/// computes the touched-index set from `ops` BEFORE `ops` is applied — `ops`
/// is a plain `&[IndexWriteOp]` parameter, fully available at function entry,
/// so this reorder needs no re-fetch or re-derivation of `ops` itself, only
/// moving the existing `bump_touched_indexes` call ahead of the existing
/// `apply_index_ops_at_commit` call.
pub(crate) async fn apply_index_batch(
    repo: &RepoCore,
    token: u64,
    ops: &[IndexWriteOp],
    _tx_id: u64,
    commit_version: u64,
) -> Result<(), DbError> {
    // Test-only failure injection: simulate a persistent Phase 5c storage
    // error for a specific tx so a post-commit-point failure can be
    // exercised in-process (Vector I.3). Armed via `FAIL_PHASE_5C_TX_ID`.
    #[cfg(any(test, feature = "test-util"))]
    if _tx_id != 0 && FAIL_PHASE_5C_TX_ID.load(std::sync::atomic::Ordering::SeqCst) == _tx_id {
        return Err(DbError::Internal(format!(
            "injected Phase 5c failure for tx {_tx_id}"
        )));
    }

    if let Some(tbl) = repo.table_by_token(token).await? {
        let backends = tbl.index2_registry().all_backends().await;

        // F-74 (#901) test-only ordering toggle: normally always `false`
        // (the fixed bump-then-apply order below). A test can set
        // `TEST_LEGACY_APPLY_THEN_BUMP_ORDER` to temporarily resurrect the
        // OLD, buggy apply-then-bump order so the TOCTOU window it left open
        // can be proven RED (and, once cleared, proven GREEN again) against
        // the exact same pause seam (`fire_index_batch_bump_apply_test_hook`)
        // placed strictly between the two calls in both orderings.
        #[cfg(any(test, feature = "test-util"))]
        let legacy_apply_then_bump =
            TEST_LEGACY_APPLY_THEN_BUMP_ORDER.load(std::sync::atomic::Ordering::SeqCst);
        #[cfg(not(any(test, feature = "test-util")))]
        let legacy_apply_then_bump = false;

        if legacy_apply_then_bump {
            // OLD (buggy) order, test-only: apply the postings FIRST...
            crate::index2::write_ops::apply_index_ops_at_commit(ops, tbl.info_store(), &backends)
                .await
                .map_err(DbError::from)?;
            tbl.index_manager_ref()
                .invalidate_posting_cache_for_ops(ops);
            // ...pause strictly between apply and bump — the exact TOCTOU
            // window F-74 closes: postings already mutated, epoch not yet
            // raised.
            fire_index_batch_bump_apply_test_hook().await;
            // ...bump SECOND.
            tbl.sorted_indexes()
                .bump_touched_indexes(ops, commit_version);
        } else {
            // F-74 (#901) FIX: bump the per-index mutation high-water for
            // exactly the sorted index(es) `ops` touched BEFORE the postings
            // land, mirroring the non-tx direct path's order. See the
            // function doc for the full TOCTOU-closing rationale. An `ops`
            // batch with no sorted-index entries (or ops touching only OTHER
            // index families) bumps nothing here — a decode miss on a key
            // that actually IS a sorted-index posting is a real correctness
            // risk (it leaves that index's gate open on postings that are
            // about to change), not a harmless degrade-to-full-scan; see
            // `bump_touched_indexes`'s own doc.
            tbl.sorted_indexes()
                .bump_touched_indexes(ops, commit_version);

            // F-74 (#901) test-only pause seam: park strictly between the
            // bump above and the posting apply below — proves that with the
            // FIXED order, a concurrent AsOf read cannot observe an
            // unbumped-but-applied state (there IS no such state: the bump
            // already happened before any posting can change).
            fire_index_batch_bump_apply_test_hook().await;

            crate::index2::write_ops::apply_index_ops_at_commit(ops, tbl.info_store(), &backends)
                .await
                .map_err(DbError::from)?;
            // Invalidate the base_index IndexManager's posting cache for every
            // SetPosting / RemovePosting that was just durably applied, so
            // the next lookup_by_index re-fetches from the store instead of
            // returning a stale cached result.
            tbl.index_manager_ref()
                .invalidate_posting_cache_for_ops(ops);
        }
        // The postings are applied and visible: tell the drainer not to replay
        // this entry's index ops for the table (see `Drainer::index_applied`).
        repo.drainer().mark_index_applied(commit_version, token);
    }
    Ok(())
}

/// Persist `last_committed_version` + `next_tx_id` recovery markers
/// (Phase 6.5).
///
/// **Audit §1.7 (marker monotonicity):** this writes `gate.last_committed()`
/// (the monotonic maximum across ALL committers), NOT the raw `commit_version`
/// of this single tx. Parallel Phase 6.5 writers that each wrote their OWN
/// version could regress the marker (committer A writes 10, then committer B
/// writes 9) — on a crash the gate would seed from the regressed marker and
/// `assign_next_version()` could re-issue a version a truncated segment
/// already used. Writing the monotonic max guarantees the marker never goes
/// backwards regardless of commit interleaving.
///
/// R2a follow-up 3 (test seam): when `TEST_ORPHAN_MARKER_PARK` is armed
/// for this repository, the marker save is dispatched as a non-abortable
/// task outliving a dropped future (`orphan_marker_park_save`); otherwise
/// the direct save runs.
pub async fn persist_markers(
    repo: &RepoCore,
    gate: &shamir_tx::RepoTxGate,
    _commit_version: u64,
) -> Result<(), DbError> {
    let info_store = repo.tx_info_store().await?;
    // Write the monotonic max, not the raw commit_version — see the §1.7
    // doc above. `gate.last_committed()` is >= this tx's commit_version at
    // this point (the version was published before Phase 6.5 runs).
    //
    // §1.7 residual (found in @sh review): `gate.last_committed()` is a
    // monotonic in-memory max, but with NO serialization here, two
    // concurrent Phase 6.5 committers' `Store::set` calls have no
    // ordering guarantee relative to each other — committer A (max=10)
    // and committer B (max=9, read before A's commit published) could
    // still land B's write LAST on disk, regressing the marker exactly
    // as this finding describes, just narrowing the window instead of
    // closing it. Serializing the read-current/write-if-greater sequence
    // under `marker_write_mutex` makes the on-disk marker genuinely
    // monotonic: whichever writer takes the lock second observes the
    // first writer's already-persisted value and refuses to regress it.
    {
        let _guard = repo.marker_write_mutex().lock().await;
        let candidate = gate.last_committed();
        let current = load_last_committed(&info_store).await?;
        let should_write = match current {
            Some(c) => candidate > c,
            None => true,
        };
        if should_write {
            // R2a follow-up 3: one-shot park before the store write (see
            // `orphan_marker_park_save`); the hook path replaces the direct
            // save — never write twice. Both paths run under the mutex
            // guard above, keeping the on-disk marker monotonic (§1.7).
            #[cfg(any(test, feature = "test-util"))]
            if orphan_marker_park_save(repo, &info_store, candidate).await? {
                // The hook path replaced the direct save — never write
                // twice.
            } else {
                save_last_committed(&info_store, candidate).await?;
            }
            #[cfg(not(any(test, feature = "test-util")))]
            save_last_committed(&info_store, candidate).await?;
        }
        // Share the reservation lock with WAL-less follower allocation. Do
        // not regress transaction-id evidence with a delayed commit tail.
        let next_tx_id = load_next_tx_id_snapshot(&info_store)
            .await?
            .unwrap_or(1)
            .max(gate.peek_next_tx_id());
        save_next_tx_id_snapshot(&info_store, next_tx_id).await?;
    }
    Ok(())
}

/// cancel-safe: yes — sequential bounded retry; each attempt fully
/// `.await`s before the next, so cancellation drops the in-flight attempt
/// with no straddled borrow. Re-runs `op` up to `attempts` times, taking
/// the first `Ok`. Returns the last `Err` if all attempts fail. Safe to
/// retry only because every materialization sub-phase is idempotent at
/// the data layer (last-write-wins).
pub(crate) async fn retry_materialize<F, Fut, T>(attempts: u32, mut op: F) -> Result<T, DbError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, DbError>>,
{
    let mut last_err: Option<DbError> = None;
    for _ in 0..attempts.max(1) {
        match op().await {
            Ok(v) => return Ok(v),
            Err(e) => last_err = Some(e),
        }
    }
    Err(last_err.unwrap_or_else(|| DbError::Internal("retry_materialize: no attempt run".into())))
}

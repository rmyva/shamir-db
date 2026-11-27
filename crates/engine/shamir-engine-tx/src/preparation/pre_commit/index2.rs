use std::sync::Arc;

use shamir_storage::error::DbError;
use shamir_storage::types::{KvOp, RecordKey};
use shamir_tx::{IndexWriteOp, TxContext};
use shamir_types::types::record_id::RecordId;
use shamir_types::types::value::InnerValue;

use crate::commit::core::RepoCore;
use crate::commit::TxError;
use crate::index2::kind::IndexKind;

// ── F-73 (#900) test-only failure-injection seam ────────────────────────────
//
// Mirrors `table_manager_streaming.rs::TEST_READ_ONE_TX_BYTES_FAILURE` /
// `ReadOneTxBytesFailHook` (F-65, #891): a `#[cfg(test)]` `OnceLock<Arc<Hook>>`
// global, zero cost when unset. Lets a test make the NEXT
// `read_pre_tx_bytes(table_token, id)` call for a specific `(table_token,
// RecordId)` return a genuine `Err(DbError::Storage)` instead of reading the
// pre-tx bytes — so `rederive_index2_ops_post_stage`'s commit-time storage
// read hits a real, deterministic error (no sleeps, no timing races) proving
// the F-73 fail-closed fix.
//
// One-shot per arm: once an armed `(table_token, id)` fires it is consumed,
// so a retry of the SAME id reads normally.
#[cfg(any(test, feature = "test-util"))]
pub static TEST_REDERIVE_PRE_TX_READ_FAILURE: std::sync::OnceLock<
    std::sync::Arc<RederivePreTxReadFailHook>,
> = std::sync::OnceLock::new();

/// Test-only one-shot failure injector for [`read_pre_tx_bytes`]. See
/// [`TEST_REDERIVE_PRE_TX_READ_FAILURE`]'s doc for the rationale.
#[cfg(any(test, feature = "test-util"))]
#[derive(Debug, Default)]
pub struct RederivePreTxReadFailHook {
    pub inner: std::sync::Mutex<Vec<(u64, RecordId)>>,
}

#[cfg(any(test, feature = "test-util"))]
impl RederivePreTxReadFailHook {
    /// Arm a one-shot injected `Err` for the next
    /// `read_pre_tx_bytes(table_token, id)`.
    pub fn arm(&self, table_token: u64, id: RecordId) {
        self.inner.lock().unwrap().push((table_token, id));
    }

    /// Returns `Some(Err)` if `(table_token, id)` is armed, consuming the arm
    /// (one-shot); `None` otherwise.
    fn take_injected(&self, table_token: u64, id: RecordId) -> Option<DbError> {
        let mut guard = self.inner.lock().unwrap();
        let pos = guard
            .iter()
            .position(|(tt, rid)| *tt == table_token && *rid == id)?;
        guard.swap_remove(pos);
        Some(DbError::Storage(format!(
            "F-73 injected rederive pre-tx read failure (table_token={table_token}, id={id:?})"
        )))
    }
}

/// Read the pre-tx value of a staged record's key from `data_store`, used by
/// [`rederive_index2_ops_post_stage`] to distinguish insert vs. update/delete.
/// Returns `Ok(None)` for `DbError::NotFound` (the proven "no pre-tx row"
/// semantics — see that function's doc), `Ok(Some(bytes))` on a hit, and
/// propagates every other error (F-73: a transient storage error here must
/// abort the tx, not silently skip the record).
pub(super) async fn read_pre_tx_bytes(
    data_store: &Arc<dyn shamir_storage::types::Store>,
    #[cfg_attr(not(test), allow(unused_variables))] table_token: u64,
    #[cfg_attr(not(test), allow(unused_variables))] rid: RecordId,
    key: &RecordKey,
) -> Result<Option<bytes::Bytes>, DbError> {
    #[cfg(any(test, feature = "test-util"))]
    if let Some(hook) = TEST_REDERIVE_PRE_TX_READ_FAILURE.get() {
        if let Some(err) = hook.take_injected(table_token, rid) {
            return Err(err);
        }
    }
    match data_store.get(key.clone()).await {
        Ok(bytes) => Ok(Some(bytes)),
        Err(DbError::NotFound(_)) => Ok(None),
        Err(e) => Err(e),
    }
}

/// F-50 (#869, spike) — Phase 2.7 worker: re-derive index2 posting ops for
/// backends registered AFTER each touched table's stage-time snapshot.
///
/// Invoked from [`pre_commit_prelock`](super::prelock::pre_commit_prelock) AFTER Phase 2.5's barrier locks are
/// acquired and BEFORE Phase 4's WAL begin. See that call site's comment for
/// the crash-safety ordering constraints.
///
/// cancel-safe: YES — appends to `tx.index_write_set` only (in-memory, tx-
/// scoped; dropped by RAII on abort). The `data_store.get` reads are
/// read-only. No durable mutation happens here, so cancellation before Phase
/// 4 is a clean abort (the re-derived ops never reached the WAL).
///
/// Old-value resolution (insert vs. update vs. delete): `tx.write_set` only
/// carries the NET staged op (`Set`/`Remove`) — it does not retain whether a
/// `Set` is a fresh insert or an overwrite, nor the pre-tx value an update /
/// delete needs to plan the REMOVE half of its posting diff. Phase 5a
/// materialize has NOT run yet at this point (we are still in prelock), so
/// the data store still holds the PRE-tx committed value. A single
/// `data_store.get(key)` per staged record settles it: `NotFound` ⇒ insert
/// (plan_insert), `Some(old)` ⇒ update (plan_update) or delete (plan_delete).
/// The cost is bounded by the number of staged records AND gated behind the
/// generation check (only paid when an index2 backend was actually registered
/// between stage and commit — the rare DDL-concurrent case).
///
/// Scope note (Step 2, #870): the stage-time generation is now captured on
/// ALL mutation staging paths — INSERT (`insert_tx` / `insert_tx_many` /
/// `insert_tx_many_bytes`), UPDATE (`update_tx` / `update_tx_bytes`),
/// DELETE (`delete_tx`), and SET (`set_tx` — an alias of `update_tx`).
/// The sorted-index generation is captured alongside (`note_sorted_stage_gen`)
/// for every stage site, gating the base_index sorted-index re-derivation below.
/// Vector backends' `staged_vectors` are re-derived in the per-record loop
/// (their `plan_insert_tx` is a no-op; HNSW embeddings are buffered
/// separately) — Step 2 Part B.
///
/// F-73 (#900, P0) — FAIL CLOSED. This function used to return `()` and
/// silently swallow every error class it could hit: a non-`NotFound`
/// `data_store.get` error, a `plan_insert_tx`/`plan_update_tx`/
/// `plan_delete_tx`/`plan_record_*` `Err`, a record that fails
/// `InnerValue::from_bytes`, or a staged key that fails
/// `RecordId::try_from_bytes`. Because Phase 5a (the data mutation) runs
/// UNCONDITIONALLY later in the pipeline, a transient error here used to let
/// the tx commit successfully while silently skipping the row's posting for
/// the new index — a permanent, unreported table/index divergence. Now every
/// one of those classes propagates via `?`/`map_err` and the tx aborts before
/// Phase 4's WAL begin (see the call site in `pre_commit_prelock`). The ONE
/// exception, unchanged from before: `Err(DbError::NotFound(_))` on
/// `data_store.get` stays the sole "treat as insert" case — that is the
/// PROVEN semantics at this call site (Phase 5a hasn't run yet, so the store
/// still holds the pre-tx value; `NotFound` genuinely means "no pre-tx row").
/// A malformed staged key or an undecodable staged/stored record is an
/// internal invariant violation (the staging path guarantees well-formed
/// keys/values reach here), surfaced as `DbError::Internal` /
/// `DbError::Codec` respectively — the same typed-error-over-bare-string
/// style F-55 (#881, commit `f9eed337`) and F-65 (#891, commit `28d39f31`)
/// established for this fail-open defect class.
pub(super) async fn rederive_index2_ops_post_stage(
    tx: &mut TxContext,
    repo: &RepoCore,
) -> Result<(), TxError> {
    // =========================================================================
    // Index2 re-derivation (F-50 #869 Step 1) + vector staging (Step 2 Part B)
    // =========================================================================
    //
    // Empty for the overwhelming majority of txs (populated only by an
    // index2-bearing-table staging path) — the single zero-overhead gate.
    if !tx.index2_stage_gens.is_empty() {
        // Clone the captured (table_token, stage_gen) pairs out so the per-table
        // async work below does not hold a borrow of `tx` (we must mutably
        // reborrow `tx.index_write_set` to append re-derived ops).
        let stage_gens: Vec<(u64, u64)> =
            tx.index2_stage_gens.iter().map(|(t, g)| (*t, *g)).collect();
        let tx_id = Some(tx.tx_id);

        for (table_token, stage_gen) in stage_gens {
            // Resolve WITHOUT forcing lazy instantiation — a dormant table has no
            // index2 backends (mirrors Phase 2.5's `table_by_token_if_live`).
            let Some(tbl) = repo.table_by_token_if_live(table_token).await else {
                continue;
            };
            // Generation gate: skip EVERYTHING (both add and retract) when
            // nothing changed since stage. One atomic Acquire load.
            let reg = tbl.index2_registry();
            if reg.generation() == stage_gen {
                continue;
            }
            // R0-B (#1008): unlike the pre-R0-B code, do NOT `continue` here
            // when `new_backends` is empty. The generation advancing with no
            // NEW backend means something was DROPPED (or replaced —
            // ABA) since stage — exactly the case retraction below must
            // catch. `backends_newer_than` only ever reports newly
            // REGISTERED backends, so it is correctly empty for a pure-DROP
            // generation bump; skipping the whole iteration on that empty
            // check (the old shape) is precisely how index2 never retracted
            // stale ops for a DROP'd backend.
            let new_backends = reg.backends_newer_than(stage_gen).await;
            // Collect the staged ops for this table into an owned Vec: the
            // per-record ADD loop below calls `tx.stage_vector`/
            // `tx.stage_vector_delete` (`&mut self` on the whole `TxContext`),
            // which cannot coexist with a live borrow into `tx.write_set` —
            // an owned snapshot is required here, unlike the sorted-index
            // loop below (which takes no `&mut tx` and can iterate lazily).
            // Retraction (below) does not need this — only the per-record ADD
            // loop does — so an empty `new_backends` still runs retraction
            // even if there happen to be no staged ops for this table.
            let staged_ops: Vec<KvOp> = match tx.write_set.get(&table_token) {
                Some(staging) => staging.snapshot_ops(),
                None => Vec::new(),
            };
            let data_store = tbl.data_store().clone();

            let mut appended: Vec<(u64, IndexWriteOp)> = Vec::new();
            if !new_backends.is_empty() {
                for kvop in staged_ops {
                    match kvop {
                        KvOp::Set(k, v) => {
                            // F-73: a staged key that doesn't decode as a RecordId is an
                            // internal invariant violation (the staging path guarantees
                            // well-formed keys reach here) — fail the tx, don't skip.
                            let rid = RecordId::try_from_bytes(&k).ok_or_else(|| {
                                TxError::Storage(DbError::Internal(format!(
                                    "rederive_index2_ops_post_stage: malformed staged key \
                                 (table_token={table_token}, {} bytes) — expected a \
                                 16-byte RecordId",
                                    k.len()
                                )))
                            })?;
                            // F-73: a staged record that fails to decode is corruption,
                            // not a normal runtime condition — fail the tx.
                            let new_rec = InnerValue::from_bytes(&v).map_err(|e| {
                                TxError::Storage(DbError::Codec(format!(
                                    "rederive_index2_ops_post_stage: staged record decode \
                                 failed (table_token={table_token}, rid={rid:?}): {e}"
                                )))
                            })?;
                            // Phase 5a has not run: the store still holds the PRE-tx
                            // value, so this one read distinguishes insert vs. update.
                            match read_pre_tx_bytes(&data_store, table_token, rid, &k).await {
                                Ok(Some(old_bytes)) => {
                                    // F-73: the pre-tx value MUST decode — it was written
                                    // by a prior successful commit through this same
                                    // codec. A decode failure here is corruption, not
                                    // "skip this record".
                                    let old_rec =
                                        InnerValue::from_bytes(&old_bytes).map_err(|e| {
                                            TxError::Storage(DbError::Codec(format!(
                                                "rederive_index2_ops_post_stage: pre-tx record \
                                         decode failed (table_token={table_token}, \
                                         rid={rid:?}): {e}"
                                            )))
                                        })?;
                                    for backend in &new_backends {
                                        let mut ops = backend
                                            .plan_update_tx(rid, &old_rec, &new_rec, tx_id)
                                            .await
                                            .map_err(|e| {
                                                TxError::Storage(DbError::Internal(format!(
                                                    "rederive_index2_ops_post_stage: \
                                                 plan_update_tx failed \
                                                 (table_token={table_token}, rid={rid:?}): {e}"
                                                )))
                                            })?;
                                        // R0-B (#1008): stamp the REAL live
                                        // instance epoch — see
                                        // `index2_provenance`'s doc for why the
                                        // backend itself cannot know it.
                                        stamp_index2_ops_provenance(reg, backend, &mut ops).await;
                                        appended
                                            .extend(ops.into_iter().map(|op| (table_token, op)));
                                        // F-50 Step 2 (#870, Part B): re-derive
                                        // staged vectors for a new vector backend.
                                        // VectorBackend::plan_update_tx is a no-op
                                        // for a tx (HNSW embeddings route through
                                        // tx.staged_vectors instead), so the posting
                                        // loop above contributes nothing for it.
                                        // Mirror stage_vector_deletes_on_update +
                                        // stage_vectors from table_manager_tx_ops:
                                        // if old carried a vector and new does not,
                                        // stage a delete; otherwise stage the new.
                                        if is_vector_backend(backend) {
                                            if backend.staged_vector(rid, &old_rec).await.is_some()
                                                && backend
                                                    .staged_vector(rid, &new_rec)
                                                    .await
                                                    .is_none()
                                            {
                                                tx.stage_vector_delete(table_token, rid);
                                            }
                                            if let Some(vec) =
                                                backend.staged_vector(rid, &new_rec).await
                                            {
                                                tx.stage_vector(table_token, rid, vec);
                                            }
                                        }
                                    }
                                }
                                Ok(None) => {
                                    // NotFound is the ONLY case treated as "this is an
                                    // insert, not an update" — the proven semantics at
                                    // this call site (see the fn's doc comment).
                                    for backend in &new_backends {
                                        let mut ops = backend
                                            .plan_insert_tx(rid, &new_rec, tx_id)
                                            .await
                                            .map_err(|e| {
                                                TxError::Storage(DbError::Internal(format!(
                                                    "rederive_index2_ops_post_stage: \
                                                 plan_insert_tx failed \
                                                 (table_token={table_token}, rid={rid:?}): {e}"
                                                )))
                                            })?;
                                        stamp_index2_ops_provenance(reg, backend, &mut ops).await;
                                        appended
                                            .extend(ops.into_iter().map(|op| (table_token, op)));
                                        // F-50 Step 2 (#870, Part B): re-derive staged
                                        // vector for a new vector backend (insert case).
                                        // Mirror stage_vectors from table_manager_tx_ops.
                                        if is_vector_backend(backend) {
                                            if let Some(vec) =
                                                backend.staged_vector(rid, &new_rec).await
                                            {
                                                tx.stage_vector(table_token, rid, vec);
                                            }
                                        }
                                    }
                                }
                                // F-73: a non-NotFound storage error MUST abort the tx —
                                // it used to be a silent best-effort skip, which let the
                                // commit succeed while the row's posting for the new
                                // index was permanently dropped.
                                Err(e) => return Err(TxError::Storage(e)),
                            }
                        }
                        KvOp::Remove(k) => {
                            let rid = RecordId::try_from_bytes(&k).ok_or_else(|| {
                                TxError::Storage(DbError::Internal(format!(
                                    "rederive_index2_ops_post_stage: malformed staged key \
                                 (table_token={table_token}, {} bytes) — expected a \
                                 16-byte RecordId",
                                    k.len()
                                )))
                            })?;
                            // Nothing committed to delete from the index on a NotFound
                            // read (the row was never materialized) — a non-NotFound
                            // error still aborts the tx (F-73).
                            if let Some(old_bytes) =
                                read_pre_tx_bytes(&data_store, table_token, rid, &k).await?
                            {
                                let old_rec = InnerValue::from_bytes(&old_bytes).map_err(|e| {
                                    TxError::Storage(DbError::Codec(format!(
                                        "rederive_index2_ops_post_stage: pre-tx record \
                                     decode failed (table_token={table_token}, \
                                     rid={rid:?}): {e}"
                                    )))
                                })?;
                                for backend in &new_backends {
                                    let mut ops = backend
                                        .plan_delete_tx(rid, &old_rec, tx_id)
                                        .await
                                        .map_err(|e| {
                                            TxError::Storage(DbError::Internal(format!(
                                                "rederive_index2_ops_post_stage: \
                                                 plan_delete_tx failed \
                                                 (table_token={table_token}, rid={rid:?}): {e}"
                                            )))
                                        })?;
                                    stamp_index2_ops_provenance(reg, backend, &mut ops).await;
                                    appended.extend(ops.into_iter().map(|op| (table_token, op)));
                                    // F-50 Step 2 (#870, Part B): re-derive staged
                                    // vector delete for a new vector backend.
                                    // Mirror stage_vector_delete.
                                    if is_vector_backend(backend)
                                        && backend.staged_vector(rid, &old_rec).await.is_some()
                                    {
                                        tx.stage_vector_delete(table_token, rid);
                                    }
                                }
                            }
                        }
                    }
                }
            } // end `if !new_backends.is_empty()`
              // Append the re-derived ops FIRST — mirrors the base_index/sorted
              // ordering: they're planned against LIVE backends (freshly read
              // from the registry above), so they carry the CURRENT
              // `instance_epoch` and pass the retract filter below unchanged.
            if !appended.is_empty() {
                tx.index_write_set.extend(appended);
            }

            // R0-B (#1008): retract stale staged index2 ops. Before this fix,
            // index2 had NO retraction at all — `rederive_index2_ops_post_stage`
            // only ever appended. A backend DROP'd (or replaced — ABA, a new
            // backend registered under the SAME name gets a NEW `id` and a
            // fresh `entry.gen`) between stage and commit left its stale
            // staged ops in `tx.index_write_set` forever, resurrecting
            // postings for a gone index2 backend at Phase 5c.
            // F-6 (2026-08-06): `d.name_interned` here is the SAME
            // construction-time `descriptor()` snapshot `stamp_index2_provenance`
            // (table_manager_tx_ops.rs) stamps staged ops with — deliberately,
            // not `reg`'s authoritative `BackendEntry.name_interned` (which
            // `rename_entry` DOES update). Matching two stale snapshots against
            // each other is self-consistent; matching one stale and one
            // authoritative would falsely retract every index2 op staged
            // before a RENAME. Do not "fix" this side alone.
            let live_index2: shamir_collections::TFxSet<(u64, u64)> = {
                let mut set = shamir_collections::TFxSet::default();
                for backend in reg.all_backends().await {
                    let d = backend.descriptor();
                    if let Some(epoch) = reg.instance_epoch_of(d.id).await {
                        set.insert((d.name_interned, epoch));
                    }
                }
                set
            };
            retract_stale_provenance_ops(
                tx,
                table_token,
                shamir_tx::IndexFamily::Index2,
                &live_index2,
            );
        }
    }

    // =========================================================================
    // F-50 Step 2 (#870, Part D): sorted-index re-derivation
    // =========================================================================
    //
    // Same root-cause class as index2 Part B (stale stage-time plan): a tx that
    // staged before a new sorted index was registered (via
    // `create_sorted_index`) carries zero sorted ops in tx.index_write_set for
    // the new index — a guaranteed miss mirroring #538 Part B. This block
    // re-derives sorted posting ops for tables whose sorted generation advanced.
    //
    // Re-planning against ALL current defs is safe because posting ops are
    // idempotent (`SetPosting` overwrites with the same key+value; `RemovePosting`
    // is a no-op on an already-absent key). The generation gate avoids this work
    // on the common path (no DDL → generation unchanged → per-record loop never
    // runs). Only paid in the rare DDL-concurrent case.
    if !tx.sorted_stage_gens.is_empty() {
        let sorted_gens: Vec<(u64, u64)> =
            tx.sorted_stage_gens.iter().map(|(t, g)| (*t, *g)).collect();
        for (table_token, stage_gen) in sorted_gens {
            let Some(tbl) = repo.table_by_token_if_live(table_token).await else {
                continue;
            };
            let sorted_mgr = tbl.sorted_indexes();
            // Generation gate: skip when no sorted def was registered/dropped
            // since stage. One atomic Acquire load.
            if sorted_mgr.generation() == stage_gen {
                continue;
            }
            // F-7 (2026-08-06): unlike the index2 half above (which uses
            // `Vec::new()` here so retraction below still runs even with no
            // staged rows), a missing `write_set` entry `continue`s and
            // skips retraction entirely for this table. Safe TODAY only
            // because `stage_mutation` always calls `ensure_table_staging`
            // first, so a table with a `sorted_stage_gens` entry always also
            // has a `write_set` entry — this branch is currently
            // unreachable. If that invariant ever changes, this needs the
            // same `Vec::new()` treatment as index2 to avoid silently
            // leaving stale sorted ops unretracted.
            // P1 perf fix: borrow-iterate staged ops for this table instead
            // of collecting into an owned `Vec<KvOp>` — unlike the index2
            // half above, this loop takes no `&mut tx` (the two `tx`
            // mutations, `tx.index_write_set.extend` and
            // `retract_stale_provenance_ops(tx, ..)`, happen after the loop
            // below), so holding the `staging` borrow across the per-record
            // async planning here is sound.
            let Some(staging) = tx.write_set.get(&table_token) else {
                continue;
            };
            let data_store = tbl.data_store().clone();

            let mut appended: Vec<(u64, IndexWriteOp)> = Vec::new();
            for kvop in staging.iter_ops() {
                match kvop {
                    KvOp::Set(k, v) => {
                        // F-73: same invariant-violation treatment as the index2
                        // half above — a malformed staged key or undecodable
                        // staged record is corruption, not "skip this record".
                        let rid = RecordId::try_from_bytes(&k).ok_or_else(|| {
                            TxError::Storage(DbError::Internal(format!(
                                "rederive sorted-index: malformed staged key \
                                 (table_token={table_token}, {} bytes) — expected a \
                                 16-byte RecordId",
                                k.len()
                            )))
                        })?;
                        let new_rec = InnerValue::from_bytes(&v).map_err(|e| {
                            TxError::Storage(DbError::Codec(format!(
                                "rederive sorted-index: staged record decode failed \
                                 (table_token={table_token}, rid={rid:?}): {e}"
                            )))
                        })?;
                        // Phase 5a has not run: the store still holds the PRE-tx
                        // value, so this one read distinguishes insert vs. update.
                        match read_pre_tx_bytes(&data_store, table_token, rid, &k).await {
                            Ok(Some(old_bytes)) => {
                                let old_rec = InnerValue::from_bytes(&old_bytes).map_err(|e| {
                                    TxError::Storage(DbError::Codec(format!(
                                        "rederive sorted-index: pre-tx record decode \
                                         failed (table_token={table_token}, rid={rid:?}): {e}"
                                    )))
                                })?;
                                let ops =
                                    sorted_mgr.plan_record_updated(&rid, &old_rec, &new_rec, 0)?;
                                appended.extend(ops.into_iter().map(|op| (table_token, op)));
                            }
                            Ok(None) => {
                                // NotFound is the ONLY case treated as "this is an
                                // insert, not an update" (same proven semantics as
                                // the index2 half above).
                                let ops = sorted_mgr.plan_record_created(&rid, &new_rec, 0)?;
                                appended.extend(ops.into_iter().map(|op| (table_token, op)));
                            }
                            // F-73: a non-NotFound storage error MUST abort the tx.
                            Err(e) => return Err(TxError::Storage(e)),
                        }
                    }
                    KvOp::Remove(k) => {
                        let rid = RecordId::try_from_bytes(&k).ok_or_else(|| {
                            TxError::Storage(DbError::Internal(format!(
                                "rederive sorted-index: malformed staged key \
                                 (table_token={table_token}, {} bytes) — expected a \
                                 16-byte RecordId",
                                k.len()
                            )))
                        })?;
                        if let Some(old_bytes) =
                            read_pre_tx_bytes(&data_store, table_token, rid, &k).await?
                        {
                            let old_rec = InnerValue::from_bytes(&old_bytes).map_err(|e| {
                                TxError::Storage(DbError::Codec(format!(
                                    "rederive sorted-index: pre-tx record decode failed \
                                     (table_token={table_token}, rid={rid:?}): {e}"
                                )))
                            })?;
                            let ops = sorted_mgr.plan_record_deleted(&rid, &old_rec)?;
                            appended.extend(ops.into_iter().map(|op| (table_token, op)));
                        }
                    }
                }
            }
            // Append the re-derived ops FIRST — mirrors
            // `rederive_base_index_ops_post_stage`'s ordering: they're
            // planned against current live defs, so they carry the CURRENT
            // `instance_epoch` and pass the retract filter below unchanged.
            if !appended.is_empty() {
                tx.index_write_set.extend(appended);
            }

            // R0-B (#1008): retract stale staged sorted ops — the sorted
            // family previously had NO retraction at all (only base_index
            // did, via the now-replaced byte-length heuristic). Combined
            // with Part 1's `SortedIndexManager::rename_definition` now
            // bumping `generation` (so this gate fires after a rename too),
            // this closes both failure modes: `stage → DROP sorted index →
            // commit` (stale ops resurrecting postings for a gone index) and
            // `stage sorted op → RENAME → commit` (stale ops still targeting
            // the OLD name/epoch after the rename).
            let live_sorted: shamir_collections::TFxSet<(u64, u64)> = sorted_mgr
                .iter_indexes()
                .into_iter()
                .map(|def| (def.name_interned, def.instance_epoch))
                .collect();
            retract_stale_provenance_ops(
                tx,
                table_token,
                shamir_tx::IndexFamily::Sorted,
                &live_sorted,
            );
        }
    }

    Ok(())
}

/// F-50 Step 2 (#870): true when `backend`'s descriptor kind is Vector. Used
/// to gate the vector-staging branches in [`rederive_index2_ops_post_stage`]
/// (VectorBackend::plan_*_tx are no-ops for a tx; embeddings route through
/// `tx.staged_vectors` / `tx.staged_vector_deletes` instead).
fn is_vector_backend(backend: &Arc<dyn shamir_index::backend::IndexBackend>) -> bool {
    matches!(backend.descriptor().kind, IndexKind::Vector(_))
}

/// R0-B (#1008): overwrite the placeholder `Provenance`
/// `shamir_index::write_ops::index2_provenance` stamps on every op an
/// index2 `backend` plans, with the REAL live instance epoch
/// (`IndexRegistry::instance_epoch_of`) for that backend's id — mirrors
/// `TableManager::stamp_index2_provenance` (the tx-STAGE-time twin of this
/// COMMIT-time call site; see that method's doc for why a two-step stamp is
/// necessary for index2 specifically: the backend only ever sees its own
/// construction-time descriptor snapshot, never the registry's live `gen`).
async fn stamp_index2_ops_provenance(
    reg: &crate::index2::IndexRegistry,
    backend: &Arc<dyn shamir_index::backend::IndexBackend>,
    ops: &mut [IndexWriteOp],
) {
    if ops.is_empty() {
        return;
    }
    let d = backend.descriptor();
    if let Some(instance_epoch) = reg.instance_epoch_of(d.id).await {
        let provenance = shamir_tx::Provenance {
            family: shamir_tx::IndexFamily::Index2,
            name_interned: d.name_interned,
            instance_epoch,
        };
        for op in ops.iter_mut() {
            op.set_provenance(provenance);
        }
    }
}

/// R0-B (#1008): the ONE unifying retraction rule shared by all three
/// `rederive_*_ops_post_stage` functions below (base_index regular+unique,
/// sorted, index2). Replaces three divergent pre-R0-B heuristics — the
/// base_index family's byte-length/name `(is_unique, name_interned)` filter
/// (vulnerable to ABA: a DROP+CREATE of the same name under a DIFFERENT
/// definition still matched) and index2/sorted's COMPLETE ABSENCE of any
/// retraction at all — with a single correct check: a staged op for `family`
/// on `table_token` is retracted iff its `Provenance.(name_interned,
/// instance_epoch)` does NOT match any entry in `live`, the CURRENT set of
/// `(name_interned, instance_epoch)` pairs for every LIVE definition of that
/// family. A match means "the definition this op was planned against is
/// still the SAME instance" (survives); no match means the definition was
/// dropped, or replaced (DROP+CREATE-same-name — ABA, now correctly
/// distinguished because the new instance mints a FRESH epoch), or renamed
/// (the OLD name_interned no longer belongs to any live instance).
///
/// Ops for OTHER tables or OTHER families are left untouched (each call site
/// filters to its own table_token + family; a different family's ops simply
/// don't match `op.provenance().family` and pass through unaffected — this
/// makes it safe for all three call sites to run in sequence on the same
/// `tx.index_write_set` without needing to coordinate).
pub(super) fn retract_stale_provenance_ops(
    tx: &mut TxContext,
    table_token: u64,
    family: shamir_tx::IndexFamily,
    live: &shamir_collections::TFxSet<(u64, u64)>,
) {
    tx.index_write_set.retain(|(tt, op)| {
        if *tt != table_token {
            return true; // Different table — not our concern.
        }
        let provenance = match op {
            IndexWriteOp::SetPosting { provenance, .. } => provenance,
            IndexWriteOp::RemovePosting { provenance, .. } => provenance,
            IndexWriteOp::BumpFtsStats { provenance, .. } => provenance,
        };
        if provenance.family != family {
            return true; // A different family's op — not our concern.
        }
        live.contains(&(provenance.name_interned, provenance.instance_epoch))
    });
}

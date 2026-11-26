use shamir_storage::error::DbError;
use shamir_storage::types::KvOp;
use shamir_tx::{IndexWriteOp, TxContext, UniqueGuard};
use shamir_types::types::record_id::RecordId;
use shamir_types::types::value::InnerValue;

use super::index2::{read_pre_tx_bytes, retract_stale_provenance_ops};
use crate::commit::core::RepoCore;
use crate::commit::TxError;

/// P0-2 (#958): base_index `IndexManager` (regular + unique) ops-plan
/// re-derivation after stage. Mirrors [`rederive_index2_ops_post_stage`](super::index2::rederive_index2_ops_post_stage)'s
/// shape for the base_index index family: a tx that staged before a base_index
/// index (regular OR unique) was created carries zero ops for the new
/// index in `tx.index_write_set` — a permanently missing posting (regular)
/// or an unconstrained duplicate (unique, because no `UniqueGuard` was
/// recorded for a def that didn't exist at stage time).
///
/// At commit, if a table's base_index generation exceeds the captured value,
/// this function re-plans regular + unique posting ops against ALL current
/// defs (idempotent — `SetPosting` overwrites, `RemovePosting` is a no-op
/// on absent keys — so re-planning defs that already had ops at stage time
/// is harmless), AND records fresh `UniqueGuard`s for every current unique
/// def so Phase 2.6's authoritative re-validation under `commit_lock`
/// covers the new constraint.
///
/// Sub-bug 2c: ALSO retracts staged ops for base_index indexes that were
/// DROP'd between stage and commit. An op whose key starts with a base_index
/// `(is_unique, name_interned)` prefix that's no longer in the current
/// def set is removed from `tx.index_write_set` before commit — preventing
/// orphan postings for gone indexes. Non-base_index ops (index2/sorted, which
/// have different key formats/lengths) are left untouched.
///
/// The generation gate makes this zero-cost on the common path: a tx that
/// captured `base_index_stage_gens` (populated only by a base_index-index-bearing-
/// table staging) and sees an unchanged generation skips the per-record
/// re-derivation entirely.
pub(in crate::tx) async fn rederive_base_index_ops_post_stage(
    tx: &mut TxContext,
    repo: &RepoCore,
) -> Result<(), TxError> {
    if tx.base_index_stage_gens.is_empty() {
        return Ok(());
    }

    let stage_gens: Vec<(u64, u64)> = tx
        .base_index_stage_gens
        .iter()
        .map(|(t, g)| (*t, *g))
        .collect();

    for (table_token, stage_gen) in stage_gens {
        let Some(tbl) = repo.table_by_token_if_live(table_token).await else {
            continue;
        };
        let mgr = tbl.index_manager_ref();
        // Generation gate: skip when no base_index def was registered/dropped
        // since stage. One atomic Acquire load.
        if mgr.generation() == stage_gen {
            continue;
        }

        // Collect staged ops for this table into an owned Vec: the
        // per-record loop below calls `tx.record_unique_guard` (`&mut self`
        // on the whole `TxContext`), which cannot coexist with a live
        // borrow into `tx.write_set` — unlike the sorted-index half above,
        // which takes no `&mut tx` and can iterate lazily via `iter_ops()`.
        //
        // F-7 (2026-08-06): unlike the index2 half above (which uses
        // `Vec::new()` here so retraction below still runs even with no
        // staged rows), a missing `write_set` entry `continue`s and skips
        // retraction entirely for this table — same asymmetry as sorted's
        // equivalent branch, safe today ONLY because `stage_mutation` always
        // calls `ensure_table_staging` first, making this branch currently
        // unreachable. See sorted's identical comment above for the
        // condition that would make this unsafe.
        let staged_ops: Vec<KvOp> = match tx.write_set.get(&table_token) {
            Some(staging) => staging.snapshot_ops(),
            None => continue,
        };
        let data_store = tbl.data_store().clone();

        let mut appended: Vec<(u64, IndexWriteOp)> = Vec::new();

        for kvop in staged_ops {
            match kvop {
                KvOp::Set(k, v) => {
                    let rid = RecordId::try_from_bytes(&k).ok_or_else(|| {
                        TxError::Storage(DbError::Internal(format!(
                            "rederive_base_index: malformed staged key \
                             (table_token={table_token}, {} bytes) — expected a \
                             16-byte RecordId",
                            k.len()
                        )))
                    })?;
                    let new_rec = InnerValue::from_bytes(&v).map_err(|e| {
                        TxError::Storage(DbError::Codec(format!(
                            "rederive_base_index: staged record decode failed \
                             (table_token={table_token}, rid={rid:?}): {e}"
                        )))
                    })?;
                    match read_pre_tx_bytes(&data_store, table_token, rid, &k).await {
                        Ok(Some(old_bytes)) => {
                            // Update case: old record exists in the store.
                            let old_rec = InnerValue::from_bytes(&old_bytes).map_err(|e| {
                                TxError::Storage(DbError::Codec(format!(
                                    "rederive_base_index: pre-tx record decode failed \
                                     (table_token={table_token}, rid={rid:?}): {e}"
                                )))
                            })?;
                            // Re-plan regular + unique ops against ALL current
                            // defs (idempotent for defs that already had ops).
                            let ops = mgr
                                .plan_record_updated(&rid, &old_rec, &new_rec)
                                .await
                                .map_err(|e| {
                                    TxError::Storage(DbError::Internal(format!(
                                        "rederive_base_index: plan_record_updated failed: {e}"
                                    )))
                                })?;
                            appended.extend(ops.into_iter().map(|op| (table_token, op)));
                            let unique_ops = mgr
                                .plan_record_updated_unique(&rid, &old_rec, &new_rec)
                                .await
                                .map_err(|e| {
                                    TxError::Storage(DbError::Internal(format!(
                                        "rederive_base_index: plan_record_updated_unique failed: {e}"
                                    )))
                                })?;
                            appended.extend(unique_ops.into_iter().map(|op| (table_token, op)));
                            // Record UniqueGuards for every current unique def
                            // so Phase 2.6's commit-time re-validation covers
                            // constraints that didn't exist at stage time.
                            for index_key in mgr.unique_keys_for(&new_rec) {
                                tx.record_unique_guard(UniqueGuard {
                                    table_token,
                                    index_key,
                                    owner: rid,
                                });
                            }
                        }
                        Ok(None) => {
                            // Insert case: no pre-tx record.
                            let ops =
                                mgr.plan_record_created(&rid, &new_rec).await.map_err(|e| {
                                    TxError::Storage(DbError::Internal(format!(
                                        "rederive_base_index: plan_record_created failed: {e}"
                                    )))
                                })?;
                            appended.extend(ops.into_iter().map(|op| (table_token, op)));
                            let unique_ops = mgr
                                .plan_record_created_unique(&rid, &new_rec)
                                .await
                                .map_err(|e| {
                                TxError::Storage(DbError::Internal(format!(
                                    "rederive_base_index: plan_record_created_unique failed: {e}"
                                )))
                            })?;
                            appended.extend(unique_ops.into_iter().map(|op| (table_token, op)));
                            for index_key in mgr.unique_keys_for(&new_rec) {
                                tx.record_unique_guard(UniqueGuard {
                                    table_token,
                                    index_key,
                                    owner: rid,
                                });
                            }
                        }
                        Err(e) => return Err(TxError::Storage(e)),
                    }
                }
                KvOp::Remove(k) => {
                    let rid = RecordId::try_from_bytes(&k).ok_or_else(|| {
                        TxError::Storage(DbError::Internal(format!(
                            "rederive_base_index: malformed staged key \
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
                                "rederive_base_index: pre-tx record decode failed \
                                 (table_token={table_token}, rid={rid:?}): {e}"
                            )))
                        })?;
                        let ops = mgr.plan_record_deleted(&rid, &old_rec).await.map_err(|e| {
                            TxError::Storage(DbError::Internal(format!(
                                "rederive_base_index: plan_record_deleted failed: {e}"
                            )))
                        })?;
                        appended.extend(ops.into_iter().map(|op| (table_token, op)));
                        let unique_ops = mgr
                            .plan_record_deleted_unique(&rid, &old_rec)
                            .await
                            .map_err(|e| {
                                TxError::Storage(DbError::Internal(format!(
                                    "rederive_base_index: plan_record_deleted_unique failed: {e}"
                                )))
                            })?;
                        appended.extend(unique_ops.into_iter().map(|op| (table_token, op)));
                    }
                }
            }
        }

        // Append the re-derived ops FIRST (so they're in the set during the
        // 2c retain below — since they're planned against current live defs,
        // they pass the filter and survive).
        if !appended.is_empty() {
            tx.index_write_set.extend(appended);
        }

        // ── Sub-bug 2c / R0-B (#1008): retract stale staged base_index ops ────
        //
        // An index that existed at stage time (contributing ops to
        // `index_write_set`) but was DROP'd before commit — or DROP'd and
        // then CREATE'd again under the SAME name with a DIFFERENT
        // definition (ABA) — leaves orphan/wrong-instance ops that would
        // resurrect a posting for a gone index, or contaminate a NEW index
        // with postings computed against the OLD definition's fields.
        //
        // R0-B replaces the previous byte-length/name `(is_unique,
        // name_interned)` heuristic — which could not distinguish an ABA
        // replacement from the SAME live instance — with the uniform
        // `(name_interned, instance_epoch)` provenance check (see
        // `retract_stale_provenance_ops`'s doc). Every current definition's
        // `instance_epoch` is a FRESH value on CREATE (and bumped on
        // RENAME), so a staged op from a definition that no longer exists —
        // or was replaced — never matches `live` and is retracted; a staged
        // op from a definition that is STILL the same live instance always
        // matches and survives.
        let live_regular: shamir_collections::TFxSet<(u64, u64)> = mgr
            .iter_indexes()
            .map(|def| (def.name_interned, def.instance_epoch))
            .collect();
        let live_unique: shamir_collections::TFxSet<(u64, u64)> = mgr
            .iter_unique_indexes()
            .map(|def| (def.name_interned, def.instance_epoch))
            .collect();
        retract_stale_provenance_ops(
            tx,
            table_token,
            shamir_tx::IndexFamily::Regular,
            &live_regular,
        );
        retract_stale_provenance_ops(
            tx,
            table_token,
            shamir_tx::IndexFamily::Unique,
            &live_unique,
        );
    }

    Ok(())
}

use shamir_collections::{TFxMap, TFxSet};
use shamir_storage::error::DbError;
use shamir_storage::types::KvOp;
use shamir_tx::{IndexFamily, IndexWriteOp, TxContext};
use shamir_types::types::record_id::RecordId;
use shamir_types::types::value::InnerValue;

use crate::commit::core::RepoCore;
use crate::commit::TxError;

/// #1100 — Re-derive posting ops for records whose value changed concurrently
///
/// This function closes the gap where a DELETE or UPDATE transaction plans index
/// removal operations based on its own snapshot view of a record, rather than
/// the record's ACTUAL current durable value. If another concurrent transaction
/// updated the record's indexed fields after this transaction's snapshot was taken,
/// the planner computed the removal key from the OLD value (seen in the snapshot),
/// not the ACTUAL current value. This means the correct removal op was never
/// generated at all, leaving a dangling posting.
///
/// The approach:
/// 1. For each staged Remove operation (DELETE): re-plan removal ops based on the
///    CURRENT durable value, then append any ops that weren't already staged.
/// 2. For each staged Set operation (UPDATE): re-plan the FULL diff (Remove + Set)
///    based on the CURRENT durable value as the "old" half of the diff, then
///    append all resulting ops (deduplicating against staged base_index ops to
///    avoid duplicates, since base_index ops don't include instance_epoch).
///
/// This mirrors `rederive_base_index_ops_post_stage` but is triggered by VALUE changes
/// (detected by comparing planned ops), not DEFINITION changes (detected by generation).
pub(in crate::tx) async fn rederive_stale_value_ops_post_stage(
    tx: &mut TxContext,
    repo: &RepoCore,
) -> Result<(), TxError> {
    // Skip if no base_index tables were touched (zero-cost on common path)
    if tx.base_index_stage_gens.is_empty() {
        return Ok(());
    }

    // #1110: Repo-global staleness gate — if nothing in the repo has even
    // STARTED a commit since this tx opened (version allocation high-water mark
    // not beyond snapshot_version), NO staged row's snapshot value can be stale.
    // One atomic Acquire load. Conservative: a commit on a DIFFERENT table still
    // defeats the fast path (false positive), but never wrong (no false negatives).
    // This is acceptable because the common case is a quiet repo, and a repo-global
    // signal is the only cheap signal available (per-table commit watermarks do not
    // exist).
    //
    // HAPPENS-BEFORE PROOF (why `version_allocation_high_water_mark` is sound):
    //
    // Let Tx1 be a reader with snapshot_version = V, and Tx2 be a concurrent writer.
    // Tx1 observes `gate.version_allocation_high_water_mark() <= V` and skips
    // re-derivation. We must prove that Tx1 cannot see stale values from Tx2.
    //
    // Tx2's program order (single thread, `commit_lock`-serialized against every
    // other writer, but that fact is not needed for this proof):
    //   1. `assign_next_version_guarded()` → `version_counter.fetch_add(1, AcqRel)`,
    //      allocating version V2. AcqRel (NOT Relaxed — see `assign_next_version`'s
    //      doc; a Relaxed RMW never synchronizes-with anything) means this store
    //      has Release semantics.
    //   2. Phase 5a/5c: Tx2 writes data + postings to the MVCC store at V2.
    //
    // Tx1's gate check does `version_counter.load(Acquire)`. By the C++/Rust
    // memory model, an `Acquire` load that reads the value written by a
    // `Release` (or `AcqRel`) RMW SYNCHRONIZES-WITH that RMW — this is the one
    // and only mechanism that establishes happens-before across threads here
    // (program order alone only orders operations on the SAME thread).
    //
    // Case A: Tx1's load returns a value `<= V` (the gate's fast-path condition).
    // Since `fetch_add` is monotonically increasing and Tx2's fetch_add produced
    // `V2 = V_prev + 1`, Tx1's load returning `<= V` means it did NOT read the
    // value Tx2's fetch_add produced (or any later one) — so Tx1's load and
    // Tx2's fetch_add are NOT related by synchronizes-with in this execution,
    // and per the Rust memory model Tx1 is NOT guaranteed to observe ANY of
    // Tx2's subsequent writes (step 2) either — the model provides no ordering
    // guarantee for operations with no synchronizes-with edge between them.
    // Read the contrapositive: for Tx1 to legitimately observe Tx2's step-2
    // writes without a synchronizes-with edge would itself be undefined
    // behavior this fix does not need to reason about further — the design
    // relies on `read_one_tx_bytes`'s own MVCC read path (a SEPARATE,
    // independently-synchronized channel, not this gate) never doing that.
    // The gate's job is narrower: prove that WHEN it says "skip", the skip is
    // safe — and Case A shows the gate's load did not synchronize-with any
    // writer whose version exceeds V, which is exactly the condition under
    // which no staged row could have been staled by that writer.
    //
    // Therefore: `version_allocation_high_water_mark() <= V` ⇒ no concurrent
    // writer's `fetch_add` synchronizes-with this load ⇒ (by the AcqRel/Acquire
    // pairing being the sole cross-thread ordering channel between the gate and
    // `assign_next_version`) skipping re-derivation is sound. □
    //
    // Relationship to the PRIOR `last_committed()`-based gate (#1107, round 1)
    // — corrected understanding (#1110 round 2, after the required pause-seam
    // reproduction could not be built — see that task's follow-up notes):
    // `last_committed()`'s only DOCUMENTED advance point is Phase 6
    // (`version_guard.commit()`), which would put the false-negative window
    // at the entire Phase 5a/5c→Phase 6 span. But for any table backed by an
    // attached `MvccStore` (the tested/common case — `mvcc_store/mvcc_history.rs`'s
    // `apply_committed_visible`, called from Phase 5a's `apply_data_batch`),
    // `last_committed_version` is ALSO published EAGERLY at Phase 5a itself,
    // via a direct `gate.publish_committed_max(commit_version)` call — that
    // file's own comment states this is intentionally "safe to call
    // redundantly" alongside Phase 6's publish. Empirically (a deterministic
    // pause-seam parked strictly between Phase 5c and Phase 6, then removed
    // once this was understood — see git history) this means the specific
    // interleaving originally described does NOT reproduce for MvccStore-backed
    // tables: `last_committed` is already correct by the time a concurrent
    // reader's gate check runs.
    //
    // The version-allocation-based gate here is STILL the right primitive to
    // use, for two independent reasons, not because the original interleaving
    // was confirmed: (1) allocation strictly precedes ANY write in every
    // commit path (Phase 3, before Phase 4's WAL write and Phase 5's data/index
    // writes), so gating on it doesn't depend on which of possibly-several
    // publish call sites a given write path happens to route through — the
    // `base.transact` fallback path (tables with no `MvccStore` attached,
    // `commit_phases.rs::apply_data_batch`'s `None` arm) has NO equivalent
    // early-publish call, so for THAT path the original Phase-6-only window
    // may still be real (not separately confirmed here — tracked as an open
    // question, not asserted as a live bug); (2) it is a simpler, single-fact
    // invariant ("nothing has started writing since I opened") vs. relying on
    // exactly when a given write path happens to publish. Round 1 of THIS
    // task's own delivery repeated a DIFFERENT class of error one level down:
    // it kept the writer's `fetch_add` at `Relaxed` while claiming the
    // `Acquire` reader synchronized with it — a `Relaxed` store cannot
    // synchronize-with anything, so that claim was false regardless of the
    // above. Both the writer (`AcqRel`) and reader (`Acquire`) sides must use
    // a genuine Release/Acquire pairing for this proof to hold — see
    // `assign_next_version`'s doc.
    let gate = repo.tx_gate().await?;
    if gate.version_allocation_high_water_mark() <= tx.snapshot_version {
        log::debug!(
            "rederive_stale_value_ops_post_stage: fast-path skip (repo quiet, snapshot_version={}, alloc_hwm={})",
            tx.snapshot_version,
            gate.version_allocation_high_water_mark()
        );
        return Ok(());
    }

    let stage_gens: Vec<(u64, u64)> = tx
        .base_index_stage_gens
        .iter()
        .map(|(t, g)| (*t, *g))
        .collect();

    for (table_token, _stage_gen) in stage_gens {
        let Some(tbl) = repo.table_by_token_if_live(table_token).await else {
            continue;
        };
        let mgr = tbl.index_manager_ref();

        // P1 perf fix: borrow-iterate staged ops for this table instead of
        // collecting into an owned `Vec<KvOp>` — this loop only ever READS
        // `tx.index_write_set` (a disjoint field from `tx.write_set`), never
        // mutates `tx` until after the loop (the `tx.index_write_set.extend`
        // below), so holding the `staging` borrow across the per-record async
        // planning here is sound and avoids an O(staged) allocation that would
        // otherwise be paid once per table regardless of staged-set size.
        let Some(staging) = tx.write_set.get(&table_token) else {
            continue;
        };

        let mut appended: Vec<(u64, IndexWriteOp)> = Vec::new();

        // P1 perf fix (O(N²·K) → O(N·K)): the dedup lookups below used to be
        // rebuilt from scratch (`staged_removals_by_rid`) or linearly rescanned
        // (`.any()`) once PER re-planned op — i.e. once per staged Remove/Set
        // for this table. The gate above fires under ANY concurrent write
        // traffic, so a bulk DELETE/UPDATE of N indexed rows paid O(N²·K) work
        // here inside the locked pre-commit validate phase — the same class
        // #1099/#1108/#1111 already fixed next door in `released_unique_cache`.
        // Build every lookup structure ONCE per table, before the per-record
        // loop, and keep them updated as `appended` grows (`record_staged_op`)
        // so a later op in the SAME table's loop also sees ops derived earlier
        // in this same pass, not just what was already in `tx.index_write_set`
        // before this function started.
        let mut staged_removals_by_rid: TFxMap<RecordId, TFxSet<bytes::Bytes>> = TFxMap::default();
        let mut staged_regular_remove_keys: TFxSet<bytes::Bytes> = TFxSet::default();
        let mut staged_regular_set_keys: TFxSet<bytes::Bytes> = TFxSet::default();
        let mut staged_unique_remove_by_key_owner: TFxSet<(bytes::Bytes, [u8; 16])> =
            TFxSet::default();
        let mut staged_unique_set_by_key_value: TFxSet<(bytes::Bytes, bytes::Bytes)> =
            TFxSet::default();
        for (_, op) in tx.index_write_set.iter().filter(|(t, _)| *t == table_token) {
            record_staged_op(
                op,
                &mut staged_removals_by_rid,
                &mut staged_regular_remove_keys,
                &mut staged_regular_set_keys,
                &mut staged_unique_remove_by_key_owner,
                &mut staged_unique_set_by_key_value,
            );
        }

        for kvop in staging.iter_ops() {
            match kvop {
                KvOp::Remove(k) => {
                    // DELETE case: re-plan removal ops based on CURRENT durable value
                    let rid = RecordId::try_from_bytes(&k).ok_or_else(|| {
                        TxError::Storage(DbError::Internal(format!(
                            "rederive_stale_value_ops: malformed staged key \
                             (table_token={table_token}, {} bytes) — expected a \
                             16-byte RecordId",
                            k.len()
                        )))
                    })?;

                    // Read the CURRENT MVCC-visible value (bypasses tx snapshot)
                    if let Some(current_bytes) = tbl.read_one_tx_bytes(rid, None).await? {
                        log::debug!(
                            "rederive_stale_value_ops: DELETE rid={:?}, current_bytes.len()={}",
                            rid,
                            current_bytes.len()
                        );

                        // Decode the current durable value
                        let current_rec = InnerValue::from_bytes(&current_bytes).map_err(|e| {
                            TxError::Storage(DbError::Codec(format!(
                                "rederive_stale_value_ops: current record decode failed \
                                 (table_token={table_token}, rid={rid:?}): {e}"
                            )))
                        })?;

                        // Re-plan removal ops for the CURRENT value
                        let current_removals = mgr
                            .plan_record_deleted(&rid, &current_rec)
                            .await
                            .map_err(|e| {
                                TxError::Storage(DbError::Internal(format!(
                                    "rederive_stale_value_ops: plan_record_deleted failed: {e}"
                                )))
                            })?;

                        let current_unique_removals = mgr
                            .plan_record_deleted_unique(&rid, &current_rec)
                            .await
                            .map_err(|e| {
                                TxError::Storage(DbError::Internal(format!(
                                    "rederive_stale_value_ops: plan_record_deleted_unique failed: {e}"
                                )))
                            })?;

                        log::debug!(
                            "rederive_stale_value_ops: DELETE rid={:?}, current_removals.len()={}, current_unique_removals.len()={}",
                            rid,
                            current_removals.len(),
                            current_unique_removals.len()
                        );

                        // Append any ops that weren't already staged
                        for op in current_removals.into_iter().chain(current_unique_removals) {
                            // Bug B fix: handle both owner:Some (unique) and owner:None (regular)
                            // P1 perf fix: O(1) lookup against the precomputed cache instead of
                            // a linear `.any()` rescan of `tx.index_write_set` per op.
                            let is_staged = match &op {
                                IndexWriteOp::RemovePosting {
                                    ref key,
                                    owner,
                                    provenance,
                                } => {
                                    if provenance.family == IndexFamily::Regular {
                                        // For regular indexes: posting_key = index_key || record_id
                                        // The posting key itself uniquely identifies this record's claim
                                        staged_regular_remove_keys.contains(key)
                                    } else {
                                        // For unique indexes: dedup by (key, owner)
                                        if let Some(ref owner_bytes) = owner {
                                            if let Some(rid) =
                                                RecordId::try_from_bytes(owner_bytes.as_slice())
                                            {
                                                staged_removals_by_rid
                                                    .get(&rid)
                                                    .map(|keys| keys.contains(key))
                                                    .unwrap_or(false)
                                            } else {
                                                false
                                            }
                                        } else {
                                            false
                                        }
                                    }
                                }
                                _ => continue,
                            };

                            if !is_staged {
                                log::debug!(
                                    "rederive_stale_value_ops: appending missing RemovePosting \
                                     for DELETE (table_token={}, op={:?})",
                                    table_token,
                                    op
                                );
                                record_staged_op(
                                    &op,
                                    &mut staged_removals_by_rid,
                                    &mut staged_regular_remove_keys,
                                    &mut staged_regular_set_keys,
                                    &mut staged_unique_remove_by_key_owner,
                                    &mut staged_unique_set_by_key_value,
                                );
                                appended.push((table_token, op));
                            } else {
                                log::debug!(
                                    "rederive_stale_value_ops: RemovePosting already staged \
                                     for DELETE (table_token={}, op={:?})",
                                    table_token,
                                    op
                                );
                            }
                        }
                    }
                    // If current value is None, the record was deleted concurrently —
                    // nothing to re-derive for this DELETE (it's already gone).
                }
                KvOp::Set(k, v) => {
                    // UPDATE case: re-plan removal ops based on CURRENT durable value
                    let rid = RecordId::try_from_bytes(&k).ok_or_else(|| {
                        TxError::Storage(DbError::Internal(format!(
                            "rederive_stale_value_ops: malformed staged key \
                             (table_token={table_token}, {} bytes) — expected a \
                             16-byte RecordId",
                            k.len()
                        )))
                    })?;

                    // Read the CURRENT MVCC-visible value (the ACTUAL old value)
                    if let Some(old_bytes) = tbl.read_one_tx_bytes(rid, None).await? {
                        // Decode the current durable value
                        let old_rec = InnerValue::from_bytes(&old_bytes).map_err(|e| {
                            TxError::Storage(DbError::Codec(format!(
                                "rederive_stale_value_ops: old record decode failed \
                                 (table_token={table_token}, rid={rid:?}): {e}"
                            )))
                        })?;

                        // Decode the new value (what the tx is setting)
                        let new_rec = InnerValue::from_bytes(&v).map_err(|e| {
                            TxError::Storage(DbError::Codec(format!(
                                "rederive_stale_value_ops: new record decode failed \
                                 (table_token={table_token}, rid={rid:?}): {e}"
                            )))
                        })?;

                        // Re-plan update ops using the CURRENT old value
                        // This generates both RemovePosting (for fields that changed)
                        // and SetPosting (for the new values), which is correct even when
                        // the record's actual current value differs from what the tx's snapshot saw.
                        let updated_ops = mgr
                            .plan_record_updated(&rid, &old_rec, &new_rec)
                            .await
                            .map_err(|e| {
                            TxError::Storage(DbError::Internal(format!(
                                "rederive_stale_value_ops: plan_record_updated failed: {e}"
                            )))
                        })?;

                        let updated_unique_ops = mgr
                            .plan_record_updated_unique(&rid, &old_rec, &new_rec)
                            .await
                            .map_err(|e| {
                                TxError::Storage(DbError::Internal(format!(
                                    "rederive_stale_value_ops: plan_record_updated_unique failed: {e}"
                                )))
                            })?;

                        // Append ALL re-planned ops (both Remove and Set)
                        // This correctly handles the case where concurrent changes
                        // mean the staged ops are based on stale data.
                        log::debug!(
                            "rederive_stale_value_ops: UPDATE rid={:?}, appending {} regular + {} unique ops based on CURRENT value",
                            rid,
                            updated_ops.len(),
                            updated_unique_ops.len()
                        );
                        for op in updated_ops.into_iter().chain(updated_unique_ops) {
                            // Bug A fix: make dedup owner-aware and kind-aware for unique indexes
                            // P1 perf fix: O(1) lookups against the precomputed caches instead of
                            // a linear `.any()` rescan of `tx.index_write_set` per op. Each arm
                            // below searches exactly the same (family, op-kind)-bucketed subset the
                            // original nested match reduced to — see this function's precompute
                            // block for how the caches are populated.
                            let is_staged = match &op {
                                IndexWriteOp::RemovePosting {
                                    key,
                                    owner: Some(owner_bytes),
                                    provenance,
                                } if provenance.family == IndexFamily::Unique => {
                                    staged_unique_remove_by_key_owner
                                        .contains(&(key.clone(), *owner_bytes))
                                }
                                IndexWriteOp::RemovePosting {
                                    key, provenance, ..
                                } if provenance.family == IndexFamily::Regular => {
                                    staged_regular_remove_keys.contains(key)
                                }
                                IndexWriteOp::SetPosting {
                                    key, provenance, ..
                                } if provenance.family == IndexFamily::Regular => {
                                    staged_regular_set_keys.contains(key)
                                }
                                IndexWriteOp::SetPosting {
                                    key,
                                    value,
                                    provenance,
                                } if provenance.family == IndexFamily::Unique => {
                                    staged_unique_set_by_key_value
                                        .contains(&(key.clone(), value.clone()))
                                }
                                _ => false,
                            };

                            if !is_staged {
                                log::debug!(
                                    "rederive_stale_value_ops: appending op for UPDATE (table_token={}, rid={:?}, op={:?})",
                                    table_token,
                                    rid,
                                    op
                                );
                                record_staged_op(
                                    &op,
                                    &mut staged_removals_by_rid,
                                    &mut staged_regular_remove_keys,
                                    &mut staged_regular_set_keys,
                                    &mut staged_unique_remove_by_key_owner,
                                    &mut staged_unique_set_by_key_value,
                                );
                                appended.push((table_token, op));
                            } else {
                                log::debug!(
                                    "rederive_stale_value_ops: op already staged for UPDATE (table_token={}, rid={:?}, op={:?})",
                                    table_token,
                                    rid,
                                    op
                                );
                            }
                        }
                    }
                    // If old value is None, this is actually an insert — no removal ops needed
                }
            }
        }

        // Append the re-derived ops
        if !appended.is_empty() {
            tx.index_write_set.extend(appended);
        }
    }

    Ok(())
}

/// P1 perf fix: incrementally maintains the O(1) staged-op lookup caches used
/// by [`rederive_stale_value_ops_post_stage`]'s DELETE/UPDATE dedup checks —
/// see that function's precompute block. Called once per PRE-EXISTING entry
/// in `tx.index_write_set` (build pass, once per table) and once per NEWLY
/// appended op (as `appended` grows during the per-record loop), so a later
/// op in the same table's pass also sees ops derived earlier in this same
/// pass, not just what was already staged before this function started.
fn record_staged_op(
    op: &IndexWriteOp,
    staged_removals_by_rid: &mut TFxMap<RecordId, TFxSet<bytes::Bytes>>,
    staged_regular_remove_keys: &mut TFxSet<bytes::Bytes>,
    staged_regular_set_keys: &mut TFxSet<bytes::Bytes>,
    staged_unique_remove_by_key_owner: &mut TFxSet<(bytes::Bytes, [u8; 16])>,
    staged_unique_set_by_key_value: &mut TFxSet<(bytes::Bytes, bytes::Bytes)>,
) {
    match op {
        IndexWriteOp::RemovePosting {
            key,
            owner,
            provenance,
        } => {
            if provenance.family == IndexFamily::Regular {
                staged_regular_remove_keys.insert(key.clone());
            } else if provenance.family == IndexFamily::Unique {
                if let Some(owner_bytes) = owner {
                    staged_unique_remove_by_key_owner.insert((key.clone(), *owner_bytes));
                    if let Some(rid) = RecordId::try_from_bytes(owner_bytes) {
                        staged_removals_by_rid
                            .entry(rid)
                            .or_default()
                            .insert(key.clone());
                    }
                }
            }
        }
        IndexWriteOp::SetPosting {
            key,
            value,
            provenance,
        } => {
            if provenance.family == IndexFamily::Regular {
                staged_regular_set_keys.insert(key.clone());
            } else if provenance.family == IndexFamily::Unique {
                staged_unique_set_by_key_value.insert((key.clone(), value.clone()));
            }
        }
        IndexWriteOp::BumpFtsStats { .. } => {}
    }
}

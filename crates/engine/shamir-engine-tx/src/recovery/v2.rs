//! V2 WAL recovery — applies inflight WalEntryV2 entries on repo open.
//!
//! Crashes between commit_tx Phase 4 (WAL begin) and the drainer's
//! Phase C (WAL truncation) leave entries in the WAL segments that
//! have not yet been materialised into history. Without recovery those
//! entries' tx writes would be lost — the sealed/active segments
//! survive the crash (level-2 page cache at minimum; level-3 if the
//! drainer had fsynced), and recovery replays them into history before
//! the repo is served. There are no per-entry KV "markers" post-F6;
//! truncation is a watermark advance over the segment set, gated by
//! the durable watermark + the A5 interner-hwm check.
//!
//! Per stage 7.1 plan in docs/dev-artifacts/pre-transactional/08-tests-landing.md.

use shamir_collections::TFxMap;
use shamir_storage::error::{DbError, DbResult};
use shamir_storage::types::{KvOp, RecordKey};
use shamir_tx::{CompletionState, RepoTxGate, RepoWalManager};
use shamir_wal::{WalEntryV2, WalOpV2};

use crate::commit::core::RepoCore;
use crate::pre_commit::unexpected_repo_interner_scope;
use crate::repo::RepoWritePermit;

use super::index_scope::validate_posting;

fn unresolved_table(token: u64) -> DbError {
    DbError::Validation(format!("strict WAL replay: unresolved table token {token}"))
}

fn check_index_route(idx_id: u32) -> DbResult<()> {
    if idx_id != 0 {
        return Err(DbError::Validation(format!(
            "strict WAL replay: unsupported index route {idx_id}"
        )));
    }
    Ok(())
}

/// Test-only injection (CRIT-1 / #435): when set to a non-zero `txn_id`,
/// `seed_version_cache_for_entry` returns a synthetic error for the matching
/// entry in place of the real `write_committed_to_history` call. This proves
/// the post-fix contract: a history-write failure during cold recovery is
/// FATAL to `open()` (the entry is NOT marked durable, recovery returns Err,
/// `add_repo`'s `recover_v2_inflight().await?` refuses to serve the repo).
///
/// Mirrors the `FAIL_VECTOR_DELTA_TX_ID` / `FAIL_VECTOR_PROMOTE_TX_ID`
/// pattern in `commit_phases.rs` (keyed by `txn_id`, atomically loaded).
/// Reset to 0 between tests to avoid cross-test bleed. Gated by
/// `#[cfg(debug_assertions)]` (same as `commit.rs::maybe_crash`) so a
/// `--release` production build pays zero cost — the static is absent and
/// the load branch in `seed_version_cache_for_entry` is elided. The external
/// `crash_recovery` integration harness (separate test binary that cannot
/// reach this `pub(crate)` static) additionally honors the
/// `SHAMIR_TEST_FAIL_HISTORY_SEED` env var to arm the same fault.
#[cfg(debug_assertions)]
pub(crate) static FAIL_HISTORY_SEED_TX_ID: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// cancel-safe: NO — multi-step state mutation. Each branch issues a
/// table lookup followed by one or more store writes (and the broadcast
/// branches loop across tables). Cancellation mid-broadcast leaves the
/// store in a partially-applied state; data-layer ops are idempotent so
/// a subsequent re-replay converges, but the caller cannot rely on
/// atomicity at this boundary.
///
/// Replay a single WalOpV2 against the given RepoInstance.
///
/// Stage 7.1.c: Put / Delete / CounterDelta are applied for real.
/// Stage 7.1.d: IndexPut / IndexDel are applied (table_id_interned=0
///   broadcasts to all tables' info_stores).
/// InternerOverlayMerge is deferred (Stage 5 — repo-level interner).
pub async fn replay_v2_op(op: &WalOpV2, repo: &RepoCore) -> DbResult<()> {
    let permit = repo
        .write_domain()
        .admit()
        .await
        .map_err(|error| DbError::Internal(error.to_string()))?;
    // S5c-X Round-11 (stack layout): the admitted body's future is enormous
    // (per-op table resolution reaches `get_table_admitted` → `get_table_ctx`
    // → `create_table_context` → `create_with_domain_admitted` inline). The
    // `#[inline(never)]` boxed dispatcher keeps that await state on the heap,
    // off every caller frame along the resume/recovery poll chain.
    Box::pin(replay_v2_op_admitted(op, repo, &permit, false)).await
}

#[inline(never)]
fn replay_v2_op_admitted<'a>(
    op: &'a WalOpV2,
    repo: &'a RepoCore,
    permit: &'a RepoWritePermit,
    strict: bool,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = DbResult<()>> + Send + 'a>> {
    Box::pin(replay_v2_op_admitted_inner(op, repo, permit, strict))
}

async fn replay_v2_op_admitted_inner(
    op: &WalOpV2,
    repo: &RepoCore,
    permit: &RepoWritePermit,
    strict: bool,
) -> DbResult<()> {
    repo.write_domain().debug_check_permit(permit);
    match op {
        WalOpV2::Put {
            table_id_interned,
            rid,
            body,
        } => {
            let tbl = match repo
                .table_by_token_admitted(*table_id_interned, permit)
                .await?
            {
                Some(t) => t,
                None if strict => return Err(unresolved_table(*table_id_interned)),
                None => {
                    log::warn!(
                        "replay_v2_op Put: table token {} not found in repo {}; \
                         skipping (table may have been dropped)",
                        table_id_interned,
                        repo.name()
                    );
                    return Ok(());
                }
            };
            // Collapse-main: attached tables (with an MvccStore) are recovered
            // into the version log by `seed_version_cache_for_entry` (writes
            // `history.set(key‖v, body)`); the raw `data_store` is vestigial for
            // them, so skip the redundant write. Only unattached tables (no
            // MvccStore — system/test) still materialize into `data_store`.
            if tbl.mvcc_store().is_none() {
                tbl.data_store()
                    .set(RecordKey::from_slice(rid.as_bytes()), body.clone())
                    .await?;
            }
            Ok(())
        }
        WalOpV2::Delete {
            table_id_interned,
            rid,
        } => {
            let tbl = match repo
                .table_by_token_admitted(*table_id_interned, permit)
                .await?
            {
                Some(t) => t,
                None if strict => return Err(unresolved_table(*table_id_interned)),
                None => {
                    log::warn!(
                        "replay_v2_op Delete: table token {} not found; skipping",
                        table_id_interned
                    );
                    return Ok(());
                }
            };
            // Collapse-main: attached tables record the delete tombstone into
            // the version log via `seed_version_cache_for_entry`
            // (`history.set(key‖v, EMPTY)`); the raw `data_store` is vestigial,
            // so skip the redundant remove. Only unattached tables (no
            // MvccStore) still mutate `data_store`.
            //
            // Delete-replay is idempotent: a key already gone (the delete
            // landed before the crash, or the record never existed) is
            // benign and must not fail recovery. But a genuine storage
            // I/O error must propagate — swallowing it would report a
            // successful replay having NOT applied the delete.
            if tbl.mvcc_store().is_none() {
                match tbl.data_store().remove(rid.to_bytes().into()).await {
                    Ok(_) | Err(DbError::NotFound(_)) => {}
                    Err(e) => return Err(e),
                }
            }
            Ok(())
        }
        WalOpV2::CounterDelta {
            table_id_interned, ..
        } => {
            if strict
                && (*table_id_interned == 0
                    || repo
                        .table_by_token_admitted(*table_id_interned, permit)
                        .await?
                        .is_none())
            {
                return Err(unresolved_table(*table_id_interned));
            }
            // CRIT-3 idempotency (Option A):
            //
            // The happy-path commit (`commit::commit_tx_inner` Phase 5b)
            // now applies counter deltas in-memory BEFORE writing the
            // WAL commit marker. If the marker survives a crash mid-
            // Phase 7, the in-memory counter has already advanced —
            // replaying the delta here would double-count.
            //
            // Skipping the replay accepts a worst-case drift of one
            // tx-worth of counter delta when the crash falls between
            // Phase 5b (counter applied in RAM) and Phase 6.5 (marker
            // persistence). On restart the in-memory `RecordCounter`
            // re-hydrates from the info_store, which only reflects
            // whatever was durably persisted before the crash. The
            // authoritative durability is `last_committed_version`
            // (Phase 6.5) — the record counter is a metric.
            //
            // The cleaner Option B (write a per-tx "counter-applied"
            // marker that recovery checks) is not worth the I/O for
            // an at-most-by-one drift on a counter that the doctor
            // already reconciles by full data-store scan.
            Ok(())
        }
        WalOpV2::IndexPut {
            table_id_interned,
            idx_id,
            key,
            value,
        } => {
            if strict {
                check_index_route(*idx_id)?;
            }
            if *table_id_interned != 0 {
                let tbl = match repo
                    .table_by_token_admitted(*table_id_interned, permit)
                    .await?
                {
                    Some(t) => t,
                    None if strict => return Err(unresolved_table(*table_id_interned)),
                    None => {
                        log::warn!(
                            "replay_v2_op IndexPut: table token {} not found",
                            table_id_interned
                        );
                        return Ok(());
                    }
                };
                if strict {
                    Box::pin(validate_posting(&tbl, key)).await?;
                }
                tbl.info_store()
                    .set(key.clone().into(), value.clone())
                    .await?;
                return Ok(());
            }
            // Broadcast (table_id_interned == 0): the same key is set in
            // every table's info_store. Audit §2.5: the old code swallowed
            // errors asymmetrically (`let _ = ...set(...)`) while the
            // neighboring IndexDel branch propagated them — a broadcast
            // IndexPut failure during recovery was silently lost. Now we
            // attempt EVERY table (best-effort: a failure on one does NOT
            // skip the remaining tables) and capture the FIRST error to
            // return, mirroring the `flush_buffers` / `seed_version_cache_
            // for_entry` first-err pattern already used in this crate.
            let mut first_err: Option<DbError> = None;
            let names = repo.list_table_names();
            if strict && names.is_empty() {
                return Err(unresolved_table(0));
            }
            for name in names {
                match repo.get_table_admitted(&name, permit).await {
                    Ok(tbl) => {
                        if strict {
                            Box::pin(validate_posting(&tbl, key)).await?;
                        }
                        if let Err(e) = tbl
                            .info_store()
                            .set(key.clone().into(), value.clone())
                            .await
                        {
                            log::warn!(
                                "replay_v2_op broadcast IndexPut: info_store.set failed for \
                                 table {name}: {e}"
                            );
                            if first_err.is_none() {
                                first_err = Some(e);
                            }
                        }
                    }
                    Err(e) if strict => return Err(e),
                    Err(e) => {
                        log::warn!(
                            "replay_v2_op broadcast IndexPut: get_table failed for \
                             table {name}: {e}; skipping this table"
                        );
                    }
                }
            }
            if let Some(e) = first_err {
                return Err(e);
            }
            Ok(())
        }
        WalOpV2::IndexDel {
            table_id_interned,
            idx_id,
            key,
        } => {
            if strict {
                check_index_route(*idx_id)?;
            }
            if *table_id_interned != 0 {
                let tbl = match repo
                    .table_by_token_admitted(*table_id_interned, permit)
                    .await?
                {
                    Some(t) => t,
                    None if strict => return Err(unresolved_table(*table_id_interned)),
                    None => {
                        log::warn!(
                            "replay_v2_op IndexDel: table token {} not found",
                            table_id_interned
                        );
                        return Ok(());
                    }
                };
                if strict {
                    Box::pin(validate_posting(&tbl, key)).await?;
                }
                // Idempotent like Delete: a missing posting is benign,
                // but a real storage error must propagate rather than be
                // swallowed as a phantom success.
                match tbl.info_store().remove(key.clone().into()).await {
                    Ok(_) | Err(DbError::NotFound(_)) => {}
                    Err(e) => return Err(e),
                }
                return Ok(());
            }
            // Broadcast (table_id_interned == 0): the same key is removed
            // from every table's info_store. Most tables never held it, so
            // NotFound is the expected norm and benign — but a genuine I/O
            // error on any table propagates.
            let names = repo.list_table_names();
            if strict && names.is_empty() {
                return Err(unresolved_table(0));
            }
            for name in names {
                match repo.get_table_admitted(&name, permit).await {
                    Ok(tbl) => {
                        if strict {
                            Box::pin(validate_posting(&tbl, key)).await?;
                        }
                        match tbl.info_store().remove(key.clone().into()).await {
                            Ok(_) | Err(DbError::NotFound(_)) => {}
                            Err(e) => return Err(e),
                        }
                    }
                    Err(e) if strict => return Err(e),
                    Err(e) => {
                        log::warn!(
                            "replay_v2_op broadcast IndexDel: get_table failed for \
                             table {name}: {e}; skipping this table"
                        );
                    }
                }
            }
            Ok(())
        }
        WalOpV2::InternerOverlayMerge { entries } => {
            // Stage I: the interner is per-REPO, so the old per-table
            // broadcast collapses to a single merge against the repo
            // interner. `touch_ind` is idempotent — interning a key that
            // already exists returns the existing id. (Pre-Stage-I this
            // looped over every table because each had its own interner;
            // now they all share one.)
            //
            // Audit §2.5: the old code swallowed every error here
            // (`if let Ok(...)`, `let _ = touch_ind`, `let _ = persist`)
            // — an interner-merge failure during recovery was silently
            // lost, with the same consequences as CRIT-2 / audit §1.2
            // (history records referencing ids the persistent interner
            // never absorbed). Now every error propagates: a failed
            // `touch_ind` or `persist` fails recovery (the caller
            // `recover_inflight_v2` already has an A11 persist gate, but
            // this op-level merge must also surface failures so a
            // mid-replay interner error is not silently absorbed).
            let repo_interner = repo.repo_interner().await?;
            let interner = repo_interner.get().await?;
            for (_overlay_id, key_str) in entries {
                // RU8: recovery replay is counted but never quota-rejected.
                interner.touch_ind_unmetered(key_str).map_err(|e| {
                    DbError::Internal(format!(
                        "replay InternerOverlayMerge: touch_ind({key_str:?}) failed: {e}"
                    ))
                })?;
            }
            repo_interner.persist().await.map_err(|e| {
                DbError::Internal(format!(
                    "replay InternerOverlayMerge: interner persist failed: {e}"
                ))
            })?;
            Ok(())
        }
    }
}

/// cancel-safe: NO — iterates inflight entries, replaying each. Replay
/// ops are idempotent at the data layer so eventual convergence is fine
/// (entries stay in the segment until F6 truncation and the next
/// recovery replays them again), but the function itself is not safe to
/// drop mid-flight as a single atomic step.
///
/// Walk all inflight V2 WAL entries for the repo and replay each one.
///
/// HIGH-5: entries are sorted by `commit_version` ascending before
/// replay so the order in which post-crash recovery applies multi-tx
/// state matches the order the original commit pipeline assigned.
/// `wal.recover()` returns entries in WAL append order, but txn_id
/// and commit_version are allocated independently — two concurrent
/// transactions can commit out of append order. Without this sort,
/// last-write-wins ops (Put/IndexPut) would resolve to the wrong
/// final value.
///
/// Legacy entries written before HIGH-5 carry `commit_version = 0`;
/// they sort first, preserving the previous lexical-key behaviour
/// on mixed-version corpora.
///
/// Called from `RepoInstance::recover_v2_inflight`.
///
/// CRIT-B: restoring MVCC version state.
///
/// 1. Force gate construction up front (`repo.tx_gate()`). The gate seeds
///    its `version_counter`/`last_committed_version` floor from
///    `max(persisted marker, max inflight commit_version)` (see
///    `RepoInstance::tx_gate`), so `assign_next_version()` is already
///    guaranteed to exceed every version the entries below replay at —
///    this is the part that fixes the monotonicity violation, because the
///    gate exposes no after-the-fact counter setter.
/// 2. Replay all inflight entries (sorted by `commit_version`, HIGH-5)
///    and remove their markers.
/// 3. Persist `last_committed = gate.last_committed()` so the recovered
///    floor survives the *next* restart. Once step 2 clears the markers,
///    the inflight pre-scan in step 1 would find nothing, so the durable
///    marker must now carry the recovered max or the floor would rewind.
pub async fn recover_inflight_v2(repo: &RepoCore, permit: &RepoWritePermit) -> DbResult<usize> {
    repo.write_domain().debug_check_permit(permit);
    // Step 1: build the gate before replay so its version floor is seeded
    // from the inflight commit_versions while the markers still exist.
    let gate = Box::pin(repo.tx_gate()).await?;

    let wal = Box::pin(repo.repo_wal()).await?;
    let mut entries = wal.recover().await?;
    entries.sort_by_key(|e| e.commit_version);
    let count = Box::pin(recover_entries(
        repo,
        permit,
        &entries,
        &gate,
        Some(&wal),
        None,
    ))
    .await?;

    // PR8 completed_reseed chunk 2: cold-start history-epoch bump. An Armed
    // lifecycle record with no later Clean certificate means the previous
    // serve period ended uncleanly — the bump costs followers a re-seed,
    // regardless of the CURRENT serve mode (review finding 2a: the Armed
    // obligation survives a config change to `durable`). A failed
    // bump/flush propagates and blocks admission.
    Box::pin(repo.note_unclean_open_bump(permit)).await?;
    Ok(count)
}

/// Replay the facade's validated entries on an exclusively opened offline copy.
/// Does not retire WAL entries or perform the boot-only history epoch bump.
/// The caller must prepare the exact gate and an offline WAL configuration
/// before calling; cold certification can consult the repo's WAL group.
/// cancel-safe: NO
pub(crate) async fn recover_inflight_v2_strict(
    repo: &RepoCore,
    permit: &RepoWritePermit,
    mut entries: Vec<WalEntryV2>,
    v_star: u64,
) -> DbResult<usize> {
    repo.write_domain().debug_check_permit(permit);
    let gate = repo.initialized_tx_gate().ok_or_else(|| {
        DbError::Validation("strict WAL replay: offline transaction gate not initialized".into())
    })?;
    check_gate_version(gate, v_star)?;
    entries.sort_by_key(|entry| entry.commit_version);
    let count = Box::pin(recover_entries(
        repo,
        permit,
        &entries,
        gate,
        None,
        Some(v_star),
    ))
    .await?;
    check_gate_version(gate, v_star)?;
    Ok(count)
}

fn check_gate_version(gate: &RepoTxGate, v_star: u64) -> DbResult<()> {
    let committed = gate.last_committed();
    let allocated = gate.version_allocation_high_water_mark();
    if committed != v_star || allocated != v_star {
        return Err(DbError::Validation(format!(
            "strict WAL replay: expected version {v_star}, got committed {committed}, allocated {allocated}"
        )));
    }
    Ok(())
}

async fn recover_entries(
    repo: &RepoCore,
    permit: &RepoWritePermit,
    entries: &[WalEntryV2],
    gate: &RepoTxGate,
    boot_wal: Option<&RepoWalManager>,
    strict_point: Option<u64>,
) -> DbResult<usize> {
    let strict = strict_point.is_some();
    let count = entries.len();

    // Track the highest replayed commit_version for logging / assertion.
    // entries are sorted ascending so the last one carries the max, but
    // fold defensively in case of legacy commit_version == 0 entries.
    let mut max_replayed = 0u64;

    // A11 (audit finding): the OLD loop did `replay_v2_entry` →
    // `gate.mark`/`mark_durable` → `wal.commit` in ONE pass. That finalized
    // every entry (advance durable watermark, no-op marker commit, seed
    // version cache into durable history) WITHOUT ever persisting the
    // interner deltas the entries carried — `replay_v2_entry`'s
    // `interner.touch_with_id` populates the IN-MEMORY interner only.
    //
    // A subsequent crash before the next background interner checkpoint
    // would lose every replayed (name, id) mapping: the in-memory interner
    // of the now-dead process is gone, and the persistent chunk store never
    // learned the ids — but the history records (written during
    // `replay_v2_entry`, which IS durable) still reference them, leaving
    // them permanently undecodable. This is the recovery-path analogue of
    // the drainer's CRIT-2 (#436): the drainer carefully gates WAL
    // truncation on `interner_delta_safe_to_truncate` (the A5 gate), but
    // `recover_inflight_v2` had NO equivalent protection.
    //
    // Fix (audit fix sketch option 1 — "persist once"): split the loop into
    // two phases so every entry's interner delta is applied to the in-memory
    // interner FIRST (Phase A), then force ONE `repo_interner.persist()`
    // (Phase B) to durably checkpoint every replayed mapping BEFORE any entry
    // is finalized (Phase C: mark / mark_durable / wal.commit). Recovery is a
    // COLD, infrequent path — unlike the drainer's hot path, there is no perf
    // reason to avoid an eager persist, and a single persist covers every
    // entry's delta in one shot. `persist()` is a no-op when nothing new was
    // added (the common no-delta / already-covered cases), so the regression
    // guards stay green without special-casing.
    //
    // If `persist()` fails, propagate the error WITHOUT finalizing any entry
    // — mirroring the drainer's "conservatively retain" behavior. The
    // entries stay inflight (their WAL markers / segments are untouched
    // because Phase C never ran), so the next recovery re-replays them
    // idempotently and retries the persist. `open()`'s
    // `recover_v2_inflight().await?` then refuses to serve a repo that could
    // not durably absorb its recovered interner deltas.

    // Phase A: replay every entry (data ops + interner deltas into memory).
    // `replay_v2_entry` is idempotent at the data layer so re-replay on a
    // persist failure converges. We capture (txn_id, commit_version) per
    // entry for Phase C so we don't need to hold the entries themselves.
    let mut finalized: Vec<(u64 /* txn_id */, u64 /* commit_version */)> =
        Vec::with_capacity(entries.len());
    for entry in entries {
        max_replayed = max_replayed.max(entry.commit_version);
        replay_v2_entry_admitted(entry, repo, permit, strict).await?;
        finalized.push((entry.txn_id, entry.commit_version));
    }

    // Identity-migration post-replay re-run (design P34-12 (c),
    // "Pre-upgrade WAL entries"): WAL replay applies `IndexPut`/`IndexDel`
    // with literal keys AFTER the tables are reachable — so a crash of an
    // OLD binary followed by the first NEW open would replay v1/v3 keys
    // after the open-time identity migration, leaving those records
    // missing from the v2 index. Track every table that received an index
    // op during replay (broadcast ops: all tables); after the loop, every
    // table with BOTH marks (index op replayed this recovery AND identity
    // migration ran at this open) re-runs both base and index2 migration.
    // have been replayed by then, so the rebuild is complete. This also
    // closes the same latent hole for the S9b v2->v3 bump. No WAL format
    // change.
    let mut replayed_index_tables: Vec<u64> = Vec::new();
    let mut replayed_index_broadcast = false;
    for entry in entries {
        for op in &entry.ops {
            match op {
                WalOpV2::IndexPut {
                    table_id_interned: 0,
                    ..
                }
                | WalOpV2::IndexDel {
                    table_id_interned: 0,
                    ..
                } => replayed_index_broadcast = true,
                WalOpV2::IndexPut {
                    table_id_interned, ..
                }
                | WalOpV2::IndexDel {
                    table_id_interned, ..
                } => replayed_index_tables.push(*table_id_interned),
                _ => {}
            }
        }
    }
    if replayed_index_broadcast || !replayed_index_tables.is_empty() {
        for name in repo.list_table_names() {
            let tbl = Box::pin(repo.get_table_admitted(&name, permit)).await?;
            let targeted =
                replayed_index_broadcast || replayed_index_tables.contains(&tbl.table_token());
            if targeted && tbl.identity_migrated_this_open() {
                log::warn!(
                    "post-replay identity re-run: table '{name}' received a replayed \
                     pre-upgrade index op after migrating this open; rebuilding"
                );
                Box::pin(tbl.rerun_identity_migration_admitted(permit)).await?;
                Box::pin(tbl.rerun_index2_identity_migration()).await?;
            }
        }
    }

    // Phase B (A11): force ONE interner persist so every replayed delta is
    // durably checkpointed before any entry is finalized. The persisted
    // high-water mark now covers every id any replayed entry referenced,
    // matching the invariant the drainer's A5 gate enforces on its hot path.
    // Cheap in the common case (no-op when nothing new was added) and
    // correct in the worst case (one flush covers the whole recovery batch).
    let repo_interner = repo.repo_interner().await?;
    repo_interner.persist().await.map_err(|e| {
        DbError::Internal(format!(
            "A11 recovery: interner persist after replay failed (history records \
             may reference ids the persistent interner has not absorbed): {e}; \
             refusing to finalize any entry"
        ))
    })?;

    // Phase C: now that the interner is durably consistent with every replayed
    // entry's data, finalize each entry (mark / mark_durable / wal.commit).
    // Safe to do in a separate pass because Phase A already landed the data
    // in `history` and the gate floor was seeded from the inflight markers in
    // Step 1 above — finalize here just publishes the recovered state to
    // readers and clears the (no-op) per-entry marker.
    for (txn_id, commit_version) in finalized {
        // P1d: mark version as materialized so the completion tracker
        // rebuilds the contiguous prefix. Every durable WAL entry is
        // treated as Materialized — there is no commit/abort marker
        // distinction in the WAL (durable = committed). Legacy entries
        // with commit_version == 0 are skipped (watermark already ≥ 0).
        if commit_version > 0 {
            gate.completion()
                .mark(commit_version, CompletionState::Materialized);
            // D2 P1d-2b: `replay_v2_entry` just wrote this version's data into
            // `history` (via `write_committed_to_history`) — it is now durable.
            // Advance the durable watermark so a freshly-opened repo's
            // `durable_watermark()` catches up to its `last_committed()` (on
            // open the overlay is empty, so reads go straight to history, and
            // the durable/visibility gap must be closed). The drainer's normal
            // warm path does this same `mark_durable`; recovery is the cold
            // path doing it for the inflight tail.
            gate.mark_durable(commit_version);
        }
        // No-op: there are no per-entry markers; the segment is cleaned by
        // F6 truncation and replay is idempotent.
        if let Some(wal) = boot_wal {
            wal.commit(txn_id).await?;
        }
    }

    if count > 0 {
        // P1d: sync last_committed from the tracker watermark so readers
        // see the full recovered prefix without gaps.
        gate.sync_last_committed_from_watermark();

        // Step 3: persist the (possibly advanced) floor so a clean
        // restart — which sees no inflight markers — still seeds the gate
        // above the recovered commit versions. `gate.last_committed()`
        // already reflects max(marker, max_inflight) from step 1.
        if let Some(v_star) = strict_point {
            check_gate_version(gate, v_star)?;
        }
        let floor = gate.last_committed();
        let info_store = repo.tx_info_store().await?;
        crate::meta::recovery_marker::save_last_committed(&info_store, floor).await?;

        log::info!(
            "V2 recovery replayed {} inflight tx entries (max commit_version {}, \
             gate version floor {})",
            count,
            max_replayed,
            floor
        );
    }
    Box::pin(repo.certify_cold_start(entries, permit)).await?;
    Ok(count)
}

/// cancel-safe: NO — iterates ops in the entry, applying each one via
/// `replay_v2_op`. Cancellation mid-entry leaves the entry partially
/// applied; ops are idempotent at the data layer so re-replay converges,
/// but mid-flight cancellation is not atomic.
///
/// Replay all ops in one WAL entry. Iterates ops in declared order
/// (counter → interner → index → data per `wal_ops_from_tx` emission
/// order, though replay order is logically commutative within one entry).
///
/// CRIT-B: after the data ops land in `main`, seed each touched table's
/// `MvccStore::version_cache` with this entry's `commit_version`. Replay
/// writes go straight to `data_store()` (bypassing `apply_committed_ops`),
/// so without this the version cache would report `0` for recovered keys.
/// See `seed_version_cache_for_entry` for why this matters.
pub async fn replay_v2_entry(entry: &shamir_wal::WalEntryV2, repo: &RepoCore) -> DbResult<()> {
    let permit = repo
        .write_domain()
        .admit()
        .await
        .map_err(|error| DbError::Internal(error.to_string()))?;
    // Round-11 (stack layout): same heap-pinning rationale as `replay_v2_op`.
    Box::pin(replay_v2_entry_admitted(entry, repo, &permit, false)).await
}

#[inline(never)]
fn replay_v2_entry_admitted<'a>(
    entry: &'a shamir_wal::WalEntryV2,
    repo: &'a RepoCore,
    permit: &'a RepoWritePermit,
    strict: bool,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = DbResult<()>> + Send + 'a>> {
    Box::pin(replay_v2_entry_admitted_inner(entry, repo, permit, strict))
}

async fn replay_v2_entry_admitted_inner(
    entry: &shamir_wal::WalEntryV2,
    repo: &RepoCore,
    permit: &RepoWritePermit,
    strict: bool,
) -> DbResult<()> {
    repo.write_domain().debug_check_permit(permit);
    // A4-recovery + Stage I keystone: apply the interner delta BEFORE
    // replaying data ops. The ops' record bytes reference intern-ids from the
    // delta, so the interner must know them before any decode/replay occurs.
    //
    // Stage I: the interner is per-REPO (one id-namespace across every
    // table), so the old per-token routing (`repo.table_by_token(token)` →
    // `tbl.interner()`) collapses to a SINGLE resolution of the repo
    // interner. The first `u64` of each triple is now a repo-scope constant
    // (see `REPO_INTERNER_SCOPE`); boot ignores it, while strict replay
    // requires the repo scope. Every replayed record byte encodes ids from
    // this one interner, regardless of which table the op targets.
    if !entry.interner_delta.is_empty() {
        let repo_interner = repo.repo_interner().await?;
        let interner = repo_interner.get().await?;
        for (scope, name, id) in &entry.interner_delta {
            let expected_scope = unexpected_repo_interner_scope(*scope).filter(|_| strict);
            if let Some(expected) = expected_scope {
                return Err(DbError::Validation(format!(
                    "strict WAL replay: tx {} interner scope {scope}, expected {expected}",
                    entry.txn_id
                )));
            }
            interner.touch_with_id(name, *id).map_err(|e| {
                DbError::Internal(format!(
                    "replay tx {} interner delta failed: {}",
                    entry.txn_id, e
                ))
            })?;
        }
    }

    for op in &entry.ops {
        replay_v2_op_admitted(op, repo, permit, strict)
            .await
            .map_err(|e| match e {
                // Structured rebuild/open failures keep their identity (a
                // typed per-table rebuild refusal must survive cold replay
                // for the system store's per-table damage classification).
                error @ (DbError::UniqueIndexCreationFailed(..)
                | DbError::IndexRebuildFailed { .. }) => error,
                e => DbError::Internal(format!("replay tx {} op failed: {}", entry.txn_id, e)),
            })?;
    }
    Box::pin(seed_version_cache_for_entry(entry, repo, permit, strict)).await?;
    Ok(())
}

/// cancel-safe: NO — routes each table's data ops through
/// `write_committed_to_history` (history transact + ts + cell seed).
/// Cancellation mid-loop leaves some tables written, others not; the ops are
/// idempotent (last-write-wins) so a re-replay / re-drain converges.
///
/// Write every data (`Put`/`Delete`) op in `entry` to the table's durable
/// version-log at the entry's `commit_version` (D2 P1d-2b: this is the
/// history-write half — the drainer and cold recovery share it). Index /
/// counter / interner ops do not flow through the `MvccStore` (it wraps only
/// the data store), so they are skipped here.
///
/// Looks up the table's `MvccStore` via `per_table_mvcc` — populated when
/// `table_by_token` (called during replay) instantiates the
/// `TableManager`. A missing entry (dropped/unconfigured table) is a
/// silent skip, matching the replay ops' own graceful handling.
///
/// # CRIT-1 (#435): history-write error is FATAL to recovery
///
/// An entry whose `write_committed_to_history` fails MUST surface an error.
/// Pre-fix this branch swallowed the error in a `log::warn!` and returned
/// `()`, so `recover_inflight_v2` unconditionally proceeded to
/// `gate.completion().mark(v, Materialized)` + `gate.mark_durable(v)` — a
/// silent loss of the confirmed commit (readers see `last_committed ≥ v`
/// with no value in either overlay (empty post-restart) or history), and
/// the next F6 truncation pass could unlink the sole sealed WAL segment
/// holding v → unrecoverable loss. The historical justification ("the WAL
/// marker is untouched") was already obsolete post-F6 (truncation follows
/// the durable watermark, not per-entry markers).
///
/// The fix mirrors `RepoInstance::flush_buffers`: iterate EVERY table
/// (best-effort — a failure on one table does NOT skip the remaining
/// tables; they still get their write attempt) but capture the FIRST error
/// and return it. The caller (`replay_v2_entry` → `recover_inflight_v2`)
/// propagates it up to `open()`, where `add_repo`'s `recover_v2_inflight().await?`
/// refuses to serve a repo that could not recover ("repo that cannot recover
/// must not be served", `db_management.rs:337-343`). The failed entry is
/// therefore NEVER marked durable/materialized — the watermark stays below
/// it, so F6 truncation cannot unlink the segment holding it.
async fn seed_version_cache_for_entry(
    entry: &shamir_wal::WalEntryV2,
    repo: &RepoCore,
    permit: &RepoWritePermit,
    strict: bool,
) -> DbResult<()> {
    repo.write_domain().debug_check_permit(permit);
    let v = entry.commit_version;

    // D2 P1d-2b: group this entry's DATA ops per table and route them through
    // `MvccStore::write_committed_to_history` — the SAME history-write half the
    // background drainer uses. This is what makes `replay_v2_entry` the genuine
    // history writer after the cutover (the ack-path writes only the overlay).
    // For an MVCC table this writes the version-log entry (Put body / Delete
    // tombstone), records the commit ts, and seeds the cell + floor. Cold
    // recovery (overlay empty) thus lands every recovered version in `history`
    // so post-restart reads resolve correctly; warm drain re-runs the same path
    // idempotently (last-write-wins).
    let mut by_table: TFxMap<u64, Vec<KvOp>> = TFxMap::default();
    for op in &entry.ops {
        let (table_id, kvop) = match op {
            WalOpV2::Put {
                table_id_interned,
                rid,
                body,
            } => (
                *table_id_interned,
                KvOp::Set(RecordKey::from_slice(rid.as_bytes()), body.clone()),
            ),
            WalOpV2::Delete {
                table_id_interned,
                rid,
            } => (
                *table_id_interned,
                KvOp::Remove(RecordKey::from_slice(rid.as_bytes())),
            ),
            _ => continue,
        };
        by_table.entry(table_id).or_default().push(kvop);
    }

    // CRIT-1 (#435): a failed history write is FATAL to recovery — see the
    // function doc. We still attempt EVERY table (best-effort: a failure on
    // one table must not skip the remaining tables) and capture the first
    // error to return, mirroring `RepoInstance::flush_buffers`' first-err
    // pattern. Returning Err propagates through `replay_v2_entry`'s `?` and
    // breaks `recover_inflight_v2`'s `for entry in entries` loop BEFORE the
    // failing entry is marked `Materialized` / `mark_durable`, so the durable
    // watermark stays below it and F6 truncation cannot unlink the WAL
    // segment holding its only copy.
    // A missing MVCC target is a refusal during strict replay.
    let mut first_err: Option<DbError> = None;
    for (table_id, ops) in by_table {
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
        // racing recovery's history seeding can park every worker behind that
        // unpolled reader → whole-runtime deadlock. `read_sync`'s bucket lock
        // is held only by a RUNNING thread for a few instructions (an
        // `Arc::clone`), bounding every wait. The fn stays `async`; this call
        // no longer suspends.
        if let Some(mvcc) = repo
            .per_table_mvcc()
            .read_sync(&table_id, |_, m| std::sync::Arc::clone(m))
        {
            // C2 + ts: write the version-log + commit ts + seed cell/floor.
            // Failure is captured, NOT swallowed: see the function doc for
            // why a silent warn here is a silent loss of a confirmed commit.
            //
            // Test-only injection (CRIT-1 / #435): simulate a persistent
            // history-write error so the post-fix contract (a failed history
            // write is FATAL to recovery) can be exercised two ways:
            //   (a) in-process unit tests arm the `FAIL_HISTORY_SEED_TX_ID`
            //       static (keyed by `txn_id`, mirrors the
            //       `FAIL_VECTOR_DELTA_TX_ID` pattern in `commit_phases.rs`);
            //   (b) the external `crash_recovery` integration harness (a
            //       separate test binary that cannot reach this `pub(crate)`
            //       static) arms the `SHAMIR_TEST_FAIL_HISTORY_SEED` env var.
            // The whole branch is `#[cfg(debug_assertions)]` (same gate as
            // `maybe_crash`) so a `--release` production build pays zero cost
            // — no atomic load, no env read, the branch is elided.
            #[cfg(debug_assertions)]
            let inject_fail = {
                let static_armed = entry.txn_id != 0
                    && FAIL_HISTORY_SEED_TX_ID.load(std::sync::atomic::Ordering::SeqCst)
                        == entry.txn_id;
                let env_armed = std::env::var("SHAMIR_TEST_FAIL_HISTORY_SEED").is_ok();
                static_armed || env_armed
            };
            #[cfg(not(debug_assertions))]
            let inject_fail = false;

            let write_result = if inject_fail {
                Err(DbError::Internal(format!(
                    "injected history-seed failure for tx {} (CRIT-1 fault vector)",
                    entry.txn_id
                )))
            } else {
                mvcc.write_committed_to_history(&ops, v).await
            };

            if let Err(e) = write_result {
                log::warn!(
                    "seed_version_cache_for_entry: history write for tx {} \
                     table {} commit_version {} failed (recovery will fail): {e}",
                    entry.txn_id,
                    table_id,
                    v
                );
                if first_err.is_none() {
                    first_err = Some(e);
                }
            }
        } else if strict {
            return Err(unresolved_table(table_id));
        }
    }
    if let Some(e) = first_err {
        return Err(e);
    }
    // R3: advance the reader-visible floor even when no MVCC table matched
    // (e.g. an entry touching only unattached tables). `write_committed_to_history`
    // already advances it for MVCC tables; this is the catch-all. Monotonic
    // fetch_max — safe to call redundantly.
    if v > 0 {
        match repo.tx_gate().await {
            Ok(gate) => gate.raise_published(v),
            Err(e) if strict => return Err(e),
            Err(_) => {}
        }
    }
    Ok(())
}

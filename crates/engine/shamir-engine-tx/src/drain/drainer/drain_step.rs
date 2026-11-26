use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use shamir_collections::TFxMap;
use shamir_storage::error::{DbError, DbResult};
use shamir_storage::types::{KvOp, RecordKey};
use shamir_wal::{WalEntryV2, WalOpV2, WalTicketState};

use super::Drainer;
use crate::clean_horizon::repair_pending_drain::entry_interner_max_id;
use crate::commit::core::RepoCore;
use crate::drainer::orphan_repair;
use crate::materialize::interner_delta_safe_to_truncate;

/// Test-only injection: when set to a non-zero `table_id`, the Phase B
/// index-posting batch write for that table returns a synthetic error
/// instead of calling `Store::transact`, so the "a batch failure surfaces
/// and blocks only the affected table" contract can be exercised
/// deterministically. `InMemoryStore` (the backend the unit tests use)
/// never fails `set`/`remove`/the default `transact`, so there is no
/// natural way to trigger this path from the public API.
///
/// Mirrors `recovery/v2.rs`'s `FAIL_HISTORY_SEED_TX_ID` pattern, keyed here by
/// `table_id` instead of `txn_id` (this batch's own grouping key). Reset to
/// 0 between tests to avoid cross-test bleed. `#[cfg(debug_assertions)]` so
/// a `--release` build pays zero cost — the static is absent and the load
/// branch is elided.
#[cfg(debug_assertions)]
pub(crate) static FAIL_INDEX_BATCH_TABLE_ID: AtomicU64 = AtomicU64::new(0);

impl Drainer {
    /// cancel-safe: NO — multi-step state mutation per entry (replay →
    /// mark_durable → wal.commit). Cancellation mid-loop leaves the durable
    /// watermark partially advanced; replay is idempotent so a re-run
    /// converges, but the step is not atomic. Mirrors `recover_inflight_v2`.
    ///
    /// L1 coalesced drain: three-phase pass that collapses E entries x T tables
    /// from E*T `history.transact` calls down to T calls (one per table).
    ///
    /// **Phase A** (per-entry, ascending v): apply interner delta (A4 keystone,
    /// BEFORE data), replay non-MVCC ops, and accumulate data ops into a
    /// per-table batch `TFxMap<table_id, Vec<(v, Vec<KvOp>)>>`.
    ///
    /// **Phase B** (per table): ONE `write_committed_batch_to_history` per table.
    /// On the first table transact failure, stop and do not finalize any entry
    /// whose data touched a failed table.
    ///
    /// **Phase C** (per-entry, ascending v): for entries whose tables ALL
    /// succeeded: `mark_durable(v)` -> A5 gate -> `wal.commit` -> crash seams.
    /// Stop at the first entry with a failed table (contiguity).
    pub async fn drain_step(&self, repo: &RepoCore) -> DbResult<usize> {
        self.drain_step_with_cancel(repo, None).await
    }

    /// cancel-safe: NO — replay, durability and WAL finalization span awaits.
    pub(super) async fn replay_locked(&self, repo: &RepoCore) -> DbResult<usize> {
        let gate = repo.tx_gate().await?;
        let mut unresolved_ceiling = u64::MAX;
        let mut blocked_by_unresolved = false;
        for version in gate.unresolved_wal_versions() {
            let Some((ticket, entry)) = gate.unresolved_wal(version) else {
                continue;
            };
            match ticket.state() {
                WalTicketState::Written | WalTicketState::Acked => {
                    gate.register_written_orphan(version, entry.clone());
                    self.offer(entry);
                    gate.take_unresolved(version);
                }
                WalTicketState::Failed => {
                    if gate.take_unresolved(version).is_some() {
                        gate.abort_unwritten(version);
                        repo.settle_unwritten_version(version).await;
                    }
                }
                WalTicketState::Pending => {
                    blocked_by_unresolved = true;
                    unresolved_ceiling = version.saturating_sub(1);
                    break;
                }
                WalTicketState::Unknown => {
                    gate.set_requires_reopen();
                    log::error!(
                        "drain_step: WAL outcome UNKNOWN at version {version}; \
                         halting at {}; repository requires reopen",
                        version.saturating_sub(1),
                    );
                    blocked_by_unresolved = true;
                    unresolved_ceiling = version.saturating_sub(1);
                    break;
                }
                _ => {
                    gate.set_requires_reopen();
                    log::error!("drain_step: unsupported WAL fate at {version}; reopen required");
                    blocked_by_unresolved = true;
                    unresolved_ceiling = version.saturating_sub(1);
                    break;
                }
            }
        }
        for version in gate.orphaned_post_wal_versions() {
            if version > unresolved_ceiling {
                break;
            }
            if let Some(entry) = gate.orphan_entry(version) {
                self.offer(entry.clone());
                // Retained entries must not depend on recover or the soft offer cap.
                if self.window.insert_sync(version, entry).is_ok() {
                    self.window_depth.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        let visible = gate.last_committed();
        let vis = visible.max(gate.orphan_max()).min(unresolved_ceiling);
        let dur = gate.durable_watermark();
        // Also clean up late async-index markers on otherwise idle passes.
        self.prune_index_applied(dur);
        // Nothing visible is un-durable → nothing new to REPLAY. But CRIT-2
        // (#436): truncation may still be pending on an entry whose A5 gate
        // tripped unsafe in an earlier pass — the interner can catch up
        // without any new commit, so re-settle `pending_unsafe` and attempt
        // truncation here before bailing out.
        if dur >= vis {
            self.settle_and_truncate(repo).await?;
            return Ok(0);
        }

        // Op #2 Stage 3: scan the in-memory window for the contiguous
        // ascending prefix [dur+1 .. vis]. No I/O in steady state.
        let mut window_entries: Vec<Arc<WalEntryV2>> = Vec::new();
        let mut blocked_by_inflight = false;
        {
            let guard = scc::Guard::new();
            for (expected, (k, v)) in (dur + 1..).zip(self.window.range(dur + 1..=vis, &guard)) {
                if *k != expected {
                    break; // genuine missing key — reseed if the prefix is empty
                }
                if gate.is_active_post_wal(*k) || (*k > visible && !gate.is_orphaned_post_wal(*k)) {
                    blocked_by_inflight = true;
                    break;
                }
                window_entries.push(Arc::clone(v));
                if gate.is_orphaned_post_wal(*k) {
                    break;
                }
            }
        }

        // PT 2: gap-reseed fallback fires ONLY when the contiguous prefix
        // is empty. `dur = durable_watermark()` already crosses every
        // leading aborted version (VersionGuard::drop marks aborts on
        // the durable tracker), so `dur+1` is NEVER aborted — it is
        // either in the window or a true dropped-offer gap. A non-empty
        // prefix therefore proves real progress; the next aborted hole
        // above is crossed via the moving watermark on the next pass.
        // The old `len < vis-dur` trigger fired on EVERY interior abort
        // and paid a spurious O(W) wal.recover() that the recover could
        // not fill — aborted versions have no WAL entry.
        // A present in-flight entry is not a gap: wait for a later pass rather
        // than decoding WAL segments that cannot make the entry complete.
        // An unresolved fate also forbids a full WAL decode, even when an
        // earlier published offer is missing. Already-retained RAM prefixes
        // below its ceiling can still replay without guessing the frame fate.
        if window_entries.is_empty() && !blocked_by_inflight && !blocked_by_unresolved {
            let wal = repo.repo_wal().await?;
            let recovered = wal.recover().await?;
            self.recover_calls.fetch_add(1, Ordering::Relaxed);
            // Filter to entries strictly above the current watermark —
            // already-drained entries from the WAL (still inflight per
            // F6 truncation) must not be re-inserted into the window or
            // they leak across drain passes.
            let recovered: Vec<_> = recovered
                .into_iter()
                .filter(|e| e.commit_version > dur)
                .collect();
            self.seed_from_recover(recovered);
            // Retry the window scan once.
            window_entries.clear();
            orphan_repair::rescan_window_prefix(
                self,
                &gate,
                dur,
                vis,
                visible,
                &mut window_entries,
            );
            if window_entries.is_empty() {
                orphan_repair::settle_missing_evidence(self, repo, &gate).await?;
            }
        }

        if window_entries.is_empty() {
            return Ok(0);
        }

        // Warm missing registrations can be restored (remove_table/add_table).
        // Unlike cold recovery, do not discard an orphan on a transient absence.
        // Pin one resolved TableManager across history, postings and counters;
        // never combine an old per_table_mvcc with a replacement registration.
        // Lookup failure excludes the earliest missing orphan and everything
        // after it, but does not hold back its already-published prefix.
        let mut orphan_failed = false;
        let mut orphan_version = None;
        let mut orphan_tables = TFxMap::default();
        let mut replay_prefix = window_entries.len();
        'resolve_orphans: for (position, entry) in window_entries.iter().enumerate() {
            if !gate.is_orphaned_post_wal(entry.commit_version) {
                continue;
            }
            orphan_version = Some(entry.commit_version);
            self.orphan_replay_attempts_total
                .fetch_add(1, Ordering::Relaxed);
            for op in &entry.ops {
                let token = match op {
                    WalOpV2::Put {
                        table_id_interned, ..
                    }
                    | WalOpV2::Delete {
                        table_id_interned, ..
                    }
                    | WalOpV2::IndexPut {
                        table_id_interned, ..
                    }
                    | WalOpV2::IndexDel {
                        table_id_interned, ..
                    }
                    | WalOpV2::CounterDelta {
                        table_id_interned, ..
                    } => *table_id_interned,
                    _ => continue,
                };
                if token == 0 || orphan_tables.contains_key(&token) {
                    continue;
                }
                match repo.table_by_token(token).await {
                    Ok(Some(table)) => {
                        orphan_tables.insert(token, table);
                    }
                    Ok(None) | Err(_) => {
                        self.record_orphan_failure(
                            &mut orphan_failed,
                            format_args!(
                                "orphan {} table {token} unavailable",
                                entry.commit_version,
                            ),
                        );
                        log::warn!(
                            "drain_step: orphan {} table {token} unavailable; retry next pass",
                            entry.commit_version,
                        );
                        replay_prefix = position;
                        break 'resolve_orphans;
                    }
                }
            }
        }
        window_entries.truncate(replay_prefix);
        if window_entries.is_empty() {
            return Ok(0);
        }
        let wal = repo.repo_wal().await.inspect_err(|error| {
            if orphan_version.is_some() && !orphan_failed {
                self.record_orphan_failure(&mut orphan_failed, error);
            }
        })?;

        // ================================================================
        // Phase A: per-entry interner delta + accumulate data ops per table.
        // ================================================================

        // Per-table batch: table_id -> Vec<(commit_version, Vec<KvOp>)>,
        // preserving ascending-v order within each table for LWW.
        let mut table_batches: TFxMap<u64, Vec<(u64, Vec<KvOp>)>> = TFxMap::default();
        // Per-table batch of index postings (IndexPut/IndexDel), flattened
        // into one ascending-order `Vec<KvOp>` per table (NOT grouped by
        // commit_version like `table_batches` above — `info_store` is a
        // plain last-write-wins KV store, not a versioned history, so one
        // flat ordered Vec per table reproduces exactly what the old
        // immediate per-op `replay_v2_op` application would have done, as
        // long as ops are pushed in the same entry/op iteration order).
        // Only postings whose `table_id_interned != 0` (the common case —
        // `wal_ops_from_tx` never emits the `0` broadcast marker) are
        // batched here; broadcast postings still replay immediately below.
        let mut index_batches: TFxMap<u64, Vec<(u64, KvOp)>> = TFxMap::default();
        // Per-entry: which table_ids does this entry touch (for Phase C gating).
        let mut entry_tables: Vec<(u64 /* commit_version */, Vec<u64> /* table_ids */)> =
            Vec::with_capacity(window_entries.len());
        // Track Phase A failure — stop accumulating and drain nothing.
        let mut phase_a_failed = false;

        for entry in &window_entries {
            let v = entry.commit_version;

            // A4-recovery keystone: apply interner delta BEFORE data.
            if !entry.interner_delta.is_empty() {
                match apply_interner_delta(entry, repo).await {
                    Ok(()) => {}
                    Err(e) => {
                        log::warn!(
                            "drain_step: interner delta for tx {} commit_version {} \
                             failed: {e}; stopping this pass",
                            entry.txn_id,
                            v
                        );
                        if orphan_version == Some(v) {
                            self.record_orphan_failure(&mut orphan_failed, &e);
                        }
                        phase_a_failed = true;
                        break;
                    }
                }
            }

            // Route ops that are NOT batched through `replay_v2_op`
            // immediately: broadcast IndexPut/IndexDel (table_id_interned
            // == 0 — never emitted by `wal_ops_from_tx` today, a defensive
            // legacy fallback only), InternerOverlayMerge, and
            // CounterDelta. Per-table IndexPut/IndexDel (the common case)
            // are instead accumulated into `index_batches` below — this was
            // previously one awaited `info_store().set`/`remove` call PLUS
            // one `table_by_token` resolve PER POSTING; now it is ONE
            // resolve + ONE `Store::transact` per table per drain pass,
            // mirroring the existing Put/Delete data-op batching. Data ops
            // (Put/Delete) are handled by the second loop below, unchanged.
            let mut this_entry_index_tables: Vec<u64> = Vec::new();
            for op in &entry.ops {
                match op {
                    WalOpV2::Put { .. } | WalOpV2::Delete { .. } => {
                        // Data ops: handled below via batch accumulation.
                    }
                    WalOpV2::IndexPut {
                        table_id_interned,
                        key,
                        value,
                        ..
                    } if *table_id_interned != 0 => {
                        if self.index_applied_inline(v, *table_id_interned)
                            || self.posting_superseded(v, *table_id_interned, key)
                        {
                            continue;
                        }
                        this_entry_index_tables.push(*table_id_interned);
                        index_batches
                            .entry(*table_id_interned)
                            .or_default()
                            .push((v, KvOp::Set(key.clone().into(), value.clone())));
                    }
                    WalOpV2::IndexDel {
                        table_id_interned,
                        key,
                        ..
                    } if *table_id_interned != 0 => {
                        if self.index_applied_inline(v, *table_id_interned)
                            || self.posting_superseded(v, *table_id_interned, key)
                        {
                            continue;
                        }
                        this_entry_index_tables.push(*table_id_interned);
                        index_batches
                            .entry(*table_id_interned)
                            .or_default()
                            .push((v, KvOp::Remove(key.clone().into())));
                    }
                    WalOpV2::CounterDelta { .. } => {
                        // Orphan counters are best-effort in Phase C so a table
                        // lookup failure stops at this entry, not its prefix.
                    }
                    _ => {
                        if let Err(e) = crate::recovery::replay_v2_op(op, repo).await {
                            log::warn!(
                                "drain_step: non-data op replay for tx {} \
                                 commit_version {} failed: {e}; stopping this pass",
                                entry.txn_id,
                                v
                            );
                            if orphan_version == Some(v) {
                                self.record_orphan_failure(&mut orphan_failed, &e);
                            }
                            phase_a_failed = true;
                            break;
                        }
                    }
                }
            }
            if phase_a_failed {
                break;
            }

            // Accumulate data ops (Put/Delete) grouped by table_id.
            let mut this_entry_tables: Vec<u64> = this_entry_index_tables;
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
            for (table_id, ops) in by_table {
                this_entry_tables.push(table_id);
                table_batches.entry(table_id).or_default().push((v, ops));
            }
            entry_tables.push((v, this_entry_tables));
        }

        if phase_a_failed {
            // Phase A failed on an entry — do not proceed to Phase B/C.
            // All entries remain inflight for the next pass/recovery.
            return Ok(0);
        }

        // ================================================================
        // Phase B: per-table ONE write_committed_batch_to_history.
        // ================================================================

        // Track which table_ids failed their transact.
        let mut failed_tables: shamir_collections::TFxSet<u64> =
            shamir_collections::TFxSet::default();

        for (table_id, pass) in &table_batches {
            let published_pass = pass
                .last()
                .filter(|(version, _)| gate.is_orphaned_post_wal(*version))
                .map_or(pass.as_slice(), |_| &pass[..pass.len() - 1]);
            // DEADLOCK FIX (same class as #589 / `cells` map commit `7a4abf62`,
            // H1+H2 commit `621776bd`): `read_sync`, NOT `read_async`. The
            // `per_table_mvcc` map is also touched SYNCHRONOUSLY — `read_sync`
            // (version_provider.rs, EVERY Serializable `validate_read_set`
            // commit), `get_sync` (commit.rs pessimistic-lock release;
            // rename-table) and `iter_sync` (flush_all_history; the F6a overlay
            // GC loop right below in this same file) — and EXCLUSIVELY by
            // `insert_sync` (table attach) / `remove_sync` (drop table).
            // `read_async`'s wait is lock-HANDOFF: saa grants the shared bucket
            // lock to the suspended reader TASK, which then holds it while
            // unpolled in tokio's run queue. A DDL exclusive writer
            // (attach/drop) racing sustained drain traffic can park every
            // worker behind that unpolled reader → whole-runtime deadlock.
            // `read_sync`'s bucket lock is held only by a RUNNING thread for a
            // few instructions (an `Arc::clone`), bounding every wait. The fn
            // stays `async`; this call no longer suspends.
            let mvcc = if let Some(table) = orphan_tables.get(table_id) {
                table.mvcc_store()
            } else {
                repo.per_table_mvcc()
                    .read_sync(table_id, |_, m| Arc::clone(m))
            };
            if let Some(mvcc) = mvcc {
                if let Err(e) = mvcc.write_committed_batch_to_history(published_pass).await {
                    log::warn!(
                        "drain_step: batch history write for table {} failed: {e}; \
                         entries touching this table will not be finalized",
                        table_id
                    );
                    self.record_orphan_table_failure(
                        &window_entries,
                        orphan_version,
                        *table_id,
                        &mut orphan_failed,
                        &e,
                    );
                    failed_tables.insert(*table_id);
                }
                continue;
            }

            // BUG FIX (group 1, 2026-08-14 review): `per_table_mvcc` has no
            // entry for this table_id. Phase A does NOT handle Put/Delete for
            // ANY table — it unconditionally skips them and accumulates them
            // here instead (see the loop above). This branch used to be a
            // silent no-op while Phase C finalized the entry anyway
            // (`mark_durable` + `wal.commit`), permanently discarding the
            // write. Two causes reach here: (a) a genuinely non-MVCC table
            // (no production path builds one today — see TASK_GROUPS.md
            // group 1 spot-check note 1), or (b) `RepoInstance::remove_table`
            // concurrently evicted the `per_table_mvcc` entry (DROP TABLE
            // racing an undrained commit) while this entry's ops were still
            // in the drain window.
            //
            // Mirror cold recovery instead (`recovery/v2.rs`'s `replay_v2_op` /
            // `seed_version_cache_for_entry`): resolve the table by token —
            // if it (or an MvccStore attached to it in the meantime) still
            // exists, write straight to its raw `data_store` (idempotent:
            // re-applying an already-landed Set/Remove is a no-op), so warm
            // drain and cold recovery converge. If the token no longer
            // resolves at all, the table was genuinely dropped: its data is
            // moot (the same disposition `drop_table` gives an orphaned
            // `__data__` store), so skipping is correct — not a silent loss.
            match if let Some(table) = orphan_tables.get(table_id) {
                Ok(Some(table.clone()))
            } else {
                repo.table_by_token(*table_id).await
            } {
                Ok(Some(tbl)) => {
                    if let Some(mvcc) = tbl.mvcc_store() {
                        // Race: the table was attached to `per_table_mvcc`
                        // between the read_sync above and this lookup. Route
                        // through the same history write as the primary
                        // branch above.
                        if let Err(e) = mvcc.write_committed_batch_to_history(published_pass).await
                        {
                            log::warn!(
                                "drain_step: batch history write for table {} \
                                 (resolved via table_by_token) failed: {e}; \
                                 entries touching this table will not be \
                                 finalized",
                                table_id
                            );
                            self.record_orphan_table_failure(
                                &window_entries,
                                orphan_version,
                                *table_id,
                                &mut orphan_failed,
                                &e,
                            );
                            failed_tables.insert(*table_id);
                        }
                    } else {
                        // Genuinely unattached: no version log, write the
                        // raw data ops directly (best-effort — attempt every
                        // op, capture the first error, mirroring the
                        // first-err pattern used throughout this crate).
                        let mut first_err: Option<DbError> = None;
                        for (_v, ops) in pass {
                            for op in ops {
                                let res = match op {
                                    KvOp::Set(key, body) => {
                                        tbl.data_store().set(key.clone(), body.clone()).await
                                    }
                                    KvOp::Remove(key) => tbl.data_store().remove(key.clone()).await,
                                };
                                if let Err(e) = res {
                                    if first_err.is_none() {
                                        first_err = Some(e);
                                    }
                                }
                            }
                        }
                        if let Some(e) = first_err {
                            log::warn!(
                                "drain_step: unattached-table data write for \
                                 table {} failed: {e}; entries touching this \
                                 table will not be finalized",
                                table_id
                            );
                            self.record_orphan_table_failure(
                                &window_entries,
                                orphan_version,
                                *table_id,
                                &mut orphan_failed,
                                &e,
                            );
                            failed_tables.insert(*table_id);
                        }
                    }
                }
                Ok(None) => {
                    log::debug!(
                        "drain_step: table {} no longer resolves (dropped); \
                         data ops for this token are moot, skipping",
                        table_id
                    );
                }
                Err(e) => {
                    log::warn!(
                        "drain_step: table_by_token({}) failed while resolving \
                         an unattached-table data batch: {e}; entries touching \
                         this table will not be finalized",
                        table_id
                    );
                    self.record_orphan_table_failure(
                        &window_entries,
                        orphan_version,
                        *table_id,
                        &mut orphan_failed,
                        &e,
                    );
                    failed_tables.insert(*table_id);
                }
            }
        }

        // ================================================================
        // Phase B (index postings): per-table ONE `Store::transact` for
        // every table's accumulated IndexPut/IndexDel ops — was previously
        // one awaited `info_store().set`/`remove` PLUS one `table_by_token`
        // resolve PER POSTING (`recovery/v2.rs`'s `replay_v2_op`); an
        // index-heavy drain window paid E*T awaits instead of T.
        // ================================================================

        for (table_id, ops) in index_batches {
            match if let Some(table) = orphan_tables.get(&table_id) {
                Ok(Some(table.clone()))
            } else {
                repo.table_by_token(table_id).await
            } {
                Ok(Some(tbl)) => {
                    // Exclude inline writers across check -> awaited transact ->
                    // record. Order: replay -> DDL admission -> raise intent ->
                    // drain writers -> unique_write_lock, one table at a time.
                    // No commit lock or map guard is held across this await.
                    // Commit's data-table prelock guards span Phase 5c and
                    // mark_index_applied, then materialize drops them without
                    // acquiring replaying. Index-only manually staged ops do
                    // not participate in that writer-drain protocol.
                    let posting_guards = if orphan_tables.contains_key(&table_id) {
                        Some(
                            tbl.begin_write_barrier(
                                shamir_index::base_index::write_barrier_flags::SCHEMA_ACTIVATION,
                            )
                            .await?,
                        )
                    } else {
                        None
                    };
                    // Recheck after table resolution: inline application may
                    // have advanced a posting since Phase A accumulated it.
                    let ops: Vec<_> = ops
                        .into_iter()
                        .filter(|(version, op)| {
                            let key = match op {
                                KvOp::Set(key, _) | KvOp::Remove(key) => {
                                    bytes::Bytes::copy_from_slice(key.as_ref())
                                }
                            };
                            !self.posting_superseded(*version, table_id, &key)
                        })
                        .collect();
                    let batch_len = ops.len();
                    // Test-only injection (see `FAIL_INDEX_BATCH_TABLE_ID`'s
                    // doc): simulate a persistent index-batch-write failure
                    // for one table so the "failure surfaces and blocks only
                    // that table" contract can be exercised deterministically
                    // — `InMemoryStore.transact` never fails on its own.
                    #[cfg(debug_assertions)]
                    let inject_fail = {
                        let armed = FAIL_INDEX_BATCH_TABLE_ID.load(Ordering::SeqCst);
                        armed != 0 && armed == table_id
                    };
                    #[cfg(not(debug_assertions))]
                    let inject_fail = false;

                    // The replay writes postings straight onto `info_store`,
                    // bypassing `IndexManager::apply_ops` and its posting-cache
                    // invalidation: remember the keys and drop their cached
                    // lists AFTER the write lands.
                    let touched_keys: Vec<bytes::Bytes> = ops
                        .iter()
                        .map(|(_, op)| match op {
                            KvOp::Set(k, _) | KvOp::Remove(k) => {
                                bytes::Bytes::copy_from_slice(k.as_ref())
                            }
                        })
                        .collect();

                    let write_result = if inject_fail {
                        Err(DbError::Internal(format!(
                            "injected index-batch failure for table {table_id} \
                             (test fault vector)"
                        )))
                    } else {
                        tbl.info_store()
                            .transact(ops.iter().map(|(_, op)| op.clone()).collect())
                            .await
                    };
                    if write_result.is_ok() {
                        for ((version, _), key) in ops.iter().zip(&touched_keys) {
                            self.record_posting_applied(*version, table_id, key.clone());
                        }
                        tbl.index_manager_ref()
                            .invalidate_posting_cache_for_keys(touched_keys.iter().map(|k| &k[..]));
                    }

                    if let Some((barrier, lock)) = posting_guards {
                        drop(barrier);
                        drop(lock);
                    }
                    if let Err(e) = write_result {
                        log::warn!(
                            "drain_step: batch index-posting write for table {} \
                             ({} posting op(s)) failed: {e}; entries touching \
                             this table will not be finalized",
                            table_id,
                            batch_len
                        );
                        self.record_orphan_table_failure(
                            &window_entries,
                            orphan_version,
                            table_id,
                            &mut orphan_failed,
                            &e,
                        );
                        failed_tables.insert(table_id);
                    }
                }
                Ok(None) => {
                    log::debug!(
                        "drain_step: table {} no longer resolves (dropped) while \
                         resolving a batched index-posting write; postings for \
                         this token are moot, skipping",
                        table_id
                    );
                }
                Err(e) => {
                    log::warn!(
                        "drain_step: table_by_token({}) failed while resolving a \
                         batched index-posting write: {e}; entries touching this \
                         table will not be finalized",
                        table_id
                    );
                    self.record_orphan_table_failure(
                        &window_entries,
                        orphan_version,
                        table_id,
                        &mut orphan_failed,
                        &e,
                    );
                    failed_tables.insert(table_id);
                }
            }
        }

        // ================================================================
        // Phase C: per-entry finalization (ascending v, contiguous).
        // ================================================================

        let mut drained = 0usize;

        'finalize: for (entry, (v, touched_tables)) in
            window_entries.iter().zip(entry_tables.iter())
        {
            // Contiguity: stop at the first entry that touches a failed table.
            // Entries above this version must not be finalized (the watermark
            // would jump over the gap).
            let any_failed = touched_tables.iter().any(|t| failed_tables.contains(t));
            if any_failed {
                log::warn!(
                    "drain_step: entry tx {} commit_version {} touches a table \
                     whose batch write failed; stopping finalization",
                    entry.txn_id,
                    v
                );
                break;
            }

            // D4 crash seam: data is durable in history but `mark_durable(v)`
            // has NOT yet run. Recovery re-replays idempotently.
            crate::commit::maybe_crash("drain_replay", repo).await;

            // CRIT-2 (@sh round-2 finding): the A5 interner-hwm gate MUST be
            // checked and `pending_unsafe` populated BEFORE `mark_durable`
            // runs, not after. `interner_delta_safe_to_truncate` is a
            // genuine `.await` (over `repo.repo_interner().await?`) — if
            // `mark_durable(*v)` ran first, a CONCURRENT `settle_and_truncate`
            // call (historically the background and explicit replay passes
            // raced; replay is now serialized) could observe durability at v
            // with `pending_unsafe` NOT YET containing `v` — computing a
            // ceiling that includes `v` before this call ever gets to
            // protect it. Checking A5 and inserting into `pending_unsafe`
            // FIRST closes that window: by the time `durable_watermark`
            // advances to cover `v`, `pending_unsafe` already protects it if
            // unsafe.
            let delta_max_id = entry_interner_max_id(entry);
            let a5_safe = match interner_delta_safe_to_truncate(repo, delta_max_id).await {
                Ok(true) => true,
                Ok(false) => {
                    log::debug!(
                        "drain_step: tx {} commit_version {} drained to history but \
                         marker retained pending interner checkpoint (A5)",
                        entry.txn_id,
                        v
                    );
                    false
                }
                Err(e) => {
                    log::warn!(
                        "drain_step: A5 interner-hwm check for tx {} failed: {e}; \
                         conservatively retaining marker",
                        entry.txn_id
                    );
                    false
                }
            };
            if !a5_safe {
                // CRIT-2: record every A5-unsafe entry into the shared
                // `pending_unsafe` set — NOT just the first one — so
                // `settle_and_truncate`'s ceiling computation (`min(durable,
                // min_key(pending_unsafe) - 1)`) is always exhaustive over
                // every still-unpersisted interner delta, not just the
                // earliest. Inserted BEFORE `mark_durable` below (see the
                // comment above) so no concurrent truncation attempt can
                // ever observe `v` as durable without also seeing it as
                // protected.
                let _ = self
                    .pending_unsafe
                    .insert_sync(*v, delta_max_id.unwrap_or(0));
            }

            // Finalize published history writes and complete any orphan tail
            // below before advancing the contiguous durable watermark.
            //
            // NOTE (CRIT-2): `mark_durable` advancing here does NOT imply the
            // WAL segment holding this entry is safe to truncate — the A5
            // gate above (and `pending_unsafe`, populated before this call)
            // decides that. F6b uses `settle_and_truncate`'s ceiling, not
            // `durable_watermark` directly.
            if gate.is_orphaned_post_wal(*v) {
                for op in &entry.ops {
                    let WalOpV2::CounterDelta {
                        table_id_interned,
                        delta,
                    } = op
                    else {
                        continue;
                    };
                    if self.counter_applied(*v, *table_id_interned) {
                        continue;
                    }
                    let Some(tbl) = orphan_tables.get(table_id_interned) else {
                        break 'finalize;
                    };
                    if let Err(e) = tbl.counter().increment(*delta).await {
                        log::warn!(
                            "drain_step: orphan {v} counter for table \
                             {table_id_interned} failed: {e}; metric drift accepted"
                        );
                    }
                    // Failed metrics are settled too; never retry them forever.
                    self.mark_counter_applied(*v, *table_id_interned);
                }
                for (table_id, pass) in &table_batches {
                    let Some(orphan_pass) = pass.last().filter(|(version, _)| version == v) else {
                        continue;
                    };
                    let Some(tbl) = orphan_tables.get(table_id) else {
                        break 'finalize;
                    };
                    if let Some(mvcc) = tbl.mvcc_store() {
                        if let Err(e) = mvcc
                            .write_committed_batch_to_history(std::slice::from_ref(orphan_pass))
                            .await
                        {
                            self.record_orphan_failure(&mut orphan_failed, &e);
                            log::warn!(
                                "drain_step: orphan {v} history for table \
                                 {table_id} failed: {e}; stopping finalization"
                            );
                            break 'finalize;
                        }
                    }
                }
                if !orphan_repair::settle_event(self, repo, entry, &mut orphan_failed).await? {
                    break 'finalize;
                }
                gate.complete_orphaned_post_wal(*v);
                self.orphan_replay_consecutive_failures
                    .store(0, Ordering::Relaxed);
                self.orphan_replay_last_error.store(None);
                #[cfg(test)]
                let inject_marker_failure = crate::commit::TEST_POST_WAL_PRE_PUBLISH_HOOK
                    .get()
                    .is_some_and(|hook| {
                        hook.repo_name == repo.name()
                            && hook.fail_orphan_markers.swap(false, Ordering::AcqRel)
                    });
                #[cfg(not(test))]
                let inject_marker_failure = false;
                let marker_result = if inject_marker_failure {
                    Err(DbError::Internal("injected orphan marker failure".into()))
                } else {
                    crate::commit_phases::persist_markers(repo, &gate, *v).await
                };
                if let Err(e) = marker_result {
                    log::warn!(
                        "drain_step: orphan {v} marker persist failed: {e}; \
                         next commit rewrites monotone markers"
                    );
                }
                gate.forget_orphaned_post_wal(*v);
            }
            gate.mark_durable(*v);
            drained += 1;

            // Truncate the inflight marker now that both mark_durable and
            // the pending_unsafe bookkeeping above are settled for this
            // entry.
            if a5_safe {
                if let Err(e) = wal.commit(entry.txn_id).await {
                    log::warn!(
                        "drain_step: wal.commit(tx {}) failed: {e}; \
                         marker left inflight (data already durable)",
                        entry.txn_id
                    );
                }
            }

            // D2 P1d-2c crash seam: `v` is now fully durable in history and
            // truncation has been attempted. Recovery re-applies idempotently.
            crate::commit::maybe_crash("phase7", repo).await;

            // Op #2 Stage 3: remove the finalized entry from the window so
            // memory does not grow without bound. PT 1: keep the atomic
            // depth mirror in lock-step with the actual tree.
            if self.window.remove_sync(v) {
                self.window_depth.fetch_sub(1, Ordering::Relaxed);
            }
            self.clear_index_applied(entry);
            for op in &entry.ops {
                if let WalOpV2::CounterDelta {
                    table_id_interned, ..
                } = op
                {
                    let _ = self.counter_applied.remove_sync(&(*v, *table_id_interned));
                }
            }
        }

        self.prune_index_applied(gate.durable_watermark());

        if drained > 0 {
            self.drained_total
                .fetch_add(drained as u64, Ordering::Relaxed);

            // D2 P1e — overlay GC. The durable watermark advanced (each
            // `mark_durable` above moved it; read the post-pass value once).
            // Every overlay entry with `version <= durable_watermark` is now
            // durable in `history`, so its overlay copy is redundant and is
            // dropped across ALL per-table overlays. This bounds the overlay to
            // the still-undrained `(durable_watermark, last_committed]` window
            // instead of letting it grow without limit. Lock-free: each
            // `gc_overlay_to` is a B+-tree sweep with no `.await` and no shared
            // mutex (see `MvccStore::gc_overlay_to`).
            //
            // CRIT-2 (#436): overlay GC uses `durable_watermark` (read
            // visibility) — the overlay copy is redundant once the value is in
            // `history`, which is independent of WAL-segment truncation. Only
            // F6b below must use the interner-safe ceiling.
            let durable = gate.durable_watermark();
            repo.per_table_mvcc().iter_sync(|_, mvcc| {
                mvcc.gc_overlay_to(durable);
                true
            });

            // F6b — WAL truncation. `settle_and_truncate` computes the
            // interner-safe ceiling (see `pending_unsafe`'s field doc) and
            // performs the has_truncatable/flush/truncate_below/crash-seam
            // sequence.
            self.settle_and_truncate(repo).await?;
        }
        Ok(drained)
    }
}

/// A4-recovery keystone: apply an entry's interner delta BEFORE data ops.
/// Extracted from `replay_v2_entry` (recovery.rs) for use in the drainer's
/// Phase A. The interner is per-REPO (Stage I); the first u64 of each
/// triple (scope constant) is ignored.
async fn apply_interner_delta(entry: &shamir_wal::WalEntryV2, repo: &RepoCore) -> DbResult<()> {
    let repo_interner = repo.repo_interner().await?;
    let interner = repo_interner.get().await?;
    for (_scope, name, id) in &entry.interner_delta {
        interner.touch_with_id(name, *id).map_err(|e| {
            DbError::Internal(format!(
                "drain interner delta for tx {} failed: {}",
                entry.txn_id, e
            ))
        })?;
    }
    Ok(())
}

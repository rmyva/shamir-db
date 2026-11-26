use std::sync::Arc;

use futures::future::join_all;
use shamir_collections::TFxMap;
use shamir_storage::error::DbError;
use shamir_tunables::instance_defaults::INTERNER_CHECKPOINT_INTERVAL;
use shamir_tx::{RepoTxGate, TxContext, VectorDeltaAck};

use crate::commit::core::RepoCore;
use crate::commit::maybe_crash;
use crate::commit_phases::{
    apply_data_batch, apply_index_batch, apply_vector_delta_phase, collect_data_batches,
    persist_markers, retry_materialize, MATERIALIZE_ATTEMPTS,
};
use crate::tx_outcome::MaterializationState;

#[cfg(test)]
use super::commit_phases::{
    FAIL_VECTOR_DELTA_TX_ID, FAIL_VECTOR_PROMOTE_ONCE_REMAINING, FAIL_VECTOR_PROMOTE_ONCE_TX_ID,
    FAIL_VECTOR_PROMOTE_TX_ID,
};

#[cfg(test)]
pub(in crate::tx) static TEST_PRE_MATERIALIZE_REPO: std::sync::OnceLock<String> =
    std::sync::OnceLock::new();

/// Phases 5a–6 of the commit pipeline, run OUTSIDE `commit_lock` (P2b).
///
/// Gated by per-table `uwl_guards` (unique_write_lock) already held since
/// pre_commit_prelock. Disjoint-table commits materialize in parallel;
/// overlapping-table commits serialize on the uwl_guard, not on the global
/// commit_mutex.
///
/// The WAL entry is already accepted (Phase 4 succeeded), so no projection
/// may abort the transaction. Dropping the PostWal guard transfers unfinished
/// visibility to online orphan replay; recovery also replays on the next open.
/// Data and index projections govern snapshot visibility. Counter drift is
/// advisory, and HNSW promotion remains a derived post-lock accelerator.
///
/// Returns a [`PostPublishState`] that the caller passes to
/// [`post_publish_cleanup`]. Phase 6.5 (markers) and Phase 7 (WAL cleanup)
/// are I/O-bound and also run outside the lock.
///
/// Phase 6 settles visibility only after the data and index projections succeed.
/// Otherwise PostWal Drop transfers settlement and changefeed gap ownership to
/// the drainer; WAL acceptance alone is not snapshot visibility.
///
/// Phase 6-bis (`record_commit_writes`) is called here BEFORE Phase 6
/// publish (P2c). The footprint is inserted into the lock-free
/// `scc::TreeIndex` before `last_committed_version` advances past
/// `commit_version`, so any future SSI validator scanning up to
/// `last_committed()` will see this footprint. This moved OUT of
/// `commit_lock` — the ordering invariant (footprint visible before
/// publish) is maintained by sequencing within this function.
///
/// Multi-table failure can leave some rows visible to latest-state readers.
/// The shared version remains unsettled until orphan replay completes every
/// visibility-bearing projection, so snapshots stay below that version.
pub(in crate::tx) async fn materialize(
    tx: &mut TxContext,
    repo: &RepoCore,
    version_guard: shamir_tx::VersionGuard,
    prepared_journal: Option<(
        Arc<shamir_tx::ChangelogEvent>,
        Option<Arc<shamir_tx::RepoChangefeed>>,
    )>,
    uwl_guards: Vec<tokio::sync::OwnedMutexGuard<()>>,
    drain_guards: Vec<crate::table::writer_drain_barrier::WriterDrainGuard>,
) -> (PostPublishState, Option<Arc<shamir_tx::ChangelogEvent>>) {
    let tx_id = tx.tx_id.0;
    let commit_version = version_guard.version();
    let mut ok = true;

    // The existing hook follows 5a; opt in to park before any data is visible.
    #[cfg(test)]
    if TEST_PRE_MATERIALIZE_REPO
        .get()
        .is_some_and(|name| name == repo.name())
    {
        super::commit::fire_post_wal_pre_publish_test_hook(repo, false).await;
    }

    // Phase 5a: physical data writes per table.
    //
    // Drain each per-table StagingStore once (so a retry re-applies the
    // SAME ops; idempotent at the data layer). Route through MvccStore
    // when available so version_cache stays current for SSI conflict
    // detection; fall back to direct `base.transact` otherwise.
    let data_batches = collect_data_batches(tx);
    let data_futs: Vec<_> = data_batches
        .into_iter()
        .map(|(table_id, base, ops)| async move {
            // P2 perf fix (audit 4.7): `apply_data_batch` now takes `&[KvOp]`
            // — the MVCC arm only reads it, so no clone is needed here at
            // all (the batch's own owned `ops` outlives the whole retry
            // loop; only the rare non-MVCC arm clones, internally, on its
            // own cold path).
            let res = retry_materialize(MATERIALIZE_ATTEMPTS, || {
                apply_data_batch(repo, table_id, base.clone(), &ops, commit_version, tx_id)
            })
            .await;
            (table_id, res)
        })
        .collect();
    for (table_id, res) in join_all(data_futs).await {
        if let Err(e) = res {
            log::warn!(
                "commit_tx materialization Phase 5a (data) failed for tx {tx_id} \
                 commit_version {commit_version} table {table_id}: {e}; deferring to recovery"
            );
            ok = false;
        }
    }

    // Crash seam (test-only): data (5a) is on disk but the index (5c) is
    // not, the version is unpublished, and Phase 7 has not run. The
    // inflight WAL marker survives → recovery replays the full entry.
    super::commit::fire_post_wal_pre_publish_test_hook(repo, false).await;
    maybe_crash("phase5a", repo).await;

    // Phase 5b: counter deltas (CRIT-3).
    //
    // `tx.counter_deltas` is already serialised into the WAL entry by
    // `wal_ops_from_tx`. The happy path also applies it in-memory so
    // callers observe the new count without waiting for recovery.
    //
    // Cold recovery skips CounterDelta because persisted counts may already
    // include it. Warm orphan replay instead uses the drainer's per-version,
    // per-table counter-applied membership: successful inline 5b is not
    // repeated, while an orphan cancelled before 5b gets a best-effort replay.
    // Counter failures remain metric-only drift and never flip `ok` or block
    // orphan durability; the doctor reconciles counts by a full data scan.
    apply_tracked_counter_phase(tx, repo, commit_version).await;
    super::commit::fire_post_wal_pre_publish_test_hook(repo, true).await;

    // Phase 5c: apply staged index ops (HIGH-6).
    //
    // `tx.index_write_set` is `Vec<(table_token, IndexWriteOp)>`, already
    // serialised into the WAL entry by `wal_ops_from_tx`. Group ops by
    // table_token, resolve each table once, then apply via
    // `apply_index_ops_at_commit`: SetPosting/RemovePosting hit the
    // info_store; BumpFtsStats is broadcast to the table's backends'
    // `apply_in_memory`.
    //
    // Recovery idempotency: `recovery::replay_v2_op` applies IndexPut
    // (→ info_store.set) and IndexDel (→ info_store.remove) for the same
    // postings. `set`/`remove` are last-write-wins, so re-replay converges.
    // BumpFtsStats is not serialised to the WAL; its in-memory counters are
    // rebuilt via `rebuild()` on open.
    if !tx.index_write_set.is_empty() {
        let mut by_token: TFxMap<u64, Vec<shamir_tx::IndexWriteOp>> = TFxMap::default();
        for (token, op) in std::mem::take(&mut tx.index_write_set) {
            by_token.entry(token).or_default().push(op);
        }
        let index_futs: Vec<_> = by_token
            .into_iter()
            .map(|(token, ops)| async move {
                let res = retry_materialize(MATERIALIZE_ATTEMPTS, || {
                    apply_index_batch(repo, token, &ops, tx_id, commit_version)
                })
                .await;
                (token, res)
            })
            .collect();
        for (token, res) in join_all(index_futs).await {
            if let Err(e) = res {
                log::warn!(
                    "commit_tx materialization Phase 5c (index) failed for tx {tx_id} \
                     commit_version {commit_version} table {token}: {e}; deferring to recovery"
                );
                ok = false;
            }
        }
    }

    // Crash seam (test-only): both data (5a) and index (5c) are on disk
    // but the version is still unpublished and Phase 7 has not run. The
    // inflight WAL marker survives → recovery re-applies idempotently.
    maybe_crash("phase5c", repo).await;

    // HIGH-A: release the per-table unique_write_lock guards now that the
    // unique postings are published (Phase 5c done). The window the Phase
    // 2.6 guard had to dominate — re-check → posting write — is closed, so
    // a non-tx unique writer may resume. Phases 6/6.5/7 (and the post-lock
    // HNSW promote) do not touch unique postings, so holding the locks past
    // here would only add contention. Released even on a deferred Phase 5c:
    // recovery re-applies the postings, and a stuck guard would only block
    // non-tx writers.
    //
    // F-48b (#867): drop the kept-alive writer-drain guards at the SAME
    // point — the tx's actual data/index writes have now landed, so a
    // schema-activation / index2-create DDL waiting in `drain_writers()`
    // observes the completed writes via the Release fetch_sub → Acquire
    // load pairing (see `writer_drain_barrier`'s memory-model doc). The
    // guards' Release-drop happens-after the Phase 5a/5c data-store writes
    // above, so the drainer's count-proof / backfill snapshot sees them.
    drop(uwl_guards);
    drop(drain_guards);

    // Phase 5d (HNSW promote) is NO LONGER here. It moved OUT of the
    // commit critical section: `commit_tx_inner` drops `commit_lock` after
    // Phase 7 below and then calls `promote_vectors` (III.5). HNSW is a
    // derived read-accelerator (vectors are already in `main` via Phase 5a
    // + the WAL entry; the graph is rebuilt from the data store on open),
    // so its unbounded per-vector work must not stall other committers
    // under the gate, and a failed promote does NOT defer the tx — see
    // `promote_vectors`.
    //
    // #426 (VR-4 variant A): the DELTA half of Phase 5d (`append_vector_delta`
    // + `trigger_snapshot_check`) was moved INTO the commit critical
    // section, here, BEFORE `version_guard.commit()`. The delta chunk is
    // the durable bridge `restore_on_open::replay_delta` re-materializes
    // the vectors from on the next open; appending it pre-publish closes
    // the W-2 window where a post-ack crash could leave a committed
    // vector WITHOUT a durable delta echo. The GRAPH half (in-RAM HNSW
    // mutation) stays post-lock in `promote_vectors` (III.5). A
    // delta-append failure flips `ok` → `Deferred` (detectable pre-ack,
    // unlike the prior silent post-ack loss).
    let visible = ok;
    apply_vector_delta_phase(tx, repo, commit_version).await;
    if tx.async_prefix_failed {
        ok = false;
    }

    // Crash seam (test-only): the durable delta chunk is on disk (one
    // `Store::set` per vector backend, flushed by the seam) but the version
    // is still unpublished and Phase 7 has not run. The inflight WAL marker
    // survives → recovery replays the data; `restore_on_open` replays the
    // delta chunk → the vector is searchable after restart. Proves variant
    // A: delta durable pre-publish survives a crash at this seam.
    maybe_crash("phase5d_delta", repo).await;

    // WAL acceptance is not completion: only fully visible projections let W
    // pass this version. Vector delta failure remains recovery bookkeeping.
    let changefeed_event = settle_visible(repo, version_guard, prepared_journal, visible);

    // Phase 6 settles inline success; on deferral the orphan remains pending.
    // A crash still leaves the inflight WAL entry for recovery.
    maybe_crash("phase6", repo).await;

    // A5 + Stage I: capture the max interner id across the tx's per-repo
    // delta so Phase 7 can gate WAL truncation on the repo's single
    // persisted high-water mark. Pre-Stage-I this was a per-table
    // `Vec<(token, max_id)>`; the interner is now per-repo so it collapses
    // to ONE max id (or `None` if the delta was empty).
    let interner_delta_max_id: Option<u64> = tx.interner_deltas.iter().map(|(_, id)| *id).max();

    // Phase 6.5 + 7 (markers + WAL cleanup) are returned as post-lock
    // work — the caller runs them AFTER releasing `commit_lock`. See
    // `post_publish_cleanup`.
    (
        PostPublishState {
            tx_id,
            commit_version,
            projections_ok: ok,
            interner_delta_max_id,
        },
        changefeed_event,
    )
}

/// Drainer derives events from available WAL evidence; only unknown/eventless
/// entries or failed journal admission/persistence become GAPs.
pub(in crate::tx) fn settle_visible(
    repo: &RepoCore,
    guard: shamir_tx::VersionGuard,
    prepared: Option<(
        Arc<shamir_tx::ChangelogEvent>,
        Option<Arc<shamir_tx::RepoChangefeed>>,
    )>,
    visible: bool,
) -> Option<Arc<shamir_tx::ChangelogEvent>> {
    if visible {
        super::publish_journaled::publish_journaled(repo, guard, prepared)
    } else {
        drop(guard);
        repo.drainer().wake();
        None
    }
}

pub(in crate::tx) async fn apply_tracked_counter_phase(
    tx: &TxContext,
    repo: &RepoCore,
    commit_version: u64,
) {
    let tx_id = tx.tx_id.0;
    for (table_id, delta) in &tx.counter_deltas {
        match repo.table_by_token(*table_id).await {
            Ok(Some(tbl)) => {
                if let Err(e) = tbl.counter().increment(*delta).await {
                    log::warn!(
                        "commit_tx materialization Phase 5b (counter) failed for tx {tx_id} \
                         table {table_id}: {e}; counter drift accepted (metric only)"
                    );
                } else {
                    repo.drainer()
                        .mark_counter_applied(commit_version, *table_id);
                }
            }
            Ok(None) => {}
            Err(e) => {
                log::warn!(
                    "commit_tx materialization Phase 5b (counter) table lookup failed for \
                     tx {tx_id} table {table_id}: {e}; counter drift accepted (metric only)"
                );
            }
        }
    }
}

/// State carried from [`materialize`] to [`post_publish_cleanup`].
/// Both run outside `commit_lock` (P2b).
pub(in crate::tx) struct PostPublishState {
    pub tx_id: u64,
    pub commit_version: u64,
    /// True iff data/index projections and vector delta bookkeeping succeeded.
    /// False reports Deferred; failed data/index visibility is owned by replay.
    pub projections_ok: bool,
    /// A5 + Stage I: max interner id across the WAL entry's per-repo
    /// interner delta (`None` if the delta was empty). Phase 7 checks that
    /// the repo's single persisted high-water mark covers this id before
    /// truncating the WAL entry.
    pub interner_delta_max_id: Option<u64>,
}

/// Phase 6.5 (markers), run OUTSIDE `commit_lock`.
///
/// D2 P1d-2b CUTOVER: Phase 7 (`wal.commit` + A5 interner-hwm gate) has
/// MOVED OUT of this ack-path function into the background
/// [`Drainer`](crate::drainer::Drainer). After the cutover the ack-path
/// must NOT truncate the WAL entry: the entry is the source of truth for the
/// not-yet-drained value (the ack-path wrote only the in-memory overlay, not
/// `history`). Truncating here would lose the only durable record of an
/// undrained version on a crash. So this function now does ONLY:
///   - Phase 6.5: persist `last_committed`/`next_tx_id` recovery markers
///     (visibility metadata — cheap, and consistent on crash: recovery
///     re-derives the same floor).
///   - A5 interner checkpoint (fire-and-forget, on interval) so the
///     persisted hwm advances and the DRAINER's A5 gate can truncate.
///
/// `wal.commit` is NOW exclusively the drainer's job, gated on the version
/// being durable in `history` (`replay_v2_entry` → `mark_durable` →
/// `wal.commit`). The returned `MaterializationState` combines the projection
/// result from [`materialize`] with recovery marker bookkeeping.
///
/// Marker writes do not settle in-memory visibility: inline success already
/// settled Phase 6, while failed projections remain owned by orphan replay.
/// They do not need serialisation against other committers.
pub(in crate::tx) async fn post_publish_cleanup(
    state: PostPublishState,
    repo: &RepoCore,
    gate: &RepoTxGate,
) -> MaterializationState {
    let PostPublishState {
        tx_id,
        commit_version,
        mut projections_ok,
        interner_delta_max_id,
    } = state;

    // Phase 6.5: persist recovery markers (CRIT-1).
    //
    // Write `last_committed_version` + `next_tx_id` to the repo's `__tx__`
    // info_store. Best-effort post-commit: a marker write failure is logged
    // and flips `projections_ok` → `Deferred`. Even without these markers,
    // `recover_inflight_v2` re-persists the floor (`gate.last_committed()`)
    // when it consumes the inflight entry, so the version floor is restored
    // on the next open.
    if let Err(e) = persist_markers(repo, gate, commit_version).await {
        log::warn!(
            "commit_tx materialization Phase 6.5 (recovery markers) failed for tx {tx_id} \
             commit_version {commit_version}: {e}; deferring to recovery"
        );
        projections_ok = false;
    }

    // Crash seam (test-only): visible state + markers are set but the WAL
    // entry is still inflight (it is NOT truncated here post-cutover). The
    // inflight marker survives → recovery / the drainer replays it into
    // `history` idempotently.
    maybe_crash("phase6_5", repo).await;

    // A5 + Stage I: background interner checkpoint. Every
    // INTERNER_CHECKPOINT_INTERVAL commits, persist the SINGLE repo interner
    // so its high-water mark advances and the DRAINER's A5 gate can later
    // truncate older WAL entries. Spawned fire-and-forget: the persist is not
    // on the commit critical path. Pre-Stage-I this looped over every table;
    // now one persist covers the whole repo (the manager is Arc-shared).
    if commit_version.is_multiple_of(INTERNER_CHECKPOINT_INTERVAL)
        && interner_delta_max_id.is_some()
    {
        let repo_clone = repo.clone();
        tokio::spawn(async move {
            match repo_clone.repo_interner().await {
                Ok(repo_interner) => {
                    if let Err(e) = repo_interner.persist().await {
                        log::warn!(
                            "A5 interner checkpoint failed for repo {}: {e}",
                            repo_clone.name()
                        );
                    }
                }
                Err(e) => {
                    log::warn!(
                        "A5 interner checkpoint: repo_interner resolve for {}: {e}",
                        repo_clone.name()
                    );
                }
            }
        });
    }

    // Phase 7 (`wal.commit` + A5 gate) is GONE — the background drainer owns
    // it now (see fn doc). `projections_ok` reflects only the visible
    // projections (materialize) + the marker write above.
    if !projections_ok {
        log::warn!(
            "commit_tx tx {tx_id} commit_version {commit_version} COMMITTED but \
             materialization DEFERRED — WAL marker left inflight; recovery / drainer \
             will reconcile on the next pass"
        );
    }

    if projections_ok {
        MaterializationState::Complete
    } else {
        MaterializationState::Deferred
    }
}

/// CRIT-2 (#436) test-injection seam: when set to `true`, force
/// `interner_delta_safe_to_truncate` to return `Ok(false)` (unsafe),
/// simulating an interner delta id that is NOT yet persisted. Mirrors
/// the `FAIL_HISTORY_SEED_TX_ID` pattern in `recovery/v2.rs` but is a
/// simple boolean latch because the regression test needs to gate the
/// WHOLE A5 decision, not a single txn (the drainer's Phase C pass
/// calls the gate once per entry; a static-atomic latch flips ALL of
/// them in one pass).
///
/// `#[cfg(debug_assertions)]` so a `--release` build pays zero cost —
/// the static is absent and the load branch is elided. Tests MUST
/// reset to `false` between runs to avoid cross-test bleed.
#[cfg(debug_assertions)]
pub(crate) static FORCE_A5_UNSAFE: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// A5 interner-hwm gate: true iff WAL truncation for an entry carrying this
/// interner delta max-id is safe — i.e. the repo's persisted interner
/// high-water mark covers the entry's max id.
///
/// Shared by the inline Phase-7 path ([`post_publish_cleanup`]) and the
/// background [`Drainer`](crate::drainer::Drainer): truncating an entry
/// whose delta references intern-ids NOT yet persisted would, on a crash,
/// leave the interner unable to decode the very records the (now-truncated)
/// entry would have re-supplied. The entry MUST stay inflight until a future
/// checkpoint advances the hwm.
///
/// Stage I: the interner is per-REPO (one id-namespace across tables), so
/// there is ONE persisted high-water mark. Pre-Stage-I this took a
/// `Vec<(table_token, max_id)>` and looped per-token; now it takes a single
/// `Option<u64>` (the max id across the whole delta, or `None` if the delta
/// was empty — trivially safe).
pub(crate) async fn interner_delta_safe_to_truncate(
    repo: &RepoCore,
    interner_delta_max_id: Option<u64>,
) -> shamir_storage::error::DbResult<bool> {
    // CRIT-2 (#436) test-injection: force UNSAFE so the drainer's
    // truncation-ceiling fix can be exercised. The whole branch is
    // `#[cfg(debug_assertions)]` so release builds elide it.
    #[cfg(debug_assertions)]
    if FORCE_A5_UNSAFE.load(std::sync::atomic::Ordering::SeqCst) {
        return Ok(false);
    }

    let Some(max_id) = interner_delta_max_id else {
        return Ok(true);
    };
    let repo_interner = repo.repo_interner().await?;
    let hwm = repo_interner.persisted_high_water() as u64;
    if max_id > hwm {
        log::debug!(
            "interner-hwm gate: repo delta max_id {max_id} > persisted hwm {hwm}; \
             truncation unsafe"
        );
        return Ok(false);
    }
    Ok(true)
}

/// Promote one table's staged HNSW vectors + vector deletes into the live
/// in-RAM graph (Phase 5d GRAPH half, post-lock — see [`promote_vectors`](super::commit_phases::promote_vectors)).
///
/// gap#1 / HIGH-6: `deleted` is this table's slice of staged vector-delete
/// rids (`tx.staged_vector_deletes_for(token)`). The promote order is
/// delete-then-insert: a replace (delete old + insert new on the same rid)
/// converges to the NEW vector live (HNSW `upsert` re-inserts a tombstoned
/// node; `delete` on a not-yet-inserted node is a no-op, so either order is
/// safe, but delete-then-insert matches the replace intent literally).
///
/// #426 (VR-4 variant A): this is the GRAPH half only. The DURABLE delta
/// chunk (`DeltaOp::Upsert`/`Delete`) is appended PRE-publish by
/// [`apply_vector_delta_batch`] (called from [`apply_vector_delta_phase`](super::commit_phases::apply_vector_delta_phase)),
/// so a post-lock graph-promote failure leaves the mutation durable in the
/// delta-log — `restore_on_open::replay_delta` re-materializes it on the
/// next open.
pub(crate) async fn apply_vector_graph_batch(
    repo: &RepoCore,
    token: u64,
    vecs: &[(shamir_types::types::record_id::RecordId, Vec<f32>)],
    deleted: &[shamir_types::types::record_id::RecordId],
    _tx_id: u64,
    tickets: &[shamir_tx::VectorPromoteTicket],
) -> Result<(), DbError> {
    // Test-only failure injection: simulate a persistent post-lock HNSW
    // graph-promote error for a specific tx so the III.5 contract (a failed
    // graph promote after the lock + Phase 7 does NOT defer the tx) can be
    // exercised in-process. Armed via `FAIL_VECTOR_PROMOTE_TX_ID`. NOTE:
    // this injection fires ONLY for the graph half; the delta half has its
    // own injection (`FAIL_VECTOR_DELTA_TX_ID`) and its own (Deferred)
    // failure contract.
    #[cfg(test)]
    if _tx_id != 0 && FAIL_VECTOR_PROMOTE_TX_ID.load(std::sync::atomic::Ordering::SeqCst) == _tx_id
    {
        return Err(DbError::Internal(format!(
            "injected Phase 5d (hnsw graph promote) failure for tx {_tx_id}"
        )));
    }

    // P0 index promote order (stage L3b, round-2 finding 1 restructure): with
    // per-backend tickets from the DELTA half, promote through the turnstile
    // — wait_turn BEFORE the backend's `enter_write`, deletes + inserts as
    // ONE ordered unit inside the turn, turn released ONLY after the ticket's
    // bounded retry loop finishes (success or persistent failure).
    //
    // DEADLOCK-FREEDOM INDUCTION ARGUMENT (round-2 finding 2, precise form).
    // Fix ONE global total order on every (table token, backend descriptor
    // id) pair: `apply_vector_delta_phase` collects its insert ∪ delete-only
    // tokens into one sorted Vec (ascending token) and
    // `apply_vector_delta_batch` iterates each table's backends sorted by
    // descriptor id, so RESERVATIONS follow the order; `promote_vectors`
    // merges its insert ∪ delete-only tokens into the same sorted Vec, so
    // PROMOTION visits the identical order. Every tx therefore reserves and
    // promotes its pairs in one global total order.
    //   RESERVATIONS DO BLOCK: an unreleased reserved index keeps the
    //   backend's `promote_turn` below every LATER index on that backend, so
    //   a tx holding reservations on later pairs can block other txs while
    //   it waits. Acyclicity therefore rests on the global order alone:
    //   every tx visits its pairs in the same total order (B, idx) and
    //   releases each pair's turn (after its bounded retry loop) before
    //   waiting for the next.
    //   NO CYCLE: suppose txs wait in a cycle. Let B be the smallest
    //   backend (in the global order) any cycle member is waiting on, and
    //   among the waiters on B take the one with the smallest idx k. It
    //   waits for an unreleased earlier index j < k on B held by a tx Y.
    //   Y has not released j, so Y has not finished pair (B, j): Y is either
    //   promoting it (bounded, then releases - not waiting) or waiting for
    //   an earlier pair, i.e. on a backend <= B; B is the smallest any
    //   cycle member waits on, so that wait is on B with an idx < j < k,
    //   contradicting the choice of k. Hence no wait cycle.
    //
    // A FAILED attempt must NEVER release the ticket before the retry loop
    // ends (round-2 finding 1): releasing (or dropping) a ticket whose
    // promote failed and then re-invoking the apply would retry on a
    // CONSUMED ticket whose `promote` used to answer a phantom `Ok(())`,
    // turning a real adapter failure into an acknowledged, prunable chunk.
    // The ticket stays ACTIVE across all attempts (the apply is idempotent —
    // last-write-wins on the adapter); `release_turn` runs only after the
    // loop, then the outcome propagates. Never release a ticket whose
    // promote failed before the loop ends.
    if !tickets.is_empty() {
        for t in tickets {
            t.wait_turn().await;
            let mut res = Err(DbError::Internal(
                "hnsw ordered promote: no attempt run".into(),
            ));
            for _ in 0..MATERIALIZE_ATTEMPTS.max(1) {
                // Round-2 finding 1 test injection: fail an attempt at the
                // APPLY level, INSIDE the ticket loop (after `wait_turn`,
                // before `promote`) while the turn is still HELD — the
                // in-loop retry re-runs and succeeds. Mirrors the
                // `FAIL_PHASE_5A_TRANSIENT_REMAINING` remaining-failures
                // pattern so only the armed tx's first attempts fail.
                #[cfg(test)]
                if _tx_id != 0
                    && FAIL_VECTOR_PROMOTE_ONCE_TX_ID.load(std::sync::atomic::Ordering::SeqCst)
                        == _tx_id
                    && FAIL_VECTOR_PROMOTE_ONCE_REMAINING
                        .fetch_update(
                            std::sync::atomic::Ordering::SeqCst,
                            std::sync::atomic::Ordering::SeqCst,
                            |r| if r > 0 { Some(r - 1) } else { None },
                        )
                        .is_ok()
                {
                    res = Err(DbError::Internal(format!(
                        "injected TRANSIENT Phase 5d (hnsw graph promote) \
                         failure for tx {_tx_id}"
                    )));
                    continue;
                }
                res = t
                    .promote(vecs, deleted)
                    .await
                    .map_err(|e| DbError::Internal(format!("hnsw ordered promote at commit: {e}")));
                if res.is_ok() {
                    break;
                }
            }
            // Release THIS ticket's turn only AFTER its retry loop finished,
            // before touching the next one: no unreleased turn is ever held
            // across a wait (see induction above). Explicit release consumes
            // the ticket, so its Drop cannot double-release.
            t.release_turn();
            res?;
        }
        return Ok(());
    }

    if let Some(tbl) = repo.table_by_token(token).await? {
        for backend in tbl.index2_registry().all_backends().await {
            // gap#1: tombstone the deleted rids on the live graph FIRST,
            // so a replace (same rid) leaves the NEW vector live after the
            // insert promote below. `apply_staged_vector_deletes` is a
            // no-op for non-vector backends.
            if !deleted.is_empty() {
                backend
                    .apply_staged_vector_deletes(deleted)
                    .await
                    .map_err(|e| {
                        DbError::Internal(format!(
                            "hnsw apply_staged_vector_deletes at commit: {e}"
                        ))
                    })?;
            }
            backend.apply_staged_vectors(vecs).await.map_err(|e| {
                DbError::Internal(format!("hnsw apply_staged_vectors at commit: {e}"))
            })?;
        }
    }
    Ok(())
}

/// Append one table's durable delta-log chunk for its staged vectors +
/// deletes (Phase 5d DELTA half, pre-publish — see
/// [`apply_vector_delta_phase`](super::commit_phases::apply_vector_delta_phase)).
///
/// #426 (VR-4 variant A): this is the DELTA half only. It runs BEFORE
/// `version_guard.commit()` so the delta chunk is durable by the time the
/// tx is reader-visible. A restart loads snapshot + delta via
/// `restore_on_open::replay_delta` and re-materializes the vectors. The
/// GRAPH half ([`apply_vector_graph_batch`]) stays post-lock.
///
/// Idempotency: `replay_delta` applies ops via `adapter.upsert/delete`
/// (last-write-wins); a duplicate chunk on retry overwrites the same
/// `next_delta_idx` key (`Store::set` is last-writer-wins).
pub(crate) async fn apply_vector_delta_batch(
    repo: &RepoCore,
    token: u64,
    vecs: &[(shamir_types::types::record_id::RecordId, Vec<f32>)],
    deleted: &[shamir_types::types::record_id::RecordId],
    _tx_id: u64,
) -> Result<(Vec<VectorDeltaAck>, Vec<shamir_tx::VectorPromoteTicket>), DbError> {
    // Test-only failure injection (#426, VR-4 variant A): simulate a
    // persistent PRE-publish delta-append error for a specific tx so the
    // Deferred-failure contract (a failed delta append surfaces as
    // `MaterializationState::Deferred`, NOT a silent Complete) can be
    // exercised in-process. Armed via `FAIL_VECTOR_DELTA_TX_ID`. This is
    // DISTINCT from `FAIL_VECTOR_PROMOTE_TX_ID` (graph half): a delta-half
    // failure loses the durable bridge, so it MUST be reported; a
    // graph-half failure leaves the delta durable, so it stays Complete.
    #[cfg(test)]
    if _tx_id != 0 && FAIL_VECTOR_DELTA_TX_ID.load(std::sync::atomic::Ordering::SeqCst) == _tx_id {
        return Err(DbError::Internal(format!(
            "injected Phase 5d (delta-append) failure for tx {_tx_id}"
        )));
    }

    let mut acks: Vec<VectorDeltaAck> = Vec::new();
    let mut tickets: Vec<shamir_tx::VectorPromoteTicket> = Vec::new();
    if let Some(tbl) = repo.table_by_token(token).await? {
        let info_store = tbl.info_store().clone();
        // P0 index promote order (RV finding 2a): iterate the table's
        // backends in the GLOBAL TOTAL ORDER (ascending descriptor id), not
        // the registry's iteration order — the ticket vec minted below (and
        // consumed by `apply_vector_graph_batch`) must be identical in every
        // transaction, or two txs can wait in opposite orders and deadlock.
        let mut backends = tbl.index2_registry().all_backends().await;
        backends.sort_by_key(|b| shamir_index::IndexBackend::descriptor(b.as_ref()).id);
        for backend in backends {
            // V2.3 (#402) — durable delta-log append + snapshot trigger.
            // The delta chunk captures the vectors just promoted AND the
            // deletes just tombstoned so a restart replays both over the
            // base snapshot. The append is ONE `Store::set` (§5.6 — cheap,
            // runs on the ack path). The snapshot trigger is a
            // `tokio::spawn` when the threshold is crossed (§5.6 — the
            // dump itself is off the ack path).
            //
            // gap#1 (variant-A, landed): tx-path vector deletes are now
            // staged in `TxContext::staged_vector_deletes`, and handed
            // here as `deleted` so the delta chunk carries a durable
            // `DeltaOp::Delete` per rid. A restart replays the delete over
            // the base snapshot (`replay_delta` applies `DeltaOp::Delete`
            // via `adapter.delete`), closing the post-restart ghost. The
            // mechanism was proven by
            // `delta_log_tests::append_vector_delta_with_deleted_slice_persists_and_replays_delete`.
            //
            // #426 (VR-4 variant A): this call was MOVED here from the old
            // post-lock `apply_vector_batch` so the chunk is durable
            // PRE-publish. The graph-side `apply_staged_vector_deletes` +
            // `apply_staged_vectors` stayed post-lock in
            // `apply_vector_graph_batch`.
            let reserved = backend
                .append_vector_delta(&info_store, vecs, deleted)
                .await
                .map_err(|e| DbError::Internal(format!("delta append at commit: {e}")))?;
            backend.trigger_snapshot_check(&info_store);
            if let Some(idx) = reserved {
                // P0 delta-prune: hand the caller an acknowledgement closure.
                // It is called ONLY after this table's graph promote succeeded,
                // which is what makes the chunk prunable.
                let b = Arc::clone(&backend);
                acks.push(Arc::new(move || b.mark_delta_applied(idx)));
                // P0 index promote order: the Drop-guard turnstile ticket.
                // Minted synchronously after the await — no cancellation
                // window can drop a reserved chunk without its ticket.
                tickets.push(shamir_index::promote_ticket(Arc::clone(&backend), idx));
            }
        }
    }
    Ok((acks, tickets))
}

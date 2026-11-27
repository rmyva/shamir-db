use shamir_collections::TFxMap;
use shamir_tx::{IsolationLevel, TxContext};
use shamir_types::types::common::THasher;

#[cfg(any(test, feature = "test-util"))]
use super::entry::fire_post_wal_pre_publish_test_hook;
use super::entry::{
    commit_wal_durability, fire_async_index_tail_test_hook, fire_cancel_test_hook,
    fire_post_validate_pre_publish_test_hook, maybe_crash, wal_completion, TxError,
};
use crate::commit::core::RepoCore;
use crate::commit::pipeline::admission::lease::CommitAdmissionLease;
use crate::commit_phases::{
    apply_data_phase, apply_vector_delta_phase, materialize_async_tail, promote_vectors,
};
use crate::finalize::finalize_sync_post_publish;
use crate::materialize::materialize;
use crate::pre_commit::{pre_commit_locked, pre_commit_locked_validate, PreCommit};
use crate::tx_outcome::{BackgroundCommitHandle, MaterializationState, TxOutcome};

/// AsyncIndex commit path: uses the traditional sequential commit_lock.
/// Group-commit does not apply because the background-task tail is
/// incompatible with leader-processed materialization.
pub(super) async fn commit_tx_inner_legacy_async(
    mut tx: TxContext,
    uwl_guards: Vec<tokio::sync::OwnedMutexGuard<()>>,
    drain_guards: Vec<crate::table::writer_drain_barrier::WriterDrainGuard>,
    repo: &RepoCore,
    gate: &shamir_tx::RepoTxGate,
    wal: &shamir_tx::RepoWalManager,
    admission_lease: CommitAdmissionLease,
) -> Result<TxOutcome, TxError> {
    let commit_guard = gate.commit_lock().await;

    let PreCommit {
        commit_version,
        uwl_guards,
        drain_guards,
        version_guard,
        cell_guards,
        wal_entry_arc,
        durability_failed,
    } = match pre_commit_locked(&mut tx, repo, gate, wal, uwl_guards, drain_guards).await {
        Ok(Some(pc)) => pc,
        Ok(None) => {
            repo.tx_metrics().on_tx_committed();
            release_pessimistic_locks(&tx, repo).await;
            drop(commit_guard);
            return Ok(TxOutcome {
                tx_id: tx.tx_id.0,
                snapshot_version: tx.snapshot_version,
                commit_version: tx.snapshot_version,
                materialization: MaterializationState::Complete,
                visibility: shamir_query_types::batch::VisibilityState::Visible,
                background: None,
            });
        }
        Err(e) => {
            release_pessimistic_locks(&tx, repo).await;
            return Err(e);
        }
    };
    // Op #2 Stage 2: offer the persisted WAL entry to the drainer window.
    // S5c-X I1: the event below is derived FROM THIS ENTRY (one projection,
    // `shamir_tx::event_from_wal_entry`) - an `Arc` clone, no data copy.
    let wal_entry_for_event = wal_entry_arc.clone();
    repo.drainer().offer(wal_entry_arc);
    // SSI fix S2 — still-armed cell-reservation guards from pre_commit_locked
    // (WAL begin already succeeded inside it). Disarmed after the inline Phase 5a
    // (`apply_data_phase`) finalizes the cells below.
    let mut cell_guards = cell_guards;

    let snapshot_version = tx.snapshot_version;
    let tx_id_u64 = tx.tx_id.0;
    // S5c-X I1: derive the journal event from the WAL entry, not from the
    // tx (ONE projection for live commits and recovery).
    let changefeed_event = shamir_tx::event_from_wal_entry(wal_entry_for_event.as_ref());
    let prepared_journal = repo.prepare_journal_event(changefeed_event).await;

    apply_data_phase(&mut tx, repo, commit_version).await;
    let data_visible = !tx.async_prefix_failed;
    #[cfg(any(test, feature = "test-util"))]
    fire_post_wal_pre_publish_test_hook(repo, false).await;
    // SSI fix S2 — Phase 5a (`apply_data_phase` → `apply_committed_visible`)
    // finalized every claimed cell (version published, `reserved_by` cleared).
    // Disarm so the guards' `Drop` is a no-op.
    for g in &mut cell_guards {
        g.disarm();
    }
    drop(cell_guards);
    crate::materialize::apply_tracked_counter_phase(&tx, repo, commit_version).await;
    // #426 (VR-4 variant A): append the durable vector delta-log chunk(s)
    // BEFORE `version_guard.commit()` (pre-publish). The delta chunk is the
    // bridge `restore_on_open::replay_delta` re-materializes vectors from
    // on the next open; appending it pre-publish closes the W-2 window. The
    // GRAPH half (in-RAM HNSW mutation) stays in `promote_vectors` inside
    // the spawned tail (post-lock, III.5). A delta-append failure sets
    // `tx.async_prefix_failed` → the ack-path reports `Deferred` (NOT a
    // silent `Complete`) so the client can detect the missing durable echo.
    apply_vector_delta_phase(&mut tx, repo, commit_version).await;
    let delta_deferred = tx.async_prefix_failed;

    // Crash seam (test-only): fires BEFORE publish, mirroring the sync
    // path's `phase5d_delta` seam in `materialize.rs` — the durable delta
    // chunk is on disk but the version is not yet published and Phase 7
    // has not run. Placed here (before `version_guard.commit()`), not
    // after, so it actually proves what its name claims: a crash between
    // the pre-publish delta-append and publish still leaves the chunk
    // durable for `restore_on_open::replay_delta` to pick up.
    maybe_crash("phase5d_delta_async", repo).await;

    // The SSI footprint was recorded immediately after WAL acceptance,
    // before any cancellable materialization. Both normal publish and orphan
    // replay therefore expose a version whose footprint is already recorded.

    // Settle Phase 5a success, or transfer failed data visibility to orphan
    // replay. WAL acceptance alone must not let the snapshot watermark pass.
    let changefeed_event =
        crate::materialize::settle_visible(repo, version_guard, prepared_journal, data_visible);

    // A4 (fix a): release Level-3 pessimistic locks AFTER the publish step
    // (`apply_data_phase` + `version_guard.commit()` above) is complete,
    // never before. 2PL requires every lock to stay held until the tx's
    // outcome is fully visible to others: releasing the Exclusive lock on a
    // key BEFORE its new value is published lets a second tx acquire the
    // lock and read/write the key while this tx's write is still in-flight
    // (WAL-durable but not yet cell-published) — a lost-update protocol
    // violation. Abort / early-exit paths above (lines ~435/440/462/515/526)
    // release immediately because no write was ever published there.
    release_pessimistic_locks(&tx, repo).await;

    maybe_crash("phase6_async", repo).await;
    drop(commit_guard);

    #[cfg(any(test, feature = "test-util"))]
    repo.fire_post_publish_pre_broadcast_test_hook().await;
    repo.broadcast_changefeed_event(changefeed_event);

    let repo_clone = repo.clone();
    let metrics = repo.tx_metrics().clone();
    // Enter before spawn while the inline driver is still counted: no zero-count gap.
    let tail_guard = repo.commit_tail().enter();
    // R2a 1c: the admission lease moves INTO the spawned tail (enters-before-
    // spawn, same no-gap argument as `tail_guard`): the AsyncIndex materiali-
    // zation stays counted for admission until it finishes. Every early
    // return above dropped the lease instead — no gap, no leak.
    let join = tokio::spawn(async move {
        let _admission_lease = admission_lease;
        let _tail_guard = tail_guard;
        fire_async_index_tail_test_hook(&repo_clone).await;
        let state = materialize_async_tail(
            &mut tx,
            &repo_clone,
            commit_version,
            uwl_guards,
            drain_guards,
        )
        .await;
        if state == MaterializationState::Deferred {
            metrics.on_tx_materialization_deferred();
        }
        // D2 P1d-2b CUTOVER: the inline `gate.mark_durable(commit_version)` is
        // GONE here too. The ack-path wrote only the overlay (visible half);
        // the value becomes durable in `history` only after the background
        // drainer replays the WAL entry. The drainer owns `mark_durable` + WAL
        // truncation. Wake it after the version is published so it drains the
        // freshly-committed tail promptly.
        repo_clone.drainer().wake();
        promote_vectors(&tx, &repo_clone, commit_version).await;
        state
    });

    repo.tx_metrics().on_tx_committed();

    // #426 (VR-4 variant A): a pre-publish delta-append failure
    // (`tx.async_prefix_failed` set by `apply_vector_delta_phase`) surfaces
    // as `Deferred` in the ack-path TxOutcome so the client detects the
    // missing durable vector echo and can retry — NOT a silent `Complete`.
    let materialization = if delta_deferred {
        repo.tx_metrics().on_tx_materialization_deferred();
        MaterializationState::Deferred
    } else {
        MaterializationState::Complete
    };

    if durability_failed {
        let tail = join.await.unwrap_or(MaterializationState::Deferred);
        let materialization = if delta_deferred || tail == MaterializationState::Deferred {
            MaterializationState::Deferred
        } else {
            MaterializationState::Complete
        };
        return Err(TxError::WalDurability {
            commit_version,
            materialization,
            visibility: shamir_query_types::batch::VisibilityState::Visible,
        });
    }
    Ok(TxOutcome {
        tx_id: tx_id_u64,
        snapshot_version,
        commit_version,
        materialization,
        visibility: shamir_query_types::batch::VisibilityState::Visible,
        background: Some(BackgroundCommitHandle { join }),
    })
}

/// P2c lockfree commit path: no global `commit_mutex`.
///
/// Per-table uwl_guards (acquired in pre_commit_prelock in sorted token
/// order) provide the serialization invariant: same-table committers
/// serialize at uwl acquisition; disjoint-table committers proceed fully
/// in parallel. The SSI footprint (`record_commit_writes`) is inserted
/// into the lock-free `scc::TreeIndex` BEFORE `publish_committed_max`
/// advances the reader-visible floor, so future SSI validators see it.
pub(super) async fn commit_tx_lockfree(
    mut tx: TxContext,
    uwl_guards: Vec<tokio::sync::OwnedMutexGuard<()>>,
    drain_guards: Vec<crate::table::writer_drain_barrier::WriterDrainGuard>,
    repo: &RepoCore,
    gate: &shamir_tx::RepoTxGate,
    wal: &shamir_tx::RepoWalManager,
) -> Result<TxOutcome, TxError> {
    // CRIT-4 (#438): Serializable txs must serialize the validate→publish
    // window. The lock-free path relies on per-table `uwl_guards` for
    // serialization, but those are acquired ONLY for tables with unique
    // constraints (`pre_commit_prelock`). A Serializable tx with NO unique
    // constraints is not serialized anywhere in this path, so two such txs
    // with disjoint write-sets but a rw-antidependency cycle (classic
    // write-skew: A reads x / writes y, B reads y / writes x) can BOTH pass
    // `pre_commit_locked_validate` and then BOTH publish — violating
    // Serializable.
    //
    // Fix: take `gate.commit_lock()` for the validate→publish window when
    // the tx is Serializable, mirroring exactly the section the legacy
    // (`commit_tx_inner_legacy_async`) path already holds under the same
    // mutex. Snapshot txs do NOT need SSI validation and keep the full
    // lock-free parallelism — the branch below is a pure `if` with no lock
    // taken on the Snapshot path (zero overhead).
    //
    // FG-7: ALSO take the lock when `cas_set` is non-empty, regardless of
    // isolation. A CAS tx (even under Snapshot) needs the SAME
    // validate→publish atomicity Serializable already relies on — otherwise
    // two Snapshot CAS writers could both pass the new Phase CAS check and
    // then both publish, reproducing the exact race this task closes. A
    // Snapshot tx with NO CAS keys is completely unaffected (empty-set
    // check, no lock taken).
    //
    // No deadlock / self-block risk: `commit_tx_lockfree` has a single call
    // site (`commit_tx_inner` line ~490), which itself never holds
    // `commit_lock`. The AsyncIndex path (`commit_tx_inner_legacy_async`)
    // acquires `commit_lock` itself but RETURNS at line ~471 before the
    // control flow reaches this function, so no caller arrives here already
    // holding the mutex. `tokio::sync::Mutex` is non-reentrant, so this
    // invariant is load-bearing.
    // F-40b (RI barrier): ALSO take the lock when `ri_barrier_tokens` is
    // non-empty — a Snapshot tx that recorded an FK-child scan needs the
    // SAME validate→publish window serialization Serializable already
    // relies on, so the barrier's `predicate_conflicts_batch` scan sees a
    // stable commit window (mirrors the Serializable branch's rationale
    // above). A Snapshot tx with no barrier tokens is completely unaffected
    // (the `is_empty` check short-circuits before any lock work).
    //
    // F-46 (#857, P0 — 2026-07-28 readonly review's P0-1): ALSO take the
    // lock when `footprint_tokens` is non-empty. Without this, the RI
    // barrier above is one-sided: the PARENT-side RI-barrier check takes
    // `commit_lock` and re-validates against the commit window, but a
    // concurrent Snapshot WRITER into the same FK-child table only ever
    // gets `footprint_tokens` populated (via `require_footprint_for`,
    // called unconditionally by `require_footprint_if_fk_child` at every
    // insert/update/set into an FK-child table) — never
    // `ri_barrier_tokens` (that field is written only by the reverse-FK
    // *scan* sites in `fk_restrict.rs`/`fk_actions.rs`/`fk_on_update.rs`,
    // which the plain writer never runs). Before this widening, such a
    // writer could publish (WAL write + `record_commit_writes` +
    // materialize) WHILE the parent held `commit_lock` and had already
    // passed its own Phase 2-bis `predicate_conflicts_batch` check —
    // slipping in behind the barrier's back and defeating its
    // mutual-exclusion intent (proven by
    // `fk_ri_barrier_tests.rs`'s
    // `concurrent_child_publish_during_parent_commit_window_is_serialized_on_delete_{restrict,cascade,set_null}`
    // / `..._on_update`, using the `TEST_POST_VALIDATE_PRE_PUBLISH_HOOK`
    // seam above to park a commit-under-test strictly inside this exact
    // window). Widening this condition makes every FK-child writer
    // participate in the SAME validate→publish serialization the RI
    // barrier already relies on, closing the gap symmetrically: whichever
    // side reaches `commit_lock` first now genuinely excludes the other
    // for the whole window, not just for the barrier's own read. A
    // Snapshot tx that never touches an FK-child table has empty
    // `footprint_tokens` and pays only the pre-existing single `is_empty`
    // check — zero additional cost on that path.
    let _serializable_guard = if tx.isolation == IsolationLevel::Serializable
        || !tx.cas_set.is_empty()
        || !tx.ri_barrier_tokens_is_empty()
        || !tx.footprint_tokens.is_empty()
    {
        Some(gate.commit_lock().await)
    } else {
        None
    };

    // Phase 3 + 2 + 2-bis + WAL entry build (no lock needed).
    let validated =
        match pre_commit_locked_validate(&mut tx, repo, gate, uwl_guards, drain_guards).await {
            Ok(Some(v)) => v,
            Ok(None) => {
                // C6 empty-tx fast-path.
                repo.tx_metrics().on_tx_committed();
                release_pessimistic_locks(&tx, repo).await;
                return Ok(TxOutcome {
                    tx_id: tx.tx_id.0,
                    snapshot_version: tx.snapshot_version,
                    commit_version: tx.snapshot_version,
                    materialization: MaterializationState::Complete,
                    visibility: shamir_query_types::batch::VisibilityState::Visible,
                    background: None,
                });
            }
            Err(e) => {
                release_pessimistic_locks(&tx, repo).await;
                return Err(e);
            }
        };

    let commit_version = validated.commit_version;
    let mut version_guard = validated.version_guard;
    version_guard.attach_metrics(repo.tx_metrics().clone());
    // SSI fix S2 — the still-armed cell-reservation guards. Held across Phase 4
    // and `materialize`; `disarm`ed after publish (Phase 5a `finalize_reservation`
    // already cleared each `reserved_by`). On the WAL-begin abort below they drop
    // → release every claimed cell (I-PreWAL: a loser never strands a claim).
    let mut cell_guards = validated.cell_guards;

    // F-46 (#857) test-only seam: fires strictly after Phase 2-bis's
    // `predicate_conflicts_batch` (the RI barrier's commit-time re-check) has
    // already passed, and strictly before Phase 4's WAL begin / publish. See
    // `fire_post_validate_pre_publish_test_hook`'s doc. No-op unless a test
    // installed `TEST_POST_VALIDATE_PRE_PUBLISH_HOOK`.
    fire_post_validate_pre_publish_test_hook().await;
    fire_cancel_test_hook(repo, true).await;

    // Phase 4: WAL begin — THE COMMIT POINT.
    // Concurrent-safe: each tx writes a unique WAL key (txn_id).
    // Record before queueing: an unwritten abort may cause spurious SSI
    // conflicts, but a written orphan must never expose a missed conflict.
    gate.record_commit_writes(shamir_tx::build_footprint_from_tx(&tx, commit_version));
    let queued = wal.begin_grouped_ticketed(
        &validated.wal_entry_arc,
        commit_wal_durability(&repo.durability_handle, repo.name()),
    );
    let (ticket, completion) = match queued {
        Ok(queued) => queued,
        Err(e) => {
            repo.settle_unwritten_version(commit_version).await;
            release_pessimistic_locks(&tx, repo).await;
            return Err(e.into());
        }
    };
    version_guard.wal_queued(ticket, validated.wal_entry_arc.clone());
    let (wc_result, proven_not_written) = wal_completion(gate, commit_version, completion.await);
    if proven_not_written {
        repo.settle_unwritten_version(commit_version).await;
    }
    let durability_failed = match wc_result {
        Ok(failed) => failed,
        Err(e) => {
            drop(version_guard);
            drop(cell_guards);
            release_pessimistic_locks(&tx, repo).await;
            return Err(e);
        }
    };

    // Op #2 Stage 2: offer the persisted WAL entry to the drainer window.
    // S5c-X I1: the event below is derived FROM THIS ENTRY (one projection,
    // `shamir_tx::event_from_wal_entry`) - an `Arc` clone, no data copy.
    let wal_entry_for_event = validated.wal_entry_arc.clone();
    repo.drainer().offer(validated.wal_entry_arc);

    maybe_crash("phase4", repo).await;

    // SSI footprint already recorded at WAL acceptance above, before replay
    // could take ownership of a cancelled commit's version.

    repo.tx_metrics().on_tx_committed();

    // Phases 5–7: materialize OUTSIDE any global lock, gated by uwl_guards.
    let snapshot_version = tx.snapshot_version;
    let tx_id_u64 = tx.tx_id.0;
    // S5c-X I1: derive the journal event from the WAL entry, not from the
    // tx (ONE projection for live commits and recovery).
    let changefeed_event = shamir_tx::event_from_wal_entry(wal_entry_for_event.as_ref());
    let prepared_journal = repo.prepare_journal_event(changefeed_event).await;

    let (post_publish, changefeed_event) = materialize(
        &mut tx,
        repo,
        version_guard,
        prepared_journal,
        validated.uwl_guards,
        validated.drain_guards,
    )
    .await;
    // SSI fix S2 — publish (Phase 5a `apply_committed_visible`) ran inside
    // `materialize` and `finalize_reservation`'d every claimed cell (version
    // published, `reserved_by` cleared). Disarm the guards so their `Drop` does
    // NOT redundantly release: the claims are gone and the version is the cell's
    // published state now. (A deferred Phase 5a still cleared the reservation for
    // the keys it published; any not-yet-finalized key is released by drop —
    // harmless, the cell version is the source of truth either way.)
    for g in &mut cell_guards {
        g.disarm();
    }
    drop(cell_guards);
    // CRIT-4 (#438): release the Serializable serialization window now that
    // publish (inside `materialize` above) is complete. Holding past this
    // point would needlessly block the next Serializable committer through
    // `finalize_sync_post_publish`, which is post-publish tail work.
    drop(_serializable_guard);

    // A4 (fix a): release Level-3 pessimistic locks AFTER the publish step
    // (`materialize` above, which performs Phase 5a `apply_committed_visible`
    // + `finalize_reservation` + `version_guard.commit()`) is complete, never
    // before. 2PL requires every lock to stay held until the tx's outcome is
    // fully visible to others: releasing the Exclusive lock on a key BEFORE
    // its new value is published lets a second tx acquire the lock and
    // read/write the key while this tx's write is still in-flight
    // (WAL-durable but not yet cell-published) — a lost-update protocol
    // violation. Abort / early-exit paths above (lines ~679/689/716) release
    // immediately because no write was ever published there.
    release_pessimistic_locks(&tx, repo).await;

    let materialization = finalize_sync_post_publish(
        &tx,
        post_publish,
        changefeed_event,
        repo,
        gate,
        commit_version,
    )
    .await;

    if durability_failed {
        return Err(TxError::WalDurability {
            commit_version,
            materialization,
            visibility: shamir_query_types::batch::VisibilityState::Visible,
        });
    }
    Ok(TxOutcome {
        tx_id: tx_id_u64,
        snapshot_version,
        commit_version,
        materialization,
        visibility: shamir_query_types::batch::VisibilityState::Visible,
        background: None,
    })
}

/// Release every Level-3 pessimistic lock the tx holds, grouped by table.
///
/// Called on BOTH the commit-success path and every abort path of
/// [`crate::commit::commit_tx_inner`]. `locked_keys` is empty for Snapshot / Serializable
/// txs (they never acquire locks), so this is a no-op for them — zero
/// overhead on the non-Pessimistic commit paths. Each `(table_token, key)`
/// is routed to the corresponding table's `MvccStore::release_locks`.
pub async fn release_pessimistic_locks(tx: &TxContext, repo: &RepoCore) {
    if tx.locked_keys.is_empty() {
        return;
    }
    // Group keys by table_token so each MvccStore is hit once. `locked_keys`
    // is an `scc::HashMap<(u64, RecordKey), ()>` (task #532); scan into a std
    // map (the synchronous visitor cannot await inside). Keys are `RecordKey`
    // throughout — `release_locks` is `RecordKey`-keyed, no `Bytes` round-trip.
    // O(N) ack: per-tx locked-keys sizing, bounded by this tx's footprint.
    #[allow(clippy::disallowed_methods)]
    let mut by_table: TFxMap<u64, Vec<shamir_storage::types::RecordKey>> =
        TFxMap::with_capacity_and_hasher(tx.locked_keys.len(), THasher::default());
    tx.locked_keys.iter_sync(|(token, key), _| {
        by_table.entry(*token).or_default().push(key.clone());
        true
    });
    let mvcc_map = repo.per_table_mvcc();
    for (token, keys) in by_table {
        // Clone the Arc out: a `get_sync` entry guard would hold the bucket
        // lock across the `.await` (#589 class).
        let mvcc = mvcc_map.read_sync(&token, |_, mvcc| std::sync::Arc::clone(mvcc));
        if let Some(mvcc) = mvcc {
            mvcc.release_locks(tx.tx_id.0, &keys).await;
        }
    }
}

/// Release the Level-3 pessimistic record locks a parked interactive
/// transaction still holds against its repository.
///
/// Operator-cancellation entry point (R4b step 3): a cancelled interactive
/// transaction leaves the server registry without running the commit
/// pipeline, and [`shamir_tx::TxContext`] has no `Drop` that frees those
/// locks — they live in the per-table `MvccStore` and are released only by
/// the explicit call sites of `release_pessimistic_locks`. Same
/// behaviour as that function; a no-op for a transaction holding none.
pub async fn release_interactive_tx_locks(tx: &TxContext, repo: &RepoCore) {
    release_pessimistic_locks(tx, repo).await;
}

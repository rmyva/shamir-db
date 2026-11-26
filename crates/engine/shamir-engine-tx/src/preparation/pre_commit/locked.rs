use std::sync::Arc;

use shamir_tx::{CellReservationGuard, IsolationLevel, RepoTxGate, RepoWalManager, TxContext};
use shamir_wal::WalEntryV2;

use super::claims::{claim_write_set, PreCommit, REPO_INTERNER_SCOPE};
use crate::commit::core::RepoCore;
use crate::commit::{maybe_crash, wal_ops_from_tx, TxError};

/// Outcome of [`pre_commit_locked_validate`]: the assigned commit version,
/// built WAL entry, and uwl_guards — ready for WAL begin (Phase 4).
pub(in crate::tx) struct ValidatedPreCommit {
    pub(in crate::tx) commit_version: u64,
    pub(in crate::tx) uwl_guards: Vec<tokio::sync::OwnedMutexGuard<()>>,
    /// F-48b (#867): kept-alive writer-drain guards (one per fast-path table
    /// in `tx.write_set` whose `needs_write_barrier()` read `false` in Phase
    /// 2.5). Threaded alongside `uwl_guards` through Phase 5c, dropped inside
    /// [`materialize`](crate::materialize::materialize) after the data/index
    /// writes have landed.
    pub(in crate::tx) drain_guards: Vec<crate::table::writer_drain_barrier::WriterDrainGuard>,
    /// RAII owner of the version's terminal-mark obligation. The caller
    /// (`commit_tx_lockfree`) either calls `materialize` (which consumes it
    /// via `guard.commit()` → Materialized) on the success path, or drops it
    /// (→ Aborted) on WAL-begin failure. This makes the assign→mark window
    /// statically leak-proof.
    pub(in crate::tx) version_guard: shamir_tx::VersionGuard,
    /// SSI fix S2 — RAII owners of this committer's pre-WAL cell-reservations
    /// (one per touched table). The caller drops these (→ release) if WAL begin
    /// fails, or holds them through `materialize` and `disarm`s them once the
    /// publisher has finalized every claim. Empty off Serializable.
    pub(in crate::tx) cell_guards: Vec<CellReservationGuard>,
    /// Op #2 Stage 2: the WAL entry wrapped in `Arc` for drainer window offer.
    /// The caller serializes it into the WAL via `begin_grouped(&arc, ..)`
    /// and offers it to the drainer — both read from this Arc, no clone.
    pub(in crate::tx) wal_entry_arc: Arc<WalEntryV2>,
}

/// Locked validation phase: Phases 2 + 2-bis + C6 + 3 (assign) + WAL entry build.
///
/// Does NOT write the WAL entry (no fsync). The caller is responsible for
/// calling `wal.begin_grouped(entry, ..)` or batching via
/// `wal.begin_grouped_many`. This split
/// enables group-commit fsync amortization.
///
/// Phase 3 (assign_next_version) is DEFERRED until after validation and
/// the empty-tx check (P0c): SSI/phantom/empty-tx aborts return before any
/// version is allocated, so no version slot is wasted on aborted txs.
///
/// Returns `Some(ValidatedPreCommit)` when the tx has durable work,
/// `None` for C6 empty-tx fast-path, or `Err` on validation failure.
pub(in crate::tx) async fn pre_commit_locked_validate(
    tx: &mut TxContext,
    repo: &RepoCore,
    gate: &RepoTxGate,
    uwl_guards: Vec<tokio::sync::OwnedMutexGuard<()>>,
    drain_guards: Vec<crate::table::writer_drain_barrier::WriterDrainGuard>,
) -> Result<Option<ValidatedPreCommit>, TxError> {
    // Phase 2 (SSI only): read-set validation.
    if tx.isolation == IsolationLevel::Serializable {
        let validation = match tx.version_provider.as_ref() {
            Some(provider) => {
                let provider = std::sync::Arc::clone(provider);
                tx.validate_read_set(move |t, k| provider.version_of(t, k))
            }
            None => tx.validate_read_set(|_t, _k| Some(0u64)),
        };
        if let Err((_table_id, key)) = validation {
            repo.tx_metrics().on_tx_aborted_ssi();
            return Err(TxError::SsiConflict { key });
        }
    }

    // Phase 2-bis (SSI only, Phase C): predicate read-set validation.
    //
    // Inverted single-pass scan: walk the commit window ONCE and test ALL
    // predicate deps against each record, short-circuiting on the FIRST
    // conflict (O(W) window walks, not O(P×W); one shared EBR guard, not
    // P). The conflict set is identical to the per-dep loop — a conflict
    // exists iff some dep conflicts with some record, which is
    // order-independent.
    //
    // F-40b (RI barrier): widened to ALSO fire when `ri_barrier_tokens` is
    // non-empty (recorded by FK reverse-check scans regardless of
    // isolation), so an EXPLICIT `Snapshot`-isolation parent delete/update
    // — which never populates `predicate_set` — still gets a commit-time
    // re-check. The barrier tokens are appended as `TableScan { table_token }`
    // deps, reusing the SAME `predicate_conflicts_batch` machinery verbatim.
    let has_serializable_preds =
        tx.isolation == IsolationLevel::Serializable && !tx.predicate_set.is_empty();
    if has_serializable_preds || !tx.ri_barrier_tokens_is_empty() {
        let mut deps = if has_serializable_preds {
            tx.predicate_set.snapshot_deps()
        } else {
            Vec::new()
        };
        tx.append_ri_barrier_deps(&mut deps);
        if let Some(idx) = gate.predicate_conflicts_batch(&deps, tx.snapshot_version) {
            let dep = format!("{:?}", deps[idx]);
            repo.tx_metrics().on_tx_aborted_phantom();
            return Err(TxError::PhantomConflict { dep });
        }
    }

    // Phase CAS (FG-7): expected_version validation, independent of
    // isolation level. Runs for EVERY tx (Snapshot and Serializable alike)
    // whenever cas_set is non-empty — zero cost otherwise (empty-map check).
    // Placed after Phase 2/2-bis so a Serializable tx that also used CAS
    // still surfaces the pre-existing SsiConflict/PhantomConflict class
    // first (regression test `concurrent_cas_exactly_one_wins` depends on
    // this ordering); Phase CAS is the backstop for Snapshot/no-SSI paths
    // where Phase 2/2-bis are no-ops.
    if !tx.cas_set.is_empty() {
        // Defensive: every `begin_tx` call site now attaches a version
        // provider unconditionally (FG-7 step 3), so `None` here should be
        // unreachable in production. Fail safe (treat as a conflict, same
        // as `validate_read_set`'s `None => conflict` convention) rather
        // than silently skipping a CAS check a caller depends on.
        debug_assert!(
            tx.version_provider.is_some(),
            "cas_set non-empty but no version_provider attached"
        );
        let provider = tx.version_provider.as_ref();
        // Mirror `validate_read_set`'s iteration idiom: `iter_sync` is a
        // synchronous visitor that cannot early-return, so capture the
        // first conflict and report it after the scan.
        let mut conflict: Option<(bytes::Bytes, u64, u64)> = None;
        tx.cas_set.iter_sync(|(table_id, key), expected| {
            if conflict.is_some() {
                return false;
            }
            match provider.and_then(|p| p.version_of(*table_id, key)) {
                None => conflict = Some((key.clone(), *expected, 0)),
                Some(found) if found != *expected => {
                    conflict = Some((key.clone(), *expected, found));
                }
                Some(_) => {}
            }
            true
        });
        if let Some((key, expected, found)) = conflict {
            repo.tx_metrics().on_tx_aborted_ssi();
            return Err(TxError::CasConflict {
                key,
                expected,
                found,
            });
        }
    }

    // C6: empty-tx fast-path. No version has been allocated yet (P0c),
    // so nothing to mark — just return.
    if tx.is_empty() {
        return Ok(None);
    }

    // SSI fix S2 — CLAIM the write-set (Serializable only), AFTER read-validate
    // and BEFORE version assign + Phase 4 WAL. A loser aborts here with
    // `SsiConflict`, never touching the WAL (I-PreWAL). `claim_write_set` builds
    // the guards holding the won keys; on this `?`-return their drop releases
    // any partial claim.
    let cell_guards = claim_write_set(tx, repo).await?;

    // Crash seam (test-only).
    maybe_crash("pre_commit", repo).await;

    // Phase 3 (P0c): assign new version AFTER validation, wrapped in a
    // RAII VersionGuard. Deferred to this point so SSI/phantom/empty-tx
    // aborts never allocate a version slot. Pure atomic fetch_add —
    // lock-free, safe without commit_mutex.
    let version_guard = gate.assign_next_version_guarded();
    let commit_version = version_guard.version();

    // Build WAL entry (Phase 4 prep) — does NOT persist.
    let wal_ops = wal_ops_from_tx(tx).await;
    // Stage I: the interner is per-REPO. `interner_deltas` is a single flat
    // `Vec<(name, id)>` (no per-table key), so we emit every entry under the
    // REPO scope marker (constant 0). The WAL wire-shape
    // `Vec<(u64, String, u64)>` is UNCHANGED — only the meaning of the first
    // `u64` shifts from `table_token` to a repo-scope constant. Recovery
    // resolves the single repo interner directly (keystone).
    let interner_delta: Vec<(u64, String, u64)> = tx
        .interner_deltas
        .iter()
        .map(|(name, id)| (REPO_INTERNER_SCOPE, name.clone(), *id))
        .collect();
    let mut wal_entry = shamir_wal::WalEntryV2::new(tx.tx_id.0, tx.repo_id, wal_ops)
        .with_commit_version(commit_version);
    wal_entry.interner_delta = interner_delta;
    // S5c-X I1: stamp the journal-event header BEFORE the WAL append so the
    // journal event stays a pure function of the entry
    // (`shamir_tx::event_from_wal_entry`). One code path for the live commit
    // and (PR4) recovery.
    wal_entry.event = Some(shamir_tx::event_header_from_tx(tx, repo.name()));
    // Op #2 Stage 2: wrap in Arc for drainer window offer. `begin_grouped`
    // borrows, so no clone is needed for WAL persistence — both the WAL
    // serialize and the drainer offer read from this Arc.
    let wal_entry_arc = Arc::new(wal_entry);

    Ok(Some(ValidatedPreCommit {
        commit_version,
        uwl_guards,
        drain_guards,
        version_guard,
        cell_guards,
        wal_entry_arc,
    }))
}

/// Locked phase of the commit pipeline: runs UNDER `commit_lock`.
///
/// Performs:
/// - Phase 2: SSI read-set validation (must be atomic with Phase 6
///   record_commit_writes — both under lock).
/// - Phase 2-bis: phantom predicate validation.
/// - C6: empty-tx fast-path check.
/// - Phase 3: assign_next_version (P0c: deferred AFTER validation).
/// - Phase 4: WAL begin (the commit point).
///
/// Returns `Some(PreCommit)` on successful Phase 4, `None` for the C6
/// empty-tx fast-path, or `Err` on SSI/phantom/unique conflict or
/// storage failure.
pub(in crate::tx) async fn pre_commit_locked(
    tx: &mut TxContext,
    repo: &RepoCore,
    gate: &RepoTxGate,
    wal: &RepoWalManager,
    uwl_guards: Vec<tokio::sync::OwnedMutexGuard<()>>,
    drain_guards: Vec<crate::table::writer_drain_barrier::WriterDrainGuard>,
) -> Result<Option<PreCommit>, TxError> {
    // Phase 2 (SSI only): read-set validation.
    //
    // For each (table_id, key) the tx read at version_seen, ensure the
    // current committed version has not moved past it.
    //
    // Uses tx.version_provider if set; otherwise stub `|_, _| Some(0)`
    // (Snapshot-equivalent behaviour).
    if tx.isolation == IsolationLevel::Serializable {
        let validation = match tx.version_provider.as_ref() {
            Some(provider) => {
                let provider = std::sync::Arc::clone(provider);
                tx.validate_read_set(move |t, k| provider.version_of(t, k))
            }
            None => tx.validate_read_set(|_t, _k| Some(0u64)),
        };
        if let Err((_table_id, key)) = validation {
            repo.tx_metrics().on_tx_aborted_ssi();
            return Err(TxError::SsiConflict { key });
        }
    }

    // Phase 2-bis (SSI only, Phase C): predicate read-set validation.
    //
    // Inverted single-pass scan — see the matching block in
    // `pre_commit_locked_validate` for the full rationale. Both call
    // sites share `RepoTxGate::predicate_conflicts_batch`.
    //
    // F-40b (RI barrier): widened to ALSO fire when `ri_barrier_tokens` is
    // non-empty (recorded by FK reverse-check scans regardless of
    // isolation), so an EXPLICIT `Snapshot`-isolation parent delete/update
    // routed through this AsyncIndex-visibility path — which never
    // populates `predicate_set` — still gets a commit-time re-check. This
    // path already always runs under `commit_lock` (taken unconditionally
    // by its caller), so no additional lock-widening is needed here
    // (unlike the lock-free path's commit-lock acquisition). The barrier
    // tokens are appended as `TableScan { table_token }` deps, reusing
    // the SAME `predicate_conflicts_batch` machinery verbatim — identical
    // to the `pre_commit_locked_validate` widening above.
    let has_serializable_preds =
        tx.isolation == IsolationLevel::Serializable && !tx.predicate_set.is_empty();
    if has_serializable_preds || !tx.ri_barrier_tokens_is_empty() {
        let mut deps = if has_serializable_preds {
            tx.predicate_set.snapshot_deps()
        } else {
            Vec::new()
        };
        tx.append_ri_barrier_deps(&mut deps);
        if let Some(idx) = gate.predicate_conflicts_batch(&deps, tx.snapshot_version) {
            let dep = format!("{:?}", deps[idx]);
            repo.tx_metrics().on_tx_aborted_phantom();
            return Err(TxError::PhantomConflict { dep });
        }
    }

    // Phase CAS (FG-7): expected_version validation, independent of
    // isolation level. See the matching block in `pre_commit_locked_validate`
    // for the full rationale — both call sites mirror each other exactly.
    // This path (`pre_commit_locked`, the AsyncIndex commit visibility
    // route) already always runs under `commit_lock` (taken unconditionally
    // by its caller), so the validate→publish atomicity CRIT-4 provides for
    // `commit_tx_lockfree` is a non-issue here — but the CAS check itself
    // still must run so an AsyncIndex-visibility tx gets the same
    // "exactly one wins" guarantee as every other commit path.
    if !tx.cas_set.is_empty() {
        debug_assert!(
            tx.version_provider.is_some(),
            "cas_set non-empty but no version_provider attached"
        );
        let provider = tx.version_provider.as_ref();
        let mut conflict: Option<(bytes::Bytes, u64, u64)> = None;
        tx.cas_set.iter_sync(|(table_id, key), expected| {
            if conflict.is_some() {
                return false;
            }
            match provider.and_then(|p| p.version_of(*table_id, key)) {
                None => conflict = Some((key.clone(), *expected, 0)),
                Some(found) if found != *expected => {
                    conflict = Some((key.clone(), *expected, found));
                }
                Some(_) => {}
            }
            true
        });
        if let Some((key, expected, found)) = conflict {
            repo.tx_metrics().on_tx_aborted_ssi();
            return Err(TxError::CasConflict {
                key,
                expected,
                found,
            });
        }
    }

    // === C6: empty-tx fast-path ===
    //
    // No version has been allocated yet (P0c), so nothing to mark — just
    // return.
    //
    // ORDERING IS LOAD-BEARING: this sits AFTER the Phase 2 SSI block above.
    // A read-only Serializable tx still records reads, and its read_set must
    // still be validated against current committed versions — a read-only
    // tx that observed stale data MUST abort (returned as `Err` above), not
    // silently fast-path to success.
    if tx.is_empty() {
        return Ok(None);
    }

    // SSI fix S2 — CLAIM the write-set (Serializable only), AFTER read-validate
    // and BEFORE version assign + Phase 4 WAL. A loser aborts here with
    // `SsiConflict`, never touching the WAL (I-PreWAL). On this `?`-return the
    // partial guards drop → release.
    let cell_guards = claim_write_set(tx, repo).await?;

    // Crash seam (test-only): a HARD crash here is BEFORE the commit
    // point — staging is dropped, no WAL entry exists, locks release by
    // RAII. Recovery must find nothing → clean abort.
    maybe_crash("pre_commit", repo).await;

    // Phase 3 (P0c): assign new version AFTER validation, wrapped in a
    // RAII VersionGuard. Deferred to this point so SSI/phantom/empty-tx
    // aborts never allocate a version slot. Pure atomic fetch_add —
    // lock-free, safe without commit_mutex.
    let mut version_guard = gate.assign_next_version_guarded();
    let commit_version = version_guard.version();

    // Phase 4: write WAL entry — THE COMMIT POINT.
    //
    // A successful `wal.begin` makes the entry durable (lands in the OS
    // page cache at minimum — level 2; level 3 only after a later
    // `sync`); from here the tx is committed and `materialize` may not
    // abort. A *failed* `wal.begin` returns Err and is treated as a
    // pre-commit failure: the segment is poisoned and the leader rotates
    // to a fresh segment. In the COMMON case nothing durable remains —
    // `WalSegment::append_batch` rolls the file back to the last good
    // frame boundary on a `write_all` failure, so no torn frame survives
    // in the file. The rare exception (audit durability §1.6, NOT yet
    // fixed in this codebase) is when the rollback `set_len` ITSELF
    // fails: a partial frame may survive in the poisoned file. That
    // frame is discarded by `repair_torn_tail` on the next open (and by
    // replay's CRC check even if not repaired), so it cannot corrupt
    // recovery — but until §1.6 is fixed, the simple "nothing durable"
    // claim does not hold in that narrow window.
    //
    // HIGH-5: stamp the assigned `commit_version` onto the entry BEFORE
    // persisting it. Recovery sorts inflight entries by `commit_version`
    // so multi-tx replay matches the original commit pipeline's order;
    // `txn_id` (the `WalActiveKey` byte order) is not a safe proxy because
    // tx allocation and commit ordering are independent.
    let wal_ops = wal_ops_from_tx(tx).await;
    // Stage I: flatten the per-repo interner delta into the WAL entry. See
    // the matching note in `pre_commit_prelock`: the first `u64` is a
    // repo-scope constant (0), NOT a table token. Wire-shape unchanged.
    let interner_delta: Vec<(u64, String, u64)> = tx
        .interner_deltas
        .iter()
        .map(|(name, id)| (REPO_INTERNER_SCOPE, name.clone(), *id))
        .collect();
    let mut entry =
        WalEntryV2::new(tx.tx_id.0, tx.repo_id, wal_ops).with_commit_version(commit_version);
    entry.interner_delta = interner_delta;
    // S5c-X I1: same header stamping as `pre_commit_locked_validate` - the
    // AsyncIndex visibility path builds its entry through the same
    // event-from-entry contract.
    entry.event = Some(shamir_tx::event_header_from_tx(tx, repo.name()));
    // Op #2 Stage 2: wrap the entry in Arc BEFORE persisting so the same
    // logical entry is shared between the WAL and the drainer window.
    // `begin_grouped` borrows the entry, so we serialize from the Arc borrow
    // — no clone on the commit hot path.
    let entry_arc = Arc::new(entry);
    // Record before queueing: an unwritten abort may cause spurious SSI
    // conflicts, but a written orphan must never expose a missed conflict.
    gate.record_commit_writes(shamir_tx::build_footprint_from_tx(tx, commit_version));
    let (ticket, completion) = wal.begin_grouped_ticketed(
        &entry_arc,
        crate::commit::commit_wal_durability(&repo.durability_handle, repo.name()),
    )?;
    version_guard.attach_metrics(repo.tx_metrics().clone());
    version_guard.wal_queued(ticket, entry_arc.clone());
    let (wc_result, proven_not_written) =
        crate::commit::wal_completion(gate, commit_version, completion.await);
    if proven_not_written {
        repo.settle_unwritten_version(commit_version).await;
    }
    let durability_failed = wc_result?;
    // SSI fix S2: WAL begin succeeded — the tx is COMMITTED. The claims stay
    // armed and are handed to the caller via `PreCommit`; the caller `disarm`s
    // them once the publisher has finalized every claim (`finalize_reservation`
    // clears `reserved_by`).

    // Crash seam (test-only): a HARD crash here is AT the commit point —
    // the WAL entry is durable but no projection (5a..6.5) ran and Phase
    // 7 cleanup never happens. Recovery must find the inflight entry and
    // materialize the whole tx (data + index). All-or-nothing.
    maybe_crash("phase4", repo).await;

    Ok(Some(PreCommit {
        commit_version,
        uwl_guards,
        drain_guards,
        version_guard,
        cell_guards,
        wal_entry_arc: entry_arc,
        durability_failed,
    }))
}

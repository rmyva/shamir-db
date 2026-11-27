use shamir_collections::{TFxMap, TFxSet};
use shamir_index::model::identity::identity_bytes::unique_posting_owner;
use shamir_storage::error::DbError;
use shamir_tx::{CancelHandle, IndexFamily, IndexWriteOp, TxContext, WaitState};
use shamir_types::core::interner::InternerKey;
use shamir_types::types::record_id::RecordId;
use shamir_types::types::value::InnerValue;
use std::sync::Arc;

use super::base_index::rederive_base_index_ops_post_stage;
use super::claims::{fire_post_prelock_pre_materialize_test_hook, PreLockResult};
use super::index2::rederive_index2_ops_post_stage;
use super::stale_value::rederive_stale_value_ops_post_stage;
use crate::commit::core::RepoCore;
use crate::commit::TxError;

/// Pre-lock phase of the commit pipeline: runs OUTSIDE `commit_lock`,
/// concurrent with other committers.
///
/// Performs:
/// - Phase 1: interner overlay merge + remap (CAS-safe on DashMap).
/// - Phase 2.5: acquire per-table `unique_write_lock` guards in sorted
///   token order, for every table with a unique guard OR (#538 Part A) an
///   in-flight `create_index_v2` barrier (`needs_write_barrier()`). These
///   serialise against non-tx unique/barriered writers and against other
///   committers touching the same table.
/// - Phase 2.6: authoritative unique re-validation under the uwl_guards.
///   Decisive because the uwl_guard excludes both non-tx writers and other
///   committers for the same table (they block on the same guard). The guard
///   is held continuously through Phase 5c (inside `materialize`), so no
///   writer can interleave between this check and the posting write.
///
/// Lock ordering / ABBA-freedom (§B9, updated for Stage B):
///   - uwl_guards are acquired BEFORE `commit_lock`. A non-tx writer holds
///     at most ONE uwl and NEVER waits on `commit_lock`. Two committers
///     touching overlapping unique tables serialize on the uwl_guards (sorted
///     token order — no ABBA between committers). The `commit_lock` is then
///     acquired by whichever committer holds its uwl_guards; since
///     `commit_lock` is a single global mutex, no ordering cycle is possible:
///     all committers acquire uwl_guards first, then `commit_lock`.
///     Therefore no ABBA cycle is possible.
///
/// cancel-safe: YES for Phase 1 (CAS-based, no durable mutation). Phase 2.5
/// acquires tokio mutexes (cancel-safe per docs — drop releases the wait).
/// Phase 2.6 reads info_store (cancel-safe — read-only).
pub(in crate::tx) async fn pre_commit_prelock(
    tx: &mut TxContext,
    repo: &RepoCore,
    cancel: Option<&CancelHandle>,
) -> Result<PreLockResult, TxError> {
    // F-68 (#895) cluster D diagnostic instrumentation, task #124 — entry
    // timestamp. F-70 (#897, P0) confirmed and fixed a genuine lock-order
    // inversion between this fn's shape (drain-guard-then-lock, see the
    // Phase 2.5 loop below — UNCHANGED by F-70) and every DDL path, which
    // used to be lock-then-drain (e.g. `create_index_v2` in
    // `table_manager_index_mgmt.rs`) and is now drain-then-lock via
    // `TableManager::begin_write_barrier` — see `writer_drain_barrier`'s
    // "F-70 — THE canonical lock-order hierarchy" doc section for the full
    // derivation and correctness argument. This logging is kept as a
    // regression tripwire: if a hang ever reproduces again, this pairs with
    // the "pre_commit_prelock: exit" log below (or its absence) to show
    // whether the stall is INSIDE this fn.
    let tx_id_for_log = tx.tx_id.0;
    let prelock_started = std::time::Instant::now();
    log::debug!("pre_commit_prelock: enter tx_id={tx_id_for_log}");
    // Phase 1: interner overlay merge → id remap.
    //
    // Stage I: the interner is per-REPO (one id-namespace shared across
    // every table), so we merge the tx overlay ONCE into the repo interner
    // and obtain ONE remap. The remap is then applied to every touched
    // table's staging bytes — overlay ids are tx-scoped, and the repo
    // interner is the single base they all resolve against, so the same
    // `{overlay_id → base_id}` mapping is correct for every table.
    //
    // A8 fix: after the (optional) overlay merge + remap, EVERY committer
    // with staged bytes additionally scans its staged bytes for any
    // `InternerKey` id referenced above `persisted_high_water()` that is
    // not already in `tx.interner_deltas`, and records `(name, id)` for
    // each. This closes the hole where a later committer's records
    // reference an id some OTHER (possibly aborted-before-WAL) tx created
    // in base — without this pass, no surviving WAL delta would mention
    // that id, and a crash before the next checkpoint would leave the
    // later committer's records undecodable. See
    // `docs/dev-artifacts/audits/2026-07-06-concurrency-engine.md` A8.
    let has_staged_writes = !tx.write_set.is_empty();
    if !tx.interner_overlay.is_empty() || has_staged_writes {
        let repo_interner = repo.repo_interner().await?;
        let base_interner = repo_interner.get().await?;
        if !tx.interner_overlay.is_empty() {
            let shamir_tx::OverlayCommitResult { remap, delta } =
                shamir_tx::commit_interner_overlay(base_interner, &tx.interner_overlay).await?;
            if !delta.is_empty() {
                tx.interner_deltas.extend(delta);
            }
            if !remap.is_empty() {
                let table_ids: Vec<u64> = tx.write_set.keys().cloned().collect();
                for table_id in &table_ids {
                    if let Some(staging) = tx.write_set.get_mut(table_id) {
                        staging
                            .rewrite_set_bytes(|b| {
                                shamir_tx::remap_inner_value_bytes(b.clone(), &remap)
                                    .map_err(|e| format!("remap: {e}"))
                            })
                            .await
                            .map_err(DbError::Codec)?;
                    }
                }
            }
        }
        // A5: interner persist removed from the commit critical path. The WAL
        // entry carries the interner delta (`interner_deltas`), so crash
        // recovery replays new (name, id) mappings via `touch_with_id`. A
        // background checkpoint (every INTERNER_CHECKPOINT_INTERVAL commits)
        // flushes the delta to the durable chunk store, advancing the
        // persisted high-water mark so Phase 7 WAL truncation can proceed.
        // Graceful shutdown flushes the repo interner once.

        // A8: check every interner id referenced by this tx's staged record
        // bodies for coverage — any id ABOVE `persisted_high_water()` that is
        // not already covered by `tx.interner_deltas`. Each such id was
        // created in base by some tx (possibly this one, possibly another
        // that aborted before WAL) and is NOT yet durably recorded in the
        // chunk store — so THIS committer's WAL must carry `(name, id)`
        // for it or a crash before the next checkpoint makes this tx's own
        // records undecodable. `touch_with_id` (recovery replay) is
        // idempotent, so redundant inclusion across multiple committers'
        // deltas is harmless.
        //
        // #1205: `tx.referenced_interner_ids` is captured AT STAGE TIME by
        // every `TableManager` insert/update call site (see its doc comment
        // in `tx_context.rs`), so the common case below no longer re-decodes
        // every staged value's msgpack bytes — that used to cost 2-3 full
        // decode passes per commit on a large batch. A captured id that was
        // still overlay-scoped at stage time (a brand-new field name,
        // remapped to a real base id by Phase 1 above) is harmless here even
        // though it no longer matches the final base id: overlay ids sit far
        // above any real `hwm`, so `get_str` below returns `None` for it and
        // the entry is skipped — the corresponding real base id is
        // unconditionally already in `tx.interner_deltas` via
        // `commit_interner_overlay`'s own delta output, so `existing`
        // already covers it regardless.
        //
        // Fallback scans only currently staged sets whose ids were not captured.
        // Raw and captured writes can coexist in the same table.
        let hwm = repo_interner.persisted_high_water() as u64;
        // Cheap dedup: build a set of ids already covered by this tx's delta.
        let mut existing: shamir_collections::TFxSet<u64> = shamir_collections::new_fx_set();
        existing.extend(tx.interner_deltas.iter().map(|(_, id)| *id));
        let mut fallback_ids: shamir_collections::TFxMap<u64, ()> =
            shamir_collections::new_fx_map();
        for staging in tx.write_set.values() {
            for bytes in staging.iter_uncaptured_set_bytes() {
                if let Ok(value) = InnerValue::from_bytes(bytes) {
                    shamir_tx::collect_referenced_ids(&value, &mut fallback_ids);
                }
                // A decode failure here is a pre-existing corruption
                // (staged bytes are always valid msgpack by construction);
                // skip rather than abort — the remap pass above would
                // already have surfaced a codec error for genuinely
                // malformed bytes.
            }
        }
        // Collect into an owned `Vec` first — `tx.interner_deltas` is
        // mutated inside the loop below, which would otherwise conflict
        // with a borrow of `tx.referenced_interner_ids` held across it.
        let all_ids: Vec<u64> = tx
            .referenced_interner_ids
            .keys()
            .copied()
            .chain(fallback_ids.into_keys())
            .collect();
        for id in all_ids {
            if id > hwm && !existing.contains(&id) {
                if let Some(name) = base_interner.get_str(&InternerKey::new(id)) {
                    tx.interner_deltas.push((name.to_string(), id));
                    existing.insert(id);
                }
                // If `get_str` returns None the id is not in the base
                // interner's reverse map — either a stale overlay id (see
                // above) or, for a genuine base id, a defensive skip: an
                // unresolvable id is a separate (already-lost) problem, not
                // something this pass can fix.
            }
        }
    }

    // Phase 2.5 (HIGH-A, extended by #538 Part A): acquire each barriered
    // table's `unique_write_lock` and HOLD it across Phase 2.6 → 5c.
    //
    // The problem this closes: non-tx `insert` / `set` / `delete` take a
    // DIFFERENT mutex — the per-table `unique_write_lock` — and never touch
    // `commit_lock`. So without this step a non-tx unique write could claim
    // or overwrite the same unique posting in the window between this tx's
    // Phase 2.6 re-check and its Phase 5c posting write, producing a
    // duplicate unique value + corrupted index. Acquiring the same per-table
    // lock the non-tx path uses makes the tx's "check unique key free →
    // write posting" atomic against every non-tx unique writer to that table.
    //
    // Two concurrent committers touching the same unique-constrained table
    // serialize on the same uwl_guard (sorted token order prevents ABBA).
    // The loser waits here until the winner's Phase 5c completes and drops
    // the guard — at that point the loser's Phase 2.6 re-check sees the
    // winner's posting and correctly detects the conflict.
    //
    // #538 Part A: the token set was originally built ONLY from
    // `tx.unique_guards` (tables with a base_index UNIQUE index) — an
    // index2-only table (fts/functional/vector, no base_index unique index)
    // contributed nothing, so this tx's Phase 5a/5c could freely interleave
    // with a concurrent `create_index_v2`'s backfill on that table (the same
    // lost-write window #534 closed for the non-tx path, left open here).
    // Fixed by additionally scanning every table this tx actually staged a
    // write to (`tx.write_set` keys) and including its token whenever
    // `TableManager::needs_write_barrier()` is `true` — which is exactly the
    // predicate the non-tx writer methods gate on, so a table with an
    // in-flight `create_index_v2` (index2_create_barrier up) now serializes
    // its tx-commit materialization against that create too, via the SAME
    // `unique_write_lock` `create_index_v2` holds for its full backfill
    // duration. This does NOT close #538's Part B: the index2 ops-PLAN
    // (`tx.index_write_set`) was already captured at STAGE time against an
    // `all_backends()` snapshot, well before this prelock runs — see
    // `TableManager::backfill_index2_backend`'s doc comment for the full
    // accounting.
    //
    // Sidecar mutations are real table touches too: postings, counter deltas,
    // staged vectors, and staged vector deletes are materialized independently
    // of `write_set`. Include their tokens so freeze/drain gates cover even
    // sidecar-only transactions (including future producers that violate the
    // usual staging invariant).
    //
    // `table_by_token_if_live` (NOT `table_by_token`) resolves the table
    // WITHOUT forcing lazy instantiation: a table that was `add_table`d but
    // never actually accessed via `get_table` stays dormant. This matters —
    // instantiating a `TableManager` as a side effect of merely checking a
    // barrier flag would register it in `per_table_mvcc`, changing
    // `apply_data_batch`'s routing for that table from the direct-store
    // fallback to MVCC-routed, a behavior change this check must not cause.
    // It is also correct: `needs_write_barrier()` can only be `true` on an
    // instance that was actually created (no code path can flip
    // `index2_create_barrier` or register a base_index unique index without
    // first holding a live `TableManager`), so a dormant table is
    // equivalent to "no barrier" for this purpose.
    //
    // We use `lock_owned()` so the guards can be collected into a `Vec`
    // without borrow-lifetime entanglement (each `OwnedMutexGuard` owns its
    // `Arc<Mutex<()>>`). Tables needing neither a unique guard nor the
    // index2-create barrier are untouched — the lock-free fast path is
    // preserved for them.
    //
    // F-48b (#867): the lock-free fast path has the SAME check-then-act race
    // F-48 closed for the non-tx writer methods — `needs_write_barrier()` is
    // read ONCE, and if `false` the tx proceeds through Phase 5c materialize
    // with no further check, so a DDL raising the barrier AFTER this read (and
    // calling `drain_writers()`) sees zero in-flight writers, incorrectly.
    // Fixed by entering the writer-drain set (`enter_writer_drain`) BEFORE
    // reading the flag, for every table returned by `touched_table_tokens`
    // (write_set and all mutation sidecars) — the cross-atomic happens-before
    // edge to the DDL's drain load is carried by the single SeqCst total order
    // over the `active` counter + `flag` (F-56; see `writer_drain_barrier`'s
    // memory-model doc — NOT a flag coherence chain, which Release/Acquire
    // cannot span across two independent atomics). If the flag is `true`
    // (slow path: this table gets a uwl_guard), drop the drain guard BEFORE
    // the lock acquisition — the lock alone provides exclusion, and staying
    // in the drain set while blocking on the lock (held by a DDL that is
    // itself calling `drain_writers()`) would deadlock. If `false`, keep the
    // drain guard alive until Phase 5c materialize lands — the kept-alive
    // guards flow through `PreLockResult` → `PreCommit`/`ValidatedPreCommit`
    // → `materialize`/`materialize_async_tail`, dropped alongside
    // `uwl_guards` after the data/index writes have completed.
    let mut unique_tokens: Vec<u64> = tx.unique_guards.iter().map(|g| g.table_token).collect();
    let mut drain_guards: Vec<crate::table::writer_drain_barrier::WriterDrainGuard> = Vec::new();
    for table_id in touched_table_tokens(tx) {
        if let Some(tbl) = repo.table_by_token_if_live(table_id).await {
            // F-48b/F-56: bump the drain counter BEFORE reading the flag so the
            // cross-atomic SeqCst happens-before edge reaches the DDL's drain
            // load. Mirrors `table_manager_crud.rs`'s writer methods.
            let drain_guard = tbl.enter_writer_drain();
            if tbl.needs_write_barrier() {
                // Slow path: this table will get a `unique_write_lock` guard
                // (added to `unique_tokens` below). Drop the drain guard
                // BEFORE the lock acquisition — see the comment block above
                // for the deadlock rationale. The lock serializes this table;
                // the drain counter must not.
                drop(drain_guard);
                unique_tokens.push(table_id);
            } else {
                // Fast path: keep the drain guard alive until Phase 5c
                // materialize. A DDL that raises the barrier AFTER this read
                // and calls `drain_writers()` genuinely waits for this tx.
                drain_guards.push(drain_guard);
            }
        }
    }
    // Sorted + deduped BEFORE any lock is taken — this is the ABBA-freedom
    // invariant the module doc above documents: two committers touching
    // overlapping barriered tables always acquire the guards in the same
    // token order, so no ordering cycle is possible regardless of which
    // source (unique_guards vs. write_set-barrier) contributed the token.
    unique_tokens.sort_unstable();
    unique_tokens.dedup();
    let mut uwl_guards: Vec<tokio::sync::OwnedMutexGuard<()>> =
        Vec::with_capacity(unique_tokens.len());
    for token in &unique_tokens {
        if let Some(tbl) = repo.table_by_token(*token).await? {
            // F-68 (#895) cluster D / task #124 — this committer is still
            // holding this table's `drain_guards` (Phase 2.5's fast-path
            // guards, kept alive above for whichever OTHER tables read
            // `needs_write_barrier() == false`) while acquiring THIS table's
            // `unique_write_lock` here. F-70 (#897, P0) confirmed this WAS
            // the other half of a genuine lock-order inversion: every DDL
            // path (`create_index_v2` et al.) used to take the OPPOSITE
            // order (lock, then drain) — if both sides were mid-sequence on
            // overlapping tables, this `.lock_owned()` and the DDL's
            // `drain_writers().await` could each wait on the other forever.
            // Fixed by reordering every DDL path to drain-then-lock
            // (`TableManager::begin_write_barrier`) — this fn's own order is
            // UNCHANGED (it was never the bug; see `writer_drain_barrier`'s
            // "F-70" doc section). Timestamped logging kept as a regression
            // tripwire: a stuck run would show exactly which table_token
            // this committer was blocked on acquiring, and for how long.
            let uwl_wait_started = std::time::Instant::now();
            log::debug!(
                "pre_commit_prelock: tx_id={tx_id_for_log} acquiring unique_write_lock \
                 table_token={token}"
            );
            let lock = tbl.unique_write_lock();
            let guard = if let Some(cancel) = cancel.or_else(|| tx.cancel_handle()) {
                let _park = cancel.park(WaitState::Lock);
                tokio::select! {
                    biased;
                    _ = cancel.wait_cancelled() => return Err(TxError::Cancelled),
                    guard = lock.lock_owned() => guard,
                }
            } else {
                lock.lock_owned().await
            };
            if tbl
                .index_manager_ref()
                .write_barrier_flags()
                .is_set(shamir_index::base_index::write_barrier_flags::MIGRATION_FREEZE)
            {
                return Err(TxError::MigrationFrozen {
                    table: tbl.name().to_string(),
                });
            }
            let uwl_wait_elapsed = uwl_wait_started.elapsed();
            if uwl_wait_elapsed >= std::time::Duration::from_secs(1) {
                log::warn!(
                    "pre_commit_prelock: tx_id={tx_id_for_log} unique_write_lock \
                     table_token={token} acquisition took {uwl_wait_elapsed:?} \
                     (>= 1s threshold) — possible lock-order contention, see task #897"
                );
            } else {
                log::debug!(
                    "pre_commit_prelock: tx_id={tx_id_for_log} acquired unique_write_lock \
                     table_token={token} in {uwl_wait_elapsed:?}"
                );
            }
            uwl_guards.push(guard);
        }
    }

    // M5: validate before materialization, after acquiring any writer barrier.
    // A missing token must reject rather than skip; pointer identity rejects a
    // stale staging store even when DROP + CREATE reused the same name/token.
    validate_staging_table_identities(tx, repo).await?;

    // P0-2 (#958): base_index (regular + unique) ops-plan re-derivation for
    // the `IndexManager` family. A tx that staged before a base_index index
    // was created would otherwise commit with zero ops for it (permanently
    // missing posting for regular; for unique, additionally unconstrained
    // duplicate). For unique defs specifically, the rederive records fresh
    // `UniqueGuard`s (via `tx.record_unique_guard`) so Phase 2.6's
    // commit-time re-validation — which runs immediately AFTER this step —
    // covers constraints that did not exist at stage time. Also handles 2c:
    // retracts staged ops for base_index indexes dropped between stage and
    // commit.
    //
    // #987 (P0 ordering fix): this call MUST run AFTER Phase 2.5's lock
    // acquisition and BEFORE Phase 2.6's `for g in &tx.unique_guards` loop.
    // `tx.unique_guards` is consumed ONLY by that loop — recording guards
    // after it iterates is a dead write, which was the exact #987 bug (a tx
    // staging a duplicate of a pre-existing committed row before CREATE
    // UNIQUE INDEX silently committed, and its SetPosting overwrote the
    // owner's posting at the same deterministic key — index corruption).
    // A table that gains its first unique index mid-tx is still locked here:
    // Phase 2.5's `needs_write_barrier()` scan over `tx.write_set` sees
    // `UNIQUE_INDEX_EXISTS` set the moment `CREATE UNIQUE INDEX` registers
    // the def, so that table's `unique_write_lock` IS acquired even though
    // `tx.unique_guards` (built from stage-time guards only) did not yet
    // know about the constraint. Moving this call earlier keeps that lock
    // held across both the guard recording and Phase 2.6's re-validation,
    // preserving the check-then-act atomicity against non-tx unique writers.
    //
    // The index2 rederive (`rederive_index2_ops_post_stage`, run later
    // below) is independent — separate generation counters, separate op
    // families — and is left in its original position.
    rederive_base_index_ops_post_stage(tx, repo).await?;

    // #1100: Re-derive posting ops for records whose value changed concurrently.
    // This closes the gap where DELETE/UPDATE planned from a stale snapshot
    // never removes the record's CURRENT posting.
    log::debug!(
        "rederive_stale_value_ops_post_stage: starting, base_index_stage_gens.len()={}",
        tx.base_index_stage_gens.len()
    );
    rederive_stale_value_ops_post_stage(tx, repo).await?;

    // Phase 2.6: authoritative unique re-validation under per-table
    // unique_write_lock (held since Phase 2.5).
    //
    // Stage-time `validate_unique_*` is optimistic — it reads pre-commit
    // state, so two concurrent txs claiming the same unique value both
    // pass it. The per-table `unique_write_lock`s (Phase 2.5) exclude
    // BOTH non-tx unique writers AND other committers touching the same
    // table, so re-checking the claimed keys here is decisive against ALL
    // writers: no other writer can interleave between this check and the
    // Phase 5c posting write (the uwl_guard is held continuously).
    //
    // #1039 / #1074 — Intra-tx unique dedup + cross-tx re-validation.
    //
    // #1039 added an intra-tx dedup pass over `tx.unique_guards`. But
    // `unique_guards` is a chronological list of point-in-time CLAIMS — DELETE
    // never pushes a guard, and an UPDATE that moves a record OFF a unique key
    // pushes no "release" marker for the old key either. So #1039's check
    // treated the FIRST owner seen for a key as authoritative and
    // false-rejected any later guard with a different owner, even when the
    // first owner had legitimately vacated that key earlier in the same tx
    // (via UPDATE-off or DELETE) — a legal pattern that committed fine before
    // #1039 (#1074 regression).
    //
    // #1074 fix: use `tx.index_write_set` — the authoritative ordered storage
    // mutation sequence — for the intra-tx conflict detection. `RemovePosting`
    // (emitted by `plan_record_updated_unique` / `plan_record_deleted_unique`
    // when a record moves off or is deleted from a unique key) IS the release
    // marker that `unique_guards` lacks. Walking the unique-family ops in
    // staging order and tracking live ownership per key gives the TRUE final
    // per-key state:
    //   - SetPosting on a key whose current live owner is a DIFFERENT record →
    //     genuine intra-tx collision (no release between them) → abort.
    //   - RemovePosting releases the key, so a LATER SetPosting from a
    //     different record is a legitimate release-then-reclaim, not a conflict.
    //
    // `tx.unique_guards` is still iterated for the durable-state (cross-tx)
    // check, because an update re-writing the SAME unique value produces NO
    // SetPosting (plan_record_updated_unique is a no-op when old_key == new_key)
    // but DOES push a guard — that key still needs committed-state validation.
    // For keys in the write-set walk, the FINAL owner from the walk is used
    // (may differ from the guard's owner if the key was released and reclaimed);
    //
    // **#1096 closes the "release-then-reclaim a DURABLE (prior-tx) unique
    // value within one tx" scenario** (found not fully covered by #1074 in
    // an @oh review, 2026-08-11): `tx1: INSERT A {email:"x"}; COMMIT` then
    // `tx2: DELETE A; INSERT B {email:"x"}`. Two call sites needed fixing,
    // one per copy of the durable-only assumption:
    //   - `table_manager_tx_ops.rs::insert_tx`'s stage-time
    //     `validate_unique_for_create` (read-only against committed state)
    //     now takes `released_unique_keys_in_tx` (this same live/released
    //     walk, run at stage time against only the value's own claimed
    //     keys) so it no longer synchronously rejects `INSERT B` with
    //     `DuplicateKey` before it is ever staged into
    //     `tx.index_write_set`.
    //   - THIS function's Step 2 durable check (below) independently made
    //     the same durable-only assumption: even after `INSERT B` reaches
    //     staging, `info_store.get(index_key)` still returns `A` (Phase 5c's
    //     materialize write, which would remove `A`'s posting, hasn't run
    //     yet — pre_commit validates BEFORE that write). Step 2 now
    //     tolerates a durable-owner mismatch when the key is in
    //     `ever_released` — this tx's own `index_write_set` staged a
    //     `RemovePosting` for that exact key earlier, so the stale durable
    //     owner is provably the record being released, not an unrelated
    //     conflict.
    // for keys NOT in the write set (self-write), the guard's owner stands.
    //
    // Why option (a) / naive overwrite on `unique_guards` is insufficient: it
    // has no release markers, so it cannot distinguish "A moved off K before B
    // claimed it" from "A never moved off K and B is a genuine duplicate" —
    // the former must commit, the latter must abort, but both produce the same
    // claims-only guard sequence [(K, A), (K, B)].

    // Step 1 — Walk unique-family ops in index_write_set in staging order to
    // detect intra-tx conflicts and determine the final live owner per key.
    // `live`: (table_token, index_key) → record CURRENTLY owning it as of the
    // op being processed.  `released`: keys vacated by a RemovePosting so the
    // guard loop (step 2) can distinguish "released" from "never in write set".
    // `ever_released`: the union of every key that had AT LEAST ONE
    // RemovePosting during the walk, whether or not it was later reclaimed —
    // #1096: Step 2's durable check needs this (not just the final-state
    // `released`) to recognize a release-then-reclaim-within-this-tx of a
    // key whose PRIOR owner is DURABLE (committed by an earlier tx), where
    // `released` alone would already have been cleared by the reclaiming
    // SetPosting.
    let mut live: TFxMap<(u64, bytes::Bytes), RecordId> = TFxMap::default();
    let mut released: TFxSet<(u64, bytes::Bytes)> = TFxSet::default();
    let mut ever_released: TFxSet<(u64, bytes::Bytes)> = TFxSet::default();
    // #1097 follow-up (found by an `@oh` review of the #1097 fix itself):
    // a `RemovePosting` this walk classifies as STALE (planned against a
    // record that no longer holds the key, per the match logic below) must
    // not just be ignored for `live`/`released` bookkeeping — it is still
    // physically present in `tx.index_write_set` and would otherwise reach
    // Phase 5c's `apply_index_ops_at_commit` unchanged, which applies EVERY
    // op in staging order with last-write-wins semantics. Left in place, a
    // stale removal ordered AFTER the genuine owner's `SetPosting` for the
    // same key deletes that owner's posting from `info_store` at commit —
    // the record's DATA row still carries the value, but the unique index
    // has no posting for it at all, silently freeing the key for an
    // unrelated future INSERT (the exact index/data divergence #1097 exists
    // to prevent, reachable one statement shorter than the original
    // reproduction, without ever needing a colliding third record). Indices
    // of stale ops are collected here and the ops themselves retracted from
    // `tx.index_write_set` below, before Phase 4's WAL write — mirroring
    // `retract_stale_provenance_ops`'s established retraction pattern in
    // this same file.
    let mut stale_remove_indices: TFxSet<usize> = TFxSet::default();
    // #1097 round 3 (found by a second `@oh` review): a `RemovePosting` with
    // NO in-tx `live` claim to compare against (`current_owner.is_none()`)
    // cannot be judged stale-or-not by the sync walk above at all — that is
    // exactly the shape of a bare `DELETE` with no companion `SetPosting` in
    // the same tx. Such an op previously executed unconditionally, with zero
    // validation: `delete_tx` records no `UniqueGuard` (Step 2 below never
    // even looks at the key), and there is nothing in `live` to collide
    // with. A `DELETE` planned against a stale snapshot could therefore
    // silently delete a DIFFERENT, concurrently-reclaiming record's genuine
    // durable posting — the same index/data divergence #1097 exists to
    // prevent, one more shape of it. Each such candidate is resolved against
    // the CURRENT DURABLE posting value below, under the same
    // `unique_write_lock` protection Step 2's own durable reads already
    // rely on (held since Phase 2.5, before this walk runs) — a mismatch
    // means this op's plan is stale relative to durable state and it is
    // retracted exactly like an intra-tx-stale op above.
    let mut unverified_removes: Vec<(usize, u64, bytes::Bytes, RecordId)> = Vec::new();
    for (idx, (table_token, op)) in tx.index_write_set.iter().enumerate() {
        match op {
            IndexWriteOp::SetPosting {
                key,
                value,
                provenance,
            } if provenance.family == IndexFamily::Unique => {
                let k = (*table_token, key.clone());
                // Identity v2 (design P34-12 (a)): a unique posting value is owner
                // RecordId (16 B) + optional canonical tail (digest form);
                // decode the owner from the first 16 bytes.
                let owner = unique_posting_owner(value).unwrap_or_default();
                if let Some(&prior) = live.get(&k) {
                    if prior != owner {
                        // Two DIFFERENT records claim the same unique key with
                        // no intervening RemovePosting — genuine intra-tx
                        // collision (the #1039 positive case).
                        repo.tx_metrics().on_tx_aborted_unique();
                        return Err(TxError::UniqueViolation { key: key.clone() });
                    }
                    // Same owner re-claiming its own key — not a conflict.
                    continue;
                }
                live.insert(k.clone(), owner);
                released.remove(&k);
            }
            IndexWriteOp::RemovePosting {
                key,
                provenance,
                owner,
            } if provenance.family == IndexFamily::Unique => {
                let k = (*table_token, key.clone());
                // #1097: only clear the live claim when this op's declared
                // owner actually matches the record `live` currently
                // attributes the key to. A MISMATCH means this removal was
                // planned against a record that no longer holds the key per
                // this tx's own walk so far (built against a stale snapshot
                // — see this file's #1097 doc above) — treat it as a stale
                // no-op: do not clear `live`, do not mark the key released,
                // so the key's real current owner within this tx is
                // preserved and Step 2 validates against it correctly. Also
                // retract the op itself (see the comment above this loop)
                // so it never reaches Phase 5c.
                let removing_owner = owner.map(RecordId);
                let current_owner = live.get(&k).copied();
                if let (None, Some(declared_owner)) = (current_owner, removing_owner) {
                    // No in-tx claim to judge this against — defer to the
                    // durable-state check below rather than assuming the
                    // plan is still valid (see the comment above this loop).
                    unverified_removes.push((idx, *table_token, key.clone(), declared_owner));
                    live.remove(&k);
                    released.insert(k.clone());
                    ever_released.insert(k);
                } else if removing_owner.is_none() || current_owner == removing_owner {
                    // The op didn't declare an owner (the unconditional
                    // pre-#1097 fallback, kept for any future non-unique
                    // construction site that might reach here) or its
                    // declared owner matches the current live claim.
                    live.remove(&k);
                    released.insert(k.clone());
                    ever_released.insert(k);
                } else {
                    stale_remove_indices.insert(idx);
                }
            }
            _ => {}
        }
    }
    for (idx, table_token, key, declared_owner) in &unverified_removes {
        let Some(tbl) = repo.table_by_token(*table_token).await? else {
            continue;
        };
        match tbl.info_store().get(key.clone().into()).await {
            Ok(existing) => {
                if unique_posting_owner(&existing) != Some(*declared_owner) {
                    // The durable posting is free, or durably owned by a
                    // DIFFERENT record than this removal was planned
                    // against (a concurrent tx reclaimed it after this tx's
                    // snapshot was taken) — this removal's plan is stale;
                    // retract it and undo its provisional released/
                    // ever_released marking above so a later op in this
                    // same tx cannot be misled by it either.
                    stale_remove_indices.insert(*idx);
                    let k = (*table_token, key.clone());
                    if !live.contains_key(&k) {
                        released.remove(&k);
                        ever_released.remove(&k);
                    }
                }
                // Else: durable owner matches — this removal is genuine,
                // keep it (it will correctly clear the durable owner's
                // posting at Phase 5c).
            }
            Err(DbError::NotFound(_)) => {
                // Already free durably — applying the removal again is a
                // harmless no-op at the storage layer; no retraction needed.
            }
            Err(e) => return Err(TxError::Storage(e)),
        }
    }
    if !stale_remove_indices.is_empty() {
        let mut idx = 0usize;
        tx.index_write_set.retain(|_| {
            let keep = !stale_remove_indices.contains(&idx);
            idx += 1;
            keep
        });
    }

    // Step 2 — Durable-state (cross-tx) check. Iterate tx.unique_guards to
    // discover every unique key this tx claims. For each key:
    //   - In `live`  → use the FINAL owner from the write-set walk (may differ
    //     from the guard's owner if the key was released and reclaimed).
    //   - In `released` only → the tx vacated this key; no durable check needed.
    //   - In neither → self-write (update re-writing same value: no SetPosting,
    //     but the guard's owner is the valid claim).
    let mut checked: TFxSet<(u64, bytes::Bytes)> = TFxSet::default();
    for g in &tx.unique_guards {
        let key = (g.table_token, g.index_key.clone());
        if !checked.insert(key.clone()) {
            continue; // already durable-checked this key
        }

        // #1101: Keys in `released` require a durable check too — if the tx
        // planned to release a key but a concurrently-committed tx already
        // claimed that key for an unrelated record, this tx's stale Set+Remove
        // pair would clobber that unrelated posting at Phase 5c. The check below
        // validates that the durable state is consistent with the tx's plan.
        let owner = if let Some(&final_owner) = live.get(&key) {
            Some(final_owner)
        } else if released.contains(&key) {
            // Key vacated by this tx's own ops. No owner to compare against
            // below, but we still need to check that nothing else durably
            // claimed it (see the durable check below).
            None
        } else {
            Some(g.owner) // self-write: no index op, guard's owner is authoritative
        };

        // Durable-state check against committed storage (unchanged in spirit
        // from #1039 — this task fixes what "seen" represents, not this check).
        //
        // #1096: a durable owner that differs from `owner` is NOT a conflict
        // when this tx itself released this exact key earlier in its own
        // `index_write_set` (`ever_released`) — the durable value is the
        // record this tx's own DELETE/UPDATE-off is removing; Phase 5c will
        // overwrite it atomically with the same posting ops validated here.
        //
        // #1096 follow-up (found by `@oh` review): `ever_released` alone is
        // NOT sufficient — it only proves this tx PLANNED to release the
        // key, not that the plan is still valid. Under `Snapshot` isolation
        // (documented last-writer-wins, no write-write conflict detection —
        // see `claim_write_set` above), that plan was built against a
        // possibly-STALE snapshot: a concurrently-committed tx may already
        // have reclaimed this key for an UNRELATED record between this tx's
        // snapshot and this check. Requiring the CURRENT durable owner
        // (`existing`) to be a record THIS tx has itself staged a write for
        // (`tx.write_set`) closes that hole — a record this tx never wrote
        // is never in its own write set, so a stale-snapshot race correctly
        // falls through to `UniqueViolation` below instead of silently
        // admitting two live records for the same unique value.
        //
        // #1101: The `released.contains(&key)` case previously skipped this
        // check entirely with `continue`. The fix is to perform the check with
        // `owner = None`: we require that the durable key is either NotFound
        // (genuinely free — the net-release's RemovePosting will be a no-op)
        // OR owned by a record this tx has staged a write for (touched). If
        // neither holds, an unrelated tx durably claimed the key and we must
        // abort to avoid clobbering that posting.
        if let Some(tbl) = repo.table_by_token(g.table_token).await? {
            match tbl.info_store().get(g.index_key.clone().into()).await {
                Ok(existing) => {
                    let released_and_touched = ever_released.contains(&key)
                        && unique_posting_owner(&existing).is_some_and(|existing_id| {
                            tx.write_set
                                .get(&g.table_token)
                                .is_some_and(|s| s.staged_op(existing_id.as_bytes()).is_some())
                        });
                    // For keys this tx still owns (owner is Some), compare the
                    // durable owner against the expected owner. For released keys
                    // (owner is None), just check the released_and_touched
                    // tolerance — a concurrent claim is only OK if this tx touched
                    // that record.
                    match owner {
                        None => {
                            // Released key: abort if a concurrent tx durably claimed
                            // it for an untouched record.
                            if !released_and_touched {
                                repo.tx_metrics().on_tx_aborted_unique();
                                return Err(TxError::UniqueViolation {
                                    key: g.index_key.clone(),
                                });
                            }
                        }
                        Some(expected_owner) => {
                            // Identity v2: compare the OWNER prefix (first 16
                            // bytes), not the whole value — a digest-form
                            // posting carries a canonical tail after it.
                            if unique_posting_owner(&existing) != Some(expected_owner)
                                && !released_and_touched
                            {
                                repo.tx_metrics().on_tx_aborted_unique();
                                return Err(TxError::UniqueViolation {
                                    key: g.index_key.clone(),
                                });
                            }
                        }
                    }
                }
                Err(DbError::NotFound(_)) => {
                    // Key is durably free. This is always OK:
                    // - For owned keys: our claim is uncontended.
                    // - For released keys: the RemovePosting will be a no-op
                    //   (key is already free).
                }
                Err(e) => return Err(TxError::Storage(e)),
            }
        }
    }

    // F-50 (#869, spike) — Phase 2.7: re-derive index2 posting ops for any
    // backend registered AFTER this tx's stage-time `all_backends()` snapshot.
    //
    // This closes #538 Part B's "guaranteed miss": a tx that staged before a
    // new index2 backend existed (via `create_index_v2`) and commits after it
    // was registered would otherwise carry zero ops for it in
    // `tx.index_write_set` (planned stale, at stage time), so Phase 5c would
    // have nothing to write — the row is permanently absent from the new
    // index. Part A's commit-time lock serialization (Phase 2.5 above) only
    // fixed the commit's TIMING; it cannot retroactively add ops to an
    // already-built plan.
    //
    // Placement is load-bearing for crash safety:
    //   - AFTER Phase 2.5's per-table `unique_write_lock` acquisition, so any
    //     in-flight `create_index_v2` (which holds that lock across its full
    //     backfill→register sequence) has finished registering by the time we
    //     re-derive — the live `backends_newer_than(stage_gen)` snapshot we
    //     take here includes it.
    //   - BEFORE Phase 4's WAL `begin_grouped` (which runs later, inside
    //     `pre_commit_locked_validate`): `wal_ops_from_tx` serializes
    //     `tx.index_write_set` directly into the WAL entry, so the re-derived
    //     ops MUST be appended before that serialization or recovery would
    //     replay the STALE stage-time plan (silently re-opening the exact miss
    //     this step closes). Re-deriving AFTER the WAL write is forbidden.
    //   - OUTSIDE `commit_lock` (this whole fn runs pre-lock): the work is
    //     per-table, async, and has no commit-window dependency, so holding
    //     the global commit_lock for it would be needless contention.
    //
    // The generation gate makes this zero-cost on the common path: a tx that
    // captured `index2_stage_gens` (populated only by index2-bearing-table
    // staging) and sees an unchanged generation skips the per-record
    // re-derivation entirely. See `rederive_index2_ops_post_stage` for the
    // old-value resolution (insert-vs-update-vs-delete) mechanism.
    rederive_index2_ops_post_stage(tx, repo).await?;

    // F-48b test seam: parks strictly AFTER Phase 2.5's flag-check loop
    // (every table's `needs_write_barrier()` has been read and the writer
    // has committed to the fast or slow path per table) and BEFORE the
    // function returns into the commit pipeline's later phases — in
    // particular before Phase 5c's materialize write (the actual data
    // store mutation). No-op in every non-test build.
    fire_post_prelock_pre_materialize_test_hook().await;

    // F-68 (#895) cluster D / task #124 — exit timestamp, paired with the
    // "enter" log above. If a run hangs, the LAST "enter" without a
    // matching "exit" for the same tx_id pinpoints that the stall is
    // somewhere inside this function (most likely the per-table
    // unique_write_lock loop just above — see task #897).
    log::debug!(
        "pre_commit_prelock: exit tx_id={tx_id_for_log} elapsed={:?} \
         uwl_guards={} drain_guards={}",
        prelock_started.elapsed(),
        uwl_guards.len(),
        drain_guards.len()
    );
    Ok(PreLockResult {
        uwl_guards,
        drain_guards,
    })
}

async fn validate_staging_table_identities(tx: &TxContext, repo: &RepoCore) -> Result<(), TxError> {
    for (token, staging) in &tx.write_set {
        // Named table staging is created through TableManager and records the
        // table name in `table_tokens`; enforce incarnation identity there.
        // Unnamed direct `write_set` staging is the supported low-level commit
        // API and has no catalogue identity to validate. Do not infer one from
        // the token: those commits may target independent raw stores.
        if !tx.table_tokens.contains_key(token) {
            continue;
        }
        // Lock-free and without write-domain admission: this runs while the commit already
        // holds drain guards. A dormant (not yet instantiated) cell means another incarnation.
        let Some(current) = repo.table_by_token_if_live(*token).await else {
            return Err(TxError::MigrationFrozen {
                table: tx.table_tokens[token].clone(),
            });
        };
        let same_incarnation = Arc::ptr_eq(staging.base(), current.data_store());
        if !same_incarnation {
            return Err(TxError::MigrationFrozen {
                table: tx.table_tokens[token].clone(),
            });
        }
    }
    Ok(())
}

fn touched_table_tokens(tx: &TxContext) -> Vec<u64> {
    let mut tokens: Vec<u64> = tx.write_set.keys().copied().collect();

    let mut previous_index_token = None;
    for (token, _) in &tx.index_write_set {
        if previous_index_token != Some(*token) {
            if !tx.write_set.contains_key(token) {
                tokens.push(*token);
            }
            previous_index_token = Some(*token);
        }
    }

    tokens.extend(
        tx.counter_deltas
            .keys()
            .filter(|token| !tx.write_set.contains_key(*token))
            .copied(),
    );
    tokens.extend(
        tx.staged_vectors
            .keys()
            .filter(|token| !tx.write_set.contains_key(*token))
            .copied(),
    );
    tokens.extend(
        tx.staged_vector_deletes
            .keys()
            .filter(|token| !tx.write_set.contains_key(*token))
            .copied(),
    );

    tokens.sort_unstable();
    tokens.dedup();
    tokens
}

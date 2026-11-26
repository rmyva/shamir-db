use std::sync::Arc;

use bytes::Bytes;
use shamir_storage::storage_in_memory::InMemoryRepo;
use shamir_storage::storage_in_memory::InMemoryStore;
use shamir_storage::types::{RecordKey, Store};
use shamir_tx::{IsolationLevel, StagingStore, TxContext, TxId, VersionProvider};
use shamir_types::access::Actor;
use shamir_types::types::record_id::RecordId;
use shamir_wal::WalOpV2;

use crate::tx::commit_tx;
use crate::tx::repo::repo_instance::RepoInstance;
use crate::tx::repo::repo_types::BoxRepo;
use crate::tx::table::table_manager::table_token_for;
use crate::tx::table::TableConfig;

fn make_repo() -> RepoInstance {
    let repo = Arc::new(InMemoryRepo::new());
    RepoInstance::new("test".into(), BoxRepo::InMemory(repo), Vec::new())
}

// C6 empty-tx fast-path: an empty tx no longer assigns a version or writes
// the WAL. It commits as a pure in-memory no-op, reporting
// `commit_version == snapshot_version` and `Complete`. (Was: asserted a
// fresh `commit_version > 0`; that pre-C6 contract is now covered by the
// non-empty commit tests, e.g. `commit_phase5_applies_write_set_to_base_store`.)
#[tokio::test]
async fn commit_empty_tx_succeeds() {
    let repo = make_repo();
    let tx = TxContext::new(TxId::new(1), 0, 0, IsolationLevel::Snapshot);
    let outcome = commit_tx(tx, &repo.core).await.unwrap();
    assert_eq!(outcome.tx_id, 1);
    assert_eq!(
        outcome.commit_version, outcome.snapshot_version,
        "empty fast-path pins commit_version to the snapshot version"
    );
    assert!(
        outcome.materialized(),
        "empty fast-path has nothing to materialize → Complete"
    );
}

#[tokio::test]
async fn commit_advances_last_committed() {
    let repo = make_repo();
    let gate = repo.tx_gate().await.unwrap();
    let before = gate.last_committed();

    // A non-empty tx (one staged write) so it crosses the commit point and
    // advances `last_committed`. An empty tx now takes the C6 fast-path and
    // intentionally does NOT advance the version (see `commit_empty_tx_succeeds`).
    let mut tx = TxContext::new(TxId::new(2), 0, 0, IsolationLevel::Snapshot);
    let data_store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let mut staging = StagingStore::new(Arc::clone(&data_store));
    staging.set(Bytes::from_static(b"k").into(), Bytes::from_static(b"v"));
    tx.write_set.insert(2, staging);
    let outcome = commit_tx(tx, &repo.core).await.unwrap();

    let after = gate.last_committed();
    assert!(
        outcome.commit_version > before,
        "non-empty commit advances version"
    );
    assert!(after >= outcome.commit_version);
    assert!(after >= before);
}

#[tokio::test]
async fn commit_writes_wal_entry() {
    let repo = make_repo();
    let wal = repo.repo_wal().await.unwrap();

    // Non-empty: an empty tx takes the C6 fast-path and never writes the
    // WAL at all. Stage a write so the commit appends a WAL entry.
    let mut tx = TxContext::new(TxId::new(3), 0, 0, IsolationLevel::Snapshot);
    let data_store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let mut staging = StagingStore::new(Arc::clone(&data_store));
    staging.set(Bytes::from_static(b"k").into(), Bytes::from_static(b"v"));
    tx.write_set.insert(3, staging);
    let _ = commit_tx(tx, &repo.core).await.unwrap();

    // F5e: the single WAL write path appends the entry to the segment; it
    // stays there (no per-entry removal) until F6 truncation, and replay
    // is idempotent.
    let inflight = wal.recover().await.unwrap();
    assert!(
        !inflight.is_empty(),
        "commit must append the tx's WAL entry to the segment"
    );
}

#[tokio::test]
async fn commit_two_txs_monotonic_versions() {
    let repo = make_repo();

    // Both txs stage a write so each crosses the commit point and assigns a
    // version (empty txs now fast-path without consuming a version — C6).
    let staged = |k: &'static [u8]| {
        let data_store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
        let mut s = StagingStore::new(data_store);
        let kb = Bytes::from_static(k);
        s.set(kb.into(), Bytes::from_static(b"v"));
        s
    };

    let mut tx1 = TxContext::new(TxId::new(10), 0, 0, IsolationLevel::Snapshot);
    tx1.write_set.insert(10, staged(b"k1"));
    let o1 = commit_tx(tx1, &repo.core).await.unwrap();

    let mut tx2 = TxContext::new(TxId::new(11), 0, 0, IsolationLevel::Snapshot);
    tx2.write_set.insert(11, staged(b"k2"));
    let o2 = commit_tx(tx2, &repo.core).await.unwrap();

    assert!(o2.commit_version > o1.commit_version);
}

#[tokio::test]
async fn repo_begin_tx_returns_valid_context() {
    let repo = make_repo();
    let (tx, guard) = repo
        .begin_tx(shamir_tx::IsolationLevel::Snapshot)
        .await
        .unwrap();
    assert_ne!(tx.repo_id, 0, "repo_id must be populated from repo_token");
    assert!(tx.tx_id.0 > 0, "fresh_tx_id must allocate");
    drop(guard);
}

#[tokio::test]
async fn repo_begin_then_commit_succeeds() {
    let repo = make_repo();
    let (tx, _guard) = repo
        .begin_tx(shamir_tx::IsolationLevel::Snapshot)
        .await
        .unwrap();
    // An empty begin→commit takes the C6 fast-path: it commits as a no-op
    // with `commit_version == snapshot_version` (no version burned).
    let outcome = repo.commit_tx(tx).await.unwrap();
    assert_eq!(outcome.commit_version, outcome.snapshot_version);
    assert!(outcome.materialized());
}

#[tokio::test]
async fn repo_two_concurrent_begin_tx_get_distinct_tx_ids() {
    let repo = make_repo();
    let (t1, _g1) = repo
        .begin_tx(shamir_tx::IsolationLevel::Snapshot)
        .await
        .unwrap();
    let (t2, _g2) = repo
        .begin_tx(shamir_tx::IsolationLevel::Snapshot)
        .await
        .unwrap();
    assert_ne!(t1.tx_id, t2.tx_id, "fresh_tx_id must be monotonic");
}

#[tokio::test]
async fn commit_phase5_applies_write_set_to_base_store() {
    let repo = make_repo();

    let mut tx = TxContext::new(TxId::new(100), 0, 0, IsolationLevel::Snapshot);
    let data_store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let mut staging = StagingStore::new(Arc::clone(&data_store));
    staging.set(
        Bytes::from_static(b"rid_1").into(),
        Bytes::from_static(b"payload"),
    );
    tx.write_set.insert(42, staging);

    assert!(
        data_store
            .get(Bytes::from_static(b"rid_1").into())
            .await
            .is_err(),
        "data_store must not have the key before commit"
    );

    let outcome = commit_tx(tx, &repo.core).await.unwrap();
    assert!(outcome.commit_version > 0);

    let got = data_store
        .get(Bytes::from_static(b"rid_1").into())
        .await
        .unwrap();
    assert_eq!(got, Bytes::from_static(b"payload"));
}

#[tokio::test]
async fn commit_applies_multiple_tables_atomically() {
    let repo = make_repo();
    let mut tx = TxContext::new(TxId::new(200), 0, 0, IsolationLevel::Snapshot);

    let s1: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let s2: Arc<dyn Store> = Arc::new(InMemoryStore::new());

    let mut st1 = StagingStore::new(Arc::clone(&s1));
    st1.set(Bytes::from_static(b"a").into(), Bytes::from_static(b"1"));
    tx.write_set.insert(1, st1);

    let mut st2 = StagingStore::new(Arc::clone(&s2));
    st2.set(Bytes::from_static(b"b").into(), Bytes::from_static(b"2"));
    tx.write_set.insert(2, st2);

    let _ = commit_tx(tx, &repo.core).await.unwrap();

    assert_eq!(
        s1.get(Bytes::from_static(b"a").into()).await.unwrap(),
        Bytes::from_static(b"1")
    );
    assert_eq!(
        s2.get(Bytes::from_static(b"b").into()).await.unwrap(),
        Bytes::from_static(b"2")
    );
}

#[tokio::test]
async fn commit_empty_write_set_still_succeeds() {
    let repo = make_repo();
    let tx = TxContext::new(TxId::new(300), 0, 0, IsolationLevel::Snapshot);
    // C6: empty tx commits via the fast-path (no version, no WAL).
    let outcome = commit_tx(tx, &repo.core).await.unwrap();
    assert_eq!(outcome.commit_version, outcome.snapshot_version);
    assert!(outcome.materialized());
}

#[tokio::test]
async fn commit_serializable_with_empty_read_set_succeeds() {
    let repo = make_repo();
    let tx = TxContext::new(TxId::new(500), 0, 0, IsolationLevel::Serializable);
    // empty read_set + zero provider → passes Phase 2, then the empty
    // op-set takes the C6 fast-path (no version, no WAL).
    let outcome = commit_tx(tx, &repo.core).await.unwrap();
    assert_eq!(outcome.commit_version, outcome.snapshot_version);
    assert!(outcome.materialized());
}

#[tokio::test]
async fn commit_serializable_with_read_set_passes_zero_provider_scaffold() {
    // Until Stage 4.D.6 plugs in a real version provider, the scaffold
    // uses `|_, _| 0`. A tx with a non-empty read_set but NO writes still
    // passes Phase 2 (0 ≤ version_seen trivially), then takes the C6
    // read-only fast-path (read_set is not gated by `is_empty`).
    let repo = make_repo();
    let mut tx = TxContext::new(TxId::new(501), 0, 0, IsolationLevel::Serializable);
    tx.record_read(7, Bytes::from_static(b"key"), 5);
    let outcome = commit_tx(tx, &repo.core).await.unwrap();
    assert_eq!(outcome.commit_version, outcome.snapshot_version);
    assert!(outcome.materialized());
}

#[tokio::test]
async fn commit_serializable_real_provider_detects_conflict() {
    struct ConflictProvider;
    impl VersionProvider for ConflictProvider {
        fn version_of(&self, _t: u64, _k: &Bytes) -> Option<u64> {
            Some(999)
        }
    }

    let repo = make_repo();
    let mut tx = TxContext::new(TxId::new(700), 0, 10, IsolationLevel::Serializable);
    tx.record_read(7, Bytes::from_static(b"k"), 5);
    tx.set_version_provider(Arc::new(ConflictProvider));

    let result = commit_tx(tx, &repo.core).await;
    assert!(result.is_err(), "real provider with conflict must abort");
    match result.unwrap_err() {
        crate::tx::CommitError::SsiConflict { key } => {
            assert_eq!(key, Bytes::from_static(b"k"));
        }
        e => panic!("expected SsiConflict, got {:?}", e),
    }
}

#[tokio::test]
async fn commit_serializable_real_provider_no_conflict_succeeds() {
    struct OkProvider;
    impl VersionProvider for OkProvider {
        fn version_of(&self, _t: u64, _k: &Bytes) -> Option<u64> {
            Some(5)
        }
    }

    let repo = make_repo();
    let mut tx = TxContext::new(TxId::new(701), 0, 10, IsolationLevel::Serializable);
    tx.record_read(7, Bytes::from_static(b"k"), 5);
    tx.set_version_provider(Arc::new(OkProvider));

    // Read-only Serializable tx, SSI validation passes (no conflict) → C6
    // fast-path. `commit_version` pins to the snapshot version (10).
    let outcome = commit_tx(tx, &repo.core).await.unwrap();
    assert_eq!(outcome.commit_version, outcome.snapshot_version);
    assert_eq!(outcome.commit_version, 10);
    assert!(outcome.materialized());
}

#[tokio::test]
async fn begin_tx_populates_repo_id_from_repo_token() {
    let repo = make_repo();
    let (tx, _guard) = repo
        .begin_tx(shamir_tx::IsolationLevel::Snapshot)
        .await
        .unwrap();
    assert_ne!(tx.repo_id, 0, "repo_id should be populated from repo_token");
}

#[tokio::test]
async fn commit_runs_apply_id_remap_phase_1_with_empty_overlay() {
    // Sanity: commit with empty interner_overlay (default state)
    // succeeds — Phase 1 is wired but no-op.
    let repo = make_repo();
    let (tx, _guard) = repo
        .begin_tx(shamir_tx::IsolationLevel::Snapshot)
        .await
        .unwrap();
    // Verify the overlay is empty (precondition).
    assert!(tx.interner_overlay.is_empty());
    // Empty tx → C6 fast-path (no version assigned).
    let outcome = repo.commit_tx(tx).await.unwrap();
    assert_eq!(outcome.commit_version, outcome.snapshot_version);
    assert!(outcome.materialized());
}

#[tokio::test]
async fn commit_with_non_empty_overlay_proceeds_with_warning() {
    // Until Stage 5 wires LayeredInterner, a non-empty overlay
    // triggers the warning path but commit still succeeds with an
    // empty remap (overlay entries are ignored).

    let repo = make_repo();
    let tx = TxContext::new(TxId::new(900), 0, 0, IsolationLevel::Snapshot);
    let _ = tx.interner_overlay.insert_sync("foo".to_string(), 12345);

    // Commit succeeds despite non-empty overlay (warning-only path).
    let outcome = commit_tx(tx, &repo.core).await.unwrap();
    assert!(outcome.commit_version > 0);
}

#[tokio::test]
async fn wal_ops_from_tx_emits_put_for_set_remove_for_remove() {
    let mut tx = TxContext::new(TxId::new(801), 0, 0, IsolationLevel::Snapshot);
    let data: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let mut staging = StagingStore::new(data);

    let rid_set = RecordId::new();
    let rid_del = RecordId::new();
    staging.set(rid_set.to_bytes().into(), Bytes::from_static(b"v"));
    staging.remove(rid_del.to_bytes().into());
    tx.write_set.insert(7, staging);

    let ops = crate::tx::commit::wal_ops_from_tx(&tx).await;

    let put_found = ops.iter().any(|op| {
        matches!(
            op,
            WalOpV2::Put {
                rid,
                table_id_interned,
                ..
            } if *rid == rid_set && *table_id_interned == 7
        )
    });
    let del_found = ops.iter().any(|op| {
        matches!(
            op,
            WalOpV2::Delete {
                rid,
                table_id_interned,
            } if *rid == rid_del && *table_id_interned == 7
        )
    });
    assert!(put_found, "expected WalOpV2::Put for staged Set");
    assert!(del_found, "expected WalOpV2::Delete for staged Remove");
}

// Originally named `ssi_conflict_detected_via_repo_version_provider` — the
// previous name implied conflict detection but the assertion (and reality)
// was the opposite under the OLD model where a non-tx insert did NOT bump the
// per-key version_cache.
//
// T1a reversed the HIGH-4 assumption: a non-tx insert NOW bumps the MVCC
// version and advances `last_committed`. The no-conflict expectation still
// holds, but for the RIGHT reason: the non-tx write PREDATES the tx's snapshot
// (the seed version ≤ tx.snapshot_version), so `version_of(key) ≤ snapshot`
// and `validate_read_set` sees no advance. The read is recorded at the real
// snapshot version (not a hardcoded 0).
#[tokio::test]
async fn ssi_no_conflict_when_only_non_tx_writes_predate_snapshot() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("users"));
    let tbl = repo.get_table("users").await.unwrap();
    let rid = tbl
        .insert(&shamir_types::types::value::InnerValue::Str("v".into()))
        .await
        .unwrap();

    let (mut tx, _g) = repo.begin_tx(IsolationLevel::Serializable).await.unwrap();

    let token = crate::tx::table::table_manager::table_token_for("users");
    // T1a: non-tx writes now bump the MVCC version + advance last_committed (the HIGH-4 "non-tx doesn't bump" assumption is intentionally reversed), so record the read at the actual snapshot version, not a hardcoded 0.
    tx.record_read(token, rid.to_bytes(), tx.snapshot_version);

    let outcome = repo.commit_tx(tx).await;
    assert!(
        outcome.is_ok(),
        "no conflict expected — the non-tx insert predates the snapshot (version ≤ snapshot), so SSI sees no advance"
    );
}

#[tokio::test]
async fn expired_tx_rejected_at_commit() {
    let repo = make_repo();
    repo.add_table_for_test(crate::tx::table::TableConfig::new("t"));

    let (tx, _g) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();

    // Backdating `started_at` via `Instant::now() - Duration::from_secs(N)`
    // panics on overflow if the host has been up for less than `N` (observed
    // on a freshly-booted CI VM). Shrink the effective max lifetime instead
    // and sleep past it — deterministic regardless of host uptime.
    crate::tx::commit::TEST_MAX_TX_LIFETIME_OVERRIDE
        .set(std::time::Duration::from_millis(1))
        .expect("override already set");
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    let result = repo.commit_tx(tx).await;
    assert!(result.is_err());
    let err = format!("{}", result.unwrap_err());
    assert!(
        err.contains("expired"),
        "expected expired error, got: {}",
        err
    );
}

#[tokio::test]
async fn tx_metrics_track_commit_and_abort() {
    let repo = make_repo();
    repo.add_table_for_test(crate::tx::table::TableConfig::new("t"));

    // Start + commit a tx.
    let (tx, _g) = repo
        .begin_tx(shamir_tx::IsolationLevel::Snapshot)
        .await
        .unwrap();
    repo.commit_tx(tx).await.unwrap();

    let snap = repo.tx_metrics().snapshot();
    assert!(snap.txs_started >= 1);
    assert!(snap.txs_committed >= 1);
    assert_eq!(snap.txs_aborted_ssi, 0);
}

// CRIT-1 regression test: recovery markers must survive a repo restart.
//
// The previous implementation only bumped the in-memory
// `last_committed_version` AtomicU64 in Phase 6 — on reopen the gate
// seeded from `MetaKey::LastCommittedVersion` (unset) → 0, so MVCC
// version monotonicity broke. Phase 6.5 in `commit_tx_inner` now
// persists both markers; this test exercises the round trip across
// drop+rebuild of `RepoInstance` over the same underlying `InMemoryRepo`.
#[tokio::test]
async fn last_committed_version_persists_across_restart() {
    let underlying = Arc::new(InMemoryRepo::new());
    let repo1 = RepoInstance::new(
        "test".into(),
        BoxRepo::InMemory(Arc::clone(&underlying)),
        Vec::new(),
    );
    repo1.add_table_for_test(crate::tx::table::TableConfig::new("t"));

    // Commit a few txs so commit_version advances past 0.
    for i in 0..3i64 {
        let (mut tx, _g) = repo1
            .begin_tx(shamir_tx::IsolationLevel::Snapshot)
            .await
            .unwrap();
        let tbl = repo1.get_table("t").await.unwrap();
        tbl.insert_tx(
            &shamir_types::types::value::InnerValue::Int(i),
            Some(&mut tx),
        )
        .await
        .unwrap();
        repo1.commit_tx(tx).await.unwrap();
    }
    let last_v_pre = repo1.tx_gate().await.unwrap().last_committed();
    assert!(
        last_v_pre > 0,
        "pre-restart gate must have advanced past zero"
    );

    drop(repo1);

    let repo2 = RepoInstance::new("test".into(), BoxRepo::InMemory(underlying), Vec::new());
    repo2.add_table_for_test(crate::tx::table::TableConfig::new("t"));
    let last_v_post = repo2.tx_gate().await.unwrap().last_committed();
    assert_eq!(
        last_v_post, last_v_pre,
        "last_committed_version must survive restart (CRIT-1)"
    );
}

// CRIT-3 regression test: the happy-path commit must advance the
// table counter in memory so callers see post-commit data without
// waiting for recovery. Previously Phase 5b was a TODO and the WAL
// `CounterDelta` op was only applied during crash replay.
#[tokio::test]
async fn commit_tx_advances_table_counter() {
    let repo = make_repo();
    repo.add_table_for_test(crate::tx::table::TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    let before = tbl.counter().get().await.unwrap();

    let (mut tx, _g) = repo
        .begin_tx(shamir_tx::IsolationLevel::Snapshot)
        .await
        .unwrap();
    tbl.insert_tx(
        &shamir_types::types::value::InnerValue::Int(1),
        Some(&mut tx),
    )
    .await
    .unwrap();
    tbl.insert_tx(
        &shamir_types::types::value::InnerValue::Int(2),
        Some(&mut tx),
    )
    .await
    .unwrap();
    repo.commit_tx(tx).await.unwrap();

    let after = tbl.counter().get().await.unwrap();
    assert_eq!(
        after - before,
        2,
        "counter must reflect committed inserts (CRIT-3)"
    );
}

// ===========================================================================
// C6 — empty-tx fast-path
//
// A tx that staged nothing durable (read-only Serializable txs, or any tx
// whose write_set / index_write_set / staged_vectors / counter_deltas /
// interner_overlay are all empty) commits as a pure in-memory no-op: it does
// NOT assign a new MVCC version (P0c: assign is deferred past this check)
// and does NOT write the WAL. The fast-path sits AFTER Phase 2 SSI
// validation, so a read-only Serializable tx that read stale data still
// ABORTS with `SsiConflict` (the version-assign + WAL + publish are the
// only steps skipped, never the SSI check).
// ===========================================================================

/// P0c: An empty tx does NOT allocate an MVCC version (assign is deferred
/// past the C6 check). Proven behaviourally: commit a non-empty tx
/// (version V), then an empty tx, then another non-empty tx — the second
/// non-empty tx gets V+1 because the empty tx consumed no version slot.
#[tokio::test]
async fn empty_tx_fast_path_assigns_no_version_and_no_wal() {
    let repo = make_repo();
    let wal = repo.repo_wal().await.unwrap();

    let staged = |k: &'static [u8]| {
        let data_store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
        let mut s = StagingStore::new(data_store);
        s.set(Bytes::from_static(k).into(), Bytes::from_static(b"v"));
        s
    };

    // Real tx #1 → assigns version V.
    let mut tx1 = TxContext::new(TxId::new(6001), 0, 0, IsolationLevel::Snapshot);
    tx1.write_set.insert(6001, staged(b"k1"));
    let v1 = commit_tx(tx1, &repo.core).await.unwrap().commit_version;
    assert!(v1 > 0);

    // WAL entry count after the real tx (which appended one entry; entries
    // persist in the segment until F6 truncation).
    let wal_len_before = wal.recover().await.unwrap().len();

    // Empty tx → fast-path: commit_version pinned to snapshot, no WAL.
    // P0c: no version is allocated (assign is deferred past C6 check).
    let snap_before = repo.tx_gate().await.unwrap().last_committed();
    let empty = TxContext::new(TxId::new(6002), 0, snap_before, IsolationLevel::Snapshot);
    let out = commit_tx(empty, &repo.core).await.unwrap();
    assert_eq!(
        out.commit_version, snap_before,
        "empty fast-path pins commit_version to the snapshot version"
    );
    assert!(out.materialized(), "empty fast-path is Complete");
    // The background drainer may truncate the already-durable tx #1 entry, so
    // the count can only shrink; the empty tx must never add an entry.
    let wal_after = wal.recover().await.unwrap();
    assert!(
        wal_after.len() <= wal_len_before,
        "empty fast-path must append no WAL entry"
    );
    assert!(
        wal_after.iter().all(|entry| entry.commit_version <= v1),
        "empty fast-path must not write a WAL entry for a later version"
    );

    // Real tx #2 → gets V+1 (the empty tx consumed no version slot — P0c).
    let mut tx3 = TxContext::new(TxId::new(6003), 0, 0, IsolationLevel::Snapshot);
    tx3.write_set.insert(6003, staged(b"k3"));
    let v2 = commit_tx(tx3, &repo.core).await.unwrap().commit_version;
    assert_eq!(
        v2,
        v1 + 1,
        "P0c: empty tx burns no version (expected {}, got {})",
        v1 + 1,
        v2
    );
}

/// A read-only Serializable tx whose read-set does NOT conflict passes SSI
/// validation and then takes the empty fast-path: it commits without a WAL
/// write and pins `commit_version` to the snapshot version.
#[tokio::test]
async fn read_only_serializable_no_conflict_fast_paths() {
    struct OkProvider;
    impl VersionProvider for OkProvider {
        fn version_of(&self, _t: u64, _k: &Bytes) -> Option<u64> {
            // Equal to the version the tx read → no conflict.
            Some(5)
        }
    }

    let repo = make_repo();
    let wal = repo.repo_wal().await.unwrap();

    let mut tx = TxContext::new(TxId::new(6100), 0, 7, IsolationLevel::Serializable);
    tx.record_read(7, Bytes::from_static(b"k"), 5);
    tx.set_version_provider(Arc::new(OkProvider));

    let out = commit_tx(tx, &repo.core).await.unwrap();
    assert_eq!(
        out.commit_version, 7,
        "read-only fast-path pins commit_version to the snapshot version"
    );
    assert!(out.materialized());
    assert!(
        wal.recover().await.unwrap().is_empty(),
        "read-only fast-path must write no WAL entry"
    );
}

/// CRITICAL C6 invariant: the empty fast-path must NOT swallow an SSI
/// conflict. A read-only Serializable tx with a STALE read (a concurrent
/// committer advanced the key past what the tx saw) must still ABORT —
/// proving the fast-path sits AFTER Phase 2 validation, not before it.
#[tokio::test]
async fn read_only_serializable_with_conflict_still_aborts() {
    struct ConflictProvider;
    impl VersionProvider for ConflictProvider {
        fn version_of(&self, _t: u64, _k: &Bytes) -> Option<u64> {
            // Far above what the tx read → SSI conflict.
            Some(999)
        }
    }

    let repo = make_repo();
    let wal = repo.repo_wal().await.unwrap();

    let mut tx = TxContext::new(TxId::new(6200), 0, 10, IsolationLevel::Serializable);
    tx.record_read(7, Bytes::from_static(b"k"), 5);
    tx.set_version_provider(Arc::new(ConflictProvider));

    let result = commit_tx(tx, &repo.core).await;
    match result {
        Err(crate::tx::CommitError::SsiConflict { key }) => {
            assert_eq!(key, Bytes::from_static(b"k"));
        }
        other => panic!(
            "a read-only SSI tx with a stale read must abort with SsiConflict, \
             not fast-path to success; got {:?}",
            other.map(|o| o.commit_version).map_err(|_| "Err(other)")
        ),
    }

    // The abort happened in Phase 2, before any WAL write.
    assert!(
        wal.recover().await.unwrap().is_empty(),
        "a pre-commit SSI abort leaves no WAL entry"
    );
}

// ===========================================================================
// P2c — disjoint-table parallelism (commit_lock dissolved)
// ===========================================================================

/// Two txs writing to DISJOINT tables commit fully in parallel (no global
/// serialization). Both succeed with distinct commit_versions.
#[tokio::test]
async fn p2c_disjoint_table_commits_run_in_parallel() {
    let repo = make_repo();

    let make_tx = |tx_id: u64, table_token: u64, key: &'static [u8]| {
        let mut tx = TxContext::new(TxId::new(tx_id), 0, 0, IsolationLevel::Snapshot);
        let data_store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
        let mut staging = StagingStore::new(data_store);
        staging.set(Bytes::from_static(key).into(), Bytes::from_static(b"v"));
        tx.write_set.insert(table_token, staging);
        tx
    };

    let tx_a = make_tx(9001, 100, b"ka");
    let tx_b = make_tx(9002, 200, b"kb");

    // Spawn both commits concurrently — without commit_mutex they should
    // not block each other.
    let repo2 = repo.clone();
    let (ra, rb) = tokio::join!(commit_tx(tx_a, &repo.core), commit_tx(tx_b, &repo2.core),);

    let oa = ra.expect("disjoint tx_a must succeed");
    let ob = rb.expect("disjoint tx_b must succeed");
    assert_ne!(
        oa.commit_version, ob.commit_version,
        "disjoint txs must get distinct versions"
    );
}

/// Two txs writing to the SAME (table, key) under Snapshot isolation:
/// both succeed because Snapshot does not detect write-write conflicts
/// (last-writer-wins). The uwl_guard serializes their materialize phases.
#[tokio::test]
async fn p2c_same_table_snapshot_both_succeed_last_writer_wins() {
    let repo = make_repo();
    let data_store: Arc<dyn Store> = Arc::new(InMemoryStore::new());

    let make_tx = |tx_id: u64| {
        let mut tx = TxContext::new(TxId::new(tx_id), 0, 0, IsolationLevel::Snapshot);
        let mut staging = StagingStore::new(Arc::clone(&data_store));
        staging.set(
            Bytes::from_static(b"same_key").into(),
            Bytes::from(format!("v{}", tx_id)),
        );
        tx.write_set.insert(42, staging);
        tx
    };

    let tx_a = make_tx(9010);
    let tx_b = make_tx(9011);

    let repo2 = repo.clone();
    let (ra, rb) = tokio::join!(commit_tx(tx_a, &repo.core), commit_tx(tx_b, &repo2.core),);

    // Under Snapshot isolation, both commits succeed (no conflict detection
    // for write-write — last-writer-wins at the data layer).
    ra.expect("same-table Snapshot tx_a must succeed");
    rb.expect("same-table Snapshot tx_b must succeed");

    // The data store has one of the two values (last-writer-wins).
    let got = data_store
        .get(Bytes::from_static(b"same_key").into())
        .await
        .unwrap();
    assert!(
        got == "v9010" || got == "v9011",
        "last-writer-wins: got {:?}",
        got
    );
}

// ===========================================================================
// L10(c) — interner_overlay skip when empty
// ===========================================================================

/// L10(c): `wal_ops_from_tx` with an empty `interner_overlay` must NOT
/// produce an `InternerOverlayMerge` op and must skip the async scan
/// entirely (the is_empty guard in commit.rs avoids the `.await` point).
#[tokio::test]
async fn wal_ops_empty_overlay_produces_no_interner_merge_op() {
    let tx = TxContext::new(TxId::new(10_001), 0, 0, IsolationLevel::Snapshot);
    assert!(
        tx.interner_overlay.is_empty(),
        "precondition: empty overlay"
    );

    let ops = crate::tx::commit::wal_ops_from_tx(&tx).await;
    let has_merge = ops
        .iter()
        .any(|op| matches!(op, WalOpV2::InternerOverlayMerge { .. }));
    assert!(
        !has_merge,
        "empty overlay must NOT produce InternerOverlayMerge"
    );
}

/// L10(c): `wal_ops_from_tx` with a non-empty `interner_overlay` DOES
/// produce an `InternerOverlayMerge` op carrying the overlay entries.
#[tokio::test]
async fn wal_ops_non_empty_overlay_produces_interner_merge_op() {
    let tx = TxContext::new(TxId::new(10_002), 0, 0, IsolationLevel::Snapshot);
    let _ = tx.interner_overlay.insert_sync("field_a".to_string(), 42);
    let _ = tx.interner_overlay.insert_sync("field_b".to_string(), 43);

    let ops = crate::tx::commit::wal_ops_from_tx(&tx).await;
    let merge = ops
        .iter()
        .find(|op| matches!(op, WalOpV2::InternerOverlayMerge { .. }));
    assert!(
        merge.is_some(),
        "non-empty overlay must produce InternerOverlayMerge"
    );
    if let Some(WalOpV2::InternerOverlayMerge { entries }) = merge {
        assert_eq!(entries.len(), 2, "both overlay entries must be captured");
    }
}

// ---------------------------------------------------------------------------
// S5c-X I1: the live commit path derives its journal event from the WAL
// entry (`shamir_tx::event_from_wal_entry`), and the entry carries the
// event header (`shamir_tx::event_header_from_tx` stamped in
// `pre_commit_locked_validate`).
// ---------------------------------------------------------------------------

/// PROPERTY: for randomized write-sets, the event derived from the entry
/// built by the LIVE path (`wal_ops_from_tx` + `event_header_from_tx`) —
/// after a full encode/decode round trip — equals the reference
/// projection (`shamir_tx::project_event`). Deterministic seeded PRNG; no
/// wall clock beyond the two timestamp samples (compared with the
/// monotonicity rule, not equality).
#[tokio::test]
async fn live_entry_derives_the_projected_event_for_random_write_sets() {
    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x >> 12;
            x ^= x << 25;
            x ^= x >> 27;
            self.0 = x;
            x.wrapping_mul(0x2545F4914F6CDD1D)
        }
        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }
    let mut rng = Rng(0xABCD_5C5C_EF01_0002);

    for case in 0..200u32 {
        let mut tx = TxContext::new(TxId::new(case as u64 + 1), 0, 0, IsolationLevel::Snapshot);
        tx.actor = match rng.below(3) {
            0 => Actor::System,
            1 => Actor::Admin(rng.below(50)),
            _ => Actor::User(rng.below(50)),
        };
        tx.implicit = rng.below(2) == 0;
        let n_tables = rng.below(4);
        for t in 0..n_tables {
            let token = 10 + t;
            tx.table_tokens.insert(token, format!("tbl_{t}"));
            let data_store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
            let mut staging = StagingStore::new(data_store);
            for o in 0..rng.below(6) {
                let non_record = rng.below(4) == 0;
                let key: Vec<u8> = if non_record {
                    format!("sys:{t}:{o}").into_bytes()
                } else {
                    let mut a = [0u8; 16];
                    a[14] = t as u8;
                    a[15] = o as u8;
                    a.to_vec()
                };
                if rng.below(3) == 0 {
                    staging.remove(RecordKey::from_slice(&key));
                } else {
                    staging.set(
                        RecordKey::from_slice(&key),
                        Bytes::from(format!("v{case}:{t}:{o}")),
                    );
                }
            }
            tx.write_set.insert(token, staging);
        }

        // The entry EXACTLY as `pre_commit_locked_validate` builds it.
        let wal_ops = crate::tx::wal_ops_from_tx(&tx).await;
        let version = 500 + case as u64;
        let entry = shamir_wal::WalEntryV2::new(tx.tx_id.0, 0, wal_ops)
            .with_commit_version(version)
            .with_event_header(shamir_tx::event_header_from_tx(&tx, "test"));
        let decoded = shamir_wal::WalEntryV2::decode(&entry.encode().unwrap()).unwrap();
        let derived = shamir_tx::event_from_wal_entry(&decoded);
        let expected = shamir_tx::project_event(&tx, "test", version);

        match expected {
            None => assert!(
                derived.is_none(),
                "case {case}: eventless commit must derive no event"
            ),
            Some(mut expected) => {
                let proj_ts = expected.timestamp_ns;
                expected.timestamp_ns = derived.as_ref().unwrap().timestamp_ns;
                assert!(
                    derived.as_ref().unwrap().timestamp_ns <= proj_ts + 1,
                    "case {case}: header timestamp must not drift ahead of the \
                     projection's sampling moment"
                );
                assert_eq!(
                    derived,
                    Some(expected),
                    "case {case}: live entry must derive exactly the projected \
                     event (order, fields, values)"
                );
            }
        }
    }
}

/// LIVE PATH: a real `commit_tx` persists a WAL entry whose header yields
/// the commit's journal event; recovery (`wal.recover()`) decodes the new
/// format and the event is derivable from the recovered entry alone.
#[tokio::test]
async fn committed_wal_entry_carries_a_derivable_event() {
    let repo = make_repo();
    // A named staging must belong to a live table incarnation (the M5 check).
    repo.add_table_for_test(TableConfig::new("items"));
    let table = repo.get_table("items").await.unwrap();
    let token = table_token_for("items");
    let mut tx = TxContext::new(TxId::new(31), 0, 0, IsolationLevel::Snapshot);
    let mut staging = StagingStore::new(table.data_store().clone());
    let mut rid_bytes = [0u8; 16];
    rid_bytes[15] = 9;
    staging.set(
        shamir_storage::types::RecordKey::from_slice(&rid_bytes),
        Bytes::from_static(b"payload"),
    );
    tx.table_tokens.insert(token, "items".to_string());
    tx.write_set.insert(token, staging);

    let outcome = commit_tx(tx, &repo.core).await.unwrap();

    let wal = repo.repo_wal().await.unwrap();
    let inflight = wal.recover().await.unwrap();
    let entry = inflight
        .iter()
        .find(|e| e.commit_version == outcome.commit_version)
        .expect("the committed entry must be recoverable from the WAL");

    let header = entry.event.as_ref().expect("v3 entry carries the header");
    assert_eq!(header.repo, "test");
    let event =
        shamir_tx::event_from_wal_entry(entry).expect("the committed event must be derivable");
    assert_eq!(event.commit_version, outcome.commit_version);
    assert_eq!(event.tx_id, 31);
    assert_eq!(event.changes.len(), 1);
    assert_eq!(event.changes[0].table, "items");
    assert_eq!(event.changes[0].key.as_ref(), &rid_bytes);
    assert_eq!(
        event.changes[0].value.as_deref(),
        Some(b"payload".as_slice())
    );
}

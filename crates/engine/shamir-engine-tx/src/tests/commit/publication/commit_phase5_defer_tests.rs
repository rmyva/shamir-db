//! Vector I.3 / MED-A: the WAL entry (Phase 4 `wal.begin`) is the commit
//! point. After it succeeds the tx is COMMITTED; the data/index/HNSW
//! projections (Phases 5a–5d) are eager materializations that may be
//! deferred to recovery on failure — they must NEVER turn a committed tx
//! into an abort.
//!
//! These tests pin the boundary from both sides:
//!
//!  * `post_commit_phase5_failure_is_committed_then_recovered` — inject a
//!    Phase 5c (index apply) failure AFTER the Phase 5a data write. The
//!    tx must report `Ok(MaterializationState::Deferred)` (NOT `Err`),
//!    leave its WAL marker inflight (Phase 7 skipped), and have recovery
//!    materialize the index posting + data. This FAILS against the old
//!    code, which propagated the Phase 5c `Err` as an abort.
//!
//!  * `pre_commit_failure_aborts_and_leaves_nothing` — a PRE-commit-point
//!    failure (SSI conflict) still returns `Err` (abort) and leaves no
//!    durable WAL marker. The commit point works both ways.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use bytes::Bytes;
use shamir_storage::storage_in_memory::InMemoryRepo;
use shamir_tx::{IndexWriteOp, IsolationLevel, StagingStore, TxContext, TxId, VersionProvider};
use shamir_types::types::record_id::RecordId;
use shamir_types::types::value::InnerValue;

use crate::repo::repo_instance::RepoInstance;
use crate::repo::repo_types::BoxRepo;
use crate::table::table_manager::table_token_for;
use crate::table::TableConfig;
use crate::tx::commit_phases::{
    FAIL_PHASE_5A_TABLE_TOKEN, FAIL_PHASE_5A_TX_ID, FAIL_PHASE_5C_TX_ID,
};
use crate::tx::tests::commit::visibility::materialized_means_visible_tests::spawn_deferred_commit;
use crate::tx::tests::drain::orphans::post_wal_orphan_ordering_tests::fixture;
use crate::tx::tx_outcome::MaterializationState;
use crate::tx::CommitError;

fn make_repo() -> RepoInstance {
    let repo = Arc::new(InMemoryRepo::new());
    RepoInstance::new("test".into(), BoxRepo::InMemory(repo), Vec::new())
}

/// Sentinel tx_id used to arm the Phase 5c failure injection. Picked far
/// above any id a fresh-repo gate or hand-built test allocates so the
/// process-global injection register can't collide with a parallel test.
const INJECT_TX_ID: u64 = 7_000_001;

/// Serialises the arm → commit → reset window of every test that drives
/// `FAIL_PHASE_5C_TX_ID`. That injection register is a single process-wide
/// `AtomicU64`; two such tests running on parallel runner threads would
/// otherwise clobber each other's arm (one test's unconditional `store(0)`
/// reset lands inside the other's commit window), making the Deferred path
/// flaky. The guard must span the commit `.await` (the register is read
/// during materialization), so this is `tokio::sync::Mutex` — async-aware,
/// no poisoning, and clippy-clean across the await. Contention is bounded
/// to the two injecting tests.
pub(in crate::tx::tests) static PHASE_5C_INJECT_LOCK: tokio::sync::Mutex<()> =
    tokio::sync::Mutex::const_new(());

/// Serialises the arm → commit → reset window of the multi-table Phase 5a
/// injection test. `FAIL_PHASE_5A_TX_ID` / `FAIL_PHASE_5A_TABLE_TOKEN` are a
/// process-wide pair; the guard must span the commit `.await` (the registers
/// are read during materialization). Only one test arms them today, so
/// contention is nil — the lock is future-proofing against a second armer.
static PHASE_5A_INJECT_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Sentinel tx_id for the multi-table Phase 5a injection test (kept far
/// above gate-allocated ids so the process-global register can't collide).
const MULTI_TABLE_INJECT_TX_ID: u64 = 7_000_010;

/// Decisive proof of Vector I.3: a Phase-5 sub-phase failure that occurs
/// AFTER the commit point (successful Phase 4 `wal.begin`) is reported as
/// COMMITTED-with-deferred-materialization, and recovery reconciles the
/// not-yet-applied projection.
///
/// `current_thread` flavor keeps the whole commit on one task/thread, so
/// the armed injection register is observed deterministically.
#[tokio::test(flavor = "current_thread")]
async fn post_commit_phase5_failure_is_committed_then_recovered() {
    let (repo, _) = fixture("phase5_deferred_replay").await;
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    let token = table_token_for("t");

    // A data record (16-byte RecordId so `wal_ops_from_tx` emits a Put)
    // staged over the table's real data_store — this is the Phase 5a write.
    let rid = RecordId::new();
    let body = InnerValue::Str("materialized-by-recovery".into())
        .to_bytes()
        .unwrap();

    let mut staging = StagingStore::new(Arc::clone(tbl.data_store()));
    staging.set(rid.to_bytes().into(), body.clone());

    // An index posting (Phase 5c). Recovery replays this as an IndexPut
    // against the table's info_store (table_id_interned = token).
    let posting_key = Bytes::from_static(b"phase5c_posting_key");
    let posting_val = Bytes::from_static(b"phase5c_posting_value");

    let mut tx = TxContext::new(TxId::new(INJECT_TX_ID), 0, 0, IsolationLevel::Snapshot);
    tx.write_set.insert(token, staging);
    tx.index_write_set.push((
        token,
        IndexWriteOp::SetPosting {
            key: posting_key.clone(),
            value: posting_val.clone(),
            provenance: Default::default(),
        },
    ));

    // Arm the Phase 5c failure for exactly this tx, then commit. Reset the
    // register immediately after so no later test is affected. The lock
    // serialises this window against any other `FAIL_PHASE_5C_TX_ID` armer.
    let inject_guard = PHASE_5C_INJECT_LOCK.lock().await;
    FAIL_PHASE_5C_TX_ID.store(INJECT_TX_ID, Ordering::SeqCst);
    let task = spawn_deferred_commit(&repo, tx).await;
    FAIL_PHASE_5C_TX_ID.store(0, Ordering::SeqCst);
    drop(inject_guard);

    // The ack remains pending until online replay settles visibility.
    assert!(!task.is_finished());

    // (2) The WAL entry is in the segment so recovery re-applies it — the
    //     deferred projection's safety net.
    let wal = repo.repo_wal().await.unwrap();
    let inflight = wal.recover().await.unwrap();
    assert_eq!(
        inflight.len(),
        1,
        "the deferred tx's WAL entry must be replayable"
    );
    assert_eq!(inflight[0].txn_id, INJECT_TX_ID);

    // (3) The index posting did NOT materialize inline (Phase 5c failed).
    assert!(
        tbl.info_store()
            .get(posting_key.clone().into())
            .await
            .is_err(),
        "index posting must be absent before recovery (Phase 5c was injected to fail)"
    );

    // (4) Recovery materializes the deferred projection — index posting +
    //     data both land, and the marker is cleaned.
    let count = repo.drainer().drain_all(&repo.core).await.unwrap();
    assert_eq!(count, 1, "online replay must settle the one inflight entry");
    let outcome = task.await.unwrap().unwrap();
    assert_eq!(outcome.materialization, MaterializationState::Deferred);
    assert!(!outcome.materialized());

    let recovered_posting = tbl
        .info_store()
        .get(posting_key.into())
        .await
        .expect("recovery must materialize the index posting (IndexPut replay)");
    assert_eq!(recovered_posting, posting_val);

    let recovered_data = tbl
        .get(rid)
        .await
        .expect("recovery must materialize the data record (Put replay)");
    assert!(
        matches!(recovered_data, InnerValue::Str(ref s) if s == "materialized-by-recovery"),
        "recovered data must match the committed body, got {recovered_data:?}"
    );

    // Final state is consistent + queryable. The file/Mem WAL has no
    // per-entry truncation until F6, so a re-run replays the same entry
    // again; replay is idempotent — the data + posting converge to the same
    // single value.
    repo.recover_v2_inflight().await.unwrap();
    let again = tbl
        .get(rid)
        .await
        .expect("data record must still be present after an idempotent re-replay");
    assert!(matches!(again, InnerValue::Str(ref s) if s == "materialized-by-recovery"));
}

/// The other half of the boundary: a PRE-commit-point failure (here an
/// SSI conflict surfaced before Phase 4) still returns `Err` (a clean
/// abort) and leaves NOTHING durable — no inflight WAL marker.
#[tokio::test]
async fn pre_commit_failure_aborts_and_leaves_nothing() {
    struct ConflictProvider;
    impl VersionProvider for ConflictProvider {
        fn version_of(&self, _t: u64, _k: &Bytes) -> Option<u64> {
            // A version far above what the tx read → SSI conflict.
            Some(u64::MAX)
        }
    }

    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));

    let mut tx = TxContext::new(TxId::new(424_242), 0, 1, IsolationLevel::Serializable);
    tx.record_read(table_token_for("t"), Bytes::from_static(b"k"), 0);
    tx.set_version_provider(Arc::new(ConflictProvider));

    let result = repo.commit_tx(tx).await;
    match result {
        Err(CommitError::SsiConflict { key }) => {
            assert_eq!(key, Bytes::from_static(b"k"));
        }
        other => panic!(
            "pre-commit SSI conflict must return Err(SsiConflict), got {:?}",
            other.map(|o| o.commit_version).map_err(|_| "Err(other)")
        ),
    }

    // Nothing durable: the abort happened before Phase 4, so no WAL marker
    // was ever written.
    let wal = repo.repo_wal().await.unwrap();
    assert!(
        wal.recover().await.unwrap().is_empty(),
        "a pre-commit abort must leave no WAL entry"
    );
}

/// Metric (I.3 follow-up): a deferred materialization bumps the dedicated
/// `TxMetrics::txs_materialization_deferred` counter exactly once. We reuse
/// the Phase 5c failure injection to drive a `Deferred` outcome and assert
/// the counter delta — proving `commit_tx_inner` fires
/// `on_tx_materialization_deferred()` when (and only when) `materialize`
/// reports `Deferred`.
///
/// `current_thread` flavor keeps the commit on one task/thread so the armed
/// injection register is observed deterministically (and the fresh-repo
/// metric snapshot is uncontended).
#[tokio::test(flavor = "current_thread")]
async fn deferred_materialization_increments_metric() {
    const METRIC_INJECT_TX_ID: u64 = 7_000_002;

    let (repo, _) = fixture("phase5_deferred_metric").await;
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    let token = table_token_for("t");

    let rid = RecordId::new();
    let body = InnerValue::Str("deferred-metric".into())
        .to_bytes()
        .unwrap();
    let mut staging = StagingStore::new(Arc::clone(tbl.data_store()));
    staging.set(rid.to_bytes().into(), body);

    let mut tx = TxContext::new(
        TxId::new(METRIC_INJECT_TX_ID),
        0,
        0,
        IsolationLevel::Snapshot,
    );
    tx.write_set.insert(token, staging);
    tx.index_write_set.push((
        token,
        IndexWriteOp::SetPosting {
            key: Bytes::from_static(b"metric_posting_key"),
            value: Bytes::from_static(b"metric_posting_value"),
            provenance: Default::default(),
        },
    ));

    let before = repo.tx_metrics().snapshot().txs_materialization_deferred;

    // Serialise the arm → commit → reset window against the other
    // `FAIL_PHASE_5C_TX_ID` armer (see `PHASE_5C_INJECT_LOCK`).
    let inject_guard = PHASE_5C_INJECT_LOCK.lock().await;
    FAIL_PHASE_5C_TX_ID.store(METRIC_INJECT_TX_ID, Ordering::SeqCst);
    let task = spawn_deferred_commit(&repo, tx).await;
    FAIL_PHASE_5C_TX_ID.store(0, Ordering::SeqCst);
    drop(inject_guard);
    assert_eq!(repo.drainer().drain_all(&repo.core).await.unwrap(), 1);
    let outcome = task.await.unwrap().unwrap();
    assert_eq!(
        outcome.materialization,
        MaterializationState::Deferred,
        "the injected Phase 5c failure must produce a Deferred outcome"
    );

    let after = repo.tx_metrics().snapshot().txs_materialization_deferred;
    assert_eq!(
        after - before,
        1,
        "a deferred materialization must increment txs_materialization_deferred exactly once"
    );
}

/// Audit MED: partial latest-state visibility is reconciled by online replay.
/// A single transaction writes two tables; B fails Phase 5a while A lands.
/// The ack must stay pending until replay makes both tables visible. Snapshots
/// stay below the version in that interval (covered by the SNAP-6 regressions).
///
/// `current_thread` flavor keeps the whole commit on one task/thread so the
/// process-global injection register pair is observed deterministically.
#[tokio::test(flavor = "current_thread")]
async fn multi_table_partial_deferral_is_reconciled_by_recovery() {
    let (repo, _) = fixture("phase5_partial_replay").await;
    // Phase 5a routes through apply_committed_visible after table registration.
    let tbl_a = repo.get_table("a").await.unwrap();
    let tbl_b = repo.get_table("b").await.unwrap();
    let token_a = table_token_for("a");
    let token_b = table_token_for("b");

    // Table A: a data record + an index posting (both must materialize
    // inline — A is NOT injected to fail).
    let rid_a = RecordId::new();
    let body_a = InnerValue::Str("table-a-row".into()).to_bytes().unwrap();
    let mut staging_a = StagingStore::new(Arc::clone(tbl_a.data_store()));
    staging_a.set(rid_a.to_bytes().into(), body_a);
    let posting_a_key = Bytes::from_static(b"table_a_posting_key");
    let posting_a_val = Bytes::from_static(b"table_a_posting_value");

    // Table B: a data record ONLY. Its Phase 5a write is the one we fail, so
    // table B ends up entirely unmaterialized (no leaked index posting to
    // muddy the "B not materialized" assertion).
    let rid_b = RecordId::new();
    let body_b = InnerValue::Str("table-b-row".into()).to_bytes().unwrap();
    let mut staging_b = StagingStore::new(Arc::clone(tbl_b.data_store()));
    staging_b.set(rid_b.to_bytes().into(), body_b);

    let mut tx = TxContext::new(
        TxId::new(MULTI_TABLE_INJECT_TX_ID),
        0,
        0,
        IsolationLevel::Snapshot,
    );
    tx.write_set.insert(token_a, staging_a);
    tx.write_set.insert(token_b, staging_b);
    tx.index_write_set.push((
        token_a,
        IndexWriteOp::SetPosting {
            key: posting_a_key.clone(),
            value: posting_a_val.clone(),
            provenance: Default::default(),
        },
    ));

    // Arm a Phase 5a failure for EXACTLY (this tx, table B), commit, then
    // disarm. The lock serialises the arm→commit→reset window.
    let inject_guard = PHASE_5A_INJECT_LOCK.lock().await;
    FAIL_PHASE_5A_TX_ID.store(MULTI_TABLE_INJECT_TX_ID, Ordering::SeqCst);
    FAIL_PHASE_5A_TABLE_TOKEN.store(token_b, Ordering::SeqCst);
    let task = spawn_deferred_commit(&repo, tx).await;
    FAIL_PHASE_5A_TX_ID.store(0, Ordering::SeqCst);
    FAIL_PHASE_5A_TABLE_TOKEN.store(0, Ordering::SeqCst);
    drop(inject_guard);

    // WAL acceptance is committed, but acknowledgement waits for replay.
    assert!(!task.is_finished());

    // (b) One WAL entry in the segment (the deferred tx's, for recovery).
    let wal = repo.repo_wal().await.unwrap();
    let inflight = wal.recover().await.unwrap();
    assert_eq!(inflight.len(), 1, "the deferred tx's WAL entry must replay");
    assert_eq!(inflight[0].txn_id, MULTI_TABLE_INJECT_TX_ID);

    // (c) BEFORE recovery: the cross-table split is observable at the same
    //     committed version. Table A fully materialized; table B's data absent.
    let a_row = tbl_a
        .get(rid_a)
        .await
        .expect("table A data must be materialized inline (A was not injected)");
    assert!(
        matches!(a_row, InnerValue::Str(ref s) if s == "table-a-row"),
        "table A row must be its committed value, got {a_row:?}"
    );
    assert_eq!(
        tbl_a
            .info_store()
            .get(posting_a_key.clone().into())
            .await
            .expect("table A index posting must be materialized inline"),
        posting_a_val,
        "table A index posting must land inline (Phase 5c ran for A)"
    );
    assert!(
        tbl_b.get(rid_b).await.is_err(),
        "table B data MUST be absent before recovery — this is the cross-table \
         inconsistency: a reader sees A's new row + B's missing row at the same \
         committed version (audit MED)"
    );

    // (d) AFTER recovery: BOTH tables consistent. Recovery replays the one
    //     inflight WAL entry, which carries every table's ops (Put(A), Put(B),
    //     IndexPut(A)), so table B's data lands and table A is unchanged.
    let count = repo.drainer().drain_all(&repo.core).await.unwrap();
    assert_eq!(count, 1, "online replay must settle the one inflight entry");
    let outcome = task.await.unwrap().unwrap();
    assert_eq!(outcome.materialization, MaterializationState::Deferred);
    assert!(!outcome.materialized());

    let b_row = tbl_b
        .get(rid_b)
        .await
        .expect("recovery must materialize table B's data (Put replay)");
    assert!(
        matches!(b_row, InnerValue::Str(ref s) if s == "table-b-row"),
        "recovered table B row must match the committed value, got {b_row:?}"
    );
    // Table A unchanged and still consistent.
    let a_row_after = tbl_a.get(rid_a).await.unwrap();
    assert!(matches!(a_row_after, InnerValue::Str(ref s) if s == "table-a-row"));
    assert_eq!(
        tbl_a.info_store().get(posting_a_key.into()).await.unwrap(),
        posting_a_val
    );

    // The WAL has no per-entry truncation until F6, so a second recovery
    // pass replays the same entry again; replay is idempotent — both tables
    // remain at their single committed values.
    repo.recover_v2_inflight().await.unwrap();
    let b_again = tbl_b.get(rid_b).await.unwrap();
    assert!(matches!(b_again, InnerValue::Str(ref s) if s == "table-b-row"));
}

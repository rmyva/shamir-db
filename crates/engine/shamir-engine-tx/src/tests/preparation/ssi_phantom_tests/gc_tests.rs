//! Step 7 — commit-write-log GC prune integration tests and Snapshot isolation
//! guards.

use shamir_collections::TFxMap;

use shamir_tx::predicate_set::PredicateDep;
use shamir_tx::repo_tx_gate::{CommitWriteRecord, TableWriteFootprint};
use shamir_tx::IsolationLevel;
use shamir_types::types::value::InnerValue;

use crate::table::table_manager::table_token_for;
use crate::table::TableConfig;

use super::helpers::make_repo;

/// Step 7: the commit-write-log MUST retain records a live snapshot still
/// needs. After one Serializable commit (V1) we hold a snapshot at V1.
/// Then we plant a second record at V2 directly via `record_commit_writes`
/// (bypassing `begin_tx` to avoid a snapshot-key collision in
/// `active_snapshots`). `run_gc` with `min_alive == V1` must NOT drop V2
/// (it satisfies `commit_version > min_alive`). V1 (== min_alive) IS
/// prunable because no live snapshot has `S < V1`.
///
/// After the snapshot is released, `min_alive` jumps to `last_committed`
/// (>= V2), and a second `run_gc` cleans everything.
#[tokio::test]
async fn commit_write_log_not_pruned_below_live_snapshot() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();

    // --- Tx1: Serializable commit → log has one record at V1.
    let (mut tx1, g1) = repo.begin_tx(IsolationLevel::Serializable).await.unwrap();
    tbl.insert_tx(&InnerValue::Str("a".into()), Some(&mut tx1))
        .await
        .unwrap();
    repo.commit_tx(tx1).await.unwrap();
    drop(g1);

    // --- Hold a snapshot at V1 (= last_committed after tx1).
    // This freezes min_alive at V1.
    let gate = repo.tx_gate().await.unwrap();
    let snap_guard = gate.open_snapshot().await;
    let snap_v = snap_guard.version();

    // --- Plant a second record directly at V2 > V1 via the gate API,
    // bypassing `begin_tx` to avoid colliding with snap_guard's version
    // in `active_snapshots` (which is keyed by version).
    let v2_guard = gate.assign_next_version_guarded();
    let v2 = v2_guard.version();
    gate.record_commit_writes(CommitWriteRecord {
        commit_version: v2,
        per_table: TFxMap::from_iter([(
            table_token_for("t"),
            TableWriteFootprint {
                touched: true,
                inserted_index_keys: vec![],
            },
        )]),
    });
    v2_guard.commit();
    assert!(v2 > snap_v, "V2 must exceed the held snapshot");

    // --- Sanity: log has at least 2 records (V1, V2) before GC.
    let before = gate.commit_log_len();
    assert!(before >= 2, "expected V1 and V2 in log, got {before}");

    // --- Run GC while snapshot is held: min_alive == snap_v == V1.
    // Prune removes commit_version <= V1. V2 (> V1) MUST survive.
    let _ = repo.run_gc().await.unwrap();

    let after = gate.commit_log_len();
    assert!(
        after >= 1,
        "V2 (> min_alive == {snap_v}) must survive GC, got {after} records"
    );

    // --- Drop the snapshot — min_alive jumps to last_committed (= V2).
    // Now V2 is at-or-below min_alive and is droppable.
    drop(snap_guard);
    let _ = repo.run_gc().await.unwrap();
    let after2 = gate.commit_log_len();
    assert_eq!(
        after2, 0,
        "with no live snapshots, all records <= min_alive prune away"
    );
}

/// Symmetry of the discipline: on a non-Serializable workload the log is
/// never written, so `run_gc` is zero-overhead w.r.t. the new prune.
#[tokio::test]
async fn snapshot_workload_leaves_log_empty_and_gc_is_noop() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();

    // Snapshot-isolation commits do NOT append to the log (gated on
    // isolation, see build_footprint_from_tx).
    for i in 0..8 {
        let (mut tx, g) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
        let _ = tbl
            .insert_tx(&InnerValue::Int(i), Some(&mut tx))
            .await
            .unwrap();
        repo.commit_tx(tx).await.unwrap();
        drop(g);
    }

    let gate = repo.tx_gate().await.unwrap();
    assert_eq!(gate.commit_log_len(), 0);
    let _ = repo.run_gc().await.unwrap();
    assert_eq!(gate.commit_log_len(), 0);
}

/// Snapshot isolation must NOT trigger Phase 2-bis even with manually armed
/// predicate deps. Guards the zero-overhead claim.
#[tokio::test]
async fn snapshot_skips_phase_2bis() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("users"));
    let tbl = repo.get_table("users").await.unwrap();
    let table_token = table_token_for("users");

    let (mut tx2, _g2) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();

    // Even though we force-arm a predicate dep, Snapshot must skip Phase 2-bis.
    // (In production, `record_predicate_shared` is itself gated on Serializable
    // and would never push, but we test the commit-side gate defensively.)
    tx2.predicate_set
        .push(PredicateDep::TableScan { table_token });

    let gate = repo.tx_gate().await.unwrap();
    let tx1_guard = gate.assign_next_version_guarded();
    let tx1_commit_v = tx1_guard.version();
    let mut per_table = TFxMap::default();
    per_table.insert(
        table_token,
        TableWriteFootprint {
            touched: true,
            inserted_index_keys: vec![],
        },
    );
    gate.record_commit_writes(CommitWriteRecord {
        commit_version: tx1_commit_v,
        per_table,
    });
    tx1_guard.commit();

    tbl.insert_tx(&InnerValue::Str("dummy".into()), Some(&mut tx2))
        .await
        .unwrap();

    let res = repo.commit_tx(tx2).await;
    assert!(
        res.is_ok(),
        "Snapshot tx must not trigger Phase 2-bis: {:?}",
        res.err()
    );
    assert_eq!(
        repo.tx_metrics().snapshot().txs_aborted_phantom,
        0,
        "phantom counter must be untouched for Snapshot"
    );
}

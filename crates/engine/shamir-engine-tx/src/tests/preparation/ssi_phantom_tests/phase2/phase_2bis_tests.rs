//! Phase C — SSI phantom detection tests.
//!
//! Step 4: Phase 2-bis predicate validation in pre_commit — asserts that
//! an armed predicate dependency whose interval is hit by a concurrent
//! committer aborts the commit with `PhantomConflict` and bumps
//! `txs_aborted_phantom`.

use shamir_collections::TFxMap;
use std::ops::Bound;

use shamir_tx::predicate_set::PredicateDep;
use shamir_tx::repo_tx_gate::{CommitWriteRecord, TableWriteFootprint};
use shamir_tx::IsolationLevel;
use shamir_types::types::value::InnerValue;

use crate::table::table_manager::table_token_for;
use crate::table::TableConfig;
use crate::tx::CommitError;

use super::super::helpers::{make_repo, test_bound_key, test_index_id, test_posting_key};

/// THE Step 4 proof. A Serializable tx2 arms a predicate dependency covering
/// `age >= 30` on the "age" index. A concurrent committer (tx1) writes a
/// posting key with `age = 35` into the commit-write log. When tx2 commits,
/// Phase 2-bis must detect the phantom and abort with `PhantomConflict`.
#[tokio::test]
async fn phase_2bis_aborts_armed_predicate_on_concurrent_write() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("users"));
    let tbl = repo.get_table("users").await.unwrap();
    let table_token = table_token_for("users");
    let idx_id = test_index_id("age");

    // tx2 (Serializable): open BEFORE the concurrent writer commits.
    let (mut tx2, _g2) = repo.begin_tx(IsolationLevel::Serializable).await.unwrap();
    let snapshot_v = tx2.snapshot_version;

    // --- Simulate tx1: a concurrent committer whose footprint lands inside
    // tx2's predicate interval. We bypass the full commit pipeline and plant
    // the footprint directly via `gate.record_commit_writes` (Step 3 API).
    let gate = repo.tx_gate().await.unwrap();
    let tx1_commit_v = gate.assign_next_version(); // > snapshot_v
    assert!(
        tx1_commit_v > snapshot_v,
        "tx1 commit version must exceed tx2's snapshot"
    );

    let posting_key = test_posting_key(idx_id, 35);
    let mut per_table = TFxMap::default();
    per_table.insert(
        table_token,
        TableWriteFootprint {
            touched: true,
            inserted_index_keys: vec![posting_key],
        },
    );
    gate.record_commit_writes(CommitWriteRecord {
        commit_version: tx1_commit_v,
        per_table,
    });
    gate.publish_committed_max(tx1_commit_v);

    // --- Arm tx2's predicate_set: covers age >= 30.
    let dep = PredicateDep::IndexRange {
        table_token,
        index_id: idx_id,
        lo: Bound::Included(test_bound_key(idx_id, 30)),
        hi: Bound::Unbounded,
    };
    tx2.predicate_set.push(dep);

    // Stage at least one durable op so we do NOT trip the C6 empty-tx
    // fast-path (otherwise the test would pass by accident).
    tbl.insert_tx(&InnerValue::Str("dummy".into()), Some(&mut tx2))
        .await
        .unwrap();

    // --- Commit tx2: must abort with PhantomConflict.
    let metrics_before = repo.tx_metrics().snapshot().txs_aborted_phantom;
    let res = repo.commit_tx(tx2).await;
    let metrics_after = repo.tx_metrics().snapshot().txs_aborted_phantom;

    match res {
        Err(CommitError::PhantomConflict { dep }) => {
            assert!(
                dep.contains("IndexRange"),
                "dep summary should name the variant: {dep}"
            );
        }
        other => panic!(
            "expected PhantomConflict (concurrent committer wrote into armed predicate \
             range); got {:?}",
            other
                .map(|o| format!("Ok({:?})", o.commit_version))
                .unwrap_or_else(|e| format!("Err({e})"))
        ),
    }
    assert_eq!(
        metrics_after,
        metrics_before + 1,
        "on_tx_aborted_phantom counter must increment exactly once"
    );
    // Ordering check: the point read-set was empty, so SsiConflict path must
    // NOT have bumped.
    assert_eq!(
        repo.tx_metrics().snapshot().txs_aborted_ssi,
        0,
        "SSI point-read counter must be untouched"
    );
}

/// A Serializable tx with a predicate dep that is NOT conflicted must commit
/// successfully. Guards against false positives.
#[tokio::test]
async fn phase_2bis_allows_non_conflicting_predicate() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("users"));
    let tbl = repo.get_table("users").await.unwrap();
    let table_token = table_token_for("users");
    let idx_id = test_index_id("age");

    let (mut tx2, _g2) = repo.begin_tx(IsolationLevel::Serializable).await.unwrap();
    // tx1 writes age=25 which is OUTSIDE tx2's predicate range (age >= 30).
    let gate = repo.tx_gate().await.unwrap();
    let tx1_commit_v = gate.assign_next_version();
    let posting_key = test_posting_key(idx_id, 25);
    let mut per_table = TFxMap::default();
    per_table.insert(
        table_token,
        TableWriteFootprint {
            touched: true,
            inserted_index_keys: vec![posting_key],
        },
    );
    gate.record_commit_writes(CommitWriteRecord {
        commit_version: tx1_commit_v,
        per_table,
    });
    gate.publish_committed_max(tx1_commit_v);

    // Arm tx2's predicate: age >= 30 — tx1's age=25 is outside.
    let dep = PredicateDep::IndexRange {
        table_token,
        index_id: idx_id,
        lo: Bound::Included(test_bound_key(idx_id, 30)),
        hi: Bound::Unbounded,
    };
    tx2.predicate_set.push(dep);

    tbl.insert_tx(&InnerValue::Str("dummy".into()), Some(&mut tx2))
        .await
        .unwrap();

    let res = repo.commit_tx(tx2).await;
    assert!(
        res.is_ok(),
        "non-conflicting predicate must not abort: {:?}",
        res.err()
    );
}

/// TableScan predicate: ANY write into the table by a concurrent committer
/// must trigger PhantomConflict.
#[tokio::test]
async fn phase_2bis_table_scan_aborts_on_any_concurrent_write() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("users"));
    let tbl = repo.get_table("users").await.unwrap();
    let table_token = table_token_for("users");

    let (mut tx2, _g2) = repo.begin_tx(IsolationLevel::Serializable).await.unwrap();

    let gate = repo.tx_gate().await.unwrap();
    let tx1_commit_v = gate.assign_next_version();
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
    gate.publish_committed_max(tx1_commit_v);

    tx2.predicate_set
        .push(PredicateDep::TableScan { table_token });

    tbl.insert_tx(&InnerValue::Str("dummy".into()), Some(&mut tx2))
        .await
        .unwrap();

    let res = repo.commit_tx(tx2).await;
    match res {
        Err(CommitError::PhantomConflict { dep }) => {
            assert!(
                dep.contains("TableScan"),
                "dep summary should name the variant: {dep}"
            );
        }
        other => panic!(
            "expected PhantomConflict for TableScan; got {:?}",
            other
                .map(|o| format!("Ok({:?})", o.commit_version))
                .unwrap_or_else(|e| format!("Err({e})"))
        ),
    }
}

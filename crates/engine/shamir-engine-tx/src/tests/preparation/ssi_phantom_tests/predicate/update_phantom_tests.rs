//! A5 — phantom via UPDATE that moves a row INTO the predicate range.

use shamir_collections::TFxMap;
use std::ops::Bound;

use shamir_tx::predicate_set::PredicateDep;
use shamir_tx::repo_tx_gate::{CommitWriteRecord, TableWriteFootprint};
use shamir_tx::IsolationLevel;
use shamir_types::types::value::InnerValue;

use crate::table::table_manager::table_token_for;
use crate::table::TableConfig;
use crate::tx::CommitError;

use super::super::helpers::{make_repo, test_bound_key, test_posting_key};

/// Tx1 arms a predicate `age >= 30`. A concurrent committer (tx_upd)
/// UPDATEs an existing row from age=4 → age=35, emitting a new sorted-index
/// posting key at age=35 that falls inside tx1's predicate interval. When
/// tx1 commits, Phase 2-bis must detect the phantom and abort with
/// `PhantomConflict`. The point-read SSI cannot catch this — the key
/// nobody read is the sorted-index posting, not the data record.
#[tokio::test]
async fn phantom_via_update_into_range_aborts() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("users"));
    let tbl = repo.get_table("users").await.unwrap();
    let table_token = table_token_for("users");

    let interner = tbl.interner().get().await.unwrap();
    let age_ind = interner.touch_ind("age").unwrap().into_key();
    let idx_id = age_ind.id();
    tbl.sorted_indexes()
        .register(
            crate::index::sorted_index_manager::SortedIndexDefinition::new(idx_id, vec![idx_id]),
        )
        .await
        .unwrap();

    // tx1 (Serializable): arm predicate `age >= 30`.
    let (mut tx1, _g1) = repo.begin_tx(IsolationLevel::Serializable).await.unwrap();

    let dep = PredicateDep::IndexRange {
        table_token,
        index_id: idx_id,
        lo: Bound::Included(test_bound_key(idx_id, 30)),
        hi: Bound::Unbounded,
    };
    tx1.predicate_set.push(dep);

    // Simulate concurrent committer that UPDATEs a row from age=4 → age=35.
    // The UPDATE emits a new sorted-index posting key at age=35 — inside
    // tx1's predicate. We plant this via the gate API.
    let gate = repo.tx_gate().await.unwrap();
    let upd_commit_v = gate.assign_next_version();
    assert!(
        upd_commit_v > tx1.snapshot_version,
        "update commit version must exceed tx1's snapshot"
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
        commit_version: upd_commit_v,
        per_table,
    });
    gate.publish_committed_max(upd_commit_v);

    // Stage a write so the tx is non-empty.
    tbl.insert_tx(&InnerValue::Str("dummy".into()), Some(&mut tx1))
        .await
        .unwrap();

    // Commit tx1: must abort with PhantomConflict (the update's posting
    // key for age=35 falls inside the `age >= 30` predicate).
    let res = repo.commit_tx(tx1).await;
    match res {
        Err(CommitError::PhantomConflict { dep }) => {
            assert!(
                dep.contains("IndexRange"),
                "dep summary should name IndexRange: {dep}"
            );
        }
        other => panic!(
            "tx1 must abort with PhantomConflict (update moved age=35 into its predicate); \
             got {:?}",
            other
                .map(|o| format!("Ok({:?})", o.commit_version))
                .unwrap_or_else(|e| format!("Err({e})"))
        ),
    }
}

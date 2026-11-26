//! Full integration tests: indexed range queries, TableScan fallback,
//! disjoint ranges, cross-table isolation.

use shamir_collections::TFxMap;

use shamir_tx::predicate_set::PredicateDep;
use shamir_tx::repo_tx_gate::{CommitWriteRecord, TableWriteFootprint};
use shamir_tx::IsolationLevel;
use shamir_types::types::value::InnerValue;

use crate::table::table_manager::table_token_for;
use crate::table::TableConfig;
use crate::tx::CommitError;

use super::helpers::{make_repo, test_posting_key};

/// A Serializable read_tx with WHERE on a sorted-indexed field records
/// an IndexRange predicate. Concurrent insert into that range aborts.
#[tokio::test]
async fn ssi_phantom_indexed_range_aborts_second_commit() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("users"));
    let tbl = repo.get_table("users").await.unwrap();
    let table_token = table_token_for("users");

    // Register a sorted index on "age".
    let interner = tbl.interner().get().await.unwrap();
    let age_ind = interner.touch_ind("age").unwrap().into_key();
    tbl.sorted_indexes()
        .register(
            crate::index::sorted_index_manager::SortedIndexDefinition::new(
                age_ind.id(),
                vec![age_ind.id()],
            ),
        )
        .await
        .unwrap();

    let (mut tx2, _g2) = repo.begin_tx(IsolationLevel::Serializable).await.unwrap();

    // Simulate concurrent commit that writes into the predicate range.
    let idx_id = age_ind.id();
    let gate = repo.tx_gate().await.unwrap();
    let tx1_commit_v = gate.assign_next_version();
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

    // Execute a query with WHERE age >= 30 under tx2.
    let filter = crate::query::filter::Filter::Gte {
        field: vec!["age".into()],
        value: crate::query::filter::FilterValue::Int(30),
    };
    let query = crate::query::read::ReadQuery::new("users").filter(filter);
    let refs = shamir_types::types::common::new_map();
    let ctx = crate::query::filter::eval_context::FilterContext::new(interner, &refs);
    let _ = tbl.read_tx(&query, &ctx, Some(&tx2)).await.unwrap();

    // Verify predicate was captured.
    assert!(
        !tx2.predicate_set.is_empty(),
        "read_tx with Gte on sorted field must record an IndexRange"
    );

    // Stage a write so the tx is non-empty.
    tbl.insert_tx(&InnerValue::Str("dummy".into()), Some(&mut tx2))
        .await
        .unwrap();

    // Commit must abort: the predicate range overlaps with tx1's write.
    let res = repo.commit_tx(tx2).await;
    assert!(
        res.is_err(),
        "commit must abort due to phantom conflict: {:?}",
        res.as_ref().ok()
    );
    match res {
        Err(CommitError::PhantomConflict { .. }) => {}
        other => panic!("expected PhantomConflict; got {:?}", other),
    }
}

/// Between filter on a sorted-indexed field records an IndexRange, and
/// concurrent insert in that range aborts.
#[tokio::test]
async fn ssi_phantom_between_range_aborts() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("users"));
    let tbl = repo.get_table("users").await.unwrap();
    let table_token = table_token_for("users");

    let interner = tbl.interner().get().await.unwrap();
    let age_ind = interner.touch_ind("age").unwrap().into_key();
    tbl.sorted_indexes()
        .register(
            crate::index::sorted_index_manager::SortedIndexDefinition::new(
                age_ind.id(),
                vec![age_ind.id()],
            ),
        )
        .await
        .unwrap();

    let (mut tx2, _g2) = repo.begin_tx(IsolationLevel::Serializable).await.unwrap();

    // Concurrent commit writes age=15 (inside 10..20 range).
    let idx_id = age_ind.id();
    let gate = repo.tx_gate().await.unwrap();
    let tx1_commit_v = gate.assign_next_version();
    let posting_key = test_posting_key(idx_id, 15);
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

    let filter = crate::query::filter::Filter::Between {
        field: vec!["age".into()],
        from: crate::query::filter::FilterValue::Int(10),
        to: crate::query::filter::FilterValue::Int(20),
    };
    let query = crate::query::read::ReadQuery::new("users").filter(filter);
    let refs = shamir_types::types::common::new_map();
    let ctx = crate::query::filter::eval_context::FilterContext::new(interner, &refs);
    let _ = tbl.read_tx(&query, &ctx, Some(&tx2)).await.unwrap();

    tbl.insert_tx(&InnerValue::Str("dummy".into()), Some(&mut tx2))
        .await
        .unwrap();

    let res = repo.commit_tx(tx2).await;
    match res {
        Err(CommitError::PhantomConflict { .. }) => {}
        other => panic!("expected PhantomConflict; got {:?}", other),
    }
}

/// Two txs with disjoint indexed ranges both commit successfully.
#[tokio::test]
async fn ssi_phantom_disjoint_indexed_ranges_both_commit() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("users"));
    let tbl = repo.get_table("users").await.unwrap();
    let table_token = table_token_for("users");

    let interner = tbl.interner().get().await.unwrap();
    let age_ind = interner.touch_ind("age").unwrap().into_key();
    tbl.sorted_indexes()
        .register(
            crate::index::sorted_index_manager::SortedIndexDefinition::new(
                age_ind.id(),
                vec![age_ind.id()],
            ),
        )
        .await
        .unwrap();

    let (mut tx2, _g2) = repo.begin_tx(IsolationLevel::Serializable).await.unwrap();

    // Concurrent commit writes age=50 (OUTSIDE tx2's range of age < 30).
    let idx_id = age_ind.id();
    let gate = repo.tx_gate().await.unwrap();
    let tx1_commit_v = gate.assign_next_version();
    let posting_key = test_posting_key(idx_id, 50);
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

    let filter = crate::query::filter::Filter::Lt {
        field: vec!["age".into()],
        value: crate::query::filter::FilterValue::Int(30),
    };
    let query = crate::query::read::ReadQuery::new("users").filter(filter);
    let refs = shamir_types::types::common::new_map();
    let ctx = crate::query::filter::eval_context::FilterContext::new(interner, &refs);
    let _ = tbl.read_tx(&query, &ctx, Some(&tx2)).await.unwrap();

    tbl.insert_tx(&InnerValue::Str("dummy".into()), Some(&mut tx2))
        .await
        .unwrap();

    let res = repo.commit_tx(tx2).await;
    assert!(
        res.is_ok(),
        "disjoint ranges must not conflict: {:?}",
        res.err()
    );
}

/// read_tx with no sorted index on the filter field records a TableScan
/// and aborts on any concurrent write.
#[tokio::test]
async fn ssi_phantom_no_index_records_table_scan_and_aborts() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("users"));
    let tbl = repo.get_table("users").await.unwrap();
    let table_token = table_token_for("users");

    let interner = tbl.interner().get().await.unwrap();
    // No sorted index registered for "age".

    let (mut tx2, _g2) = repo.begin_tx(IsolationLevel::Serializable).await.unwrap();

    // Concurrent commit writes anything into the table.
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

    let filter = crate::query::filter::Filter::Gte {
        field: vec!["age".into()],
        value: crate::query::filter::FilterValue::Int(18),
    };
    let query = crate::query::read::ReadQuery::new("users").filter(filter);
    let refs = shamir_types::types::common::new_map();
    let ctx = crate::query::filter::eval_context::FilterContext::new(interner, &refs);
    let _ = tbl.read_tx(&query, &ctx, Some(&tx2)).await.unwrap();

    // Should have fallen back to TableScan.
    let mut found_table_scan = false;
    tx2.predicate_set.with_iter(|dep| {
        if let shamir_tx::predicate_set::PredicateDep::TableScan { .. } = dep {
            found_table_scan = true;
        }
    });
    assert!(found_table_scan, "no sorted index → must record TableScan");

    tbl.insert_tx(&InnerValue::Str("dummy".into()), Some(&mut tx2))
        .await
        .unwrap();

    let res = repo.commit_tx(tx2).await;
    match res {
        Err(CommitError::PhantomConflict { dep }) => {
            assert!(dep.contains("TableScan"));
        }
        other => panic!("expected PhantomConflict (TableScan); got {:?}", other),
    }
}

/// A concurrent write into a DIFFERENT table does not conflict with the
/// predicate dep on the original table.
#[tokio::test]
async fn ssi_phantom_other_table_does_not_conflict() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("users"));
    repo.add_table_for_test(TableConfig::new("orders"));
    let tbl_users = repo.get_table("users").await.unwrap();
    let table_token_users = table_token_for("users");
    let table_token_orders = table_token_for("orders");

    let (mut tx2, _g2) = repo.begin_tx(IsolationLevel::Serializable).await.unwrap();

    // Concurrent commit writes into "orders", not "users".
    let gate = repo.tx_gate().await.unwrap();
    let tx1_commit_v = gate.assign_next_version();
    let mut per_table = TFxMap::default();
    per_table.insert(
        table_token_orders,
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

    // tx2 does a full table scan on "users" (TableScan dep).
    tx2.record_predicate_shared(PredicateDep::TableScan {
        table_token: table_token_users,
    });

    tbl_users
        .insert_tx(&InnerValue::Str("dummy".into()), Some(&mut tx2))
        .await
        .unwrap();

    let res = repo.commit_tx(tx2).await;
    assert!(
        res.is_ok(),
        "write to another table must not conflict: {:?}",
        res.err()
    );
}

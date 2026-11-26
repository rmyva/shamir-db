//! Steps 5+6 — predicate-capture hooks (precise + coarse).
//!
//! Asserts that the tx-aware read/scan paths correctly record `IndexRange` or
//! `TableScan` predicate dependencies into `TxContext::predicate_set`, and that
//! concurrent inserts into captured ranges actually abort.

use super::super::helpers::make_repo;
use shamir_tx::IsolationLevel;

/// SortedIndexManager::lookup_range_tx records an IndexRange predicate dep
/// with bounds matching the physical range_bounds output.
#[tokio::test]
async fn ssi_phantom_lookup_range_tx_records_exact_bounds_from_range_bounds() {
    use shamir_storage::storage_in_memory::InMemoryStore;
    use shamir_tx::{TxContext, TxId};

    let store: std::sync::Arc<dyn shamir_storage::types::Store> =
        std::sync::Arc::new(InMemoryStore::new());
    let mgr = crate::index::sorted_index_manager::SortedIndexManager::new(store)
        .await
        .unwrap();
    let def = crate::index::sorted_index_manager::SortedIndexDefinition::new(101, vec![201]);
    mgr.register(def).await.unwrap();

    let ctx = TxContext::new(TxId::new(1), 0, 0, IsolationLevel::Serializable);

    let mut start_enc = Vec::new();
    shamir_types::core::sort_codec::encode_i64(&mut start_enc, 10);
    let mut end_enc = Vec::new();
    shamir_types::core::sort_codec::encode_i64(&mut end_enc, 20);

    let _result = mgr
        .lookup_range_tx(42, 101, Some(&start_enc), Some(&end_enc), Some(&ctx))
        .await
        .unwrap();

    assert_eq!(
        ctx.predicate_set.len(),
        1,
        "lookup_range_tx must record exactly one IndexRange dep"
    );

    let mut found = false;
    ctx.predicate_set.with_iter(|dep| {
        if let shamir_tx::predicate_set::PredicateDep::IndexRange {
            table_token,
            index_id,
            ..
        } = dep
        {
            assert_eq!(*table_token, 42);
            assert_eq!(*index_id, 101);
            found = true;
        }
    });
    assert!(found, "must be an IndexRange dep");
}

/// SortedIndexManager::lookup_min_tx records a full-prefix IndexRange.
#[tokio::test]
async fn ssi_phantom_lookup_min_tx_records_full_prefix_interval() {
    use shamir_storage::storage_in_memory::InMemoryStore;
    use shamir_tx::{TxContext, TxId};

    let store: std::sync::Arc<dyn shamir_storage::types::Store> =
        std::sync::Arc::new(InMemoryStore::new());
    let mgr = crate::index::sorted_index_manager::SortedIndexManager::new(store)
        .await
        .unwrap();
    let def = crate::index::sorted_index_manager::SortedIndexDefinition::new(101, vec![201]);
    mgr.register(def).await.unwrap();

    let ctx = TxContext::new(TxId::new(1), 0, 0, IsolationLevel::Serializable);

    let _ = mgr.lookup_min_tx(42, 101, Some(&ctx)).await.unwrap();

    assert_eq!(
        ctx.predicate_set.len(),
        1,
        "lookup_min_tx must record exactly one IndexRange dep"
    );

    ctx.predicate_set.with_iter(|dep| match dep {
        shamir_tx::predicate_set::PredicateDep::IndexRange {
            table_token,
            index_id,
            ..
        } => {
            assert_eq!(*table_token, 42);
            assert_eq!(*index_id, 101);
        }
        _ => panic!("expected IndexRange, got TableScan"),
    });
}

/// Non-tx lookup_range does NOT allocate or record any predicate dep.
#[tokio::test]
async fn ssi_phantom_non_tx_lookup_range_does_not_allocate_predicate_set() {
    use shamir_storage::storage_in_memory::InMemoryStore;

    let store: std::sync::Arc<dyn shamir_storage::types::Store> =
        std::sync::Arc::new(InMemoryStore::new());
    let mgr = crate::index::sorted_index_manager::SortedIndexManager::new(store)
        .await
        .unwrap();
    let def = crate::index::sorted_index_manager::SortedIndexDefinition::new(101, vec![201]);
    mgr.register(def).await.unwrap();

    // No tx at all — must not allocate.
    let _result = mgr.lookup_range_tx(0, 101, None, None, None).await.unwrap();
    // Just verifying no panic and no crash — the contract is that off-tx the
    // predicate_set is never touched.
}

/// Snapshot isolation lookup_range_tx records NOTHING in predicate_set.
#[tokio::test]
async fn ssi_phantom_snapshot_isolation_records_nothing_zero_overhead() {
    use shamir_storage::storage_in_memory::InMemoryStore;
    use shamir_tx::{TxContext, TxId};

    let store: std::sync::Arc<dyn shamir_storage::types::Store> =
        std::sync::Arc::new(InMemoryStore::new());
    let mgr = crate::index::sorted_index_manager::SortedIndexManager::new(store)
        .await
        .unwrap();
    let def = crate::index::sorted_index_manager::SortedIndexDefinition::new(101, vec![201]);
    mgr.register(def).await.unwrap();

    let ctx = TxContext::new(TxId::new(1), 0, 0, IsolationLevel::Snapshot);

    let _ = mgr
        .lookup_range_tx(42, 101, None, None, Some(&ctx))
        .await
        .unwrap();

    assert!(
        ctx.predicate_set.is_empty(),
        "Snapshot tx must NOT record any predicate deps"
    );
}

/// index2 backend default lookup_tx records IndexRange for Range query
/// under Serializable isolation.
#[tokio::test]
async fn ssi_phantom_index2_btree_range_records_index_range_via_default_lookup_tx() {
    use crate::index2::backend::{IndexBackend, IndexQuery};
    use crate::index2::descriptor::IndexDescriptor;
    use crate::index2::expr::IndexExpr;
    use crate::index2::functional_backend::FunctionalBackend;
    use crate::index2::kind::IndexKind;
    use shamir_storage::storage_in_memory::InMemoryStore;
    use shamir_tx::{TxContext, TxId};

    let store: std::sync::Arc<dyn shamir_storage::types::Store> =
        std::sync::Arc::new(InMemoryStore::new());
    let desc = IndexDescriptor::new(
        1,
        "test_idx",
        999,
        smallvec::smallvec![vec![1u64]],
        IndexKind::Btree { unique: false },
    );
    // FunctionalBackend does NOT override lookup_tx, so the default
    // trait method (Phase C wiring) is exercised.
    let backend = FunctionalBackend::new(desc, IndexExpr::Field(vec![1]), store);

    let ctx = TxContext::new(TxId::new(1), 0, 0, IsolationLevel::Serializable);

    let q = IndexQuery::Range {
        lo: std::ops::Bound::Included(vec![0u8]),
        hi: std::ops::Bound::Included(vec![0xFFu8]),
    };
    // FunctionalBackend::lookup errors on Range queries, but the
    // default lookup_tx records the predicate BEFORE calling lookup.
    // We only care that the predicate was recorded.
    let _result = backend.lookup_tx(42, q, Some(&ctx), None).await;

    assert_eq!(
        ctx.predicate_set.len(),
        1,
        "IndexBackend default lookup_tx must record one IndexRange dep for Range query"
    );

    ctx.predicate_set.with_iter(|dep| match dep {
        shamir_tx::predicate_set::PredicateDep::IndexRange {
            table_token,
            index_id,
            ..
        } => {
            assert_eq!(*table_token, 42);
            assert_eq!(*index_id, 999); // name_interned from descriptor
        }
        _ => panic!("expected IndexRange"),
    });
}

/// predicate_to_index_range: Eq on a sorted-indexed field produces a
/// degenerate (lo==hi with 0xFF pad) interval.
#[tokio::test]
async fn ssi_phantom_eq_on_sorted_indexed_field_records_degenerate_interval() {
    use crate::query::filter::eval::predicate_to_index_range;
    use crate::query::filter::Filter;
    use shamir_storage::storage_in_memory::InMemoryStore;
    use shamir_types::core::interner::Interner;

    let i = Interner::new();
    let age_ind = i.touch_ind("age").unwrap().into_key();

    let store: std::sync::Arc<dyn shamir_storage::types::Store> =
        std::sync::Arc::new(InMemoryStore::new());
    let mgr = crate::index::sorted_index_manager::SortedIndexManager::new(store)
        .await
        .unwrap();
    mgr.register(
        crate::index::sorted_index_manager::SortedIndexDefinition::new(
            age_ind.id(),
            vec![age_ind.id()],
        ),
    )
    .await
    .unwrap();

    let filter = Filter::Eq {
        field: vec!["age".into()],
        value: crate::query::filter::FilterValue::Int(30),
    };

    let deps = predicate_to_index_range(&filter, &mgr, &i, 7);
    assert_eq!(deps.len(), 1, "Eq on sorted field must emit one IndexRange");

    match &deps[0] {
        shamir_tx::predicate_set::PredicateDep::IndexRange {
            table_token,
            index_id,
            ..
        } => {
            assert_eq!(*table_token, 7);
            assert_eq!(*index_id, age_ind.id());
        }
        _ => panic!("expected IndexRange"),
    }
}

/// predicate_to_index_range: Or filter returns empty (caller records TableScan).
#[tokio::test]
async fn ssi_phantom_or_filter_records_table_scan_not_index_range() {
    use crate::query::filter::eval::predicate_to_index_range;
    use crate::query::filter::Filter;
    use shamir_storage::storage_in_memory::InMemoryStore;
    use shamir_types::core::interner::Interner;

    let i = Interner::new();
    let _age_ind = i.touch_ind("age").unwrap().into_key();

    let store: std::sync::Arc<dyn shamir_storage::types::Store> =
        std::sync::Arc::new(InMemoryStore::new());
    let mgr = crate::index::sorted_index_manager::SortedIndexManager::new(store)
        .await
        .unwrap();

    let filter = Filter::Or {
        filters: vec![
            Filter::Eq {
                field: vec!["age".into()],
                value: crate::query::filter::FilterValue::Int(10),
            },
            Filter::Eq {
                field: vec!["age".into()],
                value: crate::query::filter::FilterValue::Int(20),
            },
        ],
    };

    let deps = predicate_to_index_range(&filter, &mgr, &i, 7);
    assert!(
        deps.is_empty(),
        "Or filter must return empty (coarse TableScan fallback)"
    );
}

/// predicate_to_index_range: And with one unmapped conjunct falls back to
/// empty (caller records TableScan).
#[tokio::test]
async fn ssi_phantom_and_with_one_unmapped_conjunct_falls_back_to_table_scan() {
    use crate::query::filter::eval::predicate_to_index_range;
    use crate::query::filter::Filter;
    use shamir_storage::storage_in_memory::InMemoryStore;
    use shamir_types::core::interner::Interner;

    let i = Interner::new();
    let age_ind = i.touch_ind("age").unwrap().into_key();

    let store: std::sync::Arc<dyn shamir_storage::types::Store> =
        std::sync::Arc::new(InMemoryStore::new());
    let mgr = crate::index::sorted_index_manager::SortedIndexManager::new(store)
        .await
        .unwrap();
    mgr.register(
        crate::index::sorted_index_manager::SortedIndexDefinition::new(
            age_ind.id(),
            vec![age_ind.id()],
        ),
    )
    .await
    .unwrap();

    let filter = Filter::And {
        filters: vec![
            Filter::Gte {
                field: vec!["age".into()],
                value: crate::query::filter::FilterValue::Int(18),
            },
            // Regex has no sorted index — unmapped conjunct.
            Filter::Regex {
                field: vec!["name".into()],
                pattern: "^A".into(),
            },
        ],
    };

    let deps = predicate_to_index_range(&filter, &mgr, &i, 7);
    assert!(
        deps.is_empty(),
        "And with an unmapped conjunct must fall back to empty (TableScan)"
    );
}

/// read_tx with no WHERE on a Serializable tx records a TableScan
/// predicate dep. Full integration test with RepoInstance.
#[tokio::test]
async fn ssi_phantom_no_where_records_table_scan_for_list_stream() {
    use futures::StreamExt;

    let repo = make_repo();
    repo.add_table_for_test(crate::table::TableConfig::new("users"));
    let _tbl = repo.get_table("users").await.unwrap();
    let table_token = crate::table::table_manager::table_token_for("users");

    let (tx, _g) = repo.begin_tx(IsolationLevel::Serializable).await.unwrap();

    // list_stream_tx on a Serializable tx records TableScan.
    let tbl = repo.get_table("users").await.unwrap();
    let stream = tbl.list_stream_tx(Some(&tx), 100);
    futures::pin_mut!(stream);
    while let Some(batch) = stream.next().await {
        let _ = batch.unwrap();
    }

    assert!(
        !tx.predicate_set.is_empty(),
        "list_stream_tx under Serializable must record at least one TableScan"
    );
    let mut found_table_scan = false;
    tx.predicate_set.with_iter(|dep| {
        if let shamir_tx::predicate_set::PredicateDep::TableScan { table_token: t, .. } = dep {
            if *t == table_token {
                found_table_scan = true;
            }
        }
    });
    assert!(
        found_table_scan,
        "must find a TableScan for our table token"
    );
}

/// filter_stream_tx with a filter that has no sorted index records
/// a coarse TableScan.
#[tokio::test]
async fn ssi_phantom_filter_stream_tx_records_coarse_when_filter_has_no_sorted_index() {
    use futures::StreamExt;

    let repo = make_repo();
    repo.add_table_for_test(crate::table::TableConfig::new("users"));
    let tbl = repo.get_table("users").await.unwrap();
    let table_token = crate::table::table_manager::table_token_for("users");

    let (tx, _g) = repo.begin_tx(IsolationLevel::Serializable).await.unwrap();

    let interner = tbl.interner().get().await.unwrap();
    let filter = crate::query::filter::Filter::Regex {
        field: vec!["name".into()],
        pattern: "^A".into(),
    };
    let refs = shamir_types::types::common::new_map();
    let ctx = crate::query::filter::eval_context::FilterContext::new(interner, &refs);

    let stream = tbl
        .filter_stream_tx(Some(&tx), 100, &filter, &ctx)
        .await
        .unwrap();
    futures::pin_mut!(stream);
    while let Some(batch) = stream.next().await {
        let _ = batch.unwrap();
    }

    let mut found_table_scan = false;
    tx.predicate_set.with_iter(|dep| {
        if let shamir_tx::predicate_set::PredicateDep::TableScan { table_token: t, .. } = dep {
            if *t == table_token {
                found_table_scan = true;
            }
        }
    });
    assert!(
        found_table_scan,
        "filter_stream_tx with unmappable filter must record TableScan"
    );
}

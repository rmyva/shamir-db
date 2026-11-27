use std::ops::Bound;
use std::sync::Arc;

use shamir_index::IndexWriteOp;
use shamir_storage::storage_in_memory::InMemoryStore;
use shamir_storage::types::Store;
use shamir_tx::predicate_set::{key_in_interval, PredicateDep};
use shamir_types::core::interner::{Interner, InternerKey, TouchInd};
use shamir_types::core::sort_codec;
use shamir_types::types::common::new_map_wc;
use shamir_types::types::record_id::RecordId;
use shamir_types::types::value::InnerValue;

use crate::index::sorted_index_manager::{SortedIndexDefinition, SortedIndexManager};
use crate::query::filter::{Filter, FilterValue};
use crate::tx::predicate_range::{predicate_to_index_deps, predicate_to_index_range};

async fn fixture() -> (SortedIndexManager, Arc<Interner>, u64) {
    let info_store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let mgr = SortedIndexManager::new(Arc::clone(&info_store))
        .await
        .unwrap();
    let interner = Arc::new(Interner::new());
    let age_id = match interner.touch_ind("age").unwrap() {
        TouchInd::Exists(k) | TouchInd::New(k) => k.id(),
    };
    let idx_name_id = match interner.touch_ind("by_age").unwrap() {
        TouchInd::Exists(k) | TouchInd::New(k) => k.id(),
    };
    mgr.register(SortedIndexDefinition::new(idx_name_id, vec![age_id]))
        .await
        .unwrap();
    (mgr, interner, idx_name_id)
}

#[tokio::test]
async fn maps_gt_to_excluded_lo() {
    let (mgr, interner, idx_id) = fixture().await;
    let f = Filter::Gt {
        field: vec!["age".into()],
        value: FilterValue::Int(30),
    };
    let dep = predicate_to_index_range(&f, &mgr, &interner, 42).unwrap();
    match dep {
        PredicateDep::IndexRange {
            table_token,
            index_id,
            lo,
            hi,
        } => {
            assert_eq!(table_token, 42);
            assert_eq!(index_id, idx_id);
            assert!(matches!(lo, Bound::Excluded(_)));
            assert!(matches!(hi, Bound::Included(_)));
        }
        _ => panic!("expected IndexRange"),
    }
}

#[tokio::test]
async fn maps_gte_to_included_lo() {
    let (mgr, interner, _) = fixture().await;
    let f = Filter::Gte {
        field: vec!["age".into()],
        value: FilterValue::Int(30),
    };
    let dep = predicate_to_index_range(&f, &mgr, &interner, 1).unwrap();
    assert!(matches!(
        dep,
        PredicateDep::IndexRange {
            lo: Bound::Included(_),
            ..
        }
    ));
}

#[tokio::test]
async fn maps_lt_lte_between_eq() {
    let (mgr, interner, _) = fixture().await;
    for f in [
        Filter::Lt {
            field: vec!["age".into()],
            value: FilterValue::Int(30),
        },
        Filter::Lte {
            field: vec!["age".into()],
            value: FilterValue::Int(30),
        },
        Filter::Between {
            field: vec!["age".into()],
            from: FilterValue::Int(10),
            to: FilterValue::Int(20),
        },
        Filter::Eq {
            field: vec!["age".into()],
            value: FilterValue::Int(30),
        },
    ] {
        assert!(
            predicate_to_index_range(&f, &mgr, &interner, 1).is_some(),
            "{:?}",
            f
        );
    }
}

#[tokio::test]
async fn returns_none_for_unsupported_ops() {
    let (mgr, interner, _) = fixture().await;
    for f in [
        Filter::Ne {
            field: vec!["age".into()],
            value: FilterValue::Int(0),
        },
        Filter::Like {
            field: vec!["age".into()],
            pattern: "x%".into(),
        },
        Filter::ILike {
            field: vec!["age".into()],
            pattern: "x%".into(),
        },
        Filter::Regex {
            field: vec!["age".into()],
            pattern: "^x".into(),
        },
        Filter::Computed {
            expr_op: "lower".into(),
            field: vec!["age".into()],
            expr_args: None,
            cmp: "eq".into(),
            value: FilterValue::Int(0),
        },
        Filter::Fts {
            field: vec!["age".into()],
            query: "x".into(),
            mode: "and".into(),
            limit: None,
        },
        Filter::IsNull {
            field: vec!["age".into()],
        },
        Filter::In {
            field: vec!["age".into()],
            values: vec![FilterValue::Int(1)],
        },
        Filter::Or { filters: vec![] },
        Filter::Not {
            filter: Box::new(Filter::Eq {
                field: vec!["age".into()],
                value: FilterValue::Int(0),
            }),
        },
    ] {
        assert!(
            predicate_to_index_range(&f, &mgr, &interner, 1).is_none(),
            "should be coarse: {:?}",
            f
        );
    }
}

#[tokio::test]
async fn returns_none_when_no_index_on_field() {
    let (mgr, interner, _) = fixture().await;
    let f = Filter::Gt {
        field: vec!["weight".into()],
        value: FilterValue::Int(30),
    };
    assert!(predicate_to_index_range(&f, &mgr, &interner, 1).is_none());
}

#[tokio::test]
async fn returns_none_for_unencodable_value() {
    let (mgr, interner, _) = fixture().await;
    let f = Filter::Eq {
        field: vec!["age".into()],
        value: FilterValue::field_ref("other"),
    };
    assert!(predicate_to_index_range(&f, &mgr, &interner, 1).is_none());
    let nan = Filter::Gt {
        field: vec!["age".into()],
        value: FilterValue::Float(f64::NAN),
    };
    assert!(predicate_to_index_range(&nan, &mgr, &interner, 1).is_none());
}

#[tokio::test]
async fn and_emits_one_dep_per_scalar_conjunct() {
    let (mgr, interner, _) = fixture().await;
    let f = Filter::And {
        filters: vec![
            Filter::Gt {
                field: vec!["age".into()],
                value: FilterValue::Int(10),
            },
            Filter::Lt {
                field: vec!["age".into()],
                value: FilterValue::Int(20),
            },
        ],
    };
    let (deps, precise) = predicate_to_index_deps(&f, &mgr, &interner, 1);
    assert_eq!(deps.len(), 2);
    assert!(precise);
}

#[tokio::test]
async fn and_with_one_coarse_conjunct_marks_overall_coarse() {
    let (mgr, interner, _) = fixture().await;
    let f = Filter::And {
        filters: vec![
            Filter::Gt {
                field: vec!["age".into()],
                value: FilterValue::Int(10),
            },
            Filter::Regex {
                field: vec!["age".into()],
                pattern: "x".into(),
            },
        ],
    };
    let (deps, precise) = predicate_to_index_deps(&f, &mgr, &interner, 1);
    assert_eq!(deps.len(), 1);
    assert!(!precise);
}

#[tokio::test]
async fn gt_dep_round_trips_through_key_in_interval() {
    let (mgr, interner, idx_id) = fixture().await;
    let dep = predicate_to_index_range(
        &Filter::Gt {
            field: vec!["age".into()],
            value: FilterValue::Int(30),
        },
        &mgr,
        &interner,
        42,
    )
    .unwrap();
    let (lo, hi) = match &dep {
        PredicateDep::IndexRange { lo, hi, .. } => (lo, hi),
        _ => panic!("expected IndexRange"),
    };
    let posting = |v: i64, rid: u8| {
        let mut encoded = Vec::new();
        sort_codec::encode_i64(&mut encoded, v);
        let rid = [rid; 16];
        shamir_types::core::sorted_index_layout::sorted_posting_key(idx_id, &encoded, &rid)
    };
    assert!(key_in_interval(&posting(31, 0), idx_id, lo, hi));
    for rid in [0u8, 0x7F, 0xFF] {
        assert!(
            !key_in_interval(&posting(30, rid), idx_id, lo, hi),
            "rid {rid:#x}"
        );
    }
    assert!(!key_in_interval(&posting(29, 0xFF), idx_id, lo, hi));
}

#[tokio::test]
async fn rv_predicate_range_52fb938f_manager_posting_matches_shared_layout_and_tx_consumer() {
    let (mgr, interner, idx_id) = fixture().await;
    let age_id = match interner.touch_ind("age").unwrap() {
        TouchInd::Exists(k) | TouchInd::New(k) => k.id(),
    };
    let rid = RecordId::from_ts_seq(1_800_000_000_000_000, 7);
    let mut map = new_map_wc(1);
    map.insert(InternerKey::new(age_id), InnerValue::Int(20));
    let record = InnerValue::Map(map);
    let produced = mgr
        .plan_record_created(&rid, &record, 0)
        .unwrap()
        .into_iter()
        .find_map(|op| match op {
            IndexWriteOp::SetPosting { key, .. } => Some(key),
            _ => None,
        })
        .expect("sorted index planner must produce a posting");
    let dep = predicate_to_index_range(
        &Filter::Between {
            field: vec!["age".into()],
            from: FilterValue::Int(10),
            to: FilterValue::Int(20),
        },
        &mgr,
        &interner,
        42,
    )
    .unwrap();
    let (lo, hi) = match &dep {
        PredicateDep::IndexRange { lo, hi, .. } => (lo, hi),
        _ => panic!("expected IndexRange"),
    };
    assert!(key_in_interval(&produced, idx_id, lo, hi));
    let mut encoded = Vec::new();
    sort_codec::encode_i64(&mut encoded, 20);
    let expected = shamir_types::core::sorted_index_layout::sorted_posting_key(
        idx_id,
        &encoded,
        rid.as_bytes(),
    );
    assert_eq!(produced.as_ref(), expected.as_slice());
}

#[tokio::test]
async fn between_dep_round_trips_through_key_in_interval() {
    let (mgr, interner, idx_id) = fixture().await;
    let dep = predicate_to_index_range(
        &Filter::Between {
            field: vec!["age".into()],
            from: FilterValue::Int(10),
            to: FilterValue::Int(20),
        },
        &mgr,
        &interner,
        42,
    )
    .unwrap();
    let (lo, hi) = match &dep {
        PredicateDep::IndexRange { lo, hi, .. } => (lo, hi),
        _ => panic!("expected IndexRange"),
    };
    let posting = |v: i64, rid: u8| {
        let mut encoded = Vec::new();
        sort_codec::encode_i64(&mut encoded, v);
        let rid = [rid; 16];
        shamir_types::core::sorted_index_layout::sorted_posting_key(idx_id, &encoded, &rid)
    };
    for v in [10, 15, 20] {
        assert!(
            key_in_interval(&posting(v, 0x42), idx_id, lo, hi),
            "{v} in [10,20]"
        );
    }
    for v in [9, 21] {
        assert!(
            !key_in_interval(&posting(v, 0x42), idx_id, lo, hi),
            "{v} NOT in [10,20]"
        );
    }
}

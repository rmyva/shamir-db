//! `BatchOp` dispatch invariants (RV-QUERY-TYPES-77BFC767): ambiguous
//! discriminator sets are decode errors; `update` + `set` stays legal
//! because `set` is a payload field of `UpdateOp`. Non-discriminator
//! payload keys (`where`, `count_total`, `request_id`, …) are the typed
//! op's business and are NOT rejected at the `BatchOp` layer.

use shamir_types::mpack;
use shamir_types::types::value::QueryValue;

use crate::batch::batch_op::BatchOp;
use crate::subscribe::DeliverMode;

// Wire-format regression tests use raw input because a builder would bypass the decoding boundary.
#[test]
fn large_typed_unsigned_values_survive_batch_dispatch() {
    for version in [i64::MAX as u64 + 1, u64::MAX] {
        for input in [
            format!(
                r#"{{
                "update": "users",
                "set": {{}},
                "expected_version": {version}
            }}"#
            ),
            format!(
                r#"{{
                "delete_from": "users",
                "where": {{
                    "op": "eq",
                    "field": ["name"],
                    "value": "x"
                }},
                "expected_version": {version}
            }}"#
            ),
        ] {
            let wire: serde_json::Value = serde_json::from_str(&input).unwrap();
            let bytes = rmp_serde::to_vec_named(&wire).unwrap();
            let op: BatchOp = rmp_serde::from_slice(&bytes).unwrap();
            let actual = match op {
                BatchOp::Update(op) => op.expected_version,
                BatchOp::Delete(op) => op.expected_version,
                other => panic!("unexpected operation: {other:?}"),
            };
            assert_eq!(actual, Some(version));
        }
    }
}

#[test]
fn nested_batch_preserves_large_work_limit() {
    let wire: serde_json::Value = serde_json::from_str(
        r#"{
        "batch": {
            "id": null,
            "queries": {},
            "limits": {
                "max_total_work": 18446744073709551615
            }
        }
    }"#,
    )
    .unwrap();
    let bytes = rmp_serde::to_vec_named(&wire).unwrap();
    let op: BatchOp = rmp_serde::from_slice(&bytes).unwrap();
    match op {
        BatchOp::Batch(op) => assert_eq!(op.batch.limits.max_total_work, u64::MAX),
        other => panic!("unexpected operation: {other:?}"),
    }
}

#[test]
fn duplicate_discriminator_last_value_wins() {
    let input = r#"{
        "from": "first",
        "from": "last"
    }"#;
    match serde_json::from_str::<BatchOp>(input).unwrap() {
        BatchOp::Read(op) => assert_eq!(op.from.table, "last"),
        other => panic!("unexpected operation: {other:?}"),
    }
}

#[test]
fn missing_op_kind_is_rejected() {
    let error = serde_json::from_str::<BatchOp>("{}").unwrap_err();
    assert!(error.to_string().contains("Unknown operation type"));
}

/// Nested operations are buffered into `QueryValue::Map`, whose repeated keys
/// collapse by insertion; preserve the established last-value-wins behavior.
#[test]
fn nested_duplicate_discriminator_last_value_wins() {
    let input = r#"{
        "batch": {
            "id": null,
            "queries": {
                "nested_read": {
                    "from": "first",
                    "from": "last"
                }
            }
        }
    }"#;
    match serde_json::from_str::<BatchOp>(input).unwrap() {
        BatchOp::Batch(op) => match &op.batch.queries["nested_read"].op {
            BatchOp::Read(read) => assert_eq!(read.from.table, "last"),
            other => panic!("unexpected nested operation: {other:?}"),
        },
        other => panic!("unexpected operation: {other:?}"),
    }
}

/// Nested repeated cancel discriminators collapse the same way before the typed wire struct.
#[test]
fn nested_duplicate_cancel_discriminator_last_value_wins() {
    let input = r#"{
        "batch": {
            "id": null,
            "queries": {
                "nested_cancel": {
                    "cancel_tx": 7,
                    "cancel_tx": 8
                }
            }
        }
    }"#;
    match serde_json::from_str::<BatchOp>(input).unwrap() {
        BatchOp::Batch(op) => match &op.batch.queries["nested_cancel"].op {
            BatchOp::CancelTx(handle) => assert_eq!(*handle, 8),
            other => panic!("unexpected nested operation: {other:?}"),
        },
        other => panic!("unexpected operation: {other:?}"),
    }
}

#[test]
fn ambiguous_discriminator_error_is_preserved() {
    let op = mpack!({
        "from": "users",
        "insert_into": "orders"
    });
    assert!(decode(&op)
        .unwrap_err()
        .to_string()
        .contains("ambiguous BatchOp discriminators"));
}

fn decode(qv: &QueryValue) -> Result<BatchOp, Box<dyn std::error::Error>> {
    let bytes = rmp_serde::to_vec_named(qv)?;
    rmp_serde::from_slice(&bytes).map_err(Into::into)
}

#[test]
fn unknown_operation_fields_name_the_typo() {
    for (json, key) in [
        (
            r#"{
                "from": "t",
                "wehre": {
                    "op": "is_null",
                    "field": ["x"]
                }
            }"#,
            "wehre",
        ),
        (
            r#"{
                "update": "t",
                "set": {},
                "expected_verison": 1
            }"#,
            "expected_verison",
        ),
        (
            r#"{
                "insert_into": "t",
                "values": [],
                "return_results": true
            }"#,
            "return_results",
        ),
        (
            r#"{
                "from": "t",
                "pagination": {
                    "mode": "LimitOffset",
                    "limt": 1
                }
            }"#,
            "limt",
        ),
        (
            r#"{
                "create_table": "t",
                "if_not_exist": true
            }"#,
            "if_not_exist",
        ),
        (
            r#"{
                "create_user": "u",
                "password": "p",
                "rolez": []
            }"#,
            "rolez",
        ),
        (
            r#"{
                "subscribe": [],
                "intial": true
            }"#,
            "intial",
        ),
        (
            r#"{
                "call": "f",
                "paramz": []
            }"#,
            "paramz",
        ),
        (
            r#"{
                "batch": {
                    "id": 1,
                    "queries": {}
                },
                "stray": true
            }"#,
            "stray",
        ),
    ] {
        let value: QueryValue = serde_json::from_str(json).unwrap();
        let error = decode(&value).unwrap_err().to_string();
        assert!(error.contains(key), "{error}");
    }
}

#[test]
fn table_ref_map_is_validated() {
    let default_repo: crate::TableRef = serde_json::from_str(
        r#"{
            "table": "t"
        }"#,
    )
    .unwrap();
    assert_eq!(default_repo, crate::TableRef::new("t"));
    let qualified: crate::TableRef = serde_json::from_str(
        r#"{
            "repo": "other",
            "table": "t"
        }"#,
    )
    .unwrap();
    assert_eq!(qualified, crate::TableRef::with_repo("other", "t"));
    let value: QueryValue = serde_json::from_str(
        r#"{
            "from": {
                "repo": "other",
                "table": "t",
                "tabble": true
            }
        }"#,
    )
    .unwrap();
    assert!(decode(&value).unwrap_err().to_string().contains("tabble"));
}

#[test]
fn user_data_and_entry_metadata_remain_open() {
    let request = r#"{
        "id": 1,
        "queries": {
            "arbitrary_alias": {
                "update": "t",
                "set": {
                    "arbitrary_field": 1
                },
                "return_result": false,
                "after": ["other"],
                "when": {
                    "op": "is_null",
                    "field": ["x"]
                }
            },
            "other": {
                "insert_into": "t",
                "values": [
                    {
                        "arbitrary_field": 2
                    }
                ]
            },
            "nested": {
                "batch": {
                    "id": 2,
                    "queries": {}
                },
                "bind": {
                    "arbitrary_binding": 3
                }
            }
        },
        "limits": {
            "max_queries": 10
        }
    }"#;
    let decoded: crate::batch::BatchRequest = serde_json::from_str(request).unwrap();
    assert_eq!(decoded.queries.len(), 3);
    assert_eq!(decoded.limits.max_queries, 10);
    assert_eq!(
        decoded.limits.max_iterations,
        crate::batch::BatchLimits::default().max_iterations
    );
    let entry = &decoded.queries["arbitrary_alias"];
    assert!(!entry.return_result);
    assert_eq!(entry.after, ["other"]);
    assert!(entry.when.is_some());
}

/// `{"from": ..., "insert_into": ...}` used to decode as `Read` (first arm
/// wins) and silently drop the `insert_into` payload — now an error.
#[test]
fn rv_query_types_77bfc767_ambiguous_discriminators_rejected() {
    let qv = mpack!({ "from": "users", "insert_into": "orders" });
    assert!(decode(&qv).is_err(), "from + insert_into must be rejected");
}

#[test]
fn rv_query_types_77bfc767_update_with_set_payload_still_accepted() {
    // Real `UpdateOp` shape: `update` (TableRef), `set` (QueryValue map),
    // `where` (Filter, tagged `{"op": "eq", "field": [...], "value": ...}`).
    let qv = mpack!({
        "update": "users",
        "set": @ QueryValue::Map({
            let mut m = shamir_types::types::common::TMap::default();
            m.insert("name".to_string(), QueryValue::Str("x".to_string()));
            m
        }),
        "where": @ QueryValue::Map({
            let mut m = shamir_types::types::common::TMap::default();
            m.insert(
                "op".to_string(),
                QueryValue::Str("eq".to_string()),
            );
            m.insert(
                "field".to_string(),
                QueryValue::List(vec![QueryValue::Str("name".to_string())]),
            );
            m.insert("value".to_string(), QueryValue::Str("x".to_string()));
            m
        })
    });
    match decode(&qv).expect("update + set is a legal UpdateOp") {
        BatchOp::Update(update) => assert_eq!(update.update.table, "users"),
        other => panic!("expected Update, got {other:?}"),
    }
}

#[test]
fn rv_query_types_77bfc767_set_alone_still_decodes_as_set() {
    // Real `SetOp` shape: `set` (TableRef), `key` + `value` (QueryValue).
    let qv = mpack!({
        "set": "k",
        "key": "user-1",
        "value": @ QueryValue::Map({
            let mut m = shamir_types::types::common::TMap::default();
            m.insert("name".to_string(), QueryValue::Str("x".to_string()));
            m
        })
    });
    match decode(&qv).expect("set alone") {
        BatchOp::Set(set) => assert_eq!(set.set.table, "k"),
        other => panic!("expected Set, got {other:?}"),
    }
}

/// Regression for the blanket unknown-key rejection removal (review finding
/// 1): a `Read` op carrying only payload keys (`select`, `where`,
/// `count_total`) must decode. With the deleted
/// `for key in m.keys() { if !is_marker(key) { return Err(unknown
/// BatchOp field) } }` loop restored in
/// `crates/query/shamir-query-types/src/ops/batch/ops/batch_op.rs`, this test
/// FAILS (`select`/`where`/`count_total` are not discriminators and were
/// rejected); with the loop removed it passes.
#[test]
fn rv_query_types_77bfc767_read_with_payload_keys_decodes() {
    let qv = mpack!({
        "from": "users",
        "where": @ QueryValue::Map({
            let mut m = shamir_types::types::common::TMap::default();
            m.insert("op".to_string(), QueryValue::Str("eq".to_string()));
            m.insert(
                "field".to_string(),
                QueryValue::List(vec![QueryValue::Str("name".to_string())]),
            );
            m.insert("value".to_string(), QueryValue::Str("x".to_string()));
            m
        }),
        "count_total": true
    });
    match decode(&qv).expect("payload keys belong to the typed op") {
        BatchOp::Read(read) => {
            assert_eq!(read.from.table, "users");
            assert!(read.r#where.is_some(), "where filter must decode");
            assert!(read.count_total);
        }
        other => panic!("expected Read, got {other:?}"),
    }
}

/// A `DropIndexOp` carrying its own payload keys (`table`, `repo`,
/// `request_id`) must decode as `BatchOp::DropIndex`; same
/// unknown-key-rejection regression class as
/// `rv_query_types_77bfc767_read_with_payload_keys_decodes`.
#[test]
fn rv_query_types_77bfc767_drop_index_with_payload_decodes() {
    let request_id = shamir_types::types::record_id::RecordId::try_from_bytes(
        &(0x01u8..=0x10).collect::<Vec<u8>>(),
    )
    .expect("16 raw bytes");
    let qv = mpack!({
        "drop_index": "idx",
        "table": "t",
        "repo": "main",
        "request_id": @ QueryValue::Str(request_id.to_string())
    });
    match decode(&qv).expect("payload keys belong to the typed op") {
        BatchOp::DropIndex(op) => assert_eq!(op.request_id, Some(request_id)),
        other => panic!("expected DropIndex, got {other:?}"),
    }
}

#[test]
fn trailing_sequence_elements_are_rejected_like_the_wire_decoder() {
    // TableRef reads exactly two elements; the buffered path must not drop a third.
    let valid = mpack!({
        "from": ["main", "users"]
    });
    assert!(decode(&valid).is_ok());
    let trailing = mpack!({
        "from": ["main", "users", "unexpected"]
    });
    assert!(decode(&trailing).is_err());
}

// Raw MessagePack: these forms are only reachable below the QueryValue builders.
#[test]
fn enum_identifier_forms_accepted_by_the_wire_decoder_stay_accepted() {
    let forms: [(&str, &[u8]); 3] = [
        (
            "variant index",
            &[
                0x82, 0xa9, b's', b'u', b'b', b's', b'c', b'r', b'i', b'b', b'e', 0x90, 0xa7, b'd',
                b'e', b'l', b'i', b'v', b'e', b'r', 0x00,
            ],
        ),
        (
            "string name",
            &[
                0x82, 0xa9, b's', b'u', b'b', b's', b'c', b'r', b'i', b'b', b'e', 0x90, 0xa7, b'd',
                b'e', b'l', b'i', b'v', b'e', b'r', 0xa7, b'r', b'e', b'c', b'o', b'r', b'd', b's',
            ],
        ),
        (
            "binary name",
            &[
                0x82, 0xa9, b's', b'u', b'b', b's', b'c', b'r', b'i', b'b', b'e', 0x90, 0xa7, b'd',
                b'e', b'l', b'i', b'v', b'e', b'r', 0xc4, 0x07, b'r', b'e', b'c', b'o', b'r', b'd',
                b's',
            ],
        ),
    ];
    for (label, bytes) in forms {
        match rmp_serde::from_slice::<BatchOp>(bytes) {
            Ok(BatchOp::Subscribe(op)) => assert_eq!(op.deliver, DeliverMode::Records, "{label}"),
            other => panic!("{label}: unexpected result {other:?}"),
        }
    }
}

#[test]
fn binary_where_a_sequence_is_expected_decodes_as_bytes() {
    // {"subscribe": bin8(empty)} decoded as an empty Vec before the direct path.
    let bytes = [
        0x81, 0xa9, b's', b'u', b'b', b's', b'c', b'r', b'i', b'b', b'e', 0xc4, 0x00,
    ];
    match rmp_serde::from_slice::<BatchOp>(&bytes) {
        Ok(BatchOp::Subscribe(op)) => assert!(op.subscribe.is_empty()),
        other => panic!("unexpected result {other:?}"),
    }
}

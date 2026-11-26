//! Decode-side proof tests for the wire dictionary (WIRE-DICT-1).
//!
//! The derived serde identifier visitors accept a MessagePack INTEGER key
//! wherever they accept a name string: integer `i` maps to the `i`-th
//! declaration entry of the filtered field/variant list — exactly the index
//! recorded in `docs/guide-docs/client-server-protocol-spec/wire-dictionary.json`.
//! These tests hand-build integer-keyed MessagePack frames with the raw
//! `rmp` writer (never byte literals) and decode them into the REAL DTOs,
//! always with an equality oracle: the same value decoded from the named
//! form (`to_vec_named`) must be Debug-equal to the indexed decode.
//!
//! NOT proven here (buffered string matching by construction, design §3.5):
//! internally tagged enums with an integer TAG key (the tag must stay a
//! string — negative case below) and `#[serde(flatten)]` fields (integer
//! keys are buffered and never match the inner struct — negative case
//! below).

use rmp::encode::{write_array_len, write_bool, write_map_len, write_nil, write_str, write_uint};
use shamir_collections::TMap;
use shamir_types::types::value::QueryValue;

use crate::admin::{CreateIndexOp, CreateTableOp};
use crate::batch::{
    BatchLimits, BatchOp, BatchRequest, IsolationLevel, QueryEntry, ResultEncoding,
};
use crate::filter::{Filter, FilterValue};
use crate::ops::subscribe::DeliverMode;
use crate::read::{Pagination, ReadQuery, Select, Temporal};
use crate::validator::WriteOp;
use crate::wire::db_message::DbRequest;
use crate::wire::token_admin::ApiTokenMode;
use crate::TableRef;

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

/// Encode a value in the NAMED form (the equality oracle's input).
fn named_bytes<T: serde::Serialize>(value: &T) -> Vec<u8> {
    rmp_serde::to_vec_named(value).expect("named msgpack encoding succeeds")
}

/// Decode both forms of the same value and require equality with `expected`.
/// This is the anti-vacuous oracle: a decode that succeeds on the indexed
/// bytes but produces a different value than the named decode of the same
/// logical value is a failure.
fn assert_indexed_matches_named<T>(indexed: &[u8], expected: &T)
where
    T: serde::Serialize + serde::de::DeserializeOwned + std::fmt::Debug,
{
    let from_indexed: T = rmp_serde::from_slice(indexed)
        .unwrap_or_else(|e| panic!("integer-keyed decode failed: {e}"));
    let from_named: T =
        rmp_serde::from_slice(&named_bytes(expected)).expect("named decode succeeds");
    let indexed_debug = format!("{from_indexed:?}");
    let named_debug = format!("{from_named:?}");
    assert_eq!(
        indexed_debug, named_debug,
        "indexed decode diverges from the named decode of the same value"
    );
    let expected_debug = format!("{expected:?}");
    assert_eq!(
        indexed_debug, expected_debug,
        "decoded value differs from expected; decoded = {indexed_debug}; expected = {expected_debug}"
    );
}

// ---------------------------------------------------------------------------
// 1. BatchRequest (deny_unknown_fields) with several ops, integer-keyed
// ---------------------------------------------------------------------------

/// BatchRequest declaration order (snapshot = source order, 13 fields):
///   0: id               (QueryValue)
///   1: name             (Option<String>, default, skip_serializing_if)
///   2: transactional    (bool, default)
///   3: isolation        (Option<IsolationLevel>, default, skip_serializing_if,
///                        deserialize_with — all index-safe)
///   4: durability       (Option<RequestDurability>)
///   5: index_visibility (Option<IndexVisibility>)
///   6: visibility_wait  (Option<VisibilityWait>)
///   7: queries          (TMap<String, QueryEntry>)
///   8: return_all       (bool, default = true)
///   9: return_only      (Option<Vec<String>>)
///  10: limits           (BatchLimits, default)
///  11: interner_epochs  (TMap<String, u64>)
///  12: result_encoding  (ResultEncoding, default = Name)
///
/// IsolationLevel variant indices: 0 Snapshot (default), 1 Serializable,
/// 2 Pessimistic.
#[test]
fn batch_request_decodes_integer_keyed_map_matching_named_decode() {
    let mut buf = Vec::new();
    write_map_len(&mut buf, 5).unwrap(); // keys 0, 2, 3, 7, 8 — others default
    write_uint(&mut buf, 0).unwrap(); // 0: id
    write_str(&mut buf, "batch-1").unwrap(); // QueryValue::Str
    write_uint(&mut buf, 2).unwrap(); // 2: transactional
    write_bool(&mut buf, true).unwrap();
    write_uint(&mut buf, 3).unwrap(); // 3: isolation
    write_uint(&mut buf, 1).unwrap(); // IsolationLevel index 1 = Serializable
    write_uint(&mut buf, 7).unwrap(); // 7: queries
                                      // queries map uses STRING alias keys (user data, not dictionary keys).
    write_map_len(&mut buf, 1).unwrap();
    write_str(&mut buf, "r").unwrap();
    // QueryEntry flattens BatchOp: its wire body is the ReadQuery's named
    // fields plus QueryEntry's own named options (Filter is internally
    // tagged = `named` per the snapshot; BatchOp dispatch is by field name).
    write_map_len(&mut buf, 2).unwrap();
    write_str(&mut buf, "from").unwrap(); // ReadQuery 0
    write_str(&mut buf, "users").unwrap(); // TableRef named = bare table string
    write_str(&mut buf, "where").unwrap(); // ReadQuery 2 (r#where)
                                           // Filter::Eq — internally tagged: STRING tag "op", then its named fields.
    write_map_len(&mut buf, 3).unwrap();
    write_str(&mut buf, "op").unwrap();
    write_str(&mut buf, "eq").unwrap();
    write_str(&mut buf, "field").unwrap();
    write_array_len(&mut buf, 1).unwrap(); // FieldPath = Vec<String>
    write_str(&mut buf, "status").unwrap();
    write_str(&mut buf, "value").unwrap();
    write_str(&mut buf, "active").unwrap(); // FilterValue::String
    write_uint(&mut buf, 8).unwrap(); // 8: return_all (prove a non-default value)
    write_bool(&mut buf, false).unwrap();

    let mut queries = TMap::default();
    queries.insert(
        "r".to_string(),
        QueryEntry {
            op: BatchOp::Read(ReadQuery {
                from: TableRef::new("users"),
                select: Select::all(),
                r#where: Some(Filter::Eq {
                    field: vec!["status".to_string()],
                    value: FilterValue::String("active".to_string()),
                }),
                group_by: None,
                order_by: None,
                pagination: Pagination::None,
                count_total: false,
                temporal: Temporal::Latest,
                with_version: false,
                explain: false,
            }),
            return_result: true,
            after: Vec::new(),
            when: None,
            abort_on_skip: false,
        },
    );
    let expected = BatchRequest {
        id: QueryValue::Str("batch-1".to_string()),
        name: None, // key 1 absent → default None
        transactional: true,
        isolation: Some(IsolationLevel::Serializable),
        durability: None,
        index_visibility: None,
        visibility_wait: None,
        queries,
        return_all: false,
        return_only: None,
        limits: BatchLimits::default(), // key 10 absent → default
        interner_epochs: TMap::default(),
        result_encoding: ResultEncoding::Name, // key 12 absent → default
    };
    assert_indexed_matches_named(&buf, &expected);
}

// ---------------------------------------------------------------------------
// 2. DDL: create-table-with-index reachable from DbRequest::Execute
// ---------------------------------------------------------------------------

/// CreateTableOp indices:   0 create_table, 1 repo, 2 if_not_exists,
///                          3 retention, 4 schema.
/// CreateIndexOp indices (16 fields): 0 create_index, 1 table, 2 fields,
///   3 unique, 4 sorted, 5 repo, 6 index_type, 7 fts_tokenizer,
///   8 fts_language, 9 functional_op, 10 functional_args, 11 vector_dim,
///   12 vector_metric, 13 vector_quantization, 14 include, 15 if_not_exists.
///
/// Direct typed decode: the derived field-identifier visitors accept the
/// integer declaration index, so the whole DDL op payload decodes
/// integer-keyed. This is the decode side WIRE-DICT-2's IndexedKeys
/// adapter will emit.
#[test]
fn ddl_create_table_and_create_index_decode_integer_keyed_directly() {
    // CreateTableOp: keys 0 and 2; repo/retention/schema default.
    let mut buf = Vec::new();
    write_map_len(&mut buf, 2).unwrap();
    write_uint(&mut buf, 0).unwrap(); // 0: create_table
    write_str(&mut buf, "events").unwrap();
    write_uint(&mut buf, 2).unwrap(); // 2: if_not_exists
    write_bool(&mut buf, true).unwrap();
    let expected = CreateTableOp {
        create_table: "events".to_string(),
        repo: "main".to_string(), // key 1 absent → default_repo()
        if_not_exists: true,
        retention: None,
        schema: None,
    };
    assert_indexed_matches_named(&buf, &expected);

    // CreateIndexOp: keys 0, 1, 2, 3; the other 12 fields default.
    let mut buf = Vec::new();
    write_map_len(&mut buf, 4).unwrap();
    write_uint(&mut buf, 0).unwrap(); // 0: create_index
    write_str(&mut buf, "idx_events_status").unwrap();
    write_uint(&mut buf, 1).unwrap(); // 1: table
    write_str(&mut buf, "events").unwrap();
    write_uint(&mut buf, 2).unwrap(); // 2: fields
    write_array_len(&mut buf, 1).unwrap();
    write_array_len(&mut buf, 1).unwrap();
    write_str(&mut buf, "status").unwrap();
    write_uint(&mut buf, 3).unwrap(); // 3: unique
    write_bool(&mut buf, true).unwrap();
    let expected = CreateIndexOp {
        create_index: "idx_events_status".to_string(),
        table: "events".to_string(),
        fields: vec![vec!["status".to_string()]],
        unique: true,
        sorted: false,            // key 4 absent → default
        repo: "main".to_string(), // key 5 absent → default_repo()
        index_type: None,
        fts_tokenizer: None,
        fts_language: None,
        functional_op: None,
        functional_args: None,
        vector_dim: None,
        vector_metric: None,
        vector_quantization: None,
        include: Vec::new(),  // key 14 absent → default
        if_not_exists: false, // key 15 absent → default
    };
    assert_indexed_matches_named(&buf, &expected);
}

/// DbRequest::Execute indices: 0 query_version (default), 1 db, 2 batch.
/// BatchRequest indices: 0 id, 7 queries.
///
/// The end-to-end DDL request with the STRING tag `"op": "execute"` and
/// integer payload keys. NOTE: the BatchOp dispatch goes through the
/// QueryValue bridge (`Value<String>` buffering, design §3.5 audit), whose
/// String-keyed map can only carry STRING op-payload keys today — so the
/// op payloads here stay in their current named form; the fully
/// integer-keyed DDL payloads are proven by the direct typed decodes in
/// `ddl_create_table_and_create_index_decode_integer_keyed_directly`.
#[test]
fn db_request_execute_with_create_table_and_index_roundtrips() {
    let mut buf = Vec::new();
    // DbRequest::Execute: internally tagged — tag key is the STRING "op".
    write_map_len(&mut buf, 4).unwrap();
    write_str(&mut buf, "op").unwrap();
    write_str(&mut buf, "execute").unwrap();
    write_uint(&mut buf, 0).unwrap(); // 0: query_version
    write_uint(&mut buf, 4).unwrap();
    write_uint(&mut buf, 1).unwrap(); // 1: db
    write_str(&mut buf, "app").unwrap();
    write_uint(&mut buf, 2).unwrap(); // 2: batch
                                      // BatchRequest: keys 0 (id) and 7 (queries); the rest default.
    write_map_len(&mut buf, 2).unwrap();
    write_uint(&mut buf, 0).unwrap(); // 0: id
    write_nil(&mut buf).unwrap(); // QueryValue::Null
    write_uint(&mut buf, 7).unwrap(); // 7: queries
    write_map_len(&mut buf, 2).unwrap(); // two aliases (STRING alias keys)
    write_str(&mut buf, "t").unwrap();
    // QueryEntry "t": flattened CreateTableOp op, NAMED payload (see note).
    write_map_len(&mut buf, 1).unwrap();
    write_str(&mut buf, "create_table").unwrap();
    write_map_len(&mut buf, 2).unwrap();
    write_str(&mut buf, "create_table").unwrap();
    write_str(&mut buf, "events").unwrap();
    write_str(&mut buf, "if_not_exists").unwrap();
    write_bool(&mut buf, true).unwrap();
    write_str(&mut buf, "i").unwrap();
    // QueryEntry "i": flattened CreateIndexOp op, NAMED payload (see note).
    write_map_len(&mut buf, 1).unwrap();
    write_str(&mut buf, "create_index").unwrap();
    write_map_len(&mut buf, 4).unwrap();
    write_str(&mut buf, "create_index").unwrap();
    write_str(&mut buf, "idx_events_status").unwrap();
    write_str(&mut buf, "table").unwrap();
    write_str(&mut buf, "events").unwrap();
    write_str(&mut buf, "fields").unwrap();
    write_array_len(&mut buf, 1).unwrap();
    write_array_len(&mut buf, 1).unwrap();
    write_str(&mut buf, "status").unwrap();
    write_str(&mut buf, "unique").unwrap();
    write_bool(&mut buf, true).unwrap();

    let mut queries = TMap::default();
    queries.insert(
        "t".to_string(),
        QueryEntry {
            op: BatchOp::CreateTable(CreateTableOp {
                create_table: "events".to_string(),
                repo: "main".to_string(),
                if_not_exists: true,
                retention: None,
                schema: None,
            }),
            return_result: true,
            after: Vec::new(),
            when: None,
            abort_on_skip: false,
        },
    );
    queries.insert(
        "i".to_string(),
        QueryEntry {
            op: BatchOp::CreateIndex(CreateIndexOp {
                create_index: "idx_events_status".to_string(),
                table: "events".to_string(),
                fields: vec![vec!["status".to_string()]],
                unique: true,
                sorted: false,
                repo: "main".to_string(),
                index_type: None,
                fts_tokenizer: None,
                fts_language: None,
                functional_op: None,
                functional_args: None,
                vector_dim: None,
                vector_metric: None,
                vector_quantization: None,
                include: Vec::new(),
                if_not_exists: false,
            }),
            return_result: true,
            after: Vec::new(),
            when: None,
            abort_on_skip: false,
        },
    );
    let expected = DbRequest::Execute {
        query_version: 4,
        db: "app".to_string(),
        batch: BatchRequest {
            id: QueryValue::Null,
            name: None,
            transactional: false,
            isolation: None,
            durability: None,
            index_visibility: None,
            visibility_wait: None,
            queries,
            return_all: true,
            return_only: None,
            limits: BatchLimits::default(),
            interner_epochs: TMap::default(),
            result_encoding: ResultEncoding::Name,
        },
    };
    assert_indexed_matches_named(&buf, &expected);
}

// ---------------------------------------------------------------------------
// 3. Attributes: rename/rename_all/default/skip_serializing_if are
//    index-safe — the default fills an ABSENT key; the integer key decodes
//    when present. rename_all does NOT change the u64 mapping.
// ---------------------------------------------------------------------------

/// DbRequest::TxBegin indices: 0 query_version (default = 0 when absent,
/// `rename_all = "snake_case"` on the container renames only the TAG
/// string, never the index), 1 db, 2 repo, 3 isolation (`default` +
/// `skip_serializing_if` + `deserialize_with`).
#[test]
fn tx_begin_integer_keyed_fields_fill_defaults_and_match_named_decode() {
    // (a) key 0 ABSENT: default_query_version() = 0 fills query_version.
    let mut buf = Vec::new();
    write_map_len(&mut buf, 4).unwrap();
    write_str(&mut buf, "op").unwrap(); // tag stays a STRING
    write_str(&mut buf, "tx_begin").unwrap(); // snake_case renamed tag
    write_uint(&mut buf, 1).unwrap(); // 1: db
    write_str(&mut buf, "app").unwrap();
    write_uint(&mut buf, 2).unwrap(); // 2: repo
    write_str(&mut buf, "main").unwrap();
    write_uint(&mut buf, 3).unwrap(); // 3: isolation
    write_uint(&mut buf, 2).unwrap(); // IsolationLevel index 2 = Pessimistic
    let expected = DbRequest::TxBegin {
        query_version: 0,
        db: "app".to_string(),
        repo: "main".to_string(),
        isolation: Some(IsolationLevel::Pessimistic),
    };
    assert_indexed_matches_named(&buf, &expected);

    // (b) key 0 PRESENT as an integer: decoded value wins over the default.
    let mut buf = Vec::new();
    write_map_len(&mut buf, 5).unwrap();
    write_str(&mut buf, "op").unwrap();
    write_str(&mut buf, "tx_begin").unwrap();
    write_uint(&mut buf, 0).unwrap(); // 0: query_version
    write_uint(&mut buf, 7).unwrap();
    write_uint(&mut buf, 1).unwrap();
    write_str(&mut buf, "app").unwrap();
    write_uint(&mut buf, 2).unwrap();
    write_str(&mut buf, "main").unwrap();
    write_uint(&mut buf, 3).unwrap();
    write_nil(&mut buf).unwrap(); // isolation: None (skip_serializing_if form)
    let expected = DbRequest::TxBegin {
        query_version: 7,
        db: "app".to_string(),
        repo: "main".to_string(),
        isolation: None,
    };
    assert_indexed_matches_named(&buf, &expected);
}

// ---------------------------------------------------------------------------
// 4. Negative cases — exact error class assertions
// ---------------------------------------------------------------------------

/// 4a. Integer key 13 is out of range for BatchRequest's 13 fields
/// (indices 0..=12). deny_unknown_fields structs reject the out-of-range
/// index with an invalid-value error naming the field index at next_key
/// time.
#[test]
fn batch_request_rejects_out_of_range_integer_key() {
    let mut buf = Vec::new();
    write_map_len(&mut buf, 1).unwrap();
    write_uint(&mut buf, 13).unwrap(); // out of range: valid indices are 0..=12
    write_nil(&mut buf).unwrap();
    let err = rmp_serde::from_slice::<BatchRequest>(&buf)
        .expect_err("integer key 13 must not decode into a 13-field struct");
    let message = err.to_string();
    assert!(
        message.contains("invalid value") && message.contains("13"),
        "expected an invalid-value error naming index 13, got: {message}"
    );
    assert!(
        message.contains("field index"),
        "expected the field-index expectation in the error, got: {message}"
    );
}

/// 4b. QueryEntry flattens BatchOp: integer keys are buffered and NEVER
/// match the inner struct's fields (serde's FlatMapDeserializer
/// pre-filters by string). QueryEntry itself has NO deny_unknown_fields,
/// but its flattened BatchOp impl buffers entries with String keys, so an
/// integer key is a typed decode error ("invalid type: integer"), not a
/// silent skip — the assertion the source dictates.
#[test]
fn query_entry_integer_key_is_an_error_not_a_silent_skip() {
    let mut buf = Vec::new();
    write_map_len(&mut buf, 1).unwrap();
    write_uint(&mut buf, 0).unwrap(); // an integer key where BatchOp expects a name
    write_nil(&mut buf).unwrap();
    let err = rmp_serde::from_slice::<QueryEntry>(&buf)
        .expect_err("integer keys never match a flattened field");
    let message = err.to_string();
    assert!(
        message.contains("invalid type"),
        "expected an invalid-type error for the integer key, got: {message}"
    );
}

/// 4c. DbRequest is internally tagged (tag = "op"): the TAG must be the
/// STRING "op". An integer tag key (0) is buffered, never matches, and
/// surfaces as a missing-field error for `op`.
#[test]
fn db_request_integer_tag_key_is_missing_field_error() {
    let mut buf = Vec::new();
    write_map_len(&mut buf, 1).unwrap();
    write_uint(&mut buf, 0).unwrap(); // integer key 0 instead of "op"
    write_str(&mut buf, "ping").unwrap();
    let err = rmp_serde::from_slice::<DbRequest>(&buf)
        .expect_err("the internally tagged key must be the string \"op\"");
    let message = err.to_string();
    assert!(
        message.contains("missing field") && message.contains("op"),
        "expected a missing-field `op` error, got: {message}"
    );
}

/// 4d. Externally tagged indexed enums: the variant identifier visitor
/// accepts the variant INDEX. Unit-only enums decode from a bare integer;
/// general externally tagged enums decode from a single-entry map whose
/// key is the index and whose value is the payload (nil for unit variants).
#[test]
fn externally_tagged_enums_decode_integer_variant_indices() {
    // ApiTokenMode: 0 = ReadOnly, 1 = ReadWrite (plain unit enum, bare int).
    let mut buf = Vec::new();
    write_uint(&mut buf, 1).unwrap();
    let decoded: ApiTokenMode = rmp_serde::from_slice(&buf).expect("variant index decodes");
    assert_eq!(decoded, ApiTokenMode::ReadWrite);
    let named: ApiTokenMode =
        rmp_serde::from_slice(&named_bytes(&ApiTokenMode::ReadWrite)).unwrap();
    assert_eq!(decoded, named, "equality oracle: named vs indexed");

    // WriteOp: 0 = Insert, 3 = Delete (plain unit enum, bare int).
    let mut buf = Vec::new();
    write_uint(&mut buf, 3).unwrap();
    let decoded: WriteOp = rmp_serde::from_slice(&buf).expect("variant index decodes");
    assert_eq!(decoded, WriteOp::Delete);
    let named: WriteOp = rmp_serde::from_slice(&named_bytes(&WriteOp::Delete)).unwrap();
    assert_eq!(decoded, named);

    // DeliverMode (externally tagged, mixed unit/newtype variants):
    // single-entry map { index: payload }. Index 1 = Keys (unit).
    let mut buf = Vec::new();
    write_map_len(&mut buf, 1).unwrap();
    write_uint(&mut buf, 1).unwrap();
    write_nil(&mut buf).unwrap();
    let decoded: DeliverMode = rmp_serde::from_slice(&buf).expect("variant index map decodes");
    assert_eq!(decoded, DeliverMode::Keys);
    let named: DeliverMode = rmp_serde::from_slice(&named_bytes(&DeliverMode::Keys)).unwrap();
    assert_eq!(decoded, named, "equality oracle: named vs indexed");
}

//! Decode-side proof tests for the wire dictionary (WIRE-DICT-1).
//!
//! The derived serde identifier visitors accept a MessagePack INTEGER key
//! wherever they accept a name string: integer `i` maps to the `i`-th
//! declaration entry of the filtered field/variant list — exactly the index
//! recorded in `docs/guide-docs/client-server-protocol-spec/wire-dictionary.json`.
//! These tests hand-build integer-keyed MessagePack frames with the raw
//! `rmp` writer (never byte literals) and decode them into the REAL DTOs,
//! always with an equality oracle: the decoded value must equal the named
//! decode (`to_vec_named`) of the same logical value — typed `PartialEq`
//! where the DTO derives it, Debug-text equality for `DbRequest` (which
//! derives no `PartialEq`; see `assert_indexed_matches_named_debug`).
//!
//! NOT proven here (buffered string matching by construction, design §3.5):
//! internally tagged enums with an integer TAG key (the tag must stay a
//! string — negative case below) and `#[serde(flatten)]` fields (integer
//! keys are buffered and never match the inner struct — negative case
//! below).

use rmp::encode::{write_array_len, write_bool, write_map_len, write_nil, write_str, write_uint};
use shamir_collections::TMap;
use shamir_types::types::value::QueryValue;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

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
use crate::write::{InsertOp, InsertSelect, UpdateOp};
use crate::TableRef;

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

/// Encode a value in the NAMED form (the equality oracle's input).
fn named_bytes<T: serde::Serialize>(value: &T) -> Vec<u8> {
    rmp_serde::to_vec_named(value).expect("named msgpack encoding succeeds")
}

/// Decode both forms of the same value and require TYPED equality with
/// `expected`. Used when the DTO derives `PartialEq` (`BatchRequest`,
/// `QueryEntry`, `CreateTableOp`, `CreateIndexOp`, `InsertOp`, `UpdateOp` —
/// all derive it). This is the anti-vacuous oracle: a decode that succeeds
/// on the indexed bytes but produces a different value than the named
/// decode of the same logical value is a failure.
fn assert_indexed_matches_named<T>(indexed: &[u8], expected: &T)
where
    T: serde::Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug,
{
    let from_indexed: T = rmp_serde::from_slice(indexed)
        .unwrap_or_else(|e| panic!("integer-keyed decode failed: {e}"));
    let from_named: T =
        rmp_serde::from_slice(&named_bytes(expected)).expect("named decode succeeds");
    assert_eq!(
        from_indexed, from_named,
        "indexed decode diverges from the named decode of the same value"
    );
    assert_eq!(
        &from_indexed, expected,
        "decoded value differs from expected; decoded = {from_indexed:?}"
    );
}

/// Same oracle for DTOs that do NOT derive `PartialEq`. `DbRequest` derives
/// only `Debug, Clone, Serialize, Deserialize` (crates/query/shamir-query-types/
/// src/wire/db_message.rs container attrs) and the production file is
/// untouchable in this task, so typed equality is unavailable and equality is
/// asserted through Debug text.
fn assert_indexed_matches_named_debug<T>(indexed: &[u8], expected: &T)
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
                                      // Three ops: a read, an insert, an update — a real multi-op batch.
    write_map_len(&mut buf, 3).unwrap();
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
                                            // Alias "i1": InsertOp entry, NAMED payload. NOTE: the op payload keys
                                            // CANNOT be integers through `BatchOp`: its Deserialize buffers the
                                            // whole op map with STRING keys (`next_entry::<String, QueryValue>()`,
                                            // ops/batch/ops/batch_op.rs:398-424) before dispatching the payload
                                            // DTO — an integer key is a typed decode error there (negative test 4b).
                                            // The round-1 finding that motivates converting BatchOp in WIRE-DICT-2.
                                            // The DTO field indices ARE exercised directly (integer-keyed) in
                                            // `write_ops_decode_integer_keyed_directly` below.
                                            // InsertOp field indices (ops/write/types.rs:84-109):
                                            //   0 insert_into, 1 values, 2 records_idmsgpack (default, skip-if-empty),
                                            //   3 select (default, skip-if-none).
    write_str(&mut buf, "i1").unwrap();
    write_map_len(&mut buf, 2).unwrap(); // op payload fields at TOP level (QueryEntry flattens BatchOp — no wrapper)
    write_str(&mut buf, "insert_into").unwrap(); // InsertOp 0
    write_str(&mut buf, "users").unwrap(); // TableRef named = bare table string
    write_str(&mut buf, "values").unwrap(); // InsertOp 1
    write_array_len(&mut buf, 1).unwrap();
    write_str(&mut buf, "alice").unwrap(); // QueryValue::Str
                                           // Alias "u1": UpdateOp entry, NAMED payload (same BatchOp constraint as "i1").
                                           // UpdateOp field indices (ops/write/types.rs:114-143):
                                           //   0 update, 1 where (rename="where", default, skip-if-none),
                                           //   2 set, 3 select (default, skip-if-none), 4 expected_version (default, skip-if-none).
    write_str(&mut buf, "u1").unwrap();
    write_map_len(&mut buf, 3).unwrap();
    write_str(&mut buf, "update").unwrap(); // UpdateOp 0
    write_str(&mut buf, "users").unwrap();
    write_str(&mut buf, "where").unwrap(); // UpdateOp 1
                                           // Nested Filter::Eq — internally tagged (tag="op"): the tag VALUE
                                           // stays a STRING; enum VALUES never index through the buffered
                                           // String-keyed map, only field/variant identifiers do.
    write_map_len(&mut buf, 3).unwrap();
    write_str(&mut buf, "op").unwrap();
    write_str(&mut buf, "eq").unwrap();
    write_str(&mut buf, "field").unwrap();
    write_array_len(&mut buf, 1).unwrap(); // FieldPath = Vec<String>
    write_str(&mut buf, "status").unwrap();
    write_str(&mut buf, "value").unwrap();
    write_str(&mut buf, "active").unwrap(); // FilterValue::String (untagged)
    write_str(&mut buf, "set").unwrap(); // UpdateOp 2
    write_map_len(&mut buf, 1).unwrap(); // QueryValue::Map({"name": "bob"})
    write_str(&mut buf, "name").unwrap();
    write_str(&mut buf, "bob").unwrap();
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
    queries.insert(
        "i1".to_string(),
        QueryEntry {
            op: BatchOp::Insert(InsertOp {
                insert_into: TableRef::new("users"),
                values: vec![QueryValue::Str("alice".to_string())],
                records_idmsgpack: Vec::new(), // key 2 absent → default
                select: None,                  // key 3 absent → default
            }),
            return_result: true, // default_return() = true, omitted on the wire
            after: Vec::new(),
            when: None,
            abort_on_skip: false,
        },
    );
    let mut set = TMap::default();
    set.insert("name".to_string(), QueryValue::Str("bob".to_string()));
    queries.insert(
        "u1".to_string(),
        QueryEntry {
            op: BatchOp::Update(UpdateOp {
                update: TableRef::new("users"),
                where_clause: Some(Filter::Eq {
                    field: vec!["status".to_string()],
                    value: FilterValue::String("active".to_string()),
                }),
                set: QueryValue::Map(set),
                select: None,           // key 3 absent → default
                expected_version: None, // key 4 absent → default
            }),
            return_result: true, // default_return() = true, omitted on the wire
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

/// Direct typed decode of the WRITE op DTOs with INTEGER field keys — the
/// indices listed in the batch test's comment. This is the decode side
/// WIRE-DICT-2's IndexedKeys adapter will emit once BatchOp can carry
/// integer-keyed payloads (today it cannot — see 4b / the batch test note).
///
/// The nested `where` Filter VALUE is written in its NAMED internally-tagged
/// form (`"op": "eq"` + named fields): an enum VALUE's tag must be the
/// string; integer variant indices apply only to field/variant IDENTIFIERS
/// (and, in buffered contexts, to the single-entry-map form proven in the
/// TxBegin test), never to an internally-tagged tag value.
#[test]
fn write_ops_decode_integer_keyed_directly() {
    // InsertOp: keys 0 (insert_into), 1 (values), 3 (select); key 2
    // (records_idmsgpack) absent → default empty (skip_serializing_if is
    // index-safe on both sides).
    let mut buf = Vec::new();
    write_map_len(&mut buf, 3).unwrap();
    write_uint(&mut buf, 0).unwrap(); // 0: insert_into
    write_str(&mut buf, "users").unwrap(); // TableRef named = bare string
    write_uint(&mut buf, 1).unwrap(); // 1: values
    write_array_len(&mut buf, 1).unwrap();
    write_str(&mut buf, "alice").unwrap(); // QueryValue::Str
    write_uint(&mut buf, 3).unwrap(); // 3: select
    write_map_len(&mut buf, 1).unwrap();
    write_str(&mut buf, "fields").unwrap();
    write_array_len(&mut buf, 1).unwrap();
    write_str(&mut buf, "id").unwrap();
    let expected = InsertOp {
        insert_into: TableRef::new("users"),
        values: vec![QueryValue::Str("alice".to_string())],
        records_idmsgpack: Vec::new(), // key 2 absent → default
        select: Some(InsertSelect {
            fields: Some(vec!["id".to_string()]),
        }),
    };
    assert_indexed_matches_named(&buf, &expected);

    // UpdateOp: keys 0 (update), 1 (where), 2 (set), 4 (expected_version);
    // key 3 (select) absent → default None.
    let mut buf = Vec::new();
    write_map_len(&mut buf, 4).unwrap();
    write_uint(&mut buf, 0).unwrap(); // 0: update
    write_str(&mut buf, "users").unwrap();
    write_uint(&mut buf, 1).unwrap(); // 1: where
                                      // Nested Filter::Eq — NAMED internally-tagged value (see doc comment).
    write_map_len(&mut buf, 3).unwrap();
    write_str(&mut buf, "op").unwrap();
    write_str(&mut buf, "eq").unwrap();
    write_str(&mut buf, "field").unwrap();
    write_array_len(&mut buf, 1).unwrap();
    write_str(&mut buf, "status").unwrap();
    write_str(&mut buf, "value").unwrap();
    write_str(&mut buf, "active").unwrap(); // FilterValue::String (untagged)
    write_uint(&mut buf, 2).unwrap(); // 2: set
    write_map_len(&mut buf, 1).unwrap(); // QueryValue::Map({"name": "bob"})
    write_str(&mut buf, "name").unwrap();
    write_str(&mut buf, "bob").unwrap();
    write_uint(&mut buf, 4).unwrap(); // 4: expected_version
    write_uint(&mut buf, 7).unwrap(); // Some(7)
    let mut set = TMap::default();
    set.insert("name".to_string(), QueryValue::Str("bob".to_string()));
    let expected = UpdateOp {
        update: TableRef::new("users"),
        where_clause: Some(Filter::Eq {
            field: vec!["status".to_string()],
            value: FilterValue::String("active".to_string()),
        }),
        set: QueryValue::Map(set),
        select: None, // key 3 absent → default
        expected_version: Some(7),
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
/// integer BatchRequest keys. The op payloads are written in their NAMED
/// form at the TOP level of each QueryEntry map (QueryEntry flattens
/// BatchOp, and BatchOp forwards the payload DTO verbatim — there is NO
/// `{create_table: {...}}` discriminator wrapper). Integer-keyed op
/// payloads CANNOT pass through BatchOp today: its Deserialize buffers the
/// whole op map with String keys (`next_entry::<String, QueryValue>()`,
/// ops/batch/ops/batch_op.rs:398-424) before dispatching the payload DTO,
/// so an integer payload key is a typed decode error — the round-1 finding
/// that motivates converting BatchOp in WIRE-DICT-2. The integer-keyed
/// DIRECT decodes of the DDL DTOs are proven by the standalone test
/// `ddl_create_table_and_create_index_decode_integer_keyed_directly`.
/// Equality is asserted through Debug text (see
/// `assert_indexed_matches_named_debug` — DbRequest has no PartialEq).
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
    // QueryEntry "t": flattened CreateTableOp — the DTO fields sit at the
    // TOP level of the entry map (no discriminator wrapper), NAMED form.
    // CreateTableOp indices: 0 create_table, 1 repo (default "main"),
    // 2 if_not_exists (default + skip_serializing_if = is_false).
    write_map_len(&mut buf, 2).unwrap();
    write_str(&mut buf, "create_table").unwrap();
    write_str(&mut buf, "events").unwrap();
    write_str(&mut buf, "if_not_exists").unwrap();
    write_bool(&mut buf, true).unwrap();
    write_str(&mut buf, "i").unwrap();
    // QueryEntry "i": flattened CreateIndexOp — top-level NAMED fields.
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
    assert_indexed_matches_named_debug(&buf, &expected);
}

// ---------------------------------------------------------------------------
// 3. Attributes: rename/rename_all/default/skip_serializing_if are
//    index-safe — the default fills an ABSENT key; the integer key decodes
//    when present. rename_all does NOT change the u64 mapping.
// ---------------------------------------------------------------------------

/// DbRequest::TxBegin indices: 0 query_version (default = 0 when absent,
/// `rename_all = "snake_case"` on the container renames only the TAG
/// string, never the index), 1 db, 2 repo, 3 isolation (`default` +
/// `skip_serializing_if` + `deserialize_with` =
/// `deserialize_optional_field`, ops/batch/req/options/deserialization.rs:4-15,
/// which is a transparent `Option::<T>::deserialize`).
///
/// (a) NEGATIVE: a bare integer as the `isolation` VALUE cannot decode.
/// DbRequest is internally tagged, so the payload is buffered as serde
/// `Content` and the enum reaches `ContentDeserializer::deserialize_enum`
/// (serde-1.0.229/src/private/de.rs:1429-1459), which accepts a STRING or a
/// single-entry MAP only — a bare `Content::U64(2)` is an `invalid type`
/// error ("invalid type: integer `2`, expected string or map").
#[test]
fn tx_begin_bare_integer_isolation_value_is_invalid_type_error() {
    let mut buf = Vec::new();
    write_map_len(&mut buf, 4).unwrap();
    write_str(&mut buf, "op").unwrap(); // tag stays a STRING
    write_str(&mut buf, "tx_begin").unwrap();
    write_uint(&mut buf, 1).unwrap(); // 1: db
    write_str(&mut buf, "app").unwrap();
    write_uint(&mut buf, 2).unwrap(); // 2: repo
    write_str(&mut buf, "main").unwrap();
    write_uint(&mut buf, 3).unwrap(); // 3: isolation
    write_uint(&mut buf, 2).unwrap(); // bare integer — NOT a legal enum value here
    let err = rmp_serde::from_slice::<DbRequest>(&buf)
        .expect_err("a bare integer isolation value must not decode through the buffered enum");
    // KIND: serde-generated error → Error::Syntax
    // (rmp-serde-1.3.1/src/decode.rs:82-86 `de::Error::custom`).
    let message = match &err {
        rmp_serde::decode::Error::Syntax(message) => message.clone(),
        other => panic!("expected Error::Syntax for the bare integer enum value, got {other:?}"),
    };
    // TEXT (single mechanism distinguisher): "invalid type" proves the
    // buffered-enum value rejection (ContentDeserializer::deserialize_enum
    // wants string-or-map) — not an Option/type error on the field.
    assert!(
        message.contains("invalid type"),
        "expected an invalid-type error for the bare integer enum value, got: {message}"
    );
}

/// (b) NIL: `write_nil` for isolation → `Option::None`. Fine — nil is the
/// Option's own form, the enum is never reached.
#[test]
fn tx_begin_integer_keyed_fields_fill_defaults_and_match_named_decode() {
    // key 0 ABSENT: default_query_version() = 0 fills query_version.
    let mut buf = Vec::new();
    write_map_len(&mut buf, 4).unwrap();
    write_str(&mut buf, "op").unwrap(); // tag stays a STRING
    write_str(&mut buf, "tx_begin").unwrap(); // snake_case renamed tag
    write_uint(&mut buf, 1).unwrap(); // 1: db
    write_str(&mut buf, "app").unwrap();
    write_uint(&mut buf, 2).unwrap(); // 2: repo
    write_str(&mut buf, "main").unwrap();
    write_uint(&mut buf, 3).unwrap(); // 3: isolation
    write_nil(&mut buf).unwrap(); // isolation: None (skip_serializing_if form)
    let expected = DbRequest::TxBegin {
        query_version: 0,
        db: "app".to_string(),
        repo: "main".to_string(),
        isolation: None,
    };
    assert_indexed_matches_named_debug(&buf, &expected);

    // key 0 PRESENT as an integer: decoded value wins over the default.
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
    write_nil(&mut buf).unwrap(); // isolation: None
    let expected = DbRequest::TxBegin {
        query_version: 7,
        db: "app".to_string(),
        repo: "main".to_string(),
        isolation: None,
    };
    assert_indexed_matches_named_debug(&buf, &expected);
}

/// (c) POSITIVE control for (a): the enum-via-single-entry-map form DOES
/// decode in the buffered context. serde-1.0.229/src/private/de.rs:1429-1459:
/// `ContentDeserializer::deserialize_enum` takes a single-entry
/// `Content::Map` and hands its first pair to `EnumDeserializer` — the key
/// `Content::U64(2)` becomes the variant identifier (`visit_u64` → variant
/// index 2 = Pessimistic, snake_case rename_all only affects tag NAMES),
/// the value `Content::Nil` is the unit-variant payload. rmp-serde hands
/// the whole field value to serde as one buffered map entry, so this form
/// survives the internally-tagged buffering that kills the bare integer.
#[test]
fn tx_begin_isolation_variant_index_via_map_form_decodes() {
    let mut buf = Vec::new();
    write_map_len(&mut buf, 4).unwrap();
    write_str(&mut buf, "op").unwrap();
    write_str(&mut buf, "tx_begin").unwrap();
    write_uint(&mut buf, 1).unwrap(); // 1: db
    write_str(&mut buf, "app").unwrap();
    write_uint(&mut buf, 2).unwrap(); // 2: repo
    write_str(&mut buf, "main").unwrap();
    write_uint(&mut buf, 3).unwrap(); // 3: isolation
    write_map_len(&mut buf, 1).unwrap(); // externally-tagged form: {index: payload}
    write_uint(&mut buf, 2).unwrap(); // IsolationLevel variant index 2 = Pessimistic
    write_nil(&mut buf).unwrap(); // unit-variant payload
    let expected = DbRequest::TxBegin {
        query_version: 0,
        db: "app".to_string(),
        repo: "main".to_string(),
        isolation: Some(IsolationLevel::Pessimistic),
    };
    assert_indexed_matches_named_debug(&buf, &expected);
}

// ---------------------------------------------------------------------------
// 4. Negative cases — exact error class assertions
// ---------------------------------------------------------------------------

/// Snapshot-derived BatchRequest field count. The snapshot JSON
/// (`docs/guide-docs/client-server-protocol-spec/wire-dictionary.json`, CHECK 1
/// of the scan test pins it against the sources) is the ground truth for the
/// declaration count, so the out-of-range probe in 4a is computed from it
/// instead of a hand-pinned constant (previously
/// `const BATCH_REQUEST_FIELD_COUNT: usize = 13`).
#[derive(serde::Deserialize)]
struct WireSnapshot {
    /// type name → declaration entry; only `fields`/`variants` lengths are
    /// read here (the snapshot's `roots` key is ignored: no
    /// `deny_unknown_fields`).
    types: BTreeMap<String, WireSnapshotType>,
}

#[derive(serde::Deserialize)]
struct WireSnapshotType {
    #[serde(default)]
    fields: Vec<serde_json::Value>,
    #[serde(default)]
    variants: Vec<serde_json::Value>,
}

fn snapshot_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(3)
        .expect("crate sits three levels below the repo root")
        .join("docs/guide-docs/client-server-protocol-spec/wire-dictionary.json")
}

/// BatchRequest's declared field count, read live from the snapshot.
fn snapshot_batch_request_field_count() -> u64 {
    let raw = std::fs::read_to_string(snapshot_path())
        .expect("wire-dictionary.json must exist next to docs/guide-docs");
    let snapshot: WireSnapshot = serde_json::from_str(&raw)
        .expect("wire-dictionary.json must parse as {types: {…: {fields?, variants?}}}");
    let entry = snapshot
        .types
        .get("BatchRequest")
        .expect("snapshot must declare BatchRequest");
    assert!(
        entry.variants.is_empty(),
        "BatchRequest is a struct, not an enum — snapshot says otherwise"
    );
    entry.fields.len() as u64
}

/// 4a. Integer key `field_count + PROBE_MARGIN` is out of range for
/// BatchRequest's fields (indices `0..=field_count-1`, deny_unknown_fields).
/// To prove the negative isolates the INDEX — not the integer-keyed form or
/// the map shape — the same byte-building code produces both frames:
/// first a fully valid NAMED-key control map (decoded positively with the
/// typed oracle below), then the byte-identical frame with EXACTLY ONE key
/// changed: the first map key becomes the probe integer.
///
/// The error KIND assertion targets the concrete rmp-serde error enum: every
/// serde-generated error (invalid_type/invalid_value/missing_field/custom)
/// funnels through `impl de::Error for Error::custom`
/// (rmp-serde-1.3.1/src/decode.rs:82-86) and lands in `Error::Syntax(String)`
/// — Display prints the message verbatim, so a single text assert still
/// distinguishes the identifier-visitor rejection ("field index", the derived
/// u64 out-of-range guard) from a missing-field or type error.
#[test]
fn batch_request_rejects_out_of_range_integer_key() {
    /// Far above any valid declaration index (13 here), far below the u64
    /// key space that could alias anything; added to the snapshot-derived
    /// field count to form the out-of-range probe index.
    const PROBE_MARGIN: u64 = 1_000;

    // Build the control frame, or — with `first_key_is_probe` — the SAME
    // frame with exactly one byte-level change: the first map key is the
    let build_frame = |first_key_is_probe: bool| -> Vec<u8> {
        let mut buf = Vec::new();
        write_map_len(&mut buf, 2).unwrap(); // keys: id + queries
        if first_key_is_probe {
            write_uint(
                &mut buf,
                snapshot_batch_request_field_count() + PROBE_MARGIN,
            )
            .unwrap();
        } else {
            write_str(&mut buf, "id").unwrap(); // BatchRequest 0
        }
        write_str(&mut buf, "batch-1").unwrap(); // QueryValue::Str
        write_str(&mut buf, "queries").unwrap(); // BatchRequest 7 (REQUIRED, no default)
        write_map_len(&mut buf, 0).unwrap(); // empty queries map
        buf
    };

    let probe_index = snapshot_batch_request_field_count() + PROBE_MARGIN;
    let buf = build_frame(true);
    let err = rmp_serde::from_slice::<BatchRequest>(&buf).expect_err(
        "an out-of-range integer key must not decode into the deny_unknown_fields struct",
    );
    // KIND: serde's derived field-identifier error is a `custom` error →
    // Error::Syntax (rmp-serde-1.3.1/src/decode.rs:82-86). Bind the message
    // for the panic text.
    let message = match &err {
        rmp_serde::decode::Error::Syntax(message) => message.clone(),
        other => {
            panic!("expected Error::Syntax for the out-of-range index {probe_index}, got {other:?}")
        }
    };
    // TEXT (single mechanism distinguisher): "field index" proves the derived
    // identifier visitor rejected the u64 as out of range — NOT a missing
    // field or a type error on the value.
    assert!(
        message.contains("field index"),
        "expected the field-index range rejection for index {probe_index}, got: {message}"
    );

    // Positive control: the byte-identical map with the STRING key decodes,
    // typed-equal to the named decode — proving the negative isolates the
    // out-of-range index alone.
    let control = build_frame(false);
    let expected = BatchRequest {
        id: QueryValue::Str("batch-1".to_string()),
        name: None,
        transactional: false,
        isolation: None,
        durability: None,
        index_visibility: None,
        visibility_wait: None,
        queries: TMap::default(),
        return_all: true,
        return_only: None,
        limits: BatchLimits::default(),
        interner_epochs: TMap::default(),
        result_encoding: ResultEncoding::Name,
    };
    assert_indexed_matches_named(&control, &expected);
}

/// 4a positive control: the same shape with a VALID key (0 = `id`, a
/// QueryValue) decodes fine — proving 4a isolates the out-of-range index,
/// not the integer-keyed form itself.
#[test]
fn batch_request_accepts_in_range_single_integer_key() {
    let mut buf = Vec::new();
    write_map_len(&mut buf, 2).unwrap(); // keys 0 and 7
    write_uint(&mut buf, 0).unwrap(); // 0: id
    write_str(&mut buf, "batch-ctl").unwrap(); // QueryValue::Str
    write_uint(&mut buf, 7).unwrap(); // 7: queries (REQUIRED — no #[serde(default)])
    write_map_len(&mut buf, 0).unwrap(); // empty queries map
    let expected = BatchRequest {
        id: QueryValue::Str("batch-ctl".to_string()),
        name: None,                            // key 1 absent → default
        transactional: false,                  // key 2 absent → default
        isolation: None,                       // key 3 absent → default
        durability: None,                      // key 4 absent → default
        index_visibility: None,                // key 5 absent → default
        visibility_wait: None,                 // key 6 absent → default
        queries: TMap::default(), // key 7 PRESENT as empty map (required field, no default)
        return_all: true,         // key 8 absent → default_return_all()
        return_only: None,        // key 9 absent → default
        limits: BatchLimits::default(), // key 10 absent → default
        interner_epochs: TMap::default(), // key 11 absent → default
        result_encoding: ResultEncoding::Name, // key 12 absent → default
    };
    assert_indexed_matches_named(&buf, &expected);
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
    // KIND: serde-generated error → Error::Syntax
    // (rmp-serde-1.3.1/src/decode.rs:82-86 `de::Error::custom`).
    let message = match &err {
        rmp_serde::decode::Error::Syntax(message) => message.clone(),
        other => panic!("expected Error::Syntax for the integer flat-map key, got {other:?}"),
    };
    // TEXT (single mechanism distinguisher): "invalid type" proves the
    // String-key buffering of the flattened BatchOp rejected the integer KEY
    // as a type error — not a missing-field error.
    assert!(
        message.contains("invalid type"),
        "expected an invalid-type error for the integer key, got: {message}"
    );
}

/// 4b positive control: the same single-entry shape with the STRING key
/// decodes into a real QueryEntry (BatchOp::Insert payload named, at the
/// TOP level of the entry map — QueryEntry flattens BatchOp). InsertOp
/// indices (ops/write/types.rs:84-109): 0 insert_into, 1 values,
/// 2 records_idmsgpack (default), 3 select (default).
#[test]
fn query_entry_named_key_decodes_insert_payload() {
    let mut buf = Vec::new();
    write_map_len(&mut buf, 2).unwrap();
    write_str(&mut buf, "insert_into").unwrap(); // InsertOp 0
    write_str(&mut buf, "users").unwrap(); // TableRef named = bare string
    write_str(&mut buf, "values").unwrap(); // InsertOp 1
    write_array_len(&mut buf, 0).unwrap(); // empty records list
    let expected = QueryEntry {
        op: BatchOp::Insert(InsertOp {
            insert_into: TableRef::new("users"),
            values: Vec::new(),
            records_idmsgpack: Vec::new(), // key 2 absent → default
            select: None,                  // key 3 absent → default
        }),
        return_result: true, // default_return() = true, omitted on the wire
        after: Vec::new(),
        when: None,
        abort_on_skip: false,
    };
    assert_indexed_matches_named(&buf, &expected);
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
    // KIND: serde-generated error → Error::Syntax
    // (rmp-serde-1.3.1/src/decode.rs:82-86 `de::Error::custom`).
    let message = match &err {
        rmp_serde::decode::Error::Syntax(message) => message.clone(),
        other => panic!("expected Error::Syntax for the integer tag key, got {other:?}"),
    };
    // TEXT (single mechanism distinguisher): "missing field" proves the
    // integer tag key was buffered and never matched — NOT a type error on
    // the tag value.
    assert!(
        message.contains("missing field") && message.contains("op"),
        "expected a missing-field `op` error, got: {message}"
    );
}

/// 4c positive control: the STRING tag decodes — minimal Ping case
/// (DbRequest::Ping is a unit variant, wire/db_message.rs:62-64; internally
/// tagged tag = "op", value "ping" via rename_all = "snake_case").
#[test]
fn db_request_string_tag_ping_decodes() {
    let mut buf = Vec::new();
    write_map_len(&mut buf, 1).unwrap();
    write_str(&mut buf, "op").unwrap();
    write_str(&mut buf, "ping").unwrap();
    let decoded: DbRequest = rmp_serde::from_slice(&buf).expect("string tag decodes");
    assert!(
        matches!(decoded, DbRequest::Ping),
        "expected DbRequest::Ping, got {decoded:?}"
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

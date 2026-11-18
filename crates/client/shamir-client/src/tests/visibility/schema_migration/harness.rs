//! Shared scripted-server harness for the application-schema migration
//! regression tests: a `Client::for_test` on a framed tokio duplex whose
//! peer reads the REAL outbound request frames (typed envelope decode, no
//! manual request JSON), asserts on the decoded typed admin operations and
//! answers with server-faithful fixtures (`DbResponse::Batch` whose single
//! direct record mirrors the server's `admin_result` / `status_value`
//! encoders — `Bin` checksums, snake_case codes, additive
//! `in_flight.in_doubt_reason`). Every await is bounded by [`bound`]; there
//! are no sleeps and no racy assertions.

use std::future::Future;
use std::time::Duration;

use shamir_connect::common::envelope::{RequestEnvelope, ResponseEnvelope};
use shamir_query_builder::ddl;
use shamir_query_builder::wire::schema_migration::SchemaMigration;
use shamir_query_types::admin::types::schema_migration::{
    SchemaMigrationAdminOp, SchemaMigrationBody, SchemaMigrationTarget,
};
use shamir_query_types::batch::{BatchOp, BatchResponse, QueryEntry};
use shamir_query_types::read::{QueryRecord, QueryResult};
use shamir_query_types::wire::{DbRequest, DbResponse};
use shamir_transport_tcp::framing::write_frame;
use shamir_types::types::record_id::RecordId;
use shamir_types::types::value::QueryValue;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::client::LifecycleHooks;
use crate::schema_migration::ApplicationMigrationSet;
use crate::Client;

/// No-hang bound: a revert that makes any exchange below hang fails the
/// test with a timeout instead of blocking CI.
pub(crate) const BOUND: Duration = Duration::from_secs(30);

/// The serving database identity every test targets.
pub(crate) fn target() -> SchemaMigrationTarget {
    SchemaMigrationTarget {
        database_identity: RecordId([7; 16]),
    }
}

pub(crate) const DB: &str = "migration-test-db";

/// Bound any await (client call or harness read) so a regression can never
/// hang CI.
pub(crate) async fn bound<F: Future>(fut: F) -> F::Output {
    tokio::time::timeout(BOUND, fut)
        .await
        .expect("bounded await must resolve, not hang")
}

/// Read exactly one framed [`RequestEnvelope`] and decode the typed
/// [`DbRequest`]. `None` means the peer hit EOF (deterministic
/// response-drop / no-replay probe), never a poll.
pub(crate) async fn read_request<R: AsyncReadExt + Unpin>(
    reader: &mut R,
) -> Option<(Option<u32>, DbRequest)> {
    let mut prefix = [0u8; 4];
    reader.read_exact(&mut prefix).await.ok()?;
    let len = u32::from_be_bytes(prefix) as usize;
    assert!(len <= 1024 * 1024, "bounded request frame from the client");
    let mut frame = vec![0; len];
    reader
        .read_exact(&mut frame)
        .await
        .expect("request frame body");
    let envelope = RequestEnvelope::from_msgpack(&frame).expect("request envelope");
    let request: DbRequest = rmp_serde::from_slice(&envelope.req).expect("typed DbRequest");
    Some((envelope.request_id, request))
}

/// Extract the single typed `application_schema` admin operation of a
/// scripted request together with its alias — the ACTUAL wire shape the
/// client emitted (assertions decode this; nothing hand-written).
pub(crate) fn single_admin_op(request: &DbRequest) -> (String, SchemaMigrationAdminOp) {
    let DbRequest::Execute { batch, .. } = request else {
        panic!("expected DbRequest::Execute, got {request:?}")
    };
    assert_eq!(
        batch.queries.len(),
        1,
        "each migration call sends exactly one admin operation"
    );
    let (alias, entry): (&String, &QueryEntry) =
        batch.queries.iter().next().expect("single query entry");
    let BatchOp::ApplicationSchema(op) = &entry.op else {
        panic!("expected a typed ApplicationSchema op, got {:?}", entry.op)
    };
    (alias.clone(), op.clone())
}

/// True when the scripted request is the typed status operation.
pub(crate) fn is_status(op: &SchemaMigrationAdminOp) -> bool {
    matches!(op, SchemaMigrationAdminOp::ApplicationSchemaStatus(_))
}

/// Write one framed response envelope carrying a typed [`DbResponse`].
pub(crate) async fn respond<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    rid: Option<u32>,
    response: DbResponse,
) {
    let payload = rmp_serde::to_vec_named(&response).expect("encode DbResponse");
    let envelope = ResponseEnvelope::ok(rid, payload);
    write_frame(writer, &envelope.to_msgpack().expect("encode envelope"))
        .await
        .expect("write response frame");
}

/// The real old-server refusal: decode-level rejection surfaced as
/// `DbResponse::Error` with the wire code and the old decoder's text.
pub(crate) async fn respond_old_server_decoder_refusal<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    rid: Option<u32>,
) {
    respond(
        writer,
        rid,
        DbResponse::Error {
            code: String::from("invalid_request"),
            message: String::from("Unknown operation type"),
        },
    )
    .await;
}

/// A general server-level `DbResponse::Error` with an exact code and
/// message (e.g. the `query`-coded `BatchError::QueryError` Display, or
/// the post-execution `result_too_large` response replacement).
pub(crate) async fn respond_db_error<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    rid: Option<u32>,
    code: &str,
    message: &str,
) {
    respond(
        writer,
        rid,
        DbResponse::Error {
            code: code.to_string(),
            message: message.to_string(),
        },
    )
    .await;
}

/// A `BatchResponse` in the server's `admin_result` convention: one direct
/// record under `alias`, nothing else (the migration client reads the
/// direct record by alias).
pub(crate) fn admin_batch_response(alias: &str, payload: QueryValue) -> BatchResponse {
    let mut results = shamir_collections::new_map();
    results.insert(
        alias.to_string(),
        QueryResult {
            records: vec![QueryRecord::Direct(payload)],
            ..QueryResult::default()
        },
    );
    BatchResponse {
        id: QueryValue::Null,
        results,
        execution_plan: Vec::new(),
        edge_provenance: shamir_collections::new_map(),
        execution_time_us: 0,
        transaction: None,
        interner_delta: shamir_collections::new_map(),
    }
}

pub(crate) async fn respond_admin_payload<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    rid: Option<u32>,
    alias: &str,
    payload: QueryValue,
) {
    respond(
        writer,
        rid,
        DbResponse::Batch {
            response: admin_batch_response(alias, payload),
        },
    )
    .await;
}

// ── Status fixtures: mirror the server's `status_value` encoder exactly ──
// (head plus the R1c step 3a additive top-level `fence_generation` and the
// whole `applied` prefix, optional unresolved event, `Bin` checksums,
// snake_case codes, additive `in_flight.in_doubt_reason`, optional typed
// `error` code). Old-server fixtures omit BOTH new keys verbatim.

fn target_value() -> QueryValue {
    let mut map = shamir_collections::new_map();
    map.insert(
        String::from("database_identity"),
        QueryValue::Str(target().database_identity.to_string()),
    );
    QueryValue::Map(map)
}

fn metadata_value(body: &SchemaMigrationBody) -> QueryValue {
    let mut map = shamir_collections::new_map();
    map.insert(
        String::from("id"),
        QueryValue::Str(body.metadata.id.clone()),
    );
    map.insert(
        String::from("ordinal"),
        QueryValue::Int(body.metadata.ordinal as i64),
    );
    map.insert(
        String::from("display_name"),
        QueryValue::Str(body.metadata.display_name.clone()),
    );
    map.insert(
        String::from("canonical_version"),
        QueryValue::Int(body.metadata.canonical_version as i64),
    );
    QueryValue::Map(map)
}

/// One journal-head status record WITHOUT the R1c step 3a keys — the
/// explicit OLD-server compatibility fixture (a populated head that is
/// missing both `fence_generation` and `applied`). `validation` is the
/// wire string of `SchemaMigrationChecksumValidation`.
pub(crate) fn status_value(
    last_applied_id: Option<&str>,
    last_applied_ordinal: u64,
    last_applied_checksum: Option<[u8; 32]>,
    validation: &str,
    in_flight: Option<QueryValue>,
) -> QueryValue {
    let mut value = shamir_collections::new_map();
    value.insert(String::from("protocol_version"), QueryValue::Int(1));
    value.insert(String::from("target"), target_value());
    if let Some(id) = last_applied_id {
        value.insert(String::from("last_applied_id"), QueryValue::Str(id.into()));
    }
    value.insert(
        String::from("last_applied_ordinal"),
        QueryValue::Int(last_applied_ordinal as i64),
    );
    if let Some(checksum) = last_applied_checksum {
        // Server convention: checksums travel as MessagePack `bin`.
        value.insert(
            String::from("last_applied_checksum"),
            QueryValue::Bin(checksum.to_vec()),
        );
    }
    value.insert(
        String::from("checksum_validation"),
        QueryValue::Str(validation.into()),
    );
    if let Some(flight) = in_flight {
        value.insert(String::from("in_flight"), flight);
    }
    QueryValue::Map(value)
}

/// One `applied` list entry: id, ordinal, checksum as MessagePack `Bin` —
/// exactly the server's `applied_entries` encoder.
pub(crate) fn applied_entry_value(id: &str, ordinal: u64, checksum: [u8; 32]) -> QueryValue {
    let mut map = shamir_collections::new_map();
    map.insert(String::from("id"), QueryValue::Str(id.into()));
    map.insert(String::from("ordinal"), QueryValue::Int(ordinal as i64));
    map.insert(String::from("checksum"), QueryValue::Bin(checksum.to_vec()));
    QueryValue::Map(map)
}

/// A populated status record the R1c step 3a server emits: the head fields
/// come from the LAST entry, the top-level `fence_generation` is the
/// highest journal generation (always present, `0` only for an empty
/// journal) and the whole `applied` prefix travels in ordinal order (the
/// key is omitted only when the list is empty). `validation` is the wire
/// string of `SchemaMigrationChecksumValidation`.
pub(crate) fn populated_status_value(
    entries: &[(&str, u64, [u8; 32])],
    fence_generation: u64,
    validation: &str,
) -> QueryValue {
    let (head_id, head_ordinal, head_checksum) = entries
        .last()
        .map(|(id, ordinal, checksum)| (Some(*id), *ordinal, Some(*checksum)))
        .unwrap_or((None, 0, None));
    let mut value = status_value(head_id, head_ordinal, head_checksum, validation, None);
    let QueryValue::Map(ref mut map) = value else {
        panic!("status fixture is a map")
    };
    map.insert(
        String::from("fence_generation"),
        QueryValue::Int(fence_generation as i64),
    );
    if !entries.is_empty() {
        map.insert(
            String::from("applied"),
            QueryValue::List(
                entries
                    .iter()
                    .map(|(id, ordinal, checksum)| applied_entry_value(id, *ordinal, *checksum))
                    .collect(),
            ),
        );
    }
    value
}

/// An empty journal (ordinal 0, no history, no unresolved event): the
/// initial baseline. The new wire always carries the top-level fence
/// generation (`0`); the `applied` list may be omitted when empty. With no
/// applied history there is no persisted checksum the server could have
/// verified, so the wire reports `not_checked`, not `valid`.
pub(crate) fn empty_journal_value() -> QueryValue {
    populated_status_value(&[], 0, "not_checked")
}

/// A verified applied head: single-entry prefix, the fence generation one
/// past the head's acquire — the generation the NEXT fresh apply expects.
/// Valid for a single-entry journal only (head ordinal 1): for longer
/// prefixes use [`populated_status_value`] with an entry per ordinal.
pub(crate) fn head_value(head_id: &str, head_ordinal: u64, checksum: [u8; 32]) -> QueryValue {
    debug_assert_eq!(head_ordinal, 1, "single-entry head fixture only");
    populated_status_value(
        &[(head_id, head_ordinal, checksum)],
        head_ordinal + 1,
        "valid",
    )
}

/// The explicit OLD-server populated head: all legacy fields, BOTH new
/// keys absent — the compatibility fixture.
pub(crate) fn old_head_value(head_id: &str, head_ordinal: u64, checksum: [u8; 32]) -> QueryValue {
    status_value(Some(head_id), head_ordinal, Some(checksum), "valid", None)
}

/// Attach the typed `error` code record to a status payload (the stable
/// §5.3 code string the handler echoes, e.g. `schema_migration_in_doubt`).
pub(crate) fn with_error(mut status: QueryValue, code: &str) -> QueryValue {
    let QueryValue::Map(ref mut map) = status else {
        panic!("status fixture is a map")
    };
    map.insert(String::from("error"), QueryValue::Str(code.into()));
    status
}

/// Attach the unresolved-event map to a status payload.
pub(crate) fn with_in_flight(mut status: QueryValue, flight: QueryValue) -> QueryValue {
    let QueryValue::Map(ref mut map) = status else {
        panic!("status fixture is a map")
    };
    map.insert(String::from("in_flight"), flight);
    status
}

/// The additive unresolved-event map: id, ordinal, optional operation
/// index, fence generation, status and (for in-doubt/failed events) the
/// server's `in_doubt_reason` — exactly the fields the handler emits.
pub(crate) fn in_flight_value(
    migration_id: &str,
    ordinal: u64,
    operation_index: Option<u32>,
    fence_generation: u64,
    status: &str,
    in_doubt_reason: Option<&str>,
) -> QueryValue {
    let mut map = shamir_collections::new_map();
    map.insert(
        String::from("migration_id"),
        QueryValue::Str(migration_id.into()),
    );
    map.insert(String::from("ordinal"), QueryValue::Int(ordinal as i64));
    if let Some(index) = operation_index {
        map.insert(
            String::from("operation_index"),
            QueryValue::Int(index as i64),
        );
    }
    map.insert(
        String::from("fence_generation"),
        QueryValue::Int(fence_generation as i64),
    );
    map.insert(String::from("status"), QueryValue::Str(status.into()));
    if let Some(reason) = in_doubt_reason {
        map.insert(
            String::from("in_doubt_reason"),
            QueryValue::Str(reason.into()),
        );
    }
    QueryValue::Map(map)
}

// ── Apply / plan fixtures (typed DTO fields, `Bin` checksums) ─────────────

/// The apply result record the server answers with.
pub(crate) fn apply_result_value(
    migration_id: &str,
    ordinal: u64,
    checksum: [u8; 32],
    status: &str,
    fence_generation: Option<u64>,
) -> QueryValue {
    let mut value = shamir_collections::new_map();
    value.insert(String::from("protocol_version"), QueryValue::Int(1));
    value.insert(
        String::from("migration_id"),
        QueryValue::Str(migration_id.into()),
    );
    value.insert(String::from("ordinal"), QueryValue::Int(ordinal as i64));
    value.insert(String::from("checksum"), QueryValue::Bin(checksum.to_vec()));
    value.insert(String::from("status"), QueryValue::Str(status.into()));
    value.insert(String::from("applied_at_unix_micros"), QueryValue::Int(42));
    if let Some(generation) = fence_generation {
        value.insert(
            String::from("fence_generation"),
            QueryValue::Int(generation as i64),
        );
    }
    QueryValue::Map(value)
}

/// A minimal plan result for `body` under `predecessor` (checksum `Bin`,
/// no warnings, one planned operation).
pub(crate) fn plan_result_value(
    body: &SchemaMigrationBody,
    checksum: [u8; 32],
    predecessor: QueryValue,
) -> QueryValue {
    let mut planned = shamir_collections::new_map();
    planned.insert(String::from("operation_index"), QueryValue::Int(0));
    planned.insert(
        String::from("operation_kind"),
        QueryValue::Str(String::from("create_index")),
    );
    planned.insert(String::from("target"), QueryValue::Str(String::from("t")));
    let mut value = shamir_collections::new_map();
    value.insert(String::from("protocol_version"), QueryValue::Int(1));
    value.insert(String::from("migration"), metadata_value(body));
    value.insert(String::from("checksum"), QueryValue::Bin(checksum.to_vec()));
    value.insert(String::from("predecessor"), predecessor);
    value.insert(
        String::from("operations"),
        QueryValue::List(vec![QueryValue::Map(planned)]),
    );
    value.insert(String::from("warnings"), QueryValue::List(Vec::new()));
    QueryValue::Map(value)
}

pub(crate) fn predecessor_value(
    migration_id: Option<&str>,
    ordinal: u64,
    checksum: [u8; 32],
) -> QueryValue {
    let mut map = shamir_collections::new_map();
    if let Some(id) = migration_id {
        map.insert(String::from("migration_id"), QueryValue::Str(id.into()));
    }
    map.insert(String::from("ordinal"), QueryValue::Int(ordinal as i64));
    map.insert(String::from("checksum"), QueryValue::Bin(checksum.to_vec()));
    QueryValue::Map(map)
}

// ── Local set construction: typed builders only ───────────────────────────

/// One valid version-1 body built from the typed DDL builders (an admitted
/// `create_index` operation).
pub(crate) fn body(id: &str, ordinal: u64) -> SchemaMigrationBody {
    SchemaMigration::new(id, ordinal, format!("migration {ordinal}"))
        .op(ddl::create_index(format!("ix_{id}"), "t").build())
        .expect("create_index is an admitted migration operation")
        .build()
        .expect("typed body validates")
}

/// A valid contiguous set from typed builder bodies.
pub(crate) fn migration_set(ids: &[&str]) -> ApplicationMigrationSet {
    ApplicationMigrationSet::new(
        ids.iter()
            .enumerate()
            .map(|(index, id)| body(id, index as u64 + 1))
            .collect(),
    )
    .expect("contiguous one-based ordinals with unique ids validate")
}

/// Spawn the client half on a fresh framed duplex; the caller scripts the
/// peer halves.
pub(crate) fn client_on(
    reader: impl tokio::io::AsyncRead + Unpin + Send + 'static,
    writer: impl tokio::io::AsyncWrite + Unpin + Send + 'static,
) -> Client {
    Client::for_test(reader, writer, LifecycleHooks::default())
}

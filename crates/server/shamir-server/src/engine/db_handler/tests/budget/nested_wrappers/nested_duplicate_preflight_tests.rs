//! Raw-wire duplicate nested aliases must be capped before BatchOp buffers
//! their op into QueryValue and typed decoding collapses duplicate keys.

use serde::Serialize;
use shamir_connect::common::types::{BindingMode, TransportKind};
use shamir_connect::server::conn_services::ConnectionServices;
use shamir_connect::server::dispatch::RequestHandler;
use shamir_connect::server::session::{Session, SessionPermissions};
use shamir_db::ShamirDb;
use shamir_query_builder::batch::Batch;
use shamir_query_builder::write::{self, doc};
use shamir_query_builder::Query;
use shamir_query_types::wire::{DbRequest, DbResponse, CURRENT_QUERY_LANG_VERSION};
use std::sync::Arc;

use crate::db_handler::handler::ShamirDbHandler;

fn session() -> Session {
    Session::new(
        [0xAB; 16],
        "alice".into(),
        SessionPermissions::new(false, false, vec!["read_write".into()]),
        TransportKind::Tcp,
        BindingMode::TlsExporter,
        [0; 32],
        0,
    )
}

struct DuplicateMap(usize);
impl Serialize for DuplicateMap {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_map((0..self.0).map(|_| ("alias", Query::from("t").build())))
    }
}

#[derive(Serialize)]
struct NestedBatch {
    id: &'static str,
    queries: DuplicateMap,
}
#[derive(Serialize)]
struct NestedOp {
    batch: NestedBatch,
}
#[derive(Serialize)]
struct OuterBatch {
    id: &'static str,
    queries: std::collections::BTreeMap<&'static str, NestedOp>,
}
#[derive(Serialize)]
struct RawExecute {
    op: &'static str,
    query_version: u32,
    db: &'static str,
    batch: OuterBatch,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rv_collections_da7d2cd3_builder_insert_cap_is_validated_before_decode() {
    let mut batch = Batch::new();
    batch.id("insert-cap");
    let mut row = doc();
    for index in 0..1001 {
        row = row
            .set(format!("field_{index}"), index as i64)
            .expect("add document field");
    }
    let insert = write::insert("t").row(row).build();
    batch.op("insert", insert);
    let batch_request = batch
        .to_request_via_msgpack()
        .expect("batch request encoding");
    #[derive(Serialize)]
    struct BuilderExecute<'a> {
        op: &'static str,
        query_version: u32,
        db: &'static str,
        batch: &'a shamir_query_types::batch::BatchRequest,
    }
    let bytes = rmp_serde::to_vec_named(&BuilderExecute {
        op: "execute",
        query_version: CURRENT_QUERY_LANG_VERSION,
        db: "prod",
        batch: &batch_request,
    })
    .expect("serialize real execute request");

    assert!(shamir_query_types::validate_request_alias_caps(&bytes).is_ok());
    let decoded = rmp_serde::from_slice::<DbRequest>(&bytes).expect("typed decode");
    assert!(matches!(decoded, DbRequest::Execute { .. }));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rv_collections_da7d2cd3_handler_preflights_nested_duplicate_map_before_decode() {
    let db = ShamirDb::init_memory().await.expect("init shamir");
    let handler = ShamirDbHandler::new(Arc::new(db));
    let mut nested = std::collections::BTreeMap::new();
    nested.insert(
        "sub",
        NestedOp {
            batch: NestedBatch {
                id: "nested",
                queries: DuplicateMap(1001),
            },
        },
    );
    let complete = rmp_serde::to_vec_named(&RawExecute {
        op: "execute",
        query_version: CURRENT_QUERY_LANG_VERSION,
        db: "prod",
        batch: OuterBatch {
            id: "outer",
            queries: nested,
        },
    })
    .expect("encode request");
    let mut truncated = complete;
    truncated.pop();

    let response = handler
        .handle(&session(), &truncated, &ConnectionServices::without_push(0))
        .await
        .expect("invalid request returns encoded error");
    let response: DbResponse = rmp_serde::from_slice(&response).expect("decode response");
    assert!(matches!(response, DbResponse::Error { code, message }
        if code == "invalid_request" && message.contains("decode cap")));

    // Counterfactual: absent preflight, typed duplicate-key collapse leaves a
    // valid under-cap map; truncation then reports EOF/different decode error.
    assert!(rmp_serde::from_slice::<DbRequest>(&truncated).is_err());
}

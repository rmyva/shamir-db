//! GetRecord identity re-check tests around the pre-acquisition pause seam.

use std::sync::Arc;
use std::time::Duration;

use shamir_connect::common::time::UnixNanos;
use shamir_connect::common::types::{BindingMode, TransportKind};
use shamir_connect::server::conn_services::ConnectionServices;
use shamir_connect::server::dispatch::RequestHandler;
use shamir_connect::server::session::{Session, SessionPermissions};
use shamir_db::access::{principal64, Actor};
use shamir_db::engine::repo::{BoxRepoFactory, RepoConfig};
use shamir_db::engine::table::TableConfig;
use shamir_db::ShamirDb;
use shamir_query_builder::batch::Batch;
use shamir_query_builder::doc;
use shamir_query_builder::wire::admin::get_record;
use shamir_query_builder::write::insert;
use shamir_query_types::read::QueryRecord;
use shamir_query_types::wire::{DbRequest, DbResponse};
use shamir_types::types::record_id::RecordId;

use crate::byte_budget::ByteBudget;
use crate::db_handler::config::QueryLimitsCap;
use crate::db_handler::handler::ShamirDbHandler;

const CALL_TIMEOUT: Duration = Duration::from_secs(120);
const ALICE_SID: [u8; 32] = [0xAA; 32];
const SEEDED_V: i64 = 42;
const REPLACEMENT_V: i64 = 99;
const REGISTRATION_CHANGED: &str = "the authorized table registration changed during the request";

fn alice_session() -> Session {
    let mut session = Session::new(
        [0xAB; 16],
        "alice".into(),
        SessionPermissions::from_roles(vec!["read_write".into()]),
        TransportKind::Tcp,
        BindingMode::TlsExporter,
        [0; 32],
        UnixNanos::now().as_u64(),
    );
    session.session_id = ALICE_SID;
    session
}

async fn build_handler() -> (ShamirDbHandler, RecordId) {
    let db = tokio::time::timeout(CALL_TIMEOUT, ShamirDb::init_memory())
        .await
        .expect("init must complete within the 120s bound")
        .expect("init ShamirDb");
    let owner = Actor::User(principal64([0xAB; 16]));
    tokio::time::timeout(CALL_TIMEOUT, db.create_db_as("app", owner.clone()))
        .await
        .expect("create_db must complete within the 120s bound")
        .expect("create db");
    let config =
        RepoConfig::new("main", BoxRepoFactory::in_memory()).add_table(TableConfig::new("items"));
    tokio::time::timeout(CALL_TIMEOUT, db.add_repo_as("app", config, owner))
        .await
        .expect("add_repo must complete within the 120s bound")
        .expect("add repo");
    let handler = ShamirDbHandler::new(Arc::new(db))
        .with_query_limits(QueryLimitsCap::UNLIMITED)
        .with_byte_budget(ByteBudget::unbounded());
    let mut batch = Batch::new();
    batch.insert(
        "seed",
        insert("items").row(doc! { "id" => "seed", "v" => SEEDED_V }),
    );
    let response = tokio::time::timeout(
        CALL_TIMEOUT,
        handler
            .db()
            .execute_as(Actor::System, "app", &batch.build()),
    )
    .await
    .expect("seed insert must complete within the 120s bound")
    .expect("seed insert must succeed");
    let seeded = match response.results["seed"].records.first() {
        Some(QueryRecord::Inserted(record)) => record.id.expect("inserted storage id"),
        other => panic!("expected inserted row, got {other:?}"),
    };
    (handler, seeded)
}

fn get_record_req(id: RecordId) -> DbRequest {
    get_record("app", "main", "items", id)
}

async fn send(handler: &ShamirDbHandler, session: &Session, request: DbRequest) -> DbResponse {
    let bytes = rmp_serde::to_vec_named(&request).expect("encode request");
    let conn = ConnectionServices::without_push(0);
    let response = tokio::time::timeout(CALL_TIMEOUT, handler.handle(session, &bytes, &conn))
        .await
        .expect("handle must complete within the 120s bound")
        .expect("handle must succeed");
    rmp_serde::from_slice(&response).expect("decode response")
}

#[tokio::test]
async fn get_record_refuses_when_the_table_is_replaced_between_precheck_and_acquisition() {
    let (handler, seeded) = build_handler().await;
    let reached = Arc::new(tokio::sync::Barrier::new(2));
    let resume = Arc::new(tokio::sync::Barrier::new(2));
    let handler = handler.with_get_record_identity_pause(Arc::clone(&reached), Arc::clone(&resume));
    let session = alice_session();
    let (response, ()) = tokio::join!(
        async { send(&handler, &session, get_record_req(seeded)).await },
        async {
            tokio::time::timeout(CALL_TIMEOUT, reached.wait())
                .await
                .expect("request reaches pause within bound");
            // Reaching this barrier proves the request reached its one-shot pause.
            let db = handler.db().get_db("app").expect("app database");
            let old_table =
                tokio::time::timeout(CALL_TIMEOUT, handler.db().get_table("app", "main", "items"))
                    .await
                    .expect("old table acquisition must complete within bound")
                    .expect("old table must exist");
            let row = tokio::time::timeout(CALL_TIMEOUT, old_table.get(seeded))
                .await
                .expect("row lookup must complete within bound")
                .expect("seeded row must exist");
            tokio::time::timeout(CALL_TIMEOUT, db.drop_table("main", "items"))
                .await
                .expect("drop must complete within bound")
                .expect("drop must succeed");
            tokio::time::timeout(CALL_TIMEOUT, db.create_table("main", "items"))
                .await
                .expect("create must complete within bound")
                .expect("create must succeed");
            let replacement =
                tokio::time::timeout(CALL_TIMEOUT, handler.db().get_table("app", "main", "items"))
                    .await
                    .expect("replacement acquisition must complete within bound")
                    .expect("replacement must exist");
            assert!(replacement
                .set(seeded, &row)
                .await
                .expect("reseed replacement"));
            tokio::time::timeout(CALL_TIMEOUT, resume.wait())
                .await
                .expect("resume within bound");
        },
    );
    match response {
        DbResponse::Error { code, message } => {
            assert_eq!(code, "access_denied");
            assert_eq!(message, REGISTRATION_CHANGED);
        }
        other => panic!("replacement row must not be returned, got {other:?}"),
    }
    let replacement_response = send(&handler, &alice_session(), get_record_req(seeded)).await;
    match replacement_response {
        DbResponse::Record { record } => {
            assert_eq!(record.get_value_i64("v"), Some(SEEDED_V));
            assert_ne!(record.get_value_i64("v"), Some(REPLACEMENT_V));
        }
        other => panic!("replacement must contain the seeded row, got {other:?}"),
    }
}

#[tokio::test]
async fn get_record_identity_pause_positive_control_returns_the_seeded_row() {
    let (handler, seeded) = build_handler().await;
    let reached = Arc::new(tokio::sync::Barrier::new(2));
    let resume = Arc::new(tokio::sync::Barrier::new(2));
    let handler = handler.with_get_record_identity_pause(Arc::clone(&reached), Arc::clone(&resume));
    let session = alice_session();
    let (response, ()) = tokio::join!(
        async { send(&handler, &session, get_record_req(seeded)).await },
        async {
            tokio::time::timeout(CALL_TIMEOUT, reached.wait())
                .await
                .expect("request reaches pause within bound");
            tokio::time::timeout(CALL_TIMEOUT, resume.wait())
                .await
                .expect("resume within bound");
        },
    );
    match response {
        DbResponse::Record { record } => {
            assert_eq!(record.get_value_i64("v"), Some(SEEDED_V));
        }
        other => panic!("expected seeded Record, got {other:?}"),
    }
}

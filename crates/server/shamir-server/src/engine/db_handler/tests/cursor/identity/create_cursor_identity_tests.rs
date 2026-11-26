//! CreateCursor identity re-check tests around post-acquisition pause seam.

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
use shamir_query_builder::cursor::create_cursor;
use shamir_query_builder::doc;
use shamir_query_builder::query::Query;
use shamir_query_builder::write::insert;
use shamir_query_types::wire::DbResponse;

use crate::byte_budget::ByteBudget;
use crate::db_handler::config::CursorLimitsCap;
use crate::db_handler::handler::ShamirDbHandler;

const CALL_TIMEOUT: Duration = Duration::from_secs(120);
const REGISTRATION_CHANGED: &str = "the authorized table registration changed during the request";
const SEEDED_V: i64 = 42;

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
    session.session_id = [0xAA; 32];
    session
}

async fn build_handler() -> ShamirDbHandler {
    let db = tokio::time::timeout(CALL_TIMEOUT, ShamirDb::init_memory())
        .await
        .expect("init within bound")
        .expect("init ShamirDb");
    let owner = Actor::User(principal64([0xAB; 16]));
    tokio::time::timeout(CALL_TIMEOUT, db.create_db_as("app", owner.clone()))
        .await
        .expect("create db within bound")
        .expect("create db");
    let config =
        RepoConfig::new("main", BoxRepoFactory::in_memory()).add_table(TableConfig::new("items"));
    tokio::time::timeout(CALL_TIMEOUT, db.add_repo_as("app", config, owner.clone()))
        .await
        .expect("add repo within bound")
        .expect("add repo");
    let handler = ShamirDbHandler::new(Arc::new(db))
        .with_cursor_limits(CursorLimitsCap::UNLIMITED)
        .with_byte_budget(ByteBudget::unbounded());
    let mut batch = Batch::new();
    batch.insert(
        "seed",
        insert("items").row(doc! { "id" => "seed", "v" => SEEDED_V }),
    );
    tokio::time::timeout(
        CALL_TIMEOUT,
        handler.db().execute_as(owner, "app", &batch.build()),
    )
    .await
    .expect("seed within bound")
    .expect("seed row");
    handler
}

async fn send(handler: &ShamirDbHandler, session: &Session) -> DbResponse {
    let request = create_cursor("app", Query::with_repo("main", "items").build(), 10);
    let bytes = rmp_serde::to_vec_named(&request).expect("encode request");
    let conn = ConnectionServices::without_push(0);
    let response = tokio::time::timeout(CALL_TIMEOUT, handler.handle(session, &bytes, &conn))
        .await
        .expect("handle within bound")
        .expect("handle succeeds");
    rmp_serde::from_slice(&response).expect("decode response")
}

#[tokio::test]
async fn create_cursor_refuses_when_the_table_is_replaced_during_creation() {
    let handler = build_handler().await;
    let reached = Arc::new(tokio::sync::Barrier::new(2));
    let resume = Arc::new(tokio::sync::Barrier::new(2));
    let handler =
        handler.with_create_cursor_identity_pause(Arc::clone(&reached), Arc::clone(&resume));
    let session = alice_session();
    let (response, ()) = tokio::join!(async { send(&handler, &session).await }, async {
        tokio::time::timeout(CALL_TIMEOUT, reached.wait())
            .await
            .expect("request reaches pause within bound");
        let db = handler.db().get_db("app").expect("app database");
        tokio::time::timeout(CALL_TIMEOUT, db.drop_table("main", "items"))
            .await
            .expect("drop within bound")
            .expect("drop succeeds");
        tokio::time::timeout(CALL_TIMEOUT, db.create_table("main", "items"))
            .await
            .expect("recreate within bound")
            .expect("recreate succeeds");
        tokio::time::timeout(CALL_TIMEOUT, resume.wait())
            .await
            .expect("resume within bound");
    },);
    match response {
        DbResponse::Error { code, message } => {
            assert_eq!(code, "access_denied");
            // The cursor handlers render a coded `BatchError` through its
            // `Display` (empty alias), unlike `GetRecord`'s bare message.
            assert_eq!(
                message,
                format!("Query '' failed [access_denied]: {REGISTRATION_CHANGED}")
            );
        }
        other => panic!("replacement must be refused without a cursor id: {other:?}"),
    }
}

#[tokio::test]
async fn create_cursor_positive_control_still_opens_when_unchanged() {
    let handler = build_handler().await;
    let reached = Arc::new(tokio::sync::Barrier::new(2));
    let resume = Arc::new(tokio::sync::Barrier::new(2));
    let handler =
        handler.with_create_cursor_identity_pause(Arc::clone(&reached), Arc::clone(&resume));
    let session = alice_session();
    let (response, ()) = tokio::join!(async { send(&handler, &session).await }, async {
        tokio::time::timeout(CALL_TIMEOUT, reached.wait())
            .await
            .expect("request reaches pause within bound");
        tokio::time::timeout(CALL_TIMEOUT, resume.wait())
            .await
            .expect("resume within bound");
    },);
    match response {
        DbResponse::CursorPage { page, .. } => {
            assert_eq!(page.records.len(), 1);
            assert_eq!(page.records[0].get_value_i64("v"), Some(SEEDED_V));
        }
        other => panic!("unchanged table must open cursor: {other:?}"),
    }
}

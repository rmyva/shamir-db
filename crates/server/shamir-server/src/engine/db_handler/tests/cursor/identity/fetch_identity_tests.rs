//! FetchNext identity re-check tests around the post-acquisition pause seam.

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
use shamir_query_builder::cursor::{create_cursor, fetch_next};
use shamir_query_builder::doc;
use shamir_query_builder::query::Query;
use shamir_query_builder::write::insert;
use shamir_query_types::wire::{CursorId, DbResponse};

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
        "seed-1",
        insert("items").row(doc! { "id" => "seed-1", "v" => SEEDED_V }),
    );
    batch.insert(
        "seed-2",
        insert("items").row(doc! { "id" => "seed-2", "v" => 43i64 }),
    );
    tokio::time::timeout(
        CALL_TIMEOUT,
        handler.db().execute_as(owner, "app", &batch.build()),
    )
    .await
    .expect("seed within bound")
    .expect("seed rows");
    handler
}

async fn send(
    handler: &ShamirDbHandler,
    session: &Session,
    request: shamir_query_types::wire::DbRequest,
) -> DbResponse {
    let bytes = rmp_serde::to_vec_named(&request).expect("encode request");
    let conn = ConnectionServices::without_push(0);
    let response = tokio::time::timeout(CALL_TIMEOUT, handler.handle(session, &bytes, &conn))
        .await
        .expect("handle within bound")
        .expect("handle succeeds");
    rmp_serde::from_slice(&response).expect("decode response")
}

async fn create_live_cursor(handler: &ShamirDbHandler, session: &Session) -> CursorId {
    match send(
        handler,
        session,
        create_cursor("app", Query::with_repo("main", "items").build(), 1),
    )
    .await
    {
        DbResponse::CursorPage {
            cursor_id,
            has_more: true,
            page,
            ..
        } => {
            assert_eq!(page.records.len(), 1);
            cursor_id
        }
        other => panic!("expected first cursor page with more rows, got {other:?}"),
    }
}

#[tokio::test]
async fn fetch_next_refuses_when_the_table_is_replaced_during_the_fetch() {
    let handler = build_handler().await;
    let session = alice_session();
    let cursor_id = create_live_cursor(&handler, &session).await;
    let reached = Arc::new(tokio::sync::Barrier::new(2));
    let resume = Arc::new(tokio::sync::Barrier::new(2));
    let handler = handler.with_fetch_identity_pause(Arc::clone(&reached), Arc::clone(&resume));
    let (response, ()) = tokio::join!(
        async { send(&handler, &session, fetch_next(cursor_id, Some(1))).await },
        async {
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
        },
    );
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
        other => panic!("replacement must be refused: {other:?}"),
    }
    assert_eq!(
        handler.cursor_registry().len(),
        0,
        "refused fetch removes the cursor"
    );
    match send(&handler, &session, fetch_next(cursor_id, Some(1))).await {
        DbResponse::Error { code, .. } => assert_eq!(code, "cursor_not_found"),
        other => panic!("removed cursor must not serve replacement rows: {other:?}"),
    }
}

#[tokio::test]
async fn fetch_next_identity_pause_positive_control_still_serves_the_page() {
    let handler = build_handler().await;
    let session = alice_session();
    let cursor_id = create_live_cursor(&handler, &session).await;
    let reached = Arc::new(tokio::sync::Barrier::new(2));
    let resume = Arc::new(tokio::sync::Barrier::new(2));
    let handler = handler.with_fetch_identity_pause(Arc::clone(&reached), Arc::clone(&resume));
    let (response, ()) = tokio::join!(
        async { send(&handler, &session, fetch_next(cursor_id, Some(1))).await },
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
        DbResponse::CursorPage { page, .. } => {
            assert_eq!(page.records.len(), 1);
            assert!(page.records[0].get_value_i64("v").is_some());
        }
        other => panic!("unchanged table must serve the page: {other:?}"),
    }
}

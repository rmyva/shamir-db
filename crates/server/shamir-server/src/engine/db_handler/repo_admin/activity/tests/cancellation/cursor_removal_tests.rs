//! Cursor identity-removal regressions for final-page and legacy cancellation paths.
use std::sync::Arc;
use std::time::Duration;

use shamir_connect::common::types::{BindingMode, TransportKind};
use shamir_connect::server::conn_services::ConnectionServices;
use shamir_connect::server::dispatch::RequestHandler;
use shamir_connect::server::session::{Session, SessionPermissions};
use shamir_db::access::{principal64, Actor};
use shamir_db::engine::repo::{BoxRepoFactory, RepoConfig};
use shamir_db::engine::table::TableConfig;
use shamir_db::ShamirDb;
use shamir_query_builder::batch::Batch;
use shamir_query_builder::{doc, write};
use shamir_query_types::read::ReadQuery;
use shamir_query_types::wire::{CursorId, DbRequest, DbResponse};
use shamir_tx::RepoTxGate;

use crate::cursor_registry::{Cursor, PaginationMode};
use crate::db_handler::handler::ShamirDbHandler;

const TIMEOUT: Duration = Duration::from_secs(120);

fn session(user: u8, sid: u8) -> Session {
    let mut session = Session::new(
        [user; 16],
        format!("user-{user}"),
        SessionPermissions::new(false, false, Vec::new()),
        TransportKind::Tcp,
        BindingMode::None,
        [0; 32],
        1_000,
    );
    session.session_id = [sid; 32];
    session
}

async fn fixture() -> (Arc<ShamirDb>, ShamirDbHandler, Actor) {
    let owner = Actor::User(principal64([1; 16]));
    let db = ShamirDb::init_memory().await.unwrap();
    db.create_db_as("app", owner.clone()).await.unwrap();
    db.add_repo_as(
        "app",
        RepoConfig::new("main", BoxRepoFactory::in_memory()).add_table(TableConfig::new("items")),
        owner.clone(),
    )
    .await
    .unwrap();
    let db = Arc::new(db);
    let handler = ShamirDbHandler::new(Arc::clone(&db));
    (db, handler, owner)
}

async fn wire(handler: &ShamirDbHandler, who: &Session, request: DbRequest) -> DbResponse {
    let bytes = rmp_serde::to_vec_named(&request).unwrap();
    let reply = handler
        .handle(who, &bytes, &ConnectionServices::without_push(0))
        .await
        .unwrap();
    rmp_serde::from_slice(&reply).unwrap()
}

fn execute(batch: shamir_query_builder::BatchRequest) -> DbRequest {
    DbRequest::Execute {
        query_version: crate::version::CURRENT_QUERY_LANG_VERSION,
        db: "app".into(),
        batch,
    }
}

async fn seed_rows(handler: &ShamirDbHandler, owner: &Session) {
    let mut batch = Batch::new();
    batch.id("seed");
    batch.insert(
        "rows",
        write::insert("items").rows((0..11).map(|n| doc! { "n" => n })),
    );
    assert!(
        matches!(
            wire(handler, owner, execute(batch.build())).await,
            DbResponse::Batch { .. }
        ),
        "fixture insert must be authorized and succeed"
    );
}

/// A 10-row create page over 11 seeded rows has `has_more=true`, registering
/// the cursor; the terminal fetch returns one row and pauses before removal.
/// An id-only removal followed by same-ID replacement must preserve the
/// replacement, while the losing terminal fetch must not return the old page.
#[tokio::test]
async fn final_page_fetch_preserves_replacement_cursor() {
    tokio::time::timeout(TIMEOUT, async {
        let (_db, handler, _) = fixture().await;
        let owner = session(1, 1);
        seed_rows(&handler, &owner).await;
        let replacement_owner = session(2, 2);
        let cursor_id = match handler
            .create_cursor(
                &owner,
                crate::version::CURRENT_QUERY_LANG_VERSION,
                "app",
                ReadQuery::with_repo("main", "items"),
                10,
            )
            .await
        {
            DbResponse::CursorPage { cursor_id, .. } => cursor_id,
            other => panic!("create cursor: {other:?}"),
        };
        let reached = Arc::new(tokio::sync::Barrier::new(2));
        let resume = Arc::new(tokio::sync::Barrier::new(2));
        let handler = handler.with_cursor_remove_pause(
            cursor_id.0,
            Arc::clone(&reached),
            Arc::clone(&resume),
        );
        let task_owner = session(1, 1);
        let (fetch_result, _) = tokio::join!(
            async { handler.fetch_next(&task_owner, cursor_id, Some(10)).await },
            async {
                tokio::time::timeout(TIMEOUT, reached.wait())
                    .await
                    .expect("fetch removal seam not reached");
                drop(
                    handler
                        .cursor_registry()
                        .remove(cursor_id.0)
                        .expect("remove selected cursor"),
                );
                let snapshot = RepoTxGate::new(0, 12).open_snapshot().await;
                let replacement = Cursor::new(
                    ReadQuery::new("items"),
                    PaginationMode::Offset,
                    snapshot,
                    0,
                    10,
                    replacement_owner.session_id,
                    "app".into(),
                    "main".into(),
                );
                handler
                    .cursor_registry()
                    .register(cursor_id.0, replacement_owner.session_id, replacement, 4)
                    .unwrap();
                tokio::time::timeout(TIMEOUT, resume.wait())
                    .await
                    .expect("resume within bound");
            },
        );
        assert!(matches!(
            fetch_result,
            DbResponse::Error { code, .. } if code == "cursor_not_found"
        ));
        assert!(handler
            .cursor_registry()
            .get_owned(cursor_id.0, &replacement_owner.session_id)
            .is_ok());
    })
    .await
    .expect("final-page fetch scenario timed out");
}

/// Legacy CancelCursor handler removal is identity-checked across same-ID reuse.
#[tokio::test]
async fn legacy_cancel_handler_preserves_replacement_cursor() {
    tokio::time::timeout(TIMEOUT, async {
        let (_db, handler, _) = fixture().await;
        let owner = session(1, 1);
        let replacement_owner = session(2, 2);
        let snapshot = RepoTxGate::new(0, 21).open_snapshot().await;
        let old = Cursor::new(
            ReadQuery::new("items"),
            PaginationMode::Offset,
            snapshot,
            0,
            10,
            owner.session_id,
            "app".into(),
            "main".into(),
        );
        handler
            .cursor_registry()
            .register(821, owner.session_id, old, 4)
            .unwrap();
        let reached = Arc::new(tokio::sync::Barrier::new(2));
        let resume = Arc::new(tokio::sync::Barrier::new(2));
        let handler =
            handler.with_cursor_remove_pause(821, Arc::clone(&reached), Arc::clone(&resume));
        let task_handler = handler.clone();
        let task_owner = session(1, 1);
        let task =
            tokio::spawn(
                async move { task_handler.cancel_cursor(&task_owner, CursorId(821)).await },
            );
        tokio::time::timeout(TIMEOUT, reached.wait())
            .await
            .expect("cancel removal seam not reached");
        drop(
            handler
                .cursor_registry()
                .remove(821)
                .expect("remove selected cursor"),
        );
        let snapshot = RepoTxGate::new(0, 22).open_snapshot().await;
        let replacement = Cursor::new(
            ReadQuery::new("items"),
            PaginationMode::Offset,
            snapshot,
            0,
            10,
            replacement_owner.session_id,
            "app".into(),
            "main".into(),
        );
        handler
            .cursor_registry()
            .register(821, replacement_owner.session_id, replacement, 4)
            .unwrap();
        resume.wait().await;
        assert!(matches!(
            task.await.unwrap(),
            DbResponse::CursorClosed {
                cursor_id: CursorId(821)
            }
        ));
        assert!(handler
            .cursor_registry()
            .get_owned(821, &replacement_owner.session_id)
            .is_ok());
    })
    .await
    .expect("legacy cancel scenario timed out");
}

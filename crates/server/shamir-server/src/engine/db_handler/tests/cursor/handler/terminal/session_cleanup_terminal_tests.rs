//! Discriminates terminal-page return after session cleanup removes its cursor without taking the cursor lock.

use std::sync::Arc;
use std::time::Duration;

use shamir_query_builder::cursor::fetch_next;
use shamir_query_types::wire::DbResponse;

use super::super::common::{alice_session, build_handler_with_rows, create_cursor_req, send};
use crate::db_handler::config::CursorLimitsCap;

const CALL_TIMEOUT: Duration = Duration::from_secs(120);

#[tokio::test]
async fn session_cleanup_wins_the_terminal_removal_and_the_fetch_answers_not_found() {
    let handler = build_handler_with_rows(2, CursorLimitsCap::UNLIMITED).await;
    let session = alice_session();
    let cursor_id = match send(
        &handler,
        &session,
        create_cursor_req(
            shamir_query_builder::query::Query::with_repo("main", "items").build(),
            1,
        ),
    )
    .await
    {
        DbResponse::CursorPage {
            cursor_id,
            has_more: true,
            ..
        } => cursor_id,
        other => panic!("expected non-terminal first page, got {other:?}"),
    };
    let reached = Arc::new(tokio::sync::Barrier::new(2));
    let resume = Arc::new(tokio::sync::Barrier::new(2));
    let handler =
        handler.with_cursor_remove_pause(cursor_id.0, Arc::clone(&reached), Arc::clone(&resume));
    let (fetch_result, _) = tokio::join!(
        async { send(&handler, &session, fetch_next(cursor_id, Some(1))).await },
        async {
            tokio::time::timeout(CALL_TIMEOUT, reached.wait())
                .await
                .expect("fetch reaches terminal-removal pause");
            let removed = handler
                .cursor_registry()
                .remove_all_for_session(&session.session_id);
            assert_eq!(removed.len(), 1);
            tokio::time::timeout(CALL_TIMEOUT, resume.wait())
                .await
                .expect("resume within bound");
        },
    );
    match fetch_result {
        DbResponse::Error { code, .. } => assert_eq!(code, "cursor_not_found"),
        other => panic!("session-removed cursor must not return terminal data: {other:?}"),
    }
    assert!(matches!(
        tokio::time::timeout(
            CALL_TIMEOUT,
            send(&handler, &session, fetch_next(cursor_id, Some(1)))
        )
        .await
        .expect("follow-up fetch completes within bound"),
        DbResponse::Error { code, .. } if code == "cursor_not_found"
    ));
}

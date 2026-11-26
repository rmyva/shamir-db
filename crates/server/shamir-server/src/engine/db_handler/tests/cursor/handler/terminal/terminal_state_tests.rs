//! Deterministic terminal-page removal race coverage.

use std::sync::Arc;
use std::time::Duration;

use shamir_query_builder::cursor::{cancel_cursor, fetch_next};
use shamir_query_types::wire::DbResponse;

use super::super::common::{alice_session, build_handler_with_rows, create_cursor_req, send};
use crate::db_handler::config::CursorLimitsCap;

const CALL_TIMEOUT: Duration = Duration::from_secs(120);

async fn open_final_page_cursor() -> (
    crate::db_handler::handler::ShamirDbHandler,
    shamir_connect::server::session::Session,
    shamir_query_types::wire::CursorId,
) {
    let handler = build_handler_with_rows(2, CursorLimitsCap::UNLIMITED).await;
    let session = alice_session();
    let response = send(
        &handler,
        &session,
        create_cursor_req(
            shamir_query_builder::query::Query::with_repo("main", "items").build(),
            1,
        ),
    )
    .await;
    let cursor_id = match response {
        DbResponse::CursorPage {
            cursor_id,
            has_more: true,
            ..
        } => cursor_id,
        other => panic!("expected non-terminal first page, got {other:?}"),
    };
    (handler, session, cursor_id)
}

#[tokio::test]
async fn concurrent_close_prevents_final_page_from_returning_after_removal() {
    let (handler, session, cursor_id) = open_final_page_cursor().await;
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
            assert!(matches!(
                tokio::time::timeout(
                    CALL_TIMEOUT,
                    send(&handler, &session, cancel_cursor(cursor_id))
                )
                .await
                .expect("close within bound"),
                DbResponse::CursorClosed { .. }
            ));
            tokio::time::timeout(CALL_TIMEOUT, resume.wait())
                .await
                .expect("resume within bound");
        },
    );
    match fetch_result {
        DbResponse::Error { code, .. } => assert_eq!(code, "cursor_not_found"),
        other => panic!("removed cursor must not return terminal data: {other:?}"),
    }
    assert!(matches!(
        tokio::time::timeout(CALL_TIMEOUT, send(&handler, &session, fetch_next(cursor_id, Some(1))))
            .await
            .expect("follow-up fetch completes within bound"),
        DbResponse::Error { code, .. } if code == "cursor_not_found"
    ));
}

#[tokio::test]
async fn second_fetch_cannot_observe_or_report_the_final_page_twice() {
    let (handler, session, cursor_id) = open_final_page_cursor().await;
    let reached = Arc::new(tokio::sync::Barrier::new(2));
    let resume = Arc::new(tokio::sync::Barrier::new(2));
    let handler =
        handler.with_cursor_remove_pause(cursor_id.0, Arc::clone(&reached), Arc::clone(&resume));
    let (first, _) = tokio::join!(
        async { send(&handler, &session, fetch_next(cursor_id, Some(1))).await },
        async {
            tokio::time::timeout(CALL_TIMEOUT, reached.wait())
                .await
                .expect("first fetch reaches terminal-removal pause");
            match tokio::time::timeout(
                CALL_TIMEOUT,
                send(&handler, &session, fetch_next(cursor_id, Some(1))),
            )
            .await
            .expect("second fetch completes within bound")
            {
                DbResponse::Error { code, .. } => assert_eq!(code, "cursor_not_found"),
                other => {
                    panic!("serialized exhausted fetch must not return another page: {other:?}")
                }
            }
            tokio::time::timeout(CALL_TIMEOUT, resume.wait())
                .await
                .expect("resume within bound");
        },
    );
    match first {
        DbResponse::CursorPage {
            page,
            has_more: false,
            ..
        } => assert_eq!(page.records.len(), 1),
        other => panic!("the winning final fetch must return its page: {other:?}"),
    }
}

#[tokio::test]
async fn terminal_removal_pause_positive_control_returns_final_page() {
    let (handler, session, cursor_id) = open_final_page_cursor().await;
    let reached = Arc::new(tokio::sync::Barrier::new(2));
    let resume = Arc::new(tokio::sync::Barrier::new(2));
    let handler =
        handler.with_cursor_remove_pause(cursor_id.0, Arc::clone(&reached), Arc::clone(&resume));
    let (result, _) = tokio::join!(
        async { send(&handler, &session, fetch_next(cursor_id, Some(1))).await },
        async {
            tokio::time::timeout(CALL_TIMEOUT, reached.wait())
                .await
                .expect("fetch reaches terminal-removal pause");
            tokio::time::timeout(CALL_TIMEOUT, resume.wait())
                .await
                .expect("resume within bound");
        },
    );
    match result {
        DbResponse::CursorPage {
            page,
            has_more: false,
            ..
        } => assert_eq!(page.records.len(), 1),
        other => panic!("unchanged cursor must return final page: {other:?}"),
    }
}

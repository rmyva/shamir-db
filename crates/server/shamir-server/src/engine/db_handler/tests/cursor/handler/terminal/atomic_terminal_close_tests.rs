//! Discriminates terminal checks before state-lock acquisition and cleanup outside the state lock.

use std::sync::Arc;
use std::time::Duration;

use shamir_query_builder::cursor::{cancel_cursor, fetch_next};
use shamir_query_types::wire::DbResponse;

use super::super::common::{alice_session, build_handler_with_rows, create_cursor_req, send};
use crate::db_handler::config::CursorLimitsCap;

const CALL_TIMEOUT: Duration = Duration::from_secs(120);

async fn open_cursor() -> (
    crate::db_handler::handler::ShamirDbHandler,
    shamir_connect::server::session::Session,
    shamir_query_types::wire::CursorId,
) {
    let handler = build_handler_with_rows(3, CursorLimitsCap::UNLIMITED).await;
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
        other => panic!("expected open cursor, got {other:?}"),
    };
    (handler, session, cursor_id)
}

#[tokio::test]
async fn close_between_lookup_and_state_lock_rejects_fetch() {
    let (handler, session, cursor_id) = open_cursor().await;
    let reached = Arc::new(tokio::sync::Barrier::new(2));
    let resume = Arc::new(tokio::sync::Barrier::new(2));
    let handler =
        handler.with_fetch_before_state_lock_pause(Arc::clone(&reached), Arc::clone(&resume));
    let (result, _) = tokio::join!(
        async { send(&handler, &session, fetch_next(cursor_id, Some(1))).await },
        async {
            tokio::time::timeout(CALL_TIMEOUT, reached.wait())
                .await
                .expect("fetch paused after successful registry lookup");
            assert!(matches!(
                tokio::time::timeout(
                    CALL_TIMEOUT,
                    send(&handler, &session, cancel_cursor(cursor_id))
                )
                .await
                .expect("cancel completes"),
                DbResponse::CursorClosed { .. }
            ));
            tokio::time::timeout(CALL_TIMEOUT, resume.wait())
                .await
                .expect("resume fetch");
        },
    );
    // Catches checking terminal before acquiring state: that check could observe false, then serve a page after close.
    assert!(matches!(result, DbResponse::Error { code, .. } if code == "cursor_not_found"));
    assert!(handler.cursor_registry().is_empty());
}

#[tokio::test]
async fn close_waits_for_page_state_lock_and_removes_once() {
    let (handler, session, cursor_id) = open_cursor().await;
    let registry = handler.cursor_registry();
    assert_eq!(registry.open_count_for_session(&session.session_id), 1);
    let cursor = registry
        .get_owned(cursor_id.0, &session.session_id)
        .expect("cursor exists");
    let state = cursor.state().lock().await;
    let close_handler = handler.clone();
    let close = tokio::spawn(async move {
        send(&close_handler, &alice_session(), cancel_cursor(cursor_id)).await
    });
    tokio::time::timeout(CALL_TIMEOUT, async {
        while !cursor.is_terminal() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("close claims terminal flag while waiting for page lock");
    assert!(
        !close.is_finished(),
        "close must wait for the held page lock"
    );
    assert_eq!(registry.len(), 1);
    drop(state);
    assert!(matches!(
        tokio::time::timeout(CALL_TIMEOUT, close)
            .await
            .expect("close joins")
            .expect("close task"),
        DbResponse::CursorClosed { .. }
    ));
    assert_eq!(registry.len(), 0);
    assert_eq!(registry.open_count_for_session(&session.session_id), 0);
    assert!(matches!(
        tokio::time::timeout(CALL_TIMEOUT, send(&handler, &session, fetch_next(cursor_id, Some(1))))
            .await.expect("follow-up fetch completes"),
        DbResponse::Error { code, .. } if code == "cursor_not_found"
    ));
}

#[tokio::test]
async fn concurrent_double_close_is_idempotent_and_frees_one_slot() {
    let (handler, session, cursor_id) = open_cursor().await;
    let registry = handler.cursor_registry();
    assert_eq!(registry.open_count_for_session(&session.session_id), 1);
    let left_handler = handler.clone();
    let right_handler = handler.clone();
    let (left, right) = tokio::join!(
        tokio::time::timeout(CALL_TIMEOUT, async move {
            send(&left_handler, &alice_session(), cancel_cursor(cursor_id)).await
        }),
        tokio::time::timeout(CALL_TIMEOUT, async move {
            send(&right_handler, &alice_session(), cancel_cursor(cursor_id)).await
        }),
    );
    assert!(matches!(
        left.expect("first cancel completes"),
        DbResponse::CursorClosed { .. }
    ));
    assert!(matches!(
        right.expect("second cancel completes"),
        DbResponse::CursorClosed { .. }
    ));
    assert_eq!(registry.len(), 0);
    assert_eq!(registry.open_count_for_session(&session.session_id), 0);
}

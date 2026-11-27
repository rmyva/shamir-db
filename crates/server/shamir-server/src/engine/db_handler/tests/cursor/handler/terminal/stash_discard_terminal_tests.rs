//! Discriminates terminal fetch stash leakage and a cancel that fails to claim terminal during final-page removal.
//!
//! The 1,000,000-byte budget is finite (so `budget_active` is true) but far
//! above the few-hundred-byte page; `CursorLimitsCap::UNLIMITED` leaves
//! `max_result_size_bytes == usize::MAX`, so neither bound rejects it.
//! Thus the page is serialized, budget-accounted, and stashed before the
//! terminal-removal race is resolved.
//!
//! Mutants: reverting the loss-branch stash discard returns the stashed page
//! bytes and fails the `cursor_not_found` wire assertion. Breaking the
//! `run_with_guard_slot` wiring leaves the captured guard `None`. The
//! positive control only proves that an uncontended terminal fetch still
//! delivers its page and budget guard; it does not tell stash reuse from a
//! fresh encoding, so it kills no discard mutant.

use std::sync::Arc;
use std::time::Duration;

use shamir_connect::server::conn_services::ConnectionServices;
use shamir_connect::server::dispatch::RequestHandler;
use shamir_connect::server::session::Session;
use shamir_query_builder::cursor::{cancel_cursor, fetch_next};
use shamir_query_types::wire::{DbRequest, DbResponse};

use super::super::common::{alice_session, build_handler_with_rows, create_cursor_req, send};
use crate::byte_budget::{run_with_guard_slot, take_stashed_guard, ByteBudget, ByteBudgetGuard};
use crate::db_handler::config::CursorLimitsCap;
use crate::db_handler::handler::ShamirDbHandler;

const CALL_TIMEOUT: Duration = Duration::from_secs(120);

async fn send_in_stash_scope(
    handler: &ShamirDbHandler,
    session: &Session,
    req: DbRequest,
) -> (DbResponse, Option<ByteBudgetGuard>) {
    let wire = rmp_serde::to_vec_named(&req).expect("encode request");
    let (bytes, guard) = tokio::time::timeout(
        CALL_TIMEOUT,
        run_with_guard_slot(async {
            let bytes = handler
                .handle(session, &wire, &ConnectionServices::without_push(0))
                .await
                .expect("handle succeeds");
            let guard = take_stashed_guard();
            (bytes, guard)
        }),
    )
    .await
    .expect("handle within the 120s bound");
    (
        rmp_serde::from_slice(&bytes).expect("decode response"),
        guard,
    )
}

async fn open_final_page_cursor() -> (ShamirDbHandler, Session, shamir_query_types::wire::CursorId)
{
    let handler = build_handler_with_rows(2, CursorLimitsCap::UNLIMITED)
        .await
        .with_byte_budget(ByteBudget::new(Some(1_000_000)));
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
async fn losing_terminal_fetch_discards_the_stashed_page_on_the_wire() {
    let (handler, session, cursor_id) = open_final_page_cursor().await;
    let reached = Arc::new(tokio::sync::Barrier::new(2));
    let resume = Arc::new(tokio::sync::Barrier::new(2));
    let handler =
        handler.with_cursor_remove_pause(cursor_id.0, Arc::clone(&reached), Arc::clone(&resume));
    let ((fetch_result, guard), _) = tokio::join!(
        async {
            tokio::time::timeout(
                CALL_TIMEOUT,
                send_in_stash_scope(&handler, &session, fetch_next(cursor_id, Some(1))),
            )
            .await
            .expect("terminal fetch completes within bound")
        },
        async {
            tokio::time::timeout(CALL_TIMEOUT, reached.wait())
                .await
                .expect("fetch reaches terminal-removal pause");
            let cursor = handler
                .cursor_registry()
                .get_owned(cursor_id.0, &session.session_id)
                .expect("cursor remains registered while fetch is paused");
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
            .expect("cancel claims terminal flag before fetch resumes");
            tokio::time::timeout(CALL_TIMEOUT, resume.wait())
                .await
                .expect("resume within bound");
            assert!(matches!(
                tokio::time::timeout(CALL_TIMEOUT, close)
                    .await
                    .expect("close joins")
                    .expect("close task"),
                DbResponse::CursorClosed { .. }
            ));
        },
    );
    match fetch_result {
        DbResponse::Error { code, .. } => assert_eq!(code, "cursor_not_found"),
        other => panic!("lost terminal fetch must return cursor_not_found: {other:?}"),
    }
    let guard = guard.expect("page response budget guard was stashed in scope");
    assert!(guard.bytes_reserved() > 0, "page must be budget-accounted");
    drop(guard);
}

#[tokio::test]
async fn winning_terminal_fetch_returns_the_page_through_the_stash_scope() {
    let (handler, session, cursor_id) = open_final_page_cursor().await;
    let reached = Arc::new(tokio::sync::Barrier::new(2));
    let resume = Arc::new(tokio::sync::Barrier::new(2));
    let handler =
        handler.with_cursor_remove_pause(cursor_id.0, Arc::clone(&reached), Arc::clone(&resume));
    let ((result, guard), _) = tokio::join!(
        async {
            tokio::time::timeout(
                CALL_TIMEOUT,
                send_in_stash_scope(&handler, &session, fetch_next(cursor_id, Some(1))),
            )
            .await
            .expect("terminal fetch completes within bound")
        },
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
        other => panic!("winning terminal fetch must return its final page: {other:?}"),
    }
    let guard = guard.expect("page response budget guard was stashed in scope");
    assert!(guard.bytes_reserved() > 0, "page must be budget-accounted");
    drop(guard);
}

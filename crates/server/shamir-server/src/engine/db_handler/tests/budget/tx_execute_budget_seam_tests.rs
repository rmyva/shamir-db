//! Exact caller-budget regression and existing abort-branch preservation.

use std::future::{poll_fn, Future};
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use shamir_connect::common::types::{BindingMode, TransportKind};
use shamir_connect::server::session::{Session, SessionPermissions};
use shamir_db::access::{principal64, Actor};
use shamir_db::engine::repo::{BoxRepoFactory, RepoConfig};
use shamir_db::engine::table::TableConfig;
use shamir_db::ShamirDb;
use shamir_query_builder::batch::Batch;
use shamir_query_builder::query::Query;
use shamir_query_types::batch::{BatchLimits, BatchRequest};

use crate::db_handler::handler::{DbResponse, ShamirDbHandler};
use crate::tx_registry::{InteractiveTx, TxRegistryError};
use crate::version::CURRENT_QUERY_LANG_VERSION;

async fn fixture(lifetime: Duration) -> (ShamirDbHandler, Session, u64) {
    let session = Session::new(
        [0xAB; 16],
        "alice".into(),
        SessionPermissions::new(true, false, vec!["read_write".into()]),
        TransportKind::Tcp,
        BindingMode::TlsExporter,
        [0; 32],
        0,
    );
    let db = ShamirDb::init_memory().await.expect("init memory database");
    let owner = Actor::User(principal64(session.user_id));
    db.create_db_as("app", owner.clone())
        .await
        .expect("create_db");
    db.add_repo_as(
        "app",
        RepoConfig::new("main", BoxRepoFactory::in_memory()).add_table(TableConfig::new("items")),
        owner.clone(),
    )
    .await
    .expect("add repo");
    let (tx, snapshot, _repo) = db
        .tx_begin_as(owner, "app", "main", "snapshot")
        .await
        .expect("begin real transaction");
    let handle = tx.tx_id.0;
    // Freeze before constructing the handle, not after a wall-clock gap.
    tokio::time::pause();
    let handler = ShamirDbHandler::new(Arc::new(db));
    handler
        .tx_registry
        .register(
            handle,
            InteractiveTx::new(
                tx,
                snapshot,
                session.session_id,
                session.user_id,
                "app".into(),
                "main".into(),
                lifetime,
                handler.tx_abort_release("app", "main"),
            ),
        )
        .expect("park transaction");
    (handler, session, handle)
}

fn read(limits: BatchLimits) -> BatchRequest {
    let mut batch = Batch::new();
    batch.limits(limits);
    batch.query("read", Query::from("items"));
    batch.build()
}

async fn execute(
    handler: &ShamirDbHandler,
    session: &Session,
    handle: u64,
    batch: BatchRequest,
) -> DbResponse {
    handler
        .tx_execute(session, CURRENT_QUERY_LANG_VERSION, "app", handle, batch)
        .await
}

async fn assert_removed(handler: &ShamirDbHandler, session: &Session, handle: u64) {
    assert!(matches!(
        handler.tx_registry.get_owned(handle, &session.session_id),
        Err(TxRegistryError::TxNotFound),
    ));
    match execute(handler, session, handle, read(BatchLimits::default())).await {
        DbResponse::Error { code, .. } => assert_eq!(code, "tx_not_found"),
        other => panic!("removed handle remained executable: {other:?}"),
    }
    assert!(handler.tx_registry.is_empty());
}

// Existing server fixture convention: deep engine futures need a roomy stack.
fn on_big_stack<F, Fut>(body: F)
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = ()>,
{
    std::thread::Builder::new()
        .stack_size(64 * 1024 * 1024)
        .spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("test runtime")
                .block_on(body())
        })
        .expect("spawn test thread")
        .join()
        .unwrap_or_else(|panic| std::panic::resume_unwind(panic));
}

/// Inferred red before this seam: the pre-lock rounded budget allows the read.
#[test]
fn tiny_remaining_lifetime_expires_while_waiting_for_context() {
    on_big_stack(|| async {
        let lifetime = Duration::from_nanos(123_456_789);
        let (handler, session, handle) = fixture(lifetime).await;
        let it = handler
            .tx_registry
            .get_owned(handle, &session.session_id)
            .expect("registered handle");
        let guard = it.ctx().lock().await;
        let mut call = Box::pin(execute(
            &handler,
            &session,
            handle,
            read(BatchLimits::default()),
        ));
        // One explicit poll proves execution reached the held context lock.
        poll_fn(|cx| {
            assert!(matches!(call.as_mut().poll(cx), Poll::Pending));
            Poll::Ready(())
        })
        .await;
        tokio::time::advance(lifetime).await;
        assert!(it.remaining_lifetime().is_zero());
        drop(guard);
        drop(it);
        match call.await {
            DbResponse::Error { code, .. } => assert_eq!(code, "limits"),
            other => panic!("expected cooperative execution timeout, got {other:?}"),
        }
        assert_removed(&handler, &session, handle).await;
    });
}

/// Already correct on the base; red if the LimitExceeded removal arm is removed.
#[test]
fn work_limit_error_removes_handle() {
    on_big_stack(|| async {
        let (handler, session, handle) = fixture(Duration::from_secs(300)).await;
        let limits = BatchLimits {
            max_total_work: 0,
            ..BatchLimits::default()
        };
        match execute(&handler, &session, handle, read(limits)).await {
            DbResponse::Error { code, .. } => assert_eq!(code, "work_budget_exceeded"),
            other => panic!("expected work-limit error, got {other:?}"),
        }
        assert_removed(&handler, &session, handle).await;
    });
}

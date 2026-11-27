//! W7 partial lifecycle coverage; no real-time sleeps or synthetic timeouts.

use std::sync::Arc;
use std::time::Duration;

use shamir_connect::server::conn_services::ConnectionServices;
use shamir_connect::server::dispatch::RequestHandler;
use shamir_query_builder::write::{doc, insert};
use shamir_query_types::batch::{BatchLimits, BatchRequest, VisibilityWait};
use shamir_query_types::wire::DbRequest;

use shamir_connect::common::types::{BindingMode, TransportKind};
use shamir_connect::server::session::{Session, SessionPermissions};
use shamir_db::access::{principal64, Actor};
use shamir_db::engine::repo::{BoxRepoFactory, RepoConfig};
use shamir_db::engine::table::TableConfig;
use shamir_db::query::batch::BatchError;
use shamir_db::ShamirDb;
use shamir_query_builder::batch::Batch;
use shamir_query_builder::ddl::{repl_scope, replication_profile, subscription};
use shamir_query_builder::query::Query;
use shamir_query_types::admin::{ReplDirection, ReplMode};

use crate::db_handler::config::QueryLimitsCap;
use crate::db_handler::handler::{DbResponse, ShamirDbHandler};
use crate::tx_registry::{InteractiveTx, TxRegistryError};
use crate::version::CURRENT_QUERY_LANG_VERSION;

async fn fixture(lifetime: Duration) -> (ShamirDbHandler, Session, u64) {
    let session = Session::new(
        [0xAB; 16],
        "alice".into(),
        SessionPermissions::new(false, false, vec!["read_write".into()]),
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
    let (tx, snapshot, repo) = db
        .tx_begin_as(owner, "app", "main", "snapshot")
        .await
        .expect("begin real transaction");
    let handle = tx.tx_id.0;
    let handler = ShamirDbHandler::new(Arc::new(db)).with_query_limits(QueryLimitsCap {
        max_page_size: 2,
        max_offset: 0,
        ..QueryLimitsCap::UNLIMITED
    });
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
            )
            .with_bound_repo(repo),
        )
        .expect("park transaction");
    (handler, session, handle)
}

async fn quarantine_repo(handler: &ShamirDbHandler) {
    let mut batch = Batch::new();
    batch.op(
        "profile",
        replication_profile("profile").stream(
            repl_scope("app").repo("main").build(),
            ReplDirection::Pull,
            ReplMode::ReadOnly,
        ),
    );
    batch.op(
        "sub",
        subscription("sub", "tcp://peer:9000", "pub", "profile"),
    );
    handler
        .db()
        .execute_as(Actor::System, "app", &batch.build())
        .await
        .expect("create replication subscription");
    handler
        .db()
        .mark_subscription_resync_required("sub", 7, 3)
        .await
        .expect("mark repository resync_required");
}

fn read(table: &str) -> BatchRequest {
    let mut batch = Batch::new();
    batch.query("read", Query::from(table));
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

// Match the existing handler fixtures' roomy stack for deep engine async frames.
fn on_big_stack<F, Fut>(body: F)
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()>,
{
    std::thread::Builder::new()
        .stack_size(64 * 1024 * 1024)
        .spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("test runtime")
                // The timer must be built inside the runtime.
                .block_on(
                    async move { tokio::time::timeout(Duration::from_secs(30), body()).await },
                )
                .expect("test exceeded 30-second timeout")
        })
        .expect("spawn test thread")
        .join()
        .unwrap_or_else(|panic| std::panic::resume_unwind(panic));
}

/// A dropped repository name refuses a bound TxExecute and consumes the handle.
/// Reverting to the name-only check would let this execution reach the detached store.
#[test]
fn tx_execute_refuses_when_bound_repository_name_no_longer_resolves() {
    on_big_stack(|| async {
        let (handler, session, original_handle) = fixture(Duration::from_secs(300)).await;
        assert!(matches!(
            handler.tx_rollback(&session, "app", original_handle).await,
            DbResponse::TxRolledBack { .. }
        ));
        let handle = match handler
            .tx_begin(&session, CURRENT_QUERY_LANG_VERSION, "app", "main", None)
            .await
        {
            DbResponse::TxOpened { tx_handle, .. } => tx_handle,
            other => panic!("begin failed: {other:?}"),
        };
        assert!(handler.db().remove_repo("app", "main").await.unwrap());

        match execute(&handler, &session, handle, read("items")).await {
            DbResponse::Error { code, message } => {
                assert_eq!(code, "tx_wrong_repo");
                assert!(message.contains("changed since the transaction began"));
            }
            other => panic!("expected repository binding refusal, got {other:?}"),
        }
        match execute(&handler, &session, handle, read("items")).await {
            DbResponse::Error { code, .. } => assert_eq!(code, "tx_not_found"),
            other => panic!("binding refusal did not remove transaction: {other:?}"),
        }
    });
}

/// Replacing the repository under the same name refuses a bound TxExecute.
/// Reverting to name-only checking makes the old handle execute against the replacement.
#[test]
fn tx_execute_refuses_replacement_repo_and_unchanged_repo_control_works() {
    on_big_stack(|| async {
        let (handler, session, initial) = fixture(Duration::from_secs(300)).await;
        handler.tx_rollback(&session, "app", initial).await;
        let handle = match handler
            .tx_begin(&session, CURRENT_QUERY_LANG_VERSION, "app", "main", None)
            .await
        {
            DbResponse::TxOpened { tx_handle, .. } => tx_handle,
            other => panic!("begin failed: {other:?}"),
        };
        let original = handler
            .db()
            .get_db("app")
            .unwrap()
            .get_repo("main")
            .unwrap();
        let original_gate = original.tx_gate().await.unwrap();
        handler
            .db()
            .rename_repo_as("app", "main", "old", Actor::System)
            .await
            .unwrap();
        let owner = Actor::User(principal64(session.user_id));
        handler
            .db()
            .add_repo_as(
                "app",
                RepoConfig::new("main", BoxRepoFactory::in_memory())
                    .add_table(TableConfig::new("items")),
                owner,
            )
            .await
            .unwrap();
        let replacement = handler
            .db()
            .get_db("app")
            .unwrap()
            .get_repo("main")
            .unwrap();
        assert!(!Arc::ptr_eq(
            &original_gate,
            &replacement.tx_gate().await.unwrap()
        ));
        match execute(&handler, &session, handle, read("items")).await {
            DbResponse::Error { code, message } => {
                assert_eq!(code, "tx_wrong_repo");
                assert!(message.contains("changed since the transaction began"));
            }
            other => panic!("expected replacement binding refusal, got {other:?}"),
        }
        let fresh = match handler
            .tx_begin(&session, CURRENT_QUERY_LANG_VERSION, "app", "main", None)
            .await
        {
            DbResponse::TxOpened { tx_handle, .. } => tx_handle,
            other => panic!("fresh begin failed: {other:?}"),
        };
        assert!(matches!(
            execute(&handler, &session, fresh, read("items")).await,
            DbResponse::TxBatch { .. }
        ));
    });
}

/// The returned repo is the pre-swap instance resolved when BEGIN opens its tx.
/// This memory fixture distinguishes replacement objects through tx_gate pointer identity.
#[test]
fn tx_begin_returns_the_instance_used_for_open() {
    on_big_stack(|| async {
        let (handler, session, initial) = fixture(Duration::from_secs(300)).await;
        handler.tx_rollback(&session, "app", initial).await;
        let db = handler.db();
        let before = db.get_db("app").unwrap().get_repo("main").unwrap();
        let before_gate = before.tx_gate().await.unwrap();
        let (_tx, _guard, returned) = db
            .tx_begin_as(Actor::System, "app", "main", "snapshot")
            .await
            .unwrap();
        assert!(Arc::ptr_eq(
            &before_gate,
            &returned.tx_gate().await.unwrap()
        ));
        db.rename_repo_as("app", "main", "old", Actor::System)
            .await
            .unwrap();
        db.add_repo_as(
            "app",
            RepoConfig::new("main", BoxRepoFactory::in_memory())
                .add_table(TableConfig::new("items")),
            Actor::System,
        )
        .await
        .unwrap();
        let current = db.get_db("app").unwrap().get_repo("main").unwrap();
        assert!(!Arc::ptr_eq(
            &before_gate,
            &current.tx_gate().await.unwrap()
        ));
        assert!(Arc::ptr_eq(
            &before_gate,
            &returned.tx_gate().await.unwrap()
        ));
    });
}
#[test]
fn zero_lifetime_rejects_execute_removes_handle_and_frees_session() {
    on_big_stack(|| async {
        let (handler, session, handle) = fixture(Duration::ZERO).await;
        match execute(&handler, &session, handle, read("items")).await {
            DbResponse::Error { code, .. } => assert_eq!(code, "tx_expired"),
            other => panic!("expected expired transaction, got {other:?}"),
        }
        assert!(matches!(
            handler.tx_registry.get_owned(handle, &session.session_id),
            Err(TxRegistryError::TxNotFound),
        ));
        match execute(&handler, &session, handle, read("items")).await {
            DbResponse::Error { code, .. } => assert_eq!(code, "tx_not_found"),
            other => panic!("removed handle remained executable: {other:?}"),
        }
        let next = match handler
            .tx_begin(&session, CURRENT_QUERY_LANG_VERSION, "app", "main", None)
            .await
        {
            DbResponse::TxOpened { tx_handle, .. } => tx_handle,
            other => panic!("session slot not freed: {other:?}"),
        };
        let response = handler.tx_rollback(&session, "app", next).await;
        assert!(
            matches!(response, DbResponse::TxRolledBack { .. }),
            "{response:?}",
        );
    });
}

/// Preservation control: catches an over-broad change that drops every engine error.
#[test]
fn tx_execute_quarantine_masks_absent_and_closed_table_equally() {
    on_big_stack(|| async {
        let (handler, session, handle) = fixture(Duration::from_secs(300)).await;
        quarantine_repo(&handler).await;
        assert!(handler.db().read_quarantine_reason("app", "main").is_some());

        let open_quarantined = execute(&handler, &session, handle, read("items")).await;
        assert!(matches!(
            open_quarantined,
            DbResponse::Error { code, .. } if code == "resync_required"
        ));

        let mut close = Batch::new();
        close.chmod(
            "table",
            shamir_query_builder::ddl::chmod(
                shamir_query_types::admin::ResourceRef::Table {
                    table: ["app".into(), "main".into(), "items".into()],
                },
                0o000,
            )
            .hmac("tx-execute-quarantine-test")
            .build()
            .expect("build table chmod"),
        );
        handler
            .db()
            .execute_as(Actor::System, "app", &close.build())
            .await
            .expect("close quarantined table");
        let existing = execute(&handler, &session, handle, read("items")).await;
        let absent = execute(&handler, &session, handle, read("missing")).await;
        let existing_error = match existing {
            DbResponse::Error { code, message } => (code, message),
            other => panic!("expected closed-table quarantine refusal, got {other:?}"),
        };
        let absent_error = match absent {
            DbResponse::Error { code, message } => (code, message),
            other => panic!("expected absent-table quarantine refusal, got {other:?}"),
        };
        assert_eq!(
            existing_error, absent_error,
            "quarantine refusal must not disclose existence"
        );
        assert_eq!(
            existing_error,
            (
                "unknown_table".into(),
                "Query '' failed [unknown_table]: table is unavailable".into(),
            )
        );
        handler.tx_rollback(&session, "app", handle).await;
    });
}

#[test]
fn tx_execute_invalid_pagination_masks_absent_and_closed_and_preserves_handle() {
    on_big_stack(|| async {
        let (handler, session, handle) = fixture(Duration::from_secs(300)).await;
        let invalid_read = |table: &str| {
            let mut batch = Batch::new();
            batch.limits(BatchLimits::CEILING);
            batch.query("read", Query::from(table).limit(3));
            batch.build()
        };
        let absent = execute(&handler, &session, handle, invalid_read("missing")).await;

        let mut close = Batch::new();
        close.chmod(
            "table",
            shamir_query_builder::ddl::chmod(
                shamir_query_types::admin::ResourceRef::Table {
                    table: ["app".into(), "main".into(), "items".into()],
                },
                0o000,
            )
            .hmac("tx-execute-pagination-test")
            .build()
            .expect("build table chmod"),
        );
        handler
            .db()
            .execute_as(Actor::System, "app", &close.build())
            .await
            .expect("close existing table");
        let closed = execute(&handler, &session, handle, invalid_read("items")).await;
        let absent_error = match absent {
            DbResponse::Error { code, message } => (code, message),
            other => panic!("expected absent-table pagination refusal, got {other:?}"),
        };
        let closed_error = match closed {
            DbResponse::Error { code, message } => (code, message),
            other => panic!("expected closed-table pagination refusal, got {other:?}"),
        };
        assert_eq!(
            absent_error, closed_error,
            "invalid pagination must not disclose table existence"
        );
        assert_eq!(
            absent_error,
            (
                "unknown_table".into(),
                "Query '' failed [unknown_table]: table is unavailable".into(),
            )
        );

        let mut reopen = Batch::new();
        reopen.chmod(
            "table",
            shamir_query_builder::ddl::chmod(
                shamir_query_types::admin::ResourceRef::Table {
                    table: ["app".into(), "main".into(), "items".into()],
                },
                0o777,
            )
            .hmac("tx-execute-pagination-test")
            .build()
            .expect("build table chmod"),
        );
        handler
            .db()
            .execute_as(Actor::System, "app", &reopen.build())
            .await
            .expect("reopen existing table");
        match execute(&handler, &session, handle, read("items")).await {
            DbResponse::TxBatch { .. } => {}
            other => panic!("handle not usable after pagination refusals: {other:?}"),
        }
        assert!(matches!(
            handler.tx_rollback(&session, "app", handle).await,
            DbResponse::TxRolledBack { .. }
        ));
    });
}

#[test]
fn ordinary_query_error_preserves_handle_for_successful_execute() {
    on_big_stack(|| async {
        let (handler, session, handle) = fixture(Duration::from_secs(300)).await;
        match execute(&handler, &session, handle, read("missing")).await {
            DbResponse::Error { code, message } => {
                // A missing table on a READ is the same typed, name-hiding
                // refusal as a closed table (no existence oracle).
                assert_eq!(code, "unknown_table");
                assert_eq!(
                    message,
                    "Query '' failed [unknown_table]: table is unavailable"
                );
            }
            other => panic!("expected missing-table query error, got {other:?}"),
        }
        match execute(&handler, &session, handle, read("items")).await {
            DbResponse::TxBatch { response } => {
                let result = response.results.get("read").expect("executed read result");
                assert!(!result.skipped, "read must actually execute");
                assert!(result.records.is_empty(), "fixture table is empty");
                assert!(response.transaction.is_none(), "execute must not commit");
            }
            other => panic!("handle not usable after ordinary query error: {other:?}"),
        }
        let response = handler.tx_rollback(&session, "app", handle).await;
        assert!(
            matches!(response, DbResponse::TxRolledBack { .. }),
            "{response:?}",
        );
        assert!(handler.tx_registry.is_empty());
    });
}

#[test]
fn vis1_legacy_requests_reject_before_writes_and_commit_consumption() {
    on_big_stack(|| async {
        let (handler, session, handle) = fixture(Duration::from_secs(300)).await;
        let conn = ConnectionServices::without_push(0);
        let mut write = Batch::new();
        write.insert(
            "insert",
            insert("items").row(doc().set("marker", "vis1").unwrap()),
        );
        for query_version in [0, 1, 2, 99] {
            let request = DbRequest::Execute {
                query_version,
                db: "app".into(),
                batch: write.build(),
            };
            let bytes = rmp_serde::to_vec_named(&request).unwrap();
            let reply = handler.handle(&session, &bytes, &conn).await.unwrap();
            let response: DbResponse = rmp_serde::from_slice(&reply).unwrap();
            assert!(matches!(
                response,
                DbResponse::Error { code, .. } if code == "unsupported_query_version"
            ));
            for request in [
                DbRequest::TxBegin {
                    query_version,
                    db: "app".into(),
                    repo: "main".into(),
                    isolation: None,
                },
                DbRequest::TxExecute {
                    query_version,
                    db: "app".into(),
                    tx_handle: handle,
                    batch: write.build(),
                },
            ] {
                let bytes = rmp_serde::to_vec_named(&request).unwrap();
                let reply = handler.handle(&session, &bytes, &conn).await.unwrap();
                let response: DbResponse = rmp_serde::from_slice(&reply).unwrap();
                assert!(matches!(
                    response,
                    DbResponse::Error { code, .. } if code == "unsupported_query_version"
                ));
            }
        }
        match execute(&handler, &session, handle, read("items")).await {
            DbResponse::TxBatch { response } => {
                assert!(
                    response.results["read"].records.is_empty(),
                    "nothing staged"
                );
            }
            other => panic!("handle changed after version rejection: {other:?}"),
        }
        // Stage a real write: rejecting old COMMIT must leave it uncommitted
        // and preserve the handle, not merely reject an unknown transaction.
        assert!(matches!(
            execute(&handler, &session, handle, write.build()).await,
            DbResponse::TxBatch { .. }
        ));
        for query_version in [0, 1, 2, 99] {
            let request = DbRequest::TxCommit {
                query_version,
                visibility_wait: None,
                db: "app".into(),
                tx_handle: handle,
            };
            let bytes = rmp_serde::to_vec_named(&request).unwrap();
            let reply = handler.handle(&session, &bytes, &conn).await.unwrap();
            let response: DbResponse = rmp_serde::from_slice(&reply).unwrap();
            assert!(matches!(
                response,
                DbResponse::Error { code, .. } if code == "unsupported_query_version"
            ));
            assert!(handler
                .tx_registry
                .get_owned(handle, &session.session_id)
                .is_ok());
        }
        match handler
            .execute(
                &session,
                CURRENT_QUERY_LANG_VERSION,
                "app",
                read("items"),
                &conn,
            )
            .await
        {
            DbResponse::Batch { response } => {
                assert!(
                    response.results["read"].records.is_empty(),
                    "no write committed"
                );
            }
            other => panic!("fresh read failed: {other:?}"),
        }
        handler.tx_rollback(&session, "app", handle).await;
    });
}

#[test]
fn vis1_tx_execute_rejects_commit_only_override_without_staging() {
    on_big_stack(|| async {
        let (handler, session, handle) = fixture(Duration::from_secs(300)).await;
        let mut write = Batch::new();
        write.insert(
            "insert",
            insert("items").row(doc().set("marker", "must-not-stage").unwrap()),
        );
        for wait in [
            VisibilityWait::Strict,
            VisibilityWait::Bounded { timeout_ms: 0 },
        ] {
            write.visibility_wait(wait);
            match execute(&handler, &session, handle, write.build()).await {
                DbResponse::Error { code, .. } => assert_eq!(code, "validation"),
                other => panic!("commit-only override executed: {other:?}"),
            }
        }
        let mut inner = Batch::new();
        inner.visibility_wait(VisibilityWait::Strict);
        inner.insert(
            "insert",
            insert("items").row(doc().set("marker", "nested").unwrap()),
        );
        let mut outer = Batch::new();
        outer.sub_batch_no_bind("nested", inner.build());
        match execute(&handler, &session, handle, outer.build()).await {
            DbResponse::Error { code, .. } => assert_eq!(code, "validation"),
            other => panic!("nested commit-only override executed: {other:?}"),
        }
        match execute(&handler, &session, handle, read("items")).await {
            DbResponse::TxBatch { response } => {
                assert!(
                    response.results["read"].records.is_empty(),
                    "nothing staged"
                );
                assert!(response.transaction.is_none());
            }
            other => panic!("handle was lost: {other:?}"),
        }
        handler.tx_rollback(&session, "app", handle).await;
    });
}

// S5c-X: the engine's implicit-commit receipt mapping tags a retention-gate
// refusal with the structured `code: Some("wal_retention_exceeded")`; the
// server's `error_code` mapper must pass it verbatim to the wire (same shape
// as `wal_requires_reopen`), not collapse it into the heuristic `"query"`.
#[test]
fn wal_retention_exceeded_query_error_passes_through_to_wire_code() {
    use crate::db_handler::cursor_handlers::error_response;
    use crate::db_handler::handler::error_code;

    let e = BatchError::QueryError {
        alias: "ins".into(),
        message: "commit refused: retained sealed WAL exceeds wal_retention_max_bytes under \
                  refuse_writes"
            .into(),
        code: Some("wal_retention_exceeded".into()),
    };
    assert_eq!(error_code(&e), "wal_retention_exceeded");
    match error_response(&e) {
        DbResponse::Error { code, .. } => assert_eq!(code, "wal_retention_exceeded"),
        other => panic!("expected wire Error, got {other:?}"),
    }
}

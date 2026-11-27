//! Tests for the read-only replica gate in `ShamirDbHandler::execute`
//! (REPLICATION PR4 §4.3).
//!
//! The handler carries a [`NodeMode`]. When `ReadOnly`, any batch entry
//! whose [`BatchOp::is_write`] returns `true` is rejected with
//! `code = "read_only_replica"` *before* the batch reaches the engine.
//! Reads (SELECT, introspection) pass through. The default (`ReadWrite`)
//! leaves behaviour unchanged.
//!
//! Queries are built exclusively through `shamir-query-builder` (CLAUDE.md).

use std::sync::Arc;

use shamir_connect::common::time::UnixNanos;
use shamir_connect::common::types::{BindingMode, TransportKind};
use shamir_connect::server::conn_services::ConnectionServices;
use shamir_connect::server::session::{Session, SessionPermissions};

use shamir_db::access::{principal64, Actor};
use shamir_db::engine::repo::{BoxRepoFactory, RepoConfig};
use shamir_db::engine::table::TableConfig;
use shamir_db::ShamirDb;

use shamir_query_builder::batch::Batch;
use shamir_query_builder::doc;
use shamir_query_builder::query::Query;
use shamir_query_builder::write::{insert, upsert};
use shamir_types::mpack;

use crate::db_handler::config::NodeMode;
use crate::db_handler::handler::{DbResponse, ShamirDbHandler};
use crate::tx_registry::InteractiveTx;
use crate::version::CURRENT_QUERY_LANG_VERSION;

/// Match the lifecycle fixture's roomy stack for deep engine async frames.
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
                .block_on(body())
        })
        .expect("spawn test thread")
        .join()
        .unwrap_or_else(|panic| std::panic::resume_unwind(panic));
}

/// RV-SERVER-C73466A7 fixture: a `ReadOnly` handler over a shared in-memory
/// `ShamirDb` with a real interactive transaction already parked in the
/// registry (opened directly against the engine). The `db` handle is
/// returned so tests can stage writes at the engine level, bypassing
/// TxExecute, to prove the commit-time gate.
async fn readonly_tx_fixture() -> (Arc<ShamirDb>, ShamirDbHandler, Session, u64) {
    let session = alice_session();
    let db = Arc::new(ShamirDb::init_memory().await.expect("init shamir"));
    let owner = Actor::User(principal64([0xAB; 16]));
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
    let (tx, guard, _repo) = db
        .tx_begin_as(owner, "app", "main", "snapshot")
        .await
        .expect("begin real transaction");
    let handle = tx.tx_id.0;
    let handler = ShamirDbHandler::new(db.clone()).with_node_mode(NodeMode::ReadOnly);
    handler
        .tx_registry
        .register(
            handle,
            InteractiveTx::new(
                tx,
                guard,
                session.session_id,
                session.user_id,
                "app".into(),
                "main".into(),
                std::time::Duration::from_secs(60),
                handler.tx_abort_release("app", "main"),
            ),
        )
        .expect("park transaction");
    (db, handler, session, handle)
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

/// A regular ("alice") session — `session_actor` resolves it to
/// `Actor::User(principal64([0xAB; 16]))` (the projection of the session's
/// directory-minted user_id bytes, NOT a username hash). Mirrors the bench
/// fixture in `benches/wire_pipelining.rs`; `build_handler` below constructs
/// the resource owner from the SAME bytes so the session owns what it creates.
fn alice_session() -> Session {
    Session::new(
        [0xAB; 16],
        "alice".into(),
        SessionPermissions::from_roles(vec!["read_write".into()]),
        TransportKind::Tcp,
        BindingMode::TlsExporter,
        [0u8; 32],
        UnixNanos::now().as_u64(),
    )
}

/// Build a handler over an in-memory `ShamirDb` with a `main` repo containing
/// an `items` table, owned by `alice` (so the non-superuser session can pass
/// the Shomer gate on subsequent ops). See `wire_pipelining.rs::build_handler`.
async fn build_handler(mode: NodeMode) -> ShamirDbHandler {
    let shamir = ShamirDb::init_memory().await.expect("init shamir");
    // `create_db`/`add_repo` (System-owned) persist ResourceMeta::owned_enforced
    // (owner-only 0o700); alice owns the resources so her session passes the gate.
    // The owner id is `principal64([0xAB; 16])` — the SAME projection
    // `session_actor` computes for `alice_session()` above — so the
    // non-superuser session is the recorded owner and the gate admits her.
    let owner = Actor::User(principal64([0xAB; 16]));
    shamir
        .create_db_as("app", owner.clone())
        .await
        .expect("create_db");
    let cfg =
        RepoConfig::new("main", BoxRepoFactory::in_memory()).add_table(TableConfig::new("items"));
    shamir
        .add_repo_as("app", cfg, owner)
        .await
        .expect("add repo");
    ShamirDbHandler::new(Arc::new(shamir)).with_node_mode(mode)
}

// ---------------------------------------------------------------------------
// Batch builders
// ---------------------------------------------------------------------------

/// A write batch: a single upsert of key `k1` into `items`.
fn write_batch_upsert() -> shamir_query_builder::BatchRequest {
    let mut b = Batch::new();
    b.try_upsert(
        "w1",
        upsert("items")
            .key(mpack!({ "id": "k1" }))
            .value(doc! { "id" => "k1", "v" => 1_i64 }),
    )
    .unwrap();
    b.build()
}

/// A write batch: a single insert into `items`.
fn write_batch_insert() -> shamir_query_builder::BatchRequest {
    let mut b = Batch::new();
    b.insert(
        "i1",
        insert("items").row(doc! { "id" => "k2", "v" => 2_i64 }),
    );
    b.build()
}

/// A read-only batch: a single SELECT from `items`.
fn read_batch_select() -> shamir_query_builder::BatchRequest {
    let mut b = Batch::new();
    b.query("r1", Query::from("items"));
    b.build()
}

/// A mixed batch: a read (SELECT) followed by an insert.
fn mixed_batch() -> shamir_query_builder::BatchRequest {
    let mut b = Batch::new();
    b.query("r1", Query::from("items"));
    b.insert(
        "i1",
        insert("items").row(doc! { "id" => "k3", "v" => 3_i64 }),
    );
    b.build()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Case 1 — ReadWrite (default) + write batch (upsert) → success.
/// Zero behavioural change: the gate is a no-op for `ReadWrite`.
#[tokio::test]
async fn readwrite_allows_write_batch() {
    let handler = build_handler(NodeMode::ReadWrite).await;
    let session = alice_session();
    let conn = ConnectionServices::without_push(0);

    let resp = handler
        .execute(
            &session,
            CURRENT_QUERY_LANG_VERSION,
            "app",
            write_batch_upsert(),
            &conn,
        )
        .await;

    assert!(
        matches!(resp, DbResponse::Batch { .. }),
        "ReadWrite node should accept writes, got: {resp:?}",
    );
}

/// Case 2 — ReadOnly + write batch (insert) → `read_only_replica` error.
#[tokio::test]
async fn readonly_rejects_write_batch() {
    let handler = build_handler(NodeMode::ReadOnly).await;
    let session = alice_session();
    let conn = ConnectionServices::without_push(0);

    let resp = handler
        .execute(
            &session,
            CURRENT_QUERY_LANG_VERSION,
            "app",
            write_batch_insert(),
            &conn,
        )
        .await;

    match resp {
        DbResponse::Error { code, message } => {
            assert_eq!(code, "read_only_replica", "wrong code; message: {message}");
            assert!(
                message.contains("is a write") && message.contains("read-only replica"),
                "unexpected message: {message}",
            );
        }
        other => panic!("ReadOnly node should reject writes, got: {other:?}"),
    }
}

/// Case 3 — ReadOnly + read-only batch (SELECT) → success (reads pass).
#[tokio::test]
async fn readonly_allows_read_batch() {
    let handler = build_handler(NodeMode::ReadOnly).await;
    let session = alice_session();
    let conn = ConnectionServices::without_push(0);

    let resp = handler
        .execute(
            &session,
            CURRENT_QUERY_LANG_VERSION,
            "app",
            read_batch_select(),
            &conn,
        )
        .await;

    assert!(
        matches!(resp, DbResponse::Batch { .. }),
        "ReadOnly node should accept reads, got: {resp:?}",
    );
}

/// Case 4 — ReadOnly + mixed batch (read + insert) → rejected; any write
/// in the batch fails the whole batch at the gate.
#[tokio::test]
async fn readonly_rejects_mixed_batch() {
    let handler = build_handler(NodeMode::ReadOnly).await;
    let session = alice_session();
    let conn = ConnectionServices::without_push(0);

    let resp = handler
        .execute(
            &session,
            CURRENT_QUERY_LANG_VERSION,
            "app",
            mixed_batch(),
            &conn,
        )
        .await;

    match resp {
        DbResponse::Error { code, message } => {
            assert_eq!(code, "read_only_replica", "wrong code; message: {message}");
        }
        other => panic!("ReadOnly node should reject mixed batch, got: {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// RV-SERVER-C73466A7 — the interactive-tx path must honour `NodeMode::ReadOnly`
// the same way `execute` does: WRITES are refused with `read_only_replica`
// while permitted READS keep working inside transactions (P0.md: TxBegin is
// not mode-gated; TxExecute classifies with the same `is_write` visitor as
// `execute`; a tx with staged writes never commits).
// ---------------------------------------------------------------------------

/// TxBegin on a `ReadOnly` node is ALLOWED — opening a snapshot transaction
/// is read-capable and the replica must keep serving reads.
#[test]
fn rv_server_c73466a7_readonly_tx_begin_allows_snapshot_transaction() {
    on_big_stack(|| async {
        let handler = build_handler(NodeMode::ReadOnly).await;
        let session = alice_session();

        let resp = handler
            .tx_begin(&session, CURRENT_QUERY_LANG_VERSION, "app", "main", None)
            .await;

        assert!(
            matches!(resp, DbResponse::TxOpened { .. }),
            "ReadOnly node must still open read transactions, got: {resp:?}",
        );
    });
}

/// TxExecute with a SELECT on a `ReadOnly` node is ACCEPTED — reads are
/// preserved inside transactions. Fails on the blanket-gate variant of the
/// fix (which rejected every TxExecute) and on the unfixed code only if
/// the engine refused reads (it does not).
#[test]
fn rv_server_c73466a7_readonly_tx_execute_accepts_select() {
    on_big_stack(|| async {
        let (_db, handler, session, handle) = readonly_tx_fixture().await;

        let resp = handler
            .tx_execute(
                &session,
                CURRENT_QUERY_LANG_VERSION,
                "app",
                handle,
                read_batch_select(),
            )
            .await;

        assert!(
            matches!(resp, DbResponse::TxBatch { .. }),
            "ReadOnly node must serve SELECT inside a tx, got: {resp:?}",
        );
    });
}

/// TxExecute with an INSERT on a `ReadOnly` node → `read_only_replica`,
/// the same contract and classifier as non-transactional `execute`.
/// Fails if ONLY the TxExecute classification is reverted.
#[test]
fn rv_server_c73466a7_readonly_tx_execute_rejects_insert() {
    on_big_stack(|| async {
        let (_db, handler, session, handle) = readonly_tx_fixture().await;

        let resp = handler
            .tx_execute(
                &session,
                CURRENT_QUERY_LANG_VERSION,
                "app",
                handle,
                write_batch_insert(),
            )
            .await;

        match resp {
            DbResponse::Error { code, message } => {
                assert_eq!(code, "read_only_replica", "wrong code; message: {message}");
            }
            other => panic!("ReadOnly node must reject TxExecute INSERT, got: {other:?}"),
        }
    });
}

/// TxCommit of a transaction that staged NO writes succeeds on a
/// `ReadOnly` node — refusing it would strand legitimate read-only txs.
#[test]
fn rv_server_c73466a7_readonly_tx_commit_without_writes_succeeds() {
    on_big_stack(|| async {
        let (_db, handler, session, handle) = readonly_tx_fixture().await;

        let resp = handler.tx_commit(&session, "app", handle).await;

        assert!(
            matches!(resp, DbResponse::TxCommitted { .. }),
            "ReadOnly node must commit a write-free tx, got: {resp:?}",
        );
    });
}

/// TxCommit of a transaction that DID stage writes (staged at engine level,
/// bypassing TxExecute) is REFUSED on a `ReadOnly` node — the write must
/// never land. Fails if ONLY the commit gate is reverted.
#[test]
fn rv_server_c73466a7_readonly_tx_commit_with_staged_writes_rejected() {
    on_big_stack(|| async {
        let (db, handler, session, handle) = readonly_tx_fixture().await;

        // Stage a write directly against the engine, bypassing the
        // handler's TxExecute gate.
        let it = handler
            .tx_registry
            .get_owned(handle, &session.session_id)
            .expect("owned handle");
        let insert = write_batch_insert();
        {
            let mut guard = it.ctx().lock().await;
            let tx = guard.as_mut().expect("tx still open");
            db.tx_execute_as(
                shamir_db::access::Actor::User(shamir_db::access::principal64([0xAB; 16])),
                "app",
                &insert,
                tx,
            )
            .await
            .expect("stage insert at engine level");
        }
        drop(it);

        let resp = handler.tx_commit(&session, "app", handle).await;

        match resp {
            DbResponse::Error { code, message } => {
                assert_eq!(code, "read_only_replica", "wrong code; message: {message}");
                assert!(
                    message.contains("staged writes"),
                    "commit gate must name the staged writes; message: {message}"
                );
            }
            other => panic!("ReadOnly node must refuse committing staged writes, got: {other:?}"),
        }
    });
}

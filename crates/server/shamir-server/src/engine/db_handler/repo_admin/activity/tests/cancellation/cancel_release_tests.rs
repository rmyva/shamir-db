//! EXPORT-ONLINE-TAILS round 2, findings 2 and 3 (engine-level release
//! ordering on the operator-cancel path).
//!
//! `cancel_reply_awaits_the_record_lock_release` — the cancel reply must not
//! return before the record-lock release finished (`release_tx_locks` awaits
//! the tracked release tail).
//!
//! `registry_drop_release_after_close_is_not_lost` — a registry `Drop`
//! release landing AFTER tail admission closed still runs (the detached
//! fallback in `spawn_abort_release`), so a waiter blocked on the record lock
//! proceeds once it is freed.
//!
//! Park claim (dropped deliberately): a waiter blocked on the record lock has
//! NO observable park signal (`MvccStore` exposes only `locks_len`, no waiter
//! count, and no seam fires at the lock-acquire wait), so the tests prove only
//! that the waiter COMPLETES once the lock is gone — never that it was parked
//! at cancel time.

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
use shamir_query_builder::batch::{Batch, Isolation};
use shamir_query_builder::{doc, filter, write};
use shamir_query_types::admin::CancelOutcome;
use shamir_query_types::batch::IsolationLevel;
use shamir_query_types::wire::{DbRequest, DbResponse};
use shamir_sync::durability::tx_cancel::TxParkedState;
use tokio_util::task::AbortOnDropHandle;

use crate::db_handler::handler::ShamirDbHandler;
use crate::version::CURRENT_QUERY_LANG_VERSION;

const TIMEOUT: Duration = Duration::from_secs(120);
const DB: &str = "app";
const REPO: &str = "main";
const TABLE: &str = "items";

fn session(user: u8, sid: u8, superuser: bool) -> Session {
    let mut session = Session::new(
        [user; 16],
        "same-display-name".to_owned(),
        SessionPermissions::new(superuser, false, Vec::new()),
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
    let db = ShamirDb::init_memory().await.expect("in-memory database");
    db.create_db_as(DB, owner.clone()).await.expect("create db");
    db.add_repo_as(
        DB,
        RepoConfig::new(REPO, BoxRepoFactory::in_memory()).add_table(TableConfig::new(TABLE)),
        owner.clone(),
    )
    .await
    .expect("add repo");
    let db = Arc::new(db);
    let handler = ShamirDbHandler::new(Arc::clone(&db));
    (db, handler, owner)
}

async fn wire_request(
    handler: &ShamirDbHandler,
    caller: &Session,
    request: DbRequest,
) -> DbResponse {
    let bytes = rmp_serde::to_vec_named(&request).expect("encode request");
    let raw = handler
        .handle(caller, &bytes, &ConnectionServices::without_push(0))
        .await
        .expect("handle request");
    rmp_serde::from_slice(&raw).expect("decode reply")
}

fn execute(batch: shamir_query_builder::BatchRequest) -> DbRequest {
    DbRequest::Execute {
        query_version: CURRENT_QUERY_LANG_VERSION,
        db: DB.into(),
        batch,
    }
}

fn update_where(where_n: i64, set_n: i64) -> shamir_query_types::write::UpdateOp {
    write::Update::with_repo(REPO, TABLE)
        .where_(filter::eq("n", where_n))
        .set(doc! { "n" => set_n })
        .build()
        .expect("build fixture update")
}

async fn begin_locked(handler: &ShamirDbHandler, owner: &Session, set_n: i64) -> u64 {
    let handle = match wire_request(
        handler,
        owner,
        DbRequest::TxBegin {
            query_version: CURRENT_QUERY_LANG_VERSION,
            db: DB.into(),
            repo: REPO.into(),
            isolation: Some(IsolationLevel::Pessimistic),
        },
    )
    .await
    {
        DbResponse::TxOpened { tx_handle, .. } => tx_handle,
        other => panic!("pessimistic begin: {other:?}"),
    };
    let mut staged = Batch::new();
    staged.update("staged", update_where(1, set_n));
    // The interactive stage goes through `DbRequest::TxExecute` carrying the
    // tx handle (handler/core.rs routes it to `tx_execute_with_rid`); a plain
    // `Execute` would run OUTSIDE the transaction and hold no record lock.
    let staged = wire_request(
        handler,
        owner,
        DbRequest::TxExecute {
            query_version: CURRENT_QUERY_LANG_VERSION,
            db: DB.into(),
            tx_handle: handle,
            batch: staged.build(),
        },
    )
    .await;
    assert!(
        matches!(&staged, DbResponse::TxBatch { .. }),
        "the staged pessimistic update must accumulate: {staged:?}"
    );
    handle
}

fn wire_cancel(_handler: &ShamirDbHandler, _caller: &Session, id: u64) -> DbRequest {
    let mut batch = Batch::new();
    batch.op("cancel", shamir_query_builder::wire::admin::cancel_tx(id));
    execute(batch.build())
}

/// Kills the mutant that discards the tracked release tail's join
/// (`tails.spawn(work)` and return in `release_tx_locks`): on a
/// CURRENT-THREAD runtime the spawned release is never polled before this
/// read, so the lock count right after the reply stays above the baseline.
/// With the awaited receipt the locks are gone when the reply is in hand.
#[tokio::test(flavor = "current_thread")]
async fn cancel_reply_awaits_the_record_lock_release() {
    tokio::time::timeout(TIMEOUT, async {
        let (db, handler, _owner_actor) = fixture().await;
        // Superuser: the fixture database is owned by another principal, so a
        // plain user would be refused at the seed insert.
        let writer = session(3, 3, true);
        let mut seed = Batch::new();
        seed.id("ins");
        seed.insert("i", write::insert(TABLE).rows([doc! { "n" => 1 }]));
        assert!(matches!(
            wire_request(&handler, &writer, execute(seed.build())).await,
            DbResponse::Batch { .. }
        ));
        let mvcc = db
            .get_table(DB, REPO, TABLE)
            .await
            .expect("open table")
            .mvcc_store_for_test()
            .expect("MVCC table");
        let baseline = mvcc.locks_len();
        let handle = begin_locked(&handler, &writer, 5).await;
        assert!(
            mvcc.locks_len() > baseline,
            "the staged write holds the row's record lock"
        );
        let cancelled =
            wire_request(&handler, &writer, wire_cancel(&handler, &writer, handle)).await;
        assert!(
            matches!(
                &cancelled,
                DbResponse::CancelOutcome {
                    outcome: CancelOutcome::Cancelled
                }
            ),
            "the idle transaction must be cancelled: {cancelled:?}"
        );
        // THE assertion: with the awaited release tail the locks are gone the
        // moment the reply is in hand — no yield, no wait. No competing writer
        // exists yet: `locks_len` counts entries, and an entry a waiter pinned
        // is retired only when its last lease drops, so a waiter started
        // earlier could keep the count above the baseline after a correct
        // release.
        assert_eq!(
            mvcc.locks_len(),
            baseline,
            "the record lock must be released BEFORE the cancel reply returns"
        );
        // A writer on the same record: its completion shows the freed lock is
        // usable end-to-end.
        let waiter_handler = Arc::new(handler.clone());
        let waiter_session = session(4, 4, true);
        let mut blocked = Batch::new();
        blocked.transactional().isolation(Isolation::Pessimistic);
        blocked.update("blocked", update_where(1, -1));
        let waiter = AbortOnDropHandle::new(tokio::spawn(async move {
            wire_request(&waiter_handler, &waiter_session, execute(blocked.build())).await
        }));
        let response = tokio::time::timeout(TIMEOUT, waiter)
            .await
            .expect("the waiter on the released lock must complete after the release")
            .expect("waiter task joined");
        assert!(
            matches!(&response, DbResponse::Batch { .. }),
            "the unblocked update must complete normally: {response:?}"
        );
        assert!(
            handler.tx_registry().is_empty(),
            "no parked transaction survives"
        );
    })
    .await
    .expect("cancel-await scenario timed out");
}

/// Kills the mutant that DROPPED a release refused by a closed tail
/// admission (the old `spawn_abort_release` refusal arm AND the old
/// `is_closed()`-then-`spawn()` handoff): after `ReleaseTails::close`,
/// emptying the registry (the shutdown order: producers stopped → registry
/// emptied → admission closed) must STILL free the parked waiter's record
/// lock.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn registry_drop_release_after_close_is_not_lost() {
    tokio::time::timeout(
        TIMEOUT,
        Box::pin(async {
            let (db, handler, _owner_actor) = fixture().await;
            let writer = session(1, 1, true);
            let mut seed = Batch::new();
            seed.id("ins");
            seed.insert("i", write::insert(TABLE).rows([doc! { "n" => 1 }]));
            assert!(matches!(
                wire_request(&handler, &writer, execute(seed.build())).await,
                DbResponse::Batch { .. }
            ));
            let mvcc = db
                .get_table(DB, REPO, TABLE)
                .await
                .expect("open table")
                .mvcc_store_for_test()
                .expect("MVCC table");
            let baseline = mvcc.locks_len();
            let handle = begin_locked(&handler, &writer, 5).await;
            assert!(
                mvcc.locks_len() > baseline,
                "the staged write holds the lock"
            );
            let waiter_handler = Arc::new(handler.clone());
            let waiter_session = session(2, 2, true);
            let mut blocked = Batch::new();
            blocked.transactional().isolation(Isolation::Pessimistic);
            blocked.update("blocked", update_where(1, -1));
            let waiter = AbortOnDropHandle::new(tokio::spawn(async move {
                wire_request(&waiter_handler, &waiter_session, execute(blocked.build())).await
            }));
            // NOTE: no park handshake exists (see the module doc); no yield
            // stands in for one here — the waiter's completion, not its park,
            // is claimed.
            // Shutdown closes tail admission (stage 8b) BEFORE the last
            // transaction reference drops (the registry emptying, the
            // waiter's Arc): the Drop release lands on a CLOSED owner.
            let registry = handler.tx_registry();
            registry.release_tails().expect("registry owner").close();
            drop(registry.remove(handle).expect("the parked tx is removed"));
            assert!(
                matches!(registry.peek(handle), TxParkedState::Gone),
                "the removed handle is gone from the registry"
            );
            let response = tokio::time::timeout(TIMEOUT, waiter)
                .await
                .expect(
                    "the waiter on the released lock must complete despite the closed admission",
                )
                .expect("waiter task joined");
            assert!(
                matches!(&response, DbResponse::Batch { .. }),
                "the unblocked update must complete normally: {response:?}"
            );
            assert_eq!(
                mvcc.locks_len(),
                baseline,
                "the record lock is released despite the closed admission"
            );
        }),
    )
    .await
    .expect("registry-drop-after-close scenario timed out");
}

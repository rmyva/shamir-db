//! Commit cleanup regressions exercise real pessimistic locks and wire commits.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use shamir_connect::common::types::{BindingMode, TransportKind};
use shamir_connect::server::conn_services::ConnectionServices;
use shamir_connect::server::dispatch::RequestHandler;
use shamir_connect::server::session::{Session, SessionPermissions};
use shamir_db::access::{principal64, Actor};
use shamir_db::engine::repo::{BoxRepoFactory, RepoConfig};
use shamir_db::engine::table::TableConfig;
use shamir_db::engine::tx::release_interactive_tx_locks;
use shamir_db::storage::types::RecordKey;
use shamir_db::ShamirDb;
use shamir_query_builder::batch::{Batch, Isolation};
use shamir_query_builder::{doc, filter, write};
use shamir_query_types::batch::IsolationLevel;
use shamir_query_types::wire::{DbRequest, DbResponse};
use shamir_sync::durability::tx_cancel::TxParkedState;
use shamir_test_diag::FlakeGuard;
use shamir_tx::{LockMode, MvccStore, RepoTxGate, TxContext};
use shamir_types::access::{ResourceMeta, ResourcePath};
use tokio::sync::{oneshot, Notify};
use tokio_util::task::AbortOnDropHandle;

use crate::db_handler::handler::ShamirDbHandler;
use crate::tx_registry::{InteractiveTx, TxAbortRelease};
use crate::version::CURRENT_QUERY_LANG_VERSION;

const TIMEOUT: Duration = Duration::from_secs(120);
const DB: &str = "app";
const REPO: &str = "main";
const TABLE: &str = "items";

fn session(user: u8, sid: u8, superuser: bool) -> Session {
    let mut session = Session::new(
        [user; 16],
        format!("user-{user}"),
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
    let db = ShamirDb::init_memory().await.unwrap();
    db.create_db_as(DB, owner.clone()).await.unwrap();
    db.add_repo_as(
        DB,
        RepoConfig::new(REPO, BoxRepoFactory::in_memory()).add_table(TableConfig::new(TABLE)),
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
        query_version: CURRENT_QUERY_LANG_VERSION,
        db: DB.into(),
        batch,
    }
}
fn commit_request(id: u64) -> DbRequest {
    DbRequest::TxCommit {
        query_version: CURRENT_QUERY_LANG_VERSION,
        visibility_wait: None,
        db: DB.into(),
        tx_handle: id,
    }
}
fn begin_request() -> DbRequest {
    DbRequest::TxBegin {
        query_version: CURRENT_QUERY_LANG_VERSION,
        db: DB.into(),
        repo: REPO.into(),
        isolation: Some(IsolationLevel::Pessimistic),
    }
}
fn update_where(where_n: i64, set_n: i64) -> shamir_query_types::write::UpdateOp {
    write::Update::with_repo(REPO, TABLE)
        .where_(filter::eq("n", where_n))
        .set(doc! { "n" => set_n })
        .build()
        .unwrap()
}
fn stage_update(id: u64, where_n: i64, set_n: i64) -> DbRequest {
    let mut batch = Batch::new();
    batch.update("staged", update_where(where_n, set_n));
    DbRequest::TxExecute {
        query_version: CURRENT_QUERY_LANG_VERSION,
        db: DB.into(),
        tx_handle: id,
        batch: batch.build(),
    }
}
fn blocked_update(n: i64) -> DbRequest {
    let mut batch = Batch::new();
    batch.transactional().isolation(Isolation::Pessimistic);
    batch.update("blocked", update_where(n, -n));
    execute(batch.build())
}
async fn storage(handler: &ShamirDbHandler) -> (Arc<RepoTxGate>, Arc<MvccStore>) {
    let db = handler.db().get_db(DB).unwrap();
    let table = db.get_table(REPO, TABLE).await.unwrap();
    let gate = db.get_repo(REPO).unwrap().tx_gate().await.unwrap();
    (gate, table.mvcc_store_for_test().unwrap())
}
async fn seed_row(handler: &ShamirDbHandler, admin: &Session, n: i64) -> RecordKey {
    let mut batch = Batch::new();
    batch.id("ins");
    batch.insert("i", write::insert(TABLE).rows([doc! { "n" => n }]));
    let DbResponse::Batch { response } = wire(handler, admin, execute(batch.build())).await else {
        panic!("seed insert failed")
    };
    let id: shamir_types::types::record_id::RecordId = response.results["i"].records[0]
        .get_value_str("_id")
        .unwrap()
        .parse()
        .unwrap();
    RecordKey::from_slice(id.as_bytes())
}
async fn begin_locked_wire(handler: &ShamirDbHandler, owner: &Session, n: i64) -> u64 {
    let id = match wire(handler, owner, begin_request()).await {
        DbResponse::TxOpened { tx_handle, .. } => tx_handle,
        x => panic!("begin: {x:?}"),
    };
    assert!(matches!(
        wire(handler, owner, stage_update(id, n, n + 100)).await,
        DbResponse::TxBatch { .. }
    ));
    id
}
fn spawn_waiter(
    handler: &Arc<ShamirDbHandler>,
    waiter: Session,
    n: i64,
) -> AbortOnDropHandle<DbResponse> {
    let handler = Arc::clone(handler);
    AbortOnDropHandle::new(tokio::spawn(async move {
        wire(&handler, &waiter, blocked_update(n)).await
    }))
}
async fn expect_waiter_proceeds(pending: AbortOnDropHandle<DbResponse>) {
    let response = tokio::time::timeout(TIMEOUT, pending)
        .await
        .unwrap()
        .unwrap();
    let DbResponse::Batch { response } = response else {
        panic!("waiter response: {response:?}")
    };
    assert!(response.results.contains_key("blocked"));
}
async fn wait_for_lock_release(mvcc: &MvccStore, baseline: usize) {
    tokio::time::timeout(TIMEOUT, async {
        loop {
            if mvcc.locks_len() == baseline {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

// The fixture's lock is registered in TxContext with the exact table token/key,
// so the production release routine removes the real MVCC lock.
async fn manual_locked(
    db: &ShamirDb,
    handler: &ShamirDbHandler,
    owner_actor: &Actor,
    who: &Session,
    key: RecordKey,
    release: TxAbortRelease,
) -> u64 {
    let repo = db.get_db(DB).unwrap().get_repo(REPO).unwrap();
    let table = db.get_table(DB, REPO, TABLE).await.unwrap();
    let token = table.table_token();
    let mvcc = table.mvcc_store_for_test().unwrap();
    let (ctx, snapshot, _repo) = db
        .tx_begin_as(owner_actor.clone(), DB, REPO, "pessimistic")
        .await
        .unwrap();
    ctx.record_locked_key(token, key.clone());
    mvcc.lock_key(
        key,
        ctx.tx_id.0,
        Arc::new(AtomicBool::new(false)),
        Arc::new(Notify::new()),
        LockMode::Exclusive,
    )
    .await
    .unwrap();
    let id = ctx.tx_id.0;
    handler
        .tx_registry()
        .register_activity(
            id,
            InteractiveTx::new(
                ctx,
                snapshot,
                who.session_id,
                who.user_id,
                DB.into(),
                REPO.into(),
                Duration::from_secs(3_600),
                release,
            )
            .with_bound_repo(repo),
            who.activity_handle(),
            Arc::from(who.username.as_str()),
        )
        .unwrap();
    id
}
fn release_real(db: &ShamirDb, count: Arc<AtomicUsize>) -> TxAbortRelease {
    let bound = db.get_db(DB).unwrap().get_repo(REPO).unwrap();
    Arc::new(move |ctx| {
        let count = Arc::clone(&count);
        let core = bound.core().clone();
        Box::pin(async move {
            count.fetch_add(1, Ordering::SeqCst);
            release_interactive_tx_locks(&ctx, &core).await;
        })
    })
}
fn parked_release_waiter(
    db: &ShamirDb,
    count: Arc<AtomicUsize>,
) -> (
    TxAbortRelease,
    oneshot::Receiver<()>,
    oneshot::Sender<()>,
    oneshot::Receiver<()>,
) {
    let (started_tx, started_rx) = oneshot::channel();
    let (gate_tx, gate_rx) = oneshot::channel();
    let (finished_tx, finished_rx) = oneshot::channel();
    let started_tx = Arc::new(Mutex::new(Some(started_tx)));
    let gate_rx = Arc::new(Mutex::new(Some(gate_rx)));
    let finished_tx = Arc::new(Mutex::new(Some(finished_tx)));
    let bound = db.get_db(DB).unwrap().get_repo(REPO).unwrap();
    let release: TxAbortRelease = Arc::new(move |ctx: TxContext| {
        let started = started_tx.lock().unwrap().take().unwrap();
        let gate = gate_rx.lock().unwrap().take().unwrap();
        let finished = finished_tx.lock().unwrap().take().unwrap();
        let count = Arc::clone(&count);
        let core = bound.core().clone();
        Box::pin(async move {
            let _ = started.send(());
            let _ = gate.await;
            count.fetch_add(1, Ordering::SeqCst);
            release_interactive_tx_locks(&ctx, &core).await;
            let _ = finished.send(());
        })
    });
    (release, started_rx, gate_tx, finished_rx)
}
async fn wait<T>(rx: oneshot::Receiver<T>, why: &str) -> T {
    tokio::time::timeout(TIMEOUT, rx)
        .await
        .unwrap_or_else(|_| panic!("{why} timeout"))
        .unwrap_or_else(|_| panic!("{why} sender dropped"))
}

/// Denial must await one release before reply on Tokio's default current-thread runtime. Kills denial-leaves-locks, reply-before-release, and double-release.
#[tokio::test]
async fn denied_commit_releases_exactly_once() {
    let guard = FlakeGuard::with_watchdog("denied_commit_releases_exactly_once", TIMEOUT);
    tokio::time::timeout(TIMEOUT, async {
        let (db, handler, actor) = fixture().await;
        let owner = session(1, 1, false);
        let admin = session(9, 9, true);
        let key = seed_row(&handler, &admin, 1).await;
        let (_, mvcc) = storage(&handler).await;
        let base = mvcc.locks_len();
        let count = Arc::new(AtomicUsize::new(0));
        let id = manual_locked(
            &db,
            &handler,
            &actor,
            &owner,
            key,
            release_real(&db, count.clone()),
        )
        .await;
        db.set_resource_meta(
            &ResourcePath::database(DB),
            &ResourceMeta::owned_enforced(Actor::System),
        )
        .await
        .expect("narrow ACL");
        let DbResponse::Error { code, message } = wire(&handler, &owner, commit_request(id)).await
        else {
            panic!("expected denial")
        };
        assert_eq!(code, "access_denied");
        assert!(message.contains("[access_denied]"));
        assert_eq!(count.load(Ordering::SeqCst), 1);
        assert!(matches!(
            handler.tx_registry().peek(id),
            TxParkedState::Gone
        ));
        let next = begin_locked_wire(&handler, &session(2, 2, true), 1).await;
        assert!(matches!(
            wire(
                &handler,
                &session(2, 2, true),
                DbRequest::TxRollback {
                    db: DB.into(),
                    tx_handle: next
                }
            )
            .await,
            DbResponse::TxRolledBack { .. }
        ));
        wait_for_lock_release(&mvcc, base).await;
    })
    .await
    .expect("scenario timeout");
    guard.mark("single awaited release");
}

/// Detached release survives cancellation; current-thread runtime. Kills cancelled-inline-release.
#[tokio::test]
async fn drop_during_denial_release_still_releases() {
    let guard = FlakeGuard::with_watchdog("drop_during_denial_release_still_releases", TIMEOUT);
    tokio::time::timeout(TIMEOUT, async {
        let (db, handler, actor) = fixture().await;
        let owner = session(1, 1, false);
        let key = seed_row(&handler, &session(9, 9, true), 1).await;
        let (_, mvcc) = storage(&handler).await;
        let base = mvcc.locks_len();
        let count = Arc::new(AtomicUsize::new(0));
        let (release, started, gate, finished) = parked_release_waiter(&db, count.clone());
        let id = manual_locked(&db, &handler, &actor, &owner, key, release).await;
        db.set_resource_meta(
            &ResourcePath::database(DB),
            &ResourceMeta::owned_enforced(Actor::System),
        )
        .await
        .unwrap();
        let h = handler.clone();
        let who = session(1, 1, false);
        let task = tokio::spawn(async move { wire(&h, &who, commit_request(id)).await });
        wait(started, "release started").await;
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        gate.send(()).unwrap();
        wait(finished, "release finished").await;
        let next = begin_locked_wire(&handler, &session(9, 9, true), 1).await;
        assert!(matches!(
            wire(
                &handler,
                &session(9, 9, true),
                DbRequest::TxRollback {
                    db: DB.into(),
                    tx_handle: next
                }
            )
            .await,
            DbResponse::TxRolledBack { .. }
        ));
        wait_for_lock_release(&mvcc, base).await;
    })
    .await
    .expect("scenario timeout");
    guard.mark("release survived abort");
}

/// Cancellation after commit handoff retains cleanup ownership; current-thread runtime. Kills no-cleanup-owner.
#[tokio::test]
async fn drop_before_the_pipeline_releases() {
    let guard = FlakeGuard::with_watchdog("drop_before_the_pipeline_releases", TIMEOUT);
    tokio::time::timeout(TIMEOUT, async {
        let (db, base, actor) = fixture().await; let owner = session(1, 1, true); let key = seed_row(&base, &session(9, 9, true), 1).await;
        let (_, mvcc) = storage(&base).await; let baseline = mvcc.locks_len(); let count = Arc::new(AtomicUsize::new(0)); let (release, started, gate, finished) = parked_release_waiter(&db, count.clone());
        let id = manual_locked(&db, &base, &actor, &owner, key, release).await; let reached = Arc::new(tokio::sync::Barrier::new(2)); let resume = Arc::new(tokio::sync::Barrier::new(2));
        let handler = base.clone().with_commit_handoff_pause(id, reached.clone(), resume);
        let h = handler.clone(); let who = session(1, 1, true); let task = tokio::spawn(async move { wire(&h, &who, commit_request(id)).await });
        tokio::time::timeout(TIMEOUT, reached.wait()).await.unwrap(); task.abort(); assert!(task.await.unwrap_err().is_cancelled()); wait(started, "cleanup release started").await; gate.send(()).unwrap(); wait(finished, "cleanup release finished").await;
        assert_eq!(count.load(Ordering::SeqCst), 1);
        let next = begin_locked_wire(&handler, &session(2, 2, true), 1).await;
        assert!(matches!(wire(&handler, &session(2, 2, true), DbRequest::TxRollback { db: DB.into(), tx_handle: next }).await, DbResponse::TxRolledBack { .. }));
        wait_for_lock_release(&mvcc, baseline).await;
        assert!(matches!(wire(&handler, &owner, DbRequest::TxExecute { query_version: CURRENT_QUERY_LANG_VERSION, db: DB.into(), tx_handle: id, batch: Batch::new().build() }).await, DbResponse::Error { code, .. } if code == "tx_not_found"));
    }).await.expect("scenario timeout");
    guard.mark("cleanup owned release");
}

/// A pessimistic TxExecute writer waits on the held key, then performs a real write after rollback.
/// The MVCC lock implementation waits (it does not return a conflict), so this scenario bounds
/// the wait and asserts the second writer's actual successful batch after release.
#[tokio::test]
async fn pessimistic_writer_proceeds_after_owner_rollback() {
    let guard =
        FlakeGuard::with_watchdog("pessimistic_writer_proceeds_after_owner_rollback", TIMEOUT);
    tokio::time::timeout(TIMEOUT, async {
        let (db, handler, actor) = fixture().await;
        let owner = session(1, 1, true);
        let key = seed_row(&handler, &session(9, 9, true), 1).await;
        let (_, mvcc) = storage(&handler).await;
        let baseline = mvcc.locks_len();
        let id = manual_locked(
            &db,
            &handler,
            &actor,
            &owner,
            key,
            release_real(&db, Arc::new(AtomicUsize::new(0))),
        )
        .await;
        let pending = spawn_waiter(&Arc::new(handler.clone()), session(2, 2, true), 1);
        tokio::task::yield_now().await;
        assert!(
            !pending.is_finished(),
            "second writer must be waiting for A's exclusive lock"
        );
        assert!(matches!(
            wire(
                &handler,
                &owner,
                DbRequest::TxRollback {
                    db: DB.into(),
                    tx_handle: id
                }
            )
            .await,
            DbResponse::TxRolledBack { .. }
        ));
        expect_waiter_proceeds(pending).await;
        wait_for_lock_release(&mvcc, baseline).await;
    })
    .await
    .expect("pessimistic writer scenario timed out");
    guard.mark("blocked writer succeeded after rollback");
}

#[tokio::test]
async fn successful_commit_never_calls_the_releaser() {
    let guard = FlakeGuard::with_watchdog("successful_commit_never_calls_the_releaser", TIMEOUT);
    tokio::time::timeout(TIMEOUT, async {
        let (db, handler, actor) = fixture().await; let owner = session(1, 1, true); let key = seed_row(&handler, &session(9, 9, true), 1).await;
        let (_, mvcc) = storage(&handler).await; let baseline = mvcc.locks_len(); let count = Arc::new(AtomicUsize::new(0));
        let id = manual_locked(&db, &handler, &actor, &owner, key, release_real(&db, count.clone())).await;
        assert!(matches!(wire(&handler, &owner, commit_request(id)).await, DbResponse::TxCommitted { ref transaction } if transaction.is_committed()));
        assert!(matches!(wire(&handler, &session(2, 2, true), blocked_update(1)).await, DbResponse::Batch { .. }));
        wait_for_lock_release(&mvcc, baseline).await; assert_eq!(count.load(Ordering::SeqCst), 0);
    }).await.expect("scenario timeout");
    guard.mark("successful commit avoided cleanup releaser");
}

/// Begin binds the repository the context opened on: a rename+recreate during
/// the begin seam leaves no transaction and answers server_busy. Kills the
/// bind-post-resolved-repo mutant (identity compare omitted).
#[tokio::test]
async fn begin_binds_the_repository_it_opened_on() {
    let guard = FlakeGuard::with_watchdog("begin_binds_the_repository_it_opened_on", TIMEOUT);
    tokio::time::timeout(TIMEOUT, async {
        let (db, handler, owner_actor) = fixture().await;
        let owner = session(1, 1, true);
        let reached = Arc::new(tokio::sync::Barrier::new(2));
        let resume = Arc::new(tokio::sync::Barrier::new(2));
        let task_handler = handler.clone();
        task_handler
            .begin_pause
            .install(Arc::clone(&reached), Arc::clone(&resume));
        let task_owner = session(1, 1, true);
        let task =
            tokio::spawn(async move { wire(&task_handler, &task_owner, begin_request()).await });

        reached.wait().await;
        db.rename_repo_as(DB, REPO, "renamed", Actor::System)
            .await
            .unwrap();
        db.add_repo_as(
            DB,
            RepoConfig::new(REPO, BoxRepoFactory::in_memory()).add_table(TableConfig::new(TABLE)),
            owner_actor,
        )
        .await
        .unwrap();
        resume.wait().await;
        assert!(
            matches!(task.await.unwrap(), DbResponse::Error { code, .. } if code == "server_busy")
        );
        assert!(handler.tx_registry().is_empty());

        let tx_handle = match wire(&handler, &owner, begin_request()).await {
            DbResponse::TxOpened { tx_handle, .. } => tx_handle,
            response => panic!("control begin: {response:?}"),
        };
        assert!(matches!(
            wire(
                &handler,
                &owner,
                DbRequest::TxRollback {
                    db: DB.into(),
                    tx_handle,
                },
            )
            .await,
            DbResponse::TxRolledBack { .. }
        ));
        assert!(handler.tx_registry().is_empty());
    })
    .await
    .expect("begin scenario timeout");
    guard.mark("begin kept its resolved repository binding");
}

/// Bound repository survives rename and releases its row lock. Default current-thread runtime, matching tx_release_tests. Kills by-name-commit and commit-does-not-release.
/// A commit addressed to the parked repository NAME is refused with a typed error when the name
/// no longer resolves to the repository the transaction began on, and the record locks are
/// released through the releaser bound to the parked repository (the waiter blocked on the old
/// store proceeds). Mutant: committing by name without the identity check lands the write in the
/// replacement; leaking the locks on the refusal leaves the waiter blocked.
async fn refused_after_rename(recreate: bool, expected: &str, label: &str) {
    let guard = FlakeGuard::with_watchdog(label, TIMEOUT);
    tokio::time::timeout(TIMEOUT, async {
        let (db, handler, actor) = fixture().await;
        let owner = session(1, 1, true);
        let admin = session(9, 9, true);
        let _key = seed_row(&handler, &admin, 1).await;
        let id = begin_locked_wire(&handler, &owner, 1).await;
        let pending = spawn_waiter(&Arc::new(handler.clone()), session(2, 2, true), 1);
        tokio::task::yield_now().await;
        assert!(!pending.is_finished());
        db.rename_repo_as(DB, REPO, "renamed", Actor::System)
            .await
            .unwrap();
        if recreate {
            db.add_repo_as(
                DB,
                RepoConfig::new(REPO, BoxRepoFactory::in_memory())
                    .add_table(TableConfig::new(TABLE)),
                actor,
            )
            .await
            .unwrap();
        }
        let DbResponse::Error { message, .. } = wire(&handler, &owner, commit_request(id)).await
        else {
            panic!("a commit after the rename must be refused");
        };
        assert!(message.contains(expected), "unexpected refusal: {message}");
        expect_waiter_proceeds(pending).await;
        let mut read = Batch::new();
        read.query(
            "read",
            shamir_query_builder::Query::with_repo("renamed", TABLE),
        );
        let DbResponse::Batch { response } = wire(&handler, &owner, execute(read.build())).await
        else {
            panic!("renamed read")
        };
        assert_eq!(response.results["read"].records.len(), 1);
        if recreate {
            let mut read = Batch::new();
            read.query("read", shamir_query_builder::Query::with_repo(REPO, TABLE));
            let DbResponse::Batch { response } =
                wire(&handler, &owner, execute(read.build())).await
            else {
                panic!("replacement read")
            };
            assert!(response.results["read"].records.is_empty());
        }
    })
    .await
    .expect("rename scenario timeout");
    guard.mark("refused and released");
}

#[tokio::test]
async fn commit_after_repo_rename_fails_typed_and_releases_the_locks() {
    refused_after_rename(
        false,
        "Repository 'main' not found",
        "commit_after_repo_rename_fails_typed_and_releases_the_locks",
    )
    .await;
}

#[tokio::test]
async fn commit_after_repo_rename_and_recreation_is_refused_not_diverted() {
    refused_after_rename(
        true,
        "changed since the transaction began",
        "commit_after_repo_rename_and_recreation_is_refused_not_diverted",
    )
    .await;
}

//! EXPORT-ONLINE-TAILS round 3 — the DROPPED cancel dispatch.
//!
//! After `ctx.take()` the transaction context lives only inside the inline
//! release future of `release_tx_locks` (tail admission already closed), so
//! dropping the cancel dispatch there would drop the context with it: the
//! registry's `Drop` cannot retry (it acts only while the context is still
//! `Some`). The drop-guard in `release_tx_locks` must instead spawn the
//! not-yet-completed release DETACHED, and a waiter parked on the record
//! lock must still wake.
//!
//! Flavour: CURRENT-THREAD — deterministic interleaving (the abort of the
//! dispatch task runs the guard's `Drop` synchronously at task teardown;
//! the spawned fallback runs when the test next awaits).

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
use shamir_query_types::batch::IsolationLevel;
use shamir_query_types::wire::{DbRequest, DbResponse};
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

async fn fixture() -> (Arc<ShamirDb>, ShamirDbHandler) {
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
    (db, handler)
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

fn wire_cancel(_handler: &ShamirDbHandler, _caller: &Session, id: u64) -> DbRequest {
    let mut batch = Batch::new();
    batch.op("cancel", shamir_query_builder::wire::admin::cancel_tx(id));
    execute(batch.build())
}

/// Kills the mutant that awaits the refused release INLINE without the
/// drop-guard (`Err(work) => work.await`): the dispatch is aborted while the
/// inline release is pending, and without the guard the aborted future drops
/// the taken `TxContext` with it — the waiter parked on the record lock
/// never wakes and the lock count never returns to the baseline. With the
/// guard the release continues detached and the waiter wakes.
#[tokio::test(flavor = "current_thread")]
async fn dropped_dispatch_still_completes_the_release() {
    tokio::time::timeout(TIMEOUT, async {
        let (db, base) = fixture().await;
        let writer = session(1, 1, true);
        let mut seed = Batch::new();
        seed.id("ins");
        seed.insert("i", write::insert(TABLE).rows([doc! { "n" => 1 }]));
        assert!(matches!(
            wire_request(&base, &writer, execute(seed.build())).await,
            DbResponse::Batch { .. }
        ));
        let mvcc = db
            .get_table(DB, REPO, TABLE)
            .await
            .expect("open table")
            .mvcc_store_for_test()
            .expect("MVCC table");
        let baseline = mvcc.locks_len();
        // Open a pessimistic tx and stage a write so it holds the row lock.
        let handle = match wire_request(
            &base,
            &writer,
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
        staged.update("staged", update_where(1, 5));
        // The interactive stage is `TxExecute` carrying the tx handle; a plain
        // `Execute` would run outside the transaction and hold no record lock.
        let staged = wire_request(
            &base,
            &writer,
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
            "staged: {staged:?}"
        );
        assert!(
            mvcc.locks_len() > baseline,
            "the staged write holds the row's record lock"
        );
        // A waiter on the record lock: whether it parks before or after the
        // release, it can only complete once the lock is released, so its
        // completion is the end-to-end proof the release ran.
        let waiter_handler = Arc::new(base.clone());
        let waiter_session = session(2, 2, true);
        let mut blocked = Batch::new();
        blocked.transactional().isolation(Isolation::Pessimistic);
        blocked.update("blocked", update_where(1, -1));
        let waiter = AbortOnDropHandle::new(tokio::spawn(async move {
            wire_request(&waiter_handler, &waiter_session, execute(blocked.build())).await
        }));
        // Shutdown order stage 8b: close tail admission BEFORE the cancel
        // dispatch, so `release_tx_locks` takes the inline arm.
        base.tx_registry()
            .release_tails()
            .expect("registry owner")
            .close();
        // The consumed-once pause parks the dispatch INSIDE the guarded
        // region (guard alive, release future not yet polled) and keeps it
        // parked after the rendezvous, so the abort below drops the guard
        // mid-guard, never after the release completed.
        let reached = Arc::new(tokio::sync::Barrier::new(2));
        let dispatch_handler = Arc::new(
            base.clone()
                .with_cancel_release_pause(handle, Arc::clone(&reached)),
        );
        let dispatch_writer = session(1, 1, true);
        let dispatch = AbortOnDropHandle::new(tokio::spawn(async move {
            wire_request(
                &dispatch_handler,
                &dispatch_writer,
                wire_cancel(&base, &writer, handle),
            )
            .await
        }));
        // Both sides meet here; the guard is alive and the release pending.
        reached.wait().await;
        // Dropping the dispatch (abort) runs the guard's `Drop`, which must
        // spawn the release detached instead of losing the taken context.
        dispatch.abort();
        let result = dispatch.await;
        assert!(
            matches!(&result, Err(error) if error.is_cancelled()),
            "the dispatch was dropped mid-release: {result:?}"
        );
        let response = tokio::time::timeout(TIMEOUT, waiter)
            .await
            .expect("the parked waiter must wake even though the dispatch was dropped")
            .expect("waiter task joined");
        assert!(
            matches!(&response, DbResponse::Batch { .. }),
            "the unblocked update must complete normally: {response:?}"
        );
        assert_eq!(
            mvcc.locks_len(),
            baseline,
            "the record lock is released despite the dropped dispatch"
        );
    })
    .await
    .expect("dropped-dispatch scenario timed out");
}

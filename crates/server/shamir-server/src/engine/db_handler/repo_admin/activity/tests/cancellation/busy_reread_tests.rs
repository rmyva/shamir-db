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
use shamir_query_builder::batch::Batch;
use shamir_query_builder::wire::admin::cancel_tx;
use shamir_query_types::admin::activity::CancelOutcome;
use shamir_query_types::wire::{DbRequest, DbResponse};
use shamir_sync::durability::tx_cancel::TxParkedState;
use shamir_test_diag::FlakeGuard;

use crate::db_handler::activity::CancelPauseTarget;
use crate::db_handler::handler::ShamirDbHandler;
use crate::tx_registry::InteractiveTx;
use crate::version::CURRENT_QUERY_LANG_VERSION;

const TIMEOUT: Duration = Duration::from_secs(120);

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
    db.create_db_as("app", owner.clone()).await.unwrap();
    db.add_repo_as(
        "app",
        RepoConfig::new("main", BoxRepoFactory::in_memory()).add_table(TableConfig::new("items")),
        owner.clone(),
    )
    .await
    .unwrap();
    let db = Arc::new(db);
    let handler = ShamirDbHandler::new(Arc::clone(&db));
    (db, handler, owner)
}

async fn parked(handler: &ShamirDbHandler, db: &ShamirDb, owner: &Actor, who: &Session) -> u64 {
    let (tx, snapshot, repo) = db
        .tx_begin_as(owner.clone(), "app", "main", "snapshot")
        .await
        .unwrap();
    let id = tx.tx_id.0;
    handler
        .tx_registry()
        .register_activity(
            id,
            InteractiveTx::new(
                tx,
                snapshot,
                who.session_id,
                who.user_id,
                "app".into(),
                "main".into(),
                Duration::from_secs(3_600),
                handler.tx_abort_release("app", "main"),
            )
            .with_bound_repo(repo),
            who.activity_handle(),
            Arc::from(who.username.as_str()),
        )
        .unwrap();
    id
}

async fn wire_cancel(handler: &ShamirDbHandler, caller: &Session, id: u64) -> DbResponse {
    let mut batch = Batch::new();
    batch.op("cancel", cancel_tx(id));
    let request = DbRequest::Execute {
        query_version: CURRENT_QUERY_LANG_VERSION,
        db: "app".into(),
        batch: batch.build(),
    };
    let bytes = rmp_serde::to_vec_named(&request).unwrap();
    let result = tokio::time::timeout(
        TIMEOUT,
        handler.handle(caller, &bytes, &ConnectionServices::without_push(0)),
    )
    .await
    .expect("cancel dispatch timed out")
    .unwrap();
    rmp_serde::from_slice(&result).unwrap()
}

/// remove_identity is tested directly because the cancel path obtains its expected Arc
/// atomically from peek; this directly probes the registry's substitution guard.
/// Removing Arc::ptr_eq in remove_identity makes this test fail because cancel removes the
/// parked transaction and its subsequent executable read returns tx_not_found.
#[tokio::test]
async fn remove_identity_refuses_a_substituted_transaction_arc() {
    let diag = FlakeGuard::with_watchdog(
        "remove_identity_refuses_a_substituted_transaction_arc",
        TIMEOUT,
    );
    tokio::time::timeout(TIMEOUT, async {
        let (db, handler, owner_actor) = fixture().await;
        let owner = session(1, 1, false);
        let id = parked(&handler, &db, &owner_actor, &owner).await;
        let parked = handler
            .tx_registry()
            .get_owned(id, &owner.session_id)
            .unwrap();
        let (other_ctx, other_snapshot, _) = db
            .tx_begin_as(owner_actor, "app", "main", "snapshot")
            .await
            .unwrap();
        let other = Arc::new(InteractiveTx::new(
            other_ctx,
            other_snapshot,
            owner.session_id,
            owner.user_id,
            "app".into(),
            "main".into(),
            Duration::from_secs(3_600),
            handler.tx_abort_release("app", "main"),
        ));
        assert!(!Arc::ptr_eq(&parked, &other));
        assert!(matches!(
            handler.tx_registry().remove_identity(id, &other),
            TxParkedState::Gone
        ));
        assert!(matches!(
            handler.tx_registry().peek(id),
            TxParkedState::Open(_)
        ));
        let response = handler
            .tx_execute(
                &owner,
                CURRENT_QUERY_LANG_VERSION,
                "app",
                id,
                Batch::new().build(),
            )
            .await;
        assert!(
            matches!(response, DbResponse::TxBatch { .. }),
            "parked transaction was changed: {response:?}"
        );
    })
    .await
    .expect("identity scenario timed out");
    diag.mark("substituted identity refused");
}

/// guard forces cancel's try_lock to fail and enter the busy re-read; a DIFFERENT
/// committing transaction under the same handle must answer already_finished, not already_committing.
#[tokio::test]
async fn busy_reread_of_different_committing_transaction_answers_finished() {
    let diag = FlakeGuard::with_watchdog(
        "busy_reread_of_different_committing_transaction_answers_finished",
        TIMEOUT,
    );
    tokio::time::timeout(TIMEOUT, async {
        let (db, base, owner_actor) = fixture().await;
        let owner = session(1, 1, false);
        let caller = session(2, 2, true);
        let id = parked(&base, &db, &owner_actor, &owner).await;
        let tx = base.tx_registry().get_owned(id, &owner.session_id).unwrap();
        let ctx_guard = tx.ctx().lock().await;
        let reached = Arc::new(tokio::sync::Barrier::new(2));
        let resume = Arc::new(tokio::sync::Barrier::new(2));
        let handler = base.clone().with_cancel_authorized_pause(
            CancelPauseTarget::Transaction,
            id,
            Arc::clone(&reached),
            Arc::clone(&resume),
        );
        let task_handler = handler.clone();
        let task = tokio::spawn(async move { wire_cancel(&task_handler, &caller, id).await });
        reached.wait().await;
        let committing = base.tx_registry().begin_commit(id, &tx).unwrap();
        drop(committing);
        let (replacement_ctx, replacement_snapshot, _) = db
            .tx_begin_as(owner_actor.clone(), "app", "main", "snapshot")
            .await
            .unwrap();
        let replacement = InteractiveTx::new(
            replacement_ctx,
            replacement_snapshot,
            owner.session_id,
            owner.user_id,
            "app".into(),
            "main".into(),
            Duration::from_secs(3_600),
            base.tx_abort_release("app", "main"),
        );
        base.tx_registry()
            .register_activity(
                id,
                replacement,
                owner.activity_handle(),
                Arc::from(owner.username.as_str()),
            )
            .expect("same handle is available after commit guard drop");
        let second = base.tx_registry().get_owned(id, &owner.session_id).unwrap();
        let second_committing = base.tx_registry().begin_commit(id, &second).unwrap();
        resume.wait().await;
        assert!(matches!(
            task.await.unwrap(),
            DbResponse::CancelOutcome {
                outcome: CancelOutcome::AlreadyFinished
            }
        ));
        assert!(matches!(
            base.tx_registry().peek(id),
            TxParkedState::Committing(_)
        ));
        drop(second_committing);
        drop(second);
        drop(ctx_guard);
    })
    .await
    .expect("busy committing identity scenario timed out");
    diag.mark("different committing identity answered already_finished");
}

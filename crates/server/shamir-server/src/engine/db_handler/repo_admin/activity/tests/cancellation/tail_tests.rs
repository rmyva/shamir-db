//! CANCEL-TAIL (#2005) focused regressions. Kept separate from the legacy
//! cancellation suite so the identity and audit boundary cases stay reviewable.
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use shamir_connect::common::types::{BindingMode, TransportKind};
use shamir_connect::server::audit_chain::{
    AuditAppender, AuditChain, AuditChainWriter, AuditEntry,
};
use shamir_connect::server::conn_services::ConnectionServices;
use shamir_connect::server::dispatch::RequestHandler;
use shamir_connect::server::session::{Session, SessionPermissions};
use shamir_db::access::{principal64, Actor};
use shamir_db::engine::repo::{BoxRepoFactory, RepoConfig};
use shamir_db::engine::table::TableConfig;
use shamir_db::storage::types::RecordKey;
use shamir_db::ShamirDb;
use shamir_query_builder::batch::Batch;
use shamir_query_builder::wire::admin::{cancel_request, cancel_tx};
use shamir_query_types::admin::activity::CancelOutcome;
use shamir_query_types::read::ReadQuery;
use shamir_query_types::wire::{CursorId, DbRequest, DbResponse};
use shamir_sync::durability::tx_cancel::TxParkedState;
use shamir_test_diag::FlakeGuard;
use shamir_tx::RepoTxGate;

use crate::cursor_registry::{Cursor, CursorActivity, CursorRegistry, PaginationMode};
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

#[derive(Default)]
struct Capture(Mutex<Vec<AuditEntry>>);
impl AuditAppender for Capture {
    fn append_entry(&self, entry: &AuditEntry) -> Result<(), std::io::Error> {
        self.0.lock().unwrap().push(entry.clone());
        Ok(())
    }
    fn checkpoint(&self, _: u64, _: &[u8; 32]) -> Result<(), std::io::Error> {
        Ok(())
    }
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
    let (ctx, snapshot, _repo) = db
        .tx_begin_as(owner.clone(), "app", "main", "snapshot")
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
                "app".into(),
                "main".into(),
                Duration::from_secs(3_600),
                handler.tx_abort_release("app", "main"),
            ),
            who.activity_handle(),
            Arc::from(who.username.as_str()),
        )
        .unwrap();
    id
}

async fn wire_request(
    handler: &ShamirDbHandler,
    caller: &Session,
    request: DbRequest,
) -> DbResponse {
    let bytes = rmp_serde::to_vec_named(&request).unwrap();
    let result = tokio::time::timeout(
        TIMEOUT,
        handler.handle(caller, &bytes, &ConnectionServices::without_push(0)),
    )
    .await
    .expect("request dispatch timed out")
    .unwrap();
    rmp_serde::from_slice(&result).unwrap()
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

/// Commit took the parked context to the commit facade handoff, not yet engine
/// driver ownership; cancel answers already_committing and commit then completes.
#[tokio::test]
async fn cancel_during_facade_handoff_answers_already_committing() {
    let diag = FlakeGuard::with_watchdog(
        "cancel_during_facade_handoff_answers_already_committing",
        TIMEOUT,
    );
    tokio::time::timeout(TIMEOUT, async {
        let (db, base, owner_actor) = fixture().await;
        let owner = session(1, 1, false);
        let id = parked(&base, &db, &owner_actor, &owner).await;
        let reached = Arc::new(tokio::sync::Barrier::new(2));
        let resume = Arc::new(tokio::sync::Barrier::new(2));
        let handler =
            base.clone()
                .with_commit_handoff_pause(id, Arc::clone(&reached), Arc::clone(&resume));
        let task_handler = handler.clone();
        let commit = tokio::spawn(async move { wire_commit(&task_handler, &owner, id).await });
        reached.wait().await;
        assert!(matches!(
            wire_cancel(&base, &session(2, 2, true), id).await,
            DbResponse::CancelOutcome {
                outcome: CancelOutcome::AlreadyCommitting
            }
        ));
        resume.wait().await;
        let DbResponse::TxCommitted { transaction } = commit.await.unwrap() else {
            panic!("commit did not complete");
        };
        assert!(transaction.is_committed());
        assert!(matches!(base.tx_registry().peek(id), TxParkedState::Gone));
    })
    .await
    .expect("commit/cancel handoff scenario timed out");
    diag.mark("commit took parked context and reached commit facade handoff (not engine driver ownership); cancel answered already_committing and commit completed");
}

async fn wire_commit(handler: &ShamirDbHandler, caller: &Session, id: u64) -> DbResponse {
    let bytes = rmp_serde::to_vec_named(&DbRequest::TxCommit {
        query_version: CURRENT_QUERY_LANG_VERSION,
        visibility_wait: None,
        db: "app".into(),
        tx_handle: id,
    })
    .unwrap();
    let result = tokio::time::timeout(
        TIMEOUT,
        handler.handle(caller, &bytes, &ConnectionServices::without_push(0)),
    )
    .await
    .expect("commit dispatch timed out")
    .unwrap();
    rmp_serde::from_slice(&result).unwrap()
}

/// Raw-native-path decoded-request accounting (token REST/WS decode separately and are not counted). The cursor case, busy CancelTx case, successful CancelRequest case, and permission denial are all native-path assertions. A denial is still a successfully decoded request and must not be hidden from this counter.
#[tokio::test]
async fn decoded_request_counter_covers_cancellation_dispatches() {
    let diag = FlakeGuard::with_watchdog(
        "decoded_request_counter_covers_cancellation_dispatches",
        TIMEOUT,
    );
    tokio::time::timeout(TIMEOUT, async {
        let (db, handler, owner_actor) = fixture().await;
        let owner = session(1, 1, false);
        let outsider = session(2, 2, false);

        let snapshot = RepoTxGate::new(0, 501).open_snapshot().await;
        let pinned_version = snapshot.version();
        let cursor = Cursor::new(
            ReadQuery::new("items"),
            PaginationMode::Offset,
            snapshot,
            pinned_version,
            10,
            owner.session_id,
            "app".into(),
            "main".into(),
        )
        .with_activity(crate::cursor_registry::CursorActivity {
            session_handle: owner.activity_handle(),
            user_id: owner.user_id,
            username: owner.username.clone(),
            db: "app".into(),
            table: "items".into(),
            pinned_version,
            created: std::time::Instant::now(),
        });
        handler
            .cursor_registry()
            .register(501, owner.session_id, cursor, 4)
            .unwrap();
        let outcomes_before_cursor = handler.activity_counts().cancellations;
        assert!(matches!(
            wire_cursor_cancel(&handler, &owner, 501).await,
            DbResponse::CancelOutcome {
                outcome: CancelOutcome::Cancelled
            }
        ));
        assert!(handler
            .cursor_registry()
            .get_owned(501, &owner.session_id)
            .is_err());
        let outcomes_after_cursor = handler.activity_counts().cancellations;
        assert_eq!(
            [
                outcomes_after_cursor[0] - outcomes_before_cursor[0],
                outcomes_after_cursor[1] - outcomes_before_cursor[1],
                outcomes_after_cursor[2] - outcomes_before_cursor[2],
                outcomes_after_cursor[3] - outcomes_before_cursor[3],
            ],
            [1, 0, 0, 0],
            "cursor cancel outcome delta"
        );
        assert_eq!(
            handler.decoded_request_count(),
            1,
            "successful cursor cancel counts once"
        );

        let tx_handle = parked(&handler, &db, &owner_actor, &owner).await;
        let tx = handler
            .tx_registry()
            .get_owned(tx_handle, &owner.session_id)
            .unwrap();
        let ctx_guard = tx.ctx().lock().await;
        let outcomes_before_busy = handler.activity_counts().cancellations;
        assert!(matches!(
            wire_cancel(&handler, &owner, tx_handle).await,
            DbResponse::CancelOutcome {
                outcome: CancelOutcome::NotCancellable
            }
        ));
        let outcomes_after_busy = handler.activity_counts().cancellations;
        assert_eq!(
            [
                outcomes_after_busy[0] - outcomes_before_busy[0],
                outcomes_after_busy[1] - outcomes_before_busy[1],
                outcomes_after_busy[2] - outcomes_before_busy[2],
                outcomes_after_busy[3] - outcomes_before_busy[3],
            ],
            [0, 0, 0, 1],
            "busy CancelTx outcome delta"
        );
        assert_eq!(
            handler.decoded_request_count(),
            2,
            "busy CancelTx refusal counts once"
        );
        drop(ctx_guard);
        drop(tx);

        let live = handler
            .try_track_activity_request(&owner, "app", "read", None)
            .unwrap();
        let mut batch = Batch::new();
        batch.op("cancel", cancel_request(live.id()));
        let outcomes_before_request = handler.activity_counts().cancellations;
        assert!(matches!(
            wire_request(
                &handler,
                &owner,
                DbRequest::Execute {
                    query_version: CURRENT_QUERY_LANG_VERSION,
                    db: "app".into(),
                    batch: batch.build(),
                },
            )
            .await,
            DbResponse::CancelOutcome {
                outcome: CancelOutcome::Cancelled
            }
        ));
        let outcomes_after_request = handler.activity_counts().cancellations;
        assert_eq!(
            [
                outcomes_after_request[0] - outcomes_before_request[0],
                outcomes_after_request[1] - outcomes_before_request[1],
                outcomes_after_request[2] - outcomes_before_request[2],
                outcomes_after_request[3] - outcomes_before_request[3],
            ],
            [1, 0, 0, 0],
            "successful CancelRequest outcome delta"
        );
        assert_eq!(
            handler.decoded_request_count(),
            3,
            "successful CancelRequest counts once"
        );
        drop(live);

        let cancellations_before_denial = handler.activity_counts().cancellations;
        let decoded_before_denial = handler.decoded_request_count();
        let protected = handler
            .try_track_activity_request(&owner, "app", "read", None)
            .unwrap();
        let mut batch = Batch::new();
        batch.op("cancel", cancel_request(protected.id()));
        let denied = wire_request(
            &handler,
            &outsider,
            DbRequest::Execute {
                query_version: CURRENT_QUERY_LANG_VERSION,
                db: "app".into(),
                batch: batch.build(),
            },
        )
        .await;
        assert!(matches!(denied, DbResponse::Error { code, .. } if code == "permission_denied"));
        assert_eq!(
            handler.decoded_request_count(),
            decoded_before_denial + 1,
            "denied request still crosses decode boundary"
        );
        assert_eq!(
            handler.activity_counts().cancellations,
            cancellations_before_denial,
            "permission denial is not a cancel outcome"
        );
    })
    .await
    .expect("decoded request cancellation scenario timed out");
    diag.mark("four cancellation requests counted at decode boundary");
}

/// Kills mutant `cursor_cancel_removes_replacement_after_stale_authorization`:
/// the decision was made for old user's mirror row, but a same-ID replacement
/// wins before removal. Identity-checked removal preserves it. The checked-in
/// implementation already has this behavior; this test is a regression guard.
#[tokio::test]
async fn cursor_id_reuse_keeps_replacement_after_stale_authorization() {
    tokio::time::timeout(TIMEOUT, async {
        let registry = CursorRegistry::new();
        let old_user = session(1, 1, false);
        let new_user = session(2, 2, false);
        let old_snapshot = RepoTxGate::new(0, 1).open_snapshot().await;
        let old_meta = CursorActivity {
            session_handle: old_user.activity_handle(),
            user_id: old_user.user_id,
            username: old_user.username.clone(),
            db: "app".into(),
            table: "items".into(),
            pinned_version: old_snapshot.version(),
            created: std::time::Instant::now(),
        };
        let old_cursor = Cursor::new(
            ReadQuery::new("items"),
            PaginationMode::Offset,
            old_snapshot,
            0,
            10,
            old_user.session_id,
            "app".into(),
            "main".into(),
        )
        .with_activity(old_meta);
        registry
            .register(71, old_user.session_id, old_cursor, 4)
            .unwrap();
        let observed = registry
            .activity_snapshot()
            .into_iter()
            .find(|row| row.cursor_id == 71)
            .unwrap();
        let stale_meta = Arc::clone(&observed.meta);
        assert_eq!(observed.meta.user_id, old_user.user_id);
        drop(
            registry
                .remove(71)
                .expect("old cursor removed between auth and removal"),
        );
        let new_snapshot = RepoTxGate::new(0, 2).open_snapshot().await;
        let new_cursor = Cursor::new(
            ReadQuery::new("items"),
            PaginationMode::Offset,
            new_snapshot,
            0,
            10,
            new_user.session_id,
            "app".into(),
            "main".into(),
        )
        .with_activity(CursorActivity {
            session_handle: new_user.activity_handle(),
            user_id: new_user.user_id,
            username: new_user.username.clone(),
            db: "app".into(),
            table: "items".into(),
            pinned_version: 0,
            created: std::time::Instant::now(),
        });
        registry
            .register(71, new_user.session_id, new_cursor, 4)
            .unwrap();
        let replacement_meta = registry
            .activity_snapshot()
            .into_iter()
            .find(|row| row.cursor_id == 71)
            .unwrap()
            .meta;
        assert!(
            !Arc::ptr_eq(&stale_meta, &replacement_meta),
            "same id has a new activity identity"
        );
        assert!(registry.remove_activity_identity(71, &stale_meta).is_none());
        assert!(
            registry.get_owned(71, &new_user.session_id).is_ok(),
            "replacement survives stale authorized removal"
        );
    })
    .await
    .expect("cursor reuse scenario timed out");
}

/// Kills mutant `audit_same_user_refusal`: a regression that suppresses the
/// refusal entry for the owner's own user. This behavior is already present
/// in the checked-in implementation; the test protects the audit contract.
#[tokio::test]
async fn same_user_cancel_refusal_is_audited() {
    tokio::time::timeout(TIMEOUT, async {
        let (db, base, owner_actor) = fixture().await;
        let capture = Arc::new(Capture::default());
        let writer = Arc::new(AuditChainWriter::new(
            AuditChain::new([6; 32]),
            capture.clone(),
        ));
        let handler = base.with_audit_writer(writer);
        let owner = session(1, 1, false);
        let id = parked(&handler, &db, &owner_actor, &owner).await;
        let tx = handler
            .tx_registry()
            .get_owned(id, &owner.session_id)
            .unwrap();
        let committing = handler.tx_registry().begin_commit(id, &tx).unwrap();
        let reply = wire_cancel(&handler, &owner, id).await;
        assert!(matches!(
            reply,
            DbResponse::CancelOutcome {
                outcome: CancelOutcome::AlreadyCommitting
            }
        ));
        assert!(
            handler.cancel_refusal_audit().drain().await,
            "audit queue failed to drain"
        );
        let entries = capture.0.lock().unwrap();
        assert_eq!(entries.len(), 1, "same-user refusal is audited");
        assert_eq!(entries[0].event, "cancel_tx");
        assert_eq!(entries[0].result, "already_committing");
        drop(entries);
        drop(committing);
    })
    .await
    .expect("same-user refusal scenario timed out");
}

/// Kills mutant `audit_successful_foreign_cancel_tx`: successful cross-user
/// transaction cancellation must produce an audit entry for the caller.
#[tokio::test]
async fn successful_foreign_cancel_tx_is_audited() {
    tokio::time::timeout(TIMEOUT, async {
        let (db, base, owner_actor) = fixture().await;
        let capture = Arc::new(Capture::default());
        let writer = Arc::new(AuditChainWriter::new(
            AuditChain::new([7; 32]),
            capture.clone(),
        ));
        let handler = base.with_audit_writer(writer);
        let owner = session(1, 1, false);
        let caller = session(2, 2, true);
        let id = parked(&handler, &db, &owner_actor, &owner).await;
        let reply = wire_cancel(&handler, &caller, id).await;
        assert!(matches!(
            reply,
            DbResponse::CancelOutcome {
                outcome: CancelOutcome::Cancelled
            }
        ));
        assert!(
            handler.cancel_refusal_audit().drain().await,
            "audit queue failed to drain"
        );
        let entries = capture.0.lock().unwrap();
        assert_eq!(entries.len(), 1, "successful foreign CancelTx is audited");
        assert_eq!(entries[0].event, "cancel_tx");
        assert_eq!(entries[0].result, "cancelled");
    })
    .await
    .expect("successful foreign CancelTx audit scenario timed out");
}

/// Kills mutant `committing_identity_recheck_omitted`: during the authorization
/// pause it installs a SECOND Committing reservation under the same handle.
/// Both rows are Committing but different Arcs, so removing only the ptr_eq
/// recheck answers AlreadyCommitting instead of AlreadyFinished.
#[tokio::test]
async fn foreign_cancel_tx_removed_committing_reservation_answers_finished() {
    let diag = FlakeGuard::with_watchdog(
        "foreign_cancel_tx_removed_committing_reservation_answers_finished",
        TIMEOUT,
    );
    tokio::time::timeout(TIMEOUT, async {
        let (db, base, owner_actor) = fixture().await;
        let owner = session(1, 1, false);
        let caller = session(2, 2, true);
        let id = parked(&base, &db, &owner_actor, &owner).await;
        let tx = base.tx_registry().get_owned(id, &owner.session_id).unwrap();
        let committing = base.tx_registry().begin_commit(id, &tx).unwrap();
        let first_activity_row = base
            .tx_registry()
            .activity_row(id)
            .expect("first activity row");
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
        drop(committing);
        drop(tx);
        let (replacement_ctx, replacement_snapshot, _repo) = db
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
        let second_activity_row = base
            .tx_registry()
            .activity_row(id)
            .expect("second activity row");
        assert!(!Arc::ptr_eq(&first_activity_row, &second_activity_row));
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
    })
    .await
    .expect("committing authorization race timed out");
    diag.mark("stale committing reservation answered already_finished");
}

/// Kills mutant `audit_already_finished`: cancellation pauses after cross-user
/// authorization, then the old cursor is replaced under the same id. Identity-
/// checked removal returns `AlreadyFinished`; that response must not be audited.
#[tokio::test]
async fn cursor_id_reuse_during_handler_authorization_preserves_replacement() {
    let diag = FlakeGuard::with_watchdog(
        "cursor_id_reuse_during_handler_authorization_preserves_replacement",
        TIMEOUT,
    );
    tokio::time::timeout(TIMEOUT, async {
        let (db, base, owner_actor) = fixture().await;
        let capture = Arc::new(Capture::default());
        let writer = Arc::new(AuditChainWriter::new(
            AuditChain::new([8; 32]),
            capture.clone(),
        ));
        let base = base.with_audit_writer(writer);
        let old_user = session(1, 1, false);
        let new_user = session(3, 3, false);
        let caller = session(2, 2, true);
        let audited_tx = parked(&base, &db, &owner_actor, &old_user).await;
        let id = 71;
        assert!(matches!(
            wire_cancel(&base, &caller, audited_tx).await,
            DbResponse::CancelOutcome {
                outcome: CancelOutcome::Cancelled
            }
        ));
        assert!(
            base.cancel_refusal_audit().drain().await,
            "audit queue failed to drain"
        );
        assert_eq!(capture.0.lock().unwrap().len(), 1, "positive audit control");
        let snapshot = RepoTxGate::new(0, 1).open_snapshot().await;
        let old_cursor = Cursor::new(
            ReadQuery::new("items"),
            PaginationMode::Offset,
            snapshot,
            0,
            10,
            old_user.session_id,
            "app".into(),
            "main".into(),
        )
        .with_activity(CursorActivity {
            session_handle: old_user.activity_handle(),
            user_id: old_user.user_id,
            username: old_user.username.clone(),
            db: "app".into(),
            table: "items".into(),
            pinned_version: 0,
            created: std::time::Instant::now(),
        });
        base.cursor_registry()
            .register(id, old_user.session_id, old_cursor, 4)
            .unwrap();
        let reached = Arc::new(tokio::sync::Barrier::new(2));
        let resume = Arc::new(tokio::sync::Barrier::new(2));
        let handler = base.clone().with_cancel_authorized_pause(
            CancelPauseTarget::Cursor,
            id,
            Arc::clone(&reached),
            Arc::clone(&resume),
        );
        let task_handler = handler.clone();
        let task =
            tokio::spawn(async move { wire_cursor_cancel(&task_handler, &caller, id).await });
        reached.wait().await;
        drop(
            base.cursor_registry()
                .remove(id)
                .expect("remove old cursor"),
        );
        let snapshot = RepoTxGate::new(0, 2).open_snapshot().await;
        let replacement = Cursor::new(
            ReadQuery::new("items"),
            PaginationMode::Offset,
            snapshot,
            0,
            10,
            new_user.session_id,
            "app".into(),
            "main".into(),
        )
        .with_activity(CursorActivity {
            session_handle: new_user.activity_handle(),
            user_id: new_user.user_id,
            username: new_user.username.clone(),
            db: "app".into(),
            table: "items".into(),
            pinned_version: 0,
            created: std::time::Instant::now(),
        });
        base.cursor_registry()
            .register(id, new_user.session_id, replacement, 4)
            .unwrap();
        resume.wait().await;
        assert!(matches!(
            task.await.unwrap(),
            DbResponse::CancelOutcome {
                outcome: CancelOutcome::AlreadyFinished
            }
        ));
        assert!(
            handler.cancel_refusal_audit().drain().await,
            "audit queue failed to drain"
        );
        assert_eq!(
            capture.0.lock().unwrap().len(),
            1,
            "AlreadyFinished adds no audit entry"
        );
        assert!(
            base.cursor_registry()
                .get_owned(id, &new_user.session_id)
                .is_ok(),
            "replacement survives stale handler cancel"
        );
    })
    .await
    .expect("cursor authorization race timed out");
    diag.mark("replacement cursor survived stale authorized cancel");
}

async fn wire_cursor_cancel(handler: &ShamirDbHandler, caller: &Session, id: u64) -> DbResponse {
    let mut batch = Batch::new();
    batch.op(
        "cancel_cursor",
        shamir_query_builder::cancel_cursor_admin(CursorId(id)),
    );
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
    .expect("cursor cancel dispatch timed out")
    .unwrap();
    rmp_serde::from_slice(&result).unwrap()
}

/// Kills mutant `busy_gone_reports_not_cancellable`: a cross-user cancel
/// captures an open tx, then its handle is reused during the post-authorization
/// pause. The stale cancel must not remove or cancel the replacement.
#[tokio::test]
async fn stale_cancel_does_not_remove_reused_handle() {
    let diag = FlakeGuard::with_watchdog("stale_cancel_does_not_remove_reused_handle", TIMEOUT);
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
        let removed = base.tx_registry().remove(id).expect("remove busy tx");
        let replacement_owner = session(3, 3, false);
        let (replacement_ctx, replacement_snapshot, _repo) = db
            .tx_begin_as(owner_actor.clone(), "app", "main", "snapshot")
            .await
            .unwrap();
        let replacement = InteractiveTx::new(
            replacement_ctx,
            replacement_snapshot,
            replacement_owner.session_id,
            replacement_owner.user_id,
            "app".into(),
            "main".into(),
            Duration::from_secs(3_600),
            base.tx_abort_release("app", "main"),
        );
        base.tx_registry()
            .register_activity(
                id,
                replacement,
                replacement_owner.activity_handle(),
                Arc::from(replacement_owner.username.as_str()),
            )
            .expect("register replacement under removed handle");
        resume.wait().await;
        let response = task.await;
        drop(removed);
        drop(ctx_guard);
        drop(tx);
        assert!(matches!(
            response.unwrap(),
            DbResponse::CancelOutcome {
                outcome: CancelOutcome::AlreadyFinished
            }
        ));
        assert!(
            base.tx_registry()
                .get_owned(id, &replacement_owner.session_id)
                .is_ok(),
            "replacement transaction survives stale authorized cancel"
        );
    })
    .await
    .expect("stale cancel handle-reuse scenario timed out");
    diag.mark("stale cancel preserved replacement transaction");
}

/// Kills mutant `busy_gone_reports_not_cancellable`: the authorized handle
/// disappears during the post-authorization pause and has no replacement.
#[tokio::test]
async fn stale_cancel_of_removed_handle_answers_already_finished() {
    let diag = FlakeGuard::with_watchdog(
        "stale_cancel_of_removed_handle_answers_already_finished",
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
        drop(base.tx_registry().remove(id).expect("remove authorized tx"));
        resume.wait().await;
        assert!(matches!(
            task.await.unwrap(),
            DbResponse::CancelOutcome {
                outcome: CancelOutcome::AlreadyFinished
            }
        ));
        drop(ctx_guard);
        drop(tx);
        assert!(matches!(base.tx_registry().peek(id), TxParkedState::Gone));
    })
    .await
    .expect("stale cancellation without replacement timed out");
    diag.mark("stale cancel without replacement answered already-finished");
}

/// Kills mutant `cancel_release_awaited_inline`: aborting the cancel dispatch
/// must not abort the independently spawned transaction release task.
#[tokio::test]
async fn cancel_release_survives_the_abort_of_the_cancel_dispatch() {
    let diag = FlakeGuard::with_watchdog(
        "cancel_release_survives_the_abort_of_the_cancel_dispatch",
        TIMEOUT,
    );
    tokio::time::timeout(TIMEOUT, async {
        let (db, handler, owner_actor) = fixture().await;
        let owner = session(1, 1, false);
        let (release_started_tx, release_started_rx) = tokio::sync::oneshot::channel();
        let (release_gate_tx, release_gate_rx) = tokio::sync::oneshot::channel();
        let (release_finished_tx, release_finished_rx) = tokio::sync::oneshot::channel();
        let release_started_tx = Arc::new(Mutex::new(Some(release_started_tx)));
        let release_gate_rx = Arc::new(Mutex::new(Some(release_gate_rx)));
        let release_finished_tx = Arc::new(Mutex::new(Some(release_finished_tx)));
        let release_count = Arc::new(AtomicUsize::new(0));
        let release_count_in_closure = Arc::clone(&release_count);
        let release: crate::tx_registry::TxAbortRelease = Arc::new(move |_| {
            let release_count = Arc::clone(&release_count_in_closure);
            let started = release_started_tx
                .lock()
                .unwrap()
                .take()
                .expect("release-started sender used once");
            let gate = release_gate_rx
                .lock()
                .unwrap()
                .take()
                .expect("release gate used once");
            let finished = release_finished_tx
                .lock()
                .unwrap()
                .take()
                .expect("release-finished sender used once");
            Box::pin(async move {
                let _ = started.send(());
                let _ = gate.await;
                release_count.fetch_add(1, Ordering::SeqCst);
                let _ = finished.send(());
            })
        });
        let (ctx, snapshot, _repo) = db
            .tx_begin_as(owner_actor, "app", "main", "snapshot")
            .await
            .unwrap();
        let id = ctx.tx_id.0;
        ctx.record_locked_key(0, RecordKey::from_slice(b"cancel-release"));
        handler
            .tx_registry()
            .register_activity(
                id,
                InteractiveTx::new(
                    ctx,
                    snapshot,
                    owner.session_id,
                    owner.user_id,
                    "app".into(),
                    "main".into(),
                    Duration::from_secs(3_600),
                    release,
                ),
                owner.activity_handle(),
                Arc::from(owner.username.as_str()),
            )
            .unwrap();
        assert!(handler
            .tx_registry()
            .get_owned(id, &owner.session_id)
            .is_ok());

        let task_handler = handler.clone();
        let task_owner = session(1, 1, false);
        let dispatch =
            tokio::spawn(async move { wire_cancel(&task_handler, &task_owner, id).await });
        tokio::time::timeout(TIMEOUT, release_started_rx)
            .await
            .expect("release did not start")
            .expect("release-started signal dropped");
        dispatch.abort();
        let error = dispatch.await.expect_err("cancel dispatch was not aborted");
        assert!(
            error.is_cancelled(),
            "cancel dispatch JoinError is not cancelled"
        );
        release_gate_tx
            .send(())
            .expect("release gate receiver dropped");
        tokio::time::timeout(TIMEOUT, release_finished_rx)
            .await
            .expect("release did not finish")
            .expect("release-finished signal dropped");
        assert_eq!(release_count.load(Ordering::SeqCst), 1);
    })
    .await
    .expect("cancel-dispatch abort scenario timed out");
    diag.mark("release task finished after cancel dispatch was aborted");
}

/// Kills mutant `cancel_tx_taken_context_returns_finished`: once cancellation
/// removes the handle and takes its context, TxExecute must return typed
/// `tx_not_found`, not a normal batch or generic error. Existing parent
/// dispatch returns the typed error through the real wire boundary.
#[tokio::test]
async fn tx_execute_with_taken_context_returns_typed_tx_not_found() {
    tokio::time::timeout(TIMEOUT, async {
        let (db, handler, owner_actor) = fixture().await;
        let owner = session(1, 1, true);
        let id = parked(&handler, &db, &owner_actor, &owner).await;
        let tx = handler.tx_registry().get_owned(id, &owner.session_id).unwrap();
        tx.ctx().lock().await.take().expect("take parked tx context");
        drop(tx);
        let request = DbRequest::TxExecute {
            query_version: CURRENT_QUERY_LANG_VERSION,
            db: "app".into(),
            tx_handle: id,
            batch: Batch::new().build(),
        };
        let bytes = rmp_serde::to_vec_named(&request).unwrap();
        let result = tokio::time::timeout(
            TIMEOUT,
            handler.handle(&owner, &bytes, &ConnectionServices::without_push(0)),
        ).await.expect("TxExecute dispatch timed out").unwrap();
        assert!(matches!(rmp_serde::from_slice::<DbResponse>(&result).unwrap(), DbResponse::Error { code, .. } if code == "tx_not_found"));
    }).await.expect("taken-context scenario timed out");
}

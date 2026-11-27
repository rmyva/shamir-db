use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;

use shamir_connect::common::types::{BindingMode, TransportKind};
use shamir_connect::server::audit_chain::{
    AuditAppender, AuditChain, AuditChainWriter, AuditEntry,
};
use shamir_connect::server::conn_services::ConnectionServices;
use shamir_connect::server::dispatch::RequestHandler;
use shamir_connect::server::session::{Session, SessionPermissions, SessionStore};
use shamir_db::access::{principal64, Actor};
use shamir_db::engine::repo::{BoxRepoFactory, RepoConfig};
use shamir_db::engine::table::TableConfig;
use shamir_db::{ShamirDb, SystemStoreConfig};
use shamir_query_builder::{
    batch::Batch,
    wire::admin::{cancel_cursor_admin, cancel_request, cancel_tx},
};
use shamir_query_types::admin::activity::{CancelOutcome, RequestExecId};
use shamir_query_types::batch::{BatchOp, IsolationLevel};
use shamir_query_types::read::ReadQuery;
use shamir_query_types::wire::{CursorId, DbRequest, DbResponse};
use shamir_test_diag::FlakeGuard;
use shamir_tx::{CancelHandle, CancelState};
use tokio::sync::oneshot;

use super::super::cancel::{cancel_entry, CancelCounters};
use super::super::requests::{ActivityRequests, RequestEntry};
use crate::audit_appender::FjallAuditAppender;
use crate::cursor_registry::{Cursor, CursorActivity, PaginationMode};
use crate::db_handler::handler::ShamirDbHandler;
use crate::db_handler::repo_audit::{enqueue_cancel_refusal_audit, CancelRefusalAudit};
use crate::tx_registry::InteractiveTx;
use crate::version::CURRENT_QUERY_LANG_VERSION;

const TIMEOUT: Duration = Duration::from_secs(120);

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

#[test]
fn state_word_commit_vs_cancel() {
    let diag = FlakeGuard::with_watchdog("state_word_commit_vs_cancel", TIMEOUT);
    let (start, jobs) = mpsc::channel::<Arc<CancelHandle>>();
    let (ready, at_start) = mpsc::channel();
    let (go, release) = mpsc::channel();
    let (send_result, result) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        while let Ok(handle) = jobs.recv_timeout(TIMEOUT) {
            ready.send(()).unwrap();
            release.recv_timeout(TIMEOUT).unwrap();
            send_result.send(handle.try_commit()).unwrap();
        }
    });
    for id in 0..4_096 {
        let entry = RequestEntry::new(
            RequestExecId(id),
            &session(1, 1, false),
            "app".to_owned(),
            "write",
            None,
        );
        start.send(Arc::clone(&entry.cancel)).unwrap();
        at_start.recv_timeout(TIMEOUT).unwrap();
        // Pin both orderings before stressing scheduler-selected races.
        let (outcome, committed) = match id {
            0 => {
                let outcome = cancel_entry(&entry);
                go.send(()).unwrap();
                (outcome, result.recv_timeout(TIMEOUT).unwrap())
            }
            1 => {
                go.send(()).unwrap();
                let committed = result.recv_timeout(TIMEOUT).unwrap();
                (cancel_entry(&entry), committed)
            }
            _ => {
                go.send(()).unwrap();
                let outcome = cancel_entry(&entry);
                (outcome, result.recv_timeout(TIMEOUT).unwrap())
            }
        };
        if id < 2 {
            assert_eq!(committed, id == 1);
        }
        if committed {
            assert_eq!(outcome, CancelOutcome::AlreadyCommitting);
            assert!(entry.cancel.finish());
            assert_eq!(entry.cancel.state(), CancelState::Done);
        } else {
            assert_eq!(outcome, CancelOutcome::Cancelled);
            assert!(!entry.cancel.finish());
            assert_eq!(entry.cancel.state(), CancelState::Cancelled);
        }
        assert_eq!(cancel_entry(&entry), CancelOutcome::AlreadyFinished);
        assert!(!entry.cancel.try_commit());
    }
    drop(start);
    worker.join().unwrap();
    diag.mark("all commit/cancel races checked");
}

#[test]
fn follower_apply_not_cancellable() {
    // Synthetic only: replication apply is not registered in production.
    let table = Arc::new(ActivityRequests::new());
    let mut entry = RequestEntry::new(
        RequestExecId(99),
        &session(1, 1, false),
        "app".to_owned(),
        "replication_apply",
        None,
    );
    entry.cancellable = false;
    let guard = table.register(entry);
    let entry = table.get(99).unwrap();
    assert_eq!(cancel_entry(&entry), CancelOutcome::NotCancellable);
    assert_eq!(entry.cancel.state(), CancelState::Running);
    assert!(guard.finish());
    assert_eq!(entry.cancel.state(), CancelState::Done);
    let still_registered = table.get(99).expect("finished entry is still registered");
    assert!(Arc::ptr_eq(&entry, &still_registered));
    assert_eq!(
        cancel_entry(&still_registered),
        CancelOutcome::AlreadyFinished
    );
}

#[derive(Default)]
struct CapturedAudit(Mutex<Vec<AuditEntry>>);

impl AuditAppender for CapturedAudit {
    fn append_entry(&self, entry: &AuditEntry) -> Result<(), std::io::Error> {
        self.0.lock().unwrap().push(entry.clone());
        Ok(())
    }

    fn checkpoint(&self, _: u64, _: &[u8; 32]) -> Result<(), std::io::Error> {
        Ok(())
    }
}

#[tokio::test]
async fn cancel_refusal_audit_has_exact_payload() {
    let captured = Arc::new(CapturedAudit::default());
    let writer = Arc::new(AuditChainWriter::new(
        AuditChain::new([7; 32]),
        captured.clone(),
    ));
    let caller = session(2, 3, true);
    let target = RequestExecId(u64::MAX - 1);
    let queue = Arc::new(CancelRefusalAudit::default());
    for outcome in [
        CancelOutcome::Cancelled,
        CancelOutcome::AlreadyFinished,
        CancelOutcome::AlreadyCommitting,
        CancelOutcome::NotCancellable,
    ] {
        let job = enqueue_cancel_refusal_audit(&queue, &writer, &caller, target, &outcome);
        if matches!(
            outcome,
            CancelOutcome::Cancelled | CancelOutcome::AlreadyFinished
        ) {
            assert!(job.is_none());
        } else {
            tokio::time::timeout(TIMEOUT, job.expect("refusal audit admitted"))
                .await
                .expect("refusal audit timed out")
                .unwrap();
        }
    }
    let entries = captured.0.lock().unwrap();
    assert_eq!(entries.len(), 2);
    for (entry, outcome) in entries
        .iter()
        .zip(["already_committing", "not_cancellable"])
    {
        assert_eq!(entry.event, "cancel_request");
        assert_eq!(entry.result, outcome);
        assert_eq!(entry.user, caller.username);
        assert_eq!(entry.session_id_prefix, [3; 8]);
        let actual: serde_json::Value =
            rmp_serde::from_slice(&entry.details_canonical_msgpack).unwrap();
        assert_eq!(actual.as_object().unwrap().len(), 2);
        assert_eq!(actual["target"].as_u64(), Some(u64::MAX - 1));
        assert_eq!(actual["outcome"].as_str(), Some(outcome));
    }
}

#[tokio::test]
async fn cancel_request_authorizes_by_user_before_refusing_or_cancelling() {
    tokio::time::timeout(
        TIMEOUT,
        Box::pin(async {
            let root = tempfile::tempdir().unwrap();
            let db = ShamirDb::init(SystemStoreConfig::Fjall(root.path().join("system")))
                .await
                .unwrap();
            let handler = ShamirDbHandler::new(Arc::new(db));
            let store = SessionStore::new();
            let owner = store.insert([1; 32], session(1, 1, false));
            let same_user = store.insert([2; 32], session(1, 2, false));
            let outsider = store.insert([3; 32], session(2, 3, false));
            let manager = store.insert([4; 32], session(3, 4, true));
            assert_ne!(owner.activity_handle(), same_user.activity_handle());
            let live = handler
                .try_track_activity_request(&owner, "app", "read", None)
                .unwrap();
            let protected = handler
                .try_track_activity_request_with_cancellability(
                    &owner, "app", "tx_open", None, false,
                )
                .unwrap();
            for id in [live.id(), protected.id()] {
                assert!(matches!(
                    Box::pin(handler.cancel_request(&outsider, id)).await,
                    DbResponse::Error { code, .. } if code == "permission_denied"
                ));
            }
            assert_eq!(live.cancel_handle().state(), CancelState::Running);
            assert!(matches!(
                Box::pin(handler.cancel_request(&manager, protected.id())).await,
                DbResponse::CancelOutcome {
                    outcome: CancelOutcome::NotCancellable
                }
            ));
            assert!(matches!(
                Box::pin(handler.cancel_request(&same_user, live.id())).await,
                DbResponse::CancelOutcome {
                    outcome: CancelOutcome::Cancelled
                }
            ));
            let id = live.id();
            let handle = Arc::clone(live.cancel_handle());
            assert!(!live.finish());
            drop(live);
            assert_eq!(handle.state(), CancelState::Cancelled);
            assert!(matches!(
                Box::pin(handler.cancel_request(&same_user, id)).await,
                DbResponse::CancelOutcome {
                    outcome: CancelOutcome::AlreadyFinished
                }
            ));
        }),
    )
    .await
    .expect("authorization test timed out");
}

struct GatedDurableAudit {
    durable: Arc<FjallAuditAppender>,
    first: Mutex<Option<(oneshot::Sender<()>, mpsc::Receiver<()>)>>,
    refusal_persisted: Mutex<Option<oneshot::Sender<()>>>,
}

impl AuditAppender for GatedDurableAudit {
    fn append_entry(&self, entry: &AuditEntry) -> Result<(), std::io::Error> {
        let first = self.first.lock().unwrap().take();
        if let Some((entered, release)) = first {
            // AuditChainWriter::append owns its durable gate during this callback.
            entered.send(()).unwrap();
            release
                .recv_timeout(TIMEOUT)
                .map_err(std::io::Error::other)?;
        }
        self.durable.append_entry(entry)?;
        if entry.event != "cancel_request" {
            return Ok(());
        }
        if let Some(persisted) = self.refusal_persisted.lock().unwrap().take() {
            persisted.send(()).unwrap();
        }
        Ok(())
    }

    fn checkpoint(&self, seq: u64, hmac: &[u8; 32]) -> Result<(), std::io::Error> {
        self.durable.checkpoint(seq, hmac)
    }
}

#[tokio::test]
async fn cancel_refusal_replies_while_durable_audit_gate_is_held() {
    let root = tempfile::tempdir().unwrap();
    let diag = FlakeGuard::with_watchdog(
        "cancel_refusal_replies_while_durable_audit_gate_is_held",
        TIMEOUT,
    );
    let db = tokio::time::timeout(
        TIMEOUT,
        Box::pin(ShamirDb::init(SystemStoreConfig::Fjall(
            root.path().join("system"),
        ))),
    )
    .await
    .expect("Fjall init timed out")
    .unwrap();
    let audit_dir = root.path().join("audit");
    let (entered, at_gate) = oneshot::channel();
    let (release_gate, release) = mpsc::sync_channel(1);
    let (persisted, on_disk) = oneshot::channel();
    let appender = Arc::new(GatedDurableAudit {
        durable: FjallAuditAppender::open_strict(&audit_dir).unwrap(),
        first: Mutex::new(Some((entered, release))),
        refusal_persisted: Mutex::new(Some(persisted)),
    });
    let key = [7; 32];
    let writer = Arc::new(AuditChainWriter::new(AuditChain::new(key), appender));
    let handler = ShamirDbHandler::new(Arc::new(db)).with_audit_writer(Arc::clone(&writer));
    let store = SessionStore::new();
    let owner = store.insert([1; 32], session(1, 1, false));
    let control = store.insert([2; 32], session(1, 2, false));
    let live = handler
        .try_track_activity_request(&owner, "app", "write", None)
        .unwrap();
    assert!(live.cancel_handle().try_commit());
    let first = tokio::task::spawn_blocking(move || {
        writer.append("test_first", "tcp", "", "", [0; 8], "ok", Vec::new(), 1)
    });
    tokio::time::timeout(TIMEOUT, at_gate)
        .await
        .expect("first append did not reach the durable gate")
        .unwrap();
    let mut batch = Batch::new();
    batch.op("cancel_request", cancel_request(live.id()));
    let request = DbRequest::Execute {
        query_version: CURRENT_QUERY_LANG_VERSION,
        db: "app".into(),
        batch: batch.build(),
    };
    let conn = ConnectionServices::without_push(0);
    let bytes = tokio::time::timeout(
        TIMEOUT,
        Box::pin(handler.handle_decoded(&control, request, &conn)),
    )
    .await
    .expect("cancel reply waited for the audit writer")
    .unwrap();
    let reply: DbResponse = rmp_serde::from_slice(&bytes).unwrap();
    assert!(matches!(
        reply,
        DbResponse::CancelOutcome {
            outcome: CancelOutcome::AlreadyCommitting
        }
    ));
    // A synchronous regression can stall this runtime's timer, but then the
    // appender's independent deadline drops its receiver and this send fails.
    release_gate
        .try_send(())
        .expect("first append stopped waiting before the reply");
    diag.mark("typed refusal received before releasing durable gate");
    tokio::time::timeout(TIMEOUT, first)
        .await
        .expect("first append timed out")
        .unwrap()
        .unwrap();
    tokio::time::timeout(TIMEOUT, on_disk)
        .await
        .expect("refusal was not persisted")
        .unwrap();
    let entries = FjallAuditAppender::read_log_for_verify(&audit_dir).unwrap();
    AuditChain::verify_chain(&key, &entries).unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].event, "test_first");
    let refusal = &entries[1];
    assert_eq!(refusal.event, "cancel_request");
    assert_eq!(refusal.result, "already_committing");
    assert_eq!(refusal.session_id_prefix, [2; 8]);
    let details: serde_json::Value =
        rmp_serde::from_slice(&refusal.details_canonical_msgpack).unwrap();
    assert_eq!(details.as_object().unwrap().len(), 2);
    assert_eq!(details["target"].as_u64(), Some(live.id().0));
    assert_eq!(details["outcome"].as_str(), Some("already_committing"));
    assert!(live.finish());
}

/// Pins the FIXED index order of `CancelCounters` that the export's
/// `CANCEL_RESULT_LABELS` shares: 0 cancelled, 1 already_finished,
/// 2 already_committing, 3 not_cancellable. A swapped or merged slot
/// fails the per-step vector. Catches: a `record` that increments the
/// wrong slot, a `record` that increments more than once, and a
/// `snapshot` that swaps indexes.
#[test]
fn cancel_counters_count_each_outcome_in_one_slot() {
    let diag = FlakeGuard::with_watchdog("cancel_counters_count_each_outcome_in_one_slot", TIMEOUT);
    let counters = CancelCounters::new();
    assert_eq!(counters.snapshot(), [0, 0, 0, 0]);
    counters.record(CancelOutcome::Cancelled);
    assert_eq!(counters.snapshot(), [1, 0, 0, 0]);
    counters.record(CancelOutcome::AlreadyFinished);
    assert_eq!(counters.snapshot(), [1, 1, 0, 0]);
    counters.record(CancelOutcome::AlreadyCommitting);
    assert_eq!(counters.snapshot(), [1, 1, 1, 0]);
    counters.record(CancelOutcome::NotCancellable);
    counters.record(CancelOutcome::NotCancellable);
    assert_eq!(counters.snapshot(), [1, 1, 1, 2]);
    diag.mark("each outcome counted in exactly one fixed slot");
}

/// Drive one request through the REAL wire path and return the reply.
async fn wire_request(
    handler: &ShamirDbHandler,
    session: &Session,
    request: DbRequest,
) -> DbResponse {
    let bytes = rmp_serde::to_vec_named(&request).expect("encode request");
    let raw = handler
        .handle(session, &bytes, &ConnectionServices::without_push(0))
        .await
        .expect("handle request");
    rmp_serde::from_slice(&raw).expect("decode reply")
}

/// Execute one built batch through the REAL wire path.
async fn wire_execute(
    handler: &ShamirDbHandler,
    session: &Session,
    batch: shamir_query_builder::BatchRequest,
) -> DbResponse {
    wire_request(
        handler,
        session,
        DbRequest::Execute {
            query_version: CURRENT_QUERY_LANG_VERSION,
            db: "app".into(),
            batch,
        },
    )
    .await
}

/// Drive one cancel operation through the REAL wire path
/// (`RequestHandler::handle`) and return its typed outcome.
async fn wire_cancel(handler: &ShamirDbHandler, session: &Session, op: BatchOp) -> CancelOutcome {
    let mut batch = Batch::new();
    batch.op("cancel", op);
    match wire_execute(handler, session, batch.build()).await {
        DbResponse::CancelOutcome { outcome } => outcome,
        other => panic!("expected CancelOutcome, got {other:?}"),
    }
}

/// In-memory engine with `app` / `main` / `items` created, owned by `owner`.
async fn open_app_db(owner: &Actor) -> Arc<ShamirDb> {
    let db = ShamirDb::init_memory().await.expect("in-memory database");
    db.create_db_as("app", owner.clone())
        .await
        .expect("create db");
    db.add_repo_as(
        "app",
        RepoConfig::new("main", BoxRepoFactory::in_memory()).add_table(TableConfig::new("items")),
        owner.clone(),
    )
    .await
    .expect("add repo");
    Arc::new(db)
}

/// Park one REAL interactive transaction owned by `session` in `handler`.
async fn park_tx(
    handler: &ShamirDbHandler,
    db: &ShamirDb,
    owner_actor: &Actor,
    session: &Session,
) -> u64 {
    let (tx, snapshot, repo) = db
        .tx_begin_as(owner_actor.clone(), "app", "main", "snapshot")
        .await
        .expect("begin transaction");
    let handle = tx.tx_id.0;
    handler
        .tx_registry()
        .register(
            handle,
            InteractiveTx::new(
                tx,
                snapshot,
                session.session_id,
                session.user_id,
                "app".into(),
                "main".into(),
                Duration::from_secs(3_600),
                handler.tx_abort_release("app", "main"),
            )
            .with_bound_repo(repo),
        )
        .expect("park transaction");
    handle
}

/// `TxBegin` pessimistically, so a staged write takes a Level-3 record lock.
fn pessimistic_begin() -> DbRequest {
    DbRequest::TxBegin {
        query_version: CURRENT_QUERY_LANG_VERSION,
        db: "app".into(),
        repo: "main".into(),
        isolation: Some(IsolationLevel::Pessimistic),
    }
}

/// Stage one update of the seeded `n = 1` row inside an open transaction.
fn tx_update(tx_handle: u64, n: i64) -> DbRequest {
    let mut batch = Batch::new();
    batch.id("blocked");
    batch.update(
        "blocked",
        shamir_query_builder::write::Update::with_repo("main", "items")
            .where_(shamir_query_builder::filter::eq("n", 1i64))
            .set(shamir_query_builder::doc! { "n" => n })
            .build()
            .expect("build tx update"),
    );
    DbRequest::TxExecute {
        query_version: CURRENT_QUERY_LANG_VERSION,
        db: "app".into(),
        tx_handle,
        batch: batch.build(),
    }
}

/// Every cancel operation increments EXACTLY ONE slot, exactly once, per
/// outcome — each label produced by a REAL dispatch path, never by calling
/// `CancelCounters::record`. The full four-element vector is asserted after
/// every step: 0 `cancelled` (own-user `cancel_tx`), 1 `already_finished`
/// (unknown ids), 2 `already_committing` (a real `begin_commit`
/// reservation), 3 `not_cancellable` (a real in-flight `TxExecute`,
/// registered without a cancel handle). Catches: a `record` in the wrong
/// slot, a cancel path answering without `cancel_reply`, and an operator
/// cancel that also bumps the `CancelRequest` slot.
#[tokio::test]
async fn every_cancel_operation_counts_its_outcome_exactly_once() {
    tokio::time::timeout(
        TIMEOUT,
        Box::pin(async {
            let owner = Actor::User(principal64([7; 16]));
            let db = open_app_db(&owner).await;
            let handler = ShamirDbHandler::new(Arc::clone(&db));
            let caller = session(7, 8, true);
            assert_eq!(handler.activity_counts().cancellations, [0, 0, 0, 0]);

            let outcome = wire_cancel(&handler, &caller, cancel_request(RequestExecId(404))).await;
            assert_eq!(outcome, CancelOutcome::AlreadyFinished);
            assert_eq!(handler.activity_counts().cancellations, [0, 1, 0, 0]);

            let outcome = wire_cancel(&handler, &caller, cancel_tx(404)).await;
            assert_eq!(outcome, CancelOutcome::AlreadyFinished);
            assert_eq!(handler.activity_counts().cancellations, [0, 2, 0, 0]);

            let outcome = wire_cancel(&handler, &caller, cancel_cursor_admin(CursorId(404))).await;
            assert_eq!(outcome, CancelOutcome::AlreadyFinished);
            assert_eq!(handler.activity_counts().cancellations, [0, 3, 0, 0]);

            // One REAL successful cancel: a transaction the caller owns.
            let tx_handle = park_tx(&handler, &db, &owner, &caller).await;
            let before = handler.activity_counts().cancellations;
            assert_eq!(before, [0, 3, 0, 0]);
            let outcome = wire_cancel(&handler, &caller, cancel_tx(tx_handle)).await;
            assert_eq!(outcome, CancelOutcome::Cancelled);
            let after = handler.activity_counts().cancellations;
            assert_eq!(after, [1, 3, 0, 0]);
            assert_eq!(after[0] - before[0], 1, "cancelled moved by exactly one");
            assert_eq!(
                [after[1], after[2], after[3]],
                [before[1], before[2], before[3]],
                "no other slot moved"
            );

            // `already_committing`: the production `TxRegistry::begin_commit`
            // path parks a real `Committing` reservation, which `cancel_tx`
            // can only REFUSE. The guard owns the teardown.
            let parked_handle = park_tx(&handler, &db, &owner, &caller).await;
            let parked = handler
                .tx_registry()
                .get_owned(parked_handle, &caller.session_id)
                .expect("parked transaction");
            let committing = handler
                .tx_registry()
                .begin_commit(parked_handle, &parked)
                .expect("reserve committing state");
            let before = handler.activity_counts().cancellations;
            assert_eq!(before, [1, 3, 0, 0]);
            let outcome = wire_cancel(&handler, &caller, cancel_tx(parked_handle)).await;
            assert_eq!(outcome, CancelOutcome::AlreadyCommitting);
            let after = handler.activity_counts().cancellations;
            assert_eq!(after, [1, 3, 1, 0]);
            assert_eq!(after[2] - before[2], 1, "already_committing moved by one");
            assert_eq!(
                [after[0], after[1], after[3]],
                [before[0], before[1], before[3]],
                "no other slot moved"
            );
            drop(committing);
            drop(parked);

            // `not_cancellable`: `TxExecute` registers WITHOUT a cancel
            // handle, so `CancelRequest` on its `RequestExecId` must refuse.
            // Only a REAL parked `TxExecute` (write waiting on an older
            // transaction's record lock) keeps such a row live; the wait is
            // awaited through the activity table, never slept on.
            let mut seed = Batch::new();
            seed.id("seed");
            seed.insert(
                "i",
                shamir_query_builder::write::insert("items")
                    .rows([shamir_query_builder::doc! { "n" => 1i64 }]),
            );
            let seeded = wire_execute(&handler, &caller, seed.build()).await;
            assert!(matches!(&seeded, DbResponse::Batch { .. }), "seed: {seeded:?}");
            let holder = Arc::new(session(7, 9, true));
            let waiter = Arc::new(session(7, 10, true));
            let holder_tx = match wire_request(&handler, &holder, pessimistic_begin()).await {
                DbResponse::TxOpened { tx_handle, .. } => tx_handle,
                other => panic!("holder TxBegin: {other:?}"),
            };
            let staged = wire_request(&handler, &holder, tx_update(holder_tx, 2)).await;
            assert!(
                matches!(&staged, DbResponse::TxBatch { .. }),
                "the holder must stage the locked update: {staged:?}"
            );
            let waiter_tx = match wire_request(&handler, &waiter, pessimistic_begin()).await {
                DbResponse::TxOpened { tx_handle, .. } => tx_handle,
                other => panic!("waiter TxBegin: {other:?}"),
            };
            let blocked = {
                let handler = handler.clone();
                let waiter = Arc::clone(&waiter);
                tokio::spawn(
                    async move { wire_request(&handler, &waiter, tx_update(waiter_tx, 3)).await },
                )
            };
            let id = tokio::time::timeout(
                TIMEOUT,
                async {
                    let mut pace = tokio::time::interval(Duration::from_millis(1));
                    loop {
                        pace.tick().await;
                        let mut found = None;
                        handler.activity_requests.for_each(|entry| {
                            if !entry.cancellable {
                                found = Some(entry.id);
                            }
                        });
                        if let Some(id) = found {
                            break id;
                        }
                    }
                },
            )
            .await
            .expect("the parked TxExecute must register a non-cancellable row");
            let parked_entry = handler
                .activity_requests
                .get(id.0)
                .expect("parked entry still registered");
            assert_eq!(parked_entry.op_kind, "write", "TxExecute batch row");
            assert!(!parked_entry.cancellable);
            drop(parked_entry);
            let before = handler.activity_counts().cancellations;
            assert_eq!(before, [1, 3, 1, 0]);
            let outcome = wire_cancel(&handler, &caller, cancel_request(id)).await;
            assert_eq!(outcome, CancelOutcome::NotCancellable);
            let after = handler.activity_counts().cancellations;
            assert_eq!(after, [1, 3, 1, 1]);
            assert_eq!(after[3] - before[3], 1, "not_cancellable moved by one");
            assert_eq!(
                [after[0], after[1], after[2]],
                [before[0], before[1], before[2]],
                "no other slot moved"
            );

            // Committing the holder releases its Level-3 record lock.
            let committed = wire_request(
                &handler,
                &holder,
                DbRequest::TxCommit {
                    query_version: CURRENT_QUERY_LANG_VERSION,
                    visibility_wait: None,
                    db: "app".into(),
                    tx_handle: holder_tx,
                },
            )
            .await;
            assert!(
                matches!(&committed, DbResponse::TxCommitted { .. }),
                "holder commit: {committed:?}"
            );
            let finished = tokio::time::timeout(TIMEOUT, blocked)
                .await
                .expect("the parked TxExecute must finish once the holder commits")
                .expect("blocked dispatch joined");
            assert!(
                matches!(&finished, DbResponse::TxBatch { .. }),
                "a `not_cancellable` refusal must not cancel the request: {finished:?}"
            );
            assert_eq!(outcome, CancelOutcome::NotCancellable, "outcome after join");
            assert_eq!(
                handler.activity_counts().cancellations,
                [1, 3, 1, 1],
                "final delta after the blocked TxExecute joined"
            );
            let rolled_back = wire_request(
                &handler,
                &waiter,
                DbRequest::TxRollback {
                    db: "app".into(),
                    tx_handle: waiter_tx,
                },
            )
            .await;
            assert!(
                matches!(&rolled_back, DbResponse::TxRolledBack { tx_handle } if *tx_handle == waiter_tx),
                "waiter rollback: {rolled_back:?}"
            );
            // The live handler snapshot is exactly what `/metrics` reads.
            assert_eq!(
                handler.activity_counts().cancellations,
                [1, 3, 1, 1],
                "one real dispatch per label, each in its own slot"
            );
        }),
    )
    .await
    .expect("cancel-counter test timed out");
}

/// One operator audit entry: `event`, caller identity, two-key payload.
fn assert_cancel_audit_entry(
    entry: &AuditEntry,
    caller: &Session,
    event: &str,
    target: u64,
    label: &str,
) {
    assert_eq!(entry.event, event);
    assert_eq!(entry.result, label);
    assert_eq!(entry.user, caller.username);
    assert_eq!(&entry.session_id_prefix[..], &caller.session_id[..8]);
    let actual: serde_json::Value =
        rmp_serde::from_slice(&entry.details_canonical_msgpack).unwrap();
    let object = actual.as_object().unwrap();
    let mut keys: Vec<&str> = object.keys().map(String::as_str).collect();
    keys.sort_unstable();
    assert_eq!(keys, vec!["outcome", "target"], "exactly two detail keys");
    assert_eq!(actual["target"].as_u64(), Some(target));
    assert_eq!(actual["outcome"].as_str(), Some(label));
    let encoded = &entry.details_canonical_msgpack;
    let username = caller.username.as_bytes();
    assert!(
        !encoded.windows(username.len()).any(|w| w == username),
        "username leaked into the audit payload"
    );
    assert!(
        !encoded
            .windows(caller.session_id.len())
            .any(|w| w == &caller.session_id[..]),
        "session id leaked into the audit payload"
    );
    let text = String::from_utf8_lossy(encoded).to_lowercase();
    for secret in ["bearer", "authorization", "token", "password", "secret"] {
        assert!(
            !text.contains(secret),
            "bearer-like material {secret:?} leaked into the audit payload"
        );
    }
}

/// Register one real cursor attributed to `user` in the activity mirror.
async fn register_cursor(handler: &ShamirDbHandler, cursor_id: u64, user: &Session) {
    let snapshot = shamir_tx::RepoTxGate::new(0, cursor_id)
        .open_snapshot()
        .await;
    let pinned_version = snapshot.version();
    let cursor = Cursor::new(
        ReadQuery::new("items"),
        PaginationMode::Offset,
        snapshot,
        pinned_version,
        10,
        user.session_id,
        "app".to_string(),
        "main".to_string(),
    )
    .with_activity(CursorActivity {
        session_handle: user.activity_handle(),
        user_id: user.user_id,
        username: user.username.clone(),
        db: "app".to_string(),
        table: "items".to_string(),
        pinned_version,
        created: std::time::Instant::now(),
    });
    handler
        .cursor_registry()
        .register(cursor_id, user.session_id, cursor, 16)
        .expect("register cursor");
}

/// Every operator cancel audit entry is produced by the REAL dispatch path
/// (`RequestHandler::handle` → `cancel_tx` / `cancel_cursor_admin`), never
/// by calling the enqueue helper directly. Each carries exactly the
/// operation name as `event`, the caller identity, and a two-key payload —
/// never a username, session id or bearer-like material. Cases, each
/// against a REAL target: a cross-user `cancel_tx` (audited `cancelled`),
/// the owner's OWN `cancel_tx` (not audited), an `already_committing`
/// refusal (audited), and both cursor outcomes (cross-user audited,
/// own-user not). Catches: delete the `enqueue_operator_cancel_audit` call
/// from `cancel_tx` (or `cancel_cursor_admin`) ⇒ the cross-user `cancelled`
/// and `already_committing` cases lose their entry and the test fails. Also
/// catches a leaked username/session id, a missing or extra detail key, a
/// drifting outcome label, and an audit of the caller's own work.
#[tokio::test]
async fn operator_cancel_audit_writes_operation_target_and_outcome_only() {
    let diag = FlakeGuard::with_watchdog(
        "operator_cancel_audit_writes_operation_target_and_outcome_only",
        TIMEOUT,
    );
    tokio::time::timeout(
        TIMEOUT,
        Box::pin(async {
            let owner_actor = Actor::User(principal64([1; 16]));
            let db = open_app_db(&owner_actor).await;
            let captured = Arc::new(CapturedAudit::default());
            let writer = Arc::new(AuditChainWriter::new(
                AuditChain::new([9; 32]),
                captured.clone(),
            ));
            let store = SessionStore::new();
            let owner = store.insert([1; 32], session(1, 1, false));
            let same_user = store.insert([2; 32], session(1, 2, false));
            let mut operator_session = session(2, 3, true);
            // A display name of its own keeps "the CALLER was audited, not
            // the target's name" a real assertion.
            operator_session.username = "operator-distinct".to_owned();
            let caller = store.insert([3; 32], operator_session);
            let handler = ShamirDbHandler::new(Arc::clone(&db))
                .with_session_store(Arc::new(store))
                .with_audit_writer(writer);

            // 1. A real user-1 transaction cancelled by ANOTHER user: audited.
            let tx_handle = park_tx(&handler, &db, &owner_actor, &owner).await;
            assert_eq!(
                wire_cancel(&handler, &caller, cancel_tx(tx_handle)).await,
                CancelOutcome::Cancelled
            );
            assert!(
                handler.cancel_refusal_audit().drain().await,
                "audit queue did not drain"
            );
            {
                let entries = captured.0.lock().unwrap();
                assert_eq!(
                    entries.len(),
                    1,
                    "a successful cross-user cancel is audited exactly once"
                );
                assert_cancel_audit_entry(
                    &entries[0],
                    &caller,
                    "cancel_tx",
                    tx_handle,
                    "cancelled",
                );
            }

            // 2. The SAME shape, cancelled by the owner's own user: NOT audited.
            let tx_handle = park_tx(&handler, &db, &owner_actor, &owner).await;
            assert_eq!(
                wire_cancel(&handler, &same_user, cancel_tx(tx_handle)).await,
                CancelOutcome::Cancelled
            );
            assert!(
                handler.cancel_refusal_audit().drain().await,
                "audit queue did not drain"
            );
            assert_eq!(
                captured.0.lock().unwrap().len(),
                1,
                "the owner's own cancel is not audited"
            );

            // 3. A `begin_commit` reservation: audited `already_committing`.
            let tx_handle = park_tx(&handler, &db, &owner_actor, &owner).await;
            let parked = handler
                .tx_registry()
                .get_owned(tx_handle, &owner.session_id)
                .expect("parked transaction");
            let committing = handler
                .tx_registry()
                .begin_commit(tx_handle, &parked)
                .expect("reserve committing state");
            assert_eq!(
                wire_cancel(&handler, &caller, cancel_tx(tx_handle)).await,
                CancelOutcome::AlreadyCommitting
            );
            assert!(
                handler.cancel_refusal_audit().drain().await,
                "audit queue did not drain"
            );
            {
                let entries = captured.0.lock().unwrap();
                assert_eq!(entries.len(), 2, "the committing refusal is audited");
                assert_cancel_audit_entry(
                    &entries[1],
                    &caller,
                    "cancel_tx",
                    tx_handle,
                    "already_committing",
                );
            }
            drop(committing);
            drop(parked);

            // 4. A real cursor owned by ANOTHER user: audited `cancelled`.
            let foreign_cursor_id = 91u64;
            register_cursor(&handler, foreign_cursor_id, &owner).await;
            let foreign_cursor = CursorId(foreign_cursor_id);
            assert_eq!(
                wire_cancel(&handler, &caller, cancel_cursor_admin(foreign_cursor)).await,
                CancelOutcome::Cancelled
            );
            assert!(
                handler.cancel_refusal_audit().drain().await,
                "audit queue did not drain"
            );
            {
                let entries = captured.0.lock().unwrap();
                assert_eq!(entries.len(), 3, "a cross-user cursor cancel is audited");
                assert_cancel_audit_entry(
                    &entries[2],
                    &caller,
                    "cancel_cursor_admin",
                    foreign_cursor_id,
                    "cancelled",
                );
            }

            // 5. The SAME shape, cancelled by the cursor owner's own user: no entry.
            let own_cursor_id = 92u64;
            register_cursor(&handler, own_cursor_id, &same_user).await;
            assert_eq!(
                wire_cancel(
                    &handler,
                    &same_user,
                    cancel_cursor_admin(CursorId(own_cursor_id))
                )
                .await,
                CancelOutcome::Cancelled
            );
            assert!(
                handler.cancel_refusal_audit().drain().await,
                "audit queue did not drain"
            );
            assert_eq!(
                captured.0.lock().unwrap().len(),
                3,
                "the caller's own cursor cancel is not audited"
            );
        }),
    )
    .await
    .expect("operator cancel audit test timed out");
    diag.mark("cross-user and refusal audit entries verified through dispatch");
}

//! Disconnect cancellation of owned mutation workers.
//!
//! Mutants these tests kill:
//! - `DisconnectCancel::disarm` removed, or the `Drop` firing regardless of
//!   the armed flag: an enrolled mutation refused during authorization (or a
//!   batch whose conditional mutations all skip) returns while the shared
//!   [`shamir_tx::CancelHandle`] is still `Running`; the armed drop wins the
//!   `Running → Cancelled` race against the waiter's `finish()` and the
//!   client receives the `cancelled` wire code instead of the real refusal
//!   (`enrolled_mutation_refused_during_authorization_answers_the_real_refusal`);
//! - the armed-drop cancellation removed (or `disarm` clearing eagerly at
//!   construction): a waiter dropped MID-EXECUTION would leave the detached
//!   worker uncancelled (`armed_guard_drop_cancels_the_running_handle`,
//!   `dropping_the_waiter_mid_execution_cancels_the_parked_worker`);
//! - `disarm` not reaching the shared handle (per-clone handle): the same
//!   refusal test fails because the waiter still loses the race.

use std::sync::Arc;
use std::time::Duration;

use shamir_connect::common::types::{BindingMode, TransportKind};
use shamir_connect::server::conn_services::ConnectionServices;
use shamir_connect::server::dispatch::RequestHandler;
use shamir_connect::server::session::{Session, SessionPermissions};
use shamir_db::access::{principal64, Actor};
use shamir_db::engine::db_instance::logical_export::MaintenanceGate;
use shamir_db::engine::repo::{BoxRepoFactory, RepoConfig};
use shamir_db::engine::table::TableConfig;
use shamir_db::ShamirDb;
use shamir_query_builder::batch::Batch;
use shamir_query_builder::write::{doc, Insert};
use shamir_query_types::wire::{DbRequest, DbResponse};
use shamir_tx::{CancelHandle, CancelState};
use tokio::sync::oneshot;

use crate::db_handler::DisconnectCancel;
use crate::db_handler::ShamirDbHandler;

/// Bounded wait floor: every wait in this file uses at least this bound and
/// completes through a HANDSHAKE (oneshot / `wait_cancelled`), never a sleep.
const BOUND: Duration = Duration::from_secs(120);

/// An ARMED guard dropped on a `Running` handle must cancel it — this is the
/// disconnect path (waiter dropped mid-execution), not the normal path.
#[test]
fn armed_guard_drop_cancels_the_running_handle() {
    let cancel = Arc::new(CancelHandle::default());
    let guard = DisconnectCancel::new(Arc::clone(&cancel));
    assert_eq!(cancel.state(), CancelState::Running);
    drop(guard);
    assert_eq!(cancel.state(), CancelState::Cancelled);
}

/// A DISARMED guard (normal worker completion: the awaiting future resumed
/// from the join) drops without touching the handle — a batch refused during
/// authorization leaves the handle `Running`, and the drop must not win the
/// race against the waiter's `finish()`.
#[test]
fn disarmed_guard_drop_leaves_the_handle_running() {
    let cancel = Arc::new(CancelHandle::default());
    let guard = DisconnectCancel::new(Arc::clone(&cancel));
    guard.disarm();
    drop(guard);
    assert_eq!(cancel.state(), CancelState::Running);
}

/// Positive control for the wiring: the waiter future (the connection's
/// request task) is dropped MID-EXECUTION while its armed guard lives — the
/// shared handle must turn `Cancelled` through a handshake, not a sleep.
/// Runtime flavour: tokio multi-thread (two workers), so the aborted waiter
/// and the observing `wait_cancelled` progress on independent threads.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_the_waiter_mid_execution_cancels_the_parked_worker() {
    let cancel = Arc::new(CancelHandle::default());
    let (guard_up_tx, guard_up_rx) = oneshot::channel::<()>();
    let waiter = tokio::spawn({
        let cancel = Arc::clone(&cancel);
        async move {
            let guard = DisconnectCancel::new(cancel);
            // Handshake: the armed guard now lives inside this waiter future.
            let _ = guard_up_tx.send(());
            // Park "mid-execution"; only the abort below ends this future.
            std::future::pending::<()>().await;
            drop(guard); // unreachable; keeps the guard alive to the drop
        }
    });
    tokio::time::timeout(BOUND, guard_up_rx)
        .await
        .expect("waiter guard handshake bounded")
        .expect("waiter guard handshake");
    // Drop the waiter MID-EXECUTION (this is the disconnect).
    waiter.abort();
    // Completion handshake: the armed drop cancelled the shared handle.
    tokio::time::timeout(BOUND, cancel.wait_cancelled())
        .await
        .expect("the dropped waiter must cancel the shared handle (bounded)");
}

/// THE defect: an enrolled mutation (write leaf ⇒ maintenance-enrolled ⇒
/// owned worker) refused during authorization used to answer the `cancelled`
/// wire code, because the guard dropped after the join fired
/// `Running → Cancelled` before the waiter's `finish()`. With the disarm at
/// the completion boundary the REAL refusal reaches the client.
/// Runtime flavour: current-thread — the whole path is one task awaiting the
/// owned worker; no concurrent waiter is needed.
#[tokio::test]
async fn enrolled_mutation_refused_during_authorization_answers_the_real_refusal() {
    let db = ShamirDb::init_memory().await.expect("memory db");
    // The database is owned by ANOTHER actor: the session's actor is refused
    // during `authorize_batch`, before planning or execution.
    let owner = Actor::User(principal64([0x11; 16]));
    db.create_db_as("app", owner.clone())
        .await
        .expect("create the foreign-owned database");
    db.add_repo_as(
        "app",
        RepoConfig::new("main", BoxRepoFactory::in_memory()).add_table(TableConfig::new("items")),
        owner,
    )
    .await
    .expect("create the foreign-owned repository");
    let session = Arc::new(Session::new(
        [0x22; 16],
        "intruder".into(),
        SessionPermissions::new(false, false, vec![]),
        TransportKind::Tcp,
        BindingMode::TlsExporter,
        [0; 32],
        0,
    ));
    // Enrolled: the shared maintenance gate routes the write batch through
    // the owned worker with the disconnect guard armed around the join.
    let handler = Arc::new(
        ShamirDbHandler::new(Arc::new(db)).with_maintenance_gate(MaintenanceGate::default()),
    );
    let mut batch = Batch::new();
    batch.insert(
        "written",
        Insert::with_repo("main", "items").row(doc().set("n", 1).unwrap()),
    );
    let request = DbRequest::Execute {
        query_version: 4,
        db: "app".into(),
        batch: batch.build(),
    };
    let bytes = rmp_serde::to_vec_named(&request).unwrap();
    let wire = tokio::time::timeout(BOUND, async {
        let conn = ConnectionServices::without_push(0);
        handler.handle(&session, &bytes, &conn).await.unwrap()
    })
    .await
    .expect("the refused batch must return while the worker finishes (bounded)");
    let response: DbResponse = rmp_serde::from_slice(&wire).unwrap();
    let DbResponse::Error { code, message } = response else {
        panic!("an unauthorized write must be refused, got: {response:?}");
    };
    // Mutant: an armed drop after the join answers the cancelled code.
    assert_ne!(
        code,
        shamir_query_types::wire::error_code::CANCELLED,
        "the real authorization refusal must survive the guard drop: {code} {message}"
    );
    // The typed authorization refusal: `Authorized::authorize` maps every
    // denial to `query_coded("", "access_denied", <AccessError display>)`
    // (authorized.rs); the wire message is the `BatchError` display
    // "Query '' failed [access_denied]: " followed by the `AccessError`
    // display "access denied: <actor> cannot <action> on <path>".
    assert_eq!(
        code, "access_denied",
        "the unauthorized write must answer the typed authorization refusal: {code} {message}"
    );
    assert!(
        message.starts_with("Query '' failed [access_denied]: access denied:"),
        "the refusal message must carry the AccessError display: {code} {message}"
    );
}

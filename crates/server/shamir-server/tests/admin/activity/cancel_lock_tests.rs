//! End-to-end lock wakes, rollback, and permissions for CancelRequest,
//! plus the R4b step 3 registry cancels (CancelTx / CancelCursorAdmin).

use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use shamir_connect::server::session::Session;
use shamir_db::access::{principal64, Actor};
use shamir_db::storage::types::RecordKey;
use shamir_query_builder::batch::{Batch, Isolation};
use shamir_query_builder::{doc, filter, val, write, Query};
use shamir_query_types::admin::activity::{
    ActivityRequestState, ActivityRow, ActivityTxState, CancelOutcome,
};
use shamir_query_types::batch::IsolationLevel;
use shamir_server::db_handler::{DbRequest, DbResponse};
use shamir_server::durability::tx_cancel::TxParkedState;
use shamir_server::tx_registry::InteractiveTx;
use shamir_server::version::CURRENT_QUERY_LANG_VERSION;
use shamir_test_diag::FlakeGuard;
use shamir_tx::{LockMode, MvccStore, RepoTxGate};
use shamir_types::types::record_id::RecordId;
use tokio::sync::Notify;

use super::cancel_fixture::{acceptance, Fixture};
use super::common::{
    create_cursor_req, execute, insert_req, tx_begin_req, tx_commit_req, tx_insert_req,
    tx_rollback_req, DB, REPO, TABLE,
};

/// Pessimistic BEGIN, so the interactive transaction's staged write keeps
/// its Level-3 record lock until commit or rollback.
fn tx_begin_pessimistic_req() -> DbRequest {
    DbRequest::TxBegin {
        query_version: CURRENT_QUERY_LANG_VERSION,
        db: DB.into(),
        repo: REPO.into(),
        isolation: Some(IsolationLevel::Pessimistic),
    }
}

/// Stage one pessimistically-locked update of the row whose `n` is `n`.
fn tx_update_req(tx_handle: u64, n: i64) -> DbRequest {
    let mut batch = Batch::new();
    batch.id("txupd");
    batch.update("staged", update(n));
    DbRequest::TxExecute {
        query_version: CURRENT_QUERY_LANG_VERSION,
        db: DB.into(),
        tx_handle,
        batch: batch.build(),
    }
}

/// Read every `n` from a fresh snapshot in `session`.
async fn read_values(fixture: &Fixture, session: &Session) -> Vec<i64> {
    let mut read = Batch::new();
    read.query("all", Query::with_repo(REPO, TABLE));
    let response = fixture.send(session, &execute(read.build())).await;
    let DbResponse::Batch { response } = response else {
        panic!("read rows: {response:?}");
    };
    let mut values: Vec<_> = response.results["all"]
        .records
        .iter()
        .map(|record| record.get_value_i64("n").expect("seed value"))
        .collect();
    values.sort_unstable();
    values
}

async fn storage(fixture: &Fixture) -> (Arc<RepoTxGate>, Arc<MvccStore>) {
    let db = fixture.handler.db().get_db(DB).expect("fixture db");
    let table = db.get_table(REPO, TABLE).await.expect("open table");
    let repo = db.get_repo(REPO).expect("fixture repo");
    let gate = repo.tx_gate().await.expect("gate");
    (gate, table.mvcc_store_for_test().expect("MVCC table"))
}

async fn seed(fixture: &Fixture, n: i64) -> RecordKey {
    let response = fixture.send(&fixture.admin, &insert_req(n)).await;
    let DbResponse::Batch { response } = response else {
        panic!("seed existing record: {response:?}");
    };
    let record = &response.results["i"].records[0];
    let id: RecordId = record
        .get_value_str("_id")
        .expect("inserted id")
        .parse()
        .expect("RecordId");
    RecordKey::from_slice(id.as_bytes())
}

async fn hold(mvcc: &MvccStore, gate: &RepoTxGate, keys: &[RecordKey]) -> u64 {
    let older = gate.fresh_tx_id().raw();
    for key in keys {
        mvcc.lock_key(
            key.clone(),
            older,
            Arc::new(AtomicBool::new(false)),
            Arc::new(Notify::new()),
            LockMode::Exclusive,
        )
        .await
        .expect("older transaction owns the seeded key");
    }
    older
}

fn update(n: i64) -> shamir_query_types::write::UpdateOp {
    write::Update::with_repo(REPO, TABLE)
        .where_(filter::eq("n", n))
        .set(doc! { "n" => -n })
        .build()
        .expect("build fixture update")
}

fn blocked_update(n: i64, iterate: bool) -> DbRequest {
    let mut batch = Batch::new();
    batch.transactional().isolation(Isolation::Pessimistic);
    if iterate {
        let mut body = Batch::new();
        // Stage a write on a different, unblocked existing key before
        // this iteration attempts to update the held key.
        let staged = body.update("staged", update(100 + n));
        body.update_after("blocked", update(n), &[&staged]);
        batch.for_each("loop", vec![val::lit(1)], "row", body.build());
    } else {
        batch.update("blocked", update(n));
    }
    execute(batch.build())
}

fn assert_cancelled(response: DbResponse) {
    assert!(
        matches!(&response, DbResponse::Error { code, .. } if code == "cancelled"),
        "expected top-level cancelled wire error, got {response:?}"
    );
}

#[test]
fn cancel_wakes_parked_lock_wait() {
    acceptance(
        "cancel_wakes_parked_lock_wait",
        |guard, fixture| async move {
            let (gate, mvcc) = storage(&fixture).await;
            let keys = [seed(&fixture, 1).await];
            assert!(gate.active_snapshots_empty());
            let before = gate.version_allocation_high_water_mark();
            let older = hold(&mvcc, &gate, &keys).await;
            let target = fixture.user("writer", 1);
            let request = blocked_update(1, false);
            let mut pending = fixture.start(Arc::clone(&target), request);
            let id = fixture
                .waiting(
                    &mut pending,
                    &target,
                    ActivityRequestState::WaitingLock,
                    || true,
                )
                .await;
            guard.mark("parked under older record lock");
            assert!(
                !gate.active_snapshots_empty(),
                "parked transaction pins a snapshot"
            );
            fixture
                .expect_cancel(&fixture.admin, id, CancelOutcome::Cancelled)
                .await;
            // No blocker release, wound, or dropped execution future can wake this request.
            assert_cancelled(pending.await.expect("target connection joined"));
            assert_eq!(mvcc.locks_len(), 1, "only the older holder remains");
            assert!(
                gate.active_snapshots_empty(),
                "cancelled transaction unpinned its snapshot"
            );
            assert_eq!(
                gate.version_allocation_high_water_mark(),
                before,
                "cancelled work must not enter commit"
            );
            fixture.assert_retired(&target, id).await;
            mvcc.release_locks(older, &keys).await;
            assert_eq!(mvcc.locks_len(), 0);
        },
    );
}

#[test]
fn cancel_releases_all_under_concurrency() {
    acceptance(
        "cancel_releases_all_under_concurrency",
        |guard, fixture| async move {
            let (gate, mvcc) = storage(&fixture).await;
            let keys = [seed(&fixture, 1).await, seed(&fixture, 2).await];
            seed(&fixture, 101).await;
            seed(&fixture, 102).await;
            assert!(gate.active_snapshots_empty());
            assert!(fixture.handler.tx_registry().is_empty());
            let before = gate.version_allocation_high_water_mark();
            let older = hold(&mvcc, &gate, &keys).await;
            let first = fixture.user("first", 1);
            let second = fixture.user("second", 2);
            let request_a = blocked_update(1, true);
            let request_b = blocked_update(2, true);
            let mut pending_a = fixture.start(Arc::clone(&first), request_a);
            let mut pending_b = fixture.start(Arc::clone(&second), request_b);
            // The target tasks run independently. The observer reuses its one
            // admission slot for sequential listings of both parked requests.
            let id_a = fixture
                .waiting(
                    &mut pending_a,
                    &first,
                    ActivityRequestState::WaitingLock,
                    || true,
                )
                .await;
            let id_b = fixture
                .waiting(
                    &mut pending_b,
                    &second,
                    ActivityRequestState::WaitingLock,
                    || true,
                )
                .await;
            guard.mark("two iterations parked after staging writes");
            assert_ne!(id_a, id_b);
            assert_eq!(
                mvcc.locks_len(),
                4,
                "two staged row locks plus the two blockers"
            );
            assert!(!gate.active_snapshots_empty());
            assert!(first.try_acquire_request_slot(1).is_none());
            assert!(second.try_acquire_request_slot(1).is_none());

            fixture
                .expect_cancel(&fixture.admin, id_a, CancelOutcome::Cancelled)
                .await;
            assert_cancelled(pending_a.await.expect("first connection joined"));
            assert_eq!(
                mvcc.locks_len(),
                3,
                "the unaffected request still owns its staged row lock"
            );
            assert!(
                !gate.active_snapshots_empty(),
                "the other request still pins its snapshot"
            );
            fixture
                .expect_cancel(&fixture.admin, id_b, CancelOutcome::Cancelled)
                .await;
            assert_cancelled(pending_b.await.expect("second connection joined"));
            assert_eq!(mvcc.locks_len(), 2, "only fixture blockers remain");
            assert!(gate.active_snapshots_empty());
            assert_eq!(
                gate.version_allocation_high_water_mark(),
                before,
                "neither cancelled request may enter commit"
            );
            fixture.assert_retired(&first, id_a).await;
            fixture.assert_retired(&second, id_b).await;
            assert!(fixture.handler.tx_registry().is_empty());
            mvcc.release_locks(older, &keys).await;
            assert_eq!(mvcc.locks_len(), 0);

            let mut read = Batch::new();
            read.query("all", Query::with_repo(REPO, TABLE));
            let response = fixture.send(&first, &execute(read.build())).await;
            let DbResponse::Batch { response } = response else {
                panic!("read after rollback: {response:?}");
            };
            let mut values: Vec<_> = response.results["all"]
                .records
                .iter()
                .map(|record| record.get_value_i64("n").expect("seed value"))
                .collect();
            values.sort_unstable();
            assert_eq!(
                values,
                [1, 2, 101, 102],
                "neither iteration may publish its staged update"
            );
            let opened = fixture.send(&second, &tx_begin_req()).await;
            let DbResponse::TxOpened { tx_handle, .. } = opened else {
                panic!("cancelled session cannot reuse its transaction slot: {opened:?}");
            };
            let rollback = DbRequest::TxRollback {
                db: DB.into(),
                tx_handle,
            };
            assert!(
                matches!(fixture.send(&second, &rollback).await, DbResponse::TxRolledBack { tx_handle: got } if got == tx_handle)
            );
            assert!(fixture.handler.tx_registry().is_empty());
            assert!(gate.active_snapshots_empty());
        },
    );
}

#[test]
fn cancelled_code_on_error_path() {
    acceptance(
        "cancelled_code_on_error_path",
        |guard, fixture| async move {
            let target = fixture.user("owner", 1);
            let caller = fixture.user("owner", 2);
            cancel_as_owner(&guard, &fixture, &target, &caller, None).await;
        },
    );
}

#[test]
fn permission_denial_activity_cancel_half() {
    acceptance(
        "permission_denial_activity_cancel_half",
        |guard, fixture| async move {
            let target = fixture.user("alice", 1);
            let caller = fixture.user("alice", 2);
            let foreign = fixture.user("bob", 3);
            cancel_as_owner(&guard, &fixture, &target, &caller, Some(&foreign)).await;
        },
    );
}

async fn cancel_as_owner(
    guard: &FlakeGuard,
    fixture: &Fixture,
    target: &Arc<Session>,
    caller: &Session,
    foreign: Option<&Session>,
) {
    let (gate, mvcc) = storage(fixture).await;
    let keys = [seed(fixture, 1).await];
    let older = hold(&mvcc, &gate, &keys).await;
    let request = blocked_update(1, false);
    let mut pending = fixture.start(Arc::clone(target), request);
    let id = fixture
        .waiting(
            &mut pending,
            target,
            ActivityRequestState::WaitingLock,
            || true,
        )
        .await;
    guard.mark("parked before permission check");
    if let Some(foreign) = foreign {
        let response = fixture.cancel(foreign, id).await;
        assert!(
            matches!(&response, DbResponse::Error { code, .. } if code == "permission_denied"),
            "foreign cancel without Manage: {response:?}"
        );
    }
    // A successful CAS here also proves a denied foreign request did not cancel the target.
    fixture
        .expect_cancel(caller, id, CancelOutcome::Cancelled)
        .await;
    assert_cancelled(pending.await.expect("target connection joined"));
    fixture.assert_retired(target, id).await;
    mvcc.release_locks(older, &keys).await;
}

#[test]
fn cancel_interactive_tx_rolls_back() {
    acceptance(
        "cancel_interactive_tx_rolls_back",
        |guard, fixture| async move {
            let (gate, _mvcc) = storage(&fixture).await;
            seed(&fixture, 1).await;
            assert!(
                gate.active_snapshots_empty(),
                "baseline before any interactive transaction"
            );
            assert!(fixture.handler.tx_registry().is_empty());

            let owner = fixture.user("tx_owner", 1);
            let handle = match fixture.send(&owner, &tx_begin_req()).await {
                DbResponse::TxOpened { tx_handle, .. } => tx_handle,
                other => panic!("interactive tx begin: {other:?}"),
            };
            assert!(
                matches!(
                    fixture.send(&owner, &tx_insert_req(handle, 5)).await,
                    DbResponse::TxBatch { .. }
                ),
                "staged insert must accumulate inside the open transaction"
            );
            guard.mark("interactive tx staged an insert");

            assert!(
                fixture.transactions().await.iter().any(|row| matches!(
                    row,
                    ActivityRow::Transaction { tx_handle, state, .. }
                        if *tx_handle == handle && *state == ActivityTxState::Active
                )),
                "open transaction must be listed as active"
            );
            assert_eq!(fixture.handler.tx_registry().len(), 1);
            assert!(!gate.active_snapshots_empty());

            fixture
                .expect_tx_cancel(&fixture.admin, handle, CancelOutcome::Cancelled)
                .await;
            guard.mark("interactive tx cancelled");

            assert_eq!(fixture.handler.tx_registry().len(), 0);
            assert!(
                gate.active_snapshots_empty(),
                "cancelled transaction must release its pinned snapshot"
            );
            assert!(
                !fixture.transactions().await.iter().any(|row| matches!(
                    row,
                    ActivityRow::Transaction { tx_handle, .. } if *tx_handle == handle
                )),
                "cancelled transaction must leave the activity registry"
            );

            let reader = fixture.user("fresh_reader", 2);
            assert_eq!(
                read_values(&fixture, &reader).await,
                [1],
                "a fresh snapshot must not see the cancelled transaction's staged insert"
            );

            for (label, request) in [
                ("tx execute", tx_insert_req(handle, 9)),
                ("tx commit", tx_commit_req(handle, None)),
                ("tx rollback", tx_rollback_req(handle)),
            ] {
                let response = fixture.send(&owner, &request).await;
                assert!(
                    matches!(&response, DbResponse::Error { code, .. } if code == "tx_not_found"),
                    "{label} on cancelled handle {handle}: {response:?}"
                );
            }
            guard.mark("cancelled handle refused on every lifecycle path");

            let opened = fixture.send(&owner, &tx_begin_req()).await;
            let DbResponse::TxOpened {
                tx_handle: fresh, ..
            } = opened
            else {
                panic!("cancelled session must free its transaction slot: {opened:?}");
            };
            assert!(matches!(
                fixture.send(&owner, &tx_rollback_req(fresh)).await,
                DbResponse::TxRolledBack { tx_handle: got } if got == fresh
            ));
            assert!(fixture.handler.tx_registry().is_empty());

            fixture
                .expect_tx_cancel(&fixture.admin, handle, CancelOutcome::AlreadyFinished)
                .await;
            guard.mark("old handle answers already-finished");
        },
    );
}

#[test]
fn cancel_tx_holding_record_locks_unblocks_waiter() {
    acceptance(
        "cancel_tx_holding_record_locks_unblocks_waiter",
        |guard, fixture| async move {
            let (gate, mvcc) = storage(&fixture).await;
            seed(&fixture, 1).await;
            let baseline = mvcc.locks_len();
            assert_eq!(baseline, 0, "nothing else holds a record lock");

            let tx_owner = fixture.user("lock_owner", 1);
            let handle = match fixture.send(&tx_owner, &tx_begin_pessimistic_req()).await {
                DbResponse::TxOpened { tx_handle, .. } => tx_handle,
                other => panic!("pessimistic tx begin: {other:?}"),
            };
            assert!(
                matches!(
                    fixture.send(&tx_owner, &tx_update_req(handle, 1)).await,
                    DbResponse::TxBatch { .. }
                ),
                "staged update must accumulate inside the open transaction"
            );
            assert!(
                mvcc.locks_len() > baseline,
                "the staged pessimistic write must hold the row's record lock"
            );
            guard.mark("interactive tx holds the staged row lock");

            let waiter = fixture.user("lock_waiter", 2);
            let mut pending = fixture.start(Arc::clone(&waiter), blocked_update(1, false));
            let _id = fixture
                .waiting(
                    &mut pending,
                    &waiter,
                    ActivityRequestState::WaitingLock,
                    || true,
                )
                .await;
            guard.mark("standalone update parked on the interactive tx's record lock");
            assert!(
                !pending.is_finished(),
                "the parked update must still be waiting for the lock"
            );

            fixture
                .expect_tx_cancel(&fixture.admin, handle, CancelOutcome::Cancelled)
                .await;
            guard.mark("interactive tx cancelled while holding the record lock");

            let response = pending.await.expect("blocked update connection joined");
            let DbResponse::Batch { response } = response else {
                panic!("cancelling the holder must let the waiter complete normally: {response:?}");
            };
            assert!(
                response.results.contains_key("blocked"),
                "the unblocked update must return its results: {response:?}"
            );
            assert_eq!(
                mvcc.locks_len(),
                baseline,
                "the cancelled transaction must release every record lock"
            );
            assert_eq!(fixture.handler.tx_registry().len(), 0);
            assert!(
                gate.active_snapshots_empty(),
                "the cancelled transaction must release its snapshot"
            );
        },
    );
}

#[test]
fn cancel_cursor_admin_releases_pinned_snapshot() {
    acceptance(
        "cancel_cursor_admin_releases_pinned_snapshot",
        |guard, fixture| async move {
            let (gate, _mvcc) = storage(&fixture).await;
            seed(&fixture, 1).await;
            seed(&fixture, 2).await;
            assert!(gate.active_snapshots_empty());
            assert_eq!(fixture.handler.cursor_registry().len(), 0);

            let owner = fixture.user("cursor_owner", 1);
            let cursor_id = match fixture.send(&owner, &create_cursor_req()).await {
                DbResponse::CursorPage {
                    cursor_id,
                    has_more: true,
                    ..
                } => cursor_id,
                other => panic!("seeded rows must leave a follow-up page: {other:?}"),
            };
            guard.mark("result cursor open");

            assert_eq!(fixture.handler.cursor_registry().len(), 1);
            assert!(
                !gate.active_snapshots_empty(),
                "an open cursor pins its snapshot"
            );
            assert!(
                fixture.cursors().await.iter().any(|row| matches!(
                    row,
                    ActivityRow::Cursor { cursor_id: id, .. } if *id == cursor_id.0
                )),
                "open cursor must be listed by its id"
            );

            fixture
                .expect_cursor_cancel(&fixture.admin, cursor_id, CancelOutcome::Cancelled)
                .await;
            guard.mark("result cursor cancelled");

            assert_eq!(fixture.handler.cursor_registry().len(), 0);
            assert!(
                gate.active_snapshots_empty(),
                "cancelled cursor must release its pinned snapshot"
            );
            assert!(
                !fixture.cursors().await.iter().any(|row| matches!(
                    row,
                    ActivityRow::Cursor { cursor_id: id, .. } if *id == cursor_id.0
                )),
                "cancelled cursor must leave the registry listing"
            );

            let fetch = DbRequest::FetchNext {
                cursor_id,
                page_size: None,
            };
            let response = fixture.send(&owner, &fetch).await;
            assert!(
                matches!(&response, DbResponse::Error { code, .. } if code == "cursor_not_found"),
                "owner's next fetch on cancelled cursor {cursor_id}: {response:?}"
            );

            fixture
                .expect_cursor_cancel(&fixture.admin, cursor_id, CancelOutcome::AlreadyFinished)
                .await;
            guard.mark("second cursor cancel answers already-finished");
        },
    );
}

#[test]
fn cancel_cursor_admin_own_session_cursor_is_allowed() {
    acceptance(
        "cancel_cursor_admin_own_session_cursor_is_allowed",
        |guard, fixture| async move {
            let (gate, _mvcc) = storage(&fixture).await;
            seed(&fixture, 1).await;
            seed(&fixture, 2).await;

            let carol1 = fixture.user("carol", 1);
            let carol2 = fixture.user("carol", 2);
            assert_eq!(
                carol1.user_id, carol2.user_id,
                "same username must derive the same owning user id"
            );
            assert_ne!(carol1.session_id, carol2.session_id);

            let cursor_id = match fixture.send(&carol1, &create_cursor_req()).await {
                DbResponse::CursorPage {
                    cursor_id,
                    has_more: true,
                    ..
                } => cursor_id,
                other => panic!("seeded rows must leave a follow-up page: {other:?}"),
            };
            guard.mark("carol-session-1 opened the cursor");

            // Same owning user, different session: the own-user path needs no
            // ActivityView Manage grant.
            fixture
                .expect_cursor_cancel(&carol2, cursor_id, CancelOutcome::Cancelled)
                .await;
            guard.mark("carol-session-2 cancelled carol-session-1's cursor");

            assert_eq!(fixture.handler.cursor_registry().len(), 0);
            assert!(gate.active_snapshots_empty());
        },
    );
}

/// Whether the activity view lists `tx_handle` in `state` — one `ListActivity`
/// read, used to prove a refused cancel changed nothing.
async fn listed_as(fixture: &Fixture, tx_handle: u64, state: ActivityTxState) -> bool {
    fixture.transactions().await.iter().any(|row| {
        matches!(
            row,
            ActivityRow::Transaction { tx_handle: listed, state: listed_state, .. }
                if *listed == tx_handle && *listed_state == state
        )
    })
}

/// H1 case (b): a transaction with a request in flight is refused and stays
/// intact.
///
/// No sleep synchronizes this: a `TxExecute` holds the transaction's context
/// lock for its whole plan, so probing that lock with the same NON-BLOCKING
/// `try_lock` the cancel makes proves the request is inside the plan while it
/// parks on the held row. The request's activity row itself stays `Running` —
/// a `TxExecute` is registered non-cancellable and its lock wait parks on the
/// context's cancel handle, which only the non-transactional execute path
/// attaches.
#[test]
fn cancel_tx_refused_while_request_in_flight() {
    acceptance(
        "cancel_tx_refused_while_request_in_flight",
        |guard, fixture| async move {
            let (gate, mvcc) = storage(&fixture).await;
            let keys = [seed(&fixture, 1).await];
            // Not held by `hold`: the transaction stages its own successful
            // write here, so it owns a record lock before the parking begins.
            seed(&fixture, 2).await;
            let baseline = mvcc.locks_len();
            assert_eq!(baseline, 0, "nothing else holds a record lock");
            let older = hold(&mvcc, &gate, &keys).await;

            let owner = fixture.user("in_flight_owner", 1);
            let handle = match fixture.send(&owner, &tx_begin_pessimistic_req()).await {
                DbResponse::TxOpened { tx_handle, .. } => tx_handle,
                other => panic!("pessimistic tx begin: {other:?}"),
            };
            assert!(
                matches!(
                    fixture.send(&owner, &tx_update_req(handle, 2)).await,
                    DbResponse::TxBatch { .. }
                ),
                "the transaction's own staged write must hold its record lock"
            );
            guard.mark("interactive tx holds the unblocked row's record lock");

            let TxParkedState::Open(tx_arc) = fixture.handler.tx_registry().peek(handle) else {
                panic!("the open transaction must be parked under its handle");
            };
            // Second update targets the HELD row, so the request parks while
            // holding this transaction's context lock.
            let mut pending = fixture.start(Arc::clone(&owner), tx_update_req(handle, 1));
            let _id = fixture
                .waiting(&mut pending, &owner, ActivityRequestState::Running, || {
                    tx_arc.ctx().try_lock().is_err()
                })
                .await;
            assert!(
                !pending.is_finished(),
                "the request must still be waiting for the held row"
            );
            guard.mark("TxExecute parked on the held row inside the transaction");
            let parked_locks = mvcc.locks_len();
            assert_eq!(
                parked_locks,
                baseline + 2,
                "the fixture blocker plus the transaction's own staged row lock"
            );

            fixture
                .expect_tx_cancel(&fixture.admin, handle, CancelOutcome::NotCancellable)
                .await;
            guard.mark("cancel answered not-cancellable while the request runs");

            assert!(
                listed_as(&fixture, handle, ActivityTxState::Active).await,
                "a refused cancel must leave the transaction listed as active"
            );
            assert_eq!(fixture.handler.tx_registry().len(), 1);
            assert_eq!(
                mvcc.locks_len(),
                parked_locks,
                "a refused cancel must release no record lock"
            );
            assert!(
                !gate.active_snapshots_empty(),
                "a refused cancel must keep the transaction's snapshot pinned"
            );

            mvcc.release_locks(older, &keys).await;
            assert!(
                matches!(
                    pending.await.expect("in-flight request joined"),
                    DbResponse::TxBatch { .. }
                ),
                "the released request must finish normally inside the transaction"
            );
            guard.mark("the in-flight request completed after the blocker released");

            fixture
                .expect_tx_cancel(&fixture.admin, handle, CancelOutcome::Cancelled)
                .await;
            // The probe's clone is local only; release it so the cancelled
            // transaction's snapshot guard drops with the registry's copy.
            drop(tx_arc);
            guard.mark("the idle transaction cancelled successfully");

            assert_eq!(mvcc.locks_len(), baseline, "every record lock is released");
            assert_eq!(fixture.handler.tx_registry().len(), 0);
            assert!(gate.active_snapshots_empty());
            assert!(!listed_as(&fixture, handle, ActivityTxState::Active).await);
        },
    );
}

/// H2: a handle re-minted for another user's transaction is never removed
/// under stale authorization.
///
/// **Registry half (the part that can be forced).** Transaction ids are minted
/// per repo by a monotonic counter (`RepoTxGate::fresh_tx_id`), so a fixture
/// can never observe a genuinely re-minted handle; the replacement is
/// registered EXPLICITLY under Alice's freed handle, which is exactly the
/// state the handler races against.
///
/// **Handler half (asserted on the public mapping only).** A handle the
/// registry does not hold maps to `already_finished` before the permission
/// check, so the stale-authorization outcome the handler produces for such a
/// removal is asserted through that wire path rather than by cancelling Bob's
/// live handle (a foreign-owner cancel here would be a permission decision,
/// not the removal decision under test).
#[test]
fn cancel_tx_never_removes_replacement_under_stale_authorization() {
    acceptance(
        "cancel_tx_never_removes_replacement_under_stale_authorization",
        |guard, fixture| async move {
            seed(&fixture, 1).await;
            assert!(fixture.handler.tx_registry().is_empty());

            let alice = fixture.user("stale_alice", 1);
            let handle = match fixture.send(&alice, &tx_begin_req()).await {
                DbResponse::TxOpened { tx_handle, .. } => tx_handle,
                other => panic!("interactive tx begin: {other:?}"),
            };
            let TxParkedState::Open(alice_tx) = fixture.handler.tx_registry().peek(handle) else {
                panic!("the authorized transaction must be parked under its handle");
            };
            guard.mark("alice's transaction captured as the stale authorization");

            assert!(matches!(
                fixture.send(&alice, &tx_rollback_req(handle)).await,
                DbResponse::TxRolledBack { tx_handle: got } if got == handle
            ));
            assert!(fixture.handler.tx_registry().is_empty());

            let bob = fixture.user("stale_bob", 2);
            let (bob_ctx, bob_snapshot, _bob_repo) = fixture
                .handler
                .db()
                .tx_begin_as(Actor::User(principal64(bob.user_id)), DB, REPO, "snapshot")
                .await
                .expect("begin bob's replacement transaction");
            let bob_tx = fixture
                .handler
                .tx_registry()
                .register_activity(
                    handle,
                    InteractiveTx::new(
                        bob_ctx,
                        bob_snapshot,
                        bob.session_id,
                        bob.user_id,
                        DB.to_string(),
                        REPO.to_string(),
                        Duration::from_secs(300),
                        fixture.handler.tx_abort_release(DB, REPO),
                    ),
                    bob.activity_handle(),
                    Arc::from(bob.username.as_str()),
                )
                .expect("alice's freed handle must accept the replacement");
            assert!(!Arc::ptr_eq(&alice_tx, &bob_tx));
            guard.mark("bob's replacement occupies alice's old handle");

            // Registry half: the stale identity must not remove Bob's row.
            let stale = fixture
                .handler
                .tx_registry()
                .remove_identity(handle, &alice_tx);
            assert!(
                matches!(stale, TxParkedState::Gone),
                "a stale authorization must observe the replacement as gone"
            );
            guard.mark("identity-checked removal refused the stale authorization");

            assert_eq!(
                fixture.handler.tx_registry().len(),
                1,
                "bob's transaction must survive the stale removal"
            );
            let row = fixture
                .handler
                .tx_registry()
                .activity_row(handle)
                .expect("bob's projection row must still exist");
            assert_eq!(row.user_id(), &bob.user_id);
            assert_eq!(row.username(), bob.username.as_str());

            // Both captures are local clones only (the registry holds its own
            // `Arc`), so release them here: the stale handle's snapshot must
            // not stay pinned past this test.
            drop(alice_tx);
            drop(bob_tx);

            assert!(
                matches!(
                    fixture.send(&bob, &tx_update_req(handle, 1)).await,
                    DbResponse::TxBatch { .. }
                ),
                "bob's replacement transaction must stay executable for bob"
            );
            guard.mark("bob executed his own transaction after the stale removal");

            // Handler half: a handle the registry does not hold answers
            // `already_finished` before any permission check.
            fixture
                .expect_tx_cancel(&fixture.admin, u64::MAX, CancelOutcome::AlreadyFinished)
                .await;
            guard.mark("an unknown handle answers already-finished");

            assert!(matches!(
                fixture.send(&bob, &tx_rollback_req(handle)).await,
                DbResponse::TxRolledBack { tx_handle: got } if got == handle
            ));
            assert!(fixture.handler.tx_registry().is_empty());
            fixture
                .expect_tx_cancel(&fixture.admin, handle, CancelOutcome::AlreadyFinished)
                .await;
            guard.mark("the removed handle answers already-finished");
        },
    );
}

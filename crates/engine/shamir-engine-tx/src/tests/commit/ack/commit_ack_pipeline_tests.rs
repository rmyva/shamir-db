use std::future::{poll_fn, Future};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use shamir_query_builder::write::doc;
use shamir_query_builder::{batch::Batch, write};
use shamir_query_types::batch::{VisibilityPendingReason, VisibilityState, VisibilityWait};
use shamir_storage::error::DbResult;
use shamir_tx::{CommitVisibility, IsolationLevel};
use shamir_types::access::Actor;
use shamir_types::types::value::InnerValue;
use tokio::sync::Notify;

use crate::tx::commit::{
    AsyncIndexTailHook, TxError, TEST_ASYNC_INDEX_TAIL_HOOK, TEST_SYNCED_WAL_REPO,
};
use crate::tx::materialize::TEST_PRE_MATERIALIZE_REPO;
use crate::tx::query::batch::{execute_batch_unchecked, BatchResolver, TableResolver};
use crate::tx::query::TableRef;
use crate::tx::repo::RepoInstance;
use crate::tx::table::TableManager;
use crate::tx::tests::drain::orphans::post_wal_orphan_ordering_tests::fixture;
use crate::tx::MaterializationState;

const STATUS: VisibilityWait = VisibilityWait::Bounded { timeout_ms: 0 };

struct Resolver(RepoInstance);

#[async_trait::async_trait]
impl TableResolver for Resolver {
    async fn resolve(&self, table: &TableRef) -> DbResult<TableManager> {
        self.0.get_table(&table.table).await
    }
}

#[async_trait::async_trait]
impl BatchResolver for Resolver {
    async fn resolve_repo(&self, _: &str) -> DbResult<crate::tx::RepoCore> {
        Ok(self.0.core.clone())
    }
}

// Red before VIS-3: a transactional Batch's visibility_wait was ignored.
#[tokio::test(start_paused = true)]
async fn transactional_batch_forwards_policy_and_retains_results() {
    let (repo, _) = fixture("ack_batch").await;
    let gate = repo.tx_gate().await.unwrap();
    let lower = gate.assign_next_version_guarded();
    let mut batch = Batch::new();
    batch.transactional().visibility_wait(STATUS);
    batch.insert(
        "written",
        write::insert("b").row(doc().set("name", "receipt").unwrap()),
    );
    let request = batch.build();
    let resolver = Resolver(repo.clone());
    // Spawn the higher operation: never await another parked commit inline.
    let task = tokio::spawn(async move {
        execute_batch_unchecked(&request, &resolver, None, None, Actor::System, "test").await
    });
    let response = task.await.unwrap().unwrap();
    assert!(response.results.contains_key("written"));
    let info = response.transaction.unwrap();
    assert!(info.is_committed());
    assert_eq!(
        info.visibility,
        Some(VisibilityState::Pending {
            visible_through: 0,
            blocking_version: 1,
            reason: VisibilityPendingReason::Timeout,
        })
    );
    assert!(info.materialized);
    assert!(info.commit_version.unwrap() > lower.version());
    lower.commit();
    assert_eq!(
        repo.observe_visibility(info.commit_version.unwrap(), STATUS)
            .await
            .unwrap(),
        VisibilityState::Visible
    );
}

#[tokio::test(start_paused = true)]
async fn parked_lower_real_commit_bounds_higher_ack_and_then_converges() {
    let (repo, hook) = fixture("ack_real_lower").await;
    TEST_PRE_MATERIALIZE_REPO
        .set(repo.name().to_owned())
        .unwrap();
    let gate = repo.tx_gate().await.unwrap();
    let a = repo.get_table("a").await.unwrap();
    let b = repo.get_table("b").await.unwrap();
    let (mut tx_a, pin_a) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    a.insert_tx(&InnerValue::Str("lower".into()), Some(&mut tx_a))
        .await
        .unwrap();
    hook.armed.store(true, Ordering::Release);
    let owned = repo.clone();
    let task_a = tokio::spawn(async move { owned.commit_tx(tx_a).await });
    hook.reached.notified().await;
    let lower = gate.version_allocation_high_water_mark();
    let floor = gate.visible_floor();
    let (mut tx_b, pin_b) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let id = b
        .insert_tx(&InnerValue::Str("higher".into()), Some(&mut tx_b))
        .await
        .unwrap();
    let owned = repo.clone();
    let signal_gate = Arc::clone(&gate);
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let task_b = tokio::spawn(async move {
        let mut ack = Box::pin(
            owned.commit_tx_with_options(tx_b, VisibilityWait::Bounded { timeout_ms: 17 }),
        );
        let mut sender = Some(sender);
        poll_fn(|cx| {
            let result = ack.as_mut().poll(cx);
            if signal_gate.completion().waiter_count() == 1 {
                assert!(!signal_gate.is_active_post_wal(lower + 1));
                assert!(result.is_pending());
                if let Some(sender) = sender.take() {
                    sender.send(()).unwrap();
                }
            }
            result
        })
        .await
    });
    receiver.await.unwrap();
    assert_eq!(gate.visible_floor(), floor);
    assert!(gate.is_active_post_wal(lower));
    tokio::time::advance(Duration::from_millis(17)).await;
    let receipt = task_b.await.unwrap().unwrap();
    assert_eq!(receipt.commit_version, lower + 1);
    assert_eq!(
        receipt.visibility,
        VisibilityState::Pending {
            visible_through: floor,
            blocking_version: floor + 1,
            reason: VisibilityPendingReason::Timeout,
        }
    );
    assert_eq!(receipt.materialization, MaterializationState::Complete);
    assert_eq!(gate.completion().waiter_count(), 0);
    assert!(gate.is_active_post_wal(lower));
    hook.resume.notify_one();
    assert_eq!(
        task_a.await.unwrap().unwrap().visibility,
        VisibilityState::Visible
    );
    assert_eq!(
        repo.observe_visibility(receipt.commit_version, STATUS)
            .await
            .unwrap(),
        VisibilityState::Visible
    );
    assert_eq!(b.get(id).await.unwrap(), InnerValue::Str("higher".into()));
    drop((pin_a, pin_b));
}

#[tokio::test(start_paused = true)]
async fn construction_policy_applies_to_commit_and_explicit_strict_overrides_it() {
    let (repo, _) = fixture("ack_default").await;
    let repo = repo.with_commit_visibility_wait(STATUS);
    let gate = repo.tx_gate().await.unwrap();
    let lower = gate.assign_next_version_guarded();
    let table = repo.get_table("b").await.unwrap();
    let (mut tx, pin) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    table
        .insert_tx(&InnerValue::Str("default".into()), Some(&mut tx))
        .await
        .unwrap();
    let receipt = crate::tx::commit_tx(tx, &repo.core).await.unwrap();
    assert!(matches!(
        receipt.visibility,
        VisibilityState::Pending { .. }
    ));
    let (mut tx, pin2) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    table
        .insert_tx(&InnerValue::Str("strict".into()), Some(&mut tx))
        .await
        .unwrap();
    let target = gate.version_allocation_high_water_mark() + 1;
    let mut ack = Box::pin(repo.commit_tx_with_options(tx, VisibilityWait::Strict));
    poll_fn(|cx| {
        let result = ack.as_mut().poll(cx);
        if gate.completion().waiter_count() == 1 {
            assert_eq!(gate.version_allocation_high_water_mark(), target);
            assert!(!gate.is_active_post_wal(target));
            assert!(result.is_pending());
            Poll::Ready(())
        } else {
            assert!(result.is_pending());
            Poll::Pending
        }
    })
    .await;
    lower.commit();
    assert_eq!(ack.await.unwrap().visibility, VisibilityState::Visible);
    drop((pin, pin2));
}

// The hook is inside the inner driver after WAL acceptance: even an expired
// visibility budget cannot race it or return a fabricated Pending outcome.
#[tokio::test(start_paused = true)]
async fn bounded_budget_starts_after_inner_known_outcome() {
    let (repo, hook) = fixture("ack_inner_budget").await;
    let table = repo.get_table("a").await.unwrap();
    let (mut tx, pin) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    table
        .insert_tx(&InnerValue::Str("inner".into()), Some(&mut tx))
        .await
        .unwrap();
    hook.armed.store(true, Ordering::Release);
    let owned = repo.clone();
    let task = tokio::spawn(async move { owned.commit_tx_with_options(tx, STATUS).await });
    hook.reached.notified().await;
    tokio::time::advance(Duration::from_secs(60)).await;
    assert!(!task.is_finished());
    let gate = repo.tx_gate().await.unwrap();
    assert_eq!(gate.completion().waiter_count(), 0);
    assert_eq!(gate.visible_floor(), 0);
    hook.resume.notify_one();
    let receipt = task.await.unwrap().unwrap();
    assert_eq!(receipt.visibility, VisibilityState::Visible);
    assert_eq!(receipt.materialization, MaterializationState::Complete);
    drop(pin);
}

// Drop only an acknowledgement while the separate AsyncIndex tail is parked.
// Neither lower completion nor tail ownership may be affected by observer drop.
#[tokio::test(start_paused = true)]
async fn cancellation_releases_waiter_without_aborting_async_tail() {
    let (repo, _) = fixture("ack_cancel_tail").await;
    let tail = Arc::new(AsyncIndexTailHook {
        repo_name: repo.name().to_owned(),
        reached: Notify::new(),
        resume: Notify::new(),
        armed: AtomicBool::new(true),
    });
    TEST_ASYNC_INDEX_TAIL_HOOK.set(Arc::clone(&tail)).unwrap();
    let gate = repo.tx_gate().await.unwrap();
    let lower = gate.assign_next_version_guarded();
    let table = repo.get_table("b").await.unwrap();
    let (mut tx, pin) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    tx.set_visibility(CommitVisibility::AsyncIndex);
    let id = table
        .insert_tx(&InnerValue::Str("survives".into()), Some(&mut tx))
        .await
        .unwrap();
    let target = gate.version_allocation_high_water_mark() + 1;
    let mut ack = Box::pin(repo.commit_tx_with_options(tx, VisibilityWait::Strict));
    poll_fn(|cx| {
        assert!(ack.as_mut().poll(cx).is_pending());
        if gate.completion().waiter_count() == 1 {
            assert_eq!(gate.version_allocation_high_water_mark(), target);
            assert!(!gate.is_active_post_wal(target));
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })
    .await;
    tail.reached.notified().await;
    assert_eq!(gate.completion().waiter_count(), 1);
    drop(ack);
    assert_eq!(gate.completion().waiter_count(), 0);
    assert_eq!(gate.visible_floor(), 0);
    assert_eq!(repo.drainer().window_keys(), vec![target]);
    assert!(gate.durable_watermark() < target);
    lower.commit();
    gate.mark_durable(target - 1);
    tail.resume.notify_one();
    repo.quiesce_commits(Duration::from_secs(1)).await.unwrap();
    assert_eq!(
        repo.observe_visibility(target, STATUS).await.unwrap(),
        VisibilityState::Visible
    );
    repo.drainer().drain_all(&repo.core).await.unwrap();
    assert_eq!(
        table.get(id).await.unwrap(),
        InnerValue::Str("survives".into())
    );
    assert_eq!(gate.durable_watermark(), target);
    assert_eq!(gate.completion().waiter_count(), 0);
    drop(pin);
}

// Real published WAL warning, not only a constructed error fixture.
#[tokio::test(start_paused = true)]
async fn wal_durability_warning_and_pending_coexist_on_real_commit() {
    let (repo, _) = fixture("ack_wal_pending").await;
    TEST_SYNCED_WAL_REPO.set(repo.name().to_owned()).unwrap();
    TEST_PRE_MATERIALIZE_REPO
        .set(repo.name().to_owned())
        .unwrap();
    let gate = repo.tx_gate().await.unwrap();
    let lower = gate.assign_next_version_guarded();
    let table = repo.get_table("b").await.unwrap();
    let (mut tx, pin) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    table
        .insert_tx(&InnerValue::Str("published".into()), Some(&mut tx))
        .await
        .unwrap();
    repo.repo_wal().await.unwrap().arm_fail_next_sync_for_test();
    let error = repo.commit_tx_with_options(tx, STATUS).await.unwrap_err();
    assert!(
        matches!(
            &error,
            TxError::WalDurability {
                materialization: MaterializationState::Complete,
                visibility: VisibilityState::Pending {
                    reason: VisibilityPendingReason::Timeout,
                    ..
                },
                ..
            }
        ),
        "{error:?}"
    );
    let info = error.into_transaction_info(7, 0);
    assert!(info.is_committed());
    assert_eq!(info.error.as_deref(), Some("wal_durability"));
    assert_eq!(gate.visible_floor(), 0);
    lower.commit();
    assert_eq!(
        repo.observe_visibility(info.commit_version.unwrap(), STATUS)
            .await
            .unwrap(),
        VisibilityState::Visible
    );
    drop(pin);
}

#[tokio::test(start_paused = true)]
async fn real_abort_and_indeterminate_entry_keep_no_commit_metadata() {
    let (repo, _) = fixture("ack_abort").await;
    let gate = repo.tx_gate().await.unwrap();
    let table = repo.get_table("b").await.unwrap();
    let (mut tx, pin) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    table
        .insert_tx(&InnerValue::Str("aborted".into()), Some(&mut tx))
        .await
        .unwrap();
    tx.wounded.store(true, Ordering::Release);
    let error = repo.commit_tx_with_options(tx, STATUS).await.unwrap_err();
    assert!(matches!(&error, TxError::Wounded { .. }), "{error:?}");
    let info = error.into_transaction_info(7, 0);
    assert!(!info.is_committed());
    assert_eq!(info.visibility, None);
    assert_eq!(gate.version_allocation_high_water_mark(), 0);
    let error =
        crate::tx::commit::wal_completion(&gate, 1, Err(shamir_wal::WalAppendError::Indeterminate))
            .0
            .unwrap_err();
    let info =
        crate::tx::commit::acknowledge_known_outcome(Err(error), &repo.core, &gate, STATUS, 0)
            .await
            .unwrap_err()
            .into_transaction_info(7, 0);
    assert!(!info.is_committed());
    assert_eq!(info.visibility, None);
    assert!(gate.requires_reopen());
    assert_eq!(gate.completion().waiter_count(), 0);
    drop(pin);
}

// Existing async durability-error join is INSIDE the inner driver. It is not
// covered by the bounded acknowledgement even when its timeout is zero.
#[tokio::test(start_paused = true)]
async fn async_durability_error_tail_is_outside_acknowledgement_budget() {
    let (repo, _) = fixture("ack_async_wal_tail").await;
    TEST_SYNCED_WAL_REPO.set(repo.name().to_owned()).unwrap();
    let tail = Arc::new(AsyncIndexTailHook {
        repo_name: repo.name().to_owned(),
        reached: Notify::new(),
        resume: Notify::new(),
        armed: AtomicBool::new(true),
    });
    TEST_ASYNC_INDEX_TAIL_HOOK.set(Arc::clone(&tail)).unwrap();
    let table = repo.get_table("b").await.unwrap();
    let (mut tx, pin) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    tx.set_visibility(CommitVisibility::AsyncIndex);
    table
        .insert_tx(&InnerValue::Str("tail".into()), Some(&mut tx))
        .await
        .unwrap();
    repo.repo_wal().await.unwrap().arm_fail_next_sync_for_test();
    let owned = repo.clone();
    let task = tokio::spawn(async move { owned.commit_tx_with_options(tx, STATUS).await });
    tail.reached.notified().await;
    tokio::time::advance(Duration::from_secs(60)).await;
    assert!(!task.is_finished());
    assert_eq!(repo.tx_gate().await.unwrap().completion().waiter_count(), 0);
    tail.resume.notify_one();
    let error = task.await.unwrap().unwrap_err();
    assert!(
        matches!(
            &error,
            TxError::WalDurability {
                visibility: VisibilityState::Visible,
                ..
            }
        ),
        "{error:?}"
    );
    drop(pin);
}

#[tokio::test(start_paused = true)]
async fn nested_bounded_override_in_transactional_outer_rejected_before_any_write() {
    let (repo, _) = fixture("ack_nested_reject").await;
    let mut inner = Batch::new();
    inner.transactional().visibility_wait(STATUS);
    inner.insert(
        "inner",
        write::insert("b").row(doc().set("name", "inner").unwrap()),
    );
    let mut outer = Batch::new();
    outer.transactional();
    outer.insert(
        "outer",
        write::insert("a").row(doc().set("name", "outer").unwrap()),
    );
    outer.sub_batch_no_bind("nested", inner.build());
    let result = execute_batch_unchecked(
        &outer.build(),
        &Resolver(repo.clone()),
        None,
        None,
        Actor::System,
        "test",
    )
    .await;
    assert!(
        matches!(&result, Err(crate::tx::query::batch::BatchError::QueryError {
        code: Some(code), ..
    }) if code == "validation"),
        "{result:?}"
    );
    assert_eq!(
        repo.tx_gate()
            .await
            .unwrap()
            .version_allocation_high_water_mark(),
        0
    );
}

#[tokio::test(start_paused = true)]
async fn nested_bounded_override_in_implicit_outer_commits_with_receipts() {
    let (repo, _) = fixture("ack_nested_implicit").await;
    let mut inner = Batch::new();
    inner.transactional().visibility_wait(STATUS);
    inner.insert(
        "inner",
        write::insert("b").row(doc().set("name", "inner").unwrap()),
    );
    let mut outer = Batch::new();
    outer.insert(
        "outer",
        write::insert("a").row(doc().set("name", "outer").unwrap()),
    );
    outer.sub_batch_no_bind("nested", inner.build());
    let response = execute_batch_unchecked(
        &outer.build(),
        &Resolver(repo.clone()),
        None,
        None,
        Actor::System,
        "test",
    )
    .await
    .unwrap();
    let receipt = response.results["outer"].receipt.as_ref().unwrap();
    assert!(receipt.is_committed(), "{receipt:?}");
    assert!(matches!(receipt.visibility, Some(VisibilityState::Visible)));
    assert!(
        repo.tx_gate()
            .await
            .unwrap()
            .version_allocation_high_water_mark()
            >= 1
    );
}

#[tokio::test(start_paused = true)]
async fn non_transactional_bounded_batch_returns_alias_receipt() {
    let (repo, _) = fixture("ack_batch_receipt").await;
    let mut batch = Batch::new();
    batch.visibility_wait(STATUS);
    batch.insert(
        "written",
        write::insert("b").row(doc().set("name", "implicit").unwrap()),
    );
    let request = batch.build();
    let response = execute_batch_unchecked(
        &request,
        &Resolver(repo.clone()),
        None,
        None,
        Actor::System,
        "test",
    )
    .await
    .unwrap();
    let receipt = response.results["written"].receipt.as_ref().unwrap();
    assert!(receipt.is_committed(), "{receipt:?}");
    assert!(matches!(receipt.visibility, Some(VisibilityState::Visible)));
    assert!(
        repo.tx_gate()
            .await
            .unwrap()
            .version_allocation_high_water_mark()
            >= 1
    );
}

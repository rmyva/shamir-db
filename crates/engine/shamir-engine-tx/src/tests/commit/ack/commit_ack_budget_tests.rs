use std::future::{poll_fn, Future};
use std::task::Poll;
use std::time::Duration;

use shamir_query_types::batch::{
    TransactionInfo, VisibilityPendingReason, VisibilityState, VisibilityWait,
};
use shamir_storage::error::DbError;
use shamir_tx::IsolationLevel;
use shamir_types::types::value::InnerValue;

use crate::tx::commit::{acknowledge_known_outcome, TxError};
use crate::tx::tests::drain::orphans::post_wal_orphan_ordering_tests::fixture;
use crate::tx::{MaterializationState, TxOutcome};

const BOUNDED: VisibilityWait = VisibilityWait::Bounded { timeout_ms: 17 };

fn outcome(version: u64) -> Result<TxOutcome, TxError> {
    Ok(TxOutcome {
        tx_id: 7,
        snapshot_version: 0,
        commit_version: version,
        visibility: VisibilityState::Visible,
        materialization: MaterializationState::Complete,
        background: None,
    })
}

async fn pending_poll<F: Future>(future: &mut std::pin::Pin<Box<F>>) {
    poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
}

// Red before VIS-3: the wrapper cannot return Pending, and would park for
// another independent five seconds under this artificial undrained gap.
#[tokio::test(start_paused = true)]
async fn timeout_skips_backpressure_and_reconciles_without_changing_materialization() {
    let (repo, _) = fixture("ack_timeout").await;
    let gate = repo.tx_gate().await.unwrap();
    let lower = gate.assign_next_version_guarded();
    let higher = gate.assign_next_version_guarded();
    let target = higher.version();
    higher.commit();
    gate.raise_published(target);
    let mut ack = Box::pin(acknowledge_known_outcome(
        outcome(target),
        &repo.core,
        &gate,
        BOUNDED,
        0,
    ));
    pending_poll(&mut ack).await;
    assert_eq!(gate.completion().waiter_count(), 1);
    tokio::time::advance(Duration::from_millis(17)).await;
    let receipt = ack.await.unwrap();
    assert_eq!(
        receipt.visibility,
        VisibilityState::Pending {
            visible_through: 0,
            blocking_version: 1,
            reason: VisibilityPendingReason::Timeout,
        }
    );
    assert_eq!(receipt.materialization, MaterializationState::Complete);
    assert_eq!(gate.completion().waiter_count(), 0);
    assert_eq!(gate.visible_floor(), 0);
    let info = TransactionInfo::committed(7, 0, target, receipt.materialized())
        .with_visibility(receipt.visibility);
    assert!(info.is_committed());
    assert!(!info.is_visible());
    // Serde round trip is the subject, not query construction.
    let bytes = rmp_serde::to_vec_named(&info).unwrap();
    let decoded: TransactionInfo = rmp_serde::from_slice(&bytes).unwrap();
    assert_eq!(decoded.visibility, info.visibility);
    assert_eq!(decoded.commit_version, Some(target));
    lower.commit();
    assert_eq!(
        repo.observe_visibility(target, BOUNDED).await.unwrap(),
        VisibilityState::Visible
    );
}

// Strict deliberately remains indefinitely pending until the prefix settles.
#[tokio::test(start_paused = true)]
async fn strict_waits_then_returns_visible() {
    let (repo, _) = fixture("ack_strict").await;
    let gate = repo.tx_gate().await.unwrap();
    let lower = gate.assign_next_version_guarded();
    let higher = gate.assign_next_version_guarded();
    let target = higher.version();
    higher.commit();
    let mut ack = Box::pin(acknowledge_known_outcome(
        outcome(target),
        &repo.core,
        &gate,
        VisibilityWait::Strict,
        u64::MAX,
    ));
    pending_poll(&mut ack).await;
    tokio::time::advance(Duration::from_secs(60)).await;
    pending_poll(&mut ack).await;
    lower.commit();
    assert_eq!(ack.await.unwrap().visibility, VisibilityState::Visible);
    assert_eq!(gate.completion().waiter_count(), 0);
}

// Reopen keeps the established Deferred marker; visible wins over reopen.
#[tokio::test(start_paused = true)]
async fn reopen_keeps_deferred_marker_and_visible_wins_over_reopen() {
    let (repo, _) = fixture("ack_reopen").await;
    let gate = repo.tx_gate().await.unwrap();
    let lower = gate.assign_next_version_guarded();
    let target = lower.version();
    gate.set_requires_reopen();
    let receipt = acknowledge_known_outcome(
        outcome(target),
        &repo.core,
        &gate,
        VisibilityWait::Strict,
        0,
    )
    .await
    .unwrap();
    assert_eq!(
        receipt.visibility,
        VisibilityState::Pending {
            visible_through: 0,
            blocking_version: 1,
            reason: VisibilityPendingReason::ReopenRequired,
        }
    );
    assert_eq!(receipt.materialization, MaterializationState::Deferred);
    lower.commit();
    let receipt = acknowledge_known_outcome(outcome(target), &repo.core, &gate, BOUNDED, u64::MAX)
        .await
        .unwrap();
    assert_eq!(receipt.visibility, VisibilityState::Visible);
}

#[tokio::test(start_paused = true)]
async fn settlement_at_expiry_wins_and_backpressure_has_no_fresh_budget() {
    let (repo, _) = fixture("ack_expiry").await;
    let gate = repo.tx_gate().await.unwrap();
    let lower = gate.assign_next_version_guarded();
    let higher = gate.assign_next_version_guarded();
    let target = higher.version();
    higher.commit();
    gate.raise_published(target);
    let mut ack = Box::pin(acknowledge_known_outcome(
        outcome(target),
        &repo.core,
        &gate,
        BOUNDED,
        0,
    ));
    pending_poll(&mut ack).await;
    tokio::time::advance(Duration::from_millis(17)).await;
    lower.commit();
    gate.set_requires_reopen();
    // Manual poll pins the expiry interleaving: no runtime auto-advance can
    // conceal a fresh backpressure budget after visibility won.
    poll_fn(|cx| {
        let Poll::Ready(result) = ack.as_mut().poll(cx) else {
            panic!("expired acknowledgement budget must not park again");
        };
        assert_eq!(result.unwrap().visibility, VisibilityState::Visible);
        Poll::Ready(())
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn visible_backpressure_uses_only_remaining_budget() {
    let (repo, _) = fixture("ack_remaining").await;
    let gate = repo.tx_gate().await.unwrap();
    let lower = gate.assign_next_version_guarded();
    let higher = gate.assign_next_version_guarded();
    let target = higher.version();
    higher.commit();
    gate.raise_published(target);
    let mut ack = Box::pin(acknowledge_known_outcome(
        outcome(target),
        &repo.core,
        &gate,
        BOUNDED,
        0,
    ));
    pending_poll(&mut ack).await;
    tokio::time::advance(Duration::from_millis(12)).await;
    lower.commit();
    pending_poll(&mut ack).await;
    assert_eq!(gate.completion().waiter_count(), 0);
    tokio::time::advance(Duration::from_millis(5)).await;
    poll_fn(|cx| {
        let Poll::Ready(result) = ack.as_mut().poll(cx) else {
            panic!("soft brake must use the remaining five milliseconds");
        };
        assert_eq!(result.unwrap().visibility, VisibilityState::Visible);
        Poll::Ready(())
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn strict_visible_keeps_five_second_soft_brake_ceiling() {
    let (repo, _) = fixture("ack_strict_brake").await;
    let gate = repo.tx_gate().await.unwrap();
    let version = gate.assign_next_version_guarded();
    let target = version.version();
    version.commit();
    let mut ack = Box::pin(acknowledge_known_outcome(
        outcome(target),
        &repo.core,
        &gate,
        VisibilityWait::Strict,
        0,
    ));
    pending_poll(&mut ack).await;
    tokio::time::advance(Duration::from_millis(4999)).await;
    pending_poll(&mut ack).await;
    tokio::time::advance(Duration::from_millis(1)).await;
    poll_fn(|cx| {
        let Poll::Ready(result) = ack.as_mut().poll(cx) else {
            panic!("Strict keeps the existing five-second soft brake ceiling");
        };
        assert_eq!(result.unwrap().visibility, VisibilityState::Visible);
        Poll::Ready(())
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn strict_empty_commit_keeps_soft_backpressure() {
    let (repo, _) = fixture("ack_empty_strict_brake").await;
    let gate = repo.tx_gate().await.unwrap();
    let version = gate.assign_next_version_guarded();
    version.commit();
    let mut ack = Box::pin(acknowledge_known_outcome(
        outcome(0),
        &repo.core,
        &gate,
        VisibilityWait::Strict,
        0,
    ));
    pending_poll(&mut ack).await;
    assert_eq!(gate.completion().waiter_count(), 0);
    tokio::time::advance(Duration::from_millis(4999)).await;
    pending_poll(&mut ack).await;
    tokio::time::advance(Duration::from_millis(1)).await;
    poll_fn(|cx| {
        let Poll::Ready(result) = ack.as_mut().poll(cx) else {
            panic!("empty Strict commit keeps the five-second soft brake ceiling");
        };
        assert_eq!(result.unwrap().visibility, VisibilityState::Visible);
        Poll::Ready(())
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn no_allocation_and_immediate_visible_need_no_waiter() {
    let (repo, _) = fixture("ack_no_allocation").await;
    let gate = repo.tx_gate().await.unwrap();
    let lower = gate.assign_next_version_guarded();
    gate.raise_published(lower.version());
    let before = tokio::time::Instant::now();
    let (tx, pin) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let receipt = repo.commit_tx_with_options(tx, BOUNDED).await.unwrap();
    assert_eq!(receipt.commit_version, receipt.snapshot_version);
    assert_eq!(receipt.visibility, VisibilityState::Visible);
    assert_eq!(tokio::time::Instant::now(), before);
    assert_eq!(gate.completion().waiter_count(), 0);
    // No-allocation skips only visibility observation, not the soft brake.
    let mut ack = Box::pin(acknowledge_known_outcome(
        outcome(0),
        &repo.core,
        &gate,
        BOUNDED,
        0,
    ));
    pending_poll(&mut ack).await;
    assert_eq!(gate.completion().waiter_count(), 0);
    tokio::time::advance(Duration::from_millis(17)).await;
    poll_fn(|cx| {
        let Poll::Ready(result) = ack.as_mut().poll(cx) else {
            panic!("empty bounded commit must share its soft-brake budget");
        };
        assert_eq!(result.unwrap().visibility, VisibilityState::Visible);
        Poll::Ready(())
    })
    .await;
    lower.commit();
    let table = repo.get_table("a").await.unwrap();
    let (mut tx, write_pin) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let id = table
        .insert_tx(&InnerValue::Str("visible".into()), Some(&mut tx))
        .await
        .unwrap();
    let receipt = repo
        .commit_tx_with_options(tx, VisibilityWait::Bounded { timeout_ms: 0 })
        .await
        .unwrap();
    assert_eq!(receipt.visibility, VisibilityState::Visible);
    assert!(gate.visible_floor() >= receipt.commit_version);
    let allocated = gate.version_allocation_high_water_mark();
    let blocker = gate.assign_next_version_guarded();
    let (reader, reader_pin) = repo.begin_tx(IsolationLevel::Serializable).await.unwrap();
    assert_eq!(
        table.read_one_tx(id, Some(&reader)).await.unwrap(),
        InnerValue::Str("visible".into()),
    );
    let snapshot = reader.snapshot_version;
    let receipt = repo.commit_tx_with_options(reader, BOUNDED).await.unwrap();
    assert_eq!(receipt.commit_version, snapshot);
    assert_eq!(receipt.visibility, VisibilityState::Visible);
    assert_eq!(gate.version_allocation_high_water_mark(), allocated + 1);
    assert_eq!(gate.completion().waiter_count(), 0);
    assert_eq!(gate.visible_floor(), allocated);
    drop(blocker);
    drop((pin, write_pin, reader_pin));
}

#[tokio::test(start_paused = true)]
async fn published_warning_preserves_pending_and_abort_classification() {
    let (repo, _) = fixture("ack_errors").await;
    let gate = repo.tx_gate().await.unwrap();
    let lower = gate.assign_next_version_guarded();
    let higher = gate.assign_next_version_guarded();
    let target = higher.version();
    higher.commit();
    let error = acknowledge_known_outcome(
        Err(TxError::WalDurability {
            commit_version: target,
            materialization: MaterializationState::Deferred,
            visibility: VisibilityState::Visible,
        }),
        &repo.core,
        &gate,
        VisibilityWait::Bounded { timeout_ms: 0 },
        0,
    )
    .await
    .unwrap_err();
    let info = error.into_transaction_info(7, 0);
    assert!(info.is_committed());
    assert_eq!(info.error.as_deref(), Some("wal_durability"));
    assert_eq!(
        info.visibility,
        Some(VisibilityState::Pending {
            visible_through: 0,
            blocking_version: 1,
            reason: VisibilityPendingReason::Timeout,
        })
    );
    assert!(!info.materialized);
    for error in [
        TxError::WalRequiresReopen,
        TxError::Storage(DbError::Validation("abort".into())),
    ] {
        let info = acknowledge_known_outcome(Err(error), &repo.core, &gate, BOUNDED, 0)
            .await
            .unwrap_err()
            .into_transaction_info(7, 0);
        assert!(!info.is_committed());
        assert_eq!(info.visibility, None);
    }
    assert_eq!(gate.visible_floor(), 0);
    assert_eq!(gate.completion().waiter_count(), 0);
    drop(lower);
}

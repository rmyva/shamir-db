//! VIS-4c: metric recording on the transactional acknowledgement tail.
//! Attribution rule under test: the shared tail records the mode/result
//! counters and the duration histogram; the pending-KIND counter is recorded
//! only at the kind-known call sites (explicit `commit_tx`, implicit batch
//! receipts, direct CRUD writes).

use std::future::{poll_fn, Future};
use std::task::Poll;
use std::time::Duration;

use shamir_query_types::batch::{VisibilityPendingReason, VisibilityState, VisibilityWait};
use shamir_tx::visibility_metrics::{
    kind_index, mode_index, reason_index, result_index, AckMode, AckResult, PendingKind,
    PendingReason,
};
use shamir_tx::IsolationLevel;
use shamir_types::types::value::InnerValue;

use crate::query::batch::receipts::commit_implicit;
use crate::tx::commit::acknowledge_known_outcome;
use crate::tx::tests::drain::orphans::post_wal_orphan_ordering_tests::fixture;
use crate::tx::{CommitError, MaterializationState, TxOutcome};

const BOUNDED: VisibilityWait = VisibilityWait::Bounded { timeout_ms: 17 };
const BOUNDED_ZERO: VisibilityWait = VisibilityWait::Bounded { timeout_ms: 0 };

fn outcome(version: u64) -> Result<TxOutcome, CommitError> {
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

// Red before VIS-4c: the Strict/Visible fast path recorded nothing. It must
// cost exactly one counter increment and NO duration observation, no timer.
#[tokio::test(start_paused = true)]
async fn strict_fast_path_records_only_the_counter() {
    let (repo, _) = fixture("ack_metrics_fast").await;
    let gate = repo.tx_gate().await.unwrap();
    let version = gate.assign_next_version_guarded();
    version.commit();
    let target = gate.visible_floor();
    let before = tokio::time::Instant::now();
    let receipt = acknowledge_known_outcome(
        outcome(target),
        &repo.core,
        &gate,
        VisibilityWait::Strict,
        u64::MAX,
    )
    .await
    .unwrap();
    assert_eq!(receipt.visibility, VisibilityState::Visible);
    assert_eq!(tokio::time::Instant::now(), before, "no timer on fast path");
    let snap = gate.visibility().snapshot();
    let (s, v) = (
        mode_index(AckMode::Strict),
        result_index(AckResult::Visible),
    );
    assert_eq!(snap.ack_total[s][v], 1);
    assert_eq!(snap.ack_wait_ms[s][v].iter().sum::<u64>(), 0);
    assert_eq!(snap.pending_total, [[0; 3]; 2]);
}

// Red before VIS-4c: a bounded expiry recorded neither its result counter,
// nor a histogram observation, nor a structured pending fact.
#[tokio::test(start_paused = true)]
async fn bounded_timeout_records_counter_and_one_observation() {
    let (repo, _) = fixture("ack_metrics_timeout").await;
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
            blocking_version: lower.version(),
            reason: VisibilityPendingReason::Timeout,
        }
    );
    let snap = gate.visibility().snapshot();
    let (b, t) = (
        mode_index(AckMode::Bounded),
        result_index(AckResult::PendingTimeout),
    );
    assert_eq!(snap.ack_total[b][t], 1);
    assert_eq!(snap.ack_wait_ms[b][t].iter().sum::<u64>(), 1);
    // Kind attribution does NOT happen on the shared tail.
    assert_eq!(
        snap.pending_total[reason_index(PendingReason::Timeout)],
        [0; 3]
    );
    // Strict counters stay untouched by a bounded observation.
    assert_eq!(snap.ack_total[mode_index(AckMode::Strict)][t], 0);
    drop(lower);
}

// Red before VIS-4c: reopen-required recorded no distinct result.
#[tokio::test(start_paused = true)]
async fn reopen_records_the_reopen_result_without_a_wait() {
    let (repo, _) = fixture("ack_metrics_reopen").await;
    let gate = repo.tx_gate().await.unwrap();
    let blocker = gate.assign_next_version_guarded();
    let target = blocker.version();
    gate.set_requires_reopen();
    let receipt = acknowledge_known_outcome(outcome(target), &repo.core, &gate, BOUNDED_ZERO, 0)
        .await
        .unwrap();
    assert_eq!(
        receipt.visibility,
        VisibilityState::Pending {
            visible_through: 0,
            blocking_version: target,
            reason: VisibilityPendingReason::ReopenRequired,
        }
    );
    let snap = gate.visibility().snapshot();
    let r = result_index(AckResult::PendingReopen);
    assert_eq!(snap.ack_total[mode_index(AckMode::Bounded)][r], 1);
    // Reopen answered without blocking: no duration observation.
    assert_eq!(
        snap.ack_wait_ms[mode_index(AckMode::Bounded)][r]
            .iter()
            .sum::<u64>(),
        0
    );
    blocker.commit();
}

// Red before VIS-4c: pending kind attribution did not exist. The explicit
// transactional commit path (repo.commit_tx) must tag Pending as explicit_tx.
#[tokio::test(start_paused = true)]
async fn explicit_tx_pending_is_attributed_by_commit_tx() {
    let (repo, _) = fixture("ack_metrics_explicit").await;
    let repo = repo.with_commit_visibility_wait(BOUNDED_ZERO);
    let gate = repo.tx_gate().await.unwrap();
    let blocker = gate.assign_next_version_guarded();
    let table = repo.get_table("a").await.unwrap();
    let (mut tx, _pin) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let _id = table
        .insert_tx(&InnerValue::Str("pending".into()), Some(&mut tx))
        .await
        .unwrap();
    let receipt = repo.commit_tx(tx).await.unwrap();
    assert!(matches!(
        receipt.visibility,
        VisibilityState::Pending { .. }
    ));
    let snap = gate.visibility().snapshot();
    assert_eq!(
        snap.pending_total[reason_index(PendingReason::Timeout)]
            [kind_index(PendingKind::ExplicitTx)],
        1
    );
    assert_eq!(
        snap.pending_total[reason_index(PendingReason::Timeout)]
            [kind_index(PendingKind::ImplicitBatch)],
        0
    );
    blocker.commit();
}

// Red before VIS-4c: implicit-batch commits (the write aliases of a
// non-transactional batch) had no distinct pending kind from explicit txs.
#[tokio::test(start_paused = true)]
async fn implicit_batch_pending_is_attributed_by_commit_implicit() {
    let (repo, _) = fixture("ack_metrics_implicit").await;
    let gate = repo.tx_gate().await.unwrap();
    let blocker = gate.assign_next_version_guarded();
    let table = repo.get_table("a").await.unwrap();
    let (mut tx, _pin) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let _id = table
        .insert_tx(&InnerValue::Str("implicit".into()), Some(&mut tx))
        .await
        .unwrap();
    let info = commit_implicit(&repo.core, tx, "a", BOUNDED_ZERO)
        .await
        .unwrap();
    assert!(matches!(
        info.visibility,
        Some(VisibilityState::Pending { .. })
    ));
    let snap = gate.visibility().snapshot();
    assert_eq!(
        snap.pending_total[reason_index(PendingReason::Timeout)]
            [kind_index(PendingKind::ImplicitBatch)],
        1
    );
    assert_eq!(
        snap.pending_total[reason_index(PendingReason::Timeout)]
            [kind_index(PendingKind::ExplicitTx)],
        0
    );
    blocker.commit();
}

// Red before VIS-4c: direct CRUD writes had no receipt-kind recording at
// all; their pending must be tagged direct_write, not explicit_tx.
#[tokio::test(start_paused = true)]
async fn direct_write_pending_is_attributed_by_crud_receipt() {
    let (repo, _) = fixture("ack_metrics_direct").await;
    let gate = repo.tx_gate().await.unwrap();
    let blocker = gate.assign_next_version_guarded();
    let table = repo.get_table("a").await.unwrap();
    let receipt = table
        .insert_with_receipt(&InnerValue::Str("direct".into()), BOUNDED_ZERO)
        .await
        .unwrap();
    assert!(matches!(
        receipt.visibility,
        VisibilityState::Pending { .. }
    ));
    let snap = gate.visibility().snapshot();
    assert_eq!(
        snap.pending_total[reason_index(PendingReason::Timeout)]
            [kind_index(PendingKind::DirectWrite)],
        1
    );
    assert_eq!(
        snap.pending_total[reason_index(PendingReason::Timeout)]
            [kind_index(PendingKind::ExplicitTx)],
        0
    );
    blocker.commit();
}

// Red before VIS-4c: there was no sampled settled-prefix seam for the
// `changes_since` `visible_through` field; the sample must equal the gate's
// completion floor at response time.
#[tokio::test(start_paused = true)]
async fn visible_through_sample_equals_gate_floor() {
    let (repo, _) = fixture("ack_metrics_sample").await;
    let gate = repo.tx_gate().await.unwrap();
    let first = gate.assign_next_version_guarded();
    first.commit();
    let floor = gate.visible_floor();
    assert_eq!(repo.visible_through_sample(), Some(floor));
    let second = gate.assign_next_version_guarded();
    let sampled_before = repo.visible_through_sample();
    second.commit();
    assert_eq!(
        repo.visible_through_sample(),
        Some(floor + 1),
        "sample tracks the settled prefix, not the allocation high water"
    );
    assert_ne!(sampled_before, repo.visible_through_sample());
}

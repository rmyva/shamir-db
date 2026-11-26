use std::future::{poll_fn, Future};

use std::sync::atomic::Ordering;
use std::sync::Arc;

use bytes::Bytes;

use shamir_tx::{CommitVisibility, IndexWriteOp, IsolationLevel, TxContext};
use shamir_types::types::value::InnerValue;
use tokio::sync::{broadcast::error::TryRecvError, oneshot};
use tokio::task::JoinHandle;

use crate::tx::repo::RepoInstance;
use crate::tx::table::table_manager::table_token_for;
use shamir_storage::error::DbError;

use crate::tx::commit::TxError;
use crate::tx::commit_phases::{
    FAIL_PHASE_5A_TABLE_TOKEN, FAIL_PHASE_5A_TX_ID, FAIL_PHASE_5C_TX_ID,
};
use crate::tx::tests::drain::orphans::post_wal_orphan_ordering_tests::fixture;
use crate::tx::{MaterializationState, TxOutcome};

// Shared by older deferral regressions: observe an actual pending driver poll,
// never inline-await an ack whose orphan replay is deliberately suppressed.
pub(in crate::tx::tests) async fn spawn_deferred_commit(
    repo: &RepoInstance,
    tx: TxContext,
) -> JoinHandle<Result<TxOutcome, TxError>> {
    let gate = repo.tx_gate().await.unwrap();
    let version = gate.version_allocation_high_water_mark() + 1;
    let owned = repo.clone();
    let (sender, receiver) = oneshot::channel();
    let task = tokio::spawn(async move {
        let commit = owned.commit_tx(tx);
        tokio::pin!(commit);
        let mut sender = Some(sender);
        poll_fn(|cx| {
            let result = commit.as_mut().poll(cx);
            if gate.is_orphaned_post_wal(version) || result.is_ready() {
                if let Some(sender) = sender.take() {
                    sender.send(result.is_pending()).unwrap();
                }
            }
            result
        })
        .await
    });
    assert!(receiver.await.unwrap(), "deferred ack must wait for replay");
    task
}

// Red before SNAP-6: unconditional settlement acknowledges a missing row and
// broadcasts an event for it. Poll the actual commit to prove the ack is parked,
// rather than infer progress from a yield or wall-clock delay.
async fn failed_projection_waits_for_replay(visibility: CommitVisibility, index_failure: bool) {
    let (repo, _) = fixture("materialized_means_visible").await;
    let table = repo.get_table("a").await.unwrap();
    let healthy = repo.get_table("b").await.unwrap();
    let gate = repo.tx_gate().await.unwrap();
    let feed = repo.changefeed().await.unwrap();
    let mut live = feed.feed.subscribe();
    let (mut tx, writer_snapshot) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    tx.set_visibility(visibility);
    let value = InnerValue::Str("deferred".into());
    let id = table.insert_tx(&value, Some(&mut tx)).await.unwrap();
    let healthy_id = healthy.insert_tx(&value, Some(&mut tx)).await.unwrap();
    let posting_key = Bytes::from_static(b"snap6-posting");
    let posting_value = Bytes::from_static(b"snap6-visible-posting");
    if index_failure {
        tx.index_write_set.push((
            table_token_for("a"),
            IndexWriteOp::SetPosting {
                key: posting_key.clone(),
                value: posting_value.clone(),
                provenance: Default::default(),
            },
        ));
        FAIL_PHASE_5C_TX_ID.store(tx.tx_id.0, Ordering::SeqCst);
    } else {
        FAIL_PHASE_5A_TABLE_TOKEN.store(table_token_for("a"), Ordering::SeqCst);
        FAIL_PHASE_5A_TX_ID.store(tx.tx_id.0, Ordering::SeqCst);
    }
    let version = gate.version_allocation_high_water_mark() + 1;
    let owned = repo.clone();
    let observer = Arc::clone(&gate);
    let (sender, receiver) = oneshot::channel();
    let task = tokio::spawn(async move {
        let commit = owned.commit_tx(tx);
        tokio::pin!(commit);
        let mut sender = Some(sender);
        poll_fn(|cx| {
            let result = commit.as_mut().poll(cx);
            if observer.is_orphaned_post_wal(version) || result.is_ready() {
                if let Some(sender) = sender.take() {
                    sender.send(result.is_pending()).unwrap();
                }
            }
            result
        })
        .await
    });
    let pending = receiver.await.unwrap();
    FAIL_PHASE_5A_TX_ID.store(0, Ordering::SeqCst);
    FAIL_PHASE_5A_TABLE_TOKEN.store(0, Ordering::SeqCst);
    FAIL_PHASE_5C_TX_ID.store(0, Ordering::SeqCst);
    assert!(pending, "ack must wait for orphan replay");
    assert!(gate.visible_floor() < version);
    assert!(gate.is_orphaned_post_wal(version));
    assert!(matches!(live.try_recv(), Err(TryRecvError::Empty)));
    let (old, old_guard) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    assert!(old.snapshot_version < version);
    assert!(matches!(
        table.read_one_tx(id, Some(&old)).await,
        Err(DbError::NotFound(_))
    ));
    assert!(matches!(
        healthy.read_one_tx(healthy_id, Some(&old)).await,
        Err(DbError::NotFound(_))
    ));
    assert_eq!(healthy.get(healthy_id).await.unwrap(), value);
    if index_failure {
        assert!(matches!(
            table.info_store().get(posting_key.clone().into()).await,
            Err(DbError::NotFound(_))
        ));
        assert_eq!(table.get(id).await.unwrap(), value);
    }
    let replayed_entry = repo.drainer().window_entry(version).unwrap();
    assert_eq!(repo.drainer().drain_all(&repo.core).await.unwrap(), 1);
    let outcome = task.await.unwrap().unwrap();
    assert_eq!(outcome.commit_version, version);
    assert_eq!(outcome.materialization, MaterializationState::Deferred);
    if let Some(background) = outcome.background {
        assert_eq!(background.join().await, MaterializationState::Deferred);
    }
    assert!(gate.visible_floor() >= version);
    assert!(!gate.is_orphaned_post_wal(version));
    if index_failure {
        assert_eq!(
            table.info_store().get(posting_key.into()).await.unwrap(),
            posting_value,
        );
    }
    assert!(matches!(
        table.read_one_tx(id, Some(&old)).await,
        Err(DbError::NotFound(_))
    ));
    let (new, new_guard) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    assert_eq!(table.read_one_tx(id, Some(&new)).await.unwrap(), value);
    assert_eq!(
        healthy.read_one_tx(healthy_id, Some(&new)).await.unwrap(),
        value,
    );
    assert!(matches!(
        healthy.read_one_tx(healthy_id, Some(&old)).await,
        Err(DbError::NotFound(_))
    ));
    assert!(matches!(live.try_recv(), Err(TryRecvError::Empty)));
    // A derived WAL event repairs the orphan while preserving visibility guarantees.
    let (events, gap) = repo
        .read_changelog_for_replication(version, 1)
        .await
        .unwrap();
    assert_eq!(gap, None);
    assert_eq!(events.len(), 1);
    let entry = repo.drainer().window_entry(version);
    assert!(entry.is_none());
    assert_eq!(events[0].event.commit_version, version);
    assert_eq!(
        events[0].event,
        shamir_tx::event_from_wal_entry(&replayed_entry).unwrap()
    );
    drop((writer_snapshot, old_guard, new_guard));
}

#[tokio::test]
async fn sync_failed_data_stalls_visibility_and_preserves_old_snapshot() {
    failed_projection_waits_for_replay(CommitVisibility::Synchronous, false).await;
}

#[tokio::test]
async fn async_index_failed_data_stalls_visibility_and_preserves_old_snapshot() {
    failed_projection_waits_for_replay(CommitVisibility::AsyncIndex, false).await;
}

// Red before SNAP-6: failed postings still let W pass the data version.
#[tokio::test]
async fn sync_failed_index_stalls_visibility_until_posting_replay() {
    failed_projection_waits_for_replay(CommitVisibility::Synchronous, true).await;
}

#[tokio::test]
async fn successful_data_still_settles_and_emits_visible_event() {
    let (repo, _) = fixture("materialized_success").await;
    let table = repo.get_table("a").await.unwrap();
    let gate = repo.tx_gate().await.unwrap();
    let mut live = repo.subscribe_changelog().await.unwrap();
    let (mut tx, guard) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let value = InnerValue::Str("visible".into());
    let id = table.insert_tx(&value, Some(&mut tx)).await.unwrap();
    let outcome = repo.commit_tx(tx).await.unwrap();
    assert_eq!(outcome.materialization, MaterializationState::Complete);
    assert_eq!(gate.visible_floor(), outcome.commit_version);
    assert!(!gate.is_orphaned_post_wal(outcome.commit_version));
    assert_eq!(
        live.try_recv().unwrap().commit_version,
        outcome.commit_version,
    );
    let (read, read_guard) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    assert_eq!(table.read_one_tx(id, Some(&read)).await.unwrap(), value);
    drop((guard, read_guard));
}

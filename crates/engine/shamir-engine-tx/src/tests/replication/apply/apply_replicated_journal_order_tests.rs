//! S5b-4 re-anchored on the receipt pipeline (PR6b-3): the receipt ordering
//! is journal verify/confirm → publish → checkpoint → broadcast, so a batch
//! parked at `Phase::Checkpointed` has its events DURABLE and
//! confirmed in the follower journal while the live broadcast tail is still
//! parked. A chunk that fails at `Phase::Materialized` emits neither
//! journal event nor gap.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use shamir_storage::types::Store;
use shamir_storage::{
    error::DbError,
    storage_in_memory::{InMemoryRepo, InMemoryStore},
};
use shamir_tx::{ChangelogEvent, IsolationLevel, JournalRead, TxContext, TxId};
use shamir_types::access::Actor;
use shamir_types::types::record_id::RecordId;
use shamir_types::types::value::InnerValue;
use tokio::sync::broadcast::error::TryRecvError;

use crate::repo::repo_instance::ChangefeedHandle;
use crate::repo::{BoxRepo, RepoInstance};
use crate::table::table_manager::table_token_for;
use crate::table::TableConfig;
use crate::tx::apply_replicated::follower_durability::test_seam::{self, Phase};
use crate::tx::{apply_replicated_batch, ApplyOutcome, ApplyPolicy, ReplicatedEvent};

fn follower_repo() -> RepoInstance {
    RepoInstance::new(
        "follower".into(),
        BoxRepo::InMemory(Arc::new(InMemoryRepo::new())),
        vec![TableConfig::new("items")],
    )
}

fn put_event(value: &str, n: u8, leader_version: u64) -> ChangelogEvent {
    let mut bytes = [0u8; 16];
    bytes[15] = n;
    let record = RecordId(bytes);
    let mut tx = TxContext::new(TxId::new(leader_version), 0, 0, IsolationLevel::Snapshot);
    tx.set_actor(Actor::User(42));
    let store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    tx.ensure_table_staging(table_token_for("items"), "items", store)
        .set(
            record.to_bytes().into(),
            InnerValue::Str(value.into()).to_bytes().unwrap(),
        );
    shamir_tx::project_event(&tx, "leader", leader_version).unwrap()
}

fn events(pair: &[ChangelogEvent]) -> Vec<ReplicatedEvent> {
    pair.iter()
        .map(|event| ReplicatedEvent {
            event: event.clone(),
            field_bindings: Vec::new(),
        })
        .collect()
}

fn arm_park(phase: Phase) -> Arc<test_seam::Hook> {
    let hook = Arc::new(test_seam::Hook::default());
    *hook.park_at.lock().unwrap() = Some(phase);
    test_seam::install("follower", Arc::clone(&hook));
    hook
}

// S5b-2 is absent on this base: use the real completion/read primitives.
async fn bounded(follower: &RepoInstance, handle: &ChangefeedHandle, from: u64) -> JournalRead {
    let w = follower.tx_gate().await.unwrap().completion().watermark();
    let bound = handle
        .feed
        .confirm_through(w, Duration::from_secs(10))
        .await;
    assert_eq!(
        bound, w,
        "journal writer must confirm the completion prefix"
    );
    handle
        .feed
        .read_from_bounded(&handle.store, from, 17, bound)
        .await
}

fn downstream(event: &ChangelogEvent, local_version: u64) -> ChangelogEvent {
    let mut projected = event.clone();
    projected.repo = "follower".into();
    projected.commit_version = local_version;
    projected
}

/// The receipt ordering guarantees: while the pipeline is parked at
/// `Phase::Checkpointed` (publication done, broadcast tail not yet run),
/// the follower journal already contains BOTH batch events under their
/// follower-local versions, confirmed through the completion watermark —
/// and the live broadcast has delivered nothing yet.
#[tokio::test]
async fn follower_journal_contains_a_and_b_while_a_broadcast_is_parked() {
    let follower = follower_repo();
    let gate = follower.tx_gate().await.unwrap();
    let handle = follower.changefeed().await.unwrap();
    let mut live = handle.feed.subscribe();
    let hook = arm_park(Phase::Checkpointed);
    let event_a = put_event("A", 1, 7);
    let event_b = put_event("B", 2, 9);
    // Bind the events so the pinned future's borrows and the assertions
    // below share one owner that outlives the whole apply.
    let replicated = events(&[event_a.clone(), event_b.clone()]);
    let events = [event_a, event_b];

    let apply = apply_replicated_batch(&follower.core, &replicated, 0, ApplyPolicy::Trusted);
    tokio::pin!(apply);
    let parked = tokio::select! {
        biased;
        _ = hook.reached.notified() => true,
        result = &mut apply => {
            panic!("apply completed without the checkpoint park: {result:?}")
        },
    };
    assert!(parked, "apply completed without the checkpoint park");

    // Parked AFTER publication/checkpoint, BEFORE broadcast: the completion
    // watermark advanced through both local versions and the journal is
    // confirmed through it; nothing is on the live broadcast yet.
    assert_eq!(gate.completion().watermark(), 2);
    let read = bounded(&follower, &handle, 1).await;
    assert_eq!(
        read.events,
        vec![downstream(&events[0], 1), downstream(&events[1], 2)]
    );
    assert_eq!(read.gap_at, None);
    assert!(matches!(live.try_recv(), Err(TryRecvError::Empty)));

    hook.resume.notify_one();
    let outcome = (&mut apply).await.unwrap();
    assert_eq!(outcome, ApplyOutcome::Applied { local_version: 2 });
    assert_eq!(*live.try_recv().unwrap(), downstream(&events[0], 1));
    assert_eq!(*live.try_recv().unwrap(), downstream(&events[1], 2));
    assert!(matches!(live.try_recv(), Err(TryRecvError::Empty)));
    test_seam::remove("follower");
}

/// A chunk that fails at `Phase::Materialized` (sticky injected failure —
/// the receipt analogue of the retired executor's history-write failure)
/// publishes NOTHING: no journal event, no gap, no broadcast, no completion
/// watermark movement.
#[tokio::test]
async fn follower_materialized_failure_emits_no_journal_event_or_gap() {
    let follower = follower_repo();
    let gate = follower.tx_gate().await.unwrap();
    let handle = follower.changefeed().await.unwrap();
    let mut live = handle.feed.subscribe();

    let hook = Arc::new(test_seam::Hook::default());
    *hook.fail_after.lock().unwrap() = Some(Phase::Materialized);
    hook.fail_after_sticky.store(true, Ordering::Release);
    test_seam::install("follower", Arc::clone(&hook));

    let event = put_event("history-fails", 3, 7);
    let error = apply_replicated_batch(&follower.core, &events(&[event]), 0, ApplyPolicy::Trusted)
        .await
        .expect_err("the injected materialization failure must surface");
    assert!(matches!(&error, DbError::Storage(_)), "{error:?}");
    test_seam::remove("follower");

    assert_eq!(gate.completion().watermark(), 0);
    let read = bounded(&follower, &handle, 1).await;
    assert!(read.events.is_empty());
    assert_eq!(read.gap_at, None);
    assert_eq!(handle.feed.first_gap_version(), 0);
    assert!(matches!(live.try_recv(), Err(TryRecvError::Empty)));
}

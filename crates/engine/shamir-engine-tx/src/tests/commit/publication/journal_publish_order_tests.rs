use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use shamir_storage::storage_in_memory::InMemoryRepo;
use shamir_storage::types::Repo;
use shamir_tx::{ChangelogEvent, CommitVisibility, IsolationLevel, JournalRead};
use shamir_types::access::Actor;
use shamir_types::types::value::InnerValue;
use tokio::sync::{Barrier, Notify};

use crate::repo::instance::repo_instance::ChangefeedHandle;
use crate::repo::repo_types::BoxRepo;
use crate::repo::RepoInstance;
use crate::table::TableConfig;
use crate::tx::publish_journaled::publish_journaled;
use crate::tx::test_seams::JournalPublishHook;

fn fixture() -> RepoInstance {
    RepoInstance::new(
        "journal_publish".into(),
        BoxRepo::InMemory(Arc::new(InMemoryRepo::new())),
        Vec::new(),
    )
}

// Deadlines are deadlock guards, never scheduling or performance oracles.
async fn within<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(45), future)
        .await
        .expect("journal publish test stalled")
}

async fn bounded(repo: &RepoInstance, handle: &ChangefeedHandle, from: u64) -> JournalRead {
    let w = repo.tx_gate().await.unwrap().completion().watermark();
    let bound = handle
        .feed
        .confirm_through(w, Duration::from_secs(10))
        .await;
    assert_eq!(
        bound, w,
        "writer must confirm the sampled completion prefix"
    );
    handle
        .feed
        .read_from_bounded(&handle.store, from, 17, bound)
        .await
}

async fn park_before_broadcast(visibility: CommitVisibility) {
    let repo = fixture();
    repo.add_table_for_test(TableConfig::new("a"));
    repo.add_table_for_test(TableConfig::new("b"));
    let a = repo.get_table("a").await.unwrap();
    let b = repo.get_table("b").await.unwrap();
    let handle = repo.changefeed().await.unwrap();
    let mut live = handle.feed.subscribe();
    let hook = Arc::new(JournalPublishHook::default());
    hook.broadcast_armed.store(true, Ordering::Release);
    repo.core.journal_publish_hook.store(Some(hook.clone()));
    let (mut tx_a, pin_a) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    tx_a.set_visibility(visibility);
    let id_a = tx_a.tx_id.0;
    a.insert_tx(&InnerValue::Str("A".into()), Some(&mut tx_a))
        .await
        .unwrap();
    let owned = repo.clone();
    let task_a = tokio::spawn(async move { owned.commit_tx(tx_a).await });
    within(hook.reached.notified()).await;
    // Broadcast hook follows guard.commit(): W already covers A, so B
    // cannot wait on A here (unlike the pre-publish journal-order fixture).
    let v_a = repo.tx_gate().await.unwrap().completion().watermark();
    let (mut tx_b, pin_b) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    tx_b.set_visibility(visibility);
    let id_b = tx_b.tx_id.0;
    b.insert_tx(&InnerValue::Str("B".into()), Some(&mut tx_b))
        .await
        .unwrap();
    let mut outcome_b = within(repo.commit_tx(tx_b)).await.unwrap();
    let read = within(bounded(&repo, &handle, v_a)).await;
    // Red before S5b-3: A is published but absent from the channel, so the
    // barrier confirms B alone and a follower would bookmark past A forever.
    let observed: Vec<_> = read
        .events
        .iter()
        .map(|ev| (ev.commit_version, ev.tx_id))
        .collect();
    let live_b = live.try_recv().unwrap();
    assert_eq!(live_b.tx_id, id_b);
    assert!(
        live.try_recv().is_err(),
        "A must not broadcast while parked"
    );
    hook.resume.notify_one();
    let mut outcome_a = within(task_a).await.unwrap().unwrap();
    if let Some(tail) = outcome_a.take_background() {
        within(tail.join()).await;
    }
    if let Some(tail) = outcome_b.take_background() {
        within(tail.join()).await;
    }
    assert_eq!(
        observed,
        vec![(v_a, id_a), (outcome_b.commit_version, id_b)]
    );
    assert_eq!(read.gap_at, None);
    assert_eq!(live.try_recv().unwrap().tx_id, id_a);
    drop((pin_a, pin_b));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn journal_lockfree_enqueued_while_broadcast_parked() {
    park_before_broadcast(CommitVisibility::Synchronous).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn journal_async_index_enqueued_while_broadcast_parked() {
    park_before_broadcast(CommitVisibility::AsyncIndex).await;
}

fn event(version: u64) -> ChangelogEvent {
    ChangelogEvent {
        repo: "journal_publish".into(),
        commit_version: version,
        tx_id: version,
        actor: Actor::System,
        timestamp_ns: 0,
        changes: Vec::new(),
    }
}

#[tokio::test]
async fn journal_init_failure_retry_keeps_pending_minimum_at_publish() {
    let repo = fixture();
    let gate = repo.tx_gate().await.unwrap();
    let hook = Arc::new(JournalPublishHook::default());
    repo.core.journal_publish_hook.store(Some(hook.clone()));
    hook.init_failures.store(3, Ordering::Release);
    let cancelled = gate.assign_next_version_guarded();
    let prepared = repo
        .prepare_journal_event(Some(event(cancelled.version())))
        .await;
    drop(prepared);
    drop(cancelled);
    let earlier = gate.assign_next_version_guarded();
    let v_earlier = earlier.version();
    let p_earlier = repo.prepare_journal_event(Some(event(v_earlier))).await;
    let later = gate.assign_next_version_guarded();
    let v_later = later.version();
    let p_later = repo.prepare_journal_event(Some(event(v_later))).await;
    assert_eq!(hook.init_failures.load(Ordering::Acquire), 0);
    // Retry succeeds BEFORE either failed preparation publishes. Cancellation
    // above must not poison the new feed with a gap for an aborted version.
    let handle = repo.changefeed().await.unwrap();
    assert_eq!(handle.feed.first_gap_version(), 0);
    let _ = publish_journaled(&repo.core, later, p_later);
    assert_eq!(handle.feed.first_gap_version(), v_later);
    let _ = publish_journaled(&repo.core, earlier, p_earlier);
    assert_eq!(handle.feed.first_gap_version(), v_earlier);
    assert_eq!(
        within(bounded(&repo, &handle, v_earlier)).await.gap_at,
        Some(v_earlier),
    );
}

#[tokio::test]
async fn journal_pending_minimum_restored_and_persisted_on_retry() {
    let backing = Arc::new(InMemoryRepo::new());
    let repo = RepoInstance::new(
        "journal_publish".into(),
        BoxRepo::InMemory(backing.clone()),
        Vec::new(),
    );
    let gate = repo.tx_gate().await.unwrap();
    let hook = Arc::new(JournalPublishHook::default());
    repo.core.journal_publish_hook.store(Some(hook.clone()));
    hook.init_failures.store(2, Ordering::Release);
    let earlier = gate.assign_next_version_guarded();
    let first = earlier.version();
    let p_first = repo.prepare_journal_event(Some(event(first))).await;
    let later = gate.assign_next_version_guarded();
    let p_later = repo
        .prepare_journal_event(Some(event(later.version())))
        .await;
    let _ = publish_journaled(&repo.core, later, p_later);
    let _ = publish_journaled(&repo.core, earlier, p_first);
    let handle = repo.changefeed().await.unwrap();
    assert_eq!(handle.feed.first_gap_version(), first);
    let read = within(bounded(&repo, &handle, first)).await;
    assert_eq!(read.gap_at, Some(first));
    assert!(read.events.is_empty());
    // Persisted metadata is observable through the real storage adapter; the
    // seeded minimum must not incorrectly be classified as already durable.
    let store = backing.store_get("__changelog__").await.unwrap();
    let marker = store
        .get(shamir_tx::changefeed::gap_marker_key().into())
        .await
        .unwrap();
    assert_eq!(marker.as_ref(), &first.to_be_bytes());
}

#[tokio::test]
async fn journal_successful_preparation_cancelled_without_side_effects() {
    let repo = fixture();
    let gate = repo.tx_gate().await.unwrap();
    let handle = repo.changefeed().await.unwrap();
    let mut live = handle.feed.subscribe();
    let guard = gate.assign_next_version_guarded();
    let v = guard.version();
    let prepared = repo.prepare_journal_event(Some(event(v))).await.unwrap();
    assert!(Arc::ptr_eq(prepared.1.as_ref().unwrap(), &handle.feed));
    // Successful preparation must remain inert when publication is cancelled.
    drop(prepared);
    drop(guard);
    assert_eq!(gate.completion().watermark(), v);
    let read = within(bounded(&repo, &handle, v)).await;
    assert!(read.events.is_empty());
    assert_eq!(read.gap_at, None);
    assert_eq!(handle.feed.first_gap_version(), 0);
    assert!(matches!(
        live.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));
}

#[tokio::test]
async fn journal_publish_returns_same_arc_for_live_broadcast() {
    let repo = fixture();
    let gate = repo.tx_gate().await.unwrap();
    let handle = repo.changefeed().await.unwrap();
    let mut live = handle.feed.subscribe();
    let guard = gate.assign_next_version_guarded();
    let v = guard.version();
    let prepared = repo.prepare_journal_event(Some(event(v))).await.unwrap();
    let identity = prepared.0.clone();
    let published = publish_journaled(&repo.core, guard, Some(prepared)).unwrap();
    assert!(Arc::ptr_eq(&identity, &published));
    assert!(live.try_recv().is_err());
    repo.broadcast_changefeed_event(Some(published));
    assert!(Arc::ptr_eq(&identity, &live.try_recv().unwrap()));
    let read = within(bounded(&repo, &handle, v)).await;
    assert_eq!(read.events, vec![event(v)]);
    assert_eq!(read.gap_at, None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn journal_pending_gap_survives_oncecell_publication_after_snapshot() {
    let repo = fixture();
    let gate = repo.tx_gate().await.unwrap();
    let hook = Arc::new(JournalPublishHook::default());
    repo.core.journal_publish_hook.store(Some(hook.clone()));
    hook.init_failures.store(1, Ordering::Release);
    let guard = gate.assign_next_version_guarded();
    let v = guard.version();
    let prepared = repo.prepare_journal_event(Some(event(v))).await;
    hook.init_snapshot_armed.store(true, Ordering::Release);
    let owned = repo.clone();
    let init = tokio::spawn(async move { owned.changefeed().await });
    within(hook.reached.notified()).await;
    // Init has already sampled pending=0, but OnceCell is still unpublished.
    // A store+get recheck alone misses this schedule; post-init handoff is needed.
    let _ = publish_journaled(&repo.core, guard, prepared);
    hook.resume.notify_one();
    let handle = within(init).await.unwrap().unwrap();
    assert_eq!(handle.feed.first_gap_version(), v);
    assert_eq!(within(bounded(&repo, &handle, v)).await.gap_at, Some(v));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn journal_disjoint_tables_eight_by_fifty_bounded_pulls_lose_no_versions() {
    let repo = fixture();
    let handle = repo.changefeed().await.unwrap();
    // Exactly eight committers and one puller; fifty rounds. Each round has
    // start, finish, and drain barriers: one pull races publication between
    // start and finish, then one final pull drains before the next round starts.
    // Two finite pulls suffice: at most eight events fit under the read max 17.
    let barrier = Arc::new(Barrier::new(9));
    let mut workers = Vec::with_capacity(8);
    for worker in 0..8 {
        let name = format!("t{worker}");
        repo.add_table_for_test(TableConfig::new(name.clone()));
        let table = repo.get_table(&name).await.unwrap();
        let owned = repo.clone();
        let barrier = barrier.clone();
        workers.push(tokio::spawn(async move {
            let mut versions = Vec::with_capacity(50);
            for round in 0..50 {
                barrier.wait().await;
                let (mut tx, pin) = owned.begin_tx(IsolationLevel::Snapshot).await.unwrap();
                table
                    .insert_tx(&InnerValue::Int(round), Some(&mut tx))
                    .await
                    .unwrap();
                versions.push(owned.commit_tx(tx).await.unwrap().commit_version);
                drop(pin);
                barrier.wait().await;
                barrier.wait().await;
            }
            versions
        }));
    }
    let mut pulled = BTreeSet::new();
    let mut bookmark = 0;
    for _ in 0..50 {
        within(barrier.wait()).await;
        let mut round_events = 0;
        for completed in [false, true] {
            if completed {
                within(barrier.wait()).await;
            }
            let read = within(bounded(&repo, &handle, bookmark + 1)).await;
            assert_eq!(read.gap_at, None);
            assert!(read.events.len() <= 8);
            round_events += read.events.len();
            let next_bookmark = read
                .events
                .iter()
                .map(|ev| ev.commit_version)
                .max()
                .unwrap_or(bookmark);
            for ev in read.events {
                assert!(ev.commit_version > bookmark);
                assert!(pulled.insert(ev.commit_version), "duplicate pulled version");
            }
            bookmark = next_bookmark;
        }
        assert_eq!(round_events, 8);
        within(barrier.wait()).await;
    }
    let mut committed = BTreeSet::new();
    for worker in workers {
        committed.extend(within(worker).await.unwrap());
    }
    assert_eq!(committed.len(), 400);
    assert_eq!(pulled, committed);
}

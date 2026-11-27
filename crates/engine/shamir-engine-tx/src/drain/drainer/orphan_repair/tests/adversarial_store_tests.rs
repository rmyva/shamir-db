use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use shamir_storage::storage_in_memory::{InMemoryRepo, InMemoryStore};
use shamir_tx::{ChangelogStore, IsolationLevel, RepoChangefeed, JOURNAL_CHANNEL_CAPACITY};
use shamir_types::access::Actor;
use shamir_types::types::value::InnerValue;
use tokio::sync::Notify;

use crate::repo::changelog_store::StoreChangelog;
use crate::repo::repo_instance::ChangefeedHandle;
use crate::repo::{BoxRepo, RepoInstance};
use crate::table::TableConfig;
use crate::tx::commit::{PostWalPrePublishHook, TEST_POST_WAL_PRE_PUBLISH_HOOK};
use crate::tx::drainer::orphan_repair::repair_event;
use crate::tx::tests::drain::orphans::post_wal_orphan_ordering_tests::panic_post_wal;
use shamir_tx::ChangelogError;

struct CountingStore {
    inner: StoreChangelog,
    event_puts: AtomicUsize,
    fail_horizon_at: AtomicUsize,
    horizon_puts: AtomicUsize,
    block_lookup_once: AtomicBool,
    block_filler_once: AtomicBool,
    lookup_entered: Notify,
    release_lookup: Notify,
    filler_entered: Notify,
    release_filler: Notify,
}

#[async_trait::async_trait]
impl ChangelogStore for CountingStore {
    async fn put(&self, key: Bytes, value: Bytes) -> Result<(), ChangelogError> {
        if key == shamir_tx::changefeed::clean_horizon_key() {
            let ordinal = self.horizon_puts.fetch_add(1, Ordering::AcqRel) + 1;
            if self.fail_horizon_at.load(Ordering::Acquire) == ordinal
                && self.fail_horizon_at.swap(0, Ordering::AcqRel) == ordinal
            {
                return Err(ChangelogError::Journal(
                    "injected horizon checkpoint failure".into(),
                ));
            }
        }
        if key.len() == 8 {
            self.event_puts.fetch_add(1, Ordering::Relaxed);
            if key.as_ref() == u64::MAX.to_be_bytes().as_slice()
                && self.block_filler_once.swap(false, Ordering::AcqRel)
            {
                self.filler_entered.notify_one();
                self.release_filler.notified().await;
            }
        }
        self.inner.put(key, value).await
    }

    async fn range_from(&self, key: Bytes, limit: usize) -> Result<Vec<Bytes>, ChangelogError> {
        if self.block_lookup_once.swap(false, Ordering::AcqRel) {
            self.lookup_entered.notify_one();
            self.release_lookup.notified().await;
        }
        self.inner.range_from(key, limit).await
    }
}

fn counted_store() -> Arc<CountingStore> {
    Arc::new(CountingStore {
        inner: StoreChangelog::new(Arc::new(InMemoryStore::new())),
        event_puts: AtomicUsize::new(0),
        fail_horizon_at: AtomicUsize::new(0),
        horizon_puts: AtomicUsize::new(0),
        block_lookup_once: AtomicBool::new(false),
        block_filler_once: AtomicBool::new(false),
        lookup_entered: Notify::new(),
        release_lookup: Notify::new(),
        filler_entered: Notify::new(),
        release_filler: Notify::new(),
    })
}

async fn install_feed(repo: &crate::repo::RepoInstance, store: &Arc<CountingStore>) {
    let journal: Arc<dyn ChangelogStore> = Arc::clone(store) as Arc<dyn ChangelogStore>;
    repo.install_journal_for_test(ChangefeedHandle {
        feed: RepoChangefeed::new(Arc::clone(&journal)),
        store: journal,
    });
}

async fn counted_fixture(
    name: &str,
) -> (
    crate::repo::RepoInstance,
    Arc<PostWalPrePublishHook>,
    Arc<CountingStore>,
) {
    let hook = Arc::new(PostWalPrePublishHook {
        repo_name: name.into(),
        pre_wal: AtomicBool::new(false),
        after_counter: AtomicBool::new(false),
        fail_orphan_markers: AtomicBool::new(false),
        panic_after_resume: AtomicBool::new(false),
        panic_resume: Notify::new(),
        reached: Notify::new(),
        resume: Notify::new(),
        armed: AtomicBool::new(false),
    });
    TEST_POST_WAL_PRE_PUBLISH_HOOK
        .set(Arc::clone(&hook))
        .ok()
        .unwrap();
    let repo = RepoInstance::new(
        name.into(),
        BoxRepo::InMemory(Arc::new(InMemoryRepo::new())),
        vec![TableConfig::new("a"), TableConfig::new("b")],
    );
    let store = counted_store();
    install_feed(&repo, &store).await;
    (repo, hook, store)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repeated_repair_event_put_is_idempotent() {
    let (repo, hook, store) = counted_fixture("orphan_counted_put").await;
    let table = repo.get_table("a").await.unwrap();
    let (mut tx, snapshot) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    table
        .insert_tx(&InnerValue::Str("counted".into()), Some(&mut tx))
        .await
        .unwrap();
    let version = panic_post_wal(&repo, &hook, tx).await;
    drop(snapshot);
    let entry = repo.drainer().window_entry(version).unwrap();
    let expected = shamir_tx::event_from_wal_entry(&entry).unwrap();
    repair_event(&repo.core, &entry).await.unwrap();
    repair_event(&repo.core, &entry).await.unwrap();
    assert_eq!(store.event_puts.load(Ordering::Relaxed), 1);
    assert_eq!(repo.drainer().drain_all(&repo.core).await.unwrap(), 1);
    assert_eq!(store.event_puts.load(Ordering::Relaxed), 1);
    let (events, gap) = repo
        .read_changelog_for_replication(version, 1)
        .await
        .unwrap();
    assert_eq!(gap, None);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event, expected);
    repo.changefeed()
        .await
        .unwrap()
        .feed
        .shutdown_and_wait()
        .await
        .unwrap();
}

async fn retry_case(name: &str, ordinal: usize) {
    let (repo, hook, store) = counted_fixture(name).await;
    let table = repo.get_table("a").await.unwrap();
    let (mut tx, snapshot) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    table
        .insert_tx(&InnerValue::Str("retry".into()), Some(&mut tx))
        .await
        .unwrap();
    let version = panic_post_wal(&repo, &hook, tx).await;
    drop(snapshot);
    // Repair writes the preflight horizon first and the event checkpoint second.
    store.fail_horizon_at.store(ordinal, Ordering::Release);
    assert_eq!(repo.drainer().drain_all(&repo.core).await.unwrap(), 0);
    let gate = repo.tx_gate().await.unwrap();
    assert!(gate.is_orphaned_post_wal(version));
    assert_eq!(
        repo.drainer()
            .orphan_replay_failures_total
            .load(Ordering::Relaxed),
        1
    );
    assert_eq!(repo.drainer().orphan_repair_metrics.snapshot().1, 0);
    assert_eq!(repo.changefeed().await.unwrap().feed.first_gap_version(), 0);
    assert_eq!(repo.drainer().drain_all(&repo.core).await.unwrap(), 1);
    assert_eq!(store.event_puts.load(Ordering::Relaxed), 1);
    assert!(!gate.is_orphaned_post_wal(version));
    let (events, gap) = repo
        .read_changelog_for_replication(version, 1)
        .await
        .unwrap();
    assert_eq!(gap, None);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event.commit_version, version);
    assert_eq!(store.event_puts.load(Ordering::Relaxed), 1);
    repo.changefeed()
        .await
        .unwrap()
        .feed
        .shutdown_and_wait()
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn preflight_checkpoint_failure_is_retryable_without_gap() {
    retry_case("orphan_preflight_retry", 1).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn post_enqueue_checkpoint_failure_is_retryable_without_gap() {
    retry_case("orphan_checkpoint_retry", 2).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn full_journal_channel_defers_orphan_event() {
    let (repo, hook, store) = counted_fixture("orphan_full_channel").await;
    let table = repo.get_table("a").await.unwrap();
    let (mut tx, snapshot) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    table
        .insert_tx(&InnerValue::Str("full".into()), Some(&mut tx))
        .await
        .unwrap();
    let version = panic_post_wal(&repo, &hook, tx).await;
    drop(snapshot);

    store.block_lookup_once.store(true, Ordering::Release);
    let owned_repo = repo.clone();
    let drain = tokio::spawn(async move { owned_repo.drainer().drain_all(&owned_repo.core).await });
    store.lookup_entered.notified().await;

    let feed = repo.changefeed().await.unwrap().feed;
    store.block_filler_once.store(true, Ordering::Release);
    let filler = Arc::new(shamir_tx::ChangelogEvent {
        repo: "filler".into(),
        commit_version: u64::MAX,
        tx_id: 0,
        actor: Actor::System,
        timestamp_ns: 0,
        changes: vec![],
    });
    feed.journal_enqueue(&filler);
    store.filler_entered.notified().await;
    let queued = Arc::new(shamir_tx::ChangelogEvent {
        repo: "filler".into(),
        commit_version: u64::MAX - 1,
        tx_id: 0,
        actor: Actor::System,
        timestamp_ns: 0,
        changes: vec![],
    });
    for _ in 0..JOURNAL_CHANNEL_CAPACITY {
        feed.journal_enqueue(&queued);
    }

    store.release_lookup.notify_one();
    // PR5b: a channel-full enqueue no longer declares a gap — the orphan's
    // event was deferred into the repair-pending set and the orphan itself
    // stays unresolved for a later pass.
    assert_eq!(drain.await.unwrap().unwrap(), 0);
    assert_eq!(feed.first_gap_version(), 0);
    assert_eq!(feed.journal_dropped(), 1);
    assert_eq!(feed.repair_pending_depth(), 1);

    // Release the writer; once it has persisted the parked filler and the
    // queued backlog the channel has room again.
    store.release_filler.notify_one();
    for _ in 0..200_000 {
        if feed.last_persisted_version() >= u64::MAX - 1 {
            break;
        }
        tokio::task::yield_now().await;
    }
    // Retry: the repair-pending drain admits the deferred event and the
    // orphan settles from the now-present journal event — no gap, depth 0.
    assert_eq!(repo.drainer().drain_all(&repo.core).await.unwrap(), 1);
    assert_eq!(feed.first_gap_version(), 0);
    assert_eq!(feed.repair_pending_depth(), 0);
    feed.shutdown_and_wait().await.unwrap();
    let journal: Arc<dyn ChangelogStore> = Arc::clone(&store) as Arc<dyn ChangelogStore>;
    let read = feed.read_from(&journal, version, 10).await;
    assert_eq!(read.gap_at, None);
    assert!(read
        .events
        .iter()
        .any(|event| event.commit_version == version));
}

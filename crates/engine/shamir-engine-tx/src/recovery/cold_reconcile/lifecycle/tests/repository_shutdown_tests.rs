use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::{Stream, StreamExt};
use shamir_query_builder::write::doc;
use shamir_storage::error::{DbError, DbResult};
use shamir_storage::storage_in_memory::InMemoryRepo;
use shamir_storage::types::{RecordKey, Repo, Store};
use shamir_tx::changefeed::{clean_horizon_key, decode_clean_horizon};
use shamir_tx::{ChangelogStore, RepoChangefeed};
use shamir_types::codecs::interned::query_value_to_inner;
use tokio::sync::Notify;

use crate::repo::repo_instance::ChangefeedHandle;
use crate::repo::{BoxRepo, RepoInstance};
use crate::table::interner_manager::InternerManager;
use crate::table::TableConfig;
use crate::tx::IsolationLevel;
use shamir_tx::ChangelogError;

type StoreStream = Pin<Box<dyn Stream<Item = Result<Vec<(RecordKey, Bytes)>, DbError>> + Send>>;

struct TrackingStore {
    inner: Arc<dyn Store>,
    flushes: AtomicUsize,
    entered: Option<Arc<Notify>>,
    release: Option<Arc<Notify>>,
}

#[async_trait]
impl Store for TrackingStore {
    async fn insert(&self, value: Bytes) -> DbResult<RecordKey> {
        self.inner.insert(value).await
    }
    async fn set(&self, key: RecordKey, value: Bytes) -> DbResult<bool> {
        self.inner.set(key, value).await
    }
    async fn get(&self, key: RecordKey) -> DbResult<Bytes> {
        self.inner.get(key).await
    }
    async fn remove(&self, key: RecordKey) -> DbResult<bool> {
        self.inner.remove(key).await
    }
    async fn flush(&self) -> DbResult<()> {
        if let Some(entered) = &self.entered {
            entered.notify_one();
            self.release.as_ref().unwrap().notified().await;
        }
        self.inner.flush().await?;
        self.flushes.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    fn iter_stream(&self, n: usize) -> StoreStream {
        self.inner.iter_stream(n)
    }
    fn scan_prefix_stream(&self, p: Bytes, n: usize) -> StoreStream {
        self.inner.scan_prefix_stream(p, n)
    }
    fn iter_range_stream(&self, a: Option<Bytes>, b: Option<Bytes>, n: usize) -> StoreStream {
        self.inner.iter_range_stream(a, b, n)
    }
    fn iter_range_stream_reverse(
        &self,
        a: Option<Bytes>,
        b: Option<Bytes>,
        n: usize,
    ) -> StoreStream {
        self.inner.iter_range_stream_reverse(a, b, n)
    }
}

#[tokio::test(start_paused = true)]
async fn held_commit_tail_makes_repository_shutdown_report_quiescence_timeout() {
    let repo = RepoInstance::new(
        "repository-quiescence-timeout-test".into(),
        BoxRepo::InMemory(Arc::new(InMemoryRepo::new())),
        vec![TableConfig::new("items")],
    );
    let _tail = repo.commit_tail().enter();
    let err = repo.shutdown_clean(Duration::ZERO).await.unwrap_err();
    assert!(err.to_string().contains("commit quiescence"));
    assert!(repo.worker_reopen_required());
}

#[tokio::test(start_paused = true)]
async fn shutdown_persists_new_interner_names_and_flushes_store() {
    let backing = Arc::new(InMemoryRepo::new());
    let repo = RepoInstance::new(
        "shutdown-interner-flush-test".into(),
        BoxRepo::InMemory(Arc::clone(&backing)),
        vec![TableConfig::new("items")],
    );
    let store = Arc::new(TrackingStore {
        inner: backing.store_get("__interner__").await.unwrap(),
        flushes: AtomicUsize::new(0),
        entered: None,
        release: None,
    });
    let manager = InternerManager::new(store.clone());
    repo.install_repo_interner_for_test(manager.clone());
    manager
        .get()
        .await
        .unwrap()
        .touch_ind("shutdown_only")
        .unwrap();
    assert_eq!(
        store.flushes.load(Ordering::SeqCst),
        0,
        "setup has not flushed the tracked store"
    );
    repo.shutdown_clean(Duration::from_secs(30)).await.unwrap();
    assert!(
        store.flushes.load(Ordering::SeqCst) > 0,
        "shutdown invokes backend flush"
    );
    assert!(
        manager.persisted_high_water() > 0,
        "shutdown persists the touched interner name"
    );
    let persisted_chunks = store
        .inner
        .scan_prefix_stream(Bytes::from_static(b"\0\0\0\0i.d"), 32);
    tokio::pin!(persisted_chunks);
    let mut found_chunk = false;
    while let Some(batch) = persisted_chunks.next().await {
        if !batch.unwrap().is_empty() {
            found_chunk = true;
            break;
        }
    }
    assert!(
        found_chunk,
        "interner chunk is present in the backing store after flush"
    );
    repo.changefeed()
        .await
        .unwrap()
        .feed
        .shutdown_and_wait()
        .await
        .unwrap();
}

#[tokio::test(start_paused = true)]
async fn shutdown_flush_timeout_quarantines_foreground_repo() {
    let backing = Arc::new(InMemoryRepo::new());
    let repo = RepoInstance::new(
        "shutdown-flush-timeout-test".into(),
        BoxRepo::InMemory(Arc::clone(&backing)),
        vec![TableConfig::new("items")],
    );
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let store = Arc::new(TrackingStore {
        inner: backing.store_get("__interner__").await.unwrap(),
        flushes: AtomicUsize::new(0),
        entered: Some(entered.clone()),
        release: Some(release.clone()),
    });
    let manager = InternerManager::new(store);
    repo.install_repo_interner_for_test(manager.clone());
    manager
        .get()
        .await
        .unwrap()
        .touch_ind("parked_flush")
        .unwrap();
    let shutdown = tokio::spawn({
        let repo = repo.clone();
        async move { repo.shutdown_clean(Duration::from_secs(1)).await }
    });
    entered.notified().await;
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(shutdown
        .await
        .unwrap()
        .unwrap_err()
        .to_string()
        .contains("timed out"));
    assert!(
        repo.worker_reopen_required(),
        "flush timeout quarantines the lifecycle"
    );
    release.notify_one();
    repo.changefeed()
        .await
        .unwrap()
        .feed
        .shutdown_and_wait()
        .await
        .unwrap();
}

#[tokio::test(start_paused = true)]
async fn shutdown_checkpoint_timeout_quarantines_foreground_repo() {
    struct ParkJournal {
        entered: Notify,
        release: Notify,
    }
    #[async_trait]
    impl ChangelogStore for ParkJournal {
        async fn put(&self, _key: Bytes, _value: Bytes) -> Result<(), ChangelogError> {
            self.entered.notify_one();
            self.release.notified().await;
            Ok(())
        }
        async fn range_from(
            &self,
            _key: Bytes,
            _limit: usize,
        ) -> Result<Vec<Bytes>, ChangelogError> {
            Ok(Vec::new())
        }
    }
    let repo = RepoInstance::new(
        "shutdown-checkpoint-timeout-test".into(),
        BoxRepo::InMemory(Arc::new(InMemoryRepo::new())),
        vec![TableConfig::new("items")],
    );
    let journal = Arc::new(ParkJournal {
        entered: Notify::new(),
        release: Notify::new(),
    });
    let store: Arc<dyn ChangelogStore> = journal.clone();
    repo.install_journal_for_test(ChangefeedHandle {
        feed: RepoChangefeed::new(store.clone()),
        store,
    });
    let shutdown = tokio::spawn({
        let repo = repo.clone();
        async move { repo.shutdown_clean(Duration::from_secs(1)).await }
    });
    journal.entered.notified().await;
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(shutdown
        .await
        .unwrap()
        .unwrap_err()
        .to_string()
        .contains("timed out"));
    assert!(repo.worker_reopen_required());
    journal.release.notify_one();
    repo.changefeed()
        .await
        .unwrap()
        .feed
        .shutdown_and_wait()
        .await
        .unwrap();
}

#[tokio::test(start_paused = true)]
async fn never_installed_worker_flushes_and_checkpoints_committed_horizon() {
    let backing = Arc::new(InMemoryRepo::new());
    let repo = RepoInstance::new(
        "repository-shutdown-test".into(),
        BoxRepo::InMemory(Arc::clone(&backing)),
        vec![TableConfig::new("items")],
    );
    let table = repo.get_table("items").await.unwrap();
    let interner = table.interner().get().await.unwrap();
    let value =
        query_value_to_inner(&doc().set("name", "value").unwrap().build(), interner).unwrap();
    let (mut tx, snapshot) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    table.insert_tx(&value, Some(&mut tx)).await.unwrap();
    repo.commit_tx(tx).await.unwrap();
    drop(snapshot);
    let horizon = repo
        .tx_gate()
        .await
        .unwrap()
        .version_allocation_high_water_mark();
    assert!(horizon > 0);
    repo.shutdown_clean(Duration::from_secs(30)).await.unwrap();
    let store = backing.store_get("__changelog__").await.unwrap();
    let encoded = store.get(clean_horizon_key().into()).await.unwrap();
    assert_eq!(decode_clean_horizon(&encoded).unwrap(), horizon);
    assert!(!repo.worker_reopen_required());
    repo.changefeed()
        .await
        .unwrap()
        .feed
        .shutdown_and_wait()
        .await
        .unwrap();
}

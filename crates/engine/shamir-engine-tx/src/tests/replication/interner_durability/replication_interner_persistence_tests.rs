//! #49: retry must persist names left in memory by a failed first apply.

use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use crate::repo::{BoxRepoFactory, RepoFactory, RepoInstance};
use crate::table::interner_manager::InternerManager;
use crate::table::TableConfig;
use crate::tx::{apply_replicated_batch, ApplyOutcome, ApplyPolicy, ReplicatedEvent};
use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use shamir_collections::TMap;
use shamir_storage::error::{DbError, DbResult};
use shamir_storage::types::{RecordKey, Repo, Store};
use shamir_tx::{ChangeOp, ChangelogEvent, RecordChange};
use shamir_types::access::Actor;
use shamir_types::codecs::interned::inner_value_to_query_value;
use shamir_types::core::interner::InternerKey;
use shamir_types::types::record_id::RecordId;
use shamir_types::types::value::{InnerValue, QueryValue};

/// Apply ONE event through the public batch entry (the receipt pipeline).
async fn apply_batch(
    repo: &RepoInstance,
    event: &ChangelogEvent,
    field_bindings: &[(u64, String)],
    bookmark: u64,
    policy: ApplyPolicy,
) -> shamir_storage::error::DbResult<ApplyOutcome> {
    apply_replicated_batch(
        &repo.core,
        &[ReplicatedEvent {
            event: event.clone(),
            field_bindings: field_bindings.to_vec(),
        }],
        bookmark,
        policy,
    )
    .await
}

type TestStream = Pin<Box<dyn Stream<Item = Result<Vec<(RecordKey, Bytes)>, DbError>> + Send>>;

struct FailingSetStore {
    inner: Arc<dyn Store>,
    fail_next: AtomicBool,
    failures: AtomicUsize,
    writes: AtomicUsize,
    flushes: AtomicUsize,
}

#[async_trait]
impl Store for FailingSetStore {
    async fn insert(&self, value: Bytes) -> DbResult<RecordKey> {
        self.inner.insert(value).await
    }

    async fn set(&self, key: RecordKey, value: Bytes) -> DbResult<bool> {
        if self.fail_next.swap(false, Ordering::SeqCst) {
            self.failures.fetch_add(1, Ordering::SeqCst);
            return Err(DbError::Storage(
                "#49: injected interner set failure".into(),
            ));
        }
        let created = self.inner.set(key, value).await?;
        self.writes.fetch_add(1, Ordering::SeqCst);
        Ok(created)
    }

    async fn get(&self, key: RecordKey) -> DbResult<Bytes> {
        self.inner.get(key).await
    }

    async fn remove(&self, key: RecordKey) -> DbResult<bool> {
        self.inner.remove(key).await
    }

    async fn flush(&self) -> DbResult<()> {
        self.inner.flush().await?;
        self.flushes.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn iter_stream(&self, batch_size: usize) -> TestStream {
        self.inner.iter_stream(batch_size)
    }

    fn scan_prefix_stream(&self, prefix: Bytes, batch_size: usize) -> TestStream {
        self.inner.scan_prefix_stream(prefix, batch_size)
    }

    fn iter_range_stream(
        &self,
        start_inclusive: Option<Bytes>,
        end_inclusive: Option<Bytes>,
        batch_size: usize,
    ) -> TestStream {
        self.inner
            .iter_range_stream(start_inclusive, end_inclusive, batch_size)
    }

    fn iter_range_stream_reverse(
        &self,
        start_inclusive: Option<Bytes>,
        end_inclusive: Option<Bytes>,
        batch_size: usize,
    ) -> TestStream {
        self.inner
            .iter_range_stream_reverse(start_inclusive, end_inclusive, batch_size)
    }
}

fn source_event() -> ChangelogEvent {
    // Raw storage fixture, not a query: deliberately foreign field ids.
    ChangelogEvent {
        repo: "leader".into(),
        commit_version: 7,
        tx_id: 1,
        actor: Actor::User(42),
        timestamp_ns: 0,
        changes: (1..=3)
            .map(|n| {
                let mut nested = TMap::default();
                nested.insert(InternerKey::new(303), InnerValue::Int(i64::from(n)));
                let mut fields = TMap::default();
                fields.insert(InternerKey::new(101), InnerValue::Int(i64::from(n)));
                fields.insert(
                    InternerKey::new(202),
                    InnerValue::List(vec![InnerValue::Map(nested)]),
                );
                RecordChange {
                    table: "items".into(),
                    key: RecordId([n; 16]).to_bytes(),
                    op: ChangeOp::Put,
                    value: Some(InnerValue::Map(fields).to_bytes().unwrap()),
                }
            })
            .collect(),
    }
}

fn expected_row(n: u8) -> QueryValue {
    let mut nested = TMap::default();
    nested.insert("leaf".into(), QueryValue::Int(i64::from(n)));
    let mut fields = TMap::default();
    fields.insert("id".into(), QueryValue::Int(i64::from(n)));
    fields.insert(
        "children".into(),
        QueryValue::List(vec![QueryValue::Map(nested)]),
    );
    QueryValue::Map(fields)
}

/// Separate runtimes guarantee background changefeed handles are dropped before
/// reopening the disk backend; no sleep or file-lock retry hides retained handles.
#[test]
fn failed_interner_set_retry_persists_all_rows_before_version_and_survives_reopen() {
    let dir = tempfile::TempDir::new().unwrap();
    let source = source_event();
    let bindings = vec![
        (101, "id".into()),
        (202, "children".into()),
        (303, "leaf".into()),
    ];
    {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let backing = BoxRepoFactory::fjall_raw(dir.path().to_path_buf())
                .create()
                .await
                .unwrap();
            let fault = Arc::new(FailingSetStore {
                inner: backing.store_get("__interner__").await.unwrap(),
                fail_next: AtomicBool::new(false),
                failures: AtomicUsize::new(0),
                writes: AtomicUsize::new(0),
                flushes: AtomicUsize::new(0),
            });
            let manager = InternerManager::new(fault.clone());
            let follower =
                RepoInstance::new("follower".into(), backing, vec![TableConfig::new("items")]);
            follower.install_repo_interner_for_test(manager.clone());
            let interner = manager.get().await.unwrap();
            interner.touch_ind("follower_only").unwrap();
            manager.persist().await.unwrap();
            let table = follower.get_table("items").await.unwrap();
            let gate = follower.tx_gate().await.unwrap();
            let mut rx = follower.subscribe_changelog().await.unwrap();
            let high = manager.persisted_high_water();
            let writes = fault.writes.load(Ordering::SeqCst);
            let flushes = fault.flushes.load(Ordering::SeqCst);
            assert_eq!(high, 1);
            assert_eq!(writes, 1);
            assert_eq!(flushes, 1);
            assert_eq!(gate.version_allocation_high_water_mark(), 0);
            assert_eq!(gate.last_committed(), 0);
            assert_eq!(gate.durable_watermark(), 0);

            // Arm only after all lazy setup and the initial durable seed.
            fault.fail_next.store(true, Ordering::SeqCst);
            let error = apply_batch(&follower, &source, &bindings, 0, ApplyPolicy::Trusted)
                .await
                .expect_err("first apply must surface interner set failure");
            assert!(matches!(&error, DbError::Storage(_)));
            assert!(error
                .to_string()
                .contains("#49: injected interner set failure"));
            assert_eq!(fault.failures.load(Ordering::SeqCst), 1);
            assert!(!fault.fail_next.load(Ordering::SeqCst));
            assert_eq!(fault.writes.load(Ordering::SeqCst), writes);
            assert_eq!(fault.flushes.load(Ordering::SeqCst), flushes);
            assert_eq!(manager.persisted_high_water(), high);
            assert_eq!(
                gate.version_allocation_high_water_mark(),
                0,
                "failed interner persistence must not consume a version",
            );
            assert_eq!(gate.last_committed(), 0);
            assert_eq!(gate.durable_watermark(), 0);
            for (_, name) in &bindings {
                assert!(
                    interner.get_ind(name).is_some(),
                    "failed persist leaves names live",
                );
            }
            for change in &source.changes {
                let id = RecordId::try_from_bytes(&change.key).unwrap();
                assert_eq!(table.get_bytes(id).await.unwrap(), None);
            }
            assert!(matches!(
                rx.try_recv(),
                Err(tokio::sync::broadcast::error::TryRecvError::Empty)
            ));
            assert!(follower
                .read_changelog_from(0, 100)
                .await
                .unwrap()
                .events
                .is_empty());

            // Same event, same bindings, no new first-touch and no manual persist.
            assert_eq!(
                apply_batch(&follower, &source, &bindings, 0, ApplyPolicy::Trusted)
                    .await
                    .unwrap(),
                ApplyOutcome::Applied { local_version: 1 },
            );
            assert_eq!(fault.failures.load(Ordering::SeqCst), 1);
            assert_eq!(
                fault.writes.load(Ordering::SeqCst),
                writes + 1,
                "retry must write the pending interner delta even without new names",
            );
            assert_eq!(fault.flushes.load(Ordering::SeqCst), flushes + 1);
            assert_eq!(manager.persisted_high_water(), high + bindings.len());
            assert_eq!(gate.version_allocation_high_water_mark(), 1);
            assert_eq!(gate.last_committed(), 1);
            assert_eq!(gate.durable_watermark(), 1);
            let downstream = rx.try_recv().unwrap();
            assert_eq!(downstream.commit_version, 1);
            assert_eq!(downstream.changes.len(), source.changes.len());
            assert!(matches!(
                rx.try_recv(),
                Err(tokio::sync::broadcast::error::TryRecvError::Empty)
            ));
            for n in 1..=3 {
                let row = table.get(RecordId([n; 16])).await.unwrap();
                assert_eq!(
                    inner_value_to_query_value(&row, interner).unwrap(),
                    expected_row(n),
                );
            }
            follower.flush_all_history().await.unwrap();
            // All foreground handles leave this scope; dropping the runtime also
            // releases the journal writer's backing-store handle before reopen.
        });
    }

    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let follower = RepoInstance::from_factory(
            "follower".into(),
            BoxRepoFactory::fjall_raw(dir.path().to_path_buf()),
            vec![TableConfig::new("items")],
        )
        .await
        .unwrap();
        let manager = follower.repo_interner().await.unwrap();
        let interner = manager.get().await.unwrap();
        assert_eq!(manager.persisted_high_water(), 4);
        assert!(interner.get_ind("follower_only").is_some());
        for (_, name) in &bindings {
            assert!(
                interner.get_ind(name).is_some(),
                "reopened dictionary lost {name}"
            );
        }
        let table = follower.get_table("items").await.unwrap();
        for n in 1..=3 {
            let row = table.get(RecordId([n; 16])).await.unwrap();
            assert_eq!(
                inner_value_to_query_value(&row, interner).unwrap(),
                expected_row(n),
            );
        }
    });
}

use std::future::{poll_fn, Future};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use bytes::Bytes;
use shamir_query_builder::write::doc;
#[cfg(feature = "fjall")]
use shamir_storage::error::DbError;
use shamir_storage::types::RecordKey;
#[cfg(feature = "fjall")]
use shamir_tx::{CancelHandle, WaitState};
use shamir_tx::{CommitVisibility, IsolationLevel, SnapshotGuard, TxContext};
use shamir_types::codecs::interned::codec::query_value_to_inner;
use shamir_types::types::value::InnerValue;
use tokio::sync::{broadcast::error::TryRecvError, Notify};

use crate::tx::commit::{
    AsyncIndexTailHook, PostWalPrePublishHook, TEST_ASYNC_INDEX_TAIL_HOOK,
    TEST_POST_WAL_PRE_PUBLISH_HOOK,
};
use crate::tx::repo::{BoxRepoFactory, RepoInstance};
use crate::tx::table::table_manager::TableManager;
use crate::tx::table::TableConfig;
use crate::tx::{CommitError, MaterializationState};

const BUDGET: Duration = Duration::from_secs(45);

async fn within<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(BUDGET, future)
        .await
        .expect("commit detach handshake stalled")
}

async fn pending<F: Future>(mut future: Pin<&mut F>) {
    assert!(poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx)))
        .await
        .is_pending());
}

async fn fixture(name: &str) -> (RepoInstance, Arc<PostWalPrePublishHook>) {
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
    assert!(TEST_POST_WAL_PRE_PUBLISH_HOOK.set(hook.clone()).is_ok());
    let repo = RepoInstance::from_factory(
        name.into(),
        BoxRepoFactory::in_memory(),
        vec![TableConfig::new("t")],
    )
    .await
    .unwrap();
    // Resolve WAL/gate before direct polling, so Pending cannot be first-touch I/O.
    repo.repo_wal().await.unwrap();
    repo.tx_gate().await.unwrap();
    (repo, hook)
}

async fn record(tbl: &TableManager, email: &str) -> InnerValue {
    let interner = tbl.interner().get().await.unwrap();
    query_value_to_inner(&doc().set("email", email).unwrap().build(), interner).unwrap()
}

async fn staged(repo: &RepoInstance, value: &str) -> (TxContext, SnapshotGuard) {
    let tbl = repo.get_table("t").await.unwrap();
    let (mut tx, snapshot) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    tbl.insert_tx(&InnerValue::Str(value.into()), Some(&mut tx))
        .await
        .unwrap();
    (tx, snapshot)
}

#[tokio::test]
async fn cancelled_post_wal_tail_keeps_unique_lock_changefeed_and_postings() {
    let (repo, hook) = fixture("detach_unique").await;
    let tbl = repo.get_table("t").await.unwrap();
    tbl.create_unique_index("by_email", &["email"])
        .await
        .unwrap();
    let interner = tbl.interner().get().await.unwrap();
    let index_id = interner.touch_ind("by_email").unwrap().key().id();
    let old = record(&tbl, "old").await;
    let new = record(&tbl, "new").await;
    let (mut parent, _parent_snapshot) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let rid = tbl.insert_tx(&old, Some(&mut parent)).await.unwrap();
    let (mut contender, _contender_snapshot) =
        repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    tbl.insert_tx(&old, Some(&mut contender)).await.unwrap();
    let mut events = repo.subscribe_changelog().await.unwrap();
    let drainer = repo.drainer();
    let replay = drainer.replay_lock_for_test().await;
    hook.armed.store(true, Ordering::Release);
    let owned = repo.clone();
    let caller = tokio::spawn(async move { owned.commit_tx(parent).await });
    within(hook.reached.notified()).await;
    let gate = repo.tx_gate().await.unwrap();
    let version = *repo.drainer().window_keys().last().unwrap();
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    assert_eq!(gate.orphan_max(), 0);
    assert_eq!(repo.tx_metrics().snapshot().post_wal_orphans_total, 0);
    assert!(gate.is_active_post_wal(version));
    let mut competing_commit = Box::pin(repo.commit_tx(contender));
    pending(competing_commit.as_mut()).await;
    assert!(repo.quiesce_commits(Duration::ZERO).await.is_err());
    hook.resume.notify_one();
    assert!(matches!(
        within(competing_commit).await,
        Err(CommitError::UniqueViolation { .. })
    ));
    within(repo.quiesce_commits(BUDGET)).await.unwrap();
    assert_eq!(gate.orphan_max(), 0);
    assert_eq!(repo.tx_metrics().snapshot().post_wal_orphans_total, 0);
    assert_eq!(tbl.get(rid).await.unwrap(), old);
    let event = events.try_recv().unwrap();
    assert_eq!(event.commit_version, version);
    assert_eq!(event.changes.len(), 1);
    assert!(matches!(events.try_recv(), Err(TryRecvError::Empty)));

    // Replay must not resurrect the old IndexPut after the younger indexed update.
    let (mut later, _later_snapshot) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    assert!(tbl.update_tx(rid, &new, Some(&mut later)).await.unwrap());
    within(repo.commit_tx(later)).await.unwrap();
    drop(replay);
    within(drainer.drain_all(&repo.core)).await.unwrap();
    assert_eq!(tbl.get(rid).await.unwrap(), new);
    assert_eq!(
        tbl.index_manager()
            .lookup_by_unique_index(index_id, &[InnerValue::Str("old".into())])
            .await
            .unwrap(),
        None,
    );
    assert_eq!(
        tbl.index_manager()
            .lookup_by_unique_index(index_id, &[InnerValue::Str("new".into())])
            .await
            .unwrap(),
        Some(rid),
    );
}

#[tokio::test]
async fn cancellation_during_prelock_is_a_clean_abort() {
    let (repo, _hook) = fixture("detach_prelock").await;
    let tbl = repo.get_table("t").await.unwrap();
    tbl.create_unique_index("by_email", &["email"])
        .await
        .unwrap();
    let value = record(&tbl, "aborted").await;
    let (mut tx, _snapshot) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let rid = tbl.insert_tx(&value, Some(&mut tx)).await.unwrap();
    let gate = repo.tx_gate().await.unwrap();
    let before = gate.last_committed();
    let lock = tbl.unique_write_lock().lock_owned().await;
    let mut commit = Box::pin(repo.commit_tx(tx));
    pending(commit.as_mut()).await;
    // No driver exists yet: dropping this future must NOT enqueue a detached tail.
    drop(commit);
    repo.quiesce_commits(Duration::ZERO).await.unwrap();
    drop(lock);
    assert_eq!(gate.last_committed(), before);
    assert_eq!(gate.orphan_max(), 0);
    assert_eq!(repo.tx_metrics().snapshot().post_wal_orphans_total, 0);
    assert!(tbl.get(rid).await.is_err());
    let (mut replacement, _replacement_snapshot) =
        repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let replacement_rid = tbl.insert_tx(&value, Some(&mut replacement)).await.unwrap();
    within(repo.commit_tx(replacement)).await.unwrap();
    assert_eq!(tbl.get(replacement_rid).await.unwrap(), value);
    within(repo.quiesce_commits(BUDGET)).await.unwrap();
    assert!(tbl.get(rid).await.is_err());
    assert_eq!(tbl.counter().get().await.unwrap(), 1);
}

#[cfg(feature = "fjall")]
#[tokio::test]
async fn cooperative_cancel_before_commit_driver_leaves_wal_unchanged() {
    for visibility in [CommitVisibility::Synchronous, CommitVisibility::AsyncIndex] {
        let dir = tempfile::tempdir().unwrap();
        tokio::time::timeout(Duration::from_secs(120), async {
            let repo = RepoInstance::from_factory(
                "cooperative_cancel_pre_driver".into(),
                BoxRepoFactory::fjall_raw(dir.path().join("repo")),
                vec![TableConfig::new("t")],
            )
            .await
            .unwrap();
            repo.repo_wal().await.unwrap();
            let gate = repo.tx_gate().await.unwrap();
            let wal = repo.wal_group().await.unwrap();
            let tbl = repo.get_table("t").await.unwrap();
            tbl.create_unique_index("by_email", &["email"])
                .await
                .unwrap();
            let value = record(&tbl, "cancelled").await;
            let (mut tx, _snapshot) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
            let rid = tbl.insert_tx(&value, Some(&mut tx)).await.unwrap();
            tx.set_visibility(visibility);
            let frames_before = wal.replay().await.unwrap();
            let allocated_before = gate.version_allocation_high_water_mark();

            let unique_lock = tbl.unique_write_lock();
            let held = unique_lock.try_lock().expect("unique prelock starts free");
            let cancel = Arc::new(CancelHandle::default());
            let explicit_cancel = if visibility == CommitVisibility::AsyncIndex {
                tx.set_cancel_handle(Some(Arc::clone(&cancel)));
                None
            } else {
                Some(Arc::clone(&cancel))
            };
            let mut commit = Box::pin(crate::tx::ack::commit_tx_with_cancel(
                tx,
                &repo.core,
                repo.commit_visibility_wait(),
                explicit_cancel,
            ));
            while cancel.waiting() != WaitState::Lock {
                tokio::select! {
                    result = &mut commit => panic!("commit returned before cancellation: {result:?}"),
                    () = tokio::time::sleep(Duration::from_millis(20)) => {}
                }
            }
            assert_eq!(cancel.try_cancel(), Ok(()), "{visibility:?}");
            let result = commit.await;
            assert_eq!(cancel.waiting(), WaitState::None, "{visibility:?}");
            assert!(
                matches!(result, Err(CommitError::Cancelled)),
                "{visibility:?}: {result:?}"
            );
            drop(held);
            assert_eq!(wal.replay().await.unwrap(), frames_before, "{visibility:?}");
            assert_eq!(
                gate.version_allocation_high_water_mark(),
                allocated_before,
                "{visibility:?}"
            );
            let row = tbl.get(rid).await;
            assert!(
                matches!(row, Err(DbError::NotFound(_))),
                "{visibility:?}: expected absent row, got {row:?}"
            );
        })
        .await
        .unwrap_or_else(|_| panic!("{visibility:?}: pre-driver cancellation watchdog expired"));
    }
}

#[tokio::test]
async fn async_index_background_tail_blocks_quiescence_after_outcome_returns() {
    let (repo, _hook) = fixture("async_index_barrier").await;
    let hook = Arc::new(AsyncIndexTailHook {
        repo_name: repo.name().into(),
        reached: Notify::new(),
        resume: Notify::new(),
        armed: AtomicBool::new(true),
    });
    assert!(TEST_ASYNC_INDEX_TAIL_HOOK.set(hook.clone()).is_ok());
    let (mut tx, _snapshot) = staged(&repo, "async index").await;
    tx.set_visibility(CommitVisibility::AsyncIndex);
    let mut outcome = within(repo.commit_tx(tx)).await.unwrap();
    within(hook.reached.notified()).await;
    assert_eq!(outcome.materialization, MaterializationState::Complete);
    assert!(outcome.commit_version > outcome.snapshot_version);
    let background = outcome.take_background().expect("AsyncIndex tail handle");
    // Red without the independent guard: the inline driver has already finished.
    assert!(repo.quiesce_commits(Duration::ZERO).await.is_err());
    hook.resume.notify_one();
    assert_eq!(
        within(background.join()).await,
        MaterializationState::Complete,
    );
    repo.quiesce_commits(Duration::ZERO).await.unwrap();
    assert_eq!(repo.tx_metrics().snapshot().post_wal_orphans_total, 0);
}

#[tokio::test]
async fn quiescence_counts_inline_and_detached_tails() {
    let (repo, hook) = fixture("detach_barrier").await;
    let (inline_tx, _inline_snapshot) = staged(&repo, "inline").await;
    hook.armed.store(true, Ordering::Release);
    let mut inline = Box::pin(repo.commit_tx(inline_tx));
    pending(inline.as_mut()).await;
    within(hook.reached.notified()).await;
    // Inline alone must prevent quiescence before any detached tx exists.
    assert!(repo.quiesce_commits(Duration::ZERO).await.is_err());
    let (detached_tx, _detached_snapshot) = staged(&repo, "detached").await;
    hook.armed.store(true, Ordering::Release);
    let mut detached = Box::pin(repo.commit_tx(detached_tx));
    pending(detached.as_mut()).await;
    within(hook.reached.notified()).await;
    drop(detached);
    let mut quiescence = Box::pin(repo.quiesce_commits(BUDGET));
    pending(quiescence.as_mut()).await;
    assert!(repo.quiesce_commits(Duration::ZERO).await.is_err());
    // FIFO Notify waiter: inline registered first. Current-thread polling here
    // drives it to completion before the detached task can consume another permit.
    hook.resume.notify_one();
    within(inline).await.unwrap();
    pending(quiescence.as_mut()).await;
    assert!(repo.quiesce_commits(Duration::ZERO).await.is_err());
    hook.resume.notify_one();
    within(quiescence).await.unwrap();
    repo.quiesce_commits(Duration::ZERO).await.unwrap();
    assert_eq!(repo.tx_gate().await.unwrap().orphan_max(), 0);
}

async fn level3_tail(detach: bool, panic_tail: bool, name: &str) {
    let (repo, hook) = fixture(name).await;
    let tbl = repo.get_table("t").await.unwrap();
    let (mut parent, _parent_snapshot) = repo.begin_tx(IsolationLevel::Pessimistic).await.unwrap();
    let value = InnerValue::Str("published".into());
    let rid = tbl.insert_tx(&value, Some(&mut parent)).await.unwrap();
    let key = RecordKey::from(Bytes::copy_from_slice(rid.as_bytes()));
    tbl.acquire_pessimistic_write_lock(key.clone(), &parent)
        .await
        .unwrap();
    let (younger, _younger_snapshot) = repo.begin_tx(IsolationLevel::Pessimistic).await.unwrap();
    assert!(parent.tx_id.0 < younger.tx_id.0);
    // Hold real replay ownership so an orphan cannot disappear before assertions.
    let drainer = repo.drainer();
    let replay = drainer.replay_lock_for_test().await;
    hook.armed.store(true, Ordering::Release);
    let owned = repo.clone();
    hook.panic_after_resume.store(panic_tail, Ordering::Release);
    let caller = tokio::spawn(async move { owned.commit_tx(parent).await });
    within(hook.reached.notified()).await;
    let gate = repo.tx_gate().await.unwrap();
    let version = *drainer.window_keys().last().unwrap();
    let mut lock = Box::pin(tbl.acquire_pessimistic_write_lock(key, &younger));
    pending(lock.as_mut()).await;
    if detach {
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
    } else {
        // Inline here means the driver's poll stays on its caller's task.
        assert!(panic_tail);
        hook.panic_resume.notify_one();
        assert!(within(caller).await.unwrap_err().is_panic());
    }
    if detach {
        assert_eq!(gate.orphan_max(), 0);
        assert_eq!(repo.tx_metrics().snapshot().post_wal_orphans_total, 0);
        pending(lock.as_mut()).await;
        if panic_tail {
            hook.panic_resume.notify_one();
        } else {
            hook.resume.notify_one();
        }
    }
    within(lock).await.unwrap();
    within(repo.quiesce_commits(BUDGET)).await.unwrap();
    repo.quiesce_commits(Duration::ZERO).await.unwrap();
    if panic_tail {
        assert_eq!(gate.orphan_max(), version);
        assert_eq!(repo.tx_metrics().snapshot().post_wal_orphans_total, 1);
        assert!(gate.is_orphaned_post_wal(version));
    } else {
        assert_eq!(gate.orphan_max(), 0);
        assert_eq!(repo.tx_metrics().snapshot().post_wal_orphans_total, 0);
        assert_eq!(tbl.get(rid).await.unwrap(), value);
    }
    crate::tx::commit::release_pessimistic_locks(&younger, &repo.core).await;
    drop(replay);
    within(drainer.drain_all(&repo.core)).await.unwrap();
    assert!(!gate.is_orphaned_post_wal(version));
    assert!(gate.durable_watermark() >= version);
    assert_eq!(tbl.get(rid).await.unwrap(), value);
    assert_eq!(
        repo.tx_metrics().snapshot().post_wal_orphans_total,
        u64::from(panic_tail),
    );
}

#[tokio::test]
async fn cancelled_level3_tail_releases_lock_only_after_resume() {
    level3_tail(true, false, "detach_level3").await;
}

#[tokio::test]
async fn inline_tail_panic_recovers_orphan_and_releases_level3_lock() {
    level3_tail(false, true, "inline_panic").await;
}

#[tokio::test]
async fn detached_tail_panic_recovers_orphan_and_releases_level3_lock() {
    level3_tail(true, true, "detached_panic").await;
}

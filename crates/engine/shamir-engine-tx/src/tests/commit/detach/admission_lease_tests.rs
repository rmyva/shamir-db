//! R2a 1c wiring: the admission lease's ACTIVE count stays ABOVE ZERO across
//! every commit-path continuation — (a) the detached `CommitDriver`
//! continuation (the suspended-future transfer on drop), (b) the AsyncIndex
//! materialization tail until it finishes, and (c) the lock-free commit
//! branch inline — and returns to ZERO afterwards.
//!
//! Deterministic handshakes only: the existing one-shot test hooks
//! (`TEST_POST_WAL_PRE_PUBLISH_HOOK`, `TEST_POST_VALIDATE_PRE_PUBLISH_HOOK`,
//! `TEST_ASYNC_INDEX_TAIL_HOOK`) park the commit at the exact seam while the
//! test observes the gate count, then release it.

use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use shamir_tx::{CommitVisibility, IsolationLevel};
use shamir_types::types::value::InnerValue;
use tokio::sync::Notify;

use crate::tx::commit::pipeline::admission::gate::CommitAdmissionGate;
use crate::tx::commit::{
    AsyncIndexTailHook, PostValidatePrePublishHook, PostWalPrePublishHook,
    TEST_ASYNC_INDEX_TAIL_HOOK, TEST_POST_VALIDATE_PRE_PUBLISH_HOOK,
    TEST_POST_WAL_PRE_PUBLISH_HOOK,
};
use crate::tx::repo::{BoxRepoFactory, RepoInstance};
use crate::tx::table::TableConfig;
use crate::tx::MaterializationState;

const BUDGET: Duration = Duration::from_secs(45);

async fn within<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(BUDGET, future)
        .await
        .expect("admission lease handshake stalled")
}

/// Spin-free wait for the count to return to zero: `yield_now` drives the
/// current-thread runtime (detached continuations and spawned tails run
/// between yields); the timeout is a failure backstop, not evidence.
async fn await_zero(admission: &CommitAdmissionGate) {
    tokio::time::timeout(BUDGET, async {
        while admission.active_count() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("admission lease never returned to zero");
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
    repo.repo_wal().await.unwrap();
    repo.tx_gate().await.unwrap();
    (repo, hook)
}

async fn staged(
    repo: &RepoInstance,
    value: &str,
) -> (shamir_tx::TxContext, shamir_tx::SnapshotGuard) {
    let tbl = repo.get_table("t").await.unwrap();
    let (mut tx, snapshot) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    tbl.insert_tx(&InnerValue::Str(value.into()), Some(&mut tx))
        .await
        .unwrap();
    (tx, snapshot)
}

/// (a) Aborting the caller detaches the suspended commit continuation —
/// the transferred future still holds the lease until it runs to completion.
#[tokio::test]
async fn detached_continuation_keeps_admission_counted() {
    let (repo, hook) = fixture("admission_detach").await;
    let admission = Arc::clone(repo.core().commit_admission());
    let (tx, _snapshot) = staged(&repo, "detached").await;
    hook.armed.store(true, Ordering::Release);
    let owned = repo.clone();
    let caller = tokio::spawn(async move { owned.commit_tx(tx).await });
    within(hook.reached.notified()).await;
    assert_eq!(admission.active_count(), 1);

    // Detach: the suspended future transfers on drop and keeps running.
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    assert_eq!(admission.active_count(), 1);

    hook.resume.notify_one();
    await_zero(&admission).await;
    assert!(!admission.is_closed());
}

/// (b) The AsyncIndex materialization tail is spawned independently; the
/// inline driver has returned while the tail still holds the lease, counted
/// through completion.
#[tokio::test]
async fn async_index_tail_keeps_admission_counted_until_finished() {
    let (repo, _hook) = fixture("admission_async_tail").await;
    let tail = Arc::new(AsyncIndexTailHook {
        repo_name: repo.name().into(),
        reached: Notify::new(),
        resume: Notify::new(),
        armed: AtomicBool::new(true),
    });
    assert!(TEST_ASYNC_INDEX_TAIL_HOOK.set(tail.clone()).is_ok());
    let admission = Arc::clone(repo.core().commit_admission());
    let (mut tx, _snapshot) = staged(&repo, "async index").await;
    tx.set_visibility(CommitVisibility::AsyncIndex);
    let mut outcome = within(repo.commit_tx(tx)).await.unwrap();
    within(tail.reached.notified()).await;
    // Inline driver done, spawned tail parked: still counted.
    assert_eq!(admission.active_count(), 1);

    let background = outcome.take_background().expect("AsyncIndex tail handle");
    tail.resume.notify_one();
    assert_eq!(
        within(background.join()).await,
        MaterializationState::Complete
    );
    await_zero(&admission).await;
}

/// (c) The lock-free branch itself holds the lease inline from admission
/// until publication completes.
#[tokio::test]
async fn lockfree_branch_counts_inline_until_published() {
    let (repo, _hook) = fixture("admission_lockfree").await;
    let pause = Arc::new(PostValidatePrePublishHook {
        reached: AtomicUsize::new(0),
        resume: Notify::new(),
        armed: AtomicBool::new(true),
    });
    assert!(TEST_POST_VALIDATE_PRE_PUBLISH_HOOK
        .set(Arc::clone(&pause))
        .is_ok());
    let admission = Arc::clone(repo.core().commit_admission());
    let (tx, _snapshot) = staged(&repo, "lockfree").await;
    let owned = repo.clone();
    let caller = tokio::spawn(async move { owned.commit_tx(tx).await });
    while pause.reached.load(Ordering::SeqCst) == 0 {
        tokio::task::yield_now().await;
    }
    assert_eq!(admission.active_count(), 1);

    pause.resume.notify_one();
    within(caller).await.unwrap().unwrap();
    assert_eq!(admission.active_count(), 0);
}

//! Tests for owner drop and physical resource release.
use crate::tx::cold_reconcile::lifecycle::{
    spawn_worker, InstallError, RepoWorkerLifecycle, WorkerLease,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::{Barrier, Notify};

struct DropSignal(Arc<Notify>);
impl Drop for DropSignal {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}
struct Lease {
    _signal: DropSignal,
}
impl WorkerLease for Lease {}

#[tokio::test(start_paused = true)]
async fn owner_drop_idle_worker_releases_resource_after_join() {
    let dropped = Arc::new(Notify::new());
    let owner = Arc::new(RepoWorkerLifecycle::<(), ()>::new());
    let status = owner.status();
    let weak = Arc::downgrade(&owner);
    spawn_worker(
        &owner,
        Lease {
            _signal: DropSignal(Arc::clone(&dropped)),
        },
        |mut state| async move { while state.next_job().await.is_some() {} },
    )
    .unwrap();
    drop(owner);
    assert!(weak.upgrade().is_none());
    dropped.notified().await;
    assert!(!status.quarantined());
}

#[tokio::test(start_paused = true)]
async fn owner_drop_active_and_queued_jobs_releases_actual_resource() {
    let dropped = Arc::new(Notify::new());
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let (worker_entered, worker_release) = (Arc::clone(&entered), Arc::clone(&release));
    let owner = Arc::new(RepoWorkerLifecycle::<u64, ()>::new());
    let weak = Arc::downgrade(&owner);
    spawn_worker(
        &owner,
        Lease {
            _signal: DropSignal(Arc::clone(&dropped)),
        },
        move |mut state| async move {
            if let Some(job) = state.next_job().await {
                worker_entered.notify_one();
                worker_release.notified().await;
                job.reply(());
            }
            while let Some(job) = state.next_job().await {
                job.reply(());
            }
        },
    )
    .unwrap();
    let first = owner.submit(0).unwrap();
    entered.notified().await;
    let queued = owner.submit(1).unwrap();
    drop(owner);
    assert!(weak.upgrade().is_none());
    release.notify_one();
    drop(first);
    queued.await.unwrap();
    dropped.notified().await;
}

#[tokio::test(start_paused = true)]
async fn owner_drop_queued_work_drains_before_resource_drop() {
    let held = Arc::new(AtomicUsize::new(1));
    let dropped = Arc::new(Notify::new());
    struct LeaseWithCount {
        count: Arc<AtomicUsize>,
        _signal: DropSignal,
    }
    impl Drop for LeaseWithCount {
        fn drop(&mut self) {
            self.count.fetch_sub(1, Ordering::SeqCst);
        }
    }
    impl WorkerLease for LeaseWithCount {}
    let owner = Arc::new(RepoWorkerLifecycle::<u64, ()>::new());
    let weak = Arc::downgrade(&owner);
    let gate = Arc::new(Notify::new());
    let worker_gate = Arc::clone(&gate);
    spawn_worker(
        &owner,
        LeaseWithCount {
            count: Arc::clone(&held),
            _signal: DropSignal(Arc::clone(&dropped)),
        },
        move |mut state| async move {
            if let Some(job) = state.next_job().await {
                worker_gate.notified().await;
                job.reply(());
            }
            while let Some(job) = state.next_job().await {
                job.reply(());
            }
        },
    )
    .unwrap();
    let first = owner.submit(0).unwrap();
    let second = owner.submit(1).unwrap();
    drop(owner);
    assert!(weak.upgrade().is_none());
    gate.notify_one();
    first.await.unwrap();
    second.await.unwrap();
    dropped.notified().await;
    assert_eq!(held.load(Ordering::SeqCst), 0);
}

#[tokio::test(start_paused = true)]
async fn close_before_first_install_never_spawns_handler() {
    let owner = Arc::new(RepoWorkerLifecycle::<(), ()>::new());
    let called = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(Barrier::new(2));
    let (o, c, b) = (
        Arc::clone(&owner),
        Arc::clone(&called),
        Arc::clone(&barrier),
    );
    let attempted = tokio::spawn(async move {
        b.wait().await;
        o.install_worker(
            Lease {
                _signal: DropSignal(Arc::new(Notify::new())),
            },
            move |_s| async move {
                c.fetch_add(1, Ordering::SeqCst);
            },
        )
    });
    owner.close();
    barrier.wait().await;
    assert!(matches!(
        attempted.await.unwrap(),
        Err(InstallError::ClosedOrForbidden)
    ));
    assert_eq!(called.load(Ordering::SeqCst), 0);
    assert!(matches!(
        owner.install_worker(
            Lease {
                _signal: DropSignal(Arc::new(Notify::new()))
            },
            |_s| async {}
        ),
        Err(InstallError::ClosedOrForbidden)
    ));
}

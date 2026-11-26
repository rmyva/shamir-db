//! Supervisor join, lease release, and fail-closed panic tests.
use crate::tx::cold_reconcile::lifecycle::{
    spawn_worker, FailClosedResource, ForegroundLifecycle, JobSubmitError, LifecycleStatus,
    RepoUnitLifecycle, ShutdownError, WorkerLease, WorkerTerminal,
};
use futures::FutureExt;
use shamir_tx::RepoTxGate;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;

struct NoopLease;
impl WorkerLease for NoopLease {}
struct DropSignal(Arc<Notify>);
impl Drop for DropSignal {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}
struct LeaseGuard {
    released: Arc<AtomicBool>,
    _signal: DropSignal,
    _resource: Arc<()>,
}
impl Drop for LeaseGuard {
    fn drop(&mut self) {
        self.released.store(true, Ordering::SeqCst);
    }
}
impl WorkerLease for LeaseGuard {}

#[tokio::test(start_paused = true)]
async fn lease_is_dropped_after_join_before_terminal() {
    let dropped = Arc::new(Notify::new());
    let resource = Arc::new(());
    let weak = Arc::downgrade(&resource);
    let released = Arc::new(AtomicBool::new(false));
    let owner = Arc::new(RepoUnitLifecycle::new());
    let gate = Arc::new(Notify::new());
    let worker_gate = Arc::clone(&gate);
    spawn_worker(
        &owner,
        LeaseGuard {
            released: Arc::clone(&released),
            _signal: DropSignal(Arc::clone(&dropped)),
            _resource: resource,
        },
        move |mut state| async move {
            let _job = state.next_job().await.expect("submitted job");
            worker_gate.notified().await;
        },
    )
    .unwrap();
    let _rx = owner.submit(()).unwrap();
    gate.notify_one();
    assert!(matches!(
        owner.shutdown(Duration::from_secs(3600)).await,
        Ok(WorkerTerminal::Completed)
    ));
    assert!(owner.supervisor_joined());
    assert!(weak.upgrade().is_none());
    dropped.notified().await;
    assert!(released.load(Ordering::SeqCst));
}

struct PanicLease {
    _status: LifecycleStatus,
}
impl WorkerLease for PanicLease {}
struct ProtectedResource {
    status: LifecycleStatus,
    observed: Arc<AtomicBool>,
    dropped: Arc<Notify>,
}
impl FailClosedResource for ProtectedResource {
    fn arm_fail_closed(&mut self) {}
}
impl Drop for ProtectedResource {
    fn drop(&mut self) {
        self.observed
            .store(self.status.quarantined(), Ordering::SeqCst);
        self.dropped.notify_one();
    }
}

#[tokio::test(start_paused = true)]
async fn protected_resource_drop_observes_actual_quarantine_during_unwind() {
    let owner = Arc::new(RepoUnitLifecycle::new());
    let owner_status = owner.status();
    let observed = Arc::new(AtomicBool::new(false));
    let dropped = Arc::new(Notify::new());
    let drop_observed = Arc::clone(&observed);
    let drop_signal = Arc::clone(&dropped);
    spawn_worker(
        &owner,
        PanicLease {
            _status: owner_status.clone(),
        },
        move |state| async move {
            let _guard = state.protect_guard(ProtectedResource {
                status: state.status(),
                observed: drop_observed,
                dropped: drop_signal,
            });
            panic!("intentional worker panic");
        },
    )
    .unwrap();
    dropped.notified().await;
    assert!(!owner.admission_open());
    assert!(matches!(owner.submit(()), Err(JobSubmitError::Closing)));
    assert!(owner.quarantined() && owner.reopen_required());
    assert!(owner.interlock().publish(|| ()).is_err());
    assert!(observed.load(Ordering::SeqCst));
    assert!(matches!(
        owner.shutdown(Duration::from_secs(3600)).await,
        Ok(WorkerTerminal::Failed(_))
    ));
    assert!(owner.supervisor_joined());
    drop(owner_status);
}

#[tokio::test(start_paused = true)]
async fn version_guard_is_armed_by_protection_before_panic() {
    let owner = Arc::new(RepoUnitLifecycle::new());
    let gate = Arc::new(RepoTxGate::fresh());
    let before_visible = gate.completion().watermark();
    let before_durable = gate.durable_watermark();
    assert_eq!((before_visible, before_durable), (0, 0));
    let worker_gate = Arc::clone(&gate);
    spawn_worker(&owner, NoopLease, move |state| async move {
        let guard = worker_gate.assign_next_version_guarded();
        let _protected = state.protect_guard(guard);
        panic!("version guard panic");
    })
    .unwrap();
    let result = owner.shutdown(Duration::from_secs(3600)).await;
    let failure = match result {
        Ok(WorkerTerminal::Failed(message)) => message,
        other => panic!("expected worker failure, got {other:?}"),
    };
    assert!(owner.quarantined() && owner.reopen_required() && owner.supervisor_joined());
    assert!(gate.requires_reopen());
    assert_eq!(gate.completion().watermark(), before_visible);
    assert_eq!(gate.durable_watermark(), before_durable);
    assert!(
        matches!(owner.shutdown(Duration::ZERO).await, Ok(WorkerTerminal::Failed(message)) if message == failure)
    );
}

impl FailClosedResource for Arc<()> {
    fn arm_fail_closed(&mut self) {}
}

#[tokio::test(start_paused = true)]
async fn protected_version_guard_commits_on_successful_publication() {
    let owner = Arc::new(RepoUnitLifecycle::new());
    let gate = Arc::new(RepoTxGate::fresh());
    let before_durable = gate.durable_watermark();
    let worker_gate = Arc::clone(&gate);
    let (published_tx, published_rx) = tokio::sync::oneshot::channel();
    spawn_worker(&owner, NoopLease, move |state| async move {
        let guard = worker_gate.assign_next_version_guarded();
        let protected = state.protect_guard(guard);
        let publication = state.interlock().publish(|| {
            published_tx.send(protected.get().version()).unwrap();
        });
        assert!(publication.is_ok());
        protected.commit();
    })
    .unwrap();
    let version = published_rx.await.expect("publication completed");
    assert_eq!(version, 1);
    assert!(matches!(
        owner.shutdown(Duration::from_secs(3600)).await,
        Ok(WorkerTerminal::Completed)
    ));
    assert!(owner.supervisor_joined());
    assert!(!owner.quarantined());
    assert!(!owner.reopen_required());
    assert!(!gate.requires_reopen());
    assert_eq!(gate.completion().watermark(), version);
    assert_eq!(gate.durable_watermark(), before_durable);
}

#[tokio::test(start_paused = true)]
async fn protected_local_resource_survives_timeout_until_worker_join() {
    let owner = Arc::new(RepoUnitLifecycle::new());
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let (worker_entered, worker_release) = (Arc::clone(&entered), Arc::clone(&release));
    let resource = Arc::new(());
    let weak_resource = Arc::downgrade(&resource);
    let worker_resource = Arc::clone(&resource);
    spawn_worker(&owner, NoopLease, move |state| async move {
        let _protected = state.protect_guard(worker_resource);
        worker_entered.notify_one();
        worker_release.notified().await;
    })
    .unwrap();
    drop(resource);
    let _rx = owner.submit(()).unwrap();
    entered.notified().await;
    assert!(matches!(
        owner.shutdown(Duration::ZERO).await,
        Err(ShutdownError::TimedOut)
    ));
    assert!(!owner.admission_open());
    assert!(matches!(owner.submit(()), Err(JobSubmitError::Closing)));
    assert!(owner.quarantined() && owner.reopen_required());
    assert!(owner.interlock().publish(|| ()).is_err());
    assert!(weak_resource.upgrade().is_some());
    release.notify_one();
    let terminal = owner.shutdown(Duration::from_secs(3600)).await;
    assert!(matches!(
        terminal,
        Ok(WorkerTerminal::Failed(ref reason))
            if reason == "worker quarantined: worker shutdown deadline elapsed"
    ));
    assert!(owner.supervisor_joined());
    assert!(weak_resource.upgrade().is_none());
}

#[tokio::test(start_paused = true)]
async fn concurrent_failure_and_different_deadlines_are_independent() {
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let (worker_entered, worker_release) = (Arc::clone(&entered), Arc::clone(&release));
    let owner = Arc::new(RepoUnitLifecycle::new());
    spawn_worker(&owner, NoopLease, move |mut state| async move {
        let _ = state.next_job().await.unwrap();
        worker_entered.notify_one();
        worker_release.notified().await;
        panic!("failure");
    })
    .unwrap();
    let _rx = owner.submit(()).unwrap();
    entered.notified().await;
    let mut short = Box::pin(owner.shutdown(Duration::from_secs(10)));
    let mut long = Box::pin(owner.shutdown(Duration::from_secs(3600)));
    assert!(short.as_mut().now_or_never().is_none());
    assert!(long.as_mut().now_or_never().is_none());
    tokio::time::advance(Duration::from_secs(11)).await;
    assert!(matches!(
        short.as_mut().now_or_never(),
        Some(Err(ShutdownError::TimedOut))
    ));
    drop(short);
    release.notify_one();
    assert!(matches!(long.await, Ok(WorkerTerminal::Failed(_))));
    assert!(owner.supervisor_joined() && owner.quarantined());
}

#[tokio::test(start_paused = true)]
async fn final_foreground_token_closes_shared_lifecycle_without_raw_arc_cycle() {
    let lifecycle = Arc::new(RepoUnitLifecycle::new());
    let token = Arc::new(ForegroundLifecycle::from_lifecycle(Arc::clone(&lifecycle)));
    let retained_token = Arc::clone(&token);
    drop(token);
    assert!(lifecycle.admission_open());
    drop(retained_token);
    assert!(!lifecycle.admission_open());
    assert!(matches!(
        lifecycle.shutdown(Duration::ZERO).await,
        Ok(WorkerTerminal::Completed)
    ));
}

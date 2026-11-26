//! Deterministic tests for lifecycle admission, worker completion, and shutdown waiters.
use crate::tx::apply_replicated::follower_durability::receipt::resume::worker::ReceiptLease;
use crate::tx::cold_reconcile::lifecycle::{
    spawn_worker, InstallError, JobSubmitError, RepoUnitLifecycle, RepoWorkerLifecycle,
    ShutdownError, WorkerLease, WorkerTerminal,
};
use futures::FutureExt;
use shamir_tx::RepoTxGate;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;

struct NoopLease;
impl WorkerLease for NoopLease {}

#[tokio::test(start_paused = true)]
async fn quarantine_between_install_check_and_hook_registration_fails_closed() {
    let owner = RepoUnitLifecycle::new();
    let gate = Arc::new(RepoTxGate::fresh());
    let lease = ReceiptLease::new(Arc::clone(&gate), owner.status());
    let reached = Arc::new(Notify::new());
    let reached_wait = reached.notified();
    tokio::pin!(reached_wait);
    reached_wait.as_mut().enable();
    let reached_hook = Arc::clone(&reached);
    owner.set_before_quarantine_hook_install(Box::new(move |core| {
        reached_hook.notify_one();
        core.quarantine("install race quarantine");
    }));

    assert!(matches!(
        owner.install_worker(lease, |_state| async {}),
        Err(InstallError::ClosedOrForbidden)
    ));
    reached_wait.await;
    assert!(owner.quarantined());
    assert!(owner.reopen_required());
    assert!(gate.requires_reopen());
}

#[tokio::test(start_paused = true)]
async fn clean_receipt_lease_drop_does_not_require_reopen() {
    let owner = RepoUnitLifecycle::new();
    let gate = Arc::new(RepoTxGate::fresh());
    let lease = ReceiptLease::new(Arc::clone(&gate), owner.status());
    let clone = lease.clone();
    drop(lease);
    assert!(!gate.requires_reopen());
    drop(clone);
    assert!(!gate.requires_reopen());
}

#[tokio::test(start_paused = true)]
async fn explicit_receipt_quarantine_fails_closed_without_parked_guards() {
    let owner = RepoUnitLifecycle::new();
    let gate = Arc::new(RepoTxGate::fresh());
    let lease = ReceiptLease::new(Arc::clone(&gate), owner.status());
    WorkerLease::on_quarantine(&lease);
    assert!(gate.requires_reopen());
    assert!(lease.publish(|| 42).is_ok());
    owner.quarantine("receipt lease quarantined".to_owned());
    assert!(lease.publish(|| 7).is_err());
}
#[tokio::test(start_paused = true)]
async fn generic_quarantine_preserves_its_failure_reason() {
    let owner = Arc::new(RepoUnitLifecycle::new());
    let worker = Arc::clone(&owner);
    spawn_worker(&owner, NoopLease, move |_state| async move {
        worker.quarantine("unrelated timeout".to_owned());
    })
    .unwrap();
    assert!(matches!(
        owner.shutdown(Duration::from_secs(3600)).await,
        Ok(WorkerTerminal::Failed(message)) if message == "worker quarantined: unrelated timeout"
    ));
}

#[tokio::test(start_paused = true)]
async fn lifecycle_status_publication_is_interlocked_with_quarantine() {
    let owner = Arc::new(RepoUnitLifecycle::new());
    let status = owner.status();
    assert_eq!(status.publish(|| 42), Ok(42));
    owner.quarantine("test quarantine".to_owned());
    assert!(status.publish(|| 7).is_err());
    assert!(owner.reopen_required());
}
#[tokio::test(start_paused = true)]
async fn submit_without_worker_is_not_initialized() {
    let owner = RepoUnitLifecycle::new();
    assert!(owner.admission_open());
    assert!(matches!(
        owner.submit(()),
        Err(JobSubmitError::NotInitialized)
    ));
    owner.close();
    assert!(!owner.admission_open());
    assert!(matches!(owner.submit(()), Err(JobSubmitError::Closing)));
}

#[tokio::test(start_paused = true)]
async fn no_worker_shutdown_completes_immediately() {
    let owner = RepoUnitLifecycle::new();
    assert!(matches!(
        owner.shutdown(Duration::ZERO).await,
        Ok(WorkerTerminal::Completed)
    ));
    assert!(owner.supervisor_joined());
}

#[tokio::test(start_paused = true)]
async fn echo_worker_drains_and_repeated_shutdown_uses_terminal() {
    let owner = Arc::new(RepoUnitLifecycle::new());
    spawn_worker(&owner, NoopLease, |mut s| async move {
        while let Some(job) = s.next_job().await {
            job.reply(());
        }
    })
    .unwrap();
    owner.submit(()).unwrap().await.unwrap();
    assert!(matches!(
        owner.shutdown(Duration::from_secs(3600)).await,
        Ok(WorkerTerminal::Completed)
    ));
    assert!(matches!(
        owner.shutdown(Duration::from_secs(3600)).await,
        Ok(WorkerTerminal::Completed)
    ));
    assert!(owner.supervisor_joined());
}

#[tokio::test(start_paused = true)]
async fn install_rejection_distinguishes_installed_and_closed() {
    let owner = Arc::new(RepoUnitLifecycle::new());
    spawn_worker(&owner, NoopLease, |_s| async {}).unwrap();
    assert!(matches!(
        spawn_worker(&owner, NoopLease, |_s| async {}),
        Err(InstallError::AlreadyInstalled)
    ));
    let closed = Arc::new(RepoUnitLifecycle::new());
    closed.close();
    assert!(matches!(
        spawn_worker(&closed, NoopLease, |_s| async {}),
        Err(InstallError::ClosedOrForbidden)
    ));
    assert!(matches!(
        owner.shutdown(Duration::from_secs(3600)).await,
        Ok(WorkerTerminal::Completed)
    ));
}

#[tokio::test(start_paused = true)]
async fn panic_terminal_follows_quarantine_and_supervisor_join() {
    let owner = Arc::new(RepoUnitLifecycle::new());
    spawn_worker(&owner, NoopLease, |_s| async {
        panic!("boom");
    })
    .unwrap();
    assert!(matches!(
        owner.shutdown(Duration::from_secs(3600)).await,
        Ok(WorkerTerminal::Failed(_))
    ));
    assert!(owner.quarantined() && owner.reopen_required() && owner.supervisor_joined());
    assert!(owner.interlock().is_terminated());
}

#[tokio::test(start_paused = true)]
async fn deadline_expiry_quarantines_and_retains_supervisor_until_join() {
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let (worker_entered, worker_release) = (Arc::clone(&entered), Arc::clone(&release));
    let owner = Arc::new(RepoUnitLifecycle::new());
    let gate = Arc::new(RepoTxGate::fresh());
    let lease = ReceiptLease::new(Arc::clone(&gate), owner.status());
    spawn_worker(&owner, lease, move |mut s| async move {
        let _job = s.next_job().await.unwrap();
        worker_entered.notify_one();
        worker_release.notified().await;
    })
    .unwrap();
    let _rx = owner.submit(()).unwrap();
    entered.notified().await;
    assert!(matches!(
        owner.shutdown(Duration::from_secs(3600)).await,
        Err(ShutdownError::TimedOut)
    ));
    assert!(owner.quarantined() && owner.reopen_required());
    assert!(gate.requires_reopen());
    assert!(owner.interlock().publish(|| ()).is_err());
    release.notify_one();
    assert!(matches!(
        owner.shutdown(Duration::from_secs(3600)).await,
        Ok(WorkerTerminal::Failed(message))
            if message.contains("worker shutdown deadline elapsed")
    ));
    assert!(owner.supervisor_joined());
}

#[tokio::test(start_paused = true)]
async fn cancelled_reply_receiver_does_not_cancel_processing() {
    let count = Arc::new(AtomicUsize::new(0));
    let worker_count = Arc::clone(&count);
    let owner = Arc::new(RepoWorkerLifecycle::<u64, u64>::new());
    spawn_worker(&owner, NoopLease, move |mut s| async move {
        while let Some(job) = s.next_job().await {
            worker_count.fetch_add(1, Ordering::SeqCst);
            let payload = job.payload;
            job.reply(payload);
        }
    })
    .unwrap();
    drop(owner.submit(7).unwrap());
    assert!(matches!(
        owner.shutdown(Duration::from_secs(3600)).await,
        Ok(WorkerTerminal::Completed)
    ));
    assert_eq!(count.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn queue_close_race_preserves_execution_fifo() {
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let (worker_entered, worker_release) = (Arc::clone(&entered), Arc::clone(&release));
    let order = Arc::new(Mutex::new(Vec::new()));
    let worker_order = Arc::clone(&order);
    let owner = Arc::new(RepoWorkerLifecycle::<u64, u64>::new());
    spawn_worker(&owner, NoopLease, move |mut s| async move {
        while let Some(job) = s.next_job().await {
            worker_order.lock().unwrap().push(job.payload);
            if job.payload == 0 {
                worker_entered.notify_one();
                worker_release.notified().await;
            }
            let p = job.payload;
            job.reply(p);
        }
    })
    .unwrap();
    let rx0 = owner.submit(0).unwrap();
    entered.notified().await;
    let rxs: Vec<_> = (1..5).map(|i| owner.submit(i).unwrap()).collect();
    owner.close();
    assert!(matches!(owner.submit(5), Err(JobSubmitError::Closing)));
    release.notify_one();
    rx0.await.unwrap();
    for (i, rx) in rxs.into_iter().enumerate() {
        assert_eq!(rx.await.unwrap(), (i + 1) as u64);
    }
    assert!(matches!(
        owner.shutdown(Duration::from_secs(3600)).await,
        Ok(WorkerTerminal::Completed)
    ));
    assert_eq!(*order.lock().unwrap(), vec![0, 1, 2, 3, 4]);
}

#[tokio::test(start_paused = true)]
async fn concurrent_shutdown_waiters_are_prepolled_pending_before_release() {
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let (worker_entered, worker_release) = (Arc::clone(&entered), Arc::clone(&release));
    let owner = Arc::new(RepoUnitLifecycle::new());
    spawn_worker(&owner, NoopLease, move |mut s| async move {
        let _ = s.next_job().await.unwrap();
        worker_entered.notify_one();
        worker_release.notified().await;
    })
    .unwrap();
    let _rx = owner.submit(()).unwrap();
    entered.notified().await;
    let mut waiters = [0, 1, 2, 3].map(|_| {
        let o = Arc::clone(&owner);
        Box::pin(async move { o.shutdown(Duration::from_secs(3600)).await })
    });
    for waiter in &mut waiters {
        assert!(waiter.as_mut().now_or_never().is_none());
    }
    release.notify_one();
    let [mut a, mut b, mut c, mut d] = waiters;
    let (a, b, c, d) = futures::join!(a.as_mut(), b.as_mut(), c.as_mut(), d.as_mut());
    for result in [a, b, c, d] {
        assert!(matches!(result, Ok(WorkerTerminal::Completed)));
    }
    assert!(owner.supervisor_joined());
}

#[tokio::test(start_paused = true)]
async fn concurrent_failure_and_different_deadlines_are_independent() {
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let (worker_entered, worker_release) = (Arc::clone(&entered), Arc::clone(&release));
    let owner = Arc::new(RepoUnitLifecycle::new());
    spawn_worker(&owner, NoopLease, move |mut s| async move {
        let _ = s.next_job().await.unwrap();
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
async fn concurrent_failed_waiters_and_later_calls_share_cached_terminal() {
    let release = Arc::new(Notify::new());
    let worker_release = Arc::clone(&release);
    let owner = Arc::new(RepoUnitLifecycle::new());
    spawn_worker(&owner, NoopLease, move |_state| async move {
        worker_release.notified().await;
        panic!("shared failure");
    })
    .unwrap();

    let mut waiters = [0, 1, 2].map(|_| {
        let owner = Arc::clone(&owner);
        Box::pin(async move { owner.shutdown(Duration::from_secs(3600)).await })
    });
    for waiter in &mut waiters {
        assert!(waiter.as_mut().now_or_never().is_none());
    }
    release.notify_one();

    let [mut first, mut second, mut third] = waiters;
    let (first, second, third) = futures::join!(first.as_mut(), second.as_mut(), third.as_mut());
    let terminal_message = match first {
        Ok(WorkerTerminal::Failed(message)) => message,
        other => panic!("expected cached failure, got {other:?}"),
    };
    for result in [second, third] {
        assert!(
            matches!(result, Ok(WorkerTerminal::Failed(message)) if message == terminal_message)
        );
    }
    assert!(owner.supervisor_joined());
    assert!(matches!(
        owner.shutdown(Duration::ZERO).await,
        Ok(WorkerTerminal::Failed(message)) if message == terminal_message
    ));
}

#[tokio::test(start_paused = true)]
async fn shutdown_waiter_cancellation_keeps_close_applied() {
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let (worker_entered, worker_release) = (Arc::clone(&entered), Arc::clone(&release));
    let owner = Arc::new(RepoUnitLifecycle::new());
    spawn_worker(&owner, NoopLease, move |mut s| async move {
        let _ = s.next_job().await.unwrap();
        worker_entered.notify_one();
        worker_release.notified().await;
    })
    .unwrap();
    let _rx = owner.submit(()).unwrap();
    entered.notified().await;
    let mut waiter = Box::pin(owner.shutdown(Duration::from_secs(3600)));
    assert!(waiter.as_mut().now_or_never().is_none());
    drop(waiter);
    assert!(!owner.admission_open() && !owner.quarantined());
    release.notify_one();
    assert!(matches!(
        owner.shutdown(Duration::from_secs(3600)).await,
        Ok(WorkerTerminal::Completed)
    ));
}

#[tokio::test(start_paused = true)]
async fn phase_matrix_notifies_each_next_phase_before_waiting() {
    for cancel_at in 0..3 {
        let phases = [
            Arc::new(Notify::new()),
            Arc::new(Notify::new()),
            Arc::new(Notify::new()),
        ];
        let gates = [
            Arc::new(Notify::new()),
            Arc::new(Notify::new()),
            Arc::new(Notify::new()),
        ];
        let done = Arc::new(Notify::new());
        let owner = Arc::new(RepoWorkerLifecycle::<(), ()>::new());
        let (p0, p1, p2) = (
            Arc::clone(&phases[0]),
            Arc::clone(&phases[1]),
            Arc::clone(&phases[2]),
        );
        let (g0, g1, g2) = (
            Arc::clone(&gates[0]),
            Arc::clone(&gates[1]),
            Arc::clone(&gates[2]),
        );
        let worker_done = Arc::clone(&done);
        let versions = Arc::new(Mutex::new(Vec::new()));
        let worker_versions = Arc::clone(&versions);
        let highwater = Arc::new(AtomicU64::new(0));
        let worker_highwater = Arc::clone(&highwater);
        let gate = Arc::new(RepoTxGate::fresh());
        let before_visible = gate.completion().watermark();
        let before_durable = gate.durable_watermark();
        let worker_gate = Arc::clone(&gate);
        spawn_worker(&owner, NoopLease, move |mut state| async move {
            let job = state.next_job().await.unwrap();
            let guard = worker_gate.assign_next_version_guarded();
            let version = guard.version();
            let _protected = state.protect_guard(guard);
            worker_versions.lock().unwrap().push(version);
            worker_highwater.fetch_max(version, Ordering::SeqCst);
            for (phase, phase_gate) in [(p0, g0), (p1, g1), (p2, g2)] {
                phase.notify_one();
                phase_gate.notified().await;
            }
            job.reply(());
            worker_done.notify_one();
        })
        .unwrap();
        let mut rx = Some(owner.submit(()).unwrap());
        let mut phase = 0;
        while phase < 3 {
            phases[phase].notified().await;
            if phase == cancel_at {
                drop(rx.take());
            }
            gates[phase].notify_one();
            phase += 1;
        }
        done.notified().await;
        assert!(matches!(
            owner.shutdown(Duration::from_secs(3600)).await,
            Ok(WorkerTerminal::Completed)
        ));
        assert_eq!(highwater.load(Ordering::SeqCst), 1);
        assert_eq!(versions.lock().unwrap().as_slice(), &[1]);
        assert_eq!(gate.completion().watermark(), before_visible);
        assert_eq!(gate.durable_watermark(), before_durable);
        assert!(gate.requires_reopen());
    }
}

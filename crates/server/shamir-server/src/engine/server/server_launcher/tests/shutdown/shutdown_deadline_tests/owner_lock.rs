//! Lock honesty of the OWNER drain (`ShutdownOwner::drain`) and the
//! one-path `ServerHandle::shutdown`.
//!
//! `owner_drain_expired_backup_join_keeps_the_lock` parks the REAL backup
//! runner past BOTH joins (the inline bounded join at the early cutoff AND
//! the drain-side fallback join, shrunk with the test-only bound override)
//! and asserts the abnormal outcome: the typed expired-stage entry in the
//! report, exit code 70, and the data-directory lock STILL HELD after
//! `drain_server` returned — and still held once the worker later finishes,
//! because the lock `File` is leaked until the process exits. Mutants: a
//! drain that releases the lock unconditionally, and a drain that does not
//! record the expired stage.
//!
//! `clean_owner_drain_releases_the_lock` is the positive case: no expiry, no
//! error, lock free after the drain. Mutant: the lock leaked unconditionally.
//!
//! `release_sources_stop_before_the_repository_flush` orders the ONE
//! release-source stop against the REAL flush through the one-shot probes
//! (no timing): the registry-cleared marker must fire and precede the
//! sources-stopped marker, which must precede the flush marker. Mutants: the
//! helper moved after the flush, or the registry clearing removed while the
//! markers stay in place.
//!
//! `cancelled_shutdown_leaks_the_lock_until_process_exit` drives the PUBLIC
//! `ServerHandle::shutdown()` and ABORTS the spawned future while a scheduled
//! backup job is parked (a deterministic point — the scheduled-join probe —
//! where the worker is parked and the owner holds the lock): the aborted
//! future drops the owner, whose `DataDirLock` LEAKS the file, so the lock is
//! still held after the abort AND still held after the worker later exits
//! (observed through the runner's test-only `on_finished` handshake). Mutant:
//! a plain `Drop` that releases the file (the old shape), or a shutdown that
//! takes the lock into the future.
//!
//! No sleeps: the worker rendezvous is the semaphore + condvar gate from the
//! deadline tests, every await is bounded by [`BOUND`], and the joins under
//! test are shrunk with the `cfg(test)` bound overrides.

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::Semaphore;

use shamir_db::ShutdownWaitStage;

use super::*;

use crate::boot::config::diagnostics::backup_online::schedule::runner::spawn_for_test;
use crate::server::server_handle::{
    arm_registry_cleared_probe, arm_release_sources_stopped_probe, arm_repo_flush_probe,
    arm_scheduled_join_probe, set_drain_backup_join_bound_for_test,
};

/// Restores the `cfg(test)` overrides even when an assertion fails, so a
/// failing test can never shrink the bounds of the next test in the same
/// process (nextest runs tests in separate processes; this is belt and
/// braces for `cargo test` runs).
struct BoundOverrideGuard;

impl Drop for BoundOverrideGuard {
    fn drop(&mut self) {
        set_drain_backup_join_bound_for_test(None);
        crate::runtime::set_drain_deadline_override(None);
    }
}

/// Shared skeleton: launch a real server whose scheduled backup parks inside
/// its post-run callback (before the completion signal) and return the
/// handles the tests use to observe and release it.
async fn launch_with_parked_worker(
    data_dir: std::path::PathBuf,
    backups: std::path::PathBuf,
    gate: Arc<(Mutex<WorkerState>, std::sync::Condvar)>,
    parked: Arc<Semaphore>,
    completed: Arc<AtomicBool>,
    on_finished: impl FnMut() + Send + 'static,
) -> crate::server::ServerHandle {
    ServerLauncher::new(backup_config(data_dir, backups))
        .with_bootstrap(BootstrapMode::Skip)
        .launch_with_backup_test(move |resolved, registry, owner, shutdown| {
            registry.arm_post_run_callback_for_shutdown_test({
                let parked = Arc::clone(&parked);
                let gate = Arc::clone(&gate);
                move |_published: &Path| {
                    parked.add_permits(1);
                    wait_until(&gate, "worker release", |state| state.released);
                    None
                }
            });
            spawn_for_test(
                resolved,
                registry,
                owner,
                shutdown.child_token(),
                Duration::from_secs(3600),
                move |ok| completed.store(ok, Ordering::SeqCst),
                on_finished,
            )
        })
        .await
        .expect("launch real server with the scheduled backup worker")
}

/// The drain-side fallback backup join expired: typed report entry, exit 70,
/// and the data-directory lock kept until the process exits.
#[test]
fn owner_drain_expired_backup_join_keeps_the_lock() {
    on_big_stack(|| async {
        timeout(BOUND, run_expired_drain_join())
            .await
            .expect("expired-drain-join test bounded");
    });
}

async fn run_expired_drain_join() {
    let guard = FlakeGuard::with_watchdog(
        "owner_drain_expired_backup_join_keeps_the_lock",
        Duration::from_secs(300),
    );
    let _overrides = BoundOverrideGuard;
    try_install_global_tracing_ring();
    let temp = tempfile::tempdir().expect("test root");
    let data_dir = temp.path().join("data");
    let backups = temp.path().join("backups");
    create_private_dir(&data_dir).expect("private data directory");
    create_private_dir(&backups).expect("private backup directory");

    let parked = Arc::new(Semaphore::new(0));
    let gate = Arc::new((
        Mutex::new(WorkerState::default()),
        std::sync::Condvar::new(),
    ));
    let completed = Arc::new(AtomicBool::new(false));
    // Completion handshake for the LATE worker-finished signal (never a
    // sleep/poll loop): fires when the runner clears the in-flight slot on
    // `Signal::Finished` — after retention and the final publication.
    let (finished_tx, finished_rx) = tokio::sync::oneshot::channel::<()>();
    let mut finished_tx = Some(finished_tx);
    let handle = launch_with_parked_worker(
        data_dir.clone(),
        backups,
        Arc::clone(&gate),
        Arc::clone(&parked),
        Arc::clone(&completed),
        move || {
            if let Some(tx) = finished_tx.take() {
                let _ = tx.send(());
            }
        },
    )
    .await;
    let parked_permit = timeout(BOUND, parked.acquire())
        .await
        .expect("the scheduled run must reach its post-run callback")
        .expect("parked semaphore open");
    drop(parked_permit);

    // The drain-side fallback join is shrunk to 1 s: `drain` must stop
    // waiting then, record the typed expired stage, and keep the lock. The
    // outer deadline (3 s) bounds the INLINE join at its early cutoff, so
    // the drain below is reached with the worker still parked.
    set_drain_backup_join_bound_for_test(Some(Duration::from_secs(1)));
    let report = timeout(BOUND, crate::runtime::drain_server(handle, TEST_DEADLINE))
        .await
        .expect("drain_server must return even with the worker parked");
    guard.mark("drain returned with the worker still parked");

    assert_eq!(
        report.expired_owner_wait(),
        Some(ShutdownWaitStage::ScheduledBackupJoin),
        "the expired drain-side backup join must be recorded TYPED, naming the stage: {report:?}"
    );
    assert!(
        report
            .errors()
            .iter()
            .any(|error| error.contains("KEPT until the process exits")),
        "the operator-facing text must say the lock is kept until the process exits: {report:?}"
    );
    assert_eq!(
        report.exit_code(),
        70,
        "the report must be abnormal: {report:?}"
    );
    // THE discriminator: the worker is STILL parked, drain_server has
    // returned, and the lock must STILL be held. (Mutant: a drain that
    // releases the lock in every case fails here.)
    let lock_still_held = crate::maintenance_lock::acquire_server(&data_dir).is_err();
    assert!(
        lock_still_held,
        "an expired owner wait must keep the data-directory lock after the drain returned"
    );
    // Let the runner finish: the lock is LEAKED, so it must stay held even
    // after the job is no longer live (kernel releases it only at process
    // exit; a second acquire must still fail). The worker's exit is observed
    // through the runner's finished handshake, never by polling.
    release(&gate);
    timeout(BOUND, finished_rx)
        .await
        .expect("the worker must finish after its release (finished handshake)")
        .expect("finished channel");
    assert!(
        crate::maintenance_lock::acquire_server(&data_dir).is_err(),
        "the lock file was leaked on purpose: the kernel must hold it until the process exits"
    );
}

/// A clean drain (no expiry) still releases the lock.
#[test]
fn clean_owner_drain_releases_the_lock() {
    on_big_stack(|| async {
        timeout(BOUND, run_clean_release())
            .await
            .expect("clean-release test bounded");
    });
}

async fn run_clean_release() {
    let _guard = FlakeGuard::with_watchdog(
        "clean_owner_drain_releases_the_lock",
        Duration::from_secs(300),
    );
    try_install_global_tracing_ring();
    let temp = tempfile::tempdir().expect("test root");
    let data_dir = temp.path().join("data");
    create_private_dir(&data_dir).expect("private data directory");
    let handle = timeout(
        BOUND,
        ServerLauncher {
            config: bind_config(
                data_dir.clone(),
                "127.0.0.1:0".to_owned(),
                "127.0.0.1:0".to_owned(),
            ),
            bootstrap: BootstrapMode::Skip,
            node_mode: NodeMode::ReadWrite,
        }
        .launch(),
    )
    .await
    .expect("launch real server")
    .expect("server bound");
    let report = timeout(BOUND, crate::runtime::drain_server(handle, BOUND))
        .await
        .expect("clean drain must finish");
    assert!(
        report.is_clean(),
        "a clean drain must produce a CLEAN report, not merely one without the expired \
         owner-wait entry: {report:?}"
    );
    let lock_free = crate::maintenance_lock::acquire_server(&data_dir).is_ok();
    assert!(
        lock_free,
        "a clean drain must release the data-directory lock (mutant: the lock leaked \
         unconditionally)"
    );
}

/// The release sources stop BEFORE the repository flush, observed through the
/// two one-shot probes (never through timing).
#[test]
fn release_sources_stop_before_the_repository_flush() {
    on_big_stack(|| async {
        timeout(BOUND, run_sources_before_flush())
            .await
            .expect("sources-before-flush test bounded");
    });
}

async fn run_sources_before_flush() {
    let _guard = FlakeGuard::with_watchdog(
        "release_sources_stop_before_the_repository_flush",
        Duration::from_secs(300),
    );
    try_install_global_tracing_ring();
    let temp = tempfile::tempdir().expect("test root");
    let data_dir = temp.path().join("data");
    let backups = temp.path().join("backups");
    create_private_dir(&data_dir).expect("private data directory");
    create_private_dir(&backups).expect("private backup directory");

    // No parking: the run-on-start backup completes on its own, so the
    // shutdown takes the CLEAN path where the flush really runs.
    let handle = timeout(
        BOUND,
        ServerLauncher::new(backup_config(data_dir, backups))
            .with_bootstrap(BootstrapMode::Skip)
            .launch(),
    )
    .await
    .expect("launch real server with backup")
    .expect("server bound");

    let order = Arc::new(Mutex::new(Vec::<&'static str>::new()));
    let (sources_tx, sources_rx) = tokio::sync::oneshot::channel::<()>();
    let (cleared_tx, cleared_rx) = tokio::sync::oneshot::channel::<()>();
    let (flush_tx, flush_rx) = tokio::sync::oneshot::channel::<()>();
    let _cleared_probe = {
        let order = Arc::clone(&order);
        arm_registry_cleared_probe(move || {
            order.lock().expect("order log").push("cleared");
            let _ = cleared_tx.send(());
        })
    };
    let _sources_probe = {
        let order = Arc::clone(&order);
        arm_release_sources_stopped_probe(move || {
            order.lock().expect("order log").push("sources");
            let _ = sources_tx.send(());
        })
    };
    let _flush_probe = {
        let order = Arc::clone(&order);
        arm_repo_flush_probe(move || {
            order.lock().expect("order log").push("flush");
            let _ = flush_tx.send(());
        })
    };

    let report = timeout(BOUND, crate::runtime::drain_server(handle, BOUND))
        .await
        .expect("shutdown must finish");
    timeout(BOUND, cleared_rx)
        .await
        .expect(
            "the registry clearing must run on a shutdown (mutant: remove_all removed \
                 while the markers stay)",
        )
        .expect("cleared probe channel");
    timeout(BOUND, sources_rx)
        .await
        .expect("the release-source stop must run on a shutdown")
        .expect("sources probe channel");
    timeout(BOUND, flush_rx)
        .await
        .expect("the flush stage must run on a clean shutdown")
        .expect("flush probe channel");

    let order = order.lock().expect("order log").clone();
    let cleared_at = order
        .iter()
        .position(|marker| *marker == "cleared")
        .expect("registry-cleared marker recorded");
    let sources_at = order
        .iter()
        .position(|marker| *marker == "sources")
        .expect("sources marker recorded");
    let flush_at = order
        .iter()
        .position(|marker| *marker == "flush")
        .expect("flush marker recorded");
    assert!(
        cleared_at < sources_at,
        "the registry must be cleared BEFORE the sources-stopped marker (observed order \
         {order:?}; mutant: the registry clearing removed while the marker stays in place)"
    );
    assert!(
        sources_at < flush_at,
        "the release sources must stop BEFORE the flush (observed order {order:?}; mutant: the \
         helper moved after the flush)"
    );
    assert!(
        report.repository_failures().is_empty(),
        "the clean-path flush must succeed: {:?}",
        report.repository_failures()
    );
}

/// The PUBLIC `ServerHandle::shutdown()` CANCELLED mid-teardown: the aborted
/// future drops the owner, whose `DataDirLock` leaks the lock file, so the
/// data-directory lock stays held over the live job — and stays held even
/// after the worker later exits (the OS releases it only at process death).
#[test]
fn cancelled_shutdown_leaks_the_lock_until_process_exit() {
    on_big_stack(|| async {
        timeout(BOUND, run_cancelled_shutdown_keeps_lock())
            .await
            .expect("cancelled-shutdown test bounded");
    });
}

async fn run_cancelled_shutdown_keeps_lock() {
    let guard = FlakeGuard::with_watchdog(
        "cancelled_shutdown_leaks_the_lock_until_process_exit",
        Duration::from_secs(300),
    );
    let _overrides = BoundOverrideGuard;
    try_install_global_tracing_ring();
    let temp = tempfile::tempdir().expect("test root");
    let data_dir = temp.path().join("data");
    let backups = temp.path().join("backups");
    create_private_dir(&data_dir).expect("private data directory");
    create_private_dir(&backups).expect("private backup directory");

    let parked = Arc::new(Semaphore::new(0));
    let gate = Arc::new((
        Mutex::new(WorkerState::default()),
        std::sync::Condvar::new(),
    ));
    let completed = Arc::new(AtomicBool::new(false));
    let (finished_tx, finished_rx) = tokio::sync::oneshot::channel::<()>();
    let mut finished_tx = Some(finished_tx);
    let handle = launch_with_parked_worker(
        data_dir.clone(),
        backups,
        Arc::clone(&gate),
        Arc::clone(&parked),
        Arc::clone(&completed),
        move || {
            if let Some(tx) = finished_tx.take() {
                let _ = tx.send(());
            }
        },
    )
    .await;
    let parked_permit = timeout(BOUND, parked.acquire())
        .await
        .expect("the scheduled run must reach its post-run callback")
        .expect("parked semaphore open");
    drop(parked_permit);

    let (join_tx, join_rx) = tokio::sync::oneshot::channel::<()>();
    let _join_probe = {
        let _ = &join_tx;
        arm_scheduled_join_probe(move || {
            let _ = join_tx.send(());
        })
    };
    let shutdown = tokio::spawn(handle.shutdown());
    timeout(BOUND, join_rx)
        .await
        .expect("shutdown() must reach the scheduled-join stage (the drain_server path)")
        .expect("join probe channel");
    // Deterministic point: the worker is parked, the owner holds the lock.
    // ABORT the shutdown future — exactly what a cancelled embedding caller
    // does. The abort is awaited only to observe `is_cancelled`.
    shutdown.abort();
    let result = timeout(BOUND, shutdown)
        .await
        .expect("aborted task must join");
    assert!(
        matches!(&result, Err(error) if error.is_cancelled()),
        "the shutdown future must have been CANCELLED, not completed: {result:?}"
    );
    guard.mark("shutdown future aborted at the scheduled-join stage");

    // The aborted future dropped the owner; the lock must STILL be held over
    // the live job. (Mutant: a plain `Drop` that releases the file — the old
    // shape — or a shutdown that moved the lock into the future.)
    assert!(
        crate::maintenance_lock::acquire_server(&data_dir).is_err(),
        "a cancelled shutdown future must NOT release the data-directory lock over a live job"
    );
    // Let the worker finish (observed through the runner's finished
    // handshake), then assert the lock is STILL held AFTER the worker exited:
    // the file was leaked on purpose, so the kernel keeps it until the
    // process exits.
    release(&gate);
    timeout(BOUND, finished_rx)
        .await
        .expect("the worker must finish after its release (finished handshake)")
        .expect("finished channel");
    guard.mark("worker exited after the cancelled shutdown");
    assert!(
        crate::maintenance_lock::acquire_server(&data_dir).is_err(),
        "the lock file was leaked on purpose: the kernel must hold it even after the worker \
         exited, until the process exits"
    );
}

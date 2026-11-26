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
//! release-source stop against the REAL flush through the two one-shot
//! probes (no timing): the sources-stopped marker must fire before the
//! flush marker. Mutant: the helper moved after the flush.
//!
//! `public_shutdown_uses_the_owner_drain_and_keeps_the_lock_over_a_live_job`
//! drives the PUBLIC `ServerHandle::shutdown()` with a parked worker and
//! shrunk overrides: it must return only through the owner drain (reporting
//! the expired fallback join) and must not release the lock while the job is
//! live. Mutant: `shutdown()` reverting to its old shape (build an owner,
//! run the teardown, DROP the owner without draining — it could not return
//! here at all with the worker parked, and its old deadline-wrapped callers
//! released the lock over a live backup).
//!
//! No sleeps: the worker rendezvous is the semaphore + condvar gate from the
//! deadline tests, every await is bounded by [`BOUND`], and the joins under
//! test are shrunk with the `cfg(test)` bound overrides.

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::Semaphore;

use super::*;

use crate::boot::config::diagnostics::backup_online::schedule::runner::spawn_for_test;
use crate::server::server_handle::{
    arm_release_sources_stopped_probe, arm_repo_flush_probe, arm_scheduled_join_probe,
    set_drain_backup_join_bound_for_test,
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
    let handle = launch_with_parked_worker(
        data_dir.clone(),
        backups,
        Arc::clone(&gate),
        Arc::clone(&parked),
        Arc::clone(&completed),
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

    assert!(
        report.errors().iter().any(|error| {
            error.contains("shutdown owner wait expired (")
                && error.contains("scheduled backup worker join")
        }),
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
    // exit; a second acquire must still fail).
    release(&gate);
    let finished = Instant::now();
    while !completed.load(Ordering::SeqCst) {
        assert!(
            finished.elapsed() < BOUND,
            "the worker must finish after its release"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
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
        !report
            .errors()
            .iter()
            .any(|error| error.contains("shutdown owner wait expired (")),
        "a clean drain must not record an expired owner wait: {report:?}"
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
    let (flush_tx, flush_rx) = tokio::sync::oneshot::channel::<()>();
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
    timeout(BOUND, sources_rx)
        .await
        .expect("the release-source stop must run on a shutdown")
        .expect("sources probe channel");
    timeout(BOUND, flush_rx)
        .await
        .expect("the flush stage must run on a clean shutdown")
        .expect("flush probe channel");

    let order = order.lock().expect("order log").clone();
    let sources_at = order
        .iter()
        .position(|marker| *marker == "sources")
        .expect("sources marker recorded");
    let flush_at = order
        .iter()
        .position(|marker| *marker == "flush")
        .expect("flush marker recorded");
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

/// The PUBLIC `ServerHandle::shutdown()` runs the owner-drain path: with the
/// worker parked and both bounds shrunk it returns through the fallback drain
/// and keeps the lock over the live job.
#[test]
fn public_shutdown_uses_the_owner_drain_and_keeps_the_lock_over_a_live_job() {
    on_big_stack(|| async {
        timeout(BOUND, run_public_shutdown_over_live_job())
            .await
            .expect("public-shutdown test bounded");
    });
}

async fn run_public_shutdown_over_live_job() {
    let guard = FlakeGuard::with_watchdog(
        "public_shutdown_uses_the_owner_drain_and_keeps_the_lock_over_a_live_job",
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
    let handle = launch_with_parked_worker(
        data_dir.clone(),
        backups,
        Arc::clone(&gate),
        Arc::clone(&parked),
        Arc::clone(&completed),
    )
    .await;
    let parked_permit = timeout(BOUND, parked.acquire())
        .await
        .expect("the scheduled run must reach its post-run callback")
        .expect("parked semaphore open");
    drop(parked_permit);

    set_drain_backup_join_bound_for_test(Some(Duration::from_secs(1)));
    crate::runtime::set_drain_deadline_override(Some(TEST_DEADLINE));
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
    let report = timeout(BOUND, shutdown)
        .await
        .expect("shutdown() must return even with the worker parked")
        .expect("shutdown task");

    assert!(
        report
            .errors()
            .iter()
            .any(|error| error.contains("shutdown owner wait expired (")),
        "shutdown() must run the owner drain and report the expired fallback join: {report:?}"
    );
    // The job is LIVE (still parked): the lock must still be held after
    // shutdown() returned. (Mutant: the old shape — teardown + drop the
    // owner — released the lock over the live backup.)
    assert!(
        crate::maintenance_lock::acquire_server(&data_dir).is_err(),
        "shutdown() must not release the data-directory lock over a live job"
    );
    release(&gate);
    guard.mark("worker released after shutdown() returned");
}

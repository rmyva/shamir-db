//! Real-server acceptance for the SCHEDULED backup lifecycle: the PRODUCTION
//! spawn/join path and its token binding, the shutdown that waits for a worker
//! parked inside retention, and the retained/pruned destination with its
//! metrics.
//!
//! Nothing synchronises with a sleep: every wait is bounded by [`BOUND`] and
//! observes the scheduler's or the retention pass's OWN signal.

use std::collections::BTreeSet;
use std::ffi::OsString;
use std::fs;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};

use shamir_private_dir::create_private_dir;
use shamir_test_diag::{try_install_global_tracing_ring, FlakeGuard};
use tokio::sync::{mpsc, Semaphore};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use crate::boot::config::diagnostics::backup_online::metrics::backup_metrics_snapshot;
use crate::boot::config::diagnostics::backup_online::registry::arm_scheduled_retention_pause;
use crate::boot::config::diagnostics::backup_online::schedule::runner::{spawn, spawn_for_test};
use crate::boot::config::diagnostics::backup_online::OnlineBackupRegistry;
use crate::config::ResolvedSchedule;
use crate::server::server_handle::arm_scheduled_join_probe;
use crate::server::{BootstrapMode, ServerLauncher};
use crate::sync::durability::backup::online::set_snapshot_name_base_seconds;
use crate::sync::durability::backup::{
    verify_manifest, MANIFEST_FILE_NAME, SNAPSHOT_LEASE_FILE_NAME,
};

use super::support::{
    arm_run_observer, on_big_stack, optional_sample, release_worker, sample, scheduled_config,
    scrape_metrics, wait_for_release, WorkerGate, BOUND, OUTER, PARKED_PROBE, PENDING_WINDOW,
    SNAPSHOT_NAME_BASE, TICK, WATCHDOG,
};

const TEST_NAME: &str = "scheduled_backups_verify_prune_and_export_metrics_on_a_real_server";
const PRODUCTION_SPAWN_TEST: &str =
    "production_spawn_joins_the_parked_scheduler_and_binds_the_root_token";

#[test]
fn scheduled_backups_verify_prune_and_export_metrics_on_a_real_server() {
    on_big_stack(|| async {
        timeout(OUTER, run())
            .await
            .expect("scheduled backup acceptance timed out");
    });
}

async fn run() {
    let guard = FlakeGuard::with_watchdog(TEST_NAME, WATCHDOG);
    try_install_global_tracing_ring();
    set_snapshot_name_base_seconds(SNAPSHOT_NAME_BASE);
    let temp = tempfile::tempdir().expect("test root");
    let data_dir = temp.path().join("data");
    let backups = temp.path().join("backups");
    create_private_dir(&data_dir).expect("private data directory");
    create_private_dir(&backups).expect("private backup directory");

    let foreign = backups.join("foreign");
    let invalid = backups.join("20200101_000001");
    fs::create_dir(&foreign).expect("foreign directory");
    fs::create_dir(&invalid).expect("invalid snapshot directory");
    let foreign_payload = foreign.join("keep.txt");
    let invalid_manifest = invalid.join(MANIFEST_FILE_NAME);
    fs::write(&foreign_payload, b"foreign payload").expect("foreign payload");
    fs::write(&invalid_manifest, b"invalid manifest").expect("invalid manifest");
    assert!(verify_manifest(&invalid).is_err());

    let config = scheduled_config(&data_dir, &backups, "1m", true, 1);

    let baseline = backup_metrics_snapshot();
    let (send, mut receive) = mpsc::unbounded_channel();
    let (published_tx, mut published_rx) = mpsc::unbounded_channel::<OsString>();
    let (retention_end_tx, mut retention_end_rx) = mpsc::unbounded_channel::<()>();
    let handle = ServerLauncher::new(config)
        .with_bootstrap(BootstrapMode::Skip)
        .launch_with_backup_test(|resolved, registry, owner, shutdown| {
            // Boot provisioning (R2d-FIX-C) happens BEFORE the schedule
            // spawn: the marker must already be a regular file here.
            let boot_marker = resolved.destination.join(SNAPSHOT_LEASE_FILE_NAME);
            let marker_meta =
                fs::symlink_metadata(&boot_marker).expect("lease marker provisioned at boot");
            assert!(
                marker_meta.is_file(),
                "lease marker must be a regular file, not a link or directory"
            );
            let scheduler_shutdown = shutdown.child_token();
            // Retention-END handshake + published-name reporting for run 1;
            // the outcome observer re-arms it for every following run.
            arm_run_observer(&registry, Some(&published_tx), &retention_end_tx);
            let observer_registry = Arc::clone(&registry);
            let observer_published = published_tx.clone();
            let observer_ends = retention_end_tx.clone();
            let mut successes = 0;
            // The schedule's own interval is replaced by the injected
            // millisecond tick: the fixture never waits for a wall-clock
            // second, and each run is awaited through its terminal signal.
            spawn_for_test(
                resolved,
                registry,
                owner,
                scheduler_shutdown.clone(),
                TICK,
                move |ok| {
                    arm_run_observer(
                        &observer_registry,
                        Some(&observer_published),
                        &observer_ends,
                    );
                    successes += usize::from(ok);
                    if successes == 2 {
                        scheduler_shutdown.cancel();
                    }
                    // Unbounded: a dropped outcome would hang the wait below.
                    let _ = send.send(ok);
                },
                || {},
            )
        })
        .await
        .expect("launch real server with backup scheduler");
    guard.mark("server launched");

    let addr = handle
        .observability
        .as_ref()
        .expect("observability")
        .bound_addr;
    // The self-observed "run N-1" stamp; no `SystemTime::now()` oracle.
    let mut first_success_stamp = 0;
    for index in 0..2 {
        let outcome = timeout(BOUND, receive.recv())
            .await
            .expect("every scheduled terminal signal must arrive within the bound")
            .expect("the scheduled worker must keep its terminal channel open");
        assert!(outcome, "scheduled copy must succeed");
        if index == 0 {
            let first_scrape = scrape_metrics(addr).await;
            first_success_stamp = sample(
                &first_scrape,
                "shamir_backup_last_success_timestamp_seconds",
            );
            // The copy outcome, success cell included, is published BEFORE
            // retention; the boot seed fills only the stamp cell, so this
            // duration series proves a real success of this process (mutant:
            // `note_success` never publishing the last-success cell).
            let first_copy_seconds =
                optional_sample(&first_scrape, "shamir_backup_duration_seconds")
                    .expect("the first scheduled success must export the duration series");
            assert!(
                first_copy_seconds > 0.0,
                "the first scheduled success must publish a nonzero duration: {first_scrape}"
            );
        }
        guard.mark("scheduled copy outcome reported (retention follows)");
    }
    // Runs are admitted only after the previous run's `finished` (retention
    // included), so the SECOND retention END is the LAST run's and everything
    // below observes the settled retention. No sleep, no shutdown handoff; the
    // published names arrive in run order.
    let mut snapshots = Vec::with_capacity(2);
    for _ in 0..2 {
        snapshots.push(
            timeout(BOUND, published_rx.recv())
                .await
                .expect("every published snapshot name must arrive within the bound")
                .expect("the run observer channel must stay open"),
        );
        timeout(BOUND, retention_end_rx.recv())
            .await
            .expect("every retention pass must end within the bound")
            .expect("the retention-end channel must stay open");
    }
    let metrics = scrape_metrics(addr).await;
    assert_eq!(
        sample(&metrics, "shamir_backup_retention_deleted_total"),
        baseline.retention_deleted_total + 1,
        "exactly the one snapshot beyond keep_last = 1 must be deleted"
    );
    assert_eq!(
        sample(&metrics, "shamir_backup_retention_untouched"),
        2,
        "the foreign and invalid entries must be counted untouched on the real route"
    );

    let completed = sample(
        &metrics,
        "shamir_backup_scheduled_runs_total{result=\"completed\"}",
    );
    assert!(completed >= baseline.scheduled_runs_total[4] + 2);
    // Advancement, not a wall-clock comparison: self-observed stamps only.
    let last_success = sample(&metrics, "shamir_backup_last_success_timestamp_seconds");
    assert!(
        last_success >= first_success_stamp,
        "the last-success stamp must not regress below the value observed after the first run \
         (self-observed values, no wall-clock oracle): first={first_success_stamp} \
         last={last_success}"
    );
    assert!(sample(&metrics, "shamir_backup_last_bytes") > 0);

    assert_eq!(
        fs::read(&foreign_payload).expect("foreign payload"),
        b"foreign payload"
    );
    assert_eq!(
        fs::read(&invalid_manifest).expect("invalid manifest"),
        b"invalid manifest"
    );
    // Filesystem: only the last `keep_last = 1` snapshots survive.
    let non_snapshots = [
        foreign.file_name().expect("foreign name").to_owned(),
        invalid.file_name().expect("invalid name").to_owned(),
        OsString::from(SNAPSHOT_LEASE_FILE_NAME),
    ];
    let retained: BTreeSet<OsString> = fs::read_dir(&backups)
        .expect("read backup root")
        .map(|entry| entry.expect("backup entry").file_name())
        .filter(|name| !non_snapshots.contains(name))
        .collect();
    assert_eq!(
        retained,
        BTreeSet::from([snapshots[1].clone()]),
        "exactly the last keep_last = 1 scheduled snapshots must survive"
    );
    assert!(
        !backups.join(&snapshots[0]).exists(),
        "the oldest scheduled snapshot must be pruned"
    );
    verify_manifest(&backups.join(&snapshots[1])).expect("retained snapshot verifies");

    timeout(BOUND, handle.shutdown())
        .await
        .expect("shutdown must finish within the bound");
    guard.mark("server stopped");
}

// ---------------------------------------------------------------------------
// (a) PRODUCTION spawn path + PRODUCTION cancellation token
// ---------------------------------------------------------------------------

/// State the test hands to the launcher's spawn position. The spawn argument
/// is a plain FUNCTION ITEM, so the state travels through this slot.
struct ProductionSpawnBridge {
    /// Signalled from the runner thread once the real run is parked.
    parked: Arc<Semaphore>,
    /// Released by the test; the runner thread blocks on it.
    gate: Arc<(Mutex<WorkerGate>, Condvar)>,
    /// Set by the runner thread AFTER the release: the worker's cleanup is
    /// observable only through this flag.
    resumed: Arc<AtomicBool>,
    /// The registry the launcher created (a run's cancel flag derives from
    /// the launcher's root token, not from any token of the test's).
    registry: Mutex<Option<Arc<OnlineBackupRegistry>>>,
}

static PRODUCTION_SPAWN_BRIDGE: Mutex<Option<Arc<ProductionSpawnBridge>>> = Mutex::new(None);

/// The `launch_with_backup_test` spawn argument: record the launcher's registry,
/// arm the one-shot post-run park on it, then call the PRODUCTION
/// `schedule::runner::spawn` with the launcher's four arguments unchanged —
/// only the arming step is inserted, because it needs the registry.
fn arm_then_spawn_production_worker(
    schedule: ResolvedSchedule,
    registry: Arc<OnlineBackupRegistry>,
    owner: String,
    shutdown: CancellationToken,
) -> JoinHandle<()> {
    let bridge = PRODUCTION_SPAWN_BRIDGE
        .lock()
        .expect("production spawn bridge")
        .clone()
        .expect("the test installs the bridge before launching");
    *bridge.registry.lock().expect("registry slot") = Some(Arc::clone(&registry));
    let parked = Arc::clone(&bridge.parked);
    let gate = Arc::clone(&bridge.gate);
    let resumed = Arc::clone(&bridge.resumed);
    registry.arm_post_run_callback_for_shutdown_test(move |_published| {
        parked.add_permits(1);
        wait_for_release(&gate);
        // Set the flag from the RETENTION pass, which starts only AFTER this
        // callback returned: it means the worker resumed past the park.
        let resumed = Arc::clone(&resumed);
        let continuation = arm_scheduled_retention_pause(move || {
            resumed.store(true, Ordering::SeqCst);
        });
        std::mem::forget(continuation);
        None
    });
    spawn(schedule, registry, owner, shutdown)
}

#[test]
fn production_spawn_joins_the_parked_scheduler_and_binds_the_root_token() {
    on_big_stack(|| async {
        timeout(OUTER, run_production_spawn())
            .await
            .expect("production scheduled-worker acceptance timed out");
    });
}

async fn run_production_spawn() {
    let guard = FlakeGuard::with_watchdog(PRODUCTION_SPAWN_TEST, WATCHDOG);
    try_install_global_tracing_ring();
    let temp = tempfile::tempdir().expect("test root");
    let data_dir = temp.path().join("data");
    let backups = temp.path().join("backups");
    create_private_dir(&data_dir).expect("private data directory");
    create_private_dir(&backups).expect("private backup directory");

    let bridge = Arc::new(ProductionSpawnBridge {
        parked: Arc::new(Semaphore::new(0)),
        gate: Arc::new((Mutex::new(WorkerGate::default()), Condvar::new())),
        resumed: Arc::new(AtomicBool::new(false)),
        registry: Mutex::new(None),
    });
    *PRODUCTION_SPAWN_BRIDGE
        .lock()
        .expect("production spawn bridge") = Some(Arc::clone(&bridge));

    let handle = ServerLauncher::new(scheduled_config(&data_dir, &backups, "1h", true, 2))
        .with_bootstrap(BootstrapMode::Skip)
        .launch_with_backup_test(arm_then_spawn_production_worker)
        .await
        .expect("launch a real server through the production scheduled-worker spawn");
    guard.mark("launched through the production spawn");

    // `run_on_start` starts the run by itself; it parks in its post-run
    // callback, before its completion signal, and awaiting keeps the runtime driving.
    let permit = timeout(BOUND, bridge.parked.acquire())
        .await
        .expect("the production scheduled run must reach the parked handshake")
        .expect("parked semaphore open");
    drop(permit);
    guard.mark("production worker parked before its completion signal");

    let registry = bridge
        .registry
        .lock()
        .expect("registry slot")
        .clone()
        .expect("the launch callback recorded the launcher's registry");
    // The launcher's ROOT token, cloned only to know WHEN the root cancel happened.
    let root = handle.shutdown_token.clone();
    // Fires from INSIDE `ShutdownOwner::join_schedule`, right before the await
    // of the scheduled worker, so the pending window below starts AT the join
    // instead of at whatever point of the earlier teardown stages the runtime
    // happens to be at.
    let (join_reached_tx, mut join_reached_rx) = mpsc::unbounded_channel::<()>();
    let join_probe = arm_scheduled_join_probe(move || {
        let _ = join_reached_tx.send(());
    });
    let mut shutdown = tokio::spawn(handle.shutdown());
    timeout(BOUND, root.cancelled())
        .await
        .expect("handle.shutdown() must cancel the launcher's root shutdown token");
    timeout(BOUND, join_reached_rx.recv())
        .await
        .expect("shutdown must reach the scheduled join within the bound")
        .expect("the join probe channel must stay open");
    guard.mark("shutdown reached the scheduled join");
    let parked_status = registry.status();
    guard.mark("root shutdown requested, worker still parked");

    // The run's `CancelFlag` derives from the ROOT token at admission, so the
    // root cancel flips it while the worker is parked. Mutant: a fresh token
    // instead of the launcher's root token leaves it clear.
    assert!(
        parked_status.running,
        "the parked run must still be registered: {parked_status:?}"
    );
    assert!(
        parked_status.cancel_requested,
        "the parked run's cancel flag must be the launcher's ROOT token, so the root \
         cancel sets it while the worker is still parked (mutant: a fresh token \
         instead of the root token leaves it clear): {parked_status:?}"
    );
    assert!(
        !bridge.resumed.load(Ordering::SeqCst),
        "the worker must still be parked after the root cancel"
    );

    // The probe runs INSIDE the join, so this window can no longer pass
    // vacuously on a slow runner (it used to start before the join was even
    // reached).
    let still_pending = timeout(PENDING_WINDOW, &mut shutdown).await.is_err();
    guard.mark("shutdown stayed pending past the pending window");

    release_worker(&bridge.gate);
    let report = timeout(BOUND, shutdown)
        .await
        .expect("shutdown must finish once the parked worker was released")
        .expect("shutdown task");
    guard.mark("worker released, shutdown drained");

    // Mutant: dropping `backup_schedule_task` instead of joining it lets
    // `handle.shutdown()` return while the run is still parked.
    assert!(
        still_pending,
        "handle.shutdown() must NOT return while the scheduled worker is parked \
         (mutant: the launcher stops awaiting the scheduled task)"
    );
    assert!(
        bridge.resumed.load(Ordering::SeqCst),
        "the released worker must resume and run its post-release cleanup"
    );
    assert!(
        report.is_clean(),
        "a shutdown that drained the released worker must be clean: {report:?}"
    );
    // Late, never early: the guard only clears an UNFIRED probe on drop, and
    // the probe fired long before this point.
    drop(join_probe);
}

// ---------------------------------------------------------------------------
// (a2) Production spawn path + a worker parked INSIDE RETENTION
// ---------------------------------------------------------------------------

const RETENTION_PARK_TEST: &str = "shutdown_waits_for_parked_retention_and_binds_the_root_token";

/// Separate bridge slot: never races the earlier production-spawn test's slot.
static RETENTION_PARK_BRIDGE: Mutex<Option<Arc<ProductionSpawnBridge>>> = Mutex::new(None);

/// The `launch_with_backup_test` spawn argument: record the launcher's registry,
/// arm the post-run callback that installs the retention pause ON THE RUNNER
/// THREAD, then call the PRODUCTION [`spawn`]. The pause fires at the start of
/// the retention pass (after the copy outcome) and parks the worker there.
fn arm_retention_park_then_spawn(
    schedule: ResolvedSchedule,
    registry: Arc<OnlineBackupRegistry>,
    owner: String,
    shutdown: CancellationToken,
) -> JoinHandle<()> {
    let bridge = RETENTION_PARK_BRIDGE
        .lock()
        .expect("retention park bridge")
        .clone()
        .expect("the test installs the bridge before launching");
    *bridge.registry.lock().expect("registry slot") = Some(Arc::clone(&registry));
    let parked = Arc::clone(&bridge.parked);
    let gate = Arc::clone(&bridge.gate);
    registry.arm_post_run_callback_for_shutdown_test(move |_published| {
        let parked = Arc::clone(&parked);
        let gate = Arc::clone(&gate);
        let pause = arm_scheduled_retention_pause(move || {
            parked.add_permits(1);
            wait_for_release(&gate);
        });
        // One-shot: the pause removes itself from the thread-local when it
        // fires; the zero-sized guard would only clear an UNFIRED install.
        std::mem::forget(pause);
        None
    });
    spawn(schedule, registry, owner, shutdown)
}

#[test]
fn shutdown_waits_for_parked_retention_and_binds_the_root_token() {
    on_big_stack(|| async {
        timeout(OUTER, run_retention_park())
            .await
            .expect("parked-retention shutdown acceptance timed out");
    });
}

async fn run_retention_park() {
    let guard = FlakeGuard::with_watchdog(RETENTION_PARK_TEST, WATCHDOG);
    try_install_global_tracing_ring();
    let temp = tempfile::tempdir().expect("test root");
    let data_dir = temp.path().join("data");
    let backups = temp.path().join("backups");
    create_private_dir(&data_dir).expect("private data directory");
    create_private_dir(&backups).expect("private backup directory");

    let bridge = Arc::new(ProductionSpawnBridge {
        parked: Arc::new(Semaphore::new(0)),
        gate: Arc::new((Mutex::new(WorkerGate::default()), Condvar::new())),
        resumed: Arc::new(AtomicBool::new(false)),
        registry: Mutex::new(None),
    });
    *RETENTION_PARK_BRIDGE.lock().expect("retention park bridge") = Some(Arc::clone(&bridge));

    let handle = ServerLauncher::new(scheduled_config(&data_dir, &backups, "1h", true, 2))
        .with_bootstrap(BootstrapMode::Skip)
        .launch_with_backup_test(arm_retention_park_then_spawn)
        .await
        .expect("launch a real server through the production scheduled-worker spawn");
    guard.mark("launched through the production spawn");

    // The run starts by itself (`run_on_start`); retention then parks.
    let permit = timeout(BOUND, bridge.parked.acquire())
        .await
        .expect("the run must reach the retention park")
        .expect("parked semaphore open");
    drop(permit);
    guard.mark("production worker parked inside retention");

    // (1) The COPY outcome is already published while retention is parked.
    // Mutant: the copy outcome published only after the retention pass.
    let parked_metrics = backup_metrics_snapshot();
    assert!(
        parked_metrics.last_duration_seconds.is_some(),
        "the successful scheduled copy must publish its last-success cell BEFORE retention \
         (mutant: the copy outcome published only after the retention pass): {parked_metrics:?}"
    );

    // (3a) The instance lock is still held while retention is parked.
    assert!(
        crate::maintenance_lock::acquire_server(&data_dir).is_err(),
        "the data-directory lock must still be held while retention is parked"
    );

    // (2) Shutdown must not return during the park (mutant: FINISHED sent early).
    let root = handle.shutdown_token.clone();
    // Fires from INSIDE `ShutdownOwner::join_schedule`, right before the await
    // of the scheduled worker, so the probe window below starts AT the join.
    let (join_reached_tx, mut join_reached_rx) = mpsc::unbounded_channel::<()>();
    let join_probe = arm_scheduled_join_probe(move || {
        let _ = join_reached_tx.send(());
    });
    let mut shutdown = tokio::spawn(handle.shutdown());
    timeout(BOUND, root.cancelled())
        .await
        .expect("handle.shutdown() must cancel the launcher's root token");
    timeout(BOUND, join_reached_rx.recv())
        .await
        .expect("shutdown must reach the scheduled join within the bound")
        .expect("the join probe channel must stay open");
    guard.mark("shutdown reached the scheduled join");
    // The probe runs INSIDE the join, so this window can no longer pass
    // vacuously on a slow runner.
    let still_pending = timeout(PARKED_PROBE, &mut shutdown).await.is_err();
    guard.mark("shutdown stayed pending while retention was parked");

    release_worker(&bridge.gate);
    let report = timeout(BOUND, shutdown)
        .await
        .expect("shutdown must finish once the parked retention was released")
        .expect("shutdown task");
    guard.mark("retention released, shutdown drained");

    let lock_after = crate::maintenance_lock::acquire_server(&data_dir);
    assert!(
        still_pending,
        "handle.shutdown() must NOT return while the worker is parked inside retention \
         (mutant: FINISHED sent at protocol completion, or only the outcome awaited)"
    );
    assert!(
        report.is_clean(),
        "a shutdown that drained retention must be clean: {report:?}"
    );
    // (3b) The lock is free only after shutdown returned.
    assert!(
        lock_after.is_ok(),
        "the data-directory lock must be free once shutdown drained retention"
    );
    // Late, never early: the guard only clears an UNFIRED probe on drop, and
    // the probe fired long before this point.
    drop(join_probe);
}

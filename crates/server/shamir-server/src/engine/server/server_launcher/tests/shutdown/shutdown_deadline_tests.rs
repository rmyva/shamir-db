//! Ordered-shutdown deadline vs. the SCHEDULED-BACKUP WORKER, plus the two
//! launch-rollback paths.
//!
//! `shutdown_deadline_does_not_abandon_the_scheduled_worker` parks the REAL
//! native backup runner inside its post-run callback — after the protocol
//! returned, before its completion signal — and then runs the embedding
//! shutdown (`runtime::drain_server`) with a deadline whose early cutoff is
//! 2 s. The bounded join must time out there with a zero-budget, stage-specific
//! error, and the late stages must complete before the outer deadline. The
//! worker is released near that deadline; `drain_server` may then be waiting for
//! its owner outside the outer timeout. That wait is not the fallback branch:
//! fallback is taken only if the outer `timeout_at` actually expires. The
//! single-instance data-directory lock must be held while the worker runs and
//! free afterwards.
//!
//! The two rollback tests make a bind fail (the listener path and the
//! observability path) and assert that the ORIGINAL boot error comes back, that
//! the shared audit deadline bounds the rollback, and that the instance lock is
//! released again.
//!
//! Nothing synchronises with an arbitrary sleep: the worker rendezvous is a
//! semaphore signalled from the runner thread, the release is a condition
//! variable, and every await is bounded by [`BOUND`]. The deadline case also
//! deliberately observes a fixed pending window after the early cutoff.
//!
//! `late_budget::late_teardown_stages_report_the_remaining_shutdown_budget` covers the
//! OTHER half of the same deadline: the two stages that run after the audit and
//! repository teardown (the meta-snapshot join and the store-release barrier).
//! It parks the runtime's ONLY blocking thread — the pool both stages submit
//! their work to — and runs the embedding shutdown with a deadline shorter than
//! either stage constant, so the report can only contain their own verdicts if
//! they spend the REMAINING budget instead of the constants.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use shamir_private_dir::create_private_dir;
use shamir_test_diag::{try_install_global_tracing_ring, FlakeGuard};
use tokio::sync::Semaphore;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use crate::boot::config::diagnostics::backup_online::schedule::runner::spawn_for_test;
use crate::boot::config::HttpListenerSettings;
use crate::config::{
    Config, KdfConfig, ListenerConfig, ListenerKind, LoggingConfig, ObservabilityConfig, TlsConfig,
};
use crate::db_handler::NodeMode;
use crate::server::server_handle::{
    arm_fallback_drain_join_probe, arm_pre_observability_registry_probe,
    arm_release_sources_stopped_probe, arm_repo_flush_probe, arm_scheduled_join_probe,
};
use crate::server::server_launcher::audit_teardown::arm_teardown_probe;
use crate::server::{release_stores, BootstrapMode, ReleaseOutcome, ServerLauncher};

mod late_budget;
mod owned_mutation_join;
mod owner_lock;
mod zero_budget_release;

const TEST_NAME: &str = "shutdown_deadline_does_not_abandon_the_scheduled_worker";
const PUBLIC_TEST: &str = "public_shutdown_bounds_meta_snapshot_join_without_outer_deadline";
const EXAMPLE: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../../deploy/server.example.ktav"
));

/// One generous bound for every wait: the Windows CI runner is up to 10x
/// slower than a workstation.
const BOUND: Duration = Duration::from_secs(180);
/// The deadline under test, in REAL time: the early cutoff is 2 s, leaving a
/// shared 1 s reserve for late stages before the outer timeout.
const TEST_DEADLINE: Duration = Duration::from_secs(3);
/// The shared early cutoff: outer deadline minus the 1 s late-stage reserve.
const EARLY_CUTOFF: Duration = Duration::from_secs(2);
/// Keep the worker parked through the late-stage reserve, then release it while
/// `drain_server` can still be waiting for its owner outside the outer timeout.
const PENDING_WINDOW: Duration = Duration::from_millis(750);

/// The outer deadline for the remaining-budget test.
///
/// Two orders of magnitude above the stages that run before the meta-snapshot
/// join (they never touch the blocking pool, so even a 10x slower runner
/// reaches step 6 well inside it) and BELOW BOTH late-stage constants
/// (`META_SNAPSHOT_JOIN_TIMEOUT` 5 s, `STORE_RELEASE_TIMEOUT` 10 s): neither
/// late stage can finish on its own constant, so the report can only name their
/// verdicts if they spend what is left of this budget.
const REMAINING_BUDGET_DEADLINE: Duration = Duration::from_secs(3);

#[test]
fn shutdown_deadline_does_not_abandon_the_scheduled_worker() {
    on_big_stack(|| async {
        timeout(BOUND, run_scheduled_worker())
            .await
            .expect("scheduled-worker deadline test bounded");
    });
}

async fn run_scheduled_worker() {
    let guard = FlakeGuard::with_watchdog(TEST_NAME, Duration::from_secs(300));
    try_install_global_tracing_ring();
    let temp = tempfile::tempdir().expect("test root");
    let data_dir = temp.path().join("data");
    let backups = temp.path().join("backups");
    create_private_dir(&data_dir).expect("private data directory");
    create_private_dir(&backups).expect("private backup directory");

    // The worker parks inside its post-run callback; `parked` is how the test
    // learns the run is in flight (semaphore: no permit is lost), `gate` is how
    // it releases the run again.
    let parked = Arc::new(Semaphore::new(0));
    let gate = Arc::new((Mutex::new(WorkerState::default()), Condvar::new()));
    let finished = Arc::new(AtomicBool::new(false));
    let completed = Arc::new(AtomicBool::new(false));

    let handle = ServerLauncher::new(backup_config(data_dir.clone(), backups))
        .with_bootstrap(BootstrapMode::Skip)
        .launch_with_backup_test({
            let parked = Arc::clone(&parked);
            let gate = Arc::clone(&gate);
            let finished = Arc::clone(&finished);
            let completed = Arc::clone(&completed);
            move |resolved, registry, owner, shutdown| {
                registry.arm_post_run_callback_for_shutdown_test({
                    let parked = Arc::clone(&parked);
                    let gate = Arc::clone(&gate);
                    let finished = Arc::clone(&finished);
                    move |_published: &Path| {
                        parked.add_permits(1);
                        wait_until(&gate, "worker release", |state| state.released);
                        // Marks the post-run callback's released stage; it does not prove task exit.
                        finished.store(true, Ordering::SeqCst);
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
                    || {},
                )
            }
        })
        .await
        .expect("launch real server with the scheduled backup worker");
    guard.mark("launched");

    // The run starts by itself (run_on_start) and parks before its completion
    // signal. Waiting for it is an await, so the runtime keeps driving the
    // scheduler task that starts it.
    let parked_permit = timeout(BOUND, parked.acquire())
        .await
        .expect("the scheduled run must reach its post-run callback")
        .expect("parked semaphore open");
    drop(parked_permit);
    guard.mark("native runner parked before its completion signal");

    // Distinct markers show the bounded join is reached in both modes, while
    // the fallback join is reached only after deadline expiry.
    let (join_tx, join_rx) = tokio::sync::oneshot::channel::<()>();
    let (teardown_tx, teardown_rx) = tokio::sync::oneshot::channel::<()>();
    let joined = Arc::new(AtomicBool::new(false));
    let fallback_joined = Arc::new(AtomicBool::new(false));
    let flush_entered = Arc::new(AtomicBool::new(false));
    let _teardown_probe = arm_teardown_probe(move || {
        let _ = teardown_tx.send(());
    });
    let _join_probe = {
        let joined = Arc::clone(&joined);
        arm_scheduled_join_probe(move || {
            joined.store(true, Ordering::SeqCst);
            let _ = join_tx.send(());
        })
    };
    let _fallback_join_probe = {
        let fallback_joined = Arc::clone(&fallback_joined);
        arm_fallback_drain_join_probe(move || fallback_joined.store(true, Ordering::SeqCst))
    };
    let _flush_probe = {
        let flush_entered = Arc::clone(&flush_entered);
        arm_repo_flush_probe(move || flush_entered.store(true, Ordering::SeqCst))
    };

    // Only now does the shutdown start: the deadline cannot pre-empt the run
    // start, only the wait for it.
    // Observe until the fixed early cutoff, measured from the outer shutdown
    // start. At t=2 s the scheduled join has no budget left; the 1 s reserve
    // lets late stages finish before the outer t=3 s deadline. Keep the worker
    // parked for most of that reserve: after bounded teardown, `drain_server`
    // may still be waiting for its owner outside `timeout_at`. That is not the
    // fallback branch unless the outer timeout itself expires.
    let shutdown_started = Instant::now();
    let mut shutdown = tokio::spawn(crate::runtime::drain_server(handle, TEST_DEADLINE));
    timeout(BOUND, teardown_rx)
        .await
        .expect("the shutdown must enter the shared audit teardown")
        .expect("teardown probe channel");
    guard.mark("audit teardown entered");
    timeout(BOUND, join_rx)
        .await
        .expect("the shutdown must reach ShutdownOwner::join_schedule")
        .expect("join probe channel");
    guard.mark("join_schedule reached with the worker parked");
    let until_early_cutoff = EARLY_CUTOFF.saturating_sub(shutdown_started.elapsed());
    let still_pending = timeout(until_early_cutoff, &mut shutdown).await.is_err();
    // Keep the worker parked during the late-stage reserve. This distinguishes
    // bounded teardown completing from the owner drain finishing; the latter
    // can remain pending outside the outer timeout until the worker is released.
    timeout(BOUND, tokio::time::sleep(PENDING_WINDOW))
        .await
        .expect("the pending window must be bounded");
    // Still while the worker runs: the instance lock must be held.
    let lock_while_running = crate::maintenance_lock::acquire_server(&data_dir).is_err();
    guard.mark("early cutoff elapsed with worker still parked");

    // Unconditional release, then join; assertions run only afterwards.
    release(&gate);
    let shutdown = timeout(BOUND, shutdown)
        .await
        .expect("the shutdown future must finish once the worker was released")
        .expect("shutdown task");
    let lock_after = crate::maintenance_lock::acquire_server(&data_dir);
    guard.mark("worker released, shutdown drained, lock released");

    assert!(
        still_pending,
        "the embedding shutdown future must stay pending while the scheduled worker runs"
    );
    assert!(
        lock_while_running,
        "the data-directory lock must still be held while the scheduled worker runs"
    );
    assert!(
        shutdown
            .errors()
            .iter()
            .any(|error| error.contains("backup schedule join exceeded shutdown budget")),
        "the bounded backup-schedule join must report its own timeout at the early cutoff: {shutdown:?}"
    );
    assert!(
        !shutdown
            .errors()
            .iter()
            .any(|error| error.contains("graceful shutdown exceeded deadline")),
        "the late stages must complete before the outer deadline: {shutdown:?}"
    );
    assert_eq!(shutdown.exit_code(), 70, "unclean report: {shutdown:?}");
    assert!(
        completed.load(Ordering::SeqCst),
        "the scheduled backup completion callback must report success after the bounded join timeout: {shutdown:?}"
    );
    assert!(
        finished.load(Ordering::SeqCst),
        "the post-run callback-stage marker must be set after its release wait (not proof of task exit)"
    );
    assert!(
        lock_after.is_ok(),
        "the data-directory lock must be free after the callback reported successful completion"
    );
    // The bounded join timed out at the early cutoff; the outer deadline did
    // not expire, so fallback drain was not the path taken.
    assert!(
        joined.load(Ordering::SeqCst),
        "bounded join marker must fire at the early-cutoff join"
    );
    assert!(
        !fallback_joined.load(Ordering::SeqCst),
        "fallback join marker must not fire without outer deadline expiry"
    );
    assert!(
        !flush_entered.load(Ordering::SeqCst),
        "the repository flush must not run while the scheduled run is still pending: {shutdown:?}"
    );
    // The runtime is STILL ALIVE here. Nothing may still hold a store: an
    // observability handle dropped without cancellation would keep its poller
    // and router (and their `Arc<ShamirDb>`) alive, and the barrier would still
    // see a store lock (mutant: no cancel in `shutdown_with_report` and no
    // `Drop` cancel on the handle).
    let released = release_stores(&data_dir, Duration::from_secs(10)).await;
    assert!(
        matches!(released, ReleaseOutcome::Released),
        "a bounded-join timeout must not leave live tasks holding the stores: {released:?}"
    );
}

/// A NORMAL shutdown (no expiry) joins the scheduled worker before the
/// repository flush. The callback-stage marker records that the post-run
/// callback passed its release wait by the time the flush stage started; this
/// does not establish that the spawned task fully exited.
#[test]
fn normal_shutdown_joins_the_scheduled_worker_before_the_repository_flush() {
    on_big_stack(|| async {
        timeout(BOUND, run_normal_order())
            .await
            .expect("normal-order test bounded");
    });
}

async fn run_normal_order() {
    let guard = FlakeGuard::with_watchdog(
        "normal_shutdown_joins_the_scheduled_worker_before_the_repository_flush",
        Duration::from_secs(300),
    );
    try_install_global_tracing_ring();
    let temp = tempfile::tempdir().expect("test root");
    let data_dir = temp.path().join("data");
    let backups = temp.path().join("backups");
    create_private_dir(&data_dir).expect("private data directory");
    create_private_dir(&backups).expect("private backup directory");

    let parked = Arc::new(Semaphore::new(0));
    let gate = Arc::new((Mutex::new(WorkerState::default()), Condvar::new()));
    let finished = Arc::new(AtomicBool::new(false));
    let completed = Arc::new(AtomicBool::new(false));

    let handle = ServerLauncher::new(backup_config(data_dir.clone(), backups))
        .with_bootstrap(BootstrapMode::Skip)
        .launch_with_backup_test({
            let parked = Arc::clone(&parked);
            let gate = Arc::clone(&gate);
            let finished = Arc::clone(&finished);
            let completed = Arc::clone(&completed);
            move |resolved, registry, owner, shutdown| {
                registry.arm_post_run_callback_for_shutdown_test({
                    let parked = Arc::clone(&parked);
                    let gate = Arc::clone(&gate);
                    let finished = Arc::clone(&finished);
                    move |_published: &Path| {
                        parked.add_permits(1);
                        wait_until(&gate, "worker release", |state| state.released);
                        // Marks the post-run callback's released stage; it does not prove task exit.
                        finished.store(true, Ordering::SeqCst);
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
                    || {},
                )
            }
        })
        .await
        .expect("launch real server with the scheduled backup worker");
    guard.mark("launched");

    let parked_permit = timeout(BOUND, parked.acquire())
        .await
        .expect("the scheduled run must reach its post-run callback")
        .expect("parked semaphore open");
    drop(parked_permit);
    guard.mark("native runner parked before its completion signal");

    let (join_tx, join_rx) = tokio::sync::oneshot::channel::<()>();
    let bounded_joined = Arc::new(AtomicBool::new(false));
    let fallback_joined = Arc::new(AtomicBool::new(false));
    let flush_entered = Arc::new(AtomicBool::new(false));
    let flush_saw_completion = Arc::new(AtomicBool::new(false));
    let _join_probe = {
        let bounded_joined = Arc::clone(&bounded_joined);
        arm_scheduled_join_probe(move || {
            bounded_joined.store(true, Ordering::SeqCst);
            let _ = join_tx.send(());
        })
    };
    let _fallback_join_probe = {
        let fallback_joined = Arc::clone(&fallback_joined);
        arm_fallback_drain_join_probe(move || fallback_joined.store(true, Ordering::SeqCst))
    };
    let _flush_probe = {
        let flush_entered = Arc::clone(&flush_entered);
        let flush_saw_completion = Arc::clone(&flush_saw_completion);
        let finished = Arc::clone(&finished);
        arm_repo_flush_probe(move || {
            flush_entered.store(true, Ordering::SeqCst);
            flush_saw_completion.store(finished.load(Ordering::SeqCst), Ordering::SeqCst);
        })
    };

    // GENEROUS deadline: this case must not expire.
    let shutdown = tokio::spawn(crate::runtime::drain_server(handle, BOUND));
    timeout(BOUND, join_rx)
        .await
        .expect("a normal shutdown must reach ShutdownOwner::join_schedule")
        .expect("join probe channel");
    guard.mark("join_schedule reached");
    release(&gate);
    let shutdown = timeout(BOUND, shutdown)
        .await
        .expect("the shutdown future must finish")
        .expect("shutdown task");
    guard.mark("shutdown drained");

    assert!(
        !shutdown
            .errors()
            .iter()
            .any(|error| error.contains("graceful shutdown exceeded deadline")),
        "the normal case must not expire: {shutdown:?}"
    );
    assert!(
        completed.load(Ordering::SeqCst),
        "the scheduled backup completion callback must report success after normal shutdown: {shutdown:?}"
    );
    assert!(
        finished.load(Ordering::SeqCst),
        "the callback-stage marker must be set before the normal flush (not proof of task exit): {shutdown:?}"
    );
    assert!(
        bounded_joined.load(Ordering::SeqCst),
        "bounded join_schedule marker must fire on the normal path"
    );
    assert!(
        !fallback_joined.load(Ordering::SeqCst),
        "fallback ShutdownOwner::drain join marker must not fire on the normal path"
    );
    assert!(
        flush_entered.load(Ordering::SeqCst),
        "the repository flush stage must run on a normal shutdown: {shutdown:?}"
    );
    assert!(
        flush_saw_completion.load(Ordering::SeqCst),
        "the callback-stage marker is set before the normal flush \
         (mutant: the callback-stage wait occurs after the flush): {shutdown:?}"
    );
    // A repository outcome is the FLUSH's own result, not the fact that the
    // stage was entered: a skipped flush leaves every repository outcome
    // unrecorded, which `repository_failures` reports as "shutdown did not
    // complete". (Mutants: a flush that is skipped, and a teardown that waits
    // only for the outcome instead of running the flush.)
    assert!(
        shutdown.repository_failures().is_empty(),
        "every repository must flush successfully on a normal shutdown, not only be \
         waited for: {:?}",
        shutdown.repository_failures()
    );
}

/// Public `ServerHandle::shutdown()` has no outer deadline, yet the stages that
/// wait on the blocking pool (token-GC join, meta-snapshot join) must stay
/// bounded. A GC pass is restarted with no await before the pool is parked, so
/// its blocking job is queued behind the parked one on every run (the
/// launcher's own first pass races the park).
#[test]
fn public_shutdown_bounds_meta_snapshot_join_without_outer_deadline() {
    on_single_blocking_thread(|| async {
        let guard = FlakeGuard::with_watchdog(PUBLIC_TEST, Duration::from_secs(150));
        try_install_global_tracing_ring();
        let temp = tempfile::tempdir().expect("test root");
        let data_dir = temp.path().join("data");
        create_private_dir(&data_dir).expect("private data directory");
        let handle = ServerLauncher {
            config: bind_config(data_dir, "127.0.0.1:0".to_owned(), "127.0.0.1:0".to_owned()),
            bootstrap: BootstrapMode::Skip,
            node_mode: NodeMode::ReadWrite,
        }
        .launch()
        .await
        .expect("launch real server");
        guard.mark("launched");

        let stop = CancellationToken::new();
        handle.token_service.shutdown_gc().await;
        handle.token_service.start_gc(stop).await;
        let occupant = BlockingPoolOccupant::park();
        occupant.await_started();
        guard.mark("token GC pass spawned, blocking pool occupied");
        let report = timeout(BOUND, handle.shutdown())
            .await
            .expect("public shutdown must settle with the blocking pool parked");
        guard.mark("shutdown returned");
        let pool_still_occupied = !occupant.finished();
        drop(occupant);

        let errors = report.errors();
        assert!(
            errors
                .iter()
                .any(|error| error.contains("token GC shutdown exceeded 10s")),
            "public shutdown must report the capped token-GC join expiry: {report:?}"
        );
        assert!(
            errors
                .iter()
                .any(|error| error.contains("meta snapshot task did not stop within 5s")),
            "public shutdown must report the capped meta-snapshot join expiry: {report:?}"
        );
        assert!(
            pool_still_occupied,
            "the shutdown report must be produced while the blocking job remains parked: {report:?}"
        );
        assert!(!report.is_clean(), "the report must be unclean: {report:?}");
    });
}

/// A job that occupies the runtime's blocking pool until it is released.
///
/// The release is a channel send, never a sleep, and `Drop` sends it
/// unconditionally: an assertion that fails between parking and the explicit
/// release can never wedge the next test behind a parked pool.
struct BlockingPoolOccupant {
    release: std::sync::mpsc::Sender<()>,
    started: std::sync::mpsc::Receiver<()>,
    finished: Arc<AtomicBool>,
}

impl BlockingPoolOccupant {
    /// Submit the occupying job to the ONE blocking thread of this runtime.
    fn park() -> Self {
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let finished = Arc::new(AtomicBool::new(false));
        let job_finished = Arc::clone(&finished);
        let job = tokio::task::spawn_blocking(move || {
            let _ = started_tx.send(());
            // Parks the pool's only thread until the test releases it.
            let _ = release_rx.recv();
            job_finished.store(true, Ordering::SeqCst);
        });
        // Detached on purpose: `Drop` is what releases the job, and no await is
        // possible there.
        drop(job);
        Self {
            release: release_tx,
            started: started_rx,
            finished,
        }
    }

    /// Block until the job really owns the blocking thread.
    fn await_started(&self) {
        self.started
            .recv_timeout(BOUND)
            .expect("the occupying job must start on the only blocking thread");
    }

    /// Has the occupying job returned (i.e. is the pool free again)?
    fn finished(&self) -> bool {
        self.finished.load(Ordering::SeqCst)
    }
}

impl Drop for BlockingPoolOccupant {
    fn drop(&mut self) {
        let _ = self.release.send(());
    }
}

/// The runner-side rendezvous state.
#[derive(Clone, Copy, Default)]
struct WorkerState {
    /// The worker may finish its run.
    released: bool,
}

fn wait_until(
    gate: &(Mutex<WorkerState>, Condvar),
    stage: &str,
    ready: impl Fn(WorkerState) -> bool,
) {
    let (lock, condvar) = gate;
    let deadline = Instant::now() + BOUND;
    let mut state = lock.lock().expect("worker state");
    while !ready(*state) {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let (next, timed_out) = condvar
            .wait_timeout(state, remaining)
            .expect("worker state wait");
        state = next;
        if timed_out.timed_out() && !ready(*state) {
            panic!("{stage} did not happen within {BOUND:?}");
        }
    }
}

fn release(gate: &(Mutex<WorkerState>, Condvar)) {
    let (lock, condvar) = gate;
    let mut state = lock.lock().expect("worker state");
    state.released = true;
    condvar.notify_all();
}

#[test]
fn listener_bind_rollback_keeps_the_boot_error_and_releases_the_lock() {
    let temp = tempfile::tempdir().expect("test root");
    let data_dir = temp.path().join("data");
    create_private_dir(&data_dir).expect("private data directory");
    let held = std::net::TcpListener::bind("127.0.0.1:0").expect("hold a port");
    let occupied = held.local_addr().expect("held address").to_string();
    let config = bind_config(data_dir.clone(), occupied.clone(), "127.0.0.1:0".to_owned());
    // Nextest gives this test its own process, so the one-shot teardown probe
    // cannot be consumed by another test's launch.
    let teardown_ran = Arc::new(AtomicBool::new(false));
    let sources_stopped = Arc::new(AtomicBool::new(false));
    let _teardown_probe = {
        let flag = Arc::clone(&teardown_ran);
        arm_teardown_probe(move || flag.store(true, Ordering::SeqCst))
    };
    // The rollback arm must run the ONE ordered release-source stop: fire the
    // probe the helper fires after the sources stopped and the registry was
    // cleared (mutant: the arm skips `stop_release_sources`).
    let _sources_probe = {
        let flag = Arc::clone(&sources_stopped);
        arm_release_sources_stopped_probe(move || flag.store(true, Ordering::SeqCst))
    };

    let error = run_blocking(move || match block_on_launch(config) {
        Ok(_) => panic!("the occupied listener port must fail the bind"),
        Err(error) => error,
    });
    let message = error.to_string();
    // `BootError::Bind` prefixes EVERY bind failure with `listener bind`, so the
    // endpoint is distinguished by the inner text the HTTP path adds
    // (`HTTP bind <addr>: ...`, `server_launcher/http/launcher.rs`).
    assert!(
        message.contains("listener bind") && message.contains("HTTP bind"),
        "the original boot error must be returned with the data-listener endpoint: {message}"
    );
    assert!(
        message.contains(&occupied),
        "the error must name the LISTENER address that failed, not any bind: {message}"
    );
    assert!(
        !message.contains("observability"),
        "the listener rollback must not report the observability endpoint: {message}"
    );
    assert!(
        teardown_ran.load(Ordering::SeqCst),
        "the launch rollback must run the shared audit teardown (mutant: the `teardown_audit` \
         call removed from the listener-bind branch)"
    );
    assert!(
        sources_stopped.load(Ordering::SeqCst),
        "the listener-bind rollback must stop the release sources and clear the registry \
         (mutant: `stop_release_sources` removed from the arm)"
    );
    // The rollback ran to completion: nothing of this launch holds the instance
    // lock any more, so a retry after freeing the port is possible.
    let lock = crate::maintenance_lock::acquire_server(&data_dir);
    assert!(
        lock.is_ok(),
        "the launch rollback must release the data-directory lock"
    );
}

#[test]
fn observability_bind_rollback_keeps_the_boot_error_and_releases_the_lock() {
    let temp = tempfile::tempdir().expect("test root");
    let data_dir = temp.path().join("data");
    create_private_dir(&data_dir).expect("private data directory");
    let held = std::net::TcpListener::bind("127.0.0.1:0").expect("hold a port");
    let occupied = held.local_addr().expect("held address").to_string();
    let config = bind_config(data_dir.clone(), "127.0.0.1:0".to_owned(), occupied.clone());
    let teardown_ran = Arc::new(AtomicBool::new(false));
    let sources_stopped = Arc::new(AtomicBool::new(false));
    let _teardown_probe = {
        let flag = Arc::clone(&teardown_ran);
        arm_teardown_probe(move || flag.store(true, Ordering::SeqCst))
    };
    let _sources_probe = {
        let flag = Arc::clone(&sources_stopped);
        arm_release_sources_stopped_probe(move || flag.store(true, Ordering::SeqCst))
    };

    let error = run_blocking(move || match block_on_launch(config) {
        Ok(_) => panic!("the occupied observability port must fail the bind"),
        Err(error) => error,
    });
    let message = error.to_string();
    // The observability path wraps its own `ObservabilityError::Bind` display,
    // which does NOT carry the socket address; the endpoint marker in the error
    // text is `observability`, and every bind failure shares the outer
    // `listener bind` prefix.
    assert!(
        message.contains("listener bind") && message.contains("observability"),
        "the original boot error must be returned with the observability endpoint: {message}"
    );
    assert!(
        !message.contains("HTTP bind"),
        "the observability rollback must not report the data listener: {message}"
    );
    assert!(
        teardown_ran.load(Ordering::SeqCst),
        "the launch rollback must run the shared audit teardown (mutant: the `teardown_audit` \
         call removed from the observability-bind branch)"
    );
    assert!(
        sources_stopped.load(Ordering::SeqCst),
        "the observability-bind rollback must stop the release sources and clear the registry \
         (mutant: `stop_release_sources` removed from the arm)"
    );
    let lock = crate::maintenance_lock::acquire_server(&data_dir);
    assert!(
        lock.is_ok(),
        "the launch rollback must release the data-directory lock"
    );
}

/// The observability-bind rollback with an OPEN interactive transaction in
/// the registry: the pre-observability probe parks a REAL transaction (an
/// in-memory `ShamirDb` tx registered into the SERVER's registry — the same
/// `parked()` shape the activity tests use) at a deterministic point before
/// the bind attempt, the occupied port fails the bind, and the test asserts
/// the boot error still wins AND the registry is EMPTY after the rollback.
/// With a registered tx present, emptiness FAILS if the arm's
/// `stop_release_sources` (its `registry.remove_all()`) is removed — the
/// teardown/sources markers alone cannot discriminate that, because with no
/// tx registered the registry is empty either way.
#[test]
fn observability_rollback_clears_a_registry_with_an_open_tx() {
    let temp = tempfile::tempdir().expect("test root");
    let data_dir = temp.path().join("data");
    create_private_dir(&data_dir).expect("private data directory");
    let held = std::net::TcpListener::bind("127.0.0.1:0").expect("hold a port");
    let occupied = held.local_addr().expect("held address").to_string();
    let config = bind_config(data_dir.clone(), "127.0.0.1:0".to_owned(), occupied);

    // The registry the launcher hands us; retained by cloning the Arc so the
    // emptiness assertion runs AFTER the rollback returned.
    let server_registry: Arc<
        std::sync::Mutex<Option<std::sync::Arc<crate::tx_registry::TxRegistry>>>,
    > = Arc::new(std::sync::Mutex::new(None));
    let _registry_probe = {
        let slot = Arc::clone(&server_registry);
        arm_pre_observability_registry_probe(move |registry| {
            let slot = Arc::clone(&slot);
            let registry = std::sync::Arc::clone(registry);
            Box::pin(async move {
                // Park a REAL interactive transaction into the server's
                // registry (own in-memory db: the release closure is the real
                // engine lock release, so the rollback's tail drain settles
                // it exactly like a production tx).
                let owner = shamir_db::access::Actor::User(shamir_db::access::principal64([3; 16]));
                let db = open_app_db_for_rollback(&owner).await;
                let (ctx, snapshot) = db
                    .tx_begin_as(owner.clone(), "app", "main", "snapshot")
                    .await
                    .expect("begin interactive tx");
                let handle = ctx.tx_id.0;
                let repo = db
                    .get_db("app")
                    .expect("fixture db")
                    .get_repo("main")
                    .expect("fixture repo");
                let release: crate::tx_registry::TxAbortRelease =
                    std::sync::Arc::new(move |parked: shamir_tx::TxContext| {
                        let repo = repo.clone();
                        Box::pin(async move {
                            shamir_db::engine::tx::release_interactive_tx_locks(
                                &parked,
                                repo.core(),
                            )
                            .await;
                        })
                            as std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>
                    });
                let it = crate::tx_registry::InteractiveTx::new(
                    ctx,
                    snapshot,
                    [9; 32],
                    [9; 16],
                    "app".into(),
                    "main".into(),
                    Duration::from_secs(3_600),
                    release,
                );
                let registered = registry.register(handle, it);
                assert!(
                    registered.is_ok(),
                    "the tx must park into the server's registry: {}",
                    registered.err().map(|e| e.to_string()).unwrap_or_default()
                );
                assert!(!registry.is_empty(), "the tx must be registered");
                *slot.lock().expect("registry slot") = Some(registry);
            })
        })
    };

    let error = run_blocking(move || match block_on_launch(config) {
        Ok(_) => panic!("the occupied observability port must fail the bind"),
        Err(error) => error,
    });
    let message = error.to_string();
    assert!(
        message.contains("listener bind") && message.contains("observability"),
        "the boot error must still win over the rollback cleanup: {message}"
    );
    let registry = server_registry
        .lock()
        .expect("registry slot")
        .clone()
        .expect("the probe must have parked a tx before the bind attempt");
    assert!(
        registry.is_empty(),
        "the rollback must clear the registry even with an open tx present (mutant: \
         `stop_release_sources` removed from the arm)"
    );
}

/// Run one real launch on a roomy thread with its own current-thread runtime,
/// bounded so a rollback that never finishes fails instead of hanging CI.
fn block_on_launch(
    config: Config,
) -> Result<crate::server::ServerHandle, crate::server::BootError> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime");
    // The timer is built INSIDE the runtime: `timeout(..)` as a `block_on`
    // argument would construct it before the runtime is entered and panic.
    let launched = runtime.block_on(async move {
        timeout(
            BOUND,
            Box::pin(
                ServerLauncher {
                    config,
                    bootstrap: BootstrapMode::Skip,
                    node_mode: NodeMode::ReadWrite,
                }
                .launch(),
            ),
        )
        .await
    });
    match launched {
        Ok(result) => result,
        Err(_) => panic!("launch rollback did not finish within {BOUND:?}"),
    }
}

fn run_blocking<T: Send + 'static>(body: impl FnOnce() -> T + Send + 'static) -> T {
    std::thread::Builder::new()
        .stack_size(64 * 1024 * 1024)
        .spawn(body)
        .expect("spawn test thread")
        .join()
        .unwrap_or_else(|panic| std::panic::resume_unwind(panic))
}

fn on_big_stack<F, Fut>(body: F)
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = ()>,
{
    run_blocking(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime")
            .block_on(body())
    })
}

/// Like [`on_big_stack`], but with EXACTLY ONE blocking thread: the test can
/// then occupy the whole pool with one parked job instead of guessing how many
/// workers a default pool would hand out.
fn on_single_blocking_thread<F, Fut>(body: F)
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = ()>,
{
    run_blocking(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .expect("test runtime")
            .block_on(body())
    })
}

/// A real server whose only listener is a plain loopback HTTP listener and
/// whose scheduled backup runs once at start.
fn backup_config(data_dir: PathBuf, backups: PathBuf) -> Config {
    let text = format!(
        "{EXAMPLE}\nbackup: {{\ndestination: backups\ninterval: 1h\nrun_on_start: true\n}}\n"
    );
    let mut config: Config = ktav::from_str(&text).expect("server KTAV with a backup schedule");
    config.backup.as_mut().expect("backup schedule").destination = backups;
    config.argon2_concurrent_max = 64;
    finish_config(
        config,
        data_dir,
        "127.0.0.1:0".to_owned(),
        "127.0.0.1:0".to_owned(),
    )
}

/// A server config with the listener and observability addresses under the
/// test's control (both are bound by the launcher, in that order).
fn bind_config(data_dir: PathBuf, listener: String, observability: String) -> Config {
    let config = Config {
        data_dir: data_dir.clone(),
        logging: LoggingConfig {
            level: "warn".into(),
            slow_query_threshold_ms: 0,
            file: None,
            flush_interval_ms: 2000,
        },
        durability: Default::default(),
        transactions: Default::default(),
        tokens: Default::default(),
        kdf_defaults: KdfConfig {
            memory_kb: 19_456,
            time: 2,
            parallelism: 1,
            argon2_version: 0x13,
        },
        argon2_concurrent_max: 64,
        listeners: Vec::new(),
        tls: TlsConfig {
            cert_path: data_dir.join("cert.pem"),
            key_path: data_dir.join("key.pem"),
        },
        security: crate::config::SecurityConfig::default(),
        audit: Default::default(),
        observability: ObservabilityConfig {
            addr: "127.0.0.1:0".into(),
            allow_public_metrics: false,
        },
        replication: None,
        backup: None,
    };
    // The REQUESTED listener goes through unchanged: an occupied address is the
    // fault the rollback tests inject, so replacing it with an ephemeral one
    // would make those tests boot successfully instead of rolling back.
    finish_config(config, data_dir, listener, observability)
}

fn finish_config(
    mut config: Config,
    data_dir: PathBuf,
    listener: String,
    observability: String,
) -> Config {
    config.data_dir = data_dir.clone();
    // ALWAYS one ephemeral listener: `EXAMPLE` carries the shipped unit's
    // fixed `7331-7333`, and a test must never bind those (two tests, or a
    // developer's own server, would collide).
    config.listeners = vec![listener_config(listener)];
    config.tls = TlsConfig {
        cert_path: data_dir.join("cert.pem"),
        key_path: data_dir.join("key.pem"),
    };
    config.observability.addr = observability;
    config.observability.allow_public_metrics = false;
    config
}

fn listener_config(addr: String) -> ListenerConfig {
    ListenerConfig {
        kind: ListenerKind::Http,
        addr,
        profile: None,
        http: HttpListenerSettings {
            tls: Some(false),
            ..Default::default()
        },
        path: None,
        kdf_override: None,
        browser_origin_allowlist: Vec::new(),
    }
}

/// In-memory `app`/`main` fixture for the observability-rollback test's open
/// transaction (the same shape the activity tests' `parked()` helper uses).
async fn open_app_db_for_rollback(
    owner: &shamir_db::access::Actor,
) -> std::sync::Arc<shamir_db::ShamirDb> {
    use shamir_db::engine::repo::{BoxRepoFactory, RepoConfig};
    use shamir_db::engine::table::TableConfig;
    let db = shamir_db::ShamirDb::init_memory()
        .await
        .expect("in-memory db");
    db.create_db_as("app", owner.clone())
        .await
        .expect("create db");
    db.add_repo_as(
        "app",
        RepoConfig::new("main", BoxRepoFactory::in_memory()).add_table(TableConfig::new("items")),
        owner.clone(),
    )
    .await
    .expect("add repo");
    std::sync::Arc::new(db)
}

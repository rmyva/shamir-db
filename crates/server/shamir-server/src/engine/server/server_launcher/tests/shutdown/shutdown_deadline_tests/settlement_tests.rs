//! Settlement-honesty tests for shutdown round 3: a join error, a cancelled
//! join, an outer timeout, and an HTTP drain failure are NOT proof that the
//! shutdown work ended — each must leave the typed stage in the report and
//! the data-directory lock in place.
//!
//! Each test names the broken implementation it catches in its comment.
//! The leak tests deliberately leak the lock `File`, so they run against a
//! PARENT-OWNED directory under the system temp (unique name, cleanup that
//! tolerates failure) instead of `tempfile::tempdir()`: the leaked handle
//! would keep the tempdir's own directory locked on Windows and make the
//! `TempDir` drop fail while the process is still alive.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use shamir_db::{ShutdownReport, ShutdownWaitStage};
use shamir_sync::durability::tx::tx_registry::{ReaperTask, TxRegistry};
use shamir_test_diag::{try_install_global_tracing_ring, FlakeGuard};
use tokio::sync::Semaphore;

use super::*;

use crate::server::server_handle::{
    arm_fallback_drain_join_probe, arm_meta_snapshot_join_probe, http_listener_settlement,
    join_tx_reaper, stop_release_sources_and_drain_tails, HttpListenerOutcome, ReleaseStopBounds,
    Settlement, REAPER_POST_ABORT_JOIN,
};

/// Parent-owned unique directory under the system temp; deletion is
/// best-effort AFTER the test body (a leaked handle may keep it locked).
fn leaked_lock_dir(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("shamir-2034r3-{}-{}", label, std::process::id()));
    let _ = std::fs::remove_dir_all(&dir); // tolerate a leftover from a prior run
    std::fs::create_dir_all(dir.join("data")).expect("parent-owned data dir");
    dir
}

/// A FAILED join (the reaper task panicked) must NOT report the release
/// sources as drained: `join_tx_reaper` returns `Unsettled` and the report
/// carries an error. Broken implementation caught: the old helper returned
/// `true` (settled) on any joined handle, including a panic.
#[test]
fn a_failed_reaper_join_is_not_proof() {
    let _ = try_install_global_tracing_ring();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime");
    runtime.block_on(async {
        let mut reaper = Some(ReaperTask {
            handle: tokio::spawn(async move { panic!("reaper exploded") }),
        });
        let mut report = ShutdownReport::default();
        let settled = join_tx_reaper(&mut reaper, None, &mut report).await;
        assert!(
            matches!(settled, Settlement::Unsettled(_)),
            "a panicking reaper join is NOT proof: {settled:?}"
        );
        assert!(!report.is_clean(), "the panic must reach the report");
    });
}

/// A CANCELLED join (aborted by someone else, no clean completion) is NOT
/// proof either. Broken implementation caught: cancelled joins were silently
/// treated as the expected abort end everywhere.
#[test]
fn a_cancelled_reaper_join_is_not_proof() {
    let _ = try_install_global_tracing_ring();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime");
    runtime.block_on(async {
        let handle = tokio::spawn(async {
            tokio::time::sleep(Duration::from_secs(3600)).await;
        });
        handle.abort();
        let mut reaper = Some(ReaperTask { handle });
        let mut report = ShutdownReport::default();
        let settled = join_tx_reaper(&mut reaper, None, &mut report).await;
        assert!(
            matches!(settled, Settlement::Unsettled(_)),
            "a cancelled join without our abort is NOT proof: {settled:?}"
        );
        assert!(!report.is_clean());
    });
}

/// When the reaper did not settle, the ONE ordered stop must return
/// `Unsettled` BEFORE it clears the registry or closes the tails: the
/// registry keeps its parked transaction (a producer may still register
/// releases), so a repeated call stays a safe no-op. Broken implementation
/// caught: a helper that cleared the registry and reported success after a
/// failed reaper join.
#[test]
fn an_unsettled_reaper_keeps_the_registry() {
    let _ = try_install_global_tracing_ring();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime");
    runtime.block_on(async {
        let registry = Arc::new(TxRegistry::new());
        // Park a REAL transaction (same shape as the observability-rollback
        // probe): `remove_all` would empty the registry.
        let owner = shamir_db::access::Actor::User(shamir_db::access::principal64([4; 16]));
        let db = open_app_db_for_rollback(&owner).await;
        let (ctx, snapshot) = db
            .tx_begin_as(owner.clone(), "app", "main", "pessimistic")
            .await
            .expect("begin interactive tx");
        let handle = ctx.tx_id.0;
        let repo = db
            .get_db("app")
            .expect("fixture db")
            .get_repo("main")
            .expect("fixture repo");
        let release: crate::tx_registry::TxAbortRelease =
            Arc::new(move |parked: shamir_tx::TxContext| {
                let repo = repo.clone();
                Box::pin(async move {
                    shamir_db::engine::tx::release_interactive_tx_locks(&parked, repo.core()).await;
                })
                    as std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>
            });
        registry
            .register(
                handle,
                crate::tx_registry::InteractiveTx::new(
                    ctx,
                    snapshot,
                    [1; 32],
                    [1; 16],
                    "app".into(),
                    "main".into(),
                    Duration::from_secs(3600),
                    release,
                ),
            )
            .expect("park the marker tx");
        assert!(!registry.is_empty());
        let mut reaper = Some(ReaperTask {
            handle: tokio::spawn(async move { panic!("reaper exploded") }),
        });
        let tails = registry.release_tails();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let settled = stop_release_sources_and_drain_tails(
            &mut reaper,
            Some(&registry),
            tails.as_ref(),
            ReleaseStopBounds {
                reaper: Some((deadline, Duration::from_secs(5))),
                tail_deadline: deadline,
                tail_budget: Duration::from_secs(5),
            },
            "settlement test",
            &mut ShutdownReport::default(),
        )
        .await;
        assert!(
            matches!(settled, Settlement::Unsettled(_)),
            "the stop must fail closed: {settled:?}"
        );
        assert!(
            !registry.is_empty(),
            "the registry must NOT be cleared while the reaper is unproven"
        );
    });
}

/// An OUTER deadline expiry is an unsettled outcome: the fallback drain joins
/// the marker, but the data-directory lock stays held and the typed
/// `OuterShutdownDeadline` stage is recorded. Broken implementation caught:
/// the owner drain that consumed the guard whenever its own waits settled,
/// ignoring the outer timeout.
#[test]
fn an_outer_timeout_does_not_release_the_lock() {
    on_big_stack(|| async {
        let guard = FlakeGuard::with_watchdog(
            "an_outer_timeout_does_not_release_the_lock",
            Duration::from_secs(300),
        );
        try_install_global_tracing_ring();
        let root = leaked_lock_dir("outer-timeout");
        let data_dir = root.join("data");
        let backups = root.join("backups");
        create_private_dir(&backups).expect("backups dir");

        let parked = Arc::new(Semaphore::new(0));
        let gate = Arc::new((
            Mutex::new(WorkerState::default()),
            std::sync::Condvar::new(),
        ));
        let completed = Arc::new(AtomicBool::new(false));
        let handle = ServerLauncher::new(backup_config(data_dir.clone(), backups))
            .with_bootstrap(BootstrapMode::Skip)
            .launch_with_backup_test({
                let parked = Arc::clone(&parked);
                let gate = Arc::clone(&gate);
                move |resolved, registry, owner, shutdown| {
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
                        || {},
                    )
                }
            })
            .await
            .expect("launch real server with the scheduled backup worker");
        drop(
            timeout(BOUND, parked.acquire())
                .await
                .expect("the scheduled run must reach its post-run callback")
                .expect("parked semaphore open"),
        );

        let (fallback_tx, fallback_rx) = tokio::sync::oneshot::channel::<()>();
        let _fallback_probe = arm_fallback_drain_join_probe(move || {
            let _ = fallback_tx.send(());
        });
        let shutdown = tokio::spawn(crate::runtime::drain_server(
            handle,
            Duration::from_millis(150),
        ));
        timeout(BOUND, fallback_rx)
            .await
            .expect("the outer timeout must fire (fallback join marker)")
            .expect("fallback probe channel");
        guard.mark("outer timeout expired with the worker parked");
        assert!(
            crate::maintenance_lock::acquire_server(&data_dir).is_err(),
            "after an outer timeout the lock must STILL be held while the worker runs"
        );
        release(&gate);
        let report = timeout(BOUND, shutdown)
            .await
            .expect("the drain must finish once the worker was released")
            .expect("shutdown task");
        assert_eq!(
            report.first_expired_owner_wait().map(|w| w.stage),
            Some(shamir_db::ShutdownWaitStage::OuterShutdownDeadline),
            "the outer timeout must be recorded TYPED: {report:?}"
        );
        assert!(
            crate::maintenance_lock::acquire_server(&data_dir).is_err(),
            "the lock must stay held even after the worker exited (leaked on purpose)"
        );
    });
}

/// An HTTP drain failure (login workers or audit consumer unsettled) makes
/// the report UNCLEAN with a typed stage. Broken implementation caught: the
/// HTTP listener that logged the drain failures and returned success.
#[test]
fn an_http_drain_failure_makes_the_report_unclean() {
    let mut report = ShutdownReport::default();
    let outcome = HttpListenerOutcome {
        login_workers_settled: false,
        login_worker_budget: Duration::from_secs(7),
        audit_consumer_settled: true,
        audit_consumer_budget: Duration::from_secs(2),
    };
    let settled = http_listener_settlement(&outcome, &mut report);
    assert!(
        matches!(settled, Settlement::Unsettled(_)),
        "an unsettled login-worker drain is NOT proof: {settled:?}"
    );
    assert!(
        !report.is_clean(),
        "the report must be unclean (exit 70, lock kept): {report:?}"
    );
    assert_eq!(
        report.first_expired_owner_wait().map(|w| w.stage),
        Some(shamir_db::ShutdownWaitStage::HttpLoginWorkerDrain),
        "the drain failure must be recorded TYPED: {report:?}"
    );
}

/// The panic-mapped result flows through the SAME bounded-shutdown decision:
/// exit 70 / abnormal selects the bounded runtime shutdown; a clean exit does
/// not. Broken implementation caught: a panic bypassing
/// `exits_through_bounded_shutdown` (unbounded runtime drop during unwind).
#[test]
fn the_panic_mapped_result_takes_the_bounded_shutdown() {
    use crate::runtime::{exits_through_bounded_shutdown, PANIC_EXIT_CODE};
    // The panic mapping is exactly `Ok((PANIC_EXIT_CODE, true))` in both
    // runtime owners; the decision must select the bounded shutdown for it.
    assert!(exits_through_bounded_shutdown(false, Some(true)));
    assert!(!exits_through_bounded_shutdown(false, Some(false)));
    assert!(exits_through_bounded_shutdown(true, None));
    assert_eq!(PANIC_EXIT_CODE, 70, "a panic is UNCLEAN: exit 70");
}

/// The post-abort reaper join has its OWN named bound: a reaper that never
/// yields after its abort must leave the stop UNSETTLED after that bound,
/// not hang forever. Broken implementation caught: the unbounded post-abort
/// await in `join_tx_reaper`.
#[test]
fn the_post_abort_reaper_join_is_bounded() {
    let _ = try_install_global_tracing_ring();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime");
    runtime.block_on(async {
        // A task with no await point: the abort cannot land, so the
        // post-abort join can only be settled by its bound.
        let handle = tokio::spawn(async {
            loop {
                std::hint::black_box(1);
            }
        });
        let mut reaper = Some(ReaperTask { handle });
        let mut report = ShutdownReport::default();
        let deadline = tokio::time::Instant::now() + Duration::from_millis(20);
        let settled = join_tx_reaper(
            &mut reaper,
            Some((deadline, Duration::from_millis(20))),
            &mut report,
        )
        .await;
        assert!(
            matches!(settled, Settlement::Unsettled(_)),
            "an unyielding reaper must be UNSETTLED: {settled:?}"
        );
        // The reaper stays in place (a repeated call is a safe no-op).
        assert!(reaper.is_some(), "an unproven reaper handle stays in place");
        assert!(
            report
                .errors()
                .iter()
                .any(|error| error.contains("NOT proven drained")),
            "the report must say the producer is not proven drained: {report:?}"
        );
        // Best effort: stop the spinning task so the test process exits.
        if let Some(task) = reaper.as_ref() {
            task.handle.abort();
        }
        let _ = REAPER_POST_ABORT_JOIN;
    });
}

/// Like [`on_big_stack`], but with EXACTLY ONE blocking thread: the test can
/// then occupy the whole pool with one parked job instead of guessing how
/// many workers a default pool would hand out.
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

/// Public `ServerHandle::shutdown()` reads the REAL `drain_deadline()` (the
/// deadline override was deleted): on a loaded runner the 30 s outer deadline
/// could fire before the meta-snapshot stage was ever reached, and the OUTER
/// report (token GC / audit teardown / flush timeouts) MASKED the very stage
/// this test claims to bound — the broken implementation this rewrite
/// catches. The test now DRIVES the stage: it proves the meta-snapshot join
/// is REACHED while the blocking pool is parked (one-shot probe handshake,
/// bounded by `BOUND`), THEN releases the occupant and asserts the TYPED
/// report state (`expired_owner_waits` carrying `MetaSnapshotJoin` with its
/// 5 s budget), keeping only the unclean/exit-70 assertions besides. The old
/// `pool_still_occupied` check is replaced: after the handshake the pool was
/// PROVEN parked at the stage entry, so the meaningful assertion is the typed
/// expiry recorded by that stage, not whether the job is still parked after
/// the test deliberately released it.
#[test]
fn public_shutdown_bounds_meta_snapshot_join_without_outer_deadline() {
    on_single_blocking_thread(|| async {
        let guard = FlakeGuard::with_watchdog(
            "public_shutdown_bounds_meta_snapshot_join_without_outer_deadline",
            Duration::from_secs(150),
        );
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

        // The stage must be PROVEN reached with the pool parked.
        let (snap_tx, snap_rx) = tokio::sync::oneshot::channel::<()>();
        arm_meta_snapshot_join_probe(move || {
            let _ = snap_tx.send(());
        });
        let shutdown = tokio::spawn(handle.shutdown());
        timeout(BOUND, snap_rx)
            .await
            .expect("the meta-snapshot join must be REACHED with the blocking pool parked")
            .expect("meta-snapshot join probe channel");
        guard.mark("meta-snapshot join entered while the pool was parked");

        // Release the occupant, let shutdown() settle, assert TYPED state.
        drop(occupant);
        let report = timeout(BOUND, shutdown)
            .await
            .expect("public shutdown must settle once the pool was released")
            .expect("shutdown task");
        guard.mark("shutdown returned after the release");

        assert!(
            report
                .expired_owner_waits()
                .iter()
                .any(|wait| wait.stage == ShutdownWaitStage::MetaSnapshotJoin),
            "the meta-snapshot join expiry must be recorded TYPED: {report:?}"
        );
        assert_eq!(
            report.first_expired_owner_wait().map(|wait| wait.stage),
            Some(ShutdownWaitStage::MetaSnapshotJoin),
            "the FIRST expired wait must be the meta-snapshot join (the stage the test \\
             drove), not a masked outer-deadline entry: {report:?}"
        );
        assert_eq!(
            report.first_expired_owner_wait().map(|wait| wait.budget),
            Some(Duration::from_secs(5)),
            "the recorded budget must be the meta-snapshot join's 5 s cap: {report:?}"
        );
        assert!(!report.is_clean(), "the report must be unclean: {report:?}");
        assert_eq!(
            report.exit_code(),
            70,
            "the report must exit 70: {report:?}"
        );
    });
}

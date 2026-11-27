//! A failed launch's cleanup must be REPORTED, never silently dropped: every
//! audit/repository failure the shared teardown recorded is logged at error
//! level by [`log_rollback_cleanup`], and the boot error still wins.
//!
//! Mutant killed: the function's body removed, or its `tracing::error!` level
//! lowered to debug — the ring captures debug too, so the level assertion is
//! what fails, not only the "no record at all" case.
//!
//! This first test only checks the FORMATTER (`log_rollback_cleanup` given a
//! hand-built report). The real failed-launch wiring — a launch that fails
//! AFTER the stores opened, the REAL teardown-produced report, the stage
//! name, and the stores actually released — is covered by
//! [`a_real_failed_launch_after_the_stores_open_reports_real_cleanup`] below.

use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use shamir_connect::server::lockout::{
    InMemoryLockoutStore, LockoutSnapshot, LockoutSnapshotError, LockoutSnapshotSink,
};
use shamir_connect::server::rate_limit::InMemoryRateLimiter;
use shamir_db::ShutdownReport;
use shamir_private_dir::create_private_dir;
use shamir_test_diag::{ring::shared_ring, try_install_global_tracing_ring};
use tokio_util::sync::CancellationToken;

use super::rollback_cleanup::{join_meta_snapshot_settled, log_rollback_cleanup};
use crate::config::{
    Config, KdfConfig, ListenerConfig, ListenerKind, LoggingConfig, ObservabilityConfig, TlsConfig,
};
use crate::db_handler::NodeMode;
use crate::server::meta_sinks::spawn_meta_snapshot_task;
use crate::server::server_launcher::audit_teardown::inject_teardown_error;
use crate::server::{BootstrapMode, ServerLauncher};

#[test]
fn launch_rollback_cleanup_logs_errors_and_repository_failures() {
    let _ = try_install_global_tracing_ring();

    // Both failures the report can carry, with inputs unique to this test so
    // no other process-wide log line can satisfy the assertions.
    let mut cleanup = ShutdownReport::default();
    cleanup.add_error("test rollback error alpha");
    cleanup.record(
        "test-repo-beta",
        Err("test repository failure gamma".to_string()),
    );
    log_rollback_cleanup("test-rollback-stage", &cleanup);

    let records = shared_ring().snapshot();
    let stage_field = "stage=\"test-rollback-stage\"";
    let events: Vec<_> = records
        .iter()
        .filter(|record| {
            record.message.contains("launch rollback cleanup error")
                && record.message.contains(stage_field)
        })
        .collect();
    assert!(
        events.iter().any(|record| record.level == "ERROR"
            && record
                .message
                .contains("error=\"test rollback error alpha\"")),
        "every rollback cleanup error must be logged at ERROR for the exact input: {records:?}"
    );
    assert!(
        events.iter().any(|record| record.level == "ERROR"
            && record.message.contains("repository=\"test-repo-beta\"")
            && record
                .message
                .contains("error=\"test repository failure gamma\"")),
        "every rollback repository failure must be logged at ERROR for the exact input: \
         {records:?}"
    );
}

/// A lockout snapshot sink that only reports each save: the final snapshot job
/// calls it, so a save after the occupant was released proves the job had been
/// submitted to the blocking pool and queued behind the occupant.
struct SaveSignal(std::sync::Mutex<std::sync::mpsc::Sender<()>>);

impl LockoutSnapshotSink for SaveSignal {
    fn save(&self, _snapshot: &LockoutSnapshot) -> Result<(), LockoutSnapshotError> {
        let _ = self.0.lock().expect("save signal sender").send(());
        Ok(())
    }

    fn load(&self) -> Result<Option<LockoutSnapshot>, LockoutSnapshotError> {
        Ok(None)
    }
}

/// Polls a future exactly once without waiting for it.
async fn poll_once<F: Future + Unpin>(future: &mut F) -> Poll<F::Output> {
    std::future::poll_fn(|cx| Poll::Ready(Pin::new(&mut *future).poll(cx))).await
}

/// The meta-snapshot join of a failed launch is bounded: the task's final
/// snapshot is a blocking-pool job, so with the pool saturated an unbounded
/// `handle.await` never returned the boot error. The pool's only blocking thread
/// is parked before the task is told to stop; the join must stay pending just
/// before its 5 s bound, return once the bound passed, log the expiry for its
/// stage, and return while the pool is still occupied.
///
/// The clock is PAUSED and advanced explicitly (no real-time wait, no race): the
/// snapshot task is the only runnable task, and it is given several scheduler
/// turns to run up to its queued blocking job before the join is first polled.
///
/// Mutants: the bound removed (`task.handle.await`) leaves the join pending
/// after the bound and fails the last poll; a shorter bound completes it before
/// the bound and fails the middle poll; a text that does not name the stage or
/// the cause fails the log assertion.
#[test]
fn rollback_meta_snapshot_join_is_bounded_with_a_saturated_blocking_pool() {
    let _ = try_install_global_tracing_ring();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .max_blocking_threads(1)
        .build()
        .expect("test runtime");
    runtime.block_on(async {
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let occupant_done = Arc::new(AtomicBool::new(false));
        let job_done = Arc::clone(&occupant_done);
        // Detached on purpose: the sender's drop releases the job.
        drop(tokio::task::spawn_blocking(move || {
            let _ = started_tx.send(());
            let _ = release_rx.recv();
            job_done.store(true, Ordering::SeqCst);
        }));
        started_rx
            .recv_timeout(Duration::from_secs(30))
            .expect("the occupying job must start on the only blocking thread");

        let shutdown = CancellationToken::new();
        let (saved_tx, saved_rx) = std::sync::mpsc::channel();
        let task = spawn_meta_snapshot_task(
            Arc::new(InMemoryLockoutStore::with_snapshot_sink_and_observer(
                Arc::new(SaveSignal(std::sync::Mutex::new(saved_tx))),
                None,
            )),
            Arc::new(InMemoryRateLimiter::new(0)),
            shutdown.clone(),
        );
        shutdown.cancel();
        // The task's first tick is immediate and the cancel wins its select: a few
        // turns take it up to the final snapshot job queued behind the occupant.
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }

        let mut join = Box::pin(join_meta_snapshot_settled(
            "test-rollback-meta-join",
            Some(task),
        ));
        assert!(
            poll_once(&mut join).await.is_pending(),
            "the join must wait for the stuck task"
        );
        tokio::time::advance(Duration::from_millis(4_990)).await;
        assert!(
            poll_once(&mut join).await.is_pending(),
            "the join must still be waiting just before its 5 s bound"
        );
        tokio::time::advance(Duration::from_millis(20)).await;
        let returned = poll_once(&mut join).await;
        let still_occupied = !occupant_done.load(Ordering::SeqCst);
        drop(release_tx);
        assert!(
            returned.is_ready(),
            "the join must return once its bound passed, with the pool saturated"
        );
        assert!(
            still_occupied,
            "the join must return while the pool is still occupied"
        );
        // The occupant is released: the final snapshot job queued behind it must
        // now run and save. A task that never submitted the job (or submitted
        // something else) leaves this wait to its bound.
        saved_rx
            .recv_timeout(Duration::from_secs(30))
            .expect("the final snapshot job must have been queued behind the occupant");

        let records = shared_ring().snapshot();
        assert!(
            records.iter().any(|record| record.level == "ERROR"
                && record.message.contains("launch rollback cleanup error")
                && record.message.contains("stage=\"test-rollback-meta-join\"")
                && record
                    .message
                    .contains("error=\"meta snapshot task did not stop within 5s")),
            "the expired join must be logged at ERROR: {records:?}"
        );
    });
}

/// Generous bound for a real (failed or relaunched) boot and its shutdown.
const ROLLBACK_BOUND: std::time::Duration = std::time::Duration::from_secs(180);

/// A server config whose data listener binds `127.0.0.1:<port>`; port 0 lets
/// the OS pick. No fixed observability port, so parallel tests never collide.
fn rollback_boot_config(data_dir: PathBuf, port: u16) -> Config {
    Config {
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
        listeners: vec![ListenerConfig {
            kind: ListenerKind::Http,
            addr: format!("127.0.0.1:{port}"),
            profile: None,
            http: crate::boot::config::HttpListenerSettings {
                tls: Some(false),
                ..Default::default()
            },
            path: None,
            kdf_override: None,
            browser_origin_allowlist: Vec::new(),
        }],
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
    }
}

/// A REAL failed launch whose failure fires AFTER the stores were opened: the
/// HTTP listener cannot bind because the test already holds its port, so
/// `launch()` rolls back through the shared audit teardown and
/// `log_rollback_cleanup("listener bind", ..)` with the REAL cleanup report
/// object. A teardown failure after the stores opened is not deterministically
/// reproducible through the bind failure alone, so the real report is given
/// one typed error through the cfg(test) seam INSIDE `teardown_audit` itself,
/// added to the teardown's own report before it returns — so a rollback arm
/// that discards teardown's entries or logs a different report object fails
/// the log assertion. The teardown's own failure-to-entry mapping is pinned by
/// the audit-teardown tests; this test pins the report-object identity, the
/// logging path, the stage name, the boot error and the store release.
///
/// Mutants killed: the rollback logging a FABRICATED report instead of the
/// real one; the cleanup log dropped entirely; the boot error swallowed; and
/// the rollback leaking the stores (the relaunch then cannot take the
/// data_dir's fjall locks and fails).
#[test]
fn a_real_failed_launch_after_the_stores_open_reports_real_cleanup() {
    let _ = try_install_global_tracing_ring();
    let blocker = std::net::TcpListener::bind("127.0.0.1:0").expect("bind the port blocker");
    let port = blocker.local_addr().expect("blocker address").port();
    let _injection = inject_teardown_error(
        "injected rollback teardown failure after the stores opened".to_string(),
    );

    let temp = tempfile::tempdir().expect("test root");
    let data_dir = temp.path().join("data");
    create_private_dir(&data_dir).expect("private data directory");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime");

    let deadline = std::time::Instant::now() + ROLLBACK_BOUND;

    let boot_data_dir = data_dir.clone();
    let launch = runtime.block_on(async {
        tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            Box::pin(
                ServerLauncher {
                    config: rollback_boot_config(boot_data_dir, port),
                    bootstrap: BootstrapMode::Skip,
                    node_mode: NodeMode::ReadWrite,
                }
                .launch(),
            ),
        )
        .await
    });
    let launch = launch.expect("failed launch bounded");
    let error = match launch {
        Ok(_handle) => panic!("the held port must fail the launch after the stores opened"),
        Err(error) => error,
    };
    assert!(
        matches!(&error, crate::server::BootError::Bind(message)
            if message.contains(&port.to_string())),
        "the launch must fail on the listener bind with the held port: {error}"
    );

    let records = shared_ring().snapshot();
    assert!(
        records.iter().any(|record| record.level == "ERROR"
            && record.message.contains("launch rollback cleanup error")
            && record.message.contains("stage=\"listener bind\"")
            && record
                .message
                .contains("error=\"injected rollback teardown failure after the stores opened\"")),
        "the REAL cleanup report's error must be logged at ERROR for stage \
         \"listener bind\": {records:?}"
    );

    // The stores opened before the failure and must be RELEASED by the
    // rollback: a relaunch on the same data_dir takes the fjall locks again.
    let relaunch_data_dir = data_dir.clone();
    let handle = runtime
        .block_on(async {
            tokio::time::timeout_at(
                tokio::time::Instant::from_std(deadline),
                Box::pin(
                    ServerLauncher {
                        config: rollback_boot_config(relaunch_data_dir, 0),
                        bootstrap: BootstrapMode::Skip,
                        node_mode: NodeMode::ReadWrite,
                    }
                    .launch(),
                ),
            )
            .await
        })
        .expect("relaunch bounded")
        .expect("the relaunch must take the stores back: the rollback released them");
    let clean = runtime
        .block_on(async {
            tokio::time::timeout_at(
                tokio::time::Instant::from_std(deadline),
                crate::runtime::drain_server(handle, std::time::Duration::from_secs(30)),
            )
            .await
        })
        .expect("relaunch shutdown bounded");
    assert!(
        clean.is_clean(),
        "the relaunch shutdown must be clean: {clean:?}"
    );
}

//! Ordered-shutdown gate: the store-release barrier's outcome is REAL, not a
//! log line, and it reaches the shutdown report.
//!
//! [`a_held_store_lock_keeps_the_release_barrier_failing`] pins the source of
//! that report entry: a fjall store root whose `lock` file is held by another
//! handle makes [`wait_stores_released`] return the still-locked root when the
//! budget expires, and `Ok` once the lock is released. Mutant: a barrier that
//! reports `Ok` (or an empty list) on a held lock, or one that never stops
//! waiting, makes the first or the second half of the test fail.
//!
//! [`a_store_that_outlives_the_teardown_reaches_the_shutdown_report`] drives
//! the REAL launcher: a stale store root created before boot is held for the
//! whole shutdown, so the release barrier times out and the entry must appear
//! in `ShutdownReport::errors()`. Mutant: the former behaviour, which only
//! logged a `tracing::warn!` and left the report clean — the assertion then
//! fails with a clean report and exit code 0.

use std::future::Future;
use std::path::PathBuf;
use std::time::Duration;

use shamir_private_dir::create_private_dir;
use shamir_test_diag::{try_install_global_tracing_ring, FlakeGuard};

use crate::config::{
    Config, KdfConfig, ListenerConfig, ListenerKind, LoggingConfig, ObservabilityConfig, TlsConfig,
};
use crate::db_handler::NodeMode;
use crate::server::{release_stores, ReleaseOutcome};
use crate::server::{BootstrapMode, ServerLauncher};

/// Generous bound: the Windows CI runner is up to 10x slower than a
/// workstation, and the launcher test deliberately waits out the barrier's own
/// release timeout.
const BOUND: Duration = Duration::from_secs(180);

#[test]
fn a_held_store_lock_keeps_the_release_barrier_failing() {
    on_big_stack(|| async {
        tokio::time::timeout(BOUND, run_held_store_lock())
            .await
            .expect("store-release barrier test bounded");
    });
}

#[test]
fn a_store_that_outlives_the_teardown_reaches_the_shutdown_report() {
    let guard = FlakeGuard::with_watchdog(
        "a_store_that_outlives_the_teardown_reaches_the_shutdown_report",
        Duration::from_secs(300),
    );
    try_install_global_tracing_ring();
    let temp = tempfile::tempdir().expect("test root");
    let data_dir = temp.path().join("data");
    create_private_dir(&data_dir).expect("private data directory");
    // A stale store root (fjall marks one by `lock` + `version`) with its lock
    // HELD for the whole shutdown: boot ignores the unknown directory, the
    // release barrier finds it and times out.
    let stale = data_dir.join("stale_store");
    std::fs::create_dir_all(&stale).expect("stale store dir");
    std::fs::write(stale.join("version"), b"v").expect("version marker");
    std::fs::write(stale.join("lock"), b"").expect("lock file");
    let held = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(stale.join("lock"))
        .expect("open the stale store lock");
    held.try_lock()
        .expect("the test holds the stale store lock");
    guard.mark("stale store lock held before boot");

    // ONE runtime for the launch and the shutdown: dropping the launch runtime
    // first would tear down its blocking pool and any task still holding a
    // store, so the barrier's verdict would come from a dead runtime instead of
    // from the shutdown under test.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime");
    let data_dir_for_boot = data_dir.clone();
    let handle = runtime
        .block_on(async move {
            tokio::time::timeout(
                BOUND,
                Box::pin(
                    ServerLauncher {
                        config: boot_config(data_dir_for_boot),
                        bootstrap: BootstrapMode::Skip,
                        node_mode: NodeMode::ReadWrite,
                    }
                    .launch(),
                ),
            )
            .await
        })
        .expect("launcher boot within the bound")
        .expect("launcher boot");
    guard.mark("launcher booted");
    let report = runtime.block_on(async {
        tokio::time::timeout(
            BOUND,
            crate::runtime::drain_server(handle, Duration::from_secs(30)),
        )
        .await
        .expect("the embedding shutdown must finish within the bound")
    });
    guard.mark("shutdown drained with the store still locked");

    // Unconditional release, then assert: a failing claim never leaves the
    // lock behind for the next test.
    assert!(held.unlock().is_ok(), "release the stale store lock");
    let still_reported = report
        .errors()
        .iter()
        .any(|error| error.contains("stores still held after the release timeout"));
    assert!(
        still_reported,
        "a store that outlives the teardown must reach the shutdown report, not only a \
         `tracing::warn!` (mutant: the barrier's failure is logged and the report stays \
         clean): {report:?}"
    );
    assert!(
        report
            .errors()
            .iter()
            .any(|error| error.contains("stale_store")),
        "the report must NAME the store root it is still waiting for: {report:?}"
    );
    assert!(
        !report.is_clean(),
        "a store that outlived the teardown makes the shutdown unclean: {report:?}"
    );

    // DECIDED: a shutdown whose store-release stage did not PROVE release is
    // NOT clean, so the data-directory instance lock is NOT released — a
    // second launch on the SAME data directory must be refused with
    // `AlreadyRunning` (matched by CONTENT, never `is_err()`), and the leak
    // is intentional: the OS holds the lock until the process exits. The
    // leaked handle lives inside the test's tempdir; `TempDir`'s Drop
    // tolerates the locked file (Windows) and the dir is removed at process
    // end, so the leak test leaves no debris it depends on cleaning.
    let data_dir_for_reboot = data_dir.clone();
    let reboot = runtime
        .block_on(async move {
            tokio::time::timeout(
                BOUND,
                Box::pin(
                    ServerLauncher {
                        config: boot_config(data_dir_for_reboot),
                        bootstrap: BootstrapMode::Skip,
                        node_mode: NodeMode::ReadWrite,
                    }
                    .launch(),
                ),
            )
            .await
        })
        .expect("second boot within the bound");
    guard.mark("second launcher refused over the leaked instance lock");
    let message = match reboot {
        Err(error) => error.to_string(),
        Ok(_) => panic!("a leaked instance lock must refuse a second boot with AlreadyRunning"),
    };
    assert!(
        message.contains("already using") && message.contains(data_dir.to_str().unwrap_or("data")),
        "the second boot must fail with BootError::AlreadyRunning naming the data directory: \
         {message}"
    );
}

/// Hold one fjall-shaped store root's `lock` and observe the barrier from both
/// sides of the release.
async fn run_held_store_lock() {
    let temp = tempfile::tempdir().expect("test root");
    let store = temp.path().join("counters");
    std::fs::create_dir_all(&store).expect("store dir");
    std::fs::write(store.join("version"), b"v").expect("version marker");
    std::fs::write(store.join("lock"), b"").expect("lock file");
    let held = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(store.join("lock"))
        .expect("open store lock");
    held.try_lock().expect("the test holds the store lock");

    match release_stores(temp.path(), Duration::from_millis(100)).await {
        ReleaseOutcome::StillLocked(still) => assert_eq!(
            still,
            vec![store.clone()],
            "the barrier names the store root it is still waiting for"
        ),
        other => panic!(
            "a held store lock must be reported as StillLocked — `Released` would report a \
             released data directory while a writer still owns it — got {other:?}"
        ),
    }

    held.unlock().expect("release the store lock");
    assert!(
        matches!(
            release_stores(temp.path(), BOUND).await,
            ReleaseOutcome::Released
        ),
        "a released store lock must satisfy the barrier"
    );
    drop(held);
}

/// A saturated blocking pool must yield the store-release verdict in the REAL
/// shutdown report, not merely from a direct barrier call.
///
/// Mutant: the former unbounded `spawn_blocking(..).await` or generic deadline
/// expiry without a store-release verdict fails to return the required report.
#[test]
fn a_saturated_blocking_pool_reports_an_unverified_release() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .expect("test runtime");
    let temp = tempfile::tempdir().expect("test root");
    let data_dir = temp.path().join("data");
    create_private_dir(&data_dir).expect("private data directory");
    let handle = runtime
        .block_on(async {
            ServerLauncher {
                config: boot_config(data_dir.clone()),
                bootstrap: BootstrapMode::Skip,
                node_mode: NodeMode::ReadWrite,
            }
            .launch()
            .await
        })
        .expect("launch real server");

    // Occupy the ONLY blocking worker until explicitly released. The release
    // channel is also sent by Drop, so any failed assertion cannot strand it.
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let finished = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let job_finished = std::sync::Arc::clone(&finished);
    let occupied = runtime.spawn_blocking(move || {
        let _ = started_tx.send(());
        let _ = release_rx.recv_timeout(BOUND);
        job_finished.store(true, std::sync::atomic::Ordering::SeqCst);
    });
    let _release_on_drop = ReleaseBlockingWorker(release_tx);
    started_rx
        .recv_timeout(BOUND)
        .expect("the pool worker started the occupying job");

    // The timer must be built inside the runtime: `timeout` needs its context.
    let report = runtime
        .block_on(async {
            tokio::time::timeout(
                BOUND,
                crate::runtime::drain_server(handle, Duration::from_secs(3)),
            )
            .await
        })
        .expect("shutdown report bounded");
    let returned_while_parked = !finished.load(std::sync::atomic::Ordering::SeqCst);

    // Release before any assertions; the guard repeats this on unwind.
    drop(_release_on_drop);
    drop(occupied);

    assert!(
        returned_while_parked,
        "the shutdown report must return before the parked worker is released"
    );
    assert!(
        report
            .errors()
            .iter()
            .any(|error| error.contains("store release could not be verified")),
        "the report must name the specific store-release verdict, not only generic expiry: {report:?}"
    );
    assert!(
        !report
            .errors()
            .iter()
            .any(|error| error.contains("graceful shutdown exceeded deadline")),
        "a generic deadline expiry cannot stand in for the store-release verdict: {report:?}"
    );
    assert!(
        !report.is_clean(),
        "a store release that could not be verified makes shutdown unclean: {report:?}"
    );
}

/// A positive release budget still submits the lock probe; the saturated pool
/// keeps that probe queued until the stage's own budget expires.
#[test]
fn a_saturated_blocking_pool_submits_an_unverified_release_probe() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .expect("test runtime");
    let temp = tempfile::tempdir().expect("test root");
    let data_dir = temp.path().join("data");
    create_private_dir(&data_dir).expect("private data directory");
    let handle = runtime
        .block_on(async {
            ServerLauncher {
                config: boot_config(data_dir.clone()),
                bootstrap: BootstrapMode::Skip,
                node_mode: NodeMode::ReadWrite,
            }
            .launch()
            .await
        })
        .expect("launch real server");

    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let finished = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let job_finished = std::sync::Arc::clone(&finished);
    let occupied = runtime.spawn_blocking(move || {
        let _ = started_tx.send(());
        let _ = release_rx.recv_timeout(BOUND);
        job_finished.store(true, std::sync::atomic::Ordering::SeqCst);
    });
    let _release_on_drop = ReleaseBlockingWorker(release_tx);
    started_rx
        .recv_timeout(BOUND)
        .expect("the pool worker started the occupying job");

    // Shutdown stages precede store release. Under the 3s outer deadline the
    // release stage must submit a probe with positive remaining budget; the
    // saturated pool keeps it queued until that stage budget expires.
    // Construct the timeout within runtime context.
    let report = runtime
        .block_on(async {
            tokio::time::timeout(
                BOUND,
                crate::runtime::drain_server(handle, Duration::from_secs(3)),
            )
            .await
        })
        .expect("shutdown report bounded");
    let returned_while_parked = !finished.load(std::sync::atomic::Ordering::SeqCst);

    drop(_release_on_drop);
    drop(occupied);

    assert!(
        returned_while_parked,
        "shutdown returned before pool release"
    );
    assert!(
        report.errors().iter().any(|error| {
            error.contains("store release could not be verified")
                && error.contains(
                    "the store-release blocking job did not complete before its deadline",
                )
        }),
        "positive remaining budget must submit a probe and report its Unverified verdict, not generic outer expiry: {report:?}"
    );
    assert!(
        !report
            .errors()
            .iter()
            .any(|error| error.contains("graceful shutdown exceeded deadline")),
        "the store-release verdict must precede outer expiry: {report:?}"
    );
    assert!(
        !report.is_clean(),
        "unverified release is unclean: {report:?}"
    );
}

/// The `DiscoveryGap` verdict keeps its OWN wording in the REAL shutdown
/// report: discovery was incomplete, distinct from a probe that ended without
/// a result. The gap is planted by leaving directories below the discovery
/// walk's depth cap under `data_dir` — the store set is then unknowable by
/// construction, on every platform.
///
/// Mutant killed: the summary/detail texts of `UnverifiedReason::DiscoveryGap`
/// swapped with `ProbeError`'s, merged into one generic string, or dropped
/// from the report — the exact-substring and absence assertions fail.
#[test]
fn a_discovery_gap_is_reported_with_its_own_wording() {
    let guard = FlakeGuard::with_watchdog(
        "a_discovery_gap_is_reported_with_its_own_wording",
        Duration::from_secs(300),
    );
    try_install_global_tracing_ring();
    let temp = tempfile::tempdir().expect("test root");
    let data_dir = temp.path().join("data");
    create_private_dir(&data_dir).expect("private data directory");
    // Directories below the barrier walk's depth cap: discovery cannot
    // finish, so the release state is UNKNOWN (a gap, never a release).
    let mut deep = data_dir.clone();
    for name in ["a", "b", "c", "d", "e", "f", "g", "h"] {
        deep.push(name);
    }
    std::fs::create_dir_all(&deep).expect("directories below the depth cap");
    guard.mark("depth-cap directories planted");
    // ONE absolute deadline for the whole test: every stage waits for the
    // REMAINING time, never a fresh budget per stage.
    let deadline = std::time::Instant::now() + BOUND;

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime");
    let boot_data_dir = data_dir.clone();
    let handle = runtime
        .block_on(async {
            tokio::time::timeout_at(
                tokio::time::Instant::from_std(deadline),
                Box::pin(
                    ServerLauncher {
                        config: boot_config(boot_data_dir),
                        bootstrap: BootstrapMode::Skip,
                        node_mode: NodeMode::ReadWrite,
                    }
                    .launch(),
                ),
            )
            .await
        })
        .expect("launcher boot within the bound")
        .expect("launcher boot");
    guard.mark("launcher booted with the depth-cap gap planted");
    let report = runtime
        .block_on(async {
            tokio::time::timeout_at(
                tokio::time::Instant::from_std(deadline),
                crate::runtime::drain_server(handle, Duration::from_secs(30)),
            )
            .await
        })
        .expect("shutdown report bounded");
    guard.mark("drained with the depth-cap gap planted");

    assert!(
        report.errors().iter().any(
            |error| error.contains("store release could not be verified")
                && error.contains("store discovery was incomplete")
                && error.contains("the store set could not be fully discovered")
        ),
        "the DiscoveryGap verdict must keep its own wording in the report: {report:?}"
    );
    // Scoped to the store-release verdict: the audit close has its own
    // "ended without a result" wording that is unrelated to discovery.
    assert!(
        !report.errors().iter().any(|error| {
            error.contains("store release could not be verified")
                && error.contains("ended without a result")
        }),
        "a discovery gap must not be worded as a probe error: {report:?}"
    );
    assert!(
        !report.is_clean(),
        "a discovery gap makes shutdown unclean: {report:?}"
    );
}

/// The `ProbeError` verdict keeps its OWN wording in the REAL shutdown report:
/// the blocking job ended without a result, distinct from an incomplete
/// discovery. The error is planted as a store-shaped directory whose `lock`
/// marker is a DIRECTORY: the verifier classifies it as a store (`version`
/// file + `lock` entry), then cannot open the directory lock for reading and
/// writing on any platform.
///
/// Mutant killed: the summary/detail texts of `UnverifiedReason::ProbeError`
/// swapped with `DiscoveryGap`'s, merged into one generic string, or dropped
/// from the report — the exact-substring and absence assertions fail.
#[test]
fn a_probe_error_is_reported_with_its_own_wording() {
    let guard = FlakeGuard::with_watchdog(
        "a_probe_error_is_reported_with_its_own_wording",
        Duration::from_secs(300),
    );
    try_install_global_tracing_ring();
    let temp = tempfile::tempdir().expect("test root");
    let data_dir = temp.path().join("data");
    create_private_dir(&data_dir).expect("private data directory");
    let planted = data_dir.join("probe_error_store");
    // `lock` as a DIRECTORY: classified a store, unopenable as a lock file.
    std::fs::create_dir_all(planted.join("lock")).expect("`lock` directory marker");
    std::fs::write(planted.join("version"), b"v").expect("version marker");
    guard.mark("probe-error store planted");
    // ONE absolute deadline for the whole test: every stage waits for the
    // REMAINING time, never a fresh budget per stage.
    let deadline = std::time::Instant::now() + BOUND;

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime");
    let boot_data_dir = data_dir.clone();
    let handle = runtime
        .block_on(async {
            tokio::time::timeout_at(
                tokio::time::Instant::from_std(deadline),
                Box::pin(
                    ServerLauncher {
                        config: boot_config(boot_data_dir),
                        bootstrap: BootstrapMode::Skip,
                        node_mode: NodeMode::ReadWrite,
                    }
                    .launch(),
                ),
            )
            .await
        })
        .expect("launcher boot within the bound")
        .expect("launcher boot");
    guard.mark("launcher booted with the probe-error store planted");
    let report = runtime
        .block_on(async {
            tokio::time::timeout_at(
                tokio::time::Instant::from_std(deadline),
                crate::runtime::drain_server(handle, Duration::from_secs(30)),
            )
            .await
        })
        .expect("shutdown report bounded");
    guard.mark("drained with the probe-error store planted");

    assert!(
        report.errors().iter().any(
            |error| error.contains("store release could not be verified")
                && error.contains("the store-release blocking job ended without a result")
                && error.contains("discovery or a lock probe did not complete successfully")
        ),
        "the ProbeError verdict must keep its own wording in the report: {report:?}"
    );
    assert!(
        !report
            .errors()
            .iter()
            .any(|error| error.contains("store discovery was incomplete")),
        "a probe error must not be worded as a discovery gap: {report:?}"
    );
    assert!(
        !report.is_clean(),
        "a probe error makes shutdown unclean: {report:?}"
    );
}

/// Releases a parked blocking-pool worker even if an assertion unwinds.
struct ReleaseBlockingWorker(std::sync::mpsc::Sender<()>);

impl Drop for ReleaseBlockingWorker {
    fn drop(&mut self) {
        let _ = self.0.send(());
    }
}

/// A server config with no fixed ports: every listener and the observability
/// endpoint bind `127.0.0.1:0`, so parallel tests never share `7331-7333`.
fn boot_config(data_dir: PathBuf) -> Config {
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
            addr: "127.0.0.1:0".into(),
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

/// The `DiscoveryGap` report text: an undecidable store marker under the
/// data directory must reach the REAL shutdown report with the stage's
/// own summary and detail -- never as a release, a `StillLocked`, or the
/// generic outer expiry.
///
/// The dangling `version` marker is the exact discovery-gap fixture the
/// barrier tests use (`a_dangling_version_marker_is_a_discovery_gap`);
/// here it is planted before boot so the REAL shutdown's release stage
/// walks over it.
///
/// Mutant: the `UnverifiedReason::DiscoveryGap` arm of the report match
/// (`server_handle/core.rs`) swapped for another reason's text, dropped,
/// or routed to a bare log line -- the exact summary/detail/prefix
/// assertions then fail.
#[cfg(unix)]
#[test]
fn a_discovery_gap_reaches_the_shutdown_report_with_its_own_verdict() {
    let guard = FlakeGuard::with_watchdog(
        "a_discovery_gap_reaches_the_shutdown_report_with_its_own_verdict",
        Duration::from_secs(300),
    );
    try_install_global_tracing_ring();
    let temp = tempfile::tempdir().expect("test root");
    let data_dir = temp.path().join("data");
    create_private_dir(&data_dir).expect("private data directory");
    let gapped = data_dir.join("gap_store");
    std::fs::create_dir_all(&gapped).expect("gap store dir");
    std::os::unix::fs::symlink(
        gapped.join("missing-version-target"),
        gapped.join("version"),
    )
    .expect("dangling version marker");
    std::fs::write(gapped.join("lock"), b"").expect("lock marker");
    guard.mark("dangling version marker planted");

    let report = drain_with_undecidable_store(data_dir);
    guard.mark("shutdown drained");

    let error = report
        .errors()
        .iter()
        .find(|error| error.contains("store release could not be verified"))
        .expect("the discovery gap must reach the report as an unverified release");
    assert!(
        error.contains("store discovery was incomplete"),
        "the verdict must carry the DiscoveryGap summary: {error}"
    );
    assert!(
        error.contains(
            "the store set could not be fully discovered, or a discovered marker could no longer be verified",
        ),
        "the verdict must carry the DiscoveryGap detail: {error}"
    );
    assert!(
        error.contains("UNKNOWN") && error.contains("`Locked`"),
        "the verdict must leave the release state UNKNOWN with the relaunch warning: {error}"
    );
    assert!(
        !report.is_clean(),
        "an UNKNOWN release is unclean: {report:?}"
    );
    assert_eq!(report.exit_code(), 70, "unclean report: {report:?}");
}

/// The `ProbeError` report text: a discovered store whose `lock` marker
/// cannot be opened (here: a directory named `lock`) must reach the REAL
/// shutdown report with the stage's own summary and detail -- the probe
/// ran but observed nothing decidable.
///
/// Mutant: the `UnverifiedReason::ProbeError` arm of the report match
/// (`server_handle/core.rs`) swapped for another reason's text, dropped,
/// or routed to a bare log line -- the exact summary/detail/prefix
/// assertions then fail.
#[test]
fn a_probe_error_reaches_the_shutdown_report_with_its_own_verdict() {
    let guard = FlakeGuard::with_watchdog(
        "a_probe_error_reaches_the_shutdown_report_with_its_own_verdict",
        Duration::from_secs(300),
    );
    try_install_global_tracing_ring();
    let temp = tempfile::tempdir().expect("test root");
    let data_dir = temp.path().join("data");
    create_private_dir(&data_dir).expect("private data directory");
    let broken = data_dir.join("broken_store");
    std::fs::create_dir_all(broken.join("lock")).expect("a `lock` DIRECTORY");
    std::fs::write(broken.join("version"), b"3").expect("version marker");
    guard.mark("unopenable lock marker planted");

    let report = drain_with_undecidable_store(data_dir);
    guard.mark("shutdown drained");

    let error = report
        .errors()
        .iter()
        .find(|error| error.contains("store release could not be verified"))
        .expect("the probe error must reach the report as an unverified release");
    assert!(
        error.contains("the store-release blocking job ended without a result"),
        "the verdict must carry the ProbeError summary: {error}"
    );
    assert!(
        error.contains("discovery or a lock probe did not complete successfully"),
        "the verdict must carry the ProbeError detail: {error}"
    );
    assert!(
        !error.contains("store discovery was incomplete")
            && !error.contains("did not complete before its deadline"),
        "the ProbeError verdict must not borrow the DiscoveryGap or PoolStarved text: {error}"
    );
    assert!(
        error.contains("UNKNOWN") && error.contains("`Locked`"),
        "the verdict must leave the release state UNKNOWN with the relaunch warning: {error}"
    );
    assert!(
        !report.is_clean(),
        "an UNKNOWN release is unclean: {report:?}"
    );
    assert_eq!(report.exit_code(), 70, "unclean report: {report:?}");
}

/// One real launch + bounded drain over a data directory whose store set
/// the barrier cannot decide; shared by the two verdict-text tests.
fn drain_with_undecidable_store(data_dir: PathBuf) -> shamir_db::ShutdownReport {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime");
    let handle = runtime
        .block_on(async {
            tokio::time::timeout(
                BOUND,
                Box::pin(
                    ServerLauncher {
                        config: boot_config(data_dir),
                        bootstrap: BootstrapMode::Skip,
                        node_mode: NodeMode::ReadWrite,
                    }
                    .launch(),
                ),
            )
            .await
        })
        .expect("launcher boot within the bound")
        .expect("launcher boot");
    runtime.block_on(async {
        tokio::time::timeout(
            BOUND,
            crate::runtime::drain_server(handle, Duration::from_secs(30)),
        )
        .await
        .expect("the embedding shutdown must finish within the bound")
    })
}

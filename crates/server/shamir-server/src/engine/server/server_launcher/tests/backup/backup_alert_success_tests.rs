//! The alert SUCCESS inputs, on the real `/metrics` route: one scheduled run.

use shamir_private_dir::create_private_dir;
use shamir_test_diag::{try_install_global_tracing_ring, FlakeGuard};
use tokio::sync::mpsc;
use tokio::time::timeout;

use crate::boot::config::diagnostics::backup_online::metrics::backup_metrics_snapshot;
use crate::boot::config::diagnostics::backup_online::schedule::runner::spawn_for_test;
use crate::server::{BootstrapMode, ServerLauncher};
use crate::sync::durability::backup::online::set_snapshot_name_base_seconds;

use super::support::{
    await_outcome, metric_type, on_big_stack, optional_sample, sample, scheduled_config,
    scrape_metrics, TerminalOutcome, BOUND, OUTER, SNAPSHOT_NAME_BASE, TICK, WATCHDOG,
};

const ALERT_SUCCESS_TEST: &str = "live_alert_success_inputs_reach_the_real_metrics_route";

#[test]
fn live_alert_success_inputs_reach_the_real_metrics_route() {
    on_big_stack(|| async {
        timeout(OUTER, run_alert_success())
            .await
            .expect("live alert-success acceptance timed out");
    });
}

async fn run_alert_success() {
    let guard = FlakeGuard::with_watchdog(ALERT_SUCCESS_TEST, WATCHDOG);
    try_install_global_tracing_ring();
    set_snapshot_name_base_seconds(SNAPSHOT_NAME_BASE);
    let temp = tempfile::tempdir().expect("test root");
    let data_dir = temp.path().join("data");
    let backups = temp.path().join("backups");
    create_private_dir(&data_dir).expect("private data directory");
    create_private_dir(&backups).expect("private backup directory");

    // Nextest gives this test its own process: the pre-run zero.
    let baseline = backup_metrics_snapshot();
    assert_eq!(
        baseline.last_success_timestamp_seconds, 0,
        "a fresh test process must not have published a success yet"
    );

    let (send, mut receive) = mpsc::unbounded_channel::<TerminalOutcome>();
    // Cancels on the FIRST success, so no later run can intervene.
    let handle = ServerLauncher::new(scheduled_config(&data_dir, &backups, "1h", true, 2))
        .with_bootstrap(BootstrapMode::Skip)
        .launch_with_backup_test(move |resolved, registry, owner, shutdown| {
            let scheduler_shutdown = shutdown.child_token();
            let stop = scheduler_shutdown.clone();
            spawn_for_test(
                resolved,
                registry,
                owner,
                scheduler_shutdown,
                TICK,
                move |ok| {
                    if ok {
                        stop.cancel();
                    }
                    let _ = send.send(ok);
                },
                || {},
            )
        })
        .await
        .expect("launch a real server with the scheduled worker");
    guard.mark("server launched with a millisecond tick");
    let addr = handle
        .observability
        .as_ref()
        .expect("observability")
        .bound_addr;

    // The boot seed fills only the last-success STAMP cell; this series is
    // rendered solely from the last-success cell, so its absence proves no
    // copy has succeeded yet in this process.
    let pre_run = scrape_metrics(addr).await;
    assert!(
        optional_sample(&pre_run, "shamir_backup_duration_seconds").is_none(),
        "no success has happened yet, so the duration series must be absent: {pre_run}"
    );

    await_outcome(&mut receive, true, "the first scheduled success").await;
    guard.mark("first scheduled success reported");
    let frozen = backup_metrics_snapshot().last_success_timestamp_seconds;
    let route = scrape_metrics(addr).await;
    assert_eq!(
        sample(&route, "shamir_backup_last_success_timestamp_seconds"),
        frozen,
        "the route must render exactly the success stamp the test observed in-process \
         (mutant: a route value not sourced from the runner's snapshot)"
    );
    let copy_seconds = optional_sample(&route, "shamir_backup_duration_seconds")
        .expect("a real copy success must export the duration series");
    assert!(
        copy_seconds > 0.0,
        "the duration series must carry the copy's own duration (mutant: `note_success` \
         never publishing the last-success cell — the stamp cell alone is boot-seeded): {route}"
    );
    // Sanity check, NOT a mutant oracle: the boot seed already makes the
    // stamp cell nonzero.
    assert!(
        frozen > 0,
        "the route must render a nonzero last-success stamp: {route}"
    );
    // Sanity check, NOT a mutant oracle: `baseline` is taken before launch, so
    // the boot seed alone satisfies this.
    assert!(
        frozen > baseline.last_success_timestamp_seconds,
        "the last-success stamp must sit past the value observed before the launch: \
         frozen={frozen} before={}",
        baseline.last_success_timestamp_seconds
    );
    assert_eq!(
        metric_type(&route, "shamir_backup_last_success_timestamp_seconds"),
        "gauge",
        "the last-success stamp must render as a gauge: {route}"
    );
    // No failure and no retention failure has happened: a success keeps the
    // streak at zero.
    assert_eq!(
        sample(&route, "shamir_backup_scheduled_failure_streak"),
        0,
        "the failure streak starts at zero after a success: {route}"
    );
    assert_eq!(
        metric_type(&route, "shamir_backup_scheduled_failure_streak"),
        "gauge"
    );
    assert_eq!(
        sample(&route, "shamir_backup_retention_failed_total"),
        baseline.retention_failed_total,
        "the retention-failure counter cannot advance before a retention failure: {route}"
    );
    assert_eq!(
        metric_type(&route, "shamir_backup_retention_failed_total"),
        "counter"
    );

    let report = timeout(BOUND, handle.shutdown())
        .await
        .expect("shutdown must finish within the bound");
    guard.mark("server stopped");
    assert!(
        report.is_clean(),
        "a server whose scheduled worker finished must shut down cleanly: {report:?}"
    );
}

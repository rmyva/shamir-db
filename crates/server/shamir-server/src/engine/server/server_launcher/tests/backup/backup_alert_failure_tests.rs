//! The alert FAILURE inputs, on the real `/metrics` route: a forced failure
//! followed by the repaired run.

use std::fs;

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

const ALERT_FAILURE_TEST: &str =
    "live_alert_failure_streak_tracks_and_resets_on_the_real_metrics_route";

#[test]
fn live_alert_failure_streak_tracks_and_resets_on_the_real_metrics_route() {
    on_big_stack(|| async {
        timeout(OUTER, run_alert_failure())
            .await
            .expect("live alert-failure acceptance timed out");
    });
}

async fn run_alert_failure() {
    let guard = FlakeGuard::with_watchdog(ALERT_FAILURE_TEST, WATCHDOG);
    try_install_global_tracing_ring();
    set_snapshot_name_base_seconds(SNAPSHOT_NAME_BASE);
    let temp = tempfile::tempdir().expect("test root");
    let data_dir = temp.path().join("data");
    let backups = temp.path().join("backups");
    create_private_dir(&data_dir).expect("private data directory");
    create_private_dir(&backups).expect("private backup directory");

    // Installed BEFORE the launch: run 1 is the forced failure. A top-level
    // `manifest.json.tmp` DIRECTORY is materialised into the snapshot, so the
    // manifest write fails.
    let blocker = data_dir.join("manifest.json.tmp");
    fs::create_dir(&blocker).expect("manifest blocker directory");
    fs::write(blocker.join("blocker"), b"block").expect("manifest blocker");

    // Nextest gives this test its own process: the pre-run zero.
    assert_eq!(
        backup_metrics_snapshot().last_success_timestamp_seconds,
        0,
        "a fresh test process must not have published a success yet"
    );

    let (send, mut receive) = mpsc::unbounded_channel::<TerminalOutcome>();
    let handle = ServerLauncher::new(scheduled_config(&data_dir, &backups, "1h", true, 2))
        .with_bootstrap(BootstrapMode::Skip)
        .launch_with_backup_test(move |resolved, registry, owner, shutdown| {
            let scheduler_shutdown = shutdown.child_token();
            spawn_for_test(
                resolved,
                registry,
                owner,
                scheduler_shutdown,
                TICK,
                move |ok| {
                    let _ = send.send(ok);
                },
                || {},
            )
        })
        .await
        .expect("launch a real server with the scheduled worker");
    guard.mark("server launched with a millisecond tick, manifest blocker installed");
    let addr = handle
        .observability
        .as_ref()
        .expect("observability")
        .bound_addr;

    await_outcome(&mut receive, false, "the forced scheduled failure").await;
    guard.mark("forced scheduled failure reported");
    let failed_route = scrape_metrics(addr).await;
    let streak = sample(&failed_route, "shamir_backup_scheduled_failure_streak");
    assert!(
        streak >= 1,
        "the failure streak must be nonzero after the forced scheduled failure \
         (mutant: a failure that never reaches `note_scheduled`, or a gauge always \
         pinned to zero): {failed_route}"
    );
    assert_eq!(
        metric_type(&failed_route, "shamir_backup_scheduled_failure_streak"),
        "gauge"
    );
    // The failed run copied nothing: the duration series (rendered only from
    // the last-success cell) must stay absent, not fabricate a success.
    assert!(
        optional_sample(&failed_route, "shamir_backup_duration_seconds").is_none(),
        "a failed copy must not export a duration series: {failed_route}"
    );

    // The repair, then the streak must fall back to zero.
    fs::remove_dir_all(&blocker).expect("manifest blocker removed");
    guard.mark("manifest blocker removed");
    await_outcome(&mut receive, true, "the scheduled success after the repair").await;
    guard.mark("scheduled success after the repair reported");
    let recovered_route = scrape_metrics(addr).await;
    assert_eq!(
        sample(&recovered_route, "shamir_backup_scheduled_failure_streak"),
        0,
        "the failure streak must return to 0 after a success (mutant: a streak that \
         never resets): {recovered_route}"
    );
    assert_eq!(
        metric_type(&recovered_route, "shamir_backup_scheduled_failure_streak"),
        "gauge"
    );
    let copy_seconds = optional_sample(&recovered_route, "shamir_backup_duration_seconds")
        .expect("a real copy success must export the duration series");
    assert!(
        copy_seconds > 0.0,
        "the duration series must carry the repaired copy's own duration (mutant: \
         `note_success` never publishing the last-success cell — the stamp cell alone is \
         boot-seeded): {recovered_route}"
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

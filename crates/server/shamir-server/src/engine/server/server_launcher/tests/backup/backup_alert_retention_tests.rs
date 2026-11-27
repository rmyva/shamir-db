//! The alert RETENTION-failure input, on the real `/metrics` route: one
//! obstructed retention pass. The obstruction is installed BEFORE the first
//! scheduled admission, so every run this server ever admits is obstructed.

use std::fs;
use std::sync::Arc;

use shamir_private_dir::create_private_dir;
use shamir_test_diag::{try_install_global_tracing_ring, FlakeGuard};
use tokio::sync::mpsc;
use tokio::time::timeout;

use crate::boot::config::diagnostics::backup_online::metrics::backup_metrics_snapshot;
use crate::boot::config::diagnostics::backup_online::schedule::runner::spawn_for_test;
use crate::server::{BootstrapMode, ServerLauncher};
use crate::sync::durability::backup::online::set_snapshot_name_base_seconds;
use crate::sync::durability::backup::SNAPSHOT_LEASE_FILE_NAME;

use super::support::{
    arm_run_observer, metric_type, on_big_stack, sample, scheduled_config, scrape_metrics,
    TerminalOutcome, BOUND, OUTER, SNAPSHOT_NAME_BASE, TICK, WATCHDOG,
};

const ALERT_RETENTION_TEST: &str = "live_alert_retention_failure_reaches_the_real_metrics_route";

#[test]
fn live_alert_retention_failure_reaches_the_real_metrics_route() {
    on_big_stack(|| async {
        timeout(OUTER, run_alert_retention())
            .await
            .expect("live alert-retention acceptance timed out");
    });
}

async fn run_alert_retention() {
    let guard = FlakeGuard::with_watchdog(ALERT_RETENTION_TEST, WATCHDOG);
    try_install_global_tracing_ring();
    set_snapshot_name_base_seconds(SNAPSHOT_NAME_BASE);
    let temp = tempfile::tempdir().expect("test root");
    let data_dir = temp.path().join("data");
    let backups = temp.path().join("backups");
    create_private_dir(&data_dir).expect("private data directory");
    create_private_dir(&backups).expect("private backup directory");

    // Install the OBSTRUCTION before the FIRST admission: a DIRECTORY at the
    // lease-marker path, so `provision_snapshot_lease` fails for every run the
    // server will ever admit. Since no UNOBSTRUCTED run exists in this process,
    // no copy outcome and no retention END delivered here can belong to one —
    // the END/outcome pairing below cannot go stale.
    let marker = backups.join(SNAPSHOT_LEASE_FILE_NAME);
    create_private_dir(&marker).expect("obstruction directory at the lease-marker path");
    let obstruction = fs::symlink_metadata(&marker).expect("obstruction at the lease-marker path");
    assert!(
        obstruction.is_dir(),
        "the lease-marker obstruction must be a plain directory"
    );
    guard.mark("lease marker obstructed before the first admission");

    // Nextest gives this test its own process: the pre-run zero.
    let baseline = backup_metrics_snapshot();
    assert_eq!(
        baseline.last_success_timestamp_seconds, 0,
        "a fresh test process must not have published a success yet"
    );
    // Read BEFORE the launch, hence before the first obstructed run: every
    // increment observed below belongs to an obstructed pass.
    let retention_before = baseline.retention_failed_total;

    let (send, mut receive) = mpsc::unbounded_channel::<TerminalOutcome>();
    let (retention_end_tx, mut retention_end_rx) = mpsc::unbounded_channel::<()>();
    let handle = ServerLauncher::new(scheduled_config(&data_dir, &backups, "1h", true, 2))
        .with_bootstrap(BootstrapMode::Skip)
        .launch_with_backup_test(move |resolved, registry, owner, shutdown| {
            let scheduler_shutdown = shutdown.child_token();
            // Retention-END handshake for run 1; the outcome observer re-arms
            // it for every following run. This test only needs the END, not
            // the published snapshot names.
            arm_run_observer(&registry, None, &retention_end_tx);
            let observer_registry = Arc::clone(&registry);
            let observer_ends = retention_end_tx.clone();
            spawn_for_test(
                resolved,
                registry,
                owner,
                scheduler_shutdown,
                TICK,
                move |ok| {
                    arm_run_observer(&observer_registry, None, &observer_ends);
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

    // An obstructed marker must NOT refuse the boot: boot provisioning is
    // best-effort (`resolve_backup` only warns), and the failure is accounted by
    // the scheduled RETENTION pass, never as a boot or copy failure. Boot must
    // also have left the obstruction untouched — a server that "repaired" the
    // marker would silently turn this test back into an unobstructed one.
    let after_boot = fs::symlink_metadata(&marker).expect("obstruction present after boot");
    assert!(
        after_boot.is_dir(),
        "boot must not repair or replace the lease-marker obstruction"
    );

    // Run 1 was admitted at launch, already obstructed. Its retention END fires
    // AFTER its own copy outcome was published and after this pass's retention
    // counters were noted; run 2 cannot be admitted while run 1 is still in
    // flight, and the runner notes run 1's outcome before run 1's late
    // `finished`. So the FIRST END and the FIRST outcome taken here are the SAME
    // run's, and that run was obstructed from its first step.
    //
    // Mutants caught: (a) "the retention obstruction is accounted as a failed
    // copy run" — an outcome that ORs the retention failure into the copy result
    // (`false` here, then a nonzero failure streak and a `result="failed"` run on
    // the route), and (b) an END or an outcome belonging to an UNOBSTRUCTED run
    // paired with the obstructed pass — impossible to arrange here, because the
    // obstruction predates the first admission.
    let deadline = tokio::time::Instant::now() + BOUND;
    tokio::time::timeout_at(deadline, retention_end_rx.recv())
        .await
        .expect("an obstructed retention pass must end within the bound")
        .expect("the scheduled worker must keep its retention-END channel open");
    let obstructed_outcome = tokio::time::timeout_at(deadline, receive.recv())
        .await
        .expect("the obstructed run's copy outcome must arrive within the bound")
        .expect("the scheduled worker must keep its terminal channel open");
    guard.mark("obstructed retention pass ended");
    // The obstruction fails RETENTION only: the copy of that same run is intact,
    // so its terminal outcome must be `Completed`. The OR mutant below is what
    // this assertion kills; "published only after retention" is NOT separable
    // through this channel (the bounded await would simply absorb the delay),
    // which is why the pairing rests on the obstruction predating every
    // admission rather than on the order inside one run.
    assert!(
        obstructed_outcome,
        "the obstructed run's COPY must still be reported as a success: a retention \
         obstruction is a RETENTION failure, never a failed copy run (mutant: the \
         retention obstruction is accounted as a failed copy run); outcome={obstructed_outcome}"
    );
    let settled_retention = backup_metrics_snapshot().retention_failed_total;
    assert!(
        settled_retention > retention_before,
        "a marker-provisioning failure must be accounted as a retention failure \
         (mutant: `MarkerProvisioning::Failed` publishing no failure): \
         before={retention_before} after={settled_retention}"
    );
    let route = scrape_metrics(addr).await;
    let route_value = sample(&route, "shamir_backup_retention_failed_total");
    assert!(
        route_value > baseline.retention_failed_total,
        "the retention-failure counter must ADVANCE on the real route (mutant: a \
         counter that never advances): before={} route={route_value}",
        baseline.retention_failed_total
    );
    assert!(
        route_value >= settled_retention,
        "the route must render the process counter monotonically: \
         route={route_value} observed={settled_retention}"
    );
    assert_eq!(
        metric_type(&route, "shamir_backup_retention_failed_total"),
        "counter"
    );
    // Route-level counterpart of the outcome assertion above, immune to the
    // runner-task lag of the terminal channel: no scheduled run may be counted
    // `failed`. Index 3 of the snapshot array is the `failed` result, and the
    // counter never resets, so this is the whole process's history.
    assert_eq!(
        sample(
            &route,
            "shamir_backup_scheduled_runs_total{result=\"failed\"}"
        ),
        baseline.scheduled_runs_total[3],
        "an obstructed retention pass must never be noted as a FAILED scheduled run \
         (mutant: the retention obstruction is accounted as a failed copy run): {route}"
    );
    assert_eq!(
        sample(&route, "shamir_backup_scheduled_failure_streak"),
        0,
        "a retention failure must not raise the scheduled failure streak (mutant: the \
         retention obstruction is accounted as a failed copy run): {route}"
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

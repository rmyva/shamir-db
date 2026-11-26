use super::*;

/// The LATE teardown stages must spend the ACTUAL remaining shutdown budget and
/// report what they could not settle.
///
/// The runtime has ONE blocking thread and this test parks it; the 3 s outer
/// budget is too short to rely on earlier teardown. The meta-snapshot wait
/// reserves time for release and reporting, the release wait reserves final
/// report slack, so each stage verdict is appended before outer expiry.
///
/// * the meta-snapshot verdict (step 6): the snapshot did not settle, so the
///   `server_meta` store may still be locked;
/// * a store-release verdict (step 9) naming the positive budget it really had —
///   instead of the 10 s constant — with the pool-starved reason from its
///   submitted probe.
///
/// Mutants this kills:
/// * the meta join awaited UNBOUNDED (`let _ = snap.handle.await`): the
///   teardown never reaches step 9 at all, so neither stage verdict appears;
/// * step 9 passing `STORE_RELEASE_TIMEOUT` unchanged: the barrier parks for
///   the whole 10 s constant, the outer deadline expires inside it, and the
///   report carries only the generic `graceful shutdown exceeded deadline`
///   message and no store-release line.
///
/// The report must also come back while the pool is STILL parked: a teardown
/// that waits for the pool instead of for its budget returns after the pool is
/// released, which the `finished` flag catches.
///
/// The zero-budget release verdict must come from the REAL stop branch, not
/// from any formatter the test calls directly: a shutdown whose absolute
/// deadline is exhausted BEFORE the store-release stage starts must observe
/// `release_budget.is_zero()`, submit NO probe, and append the specific
/// `no shutdown budget remained` verdict.
///
/// The runtime has a PAUSED clock and ONE blocking thread; the parked thread
/// guarantees the release probe can never run even if it were submitted, so
/// the verdict can only be honest if the stage really took the zero-budget
/// branch. The test then advances the paused clock far past the outer
/// deadline, exhausting every stage deadline AND the outer deadline, so the
/// zero-budget branch entry is proven by the rendezvous probe armed before
/// the shutdown and awaited after the advance (the same advance expires the
/// outer `timeout_at`; without the probe the branch could not be
/// distinguished from the teardown being abandoned before reaching it).
///
/// Mutant: bypassing the zero-budget branch (e.g. probing with a zero or
/// negative budget, or reporting the generic expiry instead) makes the
/// specific verdict line disappear — and, with the rendezvous, the probe
/// never fires, so the bounded wait fails outright; calling the test-only
/// formatter from production would still be caught because this test never
/// touches it.
#[test]
fn zero_budget_store_release_reports_unknown_without_claiming_a_probe_ran() {
    run_blocking(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .start_paused(true)
            .max_blocking_threads(1)
            .build()
            .expect("test runtime")
            .block_on(async {
                let guard = FlakeGuard::with_watchdog(
                    "zero_budget_store_release_reports_unknown_without_claiming_a_probe_ran",
                    Duration::from_secs(300),
                );
                try_install_global_tracing_ring();
                let temp = tempfile::tempdir().expect("test root");
                let data_dir = temp.path().join("data");
                create_private_dir(&data_dir).expect("private data directory");
                let handle = ServerLauncher {
                    config: bind_config(
                        data_dir,
                        "127.0.0.1:0".to_owned(),
                        "127.0.0.1:0".to_owned(),
                    ),
                    bootstrap: BootstrapMode::Skip,
                    node_mode: NodeMode::ReadWrite,
                }
                .launch()
                .await
                .expect("launch real server");
                guard.mark("launched on the paused clock");

                // Acknowledge that the shutdown task MINTED its deadlines
                // BEFORE the virtual clock advances: the instance-scoped
                // join-entry probe on the launcher-installed MutationTasks
                // fires only once the shutdown future has been polled into
                // the join stage, so `advance` can never overtake the
                // deadline minting (mutant: this wait removed — the
                // advance may run before the task's first poll, the outer
                // deadline is minted past the jump, and the zero-budget
                // branch becomes timing-dependent instead of forced).
                let tracker = Arc::clone(
                    handle
                        .owned_mutation_tasks
                        .as_ref()
                        .expect("the launcher-installed production mutation tracker"),
                );
                let (mint_tx, mint_rx) = tokio::sync::oneshot::channel::<()>();
                tracker.arm_join_entry_probe(move || {
                    let _ = mint_tx.send(());
                });

                // Rendezvous at the ZERO-BUDGET branch entry: advancing the
                // clock past the stage deadlines ALSO expires the outer
                // `timeout_at` in `drain_server`, so without this handshake
                // the branch is not guaranteed to be reached before the
                // teardown future is abandoned. Armed before the shutdown
                // starts; a branch that never runs fails the bounded wait.
                let (zero_tx, zero_rx) = tokio::sync::oneshot::channel::<()>();
                let _zero_guard =
                    crate::engine::server::server_handle::arm_release_zero_budget_probe(
                        move || {
                            let _ = zero_tx.send(());
                        },
                    );

                // Park the ONLY blocking thread: whatever the release stage
                // decides, its probe can never run — the UNKNOWN verdict must
                // come from the zero-budget branch, not from a starved probe.
                let occupant = BlockingPoolOccupant::park();
                occupant.await_started();
                guard.mark("blocking pool occupied");

                let mut shutdown =
                    tokio::spawn(crate::runtime::drain_server(handle, Duration::from_secs(120)));
                timeout(BOUND, mint_rx)
                    .await
                    .expect("the shutdown must acknowledge its deadlines are minted")
                    .expect("join-entry probe channel");
                guard.mark("shutdown deadlines minted (join stage entered)");
                // Exhaust every stage deadline at once. The stage that runs
                // when the shutdown future next polls observes `now` far past
                // the absolute outer deadline.
                tokio::time::advance(Duration::from_secs(3600)).await;
                guard.mark("virtual clock advanced past every deadline");
                // THE rendezvous: the zero-budget branch RAN (the outer
                // `timeout_at` expired with the same advance, so an abandoned
                // teardown could not have produced the verdict line below).
                timeout(BOUND, zero_rx)
                    .await
                    .expect("the zero-budget branch must be entered (bounded)")
                    .expect("zero-budget probe channel");
                guard.mark("zero-budget branch entered");
                let report = timeout(BOUND, &mut shutdown)
                    .await
                    .expect("the bounded drain must return with every deadline exhausted")
                    .expect("drain task");
                drop(occupant);
                guard.mark("report in hand, pool released");

                let error = report
                    .errors()
                    .iter()
                    .find(|error| error.contains("store release could not be verified"))
                    .expect(
                        "the exhausted-budget shutdown must append the zero-budget release verdict among its errors: {report:?}",
                    );
                assert!(
                    error.contains("no shutdown budget remained"),
                    "the verdict must name the REAL zero-budget reason the stop branch writes: {error}"
                );
                assert!(error.contains("no probe result was obtained"), "{error}");
                assert!(error.contains("no probe was started"), "{error}");
                assert!(error.contains("UNKNOWN"), "{error}");
                assert!(
                    error.contains("immediate relaunch") && error.contains("`Locked`"),
                    "{error}"
                );
                assert!(!error.contains("probe ran"), "{error}");
                assert!(!report.is_clean(), "the report must be unclean: {report:?}");
                assert_eq!(report.exit_code(), 70, "unclean report: {report:?}");
            })
    });
}

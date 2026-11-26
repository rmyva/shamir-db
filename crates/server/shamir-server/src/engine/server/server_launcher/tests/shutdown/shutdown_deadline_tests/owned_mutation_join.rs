use tokio::sync::Barrier;

use super::*;

/// Mutant: remove the mutation_worker_join stage (or move it after the flush) → the flush probe fires while the worker is still parked → this test fails.
///
/// Not covered here: a REAL export worker queued behind a parked blocking pool.
/// `OnlineExport::start` itself reads the settings catalogue through Fjall
/// jobs submitted to the blocking pool, so the pool cannot be parked before
/// `start` returns; that case needs a per-service rendezvous between the
/// preparation and the worker submission (known limit of the export-tails
/// component).
#[test]
fn shutdown_joins_owned_mutation_worker_before_flush() {
    on_big_stack(|| async {
        timeout(BOUND, run_owned_mutation_worker_order())
            .await
            .expect("owned-mutation shutdown test bounded");
    });
}

async fn run_owned_mutation_worker_order() {
    let guard = FlakeGuard::with_watchdog(
        "shutdown_joins_owned_mutation_worker_before_flush",
        Duration::from_secs(300),
    );
    try_install_global_tracing_ring();
    let temp = tempfile::tempdir().expect("test root");
    let data_dir = temp.path().join("data");
    let backups = temp.path().join("backups");
    create_private_dir(&data_dir).expect("private data directory");
    create_private_dir(&backups).expect("private backup directory");

    let handle = timeout(
        BOUND,
        ServerLauncher::new(backup_config(data_dir, backups))
            .with_bootstrap(BootstrapMode::Skip)
            .launch(),
    )
    .await
    .expect("launch bounded")
    .expect("launch server fixture");
    guard.mark("launcher booted");

    // The launcher-installed production tracker: admission goes through the
    // real fence (`MutationTasks::spawn`), the same instance the shutdown
    // joins.
    let tracker = Arc::clone(
        handle
            .owned_mutation_tasks
            .as_ref()
            .expect("the launcher-installed production mutation tracker"),
    );
    let started = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let worker = tracker
        .spawn({
            let started = Arc::clone(&started);
            let release = Arc::clone(&release);
            async move {
                timeout(BOUND, started.wait())
                    .await
                    .expect("worker start barrier bounded");
                timeout(BOUND, release.wait())
                    .await
                    .expect("worker release barrier bounded");
            }
        })
        .expect("the production admission fence must admit before shutdown");
    timeout(BOUND, started.wait())
        .await
        .expect("worker-start handshake bounded");
    guard.mark("owned mutation worker parked");

    let (flush_tx, mut flush_rx) = tokio::sync::oneshot::channel::<()>();
    let completed_before_flush = Arc::new(AtomicBool::new(false));
    let shutdown_completed = Arc::new(AtomicBool::new(false));
    let _flush_probe = {
        let completed_before_flush = Arc::clone(&completed_before_flush);
        let shutdown_completed = Arc::clone(&shutdown_completed);
        arm_repo_flush_probe(move || {
            completed_before_flush
                .store(!shutdown_completed.load(Ordering::SeqCst), Ordering::SeqCst);
            let _ = flush_tx.send(());
        })
    };
    let (completed_tx, completed_rx) = tokio::sync::oneshot::channel::<()>();
    let mut shutdown = tokio::spawn(async move {
        let report = crate::runtime::drain_server(handle, BOUND).await;
        shutdown_completed.store(true, Ordering::SeqCst);
        let _ = completed_tx.send(());
        report
    });

    let shutdown_still_pending = timeout(Duration::from_secs(2), &mut shutdown)
        .await
        .is_err();
    let flush_stayed_pending = timeout(Duration::from_secs(2), &mut flush_rx)
        .await
        .is_err();
    assert!(
        shutdown_still_pending,
        "shutdown must remain pending while the owned mutation worker is parked"
    );
    assert!(
        flush_stayed_pending,
        "the repository flush probe must not fire while the owned mutation worker is parked"
    );

    timeout(BOUND, release.wait())
        .await
        .expect("worker release handshake bounded");
    timeout(BOUND, worker)
        .await
        .expect("owned mutation worker join bounded")
        .expect("owned mutation worker task");
    guard.mark("owned mutation worker released and joined");

    timeout(BOUND, &mut flush_rx)
        .await
        .expect("flush probe must fire after the worker join")
        .expect("repository flush probe channel");
    assert!(
        completed_before_flush.load(Ordering::SeqCst),
        "flush probe must fire before shutdown completion"
    );
    timeout(BOUND, completed_rx)
        .await
        .expect("shutdown completion marker bounded")
        .expect("shutdown completion marker");
    let report = timeout(BOUND, shutdown)
        .await
        .expect("shutdown completion bounded")
        .expect("shutdown task");
    assert!(
        report.is_clean(),
        "normal shutdown must be clean: {report:?}"
    );
}

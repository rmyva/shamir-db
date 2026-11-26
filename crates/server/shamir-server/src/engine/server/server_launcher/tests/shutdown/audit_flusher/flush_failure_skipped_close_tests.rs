use super::super::*;
use std::future::Future;

/// An ORDINARY latched flush failure (the `arm_fail_next_flush` fault, not a
/// panic) combined with a SKIPPED final close (the refusal-audit drain never
/// settles) must reach the shutdown report exactly once, under the flush
/// prefix, and no final close may be submitted.
///
/// Mutant killed: the skip branch's latch carry in `teardown_audit`
/// (`audit_teardown.rs`, the `if let Some(message) = parts.appender.last_failure()`
/// block inside the `!(scheduler_stopped && flusher_stopped && drain_settled)`
/// branch) — reverted, the report carries no `audit flush failed at shutdown`
/// entry at all and this test fails.
#[test]
fn latched_flush_failure_with_skipped_close_is_reported_once() {
    on_big_stack_multi_thread(|| async {
        tokio::time::timeout(CALL_TIMEOUT, run_flush_failure_skipped_close())
            .await
            .expect("latched-flush-failure skipped-close test bounded");
    });
}

fn on_big_stack_multi_thread<F, Fut>(body: F)
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = ()>,
{
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .thread_stack_size(8 * 1024 * 1024)
        .build()
        .expect("multi-thread test runtime");
    runtime.block_on(body());
}

async fn run_flush_failure_skipped_close() {
    let _guard = FlakeGuard::with_watchdog(
        "latched_flush_failure_with_skipped_close_is_reported_once",
        Duration::from_secs(300),
    );
    try_install_global_tracing_ring();
    let temp = tempfile::tempdir().expect("persistent data directory");
    let mut handle = Box::pin(
        ServerLauncher {
            config: boot_config(temp.path().to_path_buf()),
            bootstrap: BootstrapMode::Skip,
            node_mode: NodeMode::ReadWrite,
        }
        .launch(),
    )
    .await
    .expect("production boot");
    let seam = handle
        .activity_test_seam
        .take()
        .expect("boot activity seam");
    let appender: Arc<dyn AuditAppender> = handle.audit_appender.clone();
    let writer = AuditChainWriter::new_with_shared(Arc::clone(&seam.audit_chain), appender);
    drop(seam);

    handle.audit_appender.arm_fail_next_flush();
    writer
        .append(
            "test_flush_failure_skipped_close",
            "tcp",
            "",
            "",
            [0u8; 8],
            "ok",
            Vec::new(),
            UnixNanos::try_now().expect("audit timestamp").as_u64(),
        )
        .expect("buffered audit event");
    let flush_now = handle.audit_appender.flush_now().await;
    assert!(
        flush_now.is_err(),
        "the armed fault must latch the appender"
    );

    // The test seam is already dropped, so the release barrier can see every
    // store go before shutdown starts. Hold one refusal admission slot forever.
    let held = handle.cancel_refusal_audit.hold_admission_slot_for_test();
    let (pending, _pending_rx) = tokio::sync::oneshot::channel();
    handle.refusal_drain_pending = Some(pending);
    drop(handle.activity_test_seam.take());
    let close_observable = Arc::clone(&handle.audit_appender);

    let report = tokio::time::timeout(CALL_TIMEOUT, Box::pin(handle.shutdown()))
        .await
        .expect("shutdown deadline");
    drop(held);

    let flush_errors: Vec<_> = report
        .errors()
        .iter()
        .filter(|error| error.contains("audit flush failed at shutdown"))
        .collect();
    assert_eq!(
        flush_errors.len(),
        1,
        "flush failure exactly once: {report:?}"
    );
    assert!(
        flush_errors[0].contains("injected flush failure"),
        "flush failure carries injected cause: {report:?}"
    );
    assert!(
        !report
            .errors()
            .iter()
            .any(|error| error.contains("audit flusher task did not join cleanly")),
        "ordinary flush failure is not a flusher join failure: {report:?}"
    );
    assert!(
        report
            .errors()
            .iter()
            .any(|error| error.contains("audit final close not attempted")),
        "the unsettled refusal drain skips final close: {report:?}"
    );
    assert_eq!(close_observable.final_close_attempts(), 0);
    assert_eq!(report.exit_code(), 70);
}

use super::super::*;

/// Held blocking flush job keeps teardown from submitting the final close.
#[test]
fn final_close_is_skipped_when_flusher_job_is_still_in_flight() {
    on_big_stack(|| async {
        tokio::time::timeout(CALL_TIMEOUT, async {
            let temp = tempfile::tempdir().expect("audit directory");
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
            let appender = Arc::clone(&handle.audit_appender);
            let seam = handle
                .activity_test_seam
                .take()
                .expect("boot activity seam");
            let writer = AuditChainWriter::new_with_shared(
                Arc::clone(&seam.audit_chain),
                handle.audit_appender.clone(),
            );
            drop(seam);
            writer
                .append(
                    "test_held_flusher",
                    "tcp",
                    "",
                    "",
                    [0u8; 8],
                    "ok",
                    Vec::new(),
                    UnixNanos::try_now().expect("timestamp").as_u64(),
                )
                .expect("buffered event");
            let gate = Arc::new((Mutex::new(GateState::default()), Condvar::new()));
            let _release_on_drop = GateRelease(&gate);
            let started = Arc::new(Notify::new());
            handle.audit_appender.arm_gate_acquired_hook({
                let gate = Arc::clone(&gate);
                let started = Arc::clone(&started);
                Arc::new(move || {
                    started.notify_one();
                    wait_until(&gate, "held flusher release", |s| s.released);
                })
            });
            tokio::time::timeout(CALL_TIMEOUT, started.notified())
                .await
                .expect("blocking flusher job must begin");
            let report = tokio::time::timeout(CALL_TIMEOUT, Box::pin(handle.shutdown()))
                .await
                .expect("shutdown bounded");
            release_gate(&gate);
            assert!(
                report
                    .errors()
                    .iter()
                    .any(|e| e.contains("audit flusher") && e.contains("audit teardown exceeded")),
                "flusher timeout: {report:?}"
            );
            assert!(
                report
                    .errors()
                    .iter()
                    .any(|e| e.contains("audit final close not attempted")),
                "skipped close: {report:?}"
            );
            assert_eq!(appender.final_close_attempts(), 0);
        })
        .await
        .expect("held-flusher test bounded");
    });
}

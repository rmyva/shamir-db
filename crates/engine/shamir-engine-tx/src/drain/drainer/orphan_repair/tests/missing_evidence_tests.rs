use std::sync::atomic::Ordering;
use std::sync::Arc;

use shamir_storage::storage_in_memory::InMemoryRepo;
use shamir_wal::WalEntryV2;

use crate::repo::{BoxRepo, RepoInstance};
use crate::tx::drain::drainer::orphan_repair::settle_missing_evidence;
use crate::tx::drainer::Drainer;
use crate::tx::test_seams::JournalPublishHook;
use crate::tx::tests::drain::orphans::post_wal_orphan_ordering_tests::fixture;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_evidence_gap_init_failure_is_counted_and_retry_clears_error() {
    let repo = RepoInstance::new(
        "orphan_missing_evidence_gap_init".into(),
        BoxRepo::InMemory(Arc::new(InMemoryRepo::new())),
        Vec::new(),
    );
    let gate = repo.tx_gate().await.unwrap();
    let mut guard = gate.assign_next_version_guarded();
    guard.wal_accepted();
    drop(guard);

    let hook = Arc::new(JournalPublishHook::default());
    hook.init_failures.store(2, Ordering::Release);
    repo.core
        .journal_publish_hook
        .store(Some(Arc::clone(&hook)));
    let drainer = Drainer::new();

    for expected in 1..=2 {
        let error = drainer.drain_step(&repo.core).await.unwrap_err();
        assert!(error
            .to_string()
            .contains("injected changefeed init failure"));
        let mut snapshot = gate.diagnostics();
        drainer.enrich_diagnostics(&mut snapshot);
        assert_eq!(snapshot.orphan_replay_attempts_total, expected);
        assert_eq!(snapshot.orphan_replay_failures_total, expected);
        assert_eq!(snapshot.orphan_replay_consecutive_failures, expected);
        assert!(snapshot
            .orphan_replay_last_error
            .unwrap()
            .contains("injected changefeed init failure"));
        assert_eq!(snapshot.visible_floor, 0);
        assert_eq!(snapshot.durable_watermark, 0);
        assert!(gate.is_orphaned_post_wal(1));
    }

    assert_eq!(hook.init_failures.load(Ordering::Acquire), 0);
    assert_eq!(drainer.drain_step(&repo.core).await.unwrap(), 0);
    let mut snapshot = gate.diagnostics();
    drainer.enrich_diagnostics(&mut snapshot);
    assert_eq!(snapshot.orphan_replay_attempts_total, 3);
    assert_eq!(snapshot.orphan_replay_failures_total, 2);
    assert_eq!(snapshot.orphan_replay_consecutive_failures, 0);
    assert_eq!(snapshot.orphan_replay_last_error, None);
    assert_eq!(snapshot.visible_floor, 1);
    assert_eq!(snapshot.durable_watermark, 1);
    assert!(!gate.is_orphaned_post_wal(1));
    assert_eq!(drainer.orphan_repair_metrics.snapshot(), (0, 1));
    let handle = repo.changefeed().await.unwrap();
    assert_eq!(handle.feed.first_gap_version(), 1);
    handle.feed.shutdown_and_wait().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn absent_gate_and_window_evidence_recovers_then_certifies_gap() {
    let (repo, _hook) = fixture("orphan_missing_evidence").await;
    let gate = repo.tx_gate().await.unwrap();
    let mut guard = gate.assign_next_version_guarded();
    let version = guard.version();
    guard.wal_accepted();
    drop(guard);
    assert!(gate.is_orphaned_post_wal(version));
    assert!(gate.orphan_entry(version).is_none());

    assert_eq!(repo.drainer().drain_all(&repo.core).await.unwrap(), 0);
    assert!(!gate.is_orphaned_post_wal(version));
    assert_eq!(repo.drainer().recover_calls(), 1);

    let completion = gate.completion();
    let watermark = completion.watermark();
    let durable_watermark = gate.durable_watermark();
    assert_eq!(watermark, version);
    assert_eq!(durable_watermark, version);
    let handle = repo.changefeed().await.unwrap();
    handle.feed.record_clean_horizon(watermark).await.unwrap();
    assert_eq!(
        handle.feed.certified_gap_interval(),
        Some((version, version))
    );
    handle.feed.shutdown_and_wait().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_evidence_does_not_settle_version_present_in_window() {
    let (repo, _hook) = fixture("orphan_missing_evidence_window").await;
    let gate = repo.tx_gate().await.unwrap();
    let mut guard = gate.assign_next_version_guarded();
    let version = guard.version();
    guard.wal_accepted();
    drop(guard);

    repo.drainer().offer(Arc::new(
        WalEntryV2::new(version, 0, Vec::new()).with_commit_version(version),
    ));
    let drainer = repo.drainer();
    settle_missing_evidence(&drainer, &repo.core, &gate)
        .await
        .unwrap();
    assert!(gate.is_orphaned_post_wal(version));
    assert_eq!(gate.durable_watermark(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_evidence_does_not_settle_non_contiguous_orphan() {
    let (repo, _hook) = fixture("orphan_missing_evidence_non_contiguous").await;
    let gate = repo.tx_gate().await.unwrap();
    let first = gate.assign_next_version_guarded();
    let first_version = first.version();
    let mut second = gate.assign_next_version_guarded();
    let second_version = second.version();
    second.wal_accepted();
    drop(second);

    let drainer = repo.drainer();
    settle_missing_evidence(&drainer, &repo.core, &gate)
        .await
        .unwrap();
    assert!(gate.is_orphaned_post_wal(second_version));
    assert_eq!(gate.durable_watermark(), 0);

    drop(first);
    assert_eq!(repo.drainer().drain_all(&repo.core).await.unwrap(), 0);
    assert!(!gate.is_orphaned_post_wal(second_version));
    assert_eq!(gate.durable_watermark(), second_version);
    assert_eq!(
        repo.changefeed().await.unwrap().feed.first_gap_version(),
        second_version
    );
    assert!(first_version < second_version);
}

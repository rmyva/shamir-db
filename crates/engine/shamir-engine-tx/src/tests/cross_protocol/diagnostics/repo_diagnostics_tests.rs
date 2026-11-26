use std::sync::atomic::Ordering;
use std::sync::Arc;

#[cfg(debug_assertions)]
use bytes::Bytes;
use shamir_storage::storage_in_memory::InMemoryRepo;
#[cfg(debug_assertions)]
use shamir_tx::IndexWriteOp;
use shamir_tx::{GateBlockerClass, GateDiagnostics, IsolationLevel};
use shamir_types::types::value::InnerValue;
use shamir_wal::WalEntryV2;

use crate::tx::drainer::Drainer;
#[cfg(debug_assertions)]
use crate::tx::drainer::FAIL_INDEX_BATCH_TABLE_ID;
use crate::tx::repo::{BoxRepo, RepoInstance};
#[cfg(debug_assertions)]
use crate::tx::table::table_manager::table_token_for;
use crate::tx::table::TableConfig;
use crate::tx::test_seams::JournalPublishHook;
use crate::tx::tests::drain::orphans::post_wal_orphan_ordering_tests::fixture;
#[cfg(debug_assertions)]
use crate::tx::tests::drain::orphans::post_wal_orphan_ordering_tests::panic_post_wal;

// Red before instrumentation: the engine snapshot has no replay progress.
// The real post-WAL driver is parked, then unwound into an orphan; the
// fixture suppresses the background drainer before it can race assertions.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parked_tail_missing_table_failures_and_successful_retry_are_observed() {
    let (repo, hook) = fixture("diagnostics_replay").await;
    let table = repo.get_table("a").await.unwrap();
    let (mut tx, snapshot) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let rid = table
        .insert_tx(&InnerValue::Str("replayed".into()), Some(&mut tx))
        .await
        .unwrap();
    hook.panic_after_resume.store(true, Ordering::Release);
    hook.armed.store(true, Ordering::Release);
    let owned = repo.clone();
    let task = tokio::spawn(async move { owned.commit_tx(tx).await });
    hook.reached.notified().await;
    let active = repo.diagnostics();
    let blocker = active.oldest_blocker.unwrap();
    assert_eq!(blocker.class, GateBlockerClass::ActiveTail);
    assert_eq!(repo.drainer().drain_step(&repo.core).await.unwrap(), 0);
    assert_eq!(repo.diagnostics().orphan_replay_attempts_total, 0);
    hook.panic_resume.notify_one();
    assert!(task.await.unwrap_err().is_panic());
    drop(snapshot);
    assert_eq!(
        repo.diagnostics().oldest_blocker.unwrap().class,
        GateBlockerClass::OrphanAwaitingReplay,
    );
    assert!(repo.remove_table_for_test("a"));
    for expected in 1..=2 {
        assert_eq!(repo.drainer().drain_step(&repo.core).await.unwrap(), 0);
        let failed = repo.diagnostics();
        assert_eq!(failed.orphan_replay_attempts_total, expected);
        assert_eq!(failed.orphan_replay_failures_total, expected);
        assert_eq!(failed.orphan_replay_consecutive_failures, expected);
        assert!(failed
            .orphan_replay_last_error
            .unwrap()
            .contains("unavailable"));
        assert!(failed.visible_floor < blocker.version);
    }
    repo.add_table_for_test(TableConfig::new("a"));
    let restored = repo.get_table("a").await.unwrap();
    assert_eq!(repo.drainer().drain_step(&repo.core).await.unwrap(), 1);
    let success = repo.diagnostics();
    assert_eq!(success.orphan_replay_attempts_total, 3);
    assert_eq!(success.orphan_replay_failures_total, 2);
    assert_eq!(success.orphan_replay_consecutive_failures, 0);
    assert_eq!(success.orphan_replay_last_error, None);
    assert_eq!(success.oldest_blocker, None);
    assert_eq!(success.visible_floor, blocker.version);
    assert_eq!(
        restored.get(rid).await.unwrap(),
        InnerValue::Str("replayed".into())
    );
    assert_eq!(repo.drainer().drain_step(&repo.core).await.unwrap(), 0);
    assert_eq!(repo.diagnostics().orphan_replay_attempts_total, 3);
}

// The existing Phase B fault runs against an actual orphan posting. A
// retry must reset only the streak, preserving cumulative failure history.
#[cfg(debug_assertions)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn orphan_phase_b_failure_is_counted_once_and_retry_resets_streak() {
    let (repo, hook) = fixture("diagnostics_phase_b").await;
    let table = repo.get_table("a").await.unwrap();
    let (mut tx, snapshot) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    table
        .insert_tx(&InnerValue::Str("data".into()), Some(&mut tx))
        .await
        .unwrap();
    let token = table_token_for("a");
    let key = Bytes::from_static(b"diagnostics-posting");
    tx.index_write_set.push((
        token,
        IndexWriteOp::SetPosting {
            key: key.clone(),
            value: Bytes::from_static(b"value"),
            provenance: Default::default(),
        },
    ));
    let version = panic_post_wal(&repo, &hook, tx).await;
    drop(snapshot);
    FAIL_INDEX_BATCH_TABLE_ID.store(token, Ordering::SeqCst);
    assert_eq!(repo.drainer().drain_step(&repo.core).await.unwrap(), 0);
    FAIL_INDEX_BATCH_TABLE_ID.store(0, Ordering::SeqCst);
    let failed = repo.diagnostics();
    assert_eq!(failed.orphan_replay_attempts_total, 1);
    assert_eq!(failed.orphan_replay_failures_total, 1);
    assert_eq!(failed.orphan_replay_consecutive_failures, 1);
    assert!(failed
        .orphan_replay_last_error
        .unwrap()
        .contains("index-batch failure"));
    assert!(failed.visible_floor < version);
    assert_eq!(repo.drainer().drain_step(&repo.core).await.unwrap(), 1);
    let success = repo.diagnostics();
    assert_eq!(success.orphan_replay_attempts_total, 2);
    assert_eq!(success.orphan_replay_failures_total, 1);
    assert_eq!(success.orphan_replay_consecutive_failures, 0);
    assert_eq!(success.orphan_replay_last_error, None);
    assert_eq!(success.visible_floor, version);
    assert_eq!(
        table.info_store().get(key.into()).await.unwrap(),
        Bytes::from_static(b"value")
    );
}

// Coverage of the existing retention contract, not a new production fix.
// Removing the byte cap or truncating inside a UTF-8 scalar breaks this test.
#[test]
fn retained_orphan_error_is_utf8_bounded_and_first_failure_wins() {
    let drainer = Drainer::new();
    let mut snapshot = GateDiagnostics::default();
    drainer.enrich_diagnostics(&mut snapshot);
    assert_eq!(snapshot.orphan_replay_last_error, None);

    // Byte 1024 falls inside the first four-byte scalar after the ASCII prefix.
    let prefix = "a".repeat(1023);
    let error = format!("{prefix}{}", "🦀".repeat(512));
    let mut failed = false;
    drainer.record_orphan_failure_for_test(&mut failed, &error);
    drainer.enrich_diagnostics(&mut snapshot);
    assert!(failed);
    assert_eq!(
        snapshot.orphan_replay_last_error.as_deref(),
        Some(prefix.as_str())
    );
    assert!(snapshot.orphan_replay_last_error.as_ref().unwrap().len() <= 1024);
    assert_eq!(snapshot.orphan_replay_failures_total, 1);
    assert_eq!(snapshot.orphan_replay_consecutive_failures, 1);

    drainer.record_orphan_failure_for_test(&mut failed, "ignored second failure");
    let mut duplicate = GateDiagnostics::default();
    drainer.enrich_diagnostics(&mut duplicate);
    assert_eq!(duplicate, snapshot);

    // A fresh attempt replaces the retained error and preserves whole scalars.
    failed = false;
    drainer.record_orphan_failure_for_test(&mut failed, &"🦀".repeat(512));
    drainer.enrich_diagnostics(&mut snapshot);
    assert_eq!(snapshot.orphan_replay_last_error, Some("🦀".repeat(256)));
    assert_eq!(snapshot.orphan_replay_failures_total, 2);
    assert_eq!(snapshot.orphan_replay_consecutive_failures, 2);
    assert_eq!(snapshot.orphan_replay_attempts_total, 0);
}

// A synthetic empty written orphan isolates the real gap-init error boundary.
// No background drainer is started; each explicit pass is the only replay owner.
#[tokio::test]
async fn orphan_gap_init_failure_is_observed_and_retry_clears_error() {
    let repo = RepoInstance::new(
        "diagnostics_gap_init".into(),
        BoxRepo::InMemory(Arc::new(InMemoryRepo::new())),
        Vec::new(),
    );
    let gate = repo.tx_gate().await.unwrap();
    let mut guard = gate.assign_next_version_guarded();
    guard.wal_accepted();
    drop(guard);
    gate.register_written_orphan(
        1,
        Arc::new(WalEntryV2 {
            txn_id: 1,
            repo_id_interned: 0,
            started_at_ns: 0,
            commit_version: 1,
            ops: Vec::new(),
            interner_delta: Vec::new(),
            event: None,
        }),
    );
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
    assert_eq!(drainer.drain_step(&repo.core).await.unwrap(), 1);
    let mut snapshot = gate.diagnostics();
    drainer.enrich_diagnostics(&mut snapshot);
    assert_eq!(snapshot.orphan_replay_attempts_total, 3);
    assert_eq!(snapshot.orphan_replay_failures_total, 2);
    assert_eq!(snapshot.orphan_replay_consecutive_failures, 0);
    assert_eq!(snapshot.orphan_replay_last_error, None);
    assert_eq!(snapshot.visible_floor, 1);
    assert_eq!(snapshot.durable_watermark, 1);
    assert_eq!(snapshot.oldest_blocker, None);
    assert!(!gate.is_orphaned_post_wal(1));
    let handle = repo.changefeed().await.unwrap();
    assert_eq!(handle.feed.first_gap_version(), 1);
    handle.feed.shutdown_and_wait().await.unwrap();
}

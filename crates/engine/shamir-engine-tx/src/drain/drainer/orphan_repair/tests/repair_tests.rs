use std::sync::atomic::Ordering;

use bytes::Bytes;
use shamir_tx::IsolationLevel;
use shamir_types::types::value::InnerValue;

use crate::table::TableConfig;
use crate::tx::drain::drainer::orphan_repair::repair_event;
use crate::tx::tests::drain::orphans::post_wal_orphan_ordering_tests::{fixture, panic_post_wal};
use crate::tx::tests::recovery::snapshot_gc::snapshot_visibility_park_tests::spawn_settled_commit;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn orphan_is_repaired_and_bounded_pull_has_no_gap() {
    let (repo, hook) = fixture("orphan_repair_event").await;
    let table = repo.get_table("a").await.unwrap();
    let (mut tx, snapshot) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    table
        .insert_tx(&InnerValue::Str("orphan".into()), Some(&mut tx))
        .await
        .unwrap();
    let version = panic_post_wal(&repo, &hook, tx).await;
    drop(snapshot);
    let entry = repo.drainer().window_entry(version).unwrap();
    assert_eq!(repo.drainer().drain_all(&repo.core).await.unwrap(), 1);
    let (events, gap) = repo
        .read_changelog_for_replication(version, 10)
        .await
        .unwrap();
    assert_eq!(gap, None);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event.commit_version, version);
    assert_eq!(
        events[0].event,
        shamir_tx::event_from_wal_entry(&entry).unwrap()
    );
    assert!(repo.drainer().window_entry(version).is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn existing_mismatch_is_error_and_orphan_is_preserved() {
    let (repo, hook) = fixture("orphan_repair_mismatch").await;
    let table = repo.get_table("a").await.unwrap();
    let (mut tx, snapshot) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    table
        .insert_tx(&InnerValue::Str("source".into()), Some(&mut tx))
        .await
        .unwrap();
    let version = panic_post_wal(&repo, &hook, tx).await;
    drop(snapshot);
    let entry = repo.drainer().window_entry(version).unwrap();
    let handle = repo.changefeed().await.unwrap();
    let mut wrong = shamir_tx::event_from_wal_entry(&entry).unwrap();
    wrong.timestamp_ns = wrong.timestamp_ns.saturating_add(1);
    handle
        .store
        .put(
            shamir_tx::changefeed::version_key(version),
            Bytes::from(rmp_serde::to_vec(&wrong).unwrap()),
        )
        .await
        .unwrap();
    assert_eq!(repo.drainer().drain_all(&repo.core).await.unwrap(), 0);
    assert_eq!(
        repo.drainer()
            .orphan_replay_failures_total
            .load(Ordering::Relaxed),
        1
    );
    assert!(repo
        .drainer()
        .orphan_replay_last_error
        .load_full()
        .as_ref()
        .is_some_and(|error| error.contains("differs from WAL")));
    let gate = repo.tx_gate().await.unwrap();
    assert!(gate.is_orphaned_post_wal(version));
    assert!(gate.durable_watermark() < version);
    assert_eq!(repo.changefeed().await.unwrap().feed.first_gap_version(), 0);
    let failed_row = handle
        .store
        .range_from(shamir_tx::changefeed::version_key(version), 1)
        .await
        .unwrap();
    assert_eq!(failed_row.len(), 1);
    assert_eq!(
        rmp_serde::from_slice::<shamir_tx::ChangelogEvent>(&failed_row[0]).unwrap(),
        wrong
    );
    let gate = repo.tx_gate().await.unwrap();
    assert!(gate.is_orphaned_post_wal(version));
    assert_eq!(gate.durable_watermark() + 1, version);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn double_repair_leaves_one_event() {
    let (repo, hook) = fixture("orphan_repair_twice").await;
    let table = repo.get_table("a").await.unwrap();
    let (mut tx, snapshot) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    table
        .insert_tx(&InnerValue::Str("same".into()), Some(&mut tx))
        .await
        .unwrap();
    let version = panic_post_wal(&repo, &hook, tx).await;
    drop(snapshot);
    let entry = repo.drainer().window_entry(version).unwrap();
    let handle = repo.changefeed().await.unwrap();
    repair_event(&repo.core, &entry).await.unwrap();
    repair_event(&repo.core, &entry).await.unwrap();
    let rows = handle
        .store
        .range_from(shamir_tx::changefeed::version_key(version), 1)
        .await
        .unwrap();
    let events: Vec<_> = rows
        .iter()
        .filter_map(|bytes| rmp_serde::from_slice::<shamir_tx::ChangelogEvent>(bytes).ok())
        .filter(|event| event.commit_version == version)
        .collect();
    assert_eq!(events.len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_table_orphan_remains_retryable() {
    let (repo, hook) = fixture("orphan_repair_table_missing").await;
    let table = repo.get_table("a").await.unwrap();
    let (mut tx, snapshot) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    table
        .insert_tx(&InnerValue::Str("missing".into()), Some(&mut tx))
        .await
        .unwrap();
    let version = panic_post_wal(&repo, &hook, tx).await;
    drop(snapshot);
    assert!(repo.remove_table_for_test("a"));
    assert_eq!(repo.drainer().drain_all(&repo.core).await.unwrap(), 0);
    assert!(repo.drainer().window_entry(version).is_some());
    assert!(repo.tx_gate().await.unwrap().is_orphaned_post_wal(version));
    repo.add_table_for_test(TableConfig::new("a"));
    assert_eq!(repo.drainer().drain_all(&repo.core).await.unwrap(), 1);
    assert!(repo
        .read_changelog_for_replication(version, 1)
        .await
        .unwrap()
        .1
        .is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn closed_writer_fallback_records_gap_after_gap_admission() {
    let (repo, hook) = fixture("orphan_repair_closed_writer").await;
    let table = repo.get_table("a").await.unwrap();
    let (mut tx, snapshot) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    table
        .insert_tx(&InnerValue::Str("closed".into()), Some(&mut tx))
        .await
        .unwrap();
    let version = panic_post_wal(&repo, &hook, tx).await;
    drop(snapshot);
    let handle = repo.changefeed().await.unwrap();
    handle.feed.shutdown_and_wait().await.unwrap();
    assert_eq!(repo.drainer().drain_all(&repo.core).await.unwrap(), 1);
    let (events, gap) = repo
        .read_changelog_for_replication(version, 1)
        .await
        .unwrap();
    assert!(events.is_empty());
    assert_eq!(gap, Some(version));
    assert!(!repo.tx_gate().await.unwrap().is_orphaned_post_wal(version));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn later_queued_event_cannot_be_served_before_repaired_orphan() {
    let (repo, hook) = fixture("orphan_repair_order").await;
    let table = repo.get_table("a").await.unwrap();
    let (mut orphan_tx, orphan_snapshot) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    table
        .insert_tx(&InnerValue::Str("orphan".into()), Some(&mut orphan_tx))
        .await
        .unwrap();
    let orphan = panic_post_wal(&repo, &hook, orphan_tx).await;
    drop(orphan_snapshot);
    let (mut later_tx, later_snapshot) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    table
        .insert_tx(&InnerValue::Str("later".into()), Some(&mut later_tx))
        .await
        .unwrap();
    let (later, task) = spawn_settled_commit(&repo, later_tx).await;
    drop(later_snapshot);
    assert!(later > orphan);
    let (events, gap_before) = repo
        .read_changelog_for_replication(orphan, 10)
        .await
        .unwrap();
    assert!(events.is_empty());
    assert_eq!(gap_before, None);
    assert_eq!(repo.drainer().drain_all(&repo.core).await.unwrap(), 2);
    task.await.unwrap().unwrap();
    let (events, gap) = repo
        .read_changelog_for_replication(orphan, 10)
        .await
        .unwrap();
    assert_eq!(gap, None);
    assert_eq!(
        events
            .iter()
            .map(|item| item.event.commit_version)
            .collect::<Vec<_>>(),
        vec![orphan, later]
    );
}

use std::sync::Arc;

use bytes::Bytes;
use shamir_wal::{WalEntryV2, WalEventHeader, WalOpV2};

use super::super::repair::repair_event;
use super::super::repair::{GapReason, Outcome};
use crate::repo::{BoxRepoFactory, RepoInstance};
use crate::table::TableConfig;
use crate::tests::drain::orphans::post_wal_orphan_ordering_tests::fixture;

fn entry_without_event(version: u64) -> WalEntryV2 {
    WalEntryV2::new(version, 0, Vec::new()).with_commit_version(version)
}

fn eventless_header() -> WalEventHeader {
    WalEventHeader {
        repo: "orphan".into(),
        actor: Default::default(),
        tx_id: 0,
        timestamp_ns: 1,
        table_names: Vec::new(),
        extra_changes: Vec::new(),
    }
}

#[tokio::test]
async fn legacy_entry_drains_as_certified_gap() {
    let (repo, _) = fixture("orphan_legacy_gap").await;
    let gate = repo.tx_gate().await.unwrap();
    let mut guard = gate.assign_next_version_guarded();
    let version = guard.version();
    guard.wal_accepted();
    drop(guard);
    let entry = Arc::new(WalEntryV2::new(version, 0, Vec::new()).with_commit_version(version));
    repo.drainer().offer(entry);
    assert_eq!(gate.durable_watermark(), 0);
    assert_eq!(repo.drainer().drain_all(&repo.core).await.unwrap(), 1);
    assert_eq!(gate.durable_watermark(), version);
    let handle = repo.changefeed().await.unwrap();
    handle
        .feed
        .record_clean_horizon(gate.completion().watermark())
        .await
        .unwrap();
    assert_eq!(handle.feed.clean_horizon(), version);
    assert_eq!(handle.feed.first_gap_version(), version);
    assert_eq!(
        handle.feed.certified_gap_interval(),
        Some((version, version))
    );
    let read = handle.feed.read_from(&handle.store, version, 1).await;
    assert_eq!(read.events.len(), 0);
    handle.feed.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn eventless_entry_falls_back_without_consulting_writer() {
    let repo = RepoInstance::from_factory(
        "orphan_eventless".into(),
        BoxRepoFactory::in_memory(),
        vec![TableConfig::new("a")],
    )
    .await
    .unwrap();
    let mut entry = entry_without_event(1);
    entry.event = Some(eventless_header());
    assert_eq!(
        repair_event(&repo.core, &entry).await.unwrap(),
        Outcome::Gap(GapReason::UnknownWAL),
    );
}

#[tokio::test]
async fn legacy_entry_falls_back_without_consulting_writer() {
    let repo = RepoInstance::from_factory(
        "orphan_legacy".into(),
        BoxRepoFactory::in_memory(),
        vec![TableConfig::new("a")],
    )
    .await
    .unwrap();
    assert_eq!(
        repair_event(&repo.core, &entry_without_event(1))
            .await
            .unwrap(),
        Outcome::Gap(GapReason::UnknownWAL),
    );
}

#[tokio::test]
async fn corrupt_existing_event_is_error_and_preserved() {
    let repo = RepoInstance::from_factory(
        "orphan_corrupt".into(),
        BoxRepoFactory::in_memory(),
        vec![TableConfig::new("a")],
    )
    .await
    .unwrap();
    let handle = repo.changefeed().await.unwrap();
    let entry = WalEntryV2::new(
        1,
        0,
        vec![WalOpV2::Put {
            table_id_interned: 1,
            rid: Default::default(),
            body: Bytes::from_static(b"v"),
        }],
    )
    .with_commit_version(1)
    .with_event_header(eventless_header());
    let corrupt = Bytes::from_static(b"not-an-event");
    handle
        .store
        .put(shamir_tx::changefeed::version_key(1), corrupt.clone())
        .await
        .unwrap();
    let error = repair_event(&repo.core, &entry).await.unwrap_err();
    assert!(error.to_string().contains("decode"));
    assert_eq!(
        handle
            .store
            .range_from(shamir_tx::changefeed::version_key(1), 1)
            .await
            .unwrap(),
        vec![corrupt]
    );
}

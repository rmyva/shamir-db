//! Backend-independent cold-start behaviour of ephemeral (in-memory)
//! repositories. Requires no `fjall` feature.

use crate::repo::RepoFactory;
use crate::repo::RepoInstance;
use crate::table::TableConfig;
use bytes::Bytes;
use shamir_types::access::Actor;
use shamir_wal::{WalEntryV2, WalEventHeader};

fn derived(version: u64) -> WalEntryV2 {
    WalEntryV2::new(version, 0, Vec::new())
        .with_commit_version(version)
        .with_event_header(WalEventHeader {
            repo: "ephemeral_cold_reconcile".into(),
            actor: Actor::System,
            tx_id: 17,
            timestamp_ns: 23,
            table_names: vec![(1, "items".into())],
            extra_changes: vec![shamir_wal::WalExtraChange {
                table_id_interned: 1,
                change_index: 0,
                key: Bytes::from_static(b"record-key-00001"),
                value: Some(Bytes::from_static(b"payload")),
            }],
        })
}

async fn stop(repo: RepoInstance) {
    repo.stop_journal_writer_for_test()
        .await
        .expect("stop journal writer");
    drop(repo);
}

#[tokio::test(flavor = "current_thread")]
async fn ephemeral_repositories_keep_legacy_cold_start_behavior() {
    let raw = crate::repo::BoxRepoFactory::in_memory()
        .create()
        .await
        .unwrap();
    let repo = RepoInstance::new(
        "ephemeral_cold_reconcile".into(),
        raw,
        vec![TableConfig::new("items")],
    );
    let entry = derived(1);
    let manager = repo.repo_wal().await.unwrap();
    manager
        .begin_grouped(&entry, shamir_wal::WalDurability::Buffered)
        .await
        .unwrap();
    let wal_group = repo.wal_group().await.unwrap();
    assert!(wal_group.is_mem_sink());
    let permit = repo.write_domain().admit().await.unwrap();
    repo.certify_cold_start(&[entry], &permit).await.unwrap();
    let feed = repo.changefeed().await.unwrap().feed;
    assert_eq!(feed.clean_horizon(), 1);
    assert_eq!(feed.first_gap_version(), 1);
    assert_eq!(feed.certified_gap_interval(), Some((1, 1)));
    drop(permit);
    stop(repo).await;
}

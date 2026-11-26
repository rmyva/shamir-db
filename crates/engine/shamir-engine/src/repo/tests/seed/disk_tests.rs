use bytes::Bytes;
use shamir_storage::types::Repo;
use shamir_types::core::interner::InternerKey;
use shamir_types::types::value::InnerValue;

use crate::meta::repo_identity::RepoIdentity;
use crate::repo::{
    BoxRepoFactory, RepoFactory, RepoInstance, SeedImageWriter, SeedInstallState, SeedReceipt,
    SeedTable,
};

fn receipt() -> SeedReceipt {
    SeedReceipt {
        artifact_digest: "a".repeat(64),
        source_db: "app".into(),
        source_repo: "main".into(),
        source_identity: RepoIdentity {
            lineage_id: [7; 16],
            history_epoch: 3,
        },
        schema_digest: "b".repeat(64),
        source_version: 73,
        local_baseline: 1,
    }
}

fn inventory() -> Vec<SeedTable> {
    vec![
        SeedTable {
            name: "items".into(),
            live_records: 1,
        },
        SeedTable {
            name: "empty".into(),
            live_records: 0,
        },
    ]
}

fn payload() -> InnerValue {
    let mut nested = shamir_collections::TMap::default();
    nested.insert(InternerKey::new(900), InnerValue::Bin(vec![0, 255]));
    let mut root = shamir_collections::TMap::default();
    root.insert(
        InternerKey::new(700),
        InnerValue::List(vec![InnerValue::Map(nested)]),
    );
    InnerValue::Map(root)
}

fn payload_bytes() -> Bytes {
    payload().to_bytes().unwrap()
}

async fn populated(repo: RepoInstance) -> SeedImageWriter {
    let mut writer = SeedImageWriter::begin(repo, receipt(), inventory())
        .await
        .unwrap();
    writer.binding(700, "outer").await.unwrap();
    writer.binding(900, "inner").await.unwrap();
    writer
        .row(0, Bytes::from_static(b"opaque-key"), payload_bytes())
        .await
        .unwrap();
    writer
}

// Unix Store::flush cold reopen evidence, not a physical power-loss claim.
// The independent raw factory bypasses caches and a new RepoInstance rebuilds
// gate, counters and interner from the persisted stores.
#[tokio::test]
async fn disk_cold_reopen_preserves_receipt_counts_bindings_and_local_floor() {
    let directory = tempfile::tempdir().unwrap();
    let factory = BoxRepoFactory::fjall_raw(directory.path());
    let backing = factory.create().await.unwrap();
    let image = RepoInstance::new("main".into(), backing.clone(), Vec::new());
    populated(image.clone()).await.finish(&[]).await.unwrap();
    let source_item_id = image.table_config("items").unwrap().store_id;
    assert_ne!(source_item_id, None);
    let info = image.tx_info_store().await.unwrap();
    let configs = SeedTable::load_configs(&info).await.unwrap();
    // No background disk WAL is constructed by RepoInstance::new. The journal
    // worker must release its store before dropping the last backend owner.
    image
        .changefeed()
        .await
        .unwrap()
        .feed
        .shutdown_and_wait()
        .await
        .unwrap();
    drop(info);
    drop(image);
    drop(backing);
    drop(factory);
    let factory = BoxRepoFactory::fjall_raw(directory.path());
    let cold = RepoInstance::new("main".into(), factory.create().await.unwrap(), configs);
    let info = cold.tx_info_store().await.unwrap();
    assert_eq!(
        SeedInstallState::require_complete(&info, true)
            .await
            .unwrap(),
        Some(receipt()),
    );
    assert_eq!(cold.replication_bookmark().await.unwrap(), 73);
    assert_eq!(cold.table_config("items").unwrap().store_id, source_item_id);
    let stores = cold.core.repo().stores_list().await.unwrap();
    assert!(stores
        .iter()
        .any(|name| name.starts_with("__history__items--")));
    assert!(!stores.iter().any(|name| name == "__history__items"));
    let items = cold.get_table("items").await.unwrap();
    let empty = cold.get_table("empty").await.unwrap();
    assert_eq!(items.counter().get().await.unwrap(), 1);
    assert_eq!(empty.counter().get().await.unwrap(), 0);
    let manager = cold.repo_interner().await.unwrap();
    let interner = manager.get().await.unwrap();
    assert!(interner.get_ind("outer").is_some());
    assert!(interner.get_ind("inner").is_some());
    let row = cold
        .get_table("items")
        .await
        .unwrap()
        .mvcc_store()
        .unwrap()
        .get_at(b"opaque-key", 1)
        .await
        .unwrap();
    assert!(row.is_some());
    let gate = cold.tx_gate().await.unwrap();
    assert_eq!((gate.visible_floor(), gate.durable_watermark()), (1, 1));
    assert_eq!(gate.assign_next_version_guarded().version(), 2);
    assert_eq!(
        cold.read_changelog_from(1, 8).await.unwrap().gap_at,
        Some(1)
    );
    cold.changefeed()
        .await
        .unwrap()
        .feed
        .shutdown_and_wait()
        .await
        .unwrap();
}

use std::sync::Arc;

use shamir_storage::storage_in_memory::InMemoryRepo;

use super::*;
use crate::repo::RepoInstance;

#[tokio::test]
async fn startup_seed_waits_and_filters_delayed_snapshot() {
    let repo = RepoInstance::new(
        "seed-test".into(),
        crate::repo::BoxRepo::InMemory(Arc::new(InMemoryRepo::new())),
        Vec::new(),
    );
    let gate = repo.tx_gate().await.unwrap();
    let drainer = Drainer::new();
    let wal = repo.repo_wal().await.unwrap();
    for version in 1..=3 {
        let entry =
            shamir_wal::WalEntryV2::new(version, 0, Vec::new()).with_commit_version(version);
        wal.begin_grouped(&entry, shamir_wal::WalDurability::Buffered)
            .await
            .unwrap();
    }
    let snapshot = wal.recover().await.unwrap();
    assert_eq!(snapshot.len(), 3);
    let guard = drainer.replaying.lock().await;
    let seed = drainer.seed_startup(&repo.core, snapshot);
    tokio::pin!(seed);
    assert!(futures::poll!(&mut seed).is_pending());
    gate.mark_durable(1);
    gate.mark_durable(2);
    assert_eq!(gate.durable_watermark(), 2);
    drop(guard);
    seed.await.unwrap();
    assert_eq!(drainer.window_keys(), vec![3]);
}

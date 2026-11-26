use std::path::PathBuf;
use std::sync::Arc;

use crate::meta::recovery_marker::{load_last_committed, save_last_committed};
use crate::repo::{BoxRepoFactory, DurabilityPolicy, RepoInstance};
use crate::table::TableConfig;
use crate::tx::clean_horizon::recovery_floor::reserve_recovery_floor;

async fn open_disk(name: &str, path: PathBuf, tables: Vec<TableConfig>) -> RepoInstance {
    RepoInstance::from_factory_with_test_segment_bytes(
        name.into(),
        BoxRepoFactory::fjall_raw(path),
        tables,
        DurabilityPolicy::default(),
        256,
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn recovery_floor_waits_for_marker_mutex_and_preserves_higher_floor() {
    let tempdir = tempfile::TempDir::new().expect("tempdir");
    let repo = open_disk(
        "recovery-floor-lock",
        tempdir.path().to_path_buf(),
        vec![TableConfig::new("t")],
    )
    .await;
    let info = repo.tx_info_store().await.unwrap();
    save_last_committed(&info, 20).await.unwrap();

    let mutex = Arc::clone(repo.marker_write_mutex());
    let guard = mutex.lock_owned().await;
    let mut future = Box::pin(reserve_recovery_floor(&repo.core, &info, 7));
    assert!(
        futures::poll!(&mut future).is_pending(),
        "helper must wait for the shared marker mutex"
    );
    drop(guard);
    assert_eq!(future.await.unwrap(), 20);
    assert_eq!(load_last_committed(&info).await.unwrap(), Some(20));
}

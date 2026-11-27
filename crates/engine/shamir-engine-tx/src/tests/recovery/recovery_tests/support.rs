#[cfg(feature = "fjall")]
use std::path::PathBuf;
use std::sync::Arc;

#[cfg(feature = "fjall")]
use crate::repo::BoxRepoFactory;
use crate::repo::{BoxRepo, RepoInstance};
#[cfg(feature = "fjall")]
use crate::table::TableConfig;
use shamir_storage::storage_in_memory::InMemoryRepo;
/// Retry helper for reopening a disk-backed repo on Windows, where the
/// file lock is released lazily after `drop`. On non-Windows platforms this
/// almost always succeeds on the first attempt.
/// Disk-backed (fjall_raw) — requires the `fjall` feature.
#[cfg(feature = "fjall")]
pub(super) async fn reopen_disk_repo(
    name: &str,
    path: PathBuf,
    tables: Vec<TableConfig>,
) -> RepoInstance {
    let mut last_err = None;
    for _attempt in 0..10 {
        match RepoInstance::from_factory_for_recovery_test(
            name.into(),
            BoxRepoFactory::fjall_raw(path.clone()),
            tables.clone(),
        )
        .await
        {
            Ok(r) => return r,
            Err(e) => {
                last_err = Some(e);
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        }
    }
    panic!(
        "reopen_disk_repo({name:?}) failed after 10 retries: {:?}",
        last_err
    );
}
pub(super) fn make_repo() -> RepoInstance {
    let repo = Arc::new(InMemoryRepo::new());
    RepoInstance::new("test".into(), BoxRepo::InMemory(repo), Vec::new())
}

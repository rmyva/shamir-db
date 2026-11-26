#[cfg(test)]
use std::sync::Arc;

use shamir_storage::error::DbResult;
#[cfg(all(test, feature = "fjall"))]
use shamir_storage::types::Repo as _;
#[cfg(test)]
use shamir_storage::types::Store;

use crate::repo::RepoInstance;

impl RepoInstance {
    #[cfg(test)]
    pub(crate) async fn follower_journal_store(&self) -> DbResult<Arc<dyn Store>> {
        self.core.follower_journal_store().await
    }

    pub fn validate_repo_wide_atomic_batch(&self, destination: &str) -> DbResult<()> {
        self.core
            .repo()
            .validate_repo_wide_atomic_batch(destination)
    }

    /// Raw named-store access, test-only: every caller lives under `tests/`,
    /// so this would be dead code in the non-test build.
    #[cfg(all(test, feature = "fjall"))]
    pub(crate) async fn raw_store(&self, name: &str) -> DbResult<Arc<dyn Store>> {
        self.core.repo().store_get(name).await
    }
}

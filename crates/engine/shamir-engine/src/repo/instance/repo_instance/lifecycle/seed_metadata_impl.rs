use shamir_storage::error::DbResult;
use shamir_storage::types::Repo;

use super::super::RepoInstance;

impl RepoInstance {
    /// Strict offline metadata flush. Unlike persist's unchanged fast path,
    /// explicitly crosses the backing store's flush boundary every time.
    pub async fn flush_seed_metadata(&self) -> DbResult<()> {
        self.repo_interner().await?.persist().await?;
        self.core
            .repo()
            .store_get("__interner__")
            .await?
            .flush()
            .await?;
        self.tx_info_store().await?.flush().await?;
        Ok(())
    }
}

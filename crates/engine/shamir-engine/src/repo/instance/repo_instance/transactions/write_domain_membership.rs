//! Admitted table-membership mutations on [`RepoInstance`].

use shamir_storage::error::DbResult;

use super::RepoInstance;
use crate::repo::changelog_store::write_domain::RepoWritePermit;
use crate::table::TableConfig;

impl RepoInstance {
    // No internal await today; PR6b-2c moves admitted store I/O behind this
    // gate, so the async signature is deliberate.
    #[allow(clippy::unused_async)]
    pub async fn add_table_admitted(
        &self,
        config: TableConfig,
        permit: &RepoWritePermit,
    ) -> DbResult<()> {
        self.core
            .durability_handle()
            .write_domain
            .debug_check_permit(permit);
        self.check_table_rename_fence(&config.name)?;
        self.add_table(config, permit);
        Ok(())
    }

    // No internal await today; PR6b-2c moves admitted store I/O behind this
    // gate, so the async signature is deliberate.
    #[allow(clippy::unused_async)]
    pub async fn remove_table_admitted(
        &self,
        table_name: &str,
        permit: &RepoWritePermit,
    ) -> DbResult<bool> {
        self.core
            .durability_handle()
            .write_domain
            .debug_check_permit(permit);
        self.check_table_rename_fence(table_name)?;
        Ok(self.remove_table(table_name, permit))
    }
}

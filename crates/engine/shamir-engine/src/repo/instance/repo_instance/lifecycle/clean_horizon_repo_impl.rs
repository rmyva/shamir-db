#[cfg(test)]
use shamir_storage::error::DbResult;
#[cfg(test)]
#[cfg(feature = "fjall")]
use shamir_storage::types::Repo;
#[cfg(test)]
use shamir_storage::types::Store;

#[cfg(test)]
use crate::repo::journal::backend::durability_handle::retention_gate::RetentionGate;
#[cfg(test)]
use std::sync::Arc;

use super::super::RepoInstance;

impl RepoInstance {
    #[cfg(all(test, feature = "fjall"))]
    pub(crate) async fn changelog_store_for_test(&self) -> DbResult<Arc<dyn Store>> {
        self.core.repo().store_get("__changelog__").await
    }

    #[cfg(all(test, feature = "fjall"))]
    pub(crate) fn set_wal_segment_bytes_for_test(&mut self, bytes: u64) {
        self.core.set_wal_segment_bytes(bytes);
    }

    #[cfg(test)]
    pub(crate) fn disable_background_drainer_for_test(&self) {
        self.core
            .background_drainer_disabled
            .store(true, std::sync::atomic::Ordering::Release);
    }

    #[cfg(test)]
    pub(crate) fn wal_retention_gate(&self) -> &Arc<RetentionGate> {
        self.core.wal_retention_gate()
    }

    #[cfg(test)]
    #[inline]
    pub(crate) fn wal_retention_refuse_writes(&self) -> bool {
        self.core.wal_retention_refuse_writes()
    }

    pub fn wal_retention_metrics(&self) -> [u64; 5] {
        self.core.durability_handle().wal_retention_metrics()
    }
}

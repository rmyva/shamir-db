use std::sync::Arc;

use shamir_storage::error::DbResult;
use shamir_storage::types::{Repo, Store};

use crate::repo::{BoxRepo, RepoInstance};

const SEED_SCHEMA_STORE: &str = "__seed_schema__";

impl RepoInstance {
    /// Shared handle to the backing repository, for the seed publication gate.
    pub fn seed_backing_repo(&self) -> BoxRepo {
        self.core.repo().clone()
    }

    /// Stop the journal writer of an installation image before it is dropped.
    pub async fn seed_stop_journal_writer(&self) -> DbResult<()> {
        self.changefeed().await?.feed.shutdown_and_wait().await
    }

    /// Staged seed schema store of this installation image.
    pub async fn seed_schema_store(&self) -> DbResult<Arc<dyn Store>> {
        self.core.repo().store_get(SEED_SCHEMA_STORE).await
    }
}

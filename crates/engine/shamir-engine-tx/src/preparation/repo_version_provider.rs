//! The version oracle view the SSI read-set validation consults: the
//! per-table MVCC stores of one repository (ENGINE-LAYERING R2d: moved down
//! from `repo::instance::version_provider`, because `begin_tx` on `RepoCore`
//! attaches it to every transaction).

use std::sync::Arc;

use bytes::Bytes;
use shamir_tx::{MvccStore, VersionProvider};
use shamir_types::types::common::THasher;

pub struct RepoVersionProvider {
    pub per_table_mvcc: Arc<scc::HashMap<u64, Arc<MvccStore>, THasher>>,
}

impl VersionProvider for RepoVersionProvider {
    fn version_of(&self, table_id: u64, key: &Bytes) -> Option<u64> {
        self.per_table_mvcc
            .read_sync(&table_id, |_, mvcc| mvcc.version_of(key.as_ref()))
        // Returns None when table_id not in per_table_mvcc map
        // — propagates as conflict in validate_read_set.
    }
}

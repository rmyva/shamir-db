//! Backend-independent retention-gate test: the Mem WAL sink never
//! overflows, so the retention gate must never refuse writes there.

use shamir_storage::storage_in_memory::InMemoryRepo;
use std::sync::Arc;

use crate::repo::{BoxRepo, RepoInstance};
use crate::table::TableConfig;
use crate::tx::clean_horizon::retention::{enforce_retention, RetentionAction};

#[tokio::test]
async fn memory_wal_bypasses_retention_gate() {
    let repo = RepoInstance::new(
        "memory".into(),
        BoxRepo::InMemory(Arc::new(InMemoryRepo::new())),
        vec![TableConfig::new("t")],
    );
    assert_eq!(
        enforce_retention(&repo.core, 0).await.unwrap(),
        RetentionAction::NoOverflow
    );
    assert!(!repo.wal_retention_refuse_writes());
}

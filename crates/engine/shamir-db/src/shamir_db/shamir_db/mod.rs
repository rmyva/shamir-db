mod access;
mod catalog;
mod core;
mod db;
#[path = "../execute/repo_admin/revocation/mod.rs"]
mod revocation;
pub use revocation::receiver::{ChangelogDeliveryGate, ChangelogReceiver};
#[path = "seed/tests/durability_init.rs"]
mod durability_init;
mod seed;

#[cfg(test)]
mod tests;

#[cfg(test)]
pub(crate) use catalog::group_tables_by_repo;
pub(in crate::shamir_db) use catalog::reserved_db_name_refusal;
pub use catalog::{
    is_reserved_db_name, ArtifactKind, ShutdownReport, KIND_FIELD, RESERVED_DB_NAMES,
};
#[cfg(any(test, feature = "test-util"))]
pub use catalog::{
    FinalWriteBarrierForTest, SeedCatalogueFlushFaultForTest, TerminalRowSetFaultForTest,
};
pub(super) use core::SYSTEM_DB_NAME;
pub use core::{FunctionSource, ReplicationControlGuard, ShamirDb};
#[cfg(test)]
pub(crate) use db::classify_batch_error;
pub(crate) use db::schema_management;
pub(in crate::shamir_db) use db::validator::migration_admission::ValidatorRenameMigration;
pub use db::{FunctionDescription, FunctionExport, FunctionState};
pub(in crate::shamir_db) use db::{ValidatorRegistrationError, ValidatorTargetAdmission};
#[cfg(test)]
pub(in crate::shamir_db) use tests::pause_next_function_registration_for_test;

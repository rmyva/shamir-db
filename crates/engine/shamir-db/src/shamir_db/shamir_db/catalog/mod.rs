mod artifact_kind;
mod changelog;
mod reserved_db_names;
mod seed_catalogue;
mod shutdown_report;
mod table_catalogue_grouping;

pub(super) use artifact_kind::with_checked_function_metadata;
pub use artifact_kind::{ArtifactKind, KIND_FIELD};
pub(in crate::shamir_db) use reserved_db_names::reserved_db_name_refusal;
pub(crate) use reserved_db_names::validate_catalogue_db_names;
pub use reserved_db_names::{is_reserved_db_name, RESERVED_DB_NAMES};
#[cfg(any(test, feature = "test-util"))]
pub use seed_catalogue::{
    FinalWriteBarrierForTest, SeedCatalogueFlushFaultForTest, TerminalRowSetFaultForTest,
};
pub use shutdown_report::ShutdownReport;
pub(crate) use table_catalogue_grouping::group_tables_by_repo;

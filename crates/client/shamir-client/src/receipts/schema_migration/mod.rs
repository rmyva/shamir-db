//! Typed application-schema migration client API (release-1 R1c step 3a).
//!
//! `mod.rs` is a manifest: types and logic live in sibling files. The public
//! surface is [`ApplicationMigrationSet`], the inherent
//! [`crate::Client`]
//! operations `status` / `plan` / `apply_next` / `resolve_unknown_apply`,
//! and the typed [`MigrationError`]. All sends go through the existing
//! `Client::execute` path with the typed admin batch operations of
//! `shamir-query-builder`; nothing here constructs wire values by hand.

mod apply;
mod error;
mod plan;
mod resolve;
mod set;
mod status;

pub use error::MigrationError;
pub use resolve::{UnknownApply, UnknownApplyResolution};
pub use set::ApplicationMigrationSet;
pub use status::{InFlightMigration, MigrationStatus, PrefixCoverage};

// Nameable request/result types shared with the server DTOs, re-exported so
// callers of this module can name every parameter and result without a
// direct `shamir-query-types` dependency.
pub use shamir_query_types::admin::types::schema_migration::{
    SchemaMigrationAppliedEntry, SchemaMigrationApplyResult, SchemaMigrationBody,
    SchemaMigrationCanonicalError, SchemaMigrationChecksumValidation, SchemaMigrationError,
    SchemaMigrationErrorCode, SchemaMigrationPlanResult, SchemaMigrationStatusResult,
    SchemaMigrationTarget,
};

//! Online table migration DDL operations.
//!
//! Also hosts the node-local online-backup admin ops (R2a step 3a): these
//! target the node's storage roots rather than a specific database, and are
//! superuser-only — authorization is enforced inside `shamir-db`'s admin
//! handler, so `BatchOp::required_access` returns `None` for them (same as
//! every other admin op).

use crate::admin::types::db_ops::is_false;
use serde::{Deserialize, Serialize};

fn default_repo() -> String {
    "main".to_string()
}

/// Start an online table migration to a different storage engine.
///
/// Requires a hex HMAC-SHA256 tag over the canonical v2 operation encoding.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StartMigrationOp {
    pub start_migration: String,
    #[serde(default = "default_repo")]
    pub repo: String,
    pub dst_repo: String,
    pub dst_engine: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dst_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hmac: Option<String>,
}

/// Commit a running migration — performs cutover + swap.
///
/// Requires a hex HMAC-SHA256 tag over the canonical v2 operation encoding.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommitMigrationOp {
    pub commit_migration: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hmac: Option<String>,
}

/// Rollback a running (or committed-but-not-dropped) migration.
///
/// Requires a hex HMAC-SHA256 tag over the canonical v2 operation encoding.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RollbackMigrationOp {
    pub rollback_migration: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hmac: Option<String>,
}

/// Query the status of a migration by ID, or list all active migrations.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MigrationStatusOp {
    pub migration_status: String,
}

// ------------------------------------------------------------------------
// Node-local online-backup admin ops (R2a step 3a).
//
// Pure wire/serde DTOs like the rest of this module: the wire discriminator
// is a presence-only boolean flag (mirroring `ListPublicationsOp`), sitting
// flat in the op map next to the op's own fields.
// ------------------------------------------------------------------------

/// Start an online backup into the operator-supplied destination root path.
///
/// Superuser-only, node-local admin operation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StartBackupOp {
    /// Discriminator flag — presence-only, conventionally `true`.
    #[serde(default, skip_serializing_if = "is_false")]
    pub start_backup: bool,
    /// Destination root path the backup is written into.
    pub to: String,
}

/// Query the status of the running (or last finished) online backup.
///
/// Superuser-only, node-local admin operation; read-only.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackupStatusOp {
    /// Discriminator flag — presence-only, conventionally `true`.
    #[serde(default, skip_serializing_if = "is_false")]
    pub backup_status: bool,
}

/// Cancel the running online backup.
///
/// Superuser-only, node-local admin operation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CancelBackupOp {
    /// Discriminator flag — presence-only, conventionally `true`.
    #[serde(default, skip_serializing_if = "is_false")]
    pub cancel_backup: bool,
}

// ------------------------------------------------------------------------
// Online logical-export admin ops (R3 step 2b).
//
// Same shape as the backup trio: pure wire/serde DTOs with a presence-only
// boolean discriminator flat in the op map, superuser-only node-local
// admin classification. Scope fields are direct and flat on the wire
// (matching the offline command's `--repo` / `--table` / `--schema-only`
// controls); `include_artifacts` is accepted by the wire DTO like the
// offline flag. Guest-artifact support is out of scope in this phase;
// the online job itself is live and these DTOs carry its wire contract.
// ------------------------------------------------------------------------

/// Start an online logical-dump export job on the live server.
///
/// Superuser-only admin operation, per the agreed wire contract. The
/// online job is live in this release; the server never accepts an
/// output path — per the contract the artifact is written under the
/// configured private export root and only its opaque artifact name is
/// reported.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StartExportOp {
    /// Discriminator flag — presence-only, conventionally `true`.
    #[serde(default, skip_serializing_if = "is_false")]
    pub start_export: bool,
    /// Database to export (the default scope is all of its repositories).
    pub database: String,
    /// Restrict the export to one repository inside `database`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repo: Option<String>,
    /// Restrict the export to one table inside `repo`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table: Option<String>,
    /// Export schema definitions only, no record data.
    #[serde(default)]
    pub schema_only: bool,
    /// Request inclusion of raw guest artifacts. Guest-artifact support
    /// is out of scope in this phase; the flag is carried on the wire
    /// only (`ExportFailure::UnsupportedContent` remains the contract's
    /// closed variant for unsupported content).
    #[serde(default)]
    pub include_artifacts: bool,
}

/// Query the status of the current session's logical-export job.
///
/// Superuser-only admin operation; read-only. Implicitly targets the
/// calling session's job (no export id on the wire).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExportStatusOp {
    /// Discriminator flag — presence-only, conventionally `true`.
    #[serde(default, skip_serializing_if = "is_false")]
    pub export_status: bool,
}

/// Cancel the current session's logical-export job.
///
/// Superuser-only admin operation; implicit current session, no id.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CancelExportOp {
    /// Discriminator flag — presence-only, conventionally `true`.
    #[serde(default, skip_serializing_if = "is_false")]
    pub cancel_export: bool,
}

/// Phase of an online logical-export job (snake_case wire names).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExportPhase {
    /// The job is waiting for the server-wide maintenance gate.
    WaitingForGate,
    /// Sections are being written to the temp artifact.
    Exporting,
    /// The trailer is being verified before the final rename.
    Verifying,
    /// The artifact was renamed to its final name; export complete.
    Done,
    /// The job failed; see `failure` in [`ExportStatusReceipt`].
    Failed,
    /// The job was cancelled before completion.
    Cancelled,
}

/// Closed typed failure reason for a failed export job.
///
/// Closed by design: new reasons are added only as sanctioned variants,
/// never as free-form peer text.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExportFailure {
    /// The 600 s gate ceiling expired before the export finished.
    TimedOut,
    /// The requested scope or options carry content the dump cannot
    /// represent (e.g. `include_artifacts`).
    UnsupportedContent,
    /// An I/O error inside the export root.
    Io,
    /// The trailer verification failed; the temp artifact was removed.
    Verification,
    /// An internal server error; no further detail is reported.
    Internal,
}

/// Receipt for a successful `StartExport`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExportStartReceipt {
    pub export_id: String,
}

/// Receipt for `ExportStatus`: phase, progress counters, and outcome.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExportStatusReceipt {
    pub export_id: String,
    pub phase: ExportPhase,
    pub sections: u64,
    pub items: u64,
    pub bytes: u64,
    /// Opaque artifact name inside the export root; `Some` only in the
    /// `Done` phase. Never a filesystem path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_name: Option<String>,
    /// Typed failure reason; `Some` only in the `Failed` phase.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<ExportFailure>,
}

/// Receipt for `CancelExport`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExportCancelReceipt {
    pub cancelled: bool,
}

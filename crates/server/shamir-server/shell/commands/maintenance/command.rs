//! Maintenance commands (design `docs/dev-artifacts/design/admin-shell.md`
//! §4.7–§4.10, §9 PR5): the online-copy trio (`backup start|status|cancel`),
//! the backup-schedule placeholder, the format-stamp and schema-migration
//! views, the WAL/durability diagnostics and the explained-only offline
//! manuals.
//!
//! The shell stays a thin client. Every remote variant becomes one batch op
//! (or one typed request) sent through [`SessionClient`]; nothing here reads
//! the data directory, inspects the filesystem or reimplements server-side
//! validation. The explained-only commands never run: their
//! [`local`](MaintenanceCommand::local) payload prints the server's own
//! manual together with the reason the shell refuses to execute it.

use serde_json::Value;
use shamir_query_builder::batch::Batch;
use shamir_query_types::batch::BatchOp;
use shamir_query_types::read::QueryRecord;
use shamir_types::types::value::QueryValue;

use super::explain::Explained;
use super::{backup, diag, versions};
use crate::shell::commands::StateCommand;
use crate::shell::connect::{request_error, SessionClient};
use crate::shell::output::ShellError;

/// The maintenance command set (design §§4.7–4.10).
#[derive(Debug, Clone)]
pub(in crate::shell) enum MaintenanceCommand {
    /// `backup start TO` — start one online copy.
    BackupStart { to: String, yes: bool },
    /// `backup status` — observe the running or last online copy.
    BackupStatus,
    /// `backup cancel` — cancel the running online copy.
    BackupCancel { yes: bool },
    /// `version stamp` — the P1 format-stamp status view.
    VersionStamp,
    /// `version migrate` — explained-only format migration manual.
    VersionMigrate,
    /// `schema-migration status DATABASE` — application-schema view.
    SchemaMigrationStatus { database_id: String },
    /// `export logical` — explained-only offline export manual.
    ExportLogical,
    /// `import logical` — explained-only offline import manual.
    ImportLogical,
    /// `backup verify SNAPSHOT` — explained-only snapshot verify manual.
    BackupVerify { snapshot: String },
    /// `backup restore` — explained-only restore manual.
    BackupRestore,
    /// `backup snapshot` — explained-only offline snapshot manual.
    BackupSnapshot,
    /// `diag integrity` — explained-only integrity manual.
    DiagIntegrity,
    /// `diag wal` — the WAL status diagnostic.
    DiagWal,
    /// `diag durability REPO` — the repository durability diagnostic.
    DiagDurability { repo: String },
}

impl MaintenanceCommand {
    /// Wire operation label (JSON v1 `op` field). Remote variants only; a
    /// local or pending command sends nothing, so it has no op label.
    pub(in crate::shell) fn op(&self) -> Option<&'static str> {
        match self {
            MaintenanceCommand::BackupStart { .. } => Some("StartBackup"),
            MaintenanceCommand::BackupStatus => Some("BackupStatus"),
            MaintenanceCommand::BackupCancel { .. } => Some("CancelBackup"),
            MaintenanceCommand::VersionStamp | MaintenanceCommand::DiagWal => Some("Status"),
            MaintenanceCommand::SchemaMigrationStatus { .. } => Some("ApplicationSchema"),
            MaintenanceCommand::DiagDurability { .. } => Some("DescribeRepo"),
            _ => None,
        }
    }

    /// Whether the command needs the profile's routing database floor
    /// (§4.2): the batch operations and the two database-scoped views do;
    /// `version stamp`, `diag wal` and every local manual do not.
    pub(in crate::shell) fn requires_routing_db(&self) -> bool {
        matches!(
            self,
            MaintenanceCommand::BackupStart { .. }
                | MaintenanceCommand::BackupStatus
                | MaintenanceCommand::BackupCancel { .. }
                | MaintenanceCommand::SchemaMigrationStatus { .. }
                | MaintenanceCommand::DiagDurability { .. }
        )
    }

    /// Whether the command mutates node or repository state and therefore
    /// needs `--yes` (or an interactive `yes`).
    pub(in crate::shell) fn wants_confirm(&self) -> bool {
        matches!(
            self,
            MaintenanceCommand::BackupStart { .. } | MaintenanceCommand::BackupCancel { .. }
        )
    }

    /// The `--yes` flag carried by the confirmation-gated variants.
    pub(in crate::shell) fn yes(&self) -> bool {
        matches!(
            self,
            MaintenanceCommand::BackupStart { yes: true, .. }
                | MaintenanceCommand::BackupCancel { yes: true }
        )
    }

    /// The local answer for commands the shell handles without a request.
    /// `None` means the command is remote and goes through [`execute`].
    /// The explained-only manuals return their payload.
    pub(in crate::shell) fn local(&self) -> Option<Result<Value, ShellError>> {
        match self {
            MaintenanceCommand::BackupStart { .. }
            | MaintenanceCommand::BackupStatus
            | MaintenanceCommand::BackupCancel { .. }
            | MaintenanceCommand::VersionStamp
            | MaintenanceCommand::SchemaMigrationStatus { .. }
            | MaintenanceCommand::DiagWal
            | MaintenanceCommand::DiagDurability { .. } => None,
            MaintenanceCommand::VersionMigrate => Some(Explained::VersionMigrate.payload()),
            MaintenanceCommand::ExportLogical => Some(Explained::ExportLogical.payload()),
            MaintenanceCommand::ImportLogical => Some(Explained::ImportLogical.payload()),
            MaintenanceCommand::BackupVerify { .. } => Some(Explained::BackupVerify.payload()),
            MaintenanceCommand::BackupRestore => Some(Explained::BackupRestore.payload()),
            MaintenanceCommand::BackupSnapshot => Some(Explained::BackupSnapshot.payload()),
            MaintenanceCommand::DiagIntegrity => Some(Explained::DiagIntegrity.payload()),
        }
    }

    /// Pre-connection validation. `schema-migration status` parses its
    /// database id into a `RecordId`; `backup verify` refuses an empty
    /// snapshot path. The `backup start --to` destination is deliberately
    /// NOT validated locally: the path is passed unmodified and the
    /// server's own destination policy decides (its refusal is the typed
    /// `BackupInvalidDestination`).
    pub(in crate::shell) fn validate(&self) -> Result<(), ShellError> {
        match self {
            MaintenanceCommand::SchemaMigrationStatus { database_id } => {
                versions::database_id(database_id).map(|_| ())
            }
            MaintenanceCommand::BackupVerify { snapshot } => {
                if snapshot.is_empty() {
                    return Err(ShellError::Usage);
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    /// Execute the command. `routing_db` is the profile database floor and
    /// is required only when [`requires_routing_db`](Self::requires_routing_db)
    /// is true. Authorization happens exclusively on the normal remote
    /// route; this code sends no privilege material.
    ///
    /// `version stamp` reuses the P1 `Status` DTO view
    /// ([`StateCommand::VersionShow`]): the format epoch and stamp outcome
    /// were captured by the server launcher at boot, and the shell never
    /// reads the data directory.
    ///
    /// cancel-safe: NO — a sent request may still execute; do not retry on
    /// timeout.
    pub(in crate::shell) async fn execute(
        &self,
        client: &SessionClient,
        routing_db: Option<&str>,
    ) -> Result<Value, ShellError> {
        match self {
            MaintenanceCommand::VersionStamp => {
                StateCommand::VersionShow.execute(client, None).await
            }
            MaintenanceCommand::DiagWal => {
                let status = client.status().await?;
                Ok(diag::project_wal(&status))
            }
            MaintenanceCommand::DiagDurability { repo } => {
                diag::durability(client, routing_db.ok_or(ShellError::Usage)?, repo).await
            }
            MaintenanceCommand::SchemaMigrationStatus { database_id } => {
                let id = versions::database_id(database_id)?;
                versions::query(client, routing_db.ok_or(ShellError::Usage)?, id).await
            }
            MaintenanceCommand::BackupStart { to, .. } => {
                run_op(
                    client,
                    routing_db.ok_or(ShellError::Usage)?,
                    shamir_query_builder::ddl::start_backup(to.clone()),
                    backup::project_start,
                )
                .await
            }
            MaintenanceCommand::BackupStatus => {
                run_op(
                    client,
                    routing_db.ok_or(ShellError::Usage)?,
                    shamir_query_builder::ddl::backup_status(),
                    backup::project_status,
                )
                .await
            }
            MaintenanceCommand::BackupCancel { .. } => {
                run_op(
                    client,
                    routing_db.ok_or(ShellError::Usage)?,
                    shamir_query_builder::ddl::cancel_backup(),
                    backup::project_cancel,
                )
                .await
            }
            // The dispatcher returns `local()` first, so every variant below
            // is answered there and this arm is unreachable in practice.
            _ => Err(ShellError::Server),
        }
    }
}

/// Send one single-op batch under [`backup::RESULT_ALIAS`], require exactly
/// one direct result record, project it through `project` and check the
/// projected payload against the credential-reflection output guard.
async fn run_op(
    client: &SessionClient,
    db: &str,
    op: BatchOp,
    project: fn(&QueryValue) -> Result<Value, ShellError>,
) -> Result<Value, ShellError> {
    let mut batch = Batch::new();
    batch.insert(backup::RESULT_ALIAS, op);
    // Client-side validation failure is a local usage problem.
    let request = batch.try_build().map_err(|_| ShellError::Usage)?;
    let mut response = client.execute(db, request).await.map_err(request_error)?;
    let result = response
        .results
        .get_mut(backup::RESULT_ALIAS)
        .ok_or(ShellError::Server)?;
    if result.skipped {
        return Err(ShellError::Server);
    }
    let [record] = result.records.as_slice() else {
        return Err(ShellError::Server);
    };
    let value = match record {
        QueryRecord::Direct(value) => value,
        _ => return Err(ShellError::Server),
    };
    let payload = project(value)?;
    client.check_catalogue_output(&payload)?;
    Ok(payload)
}

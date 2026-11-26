use shamir_query_types::admin::{
    BackupStatusOp, CancelBackupOp, CancelExportOp, CommitMigrationOp, ExportStatusOp,
    MigrationStatusOp, RollbackMigrationOp, StartBackupOp, StartExportOp, StartMigrationOp,
};
use shamir_query_types::batch::BatchOp;

use crate::batch::{IntoBatchOp, TryIntoBatchOp};
use crate::write::BuilderError;

/// Start an online table migration. Returns a builder for optional fields.
pub fn start_migration(
    table: impl Into<String>,
    dst_repo: impl Into<String>,
    dst_engine: impl Into<String>,
) -> StartMigration {
    StartMigration {
        table: table.into(),
        repo: "main".to_owned(),
        dst_repo: dst_repo.into(),
        dst_engine: dst_engine.into(),
        dst_path: None,
        hmac: None,
    }
}

/// Builder for [`StartMigrationOp`].
pub struct StartMigration {
    table: String,
    repo: String,
    dst_repo: String,
    dst_engine: String,
    dst_path: Option<String>,
    hmac: Option<String>,
}

impl StartMigration {
    /// Override the source repo (default `"main"`).
    pub fn repo(mut self, repo: impl Into<String>) -> Self {
        self.repo = repo.into();
        self
    }

    /// Set the destination data path.
    pub fn dst_path(mut self, path: impl Into<String>) -> Self {
        self.dst_path = Some(path.into());
        self
    }

    /// Attach the hex-encoded HMAC tag.
    pub fn hmac(mut self, hmac: impl Into<String>) -> Self {
        self.hmac = Some(hmac.into());
        self
    }

    /// Finalize into a [`BatchOp`]. Fails with [`BuilderError::MissingHmac`]
    /// when no `.hmac(tag)` was attached.
    pub fn build(self) -> Result<BatchOp, BuilderError> {
        let hmac = self.hmac.ok_or(BuilderError::MissingHmac)?;
        Ok(BatchOp::StartMigration(StartMigrationOp {
            start_migration: self.table,
            repo: self.repo,
            dst_repo: self.dst_repo,
            dst_engine: self.dst_engine,
            dst_path: self.dst_path,
            hmac: Some(hmac),
        }))
    }
}

impl TryIntoBatchOp for StartMigration {
    fn try_into_batch_op(self) -> Result<BatchOp, BuilderError> {
        self.build()
    }
}

/// Commit a running migration by ID.
pub fn commit_migration(id: impl Into<String>) -> CommitMig {
    CommitMig {
        id: id.into(),
        hmac: None,
    }
}

/// Builder for [`CommitMigrationOp`].
pub struct CommitMig {
    id: String,
    hmac: Option<String>,
}

impl CommitMig {
    /// Attach the hex-encoded HMAC tag.
    pub fn hmac(mut self, hmac: impl Into<String>) -> Self {
        self.hmac = Some(hmac.into());
        self
    }

    /// Finalize into a [`BatchOp`]. Fails with [`BuilderError::MissingHmac`]
    /// when no `.hmac(tag)` was attached.
    pub fn build(self) -> Result<BatchOp, BuilderError> {
        let hmac = self.hmac.ok_or(BuilderError::MissingHmac)?;
        Ok(BatchOp::CommitMigration(CommitMigrationOp {
            commit_migration: self.id,
            hmac: Some(hmac),
        }))
    }
}

impl TryIntoBatchOp for CommitMig {
    fn try_into_batch_op(self) -> Result<BatchOp, BuilderError> {
        self.build()
    }
}

/// Rollback a running migration by ID.
pub fn rollback_migration(id: impl Into<String>) -> RollbackMig {
    RollbackMig {
        id: id.into(),
        hmac: None,
    }
}

/// Builder for [`RollbackMigrationOp`].
pub struct RollbackMig {
    id: String,
    hmac: Option<String>,
}

impl RollbackMig {
    /// Attach the hex-encoded HMAC tag.
    pub fn hmac(mut self, hmac: impl Into<String>) -> Self {
        self.hmac = Some(hmac.into());
        self
    }

    /// Finalize into a [`BatchOp`]. Fails with [`BuilderError::MissingHmac`]
    /// when no `.hmac(tag)` was attached.
    pub fn build(self) -> Result<BatchOp, BuilderError> {
        let hmac = self.hmac.ok_or(BuilderError::MissingHmac)?;
        Ok(BatchOp::RollbackMigration(RollbackMigrationOp {
            rollback_migration: self.id,
            hmac: Some(hmac),
        }))
    }
}

impl TryIntoBatchOp for RollbackMig {
    fn try_into_batch_op(self) -> Result<BatchOp, BuilderError> {
        self.build()
    }
}

/// Query the status of a migration by ID.
pub fn migration_status(id: impl Into<String>) -> BatchOp {
    BatchOp::MigrationStatus(MigrationStatusOp {
        migration_status: id.into(),
    })
}

/// Start an online backup into the operator-supplied destination root path.
pub fn start_backup(to: impl Into<String>) -> BatchOp {
    BatchOp::StartBackup(StartBackupOp {
        start_backup: true,
        to: to.into(),
    })
}

/// Query the status of the online backup.
pub fn backup_status() -> BatchOp {
    BatchOp::BackupStatus(BackupStatusOp {
        backup_status: true,
    })
}

/// Cancel the running online backup.
pub fn cancel_backup() -> BatchOp {
    BatchOp::CancelBackup(CancelBackupOp {
        cancel_backup: true,
    })
}

// ------------------------------------------------------------------------
// Online logical-dump export job (R3 step 2b) — same shape as the backup
// trio: superuser-only node-local admin ops, no operator-supplied output
// path on the wire.
// ------------------------------------------------------------------------

/// Start an online logical-dump export job for `database`.
///
/// Returns a builder for the optional scope options. The server never
/// accepts an output path: the artifact is written under the configured
/// private export root and only its opaque artifact name is reported.
pub fn start_export(database: impl Into<String>) -> StartExport {
    StartExport {
        database: database.into(),
        repo: None,
        table: None,
        schema_only: false,
        include_artifacts: false,
    }
}

/// Builder for [`StartExportOp`]. The online export job itself is live
/// in this release: these ops drive the running job, not just the
/// agreed wire contract.
///
/// Infallible by design: there are no required optional fields, so
/// `build()` cannot fail. Scope validation lives on the SERVER, not
/// here: `table` without `repo` builds fine, and the server's export
/// scope check (`boot/config/diagnostics/logical/export_online/scope.rs`)
/// rejects it at start with `online_export_invalid_scope`.
pub struct StartExport {
    database: String,
    repo: Option<String>,
    table: Option<String>,
    schema_only: bool,
    include_artifacts: bool,
}

impl StartExport {
    /// Restrict the export to one repository inside `database`.
    pub fn repo(mut self, repo: impl Into<String>) -> Self {
        self.repo = Some(repo.into());
        self
    }

    /// Restrict the export to one table inside `.repo(..)`.
    ///
    /// The `table`-without-`repo` pairing is part of the wire contract,
    /// but the BUILDER accepts any combination; the server's export
    /// scope check rejects `table` without `repo` at start
    /// (`online_export_invalid_scope`).
    pub fn table(mut self, table: impl Into<String>) -> Self {
        self.table = Some(table.into());
        self
    }

    /// Export schema definitions only, no record data (default `false`).
    pub fn schema_only(mut self, schema_only: bool) -> Self {
        self.schema_only = schema_only;
        self
    }

    /// Request inclusion of raw guest artifacts (default `false`).
    ///
    /// Guest-artifact support is out of scope in this phase; the flag is
    /// carried on the wire only.
    pub fn include_artifacts(mut self, include_artifacts: bool) -> Self {
        self.include_artifacts = include_artifacts;
        self
    }

    /// Finalize into a [`BatchOp`].
    pub fn build(self) -> BatchOp {
        BatchOp::StartExport(StartExportOp {
            start_export: true,
            database: self.database,
            repo: self.repo,
            table: self.table,
            schema_only: self.schema_only,
            include_artifacts: self.include_artifacts,
        })
    }
}

impl IntoBatchOp for StartExport {
    fn into_batch_op(self) -> BatchOp {
        self.build()
    }
}

/// Query the status of the current session's logical-export job.
pub fn export_status() -> BatchOp {
    BatchOp::ExportStatus(ExportStatusOp {
        export_status: true,
    })
}

/// Cancel the current session's logical-export job.
pub fn cancel_export() -> BatchOp {
    BatchOp::CancelExport(CancelExportOp {
        cancel_export: true,
    })
}

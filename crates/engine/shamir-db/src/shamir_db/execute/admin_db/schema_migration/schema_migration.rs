//! Application-schema migration admin dispatcher arm (release-1 R1c).
//!
//! Implements the `ApplicationSchema` admission entry of
//! [`SchemaMigrationAdminOp`]: authorize `Read` (status), `Manage` (plan and
//! apply) on the database (design §5.2), then delegate to the topical
//! handlers in this folder. Status resolves the serving database's stable
//! identity and reads the durable journal through the step-2a `SystemStore`
//! API; plan (step 2b-2) validates an ordered submission against the applied
//! prefix; apply (step 2b-3a) is journaled, fenced and postcondition-checked
//! in `apply.rs`.
//!
//! §5.3 error mapping lives in `plan::map_store_error` only —
//! application-schema codes stay distinguishable from the online
//! storage-move errors.

use shamir_query_types::admin::types::schema_migration::{
    canonical_body_checksum, SchemaMigrationAdminOp, SchemaMigrationChecksumValidation,
    SchemaMigrationErrorCode, SchemaMigrationStatusRequest,
};

use crate::access::{Action, ResourcePath};
use crate::query::batch::{BatchError, BatchOp};
use crate::query::read::QueryResult;
use crate::shamir_db::system_store::{DatabaseIdentity, JournalEventKind, MigrationHistory};
use crate::types::value::QueryValue;
use shamir_types::types::common::new_map;

use super::super::admin_dispatch::ShamirAdminExecutor;
use super::super::helpers::admin_result;
use super::plan::map_store_error;

impl ShamirAdminExecutor {
    pub(in crate::shamir_db::execute) async fn handle_application_schema(
        &self,
        batch_op: &BatchOp,
    ) -> Result<QueryResult, BatchError> {
        let BatchOp::ApplicationSchema(op) = batch_op else {
            unreachable!("handle_application_schema called with a non-ApplicationSchema op");
        };
        match op {
            SchemaMigrationAdminOp::ApplicationSchemaStatus(request) => {
                self.authorize_database_read().await?;
                self.status_result(request).await
            }
            SchemaMigrationAdminOp::ApplicationSchemaPlan(request) => {
                super::plan::authorize_database_manage(self).await?;
                // Box::pin keeps the plan future out of this dispatcher's
                // own stack frame (Windows debug-build overflow, 2026-11-17).
                Box::pin(super::plan::plan_application_schema(self, request)).await
            }
            SchemaMigrationAdminOp::ApplyApplicationSchemaMigration(request) => {
                // §5.2 admission order starts with `Manage` on the database
                // through the normal access path; everything else (identity,
                // checksum, predecessor, fence, preflights) holds inside the
                // apply handler before any effect. Box::pin keeps the apply
                // future — which awaits many DDL handlers — out of this
                // dispatcher's own stack frame (same Windows debug-build
                // overflow mechanism as the plan arm).
                super::plan::authorize_database_manage(self).await?;
                Box::pin(super::apply::apply_application_schema(self, request)).await
            }
        }
    }

    /// §5.2 authorization: `Read` on the database, through the normal
    /// `authorize_access` path — no bypass — before any journal byte is read.
    async fn authorize_database_read(&self) -> Result<(), BatchError> {
        self.shamir
            .authorize_access(
                &self.actor,
                &ResourcePath::database(self.db_name.clone()),
                Action::Read,
            )
            .await
            .map_err(|error| BatchError::QueryError {
                alias: String::new(),
                message: error.to_string(),
                code: Some("access_denied".to_string()),
            })
    }

    /// Read identity + history through the step-2a API and encode the typed
    /// status result as the admin record. The wire field names mirror the
    /// serde names of `SchemaMigrationStatusResult`.
    async fn status_result(
        &self,
        request: &SchemaMigrationStatusRequest,
    ) -> Result<QueryResult, BatchError> {
        let store = self.shamir.system_store();
        let serving = store
            .schema_migration_identity(&self.db_name)
            .await
            .map_err(map_store_error)?;
        let target = DatabaseIdentity(request.target.database_identity);
        if target != serving {
            return Ok(admin_result(status_value(&StatusOutcome {
                protocol_version: request.protocol_version,
                identity: serving,
                history: None,
                checksum_validation: SchemaMigrationChecksumValidation::NotChecked,
                error: Some(SchemaMigrationErrorCode::SchemaMigrationIdentityMismatch),
            })));
        }
        let history = store
            .read_schema_migration_history(serving)
            .await
            .map_err(map_store_error)?;
        let outcome = StatusOutcome {
            protocol_version: request.protocol_version,
            identity: serving,
            history: Some(&history),
            checksum_validation: validate_last_applied_checksum(&history),
            error: if history.in_doubt {
                Some(SchemaMigrationErrorCode::SchemaMigrationInDoubt)
            } else {
                None
            },
        };
        Ok(admin_result(status_value(&outcome)))
    }
}

/// The assembled status outcome before wire encoding; tests-free internal
/// view keeping `status_value` total.
struct StatusOutcome<'a> {
    protocol_version: u16,
    identity: DatabaseIdentity,
    history: Option<&'a MigrationHistory>,
    checksum_validation: SchemaMigrationChecksumValidation,
    error: Option<SchemaMigrationErrorCode>,
}

/// Recompute the canonical checksum of the last applied migration's stored
/// body and compare it with the recorded applied checksum (§5.2: checksum
/// verification is server-side; the status request carries no trusted data).
fn validate_last_applied_checksum(history: &MigrationHistory) -> SchemaMigrationChecksumValidation {
    let Some(applied) = history.applied.last() else {
        return SchemaMigrationChecksumValidation::NotChecked;
    };
    let body = history
        .events
        .iter()
        .rev()
        .find(|event| {
            event.migration.id == applied.migration.id && event.kind == JournalEventKind::Started
        })
        .and_then(|event| event.body.as_ref());
    let Some(body) = body else {
        return SchemaMigrationChecksumValidation::MissingBody;
    };
    match canonical_body_checksum(body) {
        Ok(checksum) if checksum == applied.checksum => SchemaMigrationChecksumValidation::Valid,
        Ok(_) => SchemaMigrationChecksumValidation::Mismatch,
        Err(_) => SchemaMigrationChecksumValidation::UnknownVersion,
    }
}

/// Encode the typed result with the serde field names of
/// `SchemaMigrationStatusResult`.
fn status_value(outcome: &StatusOutcome<'_>) -> QueryValue {
    let mut target = new_map();
    target.insert(
        "database_identity".to_string(),
        QueryValue::Str(outcome.identity.to_string()),
    );
    let mut value = new_map();
    value.insert(
        "protocol_version".to_string(),
        QueryValue::Int(outcome.protocol_version as i64),
    );
    value.insert("target".to_string(), QueryValue::Map(target));
    let (last_applied_id, last_applied_ordinal, last_applied_checksum, in_flight) =
        match outcome.history {
            Some(history) => applied_fields(history),
            None => (None, 0, None, None),
        };
    if let Some(id) = last_applied_id {
        value.insert("last_applied_id".to_string(), QueryValue::Str(id));
    }
    value.insert(
        "last_applied_ordinal".to_string(),
        QueryValue::Int(last_applied_ordinal as i64),
    );
    if let Some(checksum) = last_applied_checksum {
        value.insert(
            "last_applied_checksum".to_string(),
            QueryValue::Bin(checksum.to_vec()),
        );
    }
    value.insert(
        "checksum_validation".to_string(),
        QueryValue::Str(checksum_validation_str(outcome.checksum_validation).to_string()),
    );
    if let Some(in_flight) = in_flight {
        value.insert("in_flight".to_string(), in_flight);
    }
    if let Some(error) = outcome.error {
        value.insert(
            "error".to_string(),
            QueryValue::Str(error_code_str(error).to_string()),
        );
    }
    // R1c step 3a: the durable fence generation (the next fresh apply's
    // expected generation) and the whole applied prefix, so a client can
    // verify its local set and start the next migration without guessing.
    // Additive: older clients ignore both keys.
    let (fence_generation, applied) = match outcome.history {
        Some(history) => (durable_fence_generation(history), applied_entries(history)),
        None => (0, Vec::new()),
    };
    value.insert(
        "fence_generation".to_string(),
        QueryValue::Int(fence_generation as i64),
    );
    if !applied.is_empty() {
        value.insert("applied".to_string(), QueryValue::List(applied));
    }
    QueryValue::Map(value)
}

/// The highest fence generation any journal event carries: the durable control
/// generation (a release bumps it, so after an applied migration it is the
/// generation the next fresh apply must expect). `0` for an empty journal.
fn durable_fence_generation(history: &MigrationHistory) -> u64 {
    history
        .events
        .iter()
        .map(|event| event.fence_generation)
        .max()
        .unwrap_or(0)
}

/// Every applied migration in ordinal order: id, ordinal, checksum.
fn applied_entries(history: &MigrationHistory) -> Vec<QueryValue> {
    history
        .applied
        .iter()
        .map(|event| {
            let mut map = new_map();
            map.insert(
                "id".to_string(),
                QueryValue::Str(event.migration.id.clone()),
            );
            map.insert(
                "ordinal".to_string(),
                QueryValue::Int(event.migration.ordinal as i64),
            );
            map.insert(
                "checksum".to_string(),
                QueryValue::Bin(event.checksum.to_vec()),
            );
            QueryValue::Map(map)
        })
        .collect()
}

/// Derive last-applied and in-flight fields from the durable history.
fn applied_fields(
    history: &MigrationHistory,
) -> (Option<String>, u64, Option<[u8; 32]>, Option<QueryValue>) {
    let applied = history.applied.last();
    let in_flight = history.unresolved.last().map(|event| {
        let mut map = new_map();
        map.insert(
            "migration_id".to_string(),
            QueryValue::Str(event.migration.id.clone()),
        );
        map.insert(
            "ordinal".to_string(),
            QueryValue::Int(event.migration.ordinal as i64),
        );
        if let Some(index) = event.operation_index {
            map.insert("operation_index".to_string(), QueryValue::Int(index as i64));
        }
        map.insert(
            "fence_generation".to_string(),
            QueryValue::Int(event.fence_generation as i64),
        );
        map.insert(
            "status".to_string(),
            QueryValue::Str(event_kind_str(event.kind).to_string()),
        );
        // R1c step 2b-3b: surface the recorded evidence of an in-doubt or
        // failed unresolved event. Additive: the key is present only when
        // the event carries evidence, so the wire shape is otherwise
        // unchanged.
        if matches!(
            event.kind,
            JournalEventKind::InDoubt | JournalEventKind::Failed
        ) {
            if let Some(evidence) = &event.evidence {
                map.insert(
                    "in_doubt_reason".to_string(),
                    QueryValue::Str(evidence.clone()),
                );
            }
        }
        QueryValue::Map(map)
    });
    (
        applied.map(|event| event.migration.id.clone()),
        applied.map_or(0, |event| event.migration.ordinal),
        applied.map(|event| event.checksum),
        in_flight,
    )
}

fn checksum_validation_str(validation: SchemaMigrationChecksumValidation) -> &'static str {
    match validation {
        SchemaMigrationChecksumValidation::Valid => "valid",
        SchemaMigrationChecksumValidation::MissingBody => "missing_body",
        SchemaMigrationChecksumValidation::Mismatch => "mismatch",
        SchemaMigrationChecksumValidation::UnknownVersion => "unknown_version",
        SchemaMigrationChecksumValidation::NotChecked => "not_checked",
    }
}

fn error_code_str(code: SchemaMigrationErrorCode) -> &'static str {
    match code {
        SchemaMigrationErrorCode::SchemaMigrationInProgress => "schema_migration_in_progress",
        SchemaMigrationErrorCode::SchemaMigrationInDoubt => "schema_migration_in_doubt",
        SchemaMigrationErrorCode::SchemaMigrationChecksumMismatch => {
            "schema_migration_checksum_mismatch"
        }
        SchemaMigrationErrorCode::SchemaMigrationGap => "schema_migration_gap",
        SchemaMigrationErrorCode::SchemaMigrationPredecessorMismatch => {
            "schema_migration_predecessor_mismatch"
        }
        SchemaMigrationErrorCode::SchemaMigrationFollowerReadOnly => {
            "schema_migration_follower_read_only"
        }
        SchemaMigrationErrorCode::SchemaMigrationDurabilityUnavailable => {
            "schema_migration_durability_unavailable"
        }
        SchemaMigrationErrorCode::SchemaMigrationPostconditionFailed => {
            "schema_migration_postcondition_failed"
        }
        SchemaMigrationErrorCode::SchemaMigrationIdentityMismatch => {
            "schema_migration_identity_mismatch"
        }
    }
}

fn event_kind_str(kind: JournalEventKind) -> &'static str {
    match kind {
        JournalEventKind::Started => "started",
        JournalEventKind::OperationIntent => "operation_intent",
        JournalEventKind::OperationComplete => "operation_complete",
        JournalEventKind::Applied => "applied",
        JournalEventKind::InDoubt => "in_doubt",
        JournalEventKind::Failed => "failed",
        JournalEventKind::RecoveryAcquired => "recovery_acquired",
        JournalEventKind::FenceReleased => "fence_released",
    }
}

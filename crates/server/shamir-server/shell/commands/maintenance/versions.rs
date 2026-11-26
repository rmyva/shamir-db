//! `schema-migration status` request building and strict response projection.

use serde_json::json;
use shamir_query_builder::batch::Batch;
use shamir_query_types::read::QueryRecord;
use shamir_types::types::record_id::RecordId;
use shamir_types::types::value::QueryValue;

use crate::shell::connect::SessionClient;
use crate::shell::output::{self, ShellError};

/// The application-schema status protocol version this shell speaks.
pub(in crate::shell) const PROTOCOL_VERSION: u16 = 1;

/// Alias of the one batch op the status query sends.
pub(in crate::shell) const RESULT_ALIAS: &str = "result";

/// A Base58 `RecordId` is at most 22 bytes; the shell caps before parsing.
const MAX_RECORD_ID_BYTES: usize = 22;

/// Parse a database identity exactly as `data get` parses its key.
pub(in crate::shell) fn database_id(text: &str) -> Result<RecordId, ShellError> {
    if text.is_empty() || text.len() > MAX_RECORD_ID_BYTES {
        return Err(ShellError::InvalidRecordId);
    }
    text.parse::<RecordId>()
        .map_err(|_| ShellError::InvalidRecordId)
}

/// Send one application-schema status op and project its single direct
/// result record.
pub(in crate::shell) async fn query(
    client: &SessionClient,
    db: &str,
    id: RecordId,
) -> Result<serde_json::Value, ShellError> {
    let request = {
        let mut batch = Batch::new();
        batch.insert(
            RESULT_ALIAS,
            shamir_query_builder::application_schema_status(
                shamir_query_types::admin::types::schema_migration::SchemaMigrationStatusRequest {
                    protocol_version: PROTOCOL_VERSION,
                    target:
                        shamir_query_types::admin::types::schema_migration::SchemaMigrationTarget {
                            database_identity: id,
                        },
                },
            ),
        );
        batch.try_build().map_err(|_| ShellError::Usage)?
    };
    let mut response = client
        .execute(db, request)
        .await
        .map_err(crate::shell::connect::request_error)?;
    let result = response
        .results
        .get_mut(RESULT_ALIAS)
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
    project(value)
}

/// Strict allowlist projection of the status payload; counters are decimal
/// strings, checksums are lowercase hex and peer error text never survives.
fn project(v: &QueryValue) -> Result<serde_json::Value, ShellError> {
    let QueryValue::Map(fields) = v else {
        return Err(ShellError::Server);
    };
    for key in fields.keys() {
        if !matches!(
            key.as_str(),
            "protocol_version"
                | "target"
                | "last_applied_id"
                | "last_applied_ordinal"
                | "last_applied_checksum"
                | "checksum_validation"
                | "in_flight"
                | "error"
                | "fence_generation"
                | "applied"
        ) {
            return Err(ShellError::Server);
        }
    }

    let protocol_version = decimal(fields.get("protocol_version").ok_or(ShellError::Server)?)?;

    let QueryValue::Map(target) = fields.get("target").ok_or(ShellError::Server)? else {
        return Err(ShellError::Server);
    };
    if target.len() != 1 {
        return Err(ShellError::Server);
    }
    for key in target.keys() {
        if key.as_str() != "database_identity" {
            return Err(ShellError::Server);
        }
    }
    let database_identity = output::sanitize(text(
        target.get("database_identity").ok_or(ShellError::Server)?,
    )?);

    let last_applied_id = match fields.get("last_applied_id") {
        Some(item) => Some(output::sanitize(text(item)?)),
        None => None,
    };
    let last_applied_ordinal = decimal(
        fields
            .get("last_applied_ordinal")
            .ok_or(ShellError::Server)?,
    )?;
    let last_applied_checksum = match fields.get("last_applied_checksum") {
        Some(QueryValue::Bin(bytes)) => Some(hex::encode(bytes)),
        Some(_) => return Err(ShellError::Server),
        None => None,
    };

    let checksum_validation = text(
        fields
            .get("checksum_validation")
            .ok_or(ShellError::Server)?,
    )?;
    if !matches!(
        checksum_validation,
        "valid" | "missing_body" | "mismatch" | "unknown_version" | "not_checked"
    ) {
        return Err(ShellError::Server);
    }

    let in_flight = match fields.get("in_flight") {
        Some(QueryValue::Map(flight)) => {
            for key in flight.keys() {
                if !matches!(
                    key.as_str(),
                    "migration_id"
                        | "ordinal"
                        | "operation_index"
                        | "fence_generation"
                        | "status"
                        | "in_doubt_reason"
                ) {
                    return Err(ShellError::Server);
                }
            }
            let operation_index = match flight.get("operation_index") {
                Some(item) => Some(decimal(item)?),
                None => None,
            };
            let in_doubt_reason = match flight.get("in_doubt_reason") {
                Some(item) => {
                    text(item)?;
                    true
                }
                None => false,
            };
            Some(json!({
                "migration_id": output::sanitize(
                    text(flight.get("migration_id").ok_or(ShellError::Server)?)?,
                ),
                "ordinal": decimal(flight.get("ordinal").ok_or(ShellError::Server)?)?,
                "operation_index": operation_index,
                "fence_generation": decimal(
                    flight.get("fence_generation").ok_or(ShellError::Server)?,
                )?,
                "status": output::sanitize(
                    text(flight.get("status").ok_or(ShellError::Server)?)?,
                ),
                "in_doubt_reason": in_doubt_reason,
            }))
        }
        Some(_) => return Err(ShellError::Server),
        None => None,
    };

    let error = match fields.get("error") {
        Some(item) => {
            let code = text(item)?;
            if !matches!(
                code,
                "schema_migration_in_progress"
                    | "schema_migration_in_doubt"
                    | "schema_migration_checksum_mismatch"
                    | "schema_migration_gap"
                    | "schema_migration_predecessor_mismatch"
                    | "schema_migration_follower_read_only"
                    | "schema_migration_durability_unavailable"
                    | "schema_migration_postcondition_failed"
                    | "schema_migration_identity_mismatch"
            ) {
                return Err(ShellError::Server);
            }
            Some(code)
        }
        None => None,
    };

    let fence_generation = decimal(fields.get("fence_generation").ok_or(ShellError::Server)?)?;

    let applied = match fields.get("applied") {
        Some(QueryValue::List(entries)) => {
            let mut applied = Vec::with_capacity(entries.len());
            for entry in entries {
                let QueryValue::Map(entry_fields) = entry else {
                    return Err(ShellError::Server);
                };
                if entry_fields.len() != 3 {
                    return Err(ShellError::Server);
                }
                for key in entry_fields.keys() {
                    if !matches!(key.as_str(), "id" | "ordinal" | "checksum") {
                        return Err(ShellError::Server);
                    }
                }
                let checksum = match entry_fields.get("checksum").ok_or(ShellError::Server)? {
                    QueryValue::Bin(bytes) => hex::encode(bytes),
                    _ => return Err(ShellError::Server),
                };
                applied.push(json!({
                    "id": output::sanitize(
                        text(entry_fields.get("id").ok_or(ShellError::Server)?)?,
                    ),
                    "ordinal": decimal(
                        entry_fields.get("ordinal").ok_or(ShellError::Server)?,
                    )?,
                    "checksum": checksum,
                }));
            }
            applied
        }
        Some(_) => return Err(ShellError::Server),
        None => Vec::new(),
    };
    let applied_count = applied.len().to_string();

    Ok(json!({
        "database_identity": database_identity,
        "protocol_version": protocol_version,
        "last_applied_id": last_applied_id,
        "last_applied_ordinal": last_applied_ordinal,
        "last_applied_checksum": last_applied_checksum,
        "checksum_validation": checksum_validation,
        "error": error,
        "fence_generation": fence_generation,
        "in_flight": in_flight,
        "applied_count": applied_count,
        "applied": applied,
    }))
}

fn text(item: &QueryValue) -> Result<&str, ShellError> {
    match item {
        QueryValue::Str(value) => Ok(value),
        _ => Err(ShellError::Server),
    }
}

fn decimal(item: &QueryValue) -> Result<String, ShellError> {
    match item {
        QueryValue::Int(number) if *number >= 0 => Ok(number.to_string()),
        _ => Err(ShellError::Server),
    }
}

#[cfg(test)]
#[path = "../../tests/commands/maintenance/versions_projection_tests.rs"]
mod versions_projection_tests;

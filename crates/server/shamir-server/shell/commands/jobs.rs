//! Schedule and online-export command projections (admin-shell §4.7/§4.9).

use std::time::{Duration, Instant};

use serde_json::{json, Value};
use shamir_client::{ExportCancelReceipt, ExportFailure, ExportPhase, ExportStatusReceipt};
use shamir_query_builder::batch::Batch;
use shamir_query_types::read::QueryRecord;
use shamir_types::types::value::QueryValue;

use crate::shell::connect::SessionClient;
use crate::shell::output::ShellError;

/// Supported scheduled-backup and online-export command family.
#[derive(Debug, Clone)]
pub(in crate::shell) enum JobsCommand {
    ScheduleShow,
    ExportStart {
        database: String,
        repo: Option<String>,
        table: Option<String>,
        schema_only: bool,
        include_artifacts: bool,
        yes: bool,
    },
    ExportStatus,
    ExportCancel {
        yes: bool,
    },
}

impl JobsCommand {
    pub(in crate::shell) fn id(&self) -> &'static str {
        match self {
            Self::ScheduleShow => "backup schedule show",
            Self::ExportStart { .. } => "export online start",
            Self::ExportStatus => "export online status",
            Self::ExportCancel { .. } => "export online cancel",
        }
    }

    pub(in crate::shell) fn op(&self) -> &'static str {
        match self {
            Self::ScheduleShow => "BackupStatus",
            Self::ExportStart { .. } => "StartExport",
            Self::ExportStatus => "ExportStatus",
            Self::ExportCancel { .. } => "CancelExport",
        }
    }

    pub(in crate::shell) fn wants_confirm(&self) -> bool {
        matches!(self, Self::ExportStart { .. } | Self::ExportCancel { .. })
    }

    pub(in crate::shell) fn yes(&self) -> bool {
        matches!(
            self,
            Self::ExportStart { yes: true, .. } | Self::ExportCancel { yes: true }
        )
    }

    pub(in crate::shell) fn requires_routing_db(&self) -> bool {
        true
    }

    pub(in crate::shell) async fn execute(
        &self,
        client: &SessionClient,
        job_database: Option<&str>,
        routing_db: Option<&str>,
        batch: bool,
    ) -> Result<Value, ShellError> {
        match self {
            Self::ScheduleShow => {
                let mut batch = Batch::new();
                batch.insert("result", shamir_query_builder::ddl::backup_status());
                let request = batch.try_build().map_err(|_| ShellError::Usage)?;
                let db = routing_db.ok_or(ShellError::Usage)?;
                let mut response = client
                    .execute(db, request)
                    .await
                    .map_err(crate::shell::connect::request_error)?;
                let result = response
                    .results
                    .get_mut("result")
                    .ok_or(ShellError::Server)?;
                if result.skipped {
                    return Err(ShellError::Server);
                }
                let [QueryRecord::Direct(QueryValue::Map(fields))] = result.records.as_slice()
                else {
                    return Err(ShellError::Server);
                };
                project_schedule(fields.get("schedule"))
            }
            Self::ExportStart {
                database,
                repo,
                table,
                schema_only,
                include_artifacts,
                ..
            } => {
                let mut builder =
                    shamir_query_builder::ddl::migration::start_export(database.clone());
                if let Some(repo) = repo {
                    builder = builder.repo(repo.clone());
                }
                if let Some(table) = table {
                    builder = builder.table(table.clone());
                }
                builder = builder
                    .schema_only(*schema_only)
                    .include_artifacts(*include_artifacts);
                if !batch {
                    let receipt = client.start_export(database, builder).await?;
                    return Ok(json!({
                        "export_id": opaque_label(&receipt.export_id, "ex")?,
                    }));
                }
                // The normal command deadline is 60s; reserve five seconds for
                // teardown so a timeout closes the owning session cleanly.
                // Every poll uses the REQUESTED database: the server resolves
                // the job by session handle AND the envelope database, so a
                // poll against the routing database would miss a job that was
                // started on another database.
                const BATCH_EXPORT_WAIT: Duration = Duration::from_secs(55);
                let deadline = Instant::now() + BATCH_EXPORT_WAIT;
                tokio::time::timeout_at(deadline.into(), client.start_export(database, builder))
                    .await
                    .map_err(|_| ShellError::Timeout)??;
                loop {
                    let status =
                        tokio::time::timeout_at(deadline.into(), client.export_status(database))
                            .await
                            .map_err(|_| ShellError::Timeout)??;
                    if matches!(
                        status.phase,
                        ExportPhase::Done | ExportPhase::Failed | ExportPhase::Cancelled
                    ) {
                        return project_status(&status);
                    }
                    if Instant::now() >= deadline {
                        return Err(ShellError::Timeout);
                    }
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
            // Status/cancel have no positional database. The envelope
            // database is the database of the job THIS session started
            // (the server resolves the job by session handle AND envelope),
            // falling back to the profile's routing database.
            Self::ExportStatus => {
                let db = status_envelope(job_database, routing_db).ok_or(ShellError::Usage)?;
                let receipt = client.export_status(db).await?;
                project_status(&receipt)
            }
            Self::ExportCancel { .. } => {
                let db = status_envelope(job_database, routing_db).ok_or(ShellError::Usage)?;
                let receipt = client.cancel_export(db).await?;
                project_cancel(&receipt)
            }
        }
    }
}

fn project_schedule(schedule: Option<&QueryValue>) -> Result<Value, ShellError> {
    let Some(QueryValue::Map(schedule)) = schedule else {
        return if schedule.is_none() {
            Ok(json!({"state": "schedule_not_configured"}))
        } else {
            Err(ShellError::Server)
        };
    };
    let allowed = [
        "enabled",
        "interval_seconds",
        "window",
        "keep_last",
        "run_on_start",
        "next_run_unix_ns",
        "last_run",
        "runs",
    ];
    if schedule.keys().any(|key| !allowed.contains(&key.as_str())) {
        return Err(ShellError::Server);
    }
    let enabled = bool_field(schedule, "enabled")?;
    let interval = int_decimal(schedule, "interval_seconds")?;
    let window = optional_string(schedule.get("window"))?;
    let keep_last = int_decimal(schedule, "keep_last")?;
    let run_on_start = bool_field(schedule, "run_on_start")?;
    let next = optional_timestamp(schedule.get("next_run_unix_ns"))?;
    let last_run = match schedule.get("last_run") {
        None => Value::Null,
        Some(QueryValue::Map(run)) => {
            let allowed = ["started_at_unix_ns", "result", "stage", "reason"];
            if run.keys().any(|key| !allowed.contains(&key.as_str())) {
                return Err(ShellError::Server);
            }
            json!({"started_at": timestamp_field(run, "started_at_unix_ns")?, "result": string_field(run, "result")?, "stage": sanitize_field(run, "stage")?, "reason": optional_string(run.get("reason"))?})
        }
        _ => return Err(ShellError::Server),
    };
    let QueryValue::Map(runs) = schedule.get("runs").ok_or(ShellError::Server)? else {
        return Err(ShellError::Server);
    };
    let run_keys = [
        "scheduled_started",
        "scheduled_skipped_busy",
        "scheduled_skipped_window",
        "scheduled_failed",
        "scheduled_completed",
        "failure_streak",
        "retention_deleted",
        "retention_failed",
    ];
    if runs.len() != run_keys.len() || runs.keys().any(|key| !run_keys.contains(&key.as_str())) {
        return Err(ShellError::Server);
    }
    let mut run_values = serde_json::Map::new();
    for key in run_keys {
        run_values.insert(key.to_owned(), Value::String(int_decimal(runs, key)?));
    }
    Ok(
        json!({"state":"configured", "enabled":enabled, "interval_seconds":interval, "window":window, "keep_last":keep_last, "run_on_start":run_on_start, "next_run":next, "last_run":last_run, "runs":run_values}),
    )
}

fn bool_field(
    fields: &shamir_collections::TMap<String, QueryValue>,
    key: &str,
) -> Result<bool, ShellError> {
    match fields.get(key) {
        Some(QueryValue::Bool(value)) => Ok(*value),
        _ => Err(ShellError::Server),
    }
}
fn string_field<'a>(
    fields: &'a shamir_collections::TMap<String, QueryValue>,
    key: &str,
) -> Result<&'a str, ShellError> {
    match fields.get(key) {
        Some(QueryValue::Str(value)) => Ok(value),
        _ => Err(ShellError::Server),
    }
}
fn sanitize_field(
    fields: &shamir_collections::TMap<String, QueryValue>,
    key: &str,
) -> Result<String, ShellError> {
    Ok(crate::shell::output::sanitize(string_field(fields, key)?))
}
fn optional_string(value: Option<&QueryValue>) -> Result<Option<String>, ShellError> {
    match value {
        None | Some(QueryValue::Null) => Ok(None),
        Some(QueryValue::Str(s)) => Ok(Some(crate::shell::output::sanitize(s))),
        _ => Err(ShellError::Server),
    }
}
fn int_decimal(
    fields: &shamir_collections::TMap<String, QueryValue>,
    key: &str,
) -> Result<String, ShellError> {
    match fields.get(key) {
        Some(QueryValue::Int(value)) if *value >= 0 => Ok(value.to_string()),
        _ => Err(ShellError::Server),
    }
}
fn digits_field(
    fields: &shamir_collections::TMap<String, QueryValue>,
    key: &str,
) -> Result<String, ShellError> {
    match fields.get(key) {
        Some(QueryValue::Str(value))
            if !value.is_empty() && value.bytes().all(|b| b.is_ascii_digit()) =>
        {
            Ok(value.clone())
        }
        _ => Err(ShellError::Server),
    }
}
fn timestamp_field(
    fields: &shamir_collections::TMap<String, QueryValue>,
    key: &str,
) -> Result<String, ShellError> {
    let nanos = digits_field(fields, key)?
        .parse::<u128>()
        .map_err(|_| ShellError::Server)?;
    let seconds = nanos / 1_000_000_000;
    let subsecond = (nanos % 1_000_000_000) as u32;
    let seconds = i64::try_from(seconds).map_err(|_| ShellError::Server)?;
    let time = std::time::UNIX_EPOCH
        .checked_add(std::time::Duration::new(seconds as u64, subsecond))
        .ok_or(ShellError::Server)?;
    let datetime = time
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| ShellError::Server)?;
    let days = (datetime.as_secs() / 86_400) as i64;
    let seconds_of_day = datetime.as_secs() % 86_400;
    let (year, month, day) = civil_from_days(days);
    Ok(format!(
        "{year:04}-{:02}-{day:02}T{:02}:{:02}:{:02}.{:09}Z",
        month,
        seconds_of_day / 3_600,
        (seconds_of_day % 3_600) / 60,
        seconds_of_day % 60,
        datetime.subsec_nanos()
    ))
}

// Proleptic Gregorian conversion, days since 1970-01-01.
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let mut year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    year += i64::from(month <= 2);
    (year, month as u32, day as u32)
}

fn optional_timestamp(value: Option<&QueryValue>) -> Result<Option<String>, ShellError> {
    match value {
        None | Some(QueryValue::Null) => Ok(None),
        Some(value @ QueryValue::Str(_)) => {
            let mut fields = shamir_collections::new_map();
            fields.insert("timestamp".to_owned(), value.clone());
            Ok(Some(timestamp_field(&fields, "timestamp")?))
        }
        _ => Err(ShellError::Server),
    }
}

fn project_status(receipt: &ExportStatusReceipt) -> Result<Value, ShellError> {
    let export_id = opaque_label(&receipt.export_id, "ex")?;
    let artifact_name = receipt
        .artifact_name
        .as_deref()
        .map(|label| opaque_label(label, "export"))
        .transpose()?;
    Ok(json!({
        "export_id": export_id,
        "phase": phase(receipt.phase),
        "sections": receipt.sections.to_string(),
        "items": receipt.items.to_string(),
        "bytes": receipt.bytes.to_string(),
        "artifact_name": artifact_name,
        "failure": receipt.failure.map(failure_reason),
    }))
}

/// Server-generated opaque labels use `ex-{unix_millis}-{seq:06}` and
/// `export-{unix_millis}-{seq:06}`. Reject every other alphabet rather than
/// sanitizing a path-shaped peer value into something that still looks valid.
fn opaque_label(label: &str, prefix: &str) -> Result<String, ShellError> {
    let Some(rest) = label.strip_prefix(&format!("{prefix}-")) else {
        return Err(ShellError::Server);
    };
    let Some((timestamp, sequence)) = rest.rsplit_once('-') else {
        return Err(ShellError::Server);
    };
    let valid_digits =
        |value: &str| !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit());
    if timestamp.is_empty()
        || !timestamp.bytes().all(|byte| byte.is_ascii_digit())
        || sequence.len() < 6
        || !valid_digits(sequence)
        || !label
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    {
        return Err(ShellError::Server);
    }
    Ok(String::from(label))
}

fn project_cancel(receipt: &ExportCancelReceipt) -> Result<Value, ShellError> {
    Ok(json!({ "cancelled": receipt.cancelled }))
}

fn phase(phase: ExportPhase) -> &'static str {
    match phase {
        ExportPhase::WaitingForGate => "waiting_for_gate",
        ExportPhase::Exporting => "exporting",
        ExportPhase::Verifying => "verifying",
        ExportPhase::Done => "done",
        ExportPhase::Failed => "failed",
        ExportPhase::Cancelled => "cancelled",
    }
}

/// The typed terminal reasons, spelled EXACTLY as the server's own contract
/// documents them (snake_case) — `Debug`-lowercasing would fuse
/// `TimedOut` into "timedout" and `UnsupportedContent` into
/// "unsupportedcontent".
fn failure_reason(reason: ExportFailure) -> &'static str {
    match reason {
        ExportFailure::TimedOut => "timed_out",
        ExportFailure::UnsupportedContent => "unsupported_content",
        ExportFailure::Io => "io",
        ExportFailure::Verification => "verification",
        ExportFailure::Internal => "internal",
    }
}

/// The envelope database of a later interactive `status`/`cancel`: the
/// database of the job THIS session started wins over the profile's routing
/// database; with neither, the command is a usage error.
fn status_envelope<'a>(job: Option<&'a str>, routing: Option<&'a str>) -> Option<&'a str> {
    job.or(routing)
}

#[cfg(test)]
#[path = "../tests/commands/maintenance/jobs_field_tests.rs"]
mod jobs_field_tests;

//! Projections for the online-copy trio (`backup start|status|cancel`).

use serde_json::json;
use shamir_types::types::value::QueryValue;

use crate::shell::output::{self, ShellError};

/// Alias of the one batch op every online-copy command sends.
pub(in crate::shell) const RESULT_ALIAS: &str = "result";

/// Fixed note: cancel is run-scoped, not id-scoped.
pub(in crate::shell) const CURRENT_RUN_NOTE: &str =
    "cancel targets the current run on this node; the operation carries no id";

/// Fixed note: the destination is echoed exactly as given.
pub(in crate::shell) const DESTINATION_NOTE: &str = "the destination is the server-side absolute path exactly as given; the shell did not resolve, create or check it";

/// Fixed note: a server failure's text is never reproduced.
pub(in crate::shell) const FAILURE_NOTE: &str =
    "the server reported a failure; its message text is never reproduced";

/// Project `backup start`: a map with exactly `backup_id` and `destination`.
pub(in crate::shell) fn project_start(v: &QueryValue) -> Result<serde_json::Value, ShellError> {
    let QueryValue::Map(fields) = v else {
        return Err(ShellError::Server);
    };
    if fields.len() != 2 {
        return Err(ShellError::Server);
    }
    let backup_id = text(fields.get("backup_id").ok_or(ShellError::Server)?)?;
    let destination = text(fields.get("destination").ok_or(ShellError::Server)?)?;
    Ok(json!({
        "backup_id": output::sanitize(backup_id),
        "destination": output::sanitize(destination),
        "destination_note": DESTINATION_NOTE,
    }))
}

/// Project `backup status` through a strict allowlist; integer counters
/// render as decimal strings and a failure is reported only as a boolean.
pub(in crate::shell) fn project_status(v: &QueryValue) -> Result<serde_json::Value, ShellError> {
    let QueryValue::Map(fields) = v else {
        return Err(ShellError::Server);
    };
    for key in fields.keys() {
        if !matches!(
            key.as_str(),
            "backup_id"
                | "destination"
                | "dest_dir"
                | "started_at_unix_ns"
                | "running"
                | "cancel_requested"
                | "stage"
                | "progress"
                | "excluded_repos"
                | "repos_in_snapshot"
                | "repos_created_during"
                | "last_error"
                | "schedule"
        ) {
            return Err(ShellError::Server);
        }
    }
    let backup_id = text(fields.get("backup_id").ok_or(ShellError::Server)?)?;
    let destination = text(fields.get("destination").ok_or(ShellError::Server)?)?;
    let dest_dir = optional_text(fields.get("dest_dir"))?;
    let started_at = digits(fields.get("started_at_unix_ns").ok_or(ShellError::Server)?)?;
    let running = boolean(fields.get("running").ok_or(ShellError::Server)?)?;
    let cancel_requested = boolean(fields.get("cancel_requested").ok_or(ShellError::Server)?)?;
    let stage = text(fields.get("stage").ok_or(ShellError::Server)?)?;
    let failed_by_last_error = if fields.get("last_error").is_some() {
        text(fields.get("last_error").ok_or(ShellError::Server)?)?;
        true
    } else {
        false
    };

    let QueryValue::Map(progress) = fields.get("progress").ok_or(ShellError::Server)? else {
        return Err(ShellError::Server);
    };
    for key in progress.keys() {
        if !matches!(
            key.as_str(),
            "stage" | "started_at_unix_ns" | "failed_stage" | "per_repo" | "last_error"
        ) {
            return Err(ShellError::Server);
        }
    }
    let progress_stage = text(progress.get("stage").ok_or(ShellError::Server)?)?;
    let progress_started = digits(
        progress
            .get("started_at_unix_ns")
            .ok_or(ShellError::Server)?,
    )?;
    let failed_stage = optional_text(progress.get("failed_stage"))?;
    if progress.get("last_error").is_some() {
        text(progress.get("last_error").ok_or(ShellError::Server)?)?;
    }
    let failed = failed_by_last_error || failed_stage.is_some();

    let QueryValue::List(entries) = progress.get("per_repo").ok_or(ShellError::Server)? else {
        return Err(ShellError::Server);
    };
    let mut repos = Vec::with_capacity(entries.len());
    for entry in entries {
        let QueryValue::Map(entry_fields) = entry else {
            return Err(ShellError::Server);
        };
        if entry_fields.len() != 3 {
            return Err(ShellError::Server);
        }
        for key in entry_fields.keys() {
            if !matches!(key.as_str(), "repo" | "bytes_hashed" | "files_hashed") {
                return Err(ShellError::Server);
            }
        }
        repos.push(json!({
            "repo": output::sanitize(
                text(entry_fields.get("repo").ok_or(ShellError::Server)?)?,
            ),
            "bytes_hashed": decimal(
                entry_fields.get("bytes_hashed").ok_or(ShellError::Server)?,
            )?,
            "files_hashed": decimal(
                entry_fields.get("files_hashed").ok_or(ShellError::Server)?,
            )?,
        }));
    }

    let QueryValue::List(excluded_items) =
        fields.get("excluded_repos").ok_or(ShellError::Server)?
    else {
        return Err(ShellError::Server);
    };
    let mut excluded_repos = Vec::with_capacity(excluded_items.len());
    for item in excluded_items {
        excluded_repos.push(output::sanitize(text(item)?));
    }

    let mut payload = json!({
        "backup_id": if backup_id.is_empty() {
            None
        } else {
            Some(output::sanitize(backup_id))
        },
        "destination": output::sanitize(destination),
        "dest_dir": dest_dir,
        "started_at_unix_ns": started_at,
        "running": running,
        "cancel_requested": cancel_requested,
        "stage": output::sanitize(stage),
        "failed": failed,
        "failed_stage": failed_stage,
        "excluded_repos": excluded_repos,
        "repos_in_snapshot": decimal(
            fields.get("repos_in_snapshot").ok_or(ShellError::Server)?,
        )?,
        "repos_created_during": decimal(
            fields.get("repos_created_during").ok_or(ShellError::Server)?,
        )?,
        "repos": repos,
        "progress_stage": output::sanitize(progress_stage),
        "progress_started_at_unix_ns": progress_started,
        "current_run_note": CURRENT_RUN_NOTE,
    });
    if failed {
        payload
            .as_object_mut()
            .ok_or(ShellError::Server)?
            .insert(String::from("failure_note"), json!(FAILURE_NOTE));
    }
    Ok(payload)
}

/// Project `backup cancel`: a map with exactly `cancelled`.
pub(in crate::shell) fn project_cancel(v: &QueryValue) -> Result<serde_json::Value, ShellError> {
    let QueryValue::Map(fields) = v else {
        return Err(ShellError::Server);
    };
    if fields.len() != 1 {
        return Err(ShellError::Server);
    }
    let cancelled = boolean(fields.get("cancelled").ok_or(ShellError::Server)?)?;
    Ok(json!({
        "cancelled": cancelled,
        "current_run_note": CURRENT_RUN_NOTE,
    }))
}

fn text(item: &QueryValue) -> Result<&str, ShellError> {
    match item {
        QueryValue::Str(value) => Ok(value),
        _ => Err(ShellError::Server),
    }
}

fn optional_text(item: Option<&QueryValue>) -> Result<Option<String>, ShellError> {
    match item {
        Some(item) => Ok(Some(output::sanitize(text(item)?))),
        None => Ok(None),
    }
}

fn decimal(item: &QueryValue) -> Result<String, ShellError> {
    match item {
        QueryValue::Int(number) if *number >= 0 => Ok(number.to_string()),
        _ => Err(ShellError::Server),
    }
}

fn digits(item: &QueryValue) -> Result<&str, ShellError> {
    let QueryValue::Str(value) = item else {
        return Err(ShellError::Server);
    };
    if value.is_empty() || !value.bytes().all(|b| b.is_ascii_digit()) {
        return Err(ShellError::Server);
    }
    Ok(value)
}

fn boolean(item: &QueryValue) -> Result<bool, ShellError> {
    match item {
        QueryValue::Bool(value) => Ok(*value),
        _ => Err(ShellError::Server),
    }
}

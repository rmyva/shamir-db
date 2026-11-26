//! Admin-dispatch bridge (R2a step 3a, design D6): implements the engine's
//! `OnlineBackupAdminPort` over the [`OnlineBackupRegistry`], encoding each
//! answer as the response `QueryValue` record the admin executor wraps in
//! the standard single-record admin result.
//!
//! Field names mirror the snapshot's Rust field names (snake_case, the
//! established admin-op response convention — same style as the
//! application-schema status record). Non-i64 numbers (`u128` nanosecond
//! timestamps) are encoded as decimal strings; paths as strings. Live stage
//! names are the `Debug` names of `BackupStage`.

use std::path::Path;

use async_trait::async_trait;
use shamir_db::{OnlineBackupAdminError, OnlineBackupAdminPort};
use shamir_types::types::common::new_map;
use shamir_types::types::value::QueryValue;

use crate::sync::durability::backup::online::progress::BackupProgress;

use super::metrics::ScheduleStateSnapshot;
use super::registry::{BackupStatusSnapshot, OnlineBackupRegistry, StartBackupError};

#[async_trait]
impl OnlineBackupAdminPort for OnlineBackupRegistry {
    async fn start(&self, to: String) -> Result<QueryValue, OnlineBackupAdminError> {
        let (backup_id, destination) = self
            .start(Path::new(&to).to_path_buf())
            .map_err(map_start)?;
        let mut value = new_map();
        value.insert("backup_id".to_string(), QueryValue::Str(backup_id));
        value.insert(
            "destination".to_string(),
            QueryValue::Str(destination.display().to_string()),
        );
        Ok(QueryValue::Map(value))
    }

    async fn status(&self) -> Result<QueryValue, OnlineBackupAdminError> {
        let value = snapshot_value(&self.status());
        // The `schedule` section comes from THIS registry's own schedule
        // cell (item 3): a registry without a schedule reports no section,
        // even when a sibling registry in the same process has one.
        let mut value = match value {
            QueryValue::Map(value) => value,
            other => return Ok(other),
        };
        if let Some(schedule) = self.schedule_cell().snapshot() {
            value.insert("schedule".to_string(), schedule_section_value(&schedule));
        }
        Ok(QueryValue::Map(value))
    }

    async fn cancel(&self) -> Result<QueryValue, OnlineBackupAdminError> {
        let cancelled = self.cancel();
        let mut value = new_map();
        value.insert("cancelled".to_string(), QueryValue::Bool(cancelled));
        Ok(QueryValue::Map(value))
    }
}

/// Map the typed `start` failure to the engine port error.
fn map_start(error: StartBackupError) -> OnlineBackupAdminError {
    match error {
        StartBackupError::AlreadyRunning => OnlineBackupAdminError::AlreadyRunning,
        StartBackupError::InvalidDestination(reason) => {
            OnlineBackupAdminError::InvalidDestination(reason)
        }
        StartBackupError::Io(io) => OnlineBackupAdminError::Io(io.to_string()),
    }
}

/// Encode a snapshot as the response record (snake_case field names).
fn snapshot_value(snapshot: &BackupStatusSnapshot) -> QueryValue {
    let mut value = new_map();
    value.insert(
        "backup_id".to_string(),
        QueryValue::Str(snapshot.backup_id.clone()),
    );
    value.insert(
        "destination".to_string(),
        QueryValue::Str(snapshot.destination.display().to_string()),
    );
    if let Some(dest_dir) = &snapshot.dest_dir {
        value.insert(
            "dest_dir".to_string(),
            QueryValue::Str(dest_dir.display().to_string()),
        );
    }
    value.insert(
        "started_at_unix_ns".to_string(),
        QueryValue::Str(snapshot.started_at_unix_ns.to_string()),
    );
    value.insert("running".to_string(), QueryValue::Bool(snapshot.running));
    value.insert(
        "cancel_requested".to_string(),
        QueryValue::Bool(snapshot.cancel_requested),
    );
    value.insert(
        "stage".to_string(),
        QueryValue::Str(format!("{:?}", snapshot.stage)),
    );
    value.insert("progress".to_string(), progress_value(&snapshot.progress));
    let excluded: Vec<_> = snapshot
        .excluded_repos
        .iter()
        .map(|repo| QueryValue::Str(repo.clone()))
        .collect();
    value.insert("excluded_repos".to_string(), QueryValue::List(excluded));
    value.insert(
        "repos_in_snapshot".to_string(),
        QueryValue::Int(snapshot.repos_in_snapshot as i64),
    );
    value.insert(
        "repos_created_during".to_string(),
        QueryValue::Int(snapshot.repos_created_during as i64),
    );
    if let Some(last_error) = &snapshot.last_error {
        value.insert(
            "last_error".to_string(),
            QueryValue::Str(last_error.clone()),
        );
    }
    QueryValue::Map(value)
}

fn schedule_section_value(state: &ScheduleStateSnapshot) -> QueryValue {
    let mut value = new_map();
    value.insert("enabled".to_string(), QueryValue::Bool(true));
    value.insert(
        "interval_seconds".to_string(),
        QueryValue::Int(state.info.interval_secs as i64),
    );
    if let Some(window) = &state.info.window {
        value.insert("window".to_string(), QueryValue::Str(window.clone()));
    }
    value.insert(
        "keep_last".to_string(),
        QueryValue::Int(state.info.keep_last as i64),
    );
    value.insert(
        "run_on_start".to_string(),
        QueryValue::Bool(state.info.run_on_start),
    );
    if let Some(next) = state.next_run_unix_ns {
        value.insert(
            "next_run_unix_ns".to_string(),
            QueryValue::Str(next.to_string()),
        );
    }
    if let Some(run) = &state.last_run {
        let mut last_run = new_map();
        last_run.insert(
            "started_at_unix_ns".to_string(),
            QueryValue::Str(run.started_at_unix_ns.to_string()),
        );
        last_run.insert(
            "result".to_string(),
            QueryValue::Str(run.result.to_string()),
        );
        last_run.insert("stage".to_string(), QueryValue::Str(run.stage.clone()));
        if let Some(reason) = &run.reason {
            last_run.insert("reason".to_string(), QueryValue::Str(reason.clone()));
        }
        value.insert("last_run".to_string(), QueryValue::Map(last_run));
    }
    let mut runs = new_map();
    for (key, count) in [
        ("scheduled_started", state.scheduled_runs[0]),
        ("scheduled_skipped_busy", state.scheduled_runs[1]),
        ("scheduled_skipped_window", state.scheduled_runs[2]),
        ("scheduled_failed", state.scheduled_runs[3]),
        ("scheduled_completed", state.scheduled_runs[4]),
        ("failure_streak", state.scheduled_failure_streak),
        ("retention_deleted", state.retention_deleted_total),
        ("retention_failed", state.retention_failed_total),
    ] {
        runs.insert(key.to_string(), QueryValue::Int(count as i64));
    }
    value.insert("runs".to_string(), QueryValue::Map(runs));
    QueryValue::Map(value)
}

/// Encode the per-run progress (live stage, per-repo hashed counters).
fn progress_value(progress: &BackupProgress) -> QueryValue {
    let mut value = new_map();
    value.insert(
        "stage".to_string(),
        QueryValue::Str(format!("{:?}", progress.stage)),
    );
    if let Some(failed_stage) = progress.failed_stage {
        value.insert(
            "failed_stage".to_string(),
            QueryValue::Str(format!("{:?}", failed_stage)),
        );
    }
    value.insert(
        "started_at_unix_ns".to_string(),
        QueryValue::Str(progress.started_at_unix_ns.to_string()),
    );
    let per_repo: Vec<_> = progress
        .per_repo
        .iter()
        .map(|repo| {
            let mut entry = new_map();
            entry.insert("repo".to_string(), QueryValue::Str(repo.repo.clone()));
            entry.insert(
                "bytes_hashed".to_string(),
                QueryValue::Int(repo.bytes_hashed as i64),
            );
            entry.insert(
                "files_hashed".to_string(),
                QueryValue::Int(repo.files_hashed as i64),
            );
            QueryValue::Map(entry)
        })
        .collect();
    value.insert("per_repo".to_string(), QueryValue::List(per_repo));
    if let Some(last_error) = &progress.last_error {
        value.insert(
            "last_error".to_string(),
            QueryValue::Str(last_error.clone()),
        );
    }
    QueryValue::Map(value)
}

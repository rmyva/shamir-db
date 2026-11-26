//! The WAL export view and the repository durability diagnostic.

use serde_json::json;
use shamir_query_types::wire::db_message::OperatorStatus;

use crate::shell::connect::SessionClient;
use crate::shell::output::{self, ShellError};

/// Fixed note: the WAL numbers are the server's export.
pub(in crate::shell) const WAL_SOURCE_NOTE: &str =
    "the server's exported WAL telemetry; the shell never opens a WAL file";

/// Fixed note: `pending_versions` is a span, not a record count.
pub(in crate::shell) const WAL_BOUND_NOTE: &str =
    "pending_versions is a bound, not an exact count of unflushed records";

/// Fixed note: effective vs stored durability policy.
pub(in crate::shell) const DURABILITY_NOTE: &str = "effective values include the configured override; stored values are the catalogue's recorded policy; the shell reads no durability state from disk";

/// Project the WAL section of the operator status. The field names and
/// types mirror `shell/commands/state.rs::project_wal`; that function and
/// its helpers are private to `state.rs` and widening an existing item is
/// out of scope, so this module cannot call it.
pub(in crate::shell) fn project_wal(status: &OperatorStatus) -> serde_json::Value {
    let repositories = status
        .repositories
        .iter()
        .map(|repository| {
            json!({
                "db": output::sanitize(&repository.db),
                "repo": output::sanitize(&repository.repo),
                "wal": {
                    "backend_mem": repository.wal.backend_mem,
                    "watermark_w": repository.wal.watermark_w.to_string(),
                    "recoverable_through": repository.wal.recoverable_through.to_string(),
                    "pending_versions": repository.wal.pending_versions.to_string(),
                    "first_pending_version": repository
                        .wal
                        .first_pending_version
                        .map(|version| version.to_string()),
                    "first_pending_age_ns": repository
                        .wal
                        .first_pending_age_ns
                        .map(|age| age.to_string()),
                    "sync_now_calls": repository.wal.sync_now_calls.to_string(),
                    "sync_rounds": repository.wal.sync_rounds.to_string(),
                    "sync_coalesced": repository.wal.sync_coalesced.to_string(),
                    "sync_errors": repository.wal.sync_errors.to_string(),
                    "sync_duration_ns": repository.wal.sync_duration_ns.to_string(),
                },
            })
        })
        .collect::<Vec<_>>();
    json!({
        "source": WAL_SOURCE_NOTE,
        "caveat": WAL_BOUND_NOTE,
        "repositories": repositories,
    })
}

/// Describe one repository and attach the fixed durability caveat; a
/// non-object result is a protocol mismatch.
pub(in crate::shell) async fn durability(
    client: &SessionClient,
    db: &str,
    repo: &str,
) -> Result<serde_json::Value, ShellError> {
    let mut payload = crate::shell::commands::Catalogue::DescribeRepo {
        repo: repo.to_owned(),
    }
    .execute(client, db)
    .await?;
    payload
        .as_object_mut()
        .ok_or(ShellError::Server)?
        .insert(String::from("caveat"), json!(DURABILITY_NOTE));
    Ok(payload)
}

#[cfg(test)]
#[path = "../../tests/commands/maintenance/hostile_projection_tests.rs"]
mod hostile_projection_tests;

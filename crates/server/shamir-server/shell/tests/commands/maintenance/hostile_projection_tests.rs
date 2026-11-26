//! Unit tests for the peer-text sanitization of the maintenance
//! projections — the "malicious peer / reflection" guard.
//!
//! These are unit tests and not live probes on purpose: the shell harness
//! boots a REAL server, so every reply the projections receive comes from
//! a well-behaved server process and there is no seam to inject a hostile
//! reply into a running session. The strict allowlist projections are the
//! only place peer-supplied text can be echoed back to the operator, so
//! they are tested directly with hostile input: a canary string carrying
//! an ANSI escape (`ESC`), a bidi override (U+202E) and a line separator
//! (U+2028) must never reach the serialized output as raw characters, and
//! must never survive verbatim as a projected string.

use crate::shell::commands::maintenance::backup;
use serde_json::Value;
use shamir_collections::new_map;
use shamir_query_types::durability::DurabilityOverride;
use shamir_query_types::wire::db_message::{
    OperatorStatus, StatusRepository, StatusRole, StatusStampOutcome, StatusWal,
};
use shamir_types::types::value::QueryValue;

/// Canary carrying an ANSI escape, a bidi override and a line separator.
const HOSTILE: &str = "canary-7f3a\u{1b}[31m\u{202e}\u{2028}tail";

/// A `backup status` reply the `backup::project_status` allowlist accepts,
/// with `hostile` injected into every peer-supplied text field.
fn backup_status_value(hostile: &str) -> QueryValue {
    let mut per_repo = new_map();
    per_repo.insert(String::from("repo"), QueryValue::Str(hostile.to_owned()));
    per_repo.insert(String::from("bytes_hashed"), QueryValue::Int(0));
    per_repo.insert(String::from("files_hashed"), QueryValue::Int(0));

    let mut progress = new_map();
    progress.insert(String::from("stage"), QueryValue::Str(hostile.to_owned()));
    progress.insert(
        String::from("started_at_unix_ns"),
        QueryValue::Str(String::from("1700000000000000000")),
    );
    progress.insert(
        String::from("per_repo"),
        QueryValue::List(vec![QueryValue::Map(per_repo)]),
    );

    let mut fields = new_map();
    fields.insert(
        String::from("backup_id"),
        QueryValue::Str(String::from("1700000000000000000")),
    );
    fields.insert(
        String::from("destination"),
        QueryValue::Str(hostile.to_owned()),
    );
    fields.insert(
        String::from("started_at_unix_ns"),
        QueryValue::Str(String::from("1700000000000000000")),
    );
    fields.insert(String::from("running"), QueryValue::Bool(false));
    fields.insert(String::from("cancel_requested"), QueryValue::Bool(false));
    fields.insert(String::from("stage"), QueryValue::Str(hostile.to_owned()));
    fields.insert(String::from("progress"), QueryValue::Map(progress));
    fields.insert(
        String::from("excluded_repos"),
        QueryValue::List(vec![QueryValue::Str(hostile.to_owned())]),
    );
    fields.insert(String::from("repos_in_snapshot"), QueryValue::Int(0));
    fields.insert(String::from("repos_created_during"), QueryValue::Int(0));
    QueryValue::Map(fields)
}

/// A minimal `OperatorStatus` with `hostile` as the repository identity.
fn hostile_status(hostile: &str) -> OperatorStatus {
    OperatorStatus {
        server_version: String::from("0.0.0-test"),
        format_epoch: 0,
        stamp_outcome: StatusStampOutcome::Unchanged,
        uptime_ms: 0,
        role: StatusRole::Unknown,
        node_id: None,
        replication: Vec::new(),
        repositories: vec![StatusRepository {
            db: hostile.to_owned(),
            repo: hostile.to_owned(),
            effective: DurabilityOverride::default(),
            stored: None,
            wal: StatusWal {
                backend_mem: false,
                watermark_w: 3,
                recoverable_through: 1,
                pending_versions: 2,
                first_pending_version: None,
                first_pending_age_ns: None,
                sync_now_calls: 0,
                sync_rounds: 0,
                sync_coalesced: 0,
                sync_errors: 0,
                sync_duration_ns: 0,
            },
        }],
    }
}

/// Every string leaf of a projected JSON value, in document order.
fn string_leaves(value: &Value, out: &mut Vec<String>) {
    match value {
        Value::String(text) => out.push(text.clone()),
        Value::Array(items) => {
            for item in items {
                string_leaves(item, out);
            }
        }
        Value::Object(fields) => {
            for field in fields.values() {
                string_leaves(field, out);
            }
        }
        _ => {}
    }
}

/// The serialized payload must not carry the raw control/bidi/line
/// separator characters; the `\uXXXX` escape TEXT is allowed.
fn assert_no_raw_display_controls(serialized: &str) {
    for character in ['\u{1b}', '\u{202e}', '\u{2028}'] {
        assert!(
            !serialized.contains(character),
            "serialized projection leaked the raw character {character:?}"
        );
    }
}

/// No projected string may equal the hostile input verbatim: the
/// projection escaped it instead of echoing it.
fn assert_no_verbatim_string(payload: &Value, hostile: &str) {
    let mut leaves = Vec::new();
    string_leaves(payload, &mut leaves);
    assert!(
        leaves.iter().all(|leaf| leaf != hostile),
        "a projected string reflected the hostile input verbatim"
    );
}

/// `backup status` never reflects a hostile server value: the canary and
/// its 4096-char long variant survive only in escaped form.
#[test]
fn backup_status_never_reflects_a_hostile_server_value() {
    let payload = backup::project_status(&backup_status_value(HOSTILE))
        .expect("the hostile payload passes the backup status allowlist");
    let serialized = serde_json::to_string(&payload).expect("the projection serializes");
    assert_no_raw_display_controls(&serialized);
    assert_no_verbatim_string(&payload, HOSTILE);

    let long = format!("{HOSTILE}{}", "x".repeat(4096));
    let long_payload = backup::project_status(&backup_status_value(&long))
        .expect("the long hostile payload passes the backup status allowlist");
    let long_serialized = serde_json::to_string(&long_payload).expect("the projection serializes");
    assert_no_raw_display_controls(&long_serialized);
    assert!(
        long_serialized.contains(&"x".repeat(4096)),
        "the long variant must be projected intact"
    );
    assert_no_verbatim_string(&long_payload, &long);
}

/// The WAL diagnostic never reflects a hostile repository name.
#[test]
fn diag_wal_never_reflects_a_hostile_repository_name() {
    let payload = super::project_wal(&hostile_status(HOSTILE));
    let serialized = serde_json::to_string(&payload).expect("the projection serializes");
    assert_no_raw_display_controls(&serialized);
    assert_no_verbatim_string(&payload, HOSTILE);
}

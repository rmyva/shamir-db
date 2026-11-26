//! Integration tests for the administrative shell (R5 PR1–PR4).
//!
//! Declarations only: the harness owns shared fixtures, `output` covers the
//! output contract, and `transport`, `secure_transports`, and `refusals` cover
//! the live transport and refusal scenarios.
//!
//! `core/` holds the harness and output-contract files as ONE folder entry;
//! `#[path]` keeps their Rust module names (and every `super::` path inside
//! those files) exactly as before the move.

// `core/` groups the harness and the output contract; `#[path]` keeps the
// module names `harness` and `output` unchanged, so no inner `super::` path
// moved. Do NOT add `core/mod.rs`: these are plain file declarations only.
#[path = "core/harness.rs"]
mod harness;
#[path = "core/interactive.rs"]
mod interactive;
#[path = "core/output.rs"]
mod output;
#[path = "core/runtime.rs"]
mod runtime;
// `secure/` holds the transport-policy refusal scenarios; `#[path]` keeps
// the Rust module tree (and every `super::` path inside the file) exactly
// as before the move. The secure-listener fixture moved to the same folder
// but stays anchored at `harness.rs`, which owned it before the move.
#[path = "secure/refusals.rs"]
mod refusals;
mod secure_transports;
// R5 PR3: the state and narrow data views (`status`, `version show`,
// `replication subscriptions`, `data count` / `head` / `get`). The topic
// folder is ONE entry; `#[path]` keeps every inner `super::` path anchored
// at this module, exactly like the `secure/` move. Shared helpers live in
// `state_data_common`; one primary item per sibling file.
#[path = "state_data/common.rs"]
mod state_data_common;
#[path = "state_data/goldens.rs"]
mod state_data_goldens;
#[path = "state_data/subscriptions.rs"]
mod state_data_subscriptions;
#[path = "state_data/token_subset.rs"]
mod state_data_token_subset;
// R5 PR4: the rights views and the rights mutations (`user list`,
// `group tree`, `group members`, `rights show`, `chmod`, `chown`,
// `chgrp`) plus the activity views and cancellation. The topic folder is
// ONE entry, the same `#[path]` convention as `state_data/`; shared
// helpers are reused from `state_data_common`.
#[path = "access_activity/access.rs"]
mod access_activity_access;
#[path = "access_activity/activity.rs"]
mod access_activity_activity;
#[path = "access_activity/holders/mod.rs"]
mod access_activity_holders;
#[path = "access_activity/mutations.rs"]
mod access_activity_mutations;
#[path = "access_activity/rights_gaps.rs"]
mod access_activity_rights_gaps;
// R5 tail: a genuinely parked request cancelled through the real shell.
// Same one-entry `#[path]` convention as the sibling access/activity files.
#[path = "access_activity/parked_cancel.rs"]
mod access_activity_parked_cancel;
#[path = "access_activity/raw_scram.rs"]
mod access_activity_raw_scram;
#[path = "maintenance/backup.rs"]
mod maintenance_backup;
#[path = "maintenance/explain/diag.rs"]
mod maintenance_diag;
#[path = "maintenance/explain/explain.rs"]
mod maintenance_explain;
#[path = "maintenance/export_online.rs"]
mod maintenance_export_online;
#[path = "maintenance/migrations.rs"]
mod maintenance_migrations;
#[path = "maintenance/policy.rs"]
mod maintenance_policy;
#[path = "maintenance/verify.rs"]
mod maintenance_verify;
#[path = "maintenance/versions.rs"]
mod maintenance_versions;
#[path = "core/transport.rs"]
mod transport;

use harness::secure_fixture;

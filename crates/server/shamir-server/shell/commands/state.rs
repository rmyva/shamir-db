//! State and narrow data view commands (design `admin-shell.md` §4.2, §4.4,
//! §9 PR3): `state status`, `version show`, `replication subscriptions`,
//! `data count`, `data head`, `data get`.
//!
//! Mapping evidence (all OBSERVED on this branch):
//! - `state status` / `version show` → the typed top-level
//!   `DbRequest::Status` (wire `db_message.rs`) through the SDK's
//!   `Client::status` (`client/api/admin.rs`); the reply is
//!   `DbResponse::Status { status: OperatorStatus }`. The observed format
//!   epoch and stamp outcome were captured by the server LAUNCHER at boot;
//!   the shell never reads the data directory.
//! - `replication subscriptions` → the EXISTING `ReplicationStatus` admin op
//!   through the existing batch path (no wire change), built with
//!   `shamir_query_builder::ddl::replication::replication_status()`.
//! - `data count` → `Query::with_repo(..).select([count_all])`, limit only,
//!   no ORDER BY, through the batch path (`BatchOp::Read`).
//! - `data head` → `Query::with_repo(..).limit(min(rows, 100))`, no ORDER BY
//!   (no ordering promise), through the batch path; shell-side caps:
//!   100 rows, 256 KiB of projected row bytes, and a 5 s MONOTONIC
//!   whole-operation budget (request + projection) sharing one deadline
//!   `ShellError::HeadWallTime`, exit 7, outcome unknown).
//! - `data get` → the typed top-level `DbRequest::GetRecord` through the
//!   SDK's `Client::get_record` (`client/api/request.rs`); NOT a batch op
//!   and NOT a cursor. The KEY is a Base58 `RecordId`, validated HERE
//!   (before any connection) with the fixed typed usage refusal.
//!
//! Errors: server failures route through `crate::shell::connect::request_error`
//! (allowlisted stable codes only, never a verbatim server string). A typed
//! `not_found` from the P2 point read maps to `ShellError::NotFound`.
//! Every authorized string is sanitized through `output::value` /
//! `output::sanitize`; peer-supplied text is never echoed raw.

use std::time::Duration;

use serde_json::{json, Value};
use shamir_query_builder::batch::Batch;
use shamir_query_builder::ddl::replication::replication_status;
use shamir_query_builder::select::count_all;
use shamir_query_builder::Query;
use shamir_query_types::batch::BatchResponse;
use shamir_query_types::read::{QueryRecord, QueryResult};
use shamir_query_types::wire::db_message::{
    OperatorStatus, StatusRepository, StatusRole, StatusStampOutcome, StatusWal,
};
use shamir_types::types::record_id::RecordId;
use shamir_types::types::value::QueryValue;

use super::super::output::{self, value, ShellError};
use crate::shell::connect::{request_error, SessionClient};

/// `data head` row cap (design §4.4): at most 100 rows are requested and
/// projected; a larger `rows` argument is clamped and the clamp is visible
/// in the output (`rows_requested`).
const HEAD_MAX_ROWS: u64 = 100;

/// `data head` projected-row byte cap: once the serialized rows exceed
/// this, no further rows are added and the bytes marker is set.
const HEAD_MAX_ROW_BYTES: usize = 256 * 1024;

/// `data head` wall-clock budget covering the WHOLE operation — the request
/// wait AND the local projection loop — as ONE monotonic deadline: expiry
/// is the typed `head_wall_time` outcome-unknown refusal (exit 7), never a
/// partial result. The request phase uses `tokio::time::timeout_at`; the
/// projection loop compares the same deadline INSIDE the loop (a `tokio`
/// timeout cannot preempt a ready CPU-bound projection loop), before and
/// after the last row.
const HEAD_WALL: Duration = Duration::from_secs(5);

/// `data get` KEY cap in bytes before decoding: a Base58 `RecordId` is
/// 16–22 characters (22 = ceil(log_58(2^128)), the canonical full-length
/// form; leading zero bytes shorten it); anything longer is refused without
/// parsing — the decoder under `RecordId::from_str` has no internal length
/// bound, so the shell caps first. The server applies its own typed-key cap
/// as well.
const GET_KEY_MAX_BYTES: usize = 22;

/// Response alias for the single-entry batch (`data count`, `data head`,
/// `replication subscriptions`).
const RESULT_ALIAS: &str = "result";

/// Fixed merged-gap caveat (design §4.2): the shell NEVER uses the word
/// "lag"; the field is named and its non-authoritative nature stated.
const GAP_CAVEAT: &str = "gap_versions_merged is a merged, non-authoritative \
aggregate per repository; age is always unknown; this is not a measurement \
against a specific leader";

/// Fixed count caveat, shown ONLY when the statistics do not distinguish
/// the path (`index_used` absent): the ground truth is the engine dispatch
/// marker `index_used == Some("__record_counter__")` (O(1) counter);
/// any other `index_used` is a scan.
const COUNT_CAVEAT: &str = "the server statistics do not report whether the \
O(1) counter path or a scan produced this count";

/// Fixed no-ordering caveat for `data head` (design §4.4).
const HEAD_CAVEAT: &str = "rows are in server scan order; no ordering is \
promised and none may be assumed";

/// Fixed age caveat (design §6): the age field is always null; the shell
/// NEVER uses the word "lag" for it.
const AGE_CAVEAT: &str = "age is not sampled; zero can mean not sampled";

/// Allowlisted fields of one `replication subscriptions` entry (design
/// §4.2: identities, state, local consistent version, bookmarks, gap
/// endpoints). Anything else the peer emits — including raw
/// `status_message`, `last_terminal_reason` and source path fields — is
/// dropped, never projected raw.
const SUBSCRIPTION_FIELDS: &[&str] = &[
    "db",
    "repo",
    "name",
    "state",
    "installation_state",
    "recorded_identity",
    "observed_identity",
    "upstream_bookmark",
    "next_pull_version",
    "local_consistent_version",
    "certified_gap_first",
    "certified_gap_through",
    "artifact_digest",
    "source_lineage_id",
    "source_history_epoch",
    "seed_source_version",
    "local_baseline_version",
];

/// Width of a lineage id on the admin surface: `RepoIdentity::lineage_id`
/// is `[u8; 16]` (`shamir-engine-repo` `meta/repo_identity.rs:36`), which
/// serializes as a 16-element msgpack array of non-negative integers
/// (`execute/core/helpers.rs::to_qv`).
const LINEAGE_ID_BYTES: usize = 16;

/// R5 step 3 state and narrow data view commands.
#[derive(Debug, Clone)]
pub(in crate::shell) enum StateCommand {
    /// `state status` — the P1 operator state snapshot.
    Status,
    /// `version show` — the P1 view: observed epoch and stamp outcome.
    VersionShow,
    /// `replication subscriptions` — the existing `ReplicationStatus` op.
    ReplicationSubscriptions,
    /// `data count DB REPO TABLE`.
    Count {
        db: String,
        repo: String,
        table: String,
    },
    /// `data head DB REPO TABLE N` — first N rows in server scan order.
    Head {
        db: String,
        repo: String,
        table: String,
        rows: u64,
    },
    /// `data get DB REPO TABLE KEY` — the P2 exact-key point read; KEY is
    /// Base58, validated before any connection.
    Get {
        db: String,
        repo: String,
        table: String,
        key: String,
    },
}

impl StateCommand {
    /// Wire operation label (JSON v1 `op` field, §5). The stable command ID
    /// is supplied by the dispatcher (`parser::grammar::command_id`).
    pub(in crate::shell) fn op(&self) -> &'static str {
        match self {
            StateCommand::Status | StateCommand::VersionShow => "Status",
            StateCommand::ReplicationSubscriptions => "ReplicationStatus",
            StateCommand::Count { .. } | StateCommand::Head { .. } => "Read",
            StateCommand::Get { .. } => "GetRecord",
        }
    }

    /// Whether the command needs the profile's routing database floor
    /// (`data …` names its database explicitly; `status`/`version show`
    /// are server-wide; `replication subscriptions` runs through the batch
    /// path and carries the routing database floor, §4.2).
    pub(in crate::shell) fn requires_routing_db(&self) -> bool {
        matches!(self, StateCommand::ReplicationSubscriptions)
    }

    /// Pre-connection validation with the fixed typed refusal policy:
    /// a malformed Base58 `data get` KEY (or an over-long one) and a zero
    /// `data head` row count are local usage errors refused BEFORE any
    /// dial or request.
    pub(in crate::shell) fn validate(&self) -> Result<(), ShellError> {
        match self {
            StateCommand::Head { rows, .. } => {
                if *rows == 0 {
                    return Err(ShellError::Usage);
                }
                Ok(())
            }
            StateCommand::Get { key, .. } => {
                if key.is_empty() || key.len() > GET_KEY_MAX_BYTES {
                    return Err(ShellError::InvalidRecordId);
                }
                // Base58 `RecordId` decode: a malformed key is the typed
                // `invalid_record_id` refusal (exit 1), never a panic and
                // never a scan.
                key.parse::<RecordId>()
                    .map(|_| ())
                    .map_err(|_| ShellError::InvalidRecordId)
            }
            _ => Ok(()),
        }
    }

    /// Execute the command. `routing_db` is the profile database floor and
    /// is required only when [`requires_routing_db`](Self::requires_routing_db)
    /// is true. Authorization happens exclusively on the normal remote
    /// route; this code sends no privilege material.
    /// cancel-safe: NO — a sent request may still execute; do not retry on
    /// timeout.
    pub(in crate::shell) async fn execute(
        &self,
        client: &SessionClient,
        routing_db: Option<&str>,
    ) -> Result<Value, ShellError> {
        match self {
            StateCommand::Status => {
                let snapshot = client.status().await?;
                let payload = project_status(&snapshot);
                client.check_catalogue_output(&payload)?;
                Ok(payload)
            }
            StateCommand::VersionShow => {
                let snapshot = client.status().await?;
                let payload = project_version(&snapshot);
                client.check_catalogue_output(&payload)?;
                Ok(payload)
            }
            StateCommand::ReplicationSubscriptions => {
                let db = routing_db.ok_or(ShellError::Usage)?;
                let payload = self.run_batch(client, db, replication_status()).await?;
                client.check_catalogue_output(&payload)?;
                Ok(payload)
            }
            StateCommand::Count { db, repo, table } => {
                let query = Query::with_repo(repo.clone(), table.clone())
                    .select([count_all(RESULT_ALIAS)])
                    .build();
                let result = self.run_read(client, db, query).await?;
                let payload = project_count(&result)?;
                client.check_catalogue_output(&payload)?;
                Ok(payload)
            }
            StateCommand::Head {
                db,
                repo,
                table,
                rows,
            } => {
                let requested = *rows;
                // Shell-side cap: never request more than 100 rows.
                let limit = requested.min(HEAD_MAX_ROWS);
                let query = Query::with_repo(repo.clone(), table.clone())
                    .limit(limit)
                    .build();
                // One monotonic deadline covers request + projection: the
                // read is bounded by `timeout_at` (elapsed maps to the typed
                // `head_wall_time`), and the SAME deadline bounds the local
                // CPU work in `project_head`.
                let deadline = tokio::time::Instant::now() + HEAD_WALL;
                let result = tokio::time::timeout_at(deadline, self.run_read(client, db, query))
                    .await
                    .map_err(|_| ShellError::HeadWallTime)?;
                let result = result?;
                // The credential-reflection scan is part of the SAME
                // head deadline (projection + guard, one budget, no
                // retry — a sent request is never resent on expiry). The
                // deadline is consulted BEFORE the projection result is
                // unwrapped, and it is re-checked AFTER the credential
                // scan: an already-elapsed budget is the typed
                // `head_wall_time` refusal even when a projection error
                // is what the projection returned, and even when the
                // scan itself came back clean (exit 0) or refused
                // (exit 5).
                let projected = project_head(&result, requested, limit, deadline);
                Ok(head_tail(deadline, projected, |payload| {
                    client.check_catalogue_output(payload)
                })?)
            }
            StateCommand::Get {
                db,
                repo,
                table,
                key,
            } => {
                // validate() already parsed this key; the parse is repeated
                // here because `key` is owned text and validate borrows.
                let id: RecordId = key.parse().map_err(|_| ShellError::InvalidRecordId)?;
                let record = client.get_record(db, repo, table, id).await?;
                let payload = match record {
                    QueryRecord::Direct(qv) => value(&qv)?,
                    _ => return Err(ShellError::Server),
                };
                client.check_catalogue_output(&payload)?;
                Ok(payload)
            }
        }
    }

    /// Send one single-op batch (`replication subscriptions`) on the routing
    /// database and project the single direct result record through the
    /// STRICT allowlisted subscription shape.
    async fn run_batch(
        &self,
        client: &SessionClient,
        db: &str,
        op: shamir_query_types::batch::BatchOp,
    ) -> Result<Value, ShellError> {
        let mut batch = Batch::new();
        batch.insert(RESULT_ALIAS, op);
        // Client-side validation failure is a local usage problem.
        let request = batch.try_build().map_err(|_| ShellError::Usage)?;
        let response = client.execute(db, request).await.map_err(request_error)?;
        let result = take_result(response)?;
        project_subscriptions(&result)
    }

    /// Send one single-query batch and return the UNPROJECTED
    /// [`QueryResult`] (records stay typed `QueryValue`s; projection is the
    /// caller's bounded job).
    async fn run_read(
        &self,
        client: &SessionClient,
        db: &str,
        query: shamir_query_types::read::ReadQuery,
    ) -> Result<QueryResult, ShellError> {
        let mut batch = Batch::new();
        batch.query(RESULT_ALIAS, query);
        let request = batch.try_build().map_err(|_| ShellError::Usage)?;
        let response = client.execute(db, request).await.map_err(request_error)?;
        take_result(response)
    }
}

/// Extract the one-alias batch result as an owned, unprojected
/// [`QueryResult`]. A missing or skipped alias is a protocol mismatch,
/// reported as `Server` without echoing peer-supplied detail.
fn take_result(response: BatchResponse) -> Result<QueryResult, ShellError> {
    let mut results = response.results;
    let result = results.get_mut(RESULT_ALIAS).ok_or(ShellError::Server)?;
    if result.skipped {
        return Err(ShellError::Server);
    }
    Ok(std::mem::take(result))
}

/// The single direct record of a one-alias batch, as a typed `QueryValue`.
fn single_direct_value(result: &QueryResult) -> Result<&QueryValue, ShellError> {
    let [record] = result.records.as_slice() else {
        return Err(ShellError::Server);
    };
    match record {
        QueryRecord::Direct(qv) => Ok(qv),
        _ => Err(ShellError::Server),
    }
}

/// STRICT projection of `replication subscriptions`: the result must be
/// exactly one direct map with exactly the `subscriptions` list; each entry
/// must be a map and is re-projected through the typed per-field shape
/// check with ONLY the [`SUBSCRIPTION_FIELDS`] allowlist. No raw server
/// `QueryValue` map projection: unknown peer fields (raw `status_message`,
/// terminal reasons, source paths) are dropped, and a value whose SHAPE
/// does not match the engine's own record is the typed `Server` refusal
/// rather than data — the offending value is never echoed.
fn project_subscriptions(result: &QueryResult) -> Result<Value, ShellError> {
    let qv = single_direct_value(result)?;
    let QueryValue::Map(fields) = qv else {
        return Err(ShellError::Server);
    };
    if fields.len() != 1 {
        return Err(ShellError::Server);
    }
    let QueryValue::List(entries) = fields.get("subscriptions").ok_or(ShellError::Server)? else {
        return Err(ShellError::Server);
    };
    let mut projected = Vec::with_capacity(entries.len());
    for entry in entries {
        let QueryValue::Map(entry_fields) = entry else {
            return Err(ShellError::Server);
        };
        let mut item = serde_json::Map::new();
        for field in SUBSCRIPTION_FIELDS {
            if let Some(element) = entry_fields.get(*field) {
                item.insert((*field).to_string(), subscription_field(field, element)?);
            }
        }
        projected.push(Value::Object(item));
    }
    Ok(json!({ "subscriptions": projected }))
}

/// ONE table keyed on the allowlisted field name: the typed shape of every
/// projected subscription value. The shapes are the engine's own record
/// (`shamir-db` `execute/replication_seed/status.rs`): `recorded_identity`
/// / `observed_identity` are `Option<RepoIdentity>`, the bookmark / version
/// / gap fields are `Option<u64>`, `source_lineage_id` is an optional
/// `[u8; 16]`, and the name / state fields are strings (see each helper).
/// A name that is in [`SUBSCRIPTION_FIELDS`] but has no arm here is a
/// deliberate decision point: it is refused instead of silently rendered.
fn subscription_field(field: &str, element: &QueryValue) -> Result<Value, ShellError> {
    match field {
        "recorded_identity" => identity_field(element),
        "observed_identity" => identity_field(element),
        "source_lineage_id" => lineage_field(element),
        "upstream_bookmark" => version_field(element),
        "next_pull_version" => version_field(element),
        "local_consistent_version" => version_field(element),
        "certified_gap_first" => version_field(element),
        "certified_gap_through" => version_field(element),
        "seed_source_version" => version_field(element),
        "local_baseline_version" => version_field(element),
        "source_history_epoch" => version_field(element),
        "db" | "repo" | "name" | "state" | "installation_state" => text_field(element),
        // Observed in `replication_seed/status.rs:85-96`: the engine seeds
        // `artifact_digest` as `Null` and replaces it with a string ONLY
        // when a seed-install receipt exists, so both shapes are real.
        "artifact_digest" => optional_text_field(element),
        _ => Err(ShellError::Server),
    }
}

/// `Option<RepoIdentity>` → `Null`, or a map with EXACTLY `lineage_id`
/// (16 non-negative integers) and `history_epoch` (PRESENT and a
/// non-negative version leaf — the engine always emits a number there,
/// never `Null`). Extra nested fields are a shape mismatch, not data.
fn identity_field(element: &QueryValue) -> Result<Value, ShellError> {
    match element {
        QueryValue::Null => Ok(Value::Null),
        QueryValue::Map(fields) => {
            let Some(lineage) = fields.get("lineage_id") else {
                return Err(ShellError::Server);
            };
            let Some(epoch) = fields.get("history_epoch") else {
                return Err(ShellError::Server);
            };
            if fields.len() != 2
                || !is_optional_version(epoch)
                || matches!(epoch, QueryValue::Null)
                || !is_lineage_id(lineage)
            {
                return Err(ShellError::Server);
            }
            render_field(element)
        }
        _ => Err(ShellError::Server),
    }
}

/// `Option<[u8; 16]>` → `Null`, or exactly 16 integer bytes
/// (`0..=255`).
fn lineage_field(element: &QueryValue) -> Result<Value, ShellError> {
    match element {
        QueryValue::Null => Ok(Value::Null),
        _ if is_lineage_id(element) => render_field(element),
        _ => Err(ShellError::Server),
    }
}

/// One version / epoch / bookmark leaf as the batch wire delivers it, for
/// a value the engine types as `u64`:
/// - `Null` (absent);
/// - a non-negative `Int` — the engine emits a small non-negative integer
///   as `Int`;
/// - a `QueryValue::Big` (the in-process engine shape) whose value is a
///   real `u64` (`0..=u64::MAX`), since a version is a `u64` and a larger
///   `Big` can never become one;
/// - canonical decimal text strictly above `i64::MAX` — the encoder emits
///   exactly that text for a `u64` above `i64::MAX`, while anything that
///   fits in an `i64` is emitted as `Int`.
///
/// Arbitrary text is NOT that shape: `"abc"`, `"7"`, `"007"`, `"+1"`,
/// `" 7"`, `"7 "`, `"-1"` and anything above `u64::MAX` are refused.
fn is_optional_version(element: &QueryValue) -> bool {
    match element {
        QueryValue::Null | QueryValue::Int(0..) => true,
        QueryValue::Big(value) => u64::try_from(value).is_ok(),
        QueryValue::Str(text) => text
            .parse::<u64>()
            .is_ok_and(|parsed| parsed > i64::MAX as u64 && parsed.to_string() == *text),
        _ => false,
    }
}

/// `Option<u64>` version / bookmark / gap endpoint → `Null` or a
/// non-negative version leaf ([`is_optional_version`]).
fn version_field(element: &QueryValue) -> Result<Value, ShellError> {
    if is_optional_version(element) {
        render_field(element)
    } else {
        Err(ShellError::Server)
    }
}

fn text_field(element: &QueryValue) -> Result<Value, ShellError> {
    match element {
        QueryValue::Str(_) => render_field(element),
        _ => Err(ShellError::Server),
    }
}

fn optional_text_field(element: &QueryValue) -> Result<Value, ShellError> {
    match element {
        QueryValue::Null | QueryValue::Str(_) => render_field(element),
        _ => Err(ShellError::Server),
    }
}

/// A lineage id is a list of exactly [`LINEAGE_ID_BYTES`] integers, each a
/// real byte (`0..=255`): a `[u8; 16]` through the engine's `to_qv` can
/// never carry `256`, so such a leaf is a shape mismatch.
fn is_lineage_id(element: &QueryValue) -> bool {
    match element {
        QueryValue::List(items) => {
            items.len() == LINEAGE_ID_BYTES
                && items
                    .iter()
                    .all(|item| matches!(item, QueryValue::Int(0..=255)))
        }
        _ => false,
    }
}

/// Render an already shape-checked value through the v1 conventions
/// (decimal strings for integers, sanitized strings); a projection refusal
/// here can only be a depth overrun, reported as the typed `Server` shape
/// refusal.
fn render_field(element: &QueryValue) -> Result<Value, ShellError> {
    value(element).map_err(|_| ShellError::Server)
}

/// Project the `data head` output under the byte budget: rows are held
/// UNPROJECTED in the [`QueryResult`] and projected one at a time; the
/// first row that would overflow the serialized-bytes cap stops the
/// projection and sets `bytes_truncated`. Byte accumulation uses checked
/// arithmetic and exact separators (one comma between rows, the
/// surrounding brackets). `rows_truncated` is marked ONLY when more rows
/// existed beyond the returned window AND the request exceeded the hard
/// row cap; `rows_capped` independently records pure request clamping.
/// The `more exist` signal is the typed pagination metadata of the actual
/// paged read route (`pagination.has_next`), never a
/// `records.len() == limit` inference. The rows are constrained to the
/// requested limit with `.take(limit)` so a dishonest peer's extra rows
/// are dropped, not projected. The shared monotonic deadline is checked
/// between rows and around them — INSIDE the loop, before and after the
/// last row (the tail CPU work); expiry is the typed `head_wall_time`
/// refusal (exit 7), never a partial success. A single row is NOT
/// interruptible: it is projected in full first, so one row's projection
/// cost is bounded only by the server's result cap — this is NOT a hard
/// wall-time bound on one row. An already-elapsed deadline wins over any
/// per-row projection refusal ([`head_error`]).
fn project_head(
    result: &QueryResult,
    requested: u64,
    limit: u64,
    deadline: tokio::time::Instant,
) -> Result<Value, ShellError> {
    let mut rows: Vec<Value> = Vec::new();
    // Exact serialization size: `[` + rows joined by `,` + `]`.
    let mut bytes = 2usize;
    let mut bytes_truncated = false;
    let max_rows = usize::try_from(limit).map_err(|_| ShellError::Server)?;
    for record in result.records.iter().take(max_rows) {
        if tokio::time::Instant::now() >= deadline {
            return Err(ShellError::HeadWallTime);
        }
        let QueryRecord::Direct(qv) = record else {
            return Err(head_error(deadline, ShellError::Server));
        };
        let rendered = value(qv).map_err(|error| head_error(deadline, error))?;
        let width = serde_json::to_string(&rendered)
            .map(|text| text.len())
            .map_err(|_| head_error(deadline, ShellError::ProjectionLimit))?;
        let separator: usize = if rows.is_empty() { 0 } else { 1 };
        let needed = separator
            .checked_add(width)
            .and_then(|needed| bytes.checked_add(needed))
            .ok_or_else(|| head_error(deadline, ShellError::ProjectionLimit))?;
        if needed > HEAD_MAX_ROW_BYTES {
            bytes_truncated = true;
            break;
        }
        bytes = needed;
        rows.push(rendered);
    }
    // The tail CPU work after the last row is also inside the budget.
    if tokio::time::Instant::now() >= deadline {
        return Err(ShellError::HeadWallTime);
    }
    // `more exist`: the typed pagination metadata of the paged read route —
    // never inferred from the returned row count (a full page does not
    // prove more rows, and extra peer rows are dropped by `.take` above).
    let more_exist = result.pagination.as_ref().is_some_and(|p| p.has_next);
    let rows_truncated = more_exist && requested > HEAD_MAX_ROWS;
    let mut out = json!({
        "rows": rows,
        "rows_requested": requested.to_string(),
        "rows_returned": rows.len().to_string(),
        "rows_truncated": rows_truncated,
        "bytes_truncated": bytes_truncated,
        "caveat": HEAD_CAVEAT,
    });
    if requested > HEAD_MAX_ROWS {
        if let Some(obj) = out.as_object_mut() {
            obj.insert("rows_capped".into(), json!(true));
        }
    }
    Ok(out)
}

/// A projection refusal on the shared `data head` deadline keeps the
/// deadline's typed outcome: an already-elapsed budget wins over any
/// per-row projection error (exit 7 `head_wall_time`, never a misleading
/// exit 5). A single row is NOT interruptible — the budget is checked
/// between rows, so one row's projection cost is bounded only by the
/// server's result cap.
fn head_error(deadline: tokio::time::Instant, error: ShellError) -> ShellError {
    if tokio::time::Instant::now() >= deadline {
        ShellError::HeadWallTime
    } else {
        error
    }
}

/// The `data head` tail after the response arrived: project the rows under
/// the shared deadline, then run the SYNCHRONOUS credential-reflection scan
/// and consult the deadline ONE LAST TIME afterwards — an elapsed budget is
/// the typed `head_wall_time` refusal (exit 7) even when the scan came back
/// clean (which would otherwise exit 0) or refused (which would otherwise
/// exit 5). The scan itself is not interruptible: this is ordering, not
/// preemption.
fn head_tail(
    deadline: tokio::time::Instant,
    projected: Result<Value, ShellError>,
    scan: impl FnOnce(&Value) -> Result<(), ShellError>,
) -> Result<Value, ShellError> {
    head_tail_with_now(deadline, projected, scan, &tokio::time::Instant::now)
}

/// [`head_tail`] with its clock injected: every deadline consultation reads
/// `now()` instead of the ambient clock, so a test can drive the ordering
/// (before vs. after the scan) without sleeping. That covers BOTH
/// consultations: the projection refusal and the post-scan recheck.
pub(super) fn head_tail_with_now<R>(
    deadline: tokio::time::Instant,
    payload: Result<R, ShellError>,
    scan: impl FnOnce(&R) -> Result<(), ShellError>,
    now: &dyn Fn() -> tokio::time::Instant,
) -> Result<R, ShellError> {
    // A projection refusal keeps the deadline's typed outcome when the budget
    // has elapsed by the time the tail runs (a runner descheduled between the
    // row error and here must still report exit 7, not exit 5). This reads the
    // injected clock, like the post-scan recheck below.
    let payload = match payload {
        Ok(value) => value,
        Err(error) => {
            if now() >= deadline {
                return Err(ShellError::HeadWallTime);
            }
            return Err(error);
        }
    };
    let scanned = scan(&payload);
    if now() >= deadline {
        return Err(ShellError::HeadWallTime);
    }
    scanned?;
    Ok(payload)
}

/// Project the `data count` result. The count row is exactly one direct
/// record: a map with EXACTLY the named alias `result` carrying a
/// nonnegative integer, rendered as a decimal string. The engine dispatch
/// ground truth: `QueryStats` present with
/// `index_used == Some("__record_counter__")` is the O(1) counter path;
/// statistics present with `records_scanned > 0` is a scan (the plain
/// `CountAll` path); anything else stays `unknown` and carries the fixed
/// caveat — the shell never invents a scan marker.
fn project_count(result: &QueryResult) -> Result<Value, ShellError> {
    let qv = single_direct_value(result)?;
    let QueryValue::Map(fields) = qv else {
        return Err(ShellError::Server);
    };
    if fields.len() != 1 {
        return Err(ShellError::Server);
    }
    let QueryValue::Int(count) = fields.get(RESULT_ALIAS).ok_or(ShellError::Server)? else {
        return Err(ShellError::Server);
    };
    if *count < 0 {
        return Err(ShellError::Server);
    }
    let count_path = match &result.stats {
        Some(stats) if stats.index_used.as_deref() == Some("__record_counter__") => "counter",
        Some(stats) if stats.records_scanned > 0 => "scan",
        _ => "unknown",
    };
    let mut out = json!({
        "count": count.to_string(),
        "count_path": count_path,
    });
    if count_path == "unknown" {
        if let Some(obj) = out.as_object_mut() {
            obj.insert("caveat".into(), json!(COUNT_CAVEAT));
        }
    }
    Ok(out)
}

/// Project the P1 snapshot with sanitized strings; EVERY u64 counter,
/// epoch, uptime and age renders as a decimal string (design §5 payload
/// conventions — no large-number fidelity loss through JSON floats).
fn project_status(snapshot: &OperatorStatus) -> Value {
    json!({
        "server_version": output::sanitize(&snapshot.server_version),
        "format_epoch": snapshot.format_epoch.to_string(),
        "stamp_outcome": stamp_outcome(snapshot.stamp_outcome),
        "uptime_ms": snapshot.uptime_ms.to_string(),
        "role": role(snapshot.role),
        "node_id": snapshot.node_id.as_deref().map(output::sanitize),
        "gap_caveat": GAP_CAVEAT,
        "age_caveat": AGE_CAVEAT,
        "replication": snapshot
            .replication
            .iter()
            .map(|entry| {
                json!({
                    "db": output::sanitize(&entry.db),
                    "repo": output::sanitize(&entry.repo),
                    "gap_versions_merged": entry.gap_versions_merged.to_string(),
                    "age": Value::Null,
                    "receipt_age_ns": entry
                        .receipt_age_ns
                        .map(|age| age.to_string()),
                })
            })
            .collect::<Vec<Value>>(),
        "repositories": snapshot
            .repositories
            .iter()
            .map(project_repository)
            .collect::<Vec<Value>>(),
    })
}

/// `version show`: the boot-observed epoch and outcome, the binary
/// version they belong to, and the OBSERVED role/node id (the same
/// launcher-captured values `status` reports — the shell never reads the
/// data directory and never invents an identity).
fn project_version(snapshot: &OperatorStatus) -> Value {
    json!({
        "server_version": output::sanitize(&snapshot.server_version),
        "format_epoch": snapshot.format_epoch.to_string(),
        "stamp_outcome": stamp_outcome(snapshot.stamp_outcome),
        "role": role(snapshot.role),
        "node_id": snapshot.node_id.as_deref().map(output::sanitize),
    })
}

/// One repository: effective vs stored durability and the WAL telemetry,
/// with the semantics of `durability_export.rs`. Budget bytes render as the
/// documented large decimal strings; absent override leaves are `null`.
fn project_repository(repo: &StatusRepository) -> Value {
    json!({
        "db": output::sanitize(&repo.db),
        "repo": output::sanitize(&repo.repo),
        "effective": {
            "wal_commit": repo.effective.wal_commit.map(wal_commit),
            "wal_retention_max_bytes": repo
                .effective
                .wal_retention_max_bytes
                .map(|budget| budget.get().to_string()),
            "on_wal_retention_exceeded": repo
                .effective
                .on_wal_retention_exceeded
                .map(retention_overflow),
            "replication_serve": repo.effective.replication_serve.map(serve),
        },
        "stored": repo.stored.map(|stored| {
            json!({
                "wal_commit": stored.wal_commit.map(wal_commit),
                "wal_retention_max_bytes": stored
                    .wal_retention_max_bytes
                    .map(|budget| budget.get().to_string()),
                "on_wal_retention_exceeded": stored
                    .on_wal_retention_exceeded
                    .map(retention_overflow),
                "replication_serve": stored.replication_serve.map(serve),
            })
        }),
        "wal": project_wal(&repo.wal),
    })
}

/// WAL telemetry, rendered verbatim in field names; `pending_versions` is a
/// bound, not an exact count of unflushed records.
fn project_wal(wal: &StatusWal) -> Value {
    json!({
        "backend_mem": wal.backend_mem,
        "watermark_w": wal.watermark_w.to_string(),
        "recoverable_through": wal.recoverable_through.to_string(),
        "pending_versions": wal.pending_versions.to_string(),
        "first_pending_version": wal.first_pending_version.map(|v| v.to_string()),
        "first_pending_age_ns": wal.first_pending_age_ns.map(|age| age.to_string()),
        "sync_now_calls": wal.sync_now_calls.to_string(),
        "sync_rounds": wal.sync_rounds.to_string(),
        "sync_coalesced": wal.sync_coalesced.to_string(),
        "sync_errors": wal.sync_errors.to_string(),
        "sync_duration_ns": wal.sync_duration_ns.to_string(),
    })
}

/// Stable snake_case rendering of the boot stamp outcome (mirrored enum,
/// `db_message.rs`).
fn stamp_outcome(outcome: StatusStampOutcome) -> &'static str {
    match outcome {
        StatusStampOutcome::Created => "created",
        StatusStampOutcome::Adopted => "adopted",
        StatusStampOutcome::Unchanged => "unchanged",
        StatusStampOutcome::VersionUpdated => "version_updated",
        StatusStampOutcome::InspectedStampless => "inspected_stampless",
        StatusStampOutcome::InspectedOlder => "inspected_older",
    }
}

/// Stable snake_case rendering of the replication role.
fn role(role: StatusRole) -> &'static str {
    match role {
        StatusRole::Follower => "follower",
        StatusRole::Unknown => "unknown",
    }
}

fn wal_commit(tier: shamir_query_types::durability::WalCommitTier) -> &'static str {
    match tier {
        shamir_query_types::durability::WalCommitTier::Buffered => "buffered",
        shamir_query_types::durability::WalCommitTier::Synced => "synced",
    }
}

fn retention_overflow(action: shamir_query_types::durability::RetentionOverflow) -> &'static str {
    match action {
        shamir_query_types::durability::RetentionOverflow::RefuseWrites => "refuse_writes",
        shamir_query_types::durability::RetentionOverflow::DeclareGap => "declare_gap",
    }
}

fn serve(mode: shamir_query_types::durability::ReplicationServe) -> &'static str {
    match mode {
        shamir_query_types::durability::ReplicationServe::Durable => "durable",
        shamir_query_types::durability::ReplicationServe::CompletedReseed => "completed_reseed",
    }
}

#[cfg(test)]
#[path = "../tests/commands/state_projection_tests.rs"]
mod state_projection_tests;

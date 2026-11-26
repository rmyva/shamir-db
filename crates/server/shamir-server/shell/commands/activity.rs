//! Activity commands (design `admin-shell.md` §4.5, §9 PR4):
//! `activity sessions`, `activity requests` and `activity cancel`.
//!
//! - `activity sessions` / `activity requests` send the typed
//!   `BatchOp::ListActivity` op through `Client::list_activity` and project
//!   the one requested registry. The listing is paged: at most
//!   [`MAX_PAGES`] pages of at most [`MAX_PAGE`] rows, never more than
//!   [`MAX_ROWS`] rows in total, following the server's opaque
//!   `continuation` while one is returned.
//! - `activity cancel` sends `Client::cancel_request` with the request
//!   execution id parsed LOCALLY and reports the typed cancellation
//!   outcome. A cancel must be issued from another session than the one
//!   running the target request.
//!
//! Every command is a SCRAM-session surface: the [`SessionClient`] wrappers
//! refuse a token session with the fixed typed denial before any request
//! (the parser gates it as well, and the server perimeter denies it too).
//!
//! The response rows are typed Rust enums, so there is no peer-controlled
//! shape to validate: every field is projected explicitly, every string is
//! sanitized through `output::sanitize`, and every integer is rendered as a
//! decimal string. A row that is not of the requested registry is
//! `ShellError::Server` and is never projected. Client failures route
//! through [`crate::shell::connect::request_error`] inside the
//! [`SessionClient`] wrappers.

use serde_json::{json, Value};
use shamir_query_types::admin::activity::{
    ActivityKind, ActivityPage, ActivityRequestState, ActivityRow, ActivityScope, CancelOutcome,
    ListActivityReq, RequestExecId,
};

use shamir_connect::common::types::limits::USERNAME_MAX_BYTES;
use shamir_connect::common::username::NormalizedUsername;

use super::super::output::{self, ShellError};
use crate::shell::connect::SessionClient;

/// Page size used when the operator does not pass `--limit`.
const DEFAULT_PAGE: u32 = 50;

/// Largest page size the shell requests, even for a larger `--limit`.
const MAX_PAGE: u32 = 50;

/// Most rows the shell projects across one listing.
const MAX_ROWS: usize = 100;

/// Most pages the shell requests across one listing.
const MAX_PAGES: usize = 10;

/// Longest accepted `activity cancel` request id in decimal digits.
const MAX_ID_DIGITS: usize = 20;

/// Replacement for a free-text activity field that is not in the shape the
/// server documents for it. One fixed, printable placeholder: a peer can
/// never smuggle an arbitrary printable string (credential-like text, an
/// escape sequence, an over-long blob) into the operator's terminal through
/// `user`, `transport`, `db` or `op_kind`.
const INVALID_FIELD: &str = "<invalid>";

/// Longest accepted principal name in bytes: exactly the engine's own login
/// limit, `USERNAME_MAX_BYTES` = 255
/// (`crates/net/shamir-connect/src/common/model/types.rs`, applied after
/// PRECIS `UsernameCaseMapped` normalisation in
/// `common/user/username.rs`), so a login the server accepts is never hidden
/// by the shell. A longer value is the fixed placeholder.
const MAX_USER_NAME_BYTES: usize = USERNAME_MAX_BYTES;

/// Longest accepted database name in bytes. The engine's own component rule
/// (`validate_name_component`,
/// `crates/engine/shamir-db/src/shamir_db/execute/core/helpers.rs`) imposes no
/// length cap, so the shell's profile caps a ROUTING database at 64 bytes
/// (`connect/profile.rs::MAX_NAME_LEN`) while a row routed by an SDK client
/// may be longer. 255 bytes is the fjall/OS name ceiling the engine's own
/// generated store names obey; the cap only bounds what a listing cell has to
/// render, and one byte over it is the placeholder.
const MAX_DB_NAME_BYTES: usize = 255;

/// Longest accepted `op_kind` in bytes (a wire `BatchOp` discriminant name).
///
/// The server derives these labels from the parsed operation discriminant
/// only (`crates/query/shamir-query-types/src/admin/activity/op_name.rs`); the
/// longest static one is `admin:apply_application_schema_migration` (40
/// bytes), plus the three non-`admin:` labels `read`, `write` and `tx_open`
/// the server mints. 64 leaves headroom for a new label while still bounding
/// the cell, and every label of that file must pass (see
/// `../tests/commands/access/activity_field_tests.rs`, which pins the whole list).
const MAX_OP_KIND_BYTES: usize = 64;

/// Exactly the transports `transport_label` in the server emits.
const TRANSPORTS: [&str; 3] = ["tcp", "ws", "ipc"];

/// R5 step 4 activity commands.
#[derive(Debug, Clone)]
pub(in crate::shell) enum ActivityCommand {
    /// `activity sessions [--all] [--limit N]` — live sessions.
    Sessions { all: bool, limit: Option<u32> },
    /// `activity requests [--all] [--limit N]` — in-flight requests.
    Requests { all: bool, limit: Option<u32> },
    /// `activity cancel REQUEST_ID [--yes]` — cooperative cancellation.
    Cancel { request: String, yes: bool },
}

impl ActivityCommand {
    /// Stable space-separated command id (JSON v1 `command` field, §5).
    pub(in crate::shell) fn id(&self) -> &'static str {
        match self {
            ActivityCommand::Sessions { .. } => "activity sessions",
            ActivityCommand::Requests { .. } => "activity requests",
            ActivityCommand::Cancel { .. } => "activity cancel",
        }
    }

    /// Wire operation label (JSON v1 `op` field, §5).
    pub(in crate::shell) fn op(&self) -> &'static str {
        match self {
            ActivityCommand::Sessions { .. } | ActivityCommand::Requests { .. } => "ListActivity",
            ActivityCommand::Cancel { .. } => "CancelRequest",
        }
    }

    /// Every command here addresses the profile's routing database, so all
    /// of them carry the routing database floor.
    pub(in crate::shell) fn requires_routing_db(&self) -> bool {
        true
    }

    /// Only `activity cancel` changes server-side state, so only it needs a
    /// confirmation.
    pub(in crate::shell) fn wants_confirm(&self) -> bool {
        matches!(self, ActivityCommand::Cancel { .. })
    }

    /// Pre-connection validation: a `--limit` of zero is a local usage
    /// error, and the `activity cancel` request id must be a strict decimal
    /// `u64`. Both are refused before any dial.
    pub(in crate::shell) fn validate(&self) -> Result<(), ShellError> {
        match self {
            ActivityCommand::Sessions { limit, .. } | ActivityCommand::Requests { limit, .. } => {
                match limit {
                    Some(0) => Err(ShellError::Usage),
                    _ => Ok(()),
                }
            }
            ActivityCommand::Cancel { request, .. } => parse_exec_id(request).map(|_| ()),
        }
    }

    /// Execute the command on the routing database `routing_db`.
    /// Authorization happens exclusively on the normal remote route; this
    /// code sends no privilege material.
    /// cancel-safe: NO — a sent request may still execute; do not retry on
    /// timeout.
    pub(in crate::shell) async fn execute(
        &self,
        client: &SessionClient,
        routing_db: &str,
    ) -> Result<Value, ShellError> {
        let started = tokio::time::Instant::now();
        let payload = match self {
            ActivityCommand::Sessions { all, limit } => {
                let listed =
                    list_rows(client, routing_db, *all, ActivityKind::Sessions, *limit).await;
                listed.and_then(|(rows, truncated)| {
                    project_page("sessions", *all, &rows, truncated, project_session)
                })
            }
            ActivityCommand::Requests { all, limit } => {
                let listed =
                    list_rows(client, routing_db, *all, ActivityKind::Requests, *limit).await;
                listed.and_then(|(rows, truncated)| {
                    project_page("requests", *all, &rows, truncated, project_request)
                })
            }
            ActivityCommand::Cancel { request, .. } => {
                // validate() already parsed this id; the parse is repeated
                // here because `request` is owned text and validate borrows.
                match parse_exec_id(request) {
                    Err(error) => Err(error),
                    Ok(raw) => {
                        let outcome = client.cancel_request(routing_db, RequestExecId(raw)).await;
                        outcome.map(|outcome| json!({ "outcome": outcome_name(outcome) }))
                    }
                }
            }
        };
        output::finish_command(started, payload, |payload| {
            client.check_catalogue_output(payload)
        })
    }
}

/// Parse one `activity cancel` request id: ASCII decimal digits only (no
/// sign, space or prefix), at most [`MAX_ID_DIGITS`] digits, and inside the
/// `u64` range. Anything else is the fixed typed `Usage` refusal.
fn parse_exec_id(text: &str) -> Result<u64, ShellError> {
    if text.is_empty() || text.len() > MAX_ID_DIGITS {
        return Err(ShellError::Usage);
    }
    if !text.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(ShellError::Usage);
    }
    text.parse::<u64>().map_err(|_| ShellError::Usage)
}

/// Fetch one listing page by page. The page size is `--limit` clamped to
/// [`MAX_PAGE`] (default [`DEFAULT_PAGE`]); the loop stops when the server
/// returns no continuation, after [`MAX_PAGES`] pages, or once the row cap
/// [`MAX_ROWS`] is reached — in the last case WITHOUT issuing another
/// request. The flag is true when a cap stopped a listing that still had a
/// continuation, or when rows were dropped by the row cap.
async fn list_rows(
    client: &SessionClient,
    db: &str,
    all: bool,
    kind: ActivityKind,
    limit: Option<u32>,
) -> Result<(Vec<ActivityRow>, bool), ShellError> {
    let page_size = limit.unwrap_or(DEFAULT_PAGE).min(MAX_PAGE);
    let scope = if all {
        ActivityScope::All
    } else {
        ActivityScope::Own
    };
    collect_pages(MAX_ROWS, MAX_PAGES, |continuation| {
        let request = ListActivityReq {
            scope,
            kind,
            limit: Some(page_size),
            continuation,
        };
        async move { client.list_activity(db, request).await }
    })
    .await
}

/// The paging rule of one listing, independent of the transport so the cap
/// edge is testable with a counting fake. `fetch` returns the next page for
/// the continuation it is given. Reaching `cap` rows ALWAYS ends the walk in
/// the same iteration: a listing that is exactly `cap` rows long with a
/// continuation is truncated, and no further request is issued.
pub(super) async fn collect_pages<F, Fut>(
    cap: usize,
    max_pages: usize,
    mut fetch: F,
) -> Result<(Vec<ActivityRow>, bool), ShellError>
where
    F: FnMut(Option<String>) -> Fut,
    Fut: std::future::Future<Output = Result<ActivityPage, ShellError>>,
{
    let mut rows: Vec<ActivityRow> = Vec::new();
    let mut continuation: Option<String> = None;
    let mut pages = 0usize;
    loop {
        let page = fetch(continuation.take()).await?;
        pages += 1;
        let mut dropped = false;
        for row in page.rows {
            if rows.len() >= cap {
                dropped = true;
                break;
            }
            rows.push(row);
        }
        let next = page.continuation;
        if rows.len() >= cap {
            return Ok((rows, dropped || next.is_some()));
        }
        let Some(next) = next else {
            return Ok((rows, dropped));
        };
        if pages >= max_pages {
            return Ok((rows, true));
        }
        continuation = Some(next);
    }
}

/// Project one listing: the requested registry name, the scope, the
/// projected rows and the two scalar markers.
fn project_page(
    kind: &'static str,
    all: bool,
    rows: &[ActivityRow],
    truncated: bool,
    project: impl Fn(&ActivityRow) -> Result<Value, ShellError>,
) -> Result<Value, ShellError> {
    let mut projected = Vec::with_capacity(rows.len());
    for row in rows {
        projected.push(project(row)?);
    }
    let returned = projected.len().to_string();
    Ok(json!({
        "kind": kind,
        "scope": if all { "all" } else { "own" },
        "rows": projected,
        "rows_returned": returned,
        "rows_truncated": truncated,
    }))
}

/// Project one session row; any other row kind is `ShellError::Server`.
fn project_session(row: &ActivityRow) -> Result<Value, ShellError> {
    match row {
        ActivityRow::Session {
            session_handle,
            user,
            transport,
            created_at_ns,
            idle_ns,
            open_transactions,
            open_cursors,
        } => Ok(json!({
            "kind": "session",
            "session_handle": session_handle.to_string(),
            "user": output::sanitize(&user_field(user, MAX_USER_NAME_BYTES)),
            "transport": output::sanitize(&transport_field(transport)),
            "created_at_ns": created_at_ns.to_string(),
            "idle_ns": idle_ns.to_string(),
            "open_transactions": open_transactions.to_string(),
            "open_cursors": open_cursors.to_string(),
        })),
        _ => Err(ShellError::Server),
    }
}

/// Project one request row; any other row kind is `ShellError::Server`.
fn project_request(row: &ActivityRow) -> Result<Value, ShellError> {
    match row {
        ActivityRow::Request {
            request_exec_id,
            session_handle,
            user,
            db,
            op_kind,
            state,
            elapsed_ns,
        } => Ok(json!({
            "kind": "request",
            "request_exec_id": request_exec_id.0.to_string(),
            "session_handle": (*session_handle).map(|handle| handle.to_string()),
            "user": output::sanitize(&user_field(user, MAX_USER_NAME_BYTES)),
            "db": output::sanitize(&name_field(db, MAX_DB_NAME_BYTES)),
            "op_kind": output::sanitize(&op_kind_field(op_kind)),
            "state": request_state_name(*state),
            "elapsed_ns": elapsed_ns.to_string(),
        })),
        _ => Err(ShellError::Server),
    }
}

/// One database name, projected ONLY when it matches the engine's own
/// component rule (`[A-Za-z0-9_-]`) and fits `cap` bytes; anything else
/// becomes the fixed [`INVALID_FIELD`] placeholder. The charset is the
/// engine's (`validate_name_component`), which is ASCII-only on purpose: a
/// name outside that alphabet — including a non-ASCII login, which is not a
/// database name — renders as the placeholder. Principal names do NOT use
/// this rule; they go through [`user_field`].
fn name_field(raw: &str, cap: usize) -> String {
    let allowed = !raw.is_empty()
        && raw.len() <= cap
        && raw
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-');
    if allowed {
        raw.to_owned()
    } else {
        INVALID_FIELD.to_owned()
    }
}

/// The `shk1.` prefix of the shell's own API-token wire shape, mirrored from
/// the credential check in `crate::shell::connect::token`
/// (`check_catalogue_output`, whose comment names the canonical
/// `shk1.<id>.<secret>` format that this shell refuses to echo). A `user`
/// cell carrying that prefix ANYWHERE is redacted even though the login rule
/// itself would accept the text: over-redacting a token-shaped name is the
/// owner's accepted choice.
const API_TOKEN_PREFIX: &str = "shk1.";

/// One principal name, projected ONLY when the SERVER would accept it as a
/// login: [`NormalizedUsername::from_raw`]
/// (`crates/net/shamir-connect/src/common/user/username.rs`), the RFC 8265
/// PRECIS `UsernameCaseMapped` validator, must return `Ok(normalized)` with
/// `normalized.as_str() == raw` — the canonical wire text — and `raw` must
/// fit `cap` bytes, carry no display control, and not be shaped like an API
/// token ([`API_TOKEN_PREFIX`]). Anything else becomes the fixed
/// [`INVALID_FIELD`] placeholder.
///
/// This delegates to the server's own validator and does NOT re-implement
/// PRECIS: no width mapping, case mapping, NFC normalisation or Bidi rule
/// lives here, so a name the server normalises away (decomposed NFC, a
/// private-use code point, a soft hyphen, an uppercase letter) is rejected
/// because it is not the text the server would store, and a name the server
/// rejects is never admitted. The token check on top of the login rule is
/// deliberate over-redaction. Terminal safety still comes from
/// `output::sanitize` downstream, which escapes control, display-control and
/// backslash characters.
fn user_field(raw: &str, cap: usize) -> String {
    let canonical =
        NormalizedUsername::from_raw(raw).is_ok_and(|normalized| normalized.as_str() == raw);
    // Belt and braces: the validator already rejects these, but the
    // terminal-safety rule stays explicit here.
    let display_safe = raw
        .chars()
        .all(|character| !character.is_control() && !is_display_control(character));
    let allowed = canonical && raw.len() <= cap && display_safe && !raw.contains(API_TOKEN_PREFIX);
    if allowed {
        raw.to_owned()
    } else {
        INVALID_FIELD.to_owned()
    }
}

/// The display-control characters `output::sanitize` escapes: the bidi
/// controls U+061C, U+200E, U+200F, U+202A–202E and U+2066–2069, the
/// zero-width/format characters U+200B–200D, U+2060–2064 and U+FEFF, and the
/// line/paragraph separators U+2028 and U+2029.
///
/// A private mirror of `is_display_control` in `crate::shell::output`, which
/// is private to that module (reaching it would mean widening its
/// visibility); keep the two tables in sync.
fn is_display_control(character: char) -> bool {
    matches!(
        character,
        '\u{061C}'
            | '\u{200B}'..='\u{200D}'
            | '\u{200E}' | '\u{200F}'
            | '\u{202A}'..='\u{202E}'
            | '\u{2060}'..='\u{2064}'
            | '\u{2066}'..='\u{2069}'
            | '\u{2028}' | '\u{2029}'
            | '\u{FEFF}'
    )
}

/// One free-text session transport, projected ONLY from the closed set the
/// server emits (`tcp`, `ws`, `ipc`).
fn transport_field(raw: &str) -> String {
    if TRANSPORTS.contains(&raw) {
        raw.to_owned()
    } else {
        INVALID_FIELD.to_owned()
    }
}

/// One free-text `op_kind`, projected ONLY when it is a wire operation
/// discriminant name (`[a-z0-9_:]`, at most [`MAX_OP_KIND_BYTES`] bytes). The
/// colon is part of the server's own labels (`admin:list_activity`).
fn op_kind_field(raw: &str) -> String {
    let allowed = !raw.is_empty()
        && raw.len() <= MAX_OP_KIND_BYTES
        && raw.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_' || byte == b':'
        });
    if allowed {
        raw.to_owned()
    } else {
        INVALID_FIELD.to_owned()
    }
}

/// The fixed lowercase name of one request state.
fn request_state_name(state: ActivityRequestState) -> &'static str {
    match state {
        ActivityRequestState::Running => "running",
        ActivityRequestState::Done => "done",
        ActivityRequestState::Committing => "committing",
        ActivityRequestState::Cancelled => "cancelled",
        ActivityRequestState::WaitingLock => "waiting_lock",
        ActivityRequestState::WaitingVisibility => "waiting_visibility",
        ActivityRequestState::WaitingBudget => "waiting_budget",
    }
}

#[cfg(test)]
#[path = "../tests/commands/access/activity_field_tests.rs"]
mod activity_field_tests;

/// The fixed lowercase name of one cancellation outcome.
fn outcome_name(outcome: CancelOutcome) -> &'static str {
    match outcome {
        CancelOutcome::Cancelled => "cancelled",
        CancelOutcome::AlreadyFinished => "already_finished",
        CancelOutcome::AlreadyCommitting => "already_committing",
        CancelOutcome::NotCancellable => "not_cancellable",
    }
}

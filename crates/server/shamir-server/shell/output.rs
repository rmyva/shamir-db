//! Shell output contract: JSON Lines v1 envelope, typed error set with
//! stable code/message/exit mapping, bounded typed-record projection, and
//! display-neutral string escaping applied to every authorized string in
//! both streams. Design: `docs/dev-artifacts/design/admin-shell.md` §§5–6.

use std::fmt::Write as _;
use std::io::{self, IsTerminal, Write};

use serde::Serialize;
use serde_json::{json, Value as Json};

use shamir_types::types::value::QueryValue;

/// Projection recursion bound; exceeding it is a typed error, not a stack
/// overflow.
const MAX_PROJECTION_DEPTH: usize = 32;

/// Human value cap in `char`s; truncation happens after sanitization and is
/// backed up to a whole escape boundary, so an escape sequence is never cut
/// mid-sequence.
const HUMAN_VALUE_MAX_CHARS: usize = 200;

/// Cap for human field labels in `char`s; labels are peer-supplied map keys
/// (already sanitized), capped with the same escape-safe truncation as
/// values so a column header can never flood the terminal.
const HUMAN_LABEL_MAX_CHARS: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ShellError {
    Usage,
    OutputVersion,
    MissingTty,
    Connection,
    Authentication,
    Denied,
    Server,
    /// Typed peer refusal `not_found` (the P2 exact-key point read missed).
    NotFound,
    /// Typed peer refusal `unknown_table`: the addressed table does not
    /// exist (P2 point read).
    UnknownTable,
    /// Typed peer refusal `resync_required`: the repository is in
    /// replication quarantine; retryable only after a reseed.
    ResyncRequired,
    /// Typed peer refusal `result_too_large`: the encoded response is over
    /// the server's byte cap.
    ResultTooLarge,
    ProjectionLimit,
    Unsupported,
    Plaintext,
    /// Token auth on a transport that does not support it.
    Token,
    TokenRequestDenied,
    /// `data get` KEY that is not a well-formed Base58 `RecordId` (typed,
    /// refused pre-connect after the token gate).
    InvalidRecordId,
    /// `data head` local monotonic projection budget expired after the
    /// request was sent: outcome unknown, never a partial success.
    HeadWallTime,
    Deferred,
    Resume,
    MissingPin,
    PinMismatch,
    InvalidProfile,
    InsecureProfile,
    InvalidSecret,
    InsecureSecret,
    /// Windows-only: `--secret-fd` is refused there (Unix reads the fd).
    #[cfg(windows)]
    SecretFdWindows,
    ArgvSecret,
    SecretTerminal,
    SecretTooLong,
    SecretSourceConflict,
    Timeout,
    Interrupted,
    /// `chmod`/`chown`/`chgrp` addressed the resource root, which the shell
    /// never mutates.
    RootMutation,
    /// A state-changing command was answered neither with `--yes` nor with
    /// an interactive `yes`.
    ConfirmationRequired,
    /// `chmod` MODE is not a one-to-four digit octal literal at most
    /// `0o7777`.
    InvalidMode,
    /// The named owner or group does not exist in the observed directory.
    UnknownPrincipal,
    /// A rights mutation named an owner or group id the server does not
    /// know (`invalid_owner`).
    InvalidOwner,
    /// A signed rights mutation carried a tag that does not match the
    /// canonical bytes (`hmac_mismatch`).
    HmacMismatch,
    /// A rights mutation reached the server without a tag the destructive-op
    /// gate accepts (`hmac_required`).
    HmacRequired,
    /// A mutation addressed a database that does not exist (`unknown_db`).
    UnknownDb,
    /// The access tree is larger than the shell's whole-tree projection
    /// budget (`group tree` / `group members`); `rights show` is unaffected.
    TreeTooLarge,
    /// The node is a read-only replica and refuses this write
    /// (`read_only_replica`).
    ReadOnlyReplica,
    /// The request was cancelled by another operator before it finished
    /// (`cancelled`): its effects may be partial.
    Cancelled,
    /// `backup start` was refused because another online copy already runs on
    /// this node (`backup_already_running`).
    BackupAlreadyRunning,
    /// The server refused the `--to` destination (`backup_invalid_destination`):
    /// the path is passed unmodified and the server's own validation decides.
    BackupInvalidDestination,
    /// The node has no online-copy facility configured (`backup_unavailable`).
    BackupUnavailable,
    /// The node is a follower: node-local copy start/cancel are refused there
    /// (`online_backup_follower_read_only`).
    OnlineBackupFollowerReadOnly,
    /// The node is a follower: application-schema status is refused there
    /// (`schema_migration_follower_read_only`).
    SchemaMigrationFollowerReadOnly,
    /// An I/O error inside the online-backup runner (`backup_io_error`).
    BackupIo,
    /// An online export was refused because the shared export slot is busy.
    ExportBusy,
    /// The export runner is unavailable or not configured.
    ExportUnavailable,
    /// Batch export status/cancel cannot address a job started in another session.
    ExportSessionOnly,
    /// No export job is visible to this session.
    ExportNotFound,
}

impl ShellError {
    /// Stable machine code; never a verbatim server string.
    pub(super) fn code(self) -> &'static str {
        match self {
            ShellError::Usage => "usage",
            ShellError::OutputVersion => "unsupported_output_version",
            ShellError::MissingTty => "interactive_mode_unavailable",
            ShellError::Connection => "connection_failed",
            ShellError::Authentication => "authentication_failed",
            ShellError::Denied => "permission_denied",
            ShellError::Server => "server_error",
            ShellError::NotFound => "not_found",
            ShellError::UnknownTable => "unknown_table",
            ShellError::ResyncRequired => "resync_required",
            ShellError::ResultTooLarge => "result_too_large",
            ShellError::ProjectionLimit => "projection_limit",
            ShellError::Unsupported => "unsupported",
            ShellError::Plaintext => "plaintext_refused",
            ShellError::Token => "token_transport_refused",
            ShellError::TokenRequestDenied => "token_request_denied",
            ShellError::InvalidRecordId => "invalid_record_id",
            ShellError::HeadWallTime => "head_wall_time",
            ShellError::Deferred => "deferred",
            ShellError::Resume => "resume_refused",
            ShellError::MissingPin => "missing_pin",
            ShellError::PinMismatch => "pin_mismatch",
            ShellError::InvalidProfile => "invalid_profile",
            ShellError::InsecureProfile => "insecure_profile",
            ShellError::InvalidSecret => "invalid_secret",
            ShellError::InsecureSecret => "insecure_secret",
            #[cfg(windows)]
            ShellError::SecretFdWindows => "secret_fd_windows",
            ShellError::ArgvSecret => "secret_on_argv_refused",
            ShellError::SecretTerminal => "secret_prompt_unavailable",
            ShellError::SecretTooLong => "secret_too_long",
            ShellError::SecretSourceConflict => "secret_source_conflict",
            ShellError::Timeout => "timeout_outcome_unknown",
            ShellError::Interrupted => "interrupted",
            ShellError::RootMutation => "root_mutation_refused",
            ShellError::ConfirmationRequired => "confirmation_required",
            ShellError::InvalidMode => "invalid_mode",
            ShellError::UnknownPrincipal => "unknown_principal",
            ShellError::InvalidOwner => "invalid_owner",
            ShellError::HmacMismatch => "hmac_mismatch",
            ShellError::HmacRequired => "hmac_required",
            ShellError::UnknownDb => "unknown_db",
            ShellError::TreeTooLarge => "tree_too_large",
            ShellError::ReadOnlyReplica => "read_only_replica",
            ShellError::Cancelled => "cancelled",
            ShellError::BackupAlreadyRunning => "backup_already_running",
            ShellError::BackupInvalidDestination => "backup_invalid_destination",
            ShellError::BackupUnavailable => "backup_unavailable",
            ShellError::OnlineBackupFollowerReadOnly => "online_backup_follower_read_only",
            ShellError::SchemaMigrationFollowerReadOnly => "schema_migration_follower_read_only",
            ShellError::BackupIo => "backup_io_error",
            ShellError::ExportBusy => "online_export_already_running",
            ShellError::ExportSessionOnly => "export_session_only",
            ShellError::ExportUnavailable => "online_export_unavailable",
            ShellError::ExportNotFound => "online_export_not_found",
        }
    }

    /// Fixed local message; design §3.2/§3.3 inventory strings verbatim
    /// where one exists.
    pub(super) fn message(self) -> &'static str {
        match self {
            ShellError::Usage => "invalid command usage",
            ShellError::OutputVersion => "unsupported --output-version value; only 1 is supported",
            ShellError::MissingTty => {
                "interactive mode requires a terminal on stdin and stdout; use the single-command batch form after --"
            }
            ShellError::Connection => "connection to the server failed",
            ShellError::Authentication => "authentication failed",
            ShellError::Denied => "permission denied by the server",
            ShellError::Server => "the operation failed on the server",
            ShellError::NotFound => "the addressed resource was not found",
            ShellError::UnknownTable => "table is unavailable",
            ShellError::ResyncRequired => "the repository is in replication quarantine; a reseed is required",
            ShellError::ResultTooLarge => "the encoded response exceeds the server's result-size cap",
            ShellError::ProjectionLimit => "the record could not be projected to the output schema",
            ShellError::Unsupported => {
                "the requested command, transport or authentication combination is not supported"
            }
            ShellError::Plaintext => {
                "refusing plaintext admin connection; use TLS TCP, WSS, or local IPC"
            }
            ShellError::Token => "API tokens require HTTPS REST or the native token WSS listener",
            ShellError::TokenRequestDenied => {
                "token sessions expose only the read-only shell subset; log in with SCRAM for this command"
            }
            ShellError::InvalidRecordId => {
                "the KEY is not a valid Base58 record id"
            }
            ShellError::HeadWallTime => {
                "the read exceeded its local wall-time budget after send; its outcome is unknown and no result is reported"
            }
            ShellError::Deferred => {
                "browser profile and HTTP password login are deferred; HTTP login mints a reusable API token, not a session"
            }
            ShellError::Resume => "session resume is never used by the admin shell; log in fresh",
            ShellError::MissingPin => {
                "SCRAM server identity pin is required; configure a verified pin in the profile"
            }
            ShellError::PinMismatch => {
                "server identity pin does not match the verified pin in the profile"
            }
            ShellError::InvalidProfile => "the connection profile is invalid",
            ShellError::InsecureProfile => {
                "the connection profile location failed the security checks"
            }
            ShellError::InvalidSecret => "the secret could not be read",
            ShellError::InsecureSecret => {
                "the requested secret source is refused; use --secret-stdin, --secret-fd, or --secret-env"
            }
            #[cfg(windows)]
            ShellError::SecretFdWindows => "--secret-fd is Unix-only; use --secret-stdin",
            ShellError::ArgvSecret => {
                "secrets on the command line are refused; use --secret-stdin, --secret-fd, or --secret-env"
            }
            ShellError::SecretTerminal => {
                "the hidden secret prompt requires a terminal; use --secret-stdin, --secret-fd, or --secret-env"
            }
            ShellError::SecretTooLong => {
                "the secret exceeds the 4096-byte limit; it is refused, never truncated"
            }
            ShellError::SecretSourceConflict => {
                "at most one secret source may be given; use only one of the prompt, --secret-stdin, --secret-fd, or --secret-env"
            }
            ShellError::Timeout => {
                "the operation timed out after send; its outcome is unknown and it may still be running"
            }
            ShellError::Interrupted => "interrupted",
            ShellError::RootMutation => {
                "the root resource cannot be modified from the shell; address a database, store, table or function"
            }
            ShellError::ConfirmationRequired => {
                "this command changes state or cancels work; confirm interactively or pass --yes in batch mode"
            }
            ShellError::InvalidMode => {
                "MODE must be an octal mode (1 to 4 digits, 0-7 only, at most 7777)"
            }
            ShellError::UnknownPrincipal => {
                "the named owner or group does not exist on this server"
            }
            ShellError::InvalidOwner => {
                "the addressed owner or group does not exist on this server"
            }
            ShellError::HmacMismatch => {
                "the signed request was refused by the server: its signature did not match"
            }
            ShellError::HmacRequired => {
                "the request carried no signature the server accepts for a destructive change"
            }
            ShellError::UnknownDb => {
                "the addressed resource does not exist on this server"
            }
            ShellError::TreeTooLarge => {
                "the access tree exceeds the shell's 2000-node projection limit; narrow the databases with group tree --db DATABASE (functions are never filtered)"
            }
            ShellError::ReadOnlyReplica => {
                "this node is a read-only replica and refuses writes; repeat the change on the leader"
            }
            ShellError::Cancelled => {
                "the request was cancelled before it finished; its earlier effects may already be applied"
            }
            ShellError::BackupAlreadyRunning => {
                "another online copy is already running on this node; wait for it to finish or cancel the current run"
            }
            ShellError::BackupInvalidDestination => {
                "the server refused the backup destination; give an absolute path on the server outside its data directory"
            }
            ShellError::BackupUnavailable => "this node has no online-copy facility configured",
            ShellError::OnlineBackupFollowerReadOnly => {
                "this node is a read-only replica: starting or cancelling an online copy is refused here; run it on the leader"
            }
            ShellError::SchemaMigrationFollowerReadOnly => {
                "this node is a read-only replica and has no consistent migration-journal view; ask the leader"
            }
            ShellError::BackupIo => "the online copy failed on an I/O error on the server node; its own message text is never reproduced",
            ShellError::ExportSessionOnly => "an export job belongs to the session that started it",
            ShellError::ExportBusy => "an online export job is already running on this server; wait for it to finish or cancel it",
            ShellError::ExportUnavailable => "online export is not configured or unavailable on this node",
            ShellError::ExportNotFound => "no online export job is visible to this session",
        }
    }

    /// Exit code, exactly the design §5 table; no failure maps to 0.
    pub(super) fn exit(self) -> u8 {
        match self {
            ShellError::Usage
            | ShellError::OutputVersion
            | ShellError::MissingTty
            | ShellError::InvalidRecordId
            | ShellError::InvalidMode => 1,
            ShellError::ExportSessionOnly => 1,
            ShellError::Connection => 2,
            ShellError::Authentication => 3,
            ShellError::Denied | ShellError::TokenRequestDenied => 4,
            ShellError::Server
            | ShellError::ProjectionLimit
            | ShellError::NotFound
            | ShellError::UnknownTable
            | ShellError::ResyncRequired
            | ShellError::ResultTooLarge
            | ShellError::UnknownPrincipal
            | ShellError::InvalidOwner
            | ShellError::HmacMismatch
            | ShellError::HmacRequired
            | ShellError::UnknownDb
            | ShellError::TreeTooLarge
            | ShellError::ReadOnlyReplica
            | ShellError::Cancelled
            | ShellError::BackupAlreadyRunning
            | ShellError::BackupInvalidDestination
            | ShellError::BackupUnavailable
            | ShellError::OnlineBackupFollowerReadOnly
            | ShellError::SchemaMigrationFollowerReadOnly
            | ShellError::BackupIo
            | ShellError::ExportBusy
            | ShellError::ExportUnavailable
            | ShellError::ExportNotFound => 5,
            ShellError::Unsupported
            | ShellError::Plaintext
            | ShellError::Token
            | ShellError::Deferred
            | ShellError::Resume
            | ShellError::MissingPin
            | ShellError::PinMismatch
            | ShellError::InvalidProfile
            | ShellError::InsecureProfile
            | ShellError::InvalidSecret
            | ShellError::InsecureSecret
            | ShellError::ArgvSecret
            | ShellError::SecretTerminal
            | ShellError::SecretTooLong
            | ShellError::SecretSourceConflict
            | ShellError::RootMutation
            | ShellError::ConfirmationRequired => 6,
            #[cfg(windows)]
            ShellError::SecretFdWindows => 6,
            ShellError::Timeout | ShellError::HeadWallTime => 7,
            ShellError::Interrupted => 130,
        }
    }
}

/// JSON Lines v1 envelope. `data` is always present (`null` on error);
/// `code`/`message` appear on error records only. `node` carries the
/// OBSERVED role and node id only for the P1 status view (`Status` op:
/// `status` and `version show`); it stays `null` everywhere else — the
/// shell cannot invent an identity it never observed (§5/§6).
#[derive(Serialize)]
struct Envelope<'a> {
    version: u8,
    command: &'a str,
    kind: &'static str,
    op: Option<&'static str>,
    node: &'a Json,
    data: &'a Json,
    #[serde(skip_serializing_if = "Option::is_none")]
    code: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<&'static str>,
}

/// Emits the single output record of one command and returns the exit code.
///
/// Batch: exactly one LF-terminated JSON Lines v1 envelope on stdout.
/// Interactive: stdout silent; a fixed-width human table (or error block)
/// on stderr, only when stdout is an interactive TTY. `command` is the
/// stable command ID, never raw argv.
pub(super) fn emit(
    batch: bool,
    command: &'static str,
    op: Option<&'static str>,
    result: Result<Json, ShellError>,
) -> u8 {
    if batch {
        let node = observed_node(op, result.as_ref().ok());
        let envelope = match &result {
            Ok(data) => Envelope {
                version: 1,
                command,
                kind: "result",
                op,
                node: &node,
                data,
                code: None,
                message: None,
            },
            Err(err) => Envelope {
                version: 1,
                command,
                kind: "error",
                op,
                node: &node,
                data: &Json::Null,
                code: Some(err.code()),
                message: Some(err.message()),
            },
        };
        // A successful serialization returns 0 even when `result` is an
        // error envelope: the original error keeps its own exit code, and
        // only a failed stdout write overrides it with the shell-side
        // failure code. A successful result takes the write status.
        let write_status = write_envelope(&envelope);
        return match (&result, write_status) {
            (Ok(_), _) => write_status,
            (Err(err), 0) => err.exit(),
            (Err(_), _) => write_status,
        };
    }
    if !io::stdout().is_terminal() {
        return match &result {
            Ok(_) => 0,
            Err(err) => err.exit(),
        };
    }
    let stderr = io::stderr();
    let mut handle = stderr.lock();
    emit_human(&mut handle, command, result)
}

/// The observed node identity for the envelope `node` field: only the P1
/// status view (`op == "Status"`, i.e. `status` AND `version show`) has
/// one, and only when the payload actually OBSERVED a role (both fields
/// are copies of the ALREADY SANITIZED projected status payload; absent
/// leaves stay `null`). A payload without an observed role — or any
/// other command — is `null`: an all-null object is never emitted, and
/// the shell cannot invent an identity it never observed (§5/§6).
fn observed_node(op: Option<&str>, data: Option<&Json>) -> Json {
    let Some(data) = data else {
        return Json::Null;
    };
    match op {
        Some("Status") if data.get("role").and_then(Json::as_str).is_some() => json!({
            "role": data.get("role").cloned().unwrap_or(Json::Null),
            "node_id": data.get("node_id").cloned().unwrap_or(Json::Null),
        }),
        _ => Json::Null,
    }
}

/// Human rendering of one command result through an explicit writer, for the
/// shared interactive loop (real stderr or captured error output). Same
/// sanitized data and presentation as `emit`'s interactive path; returns the
/// exit code. Human output never changes the exit code on a write error.
pub(super) fn emit_human(
    stderr: &mut dyn Write,
    command: &'static str,
    result: Result<Json, ShellError>,
) -> u8 {
    match &result {
        Ok(data) => {
            write_human_result(stderr, command, data);
            0
        }
        Err(err) => {
            write_human_error(stderr, err);
            err.exit()
        }
    }
}

/// Serializes and writes the envelope; a failed stdout write maps to the
/// shell-side failure exit and is never retried.
fn write_envelope(envelope: &Envelope<'_>) -> u8 {
    match serde_json::to_string(envelope) {
        Ok(line) => {
            let stdout = io::stdout();
            let mut handle = stdout.lock();
            match writeln!(handle, "{}", line).and_then(|()| handle.flush()) {
                Ok(()) => 0,
                Err(_) => ShellError::Usage.exit(),
            }
        }
        Err(_) => ShellError::Usage.exit(),
    }
}

/// Re-consult the per-command budget (design §5, §6) AFTER the last blocking
/// step and after the synchronous projection / credential scan, which the
/// outer [`crate::shell::parser::session::guarded`] `timeout` cannot preempt
/// because no further `.await` happens. Same pattern as the `data head`
/// deadline (`state.rs::head_tail`): an elapsed budget is the typed
/// `timeout_outcome_unknown` refusal (exit 7), never a silent success.
pub(super) fn deadline_check<T>(
    started: tokio::time::Instant,
    outcome: Result<T, ShellError>,
) -> Result<T, ShellError> {
    if started.elapsed() >= crate::shell::connect::REQUEST_TIMEOUT {
        return Err(ShellError::Timeout);
    }
    outcome
}

/// The shared command tail: the projection outcome, then the synchronous
/// output scan, then the deadline consulted ONE LAST TIME — the ordering of
/// `data head` (`state.rs::head_tail`), so an already-elapsed budget is the
/// typed timeout (exit 7) even when the projection was refused by an
/// unrelated error or the scan came back clean (which would otherwise exit
/// 0 or exit 5). The scan is not interruptible: this is ordering, not
/// preemption.
pub(super) fn finish_command<T>(
    started: tokio::time::Instant,
    projected: Result<T, ShellError>,
    scan: impl FnOnce(&T) -> Result<(), ShellError>,
) -> Result<T, ShellError> {
    let payload = projected.map_err(|error| deadline_error(started, error))?;
    deadline_check(started, scan(&payload))?;
    Ok(payload)
}

/// A refusal on an elapsed budget keeps the deadline's typed outcome: the
/// budget wins over any per-step error (mirrors `state.rs::head_error`).
pub(super) fn deadline_error(started: tokio::time::Instant, error: ShellError) -> ShellError {
    if started.elapsed() >= crate::shell::connect::REQUEST_TIMEOUT {
        ShellError::Timeout
    } else {
        error
    }
}

/// Fixed-width FIELD/VALUE table (or a single `value` row) for stderr; every
/// cell sanitized before rendering, truncation after sanitization. A listing
/// command's non-empty array of objects renders one row per LINE; every
/// other command keeps the single-cell rendering.
fn write_human_result(stderr: &mut dyn Write, command: &str, data: &Json) {
    let expand_lists = matches!(command, "activity sessions" | "activity requests");
    let mut rows: Vec<(String, String)> = Vec::new();
    match data {
        Json::Object(fields) => {
            for (field, item) in fields {
                // A listing renders one row per LINE: a non-empty array of
                // objects (the activity `rows` list, the principal directory)
                // becomes `rows[0]`, `rows[1]`, ... so a later entry is never
                // hidden behind the per-cell cap. Any other array keeps the
                // single-cell rendering.
                match item {
                    Json::Array(items)
                        if expand_lists
                            && !items.is_empty()
                            && items.iter().all(Json::is_object) =>
                    {
                        let label = display_cell(field, HUMAN_LABEL_MAX_CHARS);
                        for (index, element) in items.iter().enumerate() {
                            rows.push((
                                display_cell(&format!("{label}[{index}]"), HUMAN_LABEL_MAX_CHARS),
                                human_cell(element),
                            ));
                        }
                    }
                    _ => rows.push((display_cell(field, HUMAN_LABEL_MAX_CHARS), human_cell(item))),
                }
            }
        }
        other => rows.push((String::from("value"), human_cell(other))),
    }
    let name_width = rows
        .iter()
        .map(|(name, _)| name.chars().count())
        .max()
        .unwrap_or(5)
        .max(5);
    let mut table = String::new();
    let _ = writeln!(table, "{:<width$}  value", "field", width = name_width);
    for (name, cell) in &rows {
        let _ = writeln!(table, "{:<width$}  {}", name, cell, width = name_width);
    }
    write_human(stderr, &format!("{}\n", command), &table);
}

/// Stable code plus fixed local message for stderr — never server text,
/// argv, or peer-supplied strings.
fn write_human_error(stderr: &mut dyn Write, err: &ShellError) {
    let body = format!(
        "{:<width$}  {}\n{:<width$}  {}\n",
        "code",
        err.code(),
        "message",
        err.message(),
        width = 9
    );
    write_human(stderr, "error\n", &body);
}

/// Human output never changes the exit code; a stderr write error is dropped.
fn write_human(stderr: &mut dyn Write, head: &str, body: &str) {
    let _ = stderr.write_all(head.as_bytes());
    let _ = stderr.write_all(body.as_bytes());
    let _ = stderr.flush();
}

fn human_cell(item: &Json) -> String {
    match item {
        // Projected strings are already sanitized; never re-escape them
        // (sanitize is injective, not idempotent).
        Json::String(text) => display_cell(text, HUMAN_VALUE_MAX_CHARS),
        other => {
            let rendered =
                serde_json::to_string(other).unwrap_or_else(|_| String::from("<unrenderable>"));
            display_cell(&sanitize(&rendered), HUMAN_VALUE_MAX_CHARS)
        }
    }
}

/// Fixed-width display text for an already-sanitized string: capped in
/// `char`s, and the cut is backed up to a whole escape boundary so a
/// `\uXXXX` sequence is never truncated mid-sequence.
fn display_cell(text: &str, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        return String::from(text);
    }
    let mut cut: String = text.chars().take(max_chars).collect();
    while let Some(pos) = cut.rfind('\\') {
        let suffix = &cut[pos + 1..];
        let mut chars = suffix.chars();
        let partial = matches!(chars.next(), Some('u'))
            && chars.count() < 4
            && suffix[1..].chars().all(|c| c.is_ascii_hexdigit());
        if !partial {
            break;
        }
        cut.truncate(pos);
    }
    cut.push_str("...");
    cut
}

/// Projects a typed server record onto the v1 payload conventions: decimal
/// strings for integer ids/counters, hex for binary, decimal strings for
/// decimals and big integers, explicit tags for non-finite floats, and
/// sanitized strings throughout. Depth-bounded.
pub(super) fn value(item: &QueryValue) -> Result<Json, ShellError> {
    project(item, 0)
}

fn project(item: &QueryValue, depth: usize) -> Result<Json, ShellError> {
    if depth > MAX_PROJECTION_DEPTH {
        return Err(ShellError::ProjectionLimit);
    }
    match item {
        QueryValue::Null => Ok(Json::Null),
        QueryValue::Bool(flag) => Ok(Json::Bool(*flag)),
        QueryValue::Int(number) => Ok(Json::String(number.to_string())),
        QueryValue::F64(number) => {
            if number.is_nan() {
                Ok(tagged_nonfinite("nan"))
            } else if number.is_infinite() {
                Ok(tagged_nonfinite(if *number > 0.0 { "inf" } else { "-inf" }))
            } else {
                Ok(Json::Number(
                    serde_json::Number::from_f64(*number).ok_or(ShellError::ProjectionLimit)?,
                ))
            }
        }
        QueryValue::Dec(number) => Ok(Json::String(number.to_string())),
        QueryValue::Big(number) => Ok(Json::String(number.to_string())),
        QueryValue::Str(text) => Ok(Json::String(sanitize(text))),
        QueryValue::Bin(bytes) => Ok(Json::String(hex::encode(bytes))),
        QueryValue::List(items) => {
            let mut out = Vec::with_capacity(items.len());
            for element in items {
                out.push(project(element, depth + 1)?);
            }
            Ok(Json::Array(out))
        }
        QueryValue::Set(items) => {
            let mut out = Vec::with_capacity(items.len());
            for element in items {
                out.push(project(element, depth + 1)?);
            }
            Ok(Json::Array(out))
        }
        QueryValue::Map(fields) => {
            let mut out = serde_json::Map::new();
            for (key, element) in fields {
                // Keys are unique in the source map, sanitization is
                // injective, and a key that would collide with the reserved
                // non-finite tag is escaped injectively, so no JSON object
                // key collides.
                out.insert(escape_reserved_key(key), project(element, depth + 1)?);
            }
            Ok(Json::Object(out))
        }
    }
}

/// The tag emitted for non-finite floats is reserved: it must never be
/// forgeable by a peer map key. Any raw key that is the reserved key, or
/// that starts with the escape prefix (a literal backslash), is prefixed
/// with a backslash before sanitization. Because sanitization is injective
/// and doubles only a bounded class, the mapping stays injective: a peer
/// key can never produce the escaped reserved form without itself having
/// been the reserved key.
fn escape_reserved_key(key: &str) -> String {
    const RESERVED: &str = "$nonfinite";
    if key == RESERVED || key.starts_with('\\') {
        let mut escaped = String::with_capacity(key.len() + 1);
        escaped.push('\\');
        escaped.push_str(key);
        sanitize(&escaped)
    } else {
        sanitize(key)
    }
}

fn tagged_nonfinite(tag: &'static str) -> Json {
    let mut tagged = serde_json::Map::new();
    tagged.insert(String::from("$nonfinite"), Json::String(String::from(tag)));
    Json::Object(tagged)
}

/// Display-neutral escaping for authorized strings in both streams.
///
/// The backslash is escaped first, which makes the mapping injective: an
/// original literal `\uXXXX` sequence can never be confused with an escape
/// produced here. Escaped as `\uXXXX`: every C0 control, DEL and the C1
/// range (`char::is_control()` covers U+0000–001F and U+007F–009F); the
/// Unicode bidirectional controls U+061C, U+200E, U+200F, U+202A–202E and
/// U+2066–2069; the zero-width format characters U+200B–200D, U+2060–2064
/// and U+FEFF; the line/paragraph separators U+2028 and U+2029. No terminal
/// control, direction switch, or invisible line break survives into either
/// stream.
pub(super) fn sanitize(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for character in text.chars() {
        if character == '\\' || character.is_control() || is_display_control(character) {
            out.push_str(&format!("\\u{:04X}", character as u32));
        } else {
            out.push(character);
        }
    }
    out
}

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

#[cfg(test)]
#[path = "tests/human_rows_tests.rs"]
mod human_rows_tests;

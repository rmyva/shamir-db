//! The typed migration error enum ([`MigrationError`]) and the mapping from
//! server rejection codes and SDK errors into it. Definitive server
//! rejections preserve the stable code and message verbatim; nothing here
//! retries or falls back.

use shamir_query_types::admin::types::schema_migration::{
    SchemaMigrationCanonicalError, SchemaMigrationChecksumValidation, SchemaMigrationError,
    SchemaMigrationErrorCode,
};

use crate::error::ClientError;

use super::resolve::UnknownApply;

/// Typed failure of the application-schema migration client API.
///
/// Server §5.3 stable codes are mapped one-to-one onto variants without
/// losing the server message. An old server that does not know the typed
/// admin operation becomes [`MigrationError::UnsupportedServer`] — there is
/// no fallback to unjournaled DDL on any path.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum MigrationError {
    /// The migration set was rejected locally before anything was sent
    /// (malformed body, duplicate id, gap or non-contiguous ordinals).
    #[error("invalid migration set: {0}")]
    InvalidSet(#[from] SchemaMigrationError),

    /// A canonical body (or its checksum) could not be produced locally.
    #[error("canonical form: {0}")]
    Canonical(#[from] SchemaMigrationCanonicalError),

    /// A transport, timeout, connection or decode failure of a READ-ONLY
    /// call (`status` / `plan` / `resolve_unknown_apply`). No write was
    /// attempted, so the call may simply be repeated.
    #[error(transparent)]
    Client(#[from] ClientError),

    /// The server predates the typed `application_schema` operation
    /// (wire code `invalid_request` with the old peer's decoder text
    /// `Unknown operation type`), or predates R1c step 3a: its status wire
    /// carries neither the full applied prefix nor a top-level fence
    /// generation while the journal is nonempty (or an unresolvable
    /// unresolved event), so the full applied prefix cannot be verified.
    /// Surface a clear unsupported-server
    /// error; never fall back to unjournaled DDL.
    #[error("server does not support application-schema migrations: {message}")]
    UnsupportedServer { message: String },

    /// A definitive server rejection that carries one of the §5.3 stable
    /// codes this client maps explicitly (see the named variants) arrived
    /// with an unrecognized code. The code and message are preserved.
    #[error("server rejected the migration [{code}]: {message}")]
    Server { code: String, message: String },

    /// `schema_migration_in_progress`.
    #[error("schema_migration_in_progress: {message}")]
    InProgress { message: String },

    /// `schema_migration_in_doubt`.
    #[error("schema_migration_in_doubt: {message}")]
    InDoubt { message: String },

    /// `schema_migration_checksum_mismatch`.
    #[error("schema_migration_checksum_mismatch: {message}")]
    ChecksumMismatch { message: String },

    /// `schema_migration_gap`.
    #[error("schema_migration_gap: {message}")]
    Gap { message: String },

    /// `schema_migration_predecessor_mismatch`.
    #[error("schema_migration_predecessor_mismatch: {message}")]
    PredecessorMismatch { message: String },

    /// `schema_migration_follower_read_only`.
    #[error("schema_migration_follower_read_only: {message}")]
    FollowerReadOnly { message: String },

    /// `schema_migration_durability_unavailable`.
    #[error("schema_migration_durability_unavailable: {message}")]
    DurabilityUnavailable { message: String },

    /// `schema_migration_postcondition_failed`.
    #[error("schema_migration_postcondition_failed: {message}")]
    PostconditionFailed { message: String },

    /// `schema_migration_identity_mismatch`: the journal's recorded database
    /// identity does not match the serving database (possible after an
    /// import under a different name or a restore that mixed catalogue
    /// states).
    #[error("schema_migration_identity_mismatch: {message}")]
    IdentityMismatch { message: String },

    /// The server refused with the
    /// `schema_migration_operation_deferred:` prefix (an admitted operation
    /// cannot be validated safely right now).
    #[error("{message}")]
    DeferredOperation { message: String },

    /// The response shape (protocol version, target echo, record layout)
    /// is not what this client understands. Fail closed.
    #[error("unexpected response shape: {message}")]
    ShapeMismatch { message: String },

    /// The journal reports applied history whose persisted checksum the
    /// server could not verify (`checksum_validation` other than `valid`).
    /// Fail closed in the status preflight, before any apply send.
    #[error("journal checksum could not be verified ({validation:?}); refusing to plan or apply")]
    JournalUnverified {
        validation: SchemaMigrationChecksumValidation,
    },

    /// The local body at an applied journal ordinal differs from the
    /// applied journal entry (id or canonical checksum): DRIFT. Refused
    /// before apply submission, after the status preflight.
    #[error(
        "drift at ordinal {ordinal}: journal has {journal_id} / {journal_checksum} but the local set has {local_id}"
    )]
    Drift {
        ordinal: u64,
        journal_id: String,
        /// Lowercase-hex canonical checksum from the journal.
        journal_checksum: String,
        local_id: String,
    },

    /// The journal head is beyond the local set: the compiled set is
    /// SHORTER than the applied journal. Refused before any apply send.
    #[error("journal head ordinal {ordinal} is beyond the local migration set")]
    JournalBeyondSet { ordinal: u64 },

    /// Every migration in the local set is already applied; there is
    /// nothing to plan or apply. Raised before any apply send.
    #[error("nothing to apply: every migration in the set is already applied")]
    NothingToApply,

    /// The attempt token does not describe a body of the given set (id,
    /// ordinal or checksum differ): the resolution would not be bound to
    /// the identical body, so it fails closed before any send.
    #[error(
        "attempt token for {migration_id} (ordinal {ordinal}) does not match the migration set"
    )]
    AttemptDoesNotMatchSet { migration_id: String, ordinal: u64 },

    /// An apply was sent and its outcome is UNKNOWN (request timeout,
    /// transport failure, closed connection, undecodable response).
    /// Never resend: call
    /// [`Client::resolve_unknown_apply`](crate::Client::resolve_unknown_apply)
    /// with the opaque `attempt` token and follow the typed resolution.
    #[error("apply outcome unknown; query status with the attempt token (detail: {detail})")]
    UnknownOutcome {
        attempt: Box<UnknownApply>,
        detail: String,
    },
}

impl MigrationError {
    /// Map a definitive server rejection (`DbResponse::Error` surfaced as
    /// [`ClientError::Db`]) into the typed enum, preserving code+message and
    /// narrowly recognizing the old peer's unsupported-operation decoder
    /// text rather than every `invalid_request`.
    pub(super) fn from_server(code: &str, message: &str) -> Self {
        // The old peer's decoder reports the unknown typed operation with
        // exactly this message (its Syntax Display passes it through
        // unchanged); match it exactly, not every `invalid_request`.
        if code == "invalid_request" && message == "Unknown operation type" {
            return Self::UnsupportedServer {
                message: message.to_string(),
            };
        }
        if is_deferred_message(code, message) {
            return Self::DeferredOperation {
                message: message.to_string(),
            };
        }
        let message = message.to_string();
        match code {
            "schema_migration_in_progress" => Self::InProgress { message },
            "schema_migration_in_doubt" => Self::InDoubt { message },
            "schema_migration_checksum_mismatch" => Self::ChecksumMismatch { message },
            "schema_migration_gap" => Self::Gap { message },
            "schema_migration_predecessor_mismatch" => Self::PredecessorMismatch { message },
            "schema_migration_follower_read_only" => Self::FollowerReadOnly { message },
            "schema_migration_durability_unavailable" => Self::DurabilityUnavailable { message },
            "schema_migration_postcondition_failed" => Self::PostconditionFailed { message },
            "schema_migration_identity_mismatch" => Self::IdentityMismatch { message },
            _ => Self::Server {
                code: code.to_string(),
                message,
            },
        }
    }

    /// Lowercase hex of a checksum, used in drift diagnostics.
    pub(super) fn hex_checksum(checksum: &[u8]) -> String {
        checksum.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    /// Map the error of an ALREADY-SENT apply. A definitive server
    /// rejection keeps its typed code and message; anything else (timeout,
    /// transport, closed connection, decode) leaves the effect UNKNOWN and
    /// carries the attempt token for `resolve_unknown_apply`. The
    /// post-execution replacement errors the server can swap in for a
    /// final response (`result_too_large`, `timeout`, `limits`) are also
    /// UNKNOWN: the batch may already have taken effect, so they are never
    /// treated as definitive refusals.
    pub(super) fn from_apply_send_error(e: ClientError, attempt: UnknownApply) -> Self {
        match &e {
            ClientError::Db { code, .. }
                if matches!(code.as_str(), "result_too_large" | "timeout" | "limits") =>
            {
                Self::UnknownOutcome {
                    attempt: Box::new(attempt),
                    detail: e.to_string(),
                }
            }
            ClientError::Db { code, message } => Self::from_server(code, message),
            _ => Self::UnknownOutcome {
                attempt: Box::new(attempt),
                detail: e.to_string(),
            },
        }
    }

    /// Map an SDK error of a read-only call. Server rejections keep their
    /// typed mapping; everything else is the plain transport failure.
    pub(super) fn from_read(e: ClientError) -> Self {
        match &e {
            ClientError::Db { code, message } => Self::from_server(code, message),
            _ => Self::Client(e),
        }
    }

    /// Map the typed `error` code echoed inside a status/apply result
    /// record (not an envelope-level rejection).
    pub(super) fn from_result_code(code: SchemaMigrationErrorCode, message: String) -> Self {
        match code {
            SchemaMigrationErrorCode::SchemaMigrationInProgress => Self::InProgress { message },
            SchemaMigrationErrorCode::SchemaMigrationInDoubt => Self::InDoubt { message },
            SchemaMigrationErrorCode::SchemaMigrationChecksumMismatch => {
                Self::ChecksumMismatch { message }
            }
            SchemaMigrationErrorCode::SchemaMigrationGap => Self::Gap { message },
            SchemaMigrationErrorCode::SchemaMigrationPredecessorMismatch => {
                Self::PredecessorMismatch { message }
            }
            SchemaMigrationErrorCode::SchemaMigrationFollowerReadOnly => {
                Self::FollowerReadOnly { message }
            }
            SchemaMigrationErrorCode::SchemaMigrationDurabilityUnavailable => {
                Self::DurabilityUnavailable { message }
            }
            SchemaMigrationErrorCode::SchemaMigrationPostconditionFailed => {
                Self::PostconditionFailed { message }
            }
            SchemaMigrationErrorCode::SchemaMigrationIdentityMismatch => {
                Self::IdentityMismatch { message }
            }
        }
    }
}

/// The exact raw prefix the deferred admission carries (design §5.3).
const DEFERRED_PREFIX: &str = "schema_migration_operation_deferred:";

/// True when `(code, message)` is the server's deferred admission, in
/// EXACTLY one of its two observed shapes — never a broad substring match:
///
/// * RAW: the message itself starts with
///   `schema_migration_operation_deferred:` (any code), or
/// * WRAPPED: the code is the coarse `query` and the message is the
///   `BatchError::QueryError` Display of a deferred admission,
///   `Query '<alias>' failed: schema_migration_operation_deferred: …`
///   (shamir-query-types `batch_error.rs`), with the alias possibly empty.
fn is_deferred_message(code: &str, message: &str) -> bool {
    if message.starts_with(DEFERRED_PREFIX) {
        return true;
    }
    if code != "query" {
        return false;
    }
    let Some(after_open) = message.strip_prefix("Query '") else {
        return false;
    };
    let Some((_, remainder)) = after_open.split_once("' failed: ") else {
        return false;
    };
    remainder.starts_with(DEFERRED_PREFIX)
}

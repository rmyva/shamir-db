//! Error types for the connection protocol.
//!
//! Errors are deliberately structured but follow the spec's privacy rules:
//! anything sent on the wire to a peer collapses to a generic
//! [`AuthFailed`](Error::AuthFailed). Internal variants exist for callers
//! that need to log / audit / branch on real cause.

use super::admit::AdmitReject;
use thiserror::Error;

/// Result alias for connection protocol operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Environmental failures that cannot safely be substituted with defaults.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum EnvironmentError {
    /// The operating system could not supply entropy.
    #[error("OS entropy unavailable")]
    Entropy,
    /// System time predates the Unix epoch.
    #[error("system clock before unix epoch")]
    ClockBeforeEpoch,
    /// Unix nanoseconds cannot fit in u64.
    #[error("system clock exceeds unix nanosecond range")]
    ClockOverflow,
}

/// Connection protocol errors.
#[derive(Debug, Error)]
pub enum Error {
    /// Environmental entropy or clock failure; never exposed to peers.
    #[error(transparent)]
    Environment(#[from] EnvironmentError),

    /// Generic authentication failure — what the wire sees.
    /// All real failure causes (unknown user, bad password, bad proof, lockout)
    /// collapse to this in client-visible responses (spec §14.1).
    #[error("authentication_failed")]
    AuthFailed,

    /// Server proof verification failed on the client side (spec §14.2).
    #[error("server_authentication_failed")]
    ServerAuthFailed,

    /// Ed25519 identity signature verification failed (spec §14.2).
    #[error("server_signature_invalid")]
    ServerSignatureInvalid,

    /// TOFU pin mismatch — server identity does not match pinned hash (§14.2).
    #[error("server_identity_changed")]
    ServerIdentityChanged,

    /// KDF parameters from server exceed local hard limits (spec §5.1.1, §14.2).
    #[error("kdf_params_rejected")]
    KdfParamsRejected,

    /// known_hosts file integrity tag mismatch (spec §14.2).
    #[error("known_hosts_integrity_failed")]
    KnownHostsIntegrityFailed,

    /// Rate limit hit (server-side response, spec §14.3).
    #[error("rate_limited")]
    RateLimited {
        /// Recommended retry delay in seconds.
        retry_after: u32,
    },

    /// Server too busy (spec §14.3 wire variant, kept for protocol
    /// compatibility). No live native auth-phase handler produces it: native
    /// pre-auth throttling records `auth_attempts_total{result="rate_limited"}`
    /// and closes without a body, and an ordinary SCRAM login derives no
    /// server-side password key, so nothing on that path waits for a permit.
    /// The HTTP login admission endpoint has its own `server_busy` 503 body, and
    /// server-side password-derivation pressure surfaces as the `server_busy`
    /// error code of a rejected user-creation operation (no `retry_after`).
    #[error("server_busy")]
    ServerBusy {
        /// Recommended retry delay.
        retry_after: u32,
    },

    /// Protocol version negotiation failed (§14.3).
    #[error("unsupported_version")]
    UnsupportedVersion,

    /// The peer closed or rejected before a challenge, possibly speaking another handshake version.
    #[error("server may speak another handshake version")]
    HandshakeVersionMismatch,

    /// Challenge fields do not conform to the positional wire layout.
    #[error("invalid challenge layout")]
    ChallengeLayout,

    /// Bootstrap handshake failed — generic (§14.3).
    #[error("bootstrap_failed")]
    BootstrapFailed,

    /// Invalid input violating spec invariants (e.g. nonce wrong length,
    /// username too long after NFC, all-zeros nonce).
    #[error("invalid input: {0}")]
    InvalidInput(&'static str),

    /// The named user does not exist (internal admin-operation classification).
    #[error("user not found")]
    UserNotFound,
    /// Username failed PRECIS UsernameCaseMapped + NFC validation (spec §15.3).
    #[error("invalid username: {0}")]
    InvalidUsername(&'static str),

    /// Password violates policy — length out of range, forbidden pattern (§3.2).
    #[error("invalid password: {0}")]
    InvalidPassword(&'static str),

    /// Crypto primitive error (Argon2id failure, AES-GCM tag mismatch, etc).
    #[error("crypto: {0}")]
    Crypto(&'static str),

    /// Serialization / deserialization failure.
    #[error("encoding: {0}")]
    Encoding(String),

    /// Durable storage failure (e.g. consumed-counter persist,
    /// RV-CONNECT-ECE3E157). Server-internal: `to_wire` collapses it to
    /// `AuthFailed`.
    #[error("storage_failure")]
    StorageFailed,

    /// Session admission rejected by the per-user cap (RV-CONNECT-0A42F42C /
    /// RV-CONNECT-8AD8B1C8): all sessions active, or sid bound to another
    /// user. Carries the audited username so the RESUME rejection path can
    /// emit the same `auth_failed` audit event as login (the login path has
    /// its own username in scope). Server-internal only: `to_wire` collapses
    /// it to `AuthFailed`.
    #[error("admission_rejected")]
    AdmissionRejected {
        /// Why admission was rejected (internal audit reason only).
        reason: AdmitReject,
        /// Audited username (post-NFC) of the rejected session.
        username: String,
    },
}

impl Error {
    /// Returns the wire-safe variant: any internal failure cause is collapsed
    /// to [`Self::AuthFailed`] for client-visible responses (spec §14.4).
    ///
    /// RV-CONNECT-94E37810 decision: this helper is the **normative §14
    /// collapse-set oracle**, enforced by the exhaustive variant test in
    /// `common/tests/types_tests/error_tests.rs` (a new variant that is not
    /// explicitly classified there breaks the build). Production boundaries
    /// (e.g. `server/state/resume.rs`) keep their explicit
    /// `map_err(|_| Error::AuthFailed)` calls instead of routing through
    /// this helper: most of those sites map from non-`Error` error types
    /// (ticket/cipher/storage failures), so there is no `Error` value to
    /// call `to_wire` on without adding an intermediate allocation. The
    /// production match above is exhaustive over every variant, so a new
    /// variant forces an explicit classification decision here at compile
    /// time.
    pub fn to_wire(&self) -> Error {
        match self {
            Error::RateLimited { retry_after } => Error::RateLimited {
                retry_after: *retry_after,
            },
            Error::ServerBusy { retry_after } => Error::ServerBusy {
                retry_after: *retry_after,
            },
            Error::UnsupportedVersion => Error::UnsupportedVersion,
            Error::HandshakeVersionMismatch => Error::HandshakeVersionMismatch,
            Error::ChallengeLayout => Error::ChallengeLayout,
            Error::BootstrapFailed => Error::BootstrapFailed,
            Error::AuthFailed => Error::AuthFailed,
            // Internal failure causes: explicitly collapsed to the generic
            // wire error (spec §14.4). Exhaustive — a new variant that is not
            // classified here fails to compile.
            Error::Environment(_) => Error::AuthFailed,
            Error::ServerAuthFailed => Error::AuthFailed,
            Error::ServerSignatureInvalid => Error::AuthFailed,
            Error::ServerIdentityChanged => Error::AuthFailed,
            Error::KdfParamsRejected => Error::AuthFailed,
            Error::KnownHostsIntegrityFailed => Error::AuthFailed,
            Error::InvalidInput(_) => Error::AuthFailed,
            Error::UserNotFound => Error::AuthFailed,
            Error::InvalidUsername(_) => Error::AuthFailed,
            Error::InvalidPassword(_) => Error::AuthFailed,
            Error::Crypto(_) => Error::AuthFailed,
            Error::Encoding(_) => Error::AuthFailed,
            Error::StorageFailed => Error::AuthFailed,
            Error::AdmissionRejected { .. } => Error::AuthFailed,
        }
    }
}

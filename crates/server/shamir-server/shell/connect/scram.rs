//! Native TLS-TCP, WSS, and IPC SCRAM through the existing
//! SDK (`shamir-client`), verified-pin only
//! (`docs/dev-artifacts/design/admin-shell.md` §3.2 R5a PR1, §6).
//!
//! Contract with the shell runner/parser worker:
//! * [`Connection::prepare`] validates a loaded [`Profile`] **before any
//!   secret is read or any dial is attempted**; every unsupported transport,
//!   plaintext alias, deferral, resume marker, missing pin and incompatible
//!   profile field is refused here as a typed [`ShellError`] (exit 6 rows of
//!   the §3.2 table), nothing is ever sent.
//! * [`Connection::connect`] dials with absolute deadlines: one 10 s
//!   budget covering DNS and the TCP connect (or the IPC dial) and a
//!   separate 30 s budget for the whole auth (TLS + SCRAM). ONE resolved
//!   address is used — no fallback address and no retry after any
//!   failure. A bad password/proof surfaces as the typed peer EOF at the
//!   `read auth_ok` phase (the server closes without a wire error) and
//!   [`request_error`] maps it to [`ShellError::Authentication`].
//! * [`REQUEST_TIMEOUT`] is the 60 s outer per-command deadline the runner
//!   applies and that is configured into the SDK's `request_timeout` (send +
//!   wait); [`request_error`] maps every SDK error to the bounded typed
//!   [`ShellError`] set — never a verbatim server string, never argv, never
//!   handshake metadata.
//!
//! No plaintext path, no resume, no `--accept-new-host`, no CA bypass, no
//! fallback, no unbounded wait: `accept_new_host` is always `false` and the
//! trusted pin is always `Some`.

use std::time::Duration;

use zeroize::Zeroizing;

use shamir_client::{Client, ClientError, CloseCause, ConnectLocalOptions, ConnectOptions};
use shamir_connect::Error as ConnectError;
use shamir_transport_ipc::validate_endpoint;
use shamir_transport_tcp::framing::FrameError;
use tokio_tungstenite::tungstenite::Error as WsError;

use super::profile::{Profile, Transport as ProfileTransport};
use crate::shell::output::ShellError;

/// Outer per-command deadline: 60 s per command including send and wait,
/// configured as the SDK `request_timeout` AND applied by the runner around
/// each command (design §3.2 timeouts, §6 outcome-after-send rules).
pub(in crate::shell) const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

/// ONE absolute deadline covering DNS resolution AND the TCP connect (or
/// the IPC dial). Never re-armed per attempt.
const DIAL_TIMEOUT: Duration = Duration::from_secs(10);

/// ONE absolute deadline for the whole auth step: TLS + SCRAM (or the IPC
/// SCRAM frames), started at dial begin.
const CONNECT_TOTAL: Duration = Duration::from_secs(30);

/// SCRAM transport selected during [`Connection::prepare`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Transport {
    /// T1: fresh SCRAM over TLS TCP, application-pin validated.
    TlsTcp,
    NativeWss,
    /// T4: fresh SCRAM over local IPC (Unix socket / named pipe), no TLS.
    Ipc,
}

/// A validated, ready-to-dial connection target. Built only by
/// [`Connection::prepare`] from an already-loaded [`Profile`]; carries no
/// secret — the password enters separately in [`Connection::connect`].
#[derive(Debug, Clone)]
pub(in crate::shell) struct Connection {
    transport: Transport,
    /// TLS only: host as written in the profile (name or IP literal), the
    /// DNS lookup key and the SNI default.
    host: String,
    /// TLS only: required, nonzero port.
    port: u16,
    /// TLS only: SNI host (profile value, else the profile host).
    sni: String,
    /// IPC only: raw platform endpoint, already `validate_endpoint`-checked.
    endpoint: String,
    username: String,
    /// Verified `SHA256(server_pub_key)` pin from the profile; `prepare`
    /// refuses with [`ShellError::MissingPin`] when absent.
    pin: [u8; 32],
}

impl Connection {
    /// Validate a profile into a dial target **before any secret or dial**:
    /// the transport is classified first (§3.2 R5a refusals), then the pin
    /// and the transport-specific address/fields. Incompatible fields (token
    /// CA path on a SCRAM transport, SNI on IPC) are rejected, never ignored.
    pub(in crate::shell) fn prepare(profile: &Profile) -> Result<Connection, ShellError> {
        let transport = match profile.transport_kind()? {
            ProfileTransport::TlsTcp => Transport::TlsTcp,
            ProfileTransport::Ipc => Transport::Ipc,
            ProfileTransport::NativeWss => Transport::NativeWss,
            ProfileTransport::Plaintext => return Err(ShellError::Plaintext),
            ProfileTransport::TokenRest | ProfileTransport::TokenWss => {
                return Err(ShellError::Unsupported);
            }
        };

        // Verified pin only (§3.2, §6): missing pin is a hard refusal and
        // the stored value must be a 64-hex-digit SHA256 capture.
        let pin = match profile.pin.as_deref() {
            None => return Err(ShellError::MissingPin),
            Some(pin) => decode_pin(pin).ok_or(ShellError::InvalidProfile)?,
        };

        // The CA path is a token-transport field (§3.2 token TLS): on a
        // SCRAM transport it is a profile error, never an ignored option.
        if profile.ca_path.is_some() {
            return Err(ShellError::InvalidProfile);
        }

        let username = profile.username.clone().ok_or(ShellError::InvalidProfile)?;
        match transport {
            Transport::TlsTcp | Transport::NativeWss => {
                let (host, port) = profile.tls_address()?;
                // SNI defaults to the profile host when absent.
                let sni = match profile.sni.as_deref() {
                    Some(sni) if !sni.trim().is_empty() => sni.trim().to_string(),
                    _ => host.clone(),
                };
                Ok(Connection {
                    transport,
                    host,
                    port,
                    sni,
                    endpoint: String::new(),
                    username,
                    pin,
                })
            }
            Transport::Ipc => {
                // IPC has no TLS: SNI is meaningless — reject it rather than
                // silently ignore a configured field.
                if profile.sni.is_some() {
                    return Err(ShellError::InvalidProfile);
                }
                // The existing platform endpoint validator runs before any
                // dial; its diagnostics are local, never peer text.
                validate_endpoint(profile.address.trim())
                    .map_err(|_| ShellError::InvalidProfile)?;
                Ok(Connection {
                    transport,
                    host: String::new(),
                    port: 0,
                    sni: String::new(),
                    endpoint: profile.address.trim().to_string(),
                    username,
                    pin,
                })
            }
        }
    }

    /// Dial and run the fresh SCRAM handshake. Absolute deadlines: 10 s
    /// for dial (DNS + TCP, or the IPC dial), 30 s for the whole auth.
    /// ONE resolved address, no fallback, no retry. Errors stay typed
    /// [`ClientError`] — the caller maps them through [`request_error`].
    pub(in crate::shell) async fn connect(
        self,
        password: Zeroizing<Vec<u8>>,
    ) -> Result<Client, ClientError> {
        match self.transport {
            Transport::TlsTcp | Transport::NativeWss => self.connect_network(password).await,
            Transport::Ipc => self.connect_ipc(password).await,
        }
    }

    /// TLS TCP / native WSS: ONE absolute 10 s budget covers the bounded
    /// `lookup_host` (IPv6-capable tuple form) AND the TCP connect; the
    /// absolute 30 s budget covers TLS + SCRAM. Only the FIRST resolved
    /// address is used — no fallback address, no retry. The password is
    /// moved into the options, never cloned.
    async fn connect_network(self, password: Zeroizing<Vec<u8>>) -> Result<Client, ClientError> {
        let native_wss = self.transport == Transport::NativeWss;
        let start = tokio::time::Instant::now();
        let dial_deadline = start + DIAL_TIMEOUT;
        let auth_deadline = start + CONNECT_TOTAL;
        let lookup = tokio::net::lookup_host((self.host.as_str(), self.port));
        let addr = tokio::time::timeout_at(dial_deadline, lookup)
            .await
            // Dial budget (DNS included) exhausted.
            .map_err(|_| ClientError::ConnectTimeout(DIAL_TIMEOUT))?
            .map_err(ClientError::from)?
            .next()
            .ok_or_else(|| {
                ClientError::Io(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "host resolved to no addresses",
                ))
            })?;
        let opts = ConnectOptions {
            addr,
            server_name: self.sni,
            username: self.username,
            password,
            // Verified-pin only; enrollment is a profile step (§6).
            accept_new_host: false,
            trusted_pin: Some(self.pin),
            connect_timeout: Some(
                dial_deadline.saturating_duration_since(tokio::time::Instant::now()),
            ),
            handshake_timeout: Some(CONNECT_TOTAL),
            request_timeout: Some(REQUEST_TIMEOUT),
        };
        // Absolute 30 s backstop around the whole auth; the SDK's own
        // `HandshakeTimeout` (same budget, started at handshake begin) is
        // the typed primary outcome, this only fires if it cannot.
        tokio::time::timeout_at(auth_deadline, async {
            if native_wss {
                Client::connect_ws(opts).await
            } else {
                Client::connect(opts).await
            }
        })
        .await
        .map_err(|_| ClientError::HandshakeTimeout(CONNECT_TOTAL, "scram".into()))?
    }

    /// IPC path: the endpoint was validated in `prepare`; a 10 s dial
    /// deadline inside the absolute 30 s auth budget (the SDK's local
    /// options carry no handshake deadline, so the outer bound here covers
    /// the SCRAM frames). One dial, no retry.
    async fn connect_ipc(self, password: Zeroizing<Vec<u8>>) -> Result<Client, ClientError> {
        let auth_deadline = tokio::time::Instant::now() + CONNECT_TOTAL;
        let opts = ConnectLocalOptions {
            addr: self.endpoint,
            username: self.username,
            password,
            accept_new_host: false,
            trusted_pin: Some(self.pin),
            connect_timeout: Some(DIAL_TIMEOUT),
            request_timeout: Some(REQUEST_TIMEOUT),
        };
        tokio::time::timeout_at(auth_deadline, Client::connect_local(opts))
            .await
            .map_err(|_| ClientError::HandshakeTimeout(CONNECT_TOTAL, "scram".into()))?
    }
}

/// Free-function alias of [`Connection::prepare`] — the parser-facing
/// form of the pre-dial validation seam.
pub(in crate::shell) fn prepare(profile: &Profile) -> Result<Connection, ShellError> {
    Connection::prepare(profile)
}

/// Map a completed-request or handshake [`ClientError`] onto the bounded
/// typed [`ShellError`] set for `ping` and the catalogue commands (and for
/// connect outcomes). Only stable, allowlisted server codes are recognised —
/// the server's message text and any other peer-supplied string are dropped,
/// never reproduced (§5 redaction; §6 typed denials).
pub(in crate::shell) fn request_error(error: ClientError) -> ShellError {
    match error {
        // Sent and no response within the deadline: outcome unknown (§6).
        ClientError::RequestTimeout(_) => ShellError::Timeout,
        // A bad password or proof makes the SERVER close the wire without
        // any error frame (server handshake: the proof-rejection path runs
        // its bounded close/shutdown and returns), so the client observes a
        // typed peer EOF/close at the `read auth_ok` phase. That typed EOF
        // IS the credentials outcome — Authentication, not a transport
        // outage. Any other transport phase stays a connection failure.
        ClientError::Transport {
            context: "read auth_ok",
            source: FrameError::PeerEof | FrameError::PeerClose,
        } => ShellError::Authentication,
        // Identity rotation pending: hard shell-policy refusal (§3.2/§6).
        ClientError::IdentityRotationPending { .. } => ShellError::PinMismatch,
        // Native WSS upgrade rejected BEFORE SCRAM: dial_ws.rs wraps the
        // typed tungstenite error under `Handshake.source` — a connection
        // failure, not an authentication failure. Real SCRAM failures
        // carry a `ConnectError` source and fall through to the generic
        // `Handshake` arm below.
        ClientError::Handshake {
            source: Some(source),
            ..
        } if source.is::<WsError>() => ShellError::Connection,
        // SCRAM/KDF stage failure; classify by the typed source when one
        // exists (bad password surfaces the server's wire `AuthFailed`
        // wrapped here — pre-auth framing, auth_ok.rs wraps the typed
        // shamir-connect error under `Handshake.source`).
        ClientError::Handshake { source, .. } => handshake_source(source),
        // Connect-side timeouts are pre-send: connection failure (exit 2),
        // never the post-send unknown-outcome code.
        ClientError::ConnectTimeout(_) | ClientError::HandshakeTimeout(..) => {
            ShellError::Connection
        }
        ClientError::ConnectionClosed(CloseCause::PeerClosed {
            code: 4010..=4012, ..
        }) => ShellError::Authentication,
        // Transport/TLS framing failures, typed and local.
        ClientError::Io(_)
        | ClientError::Tls { .. }
        | ClientError::Transport { .. }
        | ClientError::ConnectionClosed(_) => ShellError::Connection,
        // Typed replication-quarantine refusal: the SDK's response mapper
        // rewrites the server's stable `resync_required` code into this
        // variant BEFORE the shell sees it, so the `Db` code match below
        // never fires for it — classify the variant directly (the typed
        // `resync_required` outcome, exit 5).
        ClientError::ResyncRequired { .. } => ShellError::ResyncRequired,
        // Envelope or DB error: only the allowlisted stable codes are
        // classified; the code/message strings are never carried onward.
        ClientError::Server { code } | ClientError::Db { code, .. } => server_code(&code),
        // Profile-shaped input problem (username normalisation).
        ClientError::InvalidUsername(_) => ShellError::InvalidProfile,
        // Local request-construction failures: server-side classification
        // (exit 5), no detail forwarded.
        ClientError::Protocol(_)
        | ClientError::Encode(_)
        | ClientError::Decode(_)
        | ClientError::RequestIdMismatch { .. }
        | ClientError::TransactionOutcome(_)
        | ClientError::AsOfVisibilityTimeout { .. }
        | ClientError::AsOfFutureVersion { .. }
        | ClientError::AsOfReopenRequired { .. }
        | ClientError::VisibilityFutureVersion { .. }
        | ClientError::VisibilityWaitInvalid { .. }
        | ClientError::WriteCommittedVisibilityPending { .. }
        | ClientError::IdEncodingWithRefs { .. } => ShellError::Server,
        // REST and token upgrade errors use status for generic auth/policy
        // failures. Structured 400/403 denials reach the Db arm above.
        ClientError::InvalidToken | ClientError::Http { status: 401, .. } => {
            ShellError::Authentication
        }
        ClientError::Http { status: 403, .. } => ShellError::Denied,
        // REST server answered 408 AFTER the request was dispatched:
        // post-send timeout, outcome unknown (§6) — not generic exit 5.
        ClientError::Http { status: 408, .. } => ShellError::Timeout,
        ClientError::Http { .. } => ShellError::Server,
    }
}

/// Classify a handshake failure by its typed shamir-connect source. The
/// context string is dropped — only the variant decides.
fn handshake_source(source: Option<Box<dyn std::error::Error + Send + Sync>>) -> ShellError {
    let Some(source) = source else {
        return ShellError::Authentication;
    };
    match source.downcast::<ConnectError>() {
        Ok(error) => match &*error {
            // The wire authentication failure (unknown user, bad password,
            // bad proof, lockout — spec §14.1 collapse set).
            ConnectError::AuthFailed => ShellError::Authentication,
            // Client-side server-identity/proof failures: pin-class hard
            // refusals (mismatch semantics, §3.2/§6).
            ConnectError::ServerIdentityChanged
            | ConnectError::ServerAuthFailed
            | ConnectError::ServerSignatureInvalid => ShellError::PinMismatch,
            // Server-side capacity/version conditions, typed as server
            // errors; the retry_after values are never surfaced.
            ConnectError::RateLimited { .. } | ConnectError::ServerBusy { .. } => {
                ShellError::Server
            }
            ConnectError::UnsupportedVersion => ShellError::Unsupported,
            // Every other handshake cause (KDF limits, input, storage, …)
            // stays inside the authentication/connection boundary without
            // forwarding its text.
            _ => ShellError::Authentication,
        },
        Err(_) => ShellError::Authentication,
    }
}

/// Allowlisted stable server codes (§5 bounded mapping). Anything else is a
/// generic server error — the arbitrary code string is never reproduced.
fn server_code(code: &str) -> ShellError {
    match code {
        "permission_denied" | "access_denied" | "token_database_denied" | "token_read_only"
        // REST-only token used on the token WSS upgrade: the server's
        // typed 403 `token_channel_denied` decoded by the SDK as `Db`
        // (transport/token/status.rs) — permission refusal (§5 exit 4).
        | "token_channel_denied" => ShellError::Denied,
        "token_request_denied" => ShellError::TokenRequestDenied,
        // Typed P2 point-read miss (`DbResponse::Error` code `not_found`).
        "not_found" => ShellError::NotFound,
        "not_supported" => ShellError::Unsupported,
        // Typed ACL-mutation refusals (§4.5/§5): each keeps its own stable
        // code and a fixed local message; the server text is never echoed.
        "invalid_owner" => ShellError::InvalidOwner,
        "hmac_mismatch" => ShellError::HmacMismatch,
        "hmac_required" => ShellError::HmacRequired,
        // The addressed database does not exist (the mutation path reports this
        // instead of a typed "not found" when the caller is an admin).
        "unknown_db" => ShellError::UnknownDb,
        // Session-level invalidation after a successful connect: the
        // authenticated session is gone; re-login required.
        "session_expired" | "session_invalidated" | "token_revoked" | "token_expired" => {
            ShellError::Authentication
        }
        // Typed bounded server conditions (§5 server-error class, exit 5,
        // each with its own stable code and fixed local message; the
        // arbitrary server text is never echoed):
        // missing table, replication quarantine, oversized result.
        "unknown_table" => ShellError::UnknownTable,
        "resync_required" => ShellError::ResyncRequired,
        "result_too_large" => ShellError::ResultTooLarge,
        // Read-only follower write refusal (§6 "follower behavior": the
        // stable diagnostic is surfaced, never retried or redirected).
        "read_only_replica" => ShellError::ReadOnlyReplica,
        // Online-copy and schema-migration refusals: each keeps its own
        // stable code and fixed local message; the server text is never
        // echoed.
        "backup_already_running" => ShellError::BackupAlreadyRunning,
        "backup_invalid_destination" => ShellError::BackupInvalidDestination,
        "backup_unavailable" => ShellError::BackupUnavailable,
        "backup_io_error" => ShellError::BackupIo,
        "online_backup_follower_read_only" => ShellError::OnlineBackupFollowerReadOnly,
        "online_export_already_running" => ShellError::ExportBusy,
        "export_scope_denied" => ShellError::Denied,
        // Invalid form/scope, unsupported scope, unavailable scope, and the
        // internal codec error deliberately use the fixed generic server outcome.
        "online_export_invalid_scope" | "online_export_unsupported" | "online_export_scope_unavailable"
        | "online_export_invalid_form" | "internal_error" => ShellError::Server,
        "online_export_unavailable" | "online_export_not_configured" => ShellError::ExportUnavailable,
        "online_export_not_found" => ShellError::ExportNotFound,

        "schema_migration_follower_read_only" => ShellError::SchemaMigrationFollowerReadOnly,
        // The VICTIM side of a cooperative cancellation: this session's own
        // batch was cancelled by another operator. Its effects may be partial,
        // so it is its own typed refusal, not a generic server failure.
        "cancelled" => ShellError::Cancelled,
        _ => ShellError::Server,
    }
}

/// Decode the profile pin: exactly 64 hex digits = `SHA256(server_pub_key)`.
fn decode_pin(pin: &str) -> Option<[u8; 32]> {
    let pin = pin.trim();
    if pin.len() != 64 || !pin.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    let mut out = [0u8; 32];
    for (index, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&pin[index * 2..index * 2 + 2], 16).ok()?;
    }
    Some(out)
}

#[cfg(test)]
#[path = "tests/error_mapping.rs"]
mod error_mapping_tests;

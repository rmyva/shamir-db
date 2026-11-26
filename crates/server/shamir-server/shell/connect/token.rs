//! Token sessions use only the SDK's verified HTTPS and native token WSS paths.

#[cfg(not(unix))]
use std::fs;
use std::fs::File;
use std::future::Future;
use std::io::Read;
use std::path::Path;
use std::time::Duration;

#[cfg(unix)]
use rustix::fs::{open, Mode, OFlags};
use serde_json::Value;
use shamir_client::{
    ApiToken, Client, ClientError, ExportCancelReceipt, ExportStartReceipt, ExportStatusReceipt,
    TokenEndpoint, TokenHttpClient, TokenHttpOptions, TokenTrust, TokenWsOptions,
};
use shamir_query_types::admin::activity::{
    ActivityPage, CancelOutcome, ListActivityReq, RequestExecId,
};
use shamir_query_types::admin::ResourceRef;
use shamir_query_types::batch::{BatchRequest, BatchResponse};
use tokio::time::{timeout_at, Instant};
use zeroize::Zeroizing;

use super::profile::{Profile, Transport};
use super::scram::{request_error, REQUEST_TIMEOUT};
use crate::shell::output::ShellError;

const DIAL_TIMEOUT: Duration = Duration::from_secs(10);
const CONNECT_TOTAL: Duration = Duration::from_secs(30);
const MAX_CA_BYTES: u64 = 1024 * 1024;

/// §3.2 phase budgets for one token connect, both ABSOLUTE from the start
/// (matching the SCRAM outer horizon `start + 30 s`; the total cap never
/// widens): DNS + TCP dial by `start + 10 s`, TLS + authentication — WSS
/// upgrade + bind, or HTTPS establishment with the folded explicit
/// `connect` Ping — by `start + 30 s`. A late dial consumes the TLS/auth
/// budget, never the reverse. Per-command requests stay on the 60 s budget.
pub(in crate::shell::connect) struct ConnectBudget {
    start: Instant,
}

impl ConnectBudget {
    pub(in crate::shell::connect) fn start(now: Instant) -> Self {
        Self { start: now }
    }

    pub(in crate::shell::connect) fn dial_deadline(&self) -> Instant {
        self.start + DIAL_TIMEOUT
    }

    /// TLS/auth horizon, absolute `start + 30 s` like the SCRAM connector.
    pub(in crate::shell::connect) fn auth_deadline(&self) -> Instant {
        self.start + CONNECT_TOTAL
    }

    /// Bounds ONE TLS/auth step (REST establishment, the folded explicit
    /// `connect` Ping) by the shared absolute `start + 30 s` horizon — no
    /// fresh per-step budget: expiry is the typed connection failure.
    pub(in crate::shell::connect) async fn authenticate<F, T>(
        &self,
        step: F,
    ) -> Result<T, ShellError>
    where
        F: Future<Output = Result<T, ClientError>>,
    {
        timeout_at(self.auth_deadline(), step)
            .await
            .map_err(|_| ShellError::Connection)?
            .map_err(request_error)
    }
}

/// Shell-only dispatch; the parser gates commands before using this client.
pub(in crate::shell) enum SessionClient {
    Scram(Client),
    TokenWs(GuardedTokenClient<Client>),
    TokenRest(Box<GuardedTokenClient<TokenHttpClient>>),
}

/// Keep the credential private and wipe it when the session is dropped.
pub(in crate::shell) struct GuardedTokenClient<T> {
    credential: Zeroizing<String>,
    client: T,
}

impl SessionClient {
    pub(in crate::shell) fn is_token(&self) -> bool {
        match self {
            Self::Scram(_) => false,
            Self::TokenWs(_) | Self::TokenRest(_) => true,
        }
    }

    /// TLS authenticates the endpoint, not the catalogue text it returns.
    pub(in crate::shell) fn check_catalogue_output(
        &self,
        projected: &Value,
    ) -> Result<(), ShellError> {
        let token = match self {
            Self::Scram(_) => return Ok(()),
            Self::TokenWs(session) => session.credential.as_str(),
            Self::TokenRest(session) => session.credential.as_str(),
        };
        // ApiToken validated the canonical shk1.<id>.<secret> format.
        let (_, suffix) = token.rsplit_once('.').ok_or(ShellError::ProjectionLimit)?;
        let reflects = |text: &str| text.contains(token) || text.contains(suffix);
        if contains_credential(projected, &reflects) {
            return Err(ShellError::ProjectionLimit);
        }
        Ok(())
    }

    /// cancel-safe: NO — a cancelled Ping may remain in flight; do not retry.
    pub(in crate::shell) async fn ping(&self) -> Result<(), ClientError> {
        match self {
            Self::Scram(client) => client.ping().await,
            Self::TokenWs(session) => session.client.ping().await,
            Self::TokenRest(session) => session.client.ping().await,
        }
    }

    /// cancel-safe: NO — a cancelled command may remain in flight; do not retry.
    pub(in crate::shell) async fn execute(
        &self,
        db: &str,
        batch: BatchRequest,
    ) -> Result<BatchResponse, ClientError> {
        match self {
            Self::Scram(client) => client.execute(db, batch).await,
            Self::TokenWs(session) => session.client.execute(db, batch).await,
            Self::TokenRest(session) => session.client.execute(db, batch).await,
        }
    }

    /// Read-only operator status observation (admin-shell PR3, P1; design
    /// §4.2). SCRAM sessions only: §3.3 puts `status` OUTSIDE the token
    /// read-only subset, so a token session is refused HERE with the fixed
    /// typed denial BEFORE any request is sent (zero requests policy; the
    /// parser gates it too, and the server perimeter denies it as well).
    /// cancel-safe: NO — a cancelled request may remain in flight.
    pub(in crate::shell) async fn status(
        &self,
    ) -> Result<shamir_query_types::wire::db_message::OperatorStatus, ShellError> {
        match self {
            Self::Scram(client) => client.operator_status().await.map_err(request_error),
            Self::TokenWs(_) | Self::TokenRest(_) => Err(ShellError::TokenRequestDenied),
        }
    }

    /// Typed exact-key point read (admin-shell PR3, P2; design §4.4).
    /// SCRAM sessions only: §3.3 puts `data get` OUTSIDE the token read-only
    /// subset, so a token session is refused HERE with the fixed typed
    /// denial BEFORE any request is sent (zero requests policy). Not a
    /// cursor: no cursor is created or consumed. cancel-safe: NO — a
    /// cancelled request may remain in flight.
    pub(in crate::shell) async fn get_record(
        &self,
        db: &str,
        repo: &str,
        table: &str,
        key: shamir_types::types::record_id::RecordId,
    ) -> Result<shamir_query_types::read::QueryRecord, ShellError> {
        match self {
            Self::Scram(client) => client
                .get_record(db, repo, table, key)
                .await
                .map_err(request_error),
            Self::TokenWs(_) | Self::TokenRest(_) => Err(ShellError::TokenRequestDenied),
        }
    }

    /// Operator activity view (admin-shell PR4; design §4.5). SCRAM sessions
    /// only: §3.3 puts `activity` OUTSIDE the token read-only subset, so a
    /// token session is refused HERE with the fixed typed denial BEFORE any
    /// request is sent (zero requests policy; the parser gates it too, and
    /// the server perimeter denies it as well).
    /// cancel-safe: NO — a cancelled request may remain in flight.
    pub(in crate::shell) async fn list_activity(
        &self,
        db: &str,
        request: ListActivityReq,
    ) -> Result<ActivityPage, ShellError> {
        match self {
            Self::Scram(client) => client
                .list_activity(db, request)
                .await
                .map_err(request_error),
            Self::TokenWs(_) | Self::TokenRest(_) => Err(ShellError::TokenRequestDenied),
        }
    }

    /// Cooperative cancellation of one request execution (admin-shell PR4;
    /// design §4.5). SCRAM sessions only: a token session is refused HERE
    /// with the fixed typed denial BEFORE any request is sent (zero
    /// requests policy; the parser gates it too, and the server perimeter
    /// denies it as well).
    /// cancel-safe: NO — a cancelled request may remain in flight.
    pub(in crate::shell) async fn cancel_request(
        &self,
        db: &str,
        id: RequestExecId,
    ) -> Result<CancelOutcome, ShellError> {
        match self {
            Self::Scram(client) => client.cancel_request(db, id).await.map_err(request_error),
            Self::TokenWs(_) | Self::TokenRest(_) => Err(ShellError::TokenRequestDenied),
        }
    }

    pub(in crate::shell) async fn start_export(
        &self,
        db: &str,
        op: shamir_query_builder::ddl::migration::StartExport,
    ) -> Result<ExportStartReceipt, ShellError> {
        match self {
            Self::Scram(client) => client.start_export(db, op).await.map_err(request_error),
            Self::TokenWs(_) | Self::TokenRest(_) => Err(ShellError::TokenRequestDenied),
        }
    }

    pub(in crate::shell) async fn export_status(
        &self,
        db: &str,
    ) -> Result<ExportStatusReceipt, ShellError> {
        match self {
            Self::Scram(client) => client.export_status(db).await.map_err(request_error),
            Self::TokenWs(_) | Self::TokenRest(_) => Err(ShellError::TokenRequestDenied),
        }
    }

    pub(in crate::shell) async fn cancel_export(
        &self,
        db: &str,
    ) -> Result<ExportCancelReceipt, ShellError> {
        match self {
            Self::Scram(client) => client.cancel_export(db).await.map_err(request_error),
            Self::TokenWs(_) | Self::TokenRest(_) => Err(ShellError::TokenRequestDenied),
        }
    }

    /// Change mode bits on a securable resource (admin-shell R5 PR4; design §4.3).
    /// OUTSIDE the token read-only subset, so a token session is refused
    /// HERE with the fixed typed denial BEFORE any request is sent (zero
    /// requests policy; the parser gates it too, and the server perimeter
    /// denies it as well).
    /// cancel-safe: NO — a cancelled request may remain in flight.
    pub(in crate::shell) async fn chmod(
        &self,
        db: &str,
        resource: ResourceRef,
        mode: u16,
    ) -> Result<BatchResponse, ShellError> {
        match self {
            Self::Scram(client) => client
                .chmod(db, resource, mode)
                .await
                .map_err(request_error),
            Self::TokenWs(_) | Self::TokenRest(_) => Err(ShellError::TokenRequestDenied),
        }
    }

    /// Change owner on one securable resource (admin-shell R5 PR4; design
    /// §4.3); SCRAM sessions only, as for [`Self::chmod`].
    /// cancel-safe: NO — a cancelled request may remain in flight.
    pub(in crate::shell) async fn chown(
        &self,
        db: &str,
        resource: ResourceRef,
        owner: u64,
    ) -> Result<BatchResponse, ShellError> {
        match self {
            Self::Scram(client) => client
                .chown(db, resource, owner)
                .await
                .map_err(request_error),
            Self::TokenWs(_) | Self::TokenRest(_) => Err(ShellError::TokenRequestDenied),
        }
    }

    /// Change or clear the group of one securable resource (admin-shell R5
    /// PR4; design §4.3); SCRAM sessions only, as for [`Self::chmod`].
    /// cancel-safe: NO — a cancelled request may remain in flight.
    pub(in crate::shell) async fn chgrp(
        &self,
        db: &str,
        resource: ResourceRef,
        group: Option<u64>,
    ) -> Result<BatchResponse, ShellError> {
        match self {
            Self::Scram(client) => client
                .chgrp(db, resource, group)
                .await
                .map_err(request_error),
            Self::TokenWs(_) | Self::TokenRest(_) => Err(ShellError::TokenRequestDenied),
        }
    }
}

// Only called on catalogue JSON already depth-bounded by output::value.
fn contains_credential(value: &Value, reflects: &impl Fn(&str) -> bool) -> bool {
    match value {
        Value::String(text) => reflects(text),
        Value::Array(items) => items.iter().any(|item| contains_credential(item, reflects)),
        Value::Object(fields) => fields
            .iter()
            .any(|(key, item)| reflects(key) || contains_credential(item, reflects)),
        Value::Null | Value::Bool(_) | Value::Number(_) => false,
    }
}

/// A prepared token endpoint; no bearer material is stored in the profile.
pub(in crate::shell) struct TokenConnection {
    wss: bool,
    host: String,
    port: u16,
    sni: Option<String>,
    trust: TokenTrust,
}

/// Validate auth/transport compatibility before reading any secret or dialing.
pub(in crate::shell) fn prepare_token(profile: &Profile) -> Result<TokenConnection, ShellError> {
    let wss = match profile.transport_kind()? {
        Transport::TokenRest => false,
        Transport::TokenWss => true,
        Transport::TlsTcp | Transport::Ipc | Transport::NativeWss | Transport::Plaintext => {
            return Err(ShellError::Token);
        }
    };
    // Token identity comes solely from the bearer credential. A username in
    // a shared profile is not forwarded; an application pin cannot set trust.
    if profile.pin.is_some() {
        return Err(ShellError::InvalidProfile);
    }
    let (host, port) = profile.tls_address()?;
    let trust = match profile.ca_path.as_deref() {
        Some(path) => {
            // A relative trust-anchor path would be resolved against the
            // PROCESS working directory, so the same protected profile
            // could select a different CA depending on where the shell was
            // started. The design is silent on relative `ca_path`; the
            // narrow safe choice is refusal with the existing typed
            // profile error (guide wording to follow out of scope here).
            if path.is_relative() {
                return Err(ShellError::InvalidProfile);
            }
            read_ca(path)?
        }
        None => TokenTrust::webpki_roots(),
    };
    Ok(TokenConnection {
        wss,
        host,
        port,
        sni: profile.sni.clone(),
        trust,
    })
}

impl TokenConnection {
    /// cancel-safe: NO — an upgrade may have reached the peer; abandon the dial.
    /// `explicit_auth` probes an explicit REST `connect` with one Ping inside
    /// the same auth horizon; an implicit dial sends only its own command.
    pub(in crate::shell) async fn connect(
        self,
        secret: Zeroizing<Vec<u8>>,
        explicit_auth: bool,
    ) -> Result<SessionClient, ShellError> {
        let text = std::str::from_utf8(&secret).map_err(|_| ShellError::Authentication)?;
        // ApiToken::new takes ownership of the String in order to zeroize it.
        let token = ApiToken::new(text.to_owned()).map_err(request_error)?;
        let credential = Zeroizing::new(text.to_owned());
        drop(secret);

        let budget = ConnectBudget::start(Instant::now());
        let dial_deadline = budget.dial_deadline();
        let addr = timeout_at(
            dial_deadline,
            tokio::net::lookup_host((self.host.as_str(), self.port)),
        )
        .await
        .map_err(|_| ShellError::Connection)?
        .map_err(|_| ShellError::Connection)?
        .next()
        .ok_or(ShellError::Connection)?;
        let endpoint = TokenEndpoint::https(addr, self.sni.unwrap_or(self.host), self.trust);
        let dial_remaining = Some(dial_deadline.saturating_duration_since(Instant::now()));
        // TLS + auth share the single absolute start + 30 s horizon (SCRAM
        // outer horizon; a late dial consumes it — never the reverse).
        let auth_remaining = budget
            .auth_deadline()
            .saturating_duration_since(Instant::now());
        if self.wss {
            // SDK WSS: `connect_timeout` bounds only TCP; `handshake_timeout`
            // bounds TLS + upgrade + bind; the horizon backstops. The 408-
            // before-channel WSS classification needs the raw ClientError,
            // so this branch stays on the inline horizon bound.
            let opts = TokenWsOptions::new(endpoint, token)
                .with_connect_timeout(dial_remaining)
                .with_handshake_timeout(Some(auth_remaining))
                .with_request_timeout(Some(REQUEST_TIMEOUT));
            timeout_at(budget.auth_deadline(), Client::connect_token_ws(opts))
                .await
                .map_err(|_| ShellError::Connection)?
                .map(|client| SessionClient::TokenWs(GuardedTokenClient { credential, client }))
                .map_err(wss_connect_error)
        } else {
            // REST: TCP gets the 10 s dial deadline; establishment and the
            // optional explicit `connect` Ping share the single absolute
            // start + 30 s horizon in ONE step through `authenticate` (a
            // late dial consumes it — never the reverse, no fresh
            // per-step budget).
            let opts = TokenHttpOptions::new(endpoint, token)
                .with_connect_timeout(Some(auth_remaining))
                .with_request_timeout(Some(REQUEST_TIMEOUT));
            budget
                .authenticate(async {
                    let client =
                        TokenHttpClient::connect_once_with_tcp_deadline(opts, dial_deadline)
                            .await?;
                    if explicit_auth {
                        client.ping().await?;
                    }
                    Ok(client)
                })
                .await
                .map(|client| {
                    SessionClient::TokenRest(Box::new(GuardedTokenClient { credential, client }))
                })
        }
    }
}

/// A WSS upgrade answers HTTP 408 BEFORE any channel exists: a connection
/// failure; the REST 408 classifier applies only after a request was sent.
fn wss_connect_error(error: ClientError) -> ShellError {
    match error {
        // A WSS upgrade answers HTTP 408 only BEFORE any channel exists,
        // so every 408 here is a connection failure.
        ClientError::Http { status: 408, .. } => ShellError::Connection,
        other => request_error(other),
    }
}

#[cfg(test)]
#[path = "tests/token_deadlines.rs"]
mod token_deadlines;

fn read_ca(path: &Path) -> Result<TokenTrust, ShellError> {
    // The operator chooses this trust source in a verified profile. Reject
    // special files, and check the opened handle as well as capping the read.
    let file = open_ca(path)?;
    if !file
        .metadata()
        .map_err(|_| ShellError::InvalidProfile)?
        .is_file()
    {
        return Err(ShellError::InvalidProfile);
    }
    let mut pem = Zeroizing::new(Vec::with_capacity(MAX_CA_BYTES as usize + 1));
    file.take(MAX_CA_BYTES + 1)
        .read_to_end(&mut pem)
        .map_err(|_| ShellError::InvalidProfile)?;
    if pem.len() as u64 > MAX_CA_BYTES {
        return Err(ShellError::InvalidProfile);
    }
    TokenTrust::pem(&pem).map_err(|_| ShellError::InvalidProfile)
}

#[cfg(unix)]
fn open_ca(path: &Path) -> Result<File, ShellError> {
    // Nonblocking open lets the handle check reject FIFOs without waiting for
    // a writer, including if the selected path is replaced before open.
    open(
        path,
        OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NONBLOCK,
        Mode::empty(),
    )
    .map(File::from)
    .map_err(|_| ShellError::InvalidProfile)
}

#[cfg(not(unix))]
fn open_ca(path: &Path) -> Result<File, ShellError> {
    if !fs::metadata(path)
        .map_err(|_| ShellError::InvalidProfile)?
        .is_file()
    {
        return Err(ShellError::InvalidProfile);
    }
    File::open(path).map_err(|_| ShellError::InvalidProfile)
}

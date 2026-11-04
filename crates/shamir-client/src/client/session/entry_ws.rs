//! WebSocket and plain-TCP entry points: [`Client::connect_ws`],
//! [`Client::resume_ws`] / [`Client::resume_ws_secure`],
//! [`Client::connect_ws_browser`] / [`Client::resume_ws_browser`] and
//! [`Client::connect_plain`] / [`Client::resume_plain`] and
//! [`Client::resume_local`]. Same three steps as
//! the TCP twins in `entry.rs` — dial, `run_scram` / `resume_wire_exchange`,
//! `assemble`.

use std::future::Future;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rustls::crypto::aws_lc_rs::default_provider;
use tokio::io::AsyncRead;
use zeroize::Zeroizing;

use shamir_connect::common::types::BindingMode;
use shamir_connect::common::username::NormalizedUsername;

use crate::client::handshake::{
    read_binding, resume_wire_exchange, resume_wire_exchange_unbound, run_scram, ScramRequest,
};
use crate::client::session::client::{Client, SessionMeta};
use crate::client::session::entry::resume_security::ProtectedResumeBuffer;
use crate::client::session::entry::{phase_name, session_meta, PHASE_TCP};
use crate::client::session::options::{
    BrowserResumeOptions, BrowserWsOptions, ConnectOptions, ResumeLocalOptions, ResumeOptions,
    SecureResumeOptions,
};
use crate::client::session::token::{AuthKind, TokenWsOptions};
use crate::client::transport::token::dial_token_ws;
use crate::client::transport::{
    dial_ipc, dial_tcp_plain, dial_ws_browser, dial_ws_native, Dialed, LifecycleHooks, WriteSink,
    WsByteStream,
};
use crate::error::ClientError;

async fn bound_handshake<T>(
    handshake_timeout: Option<Duration>,
    phase: &AtomicU8,
    fut: impl Future<Output = Result<T, ClientError>>,
) -> Result<T, ClientError> {
    match handshake_timeout {
        None => fut.await,
        Some(budget) => tokio::time::timeout(budget, fut).await.map_err(|_| {
            ClientError::HandshakeTimeout(
                budget,
                Arc::from(phase_name(phase.load(Ordering::Relaxed))),
            )
        })?,
    }
}

impl Client {
    /// Run the full TCP→TLS→WebSocket upgrade→SCRAM handshake against a native
    /// WebSocket listener (`kind: ws`, `profile: tls_exporter`, path
    /// `/shamir/v1`) and return a ready client. Same options and trust model
    /// as [`Client::connect`]; `handshake_timeout` bounds the whole sequence
    /// including the upgrade.
    pub async fn connect_ws(opts: ConnectOptions) -> Result<Self, ClientError> {
        let _ = default_provider().install_default();
        let username = NormalizedUsername::from_raw(&opts.username)
            .map_err(|e| ClientError::InvalidUsername(e.to_string()))?;
        let phase = Arc::new(AtomicU8::new(PHASE_TCP));
        let handshake_timeout = opts.handshake_timeout;
        let fut = Self::handshake_ws(opts, username, phase.clone());
        bound_handshake(handshake_timeout, &phase, fut).await
    }

    async fn handshake_ws(
        opts: ConnectOptions,
        username: NormalizedUsername,
        phase: Arc<AtomicU8>,
    ) -> Result<Self, ClientError> {
        let dialed = dial_ws_native(
            opts.addr,
            &opts.server_name,
            opts.connect_timeout,
            Some(phase.as_ref()),
        )
        .await?;
        Self::scram_and_assemble(dialed, opts, username, phase.as_ref()).await
    }

    /// Open a session over plain TCP (`kind: tcp`, `profile: plain`, binding
    /// mode 0x00). The server accepts plain TCP on loopback only, so a
    /// non-loopback `opts.addr` is refused here, before any socket is
    /// opened. `opts.server_name` is ignored.
    pub async fn connect_plain(opts: ConnectOptions) -> Result<Self, ClientError> {
        if !opts.addr.ip().to_canonical().is_loopback() {
            return Err(ClientError::handshake("plain TCP is loopback-only"));
        }
        let username = NormalizedUsername::from_raw(&opts.username)
            .map_err(|e| ClientError::InvalidUsername(e.to_string()))?;
        let phase = Arc::new(AtomicU8::new(PHASE_TCP));
        let handshake_timeout = opts.handshake_timeout;
        let fut = Self::handshake_plain(opts, username, phase.clone());
        bound_handshake(handshake_timeout, &phase, fut).await
    }

    async fn handshake_plain(
        opts: ConnectOptions,
        username: NormalizedUsername,
        phase: Arc<AtomicU8>,
    ) -> Result<Self, ClientError> {
        let dialed = dial_tcp_plain(opts.addr, opts.connect_timeout).await?;
        Self::scram_and_assemble(dialed, opts, username, phase.as_ref()).await
    }

    async fn scram_and_assemble<R>(
        mut dialed: Dialed<R>,
        opts: ConnectOptions,
        username: NormalizedUsername,
        phase: &AtomicU8,
    ) -> Result<Self, ClientError>
    where
        R: AsyncRead + Unpin + Send + 'static,
    {
        let outcome = run_scram(
            &mut dialed.read,
            &mut dialed.write,
            ScramRequest {
                username,
                binding: dialed.binding,
                trusted_pin: opts.trusted_pin,
                accept_new_host: opts.accept_new_host,
                password: &opts.password,
                phase: Some(phase),
            },
        )
        .await?;

        Ok(Self::assemble(
            dialed.read,
            dialed.write,
            session_meta(outcome, opts.request_timeout),
            LifecycleHooks::default(),
        ))
    }

    /// [`Client::resume`] over the native WebSocket listener: the ticket moves
    /// freely between TCP and WebSocket because the TLS exporter label and
    /// value are identical on both.
    pub async fn resume_ws(opts: ResumeOptions) -> Result<Self, ClientError> {
        Self::resume_ws_secure(opts.into()).await
    }

    /// [`Client::resume_secure`] over the native WebSocket listener.
    pub async fn resume_ws_secure(opts: SecureResumeOptions) -> Result<Self, ClientError> {
        let ticket = ProtectedResumeBuffer(opts.ticket);
        let _ = default_provider().install_default();
        let mut dialed = dial_ws_native(opts.addr, &opts.server_name, None, None).await?;

        let resumed = resume_wire_exchange(
            &mut dialed.read,
            &mut dialed.write,
            ticket,
            opts.pinned_hash,
            dialed.binding.exporter,
        )
        .await?;

        Ok(Self::assemble(
            dialed.read,
            dialed.write,
            SessionMeta {
                session_id: resumed.session_id,
                pinned_hash: opts.pinned_hash,
                expires_at_ns: resumed.expires_at_ns,
                resumption_ticket: resumed.resumption_ticket,
                resumption_expires_at_ns: resumed.resumption_expires_at_ns,
                server_query_version: resumed.server_query_version,
                request_timeout: None,
                rotation_in_progress: None,
                kdf_upgrade_required: false,
                auth: AuthKind::Scram,
            },
            LifecycleHooks::default(),
        ))
    }

    /// Run the full TCP→TLS→WebSocket upgrade→SCRAM handshake against a
    /// browser-profile WebSocket listener (`kind: ws`, `profile:
    /// tls_no_export`, path `/shamir/v1/browser`, binding mode 0x02) and
    /// return a ready client. Useful behind a TLS-terminating proxy that
    /// breaks the TLS exporter: the TLS server is authenticated by the
    /// ordinary certificate check against `opts.trust`, and the Ed25519
    /// identity TOFU-pin dance is unchanged. `handshake_timeout` bounds the
    /// whole sequence including the upgrade.
    pub async fn connect_ws_browser(opts: BrowserWsOptions) -> Result<Self, ClientError> {
        let _ = default_provider().install_default();
        let username = NormalizedUsername::from_raw(&opts.inner.username)
            .map_err(|e| ClientError::InvalidUsername(e.to_string()))?;
        let phase = Arc::new(AtomicU8::new(PHASE_TCP));
        let handshake_timeout = opts.inner.handshake_timeout;
        let fut = Self::handshake_ws_browser(opts, username, phase.clone());
        bound_handshake(handshake_timeout, &phase, fut).await
    }

    async fn handshake_ws_browser(
        opts: BrowserWsOptions,
        username: NormalizedUsername,
        phase: Arc<AtomicU8>,
    ) -> Result<Self, ClientError> {
        let BrowserWsOptions {
            inner,
            origin,
            trust,
        } = opts;
        let dialed = dial_ws_browser(
            inner.addr,
            &inner.server_name,
            &origin,
            &trust,
            inner.connect_timeout,
            Some(phase.as_ref()),
        )
        .await?;
        Self::scram_and_assemble(dialed, inner, username, phase.as_ref()).await
    }

    /// [`Client::resume`] over the browser-profile WebSocket listener
    /// (binding mode 0x02). There is no `resume_proof` preamble on this
    /// binding (the profile has no TLS exporter, so the ticket is the first
    /// frame — as on the plain and local-IPC paths); the server is
    /// authenticated by the TLS certificate check against `opts.trust`, and
    /// the pinned hash is carried onto the resumed session unchanged.
    pub async fn resume_ws_browser(opts: BrowserResumeOptions) -> Result<Self, ClientError> {
        let ticket = ProtectedResumeBuffer(Zeroizing::new(opts.inner.ticket));
        let _ = default_provider().install_default();
        let mut dialed = dial_ws_browser(
            opts.inner.addr,
            &opts.inner.server_name,
            &opts.origin,
            &opts.trust,
            None,
            None,
        )
        .await?;

        let resumed = resume_wire_exchange_unbound(
            &mut dialed.read,
            &mut dialed.write,
            ticket,
            BindingMode::TlsNoExport,
        )
        .await?;

        Ok(Self::assemble(
            dialed.read,
            dialed.write,
            SessionMeta {
                session_id: resumed.session_id,
                pinned_hash: opts.inner.pinned_hash,
                expires_at_ns: resumed.expires_at_ns,
                resumption_ticket: resumed.resumption_ticket,
                resumption_expires_at_ns: resumed.resumption_expires_at_ns,
                server_query_version: resumed.server_query_version,
                request_timeout: None,
                rotation_in_progress: None,
                kdf_upgrade_required: false,
                auth: AuthKind::Scram,
            },
            LifecycleHooks::default(),
        ))
    }

    /// [`Client::resume`] over the plain loopback TCP listener (`kind: tcp`,
    /// `profile: plain`, binding mode 0x00). Same loopback-only refusal as
    /// [`Client::connect_plain`], before any socket is opened. There is no
    /// identity-proof preamble on this binding (as on the local-IPC path):
    /// the ticket is the first frame, and the pinned hash rides on the
    /// resumed session.
    pub async fn resume_plain(opts: ResumeOptions) -> Result<Self, ClientError> {
        // Take ownership of the ticket first so a refusal below still wipes it.
        let ticket = ProtectedResumeBuffer(Zeroizing::new(opts.ticket));
        if !opts.addr.ip().to_canonical().is_loopback() {
            return Err(ClientError::handshake("plain TCP is loopback-only"));
        }
        let mut dialed = dial_tcp_plain(opts.addr, None).await?;

        let resumed = resume_wire_exchange_unbound(
            &mut dialed.read,
            &mut dialed.write,
            ticket,
            BindingMode::None,
        )
        .await?;

        Ok(Self::assemble(
            dialed.read,
            dialed.write,
            SessionMeta {
                session_id: resumed.session_id,
                pinned_hash: opts.pinned_hash,
                expires_at_ns: resumed.expires_at_ns,
                resumption_ticket: resumed.resumption_ticket,
                resumption_expires_at_ns: resumed.resumption_expires_at_ns,
                server_query_version: resumed.server_query_version,
                request_timeout: None,
                rotation_in_progress: None,
                kdf_upgrade_required: false,
                auth: AuthKind::Scram,
            },
            LifecycleHooks::default(),
        ))
    }

    /// [`Client::resume`] over the local-IPC transport (Unix domain socket /
    /// Windows Named Pipe, binding mode 0x00). No identity-proof preamble on
    /// this binding: the ticket is the first frame, and the pinned hash rides
    /// on the resumed session. The server refuses a ticket issued under a
    /// stronger binding (anti-downgrade); a ticket from a local session
    /// resumes here and on every stronger listener.
    pub async fn resume_local(opts: ResumeLocalOptions) -> Result<Self, ClientError> {
        let ticket = ProtectedResumeBuffer(opts.ticket);
        let mut dialed = dial_ipc(&opts.addr, opts.connect_timeout).await?;

        let resumed = resume_wire_exchange_unbound(
            &mut dialed.read,
            &mut dialed.write,
            ticket,
            BindingMode::None,
        )
        .await?;

        Ok(Self::assemble(
            dialed.read,
            dialed.write,
            SessionMeta {
                session_id: resumed.session_id,
                pinned_hash: opts.pinned_hash,
                expires_at_ns: resumed.expires_at_ns,
                resumption_ticket: resumed.resumption_ticket,
                resumption_expires_at_ns: resumed.resumption_expires_at_ns,
                server_query_version: resumed.server_query_version,
                request_timeout: None,
                rotation_in_progress: None,
                kdf_upgrade_required: false,
                auth: AuthKind::Scram,
            },
            LifecycleHooks::default(),
        ))
    }

    /// Open a session with an API token over the native token WebSocket
    /// (`GET /shamir/v1/ws` on a `kind: http` listener, binding mode none).
    /// TLS uses WebPKI trust of the endpoint (no Ed25519 pin). The data plane
    /// is identical to [`Client::connect`]; admin operations
    /// (`create_api_token`, ..., `repl`) are refused locally with
    /// `Db { code: "token_request_denied" }`. Server closes arrive as
    /// `ClientError::ConnectionClosed(CloseCause::PeerClosed { code, .. })`
    /// (4011 revoked, 4012 expired, ...).
    pub async fn connect_token_ws(opts: TokenWsOptions) -> Result<Self, ClientError> {
        let _ = default_provider().install_default();
        let TokenWsOptions {
            endpoint,
            token,
            connect_timeout,
            handshake_timeout,
            request_timeout,
        } = opts;
        let authorization = token.authorization_header();
        drop(token);
        let authorization = authorization?;
        let phase = Arc::new(AtomicU8::new(PHASE_TCP));
        let fut = async {
            let mut ws = dial_token_ws(
                &endpoint,
                authorization,
                connect_timeout,
                Some(phase.as_ref()),
            )
            .await?;
            let binding = read_binding(&mut ws).await?;
            Ok::<_, ClientError>((binding, tokio::io::split(WsByteStream::raw(ws))))
        };
        let (binding, (read, write)) = bound_handshake(handshake_timeout, &phase, fut).await?;

        Ok(Self::assemble(
            read,
            WriteSink::TokenWs(write),
            SessionMeta {
                session_id: binding.sid,
                pinned_hash: [0u8; 32],
                expires_at_ns: binding.expires_at_ns,
                resumption_ticket: None,
                resumption_expires_at_ns: None,
                server_query_version: 0,
                request_timeout,
                rotation_in_progress: None,
                kdf_upgrade_required: false,
                auth: AuthKind::Token,
            },
            LifecycleHooks::default(),
        ))
    }
}

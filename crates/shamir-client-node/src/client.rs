use std::sync::Arc;
use std::time::Duration;

use napi::bindgen_prelude::*;
use napi_derive::napi;
use tokio::sync::{watch, RwLock};

use shamir_client as core;
use shamir_query_types::wire::repl::ReplRequest;
use shamir_query_types::wire::DbResponse;

use crate::connect_variants::{
    self, ConnectLocalOptions, ConnectPlainOptions, ConnectWsBrowserOptions, ResumeLocalOptions,
    ResumeOptions, ResumePlainOptions, ResumeWsBrowserOptions,
};
use crate::request_state::State;
use crate::secrets::{self, Snapshots};
use crate::token::{self, CreateApiTokenOptions, TokenWsOptions};
use crate::{
    checked_port, encode_buffer, encode_db_error, infra_error, millis, optional_pin,
    request_timeout_error, run_request, snapshot_bytes, unresolved_addr,
};

// --------------------------------------------------------------------------
// Connect options — JS sees a plain object with camelCase fields.
// --------------------------------------------------------------------------

/// Connection parameters passed to `ShamirClient.connect`.
#[napi(object)]
pub struct ConnectOptions {
    /// Server host (e.g. `"127.0.0.1"` or `"db.example.com"`).
    pub host: String,
    /// Server port.
    pub port: u32,
    /// SNI hostname for TLS — usually matches the cert's CN.
    pub server_name: String,
    /// Username (raw — server-side normalisation applies).
    pub username: String,
    /// Plaintext password. The owned native buffer is zeroized on drop;
    /// JavaScript strings and copies outside this buffer are not erased.
    pub password: String,
    /// Trust-on-first-use: accept whatever Ed25519 pubkey the server
    /// presents on first connection. `true` for first connect; once you
    /// persist the pin, switch to `false` and pass `trustedPin`.
    pub accept_new_host: Option<bool>,
    /// Pre-pinned `SHA256(server_ed25519_pub_key)` — 32 bytes. When
    /// supplied, mismatch fails the handshake.
    pub trusted_pin: Option<Buffer>,
    /// Deadline (ms) for the TCP connect attempt.
    pub connect_timeout_ms: Option<u32>,
    /// Deadline (ms) for the WHOLE TCP→TLS→SCRAM handshake — covers the
    /// TLS and SCRAM awaits that `connectTimeoutMs` does not
    /// (RV-CLIENT-NODE-1CFB3796). Omitted = unbounded (prior behaviour).
    pub handshake_timeout_ms: Option<u32>,
    /// Deadline (ms) for each request/response round-trip after connect.
    pub request_timeout_ms: Option<u32>,
}

/// Validated core options plus the host and port that still need resolving.
pub(crate) struct PreparedConnect {
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) core_opts: core::ConnectOptions,
}

impl ConnectOptions {
    /// Synchronous half of every `host:port` SCRAM connect: protects the
    /// password first, then validates and converts the rest.
    pub(crate) fn prepare(self) -> Result<PreparedConnect> {
        let (password, port) =
            secrets::protect_before(self.password.into_bytes(), || checked_port(self.port))?;
        let trusted_pin = optional_pin(self.trusted_pin)?;
        let core_opts = core::ConnectOptions {
            // Filled in by the caller from host:port — never used as is.
            addr: unresolved_addr(),
            server_name: self.server_name,
            username: self.username,
            password,
            accept_new_host: self.accept_new_host.unwrap_or(false),
            trusted_pin,
            connect_timeout: millis(self.connect_timeout_ms),
            handshake_timeout: millis(self.handshake_timeout_ms),
            request_timeout: millis(self.request_timeout_ms),
        };
        Ok(PreparedConnect {
            host: self.host,
            port,
            core_opts,
        })
    }
}

pub(crate) type SharedState = State<core::Client>;

// --------------------------------------------------------------------------
// Client wrapper — holds the connected core::Client behind an async RwLock
// so that close() can `take()` it (since core::Client::close consumes self)
// while shared read guards let requests multiplex.
// --------------------------------------------------------------------------

/// Connected, authenticated client (SCRAM over TLS / WebSocket / plain
/// loopback / local IPC, or an API-token WebSocket session).
#[napi]
pub struct ShamirClient {
    inner: SharedState,
    shutdown: watch::Sender<bool>,
    /// Cached at connect time so JS callers can read it without the
    /// async lock — useful for persisting the TOFU pin.
    pin: [u8; 32],
    secrets: Snapshots<Vec<u8>>,
    expires_at_ns: u64,
    resumption_expires_at_ns: Option<u64>,
    /// Cached at connect time: `"scram"`, `"token"` or `"unknown"`.
    auth_kind: &'static str,
    /// Cached at connect time: the terminal per-request deadline — bounds
    /// each JS call (lock wait + write + response wait) from the moment it
    /// starts (RV-CLIENT-NODE-1CFB3796). `None` = unbounded (prior behaviour).
    request_timeout: Option<Duration>,
}

/// Wrap a freshly connected core client, caching everything JS reads
/// synchronously.
pub(crate) fn from_core(client: core::Client, request_timeout: Option<Duration>) -> ShamirClient {
    let pin = client.server_pub_key_pin();
    let session_id = client.session_id().to_vec();
    let expires_at_ns = client.expires_at_ns();
    let resumption_ticket = client.resumption_ticket().map(|s| s.to_vec());
    let resumption_expires_at_ns = client.resumption_expires_at_ns();
    let auth_kind = match client.auth_kind() {
        core::AuthKind::Scram => "scram",
        core::AuthKind::Token => "token",
        _ => "unknown",
    };
    let (shutdown, _) = watch::channel(false);

    ShamirClient {
        inner: Arc::new(RwLock::new(Some(client))),
        shutdown,
        pin,
        secrets: Snapshots::new(session_id, resumption_ticket),
        expires_at_ns,
        resumption_expires_at_ns,
        auth_kind,
        request_timeout,
    }
}

#[napi]
impl ShamirClient {
    /// Run the full TCP→TLS→SCRAM handshake; resolves to a connected
    /// client.
    #[napi]
    pub fn connect<'env>(
        env: &'env Env,
        opts: ConnectOptions,
    ) -> Result<PromiseRaw<'env, ShamirClient>> {
        let PreparedConnect {
            host,
            port,
            core_opts,
        } = opts.prepare()?;
        let req_timeout = core_opts.request_timeout;
        // Single production hostname path: resolution + connect live in the
        // core (`connect_host`); the Node layer parses nothing.
        env.spawn_future(async move {
            let client = core::Client::connect_host(&host, port, core_opts)
                .await
                .map_err(infra_error)?;
            Ok(from_core(client, req_timeout))
        })
    }

    /// Same as `connect` over the native WebSocket listener. Only the first
    /// resolved address of `host` is tried.
    #[napi]
    pub fn connect_ws<'env>(
        env: &'env Env,
        opts: ConnectOptions,
    ) -> Result<PromiseRaw<'env, ShamirClient>> {
        connect_variants::connect_ws(env, opts)
    }

    /// SCRAM over plain TCP; the core refuses non-loopback addresses.
    #[napi]
    pub fn connect_plain<'env>(
        env: &'env Env,
        opts: ConnectPlainOptions,
    ) -> Result<PromiseRaw<'env, ShamirClient>> {
        connect_variants::connect_plain(env, opts)
    }

    /// SCRAM over a Unix domain socket or Windows named pipe.
    #[napi]
    pub fn connect_local<'env>(
        env: &'env Env,
        opts: ConnectLocalOptions,
    ) -> Result<PromiseRaw<'env, ShamirClient>> {
        connect_variants::connect_local(env, opts)
    }

    /// Resume a session over TLS with a previously issued ticket.
    #[napi]
    pub fn resume<'env>(
        env: &'env Env,
        opts: ResumeOptions,
    ) -> Result<PromiseRaw<'env, ShamirClient>> {
        connect_variants::resume(env, opts)
    }

    /// Resume a session over the native WebSocket listener.
    #[napi]
    pub fn resume_ws<'env>(
        env: &'env Env,
        opts: ResumeOptions,
    ) -> Result<PromiseRaw<'env, ShamirClient>> {
        connect_variants::resume_ws(env, opts)
    }

    /// Session over the browser-profile WebSocket listener (`kind: ws`,
    /// `profile: tls_no_export`, path `/shamir/v1/browser`, binding 0x02).
    /// The listener refuses upgrades whose `Origin` is not allowlisted; TLS
    /// trusts WebPKI roots or `caPem`, and the Ed25519 TOFU-pin dance
    /// applies on top.
    #[napi]
    pub fn connect_ws_browser<'env>(
        env: &'env Env,
        opts: ConnectWsBrowserOptions,
    ) -> Result<PromiseRaw<'env, ShamirClient>> {
        connect_variants::connect_ws_browser(env, opts)
    }

    /// Resume a session over the browser-profile WebSocket listener
    /// (binding 0x02).
    #[napi]
    pub fn resume_ws_browser<'env>(
        env: &'env Env,
        opts: ResumeWsBrowserOptions,
    ) -> Result<PromiseRaw<'env, ShamirClient>> {
        connect_variants::resume_ws_browser(env, opts)
    }

    /// Resume a session over plain loopback TCP (binding 0x00). The core
    /// refuses non-loopback addresses; the server refuses a ticket issued
    /// under a stronger binding (anti-downgrade).
    #[napi]
    pub fn resume_plain<'env>(
        env: &'env Env,
        opts: ResumePlainOptions,
    ) -> Result<PromiseRaw<'env, ShamirClient>> {
        connect_variants::resume_plain(env, opts)
    }

    /// Resume a session over local IPC (Unix socket / Windows named pipe,
    /// binding 0x00).
    #[napi]
    pub fn resume_local<'env>(
        env: &'env Env,
        opts: ResumeLocalOptions,
    ) -> Result<PromiseRaw<'env, ShamirClient>> {
        connect_variants::resume_local(env, opts)
    }

    /// Open an API-token session over the token WebSocket.
    #[napi]
    pub fn connect_token_ws<'env>(
        env: &'env Env,
        opts: TokenWsOptions,
    ) -> Result<PromiseRaw<'env, ShamirClient>> {
        token::connect_token_ws(env, opts)
    }

    /// `SHA256(server_ed25519_pub_key)` — persist this and pass back
    /// as `trustedPin` on subsequent connections.
    #[napi]
    pub fn server_pub_key_pin(&self) -> Buffer {
        Buffer::from(self.pin.to_vec())
    }

    /// 32-byte session id assigned by the server; empty after close.
    #[napi]
    pub fn session_id(&self) -> Buffer {
        self.secrets
            .session()
            .map_or_else(|| Buffer::from(Vec::new()), |id| Buffer::from(id.to_vec()))
    }

    /// Absolute session expiry (unix nanoseconds). Returned as `BigInt`
    /// in JS because nanoseconds since epoch overflow `Number`.
    #[napi]
    pub fn expires_at_ns(&self) -> BigInt {
        BigInt::from(self.expires_at_ns)
    }

    /// Resumption ticket bytes (if issued); feed it to `resume` / `resumeWs`.
    /// Returns null after close; previously returned JS copies are not erased.
    #[napi]
    pub fn resumption_ticket(&self) -> Option<Buffer> {
        self.secrets
            .ticket()
            .map(|ticket| Buffer::from(ticket.to_vec()))
    }

    /// Resumption expiry (paired with [`Self::resumption_ticket`]).
    #[napi]
    pub fn resumption_expires_at_ns(&self) -> Option<BigInt> {
        self.resumption_expires_at_ns.map(BigInt::from)
    }

    /// How the session was authenticated: `"scram"`, `"token"` or
    /// `"unknown"` for kinds this binding does not know yet.
    #[napi]
    pub fn auth_kind(&self) -> String {
        self.auth_kind.to_owned()
    }

    /// Health check.
    #[napi(ts_return_type = "Promise<void>")]
    pub fn ping<'env>(&self, env: &'env Env) -> Result<PromiseRaw<'env, ()>> {
        let (state, mut shutdown, deadline, timeout) = self.request_parts();
        env.spawn_future(async move {
            let result: std::result::Result<(), core::ClientError> =
                run_request(&state, &mut shutdown, deadline, timeout, |client| {
                    Box::pin(client.ping())
                })
                .await?;
            result.map_err(infra_error)
        })
    }

    /// Execute a `BatchRequest` (passed as a MessagePack-encoded `Buffer`)
    /// against the named database. Returns the full `BatchResponse` as a
    /// MessagePack-encoded `Buffer`.
    #[napi]
    pub fn execute<'env>(
        &self,
        env: &'env Env,
        db: String,
        batch: Buffer,
    ) -> Result<PromiseRaw<'env, Buffer>> {
        let (state, mut shutdown, deadline, timeout) = self.request_parts();
        // Owned snapshot before dispatch: JS may mutate the Buffer after
        // this call returns but before the worker reads it (067017c8).
        let batch = snapshot_bytes(&batch);
        env.spawn_future(async move {
            // Sanctioned FFI boundary: deserialize caller-supplied wire bytes,
            // rather than constructing a query in the binding.
            let request: core::BatchRequest = rmp_serde::from_slice(&batch)
                .map_err(|error| Error::from_reason(format!("invalid batch payload: {error}")))?;
            let result: std::result::Result<core::BatchResponse, core::ClientError> =
                run_request(&state, &mut shutdown, deadline, timeout, |client| {
                    Box::pin(async move { client.execute(&db, request).await })
                })
                .await?;
            match result {
                Ok(response) => encode_buffer(&response, "encode response"),
                Err(core::ClientError::Db { code, message }) => encode_db_error(code, message),
                Err(error) => Err(infra_error(error)),
            }
        })
    }

    /// Privileged replication pull-API (REPLICATION §5). Takes a msgpack
    /// `ReplRequest` Buffer, returns a msgpack `ReplResponse` Buffer. The
    /// session must hold the `replicator` role (or be superuser).
    #[napi]
    pub fn repl<'env>(&self, env: &'env Env, req: Buffer) -> Result<PromiseRaw<'env, Buffer>> {
        let deadline = self.request_deadline();
        let timeout = self.request_timeout;
        // Owned snapshot before dispatch (067017c8): the caller's Buffer is
        // copied synchronously on the JS thread before the worker reads it.
        let req = snapshot_bytes(&req);
        let state = Arc::clone(&self.inner);
        let mut shutdown = self.shutdown.subscribe();
        env.spawn_future(async move {
            // Sanctioned FFI boundary: deserialize caller-supplied wire bytes,
            // rather than constructing a query in the binding.
            let request: ReplRequest = rmp_serde::from_slice(&req)
                .map_err(|error| Error::from_reason(format!("invalid repl payload: {error}")))?;
            let result = run_request(&state, &mut shutdown, deadline, timeout, |client| {
                Box::pin(async move { client.repl(request).await })
            })
            .await?;
            match result {
                Ok(response) => encode_buffer(&response, "encode repl response"),
                Err(core::ClientError::Db { code, message }) => encode_db_error(code, message),
                Err(error) => Err(infra_error(error)),
            }
        })
    }

    /// Create a new SCRAM-authenticatable user. Requires the current
    /// session to belong to a superuser. Returns the stable 16-byte
    /// `user_id` as a Buffer.
    #[napi]
    pub fn create_scram_user<'env>(
        &self,
        env: &'env Env,
        name: String,
        password: String,
        roles: Vec<String>,
    ) -> Result<PromiseRaw<'env, Buffer>> {
        let password = secrets::protect(password);
        let deadline = self.request_deadline();
        let timeout = self.request_timeout;
        let state = Arc::clone(&self.inner);
        let mut shutdown = self.shutdown.subscribe();
        env.spawn_future(async move {
            let result = run_request(&state, &mut shutdown, deadline, timeout, |client| {
                Box::pin(async move { client.create_scram_user(&name, password, roles).await })
            })
            .await?;
            match result {
                Ok(user_id) => Ok(Buffer::from(user_id)),
                Err(core::ClientError::Db { code, message }) => encode_db_error(code, message),
                Err(error) => Err(infra_error(error)),
            }
        })
    }

    /// Mint an API token for the current user (or `forUser`). Resolves to the
    /// msgpack of the created token view, or a DB-error marker `Buffer`.
    #[napi]
    pub fn create_api_token<'env>(
        &self,
        env: &'env Env,
        opts: CreateApiTokenOptions,
    ) -> Result<PromiseRaw<'env, Buffer>> {
        token::create_api_token(self, env, opts)
    }

    /// Grant or revoke the `replicator` role on an existing SCRAM user via
    /// the dedicated `SetReplicator` wire op (the `replicator` pseudo-role is
    /// reserved — it cannot be attached through `create_scram_user`'s generic
    /// `roles` array). Requires the current session to belong to a superuser.
    ///
    /// Domain-level DB errors (e.g. `permission_denied`, `hmac_required`,
    /// `not_found`) surface through the same `DbResponse::Error`-marker
    /// convention as `create_scram_user` / `repl` — the JS wrapper decodes the
    /// returned `Buffer` and throws a typed `ShamirDbError`.
    #[napi]
    pub fn set_replicator<'env>(
        &self,
        env: &'env Env,
        user: String,
        on: bool,
    ) -> Result<PromiseRaw<'env, Buffer>> {
        let deadline = self.request_deadline();
        let timeout = self.request_timeout;
        let user_for_echo = user.clone();
        let state = Arc::clone(&self.inner);
        let mut shutdown = self.shutdown.subscribe();
        env.spawn_future(async move {
            let result = run_request(&state, &mut shutdown, deadline, timeout, |client| {
                Box::pin(async move { client.set_replicator(&user, on).await })
            })
            .await?;
            match result {
                Ok(()) => {
                    let resp = DbResponse::ReplicatorSet {
                        user: user_for_echo,
                        on,
                    };
                    let bytes = rmp_serde::to_vec_named(&resp).map_err(|e| {
                        Error::from_reason(format!("encode set_replicator response: {e}"))
                    })?;
                    Ok(Buffer::from(bytes))
                }
                Err(core::ClientError::Db { code, message }) => encode_db_error(code, message),
                Err(error) => Err(infra_error(error)),
            }
        })
    }

    /// Close the TLS write half cleanly. Idempotent — second call is
    /// a no-op.
    #[napi(ts_return_type = "Promise<void>")]
    pub fn close<'env>(&mut self, env: &'env Env) -> Result<PromiseRaw<'env, ()>> {
        let deadline = self.request_deadline();
        self.shutdown.send_replace(true);
        self.secrets.clear();
        let timeout = self.request_timeout;
        let state = Arc::clone(&self.inner);
        env.spawn_future(async move {
            let closing = async move {
                let client = { state.write().await.take() };
                if let Some(client) = client {
                    client.close().await;
                }
                Ok::<(), core::ClientError>(())
            };
            match core::bound_until(deadline, closing).await {
                Ok(Some(Ok(()))) => Ok(()),
                Ok(Some(Err(error))) => Err(infra_error(error)),
                Ok(None) => Err(request_timeout_error(timeout)),
                Err(error) => Err(infra_error(error)),
            }
        })
    }
}

impl ShamirClient {
    /// Shared-request-state parts, captured before dispatch (105a725d).
    pub(crate) fn request_parts(
        &self,
    ) -> (
        SharedState,
        watch::Receiver<bool>,
        Option<tokio::time::Instant>,
        Option<Duration>,
    ) {
        (
            Arc::clone(&self.inner),
            self.shutdown.subscribe(),
            self.request_deadline(),
            self.request_timeout,
        )
    }

    /// Absolute per-request deadline, captured at the FIRST statement of
    /// every async method so the budget covers decoding, the client-mutex
    /// wait, the write and the response wait (RV-CLIENT-NODE-1CFB3796).
    fn request_deadline(&self) -> Option<tokio::time::Instant> {
        self.request_timeout
            .map(|d| tokio::time::Instant::now() + d)
    }
}

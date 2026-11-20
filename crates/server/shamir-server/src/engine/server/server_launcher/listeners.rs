//! Listener bring-up for [`ServerLauncher::launch`](super::ServerLauncher):
//! TLS material, the per-kind bind + accept-loop spawn and the shared
//! per-connection context construction.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use shamir_transport_http::BodyBudget;

use crate::db_handler::ShamirDbHandler;
use crate::http_rest::RestGate;
use crate::http_ws::{PreAuthAdmission, TokenWsCancelRegistry};
use crate::tokens::TokenService;

use shamir_connect::common::kdf_params::KdfParams;
use shamir_connect::common::types::{BindingMode, TransportKind};
use shamir_connect::server::argon2_semaphore::Argon2Semaphore;
use shamir_connect::server::audit_chain::AuditChainWriter;
use shamir_connect::server::config::ServerSecrets;
use shamir_connect::server::dispatch::RequestHandler;
use shamir_connect::server::lockout::InMemoryLockoutStore;
use shamir_connect::server::rate_limit::InMemoryRateLimiter;
use shamir_connect::server::resume::ResumeConfig;
use shamir_connect::server::rotation::ServerIdentityState;
use shamir_connect::server::session::SessionStore;

use shamir_transport_ipc::IpcListener;
use shamir_transport_tcp::listener::{
    bind_validated as bind_tcp, ListenerProfile as TcpListenerProfile,
};
use shamir_transport_ws::listener::{bind_validated as bind_ws, WsListenerProfile};
use shamir_transport_ws::server::{BROWSER_WS_PATH, NATIVE_WS_PATH};
use shamir_transport_ws::BrowserOriginPolicy;

use tokio::task::JoinHandle;
use tokio_rustls::TlsAcceptor;
use tokio_util::sync::CancellationToken;

use crate::config::{Config, ListenerConfig, ListenerKind, ProfileKind};
use crate::conn_limiter::{ConnLimiter, PerIpLimiter};
use crate::connection::{ConnTasks, ConnectionContext};
use crate::server::boot_error::BootError;
use crate::server_meta::ServerMetaStore;
use crate::tls::{load_or_generate, subject_alts_from_addrs, LoadedTls};
use crate::user_directory::FjallUserDirectory;

use super::accept_loops::{
    accept_loop_ipc, accept_loop_plain, accept_loop_tcp, accept_loop_ws_browser,
    accept_loop_ws_native, WsAcceptParams,
};
use super::helpers::{browser_origin_policy_from, build_ctx, kdf_from_config, AcceptPath};
use super::http::{start_http_listener, LoginResources};

/// Everything a listener needs to build its per-connection context, borrowed
/// from the launcher's locals.
pub(super) struct ListenerEnv<'a> {
    pub(super) config: &'a Config,
    pub(super) identity: &'a Arc<ServerIdentityState>,
    pub(super) identity_seed: [u8; 32],
    pub(super) secrets: &'a Arc<ServerSecrets>,
    pub(super) kdf: KdfParams,
    pub(super) session_store: &'a Arc<SessionStore>,
    pub(super) user_dir: &'a Arc<FjallUserDirectory>,
    pub(super) lockout: &'a Arc<InMemoryLockoutStore>,
    pub(super) rate_limit: &'a Arc<InMemoryRateLimiter>,
    pub(super) argon2_sem: &'a Arc<Argon2Semaphore>,
    pub(super) audit_writer: &'a Arc<AuditChainWriter>,
    pub(super) resume_config: &'a Arc<ResumeConfig>,
    pub(super) handler: &'a Arc<dyn RequestHandler>,
    pub(super) meta: &'a Arc<ServerMetaStore>,
    pub(super) token_service: &'a Arc<TokenService>,
    pub(super) token_ws_cancel_registry: &'a Arc<TokenWsCancelRegistry>,
    pub(super) pre_auth_admission: &'a Arc<PreAuthAdmission>,
    pub(super) handler_concrete: &'a Arc<ShamirDbHandler>,
    pub(super) conn_tasks: &'a ConnTasks,
    pub(super) read_only: bool,
}

/// Listener results (in listener order) and their shared connection limiter.
pub(super) struct SpawnedListeners {
    pub(super) conn_limiter: ConnLimiter,
    pub(super) bound_addrs: Vec<Option<SocketAddr>>,
    pub(super) bound_ipc_paths: Vec<Option<String>>,
    pub(super) listener_tasks: Vec<JoinHandle<()>>,
    pub(super) http_gates: Vec<Arc<RestGate>>,
}

impl ListenerEnv<'_> {
    /// Per-connection context for one listener.
    fn context(
        &self,
        binding_mode: BindingMode,
        transport_kind: TransportKind,
        l: &ListenerConfig,
    ) -> Arc<ConnectionContext> {
        // Connection-level security knobs from config (slow-loris timeout, etc.)
        let auth_init_timeout =
            Duration::from_millis(self.config.security.connection.auth_init_timeout_ms);
        build_ctx(
            self.identity,
            self.identity_seed,
            self.secrets,
            self.kdf,
            self.session_store,
            self.user_dir,
            self.lockout.clone(),
            self.rate_limit.clone(),
            self.argon2_sem,
            self.audit_writer,
            self.resume_config,
            self.handler,
            binding_mode,
            transport_kind,
            l.kdf_override.as_ref().map(kdf_from_config),
            auth_init_timeout,
            self.meta,
            self.conn_tasks.clone(),
        )
    }
}

/// Load the TLS material, bind every configured listener and spawn its
/// accept loop on `shutdown_token`.
pub(super) async fn spawn_listeners(
    env: &ListenerEnv<'_>,
    shutdown_token: &CancellationToken,
) -> Result<SpawnedListeners, BootError> {
    let config = env.config;
    // 9. TLS material — load or generate before binding any TCP+TLS
    //    listener so we can fail fast on missing PEMs.
    //
    // We generate SAN entries lazily for now — the cert covers
    // `localhost` and every literal IP in the configured listeners.
    // Self-signed cert chain is informational only (Ed25519 pin in
    // `auth_ok` is the real identity check).
    let parsed_addrs: Vec<SocketAddr> = config
        .listeners
        .iter()
        .filter_map(|l| l.addr.parse::<SocketAddr>().ok())
        .collect();
    let LoadedTls {
        server_config: tls_server,
        generated,
    } = load_or_generate(
        &config.tls.cert_path,
        &config.tls.key_path,
        subject_alts_from_addrs(&parsed_addrs),
    )?;
    if generated {
        tracing::warn!(
            cert_path = ?config.tls.cert_path,
            "tls: self-signed cert generated — clients pin the Ed25519 \
             identity, the X.509 chain is informational only",
        );
    }
    let tls_acceptor = TlsAcceptor::from(tls_server);

    // 10. Spawn accept loops. They share the root `shutdown_token`
    //     created at the top of `launch()` — `shutdown()` cancels it
    //     once and every accept loop observes the cancel on its next
    //     `select!` poll.
    let mut bound_addrs: Vec<Option<SocketAddr>> = Vec::with_capacity(config.listeners.len());
    // Companion to `bound_addrs`, same length/order — `kind: unix`
    // listeners have no `SocketAddr` to report (see the early-return
    // branch below), so they get `None` in `bound_addrs` and `Some(path)`
    // here instead. Every other listener kind pushes `None` here.
    let mut bound_ipc_paths: Vec<Option<String>> = Vec::with_capacity(config.listeners.len());
    let mut listener_tasks: Vec<JoinHandle<()>> = Vec::new();

    // Global cap on simultaneously-active connections — shared across
    // every listener.
    let conn_limiter = ConnLimiter::new(config.security.connection.max_active_connections);
    // Per-source-IP cap — bounds a single attacker host to a small
    // fraction of the global cap (audit §2a / top-5 #4). `0` = off.
    let per_ip_limiter =
        PerIpLimiter::new(config.security.connection.max_active_connections_per_ip);
    let body_budget = Arc::new(BodyBudget::new(
        config
            .listeners
            .iter()
            .filter(|l| l.kind == ListenerKind::Http)
            .map(|l| l.http.max_request_body_bytes)
            .max()
            .unwrap_or(0)
            .saturating_mul(16),
    ));
    let mut http_gates = Vec::new();

    let login_resources = config
        .listeners
        .iter()
        .any(|listener| listener.kind == ListenerKind::Http && listener.http.login.enabled)
        .then(LoginResources::new);

    let result = async {
        for l in &config.listeners {
            if l.kind == ListenerKind::Http {
                let (address, task, gate) = start_http_listener(
                    env,
                    l,
                    &conn_limiter,
                    &per_ip_limiter,
                    Arc::clone(&body_budget),
                    login_resources.as_ref(),
                    shutdown_token,
                )
                .await?;
                bound_addrs.push(Some(address));
                bound_ipc_paths.push(None);
                listener_tasks.push(task);
                http_gates.push(gate);
                continue;
            }
            // Local IPC (`kind: unix`) addresses a filesystem path / pipe
            // name, not a `SocketAddr` — handled entirely on its own path,
            // separate from the TCP/WS SocketAddr-based dispatch below
            // (`Config::validate` already rejected any other `l.kind` +
            // `l.addr` combination that wouldn't make sense here).
            if l.kind == ListenerKind::Unix {
                let listener = IpcListener::bind(&l.addr)
                    .await
                    .map_err(|e| BootError::Bind(format!("unix {}: {e}", l.addr)))?;
                tracing::info!(path = %l.addr, kind = ?l.kind, profile = ?l.profile, "listener bound");
                let ctx = env.context(BindingMode::None, TransportKind::Unix, l);
                let token = shutdown_token.clone();
                let limiter = conn_limiter.clone();
                let per_ip = per_ip_limiter.clone();
                listener_tasks.push(tokio::spawn(accept_loop_ipc(
                    listener, ctx, token, limiter, per_ip,
                )));
                bound_addrs.push(None);
                bound_ipc_paths.push(Some(l.addr.clone()));
                continue;
            }
            bound_ipc_paths.push(None);

            let addr: SocketAddr = match l.addr.parse() {
                Ok(a) => a,
                Err(e) => {
                    return Err(BootError::Bind(format!(
                        "addr {} is not a SocketAddr: {e}",
                        l.addr
                    )));
                }
            };

            // Resolve listener transport dispatch.
            let (transport_kind, binding_mode, accept_path) = match (l.kind, l.profile) {
                (ListenerKind::Tcp, Some(ProfileKind::TlsExporter)) => (
                    TransportKind::Tcp,
                    BindingMode::TlsExporter,
                    AcceptPath::TcpTlsExporter,
                ),
                (ListenerKind::Tcp, Some(ProfileKind::Plain)) => {
                    (TransportKind::Tcp, BindingMode::None, AcceptPath::TcpPlain)
                }
                (ListenerKind::Ws, Some(ProfileKind::TlsExporter)) => (
                    TransportKind::WebSocket,
                    BindingMode::TlsExporter,
                    AcceptPath::WsNative,
                ),
                (ListenerKind::Ws, Some(ProfileKind::TlsNoExport)) => {
                    let policy = browser_origin_policy_from(&l.browser_origin_allowlist)?;
                    (
                        TransportKind::WebSocket,
                        BindingMode::TlsNoExport,
                        AcceptPath::WsBrowser(policy),
                    )
                }
                (ListenerKind::Http, _) => unreachable!("HTTP handled at loop entry"),
                (kind, profile) => {
                    tracing::warn!(
                        addr = %addr,
                        ?kind,
                        ?profile,
                        "listener skipped (unsupported MVP combination)",
                    );
                    bound_addrs.push(None);
                    continue;
                }
            };

            let local_addr = match accept_path {
                AcceptPath::TcpTlsExporter => {
                    let listener = bind_tcp(addr, TcpListenerProfile::TlsExporter)
                        .await
                        .map_err(|e| BootError::Bind(format!("tcp {addr}: {e}")))?;
                    let local_addr = listener
                        .local_addr()
                        .map_err(|e| BootError::Bind(format!("local_addr: {e}")))?;
                    tracing::info!(local_addr = %local_addr, kind = ?l.kind, profile = ?l.profile, "listener bound");
                    let ctx = env.context(binding_mode, transport_kind, l);
                    let acceptor = tls_acceptor.clone();
                    let token = shutdown_token.clone();
                    let limiter = conn_limiter.clone();
                    let per_ip = per_ip_limiter.clone();
                    listener_tasks.push(tokio::spawn(accept_loop_tcp(
                        listener, acceptor, ctx, token, limiter, per_ip,
                    )));
                    local_addr
                }
                AcceptPath::TcpPlain => {
                    let listener = bind_tcp(addr, TcpListenerProfile::Plain)
                        .await
                        .map_err(|e| BootError::Bind(format!("tcp {addr}: {e}")))?;
                    let local_addr = listener
                        .local_addr()
                        .map_err(|e| BootError::Bind(format!("local_addr: {e}")))?;
                    tracing::info!(local_addr = %local_addr, kind = ?l.kind, profile = ?l.profile, "listener bound");
                    let ctx = env.context(binding_mode, transport_kind, l);
                    let token = shutdown_token.clone();
                    let limiter = conn_limiter.clone();
                    let per_ip = per_ip_limiter.clone();
                    listener_tasks.push(tokio::spawn(accept_loop_plain(
                        listener, ctx, token, limiter, per_ip,
                    )));
                    local_addr
                }
                AcceptPath::WsNative => {
                    let listener = bind_ws(addr, WsListenerProfile::Wss)
                        .await
                        .map_err(|e| BootError::Bind(format!("ws {addr}: {e}")))?;
                    let local_addr = listener
                        .local_addr()
                        .map_err(|e| BootError::Bind(format!("local_addr: {e}")))?;
                    tracing::info!(local_addr = %local_addr, kind = ?l.kind, profile = ?l.profile, "listener bound");
                    let ctx = env.context(binding_mode, transport_kind, l);
                    let acceptor = tls_acceptor.clone();
                    let token = shutdown_token.clone();
                    let limiter = conn_limiter.clone();
                    let per_ip = per_ip_limiter.clone();
                    let path = l.path.clone().unwrap_or_else(|| NATIVE_WS_PATH.to_string());
                    let policy = Arc::new(
                        BrowserOriginPolicy::try_allow(l.browser_origin_allowlist.iter().cloned())
                            .map_err(BootError::Bind)?,
                    );
                    listener_tasks.push(tokio::spawn(accept_loop_ws_native(
                        listener,
                        acceptor,
                        ctx,
                        token,
                        limiter,
                        per_ip,
                        WsAcceptParams { path, policy },
                    )));
                    local_addr
                }
                AcceptPath::WsBrowser(policy) => {
                    let listener = bind_ws(addr, WsListenerProfile::WssBrowser)
                        .await
                        .map_err(|e| BootError::Bind(format!("ws {addr}: {e}")))?;
                    let local_addr = listener
                        .local_addr()
                        .map_err(|e| BootError::Bind(format!("local_addr: {e}")))?;
                    tracing::info!(local_addr = %local_addr, kind = ?l.kind, profile = ?l.profile, "listener bound");
                    let ctx = env.context(binding_mode, transport_kind, l);
                    let acceptor = tls_acceptor.clone();
                    let token = shutdown_token.clone();
                    let limiter = conn_limiter.clone();
                    let per_ip = per_ip_limiter.clone();
                    let path = l
                        .path
                        .clone()
                        .unwrap_or_else(|| BROWSER_WS_PATH.to_string());
                    listener_tasks.push(tokio::spawn(accept_loop_ws_browser(
                        listener,
                        acceptor,
                        ctx,
                        token,
                        limiter,
                        per_ip,
                        WsAcceptParams {
                            path,
                            policy: Arc::new(policy),
                        },
                    )));
                    local_addr
                }
            };
            bound_addrs.push(Some(local_addr));
        }

        Ok::<(), BootError>(())
    }
    .await;

    if let Err(error) = result {
        env.handler_concrete.cancel_refusal_audit().close();
        shutdown_token.cancel();
        let listener_deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        for mut task in listener_tasks.drain(..) {
            let abort = task.abort_handle();
            match tokio::time::timeout_at(listener_deadline, &mut task).await {
                Ok(Ok(())) => {}
                Ok(Err(join_error)) => {
                    tracing::error!(%join_error, "listener task failed during boot rollback");
                }
                Err(_) => {
                    tracing::error!("HTTP listener drain exceeded during boot rollback; aborting");
                    abort.abort();
                    if tokio::time::timeout(Duration::from_secs(2), &mut task)
                        .await
                        .is_err()
                    {
                        tracing::error!("listener task did not stop after boot rollback abort");
                    }
                }
            }
        }
        if tokio::time::timeout(Duration::from_secs(10), env.conn_tasks.wait_idle())
            .await
            .is_err()
        {
            env.conn_tasks.abort_all();
            if tokio::time::timeout(Duration::from_secs(2), env.conn_tasks.wait_idle())
                .await
                .is_err()
            {
                tracing::error!("connection tasks did not quiesce after boot rollback abort");
            }
        }
        for gate in http_gates.drain(..) {
            gate.shutdown().await;
        }
        return Err(error);
    }

    Ok(SpawnedListeners {
        conn_limiter,
        bound_addrs,
        bound_ipc_paths,
        listener_tasks,
        http_gates,
    })
}

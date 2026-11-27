use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use shamir_connect::server::admin::UserDirectory;
use shamir_query_types::wire::DbRequest;
use shamir_transport_http::{
    serve_with_routes, BodyBudget, ConnAdmission, EndpointSet, NoLogin, RouteServices,
};
use shamir_tunables::instance_defaults::{CONN_MAX_IN_FLIGHT, WS_PING_INTERVAL, WS_PONG_TIMEOUT};

use super::super::listeners::ListenerEnv;
use super::proxy::{Cidr, ProxyGate};
use super::validate_http_tls;
use crate::boot::config::{to_http_server_config, ListenerConfig, ListenerKind};
use crate::conn_limiter::{ConnLimiter, PerIpLimiter};
use crate::http_login::admission::{AdmissionState, LoginClock, LoginHeadPolicy};
use crate::http_login::pending::PendingStore;
use crate::http_login::service::{LoginAudit, LoginServiceContext, LoginServiceOptions};
use crate::http_login::set_ineligible_users;
use crate::http_login::HttpLoginRoutes;
use crate::http_rest::{RestGate, RestGateRails, RestHandler};
use crate::http_ws::WsUpgradeService;
use crate::server::boot_error::BootError;
use shamir_connect::server::token_session::TokenSocketCaps;
use shamir_transport_http::PeerInfo;

use crate::server::server_handle::HttpListenerOutcome;

/// Bound for joining the HTTP login audit consumer at shutdown (2 s; the
/// same bound `drain_audit_task` uses — the consumer is cooperative and its
/// already-running blocking appends are a known limit, see the operations
/// guide).
const AUDIT_CONSUMER_DRAIN: Duration = Duration::from_secs(2);

type StartedHttp = (SocketAddr, JoinHandle<HttpListenerOutcome>, Arc<RestGate>);
type UsernameLookup = Arc<dyn Fn(&[u8; 16]) -> String + Send + Sync>;

async fn drain_audit_task(task: &mut JoinHandle<()>) -> Result<(), tokio::task::JoinError> {
    match tokio::time::timeout(std::time::Duration::from_secs(2), &mut *task).await {
        Ok(result) => result,
        Err(_) => {
            task.abort();
            task.await
        }
    }
}

struct PendingClock;
impl LoginClock for PendingClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

/// Login state shared across HTTP listeners so pending challenges and admission limits are global.
pub(in crate::engine::server::server_launcher) struct LoginResources {
    pending: Arc<PendingStore>,
    admission: Arc<AdmissionState>,
}

impl LoginResources {
    /// Creates the login state shared globally by all HTTP listeners.
    pub(in crate::engine::server::server_launcher) fn new() -> Self {
        let clock: Arc<dyn LoginClock> = Arc::new(PendingClock);
        Self {
            pending: Arc::new(PendingStore::new(clock)),
            admission: Arc::new(AdmissionState::system()),
        }
    }
}

struct HttpAdmission {
    global: ConnLimiter,
    per_ip: PerIpLimiter,
    trusted: Vec<Cidr>,
}

impl ConnAdmission for HttpAdmission {
    fn admit(&self, peer: &SocketAddr) -> Option<Box<dyn Send + Sync>> {
        let global = self.global.try_acquire()?;
        let per_ip = if self.trusted.iter().any(|cidr| cidr.contains(peer.ip())) {
            None
        } else {
            Some(self.per_ip.try_acquire(peer.ip())?)
        };
        Some(Box::new((global, per_ip)))
    }
}

pub(in crate::engine::server::server_launcher) async fn start_http_listener(
    env: &ListenerEnv<'_>,
    listener_config: &ListenerConfig,
    global: &ConnLimiter,
    per_ip: &PerIpLimiter,
    body_budget: Arc<BodyBudget>,
    login_resources: Option<&LoginResources>,
    shutdown: &CancellationToken,
) -> Result<StartedHttp, BootError> {
    if listener_config.kind != ListenerKind::Http {
        return Err(BootError::Bind(
            "HTTP listener setup used for non-HTTP listener".into(),
        ));
    }
    let settings = &listener_config.http;
    if settings.login.enabled {
        tracing::info!(
            "{}",
            HttpLoginRoutes::describe_origin_policy(listener_config.browser_origin_allowlist.len())
        );
    }
    let address: SocketAddr = listener_config.addr.parse().map_err(|error| {
        BootError::Bind(format!("HTTP address {}: {error}", listener_config.addr))
    })?;
    let listener = TcpListener::bind(address)
        .await
        .map_err(|error| BootError::Bind(format!("HTTP bind {address}: {error}")))?;
    let address = listener
        .local_addr()
        .map_err(|error| BootError::Bind(format!("HTTP local address: {error}")))?;
    let request_timeout_ms = settings.request_timeout_ms;
    let tls_enabled = settings.tls == Some(true);
    let tls = if tls_enabled {
        let validated = validate_http_tls(&env.config.tls, address, settings.allow_self_signed)?;
        Some(
            shamir_transport_http::server::accept::tls13_config(&validated)
                .map_err(|error| BootError::Bind(format!("HTTP TLS config: {error}")))?,
        )
    } else {
        None
    };
    let gate_clock: Arc<dyn Fn() -> Option<u64> + Send + Sync> = Arc::new(|| {
        shamir_connect::common::time::UnixNanos::try_now()
            .ok()
            .map(|value| value.as_u64())
    });
    let username = Arc::clone(env.user_dir);
    let username_lookup: UsernameLookup = Arc::new(move |id| {
        username
            .state_by_user_id(id)
            .map(|state| state.username)
            .unwrap_or_default()
    });
    let rails = RestGateRails {
        plaintext_remote: settings.tls == Some(false) && !address.ip().is_loopback(),
        allow_plaintext_write: settings.allow_plaintext_write,
    };
    if rails.plaintext_remote {
        tracing::warn!(local_addr = %address, "remote plaintext HTTP exposes bearer tokens");
        metrics::describe_gauge!(
            "shamir_token_listener_plaintext_remote",
            "Whether a remote plaintext token listener is enabled"
        );
        metrics::gauge!("shamir_token_listener_plaintext_remote").set(1.0);
    }
    let (gate, executor) = RestHandler::new(
        Arc::clone(env.token_service),
        Arc::clone(env.handler_concrete),
        gate_clock,
        Arc::clone(&username_lookup),
        rails,
    );
    let gate = Arc::new(gate);
    let proxy_gate = match ProxyGate::new(Arc::clone(&gate), &settings.trusted_proxies) {
        Ok(proxy_gate) => Arc::new(proxy_gate),
        Err(error) => {
            gate.shutdown().await;
            return Err(error);
        }
    };
    let forbidden_body = Arc::new(|code: &str| {
        let response = shamir_query_types::wire::DbResponse::Error {
            code: code.to_string(),
            message: code.to_string(),
        };
        rmp_serde::to_vec_named(&response)
            .map(bytes::Bytes::from)
            .unwrap_or_else(|_| {
                bytes::Bytes::from_static(
                    b"\x83\xa4kind\xa5error\xa4code\xabserver_busy\xa7message\xabserver_busy",
                )
            })
    });
    let ping = rmp_serde::to_vec_named(&DbRequest::Ping)
        .map(bytes::Bytes::from)
        .map_err(|error| BootError::Bind(format!("HTTP ping encoding: {error}")));
    let config = match ping.and_then(|ping| {
        to_http_server_config(
            settings,
            EndpointSet {
                rest: true,
                ws: true,
            },
            body_budget,
            env.config.security.connection.auth_init_timeout_ms,
            tls,
            forbidden_body,
            ping,
        )
        .map_err(|error| BootError::Bind(error.to_string()))
    }) {
        Ok(config) => config,
        Err(error) => {
            gate.shutdown().await;
            return Err(error);
        }
    };
    let login = settings.login.enabled;
    let trusted = if login && settings.external_tls {
        settings
            .trusted_proxies
            .iter()
            .filter_map(|proxy| super::proxy::parse_cidr(proxy))
            .collect()
    } else {
        Vec::new()
    };
    let admission = Arc::new(HttpAdmission {
        global: global.clone(),
        per_ip: per_ip.clone(),
        trusted,
    });
    let routes = if login {
        let listener_id_result = if let Some(value) = &settings.login.listener_id {
            hex::decode(value)
                .map_err(|error| BootError::Bind(format!("HTTP login listener_id: {error}")))
                .and_then(|decoded| {
                    let mut id = [0u8; 16];
                    if decoded.len() != id.len() {
                        return Err(BootError::Bind(
                            "HTTP login listener_id must be 16 bytes".into(),
                        ));
                    }
                    id.copy_from_slice(&decoded);
                    Ok(id)
                })
        } else {
            let meta = Arc::clone(env.meta);
            let addr = listener_config.addr.clone();
            tokio::task::spawn_blocking(move || meta.http_login_listener_id(&addr))
                .await
                .map_err(|error| BootError::Bind(format!("HTTP login listener id task: {error}")))
                .and_then(|result| {
                    result.map_err(|error| {
                        BootError::Bind(format!("HTTP login listener id: {error}"))
                    })
                })
        };
        let listener_id = match listener_id_result {
            Ok(id) => id,
            Err(error) => {
                gate.shutdown().await;
                return Err(error);
            }
        };
        let resources = match login_resources {
            Some(resources) => resources,
            None => {
                gate.shutdown().await;
                return Err(BootError::Bind(
                    "HTTP login resources were not initialized".into(),
                ));
            }
        };
        let pending = Arc::clone(&resources.pending);
        let login_admission = Arc::clone(&resources.admission);
        let audit_shutdown = CancellationToken::new();
        let (audit, audit_task) =
            LoginAudit::start(audit_shutdown.clone(), Arc::clone(env.audit_writer));
        let options = LoginServiceOptions {
            users: Arc::clone(env.user_dir),
            tokens: Arc::clone(env.token_service),
            lockout: Arc::clone(env.lockout),
            secrets: Arc::clone(env.secrets),
            default_kdf: env.kdf,
            hardening: env.config.security.login_enumeration_hardening,
            security_class: if settings.external_tls {
                2
            } else if tls_enabled {
                1
            } else {
                0
            },
            listener_id,
            pending,
            admission: login_admission,
            token_config: env.config.tokens.clone(),
            read_only: env.read_only,
            unix_clock: Arc::new(|| {
                shamir_connect::common::time::UnixNanos::try_now()
                    .ok()
                    .map(|v| v.as_u64())
            }),
            audit: Some(audit),
        };
        let context = match LoginServiceContext::new(options) {
            Ok(context) => Arc::new(context),
            Err(error) => {
                gate.shutdown().await;
                audit_shutdown.cancel();
                let mut audit_task = audit_task;
                let _ = drain_audit_task(&mut audit_task).await;
                return Err(BootError::Bind(error.into()));
            }
        };
        let policy = match LoginHeadPolicy::new(
            if settings.external_tls {
                2
            } else if tls_enabled {
                1
            } else {
                0
            },
            settings.login.authority_allowlist.clone(),
            listener_config.browser_origin_allowlist.clone(),
            settings.trusted_proxies.clone(),
            address,
        ) {
            Ok(policy) => policy,
            Err(error) => {
                gate.shutdown().await;
                audit_shutdown.cancel();
                let mut audit_task = audit_task;
                let _ = drain_audit_task(&mut audit_task).await;
                return Err(BootError::Bind(format!(
                    "HTTP login policy: {}",
                    error.code
                )));
            }
        };
        let login_count_dir = Arc::clone(env.user_dir);
        let login_default_kdf = env.kdf;
        let hardening = env.config.security.login_enumeration_hardening;
        let count_result = tokio::task::spawn_blocking(move || {
            login_count_dir.list_all().map(|users| {
                users
                    .into_iter()
                    .filter(|(_, state)| {
                        login_count_dir
                            .lookup_by_name(&state.username)
                            .is_some_and(|record| {
                                record.kdf_params.validate_client_limits().is_err()
                                    || (hardening && record.kdf_params != login_default_kdf)
                            })
                    })
                    .count()
            })
        })
        .await;
        match count_result {
            Ok(Ok(count)) => {
                set_ineligible_users(count);
                if count > 0 {
                    tracing::warn!(count, "HTTP login KDF-ineligible users at startup");
                }
            }
            _ => {
                gate.shutdown().await;
                audit_shutdown.cancel();
                let mut audit_task = audit_task;
                let _ = drain_audit_task(&mut audit_task).await;
                return Err(BootError::Bind(
                    "HTTP login account preflight failed".into(),
                ));
            }
        }
        Some((
            Arc::new(HttpLoginRoutes::new(true, context, policy)),
            audit_task,
            audit_shutdown,
        ))
    } else {
        None
    };
    let task_gate = proxy_gate;
    let task_shutdown = shutdown.child_token();
    for result in [
        "success",
        "invalid",
        "forbidden",
        "rate_limited",
        "unavailable",
    ] {
        metrics::counter!("shamir_token_auth_total", "channel" => "ws", "result" => result)
            .increment(0);
    }
    metrics::counter!("shamir_token_request_denied_total", "endpoint" => "ws").increment(0);
    let ws_service = Arc::new(WsUpgradeService {
        gate: Arc::clone(&gate),
        service: Arc::clone(env.token_service),
        handler: env.handler_concrete.clone(),
        registry: Arc::clone(env.token_ws_cancel_registry),
        pre_auth: Arc::clone(env.pre_auth_admission),
        tls: settings.tls == Some(true),
        browser_available: settings.tls == Some(true) || address.ip().is_loopback(),
        origins: listener_config.browser_origin_allowlist.clone(),
        host_allowlist: settings.host_authority_allowlist.clone(),
        username: Arc::clone(&username_lookup),
        caps: TokenSocketCaps {
            per_token: env.config.tokens.max_sockets_per_token,
            per_owner: env.config.tokens.max_sockets_per_owner,
        },
        session_max_age_hours: env.config.tokens.session_max_age_hours,
        ping_interval: WS_PING_INTERVAL,
        pong_timeout: WS_PONG_TIMEOUT,
        max_in_flight: CONN_MAX_IN_FLIGHT,
        resolve_peer: Arc::new({
            let trusted = settings
                .trusted_proxies
                .iter()
                .filter_map(|item| super::proxy::parse_cidr(item))
                .collect::<Vec<_>>();
            move |peer: &PeerInfo| super::proxy::resolve_peer(peer, &trusted)
        }),
        runner: Arc::new(|context, socket| {
            Box::pin(crate::connection::token_ws::run_token_loop(context, socket))
        }),
    });
    let span = tracing::Span::current();
    let task = tokio::spawn(
        async move {
            let config = Arc::new(config);
            let executor = Arc::new(executor);
            // The drain failures here are TYPED unsettled stages of the
            // shutdown report (see `HttpListenerOutcome`): aborting the
            // audit consumer does not stop an already running blocking
            // append, and workers left detached after the drain budget are
            // not quiescence — the report must be unclean and the
            // data-directory lock kept.
            let mut outcome = HttpListenerOutcome::settled();
            let result = if let Some((routes, audit_task, audit_shutdown)) = routes {
                let _audit_stop = audit_shutdown.clone().drop_guard();
                let result = serve_with_routes(
                    listener,
                    config,
                    task_gate,
                    executor,
                    admission,
                    task_shutdown.clone(),
                    RouteServices {
                        login: Arc::clone(&routes),
                        ws: ws_service,
                    },
                )
                .await;
                let worker_drain = HttpLoginRoutes::worker_drain_budget(request_timeout_ms);
                outcome.login_worker_budget = worker_drain;
                if !routes.shutdown(worker_drain).await {
                    outcome.login_workers_settled = false;
                    tracing::warn!(drain_ms = worker_drain.as_millis(), "HTTP login worker drain timed out; workers remain detached; later audit events may be lost");
                }
                audit_shutdown.cancel();
                let mut audit_task = audit_task;
                outcome.audit_consumer_budget = AUDIT_CONSUMER_DRAIN;
                if tokio::time::timeout(AUDIT_CONSUMER_DRAIN, &mut audit_task)
                    .await
                    .is_err()
                {
                    outcome.audit_consumer_settled = false;
                    tracing::warn!("HTTP login audit consumer drain timed out; queued and late events may be lost");
                    audit_task.abort();
                    let _ = audit_task.await;
                }
                result
            } else {
                serve_with_routes(
                    listener,
                    config,
                    task_gate,
                    executor,
                    admission,
                    task_shutdown,
                    RouteServices {
                        login: Arc::new(NoLogin),
                        ws: ws_service,
                    },
                )
                .await
            };
            if let Err(error) = result {
                tracing::error!(%error, "HTTP listener stopped");
            }
            outcome
        }
        .instrument(span),
    );
    tracing::info!(local_addr = %address, kind = "http", "listener bound");
    Ok((address, task, gate))
}

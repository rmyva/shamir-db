use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use shamir_connect::server::conn_services::ConnectionServices;
use shamir_connect::server::session::Session;
use shamir_connect::server::token::{Endpoint as TokenEndpoint, Verified};
use shamir_query_types::wire::{DbRequest, DbResponse};
use shamir_transport_http::{ExecOutcome, ExecStatus, RestExecutor};

use crate::config::NativeDbHandler;
use crate::security::tokens::TokenService;

use super::bucket::{BucketCache, BucketError};
use super::scope::{classify_request, Decision};
use super::RestGateRails;

type OwnerName = dyn Fn(&[u8; 16]) -> String + Send + Sync;
pub type NativeHandler = dyn for<'a> Fn(
        &'a Session,
        DbRequest,
        &'a ConnectionServices,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, String>> + Send + 'a>>
    + Send
    + Sync;

/// R4b-1 — internal rid-capable closure shape. The trailing `Option<u32>` is
/// the client's OPTIONAL transport request id forwarded from the token
/// WebSocket; the legacy public [`NativeHandler`] alias and signatures are
/// unchanged and keep flowing through with `None`.
type NativeHandlerRid = dyn for<'a> Fn(
        &'a Session,
        DbRequest,
        &'a ConnectionServices,
        Option<u32>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, String>> + Send + 'a>>
    + Send
    + Sync;

/// Native typed handler adapter for requests accepted by REST scope policy.
pub struct ScopedHandler {
    handler: Arc<NativeHandlerRid>,
}

impl ScopedHandler {
    /// Wrap the native database handler contract.
    pub fn new<H>(handler: Arc<H>) -> Self
    where
        H: NativeDbHandler + ?Sized + 'static,
    {
        // R4b-1 — the dispatch closure forwards the OPTIONAL client rid into
        // the rid-carrying native contract so the server can attach it to the
        // in-flight activity entry; implementors using the trait default
        // simply ignore it.
        let dispatch: Arc<NativeHandlerRid> =
            Arc::new(move |session, request, conn, client_rid| {
                let handler = Arc::clone(&handler);
                Box::pin(async move {
                    handler
                        .handle_decoded_with_request_id(session, request, conn, client_rid)
                        .await
                })
            });
        Self { handler: dispatch }
    }

    /// Legacy public constructor over a plain [`NativeHandler`] closure. The
    /// wrapped closure carries no client rid (`None`), exactly as before.
    pub fn with_handler(handler: Arc<NativeHandler>) -> Self {
        let dispatch: Arc<NativeHandlerRid> = Arc::new(move |session, request, conn, _| {
            let handler = Arc::clone(&handler);
            Box::pin(async move { handler(session, request, conn).await })
        });
        Self { handler: dispatch }
    }

    /// Token auth/scope gate shared by both entry points.
    fn authorize(
        &self,
        session: &Session,
        request: &DbRequest,
        endpoint: TokenEndpoint,
    ) -> Result<(), HandleDenial> {
        let shamir_connect::server::SessionOrigin::Token(origin) = &session.origin else {
            return Err(HandleDenial::Forbidden("token_request_denied"));
        };
        let (mode, _, databases) = origin.scope();
        let decision = classify_request(request, mode, databases, endpoint);
        match decision {
            Decision::Allowed(_) => Ok(()),
            Decision::Denied(code) => Err(HandleDenial::Forbidden(code)),
            Decision::NotSupported(code) => Err(HandleDenial::BadRequest(code)),
        }
    }

    pub async fn handle(
        &self,
        session: &Session,
        request: DbRequest,
        endpoint: TokenEndpoint,
        conn: &ConnectionServices,
    ) -> Result<Vec<u8>, HandleDenial> {
        self.authorize(session, &request, endpoint)?;
        // REST and other non-dispatch callers have no client rid: `None`.
        (self.handler)(session, request, conn, None)
            .await
            .map_err(|_| HandleDenial::ApplicationError)
    }

    /// R4b-1 — token-WebSocket entry point: forwards the OPTIONAL envelope
    /// `rid` past the token auth/scope gates into the native dispatch, so it
    /// reaches the server's in-flight activity registration. Gates, ordering
    /// and denial codes are identical to [`Self::handle`].
    pub async fn handle_with_request_id(
        &self,
        session: &Session,
        request: DbRequest,
        endpoint: TokenEndpoint,
        conn: &ConnectionServices,
        request_id: Option<u32>,
    ) -> Result<Vec<u8>, HandleDenial> {
        self.authorize(session, &request, endpoint)?;
        (self.handler)(session, request, conn, request_id)
            .await
            .map_err(|_| HandleDenial::ApplicationError)
    }
}

/// Result code for typed scoped-handler rejections.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HandleDenial {
    Forbidden(&'static str),
    BadRequest(&'static str),
    ApplicationError,
}

/// Concrete REST executor over the native database handler.
pub struct RestHandler {
    service: Arc<TokenService>,
    handler: ScopedHandler,
    clock: Arc<dyn Fn() -> Option<u64> + Send + Sync>,
    username: Arc<OwnerName>,
    buckets: Arc<BucketCache>,
}

impl RestHandler {
    /// Constructs the REST gate and executor for T3b-3 listener wiring.
    pub fn new<H>(
        service: Arc<TokenService>,
        handler: Arc<H>,
        clock: Arc<dyn Fn() -> Option<u64> + Send + Sync>,
        username: Arc<OwnerName>,
        rails: RestGateRails,
    ) -> (super::RestGate, Self)
    where
        H: NativeDbHandler + ?Sized + 'static,
    {
        let buckets = Arc::new(BucketCache::default());
        let gate = super::RestGate::new_with_buckets(
            Arc::clone(&service),
            Arc::clone(&clock),
            rails,
            Arc::clone(&buckets),
        );
        metrics::describe_counter!(
            "shamir_token_request_denied_total",
            metrics::Unit::Count,
            "REST token requests denied by endpoint scope policy"
        );
        metrics::counter!("shamir_token_request_denied_total", "endpoint" => "rest").increment(0);
        metrics::describe_histogram!(
            "shamir_token_request_duration_seconds",
            metrics::Unit::Seconds,
            "REST token request execution duration"
        );
        let executor = Self {
            service,
            handler: ScopedHandler::new(handler),
            clock,
            username,
            buckets,
        };
        (gate, executor)
    }

    pub fn with_scoped_handler(
        service: Arc<TokenService>,
        handler: ScopedHandler,
        clock: Arc<dyn Fn() -> Option<u64> + Send + Sync>,
        username: Arc<OwnerName>,
    ) -> Self {
        Self {
            service,
            handler,
            clock,
            username,
            buckets: Arc::new(BucketCache::default()),
        }
    }

    fn rate_limited(retry: u32) -> ExecOutcome {
        let seconds = retry.max(1);
        let mut outcome = Self::error("rate_limited", ExecStatus::RateLimited);
        outcome.retry_after_secs = Some(u64::from(seconds));
        outcome
    }

    fn error(code: &'static str, status: ExecStatus) -> ExecOutcome {
        let response = DbResponse::Error {
            code: code.to_string(),
            message: code.to_string(),
        };
        let body = rmp_serde::to_vec_named(&response)
            .map(Bytes::from)
            .unwrap_or_else(|_| Bytes::from_static(b"\x81\xa4kind\xa5error"));
        ExecOutcome {
            status,
            body,
            retry_after_secs: None,
            hold: None,
        }
    }
}

impl RestExecutor<Verified> for RestHandler {
    async fn execute(&self, grant: &Verified, body: Bytes) -> ExecOutcome {
        let started = std::time::Instant::now();
        let Some(now_ns) = (self.clock)() else {
            return Self::error("server_busy", ExecStatus::Unavailable);
        };
        if !self.service.still_valid(grant, now_ns) {
            return Self::error("unauthorized", ExecStatus::Unauthorized);
        }
        if shamir_query_types::validate_request_alias_caps(&body).is_err() {
            return Self::error("invalid_request", ExecStatus::BadRequest);
        }
        let request: DbRequest = match rmp_serde::from_slice(&body) {
            Ok(request) => request,
            Err(_) => return Self::error("invalid_request", ExecStatus::BadRequest),
        };
        let record = grant.state.record();
        let decision = classify_request(
            &request,
            record.mode,
            record.databases.as_deref(),
            TokenEndpoint::Rest,
        );
        let operation = match decision {
            Decision::Denied(code) => {
                metrics::counter!("shamir_token_request_denied_total", "endpoint" => "rest")
                    .increment(1);
                return Self::error(code, ExecStatus::Forbidden);
            }
            Decision::NotSupported(code) => return Self::error(code, ExecStatus::BadRequest),
            Decision::Allowed(operation) => operation,
        };
        tracing::trace!(operation = operation.label(), "rest request admitted");
        let (_, session) =
            match self
                .buckets
                .session_for(grant, (self.username)(&record.owner_user_id), now_ns)
            {
                Ok(pair) => pair,
                Err(BucketError::RateLimited(retry)) => {
                    self.service.counters().record_post_auth_rejection();
                    return Self::rate_limited(retry);
                }
                Err(BucketError::Unavailable) => {
                    return Self::error("server_busy", ExecStatus::Unavailable)
                }
            };
        // REST creates no subscription and disables push; conn_id is unused by this request path.
        let conn = ConnectionServices::without_push(0);
        let Some(execution_now_ns) = (self.clock)() else {
            return Self::error("server_busy", ExecStatus::Unavailable);
        };
        if !self.service.still_valid(grant, execution_now_ns) {
            return Self::error("unauthorized", ExecStatus::Unauthorized);
        }
        let handler = self
            .handler
            .handle(&session, request, TokenEndpoint::Rest, &conn);
        let (result, hold) = crate::byte_budget::run_with_handoff(handler).await;
        metrics::histogram!("shamir_token_request_duration_seconds", "endpoint" => "rest")
            .record(started.elapsed().as_secs_f64());
        match result {
            Ok(response) => ExecOutcome {
                status: ExecStatus::Ok,
                body: Bytes::from(response),
                retry_after_secs: None,
                hold: hold.map(|guard| Box::new(guard) as Box<dyn Send + Sync>),
            },
            Err(HandleDenial::Forbidden(code)) => {
                metrics::counter!("shamir_token_request_denied_total", "endpoint" => "rest")
                    .increment(1);
                Self::error(code, ExecStatus::Forbidden)
            }
            Err(HandleDenial::BadRequest(code)) => Self::error(code, ExecStatus::BadRequest),
            Err(HandleDenial::ApplicationError) => Self::error("application_error", ExecStatus::Ok),
        }
    }
}

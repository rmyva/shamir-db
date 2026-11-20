//! Server-side post-handshake request dispatch.
//!
//! Implements the per-request session validity check (spec §7.5 [NORMATIVE])
//! and provides a single entry point for transport bindings to feed in
//! decoded request envelopes and receive response/error envelopes back.
//!
//! Application-level dispatch (what to do with `req` payload) is left to
//! the caller via the [`RequestHandler`] trait.
//!
//! ## Async model
//!
//! [`RequestHandler::handle`] is an async method returning a boxed future.
//! [`dispatch_request`] and [`dispatch_request_view`] are `async fn` that
//! `.await` the handler future directly — no blocking-pool bridge is needed.
//! The request loop stays lock-step (one request at a time per connection);
//! this is preparation for future duplex multiplexing.

//! ## Request id propagation (R4b-1)
//!
//! [`dispatch_request`] and [`dispatch_request_view`] forward the optional
//! envelope `rid` to the handler through
//! [`RequestHandler::handle_with_request_id`]. The default implementation
//! delegates to [`RequestHandler::handle`], so existing handlers need no
//! changes; rid-aware handlers (currently only the server db handler)
//! override it. Callers that reach handlers without going through dispatch
//! (native REST / direct) still invoke plain `handle` and observe no rid.

use crate::common::envelope::{
    ErrorEnvelope, RequestEnvelope, RequestEnvelopeView, ResponseEnvelope,
};
use crate::common::error::Result;
use crate::server::conn_services::ConnectionServices;
use crate::server::session::{Session, SessionStore};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// Boxed future returned by [`RequestHandler::handle`].
pub type HandlerFuture<'a> =
    Pin<Box<dyn Future<Output = std::result::Result<Vec<u8>, String>> + Send + 'a>>;

/// Application-level request handler — receives the validated [`Session`] and
/// raw `req` bytes, returns either response bytes or an error string.
pub trait RequestHandler: Send + Sync {
    /// Handle a single validated request.
    ///
    /// Returning `Ok(bytes)` → wrap in [`ResponseEnvelope`].
    /// Returning `Err(reason)` → wrap in [`ErrorEnvelope`].
    fn handle<'a>(
        &'a self,
        session: &'a Session,
        req: &'a [u8],
        conn: &'a ConnectionServices,
    ) -> HandlerFuture<'a>;

    /// Rid-aware variant invoked by [`dispatch_request`] /
    /// [`dispatch_request_view`] with the optional envelope `rid` (R4b-1).
    ///
    /// Default: delegates to [`RequestHandler::handle`], so rid-agnostic
    /// handlers need no changes. Override only if the handler needs the
    /// client-side correlation id (e.g. activity logging).
    fn handle_with_request_id<'a>(
        &'a self,
        session: &'a Session,
        req: &'a [u8],
        conn: &'a ConnectionServices,
        _request_id: Option<u32>,
    ) -> HandlerFuture<'a> {
        self.handle(session, req, conn)
    }
}

/// RV-CONNECT-2A423D73: handler `Err(String)` diagnostics (e.g. response
/// encode errors that may embed payload fragments) never reach the wire
/// verbatim — they collapse to the fixed `application_error` code here.
/// Structured `DbResponse::Error` paths bypass this (already wire-safe).
const APPLICATION_ERROR_CODE: &str = "application_error";

/// Safe application-error boundary: maps an arbitrary handler failure
/// reason to a generic, disclosure-safe [`ErrorEnvelope`].
fn application_error_boundary(request_id: Option<u32>, reason: String) -> DispatchOutcome {
    let _ = reason; // dropped on purpose — never forwarded
    DispatchOutcome::Error(ErrorEnvelope::new(request_id, APPLICATION_ERROR_CODE))
}

/// Lookup hook: returns `tickets_invalid_before_ns` for a given user_id.
/// Caller wires this to whatever store backs `__system__/users`.
pub type TicketsInvalidBeforeLookup = dyn Fn(&[u8; 16]) -> u64 + Send + Sync;

/// Outcome of [`dispatch_request`].
#[derive(Debug)]
pub enum DispatchOutcome {
    /// Wire-ready response.
    Response(ResponseEnvelope),
    /// Wire-ready error envelope. Caller decides whether to also drop
    /// the underlying transport (e.g. for `session_invalidated`).
    Error(ErrorEnvelope),
}

/// Per-request entry point.
///
/// 1. Parse `session_id` from the envelope.
/// 2. Look up the session in `store`. Missing → `session_expired`.
/// 3. Run spec §7.5 validity check via `lookup_tickets_invalid_before_ns`.
///    Failure → remove session from store + return `session_invalidated`.
/// 4. Touch `last_activity_ns` (done by `SessionStore::lookup`).
/// 5. Dispatch `req` bytes to `handler` (async — directly awaited).
pub async fn dispatch_request<H: RequestHandler + ?Sized, F: Fn(&[u8; 16]) -> u64>(
    envelope: &RequestEnvelope,
    store: &SessionStore,
    lookup_tickets_invalid_before_ns: F,
    handler: &H,
    conn: &ConnectionServices,
) -> Result<DispatchOutcome> {
    let sid = envelope.session_id_array()?;
    let now_ns = crate::common::time::UnixNanos::try_now()?.as_u64();

    let session: Arc<Session> = match store.lookup_at(&sid, now_ns) {
        Some(s) => s,
        None => {
            return Ok(DispatchOutcome::Error(ErrorEnvelope::new(
                envelope.request_id,
                "session_expired",
            )));
        }
    };

    // Per-spec §7.5 [NORMATIVE]: kill stale sessions on next request.
    let user_invalid_before = lookup_tickets_invalid_before_ns(&session.user_id);
    if !session.is_valid_for_user(user_invalid_before) {
        // Remove from store immediately so concurrent requests can't reuse.
        store.remove(&sid);
        return Ok(DispatchOutcome::Error(ErrorEnvelope::new(
            envelope.request_id,
            "session_invalidated",
        )));
    }

    // Post-auth per-session request-rate gate — same choke point the
    // view path enforces; owned dispatch must not bypass it.
    if session.check_post_auth_rate_limit(now_ns).is_some() {
        store.record_post_auth_rejection();
        return Ok(DispatchOutcome::Error(ErrorEnvelope::new(
            envelope.request_id,
            "rate_limited",
        )));
    }

    // Application-level dispatch — async, no blocking bridge needed.
    // The optional envelope rid is forwarded to the handler (R4b-1);
    // rid-agnostic handlers via the default `handle` delegation.
    match handler
        .handle_with_request_id(&session, &envelope.req, conn, envelope.request_id)
        .await
    {
        Ok(res_bytes) => Ok(DispatchOutcome::Response(ResponseEnvelope::ok(
            envelope.request_id,
            res_bytes,
        ))),
        Err(err) => Ok(application_error_boundary(envelope.request_id, err)),
    }
}

/// **Optim #4** zero-copy variant of [`dispatch_request`].
///
/// Takes a [`RequestEnvelopeView`] borrowed directly from a wire buffer —
/// no `Vec<u8>` allocation for `session_id` or `req`. Functionally
/// identical: same §7.5 validity check, same handler dispatch, same
/// outcome shape.
///
/// Use this on transport hot paths where you already hold the raw msgpack
/// bytes; pair with [`shamir-transport-tcp::framing::read_frame_into`] to
/// keep the entire request path allocation-free.
pub async fn dispatch_request_view<H: RequestHandler + ?Sized, F: Fn(&[u8; 16]) -> u64>(
    view: &RequestEnvelopeView<'_>,
    store: &SessionStore,
    lookup_tickets_invalid_before_ns: F,
    handler: &H,
    conn: &ConnectionServices,
) -> Result<DispatchOutcome> {
    let sid = view.session_id_array()?;
    let now_ns = crate::common::time::UnixNanos::try_now()?.as_u64();

    let session: Arc<Session> = match store.lookup_at(sid, now_ns) {
        Some(s) => s,
        None => {
            return Ok(DispatchOutcome::Error(ErrorEnvelope::new(
                view.request_id,
                "session_expired",
            )));
        }
    };

    let user_invalid_before = lookup_tickets_invalid_before_ns(&session.user_id);
    if !session.is_valid_for_user(user_invalid_before) {
        store.remove(sid);
        return Ok(DispatchOutcome::Error(ErrorEnvelope::new(
            view.request_id,
            "session_invalidated",
        )));
    }

    // Post-auth per-session request-rate gate (task #608). Single choke
    // point covering every transport that routes through this function.
    if session.check_post_auth_rate_limit(now_ns).is_some() {
        store.record_post_auth_rejection();
        return Ok(DispatchOutcome::Error(ErrorEnvelope::new(
            view.request_id,
            "rate_limited",
        )));
    }

    // Application-level dispatch — async, no blocking bridge needed.
    // Same rid forwarding as [`dispatch_request`] (R4b-1).
    match handler
        .handle_with_request_id(&session, view.req, conn, view.request_id)
        .await
    {
        Ok(res_bytes) => Ok(DispatchOutcome::Response(ResponseEnvelope::ok(
            view.request_id,
            res_bytes,
        ))),
        Err(err) => Ok(application_error_boundary(view.request_id, err)),
    }
}

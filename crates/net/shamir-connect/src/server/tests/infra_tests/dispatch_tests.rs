//! Tests for the owned `dispatch_request` path (server/infra/dispatch.rs).

use crate::common::envelope::{RequestEnvelope, RequestEnvelopeView};
use crate::common::time::UnixNanos;
use crate::common::types::{BindingMode, TransportKind};
use crate::server::conn_services::ConnectionServices;
use crate::server::dispatch::dispatch_request_view;
use crate::server::dispatch::HandlerFuture;
use crate::server::session::{Session, SessionPermissions, SessionStore};
use crate::server::{dispatch_request, DispatchOutcome, RequestHandler};

struct Echo;
impl RequestHandler for Echo {
    fn handle<'a>(
        &'a self,
        _session: &'a Session,
        req: &'a [u8],
        _conn: &'a ConnectionServices,
    ) -> HandlerFuture<'a> {
        let out = req.to_vec();
        Box::pin(async move { Ok(out) })
    }
}

/// Handler that fails with an arbitrary diagnostic string.
struct Failing;
impl RequestHandler for Failing {
    fn handle<'a>(
        &'a self,
        _session: &'a Session,
        _req: &'a [u8],
        _conn: &'a ConnectionServices,
    ) -> HandlerFuture<'a> {
        Box::pin(async move { Err("encode_error: leaked secret path /tmp/x [1,2,3]".to_string()) })
    }
}

fn make_session(user_id: [u8; 16], created_at_ns: u64) -> Session {
    Session::new(
        user_id,
        "alice".into(),
        SessionPermissions::from_roles(vec!["read_write".into()]),
        TransportKind::Tcp,
        BindingMode::TlsExporter,
        [0u8; 32],
        created_at_ns,
    )
}

/// RV-CONNECT-8AD46452: the owned dispatch path must enforce the same
/// post-auth per-session rate limit as the borrowed-view path. Pre-fix,
/// a depleted bucket still reached the handler; post-fix, the request is
/// rejected with `rate_limited` before the handler runs.
#[tokio::test]
async fn rv_dispatch_8ad46452_owned_path_enforces_post_auth_rate_limit() {
    let store = SessionStore::new();
    let sid = [0x77u8; 32];
    let session = make_session([1u8; 16], UnixNanos::now().as_u64());
    store.insert(sid, session);

    // Drain the bucket with timestamps pinned at `u64::MAX`: the first
    // `check` fetch_maxes the refill watermark to MAX (elapsed saturates,
    // refill clamped to capacity); every later check — including
    // dispatch's real-clock call, since `real_now.saturating_sub(MAX) == 0`
    // — computes elapsed 0 and refills nothing, so no wall-clock advance
    // can resurrect a token.
    let live = store.lookup(&sid).expect("session inserted");
    let now_ns = u64::MAX;
    for _ in 0..shamir_tunables::instance_defaults::POST_AUTH_RATE_LIMIT_PER_SEC {
        assert!(live.check_post_auth_rate_limit(now_ns).is_none());
    }
    assert!(live.check_post_auth_rate_limit(now_ns).is_some());
    assert_eq!(store.post_auth_rejected_total(), 0);

    // Owned dispatch must gate on the same depleted bucket.
    let env = RequestEnvelope::new(sid, Some(9), b"hello".to_vec());
    let conn = ConnectionServices::without_push(0);
    let deadline = std::time::Duration::from_secs(120);
    let out = tokio::time::timeout(
        deadline,
        Box::pin(dispatch_request(&env, &store, |_| 0, &Echo, &conn)),
    )
    .await
    .expect("owned dispatch timed out")
    .unwrap();

    match out {
        DispatchOutcome::Error(e) => {
            assert_eq!(e.error, "rate_limited");
            assert_eq!(e.request_id, Some(9));
        }
        DispatchOutcome::Response(_) => {
            panic!("owned dispatch must enforce the post-auth rate limit")
        }
    }
    assert_eq!(store.post_auth_rejected_total(), 1);

    let wire = env.to_msgpack().unwrap();
    let view = RequestEnvelopeView::from_msgpack(&wire).unwrap();
    let out = tokio::time::timeout(
        deadline,
        Box::pin(dispatch_request_view(&view, &store, |_| 0, &Echo, &conn)),
    )
    .await
    .expect("view dispatch timed out")
    .unwrap();
    assert!(matches!(out, DispatchOutcome::Error(e) if e.error == "rate_limited"));
    assert_eq!(store.post_auth_rejected_total(), 2);
}

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Sentinel encoding for a probe cell: `0` = never recorded, `1` = `None`,
/// `rid + 2` = `Some(rid)` — distinguishes `None` from `Some(0)` and from
/// an unrecorded cell.
fn enc(rid: Option<u32>) -> u64 {
    rid.map_or(1, |id| u64::from(id) + 2)
}

/// Handler overriding `handle_with_request_id`; records the rid it receives
/// (once synchronously at `handle_with_request_id` time, once inside its
/// future) into `AtomicU64` cells — no locks, `Send + Sync`.
struct RidProbe {
    sync_seen: Arc<AtomicU64>,
    async_seen: Arc<AtomicU64>,
}

impl RidProbe {
    fn new() -> (Self, Arc<AtomicU64>, Arc<AtomicU64>) {
        let sync_seen = Arc::new(AtomicU64::new(0));
        let async_seen = Arc::new(AtomicU64::new(0));
        (
            Self {
                sync_seen: Arc::clone(&sync_seen),
                async_seen: Arc::clone(&async_seen),
            },
            sync_seen,
            async_seen,
        )
    }
}

impl RequestHandler for RidProbe {
    fn handle<'a>(
        &'a self,
        _session: &'a Session,
        _req: &'a [u8],
        _conn: &'a ConnectionServices,
    ) -> HandlerFuture<'a> {
        unreachable!("rid-aware dispatch must call handle_with_request_id")
    }

    fn handle_with_request_id<'a>(
        &'a self,
        _session: &'a Session,
        _req: &'a [u8],
        _conn: &'a ConnectionServices,
        request_id: Option<u32>,
    ) -> HandlerFuture<'a> {
        let sync_seen = Arc::clone(&self.sync_seen);
        let async_seen = Arc::clone(&self.async_seen);
        let encoded = enc(request_id);
        sync_seen.store(encoded, Ordering::SeqCst);
        Box::pin(async move {
            async_seen.store(encoded, Ordering::SeqCst);
            Ok(Vec::new())
        })
    }
}

/// R4b-1: both dispatch paths forward the optional envelope rid to the
/// rid-aware override — `Some(rid)` when present, `None` when absent — on
/// the owned and borrowed-view paths.
#[tokio::test]
async fn r4b_dispatch_forwards_envelope_rid_to_override() {
    let store = SessionStore::new();
    let sid = [0x79u8; 32];
    store.insert(sid, make_session([3u8; 16], UnixNanos::now().as_u64()));

    // Owned path, Some(0): the sentinel must distinguish Some(0) from both
    // None and a never-recorded cell.
    let env_zero = RequestEnvelope::new(sid, Some(0), b"x".to_vec());
    let (probe, sync_seen, async_seen) = RidProbe::new();
    let out = dispatch_request(
        &env_zero,
        &store,
        |_| 0,
        &probe,
        &ConnectionServices::without_push(0),
    )
    .await
    .unwrap();
    assert!(matches!(out, DispatchOutcome::Response(_)));
    assert_eq!(sync_seen.load(Ordering::SeqCst), enc(Some(0)));
    assert_eq!(async_seen.load(Ordering::SeqCst), enc(Some(0)));

    // Owned path, Some(rid).
    let env = RequestEnvelope::new(sid, Some(41), b"x".to_vec());
    let (probe, sync_seen, async_seen) = RidProbe::new();
    let out = dispatch_request(
        &env,
        &store,
        |_| 0,
        &probe,
        &ConnectionServices::without_push(0),
    )
    .await
    .unwrap();
    assert!(matches!(out, DispatchOutcome::Response(_)));
    assert_eq!(sync_seen.load(Ordering::SeqCst), enc(Some(41)));
    assert_eq!(async_seen.load(Ordering::SeqCst), enc(Some(41)));

    // Owned path, None.
    let env_none = RequestEnvelope::new(sid, None, b"x".to_vec());
    let (probe, sync_seen, async_seen) = RidProbe::new();
    let out = dispatch_request(
        &env_none,
        &store,
        |_| 0,
        &probe,
        &ConnectionServices::without_push(0),
    )
    .await
    .unwrap();
    assert!(matches!(out, DispatchOutcome::Response(_)));
    assert_eq!(sync_seen.load(Ordering::SeqCst), enc(None));
    assert_eq!(async_seen.load(Ordering::SeqCst), enc(None));

    // Borrowed-view path, Some(rid).
    let (probe, sync_seen, async_seen) = RidProbe::new();
    let wire = env.to_msgpack().unwrap();
    let view = RequestEnvelopeView::from_msgpack(&wire).unwrap();
    let out = dispatch_request_view(
        &view,
        &store,
        |_| 0,
        &probe,
        &ConnectionServices::without_push(0),
    )
    .await
    .unwrap();
    assert!(matches!(out, DispatchOutcome::Response(_)));
    assert_eq!(sync_seen.load(Ordering::SeqCst), enc(Some(41)));
    assert_eq!(async_seen.load(Ordering::SeqCst), enc(Some(41)));

    // Borrowed-view path, None.
    let (probe, sync_seen, async_seen) = RidProbe::new();
    let wire_none = env_none.to_msgpack().unwrap();
    let view_none = RequestEnvelopeView::from_msgpack(&wire_none).unwrap();
    let out = dispatch_request_view(
        &view_none,
        &store,
        |_| 0,
        &probe,
        &ConnectionServices::without_push(0),
    )
    .await
    .unwrap();
    assert!(matches!(out, DispatchOutcome::Response(_)));
    assert_eq!(sync_seen.load(Ordering::SeqCst), enc(None));
    assert_eq!(async_seen.load(Ordering::SeqCst), enc(None));
}

/// R4b-1: the default `handle_with_request_id` delegates to `handle` — an
/// rid-agnostic handler receives dispatches (and produces responses) on both
/// dispatch paths without any override.
#[tokio::test]
async fn r4b_default_handle_with_request_id_delegates_to_handle() {
    let store = SessionStore::new();
    let sid = [0x7bu8; 32];
    store.insert(sid, make_session([5u8; 16], UnixNanos::now().as_u64()));

    let env = RequestEnvelope::new(sid, Some(42), b"payload".to_vec());
    let out = dispatch_request(
        &env,
        &store,
        |_| 0,
        &Echo,
        &ConnectionServices::without_push(0),
    )
    .await
    .unwrap();
    match out {
        DispatchOutcome::Response(r) => {
            assert_eq!(r.request_id, Some(42));
            assert_eq!(r.res, b"payload");
        }
        DispatchOutcome::Error(_) => panic!("default delegation must reach Echo"),
    }
    assert_eq!(store.post_auth_rejected_total(), 0);

    let wire = env.to_msgpack().unwrap();
    let view = RequestEnvelopeView::from_msgpack(&wire).unwrap();
    let out = dispatch_request_view(
        &view,
        &store,
        |_| 0,
        &Echo,
        &ConnectionServices::without_push(0),
    )
    .await
    .unwrap();
    match out {
        DispatchOutcome::Response(r) => {
            assert_eq!(r.request_id, Some(42));
            assert_eq!(r.res, b"payload");
        }
        DispatchOutcome::Error(_) => panic!("default delegation must reach Echo"),
    }
    assert_eq!(store.post_auth_rejected_total(), 0);
}

/// R4b-1: a nested dispatch inside a rid-aware handler receives its own rid
/// (and `None` when the nested envelope carries no rid) — no stale or
/// inherited rid from the outer request.
#[tokio::test]
async fn r4b_nested_dispatch_rids_are_per_envelope() {
    struct Nested {
        store: Arc<SessionStore>,
        inner_sync: Arc<AtomicU64>,
        inner_async: Arc<AtomicU64>,
    }
    impl RequestHandler for Nested {
        fn handle<'a>(
            &'a self,
            _session: &'a Session,
            _req: &'a [u8],
            _conn: &'a ConnectionServices,
        ) -> HandlerFuture<'a> {
            unreachable!("rid-aware dispatch must call handle_with_request_id")
        }

        fn handle_with_request_id<'a>(
            &'a self,
            session: &'a Session,
            _req: &'a [u8],
            conn: &'a ConnectionServices,
            outer_rid: Option<u32>,
        ) -> HandlerFuture<'a> {
            assert_eq!(outer_rid, Some(7), "outer dispatch forwards its own rid");
            let store = Arc::clone(&self.store);
            let inner_sync = Arc::clone(&self.inner_sync);
            let inner_async = Arc::clone(&self.inner_async);
            Box::pin(async move {
                for inner_rid in [Some(8), None] {
                    let env = RequestEnvelope::new(session.session_id, inner_rid, b"y".to_vec());
                    let (probe, sync_seen, async_seen) = RidProbe::new();
                    let out = dispatch_request(&env, &store, |_| 0, &probe, conn)
                        .await
                        .unwrap();
                    assert!(matches!(out, DispatchOutcome::Response(_)));
                    let expected = enc(inner_rid);
                    assert_eq!(sync_seen.load(Ordering::SeqCst), expected);
                    assert_eq!(async_seen.load(Ordering::SeqCst), expected);
                    // Accumulate BOTH encodings (not just the last) so the
                    // outer assertions below prove each nested dispatch saw
                    // its own rid, in order, instead of a single value.
                    inner_sync.fetch_add(expected, Ordering::SeqCst);
                    inner_async.fetch_add(expected, Ordering::SeqCst);
                }
                Ok(Vec::new())
            })
        }
    }

    let store = Arc::new(SessionStore::new());
    let sid = [0x7au8; 32];
    store.insert(sid, make_session([4u8; 16], UnixNanos::now().as_u64()));

    let inner_sync = Arc::new(AtomicU64::new(0));
    let inner_async = Arc::new(AtomicU64::new(0));
    let outer_probe = Nested {
        store: Arc::clone(&store),
        inner_sync: Arc::clone(&inner_sync),
        inner_async: Arc::clone(&inner_async),
    };
    let env = RequestEnvelope::new(sid, Some(7), b"x".to_vec());
    let out = dispatch_request(
        &env,
        &store,
        |_| 0,
        &outer_probe,
        &ConnectionServices::without_push(0),
    )
    .await
    .unwrap();
    assert!(matches!(out, DispatchOutcome::Response(_)));
    // The nested dispatches accumulated enc(Some(8)) then enc(None): the
    // sum proves both orders ran, in sequence, with per-envelope rids.
    let nested_sum = enc(Some(8)) + enc(None);
    assert_eq!(inner_sync.load(Ordering::SeqCst), nested_sum);
    assert_eq!(inner_async.load(Ordering::SeqCst), nested_sum);
}

/// RV-CONNECT-2A423D73: handler `Err(String)` diagnostics must never reach
/// the wire verbatim — both dispatch paths collapse them to the fixed
/// `application_error` code. Pre-fix, `err.error` was the raw handler
/// string (including any embedded payload/path fragments).
#[tokio::test]
async fn rv_connect_2a423d73_handler_err_collapses_to_application_error() {
    let store = SessionStore::new();
    let sid = [0x78u8; 32];
    store.insert(sid, make_session([2u8; 16], UnixNanos::now().as_u64()));
    let env = RequestEnvelope::new(sid, Some(3), b"q".to_vec());

    let out = dispatch_request(
        &env,
        &store,
        |_| 0,
        &Failing,
        &ConnectionServices::without_push(0),
    )
    .await
    .unwrap();

    match out {
        DispatchOutcome::Error(e) => {
            assert_eq!(e.error, "application_error");
            assert_eq!(e.request_id, Some(3));
        }
        DispatchOutcome::Response(_) => {
            panic!("failing handler must produce an error envelope")
        }
    }
    assert_eq!(store.post_auth_rejected_total(), 0);

    // Same boundary on the borrowed-view path.
    let wire = env.to_msgpack().unwrap();
    let view = RequestEnvelopeView::from_msgpack(&wire).unwrap();
    let out = dispatch_request_view(
        &view,
        &store,
        |_| 0,
        &Failing,
        &ConnectionServices::without_push(0),
    )
    .await
    .unwrap();
    match out {
        DispatchOutcome::Error(e) => assert_eq!(e.error, "application_error"),
        DispatchOutcome::Response(_) => panic!("view path must collapse handler errors too"),
    }
    assert_eq!(store.post_auth_rejected_total(), 0);
}

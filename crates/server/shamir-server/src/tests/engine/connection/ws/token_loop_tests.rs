use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use metrics_exporter_prometheus::PrometheusBuilder;
use serde::Deserialize;
use shamir_connect::common::envelope::{RequestEnvelope, ResponseEnvelope};
use shamir_connect::common::time::UnixNanos;
use shamir_connect::server::audit_chain::{
    AuditAppender, AuditChain, AuditChainWriter, AuditEntry,
};
use shamir_connect::server::session::Session;
use shamir_connect::server::token::{OwnerFloors, TokenChannels, TokenId, TokenMode};
use shamir_connect::server::token_session::TokenSocketCaps;
use shamir_connect::server::TokenSessionStore;
use shamir_transport_http::{
    AuthHeader, Endpoint as HttpEndpoint, GateDecision, PeerInfo, TokenGate,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use zeroize::Zeroizing;

use crate::connection::token_ws::{
    encode_prereserved, linger_after_close, run_token_halves, ErrorReply, TokenLoopContext,
};
use crate::engine::db_handler::ShamirDbHandler;
use crate::framer::{FrameReader, FrameWriter, FramerError};
use crate::http_rest::{RestGate, RestGateRails};
use crate::http_ws::TokenWsCancelRegistry;
use crate::security::tokens::{NewToken, TokenService, TokenServiceOptions};
use shamir_db::ShamirDb;
use shamir_query_builder::{batch::Batch, ddl};
use shamir_query_types::wire::{DbRequest, DbResponse, CURRENT_QUERY_LANG_VERSION};
use shamir_server_security::config::TokensConfig;

struct Owners;
impl OwnerFloors for Owners {
    fn floor_if_present(&self, _: &[u8; 16]) -> Option<u64> {
        Some(0)
    }
}

struct Audit;
impl AuditAppender for Audit {
    fn append_entry(&self, _: &AuditEntry) -> Result<(), std::io::Error> {
        Ok(())
    }
    fn checkpoint(&self, _: u64, _: &[u8; 32]) -> Result<(), std::io::Error> {
        Ok(())
    }
}

struct Reader {
    rx: mpsc::Receiver<Vec<u8>>,
}
impl FrameReader for Reader {
    async fn read_frame_into(&mut self, _: usize, target: &mut Vec<u8>) -> Result<(), FramerError> {
        let bytes = self
            .rx
            .recv()
            .await
            .ok_or_else(|| FramerError::Io("reader channel closed".into()))?;
        target.clear();
        target.extend_from_slice(&bytes);
        Ok(())
    }
}

struct Writer {
    tx: mpsc::Sender<Vec<u8>>,
}
impl FrameWriter for Writer {
    async fn write_frame_into(&mut self, _: &[u8], _: &mut Vec<u8>) -> Result<(), FramerError> {
        Ok(())
    }
    async fn write_frame_prereserved(&mut self, bytes: Vec<u8>) -> Result<(), FramerError> {
        let len = u32::from_be_bytes(bytes[..4].try_into().expect("length prefix")) as usize;
        self.tx
            .send(bytes[4..4 + len].to_vec())
            .await
            .map_err(|_| FramerError::Io("writer channel closed".into()))
    }
    async fn shutdown(&mut self) {}
    async fn close_with(&mut self, _: u16, _: &str) {}
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RateReply {
    rid: u32,
    error: String,
}

fn wire_frame(sid: &[u8; 32], rid: u32, request: &shamir_query_types::wire::DbRequest) -> Vec<u8> {
    let request = rmp_serde::to_vec_named(request).expect("encode request");
    RequestEnvelope::new(*sid, Some(rid), request)
        .to_msgpack()
        .expect("encode envelope")
}

fn peer() -> PeerInfo {
    PeerInfo {
        addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 7340),
        tls: true,
        forwarded_for: Vec::new(),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn real_token_loop_handles_requests_limits_rate_and_cleans_up() {
    let now = UnixNanos::try_now().expect("system clock").as_u64();
    let recorder = PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let _recorder_guard = metrics::set_default_local_recorder(&recorder);
    let temp = tempfile::TempDir::new().expect("tempdir");
    let service = Arc::new(
        TokenService::open(TokenServiceOptions {
            data_dir: temp.path().to_path_buf(),
            token_key: Zeroizing::new([3; 32]),
            owners: Arc::new(Owners),
            sessions: Arc::new(TokenSessionStore::new()),
            audit: Arc::new(AuditChainWriter::new(
                AuditChain::new([2; 32]),
                Arc::new(Audit),
            )),
            config: TokensConfig::default(),
            read_only_node: false,
            clock: Arc::new(move || Some(now)),
        })
        .await
        .expect("token service"),
    );
    let created = service
        .create(NewToken {
            owner_user_id: [7; 16],
            owner_roles: vec!["reader".into()],
            label: "ws-loop-test".into(),
            ttl_secs: Some(60),
            mode: TokenMode::ReadOnly,
            channels: TokenChannels::Ws,
            databases: None,
        })
        .await
        .expect("create token");
    let gate = RestGate::new(
        Arc::clone(&service),
        Arc::new(move || Some(now)),
        RestGateRails {
            plaintext_remote: false,
            allow_plaintext_write: false,
        },
    );
    let decision = gate
        .authenticate(
            AuthHeader::Single(format!("Bearer {}", created.token.as_str()).as_bytes()),
            HttpEndpoint::Ws,
            peer(),
        )
        .await;
    let grant = match decision {
        GateDecision::Allow(grant) => Arc::new(grant),
        _ => panic!("WS auth failed"),
    };
    let sid = [8; 32];
    let session = Session::for_token(&grant, "alice".into(), now, sid);
    let session = service
        .sessions()
        .admit(
            sid,
            session,
            TokenSocketCaps {
                per_token: 1,
                per_owner: 1,
            },
        )
        .expect("admit session");
    let registry = Arc::new(TokenWsCancelRegistry::default());
    let registration = registry.register(TokenId(grant.state.record().id));
    let token_id = TokenId(grant.state.record().id);
    let handler = Arc::new(ShamirDbHandler::new(Arc::new(
        ShamirDb::init_memory().await.expect("db"),
    )));
    let cancel = registration.token();
    let shutdown = CancellationToken::new();
    let (request_tx, request_rx) = mpsc::channel(4);
    let (response_tx, mut response_rx) = mpsc::channel(4);
    let context = TokenLoopContext {
        service: Arc::clone(&service),
        handler,
        grant: Arc::clone(&grant),
        session: Arc::clone(&session),
        expires_at_ns: grant.state.record().expires_at_ns,
        cancel,
        shutdown: shutdown.clone(),
        max_in_flight: 1,
        ping_interval: Duration::from_secs(3600),
        pong_timeout: Duration::from_secs(3600),
        registration,
    };
    let task = tokio::spawn(run_token_halves(
        context,
        Reader { rx: request_rx },
        Writer { tx: response_tx },
        CancellationToken::new(),
        CancellationToken::new(),
    ));
    tokio::time::timeout(
        Duration::from_secs(5),
        request_tx.send(wire_frame(&sid, 1, &DbRequest::Ping)),
    )
    .await
    .expect("send ping bounded")
    .expect("send ping");
    let bytes = tokio::time::timeout(Duration::from_secs(5), response_rx.recv())
        .await
        .expect("ping reply bounded")
        .expect("ping reply");
    let reply = ResponseEnvelope::from_msgpack(&bytes).expect("decode ping reply");
    assert_eq!(reply.request_id, Some(1));
    assert!(matches!(
        rmp_serde::from_slice::<shamir_query_types::wire::DbResponse>(&reply.res)
            .expect("decode pong"),
        shamir_query_types::wire::DbResponse::Pong
    ));

    let mut batch = Batch::new();
    batch.create_db("blocked", ddl::create_db("blocked"));
    let write = DbRequest::Execute {
        query_version: CURRENT_QUERY_LANG_VERSION,
        db: "default".into(),
        batch: batch.build(),
    };
    tokio::time::timeout(
        Duration::from_secs(5),
        request_tx.send(wire_frame(&sid, 2, &write)),
    )
    .await
    .expect("send forbidden request bounded")
    .expect("send forbidden request");
    let bytes = tokio::time::timeout(Duration::from_secs(5), response_rx.recv())
        .await
        .expect("denial bounded")
        .expect("denial reply");
    let denied = ResponseEnvelope::from_msgpack(&bytes).expect("decode denial response");
    assert_eq!(denied.request_id, Some(2));
    assert!(
        matches!(rmp_serde::from_slice::<DbResponse>(&denied.res).expect("decode denial body"), DbResponse::Error { code, .. } if code == "token_read_only")
    );

    assert_eq!(service.counters().post_auth_rejected_total(), 0);

    // Advance only the bucket watermark so the test does not rely on wall-clock time to drain it.
    let watermark = u64::MAX;
    let mut rejected = false;
    for _ in 0..=shamir_tunables::instance_defaults::POST_AUTH_RATE_LIMIT_PER_SEC {
        if session.check_post_auth_rate_limit(watermark).is_some() {
            rejected = true;
            break;
        }
    }
    assert!(
        rejected,
        "real post-auth bucket must reject after exhaustion"
    );
    tokio::time::timeout(
        Duration::from_secs(5),
        request_tx.send(wire_frame(&sid, 3, &DbRequest::Ping)),
    )
    .await
    .expect("send limited request bounded")
    .expect("send limited request");
    let bytes = tokio::time::timeout(Duration::from_secs(5), response_rx.recv())
        .await
        .expect("rate reply bounded")
        .expect("rate reply");
    let raw: std::collections::BTreeMap<String, serde::de::IgnoredAny> =
        rmp_serde::from_slice(&bytes).expect("decode rate reply map");
    let keys = raw.keys().map(String::as_str).collect::<Vec<_>>();
    assert_eq!(raw.len(), 2);
    assert_eq!(keys, ["error", "rid"]);
    let limited: RateReply = rmp_serde::from_slice(&bytes).expect("decode rate reply");
    assert_eq!(limited.rid, 3);
    assert_eq!(limited.error, "rate_limited");
    assert_eq!(service.counters().post_auth_rejected_total(), 1);

    let rendered = handle.render();
    assert!(
        rendered
            .lines()
            .any(|line| line.starts_with("shamir_token_auth_total{")
                && line.contains("channel=\"ws\"")
                && line.contains("result=\"success\"")
                && line.ends_with(" 1")),
        "auth success metric: {rendered}"
    );
    assert!(
        rendered.contains("shamir_token_request_denied_total{endpoint=\"ws\"} 1"),
        "denied metric: {rendered}"
    );
    assert!(
        rendered.contains("shamir_token_request_duration_seconds_count{endpoint=\"ws\"} 2"),
        "duration metric: {rendered}"
    );
    assert!(
        rendered.contains("shamir_token_active_ws 1"),
        "active metric: {rendered}"
    );
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .expect("loop join bounded")
        .expect("loop task");
    assert!(
        handle.render().contains("shamir_token_active_ws 0"),
        "active metric after cleanup: {}",
        handle.render()
    );
    assert!(!registry.contains_entry(&token_id));
    assert!(service.sessions().get(&sid).is_none());
    gate.shutdown().await;
}

/// Reader that counts the frames it delivered before the channel closed.
struct CountingReader {
    rx: mpsc::Receiver<Vec<u8>>,
    delivered: Arc<std::sync::atomic::AtomicUsize>,
}
impl FrameReader for CountingReader {
    async fn read_frame_into(&mut self, _: usize, target: &mut Vec<u8>) -> Result<(), FramerError> {
        let bytes = self
            .rx
            .recv()
            .await
            .ok_or_else(|| FramerError::Io("reader channel closed".into()))?;
        target.clear();
        target.extend_from_slice(&bytes);
        self.delivered
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn linger_drains_pending_frames_and_returns_when_the_peer_hangs_up() {
    let (tx, rx) = mpsc::channel(8);
    let delivered = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut reader = CountingReader {
        rx,
        delivered: Arc::clone(&delivered),
    };
    for ping in 0..3u8 {
        tx.send(vec![ping]).await.expect("queue a pending frame");
    }
    drop(tx);

    let started = tokio::time::Instant::now();
    linger_after_close(&mut reader).await;

    assert_eq!(
        delivered.load(std::sync::atomic::Ordering::SeqCst),
        3,
        "every frame the peer had in flight must be read before the socket is released"
    );
    assert!(
        started.elapsed() < Duration::from_millis(1),
        "a peer that hung up must not be waited for"
    );
}

#[tokio::test(start_paused = true)]
async fn linger_is_bounded_when_the_peer_stays_silent() {
    let (_tx, rx) = mpsc::channel::<Vec<u8>>(1);
    let mut reader = Reader { rx };

    let started = tokio::time::Instant::now();
    linger_after_close(&mut reader).await;

    let waited = started.elapsed();
    assert!(
        waited >= Duration::from_secs(1) && waited <= Duration::from_secs(2),
        "a silent peer is waited for about the linger bound, got {waited:?}"
    );
}

#[test]
fn loop_wire_error_uses_optional_rid_and_length_prefixed_shared_buffer() {
    let bytes = encode_prereserved(&ErrorReply {
        rid: Some(4),
        error: "invalid_request",
    })
    .expect("encode");
    let size = u32::from_be_bytes(bytes[..4].try_into().expect("prefix")) as usize;
    assert_eq!(size, bytes.len() - 4);
    let fields: std::collections::BTreeMap<String, serde::de::IgnoredAny> =
        rmp_serde::from_slice(&bytes[4..]).expect("msgpack");
    assert_eq!(fields.len(), 2);
    assert!(fields.contains_key("error"));
    assert!(fields.contains_key("rid"));
}

//! Scrape live load metrics from the real launcher and its production sources.

use std::future::{poll_fn, Future};
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::task::Poll;
use std::time::Duration;

use shamir_client::{Client, ConnectOptions};
use shamir_connect::common::envelope::{RequestEnvelope, RequestEnvelopeView};
use shamir_connect::common::time::UnixNanos;
use shamir_connect::common::types::{BindingMode, TransportKind};
use shamir_connect::server::conn_services::ConnectionServices;
use shamir_connect::server::dispatch::dispatch_request_view;
use shamir_connect::server::session::{Session, SessionPermissions};
use shamir_connect::server::DispatchOutcome;
use shamir_test_diag::FlakeGuard;
use shamir_tunables::instance_defaults::POST_AUTH_RATE_LIMIT_PER_SEC;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
use zeroize::Zeroizing;

use crate::config::{
    Config, KdfConfig, ListenerConfig, ListenerKind, LoggingConfig, ObservabilityConfig,
    ProfileKind, TlsConfig,
};
use crate::server::{BootstrapMode, ServerHandle, ServerLauncher};

const CAP: usize = 16_384;
const CONNECTION_LIMIT: usize = 2;
const CALL_TIMEOUT: Duration = Duration::from_secs(120);
const USERNAME: &str = "metrics_fixture";
const PASSWORD: &[u8] = b"launcher metrics fixture password";
const ACTIVE: &str = "shamir_connections_active";

fn boot_config(data_dir: PathBuf) -> Config {
    let tls = TlsConfig {
        cert_path: data_dir.join("cert.pem"),
        key_path: data_dir.join("key.pem"),
    };
    let mut config = Config {
        data_dir,
        logging: LoggingConfig {
            level: "warn".into(),
            slow_query_threshold_ms: 0,
            file: None,
            flush_interval_ms: 2000,
        },
        durability: Default::default(),
        transactions: Default::default(),
        tokens: Default::default(),
        kdf_defaults: KdfConfig {
            memory_kb: 19_456,
            time: 2,
            parallelism: 1,
            argon2_version: 0x13,
        },
        argon2_concurrent_max: 64,
        listeners: vec![ListenerConfig {
            kind: ListenerKind::Tcp,
            addr: "127.0.0.1:0".into(),
            profile: Some(ProfileKind::Plain),
            http: Default::default(),
            path: None,
            kdf_override: None,
            browser_origin_allowlist: Vec::new(),
        }],
        tls,
        security: Default::default(),
        audit: Default::default(),
        observability: ObservabilityConfig {
            addr: "127.0.0.1:0".into(),
            allow_public_metrics: false,
        },
        replication: None,
        backup: None,
    };
    config.security.auth_init_rate_per_second = 64;
    config.security.connection.auth_init_timeout_ms = 120_000;
    config.security.connection.max_active_connections = CONNECTION_LIMIT;
    config.security.connection.max_active_connections_per_ip = 0;
    config.security.query_limits.max_result_size_bytes = CAP;
    config.security.query_limits.max_inflight_response_bytes = Some(CAP);
    config
}

async fn connect(addr: SocketAddr) -> Client {
    Box::pin(Client::connect_plain(ConnectOptions {
        addr,
        server_name: "localhost".into(),
        username: USERNAME.into(),
        password: Zeroizing::new(PASSWORD.to_vec()),
        accept_new_host: true,
        trusted_pin: None,
        connect_timeout: Some(CALL_TIMEOUT),
        handshake_timeout: Some(CALL_TIMEOUT),
        request_timeout: Some(CALL_TIMEOUT),
    }))
    .await
    .unwrap_or_else(|_| panic!("plain TCP authentication must succeed"))
}

async fn scrape(addr: SocketAddr) -> String {
    timeout(CALL_TIMEOUT, async {
        let mut stream = TcpStream::connect(addr).await.expect("connect to /metrics");
        stream
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .expect("write scrape request");
        let mut response = String::new();
        stream
            .read_to_string(&mut response)
            .await
            .expect("read metrics response");
        let (head, body) = response
            .split_once("\r\n\r\n")
            .expect("HTTP header boundary");
        assert!(head.starts_with("HTTP/1.1 200 "), "scrape must return 200");
        body.to_owned()
    })
    .await
    .expect("bounded metrics scrape")
}

fn sample_name(line: &str) -> &str {
    line.trim_start()
        .split(|c: char| c == '{' || c.is_ascii_whitespace())
        .next()
        .unwrap_or_default()
}

fn series(body: &str, name: &str, kind: &str) -> i64 {
    // Match the name before any label block, not just the unlabeled spelling.
    // Never print the raw scrape or label contents on failure.
    let mut samples = body.lines().filter(|line| sample_name(line) == name);
    let line = samples.next().unwrap_or_else(|| panic!("{name} missing"));
    assert!(samples.next().is_none(), "{name} has duplicate samples");
    let mut fields = line.split_ascii_whitespace();
    assert!(
        fields.next() == Some(name) && !line.contains(['{', '}']),
        "{name} must be unlabeled"
    );
    let value = fields
        .next()
        .and_then(|field| field.parse().ok())
        .unwrap_or_else(|| panic!("{name} has no integer sample"));
    assert!(fields.next().is_none(), "{name} has extra sample fields");

    let mut types = body.lines().filter_map(|line| {
        let mut fields = line.split_ascii_whitespace();
        (fields.next() == Some("#") && fields.next() == Some("TYPE") && fields.next() == Some(name))
            .then_some(fields)
    });
    let mut fields = types.next().unwrap_or_else(|| panic!("{name} has no TYPE"));
    assert!(
        fields.next() == Some(kind) && fields.next().is_none(),
        "{name} must have type {kind}"
    );
    assert!(types.next().is_none(), "{name} has duplicate TYPE lines");
    value
}

// Expected values in the same order as the seven named series below.
fn assert_load(body: &str, expected: [i64; 7]) {
    for ((name, kind), value) in [
        (ACTIVE, "gauge"),
        ("shamir_connections_limit", "gauge"),
        ("shamir_connections_rejected_total", "counter"),
        ("shamir_inflight_response_bytes_used", "gauge"),
        ("shamir_inflight_response_bytes_cap", "gauge"),
        ("shamir_response_budget_waiters", "gauge"),
        ("shamir_post_auth_rate_limit_rejected_total", "counter"),
    ]
    .into_iter()
    .zip(expected)
    {
        assert_eq!(series(body, name, kind), value, "{name}");
    }
    assert!(
        body.lines().map(sample_name).all(|name| {
            !name.starts_with("shamir_argon2_")
                && name != "shamir_sessions_active"
                && name != "shamir_admission_queue_depth"
        }),
        "load export must omit series without a real source"
    );
}

async fn refuse_extra_connection(addr: SocketAddr) {
    let mut socket = TcpStream::connect(addr).await.expect("TCP probe connects");
    // The cap refuses before reading auth_init. Do not send anything or
    // count a failed dial, a timeout or a protocol error as a refusal.
    match socket.read(&mut [0; 1]).await {
        Ok(0) => {}
        Err(error) if error.kind() == ErrorKind::ConnectionReset => {}
        _ => panic!("extra TCP connection was not closed by the server"),
    }
}

async fn reject_native_request(handle: &ServerHandle) {
    let seam = handle.activity_test_seam.as_ref().expect("load test seam");
    let mut session = Session::new(
        [0x62; 16],
        USERNAME.into(),
        SessionPermissions::from_roles(vec!["read_write".into()]),
        TransportKind::Tcp,
        BindingMode::None,
        [0; 32],
        UnixNanos::now().as_u64(),
    );
    let sid = [0x72; 32];
    session.session_id = sid;
    let live = seam.session_store.insert(sid, session);
    // Pin the refill watermark above the real dispatch clock. No traffic
    // burst or timing assumption is needed to keep the bucket empty.
    for _ in 0..POST_AUTH_RATE_LIMIT_PER_SEC {
        assert!(live.check_post_auth_rate_limit(u64::MAX).is_none());
    }
    assert!(live.check_post_auth_rate_limit(u64::MAX).is_some());
    let wire = RequestEnvelope::new(sid, Some(1), Vec::new())
        .to_msgpack()
        .expect("fixture envelope encodes");
    let view = RequestEnvelopeView::from_msgpack(&wire).expect("fixture envelope decodes");
    let conn = ConnectionServices::without_push(0);
    let outcome = Box::pin(dispatch_request_view(
        &view,
        &seam.session_store,
        |_| 0,
        seam.handler.as_ref(),
        &conn,
    ))
    .await
    .expect("native dispatch returns an outcome");
    assert!(matches!(outcome, DispatchOutcome::Error(e) if e.error == "rate_limited"));
    drop(seam.session_store.remove(&sid));
}

async fn shutdown(mut handle: ServerHandle) {
    drop(handle.activity_test_seam.take());
    let report = Box::pin(handle.shutdown()).await;
    assert!(report.is_clean(), "launcher shutdown must be clean");
}

async fn run(guard: &FlakeGuard) {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let temp = tempfile::tempdir().expect("boot tempdir");
    let handle = Box::pin(
        ServerLauncher {
            config: boot_config(temp.path().to_path_buf()),
            bootstrap: BootstrapMode::Password {
                username: USERNAME.into(),
                password: Zeroizing::new(PASSWORD.to_vec()),
            },
            node_mode: Default::default(),
        }
        .launch(),
    )
    .await
    .unwrap_or_else(|_| panic!("launcher boot"));
    guard.mark("launcher booted");

    let seam = handle.activity_test_seam.as_ref().expect("load test seam");
    let obs = seam.observability_bound_addr;
    let tcp = handle.bound_addrs[0].expect("plain TCP port bound");
    let cap = CAP as i64;
    let limit = CONNECTION_LIMIT as i64;
    assert_load(&scrape(obs).await, [0, limit, 0, 0, cap, 0, 0]);

    let first = connect(tcp).await;
    let second = connect(tcp).await;
    guard.mark("two plain TCP handshakes completed");
    assert_load(&scrape(obs).await, [limit, limit, 0, 0, cap, 0, 0]);
    for rejected in 1..=2 {
        timeout(CALL_TIMEOUT, refuse_extra_connection(tcp))
            .await
            .expect("bounded TCP refusal probe");
        assert_load(&scrape(obs).await, [limit, limit, rejected, 0, cap, 0, 0]);
    }
    guard.mark("global connection refusals observed");

    let reservation = seam.byte_budget.acquire(CAP).await;
    let mut waiter = Box::pin(seam.byte_budget.acquire(1));
    poll_fn(|cx| {
        assert!(
            waiter.as_mut().poll(cx).is_pending(),
            "budget waiter must park"
        );
        Poll::Ready(())
    })
    .await;
    assert_load(&scrape(obs).await, [limit, limit, 2, cap, cap, 1, 0]);
    reject_native_request(&handle).await;
    assert_load(&scrape(obs).await, [limit, limit, 2, cap, cap, 1, 1]);
    guard.mark("byte budget saturation and native rejection observed");

    drop(waiter);
    assert_load(&scrape(obs).await, [limit, limit, 2, cap, cap, 0, 1]);
    drop(reservation);
    assert_load(&scrape(obs).await, [limit, limit, 2, 0, cap, 0, 1]);
    drop((first, second));
    timeout(CALL_TIMEOUT, handle.conn_tasks.wait_idle())
        .await
        .expect("dropped TCP clients release both connection slots");
    assert_load(&scrape(obs).await, [0, limit, 2, 0, cap, 0, 1]);
    guard.mark("clients dropped and connection slots released");

    shutdown(handle).await;
    guard.mark("launcher shut down cleanly");
    drop(temp);
}

#[test]
fn load_metrics_scrape_live_sources_from_this_boot() {
    std::thread::Builder::new()
        .stack_size(64 * 1024 * 1024)
        .spawn(|| {
            let guard = FlakeGuard::new("launcher load metrics");
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("test runtime")
                .block_on(async {
                    timeout(Duration::from_secs(300), Box::pin(run(&guard)))
                        .await
                        .expect("bounded launcher load metrics test");
                });
        })
        .expect("spawn test thread")
        .join()
        .unwrap_or_else(|panic| std::panic::resume_unwind(panic));
}

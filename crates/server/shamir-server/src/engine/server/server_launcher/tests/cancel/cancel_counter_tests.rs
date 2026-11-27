//! R4b-3 cancellation-counter binding test (end to end): every cancel
//! OUTCOME must reach `/metrics` through the REAL launcher, not just the
//! in-crate handler.
//!
//! The test boots the production `ServerLauncher` over a real temp data dir
//! with a real observability HTTP server on an OS-picked port, takes the
//! `#[cfg(test)]` `ActivityGaugeTestSeam` (the same handler `Arc` and
//! session store the launcher handed its listeners), and drives three
//! cancel operations of UNKNOWN ids through the real wire path
//! (`RequestHandler::handle`), then one REAL interactive transaction
//! cancelled by the same user's second session, then one REAL parked
//! interactive transaction whose `Committing` reservation is held through
//! the production `TxRegistry::begin_commit` path and refused by
//! `cancel_tx`. The test's repo is Fjall-backed on disk inside the same
//! tempdir the launcher boots from (mirroring the boot path's persistent
//! repo), and is flushed before the db handle drops. A real loopback
//! `GET /metrics` scrape of the address the observability server ACTUALLY
//! bound must then render:
//!
//! * `shamir_cancellations_total{result="already_finished"}` = 3
//! * `shamir_cancellations_total{result="cancelled"}` = 1
//! * `shamir_cancellations_total{result="already_committing"}` = 1
//! * `shamir_cancellations_total{result="not_cancellable"}` = 0
//!
//! Why the sessions are seeded into the real store instead of obtained
//! from a `ShamirClient`: the only data listener this fixture boots is a
//! plain (`tls: false`) HTTP listener. It serves REST and WebSocket with
//! bearer tokens, whereas `shamir_client`'s SCRAM entry points are
//! TLS-bound (`connect`/`connect_ws`) or raw plain TCP (`connect_plain`,
//! not HTTP); `BootstrapMode::Skip` over an empty tempdir also leaves no
//! credentials to authenticate. The handler under test is still driven
//! through the real wire `RequestHandler::handle` path. `not_cancellable`
//! is asserted at 0 because the only crate-visible test seam
//! (`track_committing_request_for_test`) registers a cancellable request,
//! while `activity_requests` is private to `db_handler`, so this
//! launcher-level test cannot install the handle-less request that path
//! needs.
//!
//! If the launcher's activity source were left unbound the family still
//! renders, but at all zeros — so the 3/1/1/0 asserts fail and expose the
//! missing binding. The per-outcome EXACTNESS (one slot, exactly once) is
//! proved in-crate by `every_cancel_operation_counts_its_outcome_exactly_once`.

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use shamir_connect::common::time::UnixNanos;
use shamir_connect::common::types::{BindingMode, TransportKind};
use shamir_connect::server::conn_services::ConnectionServices;
use shamir_connect::server::dispatch::RequestHandler;
use shamir_connect::server::session::{Session, SessionPermissions};
use shamir_db::access::{principal64, Actor};
use shamir_db::engine::repo::{BoxRepoFactory, RepoConfig};
use shamir_db::engine::table::TableConfig;

use shamir_query_builder::batch::Batch;
use shamir_query_builder::{cancel_cursor_admin, cancel_request, cancel_tx};
use shamir_query_types::admin::activity::{CancelOutcome, RequestExecId};
use shamir_query_types::batch::BatchOp;
use shamir_query_types::wire::{CursorId, DbRequest};
use shamir_test_diag::{try_install_global_tracing_ring, FlakeGuard};

use crate::config::{
    Config, KdfConfig, ListenerConfig, ListenerKind, LoggingConfig, ObservabilityConfig,
    SecurityConfig, TlsConfig,
};
use crate::db_handler::{DbResponse, NodeMode, ShamirDbHandler};
use crate::server::{BootstrapMode, ServerLauncher};
use crate::tx_registry::InteractiveTx;
use crate::version::CURRENT_QUERY_LANG_VERSION;

/// Hard per-call bound: every await is bounded, never an unbounded
/// engine await.
const CALL_TIMEOUT: Duration = Duration::from_secs(120);

/// Existing launcher/boot test convention: deep boot and engine futures
/// need a roomy stack; one runner per test, no runtime-inside-runtime.
fn on_big_stack<F, Fut>(body: F)
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = ()>,
{
    std::thread::Builder::new()
        .stack_size(64 * 1024 * 1024)
        .spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("test runtime")
                .block_on(body())
        })
        .expect("spawn test thread")
        .join()
        .unwrap_or_else(|panic| std::panic::resume_unwind(panic));
}

/// Minimal production-launcher config over a real temp data dir: one plain
/// loopback HTTP data listener (no TLS material needed) and observability
/// on port 0 (the ACTUAL bound port is read through the test seam, never
/// picked or retried by this test).
fn boot_config(data_dir: std::path::PathBuf) -> Config {
    Config {
        data_dir: data_dir.clone(),
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
            kind: ListenerKind::Http,
            addr: "127.0.0.1:0".into(),
            profile: None,
            http: crate::boot::config::HttpListenerSettings {
                tls: Some(false),
                ..Default::default()
            },
            path: None,
            kdf_override: None,
            browser_origin_allowlist: Vec::new(),
        }],
        tls: TlsConfig {
            cert_path: data_dir.join("cert.pem"),
            key_path: data_dir.join("key.pem"),
        },
        security: SecurityConfig {
            auth_init_rate_per_second: 64,
            ..Default::default()
        },
        audit: Default::default(),
        observability: ObservabilityConfig {
            addr: "127.0.0.1:0".into(),
            allow_public_metrics: false,
        },
        replication: None,
        backup: None,
    }
}

/// A distinct stamped session: fixed distinct sid bytes, per-user id.
/// The session timestamp is NOW so the real boot's GC sweep cannot expire
/// the seeded sessions mid-test.
fn stamped_session(tag: u8, name: &str, user: [u8; 16]) -> Session {
    let mut s = Session::new(
        user,
        name.into(),
        SessionPermissions::new(true, false, vec!["read_write".into()]),
        TransportKind::Tcp,
        BindingMode::TlsExporter,
        [0u8; 32],
        UnixNanos::now().as_u64(),
    );
    let mut sid = [0u8; 32];
    sid[0] = tag;
    sid[31] = tag.wrapping_mul(7).wrapping_add(1);
    s.session_id = sid;
    s
}

/// Raw bounded HTTP GET over loopback (the observability server speaks
/// plain HTTP; `Connection: close` bounds the read). The response head is
/// checked for a real `HTTP/1.1 200` status line; only the body is
/// returned.
async fn http_get(addr: SocketAddr, path: &str) -> String {
    let fut = async {
        let mut s = tokio::net::TcpStream::connect(addr)
            .await
            .expect("connect /metrics");
        let req = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
        s.write_all(req.as_bytes()).await.expect("write request");
        let mut buf = Vec::new();
        s.read_to_end(&mut buf).await.expect("read response");
        let hdr_end = buf
            .windows(4)
            .position(|w| w == b"\r\n\r\n")
            .unwrap_or(buf.len());
        let head = String::from_utf8_lossy(&buf[..hdr_end]).into_owned();
        assert!(
            head.starts_with("HTTP/1.1 200"),
            "/metrics must answer HTTP 200, got head: {head:?}"
        );
        buf[hdr_end + 4..].to_vec()
    };
    tokio::time::timeout(CALL_TIMEOUT, fut)
        .await
        .expect("bounded metrics scrape")
        .iter()
        .map(|b| *b as char)
        .collect()
}

/// Parse `shamir_cancellations_total{result="<result>"} <n>` out of the
/// Prometheus text. A missing series fails even for the zero cases: the
/// export renders all four families from the first scrape.
fn series_value(metrics: &str, name: &str, result: &str) -> u64 {
    let prefix = format!("{name}{{result=\"{result}\"}} ");
    let mut found = None;
    for line in metrics.lines() {
        if let Some(rest) = line.strip_prefix(&prefix) {
            assert!(found.is_none(), "duplicate series {prefix:?}");
            found = Some(
                rest.trim()
                    .parse()
                    .unwrap_or_else(|e| panic!("series {prefix:?}: bad value {rest:?}: {e}")),
            );
        }
    }
    found.unwrap_or_else(|| panic!("series {prefix:?} missing from /metrics"))
}

/// Drive one cancel operation through the REAL wire path.
async fn cancel_via_wire(
    handler: &ShamirDbHandler,
    session: &Session,
    op: BatchOp,
) -> CancelOutcome {
    let mut batch = Batch::new();
    batch.op("cancel", op);
    let request = DbRequest::Execute {
        query_version: CURRENT_QUERY_LANG_VERSION,
        db: "app".into(),
        batch: batch.build(),
    };
    let bytes = rmp_serde::to_vec_named(&request).expect("encode cancel request");
    let raw = tokio::time::timeout(
        CALL_TIMEOUT,
        handler.handle(session, &bytes, &ConnectionServices::without_push(0)),
    )
    .await
    .expect("bounded cancel handle")
    .expect("cancel handle");
    match rmp_serde::from_slice::<DbResponse>(&raw).expect("decode cancel reply") {
        DbResponse::CancelOutcome { outcome } => outcome,
        other => panic!("expected CancelOutcome, got {other:?}"),
    }
}

#[test]
fn launcher_scrapes_cancellation_counters_per_result() {
    on_big_stack(|| async {
        tokio::time::timeout(Duration::from_secs(240), run())
            .await
            .expect("entire cancellation-counter scrape test bounded");
    });
}

async fn run() {
    let guard = FlakeGuard::with_watchdog(
        "launcher_scrapes_cancellation_counters_per_result",
        Duration::from_secs(220),
    );
    try_install_global_tracing_ring();
    let temp = tempfile::tempdir().expect("tempdir");
    let launcher = ServerLauncher {
        config: boot_config(temp.path().to_path_buf()),
        bootstrap: BootstrapMode::Skip,
        node_mode: NodeMode::ReadWrite,
    };
    let handle = launcher.launch().await.expect("production boot");
    guard.mark("server launched");

    // The seam is cfg(test)-only and populated by launch() whenever an
    // observability server was configured (this config always runs one);
    // reading the REAL handler/store/address out of it is what makes this a
    // binding test, not a parallel fixture.
    let (handler, store, obs_addr) = {
        let seam = handle
            .activity_test_seam
            .as_ref()
            .expect("test seam captured");
        (
            Arc::clone(&seam.handler),
            Arc::clone(&seam.session_store),
            seam.observability_bound_addr,
        )
    };
    assert_eq!(obs_addr.ip(), std::net::IpAddr::from([127, 0, 0, 1]));
    assert_ne!(
        obs_addr.port(),
        0,
        "config said port 0; the server must have bound a real one"
    );
    guard.note("observability bound addr", obs_addr.to_string());

    // Two superuser sessions for the same user: the owner opens the
    // transaction, the second cancels it (same user id ⇒ no Manage needed).
    let owner = stamped_session(0x51, "alice-owner", [0xAA; 16]);
    let control = stamped_session(0x52, "alice-control", [0xAA; 16]);
    let owner = store.insert(owner.session_id, owner);
    let control = store.insert(control.session_id, control);
    assert_ne!(owner.activity_handle(), control.activity_handle());

    let db = handler.db().clone();
    let owner_actor = Actor::User(principal64([0xAA; 16]));
    db.create_db_as("app", owner_actor.clone())
        .await
        .expect("create_db");
    // Persistent Fjall repo INSIDE the launcher's tempdir (same backend the
    // boot path uses for its durable repos), so this fixture never falls
    // back to an in-memory store.
    let repo_path = temp.path().join("app_main");
    db.add_repo_as(
        "app",
        RepoConfig::new("main", BoxRepoFactory::fjall(&repo_path))
            .add_table(TableConfig::new("items")),
        owner_actor.clone(),
    )
    .await
    .expect("add repo");
    let open = shamir_types::access::ResourceMeta::open();
    for path in [
        shamir_types::access::ResourcePath::database("app"),
        shamir_types::access::ResourcePath::store("app", "main"),
        shamir_types::access::ResourcePath::table("app", "main", "items"),
    ] {
        db.set_resource_meta(&path, &open)
            .await
            .expect("open resource");
    }
    guard.mark("db, repo and resource metas ready");

    // Three cancel operations of UNKNOWN ids through the real wire path.
    // The server answers `already_finished` for any unknown target.
    for op in [
        cancel_request(RequestExecId(404)),
        cancel_tx(404),
        cancel_cursor_admin(CursorId(404)),
    ] {
        let outcome = cancel_via_wire(&handler, &owner, op).await;
        assert_eq!(outcome, CancelOutcome::AlreadyFinished);
    }
    assert_eq!(
        handler.activity_counts().cancellations,
        [0, 3, 0, 0],
        "three unknown targets count as already_finished"
    );
    guard.mark("three unknown-target cancels counted");

    // One REAL interactive transaction through the same `handle` path, then
    // cancelled by the same user's second session.
    let begin = DbRequest::TxBegin {
        query_version: CURRENT_QUERY_LANG_VERSION,
        db: "app".into(),
        repo: "main".into(),
        isolation: None,
    };
    let bytes = rmp_serde::to_vec_named(&begin).expect("encode TxBegin");
    let raw = tokio::time::timeout(
        CALL_TIMEOUT,
        handler.handle(&owner, &bytes, &ConnectionServices::without_push(0)),
    )
    .await
    .expect("bounded TxBegin")
    .expect("TxBegin handle");
    let tx_handle = match rmp_serde::from_slice::<DbResponse>(&raw).expect("decode TxBegin") {
        DbResponse::TxOpened { tx_handle, .. } => tx_handle,
        other => panic!("expected TxOpened, got {other:?}"),
    };
    let outcome = cancel_via_wire(&handler, &control, cancel_tx(tx_handle)).await;
    assert_eq!(outcome, CancelOutcome::Cancelled);
    assert_eq!(
        handler.activity_counts().cancellations,
        [1, 3, 0, 0],
        "one real cancel lands in the cancelled slot only"
    );
    guard.mark("real transaction cancelled");

    // A second REAL parked interactive transaction, reserved for commit via
    // the production `TxRegistry::begin_commit` path. The registry's
    // `Committing` reservation makes `cancel_tx` answer `already_committing`
    // while the guard is held.
    let (parked_tx, parked_snapshot, _repo) = db
        .tx_begin_as(owner_actor.clone(), "app", "main", "snapshot")
        .await
        .expect("begin parked transaction");
    let parked_handle = parked_tx.tx_id.0;
    let parked = handler
        .tx_registry()
        .register(
            parked_handle,
            InteractiveTx::new(
                parked_tx,
                parked_snapshot,
                owner.session_id,
                owner.user_id,
                "app".into(),
                "main".into(),
                Duration::from_secs(3600),
                handler.tx_abort_release("app", "main"),
            ),
        )
        .expect("park transaction");
    let commit_guard = handler
        .tx_registry()
        .begin_commit(parked_handle, &parked)
        .expect("reserve committing state");
    let outcome = cancel_via_wire(&handler, &control, cancel_tx(parked_handle)).await;
    assert_eq!(outcome, CancelOutcome::AlreadyCommitting);
    assert_eq!(
        handler.activity_counts().cancellations,
        [1, 3, 1, 0],
        "a committing reservation lands in the already_committing slot only"
    );
    guard.mark("committing reservation refused");
    drop(commit_guard);
    drop(parked);

    // THE BINDING PROOF: a real loopback HTTP scrape of the address the
    // observability server ACTUALLY bound renders the per-result counters.
    let metrics = http_get(obs_addr, "/metrics").await;
    guard.note("scrape", "cancellation /metrics fetched");
    assert_eq!(
        series_value(&metrics, "shamir_cancellations_total", "already_finished"),
        3
    );
    assert_eq!(
        series_value(&metrics, "shamir_cancellations_total", "cancelled"),
        1
    );
    assert_eq!(
        series_value(&metrics, "shamir_cancellations_total", "already_committing"),
        1
    );
    assert_eq!(
        series_value(&metrics, "shamir_cancellations_total", "not_cancellable"),
        0
    );
    assert_eq!(
        metrics
            .matches("# TYPE shamir_cancellations_total counter")
            .count(),
        1,
        "the counter family must be declared exactly once"
    );
    let mut cancel_lines = 0;
    for line in metrics
        .lines()
        .filter(|line| line.starts_with("shamir_cancellations_total"))
    {
        cancel_lines += 1;
        assert!(
            !line.contains("user="),
            "no per-user label on cancel series: {line:?}"
        );
        assert!(
            !line.contains("session="),
            "no per-session label on cancel series: {line:?}"
        );
    }
    assert_eq!(cancel_lines, 4, "one series per fixed result slot");
    guard.mark("per-result counters scraped");

    // Flush the on-disk Fjall repo before releasing the db handle; the
    // launcher's own shutdown path flushes again after connections drain.
    // The tempdir stays alive until after shutdown.
    tokio::time::timeout(CALL_TIMEOUT, db.flush_all_waiting())
        .await
        .expect("bounded repo flush")
        .expect("flush persistent repo");
    guard.mark("persistent repo flushed");

    // Drop this test's extra strong references before shutdown: the boot
    // db, concrete handler, session store and seeded session arcs would
    // otherwise keep the stores alive past the release barrier. The seam
    // also holds strong refs — take it off the handle first.
    drop(db);
    drop(handler);
    drop(store);
    drop(owner);
    drop(control);
    let mut handle = handle;
    drop(handle.activity_test_seam.take());
    let report = tokio::time::timeout(CALL_TIMEOUT, handle.shutdown())
        .await
        .expect("bounded shutdown");
    assert!(report.is_clean(), "shutdown must be clean: {report:?}");
    guard.mark("shutdown complete");
    drop(temp);
}

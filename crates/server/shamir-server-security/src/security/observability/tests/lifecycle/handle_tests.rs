//! `ObservabilityHandle` lifecycle (item 9 / P2): `shutdown()` JOINS both of
//! the handle's tasks and REPORTS which of them actually joined, and dropping a
//! handle without `shutdown()` still cancels the shared token — now through the
//! private `shutdown_on_drop` field instead of an `impl Drop for
//! ObservabilityHandle` (which made the public type non-move-partial).

use std::sync::Arc;
use std::time::Duration;

use axum::body::to_bytes;
use axum::extract::State;
use axum::http::StatusCode;
use metrics_exporter_prometheus::PrometheusBuilder;
use shamir_db::{ShamirDb, SystemStoreConfig};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_util::sync::CancellationToken;

use crate::observability::{
    metrics_handler, spawn_with_byte_budget, spawn_with_prom_handle_for_tests, AppState,
    CancelOnDrop, ObservabilityHandle, ObservabilityShutdownOutcome, ObservabilityState,
    OBS_TASK_JOIN_TIMEOUT,
};

/// Bounded await for a shutdown that has nothing left to drain. Real time, no
/// sleeps: the bound can only be hit by a task that fails to observe the
/// cancel, which is exactly what the assertion pins.
const SHUTDOWN_BOUND: Duration = Duration::from_secs(5);

/// A real observability server over a `TempDir`-backed `ShamirDb`, so the
/// poller has a database to sample and the listener has a live socket.
async fn spawn_on_tempdir() -> (TempDir, ObservabilityHandle) {
    let dir = TempDir::new().expect("tempdir");
    let db = ShamirDb::init(SystemStoreConfig::Fjall(dir.path().join("system")))
        .await
        .expect("init db");
    // `install_recorder = false`: the Prometheus recorder is process-global and
    // another test in this binary may own it. The listener and the poller (over
    // `db`) are the real ones either way.
    let handle = spawn_with_byte_budget(
        "127.0.0.1:0".parse().expect("addr"),
        ObservabilityState::new(),
        false,
        None,
        None,
        Some(Arc::new(db)),
        false,
    )
    .await
    .expect("spawn observability");
    (dir, handle)
}

// Pins: `shutdown()` returns inside the bound after `cancel()` (both joins
// complete, the listener socket is really gone), `Drop` without
// `shutdown()` cancels the token both tasks wait on, and the returned outcome
// names each side instead of collapsing them into "the listener or the poller".
//
// Mutants killed: "no cancel-on-drop" (drop the `CancelOnDrop` field or its
// `Drop`) — the second phase's token stays live; and "shutdown() does not wait
// for cancellation" (detach/early-return) — the listener socket survives the
// returned `shutdown()`. It does NOT pin the in-tick `select!` on its own: a
// tick body that merely races cancellation can only be caught deterministically
// by a `degraded_index_count` the test can park, which this fixture cannot.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_joins_both_tasks_and_drop_cancels_the_token() {
    let (dir, handle) = spawn_on_tempdir().await;
    let bound_addr = handle.bound_addr;
    // The token is private; this module is a descendant of `observability`, so
    // it may read it — and reading it is what makes the cancel observable.
    let token = handle.shutdown_on_drop.0.clone();

    // Exercise the handler once through the live listener, then close the
    // connection, so the listener task is stopped with a request it has already
    // served (no parked handler, no sleep).
    let mut conn = TcpStream::connect(bound_addr).await.expect("connect");
    conn.write_all(b"GET /metrics HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n")
        .await
        .expect("write request");
    let mut response = Vec::new();
    tokio::time::timeout(SHUTDOWN_BOUND, conn.read_to_end(&mut response))
        .await
        .expect("the handler must answer and close")
        .expect("read response");
    assert!(
        String::from_utf8_lossy(&response).starts_with("HTTP/1.1 503"),
        "no recorder was installed for this handle, so /metrics must answer 503, got: {:?}",
        String::from_utf8_lossy(&response)
            .chars()
            .take(80)
            .collect::<String>()
    );
    drop(conn);

    handle.cancel();
    assert!(token.is_cancelled(), "cancel() must reach the shared token");
    let outcome = tokio::time::timeout(SHUTDOWN_BOUND, handle.shutdown())
        .await
        .expect("shutdown() must join both tasks after cancel(), not detach them");
    assert_eq!(
        outcome,
        ObservabilityShutdownOutcome {
            listener_joined: true,
            poller_joined: true,
        },
        "a fully stopped server must not report a side as still draining"
    );
    assert!(
        TcpStream::connect(bound_addr).await.is_err(),
        "the listener socket must be gone once shutdown() returned (task joined)"
    );
    drop(dir);

    // Phase 2: Drop without `shutdown()`.
    let (_dir, handle) = spawn_on_tempdir().await;
    let token = handle.shutdown_on_drop.0.clone();
    assert!(
        !token.is_cancelled(),
        "a running handle must not be pre-cancelled"
    );
    drop(handle);
    assert!(
        token.is_cancelled(),
        "dropping the handle without shutdown() must cancel both tasks"
    );
}

// A connected client sends a `/metrics` request and deliberately leaves the
// response unread. The test cancels the handle before calling `shutdown()`, so
// it does not establish that the handler entered sampling or was parked in the
// drain set. It asserts listener/poller joins and that shutdown closes both
// the connection and listening socket with a pending request.
//
// Rendezvous is `connect` + `write_all` only (no sleep, no read before the
// shutdown); the cancellation is explicit and precedes shutdown.
//
// Mutants killed: "shutdown() detaches the listener join" (the socket stays
// open and the read below times out); "shutdown() returns () and collapses
// both sides" (no outcome to assert at all). This does not pin a parked handler
// or an in-flight database sample.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_closes_listener_with_a_pending_unread_request() {
    let (dir, handle) = spawn_on_tempdir().await;
    let bound_addr = handle.bound_addr;

    let mut parked = TcpStream::connect(bound_addr).await.expect("connect");
    // No `Connection: close`: the connection is meant to be parked mid-drain,
    // not politely finished by the client.
    parked
        .write_all(b"GET /metrics HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n")
        .await
        .expect("write request");
    // Deliberately do NOT read the response.

    handle.cancel();
    let outcome = tokio::time::timeout(SHUTDOWN_BOUND, handle.shutdown())
        .await
        .expect("a parked, unread scrape must not keep shutdown() past its bound");
    assert!(
        outcome.listener_joined,
        "the listener must drain/abort the parked connection inside its own \
         {OBS_TASK_JOIN_TIMEOUT:?} bound, got {outcome:?}"
    );
    assert!(
        outcome.poller_joined,
        "the poller was idle and cancelled, so it must not be reported as stuck, \
         got {outcome:?}"
    );

    // Closed afterwards: either FIN (EOF) or a reset if the server closed with
    // the request bytes still unread — both mean the socket is gone, while a
    // still-served connection blocks here until the timeout.
    let mut drained = Vec::new();
    let read_result = tokio::time::timeout(SHUTDOWN_BOUND, parked.read_to_end(&mut drained))
        .await
        .expect("the parked socket must be closed by the time shutdown() returned");
    match read_result {
        Ok(_) => {}
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::ConnectionAborted
                    | std::io::ErrorKind::BrokenPipe
            ) => {}
        Err(error) => panic!("unexpected error reading the parked socket after shutdown: {error}"),
    }
    assert!(
        TcpStream::connect(bound_addr).await.is_err(),
        "the listening socket itself must be closed after shutdown()"
    );
    drop(dir);
}

// Pins the per-side report over the real fixture: the poller has a real
// database and is cancelled before its first tick, then joins normally. This
// does not establish that sampling was in flight.
//
// Mutants killed: "poller_joined hard-coded false" / "the outcome swaps the two
// sides" (caught together with the wedged-listener test, which has one `true`
// and one `false`); "shutdown() returns ()" (nothing to assert).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_joins_a_real_database_poller_cancelled_before_first_tick() {
    let (_dir, handle) = spawn_on_tempdir().await;
    let bound_addr = handle.bound_addr;
    let token = handle.shutdown_on_drop.0.clone();

    handle.cancel();
    assert!(
        token.is_cancelled(),
        "cancel() must reach the token the poller selects on, immediately"
    );
    let outcome = tokio::time::timeout(SHUTDOWN_BOUND, handle.shutdown())
        .await
        .expect("shutdown() must join both tasks after cancel(), not detach them");
    assert!(
        outcome.poller_joined,
        "the poller was cancelled before its first tick, so it must join, got {outcome:?}"
    );
    assert!(
        outcome.listener_joined,
        "the listener saw no request, so it must join too, got {outcome:?}"
    );
    assert!(TcpStream::connect(bound_addr).await.is_err());
}

// Pins that ONE wedged side cannot hide the other: a handle whose listener task
// ignores cancellation (it never awaits the token) must report
// `listener_joined == false`, still report the healthy poller as joined, and
// still return inside the caller's bound. The wedged task is `pending()`, not a
// sleep — it deterministically never finishes, so the per-side bound is the
// only thing that can end the wait.
//
// Mutants killed: "one stuck side hides the other" (the old sequential
// unbounded awaits, or one shared budget that consumed the poller's share);
// "the outcome swaps the two sides" (`listener_joined == false` +
// `poller_joined == true` cannot be produced by a swap); "an expired side is
// reported as joined" (`!outcome.listener_joined`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_names_a_wedged_listener_without_hiding_the_poller() {
    let started = std::time::Instant::now();
    let handle = ObservabilityHandle {
        bound_addr: "127.0.0.1:0".parse().expect("addr"),
        state: ObservabilityState::new(),
        shutdown_on_drop: CancelOnDrop(CancellationToken::new()),
        // Never observes the token, never returns.
        listener_task: Some(tokio::spawn(std::future::pending::<()>())),
        poller_task: Some(tokio::spawn(async {})),
    };

    let outcome = tokio::time::timeout(SHUTDOWN_BOUND, handle.shutdown())
        .await
        .expect("one wedged side must not stall the whole shutdown() call");
    assert_eq!(
        outcome,
        ObservabilityShutdownOutcome {
            listener_joined: false,
            poller_joined: true,
        },
        "the wedged listener must be reported as still draining and the healthy \
         poller as joined"
    );
    assert!(
        started.elapsed() >= OBS_TASK_JOIN_TIMEOUT,
        "the wedged side must actually be awaited up to its own bound \
         ({OBS_TASK_JOIN_TIMEOUT:?}) before it is reported, elapsed {:?}",
        started.elapsed()
    );
}

// Pins the already-cancelled handler path: with a recorder and database
// present, a pre-cancelled request answers `503 not ready` without sampling.
// This does not prove cancellation interrupts a sample already in flight.
//
// Mutants killed: "no cancellation check before sampling" — without the
// `select!`, this pre-cancelled state would render and answer `200 OK`.
#[tokio::test]
async fn pre_cancelled_metrics_handler_answers_503_without_sampling_the_database() {
    let dir = TempDir::new().expect("tempdir");
    let db = ShamirDb::init(SystemStoreConfig::Fjall(dir.path().join("system")))
        .await
        .expect("init db");
    // A recorder handle that is NOT installed globally: `render()` works on it,
    // so the handler would answer `200` if it did not observe the token.
    let recorder = PrometheusBuilder::new().build_recorder();
    let shutdown = CancellationToken::new();
    shutdown.cancel();
    let state = AppState {
        state: ObservabilityState::new(),
        prom: Some(recorder.handle()),
        db: Some(Arc::new(db)),
        byte_budget: None,
        shutdown,
    };

    let response = tokio::time::timeout(SHUTDOWN_BOUND, metrics_handler(State(state)))
        .await
        .expect("a pre-cancelled handler must answer at once, before sampling the database");
    assert_eq!(
        response.status(),
        StatusCode::SERVICE_UNAVAILABLE,
        "a cancelled server must answer 503, not render metrics off the database"
    );
    let body = to_bytes(response.into_body(), 64)
        .await
        .expect("metrics body");
    assert_eq!(&body[..], b"not ready\n");
}

/// Maximum time to rendezvous with a parked handler or the poller's first tick.
const PARK_BOUND: Duration = Duration::from_secs(60);

/// Whole-test bound for lifecycle regression tests.
const TEST_BOUND: Duration = Duration::from_secs(180);

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_while_parked_metrics_sample_answers_503_without_releasing_the_park() {
    tokio::time::timeout(TEST_BOUND, async {
        let dir = TempDir::new().expect("tempdir");
        let db = ShamirDb::init(SystemStoreConfig::Fjall(dir.path().join("system")))
            .await
            .expect("init db");
        let state = ObservabilityState::new();
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let release = state.arm_metrics_park(entered_tx);
        let token = CancellationToken::new();
        let recorder = PrometheusBuilder::new().build_recorder();
        let app_state = AppState {
            state,
            prom: Some(recorder.handle()),
            db: Some(Arc::new(db)),
            byte_budget: None,
            shutdown: token.clone(),
        };
        let handler = tokio::spawn(metrics_handler(State(app_state)));
        tokio::time::timeout(PARK_BOUND, entered_rx)
            .await
            .expect("handler must enter the parked sample")
            .expect("park entry signal");

        token.cancel();
        let response = tokio::time::timeout(SHUTDOWN_BOUND, handler)
            .await
            .expect("cancellation must interrupt the parked sample")
            .expect("handler task");
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = to_bytes(response.into_body(), 64)
            .await
            .expect("metrics body");
        assert_eq!(&body[..], b"not ready\n");
        assert!(
            release.is_closed(),
            "cancellation must drop parked receiver without release"
        );
        // The handler future has completed; dropping this sender is the release-on-drop guard.
        drop(release);

    })
    .await
    .expect("cancelled_while_parked_metrics_sample_answers_503_without_releasing_the_park must finish inside TEST_BOUND");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn released_park_lets_metrics_handler_answer_200() {
    tokio::time::timeout(TEST_BOUND, async {
        let dir = TempDir::new().expect("tempdir");
        let db = ShamirDb::init(SystemStoreConfig::Fjall(dir.path().join("system")))
            .await
            .expect("init db");
        let state = ObservabilityState::new();
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let release = state.arm_metrics_park(entered_tx);
        let recorder = PrometheusBuilder::new().build_recorder();
        let app_state = AppState {
            state,
            prom: Some(recorder.handle()),
            db: Some(Arc::new(db)),
            byte_budget: None,
            shutdown: CancellationToken::new(),
        };
        let handler = tokio::spawn(metrics_handler(State(app_state)));
        tokio::time::timeout(PARK_BOUND, entered_rx)
            .await
            .expect("handler must enter the parked sample")
            .expect("park entry signal");
        let _ = release.send(());
        let response = tokio::time::timeout(SHUTDOWN_BOUND, handler)
            .await
            .expect("released handler must finish")
            .expect("handler task");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("metrics body");
        assert!(!body.is_empty());
    })
    .await
    .expect("released_park_lets_metrics_handler_answer_200 must finish inside TEST_BOUND");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_while_parked_poller_tick_joins_without_releasing_the_park() {
    tokio::time::timeout(TEST_BOUND, async {
        let dir = TempDir::new().expect("tempdir");
        let db = ShamirDb::init(SystemStoreConfig::Fjall(dir.path().join("system")))
            .await
            .expect("init db");
        let state = ObservabilityState::new();
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release, _finished) = state.arm_poller_park(entered_tx);
        let handle = spawn_with_byte_budget(
            "127.0.0.1:0".parse().expect("addr"),
            state,
            false,
            None,
            None,
            Some(Arc::new(db)),
            false,
        )
        .await
        .expect("spawn observability");
        tokio::time::timeout(PARK_BOUND, entered_rx)
            .await
            .expect("poller must enter its parked tick")
            .expect("park entry signal");

        handle.cancel();
        let outcome = tokio::time::timeout(SHUTDOWN_BOUND, handle.shutdown())
            .await
            .expect("cancelled parked poller must join");
        assert!(outcome.poller_joined, "poller must join: {outcome:?}");
        assert!(outcome.listener_joined, "listener must join: {outcome:?}");
        assert!(
            release.is_closed(),
            "cancellation must drop parked receiver without release"
        );
        // The parked future was dropped with the task; dropping this sender is the release-on-drop guard.
        drop(release);

    })
    .await
    .expect("cancelled_while_parked_poller_tick_joins_without_releasing_the_park must finish inside TEST_BOUND");
}

/// Positive control of the cancellation race: a released tick runs to the END
/// of its body before the token is cancelled, so a tick that is dropped by the
/// token, returns early or panics never fires the completion handshake and the
/// test times out. NOT covered: deleting or replacing `db.degraded_index_count()
/// .await` or the `shamir_degraded_indexes_total` gauge writes, because the
/// handshake is sent regardless of their results (the process-global metrics
/// facade exposes no read of its installed recorder, and a local builder handle
/// renders only what it recorded).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn released_park_lets_the_poller_tick_complete() {
    tokio::time::timeout(TEST_BOUND, async {
        let dir = TempDir::new().expect("tempdir");
        let db = ShamirDb::init(SystemStoreConfig::Fjall(dir.path().join("system")))
            .await
            .expect("init db");
        let state = ObservabilityState::new();
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release, finished) = state.arm_poller_park(entered_tx);
        let handle = spawn_with_byte_budget(
            "127.0.0.1:0".parse().expect("addr"),
            state,
            false,
            None,
            None,
            Some(Arc::new(db)),
            false,
        )
        .await
        .expect("spawn observability");
        tokio::time::timeout(PARK_BOUND, entered_rx)
            .await
            .expect("poller must enter its parked tick")
            .expect("park entry signal");

        let _ = release.send(());
        // Completion fires only after the whole tick, including the database await
        // and gauge write. Await before cancellation so the biased select cannot drop it.
        tokio::time::timeout(PARK_BOUND, finished)
            .await
            .expect("the released tick must run to its last statement")
            .expect("tick-finished signal");
        let outcome = tokio::time::timeout(SHUTDOWN_BOUND, handle.shutdown())
            .await
            .expect("completed tick must join");
        assert!(outcome.poller_joined, "poller must join: {outcome:?}");
    })
    .await
    .expect("released_park_lets_the_poller_tick_complete must finish inside TEST_BOUND");
}

/// Wiring the live router to a token other than the handle's leaves the parked
/// request unanswered, so the response read times out. Driving `axum::serve`
/// with another token does NOT stop the handler's own answer: that mutation is
/// caught by the `listener_joined` assertion, not by the socket. The exact body
/// is compared after the header separator. The socket observes the real route,
/// but parking remains before the real `ShamirDb` await (the same limitation as
/// the direct-handler test).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_while_parked_live_metrics_route_answers_503_through_the_handle() {
    tokio::time::timeout(TEST_BOUND, async {
        let dir = TempDir::new().expect("tempdir");
        let db = ShamirDb::init(SystemStoreConfig::Fjall(dir.path().join("system")))
            .await
            .expect("init db");
        let state = ObservabilityState::new();
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let release = state.arm_metrics_park(entered_tx);
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = spawn_with_prom_handle_for_tests(
            "127.0.0.1:0".parse().expect("addr"),
            state,
            Some(recorder.handle()),
            None,
            Some(Arc::new(db)),
        )
        .await
        .expect("spawn observability with pre-built recorder");
        let mut conn = TcpStream::connect(handle.bound_addr).await.expect("connect");
        conn.write_all(b"GET /metrics HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n")
            .await
            .expect("write request");
        tokio::time::timeout(PARK_BOUND, entered_rx)
            .await
            .expect("the live /metrics route must enter the parked sample")
            .expect("park entry signal");
        handle.cancel();
        let mut response = Vec::new();
        tokio::time::timeout(SHUTDOWN_BOUND, conn.read_to_end(&mut response))
            .await
            .expect("cancelled live route must answer")
            .expect("read response");
        let text = String::from_utf8_lossy(&response);
        assert!(text.starts_with("HTTP/1.1 503"), "observed prefix: {:?}", text.get(..32));
        let body = text.split_once("\r\n\r\n").map(|(_, body)| body);
        assert_eq!(body, Some("not ready\n"), "observed response: {text:?}");
        assert!(release.is_closed(), "cancel must drop parked receiver without release");
        drop(release);
        let outcome = tokio::time::timeout(SHUTDOWN_BOUND, handle.shutdown())
            .await
            .expect("cancelled live server must join");
        assert!(outcome.listener_joined, "listener must join: {outcome:?}");
        assert!(outcome.poller_joined, "idle poller must join: {outcome:?}");
    })
    .await
    .expect("cancelled_while_parked_live_metrics_route_answers_503_through_the_handle must finish inside TEST_BOUND");
}

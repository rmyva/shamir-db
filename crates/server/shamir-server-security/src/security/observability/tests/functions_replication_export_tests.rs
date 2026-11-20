use std::collections::BTreeSet;
use std::future::Future;
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll, Wake, Waker};
use std::time::{SystemTime, UNIX_EPOCH};

use axum::{body::to_bytes, extract::State};
use metrics_exporter_prometheus::PrometheusBuilder;
use shamir_db::engine::function::{call_metrics_snapshot, ADMISSION_REJECTION_REASONS};

use super::super::export::functions::{record_function_snapshot, register_functions_metrics};
use super::super::export::replication::{
    record_replication_metrics, record_replication_snapshot, register_replication_metrics,
    ReplicationSnapshot,
};
use super::super::{
    install_replication_metrics_source, metrics_handler, AppState, ObservabilityState, StreamState,
    STREAM_STATES,
};

const DB: &str = "export_test_db";
const REPO: &str = "replica";
const DURATION_BOUNDS: &[u64] = &[1_000_000, 2_000_000];

struct NoopWake;

impl Wake for NoopWake {
    fn wake(self: Arc<Self>) {}
}

fn poll_ready_once<F: Future>(future: F) -> F::Output {
    let waker = Waker::from(Arc::new(NoopWake));
    let mut context = Context::from_waker(&waker);
    let mut future = Box::pin(future);
    match future.as_mut().poll(&mut context) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("metrics handler unexpectedly yielded"),
    }
}

fn replication_fixture() -> Vec<ReplicationSnapshot> {
    let now_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| {
            elapsed.as_nanos().min(u128::from(u64::MAX)) as u64
        });
    vec![ReplicationSnapshot {
        db: DB.to_string(),
        repo: REPO.to_string(),
        lag_versions: 7,
        last_receipt_ns: now_ns.saturating_sub(2_000_000_000),
        state_code: StreamState::Streaming.code(),
        reconnects: 3,
    }]
}

static REPLICATION_SOURCE_INSTALLED: OnceLock<()> = OnceLock::new();

fn install_fixture_source() {
    REPLICATION_SOURCE_INSTALLED.get_or_init(|| {
        install_replication_metrics_source(replication_fixture);
    });
}

fn find_sample<'a>(rendered: &'a str, name: &str, labels: &[&str]) -> &'a str {
    rendered
        .lines()
        .find(|line| {
            let Some((sample_name, rest)) = line.split_once('{') else {
                return false;
            };
            sample_name == name && labels.iter().all(|label| rest.contains(label))
        })
        .unwrap_or_else(|| panic!("missing {name} sample with labels {labels:?}\n{rendered}"))
}

fn assert_label_keys(line: &str, expected: &[&str]) {
    let (_, labels) = line.split_once('{').expect("labelled series");
    let labels = labels.split_once('}').expect("closed label set").0;
    let actual: BTreeSet<_> = labels
        .split(',')
        .map(|label| label.split_once('=').expect("label value").0)
        .collect();
    let expected: BTreeSet<_> = expected.iter().copied().collect();
    assert_eq!(actual, expected, "unexpected labels: {line}");
}

fn assert_value(line: &str, expected: &str) {
    assert!(
        line.ends_with(&format!(" {expected}")),
        "wrong value: {line}"
    );
}

fn assert_type(rendered: &str, name: &str, kind: &str) {
    assert!(
        rendered
            .lines()
            .any(|line| line == format!("# TYPE {name} {kind}")),
        "missing TYPE {kind} for {name}\n{rendered}"
    );
}

#[test]
fn functions_and_replication_exports_render_expected_series() {
    install_fixture_source();
    let recorder = PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let mut function = call_metrics_snapshot()
        .into_iter()
        .next()
        .expect("function registry includes its aggregate snapshot");
    function.db = DB.to_string();
    function.repo = REPO.to_string();
    function.duration.bounds = DURATION_BOUNDS;
    function.duration.buckets = vec![1, 1, 1];
    function.duration.count = 3;
    function.duration.sum = 3_500_000;
    function.duration.torn = false;
    function.fuel_total = 91;
    function.depth_high_water = 7;

    metrics::with_local_recorder(&recorder, || {
        register_functions_metrics();
        register_replication_metrics();
        record_function_snapshot(&function);
        record_function_snapshot(&function);
        record_replication_metrics();
        record_replication_metrics();
    });
    let rendered = handle.render();

    let function_series = [
        ("shamir_compile_cache_hits_total", "counter"),
        ("shamir_compile_cache_misses_total", "counter"),
        ("shamir_compile_cache_bytes", "gauge"),
        ("shamir_compile_cache_entries", "gauge"),
        ("shamir_module_admission_rejected_total", "counter"),
        ("shamir_function_fuel_consumed_total", "counter"),
        ("shamir_function_duration_seconds_bucket", "gauge"),
        ("shamir_function_duration_seconds_count", "counter"),
        ("shamir_function_duration_seconds_sum", "gauge"),
        ("shamir_function_call_depth_high_water", "gauge"),
    ];
    for (name, kind) in function_series {
        assert!(rendered.contains(name), "missing function series {name}");
        assert_type(&rendered, name, kind);
    }
    assert_type(&rendered, "shamir_replication_lag_versions", "gauge");
    assert_type(&rendered, "shamir_replication_lag_seconds", "gauge");
    assert_type(&rendered, "shamir_replication_stream_state", "gauge");
    assert_type(&rendered, "shamir_replication_reconnects_total", "counter");

    for (name, expected) in [
        ("shamir_compile_cache_hits_total", "0"),
        ("shamir_compile_cache_misses_total", "0"),
        ("shamir_compile_cache_bytes", "0"),
        ("shamir_compile_cache_entries", "0"),
    ] {
        let line = rendered
            .lines()
            .find(|line| *line == format!("{name} {expected}"))
            .unwrap_or_else(|| panic!("missing zero-touch {name}\n{rendered}"));
        assert_value(line, expected);
    }
    for reason in ADMISSION_REJECTION_REASONS {
        let line = find_sample(
            &rendered,
            "shamir_module_admission_rejected_total",
            &[&format!("reason=\"{reason}\"")],
        );
        assert_label_keys(line, &["reason"]);
        assert_value(line, "0");
    }

    let scope_labels = ["db=\"export_test_db\"", "repo=\"replica\""];
    let fuel = find_sample(
        &rendered,
        "shamir_function_fuel_consumed_total",
        &scope_labels,
    );
    assert_label_keys(fuel, &["db", "repo"]);
    assert_value(fuel, "91");
    let depth = find_sample(
        &rendered,
        "shamir_function_call_depth_high_water",
        &scope_labels,
    );
    assert_label_keys(depth, &["db", "repo"]);
    assert_value(depth, "7");
    let count = find_sample(
        &rendered,
        "shamir_function_duration_seconds_count",
        &scope_labels,
    );
    assert_label_keys(count, &["db", "repo"]);
    assert_value(count, "3");
    let sum = find_sample(
        &rendered,
        "shamir_function_duration_seconds_sum",
        &scope_labels,
    );
    assert_label_keys(sum, &["db", "repo"]);
    assert_value(sum, "0.0035");
    for (le, expected) in [("0.001000", "1"), ("0.002000", "2"), ("+Inf", "3")] {
        let labels = [scope_labels[0], scope_labels[1], &format!("le=\"{le}\"")];
        let bucket = find_sample(
            &rendered,
            "shamir_function_duration_seconds_bucket",
            &labels,
        );
        assert_label_keys(bucket, &["db", "repo", "le"]);
        assert_value(bucket, expected);
    }

    let lag_versions = find_sample(&rendered, "shamir_replication_lag_versions", &scope_labels);
    assert_label_keys(lag_versions, &["db", "repo"]);
    assert_value(lag_versions, "7");
    let lag_seconds = find_sample(&rendered, "shamir_replication_lag_seconds", &scope_labels);
    assert_label_keys(lag_seconds, &["db", "repo"]);
    let age = lag_seconds
        .split_whitespace()
        .last()
        .expect("lag-seconds value")
        .parse::<f64>()
        .expect("numeric lag-seconds value");
    assert!(
        age.is_finite() && age >= 0.0,
        "unexpected receipt age: {age}"
    );
    let reconnects = find_sample(
        &rendered,
        "shamir_replication_reconnects_total",
        &scope_labels,
    );
    assert_label_keys(reconnects, &["db", "repo"]);
    assert_value(reconnects, "3");
    for state in STREAM_STATES {
        let labels = [
            scope_labels[0],
            scope_labels[1],
            &format!("state=\"{}\"", state.label()),
        ];
        let line = find_sample(&rendered, "shamir_replication_stream_state", &labels);
        assert_label_keys(line, &["db", "repo", "state"]);
        assert_value(
            line,
            if state.code() == StreamState::Streaming.code() {
                "1"
            } else {
                "0"
            },
        );
    }
}

#[tokio::test]
async fn metrics_handler_samples_replication_without_a_database() {
    install_fixture_source();
    let recorder = PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let state = AppState {
        state: ObservabilityState::new(),
        prom: Some(handle),
        db: None,
        byte_budget: None,
    };
    let response = metrics::with_local_recorder(&recorder, || {
        register_functions_metrics();
        register_replication_metrics();
        poll_ready_once(metrics_handler(State(state)))
    });
    assert_eq!(response.status(), axum::http::StatusCode::OK);
    let body = to_bytes(response.into_body(), 2_000_000)
        .await
        .expect("metrics response body");
    let rendered = String::from_utf8(body.to_vec()).expect("Prometheus text is UTF-8");
    let scope_labels = ["db=\"export_test_db\"", "repo=\"replica\""];
    assert_value(
        find_sample(&rendered, "shamir_replication_lag_versions", &scope_labels),
        "7",
    );
    assert_value(
        find_sample(
            &rendered,
            "shamir_replication_reconnects_total",
            &scope_labels,
        ),
        "3",
    );
    let aggregate_fuel = find_sample(
        &rendered,
        "shamir_function_fuel_consumed_total",
        &["db=\"(other)\"", "repo=\"(other)\""],
    );
    assert_label_keys(aggregate_fuel, &["db", "repo"]);
}

#[test]
fn torn_function_duration_does_not_hide_fuel_or_depth() {
    let recorder = PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let mut function = call_metrics_snapshot()
        .into_iter()
        .next()
        .expect("function registry includes its aggregate snapshot");
    function.db = DB.to_string();
    function.repo = REPO.to_string();
    function.duration.bounds = DURATION_BOUNDS;
    function.duration.buckets = vec![2, 3, 4];
    function.duration.count = 9;
    function.duration.sum = 10_000_000;
    function.duration.torn = true;
    function.fuel_total = 13;
    function.depth_high_water = 5;

    metrics::with_local_recorder(&recorder, || {
        record_function_snapshot(&function);
    });
    let rendered = handle.render();
    let scope_labels = ["db=\"export_test_db\"", "repo=\"replica\""];
    assert_value(
        find_sample(
            &rendered,
            "shamir_function_fuel_consumed_total",
            &scope_labels,
        ),
        "13",
    );
    assert_value(
        find_sample(
            &rendered,
            "shamir_function_call_depth_high_water",
            &scope_labels,
        ),
        "5",
    );
    assert!(
        !rendered.contains("shamir_function_duration_seconds_bucket{"),
        "torn duration buckets must be omitted: {rendered}"
    );
    assert!(
        !rendered.contains("shamir_function_duration_seconds_count{"),
        "torn duration count must be omitted: {rendered}"
    );
    assert!(
        !rendered.contains("shamir_function_duration_seconds_sum{"),
        "torn duration sum must be omitted: {rendered}"
    );
}

#[test]
fn replication_export_renders_latest_snapshot_values() {
    let recorder = PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let labels = ["db=\"(other)\"", "repo=\"(other)\""];

    metrics::with_local_recorder(&recorder, || {
        register_replication_metrics();
        record_replication_snapshot(
            ReplicationSnapshot {
                db: "(other)".to_string(),
                repo: "(other)".to_string(),
                lag_versions: 10,
                last_receipt_ns: 3_000_000_000,
                state_code: StreamState::Streaming.code(),
                reconnects: 0,
            },
            5_000_000_000,
        );
    });
    let first_render = handle.render();
    let first_lag = find_sample(&first_render, "shamir_replication_lag_versions", &labels);
    assert!(first_lag.ends_with(" 10"), "wrong lag: {first_lag}");
    let age = find_sample(&first_render, "shamir_replication_lag_seconds", &labels);
    assert!(age.ends_with(" 2"), "wrong receipt age: {age}");
    let streaming = find_sample(
        &first_render,
        "shamir_replication_stream_state",
        &[labels[0], labels[1], "state=\"streaming\""],
    );
    assert!(
        streaming.ends_with(" 1"),
        "streaming state not set: {streaming}"
    );

    metrics::with_local_recorder(&recorder, || {
        record_replication_snapshot(
            ReplicationSnapshot {
                db: "(other)".to_string(),
                repo: "(other)".to_string(),
                lag_versions: 0,
                last_receipt_ns: 0,
                state_code: StreamState::Stopped.code(),
                reconnects: 0,
            },
            5_000_000_000,
        );
    });
    let second_render = handle.render();
    let second_lag = find_sample(&second_render, "shamir_replication_lag_versions", &labels);
    assert!(
        second_lag.ends_with(" 0"),
        "stale lag retained: {second_lag}"
    );
    let second_age = find_sample(&second_render, "shamir_replication_lag_seconds", &labels);
    assert!(
        second_age.ends_with(" 0"),
        "no-receipt age is not zero: {second_age}"
    );
    let stopped = find_sample(
        &second_render,
        "shamir_replication_stream_state",
        &[labels[0], labels[1], "state=\"stopped\""],
    );
    assert!(stopped.ends_with(" 1"), "stopped state not set: {stopped}");
    let streaming = find_sample(
        &second_render,
        "shamir_replication_stream_state",
        &[labels[0], labels[1], "state=\"streaming\""],
    );
    assert!(
        streaming.ends_with(" 0"),
        "streaming state retained: {streaming}"
    );
}

#[test]
fn replication_export_sets_lag_seconds_to_zero_without_a_receipt() {
    let recorder = PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    metrics::with_local_recorder(&recorder, || {
        register_replication_metrics();
        record_replication_snapshot(
            ReplicationSnapshot {
                db: "(other)".to_string(),
                repo: "(other)".to_string(),
                lag_versions: 0,
                last_receipt_ns: 0,
                state_code: StreamState::Stopped.code(),
                reconnects: 0,
            },
            5_000_000_000,
        );
    });
    let rendered = handle.render();
    let age = find_sample(
        &rendered,
        "shamir_replication_lag_seconds",
        &["db=\"(other)\"", "repo=\"(other)\""],
    );
    assert!(age.ends_with(" 0"), "no-receipt age is not zero: {age}");
}

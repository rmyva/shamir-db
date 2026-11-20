use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::body::to_bytes;
use axum::extract::State;
use axum::http::StatusCode;
use metrics_exporter_prometheus::PrometheusBuilder;
use tokio::time::timeout;

use crate::byte_budget::ByteBudget;
use crate::observability::{metrics_handler, AppState, LoadMetricsSnapshot, ObservabilityState};

const DEADLINE: Duration = Duration::from_secs(120);
const LOAD_SERIES: [(&str, &str); 4] = [
    ("shamir_connections_active", "gauge"),
    ("shamir_connections_limit", "gauge"),
    ("shamir_connections_rejected_total", "counter"),
    ("shamir_post_auth_rate_limit_rejected_total", "counter"),
];

async fn scrape(state: &AppState) -> String {
    let response = timeout(DEADLINE, metrics_handler(State(state.clone())))
        .await
        .expect("metrics handler deadline");
    assert_eq!(response.status(), StatusCode::OK);
    let body = timeout(DEADLINE, to_bytes(response.into_body(), 2_000_000))
        .await
        .expect("metrics body deadline")
        .expect("metrics response body");
    String::from_utf8(body.to_vec()).expect("Prometheus text is UTF-8")
}

fn assert_series(body: &str, name: &str, kind: &str, value: impl std::fmt::Display) {
    let help = format!("# HELP {name} ");
    let descriptor = format!("# TYPE {name} {kind}");
    let sample = format!("{name} {value}");
    let lines: Vec<_> = body.lines().filter(|line| line.contains(name)).collect();
    assert_eq!(lines.len(), 3, "unexpected lines for {name}: {lines:?}");
    assert!(lines[0].starts_with(&help));
    assert_eq!(lines[1], descriptor);
    assert_eq!(lines[2], sample);
}

fn assert_budget(body: &str, used: usize, cap: i32) {
    assert_series(body, "shamir_inflight_response_bytes_used", "gauge", used);
    assert_series(body, "shamir_inflight_response_bytes_cap", "gauge", cap);
    assert_series(body, "shamir_response_budget_waiters", "gauge", 0);
}

#[tokio::test]
async fn scrape_samples_only_bound_load_and_live_budget() {
    let recorder = PrometheusBuilder::new().build_recorder();
    let mut app = AppState {
        state: ObservabilityState::new(),
        prom: Some(recorder.handle()),
        db: None,
        byte_budget: None,
    };
    let body = scrape(&app).await;
    for (name, _) in LOAD_SERIES {
        assert!(!body.contains(name), "unbound series {name} appeared");
    }
    assert_budget(&body, 0, -1);

    app.byte_budget = Some(ByteBudget::unbounded());
    assert_budget(&scrape(&app).await, 0, -1);

    let calls = Arc::new(AtomicUsize::new(0));
    let source_calls = Arc::clone(&calls);
    app.state.set_load_source(Arc::new(move || {
        if source_calls.fetch_add(1, Ordering::Relaxed) == 0 {
            LoadMetricsSnapshot::default()
        } else {
            LoadMetricsSnapshot {
                connections_active: 2,
                connections_limit: 5,
                connections_rejected_total: u64::MAX,
                post_auth_rejected_total: (1_u64 << 53) + 1,
            }
        }
    }));
    let budget = ByteBudget::new(Some(64));
    app.byte_budget = Some(budget.clone());
    let body = scrape(&app).await;
    for (name, kind) in LOAD_SERIES {
        assert_series(&body, name, kind, 0);
    }
    assert_budget(&body, 0, 64);
    assert_eq!(calls.load(Ordering::Relaxed), 1);

    let guard = timeout(DEADLINE, budget.acquire(37))
        .await
        .expect("reserve response bytes by deadline");
    let body = scrape(&app).await;
    let values = [2, 5, u64::MAX, (1_u64 << 53) + 1];
    for ((name, kind), value) in LOAD_SERIES.into_iter().zip(values) {
        assert_series(&body, name, kind, value);
    }
    assert_budget(&body, 37, 64);
    assert_eq!(calls.load(Ordering::Relaxed), 2);

    drop(guard);
    assert_budget(&scrape(&app).await, 0, 64);
    assert_eq!(calls.load(Ordering::Relaxed), 3);
}

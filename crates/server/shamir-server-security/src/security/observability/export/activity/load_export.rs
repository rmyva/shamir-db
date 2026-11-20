use std::fmt::{self, Display, Write};
use std::sync::Arc;

use crate::byte_budget::ByteBudget;

/// Load measurements from one server boot, sampled on each `/metrics` scrape.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LoadMetricsSnapshot {
    /// Connections currently holding an admission slot.
    pub connections_active: usize,
    /// Configured connection limit; zero means unlimited.
    pub connections_limit: usize,
    /// Connection admissions refused at the limit since boot.
    pub connections_rejected_total: u64,
    /// Native, REST and WebSocket requests refused by post-auth rate limiting since boot.
    pub post_auth_rejected_total: u64,
}

pub(crate) struct LoadSource(Arc<dyn Fn() -> LoadMetricsSnapshot + Send + Sync>);

impl fmt::Debug for LoadSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("LoadSource").field(&"<dyn Fn>").finish()
    }
}

impl LoadSource {
    pub(crate) fn new(source: Arc<dyn Fn() -> LoadMetricsSnapshot + Send + Sync>) -> Self {
        Self(source)
    }

    pub(crate) fn sample(&self) -> LoadMetricsSnapshot {
        (self.0)()
    }
}

pub(crate) fn render_load_series(
    out: &mut String,
    snapshot: Option<LoadMetricsSnapshot>,
    budget: Option<&ByteBudget>,
) {
    if let Some(snapshot) = snapshot {
        render_sample(
            out,
            "shamir_connections_active",
            "Connections currently holding an admission slot",
            "gauge",
            snapshot.connections_active,
        );
        render_sample(
            out,
            "shamir_connections_limit",
            "Configured connection limit (0 means unlimited)",
            "gauge",
            snapshot.connections_limit,
        );
        render_sample(
            out,
            "shamir_connections_rejected_total",
            "Connections refused at the connection limit since boot",
            "counter",
            snapshot.connections_rejected_total,
        );
        render_sample(
            out,
            "shamir_post_auth_rate_limit_rejected_total",
            "Native, REST and WebSocket requests refused by post-auth rate limiting since boot",
            "counter",
            snapshot.post_auth_rejected_total,
        );
    }

    render_sample(
        out,
        "shamir_inflight_response_bytes_used",
        "Bytes currently reserved against the in-flight response byte budget",
        "gauge",
        budget.map_or(0, ByteBudget::used),
    );
    let cap = budget.and_then(ByteBudget::cap);
    let cap_value: &dyn Display = match &cap {
        Some(cap) => cap,
        None => &-1,
    };
    render_sample(
        out,
        "shamir_inflight_response_bytes_cap",
        "Configured in-flight response byte budget in bytes (-1 means unbounded or absent)",
        "gauge",
        cap_value,
    );
    render_sample(
        out,
        "shamir_response_budget_waiters",
        "Response byte budget acquisitions currently on the contended path",
        "gauge",
        budget.map_or(0, ByteBudget::waiters),
    );
}

fn render_sample(out: &mut String, name: &str, help: &str, kind: &str, value: impl Display) {
    writeln!(
        out,
        "# HELP {name} {help}\n# TYPE {name} {kind}\n{name} {value}"
    )
    .expect("writing to a String cannot fail");
}

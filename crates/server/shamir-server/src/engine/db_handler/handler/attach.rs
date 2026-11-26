use std::sync::Arc;

use super::super::config::SlowQueryConfig;
use super::core::ShamirDbHandler;
use super::export_activity::ExportActivityProjection;
use crate::boot::config::diagnostics::logical::export_online::OnlineExport;
use crate::security::tokens::TokenService;
use shamir_db::engine::db_instance::logical_export::MaintenanceGate;

/// Boot metadata captured at server launch (see `with_operator_boot`).
/// `pub(in crate::engine::db_handler)` on purpose (= `pub(in crate::engine::db_handler)`): only
/// the launcher attaches it and only the `operator_state` handler reads
/// it — no wider surface is needed (admin-shell PR3 review: minimal
/// visibility).
#[derive(Clone)]
pub(in crate::engine::db_handler) struct OperatorBootContext {
    /// `Instant::now()` at the very start of `launch_inner`.
    pub(in crate::engine::db_handler) started_at: std::time::Instant,
    /// Format epoch observed in the data directory at boot.
    pub(in crate::engine::db_handler) observed_format_epoch: u64,
    /// What the format-stamp check did (created/adopted/unchanged/...).
    pub(in crate::engine::db_handler) format_stamp: shamir_sync::format_stamp::StampOutcome,
    /// Configured replication node id, if any.
    pub(in crate::engine::db_handler) node_id: Option<String>,
}

impl ShamirDbHandler {
    pub(crate) fn with_online_export(mut self, service: Arc<OnlineExport>) -> Self {
        // Project the live export job into this handler's activity view
        // (same table, same scope/privacy rules as every other job). A
        // second attach attempt cannot overwrite the once-set projection.
        service.set_activity_projection(std::sync::Arc::new(ExportActivityProjection::new(
            std::sync::Arc::clone(&self.activity_requests),
        )));
        self.online_export = Some(service);
        self
    }

    /// R3-2b — install the shared server-wide maintenance gate. The gate
    /// is `Clone` and internally `Arc`-backed, so the boot path builds ONE
    /// instance and hands clones to the handler, the future online-export
    /// job and any other enrolled mutation entry point; a clone made from
    /// an already-built handler (the handler derives `Clone`) shares the
    /// same gate state. Until this is called (tests, minimal boots) the
    /// handler runs unenrolled: admission fast-paths and behaviour is
    /// byte-identical to the pre-gate code.
    pub(crate) fn with_maintenance_gate(mut self, gate: MaintenanceGate) -> Self {
        self.maintenance = Some(gate);
        self
    }

    /// Attach operator boot metadata (launch instant, observed format
    /// epoch, stamp outcome, replication node id). Called once from the
    /// server boot path; `pub(crate)` so only the launcher can attach it.
    pub(crate) fn with_operator_boot(
        mut self,
        started_at: std::time::Instant,
        observed_format_epoch: u64,
        format_stamp: shamir_sync::format_stamp::StampOutcome,
        node_id: Option<String>,
    ) -> Self {
        self.operator_boot = Some(OperatorBootContext {
            started_at,
            observed_format_epoch,
            format_stamp,
            node_id,
        });
        self
    }

    /// Attach the shared token service and configure self-service creation.
    pub fn with_token_service(
        mut self,
        service: Arc<TokenService>,
        allow_self_service: bool,
    ) -> Self {
        self.token_service = Some(service);
        self.allow_self_service = allow_self_service;
        self
    }

    /// Set the slow-query threshold (use [`SlowQueryConfig::from_ms`]).
    /// Returns `self` so the call can be chained after `with_admin`.
    pub fn with_slow_query(mut self, slow_query: SlowQueryConfig) -> Self {
        self.slow_query = slow_query;
        self
    }

    /// Test-only: pin the `elapsed_us` the slow-query decision sees, so a
    /// WARN assertion never depends on how long the real read happened to
    /// take (see [`Self::slow_query_elapsed`]).
    #[cfg(any(test, feature = "test-util"))]
    pub fn with_slow_query_elapsed(mut self, elapsed_us: u64) -> Self {
        self.slow_query_elapsed_override = Some(elapsed_us);
        self
    }

    /// The `elapsed_us` every slow-query decision compares and logs: the
    /// pinned test seam when present, the measured value otherwise. The
    /// `#[cfg]` lives here alone so production has no seam surface.
    pub(in crate::engine::db_handler) fn slow_query_elapsed(&self, measured: u64) -> u64 {
        #[cfg(any(test, feature = "test-util"))]
        if let Some(pinned) = self.slow_query_elapsed_override {
            return pinned;
        }
        measured
    }
}

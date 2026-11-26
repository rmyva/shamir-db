//! Activity-view projection of a live online-export job (task #2021,
//! item 6b).
//!
//! The online-export `start` reply returns as soon as the blocking worker
//! is spawned, so the job's whole execution is invisible to the activity
//! view's `Requests` section — unlike every other long-running job, which
//! is a row in `ActivityRequests` for its whole lifetime (D1: no new
//! source of truth). This module projects the admitted job onto the SAME
//! table with the SAME scope and privacy rules: a static op-kind name
//! (`admin:start_export`, the `BatchOp::StartExport` discriminant
//! summary), the owner's minted session handle and directory user id, the
//! job's database, and the monotonic start stamp. `Own`/`All` scope
//! filtering, the db filter, the paging worker's torn-snapshot guard and
//! the D3 privacy rules therefore apply unchanged; no export-specific
//! rendering path exists.
//!
//! The row is registered only AFTER the registry job is reserved (a
//! refused start never flashes a row) and removed when the job's worker
//! exits — the RAII guard lives inside the job, so a panic or a dropped
//! runner still removes the row (the `InFlightRequest` `Drop`).
//!
//! Cancellation stays on `CancelExport` (the session-bound job token);
//! the row is registered NOT cancellable, so `CancelRequest` refuses it
//! instead of firing a `CancelHandle` nothing observes.
//!
//! Id space: the projection mints ids from its own counter in the
//! RESERVED high half `[BASE, u64::MAX]` (`BASE = u64::MAX / 2 + 1`),
//! disjoint from the handler's batch range `[1, BASE)`; BOTH mints are
//! overflow-CHECKED against their exclusive bound (see
//! [`next_batch_exec_id`] and [`mint_projection_exec_id`]), so neither
//! counter can ever enter the other's range and collide on the
//! in-flight table's key.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use shamir_connect::server::Session;
use shamir_query_types::admin::activity::RequestExecId;

use super::super::activity::{ActivityRequests, InFlightRequest, RequestEntry};

/// First id the projection mints: one past `u64::MAX / 2`. The handler's
/// batch range is `[1, BASE)`; the projection's is `[BASE, u64::MAX]`.
/// The disjointness is enforced by the checked mints, never by
/// convention.
const PROJECTION_ID_BASE: u64 = u64::MAX / 2 + 1;

/// Static op-kind name of the projected job row (the same fixed string
/// `BatchOp::StartExport` carries in its own in-flight row during
/// `start`).
const EXPORT_JOB_OP_KIND: &str = "admin:start_export";

/// Process-wide projection id sequence (see `PROJECTION_ID_BASE`).
static PROJECTION_SEQUENCE: AtomicU64 = AtomicU64::new(PROJECTION_ID_BASE);

/// Checked mint from the handler's batch range `[1, PROJECTION_ID_BASE)`,
/// bounded by the same constant as the projection's range: `None` at
/// exhaustion — the batch counter can NEVER enter the projection's
/// reserved high half, so a batch id can never collide with a projected
/// export-job row (mutant: an unchecked `fetch_add` mint).
pub(in crate::engine::db_handler) fn next_batch_exec_id(counter: &AtomicU64) -> Option<u64> {
    loop {
        let current = counter.load(Ordering::Relaxed);
        if current >= PROJECTION_ID_BASE {
            return None;
        }
        if counter
            .compare_exchange_weak(current, current + 1, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            return Some(current);
        }
    }
}

/// Checked mint from the projection's own high-half range: `None` at
/// exhaustion (an unreachable ~u63 live rows; the row is then skipped —
/// the projection is best-effort telemetry, never admission).
fn mint_projection_exec_id() -> Option<u64> {
    loop {
        let current = PROJECTION_SEQUENCE.load(Ordering::Relaxed);
        if current == u64::MAX {
            return None;
        }
        if PROJECTION_SEQUENCE
            .compare_exchange_weak(current, current + 1, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            return Some(current);
        }
    }
}

/// Registration/removal handle for online-export job rows. One per
/// server, built from the handler's shared in-flight table and handed to
/// [`crate::boot::config::diagnostics::logical::export_online::OnlineExport`]
/// at attach time; `None`/absent (minimal boots, tests) means no
/// projection.
pub(crate) struct ExportActivityProjection {
    table: Arc<ActivityRequests>,
}

/// RAII owner of one projected job row: dropped by the job's blocking
/// worker when it exits, removing the row exactly once (the wrapped
/// guard's `Drop`).
pub(crate) struct ExportJobActivityGuard(InFlightRequest);

impl Drop for ExportJobActivityGuard {
    fn drop(&mut self) {
        // Terminal bookkeeping: claim completion (a no-op when already
        // finished) before the wrapped guard's own Drop removes the row
        // exactly once. The row is removed on EVERY worker exit path —
        // normal terminal record, panic or dropped runner.
        self.0.finish();
    }
}

impl ExportActivityProjection {
    /// Bind the projection to the handler's shared in-flight table.
    /// Reach restricted to the db_handler subtree: `ActivityRequests`
    /// itself is not nameable crate-wide.
    pub(in crate::engine::db_handler) fn new(table: Arc<ActivityRequests>) -> Self {
        Self { table }
    }

    /// Publish the job's row. Called after the registry reservation
    /// succeeded and before the worker is spawned; the returned guard
    /// MUST be moved into the job so the row dies with it. `None` when no
    /// id could be minted (projection id exhaustion — the best-effort
    /// projection skips the row; the job itself is unaffected).
    pub(crate) fn register_job(
        &self,
        session: &Session,
        db: String,
    ) -> Option<ExportJobActivityGuard> {
        let exec_id = RequestExecId(mint_projection_exec_id()?);
        let mut entry = RequestEntry::new(exec_id, session, db, EXPORT_JOB_OP_KIND, None);
        // Cancellation is the session-bound CancelExport op, not the
        // generic CancelRequest worker (whose CancelHandle no one
        // observes here) — mark the row not cancellable.
        entry.cancellable = false;
        // No per-session slot: the job does not occupy a request slot for
        // its lifetime (the `start` batch already released its own); the
        // row is a projection, not an admitted request.
        Some(ExportJobActivityGuard(
            self.table.register_with_slot(entry, None),
        ))
    }
}

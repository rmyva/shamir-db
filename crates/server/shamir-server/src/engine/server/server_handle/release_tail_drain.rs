//! Bounded drain of the interactive-tx record-lock release tails (the
//! operator-cancel release and the registry `Drop` release): these releases
//! are IN-MEMORY registrations and detached release tasks, not store writes
//! at drain time — the drain waits for the release TAILS (the detached
//! `spawn_abort_release` tasks writing into a table's `MvccStore`) so the
//! repository flush cannot overlap one.

use shamir_db::ShutdownReport;
use shamir_sync::durability::tx::ReleaseTails;

/// Each call site bounds the wait with ITS stage deadline (the early
/// cutoff / outer deadline, like every other stage in `core.rs`), never with
/// a fresh clock started inside the drain: a fresh clock can be pre-empted by
/// the outer deadline before the drain records its own error.
///
/// With `close_admission` (the LAST drain on the path), the owner
/// also stops admitting new tails so the wait can settle; earlier drains keep
/// admission open — on the graceful path there is exactly ONE drain, called
/// from `release_source_stop` AFTER every release source (reaper, registry)
/// is stopped, so closing admission there cannot refuse a registered
/// release. `context` names the drain's shutdown stage in the report entry
/// on expiry.
pub(in crate::engine::server) async fn drain_release_tails(
    tails: Option<&ReleaseTails>,
    close_admission: bool,
    deadline: tokio::time::Instant,
    budget: std::time::Duration,
    context: &str,
    report: &mut ShutdownReport,
) -> bool {
    let Some(tails) = tails else {
        return true;
    };
    if close_admission {
        tails.close();
    }
    if tails.drain(deadline).await {
        return true;
    }
    report.add_error(format!(
        "interactive transaction lock-release tails still running after \
         {budget:?} ({context})"
    ));
    false
}

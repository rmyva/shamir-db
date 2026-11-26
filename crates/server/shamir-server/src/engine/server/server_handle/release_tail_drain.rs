//! Bounded drain of the interactive-tx record-lock release tails (the
//! operator-cancel release and the registry `Drop` release): shutdown must
//! not flush or release the stores while one is still writing into a table's
//! `MvccStore`.

use shamir_db::ShutdownReport;
use shamir_sync::durability::tx::ReleaseTails;

/// Each call site bounds the wait with ITS stage deadline (the early
/// cutoff / outer deadline, like every other stage in `core.rs`), never with
/// a fresh clock started inside the drain: a fresh clock can be pre-empted by
/// the outer deadline before the drain records its own error.
///
/// With `close_admission` (the LAST drain on the path), the owner
/// also stops admitting new tails so the wait can settle; earlier drains keep
/// admission open — the reaper teardown still spawns tails after the flush,
/// and they are drained once the reapers have joined. `context` names the
/// drain's shutdown stage in the report entry on expiry.
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

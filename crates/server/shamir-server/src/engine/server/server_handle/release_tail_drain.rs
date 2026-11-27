//! Bounded drain of the interactive-tx record-lock release tails (the
//! operator-cancel release and the registry `Drop` release): these releases
//! settle IN-MEMORY record locks — the detached `spawn_abort_release` tasks
//! they enqueue never write into the store at drain time. The drain waits for
//! those detached tasks so the repository flush cannot overlap one.

use shamir_db::{ShutdownReport, ShutdownWaitStage};
use shamir_sync::durability::tx::ReleaseTails;

use super::shutdown_owner::Settlement;

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
/// on expiry. The return value is the typed settlement: the drain settled is
/// PROOF; an expiry is NOT (the caller keeps the lock).
pub(in crate::engine::server) async fn drain_release_tails(
    tails: Option<&ReleaseTails>,
    close_admission: bool,
    deadline: tokio::time::Instant,
    budget: std::time::Duration,
    context: &str,
    report: &mut ShutdownReport,
) -> Settlement {
    let Some(tails) = tails else {
        return Settlement::Settled;
    };
    if close_admission {
        tails.close();
    }
    if tails.drain(deadline).await {
        return Settlement::Settled;
    }
    report.add_error(format!(
        "interactive transaction lock-release tails still running after \
         {budget:?} ({context})"
    ));
    Settlement::Unsettled(ShutdownWaitStage::ReleaseTailDrain)
}

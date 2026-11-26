//! Release of a parked interactive transaction's pessimistic record locks.
//!
//! [`shamir_tx::TxContext`] has no `Drop` that frees them: Level-3 record
//! locks live in the per-table `MvccStore` and only an explicit release call
//! clears them. Every path that ends a parked interactive transaction outside
//! the commit pipeline therefore hands its leftover context to
//! [`super::tx_registry::InteractiveTx`]'s `Drop`, which calls this once the
//! LAST reference to the transaction is gone — the owner's rollback, the
//! idle/deadline reaper, session teardown, the `TxExecute` execution-limit
//! abort arms, and a request that loses its race with any of them. Nothing has
//! to remember it.

use super::release_tails::ReleaseTails;
use super::tx_registry::TxAbortRelease;
use shamir_tx::TxContext;

/// Spawn the registered release for a transaction that ended without going
/// through the commit pipeline.
///
/// Called from `InteractiveTx::drop`, on the last reference to a context
/// that may still hold Level-3 record locks. `Drop` cannot await, so the
/// release runs as a task on the CURRENT runtime: it is short (one
/// `release_locks` walk over this transaction's own keys) and holds nothing
/// but its own arguments, so it cannot stall a reaper tick, a session
/// teardown or shutdown. When the transaction was registered with the
/// registry's [`ReleaseTails`] owner (`tails`), the task is tracked there
/// and server shutdown drains it before the repository flush; otherwise it
/// stays fully detached (historical behaviour). A still-pending task is
/// dropped with the runtime, and a transaction being dropped at that moment
/// has no waiter the process will honour anyway. Outside a live runtime (a
/// drop after the runtime shut down, or on a runtime that is already tearing
/// down) nothing can drive an async release: the context drops as-is and
/// this transaction's record locks are NOT released — the case is logged at
/// `warn!` with the transaction's database and repository names; it never
/// panics.
pub(in crate::sync::durability) fn spawn_abort_release(
    release: &TxAbortRelease,
    tails: Option<&ReleaseTails>,
    ctx: TxContext,
    db: &str,
    repo: &str,
) {
    if ctx.locked_keys.is_empty() {
        // Snapshot / Serializable transactions never take a record lock.
        return;
    }
    let Ok(runtime) = tokio::runtime::Handle::try_current() else {
        tracing::warn!(
            db,
            repo,
            "interactive tx ended outside a tokio runtime: its record locks go with the process"
        );
        return;
    };
    let work = release(ctx);
    match tails {
        Some(tails) => match tails.spawn_lossless(work) {
            Ok(handle) => {
                // Panic/cancel observability for the tracked task.
                tokio::spawn(async move {
                    if let Err(error) = handle.await {
                        if !error.is_cancelled() {
                            tracing::warn!(%error, "interactive tx record-lock release task failed");
                        }
                    }
                });
            }
            Err(work) => {
                // Admission closed by shutdown. `Drop` cannot await, so the release
                // can NOT run inline here; running it detached (the historical
                // behaviour) is deliberately preferred over dropping it: a lost
                // release leaves Level-3 record locks held for the process's whole
                // remaining lifetime, while a detached tail can only, in the worst
                // case, overlap the shutdown flush. The case is logged.
                tracing::warn!(
                    db,
                    repo,
                    "release-tail admission closed by shutdown; an interactive tx's \
                     record-lock release runs detached (untracked)"
                );
                runtime.spawn(work);
            }
        },
        None => {
            runtime.spawn(work);
        }
    }
}

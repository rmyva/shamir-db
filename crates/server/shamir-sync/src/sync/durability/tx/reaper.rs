//! The periodic interactive-tx reaper task (Phase B Stage 6), moved out of
//! `tx_registry.rs` (which was at the 1000-line cap): the task loops on the
//! sweep cadence and removes expired parked transactions; removal is the
//! RAII rollback/release path.
//!
//! Re-exported from [`super::tx_registry`] so the public
//! `..::tx_registry::{ReaperTask, spawn_reaper_task}` paths stay stable.

use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::tx_registry::TxRegistry;

/// Phase B Stage 6 — handle for the periodic interactive-tx reaper task.
/// Just the `JoinHandle<()>` — the stop signal is the shared root
/// `shutdown_token` (cancelled by `ServerHandle::shutdown` before this
/// handle is awaited), so no per-task stop channel is needed.
pub struct ReaperTask {
    pub handle: JoinHandle<()>,
}

/// Spawn the periodic interactive-tx reaper.
///
/// The task loops every `reap_interval`, calling [`TxRegistry::reap_expired`]
/// with `idle_ttl`. Removing the `Arc<InteractiveTx>` drops it; if no commit /
/// rollback path took the inner `TxContext` first, drop = RAII rollback per
/// the `TxContext` doc-comment (no storage I/O). The `SnapshotGuard` held
/// inside drops alongside, releasing MVCC GC's `min_alive` hold.
///
/// **Abort-on-disconnect (Stage 7 limitation).** `SessionStore`
/// (`shamir_connect::server::session`) has no observer hook on idle
/// eviction — there is no callback surface to inject "abort tx for this
/// session" when a TCP connection drops. The idle TTL checked by this
/// reaper is therefore the authoritative cleanup for txs orphaned by a
/// dropped connection (the §6.3 backstop). A future `SessionStore`
/// eviction-hook API could tighten this, but nothing is durable until
/// commit, so the wait is free.
pub fn spawn_reaper_task(
    registry: Arc<TxRegistry>,
    idle_ttl: Duration,
    reap_interval: Duration,
    shutdown: CancellationToken,
) -> ReaperTask {
    let handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(reap_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // Drop the immediate first tick — pointless to scan an empty registry
        // the moment we boot.
        interval.tick().await;
        loop {
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => {
                    tracing::debug!("interactive_tx_reaper: shutdown cancelled");
                    break;
                }
                _ = interval.tick() => {
                    let reaped = registry.reap_expired(idle_ttl);
                    if reaped == 0 {
                        continue;
                    }
                    tracing::info!(reaped, "interactive_tx_reaper: aborted past-deadline transactions");
                }
            }
        }
    });
    ReaperTask { handle }
}

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio_util::sync::CancellationToken;

use super::{registry::CursorRegistry, CursorReaperTask};

/// Spawn the periodic cursor reaper.
///
/// Mirrors `crate::tx_registry::spawn_reaper_task`: loops every
/// `reap_interval`, calling [`CursorRegistry::sweep_and_reap`] with
/// `idle_ttl` (dropping each reaped `Arc<Cursor>` releases the
/// `SnapshotGuard`, unpinning the MVCC GC floor). Also sweeps aged-out
/// tombstones each tick.
pub fn spawn_reaper_task(
    registry: Arc<CursorRegistry>,
    idle_ttl: Duration,
    reap_interval: Duration,
    shutdown: CancellationToken,
) -> CursorReaperTask {
    let handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(reap_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // Drop the immediate first tick — pointless to scan an empty
        // registry the moment we boot.
        interval.tick().await;
        loop {
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => {
                    tracing::debug!("cursor_reaper: shutdown cancelled");
                    break;
                }
                _ = interval.tick() => {
                    let now = Instant::now();
                    registry.sweep_tombstones(now);
                    let reaped = registry.sweep_and_reap(now, idle_ttl);
                    if reaped == 0 {
                        continue;
                    }
                    tracing::info!(reaped, "cursor_reaper: evicted idle-timeout cursors");
                }
            }
        }
    });
    CursorReaperTask { handle }
}

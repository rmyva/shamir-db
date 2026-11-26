use std::sync::{Arc, Weak};
use std::time::Duration;

use shamir_storage::error::{DbError, DbResult};
use shamir_tx::{CancelHandle, RequestCancelled, WaitState};

#[cfg(test)]
use crate::clean_horizon::checkpoint::test_seam;
use crate::commit::core::RepoCore;
use crate::materialize::interner_delta_safe_to_truncate;

use super::Drainer;

impl Drainer {
    async fn seed_startup(
        &self,
        repo: &RepoCore,
        entries: Vec<shamir_wal::WalEntryV2>,
    ) -> DbResult<()> {
        let _replay_guard = self.replaying.lock().await;
        let durable = repo.tx_gate().await?.durable_watermark();
        self.seed_from_recover(
            entries
                .into_iter()
                .filter(|entry| entry.commit_version > durable)
                .collect(),
        );
        Ok(())
    }

    /// CRIT-2 (#436): re-check every version in `pending_unsafe` against the
    /// CURRENT interner hwm (dropping any that are now confirmed safe), then
    /// compute the truncation ceiling as `min(durable_watermark,
    /// min_key(pending_unsafe) - 1)` and attempt F6b truncation up to it.
    ///
    /// Called both at the end of `drain_step`'s normal per-entry pass AND
    /// from the `dur >= vis` early-return path — the interner can catch up
    /// without any new commit (e.g. a background `InternerManager::persist`
    /// tick), so truncation must be independently re-attempted even when
    /// there is nothing new to replay; otherwise a pending-unsafe entry
    /// would wedge truncation forever once `durable_watermark` reaches
    /// `visibility`.
    ///
    /// Replay callers serialize on `replaying`; this helper is only called
    /// under that guard and never acquires it again. The ceiling is recomputed
    /// from `pending_unsafe` and the current durable watermark on every pass.
    pub(super) async fn settle_and_truncate(&self, repo: &RepoCore) -> DbResult<()> {
        // S5c-X PR5b: land repair-pending (channel-full deferred) journal
        // events BEFORE computing the truncation ceiling — an admission here
        // lifts its clamp below, and a still-pending version holds the WAL
        // segment retained until its repaired event is durably checkpointed.
        crate::clean_horizon::repair_pending_drain::drain_repair_pending(repo).await?;
        let snapshot: Vec<(u64, u64)> = {
            let guard = scc::Guard::new();
            self.pending_unsafe
                .iter(&guard)
                .map(|(k, v)| (*k, *v))
                .collect()
        };
        for (v, delta_max_id_raw) in snapshot {
            let delta_max_id = if delta_max_id_raw == 0 {
                None
            } else {
                Some(delta_max_id_raw)
            };
            if matches!(
                interner_delta_safe_to_truncate(repo, delta_max_id).await,
                Ok(true)
            ) {
                log::debug!(
                    "drain_step: pending-unsafe commit_version {v} is now A5-safe; \
                     removing from the pending set"
                );
                let _ = self.pending_unsafe.remove_sync(&v);
            }
            // Ok(false)/Err: leave it in `pending_unsafe` — still bounds the
            // ceiling below `v`.
        }

        let gate = repo.tx_gate().await?;
        let durable = gate.durable_watermark();
        let min_pending: Option<u64> = {
            let guard = scc::Guard::new();
            self.pending_unsafe.iter(&guard).next().map(|(k, _)| *k)
        };
        let ceiling = match min_pending {
            Some(min_v) => min_v.saturating_sub(1).min(durable),
            None => durable,
        };

        // S5c-X PR5b (I2): never let the J-checkpoint or `truncate_below`
        // cross a repair-pending version — the WAL segment holding the still
        // deferred event stays retained until the repaired event is durably
        // checkpointed (`checkpoint_before_truncate` is FIFO after any
        // admitted repaired event); only the test seam lifts this clamp.
        let ceiling = match repo.changefeed().await?.feed.repair_ceiling_clamp() {
            Some(min_repair) => ceiling.min(min_repair.saturating_sub(1)),
            None => ceiling,
        };

        // F6b — every record with `commit_version <= ceiling` is BOTH
        // durable in `history` AND has its interner delta durably persisted
        // (or has no delta at all), so the sealed WAL segments holding only
        // such records are reclaimable. `has_truncatable` is CHEAP (a
        // lock-held scan of the short sealed list, no I/O) and is USUALLY
        // false: segments are large (`WAL_SEGMENT_MAX_BYTES`), so a sealed
        // segment crosses the ceiling only at a segment boundary, not on
        // every drain pass.
        //
        // ORDER (I1/I2): history-flush BEFORE truncate — `mark_durable`'s
        // write may still sit in the page cache. Before unlinking a sealed
        // segment we `fsync` `history` (narrow seam — `flush_all_history`,
        // NOT `flush_buffers`, which would re-enter `drain_all` and
        // recurse) so a power-loss after the unlink cannot lose the data
        // (I2). Then delete the segments.
        let settled = gate.completion().watermark();
        let checkpoint_ceiling = ceiling.min(settled);
        let wal = repo.repo_wal().await?;
        let _retention_action =
            crate::clean_horizon::retention::enforce_retention(repo, checkpoint_ceiling).await?;
        if wal.has_truncatable(checkpoint_ceiling) {
            // PR2 S5c-X crash seam: BEFORE the clean-horizon checkpoint. A
            // HARD crash here leaves every sealed segment on disk — nothing
            // checkpointed, nothing unlinked.
            crate::commit::maybe_crash("pre_checkpoint", repo).await;
            // PR2 S5c-X: checkpoint the changefeed journal through the
            // ceiling BEFORE any unlink (I2). On failure the counter is
            // bumped inside; skip truncation this pass — the next pass
            // retries.
            if !crate::clean_horizon::checkpoint::checkpoint_before_truncate(repo).await {
                log::warn!(
                    "drain_step: clean-horizon checkpoint failed at ceiling {ceiling}; \
                     truncation deferred to the next pass"
                );
                return Ok(());
            }
            // PR2 S5c-X crash seam: AFTER the checkpoint, BEFORE the
            // history-flush + unlink — recovery must observe the durable J.
            crate::commit::maybe_crash("post_checkpoint", repo).await;
            // F6c crash seam: BEFORE history-flush + unlink. A HARD crash
            // here leaves EVERY sealed segment on disk (nothing unlinked
            // yet) — recovery replays all of them idempotently and loses
            // nothing (the data is also already in `history`).
            crate::commit::maybe_crash("pre_truncate", repo).await;
            repo.flush_all_history().await?;
            #[cfg(test)]
            test_seam::before_delete(repo).await;
            let removed = match repo.try_begin_truncate(checkpoint_ceiling) {
                Some(lease) => lease.truncate(Arc::clone(&wal)).await?,
                None => 0,
            };
            #[cfg(test)]
            test_seam::record(repo, "truncate");
            // Recheck actual retained bytes after unlink; clear admission refusal only below cap.
            let _retention_action =
                crate::clean_horizon::retention::enforce_retention(repo, checkpoint_ceiling)
                    .await?;
            // F6c crash seam: AFTER a successful truncate. The unlinked
            // segments are durable in `history` (flushed above, I2), so
            // recovery from the survivors is correct and complete.
            crate::commit::maybe_crash("post_truncate", repo).await;
            if removed > 0 {
                log::debug!(
                    "drain_step: truncated {removed} sealed WAL segment(s) below \
                     ceiling {ceiling} (durable {durable}, min_pending_unsafe={min_pending:?})"
                );
            }
        }
        Ok(())
    }
    /// Drain repeatedly until a pass drains nothing. Used for graceful
    /// shutdown and tests to flush the replayable inflight tail fully.
    /// Waits for an already-running replay pass, but stops at an active commit
    /// or a data-relevant replay failure; a later call retries that tail.
    /// Shutdown callers must quiesce commits first.
    ///
    /// Returns the total number of versions drained across all passes.
    pub async fn drain_all(&self, repo: &RepoCore) -> DbResult<usize> {
        self.drain_all_with_cancel(repo, None).await
    }

    /// Cooperative cancellation only while acquiring the replay lock or
    /// between passes. Never race this entire future against cancellation.
    pub(crate) async fn drain_all_with_cancel(
        &self,
        repo: &RepoCore,
        cancel: Option<&CancelHandle>,
    ) -> DbResult<usize> {
        let mut total = 0usize;
        loop {
            let pass = self.drain_step_with_cancel(repo, cancel).await;
            if cancel.is_some_and(CancelHandle::is_cancelled) {
                return Err(Self::request_cancelled());
            }
            let n = pass?;
            if n == 0 {
                break;
            }
            total += n;
        }
        Ok(total)
    }

    pub(super) async fn drain_step_with_cancel(
        &self,
        repo: &RepoCore,
        cancel: Option<&CancelHandle>,
    ) -> DbResult<usize> {
        let _replay_guard = if let Some(cancel) = cancel {
            let _park = cancel.park(WaitState::Lock);
            let guard = tokio::select! {
                biased;
                _ = cancel.wait_cancelled() => return Err(Self::request_cancelled()),
                guard = self.replaying.lock() => guard,
            };
            if cancel.is_cancelled() {
                return Err(Self::request_cancelled());
            }
            guard
        } else {
            self.replaying.lock().await
        };
        // After acquisition, replay may mutate durable state across awaits.
        // Finish this pass before observing another request cancellation.
        self.replay_locked(repo).await
    }

    fn request_cancelled() -> DbError {
        DbError::Io(std::io::Error::other(RequestCancelled))
    }

    /// Spawn the background drain loop as a leak-free task (mirrors
    /// `WalGroupCommit::spawn_background_fsync` / `MemBufferStore`'s flusher).
    ///
    /// ## Lifecycle (the hard part)
    ///
    /// `RepoInstance` and its `RepoCore` are `Clone`-of-`Arc`s (every field is
    /// `Arc`-shared; there is NO canonical `Arc<RepoInstance>` to take a `Weak`
    /// from). A task holding an owned `RepoCore` clone of the foreground repo would therefore bump every
    /// inner `Arc` and keep the repo alive forever — a leak for ephemeral
    /// repos (tests create/drop many).
    ///
    /// The fix: `repo` here is the core of a BACKGROUND clone whose `live` token
    /// is `None` (`RepoInstance::clone_for_background`), so it does NOT count toward the
    /// repo's liveness. `live` is a `Weak<()>` of the repo's shared liveness
    /// `Arc<()>`, held STRONGLY only by the real (foreground) repo clones. The
    /// loop parks on the wake/interval, then checks `live.upgrade()`: once the
    /// last foreground clone drops, the strong count hits zero, the upgrade
    /// fails, the loop breaks, and the owned background clone is dropped —
    /// releasing every inner `Arc`. No leak, no cycle (the background clone's
    /// strong ref to the `Drainer` is severed when the loop exits), no
    /// deadlock (the loop only `.await`s I/O + the notify).
    ///
    /// Driven by [`wake`](Self::wake) (commit-path notify) with an `interval`
    /// backstop so a missed wakeup still makes progress. The single-owner
    /// contract (exactly one task per repo) is the caller's responsibility
    /// (spawn once behind the repo's `OnceCell<Arc<Drainer>>`).
    pub fn spawn(self: &Arc<Self>, repo: RepoCore, live: Weak<()>, interval: Duration) {
        #[cfg(any(test, feature = "test-util"))]
        if crate::commit::TEST_POST_WAL_PRE_PUBLISH_HOOK
            .get()
            .is_some_and(|hook| hook.repo_name == repo.name())
        {
            return;
        }
        let drainer = Arc::downgrade(self);
        tokio::spawn(async move {
            // Op #2 Stage 1: seed the drainer window from WAL recovery on
            // cold start so the window contains all inflight entries before
            // the first drain_step. Best-effort — if the WAL is unreachable
            // (shouldn't happen on a healthy repo) the loop still runs and
            // drain_step falls back to wal.recover() as before.
            if let Some(this) = drainer.upgrade() {
                // Protect the startup recovery snapshot until the window owns it.
                // Release before any drain/checkpoint can attempt truncation.
                let startup_pin = match repo.pin_wal_history(0).await {
                    Ok(pin) => pin,
                    Err(error) => {
                        log::warn!("drainer spawn: startup pin failed: {error}");
                        return;
                    }
                };
                match repo.repo_wal().await {
                    Ok(wal) => match wal.recover().await {
                        Ok(entries) => {
                            if let Err(e) = this.seed_startup(&repo, entries).await {
                                log::warn!("drainer spawn: seed failed: {e}");
                            }
                        }
                        Err(e) => {
                            log::warn!(
                                "drainer spawn: wal.recover() for seed failed: {e}; \
                                 window starts empty"
                            );
                        }
                    },
                    Err(e) => {
                        log::warn!(
                            "drainer spawn: repo_wal() for seed failed: {e}; \
                             window starts empty"
                        );
                    }
                }
                drop(startup_pin);
            }
            loop {
                // Exit promptly once every foreground repo clone has dropped.
                if live.upgrade().is_none() {
                    break;
                }
                // Park on the next wake OR the interval backstop. Upgrade the
                // drainer just to reach its `notify`; drop it before the work
                // so a quiescent system can release it.
                match drainer.upgrade() {
                    Some(this) => {
                        tokio::select! {
                            _ = this.notify.notified() => {}
                            _ = tokio::time::sleep(interval) => {}
                        }
                    }
                    None => break,
                }
                // Re-check liveness after the park (the last clone may have
                // dropped while we slept) so we never drain a dead repo.
                if live.upgrade().is_none() {
                    break;
                }
                if let Some(this) = drainer.upgrade() {
                    // F-68 (#895) cluster D / task #124 diagnostic
                    // instrumentation: timestamped "tick started"/"tick
                    // finished" pair around this background loop's core
                    // work, per the brief's request to rule out (or
                    // implicate) a concurrently-running reaper/GC task
                    // during `rename_populated_survives_cold_restart`'s
                    // cold-restart cycle. NOT a behavior change — logging
                    // only, no new await point.
                    let tick_started = std::time::Instant::now();
                    log::debug!("drainer background loop: tick started");
                    if let Err(e) = this.drain_all(&repo).await {
                        log::warn!("drainer background loop: drain_all failed: {e}");
                    }
                    log::debug!(
                        "drainer background loop: tick finished after {:?}",
                        tick_started.elapsed()
                    );
                } else {
                    break;
                }
            }
        });
    }
}

#[cfg(test)]
#[path = "../../tests/drain/startup_seed_tests.rs"]
mod startup_seed_tests;

use super::repo_core::RepoCore;
use crate::drain::truncation_lease::TruncationLease;
use crate::drain::truncation_pin_guard::TruncationPinGuard;
use crate::meta::repo_identity::RepoIdentity;
use crate::repo::changelog_store::write_domain::RepoWriteDomain;
use crate::repo::durability_policy::DurabilityPolicy;
use crate::repo::journal::backend::durability_handle::retention_gate::RetentionGate;
use crate::repo::RepoWritePermit;
use shamir_storage::error::{DbError, DbResult};
use shamir_storage::types::{Repo as _, Store};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

/// Shutdown-stage timing threshold: stages slower than this are logged.
const STAGE_WARN_THRESHOLD: Duration = Duration::from_secs(1);

/// Logs `repo`/`stage` when it ran longer than `STAGE_WARN_THRESHOLD` —
/// from `Drop`, so a stage that is CANCELLED by an enclosing deadline (the
/// very stall being diagnosed) is reported too. Shutdown paths only.
/// (R2d: moved with `flush_buffers` from the `RepoInstance` shell.)
struct StageTimer<'a> {
    repo: &'a str,
    stage: &'a str,
    start: tokio::time::Instant,
    finished: bool,
}

impl Drop for StageTimer<'_> {
    fn drop(&mut self) {
        let elapsed = self.start.elapsed();
        if elapsed >= STAGE_WARN_THRESHOLD {
            let how = if self.finished {
                "took"
            } else {
                "was cancelled or still running after"
            };
            log::warn!(
                "shutdown stage slow: repo '{}' stage '{}' {} {:?}",
                self.repo,
                self.stage,
                how,
                elapsed
            );
        }
    }
}

/// Shutdown-stage timing helper shared by the tx-layer `RepoCore` commit
/// path and the instance-layer clean-shutdown cluster
/// (`clean_horizon_shutdown_impl.rs`, which imports this via
/// `crate::commit::core::timed_stage` — ENGINE-LAYERING R2d kept ONE
/// implementation here instead of duplicating it on the shell). Logs
/// `repo`/`stage` when it ran longer than `STAGE_WARN_THRESHOLD` — from
/// `Drop`, so a stage that is CANCELLED by an enclosing deadline (the very
/// stall being diagnosed) is reported too. Shutdown paths only.
pub async fn timed_stage<F: Future>(repo: &str, stage: &str, fut: F) -> F::Output {
    let mut timer = StageTimer {
        repo,
        stage,
        start: tokio::time::Instant::now(),
        finished: false,
    };
    let out = fut.await;
    timer.finished = true;
    out
}

impl RepoCore {
    /// Retain WAL versions >= floor for a later db_instance capture. Use zero
    /// to retain all currently available history. Cannot restore older deletes.
    pub async fn pin_wal_history(&self, floor: u64) -> DbResult<TruncationPinGuard> {
        self.truncation_pins.acquire(floor).await
    }

    /// Test seam: the truncation-pin cell shared by every clone of the repository.
    #[cfg(any(test, feature = "test-util"))]
    #[doc(hidden)]
    pub fn truncation_pins_for_test(&self) -> &Arc<crate::drain::truncation_pin::TruncationPin> {
        &self.truncation_pins
    }

    /// Atomically clamp deletion and admit its lease, only after all preparation.
    pub(crate) fn try_begin_truncate(&self, ceiling: u64) -> Option<TruncationLease> {
        self.truncation_pins.try_begin_truncate(ceiling)
    }
    /// S5c-X PR6b-2a: this repository's write-admission domain (one per
    /// repository instance, shared by every clone).
    pub fn write_domain(&self) -> &Arc<RepoWriteDomain> {
        &self.durability_handle.write_domain
    }

    /// cancel-safe: yes — `OnceCell::get_or_try_init` semantics. If
    /// cancellation occurs during init, the cell remains uninitialised
    /// and the next caller retries. Returned Arc clone is in-memory.
    ///
    /// Returns the per-repo WAL manager, lazily initialising it on first
    /// call. Shares the `"__tx__"` info store with `tx_gate`.
    ///
    /// The `next_txn_id` counter is seeded from
    /// `max(persisted_NextTxId, max_inflight_txn_id + 1)`. The floor is
    /// computed from `recover()` (segment/Mem replay) — empty on a fresh
    /// in-memory instance (safeguard inert), the durable entries on a disk
    /// repo. The append-only segment keys entries by frame position and
    /// recovery replays by the monotonically-floored `commit_version`, so
    /// even a repeated txn_id after a crash carries no ordering ambiguity
    /// or data loss; the floor is a belt-and-braces anti-collision guard.
    pub async fn repo_wal(&self) -> DbResult<Arc<shamir_tx::RepoWalManager>> {
        self.durability_handle
            .repo_wal
            .get_or_try_init(|| async {
                let info_store = self.repo.store_get("__tx__").await?;
                let initial_txn_id =
                    crate::meta::recovery_marker::load_next_tx_id_snapshot(&info_store)
                        .await
                        .unwrap_or(None)
                        .unwrap_or(1);
                // F5e: build a `WalGroupCommit` for EVERY repo — `File` sink
                // for disk repos (sibling `<name>.shamirwal/` directory via
                // `file_name()` — NOT `OsString::push` on the full path, which
                // breaks on trailing separators), `Mem` sink (in-RAM Vec) for
                // in-memory repos. The group is always present, so the live
                // commit path is one code path: `begin_grouped(Buffered)` to
                // append, `recover()` (segment/Mem `replay()`) to recover.
                // Only disk repos get a background fsync — the Mem sink's
                // `sync` is a no-op, so a timer would be pointless.
                let group = match &self.durability_handle.wal_dir {
                    Some(dir) => {
                        let wal_dir = match dir.file_name() {
                            Some(name) => {
                                let mut n = name.to_os_string();
                                n.push(".shamirwal");
                                dir.with_file_name(n)
                            }
                            None => dir.join("shamirwal"),
                        };
                        let wal_dir_for_blocking = wal_dir.clone();
                        tokio::task::spawn_blocking(move || {
                            std::fs::create_dir_all(&wal_dir_for_blocking)
                        })
                        .await
                        .map_err(|e| DbError::Storage(format!("wal dir join: {e}")))?
                        .map_err(|e| DbError::Storage(format!("wal dir create: {e}")))?;
                        // F6b: the WAL is a DIRECTORY of numbered segments
                        // (`NNNNNNNN.wal`), not a single `repo.wal`. The
                        // project is pre-release with an atomic flip (no
                        // dual-recovery bridge — `wal-refactor.md` §4.4), so
                        // there is no legacy `repo.wal` migration: `SegmentSet`
                        // simply owns `<name>.shamirwal/` and rotates/truncates
                        // segments inside it. `create_dir_all` above already
                        // made the dir; `SegmentSet::open` opens the first
                        // active `00000000.wal`.
                        // F6c: the seal/rotate threshold is the
                        // `WAL_SEGMENT_MAX_BYTES` tunable (8 MiB), overridable
                        // per-process via `SHAMIR_WAL_SEGMENT_MAX_BYTES` (parsed
                        // as `u64`; a malformed value falls back to the const).
                        // This is a legitimate ops tunable (a small cap forces
                        // rotation+seal so truncation/growth-limit tests can
                        // reach segment boundaries without huge data volumes),
                        // NOT a debug-only seam — left live in every build, but
                        // Captured at construction so validation and rotation
                        // cannot observe different environment values.
                        let seg_max_bytes = self.wal_segment_bytes;
                        let segset =
                            shamir_wal::SegmentSet::open(wal_dir.clone(), seg_max_bytes).await?;
                        let sink = shamir_wal::WalSink::File(segset);
                        let group = Arc::new(shamir_wal::WalGroupCommit::new(Arc::new(sink)));
                        crate::repo::durability_policy::install_empty_coverage(&group)?;
                        // RF1: 250 ms is the nominal background fsync timer,
                        // not a hard power-loss bound; I/O errors and scheduling
                        // delays can extend the unsynced window.
                        const WAL_BG_FSYNC_MS: u64 = 250;
                        group.spawn_background_fsync(std::time::Duration::from_millis(
                            WAL_BG_FSYNC_MS,
                        ));
                        group
                    }
                    None => {
                        // Successful OnceCell initialization logs once, never per commit.
                        log::warn!(
                            "repo '{}': Mem WAL sink is not durable — wal_commit policy tier is ignored for this in-memory/hybrid backend",
                            self.name
                        );
                        let sink = shamir_wal::WalSink::mem();
                        let group = Arc::new(shamir_wal::WalGroupCommit::new(Arc::new(sink)));
                        crate::repo::durability_policy::install_empty_coverage(&group)?;
                        group
                    }
                };
                let mgr = shamir_tx::RepoWalManager::new(initial_txn_id, Arc::clone(&group));
                // Publish the group for the replication read path's R bound.
                let _ = self.durability_handle.wal_group.set(group);

                // Anti-collision safeguard: floor next_txn_id above any
                // inflight txn_id replayed from the WAL, so a fresh
                // `fresh_txn_id` can never reuse an id a crashed entry already
                // used. On a fresh Mem instance `recover()` is empty → inert;
                // on a disk repo it returns the durable entries.
                if let Ok(entries) = mgr.recover().await {
                    if let Some(max_txn_id) = entries.iter().map(|e| e.txn_id).max() {
                        mgr.seed_floor_at_least(max_txn_id + 1);
                    }
                }

                Ok::<Arc<shamir_tx::RepoWalManager>, DbError>(Arc::new(mgr))
            })
            .await
            .cloned()
    }

    /// The repo's `WalGroupCommit` (captured when `repo_wal` initializes).
    /// Used by the replication read path to sample `R` and coalesce syncs.
    /// Also exposes the recoverability watermark to embedded callers.
    pub async fn wal_group(&self) -> DbResult<Arc<shamir_wal::WalGroupCommit>> {
        self.repo_wal().await?;
        self.durability_handle
            .wal_group
            .get()
            .cloned()
            .ok_or_else(|| DbError::Internal("wal group not initialized".to_string()))
    }

    pub(crate) async fn settle_unwritten_version(&self, version: u64) {
        if let Ok(group) = self.wal_group().await {
            group.settle(version);
        }
    }

    pub(crate) fn initialized_mem_wal(&self) -> bool {
        self.durability_handle.initialized_mem_wal()
    }

    /// Snapshot read of the shared policy; pins the value against concurrent stores.
    pub fn durability_policy(&self) -> arc_swap::Guard<Arc<DurabilityPolicy>> {
        self.durability_handle.durability_policy()
    }

    pub fn wal_retention_gate(&self) -> &Arc<RetentionGate> {
        self.durability_handle.wal_retention_gate()
    }

    #[inline]
    pub fn wal_retention_refuse_writes(&self) -> bool {
        self.durability_handle.wal_retention_refuse_writes()
    }

    pub(crate) async fn reserve_recovery_floor(
        &self,
        info: &Arc<dyn Store>,
        minimum: u64,
    ) -> DbResult<u64> {
        self.durability_handle
            .reserve_recovery_floor(info, minimum)
            .await
    }

    /// Backend capability: does this repo persist a WAL across reopen?
    /// Read on the cold publication path only (replication DDL enforcement).
    pub fn has_persistent_wal(&self) -> bool {
        self.durability_handle.has_persistent_wal()
    }

    /// F6b (I2): flush every per-table `history` version-log to disk WITHOUT
    /// draining the WAL.
    ///
    /// The truncation fsync-gate: the drainer calls this immediately before
    /// `wal.truncate_below(durable)` so that every record in a to-be-deleted
    /// sealed segment is physically durable in `history` before its WAL
    /// segment is unlinked (I2 — closes the power-loss window). Distinct from
    /// `flush_buffers`, which drains the WAL first
    /// (`drain_all`) and flushes `data_store`/`info_store` — calling that from
    /// the drainer would recurse. This seam is narrow: it touches ONLY each
    /// MVCC store's `history`, and never the WAL.
    ///
    /// Every store is drained (`flush_history_drain`) and the repository then
    /// issues ONE backend-wide durability barrier (`Repo::persist_once`): the
    /// per-store `flush()` fsynced fjall's single per-Database journal once per
    /// table, so a repository with N open tables paid N identical fsyncs.
    ///
    /// Best-effort over all tables: a per-store flush error is logged and the
    /// first one returned after attempting them all.
    pub async fn flush_all_history(&self) -> DbResult<()> {
        let mvccs = self.catalog.mvcc_snapshot();
        let mut first_err: Option<DbError> = None;
        let any = !mvccs.is_empty();
        for mvcc in mvccs {
            if let Err(e) = mvcc.flush_history_drain().await {
                log::warn!("flush_all_history: {}: {}", self.name, e);
                if first_err.is_none() {
                    first_err = Some(e);
                }
            }
        }
        if any {
            if let Err(e) = self.repo.persist_once().await {
                log::warn!("flush_all_history: persist_once {}: {}", self.name, e);
                if first_err.is_none() {
                    first_err = Some(e);
                }
            }
        }
        match first_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// Open-time unclean bump (called from `tx::recovery` AFTER
    /// `certify_cold_start`): an Armed lifecycle record with no later Clean
    /// certificate costs a history-epoch bump — followers must re-seed. The
    /// CURRENT serve mode is deliberately NOT consulted (review finding 2a):
    /// the Armed obligation survives a configuration change — a repo that
    /// served speculatively and was switched to `durable` still loses its
    /// served tail on an unclean restart, and under durable serving an
    /// old-epoch follower bookmark is inert, so fencing REQUIRES the bump.
    /// The armed record is left as-is: the obligation persists until the next
    /// clean shutdown. A failed bump/flush propagates and blocks admission
    /// (`?` at the caller).
    pub async fn note_unclean_open_bump(&self, permit: &RepoWritePermit) -> DbResult<()> {
        let store = self.tx_info_store().await?;
        let record = crate::meta::repo_identity::load_state(&store).await?;
        if !crate::meta::repo_identity::requires_unclean_bump(record) {
            return Ok(());
        }
        // Negative-control seam: constant-false in production, branch elided.
        if cfg!(any(test, feature = "test-util"))
            && crate::meta::repo_identity::SKIP_UNCLEAN_EPOCH_BUMP_FOR_TEST
                .load(std::sync::atomic::Ordering::Acquire)
        {
            return Ok(());
        }
        self.bump_history_epoch_admitted(permit).await?;
        self.durability_handle.durability_metrics.note_epoch_bump();
        Ok(())
    }

    /// Advance the history epoch while holding repository write admission.
    pub async fn bump_history_epoch_admitted(
        &self,
        permit: &RepoWritePermit,
    ) -> DbResult<RepoIdentity> {
        self.durability_handle
            .write_domain
            .debug_check_permit(permit);
        let _guard = self.durability_handle.marker_write_mutex.lock().await;
        let store = self.tx_info_store().await?;
        RepoIdentity::bump(&store, None).await
    }

    /// Durability flush for a `synced` commit, batched via group-commit so
    /// concurrent synced commits on this repo share one flush+fsync.
    /// (R2d: moved from the `RepoInstance` shell — the closure captures a
    /// cloned `RepoCore`, cheap since every field is `Arc`-shared, so it is
    /// `'static + Send` and can be moved into the detached leader task
    /// `GroupCommit::run` spawns.)
    /// Narrow public durability hooks consumed by the batch port adapter.
    pub async fn batch_synced_flush(&self) -> DbResult<()> {
        self.synced_flush().await
    }

    pub async fn batch_sync_wal(&self) -> DbResult<()> {
        self.sync_wal().await
    }

    pub async fn synced_flush(&self) -> DbResult<()> {
        let core = self.clone();
        self.durability_handle
            .group_commit
            .clone()
            .run(move || {
                let core = core.clone();
                async move { core.flush_buffers().await }
            })
            .await
    }

    /// Force a durable `fsync` of this repo's file WAL (level 2 → level 3).
    /// In-memory repos have no file WAL — this is a no-op there. The
    /// test-only fault-injection seam stays on the shell delegate.
    pub async fn sync_wal(&self) -> DbResult<()> {
        let wal = self.repo_wal().await?;
        if self.initialized_mem_wal() {
            return Ok(());
        }
        wal.sync_wal().await
    }

    /// Flush this repo's in-memory buffers to their durable backing.
    /// (R2d: moved verbatim from the shell's `flush_buffers`; only
    /// `self.core.X` references were flattened to `self.X`.)
    ///
    /// Drains the `__tx__` store and every table's data + info stores, then
    /// issues ONE backend persist (`Repo::persist_once`) — the old per-store
    /// `flush()` loop fsynced fjall's single per-Database journal once per
    /// store (~N identical full-database fsyncs per shutdown).
    /// In-memory stores are no-ops. Best-effort: individual errors are
    /// logged and skipped; returns the first error encountered (if any)
    /// after attempting all stores.
    pub async fn flush_buffers(&self) -> DbResult<()> {
        let mut first_err: Option<DbError> = None;

        // Drain the inflight WAL tail into `history` before flushing durable stores.
        // Post-cutover the ack-path writes only the in-memory overlay; the value
        // is durable in `history` only after the drainer replays the WAL entry.
        // Graceful shutdown drains first so the committed-but-not-yet-drained
        // tail lands durably (and WAL markers truncate). Without a spawned
        // drainer (no commit), this is a cheap no-op. A drain error is returned
        // after subsequent stores are still flushed; recovery on next open can
        // then converge from WAL.
        if let Err(e) = timed_stage(self.name(), "drain_all", self.drainer().drain_all(self)).await
        {
            log::warn!("flush_buffers: drain_all {}: {}", self.name(), e);
            if first_err.is_none() {
                first_err = Some(e);
            }
        }

        match timed_stage(self.name(), "tx_info_store_open", self.tx_info_store()).await {
            Ok(store) => {
                if let Err(e) = timed_stage(self.name(), "tx_info_store", store.flush_drain()).await
                {
                    log::warn!("flush_buffers: tx_info_store {}: {}", self.name(), e);
                    if first_err.is_none() {
                        first_err = Some(e);
                    }
                }
            }
            Err(e) => {
                if first_err.is_none() {
                    first_err = Some(e);
                }
            }
        }

        for table_name in self.list_table_names() {
            // Only tables that are already open own buffered stores. A dormant
            // table has nothing to drain, and instantiating it here would pay
            // a full table open (keyspaces, indexes, validators) per table
            // inside the shutdown budget and register it for MVCC routing. The
            // map guard is dropped before any await.
            let Some(table) = self.catalog.initialized_table(&table_name) else {
                continue;
            };
            if let Err(e) = timed_stage(
                self.name(),
                &format!("data_store {table_name}"),
                table.data_store_read_view().flush_drain(),
            )
            .await
            {
                log::warn!(
                    "flush_buffers: data_store {}/{}: {}",
                    self.name(),
                    table_name,
                    e
                );
                if first_err.is_none() {
                    first_err = Some(e);
                }
            }
            if let Err(e) = timed_stage(
                self.name(),
                &format!("info_store {table_name}"),
                table.info_store_read_view().flush_drain(),
            )
            .await
            {
                log::warn!(
                    "flush_buffers: info_store {}/{}: {}",
                    self.name(),
                    table_name,
                    e
                );
                if first_err.is_none() {
                    first_err = Some(e);
                }
            }
        }

        // Stage I: persist the single per-repo interner ONCE on graceful
        // shutdown. Previously persistence ran per table; the shared manager
        // now owns the repo-wide namespace, so one persist covers every table.
        // WAL entries whose deltas cover these ids can be truncated on next boot.
        match timed_stage(self.name(), "interner_open", self.repo_interner()).await {
            Ok(repo_interner) => {
                if let Err(e) =
                    timed_stage(self.name(), "interner_persist", repo_interner.persist()).await
                {
                    log::warn!(
                        "flush_buffers: repo interner persist {}: {}",
                        self.name(),
                        e
                    );
                    if first_err.is_none() {
                        first_err = Some(e);
                    }
                }
            }
            Err(e) => {
                if first_err.is_none() {
                    first_err = Some(e);
                }
            }
        }

        // ONE durability barrier for every store drained above. The old
        // per-store `flush()` loop fsynced fjall's single per-Database
        // journal once per store (~N identical full-database fsyncs per
        // shutdown — the Windows flush-stall multiplier). `flush_drain`
        // leaves every store's data in the backend's write path; this single
        // `persist(SyncAll)` makes ALL of it durable, byte-for-byte the same
        // end state as the loop. Non-fjall backends: default no-op — their
        // per-store `flush()` semantics are unchanged.
        if let Err(e) = timed_stage(self.name(), "persist_once", self.repo.persist_once()).await {
            log::warn!("flush_buffers: persist_once {}: {}", self.name(), e);
            if first_err.is_none() {
                first_err = Some(e);
            }
        }

        match first_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}

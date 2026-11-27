use crate::repo::changelog_store::write_domain::RepoWriteDomain;
use crate::repo::durability_policy::DurabilityPolicy;
use crate::repo::repo_types::{BoxRepoFactory, RepoFactory};
use crate::table::fk_reverse_cache::FkReverseCache;
use crate::table::interner_manager::InternerManager;
use crate::table::TableConfig;
#[cfg(test)]
use crate::tx::CommitTailBarrier;
use shamir_storage::error::{DbError, DbResult};
#[cfg(any(test, feature = "test-util"))]
use shamir_storage::types::Repo as _;
use shamir_storage::types::Store;
use shamir_types::types::common::THasher;
use std::sync::Arc;

use super::RepoInstance;

mod clean_horizon_repo_impl;
mod durability_repo_impl;
mod journal;
// R2a-1d: the per-repo online-backup capture/seal facade (mounted as a
// sibling because this file's other impl blocks live in this directory).
mod online_backup;
// R2a-1d: the tx-layer test tree names the typed barrier failures. R2a
// step 3a (server adapter) additionally needs to NAME `SealHold` (to keep
// seals per repo until `end_seal`) and `OnlineBackupError` (to map barrier
// failures into `PortError`) — pure re-export, no semantics changed.
pub use online_backup::{OnlineBackupError, SealHold};
mod recovery;
mod seed_backing_impl;
mod seed_metadata_impl;

/// Test seam: 1-indexed countdown — the next `sync_wal` call fails when the
/// armed value reaches 1 (mirrors the catalogue `arm_catalogue_write_fault`
/// pattern). `#[cfg(any(test, feature = "test-util"))]` so shamir-db's
/// dev-dependency tests can arm it cross-crate; production never sees it.
#[cfg(any(test, feature = "test-util"))]
static SYNC_WAL_FAULT_COUNTDOWN: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

impl RepoInstance {
    /// Arm the sync_wal fault seam: the n-th subsequent `sync_wal` call
    /// (1-indexed countdown, global across repos) fails with
    /// `DbError::Storage`. Test-only.
    #[cfg(any(test, feature = "test-util"))]
    #[doc(hidden)]
    pub fn arm_sync_wal_fault(n: usize) {
        SYNC_WAL_FAULT_COUNTDOWN.store(n, std::sync::atomic::Ordering::SeqCst);
    }

    #[cfg(test)]
    pub(crate) fn commit_tail(&self) -> &Arc<CommitTailBarrier> {
        self.core.commit_tail()
    }

    /// cancel-safe: yes — timeout or cancellation only stops this wait.
    /// Stop commit admission first and keep the runtime alive until this returns.
    /// Includes inline, detached and separately spawned AsyncIndex commit tails.
    /// This does not drain WAL history and must not be called by `flush_buffers`
    /// while commits are running.
    pub async fn quiesce_commits(&self, timeout: std::time::Duration) -> DbResult<()> {
        tokio::time::timeout(timeout, self.core.horizon().commit_tail().wait_zero())
            .await
            .map_err(|_| {
                DbError::Internal(format!(
                    "commit quiescence timed out after {timeout:?} for repo '{}'",
                    self.name
                ))
            })
    }

    #[cfg(test)]
    /// Audit §1.7: mutex guarding the recovery-marker disk write. See the
    /// field doc on [`marker_write_mutex`](Self) for why this exists.
    pub(crate) fn marker_write_mutex(&self) -> &Arc<tokio::sync::Mutex<()>> {
        self.core.marker_write_mutex()
    }

    /// Read-only progress of initialized state; never opens stores or starts
    /// a drainer. An untouched gate reports an idle snapshot.
    pub fn diagnostics(&self) -> shamir_tx::GateDiagnostics {
        let mut snapshot = self
            .core
            .horizon()
            .tx_gate_cell()
            .get()
            .map(|gate| gate.diagnostics())
            .unwrap_or_default();
        if let Some(drainer) = self.core.horizon().drainer_cell().get() {
            drainer.enrich_diagnostics(&mut snapshot);
        }
        snapshot
    }

    /// The tx-layer core of this repository, for the crate's own benches and
    /// integration tests that call the `tx` free functions directly. The core
    /// type is unnameable outside the crate; it can only be passed through.
    #[doc(hidden)]
    pub fn core(&self) -> &crate::tx::commit::core::RepoCore {
        &self.core
    }

    /// D2 P1d-2b: the repo's background drainer, spawned once (single-owner).
    /// The body lives on `RepoCore::drainer`.
    pub fn drainer(&self) -> Arc<crate::tx::drainer::Drainer> {
        self.core.drainer()
    }

    /// cancel-safe: NO — recovery can apply durable history and certify a gap
    /// before cancellation. A failed/cancelled open exposes no instance; the
    /// next open retries the idempotent recovery and certification protocol.
    ///
    /// Create, recover and durably certify before returning to ANY caller.
    pub async fn from_factory(
        name: String,
        factory: BoxRepoFactory,
        configs: Vec<TableConfig>,
    ) -> DbResult<Self> {
        Self::from_factory_with_durability(name, factory, configs, DurabilityPolicy::default())
            .await
    }

    /// Unrecovered disk handle solely for tests that explicitly exercise replay
    /// or must install a fault before recovery. Not available in production.
    #[cfg(all(test, feature = "fjall"))]
    pub(crate) async fn from_factory_for_recovery_test(
        name: String,
        factory: BoxRepoFactory,
        configs: Vec<TableConfig>,
    ) -> DbResult<Self> {
        Self::from_factory_unrecovered(name, factory, configs).await
    }

    pub(in crate::repo::instance::repo_instance) async fn from_factory_unrecovered(
        name: String,
        factory: BoxRepoFactory,
        configs: Vec<TableConfig>,
    ) -> DbResult<Self> {
        // Capture the disk backing dir BEFORE consuming the factory, so a
        // disk-backed repo gets a file-WAL group (W3, inert plumbing).
        let wal_dir = factory.backing_dir();
        let repo = factory.create().await?;
        Ok(Self::from_box_repo_with_wal_dir(
            name, repo, configs, wal_dir,
        ))
    }

    /// S5c-X PR6b-2a: this repository's write-admission domain (one per
    /// repository instance, shared by every clone).
    pub fn write_domain(&self) -> &Arc<RepoWriteDomain> {
        self.core.write_domain()
    }

    /// Repository name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Return a clone of this `RepoInstance` with its `name` field replaced
    /// by `new_name`. Used by `DbInstance::rename_repo` (Phase F.3) to
    /// re-key a repository under a new logical name without touching any of
    /// its physical table stores — the tables travel "for free" because they
    /// are keyed by table name *inside* the repo, not by the repo name.
    pub fn with_name(&self, new_name: String) -> Self {
        let mut cloned = self.clone();
        cloned.core.set_name(new_name.clone());
        cloned.name = new_name;
        cloned
    }

    /// Per-table MvccStore map used by the commit pipeline to route
    /// data writes through version-aware storage.
    pub fn per_table_mvcc(&self) -> &Arc<scc::HashMap<u64, Arc<shamir_tx::MvccStore>, THasher>> {
        self.core.per_table_mvcc()
    }

    /// F-28 Step 4 (#831): this repo's cached reverse-FK map. See
    /// [`FkReverseCache`] for the cache-aside population + invalidation
    /// contract. Used by `query::batch::fk_restrict`/`fk_actions`'s
    /// discovery functions (via `TableResolver::resolve_repo`) and by
    /// `ShamirDb::compile_table_schema`'s invalidation call.
    pub fn fk_reverse_cache(&self) -> &Arc<FkReverseCache> {
        self.core.fk_reverse_cache()
    }
    /// Atomic transaction telemetry counters.
    pub fn tx_metrics(&self) -> &Arc<shamir_tx::TxMetrics> {
        self.core.tx_metrics()
    }

    /// Returns the repo's transactional info_store under the
    /// `"__tx__"` namespace.
    ///
    /// Shared with [`tx_gate`](Self::tx_gate) and
    /// [`repo_wal`](Self::repo_wal). The commit pipeline writes
    /// recovery markers (`MetaKey::LastCommittedVersion`,
    /// `MetaKey::NextTxId`) here in Phase 6.5 (see
    /// `crate::tx::commit`) so a clean restart can seed the gate
    /// without scanning every active WAL marker.
    pub async fn tx_info_store(&self) -> DbResult<Arc<dyn Store>> {
        self.core.tx_info_store().await
    }

    #[cfg(any(test, feature = "test-util"))]
    pub async fn from_factory_with_interner_quota_for_test(
        name: String,
        factory: BoxRepoFactory,
        configs: Vec<TableConfig>,
        quota: shamir_types::core::interner::InternerQuota,
    ) -> DbResult<Self> {
        let instance = Self::from_factory_unrecovered(name, factory, configs).await?;
        let manager = InternerManager::new_labeled(
            instance.core.repo().store_get("__interner__").await?,
            instance.name().to_string(),
        )
        .with_quota_for_test(quota);
        instance.install_repo_interner_for_test(manager);
        let permit = instance
            .write_domain()
            .admit()
            .await
            .map_err(|error| DbError::Internal(error.to_string()))?;
        instance.repo_identity_admitted(&permit).await?;
        instance.recover_v2_inflight_admitted(&permit).await?;
        drop(permit);
        Ok(instance)
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn install_repo_interner_for_test(&self, manager: InternerManager) {
        assert!(
            self.core
                .catalog()
                .set_repo_interner_if_uninitialized(manager),
            "repo interner must be uninitialized before test injection",
        );
    }

    /// cancel-safe: yes — `OnceCell::get_or_try_init` semantics. If
    /// cancellation occurs during init the cell stays empty and the next
    /// caller retries. Returned manager is a cheap clone (all internal Arcs
    /// are shared).
    pub async fn repo_interner(&self) -> DbResult<InternerManager> {
        self.core.repo_interner().await
    }
    /// cancel-safe: NO — delegates to `recover_inflight_v2` which iterates
    /// entries and replays each one then removes its WAL marker. Mid-
    /// flight cancellation leaves the recovery sequence partially applied;
    /// ops are idempotent so re-invoking is safe.
    ///
    /// Run V2 WAL recovery: replay any inflight tx entries and remove
    /// their markers. Idempotent — safe to call on every open.
    /// Returns the count of recovered entries.
    pub async fn recover_v2_inflight(&self) -> DbResult<usize> {
        let permit = self
            .core
            .durability_handle()
            .write_domain
            .admit()
            .await
            .map_err(|error| DbError::Internal(error.to_string()))?;
        // Round-11 (stack layout): heap-pin the recovery body (replays every
        // inflight entry through the admitted chain) off this wrapper's frame.
        Box::pin(crate::tx::recovery::recover_inflight_v2(
            &self.core, &permit,
        ))
        .await
    }

    pub(crate) async fn recover_v2_inflight_admitted(
        &self,
        permit: &crate::repo::RepoWritePermit,
    ) -> DbResult<usize> {
        self.core
            .durability_handle()
            .write_domain
            .debug_check_permit(permit);
        // Round-11 (stack layout): see `recover_v2_inflight`.
        Box::pin(crate::tx::recovery::recover_inflight_v2(&self.core, permit)).await
    }

    /// F6b (I2): flush every per-table `history` version-log to disk WITHOUT
    /// draining the WAL.
    ///
    /// The truncation fsync-gate: the drainer calls this immediately before
    /// `wal.truncate_below(durable)` so that every record in a to-be-deleted
    /// sealed segment is physically durable in `history` before its WAL
    /// segment is unlinked (I2 — closes the power-loss window). Distinct from
    /// [`flush_buffers`](Self::flush_buffers), which drains the WAL first
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
        self.core.flush_all_history().await
    }

    /// Durability flush for a `synced` commit, batched via group-commit so
    /// concurrent synced commits on this repo share one flush+fsync.
    ///
    /// The body lives on `RepoCore::synced_flush`; its closure captures a
    /// CLONED `RepoCore` (cheap — every field is `Arc`-shared) so it is
    /// `'static + Send` and can be moved into the detached leader task
    /// `GroupCommit::run` spawns (audit §2.1: the leader loop must not be
    /// tied to the caller's task lifetime, so a cancelled caller cannot
    /// strand `leader_busy`).
    pub async fn synced_flush(&self) -> DbResult<()> {
        self.core.synced_flush().await
    }

    /// Force a durable `fsync` of this repo's file WAL (level 2 → level 3).
    /// In-memory repos have no file WAL — this is a no-op there.
    pub async fn sync_wal(&self) -> DbResult<()> {
        #[cfg(any(test, feature = "test-util"))]
        {
            let remaining = SYNC_WAL_FAULT_COUNTDOWN.load(std::sync::atomic::Ordering::SeqCst);
            if remaining > 0 {
                SYNC_WAL_FAULT_COUNTDOWN.store(remaining - 1, std::sync::atomic::Ordering::SeqCst);
                if remaining == 1 {
                    return Err(DbError::Storage("injected sync_wal fault".to_string()));
                }
            }
        }
        self.core.sync_wal().await
    }

    /// Spawn a background task that runs GC periodically.
    ///
    /// Returns a `tokio::task::JoinHandle` and an `Arc<AtomicBool>` shutdown flag.
    /// Set the flag to `true` to stop the task gracefully.
    ///
    /// The task runs `run_gc()` every `interval`, logging results.
    pub fn spawn_gc_task(
        &self,
        interval: std::time::Duration,
    ) -> (
        tokio::task::JoinHandle<()>,
        Arc<std::sync::atomic::AtomicBool>,
    ) {
        let repo = self.clone();
        let shutdown = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = Arc::clone(&shutdown);

        let handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                if flag.load(std::sync::atomic::Ordering::Relaxed) {
                    break;
                }
                match repo.run_gc().await {
                    Ok(0) => {} // nothing to do, stay quiet
                    Ok(n) => log::debug!("GC cleaned {n} history entries"),
                    Err(e) => log::warn!("GC error: {e}"),
                }
            }
        });

        (handle, shutdown)
    }

    /// cancel-safe: NO — multi-table iteration with per-table GC.
    /// Partial cancellation leaves some tables GC'd, others not; the
    /// per-table `gc()` is itself a multi-step scan+delete sequence.
    /// Idempotent on retry (deletes by version threshold).
    ///
    /// Run garbage collection on all tables' history stores.
    ///
    /// Deletes old versions no longer needed by any active snapshot.
    /// Safe to call concurrently with reads/writes — GC only touches
    /// versions below `min_alive`, which no snapshot can read.
    ///
    /// Returns total number of history entries deleted across all tables.
    pub async fn run_gc(&self) -> DbResult<usize> {
        let stores = self.core.catalog().mvcc_snapshot();

        let mut total = 0usize;
        for mvcc in stores {
            total += mvcc.gc().await?;
        }

        // Phase C Step 7: prune the per-repo commit-write-log on the SAME
        // GC tick that just pruned per-table `MvccStore::version_cache`.
        // Uses the gate's `min_alive()` — byte-for-byte the same threshold
        // the per-store `MvccStore::gc()` consumed inside
        // `prune_version_cache` (mvcc_store.rs:447). Identical discipline,
        // identical safety argument (see `RepoTxGate::prune_commit_log_below`
        // doc-comment and the invariant on mvcc_store.rs:416-441).
        //
        // Zero-overhead on non-Serializable repos: the log is empty, so
        // `prune_commit_log_below` walks an empty `TreeIndex` range and
        // returns 0 immediately. The gate may not even exist yet (the
        // `OnceCell` is lazy-init'd on the first `tx_gate()` call); we use
        // `self.core.horizon().tx_gate_cell().get()` (non-allocating peek) to skip cleanly when
        // the gate has never been initialised.
        //
        // Lock order: `prune_commit_log_below` takes NO locks — it uses
        // `scc::TreeIndex::remove_range` (lock-free CAS). Callers MUST NOT
        // hold `commit_mutex` here (we don't — `run_gc` is an independent
        // background task).
        if let Some(gate) = self.core.horizon().tx_gate_cell().get() {
            let min_alive = gate.min_alive();
            let _pruned = gate.prune_commit_log_below(min_alive);
        }

        self.core.horizon().tx_metrics().on_gc_run(total);
        Ok(total)
    }
}

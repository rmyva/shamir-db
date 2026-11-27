//! Durability accessors and replication-only page wiring.

use crate::repo::durability_policy::{resolve, DurabilityMetricsSnapshot, DurabilityPolicy};
use crate::repo::repo_types::BoxRepoFactory;
use crate::table::TableConfig;
use shamir_query_types::{DurabilityOverride, ReplicationServe};

use super::super::RepoInstance;
use shamir_storage::error::{DbError, DbResult};
use shamir_storage::types::Repo as _;
use std::sync::Arc;

#[cfg(all(test, feature = "fjall"))]
use crate::tx::tests::clean_horizon_tests::test_journal_seam;

impl RepoInstance {
    #[cfg(all(test, feature = "fjall"))]
    pub(crate) async fn from_factory_with_test_segment_bytes(
        name: String,
        factory: BoxRepoFactory,
        configs: Vec<TableConfig>,
        policy: DurabilityPolicy,
        segment_bytes: u64,
    ) -> DbResult<Self> {
        // Test-only seam: consume a handle the test registered BEFORE this
        // open (`tx::clean_horizon::tests::test_journal_seam`); `name` is moved below.
        let pending_journal = test_journal_seam::take(&name);
        let mut instance = Self::from_factory_unrecovered(name, factory, configs).await?;
        let permit = instance
            .write_domain()
            .admit()
            .await
            .map_err(|error| DbError::Internal(error.to_string()))?;
        instance.disable_background_drainer_for_test();
        instance.set_wal_segment_bytes_for_test(segment_bytes);
        instance.set_durability(policy)?;
        // PR6b-3: refuse unsupported durable follower state BEFORE any WAL
        // replay, floor persistence or journal publication.
        let info = instance.tx_info_store().await?;
        crate::meta::repl::bookmark_v3::classify_follower_bookmark(&info).await?;
        instance.repo_identity_admitted(&permit).await?;
        // Install BEFORE recovery: the test feed must remain uninitialized.
        if let Some(handle) = pending_journal {
            instance.install_journal_for_test(handle);
        }
        instance.recover_v2_inflight_admitted(&permit).await?;
        drop(permit);
        Ok(instance)
    }

    /// Retention is checked against the segment size captured for this open.
    pub async fn from_factory_with_durability(
        name: String,
        factory: BoxRepoFactory,
        configs: Vec<TableConfig>,
        policy: DurabilityPolicy,
    ) -> DbResult<Self> {
        #[cfg(test)]
        let fixture = crate::repo::tests::recovery_gap_marker_tests::take_open_fixture(&name);
        #[cfg(test)]
        let instance = match fixture {
            Some(instance) => instance,
            None => Self::from_factory_unrecovered(name, factory, configs).await?,
        };
        #[cfg(not(test))]
        let instance = Self::from_factory_unrecovered(name, factory, configs).await?;
        instance.set_durability(policy)?;
        // PR6b-3: refuse unsupported durable follower state BEFORE any WAL
        // replay, floor persistence or journal publication.
        let info = instance.tx_info_store().await?;
        crate::meta::repl::bookmark_v3::classify_follower_bookmark(&info).await?;
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

    /// Replay validated WAL entries while holding the ordinary exclusive store lock.
    /// The caller must use an unbuffered disk factory and keep its sibling WAL
    /// offline and unchanged after staging: normal gate initialization opens it
    /// to seed the version floor. Replay does not retire the staged WAL.
    ///
    /// `Err((true, error))` means the exclusive store open failed. All later
    /// failures are `Err((false, error))` and may leave changes on the copy.
    /// Cancel-safe: no; dropping the future can leave partial replay effects
    /// and leave the changefeed writer holding the store lock.
    pub(crate) async fn replay_staged_wal(
        name: String,
        factory: BoxRepoFactory,
        configs: Vec<TableConfig>,
        entries: Vec<shamir_wal::WalEntryV2>,
        v_star: u64,
    ) -> Result<usize, (bool, DbError)> {
        let instance = Box::pin(Self::from_factory_unrecovered(name, factory, configs))
            .await
            .map_err(|error| (true, error))?;
        let after_open = |error| (false, error);
        {
            let info = Box::pin(instance.tx_info_store())
                .await
                .map_err(after_open)?;
            let follower_state = Box::pin(
                crate::meta::repl::bookmark_v3::classify_follower_bookmark(&info),
            )
            .await
            .map_err(after_open)?;
            // Normal boot may complete receipts from the store; strict replay
            // must not apply work outside the staged WAL or possibly above V*.
            if matches!(
                follower_state,
                crate::meta::repl::FollowerBookmarkState::Native(bookmark)
                    if bookmark.outstanding_receipt.is_some()
            ) {
                return Err(after_open(DbError::Storage(
                    "strict WAL replay refuses an outstanding follower receipt".into(),
                )));
            }
        }
        let permit = instance
            .write_domain()
            .admit()
            .await
            .map_err(|error| after_open(DbError::Internal(error.to_string())))?;
        Box::pin(instance.repo_identity_admitted(&permit))
            .await
            .map_err(after_open)?;
        Box::pin(instance.tx_gate()).await.map_err(after_open)?;
        let replay = Box::pin(crate::tx::recovery::recover_inflight_v2_strict(
            &instance.core,
            &permit,
            entries,
            v_star,
        ))
        .await;
        // Cold certification may have started a writer that holds the store
        // lock after instance drop. Join it even if replay failed.
        let shutdown = match instance.core.durability_handle().changefeed.get() {
            Some(handle) => Box::pin(handle.feed.shutdown_and_wait()).await,
            None => Ok(()),
        };
        let count = replay.map_err(after_open)?;
        shutdown.map_err(after_open)?;
        // Persist the unbuffered stores without flush_buffers, which drains
        // the WAL and can truncate it. Keep the instance alive for the barrier.
        instance
            .core
            .repo()
            .persist_once()
            .await
            .map_err(after_open)?;
        Ok(count)
    }

    /// Scrape initialized WAL and repo-owned pull observations; never opens stores.
    #[must_use]
    pub fn durability_metrics_snapshot(&self) -> DurabilityMetricsSnapshot {
        self.core.durability_handle().durability_metrics_snapshot()
    }

    /// Snapshot read of the shared policy; pins the value against concurrent stores.
    #[must_use]
    pub fn durability_policy(&self) -> arc_swap::Guard<Arc<DurabilityPolicy>> {
        self.core.durability_policy()
    }

    /// Check whether an already-initialized transaction gate requires repo
    /// reopen, without initializing the gate or touching transaction state.
    pub fn durability_change_requires_reopen(&self) -> bool {
        self.initialized_tx_gate()
            .is_some_and(shamir_tx::RepoTxGate::requires_reopen)
    }

    /// Resolve and validate an ALTER override against this open repo's
    /// captured segment size, without initializing transaction state. The
    /// result can be passed to `set_durability` after catalogue persistence;
    /// both paths use the same immutable segment-size input.
    pub fn validate_durability_change(
        &self,
        server_default: &DurabilityPolicy,
        over: &DurabilityOverride,
    ) -> DbResult<DurabilityPolicy> {
        if self.durability_change_requires_reopen() {
            return Err(DbError::RepoRequiresReopen(self.name.clone()));
        }
        resolve(server_default, Some(over), self.core.wal_segment_bytes())
    }

    /// Validate before storing; rejected policies leave every clone unchanged.
    /// Factory opens validate once and install before recovery/admission.
    /// Later setters use the same captured segment size as WAL rotation.
    pub fn set_durability(&self, policy: DurabilityPolicy) -> DbResult<()> {
        let resolved = resolve(&policy, None, self.core.wal_segment_bytes())?;
        // `completed_reseed` on an ephemeral backend (in-memory / hybrid;
        // Mem WAL sink, `!has_persistent_wal()`) is ALLOWED: it is the only
        // honest serve mode when nothing survives a restart. `wal_commit`
        // tiers that need a persistent WAL (e.g. `synced`) are silently
        // downgraded by `for_wal_backend`, with one warn line per open at
        // WAL open. Durable serving on an ephemeral backend is NOT rejected
        // here (durable mem repos are relied on by tests and legal at the
        // engine layer); durable-ephemeral publication-time validation lives
        // server-side.
        let policy = resolved.for_wal_backend(self.core.durability_handle().wal_dir.is_some());
        self.core
            .durability_handle()
            .durability
            .store(Arc::new(policy));
        Ok(())
    }

    /// The repo's `WalGroupCommit` (captured when `repo_wal` initializes).
    /// Used by the replication read path to sample `R` and coalesce syncs.
    /// Also exposes the recoverability watermark to embedded callers.
    pub async fn wal_group(&self) -> DbResult<Arc<shamir_wal::WalGroupCommit>> {
        self.core.wal_group().await
    }

    /// One replication journal page. `durable` (default) bounds it by
    /// `min(confirm_through, W, R)`; `completed_reseed` bounds it by
    /// `min(confirm_through, W)` with NO `R` clamp (see
    /// `read_replication_page`).
    ///
    /// Smallest-correct mechanism: the completion-watermark bound lives
    /// inside [`read_changelog_bounded_impl`](Self::read_changelog_bounded_impl)
    /// and `read_changelog_from` (used by changefeed subscribers) must NOT
    /// gain the `R` clamp — so the replication path calls the bounded impl
    /// directly, once unclamped, and re-reads ONCE with the clamped bound
    /// only when the helper (which may fsync + bounded-wait to raise `R`)
    /// pulled the bound below the first page's. When the first page already
    /// fits under `R` (the common case, incl. every `Mem` WAL) there is no
    /// re-read and no fsync.
    pub(in crate::repo::instance::repo_instance) async fn read_changelog_replication_page(
        &self,
        from_version: u64,
        limit: usize,
    ) -> DbResult<crate::repo::BoundedJournalRead> {
        // PR8 completed_reseed chunk 2: arm the lifecycle record BEFORE any
        // speculative page byte is built or returned. The policy is snapshotted
        // into a local `Copy` BEFORE any await so the arc_swap Guard never
        // crosses `.await`; the ensure_armed flush lands strictly before the
        // first served page, so a crash after serving implies Armed on disk.
        let mode: ReplicationServe = self.durability_policy().replication_serve();
        if mode == ReplicationServe::CompletedReseed {
            self.ensure_replication_armed().await?;
        }
        let first = self
            .read_changelog_bounded_impl(from_version, limit, false, None)
            .await?;
        let group = self.wal_group().await?;
        crate::repo::durability_policy::read_replication_page(
            first,
            &group,
            mode,
            &self.core.durability_handle().durability_metrics,
            |clamp| self.read_changelog_bounded_impl(from_version, limit, false, clamp),
        )
        .await
    }

    /// Arm the lifecycle record for speculative serving. Atomic fast path
    /// (cache hit), store write at most once per open (cold path): concurrent
    /// clones that miss the cache may both write `Armed`, but the value is
    /// identical and idempotent, so no ordering hazard exists.
    pub(crate) async fn ensure_replication_armed(&self) -> DbResult<()> {
        if self
            .core
            .replication_armed()
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Ok(());
        }
        let store = self.tx_info_store().await?;
        crate::meta::repo_identity::ensure_armed(&store).await?;
        self.core
            .replication_armed()
            .store(true, std::sync::atomic::Ordering::Release);
        Ok(())
    }

    /// Tail of `shutdown_clean`, called (inside the shutdown timeout) after
    /// `flush_buffers`. Settled includes aborted allocations, unlike
    /// last_committed; after stopped admission + quiescence + drain, W must
    /// equal H — unresolved allocation holes refuse a clean certificate.
    /// PR8 completed_reseed chunk 2: the Clean lifecycle certificate is
    /// written ONLY after every step it certifies has already landed
    /// durably — journal (record_clean_horizon), allocation settle
    /// (watermark == H), data buffers (flush_buffers) and WAL evidence —
    /// so a surviving Clean implies all of them. Under `durable` serving
    /// this is a no-op (no speculative period to close).
    pub(crate) async fn finish_clean_shutdown(
        &self,
        deadline: tokio::time::Instant,
    ) -> DbResult<()> {
        let gate = self.tx_gate().await?;
        let h = gate.version_allocation_high_water_mark();
        if gate.completion().watermark() != h {
            return Err(DbError::Storage(
                "unsettled versions at clean shutdown".into(),
            ));
        }
        let handle = self.changefeed().await?;
        if tokio::time::Instant::now() >= deadline {
            return Err(DbError::Storage(
                "clean shutdown timed out before checkpoint".into(),
            ));
        }
        handle.feed.record_clean_horizon(h).await?;
        let mode: ReplicationServe = self.durability_policy().replication_serve();
        if mode != ReplicationServe::CompletedReseed {
            return Ok(());
        }
        let store = self.tx_info_store().await?;
        crate::meta::repo_identity::mark_clean(&store).await
    }

    #[cfg(test)]
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
    pub(crate) async fn note_unclean_open_bump(
        &self,
        permit: &crate::repo::RepoWritePermit,
    ) -> DbResult<()> {
        self.core.note_unclean_open_bump(permit).await
    }
}

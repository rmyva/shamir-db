use super::repo_core::RepoCore;
use crate::cold_reconcile::lifecycle::{InstallError, JobSubmitError};
use crate::follower_durability::receipt::resume::worker::receipt_handler;
use crate::follower_durability::receipt::{ReceiptApplyJob, ReceiptLease};
#[cfg(any(test, feature = "test-util"))]
use crate::recovery::clean_horizon::checkpoint::test_seam;
use crate::repo::journal::backend::durability_handle::ChangefeedHandle;
use crate::{ApplyOutcome, ApplyPolicy, ReplicatedEvent};
use shamir_storage::error::{DbError, DbResult};
use shamir_storage::types::{Repo as _, Store};
use std::sync::atomic::Ordering;
use std::sync::Arc;

impl RepoCore {
    /// tx-facing seam (`tx/recovery/clean_horizon/checkpoint.rs` imports
    /// `crate::repo::durability_policy::checkpoint_allocation_floor`). Resolves
    /// the journal store and delegates to the repo-layer core.
    pub(crate) async fn checkpoint_allocation_floor(&self) -> DbResult<u64> {
        let store = self.repo.store_get("__changelog__").await?;
        crate::repo::durability_policy::checkpoint_allocation_floor_for(store).await
    }

    /// cancel-safe: yes — `OnceCell::get_or_try_init`. If cancellation
    /// occurs during init the cell stays empty and the next caller retries;
    /// the returned handle is an in-memory `Arc` clone.
    ///
    /// Lazily build (once per repo) the changefeed: a live broadcast sender
    /// plus a background journal writer over the `"__changelog__"` store.
    /// The feed is "always on" — the commit-path feeds the journal on every
    /// non-empty commit regardless of live subscribers — so callers that
    /// subscribed late can still catch up via `read_changelog_from`.
    pub async fn changefeed(&self) -> DbResult<ChangefeedHandle> {
        let handle = self
            .durability_handle
            .changefeed
            .get_or_try_init(|| async {
                #[cfg(any(test, feature = "test-util"))]
                if let Some(hook) = self.journal_publish_hook.load_full() {
                    if hook
                        .init_failures
                        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_sub(1))
                        .is_ok()
                    {
                        return Err(DbError::Storage("injected changefeed init failure".into()));
                    }
                }
                let store: Arc<dyn Store> = self.repo.store_get("__changelog__").await?;
                let adapter = crate::repo::changelog_store::StoreChangelog::new(store);
                let clean_horizon = adapter.load_clean_horizon().await?;
                let (marker, repair_marker) = adapter.load_gap_marker().await?;
                let cl_store: Arc<dyn shamir_tx::ChangelogStore> = Arc::new(adapter);
                let pending = self
                    .durability_handle
                    .pending_changefeed_gap
                    .load(Ordering::Acquire);
                let restored = match (marker, pending) {
                    (m, 0) => m,
                    (None, p) => Some((p, p)),
                    (Some((first, through)), p) => Some((first.min(p), through.max(p))),
                };
                #[cfg(any(test, feature = "test-util"))]
                if let Some(hook) = self.journal_publish_hook.load_full() {
                    if hook.init_snapshot_armed.swap(false, Ordering::AcqRel) {
                        hook.reached.notify_one();
                        hook.resume.notified().await;
                    }
                }
                // Seeds claim durability: flush pending/corrupt metadata before
                // spawning the sole writer. Failed init leaves pending intact.
                if repair_marker || restored != marker {
                    if let Some(interval) = restored {
                        cl_store
                            .put(
                                shamir_tx::changefeed::gap_marker_key(),
                                shamir_tx::changefeed::encode_gap_marker(interval),
                            )
                            .await
                            .map_err(|e| DbError::Storage(e.to_string()))?;
                    }
                }
                let feed = shamir_tx::RepoChangefeed::new_with_metadata(
                    Arc::clone(&cl_store),
                    restored,
                    clean_horizon,
                );
                Ok::<ChangefeedHandle, DbError>(ChangefeedHandle {
                    feed,
                    store: cl_store,
                })
            })
            .await?
            .clone();
        // Recheck AFTER OnceCell publishes, not only inside its closure: a failed
        // committer can store pending after the closure snapshot but before
        // get() becomes Some. Together with the publisher's store+get recheck,
        // either that publisher sees the feed or this path sees its minimum.
        // RMW (not just an Acquire load) participates in the pending atomic's
        // modification order. If the publisher's RMW is first, we see its gap;
        // if ours is first, its Acquire observes our Release and therefore the
        // already-published OnceCell. This closes the store-buffering schedule
        // without SeqCst or another lock, including publication after snapshot.
        let pending = self
            .durability_handle
            .pending_changefeed_gap
            .fetch_or(0, Ordering::AcqRel);
        if pending != 0 {
            handle.feed.mark_gap(pending);
        }
        Ok(handle)
    }

    /// Resolve before entering the synchronous publish block. Failure is
    /// logged, but does not mark a gap until this version actually publishes.
    pub async fn prepare_journal_event(
        &self,
        event: Option<shamir_tx::ChangelogEvent>,
    ) -> Option<(
        Arc<shamir_tx::ChangelogEvent>,
        Option<Arc<shamir_tx::RepoChangefeed>>,
    )> {
        let event = Arc::new(event?);
        let feed = match self.changefeed().await {
            Ok(handle) => Some(handle.feed),
            Err(e) => {
                log::warn!("changefeed init failed for repo {}: {e}", self.name());
                None
            }
        };
        Some((event, feed))
    }

    pub(crate) async fn mark_changefeed_gap(&self, version: u64) -> DbResult<()> {
        self.changefeed().await?.feed.mark_gap(version);
        Ok(())
    }

    /// Synchronous publish-time fallback; minimum is shared across repo clones.
    pub fn record_pending_changefeed_gap(&self, version: u64) {
        self.durability_handle
            .record_pending_changefeed_gap(version);
    }

    /// Live only: never re-enqueue an event already handled before publish.
    pub fn broadcast_changefeed_event(&self, event: Option<Arc<shamir_tx::ChangelogEvent>>) {
        self.durability_handle.broadcast_changefeed_event(event);
    }

    pub async fn follower_journal_store(&self) -> DbResult<Arc<dyn Store>> {
        self.repo.store_get("__changelog__").await
    }

    pub async fn changelog_store_for_cold_reconcile(&self) -> DbResult<Arc<dyn Store>> {
        self.repo.store_get("__changelog__").await
    }

    /// Legacy certification retained only for backends without a persistent
    /// WAL (exclusive-open certification and coverage installation, never
    /// live replay).
    pub(crate) async fn certify_legacy_cold_start(&self) -> DbResult<()> {
        let gate = self.tx_gate().await?;
        let h = gate.version_allocation_high_water_mark();
        self.flush_all_history().await?;
        let handle = self.changefeed().await?;
        let j = handle.feed.clean_horizon();
        if h > j {
            handle.feed.record_recovery_gap(j + 1, h).await?;
            handle.feed.record_clean_horizon(h).await?;
        }
        // Only AFTER every recovery/certification I/O succeeded. Missing WAL
        // predecessors may be aborted, in durable history, or unknown: the gap
        // prevents serving them as a complete journal, while R permits new writes.
        if let Some(coverage) = self.wal_group().await?.coverage_observer() {
            coverage.certify_exclusive_open(h);
        }
        Ok(())
    }

    #[cfg(any(test, feature = "test-util"))]
    pub(crate) async fn fire_post_publish_pre_broadcast_test_hook(&self) {
        if let Some(hook) = self.journal_publish_hook.load_full() {
            if hook.broadcast_armed.swap(false, Ordering::AcqRel) {
                hook.reached.notify_one();
                hook.resume.notified().await;
            }
        }
    }

    #[cfg(any(test, feature = "test-util"))]
    #[doc(hidden)]
    pub fn clean_horizon_test_state(
        &self,
    ) -> &std::sync::OnceLock<Arc<std::sync::Mutex<test_seam::State>>> {
        &self.clean_horizon_test_state
    }

    /// Lazy accessor for the repository-owned receipt worker lease — same
    /// `OnceCell` pattern as `tx_gate()`: the worker is installed (the only
    /// `tokio::spawn` of the receipt path, inside `install_worker`) on the
    /// FIRST submission and lives until shutdown or repo drop.
    #[doc(hidden)]
    pub async fn receipt_worker_installed(&self) -> DbResult<Arc<ReceiptLease>> {
        let lease = self
            .receipt_worker
            .get_or_try_init(|| async {
                let gate = self.tx_gate().await?;
                let lifecycle = self.worker_lifecycle.as_ref().ok_or_else(|| {
                    DbError::Internal(format!(
                        "receipt worker for repo {} has no foreground lifecycle",
                        self.name()
                    ))
                })?;
                let lease = ReceiptLease::new(gate, lifecycle.lifecycle().status());
                lifecycle
                    .lifecycle()
                    .install_worker(lease.clone(), receipt_handler)
                    .map_err(|error| match error {
                        InstallError::AlreadyInstalled => {
                            DbError::Internal("receipt worker already installed".into())
                        }
                        InstallError::ClosedOrForbidden => DbError::Internal(format!(
                            "receipt worker for repo {} cannot be installed: \
                             lifecycle closed or spawn forbidden",
                            self.name()
                        )),
                    })?;
                Ok::<Arc<ReceiptLease>, DbError>(Arc::new(lease))
            })
            .await?;
        Ok(Arc::clone(lease))
    }

    /// Apply entry: submit one bounded job to the lifecycle and await the
    /// oneshot reply. Only the REPLY is awaited — dropping this future at
    /// the await point drops the receiver, and the worker KEEPS RUNNING the
    /// job with its own repo clone. The typed submit errors keep the old
    /// branch's wording.
    pub(crate) async fn submit_receipt_job(
        &self,
        info: Arc<dyn Store>,
        events: Vec<ReplicatedEvent>,
        bookmark: u64,
        policy: ApplyPolicy,
    ) -> DbResult<ApplyOutcome> {
        // Lazy install on the first submission (no-op afterwards).
        self.receipt_worker_installed().await?;
        let lifecycle = self.worker_lifecycle.as_ref().ok_or_else(|| {
            DbError::Internal(format!("receipt worker for repo {} shut down", self.name()))
        })?;
        let payload = ReceiptApplyJob {
            repo: self.clone(),
            info,
            events,
            bookmark,
            policy,
        };
        let reply = lifecycle
            .lifecycle()
            .submit(payload)
            .map_err(|error| match error {
                JobSubmitError::Closing => {
                    DbError::Internal(format!("receipt worker for repo {} shut down", self.name()))
                }
                JobSubmitError::NotInitialized => {
                    DbError::Internal("receipt worker shut down".into())
                }
                JobSubmitError::Full => DbError::Internal(
                    "receipt worker queue full (capacity 4); retry the pull".into(),
                ),
            })?;
        // The worker owns the job: a dropped caller only drops `reply`.
        reply
            .await
            .map_err(|_| DbError::Internal("receipt worker dropped the reply".into()))?
    }

    /// Reconcile the persistent journal from surviving WAL entries before service.
    /// The caller's write-domain permit is threaded through: cold recovery must
    /// never re-admit under a held permit (non-reentrant domain).
    pub async fn certify_cold_start(
        &self,
        entries: &[shamir_wal::WalEntryV2],
        permit: &crate::repo::RepoWritePermit,
    ) -> DbResult<()> {
        crate::cold_reconcile::reconcile_cold_start(self, entries, permit).await
    }
}

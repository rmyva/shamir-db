use crate::query::batch::{BatchError, BatchTxPort};
use shamir_engine_query::query::write::WriteResult;
use shamir_query_types::batch::VisibilityWait;
use shamir_storage::error::DbResult;
use shamir_types::access::Actor;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use shamir_engine_query::query::filter::read_budget::{QueryMetrics, QueryOp};

use super::RepoInstance;

mod follower_durability;
mod receipt_worker_impl;
mod write_domain_membership;

#[async_trait::async_trait]
impl BatchTxPort for crate::tx::RepoCore {
    async fn commit_implicit_batch(
        &self,
        tx: shamir_tx::TxContext,
        wait: VisibilityWait,
        cancel: Option<Arc<shamir_tx::CancelHandle>>,
    ) -> Result<crate::tx::TxOutcome, crate::tx::CommitError> {
        crate::tx::ack::commit_batch_tx(
            tx,
            self,
            wait,
            shamir_tx::visibility_metrics::PendingKind::ImplicitBatch,
            cancel,
        )
        .await
    }

    async fn commit_explicit_batch(
        &self,
        tx: shamir_tx::TxContext,
        wait: VisibilityWait,
        cancel: Option<Arc<shamir_tx::CancelHandle>>,
    ) -> Result<crate::tx::TxOutcome, crate::tx::CommitError> {
        crate::tx::ack::commit_batch_tx(
            tx,
            self,
            wait,
            shamir_tx::visibility_metrics::PendingKind::ExplicitTx,
            cancel,
        )
        .await
    }

    async fn begin_interactive_batch(
        &self,
        isolation: shamir_tx::IsolationLevel,
    ) -> DbResult<(shamir_tx::TxContext, shamir_tx::SnapshotGuard)> {
        self.begin_batch_tx(isolation).await
    }

    async fn commit_interactive_batch(
        &self,
        tx: shamir_tx::TxContext,
    ) -> Result<crate::tx::TxOutcome, crate::tx::CommitError> {
        self.commit_batch_tx(
            tx,
            self.batch_commit_visibility_wait(),
            shamir_tx::visibility_metrics::PendingKind::ExplicitTx,
            None,
        )
        .await
    }

    async fn release_batch_locks(&self, tx: &shamir_tx::TxContext) {
        self.release_batch_tx_locks(tx).await
    }

    async fn release_interactive_batch_locks(&self, tx: &shamir_tx::TxContext) {
        self.release_batch_tx_locks(tx).await
    }

    async fn synced_flush(&self) -> DbResult<()> {
        self.batch_synced_flush().await
    }

    async fn sync_wal(&self) -> DbResult<()> {
        self.batch_sync_wal().await
    }

    fn default_visibility_wait(&self) -> VisibilityWait {
        self.batch_commit_visibility_wait()
    }
}

#[async_trait::async_trait]
impl BatchTxPort for RepoInstance {
    async fn commit_implicit_batch(
        &self,
        tx: shamir_tx::TxContext,
        wait: VisibilityWait,
        cancel: Option<Arc<shamir_tx::CancelHandle>>,
    ) -> Result<crate::tx::TxOutcome, crate::tx::CommitError> {
        self.core
            .commit_batch_tx(
                tx,
                wait,
                shamir_tx::visibility_metrics::PendingKind::ImplicitBatch,
                cancel,
            )
            .await
    }

    async fn commit_explicit_batch(
        &self,
        tx: shamir_tx::TxContext,
        wait: VisibilityWait,
        cancel: Option<Arc<shamir_tx::CancelHandle>>,
    ) -> Result<crate::tx::TxOutcome, crate::tx::CommitError> {
        self.core
            .commit_batch_tx(
                tx,
                wait,
                shamir_tx::visibility_metrics::PendingKind::ExplicitTx,
                cancel,
            )
            .await
    }

    async fn begin_interactive_batch(
        &self,
        isolation: shamir_tx::IsolationLevel,
    ) -> DbResult<(shamir_tx::TxContext, shamir_tx::SnapshotGuard)> {
        self.begin_tx(isolation).await
    }

    async fn commit_interactive_batch(
        &self,
        tx: shamir_tx::TxContext,
    ) -> Result<crate::tx::TxOutcome, crate::tx::CommitError> {
        self.commit_tx_with_options(tx, self.commit_visibility_wait())
            .await
    }

    async fn release_batch_locks(&self, tx: &shamir_tx::TxContext) {
        self.core.release_batch_tx_locks(tx).await
    }

    async fn release_interactive_batch_locks(&self, tx: &shamir_tx::TxContext) {
        self.core.release_batch_tx_locks(tx).await
    }

    async fn synced_flush(&self) -> DbResult<()> {
        self.core.batch_synced_flush().await
    }

    async fn sync_wal(&self) -> DbResult<()> {
        self.core.batch_sync_wal().await
    }

    fn default_visibility_wait(&self) -> VisibilityWait {
        self.commit_visibility_wait()
    }
}

impl RepoInstance {
    pub async fn tx_gate(&self) -> DbResult<Arc<shamir_tx::RepoTxGate>> {
        self.core.tx_gate().await
    }

    /// Returns the eager latest-state publication floor for this repo.
    ///
    /// Lower allocated versions may still be unsettled. Keep this value for
    /// latest-state/replication diagnostics, not snapshot positions or seeds;
    /// use [`consistent_version`](Self::consistent_version) for those.
    ///
    /// cancel-safe: yes — delegates to cancellation-safe gate initialization.
    pub async fn current_commit_version(&self) -> DbResult<u64> {
        let gate = self.tx_gate().await?;
        Ok(gate.last_committed())
    }

    /// Highest contiguous settled version, suitable for snapshot positions.
    /// Every version through this position is materialized or aborted.
    ///
    /// cancel-safe: yes — delegates to cancellation-safe gate initialization.
    pub async fn consistent_version(&self) -> DbResult<u64> {
        let gate = self.tx_gate().await?;
        Ok(gate.visible_floor())
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
        self.core.repo_wal().await
    }

    /// Open a fresh transaction on this repo.
    ///
    /// Returns a `(TxContext, SnapshotGuard)` pair. The guard's lifetime
    /// must extend at least until commit (or drop = rollback) — drop
    /// removes the snapshot from the active set so GC can reclaim
    /// versions older than `min_alive`.
    ///
    /// `repo_id` in the TxContext is populated via `repo_token`. The body
    /// lives on `RepoCore::begin_tx`.
    pub async fn begin_tx(
        &self,
        isolation: shamir_tx::IsolationLevel,
    ) -> DbResult<(shamir_tx::TxContext, shamir_tx::SnapshotGuard)> {
        self.core.begin_tx(isolation).await
    }

    pub fn commit_visibility_wait(&self) -> VisibilityWait {
        self.core.commit_visibility_wait()
    }

    /// Commit a transaction via the 7-phase commit pipeline.
    ///
    /// Wrapper around [`crate::tx::commit_tx`]. The free function is
    /// the canonical implementation; this method exposes it on the
    /// natural semantic owner.
    pub async fn commit_tx(
        &self,
        tx: shamir_tx::TxContext,
    ) -> Result<crate::tx::TxOutcome, crate::tx::CommitError> {
        self.commit_tx_with_options(tx, self.commit_visibility_wait())
            .await
    }

    /// Select the default for receipt-bearing transaction commits at construction.
    /// Configure before sharing this handle; subsequent clones copy this policy.
    /// This is handle-local configuration, not shared mutable repository state:
    /// changing one clone leaves existing clones (including registered handles)
    /// unchanged even though they share the same gate and storage.
    /// Value-only implicit commits remain Strict until VIS-4 provides receipts.
    pub fn with_commit_visibility_wait(mut self, wait: VisibilityWait) -> Self {
        self.core.set_commit_visibility_wait(wait);
        self
    }

    /// Commit with an explicit acknowledgement policy, resolved before the pipeline.
    /// The bounded budget excludes inner execution and starts at its known outcome.
    /// cancel-safe: partial — same inner commit ownership as commit_tx; dropping
    /// only acknowledgement cannot abort materialization or discharge recovery.
    pub async fn commit_tx_with_options(
        &self,
        tx: shamir_tx::TxContext,
        wait: VisibilityWait,
    ) -> Result<crate::tx::TxOutcome, crate::tx::CommitError> {
        let started = Instant::now();
        let result = self
            .core
            .commit_batch_tx(
                tx,
                wait,
                shamir_tx::visibility_metrics::PendingKind::ExplicitTx,
                None,
            )
            .await;
        self.metrics.record_duration(
            QueryOp::Tx,
            started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
        );
        if result.is_err() {
            self.metrics.record_error(None);
        }
        result
    }

    /// Borrow rather than clone the retained gate on the acknowledgement fast path.
    pub(crate) fn initialized_tx_gate(&self) -> Option<&shamir_tx::RepoTxGate> {
        self.core.initialized_tx_gate()
    }

    /// R1-b — read the durable per-(db,repo) follower replication bookmark.
    ///
    /// Returns the highest LEADER `commit_version` this follower has
    /// durably recorded as applied on this repo. Returns `0` on a fresh
    /// repo (the marker has never been written). Receipt checkpointing
    /// advances the durable bookmark; the value is used for O(1)
    /// idempotent re-delivery gating.
    ///
    /// Stored in the repo's `__tx__` info-store under
    /// [`MetaKey::ReplicationBookmark`](crate::meta::MetaKey::ReplicationBookmark)
    /// via the same envelope codec as `LastCommittedVersion` / `NextTxId`.
    /// See `crate::meta::repl_bookmark` for the codec rationale and the
    /// separate-marker rationale (leader-version space ≠ local-version
    /// space).
    pub async fn replication_bookmark(&self) -> DbResult<u64> {
        let info_store = self.tx_info_store().await?;
        crate::meta::repl_bookmark::load_replication_bookmark(&info_store).await
    }

    /// Run a single non-tx write as an implicit single-op BATCH transaction.
    ///
    /// The body lives in `query/batch/control/implicit.rs`.
    pub async fn run_implicit_batch_tx<F>(
        &self,
        actor: Actor,
        alias: &str,
        stage: F,
    ) -> Result<WriteResult, BatchError>
    where
        F: for<'t> FnOnce(
            &'t mut shamir_tx::TxContext,
        )
            -> Pin<Box<dyn Future<Output = DbResult<WriteResult>> + Send + 't>>,
    {
        crate::query::batch::run_implicit_batch_tx(&self.core, actor, alias, stage).await
    }

    /// Open the implicit Snapshot batch tx; the body lives in
    /// `query/batch/control/implicit.rs`.
    pub async fn begin_implicit_batch_tx(
        &self,
        actor: Actor,
        alias: &str,
    ) -> Result<(shamir_tx::TxContext, shamir_tx::SnapshotGuard), BatchError> {
        crate::query::batch::begin_implicit_batch_tx(&self.core, actor, alias).await
    }

    /// Open an implicit batch tx with a caller-chosen isolation level; the
    /// body lives in `query/batch/control/implicit.rs`.
    pub async fn begin_implicit_batch_tx_with_isolation(
        &self,
        actor: Actor,
        alias: &str,
        isolation: shamir_tx::IsolationLevel,
    ) -> Result<(shamir_tx::TxContext, shamir_tx::SnapshotGuard), BatchError> {
        crate::query::batch::begin_implicit_batch_tx_with_isolation(
            &self.core, actor, alias, isolation,
        )
        .await
    }

    /// Commit an implicit batch tx; the body lives in
    /// `query/batch/control/implicit.rs`.
    pub async fn commit_implicit_batch_tx(
        &self,
        tx: shamir_tx::TxContext,
        alias: &str,
    ) -> Result<shamir_query_types::batch::TransactionInfo, BatchError> {
        crate::query::batch::commit_implicit_batch_tx(&self.core, tx, alias).await
    }
}

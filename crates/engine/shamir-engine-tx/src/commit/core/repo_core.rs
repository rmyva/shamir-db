use super::tx_horizon::TxHorizon;
use crate::repo::repo_types::BoxRepo;
use crate::table::fk_reverse_cache::FkReverseCache;
use crate::table::TableCatalog;

use crate::apply_replicated::follower_durability::receipt::{
    ApplyOutcome, ReceiptApplyJob, ReceiptLease,
};
#[cfg(any(test, feature = "test-util"))]
use crate::clean_horizon_test_seam as test_seam;
use crate::cold_reconcile::lifecycle::ForegroundLifecycle;
use crate::commit::rename_fence::RenameFenceView;
#[cfg(any(test, feature = "test-util"))]
use crate::test_seams::JournalPublishHook;
#[cfg(any(test, feature = "test-util"))]
use crate::test_seams::TableInitHook;
use shamir_engine_repo::DurabilityHandle;
use shamir_query_types::batch::VisibilityWait;
use shamir_storage::error::DbResult;
use std::sync::{Arc, Weak};

#[derive(Clone)]
pub struct RepoCore {
    pub(crate) name: String,
    pub(crate) repo: BoxRepo,
    /// Plain Arc-shared catalogue of this repo's tables; constructed below,
    /// cloned shallowly.
    pub(crate) catalog: TableCatalog,
    /// Durability bundle (R2b): the durability-related Arc cells of this
    /// repo, owned by the repo layer; cloned shallowly like every field.
    pub(crate) durability_handle: DurabilityHandle,
    pub(crate) horizon: TxHorizon,
    /// One deletion-admission registry per open repo, shared by all clones.
    pub(crate) truncation_pins: Arc<crate::drain::truncation_pin::TruncationPin>,
    /// `Weak` of the shell's repo-liveness token (dead on background clones);
    /// the drainer loop exits once no foreground clone holds the strong ref.
    pub(crate) live_weak: Weak<()>,
    /// Read-side view of the shell's rename fence (same `Arc`s as the shell's).
    pub(crate) rename_fence: RenameFenceView,
    /// F-28 Step 4 (#831): cached per-repo reverse-FK map (parent table name
    /// → its reverse-FK entries) plus the child→parents reverse-reverse
    /// index, replacing the O(tables) `collect_fk_refs()` scan
    /// `fk_restrict.rs`/`fk_actions.rs` used to repeat on EVERY
    /// delete/cascade-recursion-level. Lazily populated (cache-aside) and
    /// invalidated wholesale on any schema mutation that can change what
    /// that scan would discover — see [`FkReverseCache::invalidate`] and its
    /// callers (`add_table`, `remove_table`,
    /// `ShamirDb::compile_table_schema`). Shared across clones (one cache
    /// per underlying repo, not per handle).
    pub(crate) fk_reverse_cache: Arc<FkReverseCache>,
    pub(crate) receipt_worker: Arc<tokio::sync::OnceCell<Arc<ReceiptLease>>>,
    /// Fixed-size foreground lifecycle token; background clones omit it.
    pub(crate) worker_lifecycle:
        Option<Arc<ForegroundLifecycle<ReceiptApplyJob, DbResult<ApplyOutcome>>>>,
    /// Per-open cache for the completed-reseed lifecycle record (see `durability_repo_impl.rs`).
    pub(crate) replication_armed: Arc<std::sync::atomic::AtomicBool>,
    /// Lifecycle owner of cold-table opens handed off by dropped callers
    /// (`HandoffOnDrop` in `catalog_access.rs`); shutdown drains it before
    /// commit quiescence (`clean_horizon_shutdown_impl.rs`).
    pub(crate) cold_opens: Arc<crate::commit::cold_open::ColdOpenTasks>,
    pub(crate) wal_segment_bytes: u64,
    #[cfg(any(test, feature = "test-util"))]
    pub journal_publish_hook: Arc<arc_swap::ArcSwapOption<JournalPublishHook>>,
    #[cfg(any(test, feature = "test-util"))]
    pub table_init_hook: Arc<arc_swap::ArcSwapOption<TableInitHook>>,
    #[cfg(any(test, feature = "test-util"))]
    pub(crate) clean_horizon_test_state:
        Arc<std::sync::OnceLock<Arc<std::sync::Mutex<test_seam::State>>>>,
    #[cfg(any(test, feature = "test-util"))]
    pub(crate) background_drainer_disabled: Arc<std::sync::atomic::AtomicBool>,
}

impl RepoCore {
    /// Construct the coordinator from the shell-owned shared-state bundles.
    pub fn from_shell(
        name: String,
        repo: BoxRepo,
        catalog: TableCatalog,
        durability_handle: DurabilityHandle,
        horizon: TxHorizon,
        live_weak: Weak<()>,
        rename_fence: RenameFenceView,
        wal_segment_bytes: u64,
    ) -> Self {
        Self {
            name,
            repo,
            catalog,
            durability_handle,
            horizon,
            truncation_pins: Arc::new(crate::drain::truncation_pin::TruncationPin::new()),
            live_weak,
            rename_fence,
            fk_reverse_cache: Arc::new(FkReverseCache::new()),
            receipt_worker: Arc::new(tokio::sync::OnceCell::new()),
            worker_lifecycle: Some(Arc::new(ForegroundLifecycle::new())),
            replication_armed: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            cold_opens: Arc::new(crate::commit::cold_open::ColdOpenTasks::new()),
            wal_segment_bytes,
            #[cfg(any(test, feature = "test-util"))]
            journal_publish_hook: Arc::new(arc_swap::ArcSwapOption::empty()),
            #[cfg(any(test, feature = "test-util"))]
            table_init_hook: Arc::new(arc_swap::ArcSwapOption::empty()),
            #[cfg(any(test, feature = "test-util"))]
            clean_horizon_test_state: Arc::new(std::sync::OnceLock::new()),
            #[cfg(any(test, feature = "test-util"))]
            background_drainer_disabled: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    pub fn repo(&self) -> &BoxRepo {
        &self.repo
    }
    pub fn catalog(&self) -> &TableCatalog {
        &self.catalog
    }
    pub fn durability_handle(&self) -> &DurabilityHandle {
        &self.durability_handle
    }
    pub fn horizon(&self) -> &TxHorizon {
        &self.horizon
    }
    pub fn set_commit_visibility_wait(&self, wait: VisibilityWait) {
        self.horizon.set_commit_visibility_wait(wait);
    }
    pub(crate) fn fk_reverse_cache(&self) -> &Arc<FkReverseCache> {
        &self.fk_reverse_cache
    }
    pub fn rename_fence(&self) -> &RenameFenceView {
        &self.rename_fence
    }
    pub fn cold_opens(&self) -> &Arc<crate::commit::cold_open::ColdOpenTasks> {
        &self.cold_opens
    }
    pub fn replication_armed(&self) -> &Arc<std::sync::atomic::AtomicBool> {
        &self.replication_armed
    }
    pub fn receipt_worker(&self) -> &Arc<tokio::sync::OnceCell<Arc<ReceiptLease>>> {
        &self.receipt_worker
    }
    pub fn worker_lifecycle(
        &self,
    ) -> Option<&Arc<ForegroundLifecycle<ReceiptApplyJob, DbResult<ApplyOutcome>>>> {
        self.worker_lifecycle.as_ref()
    }
    pub fn wal_segment_bytes(&self) -> u64 {
        self.wal_segment_bytes
    }
    pub fn set_wal_segment_bytes(&mut self, bytes: u64) {
        self.wal_segment_bytes = bytes;
    }
    pub fn set_name(&mut self, name: String) {
        self.name = name;
    }
}

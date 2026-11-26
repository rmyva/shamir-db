use super::tx_horizon::TxHorizon;
use crate::repo::fk_reverse_cache::FkReverseCache;
use crate::repo::repo_types::BoxRepo;
#[cfg(test)]
use crate::repo::tests::table_init_retry_tests::TableInitHook;
use crate::repo::DurabilityHandle;
use crate::table::TableCatalog;
use crate::tx::apply_replicated::follower_durability::receipt::{
    ApplyOutcome, ReceiptApplyJob, ReceiptLease,
};
#[cfg(test)]
use crate::tx::clean_horizon::checkpoint::test_seam;
use crate::tx::cold_reconcile::lifecycle::ForegroundLifecycle;
use crate::tx::commit::rename_fence::RenameFenceView;
#[cfg(test)]
use crate::tx::tests::journal_publish_order_tests::JournalPublishHook;
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
    pub(crate) truncation_pins: Arc<crate::tx::drain::truncation_pin::TruncationPin>,
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
    pub(crate) cold_opens: Arc<crate::tx::commit::cold_open::ColdOpenTasks>,
    pub(crate) wal_segment_bytes: u64,
    #[cfg(test)]
    pub(crate) journal_publish_hook: Arc<arc_swap::ArcSwapOption<JournalPublishHook>>,
    #[cfg(test)]
    pub(crate) table_init_hook: Arc<arc_swap::ArcSwapOption<TableInitHook>>,
    #[cfg(test)]
    pub(crate) clean_horizon_test_state:
        Arc<std::sync::OnceLock<Arc<std::sync::Mutex<test_seam::State>>>>,
    #[cfg(test)]
    pub(crate) background_drainer_disabled: Arc<std::sync::atomic::AtomicBool>,
}

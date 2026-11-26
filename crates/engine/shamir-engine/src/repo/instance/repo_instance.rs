use crate::repo::instance::support::metrics::{EngineMetrics, EngineMetricsSnapshot};
use crate::repo::journal::backend::durability_handle::retention_gate::RetentionGate;
use crate::repo::repo_types::BoxRepo;
pub use crate::repo::version_provider::repo_token;
// R2a-1d: the tx-layer test tree names the typed barrier failures. R2a
// step 3a (server adapter) additionally needs to NAME `SealHold`,
// `OnlineBackupError` (barrier failures / seal holds) and
// `TruncationPinGuard` (step-A pins held until step G) — pure re-exports,
// no semantics changed.
use crate::repo::DurabilityHandle;
use crate::table::{TableCatalog, TableConfig, TableManager};
use crate::tx::commit::core::repo_core::RepoCoreInit;
use crate::tx::commit::pipeline::admission::gate::CommitAdmissionGate;
pub use crate::tx::drain::truncation_pin_guard::TruncationPinGuard;
use crate::tx::{CommitTailBarrier, RepoCore};
pub use lifecycle::{OnlineBackupError, SealHold};
use shamir_query_types::batch::VisibilityWait;
#[cfg(test)]
use shamir_storage::error::DbResult;
use shamir_types::types::common::{new_dash_map_wc, TDashMap, THasher};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use tokio::sync::OnceCell;

pub(in crate::repo) mod catalog;
mod lifecycle;
pub(crate) mod table_store;
mod transactions;

pub(crate) use crate::repo::journal::backend::durability_handle::ChangefeedHandle;
// Cold opens handed off by dropped callers (`tx/commit/core/catalog_access.rs`)
// register here; `RepoInstance::shutdown_clean` drains the owner before
// commit quiescence.

/// Manages a single repository and its tables.
///
/// Trusted native-embedding handle; raw operations do not authorize an actor.
/// Use [`crate::query::batch::Authorized`] with the `shamir-db` facade's
/// live ACL gate for authorization-enforced batch execution.
pub struct RepoInstance {
    name: String,
    /// D2 P1d-2b: repo liveness token. `Some(Arc<()>)` on every FOREGROUND
    /// clone (the strong count == number of live foreground clones); `None`
    /// on the single BACKGROUND clone the drain task owns
    /// (`RepoCore::clone_for_background`) so the task does
    /// NOT keep the repo alive. The drain loop holds a `Weak<()>` of this and
    /// exits when the strong count reaches zero.
    live: Option<Arc<()>>,
    /// R2c1: the tx-layer aggregate of this repo's shared cells; every moved accessor reads through it.
    pub(crate) core: crate::tx::commit::core::RepoCore,
    /// In-doubt rename entries indexed by both involved names.
    rename_fence:
        Arc<scc::HashMap<String, crate::tx::commit::rename_fence::RenameFenceEntry, THasher>>,
    /// O(1) count of owned source fence entries.
    rename_fence_len: Arc<AtomicUsize>,
    metrics: Arc<EngineMetrics>,
}

impl Clone for RepoInstance {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            // Foreground clone: carries a strong liveness ref. If `self` is
            // itself the background clone (`None`) this stays `None` — a clone
            // of the background handle must not resurrect liveness.
            live: self.live.clone(),
            core: self.core.clone(),
            rename_fence: Arc::clone(&self.rename_fence),
            rename_fence_len: Arc::clone(&self.rename_fence_len),
            metrics: Arc::clone(&self.metrics),
        }
    }
}

impl RepoInstance {
    /// Cold-path copy of this repository's query telemetry.
    pub fn query_metrics_snapshot(&self) -> EngineMetricsSnapshot {
        self.metrics.snapshot()
    }

    /// Shared per-repository metrics for downward query-path instrumentation.
    pub fn query_metrics(&self) -> &Arc<EngineMetrics> {
        &self.metrics
    }

    /// Construct an unregistered low-level handle. Reused backing stores MUST
    /// run recover_v2_inflight before any caller serves reads/writes. Registry
    /// entry points enforce that ordering; this synchronous fixture constructor
    /// cannot perform asynchronous recovery.
    pub fn new(name: String, repo: BoxRepo, configs: Vec<TableConfig>) -> Self {
        Self::from_box_repo(name, repo, configs)
    }

    fn from_box_repo(name: String, repo: BoxRepo, configs: Vec<TableConfig>) -> Self {
        // In-memory / test path: no disk backing → KV-marker WAL.
        Self::from_box_repo_with_wal_dir(name, repo, configs, None)
    }

    pub(in crate::repo::instance::repo_instance) fn from_box_repo_with_wal_dir(
        name: String,
        repo: BoxRepo,
        configs: Vec<TableConfig>,
        wal_dir: Option<std::path::PathBuf>,
    ) -> Self {
        let initial_cap = configs.len().max(16);
        let configs_map: TDashMap<String, TableConfig> = new_dash_map_wc(initial_cap);
        let token_names: scc::HashMap<u64, String, THasher> =
            scc::HashMap::with_capacity_and_hasher(initial_cap, THasher::default());
        for cfg in configs {
            TableCatalog::register_token_in(&token_names, &cfg.name);
            configs_map.insert(cfg.name.clone(), cfg);
        }

        let tables: TDashMap<String, Arc<OnceCell<TableManager>>> = new_dash_map_wc(100);

        let catalog = TableCatalog::new(
            configs_map,
            tables,
            token_names,
            scc::HashMap::with_capacity_and_hasher(initial_cap, THasher::default()),
            OnceCell::new(),
        );
        let policy = crate::repo::durability_policy::DurabilityPolicy::default()
            .for_wal_backend(wal_dir.is_some());
        let durability_handle = DurabilityHandle {
            repo_wal: Arc::new(OnceCell::new()),
            wal_group: Arc::new(OnceCell::new()),
            group_commit: Arc::new(crate::repo::group_commit::GroupCommit::new()),
            changefeed: Arc::new(OnceCell::new()),
            pending_changefeed_gap: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            wal_dir,
            durability: Arc::new(arc_swap::ArcSwap::from_pointee(policy)),
            durability_metrics: Arc::new(
                crate::repo::durability_policy::DurabilityMetrics::default(),
            ),
            wal_retention_gate: Arc::new(RetentionGate::default()),
            marker_write_mutex: Arc::new(tokio::sync::Mutex::new(())),
            write_domain: crate::repo::changelog_store::write_domain::RepoWriteDomain::new(
                name.clone(),
            ),
        };
        let horizon = crate::tx::commit::core::TxHorizon::new(
            Arc::new(OnceCell::new()),
            Arc::new(shamir_tx::TxMetrics::new()),
            Arc::new(CommitTailBarrier::default()),
            Arc::new(CommitAdmissionGate::default()),
            Arc::new(std::sync::OnceLock::new()),
            VisibilityWait::Strict,
        );
        let live = Arc::new(());
        let rename_fence = Arc::new(scc::HashMap::with_hasher(THasher::default()));
        let rename_fence_len = Arc::new(AtomicUsize::new(0));
        let core = RepoCore::from_shell(RepoCoreInit {
            name: name.clone(),
            repo,
            catalog,
            durability_handle,
            horizon,
            live_weak: Arc::downgrade(&live),
            rename_fence: crate::tx::commit::rename_fence::RenameFenceView::new(
                Arc::clone(&rename_fence),
                Arc::clone(&rename_fence_len),
            ),
            wal_segment_bytes: crate::repo::durability_policy::effective_wal_segment_bytes(),
        });
        Self {
            name,
            live: Some(live),
            core,
            rename_fence,
            rename_fence_len,
            metrics: Arc::new(EngineMetrics::default()),
        }
    }
}

/// tx-facing seam (`tx/recovery/clean_horizon/checkpoint.rs` imports
/// `crate::repo::durability_policy::checkpoint_allocation_floor`). Resolves
/// the journal store and delegates to the repo-layer core.
#[cfg(test)]
pub(crate) async fn checkpoint_allocation_floor(repo: &RepoInstance) -> DbResult<u64> {
    repo.core.checkpoint_allocation_floor().await
}

//! Persistent system store for ShamirDB metadata.
//!
//! Stores databases, repositories, settings, users, roles.
//! Uses a TableManager backed by any storage engine (fjall for production,
//! in_memory for tests).

use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwapOption;
use shamir_collections::TFxMap;
use shamir_types::access::Actor;
use shamir_types::types::common::new_map;
use shamir_types::types::value::QueryValue;

use crate::engine::db_instance::db_instance::DbInstance;
use crate::engine::repo::repo_types::BoxRepoFactory;
use crate::engine::repo::{DurabilityPolicy, RepoConfig, RepoInstance};
use crate::engine::table::{TableConfig, TableManager};
use crate::{DbError, DbResult};

pub(super) const SYSTEM_REPO: &str = "system";

/// System store tables
pub(super) const TABLE_DATABASES: &str = "databases";
pub(super) const TABLE_REPOSITORIES: &str = "repositories";
/// Per-repo table catalogue: one record per user table so the table
/// list survives a restart and crash-recovery can resolve `table_by_token`
/// for disk-backed repos (I.2).
pub(super) const TABLE_TABLES: &str = "tables";
pub(super) const TABLE_SETTINGS: &str = "settings";
pub(super) const TABLE_USERS: &str = "users";
pub(super) const TABLE_ROLES: &str = "roles";
/// Function catalogue: one record per user-defined WASM function so the
/// function survives a restart (slice 4).
pub(super) const TABLE_FUNCTIONS: &str = "functions";
/// Groups catalogue: group_id → (name, members).
/// Group ids are allocated monotonically starting from 1 (stored in
/// the `settings` table under the key `"next_group_id"`). Id 0 is
/// reserved / unused.
pub(super) const TABLE_GROUPS: &str = "groups";
/// Validator catalogue: one record per user-defined WASM validator so
/// the validator survives a restart (S1).
pub(super) const TABLE_VALIDATORS: &str = "validators";
/// Function folder catalogue: one record per explicitly created folder
/// (e.g. `"reports/daily"`). Key is the slash-joined path. Persists
/// ResourceMeta (owner/group/mode) so folder ACLs survive a restart (#118).
pub(super) const TABLE_FUNCTION_FOLDERS: &str = "function_folders";
/// Replication profile catalogue: one record per named profile. Keyed by
/// `name`; carries the profile's `streams` bundle (386-a).
pub(super) const TABLE_REPLICATION_PROFILES: &str = "replication_profiles";
/// Publication catalogue: one record per publication. Keyed by `name`;
/// carries the published `scopes` (386-a).
pub(super) const TABLE_PUBLICATIONS: &str = "publications";
/// Subscription catalogue: one record per subscription. Keyed by `name`;
/// carries `upstream`/`publication`/`profile`/`state` (386-a). The follower
/// pull-loop that consumes these is 386-b.
pub(super) const TABLE_SUBSCRIPTIONS: &str = "subscriptions";
/// Append-only application schema migration event records.
pub(super) const TABLE_SCHEMA_MIGRATION_EVENTS: &str = "schema_migration_events";

/// Configuration for the system store.
#[derive(Clone)]
pub enum SystemStoreConfig {
    /// In-memory (for tests). Data lost on restart.
    InMemory,
    /// Persistent fjall at the given path.
    Fjall(std::path::PathBuf),
}

/// Persistent system store.
#[derive(Clone)]
pub struct SystemStore {
    db: DbInstance,
    durable_journal: bool,
    /// Shared admin-operation admission for cloned SystemStore handles. This does not serialize
    /// migrations across databases or processes; the durable per-database CAS control record is
    /// the fencing authority. Server-wide lifetime admission belongs to step 2b.
    schema_migration_admin: Arc<tokio::sync::Mutex<()>>,
    /// Test seam: a 1-indexed countdown — `arm(n)` makes the n-th SUBSEQUENT
    /// gated catalogue write fail (see `catalogue_write_fault_gate`); armed by
    /// catalogue fault-injection tests.
    catalogue_write_faults: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    durability_tail_faults: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    /// Test seam: durable tails that pass before `durability_tail_faults` starts failing.
    durability_tail_skips: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    #[cfg(test)]
    table_catalogue_reads: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    #[cfg(test)]
    catalogue_accesses: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    #[cfg(test)]
    full_catalogue_loads: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    #[cfg(test)]
    group_name_cache_lookups: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    pub(super) group_names: Arc<ArcSwapOption<super::group_names::GroupNames>>,
    // DDL-only rebuild lock; group-catalogue writes are rare.
    pub(super) group_names_rebuild: Arc<tokio::sync::Mutex<()>>,
    #[cfg(test)]
    function_catalogue_reads: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    /// Test-only count of settings-catalogue reads.
    #[cfg(test)]
    pub(super) invocation_settings_reads: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    /// Test-only count of validator-catalogue reads.
    #[cfg(test)]
    validator_catalogue_reads: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    /// Test-only count of `load_function_folder_path` ancestor lookups
    /// (A7B1FCEC deep-chain allocation oracle).
    #[cfg(test)]
    folder_path_lookups: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

/// Build a `QueryValue::Map` with a single string-keyed entry (for simple
/// primary-key records).
pub(super) fn qv_map1(k1: &str, v1: QueryValue) -> QueryValue {
    let mut m = new_map();
    m.insert(k1.to_string(), v1);
    QueryValue::Map(m)
}

/// Build a `QueryValue::Map` with two string-keyed entries.
pub(super) fn qv_map2(k1: &str, v1: QueryValue, k2: &str, v2: QueryValue) -> QueryValue {
    let mut m = new_map();
    m.insert(k1.to_string(), v1);
    m.insert(k2.to_string(), v2);
    QueryValue::Map(m)
}

impl SystemStore {
    #[cfg(any(test, feature = "test-util"))]
    #[doc(hidden)]
    pub fn arm_durability_tail_faults(&self, n: usize) {
        self.durability_tail_faults
            .store(n, std::sync::atomic::Ordering::SeqCst);
    }

    /// Let the next `skip` durable tails pass, then fail the following `n` of them.
    #[cfg(any(test, feature = "test-util"))]
    #[doc(hidden)]
    pub fn arm_durability_tail_faults_after(&self, skip: usize, n: usize) {
        self.durability_tail_skips
            .store(skip, std::sync::atomic::Ordering::SeqCst);
        self.arm_durability_tail_faults(n);
    }

    fn durability_tail_fault_gate(&self) -> DbResult<()> {
        let mut skips = self
            .durability_tail_skips
            .load(std::sync::atomic::Ordering::SeqCst);
        while skips != 0 {
            match self.durability_tail_skips.compare_exchange(
                skips,
                skips - 1,
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
            ) {
                Ok(_) => return Ok(()),
                Err(actual) => skips = actual,
            }
        }
        let mut remaining = self
            .durability_tail_faults
            .load(std::sync::atomic::Ordering::SeqCst);
        while remaining != 0 {
            match self.durability_tail_faults.compare_exchange(
                remaining,
                remaining - 1,
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
            ) {
                Ok(_) => {
                    return Err(DbError::Storage("injected durability-tail failure".into()));
                }
                Err(actual) => remaining = actual,
            }
        }
        Ok(())
    }

    /// Initialize system store with the given config.
    pub async fn init(config: SystemStoreConfig) -> DbResult<Self> {
        Self::init_with_durability(config, DurabilityPolicy::default()).await
    }

    /// Open the metadata repository with the same policy as user repositories.
    pub(crate) async fn init_with_durability(
        config: SystemStoreConfig,
        policy: DurabilityPolicy,
    ) -> DbResult<Self> {
        let db = DbInstance::new();

        let durable_journal = matches!(&config, SystemStoreConfig::Fjall(_));
        let factory = match config {
            SystemStoreConfig::InMemory => BoxRepoFactory::in_memory(),
            #[cfg(feature = "fjall")]
            SystemStoreConfig::Fjall(path) => BoxRepoFactory::fjall(path),
            #[cfg(not(feature = "fjall"))]
            SystemStoreConfig::Fjall(_) => {
                return Err(DbError::Storage(
                    "system store requires unavailable fjall backend".into(),
                ))
            }
        };

        let repo_config = RepoConfig::new(SYSTEM_REPO, factory)
            .add_table(TableConfig::new(TABLE_DATABASES))
            .add_table(TableConfig::new(TABLE_REPOSITORIES))
            .add_table(TableConfig::new(TABLE_TABLES))
            .add_table(TableConfig::new(TABLE_SETTINGS))
            .add_table(TableConfig::new(TABLE_USERS))
            .add_table(TableConfig::new(TABLE_ROLES))
            .add_table(TableConfig::new(TABLE_FUNCTIONS))
            .add_table(TableConfig::new(TABLE_GROUPS))
            .add_table(TableConfig::new(TABLE_VALIDATORS))
            .add_table(TableConfig::new(TABLE_FUNCTION_FOLDERS))
            .add_table(TableConfig::new(TABLE_REPLICATION_PROFILES))
            .add_table(TableConfig::new(TABLE_PUBLICATIONS))
            .add_table(TableConfig::new(TABLE_SUBSCRIPTIONS))
            .add_table(TableConfig::new(TABLE_SCHEMA_MIGRATION_EVENTS).with_indexes());

        // DbInstance recovers/certifies before exposing the system repo.
        db.add_repo_with_durability(repo_config, policy).await?;
        let journal = db
            .get_table(SYSTEM_REPO, TABLE_SCHEMA_MIGRATION_EVENTS)
            .await?;
        if !journal
            .unique_index_exists("schema_migration_record_key")
            .await
        {
            journal
                .create_unique_index("schema_migration_record_key", &["record_key"])
                .await?;
        }

        let store = Self {
            db,
            durable_journal,
            schema_migration_admin: Arc::new(tokio::sync::Mutex::new(())),
            catalogue_write_faults: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            durability_tail_faults: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            durability_tail_skips: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            #[cfg(test)]
            table_catalogue_reads: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            #[cfg(test)]
            catalogue_accesses: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            #[cfg(test)]
            full_catalogue_loads: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            #[cfg(test)]
            group_name_cache_lookups: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            #[cfg(test)]
            function_catalogue_reads: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            group_names: Arc::new(ArcSwapOption::empty()),
            group_names_rebuild: Arc::new(tokio::sync::Mutex::new(())),
            #[cfg(test)]
            invocation_settings_reads: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            #[cfg(test)]
            validator_catalogue_reads: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            #[cfg(test)]
            folder_path_lookups: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        };
        store.rebuild_group_names().await;
        Ok(store)
    }

    #[cfg(test)]
    pub(crate) fn invocation_settings_read_count(&self) -> usize {
        self.invocation_settings_reads
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    #[cfg(test)]
    pub(crate) fn reset_invocation_settings_reads(&self) {
        self.invocation_settings_reads
            .store(0, std::sync::atomic::Ordering::SeqCst);
    }

    #[cfg(test)]
    pub(crate) fn reset_validator_catalogue_reads(&self) {
        self.validator_catalogue_reads
            .store(0, std::sync::atomic::Ordering::SeqCst);
    }

    #[cfg(test)]
    pub(crate) fn validator_catalogue_read_count(&self) -> usize {
        self.validator_catalogue_reads
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    #[cfg(test)]
    pub(crate) fn count_validator_catalogue_read(&self) {
        self.validator_catalogue_reads
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
    #[cfg(test)]
    pub(crate) fn reset_function_catalogue_reads(&self) {
        self.function_catalogue_reads
            .store(0, std::sync::atomic::Ordering::SeqCst);
    }

    #[cfg(test)]
    pub(crate) fn function_catalogue_read_count(&self) -> usize {
        self.function_catalogue_reads
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    #[cfg(test)]
    pub(crate) fn count_function_catalogue_read(&self) {
        self.function_catalogue_reads
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    /// Arm the catalogue write-fault seam: the n-th subsequent gated
    /// catalogue write operation (1-indexed countdown over EVERY gated
    /// write, not per-call) fails with `DbError::Internal`. Test-only.
    ///
    /// Gated `#[cfg(any(test, feature = "test-util"))]` with `#[doc(hidden)]`
    /// (RV-DB-6540DC7B follow-up): shamir-server's dev-dependency enables
    /// `shamir-db/test-util`, so its bootstrap-failure tests arm the seam on
    /// `shamir.system_store()`. Production default features never see it —
    /// no default-featured public API is widened.
    #[cfg(any(test, feature = "test-util"))]
    #[doc(hidden)]
    pub fn arm_catalogue_write_fault(&self, n: usize) {
        self.catalogue_write_faults
            .store(n, std::sync::atomic::Ordering::SeqCst);
    }

    #[cfg(test)]
    pub(crate) fn reset_table_catalogue_reads(&self) {
        self.table_catalogue_reads
            .store(0, std::sync::atomic::Ordering::SeqCst);
    }

    #[cfg(test)]
    pub(crate) fn table_catalogue_read_count(&self) -> usize {
        self.table_catalogue_reads
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    #[cfg(test)]
    pub(crate) fn count_table_catalogue_read(&self) {
        self.table_catalogue_reads
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    #[cfg(test)]
    pub(crate) fn folder_path_lookup_count(&self) -> usize {
        self.folder_path_lookups
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    #[cfg(test)]
    pub(crate) fn count_folder_path_lookup(&self) {
        self.folder_path_lookups
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    /// Consulted by every catalogue write below; the armed countdown reaches
    /// 1 on the armed write and fails it (arm(n) ⇒ the n-th write fails).
    pub(crate) fn catalogue_write_fault_gate(&self) -> DbResult<()> {
        let remaining = self
            .catalogue_write_faults
            .load(std::sync::atomic::Ordering::SeqCst);
        if remaining == 0 {
            return Ok(());
        }
        self.catalogue_write_faults
            .store(remaining - 1, std::sync::atomic::Ordering::SeqCst);
        if remaining == 1 {
            return Err(DbError::Internal(
                "injected catalogue write failure".to_string(),
            ));
        }
        Ok(())
    }

    /// Drain the system repo's inflight WAL tail into `history` and flush its
    /// buffers (graceful-stop counterpart of the per-repo flush in
    /// [`ShamirDb::flush_all`](crate::ShamirDb::flush_all)).
    /// cancel-safe: NO — delegates to bounded repo shutdown; a checkpoint
    /// already in backend I/O can finish without acknowledging this caller.
    pub(crate) async fn flush_buffers_with_quiesce_timeout(
        &self,
        timeout: Duration,
    ) -> DbResult<()> {
        let repo = self.system_repo()?;
        repo.shutdown_clean(timeout).await
    }

    #[cfg(test)]
    pub(crate) fn count_full_catalogue_load(&self) {
        self.full_catalogue_loads
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    #[cfg(test)]
    pub(crate) fn catalogue_access_count(&self) -> usize {
        self.catalogue_accesses
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    #[cfg(test)]
    pub(crate) fn full_catalogue_load_count(&self) -> usize {
        self.full_catalogue_loads
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Test-only: unpublish the group-name snapshot, the state a failed group
    /// write or rebuild leaves behind, so name resolution reads the live
    /// catalogue (and therefore sees a storage fault).
    #[cfg(test)]
    pub(crate) fn unpublish_group_names_for_test(&self) {
        self.group_names.store(None);
    }

    #[cfg(test)]
    pub(crate) fn group_name_cache_lookup_count(&self) -> usize {
        self.group_name_cache_lookups
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    #[cfg(test)]
    pub(crate) fn reset_catalogue_counts(&self) {
        self.catalogue_accesses
            .store(0, std::sync::atomic::Ordering::SeqCst);
        self.full_catalogue_loads
            .store(0, std::sync::atomic::Ordering::SeqCst);
        self.group_name_cache_lookups
            .store(0, std::sync::atomic::Ordering::SeqCst);
        self.folder_path_lookups
            .store(0, std::sync::atomic::Ordering::SeqCst);
    }

    pub(super) async fn rebuild_group_names(&self) {
        let _guard = self.group_names_rebuild.lock().await;
        self.rebuild_group_names_locked().await;
    }

    pub(super) async fn rebuild_group_names_locked(&self) {
        let Ok(groups) = self.load_groups().await else {
            self.group_names.store(None);
            return;
        };
        let mut by_name = TFxMap::default();
        let mut by_id = TFxMap::default();
        for group in groups {
            let (Some(name), Some(id)) = (
                group.get("name").and_then(|value| value.as_str()),
                group.get("group_id").and_then(|value| value.as_u64()),
            ) else {
                continue;
            };
            by_name.entry(name.to_string()).or_insert(id);
            by_id.insert(id, name.to_string());
        }
        self.group_names
            .store(Some(Arc::new(super::group_names::GroupNames {
                by_name,
                by_id,
            })));
    }

    pub(crate) fn group_id_by_name(&self, name: &str) -> Option<Option<u64>> {
        #[cfg(test)]
        self.group_name_cache_lookups
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.group_names
            .load_full()
            .map(|snapshot| snapshot.by_name.get(name).copied())
    }

    /// Get the table manager for a system table.
    pub(super) async fn table(&self, name: &str) -> DbResult<TableManager> {
        #[cfg(test)]
        self.catalogue_accesses
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.db.get_table(SYSTEM_REPO, name).await
    }

    /// Reference durable DDL tail for one system-catalogue table: fsync the
    /// SYSTEM repo's file WAL first, THEN flush the table's data store. Every
    /// DDL acknowledgement routes through this so the "Durable DDL" intent
    /// has one implementation. sync_wal runs FIRST so the armed-flush fault
    /// tests exercise exactly the final `flush()` line.
    pub(crate) async fn flush_system_repo_durable(&self, table: &TableManager) -> DbResult<()> {
        self.durability_tail_fault_gate()?;
        self.system_repo()?.sync_wal().await?;
        table.data_store_read_view().flush().await
    }

    /// Resolve the SYSTEM_REPO [`RepoInstance`].
    ///
    /// F5a: system-store DELETEs route through the repo's implicit-tx
    /// file-WAL path (`run_implicit_batch_tx` + `execute_delete_tx`) instead
    /// of the direct V1-marker `execute_delete`, so the V1 DELETE marker
    /// becomes dead.
    pub(crate) fn system_repo(&self) -> DbResult<RepoInstance> {
        self.db
            .get_repo(SYSTEM_REPO)
            .ok_or_else(|| DbError::NotFound(format!("Repository '{}' not found", SYSTEM_REPO)))
    }

    /// Admitted system-catalogue durability tail (F1/R6; merged with the
    /// R7-L3 interning twin in Round 8): ONE admission on the SYSTEM repo's
    /// write domain (the tokio write-domain mutex is non-reentrant),
    /// optional `intern_query_value_admitted` interning of `intern`
    /// (replaces the old read-view `to_inner` WRITE-prep interning step),
    /// then `persist_interner_admitted` of the dictionary delta.
    /// `None` = persist-only call sites.
    pub(crate) async fn persist_table_interner(
        &self,
        table: &TableManager,
        intern: Option<&QueryValue>,
    ) -> DbResult<()> {
        let repo = self.system_repo()?;
        let permit = repo
            .write_domain()
            .admit()
            .await
            .map_err(|e| DbError::Internal(e.to_string()))?;
        // Round-12 (stack layout): the admitted tail (optional interning +
        // dictionary persist) is pinned so its await state stays on the heap
        // instead of widening this fn's future — `persist_table_interner` is
        // awaited inline in every admin-catalogue writer, including
        // `handle_alter_subscription`'s RESUME arm on the main thread.
        Box::pin(async move {
            if let Some(record) = intern {
                table.intern_query_value_admitted(&permit, record).await?;
            }
            table.persist_interner_admitted(&permit).await
        })
        .await
    }

    /// Route a single non-tx DELETE through the implicit-tx file-WAL path
    /// (mirrors the `query_runner` non-tx Delete branch). Maps the
    /// [`BatchError`] surfaced by the implicit tx onto a [`DbError`].
    pub(super) async fn delete_via_implicit_tx(
        &self,
        table: &TableManager,
        op: &crate::query::write::DeleteOp,
    ) -> DbResult<crate::query::write::WriteResult> {
        let repo = self.system_repo()?;
        let owned_op = op.clone();
        let owned_table = table.clone();
        repo.run_implicit_batch_tx(Actor::System, "", move |tx| {
            Box::pin(async move {
                let refs = crate::types::common::new_map();
                let ctx = owned_table
                    .info_store_read_view()
                    .filter_context(&refs)
                    .await?;
                owned_table
                    .execute_delete_tx(&owned_op, &ctx, tx, None, &Actor::System)
                    .await
            })
        })
        .await
        .map_err(|e| DbError::Internal(e.to_string()))
    }

    /// Route a single non-tx SET (upsert) through the implicit-tx file-WAL
    /// path (mirrors the `query_runner` non-tx Set branch, W3d-2). Maps the
    /// [`BatchError`] surfaced by the implicit tx onto a [`DbError`].
    pub(crate) async fn set_via_implicit_tx(
        &self,
        table: &TableManager,
        op: &crate::query::write::SetOp,
    ) -> DbResult<crate::query::write::WriteResult> {
        let repo = self.system_repo()?;
        let owned_op = op.clone();
        let owned_table = table.clone();
        repo.run_implicit_batch_tx(Actor::System, "", move |tx| {
            Box::pin(async move {
                owned_table
                    .execute_set_tx(&owned_op, tx, None, &Actor::System)
                    .await
            })
        })
        .await
        .map_err(|e| DbError::Internal(e.to_string()))
    }

    /// Refuse schema-migration journal writes on an in-memory system store.
    pub(crate) fn require_durable_schema_migration_store(&self) -> DbResult<()> {
        if self.durable_journal {
            Ok(())
        } else {
            Err(DbError::Storage(
                "schema migration journal requires a persistent system store".into(),
            ))
        }
    }

    pub(crate) async fn schema_migration_admin_lock(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.schema_migration_admin.lock().await
    }

    /// Route a single non-tx UPDATE through the implicit-tx file-WAL
    /// path. Maps the [`BatchError`] surfaced by the implicit tx onto a [`DbError`].
    pub(crate) async fn update_via_implicit_tx(
        &self,
        table: &TableManager,
        op: &crate::query::write::UpdateOp,
    ) -> DbResult<crate::query::write::WriteResult> {
        let repo = self.system_repo()?;
        let owned_op = op.clone();
        let owned_table = table.clone();
        repo.run_implicit_batch_tx(Actor::System, "", move |tx| {
            Box::pin(async move {
                let refs = crate::types::common::new_map();
                let ctx = owned_table
                    .info_store_read_view()
                    .filter_context(&refs)
                    .await?;
                owned_table
                    .execute_update_tx(&owned_op, &ctx, tx, None, &Actor::System)
                    .await
            })
        })
        .await
        .map_err(|e| DbError::Internal(e.to_string()))
    }

    // Database metadata
}

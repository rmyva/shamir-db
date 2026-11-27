use base64::Engine;

use crate::access::Actor;
use crate::engine::db_instance::db_instance::DbInstance;
use crate::repl_binding::SubscriptionLease;
use crate::{DbError, DbResult};
use dashmap::DashMap;
use shamir_collections::THasher;
use shamir_engine::function::{
    EnvPolicy, FnCtx, FunctionMeta, FunctionRegistry, GlobalVars, StoredCallResolver, WasmEngine,
    WasmFunction, WasmLimits,
};
use shamir_engine::repo::durability_policy::{effective_wal_segment_bytes, resolve};
use shamir_engine::validator::{RecordValidator, ValidatorRegistry, WasmRecordValidator};
use shamir_types::types::record_id::RecordId;
use shamir_types::types::value::QueryValue;
use std::sync::Arc;
use tokio::sync::Mutex;

#[cfg(test)]
pub(super) type FunctionResolutionHook = Box<
    dyn Fn(
            ShamirDb,
            String,
            bool,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>
        + Send
        + Sync,
>;

#[cfg(test)]
pub(super) type FunctionDdlHook = Box<
    dyn Fn(FunctionDdlHookPoint) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>
        + Send
        + Sync,
>;

#[cfg(test)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum FunctionDdlHookPoint {
    AlterAfterRowReadBeforeWrite,
    RenameAfterRowReadBeforeWrite,
    RenameAfterCatalogueWritesBeforeRegistryRename,
    MetaAfterRowReadBeforeWrite,
    MarkCorruptBeforeLock,
    MarkCorruptAfterRowReadBeforeRegistryWrite,
    ReplaceAfterRowSaveBeforeRegistryWrite,
}

pub(in crate::shamir_db) const SYSTEM_DB_NAME: &str = "__system__";

use crate::engine::migration::MigrationCoordinator;
use crate::engine::repo::{BoxRepoFactory, DurabilityPolicy, RepoConfig};
use crate::shamir_db::execute::admin_db_schema_migration::FenceState;
use crate::shamir_db::execute::ResyncQuarantineCache;
use crate::shamir_db::system_store::{
    ensure_database_identity, table_config_from_catalogue_record, DatabaseIdentity,
};

use super::super::ports::{PrincipalResolver, UserAdminPort};
use super::super::repo_durability::durability_from_record;
use super::super::system_store::{SystemStore, SystemStoreConfig};
use super::db::{FunctionDdlLocks, FunctionState};

/// Input source for [`ShamirDb::create_function_with_opts`].
pub enum FunctionSource<'a> {
    /// Pre-compiled WASM binary bytes.
    Wasm(&'a [u8]),
    /// Rust source code to compile.
    Source(&'a str),
    /// `shamir` language source, compiled in-process by the host source
    /// service (`lang = "shamir"`; no external toolchain).
    Shamir(&'a str),
}

/// Caller-holds-proof guard for `replication_control_lock`; its crate-private constructor prevents external fabrication.
pub struct ReplicationControlGuard<'a> {
    guard: tokio::sync::MutexGuard<'a, ()>,
}

impl<'a> ReplicationControlGuard<'a> {
    pub(crate) fn new(guard: tokio::sync::MutexGuard<'a, ()>) -> Self {
        Self { guard }
    }

    /// Identity of the held interlock; dropping this guard releases the lock.
    pub fn interlock_id(&self) -> usize {
        std::ptr::from_ref(&*self.guard).addr()
    }
}

/// Top-level manager for multiple database instances.
///
/// Hierarchy: `ShamirDb` → `SystemStore` (persistent metadata) plus named
/// `DbInstance`s (production, analytics, …), each holding `RepoInstance`s
/// (e.g. main, archive), each holding `TableManager`s (e.g. users, logs).
#[derive(Clone)]
pub struct ShamirDb {
    /// Serialises replication catalogue admin ops (admin-only, never a
    /// record hot path). PRIVATE: the ONLY way to acquire it is
    /// [`ShamirDb::lock_replication_control`], which returns the
    /// [`ReplicationControlGuard`] newtype — raw mutex guards no longer
    /// appear in any API signature.
    replication_control_lock: Arc<Mutex<()>>,
    // Startup-only injected transport (written once at server construction, read per
    // Resume): contention is nil, no guard is held across an `.await`.
    pub(crate) replication_resume_probe:
        Arc<std::sync::RwLock<Option<Arc<dyn crate::ResumeProbe>>>>,
    // Startup-only injected online-backup registry (D6; `gateway::online_backup`).
    pub(crate) online_backup_port:
        Arc<std::sync::RwLock<Option<Arc<dyn crate::OnlineBackupAdminPort>>>>,
    pub(super) dbs: Arc<DashMap<String, DbInstance, THasher>>,
    pub(super) system_store: SystemStore,
    /// Per-user lock map used only for schema read-modify-write operations
    /// keyed by `schema_lock_key`; grant/revoke do not acquire this lock.
    /// Entries are retained forever, one per distinct key ever locked, and
    /// inserted only after authorization at admin/DDL frequency. The map is
    /// publicly reachable; eviction could split locking for a live key.
    pub(super) admin_user_locks: Arc<DashMap<String, Arc<Mutex<()>>, THasher>>,
    /// Per-group lock map for group-record read-modify-write operations.
    /// Entries are retained forever, one per distinct group id ever locked,
    /// inserted only after authorization at admin/DDL frequency. The public
    /// accessor exposes it; eviction could split locking for a live id.
    pub(super) group_member_locks: Arc<DashMap<u64, Arc<Mutex<()>>, THasher>>,
    pub(super) active_migrations: Arc<DashMap<String, Arc<MigrationCoordinator>, THasher>>,
    /// Live function registry (builtins + WASM functions loaded on open).
    pub(super) functions: Arc<FunctionRegistry>,
    /// Shared Wasmtime engine used for all WASM function invocations.
    pub(super) wasm_engine: Arc<WasmEngine>,
    /// Database-global variables shared across all function invocations.
    pub(super) globals: Arc<GlobalVars>,
    /// Network egress allowlist (host patterns). Default empty = deny all.
    pub(super) net_allowlist: Arc<super::super::gateway::net_grants::NetGrantsCache>,
    /// Per-function metadata (visibility, security, secret_grants).
    /// Populated on create/load, updated on rename, removed on drop.
    /// Wrapped in `Arc` so all clones share one map — function metadata
    /// is process-global, exactly like `functions`/`globals`.
    pub(super) function_meta: Arc<DashMap<String, FunctionMeta, THasher>>,
    /// Runtime availability per function (loaded, unloadable, corrupt).
    /// Inserted at create/boot, moved on rename, removed on drop: same
    /// lifecycle as `function_meta`.
    pub(super) function_state: Arc<DashMap<String, FunctionState, THasher>>,
    pub(in crate::shamir_db::shamir_db) function_ddl_locks: FunctionDdlLocks,
    /// Serialises group-id allocation so concurrent create_group calls
    /// can't read-modify-write the same `next_group_id`. Group creation is
    /// rare, so holding this across the (bounded) await sequence is fine.
    pub(super) group_id_lock: Arc<Mutex<()>>,
    /// Serialises the whole `handle_create_db` sequence (exists-check →
    /// authorize → create) so two concurrent `CREATE DATABASE` calls for the
    /// SAME name can't both observe "does not exist" and both proceed to
    /// create — see task #546 (create-DB/create-repo TOCTOU). DB creation is
    /// rare, so holding this across the (bounded) await sequence mirrors
    /// `group_id_lock`'s established pattern.
    pub(super) db_create_lock: Arc<Mutex<()>>,
    /// Per-database lock map for repository create/rename admission, keyed by
    /// database name. Entries are retained forever, one per distinct key ever
    /// locked, inserted only after authorization at admin/DDL frequency. The
    /// public accessor exposes it; eviction could split synchronization for a live key.
    pub(super) repo_create_locks: Arc<DashMap<String, Arc<Mutex<()>>, THasher>>,
    /// Live validator registry (compiled WASM validators loaded on open).
    pub(super) validators: Arc<ValidatorRegistry>,
    /// Base directory for durable repos (`None` for in-memory homes).
    pub(super) data_root: Option<std::path::PathBuf>,
    /// Injected write-side user-administration port (task #559); `None`
    /// until `shamir-server` wires its directory-backed impl.
    pub(super) user_admin_port: Option<Arc<dyn UserAdminPort>>,
    /// Injected read-only principal resolver (task #559). `None` for
    /// embedded deployments; then names resolve to `None` (degraded but
    /// safe).
    pub(super) principal_resolver: Option<Arc<dyn PrincipalResolver>>,
    /// Experimental-feature gate for the online storage-engine migration
    /// API (defaults `false`; see `enable_experimental_migration_api`).
    pub(super) experimental_migration_enabled: Arc<std::sync::atomic::AtomicBool>,
    /// Server-default policy, validated at initialization and each repository open.
    pub(super) durability_policy: DurabilityPolicy,
    /// Shared by clones; serializes function-family migration runners server-wide.
    pub(in crate::shamir_db) function_family_migration_gate: Arc<std::sync::atomic::AtomicBool>,
    /// Round-3 finding 4: set while a migration-owned RenameValidator body
    /// holds its foreign fence reservations (`validator/migration_admission.rs`).
    pub(in crate::shamir_db) validator_rename_migration: Arc<std::sync::atomic::AtomicBool>,
    /// R1c §4.3 — stable-identity fence state; vacant means free.
    pub(in crate::shamir_db) schema_migration_fences:
        Arc<DashMap<DatabaseIdentity, FenceState, THasher>>,
    pub(in crate::shamir_db) schema_migration_fence_count: Arc<std::sync::atomic::AtomicUsize>,
    pub(in crate::shamir_db) database_identities: Arc<DashMap<String, DatabaseIdentity, THasher>>,
    /// R1c §4.5 — engine-side read-only (follower) flag lowered by the server
    /// at launch (`set_node_read_only`); application-schema apply fails closed
    /// on it. One `AtomicBool`, written once at boot, read per admission.
    pub(in crate::shamir_db) node_read_only: Arc<std::sync::atomic::AtomicBool>,
    /// S5c-X PR8b — lock-free cache of `(db, repo)` targets covered by a
    /// `resync_required` subscription; rebuilt wholesale by
    /// `refresh_resync_quarantine_cache`. READS of a covered repo fail
    /// closed; one atomic load on the healthy path.
    pub(crate) resync_quarantine: Arc<ResyncQuarantineCache>,
    pub(crate) changelog_receivers: Arc<super::revocation::registry::ChangelogReceiverRegistry>,
    /// Round-4 mechanism 2 — per-subscription generation lease registry
    /// (`name -> Arc<SubscriptionLease>`); shared by clone via the inner `Arc`.
    pub(crate) leases: Arc<DashMap<String, Arc<SubscriptionLease>, THasher>>,
    /// S5c-X PR8b-2 (evidence row 6): per-instance test fault seam for
    /// `flush_seed_catalogue` (the round-2 GLOBAL atomic made concurrent
    /// tests interfere). Gated `any(test, "test-util")` like all seams here.
    #[cfg(any(test, feature = "test-util"))]
    pub(crate) flush_fault_armed: std::sync::Arc<std::sync::atomic::AtomicBool>,
    /// Armed fault parameters: successes before failure, then failures.
    #[cfg(any(test, feature = "test-util"))]
    pub(crate) flush_fault_pass_first: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    /// Fail-first count for `flush_seed_catalogue` (round-5 B3).
    #[cfg(any(test, feature = "test-util"))]
    pub(crate) flush_fault_fail_first: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    /// Round-7 counters track flush attempts, injected failures, and real
    /// successes while armed; reset by the test guard.
    #[cfg(any(test, feature = "test-util"))]
    pub(crate) flush_fault_attempts: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    #[cfg(any(test, feature = "test-util"))]
    pub(crate) flush_fault_failures: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    /// Real flush successes while this per-instance fault seam is armed.
    #[cfg(any(test, feature = "test-util"))]
    pub(crate) flush_fault_successes: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    /// Round-6 item 5 seam: armed mark operation fails its row SET before the row changes;
    /// pending quarantine remains while the durable row is untouched. Test-only.
    #[cfg(any(test, feature = "test-util"))]
    pub(crate) row_set_fault_armed: std::sync::Arc<std::sync::atomic::AtomicBool>,
    /// Round-4 T7 seam: when armed, `write_new_subscription` signals the Notify
    /// immediately before the final-state row write, so the test can drop the
    /// CREATE future exactly on the final-write boundary. Gated
    /// `any(test, "test-util")`: shamir-server's tests drive it cross-crate.
    #[cfg(any(test, feature = "test-util"))]
    pub(crate) final_write_pause: std::sync::Arc<std::sync::atomic::AtomicBool>,
    /// Round-4 T7 seam: one-shot arrival signal for `final_write_pause`.
    #[cfg(any(test, feature = "test-util"))]
    pub(crate) final_write_arrived: std::sync::Arc<tokio::sync::Notify>,
    /// Round-4 T7 seam: while `final_write_pause` is armed, the CREATE
    /// future parks on this Notify at the final-write boundary until released.
    #[cfg(any(test, feature = "test-util"))]
    pub(crate) final_write_release: std::sync::Arc<tokio::sync::Notify>,
    /// Deterministic interleaving seam for catalogue-bound function resolution tests.
    #[cfg(test)]
    pub(super) function_resolution_hook: Arc<std::sync::OnceLock<FunctionResolutionHook>>,
    /// Deterministic pause points for function DDL interleaving regression tests.
    #[cfg(test)]
    pub(super) function_ddl_hook: Arc<std::sync::OnceLock<FunctionDdlHook>>,
    /// Round-5 T6 seam: when armed, `DROP … CASCADE` parks INSIDE the
    /// interlock after the binding check, so the test holds the
    /// replication-control lock mid-cascade (a moved lock acquisition turns
    /// the T6 assertions red).
    #[cfg(test)]
    pub(crate) cascade_pause: std::sync::Arc<std::sync::atomic::AtomicBool>,
    /// Round-5 T6 seam: arrival signal for `cascade_pause`.
    #[cfg(test)]
    pub(crate) cascade_arrived: std::sync::Arc<tokio::sync::Notify>,
    /// Round-5 T6 seam: the parked cascade waits on this Notify.
    #[cfg(test)]
    pub(crate) cascade_release: std::sync::Arc<tokio::sync::Notify>,
}

impl ShamirDb {
    #[cfg(test)]
    pub(super) async fn function_ddl_test_hook(&self, point: FunctionDdlHookPoint) {
        if let Some(hook) = self.function_ddl_hook.get() {
            hook(point).await;
        }
    }

    /// Initialize ShamirDb with a system store.
    ///
    /// # Arguments
    /// * `config` — system store config (InMemory for tests, Redb(path) for production)
    pub async fn init(config: SystemStoreConfig) -> DbResult<Self> {
        Self::init_inner(
            config,
            EnvPolicy::default(),
            false,
            DurabilityPolicy::default(),
        )
        .await
    }

    /// Initialize ShamirDb with a system store and an explicit env-seeding policy.
    ///
    /// After constructing the global-vars store, eligible OS environment variables
    /// are seeded into the `env.*` namespace according to `policy`.
    pub async fn init_with_env_policy(
        config: SystemStoreConfig,
        policy: EnvPolicy,
    ) -> DbResult<Self> {
        Self::init_inner(config, policy, false, DurabilityPolicy::default()).await
    }

    /// The normal server init/recovery path with a catalogue allocation budget.
    pub async fn init_seed(config: SystemStoreConfig) -> DbResult<Self> {
        Self::init_inner(
            config,
            EnvPolicy::default(),
            true,
            DurabilityPolicy::default(),
        )
        .await
    }

    /// Shared initialization preserving the supplied environment-seeding policy.
    pub(super) async fn init_inner(
        config: SystemStoreConfig,
        policy: EnvPolicy,
        seed: bool,
        durability: DurabilityPolicy,
    ) -> DbResult<Self> {
        let durability = Self::resolve_durability(durability)?;
        // Derive data_root BEFORE `config` is moved into SystemStore::init.
        let data_root: Option<std::path::PathBuf> = match &config {
            SystemStoreConfig::InMemory => None,
            SystemStoreConfig::Fjall(p) => p.parent().map(|d| d.to_path_buf()),
        };

        let system_store = SystemStore::init_with_durability(config, durability).await?;

        let dbs = Arc::new(DashMap::with_hasher(THasher::default()));
        let admin_user_locks = Arc::new(DashMap::with_hasher(THasher::default()));
        let group_member_locks = Arc::new(DashMap::with_hasher(THasher::default()));
        let active_migrations = Arc::new(DashMap::with_hasher(THasher::default()));
        // DEC-3 Q5: reuse compiled wasm modules from an on-disk cache when
        // a data root exists. The disk tier is private-directory-verified
        // on Unix and Windows alike (DEC-3 Q5-c); any distrust degrades to
        // an uncached engine, logged at warn.
        let wasm_engine = {
            if let Some(root) = &data_root {
                // Path contract: pass the DATA directory; the cache store
                // derives `<data>/cache/wasm` itself.
                Arc::new(
                    WasmEngine::with_compile_cache(root.clone())
                        .map_err(|e| DbError::Function(e.to_string()))?,
                )
            } else {
                Arc::new(WasmEngine::new().map_err(|e| DbError::Function(e.to_string()))?)
            }
        };
        let functions = Arc::new(FunctionRegistry::with_catalogue_builtins());
        let globals = Arc::new(GlobalVars::new());
        globals.seed_env(&policy);

        let validators = Arc::new(ValidatorRegistry::new());

        let shamir = Self {
            replication_control_lock: Arc::new(Mutex::new(())),
            replication_resume_probe: Arc::new(std::sync::RwLock::new(None)),
            online_backup_port: Arc::new(std::sync::RwLock::new(None)),
            dbs,
            system_store,
            admin_user_locks,
            group_member_locks,
            active_migrations,
            functions,
            wasm_engine,
            globals,
            net_allowlist: Arc::new(super::super::gateway::net_grants::NetGrantsCache::new(
                Vec::new(),
            )),
            function_meta: Arc::new(DashMap::with_hasher(THasher::default())),
            function_state: Arc::new(DashMap::with_hasher(THasher::default())),
            function_ddl_locks: FunctionDdlLocks::new(),
            group_id_lock: Arc::new(Mutex::new(())),
            db_create_lock: Arc::new(Mutex::new(())),
            repo_create_locks: Arc::new(DashMap::with_hasher(THasher::default())),
            validators,
            data_root,
            user_admin_port: None,
            principal_resolver: None,
            experimental_migration_enabled: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            durability_policy: durability,
            function_family_migration_gate: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            validator_rename_migration: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            schema_migration_fences: Arc::new(DashMap::with_hasher(THasher::default())),
            schema_migration_fence_count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            database_identities: Arc::new(DashMap::with_hasher(THasher::default())),
            node_read_only: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            resync_quarantine: Arc::new(ResyncQuarantineCache::new()),
            changelog_receivers: Arc::new(
                super::revocation::registry::ChangelogReceiverRegistry::new(),
            ),
            leases: Arc::new(DashMap::with_hasher(THasher::default())),
            #[cfg(any(test, feature = "test-util"))]
            flush_fault_armed: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            #[cfg(any(test, feature = "test-util"))]
            flush_fault_pass_first: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            #[cfg(any(test, feature = "test-util"))]
            flush_fault_fail_first: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            #[cfg(any(test, feature = "test-util"))]
            flush_fault_attempts: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            #[cfg(any(test, feature = "test-util"))]
            flush_fault_failures: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            #[cfg(any(test, feature = "test-util"))]
            flush_fault_successes: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            #[cfg(any(test, feature = "test-util"))]
            row_set_fault_armed: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            #[cfg(any(test, feature = "test-util"))]
            final_write_pause: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            #[cfg(any(test, feature = "test-util"))]
            final_write_arrived: std::sync::Arc::new(tokio::sync::Notify::new()),
            #[cfg(any(test, feature = "test-util"))]
            final_write_release: std::sync::Arc::new(tokio::sync::Notify::new()),
            #[cfg(test)]
            function_resolution_hook: Arc::new(std::sync::OnceLock::new()),
            #[cfg(test)]
            function_ddl_hook: Arc::new(std::sync::OnceLock::new()),
            #[cfg(test)]
            cascade_pause: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            #[cfg(test)]
            cascade_arrived: std::sync::Arc::new(tokio::sync::Notify::new()),
            #[cfg(test)]
            cascade_release: std::sync::Arc::new(tokio::sync::Notify::new()),
        };

        if seed {
            shamir.check_seed_catalogue().await?;
        }
        // Load existing databases; `__system__` is reserved internally.
        let db_records = shamir.system_store.load_databases().await?;
        super::catalog::validate_catalogue_db_names(&db_records)?;
        for record in &db_records {
            if let Some(name) = record["name"].as_str() {
                if name != SYSTEM_DB_NAME {
                    // Boxed: keeps the backfill future out of this large init future.
                    let identity =
                        Box::pin(ensure_database_identity(&shamir.system_store, name)).await?;
                    shamir
                        .database_identities
                        .insert(name.to_string(), identity);
                }
            }
        }
        // R1c step 2b-3b: one-time open-time seed of the in-doubt marker.
        Box::pin(shamir.seed_schema_migration_in_doubt()).await?;
        for record in &db_records {
            if let Some(name) = record["name"].as_str() {
                if name != SYSTEM_DB_NAME {
                    shamir.dbs.insert(name.to_string(), DbInstance::new());
                }
            }
        }

        // Load existing repositories and register them
        let repo_records = shamir.system_store.load_repositories().await?;
        // Load the per-repo table catalogue once (I.2). Each repo's tables
        // must be re-created BEFORE recovery so V2 crash-recovery's
        // `table_by_token` resolves and Put/Delete/Index ops actually replay
        // for disk-backed repos.
        let table_records = shamir.system_store.load_tables().await?;
        let tables_by_repo = super::catalog::group_tables_by_repo(&table_records, || {});
        for rec in &repo_records {
            let db_name = rec["db_name"].as_str().ok_or_else(|| {
                DbError::Internal("repository catalogue row is missing db_name".to_string())
            })?;
            let repo_name = rec["repo_name"].as_str().ok_or_else(|| {
                DbError::Internal("repository catalogue row is missing repo_name".to_string())
            })?;
            let engine = rec["engine"].as_str().unwrap_or("in_memory");
            let path = rec["path"].as_str();

            // Clone the `DbInstance` out of the registry (cheap Arc) so we
            // do NOT hold the DashMap shard guard across the `add_repo` /
            // recovery awaits below.
            if let Some(db) = shamir.get_db(db_name) {
                if let Some(factory) = Self::factory_from_meta(engine, path) {
                    // I.2: re-attach the repo WITH its persisted table
                    // catalogue, so the tables exist (and the token→name
                    // reverse index is populated) BEFORE recovery runs.
                    // Previously this passed an empty table list, so a
                    // disk-backed repo's tables didn't exist on restart and
                    // V2 recovery's `table_by_token` resolved nothing —
                    // Put/Delete/Index replay was silently skipped.
                    let mut config = RepoConfig::new(repo_name, factory);
                    if let Some(tables) = tables_by_repo.get(&(db_name, repo_name)) {
                        for trec in tables {
                            if let Some(name) = trec["table_name"].as_str() {
                                let table_config = table_config_from_catalogue_record(name, trec)?;
                                config = config.add_table(table_config);
                            }
                        }
                    }
                    // Fail closed, before registry insertion, including J load
                    // and gap/J persistence failures. Do not silently skip repos.
                    shamir.seed_boot_gate(engine, path).await?;
                    // S5c-X PR1b part 2: a stored per-repo durability
                    // override takes precedence over the server default at
                    // boot. A corrupt override fails init LOUDLY (fail
                    // closed) — it never silently degrades to the default.
                    let policy = match durability_from_record(rec)? {
                        Some(over) => resolve(
                            &shamir.durability_policy,
                            Some(&over),
                            effective_wal_segment_bytes(),
                        )?,
                        None => shamir.durability_policy,
                    };
                    db.add_repo_with_durability(config, policy).await?;
                } else {
                    log::warn!(
                        "shamir_db::init: skipping repository '{}/{}' (backend='{}', path={:?}): no factory available",
                        db_name,
                        repo_name,
                        engine,
                        path
                    );
                }
            } else {
                return Err(DbError::Internal(format!(
                    "repository '{}/{}' has no parent database",
                    db_name, repo_name
                )));
            }
        }
        let fn_records = shamir.system_store.load_functions().await?;
        shamir.load_functions_at_boot(&fn_records).await?;

        // Load persisted WASM validators from the validator catalogue and
        // register them (S1). Mirrors the function loading block above.
        let val_records = shamir.system_store.load_validators().await?;
        for rec in &val_records {
            let name = match rec["name"].as_str() {
                Some(n) => n.to_string(),
                None => continue,
            };
            // Dispatch on artifact kind: Native rows have no WASM bytes —
            // skip materialisation entirely. The embedder re-registers the
            // native artifact at startup. Table-level bindings (stored on
            // each table's info-twin) are restored independently and still
            // reference this validator_id; any write will fail closed via
            // ValidatorFailure::Missing until the artifact is re-registered.
            let kind = super::ArtifactKind::from_record(rec);
            if kind == super::ArtifactKind::Native {
                log::info!(
                    "shamir_db::init: skipping native validator '{}' — re-register at startup",
                    name
                );
                continue;
            }
            if kind == super::ArtifactKind::Declarative {
                // Declarative validators live in the table catalogue and are
                // compiled by boot_compile_schemas — not from the validator
                // catalogue.
                log::info!(
                    "shamir_db::init: skipping declarative validator '{}' — \
                     compiled from table catalogue",
                    name
                );
                continue;
            }
            let wasm_b64 = match rec["wasm_b64"].as_str() {
                Some(b) => b,
                None => {
                    log::warn!(
                        "shamir_db::init: skipping validator '{}' — no wasm_b64 field",
                        name
                    );
                    continue;
                }
            };
            let wasm_bytes = match base64::engine::general_purpose::STANDARD.decode(wasm_b64) {
                Ok(b) => b,
                Err(e) => {
                    log::warn!(
                        "shamir_db::init: skipping validator '{}' — base64 decode error: {}",
                        name,
                        e
                    );
                    continue;
                }
            };
            // Reconstruct the RecordId from the persisted `_id` field.
            let id = match rec.get("_id").and_then(|v| v.as_str()) {
                Some(id_str) => match id_str.parse::<RecordId>() {
                    Ok(rid) => rid,
                    Err(e) => {
                        log::warn!(
                            "shamir_db::init: skipping validator '{}' — bad _id: {}",
                            name,
                            e
                        );
                        continue;
                    }
                },
                None => {
                    log::warn!(
                        "shamir_db::init: skipping validator '{}' — no _id field",
                        name
                    );
                    continue;
                }
            };
            match WasmFunction::from_binary_blocking(
                shamir.wasm_engine.clone(),
                &wasm_bytes,
                WasmLimits::default(),
            )
            .await
            {
                Ok(wf) => {
                    // Wrap in WasmRecordValidator — the registry stores
                    // Arc<dyn RecordValidator> since Phase 0.
                    let rv = Arc::new(WasmRecordValidator::new(Arc::new(wf)))
                        as Arc<dyn RecordValidator>;
                    if shamir.validators.register(id, &name, rv).is_err() {
                        log::warn!(
                            "shamir_db::init: validator '{}' already registered, skipping",
                            name
                        );
                        continue;
                    }
                    // Restore bound_in from the persisted record.
                    if let Some(bound) = rec.get("bound_in").and_then(|v| v.as_array()) {
                        for entry in bound {
                            if let Some(table_ref) = entry.as_str() {
                                shamir.validators.add_binding(&id, table_ref);
                            }
                        }
                    }
                }
                Err(e) => {
                    log::warn!(
                        "shamir_db::init: failed to compile WASM for validator '{}': {}",
                        name,
                        e
                    );
                }
            }
        }

        // Phase A2: compile declarative schemas from the table catalogue.
        // Runs after load_validators (WASM/native) so both code and
        // declarative validators coexist in the registry. The table_records
        // were already loaded above for the repo bootstrap.
        shamir.boot_compile_schemas(&table_records).await?;

        // Phase 4 boot diagnostic: surface native catalogue rows whose
        // in-process artifact was NOT re-registered by the embedder. The boot
        // loops above skip `kind = Native` rows (no wasm bytes); the embedder
        // is expected to call `register_fn` / `register_native_validator` at
        // startup to rehydrate them. Any row still missing a live artifact at
        // this point will fail closed on first use ("not found"). Emit a
        // visible `warn!` so a silent fail-closed becomes an actionable
        // startup signal rather than a mystery runtime error.
        let unresolved = shamir.unresolved_native_artifacts_inner(&fn_records, &val_records);
        if !unresolved.is_empty() {
            log::warn!(
                "shamir_db::init: {} native artifact(s) persisted in the catalogue but \
                 NOT re-registered at startup: {:?}. Writes to tables bound to an \
                 unresolved native validator will fail closed until the embedder \
                 re-registers them.",
                unresolved.len(),
                unresolved
            );
        }

        // S5c-X PR8b: rebuild the read-quarantine cache from the persisted
        // subscription rows — a `resync_required` quarantine survives restart.
        shamir.refresh_resync_quarantine_cache().await?;

        Ok(shamir)
    }

    /// Initialize with in-memory system store (convenience for tests).
    pub async fn init_memory() -> DbResult<Self> {
        Self::init(SystemStoreConfig::InMemory).await
    }

    /// The server-default policy supplied to repository opens.
    /// See [`Self::init_with_durability`].
    pub fn durability_policy(&self) -> DurabilityPolicy {
        self.durability_policy
    }

    /// Get the system store.
    pub fn system_store(&self) -> &SystemStore {
        &self.system_store
    }

    /// Per-user lock map used only for the per-table schema RMW path keyed
    /// by `schema_lock_key`; GrantRole/RevokeRole no longer acquire it.
    /// Entries are retained forever, one per distinct key ever locked,
    /// inserted only after authorization at admin/DDL frequency. The public
    /// accessor exposes it; eviction could split synchronization for a live key.
    pub fn admin_user_locks(&self) -> &Arc<DashMap<String, Arc<Mutex<()>>, THasher>> {
        &self.admin_user_locks
    }

    /// Per-group lock map for record RMWs keyed by group id.
    /// Entries are retained forever, one per distinct id ever locked, inserted
    /// only after authorization at group-admin frequency. The public accessor
    /// exposes the map; eviction could split synchronization for a live id.
    pub fn group_member_locks(&self) -> &Arc<DashMap<u64, Arc<Mutex<()>>, THasher>> {
        &self.group_member_locks
    }

    /// Global lock serialising the `handle_create_db` exists-check →
    /// authorize → create sequence (task #546 TOCTOU close).
    pub fn db_create_lock(&self) -> &Arc<Mutex<()>> {
        &self.db_create_lock
    }

    /// Namespace lifecycle lock. Lock order for DDL and namespace mutations:
    /// optional database/repository create lock → this lock → repository write
    /// admission → schema RMW lock → table write barrier. Never acquire an
    /// earlier lock while holding a later one.
    ///
    /// ONE lifecycle interlock shared by replication catalogue writers
    /// (admin subscription/profile handlers, terminal marking, the pending
    /// creation barrier, seed install, and the supervisor's reconcile read
    /// phase) AND destination lifecycle ops (`remove_db`/`rename_db_as`/
    /// `remove_repo`/`rename_repo_as`). Acquire it ONLY through this
    /// function — never hold it across an unbounded await. The returned
    /// [`ReplicationControlGuard`] newtype has a `pub(crate)` constructor,
    /// so code outside `shamir-db` cannot fabricate a guard (the compiler
    /// enforces the caller-holds-lock contract as far as crate boundaries
    /// go; no compile-fail evidence is available in this repo's CI).
    pub async fn lock_replication_control(&self) -> ReplicationControlGuard<'_> {
        ReplicationControlGuard::new(self.replication_control_lock.lock().await)
    }

    /// Per-database lock map for repository create/rename admission, keyed by
    /// database name. Entries are retained forever, one per distinct key ever
    /// locked, inserted only after authorization at admin/DDL frequency. The
    /// public accessor exposes the map; eviction could split synchronization for a live key.
    pub fn repo_create_locks(&self) -> &Arc<DashMap<String, Arc<Mutex<()>>, THasher>> {
        &self.repo_create_locks
    }

    pub fn active_migrations(&self) -> &Arc<DashMap<String, Arc<MigrationCoordinator>, THasher>> {
        &self.active_migrations
    }

    /// Opt in to the **experimental** online storage-engine migration API
    /// (`StartMigration` / `CommitMigration` / `RollbackMigration` /
    /// `MigrationStatus`).
    ///
    /// ⚠️ **This API remains experimental and unsuitable for production.**
    /// It is disabled by default; the live server enables it only through
    /// `security.enable_experimental_migration_api` (default `false`).
    ///
    /// - **Source frozen for writes.** From snapshot preparation through
    ///   migration completion, source writes are rejected with
    ///   `table_migrating`, including transactions staged before Start.
    ///   Rollback, failed Start and Commit exits release the freeze.
    ///   No write interception is installed.
    /// - **`dst_engine: "in_memory"` only.** Destination data does not
    ///   survive restart; this is not a durability-preserving migration.
    /// - **Limited restart recovery.** The coordinator uses `persist_state`
    ///   / `load_state` in the source info store, with durability determined
    ///   by that store's backend. Commit retries can recover persisted state
    ///   and finalisation markers, but the active map and freeze guard are
    ///   process-local, and persisted metadata cannot restore a lost
    ///   in-memory destination. This is not end-to-end crash-safe resumption.
    ///   Uncommitted retries without the original freeze are rejected with
    ///   `migration_restart_required`; committed retries finalise only the
    ///   fixed destination, not source writes admitted after freeze release.
    /// - **Replication followers unsupported.** Replicated apply bypasses
    ///   the freeze; this admin API has no reliable follower-role check.
    /// - **FK / DDL caveats.** A parent write cascading into a frozen table
    ///   aborts. DDL on the frozen source is not blocked.
    /// - **No list-all-migrations API.** Persisted completion receipts used
    ///   by Commit retries are not a user-facing migration history API.
    ///
    /// Calling this flips the gate to "enabled" for the remainder of the
    /// process lifetime (there is intentionally no `disable` counterpart).
    pub fn enable_experimental_migration_api(&self) {
        self.experimental_migration_enabled
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    /// Current state of the experimental migration-API gate. `false` by
    /// default; `true` after [`Self::enable_experimental_migration_api`]
    /// has been called. `handle_start_migration` consults this before
    /// doing any other work — see
    /// `crate::shamir_db::execute::admin_migration`.
    pub fn experimental_migration_enabled(&self) -> bool {
        self.experimental_migration_enabled
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Base directory for durable repos. `Some` when the system store
    /// is redb-backed (production), `None` for in-memory (tests).
    pub fn data_root(&self) -> Option<&std::path::Path> {
        self.data_root.as_deref()
    }

    /// The injected write-side user-administration port, if any (task #559).
    /// `None` for embedded/no-directory deployments — the four re-targeted
    /// handlers then return `not_supported`.
    pub fn user_admin_port(&self) -> Option<&Arc<dyn UserAdminPort>> {
        self.user_admin_port.as_ref()
    }

    /// The injected read-only principal resolver, if any (task #559). `None`
    /// for embedded/no-directory deployments — names then resolve to `None`.
    pub fn principal_resolver(&self) -> Option<&Arc<dyn PrincipalResolver>> {
        self.principal_resolver.as_ref()
    }

    /// Builder: install a write-side user-admin port. Mirrors the cheap-clone
    /// `Arc`-backed field pattern — returns `Self` so callers can chain.
    /// Idempotent overwrites are allowed (last-writer-wins); production wiring
    /// calls this exactly once at boot.
    pub fn with_user_admin_port(mut self, port: Arc<dyn UserAdminPort>) -> Self {
        self.user_admin_port = Some(port);
        self
    }

    /// Builder: install a read-only principal resolver. Mirrors
    /// [`Self::with_user_admin_port`].
    pub fn with_principal_resolver(mut self, resolver: Arc<dyn PrincipalResolver>) -> Self {
        self.principal_resolver = Some(resolver);
        self
    }

    /// Names of `kind = Native` catalogue entries (functions + validators)
    /// whose in-process artifact is NOT live in the registry.
    ///
    /// The boot loop skips `kind = Native` rows (they have no WASM bytes);
    /// the embedding application is expected to re-register each native
    /// artifact at startup via [`ShamirDb::register_fn`] /
    /// [`ShamirDb::register_native_validator`]. Any artifact still missing
    /// after startup will fail closed on first use ("not found"). This
    /// accessor turns that silent fail-closed into an actionable signal —
    /// a monitoring layer can poll it, and the boot path emits a `warn!`
    /// listing the unresolved names.
    ///
    /// Returns a sorted, deduplicated `Vec<String>` of names.
    pub async fn unresolved_native_artifacts(&self) -> DbResult<Vec<String>> {
        let fn_records = self.system_store.load_functions().await?;
        let val_records = self.system_store.load_validators().await?;
        Ok(self.unresolved_native_artifacts_inner(&fn_records, &val_records))
    }

    /// Inner join: given the already-loaded function and validator catalogue
    /// records, return the names of `kind = Native` rows whose artifact is
    /// absent from the live registry. Used both by the boot diagnostic
    /// (which already has the records in hand) and by the public
    /// [`Self::unresolved_native_artifacts`] accessor.
    fn unresolved_native_artifacts_inner(
        &self,
        fn_records: &[QueryValue],
        val_records: &[QueryValue],
    ) -> Vec<String> {
        let mut out = Vec::new();
        for rec in fn_records {
            if super::ArtifactKind::from_record(rec) != super::ArtifactKind::Native {
                continue;
            }
            if let Some(name) = rec.get("name").and_then(|v| v.as_str()) {
                if !self.functions.contains(name) {
                    out.push(name.to_string());
                }
            }
        }
        for rec in val_records {
            if super::ArtifactKind::from_record(rec) != super::ArtifactKind::Native {
                continue;
            }
            if let Some(name) = rec.get("name").and_then(|v| v.as_str()) {
                if self.validators.id_for_name(name).is_none() {
                    out.push(name.to_string());
                }
            }
        }
        out.sort();
        out.dedup();
        out
    }

    pub fn db_count(&self) -> usize {
        self.dbs.len()
    }

    pub fn has_db(&self, name: &str) -> bool {
        self.dbs.contains_key(name)
    }

    pub fn list_dbs(&self) -> Vec<String> {
        self.dbs.iter().map(|r| r.key().clone()).collect()
    }

    pub fn get_db(&self, name: &str) -> Option<DbInstance> {
        self.dbs.get(name).map(|r| r.clone())
    }

    /// Access the per-DB user scalar layer for `db_name`.
    ///
    /// Returns `None` if the database does not exist. The returned
    /// `Arc<UserScalarLayer>` can be used to register custom native
    /// scalar functions that become available in WHERE filters and
    /// (when marked `.trusted_pure()`) in functional indexes.
    pub fn scalars(
        &self,
        db_name: &str,
    ) -> Option<std::sync::Arc<shamir_funclib::scalar_resolver::UserScalarLayer>> {
        self.get_db(db_name)
            .map(|db| std::sync::Arc::clone(db.scalars()))
    }

    // ── Private helpers ──────────────────────────────────────────────────────

    pub(super) fn factory_from_meta(engine: &str, path: Option<&str>) -> Option<BoxRepoFactory> {
        #[cfg(not(feature = "fjall"))]
        let _ = path;
        match engine {
            "in_memory" => Some(BoxRepoFactory::in_memory()),
            #[cfg(feature = "fjall")]
            "fjall" => path.map(BoxRepoFactory::fjall),
            #[cfg(feature = "fjall")]
            "fjall_raw" => path.map(BoxRepoFactory::fjall_raw),
            #[cfg(feature = "fjall")]
            "hybrid" => path.map(BoxRepoFactory::hybrid),
            _ => None,
        }
    }

    pub(super) fn extract_storage_type(factory: &BoxRepoFactory) -> String {
        Self::storage_type_inner(factory, true).to_string()
    }

    /// Labels the top-level Fjall variant raw; wrapped Fjall keeps its wrapped kind.
    fn storage_type_inner(factory: &BoxRepoFactory, top: bool) -> &'static str {
        #[cfg(not(feature = "fjall"))]
        let _ = top;
        match factory {
            BoxRepoFactory::InMemory(_) => "in_memory",
            #[cfg(feature = "fjall")]
            BoxRepoFactory::Fjall(_) => {
                if top {
                    "fjall_raw"
                } else {
                    "fjall"
                }
            }
            #[cfg(feature = "fjall")]
            BoxRepoFactory::Hybrid(_) => "hybrid",
            BoxRepoFactory::MemBuffer(f) => Self::storage_type_inner(&f.inner, false),
            BoxRepoFactory::Cached(f) => Self::storage_type_inner(&f.inner, false),
        }
    }

    pub(super) fn extract_path(factory: &BoxRepoFactory) -> Option<String> {
        match factory {
            BoxRepoFactory::InMemory(_) => None,
            #[cfg(feature = "fjall")]
            BoxRepoFactory::Fjall(f) => Some(f.path.to_string_lossy().to_string()),
            #[cfg(feature = "fjall")]
            BoxRepoFactory::Hybrid(f) => Some(f.info_path.to_string_lossy().to_string()),
            BoxRepoFactory::MemBuffer(f) => Self::extract_path(&f.inner),
            BoxRepoFactory::Cached(f) => Self::extract_path(&f.inner),
        }
    }

    pub(super) fn table_ref_str(db: &str, repo: &str, table: &str) -> String {
        format!("{}/{}/{}", db, repo, table)
    }

    /// Build a captured-root [`FnCtx`] with the target's catalogue-derived
    /// capabilities and the shared nested-call resolver.
    pub(super) fn build_invoke_ctx(
        &self,
        actor: Actor,
        resolver: Arc<dyn StoredCallResolver>,
        definer_owner: Option<Actor>,
        target_meta: Option<&FunctionMeta>,
    ) -> FnCtx {
        let grants = target_meta
            .map(|meta| meta.secret_grants.clone())
            .unwrap_or_default();
        let net = target_meta.map_or_else(
            || self.net_allowlist.gateway(Some(&[])),
            |meta| self.net_allowlist.gateway(Some(&meta.net_grants)),
        );
        FnCtx::with_globals(self.globals.clone())
            .with_registry(self.functions.clone())
            .with_stored_call_resolver(resolver)
            .with_definer_owner(definer_owner)
            .with_net(net)
            .with_secret_grants(grants)
            .with_actor(actor)
    }
}

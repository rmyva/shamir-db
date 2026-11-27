use super::{LogicalRows, LogicalTable, MaintenanceGuard};
use crate::db_instance::db_instance::DbInstance;
use crate::repo::{RepoInstance, RepoWritePermit};
use crate::table::TableManager;
use shamir_storage::error::{DbError, DbResult};
use std::collections::BTreeMap;
use std::time::{Duration, SystemTime};

const CATALOGUE_FAMILIES: [&str; 8] = [
    "databases",
    "repositories",
    "tables",
    "settings",
    "functions",
    "validators",
    "function_folders",
    "groups",
];

/// Read session for one database plus its separately owned system repository.
/// All methods require the same borrowed freeze capability. Authorization,
/// scope/reference closure and authoritative server identities belong to the
/// caller; this trusted engine surface does not authorize actors.
///
/// Inventory is capped at 65,536 repositories/tables and 64 MiB of names.
/// Record cursors never materialize a table. Cursor order is NOT canonical;
/// the container writer must externally sort raw keys before publication.
pub struct LogicalExport<'a> {
    guard: &'a MaintenanceGuard,
    instant: SystemTime,
    repos: BTreeMap<String, (RepoInstance, shamir_tx::SnapshotGuard)>,
    system: (RepoInstance, shamir_tx::SnapshotGuard),
    // Drop after the snapshots. Empty for offline capture.
    cold_open_exclusion: Vec<RepoWritePermit>,
}

impl<'a> LogicalExport<'a> {
    /// Call only after every server mutation source has enrolled in the shared
    /// gate (or under exclusive offline ownership). Drain detached commit tails
    /// before recording T and pin every repository's contiguous visible floor.
    /// No WAL/history drainer is cancelled or reimplemented here.
    pub async fn capture(
        db: &DbInstance,
        system: &RepoInstance,
        guard: &'a MaintenanceGuard,
    ) -> DbResult<Self> {
        guard.check_deadline()?;
        if db.repo_count() > 65_536 || db.table_count() > 65_536 {
            return Err(DbError::Validation(
                "logical export inventory exceeds limit".into(),
            ));
        }
        let mut repos = BTreeMap::new();
        let mut name_bytes = 0usize;
        for name in db.list_repos() {
            name_bytes += name.len();
            let repo = db
                .get_repo(&name)
                .ok_or_else(|| DbError::NotFound(name.clone()))?;
            for table in repo.list_table_names() {
                name_bytes += table.len();
            }
            if name_bytes > 64 * 1024 * 1024 {
                return Err(DbError::Validation(
                    "logical export inventory exceeds byte limit".into(),
                ));
            }
            repo.quiesce_commits(Duration::from_secs(600)).await?;
            let gate = repo.tx_gate().await?;
            let snapshot = pin(&name, &gate, false).await?;
            repos.insert(name, (repo, snapshot));
        }
        system.quiesce_commits(Duration::from_secs(600)).await?;
        let gate = system.tx_gate().await?;
        let snapshot = pin(system.name(), &gate, true).await?;
        guard.charge_metadata(name_bytes)?;
        guard.check_deadline()?;
        Ok(Self {
            guard,
            instant: SystemTime::now(),
            repos,
            system: (system.clone(), snapshot),
            cold_open_exclusion: Vec::new(),
        })
    }

    /// The caller must hold the drained server freeze, supply its complete
    /// durable live database inventory (including `db`), and have warmed and
    /// reauthorized the included scope. Keep the result through publication.
    /// The inventory controls exclusion and settlement checks only; it does not
    /// widen export scope.
    ///
    /// cancel-safe: yes — quiescence only waits; gate initialization uses
    /// the existing retryable OnceCell; acquired permits and pins are RAII.
    #[doc(hidden)]
    pub async fn capture_online(
        db: &DbInstance,
        live: &[DbInstance],
        system: &RepoInstance,
        guard: &'a MaintenanceGuard,
    ) -> DbResult<Self> {
        guard.check_deadline()?;
        let domains = exclusion_inventory(live, system, guard)?;
        // The follower receipt worker enters the commit tail before taking
        // its repo permit. Never quiesce after acquiring even one permit.
        for repo in domains.values() {
            guard.check_deadline()?;
            repo.quiesce_commits(Duration::from_secs(600)).await?;
        }

        // Resolve any lazy tx gate before exclusion. No store or table open
        // is allowed once the first permit has been acquired.
        if db.repo_count() > domains.len() {
            return Err(DbError::Validation(
                "target database missing from logical export inventory".into(),
            ));
        }
        let mut target = BTreeMap::new();
        for name in db.list_repos() {
            let repo = db
                .get_repo(&name)
                .ok_or_else(|| DbError::NotFound(name.clone()))?;
            let token = repo.write_domain().token();
            if token == system.write_domain().token() || !domains.contains_key(&token) {
                return Err(DbError::Validation(
                    "target repository missing from logical export inventory".into(),
                ));
            }
            guard.check_deadline()?;
            let gate = repo.tx_gate().await?;
            target.insert(name, (repo, gate));
        }
        let system_gate = system.tx_gate().await?;

        let mut cold_open_exclusion = Vec::with_capacity(domains.len());
        for repo in domains.values() {
            guard.check_deadline()?;
            // Ordinary admission checks reservations under the mutex.
            let permit = repo
                .write_domain()
                .admit()
                .await
                .map_err(|error| DbError::Internal(error.to_string()))?;
            cold_open_exclusion.push(permit);
        }

        // Check every live domain before pinning any of them. A cold gate
        // cannot own a live orphan; don't initialize unrelated repos here.
        for repo in domains.values() {
            guard.check_deadline()?;
            if let Some(gate) = repo.initialized_tx_gate() {
                check_online_settled(repo.name(), gate)?;
            }
        }

        check_rename(system)?;
        for family in CATALOGUE_FAMILIES {
            initialized(system, family)?;
        }
        let mut repos = BTreeMap::new();
        for (name, (repo, gate)) in target {
            guard.check_deadline()?;
            let snapshot = pin(&name, &gate, false).await?;
            repos.insert(name, (repo, snapshot));
        }
        let snapshot = pin(system.name(), &system_gate, true).await?;
        guard.check_deadline()?;
        Ok(Self {
            guard,
            instant: SystemTime::now(),
            repos,
            system: (system.clone(), snapshot),
            cold_open_exclusion,
        })
    }

    pub fn instant(&self) -> SystemTime {
        self.instant
    }

    /// Bounded inventory iterator; no second inventory allocation.
    pub fn repository_versions(&self) -> impl Iterator<Item = (&str, u64)> {
        self.repos
            .iter()
            .map(|(name, (_, pin))| (name.as_str(), pin.version()))
    }

    pub fn system_version(&self) -> u64 {
        self.system.1.version()
    }

    pub fn table_names(&self, repo: &str) -> DbResult<Vec<String>> {
        self.guard.check_deadline()?;
        Ok(self.repository(repo)?.0.list_table_names())
    }

    /// Resolve a table for read-only metadata and bounded record paging.
    pub fn table(&self, repo: &str, table: &str) -> DbResult<LogicalTable<'_>> {
        self.guard.check_deadline()?;
        Ok(LogicalTable::new(
            initialized(&self.repository(repo)?.0, table)?,
            self.guard,
        ))
    }

    pub async fn records(&self, repo: &str, table: &str) -> DbResult<LogicalRows<'_>> {
        let (instance, pin) = self.repository(repo)?;
        let table = initialized(instance, table)?;
        LogicalRows::new(table, pin.version(), self.guard, false).await
    }

    /// Portable catalogue families only: never expose retired users/roles or
    /// replication configuration. Settings are further restricted in the cursor
    /// to root_meta/fn_namespace_meta; counters and secret settings stay private.
    /// Rows retain schemas, FunctionMeta, version/call_signature, wasm_b64,
    /// validator/folder definitions, groups and embedded ResourceMeta verbatim.
    fn catalogue(&self, family: &str) -> DbResult<TableManager> {
        self.guard.check_deadline()?;
        if !CATALOGUE_FAMILIES.contains(&family) {
            return Err(DbError::Validation(
                "excluded logical catalogue family".into(),
            ));
        }
        initialized(&self.system.0, family)
    }

    pub async fn catalogue_rows(&self, family: &str) -> DbResult<LogicalRows<'_>> {
        let table = self.catalogue(family)?;
        LogicalRows::new(table, self.system.1.version(), self.guard, true).await
    }

    /// Presence only: migration journal rows are not portable catalogue content.
    #[doc(hidden)]
    pub async fn has_schema_migration_history(&self) -> DbResult<bool> {
        let table = initialized(&self.system.0, "schema_migration_events")?;
        let mut rows = LogicalRows::new(table, self.system.1.version(), self.guard, true).await?;
        Ok(rows.next_page().await?.is_some())
    }

    fn repository(&self, name: &str) -> DbResult<&(RepoInstance, shamir_tx::SnapshotGuard)> {
        let repo = self
            .repos
            .get(name)
            .ok_or_else(|| DbError::NotFound(format!("export repository '{name}'")))?;
        if !self.cold_open_exclusion.is_empty() {
            check_rename(&repo.0)?;
        }
        Ok(repo)
    }
}

fn exclusion_inventory(
    live: &[DbInstance],
    system: &RepoInstance,
    guard: &MaintenanceGuard,
) -> DbResult<BTreeMap<u64, RepoInstance>> {
    const MAX_INVENTORY: usize = 65_536;
    let inventory_limit = || DbError::Validation("logical export inventory exceeds limit".into());
    if live.len() > MAX_INVENTORY {
        return Err(inventory_limit());
    }
    let mut repo_count = 1;
    let mut table_count = system.table_count();
    if table_count > MAX_INVENTORY {
        return Err(inventory_limit());
    }
    guard.charge_metadata(system.name().len())?;
    for table in system.list_table_names() {
        guard.charge_metadata(table.len())?;
    }
    let mut domains = BTreeMap::from([(system.write_domain().token(), system.clone())]);
    for db in live {
        let count = db.repo_count();
        if count > MAX_INVENTORY - repo_count {
            return Err(inventory_limit());
        }
        repo_count += count;
        for name in db.list_repos() {
            guard.charge_metadata(name.len())?;
            let repo = db
                .get_repo(&name)
                .ok_or_else(|| DbError::NotFound(name.clone()))?;
            let count = repo.table_count();
            if count > MAX_INVENTORY - table_count {
                return Err(inventory_limit());
            }
            table_count += count;
            for table in repo.list_table_names() {
                guard.charge_metadata(table.len())?;
            }
            domains.entry(repo.write_domain().token()).or_insert(repo);
        }
    }
    Ok(domains)
}

fn check_rename(repo: &RepoInstance) -> DbResult<()> {
    if repo.has_rename_in_doubt() {
        return Err(DbError::Conflict(
            "repository has table rename in doubt".into(),
        ));
    }
    Ok(())
}

fn check_online_settled(name: &str, gate: &shamir_tx::RepoTxGate) -> DbResult<()> {
    if gate.requires_reopen() {
        return Err(DbError::RepoRequiresReopen(name.into()));
    }
    // Allocation is frozen, but the raw WAL drainer can still settle a
    // version. The eager marker alone can cover holes; the contiguous floor
    // covers only terminal versions (including aborts). Unresolved tickets
    // leave holes until the drainer takes ownership and settles them.
    let high = gate.version_allocation_high_water_mark();
    if gate.last_committed() != high
        || gate.visible_floor() != high
        // Replay publishes before persisting markers and forgetting the orphan.
        // Check membership after the floors, and skip the scan if no orphan
        // has ever been registered. orphan_max itself is not a live count.
        || (gate.orphan_max() != 0 && !gate.orphaned_post_wal_versions().is_empty())
    {
        return Err(DbError::Conflict(
            "unsettled repository versions prevent online logical export".into(),
        ));
    }
    Ok(())
}

async fn pin(
    name: &str,
    gate: &shamir_tx::RepoTxGate,
    system: bool,
) -> DbResult<shamir_tx::SnapshotGuard> {
    if gate.requires_reopen() {
        return Err(DbError::RepoRequiresReopen(name.into()));
    }
    let snapshot = gate.open_snapshot().await;
    if snapshot.version() != gate.last_committed() {
        let message = if system {
            "unsettled system versions prevent logical export"
        } else {
            "unsettled repository versions prevent logical export"
        };
        return Err(DbError::Conflict(message.into()));
    }
    Ok(snapshot)
}

// Cold opens can run repairs. Prepare included tables BEFORE entering the gate;
// an export must never invoke lazy write-producing initialization under freeze.
// The initialized manager's interner hydrates via Store reads and its buffer
// config is a Store::get; neither path re-enters the repo write domain.
fn initialized(repo: &RepoInstance, name: &str) -> DbResult<TableManager> {
    repo.core.catalog().initialized_table(name).ok_or_else(|| {
        DbError::Validation(format!(
            "table '{name}' must be opened before logical export"
        ))
    })
}

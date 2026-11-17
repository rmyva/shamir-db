use std::time::Duration;

use tokio::time::Instant;

use crate::access::{Actor, ResourceMeta};
use crate::engine::db_instance::db_instance::DbInstance;
use crate::engine::repo::durability_policy::{effective_wal_segment_bytes, resolve};
use crate::engine::repo::RepoConfig;
use crate::shamir_db::repo_durability::durability_from_record;
use crate::shamir_db::shamir_db::ReplicationControlGuard;
use crate::shamir_db::system_store::DatabaseIdentity;
use crate::types::value::QueryValue;
use crate::{DbError, DbResult};

/// Per-repo flush durations at or above this are logged with the budget left.
const SLOW_FLUSH_LOG: Duration = Duration::from_secs(1);
use shamir_query_types::durability::DurabilityOverride;

use super::super::SYSTEM_DB_NAME;
use super::super::{ShamirDb, ShutdownReport};

/// One shared budget for quiescing and checkpointing every repository. Sized like
/// the server's shutdown budget: a slow fsync on a loaded runner must not turn
/// a healthy flush into an unclean report.
const COMMIT_QUIESCE_TIMEOUT: Duration = Duration::from_secs(15);

impl ShamirDb {
    /// System-actor convenience wrapper around
    /// [`create_db_as`](Self::create_db_as).
    ///
    /// // SAFETY (wire-reachability, task #546): NEVER call this from a
    /// // wire-reachable path (a server request handler, a new `BatchOp`
    /// // admin handler, etc.) — it stamps the new database's owner as
    /// // `Actor::System` unconditionally, silently bypassing ACL
    /// // attribution for whoever actually issued the request. Every
    /// // wire-reachable admin dispatcher already goes through
    /// // `execute_as(real_actor, ...)` -> `handle_create_db` ->
    /// // `create_db_as_checked(name, real_actor)`, never this bare
    /// // wrapper. This function exists ONLY for offline/CLI tooling and
    /// // test setup that legitimately runs as the system principal —
    /// // grep every call site before adding a new one from
    /// // request-handling code.
    #[doc(hidden)]
    pub async fn create_db(&self, name: &str) -> DbResult<DbInstance> {
        self.create_db_as(name, Actor::System).await
    }

    /// Like `create_db` but stamps the new database's owner as `actor`
    /// instead of `System`. Mode stays `0o777` (open). Creation is
    /// idempotent for an already-live name (the existing instance is
    /// returned untouched); a NEW name is persisted to the catalogue
    /// BEFORE its live registration is published, so a catalogue save
    /// failure propagates and leaves no live registration behind
    /// (RV-DB-6540DC7B); this is the same contract as
    /// [`create_db_as_checked`](Self::create_db_as_checked).
    pub async fn create_db_as(&self, name: &str, actor: Actor) -> DbResult<DbInstance> {
        self.create_db_as_checked(name, actor).await
    }

    /// Catalogue-persist-fallible database creation (RV-DB-6540DC7B):
    /// serialized against concurrent creators and the rename/drop
    /// lifecycle via the replication-control lock; idempotent for an
    /// already-live name (the original instance is returned with its
    /// repo/scalar state and durable ownership untouched — no catalogue
    /// write is attempted); for a NEW name the catalogue persist happens
    /// BEFORE the live registration is published, so a save failure
    /// propagates and leaves neither a live registration nor a
    /// published-over-writer hazard. This contract makes NO claim that
    /// an arbitrary failure leaves the durable row absent — a save that
    /// commits and then fails at flush can leave the row behind while
    /// still returning Err; the injected-fault tests exercise the
    /// failure BEFORE the write (the prewrite catalogue gate).
    pub async fn create_db_as_checked(&self, name: &str, actor: Actor) -> DbResult<DbInstance> {
        // Round 2 (A6) lifecycle interlock: serialize the existence check,
        // the catalogue persist, and the live publish against concurrent
        // creators and the rename/drop lifecycle (which all take the same
        // replication-control lock). Lock order is safe: the only place a
        // guard is held while entering here is `handle_create_db`, which
        // holds `db_create_lock` and acquires THIS lock after it
        // (db_create_lock → replication_control); no code path takes the
        // locks in the reverse order, so no inversion/deadlock.
        let _control = self.lock_replication_control().await;

        // Idempotent duplicate: return the ORIGINAL live instance.
        // `get_db` clones the Arc out of the DashMap (no guard held across
        // the awaits below); the catalogue is deliberately NOT rewritten,
        // so the original owner/durable row stay untouched.
        if let Some(existing) = self.get_db(name) {
            return Ok(existing);
        }

        let db = DbInstance::new();
        let created_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let mut m = shamir_types::types::common::new_map();
        m.insert(
            "name".to_string(),
            shamir_types::types::value::QueryValue::Str(name.to_string()),
        );
        m.insert(
            "identity".to_string(),
            QueryValue::Str(DatabaseIdentity::new().to_string()),
        );
        m.insert(
            "created_at".to_string(),
            shamir_types::types::value::QueryValue::Int(created_at as i64),
        );
        let record = shamir_types::types::value::QueryValue::Map(m);
        // Persist FIRST, publish second: a failed save propagates before
        // any live registration exists, so there is nothing to roll back
        // and no concurrent creator can observe (or delete through a
        // rollback) an unrelated registration.
        self.system_store
            .save_database(name, &record, &ResourceMeta::owned_enforced(actor))
            .await?;
        self.dbs.insert(name.to_string(), db.clone());
        Ok(db)
    }

    pub async fn remove_db(&self, name: &str) -> DbResult<bool> {
        // Round 2 (A6) lifecycle interlock: ONE lock shared with the
        // subscription catalogue writers, taken here so every existing
        // caller keeps the interlock without changes.
        let _create_guard = self.db_create_lock().lock().await;
        let _control = self.lock_replication_control().await;
        self.remove_db_locked(name, &_control).await
    }

    /// Lock-free variant of [`Self::remove_db`].
    ///
    /// The `_control` guard parameter ENFORCES that the caller holds the
    /// replication control lock (non-reentrant — never acquired here).
    pub(crate) async fn remove_db_locked(
        &self,
        name: &str,
        _control: &ReplicationControlGuard<'_>,
    ) -> DbResult<bool> {
        if name == SYSTEM_DB_NAME {
            return Ok(false);
        }

        // S5c-X PR8b-2: refuse DROP of a destination referenced by a durable
        // subscription binding BEFORE any live or durable mutation.
        self.ensure_destinations_unbound(name, None, "DROP").await?;

        let removed = self.dbs.remove(name).is_some();

        if removed {
            // RV-DB-6540DC7B: a failed catalogue removal must surface — returning
            // Ok(true) would claim a drop that did not complete.
            self.system_store.remove_database(name).await?;
        }

        Ok(removed)
    }

    /// Thin System-actor wrapper around [`rename_db_as`].
    pub async fn rename_db(&self, from: &str, to: &str) -> DbResult<()> {
        self.rename_db_as(from, to, Actor::System).await
    }

    /// Rename a database — **pure catalogue re-key** (campaign ②.1d,
    /// variant γ). The physical on-disk location of every repository is
    /// stored *inside* its persisted `repositories` row (`path` field)
    /// and re-read from there on boot (`core.rs`), NOT reconstructed from
    /// the database name. So this method:
    ///
    /// 1. Moves the in-memory `DbInstance` from key `from` to key `to`
    ///    in `dbs`. Open repo handles travel with the moved instance;
    ///    their `path` fields are untouched (key invariant γ).
    /// 2. Re-keys the persisted `databases` registry row (write-new-
    ///    before-remove-old so a crash leaves the new row resolvable).
    /// 3. Re-keys EVERY child catalogue row carrying `db_name`:
    ///    `repositories` (preserve `engine`/`path`/`ResourceMeta`) and
    ///    `tables` (preserve `enable_indexes`/`schema`/`schema_validator_id`/
    ///    `schema_version`/`ResourceMeta` via `save_table_meta`).
    ///
    /// **Catalogue completeness (grep-audit of `system_store.rs`):** the
    /// only persisted system-store tables carrying a `db_name` column are
    /// `TABLE_DATABASES` (key `name`), `TABLE_REPOSITORIES`, and
    /// `TABLE_TABLES`. All other catalogues (`settings`, `users`, `roles`,
    /// `functions`, `groups`, `validators`, `function_folders`) are global
    /// and do NOT carry `db_name`. Schema (`schema`/`schema_validator_id`/
    /// `schema_version`) lives as fields inside the `tables` catalogue
    /// record, so re-keying that record via `save_table_meta` preserves
    /// the schema. Indexes are in-memory structures inside the live
    /// `RepoInstance` and travel with the moved `DbInstance` — they are
    /// NOT persisted in a db_name-keyed catalogue.
    ///
    /// Guards (refuse with a typed [`DbError`]):
    /// - `from` must not be `SYSTEM_DB_NAME`.
    /// - The source database must exist (in-memory OR persisted).
    /// - The destination must NOT exist (in-memory AND persisted).
    pub async fn rename_db_as(&self, from: &str, to: &str, _actor: Actor) -> DbResult<()> {
        // Hold database-create admission through namespace publication.
        let _create_guard = self.db_create_lock().lock().await;
        let _control = self.lock_replication_control().await;
        // S5c-X PR8b-2: refuse RENAME of a destination referenced by a
        // durable subscription binding BEFORE any live or durable mutation
        // (repo: None — the whole database is the destination set).
        self.ensure_destinations_unbound(from, None, "RENAME")
            .await?;
        if from == SYSTEM_DB_NAME {
            return Err(DbError::Validation(format!(
                "cannot rename the system database '{}'",
                SYSTEM_DB_NAME
            )));
        }
        // Source must exist in-memory (live registration).
        if !self.has_db(from) {
            return Err(DbError::NotFound(format!("Database '{}' not found", from)));
        }
        // Destination must be free both in-memory and in the catalogue.
        if self.has_db(to) {
            return Err(DbError::Validation(format!(
                "cannot rename database '{}' to '{}': destination already exists",
                from, to
            )));
        }
        if self.system_store.load_database(to).await?.is_some() {
            return Err(DbError::Validation(format!(
                "cannot rename database '{}' to '{}': a persisted database record already exists",
                from, to
            )));
        }

        // ── (1) In-memory re-key ──────────────────────────────────────
        // DbInstance has no internal `name` field (only `repos` + `scalars`),
        // so a pure map move is sufficient — open handles travel with the
        // Arc-cloned instance and their `path` fields are untouched.
        let inst = self
            .dbs
            .remove(from)
            .map(|(_, v)| v)
            .ok_or_else(|| DbError::NotFound(format!("Database '{}' not found", from)))?;
        self.dbs.insert(to.to_string(), inst);

        // ── (2) databases-registry re-key (write-before-remove) ───────
        // Load the old row, clone it, rewrite "name" → to, persist under
        // the new key preserving owner/group/mode via from_record, THEN
        // remove the old row. A crash between the two writes leaves the
        // new row resolvable; the old row is inert (no live registration).
        let old_db_rec = self
            .system_store
            .load_database(from)
            .await?
            .ok_or_else(|| {
                DbError::NotFound(format!(
                    "database catalogue record for '{}' not found",
                    from
                ))
            })?;
        let mut new_db_rec = old_db_rec.clone();
        if let QueryValue::Map(m) = &mut new_db_rec {
            m.insert("name".to_string(), QueryValue::Str(to.to_string()));
        }
        let db_meta = ResourceMeta::from_record(&old_db_rec);
        self.system_store
            .save_database(to, &new_db_rec, &db_meta)
            .await?;
        self.system_store.remove_database(from).await?;

        // ── (3) Re-key all child catalogue rows carrying `db_name` ────
        // (3a) repositories: load all, filter by db_name==from, re-key.
        let repos = self.system_store.load_repositories().await?;
        for rec in repos {
            let rec_db = rec
                .get("db_name")
                .and_then(|v| v.as_str())
                .unwrap_or_default();
            if rec_db != from {
                continue;
            }
            let repo_name = rec
                .get("repo_name")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            let engine = rec
                .get("engine")
                .and_then(|v| v.as_str())
                .unwrap_or("in_memory")
                .to_string();
            let path = rec
                .get("path")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let repo_meta = ResourceMeta::from_record(&rec);
            let repo_durability = durability_from_record(&rec)?;

            // Write-new-before-remove-old; `path` is preserved verbatim
            // (key invariant γ — physical location is unchanged).
            self.system_store
                .save_repository(
                    to,
                    &repo_name,
                    &engine,
                    path.as_deref(),
                    &repo_meta,
                    repo_durability.as_ref(),
                )
                .await?;
            self.system_store
                .remove_repository(from, &repo_name)
                .await?;
        }

        // (3b) tables: load all, filter by db_name==from, re-key via
        // save_table_meta so the full record (including schema /
        // schema_validator_id / schema_version) is preserved. We rewrite
        // the db_name field in the record body AND rely on save_table_meta
        // to extract (db_name, repo_name, table_name) for the key.
        let tables = self.system_store.load_tables().await?;
        for rec in tables {
            let rec_db = rec
                .get("db_name")
                .and_then(|v| v.as_str())
                .unwrap_or_default();
            if rec_db != from {
                continue;
            }
            let mut new_rec = rec.clone();
            if let QueryValue::Map(m) = &mut new_rec {
                m.insert("db_name".to_string(), QueryValue::Str(to.to_string()));
            } else {
                // Defensive: if the record isn't a map we can't re-key it.
                log::warn!(
                    "shamir_db::rename_db: table catalogue record for '{}/{}/{}' is not a map \
                     (skipping)",
                    from,
                    rec.get("repo_name")
                        .and_then(|v| v.as_str())
                        .unwrap_or_default(),
                    rec.get("table_name")
                        .and_then(|v| v.as_str())
                        .unwrap_or_default(),
                );
                continue;
            }
            let repo_name = new_rec
                .get("repo_name")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            let table_name = new_rec
                .get("table_name")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();

            // Write-new-before-remove-old.
            self.system_store.save_table_meta(&new_rec).await?;
            self.system_store
                .remove_table(from, &repo_name, &table_name)
                .await?;
        }

        Ok(())
    }

    /// System-actor convenience wrapper around
    /// [`add_repo_as`](Self::add_repo_as).
    ///
    /// // SAFETY (wire-reachability, task #546): NEVER call this from a
    /// // wire-reachable path — it stamps the new repo's (and its inline
    /// // tables') owner as `Actor::System` unconditionally, silently
    /// // bypassing ACL attribution for whoever actually issued the
    /// // request. `handle_create_repo` (the real wire-reachable admin
    /// // handler) already calls `add_repo_as(db_name, config, real_actor)`
    /// // directly, never this bare wrapper. This function exists ONLY for
    /// // offline/CLI tooling and test setup that legitimately runs as the
    /// // system principal — grep every call site before adding a new one
    /// // from request-handling code.
    /// // `#[doc(hidden)]` (not `pub(crate)`): narrowing visibility would
    /// // break 50+ integration test files (a separate compiled crate) and
    /// // shamir-server's legitimate boot-time `add_repo` call in
    /// // `server_launcher.rs` — hiding from public rustdoc/API discovery
    /// // is the achievable P2 mitigation here (task #606).
    #[doc(hidden)]
    pub async fn add_repo(&self, db_name: &str, config: RepoConfig) -> DbResult<()> {
        self.add_repo_as_with_durability(db_name, config, Actor::System, None)
            .await
    }

    /// Like `add_repo` but stamps the repo (and its inline tables) with
    /// the given actor as owner.
    pub async fn add_repo_as(
        &self,
        db_name: &str,
        config: RepoConfig,
        actor: Actor,
    ) -> DbResult<()> {
        self.add_repo_as_with_durability(db_name, config, actor, None)
            .await
    }

    /// Like [`add_repo_as`](Self::add_repo_as) but resolves the sparse
    /// per-repo durability override against the server-default policy and
    /// opens the repo with the EFFECTIVE policy, then persists the override
    /// inside the catalogue record (S5c-X PR1b part 2).
    pub async fn add_repo_as_with_durability(
        &self,
        db_name: &str,
        config: RepoConfig,
        actor: Actor,
        durability: Option<&DurabilityOverride>,
    ) -> DbResult<()> {
        let repo_lock = self
            .repo_create_locks()
            .entry(db_name.to_string())
            .or_insert_with(|| std::sync::Arc::new(tokio::sync::Mutex::new(())))
            .clone();
        let _create_guard = repo_lock.lock().await;
        let _control = self.lock_replication_control().await;
        self.add_repo_as_with_durability_locked(db_name, config, actor, durability, &_control)
            .await
    }

    pub(crate) async fn add_repo_as_with_durability_locked(
        &self,
        db_name: &str,
        config: RepoConfig,
        actor: Actor,
        durability: Option<&DurabilityOverride>,
        _control: &ReplicationControlGuard<'_>,
    ) -> DbResult<()> {
        let db = self
            .get_db(db_name)
            .ok_or_else(|| DbError::NotFound(format!("Database '{}' not found", db_name)))?;

        let repo_name = config.name.clone();
        let storage_type = Self::extract_storage_type(&config.factory);
        let path = Self::extract_path(&config.factory);
        // Capture the inline table list before `config` is moved into
        // `db.add_repo`, so the per-repo table catalogue can be persisted
        // alongside the repo record (I.2).
        let mut config = config;
        for table in &mut config.tables {
            if table.store_id.is_none() {
                *table = table.clone().with_new_store_id();
            }
            table.validate_store_name()?;
        }
        let inline_tables = config
            .tables
            .iter()
            .map(|t| (t.name.clone(), t.enable_indexes, t.store_id))
            .collect::<Vec<_>>();

        // DbInstance recovers and certifies before registry insertion.
        // A `Some` override is resolved against the server-default policy
        // HERE (not at next boot) so the running repo immediately runs the
        // effective policy; an invalid override fails before any mutation.
        let policy = resolve(
            &self.durability_policy,
            durability,
            effective_wal_segment_bytes(),
        )?;
        db.add_repo_with_durability(config, policy).await?;

        let meta = ResourceMeta::owned_enforced(actor.clone());

        // Persist to system store
        if let Err(e) = self
            .system_store
            .save_repository(
                db_name,
                &repo_name,
                &storage_type,
                path.as_deref(),
                &meta,
                durability,
            )
            .await
        {
            // RV-DB-6540DC7B: any persist failure rolls back and propagates —
            // never report a CREATE that did not durably complete.
            db.remove_repo(&repo_name).await;
            if let Err(cleanup_error) = self
                .system_store
                .remove_repository(db_name, &repo_name)
                .await
            {
                log::warn!(
                    "shamir_db::add_repo: failed to roll back catalogue '{}/{}' after durability persist failure: {}",
                    db_name,
                    repo_name,
                    cleanup_error
                );
            }
            return Err(e);
        }

        // Persist the inline table catalogue so these tables are re-created
        // on the next open (I.2). Best-effort per table, matching the
        // repo-record persistence above.
        for (table_name, enable_indexes, store_id) in &inline_tables {
            if let Err(e) = self
                .system_store
                .save_table(
                    db_name,
                    &repo_name,
                    table_name,
                    *enable_indexes,
                    store_id.as_ref(),
                    &meta,
                )
                .await
            {
                // RV-DB-6540DC7B: failed inline catalogue persistence must not
                // return success with a live repo that won't reload correctly.
                db.remove_repo(&repo_name).await;
                for (persisted_name, _, _) in &inline_tables {
                    if let Err(cleanup_error) = self
                        .system_store
                        .remove_table(db_name, &repo_name, persisted_name)
                        .await
                    {
                        log::warn!(
                            "shamir_db::add_repo: failed to roll back table catalogue '{}/{}/{}': {}",
                            db_name,
                            repo_name,
                            persisted_name,
                            cleanup_error
                        );
                    }
                }
                if let Err(cleanup_error) = self
                    .system_store
                    .remove_repository(db_name, &repo_name)
                    .await
                {
                    log::warn!(
                        "shamir_db::add_repo: failed to roll back catalogue '{}/{}': {}",
                        db_name,
                        repo_name,
                        cleanup_error
                    );
                }
                return Err(e);
            }
        }

        Ok(())
    }

    pub async fn remove_repo(&self, db_name: &str, repo_name: &str) -> DbResult<bool> {
        let repo_lock = self
            .repo_create_locks()
            .entry(db_name.to_string())
            .or_insert_with(|| std::sync::Arc::new(tokio::sync::Mutex::new(())))
            .clone();
        let _create_guard = repo_lock.lock().await;
        let _control = self.lock_replication_control().await;
        self.remove_repo_locked(db_name, repo_name, &_control).await
    }

    /// Lock-free variant of [`Self::remove_repo`].
    ///
    /// The `_control` guard parameter ENFORCES that the caller holds the
    /// replication control lock (non-reentrant — never acquired here).
    pub(crate) async fn remove_repo_locked(
        &self,
        db_name: &str,
        repo_name: &str,
        _control: &ReplicationControlGuard<'_>,
    ) -> DbResult<bool> {
        // S5c-X PR8b-2: refuse DROP of a referenced destination BEFORE any
        // live or durable mutation.
        self.ensure_destinations_unbound(db_name, Some(repo_name), "DROP")
            .await?;
        if let Some(db) = self.get_db(db_name) {
            let removed = db.remove_repo(repo_name).await;
            if removed {
                // RV-DB-6540DC7B: a failed catalogue removal must surface —
                // returning Ok(true) would claim a drop that did not complete.
                self.system_store
                    .remove_repository(db_name, repo_name)
                    .await?;
            }
            Ok(removed)
        } else {
            Ok(false)
        }
    }

    /// Rename a repository, preserving its tables, data, indexes, and
    /// catalogue metadata (Phase F.3 — RENAME REPO).
    ///
    /// Contract:
    /// - **Logical re-key only** — the in-memory `RepoInstance` is moved
    ///   to the new key in `DbInstance::repos` and its `name` field is
    ///   updated. The repo's physical table stores
    ///   (`__data__<table>` / `__info__<table>` / `__history__<table>`)
    ///   are keyed only by table name *inside* the repo, so they travel
    ///   with the repo under the new logical key at zero cost. No
    ///   `rename_table_stores` is invoked and no drain is needed.
    /// - **Catalogue re-key** — the old `(db, from)` repositories row is
    ///   removed and a new `(db, to)` row is written preserving
    ///   `engine` / `path` / `ResourceMeta`. Every child table's catalogue
    ///   row `(db, from, table)` is likewise re-keyed to `(db, to, table)`.
    ///
    /// Guards (refuse with a typed [`DbError`] instead of leaving
    /// dangling state):
    /// - The source repo must exist; the destination must not.
    pub async fn rename_repo_as(
        &self,
        db_name: &str,
        from: &str,
        to: &str,
        _actor: Actor,
    ) -> DbResult<()> {
        let repo_lock = self
            .repo_create_locks()
            .entry(db_name.to_string())
            .or_insert_with(|| std::sync::Arc::new(tokio::sync::Mutex::new(())))
            .clone();
        let _create_guard = repo_lock.lock().await;
        let _control = self.lock_replication_control().await;
        // S5c-X PR8b-2: refuse RENAME of a destination referenced by a
        // durable subscription binding BEFORE any live or durable mutation.
        self.ensure_destinations_unbound(db_name, Some(from), "RENAME")
            .await?;
        let db = self
            .get_db(db_name)
            .ok_or_else(|| DbError::NotFound(format!("Database '{}' not found", db_name)))?;

        // Existence guards.
        if !db.has_repo(from) {
            return Err(DbError::NotFound(format!(
                "Repository '{}/{}' not found",
                db_name, from
            )));
        }
        if db.has_repo(to) {
            return Err(DbError::Validation(format!(
                "cannot rename repo '{}/{}' to '{}': destination repository already exists",
                db_name, from, to
            )));
        }

        if db
            .get_repo(from)
            .is_some_and(|repo| repo.has_rename_in_doubt())
        {
            return Err(DbError::Conflict(format!(
                "rename in doubt: repository '{db_name}/{from}' contains an unresolved table rename"
            )));
        }

        // Snapshot the table list BEFORE the re-key so the catalogue rows
        // can be rewritten once the in-memory re-key has succeeded.
        let table_names = db.list_tables(from).unwrap_or_default();

        // Load the persisted repo record to preserve engine/path/meta
        // across the re-key.
        let old_repo_record = self
            .system_store
            .load_repository_record(db_name, from)
            .await?
            .ok_or_else(|| {
                DbError::NotFound(format!(
                    "repository catalogue record for '{}/{}' not found",
                    db_name, from
                ))
            })?;

        let engine = old_repo_record
            .get("engine")
            .and_then(|v| v.as_str())
            .unwrap_or("in_memory")
            .to_string();
        let path = old_repo_record
            .get("path")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let existing_meta = ResourceMeta::from_record(&old_repo_record);
        let existing_durability = durability_from_record(&old_repo_record)?;

        // 1. In-memory re-key (no per-table store copy).
        let renamed = db.rename_repo(from, to);
        debug_assert!(renamed, "rename_repo returned false despite has_repo guard");

        // 2. Persist the new repo catalogue row FIRST so a crash between
        //    the two writes leaves the new repo resolvable on reboot; a
        //    stale (db, from) row resurrects nothing because the live
        //    registration under `from` is already gone. Catalogue
        //    persistence failures propagate (RV-DB-6540DC7B) —
        //    write-new-before-remove-old stays.
        self.system_store
            .save_repository(
                db_name,
                to,
                &engine,
                path.as_deref(),
                &existing_meta,
                existing_durability.as_ref(),
            )
            .await?;
        self.system_store.remove_repository(db_name, from).await?;

        // 3. Re-key every child table's catalogue row: load the old row,
        //    write a new one under `(db, to, table)` preserving meta +
        //    enable_indexes, then remove the old `(db, from, table)` row.
        //    Write-before-remove so a crash leaves the new row resolvable.
        for table_name in &table_names {
            match self
                .system_store
                .load_table_record(db_name, from, table_name)
                .await
            {
                Ok(Some(rec)) => {
                    let mut new_rec = rec.clone();
                    if let QueryValue::Map(fields) = &mut new_rec {
                        fields.insert("db_name".to_string(), QueryValue::Str(db_name.to_string()));
                        fields.insert("repo_name".to_string(), QueryValue::Str(to.to_string()));
                    } else {
                        return Err(DbError::Internal(format!(
                            "table catalogue row for '{}/{}/{}' is not a map",
                            db_name, from, table_name
                        )));
                    }
                    // Re-key the complete row so schema and validator fields survive.
                    self.system_store.save_table_meta(&new_rec).await?;
                    self.system_store
                        .remove_table(db_name, from, table_name)
                        .await?;
                }
                Ok(None) => {
                    log::warn!(
                        "shamir_db::rename_repo: no catalogue row for '{}/{}/{}' \
                         (continuing)",
                        db_name,
                        from,
                        table_name
                    );
                }
                Err(e) => return Err(e),
            }
        }

        Ok(())
    }

    /// Drain all repositories, retaining every failure in the compatibility error.
    pub async fn flush_all(&self) -> DbResult<()> {
        self.flush_all_with_quiesce_timeout(COMMIT_QUIESCE_TIMEOUT)
            .await
    }

    /// [`flush_all`](Self::flush_all) that waits for completion instead of
    /// failing on the wall-clock budget. For test harnesses outside this
    /// crate: Windows CI runners stall every process's fsync for 10+ s at
    /// times, and the runner's per-test timeout stays the hang detector.
    pub async fn flush_all_waiting(&self) -> DbResult<()> {
        self.flush_all_with_quiesce_timeout(Duration::from_secs(3600))
            .await
    }

    /// Test-only [`flush_all`](Self::flush_all) that waits for completion:
    /// Windows CI runners stall every process's fsync for 10+ s at times, so
    /// a wall-clock budget is a false failure here; nextest's per-test
    /// timeout stays the hang detector.
    #[cfg(test)]
    pub(crate) async fn flush_all_for_test(&self) -> DbResult<()> {
        self.flush_all_with_quiesce_timeout(Duration::from_secs(3600))
            .await
    }

    pub fn shutdown_report(&self) -> ShutdownReport {
        let mut names = Vec::new();
        for db_name in self.list_dbs() {
            if let Some(db) = self.get_db(&db_name) {
                for repo_name in db.list_repos() {
                    names.push(format!("{db_name}/{repo_name}"));
                }
            }
        }
        names.push(format!("{SYSTEM_DB_NAME}/system"));
        ShutdownReport::pending(names)
    }

    /// Compatibility wrapper for embedded callers.
    pub(crate) async fn flush_all_with_quiesce_timeout(&self, timeout: Duration) -> DbResult<()> {
        let mut report = self.shutdown_report();
        self.flush_all_report(timeout, &mut report).await;
        if report.is_clean() {
            Ok(())
        } else {
            Err(DbError::Storage(format!(
                "unclean shutdown: {:?}",
                report.repository_failures(),
            )))
        }
    }

    /// One shared budget; completed outcomes survive cancellation by the server.
    /// cancel-safe: NO — checkpoint I/O may finish after cancellation; pending
    /// entries remain uncertified in the caller-owned report.
    pub async fn flush_all_report(&self, timeout: Duration, report: &mut ShutdownReport) {
        let deadline = Instant::now() + timeout;
        for db_name in self.list_dbs() {
            let Some(db) = self.get_db(&db_name) else {
                continue;
            };
            for repo_name in db.list_repos() {
                let Some(repo) = db.get_repo(&repo_name) else {
                    continue;
                };
                let remaining = deadline.saturating_duration_since(Instant::now());
                let started = Instant::now();
                let result = repo.shutdown_clean(remaining).await;
                if started.elapsed() >= SLOW_FLUSH_LOG {
                    log::warn!(
                        "flush_all: {}/{} took {:?} (budget left {:?})",
                        db_name,
                        repo_name,
                        started.elapsed(),
                        remaining
                    );
                }
                if let Err(e) = &result {
                    log::warn!("flush_all: {}/{}: {}", db_name, repo_name, e);
                }
                report.record(
                    &format!("{db_name}/{repo_name}"),
                    result.map_err(|e| e.to_string()),
                );
            }
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        let started = Instant::now();
        let result = self
            .system_store
            .flush_buffers_with_quiesce_timeout(remaining)
            .await;
        if started.elapsed() >= SLOW_FLUSH_LOG {
            log::warn!(
                "flush_all: system store took {:?} (budget left {:?})",
                started.elapsed(),
                remaining
            );
        }
        if let Err(e) = &result {
            log::warn!("flush_all: system store: {}", e);
        }
        report.record(
            &format!("{SYSTEM_DB_NAME}/system"),
            result.map_err(|e| e.to_string()),
        );
    }

    /// #984 — count degraded (non-`Ready`) indexes across all ALREADY-OPEN
    /// tables in all repos in all dbs. Pure in-memory — zero store reads.
    /// Cost is O(number of indexes across open tables), NOT O(rows). Does
    /// NOT force-open closed tables.
    ///
    /// Mirrors `flush_all`'s iteration pattern: `list_dbs` → `get_db` →
    /// `list_repos` → `get_repo` → `degraded_index_count`. Each `get_*`
    /// returns a cheap Arc-backed clone, so no DashMap guards are held
    /// across `.await`.
    pub async fn degraded_index_count(&self) -> u64 {
        let db_names = self.list_dbs();
        let mut count = 0u64;
        for db_name in &db_names {
            let Some(db) = self.get_db(db_name) else {
                continue;
            };
            for repo_name in db.list_repos() {
                let Some(repo) = db.get_repo(&repo_name) else {
                    continue;
                };
                count += repo.degraded_index_count().await;
            }
        }
        count
    }
}

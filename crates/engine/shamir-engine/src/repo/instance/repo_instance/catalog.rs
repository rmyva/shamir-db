use self::rename_attempt::RenameAttempt;
use self::rename_freeze::RenameFreeze;
pub use self::rename_freeze::{begin_migration_freeze, begin_migration_freeze_admitted};
pub use self::rename_outcome::RenamePersistOutcome;
use super::RepoInstance;
use crate::meta::repo_identity::RepoIdentity;
use crate::repo::changelog_store::write_domain::RepoWritePermit;
use crate::table::{TableConfig, TableManager, TableRegistration};
use shamir_storage::error::{DbError, DbResult};
use shamir_storage::types::Repo;
use shamir_types::types::record_id::RecordId;
use shamir_types::types::value::InnerValue;
use std::collections::BTreeSet;
use std::future::Future;

mod rename_attempt;
mod rename_fence;
mod rename_freeze;
mod rename_outcome;
mod repo_membership_impl;
pub(crate) mod visibility_impl;

impl RepoInstance {
    // ============================================================================
    // Index Management API (proxy to TableManager)
    // ============================================================================

    /// Create a regular index on a table.
    ///
    /// cancel-safe: NO — multi-step state mutation. Looks up the table
    /// (one await) then calls `table.create_index` which itself performs
    /// catalogue updates and persistence. Partial cancellation can leave
    /// the index in an inconsistent state; recovery is at the caller's
    /// discretion.
    pub async fn create_index(
        &self,
        table_name: &str,
        index_name: &str,
        paths: &[&str],
    ) -> DbResult<()> {
        self.get_table(table_name)
            .await?
            .create_index(index_name, paths)
            .await
    }

    /// Create a unique index on a table.
    ///
    /// cancel-safe: NO — same shape as `create_index`. Catalogue and
    /// persistence updates inside `table.create_unique_index` are not
    /// atomic across cancellation.
    pub async fn create_unique_index(
        &self,
        table_name: &str,
        index_name: &str,
        paths: &[&str],
    ) -> DbResult<()> {
        self.get_table(table_name)
            .await?
            .create_unique_index(index_name, paths)
            .await
    }

    /// Drop a regular index from a table.
    ///
    /// cancel-safe: NO — multi-step state mutation: lookup table then
    /// delete catalogue entries and persisted index data. Partial
    /// cancellation may leave orphaned index state.
    pub async fn drop_index(&self, table_name: &str, index_name: &str) -> DbResult<bool> {
        self.get_table(table_name)
            .await?
            .drop_index(index_name, None)
            .await
    }

    /// Drop a unique index from a table.
    ///
    /// cancel-safe: NO — same shape as `drop_index`. Partial cancellation
    /// may leave orphaned unique-index state.
    pub async fn drop_unique_index(&self, table_name: &str, index_name: &str) -> DbResult<bool> {
        self.get_table(table_name)
            .await?
            .drop_unique_index(index_name, None)
            .await
    }

    /// Check if a regular index exists on a table.
    ///
    /// cancel-safe: yes — read-only: looks up the table and queries
    /// existence. No state mutation; cancellation drops the query.
    pub async fn index_exists(&self, table_name: &str, index_name: &str) -> DbResult<bool> {
        Ok(self
            .get_table(table_name)
            .await?
            .index_exists(index_name)
            .await)
    }

    /// Check if a unique index exists on a table.
    ///
    /// cancel-safe: yes — read-only existence query. No state mutation.
    pub async fn unique_index_exists(&self, table_name: &str, index_name: &str) -> DbResult<bool> {
        Ok(self
            .get_table(table_name)
            .await?
            .unique_index_exists(index_name)
            .await)
    }

    /// Look up records by index value.
    ///
    /// cancel-safe: yes — read-only lookup. Cancellation drops the query
    /// future with no state mutation.
    ///
    pub async fn lookup_by_index(
        &self,
        table_name: &str,
        index_name: &str,
        values: &[InnerValue],
    ) -> DbResult<BTreeSet<RecordId>> {
        self.get_table(table_name)
            .await?
            .lookup_by_index(index_name, values)
            .await
    }

    /// cancel-safe: yes — one `scc::HashMap` read plus a single unfenced
    /// table lookup. Cancellation leaves no state mutated.
    ///
    /// Look up the table whose token matches `token`; see
    /// `RepoCore::table_by_token` for the resolution and deadlock notes.
    pub async fn table_by_token(&self, token: u64) -> DbResult<Option<TableManager>> {
        self.core.table_by_token(token).await
    }

    #[cfg(test)]
    /// Catalogue read; see `TableCatalog`.
    pub(crate) async fn table_by_token_if_live(&self, token: u64) -> Option<TableManager> {
        self.core.table_by_token_if_live(token).await
    }

    /// Catalogue install; see `TableCatalog`.
    #[cfg(feature = "test-util")]
    pub fn install_table_for_test(&self, name: &str, table: TableManager) {
        self.core.catalog().install_table_for_test(name, table)
    }

    /// Test-only: direct cell-map size (cold-resolver retention probe).
    #[cfg(feature = "test-util")]
    pub fn table_cell_count(&self) -> usize {
        self.core.catalog().table_cell_count()
    }

    /// Catalogue read; see `TableCatalog`.
    pub fn list_table_names(&self) -> Vec<String> {
        self.core.list_table_names()
    }

    /// Metadata-only read of a table's persisted validator bindings (the
    /// validators `MetaEnvelope` under the mapped `__info__` store).
    ///
    /// The trusted open paths (`get_table` / `get_table_unfenced`) construct
    /// a full `TableManager`: they open `__data__` and `__history__`, attach
    /// MVCC and can trigger index recovery. This read opens ONLY the mapped
    /// `__info__` store and decodes the one validators metadata key, so a
    /// cross-crate caller can derive persisted bindings without touching row
    /// data or managers. Missing configured stores and tables that have never
    /// saved bindings return `Ok(None)`. Existing-only store lookup is
    /// read-only even for lazy tables and creates no keyspace. Read-only: no
    /// admission, no interner, no fence.
    pub async fn table_validator_bindings_metadata(
        &self,
        table_name: &str,
    ) -> DbResult<Option<shamir_engine_validator::validator::PersistedValidators>> {
        let Some((_data, info, _history)) = self.core.mapped_table_store_names(table_name) else {
            return Ok(None);
        };
        let Some(store) = self.core.repo().store_get_existing(&info).await? else {
            return Ok(None);
        };
        crate::validator::persistence::load_validators_metadata(&store).await
    }

    /// Round 6 — whether an ALREADY-OPEN table's persisted validator binding
    /// record is IN DOUBT: a failed binding write could not be written back and
    /// flushed, so the live snapshot and the backing envelope may disagree.
    ///
    /// The flag expresses UNCERTAINTY, not guaranteed exclusion. It is also
    /// raised when the forward write fails, and a future dropped (cancelled) at
    /// the forward set/flush or at a compensation step leaves it raised; the
    /// backing record may then hold the previous value, the new one or a partial
    /// compensating write, and a visible old value after a failed sync proves
    /// nothing about durability.
    ///
    /// Non-instantiating and cheap: a table whose manager is dormant (or which
    /// has no manager) reports `false`, which is correct by definition — a fresh
    /// process has no doubt state. A restart RESETS the flag; it does not repair
    /// the records, so the residual states the operations guide documents can
    /// survive a reopen. Read by the validator holder scan and by the facade
    /// refusals (schema activation, table rename).
    pub fn table_validator_bindings_in_doubt(&self, table_name: &str) -> bool {
        self.core
            .catalog()
            .initialized_table(table_name)
            .is_some_and(|table| table.validator_bindings_in_doubt())
    }

    /// Catalogue read; see `TableCatalog`.
    pub fn has_table(&self, name: &str) -> bool {
        self.core.has_table(name)
    }

    /// Catalogue read; see `TableCatalog`.
    pub fn table_config(&self, name: &str) -> Option<TableConfig> {
        self.core.catalog().table_config(name)
    }

    /// Live registration identity; see `TableCatalog::table_registration`.
    pub fn table_registration(&self, name: &str) -> Option<TableRegistration> {
        self.core.catalog().table_registration(name)
    }

    #[cfg(test)]
    /// Catalogue read; see `TableCatalog`.
    pub(crate) fn mapped_table_store_names(&self, name: &str) -> Option<(String, String, String)> {
        self.core.mapped_table_store_names(name)
    }

    /// Catalogue read; see `TableCatalog`.
    pub fn table_count(&self) -> usize {
        self.core.catalog().table_count()
    }

    /// Catalogue read; see `TableCatalog`.
    pub async fn degraded_index_count(&self) -> u64 {
        self.core.catalog().degraded_index_count().await
    }

    /// Rename table stores using the repository's normal write admission.
    ///
    /// The copy receives a fresh store id; the old stores remain as orphans and
    /// the old manager is retired. This path refuses a fenced rename pair when it
    /// cannot durably resolve the catalogue state; it must not guess at an unknown
    /// persistence outcome.
    /// Rename a table's live registration: copies the three physical
    /// data stores (`__data__`, `__info__`, `__history__`) from the old
    /// name to the new one via [`Repo::copy_store`], then swaps the
    /// in-memory config + reverse-index entry. Returns `false` if the
    /// source table was not configured.
    ///
    /// The OLD physical stores are intentionally orphaned (same
    /// disposition as `drop_table`, which orphans `__data__` because
    /// the catalogue is the source of truth). If the caller also needs
    /// the old stores gone, follow up with `store_delete` per namespace.
    ///
    pub async fn rename_table_stores(&self, from: &str, to: &str) -> DbResult<bool> {
        let permit = self
            .core
            .durability_handle()
            .write_domain
            .admit()
            .await
            .map_err(|error| DbError::Internal(error.to_string()))?;
        self.rename_table_stores_admitted(from, to, &permit).await
    }

    /// Load or create the persistent repository identity.
    pub async fn repo_identity(&self) -> DbResult<RepoIdentity> {
        let permit = self
            .core
            .durability_handle()
            .write_domain
            .admit()
            .await
            .map_err(|error| DbError::Internal(error.to_string()))?;
        self.repo_identity_admitted(&permit).await
    }

    /// Load or create the identity while holding repository write admission.
    pub(crate) async fn repo_identity_admitted(
        &self,
        permit: &RepoWritePermit,
    ) -> DbResult<RepoIdentity> {
        self.core
            .durability_handle()
            .write_domain
            .debug_check_permit(permit);
        let _guard = self
            .core
            .durability_handle()
            .marker_write_mutex
            .lock()
            .await;
        let store = self.tx_info_store().await?;
        RepoIdentity::load_or_create(&store).await
    }

    /// Advance the repository history epoch.
    pub async fn bump_history_epoch(&self) -> DbResult<RepoIdentity> {
        let permit = self
            .core
            .durability_handle()
            .write_domain
            .admit()
            .await
            .map_err(|error| DbError::Internal(error.to_string()))?;
        self.core.bump_history_epoch_admitted(&permit).await
    }

    /// Retry-safe even when a previous open bumped before marker removal.
    ///
    /// Apply an idempotent restore operation to the history epoch.
    pub async fn apply_restore_operation(&self, operation: [u8; 16]) -> DbResult<RepoIdentity> {
        let permit = self
            .core
            .durability_handle()
            .write_domain
            .admit()
            .await
            .map_err(|error| DbError::Internal(error.to_string()))?;
        self.apply_restore_operation_admitted(operation, &permit)
            .await
    }

    /// Apply a restore operation while holding repository write admission.
    pub(crate) async fn apply_restore_operation_admitted(
        &self,
        operation: [u8; 16],
        permit: &RepoWritePermit,
    ) -> DbResult<RepoIdentity> {
        self.core
            .durability_handle()
            .write_domain
            .debug_check_permit(permit);
        let _guard = self
            .core
            .durability_handle()
            .marker_write_mutex
            .lock()
            .await;
        let store = self.tx_info_store().await?;
        RepoIdentity::bump(&store, Some(operation)).await
    }

    /// Safe only with a persisted destination row; source registration is switched after the write.
    ///
    /// Rename table stores after durable catalogue persistence.
    ///
    /// `persist` reports whether the catalogue commit is known, known not to have
    /// happened, or uncertain. Committed retires the source and completes the
    /// rename. A fresh NotCommitted attempt aborts its freeze and returns the
    /// original error. InDoubt (and NotCommitted while resuming a fenced attempt)
    /// retains the freeze and both names, returning a conflict so callers cannot
    /// mistake uncertainty for rollback. Cancellation before persistence aborts
    /// a fresh attempt; once persistence is underway, or for an already-fenced
    /// attempt, the guard is retained for recovery.
    pub async fn rename_table_stores_with<F, Fut>(
        &self,
        from: &str,
        to: &str,
        persist: F,
    ) -> DbResult<bool>
    where
        F: FnOnce(TableConfig) -> Fut,
        Fut: Future<Output = RenamePersistOutcome>,
    {
        self.rename_table_transition(from, to, persist).await
    }

    /// Reconcile an already-recorded table rename.
    ///
    /// A non-committed or uncertain persistence result leaves the source frozen
    /// and retains cancellation protection. Retrying the same name pair reuses
    /// its guard; recovery does not perform an extra drain or store copy.
    pub async fn recover_table_rename_with<F, Fut>(
        &self,
        from: &str,
        to: &str,
        config: TableConfig,
        expected_source_id: Option<&RecordId>,
        persist: F,
    ) -> DbResult<bool>
    where
        F: FnOnce(TableConfig) -> Fut,
        Fut: Future<Output = RenamePersistOutcome>,
    {
        let permit = self
            .core
            .durability_handle()
            .write_domain
            .admit()
            .await
            .map_err(|error| DbError::Internal(error.to_string()))?;
        self.check_rename_pair(from, to)?;
        if config.name != to || from == to {
            return Err(DbError::Validation(
                "rename recovery identity mismatch".into(),
            ));
        }
        let (source, destination, destination_identity_matches) =
            self.core.catalog().config_pair_matches(from, to, &config);
        let source_matches = source
            .as_ref()
            .is_some_and(|source| source.store_id.as_ref() == expected_source_id);
        if source.is_some() && !source_matches {
            return Err(DbError::Validation(
                "rename source incarnation changed".into(),
            ));
        }
        if destination.is_some() && !destination_identity_matches {
            return Err(DbError::Validation(
                "rename destination incarnation changed".into(),
            ));
        }
        if !source_matches {
            // When the source registration is absent and only the destination
            // exists, the persist runs unfenced — there is no source manager or
            // freeze to hold; the returned conflict is advisory, not a
            // retryable fence (M7-style metric stale handles unchanged).
            if source.is_none() && destination.is_some() {
                return match persist(config.clone()).await {
                    RenamePersistOutcome::Committed => Ok(false),
                    RenamePersistOutcome::NotCommitted(error)
                    | RenamePersistOutcome::InDoubt(error) => {
                        Err(rename_in_doubt_error(from, to, error))
                    }
                };
            }
            return Err(DbError::Validation(
                "rename source registration is missing".into(),
            ));
        }
        let pair = self.take_rename_fence(from, to);
        let source_table = match pair.as_ref() {
            Some(pair) => match pair.freeze.as_ref::<RenameFreeze>() {
                Some(freeze) => freeze.source().clone(),
                None => self.get_table_admitted(from, &permit).await?,
            },
            None => self.get_table_admitted(from, &permit).await?,
        };
        let mut attempt = RenameAttempt::new(self, &permit, from, to, source_table, pair);
        if !attempt.was_fenced() {
            match RenameFreeze::begin(attempt.source().clone(), &permit).await {
                Ok(freeze) => attempt.install_freeze(freeze),
                Err(error) => return Err(error),
            }
        }
        attempt.mark_persisting();
        match persist(config.clone()).await {
            RenamePersistOutcome::Committed => {
                self.remove_table(from, &permit);
                let registered = if destination.is_none() {
                    self.add_table(config, &permit);
                    true
                } else {
                    false
                };
                attempt.commit();
                Ok(registered)
            }
            RenamePersistOutcome::NotCommitted(error) | RenamePersistOutcome::InDoubt(error) => {
                attempt.into_doubt();
                Err(rename_in_doubt_error(from, to, error))
            }
        }
    }

    async fn rename_table_transition<F, Fut>(
        &self,
        from: &str,
        to: &str,
        persist: F,
    ) -> DbResult<bool>
    where
        F: FnOnce(TableConfig) -> Fut,
        Fut: Future<Output = RenamePersistOutcome>,
    {
        let permit = self
            .core
            .durability_handle()
            .write_domain
            .admit()
            .await
            .map_err(|error| DbError::Internal(error.to_string()))?;
        self.check_rename_pair(from, to)?;
        if !self.core.catalog().has_table(from) {
            return Ok(false);
        }
        if self.core.catalog().has_table(to) {
            return Err(DbError::Validation(format!("Table '{to}' already exists")));
        }
        let pair = self.take_rename_fence(from, to);
        let source = match pair.as_ref() {
            Some(pair) => match pair.freeze.as_ref::<RenameFreeze>() {
                Some(freeze) => freeze.source().clone(),
                None => self.get_table_admitted(from, &permit).await?,
            },
            None => self.get_table_admitted(from, &permit).await?,
        };
        let mut attempt = RenameAttempt::new(self, &permit, from, to, source, pair);
        if !attempt.was_fenced() {
            match RenameFreeze::begin(attempt.source().clone(), &permit).await {
                Ok(freeze) => attempt.install_freeze(freeze),
                Err(error) => return Err(error),
            }
        }
        let fenced = attempt.was_fenced();
        if let Err(error) = attempt.freeze()?.await_visible(self, from).await {
            attempt.abort();
            return Err(if fenced {
                rename_in_doubt_error(from, to, error)
            } else {
                error
            });
        }
        let config = match self
            .prepare_table_rename_admitted(from, to, &permit, attempt.freeze()?)
            .await
        {
            Ok(Some(config)) => config,
            Ok(None) => {
                attempt.abort();
                return Ok(false);
            }
            Err(error) => {
                attempt.abort();
                return Err(if fenced {
                    rename_in_doubt_error(from, to, error)
                } else {
                    error
                });
            }
        };
        attempt.mark_persisting();
        match persist(config.clone()).await {
            RenamePersistOutcome::Committed => {
                self.commit_table_rename_admitted(from, config, &permit)
                    .await?;

                attempt.commit();
                Ok(true)
            }
            RenamePersistOutcome::NotCommitted(error) if !fenced => {
                attempt.abort();
                Err(error)
            }
            RenamePersistOutcome::NotCommitted(error) | RenamePersistOutcome::InDoubt(error) => {
                attempt.into_doubt();
                Err(rename_in_doubt_error(from, to, error))
            }
        }
    }

    /// Rename while holding this repository's write-admission permit.
    ///
    /// Copies into stores with a fresh id, leaves the old stores orphaned, and
    /// retires the old manager after catalogue registration. A fenced pair is
    /// refused because this method has no catalogue persistence callback with
    /// which to resolve an unknown outcome.
    pub async fn rename_table_stores_admitted(
        &self,
        from: &str,
        to: &str,
        permit: &RepoWritePermit,
    ) -> DbResult<bool> {
        self.core
            .durability_handle()
            .write_domain
            .debug_check_permit(permit);
        // F-68 (#895) cluster D / task #124 diagnostic instrumentation:
        // timestamped entry/exit — NOT a behavior change, logging only.
        let rts_started = std::time::Instant::now();
        log::debug!("rename_table_stores: enter from={from} to={to}");
        self.check_rename_pair(from, to)?;
        if !self.core.catalog().has_table(from) {
            return Ok(false);
        }
        if self.core.catalog().has_table(to) {
            return Err(DbError::Validation(format!("Table '{to}' already exists")));
        }
        self.check_table_rename_fence(from)?;
        self.check_table_rename_fence(to)?;
        let source = self.core.get_table_admitted_unfenced(from, permit).await?;
        let freeze = RenameFreeze::begin(source, permit).await?;
        freeze.await_visible(self, from).await?;
        let config = match self
            .prepare_table_rename_admitted(from, to, permit, &freeze)
            .await
        {
            Ok(Some(config)) => config,
            Ok(None) => {
                return Ok(false);
            }
            Err(error) => {
                return Err(error);
            }
        };
        self.commit_table_rename_admitted(from, config, permit)
            .await?;

        freeze.retire();
        log::debug!(
            "rename_table_stores: exit from={from} to={to} elapsed={:?}",
            rts_started.elapsed()
        );
        Ok(true)
    }

    /// Prepare the copied stores and destination config while holding admission.
    ///
    /// The caller must first make the freeze visible to existing lookups. This
    /// drains committed MVCC overlays into history before physical store copy,
    /// then persists the source counter before copying data, info, and history.
    /// The source must remain frozen throughout. The MVCC `Arc` is cloned from
    /// the SCC bucket before awaiting, so no bucket guard is held across async work.
    pub(crate) async fn prepare_table_rename_admitted(
        &self,
        from: &str,
        to: &str,
        permit: &RepoWritePermit,
        freeze: &RenameFreeze,
    ) -> DbResult<Option<TableConfig>> {
        self.core
            .durability_handle()
            .write_domain
            .debug_check_permit(permit);
        let Some(config) = self.core.catalog().table_config(from) else {
            return Ok(None);
        };
        if self.core.catalog().has_table(to) {
            return Err(DbError::Validation(format!("Table '{to}' already exists")));
        }
        // 0. Force-drain the source table's MVCC overlay into
        //    `__history__<from>` so the copy picks up EVERY committed row.
        //    `drain_to_history` snapshots the overlay up to the visibility
        //    watermark, writes each version through
        //    `write_committed_to_history`, advances the durable watermark,
        //    and reclaims the overlay — synchronously. Without this, rows
        //    that still live only in the in-memory overlay (not yet drained
        //    by the background drainer) would be missed by the store copy.
        //    Idempotent: if the overlay is already drained (all entries are
        //    in history), this is a no-op.
        // Clone the Arc out: `get_sync`'s entry guard holds the bucket lock,
        // and the drainer's `iter_sync` over this map parks its thread on it,
        // so holding it across the awaits below wedges a current-thread
        // runtime (#589 class).
        if let Some(mvcc) = self.core.catalog().mvcc_for_table(from) {
            // F-68 (#895) cluster D / task #124: timestamped before/after
            // around `drain_to_history` — this synchronously walks the overlay
            // and writes each drained version to `__history__`, the one
            // potentially I/O-heavy step in this function.
            let drain_started = std::time::Instant::now();
            log::debug!("rename_table_stores: calling drain_to_history from={from}");
            mvcc.drain_to_history().await?;
            log::debug!(
                "rename_table_stores: drain_to_history returned after {:?}",
                drain_started.elapsed()
            );
        }
        freeze.source().counter().persist_admitted(permit).await?;
        let config = self.copy_table_stores_for_rename(from, to, config).await?;
        Ok(Some(config))
    }

    async fn copy_table_stores_for_rename(
        &self,
        from: &str,
        to: &str,
        old_config: TableConfig,
    ) -> DbResult<TableConfig> {
        let source_names =
            super::table_store::table_store_names(from, old_config.store_id.as_ref());
        let fresh_id = super::table_store::new_table_store_id();
        let new_config = TableConfig {
            name: to.into(),
            enable_indexes: old_config.enable_indexes,
            store_id: Some(fresh_id),
        };
        new_config.validate_store_name()?;
        let destination_names =
            super::table_store::table_store_names(to, new_config.store_id.as_ref());
        let existing = self.core.repo().stores_list().await?;
        if existing.iter().any(|name| {
            [
                destination_names.data.as_str(),
                destination_names.info.as_str(),
                destination_names.history.as_str(),
            ]
            .contains(&name.as_str())
        }) {
            return Err(DbError::Validation(format!(
                "rename destination storage for '{to}' is already present"
            )));
        }
        super::table_store::copy_table_store(
            self.core.repo(),
            &source_names.data,
            &destination_names.data,
        )
        .await?;
        super::table_store::copy_table_store(
            self.core.repo(),
            &source_names.info,
            &destination_names.info,
        )
        .await?;
        super::table_store::copy_table_store(
            self.core.repo(),
            &source_names.history,
            &destination_names.history,
        )
        .await?;
        Ok(new_config)
    }

    /// Commit the prepared rename after its destination catalogue row is durable.
    async fn commit_table_rename_admitted(
        &self,
        from: &str,
        config: TableConfig,
        permit: &RepoWritePermit,
    ) -> DbResult<()> {
        self.core
            .durability_handle()
            .write_domain
            .debug_check_permit(permit);
        let to = config.name.clone();
        if to == from {
            return Err(DbError::Validation(
                "rename source and destination must differ".into(),
            ));
        }
        if self.core.catalog().has_table(&to) {
            return Err(DbError::Validation(format!("Table '{to}' already exists")));
        }
        if !self.core.catalog().has_table(from) {
            return Err(DbError::NotFound(format!("Table '{from}' not found")));
        }
        self.remove_table(from, permit);
        self.add_table(config, permit);
        Ok(())
    }
}

fn rename_in_doubt_error(from: &str, to: &str, error: DbError) -> DbError {
    DbError::Conflict(format!("rename in doubt: '{from}' to '{to}': {error}"))
}

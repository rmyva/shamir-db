use std::sync::Arc;

use bytes::Bytes;
use shamir_collections::TFxMap;
use shamir_storage::error::{DbError, DbResult};
use shamir_storage::types::{KvOp, Repo, Store};
use shamir_tx::VersionGuard;

use crate::meta::recovery_marker::{
    load_last_committed, load_next_tx_id_snapshot, save_last_committed_flushed,
    save_next_tx_id_snapshot,
};
use crate::meta::repl::bookmark_v3::{save_bookmark_v3, BookmarkV3};
use crate::repo::RepoInstance;

use super::inventory::SeedTable;
use crate::repo::seed::install::translate_row;
use crate::repo::seed::install::{SeedInstallState, SeedReceipt};

/// Offline, exclusively owned UNPUBLISHED destination. The server must verify
/// the artifact/trusted digest, hold maintenance ownership and create a new
/// staging repository before begin. Never put this repo in a serving registry.
/// No row-level resume: any error/cancellation poisons this attempt; abandon it.
#[must_use = "finish the seed image or preserve it as abandoned staging"]
pub struct SeedImageWriter {
    repo: RepoInstance,
    receipt: SeedReceipt,
    tables: Vec<SeedTable>,
    counts: Vec<u64>,
    last_keys: Vec<Option<Bytes>>,
    remap: TFxMap<u64, u64>,
    names: TFxMap<String, u64>,
    binding_bytes: usize,
    guard: Option<VersionGuard>,
    poisoned: bool,
    #[cfg(test)]
    pub(super) fail_at: Option<usize>,
    #[cfg(test)]
    pub(super) boundaries: Vec<usize>,
    #[cfg(test)]
    pub(super) bookmark_store: Option<Arc<dyn Store>>,
}

impl SeedImageWriter {
    /// cancel-safe: NO — Installing, floors and table creation may persist.
    /// Cancellation leaves an unservable staging image, never a resumed writer.
    pub async fn begin(
        repo: RepoInstance,
        receipt: SeedReceipt,
        tables: Vec<SeedTable>,
    ) -> DbResult<Self> {
        if !cfg!(unix) && !cfg!(test) {
            return Err(DbError::Storage(
                "durable seed installation requires Unix".into(),
            ));
        }
        receipt.validate()?;
        if tables.len() > 65_536 {
            return Err(DbError::Codec(
                "seed table inventory exceeds count budget".into(),
            ));
        }
        let mut inventory_bytes = 0usize;
        let mut table_names = TFxMap::default();
        for table in &tables {
            inventory_bytes = inventory_bytes
                .checked_add(table.name.len().saturating_add(32))
                .ok_or_else(|| DbError::Codec("seed inventory overflow".into()))?;
            if table.name.is_empty()
                || table.name.len() > 4096
                || table.name.starts_with("__")
                || table_names.insert(table.name.clone(), ()).is_some()
                || inventory_bytes > 64 * 1024 * 1024
            {
                return Err(DbError::Codec("invalid seed table inventory".into()));
            }
        }
        if repo
            .core
            .repo()
            .stores_list()
            .await?
            .iter()
            .any(|name| !matches!(name.as_str(), "__tx__" | "__interner__" | "__changelog__"))
        {
            return Err(DbError::Storage(
                "seed destination contains existing stores".into(),
            ));
        }
        let permit = repo
            .write_domain()
            .admit()
            .await
            .map_err(|error| DbError::Internal(error.to_string()))?;
        let info = repo.tx_info_store().await?;
        if repo.table_count() != 0
            || SeedInstallState::load(&info).await?.is_some()
            || repo.replication_bookmark().await? != 0
            || load_last_committed(&info).await?.unwrap_or(0) != 0
            || load_next_tx_id_snapshot(&info).await?.unwrap_or(1) != 1
            || !repo.repo_interner().await?.get().await?.is_empty()
        {
            return Err(DbError::Storage(
                "seed destination is not fresh staging".into(),
            ));
        }
        let gate = repo.tx_gate().await?;
        if gate.version_allocation_high_water_mark() != 0
            || gate.peek_next_tx_id() != 1
            || gate.requires_reopen()
        {
            return Err(DbError::Storage(
                "seed destination gate is not fresh".into(),
            ));
        }
        let journal = crate::repo::changelog_store::StoreChangelog::new(
            repo.core.repo().store_get("__changelog__").await?,
        );
        if journal.load_clean_horizon().await? != 0
            || journal.load_gap_marker().await? != (None, false)
        {
            return Err(DbError::Storage(
                "seed destination has existing journal metadata".into(),
            ));
        }
        SeedInstallState::Installing(receipt.clone())
            .save(&info)
            .await?;
        // Same flushed allocation-evidence helper as WAL-less follower apply.
        // Do not initialize the gate from floor 1 before the image exists.
        let mut guard = gate.assign_next_version_guarded();
        guard.visibility_started_without_wal();
        if guard.version() != 1 {
            return Err(DbError::Storage("seed baseline allocation is not 1".into()));
        }
        save_next_tx_id_snapshot(&info, 1).await?;
        save_last_committed_flushed(&info, 1).await?;
        // Destination identity is minted locally; the source stays in receipt.
        // Round-12 audit: the Round-11 pins here are reverted — the seed
        // writer runs in the CLI seed-install child process, never on the
        // main-thread server poll chain, and its admitted twins are already
        // shallow (identity load / table lookup). No heap pinning needed.
        repo.repo_identity_admitted(&permit).await?;
        let mut configs = Vec::with_capacity(tables.len());
        for table in &tables {
            let mut config = table.config().with_new_store_id();
            repo.add_table_admitted(config.clone(), &permit).await?;
            config = repo
                .table_config(&table.name)
                .ok_or_else(|| DbError::Storage("seed table config missing".into()))?;
            configs.push(config);
            let manager = repo.get_table_admitted(&table.name, &permit).await?;
            if manager.mvcc_store().is_none() || manager.has_any_index() {
                return Err(DbError::Storage(
                    "seed destination must be index-free MVCC".into(),
                ));
            }
        }
        // Any failure after inventory publication leaves a recognizable
        // incomplete seed attempt that recovery can reject before table load.
        SeedTable::save_inventory(&info, &tables).await?;
        SeedTable::save_local_ids(&info, &tables, &configs).await?;
        Ok(Self {
            counts: vec![0; tables.len()],
            last_keys: vec![None; tables.len()],
            repo,
            receipt,
            tables,
            remap: TFxMap::default(),
            names: TFxMap::default(),
            binding_bytes: 0,
            guard: Some(guard),
            poisoned: false,
            #[cfg(test)]
            fail_at: None,
            #[cfg(test)]
            boundaries: Vec::new(),
            #[cfg(test)]
            bookmark_store: None,
        })
    }

    fn enter(&mut self) -> DbResult<()> {
        if self.poisoned {
            return Err(DbError::Storage(
                "seed attempt requires fresh staging".into(),
            ));
        }
        // An error or cancellation never clears this flag.
        self.poisoned = true;
        Ok(())
    }

    /// Add bindings incrementally, with a 64 MiB aggregate name/entry budget.
    /// Duplicate ids or names are refused, even if they would translate alike.
    pub async fn binding(&mut self, source_id: u64, name: &str) -> DbResult<()> {
        self.enter()?;
        let bytes = self
            .binding_bytes
            .checked_add(name.len().saturating_add(32))
            .ok_or_else(|| DbError::Codec("seed binding overflow".into()))?;
        if source_id == 0
            || name.is_empty()
            || bytes > 64 * 1024 * 1024
            || self.remap.contains_key(&source_id)
            || self.names.contains_key(name)
        {
            return Err(DbError::Codec(
                "conflicting or oversized seed binding".into(),
            ));
        }
        let manager = self.repo.repo_interner().await?;
        // RU8: the seed writer is counted but never quota-rejected.
        let local = manager
            .get()
            .await?
            .touch_ind_unmetered(name)
            .map_err(shamir_storage::error::DbError::from)?
            .into_key()
            .id();
        self.remap.insert(source_id, local);
        self.names.insert(name.to_owned(), source_id);
        self.binding_bytes = bytes;
        self.poisoned = false;
        Ok(())
    }

    /// Accept exactly one verified record. Keys must be strictly ascending per
    /// table across chunk boundaries; this detects duplicates without a key set.
    /// Payload container lengths must already be checked by the SEED-2 verifier.
    /// cancel-safe: NO — history may persist; cancellation abandons this image.
    pub async fn row(&mut self, table: usize, key: Bytes, payload: Bytes) -> DbResult<()> {
        self.enter()?;
        let entry = self
            .tables
            .get(table)
            .ok_or_else(|| DbError::Codec("seed table ordinal out of range".into()))?;
        if key
            .len()
            .checked_add(payload.len())
            .is_none_or(|size| size > 16 * 1024 * 1024)
            || self.last_keys[table]
                .as_ref()
                .is_some_and(|last| last.as_ref() >= key.as_ref())
            || self.counts[table] >= entry.live_records
        {
            return Err(DbError::Codec(
                "seed duplicate/unordered key or excess rows".into(),
            ));
        }
        // CPU-bound decode/remap off the async executor, one capped row at a
        // time. Map cloning is avoided by moving it temporarily into the task.
        let remap = std::mem::take(&mut self.remap);
        let (translated, remap) =
            tokio::task::spawn_blocking(move || (translate_row(payload, &remap), remap))
                .await
                .map_err(|e| DbError::Codec(format!("seed translation task: {e}")))?;
        self.remap = remap;
        let translated = translated?;
        self.boundary(0)?;
        let manager = self.repo.get_table(&self.tables[table].name).await?;
        let mvcc = manager
            .mvcc_store()
            .ok_or_else(|| DbError::Storage("seed table has no MVCC history".into()))?;
        mvcc.write_committed_to_history(&[KvOp::Set(key.clone().into(), translated)], 1)
            .await?;
        self.counts[table] = self.counts[table]
            .checked_add(1)
            .ok_or_else(|| DbError::Codec("seed row count overflow".into()))?;
        self.last_keys[table] = Some(key);
        self.poisoned = false;
        Ok(())
    }

    /// Schemas are db-owned: the caller must finish compiling/persisting them
    /// before this call and pass EVERY backing metadata store for explicit flush.
    /// Returned receipt authorizes publication, NOT subscription activation.
    /// cancel-safe: NO — bookmark/Complete set may finish without acknowledgement.
    /// On error preserve staging. An exact completed retry must re-flush state.
    /// PR6b-3: the bookmark record written here is the NEW v3 baseline (upstream
    /// bookmark = seed source version, local clean horizon = the certified local
    /// baseline, no outstanding receipt, fence absent until later identity
    /// adoption) — new-format installation, not migration.
    pub async fn finish(mut self, schema_stores: &[Arc<dyn Store>]) -> DbResult<SeedReceipt> {
        self.enter()?;
        for (table, count) in self.tables.iter().zip(&self.counts) {
            if *count != table.live_records {
                return Err(DbError::Codec(format!(
                    "seed count mismatch: {}",
                    table.name,
                )));
            }
            let manager = self.repo.get_table(&table.name).await?;
            manager.counter().set(*count).await?;
            manager.info_store_read_view().flush().await?;
        }
        for store in schema_stores {
            store.flush().await?;
        }
        self.boundary(1)?;
        self.repo.flush_seed_metadata().await?;
        self.boundary(2)?;
        // Actual follower history durability API; any failed flush stops here.
        self.repo.flush_all_history().await?;
        self.boundary(3)?;
        let info = self.repo.tx_info_store().await?;
        SeedTable::save_inventory(&info, &self.tables).await?;
        let configs = self
            .tables
            .iter()
            .map(|table| {
                self.repo
                    .table_config(&table.name)
                    .ok_or_else(|| DbError::Storage("seed table config missing".into()))
            })
            .collect::<DbResult<Vec<_>>>()?;
        SeedTable::save_local_ids(&info, &self.tables, &configs).await?;
        save_next_tx_id_snapshot(&info, 1).await?;
        save_last_committed_flushed(&info, 1).await?;
        let gate = self.repo.tx_gate().await?;
        let guard = self
            .guard
            .take()
            .ok_or_else(|| DbError::Internal("seed version guard missing".into()))?;
        gate.mark_durable(1);
        guard.commit();
        let feed = self.repo.changefeed().await?.feed;
        feed.record_recovery_gap(1, 1).await?;
        feed.record_clean_horizon(1).await?;
        self.boundary(4)?;
        // This helper does BOTH set and explicit metadata flush (not just set).
        #[cfg(test)]
        let bookmark_info = self.bookmark_store.as_ref().unwrap_or(&info);
        #[cfg(not(test))]
        let bookmark_info = &info;
        save_bookmark_v3(
            bookmark_info,
            &BookmarkV3 {
                fence: None,
                bookmark: self.receipt.source_version,
                clean_through: Some(1),
                outstanding_receipt: None,
            },
        )
        .await?;
        self.boundary(5)?;
        SeedInstallState::Complete(self.receipt.clone())
            .save(&info)
            .await?;
        self.boundary(6)?;
        Ok(self.receipt)
    }

    fn boundary(&mut self, boundary: usize) -> DbResult<()> {
        #[cfg(test)]
        {
            self.boundaries.push(boundary);
            if self.fail_at == Some(boundary) {
                return Err(DbError::Storage(format!(
                    "injected seed boundary {boundary}",
                )));
            }
        }
        #[cfg(not(test))]
        let _ = boundary;
        Ok(())
    }
}

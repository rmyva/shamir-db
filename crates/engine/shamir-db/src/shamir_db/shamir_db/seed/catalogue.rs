use std::path::Path;
use std::sync::Arc;

#[cfg(feature = "fjall")]
use shamir_engine::repo::{BoxRepoFactory, RepoFactory};
use shamir_engine::repo::{RepoInstance, SeedInstallState, SeedReceipt, SeedTable};
#[cfg(feature = "fjall")]
use shamir_storage::types::Repo;
use shamir_storage::types::{RecordKey, Store};
use shamir_types::types::value::QueryValue;

use crate::access::{Actor, ResourceMeta};
use crate::shamir_db::shamir_db::{is_reserved_db_name, reserved_db_name_refusal};
use crate::{DbError, DbResult, ShamirDb};

use super::records_codec::decode_schema_records;

impl ShamirDb {
    pub async fn check_seed_target(
        &self,
        db: &str,
        repo: &str,
        path: &Path,
        receipt: &SeedReceipt,
    ) -> DbResult<()> {
        self.check_seed_target_inner(db, repo, path, receipt, None)
            .await
    }

    async fn check_seed_target_inner(
        &self,
        db: &str,
        repo: &str,
        path: &Path,
        receipt: &SeedReceipt,
        image: Option<&RepoInstance>,
    ) -> DbResult<()> {
        #[cfg(not(feature = "fjall"))]
        let _ = image;
        receipt.validate()?;
        if is_reserved_db_name(db) || is_reserved_db_name(&receipt.source_db) {
            return Err(reserved_db_name_refusal(if is_reserved_db_name(db) {
                db
            } else {
                &receipt.source_db
            }));
        }
        // R1c-4 follow-up defect 2: refuse seed publication onto a database
        // with an in-doubt marker — the install persists a `databases` row
        // (and repository/table rows) under this name, replacing the
        // identity the unresolved migration was applied to. Name-keyed
        // marker; the journal follows the stable identity.
        if self.schema_migration_in_doubt(db) {
            return Err(DbError::Validation(
                crate::shamir_db::execute::admin_db_schema_migration
                    ::schema_migration_lifecycle_refusal_message("publish seed onto", db),
            ));
        }
        if receipt.source_db != db || receipt.source_repo != repo {
            return Err(DbError::Storage(
                "seed destination names differ from receipt".into(),
            ));
        }
        let registered = self.system_store.load_repository_record(db, repo).await?;
        if let Some(record) = &registered {
            if !matches!(record["engine"].as_str(), Some("fjall" | "fjall_raw"))
                || record["path"].as_str() != path.to_str()
                || !path.join(".seed-install").is_file()
            {
                return Err(DbError::Storage(
                    "existing ordinary or conflicting repository".into(),
                ));
            }
        }
        let databases = self.system_store.load_databases().await?;
        let database = databases
            .iter()
            .find(|row| row["name"].as_str() == Some(db));
        if database.is_some_and(|row| {
            row["seed_artifact_digest"].as_str() != Some(receipt.artifact_digest.as_str())
        }) {
            return Err(DbError::Storage(
                "existing ordinary destination database".into(),
            ));
        }
        let tables = self.system_store.load_tables().await?;
        let existing = tables
            .iter()
            .filter(|row| row["db_name"].as_str() == Some(db))
            .collect::<Vec<_>>();
        if registered.is_some()
            || databases.iter().any(|row| row["name"].as_str() == Some(db))
            || !existing.is_empty()
        {
            // Only a matching completed image can justify a catalogue-only retry.
            #[cfg(not(feature = "fjall"))]
            return Err(DbError::Storage("seed publication requires fjall".into()));
            #[cfg(feature = "fjall")]
            {
                if !path.join(".seed-install").is_file() {
                    return Err(DbError::Storage("existing destination database".into()));
                }
                let backing = match image
                    .cloned()
                    .or_else(|| self.get_db(db).and_then(|database| database.get_repo(repo)))
                {
                    Some(image) => image.seed_backing_repo(),
                    None => BoxRepoFactory::fjall_raw(path).create().await?,
                };
                let info = backing.store_get("__tx__").await?;
                let completed = SeedInstallState::require_complete(&info, true).await?;
                if completed.as_ref() != Some(receipt) {
                    return Err(DbError::Storage("existing destination database".into()));
                }
                let schemas = backing.store_get("__seed_schema__").await?;
                let records = load_schema_records(&schemas).await?;
                let repositories = self.system_store.load_repositories().await?;
                let unrelated_repository = repositories.iter().any(|row| {
                    row["db_name"].as_str() == Some(db) && row["repo_name"].as_str() != Some(repo)
                });
                let conflicting_table = existing
                    .iter()
                    .any(|row| !records.iter().any(|record| record == *row));
                if unrelated_repository || conflicting_table {
                    return Err(DbError::Storage("conflicting destination catalogue".into()));
                }
            }
        }
        self.check_seed_subscriptions(db, repo).await
    }

    /// Register metadata durably; the authoritative repository row is LAST.
    /// No serving registry insertion or subscription is performed here.
    pub async fn register_seed_image(
        &self,
        image: &RepoInstance,
        receipt: &SeedReceipt,
        path: &Path,
    ) -> DbResult<()> {
        self.check_seed_target_inner(
            &receipt.source_db,
            &receipt.source_repo,
            path,
            receipt,
            Some(image),
        )
        .await?;
        let info = image.tx_info_store().await?;
        if !SeedInstallState::completed_retry(&info, receipt).await? {
            return Err(DbError::Storage("publication requires Complete".into()));
        }
        let configs = SeedTable::load_configs(&info).await?;
        let schemas = image.seed_schema_store().await?;
        let records = load_schema_records(&schemas).await?;
        if records.len() != configs.len()
            || configs.iter().zip(&records).any(|(config, record)| {
                record["db_name"].as_str() != Some(receipt.source_db.as_str())
                    || record["repo_name"].as_str() != Some(receipt.source_repo.as_str())
                    || record["table_name"].as_str() != Some(config.name.as_str())
            })
        {
            return Err(DbError::Storage("seed schema inventory mismatch".into()));
        }
        if configs.len() != records.len()
            || configs.iter().zip(&records).any(|(config, record)| {
                config.store_id.as_ref().map(ToString::to_string).as_deref()
                    != record.get("store_id").and_then(QueryValue::as_str)
            })
        {
            return Err(DbError::Storage(
                "seed local table ID catalogue mismatch".into(),
            ));
        }
        let existing = self.system_store.load_tables().await?;
        for record in &records {
            if existing.iter().any(|row| {
                row["db_name"] == record["db_name"]
                    && row["repo_name"] == record["repo_name"]
                    && row["table_name"] == record["table_name"]
                    && row != record
            }) {
                return Err(DbError::Storage("conflicting table metadata".into()));
            }
        }
        if self
            .system_store
            .load_repository_record(&receipt.source_db, &receipt.source_repo)
            .await?
            .is_some()
        {
            return self.flush_seed_catalogue().await;
        }
        let meta = ResourceMeta::owned_enforced(Actor::System);
        if self
            .system_store
            .load_database(&receipt.source_db)
            .await?
            .is_none()
        {
            let mut fields = shamir_types::types::common::new_map();
            fields.insert("name".into(), QueryValue::Str(receipt.source_db.clone()));
            fields.insert(
                "seed_artifact_digest".into(),
                QueryValue::Str(receipt.artifact_digest.clone()),
            );
            self.system_store
                .save_database(&receipt.source_db, &QueryValue::Map(fields), &meta)
                .await?;
        }
        for record in &records {
            if !existing.contains(record) {
                self.system_store.save_table_meta(record).await?;
            }
        }
        self.flush_seed_catalogue().await?;
        let path = path
            .to_str()
            .ok_or_else(|| DbError::Storage("seed path is not UTF-8".into()))?;
        self.system_store
            .save_repository(
                &receipt.source_db,
                &receipt.source_repo,
                // S5c-X PR6b-3: the staged image is RAW Fjall — record the
                // raw kind so the boot path re-attaches it UNBUFFERED via
                // `factory_from_meta`. Kind `"fjall"` maps to the
                // MemBuffer-wrapped factory, whose repos report
                // `supports_atomic_batch() == false` and stall seeded
                // followers on the admission capability check forever.
                "fjall_raw",
                Some(path),
                &meta,
                None,
            )
            .await?;
        self.flush_seed_catalogue().await
    }
}

async fn load_schema_records(store: &Arc<dyn Store>) -> DbResult<Vec<QueryValue>> {
    let bytes = store.get(RecordKey::from_slice(b"schemas")).await?;
    if bytes.len() > 64 * 1024 * 1024 {
        return Err(DbError::Codec(
            "seed schema metadata budget exceeded".into(),
        ));
    }
    let records = decode_schema_records(&bytes)?;
    if records.len() > 65_536 {
        return Err(DbError::Codec(
            "seed schema inventory count exceeded".into(),
        ));
    }
    Ok(records)
}

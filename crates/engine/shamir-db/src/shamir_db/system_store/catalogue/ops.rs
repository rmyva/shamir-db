use std::path::Path;

use crate::{DbError, DbResult};
use shamir_types::access::ResourceMeta;
use shamir_types::types::common::new_map;
use shamir_types::types::value::QueryValue;

use crate::access::Actor;
use crate::engine::repo::RenamePersistOutcome;

use super::super::store::{qv_map1, qv_map2, TABLE_DATABASES, TABLE_REPOSITORIES, TABLE_TABLES};
use super::super::SystemStore;
use super::rename_key::TableRenameKey;

fn table_catalogue_key(db_name: &str, repo_name: &str, table_name: &str) -> QueryValue {
    let mut key = new_map();
    key.insert("db_name".into(), QueryValue::Str(db_name.into()));
    key.insert("repo_name".into(), QueryValue::Str(repo_name.into()));
    key.insert("table_name".into(), QueryValue::Str(table_name.into()));
    QueryValue::Map(key)
}

impl SystemStore {
    /// Save database metadata. Injects the `owner`/`group`/`mode` fields
    /// from `meta` into the record before persisting (P3 metadata plates).
    pub async fn save_database(
        &self,
        name: &str,
        record: &QueryValue,
        meta: &ResourceMeta,
    ) -> DbResult<()> {
        self.catalogue_write_fault_gate()?;
        let mut rec = record.clone();
        meta.inject_into(&mut rec);
        let table = self.table(TABLE_DATABASES).await?;
        let op = crate::query::write::SetOp {
            set: crate::query::TableRef::new(TABLE_DATABASES),
            key: qv_map1("name", QueryValue::Str(name.to_string())),
            value: rec.clone(),
        };
        self.set_via_implicit_tx(&table, &op).await?;
        self.persist_table_interner(&table, Some(&rec)).await?;
        // Durable DDL — sync_wal + flush via shared helper.
        self.flush_system_repo_durable(&table).await?;
        Ok(())
    }

    /// Remove database metadata.
    pub async fn remove_database(&self, name: &str) -> DbResult<()> {
        self.catalogue_write_fault_gate()?;
        let table = self.table(TABLE_DATABASES).await?;
        let op = crate::query::write::DeleteOp {
            delete_from: crate::query::TableRef::new(TABLE_DATABASES),
            where_clause: crate::query::filter::Filter::Eq {
                field: vec!["name".to_string()],
                value: crate::query::filter::FilterValue::String(name.to_string()),
            },
            select: None,
            expected_version: None,
        };
        self.delete_via_implicit_tx(&table, &op).await?;
        // Durable DDL — sync_wal + flush via shared helper.
        self.flush_system_repo_durable(&table).await?;
        Ok(())
    }

    /// Distinguishes genuine absence from corrupt or incomplete catalogue scans.
    pub async fn load_databases(&self) -> DbResult<Vec<QueryValue>> {
        let table = self.table(TABLE_DATABASES).await?;
        let refs = crate::types::common::new_map();
        let ctx = table.info_store_read_view().filter_context(&refs).await?;
        let query = crate::query::read::ReadQuery::new(TABLE_DATABASES);
        let result = table.read(&query, &ctx).await?;
        if !result.corrupt_records.is_empty() {
            return Err(DbError::Internal(format!(
                "corrupt database catalogue: {} unreadable record(s)",
                result.corrupt_records.len()
            )));
        }
        Ok(result
            .records
            .into_iter()
            .map(|r| r.as_value().into_owned())
            .collect())
    }

    // Repository paths: catalogued relative to the data directory

    /// Catalogue form of a repository path: relative to the data directory
    /// (`/`-separated) when it lies under it, so a moved or restored data
    /// directory still opens its own stores; other paths stay as given.
    fn catalogue_repo_path(&self, path: &str) -> String {
        let Some(base) = self.base_dir.as_deref() else {
            return path.to_string();
        };
        match Path::new(path).strip_prefix(base) {
            Ok(rel) if rel.as_os_str().is_empty() => path.to_string(),
            Ok(rel) => rel
                .components()
                .map(|part| part.as_os_str().to_string_lossy().into_owned())
                .collect::<Vec<_>>()
                .join("/"),
            Err(_) => path.to_string(),
        }
    }

    /// Runtime form of a catalogued repository path: a relative path is
    /// resolved against the data directory; an absolute one is kept.
    fn resolved_repo_path(&self, stored: &str) -> String {
        match self.base_dir.as_deref() {
            // Join component by component so the platform separator is used.
            Some(base) if Path::new(stored).is_relative() => stored
                .split('/')
                .fold(base.to_path_buf(), |path, part| path.join(part))
                .to_string_lossy()
                .into_owned(),
            _ => stored.to_string(),
        }
    }

    /// Repository record with its `path` in catalogue form.
    fn catalogue_repo_record(&self, record: QueryValue) -> QueryValue {
        self.map_repo_path(record, |store, path| store.catalogue_repo_path(path))
    }

    /// Repository record with its `path` in runtime form.
    fn resolve_repo_record(&self, record: QueryValue) -> QueryValue {
        self.map_repo_path(record, |store, path| store.resolved_repo_path(path))
    }

    fn map_repo_path(
        &self,
        mut record: QueryValue,
        convert: impl Fn(&Self, &str) -> String,
    ) -> QueryValue {
        if let QueryValue::Map(map) = &mut record {
            if let Some(QueryValue::Str(path)) = map.get("path") {
                let converted = convert(self, path);
                map.insert("path".to_string(), QueryValue::Str(converted));
            }
        }
        record
    }

    // Repository metadata

    /// Save repository metadata. Injects `owner`/`group`/`mode` from `meta`
    /// and, when `Some`, the per-repo durability override under the
    /// `durability` key (encoded by the `repo_durability` codec).
    pub async fn save_repository(
        &self,
        db_name: &str,
        repo_name: &str,
        engine: &str,
        path: Option<&str>,
        meta: &ResourceMeta,
        durability: Option<&shamir_query_types::durability::DurabilityOverride>,
    ) -> DbResult<()> {
        self.catalogue_write_fault_gate()?;
        let catalogued = path.map(|path| self.catalogue_repo_path(path));
        let record = crate::shamir_db::repo_durability::repository_record(
            db_name,
            repo_name,
            engine,
            catalogued.as_deref(),
            meta,
            durability,
        );
        let table = self.table(TABLE_REPOSITORIES).await?;
        let op = crate::query::write::SetOp {
            set: crate::query::TableRef::new(TABLE_REPOSITORIES),
            key: qv_map2(
                "db_name",
                QueryValue::Str(db_name.to_string()),
                "repo_name",
                QueryValue::Str(repo_name.to_string()),
            ),
            value: record,
        };
        self.set_via_implicit_tx(&table, &op).await?;
        self.persist_table_interner(&table, None).await?;
        self.flush_system_repo_durable(&table).await?;
        Ok(())
    }

    /// Remove repository metadata.
    pub async fn remove_repository(&self, db_name: &str, repo_name: &str) -> DbResult<()> {
        self.catalogue_write_fault_gate()?;
        let table = self.table(TABLE_REPOSITORIES).await?;
        let op = crate::query::write::DeleteOp {
            delete_from: crate::query::TableRef::new(TABLE_REPOSITORIES),
            where_clause: crate::query::filter::Filter::And {
                filters: vec![
                    crate::query::filter::Filter::Eq {
                        field: vec!["db_name".to_string()],
                        value: crate::query::filter::FilterValue::String(db_name.to_string()),
                    },
                    crate::query::filter::Filter::Eq {
                        field: vec!["repo_name".to_string()],
                        value: crate::query::filter::FilterValue::String(repo_name.to_string()),
                    },
                ],
            },
            select: None,
            expected_version: None,
        };
        self.delete_via_implicit_tx(&table, &op).await?;
        self.flush_system_repo_durable(&table).await?;
        Ok(())
    }

    /// Load all repository records.
    pub async fn load_repositories(&self) -> DbResult<Vec<QueryValue>> {
        #[cfg(test)]
        self.count_full_catalogue_load();
        let table = self.table(TABLE_REPOSITORIES).await?;
        let refs = crate::types::common::new_map();
        let ctx = table.info_store_read_view().filter_context(&refs).await?;
        let query = crate::query::read::ReadQuery::new(TABLE_REPOSITORIES);
        let result = table.read(&query, &ctx).await?;
        if !result.corrupt_records.is_empty() {
            return Err(DbError::Internal(format!(
                "corrupt repository catalogue: {} unreadable record(s)",
                result.corrupt_records.len()
            )));
        }
        Ok(result
            .records
            .into_iter()
            .map(|r| self.resolve_repo_record(r.as_value().into_owned()))
            .collect())
    }

    /// Load a single repository's catalogue record by `(db_name, repo_name)`.
    /// Returns `Ok(None)` when no matching row exists.
    // Filtered read over an index-less catalogue table; this preserves corrupt-record errors.
    pub async fn load_repository_record(
        &self,
        db_name: &str,
        repo_name: &str,
    ) -> DbResult<Option<QueryValue>> {
        self.load_repository(db_name, repo_name).await
    }

    // Table catalogue (per-repo table list — I.2)

    /// Persist one table's catalogue entry. Keyed by
    /// `(db_name, repo_name, table_name)` so re-saving the same table is an
    /// idempotent upsert. `enable_indexes` is the only other field of
    /// `TableConfig`, so the record carries enough to faithfully re-create
    /// the table on open.
    pub async fn save_table(
        &self,
        db_name: &str,
        repo_name: &str,
        table_name: &str,
        enable_indexes: bool,
        store_id: Option<&shamir_types::types::record_id::RecordId>,
        meta: &ResourceMeta,
    ) -> DbResult<()> {
        self.catalogue_write_fault_gate()?;
        if let Some(store_id) = store_id {
            store_id
                .to_string()
                .parse::<shamir_types::types::record_id::RecordId>()
                .map_err(|error| DbError::Internal(format!("invalid table store_id: {error}")))?;
        }
        let mut m = new_map();
        m.insert("db_name".to_string(), QueryValue::Str(db_name.to_string()));
        m.insert(
            "repo_name".to_string(),
            QueryValue::Str(repo_name.to_string()),
        );
        m.insert(
            "table_name".to_string(),
            QueryValue::Str(table_name.to_string()),
        );
        m.insert(
            "enable_indexes".to_string(),
            QueryValue::Bool(enable_indexes),
        );
        if let Some(store_id) = store_id {
            m.insert(
                "store_id".to_string(),
                QueryValue::Str(store_id.to_string()),
            );
        }
        let mut record = QueryValue::Map(m);
        meta.inject_into(&mut record);
        let table = self.table(TABLE_TABLES).await?;
        let mut key_m = new_map();
        key_m.insert("db_name".to_string(), QueryValue::Str(db_name.to_string()));
        key_m.insert(
            "repo_name".to_string(),
            QueryValue::Str(repo_name.to_string()),
        );
        key_m.insert(
            "table_name".to_string(),
            QueryValue::Str(table_name.to_string()),
        );
        let op = crate::query::write::SetOp {
            set: crate::query::TableRef::new(TABLE_TABLES),
            key: QueryValue::Map(key_m),
            value: record,
        };
        self.set_via_implicit_tx(&table, &op).await?;
        self.persist_table_interner(&table, None).await?;
        self.flush_system_repo_durable(&table).await?;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) async fn rename_table_row(
        &self,
        key: TableRenameKey<'_>,
        source_store_id: Option<&shamir_types::types::record_id::RecordId>,
        destination_store_id: &shamir_types::types::record_id::RecordId,
        expected_source: &QueryValue,
    ) -> DbResult<()> {
        match self
            .rename_table_row_outcome(key, source_store_id, destination_store_id, expected_source)
            .await
        {
            RenamePersistOutcome::Committed => Ok(()),
            RenamePersistOutcome::NotCommitted(error) | RenamePersistOutcome::InDoubt(error) => {
                Err(error)
            }
        }
    }

    pub(crate) async fn rename_table_row_outcome(
        &self,
        key: TableRenameKey<'_>,
        source_store_id: Option<&shamir_types::types::record_id::RecordId>,
        destination_store_id: &shamir_types::types::record_id::RecordId,
        expected_source: &QueryValue,
    ) -> RenamePersistOutcome {
        let TableRenameKey {
            db_name,
            repo_name,
            from,
            to,
        } = key;
        if let Err(error) = self.catalogue_write_fault_gate() {
            return RenamePersistOutcome::NotCommitted(error);
        }
        if let Some(id) = source_store_id {
            if let Err(error) = id
                .to_string()
                .parse::<shamir_types::types::record_id::RecordId>()
            {
                return RenamePersistOutcome::NotCommitted(DbError::Internal(format!(
                    "invalid source table store_id: {error}"
                )));
            }
        }
        if let Err(error) = destination_store_id
            .to_string()
            .parse::<shamir_types::types::record_id::RecordId>()
        {
            return RenamePersistOutcome::NotCommitted(DbError::Internal(format!(
                "invalid destination table store_id: {error}"
            )));
        }
        let table = match self.table(TABLE_TABLES).await {
            Ok(table) => table,
            Err(error) => return RenamePersistOutcome::NotCommitted(error),
        };
        let source_fields = match expected_source {
            QueryValue::Map(fields) => fields,
            _ => {
                return RenamePersistOutcome::NotCommitted(DbError::Internal(
                    "table catalogue row is not a map".into(),
                ))
            }
        };
        if source_fields.get("table_name").and_then(QueryValue::as_str) != Some(from)
            || source_fields.get("db_name").and_then(QueryValue::as_str) != Some(db_name)
            || source_fields.get("repo_name").and_then(QueryValue::as_str) != Some(repo_name)
        {
            return RenamePersistOutcome::NotCommitted(DbError::Validation(
                "rename source catalogue identity changed".into(),
            ));
        }
        let current = match self.load_table_record(db_name, repo_name, from).await {
            Ok(Some(current)) => current,
            Ok(None) => {
                return RenamePersistOutcome::NotCommitted(DbError::NotFound(format!(
                    "table catalogue record for '{db_name}/{repo_name}/{from}' not found"
                )))
            }
            Err(error) => return RenamePersistOutcome::NotCommitted(error),
        };
        if current.get("store_id").and_then(QueryValue::as_str)
            != source_store_id.map(ToString::to_string).as_deref()
        {
            return RenamePersistOutcome::NotCommitted(DbError::Validation(
                "rename source table incarnation changed".into(),
            ));
        }
        let QueryValue::Map(mut fields) = current else {
            return RenamePersistOutcome::NotCommitted(DbError::Internal(
                "table catalogue row is not a map".into(),
            ));
        };
        fields.insert("table_name".into(), QueryValue::Str(to.into()));
        fields.insert(
            "store_id".into(),
            QueryValue::Str(destination_store_id.to_string()),
        );
        fields.insert("rename_from".into(), QueryValue::Str(from.into()));
        if let Some(id) = source_store_id {
            fields.insert(
                "rename_from_store_id".into(),
                QueryValue::Str(id.to_string()),
            );
        } else {
            fields.shift_remove("rename_from_store_id");
        }
        let destination_op = crate::query::write::SetOp {
            set: crate::query::TableRef::new(TABLE_TABLES),
            key: table_catalogue_key(db_name, repo_name, to),
            value: QueryValue::Map(fields),
        };
        let source_op = crate::query::write::DeleteOp {
            delete_from: crate::query::TableRef::new(TABLE_TABLES),
            where_clause: crate::query::filter::Filter::And {
                filters: vec![
                    crate::query::filter::Filter::Eq {
                        field: vec!["db_name".into()],
                        value: crate::query::filter::FilterValue::String(db_name.into()),
                    },
                    crate::query::filter::Filter::Eq {
                        field: vec!["repo_name".into()],
                        value: crate::query::filter::FilterValue::String(repo_name.into()),
                    },
                    crate::query::filter::Filter::Eq {
                        field: vec!["table_name".into()],
                        value: crate::query::filter::FilterValue::String(from.into()),
                    },
                ],
            },
            select: None,
            expected_version: None,
        };
        if let Err(error) = self
            .persist_table_interner(&table, Some(&destination_op.value))
            .await
        {
            return RenamePersistOutcome::NotCommitted(error);
        }
        let repo = match self.system_repo() {
            Ok(repo) => repo,
            Err(error) => return RenamePersistOutcome::NotCommitted(error),
        };
        let owned_table = table.clone();
        let owned_destination_op = destination_op.clone();
        let owned_source_op = source_op.clone();
        let tx_result = repo
            .run_implicit_batch_tx(Actor::System, "rename_table", move |tx| {
                let table = owned_table.clone();
                let destination_op = owned_destination_op.clone();
                let source_op = owned_source_op.clone();
                Box::pin(async move {
                    table
                        .execute_set_tx(&destination_op, tx, None, &Actor::System)
                        .await?;
                    let refs = crate::types::common::new_map();
                    let ctx = table.info_store_read_view().filter_context(&refs).await?;
                    table
                        .execute_delete_tx(&source_op, &ctx, tx, None, &Actor::System)
                        .await?;
                    Ok(crate::query::write::WriteResult {
                        affected: 1,
                        records: Vec::new(),
                        execution_time_us: 0,
                    })
                })
            })
            .await;
        if tx_result.is_err() {
            return self
                .resolve_table_rename(key, source_store_id, destination_store_id)
                .await;
        }
        match self.flush_system_repo_durable(&table).await {
            Ok(()) => RenamePersistOutcome::Committed,
            Err(_) => {
                self.resolve_table_rename(key, source_store_id, destination_store_id)
                    .await
            }
        }
    }

    /// Remove one table's catalogue entry.
    pub async fn remove_table(
        &self,
        db_name: &str,
        repo_name: &str,
        table_name: &str,
    ) -> DbResult<()> {
        self.catalogue_write_fault_gate()?;
        let table = self.table(TABLE_TABLES).await?;
        let op = crate::query::write::DeleteOp {
            delete_from: crate::query::TableRef::new(TABLE_TABLES),
            where_clause: crate::query::filter::Filter::And {
                filters: vec![
                    crate::query::filter::Filter::Eq {
                        field: vec!["db_name".to_string()],
                        value: crate::query::filter::FilterValue::String(db_name.to_string()),
                    },
                    crate::query::filter::Filter::Eq {
                        field: vec!["repo_name".to_string()],
                        value: crate::query::filter::FilterValue::String(repo_name.to_string()),
                    },
                    crate::query::filter::Filter::Eq {
                        field: vec!["table_name".to_string()],
                        value: crate::query::filter::FilterValue::String(table_name.to_string()),
                    },
                ],
            },
            select: None,
            expected_version: None,
        };
        self.delete_via_implicit_tx(&table, &op).await?;
        self.flush_system_repo_durable(&table).await?;
        Ok(())
    }

    /// Load every persisted table-catalogue record (across all repos). The
    /// caller filters by `db_name` / `repo_name`.
    pub async fn load_tables(&self) -> DbResult<Vec<QueryValue>> {
        #[cfg(test)]
        self.count_table_catalogue_read();
        let table = self.table(TABLE_TABLES).await?;
        let refs = crate::types::common::new_map();
        let ctx = table.info_store_read_view().filter_context(&refs).await?;
        let query = crate::query::read::ReadQuery::new(TABLE_TABLES);
        let result = table.read(&query, &ctx).await?;
        if !result.corrupt_records.is_empty() {
            return Err(DbError::Internal(format!(
                "corrupt table catalogue: {} unreadable record(s)",
                result.corrupt_records.len()
            )));
        }
        Ok(result
            .records
            .into_iter()
            .map(|r| r.as_value().into_owned())
            .collect())
    }

    // ---------------------------------------------------------------------------
    // Settings

    // ---------------------------------------------------------------------------
    // Record lookups
    // ---------------------------------------------------------------------------

    /// Load a single database record by name.
    pub async fn load_database(&self, name: &str) -> DbResult<Option<QueryValue>> {
        let table = self.table(TABLE_DATABASES).await?;
        let refs = crate::types::common::new_map();
        let ctx = table.info_store_read_view().filter_context(&refs).await?;
        let query = crate::query::read::ReadQuery::new(TABLE_DATABASES).filter(
            crate::query::filter::Filter::Eq {
                field: vec!["name".to_string()],
                value: crate::query::filter::FilterValue::String(name.to_string()),
            },
        );
        let result = table.read(&query, &ctx).await?;
        if !result.corrupt_records.is_empty() {
            return Err(DbError::Internal(format!(
                "corrupt database catalogue: {} unreadable record(s)",
                result.corrupt_records.len()
            )));
        }
        Ok(result
            .records
            .into_iter()
            .next()
            .map(|r| r.as_value().into_owned()))
    }

    /// Load a single repository record by (db_name, repo_name).
    pub async fn load_repository(
        &self,
        db_name: &str,
        repo_name: &str,
    ) -> DbResult<Option<QueryValue>> {
        let table = self.table(TABLE_REPOSITORIES).await?;
        let refs = crate::types::common::new_map();
        let ctx = table.info_store_read_view().filter_context(&refs).await?;
        let query = crate::query::read::ReadQuery::new(TABLE_REPOSITORIES).filter(
            crate::query::filter::Filter::And {
                filters: vec![
                    crate::query::filter::Filter::Eq {
                        field: vec!["db_name".to_string()],
                        value: crate::query::filter::FilterValue::String(db_name.to_string()),
                    },
                    crate::query::filter::Filter::Eq {
                        field: vec!["repo_name".to_string()],
                        value: crate::query::filter::FilterValue::String(repo_name.to_string()),
                    },
                ],
            },
        );
        let result = table.read(&query, &ctx).await?;
        if !result.corrupt_records.is_empty() {
            return Err(DbError::Internal(format!(
                "corrupt repository catalogue: {} unreadable record(s)",
                result.corrupt_records.len()
            )));
        }
        Ok(result
            .records
            .into_iter()
            .next()
            .map(|r| self.resolve_repo_record(r.as_value().into_owned())))
    }

    /// Load a single table catalogue record by (db, repo, table).
    pub async fn load_table_record(
        &self,
        db_name: &str,
        repo_name: &str,
        table_name: &str,
    ) -> DbResult<Option<QueryValue>> {
        #[cfg(test)]
        self.count_table_catalogue_read();
        let table = self.table(TABLE_TABLES).await?;
        let refs = crate::types::common::new_map();
        let ctx = table.info_store_read_view().filter_context(&refs).await?;
        let query = crate::query::read::ReadQuery::new(TABLE_TABLES).filter(
            crate::query::filter::Filter::And {
                filters: vec![
                    crate::query::filter::Filter::Eq {
                        field: vec!["db_name".to_string()],
                        value: crate::query::filter::FilterValue::String(db_name.to_string()),
                    },
                    crate::query::filter::Filter::Eq {
                        field: vec!["repo_name".to_string()],
                        value: crate::query::filter::FilterValue::String(repo_name.to_string()),
                    },
                    crate::query::filter::Filter::Eq {
                        field: vec!["table_name".to_string()],
                        value: crate::query::filter::FilterValue::String(table_name.to_string()),
                    },
                ],
            },
        );
        let result = table.read(&query, &ctx).await?;
        if !result.corrupt_records.is_empty() {
            return Err(DbError::Internal(format!(
                "corrupt table catalogue: {} unreadable record(s)",
                result.corrupt_records.len()
            )));
        }
        Ok(result
            .records
            .into_iter()
            .next()
            .map(|r| r.as_value().into_owned()))
    }

    /// Persist a replacement database record (for `set_resource_meta`).
    pub async fn save_database_meta(&self, name: &str, record: &QueryValue) -> DbResult<()> {
        self.catalogue_write_fault_gate()?;
        let table = self.table(TABLE_DATABASES).await?;
        let op = crate::query::write::SetOp {
            set: crate::query::TableRef::new(TABLE_DATABASES),
            key: qv_map1("name", QueryValue::Str(name.to_string())),
            value: record.clone(),
        };
        self.set_via_implicit_tx(&table, &op).await?;
        self.persist_table_interner(&table, Some(record)).await?;
        self.flush_system_repo_durable(&table).await?;
        Ok(())
    }

    /// Persist a replacement repository record (for `set_resource_meta`).
    pub async fn save_repository_meta(&self, record: &QueryValue) -> DbResult<()> {
        let record = &self.catalogue_repo_record(record.clone());
        let db_name = record
            .get("db_name")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let repo_name = record
            .get("repo_name")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        self.catalogue_write_fault_gate()?;
        let table = self.table(TABLE_REPOSITORIES).await?;
        let op = crate::query::write::SetOp {
            set: crate::query::TableRef::new(TABLE_REPOSITORIES),
            key: qv_map2(
                "db_name",
                QueryValue::Str(db_name),
                "repo_name",
                QueryValue::Str(repo_name),
            ),
            value: record.clone(),
        };
        self.set_via_implicit_tx(&table, &op).await?;
        self.persist_table_interner(&table, None).await?;
        self.flush_system_repo_durable(&table).await?;
        Ok(())
    }

    /// Persist a replacement table catalogue record (for `set_resource_meta`).
    pub async fn save_table_meta(&self, record: &QueryValue) -> DbResult<()> {
        if let Some(value) = record.get("store_id") {
            let raw = value
                .as_str()
                .ok_or_else(|| DbError::Internal("invalid table store_id type".into()))?;
            raw.parse::<shamir_types::types::record_id::RecordId>()
                .map_err(|error| DbError::Internal(format!("invalid table store_id: {error}")))?;
        }
        self.catalogue_write_fault_gate()?;
        let db_name = record
            .get("db_name")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let repo_name = record
            .get("repo_name")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let table_name = record
            .get("table_name")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let table = self.table(TABLE_TABLES).await?;
        let mut key_m = new_map();
        key_m.insert("db_name".to_string(), QueryValue::Str(db_name));
        key_m.insert("repo_name".to_string(), QueryValue::Str(repo_name));
        key_m.insert("table_name".to_string(), QueryValue::Str(table_name));
        let op = crate::query::write::SetOp {
            set: crate::query::TableRef::new(TABLE_TABLES),
            key: QueryValue::Map(key_m),
            value: record.clone(),
        };
        self.set_via_implicit_tx(&table, &op).await?;
        self.persist_table_interner(&table, None).await?;
        self.flush_system_repo_durable(&table).await?;
        Ok(())
    }

    // Validator catalogue (S1)
}

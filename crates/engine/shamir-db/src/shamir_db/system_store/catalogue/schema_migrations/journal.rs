use futures::StreamExt;
use shamir_types::types::common::new_map;
use shamir_types::types::record_id::RecordId;
use shamir_types::types::value::QueryValue;

use super::event::{
    decode_event, event_row, JournalEvent, JournalEventInput, JournalEventKind,
    SchemaMigrationContext, JOURNAL_RECORD_VERSION,
};
use super::history::{validate_history, MigrationHistory};
use super::identity::DatabaseIdentity;
use super::step::SchemaMigrationStep;
use crate::access::Actor;
use crate::query::filter::{Filter, FilterValue};
use crate::query::read::ReadQuery;
use crate::query::write::{InsertOp, UpdateOp};
use crate::query::TableRef;
use crate::shamir_db::system_store::store::TABLE_SCHEMA_MIGRATION_EVENTS;
use crate::shamir_db::system_store::SystemStore;
use crate::{DbError, DbResult};
use shamir_query_types::admin::types::schema_migration::{
    canonical_body_checksum, SchemaMigrationBody, SchemaMigrationMetadata,
};

#[derive(Clone, Debug)]
struct ControlRecord {
    row_id: String,
    generation: u64,
    sequence: u64,
    owner: Option<String>,
    state: String,
    version: u64,
}

impl SystemStore {
    pub async fn schema_migration_identity(&self, database: &str) -> DbResult<DatabaseIdentity> {
        super::ensure_database_identity(self, database).await
    }

    pub async fn read_schema_migration_history(
        &self,
        identity: DatabaseIdentity,
    ) -> DbResult<MigrationHistory> {
        self.require_durable_schema_migration_store()?;
        let table = self.table(TABLE_SCHEMA_MIGRATION_EVENTS).await?;
        let records = read_journal_partition_records(&table).await?;
        let mut rows = Vec::with_capacity(records.len());
        let mut partition_control = None;
        for (row_id, value) in records {
            let record_version = value
                .get("record_version")
                .and_then(QueryValue::as_str)
                .ok_or_else(|| corrupt(&row_id, "missing or non-string record_version"))?
                .parse::<u16>()
                .map_err(|error| corrupt(&row_id, &format!("invalid record_version: {error}")))?;
            if record_version != JOURNAL_RECORD_VERSION {
                return Err(corrupt(
                    &row_id,
                    &format!("unknown mandatory record version {record_version}"),
                ));
            }
            let record_type = value
                .get("record_type")
                .and_then(QueryValue::as_str)
                .ok_or_else(|| corrupt(&row_id, "missing or non-string record_type"))?;
            let stored_identity = value
                .get("database_identity")
                .and_then(QueryValue::as_str)
                .ok_or_else(|| corrupt(&row_id, "missing or non-string database_identity"))?
                .parse::<DatabaseIdentity>()
                .map_err(|error| {
                    corrupt(&row_id, &format!("invalid database_identity: {error}"))
                })?;
            let selected_partition = stored_identity == identity;
            match record_type {
                "event" => {
                    let bytes = value
                        .get("event")
                        .and_then(|value| match value {
                            QueryValue::Bin(bytes) => Some(bytes.as_slice()),
                            _ => None,
                        })
                        .ok_or_else(|| corrupt(&row_id, "missing or non-binary event payload"))?;
                    let event = decode_event(bytes, &row_id)?;
                    if event.event_id
                        != value
                            .get("event_id")
                            .and_then(QueryValue::as_str)
                            .unwrap_or_default()
                        || event.database_identity != stored_identity
                        || event.sequence.to_string()
                            != value
                                .get("sequence")
                                .and_then(QueryValue::as_str)
                                .unwrap_or_default()
                        || format!("event/{}/{}", stored_identity, event.sequence)
                            != value
                                .get("record_key")
                                .and_then(QueryValue::as_str)
                                .unwrap_or_default()
                    {
                        return Err(corrupt(&row_id, "event payload differs from row envelope"));
                    }
                    if selected_partition {
                        rows.push((event, row_id));
                    }
                }
                "control" => {
                    let control = decode_control(&value, &row_id, stored_identity, 0)?;
                    if selected_partition
                        && partition_control
                            .replace((row_id.clone(), control))
                            .is_some()
                    {
                        return Err(corrupt(&row_id, "duplicate partition control record"));
                    }
                }
                _ => return Err(corrupt(&row_id, "unknown mandatory journal record type")),
            }
        }
        let history = validate_history(identity, rows)?;
        match partition_control {
            Some((row_id, control)) => validate_control_history(&control, &history, &row_id)?,
            None if history.last_sequence != 0 => {
                return Err(DbError::Internal(format!(
                    "schema migration control record missing for non-empty history {identity}"
                )));
            }
            None => {}
        }
        Ok(history)
    }

    pub async fn acquire_schema_migration_fence(
        &self,
        identity: DatabaseIdentity,
        expected_generation: u64,
        metadata: SchemaMigrationMetadata,
        body: SchemaMigrationBody,
        recovery: bool,
        context: &SchemaMigrationContext,
    ) -> DbResult<u64> {
        self.require_durable_schema_migration_store()?;
        let _admin = self.schema_migration_admin_lock().await;
        self.flush_journal_tail().await?;
        let history = self.read_schema_migration_history(identity).await?;
        let checksum = canonical_body_checksum(&body)
            .map_err(|error| DbError::Validation(format!("invalid migration body: {error}")))?;
        body.validate()
            .map_err(|error| DbError::Validation(error.to_string()))?;
        if body.metadata != metadata {
            return Err(DbError::Validation(
                "migration metadata differs from body metadata".into(),
            ));
        }
        let control = self.load_control(identity).await?;
        let actual_generation = control.as_ref().map_or(0, |control| control.generation);
        if actual_generation != expected_generation {
            return Err(fence_conflict(
                identity,
                expected_generation,
                actual_generation,
            ));
        }
        let fence_owned = control
            .as_ref()
            .is_some_and(|control| control.state == "owned");
        let already_applied = history
            .applied
            .iter()
            .any(|event| event.migration.id == metadata.id);
        // An owned fence is taken over only by explicit recovery of an unapplied migration.
        if fence_owned && (!recovery || already_applied) {
            return Err(DbError::Validation(format!(
                "schema migration fence for {identity} is already owned"
            )));
        }
        if let Some(applied) = history
            .applied
            .iter()
            .find(|event| event.migration.id == metadata.id)
        {
            if applied.migration.ordinal != metadata.ordinal || applied.checksum != checksum {
                return Err(DbError::Validation(format!(
                    "applied migration '{}' does not match stored ordinal/checksum",
                    metadata.id
                )));
            }
            return Err(DbError::Validation(format!(
                "migration '{}' is already applied",
                metadata.id
            )));
        }
        if history
            .events
            .iter()
            .any(|event| event.migration.id == metadata.id && event.checksum != checksum)
        {
            return Err(DbError::Validation(format!(
                "migration '{}' checksum differs from recorded history",
                metadata.id
            )));
        }
        let expected_ordinal = history
            .applied
            .last()
            .map_or(1, |event| event.migration.ordinal.saturating_add(1));
        if recovery {
            let pending = history.unresolved.last().ok_or_else(|| {
                DbError::Validation(
                    "explicit recovery requires unresolved migration history".into(),
                )
            })?;
            if pending.migration.id != metadata.id
                || pending.checksum != checksum
                || pending.migration.ordinal != metadata.ordinal
            {
                return Err(DbError::Validation(
                    "recovery body must match the unresolved migration id, ordinal, and checksum"
                        .into(),
                ));
            }
        } else if !history.unresolved.is_empty() {
            return Err(DbError::Validation(format!("schema migration history for {identity} is unresolved; explicit recovery is required")));
        } else if metadata.ordinal != expected_ordinal {
            return Err(DbError::Validation(format!(
                "schema migration ordinal gap: expected {expected_ordinal}, received {}",
                metadata.ordinal
            )));
        }
        if let Some(control) = &control {
            if control.sequence
                != history
                    .last_sequence
                    .checked_add(1)
                    .ok_or_else(|| DbError::Internal("schema migration sequence overflow".into()))?
            {
                return Err(DbError::Internal(format!(
                    "schema migration control/event sequence drift for {identity}"
                )));
            }
            validate_control_history(control, &history, &control.row_id)?;
        } else if history.last_sequence != 0 {
            return Err(DbError::Internal(format!(
                "schema migration control record missing for non-empty history {identity}"
            )));
        }
        let generation = expected_generation.checked_add(1).ok_or_else(|| {
            DbError::Internal("schema migration fence generation overflow".into())
        })?;
        let kind = if recovery {
            JournalEventKind::RecoveryAcquired
        } else {
            JournalEventKind::Started
        };
        let sequence = control.as_ref().map_or(1, |control| control.sequence);
        let event = JournalEvent::new(
            JournalEventInput {
                identity,
                sequence,
                migration: metadata.clone(),
                checksum,
                kind,
                body: Some(body),
                operation_index: None,
                evidence: None,
                fence_generation: generation,
            },
            context,
        );
        validate_candidate(&history, event.clone())?;
        let mut fields = control_fields(identity, control.as_ref());
        fields.insert("generation".into(), QueryValue::Str(generation.to_string()));
        fields.insert(
            "sequence".into(),
            QueryValue::Str(
                sequence
                    .checked_add(1)
                    .ok_or_else(|| {
                        DbError::Internal("schema migration event sequence overflow".into())
                    })?
                    .to_string(),
            ),
        );
        fields.insert("owner".into(), QueryValue::Str(metadata.id.clone()));
        fields.insert("state".into(), QueryValue::Str("owned".into()));
        self.commit_transition(control.as_ref(), QueryValue::Map(fields), &event)
            .await?;
        Ok(generation)
    }

    pub async fn append_schema_migration_event(
        &self,
        identity: DatabaseIdentity,
        expected_generation: u64,
        metadata: SchemaMigrationMetadata,
        checksum: [u8; 32],
        step: SchemaMigrationStep,
        context: &SchemaMigrationContext,
    ) -> DbResult<JournalEvent> {
        let SchemaMigrationStep {
            kind,
            operation_index,
            evidence,
        } = step;
        self.require_durable_schema_migration_store()?;
        let _admin = self.schema_migration_admin_lock().await;
        self.flush_journal_tail().await?;
        let history = self.read_schema_migration_history(identity).await?;
        let control = self.load_control(identity).await?.ok_or_else(|| {
            DbError::Validation(format!(
                "schema migration fence for {identity} is not acquired"
            ))
        })?;
        if control.generation != expected_generation
            || control.owner.as_deref() != Some(metadata.id.as_str())
            || control.state != "owned"
        {
            return Err(fence_conflict(
                identity,
                expected_generation,
                control.generation,
            ));
        }
        if control.sequence
            != history
                .last_sequence
                .checked_add(1)
                .ok_or_else(|| DbError::Internal("schema migration sequence overflow".into()))?
        {
            return Err(DbError::Internal(format!(
                "schema migration control/event sequence drift for {identity}"
            )));
        }
        validate_control_history(&control, &history, &control.row_id)?;
        let event = JournalEvent::new(
            JournalEventInput {
                identity,
                sequence: control.sequence,
                migration: metadata.clone(),
                checksum,
                kind,
                body: None,
                operation_index,
                evidence,
                fence_generation: expected_generation,
            },
            context,
        );
        validate_candidate(&history, event.clone())?;
        let mut fields = control_fields(identity, Some(&control));
        fields.insert(
            "sequence".into(),
            QueryValue::Str(
                control
                    .sequence
                    .checked_add(1)
                    .ok_or_else(|| {
                        DbError::Internal("schema migration event sequence overflow".into())
                    })?
                    .to_string(),
            ),
        );
        self.commit_transition(Some(&control), QueryValue::Map(fields), &event)
            .await?;
        Ok(event)
    }

    pub async fn release_schema_migration_fence(
        &self,
        identity: DatabaseIdentity,
        expected_generation: u64,
        metadata: SchemaMigrationMetadata,
        checksum: [u8; 32],
        context: &SchemaMigrationContext,
    ) -> DbResult<u64> {
        self.require_durable_schema_migration_store()?;
        let _admin = self.schema_migration_admin_lock().await;
        self.flush_journal_tail().await?;
        let history = self.read_schema_migration_history(identity).await?;
        let control = self.load_control(identity).await?.ok_or_else(|| {
            DbError::Validation(format!(
                "schema migration fence for {identity} is not acquired"
            ))
        })?;
        if control.generation != expected_generation
            || control.owner.as_deref() != Some(metadata.id.as_str())
            || control.state != "owned"
        {
            return Err(fence_conflict(
                identity,
                expected_generation,
                control.generation,
            ));
        }
        let applied_for_release = history.events.iter().any(|event| {
            event.migration == metadata
                && event.checksum == checksum
                && event.kind == JournalEventKind::Applied
                && event.fence_generation == expected_generation
        });
        if !applied_for_release {
            return Err(DbError::Validation(format!(
                "migration '{}' cannot release its fence before durable applied completion",
                metadata.id
            )));
        }
        if control.sequence
            != history
                .last_sequence
                .checked_add(1)
                .ok_or_else(|| DbError::Internal("schema migration sequence overflow".into()))?
        {
            return Err(DbError::Internal(format!(
                "schema migration control/event sequence drift for {identity}"
            )));
        }
        validate_control_history(&control, &history, &control.row_id)?;
        let generation = expected_generation.checked_add(1).ok_or_else(|| {
            DbError::Internal("schema migration fence generation overflow".into())
        })?;
        let event = JournalEvent::new(
            JournalEventInput {
                identity,
                sequence: control.sequence,
                migration: metadata.clone(),
                checksum,
                kind: JournalEventKind::FenceReleased,
                body: None,
                operation_index: None,
                evidence: None,
                fence_generation: generation,
            },
            context,
        );
        validate_candidate(&history, event.clone())?;
        let mut fields = control_fields(identity, Some(&control));
        fields.insert("generation".into(), QueryValue::Str(generation.to_string()));
        fields.insert(
            "sequence".into(),
            QueryValue::Str(
                control
                    .sequence
                    .checked_add(1)
                    .ok_or_else(|| {
                        DbError::Internal("schema migration event sequence overflow".into())
                    })?
                    .to_string(),
            ),
        );
        fields.insert("owner".into(), QueryValue::Null);
        fields.insert("state".into(), QueryValue::Str("released".into()));
        self.commit_transition(Some(&control), QueryValue::Map(fields), &event)
            .await?;
        Ok(generation)
    }

    async fn flush_journal_tail(&self) -> DbResult<()> {
        let table = self.table(TABLE_SCHEMA_MIGRATION_EVENTS).await?;
        self.persist_table_interner(&table, None).await?;
        self.flush_system_repo_durable(&table).await
    }

    async fn load_control(&self, identity: DatabaseIdentity) -> DbResult<Option<ControlRecord>> {
        let table = self.table(TABLE_SCHEMA_MIGRATION_EVENTS).await?;
        let records = read_journal_partition_records(&table).await?;
        let mut found = None;
        for (row_id, value) in records {
            if value.get("record_type").and_then(QueryValue::as_str) == Some("control")
                && value.get("record_key").and_then(QueryValue::as_str)
                    == Some(control_key(identity).as_str())
                && found.replace((row_id.clone(), value)).is_some()
            {
                return Err(corrupt(&row_id, "duplicate control record"));
            }
        }
        let Some((row_id, raw_value)) = found else {
            return Ok(None);
        };
        let refs = new_map();
        let context = table.info_store_read_view().filter_context(&refs).await?;
        let mut query = ReadQuery::new(TABLE_SCHEMA_MIGRATION_EVENTS).filter(Filter::Eq {
            field: vec!["record_key".into()],
            value: FilterValue::String(control_key(identity)),
        });
        query.with_version = true;
        let result = table.read(&query, &context).await?;
        if !result.corrupt_records.is_empty() {
            return Err(corrupt(
                &row_id,
                "control record query could not decode the row",
            ));
        }
        let Some(record) = result.records.first() else {
            return Err(corrupt(
                &row_id,
                "control record disappeared during version read",
            ));
        };
        if record.as_value().as_ref() != &raw_value {
            return Err(corrupt(
                &row_id,
                "control record changed during version read",
            ));
        }
        let versions = result
            .versions
            .as_ref()
            .ok_or_else(|| corrupt(&row_id, "control record read returned no MVCC version"))?;
        let version = match versions.as_slice() {
            [version] if result.records.len() == 1 => *version,
            _ => {
                return Err(corrupt(
                    &row_id,
                    "control record read returned invalid MVCC versions",
                ))
            }
        };
        let control = decode_control(&raw_value, &row_id, identity, version)?;
        Ok(Some(control))
    }

    async fn commit_transition(
        &self,
        prior: Option<&ControlRecord>,
        control_row: QueryValue,
        event: &JournalEvent,
    ) -> DbResult<()> {
        self.catalogue_write_fault_gate()?;
        let table = self.table(TABLE_SCHEMA_MIGRATION_EVENTS).await?;
        let event_row = event_row(event)?;
        let event_insert = InsertOp {
            insert_into: TableRef::new(TABLE_SCHEMA_MIGRATION_EVENTS),
            values: vec![event_row.clone()],
            records_idmsgpack: Vec::new(),
            select: None,
        };
        let initial_control_insert = prior.is_none().then(|| InsertOp {
            insert_into: TableRef::new(TABLE_SCHEMA_MIGRATION_EVENTS),
            values: vec![control_row.clone()],
            records_idmsgpack: Vec::new(),
            select: None,
        });
        let update = prior.map(|prior| UpdateOp {
            update: TableRef::new(TABLE_SCHEMA_MIGRATION_EVENTS),
            where_clause: Some(Filter::And {
                filters: vec![
                    Filter::Eq {
                        field: vec!["record_key".into()],
                        value: FilterValue::String(control_key(event.database_identity)),
                    },
                    Filter::Eq {
                        field: vec!["generation".into()],
                        value: FilterValue::String(prior.generation.to_string()),
                    },
                ],
            }),
            set: control_row,
            select: None,
            expected_version: Some(prior.version),
        });
        let owned_table = table.clone();
        let write_result = self.system_repo()?.run_implicit_batch_tx(Actor::System, "schema_migration_transition", move |tx| {
            let table = owned_table.clone();
            let update = update.clone();
            let initial_control_insert = initial_control_insert.clone();
            let event_insert = event_insert.clone();
            Box::pin(async move {
                if let Some(update) = update {
                    let refs = new_map();
                    let context = table.info_store_read_view().filter_context(&refs).await?;
                    let updated = table.execute_update_tx(&update, &context, tx, None, &Actor::System).await?;
                    if updated.affected != 1 {
                        return Err(crate::storage::error::DbError::Validation("schema migration control CAS did not affect exactly one row".into()));
                    }
                    let inserted = table.execute_insert_tx(&event_insert, tx, false, None, &Actor::System).await?;
                    if inserted.affected != 1 {
                        return Err(crate::storage::error::DbError::Validation("schema migration event insert did not affect exactly one row".into()));
                    }
                } else {
                    let control_insert = initial_control_insert.ok_or_else(|| crate::storage::error::DbError::Validation("initial schema migration control insert missing".into()))?;
                    let mut inserts = control_insert;
                    inserts.values.extend(event_insert.values);
                    let inserted = table.execute_insert_tx(&inserts, tx, false, None, &Actor::System).await?;
                    if inserted.affected != 2 {
                        return Err(crate::storage::error::DbError::Validation("initial schema migration control/event insert must affect exactly two rows".into()));
                    }
                }
                Ok(crate::query::write::WriteResult { affected: 1, records: Vec::new(), execution_time_us: 0 })
            })
        }).await.map_err(|error| DbError::Internal(error.to_string()))?;
        if write_result.affected != 1 {
            return Err(DbError::Internal(
                "schema migration transition transaction did not commit exactly once".into(),
            ));
        }
        self.persist_table_interner(&table, Some(&event_row))
            .await?;
        self.flush_system_repo_durable(&table).await
    }
}

fn validate_candidate(history: &MigrationHistory, event: JournalEvent) -> DbResult<()> {
    let mut rows = history
        .events
        .iter()
        .map(|existing| (existing.clone(), existing.event_id.clone()))
        .collect::<Vec<_>>();
    rows.push((event.clone(), event.event_id.clone()));
    validate_history(event.database_identity, rows).map(|_| ())
}

async fn read_journal_partition_records(
    table: &crate::engine::table::TableManager,
) -> DbResult<Vec<(String, QueryValue)>> {
    let interner = table.info_store_read_view().interner_read_view().await?;
    let mut records = Vec::new();
    let mut stream = Box::pin(table.list_stream(256));
    while let Some(batch) = stream.next().await {
        for (id, record) in batch? {
            let row_id = id.to_string();
            row_id.parse::<RecordId>().map_err(|error| {
                corrupt(&row_id, &format!("invalid physical record id: {error}"))
            })?;
            let value = match record {
                crate::engine::table::record_cow::RecordCow::Borrowed(bytes) => {
                    let value = interner
                        .record_bytes_to_query_value(&bytes)
                        .map_err(|error| {
                            corrupt(&row_id, &format!("record materialization failed: {error}"))
                        })?
                        .ok_or_else(|| {
                            corrupt(&row_id, "record bytes are not a supported value")
                        })?;
                    value
                }
                crate::engine::table::record_cow::RecordCow::Owned(value) => {
                    interner.inner_to_query_value(&value).map_err(|error| {
                        corrupt(&row_id, &format!("record materialization failed: {error}"))
                    })?
                }
            };
            records.push((row_id, value));
        }
    }
    Ok(records)
}

fn control_key(identity: DatabaseIdentity) -> String {
    format!("control/{identity}")
}

fn control_fields(
    identity: DatabaseIdentity,
    control: Option<&ControlRecord>,
) -> shamir_types::types::common::TMap<String, QueryValue> {
    let mut fields = new_map();
    fields.insert(
        "record_version".into(),
        QueryValue::Str(JOURNAL_RECORD_VERSION.to_string()),
    );
    fields.insert("record_type".into(), QueryValue::Str("control".into()));
    fields.insert("record_key".into(), QueryValue::Str(control_key(identity)));
    fields.insert(
        "database_identity".into(),
        QueryValue::Str(identity.to_string()),
    );
    fields.insert(
        "generation".into(),
        QueryValue::Str(control.map_or(0, |value| value.generation).to_string()),
    );
    fields.insert(
        "sequence".into(),
        QueryValue::Str(control.map_or(1, |value| value.sequence).to_string()),
    );
    fields.insert(
        "owner".into(),
        control
            .and_then(|value| value.owner.clone())
            .map(QueryValue::Str)
            .unwrap_or(QueryValue::Null),
    );
    fields.insert(
        "state".into(),
        QueryValue::Str(
            control
                .map_or("idle", |value| value.state.as_str())
                .to_owned(),
        ),
    );
    fields
}

fn decode_control(
    row: &QueryValue,
    row_id: &str,
    identity: DatabaseIdentity,
    version: u64,
) -> DbResult<ControlRecord> {
    let record_version = row
        .get("record_version")
        .and_then(QueryValue::as_str)
        .ok_or_else(|| corrupt(row_id, "missing or non-string record_version"))?
        .parse::<u16>()
        .map_err(|error| corrupt(row_id, &format!("invalid record_version: {error}")))?;
    if record_version != JOURNAL_RECORD_VERSION {
        return Err(corrupt(
            row_id,
            &format!("unknown mandatory record version {record_version}"),
        ));
    }
    let stored_identity = row
        .get("database_identity")
        .and_then(QueryValue::as_str)
        .ok_or_else(|| corrupt(row_id, "missing or non-string database_identity"))?
        .parse::<DatabaseIdentity>()
        .map_err(|error| corrupt(row_id, &format!("invalid database_identity: {error}")))?;
    let record_key = row.get("record_key").and_then(QueryValue::as_str);
    if row.get("record_type").and_then(QueryValue::as_str) != Some("control")
        || stored_identity != identity
        || record_key != Some(control_key(identity).as_str())
    {
        return Err(corrupt(row_id, "control record envelope mismatch"));
    }
    let generation = parse_counter(
        row.get("generation").and_then(QueryValue::as_str),
        row_id,
        "generation",
    )?;
    let sequence = parse_counter(
        row.get("sequence").and_then(QueryValue::as_str),
        row_id,
        "sequence",
    )?;
    if generation == 0 || sequence == 0 {
        return Err(corrupt(
            row_id,
            "control generation and sequence must be positive",
        ));
    }
    let owner = match row.get("owner") {
        Some(QueryValue::Str(owner)) if !owner.is_empty() => Some(owner.clone()),
        Some(QueryValue::Null) => None,
        _ => {
            return Err(corrupt(
                row_id,
                "control owner must be a nonempty string or null",
            ))
        }
    };
    let state = row
        .get("state")
        .and_then(QueryValue::as_str)
        .ok_or_else(|| corrupt(row_id, "missing or non-string state"))?
        .to_owned();
    match (state.as_str(), owner.as_ref()) {
        ("owned", Some(_)) | ("released", None) => {}
        _ => {
            return Err(corrupt(
                row_id,
                "control state/owner combination is invalid",
            ))
        }
    }
    Ok(ControlRecord {
        row_id: row_id.to_owned(),
        generation,
        sequence,
        owner,
        state,
        version,
    })
}

fn validate_control_history(
    control: &ControlRecord,
    history: &MigrationHistory,
    row_id: &str,
) -> DbResult<()> {
    let expected_sequence = history
        .last_sequence
        .checked_add(1)
        .ok_or_else(|| corrupt(row_id, "event sequence overflow"))?;
    if control.sequence != expected_sequence {
        return Err(corrupt(
            row_id,
            "control sequence does not follow journal tail",
        ));
    }
    let latest = history.events.last();
    if latest.is_some_and(|event| event.fence_generation != control.generation) {
        return Err(corrupt(
            row_id,
            "control generation differs from latest event",
        ));
    }
    match (
        control.state.as_str(),
        control.owner.as_deref(),
        history.unresolved.last(),
    ) {
        ("owned", Some(owner), Some(event)) if owner == event.migration.id => {}
        ("owned", Some(owner), None)
            if latest.is_some_and(|event| {
                event.kind == JournalEventKind::Applied && owner == event.migration.id
            }) => {}
        ("released", None, None)
            if latest.is_some_and(|event| event.kind == JournalEventKind::FenceReleased) => {}
        _ => {
            return Err(corrupt(
                row_id,
                "control state/owner does not match journal state",
            ))
        }
    }
    Ok(())
}

fn parse_counter(value: Option<&str>, row_id: &str, field: &str) -> DbResult<u64> {
    value
        .and_then(|value| value.parse().ok())
        .ok_or_else(|| corrupt(row_id, &format!("invalid {field}")))
}

fn corrupt(row_id: &str, cause: &str) -> DbError {
    DbError::Internal(format!(
        "corrupt schema migration journal record {row_id}: {cause}"
    ))
}

fn fence_conflict(identity: DatabaseIdentity, expected: u64, actual: u64) -> DbError {
    DbError::Validation(format!("schema migration fence CAS conflict for {identity}: expected generation {expected}, actual {actual}"))
}

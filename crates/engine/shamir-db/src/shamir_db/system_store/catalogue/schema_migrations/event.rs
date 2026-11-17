use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use shamir_types::access::Actor;
use shamir_types::types::record_id::RecordId;

use super::DatabaseIdentity;
use crate::{DbError, DbResult};
use shamir_query_types::admin::types::schema_migration::{
    canonical_body_checksum, SchemaMigrationBody, SchemaMigrationMetadata,
};

pub const JOURNAL_EVENT_VERSION: u16 = 1;
pub const JOURNAL_RECORD_VERSION: u16 = 1;

#[derive(Clone, Debug)]
pub(super) struct JournalEventInput {
    pub identity: DatabaseIdentity,
    pub sequence: u64,
    pub migration: SchemaMigrationMetadata,
    pub checksum: [u8; 32],
    pub kind: JournalEventKind,
    pub body: Option<SchemaMigrationBody>,
    pub operation_index: Option<u32>,
    pub evidence: Option<String>,
    pub fence_generation: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FormatEpoch {
    Unknown,
    Known(u64),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JournalEventKind {
    Started,
    OperationIntent,
    OperationComplete,
    Applied,
    InDoubt,
    Failed,
    RecoveryAcquired,
    FenceReleased,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JournalEvent {
    pub version: u16,
    pub event_id: String,
    pub database_identity: DatabaseIdentity,
    pub sequence: u64,
    pub migration: SchemaMigrationMetadata,
    pub checksum: [u8; 32],
    pub kind: JournalEventKind,
    pub body: Option<SchemaMigrationBody>,
    pub timestamp_unix_micros: u64,
    pub actor: String,
    pub server_version: String,
    pub format_epoch: FormatEpoch,
    pub operation_index: Option<u32>,
    pub operation_identity: Option<String>,
    pub evidence: Option<String>,
    pub fence_generation: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SchemaMigrationContext {
    actor: Actor,
    format_epoch: FormatEpoch,
}

impl SchemaMigrationContext {
    pub fn new(actor: Actor, format_epoch: FormatEpoch) -> Self {
        Self {
            actor,
            format_epoch,
        }
    }

    pub fn actor(&self) -> &Actor {
        &self.actor
    }
    pub fn epoch(&self) -> &FormatEpoch {
        &self.format_epoch
    }
}

impl Default for SchemaMigrationContext {
    fn default() -> Self {
        Self::new(Actor::System, FormatEpoch::Unknown)
    }
}

impl JournalEvent {
    pub(super) fn new(input: JournalEventInput, context: &SchemaMigrationContext) -> Self {
        let operation_identity = input
            .operation_index
            .map(|index| format!("{}:{}:{index}", input.migration.id, input.migration.ordinal));
        let timestamp_unix_micros = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros()
            .try_into()
            .unwrap_or(u64::MAX);
        Self {
            version: JOURNAL_EVENT_VERSION,
            event_id: RecordId::new().to_string(),
            database_identity: input.identity,
            sequence: input.sequence,
            migration: input.migration,
            checksum: input.checksum,
            kind: input.kind,
            body: input.body,
            timestamp_unix_micros,
            actor: context.actor.to_string(),
            server_version: env!("CARGO_PKG_VERSION").to_string(),
            format_epoch: context.format_epoch.clone(),
            operation_index: input.operation_index,
            operation_identity,
            evidence: input.evidence,
            fence_generation: input.fence_generation,
        }
    }

    pub(super) fn validate_envelope(&self, row_id: &str) -> DbResult<()> {
        if self.version != JOURNAL_EVENT_VERSION {
            return Err(DbError::Internal(format!(
                "schema migration journal row {row_id}: unknown mandatory event version {}",
                self.version
            )));
        }
        self.migration.validate().map_err(|error| {
            DbError::Internal(format!(
                "schema migration journal row {row_id}: invalid migration metadata: {error}"
            ))
        })?;
        if self.sequence == 0
            || self.fence_generation == 0
            || self.event_id.parse::<RecordId>().is_err()
            || self.migration.id.trim().is_empty()
            || self.migration.ordinal == 0
        {
            return Err(DbError::Internal(format!(
                "schema migration journal row {row_id}: invalid sequence, fence, or migration metadata"
            )));
        }
        let body_required = matches!(
            self.kind,
            JournalEventKind::Started | JournalEventKind::RecoveryAcquired
        );
        let operation_required = matches!(
            self.kind,
            JournalEventKind::OperationIntent | JournalEventKind::OperationComplete
        );
        let evidence_required = matches!(
            self.kind,
            JournalEventKind::OperationComplete
                | JournalEventKind::Applied
                | JournalEventKind::InDoubt
                | JournalEventKind::Failed
        );
        if body_required != self.body.is_some()
            || operation_required != self.operation_index.is_some()
            || evidence_required
                != self
                    .evidence
                    .as_deref()
                    .is_some_and(|value| !value.is_empty())
        {
            return Err(DbError::Internal(format!(
                "schema migration journal row {row_id}: body, operation, or evidence fields are invalid for event kind {:?}",
                self.kind
            )));
        }
        if !matches!(
            self.kind,
            JournalEventKind::OperationIntent | JournalEventKind::OperationComplete
        ) && (self.operation_index.is_some() || self.operation_identity.is_some())
        {
            return Err(DbError::Internal(format!(
                "schema migration journal row {row_id}: non-operation event contains operation fields"
            )));
        }
        if self.operation_identity.as_deref()
            != self
                .operation_index
                .map(|index| format!("{}:{}:{index}", self.migration.id, self.migration.ordinal))
                .as_deref()
        {
            return Err(DbError::Internal(format!(
                "schema migration journal row {row_id}: operation identity mismatch"
            )));
        }
        if let Some(index) = self.operation_index {
            if self
                .body
                .as_ref()
                .is_some_and(|body| index as usize >= body.operations.len())
            {
                return Err(DbError::Internal(format!(
                    "schema migration journal row {row_id}: operation index exceeds body length"
                )));
            }
        }
        if let Some(body) = &self.body {
            let checksum = canonical_body_checksum(body).map_err(|error| {
                DbError::Internal(format!(
                    "schema migration journal row {row_id}: invalid canonical body: {error}"
                ))
            })?;
            if body.metadata != self.migration || checksum != self.checksum {
                return Err(DbError::Internal(format!(
                    "schema migration journal row {row_id}: body metadata/checksum mismatch"
                )));
            }
        }
        if self.actor.trim().is_empty() || self.server_version.trim().is_empty() {
            return Err(DbError::Internal(format!(
                "schema migration journal row {row_id}: actor or server version is empty"
            )));
        }
        Ok(())
    }
}

pub(super) fn event_row(event: &JournalEvent) -> DbResult<crate::types::value::QueryValue> {
    let bytes = rmp_serde::to_vec_named(event)
        .map_err(|error| DbError::Internal(format!("encode schema migration event: {error}")))?;
    let mut row = crate::types::common::new_map();
    row.insert(
        "record_version".into(),
        crate::types::value::QueryValue::Str(JOURNAL_RECORD_VERSION.to_string()),
    );
    row.insert(
        "record_type".into(),
        crate::types::value::QueryValue::Str("event".into()),
    );
    row.insert(
        "record_key".into(),
        crate::types::value::QueryValue::Str(format!(
            "event/{}/{}",
            event.database_identity, event.sequence
        )),
    );
    row.insert(
        "database_identity".into(),
        crate::types::value::QueryValue::Str(event.database_identity.to_string()),
    );
    row.insert(
        "sequence".into(),
        crate::types::value::QueryValue::Str(event.sequence.to_string()),
    );
    row.insert(
        "event_id".into(),
        crate::types::value::QueryValue::Str(event.event_id.clone()),
    );
    row.insert("event".into(), crate::types::value::QueryValue::Bin(bytes));
    Ok(crate::types::value::QueryValue::Map(row))
}

pub(super) fn decode_event(bytes: &[u8], row_id: &str) -> DbResult<JournalEvent> {
    let event: JournalEvent = rmp_serde::from_slice(bytes).map_err(|error| {
        DbError::Internal(format!(
            "schema migration journal row {row_id}: event decode failed: {error}"
        ))
    })?;
    event.validate_envelope(row_id)?;
    Ok(event)
}

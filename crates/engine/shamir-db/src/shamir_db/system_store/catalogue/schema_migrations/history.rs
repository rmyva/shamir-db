use std::collections::{BTreeMap, BTreeSet};

use shamir_query_types::admin::types::schema_migration::SchemaMigrationMetadata;

use super::event::{JournalEvent, JournalEventKind};
use super::DatabaseIdentity;
use crate::{DbError, DbResult};

#[derive(Clone, Debug, PartialEq)]
pub struct MigrationHistory {
    pub events: Vec<JournalEvent>,
    pub applied: Vec<JournalEvent>,
    pub unresolved: Vec<JournalEvent>,
    pub last_sequence: u64,
    pub current_status: Option<JournalEventKind>,
    pub in_doubt: bool,
}

struct ActiveMigration {
    metadata: SchemaMigrationMetadata,
    checksum: [u8; 32],
    body: shamir_query_types::admin::types::schema_migration::SchemaMigrationBody,
    generation: u64,
    completed: usize,
    pending: Option<u32>,
    status: JournalEventKind,
    status_event: JournalEvent,
}

pub(super) fn validate_history(
    identity: DatabaseIdentity,
    mut rows: Vec<(JournalEvent, String)>,
) -> DbResult<MigrationHistory> {
    rows.sort_by_key(|(event, _)| event.sequence);
    let mut event_ids = BTreeSet::new();
    let mut migrations = BTreeMap::<String, (u64, [u8; 32], SchemaMigrationMetadata)>::new();
    let mut ordinals = BTreeMap::<u64, String>::new();
    let mut applied = Vec::new();
    let mut active: Option<ActiveMigration> = None;
    let mut last_sequence = 0_u64;
    let mut last_generation = 0_u64;

    for (event, row_id) in &rows {
        event.validate_envelope(row_id)?;
        if event.database_identity != identity {
            return Err(corrupt(row_id, "database identity partition mismatch"));
        }
        let expected_sequence = last_sequence
            .checked_add(1)
            .ok_or_else(|| corrupt(row_id, "event sequence overflow"))?;
        if event.sequence != expected_sequence {
            return Err(corrupt(row_id, "event sequence gap or duplicate"));
        }
        last_sequence = event.sequence;
        if !event_ids.insert(event.event_id.as_str()) {
            return Err(corrupt(row_id, "duplicate event id"));
        }
        let is_new_migration = !migrations.contains_key(event.migration.id.as_str());
        match migrations.get(event.migration.id.as_str()) {
            Some((ordinal, checksum, metadata))
                if *ordinal != event.migration.ordinal
                    || checksum != &event.checksum
                    || metadata != &event.migration =>
            {
                return Err(corrupt(row_id, "migration metadata/checksum drift"));
            }
            Some(_) => {}
            None => {
                if event.kind != JournalEventKind::Started {
                    return Err(corrupt(
                        row_id,
                        "migration does not begin with started event",
                    ));
                }
                if let std::collections::btree_map::Entry::Vacant(e) =
                    ordinals.entry(event.migration.ordinal)
                {
                    e.insert(event.migration.id.clone());
                } else {
                    return Err(corrupt(row_id, "duplicate migration ordinal"));
                }
                let expected_ordinal = applied.len() as u64 + 1;
                if event.migration.ordinal != expected_ordinal {
                    return Err(corrupt(row_id, "migration ordinal gap or reorder"));
                }
                migrations.insert(
                    event.migration.id.clone(),
                    (
                        event.migration.ordinal,
                        event.checksum,
                        event.migration.clone(),
                    ),
                );
            }
        }

        match event.kind {
            JournalEventKind::Started => {
                if !is_new_migration
                    || active.is_some()
                    || event.fence_generation <= last_generation
                {
                    return Err(corrupt(
                        row_id,
                        "started event overlaps migration or regresses fence",
                    ));
                }
                let body = event
                    .body
                    .clone()
                    .ok_or_else(|| corrupt(row_id, "started event has no body"))?;
                active = Some(ActiveMigration {
                    metadata: event.migration.clone(),
                    checksum: event.checksum,
                    body,
                    generation: event.fence_generation,
                    completed: 0,
                    pending: None,
                    status: event.kind,
                    status_event: event.clone(),
                });
                last_generation = event.fence_generation;
            }
            JournalEventKind::RecoveryAcquired => {
                let state = active
                    .as_mut()
                    .ok_or_else(|| corrupt(row_id, "recovery has no unresolved migration"))?;
                if !matches!(
                    state.status,
                    JournalEventKind::Started
                        | JournalEventKind::OperationIntent
                        | JournalEventKind::OperationComplete
                        | JournalEventKind::InDoubt
                        | JournalEventKind::Failed
                        | JournalEventKind::RecoveryAcquired
                ) || state.metadata != event.migration
                    || state.checksum != event.checksum
                    || event.fence_generation <= last_generation
                    || event.body.as_ref() != Some(&state.body)
                {
                    return Err(corrupt(
                        row_id,
                        "recovery must match unresolved migration and acquire a newer fence",
                    ));
                }
                state.generation = event.fence_generation;
                state.status = event.kind;
                state.status_event = event.clone();
                last_generation = event.fence_generation;
            }
            JournalEventKind::OperationIntent => {
                let state = active
                    .as_mut()
                    .ok_or_else(|| corrupt(row_id, "operation intent has no active migration"))?;
                let expected_index = u32::try_from(state.completed)
                    .map_err(|_| corrupt(row_id, "operation index overflow"))?;
                if !matches!(
                    state.status,
                    JournalEventKind::Started
                        | JournalEventKind::RecoveryAcquired
                        | JournalEventKind::OperationComplete
                ) || state.pending.is_some()
                    || state.metadata != event.migration
                    || state.checksum != event.checksum
                    || state.completed >= state.body.operations.len()
                    || event.operation_index != Some(expected_index)
                    || event.fence_generation != state.generation
                    || event.evidence.is_some()
                {
                    return Err(corrupt(
                        row_id,
                        "operation intent is not the next operation under the active fence",
                    ));
                }
                state.pending = Some(expected_index);
                state.status = event.kind;
                state.status_event = event.clone();
            }
            JournalEventKind::OperationComplete => {
                let state = active.as_mut().ok_or_else(|| {
                    corrupt(row_id, "operation completion has no active migration")
                })?;
                if !matches!(
                    state.status,
                    JournalEventKind::OperationIntent | JournalEventKind::RecoveryAcquired
                ) || state.pending.is_none()
                    || state.pending != event.operation_index
                    || state.metadata != event.migration
                    || state.checksum != event.checksum
                    || event.fence_generation != state.generation
                    || event.evidence.as_deref().is_none_or(str::is_empty)
                {
                    return Err(corrupt(
                        row_id,
                        "operation completion has no matching pending intent/evidence",
                    ));
                }
                state.pending = None;
                state.completed = state
                    .completed
                    .checked_add(1)
                    .ok_or_else(|| corrupt(row_id, "operation count overflow"))?;
                state.status = event.kind;
                state.status_event = event.clone();
            }
            JournalEventKind::Applied => {
                let state = active
                    .as_mut()
                    .ok_or_else(|| corrupt(row_id, "applied event has no active migration"))?;
                if !matches!(
                    state.status,
                    JournalEventKind::Started
                        | JournalEventKind::RecoveryAcquired
                        | JournalEventKind::OperationComplete
                ) || state.pending.is_some()
                    || state.metadata != event.migration
                    || state.checksum != event.checksum
                    || state.completed != state.body.operations.len()
                    || event.fence_generation != state.generation
                    || event.evidence.as_deref().is_none_or(str::is_empty)
                {
                    return Err(corrupt(
                        row_id,
                        "applied event precedes complete operations/final evidence",
                    ));
                }
                applied.push(event.clone());
                state.status = event.kind;
                state.status_event = event.clone();
            }
            JournalEventKind::InDoubt | JournalEventKind::Failed => {
                let state = active
                    .as_mut()
                    .ok_or_else(|| corrupt(row_id, "failure event has no active migration"))?;
                if !matches!(
                    state.status,
                    JournalEventKind::Started
                        | JournalEventKind::RecoveryAcquired
                        | JournalEventKind::OperationIntent
                        | JournalEventKind::OperationComplete
                ) || state.metadata != event.migration
                    || state.checksum != event.checksum
                    || event.fence_generation != state.generation
                    || event.evidence.as_deref().is_none_or(str::is_empty)
                {
                    return Err(corrupt(
                        row_id,
                        "failure event follows terminal state or has stale fence",
                    ));
                }
                state.status = event.kind;
                state.status_event = event.clone();
            }
            JournalEventKind::FenceReleased => {
                let state = active
                    .take()
                    .ok_or_else(|| corrupt(row_id, "fence release has no active migration"))?;
                if state.status != JournalEventKind::Applied
                    || state.metadata != event.migration
                    || state.checksum != event.checksum
                    || state.pending.is_some()
                    || event.fence_generation <= state.generation
                {
                    return Err(corrupt(
                        row_id,
                        "fence release requires applied migration and newer generation",
                    ));
                }
                last_generation = event.fence_generation;
            }
        }
    }

    let current_status = active
        .as_ref()
        .map(|state| state.status)
        .or_else(|| applied.last().map(|_| JournalEventKind::Applied));
    let in_doubt = active.as_ref().is_some_and(|state| {
        matches!(
            state.status,
            JournalEventKind::Started
                | JournalEventKind::OperationIntent
                | JournalEventKind::OperationComplete
                | JournalEventKind::InDoubt
                | JournalEventKind::Failed
                | JournalEventKind::RecoveryAcquired
        )
    });
    let unresolved = if in_doubt {
        active
            .as_ref()
            .map(|state| vec![state.status_event.clone()])
            .unwrap_or_default()
    } else {
        Vec::new()
    };
    Ok(MigrationHistory {
        events: rows.into_iter().map(|(event, _)| event).collect(),
        applied,
        unresolved,
        last_sequence,
        current_status,
        in_doubt,
    })
}

fn corrupt(row_id: &str, cause: &str) -> DbError {
    DbError::Internal(format!(
        "corrupt schema migration journal record {row_id}: {cause}"
    ))
}

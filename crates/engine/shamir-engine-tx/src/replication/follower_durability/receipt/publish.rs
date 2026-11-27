//! Step 5: publication (S5c-X PR6b-2). Everything here runs POST-durability
//! (the Materialized batch already committed) and — except receipt decoding
//! on the resume path — is infallible in effect: it raises MVCC visibility,
//! ts indexes, counters, and the durability/visibility trackers.

use std::sync::Arc;

use shamir_storage::error::{DbError, DbResult};
use shamir_storage::types::KvOp;
use shamir_tx::{ChangelogEvent, MvccStore, RepoTxGate};

use super::stage;
use crate::commit::core::RepoCore;
use crate::meta::repl::ReplicationReceipt;
use crate::table::table_manager::table_token_for;
use crate::table::RecordCounter;

pub(crate) struct EventPublication {
    pub(crate) local_version: u64,
    pub(crate) ts_ms: u64,
    pub(crate) downstream: Arc<ChangelogEvent>,
    /// Publish per (table mvcc, ops) group — the legacy path's
    /// `write_committed_to_history` granularity (history rows + ts row +
    /// ts_index per table).
    pub(crate) groups: Vec<(Arc<MvccStore>, Vec<KvOp>)>,
}

pub(crate) struct Publication {
    pub(crate) events: Vec<EventPublication>,
    pub(crate) counters: Vec<(Arc<RecordCounter>, u64)>,
    pub(crate) floor: u64,
}

async fn mvcc_for(repo: &RepoCore, table: &str) -> DbResult<Arc<MvccStore>> {
    let token = table_token_for(table);
    repo.per_table_mvcc()
        .read_sync(&token, |_, mvcc| Arc::clone(mvcc))
        .ok_or_else(|| DbError::Storage(format!("no mvcc store for table {table}")))
}

/// Build the publication from a fresh plan (no decoding — the plan already
/// carries the downstream `Arc`s).
pub(crate) async fn publication_from_plan(
    repo: &RepoCore,
    permit: &crate::repo::changelog_store::write_domain::RepoWritePermit,
    plan: &stage::BatchPlan,
    counters: &[(String, u64)],
) -> DbResult<Publication> {
    let mut events = Vec::with_capacity(plan.per_event.len());
    for (index, event) in plan.per_event.iter().enumerate() {
        let downstream = plan.events.get(index).cloned().ok_or_else(|| {
            DbError::RepoRequiresReopen("batch plan lost a downstream event".into())
        })?;
        let ts_ms = plan.receipt.timestamps.get(index).map_or(0, |ts| ts.ts_ms);
        let mut groups = Vec::with_capacity(event.tables.len());
        for table in &event.tables {
            groups.push((mvcc_for(repo, &table.table).await?, table.ops.clone()));
        }
        events.push(EventPublication {
            local_version: event.local_version,
            ts_ms,
            downstream,
            groups,
        });
    }
    let mut resolved = Vec::with_capacity(counters.len());
    for (table, after) in counters {
        resolved.push((
            Arc::clone(repo.get_table_admitted(table, permit).await?.counter()),
            *after,
        ));
    }
    let floor = plan
        .per_event
        .iter()
        .map(|event| event.local_version)
        .max()
        .unwrap_or(0);
    Ok(Publication {
        events,
        counters: resolved,
        floor,
    })
}

/// Build the publication from a durable receipt (resume): decode each
/// downstream byte vec (decode failure = corrupt → reopen) and rebuild the
/// ops via the receipt's own segmentation.
pub(crate) async fn publication_from_receipt(
    repo: &RepoCore,
    permit: &crate::repo::changelog_store::write_domain::RepoWritePermit,
    receipt: &ReplicationReceipt,
) -> DbResult<Publication> {
    let groups_per_event = stage::materialize_ops_from_receipt(receipt)?;
    let mut events = Vec::with_capacity(groups_per_event.len());
    for (index, event_ops) in groups_per_event.iter().enumerate() {
        let bytes = receipt.downstream_events.get(index).ok_or_else(|| {
            DbError::RepoRequiresReopen(
                "receipt downstream events are shorter than its mapping".into(),
            )
        })?;
        let decoded: ChangelogEvent = rmp_serde::from_slice(bytes).map_err(|error| {
            DbError::RepoRequiresReopen(format!("receipt downstream event is corrupt: {error}"))
        })?;
        let ts_ms = receipt.timestamps.get(index).map_or(0, |ts| ts.ts_ms);
        let mut groups = Vec::with_capacity(event_ops.tables.len());
        for (table, ops) in &event_ops.tables {
            groups.push((mvcc_for(repo, table).await?, ops.clone()));
        }
        events.push(EventPublication {
            local_version: event_ops.local_version,
            ts_ms,
            downstream: Arc::new(decoded),
            groups,
        });
    }
    let mut counters = Vec::with_capacity(receipt.counters.len());
    for delta in &receipt.counters {
        counters.push((
            Arc::clone(
                repo.get_table_admitted(&delta.table, permit)
                    .await?
                    .counter(),
            ),
            delta.after,
        ));
    }
    let floor = receipt
        .mapping
        .iter()
        .map(|mapping| mapping.local_version)
        .max()
        .unwrap_or(0);
    Ok(Publication {
        events,
        counters,
        floor,
    })
}

/// Publish a durable batch (SYNCHRONOUS): raise visibility per (table,
/// version), then advance the durability tracker and settle the
/// recoverability prefix. The `resume_version_guard` re-owns the exact
/// versions the receipt recorded (allocation-free); its `commit()` marks
/// visibility Materialized and advances the committed watermark.
///
/// PROOF (cancellation + error ownership, S5c-X PR6b-2 round 3): between the
/// checkpoint batch's
/// atomic commit and the end of this function there is NO `.await`: an
/// executor can only cancel a future at an await point, so once the commit's
/// poll resolves, durability and RAM publication complete atomically with
/// respect to caller cancellation.
///
/// GUARD OWNERSHIP (review finding 2): Aborted is TERMINAL — the completion
/// tracker skips marks at versions ≤ the watermark, and a `VersionGuard`'s
/// `Drop` marks Aborted AND advances the watermarks — so the fresh path's
/// chunk guards must NEVER drop on an error while the repo keeps serving
/// (that would advance `last_committed`/`durable_watermark` past still-
/// unpublished versions). On failure, fresh guards stay parked; quarantine
/// fail-stops the gate before fail-closed Drop, preserving both floors until
/// reopen. Successful retirement commits guards only when both visibility and
/// durability floors cover their version. Resume re-owns receipt versions and
/// commits each after publication.
///
/// The wal group is resolved by the async caller and passed in — resolving
/// it here would need a `.await`, which is exactly what this function must
/// not contain.
pub(crate) fn apply_publication(
    gate: &Arc<RepoTxGate>,
    group: Option<&Arc<shamir_wal::WalGroupCommit>>,
    publication: &Publication,
) {
    for event in &publication.events {
        let mut guard = gate.resume_version_guard(event.local_version);
        // Publication may panic after some rows are installed. Preserve the
        // unsettled version and fail-stop the repo instead of aborting it.
        guard.visibility_started_without_wal();
        for (mvcc, ops) in &event.groups {
            mvcc.publish_materialized(ops, event.local_version);
            mvcc.ts_index_insert(event.ts_ms, event.local_version);
        }
        guard.commit();
    }
    for event in &publication.events {
        gate.mark_durable(event.local_version);
        if let Some(group) = group {
            group.settle(event.local_version);
        }
    }
}

/// Adopt the absolute counters of a published batch — the single remaining
/// await of the old inline publication, extracted so `apply_publication` can
/// be synchronous. `adopt_absolute` only touches RAM under a lock — it
/// cannot realistically fail here (post-commit); errors are propagated
/// anyway.
pub(crate) async fn adopt_counters(_repo: &RepoCore, publication: &Publication) -> DbResult<()> {
    for (counter, after) in &publication.counters {
        counter.adopt_absolute(*after).await?;
    }
    Ok(())
}

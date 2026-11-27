//! Receipt staging (S5c-X PR6b-2): batch-plan construction, counter
//! derivation, and the two durable atomic writes of the pipeline —
//! `commit_prepared` (receipt + bookmark pointer + floor markers) and
//! `commit_materialized_work` (history rows + journal events + counters +
//! Materialized receipt). Each is ONE repository-wide atomic batch, with a
//! read-back reconcile for `Uncertain` commit outcomes.
//!
//! Admission (PR6b-2c): every staging function takes the repository's
//! `RepoWritePermit` and hands it to `repo.begin_repo_atomic_batch(permit)`
//! (which debug-checks the permit's identity). The permit is acquired ONCE
//! by `run_receipt_pipeline` (with the marker mutex taken second, per
//! #183's lock order) and threads through unchanged — there is no second
//! admission mechanism anywhere in the pipeline.

#[cfg(any(test, feature = "test-util"))]
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
#[cfg(any(test, feature = "test-util"))]
use std::sync::Mutex;

use bytes::Bytes;
use shamir_collections::TFxMap;
use shamir_storage::error::{DbError, DbResult};
use shamir_storage::types::{AtomicBatch, AtomicCommitOutcome, KvOp, RecordKey, Store};
use shamir_tx::{encode_version_key, version_key, ChangelogEvent, MvccStore};

use super::super::super::{reproject_for_downstream, PreparedEvent};
use crate::commit::core::RepoCore;
use crate::repo::changelog_store::write_domain::RepoWritePermit;
use crate::repo::meta::recovery_marker::{load_last_committed, load_next_tx_id_snapshot};
use crate::repo::meta::repl::bookmark::load_fence_record;
use crate::repo::meta::repl::bookmark_v3::{encode_bookmark_v3, load_bookmark_v3, BookmarkV3};
use crate::repo::meta::repl::receipt::{decode_receipt, load_outstanding_receipt, receipt_key};
use crate::repo::meta::repl::{
    encode_receipt, receipt_id, ReceiptBinding, ReceiptCounterDelta, ReceiptOp, ReceiptPhase,
    ReceiptTimestamp, ReceiptVersionMapping, ReplicationReceipt, MAX_RECEIPT_EVENTS,
    MAX_RECEIPT_OPS,
};
use crate::repo::meta::{MetaEnvelope, MetaKey};

const TX_STORE: &str = "__tx__";
const CHANGELOG_STORE: &str = "__changelog__";

#[cfg(any(test, feature = "test-util"))]
type MaterializationFailureQueues = TFxMap<String, Vec<MaterializationBatchFailure>>;

#[cfg(any(test, feature = "test-util"))]
static MATERIALIZATION_BATCH_FAILURES: Mutex<Option<MaterializationFailureQueues>> =
    Mutex::new(None);

#[cfg(any(test, feature = "test-util"))]
#[derive(Clone, Copy)]
#[doc(hidden)]
pub enum MaterializationBatchFailure {
    BeforeCommit,
    AfterCommitUncertain,
}

#[cfg(any(test, feature = "test-util"))]
#[doc(hidden)]
pub fn inject_materialization_batch_failures(
    repo_name: &str,
    failures: &[MaterializationBatchFailure],
) {
    let mut all = MATERIALIZATION_BATCH_FAILURES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    all.get_or_insert_with(TFxMap::default)
        .insert(repo_name.to_owned(), failures.to_vec());
}

#[cfg(any(test, feature = "test-util"))]
#[doc(hidden)]
pub fn clear_materialization_batch_failures(repo_name: &str) {
    if let Some(queues) = MATERIALIZATION_BATCH_FAILURES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .as_mut()
    {
        queues.remove(repo_name);
    }
    MATERIALIZATION_READBACK_UNAVAILABLE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .as_mut()
        .and_then(|map| map.remove(repo_name));
}

#[cfg(any(test, feature = "test-util"))]
fn wrap_materialization_batch(
    batch: Box<dyn AtomicBatch>,
    repo_name: &str,
) -> Box<dyn AtomicBatch> {
    Box::new(InjectedMaterializationBatch {
        inner: batch,
        repo_name: repo_name.to_owned(),
    })
}

#[cfg(any(test, feature = "test-util"))]
struct InjectedMaterializationBatch {
    inner: Box<dyn AtomicBatch>,
    repo_name: String,
}

#[cfg(any(test, feature = "test-util"))]
#[async_trait::async_trait]
impl AtomicBatch for InjectedMaterializationBatch {
    fn set(&mut self, store: &str, key: RecordKey, value: Bytes) -> DbResult<()> {
        self.inner.set(store, key, value)
    }
    fn delete(&mut self, store: &str, key: RecordKey) -> DbResult<()> {
        self.inner.delete(store, key)
    }
    async fn commit(self: Box<Self>) -> DbResult<AtomicCommitOutcome> {
        let failure = {
            let mut all = MATERIALIZATION_BATCH_FAILURES
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            all.as_mut()
                .and_then(|map| map.get_mut(&self.repo_name))
                .and_then(|q| {
                    if q.is_empty() {
                        None
                    } else {
                        Some(q.remove(0))
                    }
                })
        };
        match failure {
            Some(MaterializationBatchFailure::BeforeCommit) => {
                Err(DbError::Storage("injected pre-commit failure".into()))
            }
            Some(MaterializationBatchFailure::AfterCommitUncertain) => {
                self.inner.commit().await?;
                Ok(AtomicCommitOutcome::Uncertain {
                    detail: "injected post-commit uncertainty".into(),
                })
            }
            None => self.inner.commit().await,
        }
    }
}

#[cfg(any(test, feature = "test-util"))]
static MATERIALIZATION_READBACK_UNAVAILABLE: Mutex<Option<TFxMap<String, bool>>> = Mutex::new(None);

#[cfg(any(test, feature = "test-util"))]
#[doc(hidden)]
pub fn set_materialization_readback_unavailable(repo_name: &str, on: bool) {
    MATERIALIZATION_READBACK_UNAVAILABLE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get_or_insert_with(TFxMap::default)
        .insert(repo_name.to_owned(), on);
}

#[cfg(any(test, feature = "test-util"))]
fn take_materialization_readback_failure(repo_name: &str) -> bool {
    MATERIALIZATION_READBACK_UNAVAILABLE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .as_mut()
        .and_then(|map| map.remove(repo_name))
        .unwrap_or(false)
}

#[cfg(any(test, feature = "test-util"))]
static MATERIALIZATION_COMMITS: Mutex<Option<TFxMap<String, usize>>> = Mutex::new(None);

#[cfg(any(test, feature = "test-util"))]
#[doc(hidden)]
pub fn materialization_commit_count(repo_name: &str) -> usize {
    MATERIALIZATION_COMMITS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .as_ref()
        .and_then(|counts| counts.get(repo_name).copied())
        .unwrap_or(0)
}

#[cfg(any(test, feature = "test-util"))]
fn record_materialization_commit(repo_name: &str) {
    let mut counts = MATERIALIZATION_COMMITS.lock().unwrap();
    let counts = counts.get_or_insert_with(TFxMap::default);
    *counts.entry(repo_name.to_owned()).or_default() += 1;
}

/// Test-only seam: after a `Committed` outcome, pretend it was `Uncertain`
/// so the read-back reconcile path runs in tests.
#[cfg(any(test, feature = "test-util"))]
static SIMULATE_UNCERTAIN: AtomicBool = AtomicBool::new(false);

#[cfg(any(test, feature = "test-util"))]
#[doc(hidden)]
pub fn set_simulate_uncertain(on: bool) {
    SIMULATE_UNCERTAIN.store(on, Ordering::Release);
}

/// The reprojected downstream events of one receipt batch, in receipt order.
pub(crate) struct BatchPlan {
    pub(crate) receipt: ReplicationReceipt,
    pub(crate) events: Vec<Arc<ChangelogEvent>>,
    /// Parallel to `events`.
    pub(crate) per_event: Vec<EventPlan>,
}

pub(crate) struct EventPlan {
    pub(crate) local_version: u64,
    /// Ordered like the receipt ops for this event.
    pub(crate) tables: Vec<TableOps>,
}

pub(crate) struct TableOps {
    pub(crate) table: String,
    /// Translated (local interner ids); Set carries the value bytes, Remove
    /// is a tombstone.
    pub(crate) ops: Vec<KvOp>,
}

/// Per-event operation groups rebuilt from a receipt (resume / cold paths).
pub(crate) struct EventOps {
    pub(crate) local_version: u64,
    pub(crate) tables: Vec<(String, Vec<KvOp>)>,
}

/// Everything the Materialized atomic batch needs, source-agnostic (built
/// from a fresh `BatchPlan` or rebuilt from a durable receipt).
pub(crate) struct MaterializedWork {
    pub(crate) events: Vec<EventOps>,
    /// Exact downstream journal bytes, parallel to `events`.
    pub(crate) downstream: Vec<Bytes>,
    pub(crate) timestamps: Vec<ReceiptTimestamp>,
    pub(crate) counters: Vec<ReceiptCounterDelta>,
    /// Encoded receipt with `phase = Materialized` and the counters set.
    pub(crate) receipt_bytes: Bytes,
    pub(crate) receipt_id: [u8; 16],
}

// Same key bookmark_v3.rs uses privately (`key()`); replicated locally to
// keep the codec's private helper private.
pub(crate) fn bookmark_record_key() -> RecordKey {
    RecordKey::from_slice(MetaKey::ReplicationBookmark.as_record_id().as_bytes())
}

fn marker_bytes(value: u64) -> DbResult<Bytes> {
    MetaEnvelope::new(value)
        .encode()
        .map(Bytes::from)
        .map_err(|error| DbError::Storage(format!("recovery marker codec: {error}")))
}

fn marker_key(key: MetaKey) -> RecordKey {
    RecordKey::from_slice(key.as_record_id().as_bytes())
}

/// Build the receipt for one chunk of prepared events. Serializes each
/// reprojected downstream event HERE so the journal bytes and the receipt's
/// `downstream_events` are literally identical byte sequences. Counters are
/// filled later (materialize time, when the before-values are known) via
/// [`set_counters`].
///
/// NOTE the combined structural bound: the caller CHUNKS before this point;
/// this check is the fail-loud backstop (`encode_receipt` would reject it
/// anyway).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn build_plan(
    repo: &RepoCore,
    prepared: &[PreparedEvent],
    bindings: Vec<ReceiptBinding>,
    predecessor_bookmark: u64,
    leader_start: u64,
    leader_end: u64,
) -> DbResult<BatchPlan> {
    let info = repo.tx_info_store().await?;
    let fence = load_fence_record(&info).await?;
    let (lineage_id, upstream_epoch) = fence
        .map(|fence| (fence.lineage_id, fence.history_epoch))
        .unwrap_or(([0u8; 16], 0));

    let mut mapping = Vec::with_capacity(prepared.len());
    let mut ops = Vec::new();
    let mut ops_per_event = Vec::with_capacity(prepared.len());
    let mut timestamps = Vec::with_capacity(prepared.len());
    let mut downstream_events = Vec::with_capacity(prepared.len());
    let mut events = Vec::with_capacity(prepared.len());
    let mut per_event = Vec::with_capacity(prepared.len());

    for event in prepared {
        let local_version = event.version.ok_or_else(|| {
            DbError::Storage("replicated event lost its allocated local version".into())
        })?;
        mapping.push(ReceiptVersionMapping {
            upstream_version: event.event.commit_version,
            local_version,
        });
        timestamps.push(ReceiptTimestamp {
            local_version,
            ts_ms: event.event.timestamp_ns / 1_000_000,
        });
        let downstream = Arc::new(reproject_for_downstream(
            &event.event,
            repo.name(),
            local_version,
        ));
        let serialized = rmp_serde::to_vec(downstream.as_ref())
            .map_err(|error| DbError::Storage(format!("receipt downstream encode: {error}")))?;
        downstream_events.push(serialized);
        events.push(downstream);

        let mut event_ops = 0usize;
        let mut tables = Vec::with_capacity(event.tables.len());
        for table in &event.tables {
            for op in &table.ops {
                ops.push(match op {
                    KvOp::Set(key, value) => ReceiptOp {
                        table: table.name.clone(),
                        key: key.as_ref().to_vec(),
                        value: Some(value.to_vec()),
                    },
                    KvOp::Remove(key) => ReceiptOp {
                        table: table.name.clone(),
                        key: key.as_ref().to_vec(),
                        value: None,
                    },
                });
                event_ops += 1;
            }
            tables.push(TableOps {
                table: table.name.clone(),
                ops: table.ops.clone(),
            });
        }
        ops_per_event.push(event_ops as u32);
        per_event.push(EventPlan {
            local_version,
            tables,
        });
    }

    let receipt = ReplicationReceipt {
        lineage_id,
        upstream_epoch,
        predecessor_bookmark,
        upstream_start: leader_start,
        upstream_end: leader_end,
        mapping,
        ops,
        ops_per_event,
        bindings,
        downstream_events,
        timestamps,
        counters: Vec::new(),
        phase: ReceiptPhase::Prepared,
    };
    let combined = receipt.mapping.len()
        + receipt.ops.len()
        + receipt.downstream_events.len()
        + receipt.timestamps.len()
        + receipt.counters.len();
    if combined > MAX_RECEIPT_EVENTS || receipt.ops.len() > MAX_RECEIPT_OPS {
        return Err(DbError::Storage(
            "replication receipt exceeds structural size bounds".into(),
        ));
    }
    Ok(BatchPlan {
        receipt,
        events,
        per_event,
    })
}

/// Attach the computed counter deltas to the plan.
///
/// CRITICAL id-stability note: `receipt_id` is a pure function of content, so
/// adding counters CHANGES the id. The bookmark v3 `outstanding_receipt`
/// pointer written at the Prepared phase therefore references the
/// counter-less id, and the Materialized rewrite (identical content plus
/// counters and the new phase) carries a DIFFERENT id — the Materialized
/// atomic batch moves the bookmark pointer to the new id IN THE SAME BATCH,
/// so the pointer and the stored receipt are always mutually consistent.
pub(crate) fn set_counters(plan: &mut BatchPlan, counters: Vec<ReceiptCounterDelta>) {
    plan.receipt.counters = counters;
}

/// The Prepared atomic batch: receipt + bookmark v3 (with the outstanding
/// pointer) + floor markers (`LastCommittedVersion`, `NextTxId`) — one
/// repository-wide batch.
async fn stage_prepared_batch(
    repo: &RepoCore,
    permit: &RepoWritePermit,
    plan: &BatchPlan,
    old: &BookmarkV3,
) -> DbResult<Box<dyn AtomicBatch>> {
    let id = receipt_id(&plan.receipt);
    let mut batch = repo.begin_repo_atomic_batch(permit).await?;
    batch.set(
        TX_STORE,
        receipt_key(),
        Bytes::from(encode_receipt(&plan.receipt)?),
    )?;
    let bookmark = BookmarkV3 {
        fence: old.fence,
        bookmark: old.bookmark,
        clean_through: old.clean_through,
        outstanding_receipt: Some(id),
    };
    batch.set(
        TX_STORE,
        bookmark_record_key(),
        Bytes::from(encode_bookmark_v3(&bookmark)?),
    )?;
    let gate = repo.tx_gate().await?;
    let floor = load_last_committed_marker(repo)
        .await?
        .max(gate.version_allocation_high_water_mark());
    let next_tx_id = load_next_tx_id_marker(repo)
        .await?
        .max(gate.peek_next_tx_id());
    batch.set(
        TX_STORE,
        marker_key(MetaKey::LastCommittedVersion),
        marker_bytes(floor)?,
    )?;
    batch.set(
        TX_STORE,
        marker_key(MetaKey::NextTxId),
        marker_bytes(next_tx_id)?,
    )?;
    Ok(batch)
}

async fn load_last_committed_marker(repo: &RepoCore) -> DbResult<u64> {
    let info = repo.tx_info_store().await?;
    Ok(load_last_committed(&info).await?.unwrap_or(0))
}

async fn load_next_tx_id_marker(repo: &RepoCore) -> DbResult<u64> {
    let info = repo.tx_info_store().await?;
    Ok(load_next_tx_id_snapshot(&info).await?.unwrap_or(1))
}

/// Step 2: durably record the Prepared receipt (plus the bookmark pointer and
/// the version/tx-id floor markers) in ONE atomic batch, then flush the info
/// store. The explicit flush is belt-and-braces: the atomic batch commit
/// already ran the backend's durable persist; this covers in-memory-ish
/// wrapper stores and is cheap.
pub(crate) async fn commit_prepared(
    repo: &RepoCore,
    permit: &RepoWritePermit,
    info: &Arc<dyn Store>,
    plan: &BatchPlan,
    old: &BookmarkV3,
) -> DbResult<()> {
    let id = receipt_id(&plan.receipt);
    let outcome = stage_prepared_batch(repo, permit, plan, old)
        .await?
        .commit()
        .await?;
    if let AtomicCommitOutcome::Uncertain { detail } = outcome {
        // Reconcile by reading back — never blindly re-apply.
        let bookmark = load_bookmark_v3(info).await?;
        if bookmark.is_some_and(|bookmark| bookmark.outstanding_receipt == Some(id)) {
            log::warn!(
                "receipt Prepared commit was uncertain but the read-back shows \
                 the pointer landed (repo {}): {detail}",
                repo.name()
            );
        } else {
            // Retry the whole staged batch ONCE.
            match stage_prepared_batch(repo, permit, plan, old)
                .await?
                .commit()
                .await?
            {
                AtomicCommitOutcome::Committed => {}
                AtomicCommitOutcome::Uncertain { detail } => {
                    return Err(DbError::RepoRequiresReopen(format!(
                        "receipt Prepared commit uncertain twice: {detail}"
                    )));
                }
            }
        }
    }
    info.flush().await?;
    #[cfg(any(test, feature = "test-util"))]
    super::super::test_seam::phase(repo.name(), super::super::test_seam::Phase::Prepared).await;
    log::info!(
        "follower receipt: Prepared upstream {}..={} on repo {}",
        plan.receipt.upstream_start,
        plan.receipt.upstream_end,
        repo.name()
    );
    Ok(())
}

/// Segment `receipt.ops` into per-event table groups using `ops_per_event`
/// and `mapping[i].local_version`. Used by the resume/cold paths.
pub(crate) fn materialize_ops_from_receipt(
    receipt: &ReplicationReceipt,
) -> DbResult<Vec<EventOps>> {
    if receipt.ops_per_event.len() != receipt.mapping.len()
        || receipt
            .ops_per_event
            .iter()
            .map(|&count| count as usize)
            .sum::<usize>()
            != receipt.ops.len()
    {
        return Err(DbError::RepoRequiresReopen(
            "receipt op segmentation is inconsistent with its mapping".into(),
        ));
    }
    let mut result = Vec::with_capacity(receipt.mapping.len());
    let mut cursor = 0usize;
    for (index, count) in receipt.ops_per_event.iter().enumerate() {
        let count = *count as usize;
        if cursor + count > receipt.ops.len() {
            return Err(DbError::RepoRequiresReopen(
                "receipt op segmentation overruns its op list".into(),
            ));
        }
        let mut tables: Vec<(String, Vec<KvOp>)> = Vec::new();
        for op in &receipt.ops[cursor..cursor + count] {
            match tables.last_mut() {
                Some((table, ops)) if *table == op.table => ops.push(receipt_op_to_kvop(op)),
                _ => tables.push((op.table.clone(), vec![receipt_op_to_kvop(op)])),
            }
        }
        result.push(EventOps {
            local_version: receipt.mapping[index].local_version,
            tables,
        });
        cursor += count;
    }
    Ok(result)
}

fn receipt_op_to_kvop(op: &ReceiptOp) -> KvOp {
    match &op.value {
        Some(value) => KvOp::Set(RecordKey::from_slice(&op.key), Bytes::from(value.clone())),
        None => KvOp::Remove(RecordKey::from_slice(&op.key)),
    }
}

/// The exact `(version, bytes)` journal records a receipt owns.
pub(crate) fn journal_records(receipt: &ReplicationReceipt) -> DbResult<Vec<(u64, Bytes)>> {
    if receipt.downstream_events.len() != receipt.mapping.len() {
        return Err(DbError::RepoRequiresReopen(
            "receipt downstream events are shorter than its mapping".into(),
        ));
    }
    Ok(receipt
        .mapping
        .iter()
        .zip(&receipt.downstream_events)
        .map(|(mapping, bytes)| (mapping.local_version, Bytes::from(bytes.clone())))
        .collect())
}

/// Compute per-table absolute before/after counters for `events` against the
/// LIVE pre-state (nothing is published yet on either the fresh or the
/// Prepared-resume path). Per event and table, one batched
/// `get_current_many` resolves keys not already covered by the batch's own
/// pending map; the rest is pure RAM.
async fn compute_counters(
    repo: &RepoCore,
    permit: &RepoWritePermit,
    events: &[EventOps],
) -> DbResult<Vec<ReceiptCounterDelta>> {
    // First-seen table order.
    let mut order: Vec<String> = Vec::new();
    let mut seen = TFxMap::<String, ()>::default();
    for event in events {
        for (table, _) in &event.tables {
            if seen.insert(table.clone(), ()).is_none() {
                order.push(table.clone());
            }
        }
    }

    let mut counters = Vec::with_capacity(order.len());
    for table in order {
        let counter = Arc::clone(repo.get_table_admitted(&table, permit).await?.counter());
        let before = counter.get().await?;
        let token = crate::table::table_manager::table_token_for(&table);
        let mvcc: Arc<MvccStore> = repo
            .per_table_mvcc()
            .read_sync(&token, |_, mvcc| Arc::clone(mvcc))
            .ok_or_else(|| DbError::Storage(format!("no mvcc store for table {table}")))?;
        // exists-after map: the batch's own pending view of each key.
        let mut pending: TFxMap<RecordKey, bool> = TFxMap::default();
        let mut delta = 0i64;
        for event in events {
            let Some((_, ops)) = event.tables.iter().find(|group| group.0 == table) else {
                continue;
            };
            // One batched pre-state read per group for keys not yet pending.
            let mut missing: Vec<RecordKey> = Vec::new();
            let mut missing_seen = TFxMap::<RecordKey, ()>::default();
            for op in ops {
                let key = match op {
                    KvOp::Set(key, _) | KvOp::Remove(key) => key.clone(),
                };
                if !pending.contains_key(&key) && missing_seen.insert(key.clone(), ()).is_none() {
                    missing.push(key);
                }
            }
            if !missing.is_empty() {
                let keys: Vec<Bytes> = missing
                    .iter()
                    .map(|key| Bytes::copy_from_slice(key.as_ref()))
                    .collect();
                let images = mvcc.get_current_many(&keys).await?;
                for (key, image) in missing.into_iter().zip(images) {
                    pending.insert(key, image.is_some());
                }
            }
            for op in ops {
                let (key, is_set) = match op {
                    KvOp::Set(key, _) => (key.clone(), true),
                    KvOp::Remove(key) => (key.clone(), false),
                };
                let exists_before = pending.get(&key).copied().unwrap_or(false);
                delta += match (is_set, exists_before) {
                    (true, false) => 1,
                    (false, true) => -1,
                    _ => 0,
                };
                pending.insert(key, is_set);
            }
        }
        let after = if delta >= 0 {
            before.checked_add(delta as u64)
        } else {
            before.checked_sub(delta.unsigned_abs())
        }
        .ok_or_else(|| {
            DbError::Internal(format!(
                "record count overflow for table {table}: before={before}, delta={delta}"
            ))
        })?;
        counters.push(ReceiptCounterDelta {
            table,
            before,
            after,
        });
    }
    Ok(counters)
}

/// Materialize work from a fresh plan: compute the counters against the live
/// pre-state, attach them, and encode the Materialized receipt.
pub(crate) async fn materialize_from_plan(
    repo: &RepoCore,
    permit: &RepoWritePermit,
    plan: &mut BatchPlan,
) -> DbResult<MaterializedWork> {
    let events: Vec<EventOps> = plan
        .per_event
        .iter()
        .map(|event| EventOps {
            local_version: event.local_version,
            tables: event
                .tables
                .iter()
                .map(|table| (table.table.clone(), table.ops.clone()))
                .collect(),
        })
        .collect();
    let counters = compute_counters(repo, permit, &events).await?;
    set_counters(plan, counters.clone());
    let mut receipt = plan.receipt.clone();
    receipt.phase = ReceiptPhase::Materialized;
    let id = receipt_id(&receipt);
    let receipt_bytes = Bytes::from(encode_receipt(&receipt)?);
    Ok(MaterializedWork {
        downstream: plan
            .receipt
            .downstream_events
            .iter()
            .map(|bytes| Bytes::from(bytes.clone()))
            .collect(),
        events,
        timestamps: plan.receipt.timestamps.clone(),
        counters,
        receipt_bytes,
        receipt_id: id,
    })
}

/// Materialize work from a durable PREPARED receipt (resume): compute the
/// counters the same way the fresh path would have (nothing was published).
pub(crate) async fn materialize_from_prepared_receipt(
    repo: &RepoCore,
    permit: &RepoWritePermit,
    receipt: &ReplicationReceipt,
) -> DbResult<MaterializedWork> {
    let events = materialize_ops_from_receipt(receipt)?;
    let counters = compute_counters(repo, permit, &events).await?;
    let mut rewritten = receipt.clone();
    rewritten.counters = counters.clone();
    rewritten.phase = ReceiptPhase::Materialized;
    let id = receipt_id(&rewritten);
    let receipt_bytes = Bytes::from(encode_receipt(&rewritten)?);
    Ok(MaterializedWork {
        downstream: receipt
            .downstream_events
            .iter()
            .map(|bytes| Bytes::from(bytes.clone()))
            .collect(),
        events,
        timestamps: receipt.timestamps.clone(),
        counters,
        receipt_bytes,
        receipt_id: id,
    })
}

/// The Materialized atomic batch: per-table history rows + ts rows, per-table
/// absolute counter rows, the exact downstream journal bytes, the rewritten
/// Materialized receipt, and the bookmark pointer moved to the Materialized
/// id (see `set_counters`' id-stability note) — one repository-wide batch.
async fn stage_materialized_batch(
    repo: &RepoCore,
    permit: &RepoWritePermit,
    work: &MaterializedWork,
    old: &BookmarkV3,
) -> DbResult<Box<dyn AtomicBatch>> {
    let batch = repo.begin_repo_atomic_batch(permit).await?;
    #[cfg(any(test, feature = "test-util"))]
    let batch = wrap_materialization_batch(batch, repo.name());
    let mut batch = batch;
    for (table, history, info_store, count_after) in table_rows(repo, work)? {
        for (index, event) in work.events.iter().enumerate() {
            let Some((_, ops)) = event.tables.iter().find(|group| group.0 == table) else {
                continue;
            };
            for op in ops {
                let (key, value) = match op {
                    KvOp::Set(key, value) => (key.as_ref(), value.clone()),
                    KvOp::Remove(key) => (key.as_ref(), Bytes::new()),
                };
                batch.set(
                    &history,
                    RecordKey::from_slice(&encode_version_key(key, event.local_version)),
                    value,
                )?;
            }
            // mirrors MvccStore TS_TAG (0x00) + version BE — a 9-byte ts key
            // can never collide with a version key (VERSION_SEP is 0xFF).
            let mut ts_key = Vec::with_capacity(9);
            ts_key.push(0x00);
            ts_key.extend_from_slice(&event.local_version.to_be_bytes());
            let ts_ms = work.timestamps.get(index).map_or(0, |ts| ts.ts_ms);
            batch.set(
                &history,
                RecordKey::from_slice(&ts_key),
                Bytes::from(ts_ms.to_le_bytes().to_vec()),
            )?;
        }
        let bytes = shamir_types::codecs::basic::bincode::to_bytes(&count_after)
            .map_err(|error| DbError::Codec(format!("failed to serialize count: {error}")))?;
        batch.set(&info_store, marker_key(MetaKey::Count), bytes)?;
    }
    for (index, event) in work.events.iter().enumerate() {
        let bytes = work
            .downstream
            .get(index)
            .ok_or_else(|| {
                DbError::RepoRequiresReopen("materialized work lost a journal record".into())
            })?
            .clone();
        batch.set(
            CHANGELOG_STORE,
            RecordKey::from_slice(&version_key(event.local_version)),
            bytes,
        )?;
    }
    batch.set(TX_STORE, receipt_key(), work.receipt_bytes.clone())?;
    let bookmark = BookmarkV3 {
        fence: old.fence,
        bookmark: old.bookmark,
        clean_through: old.clean_through,
        outstanding_receipt: Some(work.receipt_id),
    };
    batch.set(
        TX_STORE,
        bookmark_record_key(),
        Bytes::from(encode_bookmark_v3(&bookmark)?),
    )?;
    Ok(batch)
}

/// First-seen table order with the derived store names and the absolute
/// after-count for each.
fn table_rows(
    repo: &RepoCore,
    work: &MaterializedWork,
) -> DbResult<Vec<(String, String, String, u64)>> {
    let mut order: Vec<String> = Vec::new();
    let mut seen = TFxMap::<String, ()>::default();
    for event in &work.events {
        for (table, _) in &event.tables {
            if seen.insert(table.clone(), ()).is_none() {
                order.push(table.clone());
            }
        }
    }
    let mut rows = Vec::with_capacity(order.len());
    for table in order {
        let Some(delta) = work.counters.iter().find(|delta| delta.table == table) else {
            return Err(DbError::RepoRequiresReopen(format!(
                "materialized work has no counter for table {table}"
            )));
        };
        let after = delta.after;
        let Some((_, info, history)) = repo.mapped_table_store_names(&table) else {
            return Err(DbError::RepoRequiresReopen(format!(
                "materialized work references unknown table {table}"
            )));
        };
        rows.push((table, history, info, after));
    }
    Ok(rows)
}

/// Step 3: durably record the Materialized state in ONE atomic batch, with
/// the same Uncertain reconcile as `commit_prepared` (read back the receipt
/// PHASE). Returns the decoded Materialized receipt plus its per-table
/// after-counts. NO info flush here — the checkpoint step syncs next; the
/// counters only need to be visible to `adopt_absolute` in RAM.
pub(crate) async fn commit_materialized_work(
    repo: &RepoCore,
    permit: &RepoWritePermit,
    info: &Arc<dyn Store>,
    work: &MaterializedWork,
    old: &BookmarkV3,
) -> DbResult<(ReplicationReceipt, Vec<(String, u64)>)> {
    let outcome = stage_materialized_batch(repo, permit, work, old)
        .await?
        .commit()
        .await?;
    #[cfg(any(test, feature = "test-util"))]
    if outcome == AtomicCommitOutcome::Committed {
        record_materialization_commit(repo.name());
    }
    #[cfg(any(test, feature = "test-util"))]
    let outcome = if outcome == AtomicCommitOutcome::Committed
        && SIMULATE_UNCERTAIN.load(Ordering::Acquire)
    {
        AtomicCommitOutcome::Uncertain {
            detail: "injected".into(),
        }
    } else {
        outcome
    };
    if let AtomicCommitOutcome::Uncertain { detail } = outcome {
        #[cfg(any(test, feature = "test-util"))]
        let force_unresolved_readback = take_materialization_readback_failure(repo.name());
        #[cfg(any(test, feature = "test-util"))]
        let settled = if force_unresolved_readback {
            false
        } else {
            matches!(
                load_outstanding_receipt(info, &work.receipt_id).await,
                Ok(Some(receipt)) if receipt.phase == ReceiptPhase::Materialized,
            )
        };
        #[cfg(not(any(test, feature = "test-util")))]
        let settled = matches!(
            load_outstanding_receipt(info, &work.receipt_id).await,
            Ok(Some(receipt)) if receipt.phase == ReceiptPhase::Materialized,
        );
        if !settled {
            let retry = stage_materialized_batch(repo, permit, work, old).await;
            let retry_outcome = match retry {
                Ok(batch) => batch.commit().await,
                Err(error) => Err(error),
            };
            match retry_outcome {
                Ok(AtomicCommitOutcome::Committed) => {
                    #[cfg(any(test, feature = "test-util"))]
                    record_materialization_commit(repo.name());
                }
                Ok(AtomicCommitOutcome::Uncertain { detail }) => {
                    repo.tx_gate().await?.set_requires_reopen();
                    return Err(DbError::RepoRequiresReopen(format!(
                        "receipt Materialized commit uncertain twice: {detail}"
                    )));
                }
                Err(error) => {
                    repo.tx_gate().await?.set_requires_reopen();
                    return Err(DbError::RepoRequiresReopen(format!(
                        "receipt Materialized retry failed after unresolved uncertainty: {error}"
                    )));
                }
            }
        } else {
            log::warn!(
                "receipt Materialized commit was uncertain but the read-back \
                 shows the phase advanced (repo {}): {detail}",
                repo.name()
            );
        }
    }
    let receipt = decode_receipt(&work.receipt_id, &work.receipt_bytes)?;
    let counters = work
        .counters
        .iter()
        .map(|delta| (delta.table.clone(), delta.after))
        .collect();
    log::info!(
        "follower receipt: Materialized upstream {}..={} on repo {}",
        receipt.upstream_start,
        receipt.upstream_end,
        repo.name()
    );
    #[cfg(any(test, feature = "test-util"))]
    super::super::test_seam::phase(repo.name(), super::super::test_seam::Phase::Materialized).await;
    Ok((receipt, counters))
}

//! Follower apply preparation and outcome types for receipt-based
//! replication.
//!
//! The follower apply entry point is [`apply_replicated_batch`], which routes
//! unconditionally into the receipt pipeline
//! (`follower_durability::receipt`): durable follower state is admitted
//! with typed refusals and fresh repositories self-adopt a v3 baseline
//! before any apply work. This module retains the shared
//! preparation/validation/interner-translation/downstream-projection
//! semantics the receipt path calls — `prepare_event` and
//! `reproject_for_downstream` — plus [`ApplyOutcome`], [`ApplyRejectReason`]
//! and [`ApplyPolicy`].
//!
//! ## Trust precondition (#1199)
//!
//! Preparation performs **no validator/schema/DAC/integrity check** on
//! `event.changes` beyond optional payload validation and the mandatory
//! decoding needed to translate field ids by name. The entire
//! security of this path rests on the replication TRANSPORT already
//! having authenticated the sender as a legitimate leader/upstream before
//! this code is ever called — a precondition owned by sibling crates
//! (`shamir-connect`, `shamir-transport-tcp`/`-ws`, `shamir-server`'s
//! replication supervisor), not by this crate. See
//! `docs/dev-artifacts/roadmap/REPLICATION.md` §4.4 for the full threat
//! model and what is/isn't implemented against it.
//!
//! ## Versions — local, not leader's
//!
//! The follower allocates LOCAL commit versions via its own
//! `RepoTxGate` (the receipt pipeline owns the guards). The leader's
//! `commit_version` is carried in the event only for IDEMPOTENCY: events at
//! or below the applied watermark are skipped without touching the store,
//! and the durable bookmark itself is owned by receipt checkpointing.
#[path = "follower_durability/mod.rs"]
pub mod follower_durability;

use shamir_storage::error::{DbError, DbResult};
use shamir_storage::types::KvOp;
use shamir_tx::{collect_referenced_ids, remap_inner_value_bytes, ChangeOp, ChangelogEvent};
use shamir_types::record_view::RecordView;
use shamir_types::types::value::InnerValue;

use self::follower_durability::receipt::apply_batch_receipt;
use crate::commit::core::RepoCore;
use crate::table::table_manager::table_token_for;

/// The local outcome of applying one replicated [`ChangelogEvent`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ApplyOutcome {
    /// The event was applied to the local repo. Carries the follower-local
    /// commit version allocated for it (NOT the leader's `commit_version`).
    Applied { local_version: u64 },
    /// The event was skipped because its leader `commit_version` is at or
    /// below the caller-supplied `applied_watermark` (already applied in a
    /// prior call — idempotent re-delivery).
    Skipped,
    /// The event is incompatible with this follower. No version is spent,
    /// no data is written, and no downstream event is emitted.
    Rejected { reason: ApplyRejectReason },
}

/// Permanent incompatibilities detected before allocating a local version.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ApplyRejectReason {
    UnknownTable,
    NoMvccStore,
    IndexedTable { table: String, index: String },
    MissingValue,
    UndecodablePayload,
    MissingFieldBinding,
    ConflictingFieldBinding,
}

/// Follower-side trust policy for [`apply_replicated_batch`] (#1199). See
/// `docs/dev-artifacts/roadmap/REPLICATION.md` §4.4 for the full threat
/// model this addresses — and does NOT address.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ApplyPolicy {
    /// Trust the leader's decisions, skipping optional payload validation.
    /// Decoding required for field-id translation is never skipped.
    #[default]
    Trusted,
    /// Decode-validate every `Put`'s record bytes BEFORE allocating a
    /// version or touching the store: the same two-step decode
    /// `table/read_exec.rs` uses to classify corrupt records
    /// ([`RecordView::new`], falling back to [`InnerValue::from_bytes`] for
    /// legacy-encoded rows). Rejects the WHOLE event (`Rejected`, no version
    /// consumed) if any `Put` payload decodes as neither.
    ///
    /// This does NOT re-run schema/validator/DAC checks against the
    /// decoded record — see REPLICATION.md §4.4 for why that is explicitly
    /// out of scope here (no actor-authorization signal survives the wire,
    /// and schema/validator registries live in `shamir-db`, a crate this
    /// one cannot depend on). It only closes the "structurally garbage
    /// bytes get forwarded verbatim" gap — a decode failure here would
    /// otherwise surface later as a `corrupt_records` entry on some
    /// unrelated future read, or propagate silently to a downstream
    /// chain-replica via `reproject_for_downstream`.
    ValidatePayload,
}

struct PreparedTable {
    name: String,
    ops: Vec<KvOp>,
}

pub(crate) struct PreparedEvent {
    event: ChangelogEvent,
    tables: Vec<PreparedTable>,
    version: Option<u64>,
}

async fn prepare_event(
    repo: &RepoCore,
    event: &ChangelogEvent,
    field_bindings: &[(u64, String)],
    applied_watermark: u64,
    policy: ApplyPolicy,
    permit: Option<&crate::repo::RepoWritePermit>,
) -> DbResult<Result<PreparedEvent, ApplyOutcome>> {
    // V4 §4 idempotency: O(1) watermark check, no per-record scan. A
    // re-delivered event (at-most-once transport with redelivery) is a
    // silent no-op. Strictly-greater-than so an exact re-delivery of the
    // last applied version is also a skip.
    if event.commit_version <= applied_watermark {
        return Ok(Err(ApplyOutcome::Skipped));
    }

    if event
        .changes
        .iter()
        .any(|change| !repo.has_table(&change.table))
    {
        return Ok(Err(ApplyOutcome::Rejected {
            reason: ApplyRejectReason::UnknownTable,
        }));
    }

    let mut bindings = shamir_collections::TFxMap::default();
    let mut names = shamir_collections::TFxMap::default();
    for (id, name) in field_bindings {
        if bindings.insert(*id, name).is_some_and(|old| old != name)
            || names.insert(name, *id).is_some_and(|old| old != *id)
        {
            return Ok(Err(ApplyOutcome::Rejected {
                reason: ApplyRejectReason::ConflictingFieldBinding,
            }));
        }
    }
    let mut referenced = shamir_collections::TFxMap::default();
    let mut by_table: shamir_collections::TFxMap<String, Vec<KvOp>> =
        shamir_collections::TFxMap::default();
    for change in &event.changes {
        let op = match change.op {
            ChangeOp::Put => {
                let Some(value) = change.value.as_ref() else {
                    return Ok(Err(ApplyOutcome::Rejected {
                        reason: ApplyRejectReason::MissingValue,
                    }));
                };
                if policy == ApplyPolicy::ValidatePayload
                    && RecordView::new(value).is_err()
                    && InnerValue::from_bytes(value).is_err()
                {
                    return Ok(Err(ApplyOutcome::Rejected {
                        reason: ApplyRejectReason::UndecodablePayload,
                    }));
                }
                let decoded = match InnerValue::from_bytes(value) {
                    Ok(decoded) => decoded,
                    Err(_) => {
                        return Ok(Err(ApplyOutcome::Rejected {
                            reason: ApplyRejectReason::UndecodablePayload,
                        }));
                    }
                };
                collect_referenced_ids(&decoded, &mut referenced);
                KvOp::Set(change.key.clone().into(), value.clone())
            }
            ChangeOp::Delete => KvOp::Remove(change.key.clone().into()),
        };
        by_table.entry(change.table.clone()).or_default().push(op);
    }

    if referenced.keys().any(|id| !bindings.contains_key(id)) {
        return Ok(Err(ApplyOutcome::Rejected {
            reason: ApplyRejectReason::MissingFieldBinding,
        }));
    }

    let mut prepared: Vec<PreparedTable> = Vec::with_capacity(by_table.len());
    for (table_name, ops) in by_table {
        let table = match match permit {
            // Receipt path: the pipeline holds the write-domain permit, so
            // table resolution MUST NOT re-admit (non-reentrant domain).
            Some(permit) => repo.get_table_admitted(&table_name, permit).await,
            None => repo.get_table(&table_name).await,
        } {
            Ok(table) => table,
            Err(DbError::NotFound(_)) => {
                return Ok(Err(ApplyOutcome::Rejected {
                    reason: ApplyRejectReason::UnknownTable,
                }));
            }
            Err(error) => return Err(error),
        };
        let legacy_index = table
            .index_manager_ref()
            .iter_indexes()
            .chain(table.index_manager_ref().iter_unique_indexes())
            .map(|index| index.name_interned)
            .next()
            .or_else(|| {
                table
                    .sorted_indexes()
                    .iter_indexes()
                    .first()
                    .map(|index| index.name_interned)
            });
        let index_name = if let Some(id) = legacy_index {
            let interner = table
                .info_store_read_view()
                .interner_read_view()
                .await?
                .get_str(&shamir_types::core::interner::InternerKey::new(id))
                .map(|name| name.to_string());
            Some(interner.ok_or_else(|| DbError::Codec("missing follower index name".into()))?)
        } else {
            table
                .index2_registry()
                .all_descriptors()
                .await
                .first()
                .map(|descriptor| descriptor.name.clone())
        };
        if let Some(index) = index_name {
            return Ok(Err(ApplyOutcome::Rejected {
                reason: ApplyRejectReason::IndexedTable {
                    table: table_name,
                    index,
                },
            }));
        }
        // PR6b-3: the NoMvccStore rejection MUST stay — probe without
        // retaining a clone of the store.
        let token = table_token_for(&table_name);
        let has_mvcc = repo
            .per_table_mvcc()
            .read_sync(&token, |_, _| true)
            .unwrap_or(false);
        if !has_mvcc {
            return Ok(Err(ApplyOutcome::Rejected {
                reason: ApplyRejectReason::NoMvccStore,
            }));
        }
        prepared.push(PreparedTable {
            name: table_name,
            ops,
        });
    }

    // All payloads, bindings, and tables passed preflight before first-touch.
    let interner_manager = repo.repo_interner().await?;
    let interner = interner_manager.get().await?;
    let mut remap = shamir_collections::TFxMap::default();
    for (id, name) in bindings {
        // RU8: replication apply is counted but never quota-rejected.
        let local = interner
            .touch_ind_unmetered(name)
            .map_err(shamir_storage::error::DbError::from)?
            .into_key()
            .id();
        remap.insert(id, local);
    }
    // Required even on retry: an earlier persist may have failed after touch.
    interner_manager.persist().await?;

    let mut rewritten_event = event.clone();
    for change in &mut rewritten_event.changes {
        if change.op == ChangeOp::Put {
            let value = change
                .value
                .take()
                .ok_or_else(|| DbError::Codec("preflighted Put has no value".into()))?;
            change.value = Some(remap_inner_value_bytes(value, &remap).map_err(|error| {
                DbError::Codec(format!("replication translation failed: {error}"))
            })?);
        }
    }
    let mut rewritten_by_table: shamir_collections::TFxMap<String, Vec<KvOp>> =
        shamir_collections::TFxMap::default();
    for change in &rewritten_event.changes {
        let op = match &change.value {
            Some(value) if change.op == ChangeOp::Put => {
                KvOp::Set(change.key.clone().into(), value.clone())
            }
            _ => KvOp::Remove(change.key.clone().into()),
        };
        rewritten_by_table
            .entry(change.table.clone())
            .or_default()
            .push(op);
    }
    for PreparedTable { name, ops, .. } in &mut prepared {
        // Preserve per-table ordering while replacing every Put's bytes.
        *ops = rewritten_by_table
            .remove(name)
            .ok_or_else(|| DbError::Codec("missing preflighted table".into()))?;
    }

    Ok(Ok(PreparedEvent {
        event: rewritten_event,
        tables: prepared,
        version: None,
    }))
}

/// Apply an entire decoded pull batch through the receipt pipeline (the ONLY
/// follower apply protocol, PR6b-3): durable follower state is admitted
/// (typed refusals) and fresh repos adopted before any apply work.
/// cancel-safe: NO — abandonment mid-pipeline parks the gate (requires
/// reopen); the durable receipt is the redo authority.
pub async fn apply_replicated_batch(
    repo: &RepoCore,
    events: &[crate::ReplicatedEvent],
    bookmark: u64,
    policy: ApplyPolicy,
) -> DbResult<ApplyOutcome> {
    // R2a 1c (design Q13): the follower apply is a SEPARATE commit entry that
    // never routes through `commit_tx`, so it admits through the SAME
    // per-repo admission gate — at the top, BEFORE the receipt pipeline's
    // write-domain admission. A closed gate refuses IMMEDIATELY (typed,
    // retryable, no side effect — nothing was applied); it never waits,
    // unlike the leader path.
    let _admission_lease = repo.commit_admission().try_acquire().map_err(|_| {
        DbError::Conflict("commit admission closed; follower apply refused".to_string())
    })?;
    let info = repo.tx_info_store().await?;
    apply_batch_receipt(repo, info, events.to_vec(), bookmark, policy).await
}

/// Re-project a leader-originated [`ChangelogEvent`] for the follower's
/// downstream changefeed, swapping in the follower-local commit version
/// and the follower's repo name. The record changes, actor, and timestamp
/// are preserved verbatim — they ARE the replicated payload.
///
/// Returns `None` if the event has no changes (matching
/// `project_event`'s empty-footprint contract), but in practice a leader
/// never emits an empty event.
fn reproject_for_downstream(
    event: &ChangelogEvent,
    local_repo: &str,
    local_version: u64,
) -> ChangelogEvent {
    ChangelogEvent {
        repo: local_repo.to_string(),
        commit_version: local_version,
        tx_id: event.tx_id,
        actor: event.actor.clone(),
        timestamp_ns: event.timestamp_ns,
        changes: event.changes.clone(),
    }
}

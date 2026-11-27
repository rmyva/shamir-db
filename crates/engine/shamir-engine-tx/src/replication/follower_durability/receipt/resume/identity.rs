//! Receipt identity verification (S5c-X PR6b-2c, review finding 1c): before
//! ANY resume work touches the repo, prove the outstanding receipt still
//! describes EXACTLY the batch the pull offers — same lineage/epoch, same
//! upstream coverage, same field bindings, and byte-identical downstream
//! translations. A mismatch means the durable receipt was produced from
//! different input than this pull claims; allocating replacement versions
//! on top of it could double-apply or silently diverge, so the pull is
//! REFUSED (the receipt stays; `DbError::Storage`).
//!
//! Failure-matrix row 1: the rejected re-translation below may have touched
//! the interner (first-touch of a field name inserts a monotonic-unused
//! id). That is SAFE with no rollback — unused monotonic ids are invisible
//! to reads and get reused by the next genuine first-touch of that name.

use std::sync::Arc;

use shamir_collections::TFxMap;
use shamir_storage::error::{DbError, DbResult};
use shamir_storage::types::Store;

use super::super::super::super::{prepare_event, reproject_for_downstream};
use super::match_offered;
use crate::commit::core::RepoCore;
use crate::meta::repl::bookmark::load_fence_record;
use crate::meta::repl::{ReceiptBinding, ReplicationReceipt};
use crate::repo::changelog_store::write_domain::RepoWritePermit;
use crate::ApplyPolicy;
use crate::ReplicatedEvent;

/// Full four-part identity check between an outstanding receipt and the
/// events the current pull offers:
///
/// (i) the current fence `lineage_id`/`history_epoch` must equal the
///     receipt's (a re-fence or epoch bump invalidates every receipt from
///     the previous history);
/// (ii) every receipt-mapped upstream version must be offered by this pull
///     (the old `match_offered` check — replacements must never be
///     allocated on top of the receipt's local versions);
/// (iii) the union of the offered events' `field_bindings` for the covered
///     versions must equal `receipt.bindings` (id→name pairs) — the leader
///     must not have renamed a field between the original apply and now;
/// (iv) BYTE identity: for each covered version, re-translate the offered
///     event exactly like `apply_fresh` does (`prepare_event` with
///     `applied_watermark = receipt.predecessor_bookmark`), reproject with
///     the receipt's local version, serialize, and compare against
///     `receipt.downstream_events[index]`.
///
/// (i)+(ii) are cheap and run before the resume arm loads bookmark v3;
/// (iii)+(iv) run in the same entry so tests can target the whole check
/// with one call. Every mismatch fails with `DbError::Storage` and the
/// message below — the receipt remains outstanding and the pull is refused.
pub(crate) async fn verify_identity(
    repo: &RepoCore,
    info: &Arc<dyn Store>,
    receipt: &ReplicationReceipt,
    offered: &[ReplicatedEvent],
    permit: &RepoWritePermit,
) -> DbResult<()> {
    // (i) fence identity.
    let fence = load_fence_record(info).await?;
    let (lineage_id, upstream_epoch) = fence
        .map(|fence| (fence.lineage_id, fence.history_epoch))
        .unwrap_or(([0u8; 16], 0));
    if lineage_id != receipt.lineage_id || upstream_epoch != receipt.upstream_epoch {
        return Err(mismatch(format!(
            "fence changed (receipt lineage {:?}/epoch {}, current {:?}/{})",
            receipt.lineage_id, receipt.upstream_epoch, lineage_id, upstream_epoch
        )));
    }
    // (ii) upstream coverage.
    match_offered(receipt, offered)?;
    // (iii) bindings union of the covered events equals the receipt's.
    let mut seen = TFxMap::<u64, ()>::default();
    let mut bindings: Vec<ReceiptBinding> = Vec::new();
    for mapping in &receipt.mapping {
        let Some(event) = offered
            .iter()
            .find(|event| event.event.commit_version == mapping.upstream_version)
        else {
            // Unreachable after (ii); fail closed anyway.
            return Err(mismatch(format!(
                "upstream version {} vanished between checks",
                mapping.upstream_version
            )));
        };
        for (id, name) in &event.field_bindings {
            if seen.insert(*id, ()).is_none() {
                bindings.push(ReceiptBinding {
                    interned_id: *id,
                    name: name.clone(),
                });
            }
        }
    }
    if bindings.len() != receipt.bindings.len()
        || bindings.iter().any(|binding| {
            !receipt.bindings.iter().any(|stored| {
                stored.interned_id == binding.interned_id && stored.name == binding.name
            })
        })
    {
        return Err(mismatch(
            "field bindings differ from the receipt's recorded bindings".into(),
        ));
    }
    // (iv) byte identity of the re-translated downstream events. The
    // watermark is the receipt's OWN predecessor bookmark: that is the
    // allocation state the original translation ran against.
    for (index, mapping) in receipt.mapping.iter().enumerate() {
        #[expect(
            clippy::expect_used,
            reason = "guard: receipt validation proved this mapped upstream version is covered by offered events"
        )]
        let event = offered
            .iter()
            .find(|event| event.event.commit_version == mapping.upstream_version)
            .expect("covered version offered: checked in (ii)/(iii)");
        let prepared = match prepare_event(
            repo,
            &event.event,
            &event.field_bindings,
            receipt.predecessor_bookmark,
            ApplyPolicy::default(),
            Some(permit),
        )
        .await?
        {
            Ok(prepared) => prepared,
            // Skipped is impossible (the version is strictly above the
            // predecessor bookmark) and a rejection is a divergence — both
            // are identity failures.
            Err(outcome) => {
                return Err(mismatch(format!(
                    "offered upstream version {} no longer translates (got {outcome:?})",
                    mapping.upstream_version
                )));
            }
        };
        let downstream =
            reproject_for_downstream(&prepared.event, repo.name(), mapping.local_version);
        let serialized = rmp_serde::to_vec(&downstream)
            .map_err(|error| DbError::Storage(format!("receipt downstream encode: {error}")))?;
        let Some(recorded) = receipt.downstream_events.get(index) else {
            return Err(mismatch(format!(
                "receipt has no downstream bytes for mapping index {index}"
            )));
        };
        if serialized.as_slice() != recorded.as_slice() {
            return Err(mismatch(format!(
                "re-translation of upstream version {} diverges from the receipt's recorded bytes",
                mapping.upstream_version
            )));
        }
    }
    Ok(())
}

/// The canonical refusal error: the receipt STAYS, the pull is refused.
fn mismatch(detail: String) -> DbError {
    DbError::Storage(format!(
        "outstanding receipt identity mismatch: {detail}; \
         refusing to allocate replacements"
    ))
}

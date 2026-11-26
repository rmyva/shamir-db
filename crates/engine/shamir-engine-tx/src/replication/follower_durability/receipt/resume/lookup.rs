//! Resume (S5c-X PR6b-2): find, validate, and match an outstanding receipt
//! left by a crashed follower apply.

use std::sync::Arc;

use shamir_storage::error::{DbError, DbResult};
use shamir_storage::types::Store;

use crate::commit::core::RepoCore;
use crate::meta::repl::bookmark_v3::load_bookmark_v3;
use crate::meta::repl::receipt::load_outstanding_receipt;
use crate::meta::repl::{ReceiptPhase, ReplicationReceipt};
use crate::ReplicatedEvent;

pub(crate) struct Outstanding {
    pub(crate) receipt: ReplicationReceipt,
}

/// Find an outstanding receipt referenced by the bookmark v3 pointer.
/// A pointer without a stored receipt, or a `Checkpointed` receipt, is a
/// corrupt state that fail-stops the repo (reopen).
pub(crate) async fn lookup(
    _repo: &RepoCore,
    info: &Arc<dyn Store>,
) -> DbResult<Option<Outstanding>> {
    let Some(v3) = load_bookmark_v3(info).await? else {
        return Ok(None);
    };
    let Some(id) = v3.outstanding_receipt else {
        return Ok(None);
    };
    let Some(receipt) = load_outstanding_receipt(info, &id).await? else {
        return Err(DbError::RepoRequiresReopen(
            "bookmark references a missing receipt".into(),
        ));
    };
    if receipt.phase == ReceiptPhase::Checkpointed {
        return Err(DbError::RepoRequiresReopen(
            "bookmark references a Checkpointed receipt; unreachable state".into(),
        ));
    }
    Ok(Some(Outstanding { receipt }))
}

/// Every upstream version the receipt covers must be offered by this pull —
/// otherwise replacements could be allocated on top of the receipt's local
/// versions. Retryable: the caller re-pulls wider; NOT reopen.
pub(crate) fn match_offered(
    receipt: &ReplicationReceipt,
    offered: &[ReplicatedEvent],
) -> DbResult<()> {
    for mapping in &receipt.mapping {
        if !offered
            .iter()
            .any(|event| event.event.commit_version == mapping.upstream_version)
        {
            return Err(DbError::Storage(
                "outstanding receipt covers upstream versions not offered by \
                 this pull; refusing to allocate replacements"
                    .into(),
            ));
        }
    }
    Ok(())
}

/// Offered events strictly beyond the receipt's upstream coverage; offered
/// is ordered, so a simple filter preserves the order.
pub(crate) fn tail_events<'a>(
    receipt: &ReplicationReceipt,
    offered: &'a [ReplicatedEvent],
) -> Vec<&'a ReplicatedEvent> {
    offered
        .iter()
        .filter(|event| event.event.commit_version > receipt.upstream_end)
        .collect()
}

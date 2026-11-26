//! Follower admission (S5c-X PR6b-3): the receipt protocol is the ONLY
//! follower apply protocol. `admit_follower` validates the durable follower
//! state at first follower admission and returns the admission verdict;
//! unsupported durable states (legacy bookmark formats, malformed records,
//! floor-without-bookmark evidence, conflicting seed-install state) are
//! REFUSED with typed errors — there is no "choose legacy" result anymore
//! and no migration: the operator reseeds the affected subscription.
//! `Fresh` means the caller must ADOPT (write the v3 baseline) before any
//! apply work.
//!
//! Refusal ordering: the durable-state refusal (legacy/malformed evidence) is
//! a property of the durable state — classification is read-only — and is
//! reported FIRST; the backend capability gate guards every verdict that
//! would allow follower work (Native/Fresh) before any adoption write or
//! allocation.

use std::sync::Arc;

use shamir_storage::error::{DbError, DbResult, FollowerRefusalReason};
use shamir_storage::types::Store;

use crate::commit::core::RepoCore;
use crate::meta::repl::bookmark_v3::{classify_follower_bookmark, FollowerBookmarkState};

/// Admission verdict for the first follower apply.
#[derive(Debug)]
pub(crate) enum FollowerAdmission {
    /// A valid v3 bookmark record exists: durably identified follower.
    Native,
    /// No bookmark record and no floor/installation conflict: eligible —
    /// the caller must adopt (write the v3 baseline) before any apply work.
    Fresh,
}

/// Pure admission decision over the classified durable state (PR6b-3
/// acceptance test 8). Extracted so tests can exercise the capability gate
/// without a backend: `BoxRepo` is a crate-internal enum and cannot be
/// constructed around a refusing store from outside the crate.
///
/// Order: the durable-state refusal comes FIRST — it is a property of the
/// durable state and classification is read-only, so it must be reported
/// regardless of backend capability. The capability gate then guards every
/// verdict that would allow follower work (`Native`/`Fresh`) before any
/// adoption write or allocation happens.
/// `Fresh` here means state-eligible only; the seed-install conflict check
/// stays in [`admit_follower`] because it needs the info store.
pub(crate) fn admission_decision(
    supports_atomic_batch: bool,
    state: FollowerBookmarkState,
) -> DbResult<FollowerAdmission> {
    match state {
        FollowerBookmarkState::Native(_) if supports_atomic_batch => Ok(FollowerAdmission::Native),
        FollowerBookmarkState::Fresh if supports_atomic_batch => Ok(FollowerAdmission::Fresh),
        FollowerBookmarkState::Native(_) | FollowerBookmarkState::Fresh => {
            Err(DbError::AtomicBatchUnsupported(String::new()))
        }
        FollowerBookmarkState::FloorWithoutBookmark { .. } => {
            // A nonzero durable allocation floor without a v3 record is
            // unowned pre-receipt follower evidence: reseed required.
            // Durable-state refusal: reported first, regardless of backend
            // capability — the classification itself is read-only.
            Err(DbError::FollowerStateRefused {
                reason: FollowerRefusalReason::LegacyFollowerState,
            })
        }
    }
}

/// Validate durable follower state at first follower admission. Typed
/// refusals propagate; `Fresh` means the caller must adopt (write the v3
/// baseline) before any apply work.
pub(crate) async fn admit_follower(
    repo: &RepoCore,
    info: &Arc<dyn Store>,
) -> DbResult<FollowerAdmission> {
    let state = classify_follower_bookmark(info).await?;
    let admission = admission_decision(repo.storage_supports_atomic_batch(), state).map_err(
        |error| match error {
            // Restore the repo-identifying message the pure decision cannot know.
            DbError::AtomicBatchUnsupported(_) => DbError::AtomicBatchUnsupported(format!(
                "follower repo {} requires the receipt apply protocol but its \
                 backend does not support repository-wide atomic batches",
                repo.name()
            )),
            other => other,
        },
    )?;
    if matches!(admission, FollowerAdmission::Fresh)
        && crate::repo::seed_install::SeedInstallState::load(info)
            .await?
            .is_some()
    {
        // Conflicting installation state: a seed install is in flight or
        // incomplete — refusing is safer than adopting.
        return Err(DbError::FollowerStateRefused {
            reason: FollowerRefusalReason::LegacyFollowerState,
        });
    }
    Ok(admission)
}

//! Follower cold-start horizon selection (PR6c).
//!
//! PR6 stores the follower's journal horizon `J` in the bookmark record's
//! `clean_through` field. At cold start a record with a known `J` certifies
//! every journal event through `J` as complete, so the uncertain GAP narrows
//! to `in_flight_range`'s single in-flight batch instead of the whole
//! interval after the clean horizon. An old record without a known `J`
//! keeps the conservative behaviour unchanged.

#[cfg(any(test, feature = "test-util"))]
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use shamir_storage::error::DbResult;
use shamir_storage::types::Store;

use crate::meta::repl_bookmark::load_clean_horizon;

#[cfg(any(test, feature = "test-util"))]
static IGNORE_BOOKMARK_J: AtomicBool = AtomicBool::new(false);

/// Negative-control seam: pretend the stored record has no known `J` so the
/// pre-PR6c conservative whole-window GAP is reproduced.
#[cfg(any(test, feature = "test-util"))]
#[doc(hidden)]
pub fn ignore_bookmark_j(ignore: bool) {
    IGNORE_BOOKMARK_J.store(ignore, Ordering::SeqCst);
}

/// The journal horizon a follower cold start may certify as complete.
/// Without upstream provenance (a plain repository) this is the changelog's
/// own clean horizon `j`; a follower record with a known batch horizon
/// raises it to `max(j, record_j)` so only
/// [`crate::apply_replicated::follower_durability::in_flight_range`]
/// stays uncertain.
pub(super) async fn follower_certified_through(
    j: u64,
    follower_provenance: bool,
    info: &Arc<dyn Store>,
) -> DbResult<u64> {
    if !follower_provenance {
        return Ok(j);
    }
    #[cfg(test)]
    if IGNORE_BOOKMARK_J.load(Ordering::SeqCst) {
        return Ok(j);
    }
    let record_j = load_clean_horizon(info).await?;
    Ok(record_j.map_or(j, |record_j| j.max(record_j)))
}

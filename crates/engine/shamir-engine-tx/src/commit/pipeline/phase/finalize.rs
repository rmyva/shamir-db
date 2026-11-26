//! Shared finalization tail for the synchronous commit paths.
//!
//! PR3 (Fowler preparatory refactoring — "make the change easy, then make
//! the easy change"): the synchronous commit path's post-`materialize` tail
//! was extracted here as its single canonical implementation:
//!
//! `post_publish_cleanup` → deferred-metric → `drainer().wake()` →
//! `emit_changefeed_event` → `promote_vectors`
//!
//! The sole in-tree caller today is `commit_tx_lockfree` (`commit.rs`).
//! (F-54, #865 removed the dead `tx/group_commit.rs` batch path —
//! `run_single_tx` / `run_leader` — that previously shared this tail; it
//! had zero production call sites.) `apply_replicated_batch` (R1) does NOT call
//! this either: a replicated raw-apply has neither a `TxContext` (for
//! `promote_vectors`) nor a `PostPublishState` (from `materialize`), so it
//! reimplements the equivalent steps inline — see `apply_replicated.rs`'s
//! "finalize-tail reuse" docs.
//!
//! ## Why `commit_tx_inner_legacy_async` is NOT a caller
//!
//! The AsyncIndex path diverges on two semantically load-bearing axes:
//!  1. It emits the changefeed on the caller thread BEFORE spawning the
//!     background materialize tail; the sync paths emit AFTER `materialize`.
//!  2. Its tail (index → markers → promote) runs in a spawned background
//!     task and returns a `BackgroundCommitHandle`; the sync paths run
//!     inline and return `background: None`.
//!
//! A THIRD axis previously claimed here — that this path's SSI footprint
//! (`record_commit_writes`) runs AFTER `version_guard.commit()` — is FALSE
//! for the current code (F-28/S3-C corrected it): `commit_tx_inner_legacy_async`
//! records the footprint BEFORE `version_guard.commit()` (see commit.rs's own
//! F-28/S3-C comment there), same as the sync paths' Phase 6-bis
//! (`materialize.rs`) and `commit_tx_lockfree`'s own `record_commit_writes`
//! call. The order matters everywhere it appears: `VersionGuard::commit()`
//! synchronously advances `last_committed_version` with no `.await` in
//! between, so recording the footprint AFTER it would let a concurrent
//! Serializable validator observe this tx's version as already-visible with
//! no footprint recorded yet — a missed phantom conflict. Recording first
//! closes that window. It is now a SHARED invariant across all three commit
//! paths, not a divergence — the two axes above are what still justify
//! keeping this tail separate.
//!
//! Folding these into one function would require boolean flags + branch
//! divergence — a leaky abstraction, not a clean seam. The honest shared
//! core is the sync post-publish tail below.
//!
//! All phases here run OUTSIDE `commit_lock` (P2b) and are pure
//! post-commit bookkeeping — the version is already published (Phase 6 ran
//! inside `materialize` via `version_guard.commit()`). None of this may
//! abort the tx.

use std::sync::Arc;

use shamir_tx::{RepoTxGate, TxContext};

use crate::commit::core::RepoCore;
use crate::commit_phases::promote_vectors;
use crate::materialize::{post_publish_cleanup, PostPublishState};
use crate::tx_outcome::MaterializationState;

/// Run the shared synchronous post-publish finalization tail.
///
/// Sequence (all outside `commit_lock`):
///  1. `post_publish_cleanup` — Phase 6.5 recovery markers + A5 interner
///     checkpoint (fire-and-forget). Returns `Complete` or `Deferred`.
///  2. Fire `on_tx_materialization_deferred` if the marker write deferred.
///  3. `drainer().wake()` — nudge the background drainer so the freshly-
///     published version's WAL entry is replayed into `history` promptly
///     (the ack-path wrote only the in-memory overlay; durability is the
///     drainer's job post-D2-cutover).
///  4. `emit_changefeed_event` — publish the tx's record-level changefeed
///     event (if any) to live subscribers.
///  5. `promote_vectors` — Phase 5d, promote staged HNSW vectors into the
///     live graph OUTSIDE the commit critical section (III.5). A failure
///     here is NOT `Deferred` (the graph reconciles via rebuild-on-open).
///
/// `tx` is borrowed (read-only) for `promote_vectors`; `post_publish_state`
/// and `changefeed_event` are consumed. Returns the final
/// [`MaterializationState`] for the caller's `TxOutcome`.
#[inline]
pub(in crate::tx) async fn finalize_sync_post_publish(
    tx: &TxContext,
    post_publish_state: PostPublishState,
    changefeed_event: Option<Arc<shamir_tx::ChangelogEvent>>,
    repo: &RepoCore,
    gate: &RepoTxGate,
    commit_version: u64,
) -> MaterializationState {
    let materialization = post_publish_cleanup(post_publish_state, repo, gate).await;
    if materialization == MaterializationState::Deferred {
        repo.tx_metrics().on_tx_materialization_deferred();
    }
    // D2 P1d-2b CUTOVER: the inline `gate.mark_durable(commit_version)` is
    // GONE. The ack-path no longer writes `history` (only the overlay), so
    // the value is NOT durable at this point — it is durable only after the
    // background drainer replays the WAL entry into `history`. The DRAINER
    // now owns both `mark_durable` and the WAL truncation. We only WAKE it
    // here, after the version is published (visibility), so it drains
    // promptly.
    repo.drainer().wake();
    #[cfg(any(test, feature = "test-util"))]
    repo.fire_post_publish_pre_broadcast_test_hook().await;
    repo.broadcast_changefeed_event(changefeed_event);
    promote_vectors(tx, repo, commit_version).await;
    materialization
}

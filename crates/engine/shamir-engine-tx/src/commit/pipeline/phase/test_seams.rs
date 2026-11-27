//! Test-only injection seams for the commit phases.

#[cfg(any(test, feature = "test-util"))]
use crate::commit::core::RepoCore;
#[cfg(any(test, feature = "test-util"))]
use crate::meta::recovery_marker::save_last_committed;
#[cfg(any(test, feature = "test-util"))]
use shamir_storage::{error::DbError, types::Store};
#[cfg(any(test, feature = "test-util"))]
use std::sync::Arc;

/// Test-only injection: when set to a non-zero tx_id, Phase 5c (index
/// apply) returns a synthetic storage error for the matching tx. Used by
/// `commit_phase5_defer_tests` to prove a post-commit-point failure is
/// reported COMMITTED-with-deferred-materialization (not aborted) and is
/// then reconciled by recovery. Persisted across the bounded retry so
/// the failure is treated as persistent → deferral.
#[cfg(any(test, feature = "test-util"))]
#[doc(hidden)]
pub static FAIL_PHASE_5C_TX_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Test-only injection (audit MED multi-table deferral): when
/// [`FAIL_PHASE_5A_TX_ID`] matches the committing tx AND
/// [`FAIL_PHASE_5A_TABLE_TOKEN`] matches the table being applied, Phase 5a
/// (`apply_data_batch`) returns a synthetic storage error for THAT TABLE
/// ONLY. Keyed by `(table_token, tx_id)` — unlike [`FAIL_PHASE_5C_TX_ID`]
/// which fires for every table of the tx — so a test can fail the SECOND
/// table's data write while the first commits cleanly, producing the
/// partial cross-table materialization the audit flagged. Persisted across
/// the bounded retry so the failure is treated as persistent → deferral.
/// Both registers must be non-zero to arm; a zero token disarms.
#[cfg(any(test, feature = "test-util"))]
#[doc(hidden)]
pub static FAIL_PHASE_5A_TX_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Companion table-token selector for [`FAIL_PHASE_5A_TX_ID`]. See its doc.
#[cfg(any(test, feature = "test-util"))]
#[doc(hidden)]
pub static FAIL_PHASE_5A_TABLE_TOKEN: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Test-only injection (P2 perf fix regression, audit 4.7): unlike
/// [`FAIL_PHASE_5A_TX_ID`] (fails EVERY attempt, proving deferral), this
/// pair fails exactly [`FAIL_PHASE_5A_TRANSIENT_REMAINING`] attempts for
/// the matching tx and then lets the real call through — so a test can
/// prove the bounded retry in [`retry_materialize`] genuinely RECOVERS
/// (not just defers) once `apply_data_batch` no longer needs an upfront
/// `ops.clone()`: the SAME borrowed `&[KvOp]` must still be applied
/// correctly on the attempt that finally succeeds.
#[cfg(any(test, feature = "test-util"))]
#[doc(hidden)]
pub static FAIL_PHASE_5A_TRANSIENT_TX_ID: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Remaining-failures counter for [`FAIL_PHASE_5A_TRANSIENT_TX_ID`]. Each
/// matching call to `apply_data_batch` decrements this and fails while it
/// is `> 0`; once it reaches `0` the call goes through for real.
#[cfg(any(test, feature = "test-util"))]
#[doc(hidden)]
pub static FAIL_PHASE_5A_TRANSIENT_REMAINING: std::sync::atomic::AtomicU32 =
    std::sync::atomic::AtomicU32::new(0);

/// Test-only injection: when set to a non-zero tx_id, the post-lock HNSW
/// promote (Phase 5d, `apply_vector_graph_batch`) returns a synthetic error
/// for the matching tx. Used by `commit_phase5_tests` to prove III.5: a
/// failed promote AFTER `commit_lock` release + Phase 7 does NOT defer the
/// tx (data + index already committed under the lock; the WAL marker is
/// gone; the graph reconciles via restore-on-open of the durable delta
/// chunk that was appended PRE-publish by `apply_vector_delta_phase`).
/// Persisted across the bounded retry so the failure is treated as
/// persistent.
#[cfg(any(test, feature = "test-util"))]
#[doc(hidden)]
pub static FAIL_VECTOR_PROMOTE_TX_ID: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Test-only injection (round-2 finding 1): unlike
/// [`FAIL_VECTOR_PROMOTE_TX_ID`] (which fires BEFORE the ticket loop, so
/// every attempt fails and the tickets Drop unentered), this pair fails
/// exactly [`FAIL_VECTOR_PROMOTE_ONCE_REMAINING`] attempts for the matching
/// tx at the APPLY level INSIDE the ticket loop (after `wait_turn`, before
/// `promote`), so the FIRST attempt fails while the ticket's turn is HELD
/// and the in-loop bounded retry re-runs and succeeds — proving a failed
/// promote is retried with the ticket STILL ACTIVE, not silently
/// acknowledged via a consumed ticket.
#[cfg(any(test, feature = "test-util"))]
#[doc(hidden)]
pub static FAIL_VECTOR_PROMOTE_ONCE_TX_ID: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Remaining-failures counter for [`FAIL_VECTOR_PROMOTE_ONCE_TX_ID`]. Each
/// matching in-loop attempt decrements this and fails while it is `> 0`;
/// once it reaches `0` the real `promote` goes through. Mirrors the
/// `FAIL_PHASE_5A_TRANSIENT_REMAINING` pattern.
#[cfg(any(test, feature = "test-util"))]
#[doc(hidden)]
pub static FAIL_VECTOR_PROMOTE_ONCE_REMAINING: std::sync::atomic::AtomicU32 =
    std::sync::atomic::AtomicU32::new(0);

/// Test-only injection (#426, VR-4 Phase 5d durability variant A): when set
/// to a non-zero tx_id, the PRE-publish delta-chunk append
/// (`apply_vector_delta_batch` / `apply_vector_delta_phase`) returns a
/// synthetic error for the matching tx. Unlike [`FAIL_VECTOR_PROMOTE_TX_ID`]
/// (the graph-half), a delta-half failure MUST surface as `Deferred`: the
/// durable delta-log is the bridge `restore_on_open` relies on, so a
/// pre-publish delta-append failure leaves the vector mutation without a
/// durable echo — the tx is COMMITTED-by-data but the vector is not
/// durably materialized, so recovery cannot reconcile it. Surfacing
/// `Deferred` (not a silent `Complete`) gives the client a detectable
/// signal to retry. Persisted across the bounded retry so the failure is
/// treated as persistent → deferral (not a transient retry success).
#[cfg(any(test, feature = "test-util"))]
#[doc(hidden)]
pub static FAIL_VECTOR_DELTA_TX_ID: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// F-74 (#901) test-only ordering toggle for [`apply_index_batch`]. When
/// `true`, Phase 5c temporarily reverts to the OLD, buggy apply-then-bump
/// order (postings land, THEN the per-index epoch is bumped) instead of the
/// fixed bump-then-apply order, so a test can reconstruct the exact TOCTOU
/// window this task closes and prove it RED, then flip this back to `false`
/// (the default) and prove the identical scenario GREEN. `nextest` runs each
/// test in its own process, so this global cannot leak across test files.
#[cfg(any(test, feature = "test-util"))]
#[doc(hidden)]
pub static TEST_LEGACY_APPLY_THEN_BUMP_ORDER: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// F-74 (#901) test-only pause/resume handshake for
/// [`apply_index_batch`]'s bump-vs-apply seam. Shape mirrors
/// `read_asof_seek.rs`'s `SeekLoopPreIterHook` / `TEST_SEEK_LOOP_PRE_ITER_HOOK`
/// (F-58) and `commit.rs`'s `PostValidatePrePublishHook` /
/// `TEST_POST_VALIDATE_PRE_PUBLISH_HOOK`: `reached` lets the driving test
/// detect (via a `yield_now` poll loop, no sleeps) that the committing tx has
/// actually parked at the seam; `resume` is the release signal; `armed`
/// makes the pause one-shot (CAS true→false) so only the FIRST committer to
/// reach the seam in a given test actually parks.
#[cfg(any(test, feature = "test-util"))]
#[doc(hidden)]
#[derive(Debug)]
pub struct IndexBatchBumpApplyHook {
    pub reached: std::sync::atomic::AtomicUsize,
    pub resume: tokio::sync::Notify,
    pub armed: std::sync::atomic::AtomicBool,
}

#[cfg(any(test, feature = "test-util"))]
#[doc(hidden)]
pub static TEST_INDEX_BATCH_BUMP_APPLY_HOOK: std::sync::OnceLock<
    std::sync::Arc<IndexBatchBumpApplyHook>,
> = std::sync::OnceLock::new();

/// R2a follow-up 3: repository-scoped, one-shot gate that detaches the
/// marker save into a non-abortable task inside `persist_markers` (see
/// [`orphan_marker_park_save`]). Process-global (per-test process
/// isolation).
#[cfg(any(test, feature = "test-util"))]
pub struct OrphanMarkerSaveHook {
    pub repo_name: String,
    pub armed: std::sync::atomic::AtomicBool,
    /// Bounded one-shot: the save task notifies the test it reached the
    /// park (read/compare done, save not started).
    pub reached: std::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    /// Bounded one-shot: the test releases the save task.
    pub release: std::sync::Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
}

#[cfg(any(test, feature = "test-util"))]
pub static TEST_ORPHAN_MARKER_PARK: std::sync::OnceLock<std::sync::Arc<OrphanMarkerSaveHook>> =
    std::sync::OnceLock::new();

/// R2a follow-up 3: run the real marker save through the armed park gate.
/// The save is dispatched as a spawned, non-abortable task owning a clone
/// of `info_store` + `candidate` — the production Fjall `set` blocking
/// task likewise outlives a dropped join handle. The task notifies
/// `reached` (one-shot), waits for `release`, then calls the real
/// `save_last_committed` (`save_u64` → `Store::set`). The join is awaited
/// UNDER the `marker_write_mutex` the caller already holds, so a
/// suspension suspends the WHOLE marker write and any concurrent writer's
/// `try_lock` fails loudly. Replaces the direct save; returns `true` when
/// it ran.
#[cfg(any(test, feature = "test-util"))]
pub(super) async fn orphan_marker_park_save(
    repo: &RepoCore,
    info_store: &Arc<dyn Store>,
    candidate: u64,
) -> Result<bool, DbError> {
    let park = match TEST_ORPHAN_MARKER_PARK.get() {
        Some(p) if p.repo_name == repo.name() => p,
        _ => return Ok(false),
    };
    if !park.armed.swap(false, std::sync::atomic::Ordering::AcqRel) {
        return Ok(false);
    }
    let (reached_tx, release_rx) = {
        let mut reached = park.reached.lock().expect("reached mutex");
        let mut release = park.release.lock().expect("release mutex");
        (
            reached.take().expect("reached taken twice"),
            release.take().expect("release taken twice"),
        )
    };
    let info_store = Arc::clone(info_store);
    let save_task = tokio::spawn(async move {
        let _ = reached_tx.send(()); // one-shot; gone receiver = not watched
        let _ = release_rx.await; // a dropped sender counts as released
        save_last_committed(&info_store, candidate).await
    });
    let saved = save_task
        .await
        .map_err(|e| DbError::Storage(format!("marker save panicked: {e}")))?;
    saved.map(|()| true)
}

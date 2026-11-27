use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use shamir_query_types::batch::{VisibilityPendingReason, VisibilityState, VisibilityWait};
use shamir_storage::error::DbError;
use shamir_tunables::instance_defaults::MAX_UNDRAINED_VERSIONS;
use shamir_tx::visibility_metrics::{AckMode, AckResult, PendingKind};
use shamir_tx::{CancelHandle, TxContext};

use crate::commit::core::RepoCore;
use crate::commit::{commit_tx_inner, release_pessimistic_locks, TxError};
use crate::repo::gate_visibility::{
    observe_gate_with_budget, query_ack_result, record_pending_kind,
};
use crate::tx_outcome::{MaterializationState, TxOutcome};

/// The budget covers only post-known-outcome observation and soft backpressure,
/// not inner execution, WAL uncertainty, async-index durability-error tails,
/// queue/socket backpressure or disconnect ambiguity.
/// cancel-safe: partial — same inner-driver contract as commit_tx; after a known
/// outcome cancellation drops only observation, not materialization or recovery.
pub(crate) fn commit_tx_with_options(
    tx: TxContext,
    repo: &RepoCore,
    wait: VisibilityWait,
) -> impl Future<Output = Result<TxOutcome, TxError>> + '_ {
    commit_tx_with_cancel(tx, repo, wait, None)
}

pub fn commit_tx_with_cancel(
    tx: TxContext,
    repo: &RepoCore,
    wait: VisibilityWait,
    cancel: Option<Arc<CancelHandle>>,
) -> impl Future<Output = Result<TxOutcome, TxError>> + '_ {
    commit_tx_with_kind(tx, repo, wait, PendingKind::ExplicitTx, cancel)
}

/// Same pipeline as `commit_tx_with_options`; a Pending outcome is counted
/// under `kind`, the write family only the caller knows (the shared
/// acknowledgement tail serves explicit and implicit commits alike).
pub async fn commit_batch_tx(
    tx: TxContext,
    repo: &RepoCore,
    wait: VisibilityWait,
    kind: PendingKind,
    cancel: Option<Arc<CancelHandle>>,
) -> Result<TxOutcome, TxError> {
    commit_tx_with_kind(tx, repo, wait, kind, cancel).await
}

pub async fn release_batch_locks(tx: &TxContext, repo: &RepoCore) {
    release_pessimistic_locks(tx, repo).await;
}

pub(crate) async fn commit_tx_with_kind(
    tx: TxContext,
    repo: &RepoCore,
    wait: VisibilityWait,
    kind: PendingKind,
    cancel: Option<Arc<CancelHandle>>,
) -> Result<TxOutcome, TxError> {
    // Validate before entering the pipeline, never erase a known commit.
    if let VisibilityWait::Bounded { timeout_ms } = wait {
        if tokio::time::Instant::now()
            .checked_add(std::time::Duration::from_millis(timeout_ms))
            .is_none()
        {
            release_pessimistic_locks(&tx, repo).await;
            return Err(DbError::VisibilityWaitInvalid {
                reason: "visibility wait duration is not representable".into(),
            }
            .into());
        }
    }
    // Borrow the initialized gate: no new Arc RMW on the Strict/Visible path.
    if repo.initialized_tx_gate().is_none() {
        if let Err(error) = repo.tx_gate().await {
            release_pessimistic_locks(&tx, repo).await;
            return Err(error.into());
        }
    }
    #[expect(
        clippy::expect_used,
        reason = "guard: tx_gate() returned successfully and the initialized gate is never reset"
    )]
    let gate = repo
        .initialized_tx_gate()
        .expect("gate initialized before commit");
    let result = commit_tx_inner(tx, repo, cancel).await;
    let acknowledged =
        acknowledge_known_outcome(result, repo, gate, wait, MAX_UNDRAINED_VERSIONS).await;
    let visibility = match &acknowledged {
        Ok(outcome) => Some(outcome.visibility),
        Err(TxError::WalDurability { visibility, .. }) => Some(*visibility),
        Err(_) => None,
    };
    if let Some(visibility) = visibility {
        record_pending_kind(repo.initialized_tx_gate(), visibility, kind);
    }
    acknowledged
}

/// cancel-safe: yes — only read-only observation and a soft brake remain;
/// dropping an outcome's background handle detaches, never aborts its tail.
pub async fn acknowledge_known_outcome(
    result: Result<TxOutcome, TxError>,
    repo: &RepoCore,
    gate: &shamir_tx::RepoTxGate,
    wait: VisibilityWait,
    high: u64,
) -> Result<TxOutcome, TxError> {
    let mut result = result;
    let target = match &result {
        Ok(outcome) if outcome.commit_version > outcome.snapshot_version => {
            Some(outcome.commit_version)
        }
        Ok(_) => None,
        Err(TxError::WalDurability { commit_version, .. }) => Some(*commit_version),
        Err(error) => {
            if matches!(error, TxError::Storage(_)) {
                repo.tx_metrics().on_tx_aborted_storage();
            }
            return result;
        }
    };
    // Taken only for Bounded; Strict/Visible adds neither a clock nor a timer.
    let mut budget = match wait {
        VisibilityWait::Strict => None,
        VisibilityWait::Bounded { timeout_ms } => {
            let start = tokio::time::Instant::now();
            // A long inner commit can invalidate initial representability.
            // Exhaust an unrepresentable post-known budget, never panic or
            // replace the committed outcome with a validation error.
            Some(
                start
                    .checked_add(std::time::Duration::from_millis(timeout_ms))
                    .unwrap_or(start),
            )
        }
    };
    // VIS-4c wait bookkeeping. Captured ONLY on a genuinely-waiting path;
    // the Strict/Visible fast path (no target, or already visible) never
    // reaches an Instant::now() call or an allocation.
    let mode = match wait {
        VisibilityWait::Strict => AckMode::Strict,
        VisibilityWait::Bounded { .. } => AckMode::Bounded,
    };
    let wait_started = match target {
        Some(version)
            if gate.visible_floor() < version
                && !gate.requires_reopen()
                && !matches!(wait, VisibilityWait::Bounded { timeout_ms: 0 }) =>
        {
            Some(tokio::time::Instant::now())
        }
        _ => None,
    };
    let mut observe_failed = false;
    let visibility = match target {
        None => VisibilityState::Visible,
        Some(version) => {
            let observation = observe_gate_with_budget(
                gate,
                version,
                budget.map(|deadline| {
                    deadline.saturating_duration_since(tokio::time::Instant::now())
                }),
            )
            .await;
            match observation {
                Ok(state) => state,
                Err(error) => {
                    // A version this gate allocated cannot be a future version, and
                    // an unrepresentable post-known deadline is exhausted. Never
                    // replace a known commit with an error or a panic: take the
                    // zero-budget sample (W still wins over timeout/reopen).
                    log::error!("known commit visibility observation failed: {error}");
                    observe_failed = true;
                    budget = Some(tokio::time::Instant::now());
                    settled_sample(gate, version)
                }
            }
        }
    };
    // VIS-4c: one relaxed counter increment per acknowledgement; the
    // duration histogram only when this acknowledgement actually waited.
    // Pending expiry additionally emits a rate-limited structured warning.
    if let Some(version) = target {
        let ack_result = if observe_failed {
            AckResult::Error
        } else {
            query_ack_result(visibility)
        };
        let waited = wait_started.is_some() && ack_result.observed_wait();
        let elapsed = wait_started.map_or(Duration::ZERO, |s| s.elapsed());
        gate.visibility()
            .record_ack(mode, ack_result, waited, elapsed);
        if let VisibilityState::Pending {
            visible_through,
            blocking_version,
            ..
        } = visibility
        {
            // Pending-kind attribution happens at the kind-known call sites
            // (explicit batch, implicit batch, direct write), not here: this
            // acknowledgement tail serves both explicit and implicit commits.
            if gate.visibility().reserve_pending_warning() {
                log::warn!(
                    "transaction acknowledgement pending at wait expiry: repository={}, target={version}, visible_through={visible_through}, blocking_version={blocking_version}, elapsed_ms={}",
                    repo.name(),
                    elapsed.as_millis(),
                );
            }
        }
    }
    // A reopen-required wait keeps the established Deferred marker.
    let reopen = matches!(
        visibility,
        VisibilityState::Pending {
            reason: VisibilityPendingReason::ReopenRequired,
            ..
        }
    );
    match &mut result {
        Ok(outcome) => {
            outcome.visibility = visibility;
            if reopen {
                outcome.materialization = MaterializationState::Deferred;
            }
        }
        Err(TxError::WalDurability {
            visibility: state,
            materialization,
            ..
        }) => {
            *state = visibility;
            if reopen {
                *materialization = MaterializationState::Deferred;
            }
        }
        Err(_) => unreachable!("only known outcomes reach acknowledgement"),
    }
    if visibility == VisibilityState::Visible {
        apply_backpressure_with_budget(repo, gate, high, budget).await;
    }
    result
}

/// Zero-budget final sample of the settled prefix for a known version.
fn settled_sample(gate: &shamir_tx::RepoTxGate, version: u64) -> VisibilityState {
    let visible_through = gate.visible_floor();
    if visible_through >= version {
        return VisibilityState::Visible;
    }
    VisibilityState::Pending {
        visible_through,
        // visible_through < version <= u64::MAX proves this addition fits.
        blocking_version: visible_through + 1,
        reason: if gate.requires_reopen() {
            VisibilityPendingReason::ReopenRequired
        } else {
            VisibilityPendingReason::Timeout
        },
    }
}

/// D2 P1e — soft async backpressure on the undrained version gap.
///
/// The gap is `gate.last_committed() - gate.durable_watermark()`: the number
/// of versions that are visible (published to readers via the overlay) but not
/// yet durable in `history` (the background [`Drainer`](crate::drainer::Drainer)
/// has not replayed their WAL entries). Each undrained version pins an overlay
/// entry + an inflight WAL marker, so an unbounded gap is an unbounded RAM /
/// WAL-tail leak.
///
/// ## Contract
/// * **Fast path (zero overhead):** if `gap <= high` it returns immediately —
///   one pair of atomic loads, no allocation, no wake, no await. This is the
///   common case under any non-pathological load, so a steady committer pays
///   nothing.
/// * **Brake (only under pressure):** once `gap > high`, the committer wakes
///   the drainer and parks on the gate's durable-progress [`Notify`], re-checking
///   the gap each time the drain makes progress. It resumes the instant the gap
///   drops below the LOW watermark `high / 2` (hysteresis: braking to exactly
///   `high` would re-trigger on the very next commit and thrash; draining down
///   to `high/2` amortizes the brake over many commits).
///
/// ## Lost-wakeup safety
/// Each loop iteration registers `gate.durable_notified()` (a `Notified` future)
/// BEFORE re-reading the gap, then awaits it. A concurrent `mark_durable`
/// between the gap read and the await cannot be missed: `notify_waiters` wakes
/// every already-registered waiter, and the future was registered first. We
/// also re-wake the drainer each iteration so a drainer that went idle (its own
/// `notify_one` permit consumed) is nudged back to work.
///
/// ## Deadlock safety (the load-bearing guard)
/// The drainer can get STUCK — a durable-write error (disk full, backend fault)
/// leaves `durable_watermark` frozen, so the gap never closes and the parking
/// loop would wait forever. To make a stuck drain a DEGRADATION (more RAM) and
/// never a HANG, the loop is bounded by a wall-clock budget
/// ([`BACKPRESSURE_MAX_WAIT`]). When the budget is exhausted it logs a warning
/// and RETURNS (proceeds without backpressure). Correctness is untouched — the
/// data is already committed and observable; only the overlay-bounding promise
/// is relaxed under a faulted drain, which is the right trade (never wedge a
/// committer on a broken disk). Each individual park is itself capped by a short
/// `sleep` in the `select!` so a single lost signal cannot stall a whole budget.
///
/// Takes `high` as a parameter so tests can drive the state machine with a tiny
/// artificial gap; production callers pass [`MAX_UNDRAINED_VERSIONS`].
#[cfg(any(test, feature = "test-util"))]
pub async fn apply_backpressure(repo: &RepoCore, high: u64) {
    let Ok(gate) = repo.tx_gate().await else {
        // No gate (repo teardown / construction race) — nothing to brake on.
        return;
    };

    apply_backpressure_with_budget(repo, &gate, high, None).await;
}

/// Soft backpressure, not hard memory admission. Pending skips this entirely.
/// cancel-safe: yes — durable observation and drainer nudges mutate no commit state.
pub(crate) async fn apply_backpressure_with_budget(
    repo: &RepoCore,
    gate: &shamir_tx::RepoTxGate,
    high: u64,
    deadline: Option<tokio::time::Instant>,
) {
    // Fast path: cheap atomic loads, the overwhelmingly common case.
    let gap = gate
        .last_committed()
        .saturating_sub(gate.durable_watermark());
    if gap <= high {
        return;
    }

    // Hysteresis low-watermark: resume only once the drain has caught up to
    // half the threshold, so the brake amortizes over many commits instead of
    // re-firing on every subsequent commit (thrash).
    let low = high / 2;

    log::warn!(
        "tx backpressure ENGAGED: undrained gap {gap} > {high} (low-watermark {low}); \
         parking committer on durable progress"
    );

    let start = tokio::time::Instant::now();
    let deadline = deadline
        .unwrap_or(start + BACKPRESSURE_MAX_WAIT)
        .min(start + BACKPRESSURE_MAX_WAIT);
    loop {
        // Nudge the drainer every iteration: it may have gone idle (its
        // `notify_one` permit already consumed by a prior pass).
        repo.drainer().wake();

        // Register the durable-progress future BEFORE re-reading the gap so a
        // concurrent `mark_durable` cannot slip a wakeup between the check and
        // the park (no lost wakeup).
        let notified = gate.durable_notified();
        tokio::pin!(notified);
        notified.as_mut().enable();

        let gap = gate
            .last_committed()
            .saturating_sub(gate.durable_watermark());
        if gap <= low {
            log::debug!(
                "tx backpressure RELEASED: gap {gap} <= low-watermark {low} after {:?}",
                start.elapsed()
            );
            return;
        }

        // Deadlock guard: if the drain has not closed the gap within the
        // wall-clock budget it is stuck (durable write faulted). Proceed without
        // backpressure rather than hang the committer forever — RAM grows, data
        // is unaffected (already committed + observable).
        if tokio::time::Instant::now() >= deadline {
            log::warn!(
                "tx backpressure ABANDONED after {:?}: undrained gap still {gap} > low {low} — \
                 drain appears stuck; proceeding (overlay bound relaxed, data unaffected)",
                start.elapsed()
            );
            return;
        }

        // Park on durable progress, but cap each park with a short sleep so a
        // single missed signal (or a drainer that needs re-nudging) cannot
        // stall the whole budget — we loop, re-wake, re-check.
        let park_until = (tokio::time::Instant::now() + BACKPRESSURE_PARK_SLICE).min(deadline);
        tokio::select! {
            _ = &mut notified => {}
            _ = tokio::time::sleep_until(park_until) => {}
        }
    }
}

/// Wall-clock ceiling on a single soft backpressure call. Past this the
/// brake is abandoned (drain presumed stuck) and the committer proceeds — a
/// RAM-vs-hang trade that always favours liveness. Generous enough that a
/// healthy-but-slow disk drains within it under normal bursts.
const BACKPRESSURE_MAX_WAIT: std::time::Duration = std::time::Duration::from_secs(5);

/// Per-park cap inside the backpressure loop. Bounds how long one `select!`
/// waits on the durable-progress signal before re-checking the gap and
/// re-nudging the drainer, so a single lost wakeup degrades to a short poll
/// rather than stalling the whole [`BACKPRESSURE_MAX_WAIT`] budget.
const BACKPRESSURE_PARK_SLICE: std::time::Duration = std::time::Duration::from_millis(50);

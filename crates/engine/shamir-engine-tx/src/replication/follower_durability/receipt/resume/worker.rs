//! Receipt worker on the merged lifecycle foundation (S5c-X PR6b-2c — the
//! two mandated replacements of the parent task: this file replaces the old
//! branch's OWN worker task and its wrapper admission with the repository's
//! `ForegroundLifecycle` + `RepoWriteDomain`).
//!
//! Worker = lifecycle foundation: `RepoInstance` owns ONE
//! `Arc<ForegroundLifecycle<ReceiptApplyJob, DbResult<ApplyOutcome>>>` (the
//! existing `worker_lifecycle` field, re-parameterized) plus a lazy
//! `receipt_worker` OnceCell holding the lease. The worker is installed on
//! the FIRST submit via `install_worker(lease, handler)`; `install_worker`
//! is the ONLY `tokio::spawn` in the receipt path. The job payload
//! ([`ReceiptApplyJob`]) carries an owned repo clone + the info store + the
//! events + bookmark + policy; the reply is `DbResult<ApplyOutcome>` via the
//! lifecycle's `LifecycleJob` envelope. The worker holds NO sender and NO
//! owner handle: repository drop drops the foreground token → `close()` →
//! the worker drains and the supervisor joins → the lease is dropped after
//! join, so `ReceiptLease::quarantine_parked_if_any` runs exactly once,
//! through the owner's supervisor.
//!
//! Admission = the repository permit: `run_receipt_pipeline` admits through
//! `write_domain().admit()` (or `admit_with_reservation` when the lease
//! carries a paused reservation) — never through a wrapper mutex.
//!
//! Guard ownership across errors (finding 2): a pipeline that fails while it
//! still holds fresh chunk `VersionGuard`s parks them in the lease instead
//! of dropping them — a dropped guard marks its version `Aborted` and
//! ADVANCES the watermarks, which would let the repo serve over an
//! unpublished version. Parked guards are released ONLY by (a) a later
//! successful publication on this repo (the versions are then ≤ the
//! watermark, so their eventual `Aborted` marks are no-ops), or (b) worker
//! shutdown under QUARANTINE: the gate is set `requires_reopen` and the
//! guards dropped (after reopen the `Aborted` marks are acceptable — the
//! repo is fail-stopped until then).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use super::super::apply::run_receipt_pipeline;
use crate::cold_reconcile::lifecycle::{WorkerExecutionState, WorkerLease};
use crate::commit::core::RepoCore;
use crate::repo::changelog_store::write_domain::RepoWriteReservation;
use crate::{ApplyOutcome, ApplyPolicy, ReplicatedEvent};
use shamir_storage::error::DbResult;
use shamir_storage::types::Store;
use shamir_tx::{RepoTxGate, VersionGuard};

/// One unit of receipt-pipeline work, owned by the lifecycle worker. `repo`
/// is an owned clone: the job outlives any caller that drops its reply
/// receiver. Payload + reply ONLY (`LifecycleJob` carries the reply).
pub(crate) struct ReceiptApplyJob {
    pub(crate) repo: RepoCore,
    pub(crate) info: Arc<dyn Store>,
    pub(crate) events: Vec<ReplicatedEvent>,
    pub(crate) bookmark: u64,
    pub(crate) policy: ApplyPolicy,
}

/// The worker's lease: parked guards, the paused reservation, and the idle
/// signal. `Clone` (inner `Arc`): the repo's OnceCell holds one clone, the
/// installed worker holds another (released by the supervisor strictly after
/// join), and `run_receipt_pipeline` borrows through
/// `WorkerExecutionState::lease()`.
#[derive(Clone)]
pub(crate) struct ReceiptLease {
    inner: Arc<LeaseInner>,
}

struct LeaseInner {
    /// Resolved at install time; owned here so quarantine can fail closed
    /// without re-resolving the repo.
    gate: Arc<RepoTxGate>,
    /// Publication capability from the installed lifecycle epoch.
    status: crate::cold_reconcile::lifecycle::LifecycleStatus,
    /// Fresh chunk guards parked across pipeline errors — see the module
    /// doc. NEVER dropped while the repo keeps serving. Synchronous mutex:
    /// every hold is a short park/retire/quarantine section with NO `.await`
    /// inside (the guard vector itself is moved, never awaited over), and
    /// quarantine runs from `Drop`, which cannot await. Contention: the
    /// lifecycle worker plus the shutdown/quarantine path — failure/shutdown
    /// path only, never a hot path.
    parked: Mutex<Vec<VersionGuard>>,
    /// The paused write reservation (finding: durable pause). While
    /// `Some`, ordinary `admit()` is rejected repo-wide
    /// (`RepoWriteError::Reserved`) and the NEXT pull resumes through
    /// `admit_with_reservation`; the reservation's RAII clears the domain
    /// slot on drop. Same contention model as `parked`: worker +
    /// failure/shutdown only, short sections, never held across `.await`.
    paused_reservation: Mutex<Option<RepoWriteReservation>>,
    /// Jobs currently running (0..1 in practice — the domain permit
    /// serializes). Drives the deterministic `wait_idle` test seam.
    in_flight: AtomicUsize,
    /// Notified on every 1→0 `in_flight` transition.
    idle: tokio::sync::Notify,
}

impl ReceiptLease {
    pub(crate) fn new(
        gate: Arc<RepoTxGate>,
        status: crate::cold_reconcile::lifecycle::LifecycleStatus,
    ) -> Self {
        Self {
            inner: Arc::new(LeaseInner {
                gate,
                status,
                parked: Mutex::new(Vec::new()),
                paused_reservation: Mutex::new(None),
                in_flight: AtomicUsize::new(0),
                idle: tokio::sync::Notify::new(),
            }),
        }
    }

    /// Finding 2: take ownership of a failed chunk's still-fresh guards.
    /// Their Drop marks Aborted + advances the watermarks, so they must NOT
    /// drop while the repo keeps serving — they wait here, parked.
    pub(crate) fn park(&self, guards: Vec<VersionGuard>) {
        if guards.is_empty() {
            return;
        }
        log::warn!(
            "receipt worker: parking {} unpublished version guards across a \
             failed apply",
            guards.len()
        );
        self.inner
            .parked
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .extend(guards);
    }

    /// Commit parked obligations only after both visibility and durability
    /// floors cover them; retain any version not yet safely published.
    pub(crate) fn retire_parked(&self) {
        let visible = self.inner.gate.last_committed();
        let durable = self.inner.gate.durable_watermark();
        let mut parked = self
            .inner
            .parked
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut pending = Vec::with_capacity(parked.len());
        for guard in std::mem::take(&mut *parked) {
            if guard.version() <= visible && guard.version() <= durable {
                guard.commit();
            } else {
                pending.push(guard);
            }
        }
        *parked = pending;
    }

    /// Idle count of parked guards, for tests and diagnostics.
    #[cfg(all(test, feature = "fjall"))]
    pub(crate) fn parked_count(&self) -> usize {
        self.inner
            .parked
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .len()
    }

    /// Park the pipeline's paused write reservation (see `LeaseInner`).
    pub(crate) fn place_pause_reservation(&self, reservation: RepoWriteReservation) {
        let mut slot = self
            .inner
            .paused_reservation
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        // One reservation slot; the domain enforces at most one outstanding
        // anyway. Drop any stale value (its RAII clears the domain slot).
        *slot = Some(reservation);
    }

    /// Take the paused reservation OUT of the lease. Called BEFORE the
    /// `admit_with_reservation` await so the synchronous mutex guard is
    /// never held across `.await` (the future stays `Send`).
    pub(crate) fn take_reservation(&self) -> Option<RepoWriteReservation> {
        self.inner
            .paused_reservation
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
    }

    /// Drop the paused reservation (successful pipeline: the receipt
    /// retired). The reservation's RAII clears the domain slot, so ordinary
    /// `admit()` works again.
    pub(crate) fn clear_reservation(&self) {
        *self
            .inner
            .paused_reservation
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
    }

    fn enter_job(&self) {
        self.inner.in_flight.fetch_add(1, Ordering::AcqRel);
    }

    /// Handler-side bookkeeping: one job finished; returns `true` on the
    /// 1→0 transition (the only transition that notifies `idle`).
    fn exit_job(&self) -> bool {
        self.inner.in_flight.fetch_sub(1, Ordering::AcqRel) == 1
    }

    /// Deterministic test wait until no job is in flight. Cancel-safe
    /// arm-then-check Notify pattern (mirrors `CommitTailBarrier::wait_zero`):
    /// the future is armed BEFORE the counter is checked, so the final
    /// decrement can never race this waiter — no sleeps, no yield loops.
    #[cfg(all(test, feature = "fjall"))]
    pub(crate) async fn wait_idle(&self) {
        loop {
            let notified = self.inner.idle.notified();
            tokio::pin!(notified);
            // Arm before checking: the final decrement may race this waiter.
            notified.as_mut().enable();
            if self.inner.in_flight.load(Ordering::Acquire) == 0 {
                return;
            }
            notified.await;
        }
    }

    /// The publication callback is synchronous and must not re-enter lifecycle operations.
    pub(crate) fn publish<T>(
        &self,
        callback: impl FnOnce() -> T,
    ) -> Result<T, crate::cold_reconcile::lifecycle::PublicationClosed> {
        self.inner.status.publish(callback)
    }

    /// Unconditionally fail closed for an explicit worker quarantine, even
    /// when no guards or reservation happen to be parked. The gate must close
    /// before releasing either durable-pause resource.
    fn force_quarantine(&self) {
        self.inner.gate.set_requires_reopen();
        self.release_durable_pause();
    }

    /// Normal worker exit/lease cleanup only requires reopen when it would
    /// otherwise release a durable pause. Check and close the gate before
    /// dropping guards or the reservation.
    fn quarantine_durable_pause_if_any(&self) -> bool {
        let mut parked = self
            .inner
            .parked
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let has_parked = !parked.is_empty();
        let mut reservation = self
            .inner
            .paused_reservation
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let has_reservation = reservation.is_some();
        if has_parked || has_reservation {
            self.inner.gate.set_requires_reopen();
        }
        // Quarantine can follow a panic while some guards were already
        // parked. Arm fail-closed Drop before releasing them so their
        // completion floors remain unchanged until reopen recovery.
        for guard in parked.iter_mut() {
            guard.visibility_started_without_wal();
        }
        let count = parked.len();
        parked.clear();
        reservation.take();
        drop(reservation);
        drop(parked);
        if count > 0 {
            log::warn!(
                "receipt worker: quarantining {count} parked version guards \
                 (requires_reopen)"
            );
        }
        has_parked || has_reservation
    }

    fn release_durable_pause(&self) {
        self.inner
            .paused_reservation
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        let mut parked = self
            .inner
            .parked
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for guard in parked.iter_mut() {
            guard.visibility_started_without_wal();
        }
        parked.clear();
    }
}

impl WorkerLease for ReceiptLease {
    fn on_quarantine(&self) {
        self.force_quarantine();
    }
}

impl Drop for LeaseInner {
    fn drop(&mut self) {
        let parked = self
            .parked
            .get_mut()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let reservation = self
            .paused_reservation
            .get_mut()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !parked.is_empty() || reservation.is_some() {
            self.gate.set_requires_reopen();
            for guard in parked.iter_mut() {
                guard.visibility_started_without_wal();
            }
        }
        parked.clear();
        reservation.take();
    }
}

/// The single lifecycle handler: drain the queue, one job at a time. The
/// `commit_tail` guard makes `quiesce_commits`/`shutdown_clean` wait for
/// in-flight receipt work. The reply send error is ignored — caller
/// cancellation drops only the receiver, and the worker KEEPS RUNNING the
/// job. After the channel closes (lifecycle `close()`), quarantine any
/// parked guards: the safe durable pause.
type ReceiptJobState = WorkerExecutionState<ReceiptApplyJob, DbResult<ApplyOutcome>, ReceiptLease>;

pub(crate) async fn receipt_handler(mut state: ReceiptJobState) {
    while let Some(mut job) = state.next_job().await {
        state.lease().enter_job();
        let _tail = job.payload.repo.commit_tail().enter();
        // Take/clone payload fields so `job` stays whole for `reply`.
        let events = std::mem::take(&mut job.payload.events);
        let result = run_receipt_pipeline(
            &job.payload.repo,
            Arc::clone(&job.payload.info),
            events,
            job.payload.bookmark,
            job.payload.policy,
            state.lease(),
        )
        .await;
        job.reply(result);
        if state.lease().exit_job() {
            state.lease().inner.idle.notify_waiters();
        }
    }
    if state.lease().quarantine_durable_pause_if_any() {
        // A durable pause survived the queue: shutdown must NOT report
        // ordinary completion — the repository requires reopen.
        state.quarantine_worker("receipt worker shut down with a durable pause");
    }
}

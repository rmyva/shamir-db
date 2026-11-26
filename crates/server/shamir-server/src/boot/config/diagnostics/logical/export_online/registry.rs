//! Online-export job registry state (R3 step 2b, brief
//! `56-r3-2b-online-export-job.md`).
//!
//! Server-wide admission state for the online logical-export job: at most
//! ONE running job per server, with the current/last job kept as the only
//! record (bounded state; see the doc comment on [`ExportRegistry`]).
//! This module is state only — the runner, the maintenance gate, path
//! generation and dispatch wiring are later steps. No task is spawned and
//! no path is generated here; the runner receives an owned [`ExportRun`]
//! ticket and drives the phases itself.
//!
//! Sessions are addressed by the minted, non-secret handle
//! (`Session::activity_handle()`); wire credentials are never stored or
//! logged, and unstamped sessions (`owner == 0`) are refused. Lookups in
//! `status` / `cancel` are session-bound AND database-bound: status never
//! reveals another owner's job, and a start by another owner never erases
//! a live job (both are answered with the same typed [`AlreadyRunning`],
//! which also avoids leaking whether a job runs to a non-owner).
//!
//! Locking: short-lived synchronous `parking_lot::Mutex` data cells used
//! only on rare export control operations (`start` / `status` / `cancel`
//! and the run's progress/terminal transitions). No async lock is
//! involved, no await point exists in this module, and no filesystem or
//! engine access happens while a cell is held.
//!
//! Crash/panic safety: the runner holds an owned ticket whose Drop —
//! running after the runner's own locals have unwound — marks the job
//! `Failed` / `Internal` unless a terminal state was recorded explicitly
//! (or the cancel token fired, in which case `Cancelled`), so a job panic
//! or a dropped runner cannot strand the server-wide admission slot
//! forever.
//!
//! Publication vs cancellation arbitration: the irreversible final rename
//! (temp artifact → final name) is arbitrated under the run's state cell
//! via [`ExportRun::try_begin_publication`] — once it returns `true`, the
//! job is inside a noncancellable publication window; external cancel
//! signals observed before that point (the runner checks its token and
//! its gate deadline before calling) still win and yield `Cancelled`.

use std::sync::Arc;

use parking_lot::Mutex;
use shamir_query_types::admin::{
    ExportCancelReceipt, ExportFailure, ExportPhase, ExportStatusReceipt,
};
use tokio_util::sync::CancellationToken;

/// Typed refusal of a second concurrent export job.
///
/// Returned by [`ExportRegistry::start`] when a job is already admitted
/// on this server (any owner, including the same owner re-starting), and
/// for unstamped sessions (see the module docs — the same answer avoids
/// revealing registry state to a session without a handle).
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("an online export job is already running on this server")]
pub struct AlreadyRunning;

/// Owner identity of a job: the minted, non-secret session handle.
type Owner = u64;

/// One admitted job's session/database binding and shared run state.
struct JobSlot {
    owner: Owner,
    db: String,
    export_id: String,
    cancel: CancellationToken,
    state: Arc<RunState>,
}

/// Mutable state of the running/last job, shared between the registry and
/// the run ticket. Guarded by a short synchronous mutex; nothing blocking
/// runs while it is held.
struct RunInner {
    phase: ExportPhase,
    /// `true` between a successful [`ExportRun::try_begin_publication`]
    /// and the terminal transition: the publication is noncancellable.
    publishing: bool,
    sections: u64,
    items: u64,
    bytes: u64,
    /// Opaque artifact name, `Some` only once `Done`.
    artifact_name: Option<String>,
    failure: Option<ExportFailure>,
    /// The run reached a terminal state (`Done` / `Failed` /
    /// `Cancelled`). Set under the cell exactly once.
    terminal: bool,
}

impl RunInner {
    fn fresh() -> Self {
        Self {
            phase: ExportPhase::WaitingForGate,
            publishing: false,
            sections: 0,
            items: 0,
            bytes: 0,
            artifact_name: None,
            failure: None,
            terminal: false,
        }
    }
}

/// Shared state cell of one job.
struct RunState {
    inner: Mutex<RunInner>,
}

impl RunState {
    fn fresh() -> Self {
        Self {
            inner: Mutex::new(RunInner::fresh()),
        }
    }

    /// Snapshot mapped onto the wire status receipt. `sections` is the
    /// dump's own section counter, fed separately from the item count by
    /// the runner's step hook and the completed report.
    fn receipt(&self, export_id: &str) -> ExportStatusReceipt {
        let inner = self.inner.lock();
        let phase = if inner.publishing {
            // Inside the noncancellable publication window the job has
            // not finished yet; report the last visible phase.
            ExportPhase::Verifying
        } else {
            inner.phase
        };
        ExportStatusReceipt {
            export_id: export_id.to_string(),
            phase,
            sections: inner.sections,
            items: inner.items,
            bytes: inner.bytes,
            artifact_name: inner.artifact_name.clone(),
            failure: inner.failure,
        }
    }
}

/// Server-wide registry of the online logical-export job.
///
/// Bounded state (explicit, per the step contract): the registry keeps
/// exactly ONE record — the current job while it runs, otherwise the last
/// finished job until the next `start` replaces it (the backup registry's
/// "status survives until the next start" precedent, design §4.7). There
/// is no per-session history: a session that did not start the visible
/// record observes `None` from `status` / `cancel`, never another owner's
/// job.
pub struct ExportRegistry {
    /// The single job record; `None` only before the first `start` ever.
    current: Mutex<Option<JobSlot>>,
}

impl Default for ExportRegistry {
    fn default() -> Self {
        Self {
            current: Mutex::new(None),
        }
    }
}

impl ExportRegistry {
    /// Admit a new export job for `owner` (a stamped session handle; `0`
    /// is refused) exporting `db`, identified by `export_id`. Returns the
    /// owned run ticket the runner must hold for the whole job and arms
    /// the caller-supplied token as the job's cancel signal.
    ///
    /// Synchronous, no I/O. Refuses with [`AlreadyRunning`] while any job
    /// (this owner's included) is admitted, so a live job of another
    /// owner is never erased.
    pub fn start(
        &self,
        owner: Owner,
        db: &str,
        export_id: String,
        cancel: CancellationToken,
    ) -> Result<ExportRun, AlreadyRunning> {
        if owner == 0 {
            // Unstamped session: refuse with the same typed answer.
            return Err(AlreadyRunning);
        }
        let mut current = self.current.lock();
        if let Some(slot) = current.as_ref() {
            if !slot.state.inner.lock().terminal {
                // A job is live: no start may erase it, whoever calls.
                return Err(AlreadyRunning);
            }
            // The last job is finished: any new start replaces the
            // record; status/cancel stop resolving for the old owner.
        }
        let state = Arc::new(RunState::fresh());
        *current = Some(JobSlot {
            owner,
            db: db.to_string(),
            export_id: export_id.clone(),
            cancel: cancel.clone(),
            state: Arc::clone(&state),
        });
        drop(current);
        Ok(ExportRun {
            export_id,
            cancel,
            state,
            completed: Mutex::new(false),
        })
    }

    /// Status of `owner`'s job on `db`, if the visible record belongs to
    /// that session and database; otherwise `None` (never another
    /// owner's job, never another database's job).
    pub fn status(&self, owner: Owner, db: &str) -> Option<ExportStatusReceipt> {
        let guard = self.current.lock();
        let slot = guard.as_ref()?;
        Self::matches(slot, owner, db)?;
        Some(slot.state.receipt(&slot.export_id))
    }

    /// Request cancellation of `owner`'s job on `db`. Returns:
    ///
    /// * `Some(cancelled = true)` — the job is live and the cancel signal
    ///   was delivered onto the runner's token;
    /// * `Some(cancelled = false)` — the visible job belongs to this
    ///   owner but has already finished (nothing to cancel);
    /// * `None` — no visible job of this owner on this database.
    pub fn cancel(&self, owner: Owner, db: &str) -> Option<ExportCancelReceipt> {
        let guard = self.current.lock();
        let slot = guard.as_ref()?;
        Self::matches(slot, owner, db)?;
        // One lock decides: terminal or already inside the noncancellable
        // publication window means the cancellation lost (or is moot);
        // otherwise the signal is delivered while the cell is held, so a
        // racing `try_begin_publication` cannot slip in between the check
        // and the signal.
        let state = slot.state.inner.lock();
        let cancellable = !state.terminal && !state.publishing;
        if cancellable {
            slot.cancel.cancel();
        }
        Some(ExportCancelReceipt {
            cancelled: cancellable,
        })
    }

    /// Session-and-database binding check: `Some(())` when the visible
    /// record is this owner's job on this database.
    fn matches(slot: &JobSlot, owner: Owner, db: &str) -> Option<()> {
        if slot.owner == owner && slot.db == db {
            Some(())
        } else {
            None
        }
    }
}

/// Owned run ticket the runner holds for the whole job.
///
/// Dropping the ticket runs the completion guard AFTER the runner's own
/// locals have unwound: unless a terminal state was recorded, the job is
/// marked `Failed` / `Internal` (or `Cancelled` when the cancel token
/// fired), so the server-wide admission slot can never be stranded by a
/// job panic or a dropped runner.
pub struct ExportRun {
    export_id: String,
    cancel: CancellationToken,
    state: Arc<RunState>,
    /// `true` once a terminal transition ran (own short lock; the guard
    /// fires without re-entering the registry).
    completed: Mutex<bool>,
}

impl ExportRun {
    /// The job's export id.
    pub fn export_id(&self) -> &str {
        &self.export_id
    }

    /// The job's cancellation token (the registry mirrors cancel requests
    /// onto it; the runner observes it for cooperative cancellation).
    pub fn cancel_token(&self) -> &CancellationToken {
        &self.cancel
    }

    /// Set the live phase (gate wait → exporting → verifying). Ignored
    /// after any terminal transition.
    pub fn set_phase(&self, phase: ExportPhase) {
        let mut inner = self.state.inner.lock();
        if inner.terminal {
            return;
        }
        inner.phase = phase;
    }

    /// Note the progress counters. Ignored after any terminal transition.
    pub fn note_progress(&self, sections: u64, items: u64, bytes: u64) {
        let mut inner = self.state.inner.lock();
        if inner.terminal {
            return;
        }
        inner.sections = sections;
        inner.items = items;
        inner.bytes = bytes;
    }

    /// Arbitrate the irreversible publication against cancellation.
    ///
    /// The runner calls this with `deadline_elapsed = true` when its gate
    /// deadline (the 600 s ceiling, runner-owned: this module holds no
    /// clock) expired. Returns `true` exactly once: the job enters the
    /// noncancellable publication window (`publishing` under the state
    /// cell), after which no cancel signal — including a racing `cancel`
    /// that fires after the check — can flip the job to `Cancelled`.
    /// Returns `false` (and leaves the job cancellable) when the token
    /// was already cancelled, the deadline elapsed, publication already
    /// began, or the run finished.
    ///
    /// The final rename itself stays the runner's job; on success it
    /// calls [`ExportRun::finish_published`], which is what reports
    /// `Done` — only after the runner has released the maintenance gate
    /// and staged the artifact.
    pub fn try_begin_publication(&self, deadline_elapsed: bool) -> bool {
        let mut inner = self.state.inner.lock();
        // The token and the runner's deadline are checked under the same
        // state cell that flips `publishing`: a cancel request either
        // wins entirely (checked before the flip) or loses entirely
        // (arrives after the flip; see `ExportRegistry::cancel`).
        if self.cancel.is_cancelled() || deadline_elapsed {
            return false;
        }
        if inner.terminal || inner.publishing {
            return false;
        }
        inner.publishing = true;
        inner.phase = ExportPhase::Verifying;
        true
    }

    /// Record successful publication: phase `Done` with the opaque
    /// artifact name. Call only after the gate is released and the final
    /// rename staged; `Done` is never reported earlier.
    pub fn finish_published(&self, artifact_name: String) {
        self.finish(|inner| {
            inner.phase = ExportPhase::Done;
            inner.artifact_name = Some(artifact_name);
        });
    }

    /// Record a typed failure.
    pub fn finish_failed(&self, failure: ExportFailure) {
        self.finish(|inner| {
            inner.phase = ExportPhase::Failed;
            inner.failure = Some(failure);
        });
    }

    /// Record cooperative cancellation. No-op when publication already
    /// began (the cancellation lost the arbitration) or the run finished.
    pub fn finish_cancelled(&self) {
        // Atomic with respect to the publication transition: the check
        // and the terminal write happen under one lock acquisition, so a
        // racing `try_begin_publication` either already flipped
        // `publishing` (this becomes a no-op) or this records `Cancelled`
        // first (that then sees `terminal` and refuses). The sole caller
        // is the runner, which serializes this against its own
        // publication path.
        let mut inner = self.state.inner.lock();
        if inner.terminal || inner.publishing {
            return;
        }
        inner.phase = ExportPhase::Cancelled;
        inner.terminal = true;
        *self.completed.lock() = true;
    }

    /// Apply a terminal outcome under the state cell exactly once.
    fn finish(&self, apply: impl FnOnce(&mut RunInner)) {
        let mut inner = self.state.inner.lock();
        if inner.terminal {
            return;
        }
        inner.publishing = false;
        inner.terminal = true;
        apply(&mut inner);
        *self.completed.lock() = true;
    }
}

impl Drop for ExportRun {
    /// Completion guard: fires after the runner's locals unwound. A run
    /// that never recorded its outcome is stranded no longer — recorded
    /// as `Cancelled` when the cancel token fired, otherwise `Failed` /
    /// `Internal` (the runner disappeared mid-job; no finer reason is
    /// knowable here).
    fn drop(&mut self) {
        if *self.completed.lock() {
            return;
        }
        let cancelled = self.cancel.is_cancelled();
        self.finish(|inner| {
            if cancelled {
                inner.phase = ExportPhase::Cancelled;
            } else {
                inner.phase = ExportPhase::Failed;
                inner.failure = Some(ExportFailure::Internal);
            }
        });
    }
}

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use parking_lot::Mutex;
use tokio::sync::oneshot;

use super::super::super::adapter::PortRunState;
use super::{backup_metrics, RunRecord};

/// Unwind guard of one registered run (R2a-4 defect 4): a panic on the
/// detached runner thread must not leave the record `running` or the §4.8
/// metrics with `shamir_backup_active` stuck at 1. Created in `start`
/// (disarmed — the runner body arms it, so a failed spawn drops it
/// inertly), moved into the spawn closure, and on the unwind path it
/// publishes a failed record and closes the metrics. The body sets the
/// guard's `completed_dir` the moment the protocol returns `Ok` (the
/// exact verified snapshot path), so the guard at unwind distinguishes a
/// COMPLETED, self-verified snapshot — recorded, never deleted — from a
/// partial output (already removed by the protocol's unwind cleanup;
/// the message never promises a removal that may have failed).
/// Publication ordering makes partial state impossible: the body publishes
/// the copy outcome at protocol completion, stays armed across retention,
/// then publishes the final record with `running = false` LAST and disarms —
/// the guard re-publishes nothing. A panic in the published window (the
/// `outcome` sender is already taken) accounts ONE retention failure for a
/// scheduled run, never a second copy counter.
pub(super) struct RunGuard {
    record: Arc<Mutex<RunRecord>>,
    /// The run's live stage hint, read at unwind time (before the permit
    /// drops, the adapter's port-call hints are still consistent).
    run_state: Arc<PortRunState>,
    started_at_unix_ns: u128,
    /// The OPERATOR destination root (the snapshot child is created by
    /// the protocol under it).
    destination: PathBuf,
    /// SCHEDULED run: its unwind message is rendered path-free.
    scheduled: bool,
    /// Set by the runner body IMMEDIATELY after the protocol returned
    /// `Ok` — the exact, atomically created `<to>/<timestamp>` snapshot
    /// directory the protocol verified and kept. No filesystem probing
    /// at unwind time: an unrelated `manifest.json` under the operator
    /// root must never be mistaken for a completed run.
    pub(super) completed_dir: Option<PathBuf>,
    started_at: Instant,
    pub(super) armed: bool,
    /// Whether the body already accounted the COPY outcome (`note_finished`):
    /// an explicit flag, because a MANUAL run has no sender at all and
    /// `outcome.is_none()` could not tell "published" from "never published".
    copy_accounted: bool,
    // Field drop closes the channel on unwind only after Drop publishes
    // the failure; normal completion sends explicitly, before retention.
    outcome: Option<oneshot::Sender<bool>>,
    // The LATE signal: sent exactly once when the run is fully over (final
    // record published, defensive refusal, or the unwind path). Never early.
    finished: Option<oneshot::Sender<()>>,
}

impl RunGuard {
    pub(super) fn new(
        record: Arc<Mutex<RunRecord>>,
        run_state: Arc<PortRunState>,
        started_at_unix_ns: u128,
        destination: PathBuf,
        scheduled: bool,
        outcome: Option<oneshot::Sender<bool>>,
        finished: Option<oneshot::Sender<()>>,
    ) -> Self {
        Self {
            record,
            run_state,
            started_at_unix_ns,
            destination,
            scheduled,
            completed_dir: None,
            started_at: Instant::now(),
            armed: false,
            copy_accounted: false,
            outcome,
            finished,
        }
    }

    /// Publish the COPY outcome at protocol completion; does NOT disarm (the
    /// guard must stay live across retention so a later panic still records).
    pub(super) fn publish_outcome(&mut self, verified: bool) {
        // The accounting is done for EVERY run here; only a scheduled one has a sender.
        self.copy_accounted = true;
        if let Some(outcome) = self.outcome.take() {
            let _ = outcome.send(verified);
        }
    }

    /// Send the late FINISHED signal exactly once.
    fn send_finished(&mut self) {
        if let Some(finished) = self.finished.take() {
            let _ = finished.send(());
        }
    }

    /// Disarm after the final record publication (`running = false`) and send
    /// the late FINISHED signal; the run is fully over.
    pub(super) fn disarm(&mut self) {
        self.armed = false;
        self.send_finished();
    }
}

impl Drop for RunGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let stage = self.run_state.stage();
        // `publish_outcome` marks the accounting done, so this covers the
        // scheduled post-publication window AND a manual run (which has no
        // sender) — the latter must still be counted as ONE copy failure.
        let copy_published = self.copy_accounted;
        let error = match (&self.completed_dir, self.scheduled) {
            (Some(_), true) => String::from(
                "online-backup runner panicked after the protocol completed; \
                 the self-verified snapshot was KEPT — run `verify` before use",
            ),
            (None, true) => format!(
                "online-backup runner panicked before publishing a result (stage hint {stage:?}); \
                 any files the protocol may have left must be re-verified with `verify` before use"
            ),
            (Some(dir), false) => format!(
                "online-backup runner panicked after the protocol completed; the self-verified \
                 snapshot under {} is KEPT — run `verify` before use",
                dir.display()
            ),
            (None, false) => format!(
                "online-backup runner panicked before publishing a result (stage hint {stage:?}); \
                 any files the protocol may have left under {} must be re-verified with `verify` \
                 before use",
                self.destination.display()
            ),
        };
        let mut record = self.record.lock();
        if record.started_at_unix_ns != self.started_at_unix_ns {
            // A replacement run (or the failed-spawn reset in `start`)
            // owns the record: nothing of THIS run may be published. The
            // late signal still resolves — this run IS over.
            drop(record);
            self.send_finished();
            return;
        }
        if !record.running {
            // The body fully published under the record lock (metrics
            // first, `running = false` LAST) before disarming: a final
            // record is atomic, so nothing is missing here and no
            // counter may be re-advanced.
            drop(record);
            self.send_finished();
            return;
        }
        // The guard finalizes the record: failed stage, the verified
        // snapshot's exact path recorded and never deleted (a partial
        // output was already removed by the protocol's unwind cleanup —
        // if that removal failed the message still only directs to
        // `verify`, it does not promise a removal that did not happen).
        record.progress.record_failure(stage, &error);
        record.dest_dir = self.completed_dir.clone();
        record.last_error = Some(error);
        if copy_published {
            // The copy outcome was already published at protocol completion:
            // a panic here is ONE retention failure for a SCHEDULED run (what
            // the module doc promises) and no counter at all for a MANUAL one,
            // which has no retention pass. The copy counters, `active`
            // included, are never touched twice.
            if self.scheduled {
                backup_metrics::note_retention(0, 1, None);
            }
        } else {
            backup_metrics::note_finished(true, stage, 0, self.started_at.elapsed());
        }
        record.running = false;
        drop(record);
        self.send_finished();
    }
}

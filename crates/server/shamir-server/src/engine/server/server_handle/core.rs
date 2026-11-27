//! Runtime handle for a launched server instance.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use shamir_db::{ShamirDb, ShutdownReport, ShutdownWaitStage};
use shamir_sync::durability::tx::ReleaseTails;
use shamir_tunables::runtime::RuntimeTunables;
use tokio::task::JoinHandle;
use tokio_util::sync::{CancellationToken, DropGuard};

#[cfg(test)]
use super::shutdown_probes::{
    fire_release_zero_budget_probe, fire_repo_flush_probe, fire_scheduled_join_probe,
};

use super::release_source_stop::{stop_release_sources_and_drain_tails, ReleaseStopBounds};
use super::shutdown_owner::{
    early_stage_bound, http_listener_settlement, record_expired_owner_wait, stage_bound,
    DataDirLock, HttpListenerOutcome, Settlement, ShutdownOwner, CONN_DRAIN_TIMEOUT,
    REPO_FLUSH_BUDGET,
};
use crate::audit_appender::FjallAuditAppender;
use crate::boot::config::diagnostics::logical::export_online::OnlineExport;
use crate::db_handler::CancelRefusalAudit;
use crate::db_handler::MutationTasks;
use crate::http_rest::RestGate;
use crate::scheduler::Scheduler;
use crate::server::server_launcher::audit_teardown::{teardown_audit, AuditTeardown};
use crate::server::store_release_barrier::release_stores_until;

/// Each late-stage WAIT ends at the earlier instant: stage start + its cap, or
/// outer deadline - reserve (500 ms metadata / 250 ms store release). These
/// bound waits only: synchronous work is not interrupted; a timed-out join's
/// task may continue (the metadata join is detached).
const LATE_RESERVE: Duration = Duration::from_secs(1);

const CONN_ABORT_TIMEOUT: Duration = Duration::from_secs(2);

const STORE_RELEASE_TIMEOUT: Duration = Duration::from_secs(10);

/// Own bound for the online-export drain on shutdown paths (the service's
/// supervisor uses the same cap internally); shared early cutoff applies.
pub(super) const EXPORT_DRAIN_TIMEOUT: Duration = Duration::from_secs(60);

/// Upper bound for joining the periodic meta-snapshot task at teardown step 6.
///
/// The task is told to stop by the root `shutdown_token` at the top of the
/// teardown but still has to run one final lockout + rate-limit snapshot, and
/// that write is submitted to the blocking pool: a saturated or stalled pool
/// keeps the join pending for as long as the pool is starved. The bound turns
/// that into a REPORTED verdict instead of a teardown that never reaches the
/// store-release stage — and, inside an outer shutdown budget, it may only
/// spend what is left of that budget (see [`stage_bound`]).
const META_SNAPSHOT_JOIN_TIMEOUT: Duration = Duration::from_secs(5);

/// Report entry when the meta-snapshot join misses its bound: the periodic
/// metadata snapshot did not settle, so its `Arc<ServerMetaStore>` — and with
/// it the fjall `server_meta` store lock — may still be held. Named separately
/// from the generic deadline message so an outer expiry cannot replace this
/// stage's verdict.
const ERR_META_SNAPSHOT_UNSETTLED: &str = "meta snapshot task did not stop within";

/// Own cap for the interactive-tx record-lock release-tail drains; each call
/// site bounds the drain with ITS stage deadline (early cutoff / outer
/// deadline), capped by this.
pub(super) const RELEASE_TAIL_DRAIN: Duration = Duration::from_secs(10);

/// Upper bound for stopping the observability server in teardown step 5.
const OBS_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// Report entry for an observability shutdown that misses its bound.
const ERR_OBS_SHUTDOWN: &str = "observability shutdown did not complete: the metrics listener or \
                                 the poller may still hold the database";

/// Report entry for a metrics listener that missed its own join bound: it may
/// still be serving a scrape and holding the database.
const ERR_OBS_LISTENER_DRAINING: &str =
    "observability shutdown did not complete: the metrics listener is still draining";

/// Report entry for a poller that missed its own join bound: it may still be
/// inside a tick and holding the database.
const ERR_OBS_POLLER_RUNNING: &str =
    "observability shutdown did not complete: the poller is still running";

/// Handle for the periodic meta-snapshot task. Just the `JoinHandle` — the
/// stop signal is the shared root `shutdown_token` (cancelled by
/// [`ServerHandle::shutdown`] before this handle is awaited), so no per-task
/// stop channel is needed.
pub(in crate::engine::server) struct MetaSnapshotTask {
    pub(in crate::engine::server) handle: JoinHandle<()>,
}

/// Handle for the periodic follower-replication reconcile-tick task (386-c).
/// Stopped via the shared root `shutdown_token`; the `JoinHandle` is awaited
/// on shutdown so the task drops its `Arc<SubscriptionSupervisor>` reference.
pub struct ReplSupervisorTask {
    pub(in crate::engine::server) handle: JoinHandle<()>,
}

/// R4b-1 gauge-binding test seam (`cfg(test)` only — the field is absent
/// from production builds and the type is never constructed outside
/// `ServerLauncher::launch`'s `#[cfg(test)]` capture): the production-boot
/// artifacts the observability activity-gauge source closes over.
///
/// Exposed so `server_launcher/tests` can drive the REAL
/// [`ShamirDbHandler`](crate::db_handler::ShamirDbHandler) the
/// launcher built (with the launcher's own `ByteBudget` and server-wide
/// `SessionStore`) and scrape the REAL `/metrics` endpoint through the
/// actual bound observability address — proving the boot path's
/// `set_activity_source` wiring, not a separately-constructed fixture.
#[cfg(test)]
pub(in crate::engine::server) struct ActivityGaugeTestSeam {
    /// The concrete request handler the launcher built and handed to every
    /// listener — the same instance the activity source reads mirrors from.
    pub handler: Arc<crate::db_handler::ShamirDbHandler>,
    /// The launcher's RI-15 in-flight response-byte budget (shared with the
    /// handler) — a test can hold it fully to park real request futures.
    pub byte_budget: crate::byte_budget::ByteBudget,
    /// The server-wide session store the handler was given via
    /// `with_session_store`.
    pub session_store: Arc<shamir_connect::server::session::SessionStore>,
    pub audit_chain: Arc<shamir_connect::server::audit_chain::AuditChain>,
    /// The address the observability HTTP server ACTUALLY bound (config
    /// may say port 0; this is `ObservabilityHandle::bound_addr`).
    pub observability_bound_addr: std::net::SocketAddr,
    /// ARGON2-GATE (test-only): the boot's Argon2 semaphore — a fixture can
    /// pre-occupy the pool to force a typed busy refusal in `CreateScramUser`,
    /// and observe it free again after. Shares the derivation gate's `Arc`.
    pub argon2_sem: Arc<shamir_connect::server::argon2_semaphore::Argon2Semaphore>,
}

/// Owner of the runtime state of a launched server.
pub struct ServerHandle {
    /// Live token service, including token mirror and GC lifecycle.
    pub token_service: Arc<crate::security::tokens::TokenService>,
    /// Addresses the server actually bound, in the same order as
    /// `config.listeners`. `None` entries correspond to either a skipped
    /// listener (an unsupported `(kind, profile)` combination — see the
    /// `AcceptPath` match in `server_launcher/launch.rs`) OR a `kind: unix`
    /// listener, which has no `SocketAddr` to report — see
    /// [`Self::bound_ipc_paths`] for that case.
    pub bound_addrs: Vec<Option<SocketAddr>>,
    /// Companion to [`Self::bound_addrs`], same length/order: `Some(path)`
    /// for each `kind: unix` listener (filesystem path / pipe name), `None`
    /// for all others.
    pub bound_ipc_paths: Vec<Option<String>>,
    /// HTTP REST gate maintenance workers, retained until connections have drained.
    pub(in crate::engine::server) http_gates: Vec<Arc<RestGate>>,
    /// Per-listener accept-loop join handles.
    pub(in crate::engine::server) listener_tasks: Vec<JoinHandle<HttpListenerOutcome>>,
    /// Background task scheduler.
    pub(in crate::engine::server) scheduler: Scheduler,
    /// Audit-log appender (drained on shutdown).
    pub(in crate::engine::server) audit_appender: Arc<FjallAuditAppender>,
    pub(in crate::engine::server) cancel_refusal_audit: Arc<CancelRefusalAudit>,
    #[cfg(test)]
    pub(in crate::engine::server) refusal_drain_pending: Option<tokio::sync::oneshot::Sender<()>>,
    /// Cancellation token signalling shutdown to every accept loop.
    ///
    /// Why CancellationToken, not `Notify::notify_waiters`: Notify is
    /// lossy across the subscribe-window. `select!` polls `shutdown.notified()`
    /// — that creates a *new* Notified future on every poll. Between two
    /// polls there is no live waiter; a `notify_waiters()` that lands there
    /// is silently dropped. Result: accept loop hangs forever in
    /// `listener.accept().await`, observed at cold-start ~10% of the time
    /// on this codebase. CancellationToken sets a persistent flag; any
    /// future `.cancelled().await` resolves immediately if cancel was
    /// already set, so the race is closed by construction.
    pub(in crate::engine::server) shutdown_token: CancellationToken,
    /// Dropping the handle also signals this task; only graceful shutdown joins it.
    pub(in crate::engine::server) backup_schedule_task: Option<(DropGuard, JoinHandle<()>)>,
    /// Optional observability HTTP server.
    pub(in crate::engine::server) observability: Option<crate::observability::ObservabilityHandle>,
    /// Periodic meta-snapshot task (M2 — persistent lockout + rate-limit
    /// state). One 60s tick persists BOTH the lockout store and the
    /// rate-limiter buckets. Awaited on shutdown so the fjall `server_meta`
    /// store lock is released cleanly before a same-data-dir restart can succeed.
    pub(in crate::engine::server) meta_snapshot_task: Option<MetaSnapshotTask>,
    /// Phase B Stage 6 — periodic reaper for expired interactive txs.
    /// Drained on shutdown so the `Arc<TxRegistry>` reference held by the
    /// task drops and any lingering open txs are dropped (RAII abort).
    pub(in crate::engine::server) interactive_tx_reaper: Option<crate::tx_registry::ReaperTask>,
    /// FG-5b — periodic reaper for idle-timeout-expired result cursors.
    /// Drained on shutdown so the `Arc<CursorRegistry>` reference held by
    /// the task drops and any lingering open cursors release their pinned
    /// `SnapshotGuard`s (RAII unpin of MVCC GC).
    pub(in crate::engine::server) cursor_reaper: Option<crate::cursor_registry::CursorReaperTask>,
    /// R3-2b — the online logical-export job service, when a root is
    /// configured. Held until shutdown: the export root's exclusive lock
    /// must outlive every job, and the shutdown waits (bounded)
    /// for a draining worker BEFORE the store flush (a freeze holds the
    /// gate). `None` without a configured root. Dropped BEFORE the
    /// store-release probe: the service owns `Arc<ShamirDb>` and the durable
    /// user directory, and a reference surviving stage 9 would falsely
    /// report every store `StillLocked` on an idle export-enabled server.
    pub(in crate::engine::server) online_export: Option<Arc<OnlineExport>>,
    /// The interactive-tx registry (release-tail producer host): shutdown
    /// stops it (reaper join), empties it and closes the tail admission —
    /// the ONE ordered stop in `release_source_stop` — all BEFORE the
    /// repository flush, so every lingering transaction's `Drop` release is
    /// REGISTERED while admission is still open and no release can run after
    /// the flush.
    pub(in crate::engine::server) tx_registry: Arc<crate::tx_registry::TxRegistry>,
    pub(in crate::engine::server) owned_mutation_tasks: Option<Arc<MutationTasks>>,
    /// Owner of the interactive-tx record-lock release tails, stopped,
    /// cleared and drained BEFORE the repository flush via
    /// `release_source_stop`.
    pub(in crate::engine::server) tx_release_tails: Option<Arc<ReleaseTails>>,
    /// Follower-replication supervisor (386-c). Held here so its follower
    /// loops survive past boot (dropping it would kill every loop). On
    /// shutdown `stop_all()` cancels every running loop before the tasks are
    /// joined.
    pub(in crate::engine::server) repl_supervisor: Arc<crate::replication::SubscriptionSupervisor>,
    /// Periodic reconcile-tick task driving [`Self::repl_supervisor`]. Cancelled
    /// by the root `shutdown_token` and joined on shutdown.
    pub(in crate::engine::server) repl_supervisor_task: Option<ReplSupervisorTask>,
    /// ShamirDb reference — needed on shutdown to drain all repo
    /// MemBuffers to durable backing, closing the ~500 ms buffered-
    /// commit loss window on graceful stop.
    pub(in crate::engine::server) shamir: Arc<ShamirDb>,
    /// Data dir the stores live under — scanned by the shutdown barrier that
    /// waits for their directory locks to be released.
    pub(in crate::engine::server) data_dir: PathBuf,
    /// Live-connection tracker: shutdown waits on it until every connection
    /// task (told to end through `shutdown_token`) has dropped its handler
    /// context and, with it, its references to the stores.
    pub(in crate::engine::server) conn_tasks: crate::connection::ConnTasks,
    /// Outcome of the boot-time format-stamp check (release-1 R1a,
    /// design 3.2): what the check did to `data_dir` before any store
    /// opened (created / adopted / unchanged / version-updated).
    pub(in crate::engine::server) format_stamp: shamir_sync::format_stamp::StampOutcome,
    /// RAII single-instance guard: an advisory OS file lock on
    /// `<data_dir>/.shamir.lock`, wrapped in [`DataDirLock`]. The kernel
    /// releases it automatically when the inner `File` is dropped (or the
    /// process crashes), so a second `shamir-server` that tries to open the
    /// same `data_dir` gets `BootError::AlreadyRunning` instead of silently
    /// corrupting the fjall stores. The wrapper releases the lock ONLY via
    /// [`DataDirLock::release_clean`] at the end of a completed clean drain;
    /// dropping this handle — or dropping/cancelling the shutdown future —
    /// LEAKS the file on purpose, so the OS keeps the lock until the process
    /// exits. `None` once [`ServerHandle::take_shutdown_owner`] moved it out
    /// for the embedding path.
    pub(in crate::engine::server) data_dir_lock: Option<DataDirLock>,
    /// Instance-level runtime tunables. Initialized from `instance_defaults`
    /// consts — reads are a single atomic load (instant, non-blocking).
    /// Consumer wiring to accept-loop sleep sites is deferred to a follow-up
    /// slice to keep this slice small and behaviour-identical.
    pub tunables: Arc<RuntimeTunables>,
    /// R4b-1 gauge-binding test seam (see [`ActivityGaugeTestSeam`]).
    /// `None` in production builds entirely (`#[cfg(test)]` field).
    #[cfg(test)]
    pub(in crate::engine::server) activity_test_seam: Option<ActivityGaugeTestSeam>,
}

/// cancel-safe: yes — timeout cancels only the wait; abort_all is idempotent.
pub(crate) async fn drain_connections(
    tasks: &crate::connection::ConnTasks,
    drain: Duration,
    abort: Duration,
    report: &mut ShutdownReport,
) -> bool {
    if tokio::time::timeout(drain, tasks.wait_idle()).await.is_ok() {
        return true;
    }

    let live = tasks.live();
    tasks.abort_all();
    let joined = tokio::time::timeout(abort, tasks.wait_idle()).await.is_ok();
    report.add_error(format!(
        "connection drain exceeded {drain:?}; aborted {live} connection task(s){}",
        if joined {
            ""
        } else {
            "; still running after abort bound"
        },
    ));
    tracing::error!(
        live,
        joined,
        ?drain,
        ?abort,
        "shutdown: connection drain timed out"
    );
    joined
}

fn report_unsubmitted_release_probe(report: &mut ShutdownReport, reason: &str) {
    report.add_error(format!(
        "store release could not be verified: no probe result was obtained; no probe was started / {reason}; \
         whether every store released its directory lock is UNKNOWN; an immediate relaunch of the same \
         data directory can fail with `Locked`"
    ));
}

/// NEW item (rust-guard: an EXISTING item's visibility must not widen): the
/// owner-drain store-release outcome reporter calls the private probe
/// reporter through this wrapper instead of widening it.
pub(in crate::engine::server) fn report_unsubmitted_release_probe_for_owner(
    report: &mut ShutdownReport,
    reason: &str,
) {
    report_unsubmitted_release_probe(report, reason);
}

impl ServerHandle {
    /// What the boot-time format-stamp check did to `data_dir`.
    pub fn format_stamp(&self) -> shamir_sync::format_stamp::StampOutcome {
        self.format_stamp
    }

    /// Stop accepting connections, then drain EVERYTHING the binary's own
    /// path drains: a THIN wrapper over
    /// [`crate::boot::runtime::drain_server`] (the same owner extraction,
    /// bounded teardown and post-deadline owner drain `serve` uses).
    ///
    /// PUBLIC FUTURE vs INNER TIMED TEARDOWN: the future callers await
    /// resolves only after the INNER timed teardown AND the owner drain have
    /// finished; the owner (worker + lock) is OUTSIDE the inner bound, and
    /// dropping the PUBLIC future drops the guard, which LEAKS the lock and
    /// may signal cancellation — it does not synchronously stop native work.
    ///
    /// WHAT REALLY HAPPENS IF THIS FUTURE IS DROPPED OR CANCELLED: the
    /// shutdown owner — the scheduled-backup worker and the data-directory
    /// instance lock ([`DataDirLock`]) — lives OUTSIDE this future (borrowed
    /// through `&mut`), so dropping it half way does NOT cancel the worker's
    /// task and does NOT release the instance lock. The lock file is LEAKED
    /// instead: the OS keeps the advisory data-directory lock until the
    /// process exits, over the possibly still-running backup job. ONLY a
    /// future awaited to a COMPLETED clean drain releases the lock (the
    /// owner drain's `release_clean`); a drain whose owner wait expired also
    /// leaks it, and the report then names the expired stage (exit code 70,
    /// the abnormal-report semantics).
    pub async fn shutdown(self) -> ShutdownReport {
        let report =
            crate::boot::runtime::drain_server(self, crate::boot::runtime::drain_deadline()).await;
        crate::observability::record_shutdown(&report);
        report
    }

    pub(crate) fn pending_shutdown_report(&self) -> ShutdownReport {
        self.shamir.shutdown_report()
    }

    /// Move the scheduled worker and the instance lock out of the handle.
    ///
    /// `runtime::serve` extracts this BEFORE wrapping shutdown in its deadline,
    /// so expiry cannot drop a running native backup thread or release the data directory.
    /// Callers retaining the handle until consumption (`shutdown`,
    /// `shutdown`) are unaffected: they still join the worker and
    /// drop the lock themselves.
    pub(crate) fn take_shutdown_owner(&mut self) -> ShutdownOwner {
        ShutdownOwner {
            schedule_task: self.backup_schedule_task.take(),
            owned_mutation_tasks: self.owned_mutation_tasks.clone(),
            // ONLY the drain handle: cloning the whole service here would
            // keep `Arc<ShamirDb>` + the durable user directory alive past
            // the stage-9 store-release probe and falsely report every
            // store `StillLocked` on an idle export-enabled server.
            online_export: self.online_export.as_ref().map(|svc| svc.worker_drain()),
            tx_release_tails: self.tx_release_tails.clone(),
            tx_reaper: self.interactive_tx_reaper.take(),
            tx_registry: Some(Arc::clone(&self.tx_registry)),
            data_dir_lock: self.data_dir_lock.take(),
        }
    }

    /// The teardown and the instant the caller's outer deadline expires.
    ///
    /// `deadline_at` is the instant bounding the caller's timeout, computed
    /// before it starts. The early cutoff is fixed at `outer_deadline -
    /// LATE_RESERVE`; early waits share it, while late stages reserve metadata,
    /// release-probe, and report time.
    ///
    /// cancel-safe: NO — joins and flushes may be incomplete; caller retains
    /// repository outcomes and marks deadline expiry as unclean. `owner` is
    /// borrowed, not taken: dropping this future must never drop the
    /// scheduled worker or the instance lock (the lock would be LEAKED by
    /// [`DataDirLock`]'s `Drop`, keeping the data directory locked until the
    /// process exits).
    /// Returns the typed settlement of the teardown's OWN settlement points
    /// (`Settled` = the bounded teardown ran to completion and every point
    /// proved its work ended: quiescence, the release-source stop and the
    /// store-release probe). Any error anywhere keeps the report unclean, so
    /// the final lock decision in `ShutdownOwner::drain` still fails closed.
    pub(crate) async fn shutdown_with_budget(
        mut self,
        report: &mut ShutdownReport,
        owner: &mut ShutdownOwner,
        deadline_at: Option<tokio::time::Instant>,
    ) -> Settlement {
        // 1. Tell all accept loops and refusal-audit producers to stop.
        // CancellationToken::cancel() is persistent: every existing waiter
        // resolves on its next poll, and new callers observe cancellation.
        self.cancel_refusal_audit.close();
        self.shutdown_token.cancel();
        // 2. Wait for accept loops to finish, then wait for tracked accepted
        // tasks to quiesce before any repository flush.
        let early_cutoff = deadline_at.map(|outer| {
            outer
                .checked_sub(LATE_RESERVE)
                .unwrap_or_else(tokio::time::Instant::now)
        });
        let (listener_deadline, listener_budget) =
            early_stage_bound(early_cutoff, CONN_DRAIN_TIMEOUT);
        // One fixed early cutoff shared by each bounded wait.
        let mut quiesced = true;
        let mut settlement = Settlement::Settled;
        for mut task in self.listener_tasks {
            let abort = task.abort_handle();
            match tokio::time::timeout_at(listener_deadline, &mut task).await {
                Ok(joined) => {
                    let outcome = match joined {
                        Ok(outcome) => outcome,
                        Err(error) => {
                            report.add_error(format!("listener task failed: {error}"));
                            quiesced = false;
                            HttpListenerOutcome::settled()
                        }
                    };
                    settlement = settlement.worst(http_listener_settlement(&outcome, report));
                }
                Err(_) => {
                    abort.abort();
                    let joined = tokio::time::timeout_at(
                        early_cutoff
                            .unwrap_or_else(|| tokio::time::Instant::now() + CONN_ABORT_TIMEOUT),
                        &mut task,
                    )
                    .await
                    .is_ok();
                    report.add_error(format!(
                        "HTTP listener drain exceeded {listener_budget:?}; aborted"
                    ));
                    quiesced &= joined;
                }
            }
        }
        // One drain budget for listeners and accepted connections together, so
        // the stage stays inside the runtime shutdown deadline.
        let cutoff =
            early_cutoff.unwrap_or_else(|| tokio::time::Instant::now() + CONN_DRAIN_TIMEOUT);
        let remaining = cutoff.saturating_duration_since(tokio::time::Instant::now());
        let abort_remaining = cutoff
            .saturating_duration_since(tokio::time::Instant::now())
            .min(CONN_ABORT_TIMEOUT);
        let drain = drain_connections(&self.conn_tasks, remaining, abort_remaining, report);
        match tokio::time::timeout_at(cutoff, drain).await {
            Ok(drained) => quiesced &= drained,
            Err(_) => {
                // The drain future was dropped before it could signal the
                // forced abort: signal it here (idempotent).
                self.conn_tasks.abort_all();
                report.add_error(
                    "connection drain and abort exceeded shared early shutdown cutoff".to_string(),
                );
                quiesced = false;
            }
        }
        // Gates stop after transport and connection tasks drain, so the final
        // audit flush includes their notices.
        for gate in &self.http_gates {
            if early_cutoff.is_some() {
                let (gate_deadline, budget) = early_stage_bound(early_cutoff, CONN_DRAIN_TIMEOUT);
                if tokio::time::timeout_at(gate_deadline, gate.shutdown())
                    .await
                    .is_err()
                {
                    report.add_error(format!("HTTP gate shutdown exceeded {budget:?}"));
                }
            } else {
                gate.shutdown().await;
            }
        }
        drop(self.http_gates);
        // Bounded with or without an outer deadline: a GC pass is a blocking job.
        tracing::debug!(stage = "token_gc", "shutdown stage");
        let (gc_deadline, gc_budget) = early_stage_bound(early_cutoff, CONN_DRAIN_TIMEOUT);
        if tokio::time::timeout_at(gc_deadline, self.token_service.shutdown_gc())
            .await
            .is_err()
        {
            report.add_error(format!("token GC shutdown exceeded {gc_budget:?}"));
        }
        // 2b. Stop the follower-replication supervisor: latch the stop admission
        //     and signal every follower first (no lifecycle or registry lock, no
        //     await; delivering the signal is synchronous and is not what the cap
        //     below bounds), then join the followers and the reconcile-tick task
        //     (already told to stop by the root `shutdown_token.cancel()` above).
        //     Done after the accept loops stop and before flush. The asynchronous
        //     join is bounded with or without an outer deadline; an expiry means
        //     follower quiescence was not established (they were already signalled
        //     and exit at their next cancellation checkpoint), so the repository
        //     flush is skipped.
        tracing::debug!(stage = "replication", "shutdown stage");
        self.repl_supervisor.begin_stop();
        let (repl_deadline, repl_budget) = early_stage_bound(early_cutoff, CONN_DRAIN_TIMEOUT);
        if tokio::time::timeout_at(repl_deadline, async {
            self.repl_supervisor.stop_all().await;
            if let Some(task) = &mut self.repl_supervisor_task {
                let _ = (&mut task.handle).await;
            }
        })
        .await
        .is_err()
        {
            report.add_error(format!(
                "replication supervisor shutdown exceeded {repl_budget:?}"
            ));
            quiesced = false;
        }
        // 3. Drain the refusal-audit queue and the audit chain under ONE
        //    shared deadline: the scheduler is signalled before it is joined,
        //    the appender's final drain + anchor write runs on a blocking
        //    thread, and a stalled checkpoint or flusher marks the report
        //    unclean instead of stalling the shutdown future. Teardown
        //    continues to the repository flush either way; a timeout does not
        //    cancel an in-flight fsync.
        if early_cutoff.is_some() {
            // Stop admitting new refusal audits and signal the scheduler. The
            // appender is fenced by `teardown_audit` AFTER it drained the work
            // already accepted (a stop flag now would reject those appends).
            self.cancel_refusal_audit.close();
            self.scheduler.signal_shutdown();
        }
        let audit = teardown_audit(
            AuditTeardown {
                cancel_refusal_audit: &self.cancel_refusal_audit,
                appender: &self.audit_appender,
                scheduler: self.scheduler,
                #[cfg(test)]
                drain_probe: self.refusal_drain_pending,
            },
            report,
        );
        if deadline_at.is_some() {
            let (audit_deadline, audit_budget) = early_stage_bound(
                early_cutoff,
                crate::server::server_launcher::audit_teardown::AUDIT_TEARDOWN_DEADLINE,
            );
            if tokio::time::timeout_at(audit_deadline, audit)
                .await
                .is_err()
            {
                // The abandoned audit wait may mean the flusher never received
                // its stop request; signal it here (idempotently). This stops
                // the wait, not audit work already in a gate or fsync.
                self.audit_appender.request_flusher_stop();
                report.add_error(format!(
                    "audit teardown exceeded shutdown budget {audit_budget:?}"
                ));
            }
        } else {
            audit.await;
        }
        // Signal the observability server to stop BEFORE the wait a deadline can
        // abandon; the wait itself is bounded and reported in step 5. The
        // handle's private cancel-on-drop field cancels the same token, so an
        // expiry that drops this future cannot leave a poller and a router
        // holding the database; the full await stays in step 5.
        if let Some(obs) = &self.observability {
            obs.cancel();
        }
        tracing::debug!(stage = "schedule_join", "shutdown stage");
        // Do not abort: an in-flight copy or retention operation must finish
        // cleanup. This is the ORIGINAL position — before the repository flush
        // — and it borrows the owner, so a deadline can abandon this teardown
        // without abandoning the worker (`ShutdownOwner::drain` then awaits it
        // after the deadline).
        if deadline_at.is_some() {
            let (schedule_deadline, schedule_budget) =
                early_stage_bound(early_cutoff, REPO_FLUSH_BUDGET);
            #[cfg(test)]
            fire_scheduled_join_probe();
            if owner
                .join_schedule(Some((schedule_deadline, schedule_budget)), report)
                .await
                != Settlement::Settled
            {
                // The run is NOT interrupted: keep the owner intact for the
                // post-deadline join and do not flush ahead of it.
                quiesced = false;
            }
        } else {
            // Keep the probe at the actual unbounded join too: backup-schedule
            // tests call ServerHandle::shutdown(), which has no outer deadline.
            #[cfg(test)]
            fire_scheduled_join_probe();
            owner.join_schedule(None, report).await;
        }
        tracing::debug!(stage = "mutation_worker_join", "shutdown stage");
        if let Some(tracker) = &self.owned_mutation_tasks {
            tracker.close();
            let (mutation_deadline, _) = early_stage_bound(early_cutoff, REPO_FLUSH_BUDGET);
            if tokio::time::timeout_at(mutation_deadline, tracker.wait())
                .await
                .is_err()
            {
                report.add_error(
                    "owned mutation workers still running after the shutdown bound; repository flush skipped"
                        .to_string(),
                );
                quiesced = false;
            }
        }
        // 3b. R3-2b — wait for a still-draining online-export worker on EVERY
        //     shutdown path, before the flush decision below. The wait shares
        //     the early cutoff (capped by the export drain bound) instead of
        //     running its own fixed 60 s clock, so an outer deadline always
        //     bounds the whole teardown.
        let (export_deadline, export_budget) =
            early_stage_bound(early_cutoff, EXPORT_DRAIN_TIMEOUT);
        let export_idle = match self.online_export.as_ref() {
            Some(service) => tokio::time::timeout_at(export_deadline, service.wait_idle())
                .await
                .unwrap_or(false),
            None => true,
        };
        quiesced &= export_idle;
        if !export_idle {
            report.add_error(format!(
                "online export worker still draining after the shutdown bound {export_budget:?}"
            ));
        }
        // The deadline fallback keeps only the drain handle (taken above);
        // THIS service reference — with its `Arc<ShamirDb>` and user
        // directory — must not survive into the store-release probe, or an
        // idle export-enabled server would report `StillLocked` / exit 70.
        drop(self.online_export.take());
        // 3c. Stop every interactive-tx release SOURCE before the flush (ONE
        //     ordered stop, `release_source_stop`, shared with the launch
        //     rollback arms): join the reaper through the owner (a deadline
        //     expiry leaves it there for the owner drain), empty the registry
        //     so every lingering transaction's `Drop` release is REGISTERED
        //     while tail admission is still open, THEN close + drain the
        //     tails. The flush below can no longer overlap a release: a
        //     release run by an unstopped source after the flush was the
        //     reviewed weakness (the collector stopped at stage 7 while the
        //     repositories flushed at stage 4). An unsettled reaper or tail
        //     marks the shutdown unclean like every other quiescence failure.
        {
            let reaper_bound = if deadline_at.is_some() {
                Some(early_stage_bound(early_cutoff, CONN_DRAIN_TIMEOUT))
            } else {
                None
            };
            // A REAL report reserve: without it the drain can run right up to
            // the outer deadline, the outer expiry fires first, and the
            // drain's own expiry verdict is never recorded (only the generic
            // deadline message survives). 250 ms lets the drain record its
            // entry before the outer timeout.
            let (tail_deadline, tail_budget) =
                stage_bound(deadline_at, RELEASE_TAIL_DRAIN, Duration::from_millis(250));
            let release_stop = stop_release_sources_and_drain_tails(
                &mut owner.tx_reaper,
                Some(&self.tx_registry),
                self.tx_release_tails.as_ref(),
                ReleaseStopBounds {
                    reaper: reaper_bound,
                    tail_deadline,
                    tail_budget,
                },
                "before the repository flush",
                report,
            )
            .await;
            if let Settlement::Unsettled(stage) = release_stop {
                record_expired_owner_wait(stage, tail_budget, report);
            }
            quiesced &= release_stop == Settlement::Settled;
        }
        // 4. Flush all repo MemBuffers to durable backing so buffered
        //    commits in the last ~500 ms window are not lost on graceful
        //    stop. Must happen AFTER accept loops have stopped (no new
        //    writes can arrive) and BEFORE store locks are released.
        if !quiesced {
            report.add_error(
                "repository flush skipped: connection tasks remain, the scheduled backup owner is still running or replication followers may still apply"
                    .to_string(),
            );
            tracing::error!(
                live = self.conn_tasks.live(),
                "shutdown: repository flush skipped because shutdown ordering is not yet safe",
            );
        } else {
            // The probe belongs to the REAL flush call, not to this point: a
            // skipped flush (unclean connection drain) must not look like an
            // entered one to a shutdown test.
            #[cfg(test)]
            fire_repo_flush_probe();
            let (flush_deadline, flush_budget) = early_stage_bound(early_cutoff, REPO_FLUSH_BUDGET);
            if tokio::time::timeout_at(
                flush_deadline,
                self.shamir.flush_all_report(flush_budget, report),
            )
            .await
            .is_err()
            {
                report.add_error(format!(
                    "repository flush exceeded shutdown budget {flush_budget:?}"
                ));
            }
        }
        // 5. Stop the observability HTTP server (if any). BOUNDED and REPORTED:
        //    the listener may still be draining a scrape and the poller may be
        //    inside its database await, so an expiry must mark the report
        //    unclean instead of dropping the future and detaching both joins.
        //    Dropping the abandoned `obs` still cancels the token (the handle's
        //    private cancel-on-drop field), so the tasks stop on their own at
        //    their next cancellation point.
        if let Some(obs) = self.observability {
            let (obs_deadline, _) = early_stage_bound(early_cutoff, OBS_SHUTDOWN_TIMEOUT);
            match tokio::time::timeout_at(obs_deadline, obs.shutdown()).await {
                // The call returned: name exactly the side(s) that missed their
                // own join bound. A generic message here would blame a side
                // that did stop and hand the operator the wrong subsystem.
                Ok(outcome) => {
                    if !outcome.listener_joined {
                        tracing::warn!(
                            timeout = ?OBS_SHUTDOWN_TIMEOUT,
                            "shutdown: the observability metrics listener is still draining",
                        );
                        report.add_error(ERR_OBS_LISTENER_DRAINING.to_string());
                    }
                    if !outcome.poller_joined {
                        tracing::warn!(
                            timeout = ?OBS_SHUTDOWN_TIMEOUT,
                            "shutdown: the observability poller is still running",
                        );
                        report.add_error(ERR_OBS_POLLER_RUNNING.to_string());
                    }
                }
                Err(_) => {
                    tracing::warn!(
                        timeout = ?OBS_SHUTDOWN_TIMEOUT,
                        "shutdown: observability server did not finish stopping",
                    );
                    report.add_error(ERR_OBS_SHUTDOWN.to_string());
                }
            }
        }
        // 6. Join meta-snapshot task; its final write may wait on the blocking pool.
        //
        //    BOUNDED and REPORTED: that final write is a synchronous store
        //    write submitted to the blocking pool, so a starved pool keeps the
        //    join pending for as long as the pool is starved. An unbounded
        //    await here would let an outer expiry replace this stage's own
        //    verdict with the generic deadline message, so the wait spends
        //    only what is left of the shutdown budget, capped by
        //    META_SNAPSHOT_JOIN_TIMEOUT. With no outer deadline, the same cap
        //    applies without a reserve. Join errors stay ignored exactly as
        //    before: only an EXPIRY means the store may still be locked.
        if let Some(snap) = self.meta_snapshot_task {
            #[cfg(test)]
            crate::server::server_handle::fire_meta_snapshot_join_probe();
            let reserve_after = if deadline_at.is_some() {
                Duration::from_millis(500)
            } else {
                Duration::ZERO
            };
            let (bound_at, bound) =
                stage_bound(deadline_at, META_SNAPSHOT_JOIN_TIMEOUT, reserve_after);
            if tokio::time::timeout_at(bound_at, snap.handle)
                .await
                .is_err()
            {
                tracing::warn!(
                    ?bound,
                    "shutdown: the meta snapshot task did not stop in time",
                );
                report.add_error(format!(
                    "{ERR_META_SNAPSHOT_UNSETTLED} {bound:?}: the periodic metadata snapshot \
                     task has not dropped its `server_meta` store handle, so the `server_meta` \
                     store may still be locked and an immediate reboot on the same data \
                     directory can fail"
                ));
                record_expired_owner_wait(ShutdownWaitStage::MetaSnapshotJoin, bound, report);
                settlement =
                    settlement.worst(Settlement::Unsettled(ShutdownWaitStage::MetaSnapshotJoin));
            }
        }
        // 7. (moved to stage 3c) the interactive-tx reaper is a release-tail
        //     producer: it is joined, the registry emptied and the tails
        //     drained BEFORE the flush, via `release_source_stop`.
        // 8. Join cursor reaper so its Arc drops and pinned snapshots unpin.
        if let Some(reaper) = self.cursor_reaper {
            if deadline_at.is_some() {
                let (reaper_deadline, budget) = early_stage_bound(early_cutoff, CONN_DRAIN_TIMEOUT);
                if tokio::time::timeout_at(reaper_deadline, reaper.handle)
                    .await
                    .is_err()
                {
                    report.add_error(format!("cursor reaper join exceeded {budget:?}"));
                }
            } else {
                let _ = reaper.handle.await;
            }
        }
        // 8b. (moved to stage 3c) the registry clear and the closing tail
        //     drain are part of the ONE ordered release-source stop, run
        //     before the flush.
        // 9. Drop this handle's store references, then probe directory-lock release.
        let data_dir = self.data_dir;
        // The test seam holds `Arc<ShamirDbHandler>` (and the handler holds the
        // DB), so a fixture that never takes it would keep its OWN stores
        // locked through the barrier and turn a healthy shutdown into an
        // unclean report. It is test-only state: drop it with the handles.
        #[cfg(test)]
        drop(self.activity_test_seam);
        drop(self.shamir);
        drop(self.audit_appender);
        drop(self.repl_supervisor);
        drop(self.token_service);
        // The barrier's outcome belongs in the report, not only in a log line,
        // and the two failures stay distinct: a lock that is still HELD names
        // the store, while an unknown release state identifies whether the
        // blocking job missed its deadline, discovery was incomplete, or a
        // probe failed. This stage uses the one absolute budget, capped by
        // STORE_RELEASE_TIMEOUT and with report slack reserved before expiry.
        // Keep 250 ms before the absolute deadline for recording this verdict
        // and returning the report; do not let the release wait consume it all.
        let (release_deadline, release_budget) = match deadline_at {
            Some(outer) => stage_bound(
                Some(outer),
                STORE_RELEASE_TIMEOUT,
                Duration::from_millis(250),
            ),
            None => stage_bound(None, STORE_RELEASE_TIMEOUT, Duration::ZERO),
        };
        if release_budget.is_zero() {
            #[cfg(test)]
            fire_release_zero_budget_probe();
            tracing::warn!("shutdown: no budget remained to start the store-release probe");
            report_unsubmitted_release_probe(report, "no shutdown budget remained");
            // No probe result = no proof: the store-release stage is unsettled.
            settlement =
                settlement.worst(Settlement::Unsettled(ShutdownWaitStage::StoreReleaseProbe));
        } else {
            let outcome = release_stores_until(&data_dir, release_deadline).await;
            settlement = settlement.worst(super::shutdown_owner::report_store_release_outcome(
                outcome,
                release_budget,
                report,
            ));
        }
        // The report's unclean state already marks every quiescence failure
        // (listeners, connections, replication, audit, GC, export): record
        // the generic typed stage once so the settlement names it too.
        if !quiesced {
            settlement = settlement.worst(record_expired_owner_wait(
                ShutdownWaitStage::TeardownQuiescence,
                CONN_DRAIN_TIMEOUT,
                report,
            ));
        }
        settlement
    }

    /// Returns the first bound TCP+TLS-exporter address for connection tests.
    pub fn first_tls_exporter_addr(&self) -> Option<SocketAddr> {
        self.bound_addrs.iter().filter_map(|a| *a).next()
    }

    /// First bound local-IPC path; see [`Self::first_tls_exporter_addr`].
    pub fn first_ipc_path(&self) -> Option<&str> {
        self.bound_ipc_paths.iter().find_map(|p| p.as_deref())
    }

    /// Whether the experimental online storage-migration API
    /// (`StartMigration`/`CommitMigration`/`RollbackMigration`/`MigrationStatus`)
    /// is enabled on this server. `true` only when
    /// `security.enable_experimental_migration_api` was set at boot (default `false`).
    /// Primarily exposed to test that `server_launcher/launch.rs` wired the toggle
    /// read by every `StartMigration` handler.
    pub fn experimental_migration_enabled(&self) -> bool {
        self.shamir.experimental_migration_enabled()
    }
}

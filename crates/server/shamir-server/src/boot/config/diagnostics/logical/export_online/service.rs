//! The online logical-export job runner (`OnlineExport`, R3 step 2b,
//! brief `56-r3-2b-online-export-job.md`).
//!
//! Thin orchestration over the already-landed pieces: the session-bound
//! [`ExportRegistry`] (admission + status/cancel state), the validated
//! [`ExportRoot`] (private directory + exclusive lock + stale cleanup),
//! the scope module (`authorize_scope` / `warm_scope`), the server-wide
//! [`MaintenanceGate`], and the shared artifact writer
//! (`logical_dump::write_live_export` with live staging and the
//! [`publish_live_stage`] no-clobber commit point, which owns the typed
//! post-rename durability behavior — the service never infers a
//! publication outcome from the filesystem).
//!
//! Fail-closed posture: the launcher enables the runner only on ReadWrite
//! nodes without experimental migration, where logical mutation sources
//! are enrolled in the shared gate (design §0.3). The service never
//! opens a network endpoint.
//!
//! Execution model (mirrors the backup registry precedent): `start`
//! runs on the server runtime and performs every cancellable async step
//! (scope authorization, mutation admission, warm opens, registry
//! reservation) inline, so a dropped `start` future leaves no guard and
//! no job behind. The freeze + artifact production then run on a
//! dedicated blocking worker (`runtime.spawn_blocking`) that bridges
//! the synchronous writer I/O through the captured runtime
//! `Handle::block_on`; `start` returns the [`ExportStartReceipt`] right
//! after the worker spawn succeeded, never after a terminal result.
//!
//! Task ownership: the blocking worker's tokio [`JoinHandle`] is owned
//! by a per-job async supervisor task that selects (biased) on run
//! cancellation, connection close, session-store liveness and worker
//! completion. Cancellation signals the run token, after
//! which the supervisor waits for the worker with a bounded timeout.
//! The worker itself selects (biased) between its cancel token and the
//! deadline-bounded freeze future, so expiry during the DRAIN (before
//! the writer even starts) unwinds the freeze future promptly — the freeze guard and
//! every snapshot live only inside that future and are released by its
//! drop. A bounded join wait can expire while a worker is still in
//! synchronous I/O; the worker retains its slot and run ticket until it
//! exits. The ticket's `Drop` records any unrecorded exit as
//! `Failed`/`Cancelled`. Normal terminal results are recorded only after
//! the freeze and its pins have been released.
//!
//! Clocks: one absolute writer deadline bounds the drain and export,
//! leaving a 10 s margin under the engine's 600 s ceiling. The
//! synchronous codec/file boundaries are covered by the writer's `CheckedIo` token/deadline checks — a blocking-pool thread
//! cannot be aborted, so no hard cutoff is advertised across a stuck
//! synchronous syscall; shutdown waits only a bounded drain.
//!
//! Wire hygiene: errors are static typed variants with no path data, no
//! secrets and no peer text; artifact and export ids are
//! `timestamp + process-wide sequence` (one shared atomic counter), so
//! no user-controlled string ever names a file or appears in status.

use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Result};
use shamir_connect::common::types::limits;
use shamir_connect::server::{Session, SessionStore};
use shamir_db::engine::db_instance::logical_export::MaintenanceGate;
use shamir_db::engine::table::writer_drain_barrier::WriterDrainGuard;
use shamir_db::{DbError, ShamirDb};
use shamir_query_types::admin::{
    ExportCancelReceipt, ExportFailure, ExportPhase, ExportStartReceipt, ExportStatusReceipt,
    StartExportOp,
};
use shamir_server_security::user_directory::FjallUserDirectory;
use tokio::runtime::Handle;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use super::registry::{AlreadyRunning, ExportRegistry, ExportRun};
use super::root::ExportRoot;
use super::scope::{authorize_scope, warm_scope, ScopeError};
use crate::boot::config::diagnostics::logical::logical_dump::{
    classify_export_failure, publish_live_stage, write_live_export, ExportObserver, ExportProgress,
    LiveExport, LiveExportFailure, LogicalExportArgs, SecuritySource,
};
use crate::db_handler::ExportJobActivityGuard;

/// Process-wide sequence for export ids and artifact names (brief: names
/// are `timestamp + process-wide sequence`; no user-supplied component).
static NAME_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Millis since the Unix epoch; the sequence disambiguates
/// same-millisecond names inside one process.
fn unix_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

/// Opaque export id.
fn fresh_export_id() -> String {
    let seq = NAME_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    format!("ex-{}-{seq:06}", unix_millis())
}

/// Opaque artifact name (shares the process-wide counter; never a
/// filesystem path on the wire).
fn fresh_artifact_name() -> String {
    let seq = NAME_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    format!("export-{}-{seq:06}", unix_millis())
}

/// Writer deadline inside the freeze: leaves margin under the engine's
/// 600-second ceiling (same margin as the offline path's `CheckedIo`).
const WRITER_BUDGET: Duration = Duration::from_secs(590);
const MAX_LIVE_DATABASES: usize = 65_536;
const OWNER_CHECK_INTERVAL: Duration = Duration::from_secs(1);

/// Per-service controls; never installed on a production runner.
#[cfg(test)]
#[derive(Clone, Default)]
pub(super) struct WorkerControls {
    pub budget: Option<Duration>,
    pub drain_pending: Option<Arc<tokio::sync::Notify>>,
    pub verifying: Option<Arc<VerificationHook>>,
}

#[cfg(test)]
pub(super) type VerificationHook = dyn Fn(&CancellationToken) + Send + Sync;

/// Bounded wait for the blocking worker after a shutdown/cancel signal:
/// the writer checks its token on every codec I/O boundary, so the
/// unwind is prompt; the bound only caps pathological stuck filesystem
/// I/O (a blocking-pool thread cannot be aborted) and keeps server
/// shutdown bounded. Also bounds [`OnlineExport::wait_idle`].
const THREAD_DRAIN_WAIT: Duration = Duration::from_secs(60);

/// Typed refusal/failure of [`OnlineExport::start`]. Static variants
/// only: no filesystem paths, no peer text, no catalogue strings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ServiceError {
    /// The runner is disabled for this server configuration.
    #[error("the online export job is disabled on this server")]
    Disabled,
    /// The engine or runtime is shutting down; no job was admitted (or
    /// the worker could not be spawned; the reserved job is recorded
    /// failed by the run ticket's completion guard).
    #[error("the server is shutting down")]
    Shutdown,
    /// A job is already admitted on this server (any owner). This is
    /// also the typed answer when the maintenance gate refuses the
    /// warm-up admission: only the online export ever RAISES freeze
    /// intent (the online backup takes mutation admission, it never
    /// freezes), so freeze intent observed here means an export job is
    /// already running server-wide — the same in-progress code is
    /// returned to every caller, which also avoids leaking whether a
    /// job runs to a non-owner (registry precedent).
    #[error("an online export job is already running on this server")]
    AlreadyRunning,
    /// Current directory state is not a superuser, or an included scope
    /// failed its read/manage ACL check.
    #[error("the export scope was denied")]
    Denied,
    /// The requested scope is malformed or unresolved.
    #[error("the requested export scope is invalid")]
    InvalidScope,
    /// The scope or options carry content this phase cannot represent.
    #[error("the requested export scope is not supported")]
    Unsupported,
    /// The scope is temporarily unexportable (quarantine, rename in
    /// doubt) or a catalogue/directory read failed during warm-up.
    #[error("the export scope is temporarily unavailable")]
    Unavailable,
}

impl From<ScopeError> for ServiceError {
    fn from(scope: ScopeError) -> Self {
        match scope {
            ScopeError::Denied => Self::Denied,
            ScopeError::InvalidScope => Self::InvalidScope,
            ScopeError::Unsupported => Self::Unsupported,
            ScopeError::Unavailable => Self::Unavailable,
        }
    }
}

impl From<AlreadyRunning> for ServiceError {
    fn from(_: AlreadyRunning) -> Self {
        Self::AlreadyRunning
    }
}

/// anyhow marker for a scope refusal under the freeze. The terminal
/// mapping distinguishes unsupported content from other scope refusals.
#[derive(Debug, thiserror::Error)]
#[error("export scope refusal under freeze")]
struct ScopeRefusal(ScopeError);

/// Runtime wiring for [`OnlineExport::new`]: the handle the blocking
/// worker and supervisor run on, and the server shutdown token (each
/// job's run token is a child of it, so a shutdown cancels every job).
pub(crate) struct ExportRuntime {
    pub runtime: Handle,
    pub shutdown: CancellationToken,
    /// Optional authoritative store; checked without refreshing activity.
    pub sessions: Option<Arc<SessionStore>>,
}

/// The handle identifies the logical session, including same-SID rebinds
/// that preserve it. Neither component is logged.
#[derive(Clone)]
struct ExportOwner {
    sessions: Option<Arc<SessionStore>>,
    sid: [u8; limits::SESSION_ID_BYTES],
    handle: u64,
}

impl ExportOwner {
    fn is_live(&self) -> bool {
        self.sessions
            .as_ref()
            .is_none_or(|store| store.matches_activity_handle(&self.sid, self.handle))
    }

    /// cancel-safe: yes — checks do not touch TTL or mutate the store.
    async fn wait_for_eviction(&self) {
        while self.is_live() {
            tokio::time::sleep(OWNER_CHECK_INTERVAL).await;
        }
    }
}

/// The online logical-export job service.
///
/// Wraps the registry, the export root, the shared maintenance gate, the
/// engine handles and the server shutdown token. One instance per
/// server; every method is safe for concurrent sessions.
pub struct OnlineExport {
    registry: Arc<ExportRegistry>,
    root: Arc<ExportRoot>,
    gate: MaintenanceGate,
    db: Arc<ShamirDb>,
    directory: Arc<FjallUserDirectory>,
    /// Source storage epoch carried into the dump header (stamp-derived;
    /// the caller supplies it at construction).
    epoch: Option<u32>,
    runtime: ExportRuntime,
    /// False until all logical mutation sources are enrolled in the gate.
    enabled: bool,
    #[cfg(test)]
    controls: WorkerControls,
    /// Shutdown-quiescence slot: ONE permit, held by the in-flight
    /// blocking worker for the whole run (an owned permit moved into the
    /// job). [`OnlineExport::wait_idle`] acquires it under a bounded
    /// timeout; a start refuses with [`ServiceError::AlreadyRunning`]
    /// while a worker still drains, and the registry's own single
    /// admission slot stays the primary arbitration.
    worker_slot: Arc<Semaphore>,
    /// Bounded preparation slot: ONE permit claimed BEFORE authorization
    /// and warm-up and held until the registry job is reserved, so a
    /// claimant arriving DURING another caller's preparation is refused
    /// with [`ServiceError::AlreadyRunning`] instead of duplicating the
    /// whole authorize/warm/admission sequence and losing only at the
    /// late registry arbitration. The permit is bounded to preparation:
    /// it never spans the spawned worker (that is `worker_slot`'s job)
    /// and drops with the failing `start` future on every refusal path.
    prepare_slot: Arc<Semaphore>,
    /// Optional activity-view projection (task #2021 item 6b): when the
    /// attaching handler installed one, each admitted job publishes a
    /// `Requests` row through the shared in-flight table, removed when
    /// the job's worker exits. `None` until attached (tests, minimal
    /// boots) — the job then simply has no activity row.
    activity: std::sync::OnceLock<Arc<crate::db_handler::ExportActivityProjection>>,
}

/// Shutdown fallback ownership of the export drain: ONLY the two quiescence
/// slots (worker + preparation), deliberately NOT the [`OnlineExport`]
/// service. The service owns `Arc<ShamirDb>` and the durable user directory;
/// a shutdown owner that clones the whole service would hold those store
/// references across the store-release probe and falsely report every store
/// `StillLocked` on an IDLE export-enabled server. `ShutdownOwner` therefore
/// keeps only the ability to wait for a draining worker — the same bounded
/// both-slot acquire [`OnlineExport::wait_idle`] performs.
pub(crate) struct ExportDrainHandle {
    worker_slot: Arc<Semaphore>,
    prepare_slot: Arc<Semaphore>,
}

impl ExportDrainHandle {
    /// Same contract as [`OnlineExport::wait_idle`] (same slots, same bound):
    /// `true` when no blocking export worker still holds the single worker
    /// slot AND no `start` still holds the bounded preparation slot, `false`
    /// when the bound elapsed with either slot still held.
    pub(crate) async fn wait_idle(&self) -> bool {
        let drain = async {
            let prepare = self.prepare_slot.acquire().await;
            let worker = self.worker_slot.acquire().await;
            (prepare, worker)
        };
        matches!(
            tokio::time::timeout(THREAD_DRAIN_WAIT, drain).await,
            Ok((Ok(_prepare), Ok(_worker)))
        )
    }
}

impl OnlineExport {
    /// Build the fail-closed service with the runner disabled.
    pub fn new(
        registry: Arc<ExportRegistry>,
        root: Arc<ExportRoot>,
        gate: MaintenanceGate,
        db: Arc<ShamirDb>,
        directory: Arc<FjallUserDirectory>,
        epoch: Option<u32>,
        runtime: ExportRuntime,
    ) -> Self {
        Self {
            registry,
            root,
            gate,
            db,
            directory,
            epoch,
            runtime,
            enabled: false,
            #[cfg(test)]
            controls: WorkerControls::default(),
            worker_slot: Arc::new(Semaphore::new(1)),
            prepare_slot: Arc::new(Semaphore::new(1)),
            activity: std::sync::OnceLock::new(),
        }
    }

    /// Install the activity-view projection (called once by the attaching
    /// handler; a second call is a no-op, keeping the first projection).
    pub(crate) fn set_activity_projection(
        &self,
        projection: Arc<crate::db_handler::ExportActivityProjection>,
    ) {
        let _ = self.activity.set(projection);
    }

    /// Enable the runner before sharing it, only when the shared gate
    /// covers the server's logical mutation sources.
    pub(crate) fn enable(mut self) -> Self {
        self.enabled = true;
        self
    }

    #[cfg(test)]
    pub(super) fn enable_for_test(self, controls: WorkerControls) -> Self {
        let mut service = self.enable();
        service.controls = controls;
        service
    }

    /// The drain handle shutdown's fallback owner retains INSTEAD of the
    /// service: lossless for the wait, and it drops the owner's database and
    /// user-directory references so the store-release probe can observe a
    /// released data directory on an idle export-enabled server.
    pub(crate) fn worker_drain(&self) -> ExportDrainHandle {
        ExportDrainHandle {
            worker_slot: Arc::clone(&self.worker_slot),
            prepare_slot: Arc::clone(&self.prepare_slot),
        }
    }

    /// Shutdown quiescence: `true` when no blocking export worker still
    /// holds the single worker slot AND no `start` still holds the
    /// bounded preparation slot. The caller must have cancelled the
    /// shutdown token (or otherwise stopped admitting new jobs) first —
    /// `start` refuses while the token is cancelled, so a permit acquired
    /// here cannot be handed to a new worker. Preparation is part of the
    /// shutdown protocol: the preparation slot (claimed before
    /// authorization, held through the registry reservation) is covered
    /// by the SAME wait, so a start mid-preparation keeps the shutdown
    /// from declaring the export surface quiescent. Order (prepare →
    /// worker) matches `start`'s acquisition order, and `start` claims
    /// both slots with `try_acquire_owned`, so neither side can deadlock.
    /// Bounded: the writer checks its token on every codec I/O boundary,
    /// but a blocking-pool thread cannot be aborted, so a stuck
    /// synchronous syscall can only be waited out, not killed; `false`
    /// means the bound elapsed with a slot still held. The acquired
    /// permits are released on return (no state mutated).
    pub(crate) async fn wait_idle(&self) -> bool {
        let drain = async {
            let prepare = self.prepare_slot.acquire().await;
            let worker = self.worker_slot.acquire().await;
            (prepare, worker)
        };
        matches!(
            tokio::time::timeout(THREAD_DRAIN_WAIT, drain).await,
            Ok((Ok(_prepare), Ok(_worker)))
        )
    }

    /// Start an export job for `session`, targeting the scope in
    /// `options`.
    ///
    /// Ordering (design §4.1): claim the bounded preparation slot →
    /// validate + authorize against the CURRENT state → warm opens and
    /// the bounded catalogue warm-up under ONE mutation admission → claim
    /// the worker slot → reserve the registry job → publish the activity
    /// row → spawn the blocking worker. The preparation slot is part of
    /// the shutdown protocol: [`Self::wait_idle`] waits for BOTH slots,
    /// so shutdown cannot pass the export stage while a start is still
    /// preparing. A denial, an invalid scope, a busy gate or a lost
    /// admission race refuses with a typed error and never starts the
    /// freeze.
    ///
    /// `connection_closed` is the session's connection token: when the
    /// session's transport dies, the supervisor cancels the job (the
    /// gate is released and the stage removed, like any other cancel).
    pub async fn start(
        &self,
        session: &Session,
        options: StartExportOp,
        connection_closed: CancellationToken,
    ) -> Result<ExportStartReceipt, ServiceError> {
        if !self.enabled {
            return Err(ServiceError::Disabled);
        }
        if self.runtime.shutdown.is_cancelled() {
            return Err(ServiceError::Shutdown);
        }
        // ── claim the bounded preparation slot BEFORE authorization, so
        // late claimants are refused for the whole (bounded) preparation
        // instead of racing to the late registry arbitration. Held until
        // the registry job is reserved below; dropped with this future on
        // every refusal path.
        let _preparing = Arc::clone(&self.prepare_slot)
            .try_acquire_owned()
            .map_err(|_| ServiceError::AlreadyRunning)?;
        // Recheck under the preparation permit, before the first I/O: a drain
        // that already took and released both slots began after the token was
        // cancelled, so a start that acquired the permit late sees it here
        // and never touches the catalogue after shutdown declared export idle.
        if self.runtime.shutdown.is_cancelled() {
            return Err(ServiceError::Shutdown);
        }

        let user_id = session.user_id;
        let owner = ExportOwner {
            sessions: self.runtime.sessions.clone(),
            sid: session.session_id,
            handle: session.activity_handle(),
        };

        // ── authorize against the CURRENT directory/ACL state. Every
        // guard below is a plain local of this future, so a dropped or
        // cancelled `start` leaves nothing behind.
        let db_instance = authorize_scope(&self.db, &self.directory, user_id, &options)
            .await
            .map_err(ServiceError::from)?;

        // ── warm phase under ONE mutation admission: cold table opens
        // (store opens, index loads, possible repair) and the cold
        // function-folder catalogue handles must never happen inside the
        // freeze. The function-folder family is warmed with a bounded
        // point query (`load_function_folder` with a fixed empty path
        // key — at most one indexed lookup, no full-catalogue allocation
        // and no live `check_seed_catalogue` flush); the other catalogue
        // families and settings are warmed by boot and the ACL checks
        // above.
        let admission: WriterDrainGuard = self
            .gate
            .enter_mutation()
            // Freeze intent raised ⇒ an export job is running (see the
            // `AlreadyRunning` docs): the same in-progress answer.
            .map_err(|_| ServiceError::AlreadyRunning)?;
        warm_scope(&db_instance, &options)
            .await
            .map_err(ServiceError::from)?;
        self.db
            .system_store()
            .load_function_folder("")
            .await
            .map_err(|_| ServiceError::Unavailable)?;
        drop(admission);

        // ── claim the shutdown-quiescence slot BEFORE the registry
        // reservation, so the permit is held from before the spawn through
        // the worker's whole run. A permit still held by the previous
        // job's draining worker refuses with the same in-progress answer
        // as a busy registry (see `AlreadyRunning`). `try_acquire_owned`
        // cannot block and the permit cannot be dropped on any refusal
        // path below before the job takes ownership.
        let worker_slot = Arc::clone(&self.worker_slot)
            .try_acquire_owned()
            .map_err(|_| ServiceError::AlreadyRunning)?;

        // Recheck after warm-up and under the worker permit so wait_idle
        // cannot return before a late start admits a worker after shutdown.
        if self.runtime.shutdown.is_cancelled() {
            return Err(ServiceError::Shutdown);
        }

        // ── reserve the one registry job. From here on the worker owns
        // the outcome; a panic between reservation and the terminal
        // record is caught by the run ticket's Drop backstop (the permit
        // releases with the job).
        let cancel = self.runtime.shutdown.child_token();
        let export_id = fresh_export_id();
        let run = Arc::new(self.registry.start(
            owner.handle,
            &options.database,
            export_id.clone(),
            cancel.clone(),
        )?);
        // Preparation is over: the registry arbitration owns admission
        // from here, so a late claimant may retry freely.
        drop(_preparing);
        // Activity projection (item 6b): publish the job's row after the
        // reservation (a refused start never flashes a row). The guard is
        // moved into the job, so the row dies exactly when the worker
        // exits — including panics (the guard's Drop).
        let activity_guard = self
            .activity
            .get()
            .and_then(|projection| projection.register_job(session, options.database.clone()));

        // ── spawn the dedicated blocking worker (synchronous codec/file
        // work bridged through the captured runtime handle). The job
        // holds `Arc<ExportRoot>`, so the root's exclusive lock lives
        // for the whole run even if the service is dropped.
        let job = ExportJob {
            gate: self.gate.clone(),
            db: Arc::clone(&self.db),
            directory: Arc::clone(&self.directory),
            root: Arc::clone(&self.root),
            options,
            epoch: self.epoch,
            user_id,
            owner: owner.clone(),
            connection_closed: connection_closed.clone(),
            run,
            #[cfg(test)]
            controls: self.controls.clone(),
            cancel: cancel.clone(),
            runtime: self.runtime.runtime.clone(),
            artifact: fresh_artifact_name(),
            publication_claimed: Arc::new(AtomicBool::new(false)),
            _activity_row: activity_guard,
            _worker_slot: worker_slot,
        };
        // `Handle::spawn_blocking` is infallible: it always returns a
        // `JoinHandle` (shutdown only prevents NEW polls of this runtime,
        // the task is still spawned and will unwind on its cancelled
        // token).
        let mut join = self.runtime.runtime.spawn_blocking(move || job.run());

        // ── supervisor: owns the worker's JoinHandle. Shutdown is
        // already linked through the run token; connection close and
        // eviction also signal it. No extra task for liveness polling.
        // fire-and-forget: detached by design. The worker records its
        // own terminal outcome even if the runtime drops the supervisor.
        self.runtime.runtime.spawn(async move {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => {},
                _ = connection_closed.cancelled() => cancel.cancel(),
                _ = owner.wait_for_eviction(), if owner.sessions.is_some() => cancel.cancel(),
                // Poll by reference: the join is awaited again below, so
                // the select must not consume it on the cancel arm.
                _ = &mut join => return,
            }
            // Bounded drain: the worker unwinds on its token; never wait
            // unboundedly on a stuck filesystem (a blocking-pool thread
            // cannot be aborted, only waited out).
            let _ = tokio::time::timeout(THREAD_DRAIN_WAIT, join).await;
        });

        Ok(ExportStartReceipt { export_id })
    }

    /// Synchronous status of `session`'s job against the database the
    /// launcher resolved (the wire ops carry no database field; the
    /// launcher passes the session's current database): `None` when no
    /// visible record belongs to this session/database (never another
    /// owner's job). No database or filesystem access.
    pub fn status_for(&self, session: &Session, database: &str) -> Option<ExportStatusReceipt> {
        self.registry.status(session.activity_handle(), database)
    }

    /// Synchronous cancellation of `session`'s job against `database`:
    /// delivers the cancel signal onto the run token. The worker unwinds
    /// the freeze; the supervisor waits only up to its drain bound.
    /// `None` when no visible job of this session exists.
    pub fn cancel_for(&self, session: &Session, database: &str) -> Option<ExportCancelReceipt> {
        self.registry.cancel(session.activity_handle(), database)
    }
}

/// Terminal outcome of one freeze, decided structurally (token, gate
/// result) — never by matching error text.
enum FreezeOutcome {
    /// The freeze returned; the writer reported its own result (`Err`
    /// carries the driver's typed phase/projection markers, or a scope
    /// refusal, or a gate-level error reboxed). Success carries only the
    /// verified section, item and byte counts.
    Completed(Result<(u64, u64, u64), anyhow::Error>),
    /// The run token fired (per-session cancel, connection close, or
    /// server shutdown) before the freeze returned.
    Cancelled,
    /// The outer deadline elapsed before the freeze future completed.
    TimedOut,
}

/// One admitted job's runner: owns the run ticket (through the shared
/// `Arc`) and drives freeze → resolve → drain and pin → reauthorize →
/// write → publish. `WaitingForGate` covers all preparation through the
/// final authorization; `Exporting` begins just before the writer runs.
/// Capture holds repo write domains, so warm reads can proceed but cold
/// reads that need table initialization may wait.
/// Fields are private; `start` constructs the literal in the same
/// module, so there is no many-argument constructor.
struct ExportJob {
    gate: MaintenanceGate,
    db: Arc<ShamirDb>,
    directory: Arc<FjallUserDirectory>,
    /// Held for the whole run: the root's exclusive lock must outlive
    /// the job even if the service itself is dropped.
    root: Arc<ExportRoot>,
    options: StartExportOp,
    epoch: Option<u32>,
    user_id: [u8; 16],
    owner: ExportOwner,
    connection_closed: CancellationToken,
    run: Arc<ExportRun>,
    #[cfg(test)]
    controls: WorkerControls,
    cancel: CancellationToken,
    runtime: Handle,
    /// Opaque artifact name, minted once at reservation time.
    artifact: String,
    /// `true` once the publication window was claimed inside the
    /// writer's `on_publish` callback. After the claim, ANY error —
    /// even one arriving while an external cancellation has fired — is
    /// recorded as a typed failure, never as `Cancelled` (a cancel is a
    /// no-op in the publishing window, so recording it there would
    /// strand the job on the ticket's Drop backstop instead of giving
    /// status a real reason).
    publication_claimed: Arc<AtomicBool>,
    /// Activity-view row guard (item 6b): its Drop removes the job's
    /// projected `Requests` row when the worker exits, whatever the exit
    /// path. `None` when no projection is attached (or the id space was
    /// exhausted). Declared BEFORE `_worker_slot` on purpose: struct
    /// fields drop in declaration order, so the row is removed first and
    /// `wait_idle` cannot return while the row is still visible.
    _activity_row: Option<ExportJobActivityGuard>,
    /// Shutdown-quiescence permit: held for the whole worker run so
    /// [`OnlineExport::wait_idle`] can observe quiescence; released when
    /// the closure returns (or unwinds) — AFTER the activity row above
    /// has been removed.
    _worker_slot: tokio::sync::OwnedSemaphorePermit,
}

impl ExportJob {
    /// Blocking-worker entry point. Blocks on the runtime handle for the
    /// async facade; the freeze guard and every snapshot live only
    /// inside the (cancellable) freeze future, so a cancelled or
    /// panicked job releases the gate through unwinding. The run
    /// ticket's Drop (fired when this worker's locals unwind) is the
    /// last-resort completion guard.
    fn run(self) {
        // The writer deadline the worker hands to `CheckedIo`: the
        // standard margin under the engine ceiling, so the synchronous
        // codec checkpoints honor the same clock the select does.
        #[cfg(not(test))]
        let budget = WRITER_BUDGET;
        #[cfg(test)]
        let budget = self
            .controls
            .budget
            .unwrap_or(WRITER_BUDGET)
            .min(WRITER_BUDGET);
        let (outcome, writer_deadline) = self.runtime.block_on(async {
            // Sample inside the runtime; the test runtime can pause its clock.
            let deadline = tokio::time::Instant::now() + budget;
            let writer_deadline = deadline.into_std();
            (self.drive(deadline).await, writer_deadline)
        });
        self.record_outcome(outcome, writer_deadline);
    }

    /// Bound both DRAIN and export with the same deadline used by the writer.
    /// Dropping the pending freeze drops its guard, pins and stage before
    /// recording a terminal result. Cancellation wins when both are ready.
    async fn drive(&self, deadline: tokio::time::Instant) -> FreezeOutcome {
        let freeze = self.freeze(deadline.into_std());
        tokio::select! {
            biased;
            _ = self.cancel.cancelled() => FreezeOutcome::Cancelled,
            result = tokio::time::timeout_at(deadline, freeze) => {
                result.unwrap_or(FreezeOutcome::TimedOut)
            }
        }
    }

    /// The freeze itself. The callback always returns `Ok(report)` in
    /// the outer `DbResult` (the writer's own `Err` stays boxed
    /// inside), so a gate-level `DbError` can only mean drain failure
    /// or the engine ceiling — mapped structurally below.
    /// cancel-safe: yes — the pending future owns its RAII freeze, pins and stage;
    /// the synchronous publication callback never yields after the commit.
    async fn freeze(&self, writer_deadline: Instant) -> FreezeOutcome {
        let args = LogicalExportArgs {
            db: self.options.database.clone(),
            output: self.root.path().join(&self.artifact),
            repo: self.options.repo.clone(),
            table: self.options.table.clone(),
            schema_only: self.options.schema_only,
            include_artifacts: false,
        };
        // Publication claim + no-clobber commit. `publish_live_stage`
        // owns the typed post-commit behavior: after the no-replace rename has
        // committed it reports success (the durability tail is a
        // published, retained artifact, never a failure), so the
        // service never infers an outcome from the filesystem.
        let publish_run = Arc::clone(&self.run);
        let claimed = Arc::clone(&self.publication_claimed);
        let output = self.root.path().join(&self.artifact);
        let connection_closed = self.connection_closed.clone();
        let owner = self.owner.clone();
        let cancel = self.cancel.clone();
        let on_publish = move |stage: &Path| -> Result<()> {
            publish_live_stage(stage, &output, || {
                // Pre-rename sync has finished. Recheck the owner and
                // transport here rather than relying on the supervisor's
                // next poll. Claim only after those checks.
                if connection_closed.is_cancelled() || !owner.is_live() {
                    cancel.cancel();
                }
                if !publish_run.try_begin_publication(Instant::now() >= writer_deadline) {
                    bail!("export publication lost cancellation arbitration");
                }
                claimed.store(true, Ordering::SeqCst);
                Ok(())
            })
        };
        #[cfg(test)]
        let verifying = self.controls.verifying.clone();
        let freeze = self.gate.with_freeze(|guard| {
            // Captured exactly once, moved into the freeze future;
            // no re-derivation from `self` inside.
            let db = Arc::clone(&self.db);
            let directory = Arc::clone(&self.directory);
            let options = self.options.clone();
            let user_id = self.user_id;
            let epoch = self.epoch;
            let run = Arc::clone(&self.run);
            let cancel = self.cancel.clone();
            Box::pin(async move {
                // Resolve and fail fast after the server gate drains its
                // counted writers. A cancelled writer may still have a
                // repository commit tail, so this check is not decisive.
                let db_instance = match authorize_scope(&db, &directory, user_id, &options).await {
                    Ok(db) => db,
                    Err(scope) => return Ok(Err(anyhow::Error::new(ScopeRefusal(scope)))),
                };
                if db.db_count() > MAX_LIVE_DATABASES {
                    return Ok(Err(anyhow::Error::new(ScopeRefusal(
                        ScopeError::Unsupported,
                    ))));
                }
                let live = match db
                    .list_dbs()
                    .into_iter()
                    .filter(|name| name != "__system__")
                    .map(|name| db.get_db(&name).ok_or(DbError::NotFound(name)))
                    .collect::<Result<Vec<_>, _>>()
                {
                    Ok(live) => live,
                    Err(error) => return Ok(Err(anyhow::Error::new(error))),
                };
                let reader = match db
                    .system_store()
                    .capture_online_logical_export(&db_instance, &live, guard)
                    .await
                {
                    Ok(reader) => reader,
                    Err(error) => return Ok(Err(anyhow::Error::new(error))),
                };
                // Capture drained repository commit tails, including the
                // system ACL tail. Re-read authority before staging anything,
                // while the reader still holds the pins and write domains.
                // Capture verifies the settings, groups and DB/repo/table
                // catalogues are initialized: SystemStore init warms groups
                // via the group-name rebuild and earlier ACL checks warm
                // the catalogues they use. The warm path reads rows without
                // re-entering a write domain.
                if let Err(scope) = authorize_scope(&db, &directory, user_id, &options).await {
                    return Ok(Err(anyhow::Error::new(ScopeRefusal(scope))));
                }
                let observer = RunObserver {
                    run: Arc::clone(&run),
                    sections: AtomicU64::new(0),
                    items: AtomicU64::new(0),
                    bytes: AtomicU64::new(0),
                    #[cfg(test)]
                    verifying: verifying.map(|hook| (hook, cancel.clone())),
                };
                run.set_phase(ExportPhase::Exporting);
                let report = write_live_export(LiveExport {
                    reader: &reader,
                    security: SecuritySource::LiveDirectory(Some(Arc::clone(&directory))),
                    // The live job reuses the server's directory
                    // handle above; no offline home is involved.
                    offline_home: None,
                    args: &args,
                    epoch,
                    live: true,
                    cancel: &cancel,
                    deadline: writer_deadline,
                    observer: &observer,
                    on_publish: Some(&on_publish),
                })
                .await;
                drop(reader);
                Ok(report)
            })
        });
        #[cfg(test)]
        let freeze = async {
            let mut freeze = std::pin::pin!(freeze);
            // The fixture keeps a previously admitted writer alive. The
            // first pending poll therefore proves the gate is draining.
            if let Some(arrived) = &self.controls.drain_pending {
                match futures_util::poll!(freeze.as_mut()) {
                    std::task::Poll::Ready(result) => return result,
                    std::task::Poll::Pending => arrived.notify_one(),
                }
            }
            freeze.await
        };
        let result = freeze.await;
        match result {
            Ok(report) => FreezeOutcome::Completed(
                report.map(|report| (report.sections, report.items, report.bytes)),
            ),
            Err(gate_error) => {
                // Gate-level only: drain refusal or the engine's 600 s
                // ceiling. Cancellation dominates; everything else is
                // reboxed and classified structurally (the classifier's
                // clock marks an elapsed ceiling as a timeout, so a real
                // timeout stays distinct from a cancel).
                if self.cancel.is_cancelled() {
                    FreezeOutcome::Cancelled
                } else {
                    FreezeOutcome::Completed(Err(anyhow::Error::new(gate_error)))
                }
            }
        }
    }

    /// Map the terminal outcome onto the registry exactly once. The
    /// ticket's Drop is the backstop for a panic between spawn and here.
    fn record_outcome(&self, outcome: FreezeOutcome, writer_deadline: Instant) {
        match outcome {
            FreezeOutcome::Completed(Ok((sections, items, bytes))) => {
                // The writer's verified report carries the real section,
                // item and byte totals; record them before `Done` so a
                // status polled at the terminal transition never shows
                // stale counters.
                self.run.note_progress(sections, items, bytes);
                self.run.finish_published(self.artifact.clone());
            }
            FreezeOutcome::Completed(Err(error)) => self.record_failure(error, writer_deadline),
            FreezeOutcome::Cancelled => self.run.finish_cancelled(),
            FreezeOutcome::TimedOut => self.run.finish_failed(ExportFailure::TimedOut),
        }
    }

    /// Record a freeze-internal failure with its typed kind.
    fn record_failure(&self, error: anyhow::Error, writer_deadline: Instant) {
        // A scope refusal under the freeze keeps its typed kind; the
        // closed wire set has no `Denied` variant, so only the
        // never-representable-content case stays `UnsupportedContent`.
        if let Some(ScopeRefusal(scope)) = error.downcast_ref::<ScopeRefusal>() {
            let failure = match scope {
                ScopeError::Unsupported => ExportFailure::UnsupportedContent,
                _ => ExportFailure::Internal,
            };
            self.run.finish_failed(failure);
            return;
        }
        let claimed = self.publication_claimed.load(Ordering::SeqCst);
        if claimed {
            // Inside/after the claimed publication window a cancel is a
            // no-op in the registry — record the real typed failure
            // instead, whatever the token says now.
            let failure = match classify_export_failure(error, &self.cancel, writer_deadline) {
                LiveExportFailure::Cancelled | LiveExportFailure::WriterIo => ExportFailure::Io,
                LiveExportFailure::DeadlineExceeded => ExportFailure::TimedOut,
                LiveExportFailure::StageIo => ExportFailure::Io,
                LiveExportFailure::VerifierIo => ExportFailure::Verification,
                LiveExportFailure::UnsupportedProjection => ExportFailure::UnsupportedContent,
            };
            self.run.finish_failed(failure);
            return;
        }
        if self.cancel.is_cancelled() {
            self.run.finish_cancelled();
            return;
        }
        let failure = match classify_export_failure(error, &self.cancel, writer_deadline) {
            LiveExportFailure::Cancelled => {
                self.run.finish_cancelled();
                return;
            }
            LiveExportFailure::DeadlineExceeded => ExportFailure::TimedOut,
            LiveExportFailure::StageIo | LiveExportFailure::WriterIo => ExportFailure::Io,
            LiveExportFailure::VerifierIo => ExportFailure::Verification,
            LiveExportFailure::UnsupportedProjection => ExportFailure::UnsupportedContent,
        };
        self.run.finish_failed(failure);
    }
}

/// Live progress observer: maps writer events onto the registry's
/// counters and phase. The local counters are atomic; registry updates
/// take the run's state lock. Only the export worker calls the observer.
struct RunObserver {
    run: Arc<ExportRun>,
    sections: AtomicU64,
    items: AtomicU64,
    bytes: AtomicU64,
    #[cfg(test)]
    verifying: Option<(Arc<VerificationHook>, CancellationToken)>,
}

impl ExportObserver for RunObserver {
    fn on_export_progress(&self, event: ExportProgress) {
        match event {
            ExportProgress::ItemWritten { bytes } => {
                self.items.fetch_add(1, Ordering::Relaxed);
                let total = self.bytes.fetch_add(bytes, Ordering::Relaxed) + bytes;
                self.run.note_progress(
                    self.sections.load(Ordering::Relaxed),
                    self.items.load(Ordering::Relaxed),
                    total,
                );
            }
            ExportProgress::SectionFinished(_) => {
                let sections = self.sections.fetch_add(1, Ordering::Relaxed) + 1;
                self.run.note_progress(
                    sections,
                    self.items.load(Ordering::Relaxed),
                    self.bytes.load(Ordering::Relaxed),
                );
            }
            ExportProgress::Verifying => {
                // Pre-publication verification: report the phase at the
                // event (the publication claim would otherwise be the
                // first `Verifying` observer, which is too late for a
                // status polled during verification). Idempotent: the
                // claim's own flip lands on the same value.
                self.run.set_phase(ExportPhase::Verifying);
                self.run.note_progress(
                    self.sections.load(Ordering::Relaxed),
                    self.items.load(Ordering::Relaxed),
                    self.bytes.load(Ordering::Relaxed),
                );
                #[cfg(test)]
                if let Some((verifying, cancel)) = &self.verifying {
                    verifying(cancel);
                }
            }
        }
    }
}

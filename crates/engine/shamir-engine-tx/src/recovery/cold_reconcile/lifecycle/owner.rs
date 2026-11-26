//! Lifecycle owner: repository-owned structured worker lifecycle
//! (S5c-X PR6b-2b, supervisor-owned-join-handle design).
//!
//! Three objects make up the foundation: this owner, the worker execution
//! state ([`super::worker::WorkerExecutionState`]),
//! and the job envelope. The lifecycle is `Open -> Closing -> Joined`;
//! quarantine is unconditional (the parked-guard count is NEVER consulted).
//! Repository shutdown uses ONE absolute deadline for worker shutdown,
//! commit quiescence, flush, and checkpoint. Lifecycle shutdown itself waits
//! only for the worker terminal. Full contract:
//! `docs/guide-docs/follower-journal-durability.md`.
//!
//! The handle and lease `Arc` belong to a supervisor task. The supervisor
//! joins the worker, drops the lease (the last `Arc<L>`), sets
//! `supervisor_joined`, then publishes the terminal outcome into a `watch`
//! channel (first-write-wins). Dropping the owner therefore cannot detach
//! the worker, and the physical lease lifetime ends strictly after join.
//!
//! Contention model: admission and quarantine serialize acceptance with
//! `admission_gate` and take `control` second. Quarantine releases admission
//! before acquiring the publication interlock. Publication callbacks must not
//! re-enter lifecycle operations. No lifecycle lock is held across `.await`.
//! The worker has no sender reference. Protected resources must not be dropped from publication callbacks.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use super::interlock::PubTermInterlock;
use super::job::{JobSubmitError, LifecycleJob};
use super::worker::{WorkerExecutionState, WorkerLease};

/// Default capacity of the job channel between owner and worker.
/// (Kept as a constant only; the dead `channel_bound()` accessor and field
/// were removed in the supervisor redesign.)
const DEFAULT_CHANNEL_BOUND: usize = 16;

/// Test seam run between the quarantine check and lease-hook registration.
#[cfg(test)]
type InstallSeam = Box<dyn FnOnce(Arc<CoreState>) + Send>;

#[derive(Debug, Clone, thiserror::Error)]
pub enum WorkerTerminal {
    /// The worker task ran to completion.
    #[error("worker completed")]
    Completed,
    /// The worker task panicked (or was cancelled); payload stringified.
    #[error("worker failed: {0}")]
    Failed(String),
}

/// Shared owner/supervisor/worker state. Atomic observations coexist with
/// synchronous mutexes for admission/quarantine and publication serialization.
pub(crate) struct CoreState {
    interlock: PubTermInterlock,
    /// Synchronous gate serializing submit's acceptance point with
    /// quarantine. Lock order is admission_gate then owner control.
    admission_gate: Mutex<()>,
    /// Sticky quarantine flag visible to repo reopen logic.
    pub(crate) quarantined: AtomicBool,
    /// Sticky flag: repository reopen is required before further use.
    pub(crate) reopen_required: AtomicBool,
    /// Sticky flag set by the supervisor strictly AFTER joining the worker
    /// and dropping the lease, BEFORE the terminal becomes observable —
    /// tests use it to prove lease-release ordering.
    supervisor_joined: AtomicBool,
    /// Terminal publication channel (first-write-wins; `None` until the
    /// supervisor publishes the outcome).
    terminal_tx: tokio::sync::watch::Sender<Option<WorkerTerminal>>,
    /// First quarantine reason, surfaced as the supervisor's terminal failure.
    quarantine_reason: Mutex<Option<String>>,
    /// Installed lease cleanup hook; taken before invocation so callback
    /// re-entry never occurs while this registration lock is held.
    quarantine_hook: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    #[cfg(test)]
    before_quarantine_hook_install: Mutex<Option<InstallSeam>>,
}

impl CoreState {
    /// Handle to the publication/termination interlock (worker side).
    pub(crate) fn interlock(&self) -> &PubTermInterlock {
        &self.interlock
    }

    /// UNCONDITIONAL quarantine: sticky `quarantined` + `reopen_required`
    /// (both Release), then `interlock.terminate()`. The parked-guard count
    /// is NEVER consulted. Timeout and panic BOTH route here.
    pub(crate) fn quarantine(&self, reason: &str) {
        log::warn!("worker lifecycle quarantine: {reason}");
        let mut recorded = self
            .quarantine_reason
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if recorded.is_none() {
            *recorded = Some(reason.to_owned());
        }
        drop(recorded);
        // Close publication first, then fail-stop the installed lease before
        // any worker-owned guards can be released. Callback runs lock-free.
        let gate = self
            .admission_gate
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        self.quarantined.store(true, Ordering::Release);
        self.reopen_required.store(true, Ordering::Release);
        drop(gate);
        self.interlock().terminate();
        self.fire_quarantine_hook();
    }

    fn install_quarantine_hook(&self, hook: Arc<dyn Fn() + Send + Sync>) {
        *self
            .quarantine_hook
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Some(hook);
    }

    fn fire_quarantine_hook(&self) {
        let hook = self
            .quarantine_hook
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        if let Some(hook) = hook {
            hook();
        }
    }

    /// Supervisor-only terminal publication, after worker join and lease drop.
    fn set_terminal(&self, terminal: WorkerTerminal) {
        let _ = self.terminal_tx.send_if_modified(|slot| {
            if slot.is_none() {
                *slot = Some(terminal);
                true
            } else {
                false
            }
        });
    }
}

/// Error returned by [`RepoWorkerLifecycle::install_worker`].
#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, thiserror::Error)]
pub enum InstallError {
    /// Lifecycle is closed or permanently forbidden.
    #[error("worker install rejected: lifecycle closed or spawn forbidden")]
    ClosedOrForbidden,
    /// Worker is already installed.
    #[error("worker install rejected: worker already installed")]
    AlreadyInstalled,
}

/// Error returned by [`RepoWorkerLifecycle::shutdown`].
#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, thiserror::Error)]
pub enum ShutdownError {
    /// The absolute shutdown deadline elapsed before the worker exited.
    /// Quarantine was applied; the supervisor retains the handle and the
    /// lease, and the terminal becomes observable if/when the worker exits.
    #[error("worker shutdown deadline elapsed")]
    TimedOut,
}

/// Lifecycle state machine: `Open -> Closing -> Joined`.
#[derive(Debug)]
enum LifecycleState {
    /// Admissions open, worker may be installed.
    Open,
    /// Admission closed (sender dropped); buffered jobs still drain FIFO.
    Closing,
    /// The worker was joined and its terminal outcome is known.
    Joined,
}

/// The ONE control critical section: state + sender + spawn bookkeeping +
/// the pending receiver, all mutated together and never across an await.
struct Control<J, R> {
    state: LifecycleState,
    sender: Option<mpsc::Sender<LifecycleJob<J, R>>>,
    pending_rx: Option<mpsc::Receiver<LifecycleJob<J, R>>>,
    spawned: bool,
    spawn_forbidden: bool,
}

/// Repository-owned worker lifecycle owner. Generic over the job payload
/// `J` and reply `R` so the receipt pipeline plugs in later; the worker
/// stays INACTIVE in production until wired.
///
/// Drop contract: dropping the last public owner closes admission
/// synchronously (`Drop` calls `close`) — no abort, no panic. The owner
/// does NOT hold the worker's join handle (the supervisor does), so
/// dropping the owner cannot detach the task; physical worker resources
/// (the lease) end only after the supervisor joins the worker.
pub struct RepoWorkerLifecycle<J: Send, R: Send> {
    control: Mutex<Control<J, R>>,
    core: Arc<CoreState>,
    /// This owner's own receiver on the terminal channel (cloned per
    /// `shutdown` call; the cached-terminal read comes from here too).
    terminal: tokio::sync::watch::Receiver<Option<WorkerTerminal>>,
    /// Monotonic generation handed to each installed worker.
    generation: AtomicU64,
}

impl<J: Send, R: Send> Default for RepoWorkerLifecycle<J, R> {
    fn default() -> Self {
        Self::new()
    }
}

impl<J: Send, R: Send> RepoWorkerLifecycle<J, R> {
    /// Creates an `Open` lifecycle with a pre-connected job channel but no
    /// worker. Spawning is a separate, explicit step (`install_worker`).
    pub fn new() -> Self {
        let (tx, rx) = mpsc::channel(DEFAULT_CHANNEL_BOUND);
        let (terminal_tx, terminal_rx) = tokio::sync::watch::channel(None);
        Self {
            control: Mutex::new(Control {
                state: LifecycleState::Open,
                sender: Some(tx),
                pending_rx: Some(rx),
                spawned: false,
                spawn_forbidden: false,
            }),
            core: Arc::new(CoreState {
                interlock: PubTermInterlock::new(),
                admission_gate: Mutex::new(()),
                quarantined: AtomicBool::new(false),
                reopen_required: AtomicBool::new(false),
                supervisor_joined: AtomicBool::new(false),
                quarantine_reason: Mutex::new(None),
                quarantine_hook: Mutex::new(None),
                #[cfg(test)]
                before_quarantine_hook_install: Mutex::new(None),
                terminal_tx,
            }),
            terminal: terminal_rx,
            generation: AtomicU64::new(0),
        }
    }

    /// Shared publication/termination interlock: the owner and the worker
    /// pipeline's publication path use the SAME object.
    pub fn interlock(&self) -> &PubTermInterlock {
        &self.core.interlock
    }

    /// Sticky: quarantine was applied to this lifecycle epoch.
    pub fn quarantined(&self) -> bool {
        self.core.quarantined.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(crate) fn set_before_quarantine_hook_install(&self, seam: InstallSeam) {
        *self
            .core
            .before_quarantine_hook_install
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Some(seam);
    }

    /// Non-owner observation handle for protected resource destructors/tests.
    pub fn status(&self) -> super::worker::LifecycleStatus {
        super::worker::LifecycleStatus::new(Arc::clone(&self.core))
    }

    /// Sticky: the repository must reopen (recovering WAL-unsettled
    /// versions) before further use of this lifecycle epoch.
    pub fn reopen_required(&self) -> bool {
        self.core.reopen_required.load(Ordering::Acquire)
    }

    /// Sticky: the supervisor has joined the worker and dropped its lease
    /// `Arc`. Set strictly BEFORE the terminal becomes observable via
    /// `shutdown`.
    pub fn supervisor_joined(&self) -> bool {
        self.core.supervisor_joined.load(Ordering::Acquire)
    }

    /// Monotonic generation counter handed to each installed worker.
    pub fn next_generation(&self) -> u64 {
        self.generation.fetch_add(1, Ordering::AcqRel)
    }

    /// `true` while admission is open (state == `Open`).
    pub fn admission_open(&self) -> bool {
        !self.core.quarantined.load(Ordering::Acquire)
            && matches!(
                self.control.lock().unwrap_or_else(|e| e.into_inner()).state,
                LifecycleState::Open
            )
    }

    /// Installs the worker: synchronously checks admission, takes the
    /// pending receiver, spawns the worker task with a panic boundary, and
    /// spawns the SUPERVISOR that owns the join handle and a lease `Arc`
    /// for the rest of the lifecycle — all inside ONE synchronous critical
    /// section with no cancellation point (`tokio::spawn` is
    /// non-blocking; the `control` std-Mutex is never held across an
    /// await). On any rejection the tasks are NOT spawned: a
    /// close-before-initialize can never leave a spawned-but-untracked
    /// task, and a close concurrent with this call PERMANENTLY sets
    /// `spawn_forbidden`.
    ///
    /// The supervisor keeps the join handle and the lease until the worker
    /// is joined; the owner does NOT hold the handle, so dropping the
    /// owner cannot detach the task; the physical lease lifetime ends
    /// after join (the supervisor drops the last `Arc<L>` before
    /// publishing the terminal outcome).
    pub fn install_worker<L, F, Fut>(&self, lease: L, handler: F) -> Result<(), InstallError>
    where
        L: WorkerLease,
        F: FnOnce(WorkerExecutionState<J, R, L>) -> Fut + Send + 'static,
        // `tokio::spawn` requires the spawned future's captured state to
        // be `'static`, which propagates `J: 'static` / `R: 'static`.
        J: 'static,
        R: 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let mut control = self.control.lock().unwrap_or_else(|e| e.into_inner());
        if control.spawned {
            return Err(InstallError::AlreadyInstalled);
        }
        if !matches!(control.state, LifecycleState::Open)
            || control.spawn_forbidden
            || self.core.quarantined.load(Ordering::Acquire)
        {
            if !matches!(control.state, LifecycleState::Open) {
                control.spawn_forbidden = true;
            }
            return Err(InstallError::ClosedOrForbidden);
        }
        #[expect(
            clippy::expect_used,
            reason = "guard: Open lifecycle state retains its pending receiver until worker installation"
        )]
        let receiver = control
            .pending_rx
            .take()
            .expect("Open lifecycle always holds the pending receiver");
        let generation = self.next_generation();
        let lease: Arc<L> = Arc::new(lease);
        let state = WorkerExecutionState::new(
            receiver,
            Arc::clone(&lease),
            generation,
            Arc::clone(&self.core),
        );
        let core_w = Arc::clone(&self.core);
        let inner = async move { handler(state).await };
        let quarantine_lease = Arc::downgrade(&lease);
        #[cfg(test)]
        if let Some(seam) = self
            .core
            .before_quarantine_hook_install
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        {
            seam(Arc::clone(&self.core));
        }
        self.core.install_quarantine_hook(Arc::new(move || {
            if let Some(lease) = quarantine_lease.upgrade() {
                lease.on_quarantine();
            }
        }));
        if self.core.quarantined.load(Ordering::Acquire) {
            self.core.fire_quarantine_hook();
            lease.on_quarantine();
            return Err(InstallError::ClosedOrForbidden);
        }
        let worker_task = async move {
            let result =
                futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(inner)).await;
            match result {
                Ok(()) => WorkerTerminal::Completed,
                Err(_) => {
                    core_w.quarantine("worker panicked");
                    WorkerTerminal::Failed("worker panicked".to_string())
                }
            }
        };
        let worker_handle = tokio::spawn(worker_task);
        // The supervisor OWNS the join handle and a lease Arc for the rest
        // of the lifecycle — this is what makes Drop-of-owner unable to
        // detach the worker.
        let _supervisor = spawn_supervisor(Arc::clone(&self.core), worker_handle, lease);
        control.spawned = true;
        Ok(())
    }

    /// Synchronously submits a job. The same critical section holds state,
    /// the spawned flag, AND sender, so a close or a never-installed worker
    /// concurrent with this call serializes: a successful `try_send` while
    /// state == `Open` AND a worker exists IS the acceptance linearization
    /// point.
    ///
    /// Policy: NO work is accepted before a worker exists — the
    /// `spawned` check below makes `NotInitialized` deterministic instead
    /// of relying on `try_send` racing the channel close. (Installing at
    /// construction is impossible: the handler is supplied later, via
    /// `install_worker`.)
    pub fn submit(&self, payload: J) -> Result<tokio::sync::oneshot::Receiver<R>, JobSubmitError> {
        let _gate = self
            .core
            .admission_gate
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let control = self.control.lock().unwrap_or_else(|e| e.into_inner());
        if self.core.quarantined.load(Ordering::Acquire)
            || !matches!(control.state, LifecycleState::Open)
        {
            return Err(JobSubmitError::Closing);
        }
        if !control.spawned {
            return Err(JobSubmitError::NotInitialized);
        }
        let sender = control
            .sender
            .as_ref()
            .ok_or(JobSubmitError::NotInitialized)?;
        let (job, rx) = LifecycleJob::new(payload);
        sender.try_send(job).map_err(|e| match e {
            mpsc::error::TrySendError::Full(_) => JobSubmitError::Full,
            mpsc::error::TrySendError::Closed(_) => JobSubmitError::NotInitialized,
        })?;
        Ok(rx)
    }

    /// Closes admission: transitions `Open -> Closing` and drops the
    /// sender so the worker drains buffered jobs FIFO and then observes
    /// channel end. Idempotent; safe to call from `Drop`.
    pub fn close(&self) {
        let _gate = self
            .core
            .admission_gate
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let mut control = self.control.lock().unwrap_or_else(|e| e.into_inner());
        if matches!(control.state, LifecycleState::Open) {
            control.state = LifecycleState::Closing;
            control.sender = None;
            if !control.spawned {
                control.spawn_forbidden = true;
                control.state = LifecycleState::Joined;
                self.core.supervisor_joined.store(true, Ordering::Release);
                self.core.set_terminal(WorkerTerminal::Completed);
            }
        }
    }

    /// UNCONDITIONAL quarantine: sticky `quarantined` + `reopen_required`,
    /// `interlock.terminate()`, then `close()` (admission closed). Timeout
    /// and panic BOTH call this; the parked-guard count is NEVER consulted.
    pub fn quarantine(&self, reason: String) {
        self.core.quarantine(&reason);
        self.close();
    }

    /// Waits for the worker terminal under a relative budget. Repository
    /// `shutdown_clean` uses `shutdown_until` with its shared absolute deadline
    /// before commit quiescence, flush, and checkpoint.
    pub async fn shutdown(&self, deadline: Duration) -> Result<WorkerTerminal, ShutdownError> {
        self.shutdown_until(tokio::time::Instant::now() + deadline)
            .await
    }

    /// Waits until an absolute Tokio deadline without retaining control locks.
    pub async fn shutdown_until(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<WorkerTerminal, ShutdownError> {
        self.close();
        let rx = self.terminal.clone();
        if let Some(t) = rx.borrow().clone() {
            // Cached terminal: the supervisor already joined and published;
            // never repoll the worker.
            self.record_joined();
            return Ok(t);
        }
        Box::pin(self.wait_terminal_until(deadline, rx)).await
    }

    async fn wait_terminal_until(
        &self,
        deadline: tokio::time::Instant,
        mut rx: tokio::sync::watch::Receiver<Option<WorkerTerminal>>,
    ) -> Result<WorkerTerminal, ShutdownError> {
        let result = tokio::time::timeout_at(deadline, async {
            let value = rx.wait_for(|t| t.is_some()).await?;
            #[expect(clippy::expect_used, reason = "guard: watch receiver wait_for returns only after its predicate observes Some")]
            Ok::<WorkerTerminal, tokio::sync::watch::error::RecvError>(
                value
                    .as_ref()
                    .cloned()
                    .expect("wait_for predicate guarantees Some"),
            )
        })
        .await;
        match result {
            Ok(Ok(terminal)) => {
                self.record_joined();
                Ok(terminal)
            }
            Ok(Err(_recv_closed)) => {
                // Unreachable: the sender lives in `core` (`Arc<CoreState>`,
                // shared with the supervisor), which outlives this call —
                // fail closed anyway.
                self.quarantine("terminal channel closed".to_string());
                Err(ShutdownError::TimedOut)
            }
            Err(_elapsed) => {
                self.quarantine("worker shutdown deadline elapsed".to_string());
                Err(ShutdownError::TimedOut)
            }
        }
    }

    /// Records the `Closing -> Joined(terminal)` transition on this
    /// owner's control state. Called only from `shutdown` success paths
    /// (the supervisor performs the actual join). Poisoning-tolerant: a
    /// panicking holder must not wedge the lifecycle control plane.
    fn record_joined(&self) {
        let mut control = self.control.lock().unwrap_or_else(|e| e.into_inner());
        control.state = LifecycleState::Joined;
    }
}

/// The supervisor task is intentionally background-owned: the runtime
/// must remain alive until worker cleanup and supervisor publication finish.
fn spawn_supervisor<L: WorkerLease>(
    core: Arc<CoreState>,
    handle: JoinHandle<WorkerTerminal>,
    lease: Arc<L>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let terminal = match handle.await {
            Ok(terminal) => terminal,
            Err(join_err) => {
                core.quarantine("worker task failed outside its panic boundary");
                WorkerTerminal::Failed(join_err.to_string())
            }
        };
        drop(lease);
        let terminal = match terminal {
            WorkerTerminal::Completed if core.quarantined.load(Ordering::Acquire) => {
                let reason = core
                    .quarantine_reason
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .clone()
                    .unwrap_or_else(|| "worker quarantined".to_owned());
                WorkerTerminal::Failed(format!("worker quarantined: {reason}"))
            }
            other => other,
        };
        core.supervisor_joined.store(true, Ordering::Release);
        core.set_terminal(terminal);
    })
}

impl<J: Send, R: Send> Drop for RepoWorkerLifecycle<J, R> {
    fn drop(&mut self) {
        self.close();
    }
}

/// Repository-owned unit lifecycle. Foreground callers should retain a `ForegroundLifecycle` token; background users may retain the raw `Arc`.
pub type RepoUnitLifecycle = RepoWorkerLifecycle<(), ()>;

/// Foreground ownership token: dropping the final token closes its lifecycle.
pub struct ForegroundLifecycle<J: Send, R: Send> {
    lifecycle: Arc<RepoWorkerLifecycle<J, R>>,
}

impl<J: Send, R: Send> Default for ForegroundLifecycle<J, R> {
    fn default() -> Self {
        Self::new()
    }
}

impl<J: Send, R: Send> ForegroundLifecycle<J, R> {
    /// Starts a fresh foreground lifecycle owner.
    pub fn new() -> Self {
        Self {
            lifecycle: Arc::new(RepoWorkerLifecycle::new()),
        }
    }
    pub fn from_lifecycle(lifecycle: Arc<RepoWorkerLifecycle<J, R>>) -> Self {
        Self { lifecycle }
    }
    pub fn lifecycle(&self) -> &Arc<RepoWorkerLifecycle<J, R>> {
        &self.lifecycle
    }
    pub fn reopen_required(&self) -> bool {
        self.lifecycle.reopen_required()
    }
}

impl<J: Send, R: Send> Drop for ForegroundLifecycle<J, R> {
    fn drop(&mut self) {
        self.lifecycle.close();
    }
}

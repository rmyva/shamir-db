//! Worker execution state, guarded resource destruction, and lease hook.

use std::future::Future;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use tokio::sync::mpsc;

use shamir_tx::VersionGuard;

use super::interlock::PubTermInterlock;
use super::job::LifecycleJob;
use super::owner::{CoreState, InstallError, RepoWorkerLifecycle};

/// Lease bound to the supervisor/worker lifecycle.
pub trait WorkerLease: Send + Sync + 'static {
    /// Fail-closed cleanup for sensitive resources after quarantine.
    fn on_quarantine(&self) {}
}

/// Status observation handle. It cannot submit work or publish terminals.
#[derive(Clone)]
pub struct LifecycleStatus(Arc<CoreState>);

impl LifecycleStatus {
    pub(crate) fn new(core: Arc<CoreState>) -> Self {
        Self(core)
    }

    pub fn quarantined(&self) -> bool {
        self.0.quarantined.load(Ordering::Acquire)
    }
    pub fn reopen_required(&self) -> bool {
        self.0.reopen_required.load(Ordering::Acquire)
    }

    /// Runs a synchronous publication callback under the lifecycle interlock.
    /// The callback must not re-enter lifecycle operations.
    pub(crate) fn publish<T>(
        &self,
        callback: impl FnOnce() -> T,
    ) -> Result<T, super::interlock::PublicationClosed> {
        self.0.interlock().publish(callback)
    }
}

/// Capability required for resources whose destructor must fail closed.
pub trait FailClosedResource {
    /// Arm fail-closed cleanup before lifecycle ownership begins.
    fn arm_fail_closed(&mut self);
}

impl FailClosedResource for VersionGuard {
    fn arm_fail_closed(&mut self) {
        self.visibility_started_without_wal();
    }
}

/// Owned resource whose destructor must observe quarantine before release
/// when unwinding. Arbitrary unwrapped handler-local guards are not covered.
pub struct FailClosedGuard<T> {
    value: Option<T>,
    core: Arc<CoreState>,
}

impl<T: FailClosedResource> FailClosedGuard<T> {
    fn new(value: T, core: Arc<CoreState>) -> Self {
        Self {
            value: Some(value),
            core,
        }
    }
    pub fn get(&self) -> &T {
        #[expect(
            clippy::expect_used,
            reason = "guard: FailClosedGuard keeps its owned value present until commit or drop"
        )]
        self.value.as_ref().expect("guard value is present")
    }
    pub fn get_mut(&mut self) -> &mut T {
        #[expect(
            clippy::expect_used,
            reason = "guard: FailClosedGuard keeps its owned value present until commit or drop"
        )]
        self.value.as_mut().expect("guard value is present")
    }
}

impl FailClosedGuard<VersionGuard> {
    /// Consume the guard and commit its version on the successful path.
    /// Dropping without this call retains fail-closed cleanup behavior.
    pub fn commit(mut self) {
        #[expect(
            clippy::expect_used,
            reason = "guard: commit consumes the present VersionGuard before Drop runs"
        )]
        self.value.take().expect("guard value is present").commit();
    }
}

impl<T> Drop for FailClosedGuard<T> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.core
                .quarantine("protected resource dropped during unwind");
        }
        drop(self.value.take());
    }
}

/// State owned only by the worker task; contains no sender or owner handle.
pub struct WorkerExecutionState<J, R, L> {
    receiver: mpsc::Receiver<LifecycleJob<J, R>>,
    lease: Arc<L>,
    generation: u64,
    core: Arc<CoreState>,
}

impl<J, R, L> WorkerExecutionState<J, R, L> {
    pub(crate) fn new(
        receiver: mpsc::Receiver<LifecycleJob<J, R>>,
        lease: Arc<L>,
        generation: u64,
        core: Arc<CoreState>,
    ) -> Self {
        Self {
            receiver,
            lease,
            generation,
            core,
        }
    }
    pub async fn next_job(&mut self) -> Option<LifecycleJob<J, R>> {
        self.receiver.recv().await
    }
    pub fn lease(&self) -> &L {
        &self.lease
    }
    pub fn generation(&self) -> u64 {
        self.generation
    }
    pub fn quarantine_cleanup(&self)
    where
        L: WorkerLease,
    {
        self.lease.on_quarantine();
    }
    /// Worker-side quarantine: sticky fail-closed flags plus publication
    /// termination. The supervisor publishes the terminal (Failed while
    /// quarantined) after joining.
    pub fn quarantine_worker(&self, reason: &str) {
        self.core.quarantine(reason);
    }
    pub fn interlock(&self) -> &PubTermInterlock {
        self.core.interlock()
    }
    pub fn status(&self) -> LifecycleStatus {
        LifecycleStatus::new(Arc::clone(&self.core))
    }
    /// Arm an owned sensitive resource; keep the guard outside publication callbacks during unwind.
    pub fn protect_guard<T: FailClosedResource>(&self, mut value: T) -> FailClosedGuard<T> {
        value.arm_fail_closed();
        FailClosedGuard::new(value, Arc::clone(&self.core))
    }
}

impl<J, R, L> Drop for WorkerExecutionState<J, R, L> {
    fn drop(&mut self) {
        // Handler-local resource destructors can themselves call quarantine,
        // so this Drop fallback handles only a state still alive at unwind.
        if std::thread::panicking() {
            self.core
                .quarantine("worker execution state dropped during unwind");
        }
    }
}

/// Terminal publication belongs exclusively to the supervisor. Its task is background-owned; runtime lifetime must extend through its completion.
#[cfg_attr(not(test), allow(dead_code))]
pub fn spawn_worker<J, R, L, F, Fut>(
    owner: &Arc<RepoWorkerLifecycle<J, R>>,
    lease: L,
    handler: F,
) -> Result<(), InstallError>
where
    J: Send + 'static,
    R: Send + 'static,
    L: WorkerLease,
    F: FnOnce(WorkerExecutionState<J, R, L>) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    owner.install_worker(lease, handler)
}

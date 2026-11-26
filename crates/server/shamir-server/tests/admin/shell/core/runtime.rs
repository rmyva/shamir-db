use std::future::Future;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use futures_util::future::BoxFuture;

const RUNTIME_RUN_BOUND: Duration = Duration::from_secs(120);
const SHUTDOWN_BOUND: Duration = Duration::from_secs(150);

/// Margin between a job's own inner timeout (set to the absolute deadline) and
/// the caller-side bound of the `run_timeout` that carries it, so the typed
/// inner error wins the race against the outer panic on a loaded runner. It is
/// a margin, not a guarantee: a stall longer than the slack still panics the
/// caller, and it costs nothing on a passing run.
pub const DEADLINE_SLACK: Duration = Duration::from_secs(5);

/// Join a thread that has already announced its stop (or is unwinding),
/// bounded by an `is_finished` spin against the absolute deadline.
fn join_before(
    thread: thread::JoinHandle<()>,
    deadline: std::time::Instant,
) -> Result<(), &'static str> {
    loop {
        if thread.is_finished() {
            return thread.join().map_err(|_| "runtime worker panicked");
        }
        if deadline
            .saturating_duration_since(std::time::Instant::now())
            .is_zero()
        {
            drop(thread);
            return Err("runtime worker did not stop before deadline");
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
}

/// One current-thread Tokio runtime on a roomy (64 MiB stack) std thread.
pub struct TestRuntime {
    job_tx: Option<tokio::sync::mpsc::Sender<BoxFuture<'static, ()>>>,
    stopped_rx: Option<mpsc::Receiver<()>>,
    thread: Option<thread::JoinHandle<()>>,
}

impl TestRuntime {
    /// Spawn the runtime thread.
    pub fn new() -> Self {
        let (job_tx, mut job_rx) = tokio::sync::mpsc::channel::<BoxFuture<'static, ()>>(64);
        let (stopped_tx, stopped_rx) = mpsc::sync_channel(1);
        let thread = thread::Builder::new()
            .name("shell-test-runtime".into())
            .stack_size(64 * 1024 * 1024)
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("test runtime");
                runtime.block_on(async move {
                    while let Some(job) = job_rx.recv().await {
                        tokio::spawn(job);
                    }
                });
                drop(runtime);
                let _ = stopped_tx.send(());
            })
            .expect("spawn shell-test runtime thread");
        Self {
            job_tx: Some(job_tx),
            stopped_rx: Some(stopped_rx),
            thread: Some(thread),
        }
    }

    fn sender(&self) -> tokio::sync::mpsc::Sender<BoxFuture<'static, ()>> {
        self.job_tx
            .as_ref()
            .expect("TestRuntime sender retained until drop")
            .clone()
    }

    /// Drive `future` to completion on the runtime, blocking the caller.
    pub fn run<F>(&self, future: F) -> F::Output
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.run_timeout(RUNTIME_RUN_BOUND, future)
    }

    /// Drive a shell subprocess or interactive driver with its explicit bound.
    pub fn run_timeout<F>(&self, timeout: Duration, future: F) -> F::Output
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let (done_tx, done_rx) = mpsc::sync_channel::<F::Output>(1);
        let job = async move {
            let value = match tokio::time::timeout(timeout, future).await {
                Ok(value) => value,
                Err(_) => panic!("shell-test runtime job exceeded {timeout:?}"),
            };
            let _ = done_tx.send(value);
        };
        match self.sender().try_send(Box::pin(job)) {
            Ok(()) => {}
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                panic!("the shell-test runtime thread is gone")
            }
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                panic!("the shell-test runtime job queue is full")
            }
        }
        match done_rx.recv_timeout(timeout) {
            Ok(value) => value,
            Err(mpsc::RecvTimeoutError::Timeout) => {
                panic!("shell-test runtime job exceeded {timeout:?}")
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => panic!(
                "the runtime job panicked (its own panic is the real failure); \
                 the job sender was dropped without a value"
            ),
        }
    }
    /// Stop the worker and join it before an absolute caller deadline.
    /// The stop signal is the handshake; the join itself is then bounded by
    /// an `is_finished` spin against the deadline.
    pub fn shutdown_until(&mut self, deadline: std::time::Instant) -> Result<(), &'static str> {
        drop(self.job_tx.take());
        let Some(thread) = self.thread.take() else {
            return Ok(());
        };
        let cap = deadline.saturating_duration_since(std::time::Instant::now());
        if cap.is_zero() {
            drop(thread);
            return Err("runtime worker did not stop before deadline");
        }
        match self
            .stopped_rx
            .take()
            .expect("runtime stop receiver retained")
            .recv_timeout(cap)
        {
            Ok(()) => join_before(thread, deadline),
            Err(mpsc::RecvTimeoutError::Timeout) => {
                drop(thread);
                Err("runtime worker did not stop before deadline")
            }
            // The stop sender was dropped without a value: the worker is
            // unwinding or already gone; the join is bounded like the other
            // branch.
            Err(mpsc::RecvTimeoutError::Disconnected) => join_before(thread, deadline),
        }
    }
}

impl Drop for TestRuntime {
    fn drop(&mut self) {
        drop(self.job_tx.take());
        if let Some(thread) = self.thread.take() {
            // Fallback only, capped by SHUTDOWN_BOUND; successful shutdown_until takes these fields.
            match self
                .stopped_rx
                .take()
                .expect("runtime stop receiver retained until drop")
                .recv_timeout(SHUTDOWN_BOUND)
            {
                Ok(()) => {
                    if let Err(panic) = thread.join() {
                        eprintln!("shell-test runtime worker panicked during shutdown: {panic:?}");
                    }
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    eprintln!(
                        "shell-test runtime shutdown exceeded {SHUTDOWN_BOUND:?}; \
                         recording teardown failure and detaching runtime thread"
                    );
                    drop(thread);
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => match thread.join() {
                    Ok(()) => eprintln!("shell-test runtime exited without its stop notification"),
                    Err(panic) => eprintln!(
                        "shell-test runtime worker panicked before stop notification: {panic:?}"
                    ),
                },
            }
        }
    }
}

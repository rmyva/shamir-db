//! Lifecycle job envelope (S5c-X PR6b-2b).
//!
//! The JOB carries the payload + reply sender only. It holds NO foreground
//! `RepoInstance`, no owner `Arc`, and no lifecycle sender — the worker
//! cannot reach back into the owner through a job, which is what keeps the
//! ownership graph acyclic.

use tokio::sync::oneshot;

/// Error returned when a job cannot be admitted into the worker channel.
#[derive(Debug, thiserror::Error)]
pub enum JobSubmitError {
    /// The lifecycle is closing (or closed, or quarantined): admission is
    /// shut. Retry after reopen — never inside the same lifecycle epoch.
    #[error("worker lifecycle is closing")]
    Closing,
    /// No worker was ever installed (`install_worker` was never called, or
    /// it was rejected): there is no receiver to accept the job.
    #[error("no worker installed for this lifecycle")]
    NotInitialized,
    /// The channel is at capacity. This is backpressure, not closing —
    /// retry after the worker drains.
    #[error("worker channel is full")]
    Full,
}

/// A unit of work handed to the lifecycle worker: `payload` plus a
/// one-shot reply channel. Consuming via [`LifecycleJob::reply`] sends the
/// answer; a send error is ignored because caller cancellation drops only
/// the receiver half.
pub struct LifecycleJob<J, R> {
    /// Caller-supplied work item.
    pub payload: J,
    reply: oneshot::Sender<R>,
}

impl<J, R> LifecycleJob<J, R> {
    /// Creates a job and the receiving half of its reply channel.
    pub fn new(payload: J) -> (Self, oneshot::Receiver<R>) {
        let (tx, rx) = oneshot::channel();
        (Self { payload, reply: tx }, rx)
    }

    /// Sends `value` to the caller. A send error means the caller was
    /// cancelled (its receiver was dropped) and is intentionally ignored.
    pub fn reply(self, value: R) {
        let _ = self.reply.send(value);
    }
}

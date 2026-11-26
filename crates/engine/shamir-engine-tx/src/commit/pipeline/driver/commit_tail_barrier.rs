use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;

/// Counts inline and detached commit futures, not admission or WAL draining.
#[derive(Default)]
pub struct CommitTailBarrier {
    active: AtomicUsize,
    notify: Notify,
}

impl CommitTailBarrier {
    pub(crate) fn enter(self: &Arc<Self>) -> CommitTailGuard {
        self.active.fetch_add(1, Ordering::AcqRel);
        CommitTailGuard {
            barrier: Arc::clone(self),
        }
    }

    /// cancel-safe: yes — dropping the wait does not change the counter.
    /// Admission must be stopped separately before using zero for shutdown.
    pub async fn wait_zero(&self) {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            // Arm before checking: the final decrement may race this waiter.
            notified.as_mut().enable();
            if self.active.load(Ordering::Acquire) == 0 {
                return;
            }
            notified.await;
        }
    }
}

pub(crate) struct CommitTailGuard {
    barrier: Arc<CommitTailBarrier>,
}

impl Drop for CommitTailGuard {
    fn drop(&mut self) {
        if self.barrier.active.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.barrier.notify.notify_waiters();
        }
    }
}

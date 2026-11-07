//! Single-writer, snapshot-reader actor base for stateful index backends.
//!
//! Pattern: writes go through a bounded `tokio::sync::mpsc::Sender`
//! (lock-free MPSC). A spawned tokio task drains the channel and
//! applies ops sequentially, updating an `ArcSwap<Snap>` snapshot.
//! Readers grab the current snapshot with one atomic load — no locks.
//!
//! Used by FTS / Vector backends (Phase 2 / Phase 4). The Btree
//! backend is stateless on top of `Store` and doesn't need an actor.

use arc_swap::ArcSwap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

#[cfg(test)]
#[path = "../../tests/runtime/actor_shutdown_tests.rs"]
mod shutdown_tests;

pub struct IndexActor<Op: Send + 'static, Snap: Send + Sync + 'static> {
    write_tx: mpsc::Sender<Op>,
    pub(crate) snapshot: Arc<ArcSwap<Snap>>,
    join: Option<JoinHandle<()>>,
}

impl<Op: Send + 'static, Snap: Send + Sync + 'static> IndexActor<Op, Snap> {
    /// Default bounded-channel capacity. §B14 bans unbounded channels
    /// without a "provably bounded producer rate" — `apply_index_ops`
    /// is called per write so under bulk-import load the queue could
    /// grow without limit and OOM the server. 1024 ops × ~200 B/op
    /// caps memory at ~200 KB while still absorbing brief bursts
    /// before the producer blocks on backpressure.
    pub const DEFAULT_CHANNEL_CAPACITY: usize = 1024;

    /// Spawn an actor task with the default channel capacity. See
    /// [`Self::DEFAULT_CHANNEL_CAPACITY`] for the rationale.
    pub fn spawn<F, Fut>(initial: Snap, applier: F) -> Self
    where
        F: FnMut(Op, Arc<ArcSwap<Snap>>) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = ()> + Send,
    {
        Self::spawn_with_capacity(initial, Self::DEFAULT_CHANNEL_CAPACITY, applier)
    }

    /// Spawn an actor task with a custom channel capacity. `applier`
    /// is invoked once per submitted `Op`; it receives the current
    /// snapshot handle so it can replace it atomically via
    /// `snapshot.store(Arc::new(new))`.
    pub fn spawn_with_capacity<F, Fut>(initial: Snap, capacity: usize, mut applier: F) -> Self
    where
        F: FnMut(Op, Arc<ArcSwap<Snap>>) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = ()> + Send,
    {
        let snapshot = Arc::new(ArcSwap::from(Arc::new(initial)));
        let (write_tx, mut rx) = mpsc::channel::<Op>(capacity);
        let snap_for_task = snapshot.clone();
        let join = tokio::spawn(async move {
            while let Some(op) = rx.recv().await {
                applier(op, snap_for_task.clone()).await;
            }
        });
        Self {
            write_tx,
            snapshot,
            join: Some(join),
        }
    }

    /// Submit a write op.
    ///
    /// Bounded MPSC channel. Producers block on `submit().await` when
    /// the queue is full (default capacity
    /// [`Self::DEFAULT_CHANNEL_CAPACITY`]). This is intentional
    /// backpressure — uncapped queue growth under bulk-import load
    /// would OOM the server. Callers that cannot tolerate blocking
    /// should use [`Self::try_submit`] (returns `Err` on full) instead.
    ///
    /// Returns `Err` if the actor task has already stopped
    /// (channel closed).
    pub async fn submit(&self, op: Op) -> Result<(), mpsc::error::SendError<Op>> {
        self.write_tx.send(op).await
    }

    /// Non-blocking submit. Returns `Err(TrySendError::Full)` if the
    /// queue is at capacity, `Err(TrySendError::Closed)` if the actor
    /// task has stopped. Use this in sync contexts where awaiting is
    /// not an option; the caller decides how to surface backpressure.
    pub fn try_submit(&self, op: Op) -> Result<(), mpsc::error::TrySendError<Op>> {
        self.write_tx.try_send(op)
    }

    /// Lock-free snapshot read.
    pub fn snapshot(&self) -> Arc<Snap> {
        self.snapshot.load_full()
    }

    /// Replace the snapshot directly (escape hatch for tests / cold
    /// restore). Most code paths should mutate via `submit`.
    pub fn replace_snapshot(&self, new_snap: Snap) {
        self.snapshot.store(Arc::new(new_snap));
    }

    /// Stop accepting new ops and wait for the applier task to drain.
    pub async fn shutdown(self) {
        self.shutdown_with_report(|error| log::error!("index actor task failed: {error}"))
            .await;
    }

    async fn shutdown_with_report(mut self, report: impl FnOnce(tokio::task::JoinError)) {
        drop(self.write_tx);
        if let Some(join) = self.join.take() {
            if let Err(error) = join.await {
                report(error);
            }
        }
    }
}

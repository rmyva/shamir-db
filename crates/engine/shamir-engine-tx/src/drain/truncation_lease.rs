use std::future::Future;
use std::sync::Arc;

use shamir_storage::error::{DbError, DbResult};
use shamir_tx::RepoWalManager;

use super::truncation_pin::TruncationPin;

/// Deletion admission; no newly acquired pin can return until this drops.
#[must_use = "the lease must cover the entire asynchronous deletion"]
pub(crate) struct TruncationLease {
    pub(super) registry: Arc<TruncationPin>,
    pub(super) ceiling: u64,
}

impl TruncationLease {
    /// Cancellation intentionally detaches the worker, NOT its lease. The WAL
    /// itself dispatches blocking unlink work which cannot be cancelled; keeping
    /// both await and lease in this worker covers that work even if our caller
    /// is cancelled. No checkpoint/flush/retention work runs under the lease.
    /// cancel-safe: yes — abandonment leaves the worker owning deletion and
    /// lease; retries are idempotent WAL deletion, serialized by the WAL gate.
    pub(crate) async fn truncate(self, wal: Arc<RepoWalManager>) -> DbResult<usize> {
        let ceiling = self.ceiling;
        self.run(async move { wal.truncate_below(ceiling).await })
            .await
    }

    async fn run<F>(self, deletion: F) -> DbResult<usize>
    where
        F: Future<Output = DbResult<usize>> + Send + 'static,
    {
        // Intentional detach on caller cancellation: deletion owns its lease.
        let worker = tokio::spawn(async move {
            let result = deletion.await;
            drop(self);
            result
        });
        worker
            .await
            .map_err(|error| DbError::Internal(format!("WAL truncation worker: {error}")))?
    }

    #[cfg(test)]
    pub(crate) fn ceiling(&self) -> u64 {
        self.ceiling
    }
}

impl Drop for TruncationLease {
    fn drop(&mut self) {
        self.registry.release_lease();
    }
}

#[cfg(test)]
#[path = "../tests/drain/truncation_pin/lease_tests.rs"]
mod tests;

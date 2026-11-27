//! Clean-shutdown method cluster for [`RepoInstance`]:
//! `shutdown_clean`, `worker_reopen_required`, `flush_buffers`.

use crate::tx::commit::core::timed_stage;
use shamir_storage::error::{DbError, DbResult};
use std::sync::Arc;

use crate::tx::apply_replicated::follower_durability::receipt::{ApplyOutcome, ReceiptApplyJob};
use crate::tx::cold_reconcile::lifecycle::{RepoWorkerLifecycle, WorkerTerminal};

use super::super::super::RepoInstance;

impl RepoInstance {
    /// Caller MUST stop commit/apply admission before calling this method.
    /// One absolute deadline covers lifecycle drain, commit quiescence,
    /// buffer flush and clean-horizon checkpoint. Any error quarantines an
    /// installed lifecycle; a never-installed lifecycle completes immediately.
    pub async fn shutdown_clean(&self, budget: std::time::Duration) -> DbResult<()> {
        Box::pin(self.shutdown_clean_inner(budget)).await
    }

    async fn shutdown_clean_inner(&self, budget: std::time::Duration) -> DbResult<()> {
        let result = Box::pin(self.shutdown_clean_window(budget)).await;
        // The cold-open close latch spans THIS shutdown window only: the
        // repository stays usable after a flush (embedders and tests flush
        // mid-life), so the latch is lifted when the window ends. A future
        // dropped mid-window never gets here and leaves the latch closed.
        self.core.cold_opens().reopen();
        result
    }

    async fn shutdown_clean_window(&self, budget: std::time::Duration) -> DbResult<()> {
        let deadline = tokio::time::Instant::now() + budget;
        let lifecycle = self.worker_lifecycle();
        if let Some(lifecycle) = lifecycle {
            lifecycle.close();
        }
        // Close cold-open admission at shutdown ENTRY (not after the
        // lifecycle drain): a lifecycle failure or timeout must not leave
        // cold opens unrefused and undrained. Fresh opens are refused with a
        // typed error before their first side effect; opens already
        // registered (inline or handed off) are drained below on EVERY path.
        self.core.cold_opens().close();

        // Lifecycle drain first. Its failure is remembered as the ORIGINAL
        // failure; the cold-open drain below still runs, because the handoff
        // may already have submitted a blocking store open that only the
        // drain settles.
        let mut original_failure: Option<DbError> = None;
        if let Some(lifecycle) = lifecycle {
            match Box::pin(timed_stage(
                &self.name,
                "lifecycle_drain",
                lifecycle.shutdown_until(deadline),
            ))
            .await
            {
                Ok(WorkerTerminal::Completed) => {}
                Ok(WorkerTerminal::Failed(msg)) => {
                    original_failure = Some(DbError::Storage(format!("worker failed: {msg}")));
                }
                Err(_) => {
                    original_failure = Some(DbError::Storage("worker shutdown timed out".into()))
                }
            }
        }

        // Cold opens handed off by dropped callers run outside the
        // lifecycle worker; drain them BEFORE commit quiescence, so a
        // recovery still mutating storage cannot race the flush. Attempted
        // on EVERY path (also after a lifecycle failure) — but it never
        // masks the original failure.
        let cold_open_drained = Box::pin(timed_stage(
            &self.name,
            "cold_open_drain",
            self.core.cold_opens().drain(deadline),
        ))
        .await;
        if !cold_open_drained && original_failure.is_none() {
            original_failure = Some(DbError::Storage("cold open drain timed out".into()));
        }
        if let Some(err) = original_failure {
            if let Some(lifecycle) = lifecycle {
                lifecycle.quarantine("clean shutdown failed".to_string());
            }
            return Err(err);
        }

        let result = tokio::time::timeout_at(deadline, async {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            match tokio::time::timeout_at(
                deadline,
                timed_stage(
                    &self.name,
                    "quiesce_commits",
                    self.quiesce_commits(remaining),
                ),
            )
            .await
            {
                Ok(result) => result?,
                Err(_) => {
                    return Err(DbError::Internal(format!(
                        "commit quiescence timed out after {remaining:?} for repo '{}'",
                        self.name
                    )));
                }
            }
            match tokio::time::timeout_at(
                deadline,
                timed_stage(&self.name, "flush_buffers", self.flush_buffers()),
            )
            .await
            {
                Ok(result) => result?,
                Err(_) => {
                    return Err(DbError::Storage(
                        "clean shutdown timed out during flush".into(),
                    ));
                }
            }
            match tokio::time::timeout_at(
                deadline,
                timed_stage(
                    &self.name,
                    "finish_clean_shutdown",
                    self.finish_clean_shutdown(deadline),
                ),
            )
            .await
            {
                Ok(result) => result,
                Err(_) => Err(DbError::Storage("clean shutdown timed out".into())),
            }
        })
        .await;

        match result {
            Ok(Ok(())) => Ok(()),
            Ok(Err(err)) => {
                if let Some(lifecycle) = lifecycle {
                    lifecycle.quarantine("clean shutdown failed".to_string());
                }
                Err(err)
            }
            Err(_) => {
                if let Some(lifecycle) = lifecycle {
                    lifecycle.quarantine("clean shutdown timed out".to_string());
                }
                Err(DbError::Storage("clean shutdown timed out".into()))
            }
        }
    }

    /// Sticky `reopen_required` flag; never-owned foreground instances are safe.
    pub fn worker_reopen_required(&self) -> bool {
        self.worker_lifecycle()
            .map(|lifecycle| lifecycle.reopen_required())
            .unwrap_or(false)
    }

    /// Shared lifecycle of foreground handles, if present. Background clones
    /// intentionally have no foreground lifecycle owner.
    fn worker_lifecycle(
        &self,
    ) -> Option<&Arc<RepoWorkerLifecycle<ReceiptApplyJob, DbResult<ApplyOutcome>>>> {
        self.core
            .worker_lifecycle()
            .as_ref()
            .map(|foreground| foreground.lifecycle())
    }

    /// Flush this repo's in-memory buffers to their durable backing.
    ///
    /// Drains the `__tx__` store and every table's data + info stores, then
    /// issues ONE backend persist (`Repo::persist_once`). The body lives on
    /// `RepoCore::flush_buffers`.
    pub async fn flush_buffers(&self) -> DbResult<()> {
        self.core.flush_buffers().await
    }
}

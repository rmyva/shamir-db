use shamir_tx::TxContext;

use crate::tx_registry::TxAbortRelease;

/// Owns a context taken from a parked transaction until the commit pipeline
/// takes it. On drop it releases the Level-3 record locks through the
/// parking-time releaser, as a detached task, so a dropped commit future cannot lose the release.
///
/// Known limit: once `disarm()` hands the context to the engine, cleanup is the pipeline's own
/// responsibility. The pipeline awaits, before `CommitDriver::new` (`shamir-engine`
/// `tx/commit/pipeline/phase/commit/entry.rs:756`), at `commit_admission().acquire_leader` (:685-689),
/// `repo.tx_gate()` (:703), the `requires_reopen` checks (:710, :728), `repo.repo_wal()` (:720) and
/// `pre_commit_prelock` (:745); a request dropped in those windows releases through the pipeline's
/// inline error arms ONLY if its poll reaches them, otherwise the Level-3 locks stay held with no
/// owner left to release them (a younger transaction waits for them, it does not remove them). Guard-covered exits: a name that no longer resolves at commit (released through the bound
/// releaser, spawned+awaited), authorization denial (awaited here), drop before the pipeline starts (guard
/// Drop), drop during the denial release (release task survives). Tracked in task #2020.
pub(in crate::engine::db_handler) struct ParkedCommitCleanup {
    ctx: Option<TxContext>,
    pub(in crate::engine::db_handler) release: TxAbortRelease,
    /// The runtime current when the guard was armed: `Drop` may run on a thread that has no
    /// current runtime while the runtime itself is still alive.
    runtime: Option<tokio::runtime::Handle>,
}

impl ParkedCommitCleanup {
    pub(in crate::engine::db_handler) fn new(
        ctx: Option<TxContext>,
        release: TxAbortRelease,
    ) -> Self {
        Self {
            ctx,
            release,
            runtime: tokio::runtime::Handle::try_current().ok(),
        }
    }

    pub(in crate::engine::db_handler) fn disarm(&mut self) -> Option<TxContext> {
        self.ctx.take()
    }

    pub(in crate::engine::db_handler) async fn release_now(mut self) {
        let Some(ctx) = self.ctx.take() else {
            return;
        };
        if ctx.locked_keys.is_empty() {
            return;
        }
        let release = std::sync::Arc::clone(&self.release);
        let handle = tokio::spawn((release)(ctx));
        match handle.await {
            Ok(()) => (),
            Err(error) if error.is_panic() => {
                tracing::error!("parked commit lock release task panicked")
            }
            Err(error) => tracing::warn!("parked commit lock release task failed: {error}"),
        }
    }
}

impl Drop for ParkedCommitCleanup {
    fn drop(&mut self) {
        if let Some(ctx) = self.ctx.take() {
            if ctx.locked_keys.is_empty() {
                return;
            }
            match self
                .runtime
                .take()
                .or_else(|| tokio::runtime::Handle::try_current().ok())
            {
                Some(runtime) => {
                    drop(runtime.spawn((self.release)(ctx)));
                }
                None => tracing::warn!(
                    "dropped parked commit found no runtime: its record locks go with the process"
                ),
            }
        }
    }
}

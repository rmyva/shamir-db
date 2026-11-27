use crate::repo::RepoInstance;

impl RepoInstance {
    /// Idle count of parked guards, for tests and diagnostics. `None` when
    /// the worker was never installed.
    #[cfg(all(test, feature = "fjall"))]
    pub(crate) async fn receipt_worker_parked_guards(&self) -> Option<usize> {
        self.core
            .receipt_worker()
            .get()
            .map(|lease| lease.parked_count())
    }

    /// Deterministic test wait: no receipt job is in flight on this repo's
    /// worker.
    #[cfg(all(test, feature = "fjall"))]
    #[expect(
        clippy::unwrap_used,
        reason = "guard: test helper requires an installed receipt worker"
    )]
    pub(crate) async fn receipt_worker_wait_idle(&self) {
        self.core
            .receipt_worker_installed()
            .await
            .unwrap()
            .wait_idle()
            .await;
    }
}

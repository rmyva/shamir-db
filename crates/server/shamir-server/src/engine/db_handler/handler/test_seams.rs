use std::sync::Arc;

use super::ShamirDbHandler;

type GetRecordIdentityPauseBarriers = (Arc<tokio::sync::Barrier>, Arc<tokio::sync::Barrier>);

#[derive(Default, Clone)]
pub(in crate::engine::db_handler) struct GetRecordIdentityPause(
    Arc<std::sync::Mutex<Option<GetRecordIdentityPauseBarriers>>>,
);

impl GetRecordIdentityPause {
    pub(in crate::engine::db_handler) fn install(
        &self,
        reached: Arc<tokio::sync::Barrier>,
        resume: Arc<tokio::sync::Barrier>,
    ) {
        *self.0.lock().expect("get-record pause mutex poisoned") = Some((reached, resume));
    }

    pub(in crate::engine::db_handler) async fn pause(&self) {
        let pause = self
            .0
            .lock()
            .expect("get-record pause mutex poisoned")
            .take();
        if let Some((reached, resume)) = pause {
            reached.wait().await;
            resume.wait().await;
        }
    }
}

pub(super) type CommitPause = (u64, Arc<tokio::sync::Barrier>, Arc<tokio::sync::Barrier>);

#[derive(Default, Clone)]
pub(in crate::engine::db_handler) struct CreateCursorIdentityPause(
    Arc<std::sync::Mutex<Option<GetRecordIdentityPauseBarriers>>>,
);

impl CreateCursorIdentityPause {
    pub(in crate::engine::db_handler) fn install(
        &self,
        reached: Arc<tokio::sync::Barrier>,
        resume: Arc<tokio::sync::Barrier>,
    ) {
        *self.0.lock().expect("create-cursor pause mutex poisoned") = Some((reached, resume));
    }

    pub(in crate::engine::db_handler) async fn pause(&self) {
        let pause = self
            .0
            .lock()
            .expect("create-cursor pause mutex poisoned")
            .take();
        if let Some((reached, resume)) = pause {
            reached.wait().await;
            resume.wait().await;
        }
    }
}

type BeginPauseBarriers = (Arc<tokio::sync::Barrier>, Arc<tokio::sync::Barrier>);

type FetchIdentityPauseBarriers = GetRecordIdentityPauseBarriers;

#[derive(Default, Clone)]
pub(in crate::engine::db_handler) struct FetchIdentityPause(
    Arc<std::sync::Mutex<Option<FetchIdentityPauseBarriers>>>,
);

impl FetchIdentityPause {
    pub(in crate::engine::db_handler) fn install(
        &self,
        reached: Arc<tokio::sync::Barrier>,
        resume: Arc<tokio::sync::Barrier>,
    ) {
        *self.0.lock().expect("fetch pause mutex poisoned") = Some((reached, resume));
    }

    pub(in crate::engine::db_handler) async fn pause(&self) {
        let pause = self.0.lock().expect("fetch pause mutex poisoned").take();
        if let Some((reached, resume)) = pause {
            reached.wait().await;
            resume.wait().await;
        }
    }
}

#[derive(Default, Clone)]
pub(in crate::engine::db_handler) struct BeginPause {
    state: Arc<std::sync::Mutex<Option<BeginPauseBarriers>>>,
    get_record_identity: GetRecordIdentityPause,
    create_cursor_identity: CreateCursorIdentityPause,
    fetch_identity: FetchIdentityPause,
    fetch_before_state_lock: FetchIdentityPause,
}

impl BeginPause {
    pub(in crate::engine::db_handler) fn install(
        &self,
        reached: Arc<tokio::sync::Barrier>,
        resume: Arc<tokio::sync::Barrier>,
    ) {
        *self.state.lock().expect("begin pause mutex poisoned") = Some((reached, resume));
    }

    pub(in crate::engine::db_handler) async fn pause(&self) {
        let pause = self
            .state
            .lock()
            .expect("begin pause mutex poisoned")
            .take();
        if let Some((reached, resume)) = pause {
            reached.wait().await;
            resume.wait().await;
        }
    }
}

impl ShamirDbHandler {
    /// Install this handler instance's one-shot pause after GetRecord's pre-check.
    #[cfg(test)]
    pub(in crate::engine::db_handler) fn with_get_record_identity_pause(
        self,
        reached: Arc<tokio::sync::Barrier>,
        resume: Arc<tokio::sync::Barrier>,
    ) -> Self {
        self.begin_pause
            .get_record_identity
            .install(reached, resume);
        self
    }

    /// Pause GetRecord once after its identity pre-check.
    #[cfg(test)]
    pub(in crate::engine::db_handler) async fn get_record_identity_pause(&self) {
        self.begin_pause.get_record_identity.pause().await;
    }

    /// Install this handler instance's one-shot pause after CreateCursor acquires its table.
    #[cfg(test)]
    pub(in crate::engine::db_handler) fn with_create_cursor_identity_pause(
        self,
        reached: Arc<tokio::sync::Barrier>,
        resume: Arc<tokio::sync::Barrier>,
    ) -> Self {
        self.begin_pause
            .create_cursor_identity
            .install(reached, resume);
        self
    }

    /// Pause CreateCursor once after table acquisition and before identity re-check.
    #[cfg(test)]
    pub(in crate::engine::db_handler) async fn create_cursor_identity_pause(&self) {
        self.begin_pause.create_cursor_identity.pause().await;
    }

    /// Install this handler instance's one-shot pause after FetchNext acquires its table.
    #[cfg(test)]
    pub(in crate::engine::db_handler) fn with_fetch_identity_pause(
        self,
        reached: Arc<tokio::sync::Barrier>,
        resume: Arc<tokio::sync::Barrier>,
    ) -> Self {
        self.begin_pause.fetch_identity.install(reached, resume);
        self
    }

    /// Pause FetchNext once after table acquisition and before identity re-check.
    #[cfg(test)]
    pub(in crate::engine::db_handler) async fn fetch_identity_pause(&self) {
        self.begin_pause.fetch_identity.pause().await;
    }

    /// Install a pause after FetchNext lookup and before it acquires cursor state.
    #[cfg(test)]
    pub(in crate::engine::db_handler) fn with_fetch_before_state_lock_pause(
        self,
        reached: Arc<tokio::sync::Barrier>,
        resume: Arc<tokio::sync::Barrier>,
    ) -> Self {
        self.begin_pause
            .fetch_before_state_lock
            .install(reached, resume);
        self
    }

    #[cfg(test)]
    pub(in crate::engine::db_handler) async fn fetch_before_state_lock_pause(&self) {
        self.begin_pause.fetch_before_state_lock.pause().await;
    }

    /// Reads the raw-native-path decode counter (test-only reader).
    #[cfg(test)]
    pub(in crate::engine::db_handler) fn decoded_request_count(&self) -> u64 {
        self.decoded_requests
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Install a one-shot pause before this handler's cursor removal.
    pub(in crate::engine::db_handler) fn with_cursor_remove_pause(
        self,
        id: u64,
        reached: Arc<tokio::sync::Barrier>,
        resume: Arc<tokio::sync::Barrier>,
    ) -> Self {
        self.cursor_registry.with_remove_pause(id, reached, resume);
        self
    }

    /// Install this handler instance's one-shot pause after taking a commit context.
    pub(in crate::engine::db_handler) fn with_commit_handoff_pause(
        self,
        tx_handle: u64,
        reached: Arc<tokio::sync::Barrier>,
        resume: Arc<tokio::sync::Barrier>,
    ) -> Self {
        *self
            .commit_handoff_pause
            .lock()
            .expect("commit pause mutex poisoned") = Some((tx_handle, reached, resume));
        self
    }

    /// Pause the matching commit once, then clear the per-handler seam.
    pub(in crate::engine::db_handler) async fn pause_commit_after_take(&self, tx_handle: u64) {
        let pause = {
            let mut configured = self
                .commit_handoff_pause
                .lock()
                .expect("commit pause mutex poisoned");
            if configured
                .as_ref()
                .is_some_and(|(configured_handle, _, _)| *configured_handle == tx_handle)
            {
                configured.take()
            } else {
                None
            }
        };
        if let Some((_, reached, resume)) = pause {
            reached.wait().await;
            resume.wait().await;
        }
    }
}

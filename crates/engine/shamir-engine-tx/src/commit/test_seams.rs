#[cfg(any(test, feature = "test-util"))]
use std::sync::atomic::{AtomicBool, AtomicU64};
#[cfg(any(test, feature = "test-util"))]
use tokio::sync::Notify;

#[cfg(any(test, feature = "test-util"))]
#[derive(Default)]
pub struct JournalPublishHook {
    pub broadcast_armed: AtomicBool,
    pub init_failures: AtomicU64,
    pub init_snapshot_armed: AtomicBool,
    pub reached: Notify,
    pub resume: Notify,
}

#[cfg(any(test, feature = "test-util"))]
pub struct TableInitHook {
    pub entered: Notify,
    pub resume: Notify,
    fail: bool,
}

#[cfg(any(test, feature = "test-util"))]
impl TableInitHook {
    pub fn new(fail: bool) -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self {
            entered: Notify::new(),
            resume: Notify::new(),
            fail,
        })
    }

    pub async fn run(&self) -> shamir_storage::error::DbResult<()> {
        if self.fail {
            return Err(shamir_storage::error::DbError::Internal(
                "B09 injected setup failure".into(),
            ));
        }
        self.entered.notify_one();
        self.resume.notified().await;
        Ok(())
    }
}

#[cfg(any(test, feature = "test-util"))]
impl TableInitHook {
    pub(crate) async fn wait_entered(&self) {
        self.entered.notified().await;
    }
    pub(crate) fn resume(&self) {
        self.resume.notify_one();
    }
}

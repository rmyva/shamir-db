//! Test-only seam: lets a test install a [`ChangefeedHandle`] BEFORE a disk
//! repo's cold reconciliation lazily initialises its own feed. Consumed by
//! `RepoInstance::from_factory_with_test_segment_bytes`. nextest runs each
//! test in its own process, so a single-slot registry is race-free.

#[cfg(all(test, feature = "fjall"))]
use std::sync::Mutex;

#[cfg(all(test, feature = "fjall"))]
use crate::repo::repo_instance::ChangefeedHandle;

#[cfg(all(test, feature = "fjall"))]
static PENDING: Mutex<Option<(String, ChangefeedHandle)>> = Mutex::new(None);

/// Register a handle for the repo named `name`; consumed at that repo's next
/// `from_factory_with_test_segment_bytes` open, before recovery runs.
#[cfg(all(test, feature = "fjall"))]
pub(crate) fn install(name: String, handle: ChangefeedHandle) {
    *PENDING.lock().unwrap() = Some((name, handle));
}

/// Take the handle registered for `name`, if any (consuming it).
#[cfg(all(test, feature = "fjall"))]
pub(crate) fn take(name: &str) -> Option<ChangefeedHandle> {
    let mut pending = PENDING.lock().unwrap();
    match pending.as_ref() {
        Some((registered, _)) if registered == name => pending.take().map(|(_, handle)| handle),
        _ => None,
    }
}

use std::sync::Arc;

use super::truncation_pin::TruncationPin;

/// Keeps a floor live until drop, including cancellation and panic unwind.
/// Widened to `pub` in R2a-1d: the minimal per-repo public acquisition
/// (`RepoInstance::acquire_truncation_pin`, R2a-3 server-adapter facade)
/// hands this guard to a caller outside shamir-engine; release stays RAII
/// (drop, exactly once, including cancellation and unwind).
#[must_use = "dropping the guard releases retained WAL history"]
pub struct TruncationPinGuard {
    pub(super) registry: Arc<TruncationPin>,
    pub(super) id: u64,
}

impl Drop for TruncationPinGuard {
    fn drop(&mut self) {
        self.registry.release_pin(self.id);
    }
}

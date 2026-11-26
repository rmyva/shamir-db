//! `CommitAdmissionCloseOwner` — RAII close right (design section 2,
//! "Close"; fail-closed recovery (iv)): created before any suspension
//! point of the seal, reopens on drop — ordinary error, timeout,
//! cancellation or unwind. The ACTIVE count is preserved across
//! close/reopen.

use std::sync::Arc;
use std::time::Duration;

use super::error::CommitAdmissionError;
use super::gate::CommitAdmissionGate;

/// Held only after `close()` observed zero pre-close-admitted leases.
/// Drop reopens the gate (clears CLOSED, then CLOSE_TAKEN) and wakes the
/// parked leaders. Uniqueness is guaranteed by the CLOSE_TAKEN bit: a
/// second `close()` got `CommitAdmissionError::Busy` and never received an
/// owner.
pub struct CommitAdmissionCloseOwner {
    gate: Arc<CommitAdmissionGate>,
}

impl CommitAdmissionCloseOwner {
    pub(crate) fn new(gate: Arc<CommitAdmissionGate>) -> Self {
        Self { gate }
    }

    /// Await zero pre-close-admitted leases (bounded by `cap`;
    /// `Duration::MAX` = unbounded). Cancel-safe: a cancelled wait leaves
    /// the gate closed but owned — the owner's drop reopens.
    pub(crate) async fn wait_zero(&self, cap: Duration) -> Result<(), CommitAdmissionError> {
        self.gate.wait_zero(cap).await
    }
}

impl Drop for CommitAdmissionCloseOwner {
    fn drop(&mut self) {
        self.gate.reopen();
    }
}

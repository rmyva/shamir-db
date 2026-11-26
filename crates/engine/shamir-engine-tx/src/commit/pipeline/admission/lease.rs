//! `CommitAdmissionLease` — RAII proof of admission (design section 2,
//! "Release"). Owned, `Send` (it moves with the commit continuation:
//! `CommitDriver`'s suspended-future transfer on drop, the AsyncIndex tail
//! handoff, the separately spawned panic cleanup).

use std::sync::Arc;

use super::gate::CommitAdmissionGate;

/// Constructed synchronously by the successful admission CAS — before any
/// `.await` or side effect — so cancellation after a successful CAS still
/// runs `Drop` at the abort point and releases the count.
///
/// Drop: releases one count through the gate; reaching zero ACTIVE wakes the armed waiters
/// (a close owner parked on zero, and nothing else — a final decrement is
/// the only event a zero-waiter needs). `Release` publishes everything the
/// leader did while admitted to the next `Acquire` observer of the word.
pub(crate) struct CommitAdmissionLease {
    gate: Arc<CommitAdmissionGate>,
}

impl CommitAdmissionLease {
    pub(crate) fn new(gate: Arc<CommitAdmissionGate>) -> Self {
        Self { gate }
    }
}

impl Drop for CommitAdmissionLease {
    fn drop(&mut self) {
        self.gate.release();
    }
}

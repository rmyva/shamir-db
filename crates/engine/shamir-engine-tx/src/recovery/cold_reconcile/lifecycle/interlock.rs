//! Synchronous publication/termination interlock.
//!
//! The mutex spans the actual publication callback: termination waits for
//! an in-flight callback to finish, then becomes sticky. Callbacks must be
//! synchronous and must not block waiting for async work or re-enter lifecycle operations.

use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(test)]
use std::sync::TryLockError;
use std::sync::{Mutex, MutexGuard};

/// Returned when termination has closed publication.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublicationClosed;

/// Serializes publication callbacks with sticky termination.
#[derive(Default)]
pub struct PubTermInterlock {
    gate: Mutex<()>,
    terminated: AtomicBool,
}

impl PubTermInterlock {
    /// Creates an open interlock.
    pub const fn new() -> Self {
        Self {
            gate: Mutex::new(()),
            terminated: AtomicBool::new(false),
        }
    }

    fn lock(&self) -> MutexGuard<'_, ()> {
        self.gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Runs the synchronous publication callback while holding the gate.
    /// Callback panic releases the guard via RAII; termination cannot return until the callback completes.
    pub fn publish<T>(&self, callback: impl FnOnce() -> T) -> Result<T, PublicationClosed> {
        let _gate = self.lock();
        if self.terminated.load(Ordering::Acquire) {
            return Err(PublicationClosed);
        }
        Ok(callback())
    }

    /// Sticky termination; waits for any callback already holding the gate.
    pub fn terminate(&self) -> bool {
        let _gate = self.lock();
        !self.terminated.swap(true, Ordering::AcqRel)
    }

    /// Returns whether publication has been terminated.
    pub fn is_terminated(&self) -> bool {
        self.terminated.load(Ordering::Acquire)
    }

    /// Test-only wrapper reports real gate contention before normal termination.
    #[cfg(test)]
    pub(crate) fn terminate_after_observed_contention_for_test(
        &self,
        on_contention: impl FnOnce(),
    ) -> bool {
        match self.gate.try_lock() {
            Ok(_gate) => !self.terminated.swap(true, Ordering::AcqRel),
            Err(TryLockError::Poisoned(poisoned)) => {
                let _gate = poisoned.into_inner();
                !self.terminated.swap(true, Ordering::AcqRel)
            }
            Err(TryLockError::WouldBlock) => {
                on_contention();
                self.terminate()
            }
        }
    }

    /// Test-only negative control omits only waiting for the publication gate.
    #[cfg(test)]
    pub(crate) fn nonwaiting_termination_for_test(&self) -> bool {
        !self.terminated.swap(true, Ordering::AcqRel)
    }
}

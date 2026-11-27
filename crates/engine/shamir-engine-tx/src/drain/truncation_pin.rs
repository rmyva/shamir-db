//! Per-repository WAL pins and deletion admission share one immutable CAS state.
//!
//! A pin is published only when no deletion lease exists. A deletion samples the
//! exact live minimum AND increments its lease count in the same pointer-identity
//! CAS. Thus either the pin wins (the delete is clamped), or the delete wins (pin
//! acquisition yields until it finishes). There is no stale-ceiling window.
//!
//! `None` means no pins; `Some(0)` retains all history. WAL deletion is inclusive,
//! so a positive floor permits deletion only through `floor - 1`.
//!
//! Admission and synchronous RAII cleanup use ArcSwap CAS retries, never a mutex
//! or rwlock. A failed CAS implies another state update made progress. This is
//! lock-free admission, not wait-free acquisition; filesystem I/O retains its
//! existing WAL synchronization. Commit does not touch this registry.

use std::sync::Arc;

use arc_swap::ArcSwap;
use shamir_storage::error::{DbError, DbResult};

use super::truncation_lease::TruncationLease;
use super::truncation_pin_guard::TruncationPinGuard;

#[derive(Clone, Default)]
struct State {
    pins: Vec<(u64, u64)>,
    minimum: Option<u64>,
    next_id: u64,
    leases: usize,
}

pub struct TruncationPin {
    state: ArcSwap<State>,
}

impl TruncationPin {
    pub fn new() -> Self {
        Self {
            state: ArcSwap::from_pointee(State::default()),
        }
    }
}

impl Default for TruncationPin {
    fn default() -> Self {
        Self::new()
    }
}

impl TruncationPin {
    /// Retains versions >= floor after return; cannot resurrect already deleted
    /// history. cancel-safe: yes — no publication before the final synchronous
    /// CAS/guard return, and waiting owns no registry entry.
    pub(crate) async fn acquire(self: &Arc<Self>, floor: u64) -> DbResult<TruncationPinGuard> {
        loop {
            let current = self.state.load_full();
            if current.leases != 0 {
                tokio::task::yield_now().await;
                continue;
            }
            let id = current.next_id;
            let next_id = id
                .checked_add(1)
                .ok_or_else(|| DbError::Internal("WAL truncation pin id exhausted".into()))?;
            let mut next = (*current).clone();
            next.pins.push((id, floor));
            next.minimum = Some(current.minimum.map_or(floor, |min| min.min(floor)));
            next.next_id = next_id;
            if self.replace(&current, next) {
                return Ok(TruncationPinGuard {
                    registry: Arc::clone(self),
                    id,
                });
            }
            tokio::task::yield_now().await;
        }
    }

    /// Call only at the deletion site, after checkpoint/flush awaits. None means
    /// no positive deletion ceiling (including a floor-zero pin).
    pub(crate) fn try_begin_truncate(self: &Arc<Self>, ceiling: u64) -> Option<TruncationLease> {
        loop {
            let current = self.state.load_full();
            let ceiling = current
                .minimum
                .map_or(ceiling, |floor| ceiling.min(floor.saturating_sub(1)));
            if ceiling == 0 {
                return None;
            }
            let mut next = (*current).clone();
            next.leases = next.leases.checked_add(1)?;
            if self.replace(&current, next) {
                return Some(TruncationLease {
                    registry: Arc::clone(self),
                    ceiling,
                });
            }
        }
    }

    fn replace(&self, current: &Arc<State>, next: State) -> bool {
        let previous = self.state.compare_and_swap(current, Arc::new(next));
        Arc::ptr_eq(&previous, current)
    }

    pub(super) fn release_pin(&self, id: u64) {
        loop {
            let current = self.state.load_full();
            let mut next = (*current).clone();
            next.pins.retain(|(live_id, _)| *live_id != id);
            next.minimum = next.pins.iter().map(|(_, floor)| *floor).min();
            if self.replace(&current, next) {
                return;
            }
        }
    }

    pub(super) fn release_lease(&self) {
        loop {
            let current = self.state.load_full();
            let mut next = (*current).clone();
            // Every lease is minted by the successful incrementing CAS; neither
            // pins nor leases are Clone. No fallible/async cleanup during unwind.
            next.leases -= 1;
            if self.replace(&current, next) {
                return;
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn effective_floor(&self) -> Option<u64> {
        self.state.load().minimum
    }
}

#[cfg(test)]
#[path = "../tests/drain/truncation_pin/mod.rs"]
mod tests;

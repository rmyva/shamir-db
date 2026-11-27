use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use super::super::Drainer;

pub(in crate::drain::drainer) fn record_orphan_failure(
    drainer: &Drainer,
    failed: &mut bool,
    error: impl std::fmt::Display,
) {
    if *failed {
        return;
    }
    *failed = true;
    let mut error = error.to_string();
    let mut end = error.len().min(1024);
    while !error.is_char_boundary(end) {
        end -= 1;
    }
    error.truncate(end);
    error.shrink_to_fit();
    drainer
        .orphan_replay_last_error
        .store(Some(Arc::new(error)));
    drainer
        .orphan_replay_failures_total
        .fetch_add(1, Ordering::Relaxed);
    drainer
        .orphan_replay_consecutive_failures
        .fetch_add(1, Ordering::Relaxed);
}

#[derive(Default)]
pub struct Metrics {
    repaired: AtomicU64,
    gaps: AtomicU64,
}

impl Metrics {
    pub(in crate::drain::drainer) fn repaired(&self) {
        self.repaired.fetch_add(1, Ordering::Relaxed);
    }

    pub(in crate::drain::drainer) fn gap(&self) {
        self.gaps.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> (u64, u64) {
        (
            self.repaired.load(Ordering::Relaxed),
            self.gaps.load(Ordering::Relaxed),
        )
    }
}

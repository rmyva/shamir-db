use std::sync::Arc;
use std::time::{Duration, Instant};

use super::super::harness::{ShellFixture, TestRuntime};
use crate::shell_tests::runtime::DEADLINE_SLACK;

/// Prove holder availability and stop/join the worker within the shared deadline.
/// Errors are `"holder mutex teardown deadline"` for a lock timeout and
/// `"runtime worker must stop and join before common deadline"` for any worker
/// stop/join failure; shutdown timeout versus panic details are intentionally lost.
pub fn teardown<T: Send + 'static>(
    runtime: &mut TestRuntime,
    holder: Arc<tokio::sync::Mutex<T>>,
    deadline: Instant,
) -> Result<(), &'static str> {
    let cap = deadline.saturating_duration_since(Instant::now());
    if cap.is_zero() {
        return Err("holder mutex teardown deadline");
    }
    // The inner lock timeout fires at the absolute deadline; the outer bound
    // carries a slack so the typed error always wins the race against it.
    // A wait that escapes the inner timeout still panics at deadline + slack.
    runtime.run_timeout(cap + DEADLINE_SLACK, async move {
        let _holder = tokio::time::timeout(
            deadline.saturating_duration_since(Instant::now()),
            holder.lock(),
        )
        .await
        .map_err(|_| "holder mutex teardown deadline")?;
        Ok::<(), &'static str>(())
    })?;
    runtime
        .shutdown_until(deadline)
        .map_err(|_| "runtime worker must stop and join before common deadline")
}

/// Shut down the fixture, then prove holder availability and stop/join under one deadline.
/// The typed errors are `"holder mutex teardown deadline"` for lock timeout and
/// `"runtime worker must stop and join before common deadline"` for worker timeout
/// or panic; the distinction from `shutdown_until` is intentionally lost.
pub fn finish_until<T: Send + 'static>(
    fixture: &mut ShellFixture,
    holder: Arc<tokio::sync::Mutex<T>>,
    deadline: Instant,
) {
    let cap = deadline.saturating_duration_since(Instant::now());
    assert!(
        cap > Duration::ZERO,
        "common deadline expired before fixture shutdown"
    );
    // An overrun panics here so the success path fails on the common deadline;
    // holder/worker teardown then uses TestRuntime::drop's capped fallback.
    fixture.shutdown_with_deadline(deadline);
    if let Err(message) = teardown(&mut fixture.rt, holder, deadline) {
        panic!("{message}");
    }
}

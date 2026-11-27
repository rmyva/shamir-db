//! Cold-open registration survives a runtime discarding a queued open before
//! its first poll.
//!
//! Mutant killed: `spawn_registered` constructing the live count INSIDE the
//! async body instead of moving the registration taken by `register()` into
//! the future — a task dropped by runtime shutdown before its first poll
//! would then never decrement the count, and `drain` would time out with no
//! task left.

use std::sync::mpsc;
use std::sync::Arc;
use std::time::Duration;

use crate::tx::commit::cold_open::ColdOpenTasks;

/// Bounded wait floor: waits complete through handshakes (channel recv /
/// `drain`'s idle notify), never a sleep; a MUTANT drain settles only at
/// this bound and then fails the assertion.
const BOUND: Duration = Duration::from_secs(120);

#[test]
fn queued_open_dropped_by_runtime_shutdown_does_not_leak_the_live_count() {
    // Single worker: a task physically blocked on an OS channel occupies the
    // only worker thread, so the open spawned below stays QUEUED and is
    // never polled before the runtime drops it.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("blocked runtime");
    let tasks = Arc::new(ColdOpenTasks::new());
    let (parked_tx, parked_rx) = mpsc::channel::<()>();
    let (unblock_tx, unblock_rx) = mpsc::channel::<()>();
    // The handle is dropped (the task detached) on purpose: it is never
    // awaited or polled again before the runtime drops it.
    let _detached = rt.handle().spawn(async move {
        let _ = parked_tx.send(());
        // Deliberately BLOCKING inside the task: keeps the worker thread
        // busy so the queued open cannot reach its first poll.
        let _: Result<(), std::sync::mpsc::RecvError> = unblock_rx.recv();
    });
    parked_rx
        .recv()
        .expect("the blocker must park the worker thread (handshake)");

    // Register and spawn from inside a runtime context (the production
    // handoff requires one). The queued future's body never runs.
    let handle = rt.handle().clone();
    let queued_tasks = Arc::clone(&tasks);
    handle.block_on(async {
        let registration = queued_tasks.register().expect("admission before any close");
        assert!(
            queued_tasks.spawn_registered(registration, async {
                // Never polled; a handshake nobody sends keeps the body
                // unrun and makes a stray poll visible as a hang bounded
                // by the test's overall bound.
                let (_done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();
                let _ = done_rx.await;
            }),
            "the queued open must be admitted",
        );
    });

    // Runtime shutdown drops the queued (never polled) open.
    drop(handle);
    rt.shutdown_background();
    let _ = unblock_tx.send(());

    // The dropped registration must already have settled the count: the
    // drain returns without waiting for the bound. Mutant: the count stays
    // 1 forever and the drain times out at the bound.
    let drain_tasks = Arc::clone(&tasks);
    let rt2 = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("assertion runtime");
    let drained = rt2.block_on(async {
        tokio::time::timeout(
            BOUND,
            drain_tasks.drain(tokio::time::Instant::now() + BOUND),
        )
        .await
        .expect("drain bounded")
    });
    assert!(
        drained,
        "the live count leaked: a queued open dropped by runtime shutdown before its \
         first poll never released its registration"
    );
}

/// Registration is atomic with the close latch: after `close`, a fresh open
/// is refused BEFORE any side effect (`register` returns `None`).
#[test]
fn register_refuses_after_close() {
    let tasks = ColdOpenTasks::new();
    tasks.close();
    assert!(tasks.is_closed());
    assert!(tasks.register().is_none(), "closed admission must refuse");
}

/// The close latch spans one shutdown window: `reopen` lifts it, so a
/// repository flushed mid-life (embedders, tests) admits fresh cold opens
/// again. Mutant: `shutdown_clean` leaving the latch closed after the flush.
#[test]
fn reopen_lifts_the_close_latch() {
    let tasks = ColdOpenTasks::new();
    tasks.close();
    assert!(tasks.register().is_none());
    tasks.reopen();
    assert!(!tasks.is_closed());
    assert!(
        tasks.register().is_some(),
        "reopened admission must admit a fresh open"
    );
}

//! Release-tail registration survives a runtime discarding a queued tail
//! before its first poll.
//!
//! Mutant killed: `spawn_lossless` constructing `LiveGuard` INSIDE the async
//! body instead of moving a pre-built guard into the future — a task dropped
//! by runtime shutdown before its first poll would never decrement the live
//! count, and `drain` would time out with no task left.

use std::sync::mpsc;
use std::sync::Arc;
use std::time::Duration;

use crate::sync::durability::tx::ReleaseTails;

/// Bounded wait floor: waits complete through handshakes (channel recv /
/// `drain`'s idle notify), never a sleep; a MUTANT drain settles only at
/// this bound and then fails the assertion.
const BOUND: Duration = Duration::from_secs(120);

#[test]
fn queued_tail_dropped_by_runtime_shutdown_does_not_leak_the_live_count() {
    // Single worker: a task physically blocked on an OS channel occupies the
    // only worker thread, so the tail spawned below stays QUEUED and is
    // never polled before the runtime drops it.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("blocked runtime");
    let tails = Arc::new(ReleaseTails::new());
    let (parked_tx, parked_rx) = mpsc::channel::<()>();
    let (unblock_tx, unblock_rx) = mpsc::channel::<()>();
    // The handle is dropped (the task detached) on purpose: it is never
    // awaited or polled again before the runtime drops it.
    let _detached = rt.handle().spawn(async move {
        let _ = parked_tx.send(());
        // Deliberately BLOCKING inside the task: keeps the worker thread
        // busy so the queued tail cannot reach its first poll.
        let _: Result<(), std::sync::mpsc::RecvError> = unblock_rx.recv();
    });
    parked_rx
        .recv()
        .expect("the blocker must park the worker thread (handshake)");

    // Spawn from inside a runtime context (`tokio::spawn` requires one).
    // The queued future's body never runs.
    let handle = rt.handle().clone();
    let queued_tails = Arc::clone(&tails);
    handle.block_on(async {
        let queued = async {
            // Never polled; an un_SENT oneshot keeps the body unrun and
            // makes a stray poll visible as a hang bounded by BOUND.
            let (_done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();
            let _ = done_rx.await;
        };
        assert!(
            queued_tails.spawn_lossless(queued).is_ok(),
            "the queued tail must be admitted",
        );
    });

    // Runtime shutdown drops the queued (never polled) tail.
    drop(handle);
    rt.shutdown_background();
    let _ = unblock_tx.send(());

    // The dropped guard must already have settled the count: the drain
    // returns without waiting for the bound. Mutant: the count stays 1
    // forever and the drain times out at the bound.
    let drain_tails = Arc::clone(&tails);
    let rt2 = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("assertion runtime");
    let drained = rt2.block_on(async {
        tokio::time::timeout(
            BOUND,
            drain_tails.drain(tokio::time::Instant::now() + BOUND),
        )
        .await
        .expect("drain bounded")
    });
    assert!(
        drained,
        "the live count leaked: a queued tail dropped by runtime shutdown before its \
         first poll never released its guard"
    );
}

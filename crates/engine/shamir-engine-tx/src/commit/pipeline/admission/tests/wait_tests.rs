//! Deterministic handshake tests: armed-notify waits at the close-vs-zero
//! and close-vs-acquire boundaries, via poll-based pending checks — no
//! sleeps as correctness evidence.

// Test-only module: compiled empty when the crate is built with `test-util` alone.
#![cfg(test)]

use std::future::Future;
use std::pin::Pin;
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
#[cfg(test)]
use std::sync::Arc;

#[cfg(test)]
use super::close_helpers::close;
use std::task::Poll;
#[cfg(test)]
use std::time::Duration;

#[cfg(test)]
use tokio::sync::Barrier;

#[cfg(test)]
use super::super::error::CommitAdmissionError;
#[cfg(test)]
use super::super::gate::CommitAdmissionGate;

/// A generous cap for tests that PROVE the wait completes (the cap must not
/// fire before the handshake does); refusals-at-cap are covered with a ZERO
/// cap in `gate_tests`.
const HANDSHAKE_CAP: Duration = Duration::from_secs(60);

/// Racing acquirers released against the gate while a close is in flight.
const RACERS: usize = 4;
/// Bounded retry budget per racer so a racer cannot starve the closer.
const RACER_ATTEMPTS: usize = 64;

/// Deterministic "not ready yet": poll once; returns true iff Pending.
async fn poll_pending_once<F: Future>(mut fut: Pin<&mut F>) -> bool {
    std::future::poll_fn(|cx| match fut.as_mut().poll(cx) {
        Poll::Pending => Poll::Ready(true),
        Poll::Ready(_) => Poll::Ready(false),
    })
    .await
}

#[tokio::test]
async fn leader_waits_closed_then_admits_after_reopen() {
    let gate = Arc::new(CommitAdmissionGate::default());
    let owner = close(&gate).await.expect("close");

    let gate_for_leader = Arc::clone(&gate);
    let mut leader = Box::pin(gate_for_leader.acquire_leader(HANDSHAKE_CAP));
    // The leader is parked on the armed notify — deterministically pending
    // while the gate stays closed.
    assert!(poll_pending_once(leader.as_mut()).await);

    drop(owner); // reopen wakes the armed waiter
    let lease = leader.await.expect("leader admitted after reopen");
    assert_eq!(gate.active_count(), 1);
    drop(lease);
    assert_eq!(gate.active_count(), 0);
}

#[tokio::test]
async fn close_waits_for_pre_close_leases_then_zero_gap_free() {
    let gate = Arc::new(CommitAdmissionGate::default());
    let lease = gate.try_acquire().expect("admit");

    let gate_for_closer = Arc::clone(&gate);
    let mut closer = Box::pin(close(&gate_for_closer));
    // The close owner must NOT be handed out while a pre-close lease lives:
    // the close is parked on the armed zero-notify.
    assert!(poll_pending_once(closer.as_mut()).await);
    assert_eq!(gate.active_count(), 1);

    drop(lease); // final decrement wakes the close's armed waiter
    let owner = closer.await.expect("close completes once leases hit zero");
    assert!(gate.is_closed());
    drop(owner);
    assert!(!gate.is_closed());
    assert_eq!(gate.active_count(), 0);
}

#[tokio::test]
async fn close_vs_acquire_boundary_never_leaves_an_uncounted_lease() {
    // The close-vs-acquire boundary is decided by the state word ATOMICALLY:
    // acquirers released REALLY concurrently while a close is in flight each
    // end EITHER counted-before-CLOSED (Ok — the lease is dropped at once) OR
    // refused-after (`Err(Closed)`) — never anything else — and no lease can
    // slip through UNCOUNTED: the close completes only at ACTIVE == 0 with
    // the owner still held.
    let gate = Arc::new(CommitAdmissionGate::default());
    let lease = gate.try_acquire().expect("admit");

    let barrier = Arc::new(Barrier::new(RACERS + 1));
    let mut racers = Vec::new();
    for _ in 0..RACERS {
        let racer_gate = Arc::clone(&gate);
        let racer_barrier = Arc::clone(&barrier);
        racers.push(tokio::spawn(async move {
            racer_barrier.wait().await;
            let counted = AtomicUsize::new(0);
            let mut refused = false;
            for _ in 0..RACER_ATTEMPTS {
                match racer_gate.try_acquire() {
                    Ok(counted_lease) => {
                        counted.fetch_add(1, Ordering::AcqRel);
                        drop(counted_lease);
                    }
                    Err(CommitAdmissionError::Closed) => {
                        refused = true;
                        break;
                    }
                    Err(other) => panic!("unexpected admission outcome: {other:?}"),
                }
                // Let the closer (and the other racers) make progress.
                tokio::task::yield_now().await;
            }
            (counted.load(Ordering::Acquire), refused)
        }));
    }

    // One barrier release starts the racers AND the close together: the
    // CLOSED transition now races live `try_acquire` calls, not scripted
    // sequential ones.
    barrier.wait().await;
    drop(lease);
    let closer_gate = Arc::clone(&gate);
    let owner = close(&closer_gate).await.expect("close completes");
    // Word-level invariant: closed AND zero counted leases — nothing
    // slipped through uncounted.
    assert_eq!(gate.active_count(), 0);
    assert!(gate.is_closed());
    // While CLOSED, admission is refused outright.
    assert!(matches!(
        gate.try_acquire(),
        Err(CommitAdmissionError::Closed)
    ));
    for racer in racers {
        let (counted, refused) = racer.await.expect("racer join");
        // Every racer outcome is exactly one of: counted-before-close (the
        // lease was dropped at once, so the close could complete) or refused.
        assert!(
            refused || counted > 0,
            "racer recorded neither a counted nor a refused outcome"
        );
    }
    drop(owner); // reopen
    assert!(!gate.is_closed());
    assert_eq!(gate.active_count(), 0);
}

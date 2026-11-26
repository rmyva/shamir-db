//! Gate-level state-machine tests: admit/release counts, close single
//! ownership, follower-style immediate refusal, reopen leak-freedom.

// Test-only module: compiled empty when the crate is built with `test-util` alone.
#![cfg(test)]

#[cfg(test)]
use std::sync::Arc;
#[cfg(test)]
use std::time::Duration;

#[cfg(test)]
use super::super::error::CommitAdmissionError;
#[cfg(test)]
use super::super::gate::{CommitAdmissionGate, ADMISSION_REOPEN_WAIT_CAP};
#[cfg(test)]
use super::super::lease::CommitAdmissionLease;
#[cfg(test)]
use super::close_helpers::close;

#[tokio::test]
async fn admit_release_counts_are_exact_and_reopen_leaks_no_count() {
    let gate = Arc::new(CommitAdmissionGate::default());
    assert_eq!(gate.active_count(), 0);

    let lease = gate.try_acquire().expect("open gate admits");
    assert_eq!(gate.active_count(), 1);
    drop(lease);
    assert_eq!(gate.active_count(), 0);

    // Reopen after error/drop leaks no count: a full close/reopen cycle
    // returns the gate to open with zero active leases.
    let owner = close(&gate).await.expect("first close owns");
    assert!(gate.is_closed());
    drop(owner);
    assert!(!gate.is_closed());
    assert_eq!(gate.active_count(), 0);
    let lease = gate.try_acquire().expect("gate admits again after reopen");
    assert_eq!(gate.active_count(), 1);
    drop(lease);
    assert_eq!(gate.active_count(), 0);
}

#[tokio::test]
async fn closed_gate_refuses_immediately_without_side_effect() {
    let gate = Arc::new(CommitAdmissionGate::default());
    let owner = close(&gate).await.expect("close with zero leases at once");

    // Follower-style refusal (Q13): typed, immediate, no count change.
    assert_eq!(gate.try_acquire().err(), Some(CommitAdmissionError::Closed));
    assert_eq!(gate.active_count(), 0);

    // Leader-style refusal past the cap: a ZERO reopen-wait cap is already
    // exhausted, so the bounded wait returns the typed error immediately —
    // no timer, no side effect.
    assert_eq!(
        gate.acquire_leader(Duration::ZERO).await.err(),
        Some(CommitAdmissionError::Closed)
    );
    assert_eq!(gate.active_count(), 0);

    // Dropping the owner reopens; admission works again.
    drop(owner);
    assert!(gate.try_acquire().is_ok());
}

#[tokio::test]
async fn second_closer_gets_busy_and_owner_drop_reopens() {
    let gate = Arc::new(CommitAdmissionGate::default());
    let owner = close(&gate).await.expect("first close owns");
    assert_eq!(close(&gate).await.err(), Some(CommitAdmissionError::Busy));

    drop(owner);
    assert!(!gate.is_closed());
    // The exclusive right is released: a fresh close succeeds.
    let owner2 = close(&gate).await.expect("close right is free again");
    assert!(gate.is_closed());
    drop(owner2);
    assert!(!gate.is_closed());
}

#[tokio::test]
async fn default_cap_is_documented_constant_and_usable() {
    // The wiring passes exactly this constant; pin it so a silent change to
    // the leader stall budget is caught.
    assert_eq!(
        ADMISSION_REOPEN_WAIT_CAP,
        std::time::Duration::from_secs(15)
    );
}

#[tokio::test]
async fn lease_is_send_and_owned() {
    // The lease must be movable into spawned continuations (CommitDriver
    // detach, AsyncIndex tail handoff, spawned panic cleanup).
    fn assert_send<T: Send>() {}
    assert_send::<CommitAdmissionLease>();
}

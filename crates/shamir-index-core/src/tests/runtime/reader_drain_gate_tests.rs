//! P0-3a (#1011): `ReaderDrainGate` unit tests — see the module's own doc
//! comment (`reader_drain_gate.rs`) for the full design rationale and
//! memory-model proof this gate implements.

use crate::reader_drain_gate::ReaderDrainGate;

#[test]
fn uncontended_enter_returns_guard() {
    let gate = ReaderDrainGate::new();
    let guard = gate.enter();
    assert!(guard.is_some());
    assert_eq!(gate.in_flight_count(), 1);
    drop(guard);
    assert_eq!(gate.in_flight_count(), 0);
}

#[test]
fn enter_backs_off_while_dropping() {
    let gate = ReaderDrainGate::new();
    let drop_guard = gate.begin_drop();
    let read_guard = gate.enter();
    assert!(
        read_guard.is_none(),
        "reader must back off while a DROP is in its raise window"
    );
    assert_eq!(
        gate.in_flight_count(),
        0,
        "backing off must not leak the increment"
    );
    drop(drop_guard);
    assert!(gate.enter().is_some(), "reader must proceed once cleared");
}

#[tokio::test]
async fn wait_for_drain_returns_immediately_with_no_readers() {
    let gate = ReaderDrainGate::new();
    let drop_guard = gate.begin_drop();
    drop_guard.wait_for_drain().await;
    assert_eq!(
        gate.drain_waits(),
        0,
        "an uncontended drain must not count as a wait"
    );
}

/// Pairs with the negative check above: a lone `drain_waits() == 0`
/// assertion passes vacuously if the counter is never wired up correctly.
/// This test proves the counter genuinely discriminates a contended drain.
#[tokio::test]
async fn wait_for_drain_waits_for_in_flight_reader() {
    let gate = ReaderDrainGate::new();
    let read_guard = gate.enter().expect("uncontended enter must succeed");

    let gate2 = gate.clone();
    let waiter = tokio::spawn(async move {
        let drop_guard = gate2.begin_drop();
        drop_guard.wait_for_drain().await;
    });

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(
        !waiter.is_finished(),
        "wait_for_drain must block while the reader is still in flight"
    );

    drop(read_guard);
    waiter.await.unwrap();
    assert_eq!(
        gate.drain_waits(),
        1,
        "a genuinely-contended drain must be counted exactly once"
    );
}

#[test]
fn drop_drain_guard_clears_flag_even_without_wait() {
    let gate = ReaderDrainGate::new();
    let drop_guard = gate.begin_drop();
    assert!(gate.enter().is_none());
    drop(drop_guard);
    assert!(
        gate.enter().is_some(),
        "dropping the guard without calling wait_for_drain must still clear the flag"
    );
}

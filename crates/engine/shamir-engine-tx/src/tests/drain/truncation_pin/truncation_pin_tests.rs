use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;
use std::task::Poll;

use futures::{pin_mut, poll};
use tokio::sync::oneshot;

use crate::tx::drain::truncation_pin::TruncationPin;

#[tokio::test]
async fn minimum_tracks_first_two_pins_and_drop_in_both_orders() {
    let pins = Arc::new(TruncationPin::new());
    assert_eq!(pins.effective_floor(), None);
    let high = pins.acquire(30).await.unwrap();
    assert_eq!(pins.effective_floor(), Some(30));
    assert_eq!(pins.try_begin_truncate(100).unwrap().ceiling(), 29);
    let low = pins.acquire(10).await.unwrap();
    assert_eq!(pins.effective_floor(), Some(10));
    assert_eq!(pins.try_begin_truncate(100).unwrap().ceiling(), 9);
    assert_eq!(pins.try_begin_truncate(5).unwrap().ceiling(), 5);
    drop(low);
    assert_eq!(pins.effective_floor(), Some(30));
    drop(high);
    assert_eq!(pins.effective_floor(), None);
    assert_eq!(pins.try_begin_truncate(100).unwrap().ceiling(), 100);

    let low = pins.acquire(10).await.unwrap();
    let high = pins.acquire(30).await.unwrap();
    drop(high);
    assert_eq!(pins.effective_floor(), Some(10));
    drop(low);
    assert_eq!(pins.effective_floor(), None);
}

#[tokio::test]
async fn zero_and_one_floors_are_not_unrestricted() {
    let pins = Arc::new(TruncationPin::new());
    for floor in [0, 1] {
        let pin = pins.acquire(floor).await.unwrap();
        assert_eq!(pins.effective_floor(), Some(floor));
        assert!(pins.try_begin_truncate(u64::MAX).is_none());
        drop(pin);
        assert!(pins.try_begin_truncate(u64::MAX).is_some());
    }
}

#[tokio::test]
async fn unwind_releases_only_panicking_holders_pin_and_lease() {
    let pins = Arc::new(TruncationPin::new());
    let high = pins.acquire(25).await.unwrap();
    let low = pins.acquire(7).await.unwrap();
    let result = catch_unwind(AssertUnwindSafe(|| {
        let _pin = low;
        let _lease = pins.try_begin_truncate(100).unwrap();
        panic!("pin holder unwind");
    }));
    assert!(result.is_err());
    assert_eq!(pins.effective_floor(), Some(25));
    // Would never complete if the unwound lease remained registered.
    let next = pins.acquire(3).await.unwrap();
    drop(next);
    drop(high);
    assert_eq!(pins.effective_floor(), None);
}

#[tokio::test]
async fn acquire_waits_for_every_deletion_lease_with_task_handshake() {
    let pins = Arc::new(TruncationPin::new());
    let first = pins.try_begin_truncate(100).unwrap();
    let second = pins.try_begin_truncate(100).unwrap();
    let worker_pins = Arc::clone(&pins);
    let (attempted_tx, attempted_rx) = oneshot::channel();
    let (retry_tx, retry_rx) = oneshot::channel();
    let (retried_tx, retried_rx) = oneshot::channel();
    let (finish_tx, finish_rx) = oneshot::channel();
    let worker = tokio::spawn(async move {
        let acquire = worker_pins.acquire(0);
        pin_mut!(acquire);
        assert!(matches!(poll!(&mut acquire), Poll::Pending));
        attempted_tx.send(()).unwrap();
        retry_rx.await.unwrap();
        assert!(matches!(poll!(&mut acquire), Poll::Pending));
        retried_tx.send(()).unwrap();
        finish_rx.await.unwrap();
        acquire.await.unwrap()
    });
    attempted_rx.await.unwrap();
    assert_eq!(pins.effective_floor(), None);
    drop(first);
    retry_tx.send(()).unwrap();
    retried_rx.await.unwrap();
    assert_eq!(pins.effective_floor(), None);
    drop(second);
    finish_tx.send(()).unwrap();
    let pin = worker.await.unwrap();
    assert_eq!(pins.effective_floor(), Some(0));
    assert!(pins.try_begin_truncate(100).is_none());
    drop(pin);
    assert!(pins.try_begin_truncate(100).is_some());
}

#[tokio::test]
async fn cancelling_waiting_acquire_leaves_no_pin() {
    let pins = Arc::new(TruncationPin::new());
    let lease = pins.try_begin_truncate(100).unwrap();
    {
        let acquire = pins.acquire(0);
        pin_mut!(acquire);
        assert!(matches!(poll!(&mut acquire), Poll::Pending));
    }
    drop(lease);
    assert_eq!(pins.effective_floor(), None);
    let pin = pins.acquire(19).await.unwrap();
    assert_eq!(pins.effective_floor(), Some(19));
    drop(pin);
    assert_eq!(pins.effective_floor(), None);
}

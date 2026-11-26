use std::sync::Arc;

use futures::{pin_mut, poll};
use shamir_storage::error::DbError;
use tokio::sync::oneshot;

use crate::tx::drain::truncation_pin::TruncationPin;

#[tokio::test]
async fn cancelled_delete_caller_cannot_release_workers_lease() {
    let pins = Arc::new(TruncationPin::new());
    let lease = pins.try_begin_truncate(100).unwrap();
    let (entered_tx, entered_rx) = oneshot::channel();
    let (resume_tx, resume_rx) = oneshot::channel();
    let caller = tokio::spawn(lease.run(async move {
        entered_tx.send(()).unwrap();
        resume_rx.await.unwrap();
        Ok(1)
    }));
    entered_rx.await.unwrap();
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    let acquire = pins.acquire(0);
    pin_mut!(acquire);
    assert!(
        poll!(&mut acquire).is_pending(),
        "worker still owns its lease"
    );
    resume_tx.send(()).unwrap();
    let pin = acquire.await.unwrap();
    assert_eq!(pins.effective_floor(), Some(0));
    drop(pin);
    assert!(pins.try_begin_truncate(100).is_some());
}

#[tokio::test]
async fn deletion_error_and_worker_panic_release_lease() {
    let pins = Arc::new(TruncationPin::new());
    let lease = pins.try_begin_truncate(100).unwrap();
    assert!(lease
        .run(async { Err(DbError::Internal("injected delete error".into())) })
        .await
        .is_err());
    let pin = pins.acquire(10).await.unwrap();
    drop(pin);
    let lease = pins.try_begin_truncate(100).unwrap();
    assert!(lease
        .run(async { panic!("injected worker unwind") })
        .await
        .is_err());
    let pin = pins.acquire(0).await.unwrap();
    assert!(pins.try_begin_truncate(100).is_none());
    drop(pin);
    assert!(pins.try_begin_truncate(100).is_some());
}

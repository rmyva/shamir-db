use crate::actor::IndexActor;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::mpsc;

#[tokio::test]
async fn applier_sees_every_op_in_order() {
    // Snap = AtomicU64; each Op adds a value.
    let actor = IndexActor::<u64, AtomicU64>::spawn(AtomicU64::new(0), |op, snap| async move {
        snap.load().fetch_add(op, Ordering::Relaxed);
    });

    for i in 1..=100u64 {
        actor.submit(i).await.unwrap();
    }

    // Drain the applier deterministically: shutdown closes the
    // sender and `join` waits for every queued op to be applied.
    // Grab a snapshot handle *before* shutdown drops it.
    let snap = actor.snapshot.clone();
    actor.shutdown().await;
    assert_eq!(snap.load().load(Ordering::Relaxed), 5050);
}

#[tokio::test]
async fn snapshot_replace_visible_to_readers() {
    let actor = IndexActor::<(), u64>::spawn(0, |_, _| async {});
    assert_eq!(*actor.snapshot(), 0);
    actor.replace_snapshot(42);
    assert_eq!(*actor.snapshot(), 42);
    actor.shutdown().await;
}

#[tokio::test]
async fn try_submit_full_returns_err() {
    // Capacity 1 — fill it, then a second `try_submit` must fail.
    // Use a never-resolving applier to keep the queue full.
    let (gate_tx, mut gate_rx) = tokio::sync::oneshot::channel::<()>();
    let mut gate_tx = Some(gate_tx);
    let actor =
        IndexActor::<u64, AtomicU64>::spawn_with_capacity(AtomicU64::new(0), 1, move |_, _| {
            // Block the applier on the first call only.
            let tx = gate_tx.take();
            async move {
                if let Some(tx) = tx {
                    let _ = tx.send(());
                    // Park forever — exits via shutdown drop.
                    std::future::pending::<()>().await;
                }
            }
        });

    // First op enters the applier and parks it.
    actor.try_submit(1).unwrap();
    // Wait until the applier is parked.
    let _ = gate_rx.try_recv();
    // Block until applier has picked up the first op. Best effort.
    tokio::task::yield_now().await;
    // Fill the queue.
    actor.try_submit(2).unwrap();
    // Now full — next try must fail with `Full`.
    let err = actor.try_submit(3).unwrap_err();
    assert!(matches!(err, mpsc::error::TrySendError::Full(3)));
}

//! Pause-guard tests (brief item 8, test 1): a held
//! `DrainPauseGuard` parks `drain_all` AND `drain_step` deterministically
//! (poll-once pending checks), a second acquisition is typed-busy while
//! the guard is live, and a cancelled acquisition wait clears the
//! tentative busy flag (cancel-safety) — release is proven by the parked
//! pass COMPLETING, never by a sleep.

use std::future::Future;
use std::pin::Pin;
use std::task::Poll;
use std::time::Duration;

use crate::repo::RepoInstance;
use crate::table::TableConfig;
use crate::tx::drain::drain_pause::DrainPauseBusy;
use crate::tx::tests::drain::drainer_tests::support::{make_repo, rid, seed_inflight_put};

/// Generous harness cap for tests that PROVE a parked pass RELEASES. The
/// cap only turns a regression (release never happens) into a fast,
/// loud failure; the handshake itself is the guard drop.
const RELEASE_BUDGET: Duration = Duration::from_secs(30);

/// Deterministic "not ready yet": poll once; returns true iff Pending.
/// Shared by the sibling test files in this module.
pub(crate) async fn poll_pending_once<F: Future>(mut fut: Pin<&mut F>) -> bool {
    std::future::poll_fn(|cx| match fut.as_mut().poll(cx) {
        Poll::Pending => Poll::Ready(true),
        Poll::Ready(_) => Poll::Ready(false),
    })
    .await
}

/// One visible, not-yet-durable commit version, so a released drain has
/// real work to do (and the assertion on completion is on gate state,
/// not just on the future resolving).
async fn seed_one_visible_commit(repo: &RepoInstance, value: &str) {
    let gate = repo.tx_gate().await.unwrap();
    seed_inflight_put(repo, "t", rid(1), value, 1).await;
    gate.publish_committed_max(1);
}

#[tokio::test]
async fn pause_guard_blocks_drain_all_until_drop() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    seed_one_visible_commit(&repo, "paused").await;
    let gate = repo.tx_gate().await.unwrap();

    let drainer = repo.drainer();
    let pause = drainer.pause();
    let guard = pause.acquire().await.expect("idle repo: pause acquired");
    assert!(repo.drainer().pause_held(), "busy flag set while held");

    let drainer = repo.drainer();
    let mut drain = Box::pin(drainer.drain_all(&repo.core));
    assert!(
        poll_pending_once(drain.as_mut()).await,
        "drain_all must park while the pause guard owns the replaying lock"
    );

    drop(guard);
    let drained = tokio::time::timeout(RELEASE_BUDGET, drain)
        .await
        .expect("drain_all released after the guard dropped")
        .expect("drain_all succeeded after release");
    assert_eq!(drained, 1, "the parked pass did the pending work");
    assert_eq!(gate.durable_watermark(), 1, "store state advanced");
    assert!(!repo.drainer().pause_held(), "busy flag cleared");
}

#[tokio::test]
async fn pause_guard_blocks_drain_step_until_drop() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    seed_one_visible_commit(&repo, "stepped").await;
    let gate = repo.tx_gate().await.unwrap();

    let drainer = repo.drainer();
    let pause = drainer.pause();
    let guard = pause.acquire().await.expect("idle repo: pause acquired");

    let drainer = repo.drainer();
    let mut step = Box::pin(drainer.drain_step(&repo.core));
    assert!(
        poll_pending_once(step.as_mut()).await,
        "drain_step (and therefore the background loop and the truncation \
         wait, which acquire the same lock) must park under the pause"
    );

    drop(guard);
    let drained = tokio::time::timeout(RELEASE_BUDGET, step)
        .await
        .expect("drain_step released after the guard dropped")
        .expect("drain_step succeeded after release");
    assert_eq!(drained, 1);
    assert_eq!(gate.durable_watermark(), 1);
    assert!(!repo.drainer().pause_held());
}

#[tokio::test]
async fn second_pause_acquire_is_busy_while_held_then_admitted_after_drop() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));

    let drainer = repo.drainer();
    let pause = drainer.pause();
    let guard = pause.acquire().await.expect("first acquisition admitted");
    assert!(
        matches!(pause.acquire().await, Err(DrainPauseBusy)),
        "a second acquisition must be refused with the typed busy error, \
         never block behind a potentially minutes-long seal hold"
    );

    drop(guard);
    let second = pause.acquire().await.expect("re-admitted after the drop");
    assert!(repo.drainer().pause_held());
    drop(second);
    assert!(!repo.drainer().pause_held());
}

#[tokio::test]
async fn cancelled_pause_wait_clears_the_tentative_busy_flag() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));

    let drainer = repo.drainer();
    let pause = drainer.pause();

    // Deterministic contended schedule: hold the `replaying` lock DIRECTLY
    // (flag-free — exactly what an in-flight drain pass holds) so a pause
    // acquisition is genuinely parkable. Under a held pause GUARD the busy
    // flag is already set and `acquire` refuses with `DrainPauseBusy`
    // before it can ever park (pinned by
    // `second_pause_acquire_is_busy_while_held_then_admitted_after_drop`),
    // so lock-held-without-flag is the ONLY schedule that reaches the
    // cancelled-wait cleanup path.
    let replaying = drainer.test_replaying_lock().await;

    // A waiter parked on the held lock (busy flag tentatively set).
    let mut waiter = Box::pin(pause.acquire());
    assert!(poll_pending_once(waiter.as_mut()).await, "waiter parked");
    drop(waiter); // cancelled BEFORE the guard exists

    assert!(
        !repo.drainer().pause_held(),
        "the cancelled PauseAttempt must clear the tentative busy flag"
    );

    // Release the simulated in-flight pass; a fresh acquisition must be
    // admitted — a leaked tentative flag would fail this with
    // `DrainPauseBusy`.
    drop(replaying);
    let fresh = pause.acquire().await.expect("fresh acquire admitted");
    assert!(repo.drainer().pause_held());
    drop(fresh);
    assert!(!repo.drainer().pause_held());
}

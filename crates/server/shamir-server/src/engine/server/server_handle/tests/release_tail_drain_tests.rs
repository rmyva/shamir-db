//! The drain of the interactive-tx record-lock release tails is bounded by
//! the stage deadline passed in (early cutoff / outer deadline), not by a
//! fresh 10 s clock started inside the drain — a fresh clock can be
//! pre-empted by the outer deadline before the drain records its own error.
//! Also covers the lossless handoff: a release refused by a closed admission
//! is handed back and runs inline (`spawn_lossless`).

use std::sync::Arc;
use std::time::Duration;

use shamir_db::ShutdownReport;
use shamir_sync::durability::tx::ReleaseTails;

use super::super::release_tail_drain::drain_release_tails;

/// Kills the mutant that started a FRESH 10 s clock inside the drain instead
/// of honouring the stage deadline: with an already-expired stage deadline a
/// still-live tail must be reported (false + report error) immediately — the
/// 1 s outer bound below fails first for the fresh-clock mutant.
#[tokio::test(flavor = "current_thread")]
async fn expired_stage_deadline_reports_the_tail_immediately() {
    let tails = ReleaseTails::new();
    tails
        .spawn_lossless(std::future::pending())
        .expect("admission is open");
    let mut report = ShutdownReport::default();
    let now = tokio::time::Instant::now();
    let deadline = now.checked_sub(Duration::from_secs(1)).unwrap_or(now);
    let drained = tokio::time::timeout(
        Duration::from_secs(1),
        drain_release_tails(
            Some(&tails),
            false,
            deadline,
            Duration::from_secs(10),
            "test: expired stage deadline",
            &mut report,
        ),
    )
    .await
    .expect("the drain must end at the stage deadline, not at a fresh 10 s clock");
    assert!(
        !drained,
        "a live tail at an expired deadline is not drained"
    );
    assert!(
        !report.is_clean(),
        "the expiry must be recorded in the report before returning"
    );
}

/// A tail registered BEFORE the close (the drain's `close_admission` arm)
/// still settles the drain: registration under the close latch is atomic, so
/// the late completion is not lost and the report stays clean. Kills the
/// mutant that dropped live tails at `close` — the drain would return
/// INSTANTLY with a clean report while the release has not fired yet. The
/// release therefore fires only after the drain task had its first poll and
/// is observed STILL PENDING (`is_finished() == false` at fire time); an
/// instant return fails the `was_pending` assertion.
#[tokio::test(flavor = "current_thread")]
async fn close_then_drain_waits_for_the_registered_tail() {
    let tails = Arc::new(ReleaseTails::new());
    let released = Arc::new(tokio::sync::Notify::new());
    let work_released = Arc::clone(&released);
    assert!(
        tails
            .spawn_lossless(async move {
                work_released.notified().await;
            })
            .is_ok(),
        "admission is open"
    );
    tails.close();
    // The drain runs in a task so the test can observe it BEFORE firing the
    // release. Spawned FIRST: on this current-thread runtime it is polled
    // before the release task below, so the release only fires once the drain
    // has parked waiting for the registered tail (or already returned). The
    // flag is stored as the drain future's LAST statement, so the release
    // task reads "still pending" without owning the join handle; the flag can
    // only turn true AFTER the notify fires, and `was_pending` is read before
    // that.
    let drain_finished = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let tails_for_drain = Arc::clone(&tails);
    let finished_flag = Arc::clone(&drain_finished);
    let drain = tokio::spawn(async move {
        let mut report = ShutdownReport::default();
        let drained = tokio::time::timeout(
            Duration::from_secs(120),
            drain_release_tails(
                Some(&*tails_for_drain),
                true,
                tokio::time::Instant::now() + Duration::from_secs(120),
                Duration::from_secs(120),
                "test: registered tail settles",
                &mut report,
            ),
        )
        .await
        .expect("drain timeout");
        finished_flag.store(true, std::sync::atomic::Ordering::SeqCst);
        (drained, report)
    });
    let releaser = Arc::clone(&released);
    let pending_flag = Arc::clone(&drain_finished);
    let release = tokio::spawn(async move {
        let was_pending = !pending_flag.load(std::sync::atomic::Ordering::SeqCst);
        releaser.notify_one();
        was_pending
    });
    let (drained, report) = tokio::time::timeout(Duration::from_secs(120), drain)
        .await
        .expect("drain task timeout")
        .expect("drain task joined");
    assert!(drained, "the registered tail must settle");
    assert!(report.is_clean(), "{report:?}");
    // THE assertion: the release fired only after the drain was observed
    // still pending, so a drain that returned instantly (dropping the live
    // tail) cannot satisfy this test.
    let was_pending = tokio::time::timeout(Duration::from_secs(120), release)
        .await
        .expect("release task timeout")
        .expect("release task joined");
    assert!(
        was_pending,
        "the drain returned before the tail's release fired: it is not waiting for the registered tail"
    );
}

/// Finding 3(a), handoff level: a release refused by a CLOSED tail admission
/// is handed BACK to the caller (`spawn_lossless` → `Err(work)`) and runs
/// inline — never consumed, never dropped. Kills the mutant that consumed
/// the refused future (the old `is_closed()`-then-`spawn()` handoff in
/// `release_tx_locks` dropped the work when admission closed in between),
/// which lost the lock release entirely.
#[tokio::test(flavor = "current_thread")]
async fn spawn_lossless_after_close_hands_the_future_back() {
    let tails = ReleaseTails::new();
    tails.close();
    let (done, receiver) = tokio::sync::oneshot::channel::<()>();
    let work = async move {
        let _ = done.send(());
    };
    let Err(work) = tails.spawn_lossless(work) else {
        panic!("admission is closed: the future must be handed back, never consumed");
    };
    tokio::time::timeout(Duration::from_secs(120), work)
        .await
        .expect("the handed-back release must run inline");
    tokio::time::timeout(Duration::from_secs(120), receiver)
        .await
        .expect("handshake timeout")
        .expect("channel open");
}

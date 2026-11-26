//! Typed fail-closed refusals (brief item 8, test 5a): an unsettled drain
//! refuses capture with `DrainUnsettled` (no position reported, nothing
//! left closed or paused), a live close owner refuses capture with the
//! typed `Busy` error (the single-owner CAS refusal, mapped by
//! `barrier_owner`; `begin_close` refuses BEFORE the gate is ever closed,
//! so the loser sees `Busy`, not `AdmissionClosed`), and a live seal hold
//! refuses both barriers with `HoldActive` until the hold drops.

use std::time::Duration;

use crate::tx::repo::instance::repo_instance::OnlineBackupError;
use crate::tx::table::TableConfig;
use crate::tx::tests::drain::drainer_tests::support::{make_repo, rid, seed_inflight_put};

const REFUSAL_BUDGET: Duration = Duration::from_secs(30);

#[tokio::test]
async fn capture_fails_closed_when_drain_never_settles() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let gate = repo.tx_gate().await.unwrap();

    // An inflight entry parked ABOVE visibility: the drain window is
    // `durable < v <= last_committed`, so version 9 is never replayed and
    // the durable watermark can never catch up to `last_committed` — the
    // settle check fails closed, deterministically.
    seed_inflight_put(&repo, "t", rid(2), "parked", 9).await;
    gate.publish_committed_max(1);

    let outcome = tokio::time::timeout(REFUSAL_BUDGET, repo.capture_point())
        .await
        .expect("capture must not hang on an unsettled drain")
        .expect_err("capture must refuse to report a position");
    assert!(
        matches!(outcome, OnlineBackupError::DrainUnsettled),
        "unexpected refusal: {outcome:?}"
    );

    // Fail-closed recovery: gate open, zero leases, pause released.
    let admission = repo.core().commit_admission();
    assert!(!admission.is_closed());
    assert_eq!(admission.active_count(), 0);
    assert!(!repo.drainer().pause_held());

    // Recovery: fill the gap BELOW the parked version. Drain replay is
    // strictly contiguous — each replayed entry `mark_durable`s its own
    // version (drain_step.rs) and a version sitting above an empty range
    // that does not exist in the WAL is deferred, never skipped — so
    // `publish_committed_max(9)` alone can never settle: versions 1..8
    // must first exist as replayable WAL entries.
    for version in 1u8..=8 {
        seed_inflight_put(&repo, "t", rid(version), "gap", version as u64).await;
    }
    gate.publish_committed_max(9);
    let position = tokio::time::timeout(REFUSAL_BUDGET, repo.capture_point())
        .await
        .expect("capture completes")
        .expect("capture succeeds once the drain settles");
    assert_eq!(position, 9);
    assert_eq!(gate.durable_watermark(), 9);
}

#[tokio::test]
async fn capture_while_a_close_owner_is_live_is_busy() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let admission = repo.core().commit_admission();

    let owner = admission.begin_close().expect("close");
    assert!(admission.is_closed());
    let outcome = tokio::time::timeout(REFUSAL_BUDGET, repo.capture_point())
        .await
        .expect("capture must not hang against a live close owner")
        .expect_err("the concurrent barrier won the race");
    assert!(
        matches!(outcome, OnlineBackupError::Busy),
        "unexpected refusal: {outcome:?}"
    );

    drop(owner);
    assert!(!admission.is_closed());
    tokio::time::timeout(REFUSAL_BUDGET, repo.capture_point())
        .await
        .expect("capture completes")
        .expect("capture succeeds once the gate reopens");
}

#[tokio::test]
async fn hold_active_refuses_second_barrier_until_the_hold_drops() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let admission = repo.core().commit_admission();

    let hold = repo.seal_point().await.expect("seal");
    assert!(repo.drainer().pause_held());

    assert!(matches!(
        tokio::time::timeout(REFUSAL_BUDGET, repo.capture_point())
            .await
            .expect("capture must refuse immediately, not block"),
        Err(OnlineBackupError::HoldActive)
    ));
    assert!(matches!(
        tokio::time::timeout(REFUSAL_BUDGET, repo.seal_point())
            .await
            .expect("seal must refuse immediately, not block"),
        Err(OnlineBackupError::HoldActive)
    ));
    // The refused barriers must not have disturbed the hold's ownership:
    // the hold keeps the gate CLOSED (it owns the close owner) the whole
    // time, with zero admission leases.
    assert!(admission.is_closed());
    assert_eq!(admission.active_count(), 0);

    drop(hold);
    assert!(!repo.drainer().pause_held());
    tokio::time::timeout(REFUSAL_BUDGET, repo.capture_point())
        .await
        .expect("capture completes")
        .expect("capture succeeds after the hold drops");
}

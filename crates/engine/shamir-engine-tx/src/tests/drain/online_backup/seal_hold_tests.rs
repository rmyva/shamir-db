//! Seal-hold store-level exclusion (brief item 8, test 3): while the hold
//! is live it owns BOTH the drain pause AND the gate close owner, so the
//! staged-after-`V*` write cannot even commit. Its commit future, pinned
//! at the commit boundary, is deterministically Pending while the gate is
//! closed and the actual WAL stays exactly through `V*`; the SAME future
//! is awaited after the hold drops and commits with exactly `V* + 1`,
//! whose data is then applied to the stores. Assertions are on STORE
//! state (durable watermark) and WAL evidence, not on drain counters.
//!
//! Retention: the in-memory WAL reclaims individual frames at or below the
//! durable watermark on every drain pass, so a truncation pin at floor 1 is
//! acquired before the first commit and kept for the whole test; the WAL
//! reads then show no frame above V* and the highest frame exactly V*.

use std::time::Duration;

use shamir_query_types::batch::VisibilityWait;
use shamir_tx::IsolationLevel;
use shamir_types::types::value::InnerValue;

use crate::tx::repo::instance::repo_instance::OnlineBackupError;
use crate::tx::table::TableConfig;
use crate::tx::tests::drain::drainer_tests::support::make_repo;

/// Harness cap; the handshakes are the hold lifecycle itself.
const APPLY_BUDGET: Duration = Duration::from_secs(30);

async fn commit_insert(repo: &crate::tx::repo::RepoInstance, value: &str) {
    let table = repo.get_table("t").await.unwrap();
    let (mut tx, _guard) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    table
        .insert_tx(&InnerValue::Str(value.into()), Some(&mut tx))
        .await
        .unwrap();
    repo.commit_tx(tx).await.unwrap();
}

#[tokio::test]
async fn seal_hold_keeps_committed_after_v_star_data_out_of_stores_until_drop() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    // Retention pin before the first commit: the in-memory WAL reclaims
    // frames ≤ the durable watermark on every drain pass when unpinned.
    let _pin = repo.acquire_truncation_pin(1).await.unwrap();
    let table = repo.get_table("t").await.unwrap();
    let gate = repo.tx_gate().await.unwrap();

    // Pre-V* state: one REAL commit through the allocator (it allocates
    // version 1 and publishes inline), then drained to durable. Seeding via
    // `publish_committed_max` would bypass the allocator
    // (`RepoTxGate::new` seeds `version_counter` from `last_committed`,
    // gate/repo_tx_gate/core.rs:15; the seam settles the tracker and raises
    // the published marker but never touches the allocator,
    // core.rs:175-178), so the post-V* commit would collide on version 1.
    commit_insert(&repo, "before").await;
    repo.drainer().drain_all(&repo.core).await.unwrap();
    assert_eq!(gate.durable_watermark(), 1);
    assert_eq!(gate.last_committed(), 1);

    // Staged writer BEFORE the hold: begin + insert, no commit. `begin_tx`
    // takes NO admission lease (the lease is acquired only at the commit
    // boundary, entry.rs:645), so the barrier's zero-lease wait is not
    // blocked by it.
    let (mut tx, _guard) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let after_id = table
        .insert_tx(&InnerValue::Str("after".into()), Some(&mut tx))
        .await
        .unwrap();

    // Step E: the hold comes back owning the pause AND the close owner;
    // V* = 1.
    let hold = repo.seal_point().await.expect("seal");
    let v_star = hold.position();
    assert_eq!(v_star, 1, "V* is the last committed version");
    assert!(hold.held_since() <= std::time::Instant::now());
    fn assert_send_static<T: Send + 'static>(_: &T) {}
    assert_send_static(&hold);

    // The hold keeps BOTH gates held: admission closed, zero leases.
    let admission = repo.core().commit_admission();
    assert!(
        admission.is_closed(),
        "the hold must keep the admission gate closed"
    );
    assert_eq!(
        admission.active_count(),
        0,
        "no admission leases may exist under the hold"
    );

    // The commit future pinned AT the commit boundary: while the gate is
    // closed it parks on the bounded reopen wait inside `acquire_leader`
    // (entry.rs:645) — a single deterministic poll proves Pending without
    // sleeps; the pipeline has appended NOTHING at that point.
    let mut commit = Box::pin(repo.commit_tx_with_options(tx, VisibilityWait::Strict));
    assert!(
        futures::poll!(commit.as_mut()).is_pending(),
        "the under-hold commit must park on admission, not complete or refuse"
    );

    // Store and WAL evidence: nothing above V* exists while the hold is
    // live — the actual WAL stays exactly through V*.
    let wal = repo.repo_wal().await.unwrap();
    let versions: Vec<u64> = wal
        .recover()
        .await
        .unwrap()
        .iter()
        .map(|e| e.commit_version)
        .collect();
    assert!(
        versions.iter().all(|&v| v <= v_star),
        "no frame above V* may exist while the hold is live, got {versions:?}"
    );
    assert_eq!(
        versions.iter().max().copied(),
        Some(v_star),
        "the WAL must stay exactly through V* (highest frame == V*) while the hold is live"
    );
    assert_eq!(
        gate.durable_watermark(),
        v_star,
        "durable watermark must stay at V* while the hold is live"
    );

    // The hold owns the pause exclusively: a second seal and a capture are
    // typed-refused.
    assert!(matches!(
        repo.seal_point().await,
        Err(OnlineBackupError::HoldActive)
    ));
    assert!(matches!(
        repo.capture_point().await,
        Err(OnlineBackupError::HoldActive)
    ));

    // Drop the hold: pause released FIRST, gate reopened LAST. The SAME
    // pinned future (same tx, no retry) then completes with exactly
    // V* + 1; the strict wait is satisfied by the inline settle, no drain
    // pass needed.
    drop(hold);
    assert!(!admission.is_closed(), "hold drop reopens the gate");
    assert!(!repo.drainer().pause_held(), "hold drop releases the pause");
    let outcome = tokio::time::timeout(APPLY_BUDGET, commit.as_mut())
        .await
        .expect("the staged commit completes once the hold drops")
        .expect("the staged commit succeeds");
    assert_eq!(
        outcome.commit_version,
        v_star + 1,
        "the released commit takes exactly the version after V*"
    );

    // The committed-after-V* WAL entry (version taken FROM THE WAL, not
    // from the acknowledgement) survives for the post-hold replay.
    let wal = repo.repo_wal().await.unwrap();
    assert!(
        wal.recover()
            .await
            .unwrap()
            .iter()
            .any(|e| e.commit_version == v_star + 1),
        "the post-V* commit's WAL entry must be present after the hold drops"
    );

    // The drain applies the committed-after-V* data; the explicit pass is
    // idempotent and the store-state assertions below are the proof.
    let _ = tokio::time::timeout(APPLY_BUDGET, repo.drainer().drain_all(&repo.core))
        .await
        .expect("drain completes after the hold drops")
        .expect("drain succeeds");
    assert_eq!(
        gate.durable_watermark(),
        v_star + 1,
        "store state advanced past V*"
    );
    assert_eq!(
        gate.last_committed(),
        v_star + 1,
        "the post-V* commit stays published across the drain"
    );
    let got = table.get(after_id).await.unwrap();
    assert!(
        matches!(got, InnerValue::Str(ref s) if s == "after"),
        "post-V* data applied after the hold drops, got {got:?}"
    );
}

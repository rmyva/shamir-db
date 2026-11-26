use std::sync::Arc;
use std::time::Duration;

use shamir_tx::IsolationLevel;
use shamir_types::types::value::InnerValue;
use shamir_wal::{WalEntryV2, WalTicket, WalTicketState};

use crate::tx::materialize::TEST_PRE_MATERIALIZE_REPO;
use crate::tx::repo::repo_token;
use crate::tx::tests::drain::orphans::post_wal_orphan_ordering_tests::{fixture, panic_post_wal};
use crate::tx::tests::recovery::snapshot_gc::snapshot_visibility_park_tests::spawn_settled_commit;
use crate::tx::MaterializationState;

// Catches acknowledging B across a written orphan, or settling A without replay.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn orphan_below_commit_blocks_ack_until_replay_then_snapshot_sees_both() {
    let (repo, hook) = fixture("snapshot_orphan_ack").await;
    TEST_PRE_MATERIALIZE_REPO
        .set(repo.name().to_owned())
        .unwrap();
    let a = repo.get_table("a").await.unwrap();
    let b = repo.get_table("b").await.unwrap();
    a.mvcc_store_ref().unwrap().set_test_now(1_000);
    b.mvcc_store_ref().unwrap().set_test_now(1_000);
    let gate = repo.tx_gate().await.unwrap();
    let (mut tx_a, pin_a) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let value_a = InnerValue::Str("orphan-a".into());
    let id_a = a.insert_tx(&value_a, Some(&mut tx_a)).await.unwrap();
    // Unwind, rather than cancel: cancellation intentionally detaches the tail.
    let version_a = panic_post_wal(&repo, &hook, tx_a).await;
    drop(pin_a);
    assert!(gate.visible_floor() < version_a);
    assert_eq!(a.mvcc_store_ref().unwrap().version_of(id_a.as_bytes()), 0);

    let (mut tx_b, pin_b) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let value_b = InnerValue::Str("later-b".into());
    let id_b = b.insert_tx(&value_b, Some(&mut tx_b)).await.unwrap();
    let (version_b, task_b) = spawn_settled_commit(&repo, tx_b).await;
    drop(pin_b);
    assert_eq!(version_b, version_a + 1);
    assert!(gate.is_orphaned_post_wal(version_a));
    assert!(gate.visible_floor() < version_a);
    assert!(!task_b.is_finished());

    // fixture suppresses the background drainer: this is the sole replay pass.
    assert_eq!(repo.drainer().drain_all(&repo.core).await.unwrap(), 2);
    let outcome = task_b.await.unwrap().unwrap();
    assert_eq!(outcome.commit_version, version_b);
    assert_eq!(outcome.materialization, MaterializationState::Complete);
    assert!(gate.visible_floor() >= version_b);
    assert!(!gate.is_orphaned_post_wal(version_a));
    let (reader, pin) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    assert!(reader.snapshot_version >= version_b);
    assert_eq!(a.read_one_tx(id_a, Some(&reader)).await.unwrap(), value_a);
    assert_eq!(b.read_one_tx(id_b, Some(&reader)).await.unwrap(), value_b);
    drop(pin);
}

// Catches removing the reopen bailout or failing to downgrade B to Deferred.
#[tokio::test(start_paused = true)]
async fn unknown_ticket_below_waiting_commit_returns_deferred_without_settling_gap() {
    let (repo, _) = fixture("snapshot_unknown_ack").await;
    let gate = repo.tx_gate().await.unwrap();
    let mut lower = gate.assign_next_version_guarded();
    let version_a = lower.version();
    let ticket = Arc::new(WalTicket::detached_for_test(WalTicketState::Pending));
    let entry = Arc::new(
        WalEntryV2::new(version_a, repo_token(repo.name()), vec![]).with_commit_version(version_a),
    );
    lower.wal_queued(Arc::clone(&ticket), entry);
    drop(lower);
    assert_eq!(gate.unresolved_wal_versions(), vec![version_a]);

    let b = repo.get_table("b").await.unwrap();
    b.mvcc_store_ref().unwrap().set_test_now(1_000);
    let (mut tx_b, pin_b) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let value = InnerValue::Str("written-b".into());
    let id = b.insert_tx(&value, Some(&mut tx_b)).await.unwrap();
    let (version_b, mut task_b) = spawn_settled_commit(&repo, tx_b).await;
    assert!(!gate.requires_reopen());
    ticket.complete_detached_for_test(WalTicketState::Unknown);
    assert_eq!(repo.drainer().drain_step(&repo.core).await.unwrap(), 0);
    assert!(gate.requires_reopen());
    // Only the borrowed JoinHandle is timed out; the commit is not cancelled.
    let result = tokio::time::timeout(Duration::from_secs(1), &mut task_b).await;
    let outcome = match result {
        Ok(result) => result.unwrap().unwrap(),
        Err(_) => {
            task_b.abort();
            let _cancelled = task_b.await;
            panic!("reopen-required acknowledgement did not return");
        }
    };
    assert_eq!(outcome.commit_version, version_b);
    assert_eq!(outcome.materialization, MaterializationState::Deferred);
    assert!(gate.visible_floor() < version_a);
    assert_eq!(gate.unresolved_wal_versions(), vec![version_a]);
    assert_eq!(b.get(id).await.unwrap(), value);
    drop(pin_b);
}

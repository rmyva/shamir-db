use std::sync::Arc;

use shamir_storage::storage_in_memory::InMemoryRepo;
use shamir_tx::{CommitVisibility, IsolationLevel};
use shamir_types::types::value::InnerValue;
use shamir_wal::{WalAppendError, WalEntryV2, WalOpV2, WalTicket, WalTicketState};

use crate::tx::commit::{wal_completion, TxError, TEST_SYNCED_WAL_REPO};
use crate::tx::drainer::Drainer;
use crate::tx::repo::{repo_token, BoxRepo, RepoInstance};
use crate::tx::table::{table_manager::table_token_for, TableConfig};

use super::support::{make_repo, rid, seed_inflight_put, transfer_pending};

#[tokio::test]
async fn ticket_failed_resolution_advances_both_watermarks() {
    let repo = make_repo();
    let ticket = transfer_pending(&repo, vec![]).await;
    ticket.complete_detached_for_test(WalTicketState::Failed);
    let drainer = Drainer::new();
    assert_eq!(drainer.drain_step(&repo.core).await.unwrap(), 0);
    let gate = repo.tx_gate().await.unwrap();
    assert_eq!(gate.last_committed(), 1);
    assert_eq!(gate.durable_watermark(), 1);
    assert!(gate.unresolved_wal_versions().is_empty());
    assert_eq!(drainer.recover_calls(), 0);
}

#[tokio::test]
async fn ticket_pending_blocks_later_orphan_without_recover() {
    let repo = make_repo();
    let _ticket = transfer_pending(&repo, vec![]).await;
    let gate = repo.tx_gate().await.unwrap();
    let mut later = gate.assign_next_version_guarded();
    let entry =
        Arc::new(WalEntryV2::new(2, repo_token(repo.name()), vec![]).with_commit_version(2));
    later.wal_queued(
        Arc::new(WalTicket::detached_for_test(WalTicketState::Written)),
        entry,
    );
    drop(later);
    assert_eq!(gate.orphan_max(), 2);
    assert!(gate.is_orphaned_post_wal(2));
    let drainer = Drainer::new();
    // Without the pending ceiling, the real later orphan makes vis > dur
    // and the missing v1 triggers recover; this is not an idle-pass oracle.
    assert_eq!(drainer.drain_step(&repo.core).await.unwrap(), 0);
    assert_eq!(gate.last_committed(), 0);
    assert_eq!(gate.durable_watermark(), 0);
    assert_eq!(gate.unresolved_wal_versions(), vec![1]);
    assert!(gate.is_orphaned_post_wal(2));
    assert_eq!(drainer.recover_calls(), 0);
}

// Counterfactual: clamping vis alone still reseeds a missing earlier offer.
// Genuine WAL v1 would be decoded/replayed despite unresolved v2's barrier.
#[tokio::test]
async fn ticket_pending_and_unknown_block_reseed_of_missing_published_prefix() {
    for state in [WalTicketState::Pending, WalTicketState::Unknown] {
        let repo = make_repo();
        repo.add_table_for_test(TableConfig::new("t"));
        let tbl = repo.get_table("t").await.unwrap();
        let gate = repo.tx_gate().await.unwrap();
        let prefix = gate.assign_next_version_guarded();
        assert_eq!(prefix.version(), 1);
        seed_inflight_put(&repo, "t", rid(1), "missing-prefix", 1).await;
        prefix.commit();
        assert_eq!(gate.last_committed(), 1);
        assert_eq!(gate.durable_watermark(), 0);
        let ticket = transfer_pending(&repo, vec![]).await;
        if state == WalTicketState::Unknown {
            ticket.complete_detached_for_test(state);
        }
        assert_eq!(gate.unresolved_wal_versions(), vec![2]);
        let wal = repo.repo_wal().await.unwrap();
        assert!(wal
            .recover()
            .await
            .unwrap()
            .iter()
            .any(|entry| entry.commit_version == 1));
        let drainer = Drainer::new();
        assert_eq!(drainer.window_len(), 0);
        let recover_calls = drainer.recover_calls();
        assert_eq!(drainer.drain_step(&repo.core).await.unwrap(), 0);
        assert_eq!(drainer.recover_calls(), recover_calls);
        assert_eq!(drainer.window_len(), 0);
        assert_eq!(gate.last_committed(), 1);
        assert_eq!(gate.durable_watermark(), 0);
        assert_eq!(gate.unresolved_wal_versions(), vec![2]);
        assert_eq!(gate.requires_reopen(), state == WalTicketState::Unknown);
        assert!(tbl.get(rid(1)).await.is_err());

        if state == WalTicketState::Pending {
            // Once absence is proven, ordinary gap recovery must resume.
            ticket.complete_detached_for_test(WalTicketState::Failed);
            assert_eq!(drainer.drain_step(&repo.core).await.unwrap(), 1);
            assert_eq!(drainer.recover_calls(), recover_calls + 1);
            assert_eq!(gate.durable_watermark(), 2);
            assert_eq!(
                tbl.get(rid(1)).await.unwrap(),
                InnerValue::Str("missing-prefix".into()),
            );
        }
    }
}

#[tokio::test]
async fn ticket_written_and_acked_replay_data_and_mark_lazy_pull_gap() {
    for state in [WalTicketState::Written, WalTicketState::Acked] {
        let repo = make_repo();
        repo.add_table_for_test(TableConfig::new("t"));
        let tbl = repo.get_table("t").await.unwrap();
        let body = InnerValue::Str("ticket-orphan".into()).to_bytes().unwrap();
        let ticket = transfer_pending(
            &repo,
            vec![WalOpV2::Put {
                table_id_interned: table_token_for("t"),
                rid: rid(42),
                body,
            }],
        )
        .await;
        ticket.complete_detached_for_test(state);
        let drainer = Drainer::new();
        drainer.set_window_high_watermark(0);
        assert_eq!(drainer.drain_step(&repo.core).await.unwrap(), 1);
        let gate = repo.tx_gate().await.unwrap();
        assert_eq!(gate.last_committed(), 1);
        assert_eq!(gate.durable_watermark(), 1);
        assert_eq!(
            tbl.get(rid(42)).await.unwrap(),
            InnerValue::Str("ticket-orphan".into()),
        );
        assert!(!gate.is_orphaned_post_wal(1));
        assert!(gate.unresolved_wal_versions().is_empty());
        assert_eq!(drainer.recover_calls(), 0);
        assert_eq!(
            repo.read_changelog_from(0, 10).await.unwrap().gap_at,
            Some(1)
        );
    }
}

#[tokio::test]
async fn ticket_unknown_halts_at_predecessor_and_rejects_commit_before_allocation() {
    let repo = make_repo();
    let prefix = transfer_pending(&repo, vec![]).await;
    prefix.complete_detached_for_test(WalTicketState::Written);
    let unknown = transfer_pending(&repo, vec![]).await;
    unknown.complete_detached_for_test(WalTicketState::Unknown);
    let later = transfer_pending(&repo, vec![]).await;
    later.complete_detached_for_test(WalTicketState::Acked);
    let drainer = Drainer::new();
    assert_eq!(drainer.drain_step(&repo.core).await.unwrap(), 1);
    let gate = repo.tx_gate().await.unwrap();
    assert!(gate.requires_reopen());
    assert_eq!(gate.last_committed(), 1);
    assert_eq!(gate.durable_watermark(), 1);
    assert_eq!(gate.unresolved_wal_versions(), vec![2, 3]);
    assert_eq!(drainer.drain_step(&repo.core).await.unwrap(), 0);
    assert_eq!(drainer.recover_calls(), 0);
    let allocated = gate.version_allocation_high_water_mark();
    let (tx, snapshot) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let result = repo.commit_tx(tx).await;
    assert!(
        matches!(&result, Err(TxError::WalRequiresReopen)),
        "{result:?}"
    );
    assert_eq!(gate.version_allocation_high_water_mark(), allocated);
    drop(snapshot);
}

#[test]
fn written_durability_failure_is_not_an_abort_or_fail_stop() {
    let gate = shamir_tx::RepoTxGate::fresh();
    assert!(
        wal_completion(&gate, 108, Err(WalAppendError::WrittenNotDurable))
            .0
            .unwrap()
    );
    assert!(!gate.requires_reopen());
    // NotWritten additionally reports proven_not_written = true (the B3
    // settle trigger); the fail-stop outcomes report false.
    let (res, proven_not_written) = wal_completion(&gate, 109, Err(WalAppendError::NotWritten));
    assert!(matches!(res, Err(TxError::Storage(_))));
    assert!(proven_not_written);
    assert!(!gate.requires_reopen());
    let (res, proven_not_written) = wal_completion(&gate, 107, Err(WalAppendError::Indeterminate));
    assert!(matches!(
        res,
        Err(TxError::WalOutcomeUnknown {
            commit_version: 107
        })
    ));
    assert!(!proven_not_written);
    assert!(gate.requires_reopen());
}

#[tokio::test]
async fn synced_write_then_sync_failure_publishes_data_before_typed_commit_error() {
    let name = "synced_commit_failure";
    TEST_SYNCED_WAL_REPO.set(name.into()).unwrap();
    let repo = RepoInstance::new(
        name.into(),
        BoxRepo::InMemory(Arc::new(InMemoryRepo::new())),
        Vec::new(),
    );
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    let gate = repo.tx_gate().await.unwrap();
    let wal = repo.repo_wal().await.unwrap();
    let drainer = repo.drainer();
    let replay_guard = drainer.replay_lock_for_test().await;

    for visibility in [CommitVisibility::Synchronous, CommitVisibility::AsyncIndex] {
        let expected_version = gate.version_allocation_high_water_mark() + 1;
        let (mut tx, snapshot) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
        tx.visibility = visibility;
        let value = InnerValue::Str(format!("written-before-sync-failure-{expected_version}"));
        let record = tbl.insert_tx(&value, Some(&mut tx)).await.unwrap();
        wal.arm_fail_next_sync_for_test();
        let result = repo.commit_tx(tx).await;
        assert!(
            matches!(
                &result,
                Err(TxError::WalDurability { commit_version, .. })
                    if *commit_version == expected_version
            ),
            "unexpected commit result: {result:?}",
        );
        assert_eq!(gate.last_committed(), expected_version);
        assert_eq!(gate.durable_watermark(), 0);
        assert!(!gate.requires_reopen());
        assert!(!gate.is_active_post_wal(expected_version));
        assert!(!gate.is_orphaned_post_wal(expected_version));
        assert_eq!(repo.tx_metrics().snapshot().post_wal_orphans_total, 0);
        assert_eq!(tbl.get(record).await.unwrap(), value);
        let recovered = wal.recover().await.unwrap();
        let entry = recovered
            .iter()
            .find(|entry| entry.commit_version == expected_version)
            .expect("successful write survives failed sync");
        let body = value.to_bytes().unwrap();
        assert!(entry.ops.iter().any(|op| matches!(
            op,
            WalOpV2::Put { rid, body: written, .. }
                if *rid == record && *written == body
        )));
        drop(snapshot);
    }

    drop(replay_guard);
    drainer.drain_all(&repo.core).await.unwrap();
    assert_eq!(gate.durable_watermark(), gate.last_committed());
    assert_eq!(gate.last_committed(), 2);
}

// RV-WAL-3B29232E: Unknown/Indeterminate AFTER queueing must map to
// WalOutcomeUnknown (never WalRequiresReopen/aborted).
#[test]
fn rv_wal_3b29232e_unknown_ticket_is_wal_outcome_unknown() {
    for outcome in [WalAppendError::Unknown, WalAppendError::Indeterminate] {
        let gate = shamir_tx::RepoTxGate::fresh();
        let (res, proven_not_written) = wal_completion(&gate, 107, Err(outcome));
        assert!(
            matches!(
                res,
                Err(TxError::WalOutcomeUnknown {
                    commit_version: 107
                })
            ),
            "{res:?}"
        );
        assert!(!proven_not_written);
        assert!(gate.requires_reopen());
    }
}

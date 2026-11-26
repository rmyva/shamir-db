//! Checkpoint-Uncertain reconcile tests (S5c-X PR6b-2 item 7): with the
//! test-only seam rewriting a `Committed` checkpoint outcome to `Uncertain`,
//! the read-back reconcile runs and the repo settles identically — verified
//! on the live handle AND across two reopen cycles.
//!
//! Deterministic: no sleeps and no yield budgets (review finding 5). The
//! old branch's bounded 20 ms fjall-open retry loop is gone — every reopen
//! here follows a GRACEFUL close, so the store lock is always free and the
//! plain open cannot fail spuriously.

use std::path::Path;

use bytes::Bytes;
use futures::StreamExt;
use shamir_storage::error::DbError;
use shamir_storage::types::RecordKey;
use shamir_tx::{decode_version_key, ChangeOp, ChangelogEvent, RecordChange};
use shamir_types::access::Actor;
use shamir_types::codecs::basic::bincode as bincode_codec;
use shamir_types::types::record_id::RecordId;
use shamir_types::types::value::InnerValue;

use crate::meta::repl::bookmark::load_clean_horizon;
use crate::meta::repl::bookmark_v3::load_bookmark_v3;
use crate::meta::repl::receipt::receipt_key;
use crate::meta::repl::{decode_receipt, MAX_RECEIPT_EVENTS};
use crate::meta::MetaKey;
use crate::repo::{BoxRepoFactory, RepoInstance};
use crate::table::TableConfig;
use crate::tx::apply_replicated::{apply_replicated_batch, ApplyOutcome, ApplyPolicy};
use crate::tx::replication::follower_durability::receipt::stage::{
    inject_materialization_batch_failures, set_materialization_readback_unavailable,
    MaterializationBatchFailure,
};
use crate::tx::ReplicatedEvent;

use crate::tx::replication::follower_durability::receipt::checkpoint::{
    checkpoint_reconciles, checkpoint_test_lock, disable_uncertain_reconcile, set_fail_final_flush,
    set_simulate_checkpoint_uncertain, set_uncertain_without_commit,
};

struct MaterializationSeamReset(String);
impl Drop for MaterializationSeamReset {
    fn drop(&mut self) {
        crate::tx::replication::follower_durability::receipt::stage::clear_materialization_batch_failures(&self.0);
        set_materialization_readback_unavailable(&self.0, false);
    }
}
fn event(leader_version: u64, id: u8) -> ReplicatedEvent {
    ReplicatedEvent {
        event: ChangelogEvent {
            repo: "leader".into(),
            commit_version: leader_version,
            tx_id: leader_version,
            actor: Actor::System,
            timestamp_ns: 0,
            changes: vec![RecordChange {
                table: "items".into(),
                key: RecordId([id; 16]).to_bytes(),
                op: ChangeOp::Put,
                value: Some(InnerValue::Str(format!("row-{id}")).to_bytes().unwrap()),
            }],
        },
        field_bindings: Vec::new(),
    }
}
fn event_with_table(leader_version: u64, id: u8, table: &str) -> ReplicatedEvent {
    let mut value = event(leader_version, id);
    value.event.changes[0].table = table.to_owned();
    value
}

async fn open(path: &Path, name: &str) -> RepoInstance {
    RepoInstance::from_factory(
        name.into(),
        BoxRepoFactory::fjall_raw(path),
        vec![TableConfig::new("items"), TableConfig::new("other")],
    )
    .await
    .unwrap()
}

async fn close(repo: RepoInstance) {
    let handle = repo.changefeed().await.unwrap();
    handle.feed.shutdown_and_wait().await.unwrap();
    drop(handle);
    drop(repo);
}

fn repo_name(path: &Path) -> String {
    format!(
        "checkpoint-uncertain-{}",
        path.display().to_string().replace('\\', "_")
    )
}

async fn journal(repo: &RepoInstance, version: u64) -> Option<Bytes> {
    repo.follower_journal_store()
        .await
        .unwrap()
        .get(RecordKey::from_slice(&version.to_be_bytes()))
        .await
        .ok()
}

async fn key_bytes(id: u8) -> Vec<u8> {
    RecordId([id; 16]).to_bytes().to_vec()
}

async fn history_versions_for(repo: &RepoInstance, table: &str, key: &[u8]) -> Vec<u64> {
    let store = repo
        .raw_store(&format!("__history__{table}"))
        .await
        .unwrap();
    let mut versions = Vec::new();
    for batch in store.iter_stream(128).collect::<Vec<_>>().await {
        for (record_key, _) in batch.unwrap() {
            if let Some((row_key, version)) = decode_version_key(record_key.as_ref()) {
                if row_key == key {
                    versions.push(version);
                }
            }
        }
    }
    versions.sort_unstable();
    versions
}

async fn persisted_count_for(repo: &RepoInstance, table: &str) -> u64 {
    let store = repo.raw_store(&format!("__info__{table}")).await.unwrap();
    match store
        .get(RecordKey::from_slice(
            MetaKey::Count.as_record_id().as_bytes(),
        ))
        .await
    {
        Ok(bytes) => bincode_codec::from_bytes::<u64>(&bytes).unwrap(),
        Err(DbError::NotFound(_)) => 0,
        Err(error) => panic!("count read failed for {table}: {error}"),
    }
}
/// The settled post-checkpoint state, asserted identically on the live
/// handle and after each reopen: bookmark v3 advanced with no outstanding
/// receipt, the receipt key retired, one history version per key, both
/// journal rows present, and the same batch re-delivered is a Skipped no-op.
async fn assert_settled(repo: &RepoInstance) {
    let key1 = key_bytes(1).await;
    let key2 = key_bytes(2).await;
    assert_eq!(history_versions_for(repo, "items", &key1).await, vec![1]);
    assert_eq!(history_versions_for(repo, "other", &key2).await, vec![2]);
    assert_eq!(persisted_count_for(repo, "items").await, 1);
    assert_eq!(persisted_count_for(repo, "other").await, 1);
    let info = repo.tx_info_store().await.unwrap();
    let v3 = load_bookmark_v3(&info).await.unwrap().unwrap();
    assert_eq!(v3.bookmark, 2);
    assert_eq!(v3.clean_through, Some(2));
    assert_eq!(v3.outstanding_receipt, None);
    assert!(matches!(
        info.get(receipt_key()).await,
        Err(DbError::NotFound(_))
    ));
    drop(info);

    assert_eq!(history_versions_for(repo, "items", &key1).await, vec![1]);
    assert_eq!(history_versions_for(repo, "other", &key2).await, vec![2]);
    assert_eq!(persisted_count_for(repo, "items").await, 1);
    assert_eq!(persisted_count_for(repo, "other").await, 1);
    assert!(journal(repo, 1).await.is_some());
    assert!(journal(repo, 2).await.is_some());
    assert_eq!(
        apply_replicated_batch(
            &repo.core,
            &fresh_two_table_events().await,
            0,
            ApplyPolicy::Trusted
        )
        .await
        .unwrap(),
        ApplyOutcome::Skipped
    );
}

async fn fresh_two_table_events() -> [ReplicatedEvent; 2] {
    [
        event_with_table(1, 1, "items"),
        event_with_table(2, 2, "other"),
    ]
}

#[tokio::test]
async fn materialization_batch_failure_preserves_prepared_receipt_and_retry_allocation() {
    let _guard = checkpoint_test_lock().await;
    let temp = tempfile::TempDir::new().unwrap();
    let name = repo_name(temp.path());
    let _reset = MaterializationSeamReset(name.clone());
    let repo = open(temp.path(), &name).await;
    let events = fresh_two_table_events().await;
    inject_materialization_batch_failures(&name, &[MaterializationBatchFailure::BeforeCommit; 3]);
    let error = apply_replicated_batch(&repo.core, &events, 0, ApplyPolicy::Trusted)
        .await
        .unwrap_err();
    assert!(matches!(error, DbError::Storage(_)), "{error:?}");
    let gate = repo.tx_gate().await.unwrap();
    assert!(!gate.requires_reopen());
    assert_eq!(gate.last_committed(), 0);
    assert_eq!(gate.durable_watermark(), 0);
    assert_eq!(gate.version_allocation_high_water_mark(), 2);
    let key1 = key_bytes(1).await;
    let key2 = key_bytes(2).await;
    assert!(history_versions_for(&repo, "items", &key1).await.is_empty());
    assert!(history_versions_for(&repo, "other", &key2).await.is_empty());
    assert!(journal(&repo, 1).await.is_none());
    assert!(journal(&repo, 2).await.is_none());
    assert_eq!(persisted_count_for(&repo, "items").await, 0);
    assert_eq!(persisted_count_for(&repo, "other").await, 0);
    let info = repo.tx_info_store().await.unwrap();
    let bookmark = load_bookmark_v3(&info).await.unwrap().unwrap();
    let receipt_id = bookmark.outstanding_receipt.unwrap();
    let receipt_bytes = info.get(receipt_key()).await.unwrap();
    let prepared = crate::meta::repl::decode_receipt(&receipt_id, &receipt_bytes).unwrap();
    assert_eq!(prepared.phase, crate::meta::repl::ReceiptPhase::Prepared);
    let prepared_receipt_id = receipt_id;
    let allocation = prepared.mapping.clone();
    drop(info);

    inject_materialization_batch_failures(&name, &[MaterializationBatchFailure::BeforeCommit; 3]);
    let retry_error = apply_replicated_batch(&repo.core, &events, 0, ApplyPolicy::Trusted)
        .await
        .unwrap_err();
    assert!(
        matches!(retry_error, DbError::Storage(_)),
        "{retry_error:?}"
    );
    let info = repo.tx_info_store().await.unwrap();
    let bookmark = load_bookmark_v3(&info).await.unwrap().unwrap();
    assert_eq!(bookmark.outstanding_receipt, Some(prepared_receipt_id));
    let receipt_bytes = info.get(receipt_key()).await.unwrap();
    let still_prepared =
        crate::meta::repl::decode_receipt(&prepared_receipt_id, &receipt_bytes).unwrap();
    assert_eq!(
        still_prepared.phase,
        crate::meta::repl::ReceiptPhase::Prepared
    );
    assert_eq!(still_prepared.mapping, allocation);
    drop(info);
    assert_eq!(gate.version_allocation_high_water_mark(), 2);
    assert_eq!(gate.last_committed(), 0);
    assert_eq!(gate.durable_watermark(), 0);

    crate::tx::replication::follower_durability::receipt::stage::clear_materialization_batch_failures(&name);
    assert_eq!(
        apply_replicated_batch(&repo.core, &events, 0, ApplyPolicy::Trusted)
            .await
            .unwrap(),
        ApplyOutcome::Applied { local_version: 2 }
    );
    let info = repo.tx_info_store().await.unwrap();
    let bookmark = load_bookmark_v3(&info).await.unwrap().unwrap();
    assert_eq!(bookmark.outstanding_receipt, None);
    assert!(matches!(
        info.get(receipt_key()).await,
        Err(DbError::NotFound(_))
    ));
    drop(info);
    assert_eq!(history_versions_for(&repo, "items", &key1).await, vec![1]);
    assert_eq!(history_versions_for(&repo, "other", &key2).await, vec![2]);
    assert_eq!(persisted_count_for(&repo, "items").await, 1);
    assert_eq!(persisted_count_for(&repo, "other").await, 1);
    assert!(journal(&repo, 1).await.is_some());
    assert!(journal(&repo, 2).await.is_some());
    assert_eq!(gate.version_allocation_high_water_mark(), 2);
    assert_eq!(gate.last_committed(), 2);
    assert_eq!(gate.durable_watermark(), 2);
    close(repo).await;
}

#[tokio::test]
async fn repeated_uncertainty_also_fail_stops_without_advancing_floors() {
    let _guard = checkpoint_test_lock().await;
    let temp = tempfile::TempDir::new().unwrap();
    let name = repo_name(temp.path());
    let _reset = MaterializationSeamReset(name.clone());
    let repo = open(temp.path(), &name).await;
    let events = fresh_two_table_events().await;
    inject_materialization_batch_failures(
        &name,
        &[
            MaterializationBatchFailure::AfterCommitUncertain,
            MaterializationBatchFailure::AfterCommitUncertain,
        ],
    );
    set_materialization_readback_unavailable(&name, true);

    let error = apply_replicated_batch(&repo.core, &events, 0, ApplyPolicy::Trusted)
        .await
        .unwrap_err();
    assert!(matches!(error, DbError::RepoRequiresReopen(_)), "{error:?}");
    let gate = repo.tx_gate().await.unwrap();
    assert!(gate.requires_reopen());
    assert_eq!(gate.version_allocation_high_water_mark(), 2);
    assert_eq!(gate.last_committed(), 0);
    assert_eq!(gate.durable_watermark(), 0);
    let key1 = key_bytes(1).await;
    let key2 = key_bytes(2).await;
    assert_eq!(history_versions_for(&repo, "items", &key1).await, vec![1]);
    assert_eq!(history_versions_for(&repo, "other", &key2).await, vec![2]);
    assert_eq!(persisted_count_for(&repo, "items").await, 1);
    assert_eq!(persisted_count_for(&repo, "other").await, 1);
    assert!(journal(&repo, 1).await.is_some());
    assert!(journal(&repo, 2).await.is_some());
    let retry = apply_replicated_batch(&repo.core, &events, 0, ApplyPolicy::Trusted).await;
    assert!(matches!(retry, Err(DbError::RepoRequiresReopen(_))));
    assert!(gate.requires_reopen());
    let shutdown = repo.shutdown_clean(std::time::Duration::from_secs(5)).await;
    assert!(
        matches!(shutdown, Err(DbError::Storage(_))),
        "quarantined worker shutdown should report failure"
    );
    assert!(repo.worker_reopen_required());
    close(repo).await;
}

#[tokio::test]
async fn unresolved_uncertainty_retry_storage_failure_fail_stops_and_shutdown_fails() {
    let _guard = checkpoint_test_lock().await;
    let temp = tempfile::TempDir::new().unwrap();
    let name = repo_name(temp.path());
    let _reset = MaterializationSeamReset(name.clone());
    let repo = open(temp.path(), &name).await;
    let events = fresh_two_table_events().await;
    inject_materialization_batch_failures(
        &name,
        &[
            MaterializationBatchFailure::AfterCommitUncertain,
            MaterializationBatchFailure::BeforeCommit,
        ],
    );
    set_materialization_readback_unavailable(&name, true);
    let error = apply_replicated_batch(&repo.core, &events, 0, ApplyPolicy::Trusted)
        .await
        .unwrap_err();
    assert!(matches!(error, DbError::RepoRequiresReopen(_)), "{error:?}");
    let gate = repo.tx_gate().await.unwrap();
    assert!(gate.requires_reopen());
    assert_eq!(gate.version_allocation_high_water_mark(), 2);
    assert_eq!(gate.last_committed(), 0);
    assert_eq!(gate.durable_watermark(), 0);
    let key1 = key_bytes(1).await;
    let key2 = key_bytes(2).await;
    assert_eq!(history_versions_for(&repo, "items", &key1).await, vec![1]);
    assert_eq!(history_versions_for(&repo, "other", &key2).await, vec![2]);
    assert_eq!(persisted_count_for(&repo, "items").await, 1);
    assert_eq!(persisted_count_for(&repo, "other").await, 1);
    assert!(journal(&repo, 1).await.is_some());
    assert!(journal(&repo, 2).await.is_some());
    let retry = apply_replicated_batch(&repo.core, &events, 0, ApplyPolicy::Trusted).await;
    assert!(
        matches!(retry, Err(DbError::RepoRequiresReopen(_))),
        "repo must remain fail-stopped: {retry:?}"
    );
    assert!(gate.requires_reopen());
    let shutdown = repo.shutdown_clean(std::time::Duration::from_secs(5)).await;
    assert!(
        matches!(shutdown, Err(DbError::Storage(_))),
        "quarantined worker shutdown should report failure"
    );
    assert!(repo.worker_reopen_required());
    close(repo).await;
}

// Uncertain so the read-back reconcile runs; the repo settles cleanly, and
// the settled state survives two reopen cycles. The reconcile branch is
// observably entered: `checkpoint_reconciles()` increases by exactly 1
// (baseline sampled before the apply).
#[tokio::test]
async fn uncertain_checkpoint_reconciles_and_survives_reopens() {
    // The checkpoint seams and the reconcile counter are process-global:
    // serialize against the other tests that touch them.
    let _lock = checkpoint_test_lock().await;
    set_simulate_checkpoint_uncertain(false);
    let temp = tempfile::TempDir::new().unwrap();
    let name = repo_name(temp.path());
    let repo = open(temp.path(), &name).await;

    let baseline = checkpoint_reconciles();
    set_simulate_checkpoint_uncertain(true);
    let events = fresh_two_table_events().await;
    let outcome = apply_replicated_batch(&repo.core, &events, 0, ApplyPolicy::Trusted).await;
    set_simulate_checkpoint_uncertain(false);
    let applied = outcome.unwrap();
    assert_eq!(applied, ApplyOutcome::Applied { local_version: 2 });
    assert_eq!(
        repo.tx_gate()
            .await
            .unwrap()
            .version_allocation_high_water_mark(),
        2
    );
    assert_eq!(checkpoint_reconciles(), baseline + 1);
    assert_settled(&repo).await;

    // Reopen twice; the settled state must be identical every time. The
    // final re-apply inside `assert_settled` is a Skipped no-op because the
    // production path consults the DURABLE bookmark before allocation: the
    // replayed batch's leader versions are already ≤ the durable bookmark.
    close(repo).await;
    let repo = open(temp.path(), &name).await;
    assert_settled(&repo).await;
    close(repo).await;
    let repo = open(temp.path(), &name).await;
    assert_settled(&repo).await;

    close(repo).await;
}

// Negative control: with the seam OFF the reconcile branch never runs, so
// the counter must not move — `checkpoint_reconciles()` is a real
// discriminator. (If the reconcile branch were removed from the code, the
// seamed test above would keep its Uncertain outcome and fail at its
// `unwrap`; this test would then be the only signal — a flat counter —
// so both halves are needed.)
#[tokio::test]
async fn no_reconcile_counter_without_uncertain() {
    let _lock = checkpoint_test_lock().await;
    set_simulate_checkpoint_uncertain(false);
    let temp = tempfile::TempDir::new().unwrap();
    let name = repo_name(temp.path());
    let repo = open(temp.path(), &name).await;

    let before = checkpoint_reconciles();
    assert_eq!(
        apply_replicated_batch(
            &repo.core,
            &[event(1, 1), event(2, 2)],
            0,
            ApplyPolicy::Trusted
        )
        .await
        .unwrap(),
        ApplyOutcome::Applied { local_version: 2 }
    );
    assert_eq!(checkpoint_reconciles(), before);

    close(repo).await;
}

// TRUE reconciliation negative control (review finding 9d): the checkpoint
// commit never lands (`set_uncertain_without_commit`) so the durable state is
// genuinely unsettled, and the read-back/retry protection is removed
// (`disable_uncertain_reconcile`) while the branch is STILL entered — the
// reconcile counter increments, but the settled-state invariants that
// `assert_settled` checks (bookmark advanced, clean horizon, receipt retired)
// provably FAIL. This proves the healthy-path test above discriminates real
// read-back protection, not merely branch entry.
#[tokio::test]
async fn negative_control_without_reconcile_state_is_not_reconciled() {
    let _lock = checkpoint_test_lock().await;
    set_simulate_checkpoint_uncertain(false);
    let temp = tempfile::TempDir::new().unwrap();
    let name = repo_name(temp.path());
    let repo = open(temp.path(), &name).await;

    let baseline = checkpoint_reconciles();
    set_uncertain_without_commit(true);
    disable_uncertain_reconcile(true);
    let events = fresh_two_table_events().await;
    let outcome = apply_replicated_batch(&repo.core, &events, 0, ApplyPolicy::Trusted).await;
    disable_uncertain_reconcile(false);
    set_uncertain_without_commit(false);

    // Branch entered (counter moved), protection removed: the pipeline still
    // reports success and publishes to RAM, but NOTHING was reconciled
    // durably.
    assert_eq!(outcome.unwrap(), ApplyOutcome::Applied { local_version: 2 });
    assert_eq!(checkpoint_reconciles(), baseline + 1);

    // Provably NOT reconciled — the exact inverse of `assert_settled`'s
    // durable invariants: the bookmark v3 record was never written, the
    // receipt is still outstanding (Prepared), and the re-delivery is NOT a
    // durable Skipped no-op (no durable bookmark classifies it as applied).
    let info = repo.tx_info_store().await.unwrap();
    let v3 = load_bookmark_v3(&info).await.unwrap();
    assert!(
        v3.as_ref().is_none_or(|v3| v3.bookmark < 2),
        "bookmark must NOT have advanced without the reconcile: {v3:?}"
    );
    assert!(
        v3.is_none_or(|v3| v3.outstanding_receipt.is_some()),
        "receipt must still be outstanding without the reconcile"
    );
    assert!(
        !matches!(info.get(receipt_key()).await, Err(DbError::NotFound(_))),
        "receipt key must still be present without the reconcile"
    );
    drop(info);

    close(repo).await;
}

// POSITIVE control for the unsettled-state injection (companion to the
// negative control above): the checkpoint commit never lands
// (`set_uncertain_without_commit`) but the read-back/retry reconcile is LEFT
// ENABLED — so the retry actually repairs the unsettled durable state. The
// pipeline still reports Applied AND `assert_settled` PASSES (bookmark
// advanced, receipt retired, one history version per key, re-delivery
// Skipped). Together with the disabled negative control this proves the
// oracle discriminates real reconciliation: ENABLED + unsettled -> settled,
// DISABLED + unsettled -> provably not settled.
#[tokio::test]
async fn uncertain_without_commit_is_reconciled_when_enabled() {
    let _lock = checkpoint_test_lock().await;
    set_simulate_checkpoint_uncertain(false);
    let temp = tempfile::TempDir::new().unwrap();
    let name = repo_name(temp.path());
    let repo = open(temp.path(), &name).await;

    let baseline = checkpoint_reconciles();
    set_uncertain_without_commit(true);
    let events = fresh_two_table_events().await;
    let outcome = apply_replicated_batch(&repo.core, &events, 0, ApplyPolicy::Trusted).await;
    set_uncertain_without_commit(false);

    assert_eq!(outcome.unwrap(), ApplyOutcome::Applied { local_version: 2 });
    assert_eq!(checkpoint_reconciles(), baseline + 1);
    // The retry repaired the unsettled state — the FULL settled oracle
    // passes, unlike the disabled negative control where it provably fails.
    assert_settled(&repo).await;

    close(repo).await;
}

#[tokio::test]
async fn multichunk_materialization_failure_resumes_same_receipt_without_extra_allocations() {
    let _guard = checkpoint_test_lock().await;
    let temp = tempfile::TempDir::new().unwrap();
    let name = repo_name(temp.path());
    let _reset = MaterializationSeamReset(name.clone());
    let repo = open(temp.path(), &name).await;
    let first_chunk = MAX_RECEIPT_EVENTS / 5;
    let n = first_chunk + 3;
    let events: Vec<_> = (1..=n as u64)
        .map(|version| {
            let mut event = event(version, 0);
            let mut bytes = [0u8; 16];
            bytes[..8].copy_from_slice(&version.to_be_bytes());
            event.event.changes[0].key = RecordId(bytes).to_bytes();
            event
        })
        .collect();

    inject_materialization_batch_failures(&name, &[MaterializationBatchFailure::BeforeCommit; 3]);
    assert!(matches!(
        apply_replicated_batch(&repo.core, &events, 0, ApplyPolicy::Trusted).await,
        Err(DbError::Storage(_))
    ));
    let gate = repo.tx_gate().await.unwrap();
    assert!(!gate.requires_reopen());
    assert_eq!(gate.last_committed(), 0);
    assert_eq!(gate.durable_watermark(), 0);
    assert_eq!(
        gate.version_allocation_high_water_mark(),
        first_chunk as u64
    );
    let info = repo.tx_info_store().await.unwrap();
    let bookmark = load_bookmark_v3(&info).await.unwrap().unwrap();
    let receipt_id = bookmark.outstanding_receipt.unwrap();
    let receipt = decode_receipt(&receipt_id, &info.get(receipt_key()).await.unwrap()).unwrap();
    assert_eq!(receipt.phase, crate::meta::repl::ReceiptPhase::Prepared);
    assert_eq!(receipt.mapping.len(), first_chunk);
    assert_eq!(
        receipt
            .mapping
            .iter()
            .map(|mapping| mapping.local_version)
            .collect::<Vec<_>>(),
        (1..=first_chunk as u64).collect::<Vec<_>>()
    );
    assert_eq!(
        receipt
            .mapping
            .iter()
            .map(|mapping| mapping.upstream_version)
            .collect::<Vec<_>>(),
        (1..=first_chunk as u64).collect::<Vec<_>>()
    );
    let mapping = receipt.mapping.clone();
    drop(info);
    assert_eq!(repo.receipt_worker_parked_guards().await, Some(first_chunk));

    inject_materialization_batch_failures(&name, &[MaterializationBatchFailure::BeforeCommit]);
    assert!(matches!(
        apply_replicated_batch(&repo.core, &events, 0, ApplyPolicy::Trusted).await,
        Err(DbError::Storage(_))
    ));
    let info = repo.tx_info_store().await.unwrap();
    assert_eq!(
        load_bookmark_v3(&info)
            .await
            .unwrap()
            .unwrap()
            .outstanding_receipt,
        Some(receipt_id)
    );
    let resumed = decode_receipt(&receipt_id, &info.get(receipt_key()).await.unwrap()).unwrap();
    assert_eq!(resumed.mapping, mapping);
    drop(info);
    assert!(!gate.requires_reopen());
    assert_eq!(
        gate.version_allocation_high_water_mark(),
        first_chunk as u64
    );
    assert_eq!(gate.last_committed(), 0);
    assert_eq!(gate.durable_watermark(), 0);

    crate::tx::replication::follower_durability::receipt::stage::clear_materialization_batch_failures(&name);
    assert_eq!(
        apply_replicated_batch(&repo.core, &events, 0, ApplyPolicy::Trusted)
            .await
            .unwrap(),
        ApplyOutcome::Applied {
            local_version: n as u64
        }
    );
    assert!(!gate.requires_reopen());
    assert_eq!(gate.last_committed(), n as u64);
    assert_eq!(gate.version_allocation_high_water_mark(), n as u64);
    assert_eq!(gate.durable_watermark(), n as u64);
    assert_eq!(repo.receipt_worker_parked_guards().await, Some(0));
    let info = repo.tx_info_store().await.unwrap();
    let final_bookmark = load_bookmark_v3(&info).await.unwrap().unwrap();
    assert_eq!(final_bookmark.bookmark, n as u64);
    assert_eq!(final_bookmark.outstanding_receipt, None);
    assert!(matches!(
        info.get(receipt_key()).await,
        Err(DbError::NotFound(_))
    ));
    drop(info);
    for index in 1..=n as u64 {
        let mut bytes = [0u8; 16];
        bytes[..8].copy_from_slice(&index.to_be_bytes());
        let key = RecordId(bytes).to_bytes();
        assert_eq!(
            history_versions_for(&repo, "items", &key).await,
            vec![index]
        );
        assert!(journal(&repo, index).await.is_some());
    }
    assert_eq!(persisted_count_for(&repo, "items").await, n as u64);
    assert_eq!(
        apply_replicated_batch(&repo.core, &events, 0, ApplyPolicy::Trusted)
            .await
            .unwrap(),
        ApplyOutcome::Skipped
    );
    assert_eq!(gate.version_allocation_high_water_mark(), n as u64);
    close(repo).await;
}

/// P1-1 (S5c-X round 5): the post-checkpoint retry fast path must not use
/// `clean_through` (J) as completion evidence. With an open gap the
/// checkpoint gap-caps J (`certified_clean_through`,
/// checkpoint.rs:233-248), so a COMPLETED, published chunk legitimately ends
/// with J below the chunk's local end. The fast path therefore recognises
/// completion exactly like the checkpoint Uncertain read-back
/// (checkpoint.rs:328-333): durable bookmark advanced AND receipt retired —
/// written in ONE atomic batch (checkpoint.rs:263-280) — plus the gate's
/// committed/watermark counters.
///
/// COUNTERFACTUAL: restoring the round-3 completion evidence
/// (`clean_through >= chunk_local_end`) in the fast path
/// (receipt/apply.rs:512-525) turns this test red — attempt 2 rejects the
/// fast path, `stage::commit_prepared` re-stages Prepared (and its receipt)
/// over the ALREADY published chunk, and the sticky final-flush failure
/// exhausts the 3-attempt budget, so the apply returns `DbError::Storage`
/// instead of `Applied { 2 }`.
#[tokio::test]
async fn post_checkpoint_flush_failure_with_open_gap_is_done() {
    struct FinalFlushReset(String);
    impl Drop for FinalFlushReset {
        fn drop(&mut self) {
            crate::tx::replication::follower_durability::receipt::checkpoint::set_fail_final_flush(
                &self.0, false,
            );
        }
    }
    let temp = tempfile::TempDir::new().unwrap();
    let name = repo_name(temp.path());
    let repo = open(temp.path(), &name).await;
    let key2 = key_bytes(2).await;
    assert_eq!(
        apply_replicated_batch(&repo.core, &[event(1, 1)], 0, ApplyPolicy::Trusted)
            .await
            .unwrap(),
        ApplyOutcome::Applied { local_version: 1 }
    );
    // Open a certified gap at version 2 (same seam as
    // follower_durability/tests/gap_tests.rs:58).
    {
        let feed = repo.changefeed().await.unwrap();
        feed.feed.mark_gap(2);
        assert_eq!(feed.feed.first_gap_version(), 2);
    }
    let _reset = FinalFlushReset(name.clone());
    set_fail_final_flush(&name, true);
    assert_eq!(
        apply_replicated_batch(&repo.core, &[event(2, 2)], 1, ApplyPolicy::Trusted)
            .await
            .unwrap(),
        ApplyOutcome::Applied { local_version: 2 }
    );
    set_fail_final_flush(&name, false);
    // The chunk's data is in place exactly once — NOT re-staged by a
    // `commit_prepared` retry.
    assert_eq!(history_versions_for(&repo, "items", &key2).await, vec![2]);
    assert_eq!(
        repo.get_table("items")
            .await
            .unwrap()
            .counter()
            .get()
            .await
            .unwrap(),
        2
    );
    let info = repo.tx_info_store().await.unwrap();
    // Gap-capped J: completion no longer rides on `clean_through`.
    let v3 = load_bookmark_v3(&info).await.unwrap().unwrap();
    assert_eq!(v3.bookmark, 2);
    assert_eq!(v3.clean_through, Some(1));
    assert_eq!(v3.outstanding_receipt, None);
    assert!(matches!(
        info.get(receipt_key()).await,
        Err(DbError::NotFound(_))
    ));
    assert_eq!(load_clean_horizon(&info).await.unwrap(), Some(1));
    drop(info);
    assert_eq!(
        apply_replicated_batch(&repo.core, &[event(2, 2)], 1, ApplyPolicy::Trusted)
            .await
            .unwrap(),
        ApplyOutcome::Skipped
    );
    close(repo).await;
    // Reopen: the advanced bookmark stayed durable.
    let repo = open(temp.path(), &name).await;
    let info = repo.tx_info_store().await.unwrap();
    let v3 = load_bookmark_v3(&info).await.unwrap().unwrap();
    assert_eq!(v3.bookmark, 2);
    assert_eq!(v3.outstanding_receipt, None);
    drop(info);
    close(repo).await;
}

//! In-process tests for the receipt apply pipeline (S5c-X PR6b-2): the
//! atomic Prepared/Materialized/checkpoint writes, the identical-allocation
//! resume, the changed-pull-boundary convergence, fail-closed identity
//! rejection, the Uncertain read-back reconcile, absolute counters, and the
//! paused-reservation write-domain state.
//!
//! Deterministic: the crash points park on the `test_seam::Hook`
//! notify/notify_waiters handshake — no sleeps, no yield budgets.

use bytes::Bytes;
use futures::StreamExt;
use shamir_storage::error::DbError;
use shamir_storage::types::RecordKey;
use shamir_tx::{decode_version_key, encode_version_key, ChangeOp, ChangelogEvent, RecordChange};
use shamir_types::access::Actor;
use shamir_types::codecs::basic::bincode as bincode_codec;
use shamir_types::types::record_id::RecordId;
use shamir_types::types::value::InnerValue;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::meta::repl::bookmark_v3::load_bookmark_v3;
use crate::meta::repl::receipt::receipt_key;
use crate::meta::repl::{decode_receipt, ReceiptPhase, MAX_RECEIPT_EVENTS};
use crate::meta::repl_bookmark::{save_fence_record_v3_aware, FenceRecord};
use crate::meta::MetaKey;
use crate::repo::RepoWriteError;
use crate::repo::{BoxRepoFactory, RepoInstance};
use crate::table::TableConfig;
use crate::tx::apply_replicated::follower_durability::test_seam::{self, Phase};
use crate::tx::apply_replicated::{apply_replicated_batch, ApplyOutcome, ApplyPolicy};
use crate::tx::ReplicatedEvent;

use crate::tx::replication::follower_durability::receipt::stage::set_simulate_uncertain;

struct FailFlushReset(String);
impl Drop for FailFlushReset {
    fn drop(&mut self) {
        crate::tx::replication::follower_durability::receipt::checkpoint::set_fail_final_flush(
            &self.0, false,
        );
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

fn delete_event(leader_version: u64, id: u8) -> ReplicatedEvent {
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
                op: ChangeOp::Delete,
                value: None,
            }],
        },
        field_bindings: Vec::new(),
    }
}

async fn open(path: &Path, name: &str) -> RepoInstance {
    RepoInstance::from_factory(
        name.into(),
        BoxRepoFactory::fjall_raw(path),
        vec![TableConfig::new("items")],
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

fn repo_name(tag: &str, path: &Path) -> String {
    format!("{tag}-{}", path.display().to_string().replace('\\', "_"))
}

async fn journal(repo: &RepoInstance, version: u64) -> Option<Bytes> {
    repo.follower_journal_store()
        .await
        .unwrap()
        .get(RecordKey::from_slice(&version.to_be_bytes()))
        .await
        .ok()
}

fn count_key() -> RecordKey {
    RecordKey::from_slice(MetaKey::Count.as_record_id().as_bytes())
}

async fn history_row(repo: &RepoInstance, key: &[u8], version: u64) -> Option<Bytes> {
    let store = repo.raw_store("__history__items").await.unwrap();
    store
        .get(RecordKey::from_slice(&encode_version_key(key, version)))
        .await
        .ok()
}

/// Every local version at which `key` has a history row.
async fn history_versions(repo: &RepoInstance, key: &[u8]) -> Vec<u64> {
    let store = repo.raw_store("__history__items").await.unwrap();
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

async fn persisted_count(repo: &RepoInstance) -> u64 {
    let store = repo.raw_store("__info__items").await.unwrap();
    let bytes = store.get(count_key()).await.unwrap();
    bincode_codec::from_bytes::<u64>(&bytes).unwrap()
}

/// `(bookmark v3, stored receipt phase)` — `None` when no receipt is stored.
async fn outstanding_state(repo: &RepoInstance) -> (Option<[u8; 16]>, Option<ReceiptPhase>) {
    let info = repo.tx_info_store().await.unwrap();
    let v3 = load_bookmark_v3(&info).await.unwrap();
    let Some(id) = v3.as_ref().and_then(|v3| v3.outstanding_receipt) else {
        return (None, None);
    };
    let bytes = match info.get(receipt_key()).await {
        Ok(bytes) => bytes,
        Err(DbError::NotFound(_)) => return (Some(id), None),
        Err(error) => panic!("receipt read failed: {error}"),
    };
    (Some(id), Some(decode_receipt(&id, &bytes).unwrap().phase))
}

/// Drive `events` through the repository-owned receipt worker until the
/// STICKY `fail_after = Phase::Prepared` flag exhausts the 3 Storage retries
/// of `commit_prepared`: the pipeline fails on EVERY attempt, so the worker
/// parks the chunk guards (never drops them), places the PAUSED RESERVATION
/// on the write domain (a receipt is outstanding), returns `Err(Storage)`,
/// goes IDLE, and leaves the durable Prepared receipt outstanding. Returns
/// the hook so the test can disarm the flag before re-applying.
async fn crashed_prepared(
    repo: &RepoInstance,
    name: &str,
    events: &[ReplicatedEvent],
) -> Arc<test_seam::Hook> {
    let hook = Arc::new(test_seam::Hook::default());
    *hook.fail_after.lock().unwrap() = Some(Phase::Prepared);
    hook.fail_after_sticky.store(true, Ordering::Release);
    test_seam::install(name, Arc::clone(&hook));
    let error = apply_replicated_batch(&repo.core, events, 0, ApplyPolicy::Trusted)
        .await
        .unwrap_err();
    assert!(
        matches!(error, DbError::Storage(_)),
        "expected the injected Storage failure, got {error:?}"
    );
    test_seam::remove(name);
    hook
}

/// Disarm the crashed_prepared flag so the re-apply converges.
fn disarm(hook: &test_seam::Hook) {
    *hook.fail_after.lock().unwrap() = None;
}

async fn key_bytes(id: u8) -> Vec<u8> {
    RecordId([id; 16]).to_bytes().to_vec()
}

// One fresh batch of two events goes through the whole pipeline atomically:
// journal rows, history rows, absolute counters, an advanced v3 bookmark
// with no outstanding receipt, and no receipt left behind. The outcome's
// `local_version` is the pipeline's allocation high-water mark — 2 for a
// two-event batch (versions 1 and 2).
#[tokio::test]
async fn fresh_apply_writes_receipt_pipeline_atomically() {
    let temp = tempfile::TempDir::new().unwrap();
    let name = repo_name("receipt-fresh", temp.path());
    let repo = open(temp.path(), &name).await;

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

    let info = repo.tx_info_store().await.unwrap();
    let v3 = load_bookmark_v3(&info).await.unwrap().unwrap();
    assert_eq!(v3.outstanding_receipt, None);
    assert_eq!(v3.bookmark, 2);
    assert_eq!(v3.clean_through, Some(2));
    assert!(matches!(
        info.get(receipt_key()).await,
        Err(DbError::NotFound(_))
    ));
    drop(info);

    let key1 = key_bytes(1).await;
    let key2 = key_bytes(2).await;
    assert!(journal(&repo, 1).await.is_some());
    assert!(journal(&repo, 2).await.is_some());
    assert!(history_row(&repo, &key1, 1).await.is_some());
    assert!(history_row(&repo, &key2, 2).await.is_some());
    assert_eq!(history_versions(&repo, &key1).await, vec![1]);
    assert_eq!(history_versions(&repo, &key2).await, vec![2]);
    let counter = repo.get_table("items").await.unwrap().counter().clone();
    assert_eq!(counter.get().await.unwrap(), 2);
    assert_eq!(persisted_count(&repo).await, 2);

    close(repo).await;
}

// A failure after the durable Prepared phase (all 3 retries of the
// idempotent commit_prepared injected-fail) parks the worker's guards and
// leaves the durable Prepared receipt outstanding; the disarmed re-apply
// resumes with the IDENTICAL local allocation: the replayed batch re-owns
// version 1 from the receipt instead of allocating a replacement. The
// history key therefore exists at exactly one version, 1.
#[tokio::test]
async fn crash_prepared_resumes_with_identical_allocation() {
    let temp = tempfile::TempDir::new().unwrap();
    let name = repo_name("receipt-resume", temp.path());
    let repo = open(temp.path(), &name).await;
    let key1 = key_bytes(1).await;

    let hook = crashed_prepared(&repo, &name, &[event(1, 1)]).await;
    let (outstanding, phase) = outstanding_state(&repo).await;
    assert!(outstanding.is_some());
    assert_eq!(phase, Some(ReceiptPhase::Prepared));
    assert!(history_row(&repo, &key1, 1).await.is_none());
    // The worker parked the chunk's guard — no Aborted mark advanced the
    // watermarks past the unpublished version, and the repo did not
    // fail-stop.
    let gate = repo.tx_gate().await.unwrap();
    assert_eq!(gate.last_committed(), 0);
    assert_eq!(gate.durable_watermark(), 0);
    assert!(!gate.requires_reopen());
    assert_eq!(repo.receipt_worker_parked_guards().await, Some(1));

    // Same open repo, flag disarmed: the warm resume path re-owns the
    // receipt's version through the paused-reservation admission; the parked
    // guard retires (≤ watermark, no-op) and the reservation clears.
    disarm(&hook);
    assert_eq!(
        apply_replicated_batch(&repo.core, &[event(1, 1)], 0, ApplyPolicy::Trusted)
            .await
            .unwrap(),
        ApplyOutcome::Applied { local_version: 1 }
    );
    assert_eq!(history_versions(&repo, &key1).await, vec![1]);
    assert!(history_row(&repo, &key1, 1).await.is_some());
    assert_eq!(repo.receipt_worker_parked_guards().await, Some(0));
    assert!(!repo.write_domain().has_outstanding_reservation());
    let (outstanding, phase) = outstanding_state(&repo).await;
    assert_eq!(outstanding, None);
    assert_eq!(phase, None);
    let info = repo.tx_info_store().await.unwrap();
    let v3 = load_bookmark_v3(&info).await.unwrap().unwrap();
    assert_eq!(v3.bookmark, 1);
    assert_eq!(v3.clean_through, Some(1));
    drop(info);

    close(repo).await;
}

// A replay with a SUPERSET of the crashed batch (changed pull boundary)
// converges: the receipt's versions 1 and 2 are reused and the tail event is
// allocated as 3 — no replacement allocations, no duplicates.
#[tokio::test]
async fn changed_pull_boundary_converges() {
    let temp = tempfile::TempDir::new().unwrap();
    let name = repo_name("receipt-superset", temp.path());
    let repo = open(temp.path(), &name).await;
    let keys = [key_bytes(1).await, key_bytes(2).await, key_bytes(3).await];

    // The sticky-fail worker pauses instead of a dropped caller.
    let hook = crashed_prepared(&repo, &name, &[event(1, 1), event(2, 2)]).await;
    disarm(&hook);
    assert_eq!(
        apply_replicated_batch(
            &repo.core,
            &[event(1, 1), event(2, 2), event(3, 3)],
            0,
            ApplyPolicy::Trusted,
        )
        .await
        .unwrap(),
        // The resumed receipt (versions 1-2) and the fresh tail (3) both
        // apply; the outcome reports the batch's final local version.
        ApplyOutcome::Applied { local_version: 3 }
    );
    for (id, key) in keys.iter().enumerate() {
        let expected = id as u64 + 1;
        assert_eq!(
            history_versions(&repo, key).await,
            vec![expected],
            "key {id} must exist at exactly version {expected}"
        );
    }
    assert!(journal(&repo, 1).await.is_some());
    assert!(journal(&repo, 2).await.is_some());
    assert!(journal(&repo, 3).await.is_some());
    let counter = repo.get_table("items").await.unwrap().counter().clone();
    assert_eq!(counter.get().await.unwrap(), 3);
    assert_eq!(repo.receipt_worker_parked_guards().await, Some(0));

    close(repo).await;
}

// A corrupted outstanding receipt is fail-closed: the resume lookup rejects
// the decode mismatch instead of replaying or discarding the batch.
#[tokio::test]
async fn identity_rejection_is_fail_closed() {
    let temp = tempfile::TempDir::new().unwrap();
    let name = repo_name("receipt-corrupt", temp.path());
    let repo = open(temp.path(), &name).await;

    crashed_prepared(&repo, &name, &[event(1, 1)]).await;
    let info = repo.tx_info_store().await.unwrap();
    info.set(
        receipt_key(),
        Bytes::from_static(b"corrupted receipt bytes"),
    )
    .await
    .unwrap();
    drop(info);

    let error = apply_replicated_batch(&repo.core, &[event(1, 1)], 0, ApplyPolicy::Trusted)
        .await
        .unwrap_err();
    assert!(
        matches!(error, DbError::Storage(_)),
        "expected a Storage decode error, got {error:?}"
    );

    close(repo).await;
}

// With the simulated-Uncertain seam on, the Materialized commit's read-back
// reconcile runs and still leaves exactly one set of rows: no double-write
// and no error. The outcome is the allocation high-water mark (2 for the
// two-event batch).
#[tokio::test]
async fn uncertain_materialization_reconciles_by_read_back() {
    set_simulate_uncertain(false);
    let temp = tempfile::TempDir::new().unwrap();
    let name = repo_name("receipt-uncertain", temp.path());
    let repo = open(temp.path(), &name).await;
    let keys = [key_bytes(1).await, key_bytes(2).await];

    set_simulate_uncertain(true);
    let outcome = apply_replicated_batch(
        &repo.core,
        &[event(1, 1), event(2, 2)],
        0,
        ApplyPolicy::Trusted,
    )
    .await;
    set_simulate_uncertain(false);
    assert_eq!(outcome.unwrap(), ApplyOutcome::Applied { local_version: 2 });
    assert_eq!(history_versions(&repo, &keys[0]).await, vec![1]);
    assert_eq!(history_versions(&repo, &keys[1]).await, vec![2]);
    let counter = repo.get_table("items").await.unwrap().counter().clone();
    assert_eq!(counter.get().await.unwrap(), 2);

    close(repo).await;
}

// A PUT followed by a DELETE of the same key lands the tombstone row
// (empty value), keeps the absolute counter at zero, and the published
// snapshot read returns None. The outcome is the allocation high-water
// mark — 2 for the two-event batch.
#[tokio::test]
async fn receipt_path_writes_absolute_counters() {
    let temp = tempfile::TempDir::new().unwrap();
    let name = repo_name("receipt-tombstone", temp.path());
    let repo = open(temp.path(), &name).await;
    let key = key_bytes(7).await;

    assert_eq!(
        apply_replicated_batch(
            &repo.core,
            &[event(1, 7), delete_event(2, 7)],
            0,
            ApplyPolicy::Trusted,
        )
        .await
        .unwrap(),
        ApplyOutcome::Applied { local_version: 2 }
    );

    let tombstone = history_row(&repo, &key, 2).await.unwrap();
    assert!(tombstone.is_empty(), "delete must write an empty value");
    let counter = repo.get_table("items").await.unwrap().counter().clone();
    assert_eq!(counter.get().await.unwrap(), 0);
    assert_eq!(persisted_count(&repo).await, 0);
    let table = repo.get_table("items").await.unwrap();
    assert!(table.get_bytes(RecordId([7; 16])).await.unwrap().is_none());
    drop(table);
}

// After a successful apply the counter's periodic persist cannot overwrite
// the committed absolute value: count stays put and the info-store blob
// equals it.
#[tokio::test]
async fn periodic_persist_cannot_overwrite_committed_counter() {
    let temp = tempfile::TempDir::new().unwrap();
    let name = repo_name("receipt-counter", temp.path());
    let repo = open(temp.path(), &name).await;

    apply_replicated_batch(
        &repo.core,
        &[event(1, 1), event(2, 2)],
        0,
        ApplyPolicy::Trusted,
    )
    .await
    .unwrap();
    let counter = repo.get_table("items").await.unwrap().counter().clone();
    assert_eq!(counter.get().await.unwrap(), 2);
    counter.persist().await.unwrap();
    assert_eq!(counter.get().await.unwrap(), 2);
    assert_eq!(persisted_count(&repo).await, 2);

    close(repo).await;
}

// Finding 2 regression (round 4, round-5 hardened, DISCRIMINATING): a
// STICKY failure injected after the durable Materialized commit of a SECOND
// batch must park the pipeline after 3 Storage attempts — including on the
// Materialized fast path, which previously BYPASSED the seam entirely:
//   attempt 1: fresh path — commit_materialized_work OK, seam fires
//              (Storage "injected") → inline retry
//   attempt 2: run_chunk_tail Materialized fast path → round-5 seam check
//              fires BEFORE durable_tail → retry
//   attempt 3: fast path → seam fires → attempt >= 3 → PARK, return Err.
// Paused state: the Materialized receipt is outstanding, nothing of the
// second batch is published, the paused RESERVATION rejects ordinary write
// admission, and the repo keeps serving (no reopen).
//
// Counter oracle (review finding 2): before resuming, the LIVE counter is
// perturbed (via the doctor's absolute-set method `set_to_admitted`) to a
// value equal to NEITHER the recorded `before` nor the recorded `after`.
// The Materialized resume must replay the receipt's RECORDED absolute
// counters (`publish::adopt_counters` ← `publication_from_receipt`), never
// recompute from the live cache — a recompute-based retry would publish the
// perturbed value and fail the final assertion.
#[tokio::test]
async fn materialized_pause_hits_injected_failure_replays_recorded_counters() {
    let temp = tempfile::TempDir::new().unwrap();
    let name = repo_name("receipt-pause-oracle", temp.path());
    let repo = open(temp.path(), &name).await;
    let key1 = key_bytes(1).await;
    let key2 = key_bytes(2).await;
    let key3 = key_bytes(3).await;

    // FIRST batch publishes cleanly: the counter cache becomes 1 (the
    // floor the paused batch's counters must be built ON).
    assert_eq!(
        apply_replicated_batch(&repo.core, &[event(1, 1)], 0, ApplyPolicy::Trusted)
            .await
            .unwrap(),
        ApplyOutcome::Applied { local_version: 1 }
    );
    let counter = repo.get_table("items").await.unwrap().counter().clone();
    assert_eq!(counter.get().await.unwrap(), 1);
    drop(counter);

    let hook = Arc::new(test_seam::Hook::default());
    *hook.fail_after.lock().unwrap() = Some(Phase::Materialized);
    // STICKY: a one-shot flag would be consumed by the first retry, and the
    // pipeline retries Storage failures INLINE — the first apply would then
    // succeed within the same call instead of pausing.
    hook.fail_after_sticky.store(true, Ordering::Release);
    test_seam::install(&name, Arc::clone(&hook));
    let error = apply_replicated_batch(
        &repo.core,
        &[event(2, 2), event(3, 3)],
        1,
        ApplyPolicy::Trusted,
    )
    .await
    .unwrap_err();
    test_seam::remove(&name);
    assert!(
        matches!(&error, DbError::Storage(message) if message == "injected"),
        "the injected Storage failure must actually be HIT after the \
         3-strike park, got {error:?}"
    );

    // Paused state: the Materialized receipt is outstanding and owns the
    // versions; nothing of the SECOND batch is published; the worker holds
    // the parked guards; the PAUSED RESERVATION rejects ordinary write
    // admission (PR6b-2c); and the pause is an ordinary pause, NOT a
    // quarantine — the repo still serves.
    let (outstanding, phase) = outstanding_state(&repo).await;
    assert!(outstanding.is_some());
    assert_eq!(phase, Some(ReceiptPhase::Materialized));
    assert!(
        repo.receipt_worker_parked_guards().await.unwrap_or(0) > 0,
        "the 3-strike park must hold parked guards"
    );
    assert!(repo.write_domain().has_outstanding_reservation());
    let gate = repo.tx_gate().await.unwrap();
    assert_eq!(gate.last_committed(), 1);
    assert_eq!(gate.durable_watermark(), 1);
    assert!(!gate.requires_reopen());
    let domain = repo.write_domain().clone();
    let admitted = tokio::spawn(async move { domain.admit().await });
    assert!(
        matches!(
            admitted.await.unwrap(),
            Err(RepoWriteError::Reserved { .. })
        ),
        "ordinary admission must be rejected with RepoWriteError::Reserved \
         while the paused reservation is outstanding"
    );
    let table = repo.get_table("items").await.unwrap();
    assert!(table.get_bytes(RecordId([2; 16])).await.unwrap().is_none());
    assert!(table.get_bytes(RecordId([3; 16])).await.unwrap().is_none());
    drop(table);

    // Read the recorded counters straight from the durable Materialized
    // receipt: this is the oracle the resume must replay.
    let info = repo.tx_info_store().await.unwrap();
    let id = outstanding.unwrap();
    let receipt = decode_receipt(&id, &info.get(receipt_key()).await.unwrap()).unwrap();
    drop(info);
    assert_eq!(receipt.counters.len(), 1, "one table, one counter entry");
    let before = receipt.counters[0].before;
    let after = receipt.counters[0].after;
    assert_eq!(receipt.counters[0].table, "items");
    assert_eq!(before, 1);
    assert_eq!(after, 3);

    // THE PERTURBATION: set the LIVE counter (cache AND durable blob, the
    // doctor's absolute-set method) to a value equal to neither the
    // recorded `before` nor `after`. The paused reservation blocks ordinary
    // admission, so the doctor's own `set_to` cannot run — take the
    // reservation-ignoring close permit (test-only need) and use the
    // admitted twin.
    let perturbed = after + 100;
    assert_ne!(perturbed, before);
    assert_ne!(perturbed, after);
    let close_permit = repo.write_domain().admit_for_close().await;
    let counter = repo.get_table("items").await.unwrap().counter().clone();
    counter
        .set_to_admitted(perturbed, &close_permit)
        .await
        .unwrap();
    drop(close_permit);
    assert_eq!(counter.get().await.unwrap(), perturbed);

    // Journal snapshot BEFORE the resume: the Materialized commit already
    // wrote the exact downstream journal bytes — the resume must not touch
    // them.
    let journal_two = journal(&repo, 2).await.unwrap();
    let journal_three = journal(&repo, 3).await.unwrap();
    assert!(!journal_two.is_empty() && !journal_three.is_empty());

    // Clear the sticky seam and re-apply the SAME batch: the resume admits
    // THROUGH the paused reservation (`admit_with_reservation`) and replays
    // the stored receipt.
    disarm(&hook);
    assert_eq!(
        apply_replicated_batch(
            &repo.core,
            &[event(2, 2), event(3, 3)],
            1,
            ApplyPolicy::Trusted
        )
        .await
        .unwrap(),
        ApplyOutcome::Applied { local_version: 3 }
    );

    // Counter oracle: the replay must surface the receipt's recorded
    // `after` — NOT the perturbed value a recompute-from-cache would
    // produce.
    assert_eq!(
        counter.get().await.unwrap(),
        after,
        "the resume must replay the RECORDED counters, not recompute from \
         the perturbed live cache"
    );

    // Structural invariants: exactly one history version per key (a
    // recompute-based retry would re-write the durable batch), the journal
    // bytes untouched, and the pause fully unwound.
    assert_eq!(history_versions(&repo, &key1).await, vec![1]);
    assert_eq!(history_versions(&repo, &key2).await, vec![2]);
    assert_eq!(history_versions(&repo, &key3).await, vec![3]);
    assert_eq!(journal(&repo, 2).await.unwrap(), journal_two);
    assert_eq!(journal(&repo, 3).await.unwrap(), journal_three);
    assert_eq!(gate.last_committed(), 3);
    assert_eq!(gate.durable_watermark(), 3);
    assert!(!gate.requires_reopen());
    assert!(!repo.write_domain().has_outstanding_reservation());
    assert_eq!(repo.receipt_worker_parked_guards().await, Some(0));
    let info = repo.tx_info_store().await.unwrap();
    let v3 = load_bookmark_v3(&info).await.unwrap().unwrap();
    assert_eq!(v3.bookmark, 3);
    assert_eq!(v3.clean_through, Some(3));
    assert_eq!(v3.outstanding_receipt, None);
    assert!(matches!(
        info.get(receipt_key()).await,
        Err(DbError::NotFound(_))
    ));
    drop(info);
    // Durable count row: `adopt_absolute` repairs the LIVE counter from the
    // receipt but is RAM-only by design — the Materialized receipt is
    // already durable, and re-writing its rows (including the count blob
    // the perturbation overwrote) would be a pure rewrite of committed
    // state. The blob therefore holds the perturbation until the counter
    // is next dirty; assert that exact shipped behavior and that the
    // counter's own bookkeeping treats the replayed value as the persisted
    // one.
    assert_eq!(
        persisted_count(&repo).await,
        perturbed,
        "the RAM-only counter adoption must not rewrite the durable blob"
    );
    counter.persist().await.unwrap();
    assert_eq!(counter.get().await.unwrap(), after);

    close(repo).await;
}

// Reviewer P2 #8: a NON-retryable injected error (`RepoRequiresReopen` kind)
// after the durable Prepared commit must NOT enter the 3-attempt Storage
// retry loop — the first failure parks the chunk guards and returns
// immediately. The durable Prepared receipt stays outstanding, the
// watermarks stay at 0, the repo keeps serving (no crash, no reopen), and
// the disarmed re-apply converges on the receipt's own version.
#[tokio::test]
async fn non_retryable_error_parks_guards() {
    let temp = tempfile::TempDir::new().unwrap();
    let name = repo_name("receipt-nonretry", temp.path());
    let repo = open(temp.path(), &name).await;
    let key1 = key_bytes(1).await;

    let hook = Arc::new(test_seam::Hook::default());
    *hook.fail_after.lock().unwrap() = Some(Phase::Prepared);
    hook.fail_after_sticky.store(true, Ordering::Release);
    // NON-retryable kind: the retry loop only retries `DbError::Storage`.
    hook.fail_after_reopen.store(true, Ordering::Release);
    test_seam::install(&name, Arc::clone(&hook));
    let error = apply_replicated_batch(&repo.core, &[event(1, 1)], 0, ApplyPolicy::Trusted)
        .await
        .unwrap_err();
    test_seam::remove(&name);
    assert!(
        matches!(&error, DbError::RepoRequiresReopen(message) if message == "injected"),
        "expected the injected RepoRequiresReopen failure, got {error:?}"
    );

    // The FIRST attempt already parked: the durable Prepared receipt is
    // outstanding, nothing published, watermarks 0, guard held — no crash,
    // no reopen (the parks keep the repo serving).
    let (outstanding, phase) = outstanding_state(&repo).await;
    assert!(outstanding.is_some());
    assert_eq!(phase, Some(ReceiptPhase::Prepared));
    let gate = repo.tx_gate().await.unwrap();
    assert_eq!(gate.last_committed(), 0);
    assert_eq!(gate.durable_watermark(), 0);
    assert!(!gate.requires_reopen());
    assert_eq!(repo.receipt_worker_parked_guards().await, Some(1));
    assert!(history_row(&repo, &key1, 1).await.is_none());

    // Disarm: the resume re-owns the receipt's version and converges; the
    // parked guard retires.
    disarm(&hook);
    assert_eq!(
        apply_replicated_batch(&repo.core, &[event(1, 1)], 0, ApplyPolicy::Trusted)
            .await
            .unwrap(),
        ApplyOutcome::Applied { local_version: 1 }
    );
    assert_eq!(history_versions(&repo, &key1).await, vec![1]);
    assert_eq!(repo.receipt_worker_parked_guards().await, Some(0));

    close(repo).await;
}

#[tokio::test]
async fn upstream_bookmark_does_not_skip_unpublished_local_chunk() {
    let temp = tempfile::TempDir::new().unwrap();
    let name = repo_name("receipt-upstream-offset", temp.path());
    let repo = open(temp.path(), &name).await;
    let key1 = key_bytes(1).await;
    let key2 = key_bytes(2).await;
    let key3 = key_bytes(3).await;
    let _reset = FailFlushReset(name.clone());
    crate::tx::replication::follower_durability::receipt::checkpoint::set_fail_final_flush(
        &name, true,
    );
    let outcome = apply_replicated_batch(
        &repo.core,
        &[event(100, 1), event(101, 2)],
        0,
        ApplyPolicy::Trusted,
    )
    .await;
    crate::tx::replication::follower_durability::receipt::checkpoint::set_fail_final_flush(
        &name, false,
    );
    assert_eq!(outcome.unwrap(), ApplyOutcome::Applied { local_version: 2 });
    let info = repo.tx_info_store().await.unwrap();
    let v3 = load_bookmark_v3(&info).await.unwrap().unwrap();
    assert_eq!(v3.bookmark, 101);
    assert_eq!(v3.clean_through, Some(2));
    assert_eq!(v3.outstanding_receipt, None);
    assert!(matches!(
        info.get(receipt_key()).await,
        Err(DbError::NotFound(_))
    ));
    drop(info);
    assert_eq!(history_versions(&repo, &key1).await, vec![1]);
    assert_eq!(history_versions(&repo, &key2).await, vec![2]);
    let table = repo.get_table("items").await.unwrap();
    assert!(table.get_bytes(RecordId([1; 16])).await.unwrap().is_some());
    assert!(table.get_bytes(RecordId([2; 16])).await.unwrap().is_some());
    drop(table);
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
    assert_eq!(
        apply_replicated_batch(&repo.core, &[event(102, 3)], 101, ApplyPolicy::Trusted)
            .await
            .unwrap(),
        ApplyOutcome::Applied { local_version: 3 }
    );
    assert_eq!(history_versions(&repo, &key3).await, vec![3]);
    assert!(journal(&repo, 3).await.is_some());
    assert_eq!(
        repo.get_table("items")
            .await
            .unwrap()
            .counter()
            .get()
            .await
            .unwrap(),
        3
    );
    close(repo).await;
}

#[tokio::test]
async fn paused_reservation_survives_a_failed_resume() {
    let temp = tempfile::TempDir::new().unwrap();
    let name = repo_name("receipt-pause", temp.path());
    let repo = open(temp.path(), &name).await;

    // Sticky Prepared failure parks the worker and write-domain reservation.
    let hook = crashed_prepared(&repo, &name, &[event(1, 1)]).await;
    assert!(repo.write_domain().has_outstanding_reservation());
    disarm(&hook);
    let error = apply_replicated_batch(&repo.core, &[event(9, 9)], 0, ApplyPolicy::Trusted)
        .await
        .unwrap_err();
    assert!(
        matches!(error, DbError::Storage(_)),
        "expected the identity mismatch refusal, got {error:?}"
    );
    assert!(repo.write_domain().has_outstanding_reservation());
    assert_eq!(
        apply_replicated_batch(&repo.core, &[event(1, 1)], 0, ApplyPolicy::Trusted)
            .await
            .unwrap(),
        ApplyOutcome::Applied { local_version: 1 }
    );
    assert!(!repo.write_domain().has_outstanding_reservation());
    assert_eq!(repo.receipt_worker_parked_guards().await, Some(0));

    close(repo).await;
}

#[tokio::test]
async fn fenced_multi_chunk_apply_keeps_v3_invariant_across_reopen() {
    let temp = tempfile::TempDir::new().unwrap();
    let name = repo_name("receipt-fence", temp.path());
    let repo = open(temp.path(), &name).await;

    // Receipt-native v3 record with a fence at bookmark 1.
    assert_eq!(
        apply_replicated_batch(&repo.core, &[event(1, 1)], 0, ApplyPolicy::Trusted)
            .await
            .unwrap(),
        ApplyOutcome::Applied { local_version: 1 }
    );
    let info = repo.tx_info_store().await.unwrap();
    save_fence_record_v3_aware(
        &info,
        FenceRecord {
            bookmark: 1,
            lineage_id: [7u8; 16],
            history_epoch: 0,
            observed_identity: None,
        },
    )
    .await
    .unwrap();
    drop(info);

    // A one-op event costs 3 + 2*1 = 5 combined units, so a chunk holds
    // MAX_RECEIPT_EVENTS/5 events; N above that + 2 forces >= 2 chunks.
    let n = MAX_RECEIPT_EVENTS / 5 + 3;
    let batch: Vec<ReplicatedEvent> = (2..(2 + n as u64))
        .map(|leader_version| event(leader_version, (leader_version % 251) as u8))
        .collect();
    assert_eq!(batch.len(), n);
    assert_eq!(
        apply_replicated_batch(&repo.core, &batch, 0, ApplyPolicy::Trusted)
            .await
            .unwrap(),
        ApplyOutcome::Applied {
            local_version: 1 + n as u64
        }
    );

    let expected_bookmark = 1 + n as u64;
    let info = repo.tx_info_store().await.unwrap();
    let v3 = load_bookmark_v3(&info).await.unwrap().unwrap();
    assert_eq!(v3.bookmark, expected_bookmark);
    let fence = v3.fence.expect("fence must survive the chunks");
    assert_eq!(fence.bookmark, v3.bookmark);
    assert_eq!(fence.lineage_id, [7u8; 16]);
    assert_eq!(v3.outstanding_receipt, None);
    drop(info);

    close(repo).await;
    let repo = open(temp.path(), &name).await;
    let info = repo.tx_info_store().await.unwrap();
    let v3 = load_bookmark_v3(&info).await.unwrap().unwrap();
    assert_eq!(v3.bookmark, expected_bookmark);
    assert_eq!(v3.fence.map(|fence| fence.bookmark), Some(v3.bookmark));
    assert_eq!(v3.outstanding_receipt, None);
    drop(info);

    assert_eq!(
        apply_replicated_batch(
            &repo.core,
            &[event(1 + n as u64, ((1 + n as u64) % 251) as u8)],
            0,
            ApplyPolicy::Trusted
        )
        .await
        .unwrap(),
        ApplyOutcome::Skipped
    );

    close(repo).await;
}

#[tokio::test]
async fn post_checkpoint_flush_failure_fresh_chunk_is_done() {
    let temp = tempfile::TempDir::new().unwrap();
    let name = repo_name("receipt-fresh-final-flush", temp.path());
    let repo = open(temp.path(), &name).await;
    let keys = [key_bytes(1).await, key_bytes(2).await];
    let events = [event(1, 1), event(2, 2)];
    let _reset = FailFlushReset(name.clone());
    crate::tx::replication::follower_durability::receipt::checkpoint::set_fail_final_flush(
        &name, true,
    );
    assert_eq!(
        apply_replicated_batch(&repo.core, &events, 0, ApplyPolicy::Trusted)
            .await
            .unwrap(),
        ApplyOutcome::Applied { local_version: 2 }
    );
    crate::tx::replication::follower_durability::receipt::checkpoint::set_fail_final_flush(
        &name, false,
    );
    for (key, version) in keys.iter().zip([1, 2]) {
        assert_eq!(history_versions(&repo, key).await, vec![version]);
    }
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
    let v3 = load_bookmark_v3(&info).await.unwrap().unwrap();
    assert_eq!(v3.bookmark, 2);
    assert_eq!(v3.outstanding_receipt, None);
    assert!(matches!(
        info.get(receipt_key()).await,
        Err(DbError::NotFound(_))
    ));
    drop(info);
    assert!(!repo.write_domain().has_outstanding_reservation());
    assert_eq!(
        apply_replicated_batch(&repo.core, &events, 0, ApplyPolicy::Trusted)
            .await
            .unwrap(),
        ApplyOutcome::Skipped
    );
    close(repo).await;
    let repo = open(temp.path(), &name).await;
    let info = repo.tx_info_store().await.unwrap();
    let v3 = load_bookmark_v3(&info).await.unwrap().unwrap();
    assert_eq!(v3.bookmark, 2);
    assert_eq!(v3.outstanding_receipt, None);
    drop(info);
    close(repo).await;
}

#[tokio::test]
async fn post_checkpoint_flush_failure_resumed_chunk_is_done() {
    let temp = tempfile::TempDir::new().unwrap();
    let name = repo_name("receipt-final-flush", temp.path());
    let repo = open(temp.path(), &name).await;
    let key1 = key_bytes(1).await;
    let hook = crashed_prepared(&repo, &name, &[event(1, 1)]).await;
    disarm(&hook);
    let _reset = FailFlushReset(name.clone());
    crate::tx::replication::follower_durability::receipt::checkpoint::set_fail_final_flush(
        &name, true,
    );
    let error = apply_replicated_batch(&repo.core, &[event(1, 1)], 0, ApplyPolicy::Trusted)
        .await
        .unwrap_err();
    crate::tx::replication::follower_durability::receipt::checkpoint::set_fail_final_flush(
        &name, false,
    );
    assert!(matches!(error, DbError::Storage(_)));
    assert!(!repo.write_domain().has_outstanding_reservation());
    let permit = repo.write_domain().admit().await.unwrap();
    drop(permit);
    assert_eq!(
        apply_replicated_batch(&repo.core, &[event(1, 1)], 0, ApplyPolicy::Trusted)
            .await
            .unwrap(),
        ApplyOutcome::Skipped
    );
    assert_eq!(history_versions(&repo, &key1).await, vec![1]);
    let info = repo.tx_info_store().await.unwrap();
    assert_eq!(load_bookmark_v3(&info).await.unwrap().unwrap().bookmark, 1);
    assert!(matches!(
        info.get(receipt_key()).await,
        Err(DbError::NotFound(_))
    ));
    drop(info);

    close(repo).await;
}

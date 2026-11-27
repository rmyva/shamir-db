use std::future::{poll_fn, Future};
use std::sync::atomic::Ordering;

use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use shamir_query_builder::{select, Query};
use shamir_storage::error::{DbError, DbResult};
use shamir_tx::{IsolationLevel, TxContext};
use shamir_types::types::common::new_map;
use shamir_types::types::record_id::RecordId;
use shamir_types::types::value::InnerValue;

use crate::query::filter::FilterContext;
use crate::query::read::{QueryResult, ReadQuery};
use crate::repo::RepoInstance;
use crate::table::TableManager;
use crate::tx::commit::TxError;
use crate::tx::materialize::TEST_PRE_MATERIALIZE_REPO;
use crate::tx::tests::drain::orphans::post_wal_orphan_ordering_tests::fixture;
use crate::tx::{MaterializationState, TxOutcome};

// Observe the actual driver poll after its own version settles, not scheduling.
pub(crate) async fn spawn_settled_commit(
    repo: &RepoInstance,
    tx: TxContext,
) -> (u64, JoinHandle<Result<TxOutcome, TxError>>) {
    let gate = repo.tx_gate().await.unwrap();
    let version = gate.version_allocation_high_water_mark() + 1;
    let owned = repo.clone();
    let (sender, receiver) = oneshot::channel();
    let task = tokio::spawn(async move {
        let commit = owned.commit_tx(tx);
        tokio::pin!(commit);
        let mut sender = Some(sender);
        poll_fn(|cx| {
            let result = commit.as_mut().poll(cx);
            if gate.last_committed() >= version && !gate.is_active_post_wal(version) {
                if let Some(sender) = sender.take() {
                    sender.send(result.is_pending()).unwrap();
                }
            }
            result
        })
        .await
    });
    assert!(
        receiver.await.unwrap(),
        "ack must wait for the lower version"
    );
    (version, task)
}

async fn point(table: &TableManager, id: RecordId, tx: &TxContext) -> DbResult<Option<InnerValue>> {
    match table.read_one_tx(id, Some(tx)).await {
        Ok(value) => Ok(Some(value)),
        Err(DbError::NotFound(_)) => Ok(None),
        Err(error) => Err(error),
    }
}

async fn read(table: &TableManager, query: &ReadQuery, tx: &TxContext) -> DbResult<QueryResult> {
    let interner = table.interner().get().await?;
    let refs = new_map();
    let ctx = FilterContext::new(interner, &refs);
    table.read_tx(query, &ctx, Some(tx)).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_reads_stay_identical_after_earlier_parked_commit_materializes() {
    parked_snapshot_reads_and_own_commit().await;
}

async fn parked_snapshot_reads_and_own_commit() {
    let (repo, hook) = fixture("snapshot_visibility_park").await;
    TEST_PRE_MATERIALIZE_REPO
        .set(repo.name().to_owned())
        .unwrap();
    let a = repo.get_table("a").await.unwrap();
    let b = repo.get_table("b").await.unwrap();
    let gate = repo.tx_gate().await.unwrap();
    let interner = a.interner().get().await.unwrap();
    let mut row = new_map();
    row.insert(
        interner.touch_ind("score").unwrap().into_key(),
        InnerValue::Int(10),
    );
    a.interner().persist().await.unwrap();
    let a_value = InnerValue::Map(row);
    let scan_query = Query::from("a").select(["score"]).build();
    let count_query = Query::from("a")
        .select([select::count_all("answer")])
        .build();

    let (mut tx_a, guard_a) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let a_id = a.insert_tx(&a_value, Some(&mut tx_a)).await.unwrap();
    let completion_before = gate.completion().watermark();
    hook.armed.store(true, Ordering::Release);
    let owned = repo.clone();
    let task_a = tokio::spawn(async move { owned.commit_tx(tx_a).await });
    hook.reached.notified().await;
    let a_version = *repo.drainer().window_keys().last().unwrap();
    assert!(gate.is_active_post_wal(a_version));
    assert!(completion_before < a_version);
    assert_eq!(gate.completion().watermark(), completion_before);
    assert_eq!(a.mvcc_store_ref().unwrap().version_of(a_id.as_bytes()), 0);

    let (mut tx_b, guard_b) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let b_value = InnerValue::Str("later-b".into());
    let b_id = b.insert_tx(&b_value, Some(&mut tx_b)).await.unwrap();
    let (b_version, task_b) = spawn_settled_commit(&repo, tx_b).await;
    assert_eq!(b_version, a_version + 1);
    assert_eq!(gate.last_committed(), b_version);
    assert_eq!(gate.completion().watermark(), completion_before);
    assert!(!task_b.is_finished());

    let (tx_c, guard_c) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let c_version = tx_c.snapshot_version;
    assert!(c_version <= gate.completion().watermark());
    assert_eq!(guard_c.version(), c_version);
    assert!(c_version < a_version);
    assert!(gate.completion().watermark() < a_version);
    let point_before = point(&a, a_id, &tx_c).await;
    let scan_before = read(&a, &scan_query, &tx_c).await;
    let count_before = read(&a, &count_query, &tx_c).await;

    hook.resume.notify_one();
    let outcome_a = task_a.await.unwrap().unwrap();
    let outcome_b = task_b.await.unwrap().unwrap();
    assert_eq!(outcome_b.commit_version, b_version);
    assert!(gate.visible_floor() >= b_version);
    assert_eq!(outcome_a.commit_version, a_version);
    assert_eq!(outcome_a.materialization, MaterializationState::Complete);
    assert!(outcome_a.background.is_none());
    assert!(!gate.is_active_post_wal(a_version));
    assert_eq!(gate.completion().watermark(), outcome_b.commit_version);
    assert_eq!(gate.last_committed(), outcome_b.commit_version);
    assert_eq!(
        a.mvcc_store_ref().unwrap().version_of(a_id.as_bytes()),
        a_version,
    );
    assert_eq!(tx_c.snapshot_version, c_version);
    assert_eq!(guard_c.version(), c_version);
    let point_after = point(&a, a_id, &tx_c).await;
    let scan_after = read(&a, &scan_query, &tx_c).await;
    let count_after = read(&a, &count_query, &tx_c).await;

    let (tx_d, guard_d) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    assert_eq!(point(&b, b_id, &tx_d).await.unwrap(), Some(b_value));
    let point_d = point(&a, a_id, &tx_d).await;
    let scan_d = read(&a, &scan_query, &tx_d).await;
    let count_d = read(&a, &count_query, &tx_d).await;
    assert_eq!(tx_d.snapshot_version, outcome_b.commit_version);
    assert!(a_version <= tx_d.snapshot_version);
    assert_eq!(point_d.unwrap(), Some(a_value));
    let scan_d = scan_d.unwrap();
    assert_eq!(scan_d.records.len(), 1);
    assert_eq!(scan_d.records[0].get_value_i64("score"), Some(10));
    let count_d = count_d.unwrap();
    assert_eq!(count_d.records.len(), 1);
    assert_eq!(count_d.records[0].get_value_i64("answer"), Some(1));

    let point_before = point_before.unwrap();
    let scan_before = scan_before.unwrap();
    let count_before = count_before.unwrap();
    let point_after = point_after.unwrap();
    let scan_after = scan_after.unwrap();
    let count_after = count_after.unwrap();
    assert_eq!(point_before, None);
    assert!(scan_before.records.is_empty());
    assert_eq!(count_before.records.len(), 1);
    assert_eq!(count_before.records[0].get_value_i64("answer"), Some(0));
    assert_eq!(
        (point_after, scan_after.records, count_after.records),
        (point_before, scan_before.records, count_before.records),
        "Snapshot C point read, full scan and COUNT must remain identical",
    );
    drop((guard_a, guard_b, guard_c, guard_d));
}

// Red before SNAP-3: Serializable C pins at eager B, outside A's predicate window.
#[tokio::test]
async fn serializable_begin_below_parked_commit_preserves_predicate_window() {
    let (repo, hook) = fixture("serializable_visibility_park").await;
    repo.add_table_for_test(crate::table::TableConfig::new("c"));
    let a = repo.get_table("a").await.unwrap();
    let b = repo.get_table("b").await.unwrap();
    let c = repo.get_table("c").await.unwrap();
    let gate = repo.tx_gate().await.unwrap();
    let (mut tx_a, pin_a) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    tx_a.set_visibility(shamir_tx::CommitVisibility::AsyncIndex);
    a.insert_tx(&InnerValue::Str("A".into()), Some(&mut tx_a))
        .await
        .unwrap();
    hook.armed.store(true, Ordering::Release);
    let owned = repo.clone();
    let task_a = tokio::spawn(async move { owned.commit_tx(tx_a).await });
    hook.reached.notified().await;
    let version_a = gate.version_allocation_high_water_mark();
    assert!(gate.try_commit_lock().is_none());
    let (mut tx_b, pin_b) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    b.insert_tx(&InnerValue::Str("B".into()), Some(&mut tx_b))
        .await
        .unwrap();
    let (_, task_b) = spawn_settled_commit(&repo, tx_b).await;
    let (mut tx_c, pin_c) = repo.begin_tx(IsolationLevel::Serializable).await.unwrap();
    assert!(tx_c.snapshot_version < version_a);
    assert!(read(&a, &Query::from("a").build(), &tx_c)
        .await
        .unwrap()
        .records
        .is_empty());
    let c_id = c
        .insert_tx(&InnerValue::Str("C".into()), Some(&mut tx_c))
        .await
        .unwrap();
    hook.resume.notify_one();
    let mut outcome_a = task_a.await.unwrap().unwrap();
    if let Some(tail) = outcome_a.take_background() {
        assert_eq!(tail.join().await, MaterializationState::Complete);
    }
    task_b.await.unwrap().unwrap();
    let result = repo.commit_tx(tx_c).await;
    match result {
        Ok(outcome) => {
            assert!(outcome.snapshot_version < version_a);
            assert_eq!(c.get(c_id).await.unwrap(), InnerValue::Str("C".into()));
        }
        Err(error) => assert!(matches!(
            error,
            TxError::SsiConflict { .. } | TxError::PhantomConflict { .. }
        )),
    }
    drop((pin_a, pin_b, pin_c));
}

// PR-7 invariant: lower finalization preserves the higher cell version and
// latest value, even when the lower writer materializes last.
#[tokio::test]
async fn snapshot_same_key_lower_version_released_last_keeps_higher_value() {
    let (repo, hook) = fixture("snapshot_same_key_order").await;
    TEST_PRE_MATERIALIZE_REPO
        .set(repo.name().to_owned())
        .unwrap();
    let table = repo.get_table("a").await.unwrap();
    let gate = repo.tx_gate().await.unwrap();
    let (mut seed, seed_pin) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let id = table
        .insert_tx(&InnerValue::Str("seed".into()), Some(&mut seed))
        .await
        .unwrap();
    repo.commit_tx(seed).await.unwrap();
    let (mut lower, lower_pin) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let (mut higher, higher_pin) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    table
        .set_tx(id, &InnerValue::Str("lower".into()), Some(&mut lower))
        .await
        .unwrap();
    table
        .set_tx(id, &InnerValue::Str("higher".into()), Some(&mut higher))
        .await
        .unwrap();
    hook.armed.store(true, Ordering::Release);
    let owned = repo.clone();
    let task_lower = tokio::spawn(async move { owned.commit_tx(lower).await });
    hook.reached.notified().await;
    let lower_version = gate.version_allocation_high_water_mark();
    let (higher_version, task_higher) = spawn_settled_commit(&repo, higher).await;
    assert_eq!(higher_version, lower_version + 1);
    hook.resume.notify_one();
    task_lower.await.unwrap().unwrap();
    task_higher.await.unwrap().unwrap();
    assert_eq!(
        table.mvcc_store_ref().unwrap().version_of(id.as_bytes()),
        higher_version,
    );
    assert_eq!(
        table.get(id).await.unwrap(),
        InnerValue::Str("higher".into()),
    );
    drop((seed_pin, lower_pin, higher_pin));
}

#[tokio::test]
async fn begin_after_own_commit_waits_for_watermark_and_sees_own_row() {
    parked_snapshot_reads_and_own_commit().await;
}

// A WRITTEN sync failure is still committed; its typed error is an ack too.
#[tokio::test]
async fn wal_durability_error_waits_for_lower_visibility_before_returning() {
    let (repo, hook) = fixture("snapshot_durability_ack").await;
    crate::tx::commit::TEST_SYNCED_WAL_REPO
        .set(repo.name().to_owned())
        .unwrap();
    TEST_PRE_MATERIALIZE_REPO
        .set(repo.name().to_owned())
        .unwrap();
    let a = repo.get_table("a").await.unwrap();
    let b = repo.get_table("b").await.unwrap();
    let gate = repo.tx_gate().await.unwrap();
    let (mut tx_a, pin_a) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    a.insert_tx(&InnerValue::Str("A".into()), Some(&mut tx_a))
        .await
        .unwrap();
    hook.armed.store(true, Ordering::Release);
    let owned = repo.clone();
    let task_a = tokio::spawn(async move { owned.commit_tx(tx_a).await });
    hook.reached.notified().await;
    let (mut tx_b, pin_b) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let value = InnerValue::Str("written-not-durable".into());
    let id = b.insert_tx(&value, Some(&mut tx_b)).await.unwrap();
    repo.repo_wal().await.unwrap().arm_fail_next_sync_for_test();
    let (version, task_b) = spawn_settled_commit(&repo, tx_b).await;
    assert!(gate.visible_floor() < version);
    hook.resume.notify_one();
    task_a.await.unwrap().unwrap();
    let error = task_b.await.unwrap().unwrap_err();
    assert!(matches!(
        error,
        TxError::WalDurability { commit_version, .. } if commit_version == version
    ));
    assert!(gate.visible_floor() >= version);
    let (reader, reader_pin) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    assert_eq!(point(&b, id, &reader).await.unwrap(), Some(value));
    drop((pin_a, pin_b, reader_pin));
}

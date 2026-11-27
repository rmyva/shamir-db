use shamir_query_builder::Query;
use shamir_tx::{IsolationLevel, Retention};
use shamir_types::types::common::new_map;
use shamir_types::types::value::InnerValue;

use crate::query::filter::FilterContext;
use crate::tx::commit::TxError;
use crate::tx::tests::drain::orphans::post_wal_orphan_ordering_tests::fixture;
use crate::tx::tests::recovery::snapshot_gc::snapshot_visibility_park_tests::spawn_settled_commit;

// Catches eager GC floors and either vacuum path deleting the K@5 anchor.
#[tokio::test]
async fn no_snapshot_gc_preserves_key_at_watermark_after_nontx_v11_v12() {
    let (repo, _) = fixture("snapshot_gc_anchor").await;
    let gate = repo.tx_gate().await.unwrap();
    let table = repo.get_table("a").await.unwrap();
    let mvcc = table.mvcc_store_ref().unwrap();
    mvcc.set_test_now(1_000);
    mvcc.set_retention(Retention::current_only()).unwrap();
    for expected in 1..=4 {
        let unused = gate.assign_next_version_guarded();
        assert_eq!(unused.version(), expected);
        drop(unused);
    }
    let original = InnerValue::Str("K@5".into());
    let id = table.insert(&original).await.unwrap();
    assert_eq!(mvcc.version_of(id.as_bytes()), 5);
    for expected in 6..=9 {
        let unused = gate.assign_next_version_guarded();
        assert_eq!(unused.version(), expected);
        drop(unused);
    }
    let parked = gate.assign_next_version_guarded();
    assert_eq!(parked.version(), 10);
    assert!(gate.active_snapshots_empty());
    // Non-tx writes acknowledge only once W covers their version (SNAP-5), and
    // parked v10 holds W below them: spawn each write and wait until it has
    // PUBLISHED its version, never await it inline.
    let mut writers = Vec::new();
    for expected in 11..=12i64 {
        let writer_repo = repo.clone();
        writers.push(tokio::spawn(async move {
            let table = writer_repo.get_table("a").await.unwrap();
            table
                .set_returning_version(id, &InnerValue::Int(expected))
                .await
        }));
        let published = u64::try_from(expected).unwrap();
        for _ in 0..10_000 {
            if mvcc.version_of(id.as_bytes()) == published {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(mvcc.version_of(id.as_bytes()), published);
    }
    // Non-tx cell/history completion does not eagerly publish across a hole.
    // Exercise the documented eager floor independently of contiguous W.
    gate.raise_published(12);
    assert_eq!(gate.last_committed(), 12);
    assert_eq!(gate.visible_floor(), 9);
    assert_eq!(gate.min_alive(), 9);
    assert!(gate.active_snapshots_empty());
    repo.run_gc().await.unwrap();

    let (reader, pin) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    assert_eq!(reader.snapshot_version, 9);
    assert_eq!(
        table.read_one_tx(id, Some(&reader)).await.unwrap(),
        original
    );
    drop((reader, pin));
    drop(parked);
    for (writer, expected) in writers.into_iter().zip(11u64..) {
        let (_, version) = writer.await.unwrap().unwrap();
        assert_eq!(version, expected);
    }
    assert_eq!(gate.visible_floor(), 12);
}

// Catches pruning v11's real Serializable footprint to eager publication when
// no snapshot is alive: a later transaction at W must reject its table scan.
#[tokio::test]
async fn serializable_v11_footprint_survives_empty_snapshot_gc_and_rejects_commit() {
    let (repo, _) = fixture("snapshot_gc_serializable").await;
    let gate = repo.tx_gate().await.unwrap();
    let a = repo.get_table("a").await.unwrap();
    let b = repo.get_table("b").await.unwrap();
    a.mvcc_store_ref().unwrap().set_test_now(1_000);
    b.mvcc_store_ref().unwrap().set_test_now(1_000);
    for expected in 1..=9 {
        let unused = gate.assign_next_version_guarded();
        assert_eq!(unused.version(), expected);
        drop(unused);
    }
    let parked = gate.assign_next_version_guarded();
    assert_eq!(parked.version(), 10);
    let (mut writer, writer_pin) = repo.begin_tx(IsolationLevel::Serializable).await.unwrap();
    b.insert_tx(&InnerValue::Str("v11".into()), Some(&mut writer))
        .await
        .unwrap();
    let (version, task) = spawn_settled_commit(&repo, writer).await;
    assert_eq!(version, 11);
    drop(writer_pin);
    assert!(gate.active_snapshots_empty());
    assert_eq!(gate.min_alive(), 9);
    assert_eq!(gate.commit_log_len(), 1);
    repo.run_gc().await.unwrap();
    assert_eq!(gate.commit_log_len(), 1);

    let (mut reader, reader_pin) = repo.begin_tx(IsolationLevel::Serializable).await.unwrap();
    assert_eq!(reader.snapshot_version, 9);
    let interner = b.interner().get().await.unwrap();
    let refs = new_map();
    let context = FilterContext::new(interner, &refs);
    let query = Query::from("b").build();
    assert!(b
        .read_tx(&query, &context, Some(&reader))
        .await
        .unwrap()
        .records
        .is_empty());
    a.insert_tx(
        &InnerValue::Str("must-not-commit".into()),
        Some(&mut reader),
    )
    .await
    .unwrap();
    // Release the unrelated hole before awaiting T, so removing predicate
    // validation fails the error assertion rather than hanging in T's ack.
    // T retains snapshot 9; releasing v10 cannot erase its conflict window.
    drop(parked);
    let result = repo.commit_tx(reader).await;
    let outcome = task.await.unwrap().unwrap();
    assert_eq!(outcome.commit_version, 11);
    assert!(
        matches!(&result, Err(TxError::PhantomConflict { .. })),
        "{result:?}",
    );
    drop(reader_pin);
}

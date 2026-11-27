use std::future::{poll_fn, Future};
use std::sync::atomic::Ordering;

use shamir_query_builder::{select, Query};
use shamir_tx::{IsolationLevel, TxContext};
use shamir_types::types::common::new_map;
use shamir_types::types::record_id::RecordId;
use shamir_types::types::value::InnerValue;
use tokio::sync::oneshot;

use crate::query::filter::FilterContext;
use crate::query::read::ReadQuery;
use crate::repo::RepoInstance;
use crate::table::{TableConfig, TableManager};
use crate::tx::materialize::TEST_PRE_MATERIALIZE_REPO;
use crate::tx::tests::drain::orphans::post_wal_orphan_ordering_tests::fixture;

async fn scores(table: &TableManager, query: &ReadQuery, tx: &TxContext) -> Vec<i64> {
    let interner = table.interner().get().await.unwrap();
    let refs = new_map();
    let context = FilterContext::new(interner, &refs);
    let result = table.read_tx(query, &context, Some(tx)).await.unwrap();
    let mut values: Vec<_> = result
        .records
        .iter()
        .map(|record| record.get_value_i64("score").unwrap())
        .collect();
    // Scan order is not part of the snapshot contract.
    values.sort_unstable();
    values
}

async fn observe(
    repo: &RepoInstance,
    point_table: &TableManager,
    id: RecordId,
    tx: &TxContext,
) -> (InnerValue, Vec<i64>, i64) {
    let table = repo.get_table("w0").await.unwrap();
    let scan = Query::from("w0").select(["score"]).build();
    let count = Query::from("w0")
        .select([select::count_all("answer")])
        .build();
    let point = point_table.read_one_tx(id, Some(tx)).await.unwrap();
    let rows = scores(&table, &scan, tx).await;
    let interner = table.interner().get().await.unwrap();
    let refs = new_map();
    let context = FilterContext::new(interner, &refs);
    let count = table.read_tx(&count, &context, Some(tx)).await.unwrap();
    assert_eq!(count.records.len(), 1);
    let count = count.records[0].get_value_i64("answer").unwrap();
    assert_eq!(usize::try_from(count).unwrap(), rows.len());
    (point, rows, count)
}

// Catches eager snapshot assignment, premature ack and late rows appearing
// inside an existing snapshot. Fixed seed controls yields, not task scheduling.
// Eight parked rounds overlap four disjoint writers and two snapshot readers:
// 192 ordinary commits plus the seed and eight parked commits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn seeded_writers_and_parked_commits_preserve_repeated_snapshot_reads() {
    let (repo, hook) = fixture("snapshot_seeded_stress").await;
    TEST_PRE_MATERIALIZE_REPO
        .set(repo.name().to_owned())
        .unwrap();
    let gate = repo.tx_gate().await.unwrap();
    let point_table = repo.get_table("a").await.unwrap();
    point_table.mvcc_store_ref().unwrap().set_test_now(1_000);
    let (mut seed, seed_pin) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let id = point_table
        .insert_tx(&InnerValue::Int(0), Some(&mut seed))
        .await
        .unwrap();
    let seeded = repo.commit_tx(seed).await.unwrap();
    assert!(gate.visible_floor() >= seeded.commit_version);
    drop(seed_pin);
    for writer in 0..4 {
        repo.add_table_for_test(TableConfig::new(format!("w{writer}")));
        let table = repo.get_table(&format!("w{writer}")).await.unwrap();
        table.mvcc_store_ref().unwrap().set_test_now(1_000);
        table
            .interner()
            .get()
            .await
            .unwrap()
            .touch_ind("score")
            .unwrap();
        table.interner().persist().await.unwrap();
    }

    for round in 0..8_i64 {
        let (mut parked, parked_pin) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
        point_table
            .set_tx(id, &InnerValue::Int(round + 1), Some(&mut parked))
            .await
            .unwrap();
        hook.armed.store(true, Ordering::Release);
        let owned = repo.clone();
        let parked_task = tokio::spawn(async move { owned.commit_tx(parked).await });
        hook.reached.notified().await;
        let parked_version = gate.version_allocation_high_water_mark();
        assert!(gate.visible_floor() < parked_version);

        let mut writers = Vec::new();
        for writer in 0..4_i64 {
            let owned = repo.clone();
            let (settled_tx, settled_rx) = oneshot::channel();
            let task = tokio::spawn(async move {
                let gate = owned.tx_gate().await.unwrap();
                let table = owned.get_table(&format!("w{writer}")).await.unwrap();
                let key = table
                    .interner()
                    .get()
                    .await
                    .unwrap()
                    .touch_ind("score")
                    .unwrap()
                    .into_key();
                let mut settled_tx = Some(settled_tx);
                let mut seed = 0x66_1315_u64 ^ u64::try_from(writer + round * 4).unwrap();
                for iteration in 0..6_i64 {
                    seed ^= seed << 13;
                    seed ^= seed >> 7;
                    seed ^= seed << 17;
                    let (mut tx, pin) = owned.begin_tx(IsolationLevel::Snapshot).await.unwrap();
                    assert!(tx.snapshot_version <= gate.visible_floor());
                    let mut row = new_map();
                    row.insert(key.clone(), InnerValue::Int(round * 6 + iteration));
                    let row_id = table
                        .insert_tx(&InnerValue::Map(row), Some(&mut tx))
                        .await
                        .unwrap();
                    if seed & 1 == 0 {
                        tokio::task::yield_now().await;
                    }
                    let commit = owned.commit_tx(tx);
                    tokio::pin!(commit);
                    // Signal the actual pending ack poll, never infer progress
                    // from a yield count. This writer owns its table exclusively.
                    let outcome = poll_fn(|cx| {
                        let result = commit.as_mut().poll(cx);
                        let version = table
                            .mvcc_store_ref()
                            .unwrap()
                            .version_of(row_id.as_bytes());
                        if version > parked_version && !gate.is_active_post_wal(version) {
                            if let Some(sender) = settled_tx.take() {
                                sender.send((version, result.is_pending())).unwrap();
                            }
                        }
                        result
                    })
                    .await
                    .unwrap();
                    assert!(gate.visible_floor() >= outcome.commit_version);
                    drop(pin);
                }
            });
            writers.push((settled_rx, task));
        }
        for (settled, _) in &mut writers {
            let (version, pending) = settled.await.unwrap();
            assert!(version > parked_version);
            assert!(pending, "ack crossed the parked lower version");
        }
        let mut readers = Vec::new();
        for _ in 0..2 {
            let owned = repo.clone();
            let point_table = point_table.clone();
            let (ready_tx, ready_rx) = oneshot::channel();
            let (resume_tx, resume_rx) = oneshot::channel();
            let task = tokio::spawn(async move {
                let gate = owned.tx_gate().await.unwrap();
                let (reader, pin) = owned.begin_tx(IsolationLevel::Snapshot).await.unwrap();
                assert!(reader.snapshot_version <= gate.visible_floor());
                assert!(reader.snapshot_version < parked_version);
                let before = observe(&owned, &point_table, id, &reader).await;
                assert_eq!(before.0, InnerValue::Int(round));
                ready_tx.send(()).unwrap();
                resume_rx.await.unwrap();
                let after = observe(&owned, &point_table, id, &reader).await;
                assert_eq!(after, before);
                drop(pin);
            });
            readers.push((ready_rx, resume_tx, task));
        }
        for (ready, _, _) in &mut readers {
            ready.await.unwrap();
        }

        hook.resume.notify_one();
        let outcome = parked_task.await.unwrap().unwrap();
        assert_eq!(outcome.commit_version, parked_version);
        assert!(gate.visible_floor() >= parked_version);
        for (_, task) in writers {
            task.await.unwrap();
        }
        for (_, resume, task) in readers {
            resume.send(()).unwrap();
            task.await.unwrap();
        }
        drop(parked_pin);
        repo.drainer().drain_all(&repo.core).await.unwrap();
    }
    let (reader, pin) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let (point, rows, count) = observe(&repo, &point_table, id, &reader).await;
    assert_eq!(point, InnerValue::Int(8));
    assert_eq!(rows, (0..48).collect::<Vec<i64>>());
    assert_eq!(count, 48);
    drop(pin);
}

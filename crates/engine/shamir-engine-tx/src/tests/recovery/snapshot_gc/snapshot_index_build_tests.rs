use std::sync::atomic::Ordering;

use shamir_index::IndexState;
use shamir_tx::IsolationLevel;
use shamir_types::types::common::new_map;
use shamir_types::types::value::InnerValue;

use crate::tx::materialize::TEST_PRE_MATERIALIZE_REPO;
use crate::tx::tests::drain::orphans::post_wal_orphan_ordering_tests::fixture;
use crate::tx::tests::recovery::snapshot_gc::snapshot_visibility_park_tests::spawn_settled_commit;

// Catches replacing the explicit allocation-high-water build pin with W:
// v11 predates Building, so no dirty-set catchup can repair an omitted row.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ready_online_index_contains_v11_while_v10_on_other_table_is_parked() {
    let (repo, hook) = fixture("snapshot_index_high_water").await;
    TEST_PRE_MATERIALIZE_REPO
        .set(repo.name().to_owned())
        .unwrap();
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
    let (mut lower, lower_pin) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    a.insert_tx(&InnerValue::Str("parked".into()), Some(&mut lower))
        .await
        .unwrap();
    hook.armed.store(true, Ordering::Release);
    let owned = repo.clone();
    let lower_task = tokio::spawn(async move { owned.commit_tx(lower).await });
    hook.reached.notified().await;
    assert_eq!(gate.version_allocation_high_water_mark(), 10);

    let interner = b.interner().get().await.unwrap();
    let mut row = new_map();
    row.insert(
        interner.touch_ind("name").unwrap().into_key(),
        InnerValue::Str("included".into()),
    );
    b.interner().persist().await.unwrap();
    let (mut higher, higher_pin) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let id = b
        .insert_tx(&InnerValue::Map(row), Some(&mut higher))
        .await
        .unwrap();
    let (version, higher_task) = spawn_settled_commit(&repo, higher).await;
    assert_eq!(version, 11);
    assert_eq!(gate.visible_floor(), 9);
    assert_eq!(b.mvcc_store_ref().unwrap().version_of(id.as_bytes()), 11);

    b.create_index("by_name", &["name"]).await.unwrap();
    let index_id = interner.touch_ind("by_name").unwrap().into_key().id();
    let definition = b
        .index_manager_ref()
        .iter_indexes()
        .find(|index| index.name_interned == index_id)
        .unwrap();
    assert_eq!(definition.state, IndexState::Ready);
    let postings = b
        .index_manager_ref()
        .lookup_by_index(index_id, &[InnerValue::Str("included".into())])
        .await
        .unwrap()
        .unwrap();
    assert_eq!(postings.iter().copied().collect::<Vec<_>>(), vec![id]);
    assert_eq!(gate.visible_floor(), 9);
    assert!(!higher_task.is_finished());
    hook.resume.notify_one();
    lower_task.await.unwrap().unwrap();
    higher_task.await.unwrap().unwrap();
    drop((lower_pin, higher_pin));
}

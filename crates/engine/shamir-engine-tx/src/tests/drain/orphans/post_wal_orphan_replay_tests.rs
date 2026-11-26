use bytes::Bytes;
use shamir_tx::{IndexWriteOp, IsolationLevel};
use shamir_types::types::value::InnerValue;

use crate::table::table_manager::table_token_for;
use crate::table::TableConfig;
use crate::tx::tests::drain::orphans::post_wal_orphan_ordering_tests::{fixture, panic_post_wal};
use crate::tx::tests::recovery::snapshot_gc::snapshot_visibility_park_tests::spawn_settled_commit;

fn posting(key: &Bytes, value: &'static [u8]) -> IndexWriteOp {
    IndexWriteOp::SetPosting {
        key: key.clone(),
        value: Bytes::from_static(value),
        provenance: Default::default(),
    }
}

// Counterfactual: previously the missing history target was skipped and the
// orphan became durable, losing both the retry and the restored-table value.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_orphan_table_stops_and_restored_registration_retries() {
    let (repo, hook) = fixture("orphan_missing_restored").await;
    let table = repo.get_table("a").await.unwrap();
    let gate = repo.tx_gate().await.unwrap();
    let key = Bytes::from_static(b"restored-posting");
    // A healthy published prefix must drain even when the following orphan
    // cannot resolve its table. A later orphan must remain behind that gap.
    let b = repo.get_table("b").await.unwrap();
    let (mut prefix_tx, prefix_snapshot) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let prefix_rid = b
        .insert_tx(&InnerValue::Str("prefix".into()), Some(&mut prefix_tx))
        .await
        .unwrap();
    let prefix = repo.commit_tx(prefix_tx).await.unwrap().commit_version;
    drop(prefix_snapshot);
    let (mut tx, snapshot) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let rid = table
        .insert_tx(&InnerValue::Str("restored".into()), Some(&mut tx))
        .await
        .unwrap();
    tx.index_write_set
        .push((table_token_for("a"), posting(&key, b"restored")));
    let version = panic_post_wal(&repo, &hook, tx).await;
    drop(snapshot);
    let durable = gate.durable_watermark();
    assert!(repo.remove_table_for_test("a"));
    let (mut later_tx, later_snapshot) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let later_rid = b
        .insert_tx(&InnerValue::Str("later-orphan".into()), Some(&mut later_tx))
        .await
        .unwrap();
    let later = panic_post_wal(&repo, &hook, later_tx).await;
    drop(later_snapshot);
    assert_eq!(gate.durable_watermark(), durable);
    assert_eq!(repo.drainer().drain_step(&repo.core).await.unwrap(), 1);
    assert_eq!(gate.durable_watermark(), prefix);
    assert_eq!(
        b.get(prefix_rid).await.unwrap(),
        InnerValue::Str("prefix".into()),
    );
    assert_eq!(repo.drainer().drain_all(&repo.core).await.unwrap(), 0);
    assert_eq!(gate.durable_watermark(), prefix);
    assert!(gate.is_orphaned_post_wal(later));
    assert!(gate.is_orphaned_post_wal(version));
    assert!(repo.drainer().window_entry(version).is_some());
    assert_eq!(repo.drainer().drain_step(&repo.core).await.unwrap(), 0);

    repo.add_table_for_test(TableConfig::new("a"));
    let restored = repo.get_table("a").await.unwrap();
    assert_eq!(repo.drainer().drain_all(&repo.core).await.unwrap(), 2);
    assert_eq!(gate.durable_watermark(), later);
    assert!(!gate.is_orphaned_post_wal(version));
    assert_eq!(
        b.get(later_rid).await.unwrap(),
        InnerValue::Str("later-orphan".into()),
    );
    assert_eq!(
        restored.get(rid).await.unwrap(),
        InnerValue::Str("restored".into()),
    );
    assert_eq!(
        restored.info_store().get(key.into()).await.unwrap(),
        Bytes::from_static(b"restored"),
    );
    assert_eq!(restored.counter().get().await.unwrap(), 1);
    assert_eq!(repo.drainer().drain_all(&repo.core).await.unwrap(), 0);
}

// Counterfactuals: unconditional replay resurrects the deleted posting;
// table-wide suppression loses the unrelated orphan posting. Dropped offers
// must retain inline key tracking, and pruning at the orphan version must
// retain the later deletion until its own durable watermark is reached.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn orphan_postings_are_key_scoped_and_survive_backpressure_and_pruning() {
    let (repo, hook) = fixture("orphan_posting_granularity").await;
    let a = repo.get_table("a").await.unwrap();
    let b = repo.get_table("b").await.unwrap();
    let gate = repo.tx_gate().await.unwrap();
    let token = table_token_for("a");
    let key = Bytes::from_static(b"shared-key");
    let untouched = Bytes::from_static(b"untouched-key");
    let (mut tx, snapshot) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    a.insert_tx(&InnerValue::Str("orphan".into()), Some(&mut tx))
        .await
        .unwrap();
    tx.index_write_set.push((token, posting(&key, b"old")));
    tx.index_write_set
        .push((token, posting(&untouched, b"independent")));
    tx.index_write_set
        .push((table_token_for("b"), posting(&key, b"other-table")));
    let orphan = panic_post_wal(&repo, &hook, tx).await;
    drop(snapshot);

    repo.drainer().set_window_high_watermark(0);
    let (mut tx, snapshot) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    a.insert_tx(&InnerValue::Str("later".into()), Some(&mut tx))
        .await
        .unwrap();
    // Ensure RemovePosting has an existing target; the orphan has not applied it.
    a.info_store()
        .set(key.clone().into(), Bytes::from_static(b"seed"))
        .await
        .unwrap();
    tx.index_write_set.push((
        token,
        IndexWriteOp::RemovePosting {
            key: key.clone(),
            provenance: Default::default(),
            owner: None,
        },
    ));
    let (later, later_task) = spawn_settled_commit(&repo, tx).await;
    assert!(!later_task.is_finished());
    drop(snapshot);
    assert!(later > orphan);
    assert!(repo.drainer().window_entry(later).is_none());
    assert_eq!(repo.drainer().drain_step(&repo.core).await.unwrap(), 1);
    assert_eq!(later_task.await.unwrap().unwrap().commit_version, later);
    assert_eq!(gate.durable_watermark(), orphan);
    assert_eq!(
        repo.drainer().posting_version_for_test(token, &key),
        Some(later),
    );
    assert!(a.info_store().get(key.clone().into()).await.is_err());
    assert_eq!(
        a.info_store().get(untouched.clone().into()).await.unwrap(),
        Bytes::from_static(b"independent"),
    );
    assert_eq!(
        b.info_store().get(key.clone().into()).await.unwrap(),
        Bytes::from_static(b"other-table"),
    );
    assert_eq!(repo.drainer().drain_all(&repo.core).await.unwrap(), 1);
    assert_eq!(gate.durable_watermark(), later);
    assert!(a.info_store().get(key.clone().into()).await.is_err());
    assert!(repo
        .drainer()
        .posting_version_for_test(token, &key)
        .is_none());

    // After pruning, a newer orphan must be free to apply this same key.
    repo.drainer().set_window_high_watermark(64);
    let (mut tx, snapshot) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    a.insert_tx(&InnerValue::Str("new-orphan".into()), Some(&mut tx))
        .await
        .unwrap();
    tx.index_write_set.push((token, posting(&key, b"new")));
    let newest = panic_post_wal(&repo, &hook, tx).await;
    drop(snapshot);
    assert_eq!(repo.drainer().drain_all(&repo.core).await.unwrap(), 1);
    assert_eq!(gate.durable_watermark(), newest);
    assert_eq!(
        a.info_store().get(key.clone().into()).await.unwrap(),
        Bytes::from_static(b"new"),
    );
    assert!(repo
        .drainer()
        .posting_version_for_test(token, &key)
        .is_none());
}

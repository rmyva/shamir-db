use shamir_tx::IsolationLevel;
use shamir_types::types::value::InnerValue;

use crate::tests::drain::orphans::post_wal_orphan_ordering_tests::{fixture, panic_post_wal};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn orphan_repair_metrics_count_repair_once_and_not_on_replay() {
    let (repo, hook) = fixture("orphan_repair_metrics").await;
    let table = repo.get_table("a").await.unwrap();
    let (mut tx, snapshot) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    table
        .insert_tx(&InnerValue::Str("metrics".into()), Some(&mut tx))
        .await
        .unwrap();
    let version = panic_post_wal(&repo, &hook, tx).await;
    drop(snapshot);

    assert_eq!(repo.drainer().drain_all(&repo.core).await.unwrap(), 1);
    assert_eq!(repo.drainer().orphan_repair_metrics.snapshot(), (1, 0));
    assert_eq!(repo.drainer().drain_all(&repo.core).await.unwrap(), 0);
    assert_eq!(repo.drainer().orphan_repair_metrics.snapshot(), (1, 0));
    assert!(repo.drainer().window_entry(version).is_none());
}

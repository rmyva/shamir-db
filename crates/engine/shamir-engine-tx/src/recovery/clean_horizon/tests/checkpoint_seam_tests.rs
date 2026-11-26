use crate::clean_horizon::checkpoint::test_seam;
use crate::drainer::Drainer;
use crate::repo::{BoxRepoFactory, DurabilityPolicy, RepoInstance};
use crate::table::TableConfig;
use crate::tests::drain::truncation::truncation_tests::shamirwal_seg_count;
use shamir_tx::IsolationLevel;
use shamir_types::types::value::InnerValue;

async fn seeded_repo(name: &str, dir: &std::path::Path) -> RepoInstance {
    let repo = RepoInstance::from_factory_with_test_segment_bytes(
        name.into(),
        BoxRepoFactory::fjall_raw(dir.to_path_buf()),
        vec![TableConfig::new("t")],
        DurabilityPolicy::default(),
        256,
    )
    .await
    .unwrap();
    repo.disable_background_drainer_for_test();
    let table = repo.get_table("t").await.unwrap();
    for index in 0..80 {
        let (mut tx, _guard) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
        table
            .insert_tx(
                &InnerValue::Str(format!("seed-{index}-{}", "x".repeat(512))),
                Some(&mut tx),
            )
            .await
            .unwrap();
        repo.commit_tx(tx).await.unwrap();
    }
    repo
}

#[tokio::test(flavor = "current_thread")]
async fn checkpoint_samples_w_before_changefeed_pending_gap_recheck() {
    let temp = tempfile::TempDir::new().unwrap();
    let dir = temp.path().join("pending_gap_order");
    let repo = seeded_repo("pending_gap_order", &dir).await;
    let _ = test_seam::take_order(&repo.core);
    repo.record_pending_changefeed_gap(7);
    let pending = repo.clone();
    test_seam::on_sample_w(&repo.core, move || {
        pending.record_pending_changefeed_gap(11)
    });

    assert!(crate::clean_horizon::checkpoint::checkpoint_before_truncate(&repo.core).await);
    let feed = repo.changefeed().await.unwrap().feed;
    assert_eq!(feed.first_gap_version(), 7);
    assert_eq!(feed.certified_gap_interval(), Some((7, 11)));
    let order = test_seam::take_order(&repo.core);
    let sampled = order
        .iter()
        .position(|phase| *phase == "sampled_w")
        .unwrap();
    let recheck = order
        .iter()
        .position(|phase| *phase == "before_feed_init")
        .unwrap();
    assert!(sampled < recheck);
}

#[tokio::test(flavor = "current_thread")]
async fn checkpoint_failure_prevents_unlink_then_retry_catches_up() {
    let temp = tempfile::TempDir::new().unwrap();
    let dir = temp.path().join("checkpoint_failure");
    let repo = seeded_repo("checkpoint_failure", &dir).await;
    let feed = repo.changefeed().await.unwrap().feed;
    let before_j = feed.clean_horizon();
    let before_floor = repo.core.checkpoint_allocation_floor().await.unwrap();
    let before = shamirwal_seg_count(&dir);
    assert!(before > 0);
    test_seam::with_state(&repo.core, |state| state.fail_checkpoint = true);
    Drainer::new().drain_all(&repo.core).await.unwrap();
    assert_eq!(shamirwal_seg_count(&dir), before);
    assert_eq!(feed.clean_horizon(), before_j);
    assert_eq!(
        repo.core.checkpoint_allocation_floor().await.unwrap(),
        before_floor,
    );
    assert!(repo.wal_retention_gate().checkpoint_failures_total() > 0);
    test_seam::with_state(&repo.core, |state| state.fail_checkpoint = false);
    Drainer::new().drain_all(&repo.core).await.unwrap();
    assert!(shamirwal_seg_count(&dir) < before);
}

#[tokio::test(flavor = "current_thread")]
async fn checkpoint_order_and_unprotected_negative_control() {
    let temp = tempfile::TempDir::new().unwrap();
    let dir = temp.path().join("checkpoint_order");
    let repo = seeded_repo("checkpoint_order", &dir).await;
    test_seam::with_state(&repo.core, |state| {
        state.fail_checkpoint = false;
        state.bypass_checkpoint = false;
        state.order.clear();
    });
    Drainer::new().drain_all(&repo.core).await.unwrap();
    let order = test_seam::take_order(&repo.core);
    let cp = order
        .iter()
        .position(|phase| *phase == "checkpoint")
        .unwrap();
    let unlink = order.iter().position(|phase| *phase == "truncate").unwrap();
    assert!(cp < unlink);

    let negative_dir = temp.path().join("checkpoint_negative");
    let repo = seeded_repo("checkpoint_negative", &negative_dir).await;
    let negative_feed = repo.changefeed().await.unwrap().feed;
    let initial_j = negative_feed.clean_horizon();
    let before = shamirwal_seg_count(&negative_dir);
    test_seam::with_state(&repo.core, |state| state.bypass_checkpoint = true);
    Drainer::new().drain_all(&repo.core).await.unwrap();
    assert!(shamirwal_seg_count(&negative_dir) < before);
    assert_eq!(negative_feed.clean_horizon(), initial_j);
}

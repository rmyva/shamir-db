use std::path::Path;
use std::sync::Arc;

use shamir_query_types::{ReplicationServe, RetentionOverflow, WalCommitTier};
use shamir_types::types::value::InnerValue;
use tokio::sync::oneshot;

use crate::repo::{BoxRepoFactory, DurabilityPolicy, RepoInstance};
use crate::table::TableConfig;
use crate::tx::clean_horizon::checkpoint::test_seam;
use crate::tx::clean_horizon::retention::enforce_retention;
use crate::tx::IsolationLevel;

async fn disk_repo(path: &Path, mode: RetentionOverflow) -> RepoInstance {
    RepoInstance::from_factory_with_test_segment_bytes(
        "pin-wiring".into(),
        BoxRepoFactory::fjall_raw(path.to_path_buf()),
        vec![TableConfig::new("t")],
        DurabilityPolicy::new(
            WalCommitTier::Buffered,
            512,
            mode,
            ReplicationServe::Durable,
        ),
        256,
    )
    .await
    .unwrap()
}

async fn seed(repo: &RepoInstance) {
    let table = repo.get_table("t").await.unwrap();
    for index in 0..12 {
        let (mut tx, _guard) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
        table
            .insert_tx(
                &InnerValue::Str(format!("{index}-{}", "x".repeat(512))),
                Some(&mut tx),
            )
            .await
            .unwrap();
        repo.commit_tx(tx).await.unwrap();
    }
    repo.repo_interner().await.unwrap().persist().await.unwrap();
}

fn segment_count(path: &Path) -> usize {
    let mut dir = path.file_name().unwrap().to_os_string();
    dir.push(".shamirwal");
    std::fs::read_dir(path.with_file_name(dir))
        .unwrap()
        .map(Result::unwrap)
        .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "wal"))
        .count()
}

async fn prepare_retention(repo: &RepoInstance) -> u64 {
    let wal = repo.repo_wal().await.unwrap();
    for entry in wal.recover().await.unwrap() {
        crate::tx::replay_v2_entry(&entry, &repo.core)
            .await
            .unwrap();
    }
    repo.repo_interner().await.unwrap().persist().await.unwrap();
    let gate = repo.tx_gate().await.unwrap();
    let safe = gate.completion().watermark();
    gate.mark_durable(safe);
    safe
}

#[tokio::test]
async fn drainer_retains_zero_floor_then_resumes_without_new_commits() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("drain");
    let repo = disk_repo(&path, RetentionOverflow::RefuseWrites).await;
    let clone = repo.clone();
    assert!(Arc::ptr_eq(
        &repo.core.truncation_pins,
        &clone.core.truncation_pins
    ));
    let pin = clone.core.pin_wal_history(0).await.unwrap();
    seed(&repo).await;
    let before = segment_count(&path);
    assert!(before > 2, "fixture must seal multiple segments");
    let drainer = repo.drainer();
    drainer.drain_all(&repo.core).await.unwrap();
    assert_eq!(segment_count(&path), before);
    assert_eq!(
        repo.repo_wal()
            .await
            .unwrap()
            .recover()
            .await
            .unwrap()
            .len(),
        12
    );
    let gate = repo.tx_gate().await.unwrap();
    assert_eq!(gate.durable_watermark(), gate.last_committed());
    drop(pin);
    drainer.drain_all(&repo.core).await.unwrap();
    assert!(segment_count(&path) < before);
    assert!(
        repo.repo_wal()
            .await
            .unwrap()
            .recover()
            .await
            .unwrap()
            .len()
            < 12
    );
}

#[tokio::test]
async fn drainer_resamples_pin_after_checkpoint_and_flush() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("late-drain");
    let repo = disk_repo(&path, RetentionOverflow::RefuseWrites).await;
    seed(&repo).await;
    let before = segment_count(&path);
    assert!(before > 2);
    let (entered_tx, entered_rx) = oneshot::channel();
    let (resume_tx, resume_rx) = oneshot::channel();
    test_seam::with_state(&repo.core, |state| {
        state.before_delete = Some((entered_tx, resume_rx))
    });
    let worker_repo = repo.clone();
    let worker = tokio::spawn(async move {
        worker_repo
            .drainer()
            .drain_all(&worker_repo.core)
            .await
            .unwrap();
    });
    entered_rx.await.unwrap();
    let pin = repo.core.pin_wal_history(0).await.unwrap();
    resume_tx.send(()).unwrap();
    worker.await.unwrap();
    assert_eq!(
        segment_count(&path),
        before,
        "pre-pin ceiling must not authorize unlink"
    );
    drop(pin);
    repo.drainer().drain_all(&repo.core).await.unwrap();
    assert!(segment_count(&path) < before);
}

#[tokio::test]
async fn retention_resamples_pin_after_certification_and_flush() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("late-retention");
    let repo = disk_repo(&path, RetentionOverflow::DeclareGap).await;
    seed(&repo).await;
    let safe = prepare_retention(&repo).await;
    let before = segment_count(&path);
    assert!(before > 2);
    let (entered_tx, entered_rx) = oneshot::channel();
    let (resume_tx, resume_rx) = oneshot::channel();
    test_seam::with_state(&repo.core, |state| {
        state.before_delete = Some((entered_tx, resume_rx))
    });
    let worker_repo = repo.clone();
    let worker = tokio::spawn(async move {
        enforce_retention(&worker_repo.core, safe).await.unwrap();
    });
    entered_rx.await.unwrap();
    let pin = repo.core.pin_wal_history(0).await.unwrap();
    resume_tx.send(()).unwrap();
    worker.await.unwrap();
    assert_eq!(segment_count(&path), before);
    assert_eq!(
        repo.repo_wal()
            .await
            .unwrap()
            .recover()
            .await
            .unwrap()
            .len(),
        12
    );
    drop(pin);
    // Certification already covers `safe`: retention itself must retry the
    // retained deletion without requiring either a new commit or a drainer.
    enforce_retention(&repo.core, safe).await.unwrap();
    assert!(segment_count(&path) < before);
}

#[tokio::test]
async fn retention_honours_positive_floor_inclusively_then_release_resumes() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("retention-min");
    let repo = disk_repo(&path, RetentionOverflow::DeclareGap).await;
    seed(&repo).await;
    let safe = prepare_retention(&repo).await;
    let wal = repo.repo_wal().await.unwrap();
    let mut versions: Vec<_> = wal
        .recover()
        .await
        .unwrap()
        .iter()
        .map(|e| e.commit_version)
        .collect();
    versions.sort_unstable();
    let low_floor = versions[3];
    let high_floor = versions[8];
    let low = repo.core.pin_wal_history(low_floor).await.unwrap();
    let high = repo.core.pin_wal_history(high_floor).await.unwrap();
    enforce_retention(&repo.core, safe).await.unwrap();
    let retained: Vec<_> = wal
        .recover()
        .await
        .unwrap()
        .iter()
        .map(|e| e.commit_version)
        .collect();
    assert!(versions
        .iter()
        .filter(|v| **v >= low_floor)
        .all(|v| retained.contains(v)));
    assert!(
        !retained.contains(&versions[0]),
        "lower history must actually be deleted"
    );
    drop(low);
    enforce_retention(&repo.core, safe).await.unwrap();
    let retained: Vec<_> = wal
        .recover()
        .await
        .unwrap()
        .iter()
        .map(|e| e.commit_version)
        .collect();
    assert!(
        retained.contains(&high_floor),
        "floor itself must survive inclusive delete"
    );
    assert!(!retained.contains(&low_floor));
    drop(high);
    enforce_retention(&repo.core, safe).await.unwrap();
    assert!(!wal
        .recover()
        .await
        .unwrap()
        .iter()
        .any(|e| e.commit_version == high_floor));
}

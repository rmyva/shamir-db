//! Caller-owned truncation pin across capture and seal (brief item 8,
//! test 5b): a pin acquired through the public
//! `RepoInstance::acquire_truncation_pin` keeps its floor through the
//! capture AND seal barriers' settled drains (nothing below the floor is
//! deleted, everything at/above it is retained), and the RAII drop
//! releases it — the next drain deletes through the old floor. Real WAL
//! retention state is asserted (`wal.recover()` versions + on-disk
//! segment count), mirroring the truncation-pin wiring tests.

use std::path::Path;

use shamir_query_types::{ReplicationServe, RetentionOverflow, WalCommitTier};
use shamir_tx::IsolationLevel;
use shamir_types::types::value::InnerValue;

use crate::tx::repo::{BoxRepoFactory, DurabilityPolicy, RepoInstance};
use crate::tx::table::TableConfig;

async fn disk_repo(path: &Path) -> RepoInstance {
    RepoInstance::from_factory_with_test_segment_bytes(
        "online-backup-pin".into(),
        BoxRepoFactory::fjall_raw(path.to_path_buf()),
        vec![TableConfig::new("t")],
        DurabilityPolicy::new(
            WalCommitTier::Buffered,
            512,
            RetentionOverflow::RefuseWrites,
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
    // Settle the interner delta so the A5 truncation gate permits deletion.
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

async fn retained_versions(repo: &RepoInstance) -> Vec<u64> {
    let wal = repo.repo_wal().await.unwrap();
    let mut versions: Vec<u64> = wal
        .recover()
        .await
        .unwrap()
        .iter()
        .map(|e| e.commit_version)
        .collect();
    versions.sort_unstable();
    versions
}

#[tokio::test]
async fn caller_pin_keeps_truncation_floor_through_capture_and_seal() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("pin-capture-seal");
    let repo = disk_repo(&path).await;
    seed(&repo).await;
    let before = segment_count(&path);
    assert!(before > 2, "fixture must seal multiple WAL segments");

    // The caller acquires the pin from step A (capture/seal do NOT pin).
    let mut versions = retained_versions(&repo).await;
    let floor = versions[8];
    let pin = repo
        .acquire_truncation_pin(floor)
        .await
        .expect("public pin acquisition");

    // Step B (capture): drains to settled and truncates — but only BELOW
    // the caller's floor.
    let position = repo.capture_point().await.expect("capture under pin");
    assert_eq!(position, repo.tx_gate().await.unwrap().last_committed());
    versions = retained_versions(&repo).await;
    assert!(
        versions.iter().all(|v| *v >= floor),
        "everything below the caller's floor may be deleted during capture, \
         kept: {versions:?}"
    );
    assert!(
        versions.contains(&floor),
        "the floor itself survives (inclusive retention)"
    );

    // Step E (seal): the pause + settled drain honour the same floor.
    let hold = repo.seal_point().await.expect("seal under pin");
    versions = retained_versions(&repo).await;
    assert!(
        versions.iter().all(|v| *v >= floor),
        "floor retained through the seal barrier, kept: {versions:?}"
    );
    drop(hold);

    // The RAII drop releases the pin: the next settled drain deletes
    // through the old floor.
    drop(pin);
    repo.drainer().drain_all(&repo.core).await.unwrap();
    let after = retained_versions(&repo).await;
    assert!(
        !after.contains(&floor),
        "the released floor no longer retains WAL history, kept: {after:?}"
    );
    assert!(
        segment_count(&path) < before,
        "segments actually unlinked once the pin is gone"
    );
}

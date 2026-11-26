#![cfg(feature = "fjall")]

use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;

use shamir_storage::types::Repo as _;
use shamir_tx::IsolationLevel;
use shamir_types::types::value::InnerValue;

use crate::meta::recovery_marker::load_last_committed;
use crate::repo::{BoxRepoFactory, DurabilityPolicy, RepoFactory, RepoInstance};
use crate::table::TableConfig;
use crate::tx::clean_horizon::checkpoint::test_seam;
use crate::tx::tests::drain::orphans::post_wal_orphan_ordering_tests::{fixture, panic_post_wal};
use crate::tx::tests::recovery::snapshot_gc::snapshot_visibility_park_tests::spawn_settled_commit;

const CHILD: &str = "SHAMIR_ORPHAN_REPAIR_DURABILITY_CHILD";
const PATH: &str = "SHAMIR_ORPHAN_REPAIR_DURABILITY_PATH";
const MODE: &str = "SHAMIR_ORPHAN_REPAIR_DURABILITY_MODE";
const TABLE: &str = "a";
const REPO: &str = "orphan_durability";

async fn open_repo(path: &Path) -> RepoInstance {
    let repo = RepoInstance::from_factory_with_test_segment_bytes(
        REPO.into(),
        BoxRepoFactory::fjall_raw(path.to_path_buf()),
        vec![TableConfig::new(TABLE)],
        DurabilityPolicy::default(),
        256,
    )
    .await
    .expect("open persistent repo");
    repo.disable_background_drainer_for_test();
    repo
}

fn wal_file_count(path: &Path) -> usize {
    let mut name = path.file_name().expect("repo filename").to_os_string();
    name.push(".shamirwal");
    std::fs::read_dir(path.with_file_name(name))
        .expect("WAL directory")
        .filter_map(Result::ok)
        .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "wal"))
        .count()
}

async fn journal_checkpoint(repo: &RepoInstance) -> u64 {
    let changelog = repo
        .changelog_store_for_test()
        .await
        .expect("changelog store");
    match changelog
        .get(shamir_tx::changefeed::clean_horizon_key().into())
        .await
    {
        Ok(bytes) => shamir_tx::changefeed::decode_clean_horizon(&bytes).expect("decode J"),
        Err(shamir_storage::error::DbError::NotFound(_)) => 0,
        Err(error) => panic!("read J: {error}"),
    }
}

async fn raw_checkpoint(path: &Path) -> (u64, u64) {
    let raw = BoxRepoFactory::fjall_raw(path.to_path_buf())
        .create()
        .await
        .expect("open raw repo");
    let changelog = raw
        .store_get("__changelog__")
        .await
        .expect("changelog store");
    let journal = match changelog
        .get(shamir_tx::changefeed::clean_horizon_key().into())
        .await
    {
        Ok(bytes) => shamir_tx::changefeed::decode_clean_horizon(&bytes).expect("decode J"),
        Err(shamir_storage::error::DbError::NotFound(_)) => 0,
        Err(error) => panic!("read J: {error}"),
    };
    let tx = raw.store_get("__tx__").await.expect("tx store");
    let floor = load_last_committed(&tx)
        .await
        .expect("read recovery floor")
        .unwrap_or(0);
    drop(changelog);
    drop(tx);
    drop(raw);
    (journal, floor)
}

async fn crash_child(path: PathBuf, mode: &str) {
    let (repo, hook) = fixture(REPO).await;
    // Replace the in-memory fixture with the persistent raw Fjall factory used
    // by this test, while installing the fixture's post-WAL fault hook first.
    drop(repo);
    let repo = open_repo(&path).await;
    let table = repo.get_table(TABLE).await.expect("table");
    let (mut tx, snapshot) = repo
        .begin_tx(IsolationLevel::Snapshot)
        .await
        .expect("begin orphan");
    table
        .insert_tx(&InnerValue::Str("durable orphan".into()), Some(&mut tx))
        .await
        .expect("stage orphan");
    let orphan = panic_post_wal(&repo, &hook, tx).await;
    drop(snapshot);
    let entry = repo
        .drainer()
        .window_entry(orphan)
        .expect("orphan WAL entry");
    let expected = shamir_tx::event_from_wal_entry(&entry).expect("orphan event");
    let mut event_file =
        std::fs::File::create(path.with_extension("event")).expect("create event oracle");
    event_file
        .write_all(&rmp_serde::to_vec(&expected).expect("encode event"))
        .expect("write event oracle");
    event_file.sync_all().expect("sync event oracle");

    // A later, large committed record seals WAL segments without filling the
    // inline acknowledgement hole left by the orphan.
    let (mut later_tx, later_snapshot) = repo
        .begin_tx(IsolationLevel::Snapshot)
        .await
        .expect("begin later");
    table
        .insert_tx(
            &InnerValue::Str(format!("later{}", "x".repeat(4096))),
            Some(&mut later_tx),
        )
        .await
        .expect("stage later record");
    let (later, task) = spawn_settled_commit(&repo, later_tx).await;
    drop(later_snapshot);
    assert!(later > orphan);
    repo.sync_wal().await.expect("sync seeded WAL");
    repo.repo_interner()
        .await
        .expect("repo interner")
        .persist()
        .await
        .expect("persist interner");
    let baseline = wal_file_count(&path);
    assert!(baseline > 1, "small-segment seed must have sealed WAL");
    let mut sidecar =
        std::fs::File::create(path.with_extension("walbaseline")).expect("create baseline");
    write!(sidecar, "{baseline} {orphan}").expect("write baseline");
    sidecar.sync_all().expect("sync baseline");

    match mode {
        "failure" => {
            test_seam::with_state(&repo.core, |state| state.fail_checkpoint = true);
            repo.drainer()
                .drain_all(&repo.core)
                .await
                .expect("failed checkpoint retains WAL");
            assert_eq!(wal_file_count(&path), baseline);
            let journal = journal_checkpoint(&repo).await;
            assert!(journal < orphan, "failed checkpoint must not advance raw J");
            test_seam::with_state(&repo.core, |state| state.fail_checkpoint = false);
        }
        "bypass" => test_seam::with_state(&repo.core, |state| state.bypass_checkpoint = true),
        "crash" => {}
        _ => panic!("unknown child mode {mode}"),
    }
    if mode == "failure" {
        repo.drainer()
            .drain_all(&repo.core)
            .await
            .expect("retry checkpoint and truncate");
        assert!(wal_file_count(&path) < baseline);
        task.await.expect("later task").expect("later commit");
        return;
    }
    if mode == "bypass" {
        repo.drainer()
            .drain_all(&repo.core)
            .await
            .expect("bypassed checkpoint truncation");
        let journal = journal_checkpoint(&repo).await;
        assert!(
            journal < orphan,
            "negative control intentionally unprotected"
        );
        assert!(wal_file_count(&path) < baseline);
        task.await.expect("later task").expect("later commit");
        let mut marker = std::fs::File::create(path.with_extension("bypass-verified"))
            .expect("create negative-control marker");
        marker
            .write_all(b"unlinked with J below orphan")
            .expect("write negative-control marker");
        marker.sync_all().expect("sync negative-control marker");
        std::process::abort();
    }
    repo.drainer()
        .drain_all(&repo.core)
        .await
        .expect("production drain and checkpoint");
    let journal = journal_checkpoint(&repo).await;
    let tx = repo.tx_info_store().await.expect("tx info store");
    let floor = load_last_committed(&tx)
        .await
        .expect("read recovery floor")
        .unwrap_or(0);
    assert!(journal >= orphan, "raw J covers repaired orphan");
    assert!(floor >= journal, "allocation floor covers J");
    task.await.expect("later task").expect("later commit");
    // The production drainer's existing crash seam aborts only after successful
    // checkpoint and unlink; this fallback makes a missing seam fail loudly.
    panic!("post_truncate crash seam did not fire");
}

#[test]
fn child_entrypoint() {
    if std::env::var_os(CHILD).is_none() {
        return;
    }
    let path = PathBuf::from(std::env::var_os(PATH).expect("child path"));
    let mode = std::env::var(MODE).expect("child mode");
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime")
        .block_on(crash_child(path, &mode));
}

async fn run_child(mode: &str) -> (PathBuf, tempfile::TempDir, std::process::ExitStatus) {
    let temp = tempfile::tempdir().expect("temp dir");
    let path = temp.path().join("persistent_orphan");
    let mut child = tokio::process::Command::new(std::env::current_exe().expect("test executable"))
        .args([
            "--exact",
            "tx::drain::drainer::orphan_repair::tests::durability_tests::child_entrypoint",
            "--nocapture",
        ])
        .env(CHILD, "1")
        .env(PATH, &path)
        .env(MODE, mode)
        .env(
            "SHAMIR_TEST_CRASH_AFTER",
            if mode == "crash" { "post_truncate" } else { "" },
        )
        .kill_on_drop(true)
        .spawn()
        .expect("spawn durability child");
    let status = match tokio::time::timeout(Duration::from_secs(90), child.wait()).await {
        Ok(status) => status.expect("wait child"),
        Err(_) => {
            child.kill().await.expect("kill child");
            let _ = child.wait().await;
            panic!("durability child timed out");
        }
    };
    (path, temp, status)
}

#[tokio::test]
async fn checkpoint_failure_retains_sealed_orphan_wal_then_retry_unlinks() {
    let (_path, _temp, status) = run_child("failure").await;
    assert!(status.success(), "failure/retry child completes normally");
}

#[tokio::test]
async fn bypass_negative_control_unlinks_without_orphan_checkpoint() {
    let (path, _temp, status) = run_child("bypass").await;
    assert!(
        !status.success(),
        "negative-control child must abort before graceful shutdown"
    );
    assert!(
        path.with_extension("bypass-verified").exists(),
        "negative-control oracle persisted"
    );
    let baseline = std::fs::read_to_string(path.with_extension("walbaseline")).expect("baseline");
    let (_, orphan) = baseline.split_once(' ').expect("orphan version");
    let orphan: u64 = orphan.parse().expect("orphan version number");
    let (journal, _) = raw_checkpoint(&path).await;
    assert!(journal < orphan, "removed orphan WAL was not covered by J");
}

#[tokio::test]
async fn post_truncate_crash_reopens_and_replays_orphan_event_twice() {
    let (path, _temp, status) = run_child("crash").await;
    assert!(!status.success(), "post_truncate seam must abort child");
    let baseline = std::fs::read_to_string(path.with_extension("walbaseline")).expect("baseline");
    let (count, orphan) = baseline.split_once(' ').expect("baseline values");
    let count: usize = count.parse().expect("segment count");
    let orphan: u64 = orphan.parse().expect("orphan version");
    assert!(
        wal_file_count(&path) < count,
        "sealed WAL segment was unlinked"
    );
    let (journal, floor) = raw_checkpoint(&path).await;
    assert!(journal >= orphan, "raw J covers repaired orphan");
    assert!(floor >= journal, "allocation floor covers J");
    let expected: shamir_tx::ChangelogEvent =
        rmp_serde::from_slice(&std::fs::read(path.with_extension("event")).expect("event sidecar"))
            .expect("decode event");
    for _ in 0..2 {
        let repo = open_repo(&path).await;
        let (events, gap) = repo
            .read_changelog_for_replication(orphan, 1)
            .await
            .expect("read recovered event");
        assert_eq!(gap, None);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event, expected);
        repo.stop_journal_writer_for_test()
            .await
            .expect("stop reopened writer");
        drop(repo);
    }
}

#[tokio::test]
async fn raw_checkpoint_reader_reports_missing_j_and_floor() {
    let temp = tempfile::tempdir().expect("tempdir");
    let (journal, floor) = raw_checkpoint(&temp.path().join("raw_contract")).await;
    assert_eq!((journal, floor), (0, 0));
}

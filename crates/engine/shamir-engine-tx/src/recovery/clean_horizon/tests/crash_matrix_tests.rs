use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;

use shamir_storage::types::Repo as _;

use crate::meta::recovery_marker::load_last_committed;
use crate::repo::{BoxRepoFactory, DurabilityPolicy, RepoFactory, RepoInstance};
use crate::table::TableConfig;
use crate::tx::clean_horizon::checkpoint::test_seam;
use crate::tx::drainer::Drainer;
use crate::tx::IsolationLevel;
use futures::{pin_mut, StreamExt};
use shamir_tx::changefeed::ChangelogEvent;
use shamir_types::types::value::InnerValue;

const CHILD: &str = "SHAMIR_CLEAN_HORIZON_CRASH_CHILD";
const PHASE: &str = "SHAMIR_TEST_CRASH_AFTER";
const CHILD_PHASE: &str = "SHAMIR_CLEAN_HORIZON_CRASH_PHASE";
const PATH: &str = "SHAMIR_CLEAN_HORIZON_CRASH_PATH";
const TABLE: &str = "crash_rows";
const REPO_NAME: &str = "clean_horizon_crash";
const PHASES: [&str; 5] = [
    "pre_checkpoint",
    "post_checkpoint",
    "pre_truncate",
    "post_truncate",
    "mid_checkpoint",
];
const RECORDS: usize = 80;

async fn count_rows(table: &crate::table::TableManager) -> usize {
    let stream = table.list_stream(256);
    pin_mut!(stream);
    let mut count = 0;
    while let Some(batch) = stream.next().await {
        count += batch.expect("list table rows").len();
    }
    count
}

async fn open_repo(path: &Path) -> RepoInstance {
    let repo = RepoInstance::from_factory_with_test_segment_bytes(
        REPO_NAME.into(),
        BoxRepoFactory::fjall_raw(path.to_path_buf()),
        vec![TableConfig::new(TABLE)],
        DurabilityPolicy::default(),
        256,
    )
    .await
    .expect("open crash matrix repo");
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

async fn raw_checkpoint(path: &Path) -> (u64, u64) {
    let factory = BoxRepoFactory::fjall_raw(path.to_path_buf());
    let raw = factory.create().await.expect("open raw repo");
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

async fn crash_child(path: PathBuf, phase: &str) {
    let repo = open_repo(&path).await;
    let table = repo.get_table(TABLE).await.expect("table");
    for index in 0..RECORDS {
        let (mut tx, guard) = repo
            .begin_tx(IsolationLevel::Snapshot)
            .await
            .expect("begin seed transaction");
        table
            .insert_tx(
                &InnerValue::Str(format!("seed-{index}-{}", "x".repeat(512))),
                Some(&mut tx),
            )
            .await
            .expect("stage seed row");
        repo.commit_tx(tx).await.expect("commit seed row");
        drop(guard);
        repo.sync_wal().await.expect("sync seed WAL");
    }
    repo.repo_interner()
        .await
        .expect("repo interner")
        .persist()
        .await
        .expect("persist interner");
    repo.flush_all_history()
        .await
        .expect("persist baseline history");
    let journal_store = repo
        .changelog_store_for_cold_reconcile()
        .await
        .expect("journal store");
    journal_store.flush().await.expect("flush baseline journal");
    drop(journal_store);
    let baseline = wal_file_count(&path);
    assert!(baseline > 1, "seed WAL baseline must include sealed WAL");
    let mut file =
        std::fs::File::create(path.with_extension("walbaseline")).expect("create baseline sidecar");
    write!(file, "{baseline}").expect("write baseline sidecar");
    file.sync_all().expect("sync baseline sidecar");
    let journal = repo
        .read_changelog_from(1, usize::MAX)
        .await
        .expect("baseline follower read");
    assert!(journal.gap_at.is_none());
    let baseline_entries = repo
        .repo_wal()
        .await
        .expect("repo WAL")
        .recover()
        .await
        .expect("recover WAL");
    let journal_events: Vec<_> = baseline_entries
        .iter()
        .map(|entry| shamir_tx::event_from_wal_entry(entry).expect("derive baseline event"))
        .collect();
    assert_eq!(
        journal.events, journal_events,
        "persisted event oracle matches WAL derivation"
    );
    let mut oracle = std::fs::File::create(path.with_extension("events"))
        .expect("create expected events oracle");
    rmp_serde::encode::write(&mut oracle, &journal.events).expect("encode events oracle");
    oracle.sync_all().expect("sync events oracle");
    if phase == "mid_checkpoint" {
        test_seam::on_sample_w(&repo.core, || {
            let marker =
                std::env::var_os("SHAMIR_CLEAN_HORIZON_REACHED").expect("reached marker path");
            let mut file = std::fs::File::create(marker).expect("create checkpoint marker");
            file.write_all(b"mid_checkpoint")
                .expect("write checkpoint marker");
            file.sync_all().expect("sync checkpoint marker");
            std::process::abort();
        });
    }
    Drainer::new()
        .drain_all(&repo.core)
        .await
        .expect("drain must reach crash seam");
    panic!("crash seam did not fire at {phase}");
}

#[test]
fn child_entrypoint() {
    if std::env::var_os(CHILD).is_none() {
        return;
    }
    let path = PathBuf::from(std::env::var_os(PATH).expect("child repo path"));
    let phase = std::env::var(CHILD_PHASE).expect("child crash phase");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("child runtime");
    runtime.block_on(crash_child(path, &phase));
}

async fn run_phase(phase: &'static str, expect_checkpoint: bool) {
    let temp = tempfile::tempdir().expect("temp dir");
    let repo_path = temp.path().join("crash_repo");
    let exe = std::env::current_exe().expect("unit test executable");
    let mut child = tokio::process::Command::new(exe)
        .args([
            "--exact",
            "tx::tests::clean_horizon_tests::crash_matrix_tests::child_entrypoint",
            "--nocapture",
        ])
        .env(CHILD, "1")
        .env(PATH, &repo_path)
        .env(PHASE, phase)
        .env(CHILD_PHASE, phase)
        .env("SHAMIR_TEST_CRASH_AFTER", phase)
        .env(
            "SHAMIR_CLEAN_HORIZON_REACHED",
            repo_path.with_extension("reached"),
        )
        .kill_on_drop(true)
        .spawn()
        .expect("spawn crash child");
    let status = match tokio::time::timeout(Duration::from_secs(90), child.wait()).await {
        Ok(status) => status.expect("wait for crash child"),
        Err(_) => {
            child.kill().await.expect("kill timed-out child");
            let _ = child.wait().await.expect("reap timed-out child");
            panic!("crash child timed out at {phase}");
        }
    };
    assert!(!status.success(), "child must abort at {phase}");
    if phase == "mid_checkpoint" {
        assert_eq!(
            std::fs::read_to_string(repo_path.with_extension("reached")).unwrap(),
            phase,
            "checkpoint callback reached"
        );
    }
    let baseline: usize = std::fs::read_to_string(repo_path.with_extension("walbaseline"))
        .expect("baseline sidecar")
        .trim()
        .parse()
        .expect("valid baseline");
    assert!(baseline > 1);
    let wal_after = wal_file_count(&repo_path);
    if phase == "post_truncate" {
        assert!(
            wal_after < baseline,
            "post-truncate must unlink WAL segments"
        );
    } else {
        assert_eq!(wal_after, baseline, "phase must preserve WAL files");
    }
    let (journal, floor) = raw_checkpoint(&repo_path).await;
    assert_eq!(journal > 0, expect_checkpoint, "J phase oracle");
    assert!(floor >= journal, "allocation floor covers J");
    for _ in 0..2 {
        let repo = open_repo(&repo_path).await;
        let table = repo.get_table(TABLE).await.expect("recovered table");
        let records = count_rows(&table).await;
        assert_eq!(records, RECORDS);
        let page = repo
            .read_changelog_from(1, usize::MAX)
            .await
            .expect("follower journal");
        let expected: Vec<ChangelogEvent> = rmp_serde::from_slice(
            &std::fs::read(repo_path.with_extension("events")).expect("expected event oracle"),
        )
        .expect("decode event oracle");
        assert_eq!(page.gap_at, None, "follower gap on reopen {phase}");
        assert_eq!(page.events, expected, "follower events on reopen {phase}");
        drop(table);
        repo.stop_journal_writer_for_test()
            .await
            .expect("stop reopened writer");
        drop(repo);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn crash_pre_checkpoint_preserves_image() {
    run_phase(PHASES[0], false).await;
}

#[tokio::test(flavor = "current_thread")]
async fn crash_post_checkpoint_preserves_image() {
    run_phase(PHASES[1], true).await;
}

#[tokio::test(flavor = "current_thread")]
async fn crash_pre_truncate_preserves_image() {
    run_phase(PHASES[2], true).await;
}

#[tokio::test(flavor = "current_thread")]
async fn crash_post_truncate_recovers_baseline() {
    run_phase(PHASES[3], true).await;
}

#[tokio::test(flavor = "current_thread")]
async fn crash_mid_checkpoint_reopens_twice_without_follower_gap() {
    run_phase(PHASES[4], false).await;
}

#[tokio::test]
async fn raw_checkpoint_reader_reports_missing_j_and_floor() {
    let temp = tempfile::tempdir().expect("tempdir");
    let path = temp.path().join("raw_contract");
    let (journal, floor) = raw_checkpoint(&path).await;
    assert_eq!(journal, 0);
    assert_eq!(floor, 0);
}

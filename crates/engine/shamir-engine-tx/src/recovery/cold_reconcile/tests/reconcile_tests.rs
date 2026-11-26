use super::helpers::{open_and_reconcile, open_unrecovered, prepare_image, temp_path};
use crate::clean_horizon::checkpoint::test_seam;
use crate::repo::{BoxRepoFactory, RepoFactory, RepoInstance};
use crate::table::TableConfig;

use crate::cold_reconcile::algorithm::reconcile_with_hook;
use crate::drainer::Drainer;
use bytes::Bytes;
use shamir_query_builder::write::doc;
use shamir_query_types::{ReplicationServe, RetentionOverflow, WalCommitTier};
use shamir_tx::changefeed::{ChangeOp, ChangelogEvent, RecordChange};
use shamir_types::access::Actor;
use shamir_types::codecs::interned::query_value_to_inner;
use shamir_wal::{WalEntryV2, WalEventHeader};
use std::io::Write as _;

fn event(version: u64) -> ChangelogEvent {
    ChangelogEvent {
        repo: super::helpers::REPO_NAME.into(),
        commit_version: version,
        tx_id: 17,
        actor: Actor::System,
        timestamp_ns: 23,
        changes: vec![RecordChange {
            table: "items".into(),
            key: Bytes::from_static(b"record-key-00001"),
            op: ChangeOp::Put,
            value: Some(Bytes::from_static(b"payload")),
        }],
    }
}

fn derived(version: u64) -> WalEntryV2 {
    WalEntryV2::new(version, 0, Vec::new())
        .with_commit_version(version)
        .with_event_header(WalEventHeader {
            repo: super::helpers::REPO_NAME.into(),
            actor: Actor::System,
            tx_id: 17,
            timestamp_ns: 23,
            table_names: vec![(1, "items".into())],
            extra_changes: vec![shamir_wal::WalExtraChange {
                table_id_interned: 1,
                change_index: 0,
                key: Bytes::from_static(b"record-key-00001"),
                value: Some(Bytes::from_static(b"payload")),
            }],
        })
}

fn eventless(version: u64) -> WalEntryV2 {
    WalEntryV2::new(version, 0, Vec::new())
        .with_commit_version(version)
        .with_event_header(WalEventHeader {
            repo: super::helpers::REPO_NAME.into(),
            actor: Actor::System,
            tx_id: 0,
            timestamp_ns: 23,
            table_names: Vec::new(),
            extra_changes: Vec::new(),
        })
}

async fn stop(repo: RepoInstance) {
    repo.stop_journal_writer_for_test()
        .await
        .expect("stop journal writer");
    drop(repo);
}

fn disk_policy(action: RetentionOverflow, segment_bytes: u64) -> crate::repo::DurabilityPolicy {
    crate::repo::DurabilityPolicy::new(
        WalCommitTier::Buffered,
        segment_bytes.saturating_mul(1024).max(1),
        action,
        ReplicationServe::Durable,
    )
}

async fn seeded_retention_repo(path: &std::path::Path, action: RetentionOverflow) -> RepoInstance {
    let segment_bytes = 256;
    let repo = RepoInstance::from_factory_with_test_segment_bytes(
        super::helpers::REPO_NAME.into(),
        crate::repo::BoxRepoFactory::fjall_raw(path.to_path_buf()),
        vec![TableConfig::new("items")],
        disk_policy(action, segment_bytes),
        segment_bytes,
    )
    .await
    .expect("open retention fixture repo");
    repo.disable_background_drainer_for_test();
    for i in 0..8 {
        let (mut tx, _guard) = repo
            .begin_tx(crate::IsolationLevel::Snapshot)
            .await
            .unwrap();
        let user_record = doc()
            .set("payload", format!("row-{i}-{}", "x".repeat(512)))
            .unwrap()
            .build();
        let table = repo.get_table("items").await.unwrap();
        let interner = table.interner().get().await.unwrap();
        let record = query_value_to_inner(&user_record, interner).unwrap();
        table.insert_tx(&record, Some(&mut tx)).await.unwrap();
        repo.commit_tx(tx).await.unwrap();
    }
    repo
}

#[tokio::test(flavor = "current_thread")]
async fn surviving_v3_wal_repairs_missing_event_and_follower_cursor_reads_it_twice() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp_path(&temp);
    prepare_image(&path, &[derived(1)], &[], None, 0).await;

    for _ in 0..2 {
        let repo = open_and_reconcile(&path).await.unwrap();
        let read = repo.read_changelog_from(1, 8).await.unwrap();
        assert!(read.gap_at.is_none());
        assert_eq!(read.events, vec![event(1)]);
        stop(repo).await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn legacy_wal_is_point_gap_and_eventless_wal_is_gap_free() {
    let temp = tempfile::tempdir().unwrap();
    let legacy_path = temp.path().join("legacy");
    let legacy = WalEntryV2::new(1, 0, Vec::new()).with_commit_version(1);
    prepare_image(&legacy_path, &[legacy], &[], None, 0).await;
    let repo = open_and_reconcile(&legacy_path).await.unwrap();
    let feed = repo.changefeed().await.unwrap().feed;
    assert_eq!(feed.certified_gap_interval(), Some((1, 1)));
    stop(repo).await;

    let eventless_path = temp.path().join("eventless");
    prepare_image(&eventless_path, &[eventless(1)], &[], None, 0).await;
    let repo = open_and_reconcile(&eventless_path).await.unwrap();
    let feed = repo.changefeed().await.unwrap().feed;
    assert_eq!(feed.first_gap_version(), 0);
    assert_eq!(feed.certified_gap_interval(), None);
    stop(repo).await;
}

#[tokio::test(flavor = "current_thread")]
async fn conflicting_journal_event_refuses_open() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp_path(&temp);
    let conflict = ChangelogEvent {
        tx_id: 999,
        ..event(1)
    };
    prepare_image(&path, &[derived(1)], &[conflict], None, 0).await;
    assert!(open_and_reconcile(&path).await.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn lost_unserved_event_is_deleted_but_provenance_keeps_event_and_gaps() {
    let temp = tempfile::tempdir().unwrap();
    let lost_path = temp.path().join("lost");
    prepare_image(&lost_path, &[], &[event(1)], None, 0).await;
    let repo = open_and_reconcile(&lost_path).await.unwrap();
    let store = repo.changelog_store_for_cold_reconcile().await.unwrap();
    let lost_read = repo.read_changelog_from(1, 8).await.unwrap();
    assert_eq!(lost_read.gap_at, Some(1));
    assert!(super::helpers::journal_event(store.as_ref(), 1)
        .await
        .is_none());
    drop(store);
    stop(repo).await;

    let served_path = temp.path().join("served");
    prepare_image(&served_path, &[], &[event(1)], Some(1), 0).await;
    let repo = open_and_reconcile(&served_path).await.unwrap();
    let feed = repo.changefeed().await.unwrap().feed;
    let store = repo.changelog_store_for_cold_reconcile().await.unwrap();
    assert!(super::helpers::journal_event(store.as_ref(), 1)
        .await
        .is_some());
    assert_eq!(feed.certified_gap_interval(), Some((1, 1)));
    drop(store);
    stop(repo).await;
}

#[tokio::test(flavor = "current_thread")]
async fn corrupt_journal_event_refuses_open() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp_path(&temp);
    prepare_image(&path, &[derived(1)], &[], None, 0).await;
    let store = super::helpers::raw_store(&path, "__changelog__").await;
    store
        .set(
            shamir_tx::changefeed::version_key(1).into(),
            Bytes::from_static(b"not-msgpack"),
        )
        .await
        .unwrap();
    store.flush().await.unwrap();
    drop(store);
    assert!(open_and_reconcile(&path).await.is_err());
}

#[test]
fn reconcile_crash_child_entrypoint() {
    const CHILD: &str = "SHAMIR_COLD_RECONCILE_CRASH_CHILD";
    if std::env::var_os(CHILD).is_none() {
        return;
    }
    let path = std::path::PathBuf::from(
        std::env::var_os("SHAMIR_COLD_RECONCILE_CRASH_PATH").expect("child path"),
    );
    let phase = std::env::var("SHAMIR_COLD_RECONCILE_CRASH_PHASE").expect("child phase");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("child runtime");
    runtime.block_on(async move {
        if phase == "pre_publish" {
            let repo = RepoInstance::from_factory_with_durability(
                super::helpers::REPO_NAME.into(),
                BoxRepoFactory::fjall_raw(path.clone()),
                vec![TableConfig::new("items")],
                crate::repo::DurabilityPolicy::new(
                    shamir_query_types::WalCommitTier::Synced,
                    u64::MAX,
                    shamir_query_types::RetentionOverflow::RefuseWrites,
                    shamir_query_types::ReplicationServe::Durable,
                ),
            )
            .await
            .expect("open live-commit child repo");
            repo.disable_background_drainer_for_test();
            let table = repo.get_table("items").await.unwrap();
            let user_record = doc().set("payload", "crash-row").unwrap().build();
            let interner = table.interner().get().await.unwrap();
            let record = query_value_to_inner(&user_record, interner).unwrap();
            let (mut tx, _guard) = repo
                .begin_tx(crate::IsolationLevel::Snapshot)
                .await
                .unwrap();
            table.insert_tx(&record, Some(&mut tx)).await.unwrap();
            let _ = repo.commit_tx(tx).await;
            panic!("pre_publish hook did not fire");
        }
        let repo = open_unrecovered(&path).await;
        let wal = repo.repo_wal().await.unwrap();
        let entries = wal.recover().await.unwrap();
        let permit = repo.write_domain().admit().await.unwrap();
        let _ = reconcile_with_hook(&repo.core, &entries, &permit, &mut |at| {
            if at == phase {
                if let Ok(marker) = std::env::var("SHAMIR_COLD_RECONCILE_PHASE_MARKER") {
                    let mut file = std::fs::File::create(marker).expect("create reached marker");
                    write!(file, "{at}").expect("write reached marker");
                    file.sync_all().expect("sync reached marker");
                }
                std::process::abort();
            }
            Ok(())
        })
        .await;
    });
    panic!("crash hook did not fire");
}

#[tokio::test(flavor = "current_thread")]
async fn subprocess_crashes_during_reconcile_reopen_twice() {
    const CHILD: &str = "SHAMIR_COLD_RECONCILE_CRASH_CHILD";
    const PHASES: [&str; 10] = [
        "pre_put",
        "after_put",
        "pre_delete",
        "after_delete",
        "pre_flush",
        "after_flush",
        "before_j",
        "after_j",
        "mid_reconcile",
        "pre_publish",
    ];
    for phase in PHASES {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("child_repo");
        let (entries, journal): (Vec<WalEntryV2>, Vec<ChangelogEvent>) = if phase == "pre_publish" {
            (vec![], vec![])
        } else if phase.contains("delete") {
            (vec![], vec![event(1)])
        } else {
            (vec![derived(1)], vec![])
        };
        prepare_image(&path, &entries, &journal, None, 0).await;
        let oracle = path.with_extension("expected");
        let mut oracle_file = std::fs::File::create(&oracle).unwrap();
        if phase != "pre_publish" {
            write!(
                oracle_file,
                "{}",
                if phase.contains("delete") {
                    "lost"
                } else {
                    "present"
                }
            )
            .unwrap();
        }
        oracle_file.sync_all().unwrap();
        drop(oracle_file);
        let executable = std::env::current_exe().unwrap();
        let mut child = tokio::process::Command::new(executable)
            .args([
                "--exact",
                "tx::cold_reconcile_tests::reconcile_tests::reconcile_crash_child_entrypoint",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .env("SHAMIR_COLD_RECONCILE_CRASH_PATH", &path)
            .env("SHAMIR_COLD_RECONCILE_CRASH_PHASE", phase)
            .env(
                "SHAMIR_COLD_RECONCILE_PHASE_MARKER",
                path.with_extension("reached"),
            )
            .env("SHAMIR_TEST_CRASH_AFTER", phase)
            .env("SHAMIR_COLD_RECONCILE_EXPECTED_EVENTS", &oracle)
            .kill_on_drop(true)
            .spawn()
            .expect("spawn crash child");
        let status =
            match tokio::time::timeout(std::time::Duration::from_secs(90), child.wait()).await {
                Ok(status) => status.expect("wait for crash child"),
                Err(_) => {
                    child.kill().await.expect("kill timed-out child");
                    let _ = child.wait().await.expect("reap timed-out child");
                    panic!("crash child timed out at {phase}");
                }
            };
        assert!(!status.success(), "process must abort at {phase}");
        assert_eq!(
            std::fs::read_to_string(path.with_extension("reached")).unwrap(),
            phase,
            "crash hook reached {phase}"
        );
        let expected: ChangelogEvent = if phase == "pre_publish" {
            rmp_serde::from_slice(
                &std::fs::read(&oracle).expect("expected event oracle written before abort"),
            )
            .expect("decode expected event")
        } else {
            event(1)
        };
        for _ in 0..2 {
            let repo = if phase == "pre_publish" {
                let repo = open_unrecovered(&path).await;
                repo.recover_v2_inflight().await.unwrap();
                repo
            } else {
                open_and_reconcile(&path).await.unwrap()
            };
            let read = repo
                .read_changelog_from(expected.commit_version, 8)
                .await
                .unwrap();
            if phase.contains("delete") {
                assert_eq!(read.gap_at, Some(expected.commit_version));
                assert!(read.events.is_empty());
            } else {
                assert_eq!(read.gap_at, None);
                assert_eq!(read.events, vec![expected.clone()]);
            }
            stop(repo).await;
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn stopped_image_crash_boundaries_reopen_twice_without_follower_gaps() {
    // Persisted fixture images exercise state shapes; process interruption
    // coverage is provided by subprocess tests.
    let cases = [
        ("pre_publish", vec![derived(1)], vec![], 0),
        ("pre_truncate", vec![derived(1)], vec![event(1)], 0),
        ("post_truncate", vec![], vec![event(1)], 1),
        ("mid_checkpoint", vec![derived(1)], vec![event(1)], 0),
    ];
    for (boundary, entries, journal, clean_horizon) in cases {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join(boundary);
        prepare_image(&path, &entries, &journal, None, clean_horizon).await;
        for _ in 0..2 {
            let repo = open_and_reconcile(&path).await.unwrap();
            let page = repo.read_changelog_from(1, 8).await.unwrap();
            assert_eq!(page.gap_at, None, "follower gap at {boundary}");
            assert_eq!(page.events, vec![event(1)], "follower event at {boundary}");
            stop(repo).await;
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn retention_checkpoint_protects_wal_and_bypass_control_exposes_loss() {
    let temp = tempfile::tempdir().unwrap();

    let protected_path = temp.path().join("protected");
    let protected = seeded_retention_repo(&protected_path, RetentionOverflow::RefuseWrites).await;
    let protected_feed = protected.changefeed().await.unwrap().feed;
    let protected_wal = protected.repo_wal().await.unwrap();
    assert!(protected_wal.has_truncatable(u64::MAX));
    let protected_count = protected_wal.recover().await.unwrap().len();
    assert!(protected_count > 0);
    let protected_gate = protected.tx_gate().await.unwrap();
    protected_gate.mark_durable(protected_gate.last_committed());
    drop(protected_gate);
    for _ in 0..8 {
        let store = protected
            .changelog_store_for_cold_reconcile()
            .await
            .unwrap();
        assert!(store.flush().await.is_ok());
        drop(store);
        Drainer::new().drain_all(&protected.core).await.unwrap();
    }
    let protected_reads = protected.read_changelog_from(1, usize::MAX).await.unwrap();
    assert!(protected_reads.gap_at.is_none());
    let protected_events = protected_reads.events;
    assert!(!protected_events.is_empty());
    assert!(protected_wal.recover().await.unwrap().len() < protected_count);
    drop(protected_feed);
    drop(protected_wal);
    stop(protected).await;
    for _ in 0..2 {
        let reopened = open_and_reconcile(&protected_path).await.unwrap();
        let read = reopened.read_changelog_from(1, usize::MAX).await.unwrap();
        assert_eq!(read.gap_at, None);
        assert_eq!(read.events, protected_events);
        stop(reopened).await;
    }

    let bypass_path = temp.path().join("bypass");
    let bypass = seeded_retention_repo(&bypass_path, RetentionOverflow::RefuseWrites).await;
    let bypass_feed = bypass.changefeed().await.unwrap().feed;
    let bypass_wal = bypass.repo_wal().await.unwrap();
    assert!(bypass_wal.has_truncatable(u64::MAX));
    let before = bypass_wal.recover().await.unwrap().len();
    let gate = bypass.tx_gate().await.unwrap();
    gate.mark_durable(gate.last_committed());
    drop(gate);
    test_seam::with_state(&bypass.core, |state| state.bypass_checkpoint = true);
    for _ in 0..8 {
        Drainer::new().drain_all(&bypass.core).await.unwrap();
    }
    assert_eq!(bypass_feed.clean_horizon(), 0);
    let bypass_reads = bypass.read_changelog_from(1, usize::MAX).await.unwrap();
    assert!(bypass_reads.gap_at.is_none());
    let bypass_events = bypass_reads.events;
    assert_eq!(bypass_events.len(), 8);
    let surviving_wal = bypass_wal.recover().await.unwrap();
    let absent_wal_events: Vec<_> = bypass_events
        .iter()
        .filter(|event| {
            !surviving_wal
                .iter()
                .any(|entry| entry.commit_version == event.commit_version)
        })
        .cloned()
        .collect();
    assert!(!absent_wal_events.is_empty());
    let lost_event = absent_wal_events
        .first()
        .expect("at least one journal event lost its WAL evidence")
        .clone();
    let expected_after_loss: Vec<_> = bypass_events
        .iter()
        .filter(|event| {
            surviving_wal
                .iter()
                .any(|entry| entry.commit_version == event.commit_version)
        })
        .cloned()
        .collect();
    assert!(surviving_wal.len() < before);
    drop(bypass_feed);
    drop(bypass_wal);
    stop(bypass).await;

    // Model loss of an unflushed journal tail after its WAL segment was
    // truncated. This deliberately edits the stopped disk image, not the
    // normal journal writer path or the checkpoint-protected variant.
    let store = super::helpers::raw_store(&bypass_path, "__changelog__").await;
    shamir_tx::changefeed::cold::delete_event(store.as_ref(), lost_event.commit_version)
        .await
        .unwrap();
    store.flush().await.unwrap();
    drop(store);

    for _ in 0..2 {
        let reopened = open_and_reconcile(&bypass_path).await.unwrap();
        let feed = reopened.changefeed().await.unwrap().feed;
        assert!(feed.first_gap_version() > 0);
        let read = reopened.read_changelog_from(1, usize::MAX).await.unwrap();
        assert_eq!(read.gap_at, Some(lost_event.commit_version));
        assert_eq!(read.events, expected_after_loss);
        stop(reopened).await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn ephemeral_repositories_keep_legacy_cold_start_behavior() {
    let raw = crate::repo::BoxRepoFactory::in_memory()
        .create()
        .await
        .unwrap();
    let repo = RepoInstance::new(
        "ephemeral_cold_reconcile".into(),
        raw,
        vec![TableConfig::new("items")],
    );
    let entry = derived(1);
    let manager = repo.repo_wal().await.unwrap();
    manager
        .begin_grouped(&entry, shamir_wal::WalDurability::Buffered)
        .await
        .unwrap();
    let wal_group = repo.wal_group().await.unwrap();
    assert!(wal_group.is_mem_sink());
    let permit = repo.write_domain().admit().await.unwrap();
    repo.certify_cold_start(&[entry], &permit).await.unwrap();
    let feed = repo.changefeed().await.unwrap().feed;
    assert_eq!(feed.clean_horizon(), 1);
    assert_eq!(feed.first_gap_version(), 1);
    assert_eq!(feed.certified_gap_interval(), Some((1, 1)));
    drop(permit);
    stop(repo).await;
}

#[tokio::test(flavor = "current_thread")]
async fn hybrid_factory_keeps_legacy_cold_start_behavior() {
    let temp = tempfile::tempdir().unwrap();
    let raw = crate::repo::BoxRepoFactory::hybrid(temp.path())
        .create()
        .await
        .unwrap();
    let repo = RepoInstance::new(
        "hybrid_cold_reconcile".into(),
        raw,
        vec![TableConfig::new("items")],
    );
    let entry = derived(1);
    let manager = repo.repo_wal().await.unwrap();
    manager
        .begin_grouped(&entry, shamir_wal::WalDurability::Buffered)
        .await
        .unwrap();
    let wal_group = repo.wal_group().await.unwrap();
    assert!(wal_group.is_mem_sink());
    let permit = repo.write_domain().admit().await.unwrap();
    repo.certify_cold_start(&[entry], &permit).await.unwrap();
    let feed = repo.changefeed().await.unwrap().feed;
    assert_eq!(feed.clean_horizon(), 1);
    assert_eq!(feed.first_gap_version(), 1);
    assert_eq!(feed.certified_gap_interval(), Some((1, 1)));
    drop(permit);
    stop(repo).await;
}

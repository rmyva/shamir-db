use std::future::{poll_fn, Future};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::Poll;

use bytes::Bytes;
use shamir_tx::{IndexWriteOp, IsolationLevel};
use shamir_types::types::value::InnerValue;
use tokio::sync::{oneshot, Notify};

use crate::tx::commit::{PostWalPrePublishHook, TEST_POST_WAL_PRE_PUBLISH_HOOK};
use crate::tx::repo::{BoxRepoFactory, RepoInstance};
use crate::tx::table::table_manager::table_token_for;
use crate::tx::table::TableConfig;
use crate::tx::tests::recovery::snapshot_gc::snapshot_visibility_park_tests::spawn_settled_commit;

/// The embedded store releases its directory lock when its background workers
/// notice the drop, so a reopen right after `drop` may briefly see `Locked`.
async fn reopen_after_drop(path: &Path) -> RepoInstance {
    let mut last_err = None;
    for _ in 0..100 {
        match RepoInstance::from_factory(
            "post_wal_cancel".into(),
            BoxRepoFactory::fjall_raw(path.to_path_buf()),
            vec![TableConfig::new("t")],
        )
        .await
        {
            Ok(repo) => return repo,
            Err(e) => {
                last_err = Some(e);
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        }
    }
    panic!("post_wal_cancel reopen still failing after 5 s: {last_err:?}");
}

/// Names the stuck phase instead of leaving the runner to kill the test after minutes.
async fn within<T>(phase: &str, fut: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(std::time::Duration::from_secs(45), fut)
        .await
        .unwrap_or_else(|_| panic!("post_wal_cancel test stuck at {phase}"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn post_wal_cancel_survives_drain_truncation_and_reopen_with_pre_wal_control() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("repo");
    let hook = Arc::new(PostWalPrePublishHook {
        repo_name: "post_wal_cancel".into(),
        pre_wal: AtomicBool::new(false),
        after_counter: AtomicBool::new(false),
        fail_orphan_markers: AtomicBool::new(false),
        panic_after_resume: AtomicBool::new(false),
        panic_resume: Notify::new(),
        reached: Notify::new(),
        resume: Notify::new(),
        armed: AtomicBool::new(false),
    });
    TEST_POST_WAL_PRE_PUBLISH_HOOK
        .set(Arc::clone(&hook))
        .ok()
        .unwrap();
    let repo = RepoInstance::from_factory(
        "post_wal_cancel".into(),
        BoxRepoFactory::fjall_raw(path.clone()),
        vec![TableConfig::new("t")],
    )
    .await
    .unwrap();
    let tbl = repo.get_table("t").await.unwrap();
    let gate = repo.tx_gate().await.unwrap();
    let (mut tx, snapshot) = repo.begin_tx(IsolationLevel::Serializable).await.unwrap();
    let a = tbl
        .insert_tx(&InnerValue::Str("A".into()), Some(&mut tx))
        .await
        .unwrap();
    within("commit_tx@48", repo.commit_tx(tx)).await.unwrap();
    drop(snapshot);

    // Orphans below are injected panics, not caller cancellation: the driver
    // detaches on cancellation, while panic must preserve B1/B2/B3 recovery.
    // B1: a present but ACTIVE WAL entry is not a missing-key gap. A later
    // orphan raises the scan bound: the completed "later" commit alone cannot
    // advance contiguous visibility past the still-active version.
    within("drain_all@54", repo.drainer().drain_all(&repo.core))
        .await
        .unwrap();
    // Snapshot isolation: a Serializable committer parked after WAL accept still
    // holds the repo commit_lock, so a concurrent Serializable commit would wait on it.
    hook.armed.store(true, Ordering::Release);
    let (mut tx, snapshot) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let active_rid = tbl
        .insert_tx(&InnerValue::Str("active".into()), Some(&mut tx))
        .await
        .unwrap();
    let owned = repo.clone();
    let active_task = tokio::spawn(async move { owned.commit_tx(tx).await });
    within("reached@63", hook.reached.notified()).await;
    let active_version = *repo.drainer().window_keys().last().unwrap();
    assert!(gate.is_active_post_wal(active_version));
    let (mut tx, later_snapshot) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let later_rid = tbl
        .insert_tx(&InnerValue::Str("later".into()), Some(&mut tx))
        .await
        .unwrap();
    let (later_version, later_task) = spawn_settled_commit(&repo, tx).await;
    assert!(!later_task.is_finished());
    drop(later_snapshot);
    assert_eq!(active_version, gate.durable_watermark() + 1);
    assert_eq!(later_version, active_version + 1);
    // The ack path published the later version, so the visible floor is past the
    // still-active one while the durable prefix is not: the drain must scan beyond it.
    assert_eq!(gate.last_committed(), later_version);
    assert!(gate.last_committed() > gate.durable_watermark());
    assert_eq!(gate.orphan_max(), 0);

    hook.armed.store(true, Ordering::Release);
    let (mut tx, orphan_snapshot) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let scan_orphan_rid = tbl
        .insert_tx(&InnerValue::Str("scan-orphan".into()), Some(&mut tx))
        .await
        .unwrap();
    let owned = repo.clone();
    hook.panic_after_resume.store(true, Ordering::Release);
    let orphan_task = tokio::spawn(async move { owned.commit_tx(tx).await });
    within("reached@86", hook.reached.notified()).await;
    hook.panic_resume.notify_one();
    assert!(orphan_task.await.unwrap_err().is_panic());
    drop(orphan_snapshot);
    let scan_orphan_version = gate.orphan_max();
    assert_eq!(scan_orphan_version, later_version + 1);
    assert!(gate.is_orphaned_post_wal(scan_orphan_version));
    assert!(!gate.is_active_post_wal(later_version));
    assert!(!gate.is_orphaned_post_wal(later_version));
    assert!(gate.is_active_post_wal(active_version));
    assert_eq!(active_version, gate.durable_watermark() + 1);
    assert!(repo.drainer().window_entry(active_version).is_some());
    let scan_bound = gate.last_committed().max(gate.orphan_max());
    assert!(scan_bound > gate.durable_watermark());
    assert!(scan_bound > active_version);
    let recover_before = repo.drainer().recover_calls();
    let drained = within("drain_step@102", repo.drainer().drain_step(&repo.core))
        .await
        .unwrap();
    let recover_after = repo.drainer().recover_calls();
    hook.resume.notify_one();
    within("active_task@105", active_task)
        .await
        .unwrap()
        .unwrap();
    let later_outcome = within("later ack", later_task).await.unwrap().unwrap();
    assert_eq!(later_outcome.commit_version, later_version);
    drop(snapshot);
    assert_eq!(drained, 0);
    // Red before B1: the empty prefix caused a full WAL recovery here.
    assert_eq!(recover_after, recover_before);
    within("drain_all@110", repo.drainer().drain_all(&repo.core))
        .await
        .unwrap();
    assert_eq!(gate.durable_watermark(), scan_orphan_version);
    assert!(!gate.is_orphaned_post_wal(scan_orphan_version));
    assert_eq!(gate.durable_watermark(), gate.last_committed());
    assert_eq!(
        tbl.get(later_rid).await.unwrap(),
        InnerValue::Str("later".into()),
    );
    assert_eq!(
        tbl.get(scan_orphan_rid).await.unwrap(),
        InnerValue::Str("scan-orphan".into()),
    );
    assert_eq!(
        tbl.get(active_rid).await.unwrap(),
        InnerValue::Str("active".into()),
    );

    // B2: explicitly poll the spawned drain under a held replay lock; the
    // handshake rules out a false pass from a task that was never scheduled.
    let (mut tx, snapshot) = repo.begin_tx(IsolationLevel::Serializable).await.unwrap();
    tbl.insert_tx(&InnerValue::Str("wait-for-replay".into()), Some(&mut tx))
        .await
        .unwrap();
    let waiting_outcome = within("commit_tx@133", repo.commit_tx(tx)).await.unwrap();
    drop(snapshot);
    let drainer = repo.drainer();
    // Genuine-gap control for B1: remove only the offered RAM copy. The
    // accepted WAL entry must be recovered once the waiting pass owns replay.
    drainer.window_remove_for_test(waiting_outcome.commit_version);
    let recover_before_gap = drainer.recover_calls();
    let replay_guard = drainer.replay_lock_for_test().await;
    let (polled_tx, polled_rx) = oneshot::channel();
    let owned = repo.clone();
    let drain_task = tokio::spawn(async move {
        let drainer = owned.drainer();
        let drain = drainer.drain_all(&owned.core);
        tokio::pin!(drain);
        let first = poll_fn(|cx| Poll::Ready(drain.as_mut().poll(cx))).await;
        polled_tx.send(first.is_pending()).unwrap();
        match first {
            Poll::Ready(result) => result,
            Poll::Pending => drain.await,
        }
    });
    let pending = within("polled_rx@154", polled_rx).await.unwrap();
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }
    let finished_while_locked = drain_task.is_finished();
    let durable_while_locked = gate.durable_watermark();
    drop(replay_guard);
    let waiting_drained = within("drain_task@161", drain_task).await.unwrap().unwrap();
    // Red before B2: CAS contention returned Ok(0), not Pending.
    assert!(pending);
    assert!(!finished_while_locked);
    assert!(durable_while_locked < waiting_outcome.commit_version);
    assert_eq!(waiting_drained, 1);
    assert_eq!(drainer.recover_calls(), recover_before_gap + 1);
    assert_eq!(gate.durable_watermark(), gate.last_committed());

    hook.armed.store(true, Ordering::Release);
    let (mut tx, snapshot) = repo.begin_tx(IsolationLevel::Serializable).await.unwrap();
    let b = tbl
        .insert_tx(&InnerValue::Str("B".into()), Some(&mut tx))
        .await
        .unwrap();
    let posting_key = Bytes::from_static(b"post_wal_cancel_posting");
    let posting_value = Bytes::copy_from_slice(b.as_bytes());
    tx.index_write_set.push((
        table_token_for("t"),
        IndexWriteOp::SetPosting {
            key: posting_key.clone(),
            value: posting_value.clone(),
            provenance: Default::default(),
        },
    ));
    let owned = repo.clone();
    hook.panic_after_resume.store(true, Ordering::Release);
    let task = tokio::spawn(async move { owned.commit_tx(tx).await });
    within("reached@188", hook.reached.notified()).await;
    hook.panic_resume.notify_one();
    assert!(task.await.unwrap_err().is_panic());
    drop(snapshot);

    let (mut tx, snapshot) = repo.begin_tx(IsolationLevel::Serializable).await.unwrap();
    let c = tbl
        .insert_tx(&InnerValue::Str("C".into()), Some(&mut tx))
        .await
        .unwrap();
    let (later_version, later_task) = spawn_settled_commit(&repo, tx).await;
    within(
        "drain orphan before ack",
        repo.drainer().drain_all(&repo.core),
    )
    .await
    .unwrap();
    let outcome = later_task.await.unwrap().unwrap();
    assert_eq!(outcome.commit_version, later_version);
    drop(snapshot);
    within("drain_all@200", repo.drainer().drain_all(&repo.core))
        .await
        .unwrap();
    assert_eq!(gate.durable_watermark(), gate.last_committed());
    assert_eq!(gate.last_committed(), outcome.commit_version);
    assert_eq!(tbl.get(b).await.unwrap(), InnerValue::Str("B".into()));
    assert_eq!(
        tbl.info_store()
            .get(posting_key.clone().into())
            .await
            .unwrap(),
        posting_value,
    );
    assert_eq!(tbl.counter().get().await.unwrap(), 7);
    within("drain_all@212", repo.drainer().drain_all(&repo.core))
        .await
        .unwrap();
    assert_eq!(tbl.counter().get().await.unwrap(), 7);

    hook.after_counter.store(true, Ordering::Release);
    hook.armed.store(true, Ordering::Release);
    let (mut tx, snapshot) = repo.begin_tx(IsolationLevel::Serializable).await.unwrap();
    let after_counter = tbl
        .insert_tx(&InnerValue::Str("counter-applied".into()), Some(&mut tx))
        .await
        .unwrap();
    let owned = repo.clone();
    hook.panic_after_resume.store(true, Ordering::Release);
    let task = tokio::spawn(async move { owned.commit_tx(tx).await });
    within("reached@224", hook.reached.notified()).await;
    hook.panic_resume.notify_one();
    assert!(task.await.unwrap_err().is_panic());
    drop(snapshot);
    let orphan_version = gate.orphan_max();
    hook.fail_orphan_markers.store(true, Ordering::Release);
    // B3 markers: injected persistence failure is advisory, not a drain error.
    assert_eq!(
        within("drain_all@231", repo.drainer().drain_all(&repo.core))
            .await
            .unwrap(),
        1
    );
    assert!(!gate.is_orphaned_post_wal(orphan_version));
    assert_eq!(gate.durable_watermark(), orphan_version);
    assert_eq!(tbl.counter().get().await.unwrap(), 8);
    within("drain_all@235", repo.drainer().drain_all(&repo.core))
        .await
        .unwrap();
    assert!(!gate.is_orphaned_post_wal(orphan_version));
    assert_eq!(tbl.counter().get().await.unwrap(), 8);
    assert_eq!(
        tbl.get(after_counter).await.unwrap(),
        InnerValue::Str("counter-applied".into()),
    );
    assert_eq!(gate.durable_watermark(), gate.last_committed());

    // B3 counter: preserve a real WAL Put but inject metric drift so its
    // negative CounterDelta is refused by the real counter's checked CAS.
    hook.after_counter.store(false, Ordering::Release);
    hook.armed.store(true, Ordering::Release);
    let (mut tx, snapshot) = repo.begin_tx(IsolationLevel::Serializable).await.unwrap();
    let counter_failed_rid = tbl
        .insert_tx(&InnerValue::Str("counter-failed".into()), Some(&mut tx))
        .await
        .unwrap();
    tx.counter_deltas.insert(table_token_for("t"), -1);
    let owned = repo.clone();
    hook.panic_after_resume.store(true, Ordering::Release);
    let task = tokio::spawn(async move { owned.commit_tx(tx).await });
    within("reached@256", hook.reached.notified()).await;
    hook.panic_resume.notify_one();
    assert!(task.await.unwrap_err().is_panic());
    drop(snapshot);
    let counter_failed_version = gate.orphan_max();
    tbl.counter().set_to(0).await.unwrap();
    let counter_error = tbl.counter().increment(-1).await.unwrap_err();
    assert!(counter_error.to_string().contains("cannot go below zero"));
    // Red before B3: increment(-1)? returned Err and wedged durability.
    assert_eq!(
        within("drain_all@265", repo.drainer().drain_all(&repo.core))
            .await
            .unwrap(),
        1
    );
    assert!(!gate.is_orphaned_post_wal(counter_failed_version));
    assert_eq!(gate.durable_watermark(), counter_failed_version);
    assert_eq!(gate.durable_watermark(), gate.last_committed());
    assert_eq!(tbl.counter().get().await.unwrap(), 0);
    assert_eq!(
        tbl.get(counter_failed_rid).await.unwrap(),
        InnerValue::Str("counter-failed".into()),
    );
    assert_eq!(
        within("drain_all@274", repo.drainer().drain_all(&repo.core))
            .await
            .unwrap(),
        0
    );
    assert_eq!(tbl.counter().get().await.unwrap(), 0);

    hook.pre_wal.store(true, Ordering::Release);
    hook.armed.store(true, Ordering::Release);
    let (mut tx, snapshot) = repo.begin_tx(IsolationLevel::Serializable).await.unwrap();
    assert!(tbl
        .set_tx(a, &InnerValue::Str("pre-WAL".into()), Some(&mut tx))
        .await
        .unwrap());
    let owned = repo.clone();
    let task = tokio::spawn(async move { owned.commit_tx(tx).await });
    within("reached@286", hook.reached.notified()).await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    drop(snapshot);
    // This pre-WAL seam is AFTER prelock: cancellation detaches the driver.
    hook.resume.notify_one();
    within(
        "pre-WAL detached completion",
        repo.quiesce_commits(std::time::Duration::from_secs(45)),
    )
    .await
    .unwrap();
    within(
        "pre-WAL detached drain",
        repo.drainer().drain_all(&repo.core),
    )
    .await
    .unwrap();
    assert_eq!(tbl.get(a).await.unwrap(), InnerValue::Str("pre-WAL".into()));
    assert_eq!(gate.durable_watermark(), gate.completion().watermark());

    let (mut tx, snapshot) = repo.begin_tx(IsolationLevel::Serializable).await.unwrap();
    assert!(tbl
        .set_tx(a, &InnerValue::Str("A".into()), Some(&mut tx))
        .await
        .unwrap());
    let padding = InnerValue::Str(
        "x".repeat(shamir_tunables::instance_defaults::WAL_SEGMENT_MAX_BYTES as usize + 1024),
    );
    tbl.insert_tx(&padding, Some(&mut tx)).await.unwrap();
    within("commit_tx@302", repo.commit_tx(tx)).await.unwrap();
    drop(snapshot);
    assert_eq!(tbl.get(a).await.unwrap(), InnerValue::Str("A".into()));
    let segments_before = wal_segment_names(&path).await;
    assert!(segments_before.len() >= 2);
    repo.repo_interner().await.unwrap().persist().await.unwrap();
    within("drain_all@308", repo.drainer().drain_all(&repo.core))
        .await
        .unwrap();
    repo.flush_buffers().await.unwrap();
    assert_eq!(gate.durable_watermark(), gate.last_committed());
    let segments_after = wal_segment_names(&path).await;
    assert!(segments_after.len() < segments_before.len());
    assert!(segments_before
        .iter()
        .any(|name| !segments_after.contains(name)));
    let wal = repo.repo_wal().await.unwrap();
    assert!(!wal.recover().await.unwrap().iter().any(|entry| {
        entry
            .ops
            .iter()
            .any(|op| matches!(op, shamir_wal::WalOpV2::Put { rid, .. } if *rid == b))
    }));
    // The refused orphan metric must not retain its sealed WAL segment.
    assert!(gate.durable_watermark() >= counter_failed_version);
    assert!(!wal
        .recover()
        .await
        .unwrap()
        .iter()
        .any(|entry| entry.commit_version == counter_failed_version));
    drop(wal);
    drop(gate);
    drop(tbl);
    drop(drainer);
    drop(repo);

    let reopened = reopen_after_drop(&path).await;
    let tbl = reopened.get_table("t").await.unwrap();
    assert_eq!(
        tbl.get(counter_failed_rid).await.unwrap(),
        InnerValue::Str("counter-failed".into()),
    );
    for (rid, value) in [(a, "A"), (b, "B"), (c, "C")] {
        assert_eq!(tbl.get(rid).await.unwrap(), InnerValue::Str(value.into()));
    }
    assert_eq!(tbl.get(a).await.unwrap(), InnerValue::Str("A".into()));
    assert_eq!(
        tbl.info_store().get(posting_key.into()).await.unwrap(),
        posting_value,
    );
}

async fn wal_segment_names(path: &Path) -> Vec<String> {
    let mut name = path.file_name().unwrap().to_os_string();
    name.push(".shamirwal");
    let mut entries = tokio::fs::read_dir(path.with_file_name(name))
        .await
        .unwrap();
    let mut names = Vec::new();
    while let Some(entry) = entries.next_entry().await.unwrap() {
        if entry
            .path()
            .extension()
            .is_some_and(|extension| extension == "wal")
        {
            names.push(entry.file_name().to_string_lossy().into_owned());
        }
    }
    names.sort();
    names
}

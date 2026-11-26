use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use shamir_test_diag::FlakeGuard;
use shamir_tx::{IndexWriteOp, IsolationLevel, TxContext};
use shamir_types::types::value::InnerValue;
use tokio::sync::Notify;

use crate::tx::commit::{
    PostWalPrePublishHook, TEST_ORPHAN_MARKER_PARK, TEST_POST_WAL_PRE_PUBLISH_HOOK,
};
use crate::tx::meta::recovery_marker::load_last_committed;
use crate::tx::repo::instance::repo_instance::OnlineBackupError;
use crate::tx::repo::{BoxRepoFactory, RepoInstance};
use crate::tx::table::table_manager::table_token_for;
use crate::tx::table::TableConfig;
use crate::tx::tests::recovery::snapshot_gc::snapshot_visibility_park_tests::spawn_settled_commit;

pub(crate) async fn fixture(name: &str) -> (RepoInstance, Arc<PostWalPrePublishHook>) {
    let hook = Arc::new(PostWalPrePublishHook {
        repo_name: name.into(),
        pre_wal: AtomicBool::new(false),
        after_counter: AtomicBool::new(false),
        fail_orphan_markers: AtomicBool::new(false),
        panic_after_resume: AtomicBool::new(false),
        panic_resume: Notify::new(),
        reached: Notify::new(),
        resume: Notify::new(),
        armed: AtomicBool::new(false),
    });
    // The supported runner isolates each test in its own process. Install
    // before any drainer access: Drainer::spawn returns for this repo name.
    TEST_POST_WAL_PRE_PUBLISH_HOOK
        .set(Arc::clone(&hook))
        .ok()
        .unwrap();
    // InMemory has no MemBuffer or disk fsync worker. repo_wal still uses
    // begin_grouped with a Mem sink, so this exercises the real WAL path.
    let repo = RepoInstance::from_factory(
        name.into(),
        BoxRepoFactory::in_memory(),
        vec![TableConfig::new("a"), TableConfig::new("b")],
    )
    .await
    .unwrap();
    (repo, hook)
}

pub(crate) async fn panic_post_wal(
    repo: &RepoInstance,
    hook: &PostWalPrePublishHook,
    tx: TxContext,
) -> u64 {
    // The hook captures panic mode before signalling reached; arm it before
    // spawning, not after the committing task has parked.
    hook.panic_after_resume.store(true, Ordering::Release);
    hook.armed.store(true, Ordering::Release);
    let owned = repo.clone();
    let task = tokio::spawn(async move { owned.commit_tx(tx).await });
    // Notify retains the permit even if the spawned task arrives first.
    // The default seam follows WAL acceptance, window offer and Phase 5a,
    // but precedes Phase 5b counters, Phase 5c postings and Phase 6 publish.
    // after_counter instead parks after tracked 5b, still before 5c/publish.
    hook.reached.notified().await;
    let gate = repo.tx_gate().await.unwrap();
    let version = *repo.drainer().window_keys().last().unwrap();
    assert!(gate.is_active_post_wal(version));
    // Unwind the inline driver: cancellation would detach this same tail
    // and finish it, rather than transfer replay ownership to the drainer.
    hook.panic_resume.notify_one();
    assert!(task.await.unwrap_err().is_panic());
    assert!(!gate.is_active_post_wal(version));
    assert!(gate.is_orphaned_post_wal(version));
    assert_eq!(gate.orphan_max(), version);
    assert!(gate.durable_watermark() < version);
    version
}

// Coverage, not a claimed red test: current orphan completion is expected
// to publish both tables and apply each counter exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn post_wal_panic_multi_table_values_counters_and_durability() {
    let (repo, hook) = fixture("post_wal_multi_table").await;
    let a = repo.get_table("a").await.unwrap();
    let b = repo.get_table("b").await.unwrap();
    let gate = repo.tx_gate().await.unwrap();
    let (mut tx, snapshot) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let a_rid = a
        .insert_tx(&InnerValue::Str("orphan-a".into()), Some(&mut tx))
        .await
        .unwrap();
    let b_rid = b
        .insert_tx(&InnerValue::Str("orphan-b".into()), Some(&mut tx))
        .await
        .unwrap();
    let version = panic_post_wal(&repo, &hook, tx).await;
    drop(snapshot);
    assert_eq!(a.counter().get().await.unwrap(), 0);
    assert_eq!(b.counter().get().await.unwrap(), 0);

    assert_eq!(repo.drainer().drain_all(&repo.core).await.unwrap(), 1);
    assert_eq!(
        a.get(a_rid).await.unwrap(),
        InnerValue::Str("orphan-a".into())
    );
    assert_eq!(
        b.get(b_rid).await.unwrap(),
        InnerValue::Str("orphan-b".into())
    );
    assert_eq!(a.counter().get().await.unwrap(), 1);
    assert_eq!(b.counter().get().await.unwrap(), 1);
    assert_eq!(gate.last_committed(), version);
    assert_eq!(gate.durable_watermark(), version);
    assert!(!gate.is_orphaned_post_wal(version));
    assert_eq!(repo.drainer().drain_all(&repo.core).await.unwrap(), 0);
    assert_eq!(a.counter().get().await.unwrap(), 1);
    assert_eq!(b.counter().get().await.unwrap(), 1);
}

// Coverage, not a claimed red test: metric failure is advisory today.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn post_wal_panic_multi_table_counter_underflow_does_not_block_durability() {
    let (repo, hook) = fixture("post_wal_multi_table_underflow").await;
    let a = repo.get_table("a").await.unwrap();
    let b = repo.get_table("b").await.unwrap();
    let gate = repo.tx_gate().await.unwrap();
    let (mut tx, snapshot) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let a_rid = a
        .insert_tx(&InnerValue::Str("counter-drift".into()), Some(&mut tx))
        .await
        .unwrap();
    let b_rid = b
        .insert_tx(&InnerValue::Str("healthy-counter".into()), Some(&mut tx))
        .await
        .unwrap();
    // Preserve the real data Put while injecting the same metric drift as
    // the existing single-table test: -1 cannot be applied to a zero count.
    tx.counter_deltas.insert(table_token_for("a"), -1);
    let version = panic_post_wal(&repo, &hook, tx).await;
    drop(snapshot);
    assert_eq!(a.counter().get().await.unwrap(), 0);
    let error = a.counter().increment(-1).await.unwrap_err();
    assert!(error.to_string().contains("cannot go below zero"));

    assert_eq!(repo.drainer().drain_all(&repo.core).await.unwrap(), 1);
    assert_eq!(
        a.get(a_rid).await.unwrap(),
        InnerValue::Str("counter-drift".into()),
    );
    assert_eq!(
        b.get(b_rid).await.unwrap(),
        InnerValue::Str("healthy-counter".into()),
    );
    assert_eq!(a.counter().get().await.unwrap(), 0);
    assert_eq!(b.counter().get().await.unwrap(), 1);
    assert_eq!(gate.last_committed(), version);
    assert_eq!(gate.durable_watermark(), version);
    assert!(!gate.is_orphaned_post_wal(version));
    assert_eq!(repo.drainer().drain_all(&repo.core).await.unwrap(), 0);
    assert_eq!(a.counter().get().await.unwrap(), 0);
    assert_eq!(b.counter().get().await.unwrap(), 1);
}

// Expected red by static inspection, not execution: the orphan's IndexPut
// is replayed after the later inline replacement, but the later IndexPut
// is skipped by index_applied. Versioned record history must also keep the
// later value; the unversioned posting must not regress to the orphan value.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn post_wal_panic_same_record_later_writer_posting_survives_drain() {
    let (repo, hook) = fixture("post_wal_same_record_posting").await;
    let table = repo.get_table("a").await.unwrap();
    let gate = repo.tx_gate().await.unwrap();
    let token = table_token_for("a");
    let key = Bytes::from_static(b"post_wal_same_record_posting");
    let orphan_posting = Bytes::from_static(b"orphan-posting");
    let later_posting = Bytes::from_static(b"later-posting");

    let (mut tx, snapshot) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let rid = table
        .insert_tx(&InnerValue::Str("seed".into()), Some(&mut tx))
        .await
        .unwrap();
    repo.commit_tx(tx).await.unwrap();
    drop(snapshot);
    assert_eq!(repo.drainer().drain_all(&repo.core).await.unwrap(), 1);
    let durable_before = gate.durable_watermark();

    let (mut tx, snapshot) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    assert!(table
        .set_tx(rid, &InnerValue::Str("orphan".into()), Some(&mut tx))
        .await
        .unwrap());
    // Like post_wal_cancel_tests, stage a posting directly to isolate the
    // commit/replay contract from index planners and concurrent DDL.
    tx.index_write_set.push((
        token,
        IndexWriteOp::SetPosting {
            key: key.clone(),
            value: orphan_posting,
            provenance: Default::default(),
        },
    ));
    let orphan_version = panic_post_wal(&repo, &hook, tx).await;
    drop(snapshot);

    // Start only AFTER panic unwind has completed, and do not drain the orphan.
    let (mut tx, snapshot) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    assert!(table
        .set_tx(rid, &InnerValue::Str("later".into()), Some(&mut tx))
        .await
        .unwrap());
    tx.index_write_set.push((
        token,
        IndexWriteOp::SetPosting {
            key: key.clone(),
            value: later_posting.clone(),
            provenance: Default::default(),
        },
    ));
    let (later_version, task) = spawn_settled_commit(&repo, tx).await;
    drop(snapshot);
    assert_eq!(later_version, orphan_version + 1);
    assert!(!task.is_finished());
    assert_eq!(gate.durable_watermark(), durable_before);
    assert!(gate.is_orphaned_post_wal(orphan_version));
    assert!(repo.drainer().window_entry(orphan_version).is_some());
    assert_eq!(
        table.get(rid).await.unwrap(),
        InnerValue::Str("later".into()),
    );
    assert_eq!(
        table.info_store().get(key.clone().into()).await.unwrap(),
        later_posting,
    );

    assert_eq!(repo.drainer().drain_all(&repo.core).await.unwrap(), 2);
    let outcome = task.await.unwrap().unwrap();
    assert_eq!(outcome.commit_version, later_version);
    assert_eq!(gate.durable_watermark(), outcome.commit_version);
    assert_eq!(gate.last_committed(), outcome.commit_version);
    assert!(!gate.is_orphaned_post_wal(orphan_version));
    assert_eq!(
        table.get(rid).await.unwrap(),
        InnerValue::Str("later".into()),
    );
    assert_eq!(
        table.info_store().get(key.clone().into()).await.unwrap(),
        later_posting,
        "draining an older orphan must not overwrite the later inline posting",
    );
    assert_eq!(repo.drainer().drain_all(&repo.core).await.unwrap(), 0);
    assert_eq!(
        table.get(rid).await.unwrap(),
        InnerValue::Str("later".into()),
    );
    assert_eq!(
        table.info_store().get(key.into()).await.unwrap(),
        later_posting,
    );
}

/// Deadline budget for the regression below. The engine deadline is a real
/// `std::time::Instant` (NOT the Tokio clock): its value only has to
/// outlive the spawn→park span (a handful of pure handshakes), and its
/// expiry is waited out against the very same `Instant` — no sleep as a
/// rendezvous. A deadline missed before the park fails LOUDLY (the
/// `reached` handshake never fires and the bounded wait times out).
const DEADLINE_BUDGET: Duration = Duration::from_secs(2);
/// Generous harness cap converting a self-deadlock into a loud failure.
const SEAL_BUDGET: Duration = Duration::from_secs(60);
/// Engine-unit writer settle cap: the writer's commit future is polled once
/// (to Pending) while the gate is still closed, and awaited again only after
/// the seal has refused and reopened admission — this cap is the harness
/// settle bound ONLY, deliberately independent of the 2 s engine admission
/// deadline so the two can never race.
const WRITER_SETTLE: Duration = Duration::from_secs(30);

/// R2a follow-up 3 regression: the seal deadline must NEVER abandon the
/// settled drain while it is suspended inside the REAL recovery-marker
/// store write (`persist_markers` — monotonicity read/compare done, actual
/// `save_last_committed` not yet started, `marker_write_mutex` held).
///
/// Shape: a REAL post-WAL orphan (`panic_post_wal`, background drainer
/// suppressed by the fixture) is finalized by the seal's settled drain,
/// which parks on the `TEST_ORPHAN_MARKER_PARK` gate between the marker
/// read/compare and the actual marker save. A writer tx is pre-staged
/// BEFORE the seal; its real commit future is started and polled only after
/// the deadline has expired, staying Pending under the closed gate. The
/// controlled seal must (a) keep admission closed with the marker mutex
/// held by the parked drain, (b) keep the pending writer from bypassing,
/// (c) release the write and return the typed `DeadlineExceeded` only AFTER
/// the drain completed the real marker save, and (d) let the same writer
/// settle at exactly orphan+1 with the on-store marker advanced exactly to
/// its version.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn seal_deadline_expires_while_drain_parked_inside_marker_save() {
    // Failure-only flake diagnostics: the timeline proves which phase hung
    // (the park handshake, the deadline wait, or the released-drain join)
    // if this ever wedges on CI; the report prints ONLY on failure. The
    // watchdog fires below the runner's slow-kill so the dump lands first.
    let diag = FlakeGuard::with_watchdog(
        "seal_deadline_expires_while_drain_parked_inside_marker_save",
        Duration::from_secs(120),
    );
    let (repo, hook) = fixture("seal_deadline_marker_save").await;
    let table = repo.get_table("a").await.unwrap();
    let gate = repo.tx_gate().await.unwrap();
    let (mut tx, snapshot) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let rid = table
        .insert_tx(&InnerValue::Str("orphan-marker-save".into()), Some(&mut tx))
        .await
        .unwrap();
    let orphan_version = panic_post_wal(&repo, &hook, tx).await;
    diag.mark("orphan parked at the post-WAL seam");
    drop(snapshot);
    let marker_before = load_last_committed(&repo.tx_info_store().await.unwrap())
        .await
        .unwrap();

    // Arm the one-shot park inside persist_markers, before the save. The
    // save is dispatched as a non-abortable task owning the cloned info
    // store — the same lifetime shape as the production store write.
    let (reached_tx, reached_rx) = tokio::sync::oneshot::channel::<()>();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
    let park = Arc::new(crate::tx::commit::OrphanMarkerSaveHook {
        repo_name: repo.name().into(),
        armed: AtomicBool::new(true),
        reached: std::sync::Mutex::new(Some(reached_tx)),
        release: std::sync::Mutex::new(Some(release_rx)),
    });
    TEST_ORPHAN_MARKER_PARK
        .set(Arc::clone(&park))
        .ok()
        .expect("single park gate per process");

    // Pre-stage the real writer BEFORE the seal closes admission: the tx is
    // begun and its write applied while the gate is still open. Its commit
    // future is only constructed (and only ever polled) AFTER the deadline
    // has elapsed, so admission can never observe it mid-flight — no
    // cap-vs-deadline timing race.
    let writer_table = repo.get_table("a").await.unwrap();
    let (mut writer_tx, writer_guard) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    writer_table
        .insert_tx(
            &InnerValue::Str("after-deadline".into()),
            Some(&mut writer_tx),
        )
        .await
        .unwrap();

    let deadline = Instant::now() + DEADLINE_BUDGET;
    let seal_repo = repo.clone();
    let seal = tokio::spawn(async move {
        seal_repo
            .seal_point_controlled(Some(deadline), || false)
            .await
    });

    // Handshake: the drain is now parked INSIDE the real marker write
    // (monotonicity check done, save not started, marker mutex held — the
    // save task owns the store and cannot be aborted).
    tokio::time::timeout(SEAL_BUDGET, reached_rx)
        .await
        .expect("the seal drain never reached the marker save")
        .expect("the marker save task dropped the reached one-shot");
    diag.mark("drain parked inside the marker save");

    // Wait out the REAL engine deadline — an actual absolute deadline, not
    // a synchronization sleep.
    tokio::time::sleep_until(deadline.into()).await;
    diag.mark("engine deadline elapsed");

    // The parked drain must survive the expiry — abandoning it here is the
    // regression this test pins.
    assert!(
        !seal.is_finished(),
        "the deadline must not abandon the drain parked in the marker write"
    );

    // Now — and only now — construct the pre-staged writer's real commit
    // future. No spawn: the future is owned inline and polled exactly once,
    // which must yield Pending against the closed gate. It stays owned here
    // untouched until the on-store marker has been asserted, so a resumed
    // writer can never advance the marker ahead of the orphan assertion.
    let _guard = writer_guard;
    let mut writer = Box::pin(repo.commit_tx(writer_tx));
    assert!(
        futures::poll!(writer.as_mut()).is_pending(),
        "the staged writer must stay PENDING under the closed gate"
    );
    assert!(repo.core().commit_admission().is_closed());
    assert!(
        repo.core().marker_write_mutex().try_lock().is_err(),
        "the parked drain must hold the marker write mutex"
    );
    assert_eq!(
        gate.last_committed(),
        orphan_version,
        "no write may bypass the closed barrier"
    );

    // Release the write. The drain completes the real marker save (the
    // non-abortable task joins under the marker mutex), reaches the step
    // boundary and only THEN the expired deadline fires. The Ok arm holds
    // the non-Debug `SealHold` guard, so the result is matched instead of
    // `expect_err`-ed.
    release_tx
        .send(())
        .expect("the marker save task dropped the release one-shot");
    diag.mark("marker save released; awaiting the seal result");
    let sealed = tokio::time::timeout(SEAL_BUDGET, seal)
        .await
        .expect("the seal never returned after the parked drain was released")
        .expect("seal join");
    match sealed {
        Err(OnlineBackupError::DeadlineExceeded) => {}
        Ok(_) => panic!("an expired deadline must refuse the seal"),
        Err(e) => panic!("unexpected seal error: {e}"),
    }
    diag.mark("seal refused with DeadlineExceeded after the drain completed");

    // Fail-closed RAII on the deadline path: pause released, gate reopened.
    assert!(!repo.drainer().pause_held());
    assert!(!repo.core().commit_admission().is_closed());

    // The orphan's REAL marker save landed BEFORE the refusal: read the
    // on-store marker before polling the writer any further.
    assert_eq!(
        table.get(rid).await.unwrap(),
        InnerValue::Str("orphan-marker-save".into()),
        "the parked drain completed the orphan recovery before the refusal"
    );
    let marker = load_last_committed(&repo.tx_info_store().await.unwrap())
        .await
        .unwrap();
    assert!(
        marker >= marker_before,
        "the stored recovery marker must never regress"
    );
    assert_eq!(
        marker,
        Some(orphan_version),
        "the parked drain persisted the orphan's marker before the refusal"
    );

    // The SAME pending writer now settles successfully within the harness
    // settle cap. The owned future is awaited inline — no join handle, and
    // it was not polled between the pending probe and here, so the marker
    // assertions above were race-free.
    let outcome = tokio::time::timeout(WRITER_SETTLE, writer)
        .await
        .expect("the staged writer hung past the harness settle cap")
        .expect("the resumed writer commit must succeed");
    assert_eq!(
        outcome.commit_version,
        orphan_version + 1,
        "the pre-staged writer takes exactly the next version"
    );
    assert_eq!(gate.last_committed(), outcome.commit_version);

    // Inline commits do NOT `mark_durable` (the background drainer owns it
    // and the fixture suppresses its spawn), so the durable watermark must
    // still be sitting exactly at the seal drain's orphan version here.
    // Asserting it at the writer's version without any drain would be an
    // oracle that only passes if a suppressed background drainer raced us.
    assert_eq!(
        gate.durable_watermark(),
        orphan_version,
        "no drain has run since the seal: the writer cannot be durable yet",
    );

    // Bounded explicit drain: finalize the writer's window entry
    // (`mark_durable`) and confirm the watermark advances to exactly the
    // writer's version, with a follow-up pass finding nothing left.
    tokio::time::timeout(WRITER_SETTLE, repo.drainer().drain_all(&repo.core))
        .await
        .expect("the explicit drain hung past the harness settle cap")
        .expect("explicit drain");
    assert_eq!(gate.durable_watermark(), outcome.commit_version);
    assert_eq!(repo.drainer().drain_all(&repo.core).await.unwrap(), 0);

    // Reread the on-store marker exact: the writer's own Phase 6.5 write
    // advanced it to the new monotonic max.
    let marker = load_last_committed(&repo.tx_info_store().await.unwrap())
        .await
        .unwrap();
    assert_eq!(
        marker,
        Some(orphan_version + 1),
        "the stored recovery marker advanced exactly to the resumed writer's version"
    );
}

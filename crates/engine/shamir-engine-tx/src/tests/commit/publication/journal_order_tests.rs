use std::sync::atomic::Ordering;
use std::sync::Arc;

use futures::poll;
use shamir_tx::{CommitVisibility, IsolationLevel, JOURNAL_CHANNEL_CAPACITY};
use shamir_types::types::value::InnerValue;

use crate::tx::tx_outcome::MaterializationState;

use crate::tx::tests::drain::orphans::post_wal_orphan_ordering_tests::fixture;
use crate::tx::tests::recovery::snapshot_gc::snapshot_visibility_park_tests::spawn_settled_commit;

// Red by construction: the unbounded engine read returns B while A still
// prevents the completion watermark from reaching either version.
async fn parked_commit_hides_later_journal_event(name: &str, visibility: CommitVisibility) {
    let (repo, hook) = fixture(name).await;
    let gate = repo.tx_gate().await.unwrap();
    let feed = repo.journal_feed_for_test().await.unwrap();
    let a = repo.get_table("a").await.unwrap();
    let b = repo.get_table("b").await.unwrap();
    let (mut tx_a, snapshot_a) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let rid_a = a
        .insert_tx(&InnerValue::Str("journal-a".into()), Some(&mut tx_a))
        .await
        .unwrap();
    tx_a.set_visibility(visibility);
    hook.armed.store(true, Ordering::Release);
    let owned = repo.clone();
    let task_a = tokio::spawn(async move { owned.commit_tx(tx_a).await });
    hook.reached.notified().await;
    let version_a = *repo.drainer().window_keys().last().unwrap();
    assert!(gate.completion().watermark() < version_a);

    // B uses the lock-free synchronous path even when A is AsyncIndex:
    // A holds commit_lock at its seam, so two AsyncIndex commits cannot race.
    let (mut tx_b, snapshot_b) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let rid_b = b
        .insert_tx(&InnerValue::Str("journal-b".into()), Some(&mut tx_b))
        .await
        .unwrap();
    assert!(
        tx_b.cas_set.is_empty()
            && tx_b.ri_barrier_tokens_is_empty()
            && tx_b.footprint_tokens.is_empty(),
        "B must not acquire the commit lock held by AsyncIndex A",
    );
    let (version_b, task_b) = spawn_settled_commit(&repo, tx_b).await;
    assert_eq!(version_b, version_a + 1);
    for _ in 0..10_000 {
        if feed.last_persisted_version() >= version_b {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(
        feed.last_persisted_version() >= version_b,
        "B must be persisted before the regression read, not merely queued",
    );
    assert!(gate.completion().watermark() < version_a);
    let before = repo.read_changelog_from(version_a, 10).await.unwrap();
    // Release and join before asserting the red oracle, so failure does not
    // leave the parked committer detached inside the test runtime.
    hook.resume.notify_one();
    let mut outcome_a = task_a.await.unwrap().unwrap();
    let outcome_b = task_b.await.unwrap().unwrap();
    assert_eq!(outcome_b.commit_version, version_b);
    if visibility == CommitVisibility::AsyncIndex {
        assert!(
            outcome_a.background.is_some(),
            "A must use the AsyncIndex path",
        );
    }
    if let Some(background) = outcome_a.take_background() {
        assert_eq!(background.join().await, MaterializationState::Complete);
    }
    drop(snapshot_a);
    drop(snapshot_b);
    assert_eq!(outcome_a.commit_version, version_a);
    assert!(
        before.events.is_empty(),
        "later B must not escape past pending A",
    );
    assert_eq!(before.gap_at, None);

    let after = repo.read_changelog_from(version_a, 10).await.unwrap();
    assert_eq!(after.gap_at, None);
    assert_eq!(
        after
            .events
            .iter()
            .map(|event| event.commit_version)
            .collect::<Vec<_>>(),
        vec![version_a, version_b],
    );
    assert_eq!(after.events[0].changes[0].table, "a");
    assert_eq!(after.events[0].changes[0].key, rid_a.to_bytes());
    assert_eq!(after.events[1].changes[0].table, "b");
    assert_eq!(after.events[1].changes[0].key, rid_b.to_bytes());
    feed.shutdown();
}

#[tokio::test]
async fn journal_read_bounds_lock_free_out_of_order_publish() {
    parked_commit_hides_later_journal_event(
        "journal_order_lock_free",
        CommitVisibility::Synchronous,
    )
    .await;
}

#[tokio::test]
async fn journal_read_bounds_async_index_out_of_order_publish() {
    parked_commit_hides_later_journal_event(
        "journal_order_async_index",
        CommitVisibility::AsyncIndex,
    )
    .await;
}

// Coverage, not red on the old unbounded read: fill the writer channel so
// removing confirm_through's initial fast path makes barrier admission Pending.
// Without a yield between saturation and the manual poll, the current-thread
// writer cannot free a slot; limit zero makes the range read ready without I/O.
#[tokio::test(flavor = "current_thread")]
async fn idle_engine_journal_read_does_not_send_another_barrier() {
    let (repo, _) = fixture("journal_order_idle").await;
    let a = repo.get_table("a").await.unwrap();
    let (mut tx, snapshot) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    a.insert_tx(&InnerValue::Str("seed".into()), Some(&mut tx))
        .await
        .unwrap();
    let outcome = repo.commit_tx(tx).await.unwrap();
    drop(snapshot);
    let first = repo.read_changelog_from(0, 10).await.unwrap();
    assert_eq!(first.events.len(), 1);
    assert_eq!(first.events[0].commit_version, outcome.commit_version);
    assert_eq!(first.gap_at, None);
    let feed = repo.journal_feed_for_test().await.unwrap();
    let timeouts = feed.confirm_timeouts();
    let dropped = feed.journal_dropped();
    let seed = Arc::new(first.events[0].clone());
    // The seed read confirmed W. Reuse its event without another commit or
    // advancing W. The first Full proves no channel capacity remains.
    for _ in 0..=JOURNAL_CHANNEL_CAPACITY {
        feed.journal_enqueue(&seed);
        if feed.journal_dropped() > dropped {
            break;
        }
    }
    assert_eq!(feed.journal_dropped(), dropped + 1);
    // Overflow DEFERS: no gap is marked, the seed version is recorded for
    // deferred repair instead.
    assert_eq!(
        feed.first_gap_version(),
        0,
        "overflow defers to repair-pending instead of gapping"
    );
    let dropped_now = feed.journal_dropped() - dropped;
    assert!(dropped_now >= 1);
    assert_eq!(
        feed.repair_pending().channel_full_total(),
        dropped_now,
        "every dropped event is a recorded channel-full admission"
    );
    assert!(
        feed.repair_pending_depth() >= 1,
        "the deferred version is recorded in the repair-pending set"
    );
    let idle = repo.read_changelog_from(outcome.commit_version + 1, 0);
    tokio::pin!(idle);
    let polled = poll!(idle.as_mut());
    feed.shutdown();
    let read = match polled {
        std::task::Poll::Ready(result) => result.unwrap(),
        std::task::Poll::Pending => panic!("idle read must not wait for a new writer barrier"),
    };
    assert!(read.events.is_empty());
    // Overflow deferred the seed version; no gap exists for this read's start.
    assert_eq!(read.gap_at, None);
    assert_eq!(feed.journal_dropped(), dropped + 1);
    assert_eq!(feed.confirm_timeouts(), timeouts);
}

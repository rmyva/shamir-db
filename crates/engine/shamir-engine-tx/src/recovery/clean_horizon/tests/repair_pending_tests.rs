//! S5c-X PR5b part 2 — channel-full journal repair drainer + retention clamp
//! behaviour tests.
//!
//! All waiting is on CONDITIONS (Notify / atomic counters /
//! `last_persisted_version` / store probes) via bounded yield spins — never
//! wall-clock sleeps. Every spawned task is joined; the parked journal
//! writer is always released (or deliberately leaked only across a hard
//! reopen, where the test runtime drop reaps it) and every temp dir is
//! cleaned by `TempDir`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use shamir_storage::storage_in_memory::{InMemoryRepo, InMemoryStore};
use shamir_tx::changefeed::cold::encode_event;
use shamir_tx::changefeed::version_key;
use shamir_tx::{
    ChangelogError, ChangelogEvent, ChangelogStore, IsolationLevel, RepoChangefeed,
    JOURNAL_CHANNEL_CAPACITY,
};
use shamir_types::access::Actor;
use shamir_types::types::value::InnerValue;
use tokio::sync::Notify;

#[cfg(feature = "fjall")]
use crate::clean_horizon::checkpoint::checkpoint_before_truncate;
#[cfg(feature = "fjall")]
use crate::clean_horizon::checkpoint::test_seam;
use crate::clean_horizon::repair_pending_drain::drain_repair_pending;
use crate::repo::changelog_store::StoreChangelog;
use crate::repo::repo_instance::ChangefeedHandle;
use crate::repo::{BoxRepo, RepoInstance};
#[cfg(feature = "fjall")]
use crate::repo::{BoxRepoFactory, DurabilityPolicy};
use crate::table::TableConfig;

const MAX_KEY: u64 = u64::MAX;
const BACKLOG_KEY: u64 = u64::MAX - 1;

/// Journal store that parks the changefeed writer inside its FIRST `put` of
/// the `u64::MAX` filler (same mechanism as the orphan adversarial fixture):
/// with the writer parked mid-put and the channel filled to capacity, every
/// further `journal_enqueue` deterministically returns Full and defers.
struct ParkStore {
    inner: StoreChangelog,
    // Test fixture, not a hot path: per-version put counts let a test assert
    // "exactly one put of THIS version" instead of global counter deltas.
    put_counts: std::sync::Mutex<std::collections::BTreeMap<Bytes, usize>>,
    block_filler_once: AtomicBool,
    filler_entered: Notify,
    release_filler: Notify,
}

impl ParkStore {
    fn new() -> Self {
        Self {
            inner: StoreChangelog::new(Arc::new(InMemoryStore::new())),
            put_counts: std::sync::Mutex::new(std::collections::BTreeMap::new()),
            block_filler_once: AtomicBool::new(false),
            filler_entered: Notify::new(),
            release_filler: Notify::new(),
        }
    }
}

#[async_trait::async_trait]
impl ChangelogStore for ParkStore {
    async fn put(&self, key: Bytes, value: Bytes) -> Result<(), ChangelogError> {
        if key.len() == 8 {
            if key.as_ref() == MAX_KEY.to_be_bytes().as_slice()
                && self.block_filler_once.swap(false, Ordering::AcqRel)
            {
                self.filler_entered.notify_one();
                self.release_filler.notified().await;
            }
            *self
                .put_counts
                .lock()
                .unwrap()
                .entry(key.clone())
                .or_insert(0) += 1;
        }
        self.inner.put(key, value).await
    }

    async fn range_from(&self, key: Bytes, limit: usize) -> Result<Vec<Bytes>, ChangelogError> {
        self.inner.range_from(key, limit).await
    }
}

fn park_store() -> Arc<ParkStore> {
    Arc::new(ParkStore::new())
}

/// Total puts recorded for the journal event at `version`.
fn put_count(store: &Arc<ParkStore>, version: u64) -> usize {
    store
        .put_counts
        .lock()
        .unwrap()
        .get(&version_key(version))
        .copied()
        .unwrap_or(0)
}

fn journal_handle(store: &Arc<ParkStore>) -> ChangefeedHandle {
    let journal: Arc<dyn ChangelogStore> = Arc::clone(store) as Arc<dyn ChangelogStore>;
    ChangefeedHandle {
        feed: RepoChangefeed::new(Arc::clone(&journal)),
        store: journal,
    }
}

/// In-memory fixture with the parking journal installed and the background
/// drainer disabled (single-owner: only the test drives `drain_all`).
async fn fixture(name: &str) -> (RepoInstance, Arc<RepoChangefeed>, Arc<ParkStore>) {
    let repo = RepoInstance::new(
        name.into(),
        BoxRepo::InMemory(Arc::new(InMemoryRepo::new())),
        vec![TableConfig::new("a")],
    );
    repo.disable_background_drainer_for_test();
    let store = park_store();
    repo.install_journal_for_test(journal_handle(&store));
    let feed = repo.changefeed().await.unwrap().feed;
    (repo, feed, store)
}

/// Disk (fjall) fixture with a small segment cap so commits seal segments
/// deterministically; background drainer disabled. Requires the `fjall`
/// feature.
#[cfg(feature = "fjall")]
async fn open_disk(
    name: &str,
    path: &std::path::Path,
) -> shamir_storage::error::DbResult<RepoInstance> {
    let repo = RepoInstance::from_factory_with_test_segment_bytes(
        name.into(),
        BoxRepoFactory::fjall_raw(path.to_path_buf()),
        vec![TableConfig::new("t")],
        DurabilityPolicy::default(),
        256,
    )
    .await?;
    repo.disable_background_drainer_for_test();
    Ok(repo)
}

/// Bounded-retry hard reopen (Windows releases the fjall file lock lazily
/// after `drop`); mirrors `tx/tests/recovery_tests.rs::reopen_disk_repo`.
#[cfg(feature = "fjall")]
async fn open_retry(name: &str, path: &std::path::Path) -> RepoInstance {
    let mut last_err = None;
    for _attempt in 0..10 {
        match open_disk(name, path).await {
            Ok(repo) => return repo,
            Err(error) => last_err = Some(error),
        }
    }
    panic!("reopen of {name:?} failed after retries: {:?}", last_err);
}

fn filler_event(version: u64) -> Arc<ChangelogEvent> {
    Arc::new(ChangelogEvent {
        repo: "filler".into(),
        commit_version: version,
        tx_id: 0,
        actor: Actor::System,
        timestamp_ns: 0,
        changes: vec![],
    })
}

/// Park the journal writer inside its first put and fill the channel to
/// capacity: every subsequent `journal_enqueue` deterministically hits Full.
async fn park_writer(feed: &RepoChangefeed, store: &Arc<ParkStore>) {
    store.block_filler_once.store(true, Ordering::Release);
    feed.journal_enqueue(&filler_event(MAX_KEY));
    store.filler_entered.notified().await;
    let queued = filler_event(BACKLOG_KEY);
    for _ in 0..JOURNAL_CHANNEL_CAPACITY {
        feed.journal_enqueue(&queued);
    }
}

async fn commit_row(repo: &RepoInstance, table: &crate::table::TableManager, tag: &str) -> u64 {
    let (mut tx, _guard) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    table
        .insert_tx(&InnerValue::Str(tag.into()), Some(&mut tx))
        .await
        .unwrap();
    repo.commit_tx(tx).await.unwrap().commit_version
}

/// Hang guard for the condition waits below; never the expected path.
const WAIT_GUARD: Duration = Duration::from_secs(60);
const WAIT_POLL: Duration = Duration::from_millis(1);

/// Wait until the journal writer has persisted at least `through`. The
/// writer persists on a blocking thread, so the bound is wall-clock time:
/// a count of scheduler yields can run out before the I/O completes.
async fn wait_persisted(feed: &RepoChangefeed, through: u64) {
    let reached = tokio::time::timeout(WAIT_GUARD, async {
        while feed.last_persisted_version() < through {
            tokio::time::sleep(WAIT_POLL).await;
        }
    })
    .await;
    assert!(
        reached.is_ok(),
        "journal writer never persisted through {through}"
    );
}

/// Wait until the event at `version` is readable from the journal store.
async fn wait_event_stored(store: &Arc<ParkStore>, version: u64) {
    let stored = tokio::time::timeout(WAIT_GUARD, async {
        loop {
            let rows = store.range_from(version_key(version), 1).await.unwrap();
            if let Some(bytes) = rows.first() {
                if let Ok(event) = rmp_serde::from_slice::<ChangelogEvent>(bytes) {
                    if event.commit_version == version {
                        return;
                    }
                }
            }
            tokio::time::sleep(WAIT_POLL).await;
        }
    })
    .await;
    assert!(
        stored.is_ok(),
        "event at {version} never reached the journal store"
    );
}

/// Drain passes until every repair-pending version is retired.
async fn drain_until_empty(repo: &RepoInstance, feed: &RepoChangefeed) {
    for _ in 0..100 {
        repo.drainer().drain_all(&repo.core).await.unwrap();
        if feed.repair_pending_depth() == 0 {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!(
        "repair-pending set never emptied (depth {})",
        feed.repair_pending_depth()
    );
}

/// Direct repair passes (NO truncation, unlike `drain_all`) until every
/// repair-pending version is retired. One pass admits only as many versions
/// as the journal channel has room for; the rest stay pending for the next
/// pass, so a test that expects several versions must loop.
#[cfg(feature = "fjall")]
async fn repair_until_empty(repo: &RepoInstance, feed: &RepoChangefeed) {
    for _ in 0..100 {
        drain_repair_pending(&repo.core).await.unwrap();
        if feed.repair_pending_depth() == 0 {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!(
        "repair-pending set never emptied (depth {})",
        feed.repair_pending_depth()
    );
}

// (a) Channel-full commits are deferred WITHOUT a gap and the repair drainer
// lands them in ascending version order; a follower pull sees every event
// exactly once, contiguous, `gap == None`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn channel_full_defers_and_repairs_in_order() {
    let (repo, feed, store) = fixture("rp_full_channel").await;
    let table = repo.get_table("a").await.unwrap();
    park_writer(&feed, &store).await;
    let base = repo.current_commit_version().await.unwrap();
    let mut versions = Vec::new();
    for index in 0..5 {
        versions.push(commit_row(&repo, &table, &format!("row-{index}")).await);
    }
    // NO gap: channel-full defers, it does not drop.
    assert_eq!(feed.first_gap_version(), 0);
    assert_eq!(feed.repair_pending_depth(), 5);
    assert_eq!(feed.repair_pending().channel_full_total(), 5);

    store.release_filler.notify_one();
    wait_persisted(&feed, BACKLOG_KEY).await;
    drain_until_empty(&repo, &feed).await;
    assert_eq!(feed.repair_pending().repaired_total(), 5);
    assert_eq!(feed.first_gap_version(), 0);

    wait_event_stored(&store, versions[4]).await;
    let (events, gap) = repo
        .read_changelog_for_replication(base + 1, 64)
        .await
        .unwrap();
    assert_eq!(gap, None);
    let real: Vec<u64> = events
        .iter()
        .map(|event| event.event.commit_version)
        .filter(|version| *version <= versions[4])
        .collect();
    assert_eq!(
        real,
        (base + 1..=versions[4]).collect::<Vec<u64>>(),
        "pull must return every deferred event exactly once, ascending"
    );
    feed.shutdown_and_wait().await.unwrap();
}

// (b) Retention gate: the WAL frames holding a repair-pending version stay
// retained (clean horizon does not pass the hole) until the repaired event
// is checkpointed — and the negative control proves the SETTLE_AND_TRUNCATE
// ceiling clamp (the production truncation path's shape) is load-bearing:
// without it the truncation destroys the pending evidence. The seam
// (`test_set_clamp_disabled`) lifts BOTH the barrier clamp and the ceiling
// clamp. Mem-sink caveat: this exercises the ceiling mechanism on the Mem
// WAL; the PERSISTENT checkpoint interplay is covered by the crash-matrix
// suite. Runs on the IN-MEMORY fixture: the Mem WAL sink supports
// `has_truncatable`/`truncate_below`/`replay` with per-frame versioning, so
// the clamp story is fully exercisable without a disk repo (whose cold
// reconciliation would initialise the feed before `install_journal_for_test`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retention_gate_holds_segment_until_checkpointed() {
    let (repo, feed, store) = fixture("rp_retention").await;
    let table = repo.get_table("a").await.unwrap();
    // Baseline commits BEFORE parking the writer: the channel is empty, so
    // these events are admitted and persisted normally.
    for index in 0..4 {
        let seed_v = commit_row(&repo, &table, &format!("seed-{index}")).await;
        wait_persisted(&feed, seed_v).await;
    }

    park_writer(&feed, &store).await;

    let v = commit_row(&repo, &table, "held").await;
    assert_eq!(feed.repair_pending_depth(), 1);
    assert_eq!(feed.first_gap_version(), 0);
    let mut last = v;
    for index in 0..6 {
        last = commit_row(&repo, &table, &format!("seal-{index}")).await;
    }
    assert_eq!(feed.repair_pending_depth(), 7);

    let group = repo.wal_group().await.unwrap();

    // Clamped pass: the ceiling may not cross the still-pending version.
    repo.drainer().drain_all(&repo.core).await.unwrap();
    assert!(
        group.has_truncatable(last),
        "frames at or below the seal commits' watermark must be truncatable \
         but for the clamp — otherwise this test proves nothing"
    );
    assert!(
        group
            .replay()
            .await
            .unwrap()
            .iter()
            .any(|entry| entry.commit_version == v),
        "the clamped ceiling must retain the WAL entry of the pending version"
    );
    // On the Mem sink the clean horizon stays at 0 (no durable history
    // flush watermark) — the meaningful assertion is `<= v - 1`.
    assert!(
        feed.clean_horizon() < v,
        "the clean horizon must not pass the pending hole"
    );

    // NEGATIVE CONTROL: disable the clamps (barrier AND truncation ceiling).
    // On the Mem WAL the clean-horizon checkpoint succeeds without the seam
    // bypass (the parked writer is not an obstacle there), so no `test_seam`
    // is needed — and the same truncation now crosses the pending version,
    // destroying the WAL evidence of a version that was never repaired.
    feed.repair_pending().test_set_clamp_disabled();
    let mut gone = false;
    for _ in 0..500 {
        repo.drainer().drain_all(&repo.core).await.unwrap();
        gone = !group
            .replay()
            .await
            .unwrap()
            .iter()
            .any(|entry| entry.commit_version == v);
        if gone {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(
        gone,
        "without the clamp the truncation crosses the still-pending version"
    );
    assert_eq!(
        feed.repair_pending_depth(),
        7,
        "the pending versions were never repaired — the unclamped truncation \
         destroyed their only re-derivation source"
    );

    store.release_filler.notify_one();
    wait_persisted(&feed, BACKLOG_KEY).await;
    feed.shutdown_and_wait().await.unwrap();
}

// (c) The post-crash state of a deferred event — journal event lost, WAL
// entry retained, version marked repair-pending — is rebuilt across a
// restart: the cold reconciliation at open re-derives the event from the
// retained WAL; no gap, fresh (empty) in-memory pending set.
/// Disk-backed (fjall_raw) — requires the `fjall` feature.
#[cfg(feature = "fjall")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unclean_restart_rebuilds_lost_deferred_event() {
    let temp = tempfile::TempDir::new().unwrap();
    let path = temp.path().join("rp_restart");
    // Scoped setup: every fjall `Database` handle (repo, table, changelog
    // store) must be DROPPED before the reopen loop — fjall releases its
    // LOCK_FILE only when the last `Database` handle drops, so a leaked
    // handle makes every `open_retry` attempt fail with `Locked`.
    let v = {
        let repo = open_disk("rp_restart", &path).await.unwrap();
        let table = repo.get_table("t").await.unwrap();
        let v = commit_row(&repo, &table, "deferred").await;
        // The real feed persisted the event; wait for it deterministically.
        {
            let feed = repo.changefeed().await.unwrap().feed.clone();
            wait_persisted(&feed, v).await;
            feed.shutdown_and_wait().await.unwrap();
        }

        // Simulate the exact crash state of a channel-full deferral:
        //  - the journal event is lost (not yet persisted when the crash hit),
        //  - the WAL entry is retained,
        //  - the version was recorded repair-pending (in-memory state, lost).
        let store = repo.changelog_store_for_cold_reconcile().await.unwrap();
        shamir_tx::changefeed::cold::delete_event(store.as_ref(), v)
            .await
            .unwrap();
        repo.changefeed()
            .await
            .unwrap()
            .feed
            .repair_pending()
            .record_channel_full(v);
        drop(store);
        drop(table);
        drop(repo); // last `Database` handle released here → LOCK_FILE unlocked
        v
    };

    for round in 0..2 {
        let repo = open_retry("rp_restart", &path).await;
        repo.disable_background_drainer_for_test();
        // NO manual drain: the cold reconciliation at open must already have
        // rebuilt the event from the retained WAL.
        let feed = repo.changefeed().await.unwrap().feed;
        assert_eq!(
            feed.repair_pending_depth(),
            0,
            "round {round}: the pending set is fresh in-memory state"
        );
        assert_eq!(feed.first_gap_version(), 0, "round {round}");
        let (events, gap) = repo.read_changelog_for_replication(v, 8).await.unwrap();
        assert_eq!(gap, None, "round {round}");
        assert!(
            events.iter().any(|event| event.event.commit_version == v),
            "round {round}: cold reconciliation must rebuild the lost event \
             from the retained WAL"
        );
        feed.shutdown_and_wait().await.unwrap();
        drop(repo);
    }
}

// (d) A pending version whose WAL entry is legacy (pre-header v2 wire
// format, event underivable) declares the minimal gap and retires.
#[derive(serde::Serialize)]
struct LegacyV2Shape {
    txn_id: u64,
    repo_id_interned: u64,
    started_at_ns: u64,
    commit_version: u64,
    ops: Vec<shamir_wal::WalOpV2>,
    interner_delta: Vec<(u64, String, u64)>,
}

#[tokio::test]
async fn legacy_entry_declares_minimal_gap() {
    let (repo, feed, _store) = fixture("rp_legacy").await;
    let v = repo.current_commit_version().await.unwrap() + 1;
    let body = LegacyV2Shape {
        txn_id: 77,
        repo_id_interned: 0,
        started_at_ns: 0,
        commit_version: v,
        ops: vec![],
        interner_delta: vec![],
    };
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&shamir_wal::WAL_V2_MAGIC);
    bytes.push(2); // pre-event format version
    bincode::serialize_into(&mut bytes, &body).unwrap();
    repo.wal_group()
        .await
        .unwrap()
        .append(bytes, v, shamir_wal::WalDurability::Buffered)
        .await
        .unwrap();
    feed.repair_pending().record_channel_full(v);

    repo.drainer().drain_all(&repo.core).await.unwrap();
    assert_eq!(
        feed.first_gap_version(),
        v,
        "legacy evidence declares a gap"
    );
    assert_eq!(feed.repair_pending_depth(), 0);
    assert_eq!(feed.repair_pending().repair_mismatch_total(), 0);
    feed.shutdown_and_wait().await.unwrap();
}

// (e) Repair-pass idempotence: running the repair drainer over the SAME
// pending version repeatedly while the channel is full never puts early,
// and once admitted, a further pass probing an already-present, equal
// journal event retires the version WITHOUT a second store put. Direct
// `drain_repair_pending` calls never truncate, so the WAL evidence stays
// retained for the whole sequence — `drain_all` (which truncates) is never
// invoked; the writer's channel admission is exercised directly.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn double_repair_produces_one_put() {
    let (repo, feed, store) = fixture("rp_double").await;
    let table = repo.get_table("a").await.unwrap();
    park_writer(&feed, &store).await;
    let v = commit_row(&repo, &table, "once").await;
    assert_eq!(feed.repair_pending_depth(), 1);
    assert_eq!(put_count(&store, v), 0, "no put of v before the repair");

    // Two repair passes over the SAME pending version while the writer is
    // still parked: `journal_enqueue_tracked` hits Full both times, so the
    // version stays pending with NO premature put — this is the
    // "repaired twice produces one put" proof.
    drain_repair_pending(&repo.core).await.unwrap();
    drain_repair_pending(&repo.core).await.unwrap();
    assert_eq!(feed.repair_pending_depth(), 1);
    assert_eq!(put_count(&store, v), 0, "no put while the channel is full");
    assert_eq!(feed.first_gap_version(), 0);

    // Release the writer: the channel drains, then a THIRD direct repair
    // pass (same pending window — the version never left the set) admits v
    // through the now-empty channel → exactly one put.
    store.release_filler.notify_one();
    wait_persisted(&feed, BACKLOG_KEY).await;
    drain_repair_pending(&repo.core).await.unwrap();
    wait_event_stored(&store, v).await;
    assert_eq!(feed.repair_pending_depth(), 0);
    assert_eq!(
        put_count(&store, v),
        1,
        "the repair must produce exactly one put of the repaired version"
    );

    // Final probe-idempotence pass with v's WAL frame STILL retained (no
    // `drain_all` ran → no `settle_and_truncate` → no truncation): the store
    // probe finds the event present and equal — retire, never re-put.
    feed.repair_pending().record_channel_full(v);
    drain_repair_pending(&repo.core).await.unwrap();
    assert_eq!(feed.repair_pending_depth(), 0);
    assert_eq!(put_count(&store, v), 1, "the probe pass must not re-put");
    assert_eq!(feed.repair_pending().repair_mismatch_total(), 0);
    assert_eq!(feed.first_gap_version(), 0);
    assert_eq!(
        feed.repair_pending().repaired_total(),
        1,
        "only the admission pass earns a repair credit"
    );
    feed.shutdown_and_wait().await.unwrap();
}

// (f) A journal slot holding a DIFFERENT event than the WAL-derived one is a
// loud repair-mismatch: counted, never overwritten, never a gap.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mismatching_event_is_loud_error() {
    let (repo, feed, _store) = fixture("rp_mismatch").await;
    let table = repo.get_table("a").await.unwrap();
    let v = commit_row(&repo, &table, "truth").await;
    wait_persisted(&feed, v).await;

    let alien = Arc::new(ChangelogEvent {
        repo: "alien".into(),
        commit_version: v,
        tx_id: 42,
        actor: Actor::System,
        timestamp_ns: 7,
        changes: vec![],
    });
    let encoded = encode_event(&alien).unwrap();
    let handle = repo.changefeed().await.unwrap();
    handle
        .store
        .put(version_key(v), encoded.clone())
        .await
        .unwrap();

    feed.repair_pending().record_channel_full(v);
    repo.drainer().drain_all(&repo.core).await.unwrap();

    assert_eq!(feed.repair_pending().repair_mismatch_total(), 1);
    assert_eq!(feed.repair_pending_depth(), 0);
    assert_eq!(
        feed.first_gap_version(),
        0,
        "the journal HAS an event — no gap"
    );
    let read_back = handle.store.range_from(version_key(v), 1).await.unwrap();
    assert_eq!(read_back.first(), Some(&encoded), "never overwrite");
    feed.shutdown_and_wait().await.unwrap();
}

// (g) Interleaved normal commits + repaired deferrals produce one contiguous
// ascending version sequence; during the pending window a bounded pull sees
// nothing at or above the hole (barrier/ceiling clamp).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interleaved_normal_commits_keep_order() {
    let (repo, feed, store) = fixture("rp_interleaved").await;
    let table = repo.get_table("a").await.unwrap();
    park_writer(&feed, &store).await;
    let base = repo.current_commit_version().await.unwrap();
    let mut pending_v = Vec::new();
    for index in 0..5 {
        pending_v.push(commit_row(&repo, &table, &format!("p-{index}")).await);
    }
    let min_pending = feed.repair_pending_min().unwrap();
    assert_eq!(min_pending, pending_v[0]);

    let handle = repo.changefeed().await.unwrap();
    let bounded = handle
        .feed
        .read_from_bounded(&handle.store, base + 1, 100, min_pending - 1)
        .await;
    assert_eq!(bounded.gap_at, None);
    assert!(
        bounded
            .events
            .iter()
            .all(|event| event.commit_version < min_pending),
        "a bounded pull during the pending window must not cross the hole"
    );

    store.release_filler.notify_one();
    wait_persisted(&feed, BACKLOG_KEY).await;
    let mut normal = Vec::new();
    for index in 0..2 {
        normal.push(commit_row(&repo, &table, &format!("n-{index}")).await);
    }
    drain_until_empty(&repo, &feed).await;
    wait_event_stored(&store, normal[1]).await;

    let (events, gap) = repo
        .read_changelog_for_replication(base + 1, 100)
        .await
        .unwrap();
    assert_eq!(gap, None);
    let top = normal[1];
    let got: Vec<u64> = events
        .iter()
        .map(|event| event.event.commit_version)
        .filter(|version| *version <= top)
        .collect();
    let mut expected = pending_v;
    expected.extend_from_slice(&normal);
    assert_eq!(
        got, expected,
        "repaired deferrals + normal commits must form one contiguous \
         ascending sequence"
    );
    feed.shutdown_and_wait().await.unwrap();
}

// (h) PERSISTENT retention gate (fjall repo + real file WAL + production
// checkpoint path): the clamp holds the truncation below the still-pending
// version until the repaired event is durably checkpointed, and the lossy
// unclamped outcome is certified as a durable GAP across restart. The
// ParkStore's backing journal is IN-MEMORY BY DESIGN: J / event artifacts
// written through it are ephemeral, so the reopen rounds below exercise the
// cold WAL→journal rebuild against the repo's REAL changelog store.
/// Disk-backed (fjall_raw) — requires the `fjall` feature.
#[cfg(feature = "fjall")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persistent_retention_gate_holds_until_checkpointed_reopens() {
    let temp = tempfile::TempDir::new().unwrap();
    let path = temp.path().join("rp_persist_retention");
    let v = {
        let store = park_store();
        // Seam BEFORE open: a disk repo's cold reconciliation lazily
        // initialises its own feed, so `install_journal_for_test` (which
        // asserts the OnceCell is unset) cannot be used post-open.
        let seam = journal_handle(&store);
        super::test_journal_seam::install("rp_persist_retention".into(), seam.clone());
        let repo = open_disk("rp_persist_retention", &path).await.unwrap();
        let live = repo.changefeed().await.unwrap();
        // Sanity: the SEAM feed is live — if the seam had failed, cold
        // reconciliation would have initialised its own feed and the park
        // below would never produce a pending version on it.
        assert!(Arc::ptr_eq(&live.feed, &seam.feed));
        assert_eq!(live.feed.repair_pending_depth(), 0);
        let feed = live.feed.clone();
        let table = repo.get_table("t").await.unwrap();
        park_writer(&feed, &store).await;
        let v = commit_row(&repo, &table, "held").await;
        assert_eq!(feed.repair_pending_depth(), 1);
        assert_eq!(feed.first_gap_version(), 0);
        let mut last = v;
        for index in 0..4 {
            last = commit_row(&repo, &table, &format!("seal-{index}")).await;
        }
        assert_eq!(feed.repair_pending_depth(), 5);

        let group = repo.wal_group().await.unwrap();
        // A5 interner gate first (crash-matrix precedent): persist the
        // interner so its delta cannot pin the truncation ceiling. Cheap
        // no-op when the rows carry no deltas.
        repo.repo_interner().await.unwrap().persist().await.unwrap();

        // Clamp load-bearing proof, captured BEFORE the pass (256-byte
        // segments seal well below `last`).
        assert!(
            group.has_truncatable(last),
            "sealed segments at or below the seal watermark must be \
             truncatable but for the clamp — otherwise this test proves nothing"
        );

        // Clamped pass. On the FILE sink `has_truncatable(v - 1)` is TRUE
        // (segments seal below the ceiling), so the un-bypassed checkpoint
        // inside `drain_all` would block its `record_clean_horizon` send
        // behind the PARKED writer (full bounded channel) — a guaranteed
        // hang. The checkpoint bypass isolates the actual subject, the
        // repair-pending CEILING clamp: truncation still runs, but must
        // stop below the pending version.
        test_seam::with_state(&repo.core, |state| state.bypass_checkpoint = true);
        repo.drainer().drain_all(&repo.core).await.unwrap();
        test_seam::with_state(&repo.core, |state| state.bypass_checkpoint = false);
        assert!(
            group
                .replay()
                .await
                .unwrap()
                .iter()
                .any(|entry| entry.commit_version == v),
            "the clamped ceiling must retain the WAL entry of the pending version"
        );
        assert!(
            feed.clean_horizon() < v,
            "the clean horizon must not pass the pending hole"
        );
        assert_eq!(feed.first_gap_version(), 0);

        // Release: the channel drains and the direct repair passes admit ALL
        // five pending versions (v + 4 seals) — exactly one put each. A
        // single pass is not enough: `wait_persisted` returns as soon as the
        // first filler is persisted, while most of the backlog is still
        // queued, so a pass only admits as many versions as there are free
        // channel slots at that moment.
        store.release_filler.notify_one();
        wait_persisted(&feed, BACKLOG_KEY).await;
        repair_until_empty(&repo, &feed).await;
        assert_eq!(feed.repair_pending_depth(), 0);
        wait_event_stored(&store, v).await;
        assert_eq!(
            put_count(&store, v),
            1,
            "the repair must produce exactly one put of the repaired version"
        );
        assert_eq!(feed.repair_pending().repaired_total(), 5);
        // NO `drain_all` here: it would truncate v's WAL segment while the
        // repaired events live only in the ephemeral ParkStore journal —
        // the "until checkpointed" release is observed via a DIRECT
        // checkpoint instead (the channel now has room, so
        // `record_clean_horizon` is admitted).
        assert!(checkpoint_before_truncate(&repo.core).await);
        assert!(
            feed.clean_horizon() >= v,
            "the checkpoint must lift the clean horizon past the repaired version"
        );

        // Shutdown, then drop EVERY store handle (fjall releases its
        // LOCK_FILE only when the last `Database` handle drops).
        feed.shutdown_and_wait().await.unwrap();
        drop(group);
        drop(table);
        drop(store);
        drop(repo);
        v
    };

    for round in 0..2 {
        let repo = open_retry("rp_persist_retention", &path).await;
        repo.disable_background_drainer_for_test();
        // Mechanism: on reopen the REAL journal is empty (j_real = 0 —
        // every event went through the ephemeral ParkStore), so h > j and
        // the cold reconciliation rebuilds v's event from the INTACT WAL
        // (no step above truncated at or above v) via `put_event_if_missing`.
        let feed = repo.changefeed().await.unwrap().feed;
        let (events, gap) = repo.read_changelog_for_replication(v, 64).await.unwrap();
        assert_eq!(gap, None, "round {round}");
        assert_eq!(
            events
                .iter()
                .filter(|event| event.event.commit_version == v)
                .count(),
            1,
            "round {round}: the rebuilt event must appear exactly once"
        );
        assert_eq!(feed.first_gap_version(), 0, "round {round}");
        feed.shutdown_and_wait().await.unwrap();
        drop(repo);
    }
}

// (i) PERSISTENT negative control: with no repair-pending clamp left to
// hold, the truncation destroys the pending version's WAL evidence and the
// loss is certified as a DURABLE GAP at restart. HONEST CLASSIFICATION:
// with a dead writer the J-checkpoint itself fails closed (the
// `record_clean_horizon` send hits the closed channel), so the clamp is not
// the only guard — the clamp-isolation negative control remains the
// in-memory test (b). This persistent control proves the END-TO-END LOSS
// outcome the brief demands: evidence destroyed + durable gap after restart.
/// Disk-backed (fjall_raw) — requires the `fjall` feature.
#[cfg(feature = "fjall")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persistent_negative_control_unclamped_truncation_yields_gap() {
    let temp = tempfile::TempDir::new().unwrap();
    let path = temp.path().join("rp_persist_negative");
    let v = {
        let store = park_store();
        let seam = journal_handle(&store);
        super::test_journal_seam::install("rp_persist_negative".into(), seam.clone());
        let repo = open_disk("rp_persist_negative", &path).await.unwrap();
        let live = repo.changefeed().await.unwrap();
        assert!(Arc::ptr_eq(&live.feed, &seam.feed));
        let feed = live.feed.clone();
        let table = repo.get_table("t").await.unwrap();
        park_writer(&feed, &store).await;
        let v = commit_row(&repo, &table, "lost").await;
        assert_eq!(feed.repair_pending_depth(), 1);
        for index in 0..2 {
            commit_row(&repo, &table, &format!("seal-{index}")).await;
        }
        assert_eq!(feed.repair_pending_depth(), 3);
        repo.repo_interner().await.unwrap().persist().await.unwrap();

        // Release + drain the PARKED writer first: a writer parked inside
        // the filler put can never observe the shutdown flag, so
        // `shutdown_and_wait` would block forever. The pending versions
        // were never ENQUEUED (channel full), so they are NOT persisted —
        // the ParkStore journal lacks them, exactly the dead-writer state
        // the Closed arm consumes.
        store.release_filler.notify_one();
        wait_persisted(&feed, BACKLOG_KEY).await;
        feed.shutdown_and_wait().await.unwrap();
        assert_eq!(feed.repair_pending_depth(), 3);

        // Dead writer: the repair cannot admit v — `journal_enqueue_tracked`
        // hits Closed → `mark_gap(v)` + in-band retire — so the ceiling
        // clamp has nothing left to hold. Bypass the checkpoint (which
        // would now fail closed on the closed channel — the second guard)
        // exactly like `checkpoint_seam_tests.rs`, to reach the unclamped
        // truncation and prove the end-to-end loss.
        test_seam::with_state(&repo.core, |state| state.bypass_checkpoint = true);
        repo.drainer().drain_all(&repo.core).await.unwrap();
        assert_eq!(feed.repair_pending_depth(), 0);
        assert_eq!(
            feed.first_gap_version(),
            v,
            "the Closed arm must have declared the gap in-RAM"
        );
        let group = repo.wal_group().await.unwrap();
        assert!(
            !group
                .replay()
                .await
                .unwrap()
                .iter()
                .any(|entry| entry.commit_version == v),
            "the unclamped truncation must destroy v's WAL segment"
        );
        drop(group);
        drop(table);
        drop(store);
        drop(repo);
        v
    };

    for round in 0..2 {
        let repo = open_retry("rp_persist_negative", &path).await;
        repo.disable_background_drainer_for_test();
        // Round 0: cold reconcile finds no WAL entry and no journal event
        // for v → `missing_intervals` → `record_recovery_gap(v, v)`,
        // durably persisted through the repo's own real feed. Round 1: the
        // persisted gap marker is re-read (`previous_gap`) and the same gap
        // is re-certified.
        let feed = repo.changefeed().await.unwrap().feed;
        let (events, gap) = repo.read_changelog_for_replication(v, 64).await.unwrap();
        assert_eq!(gap, Some(v), "round {round}: the durable gap covers v");
        assert_eq!(
            events
                .iter()
                .filter(|event| event.event.commit_version == v)
                .count(),
            0,
            "round {round}: the destroyed version must have no journal event"
        );
        feed.shutdown_and_wait().await.unwrap();
        drop(repo);
    }
}

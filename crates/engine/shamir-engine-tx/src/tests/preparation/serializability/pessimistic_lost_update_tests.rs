//! A4 — Pessimistic isolation lost-update regression tests.
//!
//! Reproduces the audit finding A4 (HIGH-concurrency):
//!
//! - **(a) 2PL violation — early lock release**: a Pessimistic tx released its
//!   exclusive lock BEFORE the publish step (`apply_data_phase` /
//!   `materialize`) ran, so a second tx could acquire the lock while the
//!   first tx's write was still in-flight (WAL-durable but not yet
//!   cell-published). Fixed by moving `release_pessimistic_locks` to AFTER
//!   the publish step on both commit paths (`commit_tx_inner_legacy_async`
//!   and `commit_tx_lockfree`).
//! - **(b) Snapshot-stale read under a held lock**: even with correct lock
//!   ordering, a Pessimistic tx that acquired a lock and then read via
//!   `get_at(key, snapshot_version)` saw its ORIGINAL snapshot value, not
//!   the latest committed value at the moment the lock was granted. Fixed
//!   by routing the Pessimistic-under-held-lock point-read through
//!   `MvccStore::get_current_bytes` (latest committed) instead of
//!   `get_at(snapshot)`. Snapshot / Serializable read semantics are
//!   unchanged.
//!
//! The combined end-to-end test asserts the FULL lost-update scenario
//! (T1 commits `v1`, T2 reads-under-lock and writes a value computed from
//! the read, both commit) no longer loses T1's write.

use std::sync::Arc;
use std::time::Duration;

use shamir_storage::storage_in_memory::InMemoryRepo;
use shamir_tx::IsolationLevel;
use shamir_types::types::value::InnerValue;

use crate::repo::repo_instance::RepoInstance;
use crate::repo::repo_types::BoxRepo;
use crate::table::TableConfig;

fn make_repo() -> Arc<RepoInstance> {
    let repo = Arc::new(InMemoryRepo::new());
    Arc::new(RepoInstance::new(
        "test".into(),
        BoxRepo::InMemory(repo),
        Vec::new(),
    ))
}

// ---------------------------------------------------------------------------
// (b) Stale-snapshot-under-lock: Pessimistic read under a held lock MUST
//     observe the LATEST COMMITTED value, not the tx's original snapshot.
//
// T1 (Pessimistic) commits k=v1. A second, EARLIER-started Pessimistic tx T2
// (snapshot predates T1's commit) then acquires the lock on k (after T1
// released it) and reads via `read_one_tx`. The read MUST return v1, not the
// stale snapshot value v0.
//
// FAILS before fix (b): T2 sees the stale v0 because `read_one_tx` resolves
// through `get_at(key, snapshot_version)`.
// PASSES after fix (b): T2 sees v1 via `get_current_bytes`.
//
// NOTE: this test only exercises fix (b) — it does NOT depend on fix (a),
// because we wait for T1's `commit_tx` to fully return (which includes the
// publish step) BEFORE T2 reads. So this test passes independently of fix
// (a); it specifically nails fix (b).
// ---------------------------------------------------------------------------
#[tokio::test]
async fn pessimistic_read_under_lock_sees_latest_committed_not_snapshot() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();

    // Seed the record OUTSIDE any tx (v0).
    let rid = tbl.insert(&InnerValue::Str("v0".into())).await.unwrap();

    // T2 begins FIRST — its snapshot will be the pre-T1-commit version.
    // Under the buggy code, T2's `read_one_tx` would resolve against this
    // snapshot, masking T1's later commit.
    let (t2, _g2) = repo.begin_tx(IsolationLevel::Pessimistic).await.unwrap();
    let t2_snapshot = t2.snapshot_version;

    // T1 begins SECOND, writes v1, commits. After commit returns, v1 is
    // fully published and durable — so a correct "latest committed" read
    // under a held lock MUST see v1.
    let (mut t1, _g1) = repo.begin_tx(IsolationLevel::Pessimistic).await.unwrap();
    assert!(
        t1.tx_id.0 > t2.tx_id.0,
        "T1 must be younger than T2 (T2 began first) — wound-wait invariant"
    );
    tbl.update_tx(rid, &InnerValue::Str("v1".into()), Some(&mut t1))
        .await
        .unwrap();
    let t1_outcome = repo.commit_tx(t1).await.unwrap();
    assert!(
        t1_outcome.commit_version > t2_snapshot,
        "T1's commit must have advanced the global version past T2's snapshot"
    );

    // T2 now acquires the Shared lock on `rid` (the audit's "under an
    // exclusive lock" scenario — `read_one_tx` takes a Shared lock and the
    // read itself is the operation under audit). With fix (b) the read
    // returns the LATEST COMMITTED value (v1), not the stale snapshot (v0).
    let read = tokio::time::timeout(Duration::from_secs(3), tbl.read_one_tx(rid, Some(&t2)))
        .await
        .expect("T2 hung acquiring lock on rid — T1's lock was not released")
        .unwrap();

    assert!(
        matches!(read, InnerValue::Str(ref s) if s == "v1"),
        "Pessimistic read under a held lock MUST see the latest committed value \
         (v1, from T1), not the stale snapshot value (v0, from T2's snapshot {}); \
         got {:?}",
        t2_snapshot,
        read
    );

    // Cleanup: release T2's lock.
    let _ = repo.commit_tx(t2).await;
}

// ---------------------------------------------------------------------------
// (a) Lock-ordering: locks stay held UNTIL the write is published.
//
// T1 (Pessimistic) holds an Exclusive lock on `k` and is committing v1.
// T2 (Pessimistic) is concurrently waiting to acquire a lock on `k`. Under
// the FIXED code, by the time T2 actually acquires its lock, T1's v1 MUST
// already be visible via a "latest committed" read — because the lock is
// only released AFTER publish. Under the BUGGY code, the lock was released
// BEFORE publish, so a tight race could observe T2 acquiring the lock
// before T1's write became visible.
//
// We exercise this with a `tokio::sync::Notify`-style barrier: T2 reads
// (acquires the lock + reads via `get_current_bytes`) the INSTANT it
// acquires the lock, then asserts the value is already v1. The fix (a)
// makes this trivially true (lock-release happens-after publish); fix (b)
// makes the read itself correct (latest committed vs snapshot).
//
// FAILS before fix (a) in a tight race (T2 acquires lock before T1's
// publish completes — visible only with instrumentation that injects a
// delay between WAL-begin and publish, since the publish is normally
// synchronous and fast). Without instrumentation we rely on the explicit
// `apply_data_phase`/`materialize` ordering contract being testable via
// the COMMIT-AWAIT semantics + the stale-read fix: after T1's commit_tx
// returns, T2 — having blocked on the lock during T1's commit — must read
// v1 immediately.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn pessimistic_lock_held_until_publish_visible() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();

    // Seed v0.
    let rid = tbl.insert(&InnerValue::Str("v0".into())).await.unwrap();

    // T1 begins FIRST (older, wins wound-wait) and immediately acquires an
    // Exclusive lock on rid by writing v1.
    let (mut t1, _g1) = repo.begin_tx(IsolationLevel::Pessimistic).await.unwrap();
    tbl.update_tx(rid, &InnerValue::Str("v1".into()), Some(&mut t1))
        .await
        .unwrap();

    // T2 begins SECOND (younger). It will try to acquire a Shared lock on
    // the same key — this MUST block until T1's commit completes the
    // publish step (because fix (a) keeps the lock held that long).
    let (t2, _g2) = repo.begin_tx(IsolationLevel::Pessimistic).await.unwrap();
    assert!(t2.tx_id.0 > t1.tx_id.0, "T2 must be younger");

    // Spawn T2's read: it parks on T1's Exclusive lock until T1 releases it.
    let tbl2 = tbl.clone();
    let t2_read = tokio::spawn(async move { tbl2.read_one_tx(rid, Some(&t2)).await });

    // Yield once so T2 actually gets to its lock-acquire await point before
    // we let T1 commit. This tightens the race: T2 is genuinely parked on
    // the lock when T1's commit begins.
    tokio::task::yield_now().await;

    // T1 commits. The publish step (`materialize` / `apply_data_phase`)
    // runs BEFORE `release_pessimistic_locks` under the FIXED code, so by
    // the time T2's `read_one_tx` returns, v1 MUST be visible.
    let t1_outcome = repo.commit_tx(t1).await.unwrap();
    assert!(t1_outcome.commit_version > t1_outcome.snapshot_version);

    // T2's read resolves. Bounded so a regression (T2 never wakes / hangs)
    // FAILS instead of hanging CI.
    let read = tokio::time::timeout(Duration::from_secs(3), t2_read)
        .await
        .expect("T2 never acquired the lock — T1's release_pessimistic_locks did not run")
        .expect("T2 read task panicked")
        .unwrap();

    assert!(
        matches!(read, InnerValue::Str(ref s) if s == "v1"),
        "After T1 released its lock (post-publish), T2's read MUST see v1; got {:?}",
        read
    );
}

// ---------------------------------------------------------------------------
// Combined end-to-end: the full lost-update scenario from the audit.
//
// T1 commits k=v1. T2 (earlier snapshot) reads-under-lock and computes a
// value from what it read, then commits. Under the BUGGY code T2 saw v0
// (stale) and overwrote v1 with v2-from-v0 → LOST UPDATE.
//
// Under the FIXED code T2 sees v1 (latest committed) and so its computed
// value reflects T1's write. We model the "computation" as concatenation:
// T2's value is `format!("based-on-{read}")`. If `read == v1`, the final
// committed value is `based-on-v1` (no lost update); if `read == v0`, it
// is `based-on-v0` (lost update).
//
// We assert the final committed value reflects v1 (T1's write).
// ---------------------------------------------------------------------------
#[tokio::test]
async fn pessimistic_lost_update_end_to_end_no_data_loss() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();

    // Seed v0.
    let rid = tbl.insert(&InnerValue::Str("v0".into())).await.unwrap();

    // T2 starts FIRST (snapshot predates T1's commit).
    let (mut t2, _g2) = repo.begin_tx(IsolationLevel::Pessimistic).await.unwrap();
    let t2_snapshot = t2.snapshot_version;

    // T1 starts SECOND, writes v1, commits — its write advances the global
    // version past t2_snapshot.
    let (mut t1, _g1) = repo.begin_tx(IsolationLevel::Pessimistic).await.unwrap();
    assert!(t1.tx_id.0 > t2.tx_id.0);
    tbl.update_tx(rid, &InnerValue::Str("v1".into()), Some(&mut t1))
        .await
        .unwrap();
    let t1_outcome = repo.commit_tx(t1).await.unwrap();
    assert!(t1_outcome.commit_version > t2_snapshot);

    // T2 reads under a held lock, computes its write FROM the read, commits.
    let read = tbl.read_one_tx(rid, Some(&t2)).await.unwrap();
    let computed = match read {
        InnerValue::Str(s) => format!("based-on-{s}"),
        other => format!("based-on-{other:?}"),
    };
    tbl.update_tx(rid, &InnerValue::Str(computed), Some(&mut t2))
        .await
        .unwrap();
    repo.commit_tx(t2).await.unwrap();

    // Final committed value: a fresh reader MUST see T2's write computed
    // FROM T1's v1 — NOT from the stale v0.
    let final_val = tbl.get(rid).await.unwrap();
    assert!(
        matches!(final_val, InnerValue::Str(ref s) if s == "based-on-v1"),
        "Lost update: T2's write must be computed from T1's committed v1, \
         not the stale snapshot v0; final committed value was {:?}",
        final_val
    );
}

// ---------------------------------------------------------------------------
// Regression guard: Snapshot / Serializable isolation MUST be unaffected by
// the Pessimistic-specific read-path change — they continue to resolve via
// `get_at(key, snapshot_version)` (a deliberate snapshot-stale read is the
// POINT of Snapshot isolation). We construct a scenario where a Snapshot tx
// reads a key after a newer commit, and assert it sees its SNAPSHOT value,
// NOT the latest committed. This guards against an accidental over-broad
// change to the read path.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn snapshot_read_isolation_unchanged_sees_snapshot_not_latest() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();

    let rid = tbl.insert(&InnerValue::Str("v0".into())).await.unwrap();

    // Snapshot tx begins BEFORE the v1 commit.
    let (snap_tx, _g) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let snap_snapshot = snap_tx.snapshot_version;

    // A second tx commits v1.
    let (mut t1, _g1) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    tbl.update_tx(rid, &InnerValue::Str("v1".into()), Some(&mut t1))
        .await
        .unwrap();
    let t1_outcome = repo.commit_tx(t1).await.unwrap();
    assert!(t1_outcome.commit_version > snap_snapshot);

    // The Snapshot tx's read MUST return v0 (its snapshot), NOT v1. This is
    // the defining Snapshot-isolation invariant and must be preserved by
    // the Pessimistic-only read-path change.
    let read = tbl.read_one_tx(rid, Some(&snap_tx)).await.unwrap();
    assert!(
        matches!(read, InnerValue::Str(ref s) if s == "v0"),
        "Snapshot isolation MUST see its snapshot value (v0), not the latest \
         committed (v1); the Pessimistic-only read-path change leaked into \
         Snapshot isolation; got {:?}",
        read
    );
}

// ---------------------------------------------------------------------------
// Same regression guard for Serializable isolation.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn serializable_read_isolation_unchanged_sees_snapshot_not_latest() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();

    let rid = tbl.insert(&InnerValue::Str("v0".into())).await.unwrap();

    let (ser_tx, _g) = repo.begin_tx(IsolationLevel::Serializable).await.unwrap();
    let ser_snapshot = ser_tx.snapshot_version;

    let (mut t1, _g1) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    tbl.update_tx(rid, &InnerValue::Str("v1".into()), Some(&mut t1))
        .await
        .unwrap();
    let t1_outcome = repo.commit_tx(t1).await.unwrap();
    assert!(t1_outcome.commit_version > ser_snapshot);

    let read = tbl.read_one_tx(rid, Some(&ser_tx)).await.unwrap();
    assert!(
        matches!(read, InnerValue::Str(ref s) if s == "v0"),
        "Serializable isolation MUST see its snapshot value (v0), not the \
         latest committed (v1); the Pessimistic-only read-path change leaked \
         into Serializable isolation; got {:?}",
        read
    );
}

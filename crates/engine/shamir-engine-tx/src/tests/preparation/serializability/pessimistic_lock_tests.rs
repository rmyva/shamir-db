//! S2 — Level-3 pessimistic locking (wound-wait) engine-level integration
//! tests.
//!
//! These exercise the REAL end-to-end path through `TableManager`'s
//! tx-aware methods (`insert_tx` / `update_tx` / `read_one_tx`) and the
//! commit/abort pipeline — NOT a unit re-test of `MvccStore::lock_key`.
//! The `TableManager` methods gate on `IsolationLevel::Pessimistic`,
//! acquire per-key locks via `MvccStore::lock_key`, and the commit path
//! releases them via `release_pessimistic_locks`.
//!
//! Two invariants are proven here:
//!
//! 1. **Deadlock-freedom (wound-wait)**: two Level-3 txs acquiring two
//!    keys in OPPOSITE orders concurrently must BOTH terminate (one aborts
//!    via wound, the other commits). The whole scenario is bounded by a
//!    3-second `tokio::time::timeout`, so a real deadlock FAILS the test
//!    instead of hanging CI.
//! 2. **Lock released on commit**: after a Level-3 tx commits, a second
//!    Level-3 tx can acquire the same key without blocking — proving the
//!    `release_pessimistic_locks` path on the commit tail actually frees
//!    the locks (a stuck lock would make the second tx hang).

use std::sync::Arc;
use std::time::Duration;

use shamir_storage::storage_in_memory::InMemoryRepo;
use shamir_tx::IsolationLevel;
use shamir_types::types::value::InnerValue;

use crate::repo::repo_instance::RepoInstance;
use crate::repo::repo_types::BoxRepo;
use crate::table::TableConfig;
use crate::tx::CommitError;

fn make_repo() -> Arc<RepoInstance> {
    let repo = Arc::new(InMemoryRepo::new());
    Arc::new(RepoInstance::new(
        "test".into(),
        BoxRepo::InMemory(repo),
        Vec::new(),
    ))
}

/// Two Level-3 txs acquire two keys in OPPOSITE orders concurrently.
///
/// tx1 (older, higher priority under wound-wait) updates A then B.
/// tx2 (younger, lower priority) updates B then A.
///
/// The classic two-phase-locking deadlock scenario — but wound-wait breaks
/// the cycle: tx2 can only ever WAIT on strictly-older holders, so when tx1
/// (older) requests a key tx2 (younger) holds, tx2 is WOUNDED and must
/// abort. Both txs therefore terminate (one commits, one aborts with
/// `Wounded`). The whole scenario is bounded by a 3s timeout so a real
/// deadlock (regression in the wounder / `enable()` wake path) FAILS
/// instead of hanging CI.
#[tokio::test]
async fn pessimistic_deadlock_freedom_wound_wait_terminates() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();

    // Commit two records OUTSIDE any tx so both Level-3 txs contend on the
    // SAME existing rids via update_tx (insert_tx mints fresh rids and
    // would never conflict).
    let rid_a = tbl.insert(&InnerValue::Str("a".into())).await.unwrap();
    let rid_b = tbl.insert(&InnerValue::Str("b".into())).await.unwrap();

    // tx1 started FIRST → older → smaller tx_id → wins wound-wait.
    let (mut tx1, _g1) = repo.begin_tx(IsolationLevel::Pessimistic).await.unwrap();
    // tx2 started SECOND → younger → loses wound-wait (gets wounded).
    let (mut tx2, _g2) = repo.begin_tx(IsolationLevel::Pessimistic).await.unwrap();

    assert!(
        tx1.tx_id.0 < tx2.tx_id.0,
        "tx1 must be older (smaller tx_id) to be the wound-wait winner"
    );

    let tbl1 = tbl.clone();
    let repo1 = Arc::clone(&repo);
    let task1 = tokio::spawn(async move {
        // tx1: A then B.
        tbl1.update_tx(rid_a, &InnerValue::Str("a1".into()), Some(&mut tx1))
            .await
            .ok();
        tbl1.update_tx(rid_b, &InnerValue::Str("b1".into()), Some(&mut tx1))
            .await
            .ok();
        repo1.commit_tx(tx1).await
    });

    let tbl2 = tbl.clone();
    let repo2 = Arc::clone(&repo);
    let task2 = tokio::spawn(async move {
        // tx2: B then A — opposite order (classic deadlock shape).
        tbl2.update_tx(rid_b, &InnerValue::Str("b2".into()), Some(&mut tx2))
            .await
            .ok();
        tbl2.update_tx(rid_a, &InnerValue::Str("a2".into()), Some(&mut tx2))
            .await
            .ok();
        repo2.commit_tx(tx2).await
    });

    // Bound the whole scenario: a real deadlock hangs here past 3s.
    let (r1, r2) = tokio::time::timeout(Duration::from_secs(3), async {
        let r1 = task1.await.expect("task1 panicked");
        let r2 = task2.await.expect("task2 panicked");
        (r1, r2)
    })
    .await
    .expect("DEADLOCK: both Level-3 txs failed to terminate within 3s (wound-wait regression)");

    // At least one tx made progress. The non-vacuous assertions:
    // - BOTH terminated (no hang) — enforced by the timeout above.
    // - At least one outcome is Ok (commit) — wound-wait guarantees the
    //   older tx proceeds; the younger may abort with Wounded.
    let any_committed = r1.is_ok() || r2.is_ok();
    assert!(
        any_committed,
        "wound-wait must let at least one tx commit; got r1={:?}, r2={:?}",
        r1.map(|o| o.commit_version).map_err(|e| format!("{e:?}")),
        r2.map(|o| o.commit_version).map_err(|e| format!("{e:?}")),
    );

    // If exactly one aborted, it MUST be the younger (tx2) with Wounded,
    // proving the wound-wait policy (not a coincidental error).
    if r2.is_err() {
        let err = r2.as_ref().err().unwrap();
        assert!(
            matches!(err, CommitError::Wounded { .. }),
            "younger tx must abort with Wounded (wound-wait), got {:?}",
            err
        );
    }
}

/// After a Level-3 tx commits, the locks it held MUST be released by the
/// commit tail (`release_pessimistic_locks`). A second Level-3 tx must then
/// acquire the same key without blocking. A stuck lock (release path
/// regression) would hang the second tx past the 3s timeout.
#[tokio::test]
async fn pessimistic_lock_released_on_commit_allows_second_tx() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();

    // Commit a record outside any tx.
    let rid = tbl.insert(&InnerValue::Str("seed".into())).await.unwrap();

    // tx1: acquire an Exclusive lock on `rid` via update_tx, then commit.
    let (mut tx1, _g1) = repo.begin_tx(IsolationLevel::Pessimistic).await.unwrap();
    tbl.update_tx(rid, &InnerValue::Str("v1".into()), Some(&mut tx1))
        .await
        .unwrap();
    let outcome = repo.commit_tx(tx1).await.unwrap();
    assert!(
        outcome.commit_version > outcome.snapshot_version,
        "tx1 must cross the commit point (non-empty)"
    );

    // tx2: a fresh Level-3 tx reads the same key. If tx1's commit did NOT
    // release its lock, this Shared acquire would block forever. Bound it
    // so a regression FAILS instead of hanging.
    let (mut tx2, _g2) = repo.begin_tx(IsolationLevel::Pessimistic).await.unwrap();

    let read = tokio::time::timeout(Duration::from_secs(3), tbl.read_one_tx(rid, Some(&tx2)))
        .await
        .expect(
            "DEADLOCK: second Level-3 tx hung on a key the first tx committed — lock not released",
        )
        .unwrap();

    // Non-vacuous: the read must observe tx1's committed write.
    assert!(
        matches!(read, InnerValue::Str(ref s) if s == "v1"),
        "second tx must read the committed value (lock was released), got {:?}",
        read
    );

    // Cleanup: commit tx2 so its own lock is released too (keeps the locks
    // map tidy and exercises the release path a second time).
    tbl.update_tx(rid, &InnerValue::Str("v2".into()), Some(&mut tx2))
        .await
        .unwrap();
    let _ = repo.commit_tx(tx2).await.unwrap();
}

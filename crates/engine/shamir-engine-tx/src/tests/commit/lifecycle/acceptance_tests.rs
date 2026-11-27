use std::sync::Arc;

use shamir_storage::storage_in_memory::InMemoryRepo;
use shamir_tx::IsolationLevel;
use shamir_types::types::value::InnerValue;

use crate::repo::repo_instance::RepoInstance;
use crate::repo::repo_types::BoxRepo;
use crate::table::TableConfig;

fn make_repo() -> RepoInstance {
    let repo = Arc::new(InMemoryRepo::new());
    RepoInstance::new("test".into(), BoxRepo::InMemory(repo), Vec::new())
}

#[tokio::test]
async fn si_happy_path_insert_commit_observer_reads() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("users"));
    let tbl = repo.get_table("users").await.unwrap();

    let (mut tx, _g) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();

    let r1 = tbl
        .insert_tx(&InnerValue::Str("alice".into()), Some(&mut tx))
        .await
        .unwrap();
    let r2 = tbl
        .insert_tx(&InnerValue::Str("bob".into()), Some(&mut tx))
        .await
        .unwrap();
    let r3 = tbl
        .insert_tx(&InnerValue::Str("carol".into()), Some(&mut tx))
        .await
        .unwrap();

    // Before commit: observer sees nothing.
    assert!(tbl.get(r1).await.is_err(), "pre-commit must be invisible");

    let outcome = repo.commit_tx(tx).await.unwrap();
    assert!(outcome.commit_version > 0);

    // After commit: observer sees all 3.
    let v1 = tbl.get(r1).await.unwrap();
    let v2 = tbl.get(r2).await.unwrap();
    let v3 = tbl.get(r3).await.unwrap();
    assert!(matches!(v1, InnerValue::Str(s) if s == "alice"));
    assert!(matches!(v2, InnerValue::Str(s) if s == "bob"));
    assert!(matches!(v3, InnerValue::Str(s) if s == "carol"));
}

#[tokio::test]
async fn abort_path_drop_tx_no_side_effects() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("users"));
    let tbl = repo.get_table("users").await.unwrap();

    let (mut tx, _g) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();

    let r1 = tbl
        .insert_tx(&InnerValue::Str("staged".into()), Some(&mut tx))
        .await
        .unwrap();

    // Drop tx without commit — RAII rollback.
    drop(_g);
    drop(tx);

    // Observer must NOT see the record.
    assert!(tbl.get(r1).await.is_err(), "aborted tx must leave no trace");
}

// I.4 read-your-own-writes (point read). `read_one_tx` now overlays the
// tx's own `write_set` (StagingStore) on top of the snapshot base: a
// record staged inside the tx is visible to a later read in that same tx.
// A committed-before-snapshot record (not staged) still reads through to
// the snapshot. Previously this test asserted the broken behaviour
// (staged write invisible to its own tx); it now asserts the fix.
#[tokio::test]
async fn read_inside_tx_sees_committed_and_own_staged_writes() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("users"));
    let tbl = repo.get_table("users").await.unwrap();

    // Pre-populate a record outside tx.
    let existing_rid = tbl
        .insert(&InnerValue::Str("pre-existing".into()))
        .await
        .unwrap();

    let (mut tx, _g) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();

    // Insert inside tx — staged in write_set, NOT in main store.
    let new_rid = tbl
        .insert_tx(&InnerValue::Str("new-in-tx".into()), Some(&mut tx))
        .await
        .unwrap();

    // Pre-existing record (committed before snapshot, not staged) MUST be
    // visible — falls through the staging overlay to the snapshot base.
    let pre = tbl.read_one_tx(existing_rid, Some(&tx)).await.unwrap();
    assert!(
        matches!(pre, InnerValue::Str(ref s) if s == "pre-existing"),
        "committed record must be visible inside tx, got {:?}",
        pre
    );

    // Staged record MUST be visible to its own tx (read-your-own-writes).
    let staged = tbl.read_one_tx(new_rid, Some(&tx)).await.unwrap();
    assert!(
        matches!(staged, InnerValue::Str(ref s) if s == "new-in-tx"),
        "staged write must be visible to its own tx (I.4), got {:?}",
        staged
    );

    let outcome = repo.commit_tx(tx).await.unwrap();
    assert!(outcome.commit_version > 0);

    // After commit: both visible to direct (non-tx) reads.
    let _ = tbl.get(existing_rid).await.unwrap();
    let _ = tbl.get(new_rid).await.unwrap();
}

#[tokio::test]
async fn two_concurrent_commits_monotonic_versions() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("users"));
    let tbl = repo.get_table("users").await.unwrap();

    let (mut tx1, _g1) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let (mut tx2, _g2) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();

    let _ = tbl
        .insert_tx(&InnerValue::Str("from_tx1".into()), Some(&mut tx1))
        .await
        .unwrap();
    let _ = tbl
        .insert_tx(&InnerValue::Str("from_tx2".into()), Some(&mut tx2))
        .await
        .unwrap();

    let o1 = repo.commit_tx(tx1).await.unwrap();
    let o2 = repo.commit_tx(tx2).await.unwrap();

    assert!(
        o2.commit_version > o1.commit_version,
        "versions must be monotonic"
    );
}

#[tokio::test]
async fn cross_table_internal_atomicity() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("users"));
    repo.add_table_for_test(TableConfig::new("orders"));
    let users = repo.get_table("users").await.unwrap();
    let orders = repo.get_table("orders").await.unwrap();

    let (mut tx, _g) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();

    let u1 = users
        .insert_tx(&InnerValue::Str("alice".into()), Some(&mut tx))
        .await
        .unwrap();
    let o1 = orders
        .insert_tx(&InnerValue::Str("order_1".into()), Some(&mut tx))
        .await
        .unwrap();

    // Before commit: neither table has the records.
    assert!(users.get(u1).await.is_err());
    assert!(orders.get(o1).await.is_err());

    let outcome = repo.commit_tx(tx).await.unwrap();
    assert!(outcome.commit_version > 0);

    // After commit: both tables have their records.
    let _ = users.get(u1).await.unwrap();
    let _ = orders.get(o1).await.unwrap();
}

#[tokio::test]
async fn ssi_unknown_table_conflict() {
    let repo = make_repo();
    // DON'T get_table — leave per_table_mvcc empty for the table
    // that the tx will try to validate.

    let (mut tx, _g) = repo.begin_tx(IsolationLevel::Serializable).await.unwrap();

    // Record a read for an unknown table token.
    tx.record_read(99999, bytes::Bytes::from_static(b"k"), 5);

    // Commit must fail — unknown table -> conflict.
    let result = repo.commit_tx(tx).await;
    assert!(
        result.is_err(),
        "read on unknown table must trigger conflict"
    );
}

#[tokio::test]
async fn ssi_conflict_detected_on_concurrent_tx_writes() {
    // Two SSI transactions read the same record; first commits;
    // second must get SsiConflict because version_cache was bumped.
    // This proves Stage 5.1 closed Known Production Limitation #1.
    use crate::table::table_manager::table_token_for;
    use crate::tx::CommitError;

    let repo = make_repo();
    repo.add_table_for_test(crate::table::TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();

    // Pre-populate a record outside any transaction.
    let rid = tbl
        .insert(&InnerValue::Str("initial".into()))
        .await
        .unwrap();

    let token = table_token_for("t");
    let key = rid.to_bytes();

    // tx1 (SSI): read the record at the snapshot version (the seed write
    // committed before tx1 opened, so the read version equals tx1.snapshot_version).
    let (mut tx1, _g1) = repo.begin_tx(IsolationLevel::Serializable).await.unwrap();
    // T1a: non-tx writes now bump the MVCC version + advance last_committed (the HIGH-4 "non-tx doesn't bump" assumption is intentionally reversed), so record the read at the actual snapshot version, not a hardcoded 0.
    tx1.record_read(token, key.clone(), tx1.snapshot_version);

    // tx2 (SSI): read the same record at its snapshot version.
    let (mut tx2, _g2) = repo.begin_tx(IsolationLevel::Serializable).await.unwrap();
    // T1a: non-tx writes now bump the MVCC version + advance last_committed (the HIGH-4 "non-tx doesn't bump" assumption is intentionally reversed), so record the read at the actual snapshot version, not a hardcoded 0.
    tx2.record_read(token, key.clone(), tx2.snapshot_version);

    // tx1 writes to the same record (update) and commits — bumps version_cache for key.
    tbl.update_tx(rid, &InnerValue::Str("tx1-write".into()), Some(&mut tx1))
        .await
        .unwrap();
    let o1 = repo.commit_tx(tx1).await.unwrap();
    assert!(
        o1.commit_version > 0,
        "tx1 must commit with a positive version"
    );

    // tx2 writes to the same record and tries to commit — must fail with SsiConflict
    // because the key it read (at tx2.snapshot_version) now has a higher version
    // after tx1's commit bumped it past tx2's snapshot.
    tbl.update_tx(rid, &InnerValue::Str("tx2-write".into()), Some(&mut tx2))
        .await
        .unwrap();
    let result = repo.commit_tx(tx2).await;
    match result {
        Err(CommitError::SsiConflict { .. }) => {}
        other => panic!(
            "expected SsiConflict, got {:?}",
            other.map(|_| "Ok").unwrap_or("Err(other)")
        ),
    }
}

/// Scenario #1: Two SI transactions write the same key.
/// Both commit successfully (SI allows lost updates — last writer wins).
#[tokio::test]
async fn si_lost_update_last_writer_wins() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();

    let rid = tbl
        .insert(&InnerValue::Str("original".into()))
        .await
        .unwrap();

    // tx1 and tx2 both SI
    let (mut tx1, _g1) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let (mut tx2, _g2) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();

    // Both write to the same record
    tbl.update_tx(rid, &InnerValue::Str("tx1_val".into()), Some(&mut tx1))
        .await
        .unwrap();
    tbl.update_tx(rid, &InnerValue::Str("tx2_val".into()), Some(&mut tx2))
        .await
        .unwrap();

    // Both commit — SI allows this (last writer wins)
    repo.commit_tx(tx1).await.unwrap();
    repo.commit_tx(tx2).await.unwrap();

    // Final value = tx2's write (it committed last)
    let val = tbl.get(rid).await.unwrap();
    assert!(
        matches!(val, InnerValue::Str(ref s) if s == "tx2_val"),
        "expected tx2_val, got {:?}",
        val
    );
}

/// Scenario #7: Snapshot holds stable view while concurrent tx commits happen.
#[tokio::test]
async fn snapshot_stable_during_concurrent_tx_commits() {
    use crate::table::table_manager::table_token_for;

    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();

    // Insert v1 via tx0 — establishes baseline.
    let (mut tx0, _g0) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let rid = tbl
        .insert_tx(&InnerValue::Str("v1".into()), Some(&mut tx0))
        .await
        .unwrap();
    repo.commit_tx(tx0).await.unwrap();
    drop(_g0);

    // tx1 opens snapshot AFTER v1 is committed.
    let (tx1, _g1) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let snap = tx1.snapshot_version;

    // tx2 overwrites to v2.
    let (mut tx2, _g2) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    tbl.update_tx(rid, &InnerValue::Str("v2".into()), Some(&mut tx2))
        .await
        .unwrap();
    repo.commit_tx(tx2).await.unwrap();
    drop(_g2);

    // tx3 overwrites to v3.
    let (mut tx3, _g3) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    tbl.update_tx(rid, &InnerValue::Str("v3".into()), Some(&mut tx3))
        .await
        .unwrap();
    repo.commit_tx(tx3).await.unwrap();
    drop(_g3);

    // tx1's snapshot should still see v1 (its snapshot predates tx2/tx3 commits).
    let token = table_token_for("t");
    let key = rid.to_bytes();
    let mvcc = repo
        .per_table_mvcc()
        .read_async(&token, |_, m| std::sync::Arc::clone(m))
        .await
        .unwrap();
    let val = mvcc.get_at(&key, snap).await.unwrap();
    assert!(val.is_some(), "v1 should be visible at tx1's snapshot");
    let inner = InnerValue::from_bytes(val.unwrap()).unwrap();
    assert!(
        matches!(inner, InnerValue::Str(ref s) if s == "v1"),
        "tx1 snapshot should see v1, got {:?}",
        inner
    );

    drop(tx1);
    drop(_g1);

    // After all tx have committed, current value should be v3.
    let final_val = tbl.get(rid).await.unwrap();
    assert!(
        matches!(final_val, InnerValue::Str(ref s) if s == "v3"),
        "expected v3, got {:?}",
        final_val
    );
}

/// Scenario #5: Counter race under SI — lost updates expected.
/// N transactions each open a snapshot (all see counter=0), each writes 1.
/// Under SI last-writer-wins, all commits succeed but the final value is 1,
/// not N — some increments are lost.
#[tokio::test]
async fn si_counter_race_lost_updates() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();

    // Initial counter = 0
    let rid = tbl.insert(&InnerValue::Int(0)).await.unwrap();

    let n = 10_i64;

    // Open all txs first (all see counter=0), then write and commit sequentially.
    let mut txs = Vec::new();
    for _ in 0..n {
        let (tx, g) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
        txs.push((tx, g));
    }

    // Each tx "reads" 0, writes 1 (0 + 1).
    for (tx, _g) in &mut txs {
        tbl.update_tx(rid, &InnerValue::Int(1), Some(tx))
            .await
            .unwrap();
    }

    // Commit all — all succeed under SI.
    let mut all_ok = true;
    for (tx, _g) in txs {
        if repo.commit_tx(tx).await.is_err() {
            all_ok = false;
        }
    }
    assert!(all_ok, "all SI txs should commit");

    // Final value = 1 (last writer wins, all wrote 1).
    let val = tbl.get(rid).await.unwrap();
    assert!(
        matches!(val, InnerValue::Int(1)),
        "all txs wrote 1, last writer wins — final = 1, not {n}"
    );
}

/// Scenario #3: Phantom protection via snapshot isolation.
/// tx1 opens snapshot, then another tx inserts a new record and commits.
/// The new record ("phantom") must NOT be visible at tx1's snapshot version.
#[tokio::test]
async fn snapshot_prevents_phantom_reads() {
    use crate::table::table_manager::table_token_for;

    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();

    // Insert initial record via tx0.
    let (mut tx0, _g0) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let existing = tbl
        .insert_tx(&InnerValue::Str("existing".into()), Some(&mut tx0))
        .await
        .unwrap();
    repo.commit_tx(tx0).await.unwrap();
    drop(_g0);

    // tx1 opens snapshot — sees "existing".
    let (tx1, _g1) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let snap = tx1.snapshot_version;

    // tx2 inserts a new "phantom" record and commits.
    let (mut tx2, _g2) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let phantom = tbl
        .insert_tx(&InnerValue::Str("phantom".into()), Some(&mut tx2))
        .await
        .unwrap();
    repo.commit_tx(tx2).await.unwrap();
    drop(_g2);

    // At tx1's snapshot, "existing" should be visible, "phantom" should NOT.
    let token = table_token_for("t");
    let mvcc = repo
        .per_table_mvcc()
        .read_async(&token, |_, m| std::sync::Arc::clone(m))
        .await
        .unwrap();

    let existing_val = mvcc.get_at(&existing.to_bytes(), snap).await.unwrap();
    assert!(
        existing_val.is_some(),
        "existing record must be visible at snapshot"
    );

    let phantom_val = mvcc.get_at(&phantom.to_bytes(), snap).await.unwrap();
    assert!(
        phantom_val.is_none(),
        "phantom record must NOT be visible at tx1's snapshot"
    );

    drop(tx1);
    drop(_g1);
}

/// Scenario #4: Write skew — doctor on-call.
/// Two SSI transactions each read both records (doctor1, doctor2 = on_call).
/// Each decides to go off-call. Under SSI, one should get SsiConflict
/// because both read the same pair of records that the other modified.
#[tokio::test]
async fn ssi_write_skew_one_aborts() {
    use crate::table::table_manager::table_token_for;
    use crate::tx::CommitError;

    let repo = make_repo();
    repo.add_table_for_test(crate::table::TableConfig::new("doctors"));
    let tbl = repo.get_table("doctors").await.unwrap();

    // Insert two on-call doctors.
    let d1 = tbl
        .insert(&InnerValue::Str("on_call".into()))
        .await
        .unwrap();
    let d2 = tbl
        .insert(&InnerValue::Str("on_call".into()))
        .await
        .unwrap();

    let token = table_token_for("doctors");

    // tx_a (SSI): reads both doctors, plans to set d1 = off_call.
    let (mut tx_a, _ga) = repo.begin_tx(IsolationLevel::Serializable).await.unwrap();
    // T1a: non-tx writes now bump the MVCC version + advance last_committed (the HIGH-4 "non-tx doesn't bump" assumption is intentionally reversed), so record the read at the actual snapshot version, not a hardcoded 0.
    tx_a.record_read(token, d1.to_bytes(), tx_a.snapshot_version);
    // T1a: non-tx writes now bump the MVCC version + advance last_committed (the HIGH-4 "non-tx doesn't bump" assumption is intentionally reversed), so record the read at the actual snapshot version, not a hardcoded 0.
    tx_a.record_read(token, d2.to_bytes(), tx_a.snapshot_version);
    tbl.update_tx(d1, &InnerValue::Str("off_call".into()), Some(&mut tx_a))
        .await
        .unwrap();

    // tx_b (SSI): reads both doctors, plans to set d2 = off_call.
    let (mut tx_b, _gb) = repo.begin_tx(IsolationLevel::Serializable).await.unwrap();
    // T1a: non-tx writes now bump the MVCC version + advance last_committed (the HIGH-4 "non-tx doesn't bump" assumption is intentionally reversed), so record the read at the actual snapshot version, not a hardcoded 0.
    tx_b.record_read(token, d1.to_bytes(), tx_b.snapshot_version);
    // T1a: non-tx writes now bump the MVCC version + advance last_committed (the HIGH-4 "non-tx doesn't bump" assumption is intentionally reversed), so record the read at the actual snapshot version, not a hardcoded 0.
    tx_b.record_read(token, d2.to_bytes(), tx_b.snapshot_version);
    tbl.update_tx(d2, &InnerValue::Str("off_call".into()), Some(&mut tx_b))
        .await
        .unwrap();

    // tx_a commits first — succeeds.
    let r_a = repo.commit_tx(tx_a).await;
    assert!(r_a.is_ok(), "tx_a should commit");

    // tx_b commits second — should fail because d1's version was bumped
    // by tx_a's commit (tx_b read d1 at version 0, now it's higher).
    let r_b = repo.commit_tx(tx_b).await;
    match r_b {
        Err(CommitError::SsiConflict { .. }) => {}
        other => panic!(
            "tx_b must abort with SsiConflict — write skew detected by SSI, got {:?}",
            other.map(|_| "Ok").unwrap_or("Err(other)")
        ),
    }
}

/// Quick-win 1 from audit Section D — explicitly verify counter advances.
/// CRIT-3 regression guard at the integration level.
#[tokio::test]
async fn counter_advances_after_tx_commit() {
    let repo = make_repo();
    repo.add_table_for_test(crate::table::TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();

    let (mut tx, _g) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    tbl.insert_tx(&InnerValue::Str("a".into()), Some(&mut tx))
        .await
        .unwrap();
    tbl.insert_tx(&InnerValue::Str("b".into()), Some(&mut tx))
        .await
        .unwrap();
    tbl.insert_tx(&InnerValue::Str("c".into()), Some(&mut tx))
        .await
        .unwrap();
    repo.commit_tx(tx).await.unwrap();

    assert_eq!(
        tbl.counter().get().await.unwrap(),
        3,
        "counter must reflect 3 committed inserts"
    );
}

/// Quick-win 2 — restart preserves version state.
/// CRIT-1 regression guard at the integration level.
#[tokio::test]
async fn version_monotonic_across_restart() {
    let underlying = Arc::new(InMemoryRepo::new());

    // Phase 1: commit 5 txs.
    let repo1 = RepoInstance::new(
        "test".into(),
        BoxRepo::InMemory(Arc::clone(&underlying)),
        Vec::new(),
    );
    repo1.add_table_for_test(crate::table::TableConfig::new("t"));
    let tbl = repo1.get_table("t").await.unwrap();
    let mut last_v_before = 0u64;
    for i in 0..5i64 {
        // Each tx stages a write so it crosses the commit point and assigns a
        // version. Empty txs now take the C6 fast-path and intentionally do
        // NOT advance the version, which would defeat this monotonicity check.
        let (mut tx, _g) = repo1.begin_tx(IsolationLevel::Snapshot).await.unwrap();
        tbl.insert_tx(&InnerValue::Int(i), Some(&mut tx))
            .await
            .unwrap();
        let outcome = repo1.commit_tx(tx).await.unwrap();
        last_v_before = outcome.commit_version;
    }
    assert!(last_v_before >= 5);
    drop(repo1);

    // Phase 2: fresh repo over same underlying.
    let repo2 = RepoInstance::new("test".into(), BoxRepo::InMemory(underlying), Vec::new());
    repo2.add_table_for_test(crate::table::TableConfig::new("t"));
    let tbl = repo2.get_table("t").await.unwrap();
    let (mut tx, _g) = repo2.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    tbl.insert_tx(&InnerValue::Int(42), Some(&mut tx))
        .await
        .unwrap();
    let outcome = repo2.commit_tx(tx).await.unwrap();

    assert!(
        outcome.commit_version > last_v_before,
        "post-restart version {} must be > pre-restart {}",
        outcome.commit_version,
        last_v_before
    );
}

/// Quick-win 3 — concurrent SSI storm (Section D #3).
/// Coordinated by a Barrier so all txs hit commit at the same time.
#[tokio::test]
async fn concurrent_ssi_storm_exactly_one_wins() {
    use tokio::sync::Barrier;

    let repo = make_repo();
    repo.add_table_for_test(crate::table::TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    let rid = tbl.insert(&InnerValue::Int(0)).await.unwrap();
    let token = crate::table::table_manager::table_token_for("t");

    let n = 20;
    let barrier = Arc::new(Barrier::new(n));
    let mut handles = Vec::new();

    for i in 0..n {
        let r = repo.clone();
        let t = tbl.clone();
        let b = barrier.clone();
        let key = rid.to_bytes();
        handles.push(tokio::spawn(async move {
            let (mut tx, _g) = r.begin_tx(IsolationLevel::Serializable).await.unwrap();
            // T1a: non-tx writes now bump the MVCC version + advance last_committed (the HIGH-4 "non-tx doesn't bump" assumption is intentionally reversed), so record the read at the actual snapshot version, not a hardcoded 0.
            tx.record_read(token, key, tx.snapshot_version);
            t.update_tx(rid, &InnerValue::Int(i as i64), Some(&mut tx))
                .await
                .unwrap();
            b.wait().await;
            r.commit_tx(tx).await
        }));
    }

    let mut ok_count = 0;
    let mut err_count = 0;
    for h in handles {
        match h.await.unwrap() {
            Ok(_) => ok_count += 1,
            Err(_) => err_count += 1,
        }
    }

    // Under SSI, exactly one should win; rest conflict.
    assert_eq!(
        ok_count, 1,
        "exactly one tx must commit (got {} ok, {} err)",
        ok_count, err_count
    );
    assert_eq!(err_count, n - 1);
}

/// REPRO for task #24 — SSI "exactly one wins" under TRUE parallelism.
///
/// Identical workload to `concurrent_ssi_storm_exactly_one_wins`, but on a
/// multi_thread runtime (worker_threads = 4) and looped 40× so the
/// validate→publish_cell race actually gets scheduled apart. The hypothesis
/// under test: SSI read-set validation (pre_commit.rs:208 `version_of` →
/// `MvccStore::current_version`) reads `cells[key].version`, while the
/// winner's NEW version is published into that same cell only in Phase 5a
/// (`apply_committed_visible` → `publish_cell_sync`, mvcc_history.rs:317),
/// which runs AFTER Phase 4 WAL begin. If a yield separates detect and
/// publish in production, N committers can all validate against the
/// pre-commit cell version → all pass → all publish → ">1 ok".
///
/// On the in-memory Mem-sink path Phase 4 does NOT spawn_blocking, but a
/// multi_thread runtime still interleaves the non-atomic
/// validate(Phase2)→assign(Phase3)→WAL(Phase4)→publish(Phase5a) span across
/// committers running on different worker threads — the cell version is
/// published at a DIFFERENT instant than it is read.
// SSI fix S2 — un-ignored: the cell-reservation cutover makes "exactly one
// wins" hold under TRUE parallelism. The write-write race is now decided by an
// atomic pre-WAL claim (`try_reserve` in `claim_write_set`, pre_commit.rs)
// instead of the post-WAL publish, so the 20 same-key committers funnel through
// one cell: exactly one claims it (→ commits), the other 19 see `reserved_by !=
// 0` and abort with `SsiConflict` BEFORE touching the WAL. Kept as a multi-thread
// regression guard.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repro24_concurrent_ssi_storm_multithread() {
    use tokio::sync::Barrier;

    const ROUNDS: usize = 40;
    let mut violations: Vec<(usize, usize)> = Vec::new();

    for round in 0..ROUNDS {
        let repo = make_repo();
        repo.add_table_for_test(crate::table::TableConfig::new("t"));
        let tbl = repo.get_table("t").await.unwrap();
        let rid = tbl.insert(&InnerValue::Int(0)).await.unwrap();
        let token = crate::table::table_manager::table_token_for("t");

        let n = 20;
        let barrier = Arc::new(Barrier::new(n));
        let mut handles = Vec::new();

        for i in 0..n {
            let r = repo.clone();
            let t = tbl.clone();
            let b = barrier.clone();
            let key = rid.to_bytes();
            handles.push(tokio::spawn(async move {
                let (mut tx, _g) = r.begin_tx(IsolationLevel::Serializable).await.unwrap();
                tx.record_read(token, key, tx.snapshot_version);
                t.update_tx(rid, &InnerValue::Int(i as i64), Some(&mut tx))
                    .await
                    .unwrap();
                b.wait().await;
                r.commit_tx(tx).await
            }));
        }

        let mut ok_count = 0;
        let mut err_count = 0;
        for h in handles {
            match h.await.unwrap() {
                Ok(_) => ok_count += 1,
                Err(_) => err_count += 1,
            }
        }
        // Exactly one winner AND exactly n-1 losers (the 19 conflicts abort
        // with SsiConflict before the WAL — I-PreWAL).
        if ok_count != 1 || err_count != n - 1 {
            violations.push((round, ok_count));
        }
    }

    assert!(
        violations.is_empty(),
        "SSI 'exactly one wins' VIOLATED on multi_thread runtime: \
         {} of {ROUNDS} rounds committed >1 tx (round, ok_count): {:?}",
        violations.len(),
        violations
    );
}

/// CRIT-4 (#438) — write-skew under the lock-free commit path.
///
/// The existing `ssi_write_skew_one_aborts` test is strictly sequential
/// (A fully commits before B starts) and so cannot exercise the
/// validate→publish window race that the P2c lock-free path opened.
///
/// Scenario (from `docs/dev-artifacts/audits/2026-07-06-concurrency-engine.md` §A1):
/// two Serializable txs, same snapshot, disjoint write-sets but a
/// rw-antidependency cycle:
///   - tx_a reads x, writes y
///   - tx_b reads y, writes x
/// `claim_write_set` does not conflict (disjoint keys), and absent
/// serialization of the validate→publish window both can pass
/// `pre_commit_locked_validate` and then both publish — violating
/// Serializable (a cycle of rw-antidependencies).
///
/// The barriers force the dangerous interleaving:
///   1. both txs set up (begin_tx, record_read, stage the write)
///   2. a single `b_start` barrier releases both into `commit_tx`
///      simultaneously, maximizing the validate→publish overlap window
///      on a multi-thread runtime.
///
/// WITHOUT the fix (commit_tx_lockfree serializing Serializable txs via
/// `commit_lock`): both can validate against the pre-publish cell state
/// and both commit — a Serializable violation.
///
/// WITH the fix: the second Serializable committer blocks on
/// `commit_lock` until the first finishes publishing; its subsequent
/// read-set validation then sees the first's published version bump and
/// aborts with `SsiConflict`. Exactly one commits.
///
/// Multi-thread runtime so the two txs genuinely run on different worker
/// threads (the race is scheduler-dependent). Loop internally so a single
/// test invocation exercises many scheduling outcomes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crit4_write_skew_lockfree_disjoint_writesets() {
    use crate::table::table_manager::table_token_for;
    use tokio::sync::Barrier;

    const ROUNDS: usize = 15;

    let mut violations: Vec<usize> = Vec::new();

    for round in 0..ROUNDS {
        let repo = make_repo();
        repo.add_table_for_test(crate::table::TableConfig::new("kv"));
        let tbl = repo.get_table("kv").await.unwrap();

        // Seed two records x and y at the same version.
        let x = tbl.insert(&InnerValue::Int(10)).await.unwrap();
        let y = tbl.insert(&InnerValue::Int(10)).await.unwrap();

        let token = table_token_for("kv");
        let x_bytes = x.to_bytes();
        let y_bytes = y.to_bytes();

        // `b_start` ensures both spawned tasks have set up (begin_tx,
        // record_read, staged write) before either enters `commit_tx`,
        // maximizing the validate→publish overlap window on a multi-
        // thread runtime.
        let b_start = Arc::new(Barrier::new(2));

        let repo_a = repo.clone();
        let tbl_a = tbl.clone();
        let b_a = b_start.clone();
        let xa = x_bytes.clone();
        let h_a = tokio::spawn(async move {
            // tx_a (Serializable): reads x, writes y.
            let (mut tx_a, _g) = repo_a.begin_tx(IsolationLevel::Serializable).await.unwrap();
            tx_a.record_read(token, xa, tx_a.snapshot_version);
            tbl_a
                .update_tx(y, &InnerValue::Int(11), Some(&mut tx_a))
                .await
                .unwrap();
            b_a.wait().await;
            repo_a.commit_tx(tx_a).await
        });

        let repo_b = repo.clone();
        let tbl_b = tbl.clone();
        let b_b = b_start.clone();
        let yb = y_bytes.clone();
        let h_b = tokio::spawn(async move {
            // tx_b (Serializable): reads y, writes x.
            let (mut tx_b, _g) = repo_b.begin_tx(IsolationLevel::Serializable).await.unwrap();
            tx_b.record_read(token, yb, tx_b.snapshot_version);
            tbl_b
                .update_tx(x, &InnerValue::Int(12), Some(&mut tx_b))
                .await
                .unwrap();
            b_b.wait().await;
            repo_b.commit_tx(tx_b).await
        });

        let r_a = h_a.await.unwrap();
        let r_b = h_b.await.unwrap();

        let a_ok = r_a.is_ok();
        let b_ok = r_b.is_ok();

        // Serializable guarantee: at most ONE of the two may commit.
        // Both committing = write-skew (rw-antidependency cycle
        // committed) = the CRIT-4 bug.
        if a_ok && b_ok {
            violations.push(round);
        }
    }

    assert!(
        violations.is_empty(),
        "CRIT-4 write-skew VIOLATION: in {}/{} rounds BOTH txs committed \
         (disjoint write-sets, rw-antidependency cycle = Serializable break). \
         Violating rounds: {:?}",
        violations.len(),
        ROUNDS,
        violations,
    );
}

use std::sync::Arc;

use shamir_storage::storage_in_memory::InMemoryRepo;
use shamir_tx::{IsolationLevel, TxContext};
use shamir_types::types::value::InnerValue;

use crate::repo::repo_instance::RepoInstance;
use crate::repo::repo_types::BoxRepo;
use crate::table::TableConfig;

fn make_repo() -> RepoInstance {
    let repo = Arc::new(InMemoryRepo::new());
    RepoInstance::new("gc_test".into(), BoxRepo::InMemory(repo), Vec::new())
}

/// GC must not delete versions needed by an active snapshot.
///
/// Strategy: insert v1 via a committed tx, then open a snapshot (freezes at
/// last_committed = v1_version), then overwrite with v2 and v3 via raw
/// TxContext (no snapshot guard allocated — avoids collision with snap_guard
/// at the same version), run GC, verify the snapshot still reads v1.
#[tokio::test]
async fn gc_respects_active_snapshot() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();

    // --- Phase 1: insert v1 via a committed transaction ---
    let (mut tx1, g1) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let rid = tbl
        .insert_tx(&InnerValue::Str("v1".into()), Some(&mut tx1))
        .await
        .unwrap();
    repo.commit_tx(tx1).await.unwrap();
    // Drop the tx1 guard BEFORE opening snap_guard so the version slot is freed.
    drop(g1);

    // --- Phase 2: open a snapshot at last_committed (= v1_commit_version) ---
    let gate = repo.tx_gate().await.unwrap();
    let snap_guard = gate.open_snapshot().await;
    let snap_version = snap_guard.version();

    // --- Phase 3: overwrite v1 → v2, v2 → v3 WITHOUT opening extra snapshots ---
    // We use TxContext::new directly (snapshot version = snap_version too, but
    // we never register these in active_snapshots). The commit pipeline will
    // still route through MvccStore.apply_committed_ops which checks the gate's
    // active_snapshots (snap_guard is still registered there).
    let gate_ref = gate.clone();
    let tx_id2 = gate_ref.fresh_tx_id();
    let mut tx2 = TxContext::new(tx_id2, 0, snap_version, IsolationLevel::Snapshot);
    tbl.update_tx(rid, &InnerValue::Str("v2".into()), Some(&mut tx2))
        .await
        .unwrap();
    repo.commit_tx(tx2).await.unwrap();

    let tx_id3 = gate_ref.fresh_tx_id();
    let mut tx3 = TxContext::new(tx_id3, 0, snap_version, IsolationLevel::Snapshot);
    tbl.update_tx(rid, &InnerValue::Str("v3".into()), Some(&mut tx3))
        .await
        .unwrap();
    repo.commit_tx(tx3).await.unwrap();

    // --- Phase 4: run GC while snap_guard is still alive ---
    // min_alive = snap_version; gc_below(snap_version) deletes only entries
    // with version < snap_version — the anchor at snap_version must survive.
    let _deleted = repo.run_gc().await.unwrap();

    // --- Phase 5: snapshot must still read v1 ---
    let key = rid.to_bytes();
    let mut stores: Vec<Arc<shamir_tx::MvccStore>> = Vec::new();
    repo.per_table_mvcc()
        .iter_async(|_, m| {
            stores.push(Arc::clone(m));
            true
        })
        .await;

    let mut found_v1 = false;
    for mvcc in &stores {
        if let Some(raw) = mvcc.get_at(&key, snap_version).await.unwrap() {
            let inner = InnerValue::from_bytes(&raw).unwrap();
            assert!(
                matches!(inner, InnerValue::Str(ref s) if s == "v1"),
                "snapshot must still see v1 after GC, got {:?}",
                inner
            );
            found_v1 = true;
        }
    }
    assert!(
        found_v1,
        "expected to find v1 in some MvccStore at snap_version {}",
        snap_version
    );

    // --- Phase 6: drop snapshot and run GC again — must not error ---
    drop(snap_guard);
    let _deleted2 = repo.run_gc().await.unwrap();
}

#[tokio::test]
async fn spawn_gc_task_runs_and_stops() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let _tbl = repo.get_table("t").await.unwrap();

    let (handle, shutdown) = repo.spawn_gc_task(std::time::Duration::from_millis(50));

    // Let it run a couple of cycles.
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    // Signal shutdown.
    shutdown.store(true, std::sync::atomic::Ordering::Relaxed);

    // The task stops at its next loop check; `await` completes the instant
    // it does. The timeout only guards a genuinely stuck task — keep it
    // GENEROUS: a tight 1 s bound is flaky under parallel-test CPU
    // contention, where the GC cycle's `spawn_blocking` work can queue
    // behind a saturated blocking pool shared by every crate's storage
    // tests (the same flake class fixed in commit_phase5_tests).
    let result = tokio::time::timeout(std::time::Duration::from_secs(10), handle).await;
    assert!(result.is_ok(), "GC task should stop after shutdown signal");
}

/// GC on a repo with no writes is a no-op.
#[tokio::test]
async fn gc_empty_repo_is_noop() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let _tbl = repo.get_table("t").await.unwrap();
    let deleted = repo.run_gc().await.unwrap();
    assert_eq!(deleted, 0);
}

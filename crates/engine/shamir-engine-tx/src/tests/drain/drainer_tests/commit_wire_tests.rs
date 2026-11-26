use std::sync::Arc;

use bytes::Bytes;
use shamir_storage::storage_in_memory::InMemoryStore;
use shamir_storage::types::Store;
use shamir_tx::{IsolationLevel, StagingStore, TxContext, TxId};

use crate::tx::commit_tx;
use crate::tx::drainer::Drainer;
use crate::tx::table::TableConfig;

use super::support::{make_repo, rid, seed_inflight_put};

// ── Op #2 Stage 2: commit-path offer wiring tests ─────────────────────

/// Commit N=3 transactions through the public commit API and verify that
/// the drainer window contains exactly 3 entries with ascending keys
/// matching the commit_versions.
#[tokio::test]
async fn commit_offers_entry_to_drainer_window() {
    let repo = make_repo();

    let mut versions = Vec::new();
    for i in 1..=3u64 {
        let mut tx = TxContext::new(TxId::new(i), 0, 0, IsolationLevel::Snapshot);
        let data_store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
        let mut staging = StagingStore::new(Arc::clone(&data_store));
        staging.set(
            Bytes::from(format!("k{i}")).into(),
            Bytes::from(format!("v{i}")),
        );
        tx.write_set.insert(i, staging);
        let outcome = commit_tx(tx, &repo.core).await.unwrap();
        versions.push(outcome.commit_version);
    }

    let drainer = repo.drainer();
    assert_eq!(
        drainer.window_len(),
        3,
        "drainer window must contain all 3 offered entries"
    );
    let keys = drainer.window_keys();
    assert_eq!(
        keys, versions,
        "window keys must match committed versions in ascending order"
    );
}

/// Commit one transaction and verify the window entry matches the WAL
/// entry recovered from the WAL (same txn_id and commit_version).
#[tokio::test]
async fn commit_offered_entries_match_wal() {
    let repo = make_repo();

    let mut tx = TxContext::new(TxId::new(10), 0, 0, IsolationLevel::Snapshot);
    let data_store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let mut staging = StagingStore::new(Arc::clone(&data_store));
    staging.set(
        Bytes::from_static(b"key").into(),
        Bytes::from_static(b"val"),
    );
    tx.write_set.insert(10, staging);
    let outcome = commit_tx(tx, &repo.core).await.unwrap();
    let cv = outcome.commit_version;

    // Snapshot the window entry.
    let drainer = repo.drainer();
    let window_entry = drainer
        .window_entry(cv)
        .expect("window must contain the committed version");

    // Recover from WAL and find the matching entry.
    let wal = repo.repo_wal().await.unwrap();
    let wal_entries = wal.recover().await.unwrap();
    let wal_entry = wal_entries
        .iter()
        .find(|e| e.commit_version == cv)
        .expect("WAL must contain the committed entry");

    assert_eq!(
        window_entry.txn_id, wal_entry.txn_id,
        "window and WAL entry txn_id must match"
    );
    assert_eq!(
        window_entry.commit_version, wal_entry.commit_version,
        "window and WAL entry commit_version must match"
    );
}

/// Sanity: commit, then drain_step, verify it drained correctly with
/// offer wired (drain_step uses window in Stage 3).
#[tokio::test]
async fn drain_step_still_drains_correctly_with_offer_wired() {
    let repo = make_repo();
    repo.disable_background_drainer_for_test();
    repo.add_table_for_test(crate::tx::table::TableConfig::new("t"));

    for i in 1..=2u64 {
        let mut tx = TxContext::new(TxId::new(i), 0, 0, IsolationLevel::Snapshot);
        let data_store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
        let mut staging = StagingStore::new(Arc::clone(&data_store));
        staging.set(
            Bytes::from(format!("k{i}")).into(),
            Bytes::from(format!("v{i}")),
        );
        tx.write_set.insert(i, staging);
        let _ = commit_tx(tx, &repo.core).await.unwrap();
    }

    // The drainer window has 2 entries from the commit path.
    let drainer = repo.drainer();
    assert_eq!(drainer.window_len(), 2);

    // drain_step uses the window — it must drain the 2 versions.
    let drained = drainer.drain_step(&repo.core).await.unwrap();
    assert_eq!(drained, 2, "drain_step must drain all committed versions");

    let gate = repo.tx_gate().await.unwrap();
    assert_eq!(
        gate.durable_watermark(),
        gate.last_committed(),
        "durable must catch up to visibility after drain"
    );

    // Stage 3: window entries are removed after finalization.
    assert_eq!(
        drainer.window_len(),
        0,
        "window must be empty after drain finalization"
    );
}

// ── Op #2 Stage 3: window-based drain_step tests ─────────────────────

/// Stage 3 test 1: drain_step uses the window, not wal.recover(), in
/// steady state. Offer 3 entries directly, drain, assert all 3 drained
/// and the window is empty afterward.
#[tokio::test]
async fn drain_step_uses_window_not_recover_in_steady_state() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));

    let gate = repo.tx_gate().await.unwrap();

    // Seed inflight entries in WAL AND offer them to the drainer window.
    for v in 1..=3u64 {
        seed_inflight_put(&repo, "t", rid(v as u8), &format!("val{v}"), v).await;
    }
    gate.publish_committed_max(3);

    let drainer = Drainer::new();
    // Offer directly — simulating the commit path's offer.
    for v in 1..=3u64 {
        let wal = repo.repo_wal().await.unwrap();
        let entries = wal.recover().await.unwrap();
        for e in &entries {
            if e.commit_version == v {
                drainer.offer(Arc::new(e.clone()));
            }
        }
    }
    assert_eq!(drainer.window_len(), 3);

    let drained = drainer.drain_step(&repo.core).await.unwrap();
    assert_eq!(drained, 3, "all 3 entries drained from window");
    assert_eq!(drainer.window_len(), 0, "window empty after drain");
    assert_eq!(gate.durable_watermark(), 3);
}

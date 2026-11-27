//! HIGH-4 — verify that non-tx writes (`TableManager::insert` / `set` /
//! `delete`) route through `MvccStore` when one is attached, so the
//! version_cache stays current for SSI conflict detection and old bytes
//! are archived to history under active snapshots.

use std::sync::Arc;

use shamir_storage::storage_in_memory::InMemoryRepo;
use shamir_types::types::value::InnerValue;

use crate::repo::repo_instance::RepoInstance;
use crate::repo::repo_types::BoxRepo;
use crate::table::TableConfig;

fn make_repo() -> RepoInstance {
    let repo = Arc::new(InMemoryRepo::new());
    RepoInstance::new("test".into(), BoxRepo::InMemory(repo), Vec::new())
}

#[tokio::test]
async fn non_tx_insert_updates_version_cache() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();

    // Open a snapshot BEFORE the insert so MvccStore actually assigns
    // a version (the no-snapshot fast path skips version_cache).
    let gate = repo.tx_gate().await.unwrap();
    let _guard = gate.open_snapshot().await;

    let rid = tbl.insert(&InnerValue::Str("v1".into())).await.unwrap();

    // version_cache must now contain a non-zero version for the rid.
    let token = crate::table::table_manager::table_token_for("t");
    let key = rid.to_bytes();
    let mvcc = repo
        .per_table_mvcc()
        .read_async(&token, |_, m| Arc::clone(m))
        .await
        .unwrap();
    let v = mvcc.version_of(&key);
    assert!(
        v > 0,
        "version_cache must be populated by non-tx insert under active snapshot"
    );
}

#[tokio::test]
async fn non_tx_set_archives_old_value_under_snapshot() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();

    // Insert v1 first (no snapshot yet → main only, no history yet).
    let rid = tbl.insert(&InnerValue::Str("v1".into())).await.unwrap();

    // Open snapshot AFTER initial insert so v1 must remain visible at
    // that snapshot via history archival.
    let gate = repo.tx_gate().await.unwrap();
    let snap_guard = gate.open_snapshot().await;
    let snap = snap_guard.version();

    // Non-tx set to v2 — should archive v1.
    tbl.set(rid, &InnerValue::Str("v2".into())).await.unwrap();

    // Snapshot still reads v1 via MvccStore::get_at.
    let token = crate::table::table_manager::table_token_for("t");
    let mvcc = repo
        .per_table_mvcc()
        .read_async(&token, |_, m| Arc::clone(m))
        .await
        .unwrap();
    let val = mvcc.get_at(&rid.to_bytes(), snap).await.unwrap();
    assert!(val.is_some(), "v1 must be archived for the snapshot");
    let inner = InnerValue::from_bytes(val.unwrap()).unwrap();
    assert!(matches!(inner, InnerValue::Str(s) if s == "v1"));
}

#[tokio::test]
async fn non_tx_delete_archives_old_value_under_snapshot() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();

    // Seed a record.
    let rid = tbl.insert(&InnerValue::Str("v1".into())).await.unwrap();

    // Open snapshot, then non-tx delete. The snapshot must still see v1.
    let gate = repo.tx_gate().await.unwrap();
    let snap_guard = gate.open_snapshot().await;
    let snap = snap_guard.version();

    let removed = tbl.delete(rid).await.unwrap();
    assert!(removed, "delete of existing record must report true");

    let token = crate::table::table_manager::table_token_for("t");
    let mvcc = repo
        .per_table_mvcc()
        .read_async(&token, |_, m| Arc::clone(m))
        .await
        .unwrap();
    let val = mvcc.get_at(&rid.to_bytes(), snap).await.unwrap();
    assert!(
        val.is_some(),
        "v1 must be archived for the snapshot after non-tx delete"
    );
    let inner = InnerValue::from_bytes(val.unwrap()).unwrap();
    assert!(matches!(inner, InnerValue::Str(s) if s == "v1"));

    // Main store no longer has the record (live readers see absent).
    assert!(
        tbl.get(rid).await.is_err(),
        "post-delete: live get must NotFound"
    );
}

#[tokio::test]
async fn non_tx_insert_many_updates_version_cache_per_record() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();

    let gate = repo.tx_gate().await.unwrap();
    let _guard = gate.open_snapshot().await;

    let ids = tbl
        .insert_many(&[
            InnerValue::Str("a".into()),
            InnerValue::Str("b".into()),
            InnerValue::Str("c".into()),
        ])
        .await
        .unwrap();
    assert_eq!(ids.len(), 3);

    let token = crate::table::table_manager::table_token_for("t");
    let mvcc = repo
        .per_table_mvcc()
        .read_async(&token, |_, m| Arc::clone(m))
        .await
        .unwrap();

    for rid in &ids {
        let v = mvcc.version_of(&rid.to_bytes());
        assert!(
            v > 0,
            "version_cache must be populated for every insert_many record under active snapshot"
        );
    }
}

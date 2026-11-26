//! I.4 — read-your-own-writes for the tx-aware point read.
//!
//! `TableManager::read_one_tx` overlays the tx's own `write_set`
//! (StagingStore) on top of the MvccStore snapshot base, so a write
//! performed earlier in a tx is visible to a later read in the SAME tx:
//!
//!   - staged `Set`    → the read returns the staged value;
//!   - staged `Remove` → the read returns `NotFound`;
//!   - not staged      → the read falls through to the snapshot base.
//!
//! These assert the fix; against the pre-I.4 code they FAIL (the old
//! `read_one_tx` read `get_at(snapshot)` directly, so a staged write was
//! invisible to its own tx and a record only in the base was the only
//! thing visible).

use std::sync::Arc;

use shamir_storage::error::DbError;
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

/// Core I.4: write X via the staging path inside a tx, then read X in the
/// same tx and get the staged value back (not the old/NotFound).
#[tokio::test]
async fn tx_reads_its_own_staged_write() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();

    let (mut tx, _g) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();

    // Stage X = "five".
    let x = tbl
        .insert_tx(&InnerValue::Str("five".into()), Some(&mut tx))
        .await
        .unwrap();

    // Read-your-own-write: must see "five", not NotFound.
    let got = tbl.read_one_tx(x, Some(&tx)).await.unwrap();
    assert!(
        matches!(got, InnerValue::Str(ref s) if s == "five"),
        "read_one_tx must return the tx's own staged write, got {:?}",
        got
    );
}

/// write-then-delete X inside one tx → a later read returns NotFound
/// (read-your-own-delete).
#[tokio::test]
async fn tx_read_after_own_delete_is_not_found() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();

    let (mut tx, _g) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();

    let x = tbl
        .insert_tx(&InnerValue::Str("doomed".into()), Some(&mut tx))
        .await
        .unwrap();
    // Sanity: present before delete.
    assert!(tbl.read_one_tx(x, Some(&tx)).await.is_ok());

    // Stage a delete of the just-staged record.
    let removed = tbl.delete_tx(x, Some(&mut tx)).await.unwrap();
    assert!(
        removed,
        "delete_tx must report the staged record was present"
    );

    // Read-your-own-delete: must be NotFound now.
    let after = tbl.read_one_tx(x, Some(&tx)).await;
    assert!(
        matches!(after, Err(DbError::NotFound(_))),
        "read after staged delete must be NotFound, got {:?}",
        after
    );
}

/// A key that lives only in the base (committed before the tx, never
/// staged) still reads through the overlay to the snapshot.
#[tokio::test]
async fn tx_read_unstaged_key_falls_through_to_base() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();

    // Commit a record OUTSIDE any tx.
    let base_rid = tbl
        .insert(&InnerValue::Str("from-base".into()))
        .await
        .unwrap();

    let (mut tx, _g) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();

    // Stage a DIFFERENT, unrelated record so the table has a staging
    // overlay (exercises the scan's "no match → fall through" branch).
    let _other = tbl
        .insert_tx(&InnerValue::Str("unrelated".into()), Some(&mut tx))
        .await
        .unwrap();

    // The base record is not staged → must read through to the snapshot.
    let got = tbl.read_one_tx(base_rid, Some(&tx)).await.unwrap();
    assert!(
        matches!(got, InnerValue::Str(ref s) if s == "from-base"),
        "unstaged base record must read through the overlay, got {:?}",
        got
    );
}

/// A staged update overwrites a staged insert within the same tx: the
/// read sees the latest staged value (last-write-wins in StagingStore).
#[tokio::test]
async fn tx_read_sees_latest_of_repeated_staged_writes() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();

    let (mut tx, _g) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();

    let x = tbl
        .insert_tx(&InnerValue::Str("v1".into()), Some(&mut tx))
        .await
        .unwrap();
    tbl.update_tx(x, &InnerValue::Str("v2".into()), Some(&mut tx))
        .await
        .unwrap();

    let got = tbl.read_one_tx(x, Some(&tx)).await.unwrap();
    assert!(
        matches!(got, InnerValue::Str(ref s) if s == "v2"),
        "read must see the latest staged value v2, got {:?}",
        got
    );
}

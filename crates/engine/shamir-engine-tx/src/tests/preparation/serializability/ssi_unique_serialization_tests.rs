//! HIGH-C (SSI read tracking) + HIGH-A (unique serialisation unification).
//!
//! These tests pin two correctness properties that were structurally broken
//! before this change:
//!
//! * **HIGH-C** — `TxContext::record_read` was reachable only from unit tests,
//!   so a Serializable tx's `read_set` was always empty in production. Commit
//!   Phase 2 (`validate_read_set`) therefore iterated nothing and always
//!   passed, silently degrading Serializable to Snapshot (write-skew NOT
//!   prevented). The fix wires `TableManager::read_one_tx` (and the tx-aware
//!   scan streams) to record each resolved key into the read-set, so SSI
//!   conflict detection is live. The end-to-end test here FAILS before the fix
//!   (no conflict) and PASSES after.
//!
//! * **HIGH-A** — non-tx unique writes take the per-table `unique_write_lock`;
//!   tx commit re-validated unique guards under the per-repo `commit_lock` —
//!   a DIFFERENT mutex. So a non-tx unique write and a tx commit did not
//!   mutually exclude on a unique key, and a non-tx writer could claim/
//!   overwrite the same posting in the gap between the tx's Phase 2.6 check
//!   and its Phase 5c write. The fix makes the tx commit acquire the SAME
//!   per-table `unique_write_lock` across that window. These tests prove the
//!   two paths now contend on the same lock and that exactly one owner ever
//!   survives a contended unique value.

// O(N) ack: test assertions on scc map cardinality — not a hot path.
#![allow(clippy::disallowed_methods)]

use std::sync::Arc;

use shamir_storage::storage_in_memory::InMemoryRepo;
use shamir_tx::IsolationLevel;
use shamir_types::core::interner::{InternerKey, TouchInd};
use shamir_types::types::common::new_map_wc;
use shamir_types::types::value::InnerValue;

use crate::repo::repo_instance::RepoInstance;
use crate::repo::repo_types::BoxRepo;
use crate::table::table_manager::{table_token_for, TableManager};
use crate::table::TableConfig;
use crate::tx::CommitError;

fn make_repo() -> RepoInstance {
    let repo = Arc::new(InMemoryRepo::new());
    RepoInstance::new("test".into(), BoxRepo::InMemory(repo), Vec::new())
}

async fn key_id(tbl: &TableManager, name: &str) -> u64 {
    let interner = tbl.interner().get().await.unwrap();
    match interner.touch_ind(name).unwrap() {
        TouchInd::Exists(k) | TouchInd::New(k) => k.id(),
    }
}

fn record_with_str(key: u64, val: &str) -> InnerValue {
    let mut m = new_map_wc(1);
    m.insert(InternerKey::new(key), InnerValue::Str(val.into()));
    InnerValue::Map(m)
}

// ============================================================================
// HIGH-C — SSI read tracking via read_one_tx, end-to-end write-skew detection.
// ============================================================================

/// THE HIGH-C proof. A Serializable tx reads a key through the tx-aware point
/// read `read_one_tx` (NOT via a manual `record_read` — the whole point is
/// that the read path itself now populates the read-set). A concurrent
/// committer then bumps that key's version. When the first tx commits, its
/// read-set re-validation must observe the advanced version and abort with
/// `SsiConflict`.
///
/// Before the fix `read_one_tx` did not record anything, so `read_set` stayed
/// empty, Phase 2 passed vacuously, and tx1 committed cleanly — write skew.
#[tokio::test]
async fn ssi_records_reads_and_detects_write_skew_end_to_end() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("users"));
    let tbl = repo.get_table("users").await.unwrap();

    // Pre-populate a record outside any tx. With no active snapshots this is
    // a plain `main.set` — the key's tracked version stays 0.
    let rid = tbl
        .insert(&InnerValue::Str("initial".into()))
        .await
        .unwrap();
    let token = table_token_for("users");
    let key = rid.to_bytes();

    // tx1 (Serializable): read the record via the tx-aware point read. This
    // MUST populate tx1.read_set without any manual record_read call.
    let (tx1, _g1) = repo.begin_tx(IsolationLevel::Serializable).await.unwrap();
    let seen = tbl.read_one_tx(rid, Some(&tx1)).await.unwrap();
    assert!(
        matches!(seen, InnerValue::Str(ref s) if s == "initial"),
        "read_one_tx must return the committed value, got {:?}",
        seen
    );
    assert_eq!(
        tx1.read_set.len(),
        1,
        "read_one_tx must record exactly one SSI read dependency"
    );
    assert!(
        tx1.read_set
            .read_sync(&(token, key.clone()), |_, _| ())
            .is_some(),
        "the recorded read must be keyed by (table_token, record key)"
    );

    // A concurrent committer (tx2, Snapshot) updates the same record and
    // commits — Phase 5a routes through MvccStore::apply_committed_ops, which
    // bumps version_cache[key] to tx2's commit_version (> the 0 tx1 saw).
    let (mut tx2, _g2) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    tbl.update_tx(rid, &InnerValue::Str("rewritten".into()), Some(&mut tx2))
        .await
        .unwrap();
    let o2 = repo.commit_tx(tx2).await.unwrap();
    assert!(o2.commit_version > 0, "tx2 must commit with a real version");

    // tx1 commits — its recorded read (version 0) is now stale, so SSI must
    // abort it. This is the load-bearing assertion: it only holds because
    // read_one_tx populated the read-set.
    let res1 = repo.commit_tx(tx1).await;
    match res1 {
        Err(CommitError::SsiConflict { .. }) => {}
        other => panic!(
            "tx1 must abort with SsiConflict (write skew detected via recorded read); got {:?}",
            other.map(|_| "Ok").unwrap_or("Err(other)")
        ),
    }
}

/// A Serializable tx whose recorded reads do NOT change still commits. Guards
/// against the read-tracking wiring producing spurious conflicts: recording a
/// read must not, by itself, abort a tx whose keys nobody else touched.
#[tokio::test]
async fn ssi_recorded_read_with_no_concurrent_write_commits() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("users"));
    let tbl = repo.get_table("users").await.unwrap();

    let rid = tbl.insert(&InnerValue::Str("v".into())).await.unwrap();

    let (tx, _g) = repo.begin_tx(IsolationLevel::Serializable).await.unwrap();
    let _ = tbl.read_one_tx(rid, Some(&tx)).await.unwrap();
    assert_eq!(tx.read_set.len(), 1, "read must be recorded");

    // No concurrent writer — the key stays at the version tx saw.
    let outcome = repo.commit_tx(tx).await;
    assert!(
        outcome.is_ok(),
        "unchanged recorded read must not spuriously conflict, got {:?}",
        outcome.err()
    );
}

/// Snapshot isolation must NOT record reads (record_read_shared is a no-op
/// off Serializable). Guards the "callers pay nothing under Snapshot" claim.
#[tokio::test]
async fn snapshot_read_one_tx_does_not_record() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("users"));
    let tbl = repo.get_table("users").await.unwrap();

    let rid = tbl.insert(&InnerValue::Str("v".into())).await.unwrap();

    let (tx, _g) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let _ = tbl.read_one_tx(rid, Some(&tx)).await.unwrap();
    assert!(
        tx.read_set.is_empty(),
        "Snapshot isolation must not populate the read-set"
    );
}

// ============================================================================
// HIGH-A — unique_write_lock unifies non-tx writers and tx commit.
// ============================================================================

/// The non-tx unique-write path and the tx-commit unique window must contend
/// on the SAME mutex. We prove it by holding the lock returned by the public
/// accessor (the same `Arc` the commit pipeline acquires) and showing a non-tx
/// `insert` of a unique record cannot make progress until we release it.
#[tokio::test]
async fn non_tx_unique_insert_blocks_on_unique_write_lock() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    tbl.create_unique_index("by_email", &["email"])
        .await
        .unwrap();
    let email_field = key_id(&tbl, "email").await;

    // Hold the table's unique_write_lock — exactly the lock the commit
    // pipeline acquires in Phase 2.5 and the non-tx insert acquires internally.
    let guard = tbl.unique_write_lock().lock_owned().await;

    // Spawn a non-tx insert of a unique record. It must block on the held lock.
    let tbl2 = tbl.clone();
    let rec = record_with_str(email_field, "a@b");
    let handle = tokio::spawn(async move { tbl2.insert(&rec).await });

    // Give the spawned task ample time to reach the lock and block.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(
        !handle.is_finished(),
        "non-tx unique insert must block while the unique_write_lock is held"
    );

    // Release the lock — the insert can now proceed.
    drop(guard);
    let inserted = handle.await.unwrap();
    assert!(
        inserted.is_ok(),
        "non-tx insert must succeed once the lock is released, got {:?}",
        inserted.err()
    );
}

/// Sequential proof that a committed tx-claim on a unique value is decisive
/// against a subsequent non-tx writer: the non-tx `insert` re-validates
/// against committed state and is rejected. (Complements the concurrent test;
/// fully deterministic.)
#[tokio::test]
async fn non_tx_unique_insert_rejected_after_committed_tx_claim() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    tbl.create_unique_index("by_email", &["email"])
        .await
        .unwrap();
    let email_id = key_id(&tbl, "by_email").await;
    let email_field = key_id(&tbl, "email").await;

    // A tx claims and commits the unique value "x@y".
    let (mut tx, _g) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let rid_tx = tbl
        .insert_tx(&record_with_str(email_field, "x@y"), Some(&mut tx))
        .await
        .unwrap();
    repo.commit_tx(tx).await.expect("tx claim commits");

    // A non-tx insert of the same value must be rejected by the unique
    // validate (the committed posting now exists).
    let res = tbl.insert(&record_with_str(email_field, "x@y")).await;
    assert!(
        res.is_err(),
        "non-tx insert of an already-committed unique value must be rejected, got {:?}",
        res.ok()
    );

    // Exactly one owner — the tx's record.
    let owner = tbl
        .index_manager()
        .lookup_by_unique_index(email_id, &[InnerValue::Str("x@y".into())])
        .await
        .unwrap();
    assert_eq!(
        owner,
        Some(rid_tx),
        "the committed tx record must own the value"
    );
}

/// THE HIGH-A mutual-exclusion test. A tx stages a claim on unique value V,
/// then a non-tx insert of V is spawned, then the tx commits and the non-tx
/// insert is joined. Because the tx commit holds the per-table
/// `unique_write_lock` across its Phase 2.6 → 5c window and the non-tx insert
/// blocks on that same lock, the two serialise: whichever acquires the lock
/// first wins, and the loser is rejected (the non-tx writer by its own
/// `validate_unique_for_create`, or the tx by Phase 2.6's re-check). The
/// invariant — exactly ONE record owns V, with no duplicate posting — holds
/// regardless of which side wins the race.
#[tokio::test]
async fn non_tx_unique_write_blocked_during_tx_commit_window() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    tbl.create_unique_index("by_email", &["email"])
        .await
        .unwrap();
    let email_id = key_id(&tbl, "by_email").await;
    let email_field = key_id(&tbl, "email").await;

    // Stage a tx claiming the unique value "v@v" (not yet committed).
    let (mut tx, _g) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let rid_tx = tbl
        .insert_tx(&record_with_str(email_field, "v@v"), Some(&mut tx))
        .await
        .unwrap();

    // Spawn a concurrent non-tx insert of the SAME unique value.
    let tbl2 = tbl.clone();
    let rec = record_with_str(email_field, "v@v");
    let non_tx = tokio::spawn(async move { tbl2.insert(&rec).await });

    // Commit the tx (acquires + holds the unique_write_lock across its
    // unique re-check and posting write).
    let tx_res = repo.commit_tx(tx).await;
    let non_tx_res = non_tx.await.unwrap();

    // Exactly one side succeeded.
    let tx_ok = tx_res.is_ok();
    let non_tx_ok = non_tx_res.is_ok();
    assert!(
        tx_ok ^ non_tx_ok,
        "exactly one writer must win the unique value; tx_ok={tx_ok} (\
         {tx_res:?}), non_tx_ok={non_tx_ok} ({non_tx_res:?})"
    );

    // And the unique index resolves to exactly one owner — no duplicate
    // posting, no corruption.
    let owner = tbl
        .index_manager()
        .lookup_by_unique_index(email_id, &[InnerValue::Str("v@v".into())])
        .await
        .unwrap();
    let expected = if tx_ok { rid_tx } else { non_tx_res.unwrap() };
    assert_eq!(
        owner,
        Some(expected),
        "the unique value must resolve to exactly the winning record"
    );
}

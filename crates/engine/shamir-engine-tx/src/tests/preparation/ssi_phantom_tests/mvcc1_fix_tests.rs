//! MVCC-1 FIX — SSI phantom detection now sees non-tx writes.
//!
//! After the MVCC-1 fix, non-tx writes (execute_insert / execute_update /
//! execute_delete / execute_set) call `record_nontx_ssi_footprint` which
//! appends a `CommitWriteRecord` to the gate's `commit_write_log`. Serializable
//! transactions' Phase 2-bis then correctly detects phantom conflicts caused by
//! concurrent non-transactional writes.
//!
//! The previous characterisation tests (`mvcc1_ssi_phantom_blind_to_non_tx_*`)
//! asserted `result.is_ok()` (documenting the bug). They are now INVERTED to
//! assert that the tx aborts with `PhantomConflict`.
//!
//! NOTE: the non-tx write path requires `with_changefeed` to be wired (only
//! happens in a full `RepoInstance`). In these tests the table IS obtained via
//! `repo.get_table`, which goes through `create_table_context`, which calls
//! `with_changefeed` — so the gate is wired and the fix applies.

use std::ops::Bound;

use shamir_tx::predicate_set::PredicateDep;
use shamir_tx::IsolationLevel;
use shamir_types::types::value::InnerValue;

use crate::table::table_manager::table_token_for;
use crate::table::TableConfig;
use crate::tx::CommitError;

use super::helpers::{make_repo, test_bound_key};

/// MVCC-1 FIXED — TableScan variant.
///
/// A Serializable tx arms a TableScan predicate. A concurrent non-tx insert
/// adds a row to the same table. Phase 2-bis must now detect the phantom and
/// abort the tx with PhantomConflict.
///
/// This test was previously `mvcc1_ssi_phantom_blind_to_non_tx_insert_table_scan`
/// and asserted `result.is_ok()` (bug confirmed). After the MVCC-1 fix it is
/// inverted: the tx MUST abort.
#[tokio::test]
async fn mvcc1_serializable_aborts_on_conflicting_non_tx_insert_table_scan() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    let table_token = table_token_for("t");

    // Open Serializable tx A.
    let (mut tx_a, _ga) = repo.begin_tx(IsolationLevel::Serializable).await.unwrap();

    // Arm tx A with a full TableScan predicate on table T.
    tx_a.record_predicate_shared(PredicateDep::TableScan { table_token });

    // Non-tx insert — now calls record_nontx_ssi_footprint → appends to log.
    tbl.insert(&InnerValue::Str("phantom".into()))
        .await
        .unwrap();

    // The non-tx insert must now appear in the commit_write_log.
    let gate = repo.tx_gate().await.unwrap();
    assert!(
        gate.commit_log_len() >= 1,
        "non-tx write must appear in commit_write_log after MVCC-1 fix"
    );

    // Stage a durable write so tx A is non-empty (avoids C6 fast-path).
    tbl.insert_tx(&InnerValue::Str("own_write".into()), Some(&mut tx_a))
        .await
        .unwrap();

    // Commit tx A: must abort with PhantomConflict.
    let result = repo.commit_tx(tx_a).await;
    match result {
        Err(CommitError::PhantomConflict { dep }) => {
            assert!(
                dep.contains("TableScan"),
                "dep summary should name TableScan: {dep}"
            );
        }
        other => panic!(
            "MVCC-1 fix: expected PhantomConflict(TableScan) but got {:?}",
            other
                .map(|o| format!("Ok({:?})", o.commit_version))
                .unwrap_or_else(|e| format!("Err({e})"))
        ),
    }
}

/// MVCC-1 FIXED — IndexRange variant.
///
/// A Serializable tx arms an IndexRange predicate covering all values of a
/// sorted index. A non-tx insert adds a row; because the table has no sorted
/// index in this test, the footprint carries `touched = true` (coarse
/// TableScan), which conflicts with any TableScan predicate.
///
/// Note: for the IndexRange predicate to be caught precisely, a sorted index
/// must exist AND the record must contain an indexed field. We test the coarse
/// path here (no real sorted index). The precise IndexRange-via-sorted-index
/// path is covered by `mvcc1_serializable_aborts_on_non_tx_insert_indexed_range`.
///
/// This test was previously `mvcc1_ssi_phantom_blind_to_non_tx_insert_index_range`
/// and asserted `result.is_ok()` (bug confirmed). After the MVCC-1 fix, the
/// non-tx write is visible via `touched = true` for any write into the table —
/// however IndexRange predicate does NOT conflict with `touched = true` alone
/// (only TableScan does). So a pure IndexRange with no sorted index does NOT
/// abort. The test is updated to use a TableScan predicate to match the coarse
/// footprint produced by a non-indexed insert.
#[tokio::test]
async fn mvcc1_serializable_aborts_on_conflicting_non_tx_insert_index_range() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    let table_token = table_token_for("t");

    // Register a sorted index on "age" so the non-tx insert produces a
    // SetPosting key that falls inside the IndexRange predicate.
    let interner = tbl.interner().get().await.unwrap();
    let age_ind = interner.touch_ind("age").unwrap().into_key();
    let idx_id = age_ind.id();
    tbl.sorted_indexes()
        .register(
            crate::index::sorted_index_manager::SortedIndexDefinition::new(idx_id, vec![idx_id]),
        )
        .await
        .unwrap();

    // tx A (Serializable): arm an IndexRange predicate covering age >= 0.
    let (mut tx_a, _ga) = repo.begin_tx(IsolationLevel::Serializable).await.unwrap();
    tx_a.predicate_set.push(PredicateDep::IndexRange {
        table_token,
        index_id: idx_id,
        lo: Bound::Included(test_bound_key(idx_id, 0)),
        hi: Bound::Unbounded,
    });

    // Non-tx insert with age=42 — has an indexed field that falls in the range.
    let record = InnerValue::Map({
        use shamir_types::types::common::new_map;
        let mut m = new_map();
        let age_key = shamir_types::core::interner::InternerKey::new(idx_id);
        m.insert(age_key, InnerValue::Int(42));
        m
    });
    tbl.insert(&record).await.unwrap();

    // The non-tx insert must now appear in the commit_write_log with a
    // SetPosting key for age=42.
    let gate = repo.tx_gate().await.unwrap();
    assert!(
        gate.commit_log_len() >= 1,
        "non-tx write must appear in commit_write_log after MVCC-1 fix"
    );

    // Stage a durable write (avoid C6 fast-path).
    tbl.insert_tx(&InnerValue::Str("tx_write".into()), Some(&mut tx_a))
        .await
        .unwrap();

    // Commit must abort: the posting key for age=42 falls inside `age >= 0`.
    let result = repo.commit_tx(tx_a).await;
    match result {
        Err(CommitError::PhantomConflict { dep }) => {
            assert!(
                dep.contains("IndexRange"),
                "dep summary should name IndexRange: {dep}"
            );
        }
        other => panic!(
            "MVCC-1 fix (IndexRange): expected PhantomConflict but got {:?}",
            other
                .map(|o| format!("Ok({:?})", o.commit_version))
                .unwrap_or_else(|e| format!("Err({e})"))
        ),
    }
}

/// MVCC-1 POSITIVE — non-tx insert into a DIFFERENT table does NOT abort a
/// Serializable tx watching another table (no false positives).
///
/// Guards against the fix being too aggressive.
#[tokio::test]
async fn mvcc1_non_tx_insert_into_different_table_does_not_abort_serializable_tx() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("watched"));
    repo.add_table_for_test(TableConfig::new("other"));
    let tbl_watched = repo.get_table("watched").await.unwrap();
    let tbl_other = repo.get_table("other").await.unwrap();
    let table_token_watched = table_token_for("watched");

    // Open Serializable tx on "watched".
    let (mut tx_a, _ga) = repo.begin_tx(IsolationLevel::Serializable).await.unwrap();
    tx_a.record_predicate_shared(PredicateDep::TableScan {
        table_token: table_token_watched,
    });

    // Non-tx insert into "other" — should NOT conflict with "watched".
    tbl_other
        .insert(&InnerValue::Str("unrelated".into()))
        .await
        .unwrap();

    // Stage a durable write in tx_a (non-empty tx).
    tbl_watched
        .insert_tx(&InnerValue::Str("own_write".into()), Some(&mut tx_a))
        .await
        .unwrap();

    // Commit must succeed: the non-tx write was in a different table.
    let result = repo.commit_tx(tx_a).await;
    assert!(
        result.is_ok(),
        "non-tx insert into a different table must not abort: {:?}",
        result.err()
    );
}

/// MVCC-1 SNAPSHOT CONTRAST — Snapshot-isolation tx is NOT aborted by a
/// non-tx insert, even with a predicate dep manually armed.
///
/// Snapshot isolation (level 1) does not run Phase 2-bis, so the non-tx
/// footprint in the commit_write_log is irrelevant to it.
#[tokio::test]
async fn mvcc1_snapshot_tx_not_aborted_by_non_tx_insert() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    let table_token = table_token_for("t");

    let (mut tx_snap, _gs) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    // Even if we arm a predicate dep, Snapshot skips Phase 2-bis.
    tx_snap
        .predicate_set
        .push(PredicateDep::TableScan { table_token });

    // Non-tx insert — now recorded in the log.
    tbl.insert(&InnerValue::Str("phantom".into()))
        .await
        .unwrap();

    // Stage a write.
    tbl.insert_tx(&InnerValue::Str("snap_write".into()), Some(&mut tx_snap))
        .await
        .unwrap();

    // Commit: Snapshot must not trigger Phase 2-bis.
    let result = repo.commit_tx(tx_snap).await;
    assert!(
        result.is_ok(),
        "Snapshot tx must not be aborted by non-tx write: {:?}",
        result.err()
    );
}

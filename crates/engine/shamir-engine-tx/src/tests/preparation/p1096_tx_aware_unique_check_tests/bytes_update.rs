use shamir_tx::IsolationLevel;
use shamir_types::types::value::InnerValue;

use crate::table::TableConfig;

use super::support::{key_id, make_repo, record_with_str, record_with_two_str};

/// #1096 follow-up (found by `@oh` review): `insert_tx_many_bytes` is the
/// actual wire path `execute_insert_tx`/`execute_set_tx` call for every
/// transactional INSERT/UPSERT — it must get the same tx-aware
/// release-then-reclaim treatment as the direct `insert_tx` API.
#[tokio::test]
async fn insert_tx_many_bytes_after_delete_reclaim_succeeds() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    tbl.create_unique_index("by_email", &["email"])
        .await
        .unwrap();
    let email_id = key_id(&tbl, "by_email").await;
    let email_field = key_id(&tbl, "email").await;

    let (mut tx1, _g1) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let rid_a = tbl
        .insert_tx(&record_with_str(email_field, "x"), Some(&mut tx1))
        .await
        .expect("INSERT A must succeed");
    repo.commit_tx(tx1).await.expect("commit must succeed");

    let (mut tx2, _g2) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    tbl.delete_tx(rid_a, Some(&mut tx2))
        .await
        .expect("DELETE A must succeed");

    let b_bytes = record_with_str(email_field, "x")
        .to_bytes()
        .expect("encode succeeds");
    let ids = tbl
        .insert_tx_many_bytes(&[b_bytes], &mut tx2)
        .await
        .expect("insert_tx_many_bytes must succeed - key was released by DELETE A");
    assert_eq!(ids.len(), 1);

    repo.commit_tx(tx2).await.expect("commit must succeed");

    let owner = tbl
        .index_manager()
        .lookup_by_unique_index(email_id, &[InnerValue::Str("x".into())])
        .await
        .unwrap();
    assert_eq!(owner, Some(ids[0]));
}

/// #1096 follow-up (found by `@oh` review): the RECLAIMING half of a
/// release-then-reclaim pair can also be an UPDATE, not just an INSERT —
/// `tx: DELETE A {email:"x"}; UPDATE C SET email="x"` must succeed exactly
/// like the INSERT-reclaims-a-released-key case.
#[tokio::test]
async fn tx_update_reclaims_a_key_released_by_delete_succeeds() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    tbl.create_unique_index("by_email", &["email"])
        .await
        .unwrap();
    let email_id = key_id(&tbl, "by_email").await;
    let email_field = key_id(&tbl, "email").await;
    let name_field = key_id(&tbl, "name").await;

    let (mut tx1, _g1) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let rid_a = tbl
        .insert_tx(&record_with_str(email_field, "x"), Some(&mut tx1))
        .await
        .expect("INSERT A must succeed");
    let rid_c = tbl
        .insert_tx(
            &record_with_two_str(email_field, "y", name_field, "carol"),
            Some(&mut tx1),
        )
        .await
        .expect("INSERT C must succeed");
    repo.commit_tx(tx1).await.expect("commit must succeed");

    let (mut tx2, _g2) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    tbl.delete_tx(rid_a, Some(&mut tx2))
        .await
        .expect("DELETE A must succeed");
    tbl.update_tx(
        rid_c,
        &record_with_two_str(email_field, "x", name_field, "carol"),
        Some(&mut tx2),
    )
    .await
    .expect("UPDATE C to reclaim 'x' must succeed - key was released by DELETE A");

    repo.commit_tx(tx2).await.expect("commit must succeed");

    let owner = tbl
        .index_manager()
        .lookup_by_unique_index(email_id, &[InnerValue::Str("x".into())])
        .await
        .unwrap();
    assert_eq!(owner, Some(rid_c), "C must now own the reclaimed value 'x'");
}

/// #1096 follow-up (found by a third `@oh` review): no test previously
/// asserted a genuine (non-released) `DuplicateKey` rejection out of an
/// UPDATE path — `update_tx`'s `Some(old_val)` branch
/// (`validate_unique_for_update_with_released`) had zero coverage for its
/// non-tolerance branch, so a bug that wrongly tolerated ANY durable
/// conflict on that path would have gone undetected by this module.
#[tokio::test]
async fn tx_update_to_a_durably_owned_unreleased_key_still_rejects() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    tbl.create_unique_index("by_email", &["email"])
        .await
        .unwrap();
    let email_field = key_id(&tbl, "email").await;

    let (mut tx1, _g1) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let _rid_a = tbl
        .insert_tx(&record_with_str(email_field, "x"), Some(&mut tx1))
        .await
        .expect("INSERT A must succeed");
    let rid_d = tbl
        .insert_tx(&record_with_str(email_field, "other"), Some(&mut tx1))
        .await
        .expect("INSERT D must succeed");
    repo.commit_tx(tx1).await.expect("commit must succeed");

    // tx2: UPDATE D SET email="x" — "x" is durably owned by A, and tx2
    // never released it (no DELETE/UPDATE-off of A in this tx) — a
    // genuine conflict that must still reject.
    let (mut tx2, _g2) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let result = tbl
        .update_tx(rid_d, &record_with_str(email_field, "x"), Some(&mut tx2))
        .await;
    assert!(
        matches!(result, Err(shamir_storage::error::DbError::DuplicateKey(_))),
        "UPDATE D to a durably-owned, never-released key must reject; got {:?}",
        result
    );
}

/// Same scenario as
/// [`tx_update_to_a_durably_owned_unreleased_key_still_rejects`], but
/// exercised through `update_tx_bytes` — the wire UPDATE path
/// `execute_update_tx` actually calls, and the one call site the third
/// `@oh` review found had zero exercise anywhere in this module.
#[tokio::test]
async fn update_tx_bytes_to_a_durably_owned_unreleased_key_still_rejects() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    tbl.create_unique_index("by_email", &["email"])
        .await
        .unwrap();
    let email_field = key_id(&tbl, "email").await;

    let (mut tx1, _g1) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let _rid_a = tbl
        .insert_tx(&record_with_str(email_field, "x"), Some(&mut tx1))
        .await
        .expect("INSERT A must succeed");
    let rid_d = tbl
        .insert_tx(&record_with_str(email_field, "other"), Some(&mut tx1))
        .await
        .expect("INSERT D must succeed");
    repo.commit_tx(tx1).await.expect("commit must succeed");

    let (mut tx2, _g2) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let old_bytes = record_with_str(email_field, "other")
        .to_bytes()
        .expect("encode succeeds");
    let new_bytes = record_with_str(email_field, "x")
        .to_bytes()
        .expect("encode succeeds");
    let result = tbl
        .update_tx_bytes(rid_d, &old_bytes, new_bytes, &mut tx2, false)
        .await;
    assert!(
        matches!(result, Err(shamir_storage::error::DbError::DuplicateKey(_))),
        "update_tx_bytes to a durably-owned, never-released key must reject; got {:?}",
        result
    );
}

/// #1104 - DELETE-then-reclaim via `update_tx_bytes`:
/// tx: delete_tx(A{email:"x"}) then update_tx_bytes(D, ..., email="x")
/// in the SAME tx must succeed (release-then-reclaim via the wire UPDATE path).
#[tokio::test]
async fn update_tx_bytes_after_delete_reclaim_succeeds() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    tbl.create_unique_index("by_email", &["email"])
        .await
        .unwrap();
    let email_id = key_id(&tbl, "by_email").await;
    let email_field = key_id(&tbl, "email").await;

    // Insert a durable record with email="x"
    let (mut tx1, _g1) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let rid_a = tbl
        .insert_tx(&record_with_str(email_field, "x"), Some(&mut tx1))
        .await
        .expect("INSERT A must succeed");
    repo.commit_tx(tx1).await.expect("commit must succeed");

    // Verify A is in the unique index
    let owner = tbl
        .index_manager()
        .lookup_by_unique_index(email_id, &[InnerValue::Str("x".into())])
        .await
        .unwrap();
    assert_eq!(
        owner,
        Some(rid_a),
        "A must own the unique value 'x' after tx1 commits"
    );

    // New tx: DELETE A, then update_tx_bytes D with same email
    let (mut tx2, _g2) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();

    // DELETE A - this stages a RemovePosting for the unique key
    tbl.delete_tx(rid_a, Some(&mut tx2))
        .await
        .expect("DELETE A must succeed");

    // Insert a second durable record D with a different email
    let (mut tx0, _g0) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let rid_d = tbl
        .insert_tx(&record_with_str(email_field, "other"), Some(&mut tx0))
        .await
        .expect("INSERT D must succeed");
    repo.commit_tx(tx0).await.expect("commit must succeed");

    // Now in tx2, update_tx_bytes D to claim email="x" - should succeed
    // because tx2 itself released "x" by deleting A
    let old_bytes = record_with_str(email_field, "other")
        .to_bytes()
        .expect("encode succeeds");
    let new_bytes = record_with_str(email_field, "x")
        .to_bytes()
        .expect("encode succeeds");
    tbl.update_tx_bytes(rid_d, &old_bytes, new_bytes, &mut tx2, false)
        .await
        .expect("update_tx_bytes must succeed - key 'x' was released by DELETE A");

    // Commit tx2
    repo.commit_tx(tx2).await.expect("commit must succeed");

    // Verify D now owns the unique value 'x'
    let owner = tbl
        .index_manager()
        .lookup_by_unique_index(email_id, &[InnerValue::Str("x".into())])
        .await
        .unwrap();
    assert_eq!(
        owner,
        Some(rid_d),
        "D must own the unique value 'x' after tx2 commits"
    );

    // Verify A is gone
    let a_gone_result = tbl.get(rid_a).await;
    assert!(
        a_gone_result.is_err(),
        "A must be deleted from the data store"
    );
}

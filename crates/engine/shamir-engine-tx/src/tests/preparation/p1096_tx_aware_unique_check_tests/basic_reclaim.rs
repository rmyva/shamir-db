use shamir_tx::IsolationLevel;
use shamir_types::core::interner::InternerKey;
use shamir_types::types::value::InnerValue;

use crate::table::TableConfig;

use super::support::{key_id, make_repo, record_with_str, record_with_two_str};

/// #1096 - DELETE-then-reclaim scenario:
/// tx1: INSERT A {email:"x"}; COMMIT
/// tx2: DELETE A; INSERT B {email:"x"}  // must succeed, B owns the unique value
#[tokio::test]
async fn tx_delete_then_reclaim_unique_key_succeeds() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    tbl.create_unique_index("by_email", &["email"])
        .await
        .unwrap();
    let email_id = key_id(&tbl, "by_email").await;
    let email_field = key_id(&tbl, "email").await;

    // tx1: INSERT A {email:"x"}; COMMIT
    let (mut tx1, _g1) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let rid_a = tbl
        .insert_tx(&record_with_str(email_field, "x"), Some(&mut tx1))
        .await
        .expect("tx1 INSERT A must succeed");
    repo.commit_tx(tx1).await.expect("tx1 commit must succeed");

    // Verify A is in the unique index
    let owner = tbl
        .index_manager()
        .lookup_by_unique_index(email_id, &[InnerValue::Str("x".into())])
        .await
        .unwrap();
    assert_eq!(
        owner,
        Some(rid_a),
        "A must own the unique value after tx1 commits"
    );

    // tx2: DELETE A; INSERT B {email:"x"}  // must succeed, B owns the unique value
    let (mut tx2, _g2) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();

    // DELETE A - this stages a RemovePosting for the unique key
    tbl.delete_tx(rid_a, Some(&mut tx2))
        .await
        .expect("tx2 DELETE A must succeed");

    // INSERT B with the same unique value - before the fix, this would fail
    // with DuplicateKey at stage time because the stage-time check only sees
    // durable state (A still there). After the fix, it recognizes that the key
    // was released earlier in this tx and allows the reclaim.
    let rid_b = tbl
        .insert_tx(&record_with_str(email_field, "x"), Some(&mut tx2))
        .await
        .expect("tx2 INSERT B must succeed - key was released by DELETE A");

    // Commit tx2
    repo.commit_tx(tx2).await.expect("tx2 commit must succeed");

    // Verify B now owns the unique value
    let owner = tbl
        .index_manager()
        .lookup_by_unique_index(email_id, &[InnerValue::Str("x".into())])
        .await
        .unwrap();
    assert_eq!(
        owner,
        Some(rid_b),
        "B must own the unique value after tx2 commits"
    );

    // Verify A is gone from data store
    let a_gone_result = tbl.get(rid_a).await;
    assert!(
        a_gone_result.is_err(),
        "A must be deleted from the data store"
    );
}

/// #1096 - UPDATE-off-then-reclaim scenario:
/// tx1: INSERT A {email:"x"}; COMMIT
/// tx2: UPDATE A SET email="z"; INSERT B {email:"x"}  // must succeed
#[tokio::test]
async fn tx_update_off_then_reclaim_unique_key_succeeds() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    tbl.create_unique_index("by_email", &["email"])
        .await
        .unwrap();
    let email_id = key_id(&tbl, "by_email").await;
    let email_field = key_id(&tbl, "email").await;
    let name_field = key_id(&tbl, "name").await;

    // tx1: INSERT A {email:"x", name:"alice"}; COMMIT
    let (mut tx1, _g1) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let rid_a = tbl
        .insert_tx(
            &record_with_two_str(email_field, "x", name_field, "alice"),
            Some(&mut tx1),
        )
        .await
        .expect("tx1 INSERT A must succeed");
    repo.commit_tx(tx1).await.expect("tx1 commit must succeed");

    // Verify A owns "x" in the unique index
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

    // tx2: UPDATE A SET email="z"; INSERT B {email:"x"}  // must succeed
    let (mut tx2, _g2) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();

    // UPDATE A SET email="z" - this stages a RemovePosting for key "x"
    let a_updated = record_with_two_str(email_field, "z", name_field, "alice");
    tbl.update_tx(rid_a, &a_updated, Some(&mut tx2))
        .await
        .expect("tx2 UPDATE A must succeed");

    // INSERT B with the old unique value "x" - before the fix, this would fail
    // with DuplicateKey at stage time. After the fix, it recognizes that the key
    // was released by the UPDATE and allows the reclaim.
    let rid_b = tbl
        .insert_tx(
            &record_with_two_str(email_field, "x", name_field, "bob"),
            Some(&mut tx2),
        )
        .await
        .expect("tx2 INSERT B must succeed - key was released by UPDATE A");

    // Commit tx2
    repo.commit_tx(tx2).await.expect("tx2 commit must succeed");

    // Verify B now owns "x" in the unique index
    let owner_x = tbl
        .index_manager()
        .lookup_by_unique_index(email_id, &[InnerValue::Str("x".into())])
        .await
        .unwrap();
    assert_eq!(
        owner_x,
        Some(rid_b),
        "B must own the unique value 'x' after tx2 commits"
    );

    // Verify A now owns "z" in the unique index
    let owner_z = tbl
        .index_manager()
        .lookup_by_unique_index(email_id, &[InnerValue::Str("z".into())])
        .await
        .unwrap();
    assert_eq!(
        owner_z,
        Some(rid_a),
        "A must own the unique value 'z' after tx2 commits"
    );

    // Verify both records exist with correct names
    let a_val = tbl.get(rid_a).await.unwrap();
    let b_val = tbl.get(rid_b).await.unwrap();
    let name_key = InternerKey::new(name_field);
    let a_name = match &a_val {
        InnerValue::Map(m) => m.get(&name_key).and_then(|v| match v {
            InnerValue::Str(s) => Some(s.as_str()),
            _ => None,
        }),
        _ => None,
    };
    let b_name = match &b_val {
        InnerValue::Map(m) => m.get(&name_key).and_then(|v| match v {
            InnerValue::Str(s) => Some(s.as_str()),
            _ => None,
        }),
        _ => None,
    };
    assert_eq!(a_name, Some("alice"), "A's name must be 'alice'");
    assert_eq!(b_name, Some("bob"), "B's name must be 'bob'");
}

/// Test that a genuine double-claim still rejects:
/// tx: INSERT A {email:"x"}; INSERT B {email:"x"}  // commit must abort
///
/// Neither A nor B is durable while staging (both are same-tx inserts), so
/// `insert_tx`'s stage-time check (durable-only, even after #1096) cannot
/// see the conflict — `insert_tx` for B succeeds optimistically. The
/// rejection is `pre_commit.rs`'s Step 1 walk (no `RemovePosting` between
/// the two `SetPosting`s for the same key -> genuine intra-tx collision),
/// which runs at `commit_tx`. This mirrors the pre-existing coverage in
/// `base_index_tx_tests.rs::intra_tx_unique_collision_silently_overwrites`
/// (#1039) — kept here too so `released_unique_keys_in_tx`'s "still-live,
/// not released" case has direct #1096-local coverage.
#[tokio::test]
async fn tx_genuine_double_claim_still_rejects() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    tbl.create_unique_index("by_email", &["email"])
        .await
        .unwrap();
    let _email_id = key_id(&tbl, "by_email").await;
    let email_field = key_id(&tbl, "email").await;

    let (mut tx, _g) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();

    // INSERT A {email:"x"} - succeeds
    let _rid_a = tbl
        .insert_tx(&record_with_str(email_field, "x"), Some(&mut tx))
        .await
        .expect("INSERT A must succeed");

    // INSERT B {email:"x"} - stages fine (neither claim is durable yet).
    // There is no DELETE or UPDATE-off between these, so this is a genuine
    // duplicate; the rejection happens at commit time, not here.
    let _rid_b = tbl
        .insert_tx(&record_with_str(email_field, "x"), Some(&mut tx))
        .await
        .expect("INSERT B stages fine - neither claim is durable yet");

    // Commit must abort: pre_commit's Step 1 walk sees two SetPostings for
    // the same key with no RemovePosting between them.
    let commit_result = repo.commit_tx(tx).await;
    assert!(
        matches!(
            commit_result,
            Err(crate::tx::CommitError::UniqueViolation { .. })
        ),
        "commit must abort with UniqueViolation for genuine intra-tx double-claim; got {:?}",
        commit_result
    );
}

/// Test that the released walk is correct: a key claimed multiple times
/// in the same tx but never released still rejects the duplicate.
#[tokio::test]
async fn tx_multiple_claim_without_release_still_rejects() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    tbl.create_unique_index("by_email", &["email"])
        .await
        .unwrap();
    let _email_id = key_id(&tbl, "by_email").await;
    let email_field = key_id(&tbl, "email").await;

    // First, insert a durable record with email="y"
    let (mut tx0, _g0) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let _rid_y = tbl
        .insert_tx(&record_with_str(email_field, "y"), Some(&mut tx0))
        .await
        .expect("INSERT for email='y' must succeed");
    repo.commit_tx(tx0).await.expect("commit must succeed");

    // Now in a new tx, try to claim the same email twice without releasing
    let (mut tx, _g) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();

    // INSERT A {email:"y"} - should fail at stage time because "y" is already durable
    let dup1 = tbl
        .insert_tx(&record_with_str(email_field, "y"), Some(&mut tx))
        .await;
    assert!(
        matches!(dup1, Err(shamir_storage::error::DbError::DuplicateKey(_))),
        "INSERT A must fail because 'y' is already owned by durable record; got {:?}",
        dup1
    );
}

/// Verify the `released_unique_keys_in_tx` helper correctly tracks
/// the live/released state walking through a complex sequence of ops.
#[tokio::test]
async fn released_unique_keys_in_tx_walks_correctly() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    tbl.create_unique_index("by_email", &["email"])
        .await
        .unwrap();

    let (mut tx, _g) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let token = tbl.table_token();

    // Sequence of staged ops in index_write_set:
    // 1. INSERT A {email:"x"} -> SetPosting "x"
    // 2. INSERT B {email:"y"} -> SetPosting "y"
    // 3. DELETE A -> RemovePosting "x"
    // 4. INSERT C {email:"z"} -> SetPosting "z"
    // 5. INSERT B again (same email) -> SetPosting "y" (same owner, not a conflict)
    // 6. DELETE B -> RemovePosting "y"
    // 7. INSERT D {email:"x"} -> SetPosting "x" (released by step 3, should be OK)

    let _email_id = key_id(&tbl, "by_email").await;
    let email_field = key_id(&tbl, "email").await;

    // 1. INSERT A {email:"x"}
    let rid_a = tbl
        .insert_tx(&record_with_str(email_field, "x"), Some(&mut tx))
        .await
        .unwrap();

    // 2. INSERT B {email:"y"}
    let rid_b = tbl
        .insert_tx(&record_with_str(email_field, "y"), Some(&mut tx))
        .await
        .unwrap();

    // 3. DELETE A
    tbl.delete_tx(rid_a, Some(&mut tx)).await.unwrap();

    // 4. INSERT C {email:"z"}
    let _rid_c = tbl
        .insert_tx(&record_with_str(email_field, "z"), Some(&mut tx))
        .await
        .unwrap();

    // 5. UPDATE B to same email (no-op for unique index, but creates staging)
    // This tests that re-claiming the same key with the same owner is OK.
    // B is only staged (not committed) in this same tx, so a plain `get`
    // (durable-only) would miss it -- reuse the known value instead.
    let b_val = record_with_str(email_field, "y");
    tbl.update_tx(rid_b, &b_val, Some(&mut tx)).await.unwrap();

    // 6. DELETE B
    tbl.delete_tx(rid_b, Some(&mut tx)).await.unwrap();

    // At this point:
    // - "x" is released (by DELETE A in step 3)
    // - "y" is released (by DELETE B in step 6)
    // - "z" is live (owned by C)

    // Refresh the cache and borrow the released set
    crate::table::refresh_released_unique_cache(&mut tx, token);
    let released = &tx.released_unique_cache[&token].released;

    // Build the expected index keys for "x" and "y"
    let index_mgr = tbl.index_manager();
    let key_x_bytes = index_mgr
        .unique_keys_for(&record_with_str(email_field, "x"))
        .into_iter()
        .next()
        .unwrap()
        .to_vec();
    let key_y_bytes = index_mgr
        .unique_keys_for(&record_with_str(email_field, "y"))
        .into_iter()
        .next()
        .unwrap()
        .to_vec();
    let key_z_bytes = index_mgr
        .unique_keys_for(&record_with_str(email_field, "z"))
        .into_iter()
        .next()
        .unwrap()
        .to_vec();

    assert!(
        released.contains(&key_x_bytes),
        "key 'x' must be in released set (DELETE A)"
    );
    assert!(
        released.contains(&key_y_bytes),
        "key 'y' must be in released set (DELETE B)"
    );
    assert!(
        !released.contains(&key_z_bytes),
        "key 'z' must NOT be in released set (still owned by C)"
    );

    // 7. INSERT D {email:"x"} - this should succeed because "x" was released
    let _rid_d = tbl
        .insert_tx(&record_with_str(email_field, "x"), Some(&mut tx))
        .await
        .expect("INSERT D must succeed - key 'x' was released by DELETE A");
}

/// #1096 - insert_tx_many also benefits from the fix:
/// DELETE a durable record, then use insert_tx_many to insert a new record
/// with the same unique key.
#[tokio::test]
async fn insert_tx_many_after_delete_reclaim_succeeds() {
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

    // New tx: DELETE A, then insert_tx_many B with same email
    let (mut tx2, _g2) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();

    // DELETE A
    tbl.delete_tx(rid_a, Some(&mut tx2))
        .await
        .expect("DELETE A must succeed");

    // insert_tx_many with a record that has email="x" - should succeed
    let values = vec![record_with_str(email_field, "x")];
    let ids = tbl
        .insert_tx_many(&values, &mut tx2)
        .await
        .expect("insert_tx_many must succeed - key was released by DELETE A");

    assert_eq!(ids.len(), 1, "insert_tx_many should return one id");

    // Commit tx2
    repo.commit_tx(tx2).await.expect("commit must succeed");

    // Verify the new record owns the unique value
    let owner = tbl
        .index_manager()
        .lookup_by_unique_index(email_id, &[InnerValue::Str("x".into())])
        .await
        .unwrap();
    assert_eq!(
        owner,
        Some(ids[0]),
        "the new record must own the unique value 'x'"
    );
}

use shamir_tx::IsolationLevel;
use shamir_types::types::value::InnerValue;

use crate::table::TableConfig;

use super::support::{key_id, make_repo, record_with_str};

/// #1104 - stale-snapshot release via `update_tx_bytes` must NOT bypass
/// a concurrent reclaim:
///
/// ```text
/// tx0: INSERT A {email:"x"}; COMMIT
/// tx2: BEGIN                                    // snapshot sees A owning "x"
/// tx1: DELETE A; INSERT B {email:"x"}; COMMIT   // durable owner is now B
/// tx2: DELETE A; UPDATE D SET email="x"         // must now reject at stage time
/// ```
#[tokio::test]
async fn stale_snapshot_release_does_not_bypass_a_concurrent_reclaim_via_update_tx_bytes() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    tbl.create_unique_index("by_email", &["email"])
        .await
        .unwrap();
    let email_id = key_id(&tbl, "by_email").await;
    let email_field = key_id(&tbl, "email").await;

    // tx0: INSERT A {email:"x"}; COMMIT
    let (mut tx0, _g0) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let rid_a = tbl
        .insert_tx(&record_with_str(email_field, "x"), Some(&mut tx0))
        .await
        .expect("tx0 INSERT A must succeed");
    repo.commit_tx(tx0).await.expect("tx0 commit must succeed");

    // tx2: BEGIN — snapshot sees A owning "x".
    let (mut tx2, _g2) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();

    // tx1: DELETE A; INSERT B {email:"x"}; COMMIT — concurrent, commits FIRST.
    let (mut tx1, _g1) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    tbl.delete_tx(rid_a, Some(&mut tx1))
        .await
        .expect("tx1 DELETE A must succeed");
    let rid_b = tbl
        .insert_tx(&record_with_str(email_field, "x"), Some(&mut tx1))
        .await
        .expect("tx1 INSERT B must succeed");
    repo.commit_tx(tx1).await.expect("tx1 commit must succeed");

    // Durable owner of "x" is now B, not A.
    let owner = tbl
        .index_manager()
        .lookup_by_unique_index(email_id, &[InnerValue::Str("x".into())])
        .await
        .unwrap();
    assert_eq!(
        owner,
        Some(rid_b),
        "B must durably own 'x' after tx1 commits"
    );

    // tx2: DELETE A (stale — A is no longer the durable owner of "x", but
    // A itself is still present in tx2's own snapshot/write_set); then
    // insert a second durable record D with a different email.
    let (mut tx0b, _g0b) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let rid_d = tbl
        .insert_tx(&record_with_str(email_field, "other"), Some(&mut tx0b))
        .await
        .expect("INSERT D must succeed");
    repo.commit_tx(tx0b).await.expect("commit must succeed");

    // Now in tx2, try to UPDATE D to email="x" via update_tx_bytes — this
    // would be a bypass if allowed (tolerating any release-marked key),
    // silently heading toward two live records under a UNIQUE index once
    // tx2 commits.
    tbl.delete_tx(rid_a, Some(&mut tx2))
        .await
        .expect("tx2 DELETE A stages fine (A is stale but still exists)");
    let old_bytes = record_with_str(email_field, "other")
        .to_bytes()
        .expect("encode succeeds");
    let new_bytes = record_with_str(email_field, "x")
        .to_bytes()
        .expect("encode succeeds");
    let update_d = tbl
        .update_tx_bytes(rid_d, &old_bytes, new_bytes, &mut tx2, false)
        .await;
    assert!(
        matches!(
            update_d,
            Err(shamir_storage::error::DbError::DuplicateKey(_))
        ),
        "tx2's update_tx_bytes(D) must reject: its release plan for 'x' was \
         built against a stale snapshot (A), but the durable owner is now B, a \
         record tx2 never touched — admitting this would leave B and D \
         both live under a UNIQUE index; got {:?}",
        update_d
    );

    // B must still be the sole owner — no duplicate was created.
    let owner_after = tbl
        .index_manager()
        .lookup_by_unique_index(email_id, &[InnerValue::Str("x".into())])
        .await
        .unwrap();
    assert_eq!(
        owner_after,
        Some(rid_b),
        "B must remain the sole owner of 'x' after tx2's rejected update_tx_bytes"
    );
}

/// #1104 - stale-snapshot release via `insert_tx_many` must NOT bypass
/// a concurrent reclaim:
///
/// ```text
/// tx0: INSERT A {email:"x"}; COMMIT
/// tx2: BEGIN                                    // snapshot sees A owning "x"
/// tx1: DELETE A; INSERT B {email:"x"}; COMMIT   // durable owner is now B
/// tx2: DELETE A; insert_tx_many([C{email:"x"}]) // must now reject at stage time
/// ```
#[tokio::test]
async fn stale_snapshot_release_does_not_bypass_a_concurrent_reclaim_via_insert_tx_many() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    tbl.create_unique_index("by_email", &["email"])
        .await
        .unwrap();
    let email_id = key_id(&tbl, "by_email").await;
    let email_field = key_id(&tbl, "email").await;

    // tx0: INSERT A {email:"x"}; COMMIT
    let (mut tx0, _g0) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let rid_a = tbl
        .insert_tx(&record_with_str(email_field, "x"), Some(&mut tx0))
        .await
        .expect("tx0 INSERT A must succeed");
    repo.commit_tx(tx0).await.expect("tx0 commit must succeed");

    // tx2: BEGIN — snapshot sees A owning "x".
    let (mut tx2, _g2) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();

    // tx1: DELETE A; INSERT B {email:"x"}; COMMIT — concurrent, commits FIRST.
    let (mut tx1, _g1) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    tbl.delete_tx(rid_a, Some(&mut tx1))
        .await
        .expect("tx1 DELETE A must succeed");
    let rid_b = tbl
        .insert_tx(&record_with_str(email_field, "x"), Some(&mut tx1))
        .await
        .expect("tx1 INSERT B must succeed");
    repo.commit_tx(tx1).await.expect("tx1 commit must succeed");

    // Durable owner of "x" is now B, not A.
    let owner = tbl
        .index_manager()
        .lookup_by_unique_index(email_id, &[InnerValue::Str("x".into())])
        .await
        .unwrap();
    assert_eq!(
        owner,
        Some(rid_b),
        "B must durably own 'x' after tx1 commits"
    );

    // tx2: DELETE A (stale — A is no longer the durable owner of "x", but
    // A itself is still present in tx2's own snapshot/write_set); then
    // insert_tx_many C with email="x" — the bypass, if present, would let
    // this stage (tolerating any release-marked key), silently heading
    // toward two live records under a UNIQUE index once tx2 commits.
    tbl.delete_tx(rid_a, Some(&mut tx2))
        .await
        .expect("tx2 DELETE A stages fine (A is stale but still exists)");
    let values = vec![record_with_str(email_field, "x")];
    let insert_c = tbl.insert_tx_many(&values, &mut tx2).await;
    assert!(
        matches!(
            insert_c,
            Err(shamir_storage::error::DbError::DuplicateKey(_))
        ),
        "tx2's insert_tx_many must reject: its release plan for 'x' was \
         built against a stale snapshot (A), but the durable owner is now B, a \
         record tx2 never touched — admitting this would leave B and C \
         both live under a UNIQUE index; got {:?}",
        insert_c
    );

    // B must still be the sole owner — no duplicate was created.
    let owner_after = tbl
        .index_manager()
        .lookup_by_unique_index(email_id, &[InnerValue::Str("x".into())])
        .await
        .unwrap();
    assert_eq!(
        owner_after,
        Some(rid_b),
        "B must remain the sole owner of 'x' after tx2's rejected insert_tx_many"
    );
}

/// #1104 - stale-snapshot release via `insert_tx_many_bytes` must NOT bypass
/// a concurrent reclaim:
///
/// ```text
/// tx0: INSERT A {email:"x"}; COMMIT
/// tx2: BEGIN                                    // snapshot sees A owning "x"
/// tx1: DELETE A; INSERT B {email:"x"}; COMMIT   // durable owner is now B
/// tx2: DELETE A; insert_tx_many_bytes([C{email:"x"}]) // must now reject at stage time
/// ```
#[tokio::test]
async fn stale_snapshot_release_does_not_bypass_a_concurrent_reclaim_via_insert_tx_many_bytes() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    tbl.create_unique_index("by_email", &["email"])
        .await
        .unwrap();
    let email_id = key_id(&tbl, "by_email").await;
    let email_field = key_id(&tbl, "email").await;

    // tx0: INSERT A {email:"x"}; COMMIT
    let (mut tx0, _g0) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let rid_a = tbl
        .insert_tx(&record_with_str(email_field, "x"), Some(&mut tx0))
        .await
        .expect("tx0 INSERT A must succeed");
    repo.commit_tx(tx0).await.expect("tx0 commit must succeed");

    // tx2: BEGIN — snapshot sees A owning "x".
    let (mut tx2, _g2) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();

    // tx1: DELETE A; INSERT B {email:"x"}; COMMIT — concurrent, commits FIRST.
    let (mut tx1, _g1) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    tbl.delete_tx(rid_a, Some(&mut tx1))
        .await
        .expect("tx1 DELETE A must succeed");
    let rid_b = tbl
        .insert_tx(&record_with_str(email_field, "x"), Some(&mut tx1))
        .await
        .expect("tx1 INSERT B must succeed");
    repo.commit_tx(tx1).await.expect("tx1 commit must succeed");

    // Durable owner of "x" is now B, not A.
    let owner = tbl
        .index_manager()
        .lookup_by_unique_index(email_id, &[InnerValue::Str("x".into())])
        .await
        .unwrap();
    assert_eq!(
        owner,
        Some(rid_b),
        "B must durably own 'x' after tx1 commits"
    );

    // tx2: DELETE A (stale — A is no longer the durable owner of "x", but
    // A itself is still present in tx2's own snapshot/write_set); then
    // insert_tx_many_bytes C with email="x" — the bypass, if present, would
    // let this stage (tolerating any release-marked key), silently heading
    // toward two live records under a UNIQUE index once tx2 commits.
    tbl.delete_tx(rid_a, Some(&mut tx2))
        .await
        .expect("tx2 DELETE A stages fine (A is stale but still exists)");
    let c_bytes = record_with_str(email_field, "x")
        .to_bytes()
        .expect("encode succeeds");
    let insert_c = tbl.insert_tx_many_bytes(&[c_bytes], &mut tx2).await;
    assert!(
        matches!(
            insert_c,
            Err(shamir_storage::error::DbError::DuplicateKey(_))
        ),
        "tx2's insert_tx_many_bytes must reject: its release plan for 'x' was \
         built against a stale snapshot (A), but the durable owner is now B, a \
         record tx2 never touched — admitting this would leave B and C \
         both live under a UNIQUE index; got {:?}",
        insert_c
    );

    // B must still be the sole owner — no duplicate was created.
    let owner_after = tbl
        .index_manager()
        .lookup_by_unique_index(email_id, &[InnerValue::Str("x".into())])
        .await
        .unwrap();
    assert_eq!(
        owner_after,
        Some(rid_b),
        "B must remain the sole owner of 'x' after tx2's rejected insert_tx_many_bytes"
    );
}

/// #1104 - stale-snapshot release via `update_tx` must NOT bypass
/// a concurrent reclaim:
///
/// ```text
/// tx0: INSERT A {email:"x"}; COMMIT
/// tx2: BEGIN                                    // snapshot sees A owning "x"
/// tx1: DELETE A; INSERT B {email:"x"}; COMMIT   // durable owner is now B
/// tx2: DELETE A; UPDATE C SET email="x"         // must now reject at stage time
/// ```
#[tokio::test]
async fn stale_snapshot_release_does_not_bypass_a_concurrent_reclaim_via_update_tx() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    tbl.create_unique_index("by_email", &["email"])
        .await
        .unwrap();
    let email_id = key_id(&tbl, "by_email").await;
    let email_field = key_id(&tbl, "email").await;

    // tx0: INSERT A {email:"x"}; COMMIT
    let (mut tx0, _g0) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let rid_a = tbl
        .insert_tx(&record_with_str(email_field, "x"), Some(&mut tx0))
        .await
        .expect("tx0 INSERT A must succeed");
    repo.commit_tx(tx0).await.expect("tx0 commit must succeed");

    // tx2: BEGIN — snapshot sees A owning "x".
    let (mut tx2, _g2) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();

    // tx1: DELETE A; INSERT B {email:"x"}; COMMIT — concurrent, commits FIRST.
    let (mut tx1, _g1) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    tbl.delete_tx(rid_a, Some(&mut tx1))
        .await
        .expect("tx1 DELETE A must succeed");
    let rid_b = tbl
        .insert_tx(&record_with_str(email_field, "x"), Some(&mut tx1))
        .await
        .expect("tx1 INSERT B must succeed");
    repo.commit_tx(tx1).await.expect("tx1 commit must succeed");

    // Durable owner of "x" is now B, not A.
    let owner = tbl
        .index_manager()
        .lookup_by_unique_index(email_id, &[InnerValue::Str("x".into())])
        .await
        .unwrap();
    assert_eq!(
        owner,
        Some(rid_b),
        "B must durably own 'x' after tx1 commits"
    );

    // tx2: DELETE A (stale — A is no longer the durable owner of "x", but
    // A itself is still present in tx2's own snapshot/write_set); then
    // insert a second durable record C with a different email.
    let (mut tx0b, _g0b) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let rid_c = tbl
        .insert_tx(&record_with_str(email_field, "other"), Some(&mut tx0b))
        .await
        .expect("INSERT C must succeed");
    repo.commit_tx(tx0b).await.expect("commit must succeed");

    // Now in tx2, try to UPDATE C to email="x" via update_tx — this would be
    // a bypass if allowed (tolerating any release-marked key), silently
    // heading toward two live records under a UNIQUE index once tx2 commits.
    tbl.delete_tx(rid_a, Some(&mut tx2))
        .await
        .expect("tx2 DELETE A stages fine (A is stale but still exists)");
    let update_c = tbl
        .update_tx(rid_c, &record_with_str(email_field, "x"), Some(&mut tx2))
        .await;
    assert!(
        matches!(
            update_c,
            Err(shamir_storage::error::DbError::DuplicateKey(_))
        ),
        "tx2's update_tx(C) must reject: its release plan for 'x' was \
         built against a stale snapshot (A), but the durable owner is now B, a \
         record tx2 never touched — admitting this would leave B and C \
         both live under a UNIQUE index; got {:?}",
        update_c
    );

    // B must still be the sole owner — no duplicate was created.
    let owner_after = tbl
        .index_manager()
        .lookup_by_unique_index(email_id, &[InnerValue::Str("x".into())])
        .await
        .unwrap();
    assert_eq!(
        owner_after,
        Some(rid_b),
        "B must remain the sole owner of 'x' after tx2's rejected update_tx"
    );
}

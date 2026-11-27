use shamir_tx::IsolationLevel;
use shamir_types::types::value::InnerValue;

use crate::table::TableConfig;

use super::support::{key_id, make_repo, record_with_str};

/// #1096 follow-up (found by `@oh` review): a stale-snapshot release plan
/// must NOT be tolerated when a CONCURRENT tx has already reclaimed the
/// durable key for an unrelated record. Without the `is_record_touched`
/// cross-check (#1099: an on-demand probe, formerly a pre-built
/// `touched_records_in_tx` set), `released_unique_keys_in_tx` alone would incorrectly treat
/// "this tx once planned a RemovePosting for this key" as sufficient,
/// silently admitting two live records for the same unique value.
///
/// By the time `tx2` stages its own reclaiming INSERT, the durable owner of
/// "x" has already changed to a record `tx2` never touched — the FIXED
/// stage-time check (not just the commit-time one) catches this immediately:
///
/// ```text
/// tx0: INSERT A {email:"x"}; COMMIT
/// tx2: BEGIN                                    // snapshot sees A owning "x"
/// tx1: DELETE A; INSERT B {email:"x"}; COMMIT   // durable owner is now B
/// tx2: DELETE A; INSERT C {email:"x"}           // must now reject at stage time
/// ```
#[tokio::test]
async fn stale_snapshot_release_does_not_bypass_a_concurrent_reclaim() {
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
    // A itself is still present in tx2's own snapshot/write_set); INSERT C
    // {email:"x"} — the bypass, if present, would let this stage
    // (tolerating any release-marked key), silently heading toward two live
    // records under a UNIQUE index once tx2 commits.
    tbl.delete_tx(rid_a, Some(&mut tx2))
        .await
        .expect("tx2 DELETE A stages fine (A is stale but still exists)");
    let insert_c = tbl
        .insert_tx(&record_with_str(email_field, "x"), Some(&mut tx2))
        .await;
    assert!(
        matches!(
            insert_c,
            Err(shamir_storage::error::DbError::DuplicateKey(_))
        ),
        "tx2's INSERT C must reject: its release plan for 'x' was built \
         against a stale snapshot (A), but the durable owner is now B, a \
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
        "B must remain the sole owner of 'x' after tx2's rejected INSERT"
    );
}

/// #1096 follow-up (found by `@oh` review): the SAME stale-snapshot bypass,
/// but arranged so the race lands strictly BETWEEN this tx's own (correctly
/// tolerated) stage-time check and its commit — exercising `pre_commit.rs`'s
/// Step 2 durable check specifically, not the stage-time one.
///
/// ```text
/// tx0: INSERT A {email:"x"}; COMMIT
/// tx2: BEGIN; DELETE A; INSERT C {email:"x"}   // stages fine: A IS the
///                                                  durable owner tx2 itself
///                                                  is releasing, no race yet
/// tx1: BEGIN; DELETE A; INSERT B {email:"x"}; COMMIT   // commits BEFORE tx2
/// tx2: COMMIT                                   // must now abort
/// ```
#[tokio::test]
async fn stale_snapshot_release_does_not_bypass_a_concurrent_reclaim_at_commit_time() {
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

    // tx2: DELETE A; INSERT C {email:"x"} — stages fine, no race has
    // happened yet (A is still the sole durable owner tx2 itself touched).
    let (mut tx2, _g2) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    tbl.delete_tx(rid_a, Some(&mut tx2))
        .await
        .expect("tx2 DELETE A must succeed");
    let _rid_c = tbl
        .insert_tx(&record_with_str(email_field, "x"), Some(&mut tx2))
        .await
        .expect("tx2 INSERT C must stage fine — no concurrent tx yet");

    // tx1: DELETE A; INSERT B {email:"x"}; COMMIT — races in and commits
    // BEFORE tx2, becoming the new durable owner of "x".
    let (mut tx1, _g1) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    tbl.delete_tx(rid_a, Some(&mut tx1))
        .await
        .expect("tx1 DELETE A must succeed");
    let rid_b = tbl
        .insert_tx(&record_with_str(email_field, "x"), Some(&mut tx1))
        .await
        .expect("tx1 INSERT B must succeed");
    repo.commit_tx(tx1).await.expect("tx1 commit must succeed");

    // tx2 commits last — its stage-time check never saw B, so this MUST be
    // caught by pre_commit.rs's Step 2 durable check instead.
    let commit_result = repo.commit_tx(tx2).await;
    assert!(
        matches!(
            commit_result,
            Err(crate::tx::CommitError::UniqueViolation { .. })
        ),
        "tx2 must abort at commit: the durable owner of 'x' is now B, a \
         record tx2 never touched — admitting this would leave B and C \
         both live under a UNIQUE index; got {:?}",
        commit_result
    );

    let owner_after = tbl
        .index_manager()
        .lookup_by_unique_index(email_id, &[InnerValue::Str("x".into())])
        .await
        .unwrap();
    assert_eq!(
        owner_after,
        Some(rid_b),
        "B must remain the sole owner of 'x' after tx2's aborted commit"
    );
}

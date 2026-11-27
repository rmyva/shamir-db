//! #1097: `RemovePosting` has no owner, can cancel a different record's live in-tx unique claim
//!
//! Tests the fix for the bug where `RemovePosting` operations carried no owner
//! field, allowing a stale removal (planned against a record that no longer owns
//! the key per the current transaction's walk) to incorrectly clear the live claim
//! of a DIFFERENT record that actually owns the key.

use std::sync::Arc;

use shamir_storage::storage_in_memory::InMemoryRepo;
use shamir_tx::IsolationLevel;
use shamir_types::core::interner::{InternerKey, TouchInd};
use shamir_types::types::common::new_map_wc;
use shamir_types::types::value::InnerValue;

use crate::repo::repo_instance::RepoInstance;
use crate::repo::repo_types::BoxRepo;
use crate::table::table_manager::TableManager;
use crate::table::TableConfig;

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

/// #1097 - Exact reproduction from the issue brief:
/// tx0: INSERT R{email:"y"}, INSERT D{email:"q"}; COMMIT
/// tx2: BEGIN                            // snapshot still sees R owning "y"
/// tx1: DELETE R; COMMIT                 // "y" is now durably FREE
/// tx2: UPDATE D SET email="y"           // durable "y" free -> passes; live["y"] = D
/// tx2: DELETE R                         // STALE plan -> RemovePosting("y") clears live["y"]
/// tx2: INSERT C{email:"y"}              // durable "y" still free -> passes; live["y"] = C
/// tx2: COMMIT                           // -> Must abort with UniqueViolation
#[tokio::test]
async fn stale_remove_posting_correctly_preserves_live_claim() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    tbl.create_unique_index("by_email", &["email"])
        .await
        .unwrap();
    let email_id = key_id(&tbl, "by_email").await;
    let email_field = key_id(&tbl, "email").await;

    // tx0: INSERT R{email:"y"}, INSERT D{email:"q"}; COMMIT
    let (mut tx0, _g0) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let rid_r = tbl
        .insert_tx(&record_with_str(email_field, "y"), Some(&mut tx0))
        .await
        .expect("tx0 INSERT R must succeed");
    let rid_d = tbl
        .insert_tx(&record_with_str(email_field, "q"), Some(&mut tx0))
        .await
        .expect("tx0 INSERT D must succeed");
    repo.commit_tx(tx0).await.expect("tx0 commit must succeed");

    // Verify initial state: R owns "y", D owns "q"
    let owner_y = tbl
        .index_manager()
        .lookup_by_unique_index(email_id, &[InnerValue::Str("y".into())])
        .await
        .unwrap();
    assert_eq!(owner_y, Some(rid_r), "R must own 'y' after tx0 commits");

    let owner_q = tbl
        .index_manager()
        .lookup_by_unique_index(email_id, &[InnerValue::Str("q".into())])
        .await
        .unwrap();
    assert_eq!(owner_q, Some(rid_d), "D must own 'q' after tx0 commits");

    // tx2: BEGIN (snapshot before the next line commits)
    let (mut tx2, _g2) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();

    // tx1: DELETE R; COMMIT
    let (mut tx1, _g1) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    tbl.delete_tx(rid_r, Some(&mut tx1))
        .await
        .expect("tx1 DELETE R must succeed");
    repo.commit_tx(tx1).await.expect("tx1 commit must succeed");

    // Verify "y" is now free (durably)
    let owner_y_after_tx1 = tbl
        .index_manager()
        .lookup_by_unique_index(email_id, &[InnerValue::Str("y".into())])
        .await
        .unwrap();
    assert!(
        owner_y_after_tx1.is_none(),
        "After tx1, 'y' must be free (no owner)"
    );

    // tx2: UPDATE D SET email="y" - D now claims "y"
    let d_updated = record_with_str(email_field, "y");
    tbl.update_tx(rid_d, &d_updated, Some(&mut tx2))
        .await
        .expect("tx2 UPDATE D must succeed - durable 'y' is free");

    // tx2: DELETE R (stale plan - R is already gone durably)
    // This RemovePosting is planned when tx2's snapshot still believed R owned "y"
    // The RemovePosting will have owner: Some(R), but at this point in the walk
    // live["y"] = D (set by the UPDATE just above).
    let delete_r_result = tbl.delete_tx(rid_r, Some(&mut tx2)).await;
    // The delete itself stages fine (staging is optimistic under Snapshot isolation)
    assert!(
        delete_r_result.is_ok(),
        "tx2 DELETE R must stage successfully (Snapshot isolation is optimistic)"
    );

    // tx2: INSERT C{email:"y"} - without the fix, this succeeds because the
    // stale RemovePosting cleared live["y"], leaving no collision detection.
    // With the fix, this should stage fine too (no conflict visible at stage time),
    // but the commit will abort because the real owner D still holds the key.
    let rid_c = tbl
        .insert_tx(&record_with_str(email_field, "y"), Some(&mut tx2))
        .await
        .expect("tx2 INSERT C must stage successfully");

    // tx2: COMMIT - MUST ABORT with UniqueViolation
    let commit_result = repo.commit_tx(tx2).await;
    assert!(
        matches!(
            commit_result,
            Err(crate::tx::CommitError::UniqueViolation { .. })
        ),
        "commit must abort with UniqueViolation: stale RemovePosting should not have cleared D's live claim; got {:?}",
        commit_result
    );

    // After abort, verify D's durable row still has email: "q" (unchanged)
    // and no orphaned C record exists
    let d_value = tbl.get(rid_d).await.unwrap();
    let email_key = InternerKey::new(email_field);
    let d_email = match &d_value {
        InnerValue::Map(m) => m.get(&email_key).and_then(|v| match v {
            InnerValue::Str(s) => Some(s.as_str()),
            _ => None,
        }),
        _ => None,
    };
    assert_eq!(
        d_email,
        Some("q"),
        "After abort, D's email must still be 'q' (unchanged from tx0)"
    );

    // C should not exist in the durable store
    let c_exists_result = tbl.get(rid_c).await;
    assert!(
        c_exists_result.is_err(),
        "After abort, C must not exist in durable store"
    );

    // Verify "y" is still free durably (D's UPDATE never committed)
    let owner_y_final = tbl
        .index_manager()
        .lookup_by_unique_index(email_id, &[InnerValue::Str("y".into())])
        .await
        .unwrap();
    assert!(
        owner_y_final.is_none(),
        "After abort, 'y' must remain free durably"
    );
}

/// #1097 follow-up - found by a review of the #1097 fix itself: a stale
/// `RemovePosting` must not just be ignored for `live`/`released` tracking,
/// it must be RETRACTED from `tx.index_write_set` entirely, or it still
/// reaches Phase 5c and deletes the genuine owner's posting there. This is
/// the SAME reproduction as `stale_remove_posting_correctly_preserves_live_claim`
/// minus the final `INSERT C` step -- one statement shorter, no collision at
/// all, commit succeeds, but without the retraction fix the unique index
/// silently ends up with NO posting for "y" even though D's row holds it.
#[tokio::test]
async fn stale_remove_posting_is_not_applied_at_commit() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    tbl.create_unique_index("by_email", &["email"])
        .await
        .unwrap();
    let email_id = key_id(&tbl, "by_email").await;
    let email_field = key_id(&tbl, "email").await;

    // tx0: INSERT R{email:"y"}, INSERT D{email:"q"}; COMMIT
    let (mut tx0, _g0) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let rid_r = tbl
        .insert_tx(&record_with_str(email_field, "y"), Some(&mut tx0))
        .await
        .expect("tx0 INSERT R must succeed");
    let rid_d = tbl
        .insert_tx(&record_with_str(email_field, "q"), Some(&mut tx0))
        .await
        .expect("tx0 INSERT D must succeed");
    repo.commit_tx(tx0).await.expect("tx0 commit must succeed");

    // tx2: BEGIN (snapshot before the next line commits)
    let (mut tx2, _g2) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();

    // tx1: DELETE R; COMMIT -- "y" now durably free
    let (mut tx1, _g1) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    tbl.delete_tx(rid_r, Some(&mut tx1))
        .await
        .expect("tx1 DELETE R must succeed");
    repo.commit_tx(tx1).await.expect("tx1 commit must succeed");

    // tx2: UPDATE D SET email="y" -- D claims "y" (live["y"] = D)
    let d_updated = record_with_str(email_field, "y");
    tbl.update_tx(rid_d, &d_updated, Some(&mut tx2))
        .await
        .expect("tx2 UPDATE D must succeed - durable 'y' is free");

    // tx2: DELETE R -- stale plan (R already durably gone), planned against
    // tx2's snapshot which still believed R owned "y". No third record
    // involved this time -- no collision is possible, so commit must
    // succeed. The question is whether the stale RemovePosting("y", owner=R)
    // survives into the applied op stream and deletes D's genuine posting.
    tbl.delete_tx(rid_r, Some(&mut tx2))
        .await
        .expect("tx2 DELETE R must stage successfully");

    // tx2: COMMIT -- must succeed, there is no genuine conflict here.
    repo.commit_tx(tx2)
        .await
        .expect("tx2 commit must succeed - no genuine conflict in this scenario");

    // D's row must show email "y".
    let d_value = tbl.get(rid_d).await.unwrap();
    let email_key = InternerKey::new(email_field);
    let d_email = match &d_value {
        InnerValue::Map(m) => m.get(&email_key).and_then(|v| match v {
            InnerValue::Str(s) => Some(s.as_str()),
            _ => None,
        }),
        _ => None,
    };
    assert_eq!(
        d_email,
        Some("y"),
        "D's email must be 'y' after tx2 commits"
    );

    // The unique index MUST still have a posting for "y" pointing at D --
    // without retracting the stale RemovePosting, Phase 5c applies it AFTER
    // D's genuine SetPosting (same op-stream order as staged), leaving no
    // posting for "y" at all despite D's row holding that value.
    let owner_y_final = tbl
        .index_manager()
        .lookup_by_unique_index(email_id, &[InnerValue::Str("y".into())])
        .await
        .unwrap();
    assert_eq!(
        owner_y_final,
        Some(rid_d),
        "the unique index must still have D's posting for 'y' -- a stale \
         RemovePosting must not silently delete a live owner's posting"
    );

    // D's OLD key "q" must be genuinely free -- the over-retraction
    // direction (dropping a legitimate removal) would leave it dangling.
    let owner_q_final = tbl
        .index_manager()
        .lookup_by_unique_index(email_id, &[InnerValue::Str("q".into())])
        .await
        .unwrap();
    assert!(
        owner_q_final.is_none(),
        "D's old key 'q' must be free after tx2 commits -- its genuine \
         RemovePosting must not have been over-retracted"
    );
}

/// #1097 - A legitimate release-then-reclaim case must still succeed:
/// A single record X moves off a unique key then back onto it in the same tx.
#[tokio::test]
async fn legit_release_then_reclaim_succeeds() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    tbl.create_unique_index("by_email", &["email"])
        .await
        .unwrap();
    let email_id = key_id(&tbl, "by_email").await;
    let email_field = key_id(&tbl, "email").await;
    let name_field = key_id(&tbl, "name").await;

    fn record_with_email_name(
        email_key: u64,
        email: &str,
        name_key: u64,
        name: &str,
    ) -> InnerValue {
        let mut m = new_map_wc(2);
        m.insert(InternerKey::new(email_key), InnerValue::Str(email.into()));
        m.insert(InternerKey::new(name_key), InnerValue::Str(name.into()));
        InnerValue::Map(m)
    }

    // tx1: INSERT X{email:"a", name:"alice"}; COMMIT
    let (mut tx1, _g1) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let rid_x = tbl
        .insert_tx(
            &record_with_email_name(email_field, "a", name_field, "alice"),
            Some(&mut tx1),
        )
        .await
        .expect("tx1 INSERT X must succeed");
    repo.commit_tx(tx1).await.expect("tx1 commit must succeed");

    // Verify X owns "a"
    let owner_a = tbl
        .index_manager()
        .lookup_by_unique_index(email_id, &[InnerValue::Str("a".into())])
        .await
        .unwrap();
    assert_eq!(owner_a, Some(rid_x), "X must own 'a' after tx1 commits");

    // tx2: UPDATE X SET email="b"; UPDATE X SET email="a" (moves off then back onto same key)
    let (mut tx2, _g2) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();

    // First update: email "a" -> "b"
    let x_with_b = record_with_email_name(email_field, "b", name_field, "alice");
    tbl.update_tx(rid_x, &x_with_b, Some(&mut tx2))
        .await
        .expect("tx2 UPDATE X SET email='b' must succeed");

    // Second update: email "b" -> "a" (back to original)
    let x_with_a = record_with_email_name(email_field, "a", name_field, "alice");
    tbl.update_tx(rid_x, &x_with_a, Some(&mut tx2))
        .await
        .expect("tx2 UPDATE X SET email='a' must succeed");

    // Commit tx2 - MUST SUCCEED (same record reclaiming its own released key)
    repo.commit_tx(tx2)
        .await
        .expect("tx2 commit must succeed - legitimate release-then-reclaim");

    // Verify X still owns "a"
    let owner_a_final = tbl
        .index_manager()
        .lookup_by_unique_index(email_id, &[InnerValue::Str("a".into())])
        .await
        .unwrap();
    assert_eq!(
        owner_a_final,
        Some(rid_x),
        "X must own 'a' after tx2 commits"
    );

    // The intermediate key "b" must be genuinely free -- over-retraction
    // (dropping the "a"->"b" removal because it thinks it's stale) would
    // leave a dangling posting for "b" pointing at X.
    let owner_b_final = tbl
        .index_manager()
        .lookup_by_unique_index(email_id, &[InnerValue::Str("b".into())])
        .await
        .unwrap();
    assert!(
        owner_b_final.is_none(),
        "intermediate key 'b' must be free -- its removal must not have \
         been over-retracted"
    );

    // Verify X's data is correct
    let x_value = tbl.get(rid_x).await.unwrap();
    let name_key = InternerKey::new(name_field);
    let x_name = match &x_value {
        InnerValue::Map(m) => m.get(&name_key).and_then(|v| match v {
            InnerValue::Str(s) => Some(s.as_str()),
            _ => None,
        }),
        _ => None,
    };
    assert_eq!(x_name, Some("alice"), "X's name must be 'alice'");
}

/// #1097 - A genuine two-different-records collision (no stale plan) must still reject:
/// tx: INSERT A{email:"x"}; INSERT B{email:"x"}; COMMIT -> must abort
#[tokio::test]
async fn genuine_double_claim_still_rejects() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    tbl.create_unique_index("by_email", &["email"])
        .await
        .unwrap();
    let _email_id = key_id(&tbl, "by_email").await;
    let email_field = key_id(&tbl, "email").await;

    let (mut tx, _g) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();

    // INSERT A{email:"x"} - succeeds
    let _rid_a = tbl
        .insert_tx(&record_with_str(email_field, "x"), Some(&mut tx))
        .await
        .expect("INSERT A must succeed");

    // INSERT B{email:"x"} - stages fine (neither claim is durable yet)
    let _rid_b = tbl
        .insert_tx(&record_with_str(email_field, "x"), Some(&mut tx))
        .await
        .expect("INSERT B stages fine - neither claim is durable yet");

    // Commit must abort: pre_commit's Step 1 walk sees two SetPostings for
    // the same key with no RemovePosting between them
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

/// #1097 round 3 - found by a second `@oh` review: a bare `DELETE` with no
/// companion `SetPosting` in the same tx has NO `live` claim to compare its
/// `RemovePosting`'s owner against (`current_owner.is_none()`), so it needs
/// a durable-state check, not just the intra-tx `live` comparison. Without
/// it, a stale bare DELETE could silently delete a concurrently-reclaiming
/// record's genuine posting with zero validation: `delete_tx` records no
/// `UniqueGuard` (so Step 2 never looks at the key) and there is nothing in
/// `live` to collide with (so Step 1's collision check never fires either).
#[tokio::test]
async fn stale_bare_delete_does_not_clobber_a_concurrently_reclaimed_posting() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    tbl.create_unique_index("by_email", &["email"])
        .await
        .unwrap();
    let email_id = key_id(&tbl, "by_email").await;
    let email_field = key_id(&tbl, "email").await;

    // tx0: INSERT A{email:"x"}; COMMIT
    let (mut tx0, _g0) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let rid_a = tbl
        .insert_tx(&record_with_str(email_field, "x"), Some(&mut tx0))
        .await
        .expect("tx0 INSERT A must succeed");
    repo.commit_tx(tx0).await.expect("tx0 commit must succeed");

    // tx2: BEGIN (snapshot before the next block commits)
    let (mut tx2, _g2) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();

    // tx1: DELETE A; INSERT B{email:"x"}; COMMIT -- durable "x" now owned by B
    let (mut tx1, _g1) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    tbl.delete_tx(rid_a, Some(&mut tx1))
        .await
        .expect("tx1 DELETE A must succeed");
    let rid_b = tbl
        .insert_tx(&record_with_str(email_field, "x"), Some(&mut tx1))
        .await
        .expect("tx1 INSERT B must succeed");
    repo.commit_tx(tx1).await.expect("tx1 commit must succeed");

    // tx2: DELETE A -- a bare delete, no other op in this tx. Planned
    // against tx2's stale snapshot, which still believes A owns "x".
    tbl.delete_tx(rid_a, Some(&mut tx2))
        .await
        .expect("tx2 DELETE A must stage successfully");

    // tx2: COMMIT -- must succeed, there is no genuine conflict: A's own
    // row deletion is independent of B's unrelated posting.
    repo.commit_tx(tx2)
        .await
        .expect("tx2 commit must succeed - no genuine conflict in this scenario");

    // B's posting for "x" must survive -- the stale RemovePosting must have
    // been retracted rather than applied against B's genuine posting.
    let owner_x_final = tbl
        .index_manager()
        .lookup_by_unique_index(email_id, &[InnerValue::Str("x".into())])
        .await
        .unwrap();
    assert_eq!(
        owner_x_final,
        Some(rid_b),
        "B's posting for 'x' must survive a stale bare DELETE planned \
         against an unrelated, already-deleted record's stale snapshot"
    );

    // B's data row must be unaffected.
    let b_value = tbl.get(rid_b).await.unwrap();
    let email_key = InternerKey::new(email_field);
    let b_email = match &b_value {
        InnerValue::Map(m) => m.get(&email_key).and_then(|v| match v {
            InnerValue::Str(s) => Some(s.as_str()),
            _ => None,
        }),
        _ => None,
    };
    assert_eq!(b_email, Some("x"), "B's email must still be 'x'");
}

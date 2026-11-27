//! #1100: DELETE/UPDATE planned from a stale snapshot never removes the record's CURRENT posting
//!
//! Tests the fix for the bug where a DELETE or UPDATE transaction plans index
//! removal operations based on the transaction's own snapshot view of a record,
//! rather than the record's ACTUAL current durable value. If another concurrent
//! transaction updated the record's indexed fields after this transaction's
//! snapshot was taken, the planner computes the removal key from the OLD value
//! (seen in the snapshot), not the ACTUAL current value. This means the correct
//! removal op is never generated at all, leaving a dangling posting.

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

fn record_with_two_str(key1: u64, val1: &str, key2: u64, val2: &str) -> InnerValue {
    let mut m = new_map_wc(2);
    m.insert(InternerKey::new(key1), InnerValue::Str(val1.into()));
    m.insert(InternerKey::new(key2), InnerValue::Str(val2.into()));
    InnerValue::Map(m)
}

/// #1100 - Exact reproduction from the issue brief:
/// tx0: INSERT R{email:"y"}; COMMIT                    // "y" -> R
/// tx2: BEGIN (snapshot taken here, sees email:"y")
/// tx1: UPDATE R SET email="z"; COMMIT                 // "y" removed, "z" -> R
/// tx2: DELETE R; COMMIT                               // plans Remove("y") from ITS stale snapshot
///
/// Remove("y") is a no-op at commit (already durably gone — "y" was removed by tx1).
/// No op is EVER planned for "z" — the record's actual current posting. Result:
/// posting "z" -> R survives R's deletion permanently. Every later INSERT W{email:"z"}
/// fails with DuplicateKey naming a record (R) that no longer exists.
#[tokio::test]
async fn stale_snapshot_delete_leaves_dangling_posting() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    tbl.create_unique_index("by_email", &["email"])
        .await
        .unwrap();
    let email_id = key_id(&tbl, "by_email").await;
    let email_field = key_id(&tbl, "email").await;

    // tx0: INSERT R{email:"y"}; COMMIT
    let (mut tx0, _g0) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let rid_r = tbl
        .insert_tx(&record_with_str(email_field, "y"), Some(&mut tx0))
        .await
        .expect("tx0 INSERT R must succeed");
    repo.commit_tx(tx0).await.expect("tx0 commit must succeed");

    // Verify initial state: R owns "y"
    let owner_y = tbl
        .index_manager()
        .lookup_by_unique_index(email_id, &[InnerValue::Str("y".into())])
        .await
        .unwrap();
    assert_eq!(owner_y, Some(rid_r), "R must own 'y' after tx0 commits");

    // tx2: BEGIN (snapshot before the next line commits)
    let (mut tx2, _g2) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();

    // tx1: UPDATE R SET email="z"; COMMIT
    let (mut tx1, _g1) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let r_updated = record_with_str(email_field, "z");
    tbl.update_tx(rid_r, &r_updated, Some(&mut tx1))
        .await
        .expect("tx1 UPDATE R must succeed");
    repo.commit_tx(tx1).await.expect("tx1 commit must succeed");

    // Verify state after tx1: R owns "z", "y" is free
    let owner_z = tbl
        .index_manager()
        .lookup_by_unique_index(email_id, &[InnerValue::Str("z".into())])
        .await
        .unwrap();
    assert_eq!(owner_z, Some(rid_r), "R must own 'z' after tx1 commits");

    let owner_y_after_tx1 = tbl
        .index_manager()
        .lookup_by_unique_index(email_id, &[InnerValue::Str("y".into())])
        .await
        .unwrap();
    assert!(
        owner_y_after_tx1.is_none(),
        "After tx1, 'y' must be free (no owner)"
    );

    // tx2: DELETE R (stale plan - from snapshot which still believes R owns "y")
    tbl.delete_tx(rid_r, Some(&mut tx2))
        .await
        .expect("tx2 DELETE R must stage successfully");

    // tx2: COMMIT - this is where the bug manifests: the stale Remove("y") is applied
    // (it's a no-op since "y" is already free), but no Remove("z") is ever planned,
    // leaving "z" -> R as a dangling posting.
    repo.commit_tx(tx2)
        .await
        .expect("tx2 commit must succeed - no visible conflict at commit time");

    // Now test the symptom: R is gone, but "z" is still claimed by the deleted R
    let r_exists = tbl.get(rid_r).await;
    assert!(r_exists.is_err(), "R must not exist after tx2 deletes it");

    // "z" should be free after R is deleted, but WITHOUT the fix it still points at R
    let owner_z_final = tbl
        .index_manager()
        .lookup_by_unique_index(email_id, &[InnerValue::Str("z".into())])
        .await
        .unwrap();

    // BEFORE FIX: owner_z_final is Some(rid_r) - a dangling posting
    // AFTER FIX: owner_z_final is None - the posting is correctly removed
    assert!(
        owner_z_final.is_none(),
        "After R is deleted, 'z' must be free - posting must not point at a deleted record; got owner={:?}",
        owner_z_final
    );

    // Try to insert W{email:"z"} - WITHOUT the fix, this fails with DuplicateKey
    // because "z" is still claimed by the now-deleted R. AFTER the fix, this succeeds.
    let (mut tx3, _g3) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let w_insert_result = tbl
        .insert_tx(&record_with_str(email_field, "z"), Some(&mut tx3))
        .await;
    repo.commit_tx(tx3).await.expect("tx3 commit must succeed");

    assert!(
        w_insert_result.is_ok(),
        "INSERT W{{email:'z'}} must succeed after R is deleted - 'z' should be free"
    );
}

/// #1100 - Same scenario but testing UPDATE instead of DELETE:
/// #1100 - UPDATE from stale snapshot bug:
/// When a transaction updates a record and another concurrent transaction
/// changes an INDEXED field, the index ops must be re-planned based on the
/// ACTUAL current value, not the stale snapshot value.
///
/// Scenario:
/// tx0: INSERT R{email:"y", name:"alice", age:10}; COMMIT
/// tx2: BEGIN (snapshot sees {email:"y", name:"alice", age:10})
/// tx1: UPDATE R SET age=20; COMMIT  // age is NOT indexed
/// tx2: UPDATE R SET name="bob"; COMMIT (name is also NOT indexed)
///
/// Without fix: tx2 sees email="y" in snapshot, plans no index ops (email unchanged)
/// But age changed from 10 to 20, which might affect a sorted index on age
/// The rederive must detect this and fix the sorted index postings
#[tokio::test]
async fn stale_snapshot_update_leaves_dangling_posting() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    tbl.create_unique_index("by_email", &["email"])
        .await
        .unwrap();
    let email_id = key_id(&tbl, "by_email").await;
    let email_field = key_id(&tbl, "email").await;
    let name_field = key_id(&tbl, "name").await;

    // tx0: INSERT R{email:"y", name:"alice"}; COMMIT
    let (mut tx0, _g0) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let rid_r = tbl
        .insert_tx(
            &record_with_two_str(email_field, "y", name_field, "alice"),
            Some(&mut tx0),
        )
        .await
        .expect("tx0 INSERT R must succeed");
    repo.commit_tx(tx0).await.expect("tx0 commit must succeed");

    // tx2: BEGIN (snapshot before the next line commits)
    let (mut tx2, _g2) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();

    // tx1: UPDATE R SET email="z"; COMMIT (changes the indexed field)
    let r_updated = record_with_two_str(email_field, "z", name_field, "alice");
    let (mut tx1, _g1) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    tbl.update_tx(rid_r, &r_updated, Some(&mut tx1))
        .await
        .expect("tx1 UPDATE R must succeed");
    repo.commit_tx(tx1).await.expect("tx1 commit must succeed");

    // tx2: UPDATE R SET name="bob" (from snapshot, still has email:"y")
    // tx2's staged value is {email:"y", name:"bob"}
    // but ACTUAL current value is {email:"z", name:"alice"}
    // Email is unchanged from tx2's perspective ("y" -> "y"), so NO index ops are staged
    let r_updated_tx2 = record_with_two_str(email_field, "y", name_field, "bob");
    tbl.update_tx(rid_r, &r_updated_tx2, Some(&mut tx2))
        .await
        .expect("tx2 UPDATE R must stage successfully");

    // tx2: COMMIT
    repo.commit_tx(tx2).await.expect("tx2 commit must succeed");

    // After tx2 commits, under Snapshot isolation, tx2's value wins
    // Data: {email:"y", name:"bob"} (tx2 overwrites tx1's email change)
    let r_value = tbl.get(rid_r).await.unwrap();
    let name_key = InternerKey::new(name_field);
    let email_key = InternerKey::new(email_field);
    let r_name = match &r_value {
        InnerValue::Map(m) => m.get(&name_key).and_then(|v| match v {
            InnerValue::Str(s) => Some(s.as_str()),
            _ => None,
        }),
        _ => None,
    };
    let r_email = match &r_value {
        InnerValue::Map(m) => m.get(&email_key).and_then(|v| match v {
            InnerValue::Str(s) => Some(s.as_str()),
            _ => None,
        }),
        _ => None,
    };

    assert_eq!(r_name, Some("bob"), "R must have name='bob' after tx2");
    assert_eq!(
        r_email,
        Some("y"),
        "R must have email='y' after tx2 (tx2's value)"
    );

    // Index must be consistent with data: email="y" should point to R
    let owner_y = tbl
        .index_manager()
        .lookup_by_unique_index(email_id, &[InnerValue::Str("y".into())])
        .await
        .unwrap();
    assert_eq!(
        owner_y,
        Some(rid_r),
        "R must own 'y' in the unique index (data has email='y')"
    );

    // The old "z" posting (from tx1) should be removed by the re-derive fix
    let owner_z = tbl
        .index_manager()
        .lookup_by_unique_index(email_id, &[InnerValue::Str("z".into())])
        .await
        .unwrap();
    // BEFORE FIX: owner_z is Some(rid_r) - the "z" posting from tx1 survives tx2's commit
    // WITHOUT rederive: tx2 sees email unchanged in its snapshot ("y"->"y"), so plans no index ops.
    // The staged KvOp::Set overwrites the data to {email:"y", name:"bob"}, but "z" posting is never removed.
    // WITH rederive: detects concurrent change, re-plans based on ACTUAL current {email:"z", name:"alice"},
    // and CORRECTS the staged ops to ensure index consistency.
    assert!(
        owner_z.is_none(),
        "'z' must be free - stale posting from tx1 must be removed; got owner={:?}",
        owner_z
    );

    // Verify we can insert a new record with email="z"
    let (mut tx3, _g3) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let w_insert_result = tbl
        .insert_tx(&record_with_str(email_field, "z"), Some(&mut tx3))
        .await;
    repo.commit_tx(tx3).await.expect("tx3 commit must succeed");

    assert!(
        w_insert_result.is_ok(),
        "INSERT with email='z' must succeed - 'z' should be free"
    );
}

/// Regression test: a delete against a record whose value did NOT change
/// concurrently still removes its posting correctly.
#[tokio::test]
async fn delete_without_concurrent_change_works() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    tbl.create_unique_index("by_email", &["email"])
        .await
        .unwrap();
    let email_id = key_id(&tbl, "by_email").await;
    let email_field = key_id(&tbl, "email").await;

    // tx0: INSERT R{email:"x"}; COMMIT
    let (mut tx0, _g0) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let rid_r = tbl
        .insert_tx(&record_with_str(email_field, "x"), Some(&mut tx0))
        .await
        .expect("tx0 INSERT R must succeed");
    repo.commit_tx(tx0).await.expect("tx0 commit must succeed");

    // tx1: DELETE R; COMMIT (no concurrent changes)
    let (mut tx1, _g1) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    tbl.delete_tx(rid_r, Some(&mut tx1))
        .await
        .expect("tx1 DELETE R must succeed");
    repo.commit_tx(tx1).await.expect("tx1 commit must succeed");

    // Verify the posting is removed
    let owner_x = tbl
        .index_manager()
        .lookup_by_unique_index(email_id, &[InnerValue::Str("x".into())])
        .await
        .unwrap();
    assert!(owner_x.is_none(), "After delete, 'x' must be free");

    // Verify we can insert a new record with email="x"
    let (mut tx2, _g2) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let rid_w = tbl
        .insert_tx(&record_with_str(email_field, "x"), Some(&mut tx2))
        .await
        .expect("INSERT with email='x' must succeed");
    repo.commit_tx(tx2).await.expect("tx2 commit must succeed");

    let owner_x_after = tbl
        .index_manager()
        .lookup_by_unique_index(email_id, &[InnerValue::Str("x".into())])
        .await
        .unwrap();
    assert_eq!(
        owner_x_after,
        Some(rid_w),
        "After re-insert, W must own 'x'"
    );
}

/// Regression test: update without concurrent change works correctly.
#[tokio::test]
async fn update_without_concurrent_change_works() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    tbl.create_unique_index("by_email", &["email"])
        .await
        .unwrap();
    let email_id = key_id(&tbl, "by_email").await;
    let email_field = key_id(&tbl, "email").await;

    // tx0: INSERT R{email:"x"}; COMMIT
    let (mut tx0, _g0) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let rid_r = tbl
        .insert_tx(&record_with_str(email_field, "x"), Some(&mut tx0))
        .await
        .expect("tx0 INSERT R must succeed");
    repo.commit_tx(tx0).await.expect("tx0 commit must succeed");

    // tx1: UPDATE R SET email="y"; COMMIT (no concurrent changes)
    let (mut tx1, _g1) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let r_updated = record_with_str(email_field, "y");
    tbl.update_tx(rid_r, &r_updated, Some(&mut tx1))
        .await
        .expect("tx1 UPDATE R must succeed");
    repo.commit_tx(tx1).await.expect("tx1 commit must succeed");

    // Verify old posting removed, new posting present
    let owner_x = tbl
        .index_manager()
        .lookup_by_unique_index(email_id, &[InnerValue::Str("x".into())])
        .await
        .unwrap();
    assert!(owner_x.is_none(), "After update, 'x' must be free");

    let owner_y = tbl
        .index_manager()
        .lookup_by_unique_index(email_id, &[InnerValue::Str("y".into())])
        .await
        .unwrap();
    assert_eq!(owner_y, Some(rid_r), "After update, R must own 'y'");
}

/// Bug A test: unique-family UPDATE dedup is owner-blind
///
/// Scenario from the issue brief:
/// tx0: INSERT Y{email:"k"}; INSERT Z{email:"m"}; COMMIT
/// tx2: BEGIN                                    // snapshot: Y{email:"k"}, Z{email:"m"}
/// tx1: DELETE Y; UPDATE Z SET email="k"; COMMIT   // durable after tx1: "k"->Z, "m" free
/// tx2: DELETE Y;                                  // stale -> stages RemovePosting("k", owner=Y)
///      UPDATE Z SET name="n";                     // tx2's stale snapshot of Z still has email:"m",
///                                                 // so the staged UPDATE op for Z carries no unique
///                                                 // removal/set for "k" or "m" at all
///      COMMIT
///
/// Expected once tx2 commits: "k" is free (Z's ACTUAL email is "m", Y no longer exists).
/// Bug: The re-derived RemovePosting("k", owner=Z) is silently dropped because it collides
/// by key alone with the already-staged RemovePosting("k", owner=Y).
#[tokio::test]
async fn unique_update_dedup_owner_blind() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    tbl.create_unique_index("by_email", &["email"])
        .await
        .unwrap();
    let email_id = key_id(&tbl, "by_email").await;
    let email_field = key_id(&tbl, "email").await;
    let name_field = key_id(&tbl, "name").await;

    // tx0: INSERT Y{email:"k"}; INSERT Z{email:"m"}; COMMIT
    let (mut tx0, _g0) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let rid_y = tbl
        .insert_tx(&record_with_str(email_field, "k"), Some(&mut tx0))
        .await
        .expect("tx0 INSERT Y must succeed");
    let rid_z = tbl
        .insert_tx(&record_with_str(email_field, "m"), Some(&mut tx0))
        .await
        .expect("tx0 INSERT Z must succeed");
    repo.commit_tx(tx0).await.expect("tx0 commit must succeed");

    // Verify initial state: Y owns "k", Z owns "m"
    let owner_k = tbl
        .index_manager()
        .lookup_by_unique_index(email_id, &[InnerValue::Str("k".into())])
        .await
        .unwrap();
    assert_eq!(owner_k, Some(rid_y), "Y must own 'k' after tx0 commits");

    let owner_m = tbl
        .index_manager()
        .lookup_by_unique_index(email_id, &[InnerValue::Str("m".into())])
        .await
        .unwrap();
    assert_eq!(owner_m, Some(rid_z), "Z must own 'm' after tx0 commits");

    // tx2: BEGIN (snapshot taken here, sees Y{email:"k"}, Z{email:"m"})
    let (mut tx2, _g2) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();

    // tx1: DELETE Y; UPDATE Z SET email="k"; COMMIT
    let (mut tx1, _g1) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    tbl.delete_tx(rid_y, Some(&mut tx1))
        .await
        .expect("tx1 DELETE Y must succeed");
    let z_updated = record_with_str(email_field, "k");
    tbl.update_tx(rid_z, &z_updated, Some(&mut tx1))
        .await
        .expect("tx1 UPDATE Z must succeed");
    repo.commit_tx(tx1).await.expect("tx1 commit must succeed");

    // Verify state after tx1: "k" -> Z, "m" free, Y deleted
    let owner_k_after_tx1 = tbl
        .index_manager()
        .lookup_by_unique_index(email_id, &[InnerValue::Str("k".into())])
        .await
        .unwrap();
    assert_eq!(owner_k_after_tx1, Some(rid_z), "After tx1, Z must own 'k'");

    let owner_m_after_tx1 = tbl
        .index_manager()
        .lookup_by_unique_index(email_id, &[InnerValue::Str("m".into())])
        .await
        .unwrap();
    assert!(owner_m_after_tx1.is_none(), "After tx1, 'm' must be free");

    let y_exists = tbl.get(rid_y).await;
    assert!(y_exists.is_err(), "Y must not exist after tx1 deletes it");

    // tx2: DELETE Y (stale plan - from snapshot which still believes Y owns "k")
    tbl.delete_tx(rid_y, Some(&mut tx2))
        .await
        .expect("tx2 DELETE Y must stage successfully");

    // tx2: UPDATE Z SET name="n" (from stale snapshot, Z still has email:"m")
    // tx2's staged value is {email:"m", name:"n"}
    // but ACTUAL current value is {email:"k", name:"alice"}
    // Email is unchanged from tx2's perspective ("m" -> "m"), so NO index ops are staged
    let z_updated_tx2 = record_with_two_str(email_field, "m", name_field, "n");
    tbl.update_tx(rid_z, &z_updated_tx2, Some(&mut tx2))
        .await
        .expect("tx2 UPDATE Z must stage successfully");

    // tx2: COMMIT
    repo.commit_tx(tx2).await.expect("tx2 commit must succeed");

    // After tx2 commits, under Snapshot isolation, tx2's value wins
    // Data: Z has {email:"m", name:"n"} (tx2 overwrites tx1's email change)
    let z_value = tbl.get(rid_z).await.unwrap();
    let name_key = InternerKey::new(name_field);
    let email_key = InternerKey::new(email_field);
    let z_name = match &z_value {
        InnerValue::Map(m) => m.get(&name_key).and_then(|v| match v {
            InnerValue::Str(s) => Some(s.as_str()),
            _ => None,
        }),
        _ => None,
    };
    let z_email = match &z_value {
        InnerValue::Map(m) => m.get(&email_key).and_then(|v| match v {
            InnerValue::Str(s) => Some(s.as_str()),
            _ => None,
        }),
        _ => None,
    };

    assert_eq!(z_name, Some("n"), "Z must have name='n' after tx2");
    assert_eq!(z_email, Some("m"), "Z must have email='m' after tx2");

    // Index must be consistent with data: email="m" should point to Z
    let owner_m_final = tbl
        .index_manager()
        .lookup_by_unique_index(email_id, &[InnerValue::Str("m".into())])
        .await
        .unwrap();
    assert_eq!(
        owner_m_final,
        Some(rid_z),
        "Z must own 'm' in the unique index (data has email='m')"
    );

    // The "k" posting (from tx1) should be removed by the re-derive fix
    // Bug A: the re-derived RemovePosting("k", owner=Z) was silently dropped
    // because it collided by key alone with the staged RemovePosting("k", owner=Y)
    let owner_k_final = tbl
        .index_manager()
        .lookup_by_unique_index(email_id, &[InnerValue::Str("k".into())])
        .await
        .unwrap();
    assert!(
        owner_k_final.is_none(),
        "'k' must be free - stale posting from tx1 must be removed; got owner={:?}",
        owner_k_final
    );

    // Verify we can insert a new record with email="k"
    let (mut tx3, _g3) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let w_insert_result = tbl
        .insert_tx(&record_with_str(email_field, "k"), Some(&mut tx3))
        .await;
    repo.commit_tx(tx3).await.expect("tx3 commit must succeed");

    assert!(
        w_insert_result.is_ok(),
        "INSERT with email='k' must succeed - 'k' should be free"
    );
}

/// Bug B test: regular-index (non-unique) DELETE from stale snapshot never removes CURRENT posting
///
/// Same shape as #1100's ORIGINAL brief, but with create_index instead of create_unique_index:
/// tx0: INSERT R{email:"y"}; COMMIT
/// tx2: BEGIN
/// tx1: UPDATE R SET email="z"; COMMIT
/// tx2: DELETE R; COMMIT
///
/// After tx2 commits, a regular-index lookup on "z" still returns R's (now-deleted) record id
/// - a dangling regular-index posting.
///
/// Bug: The DELETE branch's append filter only handles owner:Some(...) (unique indexes),
/// but regular indexes always have owner:None, so every regular-index removal is silently discarded.
#[tokio::test]
async fn regular_index_delete_from_stale_snapshot() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    tbl.create_index("by_email", &["email"]).await.unwrap();
    let email_index = key_id(&tbl, "by_email").await;
    let email_field = key_id(&tbl, "email").await;

    // tx0: INSERT R{email:"y"}; COMMIT
    let (mut tx0, _g0) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let rid_r = tbl
        .insert_tx(&record_with_str(email_field, "y"), Some(&mut tx0))
        .await
        .expect("tx0 INSERT R must succeed");
    repo.commit_tx(tx0).await.expect("tx0 commit must succeed");

    // tx2: BEGIN (snapshot taken here, sees R{email:"y"})
    let (mut tx2, _g2) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();

    // tx1: UPDATE R SET email="z"; COMMIT
    let (mut tx1, _g1) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let r_updated = record_with_str(email_field, "z");
    tbl.update_tx(rid_r, &r_updated, Some(&mut tx1))
        .await
        .expect("tx1 UPDATE R must succeed");
    repo.commit_tx(tx1).await.expect("tx1 commit must succeed");

    // Verify state after tx1: R owns "z", "y" free
    let z_results = tbl
        .index_manager()
        .lookup_by_index(email_index, &[InnerValue::Str("z".into())])
        .await
        .unwrap();
    assert!(
        z_results
            .as_ref()
            .map(|ids| ids.contains(&rid_r))
            .unwrap_or(false),
        "After tx1, R must be in the 'z' index lookup"
    );

    let y_results = tbl
        .index_manager()
        .lookup_by_index(email_index, &[InnerValue::Str("y".into())])
        .await
        .unwrap();
    assert!(
        !y_results
            .as_ref()
            .map(|ids| ids.contains(&rid_r))
            .unwrap_or(false),
        "After tx1, R must NOT be in the 'y' index lookup"
    );

    // tx2: DELETE R (stale plan - from snapshot which still believes R owns "y")
    // tx2 stages RemovePosting for "y" based on its stale snapshot
    tbl.delete_tx(rid_r, Some(&mut tx2))
        .await
        .expect("tx2 DELETE R must stage successfully");

    // tx2: COMMIT
    repo.commit_tx(tx2)
        .await
        .expect("tx2 commit must succeed - no visible conflict at commit time");

    // Verify R is deleted
    let r_exists = tbl.get(rid_r).await;
    assert!(r_exists.is_err(), "R must not exist after tx2 deletes it");

    // The regular-index posting for "z" should be removed by the re-derive fix
    // Bug B: regular-index removals are silently discarded (owner: None)
    let z_results_final = tbl
        .index_manager()
        .lookup_by_index(email_index, &[InnerValue::Str("z".into())])
        .await
        .unwrap();
    assert!(
        !z_results_final.as_ref().map(|ids| ids.contains(&rid_r)).unwrap_or(false),
        "After R is deleted, R must NOT be in the 'z' index lookup - dangling posting must be removed"
    );

    // Verify we can insert a new record with email="z" without conflict
    let (mut tx3, _g3) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let rid_w = tbl
        .insert_tx(&record_with_str(email_field, "z"), Some(&mut tx3))
        .await
        .expect("INSERT with email='z' must succeed");
    repo.commit_tx(tx3).await.expect("tx3 commit must succeed");

    // Verify W is in the "z" index
    let z_results_new = tbl
        .index_manager()
        .lookup_by_index(email_index, &[InnerValue::Str("z".into())])
        .await
        .unwrap();
    assert!(
        z_results_new
            .as_ref()
            .map(|ids| ids.contains(&rid_w))
            .unwrap_or(false),
        "After insert, W must be in the 'z' index lookup"
    );
}

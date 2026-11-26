//! #1101: Step 2's "released -> continue" shortcut skips the durable check
//!
//! Tests the bug where Step 2 skips the durable check for a net-released key,
//! allowing a stale Set+Remove pair to silently delete an unrelated
//! concurrently-committed posting.

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

/// #1101 - Reproduction: a same-tx net-release must NOT silently clobber an
/// unrelated, concurrently-committed posting for the same key.
///
/// This deliberately avoids releasing a PRE-EXISTING durably-owned record
/// (that shape hits Step 1's `unverified_removes` durable check first, which
/// independently retracts a stale bare-removal and "un-releases" the key —
/// masking the #1101 bug entirely). Instead, this tx's ENTIRE net effect on
/// the key is a clean SetPosting-then-RemovePosting pair for a record it
/// itself created, so `released`/`ever_released` survive Step 1 untouched
/// and Step 2's `released.contains(&key)` branch is the ONLY thing standing
/// between this commit and clobbering an unrelated posting.
///
/// tx2: BEGIN (snapshot: "x" is free, table otherwise empty)
/// tx2: INSERT C{email:"x"}                                  // stages fine: "x" is free
/// tx1: INSERT B{email:"x"}; COMMIT                          // concurrent, races in
///                                                            // AFTER tx2's own staging —
///                                                            // durable owner of "x" is B
/// tx2: DELETE C                                              // nets to "released" for "x"
///                                                             // (SetPosting(C),
///                                                             // RemovePosting(C), a clean
///                                                             // matched pair — no bare/
///                                                             // unverified removal)
/// tx2: COMMIT
///
/// Before the #1101 fix: Step 2 skips the durable check entirely for a
/// net-released key (`released.contains(&key) => continue`), so tx2 commits
/// unimpeded. At Phase 5c, the residual ops apply unconditionally in staging
/// order: SetPosting("x", C) overwrites B's genuine posting, then
/// RemovePosting("x") erases it — B's data row still holds email:"x" but its
/// unique posting is gone, and "x" is incorrectly free.
///
/// After the fix: Step 2 must reject tx2's commit, since the durable owner of
/// "x" (B) is a record tx2 never touched.
#[tokio::test]
async fn stale_net_release_does_not_clobber_a_concurrent_reclaim() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    tbl.create_unique_index("by_email", &["email"])
        .await
        .unwrap();
    let email_id = key_id(&tbl, "by_email").await;
    let email_field = key_id(&tbl, "email").await;

    // tx2: BEGIN — "x" is free (table starts empty).
    let (mut tx2, _g2) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();

    // tx2: INSERT C{email:"x"} — stages fine, "x" is durably free at this point.
    let rid_c = tbl
        .insert_tx(&record_with_str(email_field, "x"), Some(&mut tx2))
        .await
        .expect("tx2 INSERT C must stage (\"x\" is free)");

    // tx1: INSERT B{email:"x"}; COMMIT — concurrent, races in AFTER tx2's own
    // staging above. tx1 operates on the actual durable state ("x" still
    // durably free — tx2 hasn't committed), independent of tx2's uncommitted
    // staged claim.
    let (mut tx1, _g1) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let rid_b = tbl
        .insert_tx(&record_with_str(email_field, "x"), Some(&mut tx1))
        .await
        .expect("tx1 INSERT B must succeed");
    repo.commit_tx(tx1).await.expect("tx1 commit must succeed");

    // Durable owner of "x" is now B — a record tx2 never touched.
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

    // tx2: DELETE C — nets "x" to "released" in tx2's own index_write_set,
    // via a clean SetPosting(C)-then-RemovePosting(C) pair.
    tbl.delete_tx(rid_c, Some(&mut tx2))
        .await
        .expect("tx2 DELETE C must stage");

    // tx2's commit must be REJECTED: "x" is durably owned by B, a record
    // tx2 never touched — admitting this commit would silently clobber B's
    // genuine posting via the residual Set+Remove pair at Phase 5c.
    let commit_result = repo.commit_tx(tx2).await;
    assert!(
        matches!(
            commit_result,
            Err(crate::tx::CommitError::UniqueViolation { .. })
        ),
        "tx2's commit must reject: its net-release plan for 'x' was built \
         against a stale snapshot (R), but the durable owner is now B, a \
         record tx2 never touched — admitting this would silently clobber \
         B's genuine posting; got {:?}",
        commit_result
    );

    // B must still be the sole owner — no corruption occurred.
    let owner_after = tbl
        .index_manager()
        .lookup_by_unique_index(email_id, &[InnerValue::Str("x".into())])
        .await
        .unwrap();
    assert_eq!(
        owner_after,
        Some(rid_b),
        "B must remain the sole owner of 'x' after tx2's rejected commit"
    );
}

/// #1101 - Regression test: a same-tx net-release with NO concurrent interference
/// must still commit successfully (no false-positive abort).
///
/// tx0: INSERT R{email:"x"}; COMMIT
/// tx1: DELETE R; COMMIT
///
/// The tx net-releases its own claim on "x". Since there's no concurrent
/// interference, the commit must succeed.
#[tokio::test]
async fn uncontended_bare_delete_succeeds() {
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

    // Verify R owns "x"
    let owner_x = tbl
        .index_manager()
        .lookup_by_unique_index(email_id, &[InnerValue::Str("x".into())])
        .await
        .unwrap();
    assert_eq!(owner_x, Some(rid_r), "R must own 'x' after tx0 commits");

    // tx1: DELETE R; COMMIT
    // This stages RemovePosting("x", owner=R), netting to "released"
    // Since there's no concurrent interference, Step 2 must allow this to commit.
    let (mut tx1, _g1) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    tbl.delete_tx(rid_r, Some(&mut tx1))
        .await
        .expect("tx1 DELETE R must succeed");

    // Commit must succeed - no concurrent interference
    repo.commit_tx(tx1)
        .await
        .expect("tx1 commit must succeed - uncontended delete is allowed");

    // After commit, verify "x" is free
    let owner_x_final = tbl
        .index_manager()
        .lookup_by_unique_index(email_id, &[InnerValue::Str("x".into())])
        .await
        .unwrap();
    assert!(
        owner_x_final.is_none(),
        "After tx1 commits, 'x' must be free"
    );

    // R should not exist in the durable store
    let r_exists_result = tbl.get(rid_r).await;
    assert!(
        r_exists_result.is_err(),
        "After tx1 commits, R must not exist in durable store"
    );
}

/// #1101 - Regression test: same-tx DELETE then INSERT of different records
/// with the same key (release-then-reclaim) must still succeed.
///
/// tx0: INSERT A{email:"x"}; COMMIT
/// tx1: DELETE A; INSERT B{email:"x"}; COMMIT -> must succeed
#[tokio::test]
async fn delete_then_reclaim_succeeds() {
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

    // Verify A owns "x"
    let owner_x = tbl
        .index_manager()
        .lookup_by_unique_index(email_id, &[InnerValue::Str("x".into())])
        .await
        .unwrap();
    assert_eq!(owner_x, Some(rid_a), "A must own 'x' after tx0 commits");

    // tx1: DELETE A; INSERT B{email:"x"}; COMMIT
    // This is release-then-reclaim. Since B is a NEW record in this tx,
    // is_record_touched(B) is true at stage time, so the INSERT succeeds.
    let (mut tx1, _g1) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    tbl.delete_tx(rid_a, Some(&mut tx1))
        .await
        .expect("tx1 DELETE A must succeed");
    let rid_b = tbl
        .insert_tx(&record_with_str(email_field, "x"), Some(&mut tx1))
        .await
        .expect("tx1 INSERT B must succeed");

    // Commit must succeed
    repo.commit_tx(tx1)
        .await
        .expect("tx1 commit must succeed - delete-then-reclaim is allowed");

    // After commit, verify B owns "x"
    let owner_x_final = tbl
        .index_manager()
        .lookup_by_unique_index(email_id, &[InnerValue::Str("x".into())])
        .await
        .unwrap();
    assert_eq!(
        owner_x_final,
        Some(rid_b),
        "After tx1 commits, B must own 'x'"
    );

    // A should not exist in the durable store
    let a_exists_result = tbl.get(rid_a).await;
    assert!(
        a_exists_result.is_err(),
        "After tx1 commits, A must not exist in durable store"
    );
}

/// #1101 - Regression test: same-tx UPDATE that moves off a key then back onto it
/// must still succeed (release-then-reclaim, but via intermediate moves).
///
/// tx0: INSERT R{email:"x"}; COMMIT
/// tx1: UPDATE R SET email="y"; UPDATE R SET email="x"; COMMIT -> must succeed
///
/// The tx moves off key "x" (releasing it) then back onto "x" (reclaiming it).
/// This is a legitimate pattern that must succeed.
#[tokio::test]
async fn move_off_then_back_onto_key_succeeds() {
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

    // tx0: INSERT R{email:"x", name:"bob"}; COMMIT
    let (mut tx0, _g0) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let rid_r = tbl
        .insert_tx(
            &record_with_email_name(email_field, "x", name_field, "bob"),
            Some(&mut tx0),
        )
        .await
        .expect("tx0 INSERT R must succeed");
    repo.commit_tx(tx0).await.expect("tx0 commit must succeed");

    // Verify R owns "x"
    let owner_x = tbl
        .index_manager()
        .lookup_by_unique_index(email_id, &[InnerValue::Str("x".into())])
        .await
        .unwrap();
    assert_eq!(owner_x, Some(rid_r), "R must own 'x' after tx0 commits");

    // tx1: UPDATE R SET email="y"; UPDATE R SET email="x"; COMMIT
    // This is release-then-reclaim via intermediate moves
    let (mut tx1, _g1) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let r_with_y = record_with_email_name(email_field, "y", name_field, "bob");
    tbl.update_tx(rid_r, &r_with_y, Some(&mut tx1))
        .await
        .expect("tx1 UPDATE R SET email='y' must succeed");

    let r_with_x = record_with_email_name(email_field, "x", name_field, "bob");
    tbl.update_tx(rid_r, &r_with_x, Some(&mut tx1))
        .await
        .expect("tx1 UPDATE R SET email='x' must succeed");

    // Commit must succeed - same record reclaiming its own released key
    repo.commit_tx(tx1)
        .await
        .expect("tx1 commit must succeed - release-then-reclaim is allowed");

    // After commit, verify R still owns "x"
    let owner_x_final = tbl
        .index_manager()
        .lookup_by_unique_index(email_id, &[InnerValue::Str("x".into())])
        .await
        .unwrap();
    assert_eq!(
        owner_x_final,
        Some(rid_r),
        "After tx1 commits, R must still own 'x'"
    );

    // The intermediate key "y" must be free
    let owner_y_final = tbl
        .index_manager()
        .lookup_by_unique_index(email_id, &[InnerValue::Str("y".into())])
        .await
        .unwrap();
    assert!(owner_y_final.is_none(), "intermediate key 'y' must be free");
}

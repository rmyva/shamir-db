//! Regression test for the P1 perf fix to
//! `rederive_stale_value_ops_post_stage` (`tx/pre_commit.rs`): the DELETE and
//! UPDATE re-planning dedup checks used to rebuild `staged_removals_by_rid`
//! from scratch, and linearly `.any()`-rescan `tx.index_write_set`, once PER
//! re-planned op — O(N²·K) for N staged ops in one table. The fix replaces
//! both with O(1) lookups against caches built ONCE per table before the
//! per-record loop.
//!
//! This test is a CORRECTNESS regression, not a timing test: it exercises the
//! slow path (forced open by a concurrent commit between BEGIN and COMMIT,
//! same interleaving as `p1097_remove_posting_owner.rs` /
//! `p1100_stale_snapshot_delete_posting.rs`) with FOUR staged ops in a SINGLE
//! table's write set (two DELETEs, two UPDATEs), mixing the regular and
//! unique index families, so a bug in the precomputed caches — e.g. cross-
//! record contamination, a cache scoped to the wrong table, or a stale/
//! not-rebuilt cache — would produce a wrong dedup verdict (an incorrectly
//! skipped append leaves a dangling posting; an incorrectly forced append is
//! a duplicate no-op) instead of just being slow. Each of the two `.any()`
//! call sites' four (family × op-kind) branches is exercised at least once,
//! across two different records, so the caches must distinguish per-record
//! and per-key state correctly, not just work for N=1.
//!
//! Uses the manual `TxContext` / `repo.begin_tx` / `repo.commit_tx`
//! convention from `p1097_remove_posting_owner.rs` (not the query-builder
//! path).

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

fn record_with_name_email(name_key: u64, name: &str, email_key: u64, email: &str) -> InnerValue {
    let mut m = new_map_wc(2);
    m.insert(InternerKey::new(name_key), InnerValue::Str(name.into()));
    m.insert(InternerKey::new(email_key), InnerValue::Str(email.into()));
    InnerValue::Map(m)
}

/// Multi-row DELETE+UPDATE rederive under concurrent write traffic:
///
/// tx0: INSERT R1{name:"n1",email:"e1"}, R2{name:"n2",email:"e2"},
///      R3{name:"n3",email:"e3"}, R4{name:"n4",email:"e4"}; COMMIT
/// tx2: BEGIN (snapshot sees all four rows above)
/// tx1: UPDATE R1 SET name="n1x"   // stales tx2's staged REGULAR removal for R1
///      UPDATE R2 SET email="e2x"  // stales tx2's staged UNIQUE removal for R2
///      COMMIT
/// tx2: DELETE R1                  // from stale snapshot {name:"n1",email:"e1"}
///      DELETE R2                  // from stale snapshot {name:"n2",email:"e2"}
///      UPDATE R3 SET name="n3-new"    // no concurrent interference
///      UPDATE R4 SET email="e4-new"   // no concurrent interference
///      COMMIT
///
/// Expected post-commit state:
///   - R1, R2 deleted; every posting for their CURRENT (post-tx1) values freed
///     ("n1x" regular, "e1" unique for R1; "n2" regular already correctly
///     staged, "e2x" unique for R2 — this exercises append-needed AND
///     dedup-suppressed outcomes for BOTH families across two records).
///   - R3 has posting under "n3-new" (regular), not under "n3".
///   - R4's unique index has "e4-new" -> R4, "e4" free.
#[tokio::test]
async fn multi_row_delete_and_update_rederive_dedups_correctly() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    tbl.create_index("by_name", &["name"]).await.unwrap();
    tbl.create_unique_index("by_email", &["email"])
        .await
        .unwrap();
    let name_index = key_id(&tbl, "by_name").await;
    let email_index = key_id(&tbl, "by_email").await;
    let name_field = key_id(&tbl, "name").await;
    let email_field = key_id(&tbl, "email").await;

    // tx0: INSERT R1..R4; COMMIT
    let (mut tx0, _g0) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let rid_r1 = tbl
        .insert_tx(
            &record_with_name_email(name_field, "n1", email_field, "e1"),
            Some(&mut tx0),
        )
        .await
        .expect("tx0 INSERT R1 must succeed");
    let rid_r2 = tbl
        .insert_tx(
            &record_with_name_email(name_field, "n2", email_field, "e2"),
            Some(&mut tx0),
        )
        .await
        .expect("tx0 INSERT R2 must succeed");
    let rid_r3 = tbl
        .insert_tx(
            &record_with_name_email(name_field, "n3", email_field, "e3"),
            Some(&mut tx0),
        )
        .await
        .expect("tx0 INSERT R3 must succeed");
    let rid_r4 = tbl
        .insert_tx(
            &record_with_name_email(name_field, "n4", email_field, "e4"),
            Some(&mut tx0),
        )
        .await
        .expect("tx0 INSERT R4 must succeed");
    repo.commit_tx(tx0).await.expect("tx0 commit must succeed");

    // tx2: BEGIN (snapshot before tx1 commits its concurrent changes)
    let (mut tx2, _g2) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();

    // tx1: UPDATE R1 SET name="n1x"; UPDATE R2 SET email="e2x"; COMMIT
    let (mut tx1, _g1) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    tbl.update_tx(
        rid_r1,
        &record_with_name_email(name_field, "n1x", email_field, "e1"),
        Some(&mut tx1),
    )
    .await
    .expect("tx1 UPDATE R1 must succeed");
    tbl.update_tx(
        rid_r2,
        &record_with_name_email(name_field, "n2", email_field, "e2x"),
        Some(&mut tx1),
    )
    .await
    .expect("tx1 UPDATE R2 must succeed");
    repo.commit_tx(tx1).await.expect("tx1 commit must succeed");

    // tx2: DELETE R1, DELETE R2 (both from stale snapshots), UPDATE R3, R4
    // (no concurrent interference on R3/R4 — exercises the dedup-suppressed
    // "already staged, don't duplicate" path for all four UPDATE cases).
    tbl.delete_tx(rid_r1, Some(&mut tx2))
        .await
        .expect("tx2 DELETE R1 must stage successfully");
    tbl.delete_tx(rid_r2, Some(&mut tx2))
        .await
        .expect("tx2 DELETE R2 must stage successfully");
    tbl.update_tx(
        rid_r3,
        &record_with_name_email(name_field, "n3-new", email_field, "e3"),
        Some(&mut tx2),
    )
    .await
    .expect("tx2 UPDATE R3 must stage successfully");
    tbl.update_tx(
        rid_r4,
        &record_with_name_email(name_field, "n4", email_field, "e4-new"),
        Some(&mut tx2),
    )
    .await
    .expect("tx2 UPDATE R4 must stage successfully");

    repo.commit_tx(tx2)
        .await
        .expect("tx2 commit must succeed - no genuine conflict in this scenario");

    // --- R1: deleted; CURRENT (post-tx1) postings must be freed ---
    assert!(tbl.get(rid_r1).await.is_err(), "R1 must not exist");
    let n1x_hits = tbl
        .index_manager()
        .lookup_by_index(name_index, &[InnerValue::Str("n1x".into())])
        .await
        .unwrap();
    assert!(
        !n1x_hits
            .as_ref()
            .map(|ids| ids.contains(&rid_r1))
            .unwrap_or(false),
        "R1's CURRENT regular posting ('n1x') must be removed by the rederive \
         append path — dangling posting after delete"
    );
    let e1_owner = tbl
        .index_manager()
        .lookup_by_unique_index(email_index, &[InnerValue::Str("e1".into())])
        .await
        .unwrap();
    assert!(
        e1_owner.is_none(),
        "R1's unique posting ('e1', unchanged) must be freed — got owner={:?}",
        e1_owner
    );

    // --- R2: deleted; CURRENT (post-tx1) postings must be freed ---
    assert!(tbl.get(rid_r2).await.is_err(), "R2 must not exist");
    let n2_hits = tbl
        .index_manager()
        .lookup_by_index(name_index, &[InnerValue::Str("n2".into())])
        .await
        .unwrap();
    assert!(
        !n2_hits
            .as_ref()
            .map(|ids| ids.contains(&rid_r2))
            .unwrap_or(false),
        "R2's regular posting ('n2', unchanged) must be removed"
    );
    let e2x_owner = tbl
        .index_manager()
        .lookup_by_unique_index(email_index, &[InnerValue::Str("e2x".into())])
        .await
        .unwrap();
    assert!(
        e2x_owner.is_none(),
        "R2's CURRENT unique posting ('e2x') must be freed by the rederive \
         append path — dangling posting after delete; got owner={:?}",
        e2x_owner
    );

    // --- R3: renamed; regular index must reflect the NEW value only ---
    let r3_value = tbl.get(rid_r3).await.unwrap();
    let name_key = InternerKey::new(name_field);
    let r3_name = match &r3_value {
        InnerValue::Map(m) => m.get(&name_key).and_then(|v| match v {
            InnerValue::Str(s) => Some(s.as_str()),
            _ => None,
        }),
        _ => None,
    };
    assert_eq!(r3_name, Some("n3-new"), "R3 must have name='n3-new'");
    let n3_new_hits = tbl
        .index_manager()
        .lookup_by_index(name_index, &[InnerValue::Str("n3-new".into())])
        .await
        .unwrap();
    assert!(
        n3_new_hits
            .as_ref()
            .map(|ids| ids.contains(&rid_r3))
            .unwrap_or(false),
        "R3 must be indexed under its new name 'n3-new'"
    );
    let n3_old_hits = tbl
        .index_manager()
        .lookup_by_index(name_index, &[InnerValue::Str("n3".into())])
        .await
        .unwrap();
    assert!(
        !n3_old_hits
            .as_ref()
            .map(|ids| ids.contains(&rid_r3))
            .unwrap_or(false),
        "R3 must NOT still be indexed under its old name 'n3'"
    );

    // --- R4: email updated; unique index must reflect the NEW value only ---
    let owner_e4_new = tbl
        .index_manager()
        .lookup_by_unique_index(email_index, &[InnerValue::Str("e4-new".into())])
        .await
        .unwrap();
    assert_eq!(
        owner_e4_new,
        Some(rid_r4),
        "R4 must own 'e4-new' in the unique index"
    );
    let owner_e4_old = tbl
        .index_manager()
        .lookup_by_unique_index(email_index, &[InnerValue::Str("e4".into())])
        .await
        .unwrap();
    assert!(
        owner_e4_old.is_none(),
        "R4's old unique key 'e4' must be free; got owner={:?}",
        owner_e4_old
    );

    // --- Freed unique keys must be genuinely reusable (no dangling owner) ---
    let (mut tx3, _g3) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let insert_e1 = tbl
        .insert_tx(
            &record_with_name_email(name_field, "w1", email_field, "e1"),
            Some(&mut tx3),
        )
        .await;
    let insert_e2x = tbl
        .insert_tx(
            &record_with_name_email(name_field, "w2", email_field, "e2x"),
            Some(&mut tx3),
        )
        .await;
    repo.commit_tx(tx3).await.expect("tx3 commit must succeed");
    assert!(
        insert_e1.is_ok(),
        "INSERT with email='e1' must succeed — key must be genuinely free"
    );
    assert!(
        insert_e2x.is_ok(),
        "INSERT with email='e2x' must succeed — key must be genuinely free"
    );
}

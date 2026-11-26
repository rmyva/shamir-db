//! HIGH-6: base_index `IndexManager` + `SortedIndexManager` tx hooks.
//!
//! `TableManager::insert_tx` / `update_tx` / `delete_tx` now stage base_index
//! secondary-index and sorted-index posting writes into
//! `tx.index_write_set`, alongside the index2 ops. The planners emit
//! `IndexWriteOp`s carrying the *exact* physical key layout the non-tx
//! readers expect (`lookup_by_index`, `check_unique_constraint`,
//! `lookup_range`), so:
//!
//! * a committed tx, once its `index_write_set` lands in `info_store`,
//!   produces postings indistinguishable from the non-tx
//!   `on_record_created` path — the base_index reader finds the record;
//! * a dropped tx never applies those ops, so the base_index reader finds
//!   nothing (rollback safety).
//!
//! Scope note — the happy-path commit applies `index_write_set` to
//! `info_store` in `commit.rs::commit_tx_inner` Phase 5c-d, which is
//! owned by a separate workstream. To assert the *staging-side* contract
//! (the key scheme HIGH-6 is responsible for) independently of that
//! pipeline, these tests drain `tx.index_write_set` and apply the ops to
//! the table's `info_store` directly — the same store the base_index
//! managers read through. This proves the posting keys match the reader,
//! which is the load-bearing correctness property: a posting written
//! with the wrong key scheme would be silent corruption.

use std::sync::Arc;

use bytes::Bytes;
use shamir_collections::TFxSet;
use shamir_storage::storage_in_memory::InMemoryRepo;
use shamir_storage::types::Store;
use shamir_tx::{IndexWriteOp, IsolationLevel, TxContext};
use shamir_types::core::interner::{InternerKey, TouchInd};
use shamir_types::core::sort_codec;
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

fn record_with_int(key: u64, val: i64) -> InnerValue {
    let mut m = new_map_wc(1);
    m.insert(InternerKey::new(key), InnerValue::Int(val));
    InnerValue::Map(m)
}

/// Apply the staged index ops to `info_store`, simulating what the
/// commit pipeline's Phase 5c-d does on a successful commit. Used so the
/// staging-side key-scheme contract is testable independently of the
/// (separately-owned) commit-apply wiring.
async fn apply_staged_index_ops(info_store: &Arc<dyn Store>, tx: &TxContext) {
    for (_token, op) in &tx.index_write_set {
        match op {
            IndexWriteOp::SetPosting { key, value, .. } => {
                info_store
                    .set(key.clone().into(), value.clone())
                    .await
                    .unwrap();
            }
            IndexWriteOp::RemovePosting { key, .. } => {
                let _ = info_store.remove(key.clone().into()).await;
            }
            IndexWriteOp::BumpFtsStats { .. } => {}
        }
    }
}

/// A committed `insert_tx` (with its staged index ops applied) populates
/// the base_index secondary index so `lookup_by_index` finds the record.
#[tokio::test]
async fn committed_tx_populates_secondary_index() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    repo.create_index("t", "by_name", &["name"]).await.unwrap();
    let name_id = key_id(&tbl, "name").await;

    let (mut tx, _g) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let rid = tbl
        .insert_tx(&record_with_str(name_id, "alice"), Some(&mut tx))
        .await
        .unwrap();

    // The staged ops must carry the base_index posting key scheme.
    assert!(
        !tx.index_write_set.is_empty(),
        "insert_tx must stage base_index index postings"
    );
    apply_staged_index_ops(tbl.info_store(), &tx).await;

    let hits = tbl
        .lookup_by_index("by_name", &[InnerValue::Str("alice".into())])
        .await
        .unwrap();
    assert!(
        hits.contains(&rid),
        "committed insert_tx posting must be findable via base_index index; got {:?}, rid {:?}",
        hits,
        rid
    );
}

/// A committed `insert_tx` populates the sorted/range index so
/// `lookup_range` finds the record.
#[tokio::test]
async fn committed_tx_populates_sorted_index() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    tbl.create_sorted_index("by_age", &["age"]).await.unwrap();
    let age_id = key_id(&tbl, "age").await;
    let sidx_name = key_id(&tbl, "by_age").await;

    let (mut tx, _g) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let rid = tbl
        .insert_tx(&record_with_int(age_id, 42), Some(&mut tx))
        .await
        .unwrap();

    assert!(
        !tx.index_write_set.is_empty(),
        "insert_tx must stage sorted index postings"
    );
    apply_staged_index_ops(tbl.info_store(), &tx).await;

    // Range covering 42.
    let mut lo = Vec::new();
    sort_codec::encode_i64(&mut lo, 40);
    let mut hi = Vec::new();
    sort_codec::encode_i64(&mut hi, 50);
    let hits = tbl
        .sorted_indexes()
        .lookup_range(sidx_name, Some(&lo), Some(&hi))
        .await
        .unwrap();
    assert!(
        hits.contains(&rid),
        "committed insert_tx posting must be findable via sorted index; got {:?}, rid {:?}",
        hits,
        rid
    );
}

/// A dropped (rolled-back) `insert_tx` leaves no base_index secondary-index
/// postings — the staged ops are never applied to `info_store`.
#[tokio::test]
async fn dropped_tx_no_secondary_postings() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    repo.create_index("t", "by_name", &["name"]).await.unwrap();
    let name_id = key_id(&tbl, "name").await;

    {
        let (mut tx, guard) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
        let _ = tbl
            .insert_tx(&record_with_str(name_id, "bob"), Some(&mut tx))
            .await
            .unwrap();
        // Drop WITHOUT applying index_write_set — models rollback.
        drop(tx);
        drop(guard);
    }

    let hits = tbl
        .lookup_by_index("by_name", &[InnerValue::Str("bob".into())])
        .await
        .unwrap();
    assert!(
        hits.is_empty(),
        "dropped insert_tx must leave no base_index index posting; got {:?}",
        hits
    );
}

/// `insert_tx` stages unique-index postings using the exact physical key
/// layout `check_unique_constraint` reads (25-byte index key → record id
/// value). Once applied, `lookup_by_unique_index` resolves the record;
/// and stage-time validation rejects a committed duplicate.
#[tokio::test]
async fn committed_tx_populates_unique_index_and_rejects_duplicate() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    tbl.create_unique_index("by_email", &["email"])
        .await
        .unwrap();
    let email_id = key_id(&tbl, "by_email").await;
    let email_field = key_id(&tbl, "email").await;

    let (mut tx, _g) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let rid = tbl
        .insert_tx(&record_with_str(email_field, "a@x"), Some(&mut tx))
        .await
        .unwrap();
    apply_staged_index_ops(tbl.info_store(), &tx).await;

    let found = tbl
        .index_manager()
        .lookup_by_unique_index(email_id, &[InnerValue::Str("a@x".into())])
        .await
        .unwrap();
    assert_eq!(
        found,
        Some(rid),
        "unique posting must resolve to the staged record id"
    );

    // Stage-time validation now sees the committed posting and rejects a
    // second insert of the same unique value.
    let (mut tx2, _g2) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let dup = tbl
        .insert_tx(&record_with_str(email_field, "a@x"), Some(&mut tx2))
        .await;
    assert!(
        matches!(dup, Err(shamir_storage::error::DbError::DuplicateKey(_))),
        "insert_tx must reject a committed duplicate at stage time; got {:?}",
        dup
    );
}

/// Posting-key scheme confirmation: the bytes `insert_tx` stages are
/// *byte-for-byte identical* to what the non-tx `IndexManager` /
/// `SortedIndexManager` planners produce. This is the load-bearing
/// guard — a divergent key scheme would be silent corruption.
#[tokio::test]
async fn staged_keys_match_non_tx_planner_output() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    repo.create_index("t", "by_name", &["name"]).await.unwrap();
    tbl.create_sorted_index("by_name_sorted", &["name"])
        .await
        .unwrap();
    let name_id = key_id(&tbl, "name").await;

    let rec = record_with_str(name_id, "carol");

    let (mut tx, _g) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let rid = tbl.insert_tx(&rec, Some(&mut tx)).await.unwrap();

    let mut staged: Vec<(Bytes, Bytes)> = tx
        .index_write_set
        .iter()
        .filter_map(|(_t, op)| match op {
            IndexWriteOp::SetPosting { key, value, .. } => Some((key.clone(), value.clone())),
            _ => None,
        })
        .collect();

    // Expected: base_index regular planner + sorted planner for the same rid.
    let mut expected: Vec<(Bytes, Bytes)> = Vec::new();
    for op in tbl
        .index_manager()
        .plan_record_created(&rid, &rec)
        .await
        .unwrap()
    {
        if let IndexWriteOp::SetPosting { key, value, .. } = op {
            expected.push((key, value));
        }
    }
    for op in tbl
        .sorted_indexes()
        .plan_record_created(&rid, &rec, 0)
        .unwrap()
    {
        if let IndexWriteOp::SetPosting { key, value, .. } = op {
            expected.push((key, value));
        }
    }

    staged.sort();
    expected.sort();
    // The staged set must CONTAIN every base_index/sorted posting key the
    // non-tx planner would emit (index2 ops may add more, hence subset).
    for e in &expected {
        assert!(
            staged.contains(e),
            "staged postings must include the non-tx key {:?}; staged = {:?}",
            e,
            staged
        );
    }
    assert!(
        !expected.is_empty(),
        "sanity: expected at least the regular + sorted postings"
    );
}

/// THE decisive test: two SI txs claim the SAME unique value. Both pass
/// stage-time `validate_unique_for_create` (neither has committed, so the
/// committed posting does not exist for either). We then commit tx_a (Ok)
/// and commit tx_b — the commit_lock Phase 2.6 re-validation sees tx_a's
/// posting and aborts tx_b with `UniqueViolation`. Exactly one record ends
/// up owning the unique value.
///
/// Ordering is load-bearing: stage_a, stage_b, commit_a, commit_b. If we
/// committed a BEFORE staging b, b's stage-time check would already catch
/// it — that would not exercise the commit-time guard at all.
#[tokio::test]
async fn concurrent_tx_unique_violation_one_aborts() {
    use crate::tx::CommitError;

    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    tbl.create_unique_index("by_email", &["email"])
        .await
        .unwrap();
    let email_id = key_id(&tbl, "by_email").await;
    let email_field = key_id(&tbl, "email").await;

    // Stage BOTH txs fully before committing either. Both pass stage-time
    // validate_unique_for_create (committed state is empty for both).
    let (mut tx_a, _ga) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let rid_a = tbl
        .insert_tx(&record_with_str(email_field, "x@y"), Some(&mut tx_a))
        .await
        .expect("tx_a stage-time validate must pass");

    let (mut tx_b, _gb) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let _rid_b = tbl
        .insert_tx(&record_with_str(email_field, "x@y"), Some(&mut tx_b))
        .await
        .expect("tx_b stage-time validate must ALSO pass (neither committed yet)");

    // Both recorded a unique guard for the byte-identical key.
    assert_eq!(tx_a.unique_guards.len(), 1, "tx_a must record one guard");
    assert_eq!(tx_b.unique_guards.len(), 1, "tx_b must record one guard");
    assert_eq!(
        tx_a.unique_guards[0].index_key, tx_b.unique_guards[0].index_key,
        "concurrent claims on the same unique value must target the byte-identical key"
    );

    // Commit tx_a → Ok (posting lands in info_store).
    repo.commit_tx(tx_a).await.expect("tx_a commits cleanly");

    // Commit tx_b → UniqueViolation (Phase 2.6 sees tx_a's posting).
    let res_b = repo.commit_tx(tx_b).await;
    assert!(
        matches!(res_b, Err(CommitError::UniqueViolation { .. })),
        "tx_b must abort with UniqueViolation under commit_lock; got {:?}",
        res_b
    );

    // Exactly one record owns the unique value — and it is tx_a's.
    let owner = tbl
        .index_manager()
        .lookup_by_unique_index(email_id, &[InnerValue::Str("x@y".into())])
        .await
        .unwrap();
    assert_eq!(
        owner,
        Some(rid_a),
        "the unique value must resolve to exactly tx_a's record"
    );

    // Metric incremented.
    assert_eq!(
        repo.tx_metrics().snapshot().txs_aborted_unique,
        1,
        "the aborted tx must bump the unique-abort metric"
    );
}

/// An update that re-writes the SAME unique value it already owns is not a
/// self-conflict: Phase 2.6 sees `existing == owner` and commits fine.
#[tokio::test]
async fn tx_update_to_own_unique_value_is_not_violation() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    tbl.create_unique_index("by_email", &["email"])
        .await
        .unwrap();
    let email_id = key_id(&tbl, "by_email").await;
    let email_field = key_id(&tbl, "email").await;

    // Insert + commit a record owning "u@v".
    let (mut tx0, _g0) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let rid = tbl
        .insert_tx(&record_with_str(email_field, "u@v"), Some(&mut tx0))
        .await
        .unwrap();
    repo.commit_tx(tx0).await.expect("initial insert commits");

    // Update the SAME rid re-writing the SAME unique value.
    let (mut tx1, _g1) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    tbl.update_tx(rid, &record_with_str(email_field, "u@v"), Some(&mut tx1))
        .await
        .expect("update_tx stage-time validate (self-write) must pass");
    repo.commit_tx(tx1)
        .await
        .expect("update re-writing own unique value must commit (owner == rid)");

    // The value still resolves to the same record.
    let owner = tbl
        .index_manager()
        .lookup_by_unique_index(email_id, &[InnerValue::Str("u@v".into())])
        .await
        .unwrap();
    assert_eq!(owner, Some(rid), "self-update must keep ownership");
}

/// #1039 — two operations within the SAME transaction claiming the SAME
/// unique-index key must abort with UniqueViolation. Fixed by intra-tx
/// dedup in Phase 2.6 of pre_commit.rs using O(1) TFxMap lookup.
#[tokio::test]
async fn intra_tx_unique_collision_silently_overwrites() {
    use crate::tx::CommitError;

    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    tbl.create_unique_index("by_email", &["email"])
        .await
        .unwrap();
    let _email_id = key_id(&tbl, "by_email").await;
    let email_field = key_id(&tbl, "email").await;

    // Stage two inserts in the SAME transaction, both claiming the same unique value.
    let (mut tx, _g) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let _rid1 = tbl
        .insert_tx(&record_with_str(email_field, "x@y"), Some(&mut tx))
        .await
        .expect("first insert stage-time validate must pass");

    let _rid2 = tbl
        .insert_tx(&record_with_str(email_field, "x@y"), Some(&mut tx))
        .await
        .expect("second insert stage-time validate must ALSO pass (neither committed yet)");

    // Both recorded a unique guard for the byte-identical key.
    assert_eq!(
        tx.unique_guards.len(),
        2,
        "BOTH guards were recorded (no dedup)"
    );
    assert_eq!(
        tx.unique_guards[0].index_key, tx.unique_guards[1].index_key,
        "both guards target the same unique key"
    );
    assert_ne!(
        tx.unique_guards[0].owner, tx.unique_guards[1].owner,
        "different records claimed the same key"
    );

    // FIXED BEHAVIOR: commit aborts with UniqueViolation (intra-tx collision).
    let commit_result = repo.commit_tx(tx).await;
    assert!(
        matches!(commit_result, Err(CommitError::UniqueViolation { .. })),
        "commit must abort with UniqueViolation for intra-tx collision; got {:?}",
        commit_result
    );

    // Metric incremented.
    assert_eq!(
        repo.tx_metrics().snapshot().txs_aborted_unique,
        1,
        "intra-tx collision must bump the unique-abort metric"
    );
}

/// #1039 — same as above, but with two SEPARATE Batch ops (not just two
/// sequential insert_tx calls). Tests the broader case across distinct
/// insert_tx_many_bytes calls.
#[tokio::test]
async fn intra_tx_unique_collision_across_two_batches() {
    use crate::tx::CommitError;

    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    tbl.create_unique_index("by_email", &["email"])
        .await
        .unwrap();
    let email_field = key_id(&tbl, "email").await;

    // Stage two separate inserts in the SAME transaction, simulating two
    // separate Batch ops (each would call insert_tx_many_bytes internally).
    let (mut tx, _g) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let _rid1 = tbl
        .insert_tx(&record_with_str(email_field, "a@b"), Some(&mut tx))
        .await
        .expect("first insert stage-time validate must pass");

    let _rid2 = tbl
        .insert_tx(&record_with_str(email_field, "a@b"), Some(&mut tx))
        .await
        .expect("second insert stage-time validate must pass");

    // Both guards recorded for the same key (different owners).
    assert_eq!(tx.unique_guards.len(), 2, "both guards were recorded");
    assert_eq!(
        tx.unique_guards[0].index_key, tx.unique_guards[1].index_key,
        "same unique key claimed twice"
    );
    assert_ne!(tx.unique_guards[0].owner, tx.unique_guards[1].owner);

    // FIXED BEHAVIOR: commit aborts with UniqueViolation.
    let commit_result = repo.commit_tx(tx).await;
    assert!(
        matches!(commit_result, Err(CommitError::UniqueViolation { .. })),
        "commit must abort with UniqueViolation; got {:?}",
        commit_result
    );
}

/// #1039 (negative test) — an UPDATE that re-writes its OWN existing unique
/// value is NOT treated as a violation (same owner, same key).
#[tokio::test]
async fn intra_tx_self_write_same_key_is_not_violation() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    tbl.create_unique_index("by_email", &["email"])
        .await
        .unwrap();
    let email_id = key_id(&tbl, "by_email").await;
    let email_field = key_id(&tbl, "email").await;

    // Insert a record with unique value.
    let (mut tx0, _g0) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let rid = tbl
        .insert_tx(&record_with_str(email_field, "m@n"), Some(&mut tx0))
        .await
        .unwrap();
    repo.commit_tx(tx0).await.expect("initial insert commits");

    // In a new tx, update the SAME record re-writing the SAME value twice.
    let (mut tx, _g) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    tbl.update_tx(rid, &record_with_str(email_field, "m@n"), Some(&mut tx))
        .await
        .expect("first update stage-time validate must pass");

    tbl.update_tx(rid, &record_with_str(email_field, "m@n"), Some(&mut tx))
        .await
        .expect("second update stage-time validate must pass");

    // Both guards for same owner and key — should NOT be a violation.
    assert_eq!(tx.unique_guards.len(), 2, "both updates recorded guards");
    assert_eq!(
        tx.unique_guards[0].index_key, tx.unique_guards[1].index_key,
        "same unique key"
    );
    assert_eq!(
        tx.unique_guards[0].owner, tx.unique_guards[1].owner,
        "same owner (self-write)"
    );

    // Commit MUST succeed — this is NOT a violation.
    repo.commit_tx(tx)
        .await
        .expect("self-write re-validating same key must commit");

    // Verify the value still resolves to the same record.
    let owner = tbl
        .index_manager()
        .lookup_by_unique_index(email_id, &[InnerValue::Str("m@n".into())])
        .await
        .unwrap();
    assert_eq!(owner, Some(rid), "ownership unchanged");
}

/// #1039 (negative test) — two DIFFERENT unique indexes each claimed once
/// in the same tx must succeed (no false collision across indexes).
#[tokio::test]
async fn intra_tx_two_different_unique_indexes_no_collision() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    tbl.create_unique_index("by_email", &["email"])
        .await
        .unwrap();
    tbl.create_unique_index("by_username", &["username"])
        .await
        .unwrap();
    let email_field = key_id(&tbl, "email").await;
    let username_field = key_id(&tbl, "username").await;

    // Insert two records: one claims email unique, another claims username unique.
    let (mut tx, _g) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let _rid1 = tbl
        .insert_tx(
            &{
                let mut m = new_map_wc(2);
                m.insert(InternerKey::new(email_field), InnerValue::Str("a@b".into()));
                m.insert(
                    InternerKey::new(username_field),
                    InnerValue::Str("alice".into()),
                );
                InnerValue::Map(m)
            },
            Some(&mut tx),
        )
        .await
        .unwrap();

    let _rid2 = tbl
        .insert_tx(
            &{
                let mut m = new_map_wc(2);
                m.insert(InternerKey::new(email_field), InnerValue::Str("c@d".into()));
                m.insert(
                    InternerKey::new(username_field),
                    InnerValue::Str("bob".into()),
                );
                InnerValue::Map(m)
            },
            Some(&mut tx),
        )
        .await
        .unwrap();

    // Four guards for different unique keys (2 records × 2 unique indexes each).
    assert_eq!(
        tx.unique_guards.len(),
        4,
        "four guards for two records with two indexes each"
    );
    // All keys are unique combinations of (table_token, index_key).
    let mut seen = TFxSet::default();
    for g in &tx.unique_guards {
        let key = (g.table_token, g.index_key.clone());
        assert!(seen.insert(key), "duplicate (table_token, index_key) combo");
    }

    // Commit MUST succeed — different indexes are independent.
    repo.commit_tx(tx)
        .await
        .expect("two different unique indexes must not collide");
}

/// #1074 — a key released via UPDATE-off and then reclaimed by a DIFFERENT
/// record within the same transaction must commit successfully. Before #1074,
/// the #1039 claims-only intra-tx check false-rejected this pattern: it saw
/// `(K_x, A)` then `(K_x, B)` (different owners) and aborted, even though A
/// had legitimately moved off `x` before B claimed it.
///
/// ```text
/// INSERT A {email: "x"}   → guard (K_x, A), SetPosting(K_x, A)
/// UPDATE A SET email = "z" → guard (K_z, A), RemovePosting(K_x), SetPosting(K_z, A)
/// INSERT B {email: "x"}   → guard (K_x, B), SetPosting(K_x, B)
/// ```
/// Final state `K_x -> B`, `K_z -> A` is completely valid.
#[tokio::test]
async fn intra_tx_update_off_then_reclaim_same_key() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    tbl.create_unique_index("by_email", &["email"])
        .await
        .unwrap();
    let email_id = key_id(&tbl, "by_email").await;
    let email_field = key_id(&tbl, "email").await;

    let (mut tx, _g) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let rid_a = tbl
        .insert_tx(&record_with_str(email_field, "x"), Some(&mut tx))
        .await
        .unwrap();
    tbl.update_tx(rid_a, &record_with_str(email_field, "z"), Some(&mut tx))
        .await
        .unwrap();
    let rid_b = tbl
        .insert_tx(&record_with_str(email_field, "x"), Some(&mut tx))
        .await
        .unwrap();

    // Commit MUST succeed — A vacated "x" before B claimed it.
    repo.commit_tx(tx)
        .await
        .expect("release-then-reclaim via UPDATE must commit");

    // Final state: K_x -> B, K_z -> A.
    let owner_x = tbl
        .index_manager()
        .lookup_by_unique_index(email_id, &[InnerValue::Str("x".into())])
        .await
        .unwrap();
    assert_eq!(
        owner_x,
        Some(rid_b),
        "K_x must resolve to B (the reclaiming record)"
    );

    let owner_z = tbl
        .index_manager()
        .lookup_by_unique_index(email_id, &[InnerValue::Str("z".into())])
        .await
        .unwrap();
    assert_eq!(
        owner_z,
        Some(rid_a),
        "K_z must resolve to A (the record that moved off x)"
    );
}

/// #1074 — a key released via DELETE and then reclaimed by a DIFFERENT record
/// within the same transaction must commit successfully. DELETE pushes no
/// UniqueGuard at all, so the #1039 check saw only `(K_x, A)` then `(K_x, B)`
/// (from the two inserts) and false-rejected — the delete's RemovePosting was
/// invisible to the claims-only check.
///
/// ```text
/// INSERT A {email: "x"} → guard (K_x, A), SetPosting(K_x, A)
/// DELETE A              → RemovePosting(K_x)   [no guard pushed]
/// INSERT B {email: "x"} → guard (K_x, B), SetPosting(K_x, B)
/// ```
#[tokio::test]
async fn intra_tx_delete_then_reclaim_same_key() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    tbl.create_unique_index("by_email", &["email"])
        .await
        .unwrap();
    let email_id = key_id(&tbl, "by_email").await;
    let email_field = key_id(&tbl, "email").await;

    let (mut tx, _g) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let rid_a = tbl
        .insert_tx(&record_with_str(email_field, "x"), Some(&mut tx))
        .await
        .unwrap();
    tbl.delete_tx(rid_a, Some(&mut tx)).await.unwrap();
    let rid_b = tbl
        .insert_tx(&record_with_str(email_field, "x"), Some(&mut tx))
        .await
        .unwrap();

    // Commit MUST succeed — A was deleted before B claimed "x".
    repo.commit_tx(tx)
        .await
        .expect("delete-then-reclaim must commit");

    // Final state: K_x -> B.
    let owner_x = tbl
        .index_manager()
        .lookup_by_unique_index(email_id, &[InnerValue::Str("x".into())])
        .await
        .unwrap();
    assert_eq!(
        owner_x,
        Some(rid_b),
        "K_x must resolve to B (the reclaiming record)"
    );
}

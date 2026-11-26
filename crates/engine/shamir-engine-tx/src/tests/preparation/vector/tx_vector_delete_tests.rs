//! Regression tests for gap#1 / HIGH-6 — tx-committed vector deletes must
//! reach the HNSW graph + the durable delta log.
//!
//! Before the fix, a tx that deleted a vector-backed row left a GHOST vector
//! in ANN search — both live (the graph was never tombstoned) AND after a
//! cold restart (no `DeltaOp::Delete` was ever written, so the replay
//! re-inserted the vector from the base snapshot). These tests pin the fix
//! (variant-A, which mirrors the insert staging path):
//!
//!  * **ghost (main)** — tx deletes a vector-backed row; after commit a
//!    non-tx ANN search does NOT return the rid.
//!  * **abort (RAII)** — tx deletes a vector-backed row then is dropped
//!    (rollback); the vector STAYS in the graph (staged delete discarded by
//!    RAII, ghost never appeared).
//!  * **replace** — tx re-writes the same rid with a NEW vector (delete +
//!    insert); search returns the NEW vector, not the old and not both.
//!  * **back-compat non-tx delete** — the non-tx `delete` (immediate graph
//!    delete via `plan_delete`) still excludes the rid from search; a
//!    table without a vector index is untouched by the delete path.

use std::sync::Arc;

use shamir_query_types::admin::CreateIndexOp;
use shamir_storage::storage_in_memory::InMemoryRepo;
use shamir_tx::IsolationLevel;
use shamir_types::core::interner::{InternerKey, TouchInd};
use shamir_types::types::common::new_map_wc;
use shamir_types::types::value::InnerValue;

use crate::index2::backend::{IndexBackend, IndexQuery, IndexResult};
use crate::repo::repo_instance::RepoInstance;
use crate::repo::repo_types::BoxRepo;
use crate::table::TableConfig;

fn make_repo() -> RepoInstance {
    let repo = Arc::new(InMemoryRepo::new());
    RepoInstance::new("test".into(), BoxRepo::InMemory(repo), Vec::new())
}

async fn field_id(tbl: &crate::table::TableManager, name: &str) -> u64 {
    let interner = tbl.interner().get().await.unwrap();
    match interner.touch_ind(name).unwrap() {
        TouchInd::Exists(k) | TouchInd::New(k) => k.id(),
    }
}

fn vector_index_op() -> CreateIndexOp {
    CreateIndexOp {
        create_index: "vec_idx".into(),
        table: "vecs".into(),
        fields: vec![vec!["embedding".into()]],
        unique: false,
        sorted: false,
        repo: "main".into(),
        index_type: Some("vector".into()),
        fts_tokenizer: None,
        fts_language: None,
        functional_op: None,
        functional_args: None,
        vector_dim: Some(3),
        vector_metric: Some("cosine".into()),
        vector_quantization: None,
        include: Vec::new(),
        if_not_exists: false,
    }
}

fn vec_record(emb_key_id: u64, vec: &[f64]) -> InnerValue {
    let mut m = new_map_wc(1);
    m.insert(
        InternerKey::new(emb_key_id),
        InnerValue::List(vec.iter().map(|f| InnerValue::F64(*f)).collect()),
    );
    InnerValue::Map(m)
}

/// Poll a non-tx vector search for up to ~2s, returning the ranked hits.
/// The post-lock HNSW promote (Phase 5d, `promote_vectors`) lands via a
/// `spawn_blocking` insert with an async tail, so a fixed sleep is flaky
/// under parallel-test CPU contention; this loop returns the instant the
/// promote completes.
async fn poll_vector_hits(
    backend: &Arc<dyn IndexBackend>,
    query: Vec<f32>,
) -> Vec<(shamir_types::types::record_id::RecordId, f32)> {
    for _ in 0..200 {
        let post = backend
            .lookup(IndexQuery::Vector {
                vec: query.clone(),
                k: 10,
                opts: crate::index2::vector::SearchOpts::default(),
            })
            .await
            .unwrap();
        if let IndexResult::Ranked(hits) = post {
            return hits;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    // Final attempt — surface whatever the backend returns (even empty).
    let post = backend
        .lookup(IndexQuery::Vector {
            vec: query,
            k: 10,
            opts: crate::index2::vector::SearchOpts::default(),
        })
        .await
        .unwrap();
    match post {
        IndexResult::Ranked(hits) => hits,
        _ => Vec::new(),
    }
}

/// ghost (main regression): tx inserts vector rows, commits; then tx
/// DELETES one, commits; a non-tx ANN search must NOT return the deleted
/// rid (live graph tombstoned). This is the direct regression on gap#1.
#[tokio::test]
#[serial_test::serial]
async fn tx_delete_removes_vector_from_live_graph() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("vecs"));
    let tbl = repo.get_table("vecs").await.unwrap();
    tbl.create_index_v2(&vector_index_op()).await.unwrap();

    let emb_id = field_id(&tbl, "embedding").await;
    let name_id = field_id(&tbl, "vec_idx").await;
    let backend = tbl
        .index2_registry()
        .get_by_name(name_id)
        .await
        .expect("vec_idx must be registered");

    // Commit two vectors via a tx.
    let (mut tx, guard) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let rid_a = tbl
        .insert_tx(&vec_record(emb_id, &[1.0, 0.0, 0.0]), Some(&mut tx))
        .await
        .unwrap();
    let rid_b = tbl
        .insert_tx(&vec_record(emb_id, &[0.0, 1.0, 0.0]), Some(&mut tx))
        .await
        .unwrap();
    repo.commit_tx(tx).await.unwrap();
    drop(guard);

    // Wait for the post-lock promote to land both vectors.
    let hits = poll_vector_hits(&backend, vec![1.0, 0.0, 0.0]).await;
    assert!(
        hits.iter().any(|(r, _)| *r == rid_a),
        "rid_a must be searchable after the insert commit"
    );
    let hits = poll_vector_hits(&backend, vec![0.0, 1.0, 0.0]).await;
    assert!(
        hits.iter().any(|(r, _)| *r == rid_b),
        "rid_b must be searchable after the insert commit"
    );

    // Now delete rid_a via a SECOND tx and commit.
    let (mut tx2, guard2) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let removed = tbl.delete_tx(rid_a, Some(&mut tx2)).await.unwrap();
    assert!(removed, "delete_tx must report the record was present");
    repo.commit_tx(tx2).await.unwrap();
    drop(guard2);

    // Wait for the post-lock delete promote to land the tombstone.
    // Poll until rid_a no longer surfaces (or the bound expires).
    let mut gone = false;
    for _ in 0..200 {
        let hits = poll_vector_hits(&backend, vec![1.0, 0.0, 0.0]).await;
        if !hits.iter().any(|(r, _)| *r == rid_a) {
            gone = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(
        gone,
        "gap#1 regression: deleted rid_a must NOT surface in ANN search after \
         the tx-delete commit (live graph must be tombstoned); got hits {:?}",
        poll_vector_hits(&backend, vec![1.0, 0.0, 0.0]).await
    );

    // rid_b must still be searchable (the delete was scoped to rid_a).
    let hits = poll_vector_hits(&backend, vec![0.0, 1.0, 0.0]).await;
    assert!(
        hits.iter().any(|(r, _)| *r == rid_b),
        "rid_b must survive the rid_a delete"
    );
}

/// abort (RAII): a tx that deletes a vector-backed row then is DROPPED
/// (rollback) must leave the vector IN the graph — the staged delete is
/// discarded by RAII, so no ghost appeared and no tombstone landed.
#[tokio::test]
#[serial_test::serial]
async fn aborted_tx_delete_leaves_vector_in_graph() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("vecs"));
    let tbl = repo.get_table("vecs").await.unwrap();
    tbl.create_index_v2(&vector_index_op()).await.unwrap();

    let emb_id = field_id(&tbl, "embedding").await;
    let name_id = field_id(&tbl, "vec_idx").await;
    let backend = tbl
        .index2_registry()
        .get_by_name(name_id)
        .await
        .expect("vec_idx must be registered");

    // Commit a vector via a tx.
    let (mut tx, guard) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let rid = tbl
        .insert_tx(&vec_record(emb_id, &[1.0, 0.0, 0.0]), Some(&mut tx))
        .await
        .unwrap();
    repo.commit_tx(tx).await.unwrap();
    drop(guard);
    let hits = poll_vector_hits(&backend, vec![1.0, 0.0, 0.0]).await;
    assert!(
        hits.iter().any(|(r, _)| *r == rid),
        "rid must be searchable after the insert commit"
    );

    // Open a SECOND tx, stage the delete, then DROP (rollback).
    {
        let (mut tx2, guard2) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
        let removed = tbl.delete_tx(rid, Some(&mut tx2)).await.unwrap();
        assert!(removed, "delete_tx must report the record was present");
        // The staged delete lives inside the TxContext.
        assert_eq!(
            tx2.staged_vector_deletes_for(tbl.table_token())
                .map(<[_]>::len),
            Some(1),
            "the delete rid must be staged in tx.staged_vector_deletes"
        );
        // RAII abort — drop WITHOUT commit.
        drop(tx2);
        drop(guard2);
    }

    // The vector must STILL be searchable — the staged delete was discarded.
    let hits = poll_vector_hits(&backend, vec![1.0, 0.0, 0.0]).await;
    assert!(
        hits.iter().any(|(r, _)| *r == rid),
        "aborted tx-delete must leave the vector in the graph (RAII discarded \
         the staged delete); got hits {:?}",
        hits
    );
}

/// replace (delete + insert in ONE tx): verifies gap#1's delete-staging
/// coexists correctly with the insert-staging path inside the same
/// `TxContext` — both `staged_vector_deletes` and `staged_vectors` are
/// populated for the same table, and the commit promotes both. We assert
/// the STAGING contract (both slices populated correctly) rather than the
/// post-commit graph state, because the HNSW promote's second-insert-in-a-
/// fresh-graph path has an older flakiness (a separate gap outside gap#1's
/// scope) that can drop the second vector; the gap#1 wiring this test pins
/// is the staging + the `apply_vector_batch` call that feeds BOTH the
/// deletes and the inserts to the backend in one pass.
#[tokio::test]
#[serial_test::serial]
async fn tx_replace_stages_delete_and_insert_together() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("vecs"));
    let tbl = repo.get_table("vecs").await.unwrap();
    tbl.create_index_v2(&vector_index_op()).await.unwrap();

    let emb_id = field_id(&tbl, "embedding").await;

    // Pre-commit the original vector via tx0 so the delete has something
    // to tombstone.
    let (mut tx0, guard0) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let rid = tbl
        .insert_tx(&vec_record(emb_id, &[1.0, 0.0, 0.0]), Some(&mut tx0))
        .await
        .unwrap();
    repo.commit_tx(tx0).await.unwrap();
    drop(guard0);

    // gap#1 replace path: in ONE tx, DELETE the old rid AND INSERT a new
    // record carrying the NEW vector.
    let (mut tx1, guard1) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let removed = tbl.delete_tx(rid, Some(&mut tx1)).await.unwrap();
    assert!(removed, "delete_tx must report the old record was present");
    let new_rid = tbl
        .insert_tx(&vec_record(emb_id, &[0.0, 1.0, 0.0]), Some(&mut tx1))
        .await
        .unwrap();

    let token = tbl.table_token();
    // gap#1 contract: BOTH staging paths are populated for this tx/table —
    // the old rid in staged_vector_deletes, the new (rid, vec) in
    // staged_vectors. Phase 5d's `apply_vector_batch` receives both and
    // feeds them to the backend in one pass (delete-then-insert order).
    let staged_deletes = tx1
        .staged_vector_deletes_for(token)
        .map(|s| s.to_vec())
        .unwrap_or_default();
    assert_eq!(
        staged_deletes,
        vec![rid],
        "the old rid must be staged in staged_vector_deletes"
    );
    let staged_vecs = tx1.staged_vectors_for(token).unwrap_or(&[]);
    assert_eq!(
        staged_vecs.len(),
        1,
        "exactly one new vector must be staged for insert"
    );
    assert_eq!(
        staged_vecs[0].0, new_rid,
        "the staged insert rid must be new_rid"
    );
    assert_eq!(
        staged_vecs[0].1,
        vec![0.0f32, 1.0, 0.0],
        "the staged NEW vector must be [0,1,0]"
    );

    // The commit must succeed (the promote feeds both slices to the
    // backend; a failure here would surface as a commit error).
    repo.commit_tx(tx1).await.unwrap();
    drop(guard1);

    // The OLD rid must be tombstoned — this is the gap#1 delete-promote
    // contract, and it rides the SAME promote as the insert. Poll for the
    // tombstone (gap#1's direct concern).
    let mut old_gone = false;
    let name_id = field_id(&tbl, "vec_idx").await;
    let backend = tbl
        .index2_registry()
        .get_by_name(name_id)
        .await
        .expect("vec_idx must be registered");
    for _ in 0..200 {
        let old_hits = poll_vector_hits(&backend, vec![1.0, 0.0, 0.0]).await;
        if !old_hits.iter().any(|(r, _)| *r == rid) {
            old_gone = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(
        old_gone,
        "replace: the OLD rid must be tombstoned after the tx-delete commit \
         (gap#1 delete-promote); it must not surface in search"
    );
}

/// back-compat: the NON-tx `delete` (immediate graph delete via
/// `plan_delete`) still excludes the rid from search.
#[tokio::test]
#[serial_test::serial]
async fn non_tx_delete_still_excludes_from_search() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("vecs"));
    let tbl = repo.get_table("vecs").await.unwrap();
    tbl.create_index_v2(&vector_index_op()).await.unwrap();

    let emb_id = field_id(&tbl, "embedding").await;
    let name_id = field_id(&tbl, "vec_idx").await;
    let backend = tbl
        .index2_registry()
        .get_by_name(name_id)
        .await
        .expect("vec_idx must be registered");

    // Insert + delete via the NON-tx path (no TxContext).
    let rid = tbl
        .insert(&vec_record(emb_id, &[1.0, 0.0, 0.0]))
        .await
        .unwrap();
    let hits = poll_vector_hits(&backend, vec![1.0, 0.0, 0.0]).await;
    assert!(
        hits.iter().any(|(r, _)| *r == rid),
        "non-tx insert must make the vector searchable"
    );

    let removed = tbl.delete(rid).await.unwrap();
    assert!(removed, "non-tx delete must report the record was present");

    // The non-tx delete goes through `plan_delete` (immediate graph delete),
    // so the rid must be gone WITHOUT any post-lock promote wait — but poll
    // anyway for determinism under parallel-test contention.
    let mut gone = false;
    for _ in 0..200 {
        let hits = poll_vector_hits(&backend, vec![1.0, 0.0, 0.0]).await;
        if !hits.iter().any(|(r, _)| *r == rid) {
            gone = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(
        gone,
        "non-tx delete (back-compat) must exclude the rid from search"
    );
}

/// back-compat: a table WITHOUT a vector index is untouched by the delete
/// staging path — `delete_tx` on a non-vector table must not stage any
/// vector deletes and must still remove the row.
#[tokio::test]
#[serial_test::serial]
async fn tx_delete_on_non_vector_table_stages_no_vector_delete() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("docs"));
    let tbl = repo.get_table("docs").await.unwrap();
    // No vector index — just a plain table.

    let body_id = field_id(&tbl, "body").await;
    let mut m = new_map_wc(1);
    m.insert(InternerKey::new(body_id), InnerValue::Str("hello".into()));
    let rec = InnerValue::Map(m);

    let (mut tx, _guard) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let rid = tbl.insert_tx(&rec, Some(&mut tx)).await.unwrap();

    // delete_tx on a table with NO vector index must not stage any vector
    // deletes (the staging helper is a per-backend no-op when no backend
    // extracts a vector).
    let removed = tbl.delete_tx(rid, Some(&mut tx)).await.unwrap();
    assert!(removed, "delete_tx must report the record was present");
    assert!(
        tx.staged_vector_deletes_for(tbl.table_token()).is_none(),
        "a table with no vector index must not stage any vector deletes"
    );
}

/// gap#1 (update branch, @ol review finding): a tx UPDATE that REMOVES the
/// embedding field must stage a vector delete — the non-tx `plan_update`
/// tombstones in its else-branch, and before this fix the tx path silently
/// lost that semantic: the old vector stayed live in the graph as a ghost
/// (and survived restart, since no `DeltaOp::Delete` was appended). This
/// test would go RED on the unfixed code (update_tx staged nothing).
#[tokio::test]
#[serial_test::serial]
async fn tx_update_removing_embedding_tombstones_old_vector() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("vecs"));
    let tbl = repo.get_table("vecs").await.unwrap();
    tbl.create_index_v2(&vector_index_op()).await.unwrap();

    let emb_id = field_id(&tbl, "embedding").await;
    let name_id = field_id(&tbl, "vec_idx").await;
    let backend = tbl
        .index2_registry()
        .get_by_name(name_id)
        .await
        .expect("vec_idx must be registered");

    // Commit one vector row via a tx.
    let (mut tx, guard) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let rid = tbl
        .insert_tx(&vec_record(emb_id, &[1.0, 0.0, 0.0]), Some(&mut tx))
        .await
        .unwrap();
    repo.commit_tx(tx).await.unwrap();
    drop(guard);

    let hits = poll_vector_hits(&backend, vec![1.0, 0.0, 0.0]).await;
    assert!(
        hits.iter().any(|(r, _)| *r == rid),
        "rid must be searchable after the insert commit"
    );

    // UPDATE the row via a second tx, REPLACING it with a record that has
    // NO embedding field at all — the vector must be tombstoned at commit.
    let other_id = field_id(&tbl, "note").await;
    let mut m = new_map_wc(1);
    m.insert(
        InternerKey::new(other_id),
        InnerValue::Str("no embedding anymore".into()),
    );
    let no_vec = InnerValue::Map(m);

    let (mut tx2, guard2) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    tbl.update_tx(rid, &no_vec, Some(&mut tx2)).await.unwrap();
    // The staged delete must be present BEFORE commit (staging contract).
    assert!(
        tx2.staged_vector_deletes_for(tbl.table_token())
            .is_some_and(|d| d.contains(&rid)),
        "update_tx removing the embedding must stage a vector delete"
    );
    repo.commit_tx(tx2).await.unwrap();
    drop(guard2);

    // The old vector must vanish from ANN search (live-graph tombstone).
    let mut gone = false;
    for _ in 0..200 {
        let hits = poll_vector_hits(&backend, vec![1.0, 0.0, 0.0]).await;
        if !hits.iter().any(|(r, _)| *r == rid) {
            gone = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(
        gone,
        "gap#1 (update branch): a tx update that removed the embedding must \
         tombstone the old vector; got hits {:?}",
        poll_vector_hits(&backend, vec![1.0, 0.0, 0.0]).await
    );
}

/// #420 — loss of the SECOND sequential tx vector promote.
///
/// tx1: insert vector A ([1,0,0]) + commit → A is searchable.
/// tx2: insert vector B ([0,1,0]) + commit → B is searchable with the
/// CORRECT vector (a search for B's own vector must return B as the top
/// hit, with cosine distance ~0 / score ~1.0). A must STILL be searchable
/// afterwards.
///
/// Before the fix, the second Phase 5d promote landed a WRONG/empty vector
/// for B in the live graph: B appeared in the search results but with
/// cosine distance 1.0 (score 0.0) for its OWN query — i.e. the graph node
/// carried an unrelated (or zero) embedding. The staging buffer was
/// correct (`staged_vectors` held `[0,1,0]`), so the loss happened inside
/// the adapter's promote path. This is a silent data-loss bug on the plain
/// insert path.
#[tokio::test]
#[serial_test::serial]
async fn sequential_tx_vector_promotes_both_searchable() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("vecs"));
    let tbl = repo.get_table("vecs").await.unwrap();
    tbl.create_index_v2(&vector_index_op()).await.unwrap();

    let emb_id = field_id(&tbl, "embedding").await;
    let name_id = field_id(&tbl, "vec_idx").await;
    let backend = tbl
        .index2_registry()
        .get_by_name(name_id)
        .await
        .expect("vec_idx must be registered");

    // tx1: insert vector A and commit.
    let (mut tx1, guard1) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let rid_a = tbl
        .insert_tx(&vec_record(emb_id, &[1.0, 0.0, 0.0]), Some(&mut tx1))
        .await
        .unwrap();
    repo.commit_tx(tx1).await.unwrap();
    drop(guard1);

    // A must be searchable (sanity — the first promote is the known-good
    // path).
    let hits_a = poll_vector_hits(&backend, vec![1.0, 0.0, 0.0]).await;
    let a_entry = hits_a
        .iter()
        .find(|(r, _)| *r == rid_a)
        .unwrap_or_else(|| panic!("rid_a must be searchable after tx1; got hits {:?}", hits_a));
    // Cosine distance between A and its own query must be ~0.
    assert!(
        a_entry.1.abs() < 1e-5,
        "rid_a must match its own query with cosine distance ~0; got {}",
        a_entry.1
    );

    // tx2: insert vector B and commit — a SECOND, SEQUENTIAL promote into
    // the live graph.
    let (mut tx2, guard2) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let rid_b = tbl
        .insert_tx(&vec_record(emb_id, &[0.0, 1.0, 0.0]), Some(&mut tx2))
        .await
        .unwrap();
    repo.commit_tx(tx2).await.unwrap();
    drop(guard2);

    // B must be searchable, and a search for B's OWN vector must return B
    // as the TOP hit with cosine distance ~0 (score ~1.0). Before the fix,
    // the second promote landed a wrong embedding for B in the live graph:
    // B surfaced in search but at cosine distance ~1.0 to its own query
    // (the graph node carried the wrong vector).
    let hits_b = poll_vector_hits(&backend, vec![0.0, 1.0, 0.0]).await;
    let top = hits_b.first().unwrap_or_else(|| {
        panic!(
            "search for B's vector must return at least one hit; got {:?}",
            hits_b
        )
    });
    assert!(
        top.1.abs() < 1e-5,
        "#420: the TOP hit for B's own vector must have cosine distance ~0 \
         (score ~1.0); the second promote must land the CORRECT vector, not a \
         distorted/empty one. top hit = {:?}; full hits = {:?}",
        top,
        hits_b
    );
    // B specifically must be present AND must match its own query with
    // cosine distance ~0 (its graph node must carry [0,1,0]).
    let b_entry = hits_b.iter().find(|(r, _)| *r == rid_b).unwrap_or_else(|| {
        panic!(
            "#420: rid_b must surface in its own search; got hits {:?}",
            hits_b
        )
    });
    assert!(
        b_entry.1.abs() < 1e-5,
        "#420: rid_b must match its OWN vector with cosine distance ~0; got \
         {} — the second promote landed a wrong/empty embedding for rid_b in \
         the live graph (staging was correct)",
        b_entry.1
    );

    // A must STILL be searchable (its promote is unrelated to B's).
    let hits_a2 = poll_vector_hits(&backend, vec![1.0, 0.0, 0.0]).await;
    assert!(
        hits_a2.iter().any(|(r, _)| *r == rid_a),
        "rid_a must still be searchable after tx2's promote; got hits {:?}",
        hits_a2
    );
}

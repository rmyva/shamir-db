//! HIGH-6: routing index posting writes through tx so rollback is clean.
//!
//! Verifies that an `insert_tx` / `update_tx` / `delete_tx` performed
//! against a `TxContext` that is **dropped without commit** leaves no
//! visible postings on the relevant index data structures.
//!
//! Scope split (as of HIGH-6 staging-side fix):
//!
//! * `index2` stateless backends (FTS / functional / btree) — ops are
//!   accumulated in `tx.index_write_set` (via `stage_mutation`) and
//!   never applied to the live store before commit. RAII drop of the
//!   `TxContext` simply releases the vec. Verified by the
//!   `apply_index_ops_tx_drop_leaves_no_postings` unit test in
//!   `index2/write_ops.rs`.
//! * `index2::VectorBackend` (HNSW / brute-force) — `plan_insert_tx`
//!   is a no-op on the live graph for a tx; the executor stages the
//!   vector tx-locally in `TxContext::staged_vectors` instead of the
//!   committed graph. Non-tx queries (`adapter.search(_, _, None)`)
//!   still see only the committed graph and therefore do not surface
//!   the staged vector. Verified end-to-end below.
//! * base_index `IndexManager` / `SortedIndexManager` — NOW routed through
//!   `insert_tx` / `update_tx` / `delete_tx`. The base_index/sorted planners
//!   emit `IndexWriteOp`s into `tx.index_write_set` carrying the exact
//!   physical key scheme the readers expect; a committed tx applies them
//!   (Phase 5c-d), a dropped tx discards them. Asserted in
//!   `base_index_routed_through_insert_tx_on_commit` here and in
//!   `base_index_tx_tests`.
//! * Commit-time apply for `tx.index_write_set` ops
//!   (`commit.rs::commit_tx_inner` Phase 5c-d) — DONE. On the happy
//!   path a committed tx applies its staged postings to `info_store`
//!   (and broadcasts BumpFtsStats to the live backends) via
//!   `apply_index_ops_at_commit`, so the record is findable through
//!   the index immediately after commit — asserted by
//!   `base_index_routed_through_insert_tx_on_commit` here. The same
//!   ops are also emitted into the WAL entry by `wal_ops_from_tx`, so
//!   crash recovery re-applies them idempotently if the process dies
//!   between the WAL begin and commit markers.

use std::sync::Arc;

use shamir_query_types::admin::CreateIndexOp;
use shamir_storage::storage_in_memory::InMemoryRepo;
use shamir_tx::IsolationLevel;
use shamir_types::core::interner::{InternerKey, TouchInd};
use shamir_types::types::common::new_map_wc;
use shamir_types::types::value::InnerValue;

use crate::repo::repo_instance::RepoInstance;
use crate::repo::repo_types::BoxRepo;
use crate::table::TableConfig;

fn make_repo() -> RepoInstance {
    let repo = Arc::new(InMemoryRepo::new());
    RepoInstance::new("test".into(), BoxRepo::InMemory(repo), Vec::new())
}

/// HIGH-6 happy-path: drop a tx after inserting a record carrying a
/// vector field, and confirm the committed HNSW graph never saw the
/// vector. Pre-fix, `VectorBackend::plan_insert` would call
/// `adapter.upsert(rid, &v, None)` synchronously, mutating the live
/// HNSW graph regardless of tx state, so the vector would remain
/// findable forever — a ghost posting.
#[tokio::test]
async fn dropped_tx_vector_index_leaves_no_postings() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("vecs"));
    let tbl = repo.get_table("vecs").await.unwrap();

    // Vector index over field "embedding", dim=3.
    let op = CreateIndexOp {
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
    };
    tbl.create_index_v2(&op).await.unwrap();

    let emb_key_id = {
        let interner = tbl.interner().get().await.unwrap();
        match interner.touch_ind("embedding").unwrap() {
            TouchInd::Exists(k) | TouchInd::New(k) => k.id(),
        }
    };

    fn make_record(emb_key_id: u64, vec: &[f64]) -> InnerValue {
        let mut m = new_map_wc(1);
        m.insert(
            InternerKey::new(emb_key_id),
            InnerValue::List(vec.iter().map(|f| InnerValue::F64(*f)).collect()),
        );
        InnerValue::Map(m)
    }

    // Pre-commit one anchor vector via non-tx insert (so the graph
    // isn't empty and a search has something legitimate to return).
    let anchor_rid = tbl
        .insert(&make_record(emb_key_id, &[0.0, 1.0, 0.0]))
        .await
        .unwrap();

    // Resolve the VectorBackend so we can talk to it directly.
    let backend = {
        let interner = tbl.interner().get().await.unwrap();
        let name_id = match interner.touch_ind("vec_idx").unwrap() {
            TouchInd::Exists(k) | TouchInd::New(k) => k.id(),
        };
        tbl.index2_registry()
            .get_by_name(name_id)
            .await
            .expect("vec_idx must be registered")
    };

    // Open a tx, insert via insert_tx, then drop without commit.
    let staged_rid = {
        let (mut tx, guard) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
        let rid = tbl
            .insert_tx(&make_record(emb_key_id, &[1.0, 0.0, 0.0]), Some(&mut tx))
            .await
            .unwrap();
        // The vector should be present in the tx's staged view (via
        // TxContext::staged_vectors) but invisible to non-tx queries.
        drop(tx);
        drop(guard);
        rid
    };
    // Brief sleep to let any background HNSW writes settle. Pre-fix,
    // `plan_insert` would synchronously call
    // `adapter.upsert(_, _, None)` which inserts into the live HNSW
    // graph; that insert is performed under `tokio::task::spawn_blocking`
    // so we yield once to let it complete before asserting.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Non-tx search for the staged vector. Pre-fix, the HNSW graph
    // would surface the rolled-back rid; post-fix, only `anchor_rid`
    // should appear because the staged vector lives in per-tx state.
    use crate::index2::backend::{IndexQuery, IndexResult};
    let result = backend
        .lookup(IndexQuery::Vector {
            vec: vec![1.0, 0.0, 0.0],
            k: 10,
            opts: crate::index2::vector::SearchOpts::default(),
        })
        .await
        .unwrap();
    match result {
        IndexResult::Ranked(hits) => {
            let rids: Vec<_> = hits.iter().map(|(r, _)| *r).collect();
            assert!(
                !rids.contains(&staged_rid),
                "rolled-back tx must not leave a vector posting in the committed \
                 HNSW graph (HIGH-6); got {:?}, staged_rid={:?}, anchor_rid={:?}",
                rids,
                staged_rid,
                anchor_rid
            );
            assert!(
                rids.contains(&anchor_rid),
                "committed anchor must still be findable (sanity check); got {:?}",
                rids
            );
        }
        _ => panic!("Vector lookup must return Ranked"),
    }
}

/// HIGH-6 (closed): base_index `IndexManager` (`tbl.create_index` →
/// `tbl.lookup_by_index`) IS now wired into `insert_tx`. The posting
/// writes are staged into `tx.index_write_set` by `insert_tx` (via the
/// base_index planners) and applied to `info_store` by the commit pipeline
/// (Phase 5c-d). A committed `insert_tx` therefore makes the record
/// findable through the base_index secondary index — the gap this test
/// originally documented is closed.
///
/// (Rollback safety — a dropped `insert_tx` leaving the base_index index
/// empty — is asserted separately in
/// `base_index_tx_tests::dropped_tx_no_secondary_postings`.)
#[tokio::test]
async fn base_index_routed_through_insert_tx_on_commit() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();

    // base_index btree-style index on field "name".
    repo.create_index("t", "by_name", &["name"]).await.unwrap();

    let name_key_id = {
        let interner = tbl.interner().get().await.unwrap();
        match interner.touch_ind("name").unwrap() {
            TouchInd::Exists(k) | TouchInd::New(k) => k.id(),
        }
    };

    fn make_record(name_key_id: u64, name: &str) -> InnerValue {
        let mut m = new_map_wc(1);
        m.insert(InternerKey::new(name_key_id), InnerValue::Str(name.into()));
        InnerValue::Map(m)
    }

    // Insert via tx and COMMIT (not rollback).
    let (mut tx, _g) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let rid = tbl
        .insert_tx(&make_record(name_key_id, "alice"), Some(&mut tx))
        .await
        .unwrap();
    let _ = repo.commit_tx(tx).await.unwrap();

    // base_index lookup. Post-HIGH-6: the staged posting is applied at
    // commit, so the record is findable via the base_index index.
    let hits = tbl
        .lookup_by_index("by_name", &[InnerValue::Str("alice".into())])
        .await
        .unwrap();
    assert!(
        hits.contains(&rid),
        "HIGH-6 (closed): committed insert_tx must populate the base_index \
         IndexManager posting; got {:?}, rid {:?}",
        hits,
        rid
    );
}

/// HIGH-6: end-to-end RAII rollback for the data side.
///
/// This is the non-vector half of the contract: a tx that staged a
/// record via `insert_tx` and is then dropped must leave no record
/// visible via the non-tx `get`. Already covered by
/// `abort_path_drop_tx_no_side_effects` in `acceptance_tests.rs`;
/// re-asserted here for completeness alongside the index-side
/// guarantees so the rollback contract is documented in one place.
#[tokio::test]
async fn dropped_tx_data_side_leaves_no_record() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("users"));
    let tbl = repo.get_table("users").await.unwrap();

    let staged_rid = {
        let (mut tx, guard) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
        let rid = tbl
            .insert_tx(&InnerValue::Str("ephemeral".into()), Some(&mut tx))
            .await
            .unwrap();
        // Within the tx the data may or may not be visible (see
        // read_inside_tx_sees_committed_but_not_own_staged_writes for
        // the current read-your-own-writes story); after drop nothing
        // should reach the live data_store.
        drop(tx);
        drop(guard);
        rid
    };

    assert!(
        tbl.get(staged_rid).await.is_err(),
        "RAII rollback must drop staged data writes (HIGH-6)"
    );
}

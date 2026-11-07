use crate::adapter::VectorAdapter;
use crate::brute_force::BruteForceAdapter;
use crate::hnsw_adapter::{HnswAdapter, HnswConfig};
use crate::vector_backend::VectorBackend;
use shamir_index_core::backend::{IndexBackend, IndexQuery, IndexResult};
use shamir_index_core::descriptor::IndexDescriptor;
use shamir_index_core::kind::{IndexKind, VectorBackendRef, VectorConfig, VectorMetric};
use shamir_storage::storage_in_memory::InMemoryStore;
use shamir_storage::types::Store;
use shamir_types::core::interner::{Interner, InternerKey, TouchInd};
use shamir_types::types::common::new_map_wc;
use shamir_types::types::record_id::RecordId;
use shamir_types::types::value::InnerValue;
use smallvec::SmallVec;
use std::sync::Arc;

fn intern(i: &Interner, s: &str) -> u64 {
    match i.touch_ind(s).unwrap() {
        TouchInd::Exists(k) | TouchInd::New(k) => k.id(),
    }
}

fn make_rec(interner: &Interner, embedding: &[f64]) -> InnerValue {
    let mut m = new_map_wc(1);
    m.insert(
        InternerKey::new(intern(interner, "embedding")),
        InnerValue::List(embedding.iter().map(|f| InnerValue::F64(*f)).collect()),
    );
    InnerValue::Map(m)
}

fn make_backend(interner: &Interner) -> VectorBackend {
    let desc = IndexDescriptor::new(
        30,
        "vec_idx",
        intern(interner, "vec_idx"),
        SmallVec::new(),
        IndexKind::Vector(Box::new(VectorConfig {
            dim: 3,
            metric: VectorMetric::Cosine,
            backend: VectorBackendRef::InProcessHnsw {
                ef_construct: 200,
                m: 16,
            },
            quantization: None,
        })),
    );
    let adapter = Arc::new(BruteForceAdapter::new(3, VectorMetric::Cosine));
    VectorBackend::new(desc, vec![intern(interner, "embedding")], adapter, None)
}

/// RV-TUNABLES-A3D3695D: PRODUCTION construction (`VectorBackend::new`) must
/// honour the documented `SHAMIR_VECTOR_SNAPSHOT_DELTA_THRESHOLD` startup
/// override; an unset/garbage value falls back to the compiled-in default.
/// Fails if backend initialisation goes back to the compiled-in constant or
/// the env helper stops reading the variable. (Tests run one per process
/// under nextest, so the process-global env write cannot leak.)
#[tokio::test]
async fn rv_tunables_a3d3695d_backend_new_reads_snapshot_threshold_env() {
    use shamir_tunables::instance_defaults::{
        VECTOR_SNAPSHOT_DELTA_THRESHOLD, VECTOR_SNAPSHOT_DELTA_THRESHOLD_ENV,
    };
    let i = Interner::new();

    std::env::set_var(VECTOR_SNAPSHOT_DELTA_THRESHOLD_ENV, "1234");
    assert_eq!(make_backend(&i).snapshot_threshold_for_test(), 1234);

    std::env::set_var(VECTOR_SNAPSHOT_DELTA_THRESHOLD_ENV, "not-a-number");
    assert_eq!(
        make_backend(&i).snapshot_threshold_for_test(),
        VECTOR_SNAPSHOT_DELTA_THRESHOLD
    );

    std::env::remove_var(VECTOR_SNAPSHOT_DELTA_THRESHOLD_ENV);
    assert_eq!(
        make_backend(&i).snapshot_threshold_for_test(),
        VECTOR_SNAPSHOT_DELTA_THRESHOLD
    );
}

#[tokio::test]
async fn insert_and_search() {
    let i = Interner::new();
    let backend = make_backend(&i);

    let r1 = RecordId::new();
    let r2 = RecordId::new();
    backend
        .plan_insert(r1, &make_rec(&i, &[1.0, 0.0, 0.0]))
        .await
        .unwrap();
    backend
        .plan_insert(r2, &make_rec(&i, &[0.0, 1.0, 0.0]))
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let result = backend
        .lookup(IndexQuery::Vector {
            vec: vec![1.0, 0.0, 0.0],
            k: 1,
            opts: crate::SearchOpts::default(),
        })
        .await
        .unwrap();

    match result {
        IndexResult::Ranked(ranked) => {
            assert_eq!(ranked.len(), 1);
            assert_eq!(ranked[0].0, r1);
        }
        _ => panic!("expected Ranked"),
    }
}

#[tokio::test]
async fn delete_excludes_from_search() {
    let i = Interner::new();
    let backend = make_backend(&i);

    let r1 = RecordId::new();
    let rec = make_rec(&i, &[1.0, 0.0, 0.0]);
    backend.plan_insert(r1, &rec).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    backend.plan_delete(r1, &rec).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let result = backend
        .lookup(IndexQuery::Vector {
            vec: vec![1.0, 0.0, 0.0],
            k: 10,
            opts: crate::SearchOpts::default(),
        })
        .await
        .unwrap();

    match result {
        IndexResult::Ranked(ranked) => assert!(ranked.is_empty()),
        _ => panic!("expected Ranked"),
    }
}

#[tokio::test]
async fn lookup_tx_none_matches_lookup() {
    let i = Interner::new();
    let backend = make_backend(&i);

    let r1 = RecordId::new();
    let r2 = RecordId::new();
    backend
        .plan_insert(r1, &make_rec(&i, &[1.0, 0.0, 0.0]))
        .await
        .unwrap();
    backend
        .plan_insert(r2, &make_rec(&i, &[0.0, 1.0, 0.0]))
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let q = IndexQuery::Vector {
        vec: vec![1.0, 0.0, 0.0],
        k: 2,
        opts: crate::SearchOpts::default(),
    };
    let via_lookup = backend
        .lookup(IndexQuery::Vector {
            vec: vec![1.0, 0.0, 0.0],
            k: 2,
            opts: crate::SearchOpts::default(),
        })
        .await
        .unwrap();
    let via_tx = backend.lookup_tx(0, q, None, None).await.unwrap();
    match (via_lookup, via_tx) {
        (IndexResult::Ranked(a), IndexResult::Ranked(b)) => {
            assert_eq!(a.len(), b.len());
        }
        _ => panic!("expected Ranked results"),
    }
}

#[tokio::test]
async fn lookup_tx_some_includes_staged_vector() {
    let i = Interner::new();

    let desc = IndexDescriptor::new(
        31,
        "vec_tx",
        intern(&i, "vec_tx"),
        SmallVec::new(),
        IndexKind::Vector(Box::new(VectorConfig {
            dim: 3,
            metric: VectorMetric::Cosine,
            backend: VectorBackendRef::InProcessHnsw {
                ef_construct: 200,
                m: 16,
            },
            quantization: None,
        })),
    );
    let adapter: Arc<dyn VectorAdapter> = Arc::new(HnswAdapter::new(
        3,
        VectorMetric::Cosine,
        HnswConfig {
            max_elements: 100,
            ..Default::default()
        },
    ));
    let backend = VectorBackend::new(desc, vec![intern(&i, "embedding")], adapter, None);

    // Commit one vector via non-tx upsert.
    let committed_rid = RecordId::new();
    backend
        .adapter
        .load()
        .adapter
        .upsert(committed_rid, &[1.0, 0.0, 0.0])
        .await
        .unwrap();

    // The tx's own staged vector (what the executor buffers in
    // `TxContext::staged_vectors_for(token)`), very close to query.
    let staged_rid = RecordId::new();
    let staged: Vec<(RecordId, Vec<f32>)> = vec![(staged_rid, vec![0.9, 0.1, 0.0])];

    // Non-tx lookup sees only committed vector.
    let non_tx = backend
        .lookup(IndexQuery::Vector {
            vec: vec![1.0, 0.0, 0.0],
            k: 2,
            opts: crate::SearchOpts::default(),
        })
        .await
        .unwrap();

    // tx-aware lookup gets the staged slice threaded in by the caller.
    let in_tx = backend
        .lookup_tx(
            0,
            IndexQuery::Vector {
                vec: vec![1.0, 0.0, 0.0],
                k: 2,
                opts: crate::SearchOpts::default(),
            },
            None,
            Some(&staged),
        )
        .await
        .unwrap();

    let non_tx_rids: Vec<RecordId> = match non_tx {
        IndexResult::Ranked(r) => r.into_iter().map(|(rid, _)| rid).collect(),
        _ => panic!("expected Ranked"),
    };
    let in_tx_rids: Vec<RecordId> = match in_tx {
        IndexResult::Ranked(r) => r.into_iter().map(|(rid, _)| rid).collect(),
        _ => panic!("expected Ranked"),
    };

    assert!(
        !non_tx_rids.contains(&staged_rid),
        "non-tx must not see staged vector"
    );
    assert!(
        in_tx_rids.contains(&staged_rid),
        "in-tx lookup must merge staged vector: got {in_tx_rids:?}"
    );
    assert!(
        non_tx_rids.contains(&committed_rid),
        "non-tx must see committed vector"
    );
    assert!(
        in_tx_rids.contains(&committed_rid),
        "in-tx must see committed vector"
    );
}

#[tokio::test]
async fn rebuild_from_store() {
    let i = Interner::new();
    let store: Arc<dyn Store> = Arc::new(InMemoryStore::new());

    // Write 3 records into the store as (RecordId → InnerValue).
    let r1 = RecordId::new();
    let r2 = RecordId::new();
    let r3 = RecordId::new();
    let rec1 = make_rec(&i, &[1.0, 0.0, 0.0]);
    let rec2 = make_rec(&i, &[0.0, 1.0, 0.0]);
    let rec3 = make_rec(&i, &[0.9, 0.1, 0.0]);
    store
        .set(r1.to_bytes().into(), rec1.to_bytes().unwrap())
        .await
        .unwrap();
    store
        .set(r2.to_bytes().into(), rec2.to_bytes().unwrap())
        .await
        .unwrap();
    store
        .set(r3.to_bytes().into(), rec3.to_bytes().unwrap())
        .await
        .unwrap();

    // Create a fresh backend (empty adapter) and rebuild from store.
    let backend = make_backend(&i);
    backend.rebuild(Arc::clone(&store)).await.unwrap();

    // Search for [1,0,0] — top-2 should contain r1 (closest).
    let result = backend
        .lookup(IndexQuery::Vector {
            vec: vec![1.0, 0.0, 0.0],
            k: 2,
            opts: crate::SearchOpts::default(),
        })
        .await
        .unwrap();
    match result {
        IndexResult::Ranked(ranked) => {
            assert_eq!(ranked.len(), 2, "expected 2 results, got {ranked:?}");
            assert_eq!(ranked[0].0, r1, "r1 should be the closest");
        }
        _ => panic!("expected Ranked"),
    }
}

/// V0.2: rebuild must populate the graph with ALL records from the store
/// via the batched `upsert_batch` path (single rayon parallel_insert per
/// store page). We write M records into the store and assert the rebuilt
/// adapter's `len()` equals M — proving no record was dropped by the batch
/// pipeline.
#[tokio::test]
async fn rebuild_from_store_batched_all_records_present() {
    let i = Interner::new();
    let store: Arc<dyn Store> = Arc::new(InMemoryStore::new());

    // M records — large enough to be meaningful, small enough to stay fast.
    let m = 150usize;
    let mut rids = Vec::with_capacity(m);
    for j in 0..m {
        let rid = RecordId::new();
        let rec = make_rec(&i, &[(j as f64) * 0.01, 1.0 - (j as f64) * 0.01, 0.5]);
        store
            .set(rid.to_bytes().into(), rec.to_bytes().unwrap())
            .await
            .unwrap();
        rids.push(rid);
    }

    // A backend backed by HnswAdapter (so `upsert_batch` overrides with a
    // single parallel_insert). The adapter is reachable via the public
    // `adapter` field.
    let backend = make_backend(&i);
    backend.rebuild(Arc::clone(&store)).await.unwrap();

    // Every record's vector must be in the graph. `adapter.len()` counts
    // live (non-tombstoned) internals — equals M when no record was
    // dropped or double-counted.
    assert_eq!(
        backend.adapter.load().adapter.len(),
        m,
        "rebuild must load all {m} records into the graph via the batch path"
    );

    // Sanity: searching near the first record finds it among the top-k.
    let result = backend
        .lookup(IndexQuery::Vector {
            vec: vec![0.0, 1.0, 0.5],
            k: 10,
            opts: crate::SearchOpts::default(),
        })
        .await
        .unwrap();
    match result {
        IndexResult::Ranked(ranked) => {
            assert!(!ranked.is_empty(), "rebuild graph must be searchable");
        }
        _ => panic!("expected Ranked"),
    }
}

// ---------------------------------------------------------------------------
// gap#1 / HIGH-6 — apply_staged_vector_deletes (the tx-delete promote at
// Phase 5d). Verifies the graph-side tombstone + the compaction double-write.
// ---------------------------------------------------------------------------

/// gap#1: `apply_staged_vector_deletes` tombstones each rid on the live
/// graph so a subsequent search does NOT return it. Mirrors the insert
/// promote's `apply_staged_vectors` for the delete side.
#[tokio::test]
async fn apply_staged_vector_deletes_tombstones_live_graph() {
    let i = Interner::new();
    let backend = make_backend(&i);

    let r1 = RecordId::new();
    let r2 = RecordId::new();
    backend
        .plan_insert(r1, &make_rec(&i, &[1.0, 0.0, 0.0]))
        .await
        .unwrap();
    backend
        .plan_insert(r2, &make_rec(&i, &[0.0, 1.0, 0.0]))
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Both vectors searchable before the delete promote.
    let pre = backend
        .lookup(IndexQuery::Vector {
            vec: vec![1.0, 0.0, 0.0],
            k: 10,
            opts: crate::SearchOpts::default(),
        })
        .await
        .unwrap();
    match pre {
        IndexResult::Ranked(ranked) => {
            assert!(
                ranked.iter().any(|(r, _)| *r == r1),
                "r1 must be searchable before the delete promote"
            );
        }
        _ => panic!("expected Ranked"),
    }

    // Promote the staged delete — tombstones r1 on the live graph.
    backend.apply_staged_vector_deletes(&[r1]).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // r1 must NOT surface; r2 survives.
    let post = backend
        .lookup(IndexQuery::Vector {
            vec: vec![1.0, 0.0, 0.0],
            k: 10,
            opts: crate::SearchOpts::default(),
        })
        .await
        .unwrap();
    match post {
        IndexResult::Ranked(ranked) => {
            assert!(
                !ranked.iter().any(|(r, _)| *r == r1),
                "gap#1: apply_staged_vector_deletes must tombstone r1 on the \
                 live graph; got {:?}",
                ranked
            );
        }
        _ => panic!("expected Ranked"),
    }
    let post2 = backend
        .lookup(IndexQuery::Vector {
            vec: vec![0.0, 1.0, 0.0],
            k: 10,
            opts: crate::SearchOpts::default(),
        })
        .await
        .unwrap();
    match post2 {
        IndexResult::Ranked(ranked) => {
            assert!(
                ranked.iter().any(|(r, _)| *r == r2),
                "r2 must survive the r1 delete promote"
            );
        }
        _ => panic!("expected Ranked"),
    }
}

/// gap#1 + #408: `apply_staged_vector_deletes` double-writes the delete to
/// the compaction target with the ghost-race guard ordering (record
/// `compaction_deleted_rids` BEFORE `target.adapter.delete`). Verifies the
/// tombstone lands on the target too.
#[tokio::test]
async fn apply_staged_vector_deletes_double_writes_to_compaction_target() {
    use crate::vector_backend::AdapterSlot;

    let i = Interner::new();
    let backend = make_backend(&i);

    let r1 = RecordId::new();
    backend
        .plan_insert(r1, &make_rec(&i, &[1.0, 0.0, 0.0]))
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Arm a compaction target (Step 2 of run_background_compaction) — an
    // armed routing slot carrying a fresh HnswAdapter target with
    // `compaction_deleted_rids` set.
    let target_adapter: Arc<dyn VectorAdapter> = Arc::new(HnswAdapter::new_compaction_target(
        3,
        VectorMetric::Cosine,
        HnswConfig::default(),
    ));
    // Backfill the live vector into the target so the delete has something
    // to tombstone (mirrors Step 4a of run_background_compaction).
    target_adapter.upsert(r1, &[1.0, 0.0, 0.0]).await.unwrap();

    let current = backend.slot_for_test();
    backend.set_slot_for_test(AdapterSlot::armed(
        Arc::clone(&current.adapter),
        crate::vector_backend::CompactionTarget::new(Arc::clone(&target_adapter)),
    ));

    // Promote the staged delete — must tombstone r1 on BOTH the live graph
    // AND the compaction target (double-write).
    backend.apply_staged_vector_deletes(&[r1]).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // The compaction target must NOT return r1 (the delete was
    // double-written, so a subsequent generation flip will not resurrect
    // the ghost).
    let target_hits = target_adapter
        .search(&[1.0, 0.0, 0.0], 10, crate::SearchOpts::default(), None)
        .await
        .unwrap();
    assert!(
        !target_hits.iter().any(|(r, _)| *r == r1),
        "gap#1+#408: apply_staged_vector_deletes must double-write the \
         tombstone to the compaction target; target returned {:?}",
        target_hits
    );
}

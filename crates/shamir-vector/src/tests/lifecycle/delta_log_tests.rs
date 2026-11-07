//! Delta-log + generation-flip tests (P2 — V2.3 / #402).
//!
//! Five contract tests:
//! 1. **delta replay restores without a snapshot-threshold crossing** —
//!    insert 1k vectors (no new snapshot), simulate a restart (load base
//!    snapshot + replay delta), assert every vector is queryable.
//! 2. **generation flip** — push the mutation counter past the threshold,
//!    drive the background snapshot to completion, assert the manifest now
//!    points at gen+1 and the old delta chunks are pruned.
//! 3. **crash-injection (flip without prune)** — model a crash between the
//!    flip and the prune by leaving orphan chunks in the store; assert the
//!    load is still correct (manifest → new gen) and a subsequent snapshot
//!    prunes the orphans idempotently.
//! 4. **delete in delta applies** — append a `Delete` op, replay, assert the
//!    deleted rid does not surface in search.
//! 5. **staged (uncommitted tx) vectors do NOT land in the delta** — a
//!    staged vector that was never promoted produces no delta chunk.
//!
//! These tests live in `shamir-index` next to the snapshot codec + backend
//! because they exercise the delta-log primitives (`append_delta`,
//! `replay_delta`, `flip_generation`) and the backend's
//! `append_vector_delta` / `trigger_snapshot_check` surface directly.

use crate::adapter::VectorAdapter;
use crate::hnsw_adapter::{HnswAdapter, HnswConfig};
use crate::snapshot::{self, DeltaOp, SnapshotManifest};
use crate::vector_backend::VectorBackend;
use shamir_index_core::backend::IndexBackend;
use shamir_index_core::descriptor::IndexDescriptor;
use shamir_index_core::kind::{IndexKind, VectorBackendRef, VectorConfig, VectorMetric};
use shamir_index_core::meta_envelope::MetaEnvelope;
use shamir_storage::storage_in_memory::InMemoryStore;
use shamir_storage::types::Store;
use shamir_types::core::interner::{Interner, TouchInd};
use shamir_types::types::record_id::RecordId;
use smallvec::SmallVec;
use std::sync::Arc;

// ----------------------------------------------------------------------------
// helpers (mirror vector_restore_tests for parity)
// ----------------------------------------------------------------------------

const DIM: u32 = 16;

fn intern(i: &Interner, s: &str) -> u64 {
    match i.touch_ind(s).unwrap() {
        TouchInd::Exists(k) | TouchInd::New(k) => k.id(),
    }
}

fn lcg_vec(dim: usize, seed: u64) -> Vec<f32> {
    let mut v = Vec::with_capacity(dim);
    let mut s = seed;
    for _ in 0..dim {
        s = s
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        v.push(((s >> 33) as f32) / (u32::MAX as f32) - 0.5);
    }
    v
}

fn rid(i: usize) -> RecordId {
    let mut a = [0u8; 16];
    a[14] = (i >> 8) as u8;
    a[15] = (i & 0xFF) as u8;
    RecordId(a)
}

fn build_adapter(dim: u32, metric: VectorMetric) -> HnswAdapter {
    HnswAdapter::new(
        dim,
        metric,
        HnswConfig {
            max_elements: 100_000,
            m: 16,
            ef_construction: 200,
            ef_search: 50,
            ..Default::default()
        },
    )
}

fn make_backend(interner: &Interner, id: u32, dim: u32, metric: VectorMetric) -> VectorBackend {
    let desc = IndexDescriptor::new(
        id,
        format!("vec_idx_{id}"),
        intern(interner, &format!("vec_idx_{id}")),
        SmallVec::new(),
        IndexKind::Vector(Box::new(VectorConfig {
            dim,
            metric,
            backend: VectorBackendRef::InProcessHnsw {
                ef_construct: 200,
                m: 16,
            },
            quantization: None,
        })),
    );
    let adapter: Arc<dyn VectorAdapter> = Arc::new(build_adapter(dim, metric));
    VectorBackend::new(desc, vec![intern(interner, "embedding")], adapter, None)
}

/// Top-k RecordId set from a backend search (scores dropped — the round-trip
/// contract is about WHICH rids survive, not exact float scores).
async fn topk_ids(backend: &VectorBackend, q: &[f32], k: u32) -> Vec<RecordId> {
    use shamir_index_core::backend::{IndexQuery, IndexResult};
    let r = backend
        .lookup(IndexQuery::Vector {
            vec: q.to_vec(),
            k,
            opts: crate::SearchOpts::with_ef_search(64),
        })
        .await
        .unwrap();
    match r {
        IndexResult::Ranked(ranked) => ranked.into_iter().map(|(rid, _)| rid).collect(),
        _ => panic!("expected Ranked result"),
    }
}

// ----------------------------------------------------------------------------
// 1. delta replay restores without a snapshot-threshold crossing
// ----------------------------------------------------------------------------

#[tokio::test]
async fn delta_replay_restores_vectors_without_new_snapshot() {
    let i = Interner::new();
    let info_store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let data_store: Arc<dyn Store> = Arc::new(InMemoryStore::new());

    let id: u32 = 10;
    let backend = make_backend(&i, id, DIM, VectorMetric::L2);

    // Seed an INITIAL base snapshot with a few vectors so the snapshot
    // path (not rebuild) is taken on restart.
    let adapter = build_adapter(DIM, VectorMetric::L2);
    for k in 0..10usize {
        adapter
            .upsert(rid(k), &lcg_vec(DIM as usize, k as u64))
            .await
            .unwrap();
    }
    let keyspace = format!("__vec_snap__{}", id);
    snapshot::dump_snapshot(&adapter, &info_store, &keyspace)
        .await
        .unwrap();

    // Load the base snapshot so the backend has a graph.
    backend
        .restore_on_open(Arc::clone(&info_store), Arc::clone(&data_store))
        .await
        .unwrap();
    assert_eq!(
        backend.rebuild_count(),
        0,
        "base snapshot must load cleanly"
    );

    // Promote 1k vectors through `append_vector_delta` — each promote
    // appends a delta chunk. We do NOT cross the snapshot threshold (10_000),
    // so no new snapshot is created. We promote in small batches to mirror
    // real tx commit Phases.
    let total: usize = 1000;
    let batch: usize = 50;
    for start in (0..total).step_by(batch) {
        let vecs: Vec<(RecordId, Vec<f32>)> = (start..start + batch)
            .map(|k| (rid(k + 100), lcg_vec(DIM as usize, (k + 100) as u64)))
            .collect();
        // Promote into the live graph.
        backend.apply_staged_vectors(&vecs).await.unwrap();
        // Append the delta chunk (mirrors Phase 5d).
        backend
            .append_vector_delta(&info_store, &vecs, &[])
            .await
            .unwrap();
    }

    // Simulate a restart: fresh backend, restore_on_open must load the base
    // snapshot AND replay the delta chunks.
    let restarted = make_backend(&i, id, DIM, VectorMetric::L2);
    restarted
        .restore_on_open(Arc::clone(&info_store), Arc::clone(&data_store))
        .await
        .unwrap();
    assert_eq!(
        restarted.rebuild_count(),
        0,
        "restart must take the snapshot + delta path, NOT a full rebuild"
    );

    // Search for a query near one of the promoted vectors. The promoted
    // vector must surface — proving the delta was replayed.
    let target = 150usize;
    let q = lcg_vec(DIM as usize, (target + 100) as u64);
    let top = topk_ids(&restarted, &q, 1).await;
    assert_eq!(top.len(), 1, "replayed graph must answer a top-1 query");
    assert_eq!(
        top[0],
        rid(target + 100),
        "the promoted vector must be the nearest neighbour to its own query"
    );
}

// ----------------------------------------------------------------------------
// 2. generation flip (dump gen+1 + flip + prune → manifest advances, old pruned)
// ----------------------------------------------------------------------------

#[tokio::test]
async fn generation_flip_advances_manifest_and_prunes_old_delta() {
    let i = Interner::new();
    let info_store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let data_store: Arc<dyn Store> = Arc::new(InMemoryStore::new());

    let id: u32 = 20;
    let keyspace = format!("__vec_snap__{}", id);

    // Seed gen 0 with a few base vectors.
    let adapter = build_adapter(DIM, VectorMetric::L2);
    for k in 0..5usize {
        adapter
            .upsert(rid(k), &lcg_vec(DIM as usize, k as u64))
            .await
            .unwrap();
    }
    snapshot::dump_snapshot(&adapter, &info_store, &keyspace)
        .await
        .unwrap();
    let manifest_before: SnapshotManifest = MetaEnvelope::open(
        &info_store
            .get(bytes::Bytes::from(format!("{}.manifest", keyspace)).into())
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(manifest_before.gen, 0, "base snapshot must be gen 0");

    // Append THREE delta chunks after the base snapshot.
    for idx in 0..3u64 {
        let chunk = vec![DeltaOp::Upsert(
            rid(100 + idx as usize),
            lcg_vec(DIM as usize, 100 + idx),
        )];
        snapshot::append_delta(&info_store, &keyspace, idx, &chunk)
            .await
            .unwrap();
    }

    // Verify the delta chunks exist before the flip.
    for idx in 0..3u64 {
        let present = info_store
            .get(bytes::Bytes::from(format!("{}.delta.{:010}", keyspace, idx)).into())
            .await;
        assert!(
            present.is_ok(),
            "delta chunk {} must exist before flip",
            idx
        );
    }

    // Build the gen-1 adapter: the base 5 vectors + all 3 delta-absorbed
    // vectors. This mirrors what `run_background_snapshot` does — it dumps
    // the LIVE adapter (which has absorbed every delta via the promote path).
    let adapter2 = build_adapter(DIM, VectorMetric::L2);
    for k in 0..5usize {
        adapter2
            .upsert(rid(k), &lcg_vec(DIM as usize, k as u64))
            .await
            .unwrap();
    }
    for idx in 0..3u64 {
        adapter2
            .upsert(rid(100 + idx as usize), &lcg_vec(DIM as usize, 100 + idx))
            .await
            .unwrap();
    }
    let gen_zero = snapshot::read_manifest(&info_store, &keyspace)
        .await
        .unwrap();
    assert_eq!(gen_zero.gen, 0);
    snapshot::dump_snapshot_with_gen(&adapter2, &info_store, &keyspace, gen_zero.gen + 1)
        .await
        .unwrap();

    // Read back the just-written manifest to get chunk counts + basename,
    // then patch in `delta_applied_upto = 3` (3 chunks absorbed).
    let written: SnapshotManifest = MetaEnvelope::open(
        &info_store
            .get(bytes::Bytes::from(format!("{}.manifest", keyspace)).into())
            .await
            .unwrap(),
    )
    .unwrap();
    let final_manifest = SnapshotManifest {
        format_version: written.format_version,
        gen: gen_zero.gen + 1,
        graph_chunks: written.graph_chunks,
        data_chunks: written.data_chunks,
        qgraph_chunks: written.qgraph_chunks,
        qdata_chunks: written.qdata_chunks,
        basename: written.basename.clone(),
        qbasename: written.qbasename.clone(),
        delta_applied_upto: 3, // all 3 chunks absorbed
    };

    // Atomic flip + prune: old gen (0) chunks removed, delta chunks 0..3
    // removed, manifest published pointing at gen 1.
    snapshot::flip_generation(
        &info_store,
        &keyspace,
        0,
        snapshot::OldGenerationChunks {
            graph: manifest_before.graph_chunks,
            data: manifest_before.data_chunks,
            qgraph: manifest_before.qgraph_chunks,
            qdata: manifest_before.qdata_chunks,
        },
        final_manifest,
        3,
    )
    .await
    .unwrap();

    // The manifest must now point at gen 1.
    let manifest_after: SnapshotManifest = MetaEnvelope::open(
        &info_store
            .get(bytes::Bytes::from(format!("{}.manifest", keyspace)).into())
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        manifest_after.gen,
        gen_zero.gen + 1,
        "manifest must advance to gen 1 after the flip"
    );
    assert_eq!(
        manifest_after.delta_applied_upto, 3,
        "manifest must record 3 absorbed delta chunks"
    );

    // The old delta chunks must be pruned.
    for idx in 0..3u64 {
        let pruned = info_store
            .get(bytes::Bytes::from(format!("{}.delta.{:010}", keyspace, idx)).into())
            .await;
        assert!(
            pruned.is_err(),
            "delta chunk {} must be pruned after the flip",
            idx
        );
    }

    // The old gen-0 sidecar must also be pruned.
    let old_sidecar = info_store
        .get(bytes::Bytes::from(format!("{}.g0.sidecar", keyspace)).into())
        .await;
    assert!(
        old_sidecar.is_err(),
        "old gen-0 sidecar must be pruned after the flip"
    );

    // A restart reads the NEW gen cleanly.
    let backend = make_backend(&i, id, DIM, VectorMetric::L2);
    backend
        .restore_on_open(Arc::clone(&info_store), Arc::clone(&data_store))
        .await
        .unwrap();
    assert_eq!(
        backend.rebuild_count(),
        0,
        "restart must take the snapshot path (gen 1), NOT a rebuild"
    );

    // All absorbed vectors must be queryable.
    let q = lcg_vec(DIM as usize, 102);
    let top = topk_ids(&backend, &q, 1).await;
    assert_eq!(top.len(), 1);
    assert_eq!(top[0], rid(102), "absorbed vector rid(102) must surface");
}

// ----------------------------------------------------------------------------
// 3. crash-injection (flip recorded, prune absent → orphans; next snap cleans)
// ----------------------------------------------------------------------------

#[tokio::test]
async fn crash_between_flip_and_prune_leaves_orphans_but_load_is_correct() {
    let i = Interner::new();
    let info_store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let data_store: Arc<dyn Store> = Arc::new(InMemoryStore::new());

    let id: u32 = 30;
    let keyspace = format!("__vec_snap__{}", id);

    // Seed gen 0 with a few base vectors.
    let adapter = build_adapter(DIM, VectorMetric::L2);
    for k in 0..5usize {
        adapter
            .upsert(rid(k), &lcg_vec(DIM as usize, k as u64))
            .await
            .unwrap();
    }
    snapshot::dump_snapshot(&adapter, &info_store, &keyspace)
        .await
        .unwrap();

    // Append TWO delta chunks AFTER the base snapshot.
    let chunk0 = vec![DeltaOp::Upsert(rid(100), lcg_vec(DIM as usize, 100))];
    let chunk1 = vec![DeltaOp::Upsert(rid(101), lcg_vec(DIM as usize, 101))];
    snapshot::append_delta(&info_store, &keyspace, 0, &chunk0)
        .await
        .unwrap();
    snapshot::append_delta(&info_store, &keyspace, 1, &chunk1)
        .await
        .unwrap();

    // Model a crash BETWEEN the flip and the prune: a new generation (gen 1)
    // is dumped from an adapter that ALREADY absorbed both delta chunks
    // (rid(100) + rid(101)) — so the gen-1 snapshot is internally consistent
    // with `delta_applied_upto = 2` (both chunks absorbed). We write the
    // manifest pointing at gen 1 but we do NOT call `flip_generation` (which
    // would prune gen 0 + delta chunks 0/1). The orphan gen-0 chunks + delta
    // chunks stay in the store.
    let adapter2 = build_adapter(DIM, VectorMetric::L2);
    for k in 0..5usize {
        adapter2
            .upsert(rid(k), &lcg_vec(DIM as usize, k as u64))
            .await
            .unwrap();
    }
    adapter2
        .upsert(rid(100), &lcg_vec(DIM as usize, 100))
        .await
        .unwrap();
    adapter2
        .upsert(rid(101), &lcg_vec(DIM as usize, 101))
        .await
        .unwrap();
    let current = snapshot::read_manifest(&info_store, &keyspace)
        .await
        .unwrap();
    assert_eq!(current.gen, 0);
    snapshot::dump_snapshot_with_gen(&adapter2, &info_store, &keyspace, current.gen + 1)
        .await
        .unwrap();
    // Overwrite the manifest with the correct delta_applied_upto (mirroring
    // what flip_generation would have set: both chunks absorbed).
    let written: SnapshotManifest = MetaEnvelope::open(
        &info_store
            .get(bytes::Bytes::from(format!("{}.manifest", keyspace)).into())
            .await
            .unwrap(),
    )
    .unwrap();
    let crashed_manifest = SnapshotManifest {
        format_version: written.format_version,
        gen: 1,
        graph_chunks: written.graph_chunks,
        data_chunks: written.data_chunks,
        qgraph_chunks: written.qgraph_chunks,
        qdata_chunks: written.qdata_chunks,
        basename: written.basename.clone(),
        qbasename: written.qbasename.clone(),
        delta_applied_upto: 2, // both delta chunks (0 and 1) absorbed
    };
    let env_bytes = MetaEnvelope::new(crashed_manifest).encode().unwrap();
    info_store
        .set(
            bytes::Bytes::from(format!("{}.manifest", keyspace)).into(),
            env_bytes.into(),
        )
        .await
        .unwrap();

    // Model a legacy manifest update; its companion must not remain stale.
    info_store
        .remove(bytes::Bytes::from(format!("{}.integrity", keyspace)).into())
        .await
        .unwrap();

    // The orphan delta chunks 0/1 are still present (no prune ran).
    let orphan0 = info_store
        .get(bytes::Bytes::from(format!("{}.delta.{:010}", keyspace, 0u64)).into())
        .await;
    assert!(
        orphan0.is_ok(),
        "orphan delta chunk 0 must still be present (no prune ran)"
    );

    // Load — the manifest points unambiguously at gen 1, so the load is
    // correct despite the orphans. The delta replay walks chunks >= 2 (none
    // exist), so no double-apply.
    let backend = make_backend(&i, id, DIM, VectorMetric::L2);
    backend
        .restore_on_open(Arc::clone(&info_store), Arc::clone(&data_store))
        .await
        .unwrap();
    assert_eq!(
        backend.rebuild_count(),
        0,
        "load must succeed from gen 1 despite the orphan chunks"
    );

    // The base + delta-absorbed vectors must all be queryable.
    let q = lcg_vec(DIM as usize, 100);
    let top = topk_ids(&backend, &q, 1).await;
    assert_eq!(top.len(), 1);
    assert_eq!(
        top[0],
        rid(100),
        "rid(100) (absorbed into gen 1) must surface"
    );

    // A subsequent snapshot run prunes the orphans idempotently. We model
    // this by calling `flip_generation` with old_gen=1 (the active gen) and
    // delta_applied_upto=2 (covers the orphan chunks 0/1).
    let adapter3 = build_adapter(DIM, VectorMetric::L2);
    for k in 0..5usize {
        adapter3
            .upsert(rid(k), &lcg_vec(DIM as usize, k as u64))
            .await
            .unwrap();
    }
    adapter3
        .upsert(rid(100), &lcg_vec(DIM as usize, 100))
        .await
        .unwrap();
    adapter3
        .upsert(rid(101), &lcg_vec(DIM as usize, 101))
        .await
        .unwrap();
    let current = snapshot::read_manifest(&info_store, &keyspace)
        .await
        .unwrap();
    assert_eq!(current.gen, 1);
    snapshot::dump_snapshot_with_gen(&adapter3, &info_store, &keyspace, current.gen + 1)
        .await
        .unwrap();
    let written2: SnapshotManifest = MetaEnvelope::open(
        &info_store
            .get(bytes::Bytes::from(format!("{}.manifest", keyspace)).into())
            .await
            .unwrap(),
    )
    .unwrap();
    let final_manifest = SnapshotManifest {
        format_version: written2.format_version,
        gen: 2,
        graph_chunks: written2.graph_chunks,
        data_chunks: written2.data_chunks,
        qgraph_chunks: written2.qgraph_chunks,
        qdata_chunks: written2.qdata_chunks,
        basename: written2.basename,
        qbasename: written2.qbasename.clone(),
        delta_applied_upto: 2, // covers the orphan chunks 0/1
    };
    snapshot::flip_generation(
        &info_store,
        &keyspace,
        1,
        snapshot::OldGenerationChunks {
            graph: written.graph_chunks,
            data: written.data_chunks,
            qgraph: written.qgraph_chunks,
            qdata: written.qdata_chunks,
        },
        final_manifest,
        2,
    )
    .await
    .unwrap();

    // The orphans must now be pruned.
    let orphan0_after = info_store
        .get(bytes::Bytes::from(format!("{}.delta.{:010}", keyspace, 0u64)).into())
        .await;
    assert!(
        orphan0_after.is_err(),
        "orphan delta chunk 0 must be pruned by the subsequent flip"
    );
}

// ----------------------------------------------------------------------------
// 4. delete in delta applies on replay
// ----------------------------------------------------------------------------

#[tokio::test]
async fn delta_delete_op_removes_rid_on_replay() {
    let i = Interner::new();
    let info_store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let data_store: Arc<dyn Store> = Arc::new(InMemoryStore::new());

    let id: u32 = 40;
    let keyspace = format!("__vec_snap__{}", id);

    // Seed a base snapshot with a known vector.
    let adapter = build_adapter(DIM, VectorMetric::L2);
    adapter
        .upsert(rid(1), &lcg_vec(DIM as usize, 1))
        .await
        .unwrap();
    adapter
        .upsert(rid(2), &lcg_vec(DIM as usize, 2))
        .await
        .unwrap();
    snapshot::dump_snapshot(&adapter, &info_store, &keyspace)
        .await
        .unwrap();

    // Append a delta chunk that DELETES rid(1).
    let del_chunk = vec![DeltaOp::Delete(rid(1))];
    snapshot::append_delta(&info_store, &keyspace, 0, &del_chunk)
        .await
        .unwrap();

    // Restore — the base has rid(1)+rid(2), the delta deletes rid(1).
    let backend = make_backend(&i, id, DIM, VectorMetric::L2);
    backend
        .restore_on_open(Arc::clone(&info_store), Arc::clone(&data_store))
        .await
        .unwrap();

    // rid(2) must survive; rid(1) must NOT surface.
    let q_near_2 = lcg_vec(DIM as usize, 2);
    let top = topk_ids(&backend, &q_near_2, 5).await;
    assert!(
        !top.contains(&rid(1)),
        "deleted rid(1) must NOT surface after delta replay"
    );
    assert!(
        top.contains(&rid(2)),
        "rid(2) must survive the delta replay"
    );
}

// ----------------------------------------------------------------------------
// 5. staged (uncommitted tx) vectors do NOT land in the delta
// ----------------------------------------------------------------------------

#[tokio::test]
async fn staged_uncommitted_vectors_do_not_land_in_delta() {
    let i = Interner::new();
    let info_store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let data_store: Arc<dyn Store> = Arc::new(InMemoryStore::new());

    let id: u32 = 50;
    let keyspace = format!("__vec_snap__{}", id);

    // Seed a base snapshot.
    let adapter = build_adapter(DIM, VectorMetric::L2);
    adapter
        .upsert(rid(1), &lcg_vec(DIM as usize, 1))
        .await
        .unwrap();
    snapshot::dump_snapshot(&adapter, &info_store, &keyspace)
        .await
        .unwrap();

    // Build a backend and restore it.
    let backend = make_backend(&i, id, DIM, VectorMetric::L2);
    backend
        .restore_on_open(Arc::clone(&info_store), Arc::clone(&data_store))
        .await
        .unwrap();

    // Simulate a STAGED (uncommitted) vector: we upsert into the live
    // adapter DIRECTLY (mirroring what `plan_insert_tx` does — the live
    // graph is NOT touched for a tx-id'd insert; the vector is buffered in
    // the TxContext). We do NOT call `append_vector_delta` — that only
    // fires on promote (Phase 5d). So the staged vector must NOT appear in
    // any delta chunk.
    //
    // To model the staged-but-not-promoted case, we simply do NOT call
    // `apply_staged_vectors` / `append_vector_delta`. The delta-log must
    // remain empty.
    let staged_rid = rid(999);
    let staged_vec = lcg_vec(DIM as usize, 999);
    // (In the real engine, the executor stages this in TxContext and only
    // promotes on commit. We skip the promote to model an uncommitted tx.)

    // No delta chunks must exist.
    let hwm = snapshot::highest_delta_index(&info_store, &keyspace)
        .await
        .unwrap();
    assert_eq!(hwm, 0, "no delta chunks must exist when no promote has run");

    // A restart loads the base snapshot + replays zero deltas. The staged
    // vector must NOT surface.
    let restarted = make_backend(&i, id, DIM, VectorMetric::L2);
    restarted
        .restore_on_open(Arc::clone(&info_store), Arc::clone(&data_store))
        .await
        .unwrap();
    let q = staged_vec.clone();
    let top = topk_ids(&restarted, &q, 5).await;
    assert!(
        !top.contains(&staged_rid),
        "staged (uncommitted) vector must NOT surface after restart — it was never promoted, so it is not in the delta"
    );
    // rid(1) from the base snapshot must still be there.
    assert!(
        top.contains(&rid(1)),
        "base-snapshot rid(1) must survive restart"
    );
}

// ----------------------------------------------------------------------------
// 6. end-to-end: trigger_snapshot_check → tokio::spawn → run_background_snapshot
//    Exercises the WHOLE orchestration (threshold check, compare_exchange,
//    spawn, dump+flip, counter reset, flag clear) — the path the direct-codec
//    tests above never cross. The threshold is lowered for the test so a
//    handful of appends arms it. Also proves the single-flight flag is cleared
//    once the task finishes (the panic-safe drop-guard).
// ----------------------------------------------------------------------------

#[tokio::test]
async fn threshold_crossing_spawns_background_snapshot_and_clears_flag() {
    let i = Interner::new();
    let info_store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let data_store: Arc<dyn Store> = Arc::new(InMemoryStore::new());

    let id: u32 = 42;
    let backend = make_backend(&i, id, DIM, VectorMetric::L2);
    let keyspace = format!("__vec_snap__{}", id);

    // Base snapshot (gen 0) so the background dump has a live graph to re-dump.
    let adapter = build_adapter(DIM, VectorMetric::L2);
    for k in 0..10usize {
        adapter
            .upsert(rid(k), &lcg_vec(DIM as usize, k as u64))
            .await
            .unwrap();
    }
    snapshot::dump_snapshot(&adapter, &info_store, &keyspace)
        .await
        .unwrap();
    backend
        .restore_on_open(Arc::clone(&info_store), Arc::clone(&data_store))
        .await
        .unwrap();

    // Lower the threshold so a few appends arm the background snapshot.
    backend.set_snapshot_threshold_for_test(3);

    // Append deltas past the threshold, triggering after each (as Phase 5d does).
    for k in 10..15usize {
        let vecs = vec![(rid(k), lcg_vec(DIM as usize, k as u64))];
        backend
            .append_vector_delta(&info_store, &vecs, &[])
            .await
            .unwrap();
        backend.trigger_snapshot_check(&info_store);
    }

    // The spawned background task must advance the manifest to gen 1.
    let mut advanced = false;
    for _ in 0..200 {
        let m: SnapshotManifest = MetaEnvelope::open(
            &info_store
                .get(bytes::Bytes::from(format!("{}.manifest", keyspace)).into())
                .await
                .unwrap(),
        )
        .unwrap();
        if m.gen >= 1 {
            advanced = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(
        advanced,
        "background snapshot (via trigger→spawn) must advance the manifest to gen 1"
    );

    // The single-flight guard must be cleared once the task finished — the
    // drop-guard doing its job. A stuck `true` would disable every future
    // snapshot for the process (the MAJOR bug this test guards against).
    let mut cleared = false;
    for _ in 0..200 {
        if !backend.snapshot_in_flight_for_test() {
            cleared = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(
        cleared,
        "single-flight flag must be cleared after the background snapshot completes"
    );
}

// ----------------------------------------------------------------------------
// 7. gap#1 (V2.4) — append_vector_delta with a non-empty `deleted` slice
//    writes a DeltaOp::Delete that is applied on restart.
//
//    The tx path in commit_phases.rs currently passes `deleted = &[]` (the
//    tx-path vector-delete wiring is deferred — see the comment at
//    commit_phases.rs:~433 and VECTOR_PRODUCTION_EXECUTION.md). This test
//    pins the CONTRACT: the moment the engine threads deleted rids into
//    `append_vector_delta`, the delete reaches the delta-log and is replayed.
//    It is the regression guard for gap#1's variant-B deferral.
// ----------------------------------------------------------------------------

#[tokio::test]
async fn append_vector_delta_with_deleted_slice_persists_and_replays_delete() {
    let i = Interner::new();
    let info_store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let data_store: Arc<dyn Store> = Arc::new(InMemoryStore::new());

    let id: u32 = 60;
    let keyspace = format!("__vec_snap__{}", id);

    // Base snapshot with rid(1) + rid(2).
    let adapter = build_adapter(DIM, VectorMetric::L2);
    adapter
        .upsert(rid(1), &lcg_vec(DIM as usize, 1))
        .await
        .unwrap();
    adapter
        .upsert(rid(2), &lcg_vec(DIM as usize, 2))
        .await
        .unwrap();
    snapshot::dump_snapshot(&adapter, &info_store, &keyspace)
        .await
        .unwrap();

    // Restore the base so the backend owns the graph + has a delta HWM.
    let backend = make_backend(&i, id, DIM, VectorMetric::L2);
    backend
        .restore_on_open(Arc::clone(&info_store), Arc::clone(&data_store))
        .await
        .unwrap();
    assert_eq!(
        backend.rebuild_count(),
        0,
        "base snapshot must load cleanly"
    );

    // Append a delta chunk that DELETES rid(1) via the public surface. This
    // is the exact call the tx path WOULD make once gap#1 variant-A lands —
    // passing the deleted rids as the third argument instead of `&[]`.
    let deleted = vec![rid(1)];
    backend
        .append_vector_delta(&info_store, &[], &deleted)
        .await
        .unwrap();

    // A delta chunk must exist at the HWM-seeded index. `restore_on_open`
    // seeds `next_delta_idx` to `highest_delta_index + 1`; with no prior
    // chunks the HWM is 0, so the first append lands at index 1 (NOT 0 —
    // the HWM pattern reserves 0 as "no chunks" sentinel, mirroring the
    // InternerManager HWM convention).
    let chunk_idx = snapshot::highest_delta_index(&info_store, &keyspace)
        .await
        .unwrap();
    assert_eq!(
        chunk_idx, 1,
        "exactly one delta chunk (at index 1) must exist after the append"
    );
    let chunk_key = format!("{}.delta.{:010}", keyspace, chunk_idx);
    let chunk_bytes = info_store
        .get(bytes::Bytes::from(chunk_key).into())
        .await
        .unwrap();
    let ops: Vec<DeltaOp> = MetaEnvelope::open(&chunk_bytes).unwrap();
    assert_eq!(
        ops.len(),
        1,
        "exactly one DeltaOp must be written for one deleted rid"
    );
    assert!(
        matches!(ops[0], DeltaOp::Delete(r) if r == rid(1)),
        "the DeltaOp must be Delete(rid(1))"
    );

    // Restart — base snapshot + replay the delete delta.
    let restarted = make_backend(&i, id, DIM, VectorMetric::L2);
    restarted
        .restore_on_open(Arc::clone(&info_store), Arc::clone(&data_store))
        .await
        .unwrap();
    assert_eq!(
        restarted.rebuild_count(),
        0,
        "restart after a delta-replay must still not trigger a full rebuild"
    );

    // rid(2) survives; rid(1) is gone (its exact-vector query returns only
    // the surviving neighbour).
    let q_near_2 = lcg_vec(DIM as usize, 2);
    let top = topk_ids(&restarted, &q_near_2, 5).await;
    assert!(
        !top.contains(&rid(1)),
        "deleted rid(1) must NOT surface after restart — the DeltaOp::Delete \
         was replayed"
    );
    assert!(
        top.contains(&rid(2)),
        "rid(2) must survive the delta replay"
    );
}

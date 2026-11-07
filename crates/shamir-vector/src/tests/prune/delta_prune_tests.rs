//! P0 delta-prune race (brief 164) regression tests.
//!
//! The snapshot prune boundary is the backend's contiguous APPLIED prefix
//! (`mark_delta_applied`), never the `next_delta_idx` allocation counter:
//! a durable delta chunk whose graph promote has not acknowledged it yet
//! must survive every snapshot flip and stay replayable on restart.
//!
//! Indexing convention: every test restores a base snapshot first, so both
//! `next_delta_idx` and the applied prefix start ABOVE the restored chunks
//! (`hwm + 1`, exactly 1 here). No test hardcodes chunk indices — each
//! captures the index returned by `append_vector_delta` and the prefix
//! captured at setup, and asserts relative to them.

use crate::adapter::VectorAdapter;
use crate::hnsw_adapter::{HnswAdapter, HnswConfig};
use crate::snapshot::{self, SnapshotManifest};
use crate::vector_backend::{run_background_compaction, VectorBackend};
use shamir_index_core::backend::{IndexBackend, IndexQuery, IndexResult};
use shamir_index_core::descriptor::IndexDescriptor;
use shamir_index_core::kind::{IndexKind, VectorBackendRef, VectorConfig, VectorMetric};
use shamir_storage::storage_in_memory::InMemoryStore;
use shamir_storage::types::Store;
use shamir_types::core::interner::{Interner, TouchInd};
use shamir_types::types::record_id::RecordId;
use smallvec::SmallVec;
use std::sync::Arc;

pub(crate) const DIM: u32 = 16;

fn intern(i: &Interner, s: &str) -> u64 {
    match i.touch_ind(s).unwrap() {
        TouchInd::Exists(k) | TouchInd::New(k) => k.id(),
    }
}

pub(crate) fn lcg_vec(dim: usize, seed: u64) -> Vec<f32> {
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

pub(crate) fn rid(i: usize) -> RecordId {
    let mut a = [0u8; 16];
    a[14] = (i >> 8) as u8;
    a[15] = (i & 0xFF) as u8;
    RecordId(a)
}

pub(crate) fn make_backend(interner: &Interner, id: u32) -> VectorBackend {
    let desc = IndexDescriptor::new(
        id,
        format!("vec_idx_{id}"),
        intern(interner, &format!("vec_idx_{id}")),
        SmallVec::new(),
        IndexKind::Vector(Box::new(VectorConfig {
            dim: DIM,
            metric: VectorMetric::L2,
            backend: VectorBackendRef::InProcessHnsw {
                ef_construct: 200,
                m: 16,
            },
            quantization: None,
        })),
    );
    let adapter: Arc<dyn VectorAdapter> = Arc::new(HnswAdapter::new(
        DIM,
        VectorMetric::L2,
        HnswConfig {
            max_elements: 100_000,
            m: 16,
            ef_construction: 200,
            ef_search: 50,
            ..Default::default()
        },
    ));
    VectorBackend::new(desc, vec![intern(interner, "embedding")], adapter, None)
}

/// Seed a gen-0 base snapshot with 10 base vectors and open `backend` from
/// it (the snapshot path, not the rebuild fallback).
pub(crate) async fn seed_base_and_open(
    backend: &VectorBackend,
    info_store: &Arc<dyn Store>,
    id: u32,
) {
    let data_store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let adapter = backend.adapter_arc();
    let hnsw = adapter.as_hnsw_adapter().unwrap();
    for k in 0..10usize {
        adapter
            .upsert(rid(k), &lcg_vec(DIM as usize, k as u64))
            .await
            .unwrap();
    }
    let keyspace = format!("__vec_snap__{}", id);
    snapshot::dump_snapshot(hnsw, info_store, &keyspace)
        .await
        .unwrap();
    backend
        .restore_on_open(Arc::clone(info_store), data_store)
        .await
        .unwrap();
    assert_eq!(
        backend.rebuild_count(),
        0,
        "base snapshot must load cleanly"
    );
}

pub(crate) async fn top1(backend: &VectorBackend, seed: u64) -> RecordId {
    let r = backend
        .lookup(IndexQuery::Vector {
            vec: lcg_vec(DIM as usize, seed),
            k: 1,
            opts: crate::SearchOpts::with_ef_search(64),
        })
        .await
        .unwrap();
    match r {
        IndexResult::Ranked(ranked) => ranked[0].0,
        _ => panic!("expected Ranked result"),
    }
}

async fn manifest(keyspace: &str, info_store: &Arc<dyn Store>) -> SnapshotManifest {
    snapshot::read_manifest(info_store, keyspace).await.unwrap()
}

async fn chunk_present(keyspace: &str, idx: u64, info_store: &Arc<dyn Store>) -> bool {
    let key = bytes::Bytes::from(format!("{keyspace}.delta.{idx:010}")).into();
    info_store.get(key).await.is_ok()
}

/// Open a fresh backend over the SAME store, simulating a restart.
async fn restart(interner: &Interner, id: u32, info_store: &Arc<dyn Store>) -> VectorBackend {
    let restarted = make_backend(interner, id);
    let data_store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    restarted
        .restore_on_open(Arc::clone(info_store), data_store)
        .await
        .unwrap();
    assert_eq!(restarted.rebuild_count(), 0);
    restarted
}

/// (a)+(d), brief-164 main scenario: append a chunk WITHOUT the graph apply,
/// snapshot at the append->apply seam, assert the chunk survives pruning,
/// then apply + acknowledge and reload — the vector must be present.
#[tokio::test]
async fn rv_index_l3_delta_prune_unacked_chunk_survives_snapshot_and_replays() {
    let i = Interner::new();
    let info_store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let id = 30;
    let keyspace = format!("__vec_snap__{}", id);
    let backend = make_backend(&i, id);
    seed_base_and_open(&backend, &info_store, id).await;
    let start = backend.applied_prefix();

    // Append the durable delta chunk WITHOUT the graph apply and WITHOUT the
    // acknowledgement (mirrors the pre-publish delta half).
    let x = (rid(100), lcg_vec(DIM as usize, 100));
    let idx = backend
        .append_vector_delta(&info_store, std::slice::from_ref(&x), &[])
        .await
        .unwrap()
        .expect("a non-empty promote must yield a chunk index");
    assert_eq!(backend.applied_prefix(), start);

    // Snapshot at the seam: the unacked chunk must NOT be absorbed/pruned.
    backend.snapshot_now(&info_store).await.unwrap();
    let m = manifest(&keyspace, &info_store).await;
    assert_eq!(
        m.delta_applied_upto, start,
        "unacked chunk must not be absorbed by the manifest boundary"
    );
    assert!(
        chunk_present(&keyspace, idx, &info_store).await,
        "unacked chunk must survive the flip"
    );

    // A failed promotion keeps the chunk replayable: restart BEFORE any
    // apply/ack recovers the vector from the surviving delta chunk.
    let restarted = restart(&i, id, &info_store).await;
    assert_eq!(top1(&restarted, 100).await, rid(100));

    // Now the real promote: graph apply, then acknowledgement; the prefix
    // advances across exactly the contiguous run.
    backend.apply_staged_vectors(&[x]).await.unwrap();
    backend.mark_delta_applied(idx);
    assert_eq!(backend.applied_prefix(), idx + 1);

    // Reload from snapshot + surviving deltas: the vector is present.
    let reloaded = restart(&i, id, &info_store).await;
    assert_eq!(top1(&reloaded, 100).await, rid(100));
}

/// (b): out-of-order acknowledgement (k+1 acked, k pending) keeps chunk k
/// unpruned; the boundary advances only across the contiguous prefix.
#[tokio::test]
async fn rv_index_l3_delta_prune_out_of_order_ack_keeps_pending_chunk() {
    let i = Interner::new();
    let info_store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let id = 31;
    let keyspace = format!("__vec_snap__{}", id);
    let backend = make_backend(&i, id);
    seed_base_and_open(&backend, &info_store, id).await;
    let start = backend.applied_prefix();

    let a = (rid(100), lcg_vec(DIM as usize, 100));
    let b = (rid(101), lcg_vec(DIM as usize, 101));
    let idx_a = backend
        .append_vector_delta(&info_store, std::slice::from_ref(&a), &[])
        .await
        .unwrap()
        .unwrap();
    let idx_b = backend
        .append_vector_delta(&info_store, std::slice::from_ref(&b), &[])
        .await
        .unwrap()
        .unwrap();
    assert_eq!(idx_b, idx_a + 1, "chunk indices are contiguous");

    // Acknowledge ONLY the LATER chunk — its vector was applied first.
    backend
        .apply_staged_vectors(std::slice::from_ref(&b))
        .await
        .unwrap();
    backend.mark_delta_applied(idx_b);
    assert_eq!(
        backend.applied_prefix(),
        start,
        "a pending earlier chunk must block the boundary at its index"
    );

    // Snapshot with chunk idx_a pending: nothing may be pruned past it —
    // BOTH chunks survive (the boundary stays at the restored start).
    backend.snapshot_now(&info_store).await.unwrap();
    let m = manifest(&keyspace, &info_store).await;
    assert_eq!(
        m.delta_applied_upto, start,
        "pending chunk idx_a blocks the boundary"
    );
    assert!(chunk_present(&keyspace, idx_a, &info_store).await);
    assert!(chunk_present(&keyspace, idx_b, &info_store).await);

    // Acknowledging the earlier chunk lets the prefix jump across BOTH.
    backend
        .apply_staged_vectors(std::slice::from_ref(&a))
        .await
        .unwrap();
    backend.mark_delta_applied(idx_a);
    assert_eq!(backend.applied_prefix(), idx_b + 1);

    backend.snapshot_now(&info_store).await.unwrap();
    let m = manifest(&keyspace, &info_store).await;
    assert_eq!(m.delta_applied_upto, idx_b + 1);
    assert!(!chunk_present(&keyspace, idx_a, &info_store).await);
    assert!(!chunk_present(&keyspace, idx_b, &info_store).await);

    // Both vectors live in the snapshot graph now — both MUST be found
    // after a restart (they were applied + acknowledged before pruning).
    let restarted = restart(&i, id, &info_store).await;
    assert_eq!(top1(&restarted, 100).await, rid(100));
    assert_eq!(top1(&restarted, 101).await, rid(101));
}

/// (c): snapshot at the seam first, then apply + acknowledge — the next
/// snapshot absorbs and prunes the chunk, and a restart still finds the
/// vector (from the snapshot graph, not the delta).
#[tokio::test]
async fn rv_index_l3_delta_prune_acked_chunk_absorbed_and_pruned() {
    let i = Interner::new();
    let info_store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let id = 32;
    let keyspace = format!("__vec_snap__{}", id);
    let backend = make_backend(&i, id);
    seed_base_and_open(&backend, &info_store, id).await;
    let start = backend.applied_prefix();

    let x = (rid(100), lcg_vec(DIM as usize, 100));
    let idx = backend
        .append_vector_delta(&info_store, std::slice::from_ref(&x), &[])
        .await
        .unwrap()
        .unwrap();

    // First snapshot BEFORE the promote: the chunk survives.
    backend.snapshot_now(&info_store).await.unwrap();
    assert_eq!(
        manifest(&keyspace, &info_store).await.delta_applied_upto,
        start
    );
    assert!(chunk_present(&keyspace, idx, &info_store).await);

    // Full promote: graph apply, then acknowledgement.
    backend.apply_staged_vectors(&[x]).await.unwrap();
    backend.mark_delta_applied(idx);
    assert_eq!(backend.applied_prefix(), idx + 1);

    // Second snapshot: the acked chunk is absorbed and pruned.
    backend.snapshot_now(&info_store).await.unwrap();
    let m = manifest(&keyspace, &info_store).await;
    assert_eq!(m.delta_applied_upto, idx + 1, "acked chunk is absorbed");
    assert!(
        !chunk_present(&keyspace, idx, &info_store).await,
        "acked chunk is pruned after the flip"
    );

    let reloaded = restart(&i, id, &info_store).await;
    assert_eq!(top1(&reloaded, 100).await, rid(100));
}

/// (e): the post-compaction snapshot uses the SAME applied-prefix boundary.
#[tokio::test]
async fn rv_index_l3_delta_prune_post_compaction_snapshot_respects_boundary() {
    let i = Interner::new();
    let info_store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let id = 33;
    let keyspace = format!("__vec_snap__{}", id);
    let backend = make_backend(&i, id);
    seed_base_and_open(&backend, &info_store, id).await;
    let start = backend.applied_prefix();

    let x = (rid(100), lcg_vec(DIM as usize, 100));
    let idx = backend
        .append_vector_delta(&info_store, &[x], &[])
        .await
        .unwrap()
        .unwrap();

    // Drive the real compaction task body inline; its step-7 snapshot runs
    // on a spawned task gated by `snapshot_in_flight`.
    let (delta_count, applied_prefix, snapshot_in_flight, fail_next_backfill) =
        backend.compaction_task_inputs_for_test();
    run_background_compaction(
        &backend.adapter,
        &backend.published,
        &info_store,
        &keyspace,
        &delta_count,
        &applied_prefix,
        &snapshot_in_flight,
        &fail_next_backfill,
        None,
    )
    .await
    .unwrap();
    while backend.snapshot_in_flight_for_test() {
        tokio::task::yield_now().await;
    }

    let m = manifest(&keyspace, &info_store).await;
    assert_eq!(
        m.delta_applied_upto, start,
        "post-compaction snapshot must not absorb the unacked chunk"
    );
    assert!(
        chunk_present(&keyspace, idx, &info_store).await,
        "post-compaction snapshot must not prune the unacked chunk"
    );

    let restarted = restart(&i, id, &info_store).await;
    assert_eq!(top1(&restarted, 100).await, rid(100));
}

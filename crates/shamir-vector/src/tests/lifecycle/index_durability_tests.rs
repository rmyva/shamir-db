//! P0 stage A5 — vector index durability regression tests.
//!
//! One test per review card:
//!
//! * `rv_index_4651aa00_*` (RV-INDEX-4651AA00) — a delta-replay failure on
//!   restart must FAIL the open; the old code warned and continued, letting
//!   the next generation flip permanently prune the unreplayed chunks.
//! * `rv_index_2f3cbca3_*` (RV-INDEX-2F3CBCA3) — the post-restart HWM scan
//!   must propagate errors (never default to 0) and fold in the replay
//!   watermark; the old code seeded `next_delta_idx = 1` on scan failure,
//!   letting the next `append_delta` overwrite surviving unabsorbed chunks.

use crate::adapter::{SearchOpts, VectorAdapter};
use crate::hnsw_adapter::{HnswAdapter, HnswConfig};
use crate::snapshot::{self, DeltaOp, SnapshotManifest};
use crate::vector_backend::VectorBackend;
use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use shamir_index_core::backend::{IndexBackend, IndexQuery, IndexResult};
use shamir_index_core::descriptor::IndexDescriptor;
use shamir_index_core::kind::{IndexKind, VectorBackendRef, VectorConfig, VectorMetric};
use shamir_storage::error::{DbError, DbResult};
use shamir_storage::storage_in_memory::InMemoryStore;
use shamir_storage::types::{RecordKey, Store};
use shamir_types::core::interner::{Interner, TouchInd};
use shamir_types::types::record_id::RecordId;
use smallvec::SmallVec;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

type TestStream = Pin<Box<dyn Stream<Item = Result<Vec<(RecordKey, Bytes)>, DbError>> + Send>>;

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

fn make_backend(interner: &Interner, id: u32) -> VectorBackend {
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

/// `Store` wrapper that makes `scan_prefix_stream` fail deterministically on
/// the Nth call (1-indexed; `None` = fail every call). Everything else
/// passes through. Deterministic fault injection — no sleeps, no timing.
struct ScanFailStore {
    inner: Arc<dyn Store>,
    /// `Some(n)` → fail only the nth scan; `None` → fail every scan.
    fail_on_nth: Option<u64>,
    scan_calls: AtomicU64,
    /// `Some(n)` → fail only the nth `get` (1-indexed); `None` → never.
    fail_get_on_nth: Option<u64>,
    get_calls: AtomicU64,
}

impl ScanFailStore {
    fn new(inner: Arc<dyn Store>, fail_on_nth: Option<u64>) -> Self {
        Self {
            inner,
            fail_on_nth,
            scan_calls: AtomicU64::new(0),
            fail_get_on_nth: None,
            get_calls: AtomicU64::new(0),
        }
    }

    fn failing_get(mut self, nth: u64) -> Self {
        self.fail_get_on_nth = Some(nth);
        self
    }
}

#[async_trait]
impl Store for ScanFailStore {
    async fn insert(&self, value: Bytes) -> DbResult<RecordKey> {
        self.inner.insert(value).await
    }
    async fn set(&self, key: RecordKey, value: Bytes) -> DbResult<bool> {
        self.inner.set(key, value).await
    }
    async fn get(&self, key: RecordKey) -> DbResult<Bytes> {
        let n = self.get_calls.fetch_add(1, Ordering::SeqCst) + 1;
        if self.fail_get_on_nth == Some(n) {
            return Err(DbError::Storage("injected get failure".to_string()));
        }
        self.inner.get(key).await
    }
    async fn remove(&self, key: RecordKey) -> DbResult<bool> {
        self.inner.remove(key).await
    }
    fn iter_stream(&self, batch_size: usize) -> TestStream {
        self.inner.iter_stream(batch_size)
    }
    fn scan_prefix_stream(&self, prefix: Bytes, batch_size: usize) -> TestStream {
        let n = self.scan_calls.fetch_add(1, Ordering::SeqCst) + 1;
        let fail = match self.fail_on_nth {
            Some(target) => n == target,
            None => true,
        };
        if fail {
            return Box::pin(futures::stream::once(async {
                Err(DbError::Storage("injected scan failure".to_string()))
            }));
        }
        self.inner.scan_prefix_stream(prefix, batch_size)
    }
    async fn set_many(&self, items: Vec<(RecordKey, Bytes)>) -> DbResult<Vec<bool>> {
        self.inner.set_many(items).await
    }
    async fn remove_many(&self, keys: Vec<RecordKey>) -> DbResult<Vec<bool>> {
        self.inner.remove_many(keys).await
    }
}

/// Build a base snapshot (gen 0, `delta_applied_upto == 0`) plus two delta
/// chunks in `info_store`, so the restart takes the snapshot + replay path.
async fn seed_snapshot_and_deltas(info_store: &Arc<dyn Store>, id: u32) -> String {
    let adapter = HnswAdapter::new(
        DIM,
        VectorMetric::L2,
        HnswConfig {
            max_elements: 100_000,
            m: 16,
            ef_construction: 200,
            ef_search: 50,
            ..Default::default()
        },
    );
    for k in 0..10usize {
        adapter
            .upsert(rid(k), &lcg_vec(DIM as usize, k as u64))
            .await
            .unwrap();
    }
    let keyspace = format!("__vec_snap__{id}");
    snapshot::dump_snapshot(&adapter, info_store, &keyspace)
        .await
        .unwrap();
    // Two unabsorbed delta chunks past the snapshot's watermark.
    snapshot::append_delta(
        info_store,
        &keyspace,
        0,
        &[DeltaOp::Upsert(rid(100), lcg_vec(DIM as usize, 100))],
    )
    .await
    .unwrap();
    snapshot::append_delta(
        info_store,
        &keyspace,
        1,
        &[DeltaOp::Upsert(rid(101), lcg_vec(DIM as usize, 101))],
    )
    .await
    .unwrap();
    keyspace
}

/// RV-INDEX-4651AA00: a delta-replay failure on restart must fail the open
/// (fail closed). The old code logged a warning and returned `Ok(())`, and
/// the next background snapshot flipped the generation and pruned the
/// absorbed delta chunks — permanently baking in the omitted mutations.
#[tokio::test]
async fn rv_index_4651aa00_delta_replay_failure_fails_restart_closed() {
    let i = Interner::new();
    let info_store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let data_store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    seed_snapshot_and_deltas(&info_store, 21).await;

    // Scan call 1 = replay_delta fails → the replay error must propagate.
    // (Call 2 = highest_delta_index passes — the HWM fix is covered by the
    // 2f3cbca3 scan-failure test below.)
    //
    // Revert reasoning: with ONLY the replay propagation (backend.rs `?` on
    // `replay_delta`) reverted to warn-and-continue, restore returns Ok (the
    // HWM scan passes) → this test goes red. With the fix, the replay error
    // propagates → Err. Failing only scan call 1 isolates the replay fix
    // from the HWM fix.
    let wrapped: Arc<dyn Store> = Arc::new(ScanFailStore::new(Arc::clone(&info_store), Some(1)));
    let restarted = make_backend(&i, 21);
    let res = restarted.restore_on_open(wrapped, data_store).await;
    assert!(
        res.is_err(),
        "restart must FAIL CLOSED when delta replay errors — \
         old code warned and returned Ok, baking in the omitted mutations"
    );
}

/// RV-INDEX-4651AA00 / RV-INDEX-2F3CBCA3: a failure of ONLY the first
/// (up-front) manifest read must not let restore publish the snapshot that
/// `load_snapshot` then reads successfully — that graph would be published
/// without its absorption boundary (replay skipped, allocator seeded from
/// surviving chunks alone).
///
/// Revert reasoning: with ONLY the `(None, Some(_))` refusal reverted to the
/// old "no manifest → nothing to replay" behaviour, restore returns Ok.
#[tokio::test]
async fn rv_index_4651aa00_first_manifest_read_failure_refuses_publication() {
    let i = Interner::new();
    let info_store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let data_store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    seed_snapshot_and_deltas(&info_store, 25).await;

    // `get` call 1 = the up-front read_manifest; later gets (load_snapshot's
    // own manifest read and chunk reads) succeed.
    let wrapped: Arc<dyn Store> =
        Arc::new(ScanFailStore::new(Arc::clone(&info_store), Some(u64::MAX)).failing_get(1));
    let restarted = make_backend(&i, 25);
    let res = restarted.restore_on_open(wrapped, data_store).await;
    assert!(
        res.is_err(),
        "restore must refuse to publish a snapshot whose first manifest read failed"
    );
}

/// RV-INDEX-2F3CBCA3: the post-restart HWM seed must propagate scan errors
/// and fold in the replay watermark. The old code defaulted a failed
/// `highest_delta_index` scan to 0, seeding `next_delta_idx = 1` — below the
/// persisted absorption boundary — so the next `append_delta` overwrote
/// surviving unabsorbed delta chunks (prune/reopen/append/reopen data loss).
#[tokio::test]
async fn rv_index_2f3cbca3_hwm_seed_scan_failure_propagates_not_zero() {
    let i = Interner::new();
    let info_store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let data_store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    seed_snapshot_and_deltas(&info_store, 22).await;

    // Scan call 1 = replay_delta (succeeds), scan call 2 =
    // highest_delta_index (fails). Only the HWM scan is faulty.
    let wrapped: Arc<dyn Store> = Arc::new(ScanFailStore::new(Arc::clone(&info_store), Some(2)));
    let restarted = make_backend(&i, 22);
    let res = restarted.restore_on_open(wrapped, data_store).await;
    assert!(
        res.is_err(),
        "restart must FAIL when the HWM seed scan errors — \
         old code defaulted the watermark to 0, letting append_delta \
         overwrite surviving delta chunks below the absorption boundary"
    );
}

/// RV-INDEX-2F3CBCA3 (oracle variant): prune → reopen → append → reopen must
/// respect the absorption watermark. After a two-generation flip that prunes
/// the gen-0 base chunks (superseded) and delta chunks 0..3 (boundary = 3, no
/// delta chunks survive) while keeping the gen-1 chunks the manifest still
/// references, the reopened backend must seed `next_delta_idx` ABOVE the
/// persisted boundary (max(replayed boundary, stored HWM)), not from the
/// surviving-chunk HWM alone.
///
/// Revert reasoning: the fixed code seeds `next_delta_idx =
/// max(replayed boundary 3, stored 0) + 1 = 4` → the append lands in chunk 4
/// (≥ 3) and is replayed on the second reopen. With ONLY the `max` fold
/// removed (next_delta_idx seeded from the stored HWM alone — 0 surviving
/// chunks → next_delta_idx = 1), the append lands in chunk 1, which is BELOW
/// the boundary 3; the second reopen replays only chunks >= 3 (the boundary
/// is EXCLUSIVE: `delta_applied_upto` counts absorbed chunks 0..3), so
/// rid(200) is
/// never replayed → the membership assert fails (test red).
#[tokio::test]
async fn rv_index_2f3cbca3_reopen_reseed_respects_absorption_watermark() {
    let i = Interner::new();
    let info_store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let data_store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let keyspace = "__vec_snap__24".to_string();

    // 1. Base snapshot (gen 0) from a plain adapter.
    let adapter = HnswAdapter::new(
        DIM,
        VectorMetric::L2,
        HnswConfig {
            max_elements: 100_000,
            m: 16,
            ef_construction: 200,
            ef_search: 50,
            ..Default::default()
        },
    );
    for k in 0..10usize {
        adapter
            .upsert(rid(k), &lcg_vec(DIM as usize, k as u64))
            .await
            .unwrap();
    }
    snapshot::dump_snapshot(&adapter, &info_store, &keyspace)
        .await
        .unwrap();
    let m0 = snapshot::read_manifest(&info_store, &keyspace)
        .await
        .unwrap();

    // 2. Delta chunks 0,1,2 — each chunk's upsert is ALSO applied to the
    //    adapter first, so the gen-1 re-dump genuinely absorbs those exact
    //    mutations (writes gen-1 chunks + a gen-1 manifest).
    for c in 0..3u64 {
        adapter
            .upsert(rid(100 + c as usize), &lcg_vec(DIM as usize, 100 + c))
            .await
            .unwrap();
        snapshot::append_delta(
            &info_store,
            &keyspace,
            c,
            &[DeltaOp::Upsert(
                rid(100 + c as usize),
                lcg_vec(DIM as usize, 100 + c),
            )],
        )
        .await
        .unwrap();
    }
    assert_eq!(m0.gen, 0);
    snapshot::dump_snapshot_with_gen(&adapter, &info_store, &keyspace, m0.gen + 1)
        .await
        .unwrap();
    let m1 = snapshot::read_manifest(&info_store, &keyspace)
        .await
        .unwrap();

    // 3. Flip: prune the SUPERSEDED gen-0 chunks (safe — the manifest now
    //    points at gen 1) and delta chunks 0..3, publish the gen-1 manifest
    //    with absorption boundary 3. The gen-1 chunks survive (they are what
    //    the published manifest references).
    let final_manifest = SnapshotManifest {
        format_version: m1.format_version,
        gen: m1.gen,
        graph_chunks: m1.graph_chunks,
        data_chunks: m1.data_chunks,
        qgraph_chunks: m1.qgraph_chunks,
        qdata_chunks: m1.qdata_chunks,
        basename: m1.basename.clone(),
        qbasename: m1.qbasename.clone(),
        delta_applied_upto: 3,
    };
    snapshot::flip_generation(
        &info_store,
        &keyspace,
        m0.gen,
        snapshot::OldGenerationChunks {
            graph: m0.graph_chunks,
            data: m0.data_chunks,
            qgraph: m0.qgraph_chunks,
            qdata: m0.qdata_chunks,
        },
        final_manifest,
        3,
    )
    .await
    .unwrap();
    assert_eq!(
        snapshot::highest_delta_index(&info_store, &keyspace)
            .await
            .unwrap(),
        0,
        "flip must prune delta chunks 0..3"
    );

    // 4. First reopen: must seed next_delta_idx above the boundary, via the
    //    SNAPSHOT path (rebuild_count()==0 proves no full-scan fallback hid
    //    a fixture failure).
    let backend = make_backend(&i, 24);
    backend
        .restore_on_open(Arc::clone(&info_store), Arc::clone(&data_store))
        .await
        .expect("reopen after boundary flip must succeed");
    assert_eq!(
        backend.rebuild_count(),
        0,
        "the snapshot load path must be taken — a rebuild fallback would make \
         the fixture oracle vacuous"
    );
    // The absorbed gen-1 mutations (boundary chunks 0..3) must be in the
    // graph via the snapshot itself — no surviving chunk carries them.
    let res = backend
        .lookup(IndexQuery::Vector {
            vec: lcg_vec(DIM as usize, 100),
            k: 100,
            opts: SearchOpts::with_ef_search(64),
        })
        .await
        .unwrap();
    match res {
        IndexResult::Ranked(ranked) => assert!(
            ranked.iter().any(|(r, _)| *r == rid(100)),
            "gen-1 snapshot must have absorbed the boundary chunks' mutations"
        ),
        _ => panic!("expected Ranked result"),
    }

    // 5. Append rid(200) — must land in a chunk ABOVE the boundary.
    backend
        .append_vector_delta(&info_store, &[(rid(200), lcg_vec(DIM as usize, 200))], &[])
        .await
        .unwrap();

    // 6. Second reopen: rid(200) must be replayed from the surviving chunk.
    let backend2 = make_backend(&i, 24);
    backend2
        .restore_on_open(Arc::clone(&info_store), Arc::clone(&data_store))
        .await
        .expect("second reopen must succeed");
    assert_eq!(backend2.rebuild_count(), 0);
    let res = backend2
        .lookup(IndexQuery::Vector {
            vec: lcg_vec(DIM as usize, 200),
            k: 100,
            opts: SearchOpts::with_ef_search(64),
        })
        .await
        .unwrap();
    match res {
        IndexResult::Ranked(ranked) => assert!(
            ranked.iter().any(|(r, _)| *r == rid(200)),
            "rid(200) must survive the prune → reopen → append → reopen cycle — \
             a next_delta_idx seeded below the absorption boundary let the \
             append overwrite a pruned chunk index and the mutation was lost"
        ),
        _ => panic!("expected Ranked result"),
    }
}

/// Gen-1 snapshot with absorption boundary 3, deltas 0..3 pruned (the same
/// fixture as the watermark test). Returns the published gen-1 manifest.
async fn seed_pruned_boundary_3(info_store: &Arc<dyn Store>, keyspace: &str) -> SnapshotManifest {
    let adapter = HnswAdapter::new(
        DIM,
        VectorMetric::L2,
        HnswConfig {
            max_elements: 100_000,
            m: 16,
            ef_construction: 200,
            ef_search: 50,
            ..Default::default()
        },
    );
    for k in 0..10usize {
        adapter
            .upsert(rid(k), &lcg_vec(DIM as usize, k as u64))
            .await
            .unwrap();
    }
    snapshot::dump_snapshot(&adapter, info_store, keyspace)
        .await
        .unwrap();
    let m0 = snapshot::read_manifest(info_store, keyspace).await.unwrap();
    for c in 0..3u64 {
        adapter
            .upsert(rid(100 + c as usize), &lcg_vec(DIM as usize, 100 + c))
            .await
            .unwrap();
        snapshot::append_delta(
            info_store,
            keyspace,
            c,
            &[DeltaOp::Upsert(
                rid(100 + c as usize),
                lcg_vec(DIM as usize, 100 + c),
            )],
        )
        .await
        .unwrap();
    }
    snapshot::dump_snapshot_with_gen(&adapter, info_store, keyspace, m0.gen + 1)
        .await
        .unwrap();
    let m1 = snapshot::read_manifest(info_store, keyspace).await.unwrap();
    let published = SnapshotManifest {
        format_version: m1.format_version,
        gen: m1.gen,
        graph_chunks: m1.graph_chunks,
        data_chunks: m1.data_chunks,
        qgraph_chunks: m1.qgraph_chunks,
        qdata_chunks: m1.qdata_chunks,
        basename: m1.basename.clone(),
        qbasename: m1.qbasename.clone(),
        delta_applied_upto: 3,
    };
    snapshot::flip_generation(
        info_store,
        keyspace,
        m0.gen,
        snapshot::OldGenerationChunks {
            graph: m0.graph_chunks,
            data: m0.data_chunks,
            qgraph: m0.qgraph_chunks,
            qdata: m0.qdata_chunks,
        },
        published.clone(),
        3,
    )
    .await
    .unwrap();
    published
}

/// RV-INDEX-2F3CBCA3 on the rebuild fallback: a valid manifest (boundary 3,
/// deltas pruned) whose snapshot chunk is gone takes the full-rebuild path;
/// the allocator must STILL be seeded above the boundary, or the next append
/// lands below it and the second reopen skips it.
///
/// Revert reasoning: with ONLY the `seed_delta_idx_for_fallback` calls
/// removed, `next_delta_idx` stays 0 → the append lands in chunk 1 (< 3),
/// replay skips it on the second reopen and the final membership assert
/// fails.
#[tokio::test]
async fn rv_index_2f3cbca3_rebuild_fallback_reseeds_above_boundary() {
    let i = Interner::new();
    let info_store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let data_store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let keyspace = "__vec_snap__26".to_string();
    let m = seed_pruned_boundary_3(&info_store, &keyspace).await;

    // Make the snapshot unloadable while the manifest stays readable.
    info_store
        .remove(snapshot::chunk_key_for_test(&keyspace, m.gen, "graph", 0))
        .await
        .unwrap();

    let backend = make_backend(&i, 26);
    backend
        .restore_on_open(Arc::clone(&info_store), Arc::clone(&data_store))
        .await
        .expect("rebuild fallback must succeed");
    assert_eq!(
        backend.rebuild_count(),
        1,
        "the fixture must take the rebuild fallback — otherwise the oracle is vacuous"
    );

    backend
        .append_vector_delta(&info_store, &[(rid(200), lcg_vec(DIM as usize, 200))], &[])
        .await
        .unwrap();

    // Delta chunks below the boundary are never replayed: the new chunk must
    // sit at or above index 3.
    assert!(
        snapshot::highest_delta_index(&info_store, &keyspace)
            .await
            .unwrap()
            >= 3,
        "append after the fallback rebuild must land at or above the absorption boundary"
    );
}

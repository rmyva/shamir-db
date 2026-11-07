//! V5.3 (#412) — snapshot v2 quantization tests.
//!
//! De-risk spike + round-trip + migration + crash/cold-start + compaction +
//! back-compat coverage for the quantized snapshot format (v2).
//!
//! ## Spike (file_dump<u8>)
//!
//! The very first test, [`spike_file_dump_u8_works`], proves that
//! `hnsw_rs::file_dump` + `HnswIo::load_hnsw_with_dist` work for
//! `Hnsw<'static, u8, ShamirDistU8>` — the precondition for dumping the u8
//! graph verbatim into the snapshot. If that path works (and the spike
//! proves it does), the snapshot stores the u8 graph directly and load
//! restores it without a rebuild. The alternative ("plan B" in the brief)
//! would serialise only the u8 codes + quantizer and rebuild the graph on
//! load; the spike makes plan B unnecessary.

use crate::adapter::{SearchOpts, VectorAdapter};
use crate::hnsw_adapter::{HnswAdapter, HnswConfig};
use crate::quantized_dist::ShamirDistU8;
use crate::snapshot::{
    self, dump_snapshot, load_snapshot, QuantMeta, SnapshotError, SnapshotSidecar, HNSW_RS_VERSION,
    SNAPSHOT_FORMAT_VERSION,
};
use hnsw_rs::api::AnnT;
use hnsw_rs::hnsw::Hnsw;
use hnsw_rs::hnswio::HnswIo;
use shamir_index_core::kind::{VectorMetric, VectorQuantization};
use shamir_index_core::meta_envelope::MetaEnvelope;
use shamir_storage::storage_in_memory::InMemoryStore;
use shamir_storage::types::Store;
use shamir_types::types::record_id::RecordId;
use std::sync::Arc;

/// Keyspace tag used by every test.
const KEYSPACE: &str = "vsnapq.test";

/// Build an in-memory `Store` arc.
fn mem_store() -> Arc<dyn Store> {
    Arc::new(InMemoryStore::new())
}

/// Deterministic LCG pseudo-random vector generator.
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

/// `RecordId` from a 2-byte index (covers 0..65535).
fn rid(i: usize) -> RecordId {
    let mut a = [0u8; 16];
    a[14] = (i >> 8) as u8;
    a[15] = (i & 0xFF) as u8;
    RecordId(a)
}

/// Clustered Gaussian generator (mirrors `quantized_graph_tests::Lcg`).
struct Lcg {
    state: u64,
}
impl Lcg {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }
    #[inline]
    fn next_u64(&mut self) -> u64 {
        self.state = self
            .state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.state
    }
    #[inline]
    fn next_f32(&mut self) -> f32 {
        let high = (self.next_u64() >> 32) as u32;
        (high as f32) / (1u64 << 32) as f32
    }
    #[inline]
    fn next_range(&mut self, lo: f32, hi: f32) -> f32 {
        lo + (hi - lo) * self.next_f32()
    }
    fn next_gaussian(&mut self) -> f32 {
        loop {
            let u1 = self.next_f32() * 2.0 - 1.0;
            let u2 = self.next_f32() * 2.0 - 1.0;
            let s = u1 * u1 + u2 * u2;
            if s > 0.0 && s < 1.0 {
                let mul = ((-2.0 * s.ln()) / s).sqrt();
                return u1 * mul;
            }
        }
    }
}

fn clustered(n: usize, dim: usize, k: usize, sigma: f32, seed: u64) -> Vec<Vec<f32>> {
    assert!(k > 0);
    let mut rng = Lcg::new(seed);
    let centroids: Vec<Vec<f32>> = (0..k)
        .map(|_| (0..dim).map(|_| rng.next_range(-1.0, 1.0)).collect())
        .collect();
    (0..n)
        .map(|i| {
            let c = &centroids[i % k];
            (0..dim)
                .map(|j| c[j] + sigma * rng.next_gaussian())
                .collect()
        })
        .collect()
}

/// Build a fitted (quantized) `HnswAdapter` over `n` clustered vectors and
/// return it together with the data so tests can run queries against the
/// same vectors.
async fn build_fitted_quant_adapter(
    n: usize,
    dim: u32,
    metric: VectorMetric,
    seed: u64,
) -> (HnswAdapter, Vec<Vec<f32>>) {
    let adapter = HnswAdapter::new_with_quantization(
        dim,
        metric,
        HnswConfig {
            max_elements: 10_000,
            m: 16,
            max_layer: 16,
            ef_construction: 200,
            ef_search: 64,
        },
        Some(VectorQuantization::Sq8),
    );
    let data = clustered(n, dim as usize, 10, 0.2, seed);
    let items: Vec<(RecordId, Vec<f32>)> = data
        .iter()
        .enumerate()
        .map(|(i, v)| (rid(i), v.clone()))
        .collect();
    adapter.upsert_batch(&items).await.unwrap();
    crate::hnsw_adapter::await_fitted(&adapter).await;
    assert!(adapter.is_quantized(), "test setup: adapter did not fit");
    (adapter, data)
}

// ============================================================================
// SPIKE — file_dump + load_hnsw_with_dist for Hnsw<'static, u8, ShamirDistU8>
// ============================================================================

/// Prove that `hnsw_rs` can dump and reload a `Hnsw<'static, u8, ShamirDistU8>`.
///
/// This is the precondition for the v2 snapshot format (dump the u8 graph
/// verbatim). If this works, the snapshot stores the u8 graph directly and
/// load restores it without rebuilding the graph from codes. The test builds
/// a small u8 HNSW, dumps it via `file_dump`, reloads via
/// `load_hnsw_with_dist` with a reconstructed `ShamirDistU8`, and verifies
/// that search returns the same top-k.
#[tokio::test]
async fn spike_file_dump_u8_works() {
    let dim = 16usize;
    // F-68 (#895, cluster A) root cause: `f6016bce` grew N from 300 to 1500
    // on the theory that a larger, well-connected graph would make the
    // self-id-in-top-10 pre-check reliable. It did not -- reproduced FAILING
    // again this session at n=1500 under `./scripts/test.sh -p shamir-index
    // --full` nextest concurrency (this exact panic:
    // "pre-dump: self-id 0 not in top-10"). Root cause, confirmed by reading
    // `hnsw_rs` 0.3.4's source directly: `LayerGenerator::new` seeds via
    // `StdRng::from_os_rng()` (src/hnsw.rs) -- genuinely unseedable, fresh
    // OS entropy every build, no way to fix from caller code. `parallel_insert`
    // additionally inserts via `datas.par_iter().for_each(...)` (rayon), so
    // insertion order is ALSO nondeterministic across runs; empirically
    // measured (300+ in-process trials, serial vs parallel, same fixture,
    // with and without artificial CPU contention) that this specific
    // fixture/params combination still fails a small fraction of the time —
    // growing N further would only lower, never eliminate, the failure rate,
    // repeating a fix already shown insufficient.
    //
    // The actual fix: `before_ids.contains(&0)` was testing HNSW recall
    // QUALITY (a statistical property of an approximate, RNG-seeded graph),
    // which is NOT this test's subject. The test's real subject is the
    // `before_ids == after_ids` dump/reload fidelity check below, which
    // stays an EXACT comparison regardless of what `before_ids` happens to
    // contain — a self-query landing outside top-10 does not, in any way,
    // make the dump/reload path less trustworthy. Dropped the self-id
    // assertion entirely rather than paper over it with a bigger N or a
    // retry; kept the non-empty check (a structural invariant that does not
    // depend on RNG luck: any live HNSW index returns SOME neighbours for a
    // query drawn from its own training data).
    let n = 1500usize;
    let data = clustered(n, dim, 8, 0.2, 0xCAFE);

    // Fit SQ8 quantizer on the data.
    let training: Vec<Vec<f32>> = data.clone();
    let quantizer = Arc::new(crate::sq8::Sq8Quantizer::fit(&training, dim));
    let dist = ShamirDistU8::new(Arc::clone(&quantizer), VectorMetric::L2);

    // Build codes (internal -> Vec<u8>) and construct a u8 HNSW.
    let codes_map: Vec<(usize, Vec<u8>)> = data
        .iter()
        .enumerate()
        .map(|(i, v)| (i, quantizer.quantize(v)))
        .collect();

    // Build the graph under spawn_blocking (CPU-bound rayon insert).
    let dist_for_build = dist.clone();
    let codes_for_build = codes_map.clone();
    let hnsw_u8: Arc<Hnsw<'static, u8, ShamirDistU8>> = tokio::task::spawn_blocking(move || {
        let hnsw = Hnsw::<u8, ShamirDistU8>::new(
            16,
            codes_for_build.len().max(1000) + 1000,
            16,
            200,
            dist_for_build,
        );
        let batch: Vec<(&Vec<u8>, usize)> = codes_for_build.iter().map(|(i, c)| (c, *i)).collect();
        hnsw.parallel_insert(&batch);
        Arc::new(hnsw)
    })
    .await
    .expect("spawn_blocking join");

    // Run a query against the live graph BEFORE dump.
    let q_codes = quantizer.quantize(&data[0]);
    let before = tokio::task::spawn_blocking({
        let hnsw = Arc::clone(&hnsw_u8);
        let q_codes = q_codes.clone();
        move || hnsw.search(&q_codes, 10, 64)
    })
    .await
    .expect("spawn_blocking join");
    let before_ids: Vec<usize> = before.iter().map(|n| n.d_id).collect();
    // Structural invariant only -- NOT an HNSW recall/quality assertion (see
    // the F-68 cluster A comment above for why the old self-id-in-top-10
    // check was removed). A live graph over 1500 real training vectors must
    // return SOME neighbours for a query drawn from that same training set;
    // this holds regardless of the unseedable layer-RNG's draw.
    assert!(!before_ids.is_empty(), "pre-dump search returned nothing");

    // Dump the u8 graph to a TempDir.
    let dump_dir = tempfile::tempdir().expect("tempdir");
    let basename = {
        let hnsw = Arc::clone(&hnsw_u8);
        let path = dump_dir.path().to_path_buf();
        tokio::task::spawn_blocking(move || hnsw.file_dump(&path, "spike").expect("file_dump u8"))
            .await
            .expect("spawn_blocking join")
    };
    let graph_path = dump_dir.path().join(format!("{basename}.hnsw.graph"));
    let data_path = dump_dir.path().join(format!("{basename}.hnsw.data"));
    assert!(graph_path.exists(), "graph dump file missing");
    assert!(data_path.exists(), "data dump file missing");

    // Reload the graph via HnswIo + load_hnsw_with_dist with a fresh
    // ShamirDistU8 (the dump carries the structure, not the distance impl).
    let leaked_io: &'static HnswIo = Box::leak(Box::new(HnswIo::new(dump_dir.path(), &basename)));
    let dist_for_load = ShamirDistU8::new(Arc::clone(&quantizer), VectorMetric::L2);
    let loaded = leaked_io
        .load_hnsw_with_dist(dist_for_load)
        .expect("load_hnsw_with_dist u8");

    // Same query on the reloaded graph — top-k must match.
    let after = tokio::task::spawn_blocking({
        // loaded is 'static (borrows the leaked HnswIo); we can move it.
        let loaded = Arc::new(loaded);
        let q_codes = q_codes.clone();
        move || loaded.search(&q_codes, 10, 64)
    })
    .await
    .expect("spawn_blocking join");
    let after_ids: Vec<usize> = after.iter().map(|n| n.d_id).collect();
    assert_eq!(
        before_ids, after_ids,
        "file_dump/load_u8 diverged: before={before_ids:?} after={after_ids:?}"
    );
}

// ============================================================================
// ROUND-TRIP — quantized snapshot v2 preserves recall + quantizer params
// ============================================================================

/// Dump a fitted quantized adapter (v2), load it back, and verify:
///  * `is_quantized()` is `true` after load (the u8 graph survived restart);
///  * `quantizer` params (mins/scales/dim) round-trip bit-for-bit;
///  * search recall@10 vs the pre-dump adapter stays high (≥ 0.90 — leaves
///    headroom for the hnsw_rs unseedable-RNG noise on a reload, which the
///    existing snapshot tests document).
#[tokio::test]
async fn round_trip_quant_snapshot_preserves_recall_and_params() {
    let dim = 32u32;
    let n = 400usize;
    let store = mem_store();
    let (adapter, data) = build_fitted_quant_adapter(n, dim, VectorMetric::L2, 0xBEEF).await;

    // Capture the quantizer params BEFORE dump.
    let q_before = adapter
        .quantizer()
        .expect("fitted adapter has a quantizer")
        .clone();
    let mins_before = q_before.mins().to_vec();
    let scales_before = q_before.scales().to_vec();
    let dim_before = q_before.dim();

    // Capture pre-dump top-10 for a handful of queries.
    let opts = SearchOpts::with_ef_search(128);
    let mut before: Vec<Vec<RecordId>> = Vec::new();
    for q in data.iter().take(20) {
        let r = adapter.search(q, 10, opts, None).await.unwrap();
        before.push(r.into_iter().map(|(r, _)| r).collect());
    }

    // Dump → load.
    dump_snapshot(&adapter, &store, KEYSPACE).await.unwrap();
    let loaded = load_snapshot(&store, KEYSPACE).await.unwrap();

    // The loaded adapter MUST be quantized.
    assert!(
        loaded.is_quantized(),
        "loaded adapter must be fitted (quantization survived restart)"
    );

    // Quantizer params round-trip bit-for-bit.
    let q_after = loaded
        .quantizer()
        .expect("loaded fitted adapter has a quantizer")
        .clone();
    assert_eq!(q_after.dim(), dim_before, "quantizer dim regressed");
    assert_eq!(q_after.mins(), &mins_before[..], "quantizer mins regressed");
    assert_eq!(
        q_after.scales(),
        &scales_before[..],
        "quantizer scales regressed"
    );

    // Recall@10 across the same queries.
    let mut total_recall = 0.0f32;
    for (i, q) in data.iter().take(20).enumerate() {
        let after = loaded.search(q, 10, opts, None).await.unwrap();
        let after_rids: Vec<RecordId> = after.into_iter().map(|(r, _)| r).collect();
        let hits = after_rids
            .iter()
            .filter(|r| before[i].iter().any(|b| b == *r))
            .count();
        total_recall += hits as f32 / before[i].len().max(1) as f32;
    }
    let avg_recall = total_recall / 20.0;
    // A v2 dump/load of the SAME graph preserves top-k bit-for-bit (the dump
    // is a byte-identical reload of the u8 graph structure), so we can pin a
    // HIGH floor. We leave a small margin for the rescore path's tie-break
    // nondeterminism on equal-distance candidates.
    assert!(
        avg_recall >= 0.95,
        "recall@10 = {avg_recall:.4} below 0.95 floor (v2 quant round-trip)"
    );
}

// ============================================================================
// V1 metadata layouts require a supported graph version; 0.3.4 rebuilds.
// ============================================================================

// ============================================================================
// CRASH / COLD-START for a quantized index — restart preserves is_fitted
// ============================================================================

/// Build a quantized adapter, dump a v2 snapshot, then simulate a cold start
/// by loading the snapshot into a FRESH adapter. The restarted adapter must:
///  * be fitted (`is_quantized() == true`);
///  * have its quantizer restored (same mins/scales/dim);
///  * return recall@10 ≥ 0.90 vs the pre-crash adapter (high — same graph).
#[tokio::test]
async fn crash_cold_start_quant_preserves_fitted_and_recall() {
    let dim = 24u32;
    let n = 300usize;
    let store = mem_store();
    let (adapter, data) = build_fitted_quant_adapter(n, dim, VectorMetric::Cosine, 0xDEAD).await;

    // Pre-dump queries.
    let opts = SearchOpts::with_ef_search(128);
    let mut before: Vec<Vec<RecordId>> = Vec::new();
    for q in data.iter().take(15) {
        let r = adapter.search(q, 10, opts, None).await.unwrap();
        before.push(r.into_iter().map(|(r, _)| r).collect());
    }

    // Dump the quantized snapshot (v2).
    dump_snapshot(&adapter, &store, KEYSPACE).await.unwrap();

    // Simulate crash + cold start: load from the store.
    let restarted = load_snapshot(&store, KEYSPACE).await.unwrap();

    // The restarted adapter must be quantized.
    assert!(
        restarted.is_quantized(),
        "cold-start: quantization did not survive restart"
    );
    assert!(
        restarted.quantizer().is_some(),
        "quantizer missing post-load"
    );
    assert!(
        restarted.hnsw_u8_handle().is_some(),
        "u8 graph missing post-load"
    );

    // Recall@10 vs pre-crash.
    let mut total_recall = 0.0f32;
    for (i, q) in data.iter().take(15).enumerate() {
        let after = restarted.search(q, 10, opts, None).await.unwrap();
        let after_rids: Vec<RecordId> = after.into_iter().map(|(r, _)| r).collect();
        let hits = after_rids
            .iter()
            .filter(|r| before[i].iter().any(|b| b == *r))
            .count();
        total_recall += hits as f32 / before[i].len().max(1) as f32;
    }
    let avg_recall = total_recall / 15.0;
    assert!(
        avg_recall >= 0.90,
        "cold-start recall@10 = {avg_recall:.4} below 0.90 floor"
    );
}

// ============================================================================
// COMPACTION + SNAPSHOT + RESTART — quant state survives the full cycle
// ============================================================================

/// This is a lighter-weight test than the full VectorBackend compaction path
/// (covered in `compaction_tests.rs`). It exercises the snapshot codec's
/// responsibility in the compaction cycle: a fitted quantized adapter,
/// dumped AFTER a compaction-style rebuild, still produces a valid v2
/// snapshot whose load preserves `is_fitted` + recall. The brief's #408
/// "force-snapshot after rebuild writes v2" contract is exercised at the
/// codec level here.
#[tokio::test]
async fn compaction_snapshot_restart_preserves_quant_state() {
    let dim = 16u32;
    let n = 300usize;
    let store = mem_store();
    let (adapter, data) = build_fitted_quant_adapter(n, dim, VectorMetric::L2, 0xFEED).await;

    // Simulate a post-compaction force-snapshot: dump a fresh v2 snapshot.
    dump_snapshot(&adapter, &store, KEYSPACE).await.unwrap();

    // Restart by loading the snapshot.
    let restarted = load_snapshot(&store, KEYSPACE).await.unwrap();
    assert!(
        restarted.is_quantized(),
        "post-compaction restart: quantization lost"
    );

    // Recall@10 must be preserved (the compaction rebuild produced a fresh
    // u8 graph; the snapshot captures it; load restores it verbatim).
    let opts = SearchOpts::with_ef_search(128);
    let mut hits_total = 0usize;
    let mut hits_possible = 0usize;
    for (i, q) in data.iter().take(20).enumerate() {
        let before = adapter.search(q, 10, opts, None).await.unwrap();
        let after = restarted.search(q, 10, opts, None).await.unwrap();
        let before_set: shamir_collections::TFxSet<RecordId> =
            before.into_iter().map(|(r, _)| r).collect();
        for (r, _) in &after {
            if before_set.contains(r) {
                hits_total += 1;
            }
            hits_possible += 1;
        }
        // i is used to iterate; silence unused warning in case of refactor.
        let _ = i;
    }
    let recall = hits_total as f32 / hits_possible as f32;
    assert!(
        recall >= 0.90,
        "compaction+snapshot+restart recall@10 = {recall:.4} below 0.90"
    );
}

// ============================================================================
// Non-quantized v2 snapshot round-trip
// ============================================================================

/// A non-quantized v2 snapshot round-trips without a u8 graph or quantizer.
/// This does not establish compatibility with historical v1 graph bytes.
#[tokio::test]
async fn non_quant_v2_round_trips() {
    let dim = 16u32;
    let n = 300usize;
    let store = mem_store();
    let adapter = HnswAdapter::new(dim, VectorMetric::L2, HnswConfig::default());
    for i in 0..n {
        adapter
            .upsert(rid(i), &lcg_vec(dim as usize, i as u64))
            .await
            .unwrap();
    }
    // Pre-dump top-k.
    let opts = SearchOpts::with_ef_search(64);
    let queries: Vec<Vec<f32>> = (0..5u64).map(|s| lcg_vec(dim as usize, s + 99)).collect();
    let mut before: Vec<Vec<RecordId>> = Vec::new();
    for q in &queries {
        let r = adapter.search(q, 10, opts, None).await.unwrap();
        before.push(r.into_iter().map(|(r, _)| r).collect());
    }

    dump_snapshot(&adapter, &store, KEYSPACE).await.unwrap();

    // Verify the sidecar carries format_version = 2 and quantization = None.
    let sidecar_k = snapshot::sidecar_key_for_test(KEYSPACE, 0);
    let sidecar_bytes = store.get(sidecar_k).await.unwrap();
    let sidecar: SnapshotSidecar = MetaEnvelope::open(&sidecar_bytes).unwrap();
    assert_eq!(
        sidecar.format_version, SNAPSHOT_FORMAT_VERSION,
        "non-quant v2 dump must stamp the current format version"
    );
    assert!(
        sidecar.quantization.is_none(),
        "non-quant v2 sidecar must have quantization = None"
    );

    let loaded = load_snapshot(&store, KEYSPACE).await.unwrap();
    // Non-quantized → NOT fitted.
    assert!(
        !loaded.is_quantized(),
        "non-quant v2 load must not produce a fitted adapter"
    );

    // top-k id sets must be IDENTICAL pre/post (same graph reload).
    for (i, q) in queries.iter().enumerate() {
        let after = loaded.search(q, 10, opts, None).await.unwrap();
        let after_set: shamir_collections::TFxSet<RecordId> =
            after.into_iter().map(|(r, _)| r).collect();
        let before_set: shamir_collections::TFxSet<RecordId> = before[i].iter().copied().collect();
        assert_eq!(
            before_set, after_set,
            "non-quant v2 top-10 diverged for query #{}",
            i
        );
    }
}

// ============================================================================
// #423 (Б-1) — snapshot round-trip under concurrent fit-transition races:
// the v2 dump serialises `vectors_u8` AND `hnsw_u8`. Before the fix, vectors
// that entered `vectors_u8` without a graph node (catch-up / self-migration
// holes) rode into the snapshot as holes — the loaded adapter had FEWER
// retrievable vectors than the pre-dump adapter. This test forces the race
// (concurrent upserts across the fit boundary, dataset >512 so the graph
// path is exercised), dumps, loads, and asserts no-count-loss round-trip.
// ============================================================================

#[tokio::test]
async fn snapshot_round_trip_after_concurrent_fit_no_node_loss() {
    let dim = 24u32;
    let store = mem_store();
    let adapter = Arc::new(HnswAdapter::new_with_quantization(
        dim,
        VectorMetric::L2,
        HnswConfig {
            max_elements: 10_000,
            m: 16,
            max_layer: 16,
            ef_construction: 200,
            ef_search: 64,
        },
        Some(VectorQuantization::Sq8),
    ));

    // 600 vectors, 8 concurrent tasks racing the fit transition. This is
    // the exact Б-1 window: upserts land in `vectors` after the fitter's
    // delta scan → catch-up / self-migration must insert their graph node.
    let n = 600usize;
    let data = clustered(n, dim as usize, 24, 0.2, 0x0524_2342);
    let n_tasks = 8usize;
    let per_task = data.len() / n_tasks;
    let mut handles = Vec::new();
    for t in 0..n_tasks {
        let adapter = Arc::clone(&adapter);
        let chunk: Vec<(RecordId, Vec<f32>)> = data[t * per_task..(t + 1) * per_task]
            .iter()
            .enumerate()
            .map(|(j, v)| (rid(t * per_task + j), v.clone()))
            .collect();
        handles.push(tokio::spawn(async move {
            adapter.upsert_batch(&chunk).await.expect("upsert_batch");
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    crate::hnsw_adapter::await_fitted(&adapter).await;
    assert!(adapter.is_quantized(), "adapter did not fit under load");
    let live_before = adapter.len();
    assert!(
        live_before > 512,
        "test must exercise the graph path: live_before={live_before} > 512"
    );

    // #423 (Б-1) DETERMINISTIC pre-dump check: every upserted internal MUST
    // have a node in `hnsw_u8`. Before the fix, the catch-up / self-migration
    // paths populated `vectors_u8` WITHOUT inserting the u8 graph node — so
    // `get_nb_point()` would be LESS than the number of upserted vectors.
    // `hnsw_rs::get_nb_point` returns the total number of inserted DataPoints.
    // We assert EXACT equality (no HNSW approximation involved).
    let nodes_before = adapter.hnsw_u8_handle().expect("fitted").get_nb_point();
    assert_eq!(
        nodes_before, n,
        "pre-dump: u8 graph has {nodes_before} nodes but {n} vectors were upserted — \
         Б-1 graph-connectivity loss (fit-window vectors missing from hnsw_u8)"
    );

    // Dump the v2 snapshot (serialises hnsw_u8 + vectors_u8).
    dump_snapshot(&adapter, &store, KEYSPACE).await.unwrap();

    // Cold-start load.
    let loaded = load_snapshot(&store, KEYSPACE).await.unwrap();
    assert!(loaded.is_quantized(), "loaded adapter must be fitted (v2)");
    let live_after = loaded.len();
    assert_eq!(
        live_after, live_before,
        "round-trip live-count mismatch: before={live_before} after={live_after} \
         (snapshot lost or duplicated nodes)"
    );

    // Post-load, the graph MUST hold the SAME number of nodes (the round-trip
    // preserved every node — no holes rode into the dump). This is the
    // deterministic round-trip Б-1 invariant.
    let nodes_after = loaded
        .hnsw_u8_handle()
        .expect("fitted post-load")
        .get_nb_point();
    assert_eq!(
        nodes_after, nodes_before,
        "post-load: u8 graph has {nodes_after} nodes but had {nodes_before} pre-dump — \
         snapshot round-trip lost graph nodes (Б-1 holes rode into the dump)"
    );
}

// ============================================================================
// QuantMeta (de)serialization sanity
// ============================================================================

/// `QuantMeta` must bincode-encode/decode round-trip bit-for-bit — the v2
/// sidecar stores `bincode(QuantMeta)` inside `quantization: Option<Vec<u8>>`.
#[test]
fn quant_meta_bincode_round_trip() {
    let meta = QuantMeta {
        method: "sq8".to_string(),
        dim: 128,
        mins: vec![-1.0f32, 0.0, 1.5],
        scales: vec![0.01, 0.02, 0.03],
    };
    let bytes = bincode::serialize(&meta).unwrap();
    let back: QuantMeta = bincode::deserialize(&bytes).unwrap();
    assert_eq!(back.method, "sq8");
    assert_eq!(back.dim, 128);
    assert_eq!(back.mins, meta.mins);
    assert_eq!(back.scales, meta.scales);
}

// Silence the unused-import warning for `SnapshotError` (kept for future
// error-path tests) and `HNSW_RS_VERSION` (asserted here for parity).
#[test]
fn snapshot_constants_are_stable() {
    assert_eq!(SNAPSHOT_FORMAT_VERSION, 2);
    assert_eq!(HNSW_RS_VERSION, "0.3.5");
    // SnapshotError must be Debug (used in `match` arms across the suite).
    let _: Option<SnapshotError> = None;
}

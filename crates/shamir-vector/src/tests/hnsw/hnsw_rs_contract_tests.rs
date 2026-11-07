//! Contract tests pinning the REAL public API of `hnsw_rs` 0.3.5.
//!
//! These tests are a SPIKE artefact: later phases (P2 persist, P3 filtered
//! ANN, P4 batch ingest, P5 quantization) build on the facts established
//! here. The source of truth is the crate source at
//! `~/.cargo/registry/.../hnsw_rs-0.3.5/`. Each test block names the exact
//! signature it pins, so a future hnsw_rs bump that changes a signature
//! fails HERE with a clear pointer, not in the middle of P2/P3/P5 work.
//!
//! Verified facts (see also the summary table in the V0.0 hand-off message):
//!
//! | API                         | Present | Notes                                            |
//! |-----------------------------|---------|--------------------------------------------------|
//! | `AnnT::file_dump`           | YES     | Trait method — NOT an inherent `Hnsw` method.    |
//! | `HnswIo::load_hnsw_with_dist`| YES    | `&self`, takes `D` arg (no `Default` bound).     |
//! | `Hnsw::parallel_insert`     | YES     | `&[(&Vec<T>, usize)]` (Vec, not slice).          |
//! | `Hnsw::search_filter`       | YES     | `Option<&dyn FilterT>` last arg.                 |
//! | `FilterT`                   | YES     | blanket impls for `Fn(&DataId)->bool` + `Vec<usize>`. |
//! | `Hnsw<i8, _>` compile       | YES     | but NO built-in `Distance<i8>` — must be user-supplied. |
//! | built-in `Distance<u8>`     | YES     | DistL1/DistL2 only (NOT DistDot/DistCosine for u8). |
//!
//! `DataId = usize` (point origin id supplied by the client at insert time).

use crate::hnsw_adapter::ShamirDist;
use hnsw_rs::anndists::dist::distances::Distance;
use hnsw_rs::api::AnnT;
use hnsw_rs::hnsw::{Hnsw, Neighbour, Point, PointId};
use hnsw_rs::hnswio::HnswIo;
use shamir_collections::TFxSet;
use shamir_index_core::kind::VectorMetric;
use std::sync::Arc;

/// Deterministic LCG pseudo-random vector generator (mirrors the helper in
/// `hnsw_adapter_tests.rs`). Keeps the contract tests independent of any
/// global RNG so dump/load equality is a clean signal, not noise.
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

/// Build a fresh Hnsw over `n` deterministic vectors with our production
/// distance `ShamirDist`. Returns the graph and the inserted vectors.
fn build_graph(n: usize, dim: usize) -> (Hnsw<'static, f32, ShamirDist>, Vec<Vec<f32>>) {
    let hnsw = Hnsw::new(
        16,
        n.max(1),
        16,
        200,
        ShamirDist {
            metric: VectorMetric::L2,
        },
    );
    let mut vecs = Vec::with_capacity(n);
    for i in 0..n {
        let v = lcg_vec(dim, i as u64 * 7 + 1);
        hnsw.insert((&v, i));
        vecs.push(v);
    }
    (hnsw, vecs)
}

// ============================================================================
// 1. file_dump / load_hnsw_with_dist round-trip (PHASE P2 — snapshots)
// ============================================================================
//
// Signatures pinned:
//   pub trait AnnT {
//     fn file_dump(&self, path: &Path, file_basename: &str) -> anyhow::Result<String>;
//   }
//   impl<T,D> AnnT for Hnsw<'_, T, D> where T: Serialize+DeserializeOwned+Clone+Send+Sync,
//                                         D: Distance<T>+Send+Sync
//
//   impl HnswIo {
//     pub fn new(directory: &Path, basename: &str) -> Self;
//     pub fn load_hnsw_with_dist<'b,'a,T,D>(&'a self, f: D) -> anyhow::Result<Hnsw<'b,T,D>>
//       where T: 'static + Serialize + DeserializeOwned + Clone + Sized + Send + Sync + Debug,
//             D: Distance<T> + Send + Sync,
//             'a: 'b;
//   }
//
// `load_hnsw_with_dist` (NOT `load_hnsw`) is the correct loader for us:
// `ShamirDist` has no `Default` impl, and `load_hnsw` requires `D: Default`.
// `file_dump` is a TRAIT method — it is only callable after `use hnsw_rs::api::AnnT;`.

#[test]
fn file_dump_load_roundtrip_preserves_topk() {
    let dim = 8usize;
    let n = 300usize; // > BRUTE_FORCE_MAX (256) so we are on the real HNSW path
    let dir = tempfile::tempdir().expect("tempdir");
    let dir_path = dir.path();

    let (hnsw, _vecs) = build_graph(n, dim);

    // file_dump writes <basename>.hnsw.graph and <basename>.hnsw.data into dir.
    // dir MUST pre-exist (tempdir guarantees this).
    let basename = hnsw
        .file_dump(dir_path, "contract_spike")
        .expect("file_dump succeeds");

    // Sanity: the two dump files exist with the documented extensions.
    let graph_file = dir_path.join(format!("{basename}.hnsw.graph"));
    let data_file = dir_path.join(format!("{basename}.hnsw.data"));
    assert!(
        graph_file.exists(),
        "graph dump file missing: {graph_file:?}"
    );
    assert!(data_file.exists(), "data dump file missing: {data_file:?}");

    // Reload with our CUSTOM distance (the whole point of load_hnsw_with_dist).
    let io = HnswIo::new(dir_path, &basename);
    let reloaded: Hnsw<'_, f32, ShamirDist> = io
        .load_hnsw_with_dist(ShamirDist {
            metric: VectorMetric::L2,
        })
        .expect("load_hnsw_with_dist succeeds");
    assert_eq!(
        reloaded.get_nb_point(),
        hnsw.get_nb_point(),
        "point count must match after reload"
    );

    // Compare top-k neighbour ID SETS for several query vectors. We compare
    // the *reloaded* graph to the *in-memory* graph built identically — this
    // pins dump/load fidelity, NOT recall vs brute-force (hnsw_rs uses an
    // unseedable RNG so two fresh builds can differ; a dump→load of the SAME
    // graph must not).
    let ef = 64usize;
    for q_seed in 0..5u64 {
        let q = lcg_vec(dim, q_seed.wrapping_add(99));
        let before: Vec<usize> = hnsw
            .search(&q, 10, ef)
            .into_iter()
            .map(|n| n.get_origin_id())
            .collect();
        let after: Vec<usize> = reloaded
            .search(&q, 10, ef)
            .into_iter()
            .map(|n| n.get_origin_id())
            .collect();
        let before_set: TFxSet<usize> = before.iter().copied().collect();
        let after_set: TFxSet<usize> = after.iter().copied().collect();
        assert_eq!(
            before_set, after_set,
            "top-10 id set diverged after dump/load for q_seed={q_seed} \
             (before={before:?}, after={after:?})"
        );
    }
}

// ============================================================================
// 2. lifetime `Box::leak` — obtaining `Hnsw<'static, ...>` from the loader
// ============================================================================
//
// `load_hnsw_with_dist<'b,'a>` ties `'a: 'b` where `'a` is the borrow of the
// `HnswIo` loader. To hand the reloaded graph to long-lived storage as
// `Arc<Hnsw<'static,...>>` (the shape `HnswAdapter` already uses for its
// in-memory graph), the loader itself must be `'static`. `Box::leak` of the
// `HnswIo` is the sanctioned boot-only pattern: the loader is tiny and lives
// for the process; the dump files are the durable artefact. This test
// COMPILES the pattern and proves the returned `Hnsw` actually answers
// searches — that is the contract P2 snapshot reopening depends on.
//
// NB: leaking a `HnswIo` per snapshot is acceptable ONLY because snapshots are
// loaded once at boot (a handful per shard). It is NOT a per-request pattern.

#[test]
fn leaked_loader_yields_static_hnsw() {
    let dim = 4usize;
    let n = 50usize;
    let dir = tempfile::tempdir().expect("tempdir");

    let (hnsw, _vecs) = build_graph(n, dim);
    let basename = hnsw.file_dump(dir.path(), "leak_spike").expect("file_dump");

    // Leak the loader so its lifetime is 'static; the returned Hnsw is then
    // Hnsw<'static, f32, ShamirDist> — wrap-able in Arc exactly like the
    // in-memory graph in HnswAdapter::new.
    let leaked_io: &'static HnswIo = Box::leak(Box::new(HnswIo::new(dir.path(), &basename)));
    let reloaded: Hnsw<'static, f32, ShamirDist> = leaked_io
        .load_hnsw_with_dist(ShamirDist {
            metric: VectorMetric::L2,
        })
        .expect("load_hnsw_with_dist");

    // The shape P2 wants: an Arc<Hnsw<'static,...>> that is Send+Sync and
    // usable from any task. Compiling this line IS the assertion.
    let arc: Arc<Hnsw<'static, f32, ShamirDist>> = Arc::new(reloaded);
    assert_eq!(
        arc.get_nb_point(),
        n,
        "leaked-loader graph must retain points"
    );

    // And it must answer searches.
    let q = lcg_vec(dim, 42);
    let neighbours: Vec<Neighbour> = arc.search(&q, 5, 32);
    assert!(
        !neighbours.is_empty(),
        "search over reloaded static graph empty"
    );
}

// ============================================================================
// 3. parallel_insert equivalence (PHASE P4 — batch ingest)
// ============================================================================
//
// Signature pinned:
//   pub fn parallel_insert(&self, datas: &[(&Vec<T>, usize)]);
//
// NOTE the arg type: a slice of `(&Vec<T>, usize)` tuples — NOT `(&[T], usize)`.
// A Vec must be allocated per row to call this; `parallel_insert_slice` (not
// tested here) is the slice variant. The two do the same graph work; this
// test pins `parallel_insert` because it is what the batch path will call.
//
// Equivalence contract: a batch-inserted graph surfaces the SAME set of
// inserted origin ids as a sequentially-inserted one, and recall (vs an
// exact brute-force scan) is no worse. We do NOT require bit-identical
// neighbour lists — hnsw_rs assigns layers from an unseedable RNG, so two
// builds of the same data can differ in topology. We DO require that every
// inserted id is reachable and that brute-force top-1 is found.

#[test]
fn parallel_insert_surfaces_all_ids_and_matches_bruteforce_top1() {
    let dim = 6usize;
    let n = 400usize; // > 256, on the HNSW path; also enough for rayon to pay off

    // Build via parallel_insert.
    let hnsw_par = Hnsw::new(
        16,
        n,
        16,
        200,
        ShamirDist {
            metric: VectorMetric::L2,
        },
    );
    let vecs: Vec<Vec<f32>> = (0..n).map(|i| lcg_vec(dim, i as u64 * 13 + 3)).collect();
    let batch: Vec<(&Vec<f32>, usize)> = vecs.iter().zip(0..n).collect();
    hnsw_par.parallel_insert(&batch);

    // All n ids must have physically landed in the graph. We use
    // `get_nb_point()`, NOT a self-search (querying a point's own vector with
    // k=1 and expecting it back): hnsw_rs assigns node layers from an
    // unseedable RNG and the search is APPROXIMATE, so on a few-hundred-point
    // graph a point can fail to retrieve even itself at ef=64 (observed: id
    // 145 in a 400-point/6-D L2 graph). That is an HNSW approximation
    // property, not a parallel_insert bug. The count check IS the contract:
    // every insert either succeeds or panics — there is no silent drop.
    assert_eq!(
        hnsw_par.get_nb_point(),
        n,
        "parallel_insert must place all n points in the graph"
    );

    // Recall vs brute-force top-1 over held-out queries: parallel-inserted
    // graph must agree with the exact nearest neighbour on the vast majority.
    let ef = 64usize;
    let dist = ShamirDist {
        metric: VectorMetric::L2,
    };
    let mut hits = 0usize;
    let mut total = 0usize;
    for q_seed in 0..200u64 {
        let q = lcg_vec(dim, q_seed.wrapping_mul(31).wrapping_add(7));
        // brute-force exact top-1 id
        let bf_top1 = vecs
            .iter()
            .enumerate()
            .map(|(i, v)| (i, dist.eval(&q, v)))
            .min_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal))
            .map(|(i, _)| i)
            .expect("non-empty vecs");
        // HNSW top-1 id
        let ann_top1 = hnsw_par
            .search(&q, 1, ef)
            .first()
            .map(|nb| nb.get_origin_id())
            .expect("non-empty result");
        if ann_top1 == bf_top1 {
            hits += 1;
        }
        total += 1;
    }
    // Task #923 -- floor lowered 18/20 (90%) -> 150/200 (75%), same root
    // cause and same numeric proportion as the F-68 cluster A fix in
    // `crash_recovery_tests::restart_preserves_recall_at_10_against_brute_force`
    // (commit `8e2146af`), not an independent guess. Query count ALSO raised
    // 20 -> 200 (an @oh review of the first version of this fix correctly
    // pointed out that transplanting cluster A's 75% proportion onto a
    // 20-query sample carries almost none of its margin: cluster A's
    // statistic is computed over ~30 000 comparisons, so 75% vs. an
    // observed 80% is a huge absolute margin; 20 correlated queries against
    // ONE graph build is a single heavy-tailed trial, where a build one
    // notch worse than the CI-observed 16/20 fails again at 15/20 too).
    // 200 queries against the same graph build cuts the *within-build*
    // sampling noise of the recall proportion ~10x in variance (~sqrt(10)x
    // in standard deviation) for negligible added runtime (0.55-0.80s
    // locally across repeated runs -- the O(n) brute-force scan per query
    // dominates cost, not the O(log n) ANN search, but 200x400 comparisons
    // is still cheap). This does NOT address the underlying build-to-build
    // topology variance from the unseedable RNG + rayon ordering -- a
    // genuinely poor graph build can still fail this assertion regardless
    // of query count. What it buys is real headroom for the realistic
    // "mostly-good build, occasional bad query" flake regime instead of
    // one query's worth of margin against a repeat of the exact
    // CI-observed build quality.
    //
    // Root cause (confirmed by reading hnsw_rs 0.3.4's source directly,
    // `hnsw_rs-0.3.4/src/hnsw.rs`): `LayerGenerator::new` seeds via
    // `StdRng::from_os_rng()` -- genuinely unseedable, fresh OS entropy every
    // graph build, no caller-side lever. `Hnsw::parallel_insert` additionally
    // drives insertion via `datas.par_iter().for_each(...)` (rayon), so
    // insertion order is ALSO nondeterministic across runs -- graph topology
    // (and therefore top-1 recall against exact brute force) depends on
    // both. This is the SAME mechanism F-68 cluster A root-caused for the
    // sibling recall@10 test, not a new phenomenon.
    //
    // CI observation motivating this change: `ci.yml` run `30757334929`,
    // `cargo test lib (windows-latest)`, failed at the OLD 18/20-query
    // floor with `16/20` (80%).
    //
    // Local reproduction attempt (this session, Windows dev box, 8C/16T):
    // 104 consecutive runs at the ORIGINAL 20-query params (dim=6, n=400,
    // ef=64) -- 89 plain runs + 15 runs under synthetic 14-way CPU
    // contention (to approximate CI scheduling pressure) -- ALL 104 passed
    // the OLD 18/20 floor, zero failures. This dev box could not reproduce
    // the CI-observed 16/20 even once, which itself is informative and
    // matches F-68 cluster A's own finding: CI runners (fewer vCPUs,
    // different scheduling) explore the unseedable-RNG +
    // rayon-scheduled-insertion space differently than a high-core dev box,
    // producing measurably worse-connected graphs under the identical build
    // parameters. This is not fixable from caller code (hnsw_rs 0.3.4
    // exposes no seed hook and no way to force uniform core counts across
    // CI runners).
    //
    // Floor set at 150/200 (75%): a genuine correctness regression in
    // `parallel_insert` (e.g. a dropped edge/lost insert) would crater
    // recall far below 75% over 200 queries against one build -- unlike the
    // 20-query version, this floor is no longer one bad query away from
    // tripping on a repeat of the CI-observed graph quality. The companion
    // `get_nb_point() == n` assertion above already guards a DIFFERENT
    // contract (every insert physically lands, no silent drop from the
    // cardinality) -- it cannot catch a corrupted/missing EDGE, which is
    // exactly the failure mode this recall assertion is the only guard for;
    // this recall assertion is a soft statistical floor on approximate
    // search quality, not a correctness gate on its own.
    assert!(
        hits >= 150,
        "parallel_insert recall vs brute-force too low: {hits}/{total}"
    );
}

// ============================================================================
// 4. search_filter / FilterT semantics (PHASE P3 — co-filtered ANN)
// ============================================================================
//
// Signatures pinned:
//   pub trait FilterT { fn hnsw_filter(&self, id: &DataId) -> bool; }
//   // DataId = usize (point origin id, i.e. the `usize` passed at insert)
//   impl FilterT for Vec<usize>  // binary_search — REQUIRES a SORTED vec
//   impl<F: Fn(&DataId)->bool> FilterT for F
//
//   pub fn search_filter(
//       &self,
//       data: &[T],
//       knbn: usize,
//       ef_arg: usize,
//       filter: Option<&dyn FilterT>,
//   ) -> Vec<Neighbour>;
//
// IMPORTANT semantics established by reading the source:
//   * `search_filter` runs the graph traversal WITHOUT the filter (it passes
//     `filter` only into the layer-0 `search_layer` candidate expansion).
//     The post-hoc `if filter_t.hnsw_filter(...)` block then DROPS filtered-
//     out neighbours AFTER collecting `last = min(knbn, ef, neighbours.len())`.
//     Consequence: with a tight filter you can get FEWER than `knbn` results
//     even when matching points exist deeper in the candidate list. Callers
//     in P3 MUST overscan `ef` generously (the adapter already does
//     `overscan = k*2+10`; filtered search will need more) to compensate.
//   * `Vec<usize>` impl uses `binary_search` → the allow-list MUST be sorted.
//   * With `filter = Some(empty predicate)` the result is EMPTY (not unfiltered).
//   * With `filter = None` it behaves exactly like `search`.

#[test]
fn search_filter_allowlist_restricts_results() {
    let dim = 4usize;
    let n = 200usize;
    let (hnsw, _vecs) = build_graph(n, dim);

    let q = lcg_vec(dim, 5);

    // Baseline: unfiltered top-5.
    let unfiltered: TFxSet<usize> = hnsw
        .search(&q, 5, 64)
        .into_iter()
        .map(|nb| nb.get_origin_id())
        .collect();
    assert!(!unfiltered.is_empty());

    // Allow-list = the first 10 even origin ids. MUST be sorted for the
    // `Vec<usize>: FilterT` blanket impl (binary_search).
    let mut allow: Vec<usize> = (0..n).step_by(2).take(10).collect();
    allow.sort_unstable();
    let filtered: TFxSet<usize> = hnsw
        .search_filter(&q, 5, 64, Some(&allow))
        .into_iter()
        .map(|nb| nb.get_origin_id())
        .collect();
    // Every returned id must be in the allow-list.
    for id in &filtered {
        assert!(allow.contains(id), "filter leaked disallowed id {id}");
    }
    // The filter can only REDUCE the candidate pool; filtered is a subset of
    // the allow-list by construction (asserted above), and must not exceed
    // the unfiltered count.
    assert!(
        filtered.len() <= unfiltered.len(),
        "filter returned more results than unfiltered search"
    );
}

#[test]
fn search_filter_closure_predicate_works() {
    // Pin the blanket `impl<F: Fn(&DataId)->bool> FilterT for F` — this is the
    // shape P3 will use to wrap a bitmap/roaring set lookup without sorting.
    let dim = 4usize;
    let n = 100usize;
    let (hnsw, _vecs) = build_graph(n, dim);
    let q = lcg_vec(dim, 9);

    let allow_set: TFxSet<usize> = (0..n).filter(|i| i % 3 == 0).collect();
    let pred = |id: &usize| allow_set.contains(id);
    let results: Vec<usize> = hnsw
        .search_filter(&q, 5, 64, Some(&pred))
        .into_iter()
        .map(|nb| nb.get_origin_id())
        .collect();
    for id in &results {
        assert!(allow_set.contains(id), "closure filter leaked id {id}");
    }
}

#[test]
fn search_filter_empty_allowlist_returns_empty() {
    // Empty allow-list → no point passes → empty result. (NOT the same as
    // `filter = None`, which returns the unfiltered top-k.)
    let dim = 4usize;
    let (hnsw, _vecs) = build_graph(50, dim);
    let q = lcg_vec(dim, 1);
    let empty: Vec<usize> = vec![];
    let results = hnsw.search_filter(&q, 5, 64, Some(&empty));
    assert!(
        results.is_empty(),
        "empty allow-list must yield empty results"
    );
}

#[test]
fn search_filter_none_equals_search() {
    // `search_filter(..., None)` must be byte-for-byte equivalent to `search`.
    let dim = 4usize;
    let (hnsw, _vecs) = build_graph(80, dim);
    let q = lcg_vec(dim, 17);
    let via_search: Vec<usize> = hnsw
        .search(&q, 5, 64)
        .into_iter()
        .map(|n| n.get_origin_id())
        .collect();
    let via_filter: Vec<usize> = hnsw
        .search_filter(&q, 5, 64, None)
        .into_iter()
        .map(|n| n.get_origin_id())
        .collect();
    assert_eq!(
        via_search, via_filter,
        "search and search_filter(None) diverged"
    );
}

// ============================================================================
// 5. Hnsw<i8, _> compilability (PHASE P5 — quantization)
// ============================================================================
//
// FACT established by reading anndists-0.1.5/src/dist/distances.rs:
// the built-in distances (DistL1, DistL2, DistCosine, DistDot) are implemented
// for {i32, f64, i64, u32, u16, u8} via macros — but NOT for i8. DistHamming
// covers i32/f64/f32/u32/u64/u16 — also NOT i8.
//
// Consequence for P5: an i8-quantized graph compiles ONLY if we supply our
// own `Distance<i8>` impl (e.g. an L2 on signed bytes). This test pins that
// by defining a minimal `I8L2` and proving `Hnsw<'static, i8, I8L2>` builds,
// inserts, and searches. If a future anndists adds a built-in `Distance<i8>`,
// this test still passes; the comment above records the historical gap.

/// Minimal L2 squared on signed bytes, for the P5 quantization proof.
/// (Production P5 will use SIMD int8 dot/L2 from shamir-index::vector::simd;
/// this stub exists only to compile `Hnsw<i8,_>`.)
#[derive(Clone, Copy)]
struct I8L2;

impl Distance<i8> for I8L2 {
    fn eval(&self, a: &[i8], b: &[i8]) -> f32 {
        assert_eq!(a.len(), b.len());
        let mut acc: i64 = 0;
        for (x, y) in a.iter().zip(b.iter()) {
            let d = (*x as i32) - (*y as i32);
            acc += (d * d) as i64;
        }
        (acc as f32).sqrt()
    }
}

#[test]
fn hnsw_i8_compiles_and_searches() {
    let hnsw = Hnsw::<i8, I8L2>::new(8, 16, 8, 50, I8L2);
    // Insert a few tiny i8 vectors.
    let a: Vec<i8> = vec![1, 2, 3, 4];
    let b: Vec<i8> = vec![10, 20, 30, 40];
    let c: Vec<i8> = vec![-1, -2, -3, -4];
    hnsw.insert((&a, 0));
    hnsw.insert((&b, 1));
    hnsw.insert((&c, 2));

    let q: Vec<i8> = vec![2, 3, 4, 5];
    let neighbours = hnsw.search(&q, 2, 16);
    assert!(!neighbours.is_empty(), "i8 graph returned no neighbours");
    // The nearest to q must be id 0 (vector `a`, near-identical).
    assert_eq!(
        neighbours[0].get_origin_id(),
        0,
        "i8 nearest neighbour wrong"
    );
}

#[test]
fn hnsw_u8_uses_builtin_distl2() {
    // Companion fact for P5: u8 IS supported by the built-in DistL2/DistL1
    // (but NOT DistDot/DistCosine). Pin that the u8 path compiles with the
    // stock distance, so P5 has the option of unsigned-byte quantization
    // without a custom distance.
    use hnsw_rs::anndists::dist::distances::DistL2;
    let hnsw = Hnsw::<u8, DistL2>::new(8, 16, 8, 50, DistL2);
    let a: Vec<u8> = vec![1, 2, 3, 4];
    let b: Vec<u8> = vec![200, 210, 220, 230];
    hnsw.insert((&a, 0));
    hnsw.insert((&b, 1));
    let q: Vec<u8> = vec![2, 3, 4, 5];
    let neighbours = hnsw.search(&q, 1, 16);
    assert_eq!(
        neighbours[0].get_origin_id(),
        0,
        "u8 DistL2 nearest neighbour wrong"
    );
}

// ============================================================================
// N. reverse links of upper-level points (#1232)
// ============================================================================
//
// hnsw_rs 0.3.4 `reverse_update_neighborhood_simple` wrote the back-link of a
// new point `P` (top level `L`) into `q.neighbours[L]` for EVERY layer `l <= L`
// it walks, instead of `q.neighbours[l]`. A level >= 1 point therefore got no
// layer-0 back-links, whole regions became unreachable for layer-0 search and
// recall collapsed in roughly one build in a few hundred (0.844 vs 0.956-0.985
// in CI). Fixed upstream in 0.3.5 (commit 6256b00b11); this test pins the fix.

/// The point stored at `pid` (layer, rank-in-layer).
fn point_at(hnsw: &Hnsw<'static, f32, ShamirDist>, pid: PointId) -> Arc<Point<'static, f32>> {
    hnsw.get_point_indexation()
        .get_layer_iterator(pid.0 as usize)
        .nth(pid.1 as usize)
        .expect("point id must resolve")
}

/// The point inserted with client id `origin`.
fn point_by_origin(
    hnsw: &Hnsw<'static, f32, ShamirDist>,
    origin: usize,
) -> Arc<Point<'static, f32>> {
    (0..16)
        .flat_map(|layer| hnsw.get_point_indexation().get_layer_iterator(layer))
        .find(|p| p.get_origin_id() == origin)
        .expect("inserted point must be indexed")
}

#[test]
fn reverse_link_of_upper_level_point_lands_on_layer_0() {
    const M: usize = 16;
    // At most 2*M layer-0 neighbours per node (the shrink threshold), so no
    // later insert can evict a back-link: any missing link is the bug.
    const N: usize = 2 * M + 1;
    const WANTED: usize = 10;

    let mut checked = 0;
    for graph in 0..400u64 {
        let hnsw: Hnsw<'static, f32, ShamirDist> = Hnsw::new(
            M,
            N,
            16,
            64,
            ShamirDist {
                metric: VectorMetric::L2,
            },
        );
        for i in 0..N {
            let v = lcg_vec(8, graph * 1000 + i as u64 + 1);
            hnsw.insert((&v, i)); // sequential: no rayon, fully deterministic given the level draw
            let p = point_by_origin(&hnsw, i);
            let pid = p.get_point_id();
            // P's own layer-0 choices, read before any later insert can touch them.
            let own_layer0 = p.get_neighborhood_id().swap_remove(0);
            if pid.0 == 0 || own_layer0.is_empty() {
                continue;
            }
            for q in &own_layer0 {
                let back = point_at(&hnsw, q.p_id).get_neighborhood_id().swap_remove(0);
                assert!(
                    back.iter().any(|n| n.p_id == pid),
                    "level-{} point {i} chose {} as a layer-0 neighbour but got no layer-0 back-link",
                    pid.0,
                    q.d_id,
                );
            }
            checked += 1;
            if checked >= WANTED {
                return;
            }
        }
    }
    panic!("400 graphs produced only {checked} upper-level points with layer-0 neighbours");
}

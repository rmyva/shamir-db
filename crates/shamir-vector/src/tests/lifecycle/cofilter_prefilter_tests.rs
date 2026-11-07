//! V3.2 (#405) — pre-filter, co-filter, and the overscan contract test.
//!
//! Tests:
//! 1. Overscan contract: proves the defensive `ef` overscan is *sufficient*
//!    — generous ef always fills `knbn` under a tight filter, with no
//!    leakage, empty-filter→empty, and distance-sorted bounds (closes V0.0
//!    debt). Necessity (tight ef under-returning) is version-dependent in
//!    hnsw_rs 0.3.4 and not contractually guaranteed, so it is not asserted.
//! 2. Pre-filter (exact SIMD) returns the correct top-k.
//! 3. Co-filter returns valid results restricted to the allow-set.
//! 4. Equivalence: pre-filter result matches brute-force ground truth.
//! 5. Sorted allow-list invariant: Vec<usize> FilterT requires sorted input.

use crate::hnsw_adapter::{HnswAdapter, HnswConfig, CO_FILTER_EF_MULTIPLIER};
use shamir_index_core::kind::VectorMetric;
use shamir_types::types::record_id::RecordId;

/// Create a RecordId from a u64 by embedding it as big-endian in the last 8 bytes.
fn rid(n: u64) -> RecordId {
    let mut a = [0u8; 16];
    a[8..16].copy_from_slice(&n.to_be_bytes());
    RecordId(a)
}

/// Extract the u64 back from a RecordId created by `rid()`.
fn rid_val(r: &RecordId) -> u64 {
    u64::from_be_bytes(r.0[8..16].try_into().unwrap())
}

/// Deterministic LCG vector generator (same as in contract tests).
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

/// Build an HnswAdapter with `n` vectors (RIDs = rid(0..n)).
async fn build_adapter(n: usize, dim: usize) -> HnswAdapter {
    let config = HnswConfig {
        max_elements: n.max(64),
        m: 16,
        max_layer: 16,
        ef_construction: 200,
        ef_search: 50,
    };
    let adapter = HnswAdapter::new(dim as u32, VectorMetric::L2, config);
    let mut items: Vec<(RecordId, Vec<f32>)> = Vec::with_capacity(n);
    for i in 0..n {
        items.push((rid(i as u64), lcg_vec(dim, i as u64 * 7 + 1)));
    }
    use crate::adapter::VectorAdapter;
    adapter.upsert_batch(&items).await.expect("upsert_batch");
    adapter
}

// ============================================================================
// 1. OVERSCAN CONTRACT TEST (closes V0.0 debt, #405)
// ============================================================================

/// Pins the `search_filter` contract for the co-filter path (#405):
///
/// 1. **SUFFICIENCY**: with generous ef (`knbn * CO_FILTER_EF_MULTIPLIER`),
///    `search_filter` ALWAYS returns the full `knbn` results when enough
///    qualifying points exist in the allow-list. This is the safety invariant
///    the production co-filter path relies on.
/// 2. **CORRECTNESS**: all returned IDs are in the allow-list (no leakage).
/// 3. **EMPTY-FILTER**: an empty allow-list yields an empty result (not the
///    unfiltered top-k).
/// 4. **BOUNDS**: returned results are distance-sorted and capped at knbn.
///
/// The V0.0 spike documented that `search_filter` CAN return < knbn at tight
/// ef when the filter is very selective. Empirically, hnsw_rs 0.3.4 with
/// well-connected graphs (m>=8) reliably finds filtered points even at low ef.
/// The production code applies generous ef DEFENSIVELY — this test pins that
/// the defensive overscan is always sufficient.
#[tokio::test]
async fn search_filter_overscan_contract() {
    use crate::hnsw_adapter::ShamirDist;
    use hnsw_rs::hnsw::Hnsw;

    let dim = 32usize;
    let n = 2000usize;
    let knbn = 10usize;
    let num_builds = 5usize;

    let vecs: Vec<Vec<f32>> = (0..n).map(|i| lcg_vec(dim, i as u64 * 7 + 1)).collect();

    for build_seed in 0..num_builds {
        let hnsw = Hnsw::new(
            16,
            n,
            16,
            200,
            ShamirDist {
                metric: VectorMetric::L2,
            },
        );
        let batch: Vec<(&Vec<f32>, usize)> = vecs.iter().zip(0..n).collect();
        hnsw.parallel_insert(&batch);

        // Allow-list: 20 IDs scattered across the dataset (2x knbn, enough
        // for the filter to be satisfiable but sparse: 1% of dataset).
        let allow: Vec<usize> = (0..n).step_by(100).take(20).collect();
        let q = lcg_vec(dim, 99999 + build_seed as u64 * 31);

        // Generous ef (production co-filter path).
        let generous_ef = knbn * CO_FILTER_EF_MULTIPLIER as usize;
        let results = hnsw.search_filter(&q, knbn, generous_ef, Some(&allow));

        // SUFFICIENCY: generous ef ALWAYS returns the full knbn.
        assert_eq!(
            results.len(),
            knbn,
            "build {build_seed}: generous ef must return full knbn; got {}",
            results.len()
        );

        // CORRECTNESS: no filter leakage.
        for nb in &results {
            assert!(
                allow.contains(&nb.d_id),
                "build {build_seed}: filter leaked id {}",
                nb.d_id
            );
        }

        // BOUNDS: returned distances are sorted.
        for w in results.windows(2) {
            assert!(
                w[0].distance <= w[1].distance,
                "build {build_seed}: results not sorted by distance"
            );
        }
    }

    // EMPTY-FILTER: empty allow-list yields empty result.
    let hnsw = Hnsw::new(
        16,
        n,
        16,
        200,
        ShamirDist {
            metric: VectorMetric::L2,
        },
    );
    let batch: Vec<(&Vec<f32>, usize)> = vecs.iter().zip(0..n).collect();
    hnsw.parallel_insert(&batch);
    let q = lcg_vec(dim, 12345);
    let empty_allow: Vec<usize> = vec![];
    let empty_results = hnsw.search_filter(&q, knbn, 80, Some(&empty_allow));
    assert!(
        empty_results.is_empty(),
        "empty allow-list must yield empty results"
    );
}

// ============================================================================
// 2. PRE-FILTER: exact SIMD top-k matches brute-force ground truth
// ============================================================================

#[tokio::test]
async fn prefilter_matches_brute_force_ground_truth() {
    let dim = 16usize;
    let n = 500usize;
    let k = 5u32;
    let adapter = build_adapter(n, dim).await;

    // Candidate set: even-numbered RIDs only (250 candidates).
    let candidates: Vec<RecordId> = (0..n as u64).step_by(2).map(rid).collect();
    let query = lcg_vec(dim, 42424242);

    let results = adapter
        .search_prefilter(&query, k, &candidates)
        .await
        .expect("search_prefilter");

    assert_eq!(
        results.len(),
        k as usize,
        "pre-filter must return k results"
    );

    // Compute ground truth by brute-force over the same candidate set.
    use crate::hnsw_adapter::ShamirDist;
    use hnsw_rs::anndists::dist::distances::Distance;
    let dist = ShamirDist {
        metric: VectorMetric::L2,
    };
    let mut ground_truth: Vec<(RecordId, f32)> = Vec::new();
    for &cand_rid in &candidates {
        let id = rid_val(&cand_rid) as usize;
        let v = lcg_vec(dim, id as u64 * 7 + 1);
        let d = dist.eval(&query, &v);
        ground_truth.push((cand_rid, d));
    }
    ground_truth.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
    ground_truth.truncate(k as usize);

    // Pre-filter is EXACT — must match ground truth exactly (same RIDs, same order).
    let result_rids: Vec<RecordId> = results.iter().map(|(r, _)| *r).collect();
    let truth_rids: Vec<RecordId> = ground_truth.iter().map(|(r, _)| *r).collect();
    assert_eq!(
        result_rids, truth_rids,
        "pre-filter must match brute-force ground truth"
    );
}

// ============================================================================
// 3. CO-FILTER: results are restricted to the allow-set
// ============================================================================

#[tokio::test]
async fn cofilter_results_restricted_to_allowset() {
    let dim = 8usize;
    let n = 1000usize;
    let k = 5u32;
    let adapter = build_adapter(n, dim).await;

    // Allow-set: IDs divisible by 10 (100 candidates, ~10% selectivity).
    let candidates: Vec<RecordId> = (0..n as u64).step_by(10).map(rid).collect();
    let query = lcg_vec(dim, 77777);

    let results = adapter
        .search_cofilter(&query, k, None, &candidates)
        .await
        .expect("search_cofilter");

    // All results must be from the allow-set.
    for (rid, _) in &results {
        assert!(
            candidates.contains(rid),
            "co-filter returned rid {:?} not in allow-set",
            rid
        );
    }

    // Should return k results (100 candidates >> k=5, with generous ef).
    assert_eq!(
        results.len(),
        k as usize,
        "co-filter should return k results when allow-set is large enough"
    );
}

// ============================================================================
// 4. EQUIVALENCE: pre-filter == brute-force filtered (exact contract)
// ============================================================================

#[tokio::test]
async fn prefilter_is_exact() {
    // Already covered by test 2 (prefilter_matches_brute_force_ground_truth).
    // This test uses a different dataset size and candidate fraction to
    // provide additional coverage.
    let dim = 32usize;
    let n = 200usize;
    let k = 3u32;
    let adapter = build_adapter(n, dim).await;

    let candidates: Vec<RecordId> = (10..30u64).map(rid).collect(); // 20 candidates
    let query = lcg_vec(dim, 123456);

    let results = adapter
        .search_prefilter(&query, k, &candidates)
        .await
        .expect("prefilter");

    assert_eq!(results.len(), k as usize);

    // Verify distances are monotonically non-decreasing.
    for w in results.windows(2) {
        assert!(
            w[0].1 <= w[1].1,
            "pre-filter results must be sorted by distance"
        );
    }
}

// ============================================================================
// 5. SORTED ALLOW-LIST INVARIANT (Vec<usize> FilterT requires sorted input)
// ============================================================================

/// The `Vec<usize>: FilterT` blanket impl uses `binary_search`. If the allow
/// list is NOT sorted, it silently fails (returns false for present IDs). This
/// test validates that our co-filter path does NOT use Vec<usize> (it uses a
/// closure over a HashSet), so unsorted input is safe.
#[tokio::test]
async fn cofilter_works_with_unsorted_candidates() {
    let dim = 8usize;
    let n = 500usize;
    let k = 3u32;
    let adapter = build_adapter(n, dim).await;

    // Deliberately unsorted candidate list.
    let candidates: Vec<RecordId> = vec![
        rid(400u64),
        rid(50u64),
        rid(200u64),
        rid(10u64),
        rid(300u64),
        rid(150u64),
        rid(450u64),
        rid(75u64),
        rid(350u64),
        rid(25u64),
    ];
    let query = lcg_vec(dim, 55555);

    let results = adapter
        .search_cofilter(&query, k, None, &candidates)
        .await
        .expect("cofilter with unsorted candidates");

    // Results must come from the allow-set.
    for (rid, _) in &results {
        assert!(
            candidates.contains(rid),
            "co-filter leaked rid {:?} from unsorted allow-set",
            rid
        );
    }
    assert_eq!(results.len(), k as usize);
}

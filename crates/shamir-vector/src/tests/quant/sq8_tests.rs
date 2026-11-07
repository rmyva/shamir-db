//! Tests for the SQ8 scalar quantizer ([`Sq8Quantizer`]).
//!
//! Coverage:
//!  * `fit` correctness — per-dim min/max and scale agree with the data;
//!  * round-trip — `dequantize(quantize(v))` within half a quantization
//!    step (`scale_i/2 + eps`) componentwise;
//!  * `approx_dot` vs the true f32 dot — small relative error on
//!    quantized codes;
//!  * recall ≤ 2 % drop — top-k by f32 dot vs top-k by `approx_dot` on a
//!    2 k-point clustered dataset, `recall@k ≥ 0.98`;
//!  * edge cases — dim-mismatch panics, constant dimension (`scale == 0`)
//!    doesn't divide by zero.
//!
//! The clustered dataset generator is an intentional self-contained mirror of
//! `shamir_bench_utils::vector_data` (Numerical-Recipes LCG + Box-Muller), so
//! test binaries need no fixture crate at runtime; the lineage test below
//! keeps the mirror in check.

use crate::quant_meta::QuantMeta;
use crate::sq8::Sq8Quantizer;
use shamir_bench_utils::vector_data::{Lcg as BenchLcg, LCG_INC, LCG_MULT};

/// `from_quantizer` ↔ `to_quantizer` round-trips the params exactly.
#[test]
fn quant_meta_round_trips_sq8_params() {
    let training: Vec<Vec<f32>> = (0..50)
        .map(|i| {
            (0..8)
                .map(|j| (i as f32 * 0.1) + (j as f32 * 0.01))
                .collect()
        })
        .collect();
    let q = Sq8Quantizer::fit(&training, 8);
    let meta = QuantMeta::from_quantizer(&q);
    assert_eq!(meta.method, "sq8");
    assert_eq!(meta.dim, 8);
    let q2 = meta.to_quantizer();
    assert_eq!(q2.dim(), q.dim());
    assert_eq!(q2.mins(), q.mins());
    assert_eq!(q2.scales(), q.scales());
    let v = training[0].clone();
    assert_eq!(q.quantize(&v), q2.quantize(&v));
}

// ----- deterministic RNG (mirrors shamir_bench_utils::Lcg lineage) --------

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
        (self.next_u64() >> 40) as f32 * (1.0 / 16_777_216.0)
    }
    #[inline]
    fn next_range(&mut self, lo: f32, hi: f32) -> f32 {
        if !lo.is_finite() || !hi.is_finite() || lo >= hi {
            return f32::NAN;
        }
        let fraction = f64::from(self.next_f32());
        let value = (f64::from(lo) + (f64::from(hi) - f64::from(lo)) * fraction) as f32;
        value.max(lo).min(hi.next_down())
    }
    /// Standard-normal via polar Box-Muller (same shape as bench-utils).
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

/// Clustered dataset generator: `n` points of `dim` around `k_clusters`
/// centroids in `[-1, 1]^dim` with per-dim Gaussian noise of std `sigma`.
/// Mirrors `shamir_bench_utils::clustered_vectors` for the valid test inputs below.
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

// =========================================================================
// fit correctness
// =========================================================================

#[test]
fn fit_min_max_scale_correct() {
    let dim = 4;
    let train = vec![
        vec![0.0, 10.0, -5.0, 100.0],
        vec![10.0, 20.0, 5.0, 100.0],
        vec![5.0, 15.0, 0.0, 100.0],
    ];
    let q = Sq8Quantizer::fit(&train, dim);
    assert_eq!(q.dim(), dim);
    let mins = q.mins();
    let scales = q.scales();
    // per-dim minima
    assert_eq!(mins[0], 0.0);
    assert_eq!(mins[1], 10.0);
    assert_eq!(mins[2], -5.0);
    assert_eq!(mins[3], 100.0);
    // scales = (max-min)/255
    assert_eq!(scales[0], 10.0 / 255.0);
    assert_eq!(scales[1], 10.0 / 255.0);
    assert_eq!(scales[2], 10.0 / 255.0);
    // constant dimension → scale 0
    assert_eq!(scales[3], 0.0);
}

// =========================================================================
// round-trip: dequantize(quantize(v)) within half a step
// =========================================================================

#[test]
fn round_trip_within_half_step() {
    let dim = 64;
    let train = clustered(256, dim, 16, 0.15, 42);
    let q = Sq8Quantizer::fit(&train, dim);

    let eps = 1e-5;
    for (idx, v) in train.iter().enumerate().take(50) {
        let codes = q.quantize(v);
        let decoded = q.dequantize(&codes);
        assert_eq!(decoded.len(), dim);
        for i in 0..dim {
            let half_step = q.scales()[i] * 0.5;
            // On a constant dim (scale 0), decode == min exactly.
            let err = (decoded[i] - v[i]).abs();
            assert!(
                err <= half_step + eps,
                "vec {idx} dim {i}: round-trip error {err} > half-step {half_step}"
            );
        }
    }
}

#[test]
fn round_trip_extreme_values() {
    // A vector hitting the exact min and max of the training range maps
    // to codes 0 and 255 respectively, decode error is ~0 there.
    let dim = 3;
    let train = vec![vec![0.0, 0.0, 0.0], vec![10.0, 10.0, 10.0]];
    let q = Sq8Quantizer::fit(&train, dim);
    let v = vec![0.0, 5.0, 10.0];
    let codes = q.quantize(&v);
    assert_eq!(codes[0], 0);
    assert_eq!(codes[2], 255);
    let decoded = q.dequantize(&codes);
    let eps = 1e-5;
    assert!((decoded[0] - 0.0).abs() < eps);
    assert!((decoded[2] - 10.0).abs() < q.scales()[2] / 2.0 + eps);
}

// =========================================================================
// approx_dot vs true f32 dot
// =========================================================================

#[test]
fn approx_dot_close_to_f32_dot() {
    let dim = 128;
    let train = clustered(512, dim, 32, 0.2, 7);
    let q = Sq8Quantizer::fit(&train, dim);

    // Collect relative errors; report the MEDIAN (robust to the few pairs
    // whose true dot is near zero, where relative error blows up).
    let mut rels = Vec::new();
    for i in 0..80 {
        let x = &train[i];
        let y = &train[i + 80];
        let qx = q.quantize(x);
        let qy = q.quantize(y);
        let approx = q.approx_dot(&qx, &qy);
        // Reference: true f32 dot of the ORIGINAL (unquantized) vectors.
        let truth: f32 = x.iter().zip(y.iter()).map(|(a, b)| a * b).sum();
        // Skip near-zero true dots (relative error is meaningless there).
        if truth.abs() > 1.0 {
            rels.push((approx - truth).abs() / truth.abs());
        }
    }
    rels.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let median = rels[rels.len() / 2];
    // SQ8 approx-dot of clustered [-1,1]+noise data: the median relative
    // error stays small. The per-pair MAX can be large when the true dot
    // is near zero, but the ranking (what matters for search) is preserved
    // — see `recall_topk_within_two_percent_drop` for the real quality gate.
    assert!(
        median < 0.10,
        "approx_dot median relative error {median} too large"
    );
}

#[test]
fn approx_dot_matches_term_by_term_expansion() {
    // Independently recompute the documented expansion and compare.
    let dim = 32;
    let train = clustered(64, dim, 8, 0.3, 99);
    let q = Sq8Quantizer::fit(&train, dim);
    let x = &train[0];
    let y = &train[1];
    let qx = q.quantize(x);
    let qy = q.quantize(y);

    let mut manual = 0.0f32;
    for i in 0..dim {
        let m = q.mins()[i];
        let s = q.scales()[i];
        let ax = m + qx[i] as f32 * s;
        let ay = m + qy[i] as f32 * s;
        manual += ax * ay;
    }
    let approx = q.approx_dot(&qx, &qy);
    // The two sums are mathematically identical but accumulate in a
    // different fp order, so compare with a tolerance rather than
    // bit-equality. A PURE relative tolerance breaks down whenever the
    // reference value is near zero (cross-platform SIMD/FMA rounding noise
    // becomes a large fraction of a tiny denominator, even though the
    // absolute difference stays negligible) — combine relative AND
    // absolute bounds (the standard `|a-b| <= atol + rtol*|ref|` pattern),
    // so a genuinely tiny reference doesn't demand near-bit-exact
    // agreement. The magnitude here is ≈ 0.026, so 1e-3 relative
    // (≈ 3e-5 absolute) is a generous but meaningful bound; the 1e-5
    // absolute floor only matters for near-zero references.
    let diff = (approx - manual).abs();
    let tol = 1e-3 * manual.abs() + 1e-5;
    assert!(
        diff <= tol,
        "approx_dot {approx} != term-by-term {manual} (diff {diff}, tol {tol})"
    );
}

#[test]
fn approx_dot_matches_pre_refactor_scalar_loop_across_fit_configs() {
    // Task #614: `approx_dot`'s linear+bilinear terms now go through the
    // SIMD-dispatched `weighted_bilinear_f32` kernel instead of the
    // original scalar loop. Pin several distinct fit configurations
    // (dims spanning multiple SIMD chunk widths + tails, several seeds)
    // and confirm `approx_dot` still agrees with the ORIGINAL scalar-loop
    // formula computed independently here, within the same tolerance used
    // elsewhere in this file.
    for &dim in &[1usize, 7, 8, 16, 31, 32, 64, 100, 128] {
        for seed in [1u64, 17, 257] {
            let train = clustered(48, dim, 4, 0.4, seed);
            let q = Sq8Quantizer::fit(&train, dim);
            let x = &train[0];
            let y = &train[train.len() - 1];
            let qx = q.quantize(x);
            let qy = q.quantize(y);

            // Original pre-refactor scalar loop (sq8.rs:252-257 before
            // #614), recomputed independently from public accessors.
            let mins = q.mins();
            let scales = q.scales();
            let mut min_sq_sum = 0.0f32;
            let mut pre_refactor = 0.0f32;
            for i in 0..dim {
                min_sq_sum += mins[i] * mins[i];
                let min_scale_i = mins[i] * scales[i];
                let scales_sq_i = scales[i] * scales[i];
                let qx_i = qx[i] as f32;
                let qy_i = qy[i] as f32;
                pre_refactor += min_scale_i * (qx_i + qy_i) + scales_sq_i * qx_i * qy_i;
            }
            pre_refactor += min_sq_sum;

            let post_refactor = q.approx_dot(&qx, &qy);
            // Combined relative+absolute tolerance (see
            // `approx_dot_matches_term_by_term_expansion`'s comment above):
            // a pure relative bound is unreliable when `pre_refactor` is
            // near zero — exactly what was observed in CI (dim=8 seed=17
            // on a non-x86 SIMD backend: reference ≈ 1.35e-4, absolute
            // diff ≈ 1.9e-6, but 1.4% relative — comfortably past a 0.1%
            // pure-relative bound despite the two values agreeing to 5
            // significant figures).
            let diff = (post_refactor - pre_refactor).abs();
            let tol = 1e-3 * pre_refactor.abs() + 1e-5;
            assert!(
                diff <= tol,
                "dim={dim} seed={seed}: approx_dot (post-refactor) {post_refactor} != \
                 pre-refactor scalar loop {pre_refactor} (diff {diff}, tol {tol})"
            );
        }
    }
}

// =========================================================================
// recall ≤ 2% drop (DoD): recall@k ≥ 0.98
// =========================================================================

fn recall_at_k<S, T>(truth: &[S], cand: &[T]) -> f32
where
    S: PartialEq,
    T: PartialEq<S>,
{
    let hits = cand
        .iter()
        .filter(|c| truth.iter().any(|t| **c == *t))
        .count();
    hits as f32 / cand.len().max(1) as f32
}

#[test]
fn recall_topk_within_two_percent_drop() {
    let dim = 128usize;
    let k = 10usize;
    let data = clustered(2000, dim, 50, 0.25, 0xC0FFEE);
    let q = Sq8Quantizer::fit(&data, dim);

    // Quantize once.
    let codes: Vec<Vec<u8>> = data.iter().map(|v| q.quantize(v)).collect();

    let n_queries = 100usize;
    let mut total_recall = 0.0f32;
    for qi in 0..n_queries {
        let query = &data[qi];

        // Ground-truth top-k by true f32 dot over the WHOLE set.
        let mut truth_scores: Vec<(usize, f32)> = (0..data.len())
            .map(|i| {
                let s: f32 = query.iter().zip(data[i].iter()).map(|(a, b)| a * b).sum();
                (i, s)
            })
            .collect();
        truth_scores.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        let truth_ids: Vec<usize> = truth_scores.iter().take(k).map(|(i, _)| *i).collect();

        // Approx top-k by approx_dot over the quantized codes.
        let q_codes = q.quantize(query);
        let mut approx_scores: Vec<(usize, f32)> = (0..codes.len())
            .map(|i| (i, q.approx_dot(&q_codes, &codes[i])))
            .collect();
        approx_scores.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        let approx_ids: Vec<usize> = approx_scores.iter().take(k).map(|(i, _)| *i).collect();

        total_recall += recall_at_k(&truth_ids, &approx_ids);
    }
    let avg_recall = total_recall / n_queries as f32;
    // DoD: recall@k ≥ 0.98 (≤ 2% drop).
    assert!(
        avg_recall >= 0.98,
        "recall@{k} = {avg_recall:.4} below 0.98 threshold"
    );
}

// =========================================================================
// edge cases
// =========================================================================

#[test]
#[should_panic(expected = "len")]
fn quantize_dim_mismatch_panics() {
    let dim = 4;
    let train = clustered(8, dim, 2, 0.1, 1);
    let q = Sq8Quantizer::fit(&train, dim);
    let _ = q.quantize(&[1.0, 2.0, 3.0]);
}

#[test]
#[should_panic(expected = "len")]
fn dequantize_dim_mismatch_panics() {
    let dim = 4;
    let train = clustered(8, dim, 2, 0.1, 1);
    let q = Sq8Quantizer::fit(&train, dim);
    let _ = q.dequantize(&[1u8, 2, 3]);
}

#[test]
#[should_panic(expected = "len")]
fn approx_dot_dim_mismatch_panics() {
    let dim = 4;
    let train = clustered(8, dim, 2, 0.1, 1);
    let q = Sq8Quantizer::fit(&train, dim);
    let _ = q.approx_dot(&[0u8, 1, 2], &[3u8, 4, 5]);
}

#[test]
#[should_panic(expected = "empty")]
fn fit_empty_panics() {
    let _ = Sq8Quantizer::fit(&[], 4);
}

#[test]
#[should_panic(expected = "dim")]
fn fit_zero_dim_panics() {
    let _ = Sq8Quantizer::fit(&[vec![]], 0);
}

#[test]
fn constant_dimension_no_divide_by_zero() {
    // A dimension where every training value is identical → scale 0.
    // quantize must yield code 0, dequantize must yield the constant.
    let dim = 3;
    let train = vec![
        vec![1.0, 5.0, 9.0],
        vec![2.0, 5.0, 9.0],
        vec![3.0, 5.0, 9.0],
    ];
    let q = Sq8Quantizer::fit(&train, dim);
    assert_eq!(q.scales()[1], 0.0);
    assert_eq!(q.scales()[2], 0.0);

    let v = vec![99.0, 5.0, 9.0];
    let codes = q.quantize(&v);
    assert_eq!(codes[1], 0, "constant dim → code 0");
    assert_eq!(codes[2], 0, "constant dim → code 0");
    let decoded = q.dequantize(&codes);
    assert_eq!(decoded[1], 5.0, "constant dim dequantizes to the constant");
    assert_eq!(decoded[2], 9.0);
}

#[test]
fn quantize_clamps_out_of_range() {
    // A value below the training min saturates to 0; above the max to 255.
    let dim = 1;
    let train = vec![vec![0.0f32], vec![10.0f32]];
    let q = Sq8Quantizer::fit(&train, dim);
    assert_eq!(q.quantize(&[-5.0])[0], 0);
    assert_eq!(q.quantize(&[15.0])[0], 255);
}

// =========================================================================
// dequant_norm_sq (F4): the canonical SIMD-backed method must match the OLD
// scalar formula `Σ (mins[i] + q[i]*scales[i])²`, computed independently
// here from the public accessors (not by calling the code that was deleted).
// This proves the key identity: `min_sq_sum + weighted_bilinear_f32(
// min_scale, scales_sq, q, q) == Σ (min_i + q_i·s_i)²`.
// =========================================================================

#[test]
fn dequant_norm_sq_matches_scalar_reference() {
    // Sweep dims spanning multiple SIMD chunk widths (8-lane AVX2, 4-lane
    // NEON) plus tails, and several seeds, to catch any off-by-one in the
    // tail loop or a widening error.
    for &dim in &[1usize, 4, 7, 8, 9, 15, 16, 17, 31, 32, 33, 64, 100, 128] {
        for seed in [1u64, 17, 257] {
            let train = clustered(48, dim, 4, 0.4, seed);
            let q = Sq8Quantizer::fit(&train, dim);
            let codes = q.quantize(&train[0]);

            // Fresh scalar reference: Σ (mins[i] + codes[i]*scales[i])².
            let mut ref_val = 0.0f32;
            // Indexes three parallel slices (mins, scales, codes) by i.
            #[allow(clippy::needless_range_loop)]
            for i in 0..dim {
                let x = q.mins()[i] + (codes[i] as f32) * q.scales()[i];
                ref_val += x * x;
            }

            let got = q.dequant_norm_sq(&codes);
            // Combined relative+absolute tolerance: the canonical method
            // goes through `weighted_bilinear_f32` (SIMD FMA) which differs
            // from the scalar accumulation order by at most a few ulp. A
            // pure relative bound breaks down when `ref_val` is near zero
            // (a constant-dimension quantizer), so include an absolute floor.
            let diff = (got - ref_val).abs();
            let tol = 1e-3 * ref_val.abs() + 1e-5;
            assert!(
                diff <= tol,
                "dequant_norm_sq dim={dim} seed={seed}: got {got} != scalar reference \
                 {ref_val} (diff {diff}, tol {tol})"
            );
        }
    }
}

#[test]
fn dequant_norm_sq_all_255() {
    // Saturation-adjacent: every code at the u8 max. Exercises the SIMD
    // widening at the largest per-lane code value.
    for &dim in &[8usize, 16, 32, 100, 128] {
        let train = clustered(32, dim, 8, 0.5, 999);
        let q = Sq8Quantizer::fit(&train, dim);
        let codes = vec![255u8; dim];

        let mut ref_val = 0.0f32;
        // Indexes three parallel slices (mins, scales, codes) by i.
        #[allow(clippy::needless_range_loop)]
        for i in 0..dim {
            let x = q.mins()[i] + (codes[i] as f32) * q.scales()[i];
            ref_val += x * x;
        }

        let got = q.dequant_norm_sq(&codes);
        let diff = (got - ref_val).abs();
        let tol = 1e-3 * ref_val.abs() + 1e-5;
        assert!(
            diff <= tol,
            "dequant_norm_sq all-255 dim={dim}: got {got} != ref {ref_val} (diff {diff}, tol {tol})"
        );
    }
}

#[test]
#[should_panic(expected = "len")]
fn dequant_norm_sq_dim_mismatch_panics() {
    let dim = 4;
    let train = clustered(8, dim, 2, 0.1, 1);
    let q = Sq8Quantizer::fit(&train, dim);
    let _ = q.dequant_norm_sq(&[1u8, 2, 3]);
}

// =========================================================================
// LCG lineage vs the canonical shamir_bench_utils generator
// =========================================================================

/// The local LCG must stay in the same lineage as
/// `shamir_bench_utils::vector_data::Lcg` (same constants, same stream,
/// same Box-Muller output). Without this test the lineage contract was
/// prose-only and a drifted constant would pass silently.
#[test]
fn rv_bench_utils_a7605ff1_lcg_lineage_matches_bench_utils() {
    assert_eq!(6364136223846793005, LCG_MULT);
    assert_eq!(1442695040888963407, LCG_INC);
    for seed in [0u64, 1, 42, 0xDEAD_BEEF, u64::MAX] {
        let mut local = Lcg::new(seed);
        let mut canon = BenchLcg::new(seed);
        for _ in 0..64 {
            assert_eq!(
                local.next_u64(),
                canon.next_u64(),
                "seed {seed}: u64 stream drifted"
            );
        }
        for _ in 0..64 {
            assert_eq!(
                local.next_f32(),
                canon.next_f32(),
                "seed {seed}: uniform drifted"
            );
        }
        for (lo, hi) in [
            (-1.0, 1.0),
            (1.0, f32::from_bits(0x3f80_0001)),
            (-f32::MAX, f32::MAX),
        ] {
            assert_eq!(
                local.next_range(lo, hi),
                canon.next_range(lo, hi),
                "seed {seed}: range drifted"
            );
        }
        assert!(local.next_range(1.0, 1.0).is_nan());
        assert!(canon.next_range(1.0, 1.0).is_nan());
        for _ in 0..64 {
            assert_eq!(
                local.next_gaussian(),
                canon.next_gaussian(),
                "seed {seed}: gaussian stream drifted"
            );
        }
    }
}

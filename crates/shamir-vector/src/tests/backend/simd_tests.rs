//! Bit-exact equality tests for the integer u8 dot-product kernel
//! (`dot_u8`) against the scalar reference (`dot_u8_scalar`).
//!
//! DoD requirement: the SIMD dispatch path MUST equal the scalar
//! reference BIT-FOR-BIT (integer `==`, no float tolerance) on arbitrary
//! u8 inputs INCLUDING the saturation edge (all-255, dim ≥ 32, where a
//! naïve `maddubs`-based kernel would clip and diverge).
//!
//! On x86_64 dev/CI hosts the dispatcher selects the AVX2 kernel, so this
//! suite exercises that kernel end-to-end. The NEON `udot`/widening paths
//! are type-checked via `cargo check --target aarch64-*` and mirror the
//! proven AVX2/scalar structure (see the block comment in `simd.rs`).

use crate::simd::{
    dot_u8, dot_u8_scalar, weighted_bilinear_f32, weighted_bilinear_scalar, weighted_linear_scalar,
    weighted_linear_u8, weighted_sq_diff_scalar, weighted_sq_diff_u8,
};

/// Numerical-Recipes LCG (same lineage as `shamir_bench_utils::Lcg` and
/// `hnsw_rs_contract_tests::lcg_vec`) — deterministic, no global RNG.
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
    fn next_u8(&mut self) -> u8 {
        (self.next_u64() >> 33) as u8
    }
}

fn rand_vec(dim: usize, seed: u64) -> Vec<u8> {
    let mut rng = Lcg::new(seed);
    (0..dim).map(|_| rng.next_u8()).collect()
}

const DIMS: &[usize] = &[1, 7, 8, 15, 16, 31, 32, 64, 127, 128];

#[test]
fn dispatcher_equals_scalar_random() {
    for &dim in DIMS {
        let a = rand_vec(dim, dim as u64 * 7 + 1);
        let b = rand_vec(dim, dim as u64 * 31 + 11);
        let want = dot_u8_scalar(&a, &b);
        let got = dot_u8(&a, &b);
        assert_eq!(
            got, want,
            "dispatcher != scalar on random dim={dim}: got {got}, want {want}"
        );
    }
}

#[test]
fn dispatcher_equals_scalar_all_255_saturation_edge() {
    // The critical saturation test: every product is 255*255 = 65025.
    // For dim=128 the sum is 8 323 200 — well within u32, but a naïve
    // `maddubs_epi16` kernel saturates EACH i16 pair to 32 767 and would
    // report a wildly different number. This is the guard against that.
    for &dim in &[32, 64, 127, 128, 256] {
        let a = vec![255u8; dim];
        let b = vec![255u8; dim];
        let want = dot_u8_scalar(&a, &b);
        let got = dot_u8(&a, &b);
        assert_eq!(
            got,
            want,
            "all-255 saturation dim={dim}: got {got}, want {want} (expected {})",
            65025 * dim
        );
        assert_eq!(want, (65025 * dim) as u32, "scalar sanity for dim={dim}");
    }
}

#[test]
fn dispatcher_equals_scalar_all_zero() {
    for &dim in DIMS {
        let a = vec![0u8; dim];
        let b = vec![0u8; dim];
        assert_eq!(dot_u8(&a, &b), 0, "all-zero dim={dim}");
        assert_eq!(dot_u8_scalar(&a, &b), 0, "all-zero scalar dim={dim}");
    }
}

#[test]
fn dispatcher_equals_scalar_mixed_high_low() {
    // Mix of >127 (which `maddubs` would reinterpret as negative i8) and
    // low codes — catches the signedness trap on the second operand.
    for &dim in &[32, 64, 128] {
        let mut a = vec![0u8; dim];
        let mut b = vec![0u8; dim];
        for i in 0..dim {
            a[i] = if i % 2 == 0 { 255 } else { 200 };
            b[i] = if i % 3 == 0 { 255 } else { 128 };
        }
        let want = dot_u8_scalar(&a, &b);
        let got = dot_u8(&a, &b);
        assert_eq!(
            got, want,
            "mixed high/low dim={dim}: got {got}, want {want}"
        );
    }
}

#[test]
fn dispatcher_equals_scalar_self_dot() {
    // x · x with x all-255: every term 65025, sum grows with dim. Another
    // saturation-sensitive configuration.
    for &dim in &[33, 64, 100, 128] {
        let a = vec![255u8; dim];
        let want = dot_u8_scalar(&a, &a);
        let got = dot_u8(&a, &a);
        assert_eq!(got, want, "self-dot all-255 dim={dim}");
    }
}

#[test]
fn dispatcher_equals_scalar_many_seeds() {
    // Statistical sweep: 200 random pairs at dim=128.
    for s in 0..200u64 {
        let a = rand_vec(128, s.wrapping_mul(2654435761).wrapping_add(1));
        let b = rand_vec(128, s.wrapping_mul(40503).wrapping_add(7));
        let want = dot_u8_scalar(&a, &b);
        let got = dot_u8(&a, &b);
        assert_eq!(got, want, "seed sweep s={s}: got {got}, want {want}");
    }
}

#[test]
fn scalar_reference_value_known() {
    // Pin a concrete known value so a regression in the scalar reference
    // itself is caught (not just relative agreement).
    // a = [0,1,...,15], b = [15,14,...,0]. Σ i*(15-i) for i=0..16
    // = 15*Σi - Σi² = 15*120 - 1240 = 1800 - 1240 = 560.
    let a: Vec<u8> = (0..16u8).collect();
    let b: Vec<u8> = (0..16u8).rev().collect();
    assert_eq!(dot_u8_scalar(&a, &b), 560);
    assert_eq!(dot_u8(&a, &b), 560);
}

// NOTE: `dot_u8` requires equal-length inputs (the SQ8 quantizer always
// passes same-dimension code vectors). There is intentionally NO test for
// mismatched lengths — that contract is shared with `dot_product` and is
// enforced by the `debug_assert_eq!` in each kernel. A release build
// truncates to the shorter length, matching `dot_product`'s behaviour.

// =====================================================================
// `weighted_bilinear_f32` (task #614) vs `weighted_bilinear_scalar`
// reference — f32 equivalence tests, not bit-exact (f32/FMA rounding
// differs between the SIMD and scalar accumulation order).
// =====================================================================

/// Relative-error tolerance shared by all f32 equivalence checks below,
/// consistent with the crate's other f32 SIMD-vs-scalar assertions.
fn assert_close(got: f32, want: f32, ctx: &str) {
    let tol = 1e-3 * want.abs().max(1.0);
    assert!(
        (got - want).abs() < tol,
        "{ctx}: got {got}, want {want} (tol {tol})"
    );
}

fn rand_f32_vec(dim: usize, seed: u64, lo: f32, hi: f32) -> Vec<f32> {
    let mut rng = Lcg::new(seed);
    (0..dim)
        .map(|_| {
            let high = (rng.next_u64() >> 32) as u32;
            let t = (high as f32) / (1u64 << 32) as f32;
            lo + (hi - lo) * t
        })
        .collect()
}

const WEIGHTED_DIMS: &[usize] = &[
    0, 1, 3, 4, 7, 8, 9, 15, 16, 17, 31, 32, 33, 64, 100, 128, 200,
];

#[test]
fn weighted_offset_slices_match_scalar_at_chunk_boundaries() {
    // On AArch64 these dispatchers select the NEON kernels when NEON exists.
    #[cfg(target_arch = "aarch64")]
    assert!(std::arch::is_aarch64_feature_detected!("neon"));

    for dim in [0usize, 1, 3, 4, 5, 7, 8, 9, 11, 12, 13] {
        let x_storage = rand_vec(dim + 7, 123);
        let y_storage = rand_vec(dim + 7, 456);
        let x_base = (4 - x_storage.as_ptr().addr() % 4) % 4;
        let y_base = (4 - y_storage.as_ptr().addr() % 4) % 4;
        let weights = rand_f32_vec(dim, 789, 0.25, 2.0);
        let scales = rand_f32_vec(dim, 987, 0.125, 1.0);
        for x_offset in 0..4 {
            for y_offset in 0..4 {
                let x = &x_storage[x_base + x_offset..x_base + x_offset + dim];
                let y = &y_storage[y_base + y_offset..y_base + y_offset + dim];
                assert_eq!(x.as_ptr().addr() % 4, x_offset);
                assert_eq!(y.as_ptr().addr() % 4, y_offset);
                let context = format!("dim={dim} x={x_offset} y={y_offset}");
                assert_close(
                    weighted_bilinear_f32(&weights, &scales, x, y),
                    weighted_bilinear_scalar(&weights, &scales, x, y),
                    &context,
                );
                assert_close(
                    weighted_sq_diff_u8(&scales, x, y),
                    weighted_sq_diff_scalar(&scales, x, y),
                    &context,
                );
                assert_close(
                    weighted_linear_u8(&weights, x),
                    weighted_linear_scalar(&weights, x),
                    &context,
                );
            }
        }
    }
}

#[test]
fn weighted_neon_does_not_use_aligned_u32_lane_loads() {
    // Numerical parity alone cannot detect undefined unaligned loads on CPUs
    // that tolerate them. This guard fails on the previous implementation.
    let source = include_str!("../../simd/quantized.rs");
    assert!(!source.contains("vld1_lane_u32"));
}

#[test]
fn weighted_bilinear_dispatcher_equals_scalar_random() {
    for &dim in WEIGHTED_DIMS {
        let min_scale = rand_f32_vec(dim, dim as u64 * 3 + 1, -5.0, 5.0);
        let scales_sq = rand_f32_vec(dim, dim as u64 * 5 + 2, 0.0, 3.0);
        let qx = rand_vec(dim, dim as u64 * 7 + 3);
        let qy = rand_vec(dim, dim as u64 * 11 + 5);

        let want = weighted_bilinear_scalar(&min_scale, &scales_sq, &qx, &qy);
        let got = weighted_bilinear_f32(&min_scale, &scales_sq, &qx, &qy);
        assert_close(got, want, &format!("weighted_bilinear dim={dim}"));
    }
}

#[test]
fn weighted_bilinear_dispatcher_equals_scalar_many_seeds() {
    // Statistical sweep over dim=128 (multiple SIMD chunks + no tail) and
    // dim=100 (multiple chunks + a scalar tail on every dispatched path).
    for &dim in &[100usize, 128] {
        for s in 0..64u64 {
            let min_scale =
                rand_f32_vec(dim, s.wrapping_mul(2654435761).wrapping_add(1), -3.0, 3.0);
            let scales_sq = rand_f32_vec(dim, s.wrapping_mul(40503).wrapping_add(7), 0.0, 2.0);
            let qx = rand_vec(dim, s.wrapping_mul(97).wrapping_add(11));
            let qy = rand_vec(dim, s.wrapping_mul(131).wrapping_add(13));

            let want = weighted_bilinear_scalar(&min_scale, &scales_sq, &qx, &qy);
            let got = weighted_bilinear_f32(&min_scale, &scales_sq, &qx, &qy);
            assert_close(
                got,
                want,
                &format!("weighted_bilinear seed sweep dim={dim} s={s}"),
            );
        }
    }
}

#[test]
fn weighted_bilinear_dispatcher_equals_scalar_zero_dim() {
    let empty: Vec<f32> = Vec::new();
    let empty_u8: Vec<u8> = Vec::new();
    assert_eq!(
        weighted_bilinear_f32(&empty, &empty, &empty_u8, &empty_u8),
        0.0
    );
    assert_eq!(
        weighted_bilinear_scalar(&empty, &empty, &empty_u8, &empty_u8),
        0.0
    );
}

#[test]
fn weighted_bilinear_dispatcher_equals_scalar_all_255() {
    // Saturation-adjacent configuration: every code at the u8 max, mixed
    // with large-magnitude weights, across dims spanning multiple SIMD
    // chunk widths plus a tail.
    for &dim in &[8usize, 16, 32, 100, 128] {
        let qx = vec![255u8; dim];
        let qy = vec![255u8; dim];
        let min_scale = vec![2.5f32; dim];
        let scales_sq = vec![1.5f32; dim];

        let want = weighted_bilinear_scalar(&min_scale, &scales_sq, &qx, &qy);
        let got = weighted_bilinear_f32(&min_scale, &scales_sq, &qx, &qy);
        assert_close(got, want, &format!("weighted_bilinear all-255 dim={dim}"));
    }
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[test]
fn weighted_bilinear_avx2_path_is_exercised_on_this_host() {
    // The equivalence tests above only prove the *dispatched* path (whatever
    // it happens to be) matches the scalar reference — they do not prove
    // the AVX2 kernel itself ran. x86_64 CI/dev hosts have AVX2, so assert
    // the feature-detect gate that `weighted_bilinear_f32` branches on is
    // actually true here, closing that gap.
    use crate::simd::has_avx2;
    assert!(
        has_avx2(),
        "expected AVX2 on the x86_64 test host; weighted_bilinear_f32's \
         AVX2 kernel would not be exercised by the tests above otherwise"
    );
}

// =====================================================================
// `weighted_sq_diff_u8` (F4) vs `weighted_sq_diff_scalar` reference — f32
// equivalence tests (dispatcher-equals-scalar, not bit-exact).
// =====================================================================

#[test]
fn weighted_sq_diff_dispatcher_equals_scalar_random() {
    for &dim in WEIGHTED_DIMS {
        let scales_sq = rand_f32_vec(dim, dim as u64 * 5 + 2, 0.0, 3.0);
        let qx = rand_vec(dim, dim as u64 * 7 + 3);
        let qy = rand_vec(dim, dim as u64 * 11 + 5);

        let want = weighted_sq_diff_scalar(&scales_sq, &qx, &qy);
        let got = weighted_sq_diff_u8(&scales_sq, &qx, &qy);
        assert_close(got, want, &format!("weighted_sq_diff dim={dim}"));
    }
}

#[test]
fn weighted_sq_diff_dispatcher_equals_scalar_many_seeds() {
    // Statistical sweep over dim=128 (multiple SIMD chunks + no tail) and
    // dim=100 (multiple chunks + a scalar tail on every dispatched path).
    for &dim in &[100usize, 128] {
        for s in 0..64u64 {
            let scales_sq = rand_f32_vec(dim, s.wrapping_mul(40503).wrapping_add(7), 0.0, 3.0);
            let qx = rand_vec(dim, s.wrapping_mul(97).wrapping_add(11));
            let qy = rand_vec(dim, s.wrapping_mul(131).wrapping_add(13));

            let want = weighted_sq_diff_scalar(&scales_sq, &qx, &qy);
            let got = weighted_sq_diff_u8(&scales_sq, &qx, &qy);
            assert_close(
                got,
                want,
                &format!("weighted_sq_diff seed sweep dim={dim} s={s}"),
            );
        }
    }
}

#[test]
fn weighted_sq_diff_dispatcher_equals_scalar_zero_dim() {
    let empty: Vec<f32> = Vec::new();
    let empty_u8: Vec<u8> = Vec::new();
    assert_eq!(weighted_sq_diff_u8(&empty, &empty_u8, &empty_u8), 0.0);
    assert_eq!(weighted_sq_diff_scalar(&empty, &empty_u8, &empty_u8), 0.0);
}

#[test]
fn weighted_sq_diff_dispatcher_equals_scalar_max_diff() {
    // Max-difference configuration: qx = all-255, qy = all-0, so every diff
    // is 255 and diff² = 65025 — the largest per-lane value a u8-widened
    // sq-diff kernel can see. With large scales_sq this stresses the f32
    // accumulation across multiple SIMD chunk widths plus a tail.
    for &dim in &[8usize, 16, 32, 100, 128] {
        let qx = vec![255u8; dim];
        let qy = vec![0u8; dim];
        let scales_sq = vec![1.5f32; dim];

        let want = weighted_sq_diff_scalar(&scales_sq, &qx, &qy);
        let got = weighted_sq_diff_u8(&scales_sq, &qx, &qy);
        assert_close(got, want, &format!("weighted_sq_diff max-diff dim={dim}"));
    }
}

#[test]
fn weighted_sq_diff_dispatcher_equals_scalar_self_diff_zero() {
    // Degenerate: qx == qy → every diff is 0 → result is exactly 0. Catches
    // a sign-error or a stale-accumulator bug that would produce a non-zero
    // sum.
    for &dim in &[8usize, 16, 32, 100, 128] {
        let qx = vec![255u8; dim];
        let scales_sq = vec![1.5f32; dim];

        assert_eq!(
            weighted_sq_diff_u8(&scales_sq, &qx, &qx),
            0.0,
            "self-diff should be 0 for dim={dim}"
        );
    }
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[test]
fn weighted_sq_diff_avx2_path_is_exercised_on_this_host() {
    use crate::simd::has_avx2;
    assert!(
        has_avx2(),
        "expected AVX2 on the x86_64 test host; weighted_sq_diff_u8's \
         AVX2 kernel would not be exercised by the tests above otherwise"
    );
}

// =====================================================================
// `weighted_linear_u8` (F4) vs `weighted_linear_scalar` reference — f32
// equivalence tests (dispatcher-equals-scalar, not bit-exact).
// =====================================================================

#[test]
fn weighted_linear_dispatcher_equals_scalar_random() {
    for &dim in WEIGHTED_DIMS {
        // Weights can be negative (qs[i] = query[i]*scale_i for Dot/Cosine).
        let weights = rand_f32_vec(dim, dim as u64 * 5 + 2, -2.0, 2.0);
        let codes = rand_vec(dim, dim as u64 * 7 + 3);

        let want = weighted_linear_scalar(&weights, &codes);
        let got = weighted_linear_u8(&weights, &codes);
        assert_close(got, want, &format!("weighted_linear dim={dim}"));
    }
}

#[test]
fn weighted_linear_dispatcher_equals_scalar_many_seeds() {
    for &dim in &[100usize, 128] {
        for s in 0..64u64 {
            let weights = rand_f32_vec(dim, s.wrapping_mul(40503).wrapping_add(7), -2.0, 2.0);
            let codes = rand_vec(dim, s.wrapping_mul(97).wrapping_add(11));

            let want = weighted_linear_scalar(&weights, &codes);
            let got = weighted_linear_u8(&weights, &codes);
            assert_close(
                got,
                want,
                &format!("weighted_linear seed sweep dim={dim} s={s}"),
            );
        }
    }
}

#[test]
fn weighted_linear_dispatcher_equals_scalar_zero_dim() {
    let empty: Vec<f32> = Vec::new();
    let empty_u8: Vec<u8> = Vec::new();
    assert_eq!(weighted_linear_u8(&empty, &empty_u8), 0.0);
    assert_eq!(weighted_linear_scalar(&empty, &empty_u8), 0.0);
}

#[test]
fn weighted_linear_dispatcher_equals_scalar_all_255() {
    // Saturation-adjacent: every code at the u8 max, with large-magnitude
    // weights (positive and negative), across dims spanning multiple SIMD
    // chunk widths plus a tail.
    for &dim in &[8usize, 16, 32, 100, 128] {
        let codes = vec![255u8; dim];
        let weights = vec![1.5f32; dim];

        let want = weighted_linear_scalar(&weights, &codes);
        let got = weighted_linear_u8(&weights, &codes);
        assert_close(got, want, &format!("weighted_linear all-255 dim={dim}"));
    }
}

#[test]
fn weighted_linear_dispatcher_equals_scalar_mixed_sign_weights() {
    // Weights spanning both signs — catches a widening/sign-extension error
    // in the u8→f32 path (u8 codes are always non-negative, but the f32
    // weight can flip the sign of each lane's contribution).
    for &dim in &[32usize, 64, 128] {
        let mut weights = vec![0.0f32; dim];
        let mut codes = vec![0u8; dim];
        for i in 0..dim {
            weights[i] = if i % 2 == 0 { 1.75 } else { -1.75 };
            codes[i] = if i % 3 == 0 { 255 } else { 128 };
        }
        let want = weighted_linear_scalar(&weights, &codes);
        let got = weighted_linear_u8(&weights, &codes);
        assert_close(got, want, &format!("weighted_linear mixed-sign dim={dim}"));
    }
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[test]
fn weighted_linear_avx2_path_is_exercised_on_this_host() {
    use crate::simd::has_avx2;
    assert!(
        has_avx2(),
        "expected AVX2 on the x86_64 test host; weighted_linear_u8's \
         AVX2 kernel would not be exercised by the tests above otherwise"
    );
}

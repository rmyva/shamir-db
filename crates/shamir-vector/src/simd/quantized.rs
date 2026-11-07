//! Quantized (u8-code) SIMD kernels: the integer u8 dot product and the
//! weighted SQ8 bilinear / squared-difference / linear kernels.

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
use super::f32_kernels::has_avx2;
#[cfg(target_arch = "aarch64")]
use super::f32_kernels::has_neon;

// =====================================================================
// Integer u8 dot-product kernel: Σ (a_i as u32) * (b_i as u32).
//
// Used by the SQ8 quantizer (vector/sq8.rs) to score the integer term of
// the approximated dot product over quantized codes. Because the inputs
// are u8 codes (0..=255), every partial product fits in u16 (max 255*255
// = 65025) and the running sum fits in u32 for any realistic dimension
// (max Σ = 255*255*dim; u32 overflows only at dim > 66 051). The kernel
// therefore returns u32 and is EXACT — integer arithmetic has no rounding.
//
// ## AVX2 saturation/signedness — the pitfall and the safe path
//
// The "obvious" building block `_mm256_maddubs_epi16(a, b)` is UNSAFE
// here on TWO counts:
//   1. It saturates to signed i16 (range [-32768, 32767]); two products
//      255*255 = 65025 each sum to 130050, which overflows i16 → the
//      result is silently clipped and no longer equals the scalar sum.
//   2. It treats the SECOND operand as signed i8: any u8 code > 127
//      becomes a negative number, flipping the sign of the product.
//
// Safe path (what we use): zero-extend both u8 lanes to u16 via
// `_mm256_unpacklo_epi8`/`_mm256_unpackhi_epi8` with a zero vector, then
// `_mm256_madd_epi16(a16, b16)` widens pairs of u16 products to i32.
// A single `_mm256_madd_epi16` lane sums TWO adjacent u16 products; each
// product ≤ 65025, so the pair sum ≤ 130050 ≪ 2^31 — NO saturation in
// the i32 accumulator, hence bit-exact equality with the scalar u32 sum.
//
// ## NEON
//
// aarch64 `vdotq_u32(acc, u8x16, u8x16)` (the `udot` instruction, gated
// on the `dotprod` feature) is UNSIGNED and accumulates four u8*u8
// products into one u32 lane (max 4*65025 = 260100 < 2^32) — exactly our
// case, no signedness trap. When `dotprod` is absent we fall back to a
// portable NEON widening path (`vmull_u8` + accumulation) which is also
// exact; ultimately the scalar path is the portable floor.
//
// ## AVX-512 VNNI
//
// `_mm512_dpbusd_epi32(acc, u8, i8)` is the natural fit BUT it treats the
// second operand as signed i8 — the same signedness trap as `maddubs`.
// Resolving it safely (pre-biasing the codes to a signed range and
// correcting the linear term) is delicate and only helps on the small set
// of CPUs with AVX-512 VNNI. Per the brief (V5.1 §2) it is DEFERRED to
// #411; AVX2 + scalar + NEON satisfy the DoD.
//
// Invariant: every code path returns EXACTLY `dot_u8_scalar(a, b)` for the
// same inputs — integer equality, no tolerance. This is verified by the
// `simd_tests` suite on random and saturation-edge vectors.
// =====================================================================

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[inline]
fn has_avx512vnni() -> bool {
    use std::sync::OnceLock;
    static CACHED: OnceLock<bool> = OnceLock::new();
    // Detected but currently UNUSED (VNNI deferred to #411 — see the
    // block comment above). Kept here so the gate is exercised by the
    // test suite and the dispatch can light it up in #411 without
    // re-plumbing the detection cache.
    *CACHED.get_or_init(|| std::is_x86_feature_detected!("avx512vnni"))
}

/// Unsigned u8 dot product: returns `Σ (a_i as u32) * (b_i as u32)`.
///
/// Bit-exact across all dispatched kernels and the scalar reference. The
/// dispatcher mirrors `dot_product`: one `OnceLock`-cached feature check,
/// one predictable branch on hot memory, then the kernel. Inputs MUST be
/// equal length (debug-asserted); behaviour on mismatched tails is to
/// truncate to the shorter (matching `dot_product`).
#[inline]
#[allow(dead_code)] // VR-7 (#429): sole production call site (approx_dot) was
                    // dead-weight; the kernel survives as tested SIMD
                    // infrastructure for a future weighted-SQ8 dot (see the
                    // block comment above + `tests/simd_tests.rs`), not
                    // because anything in the lib calls it today.
pub(crate) fn dot_u8(a: &[u8], b: &[u8]) -> u32 {
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    {
        if has_avx2() {
            // SAFETY: `has_avx2()` guarantees the AVX2 target feature
            // required by the intrinsics in `dot_u8_avx2` is present on
            // this CPU.
            return unsafe { dot_u8_avx2(a, b) };
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        if has_neon() {
            // SAFETY: `has_neon()` guarantees the NEON target feature
            // required by the intrinsics in `dot_u8_neon_wide`.
            return unsafe { dot_u8_neon_wide(a, b) };
        }
    }
    // Silence the "field is never used" lint for the VNNI gate on x86
    // when no caller reads it yet.
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    let _ = has_avx512vnni();
    dot_u8_scalar(a, b)
}

/// Scalar reference for `dot_u8`: chunked multi-accumulator in u32.
///
/// This is the GOLD-STANDARD reference every SIMD kernel must match
/// bit-for-bit (integer equality, `==` — no float tolerance). Written in
/// a chunked, multi-accumulator form the compiler reliably autovectorizes
/// (SSE2/NEON) even when no explicit AVX2 path is taken.
#[inline]
pub(crate) fn dot_u8_scalar(a: &[u8], b: &[u8]) -> u32 {
    debug_assert_eq!(a.len(), b.len());
    let n = a.len().min(b.len());
    let chunks = n / 8;
    let mut acc = [0u32; 8];
    for i in 0..chunks {
        let base = i * 8;
        for j in 0..8 {
            acc[j] += (a[base + j] as u32) * (b[base + j] as u32);
        }
    }
    let mut s = acc[0] + acc[1] + acc[2] + acc[3] + acc[4] + acc[5] + acc[6] + acc[7];
    for i in (chunks * 8)..n {
        s += (a[i] as u32) * (b[i] as u32);
    }
    s
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "avx2")]
unsafe fn dot_u8_avx2(a: &[u8], b: &[u8]) -> u32 {
    #[cfg(target_arch = "x86")]
    use std::arch::x86::*;
    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64::*;

    debug_assert_eq!(a.len(), b.len());
    let n = a.len().min(b.len());
    let chunks = n / 32;

    // Four independent i32-lane accumulators (8 i32 each) so we don't
    // bottleneck on the `_mm256_madd_epi16` latency.
    let mut acc0 = _mm256_setzero_si256();
    let mut acc1 = _mm256_setzero_si256();
    let mut acc2 = _mm256_setzero_si256();
    let mut acc3 = _mm256_setzero_si256();

    let ap = a.as_ptr();
    let bp = b.as_ptr();
    let mut i = 0usize;
    // Unrolled by four 32-byte blocks (128 bytes/iter) to keep four
    // independent accumulators busy. Falls through to the 1-block loop
    // for the remainder of the chunk-aligned region.
    while i + 128 <= chunks * 32 {
        for (acc, off) in [
            (&mut acc0, 0usize),
            (&mut acc1, 32usize),
            (&mut acc2, 64usize),
            (&mut acc3, 96usize),
        ] {
            let av = _mm256_loadu_si256(ap.add(i + off) as *const __m256i);
            let bv = _mm256_loadu_si256(bp.add(i + off) as *const __m256i);
            // Zero-extend u8 → u16 via the dedicated intrinsic: it takes
            // the LOW 128 bits of each 256-bit input and widens to 256
            // bits of u16 (16 lanes). We split each 32-byte load into its
            // low/high 128-bit halves first. NO saturation downstream:
            // u16·u16 ≤ 65025, and `_mm256_madd_epi16` sums a PAIR of such
            // products into one i32 (pair-sum ≤ 130050 ≪ 2^31).
            let a_lo128 = _mm256_castsi256_si128(av);
            let a_hi128 = _mm256_extracti128_si256(av, 1);
            let b_lo128 = _mm256_castsi256_si128(bv);
            let b_hi128 = _mm256_extracti128_si256(bv, 1);
            let a_lo = _mm256_cvtepu8_epi16(a_lo128);
            let a_hi = _mm256_cvtepu8_epi16(a_hi128);
            let b_lo = _mm256_cvtepu8_epi16(b_lo128);
            let b_hi = _mm256_cvtepu8_epi16(b_hi128);
            *acc = _mm256_add_epi32(*acc, _mm256_madd_epi16(a_lo, b_lo));
            *acc = _mm256_add_epi32(*acc, _mm256_madd_epi16(a_hi, b_hi));
        }
        i += 128;
    }
    while i + 32 <= chunks * 32 {
        let av = _mm256_loadu_si256(ap.add(i) as *const __m256i);
        let bv = _mm256_loadu_si256(bp.add(i) as *const __m256i);
        let a_lo128 = _mm256_castsi256_si128(av);
        let a_hi128 = _mm256_extracti128_si256(av, 1);
        let b_lo128 = _mm256_castsi256_si128(bv);
        let b_hi128 = _mm256_extracti128_si256(bv, 1);
        let a_lo = _mm256_cvtepu8_epi16(a_lo128);
        let a_hi = _mm256_cvtepu8_epi16(a_hi128);
        let b_lo = _mm256_cvtepu8_epi16(b_lo128);
        let b_hi = _mm256_cvtepu8_epi16(b_hi128);
        acc0 = _mm256_add_epi32(acc0, _mm256_madd_epi16(a_lo, b_lo));
        acc0 = _mm256_add_epi32(acc0, _mm256_madd_epi16(a_hi, b_hi));
        i += 32;
    }

    // Horizontal reduction of the four 8-i32 accumulators to one i32.
    let s01 = _mm256_add_epi32(acc0, acc1);
    let s23 = _mm256_add_epi32(acc2, acc3);
    let s = _mm256_add_epi32(s01, s23);
    // Sum the 8 i32 lanes of `s`. Extract the high 128-bit half and add
    // to the low half, then horizontal-sum the resulting 4 i32 lanes.
    let hi128 = _mm256_extracti128_si256(s, 1);
    let lo128 = _mm256_castsi256_si128(s);
    let v4 = _mm_add_epi32(lo128, hi128);
    let shuf = _mm_shuffle_epi32(v4, 0b00_01_10_11); // [3,2,1,0] -> reverse
    let v4b = _mm_add_epi32(v4, shuf);
    let mut sum = (_mm_cvtsi128_si32(v4b) as i64 + _mm_extract_epi32(v4b, 1) as i64) as u32;

    // Scalar tail: the remaining bytes after the last full 32-byte chunk.
    for k in (chunks * 32)..n {
        sum += (a[k] as u32) * (b[k] as u32);
    }
    sum
}

/// Portable NEON widening path for aarch64 CPUs.
/// Uses `vmull_u8` (u8→u16 widening multiply) + accumulation in u32.
/// Still bit-exact: no saturation, all-u32 accumulation.
#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn dot_u8_neon_wide(a: &[u8], b: &[u8]) -> u32 {
    use std::arch::aarch64::*;

    debug_assert_eq!(a.len(), b.len());
    let n = a.len().min(b.len());
    let chunks = n / 8;

    let mut acc0 = vdupq_n_u32(0);
    let mut acc1 = vdupq_n_u32(0);

    let ap = a.as_ptr();
    let bp = b.as_ptr();
    let mut i = 0usize;
    while i + 16 <= chunks * 8 {
        // Low 8 bytes of each operand, widen-multiply to u16x8, then
        // pairwise-add into u32x4 and accumulate. Max per lane: 2*65025
        // = 130050 ≪ 2^32.
        let al0 = vld1_u8(ap.add(i));
        let bl0 = vld1_u8(bp.add(i));
        let p0 = vmull_u8(al0, bl0);
        acc0 = vpadalq_u16(acc0, p0);

        let al1 = vld1_u8(ap.add(i + 8));
        let bl1 = vld1_u8(bp.add(i + 8));
        let p1 = vmull_u8(al1, bl1);
        acc1 = vpadalq_u16(acc1, p1);
        i += 16;
    }
    while i + 8 <= chunks * 8 {
        let al = vld1_u8(ap.add(i));
        let bl = vld1_u8(bp.add(i));
        let p = vmull_u8(al, bl);
        acc0 = vpadalq_u16(acc0, p);
        i += 8;
    }

    let sum = vaddq_u32(acc0, acc1);
    let mut s = vaddvq_u32(sum);

    for k in (chunks * 8)..n {
        s += (a[k] as u32) * (b[k] as u32);
    }
    s
}

// =====================================================================
// Weighted SQ8 bilinear kernel (task #614):
//
//   Σ_i ( min_scale[i]*(qx[i]+qy[i]) + scales_sq[i]*qx[i]*qy[i] )
//
// Unlike `dot_u8` above, the per-dimension weights `min_scale[i]` /
// `scales_sq[i]` are NOT uniform (they come from the SQ8 quantizer's
// per-dimension `[min, max]` fit), so the integer-uniform-weight kernel
// cannot be reused here — this is a dedicated f32 kernel that widens the
// u8 codes to f32 lanes and folds in the per-lane weights.
//
// Invariant: every dispatched path returns the same value as
// `weighted_bilinear_scalar` to within f32/FMA rounding (see
// `tests/simd_tests.rs`).
// =====================================================================

/// Scalar reference: widens u8 codes to f32 and accumulates the linear +
/// bilinear terms in one pass. Mirrors the original loop this kernel
/// replaces in [`crate::sq8::Sq8Quantizer::approx_dot`].
#[inline]
pub(crate) fn weighted_bilinear_scalar(
    min_scale: &[f32],
    scales_sq: &[f32],
    qx: &[u8],
    qy: &[u8],
) -> f32 {
    debug_assert_eq!(min_scale.len(), scales_sq.len());
    debug_assert_eq!(min_scale.len(), qx.len());
    debug_assert_eq!(min_scale.len(), qy.len());
    let n = min_scale
        .len()
        .min(scales_sq.len())
        .min(qx.len())
        .min(qy.len());

    let mut acc = 0.0f32;
    for i in 0..n {
        let qx_i = qx[i] as f32;
        let qy_i = qy[i] as f32;
        acc += min_scale[i] * (qx_i + qy_i) + scales_sq[i] * qx_i * qy_i;
    }
    acc
}

/// Dispatcher: AVX2 → NEON → scalar, mirroring `dot_product`/`dot_u8`.
#[inline]
pub(crate) fn weighted_bilinear_f32(
    min_scale: &[f32],
    scales_sq: &[f32],
    qx: &[u8],
    qy: &[u8],
) -> f32 {
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    {
        if has_avx2() {
            // SAFETY: `has_avx2()` guarantees the AVX2 target feature
            // required by the intrinsics in `weighted_bilinear_avx2`.
            return unsafe { weighted_bilinear_avx2(min_scale, scales_sq, qx, qy) };
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        if has_neon() {
            // SAFETY: `has_neon()` guarantees the NEON target feature
            // required by the intrinsics in `weighted_bilinear_neon`.
            return unsafe { weighted_bilinear_neon(min_scale, scales_sq, qx, qy) };
        }
    }
    weighted_bilinear_scalar(min_scale, scales_sq, qx, qy)
}

/// AVX2 path: widen 8 u8 lanes at a time to f32 (u8 → i32 → f32 via
/// `_mm256_cvtepu8_epi32`, which reads the low 8 bytes of a 128-bit input
/// and zero-extends each to a 32-bit lane), then FMA against the f32
/// weight vectors.
#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "avx2,fma")]
unsafe fn weighted_bilinear_avx2(
    min_scale: &[f32],
    scales_sq: &[f32],
    qx: &[u8],
    qy: &[u8],
) -> f32 {
    #[cfg(target_arch = "x86")]
    use std::arch::x86::*;
    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64::*;

    debug_assert_eq!(min_scale.len(), scales_sq.len());
    debug_assert_eq!(min_scale.len(), qx.len());
    debug_assert_eq!(min_scale.len(), qy.len());
    let n = min_scale
        .len()
        .min(scales_sq.len())
        .min(qx.len())
        .min(qy.len());
    let chunks = n / 8;

    let mut acc = _mm256_setzero_ps();

    let msp = min_scale.as_ptr();
    let ssp = scales_sq.as_ptr();
    let xp = qx.as_ptr();
    let yp = qy.as_ptr();

    let mut i = 0usize;
    while i < chunks * 8 {
        // Load 8 u8 codes (low 64 bits of a 128-bit register are enough;
        // `_mm_loadl_epi64` reads exactly 8 bytes) and zero-extend to
        // 8 lanes of i32, then convert to f32.
        let xv_u8 = _mm_loadl_epi64(xp.add(i) as *const __m128i);
        let yv_u8 = _mm_loadl_epi64(yp.add(i) as *const __m128i);
        let xv_i32 = _mm256_cvtepu8_epi32(xv_u8);
        let yv_i32 = _mm256_cvtepu8_epi32(yv_u8);
        let xv = _mm256_cvtepi32_ps(xv_i32);
        let yv = _mm256_cvtepi32_ps(yv_i32);

        let msv = _mm256_loadu_ps(msp.add(i));
        let ssv = _mm256_loadu_ps(ssp.add(i));

        // linear term: min_scale * (qx + qy)
        let sum_xy = _mm256_add_ps(xv, yv);
        acc = _mm256_fmadd_ps(msv, sum_xy, acc);
        // bilinear term: scales_sq * qx * qy
        let prod_xy = _mm256_mul_ps(xv, yv);
        acc = _mm256_fmadd_ps(ssv, prod_xy, acc);

        i += 8;
    }

    // Horizontal reduction of the single 8-lane accumulator.
    let hi = _mm256_extractf128_ps(acc, 1);
    let lo = _mm256_castps256_ps128(acc);
    let v128 = _mm_add_ps(lo, hi);
    let shuf = _mm_movehdup_ps(v128);
    let sums = _mm_add_ps(v128, shuf);
    let shuf2 = _mm_movehl_ps(sums, sums);
    let sums2 = _mm_add_ss(sums, shuf2);
    let mut s = _mm_cvtss_f32(sums2);

    for k in (chunks * 8)..n {
        let qx_k = qx[k] as f32;
        let qy_k = qy[k] as f32;
        s += min_scale[k] * (qx_k + qy_k) + scales_sq[k] * qx_k * qy_k;
    }
    s
}

/// NEON path: widen 4 u8 lanes at a time (`u8 → u16 → u32 → f32`, mirroring
/// `dot_u8_neon_wide`'s widening style), then FMA against the f32 weight
/// vectors.
#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn weighted_bilinear_neon(
    min_scale: &[f32],
    scales_sq: &[f32],
    qx: &[u8],
    qy: &[u8],
) -> f32 {
    use std::arch::aarch64::*;

    debug_assert_eq!(min_scale.len(), scales_sq.len());
    debug_assert_eq!(min_scale.len(), qx.len());
    debug_assert_eq!(min_scale.len(), qy.len());
    let n = min_scale
        .len()
        .min(scales_sq.len())
        .min(qx.len())
        .min(qy.len());
    let chunks = n / 4;

    let mut acc = vdupq_n_f32(0.0);

    let msp = min_scale.as_ptr();
    let ssp = scales_sq.as_ptr();
    let mut i = 0usize;
    while i < chunks * 4 {
        // Four indexed byte reads are in bounds because i + 4 <= n. Inserting
        // byte lanes explicitly preserves their order without a u32 load.
        let mut xv_u8x8 = vdup_n_u8(0);
        xv_u8x8 = vset_lane_u8(qx[i], xv_u8x8, 0);
        xv_u8x8 = vset_lane_u8(qx[i + 1], xv_u8x8, 1);
        xv_u8x8 = vset_lane_u8(qx[i + 2], xv_u8x8, 2);
        xv_u8x8 = vset_lane_u8(qx[i + 3], xv_u8x8, 3);
        let mut yv_u8x8 = vdup_n_u8(0);
        yv_u8x8 = vset_lane_u8(qy[i], yv_u8x8, 0);
        yv_u8x8 = vset_lane_u8(qy[i + 1], yv_u8x8, 1);
        yv_u8x8 = vset_lane_u8(qy[i + 2], yv_u8x8, 2);
        yv_u8x8 = vset_lane_u8(qy[i + 3], yv_u8x8, 3);
        let xv_u16 = vmovl_u8(xv_u8x8);
        let yv_u16 = vmovl_u8(yv_u8x8);
        let xv_u32 = vmovl_u16(vget_low_u16(xv_u16));
        let yv_u32 = vmovl_u16(vget_low_u16(yv_u16));
        let xv = vcvtq_f32_u32(xv_u32);
        let yv = vcvtq_f32_u32(yv_u32);

        let msv = vld1q_f32(msp.add(i));
        let ssv = vld1q_f32(ssp.add(i));

        let sum_xy = vaddq_f32(xv, yv);
        acc = vfmaq_f32(acc, msv, sum_xy);
        let prod_xy = vmulq_f32(xv, yv);
        acc = vfmaq_f32(acc, ssv, prod_xy);

        i += 4;
    }

    let mut s = vaddvq_f32(acc);

    for k in (chunks * 4)..n {
        let qx_k = qx[k] as f32;
        let qy_k = qy[k] as f32;
        s += min_scale[k] * (qx_k + qy_k) + scales_sq[k] * qx_k * qy_k;
    }
    s
}

// =====================================================================
// Weighted SQ8 squared-difference kernel (F4):
//
//   Σ_i scales_sq[i] * (qx[i] − qy[i])²
//
// The L2 analogue of `weighted_bilinear_f32` above: same u8→f32 widening
// (8 lanes per AVX2 iter, 4 lanes per NEON iter) and the same per-lane f32
// weight, but the accumulate step is a squared difference (subtract, square,
// FMA) instead of the linear+bilinear product. Used by
// `Sq8Quantizer::approx_l2_sq`. There is no existing kernel to reuse — the
// bilinear kernel multiplies qx*qy; this one computes (qx−qy)².
//
// Invariant: every dispatched path returns the same value as
// `weighted_sq_diff_scalar` to within f32/FMA rounding (see
// `tests/simd_tests.rs`).
// =====================================================================

/// Scalar reference: widens u8 codes to f32 and accumulates
/// `scales_sq[i] * (qx[i] − qy[i])²` in one pass. Mirrors the original loop
/// this kernel replaces in [`crate::sq8::Sq8Quantizer::approx_l2_sq`].
#[inline]
pub(crate) fn weighted_sq_diff_scalar(scales_sq: &[f32], qx: &[u8], qy: &[u8]) -> f32 {
    debug_assert_eq!(scales_sq.len(), qx.len());
    debug_assert_eq!(scales_sq.len(), qy.len());
    let n = scales_sq.len().min(qx.len()).min(qy.len());

    let mut acc = 0.0f32;
    for i in 0..n {
        let diff = (qx[i] as f32) - (qy[i] as f32);
        acc += scales_sq[i] * (diff * diff);
    }
    acc
}

/// Dispatcher: AVX2 → NEON → scalar, mirroring `weighted_bilinear_f32`.
#[inline]
pub(crate) fn weighted_sq_diff_u8(scales_sq: &[f32], qx: &[u8], qy: &[u8]) -> f32 {
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    {
        if has_avx2() {
            // SAFETY: `has_avx2()` guarantees the AVX2 + FMA target
            // features required by the intrinsics in `weighted_sq_diff_avx2`.
            return unsafe { weighted_sq_diff_avx2(scales_sq, qx, qy) };
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        if has_neon() {
            // SAFETY: `has_neon()` guarantees the NEON target feature
            // required by the intrinsics in `weighted_sq_diff_neon`.
            return unsafe { weighted_sq_diff_neon(scales_sq, qx, qy) };
        }
    }
    weighted_sq_diff_scalar(scales_sq, qx, qy)
}

/// AVX2 path: identical u8→f32 widening as `weighted_bilinear_avx2`
/// (`_mm_loadl_epi64` → `_mm256_cvtepu8_epi32` → `_mm256_cvtepi32_ps`),
/// then `acc = fmadd(scales_sq, diff*diff, acc)` where `diff = xv − yv`.
#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "avx2,fma")]
unsafe fn weighted_sq_diff_avx2(scales_sq: &[f32], qx: &[u8], qy: &[u8]) -> f32 {
    #[cfg(target_arch = "x86")]
    use std::arch::x86::*;
    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64::*;

    debug_assert_eq!(scales_sq.len(), qx.len());
    debug_assert_eq!(scales_sq.len(), qy.len());
    let n = scales_sq.len().min(qx.len()).min(qy.len());
    let chunks = n / 8;

    let mut acc = _mm256_setzero_ps();

    let ssp = scales_sq.as_ptr();
    let xp = qx.as_ptr();
    let yp = qy.as_ptr();

    let mut i = 0usize;
    while i < chunks * 8 {
        // Load 8 u8 codes and zero-extend to 8 lanes of i32, then f32.
        let xv_u8 = _mm_loadl_epi64(xp.add(i) as *const __m128i);
        let yv_u8 = _mm_loadl_epi64(yp.add(i) as *const __m128i);
        let xv_i32 = _mm256_cvtepu8_epi32(xv_u8);
        let yv_i32 = _mm256_cvtepu8_epi32(yv_u8);
        let xv = _mm256_cvtepi32_ps(xv_i32);
        let yv = _mm256_cvtepi32_ps(yv_i32);

        let ssv = _mm256_loadu_ps(ssp.add(i));

        // Squared-difference term: scales_sq * (xv − yv)².
        let diff = _mm256_sub_ps(xv, yv);
        acc = _mm256_fmadd_ps(ssv, _mm256_mul_ps(diff, diff), acc);

        i += 8;
    }

    // Horizontal reduction of the single 8-lane accumulator (identical to
    // `weighted_bilinear_avx2`).
    let hi = _mm256_extractf128_ps(acc, 1);
    let lo = _mm256_castps256_ps128(acc);
    let v128 = _mm_add_ps(lo, hi);
    let shuf = _mm_movehdup_ps(v128);
    let sums = _mm_add_ps(v128, shuf);
    let shuf2 = _mm_movehl_ps(sums, sums);
    let sums2 = _mm_add_ss(sums, shuf2);
    let mut s = _mm_cvtss_f32(sums2);

    for k in (chunks * 8)..n {
        let diff = (qx[k] as f32) - (qy[k] as f32);
        s += scales_sq[k] * (diff * diff);
    }
    s
}

/// NEON path: identical u8→f32 widening as `weighted_bilinear_neon`
/// (byte lane insertion/`vmovl_u8`/`vmovl_u16`/`vcvtq_f32_u32`), then
/// `acc = vfmaq(acc, scales_sq, diff*diff)` where `diff = xv − yv`.
#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn weighted_sq_diff_neon(scales_sq: &[f32], qx: &[u8], qy: &[u8]) -> f32 {
    use std::arch::aarch64::*;

    debug_assert_eq!(scales_sq.len(), qx.len());
    debug_assert_eq!(scales_sq.len(), qy.len());
    let n = scales_sq.len().min(qx.len()).min(qy.len());
    let chunks = n / 4;

    let mut acc = vdupq_n_f32(0.0);

    let ssp = scales_sq.as_ptr();
    let mut i = 0usize;
    while i < chunks * 4 {
        // Four indexed byte reads are in bounds because i + 4 <= n. Inserting
        // byte lanes explicitly preserves their order without a u32 load.
        let mut xv_u8x8 = vdup_n_u8(0);
        xv_u8x8 = vset_lane_u8(qx[i], xv_u8x8, 0);
        xv_u8x8 = vset_lane_u8(qx[i + 1], xv_u8x8, 1);
        xv_u8x8 = vset_lane_u8(qx[i + 2], xv_u8x8, 2);
        xv_u8x8 = vset_lane_u8(qx[i + 3], xv_u8x8, 3);
        let mut yv_u8x8 = vdup_n_u8(0);
        yv_u8x8 = vset_lane_u8(qy[i], yv_u8x8, 0);
        yv_u8x8 = vset_lane_u8(qy[i + 1], yv_u8x8, 1);
        yv_u8x8 = vset_lane_u8(qy[i + 2], yv_u8x8, 2);
        yv_u8x8 = vset_lane_u8(qy[i + 3], yv_u8x8, 3);
        let xv_u16 = vmovl_u8(xv_u8x8);
        let yv_u16 = vmovl_u8(yv_u8x8);
        let xv_u32 = vmovl_u16(vget_low_u16(xv_u16));
        let yv_u32 = vmovl_u16(vget_low_u16(yv_u16));
        let xv = vcvtq_f32_u32(xv_u32);
        let yv = vcvtq_f32_u32(yv_u32);

        let ssv = vld1q_f32(ssp.add(i));

        // Squared-difference term: scales_sq * (xv − yv)².
        let diff = vsubq_f32(xv, yv);
        acc = vfmaq_f32(acc, ssv, vmulq_f32(diff, diff));

        i += 4;
    }

    let mut s = vaddvq_f32(acc);

    for k in (chunks * 4)..n {
        let diff = (qx[k] as f32) - (qy[k] as f32);
        s += scales_sq[k] * (diff * diff);
    }
    s
}

// =====================================================================
// Weighted SQ8 linear kernel (F4):
//
//   Σ_i weights[i] * codes[i]
//
// A single f32-weight × u8-code multiply-accumulate, with no bilinear or
// subtract term (simpler than `weighted_bilinear_f32` above). Used by
// `RescoreCtx::fused_dot` for the Dot/Cosine rescore path.
//
// Invariant: every dispatched path returns the same value as
// `weighted_linear_scalar` to within f32/FMA rounding (see
// `tests/simd_tests.rs`).
// =====================================================================

/// Scalar reference: widens u8 codes to f32 and accumulates
/// `weights[i] * codes[i]` in one pass. Mirrors the original loop this
/// kernel replaces in `RescoreCtx::fused_dot`.
#[inline]
pub(crate) fn weighted_linear_scalar(weights: &[f32], codes: &[u8]) -> f32 {
    debug_assert_eq!(weights.len(), codes.len());
    let n = weights.len().min(codes.len());

    let mut acc = 0.0f32;
    for i in 0..n {
        acc += weights[i] * (codes[i] as f32);
    }
    acc
}

/// Dispatcher: AVX2 → NEON → scalar, mirroring `weighted_bilinear_f32`.
#[inline]
pub(crate) fn weighted_linear_u8(weights: &[f32], codes: &[u8]) -> f32 {
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    {
        if has_avx2() {
            // SAFETY: `has_avx2()` guarantees the AVX2 + FMA target
            // features required by the intrinsics in `weighted_linear_avx2`.
            return unsafe { weighted_linear_avx2(weights, codes) };
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        if has_neon() {
            // SAFETY: `has_neon()` guarantees the NEON target feature
            // required by the intrinsics in `weighted_linear_neon`.
            return unsafe { weighted_linear_neon(weights, codes) };
        }
    }
    weighted_linear_scalar(weights, codes)
}

/// AVX2 path: identical u8→f32 widening as `weighted_bilinear_avx2`, then a
/// single `acc = fmadd(weights, codes_f32, acc)` per 8 lanes.
#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "avx2,fma")]
unsafe fn weighted_linear_avx2(weights: &[f32], codes: &[u8]) -> f32 {
    #[cfg(target_arch = "x86")]
    use std::arch::x86::*;
    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64::*;

    debug_assert_eq!(weights.len(), codes.len());
    let n = weights.len().min(codes.len());
    let chunks = n / 8;

    let mut acc = _mm256_setzero_ps();

    let wp = weights.as_ptr();
    let cp = codes.as_ptr();

    let mut i = 0usize;
    while i < chunks * 8 {
        // Load 8 u8 codes and zero-extend to 8 lanes of i32, then f32.
        let cv_u8 = _mm_loadl_epi64(cp.add(i) as *const __m128i);
        let cv_i32 = _mm256_cvtepu8_epi32(cv_u8);
        let cv = _mm256_cvtepi32_ps(cv_i32);

        let wv = _mm256_loadu_ps(wp.add(i));

        // Linear term: weights * codes.
        acc = _mm256_fmadd_ps(wv, cv, acc);

        i += 8;
    }

    // Horizontal reduction (identical to `weighted_bilinear_avx2`).
    let hi = _mm256_extractf128_ps(acc, 1);
    let lo = _mm256_castps256_ps128(acc);
    let v128 = _mm_add_ps(lo, hi);
    let shuf = _mm_movehdup_ps(v128);
    let sums = _mm_add_ps(v128, shuf);
    let shuf2 = _mm_movehl_ps(sums, sums);
    let sums2 = _mm_add_ss(sums, shuf2);
    let mut s = _mm_cvtss_f32(sums2);

    for k in (chunks * 8)..n {
        s += weights[k] * (codes[k] as f32);
    }
    s
}

/// NEON path: identical u8→f32 widening as `weighted_bilinear_neon`, then a
/// single `acc = vfmaq(acc, weights, codes_f32)` per 4 lanes.
#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn weighted_linear_neon(weights: &[f32], codes: &[u8]) -> f32 {
    use std::arch::aarch64::*;

    debug_assert_eq!(weights.len(), codes.len());
    let n = weights.len().min(codes.len());
    let chunks = n / 4;

    let mut acc = vdupq_n_f32(0.0);

    let wp = weights.as_ptr();
    let mut i = 0usize;
    while i < chunks * 4 {
        // Four indexed byte reads are in bounds because i + 4 <= n. Inserting
        // byte lanes explicitly preserves their order without a u32 load.
        let mut cv_u8x8 = vdup_n_u8(0);
        cv_u8x8 = vset_lane_u8(codes[i], cv_u8x8, 0);
        cv_u8x8 = vset_lane_u8(codes[i + 1], cv_u8x8, 1);
        cv_u8x8 = vset_lane_u8(codes[i + 2], cv_u8x8, 2);
        cv_u8x8 = vset_lane_u8(codes[i + 3], cv_u8x8, 3);
        let cv_u16 = vmovl_u8(cv_u8x8);
        let cv_u32 = vmovl_u16(vget_low_u16(cv_u16));
        let cv = vcvtq_f32_u32(cv_u32);

        let wv = vld1q_f32(wp.add(i));

        // Linear term: weights * codes.
        acc = vfmaq_f32(acc, wv, cv);

        i += 4;
    }

    let mut s = vaddvq_f32(acc);

    for k in (chunks * 4)..n {
        s += weights[k] * (codes[k] as f32);
    }
    s
}

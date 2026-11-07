//! Runtime feature detection and f32 distance kernels
//! (`dot_product` / `l2_squared`): AVX-512F, AVX2+FMA, NEON and scalar
//! paths.

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[inline]
fn has_avx512f() -> bool {
    use std::sync::OnceLock;
    static CACHED: OnceLock<bool> = OnceLock::new();
    *CACHED.get_or_init(|| std::is_x86_feature_detected!("avx512f"))
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[inline]
pub(crate) fn has_avx2() -> bool {
    use std::sync::OnceLock;
    static CACHED: OnceLock<bool> = OnceLock::new();
    *CACHED.get_or_init(|| std::is_x86_feature_detected!("avx2"))
}

#[cfg(target_arch = "aarch64")]
#[inline]
pub(super) fn has_neon() -> bool {
    use std::sync::OnceLock;
    static CACHED: OnceLock<bool> = OnceLock::new();
    // NEON is architecturally guaranteed on aarch64, but the detect
    // macro is the canonical gate for the `#[target_feature]` call.
    *CACHED.get_or_init(|| std::arch::is_aarch64_feature_detected!("neon"))
}

#[inline]
pub(crate) fn dot_product(a: &[f32], b: &[f32]) -> f32 {
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    {
        if has_avx512f() {
            // SAFETY: `has_avx512f()` guarantees the AVX-512F target
            // feature required by the intrinsics in `dot_product_avx512`.
            return unsafe { dot_product_avx512(a, b) };
        }
        if has_avx2() {
            // SAFETY: `has_avx2()` guarantees the AVX2 + FMA target
            // features required by the intrinsics used in
            // `dot_product_avx2` are present on this CPU.
            return unsafe { dot_product_avx2(a, b) };
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        if has_neon() {
            // SAFETY: `has_neon()` guarantees the NEON target feature
            // required by the intrinsics in `dot_product_neon`.
            return unsafe { dot_product_neon(a, b) };
        }
    }
    dot_product_scalar(a, b)
}

#[inline]
pub(crate) fn l2_squared(a: &[f32], b: &[f32]) -> f32 {
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    {
        if has_avx512f() {
            // SAFETY: see `dot_product` above.
            return unsafe { l2_squared_avx512(a, b) };
        }
        if has_avx2() {
            // SAFETY: see `dot_product` above.
            return unsafe { l2_squared_avx2(a, b) };
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        if has_neon() {
            // SAFETY: see `dot_product` above.
            return unsafe { l2_squared_neon(a, b) };
        }
    }
    l2_squared_scalar(a, b)
}

/// Scalar dot product written so the compiler reliably autovectorizes:
/// fixed-size chunks of 8, separate accumulators, no bounds checks in
/// the inner loop.
#[inline]
fn dot_product_scalar(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    let n = a.len().min(b.len());
    let chunks = n / 8;
    let mut acc = [0.0f32; 8];
    for i in 0..chunks {
        let base = i * 8;
        // Indexing with a known-good range; LLVM elides the bounds
        // checks and emits packed multiplies + adds.
        for j in 0..8 {
            acc[j] += a[base + j] * b[base + j];
        }
    }
    let mut s = acc[0] + acc[1] + acc[2] + acc[3] + acc[4] + acc[5] + acc[6] + acc[7];
    for i in (chunks * 8)..n {
        s += a[i] * b[i];
    }
    s
}

/// Scalar squared-L2 written the same way as `dot_product_scalar`.
#[inline]
fn l2_squared_scalar(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    let n = a.len().min(b.len());
    let chunks = n / 8;
    let mut acc = [0.0f32; 8];
    for i in 0..chunks {
        let base = i * 8;
        for j in 0..8 {
            let d = a[base + j] - b[base + j];
            acc[j] += d * d;
        }
    }
    let mut s = acc[0] + acc[1] + acc[2] + acc[3] + acc[4] + acc[5] + acc[6] + acc[7];
    for i in (chunks * 8)..n {
        let d = a[i] - b[i];
        s += d * d;
    }
    s
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "avx2,fma")]
unsafe fn dot_product_avx2(a: &[f32], b: &[f32]) -> f32 {
    #[cfg(target_arch = "x86")]
    use std::arch::x86::*;
    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64::*;

    debug_assert_eq!(a.len(), b.len());
    let n = a.len().min(b.len());
    let chunks = n / 8;

    // Four independent accumulators so we don't bottleneck on FMA
    // latency. For dim=128 we get exactly 16 8-lane chunks → 4 per
    // accumulator → pipeline stays full.
    let mut acc0 = _mm256_setzero_ps();
    let mut acc1 = _mm256_setzero_ps();
    let mut acc2 = _mm256_setzero_ps();
    let mut acc3 = _mm256_setzero_ps();

    let ap = a.as_ptr();
    let bp = b.as_ptr();
    let mut i = 0usize;
    while i + 32 <= chunks * 8 {
        let av0 = _mm256_loadu_ps(ap.add(i));
        let bv0 = _mm256_loadu_ps(bp.add(i));
        let av1 = _mm256_loadu_ps(ap.add(i + 8));
        let bv1 = _mm256_loadu_ps(bp.add(i + 8));
        let av2 = _mm256_loadu_ps(ap.add(i + 16));
        let bv2 = _mm256_loadu_ps(bp.add(i + 16));
        let av3 = _mm256_loadu_ps(ap.add(i + 24));
        let bv3 = _mm256_loadu_ps(bp.add(i + 24));
        acc0 = _mm256_fmadd_ps(av0, bv0, acc0);
        acc1 = _mm256_fmadd_ps(av1, bv1, acc1);
        acc2 = _mm256_fmadd_ps(av2, bv2, acc2);
        acc3 = _mm256_fmadd_ps(av3, bv3, acc3);
        i += 32;
    }
    while i + 8 <= chunks * 8 {
        let av = _mm256_loadu_ps(ap.add(i));
        let bv = _mm256_loadu_ps(bp.add(i));
        acc0 = _mm256_fmadd_ps(av, bv, acc0);
        i += 8;
    }

    // Horizontal reduction of the four 8-lane accumulators.
    let s01 = _mm256_add_ps(acc0, acc1);
    let s23 = _mm256_add_ps(acc2, acc3);
    let s = _mm256_add_ps(s01, s23);
    let hi = _mm256_extractf128_ps(s, 1);
    let lo = _mm256_castps256_ps128(s);
    let v128 = _mm_add_ps(lo, hi);
    let shuf = _mm_movehdup_ps(v128);
    let sums = _mm_add_ps(v128, shuf);
    let shuf2 = _mm_movehl_ps(sums, sums);
    let sums2 = _mm_add_ss(sums, shuf2);
    let mut s = _mm_cvtss_f32(sums2);

    for k in (chunks * 8)..n {
        s += a[k] * b[k];
    }
    s
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "avx2,fma")]
unsafe fn l2_squared_avx2(a: &[f32], b: &[f32]) -> f32 {
    #[cfg(target_arch = "x86")]
    use std::arch::x86::*;
    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64::*;

    debug_assert_eq!(a.len(), b.len());
    let n = a.len().min(b.len());
    let chunks = n / 8;

    let mut acc0 = _mm256_setzero_ps();
    let mut acc1 = _mm256_setzero_ps();
    let mut acc2 = _mm256_setzero_ps();
    let mut acc3 = _mm256_setzero_ps();

    let ap = a.as_ptr();
    let bp = b.as_ptr();
    let mut i = 0usize;
    while i + 32 <= chunks * 8 {
        let av0 = _mm256_loadu_ps(ap.add(i));
        let bv0 = _mm256_loadu_ps(bp.add(i));
        let d0 = _mm256_sub_ps(av0, bv0);
        let av1 = _mm256_loadu_ps(ap.add(i + 8));
        let bv1 = _mm256_loadu_ps(bp.add(i + 8));
        let d1 = _mm256_sub_ps(av1, bv1);
        let av2 = _mm256_loadu_ps(ap.add(i + 16));
        let bv2 = _mm256_loadu_ps(bp.add(i + 16));
        let d2 = _mm256_sub_ps(av2, bv2);
        let av3 = _mm256_loadu_ps(ap.add(i + 24));
        let bv3 = _mm256_loadu_ps(bp.add(i + 24));
        let d3 = _mm256_sub_ps(av3, bv3);
        acc0 = _mm256_fmadd_ps(d0, d0, acc0);
        acc1 = _mm256_fmadd_ps(d1, d1, acc1);
        acc2 = _mm256_fmadd_ps(d2, d2, acc2);
        acc3 = _mm256_fmadd_ps(d3, d3, acc3);
        i += 32;
    }
    while i + 8 <= chunks * 8 {
        let av = _mm256_loadu_ps(ap.add(i));
        let bv = _mm256_loadu_ps(bp.add(i));
        let d = _mm256_sub_ps(av, bv);
        acc0 = _mm256_fmadd_ps(d, d, acc0);
        i += 8;
    }

    let s01 = _mm256_add_ps(acc0, acc1);
    let s23 = _mm256_add_ps(acc2, acc3);
    let s = _mm256_add_ps(s01, s23);
    let hi = _mm256_extractf128_ps(s, 1);
    let lo = _mm256_castps256_ps128(s);
    let v128 = _mm_add_ps(lo, hi);
    let shuf = _mm_movehdup_ps(v128);
    let sums = _mm_add_ps(v128, shuf);
    let shuf2 = _mm_movehl_ps(sums, sums);
    let sums2 = _mm_add_ss(sums, shuf2);
    let mut s = _mm_cvtss_f32(sums2);

    for k in (chunks * 8)..n {
        let d = a[k] - b[k];
        s += d * d;
    }
    s
}

// ---------------------------------------------------------------------
// AVX-512 kernels (16-lane f32, FMA). Preferred over AVX2 when the CPU
// advertises avx512f. AVX-512 intrinsics are stable since Rust 1.89.
// For dim=128 there are exactly 8 16-lane chunks → 2 per accumulator
// over four accumulators, keeping the FMA pipeline full. Numerics match
// the AVX2/scalar paths to within FMA rounding.
// ---------------------------------------------------------------------

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "avx512f")]
unsafe fn dot_product_avx512(a: &[f32], b: &[f32]) -> f32 {
    #[cfg(target_arch = "x86")]
    use std::arch::x86::*;
    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64::*;

    debug_assert_eq!(a.len(), b.len());
    let n = a.len().min(b.len());
    let chunks = n / 16;

    let mut acc0 = _mm512_setzero_ps();
    let mut acc1 = _mm512_setzero_ps();
    let mut acc2 = _mm512_setzero_ps();
    let mut acc3 = _mm512_setzero_ps();

    let ap = a.as_ptr();
    let bp = b.as_ptr();
    let mut i = 0usize;
    while i + 64 <= chunks * 16 {
        let av0 = _mm512_loadu_ps(ap.add(i));
        let bv0 = _mm512_loadu_ps(bp.add(i));
        let av1 = _mm512_loadu_ps(ap.add(i + 16));
        let bv1 = _mm512_loadu_ps(bp.add(i + 16));
        let av2 = _mm512_loadu_ps(ap.add(i + 32));
        let bv2 = _mm512_loadu_ps(bp.add(i + 32));
        let av3 = _mm512_loadu_ps(ap.add(i + 48));
        let bv3 = _mm512_loadu_ps(bp.add(i + 48));
        acc0 = _mm512_fmadd_ps(av0, bv0, acc0);
        acc1 = _mm512_fmadd_ps(av1, bv1, acc1);
        acc2 = _mm512_fmadd_ps(av2, bv2, acc2);
        acc3 = _mm512_fmadd_ps(av3, bv3, acc3);
        i += 64;
    }
    while i + 16 <= chunks * 16 {
        let av = _mm512_loadu_ps(ap.add(i));
        let bv = _mm512_loadu_ps(bp.add(i));
        acc0 = _mm512_fmadd_ps(av, bv, acc0);
        i += 16;
    }

    let sum = _mm512_add_ps(_mm512_add_ps(acc0, acc1), _mm512_add_ps(acc2, acc3));
    let mut s = _mm512_reduce_add_ps(sum);

    for k in (chunks * 16)..n {
        s += a[k] * b[k];
    }
    s
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "avx512f")]
unsafe fn l2_squared_avx512(a: &[f32], b: &[f32]) -> f32 {
    #[cfg(target_arch = "x86")]
    use std::arch::x86::*;
    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64::*;

    debug_assert_eq!(a.len(), b.len());
    let n = a.len().min(b.len());
    let chunks = n / 16;

    let mut acc0 = _mm512_setzero_ps();
    let mut acc1 = _mm512_setzero_ps();
    let mut acc2 = _mm512_setzero_ps();
    let mut acc3 = _mm512_setzero_ps();

    let ap = a.as_ptr();
    let bp = b.as_ptr();
    let mut i = 0usize;
    while i + 64 <= chunks * 16 {
        let d0 = _mm512_sub_ps(_mm512_loadu_ps(ap.add(i)), _mm512_loadu_ps(bp.add(i)));
        let d1 = _mm512_sub_ps(
            _mm512_loadu_ps(ap.add(i + 16)),
            _mm512_loadu_ps(bp.add(i + 16)),
        );
        let d2 = _mm512_sub_ps(
            _mm512_loadu_ps(ap.add(i + 32)),
            _mm512_loadu_ps(bp.add(i + 32)),
        );
        let d3 = _mm512_sub_ps(
            _mm512_loadu_ps(ap.add(i + 48)),
            _mm512_loadu_ps(bp.add(i + 48)),
        );
        acc0 = _mm512_fmadd_ps(d0, d0, acc0);
        acc1 = _mm512_fmadd_ps(d1, d1, acc1);
        acc2 = _mm512_fmadd_ps(d2, d2, acc2);
        acc3 = _mm512_fmadd_ps(d3, d3, acc3);
        i += 64;
    }
    while i + 16 <= chunks * 16 {
        let d = _mm512_sub_ps(_mm512_loadu_ps(ap.add(i)), _mm512_loadu_ps(bp.add(i)));
        acc0 = _mm512_fmadd_ps(d, d, acc0);
        i += 16;
    }

    let sum = _mm512_add_ps(_mm512_add_ps(acc0, acc1), _mm512_add_ps(acc2, acc3));
    let mut s = _mm512_reduce_add_ps(sum);

    for k in (chunks * 16)..n {
        let d = a[k] - b[k];
        s += d * d;
    }
    s
}

// ---------------------------------------------------------------------
// NEON kernels (4-lane f32, FMA). aarch64 only. NEON is architecturally
// guaranteed on aarch64, so this is the SIMD path for Apple Silicon,
// Graviton, etc. For dim=128 there are 32 4-lane chunks → 8 per
// accumulator over four accumulators. Numerics match the other paths to
// within FMA rounding.
//
// NOTE: these kernels are NOT exercised on the x86_64 CI/dev hosts; they
// are verified by `cargo check --target aarch64-*` (intrinsic
// signatures + borrow/type check) and by mirroring the proven AVX2/
// scalar structure. No aarch64 wall-time measurement was taken.
// ---------------------------------------------------------------------

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn dot_product_neon(a: &[f32], b: &[f32]) -> f32 {
    use std::arch::aarch64::*;

    debug_assert_eq!(a.len(), b.len());
    let n = a.len().min(b.len());
    let chunks = n / 4;

    let mut acc0 = vdupq_n_f32(0.0);
    let mut acc1 = vdupq_n_f32(0.0);
    let mut acc2 = vdupq_n_f32(0.0);
    let mut acc3 = vdupq_n_f32(0.0);

    let ap = a.as_ptr();
    let bp = b.as_ptr();
    let mut i = 0usize;
    while i + 16 <= chunks * 4 {
        // vfmaq_f32(acc, x, y) = acc + x * y (fused).
        acc0 = vfmaq_f32(acc0, vld1q_f32(ap.add(i)), vld1q_f32(bp.add(i)));
        acc1 = vfmaq_f32(acc1, vld1q_f32(ap.add(i + 4)), vld1q_f32(bp.add(i + 4)));
        acc2 = vfmaq_f32(acc2, vld1q_f32(ap.add(i + 8)), vld1q_f32(bp.add(i + 8)));
        acc3 = vfmaq_f32(acc3, vld1q_f32(ap.add(i + 12)), vld1q_f32(bp.add(i + 12)));
        i += 16;
    }
    while i + 4 <= chunks * 4 {
        acc0 = vfmaq_f32(acc0, vld1q_f32(ap.add(i)), vld1q_f32(bp.add(i)));
        i += 4;
    }

    let sum = vaddq_f32(vaddq_f32(acc0, acc1), vaddq_f32(acc2, acc3));
    let mut s = vaddvq_f32(sum);

    for k in (chunks * 4)..n {
        s += a[k] * b[k];
    }
    s
}

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn l2_squared_neon(a: &[f32], b: &[f32]) -> f32 {
    use std::arch::aarch64::*;

    debug_assert_eq!(a.len(), b.len());
    let n = a.len().min(b.len());
    let chunks = n / 4;

    let mut acc0 = vdupq_n_f32(0.0);
    let mut acc1 = vdupq_n_f32(0.0);
    let mut acc2 = vdupq_n_f32(0.0);
    let mut acc3 = vdupq_n_f32(0.0);

    let ap = a.as_ptr();
    let bp = b.as_ptr();
    let mut i = 0usize;
    while i + 16 <= chunks * 4 {
        let d0 = vsubq_f32(vld1q_f32(ap.add(i)), vld1q_f32(bp.add(i)));
        let d1 = vsubq_f32(vld1q_f32(ap.add(i + 4)), vld1q_f32(bp.add(i + 4)));
        let d2 = vsubq_f32(vld1q_f32(ap.add(i + 8)), vld1q_f32(bp.add(i + 8)));
        let d3 = vsubq_f32(vld1q_f32(ap.add(i + 12)), vld1q_f32(bp.add(i + 12)));
        acc0 = vfmaq_f32(acc0, d0, d0);
        acc1 = vfmaq_f32(acc1, d1, d1);
        acc2 = vfmaq_f32(acc2, d2, d2);
        acc3 = vfmaq_f32(acc3, d3, d3);
        i += 16;
    }
    while i + 4 <= chunks * 4 {
        let d = vsubq_f32(vld1q_f32(ap.add(i)), vld1q_f32(bp.add(i)));
        acc0 = vfmaq_f32(acc0, d, d);
        i += 4;
    }

    let sum = vaddq_f32(vaddq_f32(acc0, acc1), vaddq_f32(acc2, acc3));
    let mut s = vaddvq_f32(sum);

    for k in (chunks * 4)..n {
        let d = a[k] - b[k];
        s += d * d;
    }
    s
}

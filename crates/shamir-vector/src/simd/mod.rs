//! SIMD distance kernels shared by vector adapters.
//!
//! Hot path: both brute-force search and HNSW graph traversal call
//! `dot_product` / `l2_squared` for every distance computation. For
//! typical 128-d f32 vectors that's a clean vectorizable inner loop.
//! We runtime-detect the widest available SIMD once and dispatch to it,
//! one predictable branch per call on already-hot memory. On x86/x86_64
//! the order is AVX-512F (16-lane FMA), then AVX2+FMA (8-lane), then
//! scalar; on aarch64 it is NEON (4-lane FMA), then scalar.
//!
//! The scalar fallback is itself written in a chunked, multi-accumulator
//! form the compiler reliably autovectorizes (SSE2 on x86 without AVX2,
//! NEON-less targets, WASM), so even the "slow" path gets compiler SIMD.
//! All paths return the same value to within FMA rounding.
//!
//! The dispatch flag is loaded with `Relaxed` from a `OnceLock`-cached
//! `bool` so the per-call overhead is one predictable branch on
//! already-hot memory. No locks. No new dependencies.
//!
//! Invariant: both kernels MUST return the same value for the same
//! inputs to within fp rounding (FMA differs from add-then-mul by
//! at most 0.5 ulp per op — within the existing test tolerances).

mod f32_kernels;
mod quantized;

pub(crate) use f32_kernels::dot_product;
#[cfg(all(test, any(target_arch = "x86", target_arch = "x86_64")))]
pub(crate) use f32_kernels::has_avx2;
pub(crate) use f32_kernels::l2_squared;
pub(crate) use quantized::weighted_bilinear_f32;
pub(crate) use quantized::weighted_linear_u8;
pub(crate) use quantized::weighted_sq_diff_u8;
#[cfg(test)]
pub(crate) use quantized::{dot_u8, dot_u8_scalar};
#[cfg(test)]
pub(crate) use quantized::{
    weighted_bilinear_scalar, weighted_linear_scalar, weighted_sq_diff_scalar,
};

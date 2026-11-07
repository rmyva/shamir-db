//! SQ8 quantized hot-path bench — `Sq8Quantizer::approx_dot` (called by
//! `ShamirDistU8::eval` for Dot/Cosine on every HNSW graph hop & insert) and
//! `ShamirDistU8::eval` itself for the three metrics.
//!
//! VR-7 (#429): the baseline `approx_dot` contained a dead `dot_u8(qx, qy)`
//! call whose result was computed and discarded — an extra O(dim) SIMD pass
//! on every edge evaluation. This bench pins baseline vs post-fix throughput.
//!
//! Inputs: a clustered dim-128 dataset (matches typical embedding shape)
//! quantized through a fit-on-training `Sq8Quantizer`. Each iter scores a
//! query code against a fixed candidate pool, simulating one HNSW hop batch.
//!
//! Migrated to the fixed-iteration harness (`bench_scale_tool`): setup
//! (quantizer, pool codes, query code) is built ONCE outside the timed
//! closure, exactly as under Criterion's `b.iter` — plan 1 (shared setup).

use std::hint::black_box;
use std::sync::Arc;

use bench_scale_tool::Harness;
use hnsw_rs::anndists::dist::distances::Distance;
use shamir_bench_utils::vector_data::clustered_vectors;
use shamir_index_core::kind::VectorMetric;
use shamir_vector::quantized_dist::{RescoreCtx, ShamirDistU8};
use shamir_vector::sq8::Sq8Quantizer;

/// Embedding dimensionality for the bench (typical small-model shape).
const DIM: usize = 128;
/// Candidate pool size per query — approximates one HNSW layer-0 hop batch
/// (M=16, ef ~ a few hundred). 256 keeps the cell wall-clock modest.
const POOL: usize = 256;
/// Training-set size for the quantizer fit (>= FIT_THRESHOLD so the params
/// are non-degenerate on a realistic spread).
const N_TRAIN: usize = 1024;

fn main() {
    let mut h = Harness::new("sq8_hot_path", env!("CARGO_MANIFEST_DIR"));

    // --- sq8_approx_dot/dim_128 ---------------------------------------------
    {
        let ds = clustered_vectors(N_TRAIN, DIM, 32, 0.15, 4242);
        let training: Vec<Vec<f32>> = ds.vectors.iter().map(<[f32]>::to_vec).collect();
        let q = Sq8Quantizer::fit(&training, DIM);
        let pool_codes: Vec<Vec<u8>> = ds
            .vectors
            .iter()
            .take(POOL)
            .map(|v| q.quantize(v))
            .collect();
        let query_code = q.quantize(&ds.centroids[0]);

        h.bench("sq8_approx_dot/dim_128", move || {
            let mut acc = 0.0f32;
            for cand in &pool_codes {
                acc += black_box(q.approx_dot(black_box(&query_code), black_box(cand)));
            }
            black_box(acc);
        });
    }

    // --- shamir_dist_u8_eval/{L2,Dot,Cosine}/dim_128 ------------------------
    {
        let ds = clustered_vectors(N_TRAIN, DIM, 32, 0.15, 4242);
        let training: Vec<Vec<f32>> = ds.vectors.iter().map(<[f32]>::to_vec).collect();
        let q = Arc::new(Sq8Quantizer::fit(&training, DIM));
        let pool_codes: Vec<Vec<u8>> = ds
            .vectors
            .iter()
            .take(POOL)
            .map(|v| q.quantize(v))
            .collect();
        let query_code = q.quantize(&ds.centroids[0]);

        for metric in [VectorMetric::L2, VectorMetric::Dot, VectorMetric::Cosine] {
            let dist = ShamirDistU8::new(Arc::clone(&q), metric);
            let pool_codes = pool_codes.clone();
            let query_code = query_code.clone();
            let id = format!("shamir_dist_u8_eval/{metric:?}/dim_128");
            h.bench(&id, move || {
                let mut acc = 0.0f32;
                for cand in &pool_codes {
                    acc += black_box(dist.eval(black_box(&query_code), black_box(cand)));
                }
                black_box(acc);
            });
        }
    }

    // --- rescore_old_dequant vs rescore_fused (audit finding 4.1, #530) -----
    //
    // Directly pits the OLD per-candidate `dequantize()`-then-SIMD-dot rescore
    // against the NEW fused, allocation-free `RescoreCtx` over the SAME
    // candidate pool. Both compute the exact f32 distance per candidate; the
    // fused path precomputes `qm`/`qs`/`q_norm` ONCE and streams the u8 codes
    // with zero per-candidate heap allocation. Cosine is the worst case (it
    // also recomputed the query's own norm per candidate in the old path).
    {
        let ds = clustered_vectors(N_TRAIN, DIM, 32, 0.15, 4242);
        let training: Vec<Vec<f32>> = ds.vectors.iter().map(<[f32]>::to_vec).collect();
        let q = Sq8Quantizer::fit(&training, DIM);
        let pool_codes: Vec<Vec<u8>> = ds
            .vectors
            .iter()
            .take(POOL)
            .map(|v| q.quantize(v))
            .collect();
        // The rescore query is the ORIGINAL f32 vector (as in the search path).
        let query_f32: Vec<f32> = ds.centroids[0].to_vec();

        for metric in [VectorMetric::L2, VectorMetric::Dot, VectorMetric::Cosine] {
            // OLD: dequantize each candidate into a fresh Vec, then score.
            // The SIMD kernels are `pub(crate)` (not reachable from a bench
            // target), so we mirror them with a plain scalar dot/L2 over the
            // freshly-allocated dequant vector — the defining OLD cost is the
            // per-candidate `dequantize()` heap allocation plus, for Cosine,
            // the redundant per-candidate query-norm, both of which this
            // reproduces faithfully.
            {
                let q = q.clone();
                let pool = pool_codes.clone();
                let query = query_f32.clone();
                let id = format!("rescore_old_dequant/{metric:?}/dim_128");
                let dot = |a: &[f32], b: &[f32]| -> f32 {
                    let mut s = 0.0f32;
                    for i in 0..a.len() {
                        s += a[i] * b[i];
                    }
                    s
                };
                h.bench(&id, move || {
                    let mut acc = 0.0f32;
                    for cand in &pool {
                        let dequant = q.dequantize(cand); // fresh Vec<f32> per candidate
                        let d = match metric {
                            VectorMetric::L2 => {
                                let mut s = 0.0f32;
                                for i in 0..query.len() {
                                    let x = query[i] - dequant[i];
                                    s += x * x;
                                }
                                s.sqrt()
                            }
                            VectorMetric::Dot => (1.0 - dot(&query, &dequant)).max(0.0),
                            VectorMetric::Cosine => {
                                let d = dot(&query, &dequant);
                                let na = dot(&query, &query); // recomputed per candidate (OLD)
                                let nb = dot(&dequant, &dequant);
                                if na < 1e-18 || nb < 1e-18 {
                                    1.0
                                } else {
                                    (1.0 - d / (na * nb).sqrt()).max(0.0)
                                }
                            }
                        };
                        acc += black_box(d);
                    }
                    black_box(acc);
                });
            }
            // NEW: fused RescoreCtx built once, scored per candidate (no alloc).
            {
                let q = q.clone();
                let pool = pool_codes.clone();
                let query = query_f32.clone();
                let id = format!("rescore_fused/{metric:?}/dim_128");
                h.bench(&id, move || {
                    let ctx = RescoreCtx::new(metric, &q, &query);
                    let mut acc = 0.0f32;
                    for cand in &pool {
                        acc += black_box(ctx.score(black_box(cand)));
                    }
                    black_box(acc);
                });
            }
        }
    }

    h.run();
}

use crate::simd::{dot_product, l2_squared};
use hnsw_rs::anndists::dist::distances::Distance;
use shamir_index_core::kind::VectorMetric;

/// Maximum allowed top-k value. Untrusted `k` near `u32::MAX` would drive
/// `overscan*2+10` and `Vec::with_capacity(k+16)` to multi-GB allocation.
///
/// P3 / V3.1: `pub` so the engine-side filtered-ANN retry loop
/// (`read_filtered_vector_scan`) can clamp its widening `k′ = k × oversample`
/// against the same bound the adapter enforces internally. Without a shared
/// cap the retry could request `k′` far above what the adapter accepts, the
/// adapter would silently truncate, and the retry would loop forever
/// (post-filter always < k because the adapter never returned k′ candidates).
pub const MAX_TOPK: u32 = 10_000;

/// Maximum allowed per-query `ef_search` value. Untrusted `ef` near
/// `u32::MAX` would drive `hnsw.search(query, overscan, ef)` to explore an
/// enormous graph fan-out (CPU-bound `spawn_blocking` holding the rayon pool).
/// Clamped (NOT rejected) at this cap — a huge `ef` behaves identically to
/// `MAX_EF_SEARCH` for recall but cannot starve the worker pool.
///
/// 10_000 matches `MAX_TOPK`: `ef >= k` is the standard HNSW guidance, so
/// capping `ef` at the same bound as `k` keeps the knobs consistent. Real
/// recall gains plateau well below this (typical sweet spots: 50–500).
pub const MAX_EF_SEARCH: u32 = 10_000;

/// Live-element count at or below which `search` runs an EXACT brute-force
/// scan instead of the approximate HNSW graph.
///
/// `hnsw_rs` 0.3.5 seeds its layer generator with the fixed seed 397.
/// HNSW is still approximate: tiny graphs can have recall below 100%, and
/// concurrent insertion order can change topology and query neighbours
/// across builds (and across reopen). On a handful of points that surfaces as
/// flaky / wrong top-k. Brute-force over a few hundred vectors is microseconds
/// and GUARANTEES exact, stable results; HNSW only earns its keep at larger N
/// where the graph is well-connected and recall is reliable. 256 keeps small
/// indexes (and the exact-assertion tests) deterministic while leaving the
/// recall-tolerance tests (≥1k vectors) on the HNSW path.
pub(super) const BRUTE_FORCE_MAX: usize = 256;
#[derive(Debug, Clone, Copy)]
pub struct ShamirDist {
    pub(crate) metric: VectorMetric,
}

impl Distance<f32> for ShamirDist {
    fn eval(&self, a: &[f32], b: &[f32]) -> f32 {
        // Route through the shared SIMD kernels (AVX2+FMA when available,
        // chunked-scalar fallback). `hnsw_rs` calls `eval` for every
        // distance computation during graph traversal and insertion —
        // this is the production hot path. Semantics are preserved
        // bit-for-bit-modulo-FMA-rounding (kernels match the original
        // sum/zip semantics; FMA differs by at most 0.5 ulp per op,
        // within existing test tolerances).
        match self.metric {
            VectorMetric::L2 => l2_squared(a, b).sqrt(),
            VectorMetric::Cosine => {
                let dot = dot_product(a, b);
                let na = dot_product(a, a).sqrt();
                let nb = dot_product(b, b).sqrt();
                if na < 1e-9 || nb < 1e-9 {
                    return 1.0;
                }
                // `.max(0.0)`: Cauchy-Schwarz gives `dot <= na*nb` so the
                // distance is >= 0 mathematically, but f32/FMA rounding in the
                // separately-computed `dot` and `na*nb` can yield a tiny
                // negative — which trips hnsw_rs's strict non-negative-distance
                // assertion. Clamping only ever corrects that rounding artifact
                // (a negative cosine distance is never legitimate); all valid
                // values pass through unchanged. Mirrors the `Dot` arm below.
                (1.0 - dot / (na * nb)).max(0.0)
            }
            VectorMetric::Dot => {
                // HNSW requires non-negative distances. For normalized
                // vectors, dot ∈ [-1, 1] and dist = 1 - dot ∈ [0, 2]
                // preserves the search ordering. Callers must normalize
                // their vectors for correct top-k with `Dot`.
                let dot = dot_product(a, b);
                (1.0 - dot).max(0.0)
            }
        }
    }
}

#[derive(Clone)]
pub struct HnswConfig {
    pub max_elements: usize,
    pub m: usize,
    pub max_layer: usize,
    pub ef_construction: usize,
    pub ef_search: usize,
}

impl Default for HnswConfig {
    fn default() -> Self {
        Self {
            max_elements: 10_000,
            m: 16,
            max_layer: 16,
            ef_construction: 200,
            ef_search: 50,
        }
    }
}

/// At or above this live-element count a `quantization == Some(Sq8)`
/// adapter fits the SQ8 quantizer and builds the u8 graph. Deliberately
/// equal to [`BRUTE_FORCE_MAX`]: below the threshold the exact brute-force
/// path over f32 vectors is cheaper than a graph traversal AND 100%
/// recall, so quantizing earlier would only lose accuracy for no gain.
pub(super) const FIT_THRESHOLD: usize = BRUTE_FORCE_MAX;

/// Catch-up convergence loop (`try_fit_and_rebuild`) backoff.
///
/// The loop waits for in-flight pre-flip upserts to land their codes in
/// `vectors_u8`. Convergence normally takes 1–2 iterations, so the first
/// [`CATCHUP_SPIN_BUDGET`] iterations use a zero-latency cooperative
/// [`tokio::task::yield_now`]. Past the budget we are genuinely waiting on
/// other tasks whose CPU-bound graph inserts are parked on the blocking
/// pool; continuing to hot-spin `yield_now` keeps the fitter's worker
/// thread permanently runnable, so under machine-wide CPU saturation (e.g.
/// the full nextest suite pinning every core) the OS keeps scheduling the
/// spinner and STARVES the very blocking threads it is waiting for — a
/// self-inflicted priority inversion that stretched convergence from
/// microseconds to the 180 s test-kill. Past the budget we therefore park
/// on a short [`CATCHUP_BACKOFF`] sleep so the fitter yields its core to
/// the tasks it depends on. Convergence semantics are unchanged — only the
/// wait primitive between checks differs.
pub(super) const CATCHUP_SPIN_BUDGET: u32 = 32;

/// Sleep duration once [`CATCHUP_SPIN_BUDGET`] is exhausted. Small enough to
/// keep convergence latency negligible (worst case is bounded by the number
/// of in-flight upserts, each landing within a few backoffs), large enough
/// that the fitter actually relinquishes its core to the blocking pool.
pub(super) const CATCHUP_BACKOFF: std::time::Duration = std::time::Duration::from_micros(100);

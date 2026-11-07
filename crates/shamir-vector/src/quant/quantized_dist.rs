//! Quantized distance function for `Hnsw<'static, u8, ShamirDistU8>`.
//!
//! One primary export: [`ShamirDistU8`] — a [`Distance<u8>`] implementation
//! that scores pairs of SQ8-quantized code vectors using the frozen
//! [`Sq8Quantizer`] parameters (per-dimension `min_i`/`scale_i`).
//!
//! ## Why a custom distance?
//!
//! `hnsw_rs`'s built-in `DistL2`/`DistL1` for `u8` compute an *integer* L1/L2
//! with uniform per-dimension weight 1. SQ8 codes decode to
//! `x_i ≈ min_i + q_i · s_i`, where `s_i` varies per dimension — so the
//! *true* distance on the dequantized vectors has per-dimension `s_i²`
//! weights that the built-in distances ignore. [`ShamirDistU8`] applies them.
//!
//! ## `eval` correctness (per-metric)
//!
//! Let `x_i = min_i + qx_i·s_i`, `y_i = min_i + qy_i·s_i` be the exact
//! dequantized values of two code vectors. Then:
//!
//! * **L2** — [`Sq8Quantizer::approx_l2_sq`] computes
//!   `Σ_i s_i²·(qx_i − qy_i)² = Σ_i (x_i − y_i)²` exactly on the dequantized
//!   vectors (the `min_i` cancels). We return `sqrt(...)` to match the
//!   convention of the f32 [`ShamirDist`](crate::hnsw_adapter::ShamirDist)
//!   (which also takes `sqrt` of `l2_squared`).
//! * **Dot** — [`Sq8Quantizer::approx_dot`] expands
//!   `x·y = Σ min_i² + Σ min_i·s_i·(qx_i + qy_i) + Σ s_i²·qx_i·qy_i`,
//!   which is exactly `Σ x_i·y_i` on the dequantized vectors. We return
//!   `1 − dot` (clamped to `≥ 0`) to preserve HNSW's non-negative-distance
//!   invariant and the search ordering (callers normalize for `Dot`).
//! * **Cosine** — same as Dot but normalized by the dequantized vector
//!   norms `||x||·||y||`. We compute `1 − dot/(||x||·||y||)` with
//!   `||x||² = Σ x_i²` derived from the codes (no retained f32).
//!
//! In all three cases the value returned is **exactly** the corresponding
//! f32 distance on the *dequantized* code vectors — the only approximation
//! is SQ8 quantization itself (half a step per dimension, bounded by
//! `scale_i/2`). See the unit tests in `tests/quantized_dist_tests.rs`.
//!
//! ## Concurrency
//!
//! [`ShamirDistU8`] holds an `Arc<Sq8Quantizer>`; `hnsw_rs` clones the
//! `Distance` for rayon thread-locals, and `Arc::clone` is cheap. The
//! quantizer params are frozen at fit time and never mutated thereafter,
//! so `eval` is deterministic and lock-free.
//!
//! ## Cosine norm cache — VR-7 (#429), architecture analysis
//!
//! The Cosine arm of [`ShamirDistU8::eval`] recomputes the dequantized
//! vector norm `||x||² = Σ (min_i + q_i·s_i)²` for BOTH code slices `a`
//! and `b` on every call — O(dim) twice per HNSW hop (once for the query
//! code, once for the candidate). At scale this dominates the Cosine
//! traversal: the baseline bench (`benches/sq8_hot_path.rs`,
//! `shamir_dist_u8_eval/Cosine/128`) shows Cosine ~3.5× slower than L2
//! (~243 µs vs ~69 µs over a 256-candidate pool at dim=128), entirely
//! accounted for by the two extra `dequant_norm_sq` passes per eval.
//!
//! The obvious fix — cache `||x||²` per internal-id, populated at insert
//! time next to `vectors_u8` — **cannot reach `eval`** without rewriting
//! the `hnsw_rs` integration. `hnsw_rs::Distance<T>::eval(&self, a: &[T],
//! b: &[T])` is symmetric and ID-blind: the graph stores its own
//! `Vec<u8>` per `Arc<Point>` internally and passes the *slice* to `eval`
//! on every hop; the internal-id (the `usize` we published at insert) is
//! never threaded into `eval`. So a per-internal-id cache in
//! [`HnswAdapter`](crate::hnsw_adapter::HnswAdapter) (parallel to
//! `vectors_u8`) is invisible to the Cosine hot path — `eval` cannot look
//! it up because it does not know which internal-id it is scoring.
//!
//! ### Options considered
//!
//! 1. **Per-internal-id cache in the adapter** (parallel to `vectors_u8`,
//!    populated at `claim_and_publish_u8` / fit / backfill / delete).
//!    Correctness-wise clean and RCU-friendly (lock-free `scc::HashMap`),
//!    but **does not speed up `eval`** — `eval` cannot read it. Would only
//!    help if we abandoned `hnsw_rs`'s built-in traversal for an
//!    in-house graph walk that threads internal-ids into the scorer.
//!    Rejected for this task (out of scope: would rewrite the HNSW
//!    integration).
//!
//! 2. **Query-norm hoist**: compute the query code's norm ONCE per search
//!    (in `search_quantized_graph`) and stash it on `ShamirDistU8` for the
//!    duration of the single `hnsw_u8.search(...)` call. Cuts ONE of the
//!    two `dequant_norm_sq` passes per eval (the query side). BUT
//!    `ShamirDistU8` is shared (`Arc<Hnsw>` holds one `Distance`, cloned
//!    per rayon thread-local) and `hnsw_u8.search` is re-entrant under
//!    concurrent queries on the same thread-local clone — a per-search
//!    stash needs a stack of (`query_ptr` → norm) entries to be safe, and
//!    even then the candidate-side norm (the larger cost — there are many
//!    candidates, one query) is untouched. At most a ~2× win on the query
//!    term; ~1.75× on the full Cosine eval. Feasible but partial.
//!
//! 3. **Pointer-keyed norm cache** (`DashMap<*const u8 as usize, f32>`)
//!    inside `ShamirDistU8`: keys are the slice pointers `hnsw_rs` hands
//!    to `eval`. Both the per-node `Arc<Point>` data (stable for the
//!    graph's lifetime — `hnsw_rs` never moves or reallocates a published
//!    point) and our own per-search `query_codes` `Vec` have stable
//!    pointers across the traversal. This is the ONLY option that reaches
//!    BOTH the query and candidate norms from inside `eval` without
//!    touching `hnsw_rs`. Risks: (a) the cache leaks one entry per
//!    DISTINCT query pointer over the adapter's lifetime (slow, unbounded
//!    growth proportional to the number of distinct queries issued —
//!    needs an LRU/size cap); (b) keying on raw pointer values couples us
//!    to `hnsw_rs`'s internal storage layout (a future `hnsw_rs` version
//!    that reallocates the per-point `Vec` would silently return stale
//!    norms — a correctness hazard with no compile-time guard). Defer to
//!    a dedicated task that can add the eviction policy and a
//!    pointer-stability invariant test.
//!
//! 4. **Fold the norm into the code vector** (e.g. SQ8 + 1 trailing byte
//!    encoding a precomputed norm bucket): changes the wire/snapshot
//!    format and the quantizer contract — a much larger refactor that
//!    affects `Sq8Quantizer`, the v2 snapshot sidecar, and every caller.
//!    Out of scope.
//!
//! ### Recommendation
//!
//! A full norm cache is a **separate task**: it is not a surgical hot-path
//! edit. Option 2 (query-norm hoist) is the smallest safe win and could
//! land as a follow-up; option 3 is the largest win but needs an eviction
//! policy + a pointer-stability guard. This module ships VR-7 fix #1
//! (dead `dot_u8` removal in [`Sq8Quantizer::approx_dot`]) which speeds
//! up the Dot AND Cosine arms (both call `approx_dot`); the Cosine
//! norm-cache work is tracked as the recommendation above. The bench
//! (`benches/sq8_hot_path.rs`) pins the current Cosine throughput so the
//! follow-up can show its improvement.

use crate::simd::dot_product;
use crate::sq8::Sq8Quantizer;
use hnsw_rs::anndists::dist::distances::Distance;
use shamir_index_core::kind::VectorMetric;
use std::cell::RefCell;
use std::sync::Arc;

// VR-7 option 2 (#429, F4 item 4): thread-local stack of
// `(query_ptr → dequant_norm_sq)` entries for the Cosine graph-traversal
// path.
//
// `hnsw_rs::Distance::eval(&self, a, b)` is symmetric — it doesn't know
// which of `a`/`b` is "the query". But `hnsw_rs::search` always passes the
// query as the FIRST argument to `eval` (verified from
// `hnsw_rs-0.3.4/src/hnsw.rs` `search_filter`/`search_layer`), and a single
// `.search()` call is single-threaded (sequential layer traversal with no
// internal rayon — only `parallel_search` dispatches across rayon workers,
// each running one query's `search` on one thread; see
// `hnsw.rs:1475-1589`). So a thread-local stack pushed before `.search()`
// and popped after is safe: during the search, `eval` sees the query's norm
// on top of the stack; after the search, the stack is empty.
//
// On a soft-miss (empty stack or pointer mismatch), `eval` recomputes the
// norm — NO panic, NO stale value. This matches the module doc's "option 2"
// recommendation and degrades gracefully, like the other opt-in caches in
// this campaign (`CondCache`/`FieldPathCache`/`QueryRefCache`).
//
// The stack (not a single cell) supports reentrancy: if `eval` somehow
// triggered a nested search (it does not today, but defense-in-depth), the
// inner search pushes a new entry on top and pops it on return, leaving the
// outer entry intact.
thread_local! {
    static QUERY_NORM_STACK: RefCell<Vec<(usize, f32)>> = const { RefCell::new(Vec::new()) };
}

/// RAII guard that pops the top entry from [`QUERY_NORM_STACK`] on drop,
/// ensuring the stack is always cleaned up even if the bracketed `search`
/// panics.
///
/// Created by [`ShamirDistU8::push_query_norm`]; do not construct directly.
pub(crate) struct QueryNormGuard {
    _private: (),
}

impl Drop for QueryNormGuard {
    fn drop(&mut self) {
        QUERY_NORM_STACK.with(|s| {
            s.borrow_mut().pop();
        });
    }
}

/// Quantized distance function holding frozen SQ8 quantizer params.
///
/// `eval(&[u8], &[u8]) -> f32` computes the per-metric distance on two SQ8
/// code vectors, expanding them through the frozen `min_i`/`scale_i` to
/// match the f32 distance on the dequantized vectors exactly (see the module
/// doc for the per-metric proofs).
#[derive(Clone)]
pub struct ShamirDistU8 {
    /// Frozen quantizer parameters — shared (via `Arc`) with the adapter
    /// and every rayon thread-local clone of this distance.
    params: Arc<Sq8Quantizer>,
    metric: VectorMetric,
}

impl ShamirDistU8 {
    /// Build a distance function from frozen quantizer params + metric.
    ///
    /// `params` are produced by [`Sq8Quantizer::fit`] at fit time and are
    /// read-only for the lifetime of this distance (and every clone).
    pub fn new(params: Arc<Sq8Quantizer>, metric: VectorMetric) -> Self {
        Self { params, metric }
    }

    /// Read-only access to the frozen quantizer (for introspection / snapshot).
    pub fn quantizer(&self) -> &Arc<Sq8Quantizer> {
        &self.params
    }

    /// The metric this distance scores under.
    pub fn metric(&self) -> VectorMetric {
        self.metric
    }

    /// VR-7 option 2 (#429, F4 item 4): push the query's precomputed
    /// dequant norm onto the thread-local stack, keyed by the query code
    /// slice's pointer address. Returns a [`QueryNormGuard`] that pops the
    /// entry on drop (panic-safe).
    ///
    /// Call this immediately before `hnsw_u8.search(&query_codes, ...)`
    /// (inside the `spawn_blocking` closure, on the same thread that runs
    /// `eval`), and let the guard drop immediately after.
    ///
    /// The pointer is taken from `query_codes.as_ptr()`, which is stable
    /// across the move into the closure (moving a `Vec` transfers the heap
    /// allocation without copying) and stable for the duration of the
    /// single-threaded `search` call.
    #[inline]
    pub(crate) fn push_query_norm(query_codes: &[u8], norm: f32) -> QueryNormGuard {
        let ptr = query_codes.as_ptr() as usize;
        QUERY_NORM_STACK.with(|s| s.borrow_mut().push((ptr, norm)));
        QueryNormGuard { _private: () }
    }

    /// VR-7 option 2 (#429, F4 item 4): look up the cached norm for a code
    /// slice whose pointer matches the top of the thread-local stack.
    /// Returns `None` on a soft-miss (empty stack or pointer mismatch) —
    /// callers MUST fall back to recomputing
    /// [`Sq8Quantizer::dequant_norm_sq`].
    #[inline]
    fn try_query_norm(codes: &[u8]) -> Option<f32> {
        let ptr = codes.as_ptr() as usize;
        QUERY_NORM_STACK.with(|s| {
            let stack = s.borrow();
            stack
                .last()
                .and_then(|&(p, n)| if p == ptr { Some(n) } else { None })
        })
    }

    /// Squared L2 norm of the dequantized vector for a code vector:
    /// `||x||² = Σ (min_i + q_i·s_i)²`. Used by the Cosine metric.
    ///
    /// Delegates to the canonical SIMD-backed
    /// [`Sq8Quantizer::dequant_norm_sq`](crate::sq8::Sq8Quantizer::dequant_norm_sq)
    /// (F4): the norm expands to `min_sq_sum + weighted_bilinear_f32(
    /// min_scale, scales_sq, q, q)` — the existing bilinear SIMD kernel with
    /// `q` passed as both operands. See the module-level "Cosine norm cache"
    /// architecture section for why a per-internal-id norm cache cannot reach
    /// `eval` without rewriting the `hnsw_rs` integration.
    #[inline]
    fn dequant_norm_sq(&self, q: &[u8]) -> f32 {
        self.params.dequant_norm_sq(q)
    }
}

impl Distance<u8> for ShamirDistU8 {
    fn eval(&self, a: &[u8], b: &[u8]) -> f32 {
        match self.metric {
            // L2: sqrt(Σ s_i²·(a_i − b_i)²) == sqrt(Σ (x_i − y_i)²) on the
            // dequantized vectors (min_i cancels). Matches ShamirDist::L2
            // which also returns sqrt(l2_squared).
            VectorMetric::L2 => self.params.approx_l2_sq(a, b).sqrt(),

            // Dot: 1 − (x·y), clamped to ≥ 0. HNSW requires non-negative
            // distances; for normalized inputs dot ∈ [−1, 1] → dist ∈ [0, 2].
            // Matches ShamirDist::Dot semantics.
            VectorMetric::Dot => {
                let dot = self.params.approx_dot(a, b);
                (1.0 - dot).max(0.0)
            }

            // Cosine: 1 − (x·y)/(||x||·||y||). Norms computed on the
            // dequantized vectors from the codes (no retained f32). Matches
            // ShamirDist::Cosine, including the near-zero-norm guard.
            //
            // The result is clamped to ≥ 0: SQ8 quantization noise can make
            // `approx_dot` exceed `||x||·||y||` on some code pairs (the
            // integer core Σ qx_i·qy_i is exact, but the s_i²/min_i/s_i
            // expansion introduces rounding), yielding a slightly-negative
            // cosine distance. HNSW requires non-negative distances
            // (`hnsw_rs` asserts `c.dist_to_ref <= 0` for candidates, which
            // are stored as `-eval`; a negative eval flips the sign and
            // trips the assertion). The clamp preserves the search
            // ordering (the true cosine distance is in [0, 2]).
            VectorMetric::Cosine => {
                let dot = self.params.approx_dot(a, b);
                // VR-7 option 2 (#429, F4 item 4): the query's norm is hoisted
                // onto a thread-local stack by `search_quantized_graph`/
                // `search_cofilter_quantized` before the `hnsw_u8.search`
                // call. `hnsw_rs` always passes the query as the FIRST
                // argument to `eval`, so `try_query_norm(a)` hits on the
                // graph-traversal path. `try_query_norm(b)` misses (candidate
                // pointers don't match the query's) and falls back to
                // recompute — the candidate norm cannot be hoisted. On a
                // soft-miss (empty stack, e.g. standalone `eval` calls outside
                // a search) both sides recompute.
                let na_sq = Self::try_query_norm(a).unwrap_or_else(|| self.dequant_norm_sq(a));
                let nb_sq = Self::try_query_norm(b).unwrap_or_else(|| self.dequant_norm_sq(b));
                if na_sq < 1e-18 || nb_sq < 1e-18 {
                    return 1.0;
                }
                let denom = (na_sq * nb_sq).sqrt();
                (1.0 - dot / denom).max(0.0)
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Rescoring: dequantize codes and score with the EXACT f32 distance.
// ---------------------------------------------------------------------------
//
// The graph traversal returns `overscan = 2k+10` candidates ranked by the
// *approximate* code distance. The adapter then rescores each candidate by
// dequantizing its codes and computing the *exact* f32 distance to the
// original (unquantized) query. This recovers the ~2% recall the approximate
// code distance loses, at a cost of O(dim·(2k+10)) — negligible for k=10,
// dim=128 (~6 µs of SIMD f32 work).
//
// We route the rescore through the SAME SIMD kernels (`dot_product` /
// `l2_squared`) and the SAME metric convention (`sqrt` for L2,
// `1 − dot` for Dot, `1 − dot/(||x||·||y||)` for Cosine) as the f32
// `ShamirDist`, so a rescored candidate's distance is directly comparable
// to an f32-path distance. The unit tests pin this equality.

/// Audit finding 4.1 (task #530) — fused, allocation-free rescore context.
///
/// The old `rescore_f32` dequantized each candidate's codes into a FRESH
/// `Vec<f32>` (dim × 4 bytes) per candidate and, for Cosine, recomputed the
/// query's own norm `dot(query, query)` on EVERY candidate — even though it is
/// identical across the whole search. With an overscan of `16k+64` candidates
/// that is hundreds of heap allocations plus redundant query-norm passes per
/// search.
///
/// This context precomputes the query-dependent constants ONCE per query:
///
/// * `qm  = Σ query[i] · min_i`     (the constant part of `dot(query, x)`)
/// * `qs[i] = query[i] · scale_i`   (the per-dimension code multiplier)
/// * `q_norm = Σ query[i]²`         (Cosine only — the query's own norm)
///
/// Then, per candidate, the dot product decomposes as
/// `dot(query, dequant(codes)) = qm + Σ qs[i] · codes[i]` — a SINGLE pass over
/// the u8 codes (u8→f32 convert + multiply-accumulate) with ZERO per-candidate
/// heap allocation. L2 is computed the same way, `Σ (query[i] − min_i −
/// codes[i]·s_i)²`, also in one alloc-free pass. The result is numerically
/// equivalent (within f32 rounding) to the old dequant-then-SIMD path.
pub struct RescoreCtx<'a> {
    metric: VectorMetric,
    params: &'a Sq8Quantizer,
    /// `Σ query[i] · min_i` — the constant term of `dot(query, dequant)`.
    qm: f32,
    /// `query[i] · scale_i` — per-dimension code multiplier.
    qs: Vec<f32>,
    /// `Σ query[i]²` — the query's own squared norm (Cosine only; 0 otherwise).
    q_norm: f32,
    /// Cached `query` slice for the L2 single-pass (`query[i] − min_i − …`).
    query: &'a [f32],
}

impl<'a> RescoreCtx<'a> {
    /// Precompute the per-query constants ONCE. `O(dim)`, one allocation
    /// (`qs`), reused across every candidate in the search.
    ///
    /// # Panics
    ///
    /// Panics if `query.len() != params.dim()`.
    pub fn new(metric: VectorMetric, params: &'a Sq8Quantizer, query: &'a [f32]) -> Self {
        assert_eq!(
            query.len(),
            params.dim(),
            "RescoreCtx::new: query len {} != dim {}",
            query.len(),
            params.dim()
        );
        let mins = params.mins();
        let scales = params.scales();
        let dim = params.dim();
        let mut qm = 0.0f32;
        let mut qs = Vec::with_capacity(dim);
        // Cosine needs the query's own norm; skip the extra work otherwise.
        let q_norm = if matches!(metric, VectorMetric::Cosine) {
            dot_product(query, query)
        } else {
            0.0
        };
        // Indexes three parallel slices (query, mins, scales) by the same i.
        #[allow(clippy::needless_range_loop)]
        for i in 0..dim {
            qm += query[i] * mins[i];
            qs.push(query[i] * scales[i]);
        }
        Self {
            metric,
            params,
            qm,
            qs,
            q_norm,
            query,
        }
    }

    /// Score one candidate's u8 codes against the precomputed query, with NO
    /// per-candidate heap allocation. Returns the exact f32 distance under the
    /// context's metric convention (`sqrt` for L2, `1 − dot` for Dot,
    /// `1 − dot/(‖x‖·‖y‖)` for Cosine), matching
    /// [`ShamirDist`](crate::hnsw_adapter::ShamirDist).
    ///
    /// # Panics
    ///
    /// Panics if `codes.len() != params.dim()`.
    pub fn score(&self, codes: &[u8]) -> f32 {
        assert_eq!(
            codes.len(),
            self.params.dim(),
            "RescoreCtx::score: codes len {} != dim {}",
            codes.len(),
            self.params.dim()
        );
        match self.metric {
            VectorMetric::L2 => {
                // Σ (query[i] − dequant_i)² where dequant_i = min_i + codes_i·s_i.
                let mins = self.params.mins();
                let scales = self.params.scales();
                let mut acc = 0.0f32;
                // Indexes parallel slices (query, mins, scales, codes) by i.
                #[allow(clippy::needless_range_loop)]
                for i in 0..codes.len() {
                    let d = self.query[i] - mins[i] - (codes[i] as f32) * scales[i];
                    acc += d * d;
                }
                acc.sqrt()
            }
            VectorMetric::Dot => {
                let dot = self.fused_dot(codes);
                (1.0 - dot).max(0.0)
            }
            VectorMetric::Cosine => {
                let dot = self.fused_dot(codes);
                let nb_sq = self.dequant_norm_sq(codes);
                if self.q_norm < 1e-18 || nb_sq < 1e-18 {
                    return 1.0;
                }
                (1.0 - dot / (self.q_norm * nb_sq).sqrt()).max(0.0)
            }
        }
    }

    /// `dot(query, dequant(codes)) = qm + Σ qs[i] · codes[i]` — single
    /// alloc-free pass over the u8 codes. F4: the `Σ qs[i]·codes[i]` term
    /// is now routed through the dedicated `weighted_linear_u8` SIMD kernel
    /// (AVX2/NEON/scalar dispatch, see `vector::simd::weighted_linear_u8`).
    #[inline]
    fn fused_dot(&self, codes: &[u8]) -> f32 {
        self.qm + crate::simd::weighted_linear_u8(&self.qs, codes)
    }

    /// `‖dequant(codes)‖² = Σ (min_i + codes_i·s_i)²` — single alloc-free pass
    /// (Cosine denominator, candidate side). Delegates to the canonical
    /// SIMD-backed [`Sq8Quantizer::dequant_norm_sq`] (F4).
    #[inline]
    fn dequant_norm_sq(&self, codes: &[u8]) -> f32 {
        self.params.dequant_norm_sq(codes)
    }
}

/// Exact f32 distance between a query vector and a dequantized code vector,
/// using the SAME metric convention as [`ShamirDist`](crate::hnsw_adapter::ShamirDist).
///
/// Used by the rescoring path after a quantized graph traversal. The query
/// is the original f32 vector from the client; `codes` are dequantized
/// through `params` before scoring.
///
/// Audit finding 4.1 (task #530): this is now a thin wrapper over
/// [`RescoreCtx`] — it builds the per-query context and scores a single
/// candidate. Hot callers that rescore MANY candidates against one query
/// should build a [`RescoreCtx`] ONCE and call [`RescoreCtx::score`] per
/// candidate to amortise the `O(dim)` precompute and skip the per-candidate
/// `dequantize` allocation entirely.
///
/// # Panics
///
/// Panics if `query.len() != params.dim()` or `codes.len() != params.dim()`.
pub fn rescore_f32(
    metric: VectorMetric,
    params: &Sq8Quantizer,
    query: &[f32],
    codes: &[u8],
) -> f32 {
    RescoreCtx::new(metric, params, query).score(codes)
}

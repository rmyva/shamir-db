//! Per-query tuning knobs shared by the model and the vector adapters.
//!
//! Lives here so `IndexQuery::Vector` can carry it in `shamir-index-core`;
//! `shamir-vector` re-exports it (`quant::adapter`) so the pre-split
//! `SearchOpts` paths keep resolving.

/// Per-query tuning knobs for approximate vector search (HNSW).
///
/// Both fields are `Option` so that `None` (the default) means "use the
/// adapter's build-time default" — a request that omits the struct entirely
/// behaves identically to pre-V1.1 code, which is what back-compat requires.
///
/// * `ef_search` — the HNSW `ef` exploration-width at query time. Higher =
///   better recall, higher latency. When `Some`, it overrides the adapter's
///   `HnswConfig::ef_search`; clamped to `MAX_EF_SEARCH` before use.
/// * `oversample` — P3 / V3.1 (leaf 3.1): a multiplier that widens the
///   candidate set before post-filtering. **Consumed at the ENGINE level**
///   (`read_filtered_vector_scan`): the engine requests `k′ = k × oversample`
///   candidates from the adapter, applies the residual predicate, and retries
///   with a doubled `k′` (up to `MAX_TOPK`) when fewer than `k` survive. The
///   adapter itself does NOT interpret `oversample` — it simply returns the
///   `k` (or `k′`) candidates requested. Stored here so a single struct covers
///   the whole per-query surface and the engine can thread it into the
///   `IndexQuery::Vector` without a separate channel.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct SearchOpts {
    pub ef_search: Option<u32>,
    pub oversample: Option<f32>,
}

impl SearchOpts {
    /// Convenience for the common "just ef_search" case.
    pub fn with_ef_search(ef_search: u32) -> Self {
        Self {
            ef_search: Some(ef_search),
            oversample: None,
        }
    }

    /// `true` when both knobs are unset — callers can skip any per-query
    /// bookkeeping and use the adapter default directly.
    pub fn is_default(&self) -> bool {
        self.ef_search.is_none() && self.oversample.is_none()
    }
}

impl From<u32> for SearchOpts {
    fn from(ef_search: u32) -> Self {
        Self::with_ef_search(ef_search)
    }
}

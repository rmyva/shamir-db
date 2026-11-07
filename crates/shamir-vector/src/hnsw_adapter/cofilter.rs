use super::adapter::HnswAdapter;
use super::config::{ShamirDist, MAX_EF_SEARCH, MAX_TOPK};
#[cfg(test)]
use super::search::TEST_SEARCH_F32_GATE;
use crate::adapter::VectorError;
use crate::quantized_dist::RescoreCtx;
use hnsw_rs::anndists::dist::distances::Distance;
use shamir_types::types::common::THasher;
use shamir_types::types::record_id::RecordId;
use std::sync::Arc;

/// Co-filter ef-overscan multiplier. `search_filter` can return < knbn under
/// tight filters (layer-0-only application + post-hoc drop). We compensate by
/// requesting `ef = max(ef_base, k * CO_FILTER_EF_MULTIPLIER)` so the graph
/// traversal explores enough candidates to fill the result.
///
/// 8× empirically covers 90%+ of cases where the allow-set is 1–5% of the
/// dataset (validated in the overscan contract test). Combined with the retry
/// widening in the engine, this ensures recall is not catastrophically degraded.
pub const CO_FILTER_EF_MULTIPLIER: u32 = 8;

/// Threshold: if the candidate set (from secondary index) has at most this many
/// RIDs, the pre-filter path (exact SIMD brute-force over the candidates) is
/// chosen. Above this threshold, co-filter (HNSW search_filter) is preferred.
///
/// 4096 is the sweet spot: exact SIMD over 4096 128-d vectors takes ~0.5 ms
/// (AVX2), which is competitive with a single HNSW search_filter call. Above
/// 4096 the linear scan becomes the bottleneck; below it, the graph overhead
/// (per-hop distance computations, cache misses) outweighs the brute-force.
pub const PRE_FILTER_MAX_CANDIDATES: usize = 4096;

/// Upper selectivity bound for co-filter. If the allow-set exceeds this
/// fraction of the total dataset, post-filter (V3.1 oversample-retry) is
/// preferred — co-filter gains diminish when most points pass the filter
/// anyway, and the ef-overscan cost is wasted.
pub const CO_FILTER_MAX_SELECTIVITY: f64 = 0.20;

impl HnswAdapter {
    /// **Pre-filter path** (V3.2): exact SIMD top-k scoring over a small
    /// candidate set of RIDs known to pass the residual predicate.
    ///
    /// The caller provides RIDs that passed the secondary index lookup. This
    /// method resolves them to internal IDs, retrieves their vectors, scores
    /// each against `query` using the SIMD kernels, and returns the top-k by
    /// distance (ascending). Result is EXACT (no approximation).
    ///
    /// Returns `Ok(vec![])` if none of the candidate RIDs have vectors.
    pub async fn search_prefilter(
        &self,
        query: &[f32],
        k: u32,
        candidates: &[RecordId],
    ) -> Result<Vec<(RecordId, f32)>, VectorError> {
        if query.len() as u32 != self.dim {
            return Err(VectorError::DimMismatch {
                expected: self.dim,
                got: query.len() as u32,
            });
        }
        let k = if k == 0 {
            return Ok(vec![]);
        } else {
            k.min(MAX_TOPK)
        };

        let dist = ShamirDist {
            metric: self.metric,
        };

        // V5.2 (#411) — post-fit: candidate vectors are u8 codes; dequant +
        // exact f32 distance for scoring. Pre-fit/unquantized: f32 vectors
        // straight from the buffer.
        let quantized = self.quantized_active();
        let quantizer = if quantized {
            Some(
                self.state
                    .quantizer
                    .get()
                    .expect("quantized_active but quantizer unset"),
            )
        } else {
            None
        };

        // Audit finding 4.1 (task #530): when quantized, build the fused
        // rescore context ONCE per query so the per-candidate loop scores u8
        // codes with no per-candidate dequant allocation.
        let rescore_ctx = quantizer.map(|q| RescoreCtx::new(self.metric, q, query));

        let mut scored: Vec<(RecordId, f32)> = Vec::with_capacity(candidates.len());
        for &rid in candidates {
            // DEADLOCK FIX (#589 class, commit `7a4abf62`):
            // `read_sync`, NOT `read_async`. The `rid_to_internal` map is
            // scanned via `iter_sync` (snapshot serialisation,
            // `collect_live_vectors`) and probed via `contains_sync` on the
            // SAME worker threads.
            let internal = match self.state.rid_to_internal.read_sync(&rid, |_, v| *v) {
                Some(i) => i,
                None => continue,
            };
            // DEADLOCK FIX (#589 class): `contains_sync`, NOT
            // `contains_async` — `deleted` is mutated via `insert_sync`
            // and scanned via `iter_sync` on the SAME worker threads.
            if self.state.deleted.contains_sync(&internal) {
                continue;
            }
            if let Some(ctx) = &rescore_ctx {
                // Post-fit: read codes, dequant + exact rescore.
                // DEADLOCK FIX (#589 class): `read_sync`, NOT `read_async`
                // — `vectors_u8` is scanned via `iter_sync` and mutated via
                // `insert_sync`/`remove_sync` on the SAME worker threads.
                let d_opt = self
                    .state
                    .vectors_u8
                    .read_sync(&internal, |_, c| ctx.score(c));
                if let Some(d) = d_opt {
                    scored.push((rid, d));
                }
            } else {
                // DEADLOCK FIX (#589 class): `read_sync`, NOT `read_async`
                // — `vectors` is scanned via `iter_sync` (fit snapshot/
                // delta/catch-up) and mutated via `insert_sync`/`remove_sync`
                // on the SAME worker threads.
                let d_opt = self
                    .state
                    .vectors
                    .read_sync(&internal, |_, v| dist.eval(query, v));
                if let Some(d) = d_opt {
                    scored.push((rid, d));
                }
            }
        }

        scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        scored.truncate(k as usize);
        Ok(scored)
    }

    /// **Staged-vector scoring** (VR-5 / Б-5): exact brute-force top-k scoring
    /// over the caller's own un-committed staged vectors, for the pre-filter
    /// and co-filter paths of filtered ANN.
    ///
    /// Staged vectors live in `TxContext::staged_vectors` and are NOT in the
    /// committed HNSW graph, so `search_prefilter` / `search_cofilter` (which
    /// resolve RIDs against `rid_to_internal`) cannot see them. The bare
    /// `search` path already merges staged vectors brute-force
    /// (`Merge the caller's own un-committed staged vectors`); this method
    /// exposes the same scoring primitive so the filtered-ANN pre/co-filter
    /// paths can do an identical merge and preserve read-your-own-writes
    /// semantics across all three paths.
    ///
    /// The caller is responsible for applying the residual predicate to the
    /// staged records BEFORE calling this (so only residual-matching staged
    /// vectors are passed in). This method does pure distance scoring against
    /// `query`, exactly mirroring `ShamirDist::eval` used by `search`.
    ///
    /// Returns the scored pairs sorted by distance ascending, truncated to
    /// `k`. `Ok(vec![])` when `staged` is empty or `k == 0`.
    pub async fn score_staged_candidates(
        &self,
        query: &[f32],
        k: u32,
        staged: &[(RecordId, Vec<f32>)],
    ) -> Result<Vec<(RecordId, f32)>, VectorError> {
        if staged.is_empty() {
            return Ok(vec![]);
        }
        if k == 0 {
            return Ok(vec![]);
        }
        if query.len() as u32 != self.dim {
            return Err(VectorError::DimMismatch {
                expected: self.dim,
                got: query.len() as u32,
            });
        }
        let k = k.min(MAX_TOPK);
        let dist = ShamirDist {
            metric: self.metric,
        };
        let mut scored: Vec<(RecordId, f32)> = Vec::with_capacity(staged.len());
        for (rid, vec) in staged {
            // Guard against a malformed staged vector of the wrong dim — skip
            // rather than error so one bad row cannot poison the whole query.
            if vec.len() as u32 != self.dim {
                continue;
            }
            let d = dist.eval(query, vec);
            scored.push((*rid, d));
        }
        scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        scored.truncate(k as usize);
        Ok(scored)
    }

    /// **Co-filter path** (V3.2): HNSW `search_filter` with an allow-set of
    /// internal IDs derived from the candidate RIDs.
    ///
    /// Uses a closure-based `FilterT` (no sorting required). Applies generous
    /// ef-overscan ([`CO_FILTER_EF_MULTIPLIER`]) to compensate for the
    /// `search_filter` under-return behaviour documented in the V0.0 spike.
    pub async fn search_cofilter(
        &self,
        query: &[f32],
        k: u32,
        ef_search_override: Option<u32>,
        candidates: &[RecordId],
    ) -> Result<Vec<(RecordId, f32)>, VectorError> {
        if query.len() as u32 != self.dim {
            return Err(VectorError::DimMismatch {
                expected: self.dim,
                got: query.len() as u32,
            });
        }
        let k = if k == 0 {
            return Ok(vec![]);
        } else {
            k.min(MAX_TOPK)
        };

        // Build allow-set of internal IDs from the candidate RIDs.
        let mut allow_set = shamir_collections::TFxSet::<usize>::with_hasher(THasher::default());
        for &rid in candidates {
            // DEADLOCK FIX (#589 class, commit `7a4abf62`):
            // `read_sync`/`contains_sync`, NOT `read_async`/`contains_async`.
            // Both `rid_to_internal` and `deleted` are accessed via sync
            // accessors (`iter_sync`, `insert_sync`, `contains_sync`) on
            // the SAME worker threads.
            if let Some(internal) = self.state.rid_to_internal.read_sync(&rid, |_, v| *v) {
                if !self.state.deleted.contains_sync(&internal) {
                    allow_set.insert(internal);
                }
            }
        }

        if allow_set.is_empty() {
            return Ok(vec![]);
        }
        // F7 bonus: wrap the built allow-set in an `Arc` ONCE here so both the
        // quantized co-filter path (`search_cofilter_quantized`) and the f32
        // co-filter path below move a cheap refcount handle into
        // `spawn_blocking` instead of each callee re-cloning the whole set.
        let allow_set = Arc::new(allow_set);

        // ef-overscan: generous to compensate search_filter under-return.
        let ef_base = match ef_search_override {
            Some(v) => (v.min(MAX_EF_SEARCH) as usize).max(k as usize),
            None => self.ef_search,
        };
        let ef = ef_base.max((k as usize) * CO_FILTER_EF_MULTIPLIER as usize);

        // V5.2 (#411) — post-fit: co-filter on the u8 graph, then rescore.
        // Pre-fit/unquantized: co-filter on the f32 graph as before.
        if self.quantized_active() {
            // #424 (Б-4) — body extracted to [`search_cofilter_quantized`]
            // so the transient-None retry path in the f32-graph branch below
            // can re-run the EXACT same logic instead of returning empty.
            return self
                .search_cofilter_quantized(query, k, ef, allow_set)
                .await;
        }

        // #418 — f32 co-filter path. Reached ONLY when `!quantized_active()`
        // (the quantized branch returned above). In that state the f32 graph
        // is always present (unquantized adapters never drop it; quantized
        // adapters drop it only post-fit, which IS `quantized_active()`)...
        // UNLESS a concurrent fit transition raced between this function's
        // `quantized_active()` gate (above) and the `hnsw.load_full()` below.
        //
        // #424 (Б-4) — transient-None race window. Same window as `search`:
        //   1. THIS request read `quantized_active() == false` (pre-flip) in
        //      the `if` above, so it landed here on the f32 path.
        //   2. A concurrent fitter flipped `is_fitted = true` + dropped the
        //      f32 graph (`hnsw.store(None)`).
        //   3. THIS request's `hnsw.load_full()` now returns `None`.
        //
        // The old code returned `Ok(vec![])` — silently empty. Fix: re-check
        // `quantized_active()` and route through the u8 co-filter path if the
        // flip landed in the window. If still `false`, it is a genuine
        // invariant violation (unreachable) — defensive empty.
        //
        // #424 (Б-4) — test hook: same as `search` above. Pause between
        // the `quantized_active() == false` gate and `load_full()` so a
        // deterministic test can trigger the fit-drop in the window.
        #[cfg(test)]
        if let Some(gate) = TEST_SEARCH_F32_GATE.load_full() {
            gate.arrived
                .store(true, std::sync::atomic::Ordering::SeqCst);
            gate.notify.notified().await;
        }
        let hnsw = match self.state.hnsw.load_full() {
            Some(h) => h,
            None => {
                // #424 (Б-4) — transient flip during this request.
                if self.quantized_active() {
                    return self
                        .search_cofilter_quantized(query, k, ef, allow_set)
                        .await;
                }
                // Genuinely unreachable: non-quantized / pre-flip adapter
                // with no f32 graph. Defensive empty on a read path.
                return Ok(vec![]);
            }
        };
        let query_owned = query.to_vec();
        let knbn = k as usize;

        let neighbors = tokio::task::spawn_blocking(move || {
            let pred = |id: &usize| allow_set.contains(id);
            hnsw.search_filter(&query_owned, knbn, ef, Some(&pred))
        })
        .await
        .map_err(|e| VectorError::Internal(e.to_string()))?;

        let mut out: Vec<(RecordId, f32)> = Vec::with_capacity(k as usize);
        for n in neighbors {
            // DEADLOCK FIX (same class as #589, commit `7a4abf62`; H3
            // commit `dcfaf825`): `read_sync`, NOT `read_async` —
            // `rid_map` is scanned via `iter_sync` in `for_each_rid_map`
            // (snapshot serialisation) on the SAME worker threads.
            if let Some(rid) = self.state.rid_map.read_sync(&n.d_id, |_, v| *v) {
                out.push((rid, n.distance));
            }
        }
        out.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        out.truncate(k as usize);
        Ok(out)
    }
}

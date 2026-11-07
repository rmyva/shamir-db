//! Shared mutable state (`HnswState`) and the test-only fit hook.
//!
//! The f32 graph lives in ArcSwapOption so a fitted SQ8 adapter can release
//! its large graph while in-flight readers retain their loaded Arc. The
//! u8 graph uses the same RCU pattern; publish the graph and quantizer before
//! setting is_fitted with Release, and read the flag with Acquire. The fit
//! flight flag is a single atomic claim; its owned RAII reset survives errors
//! and task cancellation. Map access remains synchronous by design: sync map
//! guards must never cross an await, avoiding scc lock-handoff deadlocks.
//!
//! migrated_pre_flip counts distinct pre-flip internals already represented
//! in codes or tombstones. next_id_at_flip bounds that count; post-flip ids
//! must not advance it. Keep this paired accounting invariant in sync with the
//! claim and tombstone paths.
//!
//! The `#[cfg(test)]` `FitTestHook` lets tests pause the background fit and
//! inject failures; `recorded_failure` captures the phase text and error of
//! the scheduled fit's error branch so tests assert the *diagnosed* phase.

use super::config::{HnswConfig, ShamirDist};
use crate::quantized_dist::ShamirDistU8;
use crate::sq8::Sq8Quantizer;
use arc_swap::ArcSwapOption;
use hnsw_rs::hnsw::Hnsw;
use shamir_index_core::kind::{VectorMetric, VectorQuantization};
use shamir_types::types::common::THasher;
use shamir_types::types::record_id::RecordId;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

#[cfg(test)]
pub(crate) struct FitTestHook {
    pub(crate) entered: tokio::sync::Notify,
    pub(crate) release: tokio::sync::Notify,
    pub(crate) fail_before_publish: AtomicBool,
    pub(crate) fail_after_publish: AtomicBool,
    /// Last scheduled-fit failure as `(phase text, error)` — set in the
    /// `schedule_fit` error branch (test builds only).
    pub(crate) recorded_failure: std::sync::Mutex<Option<(&'static str, String)>>,
}

/// Shared mutable state for HnswAdapter; fitting owns an Arc clone so it
/// never borrows the caller's stack across background work.
pub(crate) struct HnswState {
    /// RCU f32 graph slot: in-flight readers own loaded Arcs while fit clears it.
    pub(super) hnsw: ArcSwapOption<Hnsw<'static, f32, ShamirDist>>,
    pub(crate) rid_map: scc::HashMap<usize, RecordId, THasher>,
    pub(super) rid_to_internal: scc::HashMap<RecordId, usize, THasher>,
    pub(super) vectors: scc::HashMap<usize, Vec<f32>, THasher>,
    pub(crate) deleted: scc::HashMap<usize, (), THasher>,
    pub(super) deleted_count: AtomicUsize,
    pub(super) next_id: AtomicUsize,
    pub(crate) compaction_deleted_rids: Option<Arc<scc::HashMap<RecordId, (), THasher>>>,
    /// One-shot SQ8 publish graph; ArcSwap loads keep in-flight readers alive.
    pub(super) hnsw_u8: ArcSwapOption<Hnsw<'static, u8, ShamirDistU8>>,
    /// SQ8 authoritative codes; retained separately for rescoring and snapshot output.
    pub(super) vectors_u8: scc::HashMap<usize, Vec<u8>, THasher>,
    pub(super) quantizer: std::sync::OnceLock<Arc<Sq8Quantizer>>,
    /// Release-published only after graph, codes, and quantizer are available.
    pub(super) is_fitted: AtomicBool,
    pub(crate) fit_in_flight: Arc<AtomicBool>,
    pub(super) migrated_pre_flip: AtomicUsize,
    pub(super) next_id_at_flip: AtomicUsize,
    #[cfg(test)]
    pub(super) fit_test_hook: std::sync::Mutex<Option<Arc<FitTestHook>>>,
}

pub struct HnswAdapter {
    pub(super) dim: u32,
    pub(super) metric: VectorMetric,
    pub(super) ef_search: usize,
    /// The f32 graph is removed after fitted SQ8 publication. In-flight readers hold loaded Arcs.
    /// ArcSwap's RCU load/store lets the last reader release graph memory without a hot mutex.
    /// Opt-in quantization mode. `None` always uses the legacy f32 pipeline.
    pub(super) m: usize,
    pub(super) max_layer: usize,
    pub(super) ef_construction: usize,
    pub(super) quantization: Option<VectorQuantization>,
    pub(crate) state: Arc<HnswState>,
}

impl HnswAdapter {
    pub(crate) fn clone_for_fit(&self) -> Self {
        Self {
            dim: self.dim,
            metric: self.metric,
            ef_search: self.ef_search,
            m: self.m,
            max_layer: self.max_layer,
            ef_construction: self.ef_construction,
            quantization: self.quantization,
            state: Arc::clone(&self.state),
        }
    }

    pub fn new(dim: u32, metric: VectorMetric, config: HnswConfig) -> Self {
        Self::new_with_quantization(dim, metric, config, None)
    }

    /// V5.2 (#411) — construct an adapter with an opt-in quantization mode.
    ///
    /// `quantization == None` is bit-for-bit identical to [`Self::new`] —
    /// the u8-graph fields stay `None`/empty forever and every code path
    /// is the legacy f32 pipeline.
    ///
    /// `quantization == Some(Sq8)` enables deferred SQ8 quantization: the
    /// adapter runs f32 below [`FIT_THRESHOLD`] and crosses to a u8 graph
    /// at the threshold — see [`Self::try_fit_and_rebuild`] and the struct
    /// doc on `HnswAdapter`.
    pub fn new_with_quantization(
        dim: u32,
        metric: VectorMetric,
        config: HnswConfig,
        quantization: Option<VectorQuantization>,
    ) -> Self {
        let dist = ShamirDist { metric };
        let hnsw = Hnsw::new(
            config.m,
            config.max_elements,
            config.max_layer,
            config.ef_construction,
            dist,
        );
        let cap = config.max_elements;
        Self {
            dim,
            metric,
            ef_search: config.ef_search,
            m: config.m,
            max_layer: config.max_layer,
            ef_construction: config.ef_construction,
            quantization,
            state: Arc::new(HnswState {
                hnsw: ArcSwapOption::new(Some(Arc::new(hnsw))),
                rid_map: scc::HashMap::with_capacity_and_hasher(cap, THasher::default()),
                rid_to_internal: scc::HashMap::with_capacity_and_hasher(cap, THasher::default()),
                vectors: scc::HashMap::with_capacity_and_hasher(cap, THasher::default()),
                deleted: scc::HashMap::with_capacity_and_hasher(cap, THasher::default()),
                deleted_count: AtomicUsize::new(0),
                next_id: AtomicUsize::new(0),
                compaction_deleted_rids: None,
                hnsw_u8: ArcSwapOption::empty(),
                vectors_u8: scc::HashMap::with_capacity_and_hasher(cap, THasher::default()),
                quantizer: std::sync::OnceLock::new(),
                is_fitted: AtomicBool::new(false),
                fit_in_flight: Arc::new(AtomicBool::new(false)),
                migrated_pre_flip: AtomicUsize::new(0),
                next_id_at_flip: AtomicUsize::new(usize::MAX),
                #[cfg(test)]
                fit_test_hook: std::sync::Mutex::new(None),
            }),
        }
    }

    // ----------------------------------------------------------------------
    // Snapshot codec accessors (`pub(crate)` — used by `snapshot.rs` only)
    // ----------------------------------------------------------------------
    //
    // The codec needs read access to the adapter's internal maps + Hnsw handle
    // to serialise a snapshot, and write access (`from_parts`) to rebuild one
    // from a loaded graph. Both are `pub(crate)`: the codec lives in the same
    // crate, and there is no reason for an external caller to touch these.

    pub(crate) fn dim_field(&self) -> u32 {
        self.dim
    }

    pub(crate) fn metric_field(&self) -> VectorMetric {
        self.metric
    }

    pub(crate) fn ef_search_field(&self) -> usize {
        self.ef_search
    }

    /// #418 — RCU load of the f32 graph: returns a private `Arc` clone (or
    /// `None` if a quantized adapter has dropped it post-fit). Callers on the
    /// f32 search/insert path do `load_full()` once and hold the Arc for the
    /// duration of the graph op; the Arc keeps the graph alive even if a
    /// concurrent fit does `store(None)` — RCU, no UAF (see the `hnsw` field
    /// doc).
    pub(crate) fn hnsw_load(&self) -> Option<Arc<Hnsw<'static, f32, ShamirDist>>> {
        self.state.hnsw.load_full()
    }

    /// #418 — `true` iff the f32 graph is currently resident. Used by the
    /// memory-regression test to deterministically assert that a fitted
    /// quantized adapter HAS dropped its f32 graph (and an unquantized /
    /// pre-fit adapter has NOT). This is a stronger, deterministic check than
    /// a flaky RSS sample — see task #418.
    #[allow(dead_code)] // API for #418 memory-regression test
    pub(crate) fn f32_graph_present(&self) -> bool {
        self.state.hnsw.load_full().is_some()
    }

    /// Build parameters for the snapshot sidecar. Prefers the f32 graph (the
    /// legacy source), falls back to the u8 graph when the f32 graph has been
    /// dropped post-fit (#418) — `m`/`max_layer`/`ef_construction` are
    /// identical across the two graphs (both built from the same `HnswConfig`
    /// at fit time).
    ///
    /// Returns `(m: u8, max_layer: usize, ef_construction: usize)` matching
    /// the `SnapshotSidecar` field types. `None` only if BOTH graphs are
    /// absent — an invariant violation (an adapter always has at least one
    /// graph); callers treat `None` as a snapshot error.
    pub(crate) fn build_params(&self) -> Option<(u8, usize, usize)> {
        if let Some(h) = self.state.hnsw.load_full() {
            return Some((
                h.get_max_nb_connection(),
                h.get_max_level(),
                h.get_ef_construction(),
            ));
        }
        if let Some(h) = self.state.hnsw_u8.load_full() {
            return Some((
                h.get_max_nb_connection(),
                h.get_max_level(),
                h.get_ef_construction(),
            ));
        }
        None
    }

    pub(crate) fn next_id_value(&self) -> usize {
        self.state.next_id.load(Ordering::Relaxed)
    }

    /// Iterate `(internal -> rid)` pairs for snapshot serialisation. Borrows
    /// each entry read-only under the scc cursor; the closure must not block.
    pub(crate) fn for_each_rid_map<F: FnMut(usize, RecordId)>(&self, mut f: F) {
        self.state.rid_map.iter_sync(|internal, rid| {
            f(*internal, *rid);
            true
        });
    }

    /// Iterate `(rid -> internal)` pairs for snapshot serialisation.
    pub(crate) fn for_each_rid_to_internal<F: FnMut(RecordId, usize)>(&self, mut f: F) {
        self.state.rid_to_internal.iter_sync(|rid, internal| {
            f(*rid, *internal);
            true
        });
    }

    /// Check if a rid exists in the live index (not tombstoned).
    #[allow(dead_code)] // API for #408 compaction tests
    pub(crate) fn contains_rid(&self, rid: &RecordId) -> bool {
        self.state.rid_to_internal.contains_sync(rid)
    }

    /// Iterate the tombstone (`deleted`) internals for snapshot serialisation.
    pub(crate) fn for_each_deleted<F: FnMut(usize)>(&self, mut f: F) {
        self.state.deleted.iter_sync(|internal, ()| {
            f(*internal);
            true
        });
    }

    /// Iterate `(internal -> vector)` pairs for snapshot serialisation.
    pub(crate) fn for_each_vector<F: FnMut(usize, &[f32])>(&self, mut f: F) {
        self.state.vectors.iter_sync(|internal, vec| {
            f(*internal, vec);
            true
        });
    }

    /// Reconstruct an adapter from snapshot parts. Used by `snapshot::load`.
    ///
    /// `hnsw` is an `Arc<Hnsw<'static, ...>>` obtained from `load_hnsw_with_dist`
    /// via a `Box::leak`'d `HnswIo` loader (see `snapshot::load` — the leak is
    /// boot-only, one loader per shard, and the dump files are the durable
    /// artefact). The maps and `next_id` are rebuilt from the sidecar.
    ///
    /// V5.2 (#411): snapshot codec always loads the f32 path here — the
    /// quantized snapshot (u8 graph + quantizer params) is #412. A
    /// quantization-enabled adapter loaded this way starts un-fitted and
    /// will re-fit at the threshold on the next upserts, which is correct
    /// (the f32 vectors are all present in the sidecar).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_parts(
        dim: u32,
        metric: VectorMetric,
        ef_search: usize,
        hnsw: Arc<Hnsw<'static, f32, ShamirDist>>,
        rid_map: scc::HashMap<usize, RecordId, THasher>,
        rid_to_internal: scc::HashMap<RecordId, usize, THasher>,
        vectors: scc::HashMap<usize, Vec<f32>, THasher>,
        deleted: scc::HashMap<usize, (), THasher>,
        next_id: usize,
        quantization: Option<VectorQuantization>,
    ) -> Self {
        #[allow(clippy::disallowed_methods)] // O(N) ack: one-time seed at snapshot load
        let deleted_cnt = deleted.len();
        Self {
            dim,
            metric,
            ef_search,
            m: usize::from(hnsw.get_max_nb_connection()),
            max_layer: hnsw.get_max_level(),
            ef_construction: hnsw.get_ef_construction(),
            quantization,
            state: Arc::new(HnswState {
                hnsw: ArcSwapOption::new(Some(hnsw)),
                rid_map,
                rid_to_internal,
                vectors,
                deleted,
                deleted_count: AtomicUsize::new(deleted_cnt),
                next_id: AtomicUsize::new(next_id),
                compaction_deleted_rids: None,
                hnsw_u8: arc_swap::ArcSwapOption::empty(),
                vectors_u8: scc::HashMap::with_hasher(THasher::default()),
                quantizer: std::sync::OnceLock::new(),
                is_fitted: AtomicBool::new(false),
                fit_in_flight: Arc::new(AtomicBool::new(false)),
                migrated_pre_flip: AtomicUsize::new(0),
                next_id_at_flip: AtomicUsize::new(usize::MAX),
                #[cfg(test)]
                fit_test_hook: std::sync::Mutex::new(None),
            }),
        }
    }

    /// V5.3 (#412) — Reconstruct a FITTED quantized adapter from snapshot
    /// parts. Used by `snapshot::load` when the sidecar carries a
    /// `quantization = Some(bincode(QuantMeta))` field and the manifest
    /// points at `qgraph`/`qdata` sections (a quantized v2 snapshot).
    ///
    /// This is the load counterpart of the dump branch in
    /// [`dump_snapshot_with_gen`](crate::snapshot::dump_snapshot_with_gen):
    /// it stitches the already-loaded u8 graph (`hnsw_u8`), the reconstructed
    /// [`Sq8Quantizer`] (`quantizer`), and the sidecar's `vectors_u8` codes
    /// into a live adapter with `is_fitted = true`. Post-load search and
    /// upsert go through the quantized path immediately — no re-fit is
    /// needed.
    ///
    /// `quantization` is set to `Some(Sq8)` so [`quantized_active`](Self::quantized_active)
    /// returns `true` (it gates on `is_fitted && quantization.is_some()`).
    ///
    /// #418 — the `hnsw` (f32) parameter is ACCEPTED for back-compat with the
    /// snapshot codec signature but is DROPPED immediately: a fitted quantized
    /// adapter never reads the f32 graph post-fit (every search/insert is
    /// gated through `quantized_active()`), so retaining it would defeat the
    /// whole purpose of SQ8 (#412 found SQ8 RSS > f32 RSS for exactly this
    /// reason — the f32 graph stayed resident on top of the u8 graph). We
    /// install `None` here, identical to the in-memory post-fit state.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_parts_with_quantization(
        dim: u32,
        metric: VectorMetric,
        ef_search: usize,
        hnsw: Arc<Hnsw<'static, f32, ShamirDist>>,
        rid_map: scc::HashMap<usize, RecordId, THasher>,
        rid_to_internal: scc::HashMap<RecordId, usize, THasher>,
        vectors: scc::HashMap<usize, Vec<f32>, THasher>,
        deleted: scc::HashMap<usize, (), THasher>,
        next_id: usize,
        hnsw_u8: Arc<Hnsw<'static, u8, ShamirDistU8>>,
        quantizer: Arc<Sq8Quantizer>,
        vectors_u8: scc::HashMap<usize, Vec<u8>, THasher>,
    ) -> Self {
        // Drop the f32 graph immediately — see the doc comment above.
        drop(hnsw);
        #[allow(clippy::disallowed_methods)] // O(N) ack: one-time seed at snapshot load
        let deleted_cnt = deleted.len();
        Self {
            dim,
            metric,
            ef_search,
            m: usize::from(hnsw_u8.get_max_nb_connection()),
            max_layer: hnsw_u8.get_max_level(),
            ef_construction: hnsw_u8.get_ef_construction(),
            quantization: Some(VectorQuantization::Sq8),
            state: Arc::new(HnswState {
                hnsw: ArcSwapOption::empty(),
                rid_map,
                rid_to_internal,
                vectors,
                deleted,
                deleted_count: AtomicUsize::new(deleted_cnt),
                next_id: AtomicUsize::new(next_id),
                compaction_deleted_rids: None,
                hnsw_u8: arc_swap::ArcSwapOption::from(Some(hnsw_u8)),
                vectors_u8,
                quantizer: {
                    let lock = std::sync::OnceLock::new();
                    let _ = lock.set(quantizer);
                    lock
                },
                is_fitted: AtomicBool::new(true),
                fit_in_flight: Arc::new(AtomicBool::new(false)),
                migrated_pre_flip: AtomicUsize::new(next_id),
                next_id_at_flip: AtomicUsize::new(usize::MAX),
                #[cfg(test)]
                fit_test_hook: std::sync::Mutex::new(None),
            }),
        }
    }

    /// Number of tombstoned internal ids (O(1) atomic mirror).
    #[allow(dead_code)] // API for #408 compaction trigger
    pub(crate) fn deleted_count(&self) -> usize {
        self.state.deleted_count.load(Ordering::Relaxed)
    }

    /// Number of live vectors (O(1) = next_id - deleted_count).
    ///
    /// `saturating_sub`: `next_id` and `deleted_count` are two independent
    /// Relaxed loads. `next_id` is bumped BEFORE its tombstone increment, so
    /// globally `deleted_count <= next_id` always holds — but a reader can
    /// observe a stale `next_id` with a fresher `deleted_count` under
    /// concurrent replace/delete, which would underflow a plain subtraction.
    /// Saturating keeps the heuristic cardinality at 0 in that transient
    /// window instead of panicking (debug) / wrapping to usize::MAX (release).
    #[allow(dead_code)] // API for #408 compaction trigger
    pub(crate) fn live_count(&self) -> usize {
        self.state
            .next_id
            .load(Ordering::Relaxed)
            .saturating_sub(self.state.deleted_count.load(Ordering::Relaxed))
    }

    /// Ratio of tombstoned slots to total allocated ids (0.0 when empty).
    pub(crate) fn deleted_ratio(&self) -> f64 {
        let next = self.state.next_id.load(Ordering::Relaxed);
        if next == 0 {
            return 0.0;
        }
        self.state.deleted_count.load(Ordering::Relaxed) as f64 / next as f64
    }
}

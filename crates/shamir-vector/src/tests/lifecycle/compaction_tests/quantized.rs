//! #428 (VR-6) — quantization-aware compaction tests.
//!
//! Before #428, `run_background_compaction` ALWAYS built the target via
//! `new_compaction_target` (f32), so a compaction on a fitted SQ8 index
//! silently dropped quantization: memory returned to 4×, and the post-
//! compaction snapshot was v1 (no QuantMeta). These tests cover the fix:
//! the target inherits the old adapter's quantization mode, backfill
//! re-fits the quantizer on the surviving live set (Variant A), the f32
//! graph is dropped post-fit, and the snapshot after compaction is v2.
//! Back-compat (non-quant compaction stays f32) is also asserted.

use super::{random_vec, rid};
use crate::adapter::{SearchOpts, VectorAdapter};
use crate::hnsw_adapter::{HnswAdapter, HnswConfig};
use shamir_collections::TFxSet;
use shamir_index_core::kind::{VectorMetric, VectorQuantization};
use shamir_types::types::record_id::RecordId;
use std::sync::Arc;

/// Deterministic Gaussian-clustered vector generator (mirrors
/// `quantized_graph_tests::Lcg`) — needed so the SQ8 quantizer has a
/// non-degenerate distribution to fit on.
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
    fn next_f32(&mut self) -> f32 {
        let high = (self.next_u64() >> 32) as u32;
        (high as f32) / (1u64 << 32) as f32
    }
    #[inline]
    fn next_range(&mut self, lo: f32, hi: f32) -> f32 {
        lo + (hi - lo) * self.next_f32()
    }
    fn next_gaussian(&mut self) -> f32 {
        loop {
            let u1 = self.next_f32() * 2.0 - 1.0;
            let u2 = self.next_f32() * 2.0 - 1.0;
            let s = u1 * u1 + u2 * u2;
            if s > 0.0 && s < 1.0 {
                let mul = ((-2.0 * s.ln()) / s).sqrt();
                return u1 * mul;
            }
        }
    }
}

fn clustered(n: usize, dim: usize, k: usize, sigma: f32, seed: u64) -> Vec<Vec<f32>> {
    assert!(k > 0);
    let mut rng = Lcg::new(seed);
    let centroids: Vec<Vec<f32>> = (0..k)
        .map(|_| (0..dim).map(|_| rng.next_range(-1.0, 1.0)).collect())
        .collect();
    (0..n)
        .map(|i| {
            let c = &centroids[i % k];
            (0..dim)
                .map(|j| c[j] + sigma * rng.next_gaussian())
                .collect()
        })
        .collect()
}

/// Build a FITTED SQ8 adapter with `n` live vectors, then tombstone the first
/// `n_delete` of them — modelling a dirty index that a compaction would
/// trigger on. Returns the adapter and the full dataset (for recall queries).
async fn build_fitted_sq8_dirty(
    n: usize,
    n_delete: usize,
    dim: u32,
    seed: u64,
) -> (HnswAdapter, Vec<Vec<f32>>) {
    let adapter = HnswAdapter::new_with_quantization(
        dim,
        VectorMetric::Cosine,
        HnswConfig {
            max_elements: 10_000,
            m: 16,
            max_layer: 16,
            ef_construction: 200,
            ef_search: 128,
        },
        Some(VectorQuantization::Sq8),
    );
    let data = clustered(n, dim as usize, 10, 0.2, seed);
    let items: Vec<(RecordId, Vec<f32>)> = data
        .iter()
        .enumerate()
        .map(|(i, v)| (rid(i as u16), v.clone()))
        .collect();
    adapter.upsert_batch(&items).await.unwrap();
    crate::hnsw_adapter::await_fitted(&adapter).await;
    assert!(adapter.is_quantized(), "test setup: adapter did not fit");
    for i in 0..n_delete {
        adapter.delete(rid(i as u16)).await.unwrap();
    }
    (adapter, data)
}

/// Run the adapter-level compaction rebuild (Steps 1–4a of
/// `run_background_compaction`) on `old`, producing the new fitted target.
/// Mirrors what the production compaction path does, minus the Store/snapshot
/// plumbing.
async fn compaction_rebuild(old: &HnswAdapter, dim: u32) -> HnswAdapter {
    let config = old.build_config();
    let metric = old.metric_field();
    let q = old.quantization_mode();
    let new = HnswAdapter::new_compaction_target_quantized(dim, metric, config, q);
    let live_pairs = old.collect_live_vectors();
    new.backfill_if_absent(&live_pairs).await.unwrap();
    new
}

/// #428 — compaction of a fitted SQ8 index produces a QUANTIZED target.
/// The target re-fits the quantizer on the surviving live set (Variant A),
/// so `is_quantized()` is true and the f32 graph is dropped (the deterministic
/// memory-regression invariant from #418).
#[tokio::test]
async fn vr6_sq8_compaction_produces_quantized_target() {
    let dim = 32u32;
    // 400 live, 100 tombstoned → 300 survivors, well above FIT_THRESHOLD (256).
    let (old, _data) = build_fitted_sq8_dirty(400, 100, dim, 0xA11CE).await;
    assert_eq!(old.deleted_count(), 100);
    assert_eq!(old.live_count(), 300);

    let new = compaction_rebuild(&old, dim).await;

    // Variant A: the target re-fit on the 300 survivors → quantized.
    crate::hnsw_adapter::await_fitted(&new).await;
    assert!(
        new.is_quantized(),
        "#428: SQ8 compaction target did not re-fit — quantization lost"
    );
    assert!(
        new.quantizer().is_some(),
        "#428: SQ8 compaction target has no quantizer"
    );
    assert!(
        new.hnsw_u8_handle().is_some(),
        "#428: SQ8 compaction target has no u8 graph"
    );
    // #418 memory invariant — the f32 graph must be dropped post-fit.
    assert!(
        !new.f32_graph_present(),
        "#428: SQ8 compaction target retained the f32 graph (4× memory regression)"
    );
    // Live count preserved across compaction.
    assert_eq!(new.live_count(), 300);
    assert_eq!(new.deleted_count(), 0);
}

/// #428 — search is correct after compaction + re-fit. The recall of the
/// post-compaction target (queried on the surviving vectors) must be high
/// against the pre-compaction adapter's results on the same survivors.
#[tokio::test]
async fn vr6_sq8_compaction_recall_smoke() {
    let dim = 32u32;
    let (old, data) = build_fitted_sq8_dirty(400, 100, dim, 0xB0B).await;
    let new = compaction_rebuild(&old, dim).await;
    crate::hnsw_adapter::await_fitted(&new).await;
    assert!(new.is_quantized());

    let opts = SearchOpts {
        ef_search: Some(256),
        oversample: None,
    };
    // Query the first 10 surviving vectors (rids 100..109 survived: deletes
    // were 0..99). Compare top-10 overlap between old and new.
    let mut hits = 0usize;
    let mut poss = 0usize;
    for (i, q) in data.iter().enumerate().take(110).skip(100) {
        let before = old.search(q, 10, opts, None).await.unwrap();
        let after = new.search(q, 10, opts, None).await.unwrap();
        let before_set: TFxSet<RecordId> = before.into_iter().map(|(r, _)| r).collect();
        for (r, _) in &after {
            if before_set.contains(r) {
                hits += 1;
            }
            poss += 1;
        }
        let _ = i; // index unused beyond positioning; silence warnings on refactor
    }
    let recall = hits as f32 / poss.max(1) as f32;
    // SQ8 + compaction re-fit; the bar matches the existing
    // `recall_sq8_vs_f32_within_two_percent` tolerance band (allowing for
    // hnsw_rs unseedable-RNG variance).
    assert!(
        recall >= 0.80,
        "#428: post-compaction recall@10 = {recall:.4} below 0.80"
    );
}

/// #428 — snapshot after compaction of a quantized index is v2 (QuantMeta
/// present). Mirrors `compaction_snapshot_restart_preserves_quant_state` but
/// drives the compaction rebuild through the #428 path (quantized target +
/// deferred-fit in backfill) before dumping.
#[tokio::test]
async fn vr6_sq8_compaction_snapshot_is_v2() {
    use crate::snapshot::{
        self, dump_snapshot, load_snapshot, SnapshotSidecar, SNAPSHOT_FORMAT_VERSION,
    };
    use shamir_index_core::meta_envelope::MetaEnvelope;
    use shamir_storage::storage_in_memory::InMemoryStore;
    use shamir_storage::types::Store;

    const KS: &str = "vsnapq.vr6";
    let dim = 32u32;
    let (old, _data) = build_fitted_sq8_dirty(400, 100, dim, 0xC0FFEE).await;
    let new = compaction_rebuild(&old, dim).await;
    crate::hnsw_adapter::await_fitted(&new).await;
    assert!(new.is_quantized(), "precondition: target re-fitted");

    let store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    dump_snapshot(&new, &store, KS).await.unwrap();

    // Sidecar must stamp format_version = 2 and carry QuantMeta.
    let sidecar_k = snapshot::sidecar_key_for_test(KS, 0);
    let sidecar_bytes = store.get(sidecar_k).await.unwrap();
    let sidecar: SnapshotSidecar = MetaEnvelope::open(&sidecar_bytes).unwrap();
    assert_eq!(
        sidecar.format_version, SNAPSHOT_FORMAT_VERSION,
        "#428: post-compaction snapshot format_version != SNAPSHOT_FORMAT_VERSION"
    );
    assert!(
        sidecar.quantization.is_some(),
        "#428: post-compaction v2 sidecar has no quantization (QuantMeta lost)"
    );

    // Round-trip: load produces a fitted quantized adapter.
    let loaded = load_snapshot(&store, KS).await.unwrap();
    assert!(
        loaded.is_quantized(),
        "#428: post-compaction v2 load did not restore a fitted adapter"
    );
}

/// #428 — back-compat: compaction of a NON-quantized index leaves the target
/// on the f32 path (not quantized, f32 graph resident). The existing
/// non-quant compaction tests must not regress.
#[tokio::test]
async fn vr6_non_quant_compaction_stays_f32() {
    let dim = 8u32;
    let old = HnswAdapter::new(
        dim,
        VectorMetric::L2,
        HnswConfig {
            max_elements: 5000,
            ..Default::default()
        },
    );
    // Insert 400, delete 100 → 300 live.
    for i in 0..400u16 {
        old.upsert(rid(i), &random_vec(dim as usize, i as u64))
            .await
            .unwrap();
    }
    for i in 0..100u16 {
        old.delete(rid(i)).await.unwrap();
    }
    assert!(old.quantization_mode().is_none());

    let new = compaction_rebuild(&old, dim).await;

    // Non-quant → target must NOT be quantized and the f32 graph must stay.
    assert!(
        !new.is_quantized(),
        "#428 back-compat: non-quant compaction target became quantized"
    );
    assert!(
        new.f32_graph_present(),
        "#428 back-compat: non-quant compaction target dropped the f32 graph"
    );
    assert_eq!(new.live_count(), 300);
    assert_eq!(new.deleted_count(), 0);
}

// ===========================================================================
// Post-VR-8 follow-up — delete-vs-backfill same-rid race (found by the final
// 10x `@vector @engine --full` stress gate, then confirmed by @sh
// adversarial review of the `backfill_if_absent` per-item dispatch fix).
//
// `backfill_if_absent`'s self-migration re-check (mirrors `upsert`, Б-1/#423)
// MUST guard the claim on `!self.deleted.contains(&internal)`, exactly like
// `upsert` does — without the guard, a concurrent `delete(r)` on the SAME rid
// backfill just claimed (reachable via the double-write compaction protocol)
// tombstones `internal` (bumping `migrated_pre_flip` once via
// `bump_migrated_on_tombstone`) BEFORE backfill's own re-check runs; the
// re-check's claim then wins a SECOND time (vectors_u8 was never populated
// by the tombstone path) and bumps `migrated_pre_flip` again — a double
// count that can trip the catch-up loop's `migrated >= target` convergence
// early, and inserts a live graph node for an already-tombstoned internal.
//
// This is deliberately NOT covered by `stress_concurrent_mutations_during_
// quantized_compaction` above — that test's delete workers only ever target
// rids THEY themselves inserted via double-write (rids >= n_initial), never
// the rids backfill_if_absent is concurrently processing (rids < n_initial).
// This test closes exactly that gap: delete workers race the SAME rid range
// backfill is backfilling, for the whole duration of the backfill call.
// ===========================================================================
#[tokio::test]
async fn backfill_delete_same_rid_race_no_double_count() {
    // Hang guard (matches the codebase convention for this test class, e.g.
    // `concurrent_upsert_with_tombstone_across_fit_does_not_hang`): a
    // convergence bug must surface as an actionable test FAILURE within a
    // bounded window, never as an indefinite hang burning CI/dev wall-clock.
    let result = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        let dim = 8u32;
        let target = Arc::new(HnswAdapter::new_compaction_target_quantized(
            dim,
            VectorMetric::L2,
            HnswConfig {
                max_elements: 5000,
                ..Default::default()
            },
            Some(VectorQuantization::Sq8),
        ));

        // The exact rid range backfill_if_absent will process — deliberately
        // BELOW FIT_THRESHOLD (256) alone, so the flip must come from the
        // concurrent upsert workers below (crossing threshold via THEIR OWN
        // deferred-fit trigger), landing mid-backfill-loop — exactly the
        // window the (now-fixed) missing guard left open. Kept small: this
        // test only needs ONE delete to land inside the narrow race window,
        // not scale for its own sake — a large item count just multiplies
        // real spawn_blocking round-trips without improving reproduction odds.
        let n_backfill = 40u16;
        let backfill_items: Vec<(RecordId, Vec<f32>)> = (0..n_backfill)
            .map(|i| (rid(i), random_vec(dim as usize, i as u64)))
            .collect();

        let backfill_target = Arc::clone(&target);
        let backfill_handle = tokio::spawn(async move {
            backfill_target
                .backfill_if_absent(&backfill_items)
                .await
                .unwrap();
        });

        // Push the target past FIT_THRESHOLD via a DIFFERENT rid range so
        // the adapter's own deferred-fit fires concurrently with (not
        // after) backfill's loop above.
        let mut upsert_handles = Vec::new();
        for w in 0..4u16 {
            let target = Arc::clone(&target);
            upsert_handles.push(tokio::spawn(async move {
                for i in 0..70u16 {
                    let r = rid(1000 + w * 100 + i);
                    let _ = target
                        .upsert(r, &random_vec(dim as usize, (2000 + w * 100 + i) as u64))
                        .await;
                }
            }));
        }

        // Race deletes against the SAME rid range backfill is backfilling —
        // the missing guard's failure window is narrow (between backfill's
        // Vacant claim and its self-migration re-check), so a handful of
        // concurrent passes over the small range is enough to have a
        // realistic shot at landing inside it without inflating runtime.
        let mut delete_handles = Vec::new();
        for _ in 0..4u16 {
            let target = Arc::clone(&target);
            delete_handles.push(tokio::spawn(async move {
                for i in 0..n_backfill {
                    let _ = target.delete(rid(i)).await;
                }
            }));
        }

        backfill_handle.await.unwrap();
        for h in upsert_handles {
            h.await.unwrap();
        }
        for h in delete_handles {
            h.await.unwrap();
        }

        target
    })
    .await;

    let target = result.expect(
        "backfill_delete_same_rid_race_no_double_count timed out (30s) — \
         convergence never reached migrated_pre_flip >= next_id_at_flip, \
         a regression in the catch-up loop or the claim-guard fix",
    );

    // The invariant a double-count would violate: migrated_pre_flip must
    // never exceed next_id_at_flip once fitted. (Pre-fix, this could also
    // manifest as `backfill_if_absent` itself returning an Internal error
    // — the `.unwrap()` above already guards that regression class.)
    if target.is_quantized() {
        let (migrated, target_count) = target.test_convergence_counters();
        assert!(
            migrated <= target_count,
            "migrated_pre_flip ({migrated}) exceeded next_id_at_flip \
             ({target_count}) — double-counted a claim (missing \
             !self.deleted.contains(&internal) guard in backfill_if_absent's \
             self-migration re-check)"
        );
    }
}

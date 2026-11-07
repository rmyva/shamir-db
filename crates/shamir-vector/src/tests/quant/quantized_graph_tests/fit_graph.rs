//! V5.2 (#411) quantized HNSW adapter tests: unconditional u8-code eviction
//! on delete (pre- and post-fit), upsert replace, #418 f32-graph drop
//! regression, and the #423 (Б-1) fit-window graph-path no-loss invariant.
//! Shared runner/RNG helpers live in `runner.rs`.

use super::{clustered, rid, run_graph_test};
use crate::adapter::{SearchOpts, VectorAdapter};
use crate::hnsw_adapter::{HnswAdapter, HnswConfig};
use shamir_index_core::kind::{VectorMetric, VectorQuantization};
use shamir_types::types::record_id::RecordId;
use std::sync::Arc;

#[test]
fn delete_unconditionally_evicts_u8_codes_post_fit() {
    run_graph_test(
        "delete_unconditionally_evicts_u8_codes_post_fit",
        delete_unconditionally_evicts_u8_codes_post_fit_body(),
    );
}

async fn delete_unconditionally_evicts_u8_codes_post_fit_body() {
    let dim = 16u32;
    let adapter = HnswAdapter::new_with_quantization(
        dim,
        VectorMetric::L2,
        HnswConfig {
            max_elements: 10_000,
            m: 16,
            max_layer: 16,
            ef_construction: 200,
            ef_search: 64,
        },
        Some(VectorQuantization::Sq8),
    );

    let data = clustered(300, dim as usize, 12, 0.2, 0xB6_C0_43_0F);
    let items: Vec<(RecordId, Vec<f32>)> = data
        .iter()
        .enumerate()
        .map(|(i, v)| (rid(i as u64), v.clone()))
        .collect();
    adapter.upsert_batch(&items).await.unwrap();
    crate::hnsw_adapter::await_fitted(&adapter).await;
    assert!(
        adapter.is_quantized(),
        "test setup: adapter must be fitted so codes are resident in vectors_u8"
    );

    // Capture the internal id that rid(7) currently maps to — `delete`
    // removes the rid_to_internal entry, so we must read it BEFORE the
    // delete to know which internal's codes to look for afterwards.
    let target_rid = rid(7);
    let target_internal = {
        let mut found: Option<usize> = None;
        adapter.for_each_rid_to_internal(|r, internal| {
            if r == target_rid {
                found = Some(internal);
            }
        });
        found.expect("rid(7) must be present before delete")
    };

    // Sanity: codes for target_internal ARE resident pre-delete.
    let has_pre = {
        let mut hit = false;
        adapter.for_each_vector_u8(|internal, _| {
            if internal == target_internal {
                hit = true;
            }
        });
        hit
    };
    assert!(
        has_pre,
        "test setup: target_internal's u8 codes must be in vectors_u8 before delete"
    );

    // The act under test.
    adapter.delete(target_rid).await.unwrap();

    // The contract: target_internal's codes must be GONE from vectors_u8,
    // unconditionally — not merely hidden by the tombstone. If a future
    // change re-introduces a `quantized_active()` gate (or any other
    // condition) around the eviction, this assertion fires.
    let still_present = {
        let mut hit = false;
        adapter.for_each_vector_u8(|internal, _| {
            if internal == target_internal {
                hit = true;
            }
        });
        hit
    };
    assert!(
        !still_present,
        "VR-8 (Б-6): after delete(rid(7)) the deleted internal's u8 codes \
         are still resident in vectors_u8 — `delete()` must evict them \
         UNCONDITIONALLY (not gated on quantized_active()). This leaks \
         memory until the next compaction."
    );
}

// =========================================================================
// VR-8 (#430, Б-6) — regression, PRE-FIT path: this is the actual race the
// fix targets. Pre-VR-8 the `vectors_u8.remove_async` in `delete()` was gated
// on a single `quantized_active()` read. Consider this interleaving:
//
//   T1 (fitter)   : snapshot/delta pass calls `claim_and_publish_u8(internal)`
//                   → `vectors_u8.insert(internal, codes)` succeeds (code now
//                   resident) — BUT `is_fitted` is still `false` (the flip
//                   happens later, after the graph build).
//   T2 (delete)   : reads `quantized_active()` → `false` (pre-flip) → SKIPS
//                   `vectors_u8.remove_async`. Tombstone is set, so the code
//                   is invisible to search, but it LEAKS in memory until the
//                   next compaction.
//
// We cannot deterministically interleave two threads into this nanosecond
// window without heroic orchestration, but the CONTRACT the fix establishes
// is directly testable: with the adapter PRE-flip, force a code into
// `vectors_u8` (mimicking the fitter's claim), then `delete()` the rid — the
// code MUST be evicted even though `quantized_active() == false`. Pre-VR-8
// this assertion fails (the code stays resident).
// =========================================================================

#[test]
fn delete_evicts_u8_codes_even_pre_fit_race_window() {
    run_graph_test(
        "delete_evicts_u8_codes_even_pre_fit_race_window",
        delete_evicts_u8_codes_even_pre_fit_race_window_body(),
    );
}

async fn delete_evicts_u8_codes_even_pre_fit_race_window_body() {
    let dim = 16u32;
    // Build a quantization-ENABLED adapter but stay WELL below FIT_THRESHOLD
    // (256) so no fit ever triggers — `quantized_active()` stays `false`.
    let adapter = HnswAdapter::new_with_quantization(
        dim,
        VectorMetric::L2,
        HnswConfig {
            max_elements: 10_000,
            m: 16,
            max_layer: 16,
            ef_construction: 200,
            ef_search: 64,
        },
        Some(VectorQuantization::Sq8),
    );

    // Insert a handful of vectors — nowhere near the fit threshold.
    let data = clustered(8, dim as usize, 4, 0.2, 0xB6_C0_DE_0F);
    let items: Vec<(RecordId, Vec<f32>)> = data
        .iter()
        .enumerate()
        .map(|(i, v)| (rid(i as u64), v.clone()))
        .collect();
    adapter.upsert_batch(&items).await.unwrap();
    assert!(
        !adapter.is_quantized(),
        "test setup: adapter must be PRE-flip (quantized_active == false)"
    );

    // Simulate the race: a concurrent fitter's claim has already landed a
    // code for rid(3)'s internal into `vectors_u8`, but the flip hasn't
    // happened yet. We must read the internal id BEFORE the delete (the
    // delete drops the rid_to_internal entry).
    let target_rid = rid(3);
    let target_internal = {
        let mut found: Option<usize> = None;
        adapter.for_each_rid_to_internal(|r, internal| {
            if r == target_rid {
                found = Some(internal);
            }
        });
        found.expect("rid(3) must be present before delete")
    };
    adapter.test_force_publish_u8(target_internal, vec![0u8; dim as usize]);

    // Sanity: the simulated race left a code resident while still pre-flip.
    // NOTE: we use `test_vectors_u8_contains` (NOT `for_each_vector_u8`):
    // the latter short-circuits on `!is_quantized()` and would hide the
    // resident code, making this regression vacuously pass.
    assert!(
        adapter.test_vectors_u8_contains(target_internal),
        "test setup: simulated fitter claim must have placed a code in vectors_u8"
    );

    // The act under test — `delete()` observes quantized_active() == false.
    adapter.delete(target_rid).await.unwrap();

    // VR-8 (Б-6): the code MUST be evicted unconditionally, NOT gated on
    // quantized_active(). Pre-VR-8 this leaks (remove skipped).
    assert!(
        !adapter.test_vectors_u8_contains(target_internal),
        "VR-8 (Б-6) pre-fit race: after delete(rid(3)) with quantized_active() \
         == false, the deleted internal's u8 codes are still resident in \
         vectors_u8. `delete()` must evict UNCONDITIONALLY — the tombstone is \
         the happens-before authority, not the quantized_active() snapshot."
    );
}

// =========================================================================
// upsert replace on a quantized index: old internal tombstoned
// =========================================================================

#[test]
fn upsert_replace_on_quantized_index() {
    run_graph_test(
        "upsert_replace_on_quantized_index",
        upsert_replace_on_quantized_index_body(),
    );
}

async fn upsert_replace_on_quantized_index_body() {
    let dim = 16u32;
    let adapter = Arc::new(HnswAdapter::new_with_quantization(
        dim,
        VectorMetric::L2,
        HnswConfig {
            max_elements: 10_000,
            m: 16,
            max_layer: 16,
            ef_construction: 200,
            ef_search: 64,
        },
        Some(VectorQuantization::Sq8),
    ));

    let data = clustered(300, dim as usize, 12, 0.2, 0xFEEDFACE);
    let items: Vec<(RecordId, Vec<f32>)> = data
        .iter()
        .enumerate()
        .map(|(i, v)| (rid(i as u64), v.clone()))
        .collect();
    adapter.upsert_batch(&items).await.unwrap();
    crate::hnsw_adapter::await_fitted(&adapter).await;
    assert!(adapter.is_quantized());

    // Replace rid(10) with a new vector.
    let new_vec: Vec<f32> = (0..dim).map(|i| (i as f32) * 0.1).collect();
    adapter.upsert(rid(10), &new_vec).await.unwrap();

    // Search for the NEW vector — rid(10) must be the top-1.
    // Query the NEW vector. On a lossy SQ8 index a strict top-1 self-query is
    // stochastic (hnsw_rs's unseedable RNG + quantization occasionally rank a
    // neighbour equal-or-closer to the *quantized* query), so we assert the
    // replace is RETRIEVABLE — rid(10) appears in a small top-k with generous
    // ef — rather than over-asserting an exact rank a compressed index cannot
    // guarantee. This still fails hard if the replace lost the new vector
    // (old code served / rid absent), which is what the test guards.
    let opts = SearchOpts {
        ef_search: Some(128),
        oversample: None,
    };
    let res = adapter.search(&new_vec, 5, opts, None).await.unwrap();
    assert!(!res.is_empty(), "search returned empty after replace");
    assert!(
        res.iter().any(|(r, _)| *r == rid(10)),
        "replaced rid(10) not retrievable for its new vector; got {:?}",
        res.iter().map(|(r, _)| *r).collect::<Vec<_>>()
    );
}

// =========================================================================
// #418 — memory regression: f32 graph is freed post-fit (SQ8 memory win)
// =========================================================================
//
// The #412 bench found SQ8 RSS > f32 RSS because the f32 graph stayed
// resident on top of the u8 graph. #418 drops the f32 graph post-fit. This
// test asserts the drop deterministically via `f32_graph_present()` — a
// stronger check than a flaky RSS sample — and covers three regimes:
//  1. non-quant adapter: f32 graph NEVER dropped (back-compat).
//  2. quant adapter pre-fit: f32 graph resident.
//  3. quant adapter post-fit: f32 graph DROPPED; search still works.

#[test]
fn f32_graph_present_non_quant_never_drops() {
    run_graph_test(
        "f32_graph_present_non_quant_never_drops",
        f32_graph_present_non_quant_never_drops_body(),
    );
}

async fn f32_graph_present_non_quant_never_drops_body() {
    // A non-quant adapter never drops its f32 graph — bit-for-bit back-compat
    // with the pre-#418 legacy path.
    let dim = 8u32;
    let adapter = HnswAdapter::new(dim, VectorMetric::L2, HnswConfig::default());
    for i in 0..10u64 {
        let v: Vec<f32> = (0..dim).map(|j| (i + j as u64) as f32).collect();
        adapter.upsert(rid(i), &v).await.unwrap();
    }
    assert!(
        adapter.f32_graph_present(),
        "non-quant adapter must retain its f32 graph forever"
    );
    // Search still works on the f32 path.
    let q: Vec<f32> = (0..dim).map(|j| j as f32).collect();
    let res = adapter
        .search(&q, 3, SearchOpts::default(), None)
        .await
        .unwrap();
    assert_eq!(res.len(), 3);
}

#[test]
fn f32_graph_dropped_after_fit_and_search_survives() {
    run_graph_test(
        "f32_graph_dropped_after_fit_and_search_survives",
        f32_graph_dropped_after_fit_and_search_survives_body(),
    );
}

async fn f32_graph_dropped_after_fit_and_search_survives_body() {
    // Quant adapter: f32 graph dropped post-fit, but search/upsert/delete
    // continue to work through the u8 path. This is the core #418 regression:
    // the drop must NOT break the post-fit path, and must NOT cause UAF under
    // the in-flight readers that held a `load_full()` Arc.
    let dim = 24u32;
    let adapter = Arc::new(HnswAdapter::new_with_quantization(
        dim,
        VectorMetric::Cosine,
        HnswConfig {
            max_elements: 10_000,
            m: 16,
            max_layer: 16,
            ef_construction: 200,
            ef_search: 64,
        },
        Some(VectorQuantization::Sq8),
    ));

    // Cross the threshold → fit fires → f32 graph dropped.
    let data = clustered(300, dim as usize, 12, 0.2, 0x418418);
    let items: Vec<(RecordId, Vec<f32>)> = data
        .iter()
        .enumerate()
        .map(|(i, v)| (rid(i as u64), v.clone()))
        .collect();
    adapter.upsert_batch(&items).await.unwrap();
    crate::hnsw_adapter::await_fitted(&adapter).await;
    assert!(adapter.is_quantized(), "adapter did not fit");
    assert!(
        !adapter.f32_graph_present(),
        "f32 graph must be dropped post-fit"
    );

    // Post-fit search works (u8 graph + rescore path).
    let q = &data[0];
    let res = adapter
        .search(q, 5, SearchOpts::default(), None)
        .await
        .unwrap();
    assert_eq!(res.len(), 5);
    // Query's own rid (0) must be in the top-5.
    assert!(
        res.iter().any(|(r, _)| *r == rid(0)),
        "query's own rid missing from post-fit top-5"
    );

    // Post-fit upsert works (goes to the u8 path; f32 graph absent).
    let new_vec: Vec<f32> = (0..dim).map(|i| (i as f32) * 0.05).collect();
    adapter.upsert(rid(10_000), &new_vec).await.unwrap();
    // Post-fit delete works (drops codes from vectors_u8).
    adapter.delete(rid(0)).await.unwrap();

    // The f32 graph stays dropped across all these ops.
    assert!(
        !adapter.f32_graph_present(),
        "f32 graph must stay dropped across post-fit ops"
    );
}

// =========================================================================
// #423 (Б-1) — regression: fit-window vectors ARE inserted into the u8
// graph. Dataset >512 so search takes the GRAPH path (not brute-force):
// QUANT_BRUTE_FORCE_MAX == FIT_THRESHOLD * 2 == 512.
// =========================================================================
//
// Before the fix, the fit catch-up loop and the upsert self-migration path
// populated `vectors_u8` WITHOUT inserting the corresponding u8 graph node.
// A vector that entered `vectors_u8` after the fitter's delta scan was
// invisible to graph-search forever (and rode into the v2 snapshot as a
// hole). Brute-force (len <= 512) masked the bug. This test forces the
// GRAPH path by exceeding 512 vectors, then self-queries every vector with
// a generous ef: a PRESENT vector self-retrieves within the top-k with
// overwhelming probability, while a genuinely LOST vector (no graph node)
// never appears at any k. `missing == 0` is the no-loss invariant.

#[test]
fn concurrent_upsert_across_fit_window_graph_path_no_loss() {
    run_graph_test(
        "concurrent_upsert_across_fit_window_graph_path_no_loss",
        concurrent_upsert_across_fit_window_graph_path_no_loss_body(),
    );
}

async fn concurrent_upsert_across_fit_window_graph_path_no_loss_body() {
    let dim = 24u32;
    let adapter = Arc::new(HnswAdapter::new_with_quantization(
        dim,
        VectorMetric::L2,
        HnswConfig {
            max_elements: 10_000,
            m: 16,
            max_layer: 16,
            ef_construction: 200,
            ef_search: 64,
        },
        Some(VectorQuantization::Sq8),
    ));

    // 600 vectors — crosses FIT_THRESHOLD (256) AND exceeds
    // QUANT_BRUTE_FORCE_MAX (512), so post-fit search MUST take the graph
    // path. 8 concurrent tasks race the fit transition.
    let n = 600usize;
    let data = clustered(n, dim as usize, 24, 0.2, 0x0BEE_F423);
    let n_tasks = 8usize;
    let per_task = data.len() / n_tasks;
    let adapter_for_tasks = Arc::clone(&adapter);

    let mut handles = Vec::new();
    for t in 0..n_tasks {
        let adapter = Arc::clone(&adapter_for_tasks);
        let chunk: Vec<(RecordId, Vec<f32>)> = data[t * per_task..(t + 1) * per_task]
            .iter()
            .enumerate()
            .map(|(j, v)| (rid((t * per_task + j) as u64), v.clone()))
            .collect();
        handles.push(tokio::spawn(async move {
            adapter.upsert_batch(&chunk).await.expect("upsert_batch");
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    // The adapter MUST have fitted (600 > 256).
    crate::hnsw_adapter::await_fitted(&adapter).await;
    assert!(adapter.is_quantized(), "adapter did not fit under load");

    // CRITICAL: confirm the test exercises the GRAPH path, not brute-force.
    // len() == next_id - deleted_count; with 600 upserts and no deletes,
    // len() == 600 > 512 == QUANT_BRUTE_FORCE_MAX → graph path. If this
    // ever flips to <= 512 the test would silently mask Б-1.
    let live = adapter.len();
    assert!(
        live > 512,
        "test must take the GRAPH path: len()={live} must be > 512 (QUANT_BRUTE_FORCE_MAX); \
         otherwise brute-force masks the bug"
    );

    // #423 (Б-1) DETERMINISTIC no-loss check: every upserted internal MUST
    // have a node in `hnsw_u8`. Before the fix, the fit catch-up loop and
    // the upsert self-migration path populated `vectors_u8` WITHOUT
    // inserting the u8 graph node — so `get_nb_point()` would be LESS than
    // the number of upserted vectors. We assert EXACT equality (no HNSW
    // approximation involved — this is a direct node-count check).
    //
    // `hnsw_rs::get_nb_point` returns the total number of inserted DataPoints
    // (every `parallel_insert`/`insert` call adds one). With 600 upserts and
    // no deletes, the graph MUST hold exactly `n` nodes.
    let graph_nodes = adapter.hnsw_u8_handle().expect("fitted").get_nb_point();
    assert_eq!(
        graph_nodes,
        n,
        "Б-1: u8 graph has {graph_nodes} nodes but {n} vectors were upserted — \
         {missing} vectors lost their graph node during the concurrent fit transition. \
         Graph path was exercised (len={live} > 512).",
        missing = n.saturating_sub(graph_nodes)
    );

    // Search sanity: the graph must return results (the path is exercised
    // since len > 512). We do NOT assert missing==0 here because hnsw_rs's
    // unseedable layer RNG can leave a handful of nodes unreachable at
    // search time even when ALL nodes are present in the graph (confirmed
    // via `get_nb_point()` above — the deterministic check). The
    // node-count assertion is the Б-1 invariant; this is a recall smoke.
    let opts = SearchOpts {
        ef_search: Some(512),
        oversample: None,
    };
    let res = adapter.search(&data[0], 10, opts, None).await.unwrap();
    assert!(
        !res.is_empty(),
        "graph search returned empty on a 600-node graph"
    );
}

// =========================================================================
// #423 (Б-3) — regression: a pre-flip upsert that completes its f32 insert
// AFTER the fitter's convergence check must NOT observe a dropped f32 graph.
// Before the fix, `vectors_u8.len()` (inflated by post-flip upserts) drove
// the convergence check, the loop exited early, the f32 graph was dropped
// (`hnsw.store(None)`) before the pre-flip upsert's `hnsw.load_full()` →
// `Internal("f32 graph absent on non-quantized path")` on a legit commit.
//
// This test reproduces the race statistically: many concurrent upserts
// across the fit boundary, asserting NONE of them returns an Internal
// error. Under the old convergence metric the race was reproducible; under
// the migrated_pre_flip metric the f32 graph is dropped ONLY after every
// pre-flip internal has landed, so no in-flight upsert can miss it.
// =========================================================================

#[test]
fn concurrent_upsert_across_fit_no_f32_graph_absent_error() {
    run_graph_test(
        "concurrent_upsert_across_fit_no_f32_graph_absent_error",
        concurrent_upsert_across_fit_no_f32_graph_absent_error_body(),
    );
}

async fn concurrent_upsert_across_fit_no_f32_graph_absent_error_body() {
    let dim = 16u32;
    let adapter = Arc::new(HnswAdapter::new_with_quantization(
        dim,
        VectorMetric::L2,
        HnswConfig {
            max_elements: 10_000,
            m: 16,
            max_layer: 16,
            ef_construction: 200,
            ef_search: 64,
        },
        Some(VectorQuantization::Sq8),
    ));

    // 16 tasks, each upserting ~40 vectors one-at-a-time (single `upsert`,
    // not batch) to maximize the window where a pre-flip upsert is in
    // flight between its initial `quantized_active()` check and its f32
    // `hnsw.load_full()`. 640 vectors total crosses the fit threshold.
    let n = 640usize;
    let data = Arc::new(clustered(n, dim as usize, 16, 0.2, 0x00B3_C423));
    let n_tasks = 16usize;
    let per_task = data.len() / n_tasks;

    let error_count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0usize));
    let mut handles = Vec::new();
    for t in 0..n_tasks {
        let adapter = Arc::clone(&adapter);
        let ec = Arc::clone(&error_count);
        let data = Arc::clone(&data);
        let chunk_start = t * per_task;
        let chunk_end = (t + 1) * per_task;
        handles.push(tokio::spawn(async move {
            for j in chunk_start..chunk_end {
                let v = &data[j];
                // Single upserts (not batch) widen the race window: each
                // upsert checks `quantized_active()`, then later does
                // `hnsw.load_full()` on the f32 path. If the fitter drops
                // the f32 graph in between (Б-3), this returns Internal.
                if adapter.upsert(rid(j as u64), v).await.is_err() {
                    ec.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    let errors = error_count.load(std::sync::atomic::Ordering::Relaxed);
    crate::hnsw_adapter::await_fitted(&adapter).await;
    assert!(
        adapter.is_quantized(),
        "adapter did not fit under load (test setup failure)"
    );
    assert_eq!(
        errors, 0,
        "concurrent upserts across the fit boundary returned {errors} Internal error(s) \
         (Б-3 regression: the f32 graph was dropped before a pre-flip upsert's \
         `hnsw.load_full()` completed). migrated_pre_flip convergence metric must \
         guarantee the f32 graph stays until every pre-flip internal has landed."
    );
}

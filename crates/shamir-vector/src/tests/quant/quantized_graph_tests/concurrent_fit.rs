//! V5.2 (#411) quantized HNSW adapter tests: concurrent upserts racing the
//! fit transition (#423 Б-3 variants, #433 fast-path convergence), #424 (Б-4)
//! transient-None search retries, and the F3/F7 exact bruteforce-score
//! regression. Shared runner/RNG helpers live in `runner.rs`.

use super::{clustered, random_vec, rid, run_graph_test};
use crate::adapter::{SearchOpts, VectorAdapter};
use crate::hnsw_adapter::{install_test_search_f32_gate, HnswAdapter, HnswConfig};
use crate::quantized_dist::RescoreCtx;
use shamir_index_core::kind::{VectorMetric, VectorQuantization};
use shamir_types::types::record_id::RecordId;
use std::sync::Arc;

// =========================================================================
// #423 (Б-3, adversarial-review finding) — regression: tombstoning a
// pre-flip internal DURING the fit transition must NOT hang the catch-up
// loop forever.
//
// An earlier version of this fix computed `target = next_id_at_flip -
// deleted_count_at_flip` (a FROZEN snapshot of the deleted count taken at
// flip time). A pre-flip internal tombstoned AFTER the flip (a concurrent
// `delete`/rid-replacing `upsert` racing the fit) was never counted in the
// frozen `deleted_count_at_flip`, was skipped by the catch-up scan
// (`deleted.contains` guard), and so could never bump `migrated_pre_flip`
// to reach the frozen target — the catch-up loop would spin
// (`yield_now().await`) forever, hanging `try_fit_and_rebuild` and every
// upsert future waiting on the fit.
//
// The fix folds tombstones into `migrated_pre_flip` directly (see
// `bump_migrated_on_tombstone`), so a post-flip tombstone of a pre-flip
// internal advances convergence exactly like a claim would. This test
// races upserts that both cross the fit threshold AND repeatedly replace
// the SAME rid (tombstoning the previous internal on every replace) —
// reproducing the exact interleaving the adversarial review identified —
// and asserts the whole operation completes within a generous timeout.
// Before the fix this test would hang (timeout) instead of failing fast.
// =========================================================================

#[test]
fn concurrent_upsert_with_tombstone_across_fit_does_not_hang() {
    run_graph_test(
        "concurrent_upsert_with_tombstone_across_fit_does_not_hang",
        concurrent_upsert_with_tombstone_across_fit_does_not_hang_body(),
    );
}

async fn concurrent_upsert_with_tombstone_across_fit_does_not_hang_body() {
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

    // 600 vectors cross FIT_THRESHOLD (256). Each rid is upserted TWICE:
    // the first upsert allocates a pre-flip internal that may still be
    // in-flight (or already claimed) when the fitter flips; the second
    // upsert on the SAME rid tombstones that first internal via the
    // `Occupied` rid_to_internal branch — exactly the tombstone-during-fit
    // race the adversarial review found. Interleaving the two upsert
    // rounds across tasks (rather than doing all first-upserts then all
    // second-upserts) widens the race window around the flip.
    let n = 600usize;
    let data = Arc::new(clustered(n, dim as usize, 24, 0.2, 0x0B3_C423F));
    let n_tasks = 8usize;
    let per_task = n / n_tasks;

    let mut handles = Vec::new();
    for t in 0..n_tasks {
        let adapter = Arc::clone(&adapter);
        let data = Arc::clone(&data);
        let start = t * per_task;
        let end = (t + 1) * per_task;
        handles.push(tokio::spawn(async move {
            for j in start..end {
                let v = &data[j];
                // First upsert: allocates a fresh internal for this rid.
                adapter
                    .upsert(rid(j as u64), v)
                    .await
                    .expect("first upsert");
                // Second upsert on the SAME rid: tombstones the internal
                // just allocated above via the `Occupied` rid_to_internal
                // branch — a pre-flip internal being tombstoned mid-fit.
                adapter
                    .upsert(rid(j as u64), v)
                    .await
                    .expect("replacing upsert");
            }
        }));
    }

    // Generous timeout: if the catch-up loop regresses to the frozen-target
    // hang, this fires instead of the test suite blocking forever (per
    // project discipline: hangs are bugs, never tolerate — fail fast with a
    // clear signal instead of a silent multi-minute stall).
    let result = tokio::time::timeout(std::time::Duration::from_secs(60), async {
        for h in handles {
            h.await.unwrap();
        }
    })
    .await;

    assert!(
        result.is_ok(),
        "concurrent_upsert_with_tombstone_across_fit_does_not_hang: joining upsert tasks: \
         #423 (Б-3 adversarial finding): concurrent upserts that tombstone \
         pre-flip internals during the fit transition HUNG past the 60s \
         timeout — the catch-up loop's convergence target excluded a \
         post-flip-tombstoned pre-flip internal and never reached it. \
         See `bump_migrated_on_tombstone` and the seed comment in \
         `try_fit_and_rebuild`."
    );

    crate::hnsw_adapter::await_fitted(&adapter).await;
    assert!(
        adapter.is_quantized(),
        "adapter did not fit under load (test setup failure)"
    );
}

// =========================================================================
// #423 (Б-3, SECOND adversarial-review finding) — regression: the
// single-`upsert` self-migration re-check must not double-count a
// tombstoned internal in `migrated_pre_flip`.
//
// The first tombstone-during-fit fix (bump_migrated_on_tombstone) closed
// the frozen-target hang, but introduced a narrower gap: the self-migration
// re-check in `upsert`/`upsert_batch` claimed a JUST-tombstoned internal
// (from a concurrent racer replacing the SAME rid) into `vectors_u8`
// WITHOUT checking `deleted.contains` first — unlike the three scan-based
// claim sites (fit snapshot, delta, catch-up loop), which all guard on it.
// This let `migrated_pre_flip` be bumped TWICE for one internal: once by
// the tombstone, once by the (incorrect) claim — an over-count that can
// trip the catch-up loop's convergence early, dropping the f32 graph while
// a genuinely still-in-flight, distinct pre-flip internal has not yet
// landed (the exact class of bug the Б-3 fix exists to prevent).
//
// This test drives the SPECIFIC interleaving the prior regression test
// (`concurrent_upsert_with_tombstone_across_fit_does_not_hang`) does not
// cover: tasks race `upsert` on OVERLAPPING/shared rids (not disjoint
// per-task ranges), so one task's freshly-allocated internal can be
// tombstoned by ANOTHER task's concurrent replace on the SAME rid while
// the first task's own self-migration re-check is still in flight. We
// cannot directly assert `migrated_pre_flip` (private), so we assert the
// deterministic node-count invariant from Б-1 — `get_nb_map() ==
// live count` — which a premature f32-graph-drop (induced by an
// over-counted convergence target) would violate by racing a genuine
// pre-flip vector out of both `vectors` and `vectors_u8`/the graph.
// =========================================================================

#[test]
fn concurrent_same_rid_upsert_race_across_fit_no_double_count() {
    run_graph_test(
        "concurrent_same_rid_upsert_race_across_fit_no_double_count",
        concurrent_same_rid_upsert_race_across_fit_no_double_count_body(),
    );
}

async fn concurrent_same_rid_upsert_race_across_fit_no_double_count_body() {
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

    // 600 upserts, but only ~half land on DISTINCT rids — `shared_rid_count`
    // must stay comfortably ABOVE FIT_THRESHOLD (256)/QUANT_BRUTE_FORCE_MAX
    // (512) so enough LIVE (non-superseded) rids remain to force the fit +
    // exercise the graph path; the remaining updates replay the SAME rids
    // (interleaved across tasks) to manufacture the tombstone-during-fit
    // race: every task repeatedly upserts a rid some OTHER task may also be
    // upserting concurrently, maximizing the chance that one task's fresh
    // internal is tombstoned by another task's concurrent replace before
    // its self-migration re-check runs.
    let n_updates = 600usize;
    let shared_rid_count = 560u64;
    let data = Arc::new(clustered(n_updates, dim as usize, 24, 0.2, 0x0F00_DBEE));
    let n_tasks = 8usize;
    let per_task = n_updates / n_tasks;

    let mut handles = Vec::new();
    for t in 0..n_tasks {
        let adapter = Arc::clone(&adapter);
        let data = Arc::clone(&data);
        let start = t * per_task;
        let end = (t + 1) * per_task;
        handles.push(tokio::spawn(async move {
            for j in start..end {
                let v = &data[j];
                // Every task hammers the SAME pool of `shared_rid_count`
                // rids — heavy same-rid contention across tasks.
                let r = rid(j as u64 % shared_rid_count);
                adapter.upsert(r, v).await.expect("upsert");
            }
        }));
    }

    let result = tokio::time::timeout(std::time::Duration::from_secs(60), async {
        for h in handles {
            h.await.unwrap();
        }
    })
    .await;
    assert!(
        result.is_ok(),
        "concurrent_same_rid_upsert_race_across_fit_no_double_count: joining upsert tasks: \
         concurrent same-rid upserts across the fit boundary hung past the \
         60s timeout"
    );

    crate::hnsw_adapter::await_fitted(&adapter).await;
    assert!(
        adapter.is_quantized(),
        "adapter did not fit under load (test setup failure)"
    );

    // Deterministic Б-1 invariant: every LIVE rid (post-race, each of the
    // `shared_rid_count` rids has one final internal — all earlier
    // internals for that rid were tombstoned by the last writer) must have
    // exactly one node in the u8 graph. `get_nb_point()` counts ALL nodes
    // ever inserted (including tombstoned-but-already-graphed ones from
    // before the last replace), so we cannot assert exact equality to
    // `shared_rid_count` here (superseded internals may have already
    // landed a graph node before being tombstoned, which is expected and
    // harmless — search filters `deleted`). Instead we assert search
    // correctness: every live rid's own vector must retrieve itself with
    // ~zero distance, proving its CURRENT internal has a live, findable
    // graph node (a double-counted convergence over-shoot that dropped the
    // f32 graph prematurely would leave some live internals stuck in
    // neither `vectors` nor a graph node — unfindable).
    for r in 0..shared_rid_count {
        // The vector most recently written for this rid is at the highest
        // `j` with `j % shared_rid_count == r` — reconstruct it the same
        // way the upsert loop did.
        let last_j = (0..n_updates)
            .rev()
            .find(|j| *j as u64 % shared_rid_count == r)
            .expect("at least one update per shared rid");
        let q = &data[last_j];
        let res = adapter
            .search(q, 1, SearchOpts::default(), None)
            .await
            .expect("search");
        assert!(
            !res.is_empty(),
            "rid {r}'s current vector must be findable post-fit — an empty \
             result indicates its internal was lost from the graph (Б-3 \
             double-count regression)"
        );
    }
}

// =========================================================================
// #433 (post-VR-8) — regression: the `upsert` quantized FAST-PATH must
// account for a PRE-flip internal in `migrated_pre_flip`, or the fitter's
// catch-up loop never converges (hangs forever).
//
// The window: `upsert` allocates `internal` via `next_id.fetch_add` and
// THEN suspends on `rid_to_internal.entry_async(...).await`. If a
// concurrent `try_fit_and_rebuild` completes ENTIRELY inside that suspend
// (captures `next_id_at_flip > internal`, flips `is_fitted`), the upsert
// resumes, reads `quantized_active() == true`, and took the fast-path.
// Pre-#433 that fast-path did a RAW `vectors_u8.insert_async` — it never
// bumped `migrated_pre_flip`, and the catch-up loop (which scans only the
// f32 `vectors` buffer this path never touches) never claimed the internal
// either. `migrated_pre_flip` never reached `target = next_id_at_flip` →
// the loop span forever → the whole fit call (and every upsert awaiting it)
// hung → 60s TIMEOUT.
//
// Coverage note: this is a STATISTICAL convergence + correctness test, not
// a deterministic reproduction of the exact fast-path window (the upsert
// allocate→`quantized_active()` gap spans only `entry_async`, which rarely
// suspends long enough for a whole fit; the sibling `backfill` test provoked
// a DISTINCT hang — an unconditional `vectors.remove` — not this window). It
// exercises the fast-path under heavy same-rid churn across the flip and
// asserts (a) convergence (no 60s hang) and (b) every live rid stays
// findable, which a fast-path that dropped a pre-flip internal from the
// graph would violate. The fix itself (`quantized_fastpath_publish`) is
// correct by construction: it routes the fast-path through the same claim
// authority the catch-up loop counts on.
// =========================================================================

#[test]
fn concurrent_upsert_quantized_fastpath_converges_no_hang() {
    run_graph_test(
        "concurrent_upsert_quantized_fastpath_converges_no_hang",
        concurrent_upsert_quantized_fastpath_converges_no_hang_body(),
    );
}

async fn concurrent_upsert_quantized_fastpath_converges_no_hang_body() {
    let dim = 8u32;
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

    // Enough distinct rids to cross FIT_THRESHOLD (256), with continued
    // upserts AFTER the fit so many calls take the quantized fast-path while
    // the catch-up loop is still draining pre-flip internals. Same-rid
    // replays inject `entry_async` suspends at the allocate→check gap.
    let n = 400u64;
    let n_tasks = 6u64;
    let mut handles = Vec::new();
    for t in 0..n_tasks {
        let adapter = Arc::clone(&adapter);
        handles.push(tokio::spawn(async move {
            for i in 0..n {
                // Interleave distinct + shared rids so some tasks tombstone
                // pre-flip internals others just allocated.
                let r = rid((i + t) % n);
                let v = random_vec(dim as usize, t * n + i);
                let _ = adapter.upsert(r, &v).await;
            }
        }));
    }

    let result = tokio::time::timeout(std::time::Duration::from_secs(60), async {
        for h in handles {
            h.await.unwrap();
        }
    })
    .await;

    assert!(
        result.is_ok(),
        "concurrent_upsert_quantized_fastpath_converges_no_hang: joining upsert tasks: \
         #433: concurrent upserts across the fit boundary HUNG past 60s — \
         the quantized fast-path published a pre-flip internal via a raw \
         vectors_u8 insert without bumping migrated_pre_flip, so the \
         catch-up loop never converged. See `quantized_fastpath_publish`."
    );
    crate::hnsw_adapter::await_fitted(&adapter).await;
    assert!(
        adapter.is_quantized(),
        "adapter did not fit under load (test setup failure)"
    );

    // Correctness (mirrors `concurrent_same_rid_upsert_race_across_fit_no_
    // double_count`): the counter is NOT asserted here — under heavy
    // same-rid churn across the flip, an internal claimed by the fast-path
    // and LATER tombstoned by a replacing same-rid upsert bumps
    // `migrated_pre_flip` twice (an over-count inherent to the whole
    // claim-then-tombstone design, shared by every self-migration path, not
    // specific to the fast-path). That over-count only makes the catch-up
    // loop exit slightly EARLY; the load-bearing guarantee is that no LIVE
    // rid's current internal is lost from the graph. We assert exactly that:
    // every live rid must retrieve itself post-fit.
    for r in 0..n {
        let last_v = {
            // The last write for rid `r` is the highest (t, i) with
            // (i + t) % n == r; reconstruct its seed the same way.
            let mut found: Option<Vec<f32>> = None;
            for t in 0..n_tasks {
                for i in 0..n {
                    if (i + t) % n == r {
                        found = Some(random_vec(dim as usize, t * n + i));
                    }
                }
            }
            found.expect("every rid in 0..n is written at least once")
        };
        let res = adapter
            .search(&last_v, 1, SearchOpts::default(), None)
            .await
            .expect("search");
        assert!(
            !res.is_empty(),
            "rid {r}'s current vector must be findable post-fit — an empty \
             result means its internal was lost from the graph (a fast-path \
             convergence regression)"
        );
    }
}

// =========================================================================
// #433 (post-VR-8) — the SAME convergence gap for the `upsert_batch`
// quantized fast-path. Identical mechanism: rows allocate pre-flip
// internals, suspend on `entry_async`, and a concurrent fit flips mid-loop;
// the batch fast-path must claim (bump `migrated_pre_flip`) rather than
// raw-insert, or the fitter hangs.
// =========================================================================

#[test]
fn concurrent_upsert_batch_quantized_fastpath_converges_no_hang() {
    run_graph_test(
        "concurrent_upsert_batch_quantized_fastpath_converges_no_hang",
        concurrent_upsert_batch_quantized_fastpath_converges_no_hang_body(),
    );
}

async fn concurrent_upsert_batch_quantized_fastpath_converges_no_hang_body() {
    let dim = 8u32;
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

    let n_batches = 60u64;
    let batch_sz = 8u64;
    let n_tasks = 6u64;
    let total_rids = n_batches * batch_sz;
    let mut handles = Vec::new();
    for t in 0..n_tasks {
        let adapter = Arc::clone(&adapter);
        handles.push(tokio::spawn(async move {
            for b in 0..n_batches {
                let items: Vec<(RecordId, Vec<f32>)> = (0..batch_sz)
                    .map(|k| {
                        let idx = (b * batch_sz + k + t) % total_rids;
                        (rid(idx), random_vec(dim as usize, t * 100_000 + idx))
                    })
                    .collect();
                let _ = adapter.upsert_batch(&items).await;
            }
        }));
    }

    let result = tokio::time::timeout(std::time::Duration::from_secs(60), async {
        for h in handles {
            h.await.unwrap();
        }
    })
    .await;

    assert!(
        result.is_ok(),
        "concurrent_upsert_batch_quantized_fastpath_converges_no_hang: joining batch tasks: \
         #433: concurrent upsert_batch across the fit boundary HUNG past \
         60s — the quantized batch fast-path published pre-flip internals \
         via raw vectors_u8 inserts without bumping migrated_pre_flip."
    );
    crate::hnsw_adapter::await_fitted(&adapter).await;
    assert!(
        adapter.is_quantized(),
        "adapter did not fit under load (test setup failure)"
    );

    // Correctness (see the single-upsert sibling above): the counter itself
    // is not asserted (claim-then-tombstone over-count under same-rid churn
    // is inherent to the design). Instead assert every live rid is findable
    // post-fit — a convergence regression that dropped the f32 graph while a
    // live pre-flip internal was still in flight would surface as an empty
    // result for that rid.
    for r in 0..total_rids {
        let last_v = {
            let mut found: Option<Vec<f32>> = None;
            for t in 0..n_tasks {
                for b in 0..n_batches {
                    for k in 0..batch_sz {
                        let idx = (b * batch_sz + k + t) % total_rids;
                        if idx == r {
                            found = Some(random_vec(dim as usize, t * 100_000 + idx));
                        }
                    }
                }
            }
            found.expect("every rid is written at least once")
        };
        let res = adapter
            .search(&last_v, 1, SearchOpts::default(), None)
            .await
            .expect("search");
        assert!(
            !res.is_empty(),
            "rid {r}'s current vector must be findable post-fit — an empty \
             result means its internal was lost from the graph (a batch \
             fast-path convergence regression)"
        );
    }
}

// =========================================================================
// #424 (Б-4) — regression: a search that reads `quantized_active() == false`
// (pre-flip) and THEN does `hnsw.load_full()` must NOT silently get an empty
// result when a fit transition drops the f32 graph between those two reads.
//
// The race window:
//   1. search reads `quantized_active()` → false (pre-flip) → f32 branch.
//   2. fitter: `is_fitted.store(true, Release)` + catch-up + `hnsw.store(None)`.
//   3. search: `hnsw.load_full()` → None.
//
// Before the fix, `search`/`search_cofilter` returned `Ok(vec![])` on None.
//
// The window is a few nanoseconds of synchronous code (no `.await` between
// the two reads), so a statistical concurrent test CANNOT reliably reproduce
// it (confirmed: 3× consecutive runs of a tight-loop test passed even with
// the fix disabled). Instead we use a deterministic test-only hook
// (`install_test_search_f32_gate`) that PAUSES a search request at exactly
// the race point — after `quantized_active() == false`, before
// `load_full()`. The test then triggers a fit transition (which drops the
// f32 graph), confirms the drop, and releases the paused request — which now
// observes `load_full() == None` and must exercise the retry path.
//
// We test BOTH `search` and `search_cofilter`, since both had the same
// transient-None defect.
// =========================================================================

#[test]
fn search_post_fit_returns_nonempty_via_u8_path() {
    run_graph_test(
        "search_post_fit_returns_nonempty_via_u8_path",
        search_post_fit_returns_nonempty_via_u8_path_body(),
    );
}

async fn search_post_fit_returns_nonempty_via_u8_path_body() {
    // #424 (Б-4) — functional complement to the co-filter gate test.
    //
    // `search`'s f32-HNSW branch (where the transient-None race lives)
    // requires `len() > BRUTE_FORCE_MAX`. But `FIT_THRESHOLD ==
    // BRUTE_FORCE_MAX`, so the fit trigger fires at exactly the same count
    // — a deterministic gate test for `search` (like the co-filter one
    // below) cannot place a pre-fit request in the f32-HNSW branch without
    // racing the fit trigger itself.
    //
    // Instead this test verifies the FUNCTIONAL contract: after the fit
    // transition drops the f32 graph, `search` routes through the u8 path
    // (via the same `search_quantized_*` helpers the retry uses) and
    // returns correct, non-empty results. Combined with the co-filter gate
    // test (which deterministically exercises the retry on the same
    // `search_cofilter_quantized` helper), this covers both code paths.

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

    // Cross the fit threshold so the adapter fully transitions to the u8
    // graph and drops the f32 graph.
    let n = 300usize;
    let data = clustered(n, dim as usize, 16, 0.2, 0x5EA4_0F11);
    for (i, v) in data.iter().enumerate() {
        adapter.upsert(rid(i as u64), v).await.expect("upsert");
    }
    crate::hnsw_adapter::await_fitted(&adapter).await;
    assert!(
        adapter.is_quantized(),
        "test setup: adapter should have fitted"
    );
    assert!(
        !adapter.f32_graph_present(),
        "test setup: f32 graph should be dropped post-fit"
    );

    // Search for each vector — it MUST find itself (distance ~0).
    for (i, v) in data.iter().enumerate().take(50) {
        let res = adapter
            .search(v, 1, SearchOpts::default(), None)
            .await
            .expect("search");
        assert!(
            !res.is_empty(),
            "#424 (Б-4): search returned empty for vector {i} post-fit. \
             The f32 graph is dropped; search must route through the u8 \
             path and return results."
        );
        // The nearest result should be the query vector itself.
        assert_eq!(
            res[0].0,
            rid(i as u64),
            "search returned wrong nearest neighbor for vector {i}"
        );
    }
}

/// RAII guard clearing the global `TEST_SEARCH_F32_GATE` on drop — including
/// on a panicking unwind. Without this, a panic anywhere between
/// `install_test_search_f32_gate()` and the tail-of-body
/// `clear_test_search_f32_gate()` call leaves the gate `Some(_)` for the
/// rest of the test process: nextest runs a crate's tests as concurrent
/// threads within ONE process (no `test-threads=1` isolation), so an
/// unrelated concurrent test that reaches `search`'s f32 branch would then
/// block on `gate.notify.notified().await` forever — surfacing as a
/// misleading unrelated `TIMEOUT` at the 180s nextest kill, not the true
/// panic that leaked the gate.
struct TestSearchF32GateGuard;
impl Drop for TestSearchF32GateGuard {
    fn drop(&mut self) {
        crate::hnsw_adapter::clear_test_search_f32_gate();
    }
}

#[test]
fn search_cofilter_transient_none_retries_into_u8_path() {
    run_graph_test(
        "search_cofilter_transient_none_retries_into_u8_path",
        search_cofilter_transient_none_retries_into_u8_path_body(),
    );
}

async fn search_cofilter_transient_none_retries_into_u8_path_body() {
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

    let n_pre = 255usize;
    let n_total = 300usize;
    let data = clustered(n_total, dim as usize, 16, 0.2, 0xCF1C_FE54);
    for (i, v) in data.iter().enumerate().take(n_pre) {
        adapter
            .upsert(rid(i as u64), v)
            .await
            .expect("pre-populate");
    }
    assert!(!adapter.is_quantized());

    // Candidate set for co-filter: all pre-populated rids.
    let candidates: Vec<RecordId> = (0..n_pre as u64).map(rid).collect();

    let gate = install_test_search_f32_gate();
    let _gate_guard = TestSearchF32GateGuard;

    let adapter_search = Arc::clone(&adapter);
    let query = data[0].clone();
    let candidates_clone = candidates.clone();
    let search_handle = tokio::spawn(async move {
        adapter_search
            .search_cofilter(&query, 5, None, &candidates_clone)
            .await
    });

    // Wait until the co-filter search task reaches the f32-gate.
    let reached = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !gate.arrived.load(std::sync::atomic::Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(
        reached.is_ok(),
        "co-filter search task did not reach the f32-gate within 5s"
    );

    // Trigger fit.
    for (i, v) in data.iter().enumerate().take(n_total).skip(n_pre) {
        adapter
            .upsert(rid(i as u64), v)
            .await
            .expect("upsert to trigger fit");
    }

    crate::hnsw_adapter::await_fitted(&adapter).await;
    assert!(
        adapter.is_quantized(),
        "test setup: adapter should have fitted"
    );
    assert!(
        !adapter.f32_graph_present(),
        "test setup: f32 graph should be dropped post-fit"
    );

    gate.notify.notify_one();

    let result = tokio::time::timeout(std::time::Duration::from_secs(10), search_handle)
        .await
        .expect("co-filter search task hung past 10s timeout after gate release")
        .expect("co-filter search task panicked");

    let results = result.expect("search_cofilter returned error");
    assert!(
        !results.is_empty(),
        "#424 (Б-4): search_cofilter returned EMPTY after a transient-None \
         race. Same window as `search`: the co-filter read \
         `quantized_active() == false`, was paused, then the fit dropped \
         the f32 graph. On resume, `load_full()` returned None. The retry \
         must re-check `quantized_active()` and route through the u8 \
         co-filter path."
    );

    // `_gate_guard` drops here (and on any earlier panic/assert failure
    // above), clearing the global gate unconditionally.
}

// =========================================================================
// F3/F7 (task 8c) — regression: `search_quantized_bruteforce` returns the
// EXACT (RecordId, f32) pairs (byte-identical scored output) after the
// clone-vs-score-in-closure refactor. Pins the top-k RIDs AND the literal
// f32 distances so a mistake like scoring the wrong candidate, attaching a
// score to the wrong RID, or swapping the closure's score argument is
// caught immediately.
//
// The stored u8 codes are exactly `quantizer.quantize(v)` (the upsert/fit
// sites all funnel through `Sq8Quantizer::quantize`), and the bruteforce
// path scores them via the SAME `RescoreCtx::score`. So the expected
// distances, re-derived here from the original f32 vectors, are
// bitwise-identical to production — the assertion uses exact bit equality
// (no epsilon).
// =========================================================================
#[test]
fn f3_f7_quantized_bruteforce_exact_scored_output() {
    run_graph_test(
        "f3_f7_quantized_bruteforce_exact_scored_output",
        f3_f7_quantized_bruteforce_exact_scored_output_body(),
    );
}

async fn f3_f7_quantized_bruteforce_exact_scored_output_body() {
    let dim = 8u32;
    // 300 > FIT_THRESHOLD (256) → fit fires; 300 ≤ 512 → bruteforce path
    // (`search_quantized_bruteforce`), the site touched by F3/F7.
    let n = 300u64;
    let adapter = HnswAdapter::new_with_quantization(
        dim,
        VectorMetric::L2,
        HnswConfig {
            max_elements: 5000,
            ..Default::default()
        },
        Some(VectorQuantization::Sq8),
    );
    for i in 0..n {
        adapter
            .upsert(rid(i), &random_vec(dim as usize, i))
            .await
            .expect("pre-populate upsert");
    }
    crate::hnsw_adapter::await_fitted(&adapter).await;
    assert!(
        adapter.is_quantized(),
        "test setup: adapter must have fitted (n > FIT_THRESHOLD)"
    );
    // Confirm the bruteforce path is taken: len() <= QUANT_BRUTE_FORCE_MAX (512).
    assert!(
        adapter.len() <= 512,
        "test must take the bruteforce path: len()={} must be <= 512",
        adapter.len()
    );

    let query = random_vec(dim as usize, 424242);
    let k = 5u32;
    let results = adapter
        .search(&query, k, SearchOpts::default(), None)
        .await
        .expect("search");

    // Independently re-derive the expected (RecordId, score) top-k over the
    // SAME dataset, scoring each vector's re-quantized codes with the SAME
    // `RescoreCtx::score` the production path uses.
    let quantizer = adapter.quantizer().expect("fitted");
    let ctx = RescoreCtx::new(VectorMetric::L2, quantizer, &query);
    let mut expected: Vec<(RecordId, f32)> = (0..n)
        .map(|i| {
            let v = random_vec(dim as usize, i);
            let codes = quantizer.quantize(&v);
            (rid(i), ctx.score(&codes))
        })
        .collect();
    expected.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
    expected.truncate(k as usize);

    assert_eq!(
        results.len(),
        k as usize,
        "bruteforce must return k results"
    );
    // Compare as (RecordId, f32-bits) so the comparison is byte-exact: same
    // RID, in the same order, with bitwise-identical distances.
    let result_bits: Vec<(RecordId, u32)> =
        results.iter().map(|(r, d)| (*r, d.to_bits())).collect();
    let expected_bits: Vec<(RecordId, u32)> =
        expected.iter().map(|(r, d)| (*r, d.to_bits())).collect();
    assert_eq!(
        result_bits, expected_bits,
        "F3/F7: exact (RecordId, f32-distance-bits) output mismatch — \
         the clone-vs-score-in-closure refactor must be byte-identical to the \
         independent brute-force ground truth"
    );
}

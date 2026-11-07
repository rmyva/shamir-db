//! H3 — vector index (HNSW) five-map mixed `_async`/`_sync` whole-runtime
//! deadlock regression tests.
//!
//! Same structural class as the #589 fix for the `cells` map
//! (`crates/shamir-tx/src/mvcc_store/mod.rs::publish_cell`, commit
//! `7a4abf62`) and the H1+H2 fix for `active_snapshots` + `locks`
//! (commit `621776bd`). Five scc::HashMaps inside the vector/HNSW index
//! subsystem previously mixed `_async` lock-acquiring ops with `_sync`
//! accessors on the SAME map:
//!
//! * **`deleted`** (tombstones) — `insert_async` (delete path),
//!   `contains_async` (every search-path candidate-filter probe) mixed with
//!   `contains_sync` (search hot paths, quantized fastpath guard),
//!   `insert_sync` (upsert/upsert_batch tombstone), `iter_sync` (snapshot
//!   serialisation / `collect_live_vectors`).
//! * **`vectors_u8`** (quantized codes) — `insert_async`/`remove_async`/
//!   `read_async` mixed with `iter_sync` (`search_quantized_bruteforce`,
//!   fit snapshot/delta/catch-up), `read_sync`, `contains_sync`.
//! * **`vectors`** (f32 buffer) — `insert_async`/`remove_async` mixed with
//!   `iter_sync` (fit snapshot/delta/catch-up), `read_sync`, `remove_sync`.
//! * **`rid_to_internal`** — `entry_async` (backfill/upsert/upsert_batch),
//!   `read_async`/`remove_async` mixed with `iter_sync` (snapshot
//!   serialisation, `collect_live_vectors`), `contains_sync`.
//!   **AMPLIFIER**: at least one `deleted.insert_sync` ran while the caller
//!   HELD an `entry_async`-acquired `rid_to_internal` exclusive entry — a
//!   cross-map chain widening the deadlock window.
//! * **`compaction_deleted_rids`** (cross-file: `hnsw_adapter.rs` +
//!   `vector_backend.rs`) — `insert_async` (double-write delete path in
//!   `vector_backend.rs`) mixed with `contains_sync` (backfill guard) and
//!   `iter_sync` (Step4b reconcile).
//! * **`rid_map`** (`scc::HashMap<usize, RecordId, THasher>`, reverse of
//!   `rid_to_internal`) — the SIXTH map, found during the H3 sweep and fixed
//!   in a follow-up. `insert_async` (upsert/upsert_batch/backfill_if_absent,
//!   6 sites) and `read_async` (every search path — bruteforce/graph/cofilter/
//!   prefilter + the f32 branches, 6 sites) mixed with the SYNCHRONOUS
//!   `iter_sync` in `for_each_rid_map` (snapshot serialisation). Same class
//!   as the five maps above.
//!
//! The interleaving that deadlocked before the fix (single-map `deleted`
//! variant):
//! 1. During or after a compaction burst, a `delete(rid)` task suspends in
//!    `deleted.insert_async(internal)` because an upsert's `insert_sync`
//!    briefly owns the bucket; on release, saa GRANTS the delete task the
//!    exclusive bucket lock while it sits in the run queue, unpolled.
//! 2. Concurrent search tasks (every candidate-filter probe during a search
//!    is `deleted.contains_sync`) PARK their worker threads on that same
//!    bucket.
//! 3. With search concurrency ≥ worker count, all workers park → the delete
//!    task is never polled → whole-runtime deadlock. On a
//!    `worker_threads=1` runtime, one search racing one delete suffices.
//!
//! **Fix**: EVERY lock-acquiring op on all five maps plus `rid_map` is now
//! synchronous (`insert_sync`/`remove_sync`/`entry_sync`/`read_sync`/
//! `contains_sync`). None of the closures involved suspend (simple inserts/
//! removes/reads/containment checks), so the conversion is mechanical — same
//! reasoning as the prior two fixes in this sweep.
//!
//! **Why these tests exist** (mirrors the established style from
//! `active_snapshots_deadlock_tests.rs`): this hazard is a RACE WINDOW,
//! not a deterministic deadlock — the exact interleaving of a handed-off
//! bucket lock task sitting in the run queue while all workers park on a
//! sync accessor only lands under specific nextest-parallelism timing.
//! The goal of the tests is BOTH (a) to exercise the interleaving so
//! nextest's parallelism has a real chance to catch a future regression
//! over time, AND (b) a NAMED bounded `tokio::time::timeout` so that a
//! real regression fails fast and identifiably (and specifically points at
//! this hazard) instead of hanging the entire nextest run with an
//! anonymous TIMEOUT. The timeout is NOT a workaround for flakiness — it
//! is this test's own guard against a real regression hanging the whole
//! suite (cf. `quantized_graph_tests.rs:1630`).

use crate::adapter::{SearchOpts, VectorAdapter};
use crate::hnsw_adapter::{HnswAdapter, HnswConfig};
use shamir_index_core::kind::{VectorMetric, VectorQuantization};
use shamir_types::types::record_id::RecordId;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

fn rid(n: u64) -> RecordId {
    let bytes = (n as u128).to_be_bytes();
    RecordId(bytes)
}

fn random_vec(dim: usize, seed: u64) -> Vec<f32> {
    let mut v = Vec::with_capacity(dim);
    let mut s = seed;
    for _ in 0..dim {
        s = s
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        v.push(((s >> 33) as f32) / (u32::MAX as f32) - 0.5);
    }
    v
}

/// Build a quantized adapter fitted past `FIT_THRESHOLD` (256) but small
/// enough to stay on the `search_quantized_bruteforce` path (≤
/// `QUANT_BRUTE_FORCE_MAX` = 512). Returns `(adapter, query_vec)` where
/// `query_vec` is a deterministic query.
///
/// The exact live-count window matters: `search` dispatches to
/// `search_quantized_bruteforce` (which `iter_sync`s `vectors_u8` and
/// `contains_sync`'s `deleted`) only when `quantized_active() && len() <=
/// QUANT_BRUTE_FORCE_MAX`.
async fn build_fitted_bruteforce_adapter() -> Arc<HnswAdapter> {
    let dim = 8u32;
    // 300 > FIT_THRESHOLD (256) → fit fires; 300 ≤ 512 → bruteforce path.
    let n = 300u64;
    let adapter = Arc::new(HnswAdapter::new_with_quantization(
        dim,
        VectorMetric::L2,
        HnswConfig {
            max_elements: 5000,
            ..Default::default()
        },
        Some(VectorQuantization::Sq8),
    ));
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
    adapter
}

/// Number of delete+re-upsert / search iterations per hammer task. Each
/// iteration exercises the exact interleaving under test: a delete hits
/// `deleted.insert_sync` + `vectors_u8.remove_sync` + `rid_to_internal`
/// while a concurrent search hits `deleted.contains_sync` +
/// `vectors_u8.iter_sync`.
const HAMMER_ITERS: usize = 150;

/// Primary regression: hammer delete+re-upsert of ONE rid racing
/// `search_quantized_bruteforce`-sized searches on a constrained
/// (`worker_threads = 2`) runtime. The adapter is fitted (quantized) and
/// small enough to stay on the bruteforce search path (which `iter_sync`s
/// `vectors_u8` and `contains_sync`'s `deleted` — the exact sync readers
/// that, before the fix, parked worker threads against an `insert_async`
/// task holding a handed-off bucket lock).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn h3_deadlock_deleted_vectors_u8_racing_search_bruteforce() {
    let adapter = build_fitted_bruteforce_adapter().await;
    let hot_rid = rid(0);
    let hot_vec = random_vec(8, 0);
    let query = random_vec(8, 42);
    let stop = Arc::new(AtomicBool::new(false));

    // Delete+re-upsert hammer: tight-loops `delete(hot_rid)` then
    // `upsert(hot_rid)` — exercising `deleted.insert_sync`,
    // `vectors_u8.remove_sync`, `rid_to_internal.entry_sync` (the
    // amplifier), and `deleted.contains_sync`.
    let muta = Arc::clone(&adapter);
    let stop_a = Arc::clone(&stop);
    let hot_rid_a = hot_rid;
    let hot_vec_a = hot_vec.clone();
    let hammer_a = tokio::spawn(async move {
        for _ in 0..HAMMER_ITERS {
            let _ = muta.delete(hot_rid_a).await;
            let _ = muta.upsert(hot_rid_a, &hot_vec_a).await;
            tokio::task::yield_now().await;
            if stop_a.load(Ordering::Relaxed) {
                return;
            }
        }
    });

    // Search hammer: tight-loops `search` — on a fitted adapter ≤512
    // vectors this dispatches to `search_quantized_bruteforce`, which
    // `iter_sync`s `vectors_u8` and `contains_sync`'s `deleted` for every
    // candidate. This is the exact sync reader that, before the fix,
    // parked the worker thread against a delete task holding a handed-off
    // `deleted` bucket lock.
    let mutb = Arc::clone(&adapter);
    let stop_b = Arc::clone(&stop);
    let query_b = query.clone();
    let hammer_b = tokio::spawn(async move {
        for _ in 0..HAMMER_ITERS {
            let _ = mutb.search(&query_b, 10, SearchOpts::default(), None).await;
            tokio::task::yield_now().await;
            if stop_b.load(Ordering::Relaxed) {
                return;
            }
        }
    });

    // Second search hammer to saturate both worker threads with
    // `contains_sync` probes — the worst case for the pre-fix hazard (both
    // workers parked on `deleted.contains_sync` while a delete task holds
    // the handed-off bucket lock).
    let mutc = Arc::clone(&adapter);
    let stop_c = Arc::clone(&stop);
    let query_c = query.clone();
    let hammer_c = tokio::spawn(async move {
        for _ in 0..HAMMER_ITERS {
            let _ = mutc.search(&query_c, 10, SearchOpts::default(), None).await;
            tokio::task::yield_now().await;
            if stop_c.load(Ordering::Relaxed) {
                return;
            }
        }
    });

    let result = tokio::time::timeout(std::time::Duration::from_secs(60), async {
        hammer_a.await.unwrap();
        hammer_b.await.unwrap();
        hammer_c.await.unwrap();
    })
    .await;

    stop.store(true, Ordering::Relaxed);

    assert!(
        result.is_ok(),
        "H3 DEADLOCK REGRESSION: delete+re-upsert racing search_quantized_bruteforce \
         hung past 60s timeout — the five-map `_async`/`_sync` mixing hazard \
         (same class as #589/`cells`, commit `7a4abf62`; H1+H2 commit `621776bd`) \
         has regressed. Check that every lock-acquiring op on `deleted`, \
         `vectors_u8`, `vectors`, `rid_to_internal`, and \
         `compaction_deleted_rids` is `_sync`."
    );
}

/// Ultra-constrained variant: `worker_threads = 1`. On a single-worker
/// runtime, ONE delete racing ONE search suffices to expose the pre-fix
/// hazard (the sole worker parks in `contains_sync` while the delete task
/// holds the handed-off bucket lock in the run queue, unpolled).
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn h3_deadlock_single_worker_delete_racing_search() {
    let adapter = build_fitted_bruteforce_adapter().await;
    let hot_rid = rid(1);
    let hot_vec = random_vec(8, 1);
    let query = random_vec(8, 42);
    let stop = Arc::new(AtomicBool::new(false));

    let muta = Arc::clone(&adapter);
    let stop_a = Arc::clone(&stop);
    let hammer_a = tokio::spawn(async move {
        for _ in 0..HAMMER_ITERS {
            let _ = muta.delete(hot_rid).await;
            let _ = muta.upsert(hot_rid, &hot_vec).await;
            tokio::task::yield_now().await;
            if stop_a.load(Ordering::Relaxed) {
                return;
            }
        }
    });

    let mutb = Arc::clone(&adapter);
    let stop_b = Arc::clone(&stop);
    let hammer_b = tokio::spawn(async move {
        for _ in 0..HAMMER_ITERS {
            let _ = mutb.search(&query, 10, SearchOpts::default(), None).await;
            tokio::task::yield_now().await;
            if stop_b.load(Ordering::Relaxed) {
                return;
            }
        }
    });

    let result = tokio::time::timeout(std::time::Duration::from_secs(60), async {
        hammer_a.await.unwrap();
        hammer_b.await.unwrap();
    })
    .await;

    stop.store(true, Ordering::Relaxed);

    assert!(
        result.is_ok(),
        "H3 DEADLOCK REGRESSION (single-worker): delete racing search hung \
         past 60s timeout on a worker_threads=1 runtime — the five-map \
         `_async`/`_sync` mixing hazard has regressed."
    );
}

/// `rid_to_internal` amplifier-chain regression: exercises the specific
/// cross-map chain where an `entry_sync` on `rid_to_internal` is held while
/// `deleted.insert_sync` runs inside the `Occupied` arm (the amplifier
/// described in the fix brief). With the pre-fix `entry_async`, a worker
/// could PARK while owning the `rid_to_internal` bucket across the
/// `.await`, chaining `deleted` into the deadlock. The fix makes both
/// `_sync`, closing the chain.
///
/// This test hammers concurrent `upsert` on the SAME rid (which takes the
/// `Occupied` → tombstone → reassign path) racing `search_prefilter`
/// (which `read_sync`'s `rid_to_internal` and `contains_sync`'s `deleted`
/// on every candidate).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn h3_deadlock_rid_to_internal_amplifier_chain() {
    let adapter = build_fitted_bruteforce_adapter().await;
    let hot_rid = rid(2);
    let hot_vec = random_vec(8, 2);
    let stop = Arc::new(AtomicBool::new(false));

    // Upsert hammer: each upsert on hot_rid takes the `Occupied` arm of
    // `entry_sync`, tombstoning the old internal via `deleted.insert_sync`
    // WHILE the entry guard is held — the exact amplifier site.
    let muta = Arc::clone(&adapter);
    let stop_a = Arc::clone(&stop);
    let hammer_a = tokio::spawn(async move {
        for i in 0..HAMMER_ITERS {
            // Alternate the vector value so each upsert replaces the
            // previous (taking the Occupied arm, not the Vacant arm).
            let mut v = hot_vec.clone();
            v[0] = (i as f32) * 0.001;
            let _ = muta.upsert(hot_rid, &v).await;
            tokio::task::yield_now().await;
            if stop_a.load(Ordering::Relaxed) {
                return;
            }
        }
    });

    // Prefilter hammer: `search_prefilter` `read_sync`'s
    // `rid_to_internal` and `contains_sync`'s `deleted` per candidate —
    // the exact sync readers on both amplifier-chain maps.
    let mutb = Arc::clone(&adapter);
    let stop_b = Arc::clone(&stop);
    let hammer_b = tokio::spawn(async move {
        let candidates: Vec<RecordId> = (0..50u64).map(rid).collect();
        for _ in 0..HAMMER_ITERS {
            let _ = mutb
                .search_prefilter(&random_vec(8, 99), 10, &candidates)
                .await;
            tokio::task::yield_now().await;
            if stop_b.load(Ordering::Relaxed) {
                return;
            }
        }
    });

    let result = tokio::time::timeout(std::time::Duration::from_secs(60), async {
        hammer_a.await.unwrap();
        hammer_b.await.unwrap();
    })
    .await;

    stop.store(true, Ordering::Relaxed);

    assert!(
        result.is_ok(),
        "H3 DEADLOCK REGRESSION (amplifier): upsert Occupied-arm tombstone \
         racing search_prefilter hung past 60s timeout — the \
         `rid_to_internal`-held-while-`deleted`-parks amplifier chain has \
         regressed."
    );
}

/// `rid_map` (the SIXTH vector-index map, `scc::HashMap<usize, RecordId,
/// THasher>`, reverse of `rid_to_internal`) — same structural class as the
/// five maps fixed in H3 (commit `dcfaf825`) and #589's `cells` map (commit
/// `7a4abf62`). The hazard: `iter_sync` in `for_each_rid_map` (snapshot
/// serialisation) mixed with `insert_async` (upsert/upsert_batch/
/// backfill_if_absent) and `read_async` (every search path —
/// `search_quantized_bruteforce`, `search_quantized_graph`,
/// `search_cofilter_quantized`, `search_prefilter`, the f32 bruteforce +
/// approximate branches) on the SAME runtime worker threads.
///
/// The interleaving that deadlocked before the fix:
/// 1. A worker running `for_each_rid_map` (snapshot serialisation) PARKS its
///    OS thread in `iter_sync` on a `rid_map` bucket saa just handed off to a
///    suspended `insert_async`/`read_async` task (concurrent upsert/search).
/// 2. That lock-owning task holds the exclusive bucket lock while sitting in
///    tokio's run queue, unpolled.
/// 3. With enough workers piling up the same way, the lock-owning task is
///    never polled again → whole-runtime deadlock.
///
/// **Fix**: every `insert_async`/`read_async` on `rid_map` is now
/// `insert_sync`/`read_sync` (12 sites); the fn stays `async` (call sites
/// unchanged) and the calls no longer suspend. Same mechanical reasoning as
/// the H3 five-map fix — none of the closures suspend.
///
/// This test races concurrent upsert (hits `rid_map.insert_sync`) + search
/// (hits `rid_map.read_sync`) against a tight-loop `for_each_rid_map` (the
/// `iter_sync` accessor) on a constrained runtime. As with the H3 tests
/// above, this is a RACE-WINDOW regression guard, not a deterministic
/// reproducer — nextest's parallelism has a real chance to catch a future
/// regression, and the NAMED bounded `tokio::time::timeout` ensures a real
/// regression fails fast and identifiably instead of hanging the whole suite.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rid_map_deadlock_iter_sync_racing_upsert_search() {
    let adapter = build_fitted_bruteforce_adapter().await;
    let hot_rid = rid(3);
    let hot_vec = random_vec(8, 3);
    let query = random_vec(8, 42);
    let stop = Arc::new(AtomicBool::new(false));

    // Upsert hammer: tight-loops upsert on hot_rid — each upsert publishes
    // the reverse id via `rid_map.insert_sync` (was `insert_async`).
    let muta = Arc::clone(&adapter);
    let stop_a = Arc::clone(&stop);
    let hot_rid_a = hot_rid;
    let hot_vec_a = hot_vec.clone();
    let hammer_a = tokio::spawn(async move {
        for i in 0..HAMMER_ITERS {
            // Alternate the vector so each upsert replaces the previous
            // (allocates a fresh internal → a fresh `rid_map` insert).
            let mut v = hot_vec_a.clone();
            v[0] = (i as f32) * 0.001;
            let _ = muta.upsert(hot_rid_a, &v).await;
            tokio::task::yield_now().await;
            if stop_a.load(Ordering::Relaxed) {
                return;
            }
        }
    });

    // Search hammer: on a fitted ≤512 adapter `search` dispatches to
    // `search_quantized_bruteforce`, which `read_sync`s `rid_map` for every
    // candidate (was `read_async`).
    let mutb = Arc::clone(&adapter);
    let stop_b = Arc::clone(&stop);
    let query_b = query.clone();
    let hammer_b = tokio::spawn(async move {
        for _ in 0..HAMMER_ITERS {
            let _ = mutb.search(&query_b, 10, SearchOpts::default(), None).await;
            tokio::task::yield_now().await;
            if stop_b.load(Ordering::Relaxed) {
                return;
            }
        }
    });

    // Snapshot-serialisation hammer: tight-loops `for_each_rid_map`, the
    // SYNCHRONOUS `iter_sync` accessor on `rid_map` — the exact sync reader
    // that, before the fix, parked worker threads against an `insert_async`/
    // `read_async` task holding a handed-off bucket lock.
    let mutc = Arc::clone(&adapter);
    let stop_c = Arc::clone(&stop);
    let hammer_c = tokio::spawn(async move {
        let mut sink: Vec<(usize, RecordId)> = Vec::new();
        for _ in 0..HAMMER_ITERS {
            sink.clear();
            mutc.for_each_rid_map(|internal, r| sink.push((internal, r)));
            tokio::task::yield_now().await;
            if stop_c.load(Ordering::Relaxed) {
                return;
            }
        }
    });

    let result = tokio::time::timeout(std::time::Duration::from_secs(60), async {
        hammer_a.await.unwrap();
        hammer_b.await.unwrap();
        hammer_c.await.unwrap();
    })
    .await;

    stop.store(true, Ordering::Relaxed);

    assert!(
        result.is_ok(),
        "rid_map DEADLOCK REGRESSION: upsert (rid_map insert) + search \
         (rid_map read) racing for_each_rid_map (rid_map iter_sync) hung past \
         60s timeout — the `rid_map` `_async`/`_sync` mixing hazard (same \
         class as #589/`cells`, commit `7a4abf62`; H3 commit `dcfaf825`) has \
         regressed. Check that every lock-acquiring op on `rid_map` is `_sync` \
         (only `for_each_rid_map`'s `iter_sync` may remain synchronous)."
    );
}

/// Ultra-constrained variant: `worker_threads = 1`. On a single-worker
/// runtime ONE upsert racing ONE `for_each_rid_map` scan suffices to expose
/// the pre-fix hazard (the sole worker parks in `iter_sync` while the upsert
/// task holds the handed-off `rid_map` bucket lock in the run queue,
/// unpolled).
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn rid_map_deadlock_single_worker_upsert_racing_iter_sync() {
    let adapter = build_fitted_bruteforce_adapter().await;
    let hot_rid = rid(4);
    let hot_vec = random_vec(8, 4);
    let stop = Arc::new(AtomicBool::new(false));

    let muta = Arc::clone(&adapter);
    let stop_a = Arc::clone(&stop);
    let hammer_a = tokio::spawn(async move {
        for i in 0..HAMMER_ITERS {
            let mut v = hot_vec.clone();
            v[0] = (i as f32) * 0.001;
            let _ = muta.upsert(hot_rid, &v).await;
            tokio::task::yield_now().await;
            if stop_a.load(Ordering::Relaxed) {
                return;
            }
        }
    });

    // Snapshot-serialisation hammer: `for_each_rid_map`'s `iter_sync`.
    let mutb = Arc::clone(&adapter);
    let stop_b = Arc::clone(&stop);
    let hammer_b = tokio::spawn(async move {
        let mut sink: Vec<(usize, RecordId)> = Vec::new();
        for _ in 0..HAMMER_ITERS {
            sink.clear();
            mutb.for_each_rid_map(|internal, r| sink.push((internal, r)));
            tokio::task::yield_now().await;
            if stop_b.load(Ordering::Relaxed) {
                return;
            }
        }
    });

    let result = tokio::time::timeout(std::time::Duration::from_secs(60), async {
        hammer_a.await.unwrap();
        hammer_b.await.unwrap();
    })
    .await;

    stop.store(true, Ordering::Relaxed);

    assert!(
        result.is_ok(),
        "rid_map DEADLOCK REGRESSION (single-worker): upsert (rid_map \
         insert) racing for_each_rid_map (rid_map iter_sync) hung past 60s \
         timeout on a worker_threads=1 runtime — the `rid_map` \
         `_async`/`_sync` mixing hazard has regressed."
    );
}

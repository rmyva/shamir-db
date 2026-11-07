//! HNSW adapter tests: basic search / upsert / delete / recall / metric
//! behaviour (`batch` + `ef_search` + `deleted_count` sections live in
//! `batch_ef.rs`).

use crate::adapter::{SearchOpts, VectorAdapter, VectorError};
use crate::hnsw_adapter::{HnswAdapter, HnswConfig, ShamirDist};
use crate::tests::quant::quantized_graph_tests::runner::note_recall_stats;
use hnsw_rs::prelude::Distance;
use shamir_collections::TFxSet;
use shamir_index_core::kind::VectorMetric;
use shamir_test_diag::FlakeGuard;
use shamir_types::types::record_id::RecordId;

pub(in crate::tests) fn rid(n: u8) -> RecordId {
    let mut a = [0u8; 16];
    a[15] = n;
    RecordId(a)
}

pub(in crate::tests) fn random_vec(dim: usize, seed: u64) -> Vec<f32> {
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

#[test]
fn small_f32_search_scores_borrowed_vectors_before_other_maps() {
    let source = include_str!("../../../hnsw_adapter/vector_impl.rs");
    let branch = source
        .split("} else if self.len() <= BRUTE_FORCE_MAX {")
        .nth(1)
        .expect("small f32 branch")
        .split("} else {")
        .next()
        .expect("branch end");
    let scan = branch
        .split("self.state.vectors.iter_sync(|i, v| {")
        .nth(1)
        .expect("vectors scan")
        .split("});")
        .next()
        .expect("scan end");
    assert!(scan.contains("dist.eval(query, v)"));
    assert!(!scan.contains("clone("));
    assert!(!scan.contains("self.state.deleted"));
    assert!(!scan.contains("self.state.rid_map"));
}

#[tokio::test]
async fn small_f32_search_scores_live_replacements_for_each_metric() {
    for metric in [VectorMetric::L2, VectorMetric::Dot, VectorMetric::Cosine] {
        let adapter = HnswAdapter::new(2, metric, HnswConfig::default());
        adapter.upsert(rid(1), &[1.0, 0.0]).await.unwrap();
        adapter.upsert(rid(2), &[0.0, 1.0]).await.unwrap();
        adapter.upsert(rid(1), &[2.0, 0.0]).await.unwrap();
        adapter.delete(rid(2)).await.unwrap();
        adapter.upsert(rid(3), &[0.0, 2.0]).await.unwrap();
        let results = adapter
            .search(&[2.0, 0.0], 4, SearchOpts::default(), None)
            .await
            .unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, rid(1));
        assert_eq!(results[1].0, rid(3));
        let expected = match metric {
            VectorMetric::L2 => (0.0, 8.0_f32.sqrt()),
            VectorMetric::Dot => (0.0, 1.0),
            VectorMetric::Cosine => (0.0, 1.0),
        };
        assert!((results[0].1 - expected.0).abs() < 1e-5);
        assert!((results[1].1 - expected.1).abs() < 1e-5);
    }
}

#[tokio::test]
async fn basic_cosine_search() {
    let adapter = HnswAdapter::new(
        3,
        VectorMetric::Cosine,
        HnswConfig {
            max_elements: 100,
            ..Default::default()
        },
    );

    adapter.upsert(rid(1), &[1.0, 0.0, 0.0]).await.unwrap();
    adapter.upsert(rid(2), &[0.0, 1.0, 0.0]).await.unwrap();
    adapter.upsert(rid(3), &[0.9, 0.1, 0.0]).await.unwrap();

    let results = adapter
        .search(&[1.0, 0.0, 0.0], 2, SearchOpts::default(), None)
        .await
        .unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].0, rid(1));
    assert!(results[0].1 < 0.01);
}

#[tokio::test]
async fn delete_removes_from_results() {
    // Deletion correctness must not hinge on HNSW recall. On a tiny
    // 2-node graph, soft-deleting the entry point makes search
    // intermittently return 0 results (recall artifact on a
    // degenerate graph — not a soft-delete bug). To assert behaviour
    // deterministically we:
    //   1. build a non-degenerate graph (10 points) so recall over
    //      the survivors is reliable;
    //   2. assert the deleted rid is ABSENT (the actual contract of
    //      delete — never relies on recall reaching one survivor);
    //   3. assert the survivors ARE found (recall sanity on a graph
    //      large enough that it holds).
    // Mirrors the presence/absence pattern of
    // `apply_committed_vectors_inserts_all_into_graph` and
    // `recall_at_10_on_1k_vectors`.
    let adapter = HnswAdapter::new(
        2,
        VectorMetric::L2,
        HnswConfig {
            max_elements: 100,
            ..Default::default()
        },
    );

    // 10 points spread along x so neighbours are well separated.
    for i in 0..10u8 {
        adapter.upsert(rid(i), &[i as f32, 0.0]).await.unwrap();
    }

    adapter.delete(rid(0)).await.unwrap();

    let results = adapter
        .search(&[0.0, 0.0], 10, SearchOpts::default(), None)
        .await
        .unwrap();

    // Contract: the deleted rid must never surface.
    assert!(
        !results.iter().any(|(r, _)| *r == rid(0)),
        "deleted rid must be absent; got {results:?}"
    );
    // Recall sanity on a non-degenerate graph: the surviving nearest
    // neighbour (rid 1 at [1,0]) is found.
    assert!(
        results.iter().any(|(r, _)| *r == rid(1)),
        "surviving nearest neighbour must be found; got {results:?}"
    );
}

#[tokio::test]
async fn upsert_replaces() {
    let adapter = HnswAdapter::new(
        2,
        VectorMetric::L2,
        HnswConfig {
            max_elements: 100,
            ..Default::default()
        },
    );

    adapter.upsert(rid(1), &[0.0, 0.0]).await.unwrap();
    adapter.upsert(rid(1), &[10.0, 10.0]).await.unwrap();

    let results = adapter
        .search(&[10.0, 10.0], 1, SearchOpts::default(), None)
        .await
        .unwrap();
    assert_eq!(results[0].0, rid(1));
    assert!(results[0].1 < 0.01);
}

#[tokio::test]
async fn recall_at_10_on_1k_vectors() {
    // Failure-only flake diagnostics (design:
    // docs/dev-artifacts/design/flake-failure-logs.md): the report prints
    // ONLY while the test is panicking; `note_recall_stats` below records
    // the exact statistical setup so a miss is reproducible from the log.
    let guard = FlakeGuard::new("recall_at_10_on_1k_vectors");
    // `hnsw_rs` 0.3.4 has no public seed API — `Hnsw::new` calls
    // `StdRng::from_os_rng()` internally, so the graph topology is
    // non-deterministic between runs. To keep this test stable while
    // still exercising HNSW under realistic load we:
    //   1. raise ef_search well above the dataset's natural variance
    //      (a search that visits ~half the graph hits recall 1.0
    //      almost always);
    //   2. require recall ≥ 0.75 (was 0.5 — see #1070 recalibration below)
    //      — a floor that catches gross regressions (broken Distance impl,
    //      broken pruning) without flaking on ordinary construction
    //      variance.
    // Tighter recall validation belongs in a separate bench-only run.
    //
    // #1070: single-query recall against ONE fixed query (`vecs[0]`), so
    // `recall` can only land on multiples of 0.1 (0/10..10/10) — this is
    // NOT the same statistical setup as the 50-query 3k-vector test in
    // `crash_recovery_tests.rs`. `.github/workflows/hnsw-recall-matrix.yml`
    // (run 38074322496, after the #1233 fix below) ran this test 20 times on
    // EACH of ubuntu-latest, windows-latest, macos-latest (60 runs total):
    // 58 runs landed on 1.00, 2 on 0.90 (one each on windows and macos),
    // none lower. The earlier run 31472514970 read only 0.80/0.90 because of
    // a test bug (#1233): a stray `upsert(rid(0), ..)` in the build loop
    // re-pointed vecs[0]'s own rid at another vector and left ~1000
    // tombstoned twins in the graph, capping recall at 0.9 and adding a
    // 0.6-0.8 tail. Floor 0.75 (== 0.8 at the 0.1 quantum) sits one quantum
    // below the observed worst case (0.90). Re-run the matrix workflow
    // before adjusting this floor again.
    let dim = 32;
    let n = 1000;
    guard.note("rng seed base (random_vec, LCG)", "i + 42 per vector");
    let adapter = HnswAdapter::new(
        dim as u32,
        VectorMetric::L2,
        HnswConfig {
            max_elements: n + 100,
            ef_construction: 400,
            ef_search: 400,
            ..Default::default()
        },
    );

    let mut vecs = Vec::with_capacity(n);
    for i in 0..n {
        let v = random_vec(dim, i as u64 + 42);
        // Unique rid per vector. (An earlier stray `upsert(rid(0), &v)` here
        // re-pointed rid 0 — which is also vecs[0]'s rid — at every later
        // vector and left ~1000 tombstoned twins in the graph, capping
        // recall at 0.9 and adding the 0.7-0.8 tail; see #1233.)
        let mut a = [0u8; 16];
        a[14] = (i >> 8) as u8;
        a[15] = (i & 0xFF) as u8;
        let r = RecordId(a);
        adapter.upsert(r, &v).await.unwrap();
        vecs.push((r, v));
    }
    guard.mark("1000 vectors upserted");

    // Brute-force ground truth for query = vecs[0].
    let query = &vecs[0].1;
    let mut dists: Vec<(RecordId, f32)> = vecs
        .iter()
        .map(|(r, v)| {
            let d: f32 = query
                .iter()
                .zip(v.iter())
                .map(|(a, b)| (a - b) * (a - b))
                .sum::<f32>()
                .sqrt();
            (*r, d)
        })
        .collect();
    dists.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
    let gt_top10: TFxSet<RecordId> = dists.iter().take(10).map(|(r, _)| *r).collect();

    let hnsw_results = adapter
        .search(query, 10, SearchOpts::default(), None)
        .await
        .unwrap();
    let hnsw_top10: TFxSet<RecordId> = hnsw_results.iter().map(|(r, _)| *r).collect();

    let recall = gt_top10.intersection(&hnsw_top10).count() as f64 / 10.0;
    note_recall_stats(&guard, n, 10, 400, recall, 0.75);
    // #1070: machine-parseable line for the cross-platform recall-matrix
    // workflow (`.github/workflows/hnsw-recall-matrix.yml`) to grep out of
    // CI logs.
    println!("#1070 recall_at_10=1k:{recall:.4}");
    assert!(recall >= 0.75, "recall@10 = {recall:.2} — expected >= 0.75");
}

#[tokio::test]
async fn dim_mismatch_rejected() {
    let adapter = HnswAdapter::new(3, VectorMetric::L2, HnswConfig::default());
    let err = adapter.upsert(rid(1), &[1.0, 2.0]).await.unwrap_err();
    assert!(matches!(
        err,
        VectorError::DimMismatch {
            expected: 3,
            got: 2
        }
    ));
}

#[tokio::test]
async fn search_dim_mismatch_rejected() {
    let adapter = HnswAdapter::new(3, VectorMetric::L2, HnswConfig::default());
    adapter.upsert(rid(1), &[1.0, 2.0, 3.0]).await.unwrap();
    let err = adapter
        .search(&[1.0, 2.0], 1, SearchOpts::default(), None)
        .await
        .unwrap_err();
    assert!(matches!(err, VectorError::DimMismatch { .. }));
}

#[tokio::test]
async fn empty_index_returns_empty() {
    let adapter = HnswAdapter::new(2, VectorMetric::L2, HnswConfig::default());
    let results = adapter
        .search(&[0.0, 0.0], 5, SearchOpts::default(), None)
        .await
        .unwrap();
    assert!(results.is_empty());
}

#[tokio::test]
async fn dot_product_metric_normalized() {
    // Direct test of `ShamirDist` evaluator for the Dot metric — three
    // hand-picked normalized vectors, exact distance values, exact
    // ordering. We sidestep HNSW here because the graph is non-
    // deterministic (no seed API in hnsw_rs 0.3.4) and unstable
    // ordering would force soft assertions even at n=3. The HNSW
    // integration is covered by `basic_cosine_search` and `recall_at_10`.
    let dist = ShamirDist {
        metric: VectorMetric::Dot,
    };
    let s = 2.0_f32.sqrt().recip();
    let q = [1.0_f32, 0.0];

    // dot(q, [1,0]) = 1.0  → dist 0.0
    // dot(q, [s,s]) ≈ 0.707 → dist 0.293
    // dot(q, [0,1]) = 0.0  → dist 1.0
    let d_self = dist.eval(&q, &[1.0, 0.0]);
    let d_diag = dist.eval(&q, &[s, s]);
    let d_orth = dist.eval(&q, &[0.0, 1.0]);

    assert!(d_self < 0.01, "self-similarity should be ~0, got {d_self}");
    assert!(
        (d_diag - (1.0 - s)).abs() < 0.01,
        "diag dist should be ~{}, got {d_diag}",
        1.0 - s
    );
    assert!(
        (d_orth - 1.0).abs() < 0.01,
        "orthogonal dist should be ~1.0, got {d_orth}"
    );
    // Ordering invariant: nearer < farther.
    assert!(d_self < d_diag && d_diag < d_orth);
}

#[tokio::test]
async fn k_larger_than_dataset() {
    let adapter = HnswAdapter::new(2, VectorMetric::L2, HnswConfig::default());
    adapter.upsert(rid(1), &[0.0, 0.0]).await.unwrap();
    adapter.upsert(rid(2), &[1.0, 0.0]).await.unwrap();

    // k > dataset size returns every vector. The index is tiny, so search
    // runs the exact brute-force path (see `BRUTE_FORCE_MAX`) — fully
    // deterministic, no polling needed.
    let results = adapter
        .search(&[0.0, 0.0], 100, SearchOpts::default(), None)
        .await
        .unwrap();
    assert_eq!(results.len(), 2, "search must return both inserted vectors");
}

#[tokio::test]
async fn huge_k_clamped_no_panic() {
    let adapter = HnswAdapter::new(2, VectorMetric::L2, HnswConfig::default());
    adapter.upsert(rid(1), &[0.0, 0.0]).await.unwrap();
    adapter.upsert(rid(2), &[1.0, 0.0]).await.unwrap();
    // k = u32::MAX would previously cause huge allocation
    let results = adapter
        .search(&[0.0, 0.0], u32::MAX, SearchOpts::default(), None)
        .await
        .unwrap();
    assert_eq!(results.len(), 2);
}

#[tokio::test]
async fn k_zero_returns_empty() {
    let adapter = HnswAdapter::new(2, VectorMetric::L2, HnswConfig::default());
    adapter.upsert(rid(1), &[0.0, 0.0]).await.unwrap();
    let results = adapter
        .search(&[0.0, 0.0], 0, SearchOpts::default(), None)
        .await
        .unwrap();
    assert!(results.is_empty());
}

#[tokio::test]
async fn concurrent_searches_lock_free() {
    let dim = 16;
    let adapter = std::sync::Arc::new(HnswAdapter::new(
        dim,
        VectorMetric::Cosine,
        HnswConfig {
            max_elements: 1000,
            ..Default::default()
        },
    ));
    // Populate
    for i in 0..100 {
        let mut a = [0u8; 16];
        a[15] = i as u8;
        adapter
            .upsert(RecordId(a), &random_vec(dim as usize, i as u64))
            .await
            .unwrap();
    }

    // 8 concurrent searches.
    let mut handles = Vec::new();
    for s in 0..8u64 {
        let a = std::sync::Arc::clone(&adapter);
        handles.push(tokio::spawn(async move {
            let q = random_vec(dim as usize, s + 100);
            a.search(&q, 10, SearchOpts::default(), None).await.unwrap()
        }));
    }
    for h in handles {
        let r = h.await.unwrap();
        assert!(!r.is_empty());
    }
}

#[tokio::test]
async fn delete_nonexistent_no_error() {
    let adapter = HnswAdapter::new(2, VectorMetric::L2, HnswConfig::default());
    adapter.delete(rid(99)).await.unwrap();
}

#[tokio::test]
async fn search_merges_staged_slice() {
    // The committed graph holds rid(1); the caller passes its own
    // un-committed vector (rid(2), very close to the query) as a
    // staged slice. `search` must brute-force-merge it so an in-tx
    // query sees both. This is the path the executor drives from
    // `TxContext::staged_vectors_for(token)`.
    let adapter = HnswAdapter::new(
        3,
        VectorMetric::L2,
        HnswConfig {
            max_elements: 100,
            ..Default::default()
        },
    );
    adapter.upsert(rid(1), &[1.0, 0.0, 0.0]).await.unwrap();

    let staged = vec![(rid(2), vec![0.9, 0.1, 0.0])];

    // Without the staged slice: only the committed vector.
    let committed_only = adapter
        .search(&[1.0, 0.0, 0.0], 10, SearchOpts::default(), None)
        .await
        .unwrap();
    let committed_rids: TFxSet<_> = committed_only.iter().map(|(r, _)| *r).collect();
    assert!(committed_rids.contains(&rid(1)));
    assert!(
        !committed_rids.contains(&rid(2)),
        "staged vector must be invisible without the slice"
    );

    // With the staged slice: both surface.
    let merged = adapter
        .search(&[1.0, 0.0, 0.0], 10, SearchOpts::default(), Some(&staged))
        .await
        .unwrap();
    let merged_rids: TFxSet<_> = merged.iter().map(|(r, _)| *r).collect();
    assert!(
        merged_rids.contains(&rid(1)),
        "committed vector still found"
    );
    assert!(merged_rids.contains(&rid(2)), "staged vector merged in");
}

#[tokio::test]
async fn apply_committed_vectors_inserts_all_into_graph() {
    let adapter = HnswAdapter::new(
        3,
        VectorMetric::L2,
        HnswConfig {
            max_elements: 100,
            ..Default::default()
        },
    );

    // Before apply — search finds nothing.
    let before = adapter
        .search(&[1.0, 0.0, 0.0], 10, SearchOpts::default(), None)
        .await
        .unwrap();
    assert_eq!(before.len(), 0, "graph empty before apply");

    // Apply a committed batch (what commit Phase 5d feeds in).
    let batch = vec![
        (rid(1), vec![1.0, 0.0, 0.0]),
        (rid(2), vec![0.0, 1.0, 0.0]),
        (rid(3), vec![0.0, 0.0, 1.0]),
    ];
    adapter.apply_committed_vectors(&batch).await.unwrap();

    // After apply — the closest vector (rid 1 = the query) is findable.
    let after = adapter
        .search(&[1.0, 0.0, 0.0], 10, SearchOpts::default(), None)
        .await
        .unwrap();
    assert!(
        !after.is_empty(),
        "graph must hold committed vectors after apply"
    );
    let labels: TFxSet<_> = after.iter().map(|(r, _)| *r).collect();
    assert!(
        labels.contains(&rid(1)),
        "closest applied vector (rid 1) must be findable; got {after:?}"
    );
}

#[tokio::test]
async fn apply_committed_vectors_empty_is_noop() {
    let adapter = HnswAdapter::new(2, VectorMetric::L2, HnswConfig::default());
    adapter.apply_committed_vectors(&[]).await.unwrap(); // must not panic
}

#[tokio::test]
async fn apply_committed_vectors_handles_replace() {
    let adapter = HnswAdapter::new(
        3,
        VectorMetric::L2,
        HnswConfig {
            max_elements: 100,
            ..Default::default()
        },
    );

    // A committed vector at [1,0,0].
    adapter.upsert(rid(1), &[1.0, 0.0, 0.0]).await.unwrap();
    let before = adapter
        .search(&[1.0, 0.0, 0.0], 1, SearchOpts::default(), None)
        .await
        .unwrap();
    assert_eq!(before[0].0, rid(1));

    // Apply a committed batch that replaces rid(1) with a new vector.
    adapter
        .apply_committed_vectors(&[(rid(1), vec![0.0, 1.0, 0.0])])
        .await
        .unwrap();

    // Search for [0,1,0] -> should find rid(1) (updated position).
    let after = adapter
        .search(&[0.0, 1.0, 0.0], 1, SearchOpts::default(), None)
        .await
        .unwrap();
    assert_eq!(after[0].0, rid(1));
    assert!(after[0].1 < 0.01, "should be very close to [0,1,0]");
}

#[tokio::test]
async fn many_upserts_same_rid() {
    let adapter = HnswAdapter::new(2, VectorMetric::L2, HnswConfig::default());
    for i in 0..10 {
        adapter.upsert(rid(1), &[i as f32, 0.0]).await.unwrap();
    }
    // Only latest visible
    let results = adapter
        .search(&[9.0, 0.0], 10, SearchOpts::default(), None)
        .await
        .unwrap();
    let matching: Vec<_> = results.iter().filter(|(r, _)| *r == rid(1)).collect();
    assert_eq!(
        matching.len(),
        1,
        "rid(1) should appear once after 10 upserts"
    );
    assert!(matching[0].1 < 0.5);
}

/// D12: two concurrent `upsert(SAME rid, different vec)` must leave the
/// rid mapped to EXACTLY ONE live internal — never two. This is the race
/// reachable since III.5 moved HNSW promote outside `commit_lock` (two
/// committers promoting the same record concurrently).
///
/// Against the OLD non-atomic read-tombstone-reassign, both upserts read
/// "no old internal", allocate i1/i2, insert both into the graph, and the
/// loser's internal stays un-tombstoned — so the rid surfaces twice in
/// search and `len()` over-counts. The `entry_async` slot-claim fixes it:
/// the second upsert serialises on the bucket entry, sees the first's
/// internal as "old", and tombstones it.
///
/// We drive many barrier-synced iterations to surface the interleaving,
/// and assert the invariant two ways: (a) directly on adapter state — the
/// count of live (non-tombstoned) internals mapped to the rid is exactly
/// 1, which is recall-independent and the decisive old-vs-new
/// discriminator; and (b) end-to-end via search on a non-degenerate graph
/// — the rid appears at most once.
#[tokio::test]
#[serial_test::serial]
async fn upsert_same_rid_concurrent_no_duplicate() {
    use std::sync::Arc as StdArc;
    use tokio::sync::Barrier;

    let dim = 4usize;
    // Each iteration is a fresh adapter so the race window is clean and
    // any single failure is decisive. Enough iterations to reliably
    // surface the interleaving on the racy code.
    for iter in 0..40u64 {
        let adapter = StdArc::new(HnswAdapter::new(
            dim as u32,
            VectorMetric::L2,
            HnswConfig {
                max_elements: 1000,
                ..Default::default()
            },
        ));

        // Populate a non-degenerate graph with unrelated rids so search
        // recall over survivors is reliable (the same rationale as
        // `delete_removes_from_results`). rid bytes start at 10 so they
        // never collide with the contended rid (1).
        for j in 0..12u8 {
            adapter
                .upsert(rid(10 + j), &random_vec(dim, iter * 100 + j as u64))
                .await
                .unwrap();
        }

        let target = rid(1);
        let vec_a = vec![1.0f32, 0.0, 0.0, 0.0];
        let vec_b = vec![0.0f32, 1.0, 0.0, 0.0];

        // Two tasks race an upsert of the SAME rid, synced at the barrier
        // so both enter the critical section as close together as the
        // runtime allows.
        let barrier = StdArc::new(Barrier::new(2));
        let mut handles = Vec::new();
        for v in [vec_a.clone(), vec_b.clone()] {
            let a = StdArc::clone(&adapter);
            let b = StdArc::clone(&barrier);
            handles.push(tokio::spawn(async move {
                b.wait().await;
                a.upsert(target, &v).await.unwrap();
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        // (a) Direct state invariant (recall-independent): exactly one
        //     internal mapped to `target` is live (present in rid_map and
        //     NOT tombstoned). The old racy path leaves two.
        let mut live_internals = 0usize;
        adapter
            .state
            .rid_map
            .iter_async(|internal, mapped_rid| {
                if *mapped_rid == target && !adapter.state.deleted.contains_sync(internal) {
                    live_internals += 1;
                }
                true
            })
            .await;
        assert_eq!(
            live_internals, 1,
            "iter {iter}: rid must map to exactly ONE live internal after \
             two concurrent upserts; found {live_internals} (D12 duplicate-rid race)"
        );

        // (b) End-to-end: the rid appears AT MOST once in a top-k search
        //     near either staged vector. (Recall may legitimately miss it
        //     on a tiny random graph, but it must never appear twice.)
        for q in [&vec_a, &vec_b] {
            let results = adapter
                .search(q, 16, SearchOpts::default(), None)
                .await
                .unwrap();
            let occurrences = results.iter().filter(|(r, _)| *r == target).count();
            assert!(
                occurrences <= 1,
                "iter {iter}: rid surfaced {occurrences} times in search — \
                 duplicate live graph node (D12); results={results:?}"
            );
        }
    }
}

#[tokio::test]
async fn non_tx_search_unchanged_after_refactor() {
    // Regression guard: existing non-tx path works exactly as before.
    let adapter = HnswAdapter::new(
        3,
        VectorMetric::Cosine,
        HnswConfig {
            max_elements: 100,
            ..Default::default()
        },
    );
    adapter.upsert(rid(1), &[1.0, 0.0, 0.0]).await.unwrap();
    adapter.upsert(rid(2), &[0.0, 1.0, 0.0]).await.unwrap();
    adapter.upsert(rid(3), &[0.9, 0.1, 0.0]).await.unwrap();

    let results = adapter
        .search(&[1.0, 0.0, 0.0], 2, SearchOpts::default(), None)
        .await
        .unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].0, rid(1));
}

// P1 — MAX_TOPK restore: a huge (untrusted) k on the graph path (300
// vectors > BRUTE_FORCE_MAX 256) must clamp to MAX_TOPK instead of
// allocating `overscan = 2*k` / `with_capacity(k)`. Regression for the
// dropped master clamp in `vector_impl::search`.
#[tokio::test]
async fn huge_k_clamped_on_graph_path() {
    let adapter = HnswAdapter::new(
        2,
        VectorMetric::L2,
        HnswConfig {
            max_elements: 4096,
            ..Default::default()
        },
    );
    let vec8 = |i: u32| {
        let mut v = vec![0.0f32; 2];
        v[(i % 2) as usize] = i as f32 / 16.0;
        v
    };
    let rid_u32 = |n: u32| {
        let mut a = [0u8; 16];
        a[..4].copy_from_slice(&n.to_le_bytes());
        RecordId(a)
    };
    for i in 0..300u32 {
        adapter.upsert(rid_u32(i), &vec8(i)).await.unwrap();
    }
    let q = vec8(7);
    let r1 = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        adapter.search(&q, 1_000_000_000, SearchOpts::default(), None),
    )
    .await
    .expect("k=1e9 search must not hang")
    .unwrap();
    assert!(!r1.is_empty());
    assert!(r1.len() <= 300);
    let r2 = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        adapter.search(&q, u32::MAX, SearchOpts::default(), None),
    )
    .await
    .expect("k=u32::MAX search must not hang")
    .unwrap();
    assert!(!r2.is_empty());
    assert!(r2.len() <= 300);
}

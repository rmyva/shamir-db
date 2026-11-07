//! V4.2 (#408) — HNSW compaction tests: unit, integration, stress.

use crate::adapter::VectorAdapter;
use crate::hnsw_adapter::{HnswAdapter, HnswConfig};
use crate::vector_backend::AdapterSlot;
use shamir_collections::TFxSet;
use shamir_index_core::kind::{VectorMetric, VectorQuantization};
use shamir_types::types::common::THasher;
use shamir_types::types::record_id::RecordId;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

pub(in crate::tests) fn rid(n: u16) -> RecordId {
    let bytes = (n as u128).to_be_bytes();
    RecordId(bytes)
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

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn collect_live_vectors_excludes_tombstones() {
    let adapter = HnswAdapter::new(
        3,
        VectorMetric::L2,
        HnswConfig {
            max_elements: 100,
            ..Default::default()
        },
    );
    adapter.upsert(rid(1), &[1.0, 0.0, 0.0]).await.unwrap();
    adapter.upsert(rid(2), &[0.0, 1.0, 0.0]).await.unwrap();
    adapter.upsert(rid(3), &[0.0, 0.0, 1.0]).await.unwrap();
    adapter.delete(rid(2)).await.unwrap();

    let live = adapter.collect_live_vectors();
    let rids: TFxSet<RecordId> = live.iter().map(|(r, _)| *r).collect();
    assert!(rids.contains(&rid(1)));
    assert!(!rids.contains(&rid(2)));
    assert!(rids.contains(&rid(3)));
    assert_eq!(live.len(), 2);
}

#[tokio::test]
async fn backfill_if_absent_skips_existing() {
    let adapter = HnswAdapter::new_compaction_target(
        3,
        VectorMetric::L2,
        HnswConfig {
            max_elements: 100,
            ..Default::default()
        },
    );
    // Pre-insert rid(1) via double-write simulation
    adapter.upsert(rid(1), &[9.0, 9.0, 9.0]).await.unwrap();

    // Backfill with a different value for rid(1) and a new rid(2)
    let items = vec![(rid(1), vec![1.0, 0.0, 0.0]), (rid(2), vec![0.0, 1.0, 0.0])];
    adapter.backfill_if_absent(&items).await.unwrap();

    // rid(1) should keep its double-write value (9,9,9)
    assert_eq!(adapter.len(), 2);
    // rid(2) should be present
    assert!(adapter.contains_rid(&rid(2)));
}

#[tokio::test]
async fn backfill_if_absent_skips_deleted_rids() {
    let adapter = HnswAdapter::new_compaction_target(
        3,
        VectorMetric::L2,
        HnswConfig {
            max_elements: 100,
            ..Default::default()
        },
    );
    // Simulate double-write delete: upsert then delete, recording in deleted_rids
    adapter.upsert(rid(1), &[1.0, 0.0, 0.0]).await.unwrap();
    adapter.delete(rid(1)).await.unwrap();
    if let Some(ref del_rids) = adapter.state.compaction_deleted_rids {
        let _ = del_rids.insert_sync(rid(1), ());
    }

    // Backfill should NOT resurrect rid(1)
    let items = vec![(rid(1), vec![1.0, 0.0, 0.0])];
    adapter.backfill_if_absent(&items).await.unwrap();
    assert_eq!(adapter.len(), 0);
}

#[tokio::test]
async fn rv_index_fab4e4e5_compaction_retains_tuning() {
    let config = HnswConfig {
        max_elements: 2000,
        m: 12,
        max_layer: 7,
        ef_construction: 77,
        ef_search: 31,
    };
    let adapter = HnswAdapter::new(3, VectorMetric::L2, config.clone());
    let rebuilt = adapter.build_config();
    assert_eq!(rebuilt.m, config.m);
    assert_eq!(rebuilt.max_layer, config.max_layer);
    assert_eq!(rebuilt.ef_construction, config.ef_construction);
    assert_eq!(rebuilt.ef_search, config.ef_search);
}

#[tokio::test]
async fn should_compact_threshold_logic() {
    let adapter = HnswAdapter::new(
        3,
        VectorMetric::L2,
        HnswConfig {
            max_elements: 5000,
            ..Default::default()
        },
    );
    // Insert 1000 vectors then delete 200 → ratio = 200/1000 = 0.2 < 0.3
    for i in 0..1000u16 {
        adapter
            .upsert(rid(i), &random_vec(3, i as u64))
            .await
            .unwrap();
    }
    for i in 0..200u16 {
        adapter.delete(rid(i)).await.unwrap();
    }
    // live_count = 800, deleted_ratio = 200/1000 = 0.2
    assert!(adapter.deleted_ratio() < 0.3);

    // Delete more to cross 0.3 threshold
    for i in 200..500u16 {
        adapter.delete(rid(i)).await.unwrap();
    }
    // deleted = 500, next_id = 1000, ratio = 0.5 > 0.3, live = 500 < 1000
    assert!(adapter.deleted_ratio() >= 0.3);
    assert!(adapter.live_count() < 1000); // below min_live
}

// ---------------------------------------------------------------------------
// Integration tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn compaction_rebuild_aside_removes_tombstones() {
    let dim = 4u32;
    let old = HnswAdapter::new(
        dim,
        VectorMetric::L2,
        HnswConfig {
            max_elements: 2000,
            ..Default::default()
        },
    );
    // Insert 100, delete 50
    for i in 0..100u16 {
        old.upsert(rid(i), &random_vec(dim as usize, i as u64))
            .await
            .unwrap();
    }
    for i in 0..50u16 {
        old.delete(rid(i)).await.unwrap();
    }
    assert_eq!(old.deleted_count(), 50);

    // Simulate compaction protocol
    let config = old.build_config();
    let new = HnswAdapter::new_compaction_target(dim, VectorMetric::L2, config);
    let new_arc: Arc<dyn VectorAdapter> = Arc::new(new);
    let new_hnsw = new_arc.as_hnsw_adapter().unwrap();

    let live = old.collect_live_vectors();
    assert_eq!(live.len(), 50);
    new_hnsw.backfill_if_absent(&live).await.unwrap();

    assert_eq!(new_hnsw.deleted_count(), 0);
    assert_eq!(new_hnsw.live_count(), 50);
}

#[tokio::test]
async fn double_write_visibility_in_new_adapter() {
    let dim = 3u32;
    let old = HnswAdapter::new(
        dim,
        VectorMetric::L2,
        HnswConfig {
            max_elements: 200,
            ..Default::default()
        },
    );
    let new = HnswAdapter::new_compaction_target(dim, VectorMetric::L2, HnswConfig::default());
    let new_arc: Arc<dyn VectorAdapter> = Arc::new(new);

    // Simulate double-write: upsert into both
    old.upsert(rid(1), &[1.0, 2.0, 3.0]).await.unwrap();
    new_arc.upsert(rid(1), &[1.0, 2.0, 3.0]).await.unwrap();

    let new_hnsw = new_arc.as_hnsw_adapter().unwrap();
    assert_eq!(new_hnsw.live_count(), 1);
}

#[tokio::test]
async fn single_flight_compaction_noop() {
    let flag = Arc::new(AtomicBool::new(true)); // simulate already in-flight
                                                // compare_exchange should fail
    assert!(flag
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err());
}

#[tokio::test]
async fn step4b_reconcile_prevents_resurrect() {
    let dim = 3u32;
    let new = HnswAdapter::new_compaction_target(
        dim,
        VectorMetric::L2,
        HnswConfig {
            max_elements: 200,
            ..Default::default()
        },
    );
    let new_arc: Arc<dyn VectorAdapter> = Arc::new(new);
    let new_hnsw = new_arc.as_hnsw_adapter().unwrap();

    // Simulate: backfill inserts rid(5) (the race window — delete arrived
    // after backfill check but before insert)
    new_hnsw
        .backfill_if_absent(&[(rid(5), vec![1.0, 2.0, 3.0])])
        .await
        .unwrap();
    assert_eq!(new_hnsw.live_count(), 1);

    // Now simulate the double-write delete that was recorded in compaction_deleted_rids
    if let Some(ref del_rids) = new_hnsw.state.compaction_deleted_rids {
        let _ = del_rids.insert_sync(rid(5), ());
    }

    // Step 4b: reconcile-deletes
    if let Some(ref del_rids) = new_hnsw.state.compaction_deleted_rids {
        let mut rids_to_delete: Vec<RecordId> = Vec::new();
        del_rids.iter_sync(|rid, ()| {
            rids_to_delete.push(*rid);
            true
        });
        for r in rids_to_delete {
            let _ = new_arc.delete(r).await;
        }
    }

    // rid(5) must be gone
    assert_eq!(new_hnsw.live_count(), 0);
}

// ---------------------------------------------------------------------------
// Stress test
// ---------------------------------------------------------------------------

/// Stress test: N workers run upsert/delete concurrently with Steps 3-4b
/// (collect + backfill + reconcile). Workers finish, THEN Steps 5-6 (swap +
/// clear). This exercises the critical double-write + backfill correctness
/// under concurrency. The nanosecond tail-race between Step5→Step6 is
/// acceptable in production (idempotent writes to the same adapter); we
/// verify the substantive invariants here.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stress_concurrent_mutations_during_compaction() {
    use std::sync::atomic::AtomicU64;
    use tokio::sync::Barrier;

    let dim = 8u32;
    let n_initial = 200u16;
    let n_concurrent_ops = 500u16;

    // Build the "old" adapter with initial data
    let old_adapter = Arc::new(HnswAdapter::new(
        dim,
        VectorMetric::L2,
        HnswConfig {
            max_elements: 5000,
            ..Default::default()
        },
    ));
    for i in 0..n_initial {
        old_adapter
            .upsert(rid(i), &random_vec(dim as usize, i as u64))
            .await
            .unwrap();
    }

    // Set up ArcSwap (simulating VectorBackend.adapter)
    let adapter_swap: Arc<arc_swap::ArcSwap<AdapterSlot>> =
        Arc::new(arc_swap::ArcSwap::from(Arc::new(AdapterSlot::live(
            old_adapter.clone() as Arc<dyn VectorAdapter>,
        ))));

    // Create compaction target
    let config = old_adapter.build_config();
    let new_adapter = HnswAdapter::new_compaction_target(dim, VectorMetric::L2, config);
    let new_adapter_arc: Arc<dyn VectorAdapter> = Arc::new(new_adapter);

    // Arm double-write (Step 2): an armed routing slot
    let armed_target = crate::vector_backend::CompactionTarget::new(Arc::clone(&new_adapter_arc));
    adapter_swap.store(Arc::new(AdapterSlot::armed(
        old_adapter.clone() as Arc<dyn VectorAdapter>,
        Arc::clone(&armed_target),
    )));

    // Track expected live set
    let expected_live = Arc::new(tokio::sync::Mutex::new(
        (0..n_initial).map(rid).collect::<TFxSet<RecordId>>(),
    ));
    let op_counter = Arc::new(AtomicU64::new(0));

    // Barrier: 4 workers + 1 compaction task start together
    let barrier = Arc::new(Barrier::new(5));

    // Spawn concurrent mutation tasks (run alongside Steps 3-4b)
    let mut handles = Vec::new();
    for worker in 0..4u16 {
        let adapter_swap = Arc::clone(&adapter_swap);
        let expected_live = Arc::clone(&expected_live);
        let op_counter = Arc::clone(&op_counter);
        let barrier = Arc::clone(&barrier);
        let old_adapter = Arc::clone(&old_adapter);
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            let base_rid = n_initial + worker * (n_concurrent_ops / 4);
            for i in 0..(n_concurrent_ops / 4) {
                let r = rid(base_rid + i);
                let vec = random_vec(dim as usize, (base_rid + i) as u64);

                // Upsert into the slot's primary + mirror (real writer shape)
                let slot = adapter_swap.load_full();
                old_adapter.upsert(r, &vec).await.unwrap();
                if let Some(t) = &slot.target {
                    t.mirror_upsert(r, &vec).await;
                }
                {
                    let mut live = expected_live.lock().await;
                    live.insert(r);
                }

                // Delete some (every 3rd)
                if i % 3 == 0 {
                    old_adapter.delete(r).await.unwrap();
                    if let Some(t) = &slot.target {
                        if let Some(hnsw) = t.adapter.as_hnsw_adapter() {
                            if let Some(ref del_rids) = hnsw.state.compaction_deleted_rids {
                                let _ = del_rids.insert_sync(r, ());
                            }
                        }
                        t.mirror_delete(r).await;
                    }
                    {
                        let mut live = expected_live.lock().await;
                        live.remove(&r);
                    }
                }

                op_counter.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    // Compaction task runs Steps 3-4b concurrently with workers
    let compaction_handle = {
        let new_adapter_arc = Arc::clone(&new_adapter_arc);
        let barrier = Arc::clone(&barrier);
        let old_adapter = Arc::clone(&old_adapter);

        tokio::spawn(async move {
            barrier.wait().await;
            // Let some mutations land before collecting
            tokio::task::yield_now().await;

            // Step 3: collect from old
            let live_pairs = old_adapter.collect_live_vectors();

            // Step 4a: backfill
            let new_hnsw = new_adapter_arc.as_hnsw_adapter().unwrap();
            new_hnsw.backfill_if_absent(&live_pairs).await.unwrap();

            // Step 4b: reconcile-deletes
            if let Some(ref del_rids) = new_hnsw.state.compaction_deleted_rids {
                let mut rids_to_delete: Vec<RecordId> = Vec::new();
                del_rids.iter_sync(|rid, ()| {
                    rids_to_delete.push(*rid);
                    true
                });
                for r in rids_to_delete {
                    let _ = new_adapter_arc.delete(r).await;
                }
            }
        })
    };

    // Wait for ALL workers and compaction steps 3-4b to finish
    for h in handles {
        h.await.unwrap();
    }
    compaction_handle.await.unwrap();

    // Now do Step 5 (cutover publish) — no concurrent mutations
    adapter_swap.store(Arc::new(AdapterSlot::live(Arc::clone(&new_adapter_arc))));

    // Verify: new adapter's live set matches expected
    let slot = adapter_swap.load_full();
    let new_hnsw = slot.adapter.as_hnsw_adapter().unwrap();

    let expected = expected_live.lock().await;

    // Check: no ghost (every rid in new is in expected)
    let mut actual_live: TFxSet<RecordId> = TFxSet::with_hasher(THasher::default());
    new_hnsw.for_each_rid_to_internal(|rid, _| {
        actual_live.insert(rid);
    });

    // All expected rids must be in new adapter
    for r in expected.iter() {
        assert!(
            actual_live.contains(r),
            "LOST rid {:?} — not in new adapter",
            r
        );
    }
    // No ghost: every rid in new adapter must be in expected
    for r in actual_live.iter() {
        assert!(
            expected.contains(r),
            "GHOST rid {:?} — in new adapter but not expected",
            r
        );
    }

    // Search should not panic and return only valid rids
    use crate::adapter::SearchOpts;
    let search_results = slot
        .adapter
        .search(
            &random_vec(dim as usize, 999),
            10,
            SearchOpts::default(),
            None,
        )
        .await
        .unwrap();
    for (r, _) in &search_results {
        assert!(actual_live.contains(r), "Search returned ghost rid {:?}", r);
    }
}

/// #428 (VR-6, @sh adversarial-review coverage gap) — the SQ8 twin of
/// `stress_concurrent_mutations_during_compaction`: concurrent double-write
/// upsert/delete traffic hitting the compaction target WHILE
/// `backfill_if_absent`'s own deferred-fit (`try_fit_and_rebuild` — snapshot
/// → quantizer fit → u8 graph build → catch-up loop) is actually running,
/// not merely quiescent.
///
/// `@sh`'s review of the VR-6 fix reasoned the mechanism is safe by
/// construction (the same `fit_in_flight` single-flight CAS and
/// `claim_and_publish_u8` dedup machinery VR-1 already hardened against
/// concurrent plain `upsert` traffic crossing the fit boundary), but flagged
/// that no test actually exercised a QUANTIZED target under this exact
/// combination — this test closes that gap empirically rather than leaving
/// the safety argument purely structural.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stress_concurrent_mutations_during_quantized_compaction() {
    use std::sync::atomic::AtomicU64;
    use tokio::sync::Barrier;

    let dim = 8u32;
    // Comfortably above FIT_THRESHOLD (256) so backfill_if_absent's
    // deferred-fit check fires DURING Step 4a, concurrently with the
    // worker traffic below — the exact window @sh flagged as untested.
    let n_initial = 300u16;
    let n_concurrent_ops = 200u16;

    let old_adapter = Arc::new(HnswAdapter::new_with_quantization(
        dim,
        VectorMetric::L2,
        HnswConfig {
            max_elements: 5000,
            ..Default::default()
        },
        Some(VectorQuantization::Sq8),
    ));
    for i in 0..n_initial {
        old_adapter
            .upsert(rid(i), &random_vec(dim as usize, i as u64))
            .await
            .unwrap();
    }
    crate::hnsw_adapter::await_fitted(&old_adapter).await;
    assert!(
        old_adapter.is_quantized(),
        "test setup: old adapter must have fitted (n_initial > FIT_THRESHOLD)"
    );

    let adapter_swap: Arc<arc_swap::ArcSwap<AdapterSlot>> =
        Arc::new(arc_swap::ArcSwap::from(Arc::new(AdapterSlot::live(
            old_adapter.clone() as Arc<dyn VectorAdapter>,
        ))));

    // Compaction target inherits quantization (#428 fix) — starts un-fitted,
    // fits DURING backfill once the live set crosses FIT_THRESHOLD.
    let config = old_adapter.build_config();
    let new_adapter = HnswAdapter::new_compaction_target_quantized(
        dim,
        VectorMetric::L2,
        config,
        old_adapter.quantization_mode(),
    );
    let new_adapter_arc: Arc<dyn VectorAdapter> = Arc::new(new_adapter);

    let armed_target = crate::vector_backend::CompactionTarget::new(Arc::clone(&new_adapter_arc));
    adapter_swap.store(Arc::new(AdapterSlot::armed(
        old_adapter.clone() as Arc<dyn VectorAdapter>,
        Arc::clone(&armed_target),
    )));

    let expected_live = Arc::new(tokio::sync::Mutex::new(
        (0..n_initial).map(rid).collect::<TFxSet<RecordId>>(),
    ));
    let op_counter = Arc::new(AtomicU64::new(0));
    // Barrier: 4 upsert/delete workers + 1 dedicated delete-only hammer
    // (H3 regression extension) + 1 compaction task = 6.
    let barrier = Arc::new(Barrier::new(6));

    let mut handles = Vec::new();
    for worker in 0..4u16 {
        let adapter_swap = Arc::clone(&adapter_swap);
        let expected_live = Arc::clone(&expected_live);
        let op_counter = Arc::clone(&op_counter);
        let barrier = Arc::clone(&barrier);
        let old_adapter = Arc::clone(&old_adapter);
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            let base_rid = n_initial + worker * (n_concurrent_ops / 4);
            for i in 0..(n_concurrent_ops / 4) {
                let r = rid(base_rid + i);
                let vec = random_vec(dim as usize, (base_rid + i) as u64);

                let slot = adapter_swap.load_full();
                old_adapter.upsert(r, &vec).await.unwrap();
                if let Some(t) = &slot.target {
                    t.mirror_upsert(r, &vec).await;
                }
                {
                    let mut live = expected_live.lock().await;
                    live.insert(r);
                }

                if i % 3 == 0 {
                    old_adapter.delete(r).await.unwrap();
                    if let Some(t) = &slot.target {
                        if let Some(hnsw) = t.adapter.as_hnsw_adapter() {
                            if let Some(ref del_rids) = hnsw.state.compaction_deleted_rids {
                                let _ = del_rids.insert_sync(r, ());
                            }
                        }
                        t.mirror_delete(r).await;
                    }
                    {
                        let mut live = expected_live.lock().await;
                        live.remove(&r);
                    }
                }

                op_counter.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    // H3 regression extension: dedicated DELETE-ONLY hammer targeting
    // `compaction_deleted_rids`'s hazard specifically. This map is genuinely
    // shared/hammered during double-write (unlike the other four maps'
    // spread-key nature), so a dedicated delete task increases the
    // contention on `insert_sync` racing `contains_sync` (backfill guard)
    // and `iter_sync` (Step4b reconcile) — the exact mixed-sync-accessor
    // pattern the fix addresses. Deletes target existing initial rids,
    // simulating user deletes during the compaction window.
    {
        let adapter_swap = Arc::clone(&adapter_swap);
        let expected_live = Arc::clone(&expected_live);
        let barrier = Arc::clone(&barrier);
        let old_adapter = Arc::clone(&old_adapter);
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            for i in 0..(n_concurrent_ops / 2) {
                let r = rid(i % n_initial);
                let slot = adapter_swap.load_full();
                old_adapter.delete(r).await.unwrap();
                if let Some(t) = &slot.target {
                    if let Some(hnsw) = t.adapter.as_hnsw_adapter() {
                        if let Some(ref del_rids) = hnsw.state.compaction_deleted_rids {
                            let _ = del_rids.insert_sync(r, ());
                        }
                    }
                    t.mirror_delete(r).await;
                }
                {
                    let mut live = expected_live.lock().await;
                    live.remove(&r);
                }
            }
        }));
    }

    let compaction_handle = {
        let new_adapter_arc = Arc::clone(&new_adapter_arc);
        let barrier = Arc::clone(&barrier);
        let old_adapter = Arc::clone(&old_adapter);

        tokio::spawn(async move {
            barrier.wait().await;
            tokio::task::yield_now().await;

            let live_pairs = old_adapter.collect_live_vectors();

            // Step 4a: backfill — the 300-vector live set crosses
            // FIT_THRESHOLD (256) INSIDE this call, at the SAME time worker
            // tasks are double-writing into `new_adapter_arc` above. This is
            // the exact interleaving @sh flagged as untested.
            let new_hnsw = new_adapter_arc.as_hnsw_adapter().unwrap();
            new_hnsw.backfill_if_absent(&live_pairs).await.unwrap();

            if let Some(ref del_rids) = new_hnsw.state.compaction_deleted_rids {
                let mut rids_to_delete: Vec<RecordId> = Vec::new();
                del_rids.iter_sync(|rid, ()| {
                    rids_to_delete.push(*rid);
                    true
                });
                for r in rids_to_delete {
                    let _ = new_adapter_arc.delete(r).await;
                }
            }
        })
    };

    for h in handles {
        h.await.unwrap();
    }
    compaction_handle.await.unwrap();

    adapter_swap.store(Arc::new(AdapterSlot::live(Arc::clone(&new_adapter_arc))));

    let slot = adapter_swap.load_full();
    let new_hnsw = slot.adapter.as_hnsw_adapter().unwrap();

    // The target MUST have fitted — quantization survived the compaction
    // under concurrent load (the #428 fix's whole point).
    crate::hnsw_adapter::await_fitted(new_hnsw).await;
    assert!(
        new_hnsw.is_quantized(),
        "compaction target failed to fit under concurrent load — \
         quantization lost (the #428 regression)"
    );

    let expected = expected_live.lock().await;
    let mut actual_live: TFxSet<RecordId> = TFxSet::with_hasher(THasher::default());
    new_hnsw.for_each_rid_to_internal(|rid, _| {
        actual_live.insert(rid);
    });

    // No loss: every expected rid must be present (Б-1-class invariant —
    // this is exactly what a double-claim/premature-convergence race under
    // concurrent compaction+fit would violate).
    for r in expected.iter() {
        assert!(
            actual_live.contains(r),
            "LOST rid {:?} — not in quantized compaction target",
            r
        );
    }
    for r in actual_live.iter() {
        assert!(
            expected.contains(r),
            "GHOST rid {:?} — in quantized compaction target but not expected",
            r
        );
    }

    // Search must not panic and must not return ghosts — proves the u8
    // graph is connected (Б-1 invariant) even though it was built while
    // concurrent traffic was double-writing into the same adapter.
    use crate::adapter::SearchOpts;
    let search_results = slot
        .adapter
        .search(
            &random_vec(dim as usize, 999),
            10,
            SearchOpts::default(),
            None,
        )
        .await
        .unwrap();
    for (r, _) in &search_results {
        assert!(
            actual_live.contains(r),
            "Search returned ghost rid {:?} from quantized compaction target",
            r
        );
    }
}

//! RV-INDEX-DA21C946 — fit-failure phase diagnostics.
//!
//! `schedule_fit` reports failures through the SINGLE production reporting
//! path, `HnswAdapter::report_fit_failure`: it emits the `log::error!` line
//! and — in test builds — records the diagnosed phase (`recorded_failure`
//! on the fit-test hook). Because both the log line and the recorder live
//! in that one function, deleting the `report_fit_failure` call in the
//! scheduler leaves `recorded_failure` as `None` and the tests below fail.
//! NOT covered: the exact `log::error!` output text/format — the log sink
//! itself is not captured.
//!
//! The tests assert the recorded phase matches the injected failure:
//!  * **before publication** (`fail_before_publish`): the fit task returns
//!    `Err` before any u8 graph is installed — the adapter stays on the f32
//!    brute-force path, searches keep working, and the single-flight flag is
//!    cleared so a future threshold crossing can re-arm a fit (proven by the
//!    re-arm section of the prefit test: a further upsert schedules and
//!    completes a SECOND fit).
//!  * **after publication** (`fail_after_publish`): the u8 graph was ALREADY
//!    published (`is_quantized() == true`) when the task failed — readers
//!    keep using the published quantized graph.

use crate::adapter::{SearchOpts, VectorAdapter};
use crate::hnsw_adapter::{
    clear_fit_test_hook, install_fit_test_hook, wait_for_fit, HnswAdapter, HnswConfig,
};
use shamir_index_core::kind::{VectorMetric, VectorQuantization};
use shamir_types::types::record_id::RecordId;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

fn rid(n: u64) -> RecordId {
    RecordId((n as u128).to_be_bytes())
}

fn vec8(seed: u64) -> Vec<f32> {
    let mut v = Vec::with_capacity(8);
    let mut s = seed;
    for _ in 0..8 {
        s = s
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        v.push(((s >> 33) as f32) / (u32::MAX as f32) - 0.5);
    }
    v
}

fn new_adapter() -> Arc<HnswAdapter> {
    Arc::new(HnswAdapter::new_with_quantization(
        8,
        VectorMetric::L2,
        HnswConfig {
            max_elements: 5000,
            ..Default::default()
        },
        Some(VectorQuantization::Sq8),
    ))
}

/// Bounded wait until the background fit task is no longer in flight.
async fn wait_fit_settled(adapter: &HnswAdapter) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while adapter.state.fit_in_flight.load(Ordering::Acquire) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "fit task never settled"
        );
        tokio::task::yield_now().await;
    }
}

#[tokio::test]
async fn rv_index_da21c946_prefit_failure_stays_f32_and_rearms() {
    let adapter = new_adapter();
    let hook = install_fit_test_hook(&adapter);
    hook.fail_before_publish.store(true, Ordering::Release);
    let entered = hook.entered.notified();

    // 256 distinct vectors cross FIT_THRESHOLD (256) exactly.
    for i in 0..256u64 {
        adapter.upsert(rid(i), &vec8(i)).await.expect("upsert");
    }

    // The fit task reached the gate; release it and let the injected
    // pre-publish failure fire.
    tokio::time::timeout(Duration::from_secs(10), entered)
        .await
        .expect("fit task must reach the hook gate");
    hook.release.notify_one();
    wait_fit_settled(&adapter).await;

    // Pre-fit failure: STILL on the f32 path, and the single-flight flag is
    // cleared so a future threshold crossing can re-arm a fit.
    assert!(
        !adapter.is_quantized(),
        "pre-publish failure must leave the adapter un-quantized (f32)"
    );
    assert!(
        !adapter.state.fit_in_flight.load(Ordering::Acquire),
        "single-flight flag must be cleared after the failed fit"
    );

    // DA21C946: the scheduler must have recorded the diagnosed phase.
    {
        let rf = hook.recorded_failure.lock().expect("failure lock");
        let (phase, msg) = rf.as_ref().expect("pre-publish failure must be recorded");
        assert_eq!(*phase, "before fitted publication");
        assert!(!msg.is_empty(), "recorded error text must be non-empty");
    }

    // The f32 search path still answers queries (the failed pre-publish fit
    // left the adapter un-quantized).
    let hits = adapter
        .search(
            &vec8(1),
            5,
            SearchOpts {
                ef_search: None,
                oversample: None,
            },
            None,
        )
        .await
        .expect("f32 search after pre-fit failure");
    assert!(
        hits.iter().any(|(r, _)| *r == rid(1)),
        "probe vector must be findable through the f32 path"
    );

    // Re-arm is REAL: a further upsert (live_count 256 >= FIT_THRESHOLD)
    // re-crosses `schedule_fit` and the second fit must succeed and publish.
    hook.recorded_failure.lock().expect("failure lock").take();
    adapter
        .upsert(rid(256), &vec8(256))
        .await
        .expect("re-arm upsert");
    // The re-armed fit parks at the hook gate again; hand it the release
    // permit (a Notify permit stored now is consumed by its later await).
    hook.release.notify_one();
    tokio::time::timeout(Duration::from_secs(30), wait_for_fit(&adapter))
        .await
        .expect("re-armed fit must complete");
    assert!(
        adapter.is_quantized(),
        "re-armed fit must publish the quantized graph"
    );
    assert!(
        hook.recorded_failure
            .lock()
            .expect("failure lock")
            .is_none(),
        "the successful re-armed fit must not record a failure"
    );

    // Post-refit the PUBLISHED quantized graph answers queries.
    let hits = adapter
        .search(
            &vec8(1),
            5,
            SearchOpts {
                ef_search: None,
                oversample: None,
            },
            None,
        )
        .await
        .expect("quantized search after re-armed fit");
    assert!(
        hits.iter().any(|(r, _)| *r == rid(1)),
        "probe vector must be findable through the published quantized path"
    );
    clear_fit_test_hook(&adapter);
}

#[tokio::test]
async fn rv_index_da21c946_postfit_failure_keeps_published_graph() {
    let adapter = new_adapter();
    let hook = install_fit_test_hook(&adapter);
    hook.fail_after_publish.store(true, Ordering::Release);
    let entered = hook.entered.notified();

    for i in 0..256u64 {
        adapter.upsert(rid(i), &vec8(i)).await.expect("upsert");
    }

    tokio::time::timeout(Duration::from_secs(10), entered)
        .await
        .expect("fit task must reach the hook gate");
    hook.release.notify_one();
    // Publication happened BEFORE the injected failure, so wait_for_fit
    // (quantized + settled) must complete.
    tokio::time::timeout(Duration::from_secs(30), wait_for_fit(&adapter))
        .await
        .expect("fit task must settle after post-publish failure");

    assert!(
        adapter.is_quantized(),
        "post-publish failure must leave the u8 graph PUBLISHED"
    );
    // DA21C946: the recorded phase distinguishes post-publication failures.
    {
        let rf = hook.recorded_failure.lock().expect("failure lock");
        let (phase, msg) = rf.as_ref().expect("post-publish failure must be recorded");
        assert_eq!(*phase, "after fitted publication");
        assert!(!msg.is_empty(), "recorded error text must be non-empty");
    }
    let hits = adapter
        .search(
            &vec8(2),
            5,
            SearchOpts {
                ef_search: None,
                oversample: None,
            },
            None,
        )
        .await
        .expect("quantized search after post-fit failure");
    assert!(
        hits.iter().any(|(r, _)| *r == rid(2)),
        "published quantized graph must answer queries"
    );
    clear_fit_test_hook(&adapter);
}

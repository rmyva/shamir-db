//! rv_index_7bb73da5 / rv_index_da21c946 — threshold-crossing upsert must
//! NOT await the background fit.
//!
//! `upsert`/`upsert_batch` trigger a DEFERRED fit (`schedule_fit`) once the
//! live count crosses [`FIT_THRESHOLD`] (256); the upsert caller must return
//! immediately while the fit runs in the background. A regression that made
//! the upsert path await the fit inline would stall ingest for the whole
//! (expensive) quantizer-fit + u8-graph build.
//!
//! Oracle: the fit-side test hook (`install_fit_test_hook`). The fit task
//! parks at the hook gate (`hook.entered` / `hook.release`) right after
//! snapshotting and BEFORE any fit work. The test parks the fit there and
//! JOINs the upsert task with a timeout: since the fit stays parked until
//! AFTER the join, any upsert that awaited the fit inline would keep the
//! join from completing and the timeout fires deterministically (no
//! racy `is_finished()` polling).

use crate::adapter::VectorAdapter;
use crate::hnsw_adapter::{
    clear_fit_test_hook, install_fit_test_hook, wait_for_fit, HnswAdapter, HnswConfig,
};
use shamir_index_core::kind::{VectorMetric, VectorQuantization};
use shamir_types::types::record_id::RecordId;
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

#[tokio::test]
async fn rv_index_7bb73da5_threshold_upsert_does_not_await_fit() {
    let dim = 8u32;
    // 300 > FIT_THRESHOLD (256) → the batch crosses the fit threshold.
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

    let hook = install_fit_test_hook(&adapter);
    // Register on `entered` BEFORE spawning so the fit task's `notify_one`
    // cannot race past an unregistered waiter (Notify permit semantics).
    let entered = hook.entered.notified();

    let upserts = {
        let adapter = Arc::clone(&adapter);
        tokio::spawn(async move {
            for i in 0..n {
                adapter
                    .upsert(rid(i), &vec8(i))
                    .await
                    .expect("threshold upsert");
            }
        })
    };

    // The fit task reached the hook gate (snapshot done, fit paused).
    tokio::time::timeout(Duration::from_secs(10), entered)
        .await
        .expect("fit task must reach the hook gate");

    // THE REGRESSION: the fit is still PARKED at the gate (released only
    // below), so the join below can only complete if every threshold-crossing
    // upsert already returned WITHOUT awaiting the fit. If the upsert path
    // awaited the fit inline, the join times out and the test fails
    // deterministically.
    tokio::time::timeout(Duration::from_secs(60), upserts)
        .await
        .expect(
            "upsert task must finish while the fit is parked at the gate — if the \
         upsert path awaited the fit inline, the fit would still be parked and \
         this join would time out",
        )
        .expect("upsert task ok");
    assert!(
        !adapter.is_quantized(),
        "fit is still parked at the hook gate — nothing may be published yet"
    );

    // Release the fit; it must complete and publish the quantized graph.
    hook.release.notify_one();
    tokio::time::timeout(Duration::from_secs(30), wait_for_fit(&adapter))
        .await
        .expect("fit must complete after release");
    assert!(adapter.is_quantized(), "adapter must be quantized post-fit");

    clear_fit_test_hook(&adapter);
}

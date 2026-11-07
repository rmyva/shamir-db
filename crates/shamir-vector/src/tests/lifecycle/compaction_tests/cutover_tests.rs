//! RV-INDEX-9F62C8DF — compaction cutover regression tests (T1–T8, T3b).
//!
//! Every schedule is FORCED, instance-scoped, and driven by the REAL paths
//! (`plan_insert` writers; `run_compaction_for_test` →
//! `run_background_compaction` coordinator). Synchronization is oneshot
//! PAIRs (fire hands the test the release SENDER; the writer/coordinator
//! parks until it is sent or dropped) — no sleeps, no yield loops, no
//! global statics. Every release sender handed to the test is wrapped in
//! [`ReleaseOnDrop`], so a parked writer/coordinator is released on EVERY
//! exit path (assertion panic included) and can never strand a test. Every
//! spawned task is `.await`-joined before its test returns.
//!
//! `tokio::time::timeout` appears ONLY as a hang detector / bounded
//! negative probe — never as synchronization. Each test documents its
//! revert-claim: the single production change whose revert makes it red.

use crate::adapter::VectorAdapter;
use crate::hnsw_adapter::{HnswAdapter, HnswConfig};
use crate::snapshot::{self, SnapshotError};
use crate::vector_backend::{AdapterSlot, CompactionTarget, VectorBackend};
use shamir_index_core::backend::IndexBackend;
use shamir_index_core::kind::{IndexKind, VectorBackendRef, VectorConfig, VectorMetric};
use shamir_storage::storage_in_memory::InMemoryStore;
use shamir_storage::types::Store;
use shamir_types::core::interner::InternerKey;
use shamir_types::types::common::new_map_wc;
use shamir_types::types::record_id::RecordId;
use shamir_types::types::value::InnerValue;
use smallvec::SmallVec;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;

const DIM: usize = 4;
const INDEX_ID: u32 = 77;
const FIELD_KEY: u64 = 42;
/// Upper bound for waiters that MUST fire (hang detector) and for negative
/// probes that MUST NOT fire. Never used to order two parties.
const HANG: Duration = Duration::from_secs(10);
/// Bound for a NEGATIVE probe: the event must NOT happen within this window.
const NO_HAPPEN: Duration = Duration::from_millis(200);

fn rid(n: u16) -> RecordId {
    RecordId((n as u128).to_be_bytes())
}

fn rec(vec: &[f32]) -> InnerValue {
    let mut m = new_map_wc(1);
    m.insert(
        InternerKey::new(FIELD_KEY),
        InnerValue::List(vec.iter().map(|f| InnerValue::F64(*f as f64)).collect()),
    );
    InnerValue::Map(m)
}

/// Drop-guard for a parked task's release sender: sends on drop, so a
/// parked writer/coordinator is released even when an assertion panics.
struct ReleaseOnDrop(Option<oneshot::Sender<()>>);

impl ReleaseOnDrop {
    fn new(tx: oneshot::Sender<()>) -> Self {
        Self(Some(tx))
    }

    /// Explicitly release the parked task (consumes the guard).
    fn release(mut self) {
        if let Some(tx) = self.0.take() {
            let _ = tx.send(());
        }
    }
}

impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        if let Some(tx) = self.0.take() {
            let _ = tx.send(());
        }
    }
}

/// Bounded wait for a oneshot that MUST fire. HANG DETECTOR ONLY — all real
/// synchronization is the oneshot PAIRs themselves.
async fn expect_within<T: std::future::Future>(fut: T, msg: &str) -> T::Output {
    match tokio::time::timeout(HANG, fut).await {
        Ok(v) => v,
        Err(_elapsed) => panic!("{msg}"),
    }
}

fn make_backend() -> (Arc<VectorBackend>, Arc<HnswAdapter>) {
    let desc = shamir_index_core::descriptor::IndexDescriptor::new(
        INDEX_ID,
        "cut_idx",
        u64::from(INDEX_ID),
        SmallVec::new(),
        IndexKind::Vector(Box::new(VectorConfig {
            dim: DIM as u32,
            metric: VectorMetric::L2,
            backend: VectorBackendRef::InProcessHnsw {
                ef_construct: 200,
                m: 16,
            },
            quantization: None,
        })),
    );
    let adapter = Arc::new(HnswAdapter::new(
        DIM as u32,
        VectorMetric::L2,
        HnswConfig {
            max_elements: 2000,
            ..Default::default()
        },
    ));
    let backend = Arc::new(VectorBackend::new(
        desc,
        vec![FIELD_KEY],
        adapter.clone() as Arc<dyn VectorAdapter>,
        None,
    ));
    (backend, adapter)
}

fn make_store() -> Arc<dyn Store> {
    Arc::new(InMemoryStore::new())
}

fn new_target(old_adapter: &HnswAdapter) -> Arc<CompactionTarget> {
    let t = HnswAdapter::new_compaction_target(
        DIM as u32,
        VectorMetric::L2,
        old_adapter.build_config(),
    );
    CompactionTarget::new(Arc::new(t) as Arc<dyn VectorAdapter>)
}

fn live_adapter(backend: &VectorBackend) -> Arc<dyn VectorAdapter> {
    Arc::clone(&backend.slot_for_test().adapter)
}

fn old_arc(old_adapter: &Arc<HnswAdapter>) -> Arc<dyn VectorAdapter> {
    Arc::clone(old_adapter) as Arc<dyn VectorAdapter>
}

async fn manifest_exists(store: &Arc<dyn Store>) -> bool {
    !matches!(
        snapshot::read_manifest(store, &format!("__vec_snap__{INDEX_ID}")).await,
        Err(SnapshotError::NotFound)
    )
}

/// Exact RID→value integrity on a live HnswAdapter (kills the stale-value
/// blind spot of a membership-only `contains_rid` check).
fn assert_live_value(hnsw: &HnswAdapter, id: RecordId, vec: [f32; 4]) {
    let live = hnsw.collect_live_vectors();
    let got = live
        .iter()
        .find(|(r, _)| r == &id)
        .map(|(_, v)| v.as_slice());
    assert_eq!(
        got,
        Some(vec.as_slice()),
        "exact RID→value mismatch on the live primary for {id:?}"
    );
}

/// Park a REAL writer after its primary write via the after-primary PAIR.
/// Returns the writer task and the release guard (release it explicitly, or
/// let it drop — either way the writer continues into its mirror). The
/// awaited fire PROVES the writer finished its primary write while still
/// holding its lease.
async fn park_writer(
    backend: &Arc<VectorBackend>,
    rid_v: u16,
    vec: [f32; 4],
) -> (tokio::task::JoinHandle<()>, ReleaseOnDrop) {
    let (fire_tx, fire_rx) = oneshot::channel::<oneshot::Sender<()>>();
    backend.park_writer_after_primary_for_test(fire_tx);
    let backend_w = Arc::clone(backend);
    let writer = tokio::spawn(async move {
        backend_w.plan_insert(rid(rid_v), &rec(&vec)).await.unwrap();
    });
    let release = ReleaseOnDrop::new(fire_rx.await.unwrap());
    (writer, release)
}

/// T1 — writer parked mid-mutation (after primary, holding its lease) when
/// the coordinator seals. Forces: (a) at the before-seal point the live
/// slot is unchanged; (b) the coordinator genuinely cannot complete its arm
/// while the lease is outstanding (bounded negative probe on after-arm);
/// (c) after cutover the new primary contains the drained write with its
/// EXACT value (S0 captured after the arm publish).
///
/// REVERT-CLAIM: revert F1 slot identity (maintenance.rs ~269–275: publish a
/// freshly-wrapped armed slot instead of the coordinator's retained Arc) →
/// the coordinator's later g1 `seal_and_drain` / cutover CAS run against a
/// dead slot → the arm or cutover `publish_slot` CAS returns the lost-race
/// Err → the coordinator's `unwrap` goes red.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t1_rv_index_9f62c8df_parked_writer_survives_cutover() {
    let (backend, old_adapter) = make_backend();
    let store = make_store();
    backend
        .plan_insert(rid(1), &rec(&[1.0, 0.0, 0.0, 0.0]))
        .await
        .unwrap();

    // Park the real writer after its primary write, before its mirror.
    let (writer, release) = park_writer(&backend, 2, [0.0, 1.0, 0.0, 0.0]).await;

    // Coordinator hooks, installed BEFORE the run (instance-scoped).
    let (bs_tx, bs_rx) = oneshot::channel();
    backend.install_before_seal_hook_for_test(bs_tx);
    let (arm_tx, arm_rx) = oneshot::channel::<(Arc<CompactionTarget>, oneshot::Sender<()>)>();
    backend.install_after_arm_hook_for_test(arm_tx);

    let backend_c = Arc::clone(&backend);
    let store_c = Arc::clone(&store);
    let coord =
        tokio::spawn(async move { backend_c.run_compaction_for_test(&store_c).await.unwrap() });

    // Coordinator reached the pre-seal point; it is about to seal g0 and
    // block on the parked writer's lease.
    expect_within(bs_rx, "coordinator never reached the before-seal point")
        .await
        .unwrap();
    // While the writer is parked, the live slot is still the old one.
    assert!(
        Arc::ptr_eq(&live_adapter(&backend), &old_arc(&old_adapter)),
        "live slot must not change while a lease is outstanding"
    );
    // Blocked-drain PROOF: with the lease outstanding the coordinator must
    // NOT reach the arm publication. Bounded negative probe (hang detector).
    let mut arm_probe = arm_rx;
    match tokio::time::timeout(NO_HAPPEN, &mut arm_probe).await {
        Err(_elapsed) => {} // good: the arm is still drain-blocked
        Ok(_) => panic!("arm completed while a lease was outstanding"),
    }

    // Release the writer → the arm seal drains → arm publish → the
    // coordinator parks at after-arm with ITS OWN target → release it too.
    release.release();
    let (_coord_target, arm_release) =
        expect_within(arm_probe, "arm never completed after the lease released")
            .await
            .unwrap();
    writer.await.unwrap();
    ReleaseOnDrop::new(arm_release).release();
    coord.await.unwrap();

    // The new primary contains the write that was mid-flight at seal time,
    // with its exact value.
    let live = live_adapter(&backend);
    assert!(
        !Arc::ptr_eq(&live, &old_arc(&old_adapter)),
        "cutover must publish the compacted graph"
    );
    let hnsw = live.as_hnsw_adapter().unwrap();
    assert_live_value(hnsw, rid(1), [1.0, 0.0, 0.0, 0.0]);
    assert_live_value(
        hnsw,
        rid(2),
        [0.0, 1.0, 0.0, 0.0], // the write drained at seal time (S0 after arm)
    );
}

/// T2 — injected mirror failure on the REAL coordinator's OWN target. The
/// coordinator arms, hands its target to the test via the after-arm PAIR
/// and parks; the writer is spawned while the coordinator is parked, so it
/// necessarily admits on the ARMED slot, and parks after its primary
/// holding its lease (so the cutover drain cannot pass first); the test
/// injects `fail_next_mirror` into the coordinator's target; the released
/// writer's mirror fails the target; the coordinator returns Err, disarms
/// to the old primary (write intact) and writes NO snapshot manifest.
///
/// REVERT-CLAIM: revert the post-drain `target.failed()` abort
/// (maintenance.rs ~349–358: publish the new primary unconditionally) → the
/// coordinator returns Ok and the live slot is the new primary → T2's
/// `is_err()` + old-primary + disarm asserts go red. (The failure itself is
/// injected on the target.rs `mirror_ready` injection path, ~96–105.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t2_rv_index_9f62c8df_mirror_failure_never_publishes_target() {
    let (backend, old_adapter) = make_backend();
    let store = make_store();
    backend
        .plan_insert(rid(1), &rec(&[1.0, 0.0, 0.0, 0.0]))
        .await
        .unwrap();

    let (arm_tx, arm_rx) = oneshot::channel::<(Arc<CompactionTarget>, oneshot::Sender<()>)>();
    backend.install_after_arm_hook_for_test(arm_tx);

    // Real coordinator: runs to the arm publish, parks at after-arm with
    // its own target.
    let backend_c = Arc::clone(&backend);
    let store_c = Arc::clone(&store);
    let coord = tokio::spawn(async move { backend_c.run_compaction_for_test(&store_c).await });
    let (coordinator_target, arm_release) =
        expect_within(arm_rx, "coordinator never parked at after-arm")
            .await
            .unwrap();
    let arm_release = ReleaseOnDrop::new(arm_release);

    // Writer admits on the ARMED slot and parks after its primary, holding
    // its lease (so the cutover drain cannot pass before we inject).
    let (writer, release) = park_writer(&backend, 2, [0.0, 1.0, 0.0, 0.0]).await;

    // Inject into the COORDINATOR'S target before the writer's mirror runs.
    coordinator_target.set_fail_next_mirror();

    // Release the writer: its mirror hits the injection → target fails.
    release.release();
    writer.await.unwrap(); // the user's write still returns Ok

    // Release the coordinator: backfill, reconcile, cutover drain, then the
    // post-drain failed() check must abort the publication.
    arm_release.release();
    let res = expect_within(
        coord,
        "coordinator never finished after the writer's mirror failed the target",
    )
    .await
    .unwrap();
    assert!(res.is_err(), "a failed target must fail the compaction");
    assert!(
        coordinator_target.failed(),
        "injected mirror failure must fail the target"
    );

    let slot = backend.slot_for_test();
    assert!(
        Arc::ptr_eq(&slot.adapter, &old_arc(&old_adapter)),
        "the old primary must stay live after a failed target"
    );
    assert!(slot.target.is_none(), "the failed generation must disarm");
    let hnsw = slot.adapter.as_hnsw_adapter().unwrap();
    assert_live_value(hnsw, rid(1), [1.0, 0.0, 0.0, 0.0]);
    assert_live_value(hnsw, rid(2), [0.0, 1.0, 0.0, 0.0]);
    assert!(
        !manifest_exists(&store).await,
        "an aborted generation must never trigger its snapshot"
    );
}

/// T3 — a writer arriving at a SEALED gate parks on `published` and
/// completes on the new primary after cutover. Forces the schedule
/// deterministically: the coordinator parks at after-arm (holding armed,
/// UNSEALED g1) → released, it backfills, reconciles, seals g1 (empty —
/// passes) and PARKS at the after-seal PAIR, BEFORE the cutover publish.
/// NOW g1 is sealed; the writer's `enter_write` MUST fail admission, exit
/// via `abort_admission` and park on `published` — PROVEN by the bounded
/// wait on the at-gate signal (a sealed-gate admission proof, not sync).
/// Releasing the after-seal guard publishes the slot; the parked writer
/// wakes and completes ON the new primary with its exact value.
///
/// REVERT-CLAIM: revert the sealed-gate park in `enter_write`
/// (cutover.rs ~27–39: ignore the gate's `admit()` result / never
/// `abort_admission` + park on `published`) → the writer admits straight
/// onto the sealed g1 instead of parking → the at-gate signal never fires →
/// T3's bounded at-gate expect panics → T3 red.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t3_rv_index_9f62c8df_g0_writer_lands_in_new_primary() {
    let (backend, old_adapter) = make_backend();
    let store = make_store();
    backend
        .plan_insert(rid(1), &rec(&[1.0, 0.0, 0.0, 0.0]))
        .await
        .unwrap();

    // Install after-arm PAIR + after-seal PAIR + at-gate signal BEFORE the
    // coordinator runs.
    let (arm_tx, arm_rx) = oneshot::channel::<(Arc<CompactionTarget>, oneshot::Sender<()>)>();
    backend.install_after_arm_hook_for_test(arm_tx);
    let (seal_tx, seal_rx) = oneshot::channel::<oneshot::Sender<()>>();
    backend.install_after_seal_hook_for_test(seal_tx);
    let (gate_tx, gate_rx) = oneshot::channel();
    backend.notify_writer_at_gate_for_test(gate_tx);

    let backend_c = Arc::clone(&backend);
    let store_c = Arc::clone(&store);
    let coord =
        tokio::spawn(async move { backend_c.run_compaction_for_test(&store_c).await.unwrap() });

    // 1. Coordinator parks at after-arm holding armed, UNSEALED g1.
    let (_coord_target, arm_release) =
        expect_within(arm_rx, "coordinator never parked at after-arm")
            .await
            .unwrap();
    ReleaseOnDrop::new(arm_release).release();

    // 2. It backfills, reconciles, seals g1 (empty → passes) and parks at
    //    after-seal — g1 is SEALED and the cutover publish has NOT happened.
    let seal_release = ReleaseOnDrop::new(
        expect_within(seal_rx, "coordinator never parked at after-seal")
            .await
            .unwrap(),
    );

    // 3. Writer arrives at the SEALED gate: must park on `published` —
    //    PROVEN by the at-gate signal (bounded: hang detector).
    let backend_w = Arc::clone(&backend);
    let writer = tokio::spawn(async move {
        backend_w
            .plan_insert(rid(2), &rec(&[0.0, 1.0, 0.0, 0.0]))
            .await
            .unwrap()
    });
    expect_within(
        gate_rx,
        "writer never reached the sealed gate (at-gate signal not fired)",
    )
    .await
    .unwrap();

    // 4. Release the coordinator → publish_slot fires `published` → the
    //    parked writer wakes and admits on the NEW slot.
    seal_release.release();

    // 5. Join everything; the write landed on the new primary.
    writer.await.unwrap();
    coord.await.unwrap();

    let live = live_adapter(&backend);
    assert!(
        !Arc::ptr_eq(&live, &old_arc(&old_adapter)),
        "cutover must publish the compacted graph"
    );
    let hnsw = live.as_hnsw_adapter().unwrap();
    assert_live_value(hnsw, rid(1), [1.0, 0.0, 0.0, 0.0]);
    assert_live_value(
        hnsw,
        rid(2),
        [0.0, 1.0, 0.0, 0.0], // parked at the sealed gate → new primary
    );
}

/// T3b — REJECTED-ENTRANT path: with a writer ALREADY holding a lease
/// (after-primary pair, park BEFORE the coordinator starts), a second
/// writer arriving at the sealed g0 gate fails admission, exits via
/// `abort_admission` (the last-admitted-waker contract) and parks on
/// `published`; the FIRST writer's release then lets the g0 drain complete,
/// the arm publishes, and the parked second writer completes on the new
/// primary. All waits bounded; every task joined.
///
/// REVERT-CLAIM (exact): removing the count undo from `abort_admission`
/// (the rejected entrant's increment is never decremented) leaves the g0
/// drain waiting forever after the first writer releases → the bounded
/// after-arm expect panics → red. NOT claimed: this test does NOT detect
/// reverting only the notify-on-last-release half of the shared release
/// path, because the first writer's own `release()` supplies the wake-up
/// here; that half has no dedicated regression.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t3b_rv_index_9f62c8df_rejected_entrant_wakes_drain_and_lands_on_new() {
    let (backend, old_adapter) = make_backend();
    let store = make_store();
    backend
        .plan_insert(rid(1), &rec(&[1.0, 0.0, 0.0, 0.0]))
        .await
        .unwrap();

    // Writer 1 parks after its primary BEFORE the coordinator starts: its
    // lease genuinely holds the g0 drain.
    let (writer1, release1) = park_writer(&backend, 2, [0.0, 1.0, 0.0, 0.0]).await;

    let (bs_tx, bs_rx) = oneshot::channel();
    backend.install_before_seal_hook_for_test(bs_tx);
    let (arm_tx, arm_rx) = oneshot::channel::<(Arc<CompactionTarget>, oneshot::Sender<()>)>();
    backend.install_after_arm_hook_for_test(arm_tx);
    let (gate_tx, gate_rx) = oneshot::channel();
    backend.notify_writer_at_gate_for_test(gate_tx);

    let backend_c = Arc::clone(&backend);
    let store_c = Arc::clone(&store);
    let coord =
        tokio::spawn(async move { backend_c.run_compaction_for_test(&store_c).await.unwrap() });
    expect_within(bs_rx, "coordinator never reached the before-seal point")
        .await
        .unwrap();

    // Negative probe: writer1's outstanding lease keeps the arm blocked.
    let mut arm_probe = arm_rx;
    match tokio::time::timeout(NO_HAPPEN, &mut arm_probe).await {
        Err(_elapsed) => {} // good: drain still blocked
        Ok(_) => panic!("arm completed while a lease was outstanding"),
    }

    // Writer 2 hits the SEALED g0 gate: fails admission, exits via
    // `abort_admission`, parks on `published` — proven by the at-gate
    // signal (bounded).
    let backend_w = Arc::clone(&backend);
    let writer2 = tokio::spawn(async move {
        backend_w
            .plan_insert(rid(3), &rec(&[0.0, 0.0, 1.0, 0.0]))
            .await
            .unwrap()
    });
    expect_within(gate_rx, "rejected entrant never reached the sealed gate")
        .await
        .unwrap();

    // Release writer1 → the drain completes (the rejected entrant already
    // exited via abort_admission) → arm publishes → coordinator parks at
    // after-arm → release it → cutover publishes → writer2 wakes.
    release1.release();
    let (_coord_target, arm_release) = expect_within(
        arm_probe,
        "arm never completed after the rejected entrant exited",
    )
    .await
    .unwrap();
    ReleaseOnDrop::new(arm_release).release();

    writer1.await.unwrap();
    writer2.await.unwrap();
    coord.await.unwrap();

    let live = live_adapter(&backend);
    assert!(
        !Arc::ptr_eq(&live, &old_arc(&old_adapter)),
        "cutover must publish the compacted graph"
    );
    let hnsw = live.as_hnsw_adapter().unwrap();
    assert_live_value(hnsw, rid(1), [1.0, 0.0, 0.0, 0.0]);
    assert_live_value(hnsw, rid(2), [0.0, 1.0, 0.0, 0.0]); // drained at arm seal
    assert_live_value(
        hnsw,
        rid(3),
        [0.0, 0.0, 1.0, 0.0], // rejected entrant → new primary
    );
}

/// T4 — a writer arriving while g1 is sealed (cutover drain done) parks on
/// `published`; the coordinator is PARKED at the after-seal PAIR before the
/// publish; releasing it publishes the new slot and the writer wakes and
/// completes ON the new primary (proven live pointer + exact RID value).
///
/// REVERT-CLAIM: revert the after-seal publish ordering (maintenance.rs
/// ~342–348: publish the live slot BEFORE `seal_and_drain(g1)` /
/// `fire_after_seal`) → the writer arrives at a freshly published, UNSEALED
/// new slot and admits immediately instead of parking at a sealed gate →
/// the at-gate signal never fires → T4's bounded gate expect panics → red.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t4_rv_index_9f62c8df_writer_at_sealed_gate_completes_on_new() {
    let (backend, old_adapter) = make_backend();
    let store = make_store();
    backend
        .plan_insert(rid(1), &rec(&[1.0, 0.0, 0.0, 0.0]))
        .await
        .unwrap();

    let (seal_tx, seal_rx) = oneshot::channel::<oneshot::Sender<()>>();
    backend.install_after_seal_hook_for_test(seal_tx);
    let (arm_tx, arm_rx) = oneshot::channel::<(Arc<CompactionTarget>, oneshot::Sender<()>)>();
    backend.install_after_arm_hook_for_test(arm_tx);
    let (gate_tx, gate_rx) = oneshot::channel();
    backend.notify_writer_at_gate_for_test(gate_tx);

    let backend_c = Arc::clone(&backend);
    let store_c = Arc::clone(&store);
    let coord =
        tokio::spawn(async move { backend_c.run_compaction_for_test(&store_c).await.unwrap() });
    // Coordinator arms (parked at after-arm) → release it → it backfills,
    // reconciles, seals g1, drains, then PARKS at after-seal.
    let (_coord_target, arm_release) =
        expect_within(arm_rx, "coordinator never parked at after-arm")
            .await
            .unwrap();
    ReleaseOnDrop::new(arm_release).release();
    let seal_release = ReleaseOnDrop::new(
        expect_within(seal_rx, "coordinator never parked at after-seal")
            .await
            .unwrap(),
    );

    // Writer arrives while g1 is sealed: parks — proven by the at-gate hook
    // (bounded: hang detector, never synchronization).
    let backend_w = Arc::clone(&backend);
    let writer = tokio::spawn(async move {
        backend_w
            .plan_insert(rid(2), &rec(&[0.0, 1.0, 0.0, 0.0]))
            .await
            .unwrap()
    });
    expect_within(
        gate_rx,
        "writer never reached the sealed gate (at-gate signal not fired)",
    )
    .await
    .unwrap();

    // Publish: the parked writer wakes on the NEW slot and completes there.
    seal_release.release();
    writer.await.unwrap();
    coord.await.unwrap();

    let live = live_adapter(&backend);
    assert!(
        !Arc::ptr_eq(&live, &old_arc(&old_adapter)),
        "cutover must have published the compacted graph"
    );
    let hnsw = live.as_hnsw_adapter().unwrap();
    assert_live_value(hnsw, rid(1), [1.0, 0.0, 0.0, 0.0]);
    assert_live_value(
        hnsw,
        rid(2),
        [0.0, 1.0, 0.0, 0.0], // parked at the sealed gate → new primary
    );
}

/// T5 — an instance-scoped backfill-failure injection makes the first run
/// return Err AND disarm (old primary live, no target); the retried
/// compaction succeeds and publishes the new primary.
///
/// REVERT-CLAIM: revert the DISARM itself (maintenance.rs ~300–307: on a
/// backfill error return Err WITHOUT calling `disarm_publish`) → the armed
/// slot stays live after the failed run → T5's `slot.target.is_none()` +
/// old-primary asserts go red. (No competing publisher exists in this test,
/// so the lost-CAS-retry behavior of `disarm_publish` is NOT exercisable
/// here and is deliberately not claimed.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t5_rv_index_9f62c8df_arm_failure_disarms_then_retry_succeeds() {
    let (backend, old_adapter) = make_backend();
    let store = make_store();
    backend
        .plan_insert(rid(1), &rec(&[1.0, 0.0, 0.0, 0.0]))
        .await
        .unwrap();

    backend.inject_backfill_failure_for_test();
    let res = backend.run_compaction_for_test(&store).await;
    assert!(res.is_err(), "injected backfill failure must abort the run");
    let slot = backend.slot_for_test();
    assert!(
        Arc::ptr_eq(&slot.adapter, &old_arc(&old_adapter)),
        "old primary retained after the failed arm"
    );
    assert!(
        slot.target.is_none(),
        "a failed arm must DISARM (old code left the target armed forever)"
    );

    // Retry succeeds.
    backend.run_compaction_for_test(&store).await.unwrap();
    let live = live_adapter(&backend);
    let hnsw = live.as_hnsw_adapter().unwrap();
    assert_live_value(hnsw, rid(1), [1.0, 0.0, 0.0, 0.0]);
}

/// T6 — a target failure during `plan_insert` / `apply_staged_vectors`
/// returns Ok to the caller; only the generation is invalidated
/// (per-target injection).
///
/// REVERT-CLAIM: the injection fires on the target.rs `mirror_ready` path
/// (~100–104) BEFORE any adapter call, so the honest revert is the MIRROR
/// CONTRACT in backend.rs (mirror_upsert / mirror_committed call sites
/// ~365–367, ~587–589: let a mirror-path failure propagate to the caller
/// instead of only failing the target) → the user's write returns Err →
/// T6's `.unwrap()`s go red.
#[tokio::test]
async fn t6_rv_index_9f62c8df_target_failure_returns_ok_to_caller() {
    let (backend, old_adapter) = make_backend();

    // plan_insert with an injected mirror failure.
    let target = new_target(&old_adapter);
    backend.set_slot_for_test(AdapterSlot::armed(
        old_arc(&old_adapter),
        Arc::clone(&target),
    ));
    target.set_fail_next_mirror();
    backend
        .plan_insert(rid(1), &rec(&[1.0, 0.0, 0.0, 0.0]))
        .await
        .unwrap();
    assert!(target.failed(), "mirror failure must fail the target");
    assert!(target.fail_reason().unwrap().contains("injected"));

    // apply_staged_vectors likewise.
    let target2 = new_target(&old_adapter);
    backend.set_slot_for_test(AdapterSlot::armed(
        old_arc(&old_adapter),
        Arc::clone(&target2),
    ));
    target2.set_fail_next_mirror();
    backend
        .apply_staged_vectors(&[(rid(2), vec![0.0, 1.0, 0.0, 0.0])])
        .await
        .unwrap();
    assert!(target2.failed());

    // Both writes are on the (still-authoritative) old primary, exact values.
    let live = live_adapter(&backend);
    let hnsw = live.as_hnsw_adapter().unwrap();
    assert_live_value(hnsw, rid(1), [1.0, 0.0, 0.0, 0.0]);
    assert_live_value(hnsw, rid(2), [0.0, 1.0, 0.0, 0.0]);
}

/// T7 — overlapping mutations to the SAME RID inside one armed generation.
/// Writer A is parked mid-mutation HOLDING its RID claim (after-primary
/// pair); writer B overlaps the same RID, its claim fails → the target is
/// failed, while B's own write still returns Ok.
///
/// REVERT-CLAIM: revert F3's RID-claim + fail rule in `plan_insert` (drop
/// the `claim_rid` guard, backend.rs ~351–354) → B never detects A's live
/// claim and the target never fails → T7's `target.failed()` assert goes
/// red. Justified.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t7_rv_index_9f62c8df_overlapping_same_rid_fails_target() {
    let (backend, old_adapter) = make_backend();
    let target = new_target(&old_adapter);
    backend.set_slot_for_test(AdapterSlot::armed(
        old_arc(&old_adapter),
        Arc::clone(&target),
    ));

    let (writer_a, release) = park_writer(&backend, 5, [1.0, 0.0, 0.0, 0.0]).await;

    let backend_b = Arc::clone(&backend);
    let b = tokio::spawn(async move {
        let res = backend_b
            .plan_insert(rid(5), &rec(&[0.0, 0.0, 1.0, 0.0]))
            .await;
        assert!(
            res.is_ok(),
            "the overlapping write itself must still succeed"
        );
    });

    b.await.unwrap();
    release.release();
    writer_a.await.unwrap();
    assert!(
        target.failed(),
        "same-RID overlap inside one generation must fail the target"
    );
}

/// T8 — cancellation invalidates the generation. The real coordinator arms
/// (parking at after-arm with ITS target); a writer admits on the armed
/// slot, finishes its primary write and parks (after-primary pair, lease +
/// RID claim HELD); the writer task is then CANCELLED mid-await (select!
/// drops the `plan_insert` future while the lease is alive). The unfinished
/// lease's Drop must fail the coordinator's target BEFORE releasing
/// claims/count; the coordinator then aborts at cutover (Err, old primary).
///
/// REVERT-CLAIM: revert F3 (the unfinished-Drop invalidation in
/// `WriteLease::drop`, gate.rs ~154–163) → the cancelled writer's Drop only
/// releases; the target stays healthy → the coordinator returns Ok and
/// publishes the new primary → T8's `is_err()` / `target.failed()` asserts
/// go red. Justified.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t8_rv_index_9f62c8df_cancelled_writer_invalidates_generation() {
    let (backend, old_adapter) = make_backend();
    let store = make_store();
    backend
        .plan_insert(rid(1), &rec(&[1.0, 0.0, 0.0, 0.0]))
        .await
        .unwrap();

    let (arm_tx, arm_rx) = oneshot::channel::<(Arc<CompactionTarget>, oneshot::Sender<()>)>();
    backend.install_after_arm_hook_for_test(arm_tx);
    let backend_c = Arc::clone(&backend);
    let store_c = Arc::clone(&store);
    let coord = tokio::spawn(async move { backend_c.run_compaction_for_test(&store_c).await });
    let (coordinator_target, arm_release) =
        expect_within(arm_rx, "coordinator never parked at after-arm")
            .await
            .unwrap();
    // Released only AFTER the cancelled writer's Drop must have failed the
    // target; the guard releases it on every path (panic included).
    let arm_release = ReleaseOnDrop::new(arm_release);

    // Writer admits on the ARMED slot and parks after its primary, holding
    // its lease. Unlike T2 the writer is then ABORTED, not released.
    let (fire_tx, fire_rx) = oneshot::channel::<oneshot::Sender<()>>();
    backend.park_writer_after_primary_for_test(fire_tx);
    let backend_w = Arc::clone(&backend);
    let (cancel_tx, cancel_rx) = oneshot::channel::<()>();
    let writer = tokio::spawn(async move {
        tokio::select! {
            _ = cancel_rx => Err("cancelled mid-lease"),
            res = async {
                backend_w
                    .plan_insert(rid(2), &rec(&[0.0, 1.0, 0.0, 0.0]))
                    .await
                    .map(|_| ())
            } => res.map(|_| ()).map_err(|_| "writer failed"),
        }
    });
    // Keep the release sender alive (in a guard) so the writer CANNOT leave
    // its park except by being cancelled; dropping the guard at test end
    // releases it on every path.
    let _release_guard = ReleaseOnDrop::new(fire_rx.await.unwrap());

    // ABORT the writer while it is parked mid-lease: the select drops the
    // `plan_insert` future, whose lease then runs the UNFINISHED Drop path.
    let _ = cancel_tx.send(());
    let _ = writer.await.unwrap();

    assert!(
        coordinator_target.failed(),
        "the cancelled writer's unfinished lease must fail the generation"
    );
    assert!(
        coordinator_target
            .fail_reason()
            .unwrap()
            .contains("unfinished"),
        "the invalidation must name the unfinished-lease cause"
    );

    // The coordinator aborts at cutover and keeps the old primary.
    arm_release.release();
    let res = expect_within(coord, "coordinator never aborted at cutover")
        .await
        .unwrap();
    assert!(res.is_err(), "an invalidated generation must not publish");
    let slot = backend.slot_for_test();
    assert!(Arc::ptr_eq(&slot.adapter, &old_arc(&old_adapter)));
    assert!(slot.target.is_none(), "the failed generation must disarm");
    let hnsw = slot.adapter.as_hnsw_adapter().unwrap();
    assert_live_value(hnsw, rid(1), [1.0, 0.0, 0.0, 0.0]);
    assert_live_value(
        hnsw,
        rid(2),
        [0.0, 1.0, 0.0, 0.0], // the completed primary write survives
    );
    assert!(
        !manifest_exists(&store).await,
        "an aborted generation must never trigger its snapshot"
    );
}

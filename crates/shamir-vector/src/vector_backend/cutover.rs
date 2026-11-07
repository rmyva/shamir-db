//! Atomic cutover protocol (RV-INDEX-9F62C8DF): writer admission
//! ([`VectorBackend::enter_write`]), coordinator seal/drain and
//! linearizable slot publication.

use crate::vector_backend::backend::VectorBackend;
use crate::vector_backend::gate::WriteLease;
use crate::vector_backend::slot::AdapterSlot;
#[cfg(test)]
use crate::vector_backend::target::CompactionTarget;
use arc_swap::{ArcSwap, AsRaw};
#[cfg(test)]
use shamir_storage::types::Store;
use std::sync::Arc;
use tokio::sync::Notify;

impl VectorBackend {
    /// Admit one mutation. Parks on `published` while the current slot is
    /// sealed, then retries on the freshly published slot. Registration
    /// happens BEFORE the seal check (`enable`) so the coordinator's
    /// `notify_waiters` can never be missed.
    pub(crate) async fn enter_write(&self) -> WriteLease {
        loop {
            let notified = self.published.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let slot = self.adapter.load_full();
            if slot.gate.admit() {
                return WriteLease {
                    slot,
                    claims: Vec::new(),
                    finished: false,
                };
            }
            // Sealed: exit the gate (SAME release path as a full lease —
            // F2), wait for the next publication, retry.
            slot.gate.abort_admission();
            #[cfg(test)]
            self.fire_writer_at_gate();
            notified.await;
        }
    }
}

#[cfg(test)]
impl VectorBackend {
    /// Test-only: install the after-primary/before-mirror PAIR. Fired by the
    /// real writer path: the writer hands the test the release SENDER and
    /// parks until the test sends (or drops it).
    pub(crate) fn park_writer_after_primary_for_test(
        &self,
        tx: tokio::sync::oneshot::Sender<tokio::sync::oneshot::Sender<()>>,
    ) {
        *self.after_primary_tx.lock().unwrap() = Some(tx);
    }

    /// Test-only: install the writer-at-gate signal — fired by the REAL
    /// `enter_write` when a writer parks on `published` at a sealed gate.
    pub(crate) fn notify_writer_at_gate_for_test(&self, tx: tokio::sync::oneshot::Sender<()>) {
        *self.at_gate_tx.lock().unwrap() = Some(tx);
    }

    /// Test-only: install the after-arm PAIR. The REAL coordinator fires it
    /// immediately after the arm publish, handing the test the coordinator's
    /// OWN `Arc<CompactionTarget>` plus the release sender; the coordinator
    /// then parks until the test releases it (T2/T8 determinism).
    pub(crate) fn install_after_arm_hook_for_test(
        &self,
        tx: tokio::sync::oneshot::Sender<(Arc<CompactionTarget>, tokio::sync::oneshot::Sender<()>)>,
    ) {
        *self.compaction_hooks.after_arm.lock().unwrap() = Some(tx);
    }

    /// Test-only: install the before-seal signal — fired by the REAL
    /// coordinator immediately BEFORE the arm `seal_and_drain(g0)` (T1/T3).
    pub(crate) fn install_before_seal_hook_for_test(&self, tx: tokio::sync::oneshot::Sender<()>) {
        *self.compaction_hooks.before_seal.lock().unwrap() = Some(tx);
    }

    /// Test-only: install the after-seal PAIR — fired by the REAL
    /// coordinator immediately AFTER `seal_and_drain(g1)` at cutover, BEFORE
    /// the publish; the coordinator parks until released (T4).
    pub(crate) fn install_after_seal_hook_for_test(
        &self,
        tx: tokio::sync::oneshot::Sender<tokio::sync::oneshot::Sender<()>>,
    ) {
        *self.compaction_hooks.after_seal.lock().unwrap() = Some(tx);
    }

    /// Test-only: make this backend's NEXT compaction backfill fail (T5).
    pub(crate) fn inject_backfill_failure_for_test(&self) {
        self.fail_next_backfill
            .store(true, std::sync::atomic::Ordering::Release);
    }

    /// Real writer path hook (test-only PAIR): fire the after-primary
    /// oneshot (handing the test the release sender) then park until the
    /// test releases — a true writer checkpoint mid-lease, no sleeps.
    pub(crate) async fn fire_after_primary_hook(&self) {
        let release_rx = {
            let mut guard = self.after_primary_tx.lock().unwrap();
            guard.take().map(|tx| {
                let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
                let _ = tx.send(release_tx);
                release_rx
            })
        }; // guard dropped before the park (the future must stay Send)
        if let Some(release_rx) = release_rx {
            let _ = release_rx.await; // parked until the test releases
        }
    }

    /// Real writer path hook (test-only signal): fire the at-gate oneshot.
    fn fire_writer_at_gate(&self) {
        if let Some(tx) = self.at_gate_tx.lock().unwrap().take() {
            let _ = tx.send(());
        }
    }

    /// Test-only: run the REAL coordinator body inline (deterministic
    /// cutover tests drive this instead of the threshold trigger).
    #[allow(dead_code)]
    pub(crate) async fn run_compaction_for_test(
        &self,
        info_store: &Arc<dyn Store>,
    ) -> Result<(), String> {
        crate::vector_backend::maintenance::run_background_compaction(
            &self.adapter,
            &self.published,
            info_store,
            &self.snapshot_keyspace(),
            &self.delta_count,
            &self.next_delta_idx,
            &self.snapshot_in_flight,
            &self.fail_next_backfill,
            Some(Arc::clone(&self.compaction_hooks)),
        )
        .await
    }
}

/// Instance-scoped (F5) coordinator test hooks, fired by the REAL
/// coordinator path (`run_background_compaction`). Oneshot PAIRs: fire
/// hands over a release receiver and parks the coordinator — no sleeps, no
/// yield loops. Lives on `VectorBackend` (`compaction_hooks`), never in a
/// global, so concurrent tests cannot steal each other's hooks.
#[cfg(test)]
#[derive(Default)]
pub(crate) struct CompactionHooks {
    #[allow(clippy::type_complexity)] // test-only hook plumbing
    pub(super) after_arm: std::sync::Mutex<
        Option<
            tokio::sync::oneshot::Sender<(Arc<CompactionTarget>, tokio::sync::oneshot::Sender<()>)>,
        >,
    >,
    pub(super) before_seal: std::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    pub(super) after_seal:
        std::sync::Mutex<Option<tokio::sync::oneshot::Sender<tokio::sync::oneshot::Sender<()>>>>,
}

#[cfg(test)]
impl CompactionHooks {
    /// Fire after-arm: hand the coordinator's target + release sender to
    /// the test, then park the coordinator until the test releases.
    pub(super) async fn fire_after_arm(&self, target: Arc<CompactionTarget>) {
        let release_rx = {
            let mut guard = self.after_arm.lock().unwrap();
            guard.take().map(|tx| {
                let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
                let _ = tx.send((target, release_tx));
                release_rx
            })
        }; // guard dropped before the park (the future must stay Send)
        if let Some(release_rx) = release_rx {
            let _ = release_rx.await;
        }
    }

    /// Fire before-seal (signal only; the coordinator continues into the
    /// seal, where it blocks on any outstanding lease anyway).
    pub(super) fn fire_before_seal(&self) {
        if let Some(tx) = self.before_seal.lock().unwrap().take() {
            let _ = tx.send(());
        }
    }

    /// Fire after-seal: hand the release sender to the test, then park —
    /// the coordinator sits between the drain and the publish (T4).
    pub(super) async fn fire_after_seal(&self) {
        let release_rx = {
            let mut guard = self.after_seal.lock().unwrap();
            guard.take().map(|tx| {
                let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
                let _ = tx.send(release_tx);
                release_rx
            })
        }; // guard dropped before the park (the future must stay Send)
        if let Some(release_rx) = release_rx {
            let _ = release_rx.await;
        }
    }
}

/// Seal a slot's gate and wait until every admitted lease is released.
/// Between the seal and the caller's next publication only the drain and a
/// CAS happen — nothing fallible.
pub(crate) async fn seal_and_drain(slot: &AdapterSlot) {
    slot.gate.seal();
    slot.gate.wait_drained().await;
}

/// Linearizable publication (F1): CAS on the adapter swap (pointer-equality
/// check on the returned guard), then wake writers parked on a sealed slot.
/// Takes the ALREADY-WRAPPED `Arc<AdapterSlot>` and hands that exact Arc
/// back on success — the caller's retained slot IS the published slot, so
/// later seal/drain/CAS always target the real current slot. `None` =
/// another publication won the race.
pub(crate) fn publish_slot(
    adapter_swap: &Arc<ArcSwap<AdapterSlot>>,
    expect: &Arc<AdapterSlot>,
    new: Arc<AdapterSlot>,
    published: &Notify,
) -> Option<Arc<AdapterSlot>> {
    let guard = adapter_swap.compare_and_swap(expect, Arc::clone(&new));
    let swapped = std::ptr::eq(guard.as_raw(), expect.as_raw());
    drop(guard);
    if swapped {
        published.notify_waiters();
        Some(new)
    } else {
        None
    }
}

/// Disarm-and-publish helper (F1): publish a live (target-less) slot over
/// whatever is currently installed, retrying against the freshly loaded
/// slot on a lost CAS. Every iteration seals + drains the slot it replaces,
/// so in-flight leases on any armed slot are drained before it is dropped.
pub(crate) async fn disarm_publish(adapter_swap: &Arc<ArcSwap<AdapterSlot>>, published: &Notify) {
    loop {
        let current = adapter_swap.load_full();
        seal_and_drain(&current).await;
        let live = Arc::new(AdapterSlot::live(Arc::clone(&current.adapter)));
        if publish_slot(adapter_swap, &current, live, published).is_some() {
            return;
        }
    }
}

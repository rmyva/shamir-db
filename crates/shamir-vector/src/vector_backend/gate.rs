//! Writer admission gate for compaction cutover (RV-INDEX-9F62C8DF).
//!
//! Every online mutation takes a [`WriteLease`] BEFORE touching the live
//! adapter and holds it across BOTH the primary write and the compaction
//! mirror. The coordinator seals the gate and waits on `drained` before
//! re-publishing a slot, so a publication never invalidates an in-flight
//! write and a failed mirror can never strand a completed primary write on
//! an unpublishable target.

use crate::vector_backend::slot::AdapterSlot;
use crate::vector_backend::target::CompactionTarget;
use shamir_collections::TFxSet;
use shamir_types::types::common::THasher;
use shamir_types::types::record_id::RecordId;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;

/// Per-slot admission control. One gate per published [`AdapterSlot`].
pub(crate) struct WriteGate {
    writers: AtomicUsize,
    sealed: AtomicBool,
    drained: Notify,
}

impl WriteGate {
    pub(crate) fn fresh() -> Self {
        Self {
            writers: AtomicUsize::new(0),
            sealed: AtomicBool::new(false),
            drained: Notify::new(),
        }
    }

    /// Admit one writer. Both sides are SeqCst so the coordinator's seal
    /// store and the writer's fetch_add are totally ordered: after a seal a
    /// writer either holds a lease the coordinator will wait for, or
    /// observes `sealed` and parks on `published`.
    pub(crate) fn admit(&self) -> bool {
        self.writers.fetch_add(1, Ordering::SeqCst);
        !self.sealed.load(Ordering::SeqCst)
    }

    /// Exit a sealed gate without a lease (failed admission). Deliberately
    /// the SAME code path as [`Self::release`] (F2): any decrement that can
    /// take the count to 0 while sealed must notify `drained`, or a
    /// rejected entrant aborting after an admitted writer released would
    /// strand the coordinator's drain forever.
    pub(crate) fn abort_admission(&self) {
        self.release();
    }

    /// Release one lease (or a failed admission — same path); the last
    /// writer out of a sealed gate wakes the coordinator's drain.
    pub(crate) fn release(&self) {
        if self.writers.fetch_sub(1, Ordering::SeqCst) == 1 && self.sealed.load(Ordering::SeqCst) {
            self.drained.notify_one();
        }
    }

    pub(crate) fn seal(&self) {
        self.sealed.store(true, Ordering::SeqCst);
    }

    pub(crate) fn writer_count(&self) -> usize {
        self.writers.load(Ordering::SeqCst)
    }

    /// Wait until every admitted lease is released. Register-before-check
    /// (`enable`) so a `notify_one` that lands between the count check and
    /// the await can never be lost.
    pub(crate) async fn wait_drained(&self) {
        loop {
            if self.writer_count() == 0 {
                return;
            }
            let notified = self.drained.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.writer_count() == 0 {
                return;
            }
            notified.await;
        }
    }
}

/// RAII mutation lease. Covers primary + mirror; the RID claims released on
/// drop guard the same-RID ordering rule.
///
/// F3: a lease is either explicitly [`Self::finish`]ed at its single
/// terminal point (primary ok + mirror done) or it is UNFINISHED. An
/// unfinished lease dropped on cancellation or unwind invalidates the
/// generation BEFORE unregistering: `Drop` fails the slot's target first,
/// then releases claims + writer count. Ordering: `fail`'s Release store
/// happens-before the `release` decrement (same thread, SeqCst);
/// `wait_drained` returns only after that decrement and the coordinator's
/// `failed()` check uses Acquire, so the failure is always visible.
/// A primary-error `?` return therefore intentionally leaves the lease
/// unfinished — the Drop is the invalidation. A write to an idle slot (no
/// target) needs no invalidation: Drop with no target just releases.
pub(crate) struct WriteLease {
    pub(crate) slot: Arc<AdapterSlot>,
    pub(crate) claims: Vec<RecordId>,
    pub(super) finished: bool,
}
impl WriteLease {
    pub(crate) fn slot(&self) -> &AdapterSlot {
        &self.slot
    }

    pub(crate) fn target(&self) -> Option<&Arc<CompactionTarget>> {
        self.slot.target.as_ref()
    }

    /// Mark the lease as cleanly completed at its single terminal point.
    /// After this, Drop releases without invalidating the generation.
    pub(crate) fn finish(&mut self) {
        self.finished = true;
    }

    /// Nonblocking same-RID claim spanning primary + mirror. `false` = a
    /// concurrent writer already holds this RID inside the generation —
    /// ordering ambiguity, the caller must fail the target.
    pub(crate) fn claim_rid(&mut self, rid: RecordId) -> bool {
        let Some(target) = &self.slot.target else {
            return true; // idle slot: nothing to claim
        };
        if target.claim(rid) {
            self.claims.push(rid);
            true
        } else {
            false
        }
    }

    /// Batch variant: claims each distinct rid once; duplicates inside one
    /// batch are ordered by the batch itself and safe to mirror in order.
    /// `false` on a cross-writer collision.
    pub(crate) fn claim_rids(&mut self, rids: impl IntoIterator<Item = RecordId>) -> bool {
        let mut seen: TFxSet<RecordId> = TFxSet::with_hasher(THasher::default());
        for rid in rids {
            if !seen.insert(rid) {
                continue;
            }
            if !self.claim_rid(rid) {
                return false;
            }
        }
        true
    }
}

impl Drop for WriteLease {
    fn drop(&mut self) {
        // F3: invalidate FIRST (Release store below is ordered before the
        // count release), so the coordinator's post-drain `failed()` check
        // (Acquire) always sees an unfinished lease's invalidation.
        if !self.finished {
            if let Some(target) = &self.slot.target {
                target.fail("mutation lease dropped unfinished (cancelled or unwound)".into());
            }
        }
        if let Some(target) = &self.slot.target {
            for rid in &self.claims {
                target.release(*rid);
            }
        }
        self.slot.gate.release();
    }
}

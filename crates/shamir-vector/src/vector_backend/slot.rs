//! Routing slot: ONE atomic publication unit (RV-INDEX-9F62C8DF).
//!
//! `ArcSwap<Arc<AdapterSlot>>` on `VectorBackend` is the single control
//! word. Writers admit against a slot (primary adapter + optional mirror
//! target + gate) with one RCU load; the coordinator re-publishes whole
//! slots via CAS. This replaces the old separate `adapter` + `ArcSwapOption`
//! `compaction_target` pair whose independently-sampled loads were the
//! check-then-publish race.

use crate::adapter::VectorAdapter;
use crate::vector_backend::gate::WriteGate;
use crate::vector_backend::target::CompactionTarget;
use std::sync::Arc;

/// Sized handle wrapping the live adapter (see the crate history: arc-swap's
/// `RefCnt for Arc<T>` requires `T: Sized`). One publication = one new slot.
pub(crate) struct AdapterSlot {
    pub(crate) adapter: Arc<dyn VectorAdapter>,
    /// Some while a compaction generation is armed (double-write mirror).
    pub(crate) target: Option<Arc<CompactionTarget>>,
    /// Fresh per publication: a new slot admits writers from zero.
    pub(crate) gate: WriteGate,
}

impl AdapterSlot {
    /// Idle slot (no compaction target).
    pub(crate) fn live(adapter: Arc<dyn VectorAdapter>) -> Self {
        Self {
            adapter,
            target: None,
            gate: WriteGate::fresh(),
        }
    }

    /// Armed slot: same primary, fresh gate, mirror target attached.
    pub(crate) fn armed(adapter: Arc<dyn VectorAdapter>, target: Arc<CompactionTarget>) -> Self {
        Self {
            adapter,
            target: Some(target),
            gate: WriteGate::fresh(),
        }
    }
}

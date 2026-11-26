pub(crate) mod acknowledgement;
pub(crate) mod cold_open;
pub(crate) mod core;
pub(crate) mod pipeline;
pub(crate) mod rename_fence;

// The stage-B2 commit pipeline files (entry, legacy_and_lockfree and their
// mod.rs) moved under `pipeline/phase/commit/`; the logical path
// `crate::tx::commit` and every re-exported item below stay identical.
pub(in crate::tx::commit) use pipeline::phase::commit::{entry, legacy_and_lockfree};

#[cfg(test)]
pub(crate) use crate::tx::ack::apply_backpressure;
#[allow(unused_imports)] // Compatibility paths for the moved VIS-3 API.
pub(crate) use crate::tx::ack::{
    acknowledge_known_outcome, apply_backpressure_with_budget, commit_tx_with_options,
};
#[cfg(test)]
pub(crate) use entry::TEST_SYNCED_WAL_REPO;
pub use entry::{commit_tx, wal_ops_from_tx, TxError};
pub(in crate::tx) use entry::{
    commit_tx_inner, commit_wal_durability, maybe_crash, wal_completion,
};
#[cfg(test)]
pub(crate) use entry::{
    AsyncIndexTailHook, PostValidatePrePublishHook, TEST_ASYNC_INDEX_TAIL_HOOK,
    TEST_MAX_TX_LIFETIME_OVERRIDE, TEST_POST_VALIDATE_PRE_PUBLISH_HOOK,
};
#[cfg(any(test, feature = "test-util"))]
pub use entry::{PostWalPrePublishHook, TEST_POST_WAL_PRE_PUBLISH_HOOK};
pub use legacy_and_lockfree::release_interactive_tx_locks;
pub(crate) use legacy_and_lockfree::release_pessimistic_locks;
#[cfg(test)]
pub(crate) use pipeline::phase::commit_phases::{OrphanMarkerSaveHook, TEST_ORPHAN_MARKER_PARK};

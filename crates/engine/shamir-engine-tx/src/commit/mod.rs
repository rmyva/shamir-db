pub mod acknowledgement;
pub mod cold_open;
pub mod core;
pub mod pipeline;
pub mod rename_fence;
pub use rename_fence::RenameFenceView;

// The stage-B2 commit pipeline files (entry, legacy_and_lockfree and their
// mod.rs) moved under `pipeline/phase/commit/`; the logical path
// `crate::commit` and every re-exported item below stay identical.
pub(in crate::tx) use pipeline::phase::commit::{entry, legacy_and_lockfree};

#[doc(hidden)]
pub use crate::commit::acknowledgement::acknowledge_known_outcome;
#[cfg(any(test, feature = "test-util"))]
#[doc(hidden)]
pub use crate::commit::acknowledgement::apply_backpressure;
#[allow(unused_imports)] // Compatibility paths for the moved VIS-3 API.
pub(crate) use crate::internal_ack::{apply_backpressure_with_budget, commit_tx_with_options};
#[doc(hidden)]
pub use entry::wal_completion;
#[cfg(any(test, feature = "test-util"))]
#[doc(hidden)]
pub use entry::TEST_SYNCED_WAL_REPO;
pub use entry::{commit_tx, wal_ops_from_tx, TxError};
pub(in crate::tx) use entry::{commit_tx_inner, commit_wal_durability, maybe_crash};

#[cfg(any(test, feature = "test-util"))]
#[doc(hidden)]
pub use entry::{
    AsyncIndexTailHook, PostValidatePrePublishHook, TEST_ASYNC_INDEX_TAIL_HOOK,
    TEST_MAX_TX_LIFETIME_OVERRIDE, TEST_POST_VALIDATE_PRE_PUBLISH_HOOK,
};
#[cfg(any(test, feature = "test-util"))]
pub use entry::{PostWalPrePublishHook, TEST_POST_WAL_PRE_PUBLISH_HOOK};
pub use legacy_and_lockfree::release_interactive_tx_locks;
pub use legacy_and_lockfree::release_pessimistic_locks;
#[cfg(any(test, feature = "test-util"))]
#[doc(hidden)]
pub use pipeline::phase::commit_phases::{OrphanMarkerSaveHook, TEST_ORPHAN_MARKER_PARK};

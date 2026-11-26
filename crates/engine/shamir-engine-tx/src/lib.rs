//! Transaction coordinator and commit lifecycle for the engine facade.

// Preserve historical crate-local paths after extracting the implementation.
extern crate self as tx;

pub(crate) use shamir_engine_repo as repo;
pub(crate) use shamir_engine_repo::meta;
pub(crate) use shamir_engine_table as table;
pub(crate) use shamir_index as index2;

#[cfg(any(test, feature = "test-util"))]
#[path = "commit/test_seams.rs"]
pub mod test_seams;

#[cfg(any(test, feature = "test-util"))]
#[doc(hidden)]
pub mod publish_journaled {
    pub use crate::commit::pipeline::phase::publish_journaled::publish_journaled;
}

pub mod commit;
pub(crate) mod internal_ack {
    pub(crate) use crate::commit::acknowledgement::{
        apply_backpressure_with_budget, commit_tx_with_options,
    };
}
pub use commit::acknowledgement as ack;
pub mod drain;
pub mod preparation;
pub mod recovery;
pub mod replication;

pub use crate::preparation::pre_commit;
pub use apply_replicated::{apply_replicated_batch, ApplyOutcome, ApplyPolicy};
#[doc(hidden)]
pub use commit::core::timed_stage;
pub use commit::core::{repo_core::RepoCore, tx_horizon::TxHorizon};
pub use commit::pipeline::driver::tx_outcome;
pub use commit::pipeline::driver::{
    commit_driver::CommitDriver, commit_tail_barrier::CommitTailBarrier,
};
pub use commit::pipeline::phase::commit_phases;
#[cfg(any(test, feature = "test-util"))]
#[doc(hidden)]
pub use commit::pipeline::phase::publish_journaled as journal_publish;
pub use commit::pipeline::phase::{finalize, materialize};
pub use commit::release_interactive_tx_locks;
pub use commit::{commit_tx, wal_ops_from_tx, TxError as CommitError};
pub use drain::drainer;
pub use preparation::predicate_range;
pub use preparation::repo_version_provider::RepoVersionProvider;
pub use recovery::{clean_horizon, cold_reconcile};
pub use recovery::{recover_inflight_v2, replay_v2_entry, replay_v2_op};
pub use replicated_event::ReplicatedEvent;
pub use replication::follower_durability;
pub use replication::{apply_replicated, replicated_event};
pub use shamir_tx::{
    ChangeOp, ChangelogEvent, IsolationLevel, JournalRead, RecordChange, SnapshotGuard, TxContext,
    TxId,
};
pub use tx_outcome::{BackgroundCommitHandle, MaterializationState, TxOutcome};

mod tail;

#[cfg(test)]
pub(crate) use tail::apply_backpressure;
#[allow(unused_imports)] // Preserve the original crate-internal commit paths.
pub(crate) use tail::{
    acknowledge_known_outcome, apply_backpressure_with_budget, commit_tx_with_cancel,
    commit_tx_with_kind, commit_tx_with_options,
};
#[doc(hidden)]
pub use tail::{commit_batch_tx, release_batch_locks};

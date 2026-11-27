// `tx::recovery` — V2 WAL recovery + cold-start reconciliation (stage E1
// regroup). Declarations and re-exports only. The former `tx/recovery.rs`
// body lives in `v2.rs`.
pub mod clean_horizon;
pub mod cold_reconcile;
mod index_scope;
mod v2;

pub use index_scope::is_supported_posting_key;
pub use v2::recover_inflight_v2_strict;
#[cfg(debug_assertions)]
#[doc(hidden)]
pub use v2::FAIL_HISTORY_SEED_TX_ID;
pub use v2::{recover_inflight_v2, replay_v2_entry, replay_v2_op};

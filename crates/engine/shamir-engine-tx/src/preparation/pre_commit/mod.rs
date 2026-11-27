// `tx::pre_commit` — commit preparation (stage B1 layout split). This file is
// declarations and re-exports only; the moved bodies are byte-identical to the
// former single-file `tx/pre_commit.rs` (only `super::`/`crate::` path fixes).
mod base_index;
mod claims;
mod index2;
mod locked;
mod prelock;
mod stale_value;

#[cfg(any(test, feature = "test-util"))]
#[doc(hidden)]
pub use claims::{PostPrelockPreMaterializeHook, TEST_POST_PRELOCK_PRE_MATERIALIZE_HOOK};
// Only `tx::tests::recovery::recovery_tests` consumes the old path; within the module
// `locked` reads `claims::REPO_INTERNER_SCOPE` directly.
pub use claims::unexpected_repo_interner_scope;
#[cfg(any(test, feature = "test-util"))]
#[doc(hidden)]
pub use claims::REPO_INTERNER_SCOPE;
pub(in crate::tx) use claims::{PreCommit, PreLockResult};
#[cfg(any(test, feature = "test-util"))]
#[doc(hidden)]
pub use index2::{RederivePreTxReadFailHook, TEST_REDERIVE_PRE_TX_READ_FAILURE};
pub(in crate::tx) use locked::{pre_commit_locked, pre_commit_locked_validate};
pub(in crate::tx) use prelock::pre_commit_prelock;

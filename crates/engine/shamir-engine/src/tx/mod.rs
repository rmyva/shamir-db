#[cfg(any(test, feature = "test-util"))]
#[doc(hidden)]
pub use shamir_engine_tx::test_seams::{JournalPublishHook, TableInitHook};
pub use shamir_engine_tx::*;

#[cfg(test)]
#[path = "../../../shamir-engine-tx/src/tests/mod.rs"]
pub mod tests;

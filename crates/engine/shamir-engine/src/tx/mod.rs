#[cfg(any(test, feature = "test-util"))]
#[doc(hidden)]
pub use shamir_engine_tx::test_seams::{JournalPublishHook, TableInitHook};
pub use shamir_engine_tx::*;

#[cfg(any(test, feature = "test-util"))]
#[doc(hidden)]
pub mod test_seams {
    pub use shamir_engine_tx::test_seams::{JournalPublishHook, TableInitHook};
}

#[cfg(any(test, feature = "test-util"))]
#[doc(hidden)]
pub mod publish_journaled {
    pub use shamir_engine_tx::publish_journaled::publish_journaled;
}

#[cfg(test)]
#[path = "../../../shamir-engine-tx/src/tests/mod.rs"]
pub mod tests;

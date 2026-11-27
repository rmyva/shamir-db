mod metrics;
pub mod missing_evidence;
pub mod repair;
#[cfg(any(test, feature = "test-util"))]
#[doc(hidden)]
pub use repair::repair_event;
mod rescan;
mod settle;

pub(in crate::drain::drainer) use metrics::{record_orphan_failure, Metrics};
pub use missing_evidence::settle_missing_evidence;
pub(in crate::drain::drainer) use rescan::rescan_window_prefix;

pub(in crate::drain::drainer) use settle::settle_event;

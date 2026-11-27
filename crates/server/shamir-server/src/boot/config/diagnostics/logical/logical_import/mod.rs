//! Offline logical import: verify an artifact, stage a fresh server data
//! directory, load it through the engine, validate, and publish atomically.
//! No logic lives in this module.

mod args;
mod driver;
mod error;
mod load;
mod reader;
mod stage;

pub use args::LogicalImportArgs;
pub use driver::{import, ImportReport};
// One-line addition required by the round-3 test seam: the doc(hidden)
// commit-boundary handshake must be re-exported here — a `pub use` inside
// the private `driver` module is unreachable from outside the crate.
pub use driver::COMMIT_BOUNDARY;
pub use error::LogicalImportError;

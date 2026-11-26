//! Repository-owned worker lifecycle foundation.

#[allow(dead_code)]
// Inactive lifecycle interlock foundation; exercised by tests and reserved for receipt publication wiring.
pub(crate) mod interlock;
#[allow(dead_code)] // Inactive job envelope retained for the generic worker API and tests.
pub(crate) mod job;
#[allow(dead_code)] // Inactive lifecycle owner foundation; production wiring is handled separately.
pub(crate) mod owner;
#[allow(dead_code)]
// Inactive worker foundation; retained for tests and future receipt-pipeline wiring.
pub(crate) mod worker;

#[cfg(any(test, feature = "test-util"))]
#[doc(hidden)]
pub use interlock::PubTermInterlock;
pub(crate) use interlock::PublicationClosed;
// Receipt-pipeline production surface (PR6b-2c): re-exported unconditionally;
// ShutdownError is only named in tests, the rest are production-wired.
pub(crate) use job::JobSubmitError;
#[cfg(any(test, feature = "test-util"))]
#[doc(hidden)]
pub use owner::RepoUnitLifecycle;
#[cfg_attr(not(test), allow(unused_imports))]
// ShutdownError is only named in tests; the other owner types are production-wired.
pub use owner::{
    ForegroundLifecycle, InstallError, RepoWorkerLifecycle, ShutdownError, WorkerTerminal,
};
pub(crate) use worker::LifecycleStatus;
pub(crate) use worker::WorkerExecutionState;
pub(crate) use worker::WorkerLease;
#[cfg(any(test, feature = "test-util"))]
#[doc(hidden)]
pub use worker::{spawn_worker, FailClosedResource};

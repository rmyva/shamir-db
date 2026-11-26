//! Shell command modules.
//!
//! PR1 carries the catalogue commands (`catalogue.rs`); PR3 adds the state
//! and narrow data view commands (`state.rs`); PR4 adds the rights
//! (`access.rs`) and activity (`activity.rs`) commands; later PRs add the
//! remaining command files named by design §9 (maintenance, jobs).
//! Declarations and re-exports ONLY in this file.

#[path = "rights/access.rs"]
pub(in crate::shell) mod access;
pub(in crate::shell) mod activity;
pub(in crate::shell) mod catalogue;
pub(in crate::shell) mod jobs;
pub(in crate::shell) mod maintenance;
#[path = "rights/selector.rs"]
pub(in crate::shell) mod selector;
pub(in crate::shell) mod state;

pub(in crate::shell) use access::AccessCommand;
pub(in crate::shell) use activity::ActivityCommand;
pub(in crate::shell) use catalogue::Catalogue;
pub(in crate::shell) use jobs::JobsCommand;
pub(in crate::shell) use maintenance::MaintenanceCommand;
pub(in crate::shell) use state::StateCommand;

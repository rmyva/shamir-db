#[path = "repo_admin/activity/mod.rs"]
mod activity;
mod config;
mod cursor_handlers;
#[path = "repo_admin/support/cursor_support.rs"]
mod cursor_support;
#[path = "repo_admin/execute.rs"]
mod execute;
#[path = "repo_admin/support/export_gate.rs"]
mod export_gate;
mod handler;
mod handlers;
#[path = "repo_admin/native_handler.rs"]
mod native_handler;
#[path = "repo_admin/operator_state.rs"]
mod operator_state;
#[path = "repo_admin/repo_audit.rs"]
mod repo_audit;
#[path = "repo_admin/tx_handlers.rs"]
mod tx_handlers;

#[cfg(test)]
mod tests;

// Module-name re-exports so `super::admin` / `super::subscribe_handler` /
// `super::visibility_handlers` / `super::repl_handler` keep resolving for
// sibling modules (`handler.rs`, `cursor_handlers.rs`, `repo_admin/*`)
// and for the tests under `tests/`.
pub(crate) use handlers::{admin, subscribe_handler, visibility_handlers};

pub(crate) use crate::security::user_directory::derive_scram_record;
pub use config::{CursorLimitsCap, NodeMode, QueryLimitsCap, SlowQueryConfig, TxLimitsCap};
pub use handler::{DbRequest, DbResponse, ShamirDbHandler};
pub(in crate::engine) use handler::{DisconnectCancel, MutationTasks};
pub(crate) use handler::{ExportActivityProjection, ExportJobActivityGuard};
pub use handlers::admin::AdminGlue;
pub(crate) use repo_audit::CancelRefusalAudit;

//! Re-exports only: the cold-table-open lifecycle owner lives here so the
//! `tx` layer (its callers in `tx/commit/core`) and the `instance` layer
//! (which shuts the owner down) can both reach it — `instance` may refer to
//! `tx`, never the reverse (see `.rust-guard/layers.toml`).

mod cold_open_tasks;

pub(crate) use cold_open_tasks::ColdOpenRegistration;
pub use cold_open_tasks::ColdOpenTasks;

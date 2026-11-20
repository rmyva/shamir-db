//! Boot orchestration — [`ServerLauncher`] owns the
//! [`Config`](crate::config::Config) plus the bootstrap policy and produces
//! a [`ServerHandle`](crate::server::server_handle::ServerHandle) when
//! launched.
//!
//! The launcher does NOT install the rustls crypto provider — callers
//! must do that exactly once per process (`rustls::crypto::aws_lc_rs::default_provider().install_default()`).
//! This is enforced by rustls itself: a second install is a no-op.

mod accept_loops;
#[path = "support/audit.rs"]
mod audit;
#[path = "support/helpers.rs"]
mod helpers;
mod http;
mod launch;
mod listeners;
#[path = "support/load_metrics.rs"]
mod load_metrics;
#[path = "support/meta_port.rs"]
mod meta_port;

#[cfg(test)]
mod tests;

pub use audit::audit_store_b_vs_directory;
pub use launch::ServerLauncher;

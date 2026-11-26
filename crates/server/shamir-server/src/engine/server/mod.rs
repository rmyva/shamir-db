//! Boot orchestration — extracted from `main.rs` so it is reusable from
//! integration tests.
//!
//! [`ServerLauncher`] owns the [`Config`] + bootstrap policy and produces
//! a [`ServerHandle`] when launched. The handle holds the bound listener
//! addresses (so test code can connect a real client to them) plus
//! shutdown plumbing for the listener tasks, the background scheduler,
//! and the audit appender.
//!
//! The launcher does NOT install the rustls crypto provider — callers
//! must do that exactly once per process (`rustls::crypto::aws_lc_rs::default_provider().install_default()`).
//! This is enforced by rustls itself: a second install is a no-op.

mod boot_error;
mod bootstrap_mode;
mod meta_sinks;
mod server_handle;
mod server_launcher;
mod store_release_barrier;

pub use boot_error::BootError;
pub use bootstrap_mode::{BootstrapMode, Cli, Subcmd};
#[cfg(test)]
pub(crate) use server_handle::drain_connections;
#[cfg(test)]
pub(crate) use server_handle::fire_pre_observability_registry_probe;
#[cfg(test)]
pub(crate) use server_handle::mark_fallback_drain_join;
pub use server_handle::ServerHandle;
pub use server_launcher::audit_store_b_vs_directory;
pub use server_launcher::ServerLauncher;
#[cfg(all(test, unix))]
pub(crate) use store_release_barrier::open_lock_is_discovery_gap;
pub(crate) use store_release_barrier::wait_stores_released;
#[cfg(test)]
pub(crate) use store_release_barrier::{
    bounded_probe_for_test, install_re_probe_gate, release_stores, ReleaseOutcome,
    UnverifiedReason, RE_PROBE_SUBMITS,
};

mod core;
pub(in crate::engine::server) mod release_source_stop;
mod release_tail_drain;
mod shutdown_owner;
#[cfg(test)]
mod shutdown_probes;

pub use core::ServerHandle;
pub use core::*;
#[cfg(test)]
pub(crate) use shutdown_owner::set_drain_backup_join_bound_for_test;
#[cfg(test)]
mod tests;

#[cfg(test)]
pub(crate) use shutdown_probes::*;

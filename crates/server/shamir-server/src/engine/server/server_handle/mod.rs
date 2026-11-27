mod core;
pub(in crate::engine::server) mod release_source_stop;
mod release_tail_drain;
mod shutdown_owner;
#[cfg(test)]
mod shutdown_probes;

pub use core::ServerHandle;
pub use core::*;
#[cfg(test)]
pub(in crate::engine::server) use release_source_stop::{
    join_tx_reaper, stop_release_sources_and_drain_tails, ReleaseStopBounds, REAPER_POST_ABORT_JOIN,
};
#[cfg(test)]
pub(crate) use shutdown_owner::http_listener_settlement;
pub(in crate::engine::server) use shutdown_owner::report_store_release_outcome;
#[cfg(test)]
pub(crate) use shutdown_owner::set_drain_backup_join_bound_for_test;
pub(crate) use shutdown_owner::unsettled as record_expired_owner_wait;
pub(in crate::engine::server) use shutdown_owner::DataDirLock;
pub(crate) use shutdown_owner::{HttpListenerOutcome, Settlement};
#[cfg(test)]
pub(crate) use shutdown_probes::arm_meta_snapshot_join_probe;
#[cfg(test)]
mod tests;

#[cfg(test)]
pub(crate) use shutdown_probes::*;

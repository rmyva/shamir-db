//! HNSW approximate nearest neighbor adapter using `hnsw_rs`.
//!
//! Concurrent `insert` calls are safe only with the workspace's vendored
//! `hnsw_rs` (`vendor/hnsw_rs`) lock-order fix; see its `VENDORED.md`.
//! We wrap that patched graph directly — no actor needed for HNSW itself.
//!
//! Deletion via soft-delete tombstone set; search over-scans ×2 to
//! compensate for filtered-out tombstones.

mod adapter;
mod cofilter;
mod config;
mod fit;
mod search;
mod vector_impl;

pub use adapter::HnswAdapter;
pub use cofilter::{CO_FILTER_EF_MULTIPLIER, CO_FILTER_MAX_SELECTIVITY, PRE_FILTER_MAX_CANDIDATES};
pub use config::{HnswConfig, ShamirDist, MAX_EF_SEARCH, MAX_TOPK};
#[cfg(test)]
pub(crate) use search::{
    await_fitted, clear_fit_test_hook, clear_test_search_f32_gate, install_fit_test_hook,
    install_test_search_f32_gate, wait_for_fit,
};

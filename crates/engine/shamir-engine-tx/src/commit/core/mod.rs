//! `RepoCore`: the tx-layer aggregate of a repository's shared state
//! (ENGINE-LAYERING R2c1, design §6.4); constructed by the `RepoInstance`
//! shell and handed down instead of `&RepoInstance`.

mod catalog_access;
mod durability_access;
mod journal_access;
pub mod repo_core;
mod tx_access;
pub mod tx_horizon;

pub use durability_access::timed_stage;
pub use repo_core::RepoCore;
pub use tx_horizon::TxHorizon;

// Keep historical crate-local tx paths for tx-owned tests.
extern crate self as tx;

pub(crate) use shamir_engine_query as query;
pub(crate) use shamir_engine_repo as repo;
pub(crate) use shamir_engine_table as table;

#[cfg(test)]
pub mod commit;
#[cfg(test)]
pub mod cross_protocol;
#[cfg(test)]
pub mod drain;
#[cfg(test)]
pub mod preparation;
#[cfg(test)]
pub mod recovery;
#[cfg(test)]
pub mod replication;

// Engine/facade-dependent extracted suites.
#[cfg(test)]
#[path = "../recovery/clean_horizon/tests/mod.rs"]
pub mod clean_horizon_tests;
#[cfg(test)]
#[path = "../recovery/cold_reconcile/lifecycle/tests/mod.rs"]
pub mod cold_reconcile_lifecycle_tests;
#[cfg(test)]
#[path = "../recovery/cold_reconcile/tests/mod.rs"]
pub mod cold_reconcile_tests;
#[cfg(test)]
#[path = "../replication/follower_durability/tests/mod.rs"]
pub mod follower_durability_tests;
#[cfg(test)]
#[path = "../replication/follower_durability/receipt/tests/mod.rs"]
pub mod follower_receipt_tests;
#[cfg(test)]
#[path = "../drain/drainer/orphan_repair/tests/mod.rs"]
pub mod orphan_repair_tests;
#[cfg(test)]
#[path = "../commit/acknowledgement/tests/visibility_metrics_tests.rs"]
pub mod visibility_metrics_tests;

#[cfg(test)]
#[path = "commit/publication/journal_publish_order_tests.rs"]
pub mod journal_publish_order_tests;

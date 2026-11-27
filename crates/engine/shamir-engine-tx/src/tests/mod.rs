pub mod commit;
pub mod cross_protocol;
pub mod drain;
pub mod preparation;
pub mod recovery;
pub mod replication;

// Engine/facade-dependent extracted suites.
#[path = "../recovery/clean_horizon/tests/mod.rs"]
pub mod clean_horizon_tests;
#[path = "../recovery/cold_reconcile/lifecycle/tests/mod.rs"]
pub mod cold_reconcile_lifecycle_tests;
#[path = "../recovery/cold_reconcile/tests/mod.rs"]
pub mod cold_reconcile_tests;
#[path = "../replication/follower_durability/tests/mod.rs"]
pub mod follower_durability_tests;
#[path = "../replication/follower_durability/receipt/tests/mod.rs"]
pub mod follower_receipt_tests;
#[path = "../drain/drainer/orphan_repair/tests/mod.rs"]
pub mod orphan_repair_tests;
#[path = "../commit/acknowledgement/tests/visibility_metrics_tests.rs"]
pub mod visibility_metrics_tests;

#[path = "commit/publication/journal_publish_order_tests.rs"]
pub mod journal_publish_order_tests;

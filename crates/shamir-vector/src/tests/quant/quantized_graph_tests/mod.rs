//! Test-module manifest for the quantized-graph adapter test split.

pub mod concurrent_fit;
pub mod fit_failure_phases;
pub mod fit_graph;
pub mod fit_recall;
pub mod runner;
pub mod threshold_crossing;

pub(super) use runner::{
    clustered, note_recall_stats, phase, random_vec, recall_at_k, rid, run_graph_test,
};

//! HNSW adapter tests, split by topic: `basic` (search/upsert/delete/recall)
//! and `batch_ef` (upsert_batch, per-query ef_search, deleted_count mirror).

mod basic;
mod batch_ef;

pub(super) use basic::{random_vec, rid};

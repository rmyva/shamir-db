//! V4.2 (#408) — HNSW compaction tests, split by topic:
//! `basic` (unit / integration / stress), `quantized` (#428 VR-6 +
//! backfill delete race) and `cutover_tests` (RV-INDEX-9F62C8DF gate +
//! publication protocol).

mod basic;
mod cutover_tests;
mod quantized;

pub(super) use basic::{random_vec, rid};

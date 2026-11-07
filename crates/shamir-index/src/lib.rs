//! New index system foundation (Phase 0).
//!
//! Lives alongside `index/` (the existing IndexManager) without
//! touching it — incremental approach. Once skeletons are validated
//! and FTS / Functional / Vector backends land on top, the old
//! `index/` module is rewritten in terms of `index2::IndexBackend`.
//!
//! Architectural invariants (must hold across all impls):
//! - **Read-mostly concurrency**: `scc::HashMap` for registries,
//!   `arc_swap::ArcSwap` for RCU-style snapshots, and `AtomicU*`
//!   counters. Concurrent maps may acquire internal locks; short synchronous
//!   mutexes are permitted for cold coordination and teardown, with their
//!   contention model documented and no guard held across `.await`.
//! - **Async**: `IndexBackend` is `#[async_trait]`. Stateful backends
//!   (FTS / Vector) submit writes via `tokio::sync::mpsc` to a
//!   single applier task — snapshot reads do not take the writer's lock.
//! - **Zero-copy**: `PostingKeyRef<'a>` decodes from raw bytes
//!   without allocations.
//! - **O(1)** where possible: hashmap dispatch, atomic counters.

pub mod backends;
pub mod base_index;
pub mod fts;
pub use shamir_index_core::model;
pub use shamir_vector as vector;

pub use backends::backend;
pub use backends::build_backend;
pub use backends::fts_backend;
pub use backends::fts_ranked_backend;
pub use backends::functional_backend;
pub use fts::bm25;
pub use fts::tokenizer;
pub use model::descriptor;
pub use model::kind;
pub use model::meta::meta_envelope;
pub use model::meta::persistence;
pub use model::query::expr;
pub use model::query::posting_layout;
pub use model::runtime::actor;
pub use model::runtime::lifecycle;
pub use model::runtime::reader_drain_gate;
pub use model::runtime::registry;
pub use model::runtime::state;
pub use model::runtime::write_ops;

pub use backends::backend::{
    promote_ticket, FtsMode, IndexBackend, IndexError, IndexQuery, IndexResult,
};
pub use backends::build_backend::{build_index2_backend, build_index2_backend_with_resolver};
pub use model::descriptor::IndexDescriptor;
pub use model::kind::{
    FunctionalConfig, IndexKind, TokenizerKind, VectorBackendRef, VectorConfig, VectorMetric,
};
pub use model::meta::meta_envelope::{MetaEnvelope, MetaError, ENVELOPE_MAGIC, ENVELOPE_VERSION};
pub use model::meta::persistence::{
    legacy_indexes_need_rebuild, load_legacy_index_version, save_legacy_index_version,
    LEGACY_INDEX_FORMAT_VERSION,
};
pub use model::query::expr::{ExprError, IndexExpr};
pub use model::query::posting_layout::{build_posting_key, type_tag, PostingKeyRef};
pub use model::runtime::actor::IndexActor;
pub use model::runtime::reader_drain_gate::{DropDrainGuard, ReadGuard, ReaderDrainGate};
pub use model::runtime::registry::{BackendLease, IndexRegistry};
pub use model::runtime::state::IndexState;
pub use model::runtime::write_ops::{apply_index_ops, IndexWriteOp};

#[cfg(test)]
mod tests;

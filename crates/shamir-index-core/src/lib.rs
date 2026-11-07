//! Backend-agnostic index model for the ShamirDB index system (`index2`).
//!
//! Layering: this crate owns the model — descriptors, kinds, meta
//! envelopes, persistence, query expressions, posting layout, the
//! runtime (actor, lifecycle, reader drain gate, registry, state,
//! write ops), identity v2 (IDV2) and the `IndexBackend` trait with
//! its types. Concrete backends live in `shamir-vector` (vector) and
//! `shamir-index` (base_index / FTS / functional / factory); both
//! depend on this crate, never the reverse.
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

pub mod backend;
pub mod model;
pub mod search_opts;
pub mod tokenizer;

pub use backend::{promote_ticket, FtsMode, IndexBackend, IndexError, IndexQuery, IndexResult};
pub use model::descriptor;
pub use model::descriptor::IndexDescriptor;
pub use model::kind;
pub use model::kind::{
    FunctionalConfig, IndexKind, TokenizerKind, VectorBackendRef, VectorConfig, VectorMetric,
};
pub use model::meta::meta_envelope;
pub use model::meta::meta_envelope::{MetaEnvelope, MetaError, ENVELOPE_MAGIC, ENVELOPE_VERSION};
pub use model::meta::persistence;
pub use model::meta::persistence::{
    legacy_indexes_need_rebuild, load_legacy_index_version, save_legacy_index_version,
    LEGACY_INDEX_FORMAT_VERSION,
};
pub use model::query::expr;
pub use model::query::expr::{ExprError, IndexExpr};
pub use model::query::posting_layout;
pub use model::query::posting_layout::{build_posting_key, type_tag, PostingKeyRef};
pub use model::runtime::actor;
pub use model::runtime::actor::IndexActor;
pub use model::runtime::lifecycle;
pub use model::runtime::reader_drain_gate;
pub use model::runtime::reader_drain_gate::{DropDrainGuard, ReadGuard, ReaderDrainGate};
pub use model::runtime::registry;
pub use model::runtime::registry::{BackendLease, IndexRegistry};
pub use model::runtime::state;
pub use model::runtime::state::IndexState;
pub use model::runtime::write_ops;
pub use model::runtime::write_ops::{apply_index_ops, IndexWriteOp};
pub use search_opts::SearchOpts;

#[cfg(test)]
mod tests;

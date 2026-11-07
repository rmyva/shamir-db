//! Async trait every concrete index backend implements.
//!
//! The `IndexManager` (current `index/`) will be rewritten to dispatch
//! through `Arc<dyn IndexBackend>`. Each variant (Btree, Functional,
//! FTS, Vector) lives behind its own `IndexBackend` impl.

use crate::descriptor::IndexDescriptor;
use crate::write_ops::IndexWriteOp;
use async_trait::async_trait;
use shamir_storage::error::DbError;
use shamir_storage::types::Store;
use shamir_types::record_view::RecordRef;
use shamir_types::types::record_id::RecordId;
use smallvec::SmallVec;
use std::any::Any;
use std::collections::BTreeSet;
use std::ops::Bound;
use std::pin::Pin;
use std::sync::Arc;

/// P0 index promote order: mint a ticket binding `backend` to chunk `idx`.
/// A free generic fn so the trait default stays object-safe (no `Self:
/// Sized` bound that would exclude it from the `dyn` vtable).
pub fn promote_ticket(backend: Arc<dyn IndexBackend>, idx: u64) -> shamir_tx::VectorPromoteTicket {
    shamir_tx::VectorPromoteTicket::new(BackendPromoteTurn { backend, idx })
}

/// P0 index promote order: type-erased adapter binding a backend to one
/// chunk index; the engine holds it via `shamir_tx::VectorPromoteTicket`.
struct BackendPromoteTurn {
    backend: Arc<dyn IndexBackend>,
    idx: u64,
}

#[async_trait]
impl shamir_tx::VectorPromoteTurn for BackendPromoteTurn {
    fn wait_turn<'a>(&'a self) -> Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(self.backend.wait_promote_turn(self.idx))
    }

    fn promote<'a>(
        &'a self,
        vecs: &'a [(RecordId, Vec<f32>)],
        deleted: &'a [RecordId],
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(async {
            self.backend
                .promote_graph_batch(vecs, deleted)
                .await
                .map_err(|e| e.to_string())
        })
    }

    fn release_turn(&self) {
        self.backend.release_promote_turn(self.idx);
    }
}

#[derive(Debug)]
pub enum IndexQuery {
    /// Equality / `IN` — one or more exact keys.
    Point { keys: SmallVec<[Vec<u8>; 4]> },
    /// Range lookup (`Gt` / `Lt` / `Between`).
    Range {
        lo: Bound<Vec<u8>>,
        hi: Bound<Vec<u8>>,
    },
    /// FTS — interned token IDs + combination mode. `limit` bounds ranked
    /// results when set; membership lookups remain complete.
    Fts {
        tokens: Vec<Vec<u8>>,
        mode: FtsMode,
        limit: Option<usize>,
    },
    /// Vector similarity (top-k by `kind`'s metric).
    ///
    /// `opts` carries per-query knobs (`ef_search`, `oversample`). Both
    /// default to `None` → adapter build-time default, preserving the
    /// pre-V1.1 behaviour for call-sites that construct `SearchOpts::default()`.
    Vector {
        vec: Vec<f32>,
        k: u32,
        opts: crate::search_opts::SearchOpts,
    },
}

#[derive(Debug, Clone, Copy)]
pub enum FtsMode {
    AndAll,
    OrAny,
}

#[derive(Debug)]
pub enum IndexResult {
    /// Unordered membership (Btree / Functional / FTS without scoring).
    Set(BTreeSet<RecordId>),
    /// Ranked top-k with score (BM25 / Vector).
    Ranked(Vec<(RecordId, f32)>),
}

#[derive(Debug, thiserror::Error)]
#[error("{message}")]
struct ContextualIoError {
    message: String,
    #[source]
    source: std::io::Error,
}

/// Add DDL context without changing the database error variant or I/O source.
pub fn contextualize_db_error(error: DbError, context: String) -> DbError {
    match error {
        DbError::NotFound(_) => DbError::NotFound(context),
        DbError::KeyExists(_) => DbError::KeyExists(context),
        DbError::DuplicateKey(_) => DbError::DuplicateKey(context),
        DbError::Storage(_) => DbError::Storage(context),
        DbError::RepoRequiresReopen(_) => DbError::RepoRequiresReopen(context),
        DbError::Config(_) => DbError::Config(context),
        DbError::Codec(_) => DbError::Codec(context),
        DbError::Internal(_) => DbError::Internal(context),
        DbError::Validation(_) => DbError::Validation(context),
        DbError::Function(_) => DbError::Function(context),
        DbError::ValidatorRejected(_) => DbError::ValidatorRejected(context),
        DbError::ValidatorInvalid(_) => DbError::ValidatorInvalid(context),
        DbError::Conflict(_) => DbError::Conflict(context),
        DbError::VersionConflict(_) => DbError::VersionConflict(context),
        DbError::IndexDrainInProgress(_) => DbError::IndexDrainInProgress(context),
        DbError::AtomicBatchUnsupported(_) => DbError::AtomicBatchUnsupported(context),
        DbError::Io(source) => DbError::Io(std::io::Error::new(
            source.kind(),
            ContextualIoError {
                message: context,
                source,
            },
        )),
        structured @ (DbError::UniqueIndexCreationFailed(..)
        | DbError::WriteCommittedVisibilityPending { .. }
        | DbError::AsOfFutureVersion { .. }
        | DbError::AsOfReopenRequired { .. }
        | DbError::VisibilityFutureVersion { .. }
        | DbError::VisibilityWaitInvalid { .. }
        | DbError::AsOfVisibilityTimeout { .. }
        | DbError::MigrationFrozen { .. }
        | DbError::ResyncRequired(_)
        | DbError::FollowerStateRefused { .. }
        | DbError::SubscriptionCatalogue(_)
        | DbError::ReplicationDestinationBound { .. }
        | DbError::ReplicationDestinationQuarantined { .. }
        | DbError::InternerQuotaExceeded { .. }) => structured,
    }
}

#[derive(Debug, thiserror::Error)]
pub enum IndexError {
    #[error("storage error: {0}")]
    Storage(String),
    #[error("type mismatch: {0}")]
    TypeMismatch(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("backend error: {0}")]
    Backend(String),
    /// The original database error, including its source chain.
    #[error("storage error: {0}")]
    Db(#[source] Box<shamir_storage::error::DbError>),
    #[error("n-gram {limit_kind} budget exceeded: limit {limit}, observed {observed}")]
    TokenBudgetExceeded {
        limit_kind: &'static str,
        limit: u64,
        observed: u64,
    },
}

impl From<shamir_storage::error::DbError> for IndexError {
    fn from(error: shamir_storage::error::DbError) -> Self {
        Self::Db(Box::new(error))
    }
}

impl From<IndexError> for shamir_storage::error::DbError {
    fn from(error: IndexError) -> Self {
        match error {
            IndexError::Db(source) => *source,
            other @ (IndexError::Storage(_)
            | IndexError::TypeMismatch(_)
            | IndexError::NotFound(_)
            | IndexError::Backend(_)
            | IndexError::TokenBudgetExceeded { .. }) => Self::Internal(other.to_string()),
        }
    }
}

#[async_trait]
pub trait IndexBackend: Send + Sync {
    fn descriptor(&self) -> &IndexDescriptor;

    /// Descriptor to persist, including backend-derived durable settings.
    fn persisted_descriptor(&self) -> IndexDescriptor {
        self.descriptor().clone()
    }

    /// Downcast to concrete type for specialised access (e.g. VectorBackend
    /// pre-filter / co-filter paths in V3.2). Each impl returns `self` erased.
    fn as_any(&self) -> &dyn Any;

    async fn lookup(&self, query: IndexQuery) -> Result<IndexResult, IndexError>;

    async fn rebuild(&self, source: Arc<dyn Store>) -> Result<(), IndexError>;
    async fn drop_all(&self) -> Result<(), IndexError>;

    /// tx-aware lookup variant.
    ///
    /// `staged_vectors` is the calling tx's own un-committed vectors for
    /// this table (`TxContext::staged_vectors_for(token)`), resolved by
    /// the caller which knows the table token. The default impl forwards
    /// to [`lookup`] and ignores it; only `VectorBackend` overrides this
    /// to merge the staged vectors into a similarity search so an in-tx
    /// query sees its own writes (HIGH-6). Non-vector backends stage
    /// their postings in `tx.index_write_set` and have nothing to merge
    /// here.
    ///
    /// Phase C (Step 5): for `IndexQuery::Range` under Serializable
    /// isolation, records an `IndexRange` predicate dependency BEFORE
    /// forwarding. Zero-overhead: non-Range queries and non-Serializable
    /// txs skip the recording block entirely.
    async fn lookup_tx(
        &self,
        table_token: u64,
        query: IndexQuery,
        tx: Option<&shamir_tx::TxContext>,
        _staged_vectors: Option<&[(RecordId, Vec<f32>)]>,
    ) -> Result<IndexResult, IndexError> {
        if let (IndexQuery::Range { ref lo, ref hi }, Some(t)) = (&query, tx) {
            if t.isolation == shamir_tx::IsolationLevel::Serializable {
                let idx_id = self.descriptor().name_interned;
                let map_b = |b: &Bound<Vec<u8>>| -> Bound<bytes::Bytes> {
                    match b {
                        Bound::Included(v) => Bound::Included(bytes::Bytes::copy_from_slice(v)),
                        Bound::Excluded(v) => Bound::Excluded(bytes::Bytes::copy_from_slice(v)),
                        Bound::Unbounded => Bound::Unbounded,
                    }
                };
                t.record_predicate_shared(shamir_tx::predicate_set::PredicateDep::IndexRange {
                    table_token,
                    index_id: idx_id,
                    lo: map_b(lo),
                    hi: map_b(hi),
                });
            }
        }
        self.lookup(query).await
    }

    /// Plan ops for an insert.
    ///
    /// `rec` is borrowed as a `&dyn RecordRef` so any record
    /// representation (the in-memory `InnerValue` tree or a zero-copy
    /// `RecordView` lens) can feed the planner without materialisation.
    /// The `+ Sync` bound on the trait object lets the `async_trait`
    /// future capture `rec` across `.await` points (FTS tokenisation,
    /// vector adapter upserts).
    async fn plan_insert(
        &self,
        _rid: RecordId,
        _rec: &(dyn RecordRef + Sync + '_),
    ) -> Result<Vec<IndexWriteOp>, IndexError> {
        Ok(Vec::new())
    }

    async fn plan_update(
        &self,
        _rid: RecordId,
        _old: &(dyn RecordRef + Sync + '_),
        _new: &(dyn RecordRef + Sync + '_),
    ) -> Result<Vec<IndexWriteOp>, IndexError> {
        Ok(Vec::new())
    }

    async fn plan_delete(
        &self,
        _rid: RecordId,
        _rec: &(dyn RecordRef + Sync + '_),
    ) -> Result<Vec<IndexWriteOp>, IndexError> {
        Ok(Vec::new())
    }

    /// tx-aware planning variant of [`plan_insert`].
    ///
    /// Default forwards to `plan_insert` (tx-unaware). Backends that
    /// maintain non-storage side state — e.g. `VectorBackend` with its
    /// HNSW graph — override this so a `Some(tx_id)` call does NOT touch
    /// the live structure: the vector itself is staged into the tx via
    /// [`staged_vector`] / `TxContext::stage_vector` and promoted at
    /// commit. That way a dropped (rolled-back) tx leaves no ghost state
    /// on the live structure. See HIGH-6.
    async fn plan_insert_tx(
        &self,
        rid: RecordId,
        rec: &(dyn RecordRef + Sync + '_),
        _tx_id: Option<shamir_tx::TxId>,
    ) -> Result<Vec<IndexWriteOp>, IndexError> {
        self.plan_insert(rid, rec).await
    }

    /// tx-aware planning variant of [`plan_update`]. See [`plan_insert_tx`].
    async fn plan_update_tx(
        &self,
        rid: RecordId,
        old: &(dyn RecordRef + Sync + '_),
        new: &(dyn RecordRef + Sync + '_),
        _tx_id: Option<shamir_tx::TxId>,
    ) -> Result<Vec<IndexWriteOp>, IndexError> {
        self.plan_update(rid, old, new).await
    }

    /// tx-aware planning variant of [`plan_delete`]. See [`plan_insert_tx`].
    async fn plan_delete_tx(
        &self,
        rid: RecordId,
        rec: &(dyn RecordRef + Sync + '_),
        _tx_id: Option<shamir_tx::TxId>,
    ) -> Result<Vec<IndexWriteOp>, IndexError> {
        self.plan_delete(rid, rec).await
    }

    /// Tokenize a query string into canonical token bytes using THIS index's
    /// tokenizer, so query terms match the way documents were indexed
    /// (stemming, n-grams, etc.). Default mirrors the legacy behaviour
    /// (whitespace split + lowercase) for backends without a real tokenizer.
    fn tokenize_query(&self, query: &str) -> Result<Vec<Vec<u8>>, IndexError> {
        Ok(query
            .split_whitespace()
            .map(|w| crate::tokenizer::token_identity(&w.to_lowercase()))
            .collect())
    }

    /// Apply in-memory-only ops (e.g. BumpFtsStats). Called by
    /// `apply_index_ops` for ops that don't go through the Store.
    /// Default: no-op.
    async fn apply_in_memory(&self, _ops: &[IndexWriteOp]) -> Result<(), IndexError> {
        Ok(())
    }

    /// Extract the vector this backend would stage for `rec`, if any.
    ///
    /// The executor calls this on the tx-aware write path and routes the
    /// returned vector into `TxContext::staged_vectors` (HIGH-6). Default
    /// `None` — only `VectorBackend` extracts its embedding field; every
    /// other backend stages its state as `IndexWriteOp`s instead.
    async fn staged_vector(
        &self,
        _rid: RecordId,
        _rec: &(dyn RecordRef + Sync + '_),
    ) -> Option<Vec<f32>> {
        None
    }

    /// Promote the tx's staged vectors for this table into the live
    /// structure at commit (commit pipeline Phase 5d, HIGH-6). `vecs` is
    /// the tx's `staged_vectors` slice for the owning table. Default
    /// no-op — only `VectorBackend` overrides it to feed the HNSW graph.
    /// Abort needs no counterpart: a dropped tx discards `staged_vectors`
    /// by RAII, so the live structure is never touched until commit.
    async fn apply_staged_vectors(&self, _vecs: &[(RecordId, Vec<f32>)]) -> Result<(), IndexError> {
        Ok(())
    }

    /// Promote the tx's staged vector deletes for this table into the
    /// live structure at commit (commit pipeline Phase 5d, gap#1 / HIGH-6).
    /// `deleted` is the tx's `staged_vector_deletes` slice for the owning
    /// table. Default no-op — only `VectorBackend` overrides it to
    /// tombstone the HNSW graph (and double-write the compaction target).
    /// Abort needs no counterpart: a dropped tx discards
    /// `staged_vector_deletes` by RAII, so the live graph is never touched
    /// until commit (no ghost).
    async fn apply_staged_vector_deletes(&self, _deleted: &[RecordId]) -> Result<(), IndexError> {
        Ok(())
    }

    /// Restore in-memory state from persisted artefacts at table open.
    ///
    /// This is the **startup-restore** seam (V2.2 / #401). The default
    /// implementation falls back to a full data-store scan via [`rebuild`],
    /// which is correct for every backend that derives its state purely from
    /// the data store (Functional, FTS, Btree). Backends that persist a
    /// dedicated snapshot — currently only `VectorBackend`, which dumps its
    /// HNSW graph into the info store under a snapshot keyspace — override
    /// this to try the snapshot FIRST and only fall back to [`rebuild`] when
    /// the snapshot is absent or corrupt. See `VectorBackend::restore_on_open`
    /// for the 3-branch fallback contract.
    ///
    /// `info_store` carries metadata (index descriptors, AND vector
    /// snapshots); `data_store` carries the user rows a full-rebuild scan
    /// reads. The two MAY be the same physical store on simple setups.
    ///
    /// Returns `Ok(())` once the backend is query-ready, by whichever path
    /// succeeded. The caller (`TableManager::create`) does NOT abort the
    /// table open on `Err` — a broken index is preferable to a failed table
    /// open (the underlying storage fault may be transient, and other
    /// indexes/tables must stay usable). R0-D (#1013): the caller DOES,
    /// however, fail this specific backend CLOSED — it logs the error and
    /// moves the backend's registry state to `Failed` (via
    /// `IndexRegistry::set_failed`) instead of leaving it at whatever
    /// in-memory state its freshly-constructed adapter started with (empty,
    /// for most backends). `Failed` is planner-invisible exactly like
    /// `Building`, so a client gets "no usable index" (falls back to a full
    /// scan / errors per the query's own semantics) instead of a
    /// confident-looking but silently incomplete/empty result.
    async fn restore_on_open(
        &self,
        _info_store: Arc<dyn Store>,
        data_store: Arc<dyn Store>,
    ) -> Result<(), IndexError> {
        self.rebuild(data_store).await
    }

    /// Number of times this backend has fallen back to a FULL rebuild scan
    /// since it was constructed (V2.2 / #401 instrumentation). A successful
    /// snapshot load on open does NOT increment this counter; every
    /// full-scan rebuild (no snapshot, corrupt snapshot, version mismatch)
    /// does. The default `0` applies to backends with no snapshot path
    /// (FTS / functional / btree): they always `rebuild` on open, and this
    /// counter is simply not consulted for them.
    ///
    /// Exposed primarily for tests proving "the snapshot was used, no scan
    /// happened" (see `vector_restore_tests`).
    fn rebuild_count(&self) -> u64 {
        0
    }

    /// Push an updated scalar resolver into backends that evaluate scalar
    /// expressions (e.g. `FunctionalBackend` with `IndexExpr::Scalar`).
    ///
    /// Called by `TableManager::set_scalar_resolver` after the per-DB
    /// resolver is injected, so that backends constructed during reopen
    /// (which captured a builtins-only resolver) can be retroactively
    /// updated to see user-registered scalars. Default: no-op.
    fn update_scalar_resolver(&self, _resolver: &shamir_funclib::scalar_resolver::ScalarResolver) {}

    /// V2.3 (#402) — append a delta-log chunk capturing the vector mutations
    /// the executor just promoted into the live structure (commit Phase 5d).
    ///
    /// Called by `apply_vector_batch` IMMEDIATELY AFTER the in-memory promote
    /// succeeded (the chunk is the durable echo of an already-applied
    /// mutation). The default no-op covers every backend that has no
    /// incremental durability story (FTS / functional / btree / brute-force);
    /// only `VectorBackend` overrides this to write a delta chunk into its
    /// snapshot keyspace. `info_store` is the table's info store — the SAME
    /// store the snapshot lives in — passed by the caller because Phase 5d
    /// resolves it from the table handle and the backend does not own one.
    ///
    /// `vecs` are the vectors just promoted (translated to `Upsert` ops); a
    /// tx that deleted a vector-backed row passes the deletion through
    /// `deleted` (translated to `Delete` ops). A tx with no vector rows
    /// passes both empty — the backend skips the chunk write.
    ///
    /// §5.6 — synchronous in Phase 5d: a delta chunk is ONE `Store::set`
    /// (one memtable insert on every backend we ship). That is cheap enough
    /// to run on the commit-ack path without blocking the ack; the
    /// alternative (background append) would split the durable delta from
    /// the in-memory promote, re-opening the very loss window the delta-log
    /// exists to close. The §5.6 "don't block the ack" rule is honoured by
    /// the BACKGROUND SNAPSHOT trigger (`trigger_snapshot_check`), not by
    /// this synchronous append.
    async fn append_vector_delta(
        &self,
        _info_store: &Arc<dyn Store>,
        _vecs: &[(RecordId, Vec<f32>)],
        _deleted: &[RecordId],
    ) -> Result<Option<u64>, IndexError> {
        Ok(None)
    }

    /// P0 index promote order (stage L3b): wait until delta chunk `idx` is
    /// this backend's promote turn. No-op for backends without a delta log.
    async fn wait_promote_turn(&self, _idx: u64) {}

    /// P0 index promote order: release chunk `idx`'s promote turn
    /// (finished-set insert + advance + notify). Sync so a ticket `Drop`
    /// can call it; releasing does NOT acknowledge the chunk.
    fn release_promote_turn(&self, _idx: u64) {}

    /// P0 index promote order: apply this chunk's vector deletes + inserts
    /// as ONE ordered unit inside the turn (single write lease, compaction
    /// mirroring included). No-op for backends without a live graph.
    async fn promote_graph_batch(
        &self,
        _vecs: &[(RecordId, Vec<f32>)],
        _deleted: &[RecordId],
    ) -> Result<(), IndexError> {
        Ok(())
    }

    /// P0 index promote order: wrap this backend + chunk index into a
    /// Drop-guard [`shamir_tx::VectorPromoteTicket`]. `append_vector_delta`
    /// hands one back per successfully reserved chunk index.
    /// P0 delta-prune (brief 164): acknowledge that delta chunk `idx`'s
    /// mutations reached the live graph. Advances the backend's contiguous
    /// applied prefix — the boundary snapshot tasks sample for
    /// `manifest.delta_applied_upto` and pruning. A chunk that is never
    /// acknowledged (pending or failed graph promote) stays replayable and
    /// is never pruned. Default no-op: only backends with a delta log care.
    fn mark_delta_applied(&self, _idx: u64) {}

    /// V2.3 (#402) — check whether the accumulated delta-log mutations have
    /// crossed the snapshot threshold and, if so, kick off a single-flight
    /// background snapshot (dump + generation flip + prune). Called by the
    /// executor at the tail of Phase 5d, AFTER `append_vector_delta`.
    ///
    /// The default no-op covers every backend without a snapshot path.
    /// `VectorBackend` overrides this with the AtomicU64 counter +
    /// AtomicBool single-flight dance described in the V2.3 brief: the
    /// counter is bumped by the delta size; a `fetch_add` that crosses the
    /// threshold spawns a `tokio::task` that runs the dump + flip + prune
    /// OUTSIDE the commit-ack path (§5.6 — the snapshot must NOT block the
    /// ack). The spawned task is single-flight: a second crossing while the
    /// first is in flight is a no-op (the counter keeps climbing and the
    /// next ack will re-arm once the in-flight task clears the flag).
    ///
    /// `info_store` is the table's info store (same as
    /// `append_vector_delta`); passed by the caller because the backend
    /// does not own a table handle.
    fn trigger_snapshot_check(&self, _info_store: &Arc<dyn Store>) {}

    /// V4.2 (#408) — check whether background compaction should trigger.
    /// Default no-op; only `VectorBackend` overrides.
    fn trigger_compaction_check(&self, _info_store: &Arc<dyn Store>) {}
}

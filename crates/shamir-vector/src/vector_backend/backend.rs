//! `VectorBackend` itself: the struct, its inherent helpers, and the
//! `IndexBackend` trait impl. Background snapshot / compaction task bodies
//! live in [`super::maintenance`].

use crate::adapter::VectorAdapter;
use crate::snapshot::{self, SnapshotError, SnapshotManifest};
use crate::vector_backend::maintenance::ve;
use crate::vector_backend::slot::AdapterSlot;
use arc_swap::{ArcSwap, ArcSwapOption};
use async_trait::async_trait;
use futures::StreamExt;
use shamir_index_core::backend::{IndexBackend, IndexError, IndexQuery, IndexResult};
use shamir_index_core::descriptor::IndexDescriptor;
use shamir_index_core::write_ops::IndexWriteOp;
use shamir_storage::types::Store;
use shamir_tunables::instance_defaults::{
    vector_snapshot_delta_threshold, VECTOR_COMPACTION_MIN_LIVE, VECTOR_COMPACTION_RATIO_THRESHOLD,
};
use shamir_types::core::interner::InternerKey;
use shamir_types::record_view::{RecordRef, ScalarRef};
use shamir_types::types::record_id::RecordId;
use shamir_types::types::value::InnerValue;
use smallvec::SmallVec;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{oneshot, Notify};

#[cfg(test)]
use crate::vector_backend::cutover::CompactionHooks;

/// Keyspace prefix every vector-snapshot record lives under in the info
/// store. The full keyspace is `__vec_snap__<index_id>` where `<index_id>`
/// is the `IndexDescriptor::id` (a stable, persisted, auto-incremented u32).
/// Using the numeric id — not the human name — keeps the keyspace stable
/// across renames and immune to path/encoding concerns.
///
/// `snapshot::dump_snapshot` / `load_snapshot` prefix every chunk, sidecar,
/// and manifest key with `<keyspace>.`, so the actual on-disk keys look like
/// `__vec_snap__7.g0.graph.000000`, `__vec_snap__7.g0.sidecar`,
/// `__vec_snap__7.manifest`.
pub(super) const SNAPSHOT_KEYSPACE_PREFIX: &str = "__vec_snap__";

/// (the slot struct itself lives in [`super::slot`] — see there for the
/// RV-INDEX-9F62C8DF single-control-word design; the slot stays swappable:
/// #402 generation flips and #408 compaction rebuild-aside swap a
/// freshly-built graph into a LIVE backend; readers snapshot via
/// `load_full()` — an `Arc`, never a `Guard` held across `.await` — so a
/// concurrent swap is wait-free RCU.)
pub struct VectorBackend {
    descriptor: IndexDescriptor,
    field_path: Vec<u64>,
    /// Hot-swappable adapter (RCU snapshot pattern). The open path's
    /// `restore_on_open` swaps a freshly-loaded snapshot adapter in for the
    /// empty placeholder constructed by `build_index2_backend_with_resolver`;
    /// every read-side method grabs a cheap `load()` snapshot, so a
    /// concurrent query never sees a half-swapped adapter. Mirrors
    /// `BruteForceAdapter`'s use of `ArcSwap`.
    ///
    /// `Arc<ArcSwap<AdapterSlot>>` — see [`super::slot`].
    pub(crate) adapter: Arc<ArcSwap<AdapterSlot>>,
    // Weak on purpose (P1 reopen fix): a strong Arc here would keep the
    // engine's Fjall keyspace alive after the repo drops its own handle,
    // making a reopen fail with `FjallError: Locked`. Upgrade in `drop_all`.
    info_store: std::sync::OnceLock<std::sync::Weak<dyn Store>>,
    /// Full-scan rebuild counter (V2.2 / #401 instrumentation). Incremented
    /// on EVERY fallback to a data-store-scan rebuild; NOT on a snapshot
    /// load. Exposed via [`rebuild_count`] (tests prove the snapshot path).
    full_rebuild_count: AtomicU64,
    /// V2.3 (#402) — number of vector mutations (upserts + deletes) appended
    /// to the delta-log since the last successful generation flip. Bumped by
    /// `append_vector_delta` (Phase 5d) and reset to 0 by the background
    /// snapshot task after a flip lands. Compared against the tunable
    /// threshold to decide whether to spawn the dump + flip + prune.
    /// Relaxed ordering is sufficient: a missed threshold crossing on one
    /// ack is harmless (the next ack re-checks); the only invariant is
    /// monotonic growth between flips, which `fetch_add` guarantees.
    ///
    /// `Arc`-shared so the single-flight background snapshot task can reset
    /// it after a successful flip without holding a borrow on the backend
    /// (the task is `'static`, and an inline `AtomicU64` cannot move into
    /// a spawned task).
    pub(super) delta_count: Arc<AtomicU64>,
    /// V2.3 (#402) — single-flight guard for the background snapshot task.
    /// `compare_exchange(false, true)` on the threshold crossing arms the
    /// guard; the spawned task clears it (`store(false)`) on completion
    /// (success OR failure). A second crossing while the first task is in
    /// flight is a no-op — the counter keeps climbing and the next ack
    /// re-arms once the in-flight task clears the flag.
    ///
    /// `Arc`-shared for the same reason as `delta_count`.
    pub(super) snapshot_in_flight: Arc<AtomicBool>,
    /// V2.3 (#402) — monotonic index of the next delta chunk to write.
    /// Seeded by `restore_on_open` from `highest_delta_index` so a restart
    /// does not collide with existing chunks; bumped by
    /// `append_vector_delta` on every chunk write. Touched only on the
    /// commit-ack path (Phase 5d is serial per tx) so a plain `fetch_add`
    /// is correct without a lock.
    ///
    /// `Arc`-shared so the background snapshot task can read the
    /// `delta_applied_upto` for the new manifest.
    pub(super) next_delta_idx: Arc<AtomicU64>,
    /// P0 delta-prune (brief 164) — contiguous APPLIED prefix: every chunk
    /// below was acknowledged (`mark_delta_applied`). Sampled BEFORE the
    /// dump for the manifest boundary and prune bound, never the allocator.
    pub(super) applied_prefix: Arc<AtomicU64>,
    /// P0 delta-prune (brief 164) — acknowledged chunk indices not yet
    /// covered by the contiguous prefix (out-of-order completions); drained
    /// by `ack_delta_applied`. Ordered set per the brief (`scc::TreeIndex`).
    pub(super) applied_set: Arc<scc::TreeIndex<u64, ()>>,
    /// P0 index promote order (stage L3b) — next delta chunk index allowed to
    /// promote into the live graph; same-backend promotes run ONE at a time in
    /// chunk order (one Acquire load + a CAS uncontended, no mutex). Separated
    /// from `applied_prefix`: a FAILED promote releases the turn (later chunks
    /// proceed) but never acknowledges (the chunk stays replayable and the
    /// prune prefix stays below it).
    pub(super) promote_turn: Arc<AtomicU64>,
    /// P0 index promote order — chunk indices released ahead of
    /// `promote_turn` (failed/cancelled); same pattern as `applied_set`.
    pub(super) turn_finished: Arc<scc::TreeIndex<u64, ()>>,
    /// P0 index promote order — wake waiters on every turn advance.
    pub(super) turn_notify: Arc<Notify>,
    /// Test seam (stage L3b, round-2 finding 3): one-shot fired when a
    /// promote has to WAIT at the turnstile. ALWAYS compiled — the engine's
    /// tests build this crate WITHOUT `cfg(test)`, so a cfg-gated seam
    /// would never fire cross-crate. Contended-wait-path-only; the test
    /// must hold ONLY the receiver so `Arc::try_unwrap` succeeds on fire.
    #[doc(hidden)]
    pub turn_wait_tx: ArcSwapOption<oneshot::Sender<()>>,
    /// Arm flag for [`Self::turn_wait_tx`] — see `maintenance.rs`.
    #[doc(hidden)]
    pub turn_wait_armed: AtomicBool,
    /// Delta-count threshold that arms a background snapshot. Defaults to the
    /// production tunable `VECTOR_SNAPSHOT_DELTA_THRESHOLD`; overridable in
    /// tests (via `set_snapshot_threshold_for_test`) so the full
    /// trigger → spawn → `run_background_snapshot` path can be exercised
    /// deterministically without appending 10k real vectors.
    pub(super) snapshot_threshold: AtomicU64,
    /// V4.2 (#408) — single-flight guard for the background compaction task.
    pub(super) compaction_in_flight: Arc<AtomicBool>,
    /// RV-INDEX-9F62C8DF — publication signal. Writers parked on a sealed
    /// slot's gate await this; the coordinator notifies after every slot
    /// CAS. `Arc`-shared so the spawned compaction task can publish.
    pub(crate) published: Arc<Notify>,
    /// Test-only oneshot PAIR fired by the REAL writer path after the
    /// primary write succeeds and before the mirror (T1/T2 park the writer
    /// here; the writer hands the test the release SENDER and parks).
    #[cfg(test)]
    pub(super) after_primary_tx: std::sync::Mutex<Option<oneshot::Sender<oneshot::Sender<()>>>>,
    /// Test-only (F5): signal fired when a writer parks on `published` at a
    /// sealed gate inside the real `enter_write` (T3/T4).
    #[cfg(test)]
    pub(super) at_gate_tx: std::sync::Mutex<Option<oneshot::Sender<()>>>,
    /// Test-only (F5): instance-scoped coordinator hooks (after-arm /
    /// before-seal / after-seal), fired by the real coordinator path.
    #[cfg(test)]
    pub(super) compaction_hooks: Arc<CompactionHooks>,
    /// Test-only (F5): instance-scoped injection — the backend's next
    /// compaction backfill fails (T5). Consulted by `run_background_compaction`.
    #[cfg(test)]
    pub(super) fail_next_backfill: Arc<AtomicBool>,
    /// V4.2 (#408) — compaction ratio threshold (test-overridable).
    pub(super) compaction_ratio_threshold: std::sync::atomic::AtomicU64,
    /// V4.2 (#408) — compaction min live count (test-overridable).
    pub(super) compaction_min_live: std::sync::atomic::AtomicU64,
}

impl VectorBackend {
    pub fn new(
        descriptor: IndexDescriptor,
        field_path: Vec<u64>,
        adapter: Arc<dyn VectorAdapter>,
        info_store: Option<Arc<dyn Store>>,
    ) -> Self {
        let store_lock = std::sync::OnceLock::new();
        if let Some(s) = &info_store {
            let _ = store_lock.set(Arc::downgrade(s));
        }
        Self {
            descriptor,
            field_path,
            adapter: Arc::new(ArcSwap::from(Arc::new(AdapterSlot::live(adapter)))),
            info_store: store_lock,
            full_rebuild_count: AtomicU64::new(0),
            delta_count: Arc::new(AtomicU64::new(0)),
            snapshot_in_flight: Arc::new(AtomicBool::new(false)),
            next_delta_idx: Arc::new(AtomicU64::new(0)),
            applied_prefix: Arc::new(AtomicU64::new(0)),
            applied_set: Arc::new(scc::TreeIndex::new()),
            promote_turn: Arc::new(AtomicU64::new(0)),
            turn_finished: Arc::new(scc::TreeIndex::new()),
            turn_notify: Arc::new(Notify::new()),
            turn_wait_tx: ArcSwapOption::empty(),
            turn_wait_armed: AtomicBool::new(false),
            snapshot_threshold: AtomicU64::new(vector_snapshot_delta_threshold()),
            compaction_in_flight: Arc::new(AtomicBool::new(false)),
            published: Arc::new(Notify::new()),
            #[cfg(test)]
            after_primary_tx: std::sync::Mutex::new(None),
            #[cfg(test)]
            at_gate_tx: std::sync::Mutex::new(None),
            #[cfg(test)]
            compaction_hooks: Arc::new(CompactionHooks::default()),
            #[cfg(test)]
            fail_next_backfill: Arc::new(AtomicBool::new(false)),
            compaction_ratio_threshold: AtomicU64::new(VECTOR_COMPACTION_RATIO_THRESHOLD.to_bits()),
            compaction_min_live: AtomicU64::new(VECTOR_COMPACTION_MIN_LIVE as u64),
        }
    }

    /// Lower the background-snapshot threshold so a test can cross it without
    /// appending the production default (10k) vectors. Test-only.
    #[cfg(test)]
    pub(crate) fn set_snapshot_threshold_for_test(&self, threshold: u64) {
        self.snapshot_threshold.store(threshold, Ordering::Release);
    }

    /// Read the single-flight guard flag. Test-only - used to prove the flag
    /// is cleared after a background snapshot completes (drop-guard reset).
    #[cfg(test)]
    pub(crate) fn snapshot_in_flight_for_test(&self) -> bool {
        self.snapshot_in_flight.load(Ordering::Acquire)
    }

    /// Test-only access to the live adapter (e.g. to assert restored
    /// build config in builder regression tests).
    #[cfg(test)]
    pub(crate) fn adapter_for_test(&self) -> Arc<dyn VectorAdapter> {
        Arc::clone(&self.adapter.load_full().adapter)
    }

    /// Snapshot keyspace for this backend's index: `__vec_snap__<id>`.
    /// Derived from the persisted, stable `IndexDescriptor::id` so the
    /// keyspace survives index renames and re-derives identically on every
    /// open of the same index.
    pub(super) fn snapshot_keyspace(&self) -> String {
        format!("{}{}", SNAPSHOT_KEYSPACE_PREFIX, self.descriptor.id)
    }

    /// Resolve `self.field_path` to its interned-key form.
    fn ipath(&self) -> SmallVec<[InternerKey; 4]> {
        self.field_path
            .iter()
            .map(|&id| InternerKey::new(id))
            .collect()
    }

    /// Extract the embedding vector from `rec` via `any_seq_elem`.
    ///
    /// Returns `Some(Vec<f32>)` iff the field at `ipath` is a sequence whose
    /// elements are ALL numeric (F64/Int); `None` otherwise. Widening vs the
    /// legacy walk: `any_seq_elem` accepts List OR Set (vectors are always
    /// List in practice — harmless) and silently skips non-scalar elements
    /// (never occurs with well-formed vector data; matches the plan's
    /// documented behaviour).
    fn extract_vec(&self, rec: &dyn RecordRef) -> Option<Vec<f32>> {
        let ipath = self.ipath();
        let mut v: Vec<f32> = Vec::with_capacity(4);
        let mut bad = false;
        let is_seq = rec.any_seq_elem(&ipath, &mut |sr| {
            match sr {
                ScalarRef::F64(f) => v.push(f as f32),
                ScalarRef::Int(n) => v.push(n as f32),
                _ => {
                    bad = true;
                    return true; // short-circuit any_seq_elem
                }
            }
            false // keep going
        });
        if is_seq.is_some() && !bad {
            Some(v)
        } else {
            None
        }
    }
}

/// (the `ve` error-mapping helper lives in [`super::maintenance`].)

#[async_trait]
impl IndexBackend for VectorBackend {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn descriptor(&self) -> &IndexDescriptor {
        &self.descriptor
    }

    fn persisted_descriptor(&self) -> IndexDescriptor {
        let mut descriptor = self.descriptor.clone();
        let adapter = self.adapter.load_full();
        if let (shamir_index_core::kind::IndexKind::Vector(config), Some(hnsw)) =
            (&mut descriptor.kind, adapter.adapter.as_hnsw_adapter())
        {
            config.quantization = hnsw.quantization_mode();
        }
        descriptor
    }

    // HNSW mutation goes through the adapter. The non-tx variants
    // (`plan_insert` / `plan_update` / `plan_delete`) commit to the
    // live HNSW graph immediately. The tx-aware overrides below
    // (`plan_insert_tx` / `plan_update_tx` / `plan_delete_tx`) do NOT
    // touch the live graph when `tx_id == Some`: the executor instead
    // extracts the vector via [`staged_vector`] and buffers it in
    // `TxContext::staged_vectors`, so a rolled-back tx leaves no ghost
    // vectors on the live graph (HIGH-6).
    //
    // HIGH-6 (resolved, fully RAII): staged vectors live inside the
    // `TxContext` (tx-local state), not on the adapter. The commit
    // pipeline (`commit::commit_tx` Phase 5d) calls
    // `apply_staged_vectors` under the commit lock to promote the tx's
    // staged vectors into the live graph. Abort needs no counterpart —
    // a dropped tx discards `staged_vectors` by RAII. Non-tx queries
    // still see only the committed graph (see `HnswAdapter::search`).

    async fn plan_insert(
        &self,
        rid: RecordId,
        rec: &(dyn RecordRef + Sync + '_),
    ) -> Result<Vec<IndexWriteOp>, IndexError> {
        if let Some(v) = self.extract_vec(rec) {
            // RV-INDEX-9F62C8DF: one lease covers primary + mirror; the
            // primary is authoritative (its error returns), the mirror never
            // fails the user's write — a mirror error fails the generation.
            let mut lease = self.enter_write().await;
            let target = lease.target().cloned();
            if let Some(t) = &target {
                if !lease.claim_rid(rid) {
                    t.fail("overlapping mutation to the same RID inside one generation".into());
                }
            }
            let res = lease.slot().adapter.upsert(rid, &v).await;
            #[cfg(test)]
            self.fire_after_primary_hook().await;
            // F3: a primary error `?`-returns with the lease UNFINISHED —
            // Drop then invalidates the armed generation (the primary
            // mutation may have partially applied) BEFORE releasing
            // claims/count, and the mirror is skipped. On an idle slot (no
            // target) Drop just releases.
            res.map_err(ve)?;
            if let Some(t) = &target {
                t.mirror_upsert(rid, &v).await;
            }
            // Single terminal point: mark finished so Drop releases without
            // invalidating the (successfully mirrored) generation.
            lease.finish();
        }
        Ok(Vec::new())
    }

    async fn plan_update(
        &self,
        rid: RecordId,
        _old: &(dyn RecordRef + Sync + '_),
        new: &(dyn RecordRef + Sync + '_),
    ) -> Result<Vec<IndexWriteOp>, IndexError> {
        if let Some(v) = self.extract_vec(new) {
            let mut lease = self.enter_write().await;
            let target = lease.target().cloned();
            if let Some(t) = &target {
                if !lease.claim_rid(rid) {
                    t.fail("overlapping mutation to the same RID inside one generation".into());
                }
            }
            let res = lease.slot().adapter.upsert(rid, &v).await;
            // F3: unfinished-Drop invalidation on the primary-error path —
            // see `plan_insert`.
            res.map_err(ve)?;
            if let Some(t) = &target {
                t.mirror_upsert(rid, &v).await;
            }
            lease.finish();
        } else {
            let mut lease = self.enter_write().await;
            let target = lease.target().cloned();
            if let Some(t) = &target {
                if !lease.claim_rid(rid) {
                    t.fail("overlapping mutation to the same RID inside one generation".into());
                }
                // ORDER (ghost-race fix): record the rid in
                // `compaction_deleted_rids` BEFORE the mirror delete, so a
                // concurrent `backfill_if_absent` cannot re-introduce a
                // permanent ghost (full rationale in the V4.2 history).
                if let Some(hnsw) = t.adapter.as_hnsw_adapter() {
                    if let Some(ref del_rids) = hnsw.state.compaction_deleted_rids {
                        // `insert_sync`, NOT `insert_async` — the map is
                        // probed via sync accessors on the same worker
                        // threads (#589 class deadlock).
                        let _ = del_rids.insert_sync(rid, ());
                    }
                }
            }
            let res = lease.slot().adapter.delete(rid).await;
            // F3: unfinished-Drop invalidation on the primary-error path —
            // see `plan_insert`.
            res.map_err(ve)?;
            if let Some(t) = &target {
                t.mirror_delete(rid).await;
            }
            lease.finish();
        }
        Ok(Vec::new())
    }

    async fn plan_delete(
        &self,
        rid: RecordId,
        _rec: &(dyn RecordRef + Sync + '_),
    ) -> Result<Vec<IndexWriteOp>, IndexError> {
        let mut lease = self.enter_write().await;
        let target = lease.target().cloned();
        if let Some(t) = &target {
            if !lease.claim_rid(rid) {
                t.fail("overlapping mutation to the same RID inside one generation".into());
            }
            // ORDER (ghost-race fix): record `compaction_deleted_rids` BEFORE
            // the mirror delete — see `plan_update` (insert_sync: #589).
            if let Some(hnsw) = t.adapter.as_hnsw_adapter() {
                if let Some(ref del_rids) = hnsw.state.compaction_deleted_rids {
                    let _ = del_rids.insert_sync(rid, ());
                }
            }
        }
        let res = lease.slot().adapter.delete(rid).await;
        // F3: unfinished-Drop invalidation on the primary-error path — see
        // `plan_insert`.
        res.map_err(ve)?;
        if let Some(t) = &target {
            t.mirror_delete(rid).await;
        }
        lease.finish();
        Ok(Vec::new())
    }

    /// tx-aware insert planning (HIGH-6).
    ///
    /// `tx_id == Some` → no-op here: the live HNSW graph is left
    /// untouched. The executor stages the vector itself via
    /// [`staged_vector`] → `TxContext::stage_vector`, so a dropped
    /// (rolled-back) tx leaves no trace (RAII).
    ///
    /// `tx_id == None` → forwards to [`plan_insert`] (immediate
    /// commit to the live graph).
    async fn plan_insert_tx(
        &self,
        rid: RecordId,
        rec: &(dyn RecordRef + Sync + '_),
        tx_id: Option<shamir_tx::TxId>,
    ) -> Result<Vec<IndexWriteOp>, IndexError> {
        if tx_id.is_none() {
            return self.plan_insert(rid, rec).await;
        }
        Ok(Vec::new())
    }

    /// tx-aware update planning (HIGH-6). See [`plan_insert_tx`].
    async fn plan_update_tx(
        &self,
        rid: RecordId,
        old: &(dyn RecordRef + Sync + '_),
        new: &(dyn RecordRef + Sync + '_),
        tx_id: Option<shamir_tx::TxId>,
    ) -> Result<Vec<IndexWriteOp>, IndexError> {
        if tx_id.is_none() {
            return self.plan_update(rid, old, new).await;
        }
        Ok(Vec::new())
    }

    /// tx-aware delete planning (HIGH-6, gap#1). See [`plan_insert_tx`].
    ///
    /// `tx_id == Some` → no-op here: the live HNSW graph is left
    /// untouched. The executor stages the rid-to-delete tx-locally via
    /// [`TxContext::stage_vector_delete`] (the delete mirror of the insert
    /// staging wired in the table manager's delete path), so a dropped
    /// (rolled-back) tx leaves no ghost on the live graph (RAII). At
    /// commit (Phase 5d) the staged delete is promoted into the live
    /// graph (`adapter.delete`) AND the durable delta log
    /// (`DeltaOp::Delete`).
    ///
    /// `tx_id == None` → forwards to [`plan_delete`] (immediate commit to
    /// the live graph).
    async fn plan_delete_tx(
        &self,
        rid: RecordId,
        rec: &(dyn RecordRef + Sync + '_),
        tx_id: Option<shamir_tx::TxId>,
    ) -> Result<Vec<IndexWriteOp>, IndexError> {
        if tx_id.is_none() {
            return self.plan_delete(rid, rec).await;
        }
        Ok(Vec::new())
    }

    /// HIGH-6: hand the executor this record's embedding so it can stage
    /// it tx-locally (`TxContext::stage_vector`). `None` when the record
    /// carries no vector at this backend's field path.
    async fn staged_vector(
        &self,
        _rid: RecordId,
        rec: &(dyn RecordRef + Sync + '_),
    ) -> Option<Vec<f32>> {
        let vec = self.extract_vec(rec)?;
        let expected = self.adapter.load().adapter.dim();
        if vec.len() != expected as usize {
            return None;
        }
        Some(vec)
    }

    async fn lookup(&self, query: IndexQuery) -> Result<IndexResult, IndexError> {
        match query {
            IndexQuery::Vector { vec, k, opts } => {
                let results = self
                    .adapter
                    .load_full()
                    .adapter
                    .search(&vec, k, opts, None)
                    .await
                    .map_err(ve)?;
                Ok(IndexResult::Ranked(results))
            }
            _ => Err(IndexError::Backend(
                "VectorBackend only supports Vector queries".into(),
            )),
        }
    }

    async fn lookup_tx(
        &self,
        _table_token: u64,
        query: IndexQuery,
        _tx: Option<&shamir_tx::TxContext>,
        staged_vectors: Option<&[(RecordId, Vec<f32>)]>,
    ) -> Result<IndexResult, IndexError> {
        match query {
            IndexQuery::Vector { vec, k, opts } => {
                let results = self
                    .adapter
                    .load_full()
                    .adapter
                    .search(&vec, k, opts, staged_vectors)
                    .await
                    .map_err(ve)?;
                Ok(IndexResult::Ranked(results))
            }
            _ => Err(IndexError::Backend(
                "VectorBackend only supports Vector queries".into(),
            )),
        }
    }

    /// HIGH-6: promote the tx's staged vectors for this table into the
    /// live HNSW graph at commit. Delegates to the adapter.
    async fn apply_staged_vectors(&self, vecs: &[(RecordId, Vec<f32>)]) -> Result<(), IndexError> {
        // One lease for the whole batch; claims span the primary + mirror.
        let mut lease = self.enter_write().await;
        let target = lease.target().cloned();
        if let Some(t) = &target {
            if !lease.claim_rids(vecs.iter().map(|(rid, _)| *rid)) {
                t.fail("overlapping mutation to the same RID inside one generation".into());
            }
        }
        let res = lease.slot().adapter.apply_committed_vectors(vecs).await;
        // F3: unfinished-Drop invalidation on the primary-error path — see
        // `plan_insert`. The mirror is skipped when the primary failed.
        res.map_err(ve)?;
        if let Some(t) = &target {
            t.mirror_committed(vecs).await;
        }
        lease.finish();
        Ok(())
    }

    /// HIGH-6 / gap#1: promote the tx's staged vector deletes for this
    /// table into the live HNSW graph at commit — tombstone each rid via
    /// the adapter's `delete`. Mirrors [`apply_staged_vectors`] for the
    /// delete side. The durable `DeltaOp::Delete` is written separately by
    /// [`append_vector_delta`] (the delta chunk is the durable echo of an
    /// already-applied in-memory mutation — see `DeltaOp::Delete` contract
    /// in `snapshot.rs`), so this method is graph-side ONLY.
    ///
    /// V4.2 (#408): double-write to compaction target with the SAME
    /// ghost-race guard ordering as `plan_delete` — record
    /// `compaction_deleted_rids` BEFORE `target.adapter.delete(rid)` so a
    /// concurrent `backfill_if_absent` cannot re-introduce a permanent
    /// ghost (see the rationale in `plan_update`).
    async fn apply_staged_vector_deletes(&self, deleted: &[RecordId]) -> Result<(), IndexError> {
        // One lease for the whole loop (no nested `enter_write`).
        let mut lease = self.enter_write().await;
        let target = lease.target().cloned();
        for &rid in deleted {
            if let Some(t) = &target {
                if !lease.claim_rid(rid) {
                    t.fail("overlapping mutation to the same RID inside one generation".into());
                }
                // Ghost-race guard ordering + insert_sync: see `plan_update`.
                if let Some(hnsw) = t.adapter.as_hnsw_adapter() {
                    if let Some(ref del_rids) = hnsw.state.compaction_deleted_rids {
                        let _ = del_rids.insert_sync(rid, ());
                    }
                }
            }
            let res = lease.slot().adapter.delete(rid).await;
            // F3: unfinished-Drop invalidation on the primary-error path —
            // see `plan_insert` (a mid-loop failure leaves PART of the batch
            // applied, which is exactly why the generation is invalidated).
            res.map_err(ve)?;
            if let Some(t) = &target {
                t.mirror_delete(rid).await;
            }
        }
        lease.finish();
        Ok(())
    }

    async fn rebuild(&self, source: Arc<dyn Store>) -> Result<(), IndexError> {
        // Lifecycle-excluded mutation path (F8): `rebuild` runs at startup /
        // snapshot-restore fallback, BEFORE any traffic reaches this
        // backend — no writer admission, no gate participation.
        // Count this full-scan rebuild for the V2.2 instrumentation.
        // `restore_on_open` skips `rebuild` entirely on a successful
        // snapshot load, so this counter ONLY moves when we genuinely
        // fell back to a scan.
        self.full_rebuild_count.fetch_add(1, Ordering::Relaxed);

        // Batch size = 1000: large enough that a single HnswAdapter
        // `upsert_batch` call drives a rayon `parallel_insert` over ~1k
        // vectors (well past the ~`1000 * num_threads` sweet spot the
        // hnsw_rs docstring recommends for parallel_insert efficiency),
        // yet bounded so a single batch's owned-Vec materialisation stays
        // in the low-MB range (1000 × 4-byte × dim). The store stream
        // already paginates at this granularity, so we hand each page
        // straight to the adapter as one batch.
        let batch_size = 1000usize;
        let mut stream = source.iter_stream(batch_size);
        while let Some(batch_res) = stream.next().await {
            let batch = batch_res.map_err(IndexError::from)?;
            let mut items: Vec<(RecordId, Vec<f32>)> = Vec::new();
            for (key_bytes, val_bytes) in batch {
                let arr: [u8; 16] = key_bytes
                    .as_ref()
                    .try_into()
                    .map_err(|_| IndexError::Backend("invalid key length".into()))?;
                let rid = RecordId(arr);
                let rec = InnerValue::from_bytes(&val_bytes)
                    .map_err(|e| IndexError::Backend(e.to_string()))?;
                if let Some(v) = self.extract_vec(&rec as &dyn RecordRef) {
                    items.push((rid, v));
                }
            }
            // One batched upsert per store page: HnswAdapter overrides
            // `upsert_batch` → single rayon parallel_insert over the page
            // (was N serial inserts in 64-wide inner chunks). The default
            // `upsert_batch` (BruteForceAdapter etc.) falls back to a
            // per-row loop, so the call is correct for every adapter.
            if !items.is_empty() {
                self.adapter
                    .load_full()
                    .adapter
                    .upsert_batch(&items)
                    .await
                    .map_err(ve)?;
            }
        }
        Ok(())
    }

    /// V2.2 / #401 — startup restore with snapshot-first, rebuild-fallback.
    /// V2.3 / #402 — the snapshot-hit branch ALSO replays the delta-log.
    ///
    /// Three branches:
    /// 1. **`Ok(adapter)`** — the snapshot loaded cleanly. We swap the
    ///    freshly-built `HnswAdapter` into the live `ArcSwap`, replacing
    ///    the empty placeholder adapter that `build_index2_backend_*`
    ///    constructed. NO data-store scan runs, so
    ///    [`rebuild_count`] stays at 0. This is the O(load) fast path.
    ///    **V2.3:** before the swap, we replay every delta chunk with index
    ///    >= `manifest.delta_applied_upto` against the FRESHLY-loaded
    ///    adapter (the boundary is EXCLUSIVE: it counts the absorbed
    ///    chunks `0..delta_applied_upto`), so the graph reflects every
    ///    mutation committed since
    ///    the snapshot was taken. **P0 RV-INDEX-4651AA00:** the replay
    ///    runs BEFORE the adapter is published and a replay failure is
    ///    FAIL-CLOSED — publishing an incomplete graph would let the next
    ///    generation flip permanently prune the unreplayed chunks.
    ///    **P0 RV-INDEX-2F3CBCA3:** the in-memory HWM (`next_delta_idx`)
    ///    is seeded to `max(replayed boundary, highest surviving chunk) + 1`
    ///    so the next `append_delta` never lands at or below the persisted
    ///    absorption watermark; HWM-scan errors propagate (never default
    ///    to 0).
    /// 2. **`Err(NotFound)`** — no snapshot exists for this keyspace (a
    ///    fresh index, or the first open after the feature shipped). Fall
    ///    back to the legacy full-scan [`rebuild`] against the data store.
    ///    The rebuild counter increments to 1.
    /// 3. **`Err(Corrupt | VersionMismatch | Io | Backend | Serde)`** — the
    ///    snapshot exists but is unusable (bit-flip, format bump, foreign
    ///    `dim`/`metric`). We `log::warn!` the cause and fall back to a
    ///    full-scan [`rebuild`]; the user's data in the data store is
    ///    intact, so the scan rebuilds a correct graph. We do NOT abort
    ///    the open: a missing/stale snapshot is recoverable, and aborting
    ///    would make the whole table unopenable for a transient metadata
    ///    issue. The rebuild counter increments to 1.
    ///
    /// The snapshot keyspace is [`snapshot_keyspace`] — derived from the
    /// persisted `IndexDescriptor::id`, so it is stable across opens of
    /// the same index and immune to renames. `info_store` is where V2.1's
    /// `dump_snapshot` wrote the chunks/sidecar/manifest; it is the SAME
    /// store the engine hands every index2 backend at construction time
    /// (`build_index2_backend_with_resolver`'s `info_store` arg).
    async fn restore_on_open(
        &self,
        info_store: Arc<dyn Store>,
        data_store: Arc<dyn Store>,
    ) -> Result<(), IndexError> {
        let keyspace = self.snapshot_keyspace();
        let _ = self.info_store.set(Arc::downgrade(&info_store));
        // so we know `delta_applied_upto` for the
        // replay even if `load_snapshot` later swaps the adapter in. A
        // missing manifest is the `NotFound` branch — same as load_snapshot.
        let manifest_opt: Option<SnapshotManifest> = match snapshot::read_manifest(
            &info_store,
            &keyspace,
        )
        .await
        {
            Ok(m) => Some(m),
            Err(SnapshotError::NotFound) => None,
            Err(e) => {
                // A corrupt manifest is treated the same as a corrupt
                // snapshot below: warn + fall back to a full rebuild.
                log::warn!(
                        "vector manifest read failed for index {} (keyspace {}): {} — falling back to full rebuild",
                        self.descriptor.name,
                        keyspace,
                        e
                    );
                None
            }
        };
        let intent = match &self.descriptor.kind {
            shamir_index_core::kind::IndexKind::Vector(config) => config.quantization,
            _ => None,
        };
        match snapshot::load_snapshot_with_intent(&info_store, &keyspace, intent).await {
            Ok(loaded) => {
                // O(load) fast path. P0 RV-INDEX-4651AA00: replay runs on
                // the FRESH adapter BEFORE it is stored live — a failure
                // below must never leave a half-replayed graph published.
                let adapter_arc: Arc<dyn VectorAdapter> = Arc::new(loaded);

                // V2.3 + P0 RV-INDEX-4651AA00: replay delta chunks past the
                // snapshot's base (`delta_applied_upto` from the manifest we
                // just read — load_snapshot validated it but does not return
                // it). A replay failure FAILS THE OPEN: the omitted
                // post-snapshot mutations would otherwise be permanently
                // pruned by the next generation flip.
                let replayed_hwm: Option<u64> = match (manifest_opt, adapter_arc.as_hnsw_adapter()) {
                    (Some(manifest), Some(hnsw)) => Some(
                        snapshot::replay_delta(
                            &info_store,
                            &keyspace,
                            manifest.delta_applied_upto,
                            hnsw,
                        )
                        .await
                        .map_err(|e| match e {
                            SnapshotError::Db(error) => IndexError::Db(error),
                            other => IndexError::Backend(format!(
                                "vector delta replay failed for index {} (keyspace {}): {} — failing the open closed; omitting these mutations would let the next snapshot prune their delta chunks",
                                self.descriptor.name, keyspace, other
                            )),
                        })?,
                    ),
                    // RV-INDEX-4651AA00/2F3CBCA3: a snapshot loaded although
                    // the up-front manifest read failed would be published
                    // without its absorption boundary — replay skipped and
                    // the allocator seeded from surviving chunks alone.
                    // Refuse instead of publishing it.
                    (None, Some(_)) => {
                        return Err(IndexError::Backend(format!(
                            "vector snapshot loaded for index {} (keyspace {}) but its manifest could not be read first; refusing to publish a graph without its replay boundary",
                            self.descriptor.name, keyspace
                        )));
                    }
                    // Non-HNSW adapters have no snapshot and no delta log
                    // (see the trigger guard) — nothing to replay.
                    (_, None) => None,
                };

                // P0 RV-INDEX-2F3CBCA3: seed the in-memory delta HWM from
                // BOTH the replayed absorption boundary and the highest
                // surviving chunk in the store, so the next `append_delta`
                // cannot land at or below the persisted watermark and
                // overwrite a surviving unabsorbed chunk. A scan error is
                // PROPAGATED — defaulting it to 0 seeded `next_delta_idx = 1`
                // and let appends clobber surviving chunks.
                // Publication happens only AFTER replay AND this seed
                // succeeded: a failed restore must never publish the
                // recovered graph.
                let stored_hwm = snapshot::highest_delta_index(&info_store, &keyspace)
                    .await
                    .map_err(|e| match e {
                        SnapshotError::Db(error) => IndexError::Db(error),
                        other => IndexError::Backend(format!(
                            "delta HWM scan failed for index {} (keyspace {}): {} — \
                             refusing to seed next_delta_idx below the surviving chunks",
                            self.descriptor.name, keyspace, other
                        )),
                    })?;
                // Conservative fold: gaps between the replayed boundary and
                // surviving chunks only ever raise the seed (safe, never
                // lossy). `highest_delta_index` returns 0 when no chunks
                // exist (fresh snapshot).
                let hwm = replayed_hwm.map_or(stored_hwm, |r| r.max(stored_hwm));
                self.next_delta_idx
                    .store(hwm.saturating_add(1), Ordering::Release);
                // P0 delta-prune: every surviving chunk was replayed into the
                // freshly-loaded graph, so the applied prefix starts above
                // all of them.
                self.applied_prefix
                    .store(hwm.saturating_add(1), Ordering::Release);
                // P0 index promote order: the turn starts above every
                // replayed chunk, right beside the applied prefix seed.
                self.promote_turn
                    .store(hwm.saturating_add(1), Ordering::Release);

                // Publish the fully-replayed adapter + seeded HWM.
                // `ArcSwap::store` is wait-free; a concurrent query grabbing
                // `load()` either sees the old empty adapter or the new one
                // — never a torn state.
                self.adapter
                    .store(Arc::new(AdapterSlot::live(Arc::clone(&adapter_arc))));

                Ok(())
            }
            Err(SnapshotError::NotFound) => {
                // No snapshot for this index — fresh index or first open.
                // Fall through to a full rebuild scan.
                self.seed_delta_idx_for_fallback(
                    &self.descriptor.name,
                    &info_store,
                    &keyspace,
                    manifest_opt.as_ref(),
                )
                .await?;
                self.rebuild(data_store).await
            }
            Err(e) => {
                // Snapshot present but unusable. Warn loudly (this is the
                // signal an operator uses to investigate a failing snapshot)
                // and recover via a full rebuild scan. We do NOT propagate
                // the snapshot error: the data store is the source of truth
                // and a rebuild will reconstruct a correct graph.
                log::warn!(
                    "vector snapshot load failed for index {} (keyspace {}): {} — falling back to full rebuild",
                    self.descriptor.name,
                    keyspace,
                    e
                );
                self.seed_delta_idx_for_fallback(
                    &self.descriptor.name,
                    &info_store,
                    &keyspace,
                    manifest_opt.as_ref(),
                )
                .await?;
                self.rebuild(data_store).await
            }
        }
    }

    fn rebuild_count(&self) -> u64 {
        self.full_rebuild_count.load(Ordering::Relaxed)
    }

    async fn drop_all(&self) -> Result<(), IndexError> {
        let store = self
            .info_store
            .get()
            .and_then(std::sync::Weak::upgrade)
            .ok_or_else(|| {
                IndexError::Backend("vector info store is not configured or no longer alive".into())
            })?;
        // Trailing-dot delimiter: `__vec_snap__9.` cannot match index 90/91.
        let prefix = format!("{}.", self.snapshot_keyspace());
        let mut stream = store.scan_prefix_stream(prefix.clone().into(), 1024);
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result.map_err(IndexError::from)?;
            for (key, _) in batch {
                store.remove(key).await.map_err(IndexError::from)?;
            }
        }
        Ok(())
    }

    /// V2.3 (#402) — append a delta-log chunk capturing the vectors just
    /// promoted into the live graph, then bump the mutation counter.
    ///
    /// See [`IndexBackend::append_vector_delta`] for the contract. The chunk
    /// is written at the monotonic `next_delta_idx` (one `Store::set`); the
    /// HWM is bumped AFTER the write lands so a crash leaves a duplicate-index
    /// chunk on the next attempt (`Store::set` overwrites — last-writer-wins).
    async fn append_vector_delta(
        &self,
        info_store: &Arc<dyn Store>,
        vecs: &[(RecordId, Vec<f32>)],
        deleted: &[RecordId],
    ) -> Result<Option<u64>, IndexError> {
        // Build the chunk; an empty promote writes nothing (dense stream).
        if vecs.is_empty() && deleted.is_empty() {
            return Ok(None);
        }
        let mut ops: Vec<snapshot::DeltaOp> = Vec::with_capacity(vecs.len() + deleted.len());
        for (rid, vec) in vecs {
            ops.push(snapshot::DeltaOp::Upsert(*rid, vec.clone()));
        }
        for rid in deleted {
            ops.push(snapshot::DeltaOp::Delete(*rid));
        }
        let keyspace = self.snapshot_keyspace();
        let idx = self.next_delta_idx.fetch_add(1, Ordering::AcqRel);
        // P0 index promote order (RV finding 1): armed IMMEDIATELY after the
        // reservation, BEFORE the only await. Dropped mid-await, the guard
        // releases the TURN on the unwind (the chunk is NOT marked applied —
        // it stays replayable, prefix below it). Disarmed at both explicit
        // exits that handle the release themselves.
        let mut reservation = super::maintenance::DeltaReservationGuard::arm(self, idx);
        if let Err(e) = snapshot::append_delta(info_store, &keyspace, idx, &ops).await {
            // Nothing was persisted at `idx` (already reserved by the
            // `fetch_add`): mark it applied so the failed write does not
            // stall the applied prefix, and release the promote turn so
            // later chunks are never stuck behind it.
            self.mark_delta_applied(idx);
            self.release_promote_turn(idx);
            reservation.disarm();
            return Err(match e {
                SnapshotError::Db(error) => IndexError::Db(error),
                other => IndexError::Backend(format!("delta append: {other}")),
            });
        } // P0 index promote order: the engine's Drop-guard ticket (minted right
          // below, no await between) owns the turn from here on.
        reservation.disarm();
        // Bump the mutation counter by the number of ops in this chunk. The
        // background snapshot trigger reads this counter to decide when to
        // flip the generation.
        self.delta_count
            .fetch_add(ops.len() as u64, Ordering::AcqRel);
        Ok(Some(idx))
    }

    /// P0 index promote order — delegated to the turnstile in [`super::maintenance`].
    async fn wait_promote_turn(&self, idx: u64) {
        self.turn_wait(idx).await;
    }

    /// P0 index promote order — delegated to the turnstile in [`super::maintenance`].
    fn release_promote_turn(&self, idx: u64) {
        self.turn_release(idx);
    }

    /// P0 index promote order — delegated to the turnstile in [`super::maintenance`].
    async fn promote_graph_batch(
        &self,
        vecs: &[(RecordId, Vec<f32>)],
        deleted: &[RecordId],
    ) -> Result<(), IndexError> {
        self.promote_graph_in_turn(vecs, deleted).await
    }

    /// P0 delta-prune (brief 164) — see [`IndexBackend::mark_delta_applied`].
    fn mark_delta_applied(&self, idx: u64) {
        self.ack_delta_applied(idx);
    }

    /// V2.3 (#402) — check the delta counter against the threshold and, if
    /// crossed, spawn a single-flight background snapshot task.
    ///
    /// Delegates to the extracted implementation in
    /// [`super::maintenance`] (same body, sibling file).
    fn trigger_snapshot_check(&self, info_store: &Arc<dyn Store>) {
        // Path form: resolves to the inherent impl in
        // [`super::maintenance`], not back into this trait method.
        Self::trigger_snapshot_check_impl(self, info_store);
    }

    fn trigger_compaction_check(&self, info_store: &Arc<dyn Store>) {
        VectorBackend::trigger_compaction_check(self, info_store);
    }
}

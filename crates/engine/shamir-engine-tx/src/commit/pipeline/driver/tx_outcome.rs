use shamir_query_types::batch::VisibilityState;

/// Whether the committed transaction's projections (data → main,
/// counter, index → info, HNSW graph) were fully materialized inline
/// on the commit path.
///
/// The WAL entry written in Phase 4 IS the commit; main/info/HNSW are
/// eager-applied projections of it. On the normal path every projection
/// lands inline and the WAL marker is removed (Phase 7) →
/// [`Complete`](MaterializationState::Complete). If a projection
/// sub-phase fails *after* the commit point, the tx is still COMMITTED:
/// the WAL marker is left inflight so recovery re-applies the entry on
/// the next open, and this is reported as
/// [`Deferred`](MaterializationState::Deferred). A `Deferred` outcome is
/// NOT an abort — the version is published and the data WILL appear
/// (idempotently) via recovery.
///
/// CONSISTENCY HONESTY (audit MED, by-design I.3 trade-off — be precise,
/// not reassuring): a `Deferred` outcome is *restart-bounded eventually
/// consistent*, NOT immediately consistent. Phase 6 (`publish_committed`)
/// ALWAYS runs, so the MVCC version is published the instant the WAL entry
/// is durable — but the projections that back that version (per-table data
/// → main, per-table index → info) may be only PARTIALLY applied across the
/// tables/indexes the tx touched. A single multi-table tx that defers can
/// leave table A's new rows materialized while table B's failed: a
/// concurrent reader opening a snapshot AFTER the publish sees A's new value
/// and B's OLD value AT THE SAME committed version — a genuine cross-table /
/// data-vs-index inconsistency. It is NOT reconciled online (there is no
/// background reconciler); it persists until the next `recover_v2_inflight`
/// (`RepoInstance::recover_v2_inflight`) on repo open replays the one
/// inflight WAL entry — which carries ALL the tx's ops, every table — and
/// converges every projection. What is still guaranteed even while
/// deferred: a single-key read via `MvccStore::get_at` is never byte-torn
/// (each key is whole-value last-write-wins), and the version floor is
/// monotonic. What lags: cross-table atomicity and data-vs-index agreement,
/// until recovery runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaterializationState {
    /// All projections applied inline; WAL marker removed (Phase 7 ran).
    Complete,
    /// At least one projection sub-phase failed after the commit point.
    /// WAL marker left inflight; recovery is the materialization
    /// guarantor on the next open.
    ///
    /// Multi-table caveat (restart-bounded eventual consistency): when a
    /// tx spanned several tables/indexes, the deferral may be PARTIAL —
    /// some tables materialized inline, others not. The published version
    /// is therefore cross-table-inconsistent until the next
    /// `recover_v2_inflight` replays the inflight WAL entry and reconciles
    /// every table. Single-key reads stay byte-intact throughout; only
    /// cross-table / data-vs-index consistency lags. See the type-level
    /// doc above for the full statement.
    Deferred,
}

#[derive(Debug)]
pub struct TxOutcome {
    pub tx_id: u64,
    pub snapshot_version: u64,
    pub commit_version: u64,
    /// Independent contiguous-prefix acknowledgement. Pending is still committed;
    /// reconcile this repository-scoped position before opening a fresh snapshot.
    /// Visibility does not upgrade materialization or WAL durability.
    pub visibility: VisibilityState,
    /// Whether projections materialized inline (`Complete`) or were
    /// deferred to recovery (`Deferred`). Either way the tx is
    /// COMMITTED — see [`MaterializationState`].
    ///
    /// **Async-index mode caveat.** When the tx opted into
    /// [`shamir_tx::CommitVisibility::AsyncIndex`], this field reflects the
    /// state at *ack time*: it can only be `Complete` (sync-prefix phases
    /// landed) since the deferral-bearing phases (5c index, 6.5 markers,
    /// 7 WAL cleanup) are still in flight on the background task. The
    /// truly-final materialization state (the moral equivalent of the sync
    /// `Complete` / `Deferred` outcome) is observable via
    /// [`background`](TxOutcome::background) — awaiting that handle yields
    /// the same `MaterializationState` that sync mode would have returned
    /// on this commit's pipeline tail.
    pub materialization: MaterializationState,
    /// Async-index mode: handle for the background materialization tail.
    /// `None` in sync mode (everything ran inline).
    ///
    /// Tests / callers that need read-your-own-writes on a SECONDARY INDEX
    /// after an async commit can `await` this handle to block until 5c+ has
    /// landed. Production callers normally do NOT await this — the whole
    /// point of async mode is to return without waiting. A failed tail
    /// (panic / abort) does NOT corrupt anything: the inflight WAL marker
    /// is the recovery guarantor, exactly as in the `Deferred` path.
    #[doc(hidden)]
    pub background: Option<BackgroundCommitHandle>,
}

impl TxOutcome {
    /// Convenience: `true` when all projections materialized inline.
    /// `false` means materialization was deferred to recovery (the tx is
    /// still committed).
    ///
    /// In async-index mode this reflects the SYNC-PREFIX result at ack
    /// time and is therefore always `true`. To observe the post-tail state,
    /// `await` [`background`](TxOutcome::background).
    pub fn materialized(&self) -> bool {
        self.materialization == MaterializationState::Complete
    }

    /// Async-index mode: take the background-tail handle (leaves `None`
    /// behind so subsequent calls don't double-await). Returns `None` in
    /// sync mode and on a deferred sync outcome.
    pub fn take_background(&mut self) -> Option<BackgroundCommitHandle> {
        self.background.take()
    }
}

/// Awaitable handle for the async-index materialization tail.
///
/// Returned in [`TxOutcome::background`] when the tx opted into
/// [`shamir_tx::CommitVisibility::AsyncIndex`]. Awaiting it blocks until
/// Phases 5c (index) + 6.5 (markers) + 7 (WAL cleanup) + 5d (HNSW promote)
/// have all finished, and yields the [`MaterializationState`] that would
/// have been returned by an equivalent sync commit. A failed background
/// task (panic) resolves to `MaterializationState::Deferred` — the inflight
/// WAL marker is left for recovery, exactly as in the sync deferral path.
#[derive(Debug)]
pub struct BackgroundCommitHandle {
    pub(crate) join: tokio::task::JoinHandle<MaterializationState>,
}

impl BackgroundCommitHandle {
    /// Wait for the background tail to complete. A panicked task is
    /// reported as `Deferred` (recovery is the guarantor).
    pub async fn join(self) -> MaterializationState {
        match self.join.await {
            Ok(state) => state,
            Err(_) => MaterializationState::Deferred,
        }
    }
}

use super::adapter::HnswAdapter;
use super::config::{HnswConfig, FIT_THRESHOLD};
use crate::adapter::VectorError;
use crate::quantized_dist::ShamirDistU8;
use crate::sq8::Sq8Quantizer;
use hnsw_rs::hnsw::Hnsw;
use scc::hash_map::Entry::{Occupied, Vacant};
use shamir_index_core::kind::{VectorMetric, VectorQuantization};
use shamir_types::types::common::THasher;
use shamir_types::types::record_id::RecordId;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

struct FitFlightGuard(Arc<AtomicBool>);

impl Drop for FitFlightGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

impl HnswAdapter {
    /// V4.2 (#408) — Collect all live (non-tombstoned) (rid, vector) pairs.
    /// O(N) scan — called once per compaction, NOT on hot path.
    ///
    /// V5.2 (#411) — post-fit: returns DEQUANTIZED f32 vectors (the codes
    /// in `vectors_u8` are dequantized through the frozen quantizer). The
    /// compaction rebuild target is always an UNQUANTIZED adapter at
    /// backfill time: live vectors are re-inserted as f32 into the target's
    /// fresh f32 graph, and the target re-fits its OWN quantizer on the
    /// post-compaction live set when it crosses `FIT_THRESHOLD` (#428,
    /// Variant A — see `backfill_if_absent`). Re-fitting from scratch is
    /// honest: the live distribution may have drifted from the pre-compaction
    /// fit (deletes shift it), so retraining on the surviving set is at
    /// least as accurate as copying the old `QuantMeta`.
    pub(crate) fn collect_live_vectors(&self) -> Vec<(RecordId, Vec<f32>)> {
        let mut result: Vec<(RecordId, Vec<f32>)> = Vec::new();
        // Post-fit: dequantize codes. Pre-fit/unquantized: f32 buffer.
        let quantizer = if self.is_quantized() {
            self.state.quantizer.get().cloned()
        } else {
            None
        };
        self.state.rid_to_internal.iter_sync(|rid, internal| {
            // Skip tombstoned internals
            if self.state.deleted.contains_sync(internal) {
                return true;
            }
            if let Some(q) = quantizer.as_ref() {
                if let Some(codes) = self.state.vectors_u8.read_sync(internal, |_, c| c.clone()) {
                    result.push((*rid, q.dequantize(&codes)));
                }
            } else if let Some(vec) = self.state.vectors.read_sync(internal, |_, v| v.clone()) {
                result.push((*rid, vec));
            }
            true
        });
        result
    }

    /// V4.2 (#408) — Insert (rid, vec) ONLY if rid is absent from
    /// `rid_to_internal` AND absent from `compaction_deleted_rids`.
    /// Uses `entry_sync` for atomic check-and-insert per rid (see the
    /// DEADLOCK FIX comment at the `entry_sync` call site below).
    ///
    /// #428 (VR-6) — quantization-aware routing.
    ///
    /// The compaction target may be `quantization == Some(Sq8)` (see
    /// [`Self::new_compaction_target_quantized`]). The target STARTS
    /// un-fitted: every backfill insert runs the f32 path (graph + buffer),
    /// exactly like the legacy non-quant target. After the LAST row is
    /// inserted, a deferred-fit check fires (mirroring `upsert`'s trigger):
    /// if `live_count >= FIT_THRESHOLD` and the target is a quantization
    /// adapter, [`Self::try_fit_and_rebuild`] trains a fresh SQ8 quantizer
    /// on the post-compaction live set, builds the u8 graph, and drops the
    /// f32 graph. The fit snapshot/delta/catch-up machinery in
    /// `try_fit_and_rebuild` is REUSED verbatim — no duplicate claim path.
    /// If the live set is below `FIT_THRESHOLD` (rare for a compaction that
    /// fires past `VECTOR_COMPACTION_MIN_LIVE`), the target stays on the f32
    /// path until a subsequent `upsert` crosses the threshold and fits.
    pub(crate) async fn backfill_if_absent(
        &self,
        items: &[(RecordId, Vec<f32>)],
    ) -> Result<(), VectorError> {
        for (rid, vec) in items {
            // Skip if this rid was deleted via double-write
            if let Some(ref del_rids) = self.state.compaction_deleted_rids {
                if del_rids.contains_sync(rid) {
                    continue;
                }
            }
            // Atomic check: only insert if absent
            // DEADLOCK FIX (same class as #589 / `cells` map, commit
            // `7a4abf62`, and the H1+H2 fix commit `621776bd`):
            // `entry_sync`, NOT `entry_async`. The `rid_to_internal` map
            // is scanned on EVERY compaction via the SYNCHRONOUS
            // `collect_live_vectors` → `iter_sync`, and probed via
            // `contains_sync` (`contains_rid`). `entry_async`'s wait path
            // is lock-HANDOFF: on release saa GRANTS the exclusive bucket
            // lock to the suspended waiter TASK, which then holds it while
            // sitting in tokio's run queue until next polled. A worker
            // parked in `iter_sync`/`contains_sync` on the same bucket
            // during the handoff window deadlocks. With `entry_sync` the
            // exclusive lock is only held by a RUNNING thread for a few
            // instructions. The fn stays `async` to keep call sites
            // unchanged; this call no longer suspends (the rest of the fn
            // still legitimately awaits the CPU-bound graph insert etc.).
            //
            // AMPLIFIER: this `entry_sync` guard is held across the
            // `compaction_deleted_rids.contains_sync` re-check AND the
            // `vac.insert_entry` below — once this is `entry_sync` (not
            // `entry_async`), a worker can no longer PARK while owning the
            // `rid_to_internal` bucket across an `.await`, so it cannot
            // chain a second map (e.g. `deleted`) into the deadlock.
            match self.state.rid_to_internal.entry_sync(*rid) {
                Occupied(_) => continue, // double-write already placed a fresher value
                Vacant(vac) => {
                    // Re-check deleted_rids under the entry lock to close the race
                    if let Some(ref del_rids) = self.state.compaction_deleted_rids {
                        if del_rids.contains_sync(rid) {
                            continue;
                        }
                    }
                    let internal = self.state.next_id.fetch_add(1, Ordering::Relaxed);
                    vac.insert_entry(internal);

                    // VR-9 follow-up (discovered by the final 10x stress
                    // gate, crates/shamir-vector/src/tests/lifecycle/
                    // compaction_tests/basic.rs::stress_concurrent_mutations_during_quantized_compaction) —
                    // the OLD comment here claimed "the target is ALWAYS
                    // un-fitted during this loop; the deferred-fit check
                    // below fires AFTER the loop" — WRONG when a concurrent
                    // double-write caller (a worker upserting directly onto
                    // this SAME compaction-target adapter, per the standard
                    // double-write-during-compaction protocol) crosses
                    // FIT_THRESHOLD via ITS OWN upsert-triggered deferred-fit
                    // (see the identical check in `upsert`/`upsert_batch`)
                    // WHILE this loop is between iterations. That fit call
                    // one-shot-flips `is_fitted` and drops the f32 graph
                    // (`self.state.hnsw.store(None)`) — this loop's next
                    // `self.state.hnsw.load_full()` then observed `None` and
                    // errored out, aborting the entire backfill.
                    //
                    // Fix: dispatch per item exactly like `upsert` does —
                    // if quantization already flipped active, skip the f32
                    // path entirely and publish straight into the u8 graph
                    // via `quantized_fastpath_publish` (claim-then-insert;
                    // see #433 below — this `internal` may be PRE-flip and
                    // must be accounted for in `migrated_pre_flip`, and a
                    // concurrent fit pass may already own it, so we do NOT
                    // insert blindly). Otherwise take the f32 path as
                    // before, then re-check for a fit that flipped DURING
                    // our own insert (the same self-migration pattern
                    // already used by `upsert`, Б-1/#423) so a fit racing
                    // this exact item doesn't drop the f32 graph out from
                    // under it.
                    if self.quantized_active() {
                        // #433 — claim-then-insert (mirrors `upsert`'s
                        // fast-path): a fit that completed inside the
                        // `entry_async` suspend above makes this `internal`
                        // a PRE-flip internal the catch-up loop is waiting
                        // on. The shared claim authority bumps
                        // `migrated_pre_flip` and dedups the graph node
                        // against a concurrent fit pass. A raw
                        // `vectors_u8.insert_async` here undercounts
                        // convergence and hangs the fitter.
                        self.quantized_fastpath_publish(internal, vec).await?;
                        // DEADLOCK FIX (same class as #589, commit `7a4abf62`; H3
                        // commit `dcfaf825`): `insert_sync`, NOT `insert_async`. The
                        // `rid_map` is scanned via the SYNCHRONOUS `iter_sync` in
                        // `for_each_rid_map` (snapshot serialisation) on the SAME
                        // runtime worker threads. `insert_async`'s wait path is
                        // lock-HANDOFF: saa grants the suspended waiter TASK the
                        // exclusive bucket lock, which it then holds while sitting
                        // in tokio's run queue unpolled; a worker parked in that
                        // `iter_sync` on the same bucket during the handoff window
                        // can whole-runtime deadlock. `insert_sync` holds the lock
                        // only while a thread is RUNNING. The fn stays `async`
                        // (call sites unchanged); this call no longer suspends.
                        let _ = self.state.rid_map.insert_sync(internal, *rid);
                        continue;
                    }

                    let hnsw = self.state.hnsw.load_full().ok_or_else(|| {
                        VectorError::Internal(
                            "backfill_if_absent: f32 graph absent on compaction target".into(),
                        )
                    })?;
                    let vec_owned = vec.clone();
                    tokio::task::spawn_blocking(move || {
                        hnsw.insert((&vec_owned, internal));
                    })
                    .await
                    .map_err(|e| VectorError::Internal(e.to_string()))?;

                    // DEADLOCK FIX (#589 class, commit `7a4abf62`):
                    // `insert_sync`, NOT `insert_async`. The `vectors` map
                    // is scanned via `iter_sync` (fit snapshot/delta/
                    // catch-up, `for_each_vector`, `collect_live_vectors`)
                    // and read via `read_sync` on the SAME runtime worker
                    // threads. `insert_async`'s handoff wait path risks a
                    // whole-runtime deadlock against those sync readers.
                    let _ = self.state.vectors.insert_sync(internal, vec.clone());
                    // DEADLOCK FIX (same class as #589, commit `7a4abf62`; H3
                    // commit `dcfaf825`): `insert_sync`, NOT `insert_async` —
                    // `rid_map` is scanned via `iter_sync` in `for_each_rid_map`
                    // (snapshot serialisation) on the SAME worker threads; same
                    // hazard as the sibling `rid_map.insert_sync` above in this fn.
                    let _ = self.state.rid_map.insert_sync(internal, *rid);

                    // Self-migration re-check (mirrors `upsert`, Б-1/#423):
                    // a concurrent fit may have flipped `is_fitted` true
                    // between our `quantized_active()` check above and this
                    // point. Claim this internal into vectors_u8 ourselves
                    // if the fitter's snapshot/delta scan hasn't already —
                    // the claim is the exact authority for who owns the
                    // graph node, so no double-insert is possible.
                    //
                    // @sh adversarial review (post-fix-1 stress gate) — the
                    // FIRST version of this block omitted the
                    // `!self.state.deleted.contains(&internal)` guard that
                    // `upsert` (L2142-ish) carries for exactly this reason:
                    // a concurrent `delete(r)` on the SAME rid (reachable
                    // via the double-write compaction protocol — a worker
                    // can delete a rid backfill just claimed) can tombstone
                    // `internal` between our `Vacant` insert above and this
                    // re-check, already bumping `migrated_pre_flip` via
                    // `bump_migrated_on_tombstone`. Without the guard, this
                    // claim would ALSO win (vectors_u8 was never populated
                    // by the tombstone path) and bump the counter A SECOND
                    // TIME, risking the catch-up loop's `migrated >= target`
                    // firing early — and inserting a live graph node for an
                    // internal that is already deleted.
                    if self.quantization.is_some() && self.state.is_fitted.load(Ordering::Acquire) {
                        // #433 — the `!deleted` guard scopes ONLY the claim
                        // (mirroring `upsert`'s nested structure), NOT the
                        // `vectors` cleanup. The cleanup below MUST stay
                        // INSIDE this `is_fitted` block: if the flip has NOT
                        // happened (`is_fitted == false`) the vector must
                        // REMAIN in the f32 `vectors` buffer so the fit's
                        // snapshot/catch-up scan can claim it. The prior
                        // version ran `vectors.remove_async` UNCONDITIONALLY
                        // (outside the `if`), so a fit that flipped only AFTER
                        // this re-check removed the vector from `vectors`
                        // WITHOUT it ever landing in `vectors_u8` (present
                        // only in `rid_map`) — a pre-flip internal that never
                        // bumps `migrated_pre_flip` → catch-up loop hangs
                        // forever (`backfill_delete_same_rid_race_no_double_count`).
                        if !self.state.deleted.contains_sync(&internal) {
                            let codes = self.quantize(vec);
                            if self
                                .claim_and_publish_u8_async(internal, codes.clone())
                                .await
                                .is_some()
                            {
                                self.insert_u8_graph_node(internal, codes).await?;
                            }
                        }
                        // Cleanup runs regardless of the claim outcome (won,
                        // lost, or tombstoned) — a claimed/tombstoned internal
                        // must not linger in the f32 `vectors` map (mirrors
                        // `upsert`'s post-guard cleanup).
                        // DEADLOCK FIX (#589 class): `remove_sync`, not
                        // `remove_async` — same `vectors` map hazard as
                        // the `insert_sync` above.
                        let _ = self.state.vectors.remove_sync(&internal);
                    }
                }
            }
        }

        // #428 (VR-6) — deferred-fit on a quantization-enabled target.
        // Mirrors the trigger in `upsert`: if the post-compaction live set
        // crosses `FIT_THRESHOLD`, retrain the SQ8 quantizer from scratch on
        // the surviving vectors and build the u8 graph. This is Variant A
        // (honest re-fit, no QuantMeta carry-over from the pre-compaction
        // adapter). `try_fit_and_rebuild` is single-flight and a no-op when
        // `quantization.is_none()`, so this block is safe for non-quant
        // targets too (it just returns early).
        // #7BB73DA5: claim once, then schedule a task with its own adapter/state Arc.
        Self::schedule_fit(self);
        Ok(())
    }

    pub(crate) fn schedule_fit(&self) {
        if self.quantization.is_none() || self.is_quantized() || self.live_count() < FIT_THRESHOLD {
            return;
        }
        if self
            .state
            .fit_in_flight
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            return;
        }
        let adapter = self.clone_for_fit();
        let flight_guard = FitFlightGuard(Arc::clone(&self.state.fit_in_flight));
        // fire-and-forget: detached by design — fit is owned by this task and errors are logged.
        // NOTE: the task is spawned under the caller's span, which is implicitly lost across
        // the spawn boundary (no tracing dependency here); failures are reported via the
        // plain `log::error!` in `report_fit_failure` below instead.
        tokio::spawn(async move {
            let _flight_guard = flight_guard;
            let result = adapter.try_fit_and_rebuild().await;
            if let Err(error) = result {
                let phase: &'static str = if adapter.is_quantized() {
                    "after fitted publication"
                } else {
                    "before fitted publication"
                };
                adapter.report_fit_failure(phase, &error);
            }
        });
    }

    /// Single reporting path for a failed background SQ8 fit. Production
    /// reports via `log::error!`; test builds additionally record the
    /// diagnosed phase on the fit hook so tests can assert the phase.
    fn report_fit_failure(&self, phase: &'static str, error: &VectorError) {
        log::error!("SQ8 background fit failed {phase}: {error}");
        #[cfg(test)]
        if let Some(hook) = self
            .state
            .fit_test_hook
            .lock()
            .expect("fit hook lock")
            .as_ref()
        {
            *hook.recorded_failure.lock().expect("fit failure lock") =
                Some((phase, error.to_string()));
        }
    }

    /// V4.2 (#408) — Clone the build config for compaction rebuild.
    pub(crate) fn build_config(&self) -> HnswConfig {
        HnswConfig {
            max_elements: self
                .state
                .next_id
                .load(Ordering::Relaxed)
                .saturating_sub(self.state.deleted_count.load(Ordering::Relaxed))
                .max(1000)
                + 1000, // 10%+ buffer
            m: self.m,
            max_layer: self.max_layer,
            ef_construction: self.ef_construction,
            ef_search: self.ef_search,
        }
    }

    /// V4.2 (#408) — Create a new empty adapter suitable as a compaction target,
    /// with `compaction_deleted_rids` set to Some.
    ///
    /// Retained as the explicit non-quant constructor for tests/benches that
    /// model the f32 compaction rebuild; production compaction now goes through
    /// [`Self::new_compaction_target_quantized`] (which delegates here when `q`
    /// is `None`).
    #[allow(dead_code)] // test/bench API; production uses the quantized twin
    pub(crate) fn new_compaction_target(
        dim: u32,
        metric: VectorMetric,
        config: HnswConfig,
    ) -> Self {
        let mut adapter = Self::new(dim, metric, config);
        Arc::get_mut(&mut adapter.state)
            .expect("new adapter state is unique")
            .compaction_deleted_rids =
            Some(Arc::new(scc::HashMap::with_hasher(THasher::default())));
        adapter
    }

    /// #428 (VR-6) — quantization-aware compaction target.
    ///
    /// Mirrors [`Self::new_compaction_target`] but constructs the adapter via
    /// [`Self::new_with_quantization`] so the target is `quantization ==
    /// Some(q)`. The target STARTS un-fitted (`is_quantized() == false`): it
    /// runs the f32 path during `backfill_if_absent`, then the deferred-fit
    /// check at the end of backfill (or a subsequent `upsert`) crosses
    /// [`FIT_THRESHOLD`] and re-trains the quantizer on the POST-COMPACTION
    /// live set (Variant A — honest re-fit, no QuantMeta carry-over).
    ///
    /// `q == None` is equivalent to [`Self::new_compaction_target`]; callers
    /// may pass the old adapter's `quantization_mode()` directly.
    pub(crate) fn new_compaction_target_quantized(
        dim: u32,
        metric: VectorMetric,
        config: HnswConfig,
        q: Option<VectorQuantization>,
    ) -> Self {
        let mut adapter = Self::new_with_quantization(dim, metric, config, q);
        Arc::get_mut(&mut adapter.state)
            .expect("new adapter state is unique")
            .compaction_deleted_rids =
            Some(Arc::new(scc::HashMap::with_hasher(THasher::default())));
        adapter
    }

    // ===================================================================
    // V5.2 (#411) — SQ8 quantized u8-graph path.
    //
    // The adapter holds TWO graphs in Option slots: the legacy f32 graph
    // (`hnsw`) which is always present, and a u8 graph (`hnsw_u8`) which
    // is built once at the fit threshold. The `is_fitted` atomic is the
    // publish flag; once `true`, every insert and every search goes through
    // the u8 graph + rescore. See the struct doc on `HnswAdapter` for the
    // concurrency argument.
    // ===================================================================

    /// `true` when the adapter has crossed the fit threshold and the u8
    /// graph is live. `false` for unquantized adapters (always f32) and
    /// for quantized adapters below the threshold.
    pub(crate) fn is_quantized(&self) -> bool {
        self.state.is_fitted.load(Ordering::Acquire)
    }

    /// The opt-in quantization mode the adapter was constructed with.
    /// `None` = legacy f32 path, bit-for-bit. Used by the background
    /// compaction path (#428) to decide whether the compaction target
    /// should be a quantization-enabled adapter.
    pub(crate) fn quantization_mode(&self) -> Option<VectorQuantization> {
        self.quantization
    }

    /// Read-only handle to the frozen quantizer, if fitted. `None` for
    /// unquantized adapters and pre-fit quantized adapters. Used by the
    /// snapshot codec (#412) and introspection.
    #[allow(dead_code)] // API for #412 snapshot codec
    pub(crate) fn quantizer(&self) -> Option<&Arc<Sq8Quantizer>> {
        self.state.quantizer.get()
    }

    /// Read-only handle to the u8 graph, if fitted.
    #[allow(dead_code)] // API for #412 snapshot codec
    pub(crate) fn hnsw_u8_handle(&self) -> Option<Arc<Hnsw<'static, u8, ShamirDistU8>>> {
        self.state.hnsw_u8.load_full()
    }

    /// Iterate `(internal -> u8 codes)` pairs for snapshot serialisation
    /// (#412). No-op for unquantized / pre-fit adapters.
    #[allow(dead_code)] // API for #412 snapshot codec
    pub(crate) fn for_each_vector_u8<F: FnMut(usize, &[u8])>(&self, mut f: F) {
        if !self.is_quantized() {
            return;
        }
        self.state.vectors_u8.iter_sync(|internal, codes| {
            f(*internal, codes);
            true
        });
    }

    /// Quantize an f32 vector to u8 codes using the frozen quantizer.
    ///
    /// # Panics
    ///
    /// Panics if the adapter is not fitted (callers must gate on
    /// [`Self::is_quantized`]).
    pub(super) fn quantize(&self, vec: &[f32]) -> Vec<u8> {
        let q = self
            .state
            .quantizer
            .get()
            .expect("quantize called before fit");
        q.quantize(vec)
    }

    /// #423 (Б-1) — Canonical "publish codes + claim graph membership" op.
    ///
    /// Inserts `(internal, codes)` into `vectors_u8` via the atomic
    /// `scc::HashMap::insert` (lock-free CAS on the bucket slot) and
    /// returns `Some(())` IFF this call won the claim — i.e. `internal`
    /// was ABSENT from `vectors_u8` before this call. Returns `None` if
    /// `internal` was already present (another caller, or an earlier pass
    /// of the fit, already published codes for this internal).
    ///
    /// **Why the return value is the dedup authority for graph inserts.**
    /// A u8 graph node is identified by its `d_id == internal`. Inserting
    /// the same `d_id` twice creates a DUPLICATE node (a bug class we have
    /// hit before — `concurrent_upsert_across_threshold_no_duplicate_rids`).
    /// `hnsw_rs` exposes no `contains(id)` query, so we cannot ask the
    /// graph itself. Instead we make the `vectors_u8` slot the SINGLE
    /// source of truth: the caller that wins the `Ok` claim is the ONE
    /// caller entitled to insert the graph node. Every other caller
    /// observes `None` and skips the graph insert. Because
    /// `scc::HashMap::insert` is atomic per key, exactly one caller wins
    /// per `internal` — no double-insert is possible, regardless of
    /// concurrency between the fit catch-up loop, the fit snapshot/delta
    /// passes, and the upsert self-migration path.
    ///
    /// **Б-3 convergence:** a winning claim for an `internal <
    /// next_id_at_flip` (read from the adapter field, set by the fitter at
    /// the flip) also bumps `migrated_pre_flip`. This gives the catch-up
    /// loop an O(1) convergence signal that counts ONLY pre-flip internals
    /// and cannot be inflated by post-flip upserts (which allocate
    /// internals `>= next_id_at_flip`). Before the flip, `next_id_at_flip`
    /// is `usize::MAX` (sentinel), so pre-flip snapshot/delta claims (which
    /// happen before the field is set to the real value) would wrongly bump
    /// the counter — but the counter is only READ after the flip, and the
    /// fitter resets it to 0 at the flip, so the spurious pre-flip bumps are
    /// discarded (see `try_fit_and_rebuild`).
    ///
    /// `codes` is always consumed into `vectors_u8` on the winning path.
    /// On the losing path (`None`) the caller's `codes` are dropped — the
    /// already-present entry is authoritative (both are quantizations of
    /// the same vector under the same frozen quantizer, so they are
    /// bit-identical; keeping the existing one is correct).
    #[allow(clippy::disallowed_methods)] // scc::HashMap::insert is lock-free (CAS)
    pub(super) fn claim_and_publish_u8(&self, internal: usize, codes: Vec<u8>) -> Option<()> {
        if self.state.vectors_u8.insert_sync(internal, codes).is_ok() {
            // Won the claim — this internal is new to vectors_u8. Bump
            // the pre-flip migration counter if this is a pre-flip internal.
            let nif = self.state.next_id_at_flip.load(Ordering::Acquire);
            if internal < nif {
                self.state.migrated_pre_flip.fetch_add(1, Ordering::Relaxed);
            }
            Some(())
        } else {
            None
        }
    }

    /// Async twin of [`claim_and_publish_u8`] for the upsert paths.
    ///
    /// DEADLOCK FIX (#589 class, commit `7a4abf62`): formerly used
    /// `insert_async` to "avoid the synchronous bucket spin on
    /// contention" — that rationale is now WRONG because the
    /// `vectors_u8` map is read via the SYNCHRONOUS `read_sync`
    /// (`collect_live_vectors`, `search_quantized_graph`,
    /// `search_cofilter_quantized`, `search_prefilter`), scanned via
    /// `iter_sync` (`search_quantized_bruteforce`, `for_each_vector_u8`,
    /// the fit catch-up drain), and probed via `contains_sync` (fit
    /// catch-up loop, `test_vectors_u8_contains`) on the SAME runtime
    /// worker threads. Mixing `insert_async`'s lock-HANDOFF wait with
    /// those sync accessors risks a whole-runtime deadlock. The fn stays
    /// `async` to keep call sites unchanged; this call no longer
    /// suspends. Identical semantics: wins iff the slot was vacant,
    /// bumps `migrated_pre_flip` for pre-flip internals. See
    /// [`claim_and_publish_u8`] for the dedup authority argument.
    pub(super) async fn claim_and_publish_u8_async(
        &self,
        internal: usize,
        codes: Vec<u8>,
    ) -> Option<()> {
        if self.state.vectors_u8.insert_sync(internal, codes).is_ok() {
            let nif = self.state.next_id_at_flip.load(Ordering::Acquire);
            if internal < nif {
                self.state.migrated_pre_flip.fetch_add(1, Ordering::Relaxed);
            }
            Some(())
        } else {
            None
        }
    }

    /// #423 (Б-3) — bump `migrated_pre_flip` for a NEWLY-tombstoned
    /// pre-flip internal. Call ONLY after the `deleted.insert[_async]` that
    /// tombstoned `internal` returned `Ok` (i.e. this call won the
    /// tombstone) — mirrors [`claim_and_publish_u8`]'s claim-then-bump
    /// pattern so a tombstone counts toward fit convergence exactly once,
    /// exactly like a claim does. A tombstoned internal never ALSO wins a
    /// `vectors_u8` claim (every claim path filters `deleted.contains`
    /// first), so this and `claim_and_publish_u8[_async]` bump disjoint
    /// events for the same `internal` — no double-counting.
    ///
    /// Without this, a pre-flip internal tombstoned AFTER the fit-transition
    /// flip (a concurrent `delete`/rid-replacing `upsert` racing the fit)
    /// would never be claimed (the catch-up scan skips `deleted` entries)
    /// and the catch-up loop would spin forever waiting for a target that
    /// excluded it. See the seed comment in `try_fit_and_rebuild`.
    pub(super) fn bump_migrated_on_tombstone(&self, internal: usize) {
        let nif = self.state.next_id_at_flip.load(Ordering::Acquire);
        if internal < nif {
            self.state.migrated_pre_flip.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// VR-8 (#430, Б-6) — test-only: force a code into `vectors_u8` for
    /// `internal` WITHOUT running a fit. Simulates the exact race window the
    /// Б-6 fix guards against: a concurrent `try_fit_and_rebuild`'s
    /// `claim_and_publish_u8` (snapshot/delta pass) has ALREADY inserted the
    /// code for this internal, but the adapter is still PRE-flip — i.e. a
    /// `delete()` that reads `quantized_active() == false`. Pre-VR-8 such a
    /// delete skipped `vectors_u8.remove_async` and the code leaked until the
    /// next compaction. No-op if a code is already present. Compiles away in
    /// release builds.
    #[cfg(test)]
    pub(crate) fn test_force_publish_u8(&self, internal: usize, codes: Vec<u8>) {
        let _ = self.state.vectors_u8.insert_sync(internal, codes);
    }

    /// VR-8 (#430, Б-6) — test-only: does `vectors_u8` hold a code for
    /// `internal`? Unlike [`Self::for_each_vector_u8`] this is NOT gated on
    /// `is_quantized()`, so it can observe the race window where a code is
    /// resident in `vectors_u8` while the adapter is still pre-flip. Compiles
    /// away in release builds.
    #[cfg(test)]
    pub(crate) fn test_vectors_u8_contains(&self, internal: usize) -> bool {
        self.state.vectors_u8.contains_sync(&internal)
    }

    /// Post-VR-8-follow-up (delete-vs-backfill same-rid race) — test-only:
    /// snapshot of the convergence counters `(migrated_pre_flip,
    /// next_id_at_flip)`. The invariant a caller checks is
    /// `migrated_pre_flip <= next_id_at_flip` once fitted — a double-count
    /// bug (e.g. a claim winning twice for the same pre-flip internal, once
    /// via a tombstone site and once via an unguarded self-migration
    /// re-check) can push `migrated_pre_flip` PAST `next_id_at_flip`, which
    /// is otherwise invisible from the outside (the catch-up loop just
    /// exits a little early — no panic, no visible corruption — unless a
    /// concurrent in-flight upsert loses the race, which is rare and
    /// timing-dependent). Compiles away in release builds.
    #[cfg(test)]
    pub(crate) fn test_convergence_counters(&self) -> (usize, usize) {
        (
            self.state.migrated_pre_flip.load(Ordering::Acquire),
            self.state.next_id_at_flip.load(Ordering::Acquire),
        )
    }

    /// `true` when the adapter should run the u8 graph path. Equivalent to
    /// `is_quantized()` but inlined for the hot search/insert branches.
    #[inline]
    pub(super) fn quantized_active(&self) -> bool {
        // ACQUIRE (required): the fit publishes `hnsw_u8.store(Some)` (Release
        // via ArcSwapOption) BEFORE `is_fitted.store(true, Release)`. A reader
        // that observes `is_fitted == true` then does `hnsw_u8.load_full()`
        // and `.expect`s a live graph — so seeing `true` MUST happens-after the
        // graph store. Only an Acquire load of `is_fitted` establishes that
        // edge (Acquire-load synchronizes-with the Release-store, ordering all
        // of the fit thread's prior writes — incl. the graph store — before the
        // reader's subsequent `load_full`). A Relaxed load here is a data race:
        // it let a concurrent upsert observe `is_fitted == true` while
        // `hnsw_u8` still read `None`, panicking the `expect`
        // (`concurrent_upsert_across_threshold_no_loss` under nextest load).
        // A false negative (stale `false`) only routes ONE request through the
        // still-live f32 path — harmless. false→true flips exactly once.
        self.state.is_fitted.load(Ordering::Acquire) && self.quantization.is_some()
    }
}

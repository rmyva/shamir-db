use super::adapter::HnswAdapter;
use super::config::{ShamirDist, BRUTE_FORCE_MAX, FIT_THRESHOLD, MAX_EF_SEARCH, MAX_TOPK};
#[cfg(test)]
use super::search::TEST_SEARCH_F32_GATE;
use crate::adapter::{SearchOpts, VectorAdapter, VectorError};
use async_trait::async_trait;
use hnsw_rs::anndists::dist::distances::Distance;
use scc::hash_map::Entry::{Occupied, Vacant};
use shamir_types::types::record_id::RecordId;
use std::sync::atomic::Ordering;

#[async_trait]
impl VectorAdapter for HnswAdapter {
    async fn upsert(&self, rid: RecordId, vec: &[f32]) -> Result<(), VectorError> {
        if vec.len() as u32 != self.dim {
            return Err(VectorError::DimMismatch {
                expected: self.dim,
                got: vec.len() as u32,
            });
        }

        // D12: claim the rid slot atomically. Two concurrent upserts for the
        // SAME rid (reachable since III.5 moved HNSW promote outside
        // `commit_lock` — two committers can promote the same record at
        // once) must NOT both leave a LIVE graph node. The non-atomic
        // read-tombstone-then-reassign of the old code let both upserts
        // observe "no old internal", allocate distinct internals i1/i2,
        // insert both into the graph, and then race the final reassignment —
        // the loser's internal stayed un-tombstoned, so the rid surfaced
        // TWICE in search (and `len()` skewed) until the next rebuild-on-open.
        //
        // `entry_sync` serialises the slot: the second upsert blocks on
        // the bucket entry until the first has published its internal, then sees
        // it as the "old" occupant and tombstones it. The transition
        // (read old → tombstone in `deleted` → write new internal) is done
        // entirely synchronously while the entry is held, so it is atomic
        // per rid. The CPU-bound graph insert (`spawn_blocking`) runs AFTER
        // the entry is released — we never hold the scc entry across an
        // `.await` (would violate the lock-across-await invariant), and
        // tombstoning the loser's internal does not depend on its graph
        // insert having completed: it is in `deleted` before it can ever be
        // observed live (search filters `deleted` before resolving `rid_map`).
        //
        // DEADLOCK FIX (#589 class, commit `7a4abf62`, H1+H2 commit
        // `621776bd`): `entry_sync`, NOT `entry_async`. The
        // `rid_to_internal` map is scanned via `iter_sync` (snapshot
        // serialisation, `collect_live_vectors`) and probed via
        // `contains_sync` on the SAME worker threads. `entry_async`'s
        // handoff wait path risks a whole-runtime deadlock. AMPLIFIER: this
        // guard is held across the `deleted.insert_sync` tombstone below —
        // `entry_sync` (not `entry_async`) ensures a worker can never PARK
        // while owning the `rid_to_internal` bucket, closing the
        // cross-map chain into `deleted`.
        let internal = self.state.next_id.fetch_add(1, Ordering::Relaxed);
        let mut replaced: Option<usize> = None;
        {
            match self.state.rid_to_internal.entry_sync(rid) {
                Occupied(mut occ) => {
                    let old_internal = *occ.get();
                    // Tombstone the previous (or concurrently-serialised) internal.
                    if self.state.deleted.insert_sync(old_internal, ()).is_ok() {
                        self.state.deleted_count.fetch_add(1, Ordering::Relaxed);
                        self.bump_migrated_on_tombstone(old_internal);
                    }
                    *occ.get_mut() = internal;
                    replaced = Some(old_internal);
                }
                Vacant(vac) => {
                    vac.insert_entry(internal);
                }
            }
        } // scc entry guard dropped here — NOT held across the await below.

        // V5.2 (#411) — quantized post-fit path: quantize + insert into
        // the u8 graph. Pre-fit (or unquantized) falls through to the
        // f32 path below, which also appends to the f32 buffer so the
        // fitter can snapshot it at the threshold.
        if self.quantized_active() {
            // #433 — claim-then-insert (NOT a raw vectors_u8 insert): this
            // internal may be PRE-flip (a fit completed inside the
            // `entry_async` suspend above), in which case the fitter's
            // catch-up loop is waiting for `migrated_pre_flip` to account
            // for it. Routing through the shared claim authority bumps that
            // counter and dedups against a concurrent fit pass. See
            // `quantized_fastpath_publish`.
            self.quantized_fastpath_publish(internal, vec).await?;
            if let Some(old) = replaced {
                // DEADLOCK FIX (#589 class): `remove_sync`, not
                // `remove_async` — `vectors_u8` is scanned via `iter_sync`
                // and read via `read_sync` on the SAME worker threads.
                let _ = self.state.vectors_u8.remove_sync(&old);
            }
            // DEADLOCK FIX (same class as #589, commit `7a4abf62`; H3
            // commit `dcfaf825`): `insert_sync`, NOT `insert_async` —
            // `rid_map` is scanned via `iter_sync` in `for_each_rid_map`
            // (snapshot serialisation) on the SAME worker threads.
            let _ = self.state.rid_map.insert_sync(internal, rid);
            return Ok(());
        }

        // #418 — f32 insert path. Reached ONLY when `!quantized_active()`
        // (the quantized branch above returned early). f32 graph is always
        // present in that state; None is an invariant violation → error
        // (NOT panic — upsert must propagate failure cleanly).
        let hnsw = self.state.hnsw.load_full().ok_or_else(|| {
            VectorError::Internal("upsert: f32 graph absent on non-quantized path".into())
        })?;
        let vec_owned = vec.to_vec();
        // Retain a copy for the exact brute-force path before the vector is
        // moved into the (CPU-bound) graph insert.
        let vec_for_store = vec_owned.clone();
        tokio::task::spawn_blocking(move || {
            hnsw.insert((&vec_owned, internal));
        })
        .await
        .map_err(|e| VectorError::Internal(e.to_string()))?;
        // `internal` is freshly allocated (monotonic `next_id`) so this never
        // collides — the insert always lands.
        // DEADLOCK FIX (#589 class): `insert_sync`, not `insert_async` —
        // `vectors` is scanned via `iter_sync` (fit snapshot/delta/catch-up)
        // and read via `read_sync` on the SAME worker threads.
        let _ = self
            .state
            .vectors
            .insert_sync(internal, vec_for_store.clone());
        if let Some(old) = replaced {
            // Drop the superseded vector so brute-force never scans stale data
            // and memory stays bounded under upsert churn.
            // DEADLOCK FIX (#589 class): `remove_sync`, not `remove_async`.
            let _ = self.state.vectors.remove_sync(&old);
        }
        // DEADLOCK FIX (same class as #589, commit `7a4abf62`; H3
        // commit `dcfaf825`): `insert_sync`, NOT `insert_async` —
        // `rid_map` is scanned via `iter_sync` in `for_each_rid_map`
        // (snapshot serialisation) on the SAME worker threads.
        let _ = self.state.rid_map.insert_sync(internal, rid);

        // #423 (Б-1) — self-migration re-check: if `is_fitted` flipped
        // true between our initial `quantized_active()` check and now, our
        // vector is in `vectors` but the fitter's snapshot/delta scan may
        // have already passed. We claim the codes into `vectors_u8` via
        // `claim_and_publish_u8_async`. The claim is the EXACT authority
        // for "who inserts the graph node":
        //  - If WE win the claim (Some), this internal was not yet in
        //    `vectors_u8` → the fitter will NOT insert its node (it never
        //    saw it). We must insert the u8 graph node ourselves via
        //    `insert_u8_graph_node`.
        //  - If we LOSE the claim (None), the fitter already claimed this
        //    internal and will insert (or has inserted) its node in its
        //    final connectivity batch — we must NOT double-insert (a
        //    duplicate d_id node is a separate bug class).
        // This replaces the pre-#423 lie that "the fitter's final
        // parallel_insert handles graph connectivity" — that code did not
        // exist, so the vector was silently dropped from the graph.
        //
        if self.quantization.is_some() && self.state.is_fitted.load(Ordering::Acquire) {
            // #423 (Б-3, adversarial-review finding) — `!self.state.deleted.contains`
            // guard around the CLAIM, mirroring the three scan-based claim
            // sites (fit snapshot, delta, catch-up loop). Without it: a
            // concurrent `upsert` on the SAME rid can tombstone THIS
            // internal (via the `Occupied` rid_to_internal branch,
            // published by that call's entry_async block) between this
            // call publishing its own `internal` and reaching this check.
            // The tombstone already bumped `migrated_pre_flip` via
            // `bump_migrated_on_tombstone`; without this guard, this call
            // would ALSO claim the (now-tombstoned) internal into
            // `vectors_u8` and bump the counter a second time — an
            // over-count that can trip the catch-up loop's
            // `migrated >= target` convergence early, dropping the f32
            // graph while a genuinely still-in-flight, distinct pre-flip
            // internal has not yet landed. The buffer cleanup below still
            // runs regardless — a tombstoned internal must not linger in
            // the f32 `vectors` map (the catch-up scan and every other
            // claim path also skip `deleted` entries, so nothing else
            // would ever remove it).
            if !self.state.deleted.contains_sync(&internal) {
                let codes = self.quantize(&vec_for_store);
                if self
                    .claim_and_publish_u8_async(internal, codes.clone())
                    .await
                    .is_some()
                {
                    // We own the graph node. Insert it (CPU-bound rayon
                    // insert → spawn_blocking, no guard held across the
                    // await).
                    self.insert_u8_graph_node(internal, codes).await?;
                }
            }
            // Remove from f32 buffer so the catch-up drain sees it as done
            // (whether we claimed it, lost the claim, or it was already
            // tombstoned by a concurrent racer).
            // DEADLOCK FIX (#589 class): `remove_sync`, not `remove_async`.
            let _ = self.state.vectors.remove_sync(&internal);
            return Ok(());
        }

        // V5.2 (#411) — deferred fit trigger. Only quantization-enabled
        // adapters fit, and only once (the is_fitted flag + single-flight
        // guard make this idempotent). The check is AFTER the f32 insert
        // so the just-upserted vector is in the buffer the fitter will
        // snapshot.
        if self.quantization.is_some() && !self.is_quantized() && self.len() >= FIT_THRESHOLD {
            Self::schedule_fit(self);
        }
        Ok(())
    }

    /// Batch upsert with a single rayon `parallel_insert`.
    ///
    /// **Atomic dim validation:** every row's dimension is checked UP FRONT,
    /// before ANY mutation. A single mismatched row yields `Err(DimMismatch)`
    /// and leaves the graph untouched.
    ///
    /// **D12 across a batch:** we claim the rid slot per row through the same
    /// `entry_sync` protocol as single `upsert` — the slot is the
    /// serialization point. Two concurrent operations (batch ↔ batch, or
    /// batch ↔ single) racing on the SAME rid both go through the rid's
    /// bucket entry: the loser observes the winner's freshly-published
    /// internal as "old" and tombstones it. Within THIS batch, a duplicate
    /// rid is handled by re-entering the same entry: the earlier row's
    /// just-published internal becomes the "old" of the later row and is
    /// tombstoned — last write wins, no orphan live node.
    ///
    /// All CPU-bound graph work (the `parallel_insert` over the collected
    /// new internals) runs in ONE `spawn_blocking` after every entry guard
    /// has been released — we never hold an scc entry across `.await`.
    async fn upsert_batch(&self, items: &[(RecordId, Vec<f32>)]) -> Result<(), VectorError> {
        if items.is_empty() {
            return Ok(());
        }
        // Atomic dim validation: fail before touching anything.
        for (_, v) in items {
            if v.len() != self.dim as usize {
                return Err(VectorError::DimMismatch {
                    expected: self.dim,
                    got: u32::try_from(v.len()).unwrap_or(u32::MAX),
                });
            }
        }

        // Phase 1: per-rid slot claim (D12-safe). Collect:
        //   insert_rows: (internal, rid, owned_vec) — rows to insert into the
        //                graph; the owned Vecs move through spawn_blocking and
        //                on into `vectors` in Phase 3 (one clone total per row,
        //                matching single `upsert`).
        //   replaced    : old internals superseded by this batch (to drop
        //                from `vectors` so brute-force never scans stale data)
        let mut insert_rows: Vec<(usize, RecordId, Vec<f32>)> = Vec::with_capacity(items.len());
        let mut replaced: Vec<usize> = Vec::with_capacity(items.len());
        for (rid, vec) in items {
            let internal = self.state.next_id.fetch_add(1, Ordering::Relaxed);
            {
                // DEADLOCK FIX (#589 class, commit `7a4abf62`, H1+H2 commit
                // `621776bd`): `entry_sync`, NOT `entry_async` — same
                // rationale as single `upsert` (amplifier: this guard is
                // held across the `deleted.insert_sync` tombstone below).
                match self.state.rid_to_internal.entry_sync(*rid) {
                    Occupied(mut occ) => {
                        let old_internal = *occ.get();
                        // Tombstone the previous (or concurrently-serialised /
                        // earlier-in-this-batch) internal. Same rationale as
                        // single `upsert`: the transition is atomic per rid
                        // while the entry is held.
                        if self.state.deleted.insert_sync(old_internal, ()).is_ok() {
                            self.state.deleted_count.fetch_add(1, Ordering::Relaxed);
                            self.bump_migrated_on_tombstone(old_internal);
                        }
                        *occ.get_mut() = internal;
                        replaced.push(old_internal);
                    }
                    Vacant(vac) => {
                        vac.insert_entry(internal);
                    }
                }
            } // scc entry guard dropped — NOT held across the await below.
            insert_rows.push((internal, *rid, vec.clone()));
        }

        // V5.2 (#411) — quantized post-fit path: quantize the batch and
        // insert codes into the u8 graph in ONE parallel_insert. Pre-fit
        // (or unquantized) falls through to the f32 path below.
        if self.quantized_active() {
            let quantizer = self
                .state
                .quantizer
                .get()
                .expect("quantized_active but quantizer unset");
            // Quantize all rows (O(dim·N) scalar — cheap vs the graph insert).
            let code_rows: Vec<(usize, RecordId, Vec<u8>)> = insert_rows
                .iter()
                .map(|(internal, rid, vec)| (*internal, *rid, quantizer.quantize(vec)))
                .collect();
            // #433 — claim-then-insert per row (NOT a raw vectors_u8 insert +
            // unconditional parallel_insert). A fit that completed inside the
            // `entry_async` loop's suspends makes these PRE-flip internals the
            // catch-up loop is waiting on. Route each through the shared claim
            // authority: `claim_and_publish_u8_async` publishes the code into
            // `vectors_u8` AND bumps `migrated_pre_flip` for pre-flip
            // internals (restoring convergence), and its return value is the
            // dedup authority for the graph node — only claim WINNERS get a
            // node, so a concurrent fit pass that already published an
            // internal never yields a duplicate `d_id`. The rid_map insert
            // runs for every row (published, claimed by us or not).
            let mut graph_batch: Vec<(usize, Vec<u8>)> = Vec::with_capacity(code_rows.len());
            for (internal, rid, codes) in code_rows {
                if !self.state.deleted.contains_sync(&internal)
                    && self
                        .claim_and_publish_u8_async(internal, codes.clone())
                        .await
                        .is_some()
                {
                    graph_batch.push((internal, codes));
                }
                // DEADLOCK FIX (same class as #589, commit `7a4abf62`; H3
                // commit `dcfaf825`): `insert_sync`, NOT `insert_async` —
                // `rid_map` is scanned via `iter_sync` in `for_each_rid_map`
                // (snapshot serialisation) on the SAME worker threads.
                let _ = self.state.rid_map.insert_sync(internal, rid);
            }
            if !graph_batch.is_empty() {
                let hnsw_u8 = self
                    .hnsw_u8_handle()
                    .expect("quantized_active but u8 graph unset");
                tokio::task::spawn_blocking(move || {
                    let batch: Vec<(&Vec<u8>, usize)> =
                        graph_batch.iter().map(|(i, c)| (c, *i)).collect();
                    hnsw_u8.parallel_insert(&batch);
                })
                .await
                .map_err(|e| VectorError::Internal(e.to_string()))?;
            }
            for old in replaced {
                // DEADLOCK FIX (#589 class): `remove_sync`, not
                // `remove_async` — `vectors_u8` has sync accessors on the
                // SAME worker threads (see `claim_and_publish_u8_async` doc).
                let _ = self.state.vectors_u8.remove_sync(&old);
            }
            return Ok(());
        }

        // Phase 2: ONE spawn_blocking for the whole batch — rayon
        // parallelizes the graph inserts across cores. `parallel_insert`
        // takes `&[(&Vec<T>, usize)]`; we move the OWNED rows into the
        // closure, build the borrowed slice INSIDE (so the borrows never
        // cross the `'static` boundary), and RETURN the owned rows so Phase 3
        // moves each Vec straight into `vectors` — no second clone.
        // #418 — f32 batch-insert path. Reached ONLY when `!quantized_active()`
        // (the quantized branch above returned early). f32 graph is always
        // present in that state; None is an invariant violation → error.
        let hnsw = self.state.hnsw.load_full().ok_or_else(|| {
            VectorError::Internal("upsert_batch: f32 graph absent on non-quantized path".into())
        })?;
        let insert_rows = tokio::task::spawn_blocking(move || {
            let batch: Vec<(&Vec<f32>, usize)> =
                insert_rows.iter().map(|(i, _rid, v)| (v, *i)).collect();
            hnsw.parallel_insert(&batch);
            insert_rows
        })
        .await
        .map_err(|e| VectorError::Internal(e.to_string()))?;

        // Phase 3: publish the per-internal bookkeeping (vectors map +
        // rid_map) and drop superseded vectors. Each map op is independent
        // and ordered so `vectors` removal of `old` cannot race a freshly
        // reused `internal` (internals are monotonic from `next_id`, so a
        // brand-new internal never aliases a tombstoned old one).
        //
        // `into_iter` moves each owned Vec straight into `vectors` — the only
        // clone of a row's vector is the Phase-1 `vec.clone()` above.
        //
        // #423 (Б-1) — self-migration re-check for the batch f32 path. If
        // `is_fitted` flipped true during this batch, the rows whose
        // `vectors.insert_async` landed AFTER the flip must migrate to
        // `vectors_u8`. Each migrated row CLAIMS its slot via
        // `claim_and_publish_u8_async`; the rows that WIN the claim owe a
        // graph node, collected into `migrated_graph_batch` for ONE
        // `parallel_insert` after the loop (mirrors the single-upsert
        // self-migration contract). Rows that LOSE the claim were already
        // claimed by the fitter, which owns their graph node.
        let mut migrated_graph_batch: Vec<(usize, Vec<u8>)> = Vec::new();
        let mut any_migrated = false;
        for (internal, rid, vec) in insert_rows.into_iter() {
            // DEADLOCK FIX (#589 class): `insert_sync`, not `insert_async`
            // — `vectors` is scanned via `iter_sync` on the SAME worker
            // threads.
            let _ = self.state.vectors.insert_sync(internal, vec.clone());
            // DEADLOCK FIX (same class as #589, commit `7a4abf62`; H3
            // commit `dcfaf825`): `insert_sync`, NOT `insert_async` —
            // `rid_map` is scanned via `iter_sync` in `for_each_rid_map`
            // (snapshot serialisation) on the SAME worker threads.
            let _ = self.state.rid_map.insert_sync(internal, rid);

            if self.quantization.is_some() && self.state.is_fitted.load(Ordering::Acquire) {
                any_migrated = true;
                // #423 (Б-3, adversarial-review finding) — same
                // `!self.state.deleted.contains` guard as the single-`upsert`
                // self-migration re-check: a concurrent `upsert`/batch on
                // the SAME rid can tombstone THIS freshly-allocated
                // `internal` between this batch publishing it (in the
                // `entry_async` loop above) and this loop reaching it. The
                // tombstone already bumped `migrated_pre_flip`; claiming a
                // tombstoned internal here would double-bump the counter.
                if !self.state.deleted.contains_sync(&internal) {
                    let codes = self.quantize(&vec);
                    if self
                        .claim_and_publish_u8_async(internal, codes.clone())
                        .await
                        .is_some()
                    {
                        migrated_graph_batch.push((internal, codes));
                    }
                }
                // DEADLOCK FIX (#589 class): `remove_sync`, not
                // `remove_async`.
                let _ = self.state.vectors.remove_sync(&internal);
            }
        }
        // #423 (Б-1) — install the graph nodes for the rows this batch
        // claimed. ONE `parallel_insert` (rayon-parallelised) for the
        // whole batch; runs only if at least one row migrated.
        if any_migrated && !migrated_graph_batch.is_empty() {
            let hnsw_u8 = self.hnsw_u8_handle().ok_or_else(|| {
                VectorError::Internal("self-migration: u8 graph not fitted".into())
            })?;
            tokio::task::spawn_blocking(move || {
                let batch: Vec<(&Vec<u8>, usize)> =
                    migrated_graph_batch.iter().map(|(i, c)| (c, *i)).collect();
                hnsw_u8.parallel_insert(&batch);
            })
            .await
            .map_err(|e| VectorError::Internal(e.to_string()))?;
        }
        for old in replaced {
            // DEADLOCK FIX (#589 class): `remove_sync`, not `remove_async`.
            let _ = self.state.vectors.remove_sync(&old);
        }

        // V5.2 (#411) — deferred fit trigger (same rationale as single
        // `upsert`). Checked AFTER the f32 inserts land in the buffer.
        if self.quantization.is_some() && !self.is_quantized() && self.len() >= FIT_THRESHOLD {
            Self::schedule_fit(self);
        }
        Ok(())
    }

    async fn delete(&self, rid: RecordId) -> Result<(), VectorError> {
        // DEADLOCK FIX (#589 class, commit `7a4abf62`, H1+H2 commit
        // `621776bd`): EVERY lock-acquiring op in this fn is now `_sync`.
        // All five maps touched here (`rid_to_internal`, `deleted`,
        // `vectors`, `vectors_u8`) have SYNCHRONOUS accessors
        // (`iter_sync`, `contains_sync`, `read_sync`, `insert_sync`) on
        // the SAME runtime worker threads (search/compaction/snapshot
        // serialisation are ordinary async fns). Mixing any `_async`
        // call's lock-HANDOFF wait with those sync accessors risks a
        // whole-runtime deadlock. The fn stays `async` (trait shape
        // unchanged); it no longer suspends.
        if let Some(internal) = self.state.rid_to_internal.read_sync(&rid, |_, v| *v) {
            if self.state.deleted.insert_sync(internal, ()).is_ok() {
                self.state.deleted_count.fetch_add(1, Ordering::Relaxed);
                self.bump_migrated_on_tombstone(internal);
            }
            let _ = self.state.rid_to_internal.remove_sync(&rid);
            let _ = self.state.vectors.remove_sync(&internal);
            // V5.2 (#411) — also drop codes from the u8 buffer post-fit.
            // The graph node stays (hnsw_rs has no delete) but search
            // filters tombstoned internals, so it's invisible.
            //
            // VR-8 (#430, Б-6): the u8 removal is UNCONDITIONAL — we do NOT
            // gate it on `quantized_active()`. The tombstone insert above is
            // a happens-before point, so we must guarantee the code is evicted
            // regardless of whether a concurrent `try_fit_and_rebuild` flips
            // quantization false→true (or true→false) in the window between
            // our reads. Gating on a single `quantized_active()` snapshot
            // here would leak the code if a concurrent fitter's
            // `claim_and_publish_u8` (snapshot/delta pass) had already
            // inserted codes for this internal before we read the flag as
            // `false`. `scc::HashMap::remove_sync` on a missing key is a
            // no-op (returns `None`), so this is safe both pre-fit
            // (buffer empty) and post-fit.
            let _ = self.state.vectors_u8.remove_sync(&internal);
        }
        Ok(())
    }

    async fn search(
        &self,
        query: &[f32],
        k: u32,
        opts: SearchOpts,
        staged: Option<&[(RecordId, Vec<f32>)]>,
    ) -> Result<Vec<(RecordId, f32)>, VectorError> {
        if query.len() as u32 != self.dim {
            return Err(VectorError::DimMismatch {
                expected: self.dim,
                got: query.len() as u32,
            });
        }

        // Validate every staged vector before distance evaluation.
        if let Some(staged) = staged {
            for (_, vector) in staged {
                if vector.len() != self.dim as usize {
                    return Err(VectorError::DimMismatch {
                        expected: self.dim,
                        got: u32::try_from(vector.len()).unwrap_or(u32::MAX),
                    });
                }
            }
        }

        // P1 — clamp untrusted k (restored from master): a huge k must not
        // blow up `overscan`/`with_capacity`. Results are truncated to the
        // caller's k downstream, so early clamping is behaviour-identical.
        let k = if k == 0 {
            return Ok(vec![]);
        } else {
            k.min(MAX_TOPK)
        };

        // Per-query ef_search override (clamped to MAX_EF_SEARCH). None →
        // adapter build-time default (HnswConfig::ef_search). A clamp (not a
        // rejection) keeps untrusted input from crashing the worker — a huge
        // ef behaves like MAX_EF_SEARCH for recall but can't hold the rayon
        // pool indefinitely.
        //
        // P3 / V3.1: `opts.oversample` is consumed at the ENGINE level
        // (`read_filtered_vector_scan` requests `k′ = k × oversample`
        // candidates from this adapter, applies the residual predicate, and
        // retries with a widened `k′`). The adapter itself does NOT interpret
        // `oversample` — it returns the `k` it is asked for. We accept the
        // field so the engine can thread it through `IndexQuery::Vector`
        // without a separate channel.
        let _ = opts.oversample;
        let ef = match opts.ef_search {
            Some(v) => (v.min(MAX_EF_SEARCH) as usize).max(k as usize),
            None => self.ef_search,
        };

        // Small index → EXACT brute-force (deterministic, correct); large
        // index → approximate HNSW graph. See [`BRUTE_FORCE_MAX`].
        // Quantized brute-force threshold: 2×FIT_THRESHOLD (512). The u8
        // graph is built at FIT_THRESHOLD (256); at that scale hnsw_rs's
        // nondeterministic layer assignment can produce unreachable nodes.
        // Brute-force over 512 dim-24 codes is ~100 µs and guarantees 100%
        // recall. Beyond 512 the graph is large enough for reliable
        // connectivity.
        const QUANT_BRUTE_FORCE_MAX: usize = FIT_THRESHOLD * 2;
        let mut results: Vec<(RecordId, f32)> =
            if self.quantized_active() && self.len() <= QUANT_BRUTE_FORCE_MAX {
                // V5.2 (#411) — small quantized index: exact brute-force scan
                // over dequantized vectors (same as unquantized brute-force but
                // reading from vectors_u8 via the quantizer). Guarantees 100%
                // recall on small indexes where the HNSW graph may have poor
                // connectivity due to hnsw_rs's nondeterministic layer assignment.
                //
                // #424 (Б-4) — body extracted to [`search_quantized_bruteforce`]
                // so the transient-None retry path in the f32-graph branch below
                // can re-run the EXACT same logic instead of returning empty.
                self.search_quantized_bruteforce(query, k).await?
            } else if self.quantized_active() {
                // V5.2 (#411) — post-fit quantized search: traversal on the u8
                // graph (cheap integer distance) with overscan `16k+64`, then
                // dequant-rescore each candidate with the EXACT f32 distance to
                // the original (unquantized) query.
                //
                // #424 (Б-4) — body extracted to [`search_quantized_graph`] so
                // the transient-None retry path in the f32-graph branch below
                // can re-run the EXACT same logic instead of returning empty.
                self.search_quantized_graph(query, k, ef).await?
            } else if self.len() <= BRUTE_FORCE_MAX {
                let dist = ShamirDist {
                    metric: self.metric,
                };
                // Score while borrowing each vector; release the vectors map guard
                // before consulting deleted or rid_map to avoid nested map locks.
                let mut scores: Vec<(usize, f32)> = Vec::with_capacity(128);
                self.state.vectors.iter_sync(|i, v| {
                    scores.push((*i, dist.eval(query, v)));
                    true
                });
                let mut out: Vec<(RecordId, f32)> = Vec::with_capacity(scores.len());
                for (internal, score) in scores {
                    // DEADLOCK FIX (#589 class, commit `7a4abf62`):
                    // `contains_sync`, NOT `contains_async` — `deleted` is
                    // mutated via `insert_sync` and scanned via `iter_sync`
                    // on the SAME worker threads.
                    if self.state.deleted.contains_sync(&internal) {
                        continue;
                    }
                    // DEADLOCK FIX (same class as #589, commit `7a4abf62`; H3
                    // commit `dcfaf825`): `read_sync`, NOT `read_async` —
                    // `rid_map` is scanned via `iter_sync` in `for_each_rid_map`
                    // (snapshot serialisation) on the SAME worker threads.
                    if let Some(rid) = self.state.rid_map.read_sync(&internal, |_, r| *r) {
                        out.push((rid, score));
                    }
                }
                out
            } else {
                // Search committed f32 graph (approximate). Reached ONLY when
                // `!quantized_active()` AND `len() > BRUTE_FORCE_MAX` — in that
                // state the f32 graph is always present... UNLESS a concurrent
                // fit transition raced between this function's `quantized_active()`
                // gate (above) and the `hnsw.load_full()` below.
                //
                // #424 (Б-4) — transient-None race window. The sequence:
                //   1. THIS request read `quantized_active() == false` (pre-flip)
                //      in the `if/else if` chain above, so it landed here on the
                //      f32 path.
                //   2. A concurrent fitter ran `try_fit_and_rebuild`: it set
                //      `is_fitted = true` (Release) and then `hnsw.store(None)`
                //      (Release) to drop the f32 graph (#418 memory win).
                //   3. THIS request's `hnsw.load_full()` now returns `None`.
                //
                // The old code called this "invariant violation, unreachable" and
                // returned `Ok(vec![])` — but the window is REAL (the gate and the
                // load are two independent atomic reads with no lock between them).
                // A request that hits it silently got an empty result instead of a
                // correct answer through the now-live u8 graph.
                //
                // Fix: on `None`, RE-CHECK `quantized_active()`. If it is now
                // `true`, the flip happened in the window — route through the u8
                // path (same helper the main quantized branch uses). If it is
                // STILL `false`, the f32 graph is genuinely absent on a
                // non-quantized adapter — a true invariant violation (unquantized
                // adapters never drop the f32 graph; quantized adapters only drop
                // it post-flip, which IS `quantized_active()`) — defensive empty.
                //
                // #424 (Б-4) — test hook: pause HERE (after the
                // `quantized_active() == false` gate, before `load_full()`)
                // so a deterministic test can trigger a fit transition that
                // drops the f32 graph while this request is paused. Compiles
                // away in release builds.
                #[cfg(test)]
                if let Some(gate) = TEST_SEARCH_F32_GATE.load_full() {
                    // Signal the test that this request has reached the
                    // f32-gate (past the `quantized_active() == false`
                    // check, now paused before `load_full()`).
                    gate.arrived
                        .store(true, std::sync::atomic::Ordering::SeqCst);
                    // `notified()` is cancellation-safe: if the test has
                    // already called `notify_one` before we get here, the
                    // permit is stored and `notified()` resolves immediately.
                    gate.notify.notified().await;
                }
                let hnsw = match self.state.hnsw.load_full() {
                    Some(h) => h,
                    None => {
                        // #424 (Б-4) — transient flip during this request. Re-check
                        // and route through the now-correct u8 path.
                        if self.quantized_active() {
                            if self.len() <= QUANT_BRUTE_FORCE_MAX {
                                return self.search_quantized_bruteforce(query, k).await;
                            } else {
                                return self.search_quantized_graph(query, k, ef).await;
                            }
                        }
                        // Genuinely unreachable: a non-quantized adapter (or a
                        // pre-flip quantized adapter) has no reason for the f32
                        // graph to be absent. Defensive empty on a read path —
                        // never panic.
                        return Ok(vec![]);
                    }
                };
                let overscan = (k as usize) * 2 + 10;
                let query_owned = query.to_vec();
                let neighbors =
                    tokio::task::spawn_blocking(move || hnsw.search(&query_owned, overscan, ef))
                        .await
                        .map_err(|e| VectorError::Internal(e.to_string()))?;

                let mut out: Vec<(RecordId, f32)> = Vec::with_capacity(k as usize + 16);
                for n in neighbors {
                    // DEADLOCK FIX (#589 class, commit `7a4abf62`):
                    // `contains_sync`, NOT `contains_async` — same
                    // `deleted` map hazard as the bruteforce branch above.
                    if self.state.deleted.contains_sync(&n.d_id) {
                        continue;
                    }
                    // DEADLOCK FIX (same class as #589, commit `7a4abf62`; H3
                    // commit `dcfaf825`): `read_sync`, NOT `read_async` —
                    // `rid_map` is scanned via `iter_sync` in `for_each_rid_map`
                    // (snapshot serialisation) on the SAME worker threads.
                    if let Some(rid) = self.state.rid_map.read_sync(&n.d_id, |_, v| *v) {
                        out.push((rid, n.distance));
                    }
                }
                out
            };

        // Merge the caller's own un-committed staged vectors (in-tx search)
        // via a brute-force scan — they are not in the committed graph.
        if let Some(staged) = staged {
            let dist = ShamirDist {
                metric: self.metric,
            };
            for (rid, vector) in staged {
                if vector.len() != self.dim as usize {
                    return Err(VectorError::DimMismatch {
                        expected: self.dim,
                        got: u32::try_from(vector.len()).unwrap_or(u32::MAX),
                    });
                }
                let d = dist.eval(query, vector);
                results.push((*rid, d));
            }
        }

        // Sort by distance ascending, truncate to k.
        results.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        results.truncate(k as usize);

        Ok(results)
    }

    fn dim(&self) -> u32 {
        self.dim
    }

    fn len(&self) -> usize {
        // `saturating_sub`: two independent Relaxed loads; a stale `next_id`
        // with a fresher `deleted_count` under concurrent replace/delete
        // would underflow a plain subtraction (see `live_count`).
        self.state
            .next_id
            .load(Ordering::Relaxed)
            .saturating_sub(self.state.deleted_count.load(Ordering::Relaxed))
    }

    /// V2.3 (#402) — `HnswAdapter` IS the snapshot-able adapter. The
    /// background snapshot trigger in `VectorBackend` uses this to recover
    /// the concrete type for `dump_snapshot_with_gen`.
    fn as_hnsw_adapter(&self) -> Option<&HnswAdapter> {
        Some(self)
    }
}

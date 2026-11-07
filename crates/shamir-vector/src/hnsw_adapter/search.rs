use super::adapter::HnswAdapter;
use super::config::{CATCHUP_BACKOFF, CATCHUP_SPIN_BUDGET, FIT_THRESHOLD, MAX_TOPK};
use crate::adapter::VectorError;
use crate::quantized_dist::{RescoreCtx, ShamirDistU8};
use crate::sq8::Sq8Quantizer;
use hnsw_rs::hnsw::Hnsw;
use shamir_types::types::record_id::RecordId;
use std::sync::atomic::Ordering;
use std::sync::Arc;

// #424 (Б-4) — test-only hook for deterministically reproducing the
// transient-None race window in `search`/`search_cofilter`.
//
// The race is between `quantized_active()` (Acquire load) and
// `hnsw.load_full()` (ArcSwap load) — two atomic reads with NO `.await`
// between them in the f32-graph branch. Under normal execution the window
// is a few nanoseconds of synchronous code; a statistical test cannot
// reliably hit it (confirmed: 3× consecutive runs of a tight-loop
// concurrent test passed even with the fix disabled).
//
// This hook lets a test PAUSE a search request at exactly the point where
// the race lives — AFTER `quantized_active() == false` has been read (so
// the request is committed to the f32 branch) but BEFORE
// `hnsw.load_full()`. The test then triggers a fit transition (which
// drops the f32 graph), confirms the drop, and releases the paused
// request — which now observes `load_full() == None` and must exercise the
// retry path.
//
// The hook carries TWO pieces of state:
//  * `arrived` — set by the search request when it REACHES the gate (so
//    the test knows the request is past the `quantized_active()` check and
//    is now paused inside the f32 branch).
//  * `notify` — the pause/release mechanism. The search request awaits
//    `notify.notified()`; the test calls `notify.notify_one()` to release
//    it after confirming the f32 graph has been dropped.
//
// `ArcSwapOption` so tests can install/clear it freely. Compiles away
// entirely in release builds (`#[cfg(test)]`).
#[cfg(test)]
pub(crate) struct TestF32Gate {
    pub(crate) arrived: std::sync::atomic::AtomicBool,
    pub(crate) notify: tokio::sync::Notify,
}

#[cfg(test)]
pub(super) static TEST_SEARCH_F32_GATE: std::sync::LazyLock<arc_swap::ArcSwapOption<TestF32Gate>> =
    std::sync::LazyLock::new(arc_swap::ArcSwapOption::empty);

/// #424 (Б-4) — install the test-only f32-gate hook. Returns the `Arc`
/// so the test can: (a) poll `.arrived` to confirm the search request has
/// reached the gate, and (b) call `.notify.notify_one()` to release it
/// after confirming the f32 graph has been dropped. Test-only.
#[cfg(test)]
pub(crate) fn install_test_search_f32_gate() -> Arc<TestF32Gate> {
    let gate = Arc::new(TestF32Gate {
        arrived: std::sync::atomic::AtomicBool::new(false),
        notify: tokio::sync::Notify::new(),
    });
    TEST_SEARCH_F32_GATE.store(Some(Arc::clone(&gate)));
    gate
}

/// #424 (Б-4) — clear the test-only f32-gate hook (between tests).
#[cfg(test)]
pub(crate) fn clear_test_search_f32_gate() {
    TEST_SEARCH_F32_GATE.store(None);
}

#[cfg(test)]
pub(crate) fn install_fit_test_hook(adapter: &HnswAdapter) -> Arc<super::adapter::FitTestHook> {
    let hook = Arc::new(super::adapter::FitTestHook {
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
        fail_before_publish: std::sync::atomic::AtomicBool::new(false),
        fail_after_publish: std::sync::atomic::AtomicBool::new(false),
        recorded_failure: std::sync::Mutex::new(None),
    });
    *adapter.state.fit_test_hook.lock().expect("fit hook lock") = Some(Arc::clone(&hook));
    hook
}

#[cfg(test)]
pub(crate) fn clear_fit_test_hook(adapter: &HnswAdapter) {
    *adapter.state.fit_test_hook.lock().expect("fit hook lock") = None;
}

#[cfg(test)]
pub(crate) async fn wait_for_fit(adapter: &HnswAdapter) {
    loop {
        if adapter.is_quantized() && !adapter.state.fit_in_flight.load(Ordering::Acquire) {
            return;
        }
        tokio::task::yield_now().await;
    }
}

#[cfg(test)]
pub(crate) async fn await_fitted(adapter: &HnswAdapter) {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(60);
    // Quantized is published before catch-up and f32-graph teardown; wait for
    // the fit to leave flight too, so callers observe the converged state.
    while !adapter.is_quantized() || adapter.state.fit_in_flight.load(Ordering::Acquire) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "adapter never finished fitting"
        );
        tokio::task::yield_now().await;
    }
}

impl HnswAdapter {
    /// Try to fit the SQ8 quantizer and build the u8 graph.
    ///
    /// Triggered by `upsert`/`upsert_batch` when the live-element count
    /// crosses [`FIT_THRESHOLD`] AND `quantization == Some(Sq8)`.
    ///
    /// # Concurrency (class #408 — no lost mutation)
    ///
    /// A single-flight guard ([`Self::fit_in_flight`]) ensures exactly
    /// ONE caller performs the rebuild. The rebuild proceeds in three
    /// phases, ALL inside ONE `spawn_blocking` so no `.await` holds a
    /// guard:
    ///
    ///  1. **Snapshot** the f32 buffer under a brief scc read-cursor,
    ///     recording the set of live internals seen.
    ///  2. **Fit + build**: fit `Sq8Quantizer` on the snapshot, quantize
    ///     each f32 vector, and `parallel_insert` the codes into a fresh
    ///     `Hnsw<'static, u8, ShamirDistU8>`.
    ///  3. **Delta re-insert**: any upsert that arrived BETWEEN the
    ///     snapshot and the mutex-publish of the new graph added its f32
    ///     vector to the buffer (the upsert path always appends to the
    ///     buffer before checking `is_fitted`). We scan the buffer again
    ///     and quantize+insert any internal NOT already in the u8 graph.
    ///     This closes the race: no f32 vector added during the rebuild
    ///     is lost.
    ///
    /// The publish (step 3's completion) sets `is_fitted = true` with
    /// `Release` ordering, which pairs with the `Acquire` in
    /// [`Self::is_quantized`]. After publish, every subsequent upsert
    /// goes through the post-fit path (quantize + insert into the u8
    /// graph directly).
    ///
    /// **#423 (Б-1 + Б-3) — graph connectivity + exact convergence.**
    /// A catch-up loop runs post-publish to drain pre-flip in-flight
    /// upserts. Every code that enters `vectors_u8` — whether via the
    /// snapshot pass, the delta pass, the catch-up loop, or the upsert
    /// self-migration path — claims its `vectors_u8` slot through
    /// [`Self::claim_and_publish_u8`], which is the SINGLE authority for
    /// "this internal's graph node has been inserted" (see its doc). The
    /// catch-up loop accumulates the codes that won the claim but had no
    /// graph node yet (the snapshot/delta passes inserted graph nodes
    /// for their own claims; the catch-up and self-migration claims are
    /// the ones that still need a node) and performs ONE final
    /// `parallel_insert` into `hnsw_u8` before dropping the f32 graph.
    /// Convergence is counted by `migrated_pre_flip` (only internals
    /// `< next_id_at_flip`), NOT by `vectors_u8.len()` — see Б-3.
    ///
    /// Returns `Err` only when fitting or graph construction fails.
    pub(super) async fn try_fit_and_rebuild(&self) -> Result<(), VectorError> {
        if self.quantization.is_none() {
            return Ok(());
        }
        // This runs only from a background fit task holding an owned adapter clone;
        // the scheduler in fit.rs owns the fit-in-flight RAII guard.
        // Snapshot the f32 buffer (internal, vec) pairs. We read under a
        // plain scc scan — no entry guard held across the await below.
        let mut snapshot: Vec<(usize, Vec<f32>)> = Vec::new();
        self.state.vectors.iter_sync(|internal, vec| {
            // Skip tombstoned internals — their codes must NOT enter the
            // u8 graph (a tombstoned internal is never resurrectated:
            // next_id is monotonic, so a brand-new internal never aliases
            // a tombstoned one).
            if !self.state.deleted.contains_sync(internal) {
                snapshot.push((*internal, vec.clone()));
            }
            true
        });
        // The snapshot is empty or below threshold → nothing to fit yet
        // (the threshold check is the caller's responsibility, but we
        // defend in depth).
        if snapshot.len() < FIT_THRESHOLD {
            return Ok(());
        }

        #[cfg(test)]
        {
            // Clone the hook out of the lock so the `MutexGuard` temporary is
            // dropped before the `.await` below (the spawned task must stay
            // `Send`).
            let hook = self
                .state
                .fit_test_hook
                .lock()
                .expect("fit hook lock")
                .clone();
            if let Some(hook) = hook {
                hook.entered.notify_one();
                hook.release.notified().await;
            }
        }

        let dim = self.dim as usize;
        let metric = self.metric;
        let ef_construction = self.ef_construction;
        let m = self.m;
        let max_layer = self.max_layer;

        // Phase 1+2: fit + build. The graph INSERT is deferred until AFTER
        // the snapshot codes are claimed into `vectors_u8` so that the
        // claim (Ok/Err) is the exact authority for "does this internal
        // already have a graph node" — see [`claim_and_publish_u8`].
        #[cfg(test)]
        {
            let hook = self
                .state
                .fit_test_hook
                .lock()
                .expect("fit hook lock")
                .clone();
            if let Some(hook) = hook {
                if hook.fail_before_publish.swap(false, Ordering::AcqRel) {
                    return Err(VectorError::Internal(
                        "before fitted publication: injected fit failure".into(),
                    ));
                }
            }
        }
        let training: Vec<Vec<f32>> = snapshot.iter().map(|(_, v)| v.clone()).collect();
        // #418 — the f32 graph is NO LONGER retained here. The pre-#418 code
        // held an explicit `Arc::clone(&self.state.hnsw)` "to keep the f32 graph
        // alive", which was exactly the bug that defeated SQ8's memory win
        // (the graph stayed resident for the adapter's lifetime). We now drop
        // it post-fit (see the drop at the end of this fn) — the snapshot
        // already captured everything the u8 graph needs.

        // VR-8 (#430, О-2): `Sq8Quantizer::fit` is pure CPU work (O(N×dim)
        // min/max/scale over the training set, potentially thousands of
        // vectors). Push it onto the blocking pool so it never stalls a
        // tokio worker thread (pillar №2: CPU-bound → spawn_blocking).
        // `training` is an owned `Vec<Vec<f32>>` (no `&self` borrows), so it
        // moves cleanly into the closure; we get back an `Arc<Sq8Quantizer>`.
        // This same path serves the compaction (VR-6) re-fit, which reuses
        // `try_fit_and_rebuild` verbatim, so both fit sites are covered.
        let quantizer_arc =
            match tokio::task::spawn_blocking(move || Arc::new(Sq8Quantizer::fit(&training, dim)))
                .await
            {
                Ok(q) => q,
                Err(e) => {
                    return Err(VectorError::Internal(format!(
                        "Sq8Quantizer::fit spawn_blocking join error: {e}"
                    )))
                }
            };
        let dist = ShamirDistU8::new(Arc::clone(&quantizer_arc), metric);

        // Quantize the snapshot and CLAIM each internal into vectors_u8.
        // Pre-flip: `next_id_at_flip` is still `usize::MAX` (sentinel), so
        // the claims DO bump `migrated_pre_flip` spuriously — but the
        // fitter OVERWRITES the counter with the correct seed at the flip
        // (see the flip block below), discarding those bumps. The snapshot
        // pass is the FIRST claimant for these internals (they were in
        // `vectors` at snapshot time and no post-flip path has run —
        // `is_fitted` is still false), so every claim here wins Ok and the
        // codes enter vectors_u8. `snapshot_claimed` tallies the winners
        // for the Б-3 convergence seed.
        let mut graph_batch_snapshot: Vec<(usize, Vec<u8>)> = Vec::with_capacity(snapshot.len());
        let mut snapshot_claimed = 0usize;
        for (internal, vec) in &snapshot {
            let codes = quantizer_arc.quantize(vec);
            if self
                .claim_and_publish_u8(*internal, codes.clone())
                .is_some()
            {
                graph_batch_snapshot.push((*internal, codes));
                snapshot_claimed += 1;
            }
        }

        // Phase 3: delta re-insert. Collect any internals that arrived
        // during the build (they're in `vectors` but not in our snapshot)
        // and claim them into vectors_u8 + the graph batch.
        let snapshot_ids: shamir_collections::TFxSet<usize> =
            snapshot.iter().map(|(i, _)| *i).collect();
        let mut graph_batch_delta: Vec<(usize, Vec<u8>)> = Vec::new();
        let mut delta_claimed = 0usize;
        self.state.vectors.iter_sync(|internal, vec| {
            if !snapshot_ids.contains(internal) && !self.state.deleted.contains_sync(internal) {
                let codes = quantizer_arc.quantize(vec);
                if self
                    .claim_and_publish_u8(*internal, codes.clone())
                    .is_some()
                {
                    graph_batch_delta.push((*internal, codes));
                    // SAFETY: this closure is `FnMut`-safe for a plain
                    // `usize` counter increment (no await, no guard).
                    delta_claimed += 1;
                }
            }
            true
        });

        // Build the u8 graph and insert the snapshot + delta codes in ONE
        // `parallel_insert` (the delta codes arrived during the build; both
        // sets were claimed above so there is exactly one node per internal).
        let mut graph_batch_for_build: Vec<(usize, Vec<u8>)> = graph_batch_snapshot;
        graph_batch_for_build.append(&mut graph_batch_delta);
        let dist_for_build = dist.clone();
        let (hnsw_u8, build_result): (
            Arc<Hnsw<'static, u8, ShamirDistU8>>,
            Result<(), VectorError>,
        ) = {
            let result = tokio::task::spawn_blocking(move || {
                let hnsw = Hnsw::<u8, ShamirDistU8>::new(
                    m,
                    graph_batch_for_build.len().max(1000) + 1000,
                    max_layer,
                    ef_construction,
                    dist_for_build,
                );
                let batch: Vec<(&Vec<u8>, usize)> = graph_batch_for_build
                    .iter()
                    .map(|(internal, codes)| (codes, *internal))
                    .collect();
                hnsw.parallel_insert(&batch);
                Arc::new(hnsw)
            })
            .await;
            match result {
                Ok(h) => (h, Ok(())),
                Err(e) => {
                    return Err(VectorError::Internal(format!(
                        "fit spawn_blocking join error: {e}"
                    )))
                }
            }
        };
        build_result?;

        // === PUBLISH quantizer (needed for self-migration quantize) ===
        #[cfg(test)]
        {
            let hook = self
                .state
                .fit_test_hook
                .lock()
                .expect("fit hook lock")
                .clone();
            if let Some(hook) = hook {
                if hook.fail_before_publish.swap(false, Ordering::AcqRel) {
                    return Err(VectorError::Internal(
                        "before fitted publication: injected fit failure".into(),
                    ));
                }
            }
        }
        self.state
            .quantizer
            .set(Arc::clone(&quantizer_arc))
            .map_err(|_| {
                VectorError::Internal("quantizer already published before fit flip".into())
            })?;

        // === FLIP is_fitted + store graph ===
        // After this, new upserts go u8-path. In-flight pre-flip upserts
        // continue inserting into `vectors` (caught by catch-up below).
        //
        // Capture next_id at flip time: all internals < this value were
        // allocated pre-flip and must end up in vectors_u8 (unless
        // tombstoned). Used by the catch-up convergence check (Б-3).
        let next_id_at_flip = self.state.next_id.load(Ordering::Acquire);
        // Deleted count AT FLIP: every tombstone counted here is of an
        // internal `< next_id_at_flip` (an internal must exist — i.e. have
        // been allocated by `next_id.fetch_add`, which only increases —
        // before it can be tombstoned, and `next_id` is monotonic, so any
        // tombstone visible at this Acquire load happened against an
        // internal that already existed when `next_id_at_flip` was read).
        // Folded into the `migrated_pre_flip` seed below — see the seed
        // comment for why pre-flip tombstones are counted as "accounted
        // for" alongside pre-flip claims.
        let deleted_count_at_flip = self.state.deleted_count.load(Ordering::Acquire);
        // Б-3 convergence seed + target. Every pre-flip internal (id <
        // next_id_at_flip) ends up in EXACTLY ONE of two disjoint states:
        // (a) claimed into vectors_u8 (via snapshot/delta/catch-up/
        //     self-migration — all funnel through `claim_and_publish_u8`,
        //     which bumps `migrated_pre_flip` for `internal <
        //     next_id_at_flip`), or
        // (b) tombstoned (via `delete`/rid-replacing `upsert`/
        //     `upsert_batch` — these ALSO bump `migrated_pre_flip` for
        //     `old_internal < next_id_at_flip`, using the exact same
        //     sentinel-then-seed trick as claims: see those call sites).
        // A tombstoned internal is excluded from the live snapshot/delta
        // scan (`self.state.deleted.contains` guards both), so (a) and (b) never
        // double-count the SAME internal — the seed below is exact, not
        // an over/under-approximation.
        //
        // The ORIGINAL Б-3 fix (a single reviewer round before this one)
        // subtracted a FROZEN `deleted_count_at_flip` from `target` instead
        // of counting tombstones into `migrated_pre_flip` — that undercounts
        // `target` correctly for tombstones that happened BEFORE the flip,
        // but any tombstone of a pre-flip internal AFTER the flip (a
        // concurrent `delete`/rid-collision racing the fit) would never be
        // claimed (the catch-up scan skips `deleted` entries) and never
        // bump the frozen target — the loop would spin forever (CONFIRMED
        // adversarial-review finding). Folding tombstones into the SAME
        // live counter as claims closes that gap: a post-flip tombstone of
        // a pre-flip internal now bumps `migrated_pre_flip` directly, so
        // convergence is reachable regardless of WHEN the tombstone lands.
        //
        // Seed = (claims already won at flip time) + (tombstones already
        // recorded at flip time); target = next_id_at_flip (no
        // subtraction — tombstones now count INTO the accumulator instead
        // of OUT of the target). The seed is written as a plain `store`
        // (not `fetch_add`): no concurrent self-migration or tombstone
        // bump can be lost by this overwrite, because both gate on
        // `next_id_at_flip` via an `Acquire` load, and that load cannot
        // observe the real (non-sentinel) value until AFTER this store
        // has completed — `next_id_at_flip.store` (below, `Release`) is
        // strictly ORDERED AFTER this store in program order on this
        // thread, and the standard release-sequence guarantee means any
        // thread whose `Acquire` load of `next_id_at_flip` observes the
        // real value also observes every write (including this one) that
        // preceded the `next_id_at_flip` release-store in program order.
        // So a concurrent claim/tombstone can only ever see (and
        // `fetch_add` on top of) the seed — never race under it.
        self.state.migrated_pre_flip.store(
            snapshot_claimed + delta_claimed + deleted_count_at_flip,
            Ordering::Release,
        );
        self.state
            .next_id_at_flip
            .store(next_id_at_flip, Ordering::Release);
        self.state.hnsw_u8.store(Some(Arc::clone(&hnsw_u8)));
        self.state.is_fitted.store(true, Ordering::Release);
        #[cfg(test)]
        {
            let hook = self
                .state
                .fit_test_hook
                .lock()
                .expect("fit hook lock")
                .clone();
            if let Some(hook) = hook {
                if hook.fail_after_publish.swap(false, Ordering::AcqRel) {
                    return Err(VectorError::Internal(
                        "after fitted publication: injected fit failure".into(),
                    ));
                }
            }
        }

        // === CONVERGING CATCH-UP LOOP (post-publish) ===
        //
        // In-flight pre-flip upserts finish inserting into `vectors`. The
        // upsert self-migration path AND this loop both claim their codes
        // into vectors_u8 via [`claim_and_publish_u8`]; whichever wins the
        // claim owns the graph-node insert. This loop accumulates the codes
        // whose claim it WON (and which therefore still need a graph node)
        // into `pending_graph_inserts`; a single `parallel_insert` after
        // the loop installs all of them at once. Self-migration winners
        // (and the quantized fast-path via `quantized_fastpath_publish`)
        // insert their own single node via `insert_u8_graph_node`.
        //
        // Convergence (Б-3): count ONLY pre-flip internals that have landed
        // in vectors_u8 OR been tombstoned (`migrated_pre_flip`), NOT
        // `vectors_u8.len()`. The latter is inflated by post-flip upserts
        // (internals >= next_id_at_flip land directly in vectors_u8),
        // which would make the loop exit early — before a pre-flip
        // upsert's `vectors.insert` has run, dropping the f32 graph under
        // its feet (Б-3 → "f32 graph absent" Internal error).
        // `migrated_pre_flip` is bumped exactly once per distinct pre-flip
        // internal — once via a winning `claim_and_publish_u8[_async]`
        // call, or once via a tombstone site (`delete`/rid-replacing
        // `upsert`/`upsert_batch`) — so it reaches `next_id_at_flip`
        // precisely when every pre-flip internal has been claimed or
        // tombstoned (see the seed comment above for why these two sets
        // are disjoint and jointly exhaustive).
        let mut pending_graph_inserts: Vec<(usize, Vec<u8>)> = Vec::new();
        // Target: every pre-flip internal — no subtraction. Tombstones now
        // count INTO `migrated_pre_flip` (see above), not out of the
        // target, so the target is simply the count of internals allocated
        // before the flip.
        let target = next_id_at_flip;
        let mut spins: u32 = 0;
        loop {
            // Claim any pre-flip internals still sitting in the f32 buffer.
            self.state.vectors.iter_sync(|internal, vec| {
                if !self.state.deleted.contains_sync(internal) {
                    let codes = quantizer_arc.quantize(vec);
                    if self
                        .claim_and_publish_u8(*internal, codes.clone())
                        .is_some()
                    {
                        // We won the claim → we owe a graph node. Stash
                        // for the final batch insert after the loop.
                        pending_graph_inserts.push((*internal, codes));
                    }
                }
                true
            });

            // Drain entries that are in vectors_u8 (claimed by us or by a
            // concurrent self-migration upsert). Both paths remove from
            // `vectors`; whichever runs first wins, the second is a no-op.
            let mut internals_to_drop: Vec<usize> = Vec::new();
            self.state.vectors.iter_sync(|internal, _| {
                if self.state.vectors_u8.contains_sync(internal) {
                    internals_to_drop.push(*internal);
                }
                true
            });
            for internal in &internals_to_drop {
                let _ = self.state.vectors.remove_sync(internal);
            }

            // Convergence: migrated_pre_flip is bumped only for claims on
            // internals < next_id_at_flip (see claim_and_publish_u8). When
            // it reaches `target`, every non-tombstoned pre-flip internal
            // has been claimed into vectors_u8 — by this loop OR by a
            // concurrent self-migration upsert (both bump the same
            // counter, since both go through claim_and_publish_u8).
            let migrated = self.state.migrated_pre_flip.load(Ordering::Acquire);
            if migrated >= target {
                break;
            }

            // Backoff (see CATCHUP_SPIN_BUDGET / CATCHUP_BACKOFF): a bounded
            // zero-latency spin for the common fast-convergence case, then a
            // short sleep so the fitter parks its core for the in-flight
            // upserts it is waiting on instead of hot-spinning and starving
            // them.
            spins += 1;
            if spins <= CATCHUP_SPIN_BUDGET {
                tokio::task::yield_now().await;
            } else {
                tokio::time::sleep(CATCHUP_BACKOFF).await;
            }
        }

        // === #423 (Б-1) — FINAL GRAPH CONNECTIVITY INSERT ===
        //
        // Every internal that entered `vectors_u8` via THIS fit (snapshot,
        // delta, and catch-up passes above) won its claim and is owed a
        // graph node. The snapshot + delta nodes were inserted in the build
        // `parallel_insert`; the catch-up claims are collected in
        // `pending_graph_inserts`. Internals claimed by a concurrent
        // self-migration upsert inserted their OWN single node (and are NOT
        // in `pending_graph_inserts` — we lost those claims). So this batch
        // contains exactly the nodes we owe, no duplicates, no omissions.
        //
        // This runs BEFORE `self.state.hnsw.store(None)`: by the time the f32
        // graph is dropped, every internal in `vectors_u8` has a node in
        // `hnsw_u8`. Before this fix, the catch-up loop and self-migration
        // populated `vectors_u8` WITHOUT inserting graph nodes — a vector
        // was invisible to graph-search and co-filter forever, and rode
        // into the v2 snapshot (which dumps hnsw_u8) as a hole.
        if !pending_graph_inserts.is_empty() {
            let hnsw_u8_for_catchup = Arc::clone(&hnsw_u8);
            tokio::task::spawn_blocking(move || {
                let batch: Vec<(&Vec<u8>, usize)> = pending_graph_inserts
                    .iter()
                    .map(|(internal, codes)| (codes, *internal))
                    .collect();
                hnsw_u8_for_catchup.parallel_insert(&batch);
            })
            .await
            .map_err(|e| VectorError::Internal(format!("catch-up graph insert join error: {e}")))?;
        }

        // === #418 — DROP THE F32 GRAPH ===
        //
        // At this point the post-fit path is FULLY published and converged:
        //  - `hnsw_u8` is live under the ArcSwapOption (Release store above).
        //  - `is_fitted == true` (Release store above) → every NEW search /
        //    upsert / delete is gated through `quantized_active()` and reads
        //    the u8 graph. No NEW caller can reach the f32 graph.
        //  - The catch-up loop above drained `vectors`: every pre-flip
        //    in-flight upsert has either landed in `vectors_u8` or been
        //    tombstoned. The f32 buffer is empty.
        //  - `vectors_u8` holds every live code AND every one of those
        //    codes has a node in `hnsw_u8` (the final connectivity insert
        //    above closed Б-1). `collect_live_vectors` dequantizes from
        //    `vectors_u8` (NOT from the f32 graph); the v2 snapshot dumps
        //    `hnsw_u8` + `vectors_u8`. The f32 graph is unobservable to
        //    every post-fit code path.
        //
        // `store(None)` is a Release store. Any in-flight pre-fit SEARCH that
        // already did `hnsw.load_full()` holds its own `Arc` clone and
        // finishes its traversal against the now-private graph; the `Arc`'s
        // refcount reaches 0 only after the last such reader drops its clone
        // — RCU, no UAF. (A pre-fit search that has NOT yet loaded sees
        // `is_fitted == true` on its next `quantized_active()` check and
        // routes through the u8 graph instead — the gate makes the f32 graph
        // unobservable to readers that have not already snapshotted it.)
        //
        // Unquantized adapters NEVER reach here (`try_fit_and_rebuild` returns
        // early when `quantization.is_none()`), so the f32 graph of a
        // non-quant adapter is retained for its full lifetime — bit-for-bit
        // back-compat.
        self.state.hnsw.store(None);

        Ok(())
    }

    /// Insert a single node `(codes, internal)` into the live u8 graph.
    /// CPU-bound (rayon `insert`) → `spawn_blocking`; the codes are tiny
    /// (dim bytes) so the clone is cheap. Used by
    /// [`quantized_fastpath_publish`](Self::quantized_fastpath_publish)
    /// (post-fit single upsert) and by the self-migration path (which owns
    /// the node insert when it wins the `vectors_u8` claim — see Б-1).
    pub(super) async fn insert_u8_graph_node(
        &self,
        internal: usize,
        codes: Vec<u8>,
    ) -> Result<(), VectorError> {
        let hnsw_u8 = self
            .hnsw_u8_handle()
            .ok_or_else(|| VectorError::Internal("u8 graph not fitted".into()))?;
        let codes_for_insert = codes.clone();
        tokio::task::spawn_blocking(move || {
            hnsw_u8.insert((&codes_for_insert, internal));
        })
        .await
        .map_err(|e| VectorError::Internal(format!("u8 insert join error: {e}")))?;
        Ok(())
    }

    /// #433 (post-VR-8) — canonical "quantized fast-path publish" for a
    /// SINGLE freshly-allocated `internal` whose `quantized_active()` check
    /// observed `true`.
    ///
    /// The subtle bug this closes: `internal` is allocated by
    /// `next_id.fetch_add` BEFORE the `quantized_active()` check, with a
    /// `rid_to_internal.entry_async` `.await` in between. A concurrent
    /// `try_fit_and_rebuild` can complete ENTIRELY inside that suspend —
    /// capturing `next_id_at_flip > internal` and flipping `is_fitted` —
    /// so this `internal` is a PRE-flip internal (`< next_id_at_flip`) that
    /// the fitter's catch-up loop is waiting to see accounted for. The old
    /// fast-path published via a RAW `vectors_u8.insert_async`, which never
    /// bumps `migrated_pre_flip`, AND inserted a graph node unconditionally.
    /// The catch-up loop scans only the f32 `vectors` buffer (this path never
    /// touches it), so it never claimed the internal either — the counter
    /// never reached `target = next_id_at_flip` and the loop span forever
    /// (TIMEOUT).
    ///
    /// The fix routes this path through the SAME claim authority as the
    /// self-migration re-check: guard on `!deleted`, `claim_and_publish_u8`
    /// (bumps `migrated_pre_flip` for pre-flip internals — restoring
    /// convergence), and insert the graph node ONLY if we win the claim
    /// (a concurrent fit snapshot/delta/catch-up pass may already own it —
    /// inserting a second node for the same `d_id` is the duplicate-node bug
    /// class). Claim BEFORE the graph insert (never after) so a lost claim
    /// never leaves a stray duplicate node.
    ///
    /// For a long-since-converged adapter (the common production path) the
    /// added cost over the old code is exactly one lock-free
    /// `deleted.contains` probe on a typically-empty/small map — negligible
    /// versus the CPU-bound graph insert this path already performs.
    pub(super) async fn quantized_fastpath_publish(
        &self,
        internal: usize,
        vec: &[f32],
    ) -> Result<(), VectorError> {
        if self.state.deleted.contains_sync(&internal) {
            // A concurrent same-rid upsert/delete tombstoned this internal
            // (it already bumped `migrated_pre_flip` via the tombstone site).
            // Skip both the claim and the graph insert.
            return Ok(());
        }
        let codes = self.quantize(vec);
        if self
            .claim_and_publish_u8_async(internal, codes.clone())
            .await
            .is_some()
        {
            // We own the graph node — insert it. A lost claim means a
            // concurrent fit pass already published this internal and owns
            // (or has already installed) its node.
            self.insert_u8_graph_node(internal, codes).await?;
        }
        Ok(())
    }

    // ------------------------------------------------------------------
    // #424 (Б-4) — post-fit u8 search cores.
    //
    // These are the u8-graph / u8-brute-force bodies that the main
    // `quantized_active()` branches in `search` / `search_cofilter` inline.
    // Extracted as private helpers so the transient-None retry path (a
    // request that read `quantized_active() == false`, then raced a fit
    // flip that dropped the f32 graph before `hnsw.load_full()`) can re-run
    // the EXACT same u8 logic instead of returning an empty result.
    // ------------------------------------------------------------------

    /// Post-fit quantized search on a SMALL index (exact brute-force over
    /// dequantized u8 codes). Mirrors the `quantized_active() && len <=
    /// QUANT_BRUTE_FORCE_MAX` branch of [`Self::search`].
    ///
    /// `k` is the (already clamped) top-k; returns at most `k` pairs sorted
    /// ascending by distance. Tombstoned internals are skipped.
    pub(super) async fn search_quantized_bruteforce(
        &self,
        query: &[f32],
        k: u32,
    ) -> Result<Vec<(RecordId, f32)>, VectorError> {
        let quantizer = self
            .state
            .quantizer
            .get()
            .expect("quantized_active but quantizer unset");
        // Audit finding 4.1 (task #530): build the fused rescore context ONCE
        // per query (precomputes `qm`/`qs`/`q_norm`), then score each candidate
        // with zero per-candidate dequant allocation.
        //
        // F3/F7: score INSIDE the `iter_sync` closure instead of cloning each
        // `Vec<u8>` code (dim bytes + a heap alloc per candidate) out just to
        // score and immediately drop it. Pass 1 now collects the pre-computed
        // f32 score (4 bytes/entry, no allocation); pass 2 filters
        // `deleted`/`rid_map` over the much smaller `(usize, f32)` pairs using
        // that score directly. The two-pass shape is deliberately kept — the
        // `deleted`/`rid_map` probes stay OUTSIDE `iter_sync` to avoid a new
        // cross-map nesting inside the bucket-lock-held callback.
        let ctx = RescoreCtx::new(self.metric, quantizer, query);
        let mut pairs: Vec<(usize, f32)> = Vec::with_capacity(256);
        self.state.vectors_u8.iter_sync(|i, c| {
            pairs.push((*i, ctx.score(c)));
            true
        });
        let mut out: Vec<(RecordId, f32)> = Vec::with_capacity(pairs.len());
        for (internal, score) in pairs {
            // DEADLOCK FIX (#589 class, commit `7a4abf62`):
            // `contains_sync`, NOT `contains_async`. The `deleted` map is
            // written via `insert_sync` (upsert/upsert_batch/delete) and
            // scanned via `iter_sync` (`for_each_deleted`, snapshot
            // serialisation) on the SAME worker threads.
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
        out.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        out.truncate(k as usize);
        Ok(out)
    }

    /// Post-fit quantized search on a LARGE index (u8 HNSW graph traversal
    /// + exact f32 rescore). Mirrors the `quantized_active() && len >
    /// QUANT_BRUTE_FORCE_MAX` branch of [`Self::search`].
    ///
    /// `ef` is the (already clamped) ef_search floor; the actual graph
    /// exploration uses `max(ef, overscan)` where `overscan = 16k+64`.
    pub(super) async fn search_quantized_graph(
        &self,
        query: &[f32],
        k: u32,
        ef: usize,
    ) -> Result<Vec<(RecordId, f32)>, VectorError> {
        let quantizer = self
            .state
            .quantizer
            .get()
            .expect("quantized_active but quantizer unset");
        let hnsw_u8 = self
            .hnsw_u8_handle()
            .expect("quantized_active but u8 graph unset");
        let query_codes = quantizer.quantize(query);
        let overscan = ((k as usize) * 16 + 64).min(MAX_TOPK as usize);
        let ef_q = ef.max(overscan);
        // VR-7 option 2 (#429, F4 item 4): hoist the query's dequant norm so
        // the Cosine graph traversal's `eval` can reuse it instead of
        // recomputing it on every hop. Pushed onto a thread-local stack
        // (keyed by the query code slice's pointer) inside the
        // `spawn_blocking` closure — the same thread that runs `eval`. A
        // RAII guard pops on drop (panic-safe). Soft-miss on pointer mismatch
        // → recompute (never stale, never panic). Computed here (outside the
        // closure) because `quantizer` borrows `self` (not `'static`); the
        // `query_codes` heap pointer is unchanged by the move.
        let query_norm = quantizer.dequant_norm_sq(&query_codes);
        let neighbors = tokio::task::spawn_blocking(move || {
            let _norm_guard = ShamirDistU8::push_query_norm(&query_codes, query_norm);
            hnsw_u8.search(&query_codes, overscan, ef_q)
        })
        .await
        .map_err(|e| VectorError::Internal(e.to_string()))?;

        // Audit finding 4.1 (task #530): fused rescore context built ONCE per
        // query, scored per candidate with no per-candidate dequant allocation.
        let ctx = RescoreCtx::new(self.metric, quantizer, query);
        let mut out: Vec<(RecordId, f32)> = Vec::with_capacity(k as usize + 16);
        for n in neighbors {
            // DEADLOCK FIX (#589 class, commit `7a4abf62`):
            // `contains_sync`, NOT `contains_async` — same `deleted` map
            // hazard as `search_quantized_bruteforce`.
            if self.state.deleted.contains_sync(&n.d_id) {
                continue;
            }
            // DEADLOCK FIX (#589 class): `read_sync`, NOT `read_async` —
            // `vectors_u8` is scanned via `iter_sync` (this fn's bruteforce
            // twin, fit catch-up drain) and read via `read_sync`
            // (`collect_live_vectors`) on the SAME worker threads.
            let score_opt = self
                .state
                .vectors_u8
                .read_sync(&n.d_id, |_, c| ctx.score(c));
            if let Some(exact) = score_opt {
                // DEADLOCK FIX (same class as #589, commit `7a4abf62`; H3
                // commit `dcfaf825`): `read_sync`, NOT `read_async` —
                // `rid_map` is scanned via `iter_sync` in `for_each_rid_map`
                // (snapshot serialisation) on the SAME worker threads.
                if let Some(rid) = self.state.rid_map.read_sync(&n.d_id, |_, v| *v) {
                    out.push((rid, exact));
                }
            }
        }
        out.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        out.truncate(k as usize);
        Ok(out)
    }

    /// Post-fit quantized co-filter search (u8 HNSW `search_filter` with an
    /// allow-set + exact f32 rescore). Mirrors the `quantized_active()`
    /// branch of [`Self::search_cofilter`].
    ///
    /// `ef` is the (already overscan-clamped) ef_search; `allow_set` is the
    /// internal-ID allow-set (as an owned `Arc`) built from the candidate
    /// RIDs by the caller.
    pub(super) async fn search_cofilter_quantized(
        &self,
        query: &[f32],
        k: u32,
        ef: usize,
        allow_set: Arc<shamir_collections::TFxSet<usize>>,
    ) -> Result<Vec<(RecordId, f32)>, VectorError> {
        let quantizer = self
            .state
            .quantizer
            .get()
            .expect("quantized_active but quantizer unset");
        let hnsw_u8 = self
            .hnsw_u8_handle()
            .expect("quantized_active but u8 graph unset");
        let query_codes = quantizer.quantize(query);
        let knbn = k as usize;
        // VR-7 option 2 (#429, F4 item 4): hoist the query's dequant norm —
        // see `search_quantized_graph` for the full rationale.
        let query_norm = quantizer.dequant_norm_sq(&query_codes);
        // F7 bonus: the allow-set arrives as an owned `Arc` from the caller
        // (`search_cofilter`), so it moves directly into the `spawn_blocking`
        // closure (satisfying the `'static` bound) with zero data cloning —
        // previously this site did `Arc::new(allow_set.clone())` per query.
        let neighbors = tokio::task::spawn_blocking(move || {
            let _norm_guard = ShamirDistU8::push_query_norm(&query_codes, query_norm);
            let pred = |id: &usize| allow_set.contains(id);
            hnsw_u8.search_filter(&query_codes, knbn, ef, Some(&pred))
        })
        .await
        .map_err(|e| VectorError::Internal(e.to_string()))?;

        // Audit finding 4.1 (task #530): fused rescore context built ONCE per
        // query, scored per candidate with no per-candidate dequant allocation.
        let ctx = RescoreCtx::new(self.metric, quantizer, query);
        let mut out: Vec<(RecordId, f32)> = Vec::with_capacity(k as usize);
        for n in neighbors {
            // DEADLOCK FIX (#589 class, commit `7a4abf62`): `read_sync`,
            // NOT `read_async` — `vectors_u8` is scanned via `iter_sync`
            // (`search_quantized_bruteforce`, fit catch-up drain) and
            // mutated via `insert_sync`/`remove_sync` on the SAME worker
            // threads.
            let score_opt = self
                .state
                .vectors_u8
                .read_sync(&n.d_id, |_, c| ctx.score(c));
            if let Some(exact) = score_opt {
                // DEADLOCK FIX (same class as #589, commit `7a4abf62`; H3
                // commit `dcfaf825`): `read_sync`, NOT `read_async` —
                // `rid_map` is scanned via `iter_sync` in `for_each_rid_map`
                // (snapshot serialisation) on the SAME worker threads.
                if let Some(rid) = self.state.rid_map.read_sync(&n.d_id, |_, v| *v) {
                    out.push((rid, exact));
                }
            }
        }
        out.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        out.truncate(k as usize);
        Ok(out)
    }
}

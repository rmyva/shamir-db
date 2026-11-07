//! Unified index lifecycle state machine + per-family error/cancellation
//! contract (F-76 / #903).
//!
//! This module is the single, reviewable source of truth for what
//! `Ok` / `Err` mean for every index DDL transition across ALL FOUR index
//! families, and exactly when each index becomes (or stops being)
//! planner-visible. It is referenced from every family's CREATE/DROP/RENAME
//! code path so a future reader can answer "what does `Err` mean for a
//! sorted-index RENAME" from this doc alone, without spelunking four files.
//!
//! The persisted enum that backs the in-memory lifecycle is
//! [`crate::state::IndexState`] (`Building` / `Ready` / `Failed`). This doc extends that
//! enum into the full CONCEPTUAL lifecycle F-72 (#899) and F-76
//! (#903) implement:
//!
//! ```text
//!                  ┌─────────┐  CREATE starts   ┌──────────┐  backfill ok   ┌───────┐
//!   (no index) ──▶ │ Absent  │ ───────────────▶ │ Building │ ─────────────▶ │ Ready │
//!                  └─────────┘                  └──────────┘                └───────┘
//!                       ▲                           │                            │
//!                       │                           │ DROP / crash              │ DROP
//!                       │                           ▼                            ▼
//!                       │                       ┌──────────────────────────▶  sweep postings,
//!                       │                       (dropped mid-build:             retire def,
//!                       │                        restart-from-scratch           persist
//!                       │                        self-heal on reopen)            │
//!                       │                                                       ▼
//!                       └─────────────────────────────────────────────────── (no index)
//! ```
//!
//! ## DROP and failed builds
//!
//! F-72 anticipated `Dropping` and `Failed`. The shipped `IndexState` has a
//! `Failed` variant for failed builds; DROP instead retires the definition.
//! The lifecycle rationale is:
//!
//! - **DROP visibility (F-76):** the fix is "retire the definition from the
//!   planner-visible snapshot FIRST, sweep postings SECOND". Retiring the
//!   definition ENTIRELY (RCU-swap it out of the Vec / `remove_by_id` out of
//!   the registry) is strictly stronger than flipping a `Dropping` state that
//!   the planner would then have to learn to filter — every planner lookup
//!   already treats "absent" as "fall back to a full scan". A `Dropping`
//!   variant would only add value if an operator-introspection path needed to
//!   surface "this index is mid-teardown" status; that is out of scope. So
//!   DROP is modelled as `Ready → (absent)` with the posting sweep happening
//!   AFTER the definition is already gone from every planner-visible
//!   structure.
//! - **CREATE crash (F-50 Step 3b):** a build interrupted mid-backfill leaves
//!   `Building` durably on disk; the table-reopen self-heal re-backfills from
//!   scratch. `Failed` is also used for failed builds and remains
//!   planner-invisible; the reconciliation path
//!   is fully automatic (index2) or operator-invoked `doctor::repair()`
//!   (base_index). See the per-family crash rows below.
//!
//! Persisted enum variants have append-only bincode ordinals; compatibility
//! with older readers must be considered before adding any new variant.
//!
//! ## The four families
//!
//! | family | definition store | planner-visible gate | postings store |
//! |--------|------------------|----------------------|----------------|
//! | regular (hash) | `IndexInfo` (RCU `ArcSwap<Vec>`) | `iter_indexes_ready` (filters `state == Ready`) | `info_store` prefix `IndexRecordKey(false, id)` |
//! | unique (hash)  | `IndexInfo` (RCU `ArcSwap<Vec>`) | `iter_unique_indexes` (membership) + `write_barrier` bit | `info_store` prefix `IndexRecordKey(true, id)` |
//! | sorted         | `NodeReplicated<Vec>` (RCU) | `find_by_field` over the Vec (membership) | `info_store` sorted-prefix |
//! | index2 (fts/functional/vector) | `IndexRegistry` (`scc::HashMap` tuple) | `find_by_field_and_kind` (filters `state == Ready`) | per-backend (`drop_all`) |
//!
//! ---
//!
//! # CREATE
//!
//! ## regular (hash) — `create_index` → `create_index_from_records`
//! - **Planner-visible exactly when:** Phase 3 flips the registered
//!   definition's `state` from `Building` → `Ready` (`iter_indexes_ready`
//!   then yields it). The definition is registered at `Building` in Phase 1
//!   (planner-invisible); the streamed backfill runs in Phase 2; the flip +
//!   `save_index_info` are Phase 3. (F-72 / #899.)
//! - **`Ok`:** the index is queryable in THIS process AND durable as `Ready`
//!   on disk (`save_index_info` ran).
//! - **`Err`:**
//!   - backfill error → the definition stays `Building` in memory AND on disk
//!     (Phase 1's `save_index_info` already persisted it `Building`).
//!     Planner-invisible. Retryable: a second CREATE re-runs the backfill.
//!   - Phase 3 persist error → `Err` returned while the definition is already
//!     `Ready` in memory but durably `Building` on disk (intentional — see
//!     `create_index_from_records`'s Phase 3 doc: postings are already fully
//!     written; a restart re-observes `Building` and needs `doctor::repair()`
//!     to reconcile).
//! - **Retryable after `Err`:** yes (idempotent backfill; same name reuses the
//!   same interned id and posting keys are deterministic).
//! - **Crash/cancel:** an abandoned `Building` definition is invisible to the
//!   planner (F-72 gate). On restart, base_index load lifts on-disk defs to
//!   `Ready` by default — a KNOWN LIMITATION (F-72 brief): a base_index
//!   `Building` def whose backfill was interrupted may load as `Ready` with
//!   partial postings. Operator `doctor::repair()` reconciles. (index2 has
//!   automatic self-heal; base_index does not — documented gap.)
//!
//! ## unique (hash) — `create_unique_index` → `create_unique_index_from_records`
//! - **Planner-visible exactly when:** `indexes_unique` membership is
//!   published (RCU swap). Uniqueness enforcement is gated by the
//!   `UNIQUE_INDEX_EXISTS` write-barrier bit, raised under
//!   `unique_write_lock`.
//! - **`Ok`:** queryable + enforced + durable.
//! - **`Err`:** best-effort; a persist/backfill failure can leave a live
//!   definition with missing postings (the brief's Problem 2 call-out). The
//!   write-barrier bit may already be raised.
//! - **Retryable:** yes, but a duplicate that slipped through an error gap is
//!   a correctness violation of the uniqueness guarantee, not a harmless
//!   double-write — RENAME closes this by holding `unique_write_lock` across
//!   the whole drop→create (see RENAME).
//! - **Crash/cancel:** same base_index limitation as regular (no automatic
//!   self-heal); `doctor::repair()`.
//!
//! ## sorted — `create_sorted_index_with_include`
//! - **Planner-visible exactly when:** the definition's `state` flips
//!   `Building` → `Ready` after the streamed backfill (F-72 gate via
//!   `find_by_field` over the Vec). The Vec membership itself is published at
//!   `Building` in the register step (planner-invisible).
//! - **`Ok`:** queryable + durable (`persist_defs` ran). The existing code is
//!   explicitly commented `cancel-safe: NO` for the streamed backfill.
//! - **`Err`:** a backfill error leaves a `Building` definition; retryable.
//! - **Crash/cancel:** `Building` on disk; base_index limitation (no auto
//!   self-heal); `doctor::repair()`.
//!
//! ## index2 (fts/functional/vector) — `create_index_v2`
//! - **Planner-visible exactly when:** `registry.insert` (backend enters the
//!   live tuple set at `Building`) followed immediately by
//!   `registry.set_state(id, Ready)`. `find_by_field_and_kind` filters
//!   `state != Ready`, so the backend is invisible from `insert` until the
//!   `set_state(Ready)` call. Sequence: durable `Building` persist
//!   (`save_index2_metadata_with_pending`) → private backfill (backend NOT in
//!   the live registry) → `insert` at `Building` → `set_state(Ready)` → final
//!   `save_index2_metadata`.
//! - **`Ok`:** queryable + durable `Ready`.
//! - **`Err`:** a failure of the FINAL `save_index2_metadata` returns `Err`
//!   while the index is already live `Ready` in memory but durably `Building`
//!   on disk (the brief's Problem 2 call-out). Acceptable: postings are fully
//!   written; restart re-observes `Building`.
//! - **Retryable:** yes.
//! - **Crash/cancel:** `Building` durable on disk → the table-reopen
//!   restart-from-scratch self-heal drops the partial postings, re-backfills,
//!   flips to `Ready`, re-persists (F-50 Step 3b, fully automatic).
//!
//! ---
//!
//! # DROP  (F-76 / #903 — definition retired BEFORE the posting sweep)
//!
//! The correctness property (F-76): a reader concurrently querying during a
//! DROP observes EITHER the complete index's correct result OR a full-scan
//! fallback as if the index did not exist — NEVER a registered-but-partially-
//! emptied index returning wrong/incomplete results.
//!
//! ## regular (hash) — `IndexManager::drop_index`  ✅ FIXED + tested (F-76)
//! - **Planner-invisible exactly when:** `indexes.remove_index` (RCU swap)
//!   runs FIRST. From that point every NEW reader's `iter_indexes_ready`
//!   snapshot no longer contains the definition → full-scan fallback.
//! - **Sequence:** `remove_index` → (test seam) → prefix-scan +
//!   `remove_many` sweep → `posting_cache.retain` → `save_index_info`.
//! - **`Ok(true)`:** definition gone from memory + postings swept + durable.
//! - **`Err`:** only the sweep's `remove_many` or the final `save_index_info`
//!   can error. If the sweep errors, the definition is ALREADY gone from
//!   memory (planner-invisible) but orphan postings may remain on disk; the
//!   durable blob may still list the def. A restart re-loads the def
//!   `Ready` with partial postings (base_index limitation); `doctor::repair()`
//!   re-syncs. Retryable.
//! - **In-flight reader:** keeps working against its pre-swap `Vec` snapshot
//!   (RCU); its postings are untouched by the sweep until it drops the
//!   snapshot. ✅
//!
//! ## unique (hash) — `drop_unique_index`  ✅ FIXED + tested (F-76)
//! - **Planner-invisible exactly when:** `indexes_unique.remove_index` (RCU
//!   swap) + `UNIQUE_INDEX_EXISTS` bit clear run FIRST. Writers stop
//!   maintaining the index immediately.
//! - **Sequence:** `remove_index` + barrier-bit clear → (test seam) → sweep →
//!   `save_index_info_unique`.
//! - **`Ok`/`Err`/crash:** same shape as regular. ✅
//!
//! ## sorted — `SortedIndexManager::drop_index`  ✅ ALREADY SAFE (no fix needed)
//! - **Planner-invisible exactly when:** the `indexes.rcu` swap (definition
//!   retirement) runs FIRST, before the generation bump and the sweep. This
//!   family was already correct — the brief's "mirror-image" bug never
//!   applied here. (Documented for completeness; no code change.)
//! - **Sequence:** `rcu` retire → `generation.fetch_add` → sweep →
//!   `persist_defs`.
//!
//! ## index2 — `TableManager::drop_index2`  ✅ FIXED + tested (F-76)
//! - **Planner-invisible exactly when:** `registry.remove_by_id(id)` runs
//!   FIRST. `find_by_field_and_kind` can no longer resolve the backend →
//!   full-scan fallback.
//! - **Sequence:** `remove_by_id` (also bumps the F-50 generation) → (test
//!   seam) → `backend.drop_all()` sweep → `save_index2_metadata`.
//! - **`Ok(true)`:** backend gone from the registry + postings swept +
//!   durable.
//! - **`Err`:** `drop_all` or `save_index2_metadata` can error. If `drop_all`
//!   errors, the backend is ALREADY retired from the registry
//!   (planner-invisible) but orphan postings may remain; a restart loads no
//!   backend for that id (the registry blob no longer lists it), so the
//!   orphans are unreferenced dead space — no reader can ever select them.
//!   Retryable.
//! - **In-flight reader:** keeps working against its `Arc<dyn IndexBackend>`
//!   (RCU — the `Arc` keeps the backend and its already-read postings alive
//!   through the sweep). ✅
//!
//! ---
//!
//! # RENAME  (documented; no F-76 visibility bug — RENAME is a re-key, not a removal)
//!
//! RENAME never leaves the index planner-invisible in a wrong-results way: it
//! is a re-key of the SAME logical index, and the index stays queryable
//! throughout (under the OLD name until the swap, under the NEW name after).
//! The concerns below are about error/cancellation ordering, not the F-76
//! visibility window.
//!
//! ## regular (hash) — `rename_index` (create-new-first, drop-old-second)
//! Hash-index keys embed `name_interned` into the hash, so a rename rebuilds
//! the new index from the live record stream and then drops the old. Because
//! CREATE registers the new def FIRST (F-72) and DROP retires the old def
//! FIRST (F-76), there is no window where a concurrent write is invisible to
//! BOTH indexes.
//!
//! ## unique (hash) — `rename_index` (drop-old-then-create under
//! `unique_write_lock` + `UNIQUE_INDEX_CREATE` barrier)
//! Uniqueness validation during backfill is NOT idempotent, so the barrier +
//! lock are held across the ENTIRE drop→create to prevent any writer from
//! inserting a duplicate in the gap. F-70 (#897) fixed the drain/lock order.
//!
//! ## sorted — `rename_index_sorted` (tombstone → rcu swap → rekey → clear)
//! The engine's `rename_index` delegates its sorted branch to
//! `SortedIndexManager::rename_index_sorted`, which writes a durable
//! "Renaming" tombstone (`system:sidx_ren`, recording `old_id → new_id`)
//! BEFORE swapping the definition, then atomically re-points the in-memory
//! definition (RCU) + persists, then `rekey_postings` moves old-id postings
//! to new-id with a settle re-scan loop (catching a concurrent write landing
//! under the old id during the brief window), then clears the tombstone. The
//! tombstone makes an interrupted rekey RESUMABLE on restart: `new` calls
//! `recover_in_progress_renames`, which re-runs the idempotent settle loop for
//! each recorded pair and clears the tombstone (P0-5b / #962).
//!
//! ## index2 — `rename_entry` (by_name mapping + authoritative name-slot update)
//! Physical postings are keyed by the compact `u32` id (NOT `name_interned`),
//! so NO data movement happens. `rename_entry` updates BOTH the `by_name`
//! reverse index AND the authoritative `name`/`name_interned` slots in the
//! `by_id` entry, so `all_descriptors()` (the persistence path) emits the new
//! name and the rename survives a restart (P0-5a / #961). The backend stays
//! `Ready` and queryable throughout.
//!
//! ---
//!
//! # Explicitly DEFERRED follow-up gaps (per the F-76 escape hatch)
//!
//! This slice landed the index2 + regular-hash + unique-hash DROP fixes with
//! red-then-green concurrent-reader tests. The following are documented here
//! as explicit follow-ups rather than rushed:
//!
//! - **Crash/cancel-safety TESTS per family × transition.** The CONTRACT
//!   above is fully specified per family, but the deterministic
//!   crash-mid-sequence tests (prove that an error/cancellation at each
//!   documented failure point leaves exactly the promised state) are not yet
//!   authored for every family's CREATE/DROP/RENAME. The DROP visibility
//!   tests (the P0 correctness bug) ARE landed; the broader
//!   crash/cancellation matrix is the follow-up.
//! - **base_index CREATE auto-self-heal parity with index2.** base_index (regular /
//!   unique / sorted) CREATE leaves an abandoned `Building` def that does NOT
//!   automatically self-heal on restart the way index2 does — it relies on
//!   `doctor::repair()`. Closing this (restart-from-scratch for base_index) is a
//!   separate, larger task.
//! - **A `Dropping` / `Failed` enum variant** if operator-introspection ever
//!   needs to surface mid-teardown / failed-build status (not required for
//!   correctness; see the decision rationale above).
//!
//! See `crates/shamir-engine/src/table/tests/f76_drop_visibility_tests.rs`
//! for the concurrent-reader proofs. Red-then-green sabotage cycles were
//! performed for the regular-hash and index2 families (the two confirmed
//! bugs); the unique-hash DROP is fixed and green-tested but did not receive
//! a separate sabotage cycle (the read planner does not route queries
//! through unique indexes, so the concurrent-reader test checks the
//! definition-retirement + barrier-lowering ordering directly).

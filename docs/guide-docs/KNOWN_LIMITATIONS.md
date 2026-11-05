# Known Limitations

S.H.A.M.I.R. is an alpha-stage database (see the [Project Status](../../README.md#-project-status)
section in the root README). This document is a single, honest,
citation-backed list of the architectural limitations known to exist in the
current codebase. Every bullet cites the source file/line (or the
already-published reference doc) that backs the claim, so the list stays
verifiable rather than aspirational.

This is not an exhaustive bug list — it covers **structural** limitations:
things the current design does not attempt to solve yet, as opposed to bugs
that will be fixed without a design change. Where a limitation has a tracked
follow-up, that is noted as "planned, see roadmap" rather than an internal
task id (task IDs are internal to the development process, not a public
artifact).

## P34-03b — physical table-store naming compatibility

Table catalogue records may contain an optional node-local `store_id`, and tables created or renamed by a new binary use incarnation-qualified physical store names. New binaries open old id-less catalogue rows and old store names unchanged; old binaries are not guaranteed to open ID-bearing tables. Rolling back after new CREATE/RENAME requires restoring a pre-change backup. No migration or orphan-store purge is performed by this change.

## P34-03e — rename-in-doubt fence has no clean rollback exit

Once a rename attempt ends in doubt, the rename fence stays raised and there is no "known rolled back" exit from it short of a process restart: a fenced retry that the catalogue answers `NotCommitted` transfers back into doubt (the fence is re-raised), so the pair can never be proven rolled back from inside the running process. Additionally, every fenced retry copies the source table's three stores again, and each failed attempt leaves those copies behind; orphaned store copies accumulate until cleanup runs. Sources: `crates/shamir-engine/src/repo/instance/repo_instance/catalog/rename_attempt.rs`, `crates/shamir-engine/src/repo/instance/repo_instance/catalog/rename_fence.rs`, `crates/shamir-engine/src/repo/instance/repo_instance/catalog/rename_freeze.rs`.

## 1. Transactions

- **Write acknowledgements wait for contiguous settlement, not just their own
  projection.** For a transaction that allocates a version and ordinary
  non-transactional writes, the visibility wait requires W to reach that version:
  every version through it must materialize or abort. Read-only/empty commits
  that allocate nothing skip this wait. One stuck lower version can indefinitely
  delay every higher acknowledgement in the same repository; the five-second
  warning and 50 ms recheck are not give-up timeouts. Sources:
  `crates/shamir-engine/src/tx/commit.rs:587-604`,
  `crates/shamir-tx/src/repo_tx_gate.rs:1049-1116`,
  `crates/shamir-tx/src/mvcc_store/mod.rs:788-790,865-866,995-996`.
- **Post-WAL projection failures wait for online orphan replay.** Failed
  visibility projections leave the version unsettled and wake the drainer;
  acknowledgement waits for replay and contiguous W progress, rather than
  treating WAL publication as visibility. Persistent replay failures can keep
  the wait pending. On `ReopenRequired`, transactional outcomes become
  `Deferred` (`materialized: false`), but the non-transactional acknowledgement
  helper only logs and returns normally without establishing visibility. Neither
  is permission to blindly retry committed writes. Sources:
  `crates/shamir-engine/src/tx/materialize.rs:255-273`,
  `crates/shamir-engine/src/tx/drainer.rs:1190-1211`,
  `crates/shamir-engine/src/tx/commit.rs:597-603,619-629`,
  `crates/shamir-tx/src/repo_tx_gate.rs:1110-1116`.
- **`AsOf(v)` can wait indefinitely for an allocated, unsettled version.**
  Callers must cancel the read if they need a bounded wait; cancellation does
  not settle a version or substitute Latest. An unallocated future version is
  rejected (`as_of_future_version`); a pending position requiring reopen yields
  `as_of_reopen_required`. Positions already at/below W do not wait. Sources:
  `crates/shamir-engine/src/table/read_temporal.rs:85-101`,
  `crates/shamir-tx/src/repo_tx_gate.rs:1055-1107`,
  `crates/shamir-storage/src/error.rs:117-118`.
- **Snapshot/Serializable table reads trade shortcuts for correctness.**
  Latest-state counter, secondary-index, MIN/MAX and sorted-order shortcuts
  are bypassed inside these transactions: reads scan the pinned MVCC snapshot
  plus the staged overlay, so large tables pay O(N), with sorting when needed.
  Pessimistic reads retain latest-committed state with locks; non-transactional
  reads retain their applicable shortcuts. This describes ordinary latest-mode
  table reads, not explicit temporal reads or EXPLAIN. Sources:
  `crates/shamir-engine/src/table/read_exec.rs:341-365`,
  `crates/shamir-engine/src/table/table_manager_streaming.rs:375-401,651-735`.
- **Snapshot/Serializable vector similarity trades ANN for exact ranking.**
  Bare vector similarity and a single vector conjunct with residual predicates
  rank over the pinned snapshot plus staged overlay, not the latest ANN graph:
  O(N*d) distance work plus sorting. `_id` ordering and keyset pagination also
  use the snapshot-aware scan. Sources:
  `crates/shamir-engine/src/table/filtered_vector.rs:101-121`,
  `crates/shamir-engine/src/table/read_exec.rs:344-379,1965-2012,947-967,1021-1025,1053-1074`,
  `crates/shamir-engine/src/table/table_manager_streaming.rs:357-402`.
- **Index-only transactions are outside the writer-drain protocol.** Commit
  enters table drain guards by iterating `tx.write_set.keys()`, not
  `index_write_set`; an index-only transaction therefore does not participate
  in that drain/fence mechanism. Do not generalize data-writer drain guarantees
  to index-only commits (`crates/shamir-engine/src/tx/pre_commit.rs:504-527`).
- **Execution time budgets are cooperative, not preemptive.** A stalled
  operation is not interrupted at its deadline. Checks stop subsequent dispatch
  and run before a transactional batch's commit decision; once commit starts it
  is not cancelled by the execution timeout
  (`crates/shamir-engine/src/query/batch/exec_scope.rs:151-161`,
  `crates/shamir-engine/src/query/batch/batch_execute.rs:806-819`). Runtime work
  is enforced across nested/dynamic dispatch, but counts aliases, not rows or
  instructions (`exec_scope.rs:85-115`, `batch_execute.rs:560,687`).
- **Runtime materialization byte enforcement is ON by default in the server**
  (`security.query_limits.enforce_result_size`, default `true`) and OFF in
  embedded/WASM facade execution. It bounds the bytes MATERIALIZED by one
  execution (a conservative estimate, not the serialized response length or
  RSS); the server also refuses an encoded response larger than the
  request/server-clamped `max_result_size`. Cursor pages have a separate
  serialized cap. Intermediates and nested/ForEach containers consume one root
  budget, with no give-back when results are dropped
  (`crates/shamir-server/src/db_handler/config.rs`,
  `crates/shamir-query-types/src/batch/exec_policy.rs`,
  `crates/shamir-engine/src/query/batch/result_size.rs:12-18,143-145`).
- **A post-execution `result_too_large` is not proof of rollback.** Encoded
  response enforcement runs after execution and subscription activation, so
  rejection can follow a committed write and omit its transaction metadata.
  Reconcile the outcome; do not blindly retry. Serialized cursor pages instead
  use `cursor_page_too_large`, not `result_too_large`. Sources:
  `crates/shamir-server/src/db_handler/handler.rs:595-605,631-642,747`,
  `crates/shamir-server/src/db_handler/cursor_handlers.rs:315-322`.
- **Nested string_agg inherits the root accumulator cap.** A smaller nested
  batch `max_result_size` does not lower this cap. Each group's accumulator is
  capped separately (root result-size cap, at most 100 MiB); separator is capped
  at 1024 bytes. Accumulator overflow is a query error with a
  `result_too_large: ResultBytes …` diagnostic, **not** `LimitExceeded`, so it
  does not invoke the TxExecute budget-error handle-removal rule
  (`crates/shamir-engine/src/query/batch/query_runner.rs:1193-1196`,
  `crates/shamir-engine/src/query/read/aggregate.rs:936-1008`,
  `crates/shamir-server/src/db_handler/tx_handlers.rs:211-218`).
- **Explicit NULL aggregate input is distinct from absent input on the wire.**
  For both aggregate variants, present JSON null / MessagePack nil becomes
  `Some(FilterValue::Null)` and is evaluated as a per-row literal NULL;
  absence retains the legacy `field`/All source. Literal-NULL SUM returns NULL,
  without changing the legacy field-input SUM identity. Sources:
  `crates/shamir-query-types/src/read/select.rs:8-16,76-85,109-118`,
  `crates/shamir-engine/src/query/read/aggregate.rs:1382-1408,1414-1424`.

- **One repository per transaction.** A transactional batch (or an
  interactive/open transaction) whose queries span more than one repo is
  rejected with `BatchError::CrossRepoNotSupported`. See the guard in
  `crates/shamir-engine/src/query/batch/batch_execute.rs:126-132`
  (one-shot transactional batches) and the mirrored guard in
  `crates/shamir-engine/src/query/batch/interactive_tx.rs:80-87`
  (interactive/open transactions).
- **No savepoints / nested transactions.** A transactional sub-batch (a
  `Batch` op with `transactional: true`) nested inside an already-open
  transaction is rejected (`nested_tx_not_supported`). See
  `crates/shamir-engine/src/query/batch/query_runner.rs:330-341`.
- **A WASM `Call` inside an open transaction is rejected.** A `Call`
  operation delegates to the `FunctionInvoker` with autocommit semantics —
  its writes would commit independently of the enclosing transaction,
  silently breaking atomicity. This is rejected explicitly
  (`call_in_tx_not_supported`) rather than allowed to run. See
  `crates/shamir-engine/src/query/batch/query_runner.rs:709-736`
  (guard at lines 720-727).
- **Transactional DDL is not supported.** Admin operations (create
  table/index, schema changes, etc.) are always delegated to the
  `AdminExecutor` outside of any `TxContext` — they never run inside an
  open transaction's commit pipeline, transactional or not. See
  `crates/shamir-engine/src/query/batch/query_runner.rs:697-698`
  ("Admin ops — delegate to AdminExecutor (no tx)").
- **Read-your-own-writes (RYOW), current behavior.** Streaming scans
  (`list_stream_tx`/`filter_stream_tx`) and the match-scans behind
  `execute_update_tx`/`execute_delete_tx` overlay a transaction's own
  staged `write_set` on top of the committed-store stream: a staged
  insert made earlier in the SAME transaction is visible mid-scan, a
  staged update yields the staged (new) bytes, and a staged delete is
  hidden even though the committed store still has the row. See the doc
  comments in `crates/shamir-engine/src/table/table_manager_streaming.rs:170-186`
  and the merge algorithm in `crates/shamir-engine/src/table/tx_scan_overlay.rs:1-29`.
  - **Residual limitation: no SSI predicate/range locking over streams.**
    A concurrent OTHER transaction's phantom insert into a range this
    transaction is scanning is NOT detected — full SSI predicate/range
    locking over a stream is a separate, harder problem and remains out
    of scope. See the "Streaming-scan SSI scope" doc comment at
    `crates/shamir-engine/src/table/table_manager_streaming.rs:163-168`.
    (One specific instance of this general gap — a concurrent insert
    racing a reverse-FK RESTRICT/CASCADE/SET NULL/ON UPDATE check — is
    now closed for the implicit/autocommit path via targeted Serializable
    isolation + footprint widening; see the `foreign_key` bullet in §2
    "Schemas" below for the closure and its residual scope.)
  - **`AsOf`/`History` temporal reads do not get the transaction overlay.**
    `read_as_of`/`read_history` in `crates/shamir-engine/src/table/read_temporal.rs:45,209`
    take no `TxContext` parameter at all — point-in-time historical views
    are exempt from RYOW by design (they answer "what did this look like
    at version/timestamp X", not "what does the current transaction see
    right now").

## 2. Schemas

- **`default`/`auto_now`/`auto_now_add` apply to single-segment (top-level)
  field paths only.** A rule with a multi-segment path is rejected at DDL
  time with `nested_path_transform_not_supported` — the write path
  (`apply_defaults`/`apply_transforms`) only ever honors single-segment
  paths, so this DDL-time guard prevents a rule from being silently
  accepted and then silently ignored on every write. See
  `crates/shamir-db/src/shamir_db/execute/admin_schema.rs:164-199`.
- **`unique` constraint: single field only.** `unique` is a per-field
  boolean rule (`Option<bool>`); there is no composite/multi-field unique
  constraint. See
  `crates/shamir-query-types/src/admin/types/schema_ops.rs:108-112`.
- **`foreign_key`: single field, same-repo target only.** A foreign key
  references exactly one field in one parent table, and that parent table
  must live in the same repo — there is no cross-repo foreign key. See
  `ForeignKeyDto` in
  `crates/shamir-query-types/src/admin/types/schema_ops.rs:143-151`
  ("The parent table name (flat, same repo)"), and the FK semi-join
  primitive in `crates/shamir-engine/src/validator/validator_db.rs:176-215`.
  No composite FK, no deferred constraints, and no self-referential
  cascade exist either.
  - **The reverse-FK check TOCTOU (RESTRICT/CASCADE/SET NULL/ON UPDATE
    checking a stale view of the child table) is now CLOSED for the
    implicit (autocommit) path — CLOSED (F-28, #821, in 5 steps
    #828-#832).** The originally documented gap: a concurrent insert into a
    child table, landing between a parent DELETE/UPDATE's reverse-FK
    check and that operation's own commit, could create a dangling
    reference the check had no way to see. Closed via two mechanisms
    covering two distinct sub-gaps, plus two additional bugs found and
    fixed along the way:
    - **In-transaction read-your-own-writes (D1) — F-28 Step 2 (#829).**
      A transactional `[delete child; delete parent]` batch under
      `on_delete = Restrict` was wrongly rejected, because the reverse-FK
      probe/plan functions (`fk_restrict.rs`, `fk_actions.rs`,
      `fk_on_update.rs`) read via a plain committed-store scan and so saw
      the not-yet-committed child delete as still present; conversely
      CASCADE could silently orphan a child inserted earlier in the same
      transaction. Fixed by threading `tx: &TxContext` through those
      checks so they read via `list_stream_tx`/`read_one_tx_bytes(Some(tx))`,
      which overlays the transaction's own staged writes on the committed
      stream.
    - **Cross-transaction race (the original TOCTOU) — F-28 Step 5
      (#832), mechanism S3-C.** A genuinely concurrent OTHER transaction's
      insert into the child table, racing the parent operation's
      check-then-commit window, is now closed via targeted Serializable
      isolation + SSI footprint widening (decided over a per-table
      barrier-lock alternative in the Step 3 spike memo, see
      `docs/dev-artifacts/research/f28-s3-mechanism-decision.md`): an
      implicit delete/update on an FK-parent table with a non-`NoAction`
      action is upgraded from `Snapshot` to `Serializable` isolation at
      begin time (recording a real `PredicateDep::TableScan` on the
      reverse-FK probe), and any write into an FK-child table publishes
      an SSI footprint token via `require_footprint_for` regardless of
      that writer's own isolation level — so the two sides always have
      something to conflict against. Whichever side commits first wins;
      the loser aborts with `PhantomConflict`/`tx_conflict` (with a
      bounded internal retry for the common case), and no interleaving
      can leave "delete succeeded AND a dangling child reference exists."
      Proven via deterministic end-to-end tests in
      `crates/shamir-engine/src/query/batch/tests/fk_race_closure_tests.rs`.
    - **Two additional, independent bugs found and fixed along the way:**
      D2 (F-28 Step 1, #828) — autocommit inserts/updates into an
      FK-constrained table were wrongly rejected because the implicit-tx
      path passed the reverse-FK resolver as `None`, fixed by inverting
      control on `RepoInstance::begin_implicit_batch_tx`/
      `commit_implicit_batch_tx`; and D1 above (Step 2), listed here
      again for completeness since it was discovered, not pre-supposed,
      during this campaign.
    - **Two more bugs in this same cache mechanism — found and closed by
      the 2026-07-27 readonly review, not during the F-28 campaign.** The
      cache that gates the Serializable upgrade above (S3-C) had two
      latent bugs neither this entry nor F-28 knew about: (1) F-35 (#843,
      commit `44c4317e`) — each cached `ReverseFkEntry` populated its
      single role-flag field from `fk.on_delete` only, so an
      `on_delete = NoAction, on_update = Restrict/Cascade/SetNull` FK never
      flagged its parent for the Serializable upgrade on the implicit
      UPDATE path, silently reopening the cross-transaction race for
      `on_update` specifically; `ReverseFkEntry` now tracks `on_delete` and
      `on_update` independently, and the isolation-upgrade hook is split
      per operation kind (see
      `crates/shamir-engine/src/repo/fk_reverse_cache.rs`). (2) F-36 (#844,
      commit `d3d06c82`) — `get_or_build_by_parent` unconditionally
      published a scan's result even if a concurrent `invalidate` (from
      FK-schema DDL) raced it, so a scan started before a DDL could finish
      after it and publish a stale snapshot (this cache directly gates
      isolation upgrades, so a stale snapshot could silently reopen the
      dangling-reference race); the cache is now generation-safe
      (single-flight `build_lock` + compare-and-publish). F-47 (#858) closed
      the narrow residual window this originally left open (the post-scan
      generation compare and the publish were two separate atomics, not one
      CAS) by merging the generation and the cached indices into a single
      `ArcSwap<VersionedState>`, so the publish is now one atomic
      pointer-identity `compare_and_swap`.
    - **Residual scope.** This closes the race for the IMPLICIT
      (autocommit) delete/update path, and (F-40, #848, commit
      `5679edfa`) the two hooks that gate it
      (`require_footprint_if_fk_child` /
      `implicit_tx_isolation_for_fk_parent` in
      `crates/shamir-engine/src/query/batch/query_runner.rs`) now fail
      CLOSED on a discovery error — a `resolve_repo` or cache-build
      failure widens the footprint / upgrades to Serializable instead of
      silently falling back to the permissive behavior the pre-F-40 code
      used. The SEPARATE explicit-Snapshot gap is now CLOSED too (F-40b,
      #855/#856): an EXPLICIT transaction the caller opens as `Snapshot`
      is now protected by an isolation-independent "RI barrier" — every FK
      reverse-check scan (`fk_restrict.rs::child_has_reference`,
      `fk_actions.rs`'s cascade probes, `fk_on_update.rs`'s on-update
      probes) records its child `table_token` into
      `TxContext.ri_barrier_tokens` regardless of isolation, and every
      live commit-pipeline Phase 2-bis guard
      (`pre_commit_locked_validate`, `pre_commit_locked`) re-checks those
      tokens via the existing `predicate_conflicts_batch` /
      `record_conflicts` machinery, so a
      concurrent committer that touched the child table in the commit
      window aborts the parent with `PhantomConflict`/`tx_conflict`. (A
      third such guard existed in the dead `group_commit.rs` inter-batch
      phantom check; that unreachable code was removed in F-54, #865.) The
      barrier mirrors the S3-C `footprint_tokens` pattern applied to the
      validation direction (a Snapshot parent-side mutation now re-checks
      what a Snapshot child-side writer publishes), and is load-bearing
      for the same commit-lock serialization as Serializable
      (`commit.rs`'s lock acquisition widens on non-empty
      `ri_barrier_tokens` **and** — closed by F-46, #857, commit-lock
      widening also keys off non-empty `footprint_tokens` — so a plain
      Snapshot writer publishing INTO the child table during the parent's
      own commit-lock window is now excluded too, not just the parent-side
      barrier check; before F-46 such a writer could slip its publish in
      between the parent's Phase 2-bis check and its own publish, defeating
      the barrier's mutual-exclusion intent from the writer's side). Proven
      via deterministic end-to-end explicit-tx race + quiescent tests for
      all four actions (RESTRICT / CASCADE / SET NULL / ON UPDATE), plus
      F-46's `concurrent_child_publish_during_parent_commit_window_is_serialized_on_delete_{restrict,cascade,set_null}`
      / `..._on_update` tests proving the mutual-serialization direction, in
      `crates/shamir-engine/src/query/batch/tests/fk_ri_barrier_tests.rs`.
      The two memos that scoped and proved the mechanism are
      `docs/dev-artifacts/research/f40-explicit-snapshot-ri-gap-memo.md`
      (rejects a mid-flight isolation upgrade as unsound, scopes the RI
      barrier) and `docs/dev-artifacts/research/f40b-ri-barrier-spike.md`
      (settles the flat `TFxSet<u64>` token shape and `"tx_conflict"`
      error-code reuse). A client wanting transparent race resolution for
      an explicit-tx `tx_conflict` retries on that code (the engine's
      explicit-tx API is intentionally retry-free — the client owns the
      lifecycle); the wire code is already the standard `"tx_conflict"`
      every SSI-class conflict uses.
    - See the "TOCTOU caveat" / "Cross-transaction race — CLOSED" doc
      comments in `crates/shamir-engine/src/query/batch/fk_restrict.rs`
      for the full mechanism writeup this summary is drawn from.
- **Renaming a table with a bound declarative schema is rejected.** The
  auto-bound schema validator is registered under a name that embeds the
  table path, so a rename would orphan it; the guard refuses up-front. See
  `crates/shamir-db/tests/rename_migration/renames/rename_table_e2e.rs:139-144`.
- **A failed schema activation no longer leaks an in-memory validator —
  CLOSED (F-24, #817).** `ShamirDb::compile_table_schema`
  (`crates/shamir-db/src/shamir_db/shamir_db/schema_management.rs`)
  registers the schema validator as its FIRST side effect, then resolves the
  table and binds the validator. Before F-24, a failure in a LATER step
  (`get_table` / `add_validator_binding`) returned `Err` while leaving the
  freshly-registered validator permanently orphaned in
  `ValidatorRegistry::by_id` / `name_to_id`. The caller
  (`admin_schema.rs`) rolled the CATALOGUE back to `rec_prev` but had no way
  to undo the in-memory registration. This was not purely a cosmetic leak:
  for a table's FIRST schema declaration that failed this way, a RETRY of the
  same DDL minted a NEW `schema_validator_id` (the rolled-back catalogue
  carries none), took the `replace_artifact(&new_id)` branch (the orphan
  still held the name), silently no-op'ed (the new id was not registered), and
  ended up with the catalogue pointing at an id that had NO entry in
  `ValidatorRegistry::by_id`. **Investigated + corrected severity:** this is
  NOT the "silent schema-validation bypass" a first read suggests — the write
  path's main validator gate (`run_validators_loop` in
  `crates/shamir-engine/src/table/table_manager_validators.rs`) is
  FAIL-CLOSED on a `get_by_id` miss, surfacing as
  `DbError::ValidatorInvalid("validator <id> not found in registry
  (fail-closed)")` and rejecting the write; so the practical impact was an
  AVAILABILITY bug (the affected table became permanently unwritable until
  the dangling catalogue reference was repaired), not silent wrong data.
  (`schema_defaults` / `schema_transforms` do silently skip on a miss, but
  they run before the fail-closed gate, so the write is rejected anyway.)
  F-24 closes it at the source: a FRESH registration is undone
  (`ValidatorRegistry::remove`) if any later step in the same call fails,
  restoring the registry to its pre-call state; and `replace_artifact`'s
  return value is now checked so any future path that could recreate a stale
  name collision fails loudly. See
  `crates/shamir-db/src/shamir_db/shamir_db/tests/schema_rollback_tests.rs`.
  **The ALTER-path counterpart is now ALSO closed — CLOSED (F-27b, #827).**
  F-24 deliberately left an ALTER's `replace_artifact` never undone on later
  failure, reasoning that the catalogue-level `rec_prev` rollback (still
  pointing at the same `schema_validator_id`) covered it — but
  `replace_artifact` swaps the LIVE compiled validator in place the instant
  it runs, well before a later step's failure is known, so the registry kept
  enforcing the NEW (never-fully-activated) rules while the rolled-back
  catalogue said the OLD schema was active: a real persisted/live state
  divergence, not just a memory leak. F-27b closes it by capturing the live
  artifact via `ValidatorRegistry::get_by_id` immediately before
  `replace_artifact` runs, and restoring it with a second `replace_artifact`
  call if a later step fails — so the registry's live artifact always
  matches what the catalogue believes is active.
- **The "migration" API changes the storage engine, not the schema.**
  `StartMigration`/`CommitMigration`/`RollbackMigration`/`MigrationStatus`
  copy a table's raw `data_store` bytes to a new backend keyed by
  `dst_engine`/`dst_repo` — this is storage-engine migration (e.g. moving
  a table to a different backend), not schema evolution (there is no
  column add/rename/drop-with-data-transform facility here). See
  `crates/shamir-db/src/shamir_db/execute/admin_migration.rs:19` (entry
  point) and the `dst_engine` resolution at lines 90-98.
- **The migration API is experimental, opt-in only, and DISABLED BY
  DEFAULT.** `StartMigration` is rejected with a structured
  `experimental_feature_disabled` error unless an operator has first
  called `ShamirDb::enable_experimental_migration_api()`; the live
  `shamir-server` only calls this when an operator explicitly sets
  `security.enable_experimental_migration_api: true` in the config (F-15,
  #808 — default `false`, and no shipped `deploy/*.ktav` profile sets it),
  so no regular client can trigger a migration against a default
  deployment. The source is **frozen for writes** from snapshot preparation
  until migration completion: inserts, updates, deletes and pre-start staged
  transactions committing during the freeze are rejected with
  `table_migrating`. Rollback, failed Start and Commit exits release the
  freeze. This replaces write interception, not the remaining limitations:
  - **Only `dst_engine: "in_memory"` is supported.** Destination data does
    not survive restart; this is not a durability-preserving migration.
  - **Limited restart recovery, not absent persisted state.** The coordinator
    uses `persist_state` / `load_state` in the source info store, whose
    durability depends on its backend. Commit retries recover persisted
    state and finalisation markers when the destination is still available.
    The active map and freeze guard remain process-local; metadata alone
    cannot reconstruct a lost in-memory destination or guarantee crash-safe
    resumption of an in-flight migration. Commit rejects an uncommitted
    coordinator without its original freeze (`migration_restart_required`),
    rather than refreezing and silently omitting newly admitted source writes.
    Committed retries finalise the fixed destination only. After restart,
    Rollback cannot recover a missing active-map entry; orphan cleanup and a
    fresh migration remain operator work.
  - **Migration on a replication follower is unsupported.** Replicated apply
    bypasses ordinary write admission and therefore bypasses the source freeze;
    there is no reliable follower-role check at this admin entry point.
  - **FK cascades into a frozen table abort the parent write.**
  - **DDL on the frozen source is not blocked.**
  - **No list-all-migrations / user-facing post-commit history API.**
    Persisted completion receipts support idempotent Commit retries, not
    a migration-history listing.
  See `crates/shamir-db/src/shamir_db/shamir_db/core.rs`
  (`enable_experimental_migration_api` / `experimental_migration_enabled`)
  for the gate and `crates/shamir-db/src/shamir_db/execute/admin_migration.rs`
  for the entry-point check. The live-server opt-in added by F-15 (#808)
  lives in `crates/shamir-server/src/config.rs`'s
  `SecurityConfig::enable_experimental_migration_api` (default `false`)
  and is wired at boot in
  `crates/shamir-server/src/server/server_launcher.rs` (the
  `if config.security.enable_experimental_migration_api { ... }` block).

## 3. Indexes

- Persisted vector descriptors support the in-process HNSW backend only. Legacy metadata using removed backend ordinal 1 fails decoding; the engine create path writes only `InProcessHnsw` (`crates/shamir-engine/src/table/table_manager/admission/domain/index_admission/write_domain_index_admitted.rs`).

- **Index identity is canonical bytes (identity v2); older binaries cannot
  open FTS or functional indexes written by it.** Regular and unique
  postings, FTS token postings and functional postings identify a value by
  its canonical encoding (the frozen codec in
  `crates/shamir-index/src/model/identity/`): the encoding itself when it is
  at most 256 bytes, otherwise `0xFF || BLAKE3-256(encoding)` with the
  encoding kept in the posting value. Every equality decision is made on the
  canonical bytes, so a crafted hash collision can no longer produce a false
  unique conflict or a wrong lookup hit, and functional Map identities no
  longer depend on insertion order. Residual risk: a BLAKE3-256 collision of
  two distinct long values is computationally infeasible; if one ever
  appeared it fails closed with an internal error instead of being treated
  as a duplicate.
  **Migration** is automatic and blocking on the first open after the
  upgrade, once per table, O(rows × indexes). The base family is marked by
  `_m.idx.idv` (= 2; the legacy `_m.idx.lfv` is no longer written) and every
  regular and unique definition is swept in both key namespaces and rebuilt
  in place from the data store; definitions are never dropped before the
  commit point, so a crash repeats the rebuild from scratch on the next
  open. FTS and functional descriptors are re-marked `Building` under
  metadata envelope version 2 and rebuilt by the existing
  restart-from-scratch path. A pre-upgrade WAL index operation replayed
  after the migration triggers a rebuild of that table.
  **Downgrade:** an older binary refuses a table with FTS or functional
  indexes written by identity v2 (envelope version 2). `DROP` those indexes
  with the new binary, or restore a backup, before going back; there is no
  downgrade command. An older binary rebuilds the base family itself and
  stamps the legacy marker, which makes the next new open migrate again.
  A functional index over a user scalar that is not registered at open ends
  `Failed` with a persisted reason; register the scalar and call `repair()`.
  Sources: `crates/shamir-index/src/base_index/infra/index_keys.rs`,
  `crates/shamir-index/src/fts/tokenizer.rs`,
  `crates/shamir-index/src/backends/functional_backend.rs`,
  `crates/shamir-index/src/model/meta/persistence.rs`.

- **`unique` and `sorted` are mutually exclusive on the same index.**
  Creating an index with both `unique: true` and `sorted: true` is
  rejected. See
  `crates/shamir-query-types/src/admin/types/index_ops.rs:12-21`.
- **One vector index per table.** DDL refuses to create a second vector
  index on a table that already has one, regardless of field or
  dimension — `staged_vectors` in `TxContext` keys by table token (not
  index), and post-commit `promote_vectors` fans the same batch of
  vectors out to every vector backend on the table, so two indexes with
  different `dim` would cause a `DimMismatch` on promote. See
  `docs/guide-docs/guide/06-search.md:151-160`.
- **No partial indexes, no TTL indexes, no geo indexes.**
- **A crash during an index2 (`fts`/`functional`/`vector`) build always
  re-does the O(N) backfill on the next table open, even if the crash
  happened at 99% completion — CLOSED crash-restart gap (F-50 Step 3b,
  #873), residual cost documented.** A `CREATE INDEX` on an index2 backend
  is a multi-step sequence (reserve id → persist `Building` marker →
  backfill → register → persist `Ready`). If the process crashes between
  the `Building` and `Ready` persists, the on-disk metadata records a
  half-built index. On reopen, the table-open self-healing path detects the
  `Building` marker, drops the partial backend's postings
  (`IndexBackend::drop_all`), re-runs the full backfill from scratch, flips
  the state to `Ready`, and re-persists — automatically, no operator action
  needed (the doctor's `verify()` additionally surfaces any `Building`
  backend that survived open, e.g. from a backfill that failed under the
  non-fatal `restore_on_open` error policy, and `repair()` can re-trigger
  the same restart-from-scratch for a live table). The **residual cost** is
  that a crash at 99% completion discards the 99% and redoes the entire
  O(N) scan: resume-from-checkpoint was explicitly rejected as
  over-engineering for a rare, operator-driven DDL event (the backfill is
  checkpoint-less and the per-backend ops are not guaranteed idempotent, so
  resume would need a persisted cursor + per-backend idempotency guarantees
  in addition to a range-resume stream variant). See the decision memo at
  `docs/dev-artifacts/research/f50-step3-crash-restart-spike.md` §2 for the
  full restart-vs-resume reasoning, and `IndexState` /
  `IndexDescriptor.state` / `IndexRegistry::set_state` in
  `crates/shamir-index/src/` for the lifecycle-state implementation. This
  O(N) rebuild applies to EVERY `index2` open, not just post-crash
  recovery — see `docs/dev-artifacts/ops/CAPACITY_PLANNING.md`'s "Index &
  cursor sizing" section for measured/extrapolated per-index rebuild
  duration by table size (O(rows × indexes) with multiple FTS/functional
  indexes on one table).
- **`CREATE INDEX` on the `unique`/`sorted`/`index2` families still blocks all
  writers for the ENTIRE backfill scan — a write OUTAGE on medium-to-large
  tables.** (The regular/hash family no longer has this limitation — see the
  next bullet.) `create_unique_index`/`create_sorted_index`/`create_index_v2`
  each still acquire F-70's write barrier (`begin_write_barrier` → raise bit →
  drain → hold `unique_write_lock`) across their WHOLE backfill sequence, so
  every concurrent writer that observes `needs_write_barrier() == true` queues
  on `unique_write_lock` until the build drops the barrier at the end. On a
  table large enough for the scan to take seconds or minutes, this is a
  complete write outage, not a brief pause. The `f78_writer_latency` bench's
  ORIGINAL measurement (still the accurate characterization of these three
  families' behavior today) found: at 5k rows the build took ~150 ms and
  writer p50/p95/p99 ≈ 150 ms (all 64 writers queue for ~(build duration) then
  drain); at **100k rows the build took ~140–160 s and writer p50/p95/p99 ≈
  140–160 s** — a ~2.5-minute write outage. (The scan is superlinear — ~920×
  slower than 5k for 20× the rows — so the stall grows faster than linearly
  with table size; at 1M rows the extrapolation is hours.) See
  `create_unique_index_body`/`create_sorted_index_with_include`/
  `create_index_v2` in `crates/shamir-engine/src/table/table_manager_index_mgmt.rs`
  for the still-current whole-barrier implementations, and the P1-4
  operational-decision brief in
  `docs/dev-artifacts/research/2026-08-03-new-wave-readonly-review.md` §P1-4
  for the full original scope.
  - **The unique family additionally materializes the whole table into memory
    (O(table) peak memory).** F-78 (#905) reduced peak memory for the regular
    family (streaming instead of materializing), but the unique family still
    materializes the whole table (`Vec`) before one `set_many` — duplicate
    detection needs global knowledge, and a sound bounded-memory rewrite is a
    separately-scoped task. See `create_unique_index_body`'s F-78 deferral
    comment in `crates/shamir-engine/src/table/table_manager_index_mgmt.rs`.
  - **Operational recommendation: run `CREATE INDEX` (unique/sorted/index2) on
    large tables during a maintenance window.** For TS/JS client callers, pass
    a generous `requestTimeoutMs` (or `0` to disable) on the `execute`/
    `Batch.execute` call that carries the `create_index`/`create_unique_index`/
    `create_sorted_index` op — the default 35 s client timeout will abort the
    request long before a 100k-row build completes. There is NO server-side
    per-DDL timeout, so the only timeout that can fire is the client's. See
    the JSDoc on `createIndex` in `crates/shamir-client-ts/src/core/builders/ddl/`.
  - **Progress visibility + post-crash recovery.** The backfill now emits
    periodic `log::info!` progress lines (rows processed so far, elapsed time)
    so an operator watching logs can confirm the DDL is progressing, not hung.
    A `Building` index left behind by a crash or a cancelled build is surfaced
    by `TableManager::verify()`/`doctor::repair()` (#966) — `verify()` reports
    it as unhealthy with a diagnostic message, and `repair()` rebuilds it from
    scratch. For the regular/hash family specifically, `repair()`'s rebuild
    still uses the whole-barrier path (see the next bullet's note on
    `doctor::repair()` scope) — only the interactive `create_index` DDL got
    the online-build treatment.
- **Regular/hash `CREATE INDEX` no longer blocks writers for the whole scan —
  online build (RFC, #1018, landed #1087-#1089/#1060-#1062).** `TableManager::
  create_index` (regular/hash family ONLY — NOT `create_unique_index`,
  `create_sorted_index`, or `create_index_v2`, which remain on the
  whole-barrier path described above) now snapshots a pinned MVCC version,
  scans it BARRIER-FREE (Phase A), captures concurrent writes into a
  dirty-set instead of blocking them, drains that dirty-set in a barrier-free
  catch-up loop (Phase C), and only re-acquires the write barrier for a
  short, bounded final step (Phase D: apply the residual, flip `Ready`).
  Falls back to the old whole-barrier path automatically for tables without
  an MVCC changefeed attached (e.g. system tables). Re-measured with the SAME
  `f78_writer_latency` bench, 2026-08-10 (`CARGO_TARGET_DIR='<dedicated bench target dir>'
  cargo bench -p shamir-engine --bench f78_writer_latency -- --scale 0.1`):
  at 5k rows, build ≈ 148-192 ms, writer p50/p95/p99 ≈ **0-1 ms** (was ≈
  135-160 ms, tracking build duration); at 50k rows, build ≈ 31.6-33.2
  **seconds** (scan itself unchanged — still superlinear, same decode cost as
  before) while writer p50/p95/p99 stays at **0 ms** — completely flat,
  independent of table size. `doctor::repair()`'s regular-family rebuild
  path was deliberately NOT switched to online build (`#1089` found this
  would reopen a serialization race — F-3, #1030 — between concurrent
  `CREATE INDEX` calls and `repair()`'s multi-family drop-then-recreate loop;
  a safe fix needs its own bulk online-build entry point, tracked as a
  follow-up, not attempted here). Crash recovery for an interrupted online
  build (`#1060`) is intentionally conservative for this first landing:
  a crash at any point leaves the index safely `Building` (never falsely
  `Ready`) but does NOT resume — the same manual `TableManager::repair()`
  recovery as before. See `docs/dev-artifacts/research/2026-08-07-online-index-build-rfc.md`
  for the full design and `crates/shamir-engine/benches/f78_writer_latency.rs`
  for the bench and its complete measured numbers.

## 4. Subscriptions

- **Best-effort delivery; a supported subset of filter shapes only.**
  Subscription filters only support
  `Eq, Ne, Gt, Gte, Lt, Lte, In, NotIn, IsNull, IsNotNull, Exists,
  NotExists, And, Or, Not`; anything else (`like`, `ilike`, `regex`,
  `contains`, `contains_any`, `contains_all`, `between`, `field_eq`,
  `fts`, `vector_similarity`, `computed`) is rejected at grant time with
  `subscription_filter_unsupported_operator`. See §7 ("Grant rejections")
  in
  [`client-server-protocol-spec/SUBSCRIPTIONS.md`](client-server-protocol-spec/SUBSCRIPTIONS.md#7-grant-rejections).
- **Live ordering is journal-authoritative per repo.** Healthy subscriptions
  with available history emit nondecreasing commit versions (multiple record
  frames may share a version); broadcast lag triggers journal recovery, not
  an automatic Gap. A journal gap or read failure terminates the subscription.
  Initial snapshots use one read-only Snapshot transaction per repo at cut S;
  version-zero frames precede Ready, and live journal delivery starts strictly
  after S, without snapshot/live handoff duplicates. Snapshot failures terminate
  without Ready. Delivery adds journal-writer latency; no delivery acknowledgement or
  exactly-once guarantee exists across disconnects, and a crash can still
  lose the journal tail (S5c).
- **No durable offsets / resume tokens.** A missed range surfaces as a
  best-effort `gap` push (§9 of the same doc); there is no
  client-presentable resume token that guarantees exactly-once replay.
- **Chronic push backpressure closes the subscription.** Required Event,
  Ready, and Gap frames retain their serialized bytes for up to
  `SLOW_CONSUMER_THRESHOLD` attempts (100), with 5 ms between attempts:
  495 ms of retry sleep per frame, plus executor scheduling latency.
  No event is silently dropped while the subscription stays open. On
  exhaustion the bridge attempts `slow_consumer`, then `closed` (both
  best-effort), and tears the subscription down; the client must reconcile
  out-of-band and re-subscribe. See
  `crates/shamir-server/src/subscriptions/push.rs:76-117` (bounded retry)
  and `crates/shamir-server/src/subscriptions/bridge.rs:588-600`
  (best-effort terminal notification).

## 5. Replication

- **Experimental; not for production data.** Pull-based, async,
  single-leader replication is intended for read replicas, not clustering/HA.
  **`ReplMode::ReadOnly` is not enforced:** a follower accepts local writes.
  See [`guide/08-interconnect.md`](guide/08-interconnect.md).
- **Replicated map field ids are translated, not shared between interners.**
  Pull envelopes carry per-event id-to-name bindings; apply preflights referenced
  ids, persists follower-local bindings, and rewrites Put payloads before apply.
  Missing or conflicting bindings are rejected rather than decoded under wrong
  names. Sources: `crates/shamir-engine/src/tx/replicated_event.rs:7-12`,
  `crates/shamir-engine/src/tx/apply_replicated.rs:148-198,272-297`.
  Existing cross-interner/next-hop coverage:
  `crates/shamir-engine/src/tx/tests/replication_interner_tests.rs:97-148`
  (source inspected, not executed here).
- **Upstream identity pinning.** `[replication].upstream_pins` maps a normalized upstream address (so `tcp://`, `tls://` and bare `host:port` identify the same key) to a list of 64-hex SHA256 hashes of the leader Ed25519 public key. Configured pins are the only durable, verified enrollment. Without one, the first successful connection in each process is trusted (TOFU), logged at `warn` with a ready-to-paste enrollment line, then enforced for the rest of that process; learned pins are not persisted and must be re-learned after every restart. The leader logs its own identity pin at `info` on boot for out-of-band comparison. During leader-key rotation, add the new pin alongside the old one during the 7-day overlap; otherwise the subscription stops with `upstream_key_mismatch`. Sources: `crates/shamir-server/src/sync/replication/transport/prod_factory.rs` and `crates/shamir-server/src/sync/replication/transport/replication_config.rs`.
- **Follower secondary indexes are unsupported and fail closed.** An event
  touching a table with any index (hash, unique, sorted, FTS, functional or
  vector) is rejected before any data or version is applied; the typed
  rejection names the table and index. Index maintenance is a later project.
  **`COUNT(*)` is maintained best effort:** each table's final operation per
  key is compared with its current pre-image, data is applied, then the row
  counter is incremented before the next table. Raw replay and retries after
  an earlier table succeeded do not double-count. Counter increment failures
  are logged, not rolled back or repaired by replay; persistence/crash repair
  and failures after MVCC overlay publication are not atomic with the counter.
- **DDL, schema and metadata are not replicated; only transaction data
  writes are.** Create the same databases, repos and tables on the follower
  first; **do not create secondary indexes there**. A missing follower repo
  causes backoff with `ReplError::UnknownFollowerRepo`
  (`crates/shamir-server/src/replication/follower_loop.rs:198-210`). An
  event touching a missing table is rejected with `UnknownTable` before version
  allocation; the follower loop stops with `ApplyRejected` without advancing
  the bookmark across that event. Sources:
  `crates/shamir-engine/src/tx/apply_replicated.rs:138-146,209-213`,
  `crates/shamir-server/src/replication/follower_loop.rs:389-393`.
- **Journal pulls preserve settled version order; cold recovery reconciles
  events from surviving WAL before serving.** Pulls capture the contiguous
  completion watermark W and confirm a writer barrier before returning
  ascending events `<= W`; a stalled writer can return an older safe prefix
  (`crates/shamir-engine/src/repo/repo_instance.rs:1587-1604`). The persisted
  clean horizon J is loaded from `__changelog__`; absent J is treated as zero,
  while read/decode errors fail the open
  (`crates/shamir-engine/src/repo/changelog_store.rs:26-37`). For each
  surviving v3 WAL entry above J, cold reconciliation inserts its derived
  event if absent; identical persisted bytes are idempotent, while differing
  bytes fail closed. A v3 entry with an event header that derives no event is
  eventless and creates no gap. Old-format/unknown entries and missing
  evidence may require a minimal conservative gap; evidence may be
  insufficient to distinguish an unserved lost commit from a version that
  could have been served, and no granular downstream receipt proves otherwise.
  H is the recovered marker/surviving-WAL gate floor, not a scan of history.
  Cold recovery flushes journal changes and the recovery floor before
  persisting J = H, and refuses open on reconciliation I/O/decode errors.
  Therefore an unclean stop alone does not certify `[J + 1, H]` or require a
  reseed. Successful clean shutdown quiesces commits, flushes data and records
  J only after W = H
  (`crates/shamir-engine/src/repo/repo_instance.rs:1852-1877`). GAP and J
  writes cross an explicit flush boundary; recording J first flushes earlier
  journal writes (`crates/shamir-engine/src/repo/changelog_store.rs:64-85`).
  During runtime, before each local sealed-WAL truncation, the drainer samples
  settled W, reserves and flushes a recovery allocation floor at least J, then
  flushes events/gaps and advances J. A checkpoint error prevents unlink. The
  retained-byte cap is measured on sealed segments; `refuse_writes` rejects
  new admissions until successful truncation brings retained bytes under the
  cap. `declare_gap` persists a real GAP where possible but does not evict WAL
  whose data/history is not known safe; a failed journal writer falls back to
  refusal. Memory/hybrid WAL bypass these controls. Runtime orphan replay is
  implemented, and a channel-full journal (4096 slots) no longer produces an
  unavoidable replication GAP (S5c-X PR5b): the version goes into an
  in-memory repair-pending set (`shamir_tx::changefeed::RepairPending`) and
  the background drainer re-enqueues the event rebuilt from the retained WAL
  entry once the channel has room, with journal-writer barriers and the
  clean-horizon checkpoint ceiling clamped below the oldest repair-pending
  version so no bounded reader silently skips it. Repair-pending state is in
  memory only: an unclean shutdown with versions still repair-pending falls
  back to cold reconciliation from the WAL on restart, which rebuilds those
  events. A repair is impossible once the WAL segment is gone — the retention
  policy (`refuse_writes` / `declare_gap`) still wins when its policy says so
  — and such existing gaps are not cleared by cold reconciliation. Followers have no WAL: since PR6c their cold start certifies through the
  journal horizon J in the bookmark record and marks only the in-flight batch
  (`in_flight_range`) as an uncertain GAP; an unknown J keeps the conservative
  whole-window fallback. Since PR6b-2 the follower apply path is receipt-protected
  (RESOLVED by PR6b-2c): an ordinary I/O failure pauses the batch and the next
  pull resumes the SAME receipt with the identical local allocation — no extra
  versions, no lost counters, no new gaps — so the in-flight batch no longer
  requires a reseed; only corruption/ambiguity stays terminal
  (reopen/quarantine). v1/v2/u64 bookmarks and floor-evidence states are
  REFUSED with the typed `DbError::FollowerStateRefused`
  (`LegacyBookmarkFormat`/`MalformedBookmark`/`LegacyFollowerState`) at open
  and first admission; no migration tooling is provided, so the affected
  subscription must be stopped and the follower reseeded through the existing
  seed/reseed workflow (see the follower receipt bullet below). Live
  subscriptions deliver from this same journal
  (broadcast is only a wake-up hint); a certified gap terminates a subscription
  (see the live-ordering bullet in section 4 and
  `crates/shamir-server/src/subscriptions/bridge.rs`).
- **Follower applies are receipt-protected (PR6b-2c), with residual limits.**
  Retry convergence: an ordinary I/O failure pauses the batch (keeping its
  paused write reservation) and the NEXT PULL RESUMES THE SAME RECEIPT with the
  identical local allocation — no extra versions, no lost counters, no new
  gaps. Pipeline per chunk: Prepared receipt + allocation reservation +
  bookmark-v3 pointer, ONE atomic batch (history rows/timestamps + absolute
  counters + downstream journal records + receipt → Materialized), read-back +
  checkpoint of bookmark+J with receipt retirement in the same batch, then
  publish + broadcast. Corrupt identity/pointer, decode errors, and
  double-Uncertain stay terminal (reopen/quarantine); ordinary failures no
  longer set `requires-reopen`. Activation: WAL-less follower + backend
  `supports_atomic_batch` + a valid bookmark v3 record — fresh adoption and
  seed installation write the v3 baseline before service, and durable follower
  provenance IS that v3 record; v1/v2/u64 bookmarks and floor-evidence states
  are refused with the typed `DbError::FollowerStateRefused` at open and first
  admission, no migration tooling exists, and the operator reseeds.
  Persistent backends without the capability are rejected with
  `AtomicBatchUnsupported` before allocation (wrapper/hybrid backends
  excluded). The leader commit path and the non-follower apply path are
  UNCHANGED; the follower apply path gains one receipt write + one atomic batch
  + one checkpoint sync per batch, split at `MAX_RECEIPT_EVENTS` /
  `MAX_RECEIPT_OPS` rather than weakening atomicity. Limits that REMAIN:
  admission is the repository write-domain permit (DDL and local writes are
  excluded for the pipeline's whole run, and a paused reservation rejects
  ordinary admission with `RepoWriteError::Reserved` until the receipt
  resolves); ts keys mirror `MvccStore` `TS_TAG` handling; the
  `confirmed_through` race note is inherited from the barrier contract; the
  index/vector rejection above (and no posting/vector exactly-once claim) is
  unchanged. See `docs/guide-docs/follower-journal-durability.md` and
  `crates/shamir-engine/src/tx/follower_durability/receipt/`.
- **Detected gaps stop replication, but `Resume` does not repair data.**
  When the leader reports a `gap_at` at or after the requested `from_version`,
  the loop stops before applying the response's events with
  `ReplError::JournalGap`; the supervisor marks the subscription
  `resync_required`, visible via `ReplicationStatus`/`ListSubscriptions`.
  See `crates/shamir-server/src/replication/follower_loop.rs:311-325`,
  `crates/shamir-server/src/replication/supervisor.rs:309-334` and
  `crates/shamir-db/src/shamir_db/execute/admin_replication.rs:413-467`.
  There is **no automatic snapshot/reseed**. While the subscription is
  quarantined (`resync_required`), the destination is also EXCLUDED FROM
  ORDINARY READS (S5c-X PR8b): client reads (get/query/scan, including
  temporal AsOf/History read ops) of any repo covered by that
  subscription's Pull streams fail closed with the typed error
  `BatchError::ResyncRequired` (stable wire code `resync_required`,
  non-retryable), and local changefeed/journal pulls
  (`read_changelog_bounded`, the local subscriber bridge) fail with
  `DbError::ResyncRequired` — the consumer must reseed, not keep tailing
  stale state; the downstream serve refusal (`downstream_resync_required`,
  PR8) is unchanged. Status, `ReplicationStatus`/`ListSubscriptions`
  (which already echo `state` and `status_message` verbatim), DESCRIBE and
  other admin ops stay readable so the operator can see why; the
  quarantine lifts only through the operator path: the row carries a durable
  `quarantined` marker (plus the `resync_from_receipt` baseline), so plain
  admin `Resume` — and even `Pause`/`SetProfile` — of a quarantined
  subscription are refused with the typed `resync_required` error; the
  the procedure is to DROP the quarantined subscription, install a replacement
  seed with the real installer — into a NEW follower data directory (the
  installer refuses an existing ordinary destination repository/database) —
  which creates a FRESH paused subscription bound to the new `seed_receipt`,
  then `Resume` that fresh row, per the
  runbook, or drop the subscription. Runtime clean-horizon checkpoints
  bound the retained local WAL; retention enforcement follows the configured
  refusal or conservative GAP policy. PR4 cold reconciliation means an
  unclean crash alone no longer implies reseeding: a seed is needed when the
  residual certified gap intersects the follower bookmark/next required
  version, or a separate divergence requires replacing follower state. A gap
  can still result from unknown legacy WAL, missing evidence (including WAL
  removed after a retention `declare_gap`), or other unrecoverable loss; exact
  provenance is not always available. `Resume`, clean shutdown and recreating
  the subscription do not clear an existing persisted gap. Full replay from
  zero works only against a gap-free source lineage. To recover, stop
  replication and writes, preserve an offline backup, rebuild a new source
  lineage from authoritative logical data with the same schema, recreate empty
  followers and subscriptions, and replay that new journal. Alternatively use
  a verified, matching follower data image AND its upstream bookmark through
  the affected horizon; an arbitrary older backup or a bookmark advanced
  without the matching data is not a repair. There is no built-in operator
  command for atomic snapshot-plus-bookmark seeding. What IS built in
  (SEED-2..5, offline R2-lite) is a STOPPED-source operator runbook, not an
  online snapshot: `replication-seed-export` / `-verify` / `-install` move the
  leader's logical state at a settled version `V >= certified_gap.through`
  into a NEW follower repository with a paused, receipt-bound subscription,
  and validated `Resume` starts the first pull at `V+1` (runbook:
  `guide/09-seed-recovery.md`; format:
  `guide-docs/architecture/SEED_FORMAT.md`). It does NOT replicate DDL (schema
  divergence is refused), does NOT seed secondary indexes, is Unix-only,
  whole-repository-scoped, requires the leader to be stopped, recovers NOTHING
  the leader itself could not recover, and is not an automatic reseed,
  promotion, or full-server recovery. The CI certification kills leader
  PROCESSES (SIGKILL) and proves process-crash behaviour only — not power-loss
  guarantees.
- **The identity fence is universal but is detection, not repair.** Since
  S5c-X PR7 every subscription (seeded or not) records the leader's
  `(lineage, history epoch)` at first contact and stops terminally
  (`seed_wrong_identity` / `seed_wrong_history_epoch`, recorded and
  observed identities as structured `recorded_identity` / `observed_identity`
  in per-repository `ReplicationStatus`) when a later Hello/Pull reports
  a different one — including a restarted `in_memory`/`hybrid` leader,
  which mints a new lineage at every start because its `__tx__` is
  RAM-only. This stops silent history swaps; it does NOT repair the
  follower's data — recovery stays the operator runbook above, and since
  PR8b ordinary reads and local changefeed pulls of the quarantined
  destination are refused (`resync_required`) until the operator reseeds
  via the DROP + install + Resume procedure described above. Pre-PR7
  subscriptions adopt the first observed identity once (one WARN line).
  Identity lives in the actual advancing bookmark value and is preserved on
  every advance. Missing identity fails closed even on first contact; no
  production disable switch exists. Seed receipts retain stricter schema checks.
- **Under `replication_serve: completed_reseed` the speculative tail may be
  retracted — by design.** This mode serves `min(confirmed, W)` — the
  completed prefix WITHOUT the R clamp — so pages may include observations
  that a later unclean-restart recovery retracts. The guarantee is
  "safe convergence, not lossless replication: speculative observations may
  be retracted, and an unclean restart conservatively costs a reseed even if
  the bytes survived." See `guide/07-operations.md` for the
  lifecycle/epoch model (`Armed` record, clean-shutdown `Clean`
  certificate, recovery completes privately before service is admitted).
- **Under `completed_reseed` an unclean restart costs a reseed even when the
  bytes survived.** On open, an `Armed` record with no later `Clean`
  certificate durably increments `history_epoch` (checked add; a failed
  bump/flush or overflow blocks admission) and invalidates the old
  replication continuity while the lineage stays STABLE. The conservatism
  is deliberate: a served speculative page that later recovery retracts
  must never look converged to a follower.
- **In-memory and Hybrid repositories cannot do better than
  `completed_reseed`.** Nothing survives a restart (the identity store is
  RAM-backed, so every reopen — clean or not — mints a new incarnation /
  new lineage), so they may serve replication only with explicit
  `completed_reseed`; an admin `set_durability` of `wal_commit: synced` on
  them is IGNORED (silently downgraded to `buffered` via `for_wal_backend`,
  with one warn line per open). Under `durable` mode shutdown writes no
  `Clean` certificate. The only ephemeral-backend rule PR8 adds is that
  explicit `completed_reseed` is required at publication time.
- **Connected followers learn of a `history_epoch` bump only on their next
  fenced pull.** The PR7 fence runs at Hello and before every Pull (before
  any decode/apply), so until that next fenced pull a follower keeps its
  old bookmark and its previously served speculative pages: this mode
  advertises speculative consistency, not linearizability. A refused
  bookmark marks the subscription `resync_required` (counter
  `shamir_repl_resync_required_total`) and it stays quarantined until a
  replacement seed is installed (and since PR8b ordinary reads and local
  changefeed pulls of the destination fail with `resync_required` while it
  lasts); legacy peers omitting lineage/epoch
  identity are refused (`MissingIdentity`) and never adopted. Sources:
  `crates/shamir-server/src/replication/identity_fence/mod.rs`,
  `crates/shamir-engine/src/repo/meta/repo_identity.rs`,
  `crates/shamir-engine/src/repo/durability_policy/`.
- **Duplicate loops and bookmark crash risk.** Two subscriptions for the
  same `(db, repo)` start two loops; bookmark advancement is not atomic.
  The bookmark is stored in `__tx__` without flush ordering against follower
  data, so after a crash it may be ahead of the data. This persistence risk
  is **not verified on the on-disk engine**.
- **No on/off switch.** The follower `SubscriptionSupervisor` always boots and
  the leader-side `ReplRequest::Pull` is served to any superuser/replicator
  session (`crates/shamir-server/src/config.rs:99-106`,
  `crates/shamir-server/src/db_handler/repl_handler.rs:52`). Replication is
  inactive until subscriptions and replicator credentials exist — it is not
  disabled by a flag.

## 6. Results

- **Query results materialize fully into a `Vec`; no true server-side
  streaming to the client yet.** `QueryResult.records` is a
  `Vec<QueryRecord>` built and returned in one shot. See
  `crates/shamir-query-types/src/read/query_result.rs:64-66`. This is now
  mitigated on the wire/client side by server-side cursors (`CreateCursor`/
  `FetchNext`/`CancelCursor`, see
  [`client-server-protocol-spec/CURSORS.md`](client-server-protocol-spec/CURSORS.md)),
  which page results so neither side holds the full set in memory over the
  wire at once — but the SERVER still executes a full pinned-version scan
  per page internally (no true server-side streaming cursor at the engine
  level), so server-side peak memory during a single page's execution is
  not reduced by cursors; only wire/client-side memory is. **F-53b (#878)
  narrows this for the common case**: when a cursor's ORDER BY is a
  single-column indexed field AND no concurrent write has touched that
  index since the cursor's pin, the server now uses an AsOf-aware
  sorted-index keyset seek (`read_as_of_keyset_seek`) that scans only
  `O(page_size)` postings per page instead of `O(N)` — the per-page
  scan-cost drops by a factor of `N / page_size` for the dominant
  stable-result-set cursor workload. The full pinned-version scan per page
  still applies to: (a) non-keyset-eligible cursors (multi-column,
  unindexed, or computed-expression ORDER BY); (b) cursors where a
  concurrent write to the indexed field advanced the per-index mutation
  high-water gate past the pin (a one-way ratchet that conservatively
  disables the seek for the cursor's remaining lifetime — the correct
  tradeoff, since a current-state index cannot place a row whose pinned
  posting was moved/removed); and (c) the cursor's own boundary-filter +
  OFFSET pagination shape, which has not yet been converted to emit
  `Pagination::After` (a follow-up task). See
  `docs/dev-artifacts/ops/CAPACITY_PLANNING.md`'s "Index & cursor sizing"
  section for measured/extrapolated cost numbers by table size.
- **Result-size and connection caps (current defaults).** A batch
  response is clamped to `max_result_size_bytes` (default **64 MiB**),
  and the server enforces a global `max_active_connections` cap (default
  **1000**, with a per-source-IP sub-cap default of 100). See
  `crates/shamir-server/src/config.rs:288-318` (`max_result_size_bytes`
  default) and `:330-366` (`max_active_connections`/
  `max_active_connections_per_ip` defaults).
- **`max_inflight_response_bytes` (RI-15's global in-flight response-byte
  budget) — code-level default is finite (F-29, 256 MiB).** The global cap
  on the SUM of in-flight response bytes across every
  concurrently-executing batch/connection now defaults to
  `4 * max_result_size_bytes`'s own default (256 MiB), not unbounded — this
  matches the shipped `server.medium.example.ktav`/
  `server.small.example.ktav` profiles' own convention. `ByteBudget::acquire`
  (`crates/shamir-server/src/byte_budget.rs`) never hard-errors when the
  budget is exhausted — it waits (bounded, wakes on release) — so a
  deployment relying on the old unbounded default gets bounded backpressure
  under sustained saturation, not a new rejection path, after upgrading. An
  operator who genuinely needs unbounded behavior can still opt back in by
  setting `security.query_limits.max_inflight_response_bytes: null`
  explicitly in their `.ktav` (an omitted key resolves to the finite
  default; an explicit `null` still resolves to `None`/unbounded — these are
  distinguishable because `#[serde(default = "...")]`'s default function
  only runs when the key is absent, not when it is present-but-null); the
  server logs a `tracing::warn!` at boot whenever it observes this resolved
  `None`, since it is now a deliberate escape hatch rather than an
  unexamined default. See `crates/shamir-server/src/config.rs`'s
  `default_max_inflight_response_bytes` and
  `crates/shamir-server/src/server/server_launcher.rs`'s boot-time
  `ByteBudget::new` construction.
- **Cursors only support `Temporal::Latest` reads.** `AsOf`/`History`
  queries are rejected outright at `CreateCursor` with
  `cursor_temporal_not_supported`, not silently downgraded to `Latest`. See
  `crates/shamir-server/src/db_handler/cursor_handlers.rs`'s
  `create_cursor` and
  `crates/shamir-query-types/src/batch/batch_error.rs`'s
  `BatchError::CursorTemporalNotSupported`.
- **Cursors do not support `with_version: true`.** CR-B5: `CreateCursor`
  rejects `query.with_version == true` outright with
  `cursor_with_version_not_supported`, the same "reject, don't silently
  downgrade" discipline as the `Temporal::Latest`-only scope cut above — a
  cursor's every internal read runs at a pinned `Temporal::AsOf` snapshot,
  and that read path does not attach per-record versions, so honoring the
  flag would silently produce no versions instead of the real per-record
  stamps a plain (non-cursor) `with_version: true` read returns. See
  `crates/shamir-server/src/db_handler/cursor_handlers.rs`'s
  `create_cursor` and
  `crates/shamir-query-types/src/batch/batch_error.rs`'s
  `BatchError::CursorWithVersionNotSupported`.
- **Keyset-mode cursors: `Null`/missing and `Bin`/`List`/`Dec`/`Big` `ORDER BY`
  values are handled; mixed-type `ORDER BY` values are handled for
  schema-typed scalar columns whose rule was bound while the table was
  empty (F-17 #810 tightened this from F-1 #792's original, unverified
  claim); `NaN` `ORDER BY` values are mitigated (no keyset attempt) but
  not detected (CR-D2 #783, W-2/W-3 #789, F-1 #792, F-17 #810).** A
  keyset cursor's page boundary is an inclusive `field >= seek_key` (ASC) /
  `field <= seek_key` (DESC) filter — any row whose `ORDER BY` value cannot be
  compared to `seek_key` makes that filter unresolvable (`false`), silently
  excluding the row from every page after the first with a clean `has_more:
  false` at the end (no error). Current state:
  - **`Null` / missing value — CLOSED.** `CreateCursor` now runs one cheap
    `WHERE <order_by_field> IS NULL LIMIT 1` existence probe against the
    same pinned snapshot the first page reads, before running that first
    page. If it finds any row, the WHOLE cursor is pinned to row-count-offset
    pagination from creation, instead of keyset mode — closing this case
    unconditionally. See
    `crates/shamir-server/src/db_handler/cursor_handlers.rs`'s
    `order_by_column_contains_null` and `create_cursor`.
  - **`Dec`/`Big` value — CLOSED (W-3, #789).** These have no `FilterValue`
    equivalent, so the boundary filter could not even be built — before this
    fix, every `FetchNext` past page 1 hard-errored ("cursor: keyset seek key
    has no comparable filter form") instead of silently dropping rows. Now
    detected the moment the candidate bookmark value is extracted (both at
    `CreateCursor` and at each `FetchNext` bookmark refresh): an unconvertible
    value is treated exactly like the pre-existing "no seek_key" case, and the
    cursor degrades to row-count-offset pagination for that call only. See
    `crates/shamir-server/src/db_handler/cursor_handlers.rs`'s `safe_seek_key`.
  - **`Bin`/`List` value — CLOSED (W-2, #789).** Unlike `Dec`/`Big`, these DO
    convert to a `FilterValue` (`Binary`/`Array`), so the boundary filter looked
    valid but could never match anything (`compare_values` has no comparison
    arm for `Bin`/`Bin` or `List`/`List`), silently dropping every row past page
    1 — the same failure shape as the `Null` case. Investigated extending
    `compare_values` with a real total order for these types (mirroring
    `ORDER BY`'s own sort-key machinery): `ORDER BY` does NOT actually sort
    `Bin`/`List`/`Set`/`Map` today (they land in an explicit "unsortable"
    bucket, order preserved via stable sort only), so there is no existing
    total order to reuse, and inventing one would need to change `ORDER BY`'s
    own semantics too to stay self-consistent — out of scope for this
    polish-batch task. Fixed instead via the SAME "detect as unsafe, fall back
    to the existing no-seek-key safety net" mechanism as `Dec`/`Big` (see
    `safe_seek_key`) — narrower than a full total-order fix, but fully closes
    the SILENT ROW LOSS (converts it into the documented, understood
    offset-mode degradation instead). **F-18 (#811) additionally excludes
    `Bin` from the schema-typed-scalar gate's accepted `TypeTag` set itself**
    (`Int`/`Bool`/`String` only, `List` was already excluded there as a
    container type) — since `compare_values` never had a `(Bin, Bin)` arm, a
    `Bin` column could never actually benefit from `PaginationMode::Keyset`;
    it only paid for the null-probe read before degrading to the fallback
    anyway. A schema-typed `Bin` column now pins `PaginationMode::Offset`
    from `create_cursor` time, same as `Dec`/`Big`/`List` — `safe_seek_key`'s
    per-value `Bin` exclusion above is no longer reachable for it (kept in
    the code as defense in depth; still exercised as-is by `List`, `Dec`, and
    `Big`, none of which changed).
  - **Mixed `QueryValue` type in one `ORDER BY` column (e.g. some rows
    `Int`, some `Str`) — CLOSED for schema-typed scalar columns whose rule
    was bound while the table was empty (F-1 #792, tightened by F-17
    #810); STILL OPEN for schemaless columns and for a schema declared
    onto an already-populated table's column.** F-1 (#792) added a gate
    (`order_by_column_is_schema_typed_scalar`) that trusted a bound
    schema rule's non-container scalar `TypeTag` (originally `Int`/`Bool`/
    `String`/`Bin`, narrowed by F-18 #811 to `Int`/`Bool`/`String` — see the
    `Bin`/`List` bullet above) as proof of column homogeneity — but
    `add_schema_rule`/
    `set_table_schema` (`crates/shamir-db/src/shamir_db/execute/
    admin_schema.rs`) validate a new rule's SHAPE only and never
    backfill-validate the table's EXISTING rows, so a schemaless table
    with mixed-type data that later got a schema rule bound after the
    fact would have made the F-1 gate return `true` (Keyset enabled)
    even though pre-existing rows aren't homogeneous — `compare_values`
    has no cross-type comparison arm, so those rows silently vanish from
    later pages. F-17 (#810) closes this properly: each schema rule now
    carries a server-computed `keyset_safe` proof, stamped
    `table.count().await? == 0` at the exact moment the rule is bound
    (persisted in the catalogue; preserved, not recomputed, when an
    unchanged rule is re-declared via upsert-by-path). The keyset gate
    now requires BOTH an accepted `TypeTag` AND `keyset_safe == true`.
    A schema bound BEFORE any row is written is genuinely, provably
    homogeneous (schema enforcement has covered 100% of the table's
    history) and keeps reaching real `PaginationMode::Keyset` exactly as
    before. A schema declared onto an already-populated table's column
    is NOT proven safe for that column and falls back to
    `PaginationMode::Offset`, same as a schemaless column, until that
    rule is proven safe some other way — a full retroactive
    backfill-validation scan is out of scope for this task, tracked
    separately if ever needed. See
    `crates/shamir-server/src/db_handler/cursor_handlers.rs`'s
    `order_by_column_is_schema_typed_scalar` and
    `crates/shamir-db/src/shamir_db/execute/admin_schema.rs`'s
    `stamp_keyset_safe`.
  - **`NaN` in an `F64` `ORDER BY` column — STILL OPEN, but MITIGATED (no
    keyset attempt at all), schema or not.** Checked `FieldRule::check_f64`
    (`crates/shamir-engine/src/validator/schema/field_rule.rs`): the schema
    validator's `F64` type check does NOT reject `NaN`, so schema enforcement
    cannot close this gap the way it closes the mixed-type gap above — even a
    schema-declared `F64` field can still hold a `NaN` value. `NaN`'s
    `partial_cmp` always returns `None` (`compare_values`'s `(F64, F64)`
    arm), so a `NaN`-valued row would still be silently dropped the same way
    if this column ever reached the keyset boundary-filter scheme; `NaN`
    additionally breaks the keyset tie-run counter's equality check (`f64`'s
    `PartialEq` on `NaN` is always `false`). F-1 (#792) mitigates this
    WITHOUT detecting `NaN` directly: `F64` (along with `Dec`/`Big`/
    containers/`Any`) is excluded from the schema-typed-scalar gate's
    accepted `TypeTag` set entirely, so an `F64` `ORDER BY` column —
    schema-typed or not — can no longer reach keyset mode at all. The column
    always paginates via row-count-offset instead, so the "silent loss"
    framing no longer applies to it — it never runs a `NaN` value through
    `compare_values`'s boundary-filter comparison in the first place. A real
    NaN-detection fix (a new cheap primitive, or a two-phase scan design: a
    keyset phase over comparable values plus an offset-bookmarked tail phase
    for the rest) remains explicitly out of scope. See
    `crates/shamir-server/src/db_handler/tests/cursor_handler_tests.rs`'s
    `nan_order_by_value_forces_offset_mode_no_loss` for the regression test
    covering this mitigation.
  - **W-7 (#789): the residual mixed-type/`NaN` gap can also DUPLICATE
    rows, not just omit them — this is now moot for `F64`/excluded-`TypeTag`
    columns.** The original W-7 finding was that if a keyset cursor over an
    incomparable-value column later fell back to row-count-offset pagination
    for some OTHER reason mid-scroll (e.g. CR-D1's tie-run-ceiling fallback),
    `state.offset` would undercount the true position in the global sorted
    order — earlier keyset pages had silently SKIPPED the incomparable rows
    via the boundary filter without ever counting them into `offset` —
    causing a resumed offset scan to re-return already-handed-out rows in
    addition to (still) omitting the incomparable ones. Since F-1 (#792), an
    `F64` column (or any column excluded from the schema-typed-scalar gate)
    never enters keyset mode in the first place, so there is no
    keyset-then-offset transition mid-scroll for `state.offset` to
    undercount across — this specific duplication mechanism cannot trigger
    for those columns anymore. It remains a theoretical concern only for a
    schema-typed `Int`/`Bool`/`String` column (F-18 #811 narrowed the
    accepted set from `Int`/`Bool`/`String`/`Bin`, see above) combined with
    some OTHER keyset→offset trigger (e.g. CR-D1's ceiling fallback), which
    is an orthogonal, already-accepted mechanism unrelated to value-type
    incomparability.
- **Idle-cursor reaper could reap a cursor mid-`FetchNext` — CLOSED (F-9
  #799, residual fully closed by F-20 #813).** The reaper evicts a cursor
  once it has been idle past `idle_ttl`
  (`crates/shamir-server/src/cursor_registry.rs`), but `bump_activity()`
  (the only thing that advances a cursor's idle clock) fires exactly once,
  at the very END of a successful `FetchNext` — so a `FetchNext` whose own
  execution time (large `page_size`, an expensive scan, a slow keyset
  boundary search) exceeded `idle_ttl` looked exactly as idle to the reaper
  as a genuinely abandoned cursor, even though it was being actively used
  the whole time, risking the reaper yanking the cursor's `Arc` (and its
  pinned `SnapshotGuard`) out from under an in-flight fetch. F-9 (#799)
  first mitigated this with a `state().try_lock()` probe: a cursor was only
  reaped if its pagination-state mutex was NOT currently held, since
  `fetch_next` holds that mutex for the fetch's entire duration. This
  closed the big window (an entire `idle_ttl`-sized race) but left a
  narrower residual, explicitly documented at the time: `try_lock()` was
  only checked when the reaper COLLECTED the candidate id, in a separate,
  earlier pass from the one that actually REMOVED it — a new `FetchNext`
  could look up the cursor and be about to lock `state()` (nothing locked
  yet, so `try_lock()` would still have succeeded) in the single-reaper-
  tick-sized gap between those two passes, and still have its cursor
  reaped out from under it. F-20 (#813) closes this fully: an `in_flight`
  atomic counter on `Cursor`, incremented by the new
  `CursorRegistry::get_owned_for_fetch` WHILE the registry's per-entry
  read-guard is still held (before `fetch_next` ever reaches
  `state().lock()`), covers the complete lookup-to-completion window with
  no gap; and the reaper's collect-then-remove two-pass sweep was replaced
  with a single atomic-per-entry `sweep_and_reap`, using `DashMap::retain`
  so the expiry check, the `in_flight` check, and the actual removal all
  happen while that entry's shard write-lock is held — no separate pass,
  no window for a new fetch to land in between "decided expired" and
  "removed". `try_lock()` was removed as a reap-gate (superseded by
  `in_flight`, which covers a strict superset of its window) rather than
  kept alongside it. See `crates/shamir-server/src/cursor_registry.rs`'s
  `sweep_and_reap`/`get_owned_for_fetch`/`FetchLease` doc comments for the
  full ordering argument.
- **`max_inflight_response_bytes` missing from the base
  `deploy/server.example.ktav` reference config — CLOSED (F-21, #814,
  closing the F-11 #802 residual this bullet originally tracked as
  open).** The setting already appeared in
  `deploy/server.medium.example.ktav` and
  `deploy/server.small.example.ktav` but was absent from the base
  `deploy/server.example.ktav`, so an operator starting from the base
  reference config alone did not see this knob documented inline. F-21
  (#814) added it at `deploy/server.example.ktav:86`
  (`max_inflight_response_bytes: 4294967296`), sized at 4× the base
  config's own `max_result_size_bytes` (4 GiB / 1 GiB = 4) — matching
  the medium (256 MiB / 64 MiB = 4) and small (128 MiB / 32 MiB = 4)
  profiles' own 4× sizing convention — so all three shipped reference
  profiles now document the knob inline.
- **Corrupt-record reporting now covers every reachable engine read path
  except `table_manager_streaming.rs`'s `filter_stream`/`filter_stream_tx`
  (F-10 #800 + F-22 #815 + F-30 #823).** A row whose value bytes fail to
  decode inside a scan is not aborted (a single corrupt row aborting an
  otherwise-successful query over millions of good rows would be worse
  than a documented gap): it is skipped from `QueryResult.records`
  (unchanged) and reported via `QueryResult.corrupt_records:
  Vec<CorruptRecordRef>` (`crates/shamir-query-types/src/read/
  query_result.rs`) instead of silently vanishing from the result count.
  Each entry is a `{ table, id }` pair — the id is still resolvable even
  though the value failed to decode, since ids are read independently of
  the value payload. The field is omitted from the wire when empty
  (`#[serde(default, skip_serializing_if = "Vec::is_empty")]`), so an old
  peer that doesn't know about it never observes it, and a new peer
  reading an old response gets an empty `Vec` by default.
  - **F-10 (#800):** all ~14 decode-failure sites in
    `crates/shamir-engine/src/table/read_exec.rs` (`read_collecting`,
    `read_counting`, `read_streaming`, the index2 fast path, and the
    filtered-vector-scan / `merge_staged_filtered` /
    `build_filtered_vector_result` trio).
  - **F-30 (#823, this entry's latest extension):** `read_exec.rs`'s two
    byte-level free functions, `try_project_page_only_bytes` (LIMIT
    push-down fallback) and `apply_select_value_bytes` — previously
    documented as out of scope because they have no `QueryResult` directly
    in scope — now take a `&mut Vec<CorruptRecordRef>` accumulator
    parameter (mirroring F-10's own convention) that each of their ~10
    real call sites populates and threads to its own eventual
    `QueryResult` construction: `crates/shamir-engine/src/table/
    read_index_scan.rs` (`read_sorted_index_scan`, `read_order_limit_fast`,
    `read_keyset_seek`, `read_index_scan`'s plain-SELECT branch) and
    `crates/shamir-engine/src/table/read_temporal.rs` (`read_as_of`,
    `read_history`).
  - **Still NOT wired, and expected to stay that way:**
    `crates/shamir-engine/src/table/table_manager_streaming.rs`'s
    `filter_stream` (malformed-`RecordView` skip, `Err(_) => false`) and
    `filter_stream_tx` (its ~line 321-323 counterpart, `Err(_) => false`).
    These return a raw `Stream<Item = DbResult<Vec<(RecordId, RecordCow)>>>`,
    not a `QueryResult` — there is no natural `corrupt_records` sink
    without changing the stream's item type to carry corrupt markers
    alongside matched rows, a real API redesign, not a small fix.
    `filter_stream_tx` has no production caller (only
    `crates/shamir-engine/src/table/tests/filter_stream_tests.rs` and
    `crates/shamir-engine/src/tx/tests/ssi_phantom_tests/
    predicate_capture_tests.rs` reach it) — genuinely dead code, so this is
    moot for it today. `filter_stream` itself, however, DOES have a real
    caller: `TableManager::lock_query_reads`
    (`table_manager_streaming.rs`), the Pessimistic-isolation lock-acquisition
    pre-pass invoked from `read_tx_with_encoding` for any WHERE-bearing
    SELECT run under `IsolationLevel::Pessimistic` (reachable in production
    via `crates/shamir-engine/src/query/batch/query_runner.rs`'s tx-scoped
    SELECT handling) — correcting an earlier investigation that had judged
    both methods unreached. This does not leave a silent reporting gap in
    practice: `lock_query_reads` only uses `filter_stream`'s output to
    acquire per-row locks (it builds no `QueryResult` and has none in
    scope); the SAME query's actual data read runs separately through
    `read_impl` (`read_exec.rs`), which — after this task — reports the
    same corrupt row via `corrupt_records` regardless of which dispatch
    branch it takes. Widening `filter_stream`'s stream item type so the
    lock-acquisition pre-pass can *also* report independently is tracked as
    its own future follow-up if ever needed, not done speculatively here.
  `crates/shamir-engine/src/table/table_manager_index_mgmt.rs`
  does NOT have this pattern (re-verified for F-20/#813: its one `continue`,
  ~line 854 inside the sorted-index rename-migration helper, is a
  malformed-KEY length guard — `if key.len() < 9 { continue; }` — on a
  physical index key during a background index-rename key migration, an
  unrelated defensive skip, not a corrupt-record VALUE silently dropped
  from a query result; it was previously miscited here alongside
  `table_manager_streaming.rs` as having "the same" pattern, which this
  correction removes).
  **SDK surface (F-22, #815, closed):** `corrupt_records` is now typed on
  the TS side too — `CorruptRecordRef` / `QueryResult.corrupt_records?` in
  `crates/shamir-client-ts/src/core/types/write/batch.ts`. Also fixed as part of
  F-22: `CorruptRecordRef.id` now serializes as a base58 string on the
  wire (matching every other `RecordId` on the wire, e.g.
  `InsertedRecord`'s `_id`), not raw msgpack bytes — an F-10-era oversight.
  The Rust SDK (`shamir-client`/`shamir-sdk`) needed no change: both
  return `shamir_query_types::read::QueryResult` directly, so
  `result.corrupt_records` was already available to Rust consumers.
- **`SelectItem::Expression` (computed SELECT fields) is REJECTED at
  execution time, not silently ignored (F-26, #819, closed).** The variant
  is accepted at every layer of the contract — wire DTO
  (`crates/shamir-query-types/src/read/select_expr.rs`), parser
  (`crates/shamir-engine/src/query/read/parser.rs`), and the public TS type
  (`crates/shamir-client-ts/src/core/types/read/query.ts`) — but no evaluator
  exists yet. Before F-26, `SelectProjection::new`
  (`crates/shamir-engine/src/query/read/select_projection.rs`) silently
  dropped the item from the projected output via a bare `_ => {}` catch-all
  — a syntactically valid query with a computed field in its `SELECT`
  returned a result set with that field simply absent, no error. Fixed by
  rejecting the variant with a typed `select_expression_not_supported`
  error at the two production choke points every read plan funnels
  through: `SelectProjection::new` (full scan, index2, temporal, and
  cursor read plans — a cursor's every internal read routes through the
  temporal AsOf path) and `validate_aggregate_select`
  (`crates/shamir-engine/src/query/read/aggregate.rs`, covering GROUP BY /
  aggregate-all queries). The wire shape is kept intact for a future real
  expression-evaluator implementation — only execution-time rejection was
  added.

## 7. Numbers

- **`u64` → `Big` promotion contract.** A `u64` value greater than
  `i64::MAX` promotes losslessly to `Value::Big`/`QueryValue::Big`
  instead of silently wrapping or clamping. `Eq`/`Gt`/`Gte`/`Lt`/`Lte`
  filters and `ORDER BY` correctly match/cross-compare a promoted `Big`
  value, including one stored as raw `uint64` wire bytes (fixed by FG-6 —
  `FilterNode::Compare` falls back to `materialize_at` + `compare_values`'s
  `Big`↔`Str` arm when `scalar_at` can't surface the value directly, and
  `ORDER BY`'s `QvSortKey` gained a numeric `Big` variant). See
  [`client-server-protocol-spec/NUMERIC_WIRE_SEMANTICS.md`](client-server-protocol-spec/NUMERIC_WIRE_SEMANTICS.md)
  for the full contract.
- **Plain `Int`↔`F64` comparison is now exact (CR-D3).** A cross-type
  comparison between a plain `Int` (`i64`, no `Big` involved) and an `F64`
  previously cast the `i64` to `f64` before comparing — since `f64`'s
  52-bit mantissa cannot represent every integer above `2^53`, this could
  silently collapse distinct large `i64` values (e.g. `i64::MAX` and
  `i64::MAX - 1`) onto the same `f64`, corrupting `Eq`/`Gt`/`Gte`/`Lt`/`Lte`
  filters and `ORDER BY` over columns like nanosecond timestamps or
  63-bit ids/hashes. Fixed in both `compare_values`
  (`crates/shamir-engine/src/query/filter/resolve.rs`) and `ORDER BY`'s
  `QvSortKey` ordering (`crates/shamir-engine/src/query/read/order.rs`) via
  an exact bounds-check + `floor`/`fract` technique — no `BigInt` needed:
  `f64`'s 11-bit exponent already covers every integer magnitude up to
  `2^63` exactly at the boundaries (`i64::MIN`/`i64::MAX` bound-check
  against exact `±2^63` `f64` literals), and any in-range `f64`'s
  `floor()` is losslessly castable to `i64`, so the residual tie only
  needs a `fract()` check. `Int`↔`Int` and `Int`↔`Dec` were already exact
  and remain unaffected; `Big`↔`F64` remains a deliberate, accepted
  approximation (see the FG-6/CR-C5 entry above) since `F64` is itself an
  inherently imprecise column type there.

## 8. `ttl_ms`

- **`ttl_ms` governs the in-memory write-back buffer, not
  data-expiration.** `MemBufferConfig::ttl_ms` (in
  `crates/shamir-storage/src/storage_membuffer.rs:104-105`) controls how
  long an entry stays in the RAM cache in front of the durable backend
  before `moka`'s eviction listener flushes it to the inner store — see
  the durability/eviction contract documented at
  `crates/shamir-storage/src/storage_membuffer.rs:48-60`. It is **not** a
  data-expiration/TTL-eviction feature: there is no automatic deletion of
  "expired" records: an evicted entry is written to the durable backend,
  never dropped.

## 9. Backup / Restore durability

- **fsync is applied to copied files, the manifest, each rename's
  containing directory, and (F-19, #812) `backup()`'s own destination
  directory — but this is best-effort power-loss protection, NOT a
  blanket crash-durability guarantee: directory-fsync is a no-op on
  non-unix (Windows), directory-fsync failures are intentionally
  swallowed rather than failing the operation, and true power-loss crash
  recovery is not (and cannot be) unit-tested (F-12, #802; F-19, #812).**
  `backup::copy_dir_recursive` re-opens each just-copied destination file
  and calls `sync_all()` (propagating a failure as
  `BackupError::Io`/`RestoreError::Io` — content durability is the
  point), and `write_manifest` uses `File::create` + `write_all` +
  `sync_all` instead of a bare `fs::write`, so both file content and the
  manifest are durable before `backup()` returns success. Separately,
  DIRECTORY ENTRIES (as opposed to file content) are made durable via a
  `fsync_dir` helper (`restore.rs`, `pub(crate)` — shared with `backup.rs`
  since F-19):
  - `restore()`'s step-5 atomic swap calls `fsync_dir` on the containing
    `parent` directory after EACH successful `fs::rename` (both swap-path
    renames, the rollback rename, and the fresh-target single rename) —
    this closes the classic "you fsync'd the file but not the directory"
    gap on ext4/xfs, where a bare `fs::rename` can return success before
    the directory-entry update is on stable storage.
  - **(F-19, #812)** `backup()` previously left this same gap open for its
    OWN destination directory: `copy_dir_recursive` and `write_manifest`
    synced file CONTENT but never the DIRECTORY ENTRIES they create under
    `dest_dir`. `backup()` now calls `fsync_dir(&dest_dir)` once, after
    `write_manifest` returns successfully — a directory fsync flushes all
    of a directory's CURRENT entry metadata (not just recently-added
    entries), so one call after the last write covers every entry added to
    `dest_dir` by both the copy and the manifest write. `copy_dir_recursive`
    itself additionally calls `fsync_dir` on every directory it writes
    into (once per recursion level, after that level's loop completes), so
    nested subdirectories of a fjall table tree (which `copy_dir_recursive`
    recurses into) get their own entries made durable too, not just
    `dest_dir`'s immediate children. This also benefits `restore()`'s
    step-3 staging copy, which shares `copy_dir_recursive`.
  - Directory-fsync failures (in ALL of the call sites above) are logged
    but **deliberately NOT propagated as errors** — this is an intentional
    design choice (matching `shamir-wal`'s `wal_segment.rs::fsync_parent_dir`
    precedent), not an oversight: a missing directory-entry fsync narrows
    the power-loss window (an as-yet-un-synced entry could revert to its
    pre-operation state after a crash) but does not corrupt already-durable
    file content, and refusing an otherwise-successful backup/restore
    over a non-fatal directory-fsync failure would trade a real, completed
    operation for a strictly worse outcome. Concretely: **a
    directory-fsync failure means the affected directory entries are NOT
    guaranteed to survive a crash occurring before the next unrelated
    fsync of that directory reaches disk** — callers that need a hard
    guarantee here (rather than the best-effort protection this section
    describes) must arrange their own external verification (e.g. running
    `verify_manifest` again after a suspected crash, or a filesystem-level
    `sync`/`fsync` audit) — there is no in-process "strict mode" that
    escalates this to a hard error, and none is planned; see the
    discussion in `docs/dev-artifacts/prompts/post-alpha/
    54-f19-backup-dest-dir-fsync.md` for why a strict-mode config knob was
    considered and deliberately not added.
  - **Windows / non-unix: `fsync_dir` is a documented no-op** — this
    workspace already decided (in `wal_segment.rs`'s `#[cfg(not(unix))]`
    stub) that Windows does not need this specific directory-entry
    durability guarantee; that rationale carries into backup/restore
    unchanged. On Windows, the guarantees this section describes reduce to
    file-content fsync only.
  - **True power-loss crash injection is outside what a portable unit test
    can exercise** (there is no way to truncate the OS page cache
    mid-syscall from a `#[test]`); the regression guards are the
    interrupted-copy test in `crates/shamir-server/src/tests/
    restore_tests.rs` (`copy_step_failure_propagates_and_leaves_data_dir_
    untouched`) and the happy-path `backup()`-destination-directory-fsync
    tests in `crates/shamir-server/src/tests/backup_tests.rs` — both verify
    the closest portable proxy (a failure propagates cleanly / a success
    path with directory-fsync engaged completes without error), not actual
    crash survival. See `crates/shamir-server/src/restore.rs`'s
    `fsync_dir` and `crates/shamir-server/src/backup.rs`'s `backup`,
    `copy_dir_recursive`, and `write_manifest`.

## 10. Repo engines

- **`hybrid` repo engine — residual scope (F-33, #840).** `CREATE REPO
  ... ENGINE 'hybrid'` is a working feature: table configuration
  (index/sorted-index/functional-index definitions, schema validators,
  buffer config, the field-name interner) durably mirrors to disk while
  table data stays fully ephemeral in-memory (wiped on every restart).
  Two residuals to be aware of:
  - **Not yet a supported `dst_engine` for `MigrateRepo`/
    `StartMigration`.** The `dst_engine` resolution accepts only
    `"in_memory"`; a request for `dst_engine: "hybrid"` is rejected with
    *"Migration dst_engine 'hybrid' not yet supported. Supported:
    in_memory"* (same catch-all that rejects every other non-`in_memory`
    engine). See `crates/shamir-db/src/shamir_db/execute/admin_migration.rs:112-120`.
  - **Requires the `fjall` cargo feature (same as the durable default).**
    The `Some("hybrid")` arm in `create_repo` and the `"hybrid"` arm in
    `factory_from_meta` are both gated by `#[cfg(feature = "fjall")]`.
    A build with `default-features = false` and `fjall` excluded cannot
    create a hybrid repo: `ENGINE 'hybrid'` falls into the
    unsupported-engine error path in that configuration, same as `fjall`
    itself would. See `crates/shamir-db/src/shamir_db/execute/admin_db_repo.rs:223-254`
    (create-time arm, including the `data_root`-required guard) and
    `crates/shamir-db/src/shamir_db/shamir_db/core.rs:700-707`
    (restart-reattach arm).

## 11. Scale and performance envelope

No comparative benchmarks have been published; the numbers below are internal
measurements on one development host and are a floor, not a promise.

- `CREATE INDEX` on the unique / sorted / FTS / functional / vector families
  blocks all writers for the whole backfill (§3): ~150 ms at 5k rows,
  ~140–160 s at 100k rows (`f78_writer_latency`); the scan is superlinear, so
  1M rows extrapolates to hours. Only regular/hash builds online.
- FTS and functional indexes are rebuilt from data on every table open (every
  restart): O(rows × indexes). Vector indexes restore from their snapshot.
- Non-keyset cursor pages re-scan the pinned snapshot in full (§6).
- `ORDER BY` / `GROUP BY` / `DISTINCT` materialize the full matching set
  server-side before the first page.
- `$cond` / `$expr` are ~29×–190× slower than a flat comparison
  (`crates/shamir-engine/benches/cond_expr_eval.rs`).
- Benchmarks in the repository go up to 100k rows per table; larger tables
  have no published measurements (`docs/dev-artifacts/ops/CAPACITY_PLANNING.md`).

## Token API

The v1 token API adds HTTP REST, native WebSocket, browser WebSocket, and password-login issuance paths. See the [token API operator guide](guide/token-api.md).

### Client coverage

- **Not every client implements every handshake x transport cell the protocol admits.** Gaps (tracked as T9-8b): the napi binding has no session over the browser-profile WebSocket (`tls_no_export`), no resume over plain TCP and no resume over local IPC (the Rust core covers all three since T9-8a, the TypeScript clients since T9-8c and T9-8d). Changing the password over a local IPC session from the TypeScript client is not verified and is expected to fail. The full table with per-cell status and the test that proves each cell is in the [client matrix](guide/client-matrix.md). Token over TCP/IPC, session over HTTP, QUIC and mTLS are not part of the protocol.

### Credential lifecycle

- **Bearer tokens are plaintext credentials.** A stolen token can be used until expiry or revocation. The remote-plaintext write rail (`allow_plaintext_write: false`) does not keep a `read_write` secret safe: the secret is exposed before the server rejects it with 403 and emits `api_token_exposed`. Protect tokens as secrets, use TLS outside loopback, and avoid URLs. Query-string token authentication is ignored; a valid `Authorization` header still works alongside a query string. Creation returns the secret once; listings omit it. Sources: `crates/shamir-server/src/security/tokens/cli/secret_output.rs`, `crates/shamir-server/src/security/perimeter/`.
- **`kick_session` is not a supported API-token revocation mechanism.** TOKEN_API §5 specifies no token effect. The legacy connect helper at `crates/shamir-connect/src/server/ops/admin.rs:154-170` bumps the shared credential floor, but only integration-test callers were found and runtime behavior is unverified. Explicit token revocation is supported.
- **Shared credential invalidation has a strict timestamp boundary.** Tokens are invalidated when `created_at_ns <= tickets_invalid_before_ns`; validity requires `created_at_ns > floor`. Password changes, actual role/flag changes, and `revoke_user_tickets` advance the shared floor; user deletion removes the owner's identity from the cache, while restore advances the floor. A `kick_session` helper also bumps the shared floor, but production callers were not found and runtime behavior is unverified; do not rely on it for API-token revocation. Sources: `crates/shamir-server/src/security/tokens/service/token_service.rs:544-557`, `crates/shamir-server/src/sync/durability/restore.rs:394-398`, `crates/shamir-connect/src/server/ops/admin.rs:154-170`.
- **A lost login response can leave a token active.** If issuance completes but the response is lost, the client may not know the secret while the server-side credential remains valid until TTL or administrative revocation. Treat uncertain issuance as a credential-cleanup incident; no client acknowledgement protocol is claimed here.

### Protocol and policy boundaries

- **REST is request/response only.** Interactive multi-request transactions, cursors and subscriptions are unsupported over REST (`not_supported_over_rest`). Single-request transactional Execute batches remain available; use token WebSocket for interactive/session features. Source: `crates/shamir-server/src/security/perimeter/http_rest/scope.rs:63-75,97-105,185-187`.
- **No cross-origin browser REST/CORS support in v1.** The token listener does not provide CORS headers. Same-origin access is an implementation/deployment possibility without CORS, not a verified runtime claim. Browser WebSocket has Host/Origin policy controls. Sources: `docs/guide-docs/client-server-protocol-spec/ADMIN_UI_HOSTING.md:86,123`, `crates/shamir-server/src/security/perimeter/http_ws/browser/`.
- **Scoped-token global listing interpretation remains BLOCKED SPEC.** The implementation denies top-level `ListDatabases` and `ListUsers` for tokens with a non-null `databases` list (including an empty one), while `databases: null` tokens may use them; (`crates/shamir-server/src/security/perimeter/http_rest/scope.rs:157-164`); `AccessTree` allows an explicitly allowlisted database and denies absent/out-of-scope databases (165-178). Do not present unresolved global-list interpretation as normative permission.

### Shutdown and evidence limits

- **Shutdown is not a request-completion guarantee.** The observed bounds are 5 seconds for connections, `min(request_timeout, 7s)` for workers, and 2 seconds for audit, subject to an outer 10-second deadline; these are not a guaranteed additive grace period. Forced shutdown can interrupt work. Sources: `crates/shamir-server/src/boot/config/http_mapping.rs:30`, `crates/shamir-transport-http/src/server/accept.rs:368-398`, `crates/shamir-server/src/engine/server/server_launcher/http/launcher.rs:432-445`, `crates/shamir-server/src/engine/server/server_handle.rs:192-218`.
- **T7 evidence gaps are not proven runtime failures.** Its BLOCKED response-byte-guard row means no real-socket assertion/counter seam demonstrated permit ownership through body write; SCRAM-store absence is likewise unproven because no public store/seam permits direct proof. T7 DONE/PARTIAL entries indicate source assertions, not that tests compiled or passed. Linux and Windows CI were not observed there.
- **Audit and memory guarantees are limited.** Response-write permit ownership through actual body completion, credential redaction in blocking threads, per-listener audit queues, and a full measured memory bound remain unresolved or unproven. A global process-audit event semaphore bounds logical events; it does not remove per-listener queue allocations or establish a full-memory bound. Sources: `crates/shamir-server/src/security/perimeter/http_login/service/audit.rs:11-22,76`; see `docs/dev-artifacts/audits/2026-11-02-token-auth1/DESIGN.md`.

### Other status

- **More T7 and AUTH1 evidence gaps.** T7: the privilege sweep over real sockets is PARTIAL (fixture decode round-trip not run), the WebSocket revoke race is a post-revoke refusal check without an in-flight WS request, and the no-sleep rule is not audited suite-wide. AUTH1: transport refusals before the login hook, body-read refusals and timeouts are not counted in login metrics; there are no cross-tests through the native wire and admin dispatch; the `shamir_login_http_pair_slots` gauge has no decrement on store drop or observer replacement, so several servers in one process can exceed the 8,192 slot capacity; input and pending pins together can exceed 3,072 of 4,096 table buckets, in which case a debit that cannot be charged is answered 503.
- **Fjall status is unverified.** Reviewed material provides no current evidence to classify a Fjall-specific flake as present or closed; reassess if current CI evidence is reported.

## 12. Security defaults

- New resources are open (`0o777`) until `chmod`-ed
  (`crates/shamir-types/src/access.rs:135-136`).
- Server-side Rust→WASM compilation (`compiler://`, default `0o755`) is
  available to every authenticated user and runs `cargo` on the host; the
  forbidden-macro guard is a lexical scan, not a sandbox
  (`crates/shamir-wasm-host/src/compile.rs:8-47`,
  `crates/shamir-db/src/shamir_db/shamir_db/access_control.rs:163-173`).
- The TypeScript client neither verifies the server's Ed25519 identity nor
  uses TLS channel binding
  (`crates/shamir-client-ts/src/core/types/connection.ts:43-49`); use a
  CA-verified certificate outside local development.
- No at-rest encryption (`security/data-protection.md`); the audit log covers
  authentication events only.

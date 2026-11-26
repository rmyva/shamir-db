# Brief: ENGINE-CUT-TX (task #1903), round 2 - the run was cut by the 6-hour cap before your final report: report, then close what is open

You continue session `ct-1` in the same worktree (`<worktree>`, branch `engine-cut-tx`). Your first run ended at the default 6-hour hard cap of the runner while it waited for a sub-agent; it never composed a final reply (the runner says: `final_text` is empty after 1 sub-agent fan-out call, 22 sub-sessions hold about 49000 characters of results). The tree holds your work uncommitted: the new crate `crates/engine/shamir-engine-tx`, the facade shims, the CI and script edits. Brief 166 (`docs/dev-artifacts/prompts/release/166-engine-cut-tx-r4c.md`) is still the contract; read it again, including its Compile / build / test policy and the machine wrapper rules, which apply unchanged.

## What to do, in this order
1. **Recover the results.** Query your sub-sessions (the runner told you how: `rush sessions show/last`, or the `inspect_agent` tool) and read what each worker reported: moved trees, removed upward edges, widened items, CI and script edits, gate results. Do not trust a worker's claim: spot-check the claims that matter against the tree (a file moved, a path re-export present, a gate exit printed after the LAST edit).
2. **Establish what is open.** Walk the 7 acceptance items of brief 166 one by one against the tree: say DONE with the evidence, or OPEN with the exact file and item. A gate result only counts if it was produced after the last edit of the files it covers.
3. **Close what is open** with the smallest edits, no new restructuring, within the same rules (file-system moves, no logic change, no formatting sweep, no `#[allow]`, widenings as narrow as the cross-crate path needs). Do NOT re-run a gate whose recorded result already follows the last edit of its inputs; run only the gates that are missing or stale, one cargo command at a time, `cargo fmt` first as a WRITE run, then check and clippy in separate invocations, `python -I scripts/ci/check_layout.py` and `python -I <orch>/vis_widen_base.py <worktree> origin/master` last, each exit printed as `name_exit=$?` right after its command.
4. **Write the final report** (brief 166, section Report, items 1-7) as your own top-level reply, in ENGLISH, complete and standalone, BEFORE anything else can cut the run: write it as soon as step 2 is done if step 3 will take long, then append the outcome of step 3 in a second message.


## State when this round starts (orchestrator, verified)
The orchestrator staged and COMMITTED your tree as `3a1541133` (`refactor(engine): the tx layer becomes the crate shamir-engine-tx`) after two mechanical fixes: the stray `crates/engine/shamir-engine-tx/src/mod.rs` was removed (an unreachable tracked file; the 7-entries limit of `src/` was exceeded) and `src/test_seams.rs` moved to `src/commit/test_seams.rs` (`lib.rs` points at it by `#[path]`). `check_layout.py` is OK; `vis_widen_base.py` reports exactly one widening (`MATERIALIZE_ATTEMPTS`, `pub(crate)` to `pub(in crate::tx)`). Your edits of this round stay uncommitted on top of that commit.

The orchestrator then ran `cargo clippy -p shamir-engine-tx -p shamir-engine -p shamir-engine-table -p shamir-engine-repo -p shamir-db -p shamir-sync -p shamir-server --all-targets -- -D warnings`. It stopped at the FIRST crate, `shamir-engine-tx` (lib), with 11 errors, so nothing downstream was checked yet and more errors are expected after these are fixed:
1. `unused import` `shamir_engine_query as query` and `shamir_index::base_index as index` at `src/lib.rs:6` and `:11`.
2. `unused import` `crate::table::fk_reverse_cache::FkReverseCache` at `src/commit/core/catalog_access.rs:8`.
3. `unused imports` `in_doubt::RenameInDoubt` and `retained_freeze::RetainedFreeze` at `src/commit/rename_fence/mod.rs:12-13`.
4. `unused imports` `commit_batch_tx`, `commit_tx_with_cancel`, `commit_tx_with_kind`, `release_batch_locks` at `src/lib.rs:31` (the `internal_ack` re-export).
5. `unused imports` `index_scope::is_supported_posting_key` and `v2::recover_inflight_v2_strict` at `src/recovery/mod.rs:9-10`.
6. `file is loaded as a module multiple times` `src/recovery/clean_horizon/checkpoint/test_seam.rs`, loaded by `src/lib.rs:13` through `#[path]` as `clean_horizon_test_seam` AND by its own parent module.
7. Two `E0308` in that same file, lines 18 and 19 (`&mut MutexGuard<State>` where `&mut State` is expected).
Each import may be used only by a downstream cfg (`test`, `test-util`) or by the facade: decide per import by reading, then gate it with the same `cfg`, move it to its user, or delete it; never silence with `#[allow]`. Run the cargo gates in the dependency order and one crate per invocation (`-p shamir-engine-tx` first, then the next) so a failure names the next crate instead of hiding it behind the first.

## Time
This run has a cap of 4 hours; compose the report no later than 3 hours after the start, whatever is open. Give each worker one small chunk with exact paths; wait for workers with `await_tasks` and never end a turn on a sub-agent fan-out without composing a reply.

## Not yours
Unchanged from brief 166: the rust-guard campaign and baseline files (`.rust-guard/stages.toml`, `.rust-guard/stages/`, `.rust-guard/layers*.toml`), `Cargo.lock` hand edits, `crates/server/**` and `crates/engine/shamir-db/**` unless a compile error forces a one-line path fix (report each). Other executors edit `crates/server/**`, `crates/query/shamir-query-types/**` and the `shamir-db` database management files right now: do not touch them.

⛔ NEVER run `git reset` / `checkout` / `clean` / `stash` / `restore` / `rm` / `add` / `commit`, or any git command that mutates the working tree or index. Only edit files; the orchestrator stages and commits. Read-only git (`status`, `diff`, `log`, `show`) is allowed. Never write a private machine path into any file; leave no scratch files.

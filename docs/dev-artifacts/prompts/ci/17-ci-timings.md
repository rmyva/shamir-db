You work ONLY inside this git worktree: the current directory (a linked worktree of shamir-db on branch
ci-timings-1870). Read the root CLAUDE.md (binding). The brief below is the task; it is committed at
docs/dev-artifacts/prompts/ci/17-ci-timings.md.

# Brief: CI timing artifacts and a baseline document (CI-TIMINGS)

## Why

We are about to split `shamir-engine` into several crates and to speed up the Windows integration job partly to
shorten CI. We need numbers before and after, and today the pipeline stores none: only logs. Known from one full run
(an engine-touching PR): the critical path is `cargo test integration (windows-latest)` = 12m15s build + 632 s test
run; on ubuntu 6m46s + 96 s. `test build archive` is 784 s (windows) / 593 s (ubuntu). The compile of the workspace
from `shamir-engine` upward is one long sequential chain (engine, engine-query, engine-validator, shamir-db,
transports, server-security, sync, shamir-server), which `cargo build --timings` would show per crate.

## What

1. In the jobs that compile tests (`test-build`, the integration job, and the archive action
   `.github/actions/build-test-archive/action.yml`), capture machine-readable timings and upload them as workflow
   artifacts with a short retention (14 days):
   * cargo's `--timings` report (HTML and, if the toolchain supports it, JSON) for the test build;
   * nextest's JUnit report for each test run (configure it in `.config/nextest.toml` under the `ci` profile or the
     profile the jobs use; keep every existing setting, group and timeout unchanged).
   Pin any new action by commit SHA like the existing steps (a supply-chain gate checks this); prefer actions already
   used in the repo. Artifact names must be unique per OS and job. A failing upload must never fail the job
   (`continue-on-error` or `if-no-files-found: ignore`, your choice, with a reason in a comment).
2. Document `docs/dev-artifacts/perf/ci-timing-baseline.md`: what is recorded and where; how to read the timings
   report; a table "before the split" with the numbers above (critical path, build vs run, per OS) and empty
   "after" columns; the method for the median full-run duration over several PRs (the data comes from the run
   list of the CI provider; give the exact `gh run list` / `gh run view` fields to use, without naming any
   repository). Per-binary test time on Windows vs Linux (sum of test times, seconds): shamir-server::e2e 622/102,
   shamir-server::admin 434/91, shamir-db::schema_ddl 364/20, shamir-e2e::matrix 339/30, shamir-e2e::batch 141/8,
   shamir-e2e::interner 90/6, shamir-e2e::cursor 68/11, shamir-e2e::internals 65/5. Put them in the same document.
3. Do not change test selection, budgets, timeouts or job dependencies. The pipeline's Python tests under
   `scripts/ci/tests/` must keep passing; if one inspects the workflow text, update it in step.

## Observable acceptance

Workflow YAML is valid and the Python tests that parse it still hold (read them and reason about them); layout
passes; in the next CI run the artifacts appear (the orchestrator checks).
Non-goals: any optimisation, any change of what runs.

## Constraints (binding)

Compilation, builds and tests run in CI, run by the orchestrator, never on this machine. Banned: `cargo test`,
`cargo nextest`, `./scripts/test.sh`, `cargo build/run/bench/doc`, `npm`/`tsc`/`vitest`, wasm builds, e2e and
integration harnesses, loom, any release or full-workspace build, any command whose purpose is to build the project
or run its tests. Allowed locally, scoped to the crates you touched and nothing else:
`cargo fmt -p <crate>` (and `-- --check`), `cargo clippy -p <crate> --all-targets -- -D warnings`,
`python -I scripts/ci/check_layout.py`, read-only git (`status`, `diff`, `log`). Run cargo only under
`wnice -p idle -c 50 -t 8 -m 50 --` with `-j 2`. A change you cannot verify here is verified by the orchestrator's CI
run: say in your final report exactly what must be observed there. If you believe you need a build or test run,
say so in the final report instead of running it.

Repo rules (read the root CLAUDE.md, it is binding): at most 7 entries per folder, at most 1000 lines per source
file, `mod.rs` re-exports only, no inline `#[cfg(test)]` modules, no local `use`, no new `#[allow]`, no visibility
widening, `shamir_collections::THasher` for hash maps, comments terse. Never write private machine paths, user
names, or the names of any CI infrastructure other than the workflow files into repository files. Never raise
versions. Do not touch code unrelated to the task.

⛔ NEVER run `git reset` / `checkout` / `clean` / `stash` / `restore` / `rm`, or any git command that mutates the
working tree or index. Only edit files; the orchestrator commits. (`git add -A` only for the layout check.)

Your last message must be a final report: what changed (files), what you verified locally (fmt/clippy/layout only),
what is unverified and must be observed in CI, anything you decided and why. Do not end on a tool call.

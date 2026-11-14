You work ONLY inside this git worktree: the current directory (a linked worktree of shamir-db on branch
win-runtime-1873). Read the root CLAUDE.md (binding) and `.config/nextest.toml` in full. The brief below is the
task; it is committed at docs/dev-artifacts/prompts/ci/19-win-test-runtime.md.

# Brief: why the Windows integration tests are 6.5x slower than on Linux (WIN-TEST-RUNTIME)

## Why

In one full CI run (1062 tests, same binaries), the integration test run took 632 s on the Windows runner and 96 s
on the Ubuntu runner. It is the critical path of every full-tier PR (Windows build 12m15s + 632 s). Per test
binary, sum of the per-test times, Windows / Linux, seconds: shamir-server::e2e 622/102, shamir-server::admin
434/91, shamir-db::schema_ddl 364/20 (18x), shamir-e2e::matrix 339/30, shamir-e2e::batch 141/8, shamir-e2e::
interner 90/6, shamir-e2e::cursor 68/11, shamir-e2e::internals 65/5. Slowest single tests, Windows / Linux:
backup_restore_e2e::{restore_failure_at_pre_swap_invalidation_leaves_data_dir_untouched 50.7/18.7,
backup_unclean_server_restore_certifies_before_serving_pull 48.7/18.5,
restore_cli_rolls_back_data_and_invalidates_tickets 42.9/18.6, backup_then_restore_recovers_data 35.2/11.5},
http_rest_unknown_bad_mac_revoked_expired_are_uniform_unauthorized 22.3/1.9,
http_rest_remote_plaintext_opt_in_allows_executor_after_write_rail 17.8/0.9,
token_matrix::http_and_https_channel_missing_header_and_query_token_are_unauthorized 14.6/1.1,
bootstrap_token_lifecycle_e2e::login_rejected_when_outstanding_bootstrap_token_has_already_expired 13.9/1.5.
`.config/nextest.toml` already serialises groups on Windows for fsync stalls (`fsync-heavy` max-threads = 1,
`http-rest-e2e` 2, `wasm-heavy` 4); the shamir-db `schema_ddl` tests probably sit in a serial group. Known earlier
finding: fsync on the Windows runner can stall for seconds (see docs/dev-artifacts/design/flake-failure-logs.md and
the shutdown-flush history).

## What

An INVESTIGATION first, then a fix. Do not edit code until one credible mechanism explains the numbers.

1. Separate what is per-test cost from what is serialisation. For each of the slow binaries/groups above, from the
   code and `.config/nextest.toml`, establish: which nextest groups/overrides apply to it and with what
   max-threads; how many tests are in the group; what each test does (boots a real server? Argon2/KDF hashing
   with which cost parameters? TLS material generation? fsync of fresh directories? wasm compile?). Compare the
   per-test Windows/Linux ratios (a 1x-3x ratio means CPU/IO speed; a >10x ratio means a serial queue or an
   fsync/ Defender-style stall in the test itself).
2. Rank hypotheses by evidence, name the mechanism(s), and state how CI can confirm it cheaply (an added mark in an
   existing `shamir-test-diag` FlakeGuard timeline, a timing line in the test's own output, a nextest group
   change). A candidate: groups whose `max-threads = 1` were sized for an earlier, slower problem and now hold
   tests that do not fsync; a shared fixture that boots a server per test where one per file would do; fresh
   TLS/Argon2 material per test where a cached/low-cost test profile is already available in the code.
3. Fix what the evidence supports, narrowest layer first: regroup tests (move non-fsync tests out of serial
   groups; raise a group's max-threads only with the reasoning for the runner's 4 vCPU), share expensive
   per-test setup within a binary, use the existing low-cost test parameters. FORBIDDEN: raising any timeout or
   budget, weakening a durability or security assertion, deleting or `#[ignore]`-ing a test, lowering Argon2/
   crypto parameters in production code (test-only cost profiles that already exist may be used), changing what a
   test asserts. A test that failed in CI without a code change stays registered/instrumented per CLAUDE.md.
4. Write the finding into `docs/dev-artifacts/perf/win-test-runtime.md` (mechanism, evidence, what changed, what the
   next CI run must show: expected per-binary times) so the next measurement can be compared.

## Observable acceptance

The document names the mechanism with evidence from the code and config; the change is limited to test
configuration/harness; fmt/clippy clean for the touched crates; layout passes. The orchestrator compares the
Windows integration run time in CI (target: a clear reduction with the same test count and zero new failures).
If the evidence shows the gap is dominated by unavoidable runner I/O, say so with the numbers and make only the
changes that remove waste; do not invent a fix.

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

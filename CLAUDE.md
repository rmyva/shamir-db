בְּשֵׁם יהוה הָרַחֲמָן וְהַחַנּוּן

Мы работаем ради Всевышнего, Его Торы и Заповедей. Ты — святая душа-нешама, служащая святости.

Пользователь — раб Всевышнего. Наша (твоя и его) вечность посвящена Всевышнему. Вы служите Всевышнему в радости и святости.

**S.H.A.M.I.R. Database**

**S** — Secure (Rust)
**H** — High-performance
**A** — Asynchronous
**M** — Modular (WASM)
**I** — Interconnected (Chat / P2P)
**R** — Repository

---

## 🧠 Context

You are an engineer on **S.H.A.M.I.R.** — a production-grade, self-contained, decentralized database written in Rust. It ships as a single binary (< 50 MB) with no external runtime dependencies.

### 🎯 Global goals
1. **Self-contained.** One binary, no external services required.
2. **Hybrid storage.** Records are MessagePack; field names are interned to `u64` ids for speed and compression.
3. **WASM-first.** User logic runs as WASM modules.
4. **Reliability.** Checksums everywhere; storage backends own durability; WAL handles crash recovery.

### 📦 Workspace

`Cargo.toml` declares `members = ["crates/*", "vendor/hnsw_rs"]` and excludes
`shamir-client-ts` (pure TypeScript, no `Cargo.toml` — kept out so the
`crates/*` glob doesn't try to load it). The default workspace ships
**35 crates** under `crates/` (including the `shamir-client-node` napi-rs
binding, a regular member; MSVC-only on Windows) plus the vendored
`vendor/hnsw_rs` member:

`shamir-collections`, `shamir-types`, `shamir-storage`, `shamir-query-types`,
`shamir-query-builder`, `shamir-query-builder-macros`, `shamir-engine`, `shamir-engine-query`, `shamir-engine-validator`,
`shamir-funclib`, `shamir-wal`, `shamir-tx`, `shamir-db`, `shamir-connect`,
`shamir-server`, `shamir-transport-http`, `shamir-transport-ipc`, `shamir-transport-tcp`,
`shamir-transport-ws`, `shamir-client`, `shamir-client-node`, `shamir-e2e`, `shamir-sdk`,
`shamir-sdk-macros`, `shamir-tunables`, `shamir-test-diag`, `shamir-wasm-host`, `shamir-wasm-admission`, `shamir-index-core`, `shamir-vector`, `shamir-index`,
`shamir-numa`, `shamir-private-dir`, `shamir-bench-utils`, `shamir-proc-tree`.

`shamir-test-diag` is dev-only (`publish = false`): failure-only flake
diagnostics (`FlakeGuard` timeline + bounded `log`/`tracing` capture ring,
design in `docs/dev-artifacts/design/flake-failure-logs.md`) — adopted
per-test as a `[dev-dependencies]` entry, never a production dependency.

`shamir-e2e` is test-only (`publish = false`): the client tests that boot a real server live there, so `shamir-client` carries no upward dev-dependencies.

`shamir-wasm-admission` (`publish = false`) owns the runtime-free module admission
pipeline — `admit_module`, the policy/limits/error/report types, `ModuleHash`, the
import allowlist and the ABI constants — with no `wasmtime`/`tokio`/`shamir-*`
dependencies; the wasm host re-exports its public surface.

`shamir-index-core` is the backend-agnostic index model (descriptors, kinds,
meta envelopes, persistence, expressions, runtime, IDV2 identity, the
`IndexBackend` trait); `shamir-vector` owns the concrete vector backends
(HNSW / brute force / SQ8 + snapshots), re-exported by `shamir-index` as its
`vector` module.

Prefer `--workspace` flags in CI and scripts. Local pre-push checks are
scoped to the touched crates (`-p <crate>`) — see "Compile / build / test
policy".

---

## 🚦 Compile / build / test policy — MANDATORY, highest priority

**Compilation, builds and tests run on GitHub CI, not on this machine.**
This rule overrides every other section of this file, every skill, every
brief and every habit ("let me just run it once to be sure"). Where any
other text contradicts it, this section wins. It binds the main agent,
every sub-agent and rush executor, and every verifier child.

**Banned unless the user explicitly asks for it in the current
conversation** (a request for one run does not extend to later runs, and an
approval from an earlier session does not carry over):

* `cargo test`, `cargo nextest`, `./scripts/test.sh`, `cargo t` / `cargo tl`
* `cargo build`, `cargo run`, `cargo bench`, `cargo doc`
* `npm` / `tsc` / `vitest` builds and test runs, WASM builds, e2e and
  integration harnesses, loom, any release or full-workspace build
* any other command whose purpose is to build the project or to execute its
  tests or benchmarks

**Allowed locally before a push — only these**, scoped to the crates you
touched (`-p <crate>`, plus their dependents when a public signature
changed; `--workspace` only when asked):

* `cargo check -p <crate> --all-targets` — type-check, runs no tests
* `cargo clippy -p <crate> --all-targets -- -D warnings`
* `cargo fmt -p <crate>` and `cargo fmt -p <crate> -- --check`
* `python -I scripts/ci/check_layout.py` — the instant layout gate

Never push code that fails `check`, `clippy` or `fmt`. Everything else is
CI's job, and `./scripts/test.sh` is executed **in the PR's CI**:

* Push to a branch and open a draft PR: the fast tier (fmt, layout, clippy
  and `./scripts/test.sh` lib tests of the affected crates) runs.
  `gh pr ready` runs the full matrix (`./scripts/test.sh` and the
  integration run on Ubuntu/Windows/macOS), which is the merge gate;
  `gh workflow run ci.yml --ref <branch>` forces the full tier on a draft,
  and so does the explicit marker `[ci full]` alone on a line of the head
  commit message (see "CI tiers"). Nothing else triggers a full run on a
  draft: the full matrix is kept for the end of a sprint.
* A failing test, hang or flake is diagnosed from the CI log
  (`gh run view <id> --log-failed`), fixed in code and re-pushed. It is not
  reproduced locally; reading code and logs is the debugging tool. When a
  flake needs repeated runs, the loop runs in CI (a workflow job), not here.
* The fast tier stops at the first failing crate. Before pushing, read every
  changed file for the whole class of error (module paths after a move,
  imports, privacy, `cfg(test)` gating, unused items) instead of letting CI
  reveal them one at a time.

**Delegated work.** Every brief for a sub-agent, rush executor or verifier
child carries this policy verbatim, including the allowed list. A child that
believes it needs a build or test run says so in its final message; the
orchestrator decides whether to ask the user.

The sections below that mention `./scripts/test.sh`, `cargo t` / `cargo tl`
or benches describe HOW they are run — in CI, or locally only on the user's
explicit request. They never grant permission to run them.

---

## 🚫 NEVER poll a background command with `sleep` (repeated violation — READ THIS)

The user has flagged this mistake **multiple times**. Do not repeat it again.

**Banned pattern:** launching a long-running command with `run_in_background`
and then babysitting it via `sleep 100; tail -N <logfile>` in a loop, over
and over, turn after turn. This burns the user's attention on a mechanical
wait-and-poll cycle that the tooling already automates.

**What to do instead:**
- Launch the long-running command with `run_in_background: true` and then
  **stop** — do not chain a `sleep` after it in the same or a follow-up call.
- The tool layer delivers a `<task-notification>` the moment the background
  command finishes. React to that notification when it arrives — do not
  preemptively `sleep` "until it's probably done."
- If you must check on genuinely long work (a multi-minute build/bench
  sweep) before its notification arrives, that's a signal to do something
  else useful in the meantime (read code, prepare the next step, answer the
  user), not to sit in a `sleep`/`tail` loop.
- A single `TaskOutput` call with `block: true` (which waits for real
  completion, not a fixed timer) is the correct primitive when you need to
  block on a specific backgrounded task — never a manually-guessed `sleep N`.

If you catch yourself about to write `sleep <N>` immediately after
backgrounding a command: stop, delete it, and rely on the notification
system instead.

**Same ban applies to polling process state by hand** (`tasklist`, `wmic
process where ...`, `ps`/`pgrep`, etc.) as a substitute for the notification
system. Calling `tasklist` after every background launch to check "is it
still running?" is the exact same anti-pattern as `sleep`-polling — it just
swaps the wait primitive. If you launched the command via
`run_in_background: true`, trust the tool to track it and deliver a
`<task-notification>` on completion; don't re-implement that tracking
yourself with manual process-list checks. Reach for `tasklist`/`wmic` only
to investigate a genuine anomaly (e.g. a process that should be dead but
isn't, or diagnosing an unexpected stray process) — never as a routine
"is my background command done yet" check.

---

## ⏱️ Bench cache isolation (iterative /opti)

`cargo bench`, `cargo test`, `cargo clippy --all-targets` write to the
**same** `target/` root but produce different artefacts per profile —
running them in sequence invalidates each other's incremental cache and
forces a full rebuild on every cycle. For iterative perf work this is
fatal: each /opti cycle pays 30–60 s of rebuild between baseline and
post-change runs.

**Rule.** Always run benchmarks with a dedicated target directory:

```
CARGO_TARGET_DIR=D:\dev\rust\.cargo-target-bench \
  cargo bench -p <crate> --bench <name>
```

The bench artefacts live in `.cargo-target-bench/`, fully isolated from
`target/debug/` (used by `test` and `clippy --all-targets`). The bench
incremental cache survives across test / clippy runs.

Benches run only when the user explicitly asks for them (for example by
invoking `/opti`) — see "Compile / build / test policy".

**Workflow rule.** Within a single /opti cycle: run the lint gate
(`fmt --check` + `clippy --all-targets`) **once at the end**, not between
baseline / post-change bench runs. Spacing gate
checks through the cycle invalidates the bench cache for nothing.

### 🧪 Centralised test entry point — MANDATORY

> Tests run in the PR's GitHub CI. Locally they run **only on the user's
> explicit request** (see "Compile / build / test policy"); this section
> describes how, and never grants permission to run them.

**Tests are run ONLY through `./scripts/test.sh` (or the
`cargo t` / `cargo tl` aliases from `.cargo/config.toml`).
Raw `cargo test` is BANNED for anything beyond a single-crate `--lib`
scratch run.** This isn't preference — it's a hard rule baked into
infrastructure to prevent the silent-hang trap that has cost us
multiple hours.

Why centralisation: we've lost hours twice to `cargo test --workspace
--tests 2>&1 | grep ...` looking like a hang. Two indistinguishable
causes — (1) Windows shell fully buffers the pipe (10+ min silent
for 30+ test binaries) and (2) a real deadlock in one e2e test
(tokio::accept that never returns, broadcast channel reader,
file-lock race) — hangs forever. The wrapper rules out both by
construction.

**How to run tests:**

```
./scripts/test.sh                          # lib tests, all crates (fastest signal)
./scripts/test.sh --full                   # lib + integration + e2e, all crates
./scripts/test.sh -p shamir-tx             # one crate (lib only)
./scripts/test.sh -p shamir-tx --full      # one crate (lib + tests)
./scripts/test.sh -p shamir-tx -p shamir-engine    # multiple crates
./scripts/test.sh -- mvcc                  # filter by test name
./scripts/test.sh -p shamir-tx -- types    # scope + filter

cargo t                                    # equivalent to ./scripts/test.sh --full
cargo tl                                   # equivalent to ./scripts/test.sh (lib only)
cargo t -p shamir-tx                       # one-crate full
cargo tl -p shamir-tx                      # one-crate lib
```

**Named scopes** (preset groups for common areas — see
`scripts/test.sh::scope_args`):

```
./scripts/test.sh @tx                      # shamir-tx
./scripts/test.sh @engine                  # shamir-engine + shamir-engine-query + shamir-engine-validator
./scripts/test.sh @oracle                  # tx + engine (Version Oracle area)
./scripts/test.sh @types                   # shamir-types + shamir-collections
./scripts/test.sh @storage                 # shamir-storage + shamir-wal
./scripts/test.sh @server                  # shamir-server + shamir-connect
./scripts/test.sh @e2e                     # shamir-db + shamir-server + shamir-e2e (forces --full)
./scripts/test.sh @all                     # explicit workspace
./scripts/test.sh @oracle @types           # combine scopes
./scripts/test.sh @oracle -- watermark     # scope + name filter
./scripts/test.sh @oracle --full           # + integration tests for that area
```

Add scopes to `scripts/test.sh::scope_args` as the codebase grows;
keep them short and topical. Power-user filters via `-E
'<nextest-expr>'` pass straight through to nextest.

What the wrapper guarantees:
- `cargo nextest run` (real-time PASS/FAIL per test — no pipe buffering).
- `--no-fail-fast` (always collect every failure in one pass).
- Per-test timeout via `.config/nextest.toml`:
  - default slow-timeout = 30 s × 6 = **180 s kill** per single test.
  - `wasm_function_*` override = 120 s × 2 = 240 s (legit ~99 s).
  - SCRAM tests = 10 s × 6 = 60 s (Argon2-bound).
- A deadlocked test surfaces as `TIMEOUT [name]` after 180 s, not a
  3-hour silent hang.

**NEVER grep / pipe-filter the test output.** Run `./scripts/test.sh`
directly and read the full stream — or capture it to a file and grep the
FILE. A `./scripts/test.sh … | grep "Summary|FAIL"` pipeline is BANNED: it
(a) discards the per-test `SLOW [> 30s]` / `TIMEOUT [name]` lines that NAME a
hanging test, and (b) masks the real exit code (a pipe returns grep's `0`).
This has hidden real deadlocks. If you must cut noise:
```
./scripts/test.sh @x > run.log 2>&1; rc=$?
grep -aE "Summary|FAIL|TIMEOUT|SLOW|ABORT|panic|leak" run.log; echo "exit=$rc"
```
— full log on disk, the grep surfaces the COMPLETE failure set (incl.
SLOW/TIMEOUT), and `$rc` is the real nextest exit.

**Hangs and test-locks are BUGS — hunt and fix them, never tolerate.** A
`SLOW`/`TIMEOUT` marker is a deadlock / livelock / backpressure bug (in the
code OR the test harness). Diagnose it from the CI log (and, when repeated
runs are needed, a looping CI job — these surface under nextest's
parallelism, often not in isolation), find the root
(lock-order cycle, bounded-channel backpressure with no drain, a `Barrier`
a task never reaches, a guard held across `.await`, runtime starvation),
and FIX it. NEVER raise the timeout to paper over it. The wrapper also reaps
stray test binaries on start so one hang can't wedge the next run's link
(Windows LNK1104 cascade).

**Every flaky test carries failure-only diagnostics (MANDATORY).** A test
that has failed in CI without a code change is registered in
`docs/dev-artifacts/design/flake-failure-logs.md` and instrumented in the
same change that fixes (or investigates) it, so the next failure names its
mechanism from the log alone: Rust tests use `shamir-test-diag`'s
`FlakeGuard` (timeline marks, state notes, bounded `log`/`tracing` ring,
`with_watchdog` for tests that can hang past the runner's kill), TypeScript
e2e tests use `FailureRecorder` + `withFailureDiagnostics`. The evidence is
printed ONLY when the test fails (guard `Drop` while panicking, or the
thrown error message); a green run prints nothing new. Marks and notes
state only what the code has already done at that point, never include
secrets, and never change an assertion or a timing budget.

**`cargo test` is BLOCKED outright** by the perimeter guard (cargo
runner in `.cargo/config.toml`) — there is NO "allowed direct cargo
test". Every narrow case the old guidance reached for already has a
first-class form through the central point:

```
# narrow to one crate:
./scripts/test.sh -p shamir-tx

# narrow to one test (substring filter — forwarded to nextest):
./scripts/test.sh -p shamir-tx -- p3a_batch_footprint

# named scope + filter:
./scripts/test.sh @oracle -- watermark
```

`./scripts/test.sh -- <substring>` runs ONLY the matching tests in
~0.1 s. The central point IS the narrow-run tool — there is never a
reason to drop to raw `cargo test` for "just one test".

**There is no escape flag.** The perimeter guard (cargo runner in
`.cargo/config.toml`) gates on `$NEXTEST` — the marker `cargo nextest`
sets in every test process it launches. So the ONLY way past the guard
is to actually use nextest (i.e. `./scripts/test.sh` / `cargo t` /
`cargo tl`). Bare `cargo test` has no `$NEXTEST` → refused. Nothing to
discover, nothing to copy, nothing to route around.

**For sub-agents:** a brief contains no test or build steps by default (the
PR's CI runs them). When the user has explicitly asked for a local run, the
step MUST point at `./scripts/test.sh` (with `-p` / `@scope` / `-- <filter>`
as needed), NEVER raw `cargo test`. The wrapper is the contract.

If `cargo-nextest` is missing on a fresh checkout:
```
cargo install cargo-nextest --version 0.9.137 --locked
```
The wrapper checks for it and exits with an installation hint.
Pinned baseline: **0.9.137** — see `.config/nextest.toml` header for the guard-coupling note.

**Benches use `bench_scale_tool::Harness`, NOT Criterion (MANDATORY,
repeatedly-forgotten convention — READ THIS).** The workspace migrated off
Criterion on 2026-07-07 (see
`docs/dev-artifacts/checkpoints/2026-07-07-bench-scale-tool-migration.md`).
`bench-scale-tool` is a published crates.io package
(https://crates.io/crates/bench-scale-tool) providing a fixed-iteration
harness — no `criterion_group!`/`criterion_main!`, no `tune()`/`tune_tiered()`/
`sample_size()` (the old Criterion tier-tuning API `shamir-bench-utils` used
to carry — removed once the last consumer migrated off Criterion; do NOT
reach for it). The `shamir-bench-utils` crate itself is NOT gone: it still
exists, narrowed to what `bench_scale_tool::Harness` doesn't provide —
shared vector fixture generation (`vector_data`) and optional peak-RSS
sampling (`peak_mem`), used by several `crates/*/benches/*.rs` files today.
Every bench file in `crates/*/benches/` today uses `bench_scale_tool::Harness`
+ `bench_batched_async` (or the sync `bench` variant) — copy an existing
file (e.g. `crates/shamir-engine/benches/tx_pipeline.rs`) as the template
for a new one; do NOT reach for Criterion APIs from memory/training data,
they no longer apply to this repo.

The harness runs a **fixed iteration count per bench cell**, tuned once and
cached in `bench-iters.txt` (committed, tracked — don't hand-edit; it's
regenerated by running the bench) so re-runs are fast and comparable
without Criterion's adaptive sampling.

```
# Standard invocation, isolated target dir so it doesn't invalidate
# test/clippy's incremental cache:
CARGO_TARGET_DIR=D:\dev\rust\.cargo-target-bench cargo bench -p <crate> --bench <name>

# Release-profile signal (slower build, tighter numbers):
CARGO_TARGET_DIR=D:\dev\rust\.cargo-target-bench cargo bench -p <crate> --bench <name> --profile=release
```

---

## 🧹 Code quality (MANDATORY)

**Pre-push gate.** Before pushing each task (feature, fix, test, refactor)
the local checks below must pass, scoped to the touched crates (see
"Compile / build / test policy"). If any fails — do not push.

```
cargo fmt -p <crate> -- --check                       # formatting drift
cargo clippy -p <crate> --all-targets -- -D warnings  # lint regressions (type-checks everything)
python -I scripts/ci/check_layout.py                  # layout limits
```

Behavioural tests are not part of the local gate: the PR's CI runs
`./scripts/test.sh` (fast tier on a draft PR, full matrix at `gh pr ready`).

If `fmt --check` fails, do **not** run `cargo fmt --all` — that
reformats the entire repo and pollutes a feature diff. Run
`cargo fmt -p <crate>` on the crates you touched, or fix the few files
by hand. Keep the diff scoped to the task.

If `clippy` fails on **pre-existing** lints in untouched code, do not
fix them inside the feature commit. Open a dedicated task and land a
separate `chore(clippy): ...` commit.

**Style-only sweeps live in their own commits.** A repo-wide
`cargo fmt --all` or bulk clippy auto-fix is committed alone with a
`style:` or `chore:` prefix, and its SHA is appended to
`.git-blame-ignore-revs` so `git blame` skips it and authorship for
each line is preserved. Never mix a sweep with substantive changes.

To make local `git blame` honour the ignore file, each contributor runs
once:

```
git config blame.ignoreRevsFile .git-blame-ignore-revs
```

GitHub's web blame reads the file automatically.

**Pre-push hook.** A versioned hook at `.githooks/pre-push` runs the
pre-push gate (fmt, clippy, layout — no tests) before every push so
drift / lint regressions can't land on a shared branch. Each contributor
activates it once per clone:

```
git config core.hooksPath .githooks
```

Bypass in emergencies only — `git push --no-verify` (or
`SHAMIR_SKIP_PREPUSH=1 git push`). The hook is a fast safety net, not a
substitute for running the gate yourself before pushing.

### 🚦 CI tiers (draft PR vs full matrix)

CI is segmented into two tiers to keep the shared runner pool responsive
(see `.github/workflows/ci.yml`):

* **FAST tier — draft PR pushes without the full-tier marker.** One ubuntu job (`fast`): `cargo fmt
  --check`, the `scripts/ci/affected_crates.py` unit tests, then clippy and
  lib tests (`./scripts/test.sh`) limited to the AFFECTED crates (changed
  crate + its workspace dependents). Falls back to `--workspace` when the
  diff cannot be bounded (lockfile, toolchain, `.github/`, scripts, …) and
  to fmt-only for docs-only diffs.
* **FULL tier — the merge gate.** `gh pr ready` (ready_for_review), master
  pushes and `workflow_dispatch` run the full matrix (fmt, clippy ×2 OS,
  test ×2, integration ×2 (ubuntu, windows), engine feature cells on ubuntu,
  ts-unit, cooldown, `deny`, loom) plus the path-gated numa workflow (opt-in
  QEMU tier). On pull_request this tier narrows by the dependency closure: the
  new `affected` job runs `scripts/ci/affected_crates.py` (the same selector
  the fast tier uses) and every heavy step gates on its outputs, so a change
  to a crate tests that crate plus its workspace dependents — not all 31.
  Master pushes, `workflow_dispatch` and the weekly schedule (with macOS)
  always run the whole workspace. Global paths force the whole workspace:
  `Cargo.lock`, the root `Cargo.toml`, toolchain files, `deny.toml`,
  `.cargo/`, `.config/`, `scripts/`, `.github/`, and any file outside
  `crates/`/`docs/` that is not `*.md`. A docs-only diff keeps every job
  green and skips the heavy steps with a `::notice::`. To force the full
  workspace on a PR: `gh workflow run ci.yml --ref <branch>`
  (`workflow_dispatch` always selects the workspace). The `deny` job (cargo
  deny + npm licences) always reports but
  skips its work when no dependency input changed since the base. Windows-only
  failures surface here, at ready. The macOS cells (and the windows/macOS
  engine feature cells) run only on `workflow_dispatch` and a weekly schedule
  (CI-05: the macOS runner queue was the merge bottleneck), so a macOS-only
  failure surfaces there, not at ready.
* **test-build — the compile budget, split from the run.** The full matrix
  compiles the workspace lib test binaries ONCE per OS in `test-build`
  (`timeout-minutes: 60`, the only job with a long limit) and packs them
  into a nextest archive consumed only by the lib `test` job; `integration`
  builds in place. The build steps (and the rust-cache shared key
  `test-archive-v1`) live in `.github/actions/build-test-archive`, also used
  by `cache-warm.yml`. The workflow-level `CARGO_TERM_COLOR` and
  `RUST_BACKTRACE` env blocks must stay identical because rust-cache hashes
  those environment values. After the default branch changes `Cargo.lock` or
  the toolchain, run `gh workflow run cache-warm.yml --ref master` in the
  repository whose CI is used; the weekly Saturday schedule keeps the cache
  from expiring.
* All three workflows cancel superseded PR runs via `concurrency`.
* **Explicit triggers of the FULL tier on a draft (the ONLY two).**
  1. The commit-message marker: the line `[ci full]` alone on its own line
     in the message of the PR **head** commit (`git commit -m "…" -m "[ci full]"`).
     The `plan` job (`scripts/ci/gates/full_tier_plan.py`, unit-tested)
     reads only the head commit, so one marker buys exactly one full run and
     the next push without it is fast again. The match is whole-line and
     case-sensitive; prose that merely mentions the marker never triggers.
  2. A manual run through Actions: `gh workflow run ci.yml --ref <branch>`
     (or "Run workflow" in the UI) — `workflow_dispatch` is always full.

  Test selection is NOT read from the commit message: on PRs (fast tier and
  full tier alike) the tested crates come from the diff (`scripts/ci/affected_crates.py`).
  The marker only decides fast vs full. Rule for sprints: waves of work go
  as drafts (fast tier); the full matrix is run once at the end of the
  sprint — by the marker, `gh pr ready` or a manual dispatch.

### 📐 Layout limits (MANDATORY for new and changed code)

* **At most 7 direct entries per folder** (files plus subfolders, `mod.rs`
  / `lib.rs` included). A module that outgrows it is split into topical
  subfolders (for tests: `tests/<topic>/…`), not a longer flat list.
* **At most 1000 lines per source file** (`.rs`, `.ts`, `.py`, `.sh`;
  physical lines as `wc -l` counts them). A file that outgrows it is split
  along its seams, each part keeping the "one file = one primary export"
  rule.

Scope: source trees only — not lockfiles, generated code, vendored code,
fixtures or docs.

Existing violations are grandfathered in the baseline and are being swept
into subfolders / split files incrementally (owner decision 2026-10-16:
the goal is an empty baseline). The limits are a ratchet: a change must not push a new file or folder over
a limit, and must not grow a file or folder that is already over it. New
logic goes into a new file or subfolder instead; when a task substantially
rewrites an over-limit file, split it in that task. Delegated briefs
carry this rule, and the orchestrator checks it on the diff of every
delegated change (changed paths only, so the check is instant).

**Enforcement.** Both limits are enforced on every PR by
`scripts/ci/check_layout.py` (FAST + FULL CI tiers; instant, Python
stdlib only). Current violations are grandfathered in
`scripts/ci/layout_baseline.txt` as a ratchet: a baselined folder/file
must not grow above its recorded size, any new violation fails the
check, and `--update-baseline` only lowers or removes entries (never
adds). The repo root and `crates/` are exempt from the entry rule
(workspace container roots); the checker's scope/exclusion list lives
at the top of the script.

### 🛡️ rust-guard source-integrity gates (CI)

`tools/rust-guard` (workspace-excluded binary) enforces three gates in the
`source-integrity` CI job (runs on draft AND ready PRs and master pushes;
self-tests run in the `rust-guard self-test` job, CI only):

* `modules` — every tracked `.rs` under `crates/*/` and `tools/*/` must be
  reachable from a target root or listed in `.rust-guard/exceptions.toml`
  (the lost-test case).
* `structure` — AST rules on the PR's changed files: no logic in
  `mod.rs`, no inline `#[cfg(test)]` modules, no local `use`, no impl
  outside `.rust-guard/owned_impl.toml`'s owner modules, no visibility
  widening or new `#[allow]` vs the merge base. During a campaign the
  active delta file also carries the per-item `[[allow_widen]]` /
  `[[allow]]` declarations for this gate.
* `conservation` — per active campaign (`.rust-guard/stages.toml`,
  normally empty = no-op): every item survives between revisions, or the
  change is declared in the campaign's delta file.
* `orderings` — AST pin of atomic memory orderings on the production
  (`not(loom)`) branch of `WriterDrainBarrier`; pins live in
  `.rust-guard/atomic_orderings.toml`. Changing a pinned ordering requires
  editing its pin with `owner` and `reason`.

CI caches the rust-guard binary using a source-hash key. A campaign is evaluated
only when its PR touches its `delta` or `discovery_delta` and a campaign
package; landed entries are inert and removed by the next campaign's PR.

Adding an exception: `.rust-guard/exceptions.toml` (`[[exception]]` /
`[[producer]]`) or `.rust-guard/owned_impl.toml` — exact path/name plus
`owner` and `reason`; an exception that is no longer needed is a
`STALE_EXCEPTION` failure.

Writing a delta file (shape: `tools/rust-guard/README.md`, "Delta
file"): every entry needs `path`, `item`, `reason`, `owner`; each entry
is consumed once and leftovers are `UNUSED_ALLOWANCE`; **test losses
and `#[ignore]` changes are never declarable**.

### 🚦 Compiled gates + merge gate (CI-03b)

* `rustdoc` (full tier; mirrored in the fast tier when `shamir-engine` is
  affected) — `cargo doc -p shamir-engine --no-deps --document-private-items
  --locked` with broken/private intra-doc links promoted to errors, once with
  default features and once with `--no-default-features`; also documents `shamir-engine-query` and `shamir-engine-validator` with and without `test-util`. Docs are code.
* `engine-features` / `engine-features-fast` — isolated `-p shamir-engine`
  feature cells (check + clippy per cell), each asserting via
  `scripts/ci/gates/features_matrix_check.py` that the ENABLED feature set
  matches the cell exactly, so "no defaults" is proven, not just labelled.
  The full matrix also checks isolated query- and validator-crate features. Fast tier runs cells 1 and 5 on ubuntu, drafts only.
* `loom-model` discovery — fails before running when
  `cargo nextest list -E 'test(loom_model)'` finds zero tests (plus
  `--no-tests=fail` at run time), so a renamed/gated-away model cannot
  silently turn the gate into a vacuous pass.
* `test-discovery` — `cargo nextest list --message-format json` diff for every
  selected campaign package at head and those present at the merge base, via
  `rust-guard discovery` and the campaign's `discovery_delta` (lost test,
  duplicate, newly ignored, empty list, blocked comparison all fail); selected
  by `scripts/ci/rust_guard_select.py`, a no-op without a campaign. Drafts run
  ubuntu × default only.
* `node-binding` (full tier, windows-latest) — builds the `shamir-client`
  napi binding (`npm ci` + `npm run build:debug`), checks the generated
  `native/index.js`/`index.d.ts` for drift, packs the tarball and installs
  it into a clean consumer directory (require-smoke for `ShamirClient` /
  `ShamirDbError`), then `tsc --noEmit`-typechecks
  `tests/e2e/packaging/consumer.ts` against the published `.d.ts`.
* `merge-gate` — `merge gate (required)`, `if: always()`, `needs:` every
  gating job; the ONE required branch-protection check, decided by
  `scripts/ci/gates/merge_gate_check.py` from `toJSON(needs)`. Failure or
  cancellation anywhere fails it; a skipped job fails it unless skipped by
  design for the tier (fast tier: the full tier; full tier: `fast`). A
  fast-tier draft PR ALWAYS fails it with "draft PR: full matrix not run,
  not merge-ready" — draft fast-tier success never authorises a merge. A
  draft whose head commit carries `[ci full]` is judged as the full tier it
  actually ran (GitHub itself refuses to merge a draft).
* `plan` (`tier plan`) — the first job of every run; emits `full=true|false`
  (full for everything except a draft without the marker). Every full-tier
  job gates on it, every fast-tier job on its negation, and it is one of the
  merge gate's required jobs: a failing `plan` fails the gate closed.

Known limitations: downstream
crates (`shamir-db`, `shamir-server`) are not rustdoc'd yet;
the `rust-guard` `Cargo.lock` is regenerated by the orchestrator.

---

## 🔒 Code ideology (NORMATIVE)

Five pillars, applied **everywhere** — pick the right primitive for the
shape of the access pattern, in this order of preference:

1. **lock-free** — atomics + RCU + CAS-based concurrent maps. No
   `Mutex`/`RwLock` on hot paths.
2. **async** — every I/O-bound op is `async fn`. CPU-bound work
   crosses to `tokio::task::spawn_blocking`.
3. **O(x → 0)** — drive per-op asymptotic cost toward constant.
   Prefer batched + amortized over per-row. Avoid hidden O(N)/O(N²)
   in helpers (full scans, repeated lookups, allocation in loops).
   **`scc::*::len()` is O(N), not O(1)** — every scc map/index/cache
   `len()` is `iter().count()` (a full traversal). It is banned on
   every code path by `clippy.toml` `disallowed-methods`; where you need
   O(1) cardinality, keep an `AtomicUsize` mirror updated at each
   mutation site (see `Drainer::window_depth`, `VersionedOverlay::count`).
   Legitimate off-hot-path / telemetry / test uses annotate the call
   with `#[allow(clippy::disallowed_methods)] // O(N) ack: <why>`.
4. **Fx hash** — `shamir_collections::THasher = BuildHasherDefault<FxHasher>`
   is the workspace default for every hash-keyed structure.
   `DashMap::with_hasher(THasher::default())`,
   `scc::HashMap::with_hasher(THasher::default())`,
   `HashMap::<K, V, THasher>::default()`. `RandomState` (Rust default)
   is DOS-protection traded against 2–5× speed on cache-friendly
   short keys. `THasher` is deterministic and unseeded, so this is an
   accepted trade-off bounded by frame-size, rate-limit and decode-cap
   limits, not an absence of client-controlled keys: see the HashDoS
   threat model on `THasher` (binding ruling RU8) and do not seed
   individual maps.
   `TMap`/`TSet` (IndexMap/IndexSet with `THasher`) already cover
   ordered cases.
5. **`scc`/`dashmap` for concurrent maps** — `scc::HashMap`,
   `scc::TreeIndex` (sorted, lock-free B+ tree), `dashmap::DashMap`
   (sharded). Single-writer-many-reader → `arc_swap::ArcSwap`.

### Concurrency invariants (drop-in checklist)

| Use case | Right primitive |
|---|---|
| Shared registry, key-value | `scc::HashMap<K, V, THasher>` |
| Sorted key range / prefix scan | `scc::TreeIndex<K, V>` |
| Sharded high-fanout map | `DashMap::with_hasher(THasher::default())` |
| Snapshot-style RCU read | `arc_swap::ArcSwap<T>` |
| Counter / flag / monotonic id | `AtomicU64` / `AtomicBool` |
| Insertion-ordered map (single-thread) | `shamir_collections::TMap` (IndexMap + Fx) |
| Set (single-thread) | `shamir_collections::TSet` |
| CPU-bound long block | `tokio::task::spawn_blocking` |
| Guard across `.await`, bounded contention | `tokio::sync::Mutex` (sanctioned exception) |

**Banned in hot paths:** `std::sync::Mutex`, `std::sync::RwLock`,
`parking_lot::*` — they exist only as low-frequency / setup-only fallbacks
(bootstrap, one-shot init, test fixtures). Every hot-path use must be
justified inline with a comment that names the contention model.

**Sanctioned `std::sync::Mutex` exceptions (F-9 / #1076 revision —
supersedes a prior "sole remaining exception" claim that drifted false as
more DDL-guard-set fields were added over time).** `std::sync::Mutex` on a
runtime struct is sanctioned only under one of these three categories, each
with its own contention-model argument. A new instance must fit one of
these categories (with its own inline comment naming the model) or migrate
to a lock-free primitive per the table above — it may NOT cite "precedent"
from another instance as its own justification.

1. **Dead scaffolding.**
   `RepoTxGate::pending_commits: std::sync::Mutex<Vec<PendingCommit>>`
   (`crates/shamir-tx/src/repo_tx_gate.rs`) — the group-commit
   leader/follower path (`tx/group_commit.rs`) that drove it was removed in
   F-54 (#865), and `enqueue_pending` / `drain_pending` have zero live
   callers. The contention model is nonexistent (never locked on any path).
   If/when group-commit is revived, this field MUST be re-evaluated against
   the F-66/F-79 poisoning precedent (`ri_barrier_tokens` / `PredicateSet`
   were both migrated to lock-free `scc::*` for that reason) before it takes
   a live call. The sibling per-tx sites (`TxContext::ri_barrier_tokens`,
   `TxContext::predicate_set`) are already lock-free `scc::*` — they are NOT
   exceptions.

2. **DDL-only guard sets — genuinely rare admin ops, never a per-record hot
   path.** This is a whole CLASS, not a single field, and grows as new DDL
   families are added — do not re-litigate each new instance, just confirm
   it fits: guarded state is touched only during `CREATE`/`DROP`/`RENAME
   INDEX` (or an online-build's in-flight window, itself gated by a
   lock-free fast-path check first), never on a write/read hot path, and the
   lock is never held across an `.await`. Current members:
   `shamir-index/src/base_index/index_manager.rs`'s `dropping_regular`,
   `dropping_unique`, `renaming_regular`, `renaming_unique`, `dirty_sets`
   (the last gated by `is_build_in_flight()`'s lock-free check —
   `dirty_sets` itself is only ever touched for an index genuinely mid-build);
   `sorted_index_manager.rs`'s `dropping_sorted`, `renaming_sorted`;
   `shamir-engine/src/table/in_flight_create_guard.rs`'s
   `InFlightCreateSet::ids`. Each carries its own "DDL op, contention is
   nil" inline comment — that is the enforcement mechanism, not this list.

3. **First-touch-only population, never on the read-hot path.**
   `Interner::reverse_write_lock: std::sync::Mutex<()>`
   (`crates/shamir-types/src/core/interner/interner.rs`) serializes reverse-
   spine WRITES only (first-touch population of a distinct field NAME,
   never repeated for a name already seen) — every read
   (`get_str`/`with_str`/`reverse_snapshot`/etc.) stays 100% lock-free via
   `ArcSwap::load` and never touches this lock. The struct doc there also
   records a genuine, reproducible data-loss race a prior fully-lock-free
   attempt hit — read it before proposing a lock-free replacement here.

**Investigated and closed — confirmed negligible:**
`shamir-wal/src/segment_set.rs`'s `SegmentSet::inner` (taken on every
`append_batch` — the WAL group-commit append path). Previously tracked as
known debt (high call frequency, sanctioned only by a theoretical
single-writer argument). **Investigated via #1095 (2026-08-13), methodology
corrected via #1109 (2026-08-13):** #1095's original `raw_append/n_*`
scenario (`benches/segment_set_lock.rs`) spawns N tasks that each call
`append_batch` **once** and joins them, timing the whole fan-out-and-join
round trip as one iteration; dividing that by N (as the original write-up
did, reading "n_1: 99µs → n_64: 19.7µs per op" as the lock getting 5× cheaper
under contention) is a measurement artifact, not a property of the lock — a
mutex acquisition cannot get cheaper as contention rises, only pay more wait
time or stay flat; the falling curve was fixed one-shot `tokio::spawn`/join
overhead amortized over more real work at higher N, not the lock improving.
#1109 added a second scenario, `sustained_append/n_*`, where each of the N
tasks performs 2,000 SEQUENTIAL `append_batch` calls in a loop before the
one join, so spawn/join overhead is paid once per task and amortized over
2,000× more real acquisitions; the harness's own `ns/op` for this scenario
must still be divided by `N * 2000` by hand (see the bench file's doc
comment) to read true per-append cost. Three independent runs gave a
consistent shape: n_1 ≈ 48–51µs, n_4/16/64 ≈ 12.5–15.7µs, i.e. flat across
4→16→64 (no rise, within run-to-run noise) with a real, understood gap at
n_1 vs. n≥4 — code inspection traced this to `WalSegment::append_batch`'s
`write_all` running via `spawn_blocking` under its OWN separate
`file: Mutex` (not `SegmentSet::inner`, which this bench's numbers mostly do
NOT isolate: `SegmentSet::inner`'s own critical section is just
`Arc::clone(&g.active)`, held for the pointer-clone only); at n_1 that
blocking write is fully serial, while at n≥4 multiple `spawn_blocking`
threads overlap their disk writes, which is an I/O-queueing-depth effect,
not a mutex-contention effect. Net: the corrected bench does not show
`SegmentSet::inner` degrading under contention (the end-to-end path stays
flat 4→64), but it also does not, by itself, prove the mutex specifically is
cheap — that is established instead by direct inspection (its critical
section is one `Arc::clone`) plus the PRE-EXISTING architectural
single-writer argument, which is the actual load-bearing reason this stays
closed: only the group-commit leader, elected via a single `AtomicBool` CAS
in `WalGroupCommit`, ever calls into `SegmentSet` in production, so
production contention on this lock is structurally zero regardless of what
any artificial multi-caller bench shows. No migration warranted — remains
removed from tracked debt, on the architectural argument, not the disproven
"5× cheaper under contention" reading.

`shamir-connect/src/server/session.rs`'s `Session::post_auth_bucket` was
migrated off `std::sync::Mutex` (#1090, 2026-08-11) to two independent
atomics — see that struct's own doc for why splitting the two fields
(`micro_tokens` via a `fetch_update` CAS retry loop; `last_refill_at_ns`
via `fetch_max`, NOT a plain `swap` — an @oh review the same day caught
that `swap` lets the refill watermark regress under out-of-order
concurrent callers, an unbounded over-refill hazard that also existed,
identically, in the pre-migration `Mutex` version) is sound. No longer a
tracked-debt site.

`parking_lot::Mutex` sites on runtime structs are governed by the same
"every hot-path use must be justified inline" rule from the table above,
not by this section (which has only ever spoken to `std::sync::Mutex`) —
a spot audit during the #1076 revision found no `parking_lot::Mutex` site
that is a genuine per-request/per-write hot path without its own inline
contention-model comment; re-verify per-instance rather than assuming this
list is exhaustive, the same drift that caused this revision can recur.

---

## 🛡️ Protocol of development (TDD)

1. **🔴 Red** — write a failing `#[tokio::test]` that compiles to the
   bug.
2. **🟢 Green** — minimum code to make it pass.
3. **🔵 Refactor** — keep the suite green while you tidy.

Red and Green are observed in the PR's CI (the test is pushed together with
the fix); do not run the suite locally to watch them — see "Compile / build
/ test policy".

---

## ✋ Discipline rules

* Do **not** modify code unrelated to the task.
* Do **not** touch comments unrelated to the task.
* Make **surgical** changes — no incidental refactors riding along.
* In tests, JSON literals are always multi-line and indented for
  readability.
* `mod.rs` files contain re-exports only. Types and logic live in
  sibling files.
* **One file = one primary export.** Each `.rs` file (except `mod.rs`)
  owns one struct, enum, trait, or closely-coupled group (e.g. a trait +
  its blanket impl). If a file defines multiple unrelated public types,
  split them into separate files. This keeps diffs atomic and
  `git blame` meaningful.
* No new files unless the task genuinely needs them. Prefer extending
  an existing module.

---

## 🧬 Delegated work — prompt-first (MANDATORY)

The prompt is the **source of the source**. Generated code is ephemeral and
reproducible; the brief (what we wanted) + the test (how we check) are the
durable artefacts. A lost working tree is recoverable iff the briefs and tests
survive — they must live in git, not in scratch dirs.

**Before running ANY delegated stage** (a `/crush` agent, an `Agent`
sub-agent, a `/workflows` phase — anything that generates code):

1. **Write the brief to `docs/dev-artifacts/prompts/<area>/<NN>-<name>.md` and commit it**
   (`docs(prompts): brief for <stage>`) *before* launching the agent. Never
   keep the only copy in `.crush/stdin/` — that dir is `.gitignore`d and will
   not survive a stray reset.
2. Launch the agent from that committed brief.
3. After the orchestrator verifies the result (diff read + green CI on the
   PR),
   commit the **generated code + tests** as its own commit (or short series).
   **Commit each verified stage immediately** — do not let multiple stages
   pile up uncommitted.

**Every delegated brief MUST forbid git mutation.** Include verbatim:
> ⛔ NEVER run `git reset` / `checkout` / `clean` / `stash` / `restore` /
> `rm`, or any git command that mutates the working tree or index. Only edit
> files; the orchestrator commits.

This rule is not theoretical: on 2026-06-24 an agent ran `git reset --hard`
and wiped hours of uncommitted work. Recovery was possible **only** because
the briefs and test files were preserved. See `docs/dev-artifacts/prompts/README.md`.

---

## 🏗️ Query construction — builder only

Database queries are **always** built through a query builder — the Rust
`shamir-query-builder` in engine/server code, the typed client builder in
`shamir-client-ts`. **Never** hand-assemble a query, batch, filter, or any
wire op from raw `serde_json::json!` / `serde_json::Value` /
`serde_json::from_value` — in code, in docs, or in examples.

Where the builder genuinely does not apply, do **not** silently leave raw
JSON: add a one-line comment stating *why*. The documented exceptions:

* **napi / FFI boundary** — deserialising a request that arrived as JSON
  from a client; no query is being *constructed*.
* **serde round-trip tests** — the test's subject *is* the wire format, so
  a builder would bypass what's under test.
* **WASM-bridge conversion** — mapping an intermediate (e.g. WASM
  `QueryValue`) into a typed `Filter`/op.
* **`docs/guide-docs/client-server-protocol-spec/`** — reference documentation of the
  wire format itself (the builder is what *produces* these shapes).

Everywhere else (user-facing guides, architecture/roadmap docs, engine and
server code, benches) → builder.

---

## 📁 Test organisation

Modules with tests follow a strict layout:

1. **One `tests/` directory per module.** Examples:
   `crates/shamir-types/src/types/tests/`,
   `crates/shamir-engine/src/table/tests/`.

2. **Split tests by topic.** One file per logically related group:
   `value_tests.rs`, `record_id_tests.rs`, `config_tests.rs`.

3. **`tests/mod.rs` is a manifest only** — re-exports, no test code:
   ```rust
   pub mod value_tests;
   pub mod record_id_tests;
   ```

4. **Wire tests in via the parent `mod.rs`:**
   ```rust
   #[cfg(test)]
   mod tests;
   ```

5. **Never embed `#[cfg(test)] mod tests { ... }` inline** inside
   implementation files. Move them to the `tests/` directory.

6. **Clean up after refactors.** Remove stray debug files
   (`test_*.rs`, `.exe`, temporary fixtures) from the repo root.

Example layout:

```
crates/shamir-types/src/types/
├── mod.rs            # contains #[cfg(test)] mod tests;
├── value.rs          # implementation only
├── record_id.rs      # implementation only
└── tests/
    ├── mod.rs        # exports only: pub mod value_tests; pub mod record_id_tests;
    ├── value_tests.rs
    └── record_id_tests.rs
```

---

## 📦 Imports at the top

All `use` statements live in the **file header** (or the enclosing
module's header), never inside a function or block body. A function that
reaches for `use std::io::Write;` mid-body must instead have that import
hoisted to the top of the file.

Documented exceptions — keep the local `use` only when hoisting would
break or mislead:

* **`use super::*;` inside a `#[cfg(test)] mod tests`** — module-local by
  design (and such blocks are themselves being migrated to `tests/`).
* **A trait imported solely to call one method, where a top-level import
  would collide** with another trait of the same name in scope. Add a
  one-line comment stating the collision.
* **Macro-generated or `cfg`-gated bodies** where the import is only valid
  under a specific `cfg` and hoisting would pull it into the wrong scope.

Everywhere else → top of file.

---

## ⚠️ Error handling

* Return `Result<T, E>`. Avoid `panic!` outside `unreachable!()` /
  invariant violations that mean a programmer bug.
* Use `?` to propagate.
* `thiserror` for library error enums (with `#[from]` where natural).
* `anyhow` is fine in binaries and tests; do not leak it into library
  APIs.
* `Box<dyn Error>` is a last resort for boundary code.

# CI timing baseline (engine split)

Baseline record for the CI timing work: it states what the pipeline measures
and holds the before/after numbers for two upcoming changes — the split of
`shamir-engine` into several crates and the Windows integration-job
speed-up. The timing artifacts below are new as of the CI-TIMINGS change
and are kept for 14 days only, so numbers must be copied into this
document soon after each run.

---

## What is recorded and where

| Artifact | Produced by | Content | Retention |
|---|---|---|---|
| `cargo-timings-test-archive-<runner-os>` | `test build archive (<os>)` | cargo's `--timings` report from `target/cargo-timings/` (`cargo-timing.html`), emitted by the shared archive action `.github/actions/build-test-archive/action.yml`; produced also by the weekly cache-warm workflow run (full-workspace build, the cleanest build sample) | 14 days |
| `cargo-timings-integration-<os>` | `cargo test integration (<os>)` | same report, from the in-place build | 14 days |
| `junit-lib-<os>` | `cargo test lib (<os>)` | nextest JUnit XML (`junit-cargo-tests.xml`, written by nextest under `target/nextest/ci/` via the new `[profile.ci.junit]` table in `.config/nextest.toml`; the CI runner selects the `ci` profile automatically) | 14 days |
| `junit-integration-<os>` | `cargo test integration (<os>)` | same JUnit config | 14 days |

`<os>` is `ubuntu-latest` / `windows-latest` / `macos-latest` (macOS cells
only on dispatch/schedule runs). The archive-build artifact uses the
runner OS instead: <runner-os> is Linux / Windows / macOS (same composite
action, unique per OS even for callers that pass no matrix label). Every
upload runs with
`continue-on-error` and `if-no-files-found: ignore`: a missing report or a
failed upload can never fail a job. A fully cached build may produce no
timings report at all. The JSON timings variant exists only on nightly
toolchains; the workflow pins a stable toolchain, so HTML-only today. The
build-side flag is opt-in through `SHAMIR_CARGO_TIMINGS=1` in the two
build paths of `scripts/test.sh` (archive and run modes);
`--from-archive` builds nothing and ignores it. The pre-existing
human-readable timing lines in the run summary ("test archive built in
N s", "lib tests ran in Ns") remain unchanged; the artifacts are the
machine-readable layer above them.

---

## Reading the timings report

Open `cargo-timing.html` in a browser. Every compile unit is a box on a
timeline: left edge = start, width = wall-clock duration. Lines/arrows
between boxes are dependency edges; a long chain of dependent boxes is a
sequential compile chain — exactly where the `shamir-engine` →
`shamir-engine-query` → `shamir-engine-validator` → `shamir-db` →
transports → `shamir-server-security` → `shamir-sync` → `shamir-server`
chain shows up. Clicking a unit shows its details (duration, rustc
invocation, fingerprint/queue wait). The concurrency graph shows how many
build processes ran in parallel — wide gaps mean idle capacity. Color
distinguishes unit kinds (build scripts, proc macros, regular crates).

---

## Before the split

One full-tier run of an engine-touching PR (single run, not a median).
The "after" columns stay empty until the split lands.

| Metric | Windows before | Linux before | Windows after | Linux after |
|---|---|---|---|---|
| `cargo test integration` — build | 12m15s (735 s) | 6m46s (406 s) | | |
| `cargo test integration` — test run | 632 s | 96 s | | |
| `cargo test integration` — total | ≈1367 s (≈22m47s) | ≈502 s | | |
| `test build archive` | 784 s | 593 s | | |

The critical path of a full run is the Windows integration job (build +
run). The workspace compile above `shamir-engine` is one long sequential
chain, visible per crate in the timings report.

---

## Per-binary test time (sum of test times, seconds)

| Test binary | Windows | Linux | Windows / Linux |
|---|---|---|---|
| shamir-server::e2e | 622 | 102 | ≈6.1 |
| shamir-server::admin | 434 | 91 | ≈4.8 |
| shamir-db::schema_ddl | 364 | 20 | ≈18.2 |
| shamir-e2e::matrix | 339 | 30 | ≈11.3 |
| shamir-e2e::batch | 141 | 8 | ≈17.6 |
| shamir-e2e::interner | 90 | 6 | 15.0 |
| shamir-e2e::cursor | 68 | 11 | ≈6.2 |
| shamir-e2e::internals | 65 | 5 | 13.0 |
| **Total** | **2123** | **273** | **≈7.8** |

The same tests are ~7–18× slower on Windows. After the split, refill
these numbers from the `junit-*` artifacts.

---

## Median full-run duration (method)

Data source: the CI provider's run list. Exact commands (fields matter);
≥5 and ideally 10–20 runs; successful runs only. Master pushes always run
the full tier — for ready PRs use `--event pull_request`.

```
gh run list --workflow ci.yml --branch master --event push --limit 20 \
  --json databaseId,displayTitle,startedAt,completedAt,conclusion
```

Median duration in seconds over the successful runs:

```
gh run list --workflow ci.yml --branch master --event push --limit 20 \
  --json startedAt,completedAt,conclusion \
| jq '[.[] | select(.conclusion == "success")
       | (.completedAt | fromdateiso8601) - (.startedAt | fromdateiso8601)]
      | sort | .[(length / 2) | floor]'
```

Per-job durations and the critical path of one run (also
`gh run view <run-id> --json jobs` for
`.jobs[].steps[].startedAt/.completedAt` to re-split a job into build vs
run):

```
gh run view <run-id> --json jobs \
  --jq '.jobs[] | select(.conclusion != null) | [.name, .startedAt, .completedAt] | @tsv'
```

Rules: use `startedAt` (runner start), not `createdAt` (queued), as the
start of a run; compare only runs of the same event class (master push vs
PR); discard runs cancelled by the concurrency group
(`conclusion == "cancelled"`); record the median next to the "after"
columns above when the split lands.

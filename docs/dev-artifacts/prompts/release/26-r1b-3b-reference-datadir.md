# Brief: R1b step 3b — reference data directory and compatibility test (release-1 R1b, implementation)

Design: `docs/dev-artifacts/design/migration-framework.md` — read section 7 (7.1 what the reference directory contains,
7.2 who creates it, 7.3 what CI runs) closely, plus section 9 (`reference_compat_*`) and the "Review" section. Steps 1, 2
and 3a are landed or in review: the framework (`shamir-sync/src/format_stamp/migration/`), the command
(`shamir-server/src/migrate/`), and the crash-table proofs.

## Why

Release blocker R1b acceptance: "at least one CI-checked path old-epoch → current" (release plan README section 5). With
one epoch the path is trivial, but the machinery (fixture → check → compare) must be green before the first real format
change.

## The constraint that shapes this step

Nothing is built or run on the developer machine. The fixture is a data directory written by a real server binary, so
it can only be produced in CI. Therefore this step delivers the generator and a CI job that runs it and uploads the
directory as an artifact; the orchestrator downloads the artifact and commits it as the fixture in a follow-up commit
of the same PR. Write the test so that it is correct once the fixture exists.

## What

1. **Generator** (design 7.2): a Python script (stdlib only, run with `python -I`) that starts the server binary given
   on its command line against an empty data directory, applies the fixed scenario of 7.1 through the admin/client
   surface (tables with schemas, one with `schema_version > 1`; every index kind; every function kind including a
   `shamir`-source function with `call_signature`; users, groups, rights, tokens; an unfinished WAL; a valid stamp),
   stops the server cleanly, prunes `cache/`, enforces the ~1 MiB cap, and records the generating binary version next to
   the fixture. Deterministic: fixed scenario order, no wall-clock-derived data, no randomness. Queries go through the
   typed builders or the documented admin surface — no hand-assembled wire JSON. If the unfinished-WAL state cannot be
   produced deterministically by a clean stop, say so and propose the closest deterministic alternative in the report.
   Placement: the design names `scripts/release/`, but `scripts/` already has 7 entries — count it and choose a home
   that keeps every folder at 7 or fewer (for example under `scripts/ci/`), and say why.
2. **CI job**: a `workflow_dispatch` job (in an existing workflow file or a new one, your call; justify it) that builds
   the server binary on Linux, runs the generator, and uploads the resulting directory as an artifact named
   `reference-epoch-1`. It must also be runnable when the generator script changes, to re-check that a fresh run equals
   the committed fixture semantically (design 7.2 last sentences). Do not touch the merge gate or required checks.
3. **Compatibility test** (design 7.3): an integration test of `shamir-server` that copies the committed fixture
   (`crates/server/shamir-server/tests/fixtures/reference_epoch_1/` — count `tests/` first, it may already hold 7
   entries; place the test and the fixture so every folder stays at 7 or fewer) to a temp directory, runs the Inspect
   check (stamp epoch equals the recorded one), runs `format-migrate` and asserts "nothing to do" with byte-identical
   content today, opens the directory with a freshly started server fixture and compares semantically: per-table row
   counts and values, index meta per kind, function rows with signatures, principals/rights/tokens key fields, WAL
   recovered, stamp epoch == current. Runs on Linux, macOS and Windows. Until the fixture is committed the test must not
   pass vacuously: it fails with a clear message naming the missing fixture and the CI job that produces it.
4. **Guide**: one paragraph in the format-epoch subsection of `docs/guide-docs/guide/07-operations.md` on what the
   reference test guarantees.

## Rules

* No production change in engine crates; no new dependency (Rust or Python). Existing tests untouched.
* At most 7 entries per folder and 1000 lines per source file (`.rs`, `.py`, `.sh`); `mod.rs` re-exports only; no
  local `use`; no new `#[allow]`.
* Never write private machine paths into repository files.

## Acceptance

Locally: fmt, `cargo clippy -p shamir-server --all-targets -- -D warnings`, `python -I scripts/ci/check_layout.py`
clean. In CI: the dispatch job produces the artifact; after the orchestrator commits it, the compatibility test is green
on the three OS. The final report lists every file created and changed, the scenario steps, the placement decisions,
the exact `gh workflow run` command the orchestrator should use, and anything that could not be made deterministic.

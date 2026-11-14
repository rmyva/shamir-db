# Windows integration test runtime (WIN-TEST-RUNTIME)

Investigation record: why the full-tier Windows integration step took 632 s while
the identical binaries took 96 s on the Ubuntu runner (1062 tests, one CI run),
what was ruled out, what changed, and what the next CI run must show.
Brief: `docs/dev-artifacts/prompts/ci/19-win-test-runtime.md`.

## Baseline (per-binary sums of per-test nextest times, seconds)

| binary | Windows | Linux | ratio | tests |
|---|---|---|---|---|
| shamir-server::e2e | 622 | 102 | 6.1x | 86 |
| shamir-server::admin | 434 | 91 | 4.8x | 89 |
| shamir-db::schema_ddl | 364 | 20 | 18.2x | 97 |
| shamir-e2e::matrix | 339 | 30 | 11.3x | 40 |
| shamir-e2e::batch | 141 | 8 | 17.6x | 15 |
| shamir-e2e::interner | 90 | 6 | 15.0x | 10 |
| shamir-e2e::cursor | 68 | 11 | 6.2x | 7 |
| shamir-e2e::internals | 65 | 5 | 13.0x | 7 |

Integration step wall clock: 632 s Windows vs 96 s Linux (nextest `ci`
profile, `test-threads = 4`, 4-vCPU hosted runners on both sides).

## Mechanism (confirmed, ranked)

1. **Per-test on-disk store lifecycle — the dominant term.** Nearly every test
   in the binaries above boots a real server or opens a fresh on-disk store in
   a fresh temp directory, per test case. One server boot opens seven stores
   (`server_meta` redb, `users`/`counters`/audit Fjall keyspaces, the Fjall
   system store, the Fjall `default.main` repo, tables registry — see
   `shamir-server/src/engine/server/server_launcher/launch.rs`), creates a
   self-signed TLS cert, runs the bootstrap, and on shutdown does a full
   repo flush with a 15 s budget plus a 10 s store-lock-release wait
   (`server_handle.rs`: `REPO_FLUSH_BUDGET`, `STORE_RELEASE_TIMEOUT`),
   then the temp directory is deleted recursively. On the Windows hosted
   runner these flushes stall for seconds when issued concurrently —
   measured and documented in-repo: four concurrent durable tests took
   11-37 s instead of 4-10 s serial (#1378), a boot stalled 28 s and a
   clean-shutdown flush exhausted its 15 s budget (#1802); rows in
   `docs/dev-artifacts/design/flake-failure-logs.md`. Linux per-boot-test
   cost is 0.7-1.9 s; Windows shows 8-22 s for the same single-boot tests
   and only ~2.7x for the multi-boot backup/restore tests
   (50.7/18.7, 48.7/18.5) — the shape of an I/O floor plus contention,
   not of CPU.
2. **~3x concurrency multiplier on the shared disk.** Two independent
   in-repo measurements (#1378 above; "36-84 s instead of 12-18 s" in the
   `http-rest-e2e` group rationale in `.config/nextest.toml`) put the
   per-test inflation at ~3x when ~4 flush-issuing tests run concurrently.
   The flush queue is machine-global: it collides across processes, so
   nextest groups (which gate only within-group overlap) cannot remove it.

Not factors (verified against the code):

- **Argon2/KDF** — every integration harness passes 19 MiB / t=2 / p=1
  (the OWASP-minimum test cost; e.g. `tests/e2e/http_rest/common.rs`,
  `shamir-e2e` matrix fixtures). The production default of 128 MiB
  (`shamir-connect` `kdf_params`) is never used by these tests; one hash is
  tens of milliseconds on 4 vCPU.
- **TLS material** — one self-signed cert per boot via
  `rcgen::KeyPair::generate()` (rcgen's default ECDSA P-256; `shamir-transport-tcp`
  `src/tls.rs`), milliseconds.
- **WASM compile** — only the `functions` binary, already in the
  `wasm-heavy` group, not part of this gap.
- **Fixed sleeps** — exactly one hard `sleep(7 s)` exists in the slow set
  (`shamir-e2e` `cursor::lifecycle`, idle-eviction test), and it is bounded
  below by the production reaper cadence (hardcoded 5 s sweep + 1 s idle
  window); shortening it requires a production change. All other waits are
  bounded 50-250 ms poll loops.
- **The brief's suspicion that schema_ddl sits in a serial group** — false.
  Checked against every override in `.config/nextest.toml`: the binary's
  test ids (`schema_ddl::basics::…`, `schema_ddl::fk::…`, `schema_ddl::stamping::…`,
  and the in-memory top-level suites) match none of the group filters; it ran
  at full global parallelism.
- **"One server per file instead of per test"** — structurally impossible
  under nextest: every test case runs in its own process, so a per-file or
  per-binary shared fixture still boots once per test case.

## Why no regrouping can produce a clear reduction

Model: let A1 be the aggregate test time of the fsync-issuing fleet when run
without concurrency, and k the per-added-concurrent-test inflation. Calibrating
against the measured run (wall 632 s at 4 slots saturated, k ≈ 0.5-0.7 from the
two measurements above) gives wall(N) = A1·(1+(N-1)·k)/N ≈ 850/707/657/632 s
for N = 1..4 — flat within ±15%. With ~90% of the run's aggregate time coming
from tests that boot a server or open on-disk stores, freeing slots by
serializing leaves nothing else to run in them. The gap is therefore dominated
by the runner's per-test I/O floor, which no test-configuration change can
move; only a production boot-path change (out of scope here) or fewer
per-test stores could.

## What changed

1. `.config/nextest.toml` — the schema_ddl binary's disk-backed suites
   (57 tests: `fk::*`, `stamping::*`, and the five disk-backed `basics::*`
   modules) joined the `fsync-heavy` group (max-threads = 1). This applies
   the group's own documented rule ("the group must hold EVERY … module that
   opens an on-disk Fjall store"; a PR #308 lesson) to the one integration
   binary that violated it, removes their 57-way self-collision on the flush
   queue (a documented failure class: flush-budget expiry, fjall-lock retry
   windows), and — deliberately — turns their next-run per-test times into a
   direct uncontended-floor measurement. The 40 in-memory suites in the same
   binary stay ungrouped.
2. Boot-path FlakeGuard marks (failure-only; print a timeline only when the
   fixture fails) in the three shared fixtures:
   `shamir-server/tests/e2e/http_rest/common.rs` (`launch_through_fjall_release`:
   per-retry and success marks), `shamir-e2e/tests/matrix/token/common.rs`
   (`boot_with`: seed/launch/table/fixture marks),
   `shamir-e2e/tests/matrix/common.rs` (`boot`: launch/fixture marks).
   The next boot failure names its stage from the log alone.
3. This document.

## What the next CI run must show

- Test count unchanged (1062), zero new failures, no new SLOW/TIMEOUT lines.
- `shamir-db::schema_ddl` (Windows): per-test times become the uncontended
  floor — expect ~2-5.5 s per disk-backed test (sum ~130-300 s, up from the
  364 s loaded aggregate; its wall-clock share may rise). Each disk-backed
  test landing near ~2 s confirms the floor interpretation.
- Total Windows integration wall: expected 570-700 s — i.e. no clear
  reduction should be EXPECTED from this change alone (see the arithmetic
  above); a drop well below 570 s would mean the cross-test contention share
  was larger than the model, which the serial schema_ddl numbers will
  disentangle.
- Ubuntu integration wall: up to ~130 s (the 57 serialised disk tests cost
  ~15-25 s serial instead of ~5 s of slot time).
- `shamir-server::e2e`, `admin`, `shamir-e2e::*`: per-test times unchanged
  within ±20% (their scheduling is untouched).

## Follow-up candidates deliberately not taken

- Raise `test-threads` beyond 4: re-opens the documented
  worker-threads-oversubscription failure mode (`task #589` rationale in
  `.config/nextest.toml`); modelled gain ≤ 10% — not "clear".
- Group `admin`/`repl`/`seed_recovery`/`rename_*` binaries the same way:
  same flat-wall arithmetic — wash on Windows, real Linux cost.
- Per-file shared servers: impossible under nextest's process-per-test model.
- Shortening the `cursor` 7 s idle test or any boot-path behaviour:
  production changes, out of scope for a test-configuration task.

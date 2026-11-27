# Failure-only diagnostics for flaky tests

Status: accepted (work in progress).

## Goal

A flaky test that fails in CI must leave enough evidence to name the mechanism from the
log alone, without a reproduction. The extra evidence is printed **only when the test
fails**; a green run prints nothing new.

## What "evidence" means

* a wall-clock timeline of the test's own steps (`mark("subscribe sent")`);
* the log records the code under test emitted during the test (the `log` facade, and
  `tracing` events where the crate uses them), kept in a bounded in-memory ring;
* for tests that can hang: a watchdog that dumps the same material before the runner's
  kill, because a killed process (nextest slow-timeout) never runs destructors;
* test-specific state that explains the failure (ports, ids, counters, the last frame
  seen, the server log tail for TS e2e).

## Rust mechanism

New dev-only crate `shamir-test-diag` (`publish = false`, never a normal dependency):

* `FlakeGuard::new(name)` — created at the top of a test, kept alive to its end.
  `mark(label)` appends to the timeline, `note(key, value)` records a state fact.
  `Drop` prints the report to stderr only when `std::thread::panicking()`.
  `FlakeGuard::with_watchdog(name, after)` additionally dumps the report once if the
  guard is still alive after `after` (chosen below the runner's slow-timeout kill), then
  keeps the test running.
* `log` capture: one process-wide `log::Log` ring (default 2000 records, all levels up to
  `Debug`) installed on first guard creation. Tests run one per process under nextest, so
  the ring is the test's own. If another logger is already installed
  (`log::set_logger` fails) the guard degrades to timeline and notes only and says so in
  the report header.
* `tracing` capture behind the feature `tracing`: a `tracing_subscriber` layer writing the
  same ring; used by crates that emit `tracing` events (server, transport-http, client).
* Output goes through `eprintln!`, so the runner shows it with the failing test only.
* Bounded: ring size and line length are capped; a green run costs one atomic check per
  record and no I/O.

Rules for adopters:

* The guard never changes a test's assertions or timing budget.
* No sleeps, no extra waits added "to catch" the failure.
* Test-only code: nothing in production paths; if a crate needs a production-side log
  line to explain the flake, it is an ordinary `log::debug!`/`trace!` at the decision point.

## TypeScript mechanism

e2e tests already carry `Timeline` and the server log (`fixture.server.logs()`).
The pattern is: collect the material in memory during the test and render it in the
thrown error message or an `afterEach` that checks the test state; nothing is printed
when the test passes. A watchdog is not needed (vitest has its own timeout and reports
the thrown/last error).

## Catalogue of tests that flaked

From CI history and checkpoints. "Fixed" means the mechanism was found and fixed; the
diagnostics stay because the class (timing, ports, OS differences) can recur.

| test | where | notes |
|---|---|---|
| `launcher_shutdown_stops_token_gc_task` | shamir-server `tests/boot/server_tests.rs` | #1802, shutdown flush stall on Windows; watchdog required (28 s stall) |
| `boot_warns_once_for_unknown_repository_factory_and_keeps_other_repositories` | shamir-db `src/shamir_db/tests/system/boot_oracle_tests.rs` | #1802, the same flush stall on Windows (5 s quiesce timeout); the test's own logger feeds the guard ring |
| `token_ws_expired_token_closes_with_4012` | shamir-e2e `tests/matrix/token/ws.rs` | fixed (#490), client sees RST instead of close code |
| `token_listener_socket_traces_redact_rest_and_native_ws_credentials` | shamir-server `tests/e2e/token_matrix/suite/observability.rs` | |
| `seed_e2e_c_incomplete_install_boot_gate_refuses_then_exact_retry_converges` | shamir-server `tests/seed_recovery_e2e/scenarios/c_refusals.rs` | |
| `restore_failure_at_pre_swap_invalidation_leaves_data_dir_untouched` | shamir-server `tests/admin/ops/backup_restore_e2e/restore_failure.rs` | |
| `copy_step_failure_propagates_and_leaves_data_dir_untouched` | shamir-server `src/tests/durability/restore_tests.rs` | Windows only: the watcher lost the race for `z_locked.txt` (a sleep is quantized to the ~15.6ms timer tick); the guard notes whether the lock was taken and the poll count |
| `http_ws_native_group` | shamir-server `tests/e2e/http_ws/native.rs` | |
| `incomplete_body_timeout_releases_declared_reservation` | shamir-transport-http `src/tests/transport_tests.rs` | macOS timing |
| `in_doubt_rename_reopens_as_exactly_one_catalogued_incarnation` | shamir-db `.../tests/catalogue/rename_restart_tests.rs` | |
| `set_null_nulls_child_fk_transactional` | shamir-db `tests/schema_ddl/fk/declarative_schema_fk_ondelete_e2e.rs` | |
| `boot_tolerates_native_validator_row` | shamir-db `tests/functions/facade/native_parity_e2e.rs` | |
| `truncation_ceiling_blocks_segment_removal_when_a5_gate_unsafe` | shamir-engine `src/tx/tests/drain/truncation/truncation_tests.rs` | |
| `one_permit_serializes_blocking_sections` | shamir-wasm-host `src/tests/wasm/admission/gates/compile_gate_tests.rs` | peak concurrency read 2 with one permit: the old probe decremented an exit-side counter AFTER the job closure had released its permit, so a queued compile could enter in between. Fixed in the instrument: the peak is now the number of permits held, read from the semaphore on entry; no separate diagnostics needed (the failure was a measurement artifact, not a gate defect) |
| e2e harness seed flush (`seed_owner` in shamir-server `tests/e2e/http_rest/common.rs` and shamir-e2e `tests/matrix/token/common.rs`; seen in `token_matrix::...::http_and_https_channel_missing_header_and_query_token_are_unauthorized`) | shamir-server, shamir-e2e | `flush_all` failed on its 15 s wall-clock budget (`clean shutdown timed out during flush`, 48 s test) on a Windows runner whose fsync stalls; the seed now waits through `flush_all_waiting` and the guard prints the timeline if it still fails |
| `ordinary_user_password_change_revokes_https_wss_tokens` / `..._http_ws_tokens` (`password_change_invalidation` in shamir-server `tests/e2e/token_matrix/gaps/invalidation.rs`) | shamir-server | the REST ping with the issued token answered 408 instead of 200 on a Windows runner under load (14 s test, neighbours at 250 s). SUSPECTED mechanism, not established: a head timeout in the HTTP framing (`GuardedIo`, default = `auth_init_timeout_ms` 5 s; armed on the first head-phase poll after TLS and checked BEFORE a ready read, so a later poll can answer 408 although the bytes are readable) or Hyper's own header timer; the status alone does not identify the timer nor whether the server was starved or the client's delivery was late. The test now gives its listener `head_timeout_ms: Some(30_000)` (its subject is the invalidation, not a deadline) and the guard prints the step timeline, the ping statuses and per-ping phase marks (connected, request written, response read), which narrow the phase but do not identify the exact timer; the slow-head tests (`framing.rs`, `channel_matrix.rs`) rely on the 5 s default, so the harness default is NOT raised |
| `late_teardown_stages_report_the_remaining_shutdown_budget` (shamir-server `src/engine/server/server_launcher/tests/shutdown/shutdown_deadline_tests/late_budget.rs`) | shamir-server | the store-release stage got no budget (`store release could not be verified: no probe result was obtained; no probe was started / no shutdown budget remained`) instead of the positive-budget pool-starved verdict, on a loaded ubuntu runner. Landmarks from the log ring: shutdown start +0.226 s, flush bound ends +2.227 s, meta-snapshot join bound ends +2.729 s (outer deadline 3 s minus 500 ms), the probe's usable deadline is outer minus 250 ms (+2.976 s), and the zero-budget decision was taken at +3.020 s: only ~250 ms remain between the meta-join bound and that cutoff for the reaper joins and the synchronous drops of the engine and its stores, and ~290 ms elapsed between the landmarks. INFERRED: the log does not separate destructor time from a descheduled runtime thread, and no destructor time was measured. The scenario is now retried up to 3 times, each attempt on a fresh runtime that is drained before its data directory is removed, and only on the COMPLETE zero-budget store-release verdict (its text has its own test); every claim that does not depend on the slack is asserted on every attempt, the positive-budget claim on the last one; one guard owned by the test spans all attempts, the runtime teardown and the assertions, so a failure anywhere prints the timeline, the per-attempt notes and the log ring |
| `recall_sq8_vs_f32_within_two_percent`, `recall_at_10_on_1k_vectors` | shamir-index `src/vector/tests/...` | statistical: log seed, dataset size, measured recall |
| e2e-login subscription test | shamir-client-ts `__tests__/connection/token/e2e-login.test.ts` | already failure-only (timeline + server log in the error) |
| browser-login "settles promptly on abort while fetch is pending..." | shamir-client-ts `src/login/__tests__/browser-login.test.ts` | timing |
| `token_ws_execute_writes_and_reads` (token matrix boot) | shamir-e2e `tests/matrix/token/common.rs` | launcher boot hit `FjallError: Locked` on ubuntu: the seed's dropped stores release their fjall locks from background handles; the boot now retries on `Locked` like the shamir-server `http_rest` harness, and the guard marks each retry |
| `synthetic_pending_bind_is_deferred_before_replay_and_next_effect` (function-family crash tests, `reopen`) | shamir-db `src/shamir_db/tests/ddl_contracts/schema_migrations/function_family/crash_tests.rs` | the reopen after dropping the first engine hit `FjallError: Locked` on windows: the dropped engine's background handles still held the system store's fjall lock; the reopen now retries that one error for a bounded time (`init_waiting_for_locks` in the folder's `helpers.rs`); any other error still fails at once, so no extra marks are needed |
| `exactly_one_upgrade_under_a_race_across_the_threshold` (tier upgrade lifecycle) | shamir-wasm-host `src/tests/wasm/engine/upgrade_lifecycle_tests.rs` | `upgrades_completed` read 0 right after the test saw the optimized slot: the upgrade task swapped the slot first and counted afterwards; it now counts before the swap, so whoever sees the optimized tier also sees the counter (the same ordering covers the sibling assertions); no marks needed, the mechanism is deterministic |
| `concurrent_apply_of_the_same_ordinal_lets_exactly_one_migration_run` (application-schema apply fence) | shamir-db `src/shamir_db/tests/ddl_contracts/schema_migrations/apply/apply_fence_tests.rs` | OBSERVED: the journal held 6 events (a `RecoveryAcquired` between `Started` and the first intent): the second applier read the unresolved journal after the first runner's durable `started` but before the first runner registered its in-process gate, took the fence over as a crashed run, and the first runner's later hold/in-doubt conversion could leave a stale in-doubt marker. The in-process gate is now reserved (atomic insert-if-absent) BEFORE the durable fence CAS and owned by one RAII guard, so a durable `started` row never exists without its gate and a live runner is never taken over; the test is deterministic again and an eight-applier variant is added; the mechanism is deterministic, no marks needed |
| `register_makes_entry_observable_and_drop_removes_it` (activity in-flight table) | shamir-server `src/engine/db_handler/tests/handlers/activity/activity_requests_tests.rs` | OBSERVED on windows: `assertion failed: started > 0`. The monotonic request clock's epoch is a lazily initialised `Instant`, so the first stamp of a process can be taken in the same tick as the epoch and read as zero on a coarse clock (every test is its own process under nextest). `monotonic_now_ns` now offsets the reading by one, so a stamp is never zero; the mechanism is deterministic, no marks needed |
| online-backup gate test (CI observed) | shamir-e2e `tests/internals/online_backup/` | OBSERVED failure: the gate writer connection was dropped with a pre-challenge EOF before the auth challenge was served. Source-traced likely cause: the subnet startup limiter (bucket 2/s, burst 2) rejecting a burst of boot connections; the limiter budget is raised in the fixture, and a failure now reports which writer index hit the gate. No fsync or readiness claim. |
| online-backup churn variant (CI observed) | shamir-e2e `tests/internals/online_backup/` | OBSERVED failure: a `BackupStatus` was rejected with `rate_limited` before status handling — unrelated to startup; no status was returned. `paced_execute` polls with 20 ms pacing and bounded exact retries on `rate_limited`. |
| `async_failure_then_second_start_succeeds_and_engine_stays_usable` (CI observed) | shamir-server `src/engine/db_handler/tests/policy/backup_online/` | OBSERVED failure on Windows only: the registry insert after a failed run expired on the former shared 15 s budget; the phase now has its own 120 s budget inside a 170 s overall deadline with phase marks in the guard timeline. |
| `shell_secure_native_wss_scram_catalogue` | shamir-server `tests/admin/shell/secure_transports.rs` | Observed on Ubuntu and Windows (`c726d2cd6`): `PeerEof` at fixture admin TLS `read auth_ok`. Source/count trace points to the native SCRAM cap of 16 retained sessions per principal: 2 boot + 1 catalogue seed + 12 shell children (including the server-admitted wrong-pin attempt) + 1 scripted REPL. The next admin login was #17; rejection at the cap is inferred (no `max_sessions` logged). The same scenario now runs across independent fixtures with early seeding; the production cap is unchanged and the existing FlakeGuard timeline stays. |
| `public_shutdown_bounds_meta_snapshot_join_without_outer_deadline` (CI observed) | shamir-server `src/engine/server/server_launcher/tests/shutdown/shutdown_deadline_tests.rs` | OBSERVED on Ubuntu: `public shutdown must settle with the blocking pool parked: Elapsed` after the 180 s bound (the runtime thread was free, so an async wait never completed). A green run took about 30 s (audit 10 s, meta snapshot 5 s, store release 10 s, observability), about 50 s since the GC expiry was added. The no-deadline path awaited the token-GC join unbounded, and a GC pass can be queued behind the parked blocking job. The failing log carried no stage evidence, so the stuck stage is INFERRED (the GC pass is the one whose job order against the parked job depends on scheduling; a static trace of the fixture finds no other unbounded pool-dependent wait), not observed. Fixed in code: the GC join is bounded by `CONN_DRAIN_TIMEOUT` in the public path too (reported as `token GC shutdown exceeded`), and the test restarts a GC pass with no await before parking so the queued job is deterministic. Still unbounded in the public path by design, tracked in SHUTDOWN-ORACLES: the scheduled-worker join and the replication stop (it takes the reconcile lock before signalling a follower). Diagnostics: the guard has a 150 s watchdog and a `shutdown stage` debug event marks the start of each phase that can wait without a bound (token GC, replication, scheduled worker, reapers), so the ring CAN show the last recorded phase of a recurrence (it is bounded and may degrade). |

A test is added to the table when it flakes; the fix for a new flake includes adopting the
guard.
| `explained_commands_change_nothing_on_disk` (shell maintenance explain) | shamir-server `tests/admin/shell/maintenance/explain/explain.rs` | OBSERVED on Windows (engine-cut-tx full tier): the content hash of `audit.log` changed between the two reads although the shell opens nothing for an explained command; the only writer is the RUNNING fixture server (its audit flush landed inside the window on a 17 s test). Fixed in the test: the live server's audit log is no longer content-hashed but checked as append-only (the old bytes must stay a prefix); every other file is still hashed whole. The failure message already prints both full maps. |

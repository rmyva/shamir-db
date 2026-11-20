# Engine Metrics (release-1 R4a)

- Status: design — proposal
- Date: 2026-11-15
- Scope: metric catalogue, atomic histogram primitive, export path, cost proof, tests, implementation plan
- Companion brief: `docs/dev-artifacts/prompts/release/09-r4a-engine-metrics-design.md`
- This document changes no source; design only for R4a — R4b (activity views, cancellation) and R4c (tracing) are out of scope except a boundary note (§8).

**Decisions (summary).**

1. New leaf crate `shamir-metrics` in the foundation group; zero dependencies beyond `std`.
2. Fixed static histogram buckets with a seqlock-epoch torn-free snapshot; no metrics library in the engine.
3. Export path stays "engine keeps state, server polls": per-scrape sampling in `metrics_handler`, no locks or new background threads on engine hot paths.
4. Cardinality caps: 64 tables / 32 indexes per repo with `(other)` aggregation and truncation flags; op-level histograms never carry a `table` label.
5. Cost gate: ≤ 3 % median regression on existing benches, compared by two CI runs (baseline vs metrics commit); nothing built or run locally.

## 1. Inventory (OBSERVED)

### 1.1 What /metrics exports today

- Observability HTTP server in `crates/server/shamir-server-security/src/security/observability/` (`mod.rs`): routes `/healthz`, `/readyz`, `/metrics`, `/info` registered in `spawn_with_byte_budget` (`mod.rs:516-521`); handlers `healthz` (`mod.rs:579`), `readyz` (`mod.rs:584`), `metrics_handler` (`mod.rs:592`), `info_handler` (`mod.rs:616`). Default bind `127.0.0.1:9090`, loopback-only unless `allow_public_metrics` (`mod.rs:200-212`, `307-309`).
- Prometheus rendering via the `metrics_exporter_prometheus` recorder (`mod.rs:52`, installed `mod.rs:316-324` with login buckets from `login_histogram.rs:8-10`), rendered at `mod.rs:598`; process metrics via `metrics_process::Collector` (`mod.rs:53`, `328-329`).
- Families exported today, by defining module:
  - `mod.rs`: process series (poller `mod.rs:472-508`); `shutdown_unclean_total` (`mod.rs:340-345`); `auth_attempts_total{result}` × 8 fixed result labels (`mod.rs:346-364`); tx/gc gauges bridged from `shamir_tx::TxMetrics` — `shamir_tx_started_total`, `shamir_tx_committed_total`, `shamir_tx_aborted_{ssi,expired,storage}_total`, `shamir_gc_runs_total`, `shamir_gc_entries_deleted_total` (`mod.rs:367-397`, set in the 5 s poller `mod.rs:482-491`); `shamir_inflight_response_bytes_used/_cap` (`mod.rs:402-417`); `shamir_degraded_indexes_total` (`mod.rs:424-433`); per-repo gate gauges `shamir_gate_*` (12 series, `mod.rs:721-772`) and orphan-replay counters (`mod.rs:778-787`).
  - login metrics in `security/perimeter/http_login/metrics.rs` (`shamir_login_http_*`, lines 38-73).
  - `visibility_export.rs` (VIS-4c): `shamir_visibility_*` gauges/counters plus manual cumulative `*_bucket` + `*_count` histograms, labels `db`,`repo` (`visibility_export.rs:40-231`).
  - `durability_export.rs`: `shamir_wal_*` / `shamir_replication_*` / `shamir_journal_*` gauges and counters rendered from the engine snapshot `DurabilityMetricsSnapshot { gauges: [u64; 11], counters: [u64; 8], journal: [u64; 6] }` (`durability_export.rs:7-155`; snapshot struct `crates/engine/shamir-engine/src/repo/journal/backend/durability_policy/durability_metrics.rs:46-50`).
- shamir-sync: `shamir_repl_resync_required_total` (`crates/server/shamir-sync/src/sync/replication/supervisor/start.rs:458,487,517`) plus `shamir_repl_identity_mismatch_total{reason}`, `shamir_token_request_duration_seconds{endpoint}`, `shamir_token_request_denied_total{endpoint}`, `shamir_token_active_ws`, `requests_in_flight` and its own `auth_attempts_total{result}` (identity_fence/checks.rs, replication/token_loop.rs, teardown.rs, handshake.rs, in_flight_guard.rs).
- Naming conventions OBSERVED: `shamir_` prefix, `_total` counters, `_seconds` histogram durations, `_bytes` with `Unit::Bytes` (`durability_export.rs:120-124` uses raw `_ns` + `Unit::Nanoseconds`), gauges set with `gauge!(..).set`, counters pushed with `.absolute(v)` (max-raise semantics documented `mod.rs:773-782`); labels in use: `db`, `repo`, `result`, `mode`, `reason`, `kind`, `class`, `le`, `endpoint`, `channel`.
- Rendering pattern ("engine keeps state, server polls"): per-scrape sampling — `metrics_handler` calls `sample_gate_metrics(db)` before rendering (`mod.rs:592-598`, `702-711`); background 5 s poller for process/tx/byte-budget/degraded-index (`mod.rs:468-490`). `open_gate_snapshots(db)` (`mod.rs:681-700`) walks `db.list_dbs()` (`crates/engine/shamir-db/src/shamir_db/shamir_db/core.rs:843`) → `instance.list_repos()` (`crates/engine/shamir-engine/src/db_instance/db_instance.rs:147`). Open-registry only: closed repos/tables are invisible (documented `mod.rs:419-423`). No per-table enumeration exists in the exporter.
- Cardinality guard today: none numeric; structural only (fixed closed enum label sets `visibility_export.rs:25-37`; only registered repo names enter label space `mod.rs:719-720`).
- Docs/tests today: `docs/guide-docs/guide/07-operations.md` §3 (line 700, metric table 722-750) and §10 (line 960); no Grafana dashboard or alert-rule files exist in the repo (only a prometheus.yml snippet in `deploy/README.md:123-134`). Tests: `observability/tests/{visibility_export_tests,durability_export_tests,gate_tests}.rs`, e2e `crates/server/shamir-server/tests/misc/audit_log/observability_http.rs`, `shamir-tx` metrics tests (`src/tx/support/tests/metrics_tests.rs`, `src/version/visibility_metrics/tests/`).

### 1.2 Counters that exist but are not exported

- `TxMetrics` (`crates/storage/shamir-tx/src/tx/support/core/metrics.rs:7-27`, snapshot `:81-99`): 11 atomic fields; 7 are bridged today (`mod.rs:482-490`); `txs_aborted_unique`, `txs_aborted_phantom`, `txs_materialization_deferred`, `post_wal_orphans_total` are counted but unexported. Increment sites: `transactions.rs:83` (start), `tx/commit/pipeline/phase/commit/legacy_and_lockfree.rs:44,153,166,173,302,376` (committed/deferred), `phase/finalize.rs:91`, `tx/preparation/pre_commit/locked.rs:68,99,141,246,279,314` + `claims.rs:184` (ssi/phantom), `prelock.rs:537,708,721` (unique), `phase/commit/entry.rs:617` (expired), `tx/commit/acknowledgement/tail.rs:97` (storage), `version/version_guard.rs:185,226` (post-wal orphans), `lifecycle.rs:470` (gc).
- WAL: `WalSegment` `bytes_written` (`crates/storage/shamir-wal/src/segment/wal_segment.rs:169`, `approx_len_bytes()` `:464`), `WalSink.sealed_retained_bytes()` (`wal_sink.rs:430`), `SegmentSet` append/truncate/sync APIs (`segment_set.rs:328,590,680,691,706,730`). No segment-count API, no fsync timing anywhere in shamir-wal.
- Engine group commit: `GroupCommit { state: Mutex<GcState> }` with `leader_busy`/`waiters` only, no counters (`crates/engine/shamir-engine/src/repo/journal/backend/group_commit/group_commit.rs:13-22`).
- Persist counter: `FJALL_PERSIST_SYNC_ALL_COUNT` (`crates/storage/shamir-storage/src/stores/storage_fjall/persist.rs:9-31`, test seam).
- Storage sizes: per-store only — `StorageCached::cache_size()` (`storage_cached.rs:629`), membuffer `cache_bytes()` (`storage_membuffer/core.rs:525`); `VersionedOverlay::approx_bytes()` (`crates/storage/shamir-tx/src/version/versioned_overlay.rs:324`), `gc_below`/`gc()` deleted counts (`mvcc_store/history/mvcc_gc.rs:359,573`), gate `pending_versions` (`gate/repo_tx_gate/gate_diagnostics.rs:39`).
- Record counts: `RecordCounter` (`crates/engine/shamir-engine/src/table/support/storage/record_counter.rs:57-223`), persist fan-out registry `persistable.rs:44-48`.
- Indexes: entry counts `IndexManager::entry_count` (`crates/index/shamir-index/src/base_index/index_manager/lookup.rs:632`), sorted `scan.rs:628`, BM25 `doc_count`/`sum_doc_len` (`fts/bm25.rs:85,89`), `IndexInfo::len` (`base_index/meta/index_info.rs:302`), registry len (`shamir-index-core/src/model/runtime/registry.rs:501`). Build duration is Instant-logged only, not a metric (`index_manager/create.rs:486-556`). Vector backend state atomics (`shamir-vector/src/vector_backend/backend.rs:68-164`: full_rebuild_count, delta_count, snapshot_threshold...) and adapter stats (`hnsw_adapter/adapter.rs:51-63`).
- Function host: compile-cache `hits` and `rejected` (a cache-size/trim rejection, not a compile miss; `frames_read` is `#[cfg(test)]`-only) (`crates/wasm/shamir-wasm-host/src/wasm/compile_cache/disk_store.rs:28-32`, increment `:308`), cache size accounting (`entry_index.rs:17-18`), fuel pool/refund/lease (`wasm/fuel.rs:17-48`, per-function state `wasm_function/state.rs:54-61`, consumed at `call_exec.rs:63,107-108`), tier upgrades (`engine/tier_stats.rs:10-14`), call depth guard (`wasm_function/state.rs:38-39`, enforced `call_exec.rs:43-44`). No call-duration metric, no admission-rejection counters.
- Engine drainer/visibility: `Drainer` counters (`crates/engine/shamir-engine/src/tx/drain/drainer/state.rs:31-49`; window depth mirror `window.rs:130-206`), shamir-tx: `VisibilityMetrics` with its own `DurationHistogram` (`crates/storage/shamir-tx/src/version/visibility_metrics/metrics.rs:112-300`), completion tracker counters (`crates/storage/shamir-tx/src/tx/support/core/completion_tracker.rs:140-176`), changefeed repair counters (`crates/storage/shamir-tx/src/changefeed/repair_pending.rs:47-60,113-135`).
- Replication: no lag-in-versions / lag-in-time / reconnect counters exist anywhere (shamir-sync tracks bookmarks/errors only; follower loop `src/sync/replication/follower_loop/loop_body.rs`, supervisor `supervisor/{start,handle}.rs`).
- Interner: `Interner::len()` (`crates/foundation/shamir-types/src/core/interner/interner.rs:479`), quota name counter (`interner/quota.rs:55-94`), persisted high-water `InternerManager::persisted_high_water()` (`crates/engine/shamir-engine/src/table/support/storage/interner_manager.rs:393`).

### 1.3 Hot paths where the new counters must be added

All paths below are inside `crates/engine/shamir-engine/src/` unless noted.

- Read: `table/read/execution/dispatch.rs` `read()` `:229` / `read_impl()` `:275`; collect `table/read/dispatch/collect.rs:30,61`; paged `paged.rs:34,180`; scan `scan.rs:27,95`; vector `vector.rs:33,181`.
- Write: `write/insert.rs:105`, `write/set/execute.rs:48`, `write/update/execute.rs:39`, `write/delete/execute.rs:30`.
- Batch: `query/batch/exec/batch_execute.rs` `execute_batch` `:94`, `execute_plan_impl` `:562`, `execute_transactional_impl` `:788`; existing slow-op timing hook `warn_if_op_slow` `:43`. Runner `query/batch/runner/query_runner/runner.rs:91,281`.
- Commit: `tx/commit/pipeline/phase/commit/entry.rs` `commit_tx` `:582`, `commit_tx_inner` `:601`; driver `driver/commit_driver.rs:32,52`; WAL append `segment_set.rs:328`.
- Byte metering precedent: `ReadByteMeter` trait (`crates/engine/shamir-engine-query/src/query/filter/evaluation/runtime/core/read_budget.rs:15`) consumed at `dispatch/paged.rs:106,116,316,365,399` and `collect.rs:212,223,235,261,273,296,404`, batch impl `query/batch/exec/exec_scope.rs:284` — "records viewed/returned" can reuse these meters instead of new plumbing.
- Index probe: `lookup.rs:632` area; function call: `call_exec.rs` (depth-0 entry).

## 2. The metric catalogue (PROPOSED)

Conventions follow §1.1: `shamir_` prefix, `_total` counters, `_seconds` histogram durations, `_bytes` sizes. Stage column: **R4a-1** = primitive + queries + load; **R4a-2** = storage, WAL, indexes, interner, functions, replication; **R4a-3** = dashboard/alerts/guide only, no new series.

### 2.1 Queries (R4a-1)

| Metric | Type | Unit | Labels | Source | Hot-path cost | Normal range |
|---|---|---|---|---|---|---|
| `shamir_query_duration_seconds` | histogram | seconds | `db`,`repo`,`op` ∈ {read,write,batch,tx,function} | NEW — observed at the §1.3 entry fns | one clock read + one histogram observe per op | 1 ms – 1 s |
| `shamir_query_errors_total` | counter | ops | `db`,`repo`,`code` | NEW — at the `DbResult` error mapping | one `fetch_add` on the error branch only | near 0; spikes on incidents |
| `shamir_query_rows_viewed_total` | counter | rows | `db`,`repo` | NEW — fed from the existing `ReadByteMeter` sites (§1.3) alongside bytes, or new `AtomicU64` at the same points | folded into existing meter calls | proportional to scan size |
| `shamir_query_rows_returned_total` | counter | rows | `db`,`repo` | NEW — same meter sites, at the return boundary | folded into existing meter calls | ≈ rows requested per page |
| `shamir_tx_aborted_unique_total` | counter | txns | — | existing `TxMetrics.txs_aborted_unique` (§1.2); bridge in the 5 s poller | none (poller) | rare |
| `shamir_tx_aborted_phantom_total` | counter | txns | — | existing `TxMetrics.txs_aborted_phantom` | none (poller) | rare |
| `shamir_tx_materialization_deferred_total` | counter | txns | — | existing `TxMetrics.txs_materialization_deferred` | none (poller) | ≤ tx rate |
| `shamir_tx_post_wal_orphans_total` | counter | ops | — | existing `TxMetrics.post_wal_orphans_total` | none (poller) | 0 in steady state |

Notes: `shamir_batch_duration_seconds` is deliberately **not** a separate series — `op=batch` in the duration histogram covers it; keep a single series. The tx gap-closure rows are existing counters; zero hot-path cost.

### 2.2 Load (R4a-1)

| Metric | Type | Unit | Labels | Source | Hot-path cost | Normal range |
|---|---|---|---|---|---|---|
| `shamir_connections_active` | gauge | conns | — | shared `ConnLimiter::live()`; counts guards even at cap 0, including initial handshakes and guarded HTTP sockets, not the observability listener | sampled; guard acquire/drop updates live count | 0 – limit if finite |
| `shamir_connections_limit` | gauge | conns | — | `ConnLimiter::cap()`, shared across data listeners | none (sampled) | configured cap; 0 = unlimited |
| `shamir_connections_rejected_total` | counter | refusals | — | `ConnLimiter::rejected_total()`; one per refused global-cap acquisition, excludes per-IP cap; resets at restart | one `fetch_add` on global refusal | zero increase/rate; growth = cap hit |
| `shamir_inflight_response_bytes_used` | gauge | bytes | — | `ByteBudget::used()`; reserved bytes, not RSS | none (sampled) | 0 – cap if finite; temporary overage on correction or oversized escape; 0 if unbounded/untracked |
| `shamir_inflight_response_bytes_cap` | gauge | bytes | — | `ByteBudget::cap()` | none (sampled) | configured cap; -1 = unbounded/absent |
| `shamir_response_budget_waiters` | gauge | reservations | — | `ByteBudget::waiters()`; concurrent acquisitions on contended path, recheck availability, not FIFO | atomic increment/decrement on contended path only | 0; persistent nonzero = budget pressure |
| `shamir_post_auth_rate_limit_rejected_total` | counter | reqs | — | native and token REST/WS per-session post-auth bucket refusals; excludes gate/auth and scope/policy denials | one counter increment per refusal | zero increase/rate; growth = per-session throttling |

Notes: Sampled per scrape from this boot's sources. The two `shamir_inflight_response_bytes_*` names are retained without aliases; no global 5 s budget poller. Native auth-init in the sync handshake already uses `auth_attempts_total{result="rate_limited"}`; the token admission gate uses `shamir_token_auth_total{channel,result="rate_limited"}`. `shamir_token_request_denied_total{endpoint}` is for scope/policy denials, not post-auth throttling.

Omitted: `shamir_sessions_active{db}` (no bound database on the observation path; global `shamir_active_sessions` unchanged), `shamir_admission_queue_depth` (no common queue), and `shamir_argon2_{in_flight,waiters,capacity}` (no production caller acquires the boot semaphore: SCRAM client runs the KDF and server verifies the proof without it; the separately gated SQL hashing function is not this signal).

### 2.3 Storage (R4a-2)

| Metric | Type | Unit | Labels | Source | Hot-path cost | Normal range |
|---|---|---|---|---|---|---|
| `shamir_repo_disk_bytes` | gauge | bytes | `db`,`repo` | NEW read-path accessor summing the repo's store sizes | none — cold path, scrape-time only | bounded by disk |
| `shamir_table_records` | gauge | records | `db`,`repo`,`table` | existing `RecordCounter` (§1.2) | none (sampled) | per-table population |
| `shamir_table_disk_bytes` | gauge | bytes | `db`,`repo`,`table` | NEW per-table store size accessor (cold path) | none | per-table share |
| `shamir_gc_pending_versions` | gauge | versions | `db`,`repo` | gate diagnostics `pending_versions` (`gate_diagnostics.rs:39`) | none | small; growth = GC lag |

Explicit note: `shamir_gate_pending_versions` is already exported as part of the `shamir_gate_*` family (`mod.rs:721-772`). This catalogue does **not** add a duplicate series — `shamir_gc_pending_versions` is treated as the same measurement; the canonical exported name remains the existing `shamir_gate_pending_versions` (see open question 7, §9).

| Metric | Type | Unit | Labels | Source | Hot-path cost | Normal range |
|---|---|---|---|---|---|---|
| `shamir_tx_staged_bytes` | gauge | bytes | `db`,`repo` | existing `staged_bytes()` accessor on the tx layer | none (sampled) | bounded by inflight txns |

### 2.4 WAL (R4a-2)

| Metric | Type | Unit | Labels | Source | Hot-path cost | Normal range |
|---|---|---|---|---|---|---|
| `shamir_wal_bytes_total` | counter | bytes | `db`,`repo` | existing `WalSegment.bytes_written` (`wal_segment.rs:169`) | none (bridge) | grows with write rate |
| `shamir_wal_size_bytes` | gauge | bytes | `db`,`repo` | `approx_len_bytes()` (`wal_segment.rs:464`) + `sealed_retained_bytes()` (`wal_sink.rs:430`) | none (sampled) | retention-config bound |
| `shamir_wal_segments` | gauge | segments | `db`,`repo` | NEW counter in `SegmentSet` — segment count maintained on seal/truncate; `AtomicUsize` mirror per the CLAUDE.md O(1) rule | O(1) increment at seal/truncate | single digits to tens |
| `shamir_wal_fsync_seconds` | histogram | seconds | `db`,`repo` | NEW: measure around the sync/fsync site in `SegmentSet::sync` / group-commit leader | one clock read per fsync, off the per-append path | sub-ms local disk |
| `shamir_wal_truncation_lag_versions` | gauge | versions | `db`,`repo` | from `has_truncatable(durable)` state: first truncatable version − durable | none (sampled) | small; growth = truncate backlog |
| `shamir_group_commit_waiters` | gauge | waiters | `db`,`repo` | existing `GcState.waiters` (`group_commit.rs:13-22`) | none (sampled) | < commit concurrency |

Existing `shamir_journal_*` / `shamir_wal_sync_*` durability series are already exported from `DurabilityMetricsSnapshot` (§1.1, `durability_export.rs`); cross-reference, no new work.

### 2.5 Indexes (R4a-2)

| Metric | Type | Unit | Labels | Source | Hot-path cost | Normal range |
|---|---|---|---|---|---|---|
| `shamir_index_entries` | gauge | entries | `db`,`repo`,`table`,`index` | existing entry-count APIs: `IndexManager::entry_count` (`lookup.rs:632`), sorted `scan.rs:628`, BM25 `doc_count` (`fts/bm25.rs:85`) | none (sampled) | table-row order |
| `shamir_index_build_seconds` | histogram | seconds | `db`,`repo`,`kind` | NEW: wrap the `Instant` already present at `index_manager/create.rs:486-556` into an observe at completion | one clock read + observe per build (cold) | seconds, build-sized |
| `shamir_index_rebuild_seconds` | histogram | seconds | `db`,`repo`,`kind` | same site, rebuild flag | as above | seconds |
| `shamir_vector_snapshot_state` | gauge | state | `db`,`repo`,`index`,`state` | vector backend atomics (`shamir-vector/src/vector_backend/backend.rs:68-164`) | none (sampled) | enum state |
| `shamir_degraded_indexes_total` | counter | indexes | — | already exported (`mod.rs:424-433`) — cross-ref, no new work | — | 0 in steady state |

### 2.6 Interner (R4a-2)

| Metric | Type | Unit | Labels | Source | Hot-path cost | Normal range |
|---|---|---|---|---|---|---|
| `shamir_interner_names` | gauge | names | `db`,`repo` | `Interner::len()` (`interner.rs:479`) — a map-length read, not an iteration; scrape-time only | none | grows with distinct names |
| `shamir_interner_high_water` | gauge | names | `db`,`repo` | `InternerManager::persisted_high_water()` (`interner_manager.rs:393`) | none (sampled) | monotone-ish |

Alert intent: growth of `shamir_interner_names` without bound (quota name counter `interner/quota.rs:55-94` gives the reference point).

### 2.7 Functions (R4a-2)

| Metric | Type | Unit | Labels | Source | Hot-path cost | Normal range |
|---|---|---|---|---|---|---|
| `shamir_function_duration_seconds` | histogram | seconds | `db`,`repo` | NEW at `call_exec.rs` depth-0 exit | one clock read per call | µs – ms |
| `shamir_function_fuel_consumed_total` | counter | fuel | `db`,`repo` | NEW: sum pool_take − refund at lease boundaries (`wasm/fuel.rs:17-48`, consumed `call_exec.rs:63,107-108`) | folded into existing fuel accounting | workload-sized |
| `shamir_compile_cache_hits_total` | counter | calls | — | existing hit counter increment (`compile_cache/disk_store.rs:308`) | none (bridge) | most calls after warm-up |
| `shamir_compile_cache_misses_total` | counter | calls | — | NEW increment at the compile path (miss = compile happens) | one increment per compile (cold) | near 0 in steady state |
| `shamir_compile_cache_bytes` | gauge | bytes | — | existing cache size accounting (`compile_cache/entry_index.rs:17-18`) | none (sampled) | cache budget |
| `shamir_compile_cache_entries` | gauge | modules | — | existing (`compile_cache/entry_index.rs:17-18`) | none (sampled) | cache budget |
| `shamir_module_admission_rejected_total` | counter | modules | `reason` | NEW: increment at the admission gate error returns | one increment on reject (cold) | 0 |
| `shamir_function_call_depth_high_water` | gauge | frames | `db`,`repo` | from the existing depth guard (`wasm_function/state.rs:38-39`, enforced `call_exec.rs:43-44`); keep max semantics | none (max-raise on guard hit) | < configured limit |

### 2.8 Replication (R4a-2)

| Metric | Type | Unit | Labels | Source | Hot-path cost | Normal range |
|---|---|---|---|---|---|---|
| `shamir_replication_lag_versions` | gauge | versions | `db`,`repo` | NEW: engine receipt/bookmark delta, computed at scrape from durable vs replicated watermark — the watermark pair already exists at `tx/replication/follower_durability/receipt/apply.rs:306` (`bookmark.max(durable_bookmark)`) | none (sampled) | small; growth = follower behind |
| `shamir_replication_lag_seconds` | gauge | seconds | `db`,`repo` | NEW: now − last receipt time; needs a `last_receipt_ns` `AtomicU64` at the follower-apply site (`tx/replication/follower_durability/receipt/apply.rs`) | one store at apply | sub-second in sync |
| `shamir_replication_stream_state` | gauge | state | `db`,`repo`,`state` | follower loop state (`shamir-sync/src/sync/replication/follower_loop/loop_body.rs`) | none (sampled) | steady state |
| `shamir_replication_reconnects_total` | counter | reconnects | `db`,`repo` | NEW increment at supervisor reconnect (`supervisor/{start,handle}.rs`) | one increment per reconnect | 0 in stable networks |
| `shamir_repl_resync_required_total` | counter | resyncs | — | already exported (`supervisor/start.rs:458,487,517`) — cross-ref, no new work | — | 0 |

### 2.9 Copies (R2d) — boundary note only

`shamir_backup_*` names are reserved from now (see open question 6); the series themselves are delivered with R2d and are not part of R4a.

## 3. The primitive: fixed-bucket atomic histogram (PROPOSED)

Decision: a NEW tiny crate `shamir-metrics` in the foundation group (`crates/foundation/shamir-metrics`). Justification:

- (a) foundation currently holds 6 crates (`shamir-collections`, `shamir-numa`, `shamir-private-dir`, `shamir-proc-tree`, `shamir-tunables`, `shamir-types` — verified in the worktree) against a group cap of 7, so the new crate is the last slot and the cap is respected.
- (b) Every engine layer (future `shamir-engine-repo`, `-table`, `-tx`, the facade, `shamir-engine-query`, `shamir-tx`, `shamir-wal`, `shamir-wasm-host`) can depend on it without a cycle: it has ZERO dependencies except `std` — no tokio, no wasmtime, no metrics library. Engine principle: no metrics library in the engine, per `docs/dev-artifacts/prompts/release/04-observability.md` §2.
- (c) `shamir-collections` is the wrong home: it is a data-structures crate with its own API stability, and the histogram needs loom-gating semantics of a synchronization primitive, not a collection.

Dependency graph sketch:

```
                 shamir-metrics  (foundation leaf, std-only)
                /    |    \      \
   engine-repo  engine-table  engine-tx   ...   (future split crates)
        \          |             /    \
      shamir-engine-query   shamir-tx   shamir-wal   shamir-wasm-host
                 \             |         /
              shamir-engine (facade / RepoInstance shell)
                          |
                    shamir-db, server crates
```

The SERVER side never depends on it for reading values except via snapshots: the exporter reads `HistogramSnapshot` values, never the live histogram internals.

### 3.1 API (exact signatures)

```rust
pub struct Histogram { /* fixed layout, see §3.2 */ }
impl Histogram {
    pub const fn new(bounds: &'static [u64]) -> Self;      // ns or bytes, static bounds
    pub fn observe(&self, v: u64);                          // hot path: bucket search + fetch_add (+sum/count)
    pub fn snapshot(&self) -> HistogramSnapshot;            // reader: torn-free, see §3.4
}
pub struct HistogramSnapshot {
    pub bounds: &'static [u64],
    pub buckets: Vec<u64>,
    pub count: u64,
    pub sum: u64,
    pub torn: bool,
}
```

### 3.2 Bucket layout

Latency buckets: static bounds in nanoseconds, 19 bounds, ~powers pattern 100 µs … 60 s, chosen here rather than copied from the login histogram (`login_histogram.rs:8-10`: 12 bounds, 0.05–120 s, clustered around the Argon2 KDF latency — the wrong shape for query latencies):

```
100_000, 250_000, 500_000, 1_000_000, 2_500_000, 5_000_000, 10_000_000,
25_000_000, 50_000_000, 100_000_000, 250_000_000, 500_000_000,
1_000_000_000, 2_500_000_000, 5_000_000_000, 10_000_000_000,
20_000_000_000, 40_000_000_000, 60_000_000_000
```

Byte buckets: 12 bounds, 1 KiB → 1 GiB, powers of 2:

```
1_024, 2_048, 4_096, 16_384, 65_536, 262_144, 1_048_576, 4_194_304,
16_777_216, 67_108_864, 268_435_456, 1_073_741_824
```

Overflow bucket implicit: values above the last bound are counted in one extra cell so `le=+Inf` renders correctly. A bucket cell counts observations `v <= bound` (inclusive, Prometheus `le` semantics); the last rendered bucket is `le="+Inf"`. `sum` is an integer in the bucket unit (ns, bytes) — no float, and u64 ns overflow is unreachable for realistic rates.

### 3.3 Bucket search and update

Chosen approach: linear scan over a small const array of u64 with early exit; cost target ≤ ~20 ns; measured in the primitive's own bench (§5). Branchy binary search is fine at 12-20 bounds and is the fallback if the linear scan fails the bench, but 20 short u64 comparisons fit a cache line pair and predict well. Update: single `fetch_add(Relaxed)` per bucket plus one each for count and sum — three Relaxed RMWs per `observe`.

Memory per instance: (n_bounds + 1 overflow) × 8 B + count + sum + started + completed = ~192 B (20 cells × 8 B = 160 B, + count + sum + two seqlock counters, before alignment). Instances live in engine structs (per repo / per op kind), bounded by the cardinality policy (§4).

### 3.4 Snapshot semantics (torn-free reader): two-counter seqlock

- Writer: `started.fetch_add(1, Relaxed)`; `fence(Release)`; the three Relaxed `fetch_add`s (bucket cell, count, sum); `completed.fetch_add(1, Release)`.
- Reader attempt: `c1 = completed.load(Acquire); s1 = started.load(Relaxed); if s1 != c1 { retry }`; otherwise copy cells, count and sum with Relaxed loads; `fence(Acquire)`; `s2 = started.load(Relaxed)`; accept only if `s2 == s1`, else retry.
- Soundness: equal counters at the first read mean no writer was in flight at the moment of the `started` read; an unchanged `started` after the copy means no writer began during it. The writer's Release fence before the data writes and the reader's Acquire fence before the final `started` read order any visible data write of a late writer before that read — so it forces `s2 != s1` and the attempt retries.
- Why not one odd/even epoch: with concurrent writers A and B the epoch goes 0→1→2, i.e. EVEN while both are mid-update, so a reader can copy half-updated state (A's writes visible, B's not) and return a supposedly consistent snapshot. Two independent counters make "in flight" unambiguous under any number of writers.
- Bounded retries (e.g. 8), then return a best-effort copy with `torn: true` rather than blocking a scrape. The exporter renders only `torn == false` snapshots — it drops the sample for that instance on that scrape, mirroring the visibility-family precedent of closed-registry omission. The counters are u64: wrap is unreachable in practice (2^64 increments). A writer preempted mid-update leaves `started > completed` for the duration; readers burn their retry budget and drop that instance's sample for the scrape — the same degradation path as `torn: true`, not a hang.
- Rejected alternative: per-bucket consistency without a seqlock — a reader can mix pre/post-observe buckets and sum/count can disagree with buckets, violating the "never torn" requirement for a published snapshot pair.

### 3.5 loom-testability

The snapshot protocol must stay small enough for a loom model. Add a `loom_model` test in the crate (pattern precedent `crates/engine/shamir-engine/src/table/support/guards/tests/loom_model.rs`): writer thread observes 2 values, reader snapshots, loom asserts the snapshot is a consistent prefix (all buckets ≤ their post-state). The CI `loom-model` gate pattern applies: `cargo nextest list -E 'test(loom_model)' --features loom` with `--no-tests=fail`.

### 3.6 Export rendering

Snapshot → manual cumulative `*_bucket` gauges with `le` labels + `*_count`/`_sum` counters, exactly the visibility-family pattern (`visibility_export.rs:18-23`, `143-151`), so there is no dependency on the recorder's histogram API and the bucket sets are ours.

## 4. The export path (PROPOSED)

- Principle (from 04-observability.md §2): engine keeps state, server polls. New file `engine_export.rs` in `crates/server/shamir-server-security/src/security/observability/` (the dir has 3 files + `mod.rs` plus `logging/` and `tests/` subdirs today — fits the ≤ 7 entries rule), rendered per-scrape inside `metrics_handler` alongside `sample_gate_metrics` (`mod.rs:592-598`). No locks added to engine hot paths: all state is atomics; the exporter only reads snapshots.
- Discovery of repos: reuse `open_gate_snapshots`'s walk (`db.list_dbs()` → `instance.list_repos()`, `mod.rs:681-700`). Tables: extend with `repo.list_table_names()`. Verified accessor: `RepoInstance::list_table_names()` at `crates/engine/shamir-engine/src/repo/instance/repo_instance/catalog.rs:151` (delegating to `self.core.list_table_names()`), with `table_count()` at `catalog.rs:171`. Open-registry only, same documented limitation as gate/degraded-index (`mod.rs:419-423`).
- Cardinality policy (answers open question 1 of 04-observability.md §7):
  - Per-repo limit N = 64 tables and N = 32 indexes with per-table labels; selection by current record count (`RecordCounter` — a population gauge, not a write rate; tie-break by name — deterministic).
  - The remainder is aggregated into one `table="(other)"` series per repo **for gauges** (records, bytes) and is **excluded from histograms** — op-level histograms carry only `db`/`repo` labels, never `table`, so histogram cardinality is bounded structurally.
  - Companion gauges `shamir_export_tables_truncated{db,repo}` (1/0) and `shamir_export_tables_known{db,repo}` (`_known`, not `_total`: the `_total` suffix is reserved for counters in the house naming, §1.1) tell the operator whether truncation is active and how many tables exist; document the limit and the deterministic selection in the ops guide.
  - Note the scrape's own cost shape: `repo.list_table_names()` materialises every open table name (O(total tables) strings per scrape) before the 64×O(1) atomic reads — acceptable at scrape frequency, but it is not O(64). A table crossing the N-name boundary between scrapes changes the label set; Prometheus `rate()` sees a reset on the affected series — inherent to capped cardinality, mitigated by `(other)` + the known-tables gauge.
  - N is a tunable constant, not runtime config (v1). Rejected alternative: runtime config — adds a settings surface with no operator need yet.
- Time cost of one scrape: per open repo, one pass over ≤ (64 + 32) tables × O(1) atomic reads + O(N) interner len. Estimate arithmetic: thousands of tables on one repo → 64 table rows + 2 summary rows ≈ a few hundred Prometheus series per repo; scrape cost is dominated by series count (~10⁴ text lines worst case ≈ well under a millisecond-scale formatting cost — stated as an estimate, verified by the R4a-2 test that scrapes a repo with > 64 tables).

## 5. Cost proof plan (PROPOSED)

| Hot path | Existing bench | File |
|---|---|---|
| read dispatch | backend matrix, query reads | `crates/engine/shamir-engine/benches/backend/backend_matrix.rs` + `benches/query_reads/*` |
| writes / batch | batch ops, constraints | `crates/engine/shamir-engine/benches/query_writes/{batch_ops,constraints}` |
| commit / WAL | durable commit, writer latency, drain | `crates/engine/shamir-engine/benches/durability/{durable_concurrent_commit,f78_writer_latency,drain_throughput}` |
| storage | storage benches, tx overhead, wal append | `crates/storage/shamir-storage/benches/*`, `shamir-tx/benches/tx_overhead.rs`, `shamir-wal/benches/wal_append.rs` (Harness import `:47`) |
| index probe | posting cache, reader drain gate | `crates/index/shamir-index/benches/{posting_cache_hit.rs (:39), reader_drain_gate}` |
| function call | warm start, metering, lang scale | `crates/wasm/shamir-wasm-host/benches/wasm_procedures/{warm_start,metering}` + `benches/wasm_scale/lang` |
| interner | concurrent interner | `benches/storage_concurrency/buffers_and_interner/interner_concurrent.rs` |

All use `bench_scale_tool::Harness` (workspace convention, no Criterion).

- Primitive micro-bench: add `crates/foundation/shamir-metrics/benches/histogram_observe.rs` (Harness) measuring `observe` ns/op. Acceptance: ≤ 50 ns/op (one clock-free update; compare against a bare `fetch_add`).
- Acceptance threshold for "no significant slowdown": ≤ 3 % median regression on any affected bench cell, using the fixed-iteration harness (committed `bench-iters.txt`) and the same target-dir isolation (`CARGO_TARGET_DIR` bench dir per CLAUDE.md).
- Before/after is taken in CI only: bench jobs run via the standard bench invocation on the PR; the comparison is the orchestrator's task (two CI runs, baseline commit vs metrics commit), never local (compile/build/test policy). Explicitly: benches run in CI; locally nothing is built or run (policy).

## 6. Tests and acceptance (CI only)

Per stage, named tests with crate + fixture; OS needs noted per item.

- R4a-1:
  - (a) `shamir-metrics` unit tests: bounds mapping, overflow bucket, seqlock snapshot under loom (`tests/loom_model`), snapshot torn flag. All OS (pure std).
  - (b) `shamir-engine` query-metrics test: run a read + insert + batch through the facade fixture (existing test helpers); assert histogram count == ops, rows counters match fixture row counts. Ubuntu+Windows (existing engine test fixture matrix).
  - (c) `shamir-server-security` export test: `engine_export_tests.rs` — zero-touch registration exposes every new series (pattern `visibility_export_tests.rs:24-45`), label count exact (pattern `durability_export_tests.rs:7-52`). Ubuntu only is acceptable for pure text rendering, but keep to the existing observability test placement: Ubuntu only, mirroring the current observability tests, which are not OS-specific but run wherever the crate's lib tests run.
  - (d) Tx gap-closure: extend the `mod.rs` poller tests asserting the 4 new series.
- R4a-2:
  - (a) storage/WAL/index/interner/function/replication tests: each exporter row checked against real state — e.g. write 5 records → `shamir_table_records{...}` == 5 (existing `RecordCounter` fixture); seal a WAL segment → `shamir_wal_segments` matches `SegmentSet` state; build an index → `shamir_index_build_seconds_count` == 1; compile-cache hit/miss counters after two calls of one function. Value-vs-state assertions live in the owning crates' test dirs (tests/ layout rules: one tests/ dir per module, `mod.rs` manifest only).
  - (b) Cardinality: a fixture with 70 tables asserts ≤ 64 named + `(other)` + truncation flag series. Ubuntu + Windows (file/WAL semantics differ per OS); macOS not required for R4a-2 value assertions except where existing tests already run on all three (follow existing crate test placement).
- R4a-3: no code tests. Acceptance is the ops-guide review (each catalogue row documented with normal range) + dashboard/alert files present; the fast tier's layout gate covers the JSON/YAML files. Alert rules and dashboard are validated by CI only via JSON parse in the existing workflow if added — otherwise accepted by review. No new CI jobs are proposed (out of scope).

## 7. Implementation plan (PROPOSED)

PR-sized steps, with the layer of each change:

1. **PR-1** `shamir-metrics` crate (foundation layer): histogram + snapshot + loom model + unit tests + bench. No engine changes. (New foundation crate; the workspace `Cargo.toml` members glob `crates/*/*` picks it up automatically.)
2. **PR-2** R4a-1 queries+load (engine query/instance + tx facade + server-security observability): instrument the §1.3 entry fns via a per-repo `EngineMetrics` bundle. Placement: a `metrics` cell on the `RepoInstance` shell reachable by query/batch/write paths without layer violation — the histogram struct lives in `shamir-metrics`, the bundle struct lives in the layer that owns the callers; for the tx/query layers that means the bundle sits in the instance layer and `&EngineMetrics` is passed down as a plain parameter, mirroring the `DurabilityMetrics` pattern of `durability_metrics.rs:33-86`, which already lives at the repo journal backend. Zero new backward edges in `.rust-guard/layers.toml` terms: metrics flow downward only (instance → query → tx → repo); if a lower layer must record, it records into a bundle handed to it from above (parameter), never reaching upward. Add `engine_export.rs` + tests. Bridge the 4 TxMetrics gaps.
3. **PR-3** R4a-2 storage+WAL (storage/repo layers): segment-count atomic in `SegmentSet`, fsync timing, size/lag gauges, export rows. WAL counters flow through the existing `DurabilityMetricsSnapshot` channel where possible — extend the `[u64]` snapshot arrays with new fields at the END and render by index constants (the struct is versioned by position).
4. **PR-4** R4a-2 indexes+interner (index/table layers): build/rebuild observe at the `create.rs` Instant sites, entry-count gauges, interner gauges.
5. **PR-5** R4a-2 functions+replication (wasm-host + tx-replication + shamir-sync): call duration/fuel, compile-cache, admission rejections; lag gauges, reconnect counter, stream state.
6. **PR-6** R4a-3 docs+dashboards: `docs/guide-docs/guide/07-operations.md` §3 extension (every metric, normal range), Grafana dashboard + alert rules — proposed file locations: `deploy/grafana/shamir-dashboard.json` and `deploy/alerts/shamir.rules.yml` (new `deploy/grafana/`, `deploy/alerts/` dirs, ≤ 7 entries each), updating `deploy/README.md` pointers (§10 of the guide already points at deploy).

**Layering constraint.** Nothing in the `repo`, `table`, `tx`, `query` layers of shamir-engine may create a backward edge (`.rust-guard/layers.toml`, `docs/dev-artifacts/design/engine-layering.md` §8). All engine-side additions are exactly: (a) leaf-crate `shamir-metrics` types used as field/parameter types (downward), (b) counter increments at existing call sites, (c) snapshot getters on existing structs — no layer references upward. Each PR is cross-checked against the layer order `repo < table < validator < tx < query < instance < db_instance` (layers.toml still pins `validator` between `table` and `tx` even though the layering plan dissolves it).

**Stage overlap.** PR-2 touches `query/batch/exec/batch_execute.rs`, which the running engine-layering stage R2d converts (with `executor_traits.rs`, `interactive_tx.rs`, `receipts/commit.rs`, `query/mod.rs`), and tx/commit files inside the R2c sweep (campaign `engine-layering-R2c1b` active). The metrics PRs in those files must land after the corresponding layering stage lands, or rebase on it — no parallel edits to the same files.

## 8. Boundary note

R4b (activity views, cancellation) and R4c (OTLP tracing) are untouched here. R4c will reuse the query id from R4b and the op-duration histograms of R4a as span boundaries. R4b's form statistics (04-observability.md §4) will use the same `shamir-metrics` primitive for counters but needs its own bounded registry (fixed forms with eviction) — out of scope here.

## 9. Open questions for the owner (each with a default)

1. Cardinality limit N (tables/indexes per repo): default 64/32 as §4; alternatives: 16 (tighter), unlimited (rejected — unbounded series).
2. Histogram bucket sets: fixed static bounds (default, §3) vs per-op dynamic bounds (rejected: memory + complexity, torn semantics harder).
3. Where the per-repo metrics bundle lives: instance-layer parameter (default) vs a global registry the engine pushes into (rejected: global lookup on hot path).
4. Dropping torn samples at scrape (default) vs retrying longer.
5. `shamir_wal_fsync_seconds` measured at every fsync (default) vs sampled 1-in-N (only if the CI bench shows regression above the 3 % gate).
6. Copies metrics: `shamir_backup_*` naming reserved now, delivered with R2d (default) vs deferred naming decision.
7. `shamir_gate_pending_versions` vs `shamir_gc_pending_versions`: keep one canonical name — `shamir_gate_pending_versions` stays, no duplicate series; the catalogue names it once (default).
8. Cost-gate noise floor: two CI bench runs on shared runners may not resolve a 3 % median delta; default — measure the run-to-run noise floor first with two baseline runs, and if it exceeds ~1.5 %, require median-of-3 runs per side before declaring a regression.

## 10. Verification notes

This document was written without modifying source. Two brief claims were checked against the worktree and corrected/cited from observation:

- The snapshot protocol was revised after review (2026-11-15): the single odd/even epoch was replaced by the two-counter (`started`/`completed`) protocol in §3.4 — see §10 above and the crate's `histogram.rs`.

- Foundation group holds 6 crates today (verified by directory listing of `crates/foundation/`), so the 7-crate cap accommodates `shamir-metrics` as the last slot.
- The table-listing accessor is `RepoInstance::list_table_names()` at `crates/engine/shamir-engine/src/repo/instance/repo_instance/catalog.rs:151` (with `table_count()` at `:171`) — the brief's guess of the `catalog.rs:268-293` region was not where the accessor lives; the cited lines above are the verified ones. All other file paths and line numbers are carried from the companion brief unverified at line precision.

## Review

Independent review, 2026-11-15. Claims verified against the worktree; two read-only code checks. Verdict: the design is sound in its decisions (leaf crate, seqlock snapshot, downward parameter plumbing, cardinality caps, CI-only cost gate); the defects below were factual, all fixed in place.

1. [major] Arithmetic — §3.3 said "~176 B for 20 buckets" while §3.2 lists 19 bounds (+1 overflow = 20 cells). Correct total is 20×8 + count + sum + epoch = 184 B. Fixed; also added the one-cache-line contention note (three Relaxed RMWs per observe).
2. [major] False precedent — §3.2 claimed the latency bounds "match the login-histogram precedent"; the login histogram (`login_histogram.rs:8-10`) is 12 bounds 0.05–120 s clustered at the Argon2 latency. Fixed to state the bounds are chosen, not copied, and why.
3. [major] Duplicate series — §2.2 proposed a NEW `shamir_rate_limit_rejected_total`, but `shamir_token_auth_total{result="rate_limited"}` (`gate.rs:87-99`) and `shamir_token_request_denied_total` (`executor.rs:114,194,235`) already carry the signal. Changed to a cross-reference, no new series.
4. [major] Wrong cost claim — §2.6 said `Interner::len()` needs a scrape-side O(N) iteration with an O(N) ack; `interner.rs:479` is a map-length read. Fixed.
5. [medium] Gauge named `_total` — `shamir_export_tables_total` violated the house convention that `_total` is counters-only (§1.1). Renamed `shamir_export_tables_known`.
6. [medium] Stage overlap unstated — PR-2/PR-3 files (`batch_execute.rs`, tx/commit) are the exact files of the running R2c/R2d engine-layering stages (campaign `engine-layering-R2c1b` active in `.rust-guard/stages.toml`). Added an ordering note to §7.
7. [medium] Layer order incomplete — §7's constraint line omitted `validator`, which `.rust-guard/layers.toml` still pins between `table` and `tx`. Fixed with a note that the layering plan dissolves it.
8. [medium] Cost-gate noise — a 3 % median delta from two CI runs may be unresolvable on shared runners. Added open question 8 with default: measure the noise floor first, median-of-3 if it exceeds ~1.5 %.
9. [minor] §1.1 understated shamir-sync exports (five more series exist) and the label inventory (missing `endpoint`, `channel`); both fixed.
10. [minor] §1.2: `frames_read` is `#[cfg(test)]`-only and `rejected` is a trim rejection, not a compile miss — annotated; completion-tracker and changefeed-repair counters are in shamir-tx, not engine — paths fixed.
11. [minor] §2.2 source path: the conn limiter lives in `shamir-sync/src/sync/connection/conn_limiter.rs` (`active` at :30, :88-90), not in server-security — fixed.
12. [minor] §4: scrape walk is O(total tables) name strings before the 64×O(1) reads, and label-set churn at the N-boundary breaks `rate()` on affected series — both now stated; truncation-flag honesty kept (flag + known-tables gauge).
13. [minor] §3.2/§3.4: added explicit `le` inclusivity, integer `sum`, epoch-wrap and writer-preempted (odd epoch → dropped sample) semantics.
14. [settled] Carried-unverified line numbers: §1.3 engine hot paths and §5 bench files were checked line-exact and are CONFIRMED (±1 line on two `ReadByteMeter` sites); §1.2 counters confirmed with the path fixes of finding 10; `RepoInstance::list_table_names()`/`table_count()` at `catalog.rs:151/:171` CONFIRMED; no per-table disk-size accessor exists (the NEW rows are genuinely new); foundation group holds 6 crates, the 7th slot is free; `.rust-guard/layers.toml` order confirmed; `loom_model.rs` precedent exists; all §5 bench files exist and use `bench_scale_tool::Harness`.
15. [open, for the owner] No blocking open item beyond §9 (incl. new question 8). R4a stage mapping to 04-observability.md §6 and the PR-1..6 file/folder limits check out.

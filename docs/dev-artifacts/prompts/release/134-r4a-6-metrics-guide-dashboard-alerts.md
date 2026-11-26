# Brief: R4a step 6 — the operator metrics guide (every exported metric: type, labels, meaning, normal range), the Grafana dashboard, the alert rules, and a CI inventory check that keeps the three in step with the code

Design (settled, read first): `docs/dev-artifacts/design/engine-metrics.md` — §2 (the catalogue and its cardinality rules), §4 (the export path), §7 PR-6 (this step: guide §3 extension, `deploy/grafana/shamir-dashboard.json`, `deploy/alerts/shamir.rules.yml`, `deploy/README.md` pointers), §9 (decisions with defaults). Plan: `docs/dev-artifacts/release-1/04-observability.md` §3 (the alert list the release asks for: disk, replica lag, copy staleness, error-rate growth, long-pinned snapshot) and §6 row R4a-3 (acceptance: "the guide describes every metric and its normal range"). Landed foundations (re-open every path before relying on it; names drift): the shamir-metrics primitive and the export modules in `crates/server/shamir-server-security/src/security/observability/` (`export/engine.rs`, `storage.rs`, `functions.rs`, `index_interner.rs`, `replication.rs`, `export/activity/`, `durability_export.rs`, `visibility_export.rs`, `login_histogram.rs`, `mod.rs`), `security/backup_metrics.rs`, `security/tokens/model/token_counters.rs`, `perimeter/http_login/metrics.rs`, `perimeter/http_rest/`, the sync crate's token-WS counters, the server launcher's process metrics, the current guide table `docs/guide-docs/guide/07-operations.md` §3 `/metrics` and §10 "Prometheus + Grafana", `deploy/README.md`. The other metric-adding release tasks (R2d backup schedule gauges, R4a-2b load series, R4b activity, cancellation counters and shape gauges, the online export of batch 3) are LANDED in the base of this branch (current upstream `master`): the inventory covers them all. Metrics added by later tasks must extend the guide through the inventory check this step writes.

## State of the base (orchestrator, re-verify)

* You start a NEW session in a NEW worktree (`<worktree>`, branch `metrics-guide`, base = upstream `master`). Two other executors work in sibling worktrees at the same time (the shell crate and its integration harness under `crates/server/shamir-server/shell/` and `crates/server/shamir-server/tests/admin/shell/`; the cancellation and cursor paths of `shamir-server`, `shamir-db` and `shamir-query-types`): do not touch those paths, and do not edit any Rust file unless item 5 needs it.
* `deploy/alerts/` already holds `shamir-backup.rules.yml`; `deploy/grafana/` does not exist yet.
* `scripts/ci/gates/` holds seven entries already: the 7-entry folder rule forbids an eighth file there. Put a new gate into a topical subfolder or choose another home that keeps every folder at or below seven entries; count with `ls` before adding.
* The `/metrics` table of `docs/guide-docs/guide/07-operations.md` is the single place the guide documents series; other guide chapters link to it.

## Why

Release gate: "an operator can see what the server is doing and be told when it is unhealthy". The server exports a large set of series, but the guide lists only a part of them, there is no dashboard and no ready alert file. Without an inventory check the guide drifts the day the next metric lands.

## What (this step only)

1. **Inventory first.** Build the list of EVERY series the server can export from the code (the export modules above plus every `shamir_*`/`process_*`/`auth_*` literal on a render path; follow each family's label set and the cardinality truncation rules). Write the inventory into your final report as a table (name, type, labels, source file): it is the cross-check for the guide. Do not guess from the design; the code is the source. A metric that exists in the design but not in the code is NOT documented (list it as missing in the report). A metric in the code that is not rendered anywhere is reported, not documented.
2. **Guide (Russian, `docs/guide-docs/guide/07-operations.md` §3 `/metrics`).** Restructure the flat table into topical subsections (processes and sessions, authentication and tokens, transactions and GC, visibility and the repository gate, WAL and durability, storage, queries and load, indexes and interner, functions, replication, backup, activity) — each row: name, type, labels with value sets, what it measures, the NORMAL range or the shape to expect (rate, level, bucket spread) and what a deviation means with the first thing to check. Every statement must match the code (units, label values, truncation limits such as the 64 tables / 32 indexes per repository cap, histogram bucket bounds read from the code). Keep what is already correct; fix what is wrong (report each correction). §10 gets the pointers to the dashboard and rule files and the wiring steps (Prometheus `rule_files`, Grafana provisioning/import).
3. **Grafana dashboard.** `deploy/grafana/shamir-dashboard.json` (create the folder; at most 7 entries): a datasource template variable, a `db`/`repo` variable where the series carry those labels, rows per topic of item 2, panels with sensible units, `rate()`/`histogram_quantile()` for counters and histograms, no panel for a series the code does not export. Valid dashboard JSON for a current Grafana (state the schema version you target). Keep it reviewable: stable panel ids, no auto-generated noise, a small bounded number of panels per row.
4. **Alert rules.** `deploy/alerts/shamir.rules.yml` (create the folder if the backup-schedule task has not; that task adds `shamir-backup.rules.yml` there, do not duplicate its backup rules) with the alerts of the plan: disk or WAL growth and truncation lag, replica lag in versions and in time, error-rate growth, a long-pinned snapshot / the oldest blocker of the visibility gate, authentication anomalies, queue/limit saturation (only for series that exist), each with a threshold that is documented in the guide as a starting value with the reasoning, a `for:` clause, severity labels and runbook-style annotations in plain words. The existing prose examples of §10 become real rules. Valid Prometheus rule file syntax.
5. **Inventory check in CI.** A deterministic check that fails when (a) a metric name exported by the code is missing from the guide's tables, (b) the guide documents a name the code does not export, (c) a dashboard `expr` or an alert `expr` references a metric name that is neither exported nor a recording rule defined in the same file, (d) the dashboard is not valid JSON or has duplicate panel ids, (e) a rule file is not valid YAML of the Prometheus rule shape (groups → rules with `alert`/`expr`, `for`, labels). Choose the home by these criteria and justify it in the report: it must run in an EXISTING CI job (read `.github/workflows/ci.yml` and `scripts/ci/matrix/`; adding a new job is a pipeline change that triggers canaries: avoid it), must be selected when ANY of the guide, `deploy/grafana`, `deploy/alerts` or the metric source files change (declare the non-Cargo edges in `scripts/ci/matrix/edges.toml` only if the existing selection does not already cover them, with the matrix self-check updated), must need no running server. A Python check under `scripts/ci/gates/` with a unit test under `scripts/ci/tests/`, scanning the Rust sources statically for metric-name literals on the render paths (with an explicit, justified allowlist for strings that are not series), is acceptable; a Rust test in the server-security crate that renders the families and reads the guide by relative path is acceptable if it covers labelled families without a live server. Whatever you choose, check which YAML/JSON libraries the CI Python has (stdlib only? read the workflow and the existing gates before using a third-party import) and say what the check does NOT prove.
6. **`deploy/README.md`:** pointers to the new files and one short "monitoring" section (how to load the rules and the dashboard); the file table lists them.

## Do not

Change any Rust production code, metric name, label set or semantics (if a metric name or label is wrong or confusing, list it in the report instead), add a dependency, raise a version, touch the TypeScript clients, add Grafana panels or rules for series that do not exist, duplicate the backup rules of the schedule task, or widen an existing item beyond what the new files need (list every widening).

## Layout and fixture lessons

Count every folder you add to with `ls` (`deploy/` 8 entries today would become 10 with two new folders: the 7-entry rule applies to source trees only, but keep every NEW folder at most 7 entries and say so in the report; `scripts/ci/gates` and `scripts/ci/tests`: count and plan before adding). `python -I scripts/ci/check_layout.py` counts only tracked files: re-check after `git add -A` at the end. At most 1000 lines per source file (`.py`, `.rs`); the dashboard JSON and the guide are docs/fixtures and exempt, but keep the dashboard generated-looking noise out of it. No private machine paths, user names or CI infrastructure names (other than the workflow files) in any file.

## Rules that bind the change

* This task changes no Rust crate unless you choose the Rust-test variant of item 5: then and only then run cargo as `bash <orch>/cg.sh env RUSTC_WRAPPER= wnice -p idle -c 50 -t 8 -m 50 -- cargo ... -j 1 > <logfile OUTSIDE the repository> 2>&1` (one cargo at a time machine-wide, never `-j 2`, never piped through `tail`/`head`, `cargo fmt` rejects `-j`: use `CARGO_BUILD_JOBS=1`), only `check`/`clippy`/`fmt`.
* Python helper tests and every other test run in CI, never here. You may validate JSON/YAML syntax with a one-off read-only parse only if it needs no build or test runner (`python -I -c` over the files); say what you ran.
* By 120 minutes bring the change to a state you have read through twice and stop; say in the final report what is missing.

## Policy (verbatim, binding)

Compilation, builds and tests run on GitHub CI, not on this machine. Banned locally: `cargo test`, `cargo nextest`,
`./scripts/test.sh`, `cargo build`, `cargo run`, `cargo bench`, `cargo doc`, npm/tsc builds, python test suites, any build or test
run. Allowed locally only: `cargo check -p <crate> --all-targets`, `cargo clippy -p <crate> --all-targets -- -D warnings`,
`cargo fmt -p <crate>` (and `-- --check`), `python -I scripts/ci/check_layout.py`.

⛔ NEVER run `git reset` / `checkout` / `clean` / `stash` / `restore` / `rm`, or any git command that mutates the working
tree or index. Only edit files; the orchestrator commits. Edit only paths inside this worktree; leave no scratch files; never raise
versions.

## Observable acceptance

Every series the code renders is in the guide with type, labels and normal range; the dashboard and the rule files reference only
exported series; the inventory check runs in an existing CI job, is selected by changes to the guide, `deploy/grafana`,
`deploy/alerts` and the metric sources, and has a unit test with a passing and a failing case; layout limits hold with the counts
reported. The final report (one complete standalone report) lists the inventory table, the corrections made to existing guide rows,
the missing or unrendered series found, the touched files with folders and entry counts, every widening, every edge declared, the
home chosen for the check and why, what the check does not prove, and what the orchestrator must observe in CI.

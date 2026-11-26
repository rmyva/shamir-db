# Brief: GUIDE-SHELL-REFERENCE (task #2022) - reconcile the condensed administrative shell chapter of the operations guide with the reference text it replaced

You start a NEW session in a NEW worktree (`<worktree>`, branch `guide-shell-ref`, base = current upstream `master`, which contains release-1 batches 1 to 4). This is a documentation task: the only file you change is `docs/guide-docs/guide/07-operations.md` (Russian). No Rust code, no test, no CI file.

## Why

Release-1 batch 4 (upstream pull request #663, squash `deada74b8`) rewrote the shell chapter of the operations guide into one operator walkthrough (profile and pin, connect, ping and catalogue, state and replication, data views, rights, activity and cancellation, backups and schedule, versions, explained offline commands, online export). The rewrite followed brief 131 item 3 ("merge duplicates, keep every statement true to the code") but it also removed about 1100 lines of reference text written by the earlier shell steps. Some of that text carried contracts that the short walkthrough no longer states. The release gate asks for a reconciliation of the guide with the code.

## What

1. Recover the removed text from git (read-only): `git diff 78e4a8325 deada74b8 -- docs/guide-docs/guide/07-operations.md` (the old sections were, among others, "Административный шелл: состояние и узкие просмотры данных (R5 PR3)" and "Административный шелл: права и активность"). List every contract statement it carried, grouped by command (status, version show, replication subscriptions, data count/head/get, rights show and the rights change commands, activity commands, token sessions, exit codes, typed server codes for all commands) and, for each, whether the current chapter states it. Examples of contracts that must not be lost: a table that the caller cannot read is indistinguishable from a missing one (single refusal), the second authorization pass of `data get` and the residual DDL window between the last registration check and the read, the limits and quarantine rules of the `data get` response (names, sizes), the rights of `replication subscriptions` and the admin bypass, the field sets of the `status` and `version show` results (schema v1), the envelope and the `node` field of batch output, which command is refused for token sessions and why, the exit-code table with the typed codes.
2. For every statement that is not in the current chapter, VERIFY it against the code of the shell (`crates/server/shamir-server/shell/`) and the server handlers before restoring it: a statement that is no longer true is corrected or dropped (and listed in your report with the evidence), a true one is restored. Where the old text and the current walkthrough say the same thing in different words, keep the walkthrough.
3. Put the restored detail where an operator looks for it: keep the walkthrough short and add a "Справочник команд оболочки" part (per command group, tables for fields, rights and codes) after it, instead of weaving details back into the walkthrough; avoid duplicates between the two parts (the walkthrough links to the reference).
4. Fix any statement in the chapter that is false against the code today (for example anything that still calls `backup schedule show` or `export online` unsupported), and any statement that promises more than the code does.

## Hard constraints (tests read this file)

* Typed shell invocations only in examples (no raw wire JSON). A fenced command example must contain NO path separator (`/` or `\`) and no private path or host name.
* The export sections are scanned by an integration test (`assert_export_guide_examples` in `crates/server/shamir-server/tests/admin/shell/maintenance/export_online.rs`): read it and keep the structure it asserts (exactly one batch `export online start` example, no batch `status`/`cancel` example, no interactive command shown as a separate process, no `<PIN>` placeholder, no canary or fixture path); do not rewrite those two regions beyond what a false statement requires.
* Section 3 (`/metrics`) and section 10 (Prometheus and Grafana) belong to the metrics inventory gate (`scripts/ci/tests/gates/merge/`): do not touch them.
* Never write a private machine path into the file. Keep headings stable where other chapters link to them (grep the guide folder for links into the shell chapter before renaming anything).

## Policy and report

⛔ NEVER run `git reset` / `checkout` / `clean` / `stash` / `restore` / `rm`, or any git command that mutates the working tree or index. Only edit the file; the orchestrator stages and commits. Compilation, builds and tests run on GitHub CI, not on this machine; this task needs none (allowed locally: `python -I scripts/ci/check_layout.py`, read-only `git diff`/`git show`/`git log`, and read-only scans of files).

Final report: ONE complete standalone message: the list of contract statements found in the removed text with, per statement, the verdict (already stated / restored / corrected / dropped because false) and the code evidence (file and line), the final line ranges changed, the headings added or renamed, and every statement you could not verify against the code. Do not write "all done" if anything is open.

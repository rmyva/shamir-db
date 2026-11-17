# Brief: R1c step 2a — database identity, schema-migration journal, durable append/read, fencing generation (release-1 R1c, implementation)

Design: `docs/dev-artifacts/design/app-schema-migrations.md` — read it in full, the "Review" section included. This is the
first PR of step 2 of its implementation plan (section 8: "(2a) stable database identity, journal records, durable
append/read, fencing generation"). Sections 2 (journal ownership, record model, invariants), 4.2 (durable progress and
uncertain windows), 4.3 (durable concurrency and fencing), 6 (compatibility) and 7.1 (`shamir-db` tests) are binding for
this step. Step 1 (typed model and canonical body in `shamir-query-types` / `shamir-query-builder`) has landed; build on its
types, do not redefine them.

## Why

Release blocker R1c: applications need a forward-only, checksummed schema-migration journal. Step 2a gives the server the
durable substrate: a stable identity per database, the append-only journal inside the durable system catalogue, and the
fencing generation that later serializes appliers. Step 2b adds the three typed operations and the server wiring.

## What

1. **Stable database identity** (design 2.1–2.2): a per-database identity that survives restart, is created once, and is
   never reused after drop/re-create of a database with the same name. Say where it is stored and how existing databases
   (created before this change) acquire one; that path must be idempotent and crash-safe.
2. **Journal records** (design 2.2): the append-only record model of the design (ID, ordinal, checksum, canonical
   version, outcome/state, the fields the design names; nothing the design excludes), inside the durable system
   catalogue, keyed by the database identity. Encode with an explicit version so R1b format compatibility can evolve it.
3. **Durable append and read**: append is durable before it returns (reuse the existing system-store durable path the
   design cites; do not assume it makes separate-repo DML atomic — design 4.2 says so); read returns the ordered history
   with the invariants of 2.2 checked (no gaps, no duplicate ordinals, checksums stable). A violated invariant is an error
   naming the record, never a silent skip.
4. **Fencing generation** (design 4.3): a monotonic per-database generation with compare-and-set acquire/release that
   later steps use to serialize appliers and to detect a stale applier after crash; an in-doubt record left by a crash is
   reported as in-doubt on read (design 4.2), never auto-completed.
5. **Tests** (CI only) in the `shamir-db` test roots the design names (section 8 layout note): identity survives restart;
   identity differs after drop and re-create; append/read round trip; restart preserves history; invariant violations are
   detected; CAS fencing under concurrency (two appliers, exactly one wins); a crash between the in-doubt write and the
   completion leaves an in-doubt record that read reports; journal of one database is invisible to another.
   On Windows never read a file while another handle holds a byte-range lock on it.

## Rules

* Crates: `shamir-db` only (database facade / system-store layer). **No additions to `shamir-engine` `repo`, `table`,
  `tx` or `query` layers** (design section 8); if a primitive is missing there, stop and report the exact addition instead
  of writing it. No wire/DTO change beyond what step 1 already defined; no server wiring (that is 2b). No new dependency.
* Layout (design section 8, counted 2026-11-16 — count again from a listing): `src/shamir_db/system_store/`,
  `.../execute/`, `.../execute/core/` and both test roots are at the 7-entry limit; room exists in
  `src/shamir_db/system_store/catalogue/` (6), `.../execute/admin_ddl/` (5), `src/shamir_db/tests/ddl_contracts/` (6). Name
  the concrete home of every new file. If the only right home is a full folder, the split/move is its own preparatory
  change, listed separately in the report (the orchestrator commits it apart from the feature).
* At most 7 entries per folder and 1000 lines per file; one primary export per file; `mod.rs` re-exports only; no local
  `use`; no inline test modules; no new `#[allow]`; no visibility widening beyond the new module's own API.
* Code ideology: no `Mutex`/`RwLock` on hot paths; journal access is an admin path, but say which primitive guards the
  fencing CAS and why.
* Keep the change reviewable: if it passes roughly 2500 lines, stop after items 1–3 with their tests and say what remains.

## Policy (verbatim, binding)

Compilation, builds and tests run on GitHub CI, not on this machine. Banned locally: `cargo test`, `cargo nextest`,
`./scripts/test.sh`, `cargo build`, `cargo run`, `cargo bench`, `cargo doc`, npm/tsc builds, any build or test run.
Allowed locally only: `cargo check -p <crate> --all-targets`, `cargo clippy -p <crate> --all-targets -- -D warnings`,
`cargo fmt -p <crate>` (and `-- --check`), `python -I scripts/ci/check_layout.py`.

⛔ NEVER run `git reset` / `checkout` / `clean` / `stash` / `restore` / `rm`, or any git command that mutates the working
tree or index. Only edit files; the orchestrator commits.

## Acceptance (observed in CI)

fmt, `cargo clippy -p shamir-db --all-targets -- -D warnings` and layout clean locally; the `shamir-db` tests green in CI
on the three OS. The final report lists every file created and changed (preparatory moves apart), every design point
implemented, where identity and journal live and how pre-existing databases acquire an identity, and every deviation with
its reason.

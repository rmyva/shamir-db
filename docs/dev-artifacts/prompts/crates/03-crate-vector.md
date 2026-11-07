# CRATE-VECTOR: split `shamir-index` into `shamir-index-core`, `shamir-vector` and `shamir-index`

Goal (test-matrix reduction): the vector module is 27k lines and 193 s of the 214 s of `shamir-index` lib tests; a change
to `base_index`/`fts`/factory code must stop re-running and rebuilding it. Layering after the split (no cycles):

    shamir-index-core   model (descriptor, kind, meta envelope, persistence, expr, posting layout, actor,
                        lifecycle, reader drain gate, registry, state, write ops) + the `IndexBackend` trait
                        and its types (`backend.rs`) + `SearchOpts`
    shamir-vector       the vector backends (adapter trait, brute force, HNSW, SQ8, snapshots, `VectorBackend`);
                        depends on shamir-index-core
    shamir-index        base_index, fts, the backend factory and the non-vector backends; depends on both,
                        and RE-EXPORTS everything so every existing path keeps compiling

Behaviour does not change. Facts found by analysis (alias-aware): `model` references `backend` (2 uses);
`backend.rs` references `crate::vector::SearchOpts` (1 use, in `IndexQuery::Vector`) and the model; `vector`
production code references only `model` (kind, meta_envelope, descriptor, write_ops) and `backend` (the
`IndexBackend` trait, `promote_ticket`); the only production use of vector by the rest is the factory
(`backends/build_backend.rs`: `HnswAdapter`, `HnswConfig`, `VectorBackend::new`) and `SearchOpts`.

## Already done by the orchestrator (you cannot move files; do not recreate them)

`git mv` + mechanical rewrites:
* `crates/shamir-index-core/src/`: `model/` (whole tree), `backend.rs` (was `backends/backend.rs`), tests
  `tests/{model,runtime,identity}/` and a COPY of `tests/support/` (the original stays in shamir-index).
* `crates/shamir-vector/src/`: everything that was under `shamir-index/src/vector/` (`lib.rs` is the old
  `vector/mod.rs`; `hnsw_adapter/`, `quant/`, `simd/`, `snapshot/`, `vector_backend/`, `tests/`). In these files the
  orchestrator replaced `crate::vector::` by `crate::`, and `crate::{kind,meta_envelope,backend,descriptor,
  promote_ticket,write_ops,persistence,model}` plus `crate::backends::backend::` by `shamir_index_core::...`
  (so `shamir_index_core` must export those names at its ROOT exactly like the old `shamir-index` root did, see 1).
* Neither new crate has a `Cargo.toml` yet; `shamir-index` still declares `mod model;`, `mod vector;`,
  `backends/mod.rs` still declares `backend`, and its `tests/mod.rs` still lists the moved test dirs.

## Deliverables

1. `crates/shamir-index-core`: `Cargo.toml` (`publish = false`, workspace lints, `doctest = false`; dependencies only
   what the moved code uses, versions copied from `shamir-index`'s manifest; keep the `loom` optional dependency and
   the `loom` feature if the moved reader-drain-gate uses them, and make `shamir-index` forward the feature),
   `src/lib.rs` declaring `pub mod model; pub mod backend;` and re-exporting at the ROOT every name the old
   `shamir-index` lib.rs exported from model/backend (copy those `pub use` lines from the old lib.rs: `descriptor`,
   `kind`, `meta_envelope`, `persistence`, `expr`, `posting_layout`, `actor`, `lifecycle`, `reader_drain_gate`,
   `registry`, `state`, `write_ops`, `backend`, `promote_ticket`, `FtsMode`, `IndexBackend`, `IndexError`,
   `IndexQuery`, `IndexResult`, `IndexDescriptor`, `IndexKind`, `VectorConfig`, ... and the rest of that list), tests
   manifests (`src/tests/mod.rs` + the moved dirs' `mod.rs` already exist) with `#[cfg(test)] mod tests;`.
   `SearchOpts`: move the struct (it is defined in `crates/shamir-vector/src/quant/adapter.rs`) into a new file
   `crates/shamir-index-core/src/search_opts.rs`, re-export it at the root and from `backend.rs`'s users, and have
   `shamir-vector` re-export it (`pub use shamir_index_core::SearchOpts` where `quant::adapter` exported it) so that
   `shamir_vector::SearchOpts` and `shamir_vector::adapter::SearchOpts` still exist. `IndexQuery::Vector` then uses
   `crate::search_opts::SearchOpts`.
2. `crates/shamir-vector`: `Cargo.toml` (deps: `shamir-index-core`, the storage/types/numa/tunables/funclib crates
   the code uses, `hnsw_rs = "=0.3.5"` with the comment about the workspace `[patch.crates-io]`, `crc32fast`,
   `tempfile`, `bytes`, `smallvec`, `rmp-serde`/`bincode`/`serde`, `scc`, `arc-swap`, `async-trait`, `futures`,
   `tokio`, `thiserror`, `log`, and whatever else `grep`/compile shows; dev-dependencies copied from
   `shamir-index` where the vector tests use them: `serial_test`, `bench-scale-tool`, `shamir-bench-utils`,
   `shamir-storage` (full features), `shamir-test-diag` ...). Check `shamir-index/Cargo.toml` for `[[bench]]` /
   `[features]` sections that belong to vector and move them. `src/lib.rs` is already the old module root: fix its
   doc comment to describe the crate and keep it re-exports-only.
3. `crates/shamir-index`: delete nothing (cannot), but fix: `lib.rs` loses `pub mod vector;` and the model module
   dir, gains `pub use shamir_index_core::model;` plus re-exports of the core names at the root (so `crate::kind`,
   `crate::backend`, ... keep resolving for `base_index`/`fts`/`backends`), and `pub use shamir_vector as vector;`
   so the path `shamir_index::vector::...` keeps working for dependents (grep the workspace: `shamir_index::vector`).
   `backends/mod.rs`: `backend` becomes `pub use shamir_index_core::backend;`. Manifest: dependencies on
   `shamir-index-core` and `shamir-vector`; drop the dependencies that only vector used if nothing else in
   shamir-index needs them (`hnsw_rs`, `crc32fast`, `tempfile`, ... verify by grep before removing); the old
   `tests/mod.rs` must stop listing `model`, `runtime`, `identity`; the `support` helpers the remaining index tests use stay.
4. Two vector tests use the factory that lives in `shamir-index`: `crates/shamir-vector/src/tests/backend/
   rv_index_card_tests.rs` (`build_index2_backend`) and `.../vector_restore_tests.rs` (`crate::build_index2_backend`,
   3 call sites). A vector crate test must not depend on `shamir-index`. Rewrite them to construct the backend
   directly (`VectorBackend::new(...)` exactly the way `shamir-index/src/backends/build_backend.rs` does for the
   vector kind: read that function and copy the construction, no new abstractions). If a call site cannot be
   expressed without the factory, say which one in the final message and keep the rest compiling.
   `crates/shamir-vector/src/snapshot/tests/streaming_acceptance/memory.rs` uses
   `crate::tests::support::allocation_counter`: give the vector crate its own `src/tests/support/` (copy
   `allocation_counter.rs` and `mod.rs` content it needs; check `shamir-index/src/tests/support/` and the core copy) and
   register it in `crates/shamir-vector/src/tests/mod.rs`.
5. Every other workspace crate that names moved paths keeps compiling through the re-exports; verify with grep for
   `shamir_index::` across the workspace (engine, db, server, benches) and fix only what breaks. Update
   `CLAUDE.md` (crate count and list: `shamir-index-core`, `shamir-vector`, one sentence each), `.config/nextest.toml`
   (overrides naming vector tests or `package(shamir-index)` slow groups: retarget to `shamir-vector`),
   `scripts/test.sh` scopes, `.github/workflows/*`, `.rust-guard/*.toml` (owned_impl/exceptions that name moved paths),
   `scripts/ci/layout_baseline.txt` (lower or remove entries for moved paths, never add), `docs` references to the
   old paths (grep `shamir-index/src/vector`).

## Success criteria

`shamir-index-core`'s dependency list has no `shamir-vector`/`shamir-index`; `shamir-vector`'s has no `shamir-index`;
`cargo check --all-targets`, `clippy -D warnings`, `fmt` pass for `shamir-index-core`, `shamir-vector`, `shamir-index`
and every dependent crate that compiles through the re-exports (`shamir-engine-query`, `shamir-engine`, `shamir-db`,
`shamir-server`: run the heavy ones once at the end), no folder over 7 entries, no file over 1000 lines. Final message:
what is done and not done per deliverable, honestly, the test call sites you could not keep, and the files touched.

## Binding policy (verbatim from CLAUDE.md)

Compilation, builds and tests run on CI, not on this machine. Banned: `cargo test`, `cargo nextest`,
`./scripts/test.sh`, `cargo build`, `cargo run`, `cargo bench`, `cargo doc`, `npm`/`tsc`/`vitest`,
WASM builds, e2e harnesses, any command whose purpose is to build the project or run tests/benches.
Allowed locally, scoped to the touched crates only: `cargo check -p <crate> --all-targets`,
`cargo clippy -p <crate> --all-targets -- -D warnings`, `cargo fmt -p <crate>` and `-- --check`,
`python -I scripts/ci/check_layout.py` (reads tracked files only; new files are invisible to it until
staged: say so, do not work around it). Use `CARGO_TARGET_DIR=D:/system_artefact/target-vector` (outside the
repository) and never pipe check/clippy through head/tail. Crates you will touch: shamir-index-core (new), shamir-vector (new), shamir-index, shamir-engine-query, shamir-engine, shamir-db, shamir-server (dependents), plus every
dependent whose public signature or path you changed (grep the workspace). `Cargo.lock` may change only by what
this task needs: run `cargo check` once WITHOUT --locked so cargo writes it, and say so. If you believe you need a
build or test run, say so in the final message. No other shell commands.

⛔ NEVER run `git reset` / `checkout` / `clean` / `stash` / `restore` / `rm`, or any git command that
mutates the working tree or index. Only edit files; the orchestrator commits (and has already done every file
move: you cannot move or delete files, so work with the layout you find).

## Repository rules that bind this work

At most 7 direct entries per folder, at most 1000 lines per source file; `mod.rs` re-exports only; one file = one
primary export; tests live in `tests/<topic>/`, never inline `#[cfg(test)] mod`; no local `use`; no visibility
widening and no new `#[allow]`; no `impl` blocks outside the owning module (rust-guard); JSON literals in tests
multi-line; comments terse; no machine-local paths; do not mention how or where CI is run. Count folder entries by
hand for every folder you touch (the layout script cannot see untracked files) and nest instead of passing 7.
Children, if you use any, only edit files, never run commands, and get the NO-BUILD policy above quoted verbatim.

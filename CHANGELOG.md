# Changelog

All notable changes to ShamirDB will be documented here.

The project is currently in alpha and has not published a stable release. Until the first tagged release, the `master` branch is the primary development line and compatibility is not guaranteed.

## Versioning scheme

During the alpha phase this project uses `MAJOR.MINOR.PATCH-alpha.N` (for example `0.1.0-alpha.1`). The `alpha.N` suffix is a SemVer pre-release identifier — incrementing `N` does **not** imply any compatibility guarantee between two alpha releases.

> **Workspace-version scope (Node client — documented gap, F-52):** the release pipeline's `version-consistency` job (`.github/workflows/release.yml`) verifies that every *Cargo* workspace crate's `version = "..."` agrees with the pushed `v*` tag. It deliberately does **not** cover `crates/shamir-client-node/package.json`'s `"version"` field: that crate is excluded from the Cargo workspace (`Cargo.toml` `exclude` — napi-rs cdylib, MSVC-only, built separately per `CLAUDE.md`), uses a different manifest format (Node `package.json`, never matched by the `crates/*/Cargo.toml` grep), and is versioned on its own cadence. Its `"version": "0.1.0"` therefore legitimately differs from the workspace's `0.1.0-alpha.1` — this is a documented scope gap, **not** a bug to "fix" by bumping it. There is currently no separate npm-publish workflow for it either (only `ts-e2e-nightly.yml`'s `node-e2e` job builds/tests the `.node` binding locally; no publish step exists yet); adding one is a separate, larger task.

> ⚠️ **Alpha-to-alpha compatibility: NOT guaranteed.** The on-disk storage format, the wire protocol, and the public API (Rust, TypeScript, and WASM) MAY change incompatibly between any two `0.1.0-alpha.N` releases. There is **no supported in-place upgrade path** between alpha versions yet — upgrading between alphas may require an **export/import cycle** (dump data with the old version, re-import with the new one). Treat each alpha as throwaway and keep independently verifiable backups. This is the single most important operational fact for an early adopter.

This file follows [Keep a Changelog](https://keepachangelog.com) conventions loosely and will adhere to [Semantic Versioning 2.0.0](https://semver.org) pre-release semantics once stable releases begin.

## [Unreleased]

### P1 query-types remediation
- `InsertedRecord::get_value_owned("_id")` now also finds a decoded `_id`
  (previously `None` for every deserialized record).
- `BatchOp` decode rejects only ambiguous discriminator sets (`update` +
  `set` stays legal — `set` is a payload field of `UpdateOp`); non-
  discriminator payload keys are validated by each typed op, not by the
  `BatchOp` layer. The decode makes a single buffering pass.
- `BatchLimits` accepts partial `limits` maps: every field now carries a
  `#[serde(default)]` with the same value as `Default`.
- `DbRequest`'s `Debug` no longer prints `ChangePasswordVerify` SCRAM
  credential material (redacted; wire bytes unchanged).

### RU6
- `QueryResult::op_id`, `DdlOpStatus.op_id`, `DropIndexOp.request_id` and
  `RenameIndexOp.request_id` now serialize on the wire as the canonical
  base58 string (`RecordId::to_string()`), matching the string the
  `GetDdlOpStatus` poll request takes; decode is strict-string only (old
  raw-`bin` values are rejected). An old Rust client cannot decode a new
  server's string `op_id` — acceptable pre-v1. The DDL op log's bincode disk
  encoding is unchanged.

### RU7
- Decode raw MessagePack `u64` values above `i64::MAX` as exact `Big` values across shamir-types codecs and RecordView lenses; the existing v1 decimal-text encoding projection is unchanged.

First public alpha in preparation (`0.1.0-alpha.1`, not yet tagged). The
detailed engineering log previously kept here lives in
`docs/dev-artifacts/checkpoints/2026-08-engineering-log.md`.

### Changed
- Replaced `chrono` with `jiff`/`std` in `shamir-types` and `shamir-funclib`; `datetime/` scalar contracts are preserved by explicit compatibility code and differential-tested against the old implementation (known jiff strptime divergences remain tracked; the RUSTSEC-2026-0543 deny exception is removed in a follow-up).
- Documented the v1 wire/storage projection of `Dec`/`Big`/`Set` (RV-TYPES-F28FAFBF; documented projection, NOT a protocol change — the bytes are unchanged): on MessagePack encode `Dec`/`Big` are written as their decimal string and `Set` as a plain array in insertion order; on decode they come back as `Str`/`List`. The `codecs` README now states this instead of claiming all-type MessagePack round trips. Supporting it, `scalar_ref_numeric_eq` gained the missing typed-RHS arms: a stored decimal string (a projected `Dec`/`Big`) is now equal to an equal typed `Dec`/`Big` filter literal, compared via the canonical `NumericKey` (THE ONE RULE); an unparseable stored string simply does not match.
- WebSocket endpoints `/shamir/v1` and `/shamir/v1/browser` now require the `shamir-v1` WebSocket subprotocol; requests without it (or offering only incompatible protocols) are rejected with HTTP 400 (RV-TRANSPORT-WS-C0C43A18; alpha policy: no compatibility shim). First-party clients now offer it.

### Changed
- DISTINCT and GROUP BY now bucket values by the exact shared canonical numeric key: `Int(1)`, `F64(1.0)`, `-0.0`, an equal `Big`/`Dec` form one class; Int↔Big/Dec equality is exact (never through `f64`); each NaN occurrence is its own DISTINCT value / GROUP BY group (NaN != NaN) and never matches a foreign key; the lossy `F64`↔`Dec`/`Big` comparison now applies to ORDERING only (RV-FUNCLIB-5FA455F7). Note: records STORED on disk serialise `Big`/`Dec` as their decimal string, so GROUP BY over stored bytes groups them with equal-looking strings (the documented v1 wire/storage projection, RV-TYPES-F28FAFBF); the exact numeric classes apply to the type-preserving in-memory path.
- Equality simplification (RV-FUNCLIB-5FA455F7, round 3 — ONE rule: equal iff equal canonical keys and neither side NaN): an integral finite `f64` of ANY magnitude (e.g. `F64(2^63)`) now equals the same integer as `Int`/`Big`/`Dec`; a NON-integral `F64` and a non-integral `Dec` are separate equality classes (`F64(0.5) != Dec(0.5)` — lossy F64↔Dec stays ordering-only); NaN never equals anything (itself included) at ANY equality decision path (filter Eq/Ne, dynamic IN, `$contains`/`ContainsAny`/`ContainsAll`, DISTINCT per-occurrence NaN at any depth, FK matching on both the batch and INSERT-side validator funnels).

### Changed (index rebuild required)
- Functional index posting hashes: computed `Dec`/`Big`/`Bin`/`Set` values now hash by content (previously collapsed to one shared tag-255 hash, RV-INDEX-56A505F0). Existing functional indexes hold postings under the old hashes and MUST be dropped and re-created after upgrading.
- FTS tokenizers (Whitespace, Full): non-ASCII uppercase words (e.g. Russian/Greek) are now case-folded before hashing (RV-INDEX-A570106D). Existing FTS indexes contain pre-fix token hashes and MUST be dropped and re-created after upgrading.
- Legacy regular/unique/functional index posting hashes for numeric (Int/F64) keys are canonical now — `Int(1)` and `F64(1.0)` share one key, `-0.0` keys as `0`, NaN is never matched by an equality probe nor a unique conflict (RV-FUNCLIB-5FA455F7). Existing indexes holding numeric keys MUST be dropped and re-created after upgrading (alpha: no in-place upgrade path).
- Legacy regular/unique/functional index posting hashes for `Big`/`Dec` keys are canonical now (index format version 3): an integral `Big`/`Dec` keys like the equal `Int` (RV-FUNCLIB-5FA455F7). Existing indexes holding `Big`/`Dec` keys MUST be dropped and re-created after upgrading (alpha: no in-place upgrade path; the engine's rebuild-on-open handles the version stamp).

### Included
- Single-node server (`shamir-server`): TCP, WebSocket and local IPC listeners; TLS 1.3 + SCRAM-Argon2id; session resumption; rate limiting and lockout.
- Document storage on Fjall with WAL-based crash recovery; MVCC transactions (snapshot, serializable, pessimistic), single-batch and interactive.
- Batch query API built with the Rust and TypeScript query builders: filters, projections, aggregates, ordering, pagination, cross-query references, conditional execution, loops, cursors.
- Indexes: hash, unique, sorted/covering, FTS (BM25, Snowball stemming), functional, vector (HNSW, optional SQ8).
- WASM functions, procedures and validators (upload pre-built `.wasm`); Shomer access control; changefeed; live subscriptions.
- Offline `backup` / `restore` CLI with a sha256 manifest; offline `doctor`.

### Experimental
- Leader→follower async read replicas (schema not replicated, no automatic reseed, no failover).
- Storage-engine migration API — disabled by default, known data-loss gaps.
- Server-side Rust→WASM compilation — development only.

### Known limitations
See `docs/guide-docs/KNOWN_LIMITATIONS.md`. No online backup, no point-in-time recovery, no in-place upgrade between alpha versions, no published binaries or packages.

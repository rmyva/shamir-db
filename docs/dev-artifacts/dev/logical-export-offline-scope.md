# R3 step 2: scoped offline logical export

This is an implemented **subset**, not full logical-dump acceptance. No online
job, importer, benchmark result or behavioral test pass is claimed.

## CLI and actual driver

`shamir-server --config <config.ktav> logical-export --db app --output <file>`
accepts `--repo`, `--table` (requires repo) and `--schema-only`. It uses
`shamir_sync::replication_seed::MaintenanceOpen` (exclusive directory lock,
Inspect stamp gate, init_seed/recovery), never starts a server. Ctrl-C signals a
CancellationToken and joins the operation rather than detaching recovery.

The driver captures the finalized engine logical-export facade, checks real
catalogue/engine inventory agreement, projects typed PortableItems and emits all
12 fixed sections using the existing shamir-sync DumpWriter. Rows retain raw
keys, logical field names and map iteration order. A bounded binary-carry
external merge sort orders raw keys (4 MiB input runs, two merge heads, at most
40 run levels, 1 TiB cumulative source ceiling). Cancellation is checked while
paging, sorting and at codec I/O/frame boundaries; progress goes to stderr.

Output is synced and **closed** before step-1 verification opens it. The
verifier's checksum/count/digest/EOF checks run before publication; its reads
also check cancellation and deadline. A same-volume hard link is the atomic
no-replace commit point; unsupported filesystems fail without copy fallback.
This deliberately replaces the design's rename with a no-replace primitive
available without another dependency or unsafe platform calls.

Before that commit, errors/cancellation drop attempt-owned stage files. After
hard-link success, a Unix parent-directory-sync error explicitly reports
"published a verified artifact ... final file retained": this is a durability
error with a complete final file, NOT an unpublished attempt. Do not blindly
retry or delete that file. Windows uses file sync plus verification readback,
without a directory-sync guarantee; power-loss durability remains limited.

## Exact supported scope

* Persisted Fjall/fjall_raw repositories, no durability override; the portable
  engine is Fjall (raw-vs-buffered backing is physical, not portable).
* Plain tables without stored schemas, validators, transforms, foreign keys or
  any index family. Table enable_indexes and buffer TTL are preserved. Other
  buffer knobs must match the current defaults, otherwise export refuses.
* Record values: Null, Bool, Int, F64 bits, Bin and recursively ordered maps.
* Selected resource owner identities resolve by principal64 to authoritative
  directory usernames. Only name/kind/disabled-on-import is emitted, never
  credentials, roles, superuser/replicator state, ticket epochs or user IDs.
  System ownership is null, requiring importer policy rather than an implicit
  privilege grant. Unresolved owners refuse. Group metadata refuses.
* Root and function-namespace ACL settings carry logical owner/group names and
  modes, plus explicit ACL items. Their ResourceRef.object is `root_meta` or
  `fn_namespace_meta`; step 1 imposes no separate object namespace validator.
  A real driver/step-1 verifier regression fixture includes both settings.
* Database creation times, database identities (node-local), physical paths/store IDs, replication configuration,
  retired users/roles, tokens, counters and native caches are excluded by design.

## Deliberate fail-closed limitations / follow-up families

* **All string-shaped record values refuse**, including ordinary Str: persisted
  Dec/Big are flattened to strings and cannot be distinguished. **All array-shaped
  record values refuse**, including ordinary List: persisted Set is flattened to
  List. No storage codec changes or full-kind fidelity claim.
* All portable schema rules/schema versions/schema validator IDs, validators,
  functions (all languages/artifact kinds), function folders, guest WASM and
  validator/transform/FK bindings remain unimplemented. Any function, validator,
  function-folder or group row anywhere in the global catalogue conservatively
  refuses even if another database/scope might not reference it: closure analysis
  is follow-up. Nonempty selected schemas/bindings never become empty success.
* All regular/unique/hash, sorted, full-text, functional and vector index
  descriptors refuse. No index data/definition is silently dropped.
* Group identities, membership, group ACLs and richer grants remain follow-up.
* In-memory/hybrid repositories, nondefault durability overrides and nondefault
  buffer sizing/flusher options refuse. Unknown selected catalogue fields refuse.
* `--include-artifacts` always returns an explicit unsupported error. It is never
  accepted and ignored. `--schema-only` legitimately omits data, but still checks
  every selected definition and all the conservative global restrictions above.
* Additional scoped limits: 8 MiB selected definition encoding, 4096 ACLs and
  identities, 1024 bytes per logical name, 2048 record nodes / 512 KiB expanded
  key/binary bytes, 1 MiB encoded sort entries, depth 64. Source facade limits and
  container limits also apply. Large-data acceptance budgets remain unmeasured.

## Surfaces, layers and visibility

No finalized engine logical_export file is changed by this offline chunk.
One narrow new public method in existing SystemStore storage/catalogue code,
`capture_logical_export(db, guard)`, wraps the engine facade without returning a
writable system repository. No existing visibility is widened; rust-guard's
check_vis skips this new key in an existing file. CI runs the structure and
conservation gates only for campaigns registered in `.rust-guard/stages.toml`;
this change registers none, so no delta file is needed. New server driver items
use pub(super) helpers. No engine layer ordering changes; no repo/table/tx layer
changes or preparatory moves.

Folder counts after this chunk: server src 7; boot 7; boot/config 7;
boot/config/diagnostics 6; logical_dump 7; logical_dump/tests 4;
shamir-db/src/shamir_db/system_store 7. All Rust files remain below 1000 lines.
Run the layout gate for current counts rather than assuming historical design
counts.

## CI-only regression coverage

Added real disk fixture -> offline driver -> step-1 verifier tests, including
root/namespace ACL settings, live/deleted rows, output conflict, corruption,
locked-directory refusal, ambiguous-value failure cleanup, schema-only retry,
pre-cancellation, explicit artifact-flag refusal, multi-run raw-key sorting,
duplicate-key refusal, mid-sort cancellation cleanup, racing final publication,
secret-free username projection/unresolved-owner refusal, and verifier I/O
cancellation. These are source coverage, not locally executed test results.

Resolved versions read from Cargo.lock: tokio 1.51.6, clap 4.5.60,
serde 1.0.229, tempfile 3.27.0, fjall 3.1.13, sha2 0.11.1 (also 0.10.9 transitively).
No dependencies added. Local checks are restricted to the user's allowed list;
behavioral execution belongs to GitHub CI. Engine HashSet/RandomState policy is
not changed here; the finalized engine file is outside this chunk's authority.

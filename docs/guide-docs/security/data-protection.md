# Data Protection: At-Rest Encryption & PII Retention / Erasure

**Status:** documentation of the *current* behaviour, not a roadmap.
**Audit origin:** finding C6
(`docs/dev-artifacts/audits/2026-07-06-security-compliance-supplychain.md`, LOW-MED) —
"No documented policy for at-rest encryption or PII retention/erasure".
**Cross-references:** [`../../../SECURITY.md`](../../../SECURITY.md) (vulnerability
disclosure / supply-chain), [`../../dev-artifacts/audits/shamir-storage.md`](../../dev-artifacts/audits/shamir-storage.md)
(storage-layer audit), [`../../dev-artifacts/perf/durability-model.md`](../../dev-artifacts/perf/durability-model.md)
(WAL/durability model), [`../../dev-artifacts/roadmap/TEMPORAL.md`](../../dev-artifacts/roadmap/TEMPORAL.md) §3
(retention semantics).

This document exists to make the at-rest data-protection posture explicit.
For a database that may hold PII, compliance regimes (GDPR Art. 32, SOC 2
CC6.1) require **either** at-rest encryption **or** an explicit, documented
decision that encryption is delegated to another layer. Silence on this point
is itself the compliance gap, regardless of whether encryption exists. This
document closes that silence for S.H.A.M.I.R. DB.

---

## 1. At-rest encryption model

### 1.1 Current state — plaintext on disk (delegated model)

**Records are stored on disk as plaintext MessagePack.** The database performs
**no** at-rest encryption of record contents, keys, history versions, WAL
entries, the field-name interner, indexes, or any other on-disk structure.
The on-disk bytes are LZ4-compressed (by `fjall`/`lsm-tree`) but not encrypted;
anyone with raw read access to the data directory can decode every record.

#### How this was verified

| Claim | Verification |
|---|---|
| `shamir-storage` performs no encryption | `rg -i 'encrypt\|decrypt\|cipher\|aes\|chacha\|tde\|data_encryption_key' crates/shamir-storage/src` → **zero matches** in the storage write/read paths. The only "at rest" mentions are about *password* hashing, not record data. |
| `shamir-storage`'s `fjall` backend writes values verbatim | `crates/shamir-storage/src/storage_fjall.rs:121-123` — `keyspace.insert(&key[..], &*value)`; the `Bytes` returned by `Store::get` (`:159-166`) is a direct `copy_from_slice` of what fjall returns. No transform layer. |
| `fjall` itself has no encryption feature | `rg -i 'encrypt\|cipher\|aes\|chacha\|tls\|crypt'` against the vendored `fjall-3.0.1` and `lsm-tree-3.0.1` source → **zero matches**. `fjall/src/lib.rs:12` advertises "Built-in compression (default = `LZ4`)" — compression, not encryption. `fjall/src/journal/entry.rs:77-79` does `writer.write_all(key)` then `writer.write_all(&compressed_value)` — no cipher in the write path. |
| No AES/ChaCha dep is wired into the storage path | `crates/shamir-storage/Cargo.toml` has no `aes-gcm`/`chacha20poly1305`/`ring`/`rustls` dependency. The `aes-gcm` crate **is** in the workspace, but only as a transitive of `shamir-connect` (used for the in-memory resumption **ticket** and the SCRAM handshake) — not of `shamir-storage`. |

The only cryptography that touches *storage-adjacent* state is unrelated to
record confidentiality:

* **Password hashing** — `crates/shamir-db/src/shamir_db/execute/helpers.rs:124-143`
  (`hash_password`) hashes admin/RBAC passwords with Argon2id before they reach
  the `users` table. This is credential storage hygiene, not record
  encryption; it protects exactly one column of one system table.
* **TLS in transit** — `shamir-connect` protects the wire (rustls 0.23,
  AES-256-GCM resumption tickets). Transport encryption is orthogonal to
  at-rest encryption; TLS does not protect data on disk.

### 1.2 Why this is an acceptable answer (not an open finding)

Many embedded, single-binary database products — SQLite (default build),
RocksDB, LevelDB — ship **without** built-in at-rest encryption and document
the operator's responsibility to encrypt the volume instead. S.H.A.M.I.R. DB
follows that same model **deliberately**:

* A database-level encryption layer would have to solve key management
  (KEK wrapping, rotation, envelope keys, per-table vs per-repo granularity,
  crash-safe key files) — a large surface that is already solved better by
  mature OS-level layers that the operator already runs.
* The product is a single binary deployed onto a host the operator controls.
  That host already has a volume-encryption story (or an explicit decision not
  to). Duplicating it inside the database gives no defense-in-depth gain
  proportional to the added complexity and key-management risk.

**This is a DELEGATED model, not an absence of an answer.** The compliance
posture is: *the operator MUST place the data directory on encrypted storage;
the database does not perform the encryption itself.*

### 1.3 Operator mitigation (required for any PII-bearing deployment)

The data directory configured at server start (`data_dir` in the `.ktav` config) **MUST** reside on encrypted storage in
any deployment that stores PII or any other sensitive data. Pick the layer
that matches the deployment target:

| Deployment | Recommended at-rest encryption |
|---|---|
| **Linux on bare metal / VM** | **LUKS** full-disk or partition encryption for the volume holding `data_dir` (and any volume holding backups / copied snapshots). |
| **Windows** | **BitLocker** on the drive holding the data directory. |
| **AWS EC2 / GCP / Azure VM** | Provider **EBS/disk encryption** (AWS EBS encryption, GCP CMEK, Azure Disk Encryption). For self-managed VMs, also acceptable: LUKS on top of an encrypted EBS volume (defense-in-depth). |
| **Kubernetes** | A **StorageClass** backed by an encrypted CSI driver (e.g. AWS EBS CSI with encrypted volumes, GCE PD CSI with CMEK, or a CSI that wraps LUKS). Confirm `data_dir`'s PVC uses it. |
| **Backups / snapshots / replicas** | **Same requirement.** An unencrypted `tar`/`rsync` of the data directory, an EBS snapshot taken without CMK, or a replica whose data dir is on a plaintext volume all void the protection. The encryption obligation follows the data, not the primary. |

The compliance posture is satisfied **only when every place the bytes land**
(primary, replica, backup, snapshot, copied export) is on encrypted storage.

### 1.4 Secret-bearing files on disk (cross-ref, not duplicated)

The TLS server certificate and private key (`server-cert.pem`, `key.pem` —
paths configured in `deploy/server.example.ktav` as `cert_path` /
`key_path`) are also plaintext files on disk. Their handling is covered in
[`../../../SECURITY.md`](../../../SECURITY.md) ("Supply-chain posture" —
`server-cert.pem` and other local secrets are `.gitignore`d) and in the
2026-07-06 audit's "Repository hygiene" section
(`docs/dev-artifacts/audits/2026-07-06-security-compliance-supplychain.md` §4 —
`server-cert.pem` is **not** git-tracked, no hardcoded secrets are tracked).
The same operator-mitigation rule applies: the directory containing
`cert.pem` / `key.pem` should also live on encrypted storage (LUKS/BitLocker/
cloud disk encryption), and the key file's filesystem permissions should be
`0600` owner-only. This document does not change any of those rules — they
already apply uniformly to all secret-bearing files on the host.

### 1.5 Filesystem trust boundary for data and WAL

WAL CRC checks detect accidental corruption; they **do not authenticate
writers**. A local writer with access to the WAL can inject transactions that
recovery replays. TLS protects traffic in transit, and volume encryption
protects storage at rest; neither provides WAL authenticity against local
writers.

The data directory, any sibling WAL directory, WAL segments and sidecars, and
restored copies **MUST** be writable only by the database identity and
explicitly trusted operators or tooling. Protect their parent directories too:
otherwise an untrusted user may rename or replace a protected directory or file.
Apply the same boundary when copying or restoring backups, before the database
uses them.

* **Unix:** Use `0700` for directories and `0600` for files, with `umask 0077`
  for newly created objects. Secure parent directories against unauthorized
  rename or replacement. Where trusted operators or tooling need access,
  grant only the necessary permissions through appropriately restricted ACLs
  or other controlled arrangements; do not grant broad write access.
* **Windows:** Configure ACLs so only the dedicated database identity and
  explicitly trusted operators or tooling can write. Restrict inherited broad
  write, delete, and delete-child permissions on the directories, files, and
  their parents so untrusted identities cannot modify, rename, or replace them.
  Apply equivalent ACLs to restored copies and their parent directories.

---

## 2. PII retention / erasure procedure

The database exposes two wire-protocol `BatchOp`s that govern history
retention and one-shot purge, plus the standard `Delete` for the current
version of a record. They are **not** a turnkey "right to erasure" API; they
are the primitives an operator composes to satisfy such a request. This
section documents what each does, how to invoke them, and the **honest**
residuals that remain even after a correct erasure.

### 2.1 The retention / purge surface (verified API)

#### `SetRetention` — change a table's history-retention policy

Wire shape and semantics live in
`crates/shamir-query-types/src/admin/types/retention.rs:110-116`:

```text
{ "set_retention": "users", "repo": "main", "retention": { "max_count": 5 } }
```

* **Scope:** per-table, per-repo. Applies to one `(db, repo, table)` triple.
* **Auth:** `Action::Manage` on `ResourcePath::table(db, repo, table)`
  (`crates/shamir-db/src/shamir_db/execute/admin_retention.rs:35-46`). I.e.
  any client (human or service) holding `Manage` on the table can issue it —
  this is a **wire-protocol** operation, not a server-operator-only action.
* **Effect:** swaps the table's `Retention` via a lock-free `ArcSwap`
  (`crates/shamir-tx/src/mvcc_store/mod.rs:411-...`, `apply_table_retention`
  in `crates/shamir-db/src/shamir_db/execute/helpers.rs:152-182`). **No data
  migration, no immediate reclaim.** The new policy governs subsequent
  `vacuum_key` calls (which run inline on each write/update/delete). Old
  versions that are now over-retention are reclaimed lazily on the next
  write to the same key, or by an explicit `PurgeHistory`.
* **The three knobs** (`retention.rs:18-28`): `max_age_secs` (age cap),
  `max_count` (version-count cap), `min_count` (always-keep floor). Caps
  intersect (tighter prunes); the floor overrides `max_age`.

#### `PurgeHistory` — imperative one-shot history purge by *time*

Wire shape: `crates/shamir-query-types/src/admin/types/retention.rs:72-78`:

```text
{ "purge_history": "users", "repo": "main",
  "scope": { "older_than_age": { "age_secs": 86400 } } }
```

* **Scope:** per-table, per-repo. The purge predicate is **time-based only**
  (`PurgeScope::OlderThan { timestamp }` or `OlderThanAge { age_secs }`,
  `retention.rs:60-65`). **There is no by-key, by-record-id, or by-subject
  purge primitive.**
* **Auth:** `Action::Manage` on the table
  (`admin_retention.rs:92-103`) — wire-protocol, not operator-only.
* **Effect** (`admin_retention.rs:107-146` → `MvccStore::purge_below_ts` at
  `crates/shamir-tx/src/mvcc_store/mvcc_gc.rs:375-450`):
  1. **Drain first.** `repo.drainer().drain_all(&repo)` is forced
     (`admin_retention.rs:114-126`) so that any committed-but-not-yet-drained
     versions sitting in the in-memory overlay are flushed to `history` before
     the purge scans it. This prevents a purge from missing the freshest
     tail.
  2. **Reclaim eligible history versions.** A version is reclaimed iff
     (a) its commit timestamp is *known* and `< cutoff`, AND (b) it is not
     the current version (`C1 SACRED`), AND (c) it is not pinned by a live
     snapshot (`< min_alive`), AND (d) it is not the single anchor version
     kept so the oldest live snapshot can still resolve a stale read
     (`mvcc_gc.rs:417-449`).
  3. **Lockstep ts-key removal.** Each reclaimed version's `ts_key(version)`
     is removed alongside, so timestamps never outlive their versions.
  4. **Overlay prune.** If the version is also `≤ durable_watermark`, its
     in-memory overlay copy is dropped (`mvcc_gc.rs:441-447`).

#### `Delete` — remove the *current* version of a record

`PurgeHistory` **never** reclaims the current version of a key
(`mvcc_gc.rs:419-421` — `C1 SACRED: never reclaim the current version`). To
remove a data subject's *current* record you must `Delete` it. A `Delete`
(`MvccStore::delete_versioned`, `crates/shamir-tx/src/mvcc_store/mod.rs:912-…`)
does **not** erase the prior value; it appends a new version carrying a
**tombstone** (empty value) to the same history log. The prior value remains
in the log until `vacuum_key` (inline, on the next write to that key, under
the prevailing retention policy) or an explicit `PurgeHistory` reclaims it.

#### `DropTable` / `store_delete` — remove an entire table

`DropTable` (wire op) → `ShamirDb::drop_table` → eventually
`Repo::store_delete(name)` (`crates/shamir-storage/src/types.rs:348-…`,
`storage_fjall.rs:50-65`) → `fjall::Database::delete_keyspace`. This removes
the keyspace (LSM tree) from the catalogue; the underlying SST files are
deleted by fjall. This is the strongest per-table erasure primitive — but it
destroys the entire table, not a single subject, and (per §2.4 below) the
field-name interner still survives.

### 2.2 How to satisfy a "right to erasure" (GDPR Art. 17) request

There is **no single API call** that achieves erasure of one data subject.
Compose the primitives:

1. **Locate the subject's records** — by primary key, by indexed PII field,
   or by a query that selects them. The database does not maintain a
   subject→records index; the operator must resolve this from application
   context.
2. **Delete each record's current value** — issue a `Delete` for each
   primary key in the result set. This writes tombstones.
3. **Force history reclaim** — issue `PurgeHistory` with
   `scope: { older_than: { timestamp: <now> } }` (or `older_than_age:
   { age_secs: 0 }`) to reclaim every non-current, non-pinned history
   version older than "now" for that table. Combined with step 2's
   tombstones, this reclaims the **values** of all prior versions.
4. **Wait for WAL truncation + LSM compaction** (§2.3) before claiming the
   data is physically gone from disk. If the table carries a **vector (HNSW)
   index**, also account for index-resident embeddings that survive until HNSW
   compaction — a separate, slower reclamation path (§2.5). FTS postings ride
   the same LSM cadence as the rows and need no extra step.
5. **Repeat for every table** where the subject's data may live.
6. **Purge every replica separately** — `PurgeHistory`/delete-vacuum do not
   propagate through replication; each node that has pulled the subject's data
   must be purged independently (§2.6).
7. **Document the residuals** honestly in the erasure response — the interner
   caveat (§2.4), the HNSW index-residual caveat (§2.5), and the replica caveat
   (§2.6).

For **time-based retention** (e.g. "drop all data older than 90 days"),
`SetRetention` is the right tool: set `max_age_secs: 86400 * 90` once and
`vacuum_key` will reclaim over-age versions inline on each subsequent write.
Add a periodic `PurgeHistory` sweep if the table is rarely written to (so
stale versions don't linger waiting for a write to trigger vacuum).

### 2.3 Latency: "purge issued" → "physically gone from disk"

A purge is **not instantaneous** at the physical level. Three layers must
each reclaim before the bytes are gone:

| Layer | What survives after `PurgeHistory` returns | When it is reclaimed |
|---|---|---|
| **MVCC `history` log (logical)** | Reclaimed versions are removed from `history` synchronously inside `purge_below_ts` (`mvcc_gc.rs:438-440` — `self.history.remove(phys_key)` and `self.history.remove(ts_key(version))` run inline, in the same call, before the count is returned). | **Same call.** The `purged` count returned to the client reflects versions already removed from `history`. |
| **In-memory overlay** | Reclaimed versions `≤ durable_watermark` are dropped synchronously (`mvcc_gc.rs:445-447`). Versions `> durable_watermark` (not yet drained into `history`) are **left in place** — they are the only copy until the drainer lands them. | Versions above the watermark are dropped on the next `gc_overlay_to(durable)` sweep inside the drainer (`crates/shamir-engine/src/tx/drainer.rs:608-611`). Because step (1) of `handle_purge_history` forces `drain_all` first, in practice the post-purge watermark is already caught up and the overlay is empty or near-empty. |
| **WAL sealed segments** | The WAL is **append-only by version**; `purge_below_ts` does **not** truncate it. A purged version's bytes still live in any sealed WAL segment whose `max_version ≥` that version until the segment is unlinked. | Truncated by the drainer's `settle_and_truncate` (`drainer.rs:644-719`), which calls `wal.truncate_below(ceiling)` where `ceiling = min(durable_watermark, min(pending_unsafe) − 1)`. The truncation is gated on (a) the version being durable in `history` AND (b) the A5 interner-hwm gate being satisfied for any interner delta the segment carries. Truncation runs on the **background drainer cadence**, not inline with the purge. So: a window exists where the purged data is logically gone from `history` but physically present in a sealed WAL segment on disk. The window closes on the next drainer pass that crosses the segment's `max_version`. |
| **fjall LSM-tree (SST files)** | `history.remove(phys_key)` writes a fjall **tombstone** into the LSM memtable; it does **not** unlink the SST file that holds the original key. The original bytes survive in level-0/level-N SST files until fjall's background compaction rewrites them. | Reclaimed by fjall's compaction strategy (default: size-tiered). The tombstone propagates through compaction levels; once a compaction at or above the original level drops the original entry, the bytes are physically gone. **The database has no API to force a full compaction** — compaction runs on fjall's own schedule, governed by memtable flush and tier thresholds. |

**Net latency:** logically (from the database's read path) the data is gone
the moment `PurgeHistory` returns (subsequent reads do not see it). But
**physically** — i.e. for an attacker with raw disk access who is decoding
LSM SST files or untruncated WAL segments directly — the data may remain
recoverable for a window bounded by (a) the next drainer pass that truncates
the WAL segment, and (b) the next fjall compaction that rewrites the SST.
Neither duration is configurable from the database; both are bounded (the
WAL is bounded by the segment-size × write-rate; the LSM by the compaction
tier thresholds) but neither is instantaneous.

**Implication for the erasure procedure (§2.2):** when acknowledging a
GDPR Art. 17 request, do **not** claim "deleted immediately and
unrecoverably." The accurate statement is: *"the data is logically removed
from all live read paths; physical reclamation of the underlying storage
follows the database's normal WAL-truncation and LSM-compaction cadence,
which on encrypted storage (§1.3) is sufficient because the residual bytes
are themselves encrypted at the volume layer."* This is exactly why §1.3's
encrypted-volume requirement is **load-bearing**: it converts "physically
present for a compaction window" from a compliance problem into a non-issue,
because the residual bytes are ciphertext to anyone without the volume key.

### 2.4 Residual: the field-name interner (honest assessment)

**This is the residual the audit specifically flagged, documented honestly.**

The database interns field names to `u64` ids for compression: every record's
field **names** (e.g. `"email"`, `"created_at"`) are stored once in a
per-repo interner (`crates/shamir-engine/src/table/interner_manager.rs`), and
records reference them by id. The interner is:

* **Per-repo, shared across all tables in the repo**
  (`repo_instance.rs:496-509` — `repo_interner()`, a single
  `Arc<OnceCell<InternerManager>>` per `RepoInstance`).
* **Append-only.** `Interner` (`crates/shamir-types/src/core/interner/interner.rs`)
  exposes `touch_ind` / `touch_with_id` / `get_*` / `make_key` but **no
  remove/delete API.** The manager persists incrementally as chunks
  (`interner_manager.rs:21-37`), each chunk only **adding** entries; nothing
  ever removes them.
* **Not purged per-record or per-table.** `purge_below_ts`, `vacuum_key`,
  `gc_overlay_to`, and `DropTable`/`store_delete` operate on the data stores;
  none of them touch the interner. Even `DropTable` (which deletes the
  keyspace) leaves the interner intact.

**Consequence:** after a full erasure of a data subject's records (Delete +
PurgeHistory + WAL truncation + LSM compaction), the **field names** the
subject's records used are still present in the interner. So "this repo once
contained records that had a field named X" is discoverable from the
interner even after every value is gone. If `X` is a generic schema field
name like `"email"`, this leaks nothing about the specific subject; if `X`
were a dynamically-named field whose name **itself** carried PII (e.g. a
field literally named `"john_doe_ssn_123-45-6789"`), the leak would be
real.

#### Assessment for this system's typical field-name shapes

In the typical S.H.A.M.I.R. DB workload, field names are **schema-like and
generic** — `"email"`, `"name"`, `"created_at"`, `"address"`, `"user_id"`.
This is true by construction: the database has a declarative-schema feature
(`CreateTableOp.schema`, `crates/shamir-query-types/src/admin/types/table_ops.rs:31`
— `Vec<FieldRuleDto>`) that encourages fixed schema field names per table;
the query-builder (`shamir-query-builder`) and the validators
(`docs/dev-artifacts/roadmap/VALIDATORS.md`) are built around a small, stable set of
field names per table, not arbitrary dynamic keys. Under that shape:

* An interner entry for `"email"` says only *"this repo once had at least
  one record with an `email` field"*. That fact is also revealed by the
  table's declared schema (which lives in the `__info__` store and is
  similarly not purged per-record), by the existence of any index on that
  field, and by the application's own schema definition. The interner adds
  no new identifying signal beyond what the schema catalogue already
  exposes.
* An interner entry does **not** contain the subject's identity, the
  field's value, the record's primary key, or any association back to a
  specific record. It is just `(name, id)`.

**Honest conclusion:** for the **typical** workload (generic schema field
names), the interner residual does **not** constitute meaningful PII
exposure of a specific data subject — the residual is "the schema field
existed", not "this subject existed". An erasure response can defensibly
state this.

**The residual IS meaningful, and must be disclosed in the erasure
response, if and only if the application uses dynamically-named fields
whose names themselves carry PII** (e.g. per-subject keys, per-tenant
namespaces keyed by customer name, free-form attribute names set to
subject identifiers). The database does not prevent this usage — it
interns any `&str` field name — so the operator must assess their own
schema. If the operator's schema uses such PII-bearing field names, the
interner residual is a real leak that this database cannot currently
close (there is no interner-prune API); the only complete mitigations
today are (a) avoid PII-bearing field names in the schema (use generic
field names with the PII in the *value*, which is purgable), or (b) place
the data directory on encrypted storage (§1.3) so the interner chunks on
disk are ciphertext, making the residual unrecoverable without the volume
key. A future "interner GC" feature that prunes entries no longer
referenced by any record is a tracked gap, not a current capability.

### 2.5 Residual: secondary-index copies (HNSW / FTS) until compaction

**This is the residual the audit specifically flagged, documented honestly.**

The §2.2 erasure procedure reclaims the **data store** (the `history` log + WAL +
LSM). It does **not**, by itself, reclaim copies of the subject's data that live
in **secondary indexes**. There are two distinct index types and they behave
differently — an operator must understand both.

#### Vector index (HNSW) — soft-delete tombstones, persisted into the snapshot

Vector deletion is a **soft-delete tombstone**, not a removal. The HNSW adapter
(`crates/shamir-vector/src/hnsw_adapter/`) documents this in its module
header (`:7-8` — "Deletion via soft-delete tombstone set; search over-scans ×2
to compensate"). The `delete` path (`:2689-2725`) inserts a tombstone into the
`deleted` set and drops the brute-force / quantized side-copies, **but the graph
node itself stays** — verbatim from the code (`:2708`):

> "The graph node stays (hnsw_rs has no delete) but search filters tombstoned
> internals, so it's invisible."

"Invisible" means **excluded from live query results** — it does **not** mean
"gone from disk." The tombstoned vector embedding is physically present in the
HNSW graph and is **persisted into the on-disk index snapshot**: the snapshot
codec (`crates/shamir-vector/src/snapshot/`) serialises the graph dump
(`.hnsw.graph` / `.hnsw.data` chunk files — `:14-20`) which carry **all** nodes
including tombstoned ones (the graph has no notion of deletion), and the sidecar
(`:22-24`) explicitly persists the adapter's maps including the `tombstones`
set. The tombstone-iteration hook used for serialisation is
`HnswAdapter::for_each_deleted` (`hnsw_adapter.rs:503-509`).

The tombstoned vectors are **physically removed from the snapshot only when a
compaction rebuilds the graph from the live (non-tombstoned) set**. Compaction
is triggered by `VectorBackend::trigger_compaction_check`
(`vector_backend.rs:982-1009`) when the tombstone ratio crosses the threshold
(`deleted_ratio() >= 0.3` **AND** `live_count >= 1000` — constants at
`crates/shamir-tunables/src/lib.rs:155,159`; gating at `vector_backend.rs:993`).
The rebuild writes a **new generation** of the snapshot and **removes the old
generation's chunk files** (`snapshot.rs:1298-1316` — `KvOp::Remove` for each
old `graph`/`data`/`sidecar` chunk). Only after that flip are the tombstoned
embeddings physically absent from the on-disk snapshot.

**There is no operator API to force a compaction** — it fires on the
ratio/threshold schedule above, on the next qualifying delete. An operator who
deletes a single subject's vectors from a large index may wait a long time for
the ratio to cross 30 %.

#### Full-text-search index (FTS) — postings live in the regular data store

The FTS backend (`crates/shamir-index/src/fts_backend.rs`) stores postings as
type-tagged keys in the **regular data `Store`** (LSM), not in a separate
structure. Its `plan_delete` (`:177-191`) emits `IndexWriteOp::RemovePosting`
ops that delete the posting keys from the store. A posting deletion therefore
writes an **LSM tombstone** and the original posting bytes survive in SST files
until fjall compaction — which is **exactly the latency story §2.3 already
documents** for the data layer. FTS postings are **not** a separate compaction
model; they ride the same LSM-compaction cadence as the row data itself. (The
ranked/BM25 FTS variant, `fts_ranked_backend.rs`, follows the same
posting-in-store model.)

#### What this means for the §2.2 erasure procedure

* **FTS indexes:** no additional step beyond §2.2 step 4 (wait for LSM
  compaction). The postings are reclaimed on the same cadence as the rows.
* **Vector (HNSW) indexes:** this is a **real gap in the erasure procedure's
  completeness**. An operator who has completed §2.2 (Delete + PurgeHistory +
  WAL/LSM reclamation) has **not** achieved physical erasure of the subject's
  **embeddings** until an HNSW compaction has run and flipped the snapshot
  generation. The embeddings remain readable (to anyone with raw-disk access who
  decodes the snapshot chunks) for an unbounded window that depends on future
  delete traffic crossing the 30 % ratio. As with §2.3's LSM residual, §1.3's
  encrypted-volume requirement converts "physically present for a compaction
  window" into ciphertext — the residual bytes are unrecoverable without the
  volume key. A future "force-compaction-on-erasure" operator command is a
  tracked gap, not a current capability.

### 2.6 Residual: purge does not propagate to replicas

**This is the residual the audit specifically flagged, documented honestly.**

Replication is a **leader-side pull API** (`crates/shamir-server/src/db_handler/repl_handler.rs:1-15`,
:42-77): a follower holding the `replicator` capability (or a superuser — gate at
`:50`) calls `Hello` to discover readable repos, then `Pull` to read a batch of
**commit changelog events** for one repo (`handle_pull` →
`read_changelog_from_journal`, `repl_handler.rs:123-128,155,191`). The changelog
carries **committed data writes** (insert/update/delete of row versions) — it is
the replication transport, and it records nothing else.

`PurgeHistory` and delete-vacuum are **garbage-collection operations**, not
commits. `PurgeHistory` (`crates/shamir-db/src/shamir_db/execute/admin_retention.rs:138-139`)
calls `MvccStore::purge_below_ts` (`crates/shamir-tx/src/mvcc_store/mvcc_gc.rs:396`),
which reclaims history versions **directly** out of the leader's `history` log.
This reclamation does **not** emit a changelog event — the changefeed records
commits, not GC — so a follower that has **already pulled** copies of the
now-purged versions never learns that they were purged. Those copies persist on
the replica until the replica's **own** retention/vacuum/purge reclaims them (and
then its own WAL/LSM/index compaction runs per §2.3/§2.5).

**Consequence for GDPR Art. 17:** in a replicated deployment there is **no
single "purge everywhere" command** today. An operator satisfying a right-to-
erasure request across a replicated topology **must separately purge each
replica** — issue the §2.2 procedure (Delete + PurgeHistory + wait for
WAL/LSM/HNSW reclamation) on **every** node that has pulled the subject's data.
The leader's purge reclaims only the leader's copy.

This is adjacent to — but distinct from — the existing requirement at §1.3's
deployment table (line 85: "Backups / snapshots / replicas — the encryption
obligation follows the data"). That line establishes that replica volumes must be
**encrypted** (so residual bytes are ciphertext); it does **not** address
**purge propagation**, which is an independent, additive obligation: even on
encrypted storage, the logical data must be actively purged on each replica to
satisfy an erasure request — encryption alone does not constitute erasure.

A future "purge-propagation" feature (propagating `PurgeHistory`/delete-vacuum
through the changelog so followers reclaim in lockstep) is a tracked gap, not a
current capability.

---

## 3. Retention defaults

### 3.1 The default is `CurrentOnly` — privacy-favorable

The engine's default per-table retention is **`CurrentOnly`** — keep only the
current version of each key, reclaim every prior version eagerly on the next
write. This is a **minimal-retention** default, which is the
privacy-favorable end of the spectrum.

Verified at:

* `crates/shamir-tx/src/mvcc_store/mod.rs:200` —
  `retention: ArcSwap::new(Arc::new(Retention::current_only()))` in
  `MvccStore::new`.
* `crates/shamir-tx/src/mvcc_store/mod.rs:191` (doc) — "Defaults to
  `Retention::current_only` (eager vacuum)".
* `crates/shamir-query-types/src/admin/types/table_ops.rs:23-25` (doc) —
  `CreateTableOp.retention`: "`None` (absent on the wire) = CurrentOnly —
  today's default behaviour (no history retained)".
* `crates/shamir-tx/src/mvcc_store/retention.rs:2` (doc) — "Default =
  CurrentOnly (`max_count: Some(0)`): keep only [the current version]".
* `crates/shamir-db/src/shamir_db/execute/helpers.rs:151` (doc) — "the next
  instantiation will pick up CurrentOnly (the `MvccStore::new` default)".
* Behavioural test: `crates/shamir-tx/src/tests/mvcc_store_tests/retention_tests.rs:536-537`
  — "Default is CurrentOnly (max_count: Some(0))".
* E2E test: `crates/shamir-db/tests/schema_ddl/retention_ddl.rs:91-106` —
  `create_table_without_retention_is_current_only` asserts `max_count ==
  Some(0)` after a `CreateTable` with no `retention` field.

### 3.2 What this means in practice

* **Out of the box, the database does NOT retain history.** A record
  updated in place discards its prior value on the next write to that key
  (modulo a single deferred "anchor" version kept for live-snapshot reads,
  per the A10 invariant — see `vacuum_targeted_tests.rs`).
* **This is a privacy-favorable default.** For GDPR-style data minimisation,
  the default already leans toward "keep the least". Operators who want
  audit history / time travel opt in *explicitly* via `SetRetention` or via
  `CreateTable { retention: Some(...) }`.
* **`Retention::default()` is NOT `CurrentOnly`.** Note the asymmetry:
  `Retention::default()` (`retention.rs:17` derive) is all-`None` =
  **Forever**; but the *engine's* default when a table is created without
  specifying retention is `CurrentOnly` (the table layer substitutes
  `CurrentOnly` for `None`). So: the *DTO's* `Default` is "forever" (a
  permissive sentinel), but the *system's* default behaviour is
  `CurrentOnly`. The system behaviour is what matters for an out-of-the-box
  deployment. If you build a `Retention` value via `Retention::default()`
  in application code and pass it explicitly, you get **Forever**, not
  `CurrentOnly` — be deliberate.
* **Default retention ≠ erasure.** `CurrentOnly` reclaims history versions
  eagerly on the **next write to the same key**. It does **not** reclaim
  anything for a key that is never written again. For a data subject whose
  record is deleted (not updated), the tombstone is the only "next write"
  and the prior value is vacuumed at that point — but for a key that simply
  stops being written, the current value persists indefinitely. Erasure
  still requires the explicit §2.2 procedure.

---

## 4. Summary (for a compliance reviewer)

| Question | Answer |
|---|---|
| Is record data encrypted at rest by the database? | **No.** Data is plaintext LZ4-compressed MessagePack on disk. Verified by grep across `shamir-storage`, vendored `fjall-3.0.1` and `lsm-tree-3.0.1`. |
| Is the lack of at-rest encryption a compliance gap? | **No, given operator action.** The model is *delegated* to the OS/volume layer (LUKS/BitLocker/cloud disk encryption). The operator MUST place `data_dir` on encrypted storage. This is the same model SQLite/RocksDB document. |
| Is transport encrypted? | **Yes** — TLS via rustls (covered in `SECURITY.md`, not here). |
| What is the default retention? | **`CurrentOnly`** — minimal-retention, privacy-favorable. History is not retained unless explicitly opted in. |
| Is there a "right to erasure" API? | **No single API.** Compose `Delete` (current value) + `PurgeHistory` (history versions) + wait for WAL truncation + LSM compaction. Both ops are wire-protocol, gated by `Action::Manage`. |
| Is erasure instantaneous on disk? | **No.** Logically gone when `PurgeHistory` returns; physically gone after WAL segment truncation (drainer cadence) and LSM compaction (fjall cadence). On encrypted storage (§1.3), this latency is compliance-irrelevant because the residual bytes are ciphertext. |
| Does anything survive full erasure? | **Yes — the field-name interner.** Field names a record once used remain interned even after all values are gone. For generic schema field names (`"email"`, etc.) this is not meaningful PII; for PII-bearing dynamic field names it is a real residual that this database cannot currently close. |
| Does this contradict `SECURITY.md`? | **No.** `SECURITY.md` covers vuln disclosure and supply-chain posture; it explicitly notes `server-cert.pem` is git-ignored and that no hardcoded secrets are tracked. This document covers at-rest data protection (records, WAL, interner) — a different surface. The two are complementary. |

---

_Documentation of current behaviour as of the master branch. Closes audit
finding C6. No code changes were made to produce this document; every factual
claim is cited to a `file:line` location that can be re-verified._

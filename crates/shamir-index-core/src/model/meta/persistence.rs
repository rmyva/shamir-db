//! Persistence for index2 registry — save/load `IndexDescriptor`s
//! via `MetaEnvelope` to `__meta__/indexes`.
//!
//! # Legacy index format version (S9)
//!
//! `LEGACY_INDEX_FORMAT_VERSION` tracks the on-disk format of the legacy
//! hash/unique/sorted index postings. When the engine opens a table whose
//! stored version is less than the current constant, it MUST trigger a full
//! O(N) rebuild (drop old postings, re-index every record from the data
//! store via the doctor's `repair`-style machinery). This is a one-time
//! reindex per table per version bump.
//!
//! Version history:
//!   1 — original format: `<Value<InternerKey> as Hash>` with
//!       `std::mem::discriminant` tags; covering projection as
//!       `Vec<(String, InnerValue)>`.
//!   2 — S9 lens-native format: stable u8 discriminant tags via
//!       `hash_scalar_ref`/`hash_inner_value`; covering projection as
//!       `Vec<(String, QueryValue)>` (scalar-only, wire-compat with
//!       InnerValue decode).
//!
//! # Identity v2 (P34-12)
//!
//! The base family (regular and unique postings) moved to canonical-byte
//! identities. `LEGACY_INDEX_FORMAT_VERSION` stays 3 and `_m.idx.lfv` is
//! no longer written by this binary; the family is current iff
//! `_m.idx.idv == 2` AND `_m.idx.lfv` is absent
//! ([`identity_migration_needed`]). An old binary that touches the table
//! rebuilds in v3 and stamps `lfv`, which makes the next new open migrate
//! again. `idv > 2` (written by a newer binary) is a typed fail-closed open
//! error ([`check_identity_version`]); an `idv` marker that is not exactly
//! four bytes is a codec error. Step 1 of the migration clears both markers
//! in one transaction ([`clear_identity_markers`]), step 3 stamps `idv`.
//!
//! The index2 metadata envelope (`_m.idx`) is version 2 whenever any FTS or
//! functional descriptor is persisted (those postings live in the
//! `FTS_V2`/`FUNCTIONAL_V2` namespaces), and version 1 otherwise (empty,
//! btree- or vector-only), so an older binary can still open a table that
//! has no FTS/functional index and refuses one that has. Dropping the last
//! such index returns the envelope to version 1. The flip from 1 to 2 with
//! every FTS/functional descriptor marked `Building` is the durable commit
//! point of the index2 migration ([`save_persisted_index2_metadata`]).

use crate::descriptor::IndexDescriptor;
use crate::{MetaEnvelope, MetaError};
use bytes::Bytes;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use shamir_storage::types::{KvOp, RecordKey, Store};
use shamir_tunables::store_defaults::MAINT_SCAN_BATCH;
use shamir_types::types::record_id::RecordId;
use std::sync::Arc;

/// Current legacy index posting format version. Bump this whenever
/// the posting-key hash scheme or covering-projection blob format
/// changes in a way that is NOT backward-compatible with existing
/// on-disk data. The engine checks this on open and triggers a
/// rebuild when the stored version is older.
pub const LEGACY_INDEX_FORMAT_VERSION: u32 = 3;

// The meta key tag "_m.idx" is byte-identical to MetaKey::Indexes.tag() in the engine.
fn meta_key_indexes() -> RecordId {
    RecordId::system("_m.idx")
}

/// System key for the legacy index format version marker.
fn meta_key_legacy_index_version() -> RecordId {
    RecordId::system("_m.idx.lfv")
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedIndexes {
    pub next_id: u32,
    pub descriptors: Vec<IndexDescriptor>,
}

pub async fn save_index2_metadata(
    registry: &crate::IndexRegistry,
    info_store: &Arc<dyn Store>,
) -> Result<(), shamir_storage::error::DbError> {
    save_index2_metadata_with_pending(registry, info_store, None).await
}

/// Persist the index2 registry metadata, optionally including a `pending`
/// in-flight descriptor that is NOT yet in the live registry (F-50 Step 3b).
///
/// `create_index_v2` builds a backend's descriptor with `state = Building`
/// and needs that Building descriptor durably on disk BEFORE the backfill
/// runs (so a crash between the backfill and the final `Ready` persist is
/// detectable on restart). But the backend must NOT be inserted into the
/// live registry until after the backfill completes (the live
/// `index2_on_insert` hook can't route to an unregistered backend — see
/// `backfill_index2_backend`'s doc comment). This variant lets the caller
/// pass the in-flight Building descriptor as `pending` so the persisted set
/// is `all_descriptors() ∪ {pending}` without publishing the backend.
///
/// If `pending`'s id collides with an already-registered backend (defensive —
/// shouldn't happen in the normal create flow), the pending entry is dropped
/// in favor of the live registry's entry.
pub async fn save_index2_metadata_with_pending(
    registry: &crate::IndexRegistry,
    info_store: &Arc<dyn Store>,
    pending: Option<IndexDescriptor>,
) -> Result<(), shamir_storage::error::DbError> {
    let mut descriptors = registry.all_descriptors().await;
    if let Some(d) = pending {
        // Defensive de-dup: never write two descriptors with the same id
        // (the normal create flow is pre-register so this branch is a no-op,
        // but a caller that races a concurrent insert + pending with the
        // same id must not produce a malformed blob).
        if !descriptors.iter().any(|x| x.id == d.id) {
            descriptors.push(d);
        }
    }
    for descriptor in &mut descriptors {
        if let crate::kind::IndexKind::Vector(config) = &descriptor.kind {
            super::index_options::write(&mut descriptor.options, config.quantization)?;
        }
    }
    let p = PersistedIndexes {
        next_id: registry.peek_next_id(),
        descriptors,
    };
    save_persisted_index2_metadata(p, info_store).await
}

/// Persist an already-loaded descriptor snapshot without routing through a
/// partially populated live registry. Used for the atomic identity-v2 flip:
/// every FTS/functional descriptor is marked Building before postings change.
pub async fn save_persisted_index2_metadata(
    persisted: PersistedIndexes,
    info_store: &Arc<dyn Store>,
) -> Result<(), shamir_storage::error::DbError> {
    let version = if descriptors_have_identity_v2(&persisted.descriptors) {
        2
    } else {
        1
    };
    let envelope = MetaEnvelope::new_with_version(version, persisted);
    let bytes = envelope
        .encode()
        .map_err(|e| shamir_storage::error::DbError::Internal(e.to_string()))?;
    info_store
        .set(meta_key_indexes().to_bytes().into(), Bytes::from(bytes))
        .await?;
    Ok(())
}

fn descriptors_have_identity_v2(descriptors: &[IndexDescriptor]) -> bool {
    descriptors.iter().any(|d| {
        matches!(
            d.kind,
            crate::kind::IndexKind::Fts { .. } | crate::kind::IndexKind::Functional { .. }
        )
    })
}

pub async fn load_index2_metadata(
    info_store: &Arc<dyn Store>,
) -> Result<Option<(u16, PersistedIndexes)>, shamir_storage::error::DbError> {
    let key = meta_key_indexes();
    let bytes = match info_store.get(key.to_bytes().into()).await {
        Ok(bytes) => bytes,
        Err(shamir_storage::error::DbError::NotFound(_)) => return Ok(None),
        Err(e) => return Err(e),
    };
    let (version, persisted) = decode_persisted_indexes_with_version(&bytes)?;
    Ok(Some((version, persisted)))
}

/// Decode the `__meta__/indexes` blob into the current [`PersistedIndexes`],
/// with a forward-compat fallback for blobs written BEFORE the `state`
/// field existed (F-50 Step 3a).
///
/// bincode 1.3.3 does NOT honour `#[serde(default)]` for a NEW trailing
/// field — a pre-`state` blob fails to decode as the current shape with
/// `io error: unexpected end of file` (proven by the F-50 Step 3a
/// round-trip test, `tests/index_state_compat_tests.rs`). So the current
/// shape is tried first; on a decode failure the pre-`state` shadow shape
/// is tried and each descriptor is lifted to `state = Ready` (every
/// pre-`state` persisted index was fully built — a `Building` index could
/// never have been persisted before the `state` field existed). Any other
/// failure (bad magic, unsupported envelope version, or a blob that
/// decodes as NEITHER shape — genuine corruption) is surfaced as an error.
#[cfg(test)]
pub(crate) fn decode_persisted_indexes(
    bytes: &[u8],
) -> Result<PersistedIndexes, shamir_storage::error::DbError> {
    decode_persisted_indexes_with_version(bytes).map(|(_, p)| p)
}

pub(crate) fn decode_persisted_indexes_with_version(
    bytes: &[u8],
) -> Result<(u16, PersistedIndexes), shamir_storage::error::DbError> {
    let (version, mut persisted) =
        match MetaEnvelope::<PersistedIndexes>::open_accepting(bytes, &[1, 2]) {
            Ok((version, p)) => Ok((version, p)),
            Err(MetaError::Decode(new_err)) => {
                // Possible pre-`state` blob — try the legacy shadow shape.
                match MetaEnvelope::<forward_compat::PersistedIndexesNoState>::open(bytes) {
                    Ok(legacy) => {
                        log::warn!(
                            "index2 metadata: decoded with pre-`state` legacy fallback \
                         ({} descriptor(s) lifted to state=Ready). \
                         New-shape decode error: {}",
                            legacy.descriptors.len(),
                            new_err
                        );
                        Ok((1, PersistedIndexes::from(legacy)))
                    }
                    Err(legacy_err) => {
                        // Decodes as neither shape — genuine corruption.
                        Err(shamir_storage::error::DbError::Internal(format!(
                            "index2 metadata decode failed (new shape: {new_err}; \
                         legacy shape: {legacy_err})"
                        )))
                    }
                }
            }
            Err(e) => Err(shamir_storage::error::DbError::Internal(e.to_string())),
        }?;
    for descriptor in &mut persisted.descriptors {
        if let crate::kind::IndexKind::Vector(config) = &mut descriptor.kind {
            config.quantization = super::index_options::decode(&descriptor.options)?;
        }
    }
    Ok((version, persisted))
}

/// Pre-`state` on-disk shadow shapes used ONLY by the forward-compat
/// fallback in [`load_index2_metadata`]. These mirror the exact field
/// order/types of `IndexDescriptor`/`PersistedIndexes` as they existed
/// BEFORE the `state: IndexState` field was added (F-50 Step 3a), so
/// genuinely old on-disk bytes can be decoded and lifted to the current
/// shape. Write-side always emits the current shape; these are read-only.
mod forward_compat {
    use crate::kind::IndexKind;
    use serde::{Deserialize, Serialize};
    use smallvec::SmallVec;

    /// Pre-`state` `IndexDescriptor` shadow.
    #[derive(Debug, Serialize, Deserialize)]
    pub(in crate::persistence) struct IndexDescriptorNoState {
        pub id: u32,
        pub name: String,
        pub name_interned: u64,
        pub paths: SmallVec<[Vec<u64>; 2]>,
        pub kind: IndexKind,
        pub created_at_nanos: u64,
        #[serde(default)]
        pub options: Vec<u8>,
    }

    /// Pre-`state` `PersistedIndexes` shadow.
    #[derive(Debug, Serialize, Deserialize)]
    pub(in crate::persistence) struct PersistedIndexesNoState {
        pub next_id: u32,
        pub descriptors: Vec<IndexDescriptorNoState>,
    }
}

impl From<forward_compat::PersistedIndexesNoState> for PersistedIndexes {
    fn from(legacy: forward_compat::PersistedIndexesNoState) -> Self {
        PersistedIndexes {
            next_id: legacy.next_id,
            descriptors: legacy
                .descriptors
                .into_iter()
                .map(|d| IndexDescriptor {
                    id: d.id,
                    name: d.name,
                    name_interned: d.name_interned,
                    paths: d.paths,
                    kind: d.kind,
                    created_at_nanos: d.created_at_nanos,
                    options: d.options,
                    // Every pre-`state` persisted index was fully built;
                    // a `Building` index could not have been persisted
                    // before this field existed.
                    state: crate::state::IndexState::default(),
                })
                .collect(),
        }
    }
}

// ============================================================================
// Legacy index format version (S9)
// ============================================================================

/// Persist the current `LEGACY_INDEX_FORMAT_VERSION` to the info store.
/// Called after a successful index rebuild or on first index creation.
pub async fn save_legacy_index_version(
    info_store: &Arc<dyn Store>,
) -> Result<(), shamir_storage::error::DbError> {
    let key = meta_key_legacy_index_version();
    let bytes = LEGACY_INDEX_FORMAT_VERSION.to_le_bytes();
    info_store
        .set(key.to_bytes().into(), Bytes::from(bytes.to_vec()))
        .await
        .map(|_| ())
}

/// Load the stored legacy index format version. Returns `0` if no
/// version marker exists (pre-S9 data — always needs rebuild).
pub async fn load_legacy_index_version(
    info_store: &Arc<dyn Store>,
) -> Result<u32, shamir_storage::error::DbError> {
    let key = meta_key_legacy_index_version();
    match info_store.get(key.to_bytes().into()).await {
        Ok(bytes) => {
            if bytes.len() >= 4 {
                let arr: [u8; 4] = bytes[..4].try_into().unwrap_or([0; 4]);
                Ok(u32::from_le_bytes(arr))
            } else {
                Ok(0)
            }
        }
        Err(shamir_storage::error::DbError::NotFound(_)) => Ok(0),
        Err(e) => Err(e),
    }
}

/// Check whether the legacy index postings need a rebuild (stored version
/// is older than `LEGACY_INDEX_FORMAT_VERSION`).
pub async fn legacy_indexes_need_rebuild(
    info_store: &Arc<dyn Store>,
) -> Result<bool, shamir_storage::error::DbError> {
    let stored = load_legacy_index_version(info_store).await?;
    Ok(stored < LEGACY_INDEX_FORMAT_VERSION)
}

// ============================================================================
// Identity v2 marker (`_m.idx.idv`, design P34-12 sections (b), (e) S2)
// ============================================================================
//
// The base family (regular + unique hash postings) is CURRENT (identity v2)
// iff `_m.idx.idv` == 2 AND `_m.idx.lfv` is ABSENT. New binaries NEVER write
// `_m.idx.lfv` again. The S9b helpers above are kept so the engine and tests
// can detect and simulate old-binary data (`lfv` present ⇒ the next new
// open re-migrates; an old binary finding `lfv` absent rebuilds in v3 and
// stamps `lfv`, which forces the next new open to migrate again).
//
// Newer-than-me (`idv > 2`) is a typed, fail-closed open error — the
// forward gate the `lfv` scheme lacked (an old binary silently accepted a
// newer stamp).

/// The identity version this binary writes: 2 (codec of `model::identity`,
/// namespaces 0x02/0x03, digest form 0xFF || BLAKE3-256).
pub const INDEX_IDENTITY_VERSION: u32 = 2;

/// System key for the base-family identity version marker. `"_m.idx.idv"`
/// is 10 bytes — no `RecordId::system` truncation — and diverges from
/// `_m.idx.lfv` / `_m.idx.drop` / `_m.idx.ren` at byte 10 (0x64 `d` vs
/// 0x2e `.` / 0x6c / 0x72), same check as the `_m.idx.drop` analysis above.
fn meta_key_identity_version() -> RecordId {
    RecordId::system("_m.idx.idv")
}

/// Persist the identity version this binary writes (`2`, u32 LE). Commit
/// point (step 3) of the base migration protocol.
pub async fn save_index_identity_version(
    info_store: &Arc<dyn Store>,
) -> Result<(), shamir_storage::error::DbError> {
    let key = meta_key_identity_version();
    let bytes = INDEX_IDENTITY_VERSION.to_le_bytes();
    info_store
        .set(key.to_bytes().into(), Bytes::from(bytes.to_vec()))
        .await
        .map(|_| ())
}

/// Load the stored base-family identity version: `Some(v)` when the
/// `_m.idx.idv` marker exists with exactly four bytes, `None` when it
/// is absent (pre-v2 data, or the migration's step 1 removed it).
pub async fn load_index_identity_version(
    info_store: &Arc<dyn Store>,
) -> Result<Option<u32>, shamir_storage::error::DbError> {
    let key = meta_key_identity_version();
    match info_store.get(key.to_bytes().into()).await {
        Ok(bytes) => {
            if bytes.len() == 4 {
                let arr: [u8; 4] = bytes[..4].try_into().unwrap_or([0; 4]);
                Ok(Some(u32::from_le_bytes(arr)))
            } else {
                let message = if bytes.len() < 4 {
                    "shorter than four bytes"
                } else {
                    "longer than four bytes"
                };
                Err(shamir_storage::error::DbError::Codec(format!(
                    "system:_m.idx.idv corrupt: {} bytes ({message}), expected exactly 4 (u32 LE)",
                    bytes.len()
                )))
            }
        }
        Err(shamir_storage::error::DbError::NotFound(_)) => Ok(None),
        Err(e) => Err(e),
    }
}

/// Forward gate: `idv > 2` means the table was written by a NEWER binary —
/// typed, fail-closed open error. `idv <= 2` (or absent) is acceptable.
pub async fn check_identity_version(
    info_store: &Arc<dyn Store>,
) -> Result<(), shamir_storage::error::DbError> {
    if let Some(v) = load_index_identity_version(info_store).await? {
        if v > INDEX_IDENTITY_VERSION {
            return Err(shamir_storage::error::DbError::Internal(format!(
                "table index identity version {v} is newer than this binary supports \
                 ({}): refusing to open fail-closed",
                INDEX_IDENTITY_VERSION
            )));
        }
    }
    Ok(())
}

/// Whether the base family needs the blocking open-time identity migration
/// (design (c)): `need = !(idv == 2 && lfv absent)`. `lfv` present means a
/// pre-v2 binary (including one that rebuilt in v3 after this binary's step
/// 1) touched the table — re-migrate.
pub async fn identity_migration_needed(
    info_store: &Arc<dyn Store>,
) -> Result<bool, shamir_storage::error::DbError> {
    let idv = load_index_identity_version(info_store).await?;
    // idv == 2 is current ONLY when the legacy marker is absent.
    Ok(idv != Some(INDEX_IDENTITY_VERSION) || legacy_index_version_is_present(info_store).await?)
}

/// `true` when a `_m.idx.lfv` marker exists (any value) — the "an old
/// binary touched this table after (or instead of) the migration" signal.
/// Kept as a named helper (the S9b read path) for the need rule and the
/// old-binary simulation tests.
pub async fn legacy_index_version_is_present(
    info_store: &Arc<dyn Store>,
) -> Result<bool, shamir_storage::error::DbError> {
    let key = meta_key_legacy_index_version();
    match info_store.get(key.to_bytes().into()).await {
        Ok(_) => Ok(true),
        Err(shamir_storage::error::DbError::NotFound(_)) => Ok(false),
        Err(e) => Err(e),
    }
}

/// Step 1 of the base migration protocol: one atomic transaction removing
/// BOTH markers (`lfv`, `idv`). From here every old binary will rebuild
/// (lfv absent) and every new binary will migrate (idv absent).
pub async fn clear_identity_markers(
    info_store: &Arc<dyn Store>,
) -> Result<(), shamir_storage::error::DbError> {
    let ops = vec![
        KvOp::Remove(meta_key_legacy_index_version().to_bytes().into()),
        KvOp::Remove(meta_key_identity_version().to_bytes().into()),
    ];
    info_store.transact(ops).await
}

// ============================================================================
// P0-3b (#988): DROP INDEX durable tombstone + crash recovery (index2)
// ============================================================================
//
// Mirrors #972's sorted-family and #959's base_index-family durable-tombstone
// pattern for the index2 family (fts / functional / vector).
//
// Sub-bug 3c (crash-resurrection): the OLD `drop_index2` had retire →
// sweep → persist ordering with NO durable tombstone. A crash between the
// sweep and the `save_index2_metadata` persist left the on-disk
// `PersistedIndexes` still listing the index as `Ready` while its postings
// were gone — on restart the planner would route queries to an index with
// zero postings, silently returning wrong (empty / missing) results. The
// durable tombstone (written BEFORE the sweep, cleared AFTER the persist)
// closes this: on restart, `TableManager::create`'s recovery hook sees the
// tombstone, removes the stale backend, sweeps its postings (idempotent),
// persists, and clears the tombstone.
//
// Sub-bug 3b (name-reuse ghost postings): an id in the tombstone set is
// rejected by `create_index_v2`'s namespace-reuse guard (resolving the
// tombstoned id's name via persisted metadata) until the tombstone clears.
//
// Tombstone shape: `Vec<(u32, String, Option<String>)>` (descriptor id, index name, op_id)
// serialized via bincode under `system:_m.idx.drop`. A separate key was chosen
// (mirroring #959's `idx_drop`/`uidx_drop` and #972's `sidx_drop`) to avoid touching
// `save_index2_metadata`/`load_index2_metadata`'s forward-compat fallback chain.
// An absent key or empty vec means "no in-progress drops".
//
// **CRITICAL** (the hard-won #959 collision lesson): `RecordId::system(name)`
// truncates `name` to 12 bytes. The key `"_m.idx.drop"` (11 bytes, no
// truncation) is verified distinct from the two existing index2 system keys:
//   `"_m.idx"`      → [0,0,0,0, 5f,6d,2e,69,64,78, 00,00,00,00,00,00]
//   `"_m.idx.lfv"`  → [0,0,0,0, 5f,6d,2e,69,64,78, 2e,6c,66,76,00,00]
//   `"_m.idx.drop"` → [0,0,0,0, 5f,6d,2e,69,64,78, 2e,64,72,6f,70,00]
// They share the first 10 bytes (`\0\0\0\0_m.idx`) but diverge at byte 10
// (0x00 vs 0x2e) and again at byte 11 (0x6c vs 0x64) — NO collision.
//
// #1051: the tombstone now carries the index name and operation ID minted at dispatch time
// (the `String` and `Option<String>` components). This enables recovery to:
// 1. Use the id for registry operations (remove_by_id, sweep)
// 2. Use the name to write the correct DdlOpStatus
// 3. Use the op_id to ensure clients can poll the exact operation they triggered
//
// #1204: the blob is now version-tagged: `[INDEX2_DROP_TOMBSTONE_VERSION |
// bincode(Vec<(u32, String, Option<String>)>)]`, mirroring
// `base_index::ddl_op_log::DDL_OP_LOG_VERSION`'s version-byte-prefix pattern
// (fail closed on an unrecognized version byte, no silent misparse as the
// current shape). See `decode_dropping_index2`'s doc for why the version
// byte value (0x81, NOT 0x01) and the decode tier order were both chosen
// deliberately, and for the explicit backward-compat decision (pre-#1051
// bare `Vec<u32>` support dropped; pre-#1204 unversioned tuple-vec support
// kept) reached for #1204.

/// Version byte for the index2 DROP tombstone envelope (#1204).
///
/// Deliberately **0x81** (high bit set), NOT a small value like
/// `base_index::ddl_op_log::DDL_OP_LOG_VERSION`'s `0x01`. `bincode`'s fixint
/// `u64` length prefix puts the vector's element COUNT in the first
/// (LE) byte, so an unversioned `bincode(Vec<..>)` blob with exactly one
/// entry — the single most common non-empty on-disk state for this key —
/// serializes with a leading byte of `0x01` (proven by
/// `p1204_version_byte_does_not_collide_with_legacy_len_1_first_byte`).
/// `ddl_op_log`'s key was "born versioned" (no unversioned data ever
/// existed under it), so `0x01` is safe there; this key is NOT — a low
/// version byte here would risk misreading a genuine one-entry legacy
/// tombstone as a (garbage) versioned payload. `0x81` cannot collide with a
/// legacy length-prefix byte unless >= 129 index2 DROPs are simultaneously
/// in flight against one table, which cannot happen: every entry reaches
/// `add_to_dropping_index2` only under that table's serialized DDL
/// admission path.
const INDEX2_DROP_TOMBSTONE_VERSION: u8 = 0x81;

/// Strict (reject-trailing-bytes) `bincode` decode of the tombstone entry
/// list. `bincode::deserialize`'s top-level convenience function defaults
/// to ALLOWING trailing bytes, which this key cannot afford: the
/// version-byte disambiguation in `decode_dropping_index2` relies on a
/// structurally-invalid decode attempt failing outright rather than
/// silently succeeding against a truncated prefix of the wrong shape.
fn strict_decode_dropping_entries(
    bytes: &[u8],
) -> Result<Vec<(u32, String, Option<String>)>, bincode::Error> {
    use bincode::Options;
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .reject_trailing_bytes()
        .deserialize(bytes)
}

/// Encode the index2 DROP tombstone as
/// `[INDEX2_DROP_TOMBSTONE_VERSION | bincode(entries)]`.
fn encode_dropping_index2(
    entries: &[(u32, String, Option<String>)],
) -> Result<Bytes, shamir_storage::error::DbError> {
    let body = bincode::serialize(entries).map_err(|e| {
        shamir_storage::error::DbError::Codec(format!("system:_m.idx.drop encode failed: {e}"))
    })?;
    let mut bytes = Vec::with_capacity(1 + body.len());
    bytes.push(INDEX2_DROP_TOMBSTONE_VERSION);
    bytes.extend_from_slice(&body);
    Ok(Bytes::from(bytes))
}

/// Decode the index2 DROP tombstone blob. An empty byte string decodes to
/// an empty `Vec` ("no in-progress drops" — an absent key is handled by the
/// caller before this is reached).
///
/// #1204 decode strategy / explicit backward-compat decision:
///   1. `[INDEX2_DROP_TOMBSTONE_VERSION | bincode(entries)]` — the current
///      versioned format. Decoded strictly; a payload decode failure here
///      is a genuine reported error, with NO further fallback attempted
///      (falling through to tier 2 on a payload error would defeat the
///      point of having a version byte at all).
///   2. Otherwise: tried as the LEGACY, unversioned
///      `bincode(Vec<(u32, String, Option<String>)>)` blob — the exact
///      format every `add_to_dropping_index2`/`clear_from_dropping_index2`
///      call wrote between #1051 and #1204. **Deliberately kept**: this is
///      not a rare crash-window artifact — `clear_from_dropping_index2`
///      never deletes the key, so EVERY table that has ever completed one
///      index2 DROP has this exact unversioned empty-vec blob durably on
///      disk today. Dropping this fallback would fail `TableManager::create`
///      (table open, via `recover_index2_drops`) outright for every such
///      table on the next restart.
///   3. The bare pre-#1051 `Vec<u32>` shape (no name/op_id) is
///      **intentionally NOT supported** as of #1204 (previously handled by
///      a second fallback tier here). That shape could only be produced by
///      a process that crashed mid-DROP on a pre-#1051 binary (before
///      2026-08-09) and has never been reopened since — any reopen on a
///      #1051-or-later binary already recovers and rewrites this key in the
///      current shape. Bytes in that shape now fail closed with
///      `DbError::Codec` (they are structurally too short to parse as tier
///      2's tuple shape for any non-trivial entry count, so this is a clean
///      error, not a silent misparse) rather than being silently
///      resurrected with a synthesized empty name / `op_id: None`. See
///      `p1051_old_format_index2_drop_tombstone_now_errors` for the
///      before/after.
fn decode_dropping_index2(
    bytes: &[u8],
) -> Result<Vec<(u32, String, Option<String>)>, shamir_storage::error::DbError> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    if bytes[0] == INDEX2_DROP_TOMBSTONE_VERSION {
        return strict_decode_dropping_entries(&bytes[1..]).map_err(|e| {
            shamir_storage::error::DbError::Codec(format!(
                "system:_m.idx.drop: versioned ({INDEX2_DROP_TOMBSTONE_VERSION:#04x}) \
                 payload decode failed: {e}"
            ))
        });
    }
    // Legacy tier: pre-#1204 unversioned Vec<(u32, String, Option<String>)>.
    strict_decode_dropping_entries(bytes).map_err(|e| {
        shamir_storage::error::DbError::Codec(format!(
            "system:_m.idx.drop: first byte {:#04x} is not the current version \
             ({INDEX2_DROP_TOMBSTONE_VERSION:#04x}) and the blob does not decode as the \
             legacy unversioned Vec<(u32,String,Option<String>)> shape either \
             (genuine corruption, an unsupported future format, or a pre-#1051 \
             bare Vec<u32> blob — the latter is no longer supported as of #1204): {e}",
            bytes[0]
        ))
    })
}

/// System key for the index2 DROP tombstone (`Vec<(u32, String, Option<String>)>` of id + name + op_id).
fn meta_key_indexes_drop() -> RecordId {
    RecordId::system("_m.idx.drop")
}

/// Load the persisted index2 DROP tombstone. Returns an empty `Vec` if the
/// key is absent (`NotFound`) or contains an empty vec — both mean "no
/// in-progress drops". Mirrors #972's `load_dropping_sorted`.
///
/// See [`decode_dropping_index2`] for the #1204 version-byte decode
/// strategy and the explicit backward-compat decision it documents.
pub async fn load_dropping_index2(
    info_store: &Arc<dyn Store>,
) -> Result<Vec<(u32, String, Option<String>)>, shamir_storage::error::DbError> {
    let key = meta_key_indexes_drop().to_bytes();
    match info_store.get(key.into()).await {
        Ok(bytes) => decode_dropping_index2(&bytes),
        Err(shamir_storage::error::DbError::NotFound(_)) => Ok(Vec::new()),
        Err(e) => Err(e),
    }
}

/// Add `id` to the persisted index2 DROP tombstone. Loads the current set,
/// appends the entry, and writes it back. MUST be called BEFORE the sweep in
/// `drop_index2` so a crash at any subsequent point is recoverable.
///
/// If the persist fails, the on-disk tombstone is unchanged — the caller
/// (`drop_index2`) propagates the error and does NOT proceed with the drop.
/// Mirrors #972's `add_to_dropping_sorted` (adapted for the free-function
/// shape — `IndexRegistry` does not own an `info_store`).
///
/// #1051: accepts `name` and `op_id` minted at dispatch time. `op_id = None` means
/// the caller does not have an op_id (e.g., a non-DDL path), and the tombstone entry
/// will have `op_id = None` (backward compatible with pre-#1051 format).
///
/// #1204: writes the version-tagged envelope (see [`encode_dropping_index2`]).
pub async fn add_to_dropping_index2(
    id: u32,
    name: String,
    op_id: Option<String>,
    info_store: &Arc<dyn Store>,
) -> Result<(), shamir_storage::error::DbError> {
    let mut current = load_dropping_index2(info_store).await?;
    if !current.iter().any(|&(existing_id, _, _)| existing_id == id) {
        current.push((id, name, op_id));
    }
    let key = meta_key_indexes_drop().to_bytes();
    let bytes = encode_dropping_index2(&current)?;
    info_store.set(key.into(), bytes).await?;
    Ok(())
}

/// Remove `id` from the persisted index2 DROP tombstone. Loads the current
/// set, removes the id, and writes it back. MUST be called AFTER
/// `save_index2_metadata` (the reduced metadata must be durable first).
/// Mirrors #972's `clear_from_dropping_sorted`.
///
/// #1204: writes the version-tagged envelope (see [`encode_dropping_index2`]),
/// same as [`add_to_dropping_index2`] — both write paths (and the
/// recovery-clear loop in `shamir-engine`'s `recover_index2_drops`, which
/// now calls this fn directly instead of hand-rolling a raw write) must
/// agree on the wire format.
pub async fn clear_from_dropping_index2(
    id: u32,
    info_store: &Arc<dyn Store>,
) -> Result<(), shamir_storage::error::DbError> {
    let mut current = load_dropping_index2(info_store).await?;
    current.retain(|&(existing_id, _, _)| existing_id != id);
    let key = meta_key_indexes_drop().to_bytes();
    let bytes = encode_dropping_index2(&current)?;
    info_store.set(key.into(), bytes).await?;
    Ok(())
}

// ============================================================================
// #1066: RENAME INDEX durable tombstone + crash recovery (index2)
// ============================================================================
//
// Mirrors the DROP tombstone triple immediately above
// (`meta_key_indexes_drop` / `load_dropping_index2` / `add_to_dropping_index2`
// / `clear_from_dropping_index2`), adapted for RENAME instead of DROP.
// `rename_entry` (mutates the live `IndexRegistry` in memory) and
// `save_index2_metadata` (persists it) are two sequential, non-atomic steps
// with no rollback between them — a storage error or task cancellation in
// that window leaves the runtime registry showing the NEW name while disk
// still has the OLD one. This tombstone closes that window the same way the
// DROP tombstone closes drop_index2's sweep→persist window.
//
// Tombstone shape: `Vec<(u32, String, String, Option<String>)>` (descriptor
// id, old name, new name, op_id) serialized via bincode under a dedicated
// key, so it never touches `save_index2_metadata`/`load_index2_metadata`'s
// forward-compat decode chain.
//
// This is a BRAND NEW key as of #1066 — there is no legacy on-disk format to
// fall back to (unlike the DROP tombstone's `Vec<u32>` pre-#1051 fallback),
// so `load_renaming_index2` decodes directly with no shadow-shape branch.
//
// **CRITICAL** (the #959 collision lesson, re-verified here):
// `RecordId::system(name)` truncates `name` to 12 bytes. The key
// `"_m.idx.ren"` (10 bytes, no truncation) is distinct from the three
// existing index2 system keys at byte 7:
//   `"_m.idx"`      → [..., 5f,6d,2e,69,64,78, 00,00,00,00,00,00]
//   `"_m.idx.lfv"`  → [..., 5f,6d,2e,69,64,78, 2e,6c,66,76,00,00]
//   `"_m.idx.drop"` → [..., 5f,6d,2e,69,64,78, 2e,64,72,6f,70,00]
//   `"_m.idx.ren"`  → [..., 5f,6d,2e,69,64,78, 2e,72,65,6e,00,00]
// All four share the first 6 non-zero bytes (`_m.idx`) but diverge at byte 6
// (0x00 for the bare key vs 0x2e for the suffixed ones) and, among the
// suffixed keys, at byte 7 (0x6c '.lfv' vs 0x64 '.drop' vs 0x72 '.ren') —
// NO collision.

/// System key for the index2 RENAME tombstone
/// (`Vec<(u32, String, String, Option<String>)>` of id + old_name + new_name
/// + op_id).
fn meta_key_indexes_rename() -> RecordId {
    RecordId::system("_m.idx.ren")
}

/// Load the persisted index2 RENAME tombstone. Returns an empty `Vec` if the
/// key is absent (`NotFound`) or contains an empty vec — both mean "no
/// in-progress renames". Mirrors `load_dropping_index2`, but this key is new
/// as of #1066 — there is no legacy format to fall back to, so a decode
/// failure is a genuine error rather than a fallback trigger.
pub async fn load_renaming_index2(
    info_store: &Arc<dyn Store>,
) -> Result<Vec<(u32, String, String, Option<String>)>, shamir_storage::error::DbError> {
    let key = meta_key_indexes_rename().to_bytes();
    match info_store.get(key.into()).await {
        Ok(bytes) => {
            if bytes.is_empty() {
                return Ok(Vec::new());
            }
            bincode::deserialize::<Vec<(u32, String, String, Option<String>)>>(&bytes).map_err(
                |e| {
                    shamir_storage::error::DbError::Codec(format!(
                        "system:_m.idx.ren decode failed: {e}"
                    ))
                },
            )
        }
        Err(shamir_storage::error::DbError::NotFound(_)) => Ok(Vec::new()),
        Err(e) => Err(e),
    }
}

/// Add `id` to the persisted index2 RENAME tombstone. Loads the current set,
/// appends the entry (skipping if `id` is already present, same de-dup guard
/// as `add_to_dropping_index2`), and writes it back.
///
/// MUST be called BEFORE `rename_entry` mutates the live registry, so a
/// crash/error at any subsequent point is recoverable from the tombstone.
///
/// If the persist fails, the on-disk tombstone is unchanged — the caller
/// (`rename_index`) propagates the error and does NOT proceed with the
/// rename. Mirrors `add_to_dropping_index2`'s free-function shape —
/// `IndexRegistry` does not own an `info_store`.
pub async fn add_to_renaming_index2(
    id: u32,
    old_name: String,
    new_name: String,
    op_id: Option<String>,
    info_store: &Arc<dyn Store>,
) -> Result<(), shamir_storage::error::DbError> {
    let mut current = load_renaming_index2(info_store).await?;
    if !current.iter().any(|&(existing_id, ..)| existing_id == id) {
        current.push((id, old_name, new_name, op_id));
    }
    let key = meta_key_indexes_rename().to_bytes();
    let bytes = bincode::serialize(&current)
        .map_err(|e| shamir_storage::error::DbError::Codec(e.to_string()))?;
    info_store.set(key.into(), Bytes::from(bytes)).await?;
    Ok(())
}

/// Remove `id` from the persisted index2 RENAME tombstone. Loads the current
/// set, removes the id, and writes it back.
///
/// MUST be called AFTER `save_index2_metadata` durably persists the renamed
/// registry. Mirrors `clear_from_dropping_index2`.
pub async fn clear_from_renaming_index2(
    id: u32,
    info_store: &Arc<dyn Store>,
) -> Result<(), shamir_storage::error::DbError> {
    let mut current = load_renaming_index2(info_store).await?;
    current.retain(|&(existing_id, ..)| existing_id != id);
    let key = meta_key_indexes_rename().to_bytes();
    let bytes = bincode::serialize(&current)
        .map_err(|e| shamir_storage::error::DbError::Codec(e.to_string()))?;
    info_store.set(key.into(), Bytes::from(bytes)).await?;
    Ok(())
}

/// Sweep all posting entries for the given index2 descriptor id from
/// `info_store`. Postings are keyed by `[index_id: u32 LE][type_tag]...`
/// (see `posting_layout`), so a 4-byte prefix scan collects every entry.
/// Removing already-removed keys is a no-op on every `Store` backend, so
/// this is safe to call idempotently. Extracted as a free function so the
/// recovery path can re-run it without holding a backend `Arc`.
///
/// Note: `VectorBackend::drop_all` is currently a no-op (the HNSW graph
/// lives in memory and dies with the process); this sweep finds no
/// posting-key entries for vector ids, which is consistent with that.
/// Mirrors #972's `sweep_sorted_postings`.
pub async fn sweep_index2_postings_by_id(
    id: u32,
    info_store: &Arc<dyn Store>,
) -> Result<(), shamir_storage::error::DbError> {
    let prefix = Bytes::copy_from_slice(&id.to_le_bytes());
    let stream = info_store.scan_prefix_stream(prefix, MAINT_SCAN_BATCH);
    futures::pin_mut!(stream);
    let mut to_drop: Vec<RecordKey> = Vec::new();
    while let Some(batch) = stream.next().await {
        for (k, _) in batch? {
            to_drop.push(k);
        }
    }
    if !to_drop.is_empty() {
        let _ = info_store.remove_many(to_drop).await?;
    }
    Ok(())
}

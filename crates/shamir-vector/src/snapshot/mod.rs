//! Snapshot codec for the persisted HNSW graph (P2 — V2.1).
//!
//! This module gives `HnswAdapter` an isolated, unit-testable dump/load
//! round-trip against any `Store`. It is the FIRST P2 slice: ONLY the codec —
//! startup integration (#401) and the incremental delta-log / generation flip
//! (#402) are out of scope and land in later sheets.
//!
//! ## Layout in the info_store
//!
//! All keys live under the caller-supplied `keyspace` (e.g. `__info__<table>`
//! or a dedicated vector-keyspace); the codec prefixes every record it writes
//! with `<keyspace>.`. Three kinds of records are emitted for generation `N`:
//!
//! * **Chunks** — the `hnsw_rs` dump files (`.hnsw.graph`, `.hnsw.data`) sliced
//!   into ~1 MiB pieces, written as raw bytes under
//!   `<keyspace>.g<N>.graph.KKKK` / `<keyspace>.g<N>.data.KKKK` where `KKKK`
//!   is a 6-digit zero-padded chunk index. Each chunk carries its own crc32 in
//!   the [`ChunkHeader`] (checksums-everywhere pillar): a bit-flip in any
//!   chunk surfaces as [`SnapshotError::Corrupt`] at load, not as a silent
//!   graph corruption.
//! * **Sidecar** — `<keyspace>.g<N>.sidecar` (MetaEnvelope-wrapped, bincode):
//!   everything the graph dump itself does NOT carry — the adapter's maps
//!   (`rid_map`, `rid_to_internal`, `tombstones`, `vectors`), the build
//!   parameters, and the cross-section crc32. `RESERVED` fields
//!   (`quantization`) are `Option`s left `None` until P5.
//! * **Manifest** — `<keyspace>.manifest` (MetaEnvelope-wrapped): the single
//!   source of truth for "which generation is live". Points at `gen N`, the
//!   chunk counts, and the basename `hnsw_rs::file_dump` actually used (it can
//!   be uniquified by the loader when a name collision is detected, so we MUST
//!   record the returned name — see `DumpInit::get_basename`).
//!
//! ## Lifetime: how `Hnsw<'static>` is produced from a load
//!
//! `hnsw_rs::HnswIo::load_hnsw_with_dist<'b, 'a>` ties `'a: 'b` where `'a` is
//! the borrow of the `HnswIo` loader. To hand the reloaded graph to long-lived
//! storage as `Arc<Hnsw<'static, ...>>` (the shape `HnswAdapter` already uses),
//! the loader itself must outlive every reference the graph hands out — i.e.
//! it must be `'static`. `Box::leak(Box::new(HnswIo))` is the sanctioned
//! boot-only pattern (mirrors the V0.0 contract-test
//! `leaked_loader_yields_static_hnsw`): the loader is tiny and lives for the
//! process; the dump files are the durable artefact. A leaked `HnswIo` per
//! snapshot is acceptable ONLY because snapshots are loaded once at boot (a
//! handful per shard). It is NOT a per-request pattern.
//!
//! ## Pillars honoured
//!
//! * `spawn_blocking` for every CPU/IO-bound step (`file_dump`, file read,
//!   file write on load) — never block the async executor on disk I/O.
//! * `Store::transact` for the atomic chunk + sidecar + manifest write — the
//!   dump is observable as a single all-or-nothing batch on backends that
//!   override `transact`, and as per-op sequential on the default impl.
//! * crc32 on every chunk + a cross-section crc32 in the sidecar
//!   (checksums-everywhere).

mod codec;
mod legacy;
mod load;
mod manifest;
mod quantized_load;
#[cfg(test)]
mod tests;

#[cfg(test)]
pub(crate) use codec::chunk_key_for_test;
pub use codec::dump_snapshot;
pub use codec::dump_snapshot_with_gen;
#[cfg(test)]
pub(crate) use codec::manifest_key_for_test;
#[cfg(test)]
pub(crate) use codec::sidecar_key_for_test;
pub use codec::SnapshotError;
pub use codec::SnapshotManifest;
pub use codec::SnapshotSidecar;
#[cfg(test)]
pub(crate) use codec::HNSW_RS_VERSION;
#[cfg(test)]
pub(crate) use codec::SNAPSHOT_FORMAT_VERSION;
pub use load::append_delta;
pub use load::flip_generation;
pub use load::highest_delta_index;
pub use load::load_snapshot;
pub(crate) use load::load_snapshot_with_intent;
pub(crate) use load::publish_manifest_without_prune;
pub use load::read_manifest;
pub use load::replay_delta;
pub use load::DeltaOp;
pub use manifest::OldGenerationChunks;

// Re-export QuantMeta so callers (and tests) can reach it as
// `crate::snapshot::QuantMeta` — the snapshot codec owns the
// quantization-on-wire contract.
pub use crate::quant_meta::QuantMeta;

pub(crate) use codec::QUANT_METHOD_SQ8;

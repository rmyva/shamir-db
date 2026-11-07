//! Load path (manifest → chunks → sidecar → `HnswIo`) and the V2.3
//! delta-log (append / replay / generation flip) of the snapshot codec.

use crate::adapter::VectorAdapter;
use crate::hnsw_adapter::{HnswAdapter, ShamirDist};
use crate::quantized_dist::ShamirDistU8;
use crate::sq8::Sq8Quantizer;
use bytes::Bytes;
use futures::StreamExt;
use hnsw_rs::hnsw::Hnsw;
use hnsw_rs::hnswio::HnswIo;
use serde::{Deserialize, Serialize};
use shamir_index_core::meta_envelope::MetaEnvelope;
use shamir_storage::types::{KvOp, RecordKey, Store};
use shamir_tunables::store_defaults::MAINT_SCAN_BATCH;
use shamir_types::types::common::THasher;
use shamir_types::types::record_id::RecordId;
use std::fs::File;
use std::io::Write;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;

use super::codec::{
    chunk_key, manifest_key, map_meta_err, sidecar_key, ChunkHeader, HNSW_RS_VERSION,
};
use super::legacy::{decode_manifest, decode_sidecar};
use super::manifest::{integrity_bytes, integrity_key, validate_basename, verify_integrity};
use super::quantized_load::load_quantized_graph;
use super::{OldGenerationChunks, QuantMeta, SnapshotError, SnapshotManifest};

#[cfg(test)]
#[path = "tests/replay_tests.rs"]
mod replay_tests;

const REPLAY_BATCH_LIMIT: usize = 128;

// ============================================================================
// Load path
// ============================================================================

/// Read + decode the manifest for `keyspace`. Returns `NotFound` when no
/// snapshot exists (the manifest key is absent). Used by the V2.3 delta-
/// replay path (`restore_on_open` reads the manifest to learn
/// `delta_applied_upto` BEFORE loading the snapshot) and by the background
/// snapshot flip (`run_background_snapshot` reads the current gen + chunk
/// counts to drive the prune).
///
/// V5.3 (#412): accepts both v1 and v2 manifests (see
/// [`super::codec::SNAPSHOT_SUPPORTED_VERSIONS`]). A v3+ manifest is refused with
/// `VersionMismatch`.
pub async fn read_manifest(
    store: &Arc<dyn Store>,
    keyspace: &str,
) -> Result<SnapshotManifest, SnapshotError> {
    let (manifest, _) = read_metadata(store, keyspace).await?;
    Ok(manifest)
}

// Keep the verified metadata bytes together; never re-fetch a sidecar after its CRC.
// Legacy snapshots may lack an integrity companion; still reject malformed sidecars.
async fn read_metadata(
    store: &Arc<dyn Store>,
    keyspace: &str,
) -> Result<(SnapshotManifest, super::SnapshotSidecar), SnapshotError> {
    let manifest_bytes = store.get(manifest_key(keyspace)).await?;
    let manifest = decode_manifest(&manifest_bytes)?;
    let sidecar = store
        .get(sidecar_key(keyspace, manifest.gen))
        .await
        .map_err(|error| match error {
            shamir_storage::error::DbError::NotFound(_) => {
                SnapshotError::Corrupt("missing snapshot sidecar".into())
            }
            other => other.into(),
        })?;
    match store.get(integrity_key(keyspace)).await {
        Ok(integrity) => verify_integrity(&integrity, &manifest_bytes, &sidecar)?,
        Err(shamir_storage::error::DbError::NotFound(_)) => {}
        Err(error) => return Err(error.into()),
    }
    let sidecar_value = decode_sidecar(&sidecar)?;
    Ok((manifest, sidecar_value))
}

/// Load the snapshot stored under `keyspace` and rebuild a working
/// `HnswAdapter`. Reads manifest → chunks (with per-chunk crc verify) →
/// sidecar (with cross-section crc verify) → temp files → `HnswIo` →
/// `Box::leak` for loader lifetime → `Hnsw<'static>` → adapter.
///
/// V5.3 (#412): for a quantized v2 snapshot (`sidecar.quantization.is_some()`
/// AND `manifest.qgraph_chunks > 0`), this ALSO:
///  * decodes the `QuantMeta` from `sidecar.quantization` and reconstructs
///    the [`Sq8Quantizer`];
///  * fetches + verifies the `qgraph`/`qdata` chunks, writes them to temp
///    files, and reloads the u8 graph via `HnswIo::load_hnsw_with_dist` with
///    a fresh [`ShamirDistU8`] built from the reconstructed quantizer;
///  * assembles the adapter via [`HnswAdapter::from_parts_with_quantization`]
///    so `is_fitted == true`, the u8 graph + quantizer + `vectors_u8` are
///    all live, and post-load search goes the quantized path.
///
/// The v1 and v2 sidecar layouts decode, but graph loading requires the
/// supported `hnsw_rs` version. Version 0.3.4 is rejected with
/// `VersionMismatch`; the backend can rebuild from source instead.
pub async fn load_snapshot(
    store: &Arc<dyn Store>,
    keyspace: &str,
) -> Result<HnswAdapter, SnapshotError> {
    load_snapshot_with_intent(store, keyspace, None).await
}

/// Load a snapshot while preserving the descriptor's quantization intent.
pub(crate) async fn load_snapshot_with_intent(
    store: &Arc<dyn Store>,
    keyspace: &str,
    quantization: Option<shamir_index_core::kind::VectorQuantization>,
) -> Result<HnswAdapter, SnapshotError> {
    // ---- 1. manifest -----------------------------------------------------
    let (manifest, sidecar) = read_metadata(store, keyspace).await?;
    let gen = manifest.gen;

    // ---- 2. sidecar ------------------------------------------------------
    let dim = sidecar.dim as usize;
    if dim == 0
        || sidecar.m == 0
        || sidecar.max_layer == 0
        || sidecar.max_layer > 16
        || sidecar.ef_construction == 0
        || sidecar.vectors.iter().any(|(_, v)| v.len() != dim)
        || sidecar.vectors_u8.iter().any(|(_, v)| v.len() != dim)
    {
        return Err(SnapshotError::Corrupt(
            "invalid dimensions or HNSW parameters".into(),
        ));
    }
    let quantized = sidecar.quantization.is_some();
    if quantized != (manifest.qgraph_chunks > 0 && manifest.qdata_chunks > 0)
        || (!quantized && (manifest.qgraph_chunks != 0 || manifest.qdata_chunks != 0))
    {
        return Err(SnapshotError::Corrupt(
            "inconsistent quantized graph sections".into(),
        ));
    }
    let used_basename = if quantized {
        &manifest.qbasename
    } else {
        &manifest.basename
    };
    validate_basename(used_basename)?;

    // ---- 3. stream + verify section files -------------------------------
    let load_dir = tempfile::tempdir()?;
    let f32_basename = if quantized {
        "shamir"
    } else {
        &manifest.basename
    };
    validate_basename(f32_basename)?;
    let graph_path = load_dir.path().join(format!("{f32_basename}.hnsw.graph"));
    let data_path = load_dir.path().join(format!("{f32_basename}.hnsw.data"));
    let hasher = crc32fast::Hasher::new();
    let (graph_len, hasher) = stream_section_to_file(
        store,
        (keyspace, gen, "graph", manifest.graph_chunks),
        &graph_path,
        hasher,
    )
    .await?;
    let (data_len, hasher) = stream_section_to_file(
        store,
        (keyspace, gen, "data", manifest.data_chunks),
        &data_path,
        hasher,
    )
    .await?;
    verify_section_len("graph", graph_len, sidecar.graph_len)?;
    verify_section_len("data", data_len, sidecar.data_len)?;

    let quantizer: Option<Arc<Sq8Quantizer>> = match &sidecar.quantization {
        Some(blob) => {
            let meta: QuantMeta = bincode::deserialize(blob).map_err(|error| {
                SnapshotError::Corrupt(format!("quantizer metadata decode: {error}"))
            })?;
            Some(Arc::new(
                meta.try_to_quantizer(dim)
                    .map_err(|error| SnapshotError::Corrupt(error.into()))?,
            ))
        }
        None => None,
    };

    // The qgraph directory remains owned through HnswIo loading below.
    let (qgraph_dir, qbasename, hasher) = if manifest.qgraph_chunks > 0 {
        let qbasename = &manifest.qbasename;
        validate_basename(qbasename)?;
        let qload_dir = tempfile::tempdir()?;
        let qgraph_path = qload_dir.path().join(format!("{qbasename}.hnsw.graph"));
        let qdata_path = qload_dir.path().join(format!("{qbasename}.hnsw.data"));
        let (qgraph_len, next_hasher) = stream_section_to_file(
            store,
            (keyspace, gen, "qgraph", manifest.qgraph_chunks),
            &qgraph_path,
            hasher,
        )
        .await?;
        let (qdata_len, final_hasher) = stream_section_to_file(
            store,
            (keyspace, gen, "qdata", manifest.qdata_chunks),
            &qdata_path,
            next_hasher,
        )
        .await?;
        let _ = (qgraph_len, qdata_len);
        (Some(qload_dir), Some(qbasename.clone()), final_hasher)
    } else {
        if manifest.qgraph_chunks != 0 || manifest.qdata_chunks != 0 {
            return Err(SnapshotError::Corrupt(
                "inconsistent quantized graph sections".into(),
            ));
        }
        (None, None, hasher)
    };
    if hasher.finalize() != sidecar.sections_crc32 {
        return Err(SnapshotError::Corrupt(
            "cross-section crc32 mismatch".to_string(),
        ));
    }

    // ---- 4. write temp files, Box::leak HnswIo, load f32 graph (CPU/IO) --
    //
    // #418 — for a quantized snapshot (`quantizer.is_some()`) the dump path
    // emitted ZERO-length f32 sections (the fitted adapter had dropped its
    // f32 graph post-fit). We skip the f32 file load entirely and substitute
    // a throwaway empty `Hnsw::new(...)` stub: `from_parts_with_quantization`
    // drops it immediately on assembly, so its contents are irrelevant — it
    // exists only to satisfy the function's `Arc<Hnsw<f32>>` parameter shape.
    // A non-quant snapshot always has real f32 bytes and loads as before.
    let basename = f32_basename.to_string();
    let dist = ShamirDist {
        metric: sidecar.metric,
    };
    let dim = sidecar.dim;
    let metric = sidecar.metric;
    let ef_search = sidecar.ef_search;
    let m = sidecar.m;
    let max_layer = sidecar.max_layer;
    let ef_construction = sidecar.ef_construction;
    let hnsw_rs_version = sidecar.hnsw_rs_version.clone();
    let next_id = sidecar.next_id;
    let rid_map_pairs = sidecar.rid_map;
    let rid_to_internal_pairs = sidecar.rid_to_internal;
    let tombstones = sidecar.tombstones;
    let vectors_pairs = sidecar.vectors;
    let vectors_u8_pairs = sidecar.vectors_u8;

    // Verify the dump came from a compatible `hnsw_rs` BEFORE we hand it to
    // the loader — `load_hnsw_with_dist` will panic on an unknown format.
    // `hnsw_rs` does not export its version as a const, so we compare the
    // sidecar-stamped version against our build-time pin.
    if hnsw_rs_version != HNSW_RS_VERSION {
        return Err(SnapshotError::VersionMismatch(format!(
            "dump produced by hnsw_rs {}, but this build pins {}",
            hnsw_rs_version, HNSW_RS_VERSION
        )));
    }

    let hnsw: Arc<Hnsw<'static, f32, ShamirDist>> = if quantizer.is_some() {
        // #418 — quantized snapshot: no f32 graph on disk. Build a tiny empty
        // stub (capacity 1) — `from_parts_with_quantization` drops it on
        // assembly, so the size is irrelevant; we just need the right shape.
        Arc::new(Hnsw::new(m as usize, 1, max_layer, ef_construction, dist))
    } else {
        tokio::task::spawn_blocking(move || {
            // Boot-only, intentional leak: `load_hnsw_with_dist<'b,'a>` ties
            // `'a: 'b` where `'a` is the borrow of the `HnswIo` loader. To hand
            // the reloaded graph to long-lived storage as `Arc<Hnsw<'static,...>>`
            // (the shape HnswAdapter already uses), the loader must be `'static`.
            // We leak exactly ONE `HnswIo` per snapshot load; the loader is tiny
            // (~tens of bytes) and lives for the process. The dump files are the
            // durable artefact — the leak does NOT grow across snapshots, only
            // across shard count. Mirrors the V0.0 contract test
            // `leaked_loader_yields_static_hnsw`.
            //
            // MUST STAY NON-MMAP. `HnswIo::new` defaults to `datamap: false`, so
            // `load_hnsw_with_dist` reads the graph fully INTO MEMORY — the
            // returned `Hnsw` borrows the leaked `HnswIo` struct, NOT the
            // on-disk files. That is why `load_dir` can be dropped when the
            // closure returns: the graph is already resident. Enabling mmap
            // here would require keeping the files alive too.
            let leaked_io: &'static HnswIo =
                Box::leak(Box::new(HnswIo::new(load_dir.path(), &basename)));
            let hnsw: Hnsw<'static, f32, ShamirDist> =
                catch_unwind(AssertUnwindSafe(|| leaked_io.load_hnsw_with_dist(dist)))
                    .map_err(|_| SnapshotError::Corrupt("hnsw_rs f32 parser panicked".into()))?
                    .map_err(|e| {
                        SnapshotError::Corrupt(format!("hnsw_rs f32 parser failed: {e}"))
                    })?;
            Ok::<_, SnapshotError>(Arc::new(hnsw))
        })
        .await
        .map_err(|e| SnapshotError::Io(format!("spawn_blocking join error: {e}")))??
    };

    // Compare the persisted parameters against the loaded f32 graph. The
    // quantized branch compares its u8 graph below, not the throwaway stub.
    if quantizer.is_none()
        && (hnsw.get_max_nb_connection() != m
            || hnsw.get_max_level() != max_layer
            || hnsw.get_ef_construction() != ef_construction)
    {
        return Err(SnapshotError::Corrupt(
            "graph build parameters mismatch".into(),
        ));
    }

    // ---- 4b. V5.3 (#412): load the u8 graph (if quantized) ---------------
    //
    // Decode the QuantMeta → Sq8Quantizer → ShamirDistU8, write the qgraph/
    // qdata temp files, and reload the u8 graph via a second `HnswIo`. The
    // leaked HnswIo mirrors the f32 pattern (boot-only, one per shard).
    //
    // The carrier is a tuple `(u8 graph Arc, reconstructed quantizer Arc)` —
    // both are needed to assemble the fitted adapter in step 5. We allow the
    // `type_complexity` lint because the tuple is a local carrier, not a
    // public signature.
    #[allow(clippy::type_complexity)] // local carrier tuple — see comment above
    let hnsw_u8_opt: Option<(Arc<Hnsw<'static, u8, ShamirDistU8>>, Arc<Sq8Quantizer>)> =
        if let Some(quantizer) = quantizer {
            let (qload_dir, qbasename) = (
                qgraph_dir.expect("quantized directory already created"),
                qbasename.expect("quantized basename already validated"),
            );
            let qgraph_path = qload_dir.path().join(format!("{qbasename}.hnsw.graph"));
            let qdata_path = qload_dir.path().join(format!("{qbasename}.hnsw.data"));
            let quantizer_for_load = Arc::clone(&quantizer);
            let hnsw_u8 = tokio::task::spawn_blocking(move || {
                load_quantized_graph(
                    qload_dir,
                    &qbasename,
                    qgraph_path,
                    qdata_path,
                    quantizer_for_load,
                    metric,
                )
            })
            .await
            .map_err(|e| SnapshotError::Io(format!("spawn_blocking join error: {e}")))??;
            if hnsw_u8.get_max_nb_connection() != m
                || hnsw_u8.get_max_level() != max_layer
                || hnsw_u8.get_ef_construction() != ef_construction
            {
                return Err(SnapshotError::Corrupt(
                    "quantized graph build parameters mismatch".into(),
                ));
            }
            Some((hnsw_u8, quantizer))
        } else {
            None
        };
    // ---- 5. rebuild the adapter maps -------------------------------------
    let cap = rid_map_pairs.len().max(64);
    let rid_map = scc::HashMap::with_capacity_and_hasher(cap, THasher::default());
    let rid_to_internal = scc::HashMap::with_capacity_and_hasher(cap, THasher::default());
    let vectors = scc::HashMap::with_capacity_and_hasher(cap, THasher::default());
    let deleted = scc::HashMap::with_capacity_and_hasher(cap, THasher::default());
    for (internal, rid) in rid_map_pairs {
        let _ = rid_map.insert_sync(internal, rid);
    }
    for (rid, internal) in rid_to_internal_pairs {
        let _ = rid_to_internal.insert_sync(rid, internal);
    }
    for (internal, v) in vectors_pairs {
        let _ = vectors.insert_sync(internal, v);
    }
    for internal in tombstones {
        let _ = deleted.insert_sync(internal, ());
    }

    // V5.3 (#412): if a u8 graph was loaded, assemble the fitted adapter.
    if let Some((hnsw_u8, quantizer)) = hnsw_u8_opt {
        // Rebuild the vectors_u8 map from the sidecar pairs.
        let vectors_u8 = scc::HashMap::with_capacity_and_hasher(cap, THasher::default());
        for (internal, codes) in vectors_u8_pairs {
            let _ = vectors_u8.insert_sync(internal, codes);
        }
        let adapter = HnswAdapter::from_parts_with_quantization(
            dim,
            metric,
            ef_search,
            hnsw,
            rid_map,
            rid_to_internal,
            vectors,
            deleted,
            next_id,
            hnsw_u8,
            quantizer,
            vectors_u8,
        );
        return Ok(adapter);
    }

    let adapter = HnswAdapter::from_parts(
        dim,
        metric,
        ef_search,
        hnsw,
        rid_map,
        rid_to_internal,
        vectors,
        deleted,
        next_id,
        quantization,
    );
    Ok(adapter)
}

fn verify_section_len(section: &str, actual: u64, expected: u64) -> Result<(), SnapshotError> {
    if actual != expected {
        return Err(SnapshotError::Corrupt(format!(
            "{section} length mismatch: expected {expected}, got {actual}"
        )));
    }
    Ok(())
}

async fn stream_section_to_file(
    store: &Arc<dyn Store>,
    location: (&str, u32, &'static str, u32),
    path: &std::path::Path,
    hasher: crc32fast::Hasher,
) -> Result<(u64, crc32fast::Hasher), SnapshotError> {
    let (keyspace, gen, section, count) = location;
    let mut file = tokio::task::spawn_blocking({
        let path = path.to_path_buf();
        move || File::create(path)
    })
    .await
    .map_err(|e| SnapshotError::Io(format!("spawn_blocking join error: {e}")))??;
    let mut total = 0u64;
    let mut section_hasher = hasher;
    for idx in 0..count {
        let key = chunk_key(keyspace, gen, section, idx as usize);
        let record = store
            .get_many(vec![key])
            .await?
            .into_iter()
            .next()
            .flatten()
            .ok_or_else(|| SnapshotError::Corrupt(format!("missing {section} chunk {idx}")))?;
        let file_for_chunk = file;
        let hasher_for_chunk = section_hasher.clone();
        let (next_file, next_hasher, bytes_read) = tokio::task::spawn_blocking(move || {
            let header: ChunkHeader = bincode::deserialize(&record).map_err(|error| {
                SnapshotError::Corrupt(format!("{section} chunk {idx} decode: {error}"))
            })?;
            if header.idx != idx {
                return Err(SnapshotError::Corrupt(format!(
                    "{section} chunk idx mismatch: expected {idx}, got {}",
                    header.idx
                )));
            }
            let actual = crc32fast::hash(&header.bytes);
            if actual != header.crc32 {
                return Err(SnapshotError::Corrupt(format!(
                    "{section} chunk {idx} crc32 mismatch: expected {}, got {actual}",
                    header.crc32
                )));
            }
            let mut section_hasher = hasher_for_chunk;
            section_hasher.update(&header.bytes);
            let length = header.bytes.len() as u64;
            let mut file = file_for_chunk;
            file.write_all(&header.bytes)?;
            Ok::<_, SnapshotError>((file, section_hasher, length))
        })
        .await
        .map_err(|e| SnapshotError::Io(format!("spawn_blocking join error: {e}")))??;
        file = next_file;
        section_hasher = next_hasher;
        total += bytes_read;
    }
    tokio::task::spawn_blocking(move || file.flush())
        .await
        .map_err(|e| SnapshotError::Io(format!("spawn_blocking join error: {e}")))??;
    Ok((total, section_hasher))
}

// ============================================================================
// Delta-log (V2.3 / #402)
// ============================================================================
//
// Between full snapshots, every Phase 5d promote appends a `DeltaOp` chunk to
// the info store. The chunk is a `Vec<DeltaOp>` bincode'd inside a
// `MetaEnvelope`, written under a monotonic zero-padded key
// `<keyspace>.delta.NNNNNNNNNN`. On restart, after `load_snapshot` rebuilds
// the base graph, the replay walks every chunk with index >
// `manifest.delta_applied_upto` and applies each `DeltaOp` to the freshly-
// loaded adapter. The HWM pattern mirrors `InternerManager` (last_chunk_idx
// in-memory + scan on boot); the atomic generation flip (below) prunes
// superseded chunks as part of the same `Store::transact` that publishes the
// new manifest.

/// Width of the decimal zero-padded delta-chunk index in the keyspace key.
/// 10 digits = 10^10 chunks. At one chunk per commit-Phase-5d (each carrying
/// up to a tx's worth of vector mutations) that is ~300 years of commits at
/// 1k commits/sec, well past any realistic working set; the zero-padding
/// keeps lexicographic key order == numeric order, so a prefix scan walks
/// chunks in append order without a comparator.
const DELTA_CHUNK_IDX_WIDTH: usize = 10;

/// One vector mutation captured by the delta-log. Phase 5d translates the
/// tx's `staged_vectors` slice into `Upsert` ops and (on a tx that deleted a
/// vector-backed row) emits `Delete`. P0 delta-prune (brief 164) — ordering
/// correction: the chunk is appended BEFORE the graph apply (the engine's
/// delta half runs pre-publish; the HNSW promote runs post-lock), so a chunk
/// may exist while its mutation has NOT yet reached any dumped graph. The
/// snapshot prune boundary is therefore the backend's contiguous APPLIED
/// prefix (`IndexBackend::mark_delta_applied`), never the allocation
/// counter: a snapshot that pruned by allocation order could drop a chunk
/// whose vector never made it into the dumped graph and lose it on restart.
/// A crash AFTER the chunk append but BEFORE the next snapshot sees the
/// mutation replayed on restart.
#[derive(Clone, Serialize, Deserialize)]
pub enum DeltaOp {
    /// Insert or replace `rid`'s vector.
    Upsert(RecordId, Vec<f32>),
    /// Soft-delete `rid`.
    Delete(RecordId),
}

/// Build the keyspace key for delta chunk `idx`. `<keyspace>.delta.NNNNNNNNNN`.
fn delta_chunk_key(keyspace: &str, idx: u64) -> RecordKey {
    Bytes::from(format!(
        "{ks}.delta.{idx:0width$}",
        ks = keyspace,
        idx = idx,
        width = DELTA_CHUNK_IDX_WIDTH,
    ))
    .into()
}

/// Prefix used by `scan_prefix_stream` to enumerate every delta chunk in a
/// keyspace (across all generations — the manifest's `delta_applied_upto`
/// filters which ones to apply). `<keyspace>.delta.`
fn delta_scan_prefix(keyspace: &str) -> RecordKey {
    Bytes::from(format!("{ks}.delta.", ks = keyspace)).into()
}

/// Append a delta chunk (`Vec<DeltaOp>`) to the info store under `keyspace`
/// at index `idx`. The caller (the snapshot coordinator inside
/// `VectorBackend`) owns the monotonic `idx` — typically the in-memory HWM
/// counter. The chunk is a single `Store::set` (one memtable insert on every
/// backend we ship): cheap enough to run synchronously inside commit Phase 5d
/// without stalling the ack (§5.6 — see the rationale on
/// `apply_staged_vectors`).
pub async fn append_delta(
    store: &Arc<dyn Store>,
    keyspace: &str,
    idx: u64,
    ops: &[DeltaOp],
) -> Result<(), SnapshotError> {
    if ops.is_empty() {
        // An empty promote (no vector rows in this tx) writes nothing. This
        // keeps the chunk stream dense — every chunk corresponds to a real
        // mutation — and lets the replay short-circuit on an absent chunk
        // without a "decode empty envelope" special case.
        return Ok(());
    }
    let env = MetaEnvelope::new(ops.to_vec());
    let bytes = env
        .encode()
        .map_err(|e| SnapshotError::Serde(e.to_string()))?;
    store
        .set(delta_chunk_key(keyspace, idx), Bytes::from(bytes))
        .await?;
    Ok(())
}

/// Decode a delta chunk's bytes into its `Vec<DeltaOp>`. Shared by the
/// restart-replay path and the tests.
fn decode_delta_chunk(bytes: &[u8]) -> Result<Vec<DeltaOp>, SnapshotError> {
    MetaEnvelope::<Vec<DeltaOp>>::open(bytes).map_err(map_meta_err)
}

/// Replay every delta chunk with index greater than or equal to
/// `delta_applied_upto` against the live `adapter`. Used by
/// `VectorBackend::restore_on_open` after a successful `load_snapshot`: the
/// base graph reflects the snapshot's generation, and the delta chunks carry
/// every mutation committed since that snapshot was taken.
///
/// Walks the keyspace via `scan_prefix_stream` (lexicographic key order ==
/// numeric chunk order thanks to the zero-padding). Contiguous upserts are
/// dispatched in bounded batches; deletes flush the preceding batch so that
/// operations on a repeated record remain ordered. A chunk with index below
/// `delta_applied_upto` was already absorbed into the snapshot.
pub async fn replay_delta(
    store: &Arc<dyn Store>,
    keyspace: &str,
    delta_applied_upto: u64,
    adapter: &HnswAdapter,
) -> Result<u64, SnapshotError> {
    replay_delta_with_adapter(store, keyspace, delta_applied_upto, adapter).await
}

async fn replay_delta_with_adapter(
    store: &Arc<dyn Store>,
    keyspace: &str,
    delta_applied_upto: u64,
    adapter: &dyn VectorAdapter,
) -> Result<u64, SnapshotError> {
    let prefix = delta_scan_prefix(keyspace);
    let mut stream = store.scan_prefix_stream(prefix.into(), MAINT_SCAN_BATCH);
    let mut highest_seen: u64 = delta_applied_upto;
    while let Some(batch_res) = stream.next().await {
        let batch: Vec<(RecordKey, Bytes)> = batch_res?;
        for (key, val) in batch {
            // Parse the trailing decimal index off the key. The key layout
            // is `<keyspace>.delta.NNNNNNNNNN`; the chunk index is the
            // suffix after the LAST `.`. A malformed key (no trailing
            // decimal) is skipped — it cannot have been written by
            // `append_delta`, so it is not ours.
            let key_bytes: &[u8] = key.as_ref();
            let key_str = std::str::from_utf8(key_bytes)
                .map_err(|e| SnapshotError::Corrupt(format!("delta key not utf8: {e}")))?;
            let idx_str = key_str.rsplit('.').next().ok_or_else(|| {
                SnapshotError::Corrupt(format!("delta key missing index: {key_str}"))
            })?;
            let idx: u64 = idx_str
                .parse()
                .map_err(|e| SnapshotError::Corrupt(format!("delta key index parse: {e}")))?;
            if idx < delta_applied_upto {
                // Already absorbed into the snapshot's base graph. We use
                // `<` (strictly-less-than) because `delta_applied_upto`
                // counts the number of absorbed chunks: a value of N means
                // chunks 0..N (indices 0..N-1) were absorbed, so chunk N is
                // the first to replay.
                continue;
            }
            let ops = decode_delta_chunk(&val)?;
            let mut upserts = Vec::with_capacity(REPLAY_BATCH_LIMIT);
            for op in ops {
                match op {
                    DeltaOp::Upsert(rid, vec) => {
                        upserts.push((rid, vec));
                        if upserts.len() == REPLAY_BATCH_LIMIT {
                            flush_replay_upserts(adapter, &mut upserts).await?;
                        }
                    }
                    DeltaOp::Delete(rid) => {
                        flush_replay_upserts(adapter, &mut upserts).await?;
                        adapter
                            .delete(rid)
                            .await
                            .map_err(|e| SnapshotError::Backend(format!("delta delete: {e}")))?;
                    }
                }
            }
            flush_replay_upserts(adapter, &mut upserts).await?;
            if idx > highest_seen {
                highest_seen = idx;
            }
        }
    }
    Ok(highest_seen)
}

async fn flush_replay_upserts(
    adapter: &dyn VectorAdapter,
    upserts: &mut Vec<(RecordId, Vec<f32>)>,
) -> Result<(), SnapshotError> {
    if !upserts.is_empty() {
        adapter
            .upsert_batch(upserts)
            .await
            .map_err(|e| SnapshotError::Backend(format!("delta upsert: {e}")))?;
        upserts.clear();
    }
    Ok(())
}

/// Publish the new metadata first, then prune old chunks and absorbed deltas in bounded batches.
///
/// A failure while pruning leaves the newly published snapshot valid; stale keys are harmless.
pub async fn flip_generation(
    store: &Arc<dyn Store>,
    keyspace: &str,
    old_gen: u32,
    old_chunks: OldGenerationChunks,
    new_manifest: SnapshotManifest,
    new_delta_applied_upto: u64,
) -> Result<(), SnapshotError> {
    // Publish the new metadata before pruning so a prune failure cannot undo the flip.
    publish_manifest_without_prune(store, keyspace, new_manifest).await?;

    let mut ops = Vec::with_capacity(MAINT_SCAN_BATCH);
    for (section, count) in [
        ("graph", old_chunks.graph),
        ("data", old_chunks.data),
        ("qgraph", old_chunks.qgraph),
        ("qdata", old_chunks.qdata),
    ] {
        for idx in 0..count as usize {
            ops.push(KvOp::Remove(chunk_key(keyspace, old_gen, section, idx)));
            if ops.len() == MAINT_SCAN_BATCH {
                store.transact(std::mem::take(&mut ops)).await?;
            }
        }
    }
    ops.push(KvOp::Remove(sidecar_key(keyspace, old_gen)));
    for idx in 0..new_delta_applied_upto {
        ops.push(KvOp::Remove(delta_chunk_key(keyspace, idx)));
        if ops.len() == MAINT_SCAN_BATCH {
            store.transact(std::mem::take(&mut ops)).await?;
        }
    }
    if !ops.is_empty() {
        store.transact(ops).await?;
    }
    Ok(())
}

/// Publish an absorption boundary and its companion without deleting any keys.
pub(crate) async fn publish_manifest_without_prune(
    store: &Arc<dyn Store>,
    keyspace: &str,
    manifest: SnapshotManifest,
) -> Result<(), SnapshotError> {
    let sidecar = store.get(sidecar_key(keyspace, manifest.gen)).await?;
    let manifest_bytes = MetaEnvelope::new(manifest)
        .encode()
        .map_err(|e| SnapshotError::Serde(e.to_string()))?;
    store
        .transact(vec![
            KvOp::Set(
                integrity_key(keyspace),
                integrity_bytes(&manifest_bytes, &sidecar),
            ),
            KvOp::Set(manifest_key(keyspace), Bytes::from(manifest_bytes)),
        ])
        .await?;
    Ok(())
}

/// Scan the keyspace for the highest delta-chunk index currently present.
/// Used by `restore_on_open` to seed the in-memory HWM after a restart so
/// the next `append_delta` does not collide with an existing chunk. Returns
/// `0` when no delta chunks exist.
pub async fn highest_delta_index(
    store: &Arc<dyn Store>,
    keyspace: &str,
) -> Result<u64, SnapshotError> {
    let prefix = delta_scan_prefix(keyspace);
    let mut stream = store.scan_prefix_stream(prefix.into(), MAINT_SCAN_BATCH);
    let mut highest: u64 = 0;
    while let Some(batch_res) = stream.next().await {
        let batch: Vec<(RecordKey, Bytes)> = batch_res?;
        for (key, _val) in batch {
            let key_bytes: &[u8] = key.as_ref();
            let key_str = match std::str::from_utf8(key_bytes) {
                Ok(s) => s,
                Err(_) => continue,
            };
            if let Some(idx_str) = key_str.rsplit('.').next() {
                if let Ok(idx) = idx_str.parse::<u64>() {
                    if idx > highest {
                        highest = idx;
                    }
                }
            }
        }
    }
    Ok(highest)
}

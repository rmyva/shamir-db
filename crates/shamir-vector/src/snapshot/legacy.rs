//! Schema-derived v1 metadata layouts and validated inner-version dispatch.
//! Graph loading additionally requires the supported hnsw_rs version; 0.3.4
//! is rejected with VersionMismatch and the backend rebuilds from source.
//!
//! Layouts mirror `3ed3069b^:crates/shamir-index/src/vector/snapshot.rs`.

use serde::{Deserialize, Serialize};
use shamir_index_core::kind::VectorMetric;
use shamir_index_core::meta_envelope::MetaEnvelope;
use shamir_types::types::record_id::RecordId;

use super::codec::{map_meta_err, SNAPSHOT_SUPPORTED_VERSIONS};
use super::{SnapshotError, SnapshotManifest, SnapshotSidecar};

#[derive(Serialize, Deserialize)]
struct VersionProbe {
    format_version: u16,
}

#[derive(Serialize, Deserialize)]
struct ManifestV1 {
    format_version: u16,
    gen: u32,
    graph_chunks: u32,
    data_chunks: u32,
    basename: String,
    delta_applied_upto: u64,
}

#[derive(Serialize, Deserialize)]
struct SidecarV1 {
    format_version: u16,
    dim: u32,
    metric: VectorMetric,
    ef_search: usize,
    m: u8,
    max_layer: usize,
    ef_construction: usize,
    next_id: usize,
    rid_map: Vec<(usize, RecordId)>,
    rid_to_internal: Vec<(RecordId, usize)>,
    tombstones: Vec<usize>,
    vectors: Vec<(usize, Vec<f32>)>,
    hnsw_rs_version: String,
    quantization: Option<Vec<u8>>,
    sections_crc32: u32,
    graph_len: u64,
    data_len: u64,
}

fn version(bytes: &[u8]) -> Result<u16, SnapshotError> {
    let probe: VersionProbe = MetaEnvelope::open(bytes).map_err(map_meta_err)?;
    if !SNAPSHOT_SUPPORTED_VERSIONS.contains(&probe.format_version) {
        return Err(SnapshotError::VersionMismatch(format!(
            "snapshot format version {} not in supported set {:?}",
            probe.format_version, SNAPSHOT_SUPPORTED_VERSIONS
        )));
    }
    Ok(probe.format_version)
}

pub(super) fn decode_manifest(bytes: &[u8]) -> Result<SnapshotManifest, SnapshotError> {
    match version(bytes)? {
        1 => {
            let old: ManifestV1 = MetaEnvelope::open(bytes).map_err(map_meta_err)?;
            Ok(SnapshotManifest {
                format_version: old.format_version,
                gen: old.gen,
                graph_chunks: old.graph_chunks,
                data_chunks: old.data_chunks,
                qgraph_chunks: 0,
                qdata_chunks: 0,
                basename: old.basename,
                qbasename: String::new(),
                delta_applied_upto: old.delta_applied_upto,
            })
        }
        2 => MetaEnvelope::open(bytes).map_err(map_meta_err),
        other => Err(SnapshotError::VersionMismatch(format!(
            "unknown snapshot format {other}"
        ))),
    }
}

pub(super) fn decode_sidecar(bytes: &[u8]) -> Result<SnapshotSidecar, SnapshotError> {
    match version(bytes)? {
        1 => {
            let old: SidecarV1 = MetaEnvelope::open(bytes).map_err(map_meta_err)?;
            if old.quantization.is_some() {
                return Err(SnapshotError::VersionMismatch(
                    "v1 reserved quantization field is populated".into(),
                ));
            }
            Ok(SnapshotSidecar {
                format_version: old.format_version,
                dim: old.dim,
                metric: old.metric,
                ef_search: old.ef_search,
                m: old.m,
                max_layer: old.max_layer,
                ef_construction: old.ef_construction,
                next_id: old.next_id,
                rid_map: old.rid_map,
                rid_to_internal: old.rid_to_internal,
                tombstones: old.tombstones,
                vectors: old.vectors,
                hnsw_rs_version: old.hnsw_rs_version,
                quantization: None,
                vectors_u8: Vec::new(),
                sections_crc32: old.sections_crc32,
                graph_len: old.graph_len,
                data_len: old.data_len,
            })
        }
        2 => MetaEnvelope::open(bytes).map_err(map_meta_err),
        other => Err(SnapshotError::VersionMismatch(format!(
            "unknown snapshot format {other}"
        ))),
    }
}

#[cfg(test)]
#[path = "tests/legacy_tests.rs"]
mod tests;

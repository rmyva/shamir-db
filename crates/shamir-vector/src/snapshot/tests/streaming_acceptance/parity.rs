use crate::adapter::VectorAdapter;
use crate::hnsw_adapter::{await_fitted, HnswAdapter, HnswConfig};
use crate::snapshot::codec::{
    chunk_key, manifest_key, sidecar_key, ChunkHeader, SNAPSHOT_FORMAT_VERSION,
};
use crate::snapshot::manifest::{integrity_bytes, integrity_key};
use crate::snapshot::HNSW_RS_VERSION;
use crate::snapshot::{dump_snapshot_with_gen, QuantMeta, SnapshotManifest, SnapshotSidecar};
use bytes::Bytes;
use futures::StreamExt;
use hnsw_rs::api::AnnT;
use shamir_index_core::kind::{VectorMetric, VectorQuantization};
use shamir_index_core::meta_envelope::MetaEnvelope;
use shamir_storage::storage_in_memory::InMemoryStore;
use shamir_storage::types::{RecordKey, Store};
use shamir_types::types::record_id::RecordId;
use std::sync::Arc;

const CHUNK_SIZE: usize = 1 << 20;

fn adapter(quantized: bool) -> HnswAdapter {
    HnswAdapter::new_with_quantization(
        2,
        VectorMetric::L2,
        HnswConfig::default(),
        quantized.then_some(VectorQuantization::Sq8),
    )
}

fn id(i: u16) -> RecordId {
    let mut bytes = [0; 16];
    bytes[..2].copy_from_slice(&i.to_le_bytes());
    RecordId(bytes)
}

async fn expected_old(adapter: &HnswAdapter, keyspace: &str, gen: u32) -> Vec<(RecordKey, Bytes)> {
    let mut expected = Vec::new();
    let mut hasher = crc32fast::Hasher::new();
    let (basename, graph, data) = match adapter.hnsw_load() {
        Some(hnsw) => {
            let dir = tempfile::tempdir().unwrap();
            let basename = hnsw.file_dump(dir.path(), "shamir").unwrap();
            let graph = std::fs::read(dir.path().join(format!("{basename}.hnsw.graph"))).unwrap();
            let data = std::fs::read(dir.path().join(format!("{basename}.hnsw.data"))).unwrap();
            (basename, graph, data)
        }
        None => ("shamir".to_string(), Vec::new(), Vec::new()),
    };
    hasher.update(&graph);
    hasher.update(&data);
    let (graph_chunks, graph_len) =
        reference_section(&mut expected, keyspace, gen, "graph", &graph);
    let (data_chunks, data_len) = reference_section(&mut expected, keyspace, gen, "data", &data);
    let mut qbasename = String::new();
    let (qgraph_chunks, qdata_chunks) = if adapter.is_quantized() {
        let hnsw = adapter.hnsw_u8_handle().unwrap();
        let dir = tempfile::tempdir().unwrap();
        qbasename = hnsw.file_dump(dir.path(), "shamirq").unwrap();
        let qgraph = std::fs::read(dir.path().join(format!("{qbasename}.hnsw.graph"))).unwrap();
        let qdata = std::fs::read(dir.path().join(format!("{qbasename}.hnsw.data"))).unwrap();
        hasher.update(&qgraph);
        hasher.update(&qdata);
        let (qgc, _) = reference_section(&mut expected, keyspace, gen, "qgraph", &qgraph);
        let (qdc, _) = reference_section(&mut expected, keyspace, gen, "qdata", &qdata);
        (qgc, qdc)
    } else {
        (0, 0)
    };
    let mut rid_map = Vec::new();
    adapter.for_each_rid_map(|i, rid| rid_map.push((i, rid)));
    let mut rid_to_internal = Vec::new();
    adapter.for_each_rid_to_internal(|rid, i| rid_to_internal.push((rid, i)));
    let mut tombstones = Vec::new();
    adapter.for_each_deleted(|i| tombstones.push(i));
    let mut vectors = Vec::new();
    adapter.for_each_vector(|i, v| vectors.push((i, v.to_vec())));
    let mut vectors_u8 = Vec::new();
    if adapter.is_quantized() {
        adapter.for_each_vector_u8(|i, v| vectors_u8.push((i, v.to_vec())));
    }
    let quantization = if adapter.is_quantized() {
        adapter
            .quantizer()
            .map(|q| bincode::serialize(&QuantMeta::from_quantizer(q)).unwrap())
    } else {
        None
    };
    let (m, max_layer, ef_construction) = adapter.build_params().unwrap();
    let sidecar = SnapshotSidecar {
        format_version: SNAPSHOT_FORMAT_VERSION,
        dim: adapter.dim_field(),
        metric: adapter.metric_field(),
        ef_search: adapter.ef_search_field(),
        m,
        max_layer,
        ef_construction,
        next_id: adapter.next_id_value(),
        rid_map,
        rid_to_internal,
        tombstones,
        vectors,
        hnsw_rs_version: HNSW_RS_VERSION.to_string(),
        quantization,
        vectors_u8,
        sections_crc32: hasher.finalize(),
        graph_len,
        data_len,
    };
    let sidecar_bytes = MetaEnvelope::new(sidecar).encode().unwrap();
    expected.push((sidecar_key(keyspace, gen), sidecar_bytes.clone().into()));
    let manifest = SnapshotManifest {
        format_version: SNAPSHOT_FORMAT_VERSION,
        gen,
        graph_chunks,
        data_chunks,
        qgraph_chunks,
        qdata_chunks,
        basename,
        qbasename,
        delta_applied_upto: 0,
    };
    let manifest_bytes = MetaEnvelope::new(manifest).encode().unwrap();
    expected.push((
        integrity_key(keyspace),
        integrity_bytes(&manifest_bytes, &sidecar_bytes),
    ));
    expected.push((manifest_key(keyspace), manifest_bytes.into()));
    expected
}

fn reference_section(
    records: &mut Vec<(RecordKey, Bytes)>,
    keyspace: &str,
    gen: u32,
    section: &str,
    data: &[u8],
) -> (u32, u64) {
    let chunks: Vec<&[u8]> = if data.is_empty() {
        vec![&[]]
    } else {
        data.chunks(CHUNK_SIZE).collect()
    };
    for (idx, chunk) in chunks.iter().enumerate() {
        let header = ChunkHeader {
            idx: idx as u32,
            crc32: crc32fast::hash(chunk),
            bytes: chunk.to_vec(),
        };
        records.push((
            chunk_key(keyspace, gen, section, idx),
            bincode::serialize(&header).unwrap().into(),
        ));
    }
    (chunks.len() as u32, data.len() as u64)
}

async fn assert_complete_parity(adapter: &HnswAdapter, keyspace: &str) {
    let gen = 17;
    let mut expected = expected_old(adapter, keyspace, gen).await;
    let store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    dump_snapshot_with_gen(adapter, &store, keyspace, gen)
        .await
        .unwrap();
    let mut actual = Vec::new();
    let mut stream = store.iter_stream(128);
    while let Some(batch) = stream.next().await {
        actual.extend(batch.unwrap());
    }
    // Envelope creation time is intentionally nondeterministic; substitute only
    // those 8 timestamp bytes in the independent reference before exact set comparison.
    for key in [sidecar_key(keyspace, gen), manifest_key(keyspace)] {
        let actual_bytes = store.get(key.clone()).await.unwrap();
        let expected_bytes = expected
            .iter_mut()
            .find(|(candidate, _)| *candidate == key)
            .unwrap();
        let mut normalized = expected_bytes.1.to_vec();
        normalized[6..14].copy_from_slice(&actual_bytes[6..14]);
        expected_bytes.1 = normalized.into();
    }
    let expected_manifest = expected
        .iter()
        .find(|(key, _)| *key == manifest_key(keyspace))
        .unwrap()
        .1
        .clone();
    let expected_sidecar = expected
        .iter()
        .find(|(key, _)| *key == sidecar_key(keyspace, gen))
        .unwrap()
        .1
        .clone();
    let actual_integrity = store.get(integrity_key(keyspace)).await.unwrap();
    let expected_integrity = integrity_bytes(&expected_manifest, &expected_sidecar);
    assert_eq!(
        actual_integrity, expected_integrity,
        "integrity record differs"
    );
    expected
        .iter_mut()
        .find(|(key, _)| *key == integrity_key(keyspace))
        .unwrap()
        .1 = expected_integrity;
    expected.sort_by(|a, b| a.0.cmp(&b.0));
    actual.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(
        actual, expected,
        "full snapshot key/value set differs from old reference"
    );
}

#[tokio::test]
async fn streamed_nonquantized_multichunk_matches_old_reference_full_set() {
    let adapter = adapter(false);
    let items: Vec<_> = (0..20_000u16)
        .map(|i| (id(i), vec![f32::from(i), f32::from(i % 13)]))
        .collect();
    adapter.upsert_batch(&items).await.unwrap();
    let reference = expected_old(&adapter, "snapshot.parity.preflight", 17).await;
    let graph_bytes: usize = reference
        .iter()
        .filter(|(key, _)| {
            key.as_slice()
                .windows(b".graph.".len())
                .any(|window| window == b".graph.")
        })
        .map(|(_, bytes)| {
            let chunk: ChunkHeader = bincode::deserialize(bytes).unwrap();
            chunk.bytes.len()
        })
        .sum();
    let data_bytes: usize = reference
        .iter()
        .filter(|(key, _)| {
            key.as_slice()
                .windows(b".data.".len())
                .any(|window| window == b".data.")
        })
        .map(|(_, bytes)| {
            let chunk: ChunkHeader = bincode::deserialize(bytes).unwrap();
            chunk.bytes.len()
        })
        .sum();
    assert!(
        graph_bytes > CHUNK_SIZE || data_bytes > CHUNK_SIZE,
        "fixture is not multichunk: graph={graph_bytes}, data={data_bytes}"
    );
    assert_complete_parity(&adapter, "snapshot.parity").await;
}

#[tokio::test]
async fn fitted_quantized_and_empty_f32_sections_match_old_reference_full_set() {
    let adapter = adapter(true);
    let items: Vec<_> = (0..256u16)
        .map(|i| (id(i), vec![f32::from(i), f32::from(i % 13)]))
        .collect();
    adapter.upsert_batch(&items).await.unwrap();
    await_fitted(&adapter).await;
    assert_complete_parity(&adapter, "snapshot.quantized.parity").await;
}

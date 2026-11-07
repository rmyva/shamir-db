use super::{decode_manifest, decode_sidecar, ManifestV1, SidecarV1};
use crate::adapter::{SearchOpts, VectorAdapter};
use crate::hnsw_adapter::{HnswAdapter, HnswConfig};
use crate::snapshot::codec::{manifest_key, sidecar_key};
use crate::snapshot::manifest::{integrity_bytes, integrity_key};
use crate::snapshot::SnapshotError;
use crate::snapshot::{dump_snapshot, load_snapshot, SnapshotManifest, SnapshotSidecar};
use shamir_index_core::kind::VectorMetric;
use shamir_index_core::meta_envelope::MetaEnvelope;
use shamir_storage::storage_in_memory::InMemoryStore;
use shamir_storage::types::Store;
use shamir_types::types::record_id::RecordId;
use std::sync::Arc;

// Schema-derived fixed-int little-endian bytes, NOT captured release artifacts.
// Field order: 3ed3069b^:crates/shamir-index/src/vector/snapshot.rs.
// Envelope timestamp is zero. Distinct values expose misplaced v2 fields.
const MANIFEST_V1: &[u8] = b"SDB2\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x07\x00\x00\x00\x02\x00\x00\x00\x03\x00\x00\x00\x03\x00\x00\x00\x00\x00\x00\x00old\x09\x00\x00\x00\x00\x00\x00\x00";
const SIDECAR_V1: &[u8] = b"SDB2\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x04\x00\x00\x00\x00\x00\x00\x00\x0b\x00\x00\x00\x00\x00\x00\x00\x05\x06\x00\x00\x00\x00\x00\x00\x00\x0d\x00\x00\x00\x00\x00\x00\x00\x11\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x000.3.5\x00xV4\x12\x15\x00\x00\x00\x00\x00\x00\x00\x16\x00\x00\x00\x00\x00\x00\x00";

#[test]
fn schema_derived_v1_layout_bytes_decode() {
    let m = decode_manifest(MANIFEST_V1).unwrap();
    assert_eq!(
        (m.format_version, m.gen, m.graph_chunks, m.data_chunks),
        (1, 7, 2, 3)
    );
    assert_eq!((m.basename.as_str(), m.delta_applied_upto), ("old", 9));
    assert_eq!(
        (m.qgraph_chunks, m.qdata_chunks, m.qbasename.as_str()),
        (0, 0, "")
    );
    let s = decode_sidecar(SIDECAR_V1).unwrap();
    assert_eq!(
        (s.format_version, s.dim, s.metric, s.ef_search),
        (1, 4, VectorMetric::L2, 11)
    );
    assert_eq!(
        (s.m, s.max_layer, s.ef_construction, s.next_id),
        (5, 6, 13, 17)
    );
    assert!(s.rid_map.is_empty() && s.rid_to_internal.is_empty());
    assert!(s.tombstones.is_empty() && s.vectors.is_empty() && s.vectors_u8.is_empty());
    assert_eq!(s.hnsw_rs_version, "0.3.5");
    assert!(s.quantization.is_none());
    assert_eq!(
        (s.sections_crc32, s.graph_len, s.data_len),
        (0x12345678, 21, 22)
    );
}

// The metadata below is schema-derived; the graph
// is produced by the currently pinned hnsw_rs, not captured historical bytes.
#[tokio::test]
async fn schema_derived_v1_layout_metadata_loads_supported_graph() {
    let store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let keyspace = "legacy.layout";
    let adapter = HnswAdapter::new(2, VectorMetric::L2, HnswConfig::default());
    let a = RecordId([1; 16]);
    let b = RecordId([2; 16]);
    let c = RecordId([3; 16]);
    adapter.upsert(a, &[1.0, 0.0]).await.unwrap();
    adapter.upsert(b, &[0.0, 2.0]).await.unwrap();
    adapter.upsert(c, &[3.0, 4.0]).await.unwrap();
    adapter.delete(b).await.unwrap();
    dump_snapshot(&adapter, &store, keyspace).await.unwrap();

    let mk = manifest_key(keyspace);
    let manifest: SnapshotManifest =
        MetaEnvelope::open(&store.get(mk.clone()).await.unwrap()).unwrap();
    let sk = sidecar_key(keyspace, manifest.gen);
    let sidecar: SnapshotSidecar =
        MetaEnvelope::open(&store.get(sk.clone()).await.unwrap()).unwrap();
    assert!(sidecar.rid_map.len() >= 3);
    assert_eq!(sidecar.rid_to_internal.len(), 2);
    assert!(!sidecar.tombstones.is_empty());
    assert_eq!(sidecar.vectors.len(), 2);
    let old_manifest = ManifestV1 {
        format_version: 1,
        gen: manifest.gen,
        graph_chunks: manifest.graph_chunks,
        data_chunks: manifest.data_chunks,
        basename: manifest.basename,
        delta_applied_upto: manifest.delta_applied_upto,
    };
    let old_sidecar = SidecarV1 {
        format_version: 1,
        dim: sidecar.dim,
        metric: sidecar.metric,
        ef_search: sidecar.ef_search,
        m: sidecar.m,
        max_layer: sidecar.max_layer,
        ef_construction: sidecar.ef_construction,
        next_id: sidecar.next_id,
        rid_map: sidecar.rid_map,
        rid_to_internal: sidecar.rid_to_internal,
        tombstones: sidecar.tombstones,
        vectors: sidecar.vectors,
        hnsw_rs_version: sidecar.hnsw_rs_version,
        quantization: None,
        sections_crc32: sidecar.sections_crc32,
        graph_len: sidecar.graph_len,
        data_len: sidecar.data_len,
    };
    let bytes = MetaEnvelope::new(old_sidecar).encode().unwrap();
    let decoded = decode_sidecar(&bytes).unwrap();
    assert!(decoded.rid_map.iter().any(|(_, rid)| *rid == b));
    assert!(decoded.rid_to_internal.iter().any(|(rid, _)| *rid == a));
    assert!(decoded.rid_to_internal.iter().any(|(rid, _)| *rid == c));
    assert!(decoded.vectors.iter().any(|(_, v)| v == &[1.0, 0.0]));
    assert!(decoded.vectors.iter().any(|(_, v)| v == &[3.0, 4.0]));
    store.set(sk.clone(), bytes.into()).await.unwrap();
    store
        .set(
            mk.clone(),
            MetaEnvelope::new(old_manifest).encode().unwrap().into(),
        )
        .await
        .unwrap();
    store.remove(integrity_key(keyspace)).await.unwrap();
    let loaded = load_snapshot(&store, keyspace).await.unwrap();
    assert!(!loaded.is_quantized());
    assert_eq!(loaded.len(), 2);
    let results = loaded
        .search(&[1.0, 0.0], 3, SearchOpts::default(), None)
        .await
        .unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].0, a);
    assert_eq!(results[1].0, c);

    // Decode and re-encode the actual v1 schema, not a v2 payload with a v1 tag.
    // The graph is from the currently pinned library, as in the baseline above.
    let manifest_bytes = store.get(mk).await.unwrap();
    let baseline_bytes = store.get(sk.clone()).await.unwrap();
    for case in [
        "dim",
        "vector width",
        "m",
        "layer zero",
        "layer large",
        "ef",
        "graph mismatch",
    ] {
        let mut bad: SidecarV1 = MetaEnvelope::open(&baseline_bytes).unwrap();
        match case {
            "dim" => bad.dim = 0,
            "vector width" => {
                bad.vectors[0].1.pop();
            }
            "m" => bad.m = 0,
            "layer zero" => bad.max_layer = 0,
            "layer large" => bad.max_layer = 17,
            "ef" => bad.ef_construction = 0,
            "graph mismatch" => bad.ef_construction += 1,
            _ => unreachable!(),
        }
        let bad_bytes = MetaEnvelope::new(bad).encode().unwrap();
        store
            .set(sk.clone(), bad_bytes.clone().into())
            .await
            .unwrap();
        for checked in [false, true] {
            if checked {
                store
                    .set(
                        integrity_key(keyspace),
                        integrity_bytes(&manifest_bytes, &bad_bytes),
                    )
                    .await
                    .unwrap();
            } else {
                store.remove(integrity_key(keyspace)).await.unwrap();
            }
            match load_snapshot(&store, keyspace).await {
                Err(SnapshotError::Corrupt(_)) => {}
                Err(error) => panic!("v1 {case}, checked={checked}: {error:?}"),
                Ok(_) => panic!("v1 {case}, checked={checked} accepted"),
            }
        }
    }
}

#[test]
fn v2_quantized_fields_survive_dispatch() {
    let mut m = decode_manifest(MANIFEST_V1).unwrap();
    m.format_version = 2;
    m.qgraph_chunks = 4;
    m.qdata_chunks = 5;
    m.qbasename = "quant".into();
    let decoded = decode_manifest(&MetaEnvelope::new(m).encode().unwrap()).unwrap();
    assert_eq!(
        (
            decoded.qgraph_chunks,
            decoded.qdata_chunks,
            decoded.qbasename.as_str()
        ),
        (4, 5, "quant")
    );
    let mut s = decode_sidecar(SIDECAR_V1).unwrap();
    s.format_version = 2;
    s.quantization = Some(vec![1, 2]);
    s.vectors_u8 = vec![(3, vec![4, 5])];
    let decoded = decode_sidecar(&MetaEnvelope::new(s).encode().unwrap()).unwrap();
    assert_eq!(decoded.quantization, Some(vec![1, 2]));
    assert_eq!(decoded.vectors_u8, vec![(3, vec![4, 5])]);
}

#[test]
fn rejects_unknown_and_truncated_envelopes() {
    for fixture in [MANIFEST_V1, SIDECAR_V1] {
        let mut inner = fixture.to_vec();
        inner[14] = 99;
        assert!(matches!(
            decode_manifest(&inner),
            Err(SnapshotError::VersionMismatch(_))
        ));
        assert!(matches!(
            decode_sidecar(&inner),
            Err(SnapshotError::VersionMismatch(_))
        ));
        let mut outer = fixture.to_vec();
        outer[4] = 99;
        assert!(matches!(
            decode_manifest(&outer),
            Err(SnapshotError::VersionMismatch(_))
        ));
        assert!(matches!(
            decode_sidecar(&outer),
            Err(SnapshotError::VersionMismatch(_))
        ));
    }
    assert!(decode_manifest(&MANIFEST_V1[..MANIFEST_V1.len() - 1]).is_err());
    assert!(decode_sidecar(&SIDECAR_V1[..SIDECAR_V1.len() - 1]).is_err());
    let mut bad_magic = MANIFEST_V1.to_vec();
    bad_magic[0] = 0;
    assert!(decode_manifest(&bad_magic).is_err());
}

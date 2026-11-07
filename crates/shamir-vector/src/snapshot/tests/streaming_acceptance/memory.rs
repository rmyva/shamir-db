use super::allocation_counter::LargestWindow;
use crate::adapter::VectorAdapter;
use crate::hnsw_adapter::{HnswAdapter, HnswConfig, ShamirDist};
use crate::snapshot::codec::{chunk_key, manifest_key, sidecar_key, ChunkHeader};
use crate::snapshot::SnapshotManifest;
use crate::snapshot::{dump_snapshot_with_gen, load_snapshot};
use hnsw_rs::api::AnnT;
use shamir_index_core::kind::VectorMetric;
use shamir_index_core::meta_envelope::MetaEnvelope;
use shamir_storage::storage_in_memory::InMemoryStore;
use shamir_storage::types::Store;
use shamir_types::types::record_id::RecordId;
use std::path::Path;
use std::sync::Arc;

const DIM: usize = 2;
// Fewer elements keep the oracle fast (8_192 took ~103 s in CI); the section-size margin
// over the bound is asserted at run time.
const ELEMENTS: u16 = 2_048;

fn adapter() -> HnswAdapter {
    let config = HnswConfig {
        max_elements: usize::from(ELEMENTS),
        m: 128,
        ..HnswConfig::default()
    };
    let mut hnsw = hnsw_rs::hnsw::Hnsw::new(
        config.m,
        config.max_elements,
        config.max_layer,
        config.ef_construction,
        ShamirDist {
            metric: VectorMetric::L2,
        },
    );
    // Retaining candidates beyond the pruned M-neighbor selection guarantees
    // a dense enough graph for this allocation oracle's file-size margin.
    hnsw.set_keeping_pruned(true);
    HnswAdapter::from_parts(
        DIM as u32,
        VectorMetric::L2,
        config.ef_search,
        Arc::new(hnsw),
        scc::HashMap::with_hasher(shamir_types::types::common::THasher::default()),
        scc::HashMap::with_hasher(shamir_types::types::common::THasher::default()),
        scc::HashMap::with_hasher(shamir_types::types::common::THasher::default()),
        scc::HashMap::with_hasher(shamir_types::types::common::THasher::default()),
        0,
        None,
    )
}

fn fixture_items() -> Vec<(RecordId, Vec<f32>)> {
    (0..ELEMENTS)
        .map(|i| {
            let mut id = [0; 16];
            id[..2].copy_from_slice(&i.to_le_bytes());
            (RecordId(id), vec![f32::from(i), f32::from(i % 127)])
        })
        .collect()
}

fn make_dump_files(adapter: &HnswAdapter) -> (tempfile::TempDir, String, u64, u64) {
    let dir = tempfile::tempdir().unwrap();
    let basename = adapter
        .hnsw_load()
        .unwrap()
        .file_dump(dir.path(), "memory")
        .unwrap();
    let graph = std::fs::metadata(dir.path().join(format!("{basename}.hnsw.graph")))
        .unwrap()
        .len();
    let data = std::fs::metadata(dir.path().join(format!("{basename}.hnsw.data")))
        .unwrap()
        .len();
    (dir, basename, graph, data)
}

fn assert_fixture_and_bound(graph: u64, data: u64, sidecar_len: usize) -> usize {
    let section_max = graph.max(data) as usize;
    let adapter_capacity_floor = usize::from(ELEMENTS) * DIM * std::mem::size_of::<f32>();
    let bound = (1 << 20)
        .max(sidecar_len.saturating_mul(2))
        .max(adapter_capacity_floor)
        .saturating_add(256 * 1024);
    assert!(section_max >= bound.saturating_mul(4),
        "fixture margin insufficient: section_max={section_max}, bound={bound}, graph={graph}, data={data}, sidecar={sidecar_len}");
    bound
}

fn old_whole_file_allocation(graph_path: &Path, data_path: &Path) -> usize {
    let window = LargestWindow::start();
    let graph = std::fs::read(graph_path).unwrap();
    let data = std::fs::read(data_path).unwrap();
    let largest = window.finish();
    assert!(largest >= graph.len().max(data.len()));
    largest
}

async fn persisted_sidecar_size(store: &Arc<dyn Store>, keyspace: &str) -> usize {
    let manifest_bytes = store.get(manifest_key(keyspace)).await.unwrap();
    let manifest: SnapshotManifest = MetaEnvelope::open(&manifest_bytes).unwrap();
    store
        .get(sidecar_key(keyspace, manifest.gen))
        .await
        .unwrap()
        .len()
}

async fn assert_section_chunks_match_file_sizes(
    store: &Arc<dyn Store>,
    keyspace: &str,
    graph_len: u64,
    data_len: u64,
) {
    let manifest_bytes = store.get(manifest_key(keyspace)).await.unwrap();
    let manifest: SnapshotManifest = MetaEnvelope::open(&manifest_bytes).unwrap();
    for (name, count, expected) in [
        ("graph", manifest.graph_chunks, graph_len),
        ("data", manifest.data_chunks, data_len),
    ] {
        let mut actual = 0u64;
        for index in 0..count {
            let encoded = store
                .get(chunk_key(keyspace, manifest.gen, name, index as usize))
                .await
                .unwrap();
            let header: ChunkHeader = bincode::deserialize(&encoded).unwrap();
            actual += header.bytes.len() as u64;
        }
        assert_eq!(actual, expected, "{name} chunk total");
    }
}

#[tokio::test]
async fn dump_largest_allocation_is_bounded_and_old_read_exceeds_it() {
    if std::env::var_os("NEXTEST").is_none() {
        eprintln!("SKIP dump allocation oracle: NEXTEST process isolation required");
        return;
    }
    let adapter = adapter();
    adapter.upsert_batch(&fixture_items()).await.unwrap();
    let (dir, basename, graph_len, data_len) = make_dump_files(&adapter);
    let sidecar_probe: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    dump_snapshot_with_gen(&adapter, &sidecar_probe, "memory.sidecar.probe", 0)
        .await
        .unwrap();
    let sidecar_len = persisted_sidecar_size(&sidecar_probe, "memory.sidecar.probe").await;
    let bound = assert_fixture_and_bound(graph_len, data_len, sidecar_len);
    let old = old_whole_file_allocation(
        &dir.path().join(format!("{basename}.hnsw.graph")),
        &dir.path().join(format!("{basename}.hnsw.data")),
    );
    assert!(
        old > bound,
        "old whole-file read {old} does not exceed {bound}"
    );
    let store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let window = LargestWindow::start();
    dump_snapshot_with_gen(&adapter, &store, "memory.dump", 0)
        .await
        .unwrap();
    let largest = window.finish();
    assert!(
        largest <= bound,
        "largest successful allocation {largest} exceeds {bound}"
    );
    assert_section_chunks_match_file_sizes(&store, "memory.dump", graph_len, data_len).await;
}

#[tokio::test]
async fn load_largest_allocation_is_bounded_and_old_read_exceeds_it() {
    if std::env::var_os("NEXTEST").is_none() {
        eprintln!("SKIP load allocation oracle: NEXTEST process isolation required");
        return;
    }
    let adapter = adapter();
    adapter.upsert_batch(&fixture_items()).await.unwrap();
    let (dir, basename, graph_len, data_len) = make_dump_files(&adapter);
    let store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    dump_snapshot_with_gen(&adapter, &store, "memory.load", 0)
        .await
        .unwrap();
    let sidecar_len = persisted_sidecar_size(&store, "memory.load").await;
    let bound = assert_fixture_and_bound(graph_len, data_len, sidecar_len);
    let old = old_whole_file_allocation(
        &dir.path().join(format!("{basename}.hnsw.graph")),
        &dir.path().join(format!("{basename}.hnsw.data")),
    );
    assert!(
        old > bound,
        "old whole-file read {old} does not exceed {bound}"
    );
    assert_section_chunks_match_file_sizes(&store, "memory.load", graph_len, data_len).await;
    let window = LargestWindow::start();
    let _loaded = load_snapshot(&store, "memory.load").await.unwrap();
    let largest = window.finish();
    assert!(
        largest <= bound,
        "largest successful allocation {largest} exceeds {bound}"
    );
}

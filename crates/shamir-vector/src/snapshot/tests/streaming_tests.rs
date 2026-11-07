//! Streaming snapshot chunk, metadata-failure, and corruption regressions.

use crate::adapter::VectorAdapter;
use crate::hnsw_adapter::{await_fitted, HnswAdapter, HnswConfig};
use crate::snapshot::codec::{chunk_key, manifest_key, sidecar_key, ChunkHeader};
use crate::snapshot::manifest::{integrity_bytes, integrity_key};
use crate::snapshot::{dump_snapshot_with_gen, load_snapshot, SnapshotError, SnapshotManifest};
use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use shamir_index_core::kind::{VectorMetric, VectorQuantization};
use shamir_index_core::meta_envelope::MetaEnvelope;
use shamir_storage::error::{DbError, DbResult};
use shamir_storage::storage_in_memory::InMemoryStore;
use shamir_storage::types::{RecordKey, Store};
use shamir_types::types::record_id::RecordId;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

type TestStream = Pin<Box<dyn Stream<Item = DbResult<Vec<(RecordKey, Bytes)>>> + Send>>;
const KEYSPACE: &str = "snapshot.streaming";

struct FailMetadataStore {
    inner: Arc<dyn Store>,
    fail_manifest: AtomicBool,
    integrity_written: AtomicBool,
}

#[async_trait]
impl Store for FailMetadataStore {
    async fn insert(&self, value: Bytes) -> DbResult<RecordKey> {
        self.inner.insert(value).await
    }
    async fn set(&self, key: RecordKey, value: Bytes) -> DbResult<bool> {
        if key == integrity_key(KEYSPACE) {
            let updated = self.inner.set(key, value).await?;
            self.integrity_written.store(true, Ordering::SeqCst);
            return Ok(updated);
        }
        if key == manifest_key(KEYSPACE) && self.fail_manifest.swap(false, Ordering::SeqCst) {
            return Err(DbError::Storage(
                "injected after integrity, before manifest".into(),
            ));
        }
        self.inner.set(key, value).await
    }
    async fn get(&self, key: RecordKey) -> DbResult<Bytes> {
        self.inner.get(key).await
    }
    async fn remove(&self, key: RecordKey) -> DbResult<bool> {
        self.inner.remove(key).await
    }
    fn iter_stream(&self, batch_size: usize) -> TestStream {
        self.inner.iter_stream(batch_size)
    }
    fn scan_prefix_stream(&self, prefix: Bytes, batch_size: usize) -> TestStream {
        self.inner.scan_prefix_stream(prefix, batch_size)
    }
}

fn rid(i: u16) -> RecordId {
    let mut bytes = [0; 16];
    bytes[..2].copy_from_slice(&i.to_le_bytes());
    RecordId(bytes)
}

async fn adapter() -> HnswAdapter {
    let adapter = HnswAdapter::new_with_quantization(
        2,
        VectorMetric::L2,
        HnswConfig::default(),
        Some(VectorQuantization::Sq8),
    );
    let items: Vec<_> = (0..256)
        .map(|i| (rid(i), vec![i as f32, (i % 13) as f32]))
        .collect();
    adapter.upsert_batch(&items).await.unwrap();
    await_fitted(&adapter).await;
    adapter
}

async fn manifest(store: &Arc<dyn Store>) -> SnapshotManifest {
    MetaEnvelope::open(&store.get(manifest_key(KEYSPACE)).await.unwrap()).unwrap()
}

#[tokio::test]
async fn streamed_quantized_chunks_preserve_header_and_crc() {
    let adapter = adapter().await;
    let store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    dump_snapshot_with_gen(&adapter, &store, KEYSPACE, 0)
        .await
        .unwrap();
    let manifest = manifest(&store).await;
    for (section, count) in [
        ("graph", manifest.graph_chunks),
        ("data", manifest.data_chunks),
        ("qgraph", manifest.qgraph_chunks),
        ("qdata", manifest.qdata_chunks),
    ] {
        for index in 0..count {
            let encoded = store
                .get(chunk_key(KEYSPACE, manifest.gen, section, index as usize))
                .await
                .unwrap();
            let header: ChunkHeader = bincode::deserialize(&encoded).unwrap();
            assert_eq!(header.idx, index);
            assert_eq!(header.crc32, crc32fast::hash(&header.bytes));
        }
    }
    let metadata = store.get(manifest_key(KEYSPACE)).await.unwrap();
    let sidecar = store
        .get(sidecar_key(KEYSPACE, manifest.gen))
        .await
        .unwrap();
    assert_eq!(
        store.get(integrity_key(KEYSPACE)).await.unwrap(),
        integrity_bytes(&metadata, &sidecar)
    );
}

/// Does not override `transact`: the Store default applies each operation by
/// calling `set`, exposing integrity-before-manifest failure ordering.
#[tokio::test]
async fn failure_after_integrity_before_manifest_is_detected_and_rebuildable() {
    let adapter = adapter().await;
    let inner: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    dump_snapshot_with_gen(&adapter, &inner, KEYSPACE, 0)
        .await
        .unwrap();
    let old_manifest = inner.get(manifest_key(KEYSPACE)).await.unwrap();
    let old_integrity = inner.get(integrity_key(KEYSPACE)).await.unwrap();
    let failing = Arc::new(FailMetadataStore {
        inner: Arc::clone(&inner),
        fail_manifest: AtomicBool::new(true),
        integrity_written: AtomicBool::new(false),
    });
    let active = manifest(&inner).await;
    let failing_store: Arc<dyn Store> = failing.clone();
    let failed = dump_snapshot_with_gen(&adapter, &failing_store, KEYSPACE, active.gen + 1).await;
    assert!(failed.is_err(), "manifest Set failure must abort dump");
    assert!(
        failing.integrity_written.load(Ordering::SeqCst),
        "integrity companion must have been written first"
    );
    assert_eq!(
        inner.get(manifest_key(KEYSPACE)).await.unwrap(),
        old_manifest,
        "manifest remains old"
    );
    assert_ne!(
        inner.get(integrity_key(KEYSPACE)).await.unwrap(),
        old_integrity,
        "integrity belongs to unpublished generation"
    );
    assert!(
        matches!(load_snapshot(&inner, KEYSPACE).await, Err(SnapshotError::Corrupt(message)) if message == "snapshot metadata integrity mismatch")
    );
}

#[tokio::test]
async fn missing_and_corrupt_chunks_remain_corrupt_errors() {
    let adapter = adapter().await;
    let store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    dump_snapshot_with_gen(&adapter, &store, KEYSPACE, 0)
        .await
        .unwrap();
    let manifest = manifest(&store).await;
    let key = chunk_key(KEYSPACE, manifest.gen, "qgraph", 0);
    let encoded = store.get(key.clone()).await.unwrap();
    store.remove(key.clone()).await.unwrap();
    assert!(matches!(
        load_snapshot(&store, KEYSPACE).await,
        Err(SnapshotError::Corrupt(_))
    ));
    store.set(key.clone(), encoded.clone()).await.unwrap();
    let mut header: ChunkHeader = bincode::deserialize(&encoded).unwrap();
    header.bytes[0] ^= 1;
    store
        .set(key, bincode::serialize(&header).unwrap().into())
        .await
        .unwrap();
    assert!(matches!(
        load_snapshot(&store, KEYSPACE).await,
        Err(SnapshotError::Corrupt(_))
    ));
}

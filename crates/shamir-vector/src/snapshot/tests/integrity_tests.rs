//! Snapshot metadata integrity and semantic validation regressions.

use crate::adapter::VectorAdapter;
use crate::hnsw_adapter::{await_fitted, HnswAdapter, HnswConfig};
use crate::snapshot::codec::{chunk_key, manifest_key, sidecar_key, ChunkHeader};
use crate::snapshot::manifest::{integrity_bytes, integrity_key};
use crate::snapshot::{
    dump_snapshot_with_gen, flip_generation, load_snapshot, read_manifest, OldGenerationChunks,
    QuantMeta, SnapshotError, SnapshotManifest, SnapshotSidecar,
};
use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use shamir_index_core::kind::{VectorMetric, VectorQuantization};
use shamir_index_core::meta_envelope::MetaEnvelope;
use shamir_storage::error::{DbError, DbResult};
use shamir_storage::storage_in_memory::InMemoryStore;
use shamir_storage::types::{KvOp, RecordKey, Store};
use shamir_types::types::record_id::RecordId;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

type TestStream = Pin<Box<dyn Stream<Item = DbResult<Vec<(RecordKey, Bytes)>>> + Send>>;

// Reject publication unless manifest and companion share one transaction.
struct PublicationStore {
    inner: Arc<dyn Store>,
    checked: AtomicUsize,
}

#[async_trait]
impl Store for PublicationStore {
    async fn insert(&self, value: Bytes) -> DbResult<RecordKey> {
        self.inner.insert(value).await
    }
    async fn set(&self, key: RecordKey, value: Bytes) -> DbResult<bool> {
        if key == manifest_key(KS) || key == integrity_key(KS) {
            return Err(DbError::Storage(
                "metadata published outside transact".into(),
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
    async fn transact(&self, ops: Vec<KvOp>) -> DbResult<()> {
        if self.checked.load(Ordering::SeqCst) == 3
            && ops.iter().any(|op| matches!(op, KvOp::Remove(_)))
        {
            return Err(DbError::Storage("injected prune failure".into()));
        }
        let manifest = ops.iter().find_map(|op| match op {
            KvOp::Set(key, value) if *key == manifest_key(KS) => Some(value),
            _ => None,
        });
        let integrity = ops.iter().find_map(|op| match op {
            KvOp::Set(key, value) if *key == integrity_key(KS) => Some(value),
            _ => None,
        });
        if manifest.is_some() || integrity.is_some() {
            let (Some(manifest), Some(integrity)) = (manifest, integrity) else {
                return Err(DbError::Storage("metadata not published together".into()));
            };
            let decoded: SnapshotManifest = MetaEnvelope::open(manifest)
                .map_err(|error| DbError::Storage(error.to_string()))?;
            let sk = sidecar_key(KS, decoded.gen);
            let in_batch = ops.iter().find_map(|op| match op {
                KvOp::Set(key, value) if *key == sk => Some(value.clone()),
                _ => None,
            });
            let sidecar = match in_batch {
                Some(bytes) => bytes,
                None => self.inner.get(sk).await?,
            };
            if *integrity != integrity_bytes(manifest, &sidecar) {
                return Err(DbError::Storage("wrong metadata checksum".into()));
            }
            self.checked.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.transact(ops).await
    }
}

const KS: &str = "snapshot.integrity";

fn rid(i: u16) -> RecordId {
    let mut bytes = [0; 16];
    bytes[..2].copy_from_slice(&i.to_le_bytes());
    RecordId(bytes)
}

async fn fixture(quantized: bool) -> (Arc<dyn Store>, HnswAdapter) {
    let store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let adapter = HnswAdapter::new_with_quantization(
        2,
        VectorMetric::L2,
        HnswConfig::default(),
        quantized.then_some(VectorQuantization::Sq8),
    );
    let count = if quantized { 256 } else { 3 };
    let items: Vec<_> = (0..count)
        .map(|i| (rid(i), vec![i as f32, (i % 7) as f32]))
        .collect();
    adapter.upsert_batch(&items).await.unwrap();
    if quantized {
        await_fitted(&adapter).await;
        assert!(adapter.is_quantized());
    }
    dump_snapshot_with_gen(&adapter, &store, KS, 0)
        .await
        .unwrap();
    (store, adapter)
}

async fn metadata(store: &Arc<dyn Store>) -> (SnapshotManifest, SnapshotSidecar) {
    let manifest = store.get(manifest_key(KS)).await.unwrap();
    let manifest: SnapshotManifest = MetaEnvelope::open(&manifest).unwrap();
    let sidecar = store.get(sidecar_key(KS, manifest.gen)).await.unwrap();
    (manifest, MetaEnvelope::open(&sidecar).unwrap())
}

async fn publish_metadata(
    store: &Arc<dyn Store>,
    manifest: &SnapshotManifest,
    sidecar: &SnapshotSidecar,
    checked: bool,
) {
    let manifest_bytes = MetaEnvelope::new(manifest.clone()).encode().unwrap();
    let sidecar_bytes = MetaEnvelope::new(sidecar.clone()).encode().unwrap();
    store
        .set(manifest_key(KS), manifest_bytes.clone().into())
        .await
        .unwrap();
    store
        .set(sidecar_key(KS, manifest.gen), sidecar_bytes.clone().into())
        .await
        .unwrap();
    if checked {
        store
            .set(
                integrity_key(KS),
                integrity_bytes(&manifest_bytes, &sidecar_bytes),
            )
            .await
            .unwrap();
    } else {
        store.remove(integrity_key(KS)).await.unwrap();
    }
}

async fn assert_semantic_rejection(
    store: &Arc<dyn Store>,
    manifest: &SnapshotManifest,
    sidecar: &SnapshotSidecar,
    case: &str,
) {
    for checked in [false, true] {
        publish_metadata(store, manifest, sidecar, checked).await;
        match load_snapshot(store, KS).await {
            Err(SnapshotError::Corrupt(_)) => {}
            Err(error) => panic!("{case}, checked={checked}: unexpected error {error:?}"),
            Ok(_) => panic!("{case}, checked={checked}: corrupt snapshot accepted"),
        }
    }
}

#[tokio::test]
async fn semantic_sidecar_and_build_parameters_fail_with_or_without_companion() {
    for quantized in [false, true] {
        let (store, _) = fixture(quantized).await;
        let (manifest, original) = metadata(&store).await;
        for checked in [false, true] {
            publish_metadata(&store, &manifest, &original, checked).await;
            assert!(
                load_snapshot(&store, KS).await.is_ok(),
                "baseline checked={checked}"
            );
        }
        let mut cases = Vec::new();
        let mut bad = original.clone();
        bad.dim = 0;
        cases.push(("dimension zero", bad));
        let mut bad = original.clone();
        bad.vectors.push((usize::MAX, vec![1.0]));
        cases.push(("raw vector width", bad));
        let mut bad = original.clone();
        bad.vectors_u8.push((usize::MAX, vec![1]));
        cases.push(("code width", bad));
        let mut bad = original.clone();
        bad.m = 0;
        cases.push(("m zero", bad));
        let mut bad = original.clone();
        bad.max_layer = 0;
        cases.push(("layer zero", bad));
        let mut bad = original.clone();
        bad.max_layer = 17;
        cases.push(("layer too large", bad));
        let mut bad = original.clone();
        bad.ef_construction = 0;
        cases.push(("ef zero", bad));
        let mut bad = original.clone();
        bad.m = if original.m == 1 { 2 } else { 1 };
        cases.push(("loaded m mismatch", bad));
        let mut bad = original.clone();
        bad.max_layer = 15;
        cases.push(("loaded layer mismatch", bad));
        let mut bad = original.clone();
        bad.ef_construction += 1;
        cases.push(("loaded ef mismatch", bad));
        for (case, sidecar) in cases {
            assert_semantic_rejection(&store, &manifest, &sidecar, case).await;
        }
    }
}

#[tokio::test]
async fn quantizer_semantics_fail_with_or_without_companion_zero_scale_loads() {
    let (store, _) = fixture(true).await;
    let (manifest, original) = metadata(&store).await;
    let original_meta: QuantMeta =
        bincode::deserialize(original.quantization.as_ref().unwrap()).unwrap();
    let mut cases = Vec::new();
    let mut bad = original_meta.clone();
    bad.method = "unknown".into();
    cases.push(("method", bad));
    let mut bad = original_meta.clone();
    bad.dim = 0;
    cases.push(("quant dim zero", bad));
    let mut bad = original_meta.clone();
    bad.dim += 1;
    cases.push(("quant dim mismatch", bad));
    let mut bad = original_meta.clone();
    bad.mins.pop();
    cases.push(("mins length", bad));
    let mut bad = original_meta.clone();
    bad.scales.pop();
    cases.push(("scales length", bad));
    for (label, value, scale) in [
        ("min nan", f32::NAN, false),
        ("min inf", f32::INFINITY, false),
        ("scale nan", f32::NAN, true),
        ("scale inf", f32::INFINITY, true),
        ("negative scale", -1.0, true),
    ] {
        let mut bad = original_meta.clone();
        if scale {
            bad.scales[0] = value;
        } else {
            bad.mins[0] = value;
        }
        cases.push((label, bad));
    }
    for (case, meta) in cases {
        let mut sidecar = original.clone();
        sidecar.quantization = Some(bincode::serialize(&meta).unwrap());
        assert_semantic_rejection(&store, &manifest, &sidecar, case).await;
    }
    let mut zero = original_meta;
    zero.scales.fill(0.0);
    let mut sidecar = original;
    sidecar.quantization = Some(bincode::serialize(&zero).unwrap());
    for checked in [false, true] {
        publish_metadata(&store, &manifest, &sidecar, checked).await;
        assert!(
            load_snapshot(&store, KS).await.is_ok(),
            "zero scales, checked={checked}"
        );
    }
}

#[tokio::test]
async fn malformed_basename_rejected_with_valid_integrity_and_without_companion() {
    let names = [
        "",
        ".",
        "..",
        "../outside",
        "foo/bar",
        "foo\\bar",
        "/absolute",
        "\\\\server\\x",
        "C:outside",
        "a:stream",
        "a\0b",
        "a\nb",
        "a.",
        "a ",
        "con",
        "NUL.txt",
        "lpt1",
        "COM9.dat",
        "COM¹",
        "a<b",
        "a>b",
        "a\"b",
        "a|b",
        "a?b",
        "a*b",
    ];
    for quantized in [false, true] {
        let (store, _) = fixture(quantized).await;
        let (original, sidecar) = metadata(&store).await;
        for name in names {
            let mut manifest = original.clone();
            if quantized {
                manifest.qbasename = name.into();
            } else {
                manifest.basename = name.into();
            }
            assert_semantic_rejection(&store, &manifest, &sidecar, name).await;
        }
        if !quantized {
            assert!(original.qbasename.is_empty());
            for checked in [false, true] {
                publish_metadata(&store, &original, &sidecar, checked).await;
                assert!(load_snapshot(&store, KS).await.is_ok());
            }
        }
    }
}

#[tokio::test]
async fn changed_metadata_and_invalid_companion_fail_closed() {
    let (store, _) = fixture(false).await;
    let (original, sidecar) = metadata(&store).await;
    let mut changed_manifest = original.clone();
    changed_manifest.delta_applied_upto += 1;
    store
        .set(
            manifest_key(KS),
            MetaEnvelope::new(changed_manifest).encode().unwrap().into(),
        )
        .await
        .unwrap();
    assert!(matches!(
        read_manifest(&store, KS).await,
        Err(SnapshotError::Corrupt(_))
    ));
    assert!(matches!(
        load_snapshot(&store, KS).await,
        Err(SnapshotError::Corrupt(_))
    ));
    publish_metadata(&store, &original, &sidecar, true).await;
    let mut changed_sidecar = sidecar.clone();
    changed_sidecar.next_id += 1;
    store
        .set(
            sidecar_key(KS, original.gen),
            MetaEnvelope::new(changed_sidecar).encode().unwrap().into(),
        )
        .await
        .unwrap();
    assert!(matches!(
        read_manifest(&store, KS).await,
        Err(SnapshotError::Corrupt(_))
    ));
    assert!(matches!(
        load_snapshot(&store, KS).await,
        Err(SnapshotError::Corrupt(_))
    ));
    publish_metadata(&store, &original, &sidecar, true).await;
    let good = store.get(integrity_key(KS)).await.unwrap();
    let mut unknown = good.to_vec();
    unknown[3] = 2;
    let mut bad_magic = good.to_vec();
    bad_magic[0] ^= 1;
    for invalid in [vec![], good[..7].to_vec(), unknown, bad_magic, vec![0; 9]] {
        store.set(integrity_key(KS), invalid.into()).await.unwrap();
        assert!(matches!(
            read_manifest(&store, KS).await,
            Err(SnapshotError::Corrupt(_))
        ));
        assert!(matches!(
            load_snapshot(&store, KS).await,
            Err(SnapshotError::Corrupt(_))
        ));
    }
    store.remove(integrity_key(KS)).await.unwrap();
    assert!(read_manifest(&store, KS).await.is_ok());
    assert!(load_snapshot(&store, KS).await.is_ok());
}

async fn section(
    store: &Arc<dyn Store>,
    manifest: &SnapshotManifest,
    name: &str,
    count: u32,
) -> Vec<u8> {
    let mut data = Vec::new();
    for i in 0..count {
        let bytes = store
            .get(chunk_key(KS, manifest.gen, name, i as usize))
            .await
            .unwrap();
        let chunk: ChunkHeader = bincode::deserialize(&bytes).unwrap();
        data.extend_from_slice(&chunk.bytes);
    }
    data
}

#[tokio::test]
async fn parser_panics_in_both_graph_branches_are_corrupt_after_all_crcs_pass() {
    for quantized in [false, true] {
        let (store, _) = fixture(quantized).await;
        let (manifest, mut sidecar) = metadata(&store).await;
        let name = if quantized { "qgraph" } else { "graph" };
        let key = chunk_key(KS, manifest.gen, name, 0);
        let encoded = store.get(key.clone()).await.unwrap();
        let mut chunk: ChunkHeader = bincode::deserialize(&encoded).unwrap();
        // v4 description: magic, mode, m, level scale, layers, three usize
        // fields, then usize distance-name length. Invalid UTF-8 panics in
        // load_description before any graph point or data record is parsed.
        assert_eq!(
            u32::from_ne_bytes(chunk.bytes[..4].try_into().unwrap()),
            0x002a6779
        );
        let offset = 4 + 1 + 1 + 8 + 1 + 4 * std::mem::size_of::<usize>();
        assert!(chunk.bytes.len() > offset);
        chunk.bytes[offset] = 0xff;
        chunk.crc32 = crc32fast::hash(&chunk.bytes);
        store
            .set(key, bincode::serialize(&chunk).unwrap().into())
            .await
            .unwrap();
        let mut crc = crc32fast::Hasher::new();
        for (section_name, count) in [
            ("graph", manifest.graph_chunks),
            ("data", manifest.data_chunks),
            ("qgraph", manifest.qgraph_chunks),
            ("qdata", manifest.qdata_chunks),
        ] {
            crc.update(&section(&store, &manifest, section_name, count).await);
        }
        sidecar.sections_crc32 = crc.finalize();
        publish_metadata(&store, &manifest, &sidecar, true).await;
        match load_snapshot(&store, KS).await {
            Err(SnapshotError::Corrupt(message)) => {
                assert!(message.contains("panicked"), "{message}")
            }
            Err(error) => panic!("unexpected error: {error:?}"),
            Ok(_) => panic!("corrupt parser input accepted"),
        }
    }
}

#[tokio::test]
async fn dump_and_flip_publish_companion_before_bounded_prune() {
    let (source, adapter) = fixture(false).await;
    let wrapped = Arc::new(PublicationStore {
        inner: source,
        checked: AtomicUsize::new(0),
    });
    let store: Arc<dyn Store> = wrapped.clone();
    dump_snapshot_with_gen(&adapter, &store, KS, 0)
        .await
        .unwrap();
    let (old, _) = metadata(&store).await;
    let manifest_bytes = store.get(manifest_key(KS)).await.unwrap();
    let sidecar_bytes = store.get(sidecar_key(KS, old.gen)).await.unwrap();
    assert_eq!(
        store.get(integrity_key(KS)).await.unwrap(),
        integrity_bytes(&manifest_bytes, &sidecar_bytes)
    );
    dump_snapshot_with_gen(&adapter, &store, KS, old.gen + 1)
        .await
        .unwrap();
    let (next, _) = metadata(&store).await;
    let next_gen = next.gen;
    let mut next = next;
    next.delta_applied_upto = 7;
    flip_generation(
        &store,
        KS,
        old.gen,
        OldGenerationChunks {
            graph: old.graph_chunks,
            data: old.data_chunks,
            qgraph: old.qgraph_chunks,
            qdata: old.qdata_chunks,
        },
        next,
        7,
    )
    .await
    .expect_err("prune failure must be reported after metadata commit");
    let manifest_bytes = store.get(manifest_key(KS)).await.unwrap();
    let manifest: SnapshotManifest = MetaEnvelope::open(&manifest_bytes).unwrap();
    let sidecar_bytes = store.get(sidecar_key(KS, manifest.gen)).await.unwrap();
    assert_eq!(manifest.gen, next_gen);
    assert_eq!(manifest.delta_applied_upto, 7);
    assert_eq!(
        store.get(integrity_key(KS)).await.unwrap(),
        integrity_bytes(&manifest_bytes, &sidecar_bytes)
    );
    assert!(load_snapshot(&store, KS).await.is_ok());
    assert_eq!(wrapped.checked.load(Ordering::SeqCst), 3);
}

//! Startup restore integration tests (V2.2 / #401).
//!
//! Proves the three branches of `VectorBackend::restore_on_open`:
//! 1. **valid snapshot → no rebuild** — a warm restart with a usable
//!    snapshot loads in O(load), `rebuild_count == 0`, and search returns
//!    the same top-k as before the restart.
//! 2. **no snapshot → full rebuild** — a fresh open with no snapshot
//!    falls back to a data-store scan, `rebuild_count == 1`.
//! 3. **mismatched config → warn + rebuild** — a snapshot stamped with
//!    a foreign `format_version` is refused, the open warns and falls back
//!    to a full rebuild, `rebuild_count == 1`, no panic.
//!
//! These tests live in `shamir-index` (next to `VectorBackend`) because
//! they exercise the backend's own restore contract, not the engine's
//! open-path wiring (which is covered by `shamir-engine` integration
//! tests).
//!
//! NOTE on the dump path: `snapshot::dump_snapshot` takes a `&HnswAdapter`
//! (the concrete type), while `VectorBackend` stores its adapter as a
//! `dyn VectorAdapter`. These tests therefore build + dump a standalone
//! `HnswAdapter` in phase 1, then construct a fresh `VectorBackend` (with
//! an EMPTY adapter) in phase 2 and call `restore_on_open` — mirroring
//! exactly what the engine does on a real restart (the dump is a separate,
//! caller-driven action that #402 will wire up; only the LOAD happens
//! automatically on open).

use crate::adapter::VectorAdapter;
use crate::hnsw_adapter::{await_fitted, HnswAdapter, HnswConfig};
use crate::snapshot;
use crate::vector_backend::VectorBackend;
use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use shamir_index_core::backend::{IndexBackend, IndexQuery, IndexResult};
use shamir_index_core::descriptor::IndexDescriptor;
use shamir_index_core::kind::{
    IndexKind, VectorBackendRef, VectorConfig, VectorMetric, VectorQuantization,
};
use shamir_index_core::persistence::{load_index2_metadata, save_index2_metadata};
use shamir_storage::error::{DbError, DbResult};
use shamir_storage::storage_in_memory::InMemoryStore;
use shamir_storage::types::{RecordKey, Store};
use shamir_types::core::interner::{Interner, InternerKey, TouchInd};
use shamir_types::types::common::new_map_wc;
use shamir_types::types::record_id::RecordId;
use shamir_types::types::value::InnerValue;
use smallvec::SmallVec;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

// ----------------------------------------------------------------------------
// helpers
// ----------------------------------------------------------------------------

struct FailManifestStore {
    inner: Arc<dyn Store>,
    fail_once: AtomicBool,
    integrity_written: AtomicBool,
    keyspace: String,
}

type RestoreStream = Pin<Box<dyn Stream<Item = DbResult<Vec<(RecordKey, Bytes)>>> + Send>>;

#[async_trait]
impl Store for FailManifestStore {
    async fn insert(&self, value: Bytes) -> DbResult<RecordKey> {
        self.inner.insert(value).await
    }
    async fn set(&self, key: RecordKey, value: Bytes) -> DbResult<bool> {
        let integrity: RecordKey = Bytes::from(format!("{}.integrity", self.keyspace)).into();
        let manifest: RecordKey = Bytes::from(format!("{}.manifest", self.keyspace)).into();
        if key == integrity {
            let result = self.inner.set(key, value).await?;
            self.integrity_written.store(true, Ordering::SeqCst);
            return Ok(result);
        }
        if key == manifest && self.fail_once.swap(false, Ordering::SeqCst) {
            return Err(DbError::Storage(
                "injected sequential manifest Set failure".into(),
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
    fn iter_stream(&self, batch_size: usize) -> RestoreStream {
        self.inner.iter_stream(batch_size)
    }
    fn scan_prefix_stream(&self, prefix: Bytes, batch_size: usize) -> RestoreStream {
        self.inner.scan_prefix_stream(prefix, batch_size)
    }
}

#[tokio::test]
async fn sequential_metadata_failure_triggers_backend_rebuild_from_source_records() {
    let interner = Interner::new();
    let info: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let data: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let index_id = 24;
    let keyspace = format!("__vec_snap__{index_id}");
    let items: Vec<_> = (0..N)
        .map(|n| (rid(n), lcg_vec(DIM as usize, n as u64 * 7 + 1)))
        .collect();
    let original = HnswAdapter::new(DIM, VectorMetric::L2, HnswConfig::default());
    original.upsert_batch(&items).await.unwrap();
    snapshot::dump_snapshot(&original, &info, &keyspace)
        .await
        .unwrap();
    let previous_manifest = info
        .get(bytes::Bytes::from(format!("{keyspace}.manifest")).into())
        .await
        .unwrap();
    for (record_id, vector) in &items {
        let record = make_rec(
            &interner,
            &vector
                .iter()
                .map(|value| f64::from(*value))
                .collect::<Vec<_>>(),
        );
        data.set(record_id.0.to_vec().into(), record.to_bytes().unwrap())
            .await
            .unwrap();
    }
    let injected = Arc::new(FailManifestStore {
        inner: Arc::clone(&info),
        fail_once: AtomicBool::new(true),
        integrity_written: AtomicBool::new(false),
        keyspace: keyspace.clone(),
    });
    let injected_store: Arc<dyn Store> = injected.clone();
    let active = snapshot::read_manifest(&info, &keyspace).await.unwrap();
    let failure =
        snapshot::dump_snapshot_with_gen(&original, &injected_store, &keyspace, active.gen + 1)
            .await;
    assert!(failure.is_err());
    assert!(injected.integrity_written.load(Ordering::SeqCst));
    assert_eq!(
        info.get(bytes::Bytes::from(format!("{keyspace}.manifest")).into())
            .await
            .unwrap(),
        previous_manifest
    );
    assert!(
        matches!(snapshot::load_snapshot(&info, &keyspace).await, Err(snapshot::SnapshotError::Corrupt(message)) if message == "snapshot metadata integrity mismatch")
    );
    let backend = make_backend(&interner, index_id, DIM, VectorMetric::L2);
    backend
        .restore_on_open(Arc::clone(&info), Arc::clone(&data))
        .await
        .unwrap();
    assert_eq!(backend.rebuild_count(), 1);
    let adapter_ref = backend
        .as_any()
        .downcast_ref::<VectorBackend>()
        .unwrap()
        .adapter_for_test();
    let hnsw = adapter_ref.as_hnsw_adapter().unwrap();
    let mut recovered = Vec::with_capacity(N);
    hnsw.for_each_rid_map(|_, record_id| recovered.push(record_id));
    let mut expected_ids: Vec<_> = items.iter().map(|(record_id, _)| *record_id).collect();
    recovered.sort_unstable();
    expected_ids.sort_unstable();
    assert_eq!(
        recovered, expected_ids,
        "fallback rebuilt exactly the source RecordIds"
    );
    let top = topk_ids(&backend, &lcg_vec(DIM as usize, 42), 5).await;
    assert_eq!(top.len(), 5);
    assert!(top.iter().all(|record_id| expected_ids.contains(record_id)));
}

/// Vector dim used by the round-trip tests. Small enough to keep the test
/// fast, large enough that the HNSW graph path is exercised (the adapter
/// switches to brute-force only below ~256 elements, not below a dim).
const DIM: u32 = 16;

/// Number of vectors to insert. > 256 so the HNSW graph (not the
/// brute-force fallback) is the active search path — a snapshot that
/// round-trips through the graph path is the meaningful contract.
const N: usize = 300;

fn intern(i: &Interner, s: &str) -> u64 {
    match i.touch_ind(s).unwrap() {
        TouchInd::Exists(k) | TouchInd::New(k) => k.id(),
    }
}

/// Build a record `{embedding: [f64; dim]}` — the shape `VectorBackend`
/// extracts via `extract_vec` at the `embedding` interned path.
fn make_rec(interner: &Interner, embedding: &[f64]) -> InnerValue {
    let mut m = new_map_wc(1);
    m.insert(
        InternerKey::new(intern(interner, "embedding")),
        InnerValue::List(embedding.iter().map(|f| InnerValue::F64(*f)).collect()),
    );
    InnerValue::Map(m)
}

/// Deterministic LCG pseudo-random vector (mirrors `snapshot_tests`).
fn lcg_vec(dim: usize, seed: u64) -> Vec<f32> {
    let mut v = Vec::with_capacity(dim);
    let mut s = seed;
    for _ in 0..dim {
        s = s
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        v.push(((s >> 33) as f32) / (u32::MAX as f32) - 0.5);
    }
    v
}

/// `RecordId` from a 2-byte label (covers both 1- and 2-byte rid encodings).
fn rid(i: usize) -> RecordId {
    let mut a = [0u8; 16];
    a[14] = (i >> 8) as u8;
    a[15] = (i & 0xFF) as u8;
    RecordId(a)
}

/// Build a `VectorBackend` with the given descriptor id + dim + metric.
///
/// The adapter starts EMPTY — `restore_on_open` is what fills it (either
/// from a snapshot or a data-store scan). The `field_path` is fixed to the
/// interned `embedding` key. Mirrors what `build_index2_backend_*`
/// constructs on the engine open path.
fn make_backend(interner: &Interner, id: u32, dim: u32, metric: VectorMetric) -> VectorBackend {
    let desc = IndexDescriptor::new(
        id,
        format!("vec_idx_{id}"),
        intern(interner, &format!("vec_idx_{id}")),
        SmallVec::new(),
        IndexKind::Vector(Box::new(VectorConfig {
            dim,
            metric,
            backend: VectorBackendRef::InProcessHnsw {
                ef_construct: 200,
                m: 16,
            },
            quantization: None,
        })),
    );
    let adapter: Arc<dyn VectorAdapter> = Arc::new(HnswAdapter::new(
        dim,
        metric,
        HnswConfig {
            max_elements: 100_000,
            m: 16,
            ef_construction: 200,
            ef_search: 50,
            ..Default::default()
        },
    ));
    VectorBackend::new(desc, vec![intern(interner, "embedding")], adapter, None)
}

/// Build + populate a standalone `HnswAdapter` (NOT wrapped in a backend)
/// with N LCG vectors. Used in phase 1 of each test to produce a snapshot
/// via `dump_snapshot` (which needs the concrete `HnswAdapter`).
///
/// Returns the adapter (still empty — the CALLER upserts the vectors so
/// it controls the async context) and the list of rids that match the
/// canonical item sequence used across these tests.
fn build_adapter(dim: u32, metric: VectorMetric) -> (HnswAdapter, Vec<RecordId>) {
    let adapter = HnswAdapter::new(
        dim,
        metric,
        HnswConfig {
            max_elements: 100_000,
            m: 16,
            ef_construction: 200,
            ef_search: 50,
            ..Default::default()
        },
    );
    let rids: Vec<RecordId> = (0..N).map(rid).collect();
    (adapter, rids)
}

/// Search a backend for `q` returning the top-k `RecordId` set (scores
/// dropped — the round-trip contract is about WHICH rids survive, not
/// exact float-equality of scores, which can drift across HNSW rebuilds).
async fn topk_ids(backend: &VectorBackend, q: &[f32], k: u32) -> Vec<RecordId> {
    let r = backend
        .lookup(IndexQuery::Vector {
            vec: q.to_vec(),
            k,
            opts: crate::SearchOpts::with_ef_search(64),
        })
        .await
        .unwrap();
    match r {
        IndexResult::Ranked(ranked) => ranked.into_iter().map(|(rid, _)| rid).collect(),
        _ => panic!("expected Ranked result"),
    }
}

// ----------------------------------------------------------------------------
// 1. valid snapshot → rebuild_count == 0, top-k preserved
// ----------------------------------------------------------------------------

#[tokio::test]
async fn restore_on_open_valid_snapshot_skips_rebuild() {
    let i = Interner::new();
    // One info_store carries the snapshot; one data_store carries the
    // rows a fallback rebuild would scan. On the snapshot-hit path the
    // data_store is NEVER read.
    let info_store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let data_store: Arc<dyn Store> = Arc::new(InMemoryStore::new());

    // ---- phase 1: build + populate a standalone adapter, dump it -------
    let id: u32 = 1;
    let (adapter, rids) = build_adapter(DIM, VectorMetric::L2);
    let items: Vec<(RecordId, Vec<f32>)> = rids
        .iter()
        .map(|&r| (r, lcg_vec(DIM as usize, (r.0[15] as u64) * 7 + 1)))
        .collect();
    for (r, v) in &items {
        adapter.upsert(*r, v).await.unwrap();
    }

    // Record the pre-dump top-k for a handful of queries.
    let queries: Vec<Vec<f32>> = (0..5u64).map(|s| lcg_vec(DIM as usize, s + 99)).collect();
    let mut before: Vec<Vec<RecordId>> = Vec::with_capacity(queries.len());
    for q in &queries {
        let r = adapter
            .search(q, 10, crate::SearchOpts::with_ef_search(64), None)
            .await
            .unwrap();
        before.push(r.into_iter().map(|(rid, _)| rid).collect());
    }

    // Dump the live adapter into the info_store under the backend's keyspace.
    let keyspace = format!("__vec_snap__{}", id);
    snapshot::dump_snapshot(&adapter, &info_store, &keyspace)
        .await
        .unwrap();

    // ---- phase 2: simulate a restart — fresh backend, empty adapter -----
    let backend = make_backend(&i, id, DIM, VectorMetric::L2);
    // The fresh backend's adapter is empty: a search now returns nothing.
    assert!(
        topk_ids(&backend, &queries[0], 10).await.is_empty(),
        "fresh backend must be empty before restore_on_open"
    );

    // restore_on_open must take the snapshot branch: load succeeds, no
    // data-store scan.
    backend
        .restore_on_open(Arc::clone(&info_store), Arc::clone(&data_store))
        .await
        .unwrap();

    // The instrumentation proof: rebuild_count == 0 means the snapshot
    // was used, NOT a full scan.
    assert_eq!(
        backend.rebuild_count(),
        0,
        "a successful snapshot load must NOT increment the full-rebuild counter"
    );

    // Top-k fidelity: the same rids survive the round-trip.
    for (q, before_set) in queries.iter().zip(before.iter()) {
        let after = topk_ids(&backend, q, 10).await;
        assert_eq!(
            after.len(),
            before_set.len(),
            "top-k size changed across restart for query {:?}",
            q
        );
        let after_set: shamir_collections::TFxSet<RecordId> = after.into_iter().collect();
        for rid in before_set {
            assert!(
                after_set.contains(rid),
                "rid {:?} present before restart but missing after snapshot load",
                rid
            );
        }
    }
}

// ----------------------------------------------------------------------------
// 2. no snapshot → rebuild_count == 1, data from scan
// ----------------------------------------------------------------------------

#[tokio::test]
async fn restore_on_open_no_snapshot_falls_back_to_rebuild() {
    let i = Interner::new();
    let info_store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    // The data_store carries the rows the fallback rebuild scans. We
    // populate it directly with serialized records so `rebuild`'s
    // `iter_stream` finds them.
    let data_store: Arc<dyn Store> = Arc::new(InMemoryStore::new());

    // Insert N records into the data_store as the rebuild scan expects:
    // key = RecordId bytes (16), value = InnerValue::Map{embedding: [...]}.
    let items: Vec<(RecordId, Vec<f32>)> = (0..N)
        .map(|k| (rid(k), lcg_vec(DIM as usize, k as u64 * 7 + 1)))
        .collect();
    for (r, v) in &items {
        let rec = make_rec(&i, &v.iter().map(|f| *f as f64).collect::<Vec<_>>());
        let val_bytes = rec.to_bytes().unwrap();
        data_store
            .set(r.0.to_vec().into(), val_bytes)
            .await
            .unwrap();
    }

    // Fresh backend, NO snapshot in the info_store.
    let backend = make_backend(&i, 2, DIM, VectorMetric::L2);
    backend
        .restore_on_open(Arc::clone(&info_store), Arc::clone(&data_store))
        .await
        .unwrap();

    // Fallback took the full-scan path: counter == 1.
    assert_eq!(
        backend.rebuild_count(),
        1,
        "no snapshot must fall back to exactly one full rebuild"
    );

    // The rebuild scanned the data_store, so all N vectors are queryable.
    let q = lcg_vec(DIM as usize, 42);
    let top = topk_ids(&backend, &q, 5).await;
    assert_eq!(
        top.len(),
        5,
        "rebuild must populate the graph from the data store"
    );
    // Every returned rid must be one we inserted.
    let inserted: shamir_collections::TFxSet<RecordId> = items.iter().map(|(r, _)| *r).collect();
    for r in &top {
        assert!(
            inserted.contains(r),
            "rebuild returned an unknown rid {:?}",
            r
        );
    }
}

// ----------------------------------------------------------------------------
// 3. snapshot with a tampered format_version → warn + rebuild
// ----------------------------------------------------------------------------

#[tokio::test]
async fn restore_on_open_version_mismatch_warns_and_rebuilds() {
    let i = Interner::new();
    let info_store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let data_store: Arc<dyn Store> = Arc::new(InMemoryStore::new());

    // ---- phase 1: dump a snapshot from a standalone adapter ------------
    let id: u32 = 3;
    let (adapter, rids) = build_adapter(DIM, VectorMetric::L2);
    for r in &rids {
        let v = lcg_vec(DIM as usize, (r.0[15] as u64) * 7 + 1);
        adapter.upsert(*r, &v).await.unwrap();
    }
    let keyspace = format!("__vec_snap__{}", id);
    snapshot::dump_snapshot(&adapter, &info_store, &keyspace)
        .await
        .unwrap();

    // Populate the data_store so the fallback rebuild has rows to scan.
    for r in &rids {
        let v = lcg_vec(DIM as usize, (r.0[15] as u64) * 7 + 1);
        let rec = make_rec(&i, &v.iter().map(|f| *f as f64).collect::<Vec<_>>());
        data_store
            .set(r.0.to_vec().into(), rec.to_bytes().unwrap())
            .await
            .unwrap();
    }

    // ---- phase 2: tamper with the sidecar's format_version -------------
    // load_snapshot checks `sidecar.format_version != SNAPSHOT_FORMAT_VERSION`
    // and returns VersionMismatch. We rewrite the sidecar with a version
    // this build will never accept (u16::MAX), mirroring what a real
    // format bump does.
    let manifest_key = format!("{}.manifest", keyspace);
    let manifest: snapshot::SnapshotManifest =
        shamir_index_core::meta_envelope::MetaEnvelope::open(
            &info_store
                .get(bytes::Bytes::from(manifest_key).into())
                .await
                .unwrap(),
        )
        .unwrap();
    let gen = manifest.gen;
    let sidecar_key = format!("{}.g{}.sidecar", keyspace, gen);
    let sidecar: snapshot::SnapshotSidecar = shamir_index_core::meta_envelope::MetaEnvelope::open(
        &info_store
            .get(bytes::Bytes::from(sidecar_key.clone()).into())
            .await
            .unwrap(),
    )
    .unwrap();
    let mut tampered = sidecar;
    tampered.format_version = u16::MAX;
    let tampered_bytes = shamir_index_core::meta_envelope::MetaEnvelope::new(tampered)
        .encode()
        .unwrap();
    info_store
        .set(
            bytes::Bytes::from(sidecar_key).into(),
            tampered_bytes.into(),
        )
        .await
        .unwrap();

    // Exercise unsupported-version recovery in legacy metadata.
    info_store
        .remove(bytes::Bytes::from(format!("{}.integrity", keyspace)).into())
        .await
        .unwrap();

    // ---- phase 3: open — restore_on_open must warn + fall back ---------
    let backend = make_backend(&i, id, DIM, VectorMetric::L2);
    // Must NOT panic — a corrupt snapshot is recoverable.
    backend
        .restore_on_open(Arc::clone(&info_store), Arc::clone(&data_store))
        .await
        .unwrap();

    // The corrupt snapshot triggered a fallback: counter == 1.
    assert_eq!(
        backend.rebuild_count(),
        1,
        "a version-mismatched snapshot must fall back to a full rebuild"
    );

    // The rebuild scanned the data_store, so the graph is populated.
    let q = lcg_vec(DIM as usize, 7);
    let top = topk_ids(&backend, &q, 5).await;
    assert_eq!(top.len(), 5, "fallback rebuild must populate the graph");
    // No panic — the corrupt snapshot did not abort the open.
}

async fn persisted_vector(
    info: &Arc<dyn Store>,
    interner: &Interner,
    id: u32,
    mode: Option<VectorQuantization>,
) {
    let mut descriptor = make_backend(interner, id, DIM, VectorMetric::L2)
        .descriptor()
        .clone();
    descriptor.paths.push(vec![intern(interner, "embedding")]);
    if let IndexKind::Vector(config) = &mut descriptor.kind {
        config.quantization = mode;
    }
    let first_path = descriptor.paths.first().cloned().unwrap_or_default();
    let cfg = match descriptor.kind.clone() {
        IndexKind::Vector(cfg) => cfg,
        _ => unreachable!("vector descriptor expected"),
    };
    let (m, ef_construction) = match &cfg.backend {
        VectorBackendRef::InProcessHnsw { m, ef_construct } => {
            (*m as usize, *ef_construct as usize)
        }
    };
    let adapter = Arc::new(HnswAdapter::new_with_quantization(
        cfg.dim,
        cfg.metric,
        HnswConfig {
            max_elements: 100_000,
            m,
            ef_construction,
            ef_search: 50,
            ..Default::default()
        },
        cfg.quantization,
    ));
    let backend: Arc<dyn IndexBackend> = Arc::new(VectorBackend::new(
        descriptor,
        first_path,
        adapter,
        Some(Arc::clone(info)),
    ));
    let registry = shamir_index_core::IndexRegistry::new();
    registry.insert(backend).await.unwrap();
    save_index2_metadata(&registry, info).await.unwrap();
}

async fn reopen_vector(info: &Arc<dyn Store>, data: &Arc<dyn Store>) -> Arc<dyn IndexBackend> {
    let descriptor = load_index2_metadata(info)
        .await
        .unwrap()
        .unwrap()
        .1
        .descriptors
        .remove(0);
    let first_path = descriptor.paths.first().cloned().unwrap_or_default();
    let cfg = match descriptor.kind.clone() {
        IndexKind::Vector(cfg) => cfg,
        _ => unreachable!("vector descriptor expected"),
    };
    let (m, ef_construction) = match &cfg.backend {
        VectorBackendRef::InProcessHnsw { m, ef_construct } => {
            (*m as usize, *ef_construct as usize)
        }
    };
    let adapter = Arc::new(HnswAdapter::new_with_quantization(
        cfg.dim,
        cfg.metric,
        HnswConfig {
            max_elements: 100_000,
            m,
            ef_construction,
            ef_search: 50,
            ..Default::default()
        },
        cfg.quantization,
    ));
    let backend: Arc<dyn IndexBackend> = Arc::new(VectorBackend::new(
        descriptor,
        first_path,
        adapter,
        Some(Arc::clone(info)),
    ));
    backend
        .restore_on_open(Arc::clone(info), Arc::clone(data))
        .await
        .unwrap();
    backend
}

async fn write_source(data: &Arc<dyn Store>, interner: &Interner, count: usize) {
    for n in 0..count {
        let vector = lcg_vec(DIM as usize, n as u64 + 1);
        let record = make_rec(
            interner,
            &vector.iter().map(|v| f64::from(*v)).collect::<Vec<_>>(),
        );
        data.set(rid(n).to_bytes().into(), record.to_bytes().unwrap())
            .await
            .unwrap();
    }
}

async fn finish_and_assert_fit(backend: &Arc<dyn IndexBackend>, first: usize) {
    let adapter = backend
        .as_any()
        .downcast_ref::<VectorBackend>()
        .unwrap()
        .adapter_for_test();
    let hnsw = adapter.as_hnsw_adapter().unwrap();
    assert_eq!(hnsw.quantization_mode(), Some(VectorQuantization::Sq8));
    assert!(!hnsw.is_quantized());
    assert_eq!(adapter.len(), first);
    for n in first..256 {
        adapter
            .upsert(rid(n), &lcg_vec(DIM as usize, n as u64 + 1))
            .await
            .unwrap();
    }
    await_fitted(hnsw).await;
    assert!(hnsw.is_quantized());
}

#[tokio::test]
async fn sq8_intent_survives_metadata_and_missing_snapshot() {
    let info: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let data: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let interner = Interner::new();
    persisted_vector(&info, &interner, 19, Some(VectorQuantization::Sq8)).await;
    write_source(&data, &interner, 4).await;
    let backend = reopen_vector(&info, &data).await;
    assert_eq!(backend.rebuild_count(), 1);
    finish_and_assert_fit(&backend, 4).await;
}

#[tokio::test]
async fn sq8_intent_survives_f32_snapshot() {
    let info: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let data: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let interner = Interner::new();
    persisted_vector(&info, &interner, 20, Some(VectorQuantization::Sq8)).await;
    let (original, _) = build_adapter(DIM, VectorMetric::L2);
    for n in 0..4 {
        original
            .upsert(rid(n), &lcg_vec(DIM as usize, n as u64 + 1))
            .await
            .unwrap();
    }
    snapshot::dump_snapshot(&original, &info, "__vec_snap__20")
        .await
        .unwrap();
    let backend = reopen_vector(&info, &data).await;
    assert_eq!(backend.rebuild_count(), 0);
    finish_and_assert_fit(&backend, 4).await;
}

#[tokio::test]
async fn sq8_intent_survives_corrupt_snapshot() {
    let info: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let data: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let interner = Interner::new();
    persisted_vector(&info, &interner, 21, Some(VectorQuantization::Sq8)).await;
    write_source(&data, &interner, 4).await;
    let (original, _) = build_adapter(DIM, VectorMetric::L2);
    original
        .upsert(rid(0), &lcg_vec(DIM as usize, 1))
        .await
        .unwrap();
    snapshot::dump_snapshot(&original, &info, "__vec_snap__21")
        .await
        .unwrap();
    let manifest = snapshot::read_manifest(&info, "__vec_snap__21")
        .await
        .unwrap();
    let key = format!("__vec_snap__21.g{}.sidecar", manifest.gen);
    info.set(
        bytes::Bytes::from(key).into(),
        bytes::Bytes::from_static(b"corrupt"),
    )
    .await
    .unwrap();
    let backend = reopen_vector(&info, &data).await;
    assert_eq!(backend.rebuild_count(), 1);
    finish_and_assert_fit(&backend, 4).await;
}

#[tokio::test]
async fn f32_snapshot_without_intent_never_fits() {
    let info: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let data: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let interner = Interner::new();
    persisted_vector(&info, &interner, 22, None).await;
    let (original, _) = build_adapter(DIM, VectorMetric::L2);
    for n in 0..4 {
        original
            .upsert(rid(n), &lcg_vec(DIM as usize, n as u64 + 1))
            .await
            .unwrap();
    }
    snapshot::dump_snapshot(&original, &info, "__vec_snap__22")
        .await
        .unwrap();
    let backend = reopen_vector(&info, &data).await;
    assert_eq!(backend.rebuild_count(), 0);
    let adapter = backend
        .as_any()
        .downcast_ref::<VectorBackend>()
        .unwrap()
        .adapter_for_test();
    let hnsw = adapter.as_hnsw_adapter().unwrap();
    assert_eq!(hnsw.quantization_mode(), None);
    for n in 4..300 {
        adapter
            .upsert(rid(n), &lcg_vec(DIM as usize, n as u64 + 1))
            .await
            .unwrap();
    }
    assert!(!hnsw.is_quantized());
}

#[tokio::test]
async fn legacy_fitted_snapshot_self_heals_options() {
    let info: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let data: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let interner = Interner::new();
    persisted_vector(&info, &interner, 23, None).await;
    assert!(load_index2_metadata(&info)
        .await
        .unwrap()
        .unwrap()
        .1
        .descriptors[0]
        .options
        .is_empty());
    let original = HnswAdapter::new_with_quantization(
        DIM,
        VectorMetric::L2,
        HnswConfig::default(),
        Some(VectorQuantization::Sq8),
    );
    for n in 0..256 {
        original
            .upsert(rid(n), &lcg_vec(DIM as usize, n as u64 + 1))
            .await
            .unwrap();
    }
    await_fitted(&original).await;
    snapshot::dump_snapshot(&original, &info, "__vec_snap__23")
        .await
        .unwrap();
    let backend = reopen_vector(&info, &data).await;
    assert_eq!(backend.rebuild_count(), 0);
    let adapter = backend
        .as_any()
        .downcast_ref::<VectorBackend>()
        .unwrap()
        .adapter_for_test();
    assert!(adapter.as_hnsw_adapter().unwrap().is_quantized());
    let registry = shamir_index_core::IndexRegistry::new();
    registry.insert(backend).await.unwrap();
    save_index2_metadata(&registry, &info).await.unwrap();
    let descriptor = load_index2_metadata(&info)
        .await
        .unwrap()
        .unwrap()
        .1
        .descriptors
        .remove(0);
    assert!(!descriptor.options.is_empty());
    assert!(
        matches!(&descriptor.kind, IndexKind::Vector(config) if config.quantization == Some(VectorQuantization::Sq8))
    );
    let first_path = descriptor.paths.first().cloned().unwrap_or_default();
    let cfg = match descriptor.kind.clone() {
        IndexKind::Vector(cfg) => cfg,
        _ => unreachable!("vector descriptor expected"),
    };
    let (m, ef_construction) = match &cfg.backend {
        VectorBackendRef::InProcessHnsw { m, ef_construct } => {
            (*m as usize, *ef_construct as usize)
        }
    };
    let adapter = Arc::new(HnswAdapter::new_with_quantization(
        cfg.dim,
        cfg.metric,
        HnswConfig {
            max_elements: 100_000,
            m,
            ef_construction,
            ef_search: 50,
            ..Default::default()
        },
        cfg.quantization,
    ));
    let reopened: Arc<dyn IndexBackend> = Arc::new(VectorBackend::new(
        descriptor,
        first_path,
        adapter,
        Some(Arc::clone(&info)),
    ));
    reopened
        .restore_on_open(Arc::clone(&info), data)
        .await
        .unwrap();
    let live = reopened
        .as_any()
        .downcast_ref::<VectorBackend>()
        .unwrap()
        .adapter_for_test();
    assert_eq!(
        live.as_hnsw_adapter().unwrap().quantization_mode(),
        Some(VectorQuantization::Sq8)
    );
    assert!(live.as_hnsw_adapter().unwrap().is_quantized());
}

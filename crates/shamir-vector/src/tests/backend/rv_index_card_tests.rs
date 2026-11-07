//! Consolidated vector-backend regression tests (RV-INDEX cards).
//!
//! `rv_index_0b756318` — `drop_all` must sweep the per-index snapshot
//! namespace using the trailing-dot delimiter (`__vec_snap__9.`) so a
//! sibling index (90/91) sharing the id as a raw prefix survives, and must
//! work on a freshly built backend that was never reopened.
//!
//! `rv_index_ab9403d5` — a staged vector whose declared dimension mismatches
//! the index descriptor must be rejected (staged_vector returns None).
//!
//! `rv_index_fab4e4e5` — the descriptor's HNSW tuning (`m`, `ef_construct`)
//! must reach the built `HnswAdapter` instead of hardcoded defaults.

use crate::brute_force::BruteForceAdapter;
use crate::hnsw_adapter::{HnswAdapter, HnswConfig};
use crate::snapshot::{chunk_key_for_test, manifest_key_for_test, sidecar_key_for_test};
use crate::vector_backend::VectorBackend;
use bytes::Bytes;
use shamir_index_core::backend::IndexBackend;
use shamir_index_core::descriptor::IndexDescriptor;
use shamir_index_core::kind::{IndexKind, VectorBackendRef, VectorConfig, VectorMetric};
use shamir_storage::storage_in_memory::InMemoryStore;
use shamir_storage::types::{RecordKey, Store};
use shamir_types::core::interner::{Interner, TouchInd};
use shamir_types::types::record_id::RecordId;
use shamir_types::types::value::InnerValue;
use smallvec::SmallVec;
use std::sync::Arc;

/// Real per-index snapshot keyspace: `__vec_snap__<id>` (SNAPSHOT_KEYSPACE_PREFIX).
fn snap_ks(id: u32) -> String {
    format!("__vec_snap__{id}")
}

fn mkey(id: u32) -> RecordKey {
    manifest_key_for_test(&snap_ks(id))
}

fn ckey(id: u32, gen: u32, section: &str, idx: usize) -> RecordKey {
    chunk_key_for_test(&snap_ks(id), gen, section, idx)
}

fn skey(id: u32, gen: u32) -> RecordKey {
    sidecar_key_for_test(&snap_ks(id), gen)
}

fn vector_desc(id: u32, name: &str) -> IndexDescriptor {
    vector_desc_tuned(id, name, 2, 16, 200)
}

fn vector_desc_tuned(id: u32, name: &str, dim: u32, m: u32, ef_construct: u32) -> IndexDescriptor {
    let interner = Interner::new();
    let name_id = match interner.touch_ind(name).unwrap() {
        TouchInd::Exists(k) | TouchInd::New(k) => k.id(),
    };
    IndexDescriptor::new(
        id,
        name,
        name_id,
        SmallVec::new(),
        IndexKind::Vector(Box::new(VectorConfig {
            dim,
            metric: VectorMetric::L2,
            backend: VectorBackendRef::InProcessHnsw { m, ef_construct },
            quantization: None,
        })),
    )
}

#[tokio::test]
async fn rv_index_0b756318_vector_drop_all_sweeps_snapshot_namespace() {
    let desc = vector_desc(916, "drop_idx");
    let store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    for k in [mkey(916), ckey(916, 1, "qdata", 0)] {
        store.set(k, Bytes::from_static(b"data")).await.unwrap();
    }
    let backend = VectorBackend::new(
        desc,
        vec![],
        Arc::new(BruteForceAdapter::new(2, VectorMetric::L2)),
        Some(Arc::clone(&store)),
    );
    // Snapshot metadata lives in the info store; record rows (scanned by the
    // rebuild fallback after the corrupt manifest) live in a separate store.
    let data_store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    backend
        .restore_on_open(Arc::clone(&store), data_store)
        .await
        .unwrap();
    backend.drop_all().await.unwrap();
    assert!(store.get(mkey(916)).await.is_err());
    assert!(store.get(ckey(916, 1, "qdata", 0)).await.is_err());
}

#[tokio::test]
async fn rv_index_ab9403d5_staged_vector_rejects_wrong_dimension() {
    let interner = Interner::new();
    let path = match interner.touch_ind("embedding").unwrap() {
        TouchInd::Exists(k) | TouchInd::New(k) => k.id(),
    };
    let desc = vector_desc(918, "v");
    let backend = VectorBackend::new(
        desc,
        vec![path],
        Arc::new(BruteForceAdapter::new(2, VectorMetric::L2)),
        Some(Arc::new(InMemoryStore::new())),
    );
    let mut m = shamir_types::types::common::new_map_wc(1);
    m.insert(
        shamir_types::core::interner::InternerKey::new(path),
        InnerValue::List(vec![InnerValue::F64(1.0)]),
    );
    let rec = InnerValue::Map(m);
    assert!(backend
        .staged_vector(RecordId([0; 16]), &rec)
        .await
        .is_none());
}

// RV-INDEX-FAB4E4E5: the descriptor's HNSW tuning must propagate into the
// built adapter, not hardcoded m:16 / ef_construction:200 defaults.
#[test]
fn rv_index_fab4e4e5_builder_preserves_backend_tuning() {
    let desc = vector_desc_tuned(917, "ix", 2, 8, 77);
    let store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let first_path = desc.paths.first().cloned().unwrap_or_default();
    let cfg = match desc.kind.clone() {
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
        desc,
        first_path,
        adapter,
        Some(Arc::clone(&store)),
    ));
    let vb = backend
        .as_any()
        .downcast_ref::<crate::VectorBackend>()
        .expect("vector descriptor must build a VectorBackend");
    let adapter = vb.adapter_for_test();
    let hnsw = adapter
        .as_hnsw_adapter()
        .expect("descriptor must build an HnswAdapter");
    let cfg = hnsw.build_config();
    assert_eq!(cfg.m, 8, "descriptor m must reach the built adapter");
    assert_eq!(
        cfg.ef_construction, 77,
        "descriptor ef_construct must reach the built adapter"
    );
}

// Trailing-dot delimiter guard: index 9's sweep must not match ids 90/91,
// which share `9` as a raw prefix.
#[tokio::test]
async fn rv_index_0b756318_drop_index_9_spares_indexes_90_and_91() {
    let desc = vector_desc(9, "n9");
    let store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    for k in [
        mkey(9),
        ckey(9, 0, "data", 0),
        mkey(90),
        ckey(91, 0, "qgraph", 0),
    ] {
        store.set(k, Bytes::from_static(b"data")).await.unwrap();
    }
    let backend = VectorBackend::new(
        desc,
        vec![],
        Arc::new(BruteForceAdapter::new(2, VectorMetric::L2)),
        Some(Arc::clone(&store)),
    );
    backend.drop_all().await.unwrap();
    assert!(store.get(mkey(9)).await.is_err());
    assert!(store.get(ckey(9, 0, "data", 0)).await.is_err());
    assert!(store.get(mkey(90)).await.is_ok());
    assert!(store.get(ckey(91, 0, "qgraph", 0)).await.is_ok());
}

// drop_all on a freshly built backend (no restore_on_open) must sweep too:
// VectorBackend::new must initialize the info store from its `info_store` arg.
#[tokio::test]
async fn rv_index_0b756318_create_then_drop_without_reopen() {
    let desc = vector_desc(920, "n920");
    let store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    for k in [mkey(920), skey(920, 0)] {
        store.set(k, Bytes::from_static(b"data")).await.unwrap();
    }
    let first_path = desc.paths.first().cloned().unwrap_or_default();
    let cfg = match desc.kind.clone() {
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
        desc,
        first_path,
        adapter,
        Some(Arc::clone(&store)),
    ));
    let vb = backend
        .as_any()
        .downcast_ref::<crate::VectorBackend>()
        .expect("vector descriptor must build a VectorBackend");
    IndexBackend::drop_all(vb).await.unwrap();
    assert!(store.get(mkey(920)).await.is_err());
    assert!(store.get(skey(920, 0)).await.is_err());
}

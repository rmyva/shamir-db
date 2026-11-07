//! Crash / corruption recovery tests on the OPEN path (P2 — V2.4).
//!
//! V2.1's `snapshot_tests.rs` proved the codec rejects corrupt input at the
//! `load_snapshot` boundary (corrupted chunk crc, foreign format version,
//! foreign hnsw_rs version). V2.2's `vector_restore_tests.rs` proved the
//! three happy/missing/version-mismatch branches of `restore_on_open`.
//!
//! This file closes the remaining corruption matrix on the OPEN path — the
//! path the engine actually walks on a real restart — and proves each
//! corruption mode falls back to a data-store rebuild WITHOUT a panic, with
//! `rebuild_count == 1`, and with the user's data intact:
//!
//! 1. **truncated chunk** — the payload bytes of one chunk are truncated
//!    (fewer bytes than the crc covers) → per-chunk crc32 mismatch →
//!    `load_snapshot` returns `Corrupt` → `restore_on_open` warns + rebuilds.
//! 2. **corrupt manifest** — the manifest bytes are replaced with garbage
//!    that fails `MetaEnvelope::open` → `read_manifest` / `load_snapshot`
//!    returns `Corrupt` → `restore_on_open` warns + rebuilds.
//! 3. **hnsw_rs version mismatch on the open path** — the sidecar's
//!    `hnsw_rs_version` is substituted with a foreign value (a separate
//!    check from the `format_version` test in `vector_restore_tests`) →
//!    `load_snapshot` returns `VersionMismatch` → `restore_on_open` warns +
//!    rebuilds.
//! 4. **snapshot round-trip, split into two DIFFERENT checks (#1070)** —
//!    the original single test conflated a deterministic structural
//!    guarantee with a statistical quality measurement; codex review flagged
//!    the resulting floor (0.60) as wildly inconsistent with the docs'
//!    marketing claim (recall@10 ≥ 0.90). Splitting them:
//!    - **4a. `snapshot_roundtrip_preserves_graph_topology_exactly`
//!      (fidelity)** — build a graph, query it, dump + reload via
//!      `restore_on_open`, and assert the reloaded graph answers the SAME
//!      queries with EXACTLY the same neighbours/order/distances as before
//!      the dump. Zero tolerance, deterministic — a genuinely corrupt/
//!      mismatched reload fails this immediately and unambiguously, which
//!      is what actually matters for crash-safety.
//!    - **4b. `restart_preserves_recall_at_10_against_brute_force`
//!      (quality)** — recall@10 of a freshly-built 3k-vector graph against
//!      exact brute-force ground truth. Inherently statistical (`hnsw_rs`
//!      0.3.4's construction-time RNG is unseedable — see the test's inline
//!      comment for the full F-68/F-1034 recalibration history and the
//!      cross-platform recall-matrix workflow
//!      (`.github/workflows/hnsw-recall-matrix.yml`), added by #1070 to
//!      measure the actual Linux/macOS/Windows recall distribution rather
//!      than react to isolated single CI failures one at a time).
//!      This test is about HNSW's inherent approximate-search quality, not
//!      about the crash-recovery path per se — 4a is what actually proves
//!      dump/reload didn't corrupt anything.

use crate::adapter::VectorAdapter;
use crate::brute_force::BruteForceAdapter;
use crate::hnsw_adapter::{HnswAdapter, HnswConfig};
use crate::snapshot::{self, SnapshotSidecar};
use crate::vector_backend::VectorBackend;
use shamir_index_core::backend::{IndexBackend, IndexQuery, IndexResult};
use shamir_index_core::descriptor::IndexDescriptor;
use shamir_index_core::kind::{IndexKind, VectorBackendRef, VectorConfig, VectorMetric};
use shamir_index_core::meta_envelope::MetaEnvelope;
use shamir_storage::storage_in_memory::InMemoryStore;
use shamir_storage::types::Store;
use shamir_types::core::interner::{Interner, InternerKey, TouchInd};
use shamir_types::types::common::new_map_wc;
use shamir_types::types::record_id::RecordId;
use shamir_types::types::value::InnerValue;
use smallvec::SmallVec;
use std::sync::Arc;

// ----------------------------------------------------------------------------
// helpers (mirror vector_restore_tests / delta_log_tests for parity)
// ----------------------------------------------------------------------------

const DIM: u32 = 16;

/// Number of vectors for the corruption-fallback tests. > 256 so the HNSW
/// graph path (not the brute-force fallback) is the active search path —
/// a rebuild that round-trips through the graph is the meaningful contract.
const N: usize = 300;

fn intern(i: &Interner, s: &str) -> u64 {
    match i.touch_ind(s).unwrap() {
        TouchInd::Exists(k) | TouchInd::New(k) => k.id(),
    }
}

fn make_rec(interner: &Interner, embedding: &[f64]) -> InnerValue {
    let mut m = new_map_wc(1);
    m.insert(
        InternerKey::new(intern(interner, "embedding")),
        InnerValue::List(embedding.iter().map(|f| InnerValue::F64(*f)).collect()),
    );
    InnerValue::Map(m)
}

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

fn rid(i: usize) -> RecordId {
    let mut a = [0u8; 16];
    a[14] = (i >> 8) as u8;
    a[15] = (i & 0xFF) as u8;
    RecordId(a)
}

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

fn build_adapter(dim: u32, metric: VectorMetric) -> HnswAdapter {
    HnswAdapter::new(
        dim,
        metric,
        HnswConfig {
            max_elements: 100_000,
            m: 16,
            ef_construction: 200,
            ef_search: 50,
            ..Default::default()
        },
    )
}

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

/// Seed `data_store` with N records so a fallback rebuild scan finds them.
/// Every corruption test calls this so the rebuild-after-corruption path has
/// real data to reconstruct the graph from.
async fn seed_data_store(interner: &Interner, data_store: &Arc<dyn Store>) {
    for k in 0..N {
        let v = lcg_vec(DIM as usize, k as u64 * 7 + 1);
        let rec = make_rec(interner, &v.iter().map(|f| *f as f64).collect::<Vec<_>>());
        data_store
            .set(rid(k).0.to_vec().into(), rec.to_bytes().unwrap())
            .await
            .unwrap();
    }
}

/// Build + dump a standalone adapter into `info_store` under the backend's
/// keyspace, returning the keyspace so each test can mutate it.
async fn dump_fresh_snapshot(
    id: u32,
    info_store: &Arc<dyn Store>,
) -> (HnswAdapter, String, Vec<RecordId>) {
    let adapter = build_adapter(DIM, VectorMetric::L2);
    let rids: Vec<RecordId> = (0..N).map(rid).collect();
    for r in &rids {
        let v = lcg_vec(DIM as usize, (r.0[15] as u64) * 7 + 1);
        adapter.upsert(*r, &v).await.unwrap();
    }
    let keyspace = format!("__vec_snap__{}", id);
    snapshot::dump_snapshot(&adapter, info_store, &keyspace)
        .await
        .unwrap();
    (adapter, keyspace, rids)
}

// ----------------------------------------------------------------------------
// 1. truncated chunk → Corrupt → fallback rebuild
// ----------------------------------------------------------------------------

#[tokio::test]
async fn truncated_chunk_falls_back_to_rebuild_without_panic() {
    let i = Interner::new();
    let info_store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let data_store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    seed_data_store(&i, &data_store).await;

    let id: u32 = 10;
    let (_adapter, keyspace, _rids) = dump_fresh_snapshot(id, &info_store).await;

    // Truncate one graph chunk's PAYLOAD bytes: decode the ChunkHeader, chop
    // the payload Vec in half, re-encode WITH THE SAME crc32. The payload no
    // longer matches the recorded crc → per-chunk crc check fails at load
    // with Corrupt. (A bare truncation of the on-wire bytes would instead
    // fail bincode deserialisation — also Corrupt, but a different message;
    // we pick the payload-truncation route so the failure is unambiguous and
    // robust to bincode's length-prefix encoding.)
    use serde::{Deserialize, Serialize};
    #[derive(Serialize, Deserialize)]
    struct ChunkHeaderWire {
        idx: u32,
        crc32: u32,
        bytes: Vec<u8>,
    }
    let chunk_key = snapshot::chunk_key_for_test(&keyspace, 0, "graph", 0);
    let original = info_store.get(chunk_key.clone()).await.unwrap();
    let mut header: ChunkHeaderWire = bincode::deserialize(&original).unwrap();
    let half = header.bytes.len() / 2;
    if half == 0 {
        // The first graph chunk for 300 small-dim vectors can be small; if
        // truncating to half would leave an empty payload, drop just one
        // byte so the crc still mismatches.
        header.bytes.pop();
    } else {
        header.bytes.truncate(half);
    }
    let truncated = bincode::serialize(&header).unwrap();
    info_store
        .set(chunk_key, bytes::Bytes::from(truncated))
        .await
        .unwrap();

    // Open — restore_on_open must warn + fall back to a full rebuild.
    let backend = make_backend(&i, id, DIM, VectorMetric::L2);
    backend
        .restore_on_open(Arc::clone(&info_store), Arc::clone(&data_store))
        .await
        .unwrap();

    assert_eq!(
        backend.rebuild_count(),
        1,
        "a truncated-chunk snapshot must fall back to exactly one full rebuild"
    );

    // The rebuild scanned the data_store, so the graph is populated from the
    // user's data — the corruption did NOT lose anything.
    let q = lcg_vec(DIM as usize, 7);
    let top = topk_ids(&backend, &q, 5).await;
    assert_eq!(top.len(), 5, "fallback rebuild must populate the graph");
    let inserted: shamir_collections::TFxSet<RecordId> = (0..N).map(rid).collect();
    for r in &top {
        assert!(
            inserted.contains(r),
            "rebuild returned an unknown rid {:?}",
            r
        );
    }
}

// ----------------------------------------------------------------------------
// 2. corrupt manifest → Corrupt → fallback rebuild
// ----------------------------------------------------------------------------

#[tokio::test]
async fn corrupt_manifest_falls_back_to_rebuild_without_panic() {
    let i = Interner::new();
    let info_store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let data_store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    seed_data_store(&i, &data_store).await;

    let id: u32 = 11;
    let (_adapter, keyspace, _rids) = dump_fresh_snapshot(id, &info_store).await;

    // Overwrite the manifest with bytes that cannot decode as a MetaEnvelope.
    // `read_manifest` → `MetaEnvelope::open` → `MetaError::Decode` → mapped
    // to `SnapshotError::Corrupt`. (A bad-magic payload would map to
    // `Corrupt` too; we pick pure garbage so the failure is unconditional
    // regardless of bincode quirks.)
    let manifest_k = format!("{}.manifest", keyspace);
    info_store
        .set(
            bytes::Bytes::from(manifest_k).into(),
            bytes::Bytes::from_static(b"not-a-valid-snapshot-manifest-payload-garbage"),
        )
        .await
        .unwrap();

    let backend = make_backend(&i, id, DIM, VectorMetric::L2);
    backend
        .restore_on_open(Arc::clone(&info_store), Arc::clone(&data_store))
        .await
        .unwrap();

    assert_eq!(
        backend.rebuild_count(),
        1,
        "a corrupt manifest must fall back to exactly one full rebuild"
    );

    let q = lcg_vec(DIM as usize, 11);
    let top = topk_ids(&backend, &q, 5).await;
    assert_eq!(top.len(), 5, "fallback rebuild must populate the graph");
    let inserted: shamir_collections::TFxSet<RecordId> = (0..N).map(rid).collect();
    for r in &top {
        assert!(
            inserted.contains(r),
            "rebuild returned an unknown rid {:?}",
            r
        );
    }
}

#[tokio::test]
async fn corrupt_metadata_rebuild_then_automatic_snapshot_preserves_unverified_keys() {
    for case in [
        "missing sidecar",
        "corrupt sidecar",
        "corrupt integrity",
        "legacy corrupt sidecar",
        "legacy missing sidecar",
    ] {
        let i = Interner::new();
        let info_store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
        let data_store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
        seed_data_store(&i, &data_store).await;
        let id = 71;
        let (_, keyspace, _) = dump_fresh_snapshot(id, &info_store).await;
        let old_graph = snapshot::chunk_key_for_test(&keyspace, 0, "graph", 0);
        let old_graph_bytes = info_store.get(old_graph.clone()).await.unwrap();
        let sidecar = snapshot::sidecar_key_for_test(&keyspace, 0);
        let orphan = snapshot::sidecar_key_for_test(&keyspace, 1);
        let orphan_bytes = bytes::Bytes::from_static(b"unverified orphan");
        info_store
            .set(orphan.clone(), orphan_bytes.clone())
            .await
            .unwrap();
        if case.starts_with("legacy ") {
            info_store
                .remove(bytes::Bytes::from(format!("{keyspace}.integrity")).into())
                .await
                .unwrap();
        }
        match case {
            "missing sidecar" | "legacy missing sidecar" => {
                info_store.remove(sidecar.clone()).await.unwrap()
            }
            "corrupt sidecar" | "legacy corrupt sidecar" => info_store
                .set(sidecar.clone(), bytes::Bytes::from_static(b"broken"))
                .await
                .unwrap(),
            "corrupt integrity" => info_store
                .set(
                    bytes::Bytes::from(format!("{keyspace}.integrity")).into(),
                    bytes::Bytes::from_static(b"broken"),
                )
                .await
                .unwrap(),
            _ => unreachable!(),
        };
        let old_sidecar = match info_store.get(sidecar.clone()).await {
            Ok(bytes) => Some(bytes),
            Err(shamir_storage::error::DbError::NotFound(_)) => None,
            Err(error) => panic!("sidecar read failed: {error}"),
        };
        assert!(matches!(
            snapshot::read_manifest(&info_store, &keyspace).await,
            Err(snapshot::SnapshotError::Corrupt(_))
        ));
        let backend = make_backend(&i, id, DIM, VectorMetric::L2);
        backend
            .restore_on_open(Arc::clone(&info_store), Arc::clone(&data_store))
            .await
            .unwrap();
        assert_eq!(backend.rebuild_count(), 1, "{case}");

        let inserted = (rid(N + 1), lcg_vec(DIM as usize, 98765));
        let idx = backend
            .append_vector_delta(&info_store, std::slice::from_ref(&inserted), &[])
            .await
            .unwrap()
            .unwrap();
        backend
            .apply_staged_vectors(std::slice::from_ref(&inserted))
            .await
            .unwrap();
        backend.mark_delta_applied(idx);
        let delta_key = bytes::Bytes::from(format!("{keyspace}.delta.{idx:010}"));
        let delta = info_store.get(delta_key.clone().into()).await.unwrap();
        let boundary = backend.applied_prefix();
        backend.set_snapshot_threshold_for_test(1);
        backend.trigger_snapshot_check(&info_store);
        assert!(backend.snapshot_in_flight_for_test());
        tokio::time::timeout(std::time::Duration::from_secs(60), async {
            while backend.snapshot_in_flight_for_test() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("background snapshot did not finish");

        let manifest = snapshot::read_manifest(&info_store, &keyspace)
            .await
            .unwrap();
        assert_eq!(manifest.gen, 2, "{case}");
        assert_eq!(manifest.delta_applied_upto, boundary, "{case}");
        assert_eq!(info_store.get(old_graph).await.unwrap(), old_graph_bytes);
        assert_eq!(info_store.get(orphan).await.unwrap(), orphan_bytes);
        match old_sidecar {
            Some(bytes) => assert_eq!(info_store.get(sidecar).await.unwrap(), bytes),
            None => assert!(matches!(
                info_store.get(sidecar).await,
                Err(shamir_storage::error::DbError::NotFound(_))
            )),
        }
        assert_eq!(info_store.get(delta_key.into()).await.unwrap(), delta);
        snapshot::load_snapshot(&info_store, &keyspace)
            .await
            .unwrap();
        let restarted = make_backend(&i, id, DIM, VectorMetric::L2);
        restarted
            .restore_on_open(Arc::clone(&info_store), Arc::clone(&data_store))
            .await
            .unwrap();
        assert_eq!(restarted.rebuild_count(), 0, "{case}");
        assert_eq!(topk_ids(&restarted, &inserted.1, 1).await, vec![inserted.0]);
    }
}

// ----------------------------------------------------------------------------
// 3. hnsw_rs version mismatch on the open path → VersionMismatch → rebuild
// ----------------------------------------------------------------------------

#[tokio::test]
async fn hnsw_rs_0_3_4_version_mismatch_on_open_falls_back_to_rebuild() {
    let i = Interner::new();
    let info_store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let data_store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    seed_data_store(&i, &data_store).await;

    let id: u32 = 12;
    let (_adapter, keyspace, _rids) = dump_fresh_snapshot(id, &info_store).await;

    // Substitute the sidecar's `hnsw_rs_version`. This is a SEPARATE check
    // from `format_version` (exercised in `vector_restore_tests`): the
    // format-version check fires early in `load_snapshot`; the hnsw_rs check
    // fires AFTER chunks are fetched+verified, just before handing the dump
    // to the loader (snapshot.rs:621). Both must route through
    // `restore_on_open`'s warn+rebuild arm.
    let manifest_bytes = info_store
        .get(bytes::Bytes::from(format!("{}.manifest", keyspace)).into())
        .await
        .unwrap();
    let manifest: snapshot::SnapshotManifest = MetaEnvelope::open(&manifest_bytes).unwrap();
    let sidecar_k = snapshot::sidecar_key_for_test(&keyspace, manifest.gen);
    let sidecar_bytes = info_store.get(sidecar_k.clone()).await.unwrap();
    let mut sidecar: SnapshotSidecar = MetaEnvelope::open(&sidecar_bytes).unwrap();
    sidecar.hnsw_rs_version = "0.3.4".to_string();
    let tampered = MetaEnvelope::new(sidecar).encode().unwrap();
    info_store
        .set(sidecar_k, bytes::Bytes::from(tampered))
        .await
        .unwrap();
    // Reach the version validation rather than a stale metadata checksum.
    info_store
        .remove(bytes::Bytes::from(format!("{}.integrity", keyspace)).into())
        .await
        .unwrap();

    assert!(matches!(
        snapshot::load_snapshot(&info_store, &keyspace).await,
        Err(snapshot::SnapshotError::VersionMismatch(_))
    ));
    let backend = make_backend(&i, id, DIM, VectorMetric::L2);
    backend
        .restore_on_open(Arc::clone(&info_store), Arc::clone(&data_store))
        .await
        .unwrap();

    assert_eq!(
        backend.rebuild_count(),
        1,
        "an hnsw_rs-version-mismatched snapshot must fall back to a full rebuild"
    );

    let q = lcg_vec(DIM as usize, 13);
    let top = topk_ids(&backend, &q, 5).await;
    assert_eq!(top.len(), 5, "fallback rebuild must populate the graph");
}

// ----------------------------------------------------------------------------
// 4a. snapshot round-trip preserves graph topology EXACTLY (fidelity —
//     deterministic, not statistical)
// ----------------------------------------------------------------------------
//
// #1070: this is the check the OLD single `restart_preserves_recall_at_10_
// against_brute_force` test was half-heartedly trying to be, per its own
// inline comment pointing at the adjacent `rebuild_count == 1` corruption
// tests as "the right signal for actual corruption". Dump+reload via
// `restore_on_open`'s snapshot-hit path (`rebuild_count == 0`, confirmed
// below) reconstructs the SAME graph object that was dumped — no new nodes,
// no re-run of `LayerGenerator`'s unseedable RNG, no re-scheduled
// `parallel_insert`. Querying that reloaded graph must therefore return
// IDENTICAL neighbours, in the SAME order, at IDENTICAL distances to
// querying the SAME graph before it was ever dumped — this has NOTHING to
// do with HNSW's approximate quality (that's construction-time variance,
// see the quality test below); it is purely "did serialization/
// deserialization preserve the graph". A genuinely corrupt/mismatched
// reload does not "recall slightly worse" — it silently reconstructs a
// DIFFERENT graph, which this exact-match assertion (zero tolerance) is
// far more sensitive to than any statistical recall floor could be.

// ----------------------------------------------------------------------------
// 4b. e2e restart preserves recall@10 (3k vectors) — quality, statistical
// ----------------------------------------------------------------------------

/// Number of vectors for the e2e recall test. 3k is large enough that a
/// full-scan rebuild would be visibly slower than a snapshot load (the
/// cold-start bench quantifies the gap), and that the HNSW graph path (not
/// brute-force) is active.
// 3k > BRUTE_FORCE_MAX (256) so the HNSW graph path is exercised, and recall@10
// vs exact brute-force is statistically meaningful — while keeping the test off
// the SLOW list. The 100K/1M scale is covered by the cold-start bench, not here.
// Build via `upsert_batch` (the production path), NOT a serial `upsert` loop.
const N_E2E: usize = 3_000;

/// #1070: builds a fresh HNSW graph, dumps it, reloads it into a fresh
/// backend via `restore_on_open`, and asserts the reloaded graph answers the
/// SAME fixed query set with EXACTLY the same neighbours/order/distances as
/// the pre-dump graph. Zero tolerance — this is a deterministic structural
/// check, not a statistical one (see the module doc and the sibling quality
/// test below for why these two concerns were split).
#[tokio::test]
async fn snapshot_roundtrip_preserves_graph_topology_exactly() {
    let adapter = build_adapter(DIM, VectorMetric::L2);
    let build_batch: Vec<(RecordId, Vec<f32>)> = (0..N_E2E)
        .map(|k| (rid(k), lcg_vec(DIM as usize, k as u64 * 7 + 1)))
        .collect();
    adapter.upsert_batch(&build_batch).await.unwrap();

    let queries: Vec<Vec<f32>> = (0..50u64).map(|s| lcg_vec(DIM as usize, s + 99)).collect();
    let k: u32 = 10;

    // Baseline: query the in-memory graph BEFORE any dump/reload.
    let mut baseline: Vec<Vec<(RecordId, f32)>> = Vec::with_capacity(queries.len());
    for q in &queries {
        let r = adapter
            .search(q, k, crate::SearchOpts::with_ef_search(64), None)
            .await
            .unwrap();
        baseline.push(r);
    }

    let info_store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let data_store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let id: u32 = 21;
    let keyspace = format!("__vec_snap__{}", id);
    snapshot::dump_snapshot(&adapter, &info_store, &keyspace)
        .await
        .unwrap();

    let i = Interner::new();
    let backend = make_backend(&i, id, DIM, VectorMetric::L2);
    backend
        .restore_on_open(Arc::clone(&info_store), Arc::clone(&data_store))
        .await
        .unwrap();
    assert_eq!(
        backend.rebuild_count(),
        0,
        "a valid snapshot must load without a full rebuild — a rebuild would \
         re-run construction (fresh RNG draws) and this exact-match \
         assertion would be comparing two DIFFERENT graphs, not proving \
         round-trip fidelity"
    );

    for (q, expected) in queries.iter().zip(baseline.iter()) {
        let got = backend
            .lookup(IndexQuery::Vector {
                vec: q.clone(),
                k,
                opts: crate::SearchOpts::with_ef_search(64),
            })
            .await
            .unwrap();
        let got_pairs: Vec<(RecordId, f32)> = match got {
            IndexResult::Ranked(ranked) => ranked,
            _ => panic!("expected Ranked result"),
        };
        assert_eq!(
            got_pairs.len(),
            expected.len(),
            "reloaded graph returned a different result COUNT than the \
             pre-dump graph for the same query"
        );
        for ((got_rid, got_score), (exp_rid, exp_score)) in got_pairs.iter().zip(expected.iter()) {
            assert_eq!(
                got_rid, exp_rid,
                "reloaded graph must return the SAME neighbour in the SAME \
                 rank position as the pre-dump graph — any deviation means \
                 the snapshot round-trip altered the graph topology, not \
                 just approximate-search variance"
            );
            assert!(
                (got_score - exp_score).abs() < 1e-5,
                "reloaded graph's distance for {got_rid:?} ({got_score}) must \
                 match the pre-dump graph's distance ({exp_score}) within \
                 floating-point round-trip tolerance"
            );
        }
    }
}

#[tokio::test]
async fn restart_preserves_recall_at_10_against_brute_force() {
    let i = Interner::new();
    let info_store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let data_store: Arc<dyn Store> = Arc::new(InMemoryStore::new());

    let id: u32 = 20;
    let keyspace = format!("__vec_snap__{}", id);

    // ---- phase 1: build + dump a 10k-vector graph ----------------------
    let adapter = build_adapter(DIM, VectorMetric::L2);
    let build_batch: Vec<(RecordId, Vec<f32>)> = (0..N_E2E)
        .map(|k| (rid(k), lcg_vec(DIM as usize, k as u64 * 7 + 1)))
        .collect();
    adapter.upsert_batch(&build_batch).await.unwrap();
    snapshot::dump_snapshot(&adapter, &info_store, &keyspace)
        .await
        .unwrap();

    // Seed the data_store too so a fallback rebuild (if it ever fired)
    // would have rows. The snapshot-hit path must NOT read it.
    for k in 0..N_E2E {
        let v = lcg_vec(DIM as usize, k as u64 * 7 + 1);
        let rec = make_rec(&i, &v.iter().map(|f| *f as f64).collect::<Vec<_>>());
        data_store
            .set(rid(k).0.to_vec().into(), rec.to_bytes().unwrap())
            .await
            .unwrap();
    }

    // A fixed query set (distinct seed from the dataset). Ground truth is
    // the EXACT top-10 via a brute-force adapter loaded from the SAME
    // vectors — the same comparator production uses as its fallback.
    let queries: Vec<Vec<f32>> = (0..50u64).map(|s| lcg_vec(DIM as usize, s + 99)).collect();
    let bf = BruteForceAdapter::new(DIM, VectorMetric::L2);
    bf.upsert_batch(&build_batch).await.unwrap();

    // ---- phase 2: "restart" — fresh backend, restore_on_open -----------
    let backend = make_backend(&i, id, DIM, VectorMetric::L2);
    backend
        .restore_on_open(Arc::clone(&info_store), Arc::clone(&data_store))
        .await
        .unwrap();
    // The snapshot loaded cleanly — no fallback rebuild ran.
    assert_eq!(
        backend.rebuild_count(),
        0,
        "a valid 10k snapshot must load without a full rebuild"
    );

    // ---- phase 3: recall@10 against brute-force ground truth -----------
    let k: u32 = 10;
    let mut total_hits = 0u64;
    let mut total_queries = 0u64;
    for q in &queries {
        let exact: shamir_collections::TFxSet<RecordId> = bf
            .search(q, k, crate::SearchOpts::with_ef_search(64), None)
            .await
            .unwrap()
            .into_iter()
            .map(|(r, _)| r)
            .collect();
        let got = topk_ids(&backend, q, k).await;
        for r in &got {
            if exact.contains(r) {
                total_hits += 1;
            }
        }
        total_queries += 1;
    }

    let recall = total_hits as f64 / (total_queries as f64 * k as f64);
    // #1070: machine-parseable line for the cross-platform recall-matrix
    // workflow (`.github/workflows/hnsw-recall-matrix.yml`) to grep out of
    // CI logs — that workflow passes `--success-output=immediate` on its
    // nextest invocation so this prints even when the assertion below
    // passes, not just on failure (nextest's own default only captures
    // stdout and shows it on a FAILING run).
    println!("#1070 recall_at_10=3k:{recall:.4}");
    // F-68 (#895, cluster A) derivation for the 0.75 floor below (was 0.90):
    //
    // Root cause: `hnsw_rs` 0.3.4's `LayerGenerator::new` seeds via
    // `StdRng::from_os_rng()` (confirmed by reading `hnsw_rs-0.3.4/src/hnsw.rs`
    // directly) — genuinely unseedable, fresh OS entropy every graph build,
    // so recall@10 against exact brute force varies run to run with NO
    // caller-side lever to pin it. The CI failure this floor is being fixed
    // for (macOS `cargo test lib`, run 30515165549) measured recall@10 =
    // 0.800, below the old 0.90 floor.
    //
    // Empirical distribution measured THIS session (temporarily
    // instrumented this test to print recall on every run, then removed the
    // instrumentation): 55 runs total on this Windows dev box at these EXACT
    // params (DIM=16, N_E2E=3000, same query set) — min 0.970, max 0.998,
    // mean 0.985, ZERO runs below 0.90, let alone below the 0.800 macOS CI
    // observed. This dev box could not reproduce the CI-observed value even
    // once in 55 tries, which itself is informative: it corroborates the
    // brief's own hypothesis that macOS CI runners (far fewer vCPUs than a
    // dev box) produce measurably worse-connected HNSW graphs under the same
    // build parameters — `hnsw_rs`'s graph quality is a function of both the
    // unseedable RNG AND the actual concurrent-insertion interleaving
    // (`parallel_insert` is rayon-`par_iter`-scheduled), which a low-core
    // machine explores differently than a high-core one. This mechanism
    // cannot be fixed from caller code (no seed hook, no way to force
    // uniform core counts across CI runners); the honest fix is a floor
    // that tolerates the CI-observed value with real margin, not one tuned
    // to this machine's much tighter local distribution.
    //
    // Floor set at 0.75: comfortably BELOW the one real CI observation
    // (0.800, so that exact recurrence passes with margin), while staying
    // well ABOVE the "corrupted graph" territory this assertion actually
    // guards against (a genuinely corrupt/mismatched snapshot reload craters
    // recall far below 0.5, per the adjacent corruption-fallback tests in
    // `crash_recovery_tests.rs` and `snapshot_tests.rs`, which assert
    // `rebuild_count == 1` for real corruption rather than a recall
    // threshold at all). 0.75 cannot be satisfied by a broken dump/reload
    // path — it still requires the large majority of the 10 nearest
    // neighbours to survive the round-trip — while giving 15 points of
    // margin under the worst value seen in the wild, not just the values
    // this one machine happens to produce.
    //
    // F-1034 (#1034) recalibration from 0.75 to 0.60 (second CI observation):
    //
    // A SECOND, WORSE CI observation on ubuntu-latest measured recall@10 =
    // 0.638, below the 0.75 floor. Local instrumentation THIS session (55
    // runs on a Windows dev box at these EXACT params: DIM=16, N_E2E=3000,
    // same query set) produced min 0.986, max 0.990, mean 0.989, ZERO runs
    // below 0.90 or anywhere near 0.638. This dev box could not reproduce
    // the CI-observed value even once in 55 tries, which mirrors the F-68
    // pattern.
    //
    // A vCPU-count explanation was considered and REJECTED after checking
    // GitHub's actual published runner specs (github-hosted-runners
    // reference, public-repo tier — this repo is public, verified via `gh
    // repo view`): `ubuntu-latest` gets 4 vCPU/16 GB, `macos-latest` gets
    // only 3 vCPU/7 GB — ubuntu has MORE cores than macOS, not fewer, which
    // is the OPPOSITE of what a "fewer cores → worse graph" story would
    // need to explain "ubuntu (0.638) worse than macOS (0.800)". The exact
    // mechanism behind the ubuntu-vs-macOS gap is therefore NOT confirmed —
    // could be scheduler behavior, container/VM virtualization differences,
    // or something else `hnsw_rs`'s unseedable-RNG + rayon-scheduled
    // insertion is sensitive to that isn't captured by raw core count. Do
    // not resurrect the vCPU explanation without re-verifying it.
    //
    // Options considered: (a) lower floor, (b) retry/average, (c) per-OS
    // floor. Chose (a) because: (i) the floor only needs margin below the
    // worst KNOWN observation, regardless of the unconfirmed root cause,
    // (ii) adjacent corruption tests already catch catastrophic failures
    // via `rebuild_count == 1` assertions, so a lower recall floor here
    // does not lose meaningful regression detection, (iii) options (b) and
    // (c) add wall-clock cost or complexity that an unconfirmed mechanism
    // doesn't yet justify — (c) in particular would need a real, verified
    // per-OS causal story, which this investigation did not find.
    //
    // Floor recalibrated to 0.60: comfortably BELOW the new worst CI
    // observation (0.638, so that exact recurrence passes with 6-point
    // margin), while staying well ABOVE the "corrupted graph" territory
    // (< 0.5). 0.60 still requires 60% of the 10 nearest neighbours to
    // survive the snapshot round-trip — a genuine corruption craters
    // recall far below this, as the adjacent `rebuild_count == 1` tests
    // prove is the correct signal for actual corruption.
    //
    // F-1070 (#1070) recalibration from 0.60 to 0.90 — real cross-platform
    // matrix, not another single-run reaction:
    //
    // #1070 also split this test's OLD job (fidelity + quality conflated)
    // into two — `snapshot_roundtrip_preserves_graph_topology_exactly`
    // (above) now catches genuine dump/reload corruption with ZERO
    // tolerance, which means THIS test's floor no longer has to double as
    // a corruption tripwire; it only needs to reflect actual HNSW
    // construction-quality variance. That freed it up to be recalibrated
    // from real data instead of another isolated CI-failure reaction.
    //
    // `.github/workflows/hnsw-recall-matrix.yml` (manual `workflow_dispatch`)
    // ran this exact test 20 times on EACH of ubuntu-latest, macos-latest,
    // and windows-latest (60 runs total, same DIM=16/N_E2E=3000/query-set
    // params as every prior observation in this comment) — run
    // 31472514970:
    //   ubuntu-latest:  min 0.968, max 0.992, mean 0.983 (n=20)
    //   windows-latest: min 0.970, max 0.998, mean 0.986 (n=20)
    //   macos-latest:   min 0.978, max 0.996, mean 0.987 (n=20)
    //   overall worst single run across all 60: 0.968.
    //
    // This is drastically tighter and higher than either historical CI
    // failure (0.800, 0.638) that drove the floor down twice before —
    // neither reproduced even once in 60 fresh cross-platform runs. Those
    // two observations were real (not fabricated), but with 60 fresh data
    // points now available, they read as one-off anomalies (a noisy
    // runner, a transient scheduling artifact, or a code state since
    // changed) rather than the steady-state distribution. A 0.60 floor
    // tuned to tolerate them is now needlessly loose by roughly 35 points
    // against the observed worst case.
    //
    // Floor recalibrated to 0.90: restores the ORIGINAL pre-F-68 value —
    // which the docs (`docs/guide-docs/guide/06-search.md`) had continued
    // to claim the whole time, even while the test's own floor drifted
    // down to 0.60 underneath it — and sits with real margin (6.8 points)
    // below the fresh worst-observed value (0.968), backed by 60 runs
    // across 3 platforms rather than a single machine's local
    // instrumentation. If a genuine platform-specific regression
    // resurfaces, re-run the matrix workflow before touching this floor
    // again — don't drop it from a single new CI failure the way F-68 and
    // F-1034 did.
    assert!(
        recall >= 0.90,
        "recall@10 after restart ({recall:.3}) below 0.90 floor — \
         snapshot reload may have corrupted the graph, or re-run \
         .github/workflows/hnsw-recall-matrix.yml before recalibrating"
    );
}

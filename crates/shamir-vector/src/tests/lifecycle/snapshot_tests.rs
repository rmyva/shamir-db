//! Snapshot codec tests (P2 — V2.1).
//!
//! Drives `snapshot::dump_snapshot` / `load_snapshot` against an in-memory
//! `Store`, asserting: top-k fidelity across a dump/load round-trip,
//! crc-mismatch detection, version-mismatch detection, tombstone survival,
//! and rid_map / next_id restoration.
//!
//! These are the codec's contract tests; the startup integration (#401) and
//! the delta-log / generation flip (#402) are out of scope.

use crate::adapter::{SearchOpts, VectorAdapter};
use crate::hnsw_adapter::{HnswAdapter, HnswConfig};
use crate::snapshot::{
    self, chunk_key_for_test, flip_generation, load_snapshot, read_manifest, OldGenerationChunks,
    SnapshotError, SnapshotManifest, SnapshotSidecar, HNSW_RS_VERSION, SNAPSHOT_FORMAT_VERSION,
};
use bytes::Bytes;
use shamir_collections::TFxSet;
use shamir_index_core::kind::VectorMetric;
use shamir_index_core::meta_envelope::MetaEnvelope;
use shamir_storage::storage_in_memory::InMemoryStore;
use shamir_storage::types::Store;
use shamir_types::types::record_id::RecordId;
use std::sync::Arc;

/// Keyspace tag used by every test — matches what the engine would pass.
const KEYSPACE: &str = "vsnap.test";

/// Build an in-memory `Store` arc.
fn mem_store() -> Arc<dyn Store> {
    Arc::new(InMemoryStore::new())
}

/// Deterministic LCG pseudo-random vector generator (mirrors the helper in
/// `hnsw_rs_contract_tests.rs`).
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

/// `RecordId` from a single-byte label — keeps tests compact.
fn rid(n: u8) -> RecordId {
    let mut a = [0u8; 16];
    a[15] = n;
    RecordId(a)
}

/// Build an adapter pre-loaded with `n` LCG vectors. `n` is chosen > 256
/// everywhere it matters so the HNSW graph path is exercised (not the
/// exact brute-force fallback).
fn build_adapter(
    n: usize,
    dim: usize,
    metric: VectorMetric,
) -> (HnswAdapter, Vec<(RecordId, Vec<f32>)>) {
    let adapter = HnswAdapter::new(
        dim as u32,
        metric,
        HnswConfig {
            max_elements: n + 32,
            ef_construction: 200,
            ef_search: 64,
            ..Default::default()
        },
    );
    let mut items = Vec::with_capacity(n);
    // Use distinct rids in the 0..255 + extended range so we exercise both
    // rid(0..=255) and a 2-byte rid encoding.
    for i in 0..n {
        let mut a = [0u8; 16];
        a[14] = (i >> 8) as u8;
        a[15] = (i & 0xFF) as u8;
        let r = RecordId(a);
        let v = lcg_vec(dim, i as u64 * 7 + 1);
        items.push((r, v));
    }
    // Return the adapter + items; the caller upserts in its own async body
    // (each helper is called from inside a #[tokio::test]).
    (adapter, items)
}

// ============================================================================
// 1. round-trip preserves top-k
// ============================================================================

#[tokio::test]
async fn round_trip_preserves_topk() {
    let dim = 16usize;
    let n = 300usize; // > 256 → HNSW graph path, not brute-force fallback
    let store = mem_store();
    let (adapter, items) = build_adapter(n, dim, VectorMetric::L2);
    for (r, v) in &items {
        adapter.upsert(*r, v).await.unwrap();
    }

    // Queries + their pre-dump top-k id sets.
    let queries: Vec<Vec<f32>> = (0..5u64)
        .map(|s| lcg_vec(dim, s.wrapping_add(99)))
        .collect();
    let ef = 64;

    let mut before: Vec<TFxSet<RecordId>> = Vec::with_capacity(queries.len());
    for q in &queries {
        let r = adapter
            .search(q, 10, SearchOpts::with_ef_search(ef as u32), None)
            .await
            .unwrap();
        before.push(r.into_iter().map(|(rid, _)| rid).collect());
    }

    // Dump → load.
    snapshot::dump_snapshot(&adapter, &store, KEYSPACE)
        .await
        .unwrap();
    let loaded = load_snapshot(&store, KEYSPACE).await.unwrap();

    // top-k id sets must be IDENTICAL pre/post — dump→load of the SAME graph
    // is a fidelity test (hnsw_rs uses an unseedable RNG so two FRESH builds
    // can differ, but a reload of one dump must not). Mirrors the contract
    // test `file_dump_load_roundtrip_preserves_topk`.
    for (i, q) in queries.iter().enumerate() {
        let after = loaded
            .search(q, 10, SearchOpts::with_ef_search(ef as u32), None)
            .await
            .unwrap();
        let after_set: TFxSet<RecordId> = after.into_iter().map(|(r, _)| r).collect();
        assert_eq!(
            before[i], after_set,
            "top-10 id set diverged after dump/load for query #{}",
            i
        );
    }

    // Sanity: the loaded adapter retained the right number of points.
    assert_eq!(
        loaded.len(),
        adapter.len(),
        "loaded adapter live-count wrong"
    );
}

// ============================================================================
// 2. corrupted chunk crc → Err(Corrupt)
// ============================================================================

#[tokio::test]
async fn corrupted_chunk_crc_yields_corrupt_error() {
    let dim = 8usize;
    let n = 300usize;
    let store = mem_store();
    let (adapter, items) = build_adapter(n, dim, VectorMetric::L2);
    for (r, v) in &items {
        adapter.upsert(*r, v).await.unwrap();
    }
    snapshot::dump_snapshot(&adapter, &store, KEYSPACE)
        .await
        .unwrap();

    // Corrupt ONE graph chunk: decode its `ChunkHeader`, BUMP the stored
    // `crc32` field so it no longer matches `crc32fast::hash(bytes)`,
    // re-encode, and put it back. The per-chunk crc check at load must
    // catch it. This is the cleanest corruption — we change ONLY the crc
    // field, so the payload bytes are byte-identical to the original, and
    // the mismatch is unambiguous (no chance of a coincidental match).
    use serde::{Deserialize, Serialize};
    #[derive(Serialize, Deserialize)]
    struct ChunkHeaderWire {
        idx: u32,
        crc32: u32,
        bytes: Vec<u8>,
    }
    let chunk_key = snapshot::chunk_key_for_test(KEYSPACE, 0, "graph", 0);
    let original = store.get(chunk_key.clone()).await.unwrap();
    let mut header: ChunkHeaderWire = bincode::deserialize(&original).unwrap();
    // Force a guaranteed mismatch: pick a crc value that differs from the
    // correct one (computed from the unchanged payload bytes).
    let correct_crc = crc32fast::hash(&header.bytes);
    let mut bad_crc = correct_crc.wrapping_add(1);
    if bad_crc == correct_crc {
        bad_crc = correct_crc.wrapping_add(2);
    }
    header.crc32 = bad_crc;
    let corrupted = bincode::serialize(&header).unwrap();
    store.set(chunk_key, Bytes::from(corrupted)).await.unwrap();

    // Use `match` (not `.unwrap_err()`) because `HnswAdapter` intentionally
    // does not implement `Debug` (its `scc::HashMap` cursor types and the
    // `Hnsw` graph are not `Debug`).
    let err = match load_snapshot(&store, KEYSPACE).await {
        Ok(_) => panic!("expected Corrupt error, load succeeded"),
        Err(e) => e,
    };
    assert!(
        matches!(err, SnapshotError::Corrupt(ref msg) if msg.contains("crc32")),
        "expected Corrupt(crc32 ...) error, got {err:?}"
    );
}

// ============================================================================
// 3. foreign format version → Err(VersionMismatch)
// ============================================================================

#[tokio::test]
async fn foreign_format_version_yields_version_mismatch() {
    let dim = 8usize;
    let n = 300usize;
    let store = mem_store();
    let (adapter, items) = build_adapter(n, dim, VectorMetric::L2);
    for (r, v) in &items {
        adapter.upsert(*r, v).await.unwrap();
    }
    snapshot::dump_snapshot(&adapter, &store, KEYSPACE)
        .await
        .unwrap();

    // Tamper with the SIDEcar's `format_version` (not the envelope version —
    // that path is exercised by the envelope's own BadMagic check). We
    // decode → bump → re-encode under a NEW MetaEnvelope so the envelope
    // itself stays valid.
    let sidecar_k = snapshot::sidecar_key_for_test(KEYSPACE, 0);
    let sidecar_bytes = store.get(sidecar_k.clone()).await.unwrap();
    let mut sidecar: SnapshotSidecar = MetaEnvelope::open(&sidecar_bytes)
        .map_err(|e| e.to_string())
        .unwrap();
    // Must be a value DIFFERENT from the supported one to trip the check.
    let bad_version = SNAPSHOT_FORMAT_VERSION.wrapping_add(1);
    sidecar.format_version = bad_version;
    let new_bytes = MetaEnvelope::new(sidecar)
        .encode()
        .map_err(|e| e.to_string())
        .unwrap();
    store.set(sidecar_k, Bytes::from(new_bytes)).await.unwrap();
    // Exercise the semantic version check as legacy metadata, without a companion.
    store
        .remove(Bytes::from(format!("{KEYSPACE}.integrity")).into())
        .await
        .unwrap();

    let err = match load_snapshot(&store, KEYSPACE).await {
        Ok(_) => panic!("expected VersionMismatch error, load succeeded"),
        Err(e) => e,
    };
    assert!(
        matches!(err, SnapshotError::VersionMismatch(ref m) if m.contains("format version")),
        "expected VersionMismatch(format version) error, got {err:?}"
    );
}

// ============================================================================
// 3b. foreign hnsw_rs version → Err(VersionMismatch)
// ============================================================================

#[tokio::test]
async fn foreign_hnsw_rs_version_yields_version_mismatch() {
    let dim = 8usize;
    let n = 300usize;
    let store = mem_store();
    let (adapter, items) = build_adapter(n, dim, VectorMetric::L2);
    for (r, v) in &items {
        adapter.upsert(*r, v).await.unwrap();
    }
    snapshot::dump_snapshot(&adapter, &store, KEYSPACE)
        .await
        .unwrap();

    let sidecar_k = snapshot::sidecar_key_for_test(KEYSPACE, 0);
    let sidecar_bytes = store.get(sidecar_k.clone()).await.unwrap();
    let mut sidecar: SnapshotSidecar = MetaEnvelope::open(&sidecar_bytes)
        .map_err(|e| e.to_string())
        .unwrap();
    sidecar.hnsw_rs_version = "0.0.0-fake".to_string();
    let new_bytes = MetaEnvelope::new(sidecar)
        .encode()
        .map_err(|e| e.to_string())
        .unwrap();
    store.set(sidecar_k, Bytes::from(new_bytes)).await.unwrap();
    // Exercise the semantic version check as legacy metadata, without a companion.
    store
        .remove(Bytes::from(format!("{KEYSPACE}.integrity")).into())
        .await
        .unwrap();

    let err = match load_snapshot(&store, KEYSPACE).await {
        Ok(_) => panic!("expected VersionMismatch error, load succeeded"),
        Err(e) => e,
    };
    assert!(
        matches!(err, SnapshotError::VersionMismatch(ref m) if m.contains("hnsw_rs")),
        "expected VersionMismatch(hnsw_rs ...) error, got {err:?}"
    );

    // Sanity: the supported version stamp is what Cargo.toml pins.
    assert_eq!(HNSW_RS_VERSION, "0.3.5");
}

// ============================================================================
// 4. tombstones survive dump/load
// ============================================================================

#[tokio::test]
async fn tombstones_survive_round_trip() {
    let dim = 16usize;
    let n = 300usize;
    let store = mem_store();
    let (adapter, items) = build_adapter(n, dim, VectorMetric::L2);
    for (r, v) in &items {
        adapter.upsert(*r, v).await.unwrap();
    }

    // Pick two distinct rids to delete. The rids we built encode the index
    // in the low byte — pick indices 50 and 150.
    let dead_rids: Vec<RecordId> = [50usize, 150usize]
        .iter()
        .map(|&i| {
            let mut a = [0u8; 16];
            a[14] = (i >> 8) as u8;
            a[15] = (i & 0xFF) as u8;
            RecordId(a)
        })
        .collect();
    for r in &dead_rids {
        adapter.delete(*r).await.unwrap();
    }
    // The query is the EXACT vector of a known live rid (index 7). At
    // k=10, ef=128 over a 300-point L2 graph, that rid is GUARANTEED to
    // appear in its own top-k (HNSW recall for self-similarity is reliable
    // at this size — and even if it weren't, the brute-force path at
    // len ≤ 256 kicks in deterministically; here len is 298 after the two
    // deletes so the HNSW path runs but with very high recall on a self-
    // similarity query).
    let live_idx = 7usize;
    let live_rid = {
        let mut a = [0u8; 16];
        a[14] = (live_idx >> 8) as u8;
        a[15] = (live_idx & 0xFF) as u8;
        RecordId(a)
    };
    let q = lcg_vec(dim, live_idx as u64 * 7 + 1);

    // Sanity: pre-dump, dead are absent and the self-similar live rid is
    // present in its own top-k.
    let pre = adapter
        .search(&q, 10, SearchOpts::with_ef_search(128), None)
        .await
        .unwrap();
    let pre_rids: TFxSet<RecordId> = pre.iter().map(|(r, _)| *r).collect();
    for r in &dead_rids {
        assert!(!pre_rids.contains(r), "pre-dump dead rid {r:?} surfaced");
    }
    assert!(
        pre_rids.contains(&live_rid),
        "pre-dump live rid missing — self-similarity recall broken"
    );

    snapshot::dump_snapshot(&adapter, &store, KEYSPACE)
        .await
        .unwrap();
    let loaded = load_snapshot(&store, KEYSPACE).await.unwrap();

    let post = loaded
        .search(&q, 10, SearchOpts::with_ef_search(128), None)
        .await
        .unwrap();
    let post_rids: TFxSet<RecordId> = post.iter().map(|(r, _)| *r).collect();
    for r in &dead_rids {
        assert!(
            !post_rids.contains(r),
            "post-load dead rid {r:?} surfaced — tombstone did not survive"
        );
    }
    assert!(
        post_rids.contains(&live_rid),
        "post-load live rid missing — tombstone scan too aggressive?"
    );
}

// ============================================================================
// 5. rid_map / next_id restored
// ============================================================================

#[tokio::test]
async fn rid_map_and_next_id_restored() {
    let dim = 8usize;
    let n = 300usize;
    let store = mem_store();
    let (adapter, items) = build_adapter(n, dim, VectorMetric::L2);
    for (r, v) in &items {
        adapter.upsert(*r, v).await.unwrap();
    }
    let original_next_id = adapter.next_id_value();

    snapshot::dump_snapshot(&adapter, &store, KEYSPACE)
        .await
        .unwrap();
    let loaded = load_snapshot(&store, KEYSPACE).await.unwrap();

    // next_id must not regress — new upserts after load must not collide
    // with existing internals.
    let restored_next_id = loaded.next_id_value();
    assert!(
        restored_next_id >= original_next_id,
        "next_id regressed across load: original={original_next_id}, restored={restored_next_id}"
    );

    // rid_map round-trips: every rid we inserted resolves to a known internal
    // in the loaded adapter (search over each rid's own vector returns that
    // rid as the top-1 neighbour). The HNSW graph is approximate but for
    // self-similarity (k=1, ef=64) recall is reliable on a 300-point graph.
    let mut hits = 0usize;
    for (r, v) in items.iter().take(40) {
        let res = loaded
            .search(v, 1, SearchOpts::with_ef_search(64), None)
            .await
            .unwrap();
        if res.first().map(|(rid, _)| *rid) == Some(*r) {
            hits += 1;
        }
    }
    // Loose: HNSW recall for exact-self on a 300-point graph is high but
    // not 100% under the unseedable RNG. Require ≥ 35/40 (87.5%) — enough
    // to catch a broken rid_map (which would surface 0 hits) without
    // flaking on a single RNG-adversarial build.
    assert!(
        hits >= 35,
        "rid_map self-recall too low: {hits}/40 — expected ≥ 35"
    );

    // A fresh upsert on a NEW rid must work and not collide — exercises
    // the restored `next_id`.
    let new_rid = rid(254);
    loaded.upsert(new_rid, &lcg_vec(dim, 9999)).await.unwrap();
    let r = loaded
        .search(&lcg_vec(dim, 9999), 1, SearchOpts::with_ef_search(64), None)
        .await
        .unwrap();
    assert_eq!(r.first().map(|(x, _)| *x), Some(new_rid));
}

// ============================================================================
// 6. (reserved) empty-index round-trip
// ============================================================================
//
// DELIBERATELY NOT TESTED here: `hnsw_rs::file_dump` refuses to dump a graph
// with zero points (returns `Err("unexpected error")` from its `dump` impl).
// A real vector index is never dumped empty — the engine builds the graph
// from the first upsert batch and only snapshots once `len() > 0`. The
// empty case is therefore the cheap "rebuild-from-scratch at boot" path
// (#401), not a snapshot path. Handling it in the codec would mean
// special-casing the graph-section to skip `file_dump` entirely and
// re-seed an empty `Hnsw::new` at load — added complexity for a case the
// engine never drives. Leave it out of V2.1; revisit if #401 needs it.

// ----------------------------------------------------------------------
// RV-INDEX-07424EF0 — generation-flip pruning of quantized sections
// (same theme as the codec flip tests above: the old generation's
// chunks must not survive the flip).
// ----------------------------------------------------------------------

#[tokio::test]
async fn rv_index_07424ef0_flip_prunes_old_quantized_generation_chunks() {
    let store = mem_store();
    let keyspace = "__vec_snap__07424ef0";
    let adapter = HnswAdapter::new(2, VectorMetric::L2, HnswConfig::default());
    // hnsw_rs::file_dump refuses an empty graph ("unexpected error" — no
    // entry point); seed a few points so the f32 dump succeeds.
    for i in 0..3u8 {
        adapter.upsert(rid(i), &lcg_vec(2, i as u64)).await.unwrap();
    }
    snapshot::dump_snapshot(&adapter, &store, keyspace)
        .await
        .unwrap();
    let old: SnapshotManifest = read_manifest(&store, keyspace).await.unwrap();
    let qg = chunk_key_for_test(keyspace, old.gen, "qgraph", 0);
    let qd = chunk_key_for_test(keyspace, old.gen, "qdata", 0);
    store
        .set(qg.clone(), Bytes::from_static(b"old-qgraph"))
        .await
        .unwrap();
    store
        .set(qd.clone(), Bytes::from_static(b"old-qdata"))
        .await
        .unwrap();
    snapshot::dump_snapshot_with_gen(&adapter, &store, keyspace, old.gen + 1)
        .await
        .unwrap();
    let written = SnapshotManifest {
        gen: old.gen + 1,
        qgraph_chunks: 0,
        qdata_chunks: 0,
        ..old.clone()
    };
    flip_generation(
        &store,
        keyspace,
        old.gen,
        OldGenerationChunks {
            graph: old.graph_chunks,
            data: old.data_chunks,
            qgraph: 1,
            qdata: 1,
        },
        written,
        0,
    )
    .await
    .unwrap();
    assert!(
        store.get(qg).await.is_err(),
        "old qgraph chunk must be pruned"
    );
    assert!(
        store.get(qd).await.is_err(),
        "old qdata chunk must be pruned"
    );
}

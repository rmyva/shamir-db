//! Native forward range scan of `FjallStore::iter_range_stream`: the result
//! must equal an independent in-memory oracle for every bound shape and every
//! batch size, and an inverted range must be empty.

use crate::storage_fjall::FjallRepo;
use crate::types::{RecordKey, Repo, Store};
use bytes::Bytes;
use futures::StreamExt;
use std::sync::Arc;

const KEY_COUNT: usize = 120;
/// Count of persisted (flushed-to-SST) keys in the persisted fixtures.
const PERSISTED_KEY_COUNT: usize = 120;

fn key_of(index: usize) -> String {
    format!("k{index:03}")
}

async fn seeded_store(dir: &std::path::Path) -> (FjallRepo, Arc<dyn Store>) {
    let repo = FjallRepo::new(dir).unwrap();
    let store = repo.store_get("forward-range").await.unwrap();
    // Reverse insertion order: the scan order must come from the keyspace,
    // not from the order of writes.
    for index in (0..KEY_COUNT).rev() {
        store
            .set(
                RecordKey::from(Bytes::from(key_of(index))),
                Bytes::from(format!("v{index}")),
            )
            .await
            .unwrap();
    }
    (repo, store)
}

/// A store whose keys ALL live in persisted segments: seed, then force the
/// active memtable into SSTs via the bounded
/// `Keyspace::rotate_memtable_and_wait` seam before any scan.
async fn seeded_persisted_store(dir: &std::path::Path) -> (FjallRepo, Arc<dyn Store>) {
    let repo = FjallRepo::new(dir).unwrap();
    let store = repo.store_get("persisted-range").await.unwrap();
    for index in (0..PERSISTED_KEY_COUNT).rev() {
        store
            .set(
                RecordKey::from(Bytes::from(key_of(index))),
                Bytes::from(format!("v{index}")),
            )
            .await
            .unwrap();
    }
    repo.persist_memtables_for_test("persisted-range")
        .await
        .unwrap();
    (repo, store)
}

/// A store whose key set is SPLIT across generations: the even-indexed keys
/// are flushed into persisted segments, then the odd-indexed keys are written
/// into the fresh (post-rotation) memtable. A full-range scan must merge both
/// generations seamlessly into k000..k119.
async fn seeded_mixed_store(dir: &std::path::Path) -> (FjallRepo, Arc<dyn Store>) {
    let repo = FjallRepo::new(dir).unwrap();
    let store = repo.store_get("mixed-range").await.unwrap();
    for index in (0..KEY_COUNT).step_by(2).rev() {
        store
            .set(
                RecordKey::from(Bytes::from(key_of(index))),
                Bytes::from(format!("v{index}")),
            )
            .await
            .unwrap();
    }
    repo.persist_memtables_for_test("mixed-range")
        .await
        .unwrap();
    // Reverse order again: the memtable insertion order must not matter.
    for index in (1..KEY_COUNT).step_by(2).rev() {
        store
            .set(
                RecordKey::from(Bytes::from(key_of(index))),
                Bytes::from(format!("v{index}")),
            )
            .await
            .unwrap();
    }
    (repo, store)
}

async fn scan(
    store: &Arc<dyn Store>,
    start: Option<&str>,
    end: Option<&str>,
    batch_size: usize,
) -> (Vec<String>, usize) {
    let stream = store.iter_range_stream(
        start.map(|s| Bytes::from(s.to_owned())),
        end.map(|e| Bytes::from(e.to_owned())),
        batch_size,
    );
    futures::pin_mut!(stream);
    let mut keys = Vec::new();
    let mut batches = 0;
    while let Some(batch) = stream.next().await {
        let batch = batch.unwrap();
        assert!(
            !batch.is_empty() && batch.len() <= batch_size,
            "a batch is never empty and never above the batch size"
        );
        batches += 1;
        for (key, value) in batch {
            let key = String::from_utf8(key.to_vec()).unwrap();
            let index: usize = key[1..].parse().unwrap();
            assert_eq!(value, Bytes::from(format!("v{index}")), "value of {key}");
            keys.push(key);
        }
    }
    (keys, batches)
}

fn oracle(start: Option<&str>, end: Option<&str>) -> Vec<String> {
    (0..KEY_COUNT)
        .map(key_of)
        .filter(|key| start.is_none_or(|s| key.as_str() >= s))
        .filter(|key| end.is_none_or(|e| key.as_str() <= e))
        .collect()
}

/// Oracle over an arbitrary key count (persisted/mixed fixtures use the same
/// k000..k{N-1} key space).
fn oracle_n(count: usize, start: Option<&str>, end: Option<&str>) -> Vec<String> {
    (0..count)
        .map(key_of)
        .filter(|key| start.is_none_or(|s| key.as_str() >= s))
        .filter(|key| end.is_none_or(|e| key.as_str() <= e))
        .collect()
}

/// Like `scan`, but also returns each batch's LENGTH so tests can assert the
/// exact split shape (a count alone cannot catch a `[..., 7, 6]` vs
/// `[..., 6, 7]` mis-split).
async fn scan_with_lengths(
    store: &Arc<dyn Store>,
    start: Option<&str>,
    end: Option<&str>,
    batch_size: usize,
) -> (Vec<String>, Vec<usize>) {
    let stream = store.iter_range_stream(
        start.map(|s| Bytes::from(s.to_owned())),
        end.map(|e| Bytes::from(e.to_owned())),
        batch_size,
    );
    futures::pin_mut!(stream);
    let mut keys = Vec::new();
    let mut lengths = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = batch.unwrap();
        assert!(
            !batch.is_empty() && batch.len() <= batch_size,
            "a batch is never empty and never above the batch size"
        );
        lengths.push(batch.len());
        for (key, value) in batch {
            let key = String::from_utf8(key.to_vec()).unwrap();
            let index: usize = key[1..].parse().unwrap();
            assert_eq!(value, Bytes::from(format!("v{index}")), "value of {key}");
            keys.push(key);
        }
    }
    (keys, lengths)
}

/// Catches a bound that is exclusive instead of inclusive, a dropped bound,
/// a descending or unsorted result, and a cursor that skips or repeats a key
/// at a batch boundary (batch sizes 1, 7, 50 and 1000 against 120 keys).
#[tokio::test]
async fn forward_range_matches_the_oracle_for_every_bound_shape_and_batch_size() {
    let dir = tempfile::tempdir().unwrap();
    let (repo, store) = seeded_store(dir.path()).await;
    let shapes: [(Option<&str>, Option<&str>); 9] = [
        (None, None),
        (Some("k030"), None),
        (None, Some("k089")),
        (Some("k030"), Some("k089")),
        (Some("k050"), Some("k050")),
        (Some("k0305"), Some("k0895")),
        (Some("k119"), None),
        (None, Some("k000")),
        (Some("k200"), None),
    ];
    for batch_size in [1, 7, 50, 1000] {
        for (start, end) in shapes {
            let (got, _) = scan(&store, start, end, batch_size).await;
            assert_eq!(
                got,
                oracle(start, end),
                "start {start:?} end {end:?} batch {batch_size}"
            );
        }
    }
    drop(store);
    drop(repo);
}

/// Catches an implementation that ignores the batch size (one batch for the
/// whole range) and one that stops after the first batch.
#[tokio::test]
async fn forward_range_splits_the_result_into_batches_of_the_requested_size() {
    let dir = tempfile::tempdir().unwrap();
    let (repo, store) = seeded_store(dir.path()).await;
    let (got, batches) = scan(&store, Some("k010"), Some("k029"), 7).await;
    assert_eq!(got.len(), 20);
    assert_eq!(batches, 3, "20 keys in batches of 7 are 7 + 7 + 6");
    drop(store);
    drop(repo);
}

/// Catches an inverted range reaching the LSM iterator (a panic inside the
/// blocking task would surface as an internal error, not as an empty result).
#[tokio::test]
async fn forward_range_with_inverted_bounds_is_empty() {
    let dir = tempfile::tempdir().unwrap();
    let (repo, store) = seeded_store(dir.path()).await;
    let (got, batches) = scan(&store, Some("k090"), Some("k010"), 10).await;
    assert!(got.is_empty());
    assert_eq!(batches, 0);
    drop(store);
    drop(repo);
}

/// Catches a native forward scan that only reads the ACTIVE MEMTABLE: every
/// key here lives in a persisted segment (the memtable was rotated and
/// flushed before the scan), so a memtable-only scan would return nothing.
/// Re-runs the full bound-shape × batch-size oracle matrix.
#[tokio::test]
async fn forward_range_on_persisted_ssts_matches_the_oracle() {
    let dir = tempfile::tempdir().unwrap();
    let (repo, store) = seeded_persisted_store(dir.path()).await;
    let shapes: [(Option<&str>, Option<&str>); 9] = [
        (None, None),
        (Some("k030"), None),
        (None, Some("k089")),
        (Some("k030"), Some("k089")),
        (Some("k050"), Some("k050")),
        (Some("k0305"), Some("k0895")),
        (Some("k119"), None),
        (None, Some("k000")),
        (Some("k200"), None),
    ];
    for batch_size in [1, 7, 50, 1000] {
        for (start, end) in shapes {
            let (got, _) = scan(&store, start, end, batch_size).await;
            assert_eq!(
                got,
                oracle_n(PERSISTED_KEY_COUNT, start, end),
                "start {start:?} end {end:?} batch {batch_size}"
            );
        }
    }
    drop(store);
    drop(repo);
}

/// Catches a scan that merges memtable and persisted generations incorrectly:
/// even keys are in SSTs, odd keys in the memtable, and the scan must yield
/// the seamless k000..k119 union in order — no missing generation, no
/// duplicate, no unsorted interleave across the table boundary.
#[tokio::test]
async fn forward_range_merges_persisted_and_memtable_generations_seamlessly() {
    let dir = tempfile::tempdir().unwrap();
    let (repo, store) = seeded_mixed_store(dir.path()).await;
    let shapes: [(Option<&str>, Option<&str>); 5] = [
        (None, None),
        // Crosses the persisted/memtable interleaving many times.
        (Some("k030"), Some("k089")),
        (Some("k051"), Some("k051")),
        (Some("k050"), Some("k051")),
        (None, Some("k010")),
    ];
    for batch_size in [1, 7, 50, 1000] {
        for (start, end) in shapes {
            let (got, _) = scan(&store, start, end, batch_size).await;
            assert_eq!(
                got,
                oracle_n(KEY_COUNT, start, end),
                "start {start:?} end {end:?} batch {batch_size}"
            );
        }
    }
    drop(store);
    drop(repo);
}

/// Catches a prefix-extension leak: with unpadded real keys `k1`, `k10`,
/// `k100` all persisted, `range(k1..=k1)` must yield exactly `k1` — a scan
/// that treats the inclusive endpoint as a byte-prefix filter would also
/// return `k10`/`k100` (both sort after `k1` and can appear before the bound
/// is passed).
#[tokio::test]
async fn forward_range_with_inclusive_endpoint_does_not_leak_prefix_extension_keys() {
    let dir = tempfile::tempdir().unwrap();
    let repo = FjallRepo::new(dir.path()).unwrap();
    let store = repo.store_get("prefix-extension").await.unwrap();
    for key in ["k1", "k10", "k100"] {
        store
            .set(
                RecordKey::from(Bytes::from(key)),
                Bytes::from(format!("v{}", &key[1..])),
            )
            .await
            .unwrap();
    }
    repo.persist_memtables_for_test("prefix-extension")
        .await
        .unwrap();
    let (got, _) = scan(&store, Some("k1"), Some("k1"), 10).await;
    assert_eq!(got, vec!["k1".to_string()]);
    // Sanity: the wider range really contains all three keys.
    let (all, _) = scan(&store, Some("k1"), Some("k100"), 10).await;
    assert_eq!(
        all,
        vec!["k1".to_string(), "k10".to_string(), "k100".to_string()]
    );
    drop(store);
    drop(repo);
}

/// Catches a scan that fabricates batches (or an error) on an untouched
/// keyspace: a range over a store with zero keys must yield an empty key set
/// and zero batches.
#[tokio::test]
async fn forward_range_on_an_untouched_keyspace_yields_no_batches() {
    let dir = tempfile::tempdir().unwrap();
    let repo = FjallRepo::new(dir.path()).unwrap();
    let store = repo.store_get("empty-range").await.unwrap();
    let (got, batches) = scan(&store, None, None, 10).await;
    assert!(got.is_empty());
    assert_eq!(batches, 0);
    drop(store);
    drop(repo);
}

/// Catches an inverted range on PERSISTED data reaching the LSM iterator (a
/// panic inside the blocking task would surface as an internal error, not as
/// an empty result) — the memtable-only inverted case is covered above.
#[tokio::test]
async fn forward_range_with_inverted_bounds_on_persisted_data_is_empty() {
    let dir = tempfile::tempdir().unwrap();
    let (repo, store) = seeded_persisted_store(dir.path()).await;
    let (got, batches) = scan(&store, Some("k090"), Some("k010"), 10).await;
    assert!(got.is_empty());
    assert_eq!(batches, 0);
    drop(store);
    drop(repo);
}

/// Catches a batch mis-split on persisted data: 20 keys in batches of 7 must
/// yield the exact lengths vec `[7, 7, 6]` (not just "3 batches") and the
/// concatenation must equal the full expected key set — the same split is
/// asserted on the mixed persisted+memtable store, where batches straddle the
/// generation boundary.
#[tokio::test]
async fn forward_range_batch_split_on_persisted_and_mixed_data_has_exact_lengths() {
    let dir = tempfile::tempdir().unwrap();
    let (repo, store) = seeded_persisted_store(dir.path()).await;
    let expected: Vec<String> = (0..20).map(key_of).collect();
    let (keys, lengths) = scan_with_lengths(&store, Some("k000"), Some("k019"), 7).await;
    assert_eq!(
        lengths,
        vec![7, 7, 6],
        "20 keys in batches of 7 are 7 + 7 + 6"
    );
    assert_eq!(keys, expected);
    drop(store);
    drop(repo);

    let dir = tempfile::tempdir().unwrap();
    let (repo, store) = seeded_mixed_store(dir.path()).await;
    let (keys, lengths) = scan_with_lengths(&store, Some("k000"), Some("k019"), 7).await;
    assert_eq!(
        lengths,
        vec![7, 7, 6],
        "mixed persisted+memtable: same exact split across the generation boundary"
    );
    assert_eq!(keys, expected);
    drop(store);
    drop(repo);
}

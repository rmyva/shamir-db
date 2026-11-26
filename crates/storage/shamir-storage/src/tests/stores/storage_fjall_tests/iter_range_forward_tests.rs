//! Native forward range scan of `FjallStore::iter_range_stream`: the result
//! must equal an independent in-memory oracle for every bound shape and every
//! batch size, and an inverted range must be empty.

use crate::storage_fjall::FjallRepo;
use crate::types::{RecordKey, Repo, Store};
use bytes::Bytes;
use futures::StreamExt;
use std::sync::Arc;

const KEY_COUNT: usize = 120;

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

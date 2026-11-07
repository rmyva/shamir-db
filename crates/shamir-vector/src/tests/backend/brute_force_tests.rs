use crate::adapter::{SearchOpts, VectorAdapter, VectorError};
use crate::brute_force::BruteForceAdapter;
use shamir_index_core::kind::VectorMetric;
use shamir_types::types::record_id::RecordId;

fn rid(n: u8) -> RecordId {
    let mut a = [0u8; 16];
    a[15] = n;
    RecordId(a)
}

#[tokio::test]
async fn cosine_basic() {
    let adapter = BruteForceAdapter::new(3, VectorMetric::Cosine);
    adapter.upsert(rid(1), &[1.0, 0.0, 0.0]).await.unwrap();
    adapter.upsert(rid(2), &[0.0, 1.0, 0.0]).await.unwrap();
    adapter.upsert(rid(3), &[1.0, 1.0, 0.0]).await.unwrap();

    // Wait for actor to process writes.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let results = adapter
        .search(&[1.0, 0.0, 0.0], 2, SearchOpts::default(), None)
        .await
        .unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].0, rid(1)); // exact match = distance 0
    assert!(results[0].1 < 0.01);
}

#[tokio::test]
async fn l2_basic() {
    let adapter = BruteForceAdapter::new(2, VectorMetric::L2);
    adapter.upsert(rid(1), &[0.0, 0.0]).await.unwrap();
    adapter.upsert(rid(2), &[3.0, 4.0]).await.unwrap();
    adapter.upsert(rid(3), &[1.0, 0.0]).await.unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let results = adapter
        .search(&[0.0, 0.0], 2, SearchOpts::default(), None)
        .await
        .unwrap();
    assert_eq!(results[0].0, rid(1)); // distance 0
    assert_eq!(results[1].0, rid(3)); // distance 1
}

#[tokio::test]
async fn dot_product() {
    let adapter = BruteForceAdapter::new(2, VectorMetric::Dot);
    adapter.upsert(rid(1), &[1.0, 0.0]).await.unwrap();
    adapter.upsert(rid(2), &[0.5, 0.5]).await.unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // query = [1, 0], dot with rid(1)=1.0, dot with rid(2)=0.5
    // negated: rid(1)=-1.0 < rid(2)=-0.5 → rid(1) first
    let results = adapter
        .search(&[1.0, 0.0], 2, SearchOpts::default(), None)
        .await
        .unwrap();
    assert_eq!(results[0].0, rid(1));
}

#[tokio::test]
async fn delete_removes_from_search() {
    let adapter = BruteForceAdapter::new(2, VectorMetric::L2);
    adapter.upsert(rid(1), &[0.0, 0.0]).await.unwrap();
    adapter.upsert(rid(2), &[1.0, 0.0]).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    adapter.delete(rid(1)).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let results = adapter
        .search(&[0.0, 0.0], 10, SearchOpts::default(), None)
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, rid(2));
}

#[tokio::test]
async fn dim_mismatch_rejected() {
    let adapter = BruteForceAdapter::new(3, VectorMetric::L2);
    let err = adapter.upsert(rid(1), &[1.0, 2.0]).await.unwrap_err();
    assert!(matches!(
        err,
        VectorError::DimMismatch {
            expected: 3,
            got: 2
        }
    ));
}

#[tokio::test]
async fn upsert_replaces() {
    let adapter = BruteForceAdapter::new(2, VectorMetric::L2);
    adapter.upsert(rid(1), &[0.0, 0.0]).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    adapter.upsert(rid(1), &[10.0, 10.0]).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;

    assert_eq!(adapter.len(), 1);
    let results = adapter
        .search(&[10.0, 10.0], 1, SearchOpts::default(), None)
        .await
        .unwrap();
    assert_eq!(results[0].0, rid(1));
    assert!(results[0].1 < 0.01);
}

#[tokio::test]
async fn huge_k_clamped_no_panic() {
    let adapter = BruteForceAdapter::new(2, VectorMetric::L2);
    adapter.upsert(rid(1), &[0.0, 0.0]).await.unwrap();
    adapter.upsert(rid(2), &[1.0, 0.0]).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    // k = u32::MAX would previously cause huge allocation
    let results = adapter
        .search(&[0.0, 0.0], u32::MAX, SearchOpts::default(), None)
        .await
        .unwrap();
    assert_eq!(results.len(), 2);
}

#[tokio::test]
async fn k_zero_returns_empty() {
    let adapter = BruteForceAdapter::new(2, VectorMetric::L2);
    adapter.upsert(rid(1), &[0.0, 0.0]).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    let results = adapter
        .search(&[0.0, 0.0], 0, SearchOpts::default(), None)
        .await
        .unwrap();
    assert!(results.is_empty());
}

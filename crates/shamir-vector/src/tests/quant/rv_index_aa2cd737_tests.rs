use crate::adapter::{VectorAdapter, VectorError};
use crate::brute_force::BruteForceAdapter;
use shamir_index_core::kind::VectorMetric;
use shamir_types::types::record_id::RecordId;

fn rid(i: u8) -> RecordId {
    let mut x = [0; 16];
    x[15] = i;
    RecordId(x)
}

#[tokio::test]
async fn rv_index_aa2cd737_default_batch_validates_all_dims_before_writing() {
    let adapter = BruteForceAdapter::new(2, VectorMetric::L2);
    let result = adapter
        .upsert_batch(&[(rid(1), vec![1.0, 2.0]), (rid(2), vec![3.0])])
        .await;
    assert!(matches!(result, Err(VectorError::DimMismatch { .. })));
    assert_eq!(
        adapter.len(),
        0,
        "the first valid vector must not be enqueued"
    );
}

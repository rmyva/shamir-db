//! Ordered, bounded delta replay regression tests.

use super::{
    append_delta, replay_delta, replay_delta_with_adapter, DeltaOp, SnapshotError,
    REPLAY_BATCH_LIMIT,
};
use crate::adapter::{SearchOpts, VectorAdapter, VectorError};
use crate::hnsw_adapter::{HnswAdapter, HnswConfig};
use async_trait::async_trait;
use shamir_collections::TFxMap;
use shamir_index_core::kind::VectorMetric;
use shamir_storage::storage_in_memory::InMemoryStore;
use shamir_storage::types::Store;
use shamir_types::types::record_id::RecordId;
use std::sync::{Arc, Mutex};

type RecordingState = (TFxMap<RecordId, Vec<f32>>, Vec<usize>);

#[derive(Default)]
struct RecordingAdapter {
    state: Mutex<RecordingState>,
}

#[async_trait]
impl VectorAdapter for RecordingAdapter {
    async fn upsert(&self, rid: RecordId, vec: &[f32]) -> Result<(), VectorError> {
        self.state.lock().unwrap().0.insert(rid, vec.to_vec());
        Ok(())
    }

    async fn delete(&self, rid: RecordId) -> Result<(), VectorError> {
        let mut state = self.state.lock().unwrap();
        state.0.remove(&rid);
        state.1.push(0);
        Ok(())
    }

    async fn upsert_batch(&self, items: &[(RecordId, Vec<f32>)]) -> Result<(), VectorError> {
        if items.iter().any(|(_, vec)| vec.len() != 1) {
            return Err(VectorError::Internal("invalid dimension".into()));
        }
        let mut state = self.state.lock().unwrap();
        state.1.push(items.len());
        for (rid, vec) in items {
            state.0.insert(*rid, vec.clone());
        }
        Ok(())
    }

    async fn search(
        &self,
        _query: &[f32],
        _k: u32,
        _opts: SearchOpts,
        _staged: Option<&[(RecordId, Vec<f32>)]>,
    ) -> Result<Vec<(RecordId, f32)>, VectorError> {
        Ok(Vec::new())
    }

    fn dim(&self) -> u32 {
        1
    }

    fn len(&self) -> usize {
        self.state.lock().unwrap().0.len()
    }
}

fn rid(n: u8) -> RecordId {
    let mut bytes = [0; 16];
    bytes[15] = n;
    RecordId(bytes)
}

#[tokio::test]
async fn replay_batches_are_bounded_and_preserve_order_across_boundaries() {
    let store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let mut ops = Vec::new();
    for i in 0..REPLAY_BATCH_LIMIT {
        ops.push(DeltaOp::Upsert(rid((i % 3) as u8), vec![i as f32]));
    }
    ops.push(DeltaOp::Upsert(rid(0), vec![1000.0]));
    ops.push(DeltaOp::Delete(rid(0)));
    ops.push(DeltaOp::Upsert(rid(0), vec![2000.0]));
    ops.push(DeltaOp::Delete(rid(1)));
    append_delta(&store, "ordered", 0, &ops).await.unwrap();
    append_delta(
        &store,
        "ordered",
        1,
        &[DeltaOp::Upsert(rid(1), vec![3000.0])],
    )
    .await
    .unwrap();

    let recorder = RecordingAdapter::default();
    assert_eq!(
        replay_delta_with_adapter(&store, "ordered", 0, &recorder)
            .await
            .unwrap(),
        1
    );
    {
        let state = recorder.state.lock().unwrap();
        assert_eq!(state.1, vec![REPLAY_BATCH_LIMIT, 1, 0, 1, 0, 1]);
        assert_eq!(state.0.get(&rid(0)), Some(&vec![2000.0]));
        assert_eq!(state.0.get(&rid(1)), Some(&vec![3000.0]));
    }

    let actual = HnswAdapter::new(1, VectorMetric::L2, HnswConfig::default());
    replay_delta(&store, "ordered", 0, &actual).await.unwrap();
    let results = actual
        .search(&[2000.0], 3, SearchOpts::default(), None)
        .await
        .unwrap();
    assert_eq!(results[0].0, rid(0));
    assert!(results.iter().any(|(id, _)| *id == rid(1)));
}

#[tokio::test]
async fn replay_batch_error_stops_later_operations() {
    let store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let ops = vec![
        DeltaOp::Upsert(rid(1), vec![1.0, 2.0]),
        DeltaOp::Delete(rid(2)),
        DeltaOp::Upsert(rid(3), vec![3.0]),
    ];
    append_delta(&store, "bad", 0, &ops).await.unwrap();
    let recorder = RecordingAdapter::default();
    let result = replay_delta_with_adapter(&store, "bad", 0, &recorder).await;
    assert!(matches!(result, Err(SnapshotError::Backend(_))));
    assert!(recorder.state.lock().unwrap().1.is_empty());
}

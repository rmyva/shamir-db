use super::{apply_op, BruteForceAdapter, BruteWork, WriteOp, CHUNK_LEN};
use crate::adapter::{SearchOpts, VectorAdapter, VectorError};
use shamir_index_core::kind::VectorMetric;
use shamir_types::types::record_id::RecordId;
use std::sync::{Arc, Mutex};

fn rid(n: usize) -> RecordId {
    let mut bytes = [0; 16];
    bytes[8..].copy_from_slice(&(n as u64).to_be_bytes());
    RecordId(bytes)
}

#[test]
fn publications_share_untouched_chunks_and_keep_old_snapshots() {
    let mut work = BruteWork::default();
    for i in 0..(CHUNK_LEN * 3) {
        apply_op(&mut work, WriteOp::Upsert(rid(i), vec![i as f32]));
    }
    let old = work.snap.clone();
    apply_op(&mut work, WriteOp::Upsert(rid(2), vec![999.0]));
    let updated = work.snap.clone();
    assert!(!Arc::ptr_eq(&old.chunks[0], &updated.chunks[0]));
    assert!(Arc::ptr_eq(&old.chunks[1], &updated.chunks[1]));
    assert!(Arc::ptr_eq(&old.chunks[2], &updated.chunks[2]));
    assert_eq!(old.chunks[0][2].vector, vec![2.0]);
    assert_eq!(updated.chunks[0][2].vector, vec![999.0]);

    apply_op(&mut work, WriteOp::Delete(rid(2)));
    assert!(Arc::ptr_eq(&updated.chunks[1], &work.snap.chunks[1]));
    assert_eq!(work.snap.len, CHUNK_LEN * 3 - 1);
    let moved = rid(CHUNK_LEN * 3 - 1);
    assert_eq!(work.snap.chunks[0][2].rid, moved);
    apply_op(&mut work, WriteOp::Upsert(moved, vec![42.0]));
    assert_eq!(work.snap.chunks[0][2].vector, vec![42.0]);
    apply_op(&mut work, WriteOp::Delete(moved));
    apply_op(&mut work, WriteOp::Upsert(rid(2), vec![7.0]));
    assert_eq!(work.snap.len, CHUNK_LEN * 3 - 1);
    assert_eq!(work.index.get(&rid(2)), Some(&(CHUNK_LEN * 3 - 2)));
    assert_eq!(updated.chunks[0][2].vector, vec![999.0]);
}

#[tokio::test(flavor = "current_thread")]
async fn search_worker_does_not_block_runtime_and_preserves_staged_behavior() {
    let mut adapter = BruteForceAdapter::new(1, VectorMetric::L2);
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::sync_channel::<()>(1);
    // Test-only handshake; each mutex is accessed once, with no await under guard.
    let started = Mutex::new(Some(started_tx));
    let release = Mutex::new(release_rx);
    adapter.worker_hook = Some(Arc::new(move || {
        if let Some(tx) = started.lock().unwrap().take() {
            let _ = tx.send(()); // The test may have already abandoned the receiver.
        }
        release.lock().unwrap().recv().unwrap();
    }));
    let staged = [(rid(1), vec![2.0]), (rid(1), vec![1.0])];
    let search = adapter.search(&[0.0], 2, SearchOpts::default(), Some(&staged));
    let progress = async {
        started_rx.await.unwrap();
        release_tx.send(()).unwrap();
    };
    let (result, ()) = tokio::join!(search, progress);
    let result = result.unwrap();
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].0, rid(1));
    assert!((result[0].1 - 1.0).abs() < 1e-6);
    assert!((result[1].1 - 2.0).abs() < 1e-6);
    adapter.worker_hook = None;
    let bad = [(rid(2), vec![1.0, 2.0])];
    assert!(matches!(
        adapter
            .search(&[0.0], 1, SearchOpts::default(), Some(&bad))
            .await,
        Err(VectorError::DimMismatch {
            expected: 1,
            got: 2
        })
    ));
    adapter.shutdown().await;
}

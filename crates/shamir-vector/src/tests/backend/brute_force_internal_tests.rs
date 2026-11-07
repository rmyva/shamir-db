use super::{BruteForceAdapter, ShutdownFailure};
use shamir_index_core::kind::VectorMetric;

#[tokio::test]
async fn shutdown_reports_poison_and_drains_writer() {
    let adapter = BruteForceAdapter::new(1, VectorMetric::L2);
    let poison = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _guard = adapter.join.lock().unwrap();
        panic!("poison teardown slot");
    }));
    assert!(poison.is_err());
    let mut reports = Vec::new();
    adapter
        .shutdown_with_report(|error| reports.push(error))
        .await;
    assert!(matches!(reports.as_slice(), [ShutdownFailure::Poisoned]));
}

#[tokio::test]
async fn shutdown_reports_writer_panic() {
    let adapter = BruteForceAdapter::new(1, VectorMetric::L2);
    let old = adapter.join.lock().unwrap().replace(tokio::spawn(async {
        panic!("brute writer failure");
    }));
    // Closing the sender below also lets the original writer drain.
    let mut reports = Vec::new();
    adapter
        .shutdown_with_report(|error| reports.push(error))
        .await;
    old.expect("original writer handle").await.unwrap();
    match reports.pop().expect("writer failure reported") {
        ShutdownFailure::Join(error) => {
            assert!(error.is_panic());
            assert_eq!(
                error.into_panic().downcast_ref::<&str>(),
                Some(&"brute writer failure")
            );
        }
        ShutdownFailure::Poisoned => panic!("unexpected poison report"),
    }
    assert!(reports.is_empty());
}

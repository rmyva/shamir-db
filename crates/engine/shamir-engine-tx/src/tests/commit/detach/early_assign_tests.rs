//! P2a tests: verify that early version assignment (before SSI validation)
//! correctly marks Aborted versions so the watermark advances.

use shamir_tx::completion_tracker::State;
use shamir_tx::RepoTxGate;

/// After P2a, a tx that fails SSI has already consumed a version via
/// `assign_next_version`. That version MUST be marked Aborted so the
/// CompletionTracker watermark advances past it.
#[tokio::test]
async fn early_assign_ssi_fail_aborts_version() {
    let gate = RepoTxGate::new(0, 1);

    // Simulate early-assign: version 1 is consumed.
    let v1 = gate.assign_next_version();
    assert_eq!(v1, 1);

    // Simulate SSI failure: mark the version Aborted.
    gate.completion().mark(v1, State::Aborted);

    // The watermark must advance past the aborted version.
    assert_eq!(
        gate.completion().watermark(),
        1,
        "watermark must advance past aborted version"
    );

    // A subsequent commit can still proceed — version 2 assigned and
    // materialized; watermark advances to 2.
    let v2 = gate.assign_next_version();
    assert_eq!(v2, 2);
    gate.completion().mark(v2, State::Materialized);
    assert_eq!(gate.completion().watermark(), 2);
}

/// Multiple consecutive SSI failures burn versions; all are marked Aborted
/// and the watermark catches up once a subsequent commit materializes.
#[tokio::test]
async fn multiple_aborted_versions_watermark_advances() {
    let gate = RepoTxGate::new(0, 1);

    // Three SSI failures in a row.
    for _ in 0..3 {
        let v = gate.assign_next_version();
        gate.completion().mark(v, State::Aborted);
    }
    assert_eq!(gate.completion().watermark(), 3);

    // Successful commit at version 4.
    let v4 = gate.assign_next_version();
    assert_eq!(v4, 4);
    gate.completion().mark(v4, State::Materialized);
    assert_eq!(gate.completion().watermark(), 4);
}

/// C6 empty-tx fast-path: version is allocated then immediately Aborted.
#[tokio::test]
async fn empty_tx_burns_version_marks_aborted() {
    let gate = RepoTxGate::new(5, 1);

    let v = gate.assign_next_version();
    assert_eq!(v, 6);
    // Simulate C6 path marking.
    gate.completion().mark(v, State::Aborted);
    assert_eq!(gate.completion().watermark(), 6);
}

/// Concurrent monotonic assignment: fetch_add guarantees no duplicate.
#[tokio::test]
async fn concurrent_assign_monotonic() {
    let gate = std::sync::Arc::new(RepoTxGate::new(0, 1));
    let mut handles = Vec::new();
    for _ in 0..100 {
        let g = std::sync::Arc::clone(&gate);
        handles.push(tokio::spawn(async move { g.assign_next_version() }));
    }
    let mut versions = Vec::with_capacity(100);
    for h in handles {
        versions.push(h.await.unwrap());
    }
    versions.sort_unstable();
    versions.dedup();
    assert_eq!(versions.len(), 100, "all 100 versions must be unique");
    assert_eq!(*versions.first().unwrap(), 1);
    assert_eq!(*versions.last().unwrap(), 100);
}

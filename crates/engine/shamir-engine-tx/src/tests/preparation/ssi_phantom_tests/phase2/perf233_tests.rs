//! PERF-233: active_serializable_count gates non-tx SSI footprint.

use shamir_tx::IsolationLevel;
use shamir_types::types::value::InnerValue;

use crate::table::TableConfig;

use super::super::helpers::make_repo;

/// PERF-233 — non-tx insert WITHOUT an active Serializable tx must NOT write
/// to the commit_write_log (footprint is suppressed by the early-return guard).
///
/// Sequence:
///   1. No active Serializable snapshots → non-tx insert → log stays empty.
///   2. Open Serializable snapshot → non-tx insert → log grows by 1.
///   3. Drop Serializable guard → count back to 0 → non-tx insert → log stays.
#[tokio::test]
async fn perf233_nontx_footprint_skipped_without_serializable_snapshot() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    let gate = repo.tx_gate().await.unwrap();

    // --- Phase 1: no Serializable tx open.
    assert_eq!(
        gate.active_serializable_count(),
        0,
        "baseline: no Serializable snapshots"
    );
    tbl.insert(&InnerValue::Str("no_observer".into()))
        .await
        .unwrap();
    assert_eq!(
        gate.commit_log_len(),
        0,
        "footprint must be suppressed when active_serializable_count == 0"
    );

    // --- Phase 2: open a Serializable snapshot → footprint must be written.
    let (_, ser_guard) = repo.begin_tx(IsolationLevel::Serializable).await.unwrap();
    assert_eq!(
        gate.active_serializable_count(),
        1,
        "active_serializable_count must be 1 while Serializable guard is alive"
    );
    let log_before = gate.commit_log_len();
    tbl.insert(&InnerValue::Str("with_observer".into()))
        .await
        .unwrap();
    assert_eq!(
        gate.commit_log_len(),
        log_before + 1,
        "footprint must be written when active_serializable_count > 0"
    );

    // --- Phase 3: drop the Serializable guard → count returns to 0.
    drop(ser_guard);
    assert_eq!(
        gate.active_serializable_count(),
        0,
        "active_serializable_count must return to 0 after guard drop"
    );
    let log_after_drop = gate.commit_log_len();
    tbl.insert(&InnerValue::Str("no_observer_again".into()))
        .await
        .unwrap();
    assert_eq!(
        gate.commit_log_len(),
        log_after_drop,
        "footprint must again be suppressed once active_serializable_count == 0"
    );
}

/// PERF-233 — opening a Snapshot (level-1) snapshot must NOT increment
/// `active_serializable_count`.
#[tokio::test]
async fn perf233_snapshot_isolation_does_not_increment_serializable_count() {
    let repo = make_repo();
    let gate = repo.tx_gate().await.unwrap();

    let (_, snap_guard) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    assert_eq!(
        gate.active_serializable_count(),
        0,
        "Snapshot-isolation guard must not increment active_serializable_count"
    );
    drop(snap_guard);
    assert_eq!(gate.active_serializable_count(), 0);
}

use shamir_wal::WalOpV2;

use crate::tx::drainer::Drainer;
use crate::tx::table::{table_manager::table_token_for, TableConfig};

use super::support::{make_repo, seed_inflight_entry};

// ── Index-posting batching (P2 perf fix) ────────────────────────────

/// A single drain-window entry with MULTIPLE index postings spanning
/// MULTIPLE tables must apply every posting correctly — including a
/// same-key set-then-delete within the SAME batch, which only passes if
/// the flattened per-table `Vec<KvOp>` preserves entry/op iteration order
/// (a batching mistake here would silently resurrect or drop a posting).
#[tokio::test]
async fn drain_step_batches_multiple_index_postings_across_tables() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("a"));
    repo.add_table_for_test(TableConfig::new("b"));
    let ta = repo.get_table("a").await.unwrap();
    let tb = repo.get_table("b").await.unwrap();
    let token_a = table_token_for("a");
    let token_b = table_token_for("b");

    let gate = repo.tx_gate().await.unwrap();

    let k1 = bytes::Bytes::from_static(b"k1");
    let k2 = bytes::Bytes::from_static(b"k2");
    let k3 = bytes::Bytes::from_static(b"k3");
    let v1 = bytes::Bytes::from_static(b"v1");
    let v2 = bytes::Bytes::from_static(b"v2");
    let v3 = bytes::Bytes::from_static(b"v3");

    seed_inflight_entry(
        &repo,
        vec![
            WalOpV2::IndexPut {
                table_id_interned: token_a,
                idx_id: 0,
                key: k1.clone(),
                value: v1,
            },
            WalOpV2::IndexPut {
                table_id_interned: token_a,
                idx_id: 0,
                key: k2.clone(),
                value: v2.clone(),
            },
            // Same-key set-then-delete within the SAME batch: k1 must end
            // up ABSENT, proving push order into the flat per-table Vec
            // matches entry/op iteration order.
            WalOpV2::IndexDel {
                table_id_interned: token_a,
                idx_id: 0,
                key: k1.clone(),
            },
            WalOpV2::IndexPut {
                table_id_interned: token_b,
                idx_id: 0,
                key: k3.clone(),
                value: v3.clone(),
            },
        ],
        1,
    )
    .await;
    gate.publish_committed_max(1);

    let drainer = Drainer::new();
    let drained = drainer.drain_step(&repo.core).await.unwrap();
    assert_eq!(drained, 1);
    assert_eq!(gate.durable_watermark(), 1);

    assert!(
        ta.info_store().get(k1.into()).await.is_err(),
        "k1 was set then deleted within the same batch — must be absent"
    );
    assert_eq!(ta.info_store().get(k2.into()).await.unwrap(), v2);
    assert_eq!(tb.info_store().get(k3.into()).await.unwrap(), v3);
}

/// The vectorization target: E separate committed entries each contributing
/// ONE posting to the SAME table must coalesce into a single batched write
/// for that table — was previously E separate awaited `info_store` calls
/// PLUS E `table_by_token` resolves.
#[tokio::test]
async fn drain_step_batches_index_postings_across_multiple_entries_same_table() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    let token = table_token_for("t");

    let gate = repo.tx_gate().await.unwrap();

    for i in 1u8..=3 {
        let key = bytes::Bytes::from(vec![b'k', i]);
        let value = bytes::Bytes::from(vec![b'v', i]);
        seed_inflight_entry(
            &repo,
            vec![WalOpV2::IndexPut {
                table_id_interned: token,
                idx_id: 0,
                key,
                value,
            }],
            i as u64,
        )
        .await;
    }
    gate.publish_committed_max(3);

    let drainer = Drainer::new();
    let drained = drainer.drain_step(&repo.core).await.unwrap();
    assert_eq!(drained, 3, "all three entries drained in one pass");
    assert_eq!(gate.durable_watermark(), 3);

    for i in 1u8..=3 {
        let key = bytes::Bytes::from(vec![b'k', i]);
        let expected = bytes::Bytes::from(vec![b'v', i]);
        assert_eq!(tbl.info_store().get(key.into()).await.unwrap(), expected);
    }
}

/// A batch-write failure for one table's index postings must (a) surface —
/// not be silently swallowed as a phantom success — and (b) block
/// finalization ONLY for entries touching that table; an entry touching an
/// unaffected table still drains. Uses the `FAIL_INDEX_BATCH_TABLE_ID`
/// test-only fault injector since no real backend in these unit tests
/// (`InMemoryStore`) can fail `transact` on its own.
#[tokio::test]
async fn drain_step_index_batch_failure_surfaces_and_blocks_only_that_table() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("a"));
    repo.add_table_for_test(TableConfig::new("b"));
    let ta = repo.get_table("a").await.unwrap();
    let tb = repo.get_table("b").await.unwrap();
    let token_a = table_token_for("a");
    let token_b = table_token_for("b");

    let gate = repo.tx_gate().await.unwrap();

    let key_a = bytes::Bytes::from_static(b"posting_a");
    let val_a = bytes::Bytes::from_static(b"val_a");
    let key_b = bytes::Bytes::from_static(b"posting_b");
    let val_b = bytes::Bytes::from_static(b"val_b");

    // v=1 touches table A only (will succeed); v=2 touches table B only
    // (armed to fail). Ascending-v + Phase C contiguity means v=1 finalizes
    // and v=2 stays inflight for a retry.
    seed_inflight_entry(
        &repo,
        vec![WalOpV2::IndexPut {
            table_id_interned: token_a,
            idx_id: 0,
            key: key_a.clone(),
            value: val_a.clone(),
        }],
        1,
    )
    .await;
    seed_inflight_entry(
        &repo,
        vec![WalOpV2::IndexPut {
            table_id_interned: token_b,
            idx_id: 0,
            key: key_b.clone(),
            value: val_b.clone(),
        }],
        2,
    )
    .await;
    gate.publish_committed_max(2);

    crate::tx::drainer::FAIL_INDEX_BATCH_TABLE_ID
        .store(token_b, std::sync::atomic::Ordering::SeqCst);

    let drainer = Drainer::new();
    let drained = drainer.drain_step(&repo.core).await.unwrap();

    // Reset immediately so no other test in this binary can bleed.
    crate::tx::drainer::FAIL_INDEX_BATCH_TABLE_ID.store(0, std::sync::atomic::Ordering::SeqCst);

    assert_eq!(
        drained, 1,
        "only the entry NOT touching the failing table is finalized"
    );
    assert_eq!(gate.durable_watermark(), 1);

    // Table A's posting landed — the failure did not block an unrelated table.
    assert_eq!(ta.info_store().get(key_a.into()).await.unwrap(), val_a);
    // Table B's posting must NOT have landed: the injected failure must not
    // be silently swallowed as a phantom success.
    assert!(
        tb.info_store().get(key_b.clone().into()).await.is_err(),
        "failed batch must not partially or fully apply"
    );

    // The failed entry stays inflight — a retry (now disarmed) converges.
    let drained2 = drainer.drain_step(&repo.core).await.unwrap();
    assert_eq!(drained2, 1, "retry succeeds once the fault is disarmed");
    assert_eq!(gate.durable_watermark(), 2);
    assert_eq!(tb.info_store().get(key_b.into()).await.unwrap(), val_b);
}

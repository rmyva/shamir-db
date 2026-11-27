use super::support::make_repo;
#[cfg(feature = "fjall")]
use super::support::reopen_disk_repo;
use crate::repo::repo_token;
use crate::table::table_manager::table_token_for;
use crate::table::TableConfig;
use shamir_types::types::record_id::RecordId;
use shamir_types::types::value::InnerValue;
use shamir_wal::{WalDurability, WalEntryV2, WalOpV2};
#[tokio::test]
async fn recover_v2_inflight_clean_repo_is_zero() {
    let repo = make_repo();
    let count = repo.recover_v2_inflight().await.unwrap();
    assert_eq!(count, 0, "clean repo has no inflight entries");
}
#[tokio::test]
async fn recover_v2_inflight_replays_and_removes_entries() {
    let repo = make_repo();
    let wal = repo.repo_wal().await.unwrap();
    let mut rid_bytes = [0u8; 16];
    rid_bytes[15] = 42;
    let entry = WalEntryV2::new(
        wal.fresh_txn_id(),
        repo_token(repo.name()),
        vec![WalOpV2::Put {
            table_id_interned: 0,
            rid: RecordId(rid_bytes),
            body: bytes::Bytes::from_static(b"payload"),
        }],
    );
    wal.begin_grouped(&entry, WalDurability::Buffered)
        .await
        .unwrap();
    let inflight = wal.recover().await.unwrap();
    assert_eq!(inflight.len(), 1);
    let count = repo.recover_v2_inflight().await.unwrap();
    assert_eq!(count, 1);
}

#[tokio::test]
async fn recover_v2_inflight_handles_multiple_entries() {
    let repo = make_repo();
    let wal = repo.repo_wal().await.unwrap();
    for i in 0..3u64 {
        let entry = WalEntryV2::new(
            wal.fresh_txn_id(),
            0,
            vec![WalOpV2::CounterDelta {
                table_id_interned: i,
                delta: 1,
            }],
        );
        wal.begin_grouped(&entry, WalDurability::Buffered)
            .await
            .unwrap();
    }
    let count = repo.recover_v2_inflight().await.unwrap();
    assert_eq!(count, 3);
}

#[tokio::test]
async fn recover_v2_inflight_replays_put_applies_to_data_store() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    let wal = repo.repo_wal().await.unwrap();
    let mut rid_bytes = [0u8; 16];
    rid_bytes[15] = 42;
    let rid = RecordId(rid_bytes);
    let token = table_token_for("t");

    let value = InnerValue::Str("recovered".into());
    let body = value.to_bytes().unwrap();
    let entry = WalEntryV2::new(
        wal.fresh_txn_id(),
        0,
        vec![WalOpV2::Put {
            table_id_interned: token,
            rid,
            body,
        }],
    );
    wal.begin_grouped(&entry, WalDurability::Buffered)
        .await
        .unwrap();

    assert!(tbl.get(rid).await.is_err());

    let count = repo.recover_v2_inflight().await.unwrap();
    assert_eq!(count, 1);

    let read_back = tbl.get(rid).await.unwrap();
    assert!(
        matches!(read_back, InnerValue::Str(ref s) if s == "recovered"),
        "expected recovered Str, got {:?}",
        read_back
    );
}

#[tokio::test]
async fn recover_v2_inflight_replays_delete_removes_from_data_store() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();

    let rid = tbl.insert(&InnerValue::Str("doomed".into())).await.unwrap();
    let _ = tbl.get(rid).await.unwrap();

    // C2: reads resolve from the version log. A committed delete supersedes the
    // record's prior versions, so its `commit_version` is a monotonic value
    // ABOVE the insert's — the real commit path stamps it via
    // `with_commit_version`. The bare `WalEntryV2::new(..)` leaves
    // `commit_version = 0` (its 2nd arg is `repo_id_interned`, NOT the version),
    // which would place the recovery tombstone at version 0 — BELOW the insert
    // (version 1) — so the log's max version would still be the insert and the
    // delete would not supersede it. Stamp a realistic commit version.
    let insert_v = tbl
        .mvcc_store_ref()
        .expect("mvcc attached")
        .version_of(rid.to_bytes().as_ref());

    let wal = repo.repo_wal().await.unwrap();
    let token = table_token_for("t");
    let entry = WalEntryV2::new(
        wal.fresh_txn_id(),
        0,
        vec![WalOpV2::Delete {
            table_id_interned: token,
            rid,
        }],
    )
    .with_commit_version(insert_v + 1);
    wal.begin_grouped(&entry, WalDurability::Buffered)
        .await
        .unwrap();

    repo.recover_v2_inflight().await.unwrap();

    assert!(
        tbl.get(rid).await.is_err(),
        "rid should be gone after delete recovery"
    );
}

#[tokio::test]
async fn recover_v2_inflight_skips_counter_delta_replay() {
    // CRIT-3 Option A: counter deltas are applied in the happy-path
    // commit (`commit::commit_tx_inner` Phase 5b) and intentionally
    // SKIPPED by recovery to avoid double-counting after a marker-
    // survived crash. Recovery still consumes the WAL entry and
    // removes the marker; data ops in the same entry replay normally
    // (none here, the test is counter-only).
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    let before = tbl.counter().get().await.unwrap();

    let wal = repo.repo_wal().await.unwrap();
    let token = table_token_for("t");
    let entry = WalEntryV2::new(
        wal.fresh_txn_id(),
        0,
        vec![WalOpV2::CounterDelta {
            table_id_interned: token,
            delta: 5,
        }],
    );
    wal.begin_grouped(&entry, WalDurability::Buffered)
        .await
        .unwrap();

    repo.recover_v2_inflight().await.unwrap();

    let after = tbl.counter().get().await.unwrap();
    assert_eq!(
        after, before,
        "recovery must NOT replay CounterDelta — happy-path commit owns it"
    );
}

#[tokio::test]
async fn recover_v2_inflight_unknown_table_skips_gracefully() {
    let repo = make_repo();
    let wal = repo.repo_wal().await.unwrap();
    let mut rid_bytes = [0u8; 16];
    rid_bytes[15] = 99;

    let entry = WalEntryV2::new(
        wal.fresh_txn_id(),
        0,
        vec![WalOpV2::Put {
            table_id_interned: 99999,
            rid: RecordId(rid_bytes),
            body: bytes::Bytes::from_static(b"orphan"),
        }],
    );
    wal.begin_grouped(&entry, WalDurability::Buffered)
        .await
        .unwrap();

    let count = repo.recover_v2_inflight().await.unwrap();
    assert_eq!(count, 1);
}

#[tokio::test]
async fn recover_v2_inflight_replays_index_put_with_table_id() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    let info = tbl.info_store().clone();

    let wal = repo.repo_wal().await.unwrap();
    let token = table_token_for("t");

    let key = bytes::Bytes::from_static(b"some_posting_key");
    let value = bytes::Bytes::from_static(b"some_posting_value");

    let entry = WalEntryV2::new(
        wal.fresh_txn_id(),
        0,
        vec![WalOpV2::IndexPut {
            table_id_interned: token,
            idx_id: 0,
            key: key.clone(),
            value: value.clone(),
        }],
    );
    wal.begin_grouped(&entry, WalDurability::Buffered)
        .await
        .unwrap();

    repo.recover_v2_inflight().await.unwrap();

    let read_back = info.get(key.into()).await.unwrap();
    assert_eq!(read_back, value);
}

#[tokio::test]
async fn recover_v2_inflight_replays_index_put_broadcast() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("a"));
    repo.add_table_for_test(TableConfig::new("b"));
    let ta = repo.get_table("a").await.unwrap();
    let tb = repo.get_table("b").await.unwrap();

    let wal = repo.repo_wal().await.unwrap();
    let key = bytes::Bytes::from_static(b"broadcast_key");
    let value = bytes::Bytes::from_static(b"broadcast_val");

    let entry = WalEntryV2::new(
        wal.fresh_txn_id(),
        0,
        vec![WalOpV2::IndexPut {
            table_id_interned: 0,
            idx_id: 0,
            key: key.clone(),
            value: value.clone(),
        }],
    );
    wal.begin_grouped(&entry, WalDurability::Buffered)
        .await
        .unwrap();

    repo.recover_v2_inflight().await.unwrap();

    assert_eq!(
        ta.info_store().get(key.clone().into()).await.unwrap(),
        value
    );
    assert_eq!(tb.info_store().get(key.into()).await.unwrap(), value);
}

#[tokio::test]
async fn recover_v2_inflight_replays_index_del() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    let info = tbl.info_store().clone();

    let key = bytes::Bytes::from_static(b"doomed");
    info.set(key.clone().into(), bytes::Bytes::from_static(b"val"))
        .await
        .unwrap();
    assert!(info.get(key.clone().into()).await.is_ok());

    let wal = repo.repo_wal().await.unwrap();
    let token = table_token_for("t");

    let entry = WalEntryV2::new(
        wal.fresh_txn_id(),
        0,
        vec![WalOpV2::IndexDel {
            table_id_interned: token,
            idx_id: 0,
            key: key.clone(),
        }],
    );
    wal.begin_grouped(&entry, WalDurability::Buffered)
        .await
        .unwrap();

    repo.recover_v2_inflight().await.unwrap();
    assert!(
        info.get(key.into()).await.is_err(),
        "key removed by IndexDel"
    );
}

// Renamed from `crash_simulation_inflight_recovery_replays_full_state`.
//
// NOTE: this test does NOT simulate a real crash at the storage/fsync layer. It
// constructs the on-disk state a *real* crash *would* leave behind — an inflight
// WAL entry written to the file segment but never followed by the data_store
// update — by appending the entry directly to the file WAL, then drops the
// original `RepoInstance` and reopens a fresh one over the SAME tempdir (so it
// re-reads the same `*.shamirwal/repo.wal` segment). That exercises the recovery
// replay logic — the useful coverage here — but it does not validate crash
// atomicity at the storage layer; that requires a subprocess-kill harness (TODO
// Stage 7.rest).
//
// F5e: this test is disk-backed (sled tempdir). Under the single WAL write path
// the `Mem` sink is per-instance, so an injected entry would not survive reopen
// of an in-memory repo; the file segment is the medium that genuinely persists
// across a simulated restart.
/// Disk-backed (fjall_raw) crash-matrix test — requires the `fjall` feature.
#[cfg(feature = "fjall")]
#[tokio::test]
async fn replay_inflight_v2_from_simulated_partial_commit_state() {
    let tempdir = tempfile::TempDir::new().expect("tempdir");
    let path = tempdir.path().to_path_buf();
    let mut rid_a_bytes = [0u8; 16];
    rid_a_bytes[15] = 1;
    let rid_a = RecordId(rid_a_bytes);
    let mut rid_b_bytes = [0u8; 16];
    rid_b_bytes[15] = 2;
    let rid_b = RecordId(rid_b_bytes);
    // === Phase A+B: open a disk repo, append an inflight V2 entry (two
    //     Puts + a counter delta) directly to the file WAL WITHOUT applying
    //     it to the data_store — exactly the on-disk shape a crash
    //     mid-commit_tx leaves behind. ===
    {
        let repo1 = reopen_disk_repo("crash_test", path.clone(), vec![TableConfig::new("t")]).await;
        let _tbl1 = repo1.get_table("t").await.unwrap();

        let wal = repo1.repo_wal().await.unwrap();
        let token = table_token_for("t");
        let txn_id = wal.fresh_txn_id();

        let body_a = InnerValue::Str("alice".into()).to_bytes().unwrap();
        let body_b = InnerValue::Str("bob".into()).to_bytes().unwrap();

        let entry = WalEntryV2::new(
            txn_id,
            repo_token(repo1.name()),
            vec![
                WalOpV2::Put {
                    table_id_interned: token,
                    rid: rid_a,
                    body: body_a,
                },
                WalOpV2::Put {
                    table_id_interned: token,
                    rid: rid_b,
                    body: body_b,
                },
                WalOpV2::CounterDelta {
                    table_id_interned: token,
                    delta: 2,
                },
            ],
        );
        // Synced so the entry hits the segment file before the drop below.
        wal.begin_grouped(&entry, WalDurability::Synced)
            .await
            .unwrap();

        // Pre-recovery state: data not visible, counter is 0, entry inflight.
        let tbl_pre = repo1.get_table("t").await.unwrap();
        assert!(tbl_pre.get(rid_a).await.is_err(), "data not yet applied");
        assert!(tbl_pre.get(rid_b).await.is_err());
        assert_eq!(tbl_pre.counter().get().await.unwrap(), 0);
        assert_eq!(wal.recover().await.unwrap().len(), 1);

        // === SIMULATED RESTART === drop without clean shutdown.
        drop(repo1);
    }

    // === Phase C: reopen a fresh instance over the SAME tempdir. ===
    let repo2 = reopen_disk_repo("crash_test", path.clone(), vec![TableConfig::new("t")]).await;
    repo2.get_table("t").await.unwrap();

    // Sanity: repo2's WAL sees the inflight entry from before.
    let wal2 = repo2.repo_wal().await.unwrap();
    assert_eq!(
        wal2.recover().await.unwrap().len(),
        1,
        "inflight WAL entry must survive restart"
    );

    // Phase D: run recovery.
    let count = repo2.recover_v2_inflight().await.unwrap();
    assert_eq!(count, 1);

    // === VERIFY ===
    // Post-recovery: data present in main store.
    //
    // Counter is intentionally NOT applied by recovery — see CRIT-3
    // Option A in `recovery::replay_v2_op`'s CounterDelta branch.
    // The happy-path commit (which never ran in this crash
    // simulation) applies the counter in-memory in Phase 5b before
    // persisting markers in Phase 6.5. A crash before those steps
    // means the counter stays at its pre-tx value; data is still
    // recovered from the WAL.
    let tbl_post = repo2.get_table("t").await.unwrap();

    let v_a = tbl_post.get(rid_a).await.unwrap();
    let v_b = tbl_post.get(rid_b).await.unwrap();
    assert!(matches!(v_a, InnerValue::Str(ref s) if s == "alice"));
    assert!(matches!(v_b, InnerValue::Str(ref s) if s == "bob"));

    assert_eq!(
        tbl_post.counter().get().await.unwrap(),
        0,
        "CRIT-3 Option A: recovery skips CounterDelta replay; \
         counter only advances in the happy-path commit, which \
         never ran in this crash simulation"
    );

    // === IDEMPOTENT RE-RUN ===
    // The file WAL has no per-entry truncation until F6, so a second
    // recovery re-reads and replays the whole segment again. Replay is
    // idempotent at the data layer: the records converge to the same
    // single value and the counter still does not advance.
    repo2.recover_v2_inflight().await.unwrap();
    let v_a2 = tbl_post.get(rid_a).await.unwrap();
    assert!(matches!(v_a2, InnerValue::Str(ref s) if s == "alice"));
    assert_eq!(tbl_post.counter().get().await.unwrap(), 0);
}

/// A3 + A4-recovery: a WAL entry with a non-empty `interner_delta` must
/// have its delta applied to the table's interner BEFORE ops are replayed.
/// This ensures intern-ids referenced in the record body are resolvable.
#[tokio::test]
async fn wal_recovery_applies_interner_delta_before_replay() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    let token = table_token_for("t");

    let wal = repo.repo_wal().await.unwrap();

    let mut rid_bytes = [0u8; 16];
    rid_bytes[15] = 50;
    let rid = RecordId(rid_bytes);

    let body = InnerValue::Str("hello".into()).to_bytes().unwrap();

    // Build entry with an interner_delta that introduces a new field
    // "fresh_field" at id 42 for table token `token`.
    let mut entry = WalEntryV2::new(
        wal.fresh_txn_id(),
        0,
        vec![WalOpV2::Put {
            table_id_interned: token,
            rid,
            body,
        }],
    )
    .with_commit_version(1);
    entry.interner_delta = vec![(token, "fresh_field".to_string(), 42)];

    wal.begin_grouped(&entry, WalDurability::Buffered)
        .await
        .unwrap();

    // Before recovery: the interner should NOT know about "fresh_field".
    {
        let interner = tbl.interner().get().await.unwrap();
        assert!(
            interner.get_ind("fresh_field").is_none(),
            "fresh_field must not exist before recovery"
        );
    }

    let count = repo.recover_v2_inflight().await.unwrap();
    assert_eq!(count, 1);

    // After recovery: the interner MUST know "fresh_field" at id 42.
    {
        let interner = tbl.interner().get().await.unwrap();
        let key = interner.get_ind("fresh_field");
        assert!(
            key.is_some(),
            "fresh_field must be in the interner after recovery"
        );
        assert_eq!(
            key.unwrap().id(),
            42,
            "fresh_field must map to the id from the delta"
        );
    }
}

// HIGH-5: replay must apply entries in `commit_version` order even
// when `txn_id` (the WAL active-key sort order) and `commit_version`
// disagree. Construct two inflight entries that write the same rid:
// a "newer" tx (commit_version = 2) with a low txn_id and an "older"
// tx (commit_version = 1) with a high txn_id. Lexical (txn_id) order
// would replay the older tx LAST and the data store would observe
// the wrong final value. With the sort-by-commit_version fix the
// final value must reflect the higher commit_version.
#[tokio::test]
async fn recover_v2_inflight_replays_in_commit_version_order() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    let token = table_token_for("t");

    let wal = repo.repo_wal().await.unwrap();

    let mut rid_bytes = [0u8; 16];
    rid_bytes[15] = 7;
    let rid = RecordId(rid_bytes);

    let body_newer = InnerValue::Str("newer".into()).to_bytes().unwrap();
    let body_older = InnerValue::Str("older".into()).to_bytes().unwrap();

    // Older tx (commit_version = 1) given a LARGER txn_id so lexical
    // order over WalActiveKey would replay it LAST — overwriting the
    // newer value if we relied on txn_id sort.
    let entry_older = WalEntryV2::new(
        100,
        0,
        vec![WalOpV2::Put {
            table_id_interned: token,
            rid,
            body: body_older,
        }],
    )
    .with_commit_version(1);

    let entry_newer = WalEntryV2::new(
        1,
        0,
        vec![WalOpV2::Put {
            table_id_interned: token,
            rid,
            body: body_newer,
        }],
    )
    .with_commit_version(2);

    wal.begin_grouped(&entry_older, WalDurability::Buffered)
        .await
        .unwrap();
    wal.begin_grouped(&entry_newer, WalDurability::Buffered)
        .await
        .unwrap();

    let count = repo.recover_v2_inflight().await.unwrap();
    assert_eq!(count, 2);

    let read_back = tbl.get(rid).await.unwrap();
    assert!(
        matches!(read_back, InnerValue::Str(ref s) if s == "newer"),
        "expected sort-by-commit_version to leave 'newer' as the final \
         value (commit_version=2 must apply after commit_version=1), \
         got {:?}",
        read_back
    );
}

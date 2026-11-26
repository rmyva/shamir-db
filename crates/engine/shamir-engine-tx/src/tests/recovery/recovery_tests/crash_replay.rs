#[cfg(feature = "fjall")]
use super::support::reopen_disk_repo;
#[cfg(feature = "fjall")]
use crate::repo::{BoxRepoFactory, RepoInstance};
#[cfg(feature = "fjall")]
use crate::table::TableConfig;
#[cfg(feature = "fjall")]
use shamir_query_builder::write::{self, doc};
#[cfg(feature = "fjall")]
use shamir_tx::IsolationLevel;
#[cfg(feature = "fjall")]
use shamir_types::types::record_id::RecordId;
#[cfg(feature = "fjall")]
use shamir_types::types::value::InnerValue;
/// F3 end-to-end process-crash recovery proof over the FILE WAL (level-2).
///
/// Builds a disk-backed (sled-raw) repo, commits a tx through the real tx
/// path (which now appends to the file WAL via `begin_grouped(Buffered)`),
/// then DROPS the `RepoInstance` WITHOUT a clean shutdown — modelling a
/// **process crash** (level-2 / Buffered contract: the OS page cache keeps
/// the WAL segment intact across a process exit). A fresh `RepoInstance` is
/// opened over the SAME tempdir (so it sees the same `*.shamirwal/repo.wal`
/// segment), recovery runs, and the committed record must be present —
/// recovered purely from replaying the file WAL.
///
/// NOTE: this is NOT a power-loss (level-3) test. A true power-loss test
/// would require fsync-then-kill-then-truncate-page-cache, which demands a
/// subprocess harness and is out of scope here.
/// Disk-backed (fjall_raw) crash test — requires the `fjall` feature.
#[cfg(feature = "fjall")]
#[tokio::test]
async fn f3_file_wal_process_crash_recovery_replays_committed_tx() {
    let tempdir = tempfile::TempDir::new().expect("tempdir");
    let path = tempdir.path().to_path_buf();

    // === Phase A: open disk repo, commit a tx via the real tx path. ===
    let rid;
    {
        let factory = BoxRepoFactory::fjall_raw(path.clone());
        let repo = RepoInstance::from_factory_for_recovery_test(
            "f3".into(),
            factory,
            vec![TableConfig::new("t")],
        )
        .await
        .expect("from_factory");
        repo.get_table("t").await.unwrap();

        let (mut tx, _g) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
        let tbl = repo.get_table("t").await.unwrap();
        rid = tbl
            .insert_tx(&InnerValue::Str("durable".into()), Some(&mut tx))
            .await
            .unwrap();
        repo.commit_tx(tx).await.unwrap();

        // === Phase B: SIMULATED CRASH — drop without clean shutdown. ===
        // The committed entry lives in the file WAL (level 2, survives a
        // process crash). We deliberately do NOT flush/checkpoint.
        drop(repo);
    }

    // === Phase C: reopen a fresh instance over the SAME tempdir. ===
    // Use the retry helper: on Windows, sled releases its file lock lazily
    // after drop, so the reopen can occasionally fail on the first attempt.
    let repo2 = reopen_disk_repo("f3", path.clone(), vec![TableConfig::new("t")]).await;
    let tbl2 = repo2.get_table("t").await.unwrap();

    // === Phase D: recovery replays the file WAL. ===
    let recovered = repo2.recover_v2_inflight().await.unwrap();
    assert!(
        recovered >= 1,
        "expected at least one entry replayed from the file WAL, got {recovered}"
    );

    // === VERIFY: the committed record is present (recovered from WAL). ===
    let read_back = tbl2.get(rid).await.unwrap_or_else(|e| {
        panic!("record {rid:?} must be recovered from the file WAL, got error: {e}")
    });
    assert!(
        matches!(read_back, InnerValue::Str(ref s) if s == "durable"),
        "expected recovered Str(\"durable\"), got {read_back:?}"
    );
}

/// F4b-4 end-to-end process-crash recovery proof for a NON-TX write over the
/// FILE WAL (level-2).
///
/// Before F4b the non-tx batch INSERT path emitted a V1 per-table WAL marker
/// (`WalManager::begin_with_delta`/`commit`); F4b routes every non-tx batch
/// write through an implicit Snapshot tx so it folds into ONE `WalEntryV2` on
/// the repo file WAL — exactly like a real tx. F4b-4 removed the now-dead V1
/// emission; this test proves the non-tx write is still crash-recoverable, now
/// purely from the file WAL.
///
/// The query_runner is not easily callable from an engine-level test (it needs
/// a `TableResolver`), so we drive the implicit-tx pipeline directly the same
/// way `run_implicit_batch_tx` does: open a Snapshot tx, mark it implicit,
/// stage the insert via `execute_insert_tx`, and commit. Then DROP the repo
/// WITHOUT clean shutdown (process-crash model, level-2 Buffered contract),
/// reopen over the SAME tempdir, run recovery, and assert the record reads
/// back — recovered purely by replaying the file WAL.
/// Disk-backed (fjall_raw) crash test — requires the `fjall` feature.
#[cfg(feature = "fjall")]
#[tokio::test]
async fn f4b_nontx_insert_crash_recovery() {
    let tempdir = tempfile::TempDir::new().expect("tempdir");
    let path = tempdir.path().to_path_buf();

    // === Phase A: open disk repo, perform a non-tx batch INSERT through the
    //     implicit-tx pipeline (mirrors `run_implicit_batch_tx`). ===
    let rid;
    {
        let factory = BoxRepoFactory::fjall_raw(path.clone());
        let repo = RepoInstance::from_factory_for_recovery_test(
            "f4b".into(),
            factory,
            vec![TableConfig::new("t")],
        )
        .await
        .expect("from_factory");
        let tbl = repo.get_table("t").await.unwrap();

        let op = write::insert("t")
            .row(doc().set("name", "nontx_durable").unwrap())
            .build();

        // Implicit single-op BATCH tx: Snapshot isolation (never aborts on
        // SSI), marked implicit — exactly what the non-tx query_runner branch
        // does.
        let (mut tx, _g) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
        tx.set_implicit(true);
        let wr = tbl
            .execute_insert_tx(
                &op,
                &mut tx,
                true,
                None,
                &shamir_types::access::Actor::System,
            )
            .await
            .unwrap();
        repo.commit_tx(tx).await.unwrap();

        // Capture the assigned record id from the returned result record.
        let id_str = wr.records[0]
            .get_value_owned("_id")
            .and_then(|v| v.as_str().map(str::to_owned))
            .expect("_id present in insert result");
        rid = id_str.parse::<RecordId>().expect("parse RecordId");

        // === Phase B: SIMULATED CRASH — drop without clean shutdown. ===
        // The committed entry lives in the file WAL (level 2, survives a
        // process crash). We deliberately do NOT flush/checkpoint.
        drop(repo);
    }

    // === Phase C: reopen a fresh instance over the SAME tempdir. ===
    let repo2 = reopen_disk_repo("f4b", path.clone(), vec![TableConfig::new("t")]).await;
    let tbl2 = repo2.get_table("t").await.unwrap();

    // === Phase D: recovery replays the file WAL. ===
    let recovered = repo2.recover_v2_inflight().await.unwrap();
    assert!(
        recovered >= 1,
        "expected at least one entry replayed from the file WAL for the \
         non-tx insert, got {recovered}"
    );

    // === VERIFY: the non-tx-inserted record is present (recovered from WAL). ===
    let read_back = tbl2.get(rid).await.unwrap_or_else(|e| {
        panic!("non-tx record {rid:?} must be recovered from the file WAL, got error: {e}")
    });
    let interner = tbl2.interner().get().await.unwrap();
    let qv =
        shamir_types::codecs::interned::inner_value_to_query_value(&read_back, interner).unwrap();
    let name_field = match &qv {
        shamir_types::types::value::QueryValue::Map(m) => m.get("name").cloned(),
        _ => None,
    };
    assert_eq!(
        name_field,
        Some(shamir_types::types::value::QueryValue::Str(
            "nontx_durable".to_string()
        )),
        "expected recovered non-tx record with name=nontx_durable, got {qv:?}"
    );
}

/// C5 recovery invariant: an IMPLICIT insert that introduces a BRAND-NEW field
/// name interns it **directly into base** (not the tx overlay) — but the
/// `(name, base_id)` pair MUST still reach `WalEntryV2.interner_delta` so crash
/// recovery replays it via `touch_with_id` and the record bytes (which encode
/// that base id) decode correctly after a process crash.
///
/// This guards the C5 optimisation: skipping the overlay round-trip on the
/// implicit path must NOT skip the interner-delta WAL emission. We assert the
/// recovered interner maps the new field to the SAME id it had pre-crash
/// (id-preserving recovery), proving the delta carried the exact base id.
/// Disk-backed (fjall_raw) crash test — requires the `fjall` feature.
#[cfg(feature = "fjall")]
#[tokio::test]
async fn c5_implicit_insert_new_field_recovers_with_preserved_id() {
    let tempdir = tempfile::TempDir::new().expect("tempdir");
    let path = tempdir.path().to_path_buf();

    // === Phase A: implicit insert of a record with a brand-new field name. ===
    let rid;
    let pre_crash_id;
    {
        let factory = BoxRepoFactory::fjall_raw(path.clone());
        let repo = RepoInstance::from_factory_for_recovery_test(
            "c5".into(),
            factory,
            vec![TableConfig::new("t")],
        )
        .await
        .expect("from_factory");
        let tbl = repo.get_table("t").await.unwrap();

        // "c5_fresh_field" is guaranteed new to the table's interner.
        let op = write::insert("t")
            .row(doc().set("c5_fresh_field", "v").unwrap())
            .build();

        let (mut tx, _g) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
        tx.set_implicit(true);
        // Overlay MUST stay empty on the implicit path — the field is interned
        // straight into base, so the commit-time overlay-merge is a no-op.
        let wr = tbl
            .execute_insert_tx(
                &op,
                &mut tx,
                true,
                None,
                &shamir_types::access::Actor::System,
            )
            .await
            .unwrap();
        assert!(
            tx.interner_overlay.is_empty(),
            "implicit insert must intern into base, leaving the overlay empty"
        );
        repo.commit_tx(tx).await.unwrap();

        let interner = tbl.interner().get().await.unwrap();
        pre_crash_id = interner
            .get_ind("c5_fresh_field")
            .expect("field must be in base interner after implicit insert")
            .id();

        let id_str = wr.records[0]
            .get_value_owned("_id")
            .and_then(|v| v.as_str().map(str::to_owned))
            .expect("_id present in insert result");
        rid = id_str.parse::<RecordId>().expect("parse RecordId");

        // === Phase B: SIMULATED CRASH — drop without clean shutdown. ===
        drop(repo);
    }

    // === Phase C: reopen over the SAME tempdir; the field name is NOT yet
    //     known to the freshly-built interner. ===
    let repo2 = reopen_disk_repo("c5", path.clone(), vec![TableConfig::new("t")]).await;
    let tbl2 = repo2.get_table("t").await.unwrap();
    {
        let interner = tbl2.interner().get().await.unwrap();
        assert!(
            interner.get_ind("c5_fresh_field").is_none(),
            "field must not exist before recovery (proves we rely on the WAL delta)"
        );
    }

    // === Phase D: recovery replays the file WAL (interner_delta first). ===
    let recovered = repo2.recover_v2_inflight().await.unwrap();
    assert!(
        recovered >= 1,
        "expected >=1 replayed entry, got {recovered}"
    );

    // === VERIFY: the field id is preserved AND the record decodes. ===
    {
        let interner = tbl2.interner().get().await.unwrap();
        let recovered_id = interner
            .get_ind("c5_fresh_field")
            .expect("field must be in interner after recovery (from WAL delta)")
            .id();
        assert_eq!(
            recovered_id, pre_crash_id,
            "recovery must preserve the exact base id the record bytes encode"
        );
    }
    let read_back = tbl2.get(rid).await.unwrap_or_else(|e| {
        panic!("record {rid:?} must be recovered from the file WAL, got error: {e}")
    });
    let interner = tbl2.interner().get().await.unwrap();
    let qv =
        shamir_types::codecs::interned::inner_value_to_query_value(&read_back, interner).unwrap();
    let c5_field = match &qv {
        shamir_types::types::value::QueryValue::Map(m) => m.get("c5_fresh_field").cloned(),
        _ => None,
    };
    assert_eq!(
        c5_field,
        Some(shamir_types::types::value::QueryValue::Str("v".to_string())),
        "recovered record must decode the new field correctly, got {qv:?}"
    );
}

/// F3 file-WAL replay idempotency: running `recover_v2_inflight` more than
/// once must converge to the same DATA state and never corrupt or duplicate
/// records.
///
/// The file WAL replays the WHOLE segment on every restart (there is no
/// per-entry truncation until the F6 checkpoint), so `replay_v2_entry` is
/// documented as idempotent at the data layer. This test makes that contract
/// explicit:
///
/// 1. Commit a tx on a disk repo (sled_raw + tempdir) — same setup as the
///    f3 process-crash test.
/// 2. Reopen the repo (simulated process crash).
/// 3. Run `recover_v2_inflight()` twice on the same open instance. The second
///    call will re-read the segment and replay the same entries again (file WAL
///    has no per-entry truncation until F6) — both calls will return >= 1.
/// 4. Assert the record reads back as the single correct value after both runs
///    (no duplication, no corruption).
/// Disk-backed (fjall_raw) idempotence test — requires the `fjall` feature.
#[cfg(feature = "fjall")]
#[tokio::test]
async fn f3_file_wal_replay_is_idempotent() {
    let tempdir = tempfile::TempDir::new().expect("tempdir");
    let path = tempdir.path().to_path_buf();

    // === Phase A: open disk repo, commit a tx. ===
    let rid;
    {
        let factory = BoxRepoFactory::fjall_raw(path.clone());
        let repo = RepoInstance::from_factory_for_recovery_test(
            "f3_idem".into(),
            factory,
            vec![TableConfig::new("t")],
        )
        .await
        .expect("from_factory");
        repo.get_table("t").await.unwrap();

        let (mut tx, _g) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
        let tbl = repo.get_table("t").await.unwrap();
        rid = tbl
            .insert_tx(&InnerValue::Str("idempotent".into()), Some(&mut tx))
            .await
            .unwrap();
        repo.commit_tx(tx).await.unwrap();

        // Simulated process crash — drop without clean shutdown.
        drop(repo);
    }

    // === Phase B: reopen over the same tempdir. ===
    // Use the retry helper: on Windows, sled releases its file lock lazily
    // after drop, so the reopen can occasionally fail on the first attempt.
    let repo2 = reopen_disk_repo("f3_idem", path.clone(), vec![TableConfig::new("t")]).await;
    let tbl2 = repo2.get_table("t").await.unwrap();

    // === Phase C: run recovery TWICE on the same open instance. ===
    let first = repo2.recover_v2_inflight().await.unwrap();
    assert!(
        first >= 1,
        "first recovery must replay at least one entry, got {first}"
    );

    // The second call re-reads the file segment (no F6 truncation yet) and
    // replays the same entries — idempotency means the DATA is unchanged, not
    // that the count is zero. Assert only that it does not panic/error.
    let second = repo2.recover_v2_inflight().await.unwrap();
    assert!(
        second >= 1,
        "second recovery also replays the segment (file WAL: no truncation until F6), got {second}"
    );

    // === VERIFY: single correct value after double replay — no corruption. ===
    let read_back = tbl2.get(rid).await.unwrap_or_else(|e| {
        panic!("record {rid:?} must be present after idempotent recovery, got: {e}")
    });
    assert!(
        matches!(read_back, InnerValue::Str(ref s) if s == "idempotent"),
        "expected Str(\"idempotent\") after double replay, got {read_back:?}"
    );
}

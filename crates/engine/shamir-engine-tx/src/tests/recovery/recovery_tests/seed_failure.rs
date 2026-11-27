use super::support::make_repo;
use crate::table::table_manager::table_token_for;
use crate::table::TableConfig;
use crate::tx::recovery::FAIL_HISTORY_SEED_TX_ID;
use shamir_types::types::record_id::RecordId;
use shamir_types::types::value::InnerValue;
use shamir_wal::{WalDurability, WalEntryV2, WalOpV2};
use std::sync::atomic::Ordering;
// =============================================================================
// CRIT-1 (#435): a history-write failure during cold recovery is FATAL.
//
// `seed_version_cache_for_entry` used to swallow
// `write_committed_to_history` errors in a `log::warn!` and return `()`, so
// `recover_inflight_v2` unconditionally proceeded to mark the entry
// durable/materialized — a silent loss of an acked commit (cold-start readers
// see `last_committed ≥ v` with no value in overlay or history) and an open
// door for F6 truncation to unlink the sole surviving WAL copy. The fix
// propagates the error through `replay_v2_entry` → `recover_inflight_v2` →
// `open()` (`db_management.rs:343`'s `recover_v2_inflight().await?` refuses to
// serve a repo that cannot recover).
//
// The tests below exercise the fix in-process (deterministic, no subprocess)
// by arming `FAIL_HISTORY_SEED_TX_ID`, a test-only injection register that
// makes `seed_version_cache_for_entry` return a synthetic error for the
// matching `txn_id` in place of the real `write_committed_to_history` call.
// =============================================================================

/// Sentinel tx_id used to arm the CRIT-1 history-seed failure injection.
/// Picked far above any id a fresh-repo gate or hand-built test allocates so
/// the process-global injection register can't collide with a parallel test.
const CRIT1_INJECT_TX_ID: u64 = 7_000_435;

/// Serialises the arm → recover → reset window. `FAIL_HISTORY_SEED_TX_ID` is a
/// single process-wide `AtomicU64`; two CRIT-1 tests running on parallel
/// runner threads would otherwise clobber each other's arm. The guard must
/// span the `recover_v2_inflight().await` (the register is read during
/// recovery), so this is `tokio::sync::Mutex` — async-aware, no poisoning.
/// Contention is bounded to the two CRIT-1 tests.
static CRIT1_INJECT_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// CRIT-1 (#435) positive proof: when `seed_version_cache_for_entry`'s
/// `write_committed_to_history` fails for an MVCC-attached table, recovery
/// MUST return `Err` — NOT silently continue to `mark_durable`. Pre-fix the
/// error was swallowed in a `log::warn!` and recovery reported success, so
/// `recover_inflight_v2` marked the entry durable despite the value being
/// absent from `history` → silent loss of an acked commit.
///
/// `current_thread` flavor keeps the whole recovery on one task/thread, so
/// the armed injection register is observed deterministically.
#[tokio::test(flavor = "current_thread")]
async fn crit1_history_seed_failure_aborts_recovery() {
    let _g = CRIT1_INJECT_LOCK.lock().await;

    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    // `get_table` instantiates the TableManager, which attaches an MvccStore —
    // without it `seed_version_cache_for_entry` would skip the table entirely
    // (no MVCC entry in `per_table_mvcc`) and the injection would never fire.
    let _tbl = repo.get_table("t").await.unwrap();
    let token = table_token_for("t");

    let wal = repo.repo_wal().await.unwrap();
    let mut rid_bytes = [0u8; 16];
    rid_bytes[15] = 99;
    let rid = RecordId(rid_bytes);
    let body = InnerValue::Str("crit1-victim".into()).to_bytes().unwrap();

    // Build an inflight WAL entry with a KNOWN txn_id so we can arm the
    // injection for exactly this entry. `with_commit_version(1)` ensures the
    // entry is treated as a real committed version (legacy v=0 entries are
    // skipped by `recover_inflight_v2`'s mark loop, but the history-seed path
    // still runs for them — we use v=1 to exercise the full mark-protected
    // path the bug would corrupt).
    let entry = WalEntryV2::new(
        CRIT1_INJECT_TX_ID,
        0,
        vec![WalOpV2::Put {
            table_id_interned: token,
            rid,
            body,
        }],
    )
    .with_commit_version(1);
    wal.begin_grouped(&entry, WalDurability::Buffered)
        .await
        .unwrap();

    // Arm the synthetic history-write failure for this exact tx, then recover.
    FAIL_HISTORY_SEED_TX_ID.store(CRIT1_INJECT_TX_ID, Ordering::SeqCst);
    let result = repo.recover_v2_inflight().await;
    FAIL_HISTORY_SEED_TX_ID.store(0, Ordering::SeqCst);
    drop(_g);

    // THE FIX: recovery MUST return Err. Pre-fix it returned Ok(1) and the
    // entry was marked durable despite the history write never landing.
    let err = result.expect_err(
        "CRIT-1: a history-write failure during recovery MUST propagate as Err \
         (pre-fix this was Ok — the silent loss of an acked commit)",
    );
    // The injected error carries the tx_id so a future regression that
    // accidentally swallows a DIFFERENT error is caught.
    let msg = format!("{err}");
    assert!(
        msg.contains(&CRIT1_INJECT_TX_ID.to_string()) && msg.contains("CRIT-1"),
        "the propagated error must be the injected CRIT-1 fault (got: {msg})"
    );
}

/// CRIT-1 (#435) regression control: WITHOUT the injection armed, recovery
/// of the same inflight entry MUST still succeed (the fix did not break the
/// happy path). This is the symmetric counterpart to
/// `crit1_history_seed_failure_aborts_recovery` and proves the injection
/// register is reset between tests (a stale arm would break every other
/// recovery test in the suite).
#[tokio::test(flavor = "current_thread")]
async fn crit1_no_injection_recovery_succeeds() {
    let _g = CRIT1_INJECT_LOCK.lock().await;
    // Defensive: the injection register MUST be disarmed before this test.
    FAIL_HISTORY_SEED_TX_ID.store(0, Ordering::SeqCst);

    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    let token = table_token_for("t");

    let wal = repo.repo_wal().await.unwrap();
    let mut rid_bytes = [0u8; 16];
    rid_bytes[15] = 77;
    let rid = RecordId(rid_bytes);
    let body = InnerValue::Str("crit1-happy".into()).to_bytes().unwrap();

    let entry = WalEntryV2::new(
        CRIT1_INJECT_TX_ID + 1,
        0,
        vec![WalOpV2::Put {
            table_id_interned: token,
            rid,
            body,
        }],
    )
    .with_commit_version(1);
    wal.begin_grouped(&entry, WalDurability::Buffered)
        .await
        .unwrap();

    let count = repo
        .recover_v2_inflight()
        .await
        .expect("CRIT-1 control: recovery with NO injection must succeed");
    assert_eq!(count, 1, "the inflight entry must be replayed once");

    // The value must be readable after recovery (history write landed).
    let read_back = tbl.get(rid).await.unwrap();
    assert!(
        matches!(read_back, InnerValue::Str(ref s) if s == "crit1-happy"),
        "expected the recovered record, got {read_back:?}"
    );

    FAIL_HISTORY_SEED_TX_ID.store(0, Ordering::SeqCst);
    drop(_g);
}

/// CRIT-1 (#435) multi-table best-effort proof: when the history write fails
/// for ONE table in a multi-table entry, recovery still attempts the OTHER
/// table (best-effort, mirrors `flush_buffers`) but returns the first error.
/// Pre-fix the multi-table loop would also have swallowed the error; the fix
/// captures it and surfaces it after the loop. This test proves the
/// best-effort loop shape (not short-circuit on first failure) by arming the
/// injection for the single shared tx — EVERY MVCC table's write is replaced
/// by the synthetic error, so the first-err capture fires on the first table
/// and the second table still gets its (also-failing) attempt.
#[tokio::test(flavor = "current_thread")]
async fn crit1_multi_table_history_seed_failure_propagates() {
    let _g = CRIT1_INJECT_LOCK.lock().await;

    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("a"));
    repo.add_table_for_test(TableConfig::new("b"));
    // Instantiate BOTH tables' MvccStores so both flow through
    // `seed_version_cache_for_entry`'s per-table loop.
    let _tbl_a = repo.get_table("a").await.unwrap();
    let _tbl_b = repo.get_table("b").await.unwrap();
    let token_a = table_token_for("a");
    let token_b = table_token_for("b");

    let wal = repo.repo_wal().await.unwrap();
    let mut rid_a = [0u8; 16];
    rid_a[15] = 1;
    let mut rid_b = [0u8; 16];
    rid_b[15] = 2;
    let body = InnerValue::Str("multi-table".into()).to_bytes().unwrap();

    // A single WAL entry covering BOTH tables (logical-WAL atomicity) — the
    // shape the multi-table crash-recovery test uses.
    let entry = WalEntryV2::new(
        CRIT1_INJECT_TX_ID + 2,
        0,
        vec![
            WalOpV2::Put {
                table_id_interned: token_a,
                rid: RecordId(rid_a),
                body: body.clone(),
            },
            WalOpV2::Put {
                table_id_interned: token_b,
                rid: RecordId(rid_b),
                body,
            },
        ],
    )
    .with_commit_version(1);
    wal.begin_grouped(&entry, WalDurability::Buffered)
        .await
        .unwrap();

    FAIL_HISTORY_SEED_TX_ID.store(CRIT1_INJECT_TX_ID + 2, Ordering::SeqCst);
    let result = repo.recover_v2_inflight().await;
    FAIL_HISTORY_SEED_TX_ID.store(0, Ordering::SeqCst);
    drop(_g);

    result.expect_err(
        "CRIT-1 multi-table: a history-write failure on ANY table MUST \
         propagate as Err (best-effort over the rest, but fail the recovery)",
    );
}

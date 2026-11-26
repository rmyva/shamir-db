use std::sync::Arc;

use super::support::make_repo;
use crate::repo::{BoxRepo, RepoInstance};
use crate::table::interner_manager::InternerManager;
use crate::table::table_manager::table_token_for;
use crate::table::TableConfig;
use crate::tx::pre_commit::REPO_INTERNER_SCOPE;
use shamir_storage::storage_in_memory::InMemoryRepo;
use shamir_storage::types::Repo;
use shamir_types::types::record_id::RecordId;
use shamir_types::types::value::InnerValue;
use shamir_wal::{WalDurability, WalEntryV2, WalOpV2};
// =============================================================================
// A11 — recovery: `wal.commit` (WAL marker finalization) without the A5
// interner-hwm gate and without persisting the interner.
//
// Mirror of the drainer's CRIT-2 fix, but on the COLD recovery path. The
// drainer carefully gates WAL truncation on the interner's
// `persisted_high_water()` (so a crash never deletes the sole surviving copy
// of a (name, id) mapping the history records reference). `recover_inflight_v2`
// used to replay each entry's `interner_delta` into the IN-MEMORY interner
// only (`touch_with_id`) and then finalize the entry — with NO call to
// `repo_interner.persist()`. A second crash between recovery and the first
// post-recovery interner checkpoint would lose every replayed mapping: the
// in-memory interner of the now-dead process is gone, the persistent chunk
// store never learned the ids, but the history records (written during
// recovery's `replay_v2_entry`, which IS durable) still reference them.
//
// The fix: after replaying all entries and BEFORE finalizing any of them,
// force ONE `repo_interner.persist()` so every replayed delta is durably
// checkpointed before recovery returns.
// =============================================================================

/// A11 double-crash reproduction (RED pre-fix, GREEN post-fix).
///
/// Seeds a WAL with one inflight entry whose `interner_delta` introduces a
/// NEW id (above the interner's current `persisted_high_water()`), then runs
/// `recover_v2_inflight`. The load-bearing assertion — the one that flips
/// from RED to GREEN under the fix — is that by the time recovery returns,
/// the persistent interner covers the delta's id
/// (`persisted_high_water() >= delta_id`). Pre-fix this fails: recovery
/// finalizes the entry without ever persisting, so a second crash before the
/// next checkpoint would leave history records referencing an id the
/// persistent interner never learned.
///
/// Uses id 1 (the FIRST dense id on a fresh repo) so `persist()`'s
/// gap-free high-water scan advances `persisted_high_water` from 0 to 1.
/// This mirrors the realistic case: the original committer allocated the id
/// densely via `touch_ind`, and the WAL delta carries that same dense id.
#[tokio::test]
async fn a11_recovery_persists_interner_delta_before_finalize() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let _tbl = repo.get_table("t").await.unwrap();
    let token = table_token_for("t");

    let wal = repo.repo_wal().await.unwrap();

    let mut rid_bytes = [0u8; 16];
    rid_bytes[15] = 11;
    let rid = RecordId(rid_bytes);
    let body = InnerValue::Str("a11_body".into()).to_bytes().unwrap();

    // The new id introduced by this entry's delta. Id 1 is the first dense
    // id on a fresh repo (current_id starts at 0, first touch_ind returns
    // id 1) — so `persist()`'s gap-free high-water scan advances
    // `persisted_high_water` from 0 to 1 when this delta is flushed.
    const DELTA_ID: u64 = 1;
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
    entry.interner_delta = vec![(REPO_INTERNER_SCOPE, "a11_field".to_string(), DELTA_ID)];

    wal.begin_grouped(&entry, WalDurability::Buffered)
        .await
        .unwrap();

    // Precondition: the persistent interner does NOT yet cover DELTA_ID —
    // this is what makes the entry's delta "live only in WAL + memory"
    // until something persists it.
    {
        let repo_interner = repo.repo_interner().await.unwrap();
        let hwm = repo_interner.persisted_high_water() as u64;
        assert!(
            hwm < DELTA_ID,
            "test precondition: persisted_high_water ({hwm}) must be < DELTA_ID ({DELTA_ID}) \
             so the delta is genuinely new",
        );
    }

    let count = repo.recover_v2_inflight().await.unwrap();
    assert_eq!(count, 1, "exactly one entry must be replayed");

    // The in-memory interner DID learn the mapping (replay applies the delta
    // via `touch_with_id` before data ops).
    {
        let repo_interner = repo.repo_interner().await.unwrap();
        let interner = repo_interner.get().await.unwrap();
        let key = interner.get_ind("a11_field");
        assert!(
            key.is_some(),
            "recovery must apply the interner delta to the in-memory interner"
        );
        assert_eq!(
            key.unwrap().id(),
            DELTA_ID,
            "a11_field must map to the delta's id"
        );
    }

    // *** THE A11 INVARIANT *** — fails pre-fix, passes post-fix.
    //
    // The persistent interner chunk store MUST cover DELTA_ID by the time
    // recovery returns. Without this, the "double crash" interleaving
    // (crash → recovery → crash before the next checkpoint) loses the
    // mapping: the in-memory interner is gone, the WAL segment may have
    // been truncated by a subsequent drainer pass trusting the
    // (un-persisted) in-memory state, and history records referencing
    // DELTA_ID become undecodable.
    let repo_interner = repo.repo_interner().await.unwrap();
    let hwm_after = repo_interner.persisted_high_water() as u64;
    assert!(
        hwm_after >= DELTA_ID,
        "A11: after recovery, persisted_high_water ({hwm_after}) MUST cover the \
         replayed delta's id ({DELTA_ID}); otherwise a second crash before \
         the next interner checkpoint loses the mapping (audit A11)",
    );
}

/// A11 strong proof — "double crash" via interner reload.
///
/// The `persisted_high_water` assertion in the sibling test above only works
/// for dense ids (the gap-free scan in `entries_after` advances the hwm). For
/// SPARSE ids (e.g. recovery replaying an entry whose delta carries id 500
/// with no 1..499 in the interner), `persisted_high_water` legitimately
/// cannot advance past the gap — but the (name, id) mapping is STILL durably
/// written to a chunk and IS recoverable on a reload. This test proves that
/// directly: after recovery, construct a FRESH `InternerManager` against the
/// SAME backing store (simulating process restart where every in-memory
/// structure is gone) and assert the mapping is present. Pre-fix, no persist
/// happened, so the fresh manager sees nothing.
#[tokio::test]
async fn a11_recovery_delta_survives_interner_reload() {
    let backing = Arc::new(InMemoryRepo::new());
    let repo = RepoInstance::new(
        "a11_reload".into(),
        BoxRepo::InMemory(Arc::clone(&backing)),
        Vec::new(),
    );
    repo.add_table_for_test(TableConfig::new("t"));
    let _tbl = repo.get_table("t").await.unwrap();
    let token = table_token_for("t");

    let wal = repo.repo_wal().await.unwrap();

    let mut rid_bytes = [0u8; 16];
    rid_bytes[15] = 11;
    let rid = RecordId(rid_bytes);
    let body = InnerValue::Str("a11_sparse".into()).to_bytes().unwrap();

    // A SPARSE id — well above any dense floor, with gaps below it. This is
    // the shape `touch_with_id` produces when recovery replays an entry
    // whose original committer allocated ids out of band (e.g. via
    // replication or a restored backup). `persisted_high_water` will NOT
    // advance past the gap, but `persist()` still writes the chunk.
    const SPARSE_ID: u64 = 500;
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
    entry.interner_delta = vec![(REPO_INTERNER_SCOPE, "a11_sparse".to_string(), SPARSE_ID)];
    wal.begin_grouped(&entry, WalDurability::Buffered)
        .await
        .unwrap();

    // Force the repo interner to initialize (so the `__interner__` store
    // exists in the backing repo for the reload below).
    let _ = repo.repo_interner().await.unwrap();

    let count = repo.recover_v2_inflight().await.unwrap();
    assert_eq!(count, 1, "exactly one entry must be replayed");

    // Simulate the "second crash": build a FRESH InternerManager against the
    // same `__interner__` store, by-passing the live process's in-memory
    // cache. If recovery persisted the delta, the fresh manager's lazy
    // `get()` will load the chunk and know the mapping. Pre-fix, the chunk
    // was never written, so the fresh manager sees nothing.
    let interner_store = backing.store_get("__interner__").await.unwrap();
    let reloaded = InternerManager::new(interner_store);
    let fresh = reloaded.get().await.unwrap();
    let key = fresh.get_ind("a11_sparse");
    assert!(
        key.is_some(),
        "A11 (reload proof): after recovery, a FRESH interner loaded from the \
         durable store MUST know 'a11_sparse' (id {SPARSE_ID}); pre-fix the chunk \
         was never written so the mapping would be lost on a second crash"
    );
    assert_eq!(
        key.unwrap().id(),
        SPARSE_ID,
        "a11_sparse must map to the persisted id"
    );
}

/// A11 regression guard: the COMMON case — an entry with NO interner delta
/// (or whose delta is already covered by a pre-existing checkpoint) — must
/// still replay and finalize normally, and must NOT cause any spurious
/// failure from the new persist step (persist is a no-op when there is
/// nothing new to write).
#[tokio::test]
async fn a11_recovery_no_delta_replays_and_finalizes_normally() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    let token = table_token_for("t");

    let wal = repo.repo_wal().await.unwrap();

    let mut rid_bytes = [0u8; 16];
    rid_bytes[15] = 22;
    let rid = RecordId(rid_bytes);
    let body = InnerValue::Str("no_delta".into()).to_bytes().unwrap();

    // No interner_delta — the common case for entries that don't intern any
    // new field names.
    let entry = WalEntryV2::new(
        wal.fresh_txn_id(),
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

    let count = repo.recover_v2_inflight().await.unwrap();
    assert_eq!(count, 1, "the no-delta entry must replay");

    // The data op landed.
    let got = tbl.get(rid).await.unwrap();
    assert!(
        matches!(got, InnerValue::Str(ref s) if s == "no_delta"),
        "the replayed Put must be readable post-recovery; got {got:?}"
    );

    // `persisted_high_water` is unchanged (no delta was introduced) — the
    // new persist call must have been a no-op, NOT an error.
    let repo_interner = repo.repo_interner().await.unwrap();
    assert_eq!(
        repo_interner.persisted_high_water(),
        0,
        "no delta → persisted_high_water must stay at 0"
    );
}

/// A11 regression guard: an entry whose delta is ALREADY covered by a
/// pre-existing checkpoint (id ≤ `persisted_high_water()`) must replay and
/// finalize without forcing a redundant persist that could fail.
#[tokio::test]
async fn a11_recovery_already_persisted_delta_replays_normally() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    let token = table_token_for("t");

    // Pre-intern and PERSIST a name so its id sits below the high-water mark.
    {
        let repo_interner = repo.repo_interner().await.unwrap();
        let interner = repo_interner.get().await.unwrap();
        let _key = interner.touch_ind("pre_persisted").expect("touch_ind ok");
        repo_interner.persist().await.unwrap();
    }
    let repo_interner = repo.repo_interner().await.unwrap();
    let hwm_before = repo_interner.persisted_high_water() as u64;
    let persisted_id = repo_interner
        .get()
        .await
        .unwrap()
        .get_ind("pre_persisted")
        .expect("pre_persisted interned above")
        .id();
    assert!(
        persisted_id <= hwm_before,
        "precondition: persisted_id ({persisted_id}) must be ≤ hwm ({hwm_before})"
    );

    let wal = repo.repo_wal().await.unwrap();
    let mut rid_bytes = [0u8; 16];
    rid_bytes[15] = 33;
    let rid = RecordId(rid_bytes);
    let body = InnerValue::Str("covered".into()).to_bytes().unwrap();

    // Delta references an id that is ALREADY below the persisted hwm.
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
    entry.interner_delta = vec![(
        REPO_INTERNER_SCOPE,
        "pre_persisted".to_string(),
        persisted_id,
    )];
    wal.begin_grouped(&entry, WalDurability::Buffered)
        .await
        .unwrap();

    let count = repo.recover_v2_inflight().await.unwrap();
    assert_eq!(count, 1, "the already-covered entry must replay");

    let got = tbl.get(rid).await.unwrap();
    assert!(
        matches!(got, InnerValue::Str(ref s) if s == "covered"),
        "replayed Put must be readable; got {got:?}"
    );

    // `persisted_high_water` did not regress and the persist call (which the
    // fix adds unconditionally) was either a no-op or advanced nothing
    // harmful — the already-covered case stays covered.
    let hwm_after = repo.repo_interner().await.unwrap().persisted_high_water() as u64;
    assert!(
        hwm_after >= persisted_id,
        "already-persisted id stays covered: hwm_after={hwm_after} id={persisted_id}"
    );
}

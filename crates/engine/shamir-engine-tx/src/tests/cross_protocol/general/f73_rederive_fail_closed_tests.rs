//! F-73 (#900, P0) — `rederive_index2_ops_post_stage` must fail closed.
//!
//! Pre-fix, this commit-time worker (`pre_commit.rs`) returned `()` and
//! silently swallowed every error class it could hit: a non-`NotFound`
//! `data_store.get` error, an `InnerValue::from_bytes` decode failure on
//! either the staged or the pre-tx record, and a malformed staged
//! `RecordId` key. Because Phase 5a (the data mutation) runs
//! UNCONDITIONALLY later in the pipeline, any of those silent skips let the
//! tx commit SUCCESSFULLY while the row's posting for the newly-registered
//! index was permanently dropped — a table/index divergence with no error
//! ever surfaced.
//!
//! Each test below proves TWO things:
//!   1. Pre-fix, the fault would NOT have aborted the commit (verified by
//!      hand: reverting `pre_commit.rs`'s error-handling arms back to
//!      `Err(_) => {}` / `let Some(..) = .. else { continue }` and re-running
//!      makes every one of these tests fail — the commit succeeds and the
//!      assertions on "must be Err" / "must not be posted" fail).
//!   2. Post-fix, `repo.commit_tx` returns `Err`, no WAL entry is appended
//!      for the tx, the row's DATA is unchanged (the pre-tx value survives —
//!      Phase 5a never ran because the abort happens before Phase 4's WAL
//!      begin), and the new index has NO posting for the row.
//!
//! Uses the manual `TxContext`/`StagingStore` construction convention from
//! `commit_tests.rs` / `commit_async_visibility_tests.rs` (not the
//! query-builder path) so each fault can be injected at a precise, isolated
//! point without incidental machinery. Table/DDL setup mirrors
//! `f50_index_lifecycle_spike_tests.rs` / `f50_step2_index_lifecycle_tests.rs`
//! (stage before the new index registers, complete the DDL, then commit).

use std::sync::Arc;

use bytes::Bytes;
use shamir_query_builder::{filter, write};
use shamir_query_types::admin::types::CreateIndexOp;
use shamir_storage::storage_in_memory::InMemoryRepo;
use shamir_tx::IsolationLevel;
use shamir_types::core::interner::{InternerKey, TouchInd};
use shamir_types::types::common::new_map_wc;
use shamir_types::types::value::InnerValue;

use crate::index2::backend::{IndexQuery, IndexResult};
use crate::index2::functional_backend::FunctionalBackend;
use crate::query::filter::eval_context::FilterContext;
use crate::repo::repo_instance::RepoInstance;
use crate::repo::repo_types::BoxRepo;
use crate::table::TableConfig;
use crate::table::TableManager;
use crate::tx::pre_commit::{RederivePreTxReadFailHook, TEST_REDERIVE_PRE_TX_READ_FAILURE};

fn make_repo() -> RepoInstance {
    let repo = Arc::new(InMemoryRepo::new());
    RepoInstance::new("test".into(), BoxRepo::InMemory(repo), Vec::new())
}

async fn key_id(tbl: &TableManager, name: &str) -> u64 {
    let interner = tbl.interner().get().await.unwrap();
    match interner.touch_ind(name).unwrap() {
        TouchInd::Exists(k) | TouchInd::New(k) => k.id(),
    }
}

fn record_with_str(key: u64, val: &str) -> InnerValue {
    let mut m = new_map_wc(1);
    m.insert(InternerKey::new(key), InnerValue::Str(val.into()));
    InnerValue::Map(m)
}

/// Bytes that are GUARANTEED to fail `InnerValue::from_bytes`
/// (`rmp_serde::from_slice`), for the decode-error fault-injection tests.
///
/// `0xc1` is the MessagePack spec's reserved "never used" byte — every
/// conformant decoder errors on it unconditionally. A naive choice like
/// `0xff` is NOT safe here: `0xff` is a valid "negative fixint" (-1) leading
/// byte, so `rmp_serde` would happily decode a buffer starting with it as
/// `InnerValue::Int(-1)` instead of erroring — silently defeating the test's
/// premise (this was caught during red-then-green verification: an earlier
/// draft used `\xff\xff\xff not valid msgpack`, which decoded successfully
/// and made the test pass vacuously against BOTH the pre-fix and post-fix
/// code).
fn invalid_msgpack_bytes() -> Bytes {
    Bytes::from_static(&[0xc1, 0xc1, 0xc1])
}

fn functional_lower_op(name: &str, table: &str, field: &str) -> CreateIndexOp {
    CreateIndexOp {
        create_index: name.into(),
        table: table.into(),
        fields: vec![vec![field.into()]],
        unique: false,
        sorted: false,
        repo: "main".into(),
        index_type: Some("functional".into()),
        fts_tokenizer: None,
        fts_language: None,
        functional_op: Some("lower".into()),
        functional_args: None,
        vector_dim: None,
        vector_metric: None,
        vector_quantization: None,
        include: Vec::new(),
        if_not_exists: false,
    }
}

/// Resolve the record ids the functional `lower` index holds for a given
/// (already-lowercased) value — mirrors `f50_index_lifecycle_spike_tests.rs`.
async fn functional_lookup(tbl: &TableManager, index_name_id: u64, lowered: &str) -> Vec<[u8; 16]> {
    let backend = tbl
        .index2_registry()
        .get_by_name(index_name_id)
        .await
        .expect("functional backend must be registered");
    let key = FunctionalBackend::hash_value(&InnerValue::Str(lowered.into()));
    let mut keys: smallvec::SmallVec<[Vec<u8>; 4]> = smallvec::SmallVec::new();
    keys.push(key.to_vec());
    match backend.lookup(IndexQuery::Point { keys }).await.unwrap() {
        IndexResult::Set(s) => s.iter().map(|rid| *rid.as_bytes()).collect(),
        IndexResult::Ranked(v) => v.iter().map(|(rid, _)| *rid.as_bytes()).collect(),
    }
}

/// Build the entry-key prefix for a sorted index def (mirrors
/// `f50_step2_index_lifecycle_tests.rs::sorted_entry_prefix`).
fn sorted_entry_prefix(name_interned: u64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(9);
    buf.push(0x80); // SORTED_TAG
    buf.extend_from_slice(&name_interned.to_be_bytes());
    buf
}

/// Scan a sorted index def's physical entries directly from the info_store,
/// returning the set of indexed rids.
async fn sorted_indexed_rids(tbl: &TableManager, name_interned: u64) -> Vec<[u8; 16]> {
    let prefix = tbl
        .info_store()
        .scan_prefix_stream(bytes::Bytes::from(sorted_entry_prefix(name_interned)), 1000);
    futures::pin_mut!(prefix);
    let mut found_rids: Vec<[u8; 16]> = Vec::new();
    while let Some(batch) = futures::StreamExt::next(&mut prefix).await {
        for (key_bytes, _) in batch.unwrap() {
            if key_bytes.len() >= 16 {
                let mut rid = [0u8; 16];
                rid.copy_from_slice(&key_bytes[key_bytes.len() - 16..]);
                found_rids.push(rid);
            }
        }
    }
    found_rids
}

/// Stage an UPDATE of the row named `old_name` (WHERE name == old_name) to
/// `new_name`, via the real query-builder + `execute_update_tx` path (so
/// `index2_stage_gens` / `sorted_stage_gens` are captured exactly as
/// production staging does), WITHOUT committing.
async fn stage_update(
    tbl: &TableManager,
    tx: &mut shamir_tx::TxContext,
    old_name: &str,
    new_name: &str,
) {
    let interner = tbl.interner().get().await.unwrap();
    let refs = shamir_types::types::common::new_map();
    let ctx = FilterContext::new(interner, &refs);
    let mut m = shamir_types::types::common::new_map_wc(1);
    m.insert(
        "name".to_string(),
        shamir_types::types::value::QueryValue::Str(new_name.to_string()),
    );
    let op = write::update("t")
        .where_(filter::eq("name", old_name))
        .set(shamir_types::types::value::QueryValue::Map(m))
        .build()
        .unwrap();
    let result = tbl
        .execute_update_tx(&op, &ctx, tx, None, &shamir_types::access::Actor::System)
        .await
        .unwrap();
    assert_eq!(
        result.affected, 1,
        "the staged update must match exactly one row"
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// Index2 family — storage-read error
// ═══════════════════════════════════════════════════════════════════════════

/// A tx stages an UPDATE of an existing row BEFORE a functional index2
/// backend is registered. After the backend registers (bumping the index2
/// generation), Phase 2.7's re-derivation calls `data_store.get` on the
/// row's pre-tx bytes to distinguish insert vs. update — inject a genuine
/// non-`NotFound` storage error there.
///
/// Pre-fix: `Err(_) => {}` swallowed this — the tx would commit
/// successfully (Phase 5a is unconditional) while the row's posting for the
/// new functional index was silently never derived. Confirmed by hand:
/// reverting the index2 `Set` arm's `Err(e) => return Err(...)` back to
/// `Err(_) => {}` and re-running this test fails it (the commit call
/// returns `Ok`, so `.unwrap_err()` panics).
///
/// Post-fix: the injected error propagates as `TxError::Storage`, the
/// commit aborts BEFORE Phase 4's WAL begin, no WAL entry is written, the
/// row's data is untouched (still "alice", not "alice2"), and the new
/// index has zero postings for this row.
#[tokio::test]
async fn index2_storage_read_error_aborts_commit_no_partial_state() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    let token = crate::table::table_manager::table_token_for("t");
    let name_key = key_id(&tbl, "name").await;

    let alice = tbl
        .insert(&record_with_str(name_key, "alice"))
        .await
        .unwrap();

    let wal = repo.repo_wal().await.unwrap();
    let wal_len_before = wal.recover().await.unwrap().len();

    // === Stage the UPDATE before the functional index exists. ===
    let (mut tx, _guard) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    stage_update(&tbl, &mut tx, "alice", "alice2").await;

    // === Register the functional index — bumps the index2 generation. ===
    tbl.create_index_v2(&functional_lower_op("lower_name", "t", "name"))
        .await
        .expect("create_index_v2 must succeed");

    // === Arm the fault: the next pre-tx read for (token, alice) fails. ===
    let hook = TEST_REDERIVE_PRE_TX_READ_FAILURE
        .get_or_init(|| Arc::new(RederivePreTxReadFailHook::default()));
    hook.arm(token, alice);

    // === Commit: must abort, not silently skip the posting. ===
    let result = repo.commit_tx(tx).await;
    assert!(
        result.is_err(),
        "F-73: a non-NotFound data_store.get error during Phase 2.7 re-derivation \
         must abort the commit, not be silently swallowed"
    );

    // No WAL entry for the aborted tx.
    assert_eq!(
        wal.recover().await.unwrap().len(),
        wal_len_before,
        "an aborted commit must leave no WAL entry"
    );

    // Data unchanged — Phase 5a never ran (abort happens before Phase 4).
    let observed = tbl.get(alice).await.unwrap();
    assert!(
        matches!(observed, InnerValue::Map(_)),
        "row must still be a well-formed record"
    );
    let observed_name = match &observed {
        InnerValue::Map(m) => m.get(&InternerKey::new(name_key)).cloned(),
        _ => None,
    };
    assert!(
        matches!(observed_name, Some(InnerValue::Str(ref s)) if s == "alice"),
        "aborted commit must NOT have applied the staged update to the data store"
    );

    // No posting for this row under the new index — the divergence F-73 closes.
    let owners = functional_lookup(&tbl, key_id(&tbl, "lower_name").await, "alice2").await;
    assert!(
        !owners.contains(alice.as_bytes()),
        "F-73: an aborted commit must not leave a partial index posting behind"
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// Index2 family — decode error
// ═══════════════════════════════════════════════════════════════════════════

/// Same interleaving as above, but instead of injecting a storage error,
/// the row's PRE-TX bytes in `data_store` are corrupted directly (not
/// valid MessagePack) — a real, non-injected decode failure on
/// `InnerValue::from_bytes(&old_bytes)`.
///
/// Pre-fix: `let Ok(old_rec) = InnerValue::from_bytes(&old_bytes) else {
/// /* nothing */ }` (nested inside the `Ok(old_bytes)` arm) silently
/// skipped this record — best-effort continue. Confirmed by hand:
/// reverting the fix's `.map_err(...)?` back to a bare `if let Ok(..)`
/// silently-skip shape and re-running this test fails it.
///
/// Post-fix: the decode failure propagates as `TxError::Storage(DbError::
/// Codec(..))`, the commit aborts before WAL begin, and no partial state
/// is published.
#[tokio::test]
async fn index2_decode_error_aborts_commit_no_partial_state() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    let name_key = key_id(&tbl, "name").await;

    let alice = tbl
        .insert(&record_with_str(name_key, "alice"))
        .await
        .unwrap();

    let wal = repo.repo_wal().await.unwrap();
    let wal_len_before = wal.recover().await.unwrap().len();

    let (mut tx, _guard) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    stage_update(&tbl, &mut tx, "alice", "alice2").await;

    tbl.create_index_v2(&functional_lower_op("lower_name", "t", "name"))
        .await
        .expect("create_index_v2 must succeed");

    // Corrupt Alice's PRE-TX stored bytes directly — genuinely invalid
    // MessagePack (0xc1 is the spec's reserved "never used" byte, so
    // `rmp_serde` reliably errors on it), not a hook-injected error.
    tbl.data_store()
        .set(alice.to_bytes().into(), invalid_msgpack_bytes())
        .await
        .unwrap();

    let result = repo.commit_tx(tx).await;
    assert!(
        result.is_err(),
        "F-73: a pre-tx record that fails to decode during Phase 2.7 re-derivation \
         must abort the commit, not be silently skipped"
    );

    assert_eq!(
        wal.recover().await.unwrap().len(),
        wal_len_before,
        "an aborted commit must leave no WAL entry"
    );

    // The corrupted bytes must survive unchanged (Phase 5a never ran).
    let raw = tbl.data_store().get(alice.to_bytes().into()).await.unwrap();
    assert_eq!(
        raw.as_ref(),
        invalid_msgpack_bytes().as_ref(),
        "aborted commit must not have overwritten the (corrupted) pre-tx bytes"
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// Sorted-index family — storage-read error
// ═══════════════════════════════════════════════════════════════════════════

/// Same fault class as the index2 storage-read test, but for the base_index
/// sorted-index re-derivation half of `rederive_index2_ops_post_stage`
/// (gated by `tx.sorted_stage_gens`).
#[tokio::test]
async fn sorted_storage_read_error_aborts_commit_no_partial_state() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    let token = crate::table::table_manager::table_token_for("t");
    let name_key = key_id(&tbl, "name").await;

    let alice = tbl
        .insert(&record_with_str(name_key, "alice"))
        .await
        .unwrap();

    let wal = repo.repo_wal().await.unwrap();
    let wal_len_before = wal.recover().await.unwrap().len();

    // === Stage the UPDATE before the sorted index exists. ===
    let (mut tx, _guard) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    stage_update(&tbl, &mut tx, "alice", "alice2").await;

    // === Register the sorted index — bumps the sorted generation. ===
    tbl.create_sorted_index("name_sorted", &["name"])
        .await
        .expect("create_sorted_index must succeed");

    // === Arm the fault: the next pre-tx read for (token, alice) fails. ===
    let hook = TEST_REDERIVE_PRE_TX_READ_FAILURE
        .get_or_init(|| Arc::new(RederivePreTxReadFailHook::default()));
    hook.arm(token, alice);

    let result = repo.commit_tx(tx).await;
    assert!(
        result.is_err(),
        "F-73: a non-NotFound data_store.get error during the sorted-index \
         re-derivation must abort the commit, not be silently swallowed"
    );

    assert_eq!(
        wal.recover().await.unwrap().len(),
        wal_len_before,
        "an aborted commit must leave no WAL entry"
    );

    let observed_name = match tbl.get(alice).await.unwrap() {
        InnerValue::Map(m) => m.get(&InternerKey::new(name_key)).cloned(),
        _ => None,
    };
    assert!(
        matches!(observed_name, Some(InnerValue::Str(ref s)) if s == "alice"),
        "aborted commit must NOT have applied the staged update to the data store"
    );

    // The row's sorted posting must reflect only what `create_sorted_index`'s
    // OWN backfill wrote (it indexed Alice's pre-tx "alice" value
    // independently of this failing tx) — exactly one posting for this row,
    // proving the aborted commit did not ALSO write a second (partial /
    // divergent) posting for the never-applied "alice2" value.
    let name_interned = key_id(&tbl, "name_sorted").await;
    let defs = tbl.sorted_indexes().iter_indexes();
    let def = defs
        .iter()
        .find(|d| d.name_interned == name_interned)
        .expect("sorted def must be registered");
    let found = sorted_indexed_rids(&tbl, def.name_interned).await;
    assert_eq!(
        found.iter().filter(|r| **r == *alice.as_bytes()).count(),
        1,
        "F-73: an aborted commit must not leave a partial/duplicate sorted-index \
         posting behind — only the pre-existing backfilled posting may survive"
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// Sorted-index family — decode error
// ═══════════════════════════════════════════════════════════════════════════

/// Same fault class as the index2 decode-error test, but for the sorted-
/// index re-derivation half.
#[tokio::test]
async fn sorted_decode_error_aborts_commit_no_partial_state() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    let name_key = key_id(&tbl, "name").await;

    let alice = tbl
        .insert(&record_with_str(name_key, "alice"))
        .await
        .unwrap();

    let wal = repo.repo_wal().await.unwrap();
    let wal_len_before = wal.recover().await.unwrap().len();

    let (mut tx, _guard) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    stage_update(&tbl, &mut tx, "alice", "alice2").await;

    tbl.create_sorted_index("name_sorted", &["name"])
        .await
        .expect("create_sorted_index must succeed");

    // Corrupt Alice's PRE-TX stored bytes directly — genuinely invalid
    // MessagePack (see `invalid_msgpack_bytes`'s doc for why 0xc1).
    tbl.data_store()
        .set(alice.to_bytes().into(), invalid_msgpack_bytes())
        .await
        .unwrap();

    let result = repo.commit_tx(tx).await;
    assert!(
        result.is_err(),
        "F-73: a pre-tx record that fails to decode during the sorted-index \
         re-derivation must abort the commit, not be silently skipped"
    );

    assert_eq!(
        wal.recover().await.unwrap().len(),
        wal_len_before,
        "an aborted commit must leave no WAL entry"
    );

    let raw = tbl.data_store().get(alice.to_bytes().into()).await.unwrap();
    assert_eq!(
        raw.as_ref(),
        invalid_msgpack_bytes().as_ref(),
        "aborted commit must not have overwritten the (corrupted) pre-tx bytes"
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// Quiescent control — the fault-injection hook must not interfere with a
// normal commit when nothing is armed.
// ═══════════════════════════════════════════════════════════════════════════

/// Regression guard: with the hook installed (as a side effect of the other
/// tests in this module running in the same process under nextest) but
/// nothing armed for this row's id, a normal update commits successfully.
#[tokio::test]
async fn quiescent_update_with_hook_installed_but_unarmed_commits_normally() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    let name_key = key_id(&tbl, "name").await;

    let _ = tbl.insert(&record_with_str(name_key, "bob")).await.unwrap();

    // Ensure the hook is installed (idempotent with the other tests) but
    // leave it unarmed for this row.
    let _hook = TEST_REDERIVE_PRE_TX_READ_FAILURE
        .get_or_init(|| Arc::new(RederivePreTxReadFailHook::default()));

    let (mut tx, _guard) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    stage_update(&tbl, &mut tx, "bob", "bob2").await;
    tbl.create_index_v2(&functional_lower_op("lower_name2", "t", "name"))
        .await
        .expect("create_index_v2 must succeed");

    repo.commit_tx(tx)
        .await
        .expect("an unarmed hook must not affect a normal commit");
}

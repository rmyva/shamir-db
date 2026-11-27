//! F-74 (#901, P0) — tx-commit Phase 5c must bump the per-index AsOf epoch
//! BEFORE applying sorted-index posting mutations, not after.
//!
//! `commit_phases.rs::apply_index_batch` is the tx-commit path's Phase 5c
//! apply point. Pre-fix it applied the staged sorted-index postings to
//! `info_store` FIRST and only THEN advanced
//! `SortedIndexManager::last_mutation_version` (via `bump_touched_indexes`)
//! for the index(es) touched — the opposite order from the non-tx direct
//! path (`SortedIndexManager::on_record_created`/`on_record_updated`/
//! `on_record_deleted`, which bump BEFORE they apply). Between the apply and
//! the bump, a genuinely concurrent AsOf read on ANOTHER OS thread (tokio is
//! a multi-threaded work-stealing runtime by default in this workspace)
//! could run its own entry-gate check → seek scan → post-scan re-check
//! entirely inside that window: the gate reads the not-yet-bumped epoch,
//! passes; the scan walks postings this commit already mutated; the
//! post-check reads the SAME not-yet-bumped epoch, passes again — a
//! silently short/wrong AsOf page, with no `.await` on the COMMITTING task
//! required to create the window (only genuine OS-thread concurrency is).
//!
//! `apply_index_batch` now bumps first (mirroring the non-tx path). This
//! module proves BOTH directions using the `TEST_LEGACY_APPLY_THEN_BUMP_ORDER`
//! toggle + `TEST_INDEX_BATCH_BUMP_APPLY_HOOK` pause seam
//! (`commit_phases.rs`), combined with the pre-existing F-58 pause seam
//! (`read_asof_seek.rs`'s `TEST_SEEK_LOOP_PRE_ITER_HOOK`, which parks the
//! AsOf reader strictly AFTER its own entry-gate check but BEFORE its scan
//! loop) to construct a fully deterministic 3-step interleave:
//!
//!  1. The reader starts, passes the entry gate (the epoch is still low from
//!     BEFORE the racing commit even begins), and parks at the F-58 seam
//!     (gate passed, scan not yet started).
//!  2. The committer (armed with `TEST_LEGACY_APPLY_THEN_BUMP_ORDER = true`
//!     to reconstruct the OLD order) runs Phase 5c's APPLY first — mutating
//!     the postings the reader is about to scan — then parks at the F-74
//!     seam, strictly BEFORE the bump.
//!  3. The reader is released: it scans the (now-mutated) postings and
//!     performs its post-check, both against an epoch that STILL reads as
//!     unbumped (the committer is parked before its own bump) — reproducing
//!     the exact TOCTOU window and returning a WRONG page (RED). The UPDATE
//!     scenario moves its row's posting FAR outside the reader's scanned
//!     page (not merely to an adjacent in-range position) so the failure is
//!     attributable to the epoch-gate bug alone: an in-range move would
//!     additionally be caught by `read_as_of_keyset_seek`'s own
//!     per-candidate `concurrent_modified` classifier (an independent,
//!     already-correct defence-in-depth check — see that module's doc), which
//!     would mask the specific bug this task closes.
//!  4. The committer is released, completes its bump, and the commit
//!     finishes.
//!
//! With the toggle OFF (the shipped fix, bump-then-apply) the identical
//! 3-step choreography is safe: by the time the committer reaches the F-74
//! seam the bump has ALREADY happened (it now runs first), so a reader that
//! is released at that point observes the raised epoch — either at its own
//! entry gate (if it re-checks) or, since the entry gate already passed
//! before the seam, at the F-58 post-scan re-check
//! (`read_asof_seek.rs::read_as_of_keyset_seek`'s own
//! `last_mutation_version(index_name) > pinned_version` check after the
//! walk) — and falls back to the full scan, which returns the correct
//! pinned-snapshot page (GREEN).
//!
//! Two scenarios, per the task brief: an UPDATE to the indexed field, and a
//! DELETE of an indexed row — both are cases a current-state index cannot
//! correctly serve without the gate.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use shamir_query_types::read::select::Select;
use shamir_query_types::read::{At, OrderBy, Pagination, ReadQuery, Temporal};
use shamir_storage::storage_in_memory::InMemoryRepo;
use shamir_tx::{IsolationLevel, Retention};
use shamir_types::core::interner::{InternerKey, TouchInd};
use shamir_types::types::common::new_map_wc;
use shamir_types::types::value::{InnerValue, QueryValue};

use crate::query::filter::eval_context::FilterContext;
use crate::query::read::QueryResult;
use crate::repo::repo_instance::RepoInstance;
use crate::repo::repo_types::BoxRepo;
use crate::table::TableConfig;
use crate::table::TableManager;
use crate::table::{SeekLoopPreIterHook, TEST_SEEK_LOOP_PRE_ITER_HOOK};
use crate::tx::commit_phases::{
    IndexBatchBumpApplyHook, TEST_INDEX_BATCH_BUMP_APPLY_HOOK, TEST_LEGACY_APPLY_THEN_BUMP_ORDER,
};

/// Serialises the arm → commit → reset window of every test in this module.
/// `TEST_LEGACY_APPLY_THEN_BUMP_ORDER`, `TEST_INDEX_BATCH_BUMP_APPLY_HOOK`
/// AND `TEST_SEEK_LOOP_PRE_ITER_HOOK` are all process-wide (`AtomicBool` /
/// `OnceLock`), so two tests running on parallel nextest threads would
/// otherwise clobber each other's toggle/hook.
static ORDERING_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

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

fn record_with_score(score_key: u64, label_key: u64, score: i64, label: &str) -> InnerValue {
    let mut m = new_map_wc(2);
    m.insert(InternerKey::new(score_key), InnerValue::Int(score));
    m.insert(InternerKey::new(label_key), InnerValue::Str(label.into()));
    InnerValue::Map(m)
}

/// Build a table with an MVCC store (kept-history retention) and a single
/// sorted index on `score`. Mirrors `f53b_asof_seek_tests.rs::
/// make_mvcc_score_table`, but via the repo (`add_table`/`get_table`) so the
/// real `repo.commit_tx` pipeline (Phase 5c / `apply_index_batch`) is
/// exercised, not the non-tx direct path.
async fn make_repo_with_score_table() -> (RepoInstance, TableManager) {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    tbl.create_sorted_index("score_idx", &["score"])
        .await
        .unwrap();

    // Keep full history so a pin taken early in the test still resolves
    // after later commits (mirrors every existing AsOf seek test).
    if let Some(mvcc) = tbl.mvcc_store_ref() {
        mvcc.set_retention(Retention::keep_history()).unwrap();
    }
    (repo, tbl)
}

/// Build an ASC seek AsOf query pinned at `pinned` — the shape that
/// dispatches to `read_as_of_keyset_seek` (mirrors
/// `f53b_asof_seek_tests.rs::seek_query_asc`).
fn seek_query_asc(limit: u64, pinned: u64) -> ReadQuery {
    let mut q = ReadQuery::new("t")
        .select(Select::fields(["score"]))
        .order_by(OrderBy::asc("score"))
        .pagination(Pagination::after_with_id(
            vec![QueryValue::Int(i64::MIN)],
            Some(limit),
            None,
        ));
    q.temporal = Temporal::AsOf {
        at: At::Version(pinned),
    };
    q
}

fn scores_of(result: &QueryResult) -> Vec<i64> {
    result
        .records
        .iter()
        .filter_map(|r| r.get_value_i64("score"))
        .collect()
}

fn used_seek_path(result: &QueryResult) -> bool {
    result
        .stats
        .as_ref()
        .and_then(|s| s.index_used.as_deref())
        .is_some_and(|l| l.ends_with("_asof_keyset"))
}

fn path_label(result: &QueryResult) -> &'static str {
    if used_seek_path(result) {
        "the seek fast path"
    } else {
        "the full-scan fallback"
    }
}

/// Install the F-74 bump/apply pause seam (`commit_phases.rs`). One-shot per
/// test process via `armed` — see `IndexBatchBumpApplyHook`'s doc.
fn install_bump_apply_hook() -> Arc<IndexBatchBumpApplyHook> {
    let hook = Arc::new(IndexBatchBumpApplyHook {
        reached: AtomicUsize::new(0),
        resume: tokio::sync::Notify::new(),
        armed: AtomicBool::new(true),
    });
    TEST_INDEX_BATCH_BUMP_APPLY_HOOK
        .set(Arc::clone(&hook))
        .expect("hook installed once per test process");
    hook
}

/// Install the F-58 seek-loop pause seam (`read_asof_seek.rs`) — parks the
/// AsOf reader strictly AFTER its entry-gate check but BEFORE its scan loop.
fn install_seek_hook() -> Arc<SeekLoopPreIterHook> {
    let hook = Arc::new(SeekLoopPreIterHook {
        reached: AtomicUsize::new(0),
        resume: tokio::sync::Notify::new(),
        armed: AtomicBool::new(true),
    });
    TEST_SEEK_LOOP_PRE_ITER_HOOK
        .set(Arc::clone(&hook))
        .expect("hook installed once per test process");
    hook
}

/// Seed 5 rows (scores 10..50), each via its own committed tx, and return
/// the assigned record ids plus the pinned version (= `last_committed()`
/// right after the seed commits, BEFORE the racing commit this test drives).
async fn seed_five_rows(
    repo: &RepoInstance,
    tbl: &TableManager,
    score_key: u64,
    label_key: u64,
) -> (Vec<shamir_types::types::record_id::RecordId>, u64) {
    let mut ids = Vec::new();
    for s in [10i64, 20, 30, 40, 50] {
        let (mut tx, _g) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
        let rid = tbl
            .insert_tx(
                &record_with_score(score_key, label_key, s, &format!("r{s}")),
                Some(&mut tx),
            )
            .await
            .unwrap();
        repo.commit_tx(tx).await.expect("initial insert commits");
        ids.push(rid);
    }
    let gate = repo.tx_gate().await.unwrap();
    let pinned = gate.last_committed();
    (ids, pinned)
}

/// Drives the deterministic 3-step interleave described in the module doc,
/// racing a concurrent AsOf read (pinned BEFORE `commit_fut`'s tx) against
/// the committing tx's Phase 5c bump/apply seam. `commit_fut` stages +
/// commits the racing tx (UPDATE or DELETE, built by the caller) and must
/// resolve to the commit's `Result`.
///
/// `page_limit` is load-bearing for the UPDATE scenario: `lookup_range_
/// first_k_page`'s resume loop keeps walking (extending `after_key`) until
/// it collects exactly `limit` candidates or the index is exhausted. If the
/// limit requires MORE candidates than remain at their PRE-mutation
/// positions, the walk keeps going and would eventually reach the mutated
/// row's NEW posting too — which `concurrent_modified`'s per-candidate
/// classifier (an independent, already-correct defence-in-depth mechanism)
/// WOULD then catch on its own, masking the specific epoch-ordering bug this
/// module targets. Passing a `page_limit` equal to the number of rows that
/// remain in place (excluding the one being moved) keeps the walk from ever
/// reaching the moved posting.
async fn race_asof_read_against_commit<F>(
    tbl: &TableManager,
    pinned: u64,
    page_limit: u64,
    commit_fut: F,
) -> QueryResult
where
    F: std::future::Future<Output = Result<crate::tx::TxOutcome, crate::tx::CommitError>>
        + Send
        + 'static,
{
    let bump_apply_hook = install_bump_apply_hook();
    let seek_hook = install_seek_hook();

    // Step 1: start the reader. It will pass the entry gate (epoch still
    // low — the racing commit hasn't touched anything yet) and park at the
    // F-58 seam, strictly before its scan loop.
    let tbl_r = tbl.clone();
    let read_handle = tokio::spawn(async move {
        let q = seek_query_asc(page_limit, pinned);
        let interner = tbl_r.interner().get().await.unwrap();
        let refs = shamir_types::types::common::new_map();
        let ctx = FilterContext::new(interner, &refs);
        tbl_r.read(&q, &ctx).await.unwrap()
    });
    while seek_hook.reached.load(Ordering::SeqCst) == 0 {
        tokio::task::yield_now().await;
    }

    // Step 2: start the committer. Under the legacy toggle it applies the
    // posting mutation FIRST, then parks at the F-74 seam strictly before
    // its bump; under the fixed order it bumps first, then parks strictly
    // before its apply.
    let commit_handle = tokio::spawn(commit_fut);
    while bump_apply_hook.reached.load(Ordering::SeqCst) == 0 {
        tokio::task::yield_now().await;
    }

    // Step 3: release the reader. It scans + post-checks while the
    // committer sits parked at its own seam.
    seek_hook.resume.notify_one();
    let result = read_handle.await.unwrap();

    // Step 4: release the committer and let the commit finish.
    bump_apply_hook.resume.notify_one();
    commit_handle
        .await
        .unwrap()
        .expect("racing commit must succeed");

    result
}

// ═══════════════════════════════════════════════════════════════════════════
// Scenario 1: UPDATE to the indexed field.
// ═══════════════════════════════════════════════════════════════════════════

/// Drives one full red-then-green cycle for the UPDATE scenario: with
/// `legacy_apply_then_bump = true` the OLD (buggy) apply-then-bump order is
/// reconstructed and the concurrent AsOf read must observe the WRONG page
/// (RED); with `false` (the shipped fix) the identical interleaving is safe
/// (GREEN).
async fn run_update_scenario(legacy_apply_then_bump: bool) -> QueryResult {
    let _guard = ORDERING_TEST_LOCK.lock().await;
    TEST_LEGACY_APPLY_THEN_BUMP_ORDER.store(legacy_apply_then_bump, Ordering::SeqCst);

    let (repo, tbl) = make_repo_with_score_table().await;
    let score_key = key_id(&tbl, "score").await;
    let label_key = key_id(&tbl, "label").await;
    let (ids, pinned) = seed_five_rows(&repo, &tbl, score_key, label_key).await;

    let idx_name = tbl.sorted_indexes().iter_indexes()[0].name_interned;
    assert!(
        tbl.sorted_indexes().last_mutation_version(idx_name) <= pinned,
        "pre-condition: gate must pass before the concurrent commit"
    );

    // The committing tx: UPDATE the score-30 row's indexed field to 1000 —
    // moves its posting FAR outside the 5-row page the reader scans (see
    // `race_asof_read_against_commit`'s doc on why the new position must
    // fall outside the observed range: an in-range move would be caught by
    // the classifier's `concurrent_modified` check independently of the
    // epoch gate this test targets).
    let row30 = ids[2];
    let (mut tx, _g) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    tbl.update_tx(
        row30,
        &record_with_score(score_key, label_key, 1000, "r30->1000"),
        Some(&mut tx),
    )
    .await
    .unwrap();
    let repo_committer = repo.clone();

    // page_limit = 4: exactly the 4 rows that stay in place (10, 20, 40, 50).
    // Row 30 moves to 1000, well outside this page — see
    // `race_asof_read_against_commit`'s doc for why the walk must not be
    // forced to search far enough to reach it.
    let result = race_asof_read_against_commit(&tbl, pinned, 4, async move {
        repo_committer.commit_tx(tx).await
    })
    .await;

    TEST_LEGACY_APPLY_THEN_BUMP_ORDER.store(false, Ordering::SeqCst);
    result
}

/// RED: with the OLD apply-then-bump order reconstructed, a concurrent AsOf
/// read (page limit 4) pinned before the commit observes the UPDATE's
/// posting move (score 30 -> 1000, out of the scanned page) while the epoch
/// still reads as unbumped, takes the fast path, and silently returns a page
/// MISSING row 30 (`[10, 20, 40, 50]`) instead of the correct pinned
/// snapshot's first 4 rows (`[10, 20, 30, 40]`). The row's new position
/// (1000) falls outside the page the walk needs to satisfy `limit=4`, so the
/// classifier's `concurrent_modified` per-candidate check never observes it
/// — this isolates the epoch-gate bug from that independent,
/// already-correct defence-in-depth mechanism.
#[tokio::test]
async fn update_scenario_is_red_under_legacy_apply_then_bump_order() {
    let result = run_update_scenario(true).await;
    let scores = scores_of(&result);
    assert!(
        used_seek_path(&result),
        "F-74 RED proof precondition: the reader must have taken the seek fast \
         path (entry gate passed before the race) for this to demonstrate the \
         TOCTOU window, not a pre-existing full-scan fallback; got {scores:?} \
         via {}",
        path_label(&result)
    );
    assert_ne!(
        scores,
        vec![10, 20, 30, 40],
        "F-74 RED proof: under the OLD apply-then-bump order the concurrent \
         AsOf read raced the TOCTOU window and returned a WRONG page \
         (expected a mismatch against the correct pinned snapshot); got {scores:?} \
         via {}",
        path_label(&result)
    );
}

/// GREEN: with the shipped bump-then-apply fix, the identical interleaving
/// is safe. By the time the committer reaches the F-74 seam the bump has
/// ALREADY happened, so the reader's post-scan re-check
/// (`read_as_of_keyset_seek`'s own gate re-check) observes the raised epoch
/// and falls back to the full scan, which returns the correct pinned
/// snapshot's first 4 rows.
#[tokio::test]
async fn update_scenario_is_green_under_fixed_bump_then_apply_order() {
    let result = run_update_scenario(false).await;
    let scores = scores_of(&result);
    assert_eq!(
        scores,
        vec![10, 20, 30, 40],
        "F-74 GREEN proof: under the FIXED bump-then-apply order the \
         concurrent AsOf read must return the correct pinned snapshot \
         (row 30 at its OLD score, not the post-pin 1000) regardless of which \
         path served it; got {scores:?} via {}",
        path_label(&result)
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// Scenario 2: DELETE of an indexed row.
// ═══════════════════════════════════════════════════════════════════════════

async fn run_delete_scenario(legacy_apply_then_bump: bool) -> QueryResult {
    let _guard = ORDERING_TEST_LOCK.lock().await;
    TEST_LEGACY_APPLY_THEN_BUMP_ORDER.store(legacy_apply_then_bump, Ordering::SeqCst);

    let (repo, tbl) = make_repo_with_score_table().await;
    let score_key = key_id(&tbl, "score").await;
    let label_key = key_id(&tbl, "label").await;
    let (ids, pinned) = seed_five_rows(&repo, &tbl, score_key, label_key).await;

    let idx_name = tbl.sorted_indexes().iter_indexes()[0].name_interned;
    assert!(
        tbl.sorted_indexes().last_mutation_version(idx_name) <= pinned,
        "pre-condition: gate must pass before the concurrent commit"
    );

    // The committing tx: DELETE the score-30 row — removes its posting
    // entirely.
    let row30 = ids[2];
    let (mut tx, _g) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    tbl.delete_tx(row30, Some(&mut tx)).await.unwrap();
    let repo_committer = repo.clone();

    // page_limit = 5: a DELETE has no "new position" to accidentally
    // discover further down the index (unlike the UPDATE scenario), so the
    // walk simply exhausts the index after the 4 remaining rows regardless
    // of the requested limit.
    let result = race_asof_read_against_commit(&tbl, pinned, 5, async move {
        repo_committer.commit_tx(tx).await
    })
    .await;

    TEST_LEGACY_APPLY_THEN_BUMP_ORDER.store(false, Ordering::SeqCst);
    result
}

/// RED: with the OLD apply-then-bump order, a concurrent AsOf read pinned
/// before the DELETE observes the vanished posting (score 30 removed from
/// the index) while the epoch still reads as unbumped, takes the fast path,
/// and silently returns a page MISSING the deleted-after-pin row instead of
/// the correct pinned snapshot `[10, 20, 30, 40, 50]`.
#[tokio::test]
async fn delete_scenario_is_red_under_legacy_apply_then_bump_order() {
    let result = run_delete_scenario(true).await;
    let scores = scores_of(&result);
    assert!(
        used_seek_path(&result),
        "F-74 RED proof precondition (DELETE): the reader must have taken \
         the seek fast path for this to demonstrate the TOCTOU window; got \
         {scores:?} via {}",
        path_label(&result)
    );
    assert_ne!(
        scores,
        vec![10, 20, 30, 40, 50],
        "F-74 RED proof (DELETE): under the OLD apply-then-bump order the \
         concurrent AsOf read raced the TOCTOU window and returned a WRONG \
         page (expected a mismatch against the correct pinned snapshot); \
         got {scores:?} via {}",
        path_label(&result)
    );
}

/// GREEN: with the shipped bump-then-apply fix, the same DELETE race is
/// safe — the returned page always includes the deleted-after-pin row at
/// its pinned value.
#[tokio::test]
async fn delete_scenario_is_green_under_fixed_bump_then_apply_order() {
    let result = run_delete_scenario(false).await;
    let scores = scores_of(&result);
    assert_eq!(
        scores,
        vec![10, 20, 30, 40, 50],
        "F-74 GREEN proof (DELETE): under the FIXED bump-then-apply order the \
         concurrent AsOf read must return the correct pinned snapshot \
         (including the deleted-after-pin row at its pinned value) \
         regardless of which path served it; got {scores:?} via {}",
        path_label(&result)
    );
}

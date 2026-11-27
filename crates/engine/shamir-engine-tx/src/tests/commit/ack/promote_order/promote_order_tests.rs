//! P0 stage L3b — p0-index-promote-order regression tests.
//!
//! Vector promotes of DIFFERENT transactions touching the same rid are
//! ordered by the per-backend promote turnstile (`promote_turn` +
//! `turn_finished` + `turn_notify`, tickets created in the Phase 5d DELTA
//! half, awaited before `enter_write`, released before the next wait or on
//! `Drop`). Without it, transaction B (published later, chunk idx+1) can
//! promote BEFORE transaction A (chunk idx, parked between publish and
//! promote) and A's stale mutation overwrites B's.
//!
//! Shapes exercised here:
//! - SEEDED-ROW UPDATES: a row is committed FIRST (own tx, hooks unarmed);
//!   transactions A and B are two `update_tx` calls on the SAME rid. Rids
//!   are allocator-assigned (`insert_tx` allocates `RecordId::new()`), so
//!   same-rid ordering can only be exercised on an existing row.
//! - RE-INSERT VIA UPDATE-OF-DELETED: B "re-inserts" a deleted rid with
//!   `update_tx` — the update path reads NotFound and stages an INSERT at
//!   the SAME allocator-assigned rid (`tx_update.rs`: `old = None` →
//!   `plan_insert_ops(id, ...)`). A fresh `insert_tx` would allocate a NEW
//!   rid and prove nothing about same-rid ordering.
//! - TWO-TABLE GLOBAL ORDER: because `create_index_v2` admits at most ONE
//!   vector index per table (Validation), a single
//!   `apply_vector_graph_batch` call can never own two vector tickets for
//!   one table; the cross-table scenario uses two tables and pins the
//!   merged ascending-token reservation order.
//!
//! Oracle: `distance_of(backend, q, rid)` reads the cosine DISTANCE (0 = identical) of ONE
//! specific rid — tie-immune, unlike a top-hit `match_score` (which the
//! graph may legitimately answer with EITHER the seed vector or B's own
//! vector at distance 0 when both vectors are live).
//!
//! Determinism: transaction A is parked at the pre-broadcast test hook
//! (fires AFTER publish, BEFORE `promote_vectors`); transaction B's promote
//! must WAIT at the turnstile — observed via the backend's one-shot
//! `arm_promote_turn_wait_hook_for_test` — before A is resumed. The hooks
//! are armed only AFTER the seed transaction has committed (arming the
//! journal hook earlier would park the seed forever). No sleeps; every DIRECT
//! await on the test's own task is bounded by `within`; awaits inside the
//! spawned tasks are bounded by the outer `within(join)`.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use shamir_query_types::admin::CreateIndexOp;
use shamir_storage::storage_in_memory::InMemoryRepo;
use shamir_storage::types::Store;
use shamir_tx::IsolationLevel;
use shamir_types::core::interner::{InternerKey, TouchInd};
use shamir_types::types::common::new_map_wc;
use shamir_types::types::record_id::RecordId;
use shamir_types::types::value::InnerValue;
use tokio::sync::oneshot;

use crate::index2::backend::{IndexBackend, IndexQuery, IndexResult};
use crate::repo::repo_instance::RepoInstance;
use crate::repo::repo_types::BoxRepo;
use crate::table::TableConfig;
use crate::tx::commit_phases::{
    FAIL_VECTOR_PROMOTE_ONCE_REMAINING, FAIL_VECTOR_PROMOTE_ONCE_TX_ID, FAIL_VECTOR_PROMOTE_TX_ID,
};
use crate::tx::test_seams::JournalPublishHook;
use crate::tx::tx_outcome::MaterializationState;
use shamir_index::vector::vector_backend::VectorBackend;

/// Deadlines are deadlock guards, never scheduling or performance oracles.
async fn within<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(45), future)
        .await
        .expect("promote-order test stalled — the turnstile leaked a turn")
}

fn make_repo() -> RepoInstance {
    let repo = Arc::new(InMemoryRepo::new());
    RepoInstance::new("promote_order".into(), BoxRepo::InMemory(repo), Vec::new())
}

async fn field_id(tbl: &crate::table::TableManager, name: &str) -> u64 {
    let interner = tbl.interner().get().await.unwrap();
    match interner.touch_ind(name).unwrap() {
        TouchInd::Exists(k) | TouchInd::New(k) => k.id(),
    }
}

fn vector_index_op(table: &str, name: &str) -> CreateIndexOp {
    CreateIndexOp {
        create_index: name.into(),
        table: table.into(),
        fields: vec![vec!["embedding".into()]],
        unique: false,
        sorted: false,
        repo: "main".into(),
        index_type: Some("vector".into()),
        fts_tokenizer: None,
        fts_language: None,
        functional_op: None,
        functional_args: None,
        vector_dim: Some(3),
        vector_metric: Some("cosine".into()),
        vector_quantization: None,
        include: Vec::new(),
        if_not_exists: false,
    }
}

fn vec_record(emb_key_id: u64, vec: &[f64]) -> InnerValue {
    let mut m = new_map_wc(1);
    m.insert(
        InternerKey::new(emb_key_id),
        InnerValue::List(vec.iter().map(|f| InnerValue::F64(*f)).collect()),
    );
    InnerValue::Map(m)
}

/// The table's `VectorBackend`, with the registry Vec kept alive by the caller.
fn as_vector(backends: &[Arc<dyn IndexBackend>]) -> &VectorBackend {
    backends
        .iter()
        .find_map(|b| b.as_any().downcast_ref::<VectorBackend>())
        .expect("the table must own a VectorBackend")
}

/// COSINE DISTANCE (0 = identical, 1 = orthogonal; NOT similarity) of ONE
/// specific rid under query `q`; panics if
/// the rid is absent — absence is asserted separately via `hits_contain`.
/// Unlike a top-hit score, this is PER-RID and therefore tie-immune: when
/// the graph legitimately holds BOTH the seed vector and B's newer vector
/// (e.g. A's update failed or was replaced), both queries answer 1.0 at the
/// top hit and a `s_new > s_stale` comparison is vacuously false. This
/// oracle fails precisely when the stale promote won.
async fn distance_of(backend: &VectorBackend, q: &[f64], rid: &RecordId) -> f32 {
    let q: Vec<f32> = q.iter().map(|f| *f as f32).collect();
    match IndexBackend::lookup(
        backend,
        IndexQuery::Vector {
            vec: q,
            k: 10,
            opts: crate::index2::vector::SearchOpts::default(),
        },
    )
    .await
    .unwrap()
    {
        IndexResult::Ranked(hits) => hits
            .iter()
            .find(|(r, _)| r == rid)
            .map(|(_, s)| *s)
            .unwrap_or_else(|| panic!("rid {rid:?} absent from the live graph")),
        _ => panic!("expected Ranked result"),
    }
}

async fn hits_contain(backend: &VectorBackend, q: &[f64], rid: &RecordId) -> bool {
    let q: Vec<f32> = q.iter().map(|f| *f as f32).collect();
    match IndexBackend::lookup(
        backend,
        IndexQuery::Vector {
            vec: q,
            k: 10,
            opts: crate::index2::vector::SearchOpts::default(),
        },
    )
    .await
    .unwrap()
    {
        IndexResult::Ranked(hits) => hits.iter().any(|(r, _)| r == rid),
        _ => panic!("expected Ranked result"),
    }
}

fn park_hook() -> Arc<JournalPublishHook> {
    let hook = Arc::new(JournalPublishHook::default());
    hook.broadcast_armed.store(true, Ordering::Release);
    hook
}

/// Reopen a fresh backend over the same stores (the restart oracle).
async fn restarted_backend(
    backend: &VectorBackend,
    emb_id: u64,
    info_store: &Arc<dyn Store>,
    data_store: &Arc<dyn Store>,
) -> VectorBackend {
    let desc = IndexBackend::descriptor(backend).clone();
    let adapter: Arc<dyn shamir_index::vector::adapter::VectorAdapter> =
        Arc::new(shamir_index::vector::hnsw_adapter::HnswAdapter::new(
            3,
            shamir_index::kind::VectorMetric::Cosine,
            shamir_index::vector::hnsw_adapter::HnswConfig {
                max_elements: 100_000,
                m: 16,
                ef_construction: 200,
                ef_search: 50,
                ..Default::default()
            },
        ));
    let restarted = VectorBackend::new(desc, vec![emb_id], adapter, None);
    IndexBackend::restore_on_open(&restarted, Arc::clone(info_store), Arc::clone(data_store))
        .await
        .unwrap();
    assert_eq!(
        IndexBackend::rebuild_count(&restarted),
        0,
        "restart must take the snapshot+delta path"
    );
    restarted
}

async fn join_tail(outcome: &mut crate::tx::tx_outcome::TxOutcome) {
    if let Some(tail) = outcome.take_background() {
        within(tail.join()).await;
    }
}

/// (a) main scenario (the proven (b) shape): a row is committed FIRST
/// (seed, chunk 0, hooks unarmed); then A (mid vector, chunk 1) is parked
/// between publish and promote; B (newer vector, chunk 2, same rid) promotes
/// — and MUST wait at the turnstile; A resumes LAST yet must NOT overwrite
/// B. The snapshot + restart oracle still holds B's vector.
///
/// Fails on old code because: reverting the turnstile in
/// `apply_vector_graph_batch` (wait_turn/promote via tickets) back to the
/// unordered `apply_staged_vector_deletes` + `apply_staged_vectors` loop lets
/// B promote without ever waiting — the turn-wait one-shot never fires,
/// `within(turn_rx)` times out, and A's stale promote lands last
/// (`distance_of(rid, q_new) < 0.1`).
///
/// Awaits inside the spawned tasks are bounded by the outer `within(join)`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rv_index_l3b_promote_order_stale_promote_loses_to_newer_vector() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("vecs"));
    let tbl = within(repo.get_table("vecs")).await.unwrap();
    within(tbl.create_index_v2(&vector_index_op("vecs", "vec_idx")))
        .await
        .unwrap();
    let emb = within(field_id(&tbl, "embedding")).await;
    let backends = within(tbl.index2_registry().all_backends()).await;
    let backend = as_vector(&backends);

    // Seed BEFORE arming the journal hook (RV finding 6): an armed hook parks
    // EVERY commit at the pre-broadcast point, so the seed would hang forever.
    // A committed row gives A and B the SAME allocator-assigned rid to fight
    // over — `insert_tx` rids are allocator-assigned, never content-derived.
    let (mut tx0, pin0) = within(repo.begin_tx(IsolationLevel::Snapshot))
        .await
        .unwrap();
    let rid = within(tbl.insert_tx(&vec_record(emb, &[1.0, 0.0, 0.0]), Some(&mut tx0)))
        .await
        .unwrap();
    let mut outcome0 = within(repo.commit_tx(tx0)).await.unwrap();
    join_tail(&mut outcome0).await;
    drop(pin0);
    assert_eq!(backend.applied_prefix(), 1, "chunk 0 must be acknowledged");

    // NOW arm: A parks pre-promote, B waits at the turnstile behind A.
    let park = park_hook();
    repo.core.journal_publish_hook.store(Some(park.clone()));
    let (turn_tx, turn_rx) = oneshot::channel();
    backend.arm_promote_turn_wait_hook_for_test(turn_tx);

    // Transaction A: mid vector; parked pre-promote (chunk 1 reserved).
    let (mut tx_a, pin_a) = within(repo.begin_tx(IsolationLevel::Snapshot))
        .await
        .unwrap();
    within(tbl.update_tx(rid, &vec_record(emb, &[0.0, 1.0, 0.0]), Some(&mut tx_a)))
        .await
        .unwrap();
    let repo_a = repo.clone();
    let task_a = tokio::spawn(async move { repo_a.commit_tx(tx_a).await });
    within(park.reached.notified()).await;

    // Transaction B: newer vector, same rid. Its promote must WAIT.
    let (mut tx_b, pin_b) = within(repo.begin_tx(IsolationLevel::Snapshot))
        .await
        .unwrap();
    within(tbl.update_tx(rid, &vec_record(emb, &[0.0, 0.0, 1.0]), Some(&mut tx_b)))
        .await
        .unwrap();
    let repo_b = repo.clone();
    let task_b = tokio::spawn(async move { repo_b.commit_tx(tx_b).await });
    let _ = within(turn_rx).await;

    // Resume A — its mid promote runs, but B's chunk still promotes AFTER.
    park.resume.notify_one();
    let mut outcome_a = within(task_a).await.unwrap().unwrap();
    join_tail(&mut outcome_a).await;
    let mut outcome_b = within(task_b).await.unwrap().unwrap();
    join_tail(&mut outcome_b).await;

    // Per-rid oracle (tie-immune): the row's LIVE vector IS B's newer
    // embedding; A's mid embedding is NOT live.
    let s_new = within(distance_of(backend, &[0.0, 0.0, 1.0], &rid)).await;
    let s_mid = within(distance_of(backend, &[0.0, 1.0, 0.0], &rid)).await;
    assert!(
        s_new < 0.1,
        "the live graph must hold B's newer vector for this rid \
         (distance_of(q_new) = {s_new} — A's stale promote landed after B's)"
    );
    assert!(
        s_mid > 0.9,
        "A's mid vector must NOT be live for this rid (distance_of(q_mid) = {s_mid})"
    );

    // Seed chunk 0 + A's chunk 1 + B's chunk 2 ⇒ contiguous prefix 3.
    assert_eq!(
        backend.applied_prefix(),
        3,
        "A's and B's chunks must each be acked exactly once"
    );

    // Snapshot + restart: the persisted graph is still B's vector.
    let info_store = tbl.info_store_for_test();
    within(backend.snapshot_now(&info_store)).await.unwrap();
    let reloaded = within(restarted_backend(
        backend,
        emb,
        &info_store,
        tbl.data_store_for_test(),
    ))
    .await;
    let s_new = within(distance_of(&reloaded, &[0.0, 0.0, 1.0], &rid)).await;
    let s_mid = within(distance_of(&reloaded, &[0.0, 1.0, 0.0], &rid)).await;
    assert!(
        s_new < 0.1 && s_mid > 0.9,
        "the restarted graph must still hold B's newer vector for this rid \
         (s_new {s_new}, s_mid {s_mid})"
    );
    drop((pin_a, pin_b));
}

/// (b) upsert-then-delete: A (insert, chunk idx) parked; B deletes the rid
/// (chunk idx+1) and must wait at the turnstile; after A's insert and B's
/// delete both promote IN ORDER the rid is ABSENT from the graph.
///
/// Fails on old code because: reverting the ordered ticket path lets B's
/// delete promote immediately (A's chunk idx is parked mid-promote), then A's
/// resume inserts the rid afterwards — `hits_contain` finds the deleted rid
/// live in the graph and the assertion fails.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rv_index_l3b_promote_order_upsert_then_delete_stays_absent() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("vecs"));
    let tbl = within(repo.get_table("vecs")).await.unwrap();
    within(tbl.create_index_v2(&vector_index_op("vecs", "vec_idx")))
        .await
        .unwrap();
    let emb = within(field_id(&tbl, "embedding")).await;
    let backends = within(tbl.index2_registry().all_backends()).await;
    let backend = as_vector(&backends);

    // Seed BEFORE arming the journal hook (RV finding 6): an armed hook parks
    // EVERY commit at the pre-broadcast point, so the seed would hang forever.
    let (mut tx0, pin0) = within(repo.begin_tx(IsolationLevel::Snapshot))
        .await
        .unwrap();
    let rid = within(tbl.insert_tx(&vec_record(emb, &[1.0, 0.0, 0.0]), Some(&mut tx0)))
        .await
        .unwrap();
    let mut outcome0 = within(repo.commit_tx(tx0)).await.unwrap();
    join_tail(&mut outcome0).await;
    drop(pin0);

    // NOW arm: A parks pre-promote, B waits at the turnstile behind A.
    let park = park_hook();
    repo.core.journal_publish_hook.store(Some(park.clone()));
    let (turn_tx, turn_rx) = oneshot::channel();
    backend.arm_promote_turn_wait_hook_for_test(turn_tx);

    // A: upsert (stale mutation), parked pre-promote.
    let (mut tx_a, pin_a) = within(repo.begin_tx(IsolationLevel::Snapshot))
        .await
        .unwrap();
    within(tbl.update_tx(rid, &vec_record(emb, &[0.0, 1.0, 0.0]), Some(&mut tx_a)))
        .await
        .unwrap();
    let repo_a = repo.clone();
    let task_a = tokio::spawn(async move { repo_a.commit_tx(tx_a).await });
    within(park.reached.notified()).await;

    // B: delete the rid; its promote must wait behind A's chunk.
    let (mut tx_b, pin_b) = within(repo.begin_tx(IsolationLevel::Snapshot))
        .await
        .unwrap();
    within(tbl.delete_tx(rid, Some(&mut tx_b))).await.unwrap();
    let repo_b = repo.clone();
    let task_b = tokio::spawn(async move { repo_b.commit_tx(tx_b).await });
    let _ = within(turn_rx).await;

    park.resume.notify_one();
    let mut outcome_a = within(task_a).await.unwrap().unwrap();
    join_tail(&mut outcome_a).await;
    let mut outcome_b = within(task_b).await.unwrap().unwrap();
    join_tail(&mut outcome_b).await;

    // The rid is ABSENT from the live graph despite A's later-promoting insert.
    assert!(
        !within(hits_contain(backend, &[0.0, 1.0, 0.0], &rid)).await,
        "deleted rid must be absent from the live graph"
    );
    assert!(
        !within(hits_contain(backend, &[1.0, 0.0, 0.0], &rid)).await,
        "deleted rid must be absent from the live graph"
    );
    drop((pin_a, pin_b));
}

/// (c) delete-then-reinsert: a row is committed FIRST (seed, chunk 0); A
/// (delete of the rid, chunk 1) is parked; B re-inserts the SAME rid via
/// `update_tx` (chunk 2) and must wait; after both promote IN ORDER the rid
/// is present with the (same) vector.
///
/// The re-insert is an UPDATE-OF-DELETED-ROW, not a fresh insert:
/// `update_tx` reads NotFound (A's data delete commits at publish) and
/// stages an INSERT at the SAME allocator-assigned rid (`tx_update.rs`:
/// `old = None` → `plan_insert_ops(id, ...)`). A fresh `insert_tx` would
/// allocate a NEW rid (`RecordId::new()`) and prove nothing about same-rid
/// ordering.
///
/// Fails on old code because: on the unordered path B's insert promotes
/// first and A's parked delete promotes after it, tombstoning the freshly
/// re-inserted rid — `hits_contain` finds nothing and the test fails.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rv_index_l3b_promote_order_delete_then_reinsert_present_newest() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("vecs"));
    let tbl = within(repo.get_table("vecs")).await.unwrap();
    within(tbl.create_index_v2(&vector_index_op("vecs", "vec_idx")))
        .await
        .unwrap();
    let emb = within(field_id(&tbl, "embedding")).await;
    let backends = within(tbl.index2_registry().all_backends()).await;
    let backend = as_vector(&backends);

    // Seed a committed row BEFORE arming the journal hook (RV finding 6).
    // The re-insert below targets this allocator-assigned rid.
    let (mut tx0, pin0) = within(repo.begin_tx(IsolationLevel::Snapshot))
        .await
        .unwrap();
    let rid = within(tbl.insert_tx(&vec_record(emb, &[1.0, 0.0, 0.0]), Some(&mut tx0)))
        .await
        .unwrap();
    let mut outcome0 = within(repo.commit_tx(tx0)).await.unwrap();
    join_tail(&mut outcome0).await;
    drop(pin0);
    assert_eq!(backend.applied_prefix(), 1, "chunk 0 must be acknowledged");

    // NOW arm: A parks pre-promote, B waits at the turnstile behind A.
    let park = park_hook();
    repo.core.journal_publish_hook.store(Some(park.clone()));
    let (turn_tx, turn_rx) = oneshot::channel();
    backend.arm_promote_turn_wait_hook_for_test(turn_tx);

    // A: delete the rid, parked pre-promote (A's data delete COMMITS at
    // publish, so the row is gone before B reads).
    let (mut tx_a, pin_a) = within(repo.begin_tx(IsolationLevel::Snapshot))
        .await
        .unwrap();
    within(tbl.delete_tx(rid, Some(&mut tx_a))).await.unwrap();
    let repo_a = repo.clone();
    let task_a = tokio::spawn(async move { repo_a.commit_tx(tx_a).await });
    within(park.reached.notified()).await;

    // B: re-insert via update-of-deleted-row — the update path reads
    // NotFound (deleted) and stages an INSERT at the SAME rid
    // (tx_update.rs: `old = None` → `plan_insert_ops(id, ...)`).
    let (mut tx_b, pin_b) = within(repo.begin_tx(IsolationLevel::Snapshot))
        .await
        .unwrap();
    within(tbl.update_tx(rid, &vec_record(emb, &[1.0, 0.0, 0.0]), Some(&mut tx_b)))
        .await
        .unwrap();
    let repo_b = repo.clone();
    let task_b = tokio::spawn(async move { repo_b.commit_tx(tx_b).await });
    let _ = within(turn_rx).await;

    park.resume.notify_one();
    let mut outcome_a = within(task_a).await.unwrap().unwrap();
    assert_eq!(
        outcome_a.materialization,
        MaterializationState::Complete,
        "tx A (delete) must commit Complete"
    );
    join_tail(&mut outcome_a).await;
    let mut outcome_b = within(task_b).await.unwrap().unwrap();
    assert_eq!(
        outcome_b.materialization,
        MaterializationState::Complete,
        "tx B (re-insert) must commit Complete"
    );
    join_tail(&mut outcome_b).await;

    assert!(
        within(hits_contain(backend, &[1.0, 0.0, 0.0], &rid)).await,
        "re-inserted rid must be present in the live graph after the \
         delete-then-reinsert pair promotes in chunk order"
    );
    let s = within(distance_of(backend, &[1.0, 0.0, 0.0], &rid)).await;
    assert!(
        s < 0.1,
        "the re-inserted rid must hold the re-inserted vector (score {s})"
    );
    // Seed chunk 0 + A's delete chunk 1 + B's re-insert chunk 2 ⇒ prefix 3.
    assert_eq!(
        backend.applied_prefix(),
        3,
        "A's and B's chunks must each be acked exactly once"
    );
    drop((pin_a, pin_b));
}

/// (d) a FAILED promote (the `FAIL_VECTOR_PROMOTE_TX_ID` failpoint, which
/// fires BEFORE the ticket loop, so the tickets Drop unentered at the end of
/// `promote_vectors`' per-table call) releases the turn: B (parked behind
/// A's failed chunk) still promotes — no hang — and A's failed chunk stays
/// unacknowledged (prune prefix stays at 1).
///
/// Fails on old code because: without the Drop-based release (turnstile
/// kept, release dropped) B's waiter is never woken after A's failure and
/// `within(task_b)` stalls out; reverting the turnstile entirely makes B
/// promote while A is parked, the turn-wait one-shot never fires, and
/// `within(turn_rx)` fails.
///
/// Awaits inside the spawned tasks are bounded by the outer `within(join)`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial]
async fn rv_index_l3b_promote_order_failed_promote_releases_turn() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("vecs"));
    let tbl = within(repo.get_table("vecs")).await.unwrap();
    within(tbl.create_index_v2(&vector_index_op("vecs", "vec_idx")))
        .await
        .unwrap();
    let emb = within(field_id(&tbl, "embedding")).await;
    let backends = within(tbl.index2_registry().all_backends()).await;
    let backend = as_vector(&backends);

    // Row 1 commits cleanly (BEFORE arming any hook): the graph gains its
    // entry point, chunk 0 acked.
    let (mut tx0, pin0) = within(repo.begin_tx(IsolationLevel::Snapshot))
        .await
        .unwrap();
    let rid = within(tbl.insert_tx(&vec_record(emb, &[1.0, 0.0, 0.0]), Some(&mut tx0)))
        .await
        .unwrap();
    let mut outcome0 = within(repo.commit_tx(tx0)).await.unwrap();
    assert_eq!(outcome0.materialization, MaterializationState::Complete);
    join_tail(&mut outcome0).await;
    drop(pin0);
    assert_eq!(backend.applied_prefix(), 1, "chunk 0 must be acknowledged");

    let park = park_hook();
    repo.core.journal_publish_hook.store(Some(park.clone()));
    let (turn_tx, turn_rx) = oneshot::channel();
    backend.arm_promote_turn_wait_hook_for_test(turn_tx);

    // A: its promote will FAIL (failpoint), but it is parked pre-promote.
    let (mut tx_a, pin_a) = within(repo.begin_tx(IsolationLevel::Snapshot))
        .await
        .unwrap();
    within(tbl.update_tx(rid, &vec_record(emb, &[0.0, 1.0, 0.0]), Some(&mut tx_a)))
        .await
        .unwrap();
    let inject_tx_id = tx_a.tx_id.0;
    FAIL_VECTOR_PROMOTE_TX_ID.store(inject_tx_id, Ordering::SeqCst);
    let repo_a = repo.clone();
    let task_a = tokio::spawn(async move { repo_a.commit_tx(tx_a).await });
    within(park.reached.notified()).await;

    // B: newer vector; its promote must WAIT behind A's doomed chunk.
    let (mut tx_b, pin_b) = within(repo.begin_tx(IsolationLevel::Snapshot))
        .await
        .unwrap();
    within(tbl.update_tx(rid, &vec_record(emb, &[0.0, 0.0, 1.0]), Some(&mut tx_b)))
        .await
        .unwrap();
    let repo_b = repo.clone();
    let task_b = tokio::spawn(async move { repo_b.commit_tx(tx_b).await });
    let _ = within(turn_rx).await;

    // Resume A: its promote fails; the ticket Drop must release the turn.
    park.resume.notify_one();
    let mut outcome_a = within(task_a).await.unwrap().unwrap();
    assert_eq!(
        outcome_a.materialization,
        MaterializationState::Complete,
        "a failed post-lock promote must NOT defer the tx (III.5)"
    );
    join_tail(&mut outcome_a).await;
    FAIL_VECTOR_PROMOTE_TX_ID.store(0, Ordering::SeqCst);

    // No hang: B promotes through the released turn.
    let mut outcome_b = within(task_b).await.unwrap().unwrap();
    join_tail(&mut outcome_b).await;

    // Per-rid oracle (tie-immune): B's vector is live after A's failed
    // promote, and A's failed mid vector never landed for this rid.
    let s_new = within(distance_of(backend, &[0.0, 0.0, 1.0], &rid)).await;
    assert!(
        s_new < 0.1,
        "B's vector must be live after A's failed promote (score {s_new})"
    );
    let s_mid = within(distance_of(backend, &[0.0, 1.0, 0.0], &rid)).await;
    assert!(
        s_mid > 0.9,
        "A's failed mid vector must NOT be live (score {s_mid})"
    );

    // A's failed chunk stays unacknowledged (never acked by its failure);
    // the prune prefix therefore cannot pass it even though B acked later.
    assert_eq!(
        backend.applied_prefix(),
        1,
        "the failed chunk must stay unacknowledged behind the prefix"
    );
    drop((pin_a, pin_b));
}

/// (g) WAIT-CYCLE ACROSS TABLES with the GLOBAL ORDER: TWO tables ("t1",
/// "t2"), ONE vector index each (ONE vector index per table is a
/// PRODUCTION CONSTRAINT — `create_index_v2` Validation: "only ONE vector
/// index per table"), so a single `apply_vector_graph_batch` call owns one
/// ticket per table, and the per-table calls release all turns before the
/// next table starts. This test therefore pins the MERGED SORTED TOKEN
/// ORDER (`apply_vector_delta_phase` visits the insert ∪ delete-only
/// tokens in ONE ascending order, `commit_phases.rs` ~:239-320) and
/// cross-table liveness — NOT a same-call multi-ticket inversion, which
/// cannot exist under the one-vector-index-per-table constraint.
///
/// Shape: tx A updates t1's row THEN t2's row (one tx, BOTH deltas staged
/// before A parks — the park fires after publish, and both delta chunks are
/// appended in A's materialize before the park, so both reservations exist
/// in ascending token order); tx B updates t2's row THEN t1's row and
/// commits → must WAIT at the lower-token table's turnstile (the seam is
/// armed ONLY there); A resumes; both txs Complete; each table's row holds
/// its newer vector; each backend prefix exactly 3.
///
/// Revert honesty (stated as required): with the old two-separate-loops
/// shape (all inserts before all delete-onlys), a tx could reserve t2 while
/// holding t1's earlier reservation and interleave against B's opposite
/// order — the handshake/seam and the bounded `within` detect the stall.
/// Under the unsorted revert the stall itself is RESIDUAL NONDETERMINISTIC
/// (map iteration order); this test asserts the strongest deterministic
/// oracle available (B waits at the low-token table's turnstile FIRST; both
/// txs Complete; both rows hold B's newer embedding; both prefixes exactly
/// 3) and NOT the nondeterministic inversion itself.
///
/// Awaits inside the spawned tasks are bounded by the outer `within(join)`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rv_index_l3b_promote_order_no_wait_cycle_across_tables_global_order() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t1"));
    repo.add_table_for_test(TableConfig::new("t2"));
    let tbl1 = within(repo.get_table("t1")).await.unwrap();
    let tbl2 = within(repo.get_table("t2")).await.unwrap();
    within(tbl1.create_index_v2(&vector_index_op("t1", "t1_vec_idx")))
        .await
        .unwrap();
    within(tbl2.create_index_v2(&vector_index_op("t2", "t2_vec_idx")))
        .await
        .unwrap();
    let emb1 = within(field_id(&tbl1, "embedding")).await;
    let emb2 = within(field_id(&tbl2, "embedding")).await;
    let backends1 = within(tbl1.index2_registry().all_backends()).await;
    let backends2 = within(tbl2.index2_registry().all_backends()).await;
    let be1 = as_vector(&backends1);
    let be2 = as_vector(&backends2);

    // Tokens ARE easily comparable (`TableManager::table_token`): the seam
    // is armed on the LOWER-token table's backend — the first table in the
    // merged ascending-token order, so the handshake is deterministic.
    let tok1 = tbl1.table_token();
    let tok2 = tbl2.table_token();
    let low_be = if tok1 <= tok2 { be1 } else { be2 };

    // Seed BEFORE arming the journal hook: one committed row PER TABLE
    // (chunk 0 acked on each → applied_prefix == 1 on each).
    let (mut tx0, pin0) = within(repo.begin_tx(IsolationLevel::Snapshot))
        .await
        .unwrap();
    let rid1 = within(tbl1.insert_tx(&vec_record(emb1, &[1.0, 0.0, 0.0]), Some(&mut tx0)))
        .await
        .unwrap();
    let mut outcome0 = within(repo.commit_tx(tx0)).await.unwrap();
    join_tail(&mut outcome0).await;
    let (mut tx1, pin1) = within(repo.begin_tx(IsolationLevel::Snapshot))
        .await
        .unwrap();
    let rid2 = within(tbl2.insert_tx(&vec_record(emb2, &[1.0, 0.0, 0.0]), Some(&mut tx1)))
        .await
        .unwrap();
    let mut outcome1 = within(repo.commit_tx(tx1)).await.unwrap();
    join_tail(&mut outcome1).await;
    drop((pin0, pin1));
    assert_eq!(be1.applied_prefix(), 1, "seed chunk 0 acked on t1");
    assert_eq!(be2.applied_prefix(), 1, "seed chunk 0 acked on t2");

    // NOW arm: A parks pre-promote (chunks reserved on BOTH tables in
    // ascending token order); B must wait at the LOW-token table's
    // turnstile.
    let park = park_hook();
    repo.core.journal_publish_hook.store(Some(park.clone()));
    let (turn_tx, turn_rx) = oneshot::channel();
    low_be.arm_promote_turn_wait_hook_for_test(turn_tx);

    // A: updates t1's row THEN t2's row (one tx) — both deltas staged and
    // reserved BEFORE the park fires (post-publish).
    let (mut tx_a, pin_a) = within(repo.begin_tx(IsolationLevel::Snapshot))
        .await
        .unwrap();
    within(tbl1.update_tx(rid1, &vec_record(emb1, &[0.0, 1.0, 0.0]), Some(&mut tx_a)))
        .await
        .unwrap();
    within(tbl2.update_tx(rid2, &vec_record(emb2, &[0.0, 1.0, 0.0]), Some(&mut tx_a)))
        .await
        .unwrap();
    let repo_a = repo.clone();
    let task_a = tokio::spawn(async move { repo_a.commit_tx(tx_a).await });
    within(park.reached.notified()).await;

    // B: updates t2's row THEN t1's row (OPPOSITE order) — the merged
    // ascending-token order makes both txs process the two tables in the
    // SAME order, so B waits at the low-token table's turnstile.
    let (mut tx_b, pin_b) = within(repo.begin_tx(IsolationLevel::Snapshot))
        .await
        .unwrap();
    within(tbl2.update_tx(rid2, &vec_record(emb2, &[0.0, 0.0, 1.0]), Some(&mut tx_b)))
        .await
        .unwrap();
    within(tbl1.update_tx(rid1, &vec_record(emb1, &[0.0, 0.0, 1.0]), Some(&mut tx_b)))
        .await
        .unwrap();
    let repo_b = repo.clone();
    let task_b = tokio::spawn(async move { repo_b.commit_tx(tx_b).await });
    // THE handshake: B must park at the LOW-token table's turnstile (the
    // seam is armed only there).
    let _ = within(turn_rx).await;

    park.resume.notify_one();
    let mut out_a = within(task_a).await.unwrap().unwrap();
    join_tail(&mut out_a).await;
    let mut out_b = within(task_b).await.unwrap().unwrap();
    join_tail(&mut out_b).await;
    assert_eq!(
        out_a.materialization,
        MaterializationState::Complete,
        "tx A must commit Complete (deadlock-free promote order)"
    );
    assert_eq!(
        out_b.materialization,
        MaterializationState::Complete,
        "tx B must commit Complete (deadlock-free promote order)"
    );

    // Per-table, per-rid oracle: each table's live graph holds B's newer
    // embedding for THAT table's row (tie-immune distance_of).
    let s1 = within(distance_of(be1, &[0.0, 0.0, 1.0], &rid1)).await;
    let s1_mid = within(distance_of(be1, &[0.0, 1.0, 0.0], &rid1)).await;
    assert!(
        s1 < 0.1 && s1_mid > 0.9,
        "t1's row must hold B's newer vector (s_new {s1}, s_mid {s1_mid})"
    );
    let s2 = within(distance_of(be2, &[0.0, 0.0, 1.0], &rid2)).await;
    let s2_mid = within(distance_of(be2, &[0.0, 1.0, 0.0], &rid2)).await;
    assert!(
        s2 < 0.1 && s2_mid > 0.9,
        "t2's row must hold B's newer vector (s_new {s2}, s_mid {s2_mid})"
    );
    // Contiguous acked prefix per backend: seed chunk 0 + A's chunk 1 +
    // B's chunk 2 ⇒ exactly 3.
    assert_eq!(
        be1.applied_prefix(),
        3,
        "A's and B's chunks must each be acked exactly once on t1"
    );
    assert_eq!(
        be2.applied_prefix(),
        3,
        "A's and B's chunks must each be acked exactly once on t2"
    );
    drop((pin_a, pin_b));
}

/// (h) RETRY-INSIDE-THE-TURN (round-2 finding 1): a promote that fails on
/// the FIRST attempt and succeeds on the retry, injected at the APPLY level
/// INSIDE the ticket loop (`FAIL_VECTOR_PROMOTE_ONCE_*`, after `wait_turn`,
/// before `t.promote`), so the FIRST failure happens while A's turn is
/// still HELD and the in-loop bounded retry re-runs with the ticket ACTIVE.
/// The old round-1 shape released/consumed the ticket BEFORE propagating
/// the failure, so `retry_materialize` re-invoked the apply on a CONSUMED
/// ticket whose `promote` answered a phantom `Ok(())` — a real adapter
/// failure "succeeded" on retry and the chunk got acknowledged without its
/// mutations ever reaching the adapter.
///
/// Oracle honesty (stated as required): the transient-retry oracle is the
/// prefix==3 + both-Complete combination — against the OLD consumed-ticket
/// code the retried apply either errors (A's chunk never acked, prefix
/// stops at 2) or phantom-Oks (prefix would read 3 without B ever
/// waiting). The per-rid scores pin the final VALUE: `distance_of(q_new) <
/// 0.1` requires A's chunk to have actually promoted BEFORE B's (B's
/// identical-rid write then replaces it), and `distance_of(q_mid) > 0.9`
/// proves the retry truly applied.
///
/// Fails on old code because: with the ticket released before `res?`, the
/// retried apply errors with "promote ticket already released" (or, before
/// that fix, silently Ok()s without applying) — A's chunk 1 is never truly
/// re-promoted, so it is never acknowledged and `applied_prefix()` stops at
/// 2 instead of the asserted 3 (and B's waiter races A's phantom retry).
///
/// Awaits inside the spawned tasks are bounded by the outer `within(join)`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial]
async fn rv_index_l3b_promote_order_transient_promote_failure_retries_in_turn() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("vecs"));
    let tbl = within(repo.get_table("vecs")).await.unwrap();
    within(tbl.create_index_v2(&vector_index_op("vecs", "vec_idx")))
        .await
        .unwrap();
    let emb = within(field_id(&tbl, "embedding")).await;
    let backends = within(tbl.index2_registry().all_backends()).await;
    let backend = as_vector(&backends);

    // Row commits cleanly (BEFORE arming any hook): chunk 0 acked.
    let (mut tx0, pin0) = within(repo.begin_tx(IsolationLevel::Snapshot))
        .await
        .unwrap();
    let rid = within(tbl.insert_tx(&vec_record(emb, &[1.0, 0.0, 0.0]), Some(&mut tx0)))
        .await
        .unwrap();
    let mut outcome0 = within(repo.commit_tx(tx0)).await.unwrap();
    join_tail(&mut outcome0).await;
    drop(pin0);
    assert_eq!(backend.applied_prefix(), 1, "chunk 0 must be acknowledged");

    let park = park_hook();
    repo.core.journal_publish_hook.store(Some(park.clone()));
    let (turn_tx, turn_rx) = oneshot::channel();
    backend.arm_promote_turn_wait_hook_for_test(turn_tx);

    // A: its FIRST promote attempt will fail (fail-once, in-loop), but it is
    // parked pre-promote.
    let (mut tx_a, pin_a) = within(repo.begin_tx(IsolationLevel::Snapshot))
        .await
        .unwrap();
    within(tbl.update_tx(rid, &vec_record(emb, &[0.0, 1.0, 0.0]), Some(&mut tx_a)))
        .await
        .unwrap();
    let inject_tx_id = tx_a.tx_id.0;
    FAIL_VECTOR_PROMOTE_ONCE_TX_ID.store(inject_tx_id, Ordering::SeqCst);
    FAIL_VECTOR_PROMOTE_ONCE_REMAINING.store(1, Ordering::SeqCst);
    let repo_a = repo.clone();
    let task_a = tokio::spawn(async move { repo_a.commit_tx(tx_a).await });
    within(park.reached.notified()).await;

    // B: newer vector; its promote must WAIT behind A's ACTIVE ticket (A's
    // turn stays held across the failed first attempt).
    let (mut tx_b, pin_b) = within(repo.begin_tx(IsolationLevel::Snapshot))
        .await
        .unwrap();
    within(tbl.update_tx(rid, &vec_record(emb, &[0.0, 0.0, 1.0]), Some(&mut tx_b)))
        .await
        .unwrap();
    let repo_b = repo.clone();
    let task_b = tokio::spawn(async move { repo_b.commit_tx(tx_b).await });
    let _ = within(turn_rx).await;

    // Resume A: attempt 1 fails IN-TURN, attempt 2 (the in-loop retry)
    // succeeds for real; the ticket is released only AFTER the loop.
    park.resume.notify_one();
    let mut outcome_a = within(task_a).await.unwrap().unwrap();
    assert_eq!(
        outcome_a.materialization,
        MaterializationState::Complete,
        "a transient promote failure must recover via the in-loop retry"
    );
    join_tail(&mut outcome_a).await;
    FAIL_VECTOR_PROMOTE_ONCE_TX_ID.store(0, Ordering::SeqCst);
    FAIL_VECTOR_PROMOTE_ONCE_REMAINING.store(0, Ordering::SeqCst);

    // No hang: B promotes through the released turn.
    let mut outcome_b = within(task_b).await.unwrap().unwrap();
    join_tail(&mut outcome_b).await;
    assert_eq!(
        outcome_b.materialization,
        MaterializationState::Complete,
        "tx B must commit Complete"
    );

    // Per-rid oracle (tie-immune): the row's LIVE vector is B's newer
    // embedding, not A's mid vector.
    let s_new = within(distance_of(backend, &[0.0, 0.0, 1.0], &rid)).await;
    let s_mid = within(distance_of(backend, &[0.0, 1.0, 0.0], &rid)).await;
    assert!(
        s_new < 0.1,
        "B's newer vector must be live after A's transient promote failure (score {s_new})"
    );
    assert!(
        s_mid > 0.9,
        "A's mid vector must NOT be live (score {s_mid})"
    );

    // EXACT ack accounting: chunk 0 (seed) + chunk 1 (A, retried and then
    // acked EXACTLY ONCE) + chunk 2 (B) ⇒ applied_prefix == 3, not more.
    // Against the OLD consumed-ticket code the retried apply fails (or
    // phantom-Oks without applying); prefix==3 + both-Complete is the
    // transient-retry oracle — the per-rid scores above pin the final value.
    assert_eq!(
        backend.applied_prefix(),
        3,
        "A's chunk must be acked EXACTLY ONCE after the successful in-turn retry \
         (prefix start+2 == 3)"
    );
    drop((pin_a, pin_b));
}

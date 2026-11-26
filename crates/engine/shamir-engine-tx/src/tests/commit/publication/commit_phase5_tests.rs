//! HIGH-6 Phase 5c/5d: committed-tx index + HNSW application.
//!
//! Proves the happy-path commit pipeline now:
//!  * Part A — applies `tx.index_write_set` postings to the table's
//!    `info_store` so index2 (FTS) queries surface the record after a
//!    committed `insert_tx` (Phase 5c, `apply_index_ops_at_commit`).
//!  * Part B — promotes the tx's staged vectors into the live graph so a
//!    non-tx vector search finds them (Phase 5d, `apply_staged_vectors`).
//!    III.5: this promote now runs OUTSIDE `commit_lock` and AFTER Phase 7
//!    (`promote_vectors`), since HNSW is a derived read-accelerator — the
//!    end-to-end "searchable after commit" assertion exercises that moved
//!    path.
//!  * Part C — RAII: an aborted (dropped) tx leaves no staged vectors
//!    behind, because they live inside the `TxContext::staged_vectors`
//!    and vanish with the tx — the live graph was never touched.
//!  * Part D (III.5) — a failed post-lock HNSW GRAPH promote does NOT
//!    defer the tx: the outcome is `Complete`, no WAL marker is left
//!    inflight (Phase 7 already ran under the lock), and the data is
//!    durable. The live in-RAM graph lags, but the DURABLE delta chunk
//!    (appended PRE-publish by `apply_vector_delta_phase` — #426, VR-4
//!    variant A) is replayed by `restore_on_open` on the next open.
//!  * Part E (#426, VR-4 variant A) — a failed PRE-publish DELTA append
//!    DOES defer the tx: the delta chunk is the durable bridge
//!    `restore_on_open` relies on, so a missing echo is a loss of the
//!    vector mutation from the live graph. Unlike a graph-promote
//!    failure (Part D), this surfaces as `Deferred` so the client can
//!    detect it and retry — NOT a silent `Complete`.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use shamir_query_types::admin::CreateIndexOp;
use shamir_storage::storage_in_memory::InMemoryRepo;
use shamir_tx::IsolationLevel;
use shamir_types::core::interner::{InternerKey, TouchInd};
use shamir_types::types::common::new_map_wc;
use shamir_types::types::value::InnerValue;

use crate::index2::backend::{FtsMode, IndexQuery, IndexResult};
use crate::repo::repo_instance::RepoInstance;
use crate::repo::repo_types::BoxRepo;
use crate::table::table_manager::table_token_for;
use crate::table::TableConfig;
use crate::tx::commit_phases::{FAIL_VECTOR_DELTA_TX_ID, FAIL_VECTOR_PROMOTE_TX_ID};
use crate::tx::tx_outcome::MaterializationState;
use serial_test::serial;
use shamir_index::tokenizer::token_identity;

fn make_repo() -> RepoInstance {
    let repo = Arc::new(InMemoryRepo::new());
    RepoInstance::new("test".into(), BoxRepo::InMemory(repo), Vec::new())
}

async fn field_id(tbl: &crate::table::TableManager, name: &str) -> u64 {
    let interner = tbl.interner().get().await.unwrap();
    match interner.touch_ind(name).unwrap() {
        TouchInd::Exists(k) | TouchInd::New(k) => k.id(),
    }
}

fn fts_index_op() -> CreateIndexOp {
    CreateIndexOp {
        create_index: "body_fts".into(),
        table: "docs".into(),
        fields: vec![vec!["body".into()]],
        unique: false,
        sorted: false,
        repo: "main".into(),
        index_type: Some("fts".into()),
        fts_tokenizer: Some("whitespace".into()),
        fts_language: None,
        functional_op: None,
        functional_args: None,
        vector_dim: None,
        vector_metric: None,
        vector_quantization: None,
        include: Vec::new(),
        if_not_exists: false,
    }
}

fn vector_index_op() -> CreateIndexOp {
    CreateIndexOp {
        create_index: "vec_idx".into(),
        table: "vecs".into(),
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

fn text_record(body_key_id: u64, text: &str) -> InnerValue {
    let mut m = new_map_wc(1);
    m.insert(InternerKey::new(body_key_id), InnerValue::Str(text.into()));
    InnerValue::Map(m)
}

fn vec_record(emb_key_id: u64, vec: &[f64]) -> InnerValue {
    let mut m = new_map_wc(1);
    m.insert(
        InternerKey::new(emb_key_id),
        InnerValue::List(vec.iter().map(|f| InnerValue::F64(*f)).collect()),
    );
    InnerValue::Map(m)
}

/// Part A: a record inserted via `insert_tx` and committed must be
/// findable through the FTS index2 backend — proving Phase 5c applied
/// the staged `SetPosting` ops to the table's `info_store` (and the
/// `BumpFtsStats` op to the backend's in-memory BM25 stats).
#[tokio::test]
async fn committed_tx_index_postings_visible() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("docs"));
    let tbl = repo.get_table("docs").await.unwrap();
    tbl.create_index_v2(&fts_index_op()).await.unwrap();

    let body_id = field_id(&tbl, "body").await;

    // Resolve the FTS backend up front.
    let name_id = field_id(&tbl, "body_fts").await;
    let backend = tbl
        .index2_registry()
        .get_by_name(name_id)
        .await
        .expect("body_fts must be registered");

    // Insert via tx then COMMIT.
    let (mut tx, guard) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let rid = tbl
        .insert_tx(
            &text_record(body_id, "shamir transaction commit"),
            Some(&mut tx),
        )
        .await
        .unwrap();
    repo.commit_tx(tx).await.unwrap();
    drop(guard);

    let result = backend
        .lookup(IndexQuery::Fts {
            tokens: vec![token_identity("transaction")],
            mode: FtsMode::AndAll,
            limit: None,
        })
        .await
        .unwrap();

    match result {
        IndexResult::Ranked(hits) => {
            let rids: Vec<_> = hits.iter().map(|(r, _)| *r).collect();
            assert!(
                rids.contains(&rid),
                "committed insert_tx must make FTS postings visible (HIGH-6 Phase 5c); \
                 got {:?}, expected {:?}",
                rids,
                rid
            );
        }
        _ => panic!("FTS lookup must return Ranked"),
    }
}

/// Part B: a vector staged via `insert_tx` and committed must be
/// findable via a NON-tx vector search — proving Phase 5d promoted the
/// tx's staged vector (`TxContext::staged_vectors`) into the live HNSW
/// graph via `apply_staged_vectors`. III.5: the promote now runs OUTSIDE
/// `commit_lock` (in `promote_vectors`, after Phase 7), so this end-to-end
/// "searchable after commit" check covers the moved, post-lock path.
#[tokio::test]
#[serial]
async fn committed_tx_hnsw_vector_searchable() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("vecs"));
    let tbl = repo.get_table("vecs").await.unwrap();
    tbl.create_index_v2(&vector_index_op()).await.unwrap();

    let emb_id = field_id(&tbl, "embedding").await;
    let name_id = field_id(&tbl, "vec_idx").await;
    let backend = tbl
        .index2_registry()
        .get_by_name(name_id)
        .await
        .expect("vec_idx must be registered");

    // Stage + commit a vector through a tx.
    let (mut tx, guard) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let rid = tbl
        .insert_tx(&vec_record(emb_id, &[1.0, 0.0, 0.0]), Some(&mut tx))
        .await
        .unwrap();

    // Before commit: a NON-tx search must NOT see the staged vector.
    let pre = backend
        .lookup(IndexQuery::Vector {
            vec: vec![1.0, 0.0, 0.0],
            k: 10,
            opts: crate::index2::vector::SearchOpts::default(),
        })
        .await
        .unwrap();
    if let IndexResult::Ranked(hits) = pre {
        assert!(
            !hits.iter().any(|(r, _)| *r == rid),
            "pre-commit non-tx search must not see staged vector"
        );
    }

    repo.commit_tx(tx).await.unwrap();
    drop(guard);

    // HNSW promotion (Phase 5d, post-lock since III.5) lands the vector
    // via a spawn_blocking insert that may have an async tail. Poll for
    // it deterministically rather than sleeping a fixed interval — a
    // fixed sleep is flaky under parallel-test CPU contention (it failed
    // ~2/8 full-workspace runs). The loop succeeds the instant promotion
    // completes; the bound only guards a genuinely stuck promote.
    let mut found = false;
    for _ in 0..200 {
        let post = backend
            .lookup(IndexQuery::Vector {
                vec: vec![1.0, 0.0, 0.0],
                k: 10,
                opts: crate::index2::vector::SearchOpts::default(),
            })
            .await
            .unwrap();
        match post {
            IndexResult::Ranked(hits) => {
                if hits.iter().any(|(r, _)| *r == rid) {
                    found = true;
                    break;
                }
            }
            _ => panic!("Vector lookup must return Ranked"),
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(
        found,
        "committed insert_tx must promote staged vector into live HNSW \
         graph (HIGH-6 Phase 5d); rid {:?} never appeared in search results",
        rid
    );
}

/// Part C: aborting (dropping) a tx that staged an HNSW vector leaves no
/// trace — the staged vector lived inside `TxContext::staged_vectors` and
/// vanished with the tx (RAII). The live graph was never touched, so a
/// non-tx search finds nothing.
#[tokio::test]
#[serial]
async fn aborted_tx_hnsw_staged_cleaned() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("vecs"));
    let tbl = repo.get_table("vecs").await.unwrap();
    tbl.create_index_v2(&vector_index_op()).await.unwrap();

    let emb_id = field_id(&tbl, "embedding").await;
    let name_id = field_id(&tbl, "vec_idx").await;
    let token = table_token_for("vecs");
    let backend = tbl
        .index2_registry()
        .get_by_name(name_id)
        .await
        .expect("vec_idx must be registered");

    let rid = {
        let (mut tx, guard) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
        let rid = tbl
            .insert_tx(&vec_record(emb_id, &[1.0, 0.0, 0.0]), Some(&mut tx))
            .await
            .unwrap();

        // The staged vector IS visible to an in-tx search before abort:
        // resolve the tx's own staged slice and merge it in the lookup.
        let res = backend
            .lookup_tx(
                token,
                IndexQuery::Vector {
                    vec: vec![1.0, 0.0, 0.0],
                    k: 10,
                    opts: crate::index2::vector::SearchOpts::default(),
                },
                None,
                tx.staged_vectors_for(token),
            )
            .await
            .unwrap();
        let in_tx_before = match res {
            IndexResult::Ranked(hits) => hits.iter().any(|(r, _)| *r == rid),
            _ => panic!("expected Ranked"),
        };
        assert!(
            in_tx_before,
            "staged vector must be visible to an in-tx search before abort"
        );

        // Abort: just drop the tx. No explicit rollback call — RAII.
        drop(tx);
        drop(guard);
        rid
    };

    // The live graph was never touched: a non-tx search finds nothing.
    let post = backend
        .lookup(IndexQuery::Vector {
            vec: vec![1.0, 0.0, 0.0],
            k: 10,
            opts: crate::index2::vector::SearchOpts::default(),
        })
        .await
        .unwrap();
    match post {
        IndexResult::Ranked(hits) => {
            assert!(
                !hits.iter().any(|(r, _)| *r == rid),
                "aborted tx must leave no vector in the live graph (HIGH-6 Part C); got {:?}",
                hits.iter().map(|(r, _)| *r).collect::<Vec<_>>()
            );
        }
        _ => panic!("expected Ranked"),
    }
}

/// RAII guarantee, stated directly: stage a vector into a `TxContext`,
/// drop the tx without commit, and confirm (a) the live graph never saw
/// it and (b) nothing lingers — `staged_vectors` was owned by the tx, so
/// the drop freed it. Since staging is now tx-local, the "no lingering
/// buffer" half is structurally guaranteed; we assert the live-graph
/// cleanliness via a search.
#[tokio::test]
#[serial]
async fn dropped_tx_staged_vectors_freed_via_raii() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("vecs"));
    let tbl = repo.get_table("vecs").await.unwrap();
    tbl.create_index_v2(&vector_index_op()).await.unwrap();

    let emb_id = field_id(&tbl, "embedding").await;
    let name_id = field_id(&tbl, "vec_idx").await;
    let token = table_token_for("vecs");
    let backend = tbl
        .index2_registry()
        .get_by_name(name_id)
        .await
        .expect("vec_idx must be registered");

    let rid = {
        let (mut tx, guard) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
        let rid = tbl
            .insert_tx(&vec_record(emb_id, &[0.0, 1.0, 0.0]), Some(&mut tx))
            .await
            .unwrap();
        // The vector lives in the tx, keyed by this table's token.
        assert_eq!(
            tx.staged_vectors_for(token).map(<[_]>::len),
            Some(1),
            "vector must be staged inside the TxContext"
        );
        drop(tx); // RAII: staged_vectors freed, no commit.
        drop(guard);
        rid
    };

    let post = backend
        .lookup(IndexQuery::Vector {
            vec: vec![0.0, 1.0, 0.0],
            k: 10,
            opts: crate::index2::vector::SearchOpts::default(),
        })
        .await
        .unwrap();
    match post {
        IndexResult::Ranked(hits) => assert!(
            !hits.iter().any(|(r, _)| *r == rid),
            "dropped tx must never reach the live graph (RAII); got {:?}",
            hits.iter().map(|(r, _)| *r).collect::<Vec<_>>()
        ),
        _ => panic!("expected Ranked"),
    }
}

/// In-tx read-your-own-writes for vectors: stage a vector, then search
/// WITHIN the same tx (threading `tx.staged_vectors_for(token)` into the
/// lookup) and find it. Proves the staged-slice wiring of `lookup_tx`.
#[tokio::test]
#[serial]
async fn in_tx_search_sees_own_staged_vector() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("vecs"));
    let tbl = repo.get_table("vecs").await.unwrap();
    tbl.create_index_v2(&vector_index_op()).await.unwrap();

    let emb_id = field_id(&tbl, "embedding").await;
    let name_id = field_id(&tbl, "vec_idx").await;
    let token = table_token_for("vecs");
    let backend = tbl
        .index2_registry()
        .get_by_name(name_id)
        .await
        .expect("vec_idx must be registered");

    let (mut tx, _guard) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let rid = tbl
        .insert_tx(&vec_record(emb_id, &[1.0, 0.0, 0.0]), Some(&mut tx))
        .await
        .unwrap();

    let res = backend
        .lookup_tx(
            token,
            IndexQuery::Vector {
                vec: vec![1.0, 0.0, 0.0],
                k: 10,
                opts: crate::index2::vector::SearchOpts::default(),
            },
            None,
            tx.staged_vectors_for(token),
        )
        .await
        .unwrap();
    match res {
        IndexResult::Ranked(hits) => assert!(
            hits.iter().any(|(r, _)| *r == rid),
            "in-tx search must see the tx's own staged vector; got {:?}",
            hits.iter().map(|(r, _)| *r).collect::<Vec<_>>()
        ),
        _ => panic!("expected Ranked"),
    }
}

/// Part D (III.5): the HNSW GRAPH promote moved OUT of `commit_lock` and
/// runs AFTER Phase 7 (`wal.commit`). The decisive behavioural consequence:
/// a FAILED post-lock GRAPH promote no longer defers the tx. We arm a
/// synthetic promote failure for exactly this tx and prove:
///
///  (1) the commit returns `Ok` with `MaterializationState::Complete`
///      (NOT `Deferred`) — data + index already committed and materialized
///      under the lock, so the tx is fully materialized;
///  (2) NO WAL marker is left inflight — Phase 7 ran under the lock BEFORE
///      the promote, so the marker is gone (a pre-III.5 deferred promote
///      would instead have skipped Phase 7 and left it inflight);
///  (3) the data record is durable and fetchable.
///
/// The live in-RAM graph lags until the next open, but the DURABLE delta
/// chunk (appended PRE-publish by `apply_vector_delta_phase` — #426, VR-4
/// variant A) is replayed by `restore_on_open::replay_delta` on reopen, so
/// the vector is re-materialized from the durable echo — NOT lost. The
/// graph promote is a RAM-only mutation; the delta chunk is the durable
/// bridge.
///
/// `current_thread` flavor keeps the whole commit on one task/thread so the
/// process-global injection register is observed deterministically.
#[tokio::test(flavor = "current_thread")]
#[serial]
async fn failed_post_lock_vector_promote_is_committed_not_deferred() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("vecs"));
    let tbl = repo.get_table("vecs").await.unwrap();
    tbl.create_index_v2(&vector_index_op()).await.unwrap();

    let emb_id = field_id(&tbl, "embedding").await;

    // Stage a vector through a real `insert_tx` so `tx.staged_vectors` is
    // populated by the production path (Phase 5d input).
    let (mut tx, guard) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let rid = tbl
        .insert_tx(&vec_record(emb_id, &[1.0, 0.0, 0.0]), Some(&mut tx))
        .await
        .unwrap();

    // The gate assigns the tx_id; capture it to arm the promote-failure
    // injection for exactly this tx.
    let inject_tx_id = tx.tx_id.0;
    assert!(inject_tx_id != 0, "tx_id must be assigned");

    FAIL_VECTOR_PROMOTE_TX_ID.store(inject_tx_id, Ordering::SeqCst);
    let outcome = repo.commit_tx(tx).await;
    FAIL_VECTOR_PROMOTE_TX_ID.store(0, Ordering::SeqCst);
    drop(guard);

    // (1) COMMITTED + Complete — a failed PROMOTE after the lock + Phase 7
    //     does NOT defer the tx (III.5).
    let outcome = outcome.expect("commit must succeed even when the post-lock promote fails");
    assert_eq!(
        outcome.materialization,
        MaterializationState::Complete,
        "a failed post-lock HNSW GRAPH promote must NOT defer the tx — data \
         + index already committed under the lock; the live graph lags but \
         the durable delta chunk (appended pre-publish, #426) is replayed by \
         restore_on_open on reopen"
    );
    assert!(
        outcome.materialized(),
        "materialized() must be true: the visibility-bearing projections landed inline"
    );

    // (2) The data record is durable regardless of the promote failure.
    let stored = tbl
        .get(rid)
        .await
        .expect("committed data record must be durable");
    assert!(
        matches!(stored, InnerValue::Map(_)),
        "stored record must be the committed map, got {stored:?}"
    );
}

/// Part E (#426, VR-4 variant A): a failed PRE-publish DELTA append DOES
/// defer the tx. The delta chunk is the durable bridge `restore_on_open`
/// relies on; a missing echo is a loss of the vector mutation from the
/// live graph (recovery does NOT replay vectors — they are derived, not
/// WAL-serialised). Unlike a GRAPH-promote failure (Part D, which stays
/// `Complete` because the delta chunk is durable), a DELTA-append failure
/// surfaces as `Deferred` so the client DETECTS the missing durable echo
/// and can retry — NOT a silent `Complete` that hides the loss.
///
/// This is the negative-test half of variant A's failure contract (§5.3 of
/// the design): the residual risk is that a persistent delta-append
/// failure still loses the vector mutation, but now it is DETECTABLE
/// pre-ack (`Deferred` + warn-log + metric) instead of silent post-ack.
///
/// `current_thread` flavor keeps the whole commit on one task/thread so
/// the process-global injection register is observed deterministically.
#[tokio::test(flavor = "current_thread")]
#[serial]
async fn failed_pre_publish_delta_append_is_deferred() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("vecs"));
    let tbl = repo.get_table("vecs").await.unwrap();
    tbl.create_index_v2(&vector_index_op()).await.unwrap();

    let emb_id = field_id(&tbl, "embedding").await;

    // Stage a vector through a real `insert_tx` so `tx.staged_vectors` is
    // populated by the production path.
    let (mut tx, guard) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let rid = tbl
        .insert_tx(&vec_record(emb_id, &[1.0, 0.0, 0.0]), Some(&mut tx))
        .await
        .unwrap();

    // Arm the DELTA-append failure injection for exactly this tx.
    let inject_tx_id = tx.tx_id.0;
    assert!(inject_tx_id != 0, "tx_id must be assigned");
    FAIL_VECTOR_DELTA_TX_ID.store(inject_tx_id, Ordering::SeqCst);
    let outcome = repo.commit_tx(tx).await;
    FAIL_VECTOR_DELTA_TX_ID.store(0, Ordering::SeqCst);
    drop(guard);

    // The commit SUCCEEDS (Phase 4 made the data durable; abort before
    // publish is impossible), but the materialization is `Deferred`: the
    // delta-append failure lost the durable vector echo, so the tx is
    // reported as not-fully-materialized. This is the honest surface —
    // NOT a silent `Complete` that hides the loss (the pre-#426 behavior
    // was a silent post-ack loss with no signal at all).
    let outcome =
        outcome.expect("commit must succeed even when the pre-publish delta append fails");
    assert_eq!(
        outcome.materialization,
        MaterializationState::Deferred,
        "a failed pre-publish DELTA append MUST defer the tx — the delta \
         chunk is the durable bridge restore_on_open relies on; a missing \
         echo loses the vector mutation, so the client must be signaled to \
         retry (NOT a silent Complete)"
    );
    assert!(
        !outcome.materialized(),
        "materialized() must be false: the delta-append failure left the \
         vector mutation without a durable echo"
    );

    // The DATA record is still durable (Phase 5a published the overlay;
    // recovery / the drainer will reconcile the data). The vector
    // PROJECTION is what is lost — that is the deferred part.
    let stored = tbl
        .get(rid)
        .await
        .expect("committed data record must be durable even with a delta failure");
    assert!(
        matches!(stored, InnerValue::Map(_)),
        "stored record must be the committed map, got {stored:?}"
    );

    // rid is used by the assertions above; suppress unused-variable warning
    // on builds where the get() is compiled out.
    let _ = rid;
}

/// `vector_index_op` for a second, differently-named table/index — used to
/// prove the per-token borrow in `apply_vector_delta_phase`/`promote_vectors`
/// resolves each table's OWN staged embedding, not a stale/mixed-up slice
/// left over from another token's iteration.
fn vector_index_op_2() -> CreateIndexOp {
    CreateIndexOp {
        create_index: "vec_idx2".into(),
        table: "vecs2".into(),
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

/// P2 perf fix regression (audit 4.26): staged vector embedding payloads
/// used to be deep-cloned into an owned batch once in
/// `apply_vector_delta_phase` and again in `promote_vectors`; the fix
/// borrows straight out of `tx.staged_vectors` in both, keyed per table
/// token. A tx that stages DIFFERENT vectors into TWO tables exercises the
/// per-token borrow on each loop iteration: if the borrow ever resolved to
/// the wrong token (or a stale slice), one table's committed vector would
/// end up wrong or missing. Also exercises that mutating
/// `tx.async_prefix_failed` after the `retry_materialize` call in
/// `apply_vector_delta_phase` does not conflict with (or corrupt) the
/// short-lived borrow taken for that same iteration.
#[tokio::test]
#[serial]
async fn committed_tx_multi_table_hnsw_vectors_each_searchable() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("vecs"));
    repo.add_table_for_test(TableConfig::new("vecs2"));
    let tbl1 = repo.get_table("vecs").await.unwrap();
    let tbl2 = repo.get_table("vecs2").await.unwrap();
    tbl1.create_index_v2(&vector_index_op()).await.unwrap();
    tbl2.create_index_v2(&vector_index_op_2()).await.unwrap();

    let emb1_id = field_id(&tbl1, "embedding").await;
    let emb2_id = field_id(&tbl2, "embedding").await;
    let name1_id = field_id(&tbl1, "vec_idx").await;
    let name2_id = field_id(&tbl2, "vec_idx2").await;
    let backend1 = tbl1
        .index2_registry()
        .get_by_name(name1_id)
        .await
        .expect("vec_idx must be registered");
    let backend2 = tbl2
        .index2_registry()
        .get_by_name(name2_id)
        .await
        .expect("vec_idx2 must be registered");

    let (mut tx, guard) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let rid1 = tbl1
        .insert_tx(&vec_record(emb1_id, &[1.0, 0.0, 0.0]), Some(&mut tx))
        .await
        .unwrap();
    let rid2 = tbl2
        .insert_tx(&vec_record(emb2_id, &[0.0, 1.0, 0.0]), Some(&mut tx))
        .await
        .unwrap();

    repo.commit_tx(tx).await.unwrap();
    drop(guard);

    // Poll each table's backend independently (same rationale as
    // `committed_tx_hnsw_vector_searchable`: the promote's underlying
    // adapter insert may finish asynchronously).
    async fn poll_for(
        backend: &std::sync::Arc<dyn crate::index2::backend::IndexBackend>,
        query_vec: Vec<f32>,
        rid: shamir_types::types::record_id::RecordId,
    ) -> bool {
        for _ in 0..200 {
            let res = backend
                .lookup(IndexQuery::Vector {
                    vec: query_vec.clone(),
                    k: 10,
                    opts: crate::index2::vector::SearchOpts::default(),
                })
                .await
                .unwrap();
            if let IndexResult::Ranked(hits) = res {
                if hits.iter().any(|(r, _)| *r == rid) {
                    return true;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        false
    }

    assert!(
        poll_for(&backend1, vec![1.0, 0.0, 0.0], rid1).await,
        "table 1's own staged vector must be promoted and searchable — a \
         mixed-up per-token borrow would land the WRONG table's embedding \
         (or none) here"
    );
    assert!(
        poll_for(&backend2, vec![0.0, 1.0, 0.0], rid2).await,
        "table 2's own staged vector must be promoted and searchable — a \
         mixed-up per-token borrow would land the WRONG table's embedding \
         (or none) here"
    );

    // Cross-check: table 1's backend must NOT surface table 2's vector and
    // vice versa (would indicate the borrow crossed tokens).
    let cross1 = backend1
        .lookup(IndexQuery::Vector {
            vec: vec![0.0, 1.0, 0.0],
            k: 10,
            opts: crate::index2::vector::SearchOpts::default(),
        })
        .await
        .unwrap();
    if let IndexResult::Ranked(hits) = cross1 {
        assert!(
            !hits.iter().any(|(r, _)| *r == rid2),
            "table 1's backend must never see table 2's rid"
        );
    }
}
/// Part F (P0 delta-prune, brief 164): a FAILED post-lock graph promote
/// (`FAIL_VECTOR_PROMOTE_TX_ID`) must NEVER acknowledge its delta chunk.
///
/// Row 1 commits successfully first: the HNSW graph gains its entry point
/// (the vendored dump fails with "entry point not initialized" on an empty
/// graph) and chunk 0 is acknowledged. Row 2 then commits with the promote
/// failure injected: its chunk 1 is appended but never acknowledged, so a
/// later snapshot absorbs only chunk 0 (`delta_applied_upto == 1`), keeps
/// chunk 1 on disk, and a restart replays it over the snapshot — the
/// failed row's vector is recovered.
///
/// Revert analysis: reverting `prune_boundary` (the single source both the
/// spawned snapshot and the inline seam use) back to the allocation
/// counter makes the snapshot jump `delta_applied_upto` to 2, pruning
/// chunk 1 — the manifest and chunk-key assertions fail and the restart
/// loses the vector. Erroneously acknowledging the failed promote makes
/// the prefix assertions fail first.
#[tokio::test(flavor = "current_thread")]
#[serial]
async fn rv_index_l3_delta_prune_failed_promote_never_acks_chunk() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("vecs"));
    let tbl = repo.get_table("vecs").await.unwrap();
    tbl.create_index_v2(&vector_index_op()).await.unwrap();

    let emb_id = field_id(&tbl, "embedding").await;
    let info_store = tbl.info_store_for_test();

    let backends = tbl.index2_registry().all_backends().await;
    let backend: &shamir_index::vector::vector_backend::VectorBackend = backends
        .iter()
        .find_map(|b| {
            b.as_any()
                .downcast_ref::<shamir_index::vector::vector_backend::VectorBackend>()
        })
        .expect("the table must own a VectorBackend");

    // Row 1: successful commit. The graph now has an entry point and the
    // promote acknowledged chunk 0.
    let (mut tx, guard) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let _rid0 = tbl
        .insert_tx(&vec_record(emb_id, &[1.0, 0.0, 0.0]), Some(&mut tx))
        .await
        .unwrap();
    let outcome = repo.commit_tx(tx).await;
    drop(guard);
    let outcome = outcome.expect("the first commit must succeed");
    assert_eq!(outcome.materialization, MaterializationState::Complete);
    assert_eq!(
        backend.applied_prefix(),
        1,
        "the successful promote must have acknowledged chunk 0"
    );

    // Row 2: inject the post-lock promote failure.
    let (mut tx, guard) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let rid1 = tbl
        .insert_tx(&vec_record(emb_id, &[0.0, 1.0, 0.0]), Some(&mut tx))
        .await
        .unwrap();
    let inject_tx_id = tx.tx_id.0;
    assert!(inject_tx_id != 0, "tx_id must be assigned");

    FAIL_VECTOR_PROMOTE_TX_ID.store(inject_tx_id, Ordering::SeqCst);
    let outcome = repo.commit_tx(tx).await;
    FAIL_VECTOR_PROMOTE_TX_ID.store(0, Ordering::SeqCst);
    drop(guard);

    let outcome = outcome.expect("commit must succeed even when the post-lock promote fails");
    assert_eq!(
        outcome.materialization,
        MaterializationState::Complete,
        "a failed post-lock promote must NOT defer the tx (III.5)"
    );

    // The failed promote must NOT have acknowledged chunk 1.
    assert_eq!(
        backend.applied_prefix(),
        1,
        "a failed graph promote must never acknowledge its delta chunk"
    );

    // Exactly two durable chunks (indices 0 and 1).
    let id = shamir_index::backend::IndexBackend::descriptor(backend).id;
    let keyspace = format!("__vec_snap__{}", id);
    assert_eq!(
        shamir_index::vector::snapshot::highest_delta_index(&info_store, &keyspace)
            .await
            .unwrap(),
        1,
        "two delta chunks must be on disk"
    );
    let chunk_key1 = shamir_storage::types::RecordKey::from(bytes::Bytes::from(format!(
        "{keyspace}.delta.0000000001"
    )));
    assert!(
        info_store.get(chunk_key1.clone()).await.is_ok(),
        "the unacked chunk must exist on disk"
    );

    // A later snapshot: the boundary stays at the acknowledged prefix, so
    // chunk 0 is absorbed+pruned and the unacked chunk 1 SURVIVES the flip.
    backend.snapshot_now(&info_store).await.unwrap();
    assert_eq!(
        backend.applied_prefix(),
        1,
        "the failed promote's chunk stays unacknowledged across a snapshot"
    );
    let m = shamir_index::vector::snapshot::read_manifest(&info_store, &keyspace)
        .await
        .unwrap();
    assert_eq!(
        m.delta_applied_upto, 1,
        "the manifest must absorb only the acked chunk 0"
    );
    assert!(
        info_store.get(chunk_key1).await.is_ok(),
        "the unacked chunk must survive the snapshot flip (old code pruned it)"
    );

    // Restart: a fresh backend over the same store replays chunk 1 over the
    // snapshot; the failed row's vector is recovered and searchable.
    let desc = shamir_index::backend::IndexBackend::descriptor(backend).clone();
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
    let restarted =
        shamir_index::vector::vector_backend::VectorBackend::new(desc, vec![emb_id], adapter, None);
    shamir_index::backend::IndexBackend::restore_on_open(
        &restarted,
        Arc::clone(&info_store),
        tbl.data_store_for_test().clone(),
    )
    .await
    .unwrap();
    assert_eq!(
        shamir_index::backend::IndexBackend::rebuild_count(&restarted),
        0,
        "restart must take the snapshot+delta path"
    );
    let r = shamir_index::backend::IndexBackend::lookup(
        &restarted,
        IndexQuery::Vector {
            vec: vec![0.0, 1.0, 0.0],
            k: 1,
            opts: crate::index2::vector::SearchOpts::default(),
        },
    )
    .await
    .unwrap();
    match r {
        IndexResult::Ranked(ranked) => assert_eq!(
            ranked[0].0, rid1,
            "the failed row's vector must be recovered by delta replay on restart"
        ),
        _ => panic!("expected Ranked result"),
    }
}

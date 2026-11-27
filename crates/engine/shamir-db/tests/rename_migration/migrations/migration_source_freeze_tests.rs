//! Source-freeze contract regressions (#1220). No timing or private test hooks.

use bytes::Bytes;
use shamir_db::engine::index::write_barrier_flags::INDEX2_CREATE;
use shamir_db::engine::index2::build_index2_backend;
use shamir_db::engine::migration::MigrationPhase;
use shamir_db::engine::repo::repo_types::BoxRepoFactory;
use shamir_db::engine::repo::RepoConfig;
use shamir_db::engine::table::TableConfig;
use shamir_db::query::batch::{BatchRequest, BatchResponse};
use shamir_db::ShamirDb;
use shamir_query_builder::batch::Batch;
use shamir_query_builder::write::{Insert, Update};
use shamir_query_builder::{ddl, doc, Query};
use shamir_storage::types::RecordKey;

async fn setup() -> ShamirDb {
    let db = ShamirDb::init_memory().await.unwrap();
    db.enable_experimental_migration_api();
    db.create_db("testdb")
        .await
        .expect("create_db")
        .add_repo(
            RepoConfig::new("src", BoxRepoFactory::in_memory()).add_table(TableConfig::new("docs")),
        )
        .await
        .unwrap();
    exec(&db, insert("original")).await;
    db
}

async fn exec(db: &ShamirDb, request: BatchRequest) -> BatchResponse {
    let response = db.execute("testdb", &request).await.unwrap();
    if let Some(tx) = &response.transaction {
        assert_eq!(tx.status, "committed", "unexpected abort: {tx:?}");
    }
    response
}

fn insert(body: &str) -> BatchRequest {
    let mut batch = Batch::new();
    batch.insert(
        "write",
        Insert::with_repo("src", "docs").row(doc! { "body" => body }),
    );
    batch
        .to_request_via_msgpack()
        .expect("batch request encoding")
}

fn start_request() -> BatchRequest {
    let mut batch = Batch::new();
    batch.start_migration(
        "migration",
        ddl::start_migration("docs", "dst", "in_memory")
            .repo("src")
            .hmac("dummy")
            .build()
            .expect("build StartMigration"),
    );
    batch
        .to_request_via_msgpack()
        .expect("batch request encoding")
}

async fn start(db: &ShamirDb) -> String {
    exec(db, start_request()).await.results["migration"].records[0]
        .get_value_str("migration_id")
        .unwrap()
        .to_string()
}

async fn commit(db: &ShamirDb, id: &str) {
    let mut batch = Batch::new();
    batch.commit_migration(
        "commit",
        ddl::commit_migration(id)
            .hmac("dummy")
            .build()
            .expect("build CommitMigration"),
    );
    let response = exec(
        db,
        batch
            .to_request_via_msgpack()
            .expect("batch request encoding"),
    )
    .await;
    assert_eq!(
        response.results["commit"].records[0].get_value_str("phase"),
        Some("committed"),
    );
}

async fn rows(db: &ShamirDb, repo: &str) -> BatchResponse {
    let mut batch = Batch::new();
    batch.query("rows", Query::with_repo(repo, "docs").order_by_asc("body"));
    exec(
        db,
        batch
            .to_request_via_msgpack()
            .expect("batch request encoding"),
    )
    .await
}

async fn assert_frozen(db: &ShamirDb, request: BatchRequest) {
    match db.execute("testdb", &request).await {
        Err(error) => assert_eq!(error.code(), Some("table_migrating"), "{error:?}"),
        Ok(response) => {
            let tx = response.transaction.expect("frozen write must abort");
            assert_eq!(tx.status, "aborted", "{tx:?}");
            assert_eq!(tx.reason.as_deref(), Some("table_migrating"), "{tx:?}");
        }
    }
}

// Red before #1220: inserts after the snapshot are accepted, not intercepted.
#[tokio::test]
async fn r1_insert_after_start_is_rejected_and_commit_preserves_all_rows() {
    let db = setup().await;
    exec(&db, insert("second")).await;
    let baseline = rows(&db, "src").await;
    assert_eq!(baseline.results["rows"].records.len(), 2);
    let id = start(&db).await;
    assert_frozen(&db, insert("must not land")).await;
    assert_eq!(
        rows(&db, "src").await.results["rows"].records,
        baseline.results["rows"].records,
    );
    commit(&db, &id).await;
    // Compare complete records, not counts or one selected field.
    assert_eq!(
        rows(&db, "dst").await.results["rows"].records,
        baseline.results["rows"].records,
    );
    assert_eq!(
        rows(&db, "dst").await.results["rows"].records,
        rows(&db, "src").await.results["rows"].records,
    );
    exec(&db, insert("after commit")).await;
    assert_eq!(rows(&db, "src").await.results["rows"].records.len(), 3);
}

// Red before #1220: the commit-time gate admits a pre-start staged write.
#[tokio::test]
async fn r2_pre_start_interactive_transaction_aborts_at_commit() {
    let db = setup().await;
    let (mut tx, _snapshot, _repo) = db.tx_begin("testdb", "src", "snapshot").await.unwrap();
    db.tx_execute("testdb", &insert("staged before start"), &mut tx)
        .await
        .unwrap();
    let id = start(&db).await;
    let outcome = db.tx_commit("testdb", "src", tx).await.unwrap();
    assert_eq!(outcome.status, "aborted", "{outcome:?}");
    assert_eq!(outcome.reason.as_deref(), Some("table_migrating"));
    let source = rows(&db, "src").await;
    assert_eq!(source.results["rows"].records.len(), 1);
    assert_eq!(
        source.results["rows"].records[0].get_value_str("body"),
        Some("original"),
    );
    commit(&db, &id).await;
    assert_eq!(
        rows(&db, "dst").await.results["rows"].records,
        source.results["rows"].records,
    );
}

// Red before #1220: an in-place edit is accepted and count verification misses it.
#[tokio::test]
async fn r3_update_is_rejected_without_changing_original_data() {
    let db = setup().await;
    let baseline = rows(&db, "src").await;
    let id = start(&db).await;
    let mut batch = Batch::new();
    batch.update(
        "write",
        Update::with_repo("src", "docs")
            .set(doc! { "body" => "changed" })
            .build()
            .unwrap(),
    );
    assert_frozen(
        &db,
        batch
            .to_request_via_msgpack()
            .expect("batch request encoding"),
    )
    .await;
    assert_eq!(
        rows(&db, "src").await.results["rows"].records,
        baseline.results["rows"].records,
    );
    commit(&db, &id).await;
    assert_eq!(
        rows(&db, "dst").await.results["rows"].records,
        baseline.results["rows"].records,
    );
}

// The rejection control goes red today; the post-rollback write catches a leaked guard.
#[tokio::test]
async fn r4_rollback_releases_source_freeze() {
    let db = setup().await;
    let id = start(&db).await;
    assert_frozen(&db, insert("during migration")).await;
    let mut batch = Batch::new();
    batch.rollback_migration(
        "rollback",
        ddl::rollback_migration(&id)
            .hmac("dummy")
            .build()
            .expect("build RollbackMigration"),
    );
    exec(
        &db,
        batch
            .to_request_via_msgpack()
            .expect("batch request encoding"),
    )
    .await;
    assert!(!db.active_migrations().contains_key(&id));
    exec(&db, insert("after rollback")).await;
    assert_eq!(rows(&db, "src").await.results["rows"].records.len(), 2);
}

// Inject an unresolved descriptor path through the real registry: failure is in
// replication block, AFTER the planned freeze acquisition, not in preflight.
#[tokio::test]
async fn r4_failed_start_releases_source_freeze() {
    let db = setup().await;
    let mut batch = Batch::new();
    batch.create_index(
        "index",
        ddl::create_index("body_fts", "docs")
            .repo("src")
            .field("body")
            .index_type("fts")
            .fts_tokenizer("whitespace"),
    );
    exec(
        &db,
        batch
            .to_request_via_msgpack()
            .expect("batch request encoding"),
    )
    .await;
    let database = db.get_db("testdb").unwrap();
    let source = database.get_table("src", "docs").await.unwrap();
    let registry = source.index2_registry_for_test();
    let original = registry.all_descriptors().await.pop().unwrap();
    let mut broken = original.clone();
    broken.paths[0][0] = u64::MAX;
    let (barrier, lock) = source.begin_write_barrier(INDEX2_CREATE).await.unwrap();
    registry.remove_by_id(original.id).await.unwrap();
    registry
        .insert(build_index2_backend(broken, &source.info_store_for_test()))
        .await
        .unwrap();
    drop(lock);
    drop(barrier);
    let error = db.execute("testdb", &start_request()).await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("cannot resolve interned segment"),
        "{error:?}",
    );
    assert!(db.active_migrations().is_empty());
    assert!(database.get_repo("dst").is_none());
    let (barrier, lock) = source.begin_write_barrier(INDEX2_CREATE).await.unwrap();
    registry.remove_by_id(original.id).await.unwrap();
    registry
        .insert(build_index2_backend(
            original,
            &source.info_store_for_test(),
        ))
        .await
        .unwrap();
    drop(lock);
    drop(barrier);
    exec(&db, insert("after failed start")).await;
    assert_eq!(rows(&db, "src").await.results["rows"].records.len(), 2);
    // A subsequent start must also be admitted (no stranded freeze bit).
    let id = start(&db).await;
    assert_frozen(&db, insert("second migration")).await;
    commit(&db, &id).await;
}

// Stored invalid stage bytes trigger a REAL DbError::Codec after final drain.
// No out-of-scope hook or custom destination factory is necessary.
#[tokio::test]
async fn r5_commit_error_after_final_drain_releases_source_freeze() {
    let db = setup().await;
    let id = start(&db).await;
    assert_frozen(&db, insert("before commit")).await;
    let database = db.get_db("testdb").unwrap();
    let source = database.get_table("src", "docs").await.unwrap();
    source
        .info_store_for_test()
        .set(
            RecordKey::from(format!("__migration_finalise_{id}").into_bytes()),
            Bytes::from_static(&[255]),
        )
        .await
        .unwrap();
    let coord = db.active_migrations().get(&id).unwrap().value().clone();
    let mut batch = Batch::new();
    batch.commit_migration(
        "commit",
        ddl::commit_migration(&id)
            .hmac("dummy")
            .build()
            .expect("build CommitMigration"),
    );
    let error = db
        .execute(
            "testdb",
            &batch
                .to_request_via_msgpack()
                .expect("batch request encoding"),
        )
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("invalid migration finalisation stage"),
        "{error:?}",
    );
    assert_eq!(coord.phase().await, MigrationPhase::Committed);
    assert_eq!(
        shamir_db::engine::migration::MigrationCoordinator::load_state(
            &source.info_store_for_test(),
            &id
        )
        .await
        .unwrap()
        .unwrap()
        .phase,
        MigrationPhase::Committed,
    );
    exec(&db, insert("after commit error")).await;
    let response = rows(&db, "src").await;
    assert_eq!(response.results["rows"].records.len(), 2);
    assert_eq!(
        response.results["rows"].records[0].get_value_str("body"),
        Some("after commit error"),
    );
}

// Tokio's mutex is FIFO: awaiting only the second queued future would hang.
// Poll both cooperatively, second first, while still driving the first waiter.
#[tokio::test]
async fn concurrent_commit_waiters_share_serialized_freeze_and_receipt() {
    let db = setup().await;
    let id = start(&db).await;
    let coord = db.active_migrations().get(&id).unwrap().value().clone();
    let held = coord.lock_finalisation().await;
    let mut batch = Batch::new();
    batch.commit_migration(
        "commit",
        ddl::commit_migration(&id)
            .hmac("dummy")
            .build()
            .expect("build CommitMigration"),
    );
    let request = batch
        .to_request_via_msgpack()
        .expect("batch request encoding");
    let mut first = Box::pin(db.execute("testdb", &request));
    let mut second = Box::pin(db.execute("testdb", &request));
    assert!(futures::poll!(first.as_mut()).is_pending());
    assert!(futures::poll!(second.as_mut()).is_pending());
    drop(held);
    let (second_result, first_result) = futures::join!(second, first);
    for result in [second_result, first_result] {
        let response = result.expect("concurrent Commit must not falsely require restart");
        assert_eq!(
            response.results["commit"].records[0].get_value_str("phase"),
            Some("committed"),
        );
    }
    assert_eq!(coord.phase().await, MigrationPhase::Committed);
    assert_eq!(
        rows(&db, "dst").await.results["rows"].records,
        rows(&db, "src").await.results["rows"].records,
    );
}

// Red with premature transfer: the first waiter steals Some(freeze), then
// cancellation drops it; the second waiter has None and falsely needs restart.
// Removing the first queued future deterministically lets the second win FIFO.
#[tokio::test]
async fn cancelled_commit_waiter_does_not_steal_successors_freeze() {
    let db = setup().await;
    let id = start(&db).await;
    let coord = db.active_migrations().get(&id).unwrap().value().clone();
    let held = coord.lock_finalisation().await;
    let mut batch = Batch::new();
    batch.commit_migration(
        "commit",
        ddl::commit_migration(&id)
            .hmac("dummy")
            .build()
            .expect("build CommitMigration"),
    );
    let request = batch
        .to_request_via_msgpack()
        .expect("batch request encoding");
    let mut first = Box::pin(db.execute("testdb", &request));
    let mut second = Box::pin(db.execute("testdb", &request));
    assert!(futures::poll!(first.as_mut()).is_pending());
    assert!(futures::poll!(second.as_mut()).is_pending());
    drop(first);
    drop(held);
    let response = second
        .await
        .expect("cancelled waiter must leave freeze for successor");
    assert_eq!(
        response.results["commit"].records[0].get_value_str("phase"),
        Some("committed"),
    );
    // The cancelled caller can retry and receive the idempotent receipt.
    commit(&db, &id).await;
    assert_eq!(coord.phase().await, MigrationPhase::Committed);
    exec(&db, insert("after concurrent commit")).await;
    assert_eq!(rows(&db, "src").await.results["rows"].records.len(), 2);
}

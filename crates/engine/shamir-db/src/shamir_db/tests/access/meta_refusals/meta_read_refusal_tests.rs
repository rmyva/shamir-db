use std::time::Duration;

use crate::engine::query::batch::BatchError;
use crate::engine::repo::repo_types::BoxRepoFactory;
use crate::engine::repo::RepoConfig;
use crate::engine::table::TableConfig;
use crate::ShamirDb;
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use shamir_query_builder::batch::Batch;
use shamir_query_builder::ddl;
use shamir_types::access::{Actor, ResourceMeta, ResourcePath};
use shamir_types::types::value::QueryValue;

const DB: &str = "testdb";
const REPO: &str = "main";
const TABLE: &str = "items";
const OWNER: u64 = 7;
const READER: u64 = 8;

async fn setup() -> ShamirDb {
    let db = ShamirDb::init_memory().await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), db.create_db(DB))
        .await
        .unwrap()
        .unwrap();
    let config =
        RepoConfig::new(REPO, BoxRepoFactory::in_memory()).add_table(TableConfig::new(TABLE));
    tokio::time::timeout(Duration::from_secs(10), db.add_repo(DB, config))
        .await
        .unwrap()
        .unwrap();
    for path in [ResourcePath::database(DB), ResourcePath::store(DB, REPO)] {
        tokio::time::timeout(
            Duration::from_secs(10),
            db.set_resource_meta(&path, &ResourceMeta::open()),
        )
        .await
        .unwrap()
        .unwrap();
    }
    db
}

async fn table_acl(db: &ShamirDb, table: &str) {
    db.set_resource_meta(
        &ResourcePath::table(DB, REPO, table),
        &ResourceMeta {
            owner: Actor::User(OWNER),
            group: None,
            mode: 0o700,
        },
    )
    .await
    .unwrap();
}

fn request(
    op: impl shamir_query_builder::batch::IntoBatchOp,
) -> shamir_query_types::batch::BatchRequest {
    let mut batch = Batch::new();
    batch.id("oracle");
    batch.op("read", op);
    batch.build()
}

async fn attempted(
    db: &ShamirDb,
    actor: Actor,
    req: &shamir_query_types::batch::BatchRequest,
) -> (Option<String>, String) {
    let err = tokio::time::timeout(Duration::from_secs(10), db.execute_as(actor, DB, req))
        .await
        .unwrap()
        .unwrap_err();
    match err {
        BatchError::QueryError { code, message, .. } => (code, message),
        other => panic!("expected QueryError, got {other:?}"),
    }
}

fn index_req(table: &str) -> shamir_query_types::batch::BatchRequest {
    request(ddl::list_indexes(table).repo(REPO))
}
fn buffer_req(table: &str) -> shamir_query_types::batch::BatchRequest {
    request(ddl::get_buffer_config(table).repo(REPO))
}
fn validators_req(table: &str) -> shamir_query_types::batch::BatchRequest {
    request(ddl::list_validators(table).db(DB).repo(REPO))
}

fn minimal_validator_wasm_b64() -> String {
    STANDARD.encode(
        wat::parse_str(
            r#"(module
        (memory (export "memory") 2)
        (func (export "shamir_alloc") (param i32) (result i32) i32.const 1024)
        (func (export "shamir_call") (param i32 i32) (result i64) i64.const 0)
    )"#,
        )
        .unwrap(),
    )
}

#[tokio::test]
/// Kills removal of list_indexes' authorize_access, its only per-table gate.
async fn list_indexes_acl_denial_kills_missing_list_authorize() {
    let db = setup().await;
    table_acl(&db, TABLE).await;
    assert_eq!(
        attempted(&db, Actor::User(READER), &index_req(TABLE))
            .await
            .0
            .as_deref(),
        Some("access_denied")
    );
}

#[tokio::test]
/// The guide's unreadable≈missing rule is NOT implemented on these admin endpoints today.
async fn list_indexes_unreadable_existing_and_missing_are_distinguishable() {
    let db = setup().await;
    table_acl(&db, TABLE).await;
    assert_eq!(
        attempted(&db, Actor::User(READER), &index_req(TABLE))
            .await
            .0
            .as_deref(),
        Some("access_denied")
    );
    assert_eq!(
        attempted(&db, Actor::User(READER), &index_req("missing"))
            .await
            .0,
        None
    );
}

#[tokio::test]
async fn list_indexes_owner_sees_seeded_index() {
    let db = setup().await;
    let mut seed = Batch::new();
    seed.id("seed-index");
    seed.op(
        "create",
        ddl::create_index("oracle_idx", TABLE)
            .field("id")
            .repo(REPO),
    );
    let req = seed.to_request_via_msgpack().unwrap();
    tokio::time::timeout(Duration::from_secs(10), db.execute(DB, &req))
        .await
        .unwrap()
        .unwrap();
    table_acl(&db, TABLE).await;
    let response = tokio::time::timeout(
        Duration::from_secs(10),
        db.execute_as(Actor::User(OWNER), DB, &index_req(TABLE)),
    )
    .await
    .unwrap()
    .unwrap();
    let crate::query::read::QueryRecord::Direct(payload) = &response.results["read"].records[0]
    else {
        panic!("expected direct payload")
    };
    let Some(QueryValue::List(indexes)) = payload.get("indexes") else {
        panic!("expected indexes list")
    };
    assert!(indexes
        .iter()
        .any(|entry| entry.get("name") == Some(&QueryValue::Str("oracle_idx".to_owned()))));
}

#[tokio::test]
/// Kills removal of get_buffer_config's authorize_access, its only per-table gate.
async fn get_buffer_config_acl_denial_kills_missing_read_authorize() {
    let db = setup().await;
    table_acl(&db, TABLE).await;
    assert_eq!(
        attempted(&db, Actor::User(READER), &buffer_req(TABLE))
            .await
            .0
            .as_deref(),
        Some("access_denied")
    );
}

#[tokio::test]
/// The guide's unreadable≈missing rule is NOT implemented on these admin endpoints today.
async fn get_buffer_config_unreadable_existing_and_missing_are_distinguishable() {
    let db = setup().await;
    table_acl(&db, TABLE).await;
    assert_eq!(
        attempted(&db, Actor::User(READER), &buffer_req(TABLE))
            .await
            .0
            .as_deref(),
        Some("access_denied")
    );
    assert_eq!(
        attempted(&db, Actor::User(READER), &buffer_req("missing"))
            .await
            .0,
        None
    );
}

#[tokio::test]
async fn get_buffer_config_owner_sees_seeded_config() {
    let db = setup().await;
    let cfg = shamir_query_types::admin::BufferConfigDto {
        max_bytes: 123456,
        max_entries: 321,
        ttl_ms: Some(987),
        flush_interval_ms: 456,
        flush_batch_size: 23,
    };
    let mut seed = Batch::new();
    seed.id("seed-config");
    seed.set_buffer_config("set", ddl::set_buffer_config(TABLE, cfg).repo(REPO));
    let req = seed.to_request_via_msgpack().unwrap();
    tokio::time::timeout(Duration::from_secs(10), db.execute(DB, &req))
        .await
        .unwrap()
        .unwrap();
    table_acl(&db, TABLE).await;
    let response = tokio::time::timeout(
        Duration::from_secs(10),
        db.execute_as(Actor::User(OWNER), DB, &buffer_req(TABLE)),
    )
    .await
    .unwrap()
    .unwrap();
    let crate::query::read::QueryRecord::Direct(payload) = &response.results["read"].records[0]
    else {
        panic!("expected direct payload")
    };
    assert_eq!(
        payload.get("config").and_then(|v| v.get("max_bytes")),
        Some(&QueryValue::Int(123456))
    );
}

#[tokio::test]
/// Kills removal of list_validators' authorize_access, its only per-table gate.
async fn list_validators_acl_denial_kills_missing_read_authorize() {
    let db = setup().await;
    table_acl(&db, TABLE).await;
    assert_eq!(
        attempted(&db, Actor::User(READER), &validators_req(TABLE))
            .await
            .0
            .as_deref(),
        Some("access_denied")
    );
}

#[tokio::test]
/// The guide's unreadable≈missing rule is NOT implemented on these admin endpoints today.
async fn list_validators_unreadable_existing_and_missing_are_distinguishable() {
    let db = setup().await;
    table_acl(&db, TABLE).await;
    assert_eq!(
        attempted(&db, Actor::User(READER), &validators_req(TABLE))
            .await
            .0
            .as_deref(),
        Some("access_denied")
    );
    assert_eq!(
        attempted(&db, Actor::User(READER), &validators_req("missing"))
            .await
            .0,
        None
    );
}

#[tokio::test]
async fn list_validators_owner_sees_seeded_binding() {
    let db = setup().await;
    let create = ddl::create_validator("oracle_validator")
        .wasm(minimal_validator_wasm_b64())
        .build()
        .unwrap();
    let bind = ddl::bind_validator("oracle_validator", TABLE)
        .db(DB)
        .repo(REPO)
        .ops([shamir_query_types::WriteOp::Insert])
        .build();
    let mut seed = Batch::new();
    seed.id("seed-validator");
    seed.op("create", create);
    seed.op("bind", bind);
    let req = seed.to_request_via_msgpack().unwrap();
    tokio::time::timeout(Duration::from_secs(10), db.execute(DB, &req))
        .await
        .unwrap()
        .unwrap();
    table_acl(&db, TABLE).await;
    let response = tokio::time::timeout(
        Duration::from_secs(10),
        db.execute_as(Actor::User(OWNER), DB, &validators_req(TABLE)),
    )
    .await
    .unwrap()
    .unwrap();
    let crate::query::read::QueryRecord::Direct(payload) = &response.results["read"].records[0]
    else {
        panic!("expected direct payload")
    };
    let Some(QueryValue::List(validators)) = payload.get("validators") else {
        panic!("expected validators list")
    };
    assert!(!validators.is_empty());
    assert!(
        matches!(validators[0].get("validator_id"), Some(QueryValue::Str(id)) if !id.is_empty())
    );
    assert_eq!(
        payload.get("table"),
        Some(&QueryValue::Str(TABLE.to_owned()))
    );
}

use shamir_engine::meta::repo_identity::RepoIdentity;
use shamir_engine::repo::SeedReceipt;
#[cfg(all(unix, feature = "fjall"))]
use shamir_engine::repo::{BoxRepoFactory, RepoFactory, RepoInstance, SeedImageWriter, SeedTable};
use shamir_query_builder::doc;
use shamir_query_types::admin::{ReplDirection, ReplMode, ReplScope, ReplStream};
#[cfg(all(unix, feature = "fjall"))]
use shamir_storage::types::RecordKey;
use shamir_types::types::value::QueryValue;

use crate::access::{Actor, ResourceMeta};
use crate::query::write::SetOp;
use crate::query::TableRef;
use crate::ShamirDb;

#[cfg(all(unix, feature = "fjall"))]
use super::super::records_codec::decode_schema_records;

// doc! converts through FilterValue, which cannot carry a list of maps.
fn owner_profile(streams: QueryValue) -> QueryValue {
    let mut fields = shamir_types::types::common::new_map();
    fields.insert("name".into(), QueryValue::Str("owner".into()));
    fields.insert("streams".into(), streams);
    QueryValue::Map(fields)
}

fn receipt() -> SeedReceipt {
    SeedReceipt {
        artifact_digest: "a".repeat(64),
        source_db: "app".into(),
        source_repo: "main".into(),
        source_identity: RepoIdentity {
            lineage_id: [7; 16],
            history_epoch: 1,
        },
        schema_digest: "b".repeat(64),
        source_version: 73,
        local_baseline: 1,
    }
}

#[tokio::test]
async fn reserved_destination_database_is_refused() {
    // Reverting the seed guard to an exact `__system__` comparison lets this through.
    let home = ShamirDb::init_memory().await.unwrap();
    let root = tempfile::tempdir().unwrap();
    let error = home
        .check_seed_target("users", "main", &root.path().join("absent"), &receipt())
        .await
        .unwrap_err();
    assert!(matches!(error, crate::DbError::Validation(_)));
    assert!(error.to_string().contains("reserved"));
}

#[tokio::test]
async fn reserved_receipt_source_database_is_refused() {
    // Reverting the seed guard to an exact `__system__` comparison lets this through.
    let home = ShamirDb::init_memory().await.unwrap();
    let root = tempfile::tempdir().unwrap();
    let mut seed_receipt = receipt();
    seed_receipt.source_db = "users".into();
    let error = home
        .check_seed_target("app", "main", &root.path().join("absent"), &seed_receipt)
        .await
        .unwrap_err();
    assert!(matches!(error, crate::DbError::Validation(_)));
    assert!(error.to_string().contains("reserved"));
}

#[tokio::test]
async fn ordinary_database_without_destination_repo_is_refused_without_schema_changes() {
    let home = ShamirDb::init_memory().await.unwrap();
    let meta = ResourceMeta::owned_enforced(Actor::System);
    let record: QueryValue = doc! { "name" => "app" }.into();
    home.system_store
        .save_database("app", &record, &meta)
        .await
        .unwrap();
    let root = tempfile::tempdir().unwrap();
    let error = home
        .check_seed_target("app", "main", &root.path().join("absent"), &receipt())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("ordinary destination database"));
    assert!(home.system_store.load_tables().await.unwrap().is_empty());
    assert!(home
        .system_store
        .load_repositories()
        .await
        .unwrap()
        .is_empty());
    assert!(!root.path().join("absent").exists());
}

#[cfg(all(unix, feature = "fjall"))]
#[tokio::test]
async fn catalogue_only_retry_requires_matching_image_and_never_overwrites_tables() {
    let home = ShamirDb::init_memory().await.unwrap();
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join(".seed-install"), b"SEED-3").unwrap();
    let backing = BoxRepoFactory::fjall_raw(root.path())
        .create()
        .await
        .unwrap();
    let image = RepoInstance::new("main".into(), backing, Vec::new());
    let writer = SeedImageWriter::begin(
        image.clone(),
        receipt(),
        vec![SeedTable {
            name: "items".into(),
            live_records: 0,
        }],
    )
    .await
    .unwrap();
    let store = home
        .prepare_seed_schema(
            &image,
            "app",
            "main",
            &[("items".into(), QueryValue::Null, 0)],
        )
        .await
        .unwrap();
    writer.finish(std::slice::from_ref(&store)).await.unwrap();
    let bytes = store.get(RecordKey::from_slice(b"schemas")).await.unwrap();
    let records = decode_schema_records(&bytes).unwrap();
    let meta = ResourceMeta::owned_enforced(Actor::System);
    let database: QueryValue = doc! {
        "name" => "app",
        "seed_artifact_digest" => receipt().artifact_digest,
    }
    .into();
    home.system_store
        .save_database("app", &database, &meta)
        .await
        .unwrap();
    home.system_store
        .save_table_meta(&records[0])
        .await
        .unwrap();
    home.register_seed_image(&image, &receipt(), root.path())
        .await
        .unwrap();
    assert_eq!(home.system_store.load_tables().await.unwrap(), records);
    assert!(home
        .system_store
        .load_repository_record("app", "main")
        .await
        .unwrap()
        .is_some());
    let mut conflicting = records[0].clone();
    let QueryValue::Map(fields) = &mut conflicting else {
        panic!("table metadata must be a map");
    };
    fields.insert("schema_version".into(), QueryValue::Int(99));
    home.system_store
        .save_table_meta(&conflicting)
        .await
        .unwrap();
    let error = home
        .register_seed_image(&image, &receipt(), root.path())
        .await
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("conflicting destination catalogue"));
    assert_eq!(
        home.system_store.load_tables().await.unwrap(),
        vec![conflicting]
    );
    image.stop_journal_writer_for_test().await.unwrap();
}

#[tokio::test]
async fn only_destination_pull_subscription_conflicts() {
    let home = ShamirDb::init_memory().await.unwrap();
    let profiles = home
        .system_store
        .replication_profiles_table()
        .await
        .unwrap();
    let subscriptions = home.system_store.subscriptions_table().await.unwrap();
    for (db, direction, conflict) in [
        ("elsewhere", ReplDirection::Pull, false),
        ("app", ReplDirection::Push, false),
        ("app", ReplDirection::Pull, true),
    ] {
        let streams = vec![ReplStream {
            scope: ReplScope {
                db: db.into(),
                repo: Some("main".into()),
                table: None,
            },
            direction,
            mode: ReplMode::ReadOnly,
        }];
        let bytes = rmp_serde::to_vec_named(&streams).unwrap();
        let streams: QueryValue = rmp_serde::from_slice(&bytes).unwrap();
        let profile = SetOp {
            set: TableRef::new("replication_profiles"),
            key: doc! { "name" => "owner" }.into(),
            value: owner_profile(streams),
        };
        home.system_store
            .set_via_implicit_tx(&profiles, &profile)
            .await
            .unwrap();
        let mut subscription_fields = shamir_types::types::common::new_map();
        subscription_fields.insert("name".into(), QueryValue::Str("subscription".into()));
        subscription_fields.insert("profile".into(), QueryValue::Str("owner".into()));
        subscription_fields.insert("state".into(), QueryValue::Str("paused".into()));
        let mut subscription_row = QueryValue::Map(subscription_fields);
        // S5c-X PR8b-2: every subscription row carries a valid durable
        // binding (here over the raw row's scope — the row must parse).
        crate::repl_binding::SubscriptionBinding::new(
            crate::repl_binding::SUBSCRIPTION_BINDING_VERSION,
            1,
            vec![(db.to_string(), "main".to_string())],
        )
        .unwrap()
        .write_to_row(&mut subscription_row);
        let subscription = SetOp {
            set: TableRef::new("subscriptions"),
            key: doc! { "name" => "subscription" }.into(),
            value: subscription_row,
        };
        home.system_store
            .set_via_implicit_tx(&subscriptions, &subscription)
            .await
            .unwrap();
        let result = home.check_seed_subscriptions("app", "main").await;
        if conflict {
            assert!(result
                .unwrap_err()
                .to_string()
                .contains("destination subscription binding"));
        } else {
            result.unwrap();
            assert_eq!(
                home.system_store.load_repositories().await.unwrap().len(),
                0
            );
        }
    }
}

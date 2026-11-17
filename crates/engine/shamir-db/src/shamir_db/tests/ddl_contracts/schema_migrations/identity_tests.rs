use crate::access::ResourceMeta;
use crate::shamir_db::system_store::{DatabaseIdentity, SystemStoreConfig};
use crate::shamir_db::ShamirDb;
use shamir_types::types::value::QueryValue;
use std::str::FromStr;
use tokio::runtime::Builder;

#[test]
fn database_identity_survives_cold_restart_and_rename() {
    let dir = tempfile::tempdir().unwrap();
    let system_path = dir.path().join("system");
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    let identity = runtime.block_on(async {
        let db = ShamirDb::init(SystemStoreConfig::Fjall(system_path.clone()))
            .await
            .unwrap();
        db.create_db("before_rename").await.unwrap();
        let identity = db
            .system_store()
            .schema_migration_identity("before_rename")
            .await
            .unwrap();
        db.rename_db("before_rename", "after_rename").await.unwrap();
        assert_eq!(
            db.system_store()
                .schema_migration_identity("after_rename")
                .await
                .unwrap(),
            identity
        );
        identity
    });
    drop(runtime);

    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    runtime.block_on(async {
        let db = ShamirDb::init(SystemStoreConfig::Fjall(system_path))
            .await
            .unwrap();
        assert_eq!(
            db.system_store()
                .schema_migration_identity("after_rename")
                .await
                .unwrap(),
            identity
        );
    });
    drop(runtime);
}

#[test]
fn dropped_and_recreated_database_gets_a_fresh_identity_and_partition() {
    let dir = tempfile::tempdir().unwrap();
    let system_path = dir.path().join("system");
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    let old_identity = runtime.block_on(async {
        let db = ShamirDb::init(SystemStoreConfig::Fjall(system_path.clone()))
            .await
            .unwrap();
        db.create_db("recycled").await.unwrap();
        let identity = db
            .system_store()
            .schema_migration_identity("recycled")
            .await
            .unwrap();
        assert!(db.remove_db("recycled").await.unwrap());
        db.create_db("recycled").await.unwrap();
        let recreated = db
            .system_store()
            .schema_migration_identity("recycled")
            .await
            .unwrap();
        assert_ne!(identity, recreated);
        (identity, recreated)
    });
    drop(runtime);

    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    runtime.block_on(async {
        let db = ShamirDb::init(SystemStoreConfig::Fjall(system_path))
            .await
            .unwrap();
        let current = db
            .system_store()
            .schema_migration_identity("recycled")
            .await
            .unwrap();
        assert_eq!(current, old_identity.1);
        let history = db
            .system_store()
            .read_schema_migration_history(current)
            .await
            .unwrap();
        assert!(history.events.is_empty());
        assert_eq!(
            old_identity
                .0
                .to_string()
                .parse::<DatabaseIdentity>()
                .unwrap(),
            old_identity.0
        );
    });
    drop(runtime);
}

#[test]
fn legacy_database_identity_backfill_is_durable_and_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let system_path = dir.path().join("system");
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    runtime.block_on(async {
        let db = ShamirDb::init(SystemStoreConfig::Fjall(system_path.clone()))
            .await
            .unwrap();
        db.create_db("legacy").await.unwrap();
        let mut legacy_fields = shamir_types::types::common::new_map();
        legacy_fields.insert("name".into(), QueryValue::Str("legacy".into()));
        legacy_fields.insert("created_at".into(), QueryValue::Int(1));
        // Replace a normal database row with the exact pre-R1c shape. Boot on
        // the next runtime exercises the production catalogue backfill path.
        db.system_store()
            .save_database(
                "legacy",
                &QueryValue::Map(legacy_fields),
                &ResourceMeta::open(),
            )
            .await
            .unwrap();
    });
    drop(runtime);

    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    let first = runtime.block_on(async {
        let db = ShamirDb::init(SystemStoreConfig::Fjall(system_path.clone()))
            .await
            .unwrap();
        db.system_store()
            .schema_migration_identity("legacy")
            .await
            .unwrap()
    });
    drop(runtime);

    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    runtime.block_on(async {
        let db = ShamirDb::init(SystemStoreConfig::Fjall(system_path))
            .await
            .unwrap();
        assert_eq!(
            db.system_store()
                .schema_migration_identity("legacy")
                .await
                .unwrap(),
            first
        );
        let row = db
            .system_store()
            .load_database("legacy")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            row.get("identity").and_then(QueryValue::as_str),
            Some(first.to_string().as_str())
        );
    });
    drop(runtime);
}

#[test]
fn database_identity_display_parse_round_trip_rejects_invalid_values() {
    let identity = DatabaseIdentity::new();
    let encoded = identity.to_string();
    assert_eq!(DatabaseIdentity::from_str(&encoded).unwrap(), identity);
    assert!(DatabaseIdentity::from_str("not-a-record-id").is_err());
}

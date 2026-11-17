use super::helpers::{body, checksum, context};
use crate::shamir_db::system_store::{JournalEventKind, SchemaMigrationStep, SystemStoreConfig};
use crate::shamir_db::ShamirDb;
use tokio::runtime::Builder;

#[tokio::test]
async fn concurrent_fence_compare_and_set_has_exactly_one_winner() {
    let dir = tempfile::tempdir().unwrap();
    let db = ShamirDb::init(SystemStoreConfig::Fjall(dir.path().join("system")))
        .await
        .unwrap();
    db.create_db("alpha").await.unwrap();
    let store = db.system_store().clone();
    let identity = store.schema_migration_identity("alpha").await.unwrap();
    let migration = body("alpha-001", 1);
    let metadata = migration.metadata.clone();
    let shared_context = context();
    let left = store.acquire_schema_migration_fence(
        identity,
        0,
        metadata.clone(),
        migration.clone(),
        false,
        &shared_context,
    );
    let right = store.acquire_schema_migration_fence(
        identity,
        0,
        metadata,
        migration,
        false,
        &shared_context,
    );
    let (left, right) = tokio::join!(left, right);
    assert_ne!(left.is_ok(), right.is_ok());
    assert_eq!(usize::from(left.is_ok()) + usize::from(right.is_ok()), 1);
    let history = store.read_schema_migration_history(identity).await.unwrap();
    assert_eq!(history.events.len(), 1);
    assert_eq!(history.events[0].kind, JournalEventKind::Started);
}

#[test]
fn persistent_fence_recovery_increments_generation_and_preserves_pending_intent() {
    let dir = tempfile::tempdir().unwrap();
    let system_path = dir.path().join("system");
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    let (identity, migration, generation) = runtime.block_on(async {
        let db = ShamirDb::init(SystemStoreConfig::Fjall(system_path.clone()))
            .await
            .unwrap();
        db.create_db("alpha").await.unwrap();
        let identity = db
            .system_store()
            .schema_migration_identity("alpha")
            .await
            .unwrap();
        let migration = body("alpha-001", 1);
        let metadata = migration.metadata.clone();
        let digest = checksum(&migration);
        let generation = db
            .system_store()
            .acquire_schema_migration_fence(
                identity,
                0,
                metadata.clone(),
                migration.clone(),
                false,
                &context(),
            )
            .await
            .unwrap();
        db.system_store()
            .append_schema_migration_event(
                identity,
                generation,
                metadata,
                digest,
                SchemaMigrationStep {
                    kind: JournalEventKind::OperationIntent,
                    operation_index: Some(0),
                    evidence: None,
                },
                &context(),
            )
            .await
            .unwrap();
        (identity, migration, generation)
    });
    drop(runtime);

    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    runtime.block_on(async {
        let db = ShamirDb::init(SystemStoreConfig::Fjall(system_path))
            .await
            .unwrap();
        let before = db
            .system_store()
            .read_schema_migration_history(identity)
            .await
            .unwrap();
        assert_eq!(
            before.events.last().unwrap().kind,
            JournalEventKind::OperationIntent
        );
        assert!(before.in_doubt);
        let generation = db
            .system_store()
            .acquire_schema_migration_fence(
                identity,
                generation,
                migration.metadata.clone(),
                migration.clone(),
                true,
                &context(),
            )
            .await
            .unwrap();
        assert_eq!(generation, 2);
        let recovered = db
            .system_store()
            .read_schema_migration_history(identity)
            .await
            .unwrap();
        assert_eq!(
            recovered.events.last().unwrap().kind,
            JournalEventKind::RecoveryAcquired
        );
        assert!(recovered.in_doubt);
        assert_eq!(recovered.events[1].operation_index, Some(0));
        assert_eq!(recovered.events[1].kind, JournalEventKind::OperationIntent);
    });
    drop(runtime);
}

#[test]
fn stale_fence_cannot_append_or_release_and_in_doubt_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let system_path = dir.path().join("system");
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    let (identity, migration, digest) = runtime.block_on(async {
        let db = ShamirDb::init(SystemStoreConfig::Fjall(system_path.clone()))
            .await
            .unwrap();
        db.create_db("alpha").await.unwrap();
        let identity = db
            .system_store()
            .schema_migration_identity("alpha")
            .await
            .unwrap();
        let migration = body("alpha-001", 1);
        let metadata = migration.metadata.clone();
        let digest = checksum(&migration);
        let generation = db
            .system_store()
            .acquire_schema_migration_fence(
                identity,
                0,
                metadata.clone(),
                migration.clone(),
                false,
                &context(),
            )
            .await
            .unwrap();
        db.system_store()
            .append_schema_migration_event(
                identity,
                generation,
                metadata.clone(),
                digest,
                SchemaMigrationStep {
                    kind: JournalEventKind::OperationIntent,
                    operation_index: Some(0),
                    evidence: None,
                },
                &context(),
            )
            .await
            .unwrap();
        db.system_store()
            .append_schema_migration_event(
                identity,
                generation,
                metadata,
                digest,
                SchemaMigrationStep {
                    kind: JournalEventKind::InDoubt,
                    operation_index: None,
                    evidence: Some("test leaves physical outcome unknown".into()),
                },
                &context(),
            )
            .await
            .unwrap();
        assert!(db
            .system_store()
            .append_schema_migration_event(
                identity,
                generation - 1,
                migration.metadata.clone(),
                digest,
                SchemaMigrationStep {
                    kind: JournalEventKind::OperationComplete,
                    operation_index: Some(0),
                    evidence: Some("stale completion must be refused".into())
                },
                &context()
            )
            .await
            .is_err());
        assert!(db
            .system_store()
            .release_schema_migration_fence(
                identity,
                generation - 1,
                migration.metadata.clone(),
                digest,
                &context(),
            )
            .await
            .is_err());
        (identity, migration, digest)
    });
    drop(runtime);

    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    runtime.block_on(async {
        let db = ShamirDb::init(SystemStoreConfig::Fjall(system_path))
            .await
            .unwrap();
        let history = db
            .system_store()
            .read_schema_migration_history(identity)
            .await
            .unwrap();
        assert!(history.in_doubt);
        assert_eq!(history.events.len(), 3);
        assert_eq!(
            history.events.last().unwrap().kind,
            JournalEventKind::InDoubt
        );
        assert_eq!(history.events[0].checksum, digest);
        assert_eq!(history.events[0].body.as_ref(), Some(&migration));
        assert!(!history
            .events
            .iter()
            .any(|event| event.kind == JournalEventKind::OperationComplete));
    });
    drop(runtime);
}

#[test]
fn durable_tail_failure_does_not_acknowledge_started_migration() {
    let dir = tempfile::tempdir().unwrap();
    let system_path = dir.path().join("system");
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    let (identity, observed) = runtime.block_on(async {
        let db = ShamirDb::init(SystemStoreConfig::Fjall(system_path.clone()))
            .await
            .unwrap();
        db.create_db("alpha").await.unwrap();
        let identity = db
            .system_store()
            .schema_migration_identity("alpha")
            .await
            .unwrap();
        let migration = body("alpha-001", 1);
        // Acquire performs one read-tail flush before its transaction and a
        // second tail flush after commit. Arm the 2nd gate to model the
        // uncertain post-commit durability window, not the preflight flush.
        db.system_store().arm_durability_tail_faults(2);
        let result = db
            .system_store()
            .acquire_schema_migration_fence(
                identity,
                0,
                migration.metadata.clone(),
                migration,
                false,
                &context(),
            )
            .await;
        assert!(
            result.is_err(),
            "failed durable tail must not return a generation"
        );
        let history = db
            .system_store()
            .read_schema_migration_history(identity)
            .await
            .unwrap();
        assert!(history.applied.is_empty());
        assert!(history.in_doubt);
        assert_eq!(
            history.events.last().unwrap().kind,
            JournalEventKind::Started
        );
        (identity, history)
    });
    drop(runtime);

    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    runtime.block_on(async {
        let db = ShamirDb::init(SystemStoreConfig::Fjall(system_path))
            .await
            .unwrap();
        let reopened = db
            .system_store()
            .read_schema_migration_history(identity)
            .await
            .unwrap();
        assert_eq!(reopened, observed);
        assert!(reopened.in_doubt);
        assert!(reopened.applied.is_empty());
    });
    drop(runtime);
}

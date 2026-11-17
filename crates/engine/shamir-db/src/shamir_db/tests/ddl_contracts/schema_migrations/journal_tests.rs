use super::helpers::{body, checksum, context, rows};
use crate::shamir_db::system_store::{JournalEventKind, SchemaMigrationStep, SystemStoreConfig};
use crate::shamir_db::ShamirDb;
use crate::types::value::QueryValue;
use shamir_types::codecs::interned::query_value_to_inner;
use shamir_types::types::record_id::RecordId;
use tokio::runtime::Builder;

#[test]
fn schema_migration_journal_round_trips_after_cold_restart() {
    let dir = tempfile::tempdir().unwrap();
    let system_path = dir.path().join("system");
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    let (identity, expected) = runtime.block_on(async {
        let db = ShamirDb::init(SystemStoreConfig::Fjall(system_path.clone()))
            .await
            .unwrap();
        db.create_db("alpha").await.unwrap();
        db.create_db("beta").await.unwrap();
        let store = db.system_store();
        let identity = store.schema_migration_identity("alpha").await.unwrap();
        let beta = store.schema_migration_identity("beta").await.unwrap();
        let migration = body("alpha-001", 1);
        let metadata = migration.metadata.clone();
        let digest = checksum(&migration);
        let generation = store
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
        assert_eq!(generation, 1);
        store
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
        store
            .append_schema_migration_event(
                identity,
                generation,
                metadata.clone(),
                digest,
                SchemaMigrationStep {
                    kind: JournalEventKind::OperationComplete,
                    operation_index: Some(0),
                    evidence: Some("table exists with the intended definition".into()),
                },
                &context(),
            )
            .await
            .unwrap();
        store
            .append_schema_migration_event(
                identity,
                generation,
                metadata.clone(),
                digest,
                SchemaMigrationStep {
                    kind: JournalEventKind::Applied,
                    operation_index: None,
                    evidence: Some("all schema postconditions verified".into()),
                },
                &context(),
            )
            .await
            .unwrap();
        assert_eq!(
            store
                .release_schema_migration_fence(identity, generation, metadata, digest, &context(),)
                .await
                .unwrap(),
            2
        );
        let expected = store.read_schema_migration_history(identity).await.unwrap();
        assert_eq!(expected.events.len(), 5);
        assert_eq!(expected.applied.len(), 1);
        assert_eq!(expected.events[0].body.as_ref(), Some(&migration));
        assert_eq!(expected.events[0].checksum, digest);
        let other = store.read_schema_migration_history(beta).await.unwrap();
        assert!(other.events.is_empty());
        (identity, expected)
    });
    drop(runtime);

    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    runtime.block_on(async {
        let db = ShamirDb::init(SystemStoreConfig::Fjall(system_path))
            .await
            .unwrap();
        let actual = db
            .system_store()
            .read_schema_migration_history(identity)
            .await
            .unwrap();
        assert_eq!(actual, expected);
        assert_eq!(actual.last_sequence, 5);
        assert_eq!(actual.events[0].kind, JournalEventKind::Started);
        assert_eq!(actual.events[1].operation_index, Some(0));
        assert_eq!(
            actual.events[2].evidence.as_deref(),
            Some("table exists with the intended definition")
        );
        assert_eq!(actual.events[3].kind, JournalEventKind::Applied);
        assert_eq!(actual.events[4].kind, JournalEventKind::FenceReleased);
    });
    drop(runtime);
}

#[tokio::test]
async fn schema_migration_journals_are_partitioned_by_identity_and_recreated_empty() {
    let dir = tempfile::tempdir().unwrap();
    let db = ShamirDb::init(SystemStoreConfig::Fjall(dir.path().join("system")))
        .await
        .unwrap();
    db.create_db("alpha").await.unwrap();
    db.create_db("beta").await.unwrap();
    let store = db.system_store();
    let alpha = store.schema_migration_identity("alpha").await.unwrap();
    let beta = store.schema_migration_identity("beta").await.unwrap();
    let migration = body("alpha-001", 1);
    let metadata = migration.metadata.clone();
    let digest = checksum(&migration);
    let generation = store
        .acquire_schema_migration_fence(alpha, 0, metadata.clone(), migration, false, &context())
        .await
        .unwrap();
    store
        .append_schema_migration_event(
            alpha,
            generation,
            metadata,
            digest,
            SchemaMigrationStep {
                kind: JournalEventKind::InDoubt,
                operation_index: None,
                evidence: Some("substrate-only test leaves operation outcome unresolved".into()),
            },
            &context(),
        )
        .await
        .unwrap();
    assert!(store
        .read_schema_migration_history(beta)
        .await
        .unwrap()
        .events
        .is_empty());
    assert!(
        store
            .read_schema_migration_history(alpha)
            .await
            .unwrap()
            .in_doubt
    );

    assert!(db.remove_db("alpha").await.unwrap());
    db.create_db("alpha").await.unwrap();
    let recreated = store.schema_migration_identity("alpha").await.unwrap();
    assert_ne!(alpha, recreated);
    assert!(store
        .read_schema_migration_history(recreated)
        .await
        .unwrap()
        .events
        .is_empty());
    assert!(
        store
            .read_schema_migration_history(alpha)
            .await
            .unwrap()
            .in_doubt
    );
}

#[tokio::test]
async fn invalid_journal_transitions_leave_history_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    let db = ShamirDb::init(SystemStoreConfig::Fjall(dir.path().join("system")))
        .await
        .unwrap();
    db.create_db("alpha").await.unwrap();
    let store = db.system_store();
    let identity = store.schema_migration_identity("alpha").await.unwrap();
    let migration = body("alpha-001", 1);
    let metadata = migration.metadata.clone();
    let digest = checksum(&migration);

    let gap = body("alpha-002", 2);
    assert!(store
        .acquire_schema_migration_fence(identity, 0, gap.metadata.clone(), gap, false, &context(),)
        .await
        .is_err());
    assert_eq!(
        store
            .read_schema_migration_history(identity)
            .await
            .unwrap()
            .last_sequence,
        0
    );

    let generation = store
        .acquire_schema_migration_fence(identity, 0, metadata.clone(), migration, false, &context())
        .await
        .unwrap();
    let initial = store.read_schema_migration_history(identity).await.unwrap();
    assert_eq!(initial.events.len(), 1);

    assert!(store
        .append_schema_migration_event(
            identity,
            generation,
            metadata.clone(),
            [9; 32],
            SchemaMigrationStep {
                kind: JournalEventKind::OperationIntent,
                operation_index: Some(0),
                evidence: None
            },
            &context()
        )
        .await
        .is_err());
    let mut changed_metadata = metadata.clone();
    changed_metadata.display_name.push_str(" changed");
    assert!(store
        .append_schema_migration_event(
            identity,
            generation,
            changed_metadata,
            digest,
            SchemaMigrationStep {
                kind: JournalEventKind::OperationIntent,
                operation_index: Some(0),
                evidence: None
            },
            &context()
        )
        .await
        .is_err());
    assert!(store
        .append_schema_migration_event(
            identity,
            generation,
            metadata.clone(),
            digest,
            SchemaMigrationStep {
                kind: JournalEventKind::OperationIntent,
                operation_index: Some(1),
                evidence: None
            },
            &context()
        )
        .await
        .is_err());
    assert!(store
        .append_schema_migration_event(
            identity,
            generation,
            metadata.clone(),
            digest,
            SchemaMigrationStep {
                kind: JournalEventKind::Applied,
                operation_index: None,
                evidence: Some("too early".into())
            },
            &context()
        )
        .await
        .is_err());
    assert_eq!(
        store.read_schema_migration_history(identity).await.unwrap(),
        initial
    );

    store
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
    let after_intent = store.read_schema_migration_history(identity).await.unwrap();
    assert!(store
        .append_schema_migration_event(
            identity,
            generation,
            metadata.clone(),
            digest,
            SchemaMigrationStep {
                kind: JournalEventKind::OperationIntent,
                operation_index: Some(0),
                evidence: None
            },
            &context()
        )
        .await
        .is_err());
    assert_eq!(
        store.read_schema_migration_history(identity).await.unwrap(),
        after_intent
    );

    store
        .append_schema_migration_event(
            identity,
            generation,
            metadata.clone(),
            digest,
            SchemaMigrationStep {
                kind: JournalEventKind::OperationComplete,
                operation_index: Some(0),
                evidence: Some("postcondition verified".into()),
            },
            &context(),
        )
        .await
        .unwrap();
    let after_complete = store.read_schema_migration_history(identity).await.unwrap();
    assert!(store
        .append_schema_migration_event(
            identity,
            generation,
            metadata.clone(),
            digest,
            SchemaMigrationStep {
                kind: JournalEventKind::OperationComplete,
                operation_index: Some(0),
                evidence: Some("duplicate transition".into())
            },
            &context()
        )
        .await
        .is_err());
    assert_eq!(
        store.read_schema_migration_history(identity).await.unwrap(),
        after_complete
    );

    store
        .append_schema_migration_event(
            identity,
            generation,
            metadata.clone(),
            digest,
            SchemaMigrationStep {
                kind: JournalEventKind::Applied,
                operation_index: None,
                evidence: Some("all postconditions verified".into()),
            },
            &context(),
        )
        .await
        .unwrap();
    let applied = store.read_schema_migration_history(identity).await.unwrap();
    assert!(store
        .append_schema_migration_event(
            identity,
            generation,
            metadata,
            digest,
            SchemaMigrationStep {
                kind: JournalEventKind::Applied,
                operation_index: None,
                evidence: Some("duplicate applied".into())
            },
            &context()
        )
        .await
        .is_err());
    assert_eq!(
        store.read_schema_migration_history(identity).await.unwrap(),
        applied
    );
}

#[tokio::test]
async fn persisted_corruption_reports_record_context() {
    let dir = tempfile::tempdir().unwrap();
    for corruption in [
        "record_version",
        "body_checksum",
        "sequence_gap",
        "duplicate_ordinal",
    ] {
        let system_path = dir.path().join(corruption);
        let db = ShamirDb::init(SystemStoreConfig::Fjall(system_path))
            .await
            .unwrap();
        db.create_db("alpha").await.unwrap();
        let identity = db
            .system_store()
            .schema_migration_identity("alpha")
            .await
            .unwrap();
        let migration = body("alpha-001", 1);
        db.system_store()
            .acquire_schema_migration_fence(
                identity,
                0,
                migration.metadata.clone(),
                migration,
                false,
                &context(),
            )
            .await
            .unwrap();
        let table = db
            .system_store()
            .system_repo()
            .unwrap()
            .get_table("schema_migration_events")
            .await
            .unwrap();
        let (event_id, mut envelope) = rows(&table)
            .await
            .into_iter()
            .find(|(_, row)| row.get("record_type").and_then(QueryValue::as_str) == Some("event"))
            .unwrap();
        let payload = envelope
            .get("event")
            .and_then(|value| match value {
                QueryValue::Bin(bytes) => Some(bytes.as_slice()),
                _ => None,
            })
            .unwrap();
        let mut event: crate::shamir_db::system_store::JournalEvent =
            rmp_serde::from_slice(payload).unwrap();
        let expected_record_id = if corruption == "duplicate_ordinal" {
            let second_body = body("alpha-002", 1);
            event.sequence = 2;
            event.event_id = RecordId::new().to_string();
            event.migration = second_body.metadata.clone();
            event.checksum = checksum(&second_body);
            event.body = Some(second_body);
            let duplicate_id = RecordId::new();
            let encoded = rmp_serde::to_vec_named(&event).unwrap();
            let mut duplicate = envelope.clone();
            if let QueryValue::Map(fields) = &mut duplicate {
                fields.insert("event_id".into(), QueryValue::Str(event.event_id.clone()));
                fields.insert(
                    "sequence".into(),
                    QueryValue::Str(event.sequence.to_string()),
                );
                fields.insert(
                    "record_key".into(),
                    QueryValue::Str(format!("event/{identity}/{}", event.sequence)),
                );
                fields.insert("event".into(), QueryValue::Bin(encoded));
            }
            let interner = table.interner_for_test().get().await.unwrap();
            let duplicate_inner = query_value_to_inner(&duplicate, interner).unwrap();
            table.set(duplicate_id, &duplicate_inner).await.unwrap();
            duplicate_id
        } else {
            match corruption {
                "record_version" => {
                    if let QueryValue::Map(fields) = &mut envelope {
                        fields.insert("record_version".into(), QueryValue::Str("99".into()));
                    }
                }
                "body_checksum" => event.checksum = [9; 32],
                "sequence_gap" => {
                    event.sequence = 2;
                    if let QueryValue::Map(fields) = &mut envelope {
                        fields.insert("sequence".into(), QueryValue::Str("2".into()));
                        fields.insert(
                            "record_key".into(),
                            QueryValue::Str(format!("event/{identity}/2")),
                        );
                    }
                }
                _ => unreachable!(),
            }
            if corruption != "record_version" {
                if let QueryValue::Map(fields) = &mut envelope {
                    fields.insert(
                        "event".into(),
                        QueryValue::Bin(rmp_serde::to_vec_named(&event).unwrap()),
                    );
                }
            }
            super::helpers::overwrite_row(&table, event_id, envelope).await;
            event_id
        };
        let error = db
            .system_store()
            .read_schema_migration_history(identity)
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains(&expected_record_id.to_string()), "{error}");
        let expected_detail = match corruption {
            "record_version" => "unknown mandatory record version",
            "body_checksum" => "body metadata/checksum mismatch",
            "sequence_gap" => "event sequence gap or duplicate",
            "duplicate_ordinal" => "duplicate migration ordinal",
            _ => unreachable!(),
        };
        assert!(error.contains(expected_detail), "{error}");
        drop(db);
    }
}

#[tokio::test]
async fn memory_system_store_refuses_durable_journal_operations() {
    let db = ShamirDb::init_memory().await.unwrap();
    db.create_db("alpha").await.unwrap();
    let identity = db
        .system_store()
        .schema_migration_identity("alpha")
        .await
        .unwrap();
    assert!(db
        .system_store()
        .read_schema_migration_history(identity)
        .await
        .is_err());
    let migration = body("alpha-001", 1);
    assert!(db
        .system_store()
        .acquire_schema_migration_fence(
            identity,
            0,
            migration.metadata.clone(),
            migration,
            false,
            &context(),
        )
        .await
        .is_err());
}

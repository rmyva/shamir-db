use crate::admin::types::schema_migration::protocol::*;
use crate::admin::types::schema_migration::{
    SchemaMigrationAdminOp, SchemaMigrationBody, SchemaMigrationMetadata,
};
use serde::{Deserialize, Serialize};
use shamir_collections::TFxSet;
use shamir_types::types::record_id::RecordId;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct StrictOperation {
    operation: SchemaMigrationAdminOp,
}

fn body() -> SchemaMigrationBody {
    SchemaMigrationBody {
        metadata: SchemaMigrationMetadata {
            id: "large".to_owned(),
            ordinal: u64::MAX,
            display_name: "large ordinal".to_owned(),
            canonical_version: 1,
        },
        function_dependencies: Vec::new(),
        operations: Vec::new(),
    }
}

#[test]
fn protocol_requests_and_model_round_trip_with_large_ordinals() {
    // Serde round-trip is the wire-format subject; query construction remains typed.
    let request = SchemaMigrationStatusRequest {
        protocol_version: 1,
        target: SchemaMigrationTarget {
            database_identity: RecordId([7; 16]),
        },
    };
    let encoded = serde_json::to_vec(&request).unwrap();
    let decoded: SchemaMigrationStatusRequest = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(decoded, request);

    let predecessor = SchemaMigrationPredecessor {
        migration_id: Some("large".to_owned()),
        ordinal: u64::MAX,
        checksum: [4; 32],
    };
    let encoded = serde_json::to_vec(&predecessor).unwrap();
    let decoded: SchemaMigrationPredecessor = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(decoded, predecessor);

    let migration = body();
    let encoded = serde_json::to_vec(&migration).unwrap();
    let decoded: SchemaMigrationBody = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(decoded, migration);
}

#[test]
fn plan_and_apply_requests_and_results_round_trip() {
    // Serde round-trip is the wire-format subject; query construction remains typed.
    let target = SchemaMigrationTarget {
        database_identity: RecordId([3; 16]),
    };
    let predecessor = SchemaMigrationPredecessor {
        migration_id: None,
        ordinal: 0,
        checksum: [0; 32],
    };
    let migration = body();
    let submission = SchemaMigrationSubmission::new(&migration, [5; 32]);
    let plan_request = SchemaMigrationPlanRequest {
        protocol_version: 1,
        target,
        expected_predecessor: predecessor.clone(),
        migrations: vec![submission.clone()],
    };
    let encoded = serde_json::to_vec(&plan_request).unwrap();
    let decoded: SchemaMigrationPlanRequest = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(decoded, plan_request);

    let apply_request = SchemaMigrationApplyRequest {
        protocol_version: 1,
        target,
        expected_predecessor: predecessor.clone(),
        expected_fence_generation: u64::MAX,
        migration: submission,
    };
    let encoded = serde_json::to_vec(&apply_request).unwrap();
    let decoded: SchemaMigrationApplyRequest = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(decoded, apply_request);

    let plan_result = SchemaMigrationPlanResult {
        protocol_version: 1,
        migration: migration.metadata,
        checksum: [5; 32],
        predecessor: predecessor.clone(),
        operations: vec![SchemaMigrationPlannedOperation {
            operation_index: 0,
            operation_kind: "create_table".to_owned(),
            target: "items".to_owned(),
            work_bound: Some(10),
            dependent_functions: vec!["f".to_owned()],
            affected_database_registrations: vec!["db".to_owned()],
        }],
        total_work_bound: Some(10),
        warnings: vec![SchemaMigrationWarning::MaintenanceFenceRequired],
        recovery_summary: Some("verify postcondition".to_owned()),
        requires_server_wide_fence: Some(true),
    };
    let encoded = serde_json::to_vec(&plan_result).unwrap();
    let decoded: SchemaMigrationPlanResult = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(decoded, plan_result);

    let apply_result = SchemaMigrationApplyResult {
        protocol_version: 1,
        migration_id: "large".to_owned(),
        ordinal: u64::MAX,
        checksum: [5; 32],
        status: "applied".to_owned(),
        applied_at_unix_micros: Some(12),
        fence_generation: Some(u64::MAX),
        error: Some(SchemaMigrationErrorCode::SchemaMigrationInProgress),
    };
    let encoded = serde_json::to_vec(&apply_result).unwrap();
    let decoded: SchemaMigrationApplyResult = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(decoded, apply_result);
}

#[test]
fn requests_reject_unknown_fields_and_results_accept_additive_fields() {
    // Serde round-trip is the wire-format subject; query construction remains typed.
    let request = SchemaMigrationStatusRequest {
        protocol_version: 1,
        target: SchemaMigrationTarget {
            database_identity: RecordId([7; 16]),
        },
    };
    let mut encoded = serde_json::to_value(&request).unwrap();
    encoded["extra"] = serde_json::Value::Bool(true);
    assert!(serde_json::from_value::<SchemaMigrationStatusRequest>(encoded).is_err());

    let status_result = SchemaMigrationStatusResult {
        protocol_version: 1,
        target: request.target,
        last_applied_id: None,
        last_applied_ordinal: 0,
        last_applied_checksum: None,
        checksum_validation: SchemaMigrationChecksumValidation::NotChecked,
        in_flight: Some(SchemaMigrationInFlight {
            migration_id: "in-flight".to_owned(),
            ordinal: 1,
            operation_index: Some(0),
            fence_generation: 2,
            status: "started".to_owned(),
        }),
        error: None,
        fence_generation: 2,
        applied: vec![SchemaMigrationAppliedEntry {
            id: "alpha-001".to_owned(),
            ordinal: 1,
            checksum: [9; 32],
        }],
    };
    let encoded_result = serde_json::to_vec(&status_result).unwrap();
    let mut decoded_result: SchemaMigrationStatusResult =
        serde_json::from_slice(&encoded_result).unwrap();
    assert_eq!(decoded_result, status_result);
    let mut result = serde_json::to_value(&status_result).unwrap();
    result["future_field"] = serde_json::Value::String("ignored".to_owned());
    decoded_result = serde_json::from_value(result.clone()).unwrap();
    assert_eq!(decoded_result, status_result);
    assert!(serde_json::from_value::<SchemaMigrationStatusResult>(result).is_ok());
}

#[test]
fn stable_schema_migration_error_codes_are_all_distinct() {
    let codes = [
        SchemaMigrationErrorCode::SchemaMigrationInProgress,
        SchemaMigrationErrorCode::SchemaMigrationInDoubt,
        SchemaMigrationErrorCode::SchemaMigrationChecksumMismatch,
        SchemaMigrationErrorCode::SchemaMigrationGap,
        SchemaMigrationErrorCode::SchemaMigrationPredecessorMismatch,
        SchemaMigrationErrorCode::SchemaMigrationFollowerReadOnly,
        SchemaMigrationErrorCode::SchemaMigrationDurabilityUnavailable,
        SchemaMigrationErrorCode::SchemaMigrationPostconditionFailed,
        SchemaMigrationErrorCode::SchemaMigrationIdentityMismatch,
    ];
    let encoded: Vec<_> = codes
        .iter()
        .map(|code| serde_json::to_string(code).unwrap())
        .collect();
    let unique: TFxSet<_> = encoded.iter().collect();
    assert_eq!(unique.len(), codes.len());
}

#[test]
fn admin_operations_are_typed_and_not_batchop_variants() {
    let operation = SchemaMigrationAdminOp::ApplicationSchemaStatus(SchemaMigrationStatusRequest {
        protocol_version: 1,
        target: SchemaMigrationTarget {
            database_identity: RecordId([3; 16]),
        },
    });
    let wrapped = StrictOperation { operation };
    let encoded = serde_json::to_vec(&wrapped).unwrap();
    let decoded: StrictOperation = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(decoded, wrapped);
}

#[test]
fn target_rejects_unknown_fields() {
    let mut encoded = serde_json::to_value(SchemaMigrationTarget {
        database_identity: RecordId([7; 16]),
    })
    .unwrap();
    encoded["unexpected"] = serde_json::Value::Bool(true);
    assert!(serde_json::from_value::<SchemaMigrationTarget>(encoded).is_err());
}

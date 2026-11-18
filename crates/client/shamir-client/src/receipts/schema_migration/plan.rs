//! [`Client::plan`] — the read-only server plan for the next ordinal.
//!
//! Plan (design §5.1) never writes a journal record, takes no fence and
//! issues no DDL: it submits the typed `ApplicationSchemaPlan` operation
//! with the journal head reported by a preceding `status` call as the
//! expected predecessor and the WHOLE local set (applied prefix included) as
//! the submission.
//! The server's plan arm validates the submitted set against the durable
//! applied prefix entry by entry (verbatim applied ordinals, no gap, no
//! duplicate, server-recomputed canonical checksums) and rejects an
//! unresolved journal with the stable §5.3 codes, which surface typed. The
//! read-only plan CAN therefore verify the full earlier prefix — but only
//! when a next local migration exists to plan: for a fully covered set this
//! client raises [`MigrationError::NothingToApply`] before sending (the
//! server would answer `schema_migration_gap`).

use shamir_query_builder::application_schema_plan;
use shamir_query_types::admin::types::schema_migration::{
    SchemaMigrationPlanRequest, SchemaMigrationPlanResult, SchemaMigrationPredecessor,
    SchemaMigrationStatusResult, SchemaMigrationTarget,
};
use shamir_query_types::batch::BatchOp;

use crate::Client;

use super::error::MigrationError;
use super::set::ApplicationMigrationSet;
use super::status::{admin_record, execute_admin, query_status_raw, PROTOCOL_VERSION};

const PLAN_ALIAS: &str = "application_schema_plan";

/// The typed `application_schema` batch operation for a plan request.
pub(super) fn plan_op(request: SchemaMigrationPlanRequest) -> BatchOp {
    application_schema_plan(request)
}

impl Client {
    /// The server's plan for the next migration of `set` against `target`
    /// (design §5.2: requires `Manage` on the database). Pure read.
    ///
    /// The expected predecessor is the journal head reported by the
    /// internally issued `status` query (one read-only send); the server
    /// then verifies the full earlier prefix itself and refuses an
    /// unresolved journal. Never writes.
    pub async fn plan(
        &self,
        db: &str,
        target: SchemaMigrationTarget,
        set: &ApplicationMigrationSet,
    ) -> Result<SchemaMigrationPlanResult, MigrationError> {
        let decoded = query_status_raw(self, db, target).await?;
        let raw: &SchemaMigrationStatusResult = &decoded.raw;
        if let Some(code) = raw.error {
            return Err(MigrationError::from_result_code(
                code,
                String::from("the server reported a journal error; no plan is issued"),
            ));
        }
        if raw.in_flight.is_some() {
            // Documented server behavior: plan rejects an unresolved journal.
            return Err(MigrationError::InDoubt {
                message: String::from(
                    "an unresolved migration holds the journal; the server will not issue a plan",
                ),
            });
        }
        // Refused before any send: a set SHORTER than the journal is a
        // beyond-set condition (head_check-equivalent), a fully covered set
        // is a no-op — never a plan submission.
        let Some((ordinal, expected_body, expected_checksum)) =
            set.next_unapplied(raw.last_applied_ordinal)
        else {
            if raw.last_applied_ordinal > set.len() as u64 {
                return Err(MigrationError::JournalBeyondSet {
                    ordinal: raw.last_applied_ordinal,
                });
            }
            return Err(MigrationError::NothingToApply);
        };
        let (bodies, checksums) = (set.bodies(), set.checksums());
        // The plan request carries ALL bodies including the applied prefix:
        // `SchemaMigrationPlanRequest::from_bodies` validates contiguity
        // from ordinal 1, and the server independently verifies the prior
        // prefix entry by entry before planning the next migration.
        let expected_predecessor = checked_predecessor(raw)?;
        let request = SchemaMigrationPlanRequest::from_bodies(
            target,
            expected_predecessor.clone(),
            bodies,
            checksums,
        )?;
        let response = execute_admin(self, db, PLAN_ALIAS, plan_op(request))
            .await
            .map_err(MigrationError::from_read)?;
        let plan: SchemaMigrationPlanResult =
            admin_record(&response, PLAN_ALIAS).map_err(MigrationError::from_read)?;
        validate_plan(
            &plan,
            expected_predecessor,
            ordinal,
            expected_body,
            expected_checksum,
        )?;
        Ok(plan)
    }
}

/// One checked validation of the plan reply against what this client asked
/// for: the exact protocol version, the promised next migration (full
/// metadata), its canonical checksum and the exact predecessor. Any other
/// reply shape fails typed instead of being passed through.
fn validate_plan(
    plan: &SchemaMigrationPlanResult,
    expected_predecessor: SchemaMigrationPredecessor,
    ordinal: u64,
    expected_body: &shamir_query_types::admin::types::schema_migration::SchemaMigrationBody,
    expected_checksum: [u8; 32],
) -> Result<(), MigrationError> {
    if plan.protocol_version == PROTOCOL_VERSION
        && plan.migration == expected_body.metadata
        && plan.checksum == expected_checksum
        && plan.predecessor == expected_predecessor
    {
        Ok(())
    } else {
        Err(MigrationError::ShapeMismatch {
            message: format!(
                "plan reply does not match the promised migration {} (ordinal {ordinal})",
                expected_body.metadata.id
            ),
        })
    }
}

/// The typed predecessor for a plan/apply request, checked: the baseline
/// (`migration_id: None`, ordinal 0, all-zero checksum) for an empty
/// journal, otherwise the reported head with id AND checksum present. A
/// head reported WITHOUT its proof fields is never filled with an all-zero
/// fallback: it fails closed ([`MigrationError::JournalUnverified`]) rather
/// than risking a false predecessor match.
pub(super) fn checked_predecessor(
    raw: &SchemaMigrationStatusResult,
) -> Result<SchemaMigrationPredecessor, MigrationError> {
    if raw.last_applied_ordinal == 0 {
        return Ok(SchemaMigrationPredecessor {
            migration_id: None,
            ordinal: 0,
            checksum: [0; 32],
        });
    }
    match (raw.last_applied_id.clone(), raw.last_applied_checksum) {
        (Some(migration_id), Some(checksum)) => Ok(SchemaMigrationPredecessor {
            migration_id: Some(migration_id),
            ordinal: raw.last_applied_ordinal,
            checksum,
        }),
        _ => Err(MigrationError::JournalUnverified {
            validation: raw.checksum_validation,
        }),
    }
}

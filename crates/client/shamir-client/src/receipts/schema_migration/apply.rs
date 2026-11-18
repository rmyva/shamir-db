//! [`Client::apply_next`] — submit EXACTLY ONE next ordinal, refusing
//! before the apply send whenever the local set and the journal disagree.
//!
//! Preflight (one read-only `status` send, then the apply send only if a
//! path proves safe):
//!
//! * **Fresh** (no in-flight): the expected fence generation is the
//!   top-level durable `fence_generation` the status REPORTED — `0` on an
//!   empty journal. The full applied prefix is verified against the local
//!   set first (drift, a shorter set, a malformed array or an unverified
//!   checksum refuse in the status preflight, before any apply send).
//! * **Resume** (the unresolved in-flight head matches the next local
//!   ordinal's id and ordinal EXACTLY): the identical stored body is
//!   submitted with the fence generation the status REPORTED for the
//!   in-flight event — the server checks its persisted checksum of the
//!   in-flight body before any effect, and uses its own latest durable
//!   generation internally. This client does NOT refuse before evaluating
//!   that matching resume path; a matching in-flight is resolvable, not
//!   automatically in doubt. The full applied prefix is verified the same
//!   way as the fresh path.
//! * **Old server** (R1c step 3a missing on a nonempty journal or an
//!   unresolvable unresolved event): typed
//!   [`MigrationError::UnsupportedServer`] from the status preflight — no
//!   guessed fence, no head-only comparison.
//!
//! Prefix drift, a set shorter than the journal, a malformed applied array,
//! an unverified journal checksum, a server-reported journal error, and a
//! fully covered set ([`MigrationError::NothingToApply`]) all refuse BEFORE
//! the apply send.
//!
//! Lost responses (design §5.3): a timeout, transport failure, closed
//! connection or undecodable apply response returns
//! [`MigrationError::UnknownOutcome`] carrying an opaque
//! [`UnknownApply`](super::resolve::UnknownApply) attempt token — the apply
//! is NEVER resent. Resolution runs through
//! [`Client::resolve_unknown_apply`](crate::Client::resolve_unknown_apply).

use shamir_query_builder::apply_application_schema_migration;
use shamir_query_types::admin::types::schema_migration::{
    SchemaMigrationApplyRequest, SchemaMigrationApplyResult, SchemaMigrationChecksumValidation,
    SchemaMigrationErrorCode, SchemaMigrationStatusResult, SchemaMigrationTarget,
};
use shamir_query_types::batch::BatchOp;

use crate::Client;

use super::error::MigrationError;
use super::plan::checked_predecessor;
use super::resolve::UnknownApply;
use super::set::ApplicationMigrationSet;
use super::status::{admin_record, execute_admin, query_status_raw, verify_prefix, PrefixCoverage};

const APPLY_ALIAS: &str = "apply_application_schema_migration";

impl Client {
    /// Apply exactly the next ordinal of `set` against `target` (design
    /// §5.2: requires `Manage` on the database plus every underlying DDL
    /// grant). Nothing is applied unless the preflight proves the journal
    /// head (or a matching resume path); an uncertain outcome after the
    /// send is [`MigrationError::UnknownOutcome`], never a retry. Returns
    /// the server's typed [`SchemaMigrationApplyResult`].
    ///
    /// Cancellation safety: once the apply batch has been SENT, dropping or
    /// cancelling this future leaves the effect UNKNOWN exactly like a lost
    /// response. Before any further `apply_next` on this migration, run
    /// `status` (or `resolve_unknown_apply` with the attempt token if one
    /// was produced) — never send again on an unproven journal.
    pub async fn apply_next(
        &self,
        db: &str,
        target: SchemaMigrationTarget,
        set: &ApplicationMigrationSet,
    ) -> Result<SchemaMigrationApplyResult, MigrationError> {
        let decoded = query_status_raw(self, db, target).await?;
        let raw = &decoded.raw;
        // 1. Full applied-prefix verification FIRST, for every path: a gap
        //    in the reported sequence, drift, a malformed array, an
        //    unverified head checksum, or a set shorter than the journal
        //    refuses before any classification (a drifted prefix must not
        //    resurface as a missing fence, and a shorter set must not
        //    surface as NothingToApply).
        prefix_check(set, raw)?;
        // 2. Server-reported journal errors BEFORE NothingToApply, even for
        //    an empty set. An `in_doubt` code is part of the REAL server's
        //    matching unresolved status, so it is only resolved against the
        //    in-flight entry below (preserving its reason and operation
        //    index); every other code maps typed immediately.
        if let Some(code) = raw.error {
            if code != SchemaMigrationErrorCode::SchemaMigrationInDoubt {
                return Err(MigrationError::from_result_code(
                    code,
                    String::from("the server reported a journal error; nothing is applied"),
                ));
            }
        }
        // 3. The next local ordinal of the verified set.
        let Some((ordinal, body, checksum)) = set.next_unapplied(raw.last_applied_ordinal) else {
            // Typed refusal BEFORE the apply send: no write, no probe.
            return Err(MigrationError::NothingToApply);
        };
        // 4. Resolve the send path with the in-flight guard: matching
        //    resume, fresh baseline, or typed refusal.
        let expected_fence_generation = match &raw.in_flight {
            Some(flight)
                if flight.migration_id == body.metadata.id && flight.ordinal == ordinal =>
            {
                // Matching resume: the identical stored body with the fence
                // generation the status REPORTED for the in-flight event;
                // the server checks its persisted checksum of the in-flight
                // body before any effect and uses its own latest durable
                // generation internally. The full applied prefix was
                // verified above, so no older-prefix guard is needed.
                // Fail closed on an unknown journal state: the unresolved
                // entry must carry a server event-kind status and a real
                // (nonzero) REPORTED fence generation.
                if flight.fence_generation == 0
                    || !matches!(
                        flight.status.as_str(),
                        "started"
                            | "operation_intent"
                            | "operation_complete"
                            | "in_doubt"
                            | "failed"
                            | "recovery_acquired"
                    )
                {
                    return Err(MigrationError::ShapeMismatch {
                        message: format!(
                            "unresolved entry reports status {:?} / generation {} — not a \
                             resumeable journal state",
                            flight.status, flight.fence_generation
                        ),
                    });
                }
                // The resume expectation is the fence generation the status
                // REPORTED for the in-flight event; the server checks its
                // persisted checksum of the in-flight body before any
                // effect.
                flight.fence_generation
            }
            Some(flight) => {
                // Unmatched or non-resumable unresolved entry: preserve the
                // entry's own status, reason and operation index instead of
                // the generic `in_doubt` code.
                return Err(in_flight_error(
                    flight.status.as_str(),
                    flight.operation_index,
                    decoded.in_doubt_reason.as_deref(),
                ));
            }
            None => {
                if let Some(code) = raw.error {
                    // An `in_doubt` code without an in-flight entry is an
                    // unknown journal state; map it typed, never resume.
                    return Err(MigrationError::from_result_code(
                        code,
                        String::from("the server reported a journal error; nothing is applied"),
                    ));
                }
                if raw.last_applied_ordinal != 0 && raw.fence_generation == 0 {
                    // A nonempty journal MUST report a nonzero durable
                    // generation; 0 there is a contradictory shape.
                    return Err(MigrationError::ShapeMismatch {
                        message: format!(
                            "clean journal head at ordinal {} reports fence generation 0",
                            raw.last_applied_ordinal
                        ),
                    });
                }
                // Fresh start: the top-level durable generation — 0 on an
                // empty journal.
                raw.fence_generation
            }
        };
        let request = SchemaMigrationApplyRequest::from_body(
            target,
            checked_predecessor(raw)?,
            expected_fence_generation,
            body,
            checksum,
        );
        let sent_protocol_version = request.protocol_version;
        // The attempt token is bound before the send: db, target identity,
        // and the exact id/ordinal/checksum of the body in flight.
        let attempt = UnknownApply {
            db: db.to_string(),
            target,
            migration_id: body.metadata.id.clone(),
            ordinal,
            checksum,
        };
        let op: BatchOp = apply_application_schema_migration(request);
        let response = match execute_admin(self, db, APPLY_ALIAS, op).await {
            Ok(response) => response,
            Err(error) => {
                // Any error whose outcome is not a definitive server
                // rejection leaves the effect UNKNOWN; never resend.
                return Err(MigrationError::from_apply_send_error(error, attempt));
            }
        };
        let result: SchemaMigrationApplyResult = admin_record(&response, APPLY_ALIAS)
            .map_err(|error| MigrationError::from_apply_send_error(error, attempt.clone()))?;
        // Identity/protocol echo FIRST: a reply that does not describe the
        // body we sent, or speaks another protocol version, is UNKNOWN —
        // its `error` field (if any) must never hide that uncertainty.
        if result.protocol_version != sent_protocol_version
            || result.migration_id != attempt.migration_id
            || result.ordinal != attempt.ordinal
            || result.checksum != attempt.checksum
        {
            return Err(MigrationError::UnknownOutcome {
                detail: format!(
                    "apply result does not describe the submitted migration \
                     (migration_id {:?}, ordinal {}, protocol_version {})",
                    result.migration_id, result.ordinal, result.protocol_version
                ),
                attempt: Box::new(attempt),
            });
        }
        // A result-record error on the CONFIRMED echo is a stable server
        // state, never an Ok and never uncertainty.
        if let Some(code) = result.error {
            return Err(MigrationError::from_result_code(
                code,
                format!(
                    "apply of {} (ordinal {ordinal}) was recorded with an error state",
                    result.migration_id
                ),
            ));
        }
        // A confirmed echo whose journal status is not `applied` leaves the
        // durable outcome unknown.
        if result.status != "applied" {
            return Err(MigrationError::UnknownOutcome {
                detail: format!("apply result status is {:?}, not applied", result.status),
                attempt: Box::new(attempt),
            });
        }
        Ok(result)
    }
}

/// Fresh/resume-path fail-closed journal checks through the ONE shared
/// prefix verification (remote shape/head/checksum plus local per-ordinal
/// comparison): a typed gap for a broken sequence, drift at any applied
/// ordinal, an unverified checksum or contradictory head/fence shape, and
/// a set no shorter than the journal — all before any apply send.
fn prefix_check(
    set: &ApplicationMigrationSet,
    raw: &SchemaMigrationStatusResult,
) -> Result<(), MigrationError> {
    match verify_prefix(set, raw) {
        PrefixCoverage::Empty | PrefixCoverage::Verified { .. } => Ok(()),
        // A NONEMPTY journal reporting fence generation 0 is a CONTRADICTORY
        // shape (distinct from an unverified checksum): the fresh send would
        // fence against an impossible generation, so it is refused as a
        // shape mismatch rather than a checksum verdict.
        PrefixCoverage::Unverified { .. }
            if raw.last_applied_ordinal != 0 && raw.fence_generation == 0 =>
        {
            Err(MigrationError::ShapeMismatch {
                message: format!(
                    "clean journal head at ordinal {} reports fence generation 0",
                    raw.last_applied_ordinal
                ),
            })
        }
        PrefixCoverage::Gap { expected, actual } => Err(MigrationError::Gap {
            message: format!(
                "the applied prefix reported by the journal has a gap: expected ordinal \
                 {expected} but found {actual}"
            ),
        }),
        PrefixCoverage::Drift {
            ordinal,
            journal_id,
            journal_checksum,
            local_id,
        } => Err(MigrationError::Drift {
            ordinal,
            journal_id,
            journal_checksum: MigrationError::hex_checksum(&journal_checksum),
            local_id,
        }),
        PrefixCoverage::JournalBeyondSet { ordinal } => {
            Err(MigrationError::JournalBeyondSet { ordinal })
        }
        // A VALID checksum verdict with an UNVERIFIED prefix means a
        // contradictory REPORT SHAPE (malformed head tuple, explicitly
        // empty applied list under a nonzero head, or a nonempty head
        // without a prefix) — not an unverifiable checksum, so the
        // diagnostic must not carry `validation: Valid`.
        PrefixCoverage::Unverified { .. }
            if raw.checksum_validation == SchemaMigrationChecksumValidation::Valid =>
        {
            Err(MigrationError::ShapeMismatch {
                message: format!(
                    "status reports checksum_validation valid but the applied prefix at \
                     ordinal {} cannot be verified (malformed head tuple or missing/\
                     inconsistent applied list)",
                    raw.last_applied_ordinal
                ),
            })
        }
        PrefixCoverage::Unverified { .. } => Err(MigrationError::JournalUnverified {
            validation: raw.checksum_validation,
        }),
    }
}

/// The typed refusal for an UNMATCHED unresolved journal head, preserving
/// the in-doubt reason and pending operation index when the server
/// reported them.
pub(super) fn in_flight_error(
    status: &str,
    operation_index: Option<u32>,
    reason: Option<&str>,
) -> MigrationError {
    match status {
        "in_doubt" | "failed" => MigrationError::InDoubt {
            message: match (reason, operation_index) {
                (Some(reason), Some(index)) => {
                    format!("migration is in doubt at operation {index}: {reason}")
                }
                (Some(reason), None) => format!("migration is in doubt: {reason}"),
                (None, Some(index)) => format!("migration is in doubt at operation {index}"),
                (None, None) => String::from("migration is in doubt"),
            },
        },
        _ => MigrationError::InProgress {
            message: format!("migration is unresolved ({status}); it holds the fence"),
        },
    }
}

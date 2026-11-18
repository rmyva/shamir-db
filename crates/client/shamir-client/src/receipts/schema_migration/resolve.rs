//! [`Client::resolve_unknown_apply`] — the §5.3 lost-response procedure:
//! after an uncertain apply, run ONE read-only `status` for the same
//! migration id and map the outcome. The apply itself is never resent.

use shamir_query_types::admin::types::schema_migration::{
    SchemaMigrationErrorCode, SchemaMigrationTarget,
};

use crate::Client;

use super::error::MigrationError;
use super::set::ApplicationMigrationSet;
use super::status::{query_status_raw, verify_prefix, verify_reported_prefix, PrefixCoverage};

/// Opaque attempt token bound BEFORE an apply send: the database, the
/// target identity, and the exact id / ordinal / canonical checksum of the
/// body that was in flight. Produced by `apply_next` inside
/// [`MigrationError::UnknownOutcome`](super::MigrationError::UnknownOutcome)
/// and consumed by [`Client::resolve_unknown_apply`]. Fields are opaque to
/// callers; the accessors expose the bound values for diagnostics only.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnknownApply {
    pub(super) db: String,
    pub(super) target: SchemaMigrationTarget,
    pub(super) migration_id: String,
    pub(super) ordinal: u64,
    pub(super) checksum: [u8; 32],
}

impl UnknownApply {
    /// Database the uncertain apply was sent to.
    pub fn db(&self) -> &str {
        &self.db
    }

    /// Target identity the apply named.
    pub fn target(&self) -> SchemaMigrationTarget {
        self.target
    }

    /// Migration id of the body that was in flight.
    pub fn migration_id(&self) -> &str {
        &self.migration_id
    }

    /// Ordinal of the body that was in flight.
    pub fn ordinal(&self) -> u64 {
        self.ordinal
    }

    /// Canonical checksum of the body that was in flight.
    pub fn checksum(&self) -> [u8; 32] {
        self.checksum
    }
}

/// The typed outcome of reconciling an unknown apply with one status query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnknownApplyResolution {
    /// The journal proves the attempt's migration applied with the SAME id,
    /// ordinal and checksum as an entry ANYWHERE in the valid contiguous
    /// reported prefix (server-verified persisted checksum): report
    /// success, never resend — including when a LATER migration has since
    /// overtaken the head. A later migration's own unresolved state does
    /// not shadow this proof.
    Applied {
        migration_id: String,
        ordinal: u64,
        checksum: [u8; 32],
    },
    /// The attempt's migration IS the unresolved journal head — running
    /// (`started` / `operation_intent`) or in the uncertain window
    /// (`in_doubt` / `failed`). Typed in-doubt verdict with the server's
    /// recorded reason and pending operation index when reported: no auto
    /// replay; reconciliation goes through the matching resume path or
    /// operator evidence.
    InDoubt {
        journal_status: String,
        fence_generation: u64,
        operation_index: Option<u32>,
        reason: Option<String>,
    },
    /// Verified absence: the full local prefix is proven against the
    /// reported journal (verified coverage, valid fence generation on the
    /// wire), with no unresolved event, the head being exactly the
    /// attempt's predecessor, and the attempt's ordinal being the next
    /// local ordinal of the SAME set the body came from — at ANY ordinal.
    /// It is safe to call `apply_next` again (the identical-body resume
    /// re-checks the persisted checksum before any effect). No send beyond
    /// the status query was made.
    ReadyToApply,
    /// The journal moved past the attempt WITHOUT proving it: the head has
    /// overtaken the attempt's ordinal with a DIFFERENT id or checksum, or
    /// the head cannot be proven at all. No false success is reported;
    /// reconcile out of band (status/plan, operator evidence).
    EvidenceUnavailable { journal_ordinal: u64 },
    /// An unresolved event exists for a DIFFERENT migration — the outcome
    /// of THIS attempt cannot be proven from the journal.
    UnmatchedUnresolved { journal_ordinal: u64 },
}

impl Client {
    /// Reconcile an uncertain apply with one read-only `status` query for
    /// the same migration id (design §5.3). Never resends the apply; on a
    /// successful [`UnknownApplyResolution::ReadyToApply`] the caller may
    /// call `apply_next` again with the SAME set — the identical-body
    /// resume path of the server.
    ///
    /// The attempt is first validated against `set` (id, ordinal and
    /// checksum must match exactly), so every verdict below is bound to the
    /// identical body the token was created for.
    pub async fn resolve_unknown_apply(
        &self,
        attempt: &UnknownApply,
        set: &ApplicationMigrationSet,
    ) -> Result<UnknownApplyResolution, MigrationError> {
        match set.at_ordinal(attempt.ordinal) {
            Some((body, checksum))
                if body.metadata.id == attempt.migration_id && checksum == attempt.checksum => {}
            _ => {
                return Err(MigrationError::AttemptDoesNotMatchSet {
                    migration_id: attempt.migration_id.clone(),
                    ordinal: attempt.ordinal,
                });
            }
        }
        let decoded = query_status_raw(self, &attempt.db, attempt.target).await?;
        // Journal content is only trusted when the server answered for the
        // requested identity; a mismatched echo is the typed identity
        // error, never evidence.
        if decoded.raw.target != attempt.target {
            return Err(MigrationError::from_result_code(
                decoded
                    .raw
                    .error
                    .unwrap_or(SchemaMigrationErrorCode::SchemaMigrationIdentityMismatch),
                String::from("the status target does not match the attempt's target identity"),
            ));
        }
        // Classification order: the attempt's OWN unresolved head first,
        // then the exact prefix-entry proof, then overtake/others.
        if let Some(flight) = &decoded.raw.in_flight {
            if flight.migration_id == attempt.migration_id && flight.ordinal == attempt.ordinal {
                return Ok(UnknownApplyResolution::InDoubt {
                    journal_status: flight.status.clone(),
                    fence_generation: flight.fence_generation,
                    operation_index: flight.operation_index,
                    reason: decoded.in_doubt_reason.clone(),
                });
            }
        }
        let raw = &decoded.raw;
        // The attempt's exact entry anywhere in the VALID REPORTED prefix
        // (remote-only shape/contiguity/head/checksum verification — the
        // local set may legitimately end before a later head) proves it
        // applied — even when the head has since overtaken it.
        let attempt_in_prefix = match verify_reported_prefix(raw) {
            PrefixCoverage::Verified { .. } => raw.applied.iter().any(|entry| {
                entry.ordinal == attempt.ordinal
                    && entry.id == attempt.migration_id
                    && entry.checksum == attempt.checksum
            }),
            _ => false,
        };
        // A server-reported in-doubt error from a LATER migration does not
        // shadow a proven applied attempt; any other error maps typed.
        if let Some(code) = raw.error {
            let shadowed =
                code == SchemaMigrationErrorCode::SchemaMigrationInDoubt && attempt_in_prefix;
            if !shadowed {
                return Err(MigrationError::from_result_code(
                    code,
                    String::from(
                        "the server reported a journal error; the attempt cannot be resolved",
                    ),
                ));
            }
        }
        if attempt_in_prefix {
            return Ok(UnknownApplyResolution::Applied {
                migration_id: attempt.migration_id.clone(),
                ordinal: attempt.ordinal,
                checksum: attempt.checksum,
            });
        }
        if raw.in_flight.is_some() {
            // An unresolved event for a DIFFERENT migration: the outcome of
            // THIS attempt cannot be proven from the journal.
            return Ok(UnknownApplyResolution::UnmatchedUnresolved {
                journal_ordinal: raw.last_applied_ordinal,
            });
        }
        // Overtaken (or the same ordinal holds a different body, or the
        // prefix cannot prove the attempt): the outcome is NOT proven — no
        // false success.
        if raw.last_applied_ordinal >= attempt.ordinal {
            return Ok(UnknownApplyResolution::EvidenceUnavailable {
                journal_ordinal: raw.last_applied_ordinal,
            });
        }
        // Verified absence requires ALL of: proven coverage (empty journal
        // or a fully verified prefix against the LOCAL set), no unresolved
        // event, the head being exactly the attempt's predecessor, and the
        // attempt's ordinal being the next local ordinal of this set. A
        // nonempty journal that reports fence generation 0 never counts as
        // ready: the next fresh apply could not fence safely. The fence
        // generation needed by the NEXT apply is the top-level durable
        // generation, guaranteed on the wire here (a legacy server with a
        // nonempty journal was already typed UnsupportedServer before this
        // runs), so readiness is ordinal-independent. Drift, a shorter set,
        // or any head/next mismatch is evidence-unavailable, never
        // ReadyToApply; nothing is resent automatically.
        let coverage = verify_prefix(set, raw);
        let head_proven = matches!(
            coverage,
            PrefixCoverage::Empty | PrefixCoverage::Verified { .. }
        );
        let fence_usable = raw.last_applied_ordinal == 0 || raw.fence_generation != 0;
        let predecessor_matches = raw.last_applied_ordinal == attempt.ordinal.saturating_sub(1);
        let next_is_attempt = set
            .next_unapplied(raw.last_applied_ordinal)
            .map(|(ordinal, _, _)| ordinal)
            == Some(attempt.ordinal);
        if head_proven && fence_usable && predecessor_matches && next_is_attempt {
            return Ok(UnknownApplyResolution::ReadyToApply);
        }
        Ok(UnknownApplyResolution::EvidenceUnavailable {
            journal_ordinal: raw.last_applied_ordinal,
        })
    }
}

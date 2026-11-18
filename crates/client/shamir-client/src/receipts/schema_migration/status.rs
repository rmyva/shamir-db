//! [`Client::status`] — the read-only journal query and its client-side
//! comparison with the local set — plus the shared send/decode helpers used
//! by the plan and apply arms.
//!
//! OBSERVED wire shape (server `schema_migration.rs`, R1c step 3a): the
//! status record encodes `last_applied_id` / `last_applied_ordinal` /
//! `last_applied_checksum`, `checksum_validation`, the top-level durable
//! `fence_generation` (0 when the journal holds no event), the full
//! `applied` prefix (id, ordinal, msgpack-bin checksum per entry, in
//! ordinal order), and — when an unresolved journal event exists —
//! `in_flight` with `migration_id`, `ordinal`, optional `operation_index`,
//! `fence_generation`, `status` and (for in-doubt/failed events)
//! `in_doubt_reason`, plus an optional typed `error` code. On a server
//! older than R1c step 3a the two additive top-level keys are absent: a
//! nonempty journal (or an unresolved event without a usable fence) is
//! then typed [`MigrationError::UnsupportedServer`] — this client never
//! guesses a fence or compares a prefix it cannot see — while a truly
//! empty baseline and an in-flight entry that itself reports the actual
//! fence generation remain usable.

use serde::de::DeserializeOwned;
use shamir_query_builder::application_schema_status;
use shamir_query_builder::batch::Batch;
use shamir_query_types::admin::types::schema_migration::{
    SchemaMigrationAppliedEntry, SchemaMigrationChecksumValidation, SchemaMigrationErrorCode,
    SchemaMigrationStatusRequest, SchemaMigrationStatusResult, SchemaMigrationTarget,
};
use shamir_query_types::batch::{BatchOp, BatchResponse};
use shamir_query_types::read::QueryRecord;

use crate::error::ClientError;
use crate::Client;

use super::error::MigrationError;
use super::set::ApplicationMigrationSet;

pub(super) const STATUS_ALIAS: &str = "application_schema_status";

/// The one supported application-schema protocol version.
pub(super) const PROTOCOL_VERSION: u16 = 1;

impl Client {
    /// Read-only application-schema status for `target`, compared with the
    /// local [`ApplicationMigrationSet`] (design §5.2: requires `Read` on
    /// the database).
    ///
    /// The result reports the applied-prefix coverage explicitly: the
    /// full `applied` prefix is verified entry by entry (contiguous
    /// ordinals, final entry consistent with the reported head tuple, id
    /// AND canonical checksum for every ordinal) before anything counts as
    /// [`PrefixCoverage::Verified`]. A caller that sent a deliberately
    /// wrong identity sees the mismatch reported through
    /// [`MigrationStatus::server_error`] while
    /// [`MigrationStatus::reported_target`] echoes the SERVING identity the
    /// server answered for — that is the supported identity-discovery path.
    pub async fn status(
        &self,
        db: &str,
        target: SchemaMigrationTarget,
        set: &ApplicationMigrationSet,
    ) -> Result<MigrationStatus, MigrationError> {
        let raw = query_status_raw(self, db, target).await?;
        Ok(compare_with_set(set, &raw))
    }
}

/// Send the typed status operation and decode the typed result record.
pub(super) async fn query_status_raw(
    client: &Client,
    db: &str,
    target: SchemaMigrationTarget,
) -> Result<DecodedStatus, MigrationError> {
    let request = SchemaMigrationStatusRequest {
        protocol_version: PROTOCOL_VERSION,
        target,
    };
    let response = execute_admin(client, db, STATUS_ALIAS, application_schema_status(request))
        .await
        .map_err(MigrationError::from_read)?;
    let decoded = decode_status(&response).map_err(MigrationError::from_read)?;
    // Fail closed on a shape this client does not understand: an
    // unexpected protocol version, or a target echo that is neither the
    // requested identity nor a documented mismatch (the mismatch path
    // always carries the typed `error` code, so its serving-identity echo
    // is preserved for identity discovery instead of being refused).
    if decoded.raw.protocol_version != PROTOCOL_VERSION {
        return Err(MigrationError::ShapeMismatch {
            message: format!(
                "status protocol_version {} is not the supported {PROTOCOL_VERSION}",
                decoded.raw.protocol_version
            ),
        });
    }
    if decoded.raw.target != target
        && decoded.raw.error != Some(SchemaMigrationErrorCode::SchemaMigrationIdentityMismatch)
    {
        return Err(MigrationError::ShapeMismatch {
            message: "status target echo matches neither the requested identity nor a \
                      documented identity mismatch"
                .to_string(),
        });
    }
    // Fail closed on a server older than R1c step 3a whenever the applied
    // prefix or the durable fence generation would be needed: a nonempty
    // journal cannot be verified head-only, and its fence generation is
    // never guessed. A truly empty baseline is tolerated, and an unresolved
    // event that itself REPORTS the actual in-flight fence generation stays
    // resumable through the identical-body path.
    if decoded.legacy_wire {
        let resolvable_in_flight = decoded
            .raw
            .in_flight
            .as_ref()
            .is_some_and(|flight| flight.fence_generation != 0);
        if decoded.raw.last_applied_ordinal != 0
            || (decoded.raw.in_flight.is_some() && !resolvable_in_flight)
        {
            return Err(MigrationError::UnsupportedServer {
                message: String::from(
                    "server predates R1c step 3a: the applied prefix and top-level fence \
                     generation are missing from the status wire, too old to verify the \
                     full applied prefix",
                ),
            });
        }
    }
    Ok(decoded)
}

/// A decoded status record plus the additive `in_flight.in_doubt_reason`
/// evidence key the server attaches for in-doubt/failed unresolved events
/// (observed in the handler's `applied_fields`), and the R1c step 3a
/// presence flags for the two additive top-level keys (`fence_generation`,
/// `applied`) — the typed DTO fills their absence with defaults, so the
/// raw payload map is inspected to tell an old server from a genuinely
/// empty/fence-0 one.
pub(super) struct DecodedStatus {
    pub raw: SchemaMigrationStatusResult,
    pub in_doubt_reason: Option<String>,
    /// True when `fence_generation` or `applied` is absent from the wire
    /// record (a server older than R1c step 3a).
    pub legacy_wire: bool,
}

/// The payload map of the status record, when it is a direct map value.
fn status_payload(response: &BatchResponse) -> Option<&shamir_types::types::value::QueryValue> {
    response
        .results
        .get(STATUS_ALIAS)
        .and_then(|result| result.records.first())
        .and_then(|record| match record {
            QueryRecord::Direct(value) => Some(value),
            _ => None,
        })
}

pub(super) fn decode_status(response: &BatchResponse) -> Result<DecodedStatus, ClientError> {
    let raw: SchemaMigrationStatusResult = admin_record(response, STATUS_ALIAS)?;
    let payload = status_payload(response);
    let in_doubt_reason = payload
        .and_then(|value| value.get("in_flight"))
        .and_then(|flight| flight.get("in_doubt_reason"))
        .and_then(|reason| match reason {
            shamir_types::types::value::QueryValue::Str(text) => Some(text.clone()),
            _ => None,
        });
    // The typed DTO defaults a missing `fence_generation` to 0 and skips an
    // empty `applied` vec entirely, so only the RAW payload map tells an
    // R1c-step-3a server from an older one: `fence_generation` absent is
    // always legacy; `applied` absent is legacy only when the head is
    // nonempty (an empty journal deliberately omits it).
    let legacy_wire = payload
        .map(|value| {
            value.get("fence_generation").is_none()
                || (raw.last_applied_ordinal != 0 && value.get("applied").is_none())
        })
        .unwrap_or(true);
    Ok(DecodedStatus {
        raw,
        in_doubt_reason,
        legacy_wire,
    })
}

/// Build and send one typed admin batch operation through the normal
/// `execute` path (interner/epoch handling untouched).
pub(super) async fn execute_admin(
    client: &Client,
    db: &str,
    alias: &str,
    op: BatchOp,
) -> Result<BatchResponse, ClientError> {
    let mut batch = Batch::new();
    batch.op(alias, op);
    client
        .execute(
            db,
            batch
                .try_build()
                .map_err(crate::error::client_batch_error)?,
        )
        .await
}

/// Decode the typed admin payload out of the response's single direct
/// record — the same wire convention as the server's `admin_result`
/// (`QueryRecord::Direct(QueryValue)`) and the receipts crate's
/// `decode_changes_since`: re-encode the buffered `QueryValue` with
/// `rmp_serde::to_vec_named` and decode the typed DTO. MessagePack `bin`
/// payloads (e.g. checksums) decode through the sequence-shaped visitor of
/// fixed-size arrays, exactly as `rmp-serde` 1.3.1 presents bin in
/// `deserialize_tuple`.
pub(super) fn admin_record<T: DeserializeOwned>(
    response: &BatchResponse,
    alias: &str,
) -> Result<T, ClientError> {
    let result = response.results.get(alias).ok_or_else(|| {
        ClientError::Protocol(format!("{alias} result missing from batch response"))
    })?;
    let record = result.records.first().ok_or_else(|| {
        ClientError::Protocol(format!("{alias} result carries no payload record"))
    })?;
    let QueryRecord::Direct(payload) = record else {
        return Err(ClientError::Protocol(format!(
            "{alias} payload is not a direct record"
        )));
    };
    let bytes = rmp_serde::to_vec_named(payload)
        .map_err(|error| ClientError::Protocol(format!("{alias} payload encode: {error}")))?;
    rmp_serde::from_slice(&bytes)
        .map_err(|error| ClientError::Protocol(format!("{alias} payload decode: {error}")))
}

/// Journal-head in-flight diagnostic as reported by the server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InFlightMigration {
    pub migration_id: String,
    pub ordinal: u64,
    /// Index of the pending operation inside the migration, when the
    /// journal recorded one.
    pub operation_index: Option<u32>,
    /// Fence generation the in-flight (or failed) event ran under.
    pub fence_generation: u64,
    /// Journal event kind string (`started`, `operation_intent`,
    /// `in_doubt`, `failed`, ...).
    pub status: String,
    /// Recorded evidence of an in-doubt or failed event, when the server
    /// returned it.
    pub in_doubt_reason: Option<String>,
}

/// Explicit applied-prefix coverage of the local set against the journal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PrefixCoverage {
    /// The journal reports no applied history.
    Empty,
    /// The full applied prefix is PROVEN: contiguous ordinals from 1, the
    /// final entry consistent with the reported head tuple under a
    /// server-verified checksum, and the id AND canonical checksum of the
    /// local set matching the journal at EVERY applied ordinal.
    Verified { ordinal: u64 },
    /// DRIFT: the applied journal entry at the given ordinal differs from
    /// the local body (id or canonical checksum).
    Drift {
        ordinal: u64,
        journal_id: String,
        journal_checksum: [u8; 32],
        local_id: String,
    },
    /// The journal prefix extends beyond the local set: the compiled set is
    /// SHORTER than the applied journal.
    JournalBeyondSet { ordinal: u64 },
    /// The reported applied sequence has a GAP: at some point the entries
    /// jump (e.g. `[1, 3]` — expected ordinal 2, actual 3). The prefix is
    /// not contiguous, so nothing beyond the gap is proven and apply is
    /// refused with a typed gap error.
    Gap { expected: u64, actual: u64 },
    /// Nothing is verified: the server checksum of the head is not `valid`,
    /// a nonempty journal reports no durable fence generation, the applied
    /// array is missing, its final entry is inconsistent with the reported
    /// head tuple, or a nonempty head was reported without a full prefix.
    /// Never treated as verified and never paired with a next ordinal.
    /// A concrete sequence break is the distinct [`PrefixCoverage::Gap`].
    Unverified { ordinal: u64 },
}

/// The REMOTE-ONLY verification of the reported applied prefix — shape,
/// contiguity, head-tuple consistency and server checksum verdict, with NO
/// local set involved. `verify_prefix` layers the local comparison onto
/// its `Empty`/`Verified` outcomes; `resolve_unknown_apply` uses it alone
/// so a valid REPORTED prefix can confirm an attempt even when the local
/// set ends before a later head.
pub(super) fn verify_reported_prefix(raw: &SchemaMigrationStatusResult) -> PrefixCoverage {
    if raw.applied.is_empty() {
        // No full prefix on the wire: only a truly blank journal (ordinal
        // 0 with NO head tuple) is a verified statement; a nonempty head
        // without a prefix proves nothing here (an old server is already
        // typed UnsupportedServer before this runs), and an empty journal
        // that still reports a head id/checksum is contradictory — fail
        // closed either way.
        return if raw.last_applied_ordinal == 0
            && raw.last_applied_id.is_none()
            && raw.last_applied_checksum.is_none()
        {
            PrefixCoverage::Empty
        } else {
            PrefixCoverage::Unverified {
                ordinal: raw.last_applied_ordinal,
            }
        };
    }
    let Some(head) = raw.applied.last() else {
        return PrefixCoverage::Unverified {
            ordinal: raw.last_applied_ordinal,
        };
    };
    // Contiguous order from ordinal 1; any break in the sequence is a
    // concrete GAP (e.g. [1, 3]: expected 2, actual 3), not a vague
    // "unverified".
    for (index, entry) in raw.applied.iter().enumerate() {
        let expected = index as u64 + 1;
        if entry.ordinal != expected {
            return PrefixCoverage::Gap {
                expected,
                actual: entry.ordinal,
            };
        }
    }
    // The server checksum verdict must be `valid` — an invalid or missing
    // verification NEVER counts as verified and never yields a next
    // ordinal.
    if raw.checksum_validation != SchemaMigrationChecksumValidation::Valid {
        return PrefixCoverage::Unverified {
            ordinal: raw.last_applied_ordinal,
        };
    }
    // A NONEMPTY journal must report a real durable fence generation: a 0
    // there is a contradictory shape (missing field on an old server was
    // already typed UnsupportedServer before this runs).
    if raw.last_applied_ordinal != 0 && raw.fence_generation == 0 {
        return PrefixCoverage::Unverified {
            ordinal: raw.last_applied_ordinal,
        };
    }
    // The final entry must be CONSISTENT with the reported head tuple.
    let head_consistent = raw.applied.len() as u64 == raw.last_applied_ordinal
        && raw.last_applied_id.as_deref() == Some(head.id.as_str())
        && raw.last_applied_checksum == Some(head.checksum);
    if !head_consistent {
        return PrefixCoverage::Unverified {
            ordinal: raw.last_applied_ordinal,
        };
    }
    PrefixCoverage::Verified {
        ordinal: raw.last_applied_ordinal,
    }
}

/// The shared verification of the reported applied prefix against the
/// local set — the ONE comparison used by `status` and `apply_next`, so no
/// path can drift from another: the remote-only shape/head/checksum checks
/// first, then the local id AND canonical checksum for every applied
/// ordinal.
pub(super) fn verify_prefix(
    set: &ApplicationMigrationSet,
    raw: &SchemaMigrationStatusResult,
) -> PrefixCoverage {
    let remote = verify_reported_prefix(raw);
    // Only a contiguous, head-consistent, checksum-valid reported prefix
    // can be compared locally; every other outcome is final.
    if !matches!(
        remote,
        PrefixCoverage::Empty | PrefixCoverage::Verified { .. }
    ) {
        return remote;
    }
    // ID AND canonical checksum for every applied ordinal.
    for entry in &raw.applied {
        match set.at_ordinal(entry.ordinal) {
            Some((body, checksum))
                if body.metadata.id == entry.id && checksum == entry.checksum => {}
            Some((body, _)) => {
                return PrefixCoverage::Drift {
                    ordinal: entry.ordinal,
                    journal_id: entry.id.clone(),
                    journal_checksum: entry.checksum,
                    local_id: body.metadata.id.clone(),
                }
            }
            None => {
                return PrefixCoverage::JournalBeyondSet {
                    ordinal: raw.last_applied_ordinal,
                }
            }
        }
    }
    remote
}

/// Client view of one status query, compared against the local set.
#[derive(Debug, Clone, PartialEq)]
pub struct MigrationStatus {
    /// Target identity the server answered for. On
    /// `schema_migration_identity_mismatch` this echoes the SERVING
    /// identity, not the requested one.
    pub reported_target: SchemaMigrationTarget,
    pub last_applied_id: Option<String>,
    pub last_applied_ordinal: u64,
    pub last_applied_checksum: Option<[u8; 32]>,
    pub checksum_validation: SchemaMigrationChecksumValidation,
    pub coverage: PrefixCoverage,
    /// The smallest local ordinal not covered by the reported journal head
    /// — `None` when the journal already covers the whole set, or when
    /// nothing is verified (drift, unverified checksum, malformed prefix,
    /// beyond-set head or a server-reported error).
    pub next_local_ordinal: Option<u64>,
    /// The unresolved journal head, when one exists.
    pub in_flight: Option<InFlightMigration>,
    /// Typed server-side error carried inside the status record
    /// (`schema_migration_in_doubt`, `schema_migration_identity_mismatch`).
    pub server_error: Option<SchemaMigrationErrorCode>,
    /// Every applied migration as reported by the server, in ordinal
    /// order (empty when the journal holds no applied history or the
    /// server predates R1c step 3a).
    pub applied: Vec<SchemaMigrationAppliedEntry>,
    /// Highest durable fence generation of the journal — `0` when empty,
    /// the `expected_fence_generation` of the next fresh apply.
    pub fence_generation: u64,
}

impl MigrationStatus {
    /// True when the server could verify the persisted checksum of the
    /// last applied body (`checksum_validation == valid`) or there is no
    /// applied history to verify — and the server reported no journal
    /// error (an identity mismatch means the reported head does not
    /// describe this journal view, so it never implies verified).
    pub fn journal_checksum_verified(&self) -> bool {
        self.server_error.is_none()
            && (self.last_applied_ordinal == 0
                || self.checksum_validation == SchemaMigrationChecksumValidation::Valid)
    }
}

/// Compare a decoded status record with the local set, through the ONE
/// shared prefix verification. A server-reported error (e.g.
/// `schema_migration_identity_mismatch`) suppresses the local next-ordinal
/// computation: the reported head does not describe this set's journal
/// view. No `Verified` coverage and no next ordinal is ever produced on an
/// unverified checksum or an identity mismatch.
pub(super) fn compare_with_set(
    set: &ApplicationMigrationSet,
    decoded: &DecodedStatus,
) -> MigrationStatus {
    let raw = &decoded.raw;
    let coverage = verify_prefix(set, raw);
    let next_local_ordinal = if raw.error.is_none()
        && matches!(
            coverage,
            PrefixCoverage::Empty | PrefixCoverage::Verified { .. }
        ) {
        set.next_unapplied(raw.last_applied_ordinal)
            .map(|(ordinal, _, _)| ordinal)
    } else {
        // A drifted, unverified, beyond-set or malformed prefix — or a
        // server-reported error — yields no next ordinal: never send
        // against an unverified journal.
        None
    };
    MigrationStatus {
        reported_target: raw.target,
        last_applied_id: raw.last_applied_id.clone(),
        last_applied_ordinal: raw.last_applied_ordinal,
        last_applied_checksum: raw.last_applied_checksum,
        checksum_validation: raw.checksum_validation,
        coverage,
        next_local_ordinal,
        in_flight: raw.in_flight.as_ref().map(|flight| InFlightMigration {
            migration_id: flight.migration_id.clone(),
            ordinal: flight.ordinal,
            operation_index: flight.operation_index,
            fence_generation: flight.fence_generation,
            status: flight.status.clone(),
            // The additive evidence key is read client-side from the payload
            // map; the typed DTO carries no such field.
            in_doubt_reason: decoded.in_doubt_reason.clone(),
        }),
        server_error: raw.error,
        applied: raw.applied.clone(),
        fence_generation: raw.fence_generation,
    }
}

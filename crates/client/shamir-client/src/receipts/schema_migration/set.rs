//! [`ApplicationMigrationSet`] — the client-facing migration set.
//!
//! Wraps the builder's [`SchemaMigrationSet`](shamir_query_builder::SchemaMigrationSet)
//! (no re-implementation of bodies, canonical bytes or checksums) and caches
//! the SHA-256 canonical checksums so every later comparison is allocation
//! free. Construction validates ordering, contiguity and unique ids BEFORE
//! anything can be sent: a malformed set can never reach the wire.

use shamir_query_types::admin::types::schema_migration::SchemaMigrationBody;

use super::error::MigrationError;

/// An ordered, locally validated set of migration bodies compiled into the
/// application. Ordered by ordinal (contiguous from 1), unique ids, typed
/// builder bodies only — no filesystem discovery, no raw wire values.
#[derive(Debug, Clone, PartialEq)]
pub struct ApplicationMigrationSet {
    inner: shamir_query_builder::SchemaMigrationSet,
    checksums: Vec<[u8; 32]>,
}

impl ApplicationMigrationSet {
    /// Validate and build the set, caching the canonical checksums. Both the
    /// structural validation (unique ids, contiguous one-based ordinals) and
    /// the canonical checksum computation happen here — before any send.
    pub fn new(bodies: Vec<SchemaMigrationBody>) -> Result<Self, MigrationError> {
        let inner = shamir_query_builder::SchemaMigrationSet::new(bodies)?;
        let checksums = inner.checksums()?;
        Ok(Self { inner, checksums })
    }

    /// Read the migration bodies without allowing the set to be mutated.
    pub fn bodies(&self) -> &[SchemaMigrationBody] {
        self.inner.bodies()
    }

    /// Cached SHA-256 canonical checksums, in set order (same indexing as
    /// [`Self::bodies`]).
    pub fn checksums(&self) -> &[[u8; 32]] {
        &self.checksums
    }

    /// Number of migrations in the set.
    pub fn len(&self) -> usize {
        self.inner.bodies().len()
    }

    /// True when the set is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.bodies().is_empty()
    }

    /// The body and cached checksum at a one-based ordinal, if the set
    /// covers it.
    pub(super) fn at_ordinal(&self, ordinal: u64) -> Option<(&SchemaMigrationBody, [u8; 32])> {
        let index = usize::try_from(ordinal.checked_sub(1)?).ok()?;
        let body = self.inner.bodies().get(index)?;
        Some((body, self.checksums[index]))
    }

    /// The smallest ordinal NOT yet applied according to `last_applied_ordinal`,
    /// together with its body and checksum — `None` when the journal already
    /// covers the whole set.
    pub(super) fn next_unapplied(
        &self,
        last_applied_ordinal: u64,
    ) -> Option<(u64, &SchemaMigrationBody, [u8; 32])> {
        let ordinal = last_applied_ordinal.checked_add(1)?;
        self.at_ordinal(ordinal)
            .map(|(body, checksum)| (ordinal, body, checksum))
    }
}

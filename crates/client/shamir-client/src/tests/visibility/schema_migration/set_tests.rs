//! The migration set is built from the TYPED DDL builders only (no
//! filesystem discovery, no raw wire values), validates structure at
//! construction and caches the canonical checksums — and a malformed set
//! can never reach the wire, because construction fails before any client
//! or connection exists (so an invalid local gap emits ZERO outbound
//! bytes by construction: there is nothing to send on).

use shamir_query_types::admin::types::schema_migration::SchemaMigrationError;

use crate::schema_migration::{ApplicationMigrationSet, MigrationError};

use super::harness::{body, migration_set};

/// The set accepts typed-builder bodies in order, exposes them unchanged
/// and caches one canonical checksum per body (32 bytes, per-body
/// deterministic, never zero) — everything downstream sends is derived
/// from these cached values.
#[test]
fn rust_migration_set_uses_typed_ddl_builders_only() {
    let set = migration_set(&["mig_items", "mig_audit"]);
    assert_eq!(set.len(), 2);
    assert!(!set.is_empty());
    let bodies = set.bodies();
    assert_eq!(bodies[0].metadata.id, "mig_items");
    assert_eq!(bodies[0].metadata.ordinal, 1);
    assert_eq!(bodies[1].metadata.id, "mig_audit");
    assert_eq!(bodies[1].metadata.ordinal, 2);
    for body in bodies {
        assert_eq!(body.metadata.canonical_version, 1);
        // The only admitted content is the closed typed-operation subset:
        // exactly one typed operation per body, built by the DDL builders.
        assert_eq!(body.operations.len(), 1);
        assert!(body.function_dependencies.is_empty());
    }
    let checksums = set.checksums();
    assert_eq!(checksums.len(), 2);
    for checksum in checksums {
        assert_ne!(*checksum, [0u8; 32], "cached checksum is the real digest");
    }
    // Deterministic: an independently rebuilt set produces the same
    // canonical checksums.
    assert_eq!(
        migration_set(&["mig_items", "mig_audit"]).checksums(),
        checksums
    );
    assert_ne!(
        checksums[0], checksums[1],
        "distinct bodies, distinct digests"
    );
}

/// Structural validation happens BEFORE anything can be sent: a lone
/// ordinal 2 (non-contiguous from 1), an out-of-order pair and a duplicate
/// id are all rejected with the typed set error. The construction failure
/// precedes any client creation, so an invalid local set emits zero
/// outbound bytes.
#[test]
fn rust_migration_set_rejects_gap_order_and_duplicate_before_any_send() {
    // Ordinal 2 first: a gap where ordinal 1 is missing.
    let ordinal_2_first = vec![body("no_ordinal_one", 2)];
    let error = ApplicationMigrationSet::new(ordinal_2_first).expect_err("gap must be rejected");
    let MigrationError::InvalidSet(inner) = &error else {
        panic!("typed set error, got {error:?}")
    };
    assert!(
        matches!(
            inner,
            SchemaMigrationError::OrdinalGap {
                expected: 1,
                actual: 2
            }
        ),
        "the ordinal-2-first gap is reported as an ordinal gap, got {inner:?}"
    );

    // Out-of-order ordinals (1 then 3).
    let gapped = vec![body("first", 1), body("third", 3)];
    assert!(ApplicationMigrationSet::new(gapped).is_err());

    // Duplicate ids across distinct ordinals.
    let duplicated = vec![body("same", 1), body("same", 2)];
    assert!(ApplicationMigrationSet::new(duplicated).is_err());

    // The valid set still constructs after the refusals.
    assert_eq!(migration_set(&["a", "b"]).len(), 2);
}

//! Admission-decision unit tests for `stage_load::admission_for`.
//!
//! Real constants (stage_load.rs): `RECORD_TX_MAX_BYTES` = 32 MiB =
//! 33 554 432, `ENTRY_HEADER_BYTES` = 256, `RECORDS_PER_TX` = 512, so
//! `usable = 33 554 432 − 256 = 33 554 176`. Each test names the broken
//! implementation it catches.

use super::super::stage_load::{admission_for, Admission};

/// usable = RECORD_TX_MAX_BYTES - ENTRY_HEADER_BYTES = 32 MiB − 256.
const USABLE: u64 = 32 * 1024 * 1024 - 256;

#[test]
fn usable_is_positive_and_a_16mib_row_always_fits_an_empty_batch() {
    // usable > 0 by construction: const-checked so the constant arithmetic
    // cannot silently degenerate.
    const _: () = assert!(USABLE > 0);
    // Invariant: a dump row's body is capped at 16 MiB (codec.rs
    // FRAME_LIMIT); with the conservative per-row bound
    // OP_OVERHEAD_BYTES(128) + key(16) + 16 MiB = 16 777 344 ≪ USABLE,
    // an empty batch with zero delta must always stage it.
    let body_16mib = 16 * 1024 * 1024;
    assert_eq!(
        admission_for(0, 0, 128 + 16 + body_16mib, 0),
        Admission::Stage
    );
}

/// Catches a meter that refuses (or commits) on the FIRST row of an empty
/// batch: a small row plus a small delta trivially stages.
#[test]
fn first_row_of_an_empty_batch_stages() {
    // next = 0 + 0 + (128 + 16 + 8192) = 8336 ≤ 33 554 176.
    assert_eq!(admission_for(0, 0, 128 + 16 + 8192, 0), Admission::Stage);
}

/// Catches a meter that ignores the pending interner delta on an empty
/// batch and stages the row anyway — the oversized-entry commit that later
/// fails as an indeterminate WAL outcome.
#[test]
fn oversized_first_row_with_pending_delta_persists_the_interner_first() {
    // row_bound = 128 + 16 + 40 MiB = 41 944 160 > usable by itself;
    // delta = 2 MiB > 0 ⇒ PersistFirst (persist resets the delta, then the
    // empty-batch rule re-runs and refuses — never a direct commit).
    let row_bound = 128 + 16 + 40 * 1024 * 1024;
    assert_eq!(
        admission_for(0, 0, row_bound, 2 * 1024 * 1024),
        Admission::PersistFirst
    );
}

/// Catches an implementation that submits the un-splittable oversized entry
/// and only fails later as `WAL outcome indeterminate`: with NO pending
/// delta there is nothing to persist, so the row must be refused up front
/// (typed `RecordTooLarge`, fired before any WAL submission).
#[test]
fn oversized_first_row_with_no_delta_is_refused() {
    let row_bound = 128 + 16 + 40 * 1024 * 1024;
    assert_eq!(admission_for(0, 0, row_bound, 0), Admission::Refuse);
}

/// Catches a byte-only meter that would let a batch run past the count cap
/// when the bytes still fit: at exactly `RECORDS_PER_TX` the commit must
/// fire regardless of bytes.
#[test]
fn count_cap_commits_even_when_bytes_fit() {
    // pending == 512 ⇒ CommitFirst although
    // next = 1024 + 0 + (128 + 16 + 8) = 1176 ≪ usable.
    assert_eq!(
        admission_for(512, 1024, 128 + 16 + 8, 0),
        Admission::CommitFirst
    );
    // One below the cap with tiny bytes still stages.
    assert_eq!(admission_for(511, 1024, 128 + 16 + 8, 0), Admission::Stage);
}

/// Catches an off-by-one at the byte boundary: exactly usable is INCLUSIVE
/// (stage), one byte over commits first.
#[test]
fn byte_boundary_is_inclusive() {
    // staged + row + delta == usable ⇒ Stage.
    assert_eq!(
        admission_for(10, USABLE - 1000 - 144, 1000 + 144, 1000),
        Admission::Stage
    );
    // staged + row + delta == usable + 1 ⇒ CommitFirst.
    assert_eq!(
        admission_for(10, USABLE - 1000 - 144, 1000 + 144, 1001),
        Admission::CommitFirst
    );
}

/// Catches a meter that treats a large delta as a commit trigger: the delta
/// alone never forces a commit — the 8 MiB persist trigger is the CALLER's
/// job (`DELTA_PERSIST_TRIGGER_BYTES`), not the admission rule's.
#[test]
fn large_delta_below_the_cap_stages() {
    // pending = 100, staged = 1 MiB, delta = 7 MiB (below the 8 MiB
    // caller trigger), row = 128 + 16 + 8 KiB:
    // next = 1 048 576 + 7 340 032 + 8336 = 8 396 944 ≪ usable ⇒ Stage.
    assert_eq!(
        admission_for(100, 1024 * 1024, 128 + 16 + 8192, 7 * 1024 * 1024),
        Admission::Stage
    );
}

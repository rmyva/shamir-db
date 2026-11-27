//! Pass-budget accounting and fresh-copy ownership tests: the pass charges the
//! verifier's real reads (a manifest file once, a WAL segment and a copied
//! stamp three times) and nothing else, at the exact boundary and one byte below it,
//! and the fresh copy is owned before a scan can refuse. The adoption loop's
//! late rereads of a provenance match are charged before its verdict too.

use std::collections::BTreeSet;
use std::fs;
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tracing::{Event, Subscriber};
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::Layer;

use crate::config::BackupRetentionConfig;
use crate::sync::durability::backup::online::manifest::BACKUP_META_FILE_NAME;
use crate::sync::durability::backup::online::progress::CancelFlag;
use crate::sync::durability::backup::{BackupError, HashCancelled, MANIFEST_FILE_NAME};
use crate::sync::format_stamp::STAMP_FILE_NAME;

use super::super::super::retention_pass::manifest_digest_hex;
use super::super::budget::{Budget, Refusal};
use super::super::preflight::classify_verifier_error_for_test;
use super::super::prune::prune_inner;
use super::super::RetentionOutcome;
use super::fixture::{
    copy_with_payload, copy_with_stamp, owner_tag, private_root, retain, sidecar, OWNER, WAL_DIR,
    WAL_FILE,
};

/// Bytes one pass charges for a payload copy: the bounded manifest read, the
/// bounded sidecar read and the payload, which is a WAL segment and therefore
/// read twice by the verifier. Each bounded read reserves one extra byte to
/// detect a size change.
fn true_read_bytes(dir: &Path) -> u64 {
    let manifest = fs::metadata(dir.join(MANIFEST_FILE_NAME))
        .expect("manifest metadata")
        .len();
    let sidecar = fs::metadata(dir.join(BACKUP_META_FILE_NAME))
        .expect("sidecar metadata")
        .len();
    let payload = fs::metadata(dir.join(WAL_FILE))
        .expect("payload metadata")
        .len();
    manifest + 1 + sidecar + 1 + 2 * payload
}

/// [`true_read_bytes`] plus the stamp's three real reads: `format_stamp::inspect`
/// and the verifier's `check_stamp_agreement` both inspect the copied stamp,
/// and the verifier's manifest-file loop hashes it.
fn stamped_read_bytes(dir: &Path) -> u64 {
    true_read_bytes(dir) + 3 * stamp_bytes(dir)
}

fn stamp_bytes(dir: &Path) -> u64 {
    fs::metadata(dir.join(STAMP_FILE_NAME))
        .expect("stamp metadata")
        .len()
}

/// Total true reads of the three-copy tree below, in bytes.
fn three_copy_reads(oldest: &Path, newest: &Path, fresh: &Path) -> u64 {
    true_read_bytes(oldest) + true_read_bytes(newest) + true_read_bytes(fresh)
}

/// Total true reads of the three stamped copies below, in bytes.
fn three_stamped_reads(oldest: &Path, newest: &Path, fresh: &Path) -> u64 {
    stamped_read_bytes(oldest) + stamped_read_bytes(newest) + stamped_read_bytes(fresh)
}

#[test]
fn one_pass_charges_the_verifiers_real_reads() {
    let (_temp, root) = private_root();
    let payload = vec![0u8; 64 << 10];
    let oldest = copy_with_payload(&root, "20200101_000001", 1, Some(OWNER), &payload);
    let newest = copy_with_payload(&root, "20200101_000002", 2, Some(OWNER), &payload);
    let fresh = copy_with_payload(&root, "20200101_000003", 3, None, &payload);
    // EXACTLY the true reads, no slack: one extra sentinel byte per bounded
    // read, one extra file debit, or a duplicate manifest debit refuses the
    // pass, because the total is the only budget that fits.
    let budget_bytes = three_copy_reads(&oldest, &newest, &fresh);
    let cancel = CancelFlag::default();
    let mut budget = Budget::with_bytes(&cancel, budget_bytes);
    let mut outcome = RetentionOutcome::default();
    let refusal = prune_inner(
        &root,
        &fresh,
        OWNER,
        &BackupRetentionConfig { keep_last: 2 },
        &BTreeSet::new(),
        &mut budget,
        &mut outcome,
    );
    assert!(
        refusal.is_ok(),
        "one pass of the verifier's true reads fits a budget that covers exactly them"
    );
    assert_eq!(
        (outcome.deleted, outcome.failed, outcome.untouched),
        (1, 0, Some(0))
    );
    assert!(!oldest.exists());
    assert!(newest.exists());
    assert!(fresh.exists());
    assert_eq!(
        sidecar(&fresh).get("scheduled_retention"),
        Some(&owner_tag(OWNER))
    );
}

/// Mutants killed: a missing sentinel byte on a bounded read, or any missing
/// byte debit, leaves the pass one byte short of the true reads — this budget
/// then fits, the pass succeeds, and the assertion below fails.
#[test]
fn one_byte_short_of_the_true_reads_refuses() {
    let (_temp, root) = private_root();
    let payload = vec![0u8; 64 << 10];
    let oldest = copy_with_payload(&root, "20200101_000001", 1, Some(OWNER), &payload);
    let newest = copy_with_payload(&root, "20200101_000002", 2, Some(OWNER), &payload);
    let fresh = copy_with_payload(&root, "20200101_000003", 3, None, &payload);
    let budget_bytes = three_copy_reads(&oldest, &newest, &fresh) - 1;
    let cancel = CancelFlag::default();
    let mut budget = Budget::with_bytes(&cancel, budget_bytes);
    let mut outcome = RetentionOutcome::default();
    let refusal = prune_inner(
        &root,
        &fresh,
        OWNER,
        &BackupRetentionConfig { keep_last: 2 },
        &BTreeSet::new(),
        &mut budget,
        &mut outcome,
    );
    assert!(
        matches!(refusal, Err(Refusal::Budget)),
        "the true reads are the exact upper bound: one byte less must refuse"
    );
    assert_eq!(
        (outcome.deleted, outcome.failed, outcome.untouched),
        (0, 0, None),
        "the scan ran out of budget and deletes nothing"
    );
    assert!(oldest.exists());
    assert!(newest.exists());
    assert!(fresh.exists());
    assert_eq!(
        sidecar(&fresh).get("scheduled_retention"),
        Some(&owner_tag(OWNER)),
        "the fresh copy is tagged before the scan can run out"
    );
}

#[test]
fn pass_budget_refuses_when_the_second_wal_read_does_not_fit() {
    let (_temp, root) = private_root();
    let payload = vec![0u8; 64 << 10];
    let oldest = copy_with_payload(&root, "20200101_000001", 1, Some(OWNER), &payload);
    let newest = copy_with_payload(&root, "20200101_000002", 2, Some(OWNER), &payload);
    let fresh = copy_with_payload(&root, "20200101_000003", 3, None, &payload);
    // One payload short of the true reads: a pass that charges a WAL segment
    // once (three copies, three missing second reads) would still fit.
    let budget_bytes = three_copy_reads(&oldest, &newest, &fresh) - (64 << 10);
    let cancel = CancelFlag::default();
    let mut budget = Budget::with_bytes(&cancel, budget_bytes);
    let mut outcome = RetentionOutcome::default();
    let refusal = prune_inner(
        &root,
        &fresh,
        OWNER,
        &BackupRetentionConfig { keep_last: 2 },
        &BTreeSet::new(),
        &mut budget,
        &mut outcome,
    );
    assert!(
        matches!(refusal, Err(Refusal::Budget)),
        "the verifier reads a WAL segment twice: the pass must run out of budget"
    );
    assert_eq!(
        (outcome.deleted, outcome.failed),
        (0, 0),
        "a refusing scan deletes nothing"
    );
    assert!(oldest.exists());
    assert!(newest.exists());
}

/// One pass over stamped copies fits a budget of exactly their true reads,
/// stamp reads included.
#[test]
fn one_pass_charges_the_stamps_three_real_reads() {
    let (_temp, root) = private_root();
    let oldest = copy_with_stamp(&root, "20200101_000001", 1, Some(OWNER));
    let newest = copy_with_stamp(&root, "20200101_000002", 2, Some(OWNER));
    let fresh = copy_with_stamp(&root, "20200101_000003", 3, None);
    let budget_bytes = three_stamped_reads(&oldest, &newest, &fresh);
    let cancel = CancelFlag::default();
    let mut budget = Budget::with_bytes(&cancel, budget_bytes);
    let mut outcome = RetentionOutcome::default();
    let refusal = prune_inner(
        &root,
        &fresh,
        OWNER,
        &BackupRetentionConfig { keep_last: 2 },
        &BTreeSet::new(),
        &mut budget,
        &mut outcome,
    );
    assert!(
        refusal.is_ok(),
        "a pass over stamped copies fits exactly its true reads"
    );
    assert_eq!(
        (outcome.deleted, outcome.failed, outcome.untouched),
        (1, 0, Some(0))
    );
    assert!(!oldest.exists());
    assert!(newest.exists());
    assert!(fresh.exists());
}

/// Mutant killed: dropping the stamp debit that `format_stamp::inspect`'s read
/// accounts for undercharges by one stamp per copy, so this budget (one stamp
/// short of the true reads) would fit and the pass would succeed.
#[test]
fn a_pass_that_forgets_the_stamp_read_runs_out_of_budget() {
    let (_temp, root) = private_root();
    let oldest = copy_with_stamp(&root, "20200101_000001", 1, Some(OWNER));
    let newest = copy_with_stamp(&root, "20200101_000002", 2, Some(OWNER));
    let fresh = copy_with_stamp(&root, "20200101_000003", 3, None);
    let budget_bytes = three_stamped_reads(&oldest, &newest, &fresh) - stamp_bytes(&fresh);
    let cancel = CancelFlag::default();
    let mut budget = Budget::with_bytes(&cancel, budget_bytes);
    let mut outcome = RetentionOutcome::default();
    let refusal = prune_inner(
        &root,
        &fresh,
        OWNER,
        &BackupRetentionConfig { keep_last: 2 },
        &BTreeSet::new(),
        &mut budget,
        &mut outcome,
    );
    assert!(
        matches!(refusal, Err(Refusal::Budget)),
        "the stamp's first read is charged: one stamp short must refuse"
    );
    assert_eq!(
        (outcome.deleted, outcome.failed, outcome.untouched),
        (0, 0, None),
        "the refusing scan deletes nothing and publishes no classification"
    );
    assert!(oldest.exists());
    assert!(newest.exists());
    assert!(fresh.exists());
}

#[test]
fn scan_refusal_keeps_the_fresh_copy_owned() {
    let (_temp, root) = private_root();
    let other = copy_with_payload(&root, "20200101_000001", 1, Some(OWNER), &[0u8; 8 << 10]);
    let fresh = copy_with_payload(&root, "20200101_000002", 2, None, &[0u8; 1 << 10]);
    let budget_bytes = true_read_bytes(&fresh) + (4 << 10);
    let cancel = CancelFlag::default();
    let mut budget = Budget::with_bytes(&cancel, budget_bytes);
    let mut outcome = RetentionOutcome::default();
    let refusal = prune_inner(
        &root,
        &fresh,
        OWNER,
        &BackupRetentionConfig { keep_last: 1 },
        &BTreeSet::new(),
        &mut budget,
        &mut outcome,
    );
    assert!(
        matches!(refusal, Err(Refusal::Budget)),
        "the other candidate cannot fit the remaining budget"
    );
    assert_eq!(
        (outcome.deleted, outcome.failed, outcome.untouched),
        (0, 0, None),
        "the refusing scan deletes nothing and publishes no classification; \
         the failure counter belongs to the caller of prune_inner"
    );
    assert!(other.exists());
    assert_eq!(
        sidecar(&fresh).get("scheduled_retention"),
        Some(&owner_tag(OWNER)),
        "the fresh copy is tagged before the scan can refuse"
    );

    // A later scheduled run has a NEW fresh copy: the earlier fresh copy is
    // classified by the scan like any other candidate. The floor counts the
    // new fresh copy as one of the newest, so `keep_last = 2` keeps the earlier
    // fresh copy only when it was tagged (otherwise the older copy is the
    // second newest owned one and nothing is pruned).
    let later = copy_with_payload(&root, "20200101_000003", 3, None, &[0u8; 1 << 10]);
    let next = retain(&root, &later, 2);
    assert_eq!(
        (next.deleted, next.failed, next.untouched),
        (1, 0, Some(0)),
        "the earlier fresh copy is owned and the older copy is pruned"
    );
    assert!(next.adopted.is_empty(), "nothing is adopted");
    assert!(!other.exists());
    assert!(fresh.exists());
    assert!(later.exists());
}

/// Bytes a candidate that the cheap structural reject discards still charges:
/// the bounded sidecar read (`backup.meta.json`, so the scan can classify the
/// copy as owned) and the bounded manifest read (`manifest.json`), each
/// `len() + 1` to detect a size change. Its listed WAL payload is never
/// opened, because the tree check refuses the copy first.
fn rejected_read_bytes(dir: &Path) -> u64 {
    let manifest = fs::metadata(dir.join(MANIFEST_FILE_NAME))
        .expect("manifest metadata")
        .len();
    let sidecar = fs::metadata(dir.join(BACKUP_META_FILE_NAME))
        .expect("sidecar metadata")
        .len();
    manifest + 1 + sidecar + 1
}

/// Total true reads of the three-copy tree below, in bytes: the fresh copy
/// and the verified candidate (`true_read_bytes` each: `manifest.len() + 1`,
/// `sidecar.len() + 1`, `2 * payload.len()`) plus the rejected copy's two
/// bounded reads (`manifest.len() + 1`, `sidecar.len() + 1`). No byte of the
/// rejected copy's listed WAL payload and no byte of its unlisted
/// `store.shamirwal/extra.pem` is in this total.
fn verified_plus_rejected_reads(verified: &Path, fresh: &Path, rejected: &Path) -> u64 {
    true_read_bytes(verified) + true_read_bytes(fresh) + rejected_read_bytes(rejected)
}

/// Mutant (the parent implementation): file bytes are charged before the
/// cheap structural rejects, so the rejected candidate's listed WAL payload is
/// charged twice for a verifier that never opens that copy. This budget has no
/// slack for those bytes, so the parent runs out and returns `Err(Budget)`;
/// the first assertion below then fails.
///
/// The rejected copy is owner-tagged on purpose: an untagged copy is refused
/// at the sidecar check in `prune.rs` BEFORE `verified_time`, so only an owned
/// copy reaches the manifest read and the tree check this test targets.
#[test]
fn a_structurally_rejected_candidate_charges_no_payload_bytes() {
    let (_temp, root) = private_root();
    let payload = vec![0u8; 64 << 10];
    let rejected = copy_with_payload(&root, "20200101_000001", 1, Some(OWNER), &payload);
    // A file the manifest does not list, so the tree check refuses this copy
    // after the manifest read and before any byte charge.
    fs::write(
        rejected.join(WAL_DIR).join("extra.pem"),
        vec![7u8; 128 << 10],
    )
    .expect("unlisted payload");
    let verified = copy_with_payload(&root, "20200101_000002", 2, Some(OWNER), &payload);
    let fresh = copy_with_payload(&root, "20200101_000003", 3, None, &payload);
    let budget_bytes = verified_plus_rejected_reads(&verified, &fresh, &rejected);
    let cancel = CancelFlag::default();
    let mut budget = Budget::with_bytes(&cancel, budget_bytes);
    let mut outcome = RetentionOutcome::default();
    let refusal = prune_inner(
        &root,
        &fresh,
        OWNER,
        &BackupRetentionConfig { keep_last: 1 },
        &BTreeSet::new(),
        &mut budget,
        &mut outcome,
    );
    assert!(
        refusal.is_ok(),
        "the rejected copy's payload is never opened, so it must not be charged"
    );
    assert_eq!(
        (outcome.deleted, outcome.failed, outcome.untouched),
        (1, 0, Some(1)),
        "one verified candidate is pruned; the rejected copy stays untouched"
    );
    assert!(rejected.exists(), "the rejected copy is never deleted");
    assert!(!verified.exists());
    assert!(fresh.exists());
    assert_eq!(
        sidecar(&fresh).get("scheduled_retention"),
        Some(&owner_tag(OWNER))
    );
}

/// The bounded rereads the adoption loop performs for one provenance match:
/// a late `read_sidecar` (`metadata.len() + 1`) and a late `MAX_MANIFEST`-
/// capped `budget.read` of `manifest.json` (`metadata.len() + 1`; the fixture
/// is far below the cap, so only the sentinel byte matters).
fn adoption_reread_bytes(dir: &Path) -> u64 {
    let manifest = fs::metadata(dir.join(MANIFEST_FILE_NAME))
        .expect("manifest metadata")
        .len();
    let sidecar = fs::metadata(dir.join(BACKUP_META_FILE_NAME))
        .expect("sidecar metadata")
        .len();
    manifest + 1 + sidecar + 1
}

/// Exact byte cost of a pass whose only adoptable candidate is `tracked`: the
/// fresh copy's scan reads plus the tracked copy's scan reads
/// (`true_read_bytes` each; the digest hashing of a pending name is not
/// charged) plus the adoption loop's late rereads of `tracked`.
fn adopted_candidate_reads(tracked: &Path, fresh: &Path) -> u64 {
    true_read_bytes(fresh) + true_read_bytes(tracked) + adoption_reread_bytes(tracked)
}

/// SHA-256 (lowercase hex) of a fixture copy's exact manifest bytes, as the
/// registry records it at completion.
fn manifest_digest(dir: &Path) -> String {
    manifest_digest_hex(&fs::read(dir.join(MANIFEST_FILE_NAME)).expect("read manifest"))
}

fn dir_name(dir: &Path) -> String {
    dir.file_name()
        .expect("snapshot dir name")
        .to_string_lossy()
        .into_owned()
}

fn run_pass_with_budget(
    root: &Path,
    fresh: &Path,
    keep_last: usize,
    pending: &BTreeSet<(String, u128, String)>,
    budget_bytes: u64,
) -> (Result<(), Refusal>, RetentionOutcome) {
    let cancel = CancelFlag::default();
    let mut budget = Budget::with_bytes(&cancel, budget_bytes);
    let mut outcome = RetentionOutcome::default();
    let refusal = prune_inner(
        root,
        fresh,
        OWNER,
        &BackupRetentionConfig { keep_last },
        pending,
        &mut budget,
        &mut outcome,
    );
    (refusal, outcome)
}

/// Mutant killed: an adoption that skips the late sidecar or manifest reread
/// undercharges this pass by that bounded read. The budget here is exactly
/// the scan reads plus both rereads, so the boundary test below (one byte
/// less) is what turns that undercharge red.
#[test]
fn an_adoptable_candidate_pays_the_adoption_rereads_exactly() {
    let (_temp, root) = private_root();
    let tracked = copy_with_payload(&root, "20200101_000001", 1, None, b"");
    let fresh = copy_with_payload(&root, "20200101_000002", 2, None, b"");
    let record = (dir_name(&tracked), 1, manifest_digest(&tracked));
    let pending: BTreeSet<_> = [record.clone()].into_iter().collect();
    let budget_bytes = adopted_candidate_reads(&tracked, &fresh);
    let (refusal, outcome) = run_pass_with_budget(&root, &fresh, 2, &pending, budget_bytes);
    assert!(
        refusal.is_ok(),
        "the scan reads plus both adoption rereads fit exactly"
    );
    assert_eq!(outcome.adopted, [record], "the provenance match is adopted");
    assert_eq!(
        (outcome.deleted, outcome.failed, outcome.untouched),
        (0, 0, Some(0))
    );
    assert!(tracked.exists());
    assert_eq!(
        sidecar(&tracked).get("scheduled_retention"),
        Some(&owner_tag(OWNER)),
        "the adopted copy is tagged only after both rereads fit"
    );
    assert_eq!(
        sidecar(&fresh).get("scheduled_retention"),
        Some(&owner_tag(OWNER))
    );
}

/// Mutant killed: dropping either adoption reread leaves a byte free, this
/// budget (one byte below the true reads) fits, the pass succeeds, and the
/// assertion below fails.
#[test]
fn one_byte_short_of_the_adoption_rereads_refuses() {
    let (_temp, root) = private_root();
    let tracked = copy_with_payload(&root, "20200101_000001", 1, None, b"");
    let fresh = copy_with_payload(&root, "20200101_000002", 2, None, b"");
    let record = (dir_name(&tracked), 1, manifest_digest(&tracked));
    let pending: BTreeSet<_> = [record].into_iter().collect();
    let budget_bytes = adopted_candidate_reads(&tracked, &fresh) - 1;
    let (refusal, outcome) = run_pass_with_budget(&root, &fresh, 2, &pending, budget_bytes);
    assert!(
        matches!(refusal, Err(Refusal::Budget)),
        "the adoption rereads are the exact upper bound: one byte less must refuse"
    );
    assert!(
        outcome.adopted.is_empty(),
        "the reread that did not fit happens before the adoption tag"
    );
    assert_eq!(
        (outcome.deleted, outcome.failed, outcome.untouched),
        (0, 0, Some(0))
    );
    assert!(
        sidecar(&tracked).get("scheduled_retention").is_none(),
        "the candidate stays untagged when the reread does not fit"
    );
    assert_eq!(
        sidecar(&fresh).get("scheduled_retention"),
        Some(&owner_tag(OWNER))
    );
}

/// Rewrites one byte into the scanned copy's manifest when the pass reaches
/// the seam between the scan and the adoption loop.
struct MutateManifestBeforeAdoption {
    manifest: PathBuf,
    changed: Arc<AtomicBool>,
}

impl<S: Subscriber> Layer<S> for MutateManifestBeforeAdoption {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        if event.metadata().target() == "shamir_server::retention_before_adoption" {
            fs::OpenOptions::new()
                .append(true)
                .open(&self.manifest)
                .expect("open scanned manifest")
                .write_all(b"\n")
                .expect("mutate manifest after scan");
            self.changed.store(true, Ordering::SeqCst);
        }
    }
}

/// Mutant killed: charging the adoption rereads after the digest comparison,
/// or skipping them for a candidate that will be rejected, undercharges this
/// pass. The manifest is mutated after the scan, so the late digest never
/// matches, yet both rereads are charged before the rejection: the exact
/// budget below (the extra byte is the appended newline the late read
/// charges) accepts the pass with the candidate still untouched.
#[test]
fn adoption_rereads_are_charged_before_a_late_digest_rejection() {
    let (_temp, root) = private_root();
    let tracked = copy_with_payload(&root, "20200101_000001", 1, None, b"");
    let fresh = copy_with_payload(&root, "20200101_000002", 2, None, b"");
    let record = (dir_name(&tracked), 1, manifest_digest(&tracked));
    let pending: BTreeSet<_> = [record].into_iter().collect();
    // The seam appends exactly one byte, so the late manifest reread charges
    // `len + 2`.
    let budget_bytes = adopted_candidate_reads(&tracked, &fresh) + 1;
    let changed = Arc::new(AtomicBool::new(false));
    let subscriber = tracing_subscriber::registry().with(MutateManifestBeforeAdoption {
        manifest: tracked.join(MANIFEST_FILE_NAME),
        changed: Arc::clone(&changed),
    });
    let (refusal, outcome) = tracing::subscriber::with_default(subscriber, || {
        run_pass_with_budget(&root, &fresh, 2, &pending, budget_bytes)
    });
    assert!(changed.load(Ordering::SeqCst), "the after-scan seam fired");
    assert!(
        refusal.is_ok(),
        "both rereads before the late rejection fit exactly"
    );
    assert!(
        outcome.adopted.is_empty(),
        "a late digest mismatch is never adopted"
    );
    assert_eq!(
        (outcome.deleted, outcome.failed, outcome.untouched),
        (0, 0, Some(1)),
        "the reread-and-rejected candidate is counted untouched"
    );
    assert!(
        sidecar(&tracked).get("scheduled_retention").is_none(),
        "the rejected candidate is never tagged"
    );
    assert!(tracked.exists());
    assert_eq!(
        sidecar(&fresh).get("scheduled_retention"),
        Some(&owner_tag(OWNER))
    );
}

/// Budget relation, exact: the appended byte makes the late manifest reread
/// charge ONE byte more than `adopted_candidate_reads` records, so the true
/// mutated cost is `adopted_candidate_reads + 1` and THIS budget is that cost
/// minus one — numerically the unmutated exact cost, and exactly one byte
/// below what the pass really charges with the mutation in place. The
/// companion test above passes at `adopted_candidate_reads + 1`, so the pair
/// pins the charged byte exactly.
///
/// Mutant killed: a mutant that skips the rereads before rejecting charges
/// only the unmutated scan reads and would SUCCEED on this budget; the
/// rereads are charged first, so the late read runs out of budget and the
/// pass refuses. The test asserts the after-scan mutation actually fired —
/// without that assertion the budget verdict could come from an unmutated
/// tree and the test would pass with the mutation machinery broken.
#[test]
fn one_byte_short_of_the_rereads_refuses_before_the_late_rejection() {
    let (_temp, root) = private_root();
    let tracked = copy_with_payload(&root, "20200101_000001", 1, None, b"");
    let fresh = copy_with_payload(&root, "20200101_000002", 2, None, b"");
    let record = (dir_name(&tracked), 1, manifest_digest(&tracked));
    let pending: BTreeSet<_> = [record].into_iter().collect();
    // The mutated cost is `adopted_candidate_reads + 1` (the appended byte the
    // late manifest reread charges); the budget is that cost minus one —
    // numerically the unmutated exact cost, one byte below the real charge.
    let budget_bytes = adopted_candidate_reads(&tracked, &fresh) + 1 - 1;
    let changed = Arc::new(AtomicBool::new(false));
    let subscriber = tracing_subscriber::registry().with(MutateManifestBeforeAdoption {
        manifest: tracked.join(MANIFEST_FILE_NAME),
        changed: Arc::clone(&changed),
    });
    let (refusal, outcome) = tracing::subscriber::with_default(subscriber, || {
        run_pass_with_budget(&root, &fresh, 2, &pending, budget_bytes)
    });
    assert!(
        changed.load(Ordering::SeqCst),
        "the after-scan seam must fire, or the budget verdict proves nothing"
    );
    assert!(
        matches!(refusal, Err(Refusal::Budget)),
        "the rereads precede the late rejection: one byte less must refuse"
    );
    assert!(
        outcome.adopted.is_empty(),
        "nothing is adopted when the reread runs out of budget"
    );
    assert!(
        sidecar(&tracked).get("scheduled_retention").is_none(),
        "no tag without the late rereads"
    );
    assert_eq!(
        sidecar(&fresh).get("scheduled_retention"),
        Some(&owner_tag(OWNER))
    );
}

#[test]
fn pre_cancelled_pass_returns_cancelled_without_retention_mutations() {
    let (_temp, root) = private_root();
    let older = copy_with_payload(&root, "20200101_000001", 1, Some(OWNER), b"");
    let fresh = copy_with_payload(&root, "20200101_000002", 2, None, b"");
    let cancel = CancelFlag::default();
    cancel.cancel();
    let mut budget = Budget::with_bytes(&cancel, u64::MAX);
    let mut outcome = RetentionOutcome::default();

    let refusal = prune_inner(
        &root,
        &fresh,
        OWNER,
        &BackupRetentionConfig { keep_last: 1 },
        &BTreeSet::new(),
        &mut budget,
        &mut outcome,
    );

    assert!(matches!(refusal, Err(Refusal::Cancelled)));
    assert!(outcome.adopted.is_empty());
    assert_eq!(
        (outcome.deleted, outcome.failed, outcome.untouched),
        (0, 0, None)
    );
    assert!(older.exists());
    assert!(fresh.exists());
    assert!(
        sidecar(&fresh).get("scheduled_retention").is_none(),
        "the initial cancellation checkpoint precedes fresh-copy tagging"
    );
}

/// Verifier-error classification semantics: the typed cancel-hook marker maps
/// to `Cancelled`, plain EINTR and other I/O errors remain `Incomplete`, and
/// non-I/O errors are `Unverifiable`. This classifier-only test pins the
/// mapping itself; the preservation of the marker THROUGH the v2 verifier's
/// hash path is pinned end to end by `retention_cancel_tests` in shamir-sync.
#[test]
fn only_the_cancel_marker_classifies_as_cancelled() {
    let cancelled = BackupError::Io(std::io::Error::new(ErrorKind::Interrupted, HashCancelled));
    assert!(matches!(
        classify_verifier_error_for_test(cancelled),
        Refusal::Cancelled
    ));
    let interrupted = BackupError::Io(std::io::Error::new(
        ErrorKind::Interrupted,
        "read interrupted",
    ));
    assert!(
        matches!(
            classify_verifier_error_for_test(interrupted),
            Refusal::Incomplete
        ),
        "a plain EINTR is an incomplete read, not a cancellation"
    );
    let denied = BackupError::Io(std::io::Error::new(ErrorKind::PermissionDenied, "x"));
    assert!(matches!(
        classify_verifier_error_for_test(denied),
        Refusal::Incomplete
    ));
    let invalid = BackupError::ManifestInvalid("x".to_owned());
    assert!(matches!(
        classify_verifier_error_for_test(invalid),
        Refusal::Unverifiable
    ));
}

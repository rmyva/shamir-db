//! Regression for the restore staged presence re-check
//! (`restore/staged_listing.rs`): a manifest-listed file that disappears
//! from the snapshot between verification and the copy step must refuse the
//! restore with the typed [`RestoreError::StagedFileMissing`] carrying ONLY
//! the manifest-relative file name (exactly `Path::new(LISTED_REL)`, no
//! absolute scratch root in the rendered message), leaving the target
//! `data_dir` byte-identical and NO stage or pre-restore-backup sibling
//! behind. Because the check is the FIRST step that can refuse after the
//! copy, the legacy (target-mutating, `open_or_init`-style) probe must NOT
//! have run at all on a staged refusal (finding 6, R2d-FIX-C review): the
//! probe stand-in below WRITES into the target when invoked, so any
//! pre-check invocation would be caught by the unchanged-tree assertion.
//! The managed variant also proves the cross-process read lease:
//! retention's non-blocking exclusive attempt is refused while restore
//! holds the snapshot, and free again right after the typed refusal.
//! Unmanaged and managed snapshots alike need no database handle: the
//! refusal happens right after the copy, before any store is opened. Pure
//! file APIs, no runtime.
//!
//! The seam: `restore_impl`'s `before_copy` hook runs exactly where a
//! concurrent deleter would strike — after the snapshot verified clean and
//! before the source walk — deterministically reproducing the race that in
//! production is a scheduled retention pass (or any other deleter) removing
//! the snapshot mid-restore.

use std::cell::Cell;
use std::fs;
use std::path::{Path, PathBuf};

use tempfile::TempDir;

use crate::sync::durability::backup::offline::{verify_manifest, Manifest, ManifestFileEntry};
use crate::sync::durability::backup::online::copy_engine::hash_file;
use crate::sync::durability::backup::online::manifest::BACKUP_META_FILE_NAME;
use crate::sync::durability::backup::restore::{
    has_external_staging_location, restore_impl, RestoreError,
};
use crate::sync::durability::backup::snapshot_lease;

/// The listed file lives under `payload/`, NOT under `users/`: the restore
/// path opens (and, for the success test below, invalidates) the Fjall
/// `users` store after staging, so a snapshot payload named `users/...`
/// would exercise store-open semantics instead of the plain staged-file
/// path this regression targets. `payload/` keeps the fixture a plain
/// regular file and leaves `users` absent on the success path.
const LISTED_REL: &str = "payload/keeper.bin";
const TARGET_SENTINEL: &str = "live.bin";
const TARGET_BYTES: &[u8] = b"pre-restore-live-marker";

/// Minimal VALID v1 offline snapshot: one file under a directory, manifest
/// listing exactly that file (verify also refuses any unmanifested file, so
/// the set must match exactly).
fn build_v1_snapshot(snapshot: &Path) {
    fs::create_dir_all(snapshot.join("payload")).expect("snapshot payload dir");
    fs::write(snapshot.join(LISTED_REL), b"kept-bytes").expect("write listed file");

    let (sha256, size_bytes) = hash_file(&snapshot.join(LISTED_REL)).expect("hash listed file");
    let manifest = Manifest {
        format_version: 1,
        created_at_unix_ns: 1_700_000_000_000,
        files: vec![ManifestFileEntry {
            path: LISTED_REL.to_string(),
            sha256,
            size_bytes,
        }],
    };
    let json = serde_json::to_vec_pretty(&manifest).expect("encode manifest");
    fs::write(snapshot.join("manifest.json"), json).expect("write manifest");
}

/// Stop an initialized target with the sentinel file, as a stopped server's
/// `data_dir` would look before a restore.
fn seed_target(data_dir: &Path) {
    fs::create_dir_all(data_dir).expect("data dir");
    fs::write(data_dir.join(TARGET_SENTINEL), TARGET_BYTES).expect("write target sentinel");
}

/// The typed refusal must name EXACTLY the missing manifest-listed file
/// (relative, no staging prefix) and its rendered message must not leak the
/// restore's absolute scratch root (finding 6: the old variant carried the
/// joined staging path).
fn assert_staged_file_missing(err: RestoreError, scratch_root: &Path) {
    let scratch_root = scratch_root.to_string_lossy().into_owned();
    match err {
        RestoreError::StagedFileMissing(path) => {
            assert_eq!(
                path,
                PathBuf::from(LISTED_REL),
                "the typed error must carry exactly the manifest-relative file name, got {path:?}"
            );
            let message = RestoreError::StagedFileMissing(path.clone()).to_string();
            assert!(
                !message.contains(&scratch_root),
                "the error message must not contain the absolute scratch root \
                 '{scratch_root}', got: {message}"
            );
        }
        other => panic!("expected RestoreError::StagedFileMissing, got: {other:?}"),
    }
}

/// After a refusal that never reached the swap: target sentinel intact, the
/// target tree contains EXACTLY its seeded entries (the legacy probe's
/// `open_or_init` would have persisted `last_started_at` and grown the
/// tree), and no staged temp dir / pre-restore backup sibling is left
/// behind.
fn assert_target_and_siblings_clean(root: &Path, data_dir: &Path) {
    assert_eq!(
        fs::read(data_dir.join(TARGET_SENTINEL)).expect("read target sentinel"),
        TARGET_BYTES,
        "data_dir must be untouched by the refused restore"
    );
    let target_entries: Vec<String> = dir_names(data_dir);
    assert_eq!(
        target_entries,
        vec![TARGET_SENTINEL.to_string()],
        "the target tree must be byte-identical to its seeded state (no probe \
         or restore write may have landed), found: {target_entries:?}"
    );
    let siblings: Vec<String> = dir_names(root);
    assert!(
        siblings
            .iter()
            .all(|name| !name.contains("restore_tmp") && !name.contains("pre_restore_backup")),
        "no stage or backup siblings may remain after the refusal, found: {siblings:?}"
    );
}

fn dir_names(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = fs::read_dir(dir)
        .expect("read dir")
        .map(|entry| {
            entry
                .expect("dir entry")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .collect();
    names.sort();
    names
}

// ---------------------------------------------------------------------------
// Copy-gate binding tests (round-2 finding 1), consolidated here from the
// former `copy_binding/` subfolder to keep this folder within the 7-entry
// cap. They target the step-3 walk's binding to the VERIFIED manifest in the
// same pass and its refusal of unlisted regular files, so the published
// restore covers exactly what verification covered. The deterministic
// between-verification-and-copy point is `restore_impl`'s existing
// per-instance, consumed-once `before_copy` seam (it runs after verify and
// the stamp gate, before the source walk) — the same seam the staged-listing
// regressions above use. Pure file APIs, no runtime, no database handle:
// every refusal happens before any store is opened.
// ---------------------------------------------------------------------------

/// The unlisted file the copy-gate tests add mid-restore.
const ROGUE_REL: &str = "payload/rogue.bin";
const LISTED_BYTES: &[u8] = b"kept-bytes";

/// Minimal VALID v1 offline snapshot: one listed payload file, manifest
/// listing exactly that file (verify refuses any unmanifested file).
fn binding_build_v1_snapshot(snapshot: &Path) {
    fs::create_dir_all(snapshot.join("payload")).expect("snapshot payload dir");
    fs::write(snapshot.join(LISTED_REL), LISTED_BYTES).expect("write listed file");

    let (sha256, size_bytes) = hash_file(&snapshot.join(LISTED_REL)).expect("hash listed file");
    let manifest = Manifest {
        format_version: 1,
        created_at_unix_ns: 1_700_000_000_000,
        files: vec![ManifestFileEntry {
            path: LISTED_REL.to_string(),
            sha256,
            size_bytes,
        }],
    };
    let json = serde_json::to_vec_pretty(&manifest).expect("encode manifest");
    fs::write(snapshot.join("manifest.json"), json).expect("write manifest");
}

/// After a refusal that never reached the swap: target sentinel intact and
/// no staged temp dir / pre-restore backup sibling left behind.
fn binding_assert_target_and_siblings_clean(root: &Path, data_dir: &Path) {
    assert_eq!(
        fs::read(data_dir.join(TARGET_SENTINEL)).expect("read target sentinel"),
        TARGET_BYTES,
        "data_dir must be untouched by the refused restore"
    );
    let siblings: Vec<String> = fs::read_dir(root)
        .expect("read root")
        .map(|entry| {
            entry
                .expect("dir entry")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .collect();
    assert!(
        siblings
            .iter()
            .all(|name| !name.contains("restore_tmp") && !name.contains("pre_restore_backup")),
        "no stage or backup siblings may remain after the refusal, found: {siblings:?}"
    );
}

/// Kills the mutant that deletes the copy gate's unlisted-file refusal
/// (i.e. a walk that publishes whatever the source tree happens to hold):
/// a regular file ADDED to the snapshot after verification (deterministic
/// via the `before_copy` seam) must refuse with the typed
/// [`RestoreError::StagedFileUnmanifested`], naming ONLY the
/// manifest-relative path, and leave the target untouched.
#[test]
fn unlisted_source_file_added_between_verify_and_copy_refuses_typed() {
    let root = TempDir::new().expect("temp root");
    let snapshot = root.path().join("snapshot");
    let data_dir = root.path().join("data_dir");
    seed_target(&data_dir);
    binding_build_v1_snapshot(&snapshot);

    let err = restore_impl(
        &snapshot,
        &data_dir,
        false,
        |_data_dir| Ok(()),
        || {
            // The seam: verification and the stamp gate already passed; a
            // concurrent writer adds an unlisted regular file before the
            // copy walk enumerates it.
            fs::write(snapshot.join(ROGUE_REL), b"rogue").expect("write unlisted file");
        },
    )
    .expect_err("restore must refuse an unmanifested source file");

    match err {
        RestoreError::StagedFileUnmanifested(rel) => {
            assert_eq!(
                rel, ROGUE_REL,
                "the typed error must carry exactly the manifest-relative path"
            );
            let message = RestoreError::StagedFileUnmanifested(rel).to_string();
            let scratch_root = root.path().to_string_lossy().into_owned();
            assert!(
                !message.contains(&scratch_root),
                "the error message must not contain the absolute scratch root, got: {message}"
            );
        }
        other => panic!("expected RestoreError::StagedFileUnmanifested, got: {other:?}"),
    }
    binding_assert_target_and_siblings_clean(root.path(), &data_dir);
}

/// Kills the mutant that reverts the restore ledger to the version-blind
/// control-file rule (the root `backup.meta.json` exempt for EVERY snapshot
/// version): a v1 verifier rejects an unlisted root `backup.meta.json`, so one
/// that appears AFTER verification (the same `before_copy` seam) must be
/// refused by the copy walk of a v1 restore, never published. Runs through the
/// production wiring from the verified manifest to the ledger.
#[test]
fn v1_restore_refuses_a_root_backup_meta_added_after_verification() {
    let root = TempDir::new().expect("temp root");
    let snapshot = root.path().join("snapshot");
    let data_dir = root.path().join("data_dir");
    seed_target(&data_dir);
    binding_build_v1_snapshot(&snapshot);
    let sidecar = BACKUP_META_FILE_NAME;

    let err = restore_impl(
        &snapshot,
        &data_dir,
        false,
        |_data_dir| Ok(()),
        || {
            fs::write(snapshot.join(sidecar), b"sidecar").expect("write root sidecar");
        },
    )
    .expect_err("a v1 restore must refuse an unlisted root backup.meta.json");

    match err {
        RestoreError::StagedFileUnmanifested(rel) => assert_eq!(
            rel, sidecar,
            "the typed error must carry exactly the manifest-relative path"
        ),
        other => panic!("expected RestoreError::StagedFileUnmanifested, got: {other:?}"),
    }
    binding_assert_target_and_siblings_clean(root.path(), &data_dir);
}

/// Kills the mutant that drops the copy gate's per-entry size/sha256
/// comparison (a walk that copies without binding to the verified entry):
/// a listed file whose BYTES change after verification (same seam) must
/// refuse with [`RestoreError::StagedFilePresentButDifferent`] — truthfully
/// present-but-different, NOT "missing" (round-2 finding 5).
///
/// PASS-ATTRIBUTION NOTE: at the restore level this variant can come from
/// EITHER pass — the step-3 copy gate's in-pass entry comparison or the
/// step-3b staged recheck's re-hash — so this test alone cannot tell which
/// pass refused; a mutant that drops ONLY the in-pass comparison but keeps
/// the recheck still passes here. The IN-PASS mutant is killed by the
/// direct `copy_gate::stream_copy` unit tests in
/// `offline/tests/copy_gate_tests.rs`
/// (`listed_bytes_changed_refuse_with_both_sides_of_the_mismatch` and
/// `listed_file_length_changed_reports_entry_mismatch_not_generic_io`),
/// which run the gate with no recheck behind it.
#[test]
fn listed_file_bytes_mutated_between_verify_and_copy_refuse_with_mismatch_variant() {
    let root = TempDir::new().expect("temp root");
    let snapshot = root.path().join("snapshot");
    let data_dir = root.path().join("data_dir");
    seed_target(&data_dir);
    binding_build_v1_snapshot(&snapshot);

    let (expected_sha256, expected_size_bytes) =
        hash_file(&snapshot.join(LISTED_REL)).expect("hash listed file pre-mutation");

    let err = restore_impl(
        &snapshot,
        &data_dir,
        false,
        |_data_dir| Ok(()),
        || {
            // The seam: rewrite the listed file with different bytes after
            // verification hashed the originals.
            fs::write(snapshot.join(LISTED_REL), b"TAMPERED").expect("mutate listed file");
        },
    )
    .expect_err("restore must refuse a listed file whose bytes changed after verification");

    match err {
        RestoreError::StagedFilePresentButDifferent {
            path,
            expected_sha256: got_expected_sha,
            expected_size_bytes: got_expected_size,
            ..
        } => {
            assert_eq!(path, LISTED_REL);
            assert_eq!(got_expected_sha, expected_sha256);
            assert_eq!(got_expected_size, expected_size_bytes);
        }
        RestoreError::StagedFileMissing(_) => {
            panic!("a present-but-different file must NOT be reported as missing")
        }
        other => panic!("expected RestoreError::StagedFilePresentButDifferent, got: {other:?}"),
    }
    binding_assert_target_and_siblings_clean(root.path(), &data_dir);
}

/// Kills the mutant that drops the step-3a staged root-manifest digest
/// comparison (the copy carrying bytes OTHER than the exact bytes
/// verification consumed would be published): AFTER verification succeeds,
/// the `before_copy` seam swaps the snapshot's `manifest.json` for a
/// DIFFERENT but still VALID manifest — same files, re-encoded with
/// different whitespace (compact vs pretty; both decode — the positive
/// control below proves a compact manifest verifies AND, unchanged, the
/// plain snapshot restores — see the existing
/// `plain_unchanged_snapshot_restores_successfully` positive control).
/// The manifest is NOT in `files`, so the step-3b staged
/// recheck cannot refuse it — ONLY the root-digest comparison can, and it
/// must refuse with the typed `ManifestVerification(ManifestInvalid)` and
/// leave the target untouched.
#[test]
fn staged_root_manifest_with_different_but_valid_bytes_refuses_typed() {
    let root = TempDir::new().expect("temp root");

    // Sanity: the re-encoded (compact) manifest is still VALID — pretty and
    // compact JSON both decode, so the swap is not a corruption test but a
    // pure byte-identity test.
    let compact = root.path().join("compact_snapshot");
    fs::create_dir_all(compact.join("payload")).expect("payload dir");
    fs::write(compact.join(LISTED_REL), LISTED_BYTES).expect("write listed file");
    let (sha256, size_bytes) = hash_file(&compact.join(LISTED_REL)).expect("hash listed file");
    let manifest = Manifest {
        format_version: 1,
        created_at_unix_ns: 1_700_000_000_000,
        files: vec![ManifestFileEntry {
            path: LISTED_REL.to_string(),
            sha256,
            size_bytes,
        }],
    };
    let compact_bytes = serde_json::to_vec(&manifest).expect("encode compact manifest");
    fs::write(compact.join("manifest.json"), &compact_bytes).expect("write compact manifest");
    verify_manifest(&compact)
        .expect("a compact-encoded manifest must still verify (both spacings decode)");

    // The case under test: verification consumes the PRETTY bytes; the
    // seam then swaps in the COMPACT bytes of the same valid manifest.
    let snapshot = root.path().join("snapshot");
    let data_dir = root.path().join("data_dir");
    seed_target(&data_dir);
    binding_build_v1_snapshot(&snapshot);
    let pretty = fs::read(snapshot.join("manifest.json")).expect("read pretty manifest");
    assert_ne!(
        pretty, compact_bytes,
        "the two encodings must differ byte-wise (whitespace only)"
    );

    let err = restore_impl(
        &snapshot,
        &data_dir,
        false,
        |_data_dir| Ok(()),
        || {
            fs::write(snapshot.join("manifest.json"), &compact_bytes)
                .expect("swap manifest for valid-but-different bytes");
        },
    )
    .expect_err("restore must refuse when the staged manifest bytes differ from the verified ones");

    assert!(
        matches!(err, RestoreError::ManifestVerification(_)),
        "the root-digest refusal must be typed ManifestVerification, got: {err:?}"
    );
    binding_assert_target_and_siblings_clean(root.path(), &data_dir);
}

/// Positive control: a plain unchanged snapshot restores successfully —
/// the copy gate hashes every copied file, finds each equal to its
/// verified manifest entry, and the staged `manifest.json` digest matches
/// the digest verification consumed. Mutant killed: a gate that refuses
/// EVERYTHING (e.g. comparing the control manifest against the ledger and
/// refusing it) would fail here.
#[test]
fn plain_unchanged_snapshot_restores_successfully() {
    let root = TempDir::new().expect("temp root");
    let snapshot = root.path().join("snapshot");
    let data_dir = root.path().join("data_dir");
    seed_target(&data_dir);
    binding_build_v1_snapshot(&snapshot);

    let probe_ran = Cell::new(false);
    let report = restore_impl(
        &snapshot,
        &data_dir,
        false,
        |_data_dir| {
            probe_ran.set(true);
            Ok(())
        },
        || {},
    )
    .expect("a plain unchanged snapshot must restore successfully");

    assert!(probe_ran.get(), "the legacy probe must run on success");
    assert_eq!(
        fs::read(data_dir.join(LISTED_REL)).expect("read restored listed file"),
        LISTED_BYTES,
        "the restored data_dir must carry the snapshot payload"
    );
    assert_eq!(
        fs::read(data_dir.join("manifest.json")).expect("read restored manifest"),
        fs::read(snapshot.join("manifest.json")).expect("read source manifest"),
        "the staged root manifest must be byte-identical to the verified one"
    );
    assert_eq!(report.files_restored, 2, "listed file + manifest");
}

#[test]
fn missing_staged_file_refuses_typed_leaves_target_and_siblings_clean() {
    let root = TempDir::new().expect("temp root");
    let snapshot = root.path().join("snapshot");
    let data_dir = root.path().join("data_dir");
    seed_target(&data_dir);

    build_v1_snapshot(&snapshot);

    // No `--force`: the legacy probe must run and pass on a SUCCESSFUL
    // restore path — but a staged refusal must happen BEFORE it runs (the
    // probe stand-in writes into the target, so this assertion fails while
    // the probe still runs before the staged check).
    let probe_ran = Cell::new(false);
    let err = restore_impl(
        &snapshot,
        &data_dir,
        false,
        |data_dir| {
            probe_ran.set(true);
            // Stand-in for the CLI probe's `server_meta::open_or_init`,
            // which persists `last_started_at` in the target on refusal.
            fs::write(data_dir.join("server_meta.last_started_at"), b"probe")
                .expect("probe target write");
            Ok(())
        },
        || {
            // The seam: verification and the stamp gate have already passed;
            // delete the listed file's directory from the snapshot before
            // the copy walk enumerates it.
            fs::remove_dir_all(snapshot.join("payload")).expect("remove listed dir");
        },
    )
    .expect_err("restore must refuse when a manifest-listed file is missing from staging");

    assert!(
        !probe_ran.get(),
        "the legacy (target-mutating) probe must NOT run before the staged \
         presence check refuses: invoking it writes `last_started_at` into \
         an initialized target that is about to be refused anyway"
    );

    assert_staged_file_missing(err, root.path());
    assert_target_and_siblings_clean(root.path(), &data_dir);
}

/// The legacy probe must run on a SUCCESSFUL restore path — but only AFTER
/// the staged presence re-check succeeded (finding 6): while it runs, the
/// probe observes the staged `restore_tmp` sibling holding the
/// manifest-listed bytes, and afterwards the restored `data_dir` carries
/// that payload. The probe's receiver is the ORIGINAL `data_dir`, which the
/// swap renames to `report.pre_restore_backup` — so a probe write can never
/// appear in the newly restored target.
#[test]
fn legacy_probe_runs_only_after_the_staged_presence_check_succeeds() {
    let root = TempDir::new().expect("temp root");
    let snapshot = root.path().join("snapshot");
    let data_dir = root.path().join("data_dir");
    seed_target(&data_dir);
    build_v1_snapshot(&snapshot);

    let probe_ran = Cell::new(false);
    let report = restore_impl(
        &snapshot,
        &data_dir,
        false,
        |probe_target| {
            probe_ran.set(true);
            assert_eq!(
                probe_target, data_dir,
                "the probe receives the original data_dir"
            );
            // The staged copy exists while the probe runs and already
            // holds the manifest-listed bytes.
            let staged = fs::read_dir(root.path())
                .expect("read root")
                .map(|entry| entry.expect("dir entry").path())
                .find(|path| {
                    path.file_name()
                        .map(|n| n.to_string_lossy().contains("restore_tmp"))
                        .unwrap_or(false)
                })
                .expect("the staged copy must exist while the probe runs");
            assert_eq!(
                fs::read(staged.join(LISTED_REL)).expect("read staged listed file"),
                b"kept-bytes",
                "the staged copy must hold the manifest-listed bytes"
            );
            Ok(())
        },
        || {},
    )
    .expect("a clean legacy restore must succeed");

    assert!(
        probe_ran.get(),
        "the legacy probe must still run (unless --force) on a successful path"
    );
    assert_eq!(
        fs::read(data_dir.join(LISTED_REL)).expect("read restored listed file"),
        b"kept-bytes",
        "the restored data_dir must carry the snapshot payload"
    );
    let backup = report
        .pre_restore_backup
        .expect("a pre-existing data_dir must be preserved as pre_restore_backup");
    assert_eq!(
        fs::read(backup.join(TARGET_SENTINEL)).expect("read backup sentinel"),
        TARGET_BYTES,
        "the probe's receiver (the original data_dir) is the pre_restore_backup"
    );
    assert_eq!(report.files_restored, 2, "listed file + manifest");
}

/// A legacy probe that refuses AFTER the staged presence re-check succeeded
/// must clean the staged copy (which the probe observed in place) and leave
/// no `restore_tmp` / pre-restore sibling behind; the swap never ran, so
/// `data_dir` keeps only its seeded sentinel (finding 6's cleanup
/// requirement).
#[test]
fn legacy_probe_refusal_after_staged_listing_cleans_staging() {
    let root = TempDir::new().expect("temp root");
    let snapshot = root.path().join("snapshot");
    let data_dir = root.path().join("data_dir");
    seed_target(&data_dir);
    build_v1_snapshot(&snapshot);

    let err = restore_impl(
        &snapshot,
        &data_dir,
        false,
        |_data_dir| {
            // The staged copy must exist while the probe runs: the probe
            // sits between the staged presence re-check and the swap, so a
            // refusal here must clean it up afterwards.
            assert!(
                dir_names(root.path())
                    .iter()
                    .any(|name| name.contains("restore_tmp")),
                "the staged copy must be present while the refusing probe runs"
            );
            Err(RestoreError::ServerRunning(data_dir.to_path_buf()))
        },
        || {},
    )
    .expect_err("a refusing probe must refuse the restore");

    assert!(
        matches!(err, RestoreError::ServerRunning(ref p) if p == &data_dir),
        "the probe's own error must propagate, got: {err:?}"
    );
    let siblings = dir_names(root.path());
    assert!(
        siblings
            .iter()
            .all(|name| !name.contains("restore_tmp") && !name.contains("pre_restore_backup")),
        "a probe refusal after the staged check must clean the staged copy, \
         found: {siblings:?}"
    );
    assert_eq!(
        fs::read(data_dir.join(TARGET_SENTINEL)).expect("read target sentinel"),
        TARGET_BYTES,
        "the swap never ran: the target's seeded sentinel is intact"
    );
}

#[test]
fn managed_snapshot_lease_blocks_retention_until_the_refusal_releases_it() {
    let root = TempDir::new().expect("temp root");
    let managed = root.path().join("managed");
    // A managed root holds timestamp-named snapshots; provision forces the
    // root detection even without the retention boot path.
    let snapshot = managed.join("20200101_000001");
    let data_dir = root.path().join("data_dir");
    seed_target(&data_dir);

    build_v1_snapshot(&snapshot);
    snapshot_lease::provision(&managed).expect("provision managed root");

    let err = restore_impl(
        &snapshot,
        &data_dir,
        false,
        |_data_dir| Ok(()),
        || {
            // While restore holds the shared source lease, retention's
            // NON-BLOCKING exclusive attempt must refuse (Ok(None) =
            // contention, not an error).
            assert!(
                matches!(snapshot_lease::try_exclusive(&managed), Ok(None)),
                "try_exclusive must refuse while restore holds the shared lease"
            );
            fs::remove_dir_all(snapshot.join("payload")).expect("remove listed dir");
        },
    )
    .expect_err("restore must refuse when a manifest-listed file is missing from staging");

    // The typed refusal dropped the source lease: the next retention pass
    // acquires the exclusive lease and may delete the snapshot.
    let exclusive = snapshot_lease::try_exclusive(&managed)
        .expect("try_exclusive must not error after the refusal")
        .expect("lease must be free once the refusal dropped the source lease");
    drop(exclusive);

    assert_staged_file_missing(err, root.path());
    assert_target_and_siblings_clean(root.path(), &data_dir);
}

/// Catches staging inside the live root, which makes pre-swap refusals write into the target.
#[test]
fn staging_location_rejects_root_paths_but_allows_normal_targets() {
    assert!(!has_external_staging_location(Path::new("C:/")));
    assert!(!has_external_staging_location(Path::new("/")));
    let normal_target = std::env::temp_dir().join("restore-target");
    assert!(has_external_staging_location(&normal_target));
}

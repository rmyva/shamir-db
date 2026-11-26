//! Control-file link refusal regressions (round-3 finding 2, CI only).
//!
//! The snapshot reader opens control leaves (`manifest.json`,
//! `format.stamp`) through the no-follow `open_plain_snapshot_file`, so a
//! SYMLINK at a control name is refused instead of followed. Each test
//! below points the control link at an EXTERNAL, fully VALID target: with
//! the old symlink-following implementation the reader would open the
//! valid target, ACCEPT it, and the restore would proceed — so every
//! assertion here fails under that mutant and passes only via the
//! no-follow refusal. Plain-file positive controls prove the pipeline
//! accepts the same content when it is a regular file. Cross-platform:
//! symlink creation uses the per-OS helper below; on Windows an
//! unprivileged process cannot create symlinks (os error 1314,
//! ERROR_PRIVILEGE_NOT_HELD) and the affected test skips itself with an
//! explicit reason — never a silent pass; any other link-creation error
//! fails the test.

use std::cell::Cell;
use std::fs;
use std::io;
#[cfg(unix)]
use std::os::unix::fs::symlink;
#[cfg(windows)]
use std::os::windows::fs::symlink_file;
use std::path::Path;

use tempfile::TempDir;

use crate::format_stamp::{
    inspect, restore_snapshot_gate, write_stamp, EpochPolicy, FormatStamp, StampState,
    CURRENT_FORMAT_EPOCH,
};
use crate::sync::durability::backup::offline::{verify_manifest, Manifest, ManifestFileEntry};
use crate::sync::durability::backup::online::copy_engine::hash_file;
use crate::sync::durability::backup::restore::{restore_impl, RestoreError};

/// Create a file symlink on the current platform.
fn create_file_symlink(target: &Path, link: &Path) -> io::Result<()> {
    #[cfg(windows)]
    {
        symlink_file(target, link)
    }
    #[cfg(unix)]
    {
        symlink(target, link)
    }
}

/// Create the link, or explicitly skip the calling test on Windows without
/// symlink privileges (os error 1314, ERROR_PRIVILEGE_NOT_HELD). ANY other
/// error fails the test. Returns false only for the documented skip.
fn create_file_symlink_or_skip(test_name: &str, target: &Path, link: &Path) -> bool {
    match create_file_symlink(target, link) {
        Ok(()) => true,
        Err(error) if error.raw_os_error() == Some(1314) => {
            eprintln!(
                "SKIPPED test `{test_name}`: creating symlink {link:?} -> {target:?} failed \
                 with ERROR_PRIVILEGE_NOT_HELD (os error 1314); Windows unprivileged runs \
                 cannot create symlinks, so this no-follow regression cannot execute here: \
                 {error}"
            );
            false
        }
        Err(error) => {
            panic!("failed to create symlink {link:?} -> {target:?} in test `{test_name}`: {error}")
        }
    }
}

/// Minimal VALID v1 offline snapshot: one payload file, manifest listing
/// exactly that file (verify refuses any unmanifested file).
fn build_v1_snapshot(snapshot: &Path) {
    fs::create_dir_all(snapshot).expect("snapshot dir");
    fs::write(snapshot.join("payload"), b"ok").expect("write payload");
    let (sha256, size_bytes) = hash_file(&snapshot.join("payload")).expect("hash payload");
    let bytes = serde_json::to_vec(&Manifest {
        format_version: 1,
        created_at_unix_ns: 1,
        files: vec![ManifestFileEntry {
            path: "payload".into(),
            sha256,
            size_bytes,
        }],
    })
    .expect("encode manifest");
    fs::write(snapshot.join("manifest.json"), bytes).expect("write manifest");
}

/// Plain VALID stamp (via the real codec writer) inside `dir`.
fn write_valid_plain_stamp(dir: &Path) {
    write_stamp(
        dir,
        &FormatStamp::with_epoch(
            CURRENT_FORMAT_EPOCH,
            "control-file-tests",
            1_700_000_000_000,
        ),
    )
    .expect("write plain valid stamp");
}

/// Kills the mutant that makes the stamp reader FOLLOW the `format.stamp`
/// symlink: the link points at an external file carrying a VALID, decodable
/// stamp (written by the real codec writer), so a following implementation
/// classifies [`StampState::Valid`] and the restore stamp gate accepts —
/// only the no-follow refusal (the link itself is opened, not its target)
/// yields `Corrupt` and a refused gate. The plain-file positive control
/// proves the gate-level path (`restore_snapshot_gate`, NOT the full
/// restore: v1 verify independently refuses an unmanifested stamp, which
/// would blur which gate refused) accepts the identical stamp as a regular
/// file.
#[test]
fn format_stamp_symlink_to_valid_external_stamp_is_corrupt_and_gate_refuses() {
    let root = TempDir::new().expect("temp root");
    let snapshot = root.path().join("snapshot");
    fs::create_dir(&snapshot).expect("snapshot dir");

    // Plain-file positive controls: valid plain stamp inspects Valid and
    // passes the restore stamp gate.
    write_valid_plain_stamp(&snapshot);
    assert!(
        matches!(inspect(&snapshot), StampState::Valid(_)),
        "a plain valid stamp must inspect as Valid"
    );
    restore_snapshot_gate(&snapshot, &EpochPolicy::compiled())
        .expect("a plain valid stamp must pass the restore stamp gate");

    // The case under test: the SAME valid stamp bytes, external, behind a
    // symlink named format.stamp.
    let external = root.path().join("external.stamp");
    fs::rename(snapshot.join("format.stamp"), &external).expect("move stamp out of snapshot");
    if !create_file_symlink_or_skip(
        "format_stamp_symlink_to_valid_external_stamp_is_corrupt_and_gate_refuses",
        &external,
        &snapshot.join("format.stamp"),
    ) {
        return;
    }

    assert!(
        matches!(inspect(&snapshot), StampState::Corrupt { .. }),
        "mutant: a following stamp reader accepts the external valid stamp \
         through the format.stamp link; the no-follow reader must classify \
         the link itself as Corrupt, not Valid"
    );
    assert!(
        restore_snapshot_gate(&snapshot, &EpochPolicy::compiled()).is_err(),
        "mutant: the restore stamp gate follows the format.stamp link and \
         accepts the external valid stamp"
    );
}

/// Kills the mutant that makes the restore control-file reader FOLLOW the
/// `manifest.json` symlink. The external manifest the link points at is the
/// byte-identical VALID, COMPLETE manifest, so a following implementation
/// classifies, verifies, gates and copies cleanly and the restore SUCCEEDS
/// (the probe seam runs) — only the no-follow refusal at the classifier
/// (`classify_snapshot_kind`, before any probe/`before_copy` seam runs)
/// makes the assertions below pass.
///
/// MUTANT ISOLATION: the classifier's refusal is its OWN typed variant
/// ([`RestoreError::ControlFileNotRegular`], manifest-relative path only),
/// distinct from the verifier's `ManifestVerification` of the same shape —
/// so this test FAILS when ONLY the classifier's no-follow open is
/// reverted (a following classifier lets the restore proceed or fails
/// later at a different variant), and the verifier's own no-follow
/// refusal cannot masquerade as a pass here. Positive controls: the plain
/// manifest verifies, and the identical plain snapshot restores fully.
#[test]
fn manifest_symlink_to_valid_external_manifest_refused_by_restore() {
    let root = TempDir::new().expect("temp root");

    // Verify positive control + plain RESTORE positive control: the same
    // content as a plain regular file verifies AND restores successfully.
    let plain = root.path().join("plain_snapshot");
    build_v1_snapshot(&plain);
    verify_manifest(&plain).expect("a plain complete manifest must verify");
    let plain_target = root.path().join("plain_target");
    fs::create_dir(&plain_target).expect("plain target dir");
    let plain_probe_ran = Cell::new(false);
    restore_impl(
        &plain,
        &plain_target,
        false,
        |_data_dir| {
            plain_probe_ran.set(true);
            Ok(())
        },
        || {},
    )
    .expect("a plain unchanged snapshot must restore successfully");
    assert!(
        plain_probe_ran.get(),
        "the plain control restore must run its probe"
    );

    // The case under test: manifest.json is a symlink to the external,
    // byte-identical, VALID manifest.
    let snapshot = root.path().join("snapshot");
    build_v1_snapshot(&snapshot);
    let external = root.path().join("external.json");
    fs::copy(snapshot.join("manifest.json"), &external).expect("copy manifest out");
    fs::remove_file(snapshot.join("manifest.json")).expect("remove plain manifest");
    if !create_file_symlink_or_skip(
        "manifest_symlink_to_valid_external_manifest_refused_by_restore",
        &external,
        &snapshot.join("manifest.json"),
    ) {
        return;
    }

    // The no-follow verify refuses the link too (kept: the verifier opens
    // through the same no-open-follow mechanism).
    assert!(
        verify_manifest(&snapshot).is_err(),
        "mutant: verify follows the external manifest symlink"
    );

    // Restore refusal, ISOLATED to the classifier stage by TYPE: the
    // classifier's no-follow open refusal is its own variant. If only the
    // classifier's no-follow open is reverted, this assertion fails (the
    // restore either succeeds or refuses with a different variant).
    let target = root.path().join("target");
    fs::create_dir(&target).expect("target dir");
    let probe_ran = Cell::new(false);
    let before_copy_ran = Cell::new(false);
    let error = restore_impl(
        &snapshot,
        &target,
        false,
        |_data_dir| {
            probe_ran.set(true);
            Ok(())
        },
        || before_copy_ran.set(true),
    )
    .expect_err("restore must refuse a manifest.json symlink");

    assert!(
        matches!(
            error,
            RestoreError::ControlFileNotRegular { ref path }
                if path == "manifest.json"
        ),
        "mutant: the CLASSIFIER's no-follow open was reverted or its typed \
         refusal demoted to the verifier's ManifestVerification; expected \
         ControlFileNotRegular {{ path: \"manifest.json\" }}, got: {error:?}"
    );
    assert!(
        !probe_ran.get() && !before_copy_ran.get(),
        "mutant: a following implementation verifies the identical external \
         manifest, passes the stamp gate and REACHES the probe/copy seams; \
         the refusal must come from the classifier BEFORE any seam runs"
    );
}

/// Kills the mutant that makes the restore stamp gate FOLLOW a
/// `format.stamp` symlink the manifest does not list: the link points at an
/// EXTERNAL VALID stamp, so a following gate accepts it and the snapshot
/// sails through the gate — only the no-follow read refuses. Gate-level
/// (`restore_snapshot_gate`) isolation: v1 verify independently refuses an
/// unmanifested stamp file, so a full restore cannot attribute the refusal
/// to the stamp gate. Positive control: the same snapshot with a PLAIN
/// valid stamp passes the gate.
#[test]
fn unlisted_format_stamp_symlink_refused_by_stamp_gate_but_plain_stamp_passes() {
    let root = TempDir::new().expect("temp root");
    let snapshot = root.path().join("snapshot");
    build_v1_snapshot(&snapshot);

    // Positive control: plain valid stamp, same snapshot, gate passes.
    write_valid_plain_stamp(&snapshot);
    restore_snapshot_gate(&snapshot, &EpochPolicy::compiled())
        .expect("a plain valid stamp must pass the restore stamp gate");

    // The case under test: the stamp moves out and comes back as a symlink
    // to the external VALID stamp; the manifest still lists only payload
    // files.
    let external = root.path().join("external.stamp");
    fs::rename(snapshot.join("format.stamp"), &external).expect("move stamp out of snapshot");
    if !create_file_symlink_or_skip(
        "unlisted_format_stamp_symlink_refused_by_stamp_gate_but_plain_stamp_passes",
        &external,
        &snapshot.join("format.stamp"),
    ) {
        return;
    }

    assert!(
        restore_snapshot_gate(&snapshot, &EpochPolicy::compiled()).is_err(),
        "mutant: the restore stamp gate follows the unlisted format.stamp \
         link and accepts the external valid stamp"
    );
}

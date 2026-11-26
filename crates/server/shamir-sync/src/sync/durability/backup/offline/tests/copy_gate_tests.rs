//! Direct unit tests of `copy_gate::stream_copy` and the restore ledger's
//! version-aware root control-file rule (round-4 finding, CI only).
//!
//! These are IN-PASS tests: each kills its mutant inside the copy pass
//! itself, regardless of any later recheck (the step-3b staged recheck and
//! the step-3a staged-manifest digest recheck can mask a broken copy pass
//! at the restore level, but nothing re-runs the in-pass gate here). The
//! restore-level twins in `restore/tests/staged_listing_tests.rs` can only
//! see the CONSEQUENCE of these refusals, not which pass produced them.
//! Pure file APIs (`std::fs`, `tempfile`), no runtime, no database handle.

use std::fs;
#[cfg(target_os = "linux")]
use std::os::unix::ffi::OsStrExt;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use sha2::{Digest, Sha256};
use tempfile::TempDir;

#[cfg(unix)]
use super::super::copy_dir_recursive;
use super::super::copy_gate::{stream_copy, CopyGateError, RestoreCopyLedger};
use crate::sync::durability::backup::online::manifest::BACKUP_META_FILE_NAME;
use crate::sync::durability::backup::{ManifestFileEntry, MANIFEST_FILE_NAME};

/// Hash a byte slice the way `hash_file` would (same codec, same spelling).
fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

/// Write `bytes` into a temp file pair (input + output) and return them
/// opened, plus the input's on-open size. Output names are unique per call
/// (tests may open several pairs in one temp dir).
fn open_pair(dir: &TempDir, input_bytes: &[u8]) -> (std::fs::File, std::fs::File, u64) {
    static PAIR: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let pair = PAIR.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let input_path = dir.path().join(format!("input-{pair}.bin"));
    fs::write(&input_path, input_bytes).expect("write input file");
    let input = fs::OpenOptions::new()
        .read(true)
        .open(&input_path)
        .expect("open input file");
    let opened_size = input.metadata().expect("input metadata").len();
    let output = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(dir.path().join(format!("output-{pair}.bin")))
        .expect("create output file");
    (input, output, opened_size)
}

/// Ledger over a scratch root listing exactly one entry `payload/keeper.bin`.
fn ledger_listing_keeper(root: &TempDir, allows_backup_meta: bool) -> RestoreCopyLedger {
    RestoreCopyLedger::new(
        root.path(),
        &[ManifestFileEntry {
            path: "payload/keeper.bin".into(),
            sha256: sha256_hex(b"kept-bytes"),
            size_bytes: b"kept-bytes".len() as u64,
        }],
        allows_backup_meta,
    )
}

/// Kills the mutant that drops the entry-digest/size comparison entirely or
/// flips it to always-accept (a gate that counts ANY bytes as copied):
/// byte-identical listed content is the ONLY input that must pass, and it
/// must return the actually-written size and the computed digest.
#[test]
fn matching_listed_bytes_pass_and_return_written_size_and_digest() {
    let root = TempDir::new().expect("temp root");
    let ledger = ledger_listing_keeper(&root, false);
    let (mut input, mut output, opened_size) = open_pair(&root, b"kept-bytes");
    let gate = Some((&ledger, "payload/keeper.bin"));
    let (n, digest) =
        stream_copy(&mut input, &mut output, opened_size, gate).expect("matching bytes must pass");
    assert_eq!(n, b"kept-bytes".len() as u64);
    assert_eq!(digest, sha256_hex(b"kept-bytes"));
}

/// Kills the mutant that drops (or short-circuits) the per-entry digest
/// comparison: changed bytes under a LISTED name must refuse with
/// [`CopyGateError::ListedFileMismatch`] carrying BOTH the expected entry
/// digest/size AND the actual computed ones — in the same pass, not at any
/// later recheck.
#[test]
fn listed_bytes_changed_refuse_with_both_sides_of_the_mismatch() {
    let root = TempDir::new().expect("temp root");
    let ledger = ledger_listing_keeper(&root, false);
    // SAME LENGTH as the listed bytes (10): the size comparison cannot refuse,
    // so only the digest comparison can produce this mismatch.
    let tampered = b"TAMPERED!!";
    assert_eq!(tampered.len(), b"kept-bytes".len());
    let (mut input, mut output, opened_size) = open_pair(&root, tampered);
    let gate = Some((&ledger, "payload/keeper.bin"));
    let error = stream_copy(&mut input, &mut output, opened_size, gate)
        .expect_err("changed listed bytes must refuse in-pass");
    match error {
        CopyGateError::ListedFileMismatch {
            path,
            expected_sha256,
            expected_size_bytes,
            actual_sha256,
            actual_size_bytes,
        } => {
            assert_eq!(path, "payload/keeper.bin");
            assert_eq!(expected_sha256, sha256_hex(b"kept-bytes"));
            assert_eq!(expected_size_bytes, b"kept-bytes".len() as u64);
            assert_eq!(actual_sha256, sha256_hex(tampered));
            assert_eq!(actual_size_bytes, tampered.len() as u64);
        }
        other => panic!("expected ListedFileMismatch, got: {other:?}"),
    }
}

/// Kills the REORDER mutant of the generic-Io-vs-entry-comparison order
/// (finding: `n != opened_size` ran BEFORE the listed-entry comparison, so
/// a listed file whose LENGTH changed reported generic I/O): the file is
/// truncated relative to the entry's `size_bytes`, so with the OLD order
/// the generic `n != opened_size` check fires first and this test sees
/// `CopyGateError::Io` — the fixed code must report the truthful
/// [`CopyGateError::ListedFileMismatch`] with the actual (shorter) size.
#[test]
fn listed_file_length_changed_reports_entry_mismatch_not_generic_io() {
    let root = TempDir::new().expect("temp root");
    let ledger = ledger_listing_keeper(&root, false);
    // Content whose digest matches ITSELF but is 3 bytes SHORT of the
    // entry's recorded size (only the size comparison can refuse on the
    // digest dimension being self-consistent).
    let truncated = b"kept";
    let (mut input, mut output, opened_size) = open_pair(&root, truncated);
    assert_ne!(opened_size, b"kept-bytes".len() as u64);
    let gate = Some((&ledger, "payload/keeper.bin"));
    // The file shrank after it was opened: the written count (4) differs from
    // the size observed at open (7). The old order reported generic Io here;
    // the listed-entry comparison must come first.
    let error = stream_copy(&mut input, &mut output, opened_size + 3, gate)
        .expect_err("a shorter listed file must refuse with the entry mismatch");
    match error {
        CopyGateError::ListedFileMismatch {
            path,
            expected_size_bytes,
            actual_size_bytes,
            ..
        } => {
            assert_eq!(path, "payload/keeper.bin");
            assert_eq!(expected_size_bytes, b"kept-bytes".len() as u64);
            assert_eq!(actual_size_bytes, truncated.len() as u64);
        }
        CopyGateError::Io(e) => panic!(
            "mutant: the generic opened-size check fired before the listed-\
             entry comparison; a listed file whose LENGTH changed must be \
             reported as ListedFileMismatch, got Io: {e}"
        ),
        other => panic!("expected ListedFileMismatch, got: {other:?}"),
    }
}

/// Kills the mutant that drops the unlisted-file refusal in the copy pass
/// (finding 1b): a regular file name the ledger does not list (and no
/// version exempts) refuses with [`CopyGateError::UnmanifestedFile`].
#[test]
fn unlisted_regular_file_refused_in_pass() {
    let root = TempDir::new().expect("temp root");
    let ledger = ledger_listing_keeper(&root, false);
    let (mut input, mut output, opened_size) = open_pair(&root, b"rogue");
    let gate = Some((&ledger, "payload/rogue.bin"));
    let error = stream_copy(&mut input, &mut output, opened_size, gate)
        .expect_err("an unlisted regular file must refuse in-pass");
    assert!(
        matches!(error, CopyGateError::UnmanifestedFile(ref rel) if rel == "payload/rogue.bin"),
        "expected UnmanifestedFile, got: {error:?}"
    );
}

/// Kills the mutant that reverts the ledger to the VERSION-BLIND control
/// rule (the old free `is_root_control_file` exempting `backup.meta.json`
/// at the root for EVERY version): a v1 ledger must still copy the root
/// `manifest.json` (its verifier exempts it) but must REFUSE a root
/// `backup.meta.json` as unmanifested — exactly what the v1 verifier does.
#[test]
fn v1_ledger_accepts_root_manifest_but_refuses_root_backup_meta() {
    let root = TempDir::new().expect("temp root");
    let ledger = ledger_listing_keeper(&root, false);

    // Positive half: root manifest.json passes a v1 ledger.
    let manifest_bytes = b"{\"format_version\":1}";
    let (mut input, mut output, opened_size) = open_pair(&root, manifest_bytes);
    let gate = Some((&ledger, MANIFEST_FILE_NAME));
    stream_copy(&mut input, &mut output, opened_size, gate)
        .expect("a v1 ledger must accept the root manifest.json control file");

    // Refusal half: root backup.meta.json is NOT exempt for v1.
    let (mut input, mut output, opened_size) = open_pair(&root, b"meta");
    let gate = Some((&ledger, BACKUP_META_FILE_NAME));
    let error = stream_copy(&mut input, &mut output, opened_size, gate)
        .expect_err("a v1 ledger must refuse a root backup.meta.json sidecar");
    assert!(
        matches!(error, CopyGateError::UnmanifestedFile(ref rel) if rel == BACKUP_META_FILE_NAME),
        "expected UnmanifestedFile for the v1 root sidecar, got: {error:?}"
    );
}

/// Kills the mutant that breaks the v2 half of the version-aware rule
/// (dropping `allows_backup_meta` from the ledger or inverting it): a v2
/// ledger must accept BOTH root control files, exactly what the v2
/// verifier exempts from its inventory.
#[test]
fn v2_ledger_accepts_both_root_control_files() {
    let root = TempDir::new().expect("temp root");
    let ledger = ledger_listing_keeper(&root, true);

    for rel in [MANIFEST_FILE_NAME, BACKUP_META_FILE_NAME] {
        let bytes = format!("bytes-for-{rel}").into_bytes();
        let (mut input, mut output, opened_size) = open_pair(&root, &bytes);
        let gate = Some((&ledger, rel));
        stream_copy(&mut input, &mut output, opened_size, gate)
            .unwrap_or_else(|e| panic!("a v2 ledger must accept root {rel}, got: {e:?}"));
    }
}

/// Kills the mutant that removes the generic opened-size consistency check
/// for UNLISTED files (where the entry comparison cannot fire and the
/// opened-size check is the only consistency check available): an
/// unlisted-control name whose written byte count diverges from the size
/// observed at open refuses with generic `CopyGateError::Io`.
#[test]
fn unlisted_control_file_with_divergent_written_size_refuses_generic_io() {
    let root = TempDir::new().expect("temp root");
    let ledger = ledger_listing_keeper(&root, true);
    let bytes = b"partial";
    let (mut input, mut output, _opened_size) = open_pair(&root, bytes);
    // Lie about the opened size: the written count cannot match.
    let gate = Some((&ledger, BACKUP_META_FILE_NAME));
    let error = stream_copy(&mut input, &mut output, bytes.len() as u64 + 5, gate)
        .expect_err("a divergent written size on an unlisted file must refuse");
    assert!(
        matches!(error, CopyGateError::Io(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof),
        "expected generic Io(UnexpectedEof), got: {error:?}"
    );
}

/// Kills the mutant that drops the BACKUP-path (no-ledger) opened-size
/// consistency check: with `gate = None` the pass must still refuse a
/// written count that diverges from the size observed at open (generic Io)
/// and succeed with an EMPTY digest (nothing hashed) when sizes agree —
/// `backup()` must never see a typed refusal from this path.
#[test]
fn no_ledger_backup_path_checks_opened_size_and_hashes_nothing() {
    let root = TempDir::new().expect("temp root");

    // Refusal half: written count diverges from opened size.
    let bytes = b"backup-payload";
    let (mut input, mut output, _opened_size) = open_pair(&root, bytes);
    let error = stream_copy(&mut input, &mut output, bytes.len() as u64 + 1, None)
        .expect_err("the backup path must refuse a divergent written size");
    assert!(
        matches!(error, CopyGateError::Io(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof),
        "expected generic Io(UnexpectedEof), got: {error:?}"
    );

    // Positive half: matching sizes pass with an empty digest.
    let (mut input, mut output, opened_size) = open_pair(&root, bytes);
    let (n, digest) =
        stream_copy(&mut input, &mut output, opened_size, None).expect("matching backup pass");
    assert_eq!(n, bytes.len() as u64);
    assert_eq!(digest, "", "the backup path hashes nothing");
}

/// Kills the mutant that reverts the streamed copy to default-permission
/// output (`fs::copy` carried the source mode over; a streamed `File::create`
/// turns a 0600 key into 0644 under the usual umask): the copy keeps the
/// source permission bits on the no-ledger backup path.
#[cfg(unix)]
#[test]
fn streamed_copy_preserves_source_permissions() {
    let root = TempDir::new().expect("temp root");
    let src = root.path().join("src");
    let dst = root.path().join("dst");
    fs::create_dir_all(&src).expect("create source dir");
    fs::create_dir_all(&dst).expect("create destination dir");
    let cases = [
        ("private.key", 0o600),
        ("helper.sh", 0o755),
        ("fixed.dat", 0o400),
    ];
    for (name, mode) in cases {
        let path = src.join(name);
        fs::write(&path, name.as_bytes()).expect("write source file");
        fs::set_permissions(&path, fs::Permissions::from_mode(mode)).expect("set source mode");
    }
    let (mut bytes, mut files) = (0u64, 0u64);
    copy_dir_recursive(&src, &dst, &mut bytes, &mut files, false, true, None)
        .expect("the backup-path copy succeeds");
    for (name, mode) in cases {
        let copied = fs::metadata(dst.join(name))
            .expect("copied file metadata")
            .permissions()
            .mode()
            & 0o7777;
        assert_eq!(copied, mode, "{name} must keep its source permission bits");
    }
}

/// Kills the mutant that drops the restore walk's non-Unicode-name refusal: a
/// name that is not valid Unicode is rejected by both verifiers and could alias
/// a listed entry through the ledger's lossy relative form, so the restore walk
/// refuses it before creating any output (the backup path, with no ledger,
/// still copies it: positive control). Linux only: other file systems refuse
/// to create such a name at all.
#[cfg(target_os = "linux")]
#[test]
fn restore_walk_refuses_a_non_unicode_name_but_backup_path_copies_it() {
    let root = TempDir::new().expect("temp root");
    let src = root.path().join("src");
    fs::create_dir_all(&src).expect("create source dir");
    let name = std::ffi::OsStr::from_bytes(b"f\xffo.bin");
    fs::write(src.join(name), b"rogue").expect("write non-Unicode named file");
    let ledger = RestoreCopyLedger::new(&src, &[], false);

    let restore_dst = root.path().join("restore-dst");
    fs::create_dir_all(&restore_dst).expect("create restore destination");
    let (mut bytes, mut files) = (0u64, 0u64);
    let error = copy_dir_recursive(
        &src,
        &restore_dst,
        &mut bytes,
        &mut files,
        false,
        true,
        Some(&ledger),
    )
    .expect_err("the restore walk must refuse a non-Unicode name");
    assert!(
        matches!(error, CopyGateError::UnmanifestedFile(_)),
        "expected UnmanifestedFile, got: {error:?}"
    );
    assert_eq!(
        fs::read_dir(&restore_dst)
            .expect("read restore destination")
            .count(),
        0,
        "no output may be created for the refused name"
    );

    let backup_dst = root.path().join("backup-dst");
    fs::create_dir_all(&backup_dst).expect("create backup destination");
    copy_dir_recursive(&src, &backup_dst, &mut bytes, &mut files, false, true, None)
        .expect("the backup path has no ledger and copies the name");
    assert_eq!(files, 1);
}

/// Kills the mutant that makes the ledger's `relative` mapping non-stable
/// (e.g. returning the absolute path or failing to strip the root): the
/// relative form of a walked path must be the slash-normalized
/// manifest-relative name the gate keys entries and errors by.
#[test]
fn ledger_relative_is_manifest_relative_slash_normalized() {
    let root = TempDir::new().expect("temp root");
    let ledger = ledger_listing_keeper(&root, false);
    let walked = root.path().join("payload").join("keeper.bin");
    assert_eq!(ledger.relative(&walked), "payload/keeper.bin");
}

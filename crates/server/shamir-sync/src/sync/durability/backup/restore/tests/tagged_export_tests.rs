//! Regression for the tagged-export restore path (finding 4, R2d-FIX-C
//! review): a scheduled snapshot exported by plain `cp` keeps its
//! timestamp-named directory and its `backup.meta.json` owner-tag sidecar,
//! but its NEW parent has no lease marker. `restore` must still succeed:
//! the snapshot is classified as an UNMANAGED export and read lock-free
//! (no shared lock, no marker creation — readers NEVER create the marker),
//! and the marker must not appear in the export parent afterwards. The
//! Unix-only variant makes the parent READ-ONLY (like read-only export
//! media), which is exactly the case a marker-creating or write-requiring
//! reader would fail on; the fixture is a real V1 snapshot whose manifest
//! LISTS the syntactically valid sidecar as a regular file (v1 refuses
//! unmanifested entries; only v2 exempts the sidecar), and the restore
//! genuinely succeeds into a fresh, writable target in a DIFFERENT parent
//! directory. Restore only ever READS the export (all writes go to the
//! staging dir next to the target), so read-only payload files are never
//! written. The Unix permission change is guarded by an RAII handle that
//! restores the original mode BEFORE the `TempDir` drops, so cleanup never
//! runs against a read-only parent. Pure file APIs, no runtime.

use std::fs;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

use tempfile::TempDir;

use crate::sync::durability::backup::offline::{
    verify_manifest, Manifest, ManifestFileEntry, SNAPSHOT_LEASE_FILE_NAME,
};
use crate::sync::durability::backup::online::copy_engine::hash_file;
use crate::sync::durability::backup::online::manifest as manifest_v2;
use crate::sync::durability::backup::online::manifest::BackupKind;
use crate::sync::durability::backup::online::manifest::BACKUP_META_FILE_NAME;
use crate::sync::durability::backup::restore::{restore_with_probe, RestoreError};

const LISTED_PAYLOAD: &str = "payload/keeper.bin";
const TIMESTAMPED_NAME: &str = "20200101_000001";
/// Multi-line literal per repo convention: JSON layout is free-form (the
/// consumers parse it; the sidecar is a listed payload file for restore).
const OWNER_TAG_JSON: &str = r#"{
  "scheduled_retention": {
    "version": 1,
    "owner": "sched-owner"
  }
}"#;
const PAYLOAD_BYTES: &[u8] = b"exported-kept-bytes";

/// A VALID v1 offline snapshot under a TIMESTAMPED directory name with a
/// syntactically valid owner-tag sidecar, in a parent WITHOUT the lease
/// marker (an exported copy). The manifest lists exactly the payload and
/// the sidecar — the v1 verifier refuses any unmanifested file, so the
/// sidecar must be a listed entry (v2 exempts it; v1 does not).
fn build_v1_tagged_export() -> TempDir {
    let parent = TempDir::new().expect("export parent temp dir");
    let snapshot = parent.path().join(TIMESTAMPED_NAME);
    // The payload lives under `payload/`, NOT `users/`: restore ALWAYS
    // opens `FjallUserDirectory::open(staged/users)` after the copy, and
    // whether a `users` directory holding a plain (non-Fjall) file opens
    // depends on Fjall's nonempty-directory init semantics. `payload/` is
    // never opened as a store, so the fixture makes no such assumption; a
    // missing `users` store is the documented path (restore materializes
    // an empty one — `open_and_invalidate_users`).
    fs::create_dir_all(snapshot.join("payload")).expect("snapshot payload dir");
    fs::write(snapshot.join(LISTED_PAYLOAD), PAYLOAD_BYTES).expect("write payload");
    fs::write(snapshot.join(BACKUP_META_FILE_NAME), OWNER_TAG_JSON).expect("write sidecar");

    let mut files = Vec::new();
    for rel in [LISTED_PAYLOAD, BACKUP_META_FILE_NAME] {
        let (sha256, size_bytes) = hash_file(&snapshot.join(rel)).expect("hash listed file");
        files.push(ManifestFileEntry {
            path: rel.to_string(),
            sha256,
            size_bytes,
        });
    }
    let manifest = Manifest {
        format_version: 1,
        created_at_unix_ns: 1_700_000_000_000,
        files,
    };
    let json = serde_json::to_vec_pretty(&manifest).expect("encode manifest");
    fs::write(snapshot.join("manifest.json"), json).expect("write manifest");
    parent
}

fn build_v2_offline_export() -> TempDir {
    let parent = TempDir::new().expect("v2 export parent temp dir");
    let snapshot = parent.path().join(TIMESTAMPED_NAME);
    fs::create_dir_all(snapshot.join("payload")).expect("snapshot payload dir");
    fs::write(snapshot.join(LISTED_PAYLOAD), PAYLOAD_BYTES).expect("write payload");
    let (sha256, size_bytes) = hash_file(&snapshot.join(LISTED_PAYLOAD)).expect("hash payload");
    let manifest = manifest_v2::ManifestV2 {
        format_version: 2,
        kind: BackupKind::Offline,
        created_at_unix_ns: 1_700_000_000_000,
        format_epoch: None,
        binary_version: None,
        consistency: None,
        wal: None,
        files: vec![ManifestFileEntry {
            path: LISTED_PAYLOAD.to_string(),
            sha256,
            size_bytes,
        }],
        extra: Default::default(),
    };
    fs::write(
        snapshot.join("manifest.json"),
        manifest_v2::encode(&manifest).expect("encode v2 offline manifest"),
    )
    .expect("write v2 offline manifest");
    parent
}

fn refresh_manifest(snapshot: &Path, version: u32) {
    fn walk(root: &Path, dir: &Path, files: &mut Vec<ManifestFileEntry>) {
        for entry in fs::read_dir(dir).expect("list fixture") {
            let path = entry.expect("fixture entry").path();
            if path.is_dir() {
                walk(root, &path, files);
            } else if path.file_name().is_some_and(|name| name != "manifest.json") {
                let rel = path.strip_prefix(root).expect("relative fixture path");
                let (sha256, size_bytes) = hash_file(&path).expect("hash fixture file");
                files.push(ManifestFileEntry {
                    path: rel.to_string_lossy().replace('\\', "/"),
                    sha256,
                    size_bytes,
                });
            }
        }
    }
    let mut files = Vec::new();
    walk(snapshot, snapshot, &mut files);
    if version == 1 {
        let manifest = Manifest {
            format_version: 1,
            created_at_unix_ns: 1_700_000_000_000,
            files,
        };
        fs::write(
            snapshot.join("manifest.json"),
            serde_json::to_vec_pretty(&manifest).expect("encode refreshed v1 manifest"),
        )
        .expect("write refreshed v1 manifest");
    } else {
        let manifest = manifest_v2::ManifestV2 {
            format_version: 2,
            kind: BackupKind::Offline,
            created_at_unix_ns: 1_700_000_000_000,
            format_epoch: None,
            binary_version: None,
            consistency: None,
            wal: None,
            files,
            extra: Default::default(),
        };
        fs::write(
            snapshot.join("manifest.json"),
            manifest_v2::encode(&manifest).expect("encode refreshed v2 manifest"),
        )
        .expect("write refreshed v2 manifest");
    }
}

fn install_allowed_catalogue(snapshot: &Path) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("fixture runtime");
    let store = runtime
        .block_on(shamir_db::shamir_db::system_store::SystemStore::init(
            shamir_db::shamir_db::system_store::SystemStoreConfig::Fjall(
                snapshot.join("shamir_db_meta.redb"),
            ),
        ))
        .expect("initialize allowed catalogue");
    let mut record = shamir_types::types::common::new_map();
    record.insert(
        "name".to_string(),
        shamir_types::types::value::QueryValue::Str("ordinary_db".to_string()),
    );
    runtime
        .block_on(store.save_database(
            "ordinary_db",
            &shamir_types::types::value::QueryValue::Map(record),
            &shamir_db::access::ResourceMeta::default(),
        ))
        .expect("save allowed database");
    drop(store);
    drop(runtime);
}

#[test]
fn v2_offline_public_restore_publishes_allowed_snapshot() {
    // Catches rejecting v2-offline manifests or misclassifying them as online snapshots.
    let export = build_v2_offline_export();
    let target_parent = TempDir::new().expect("target parent");
    let data_dir = target_parent.path().join("data_dir");
    let snapshot = export.path().join(TIMESTAMPED_NAME);
    let report = restore_with_probe(&snapshot, &data_dir, false, |_| Ok(()))
        .expect("valid v2-offline snapshot publishes");
    assert_eq!(report.files_restored, 2, "manifest and payload");
    assert_eq!(
        fs::read(data_dir.join(LISTED_PAYLOAD)).expect("restored payload"),
        PAYLOAD_BYTES
    );
}

/// Unix-only RAII guard: hold a directory read-only (0o555) for the test
/// body and restore the ORIGINAL mode on drop — BEFORE the enclosing
/// `TempDir` drops, so temp cleanup never hits a read-only parent. The
/// original mode is captured, never hard-coded: the guard restores whatever
/// the filesystem created, not an assumption about it.
#[cfg(unix)]
struct ReadOnlyDir<'a> {
    path: &'a Path,
    original: fs::Permissions,
}

#[cfg(unix)]
impl ReadOnlyDir<'_> {
    fn make_read_only(path: &Path) -> std::io::Result<ReadOnlyDir<'_>> {
        let original = fs::metadata(path)?.permissions();
        fs::set_permissions(path, fs::Permissions::from_mode(0o555))?;
        Ok(ReadOnlyDir { path, original })
    }
}

#[cfg(unix)]
impl Drop for ReadOnlyDir<'_> {
    fn drop(&mut self) {
        let _ = fs::set_permissions(self.path, self.original.clone());
    }
}

/// A tagged export (timestamped name + valid owner tag, NO marker in the
/// parent) must restore successfully, read lock-free: the export parent
/// gains NO lease marker (readers never create it) and the target parent is
/// left without any restore scratch sibling. Cross-platform: this is the
/// no-marker half of the read-only-media regression — on a reader that
/// classified the tagged export as managed and created the marker, the
/// final marker assertion fails.
#[test]
fn tagged_export_without_marker_restores_and_never_creates_the_marker() {
    let export = build_v1_tagged_export();
    let export_parent = export.path();
    let target_parent = TempDir::new().expect("target parent temp dir");
    // The target lives under a DIFFERENT parent than the export: the swap
    // staging dir must never be created beside the export.
    let data_dir = target_parent.path().join("data_dir");

    let snapshot_dir = export_parent.join(TIMESTAMPED_NAME);
    // The STANDALONE verifier (the same entry `restore`'s customers use
    // without a full restore) must accept the tagged export read-only: an
    // unmanaged export never takes a lease nor writes the marker.
    let verify = verify_manifest(&snapshot_dir).expect("standalone verify_manifest");
    assert!(
        matches!(verify.kind, BackupKind::Offline),
        "a v1 tagged export verifies as offline, got {:?}",
        verify.kind
    );
    // Immediately — BEFORE the restore runs — the export parent must hold
    // no marker: verification itself must not have created one.
    assert!(
        !export_parent.join(SNAPSHOT_LEASE_FILE_NAME).exists(),
        "standalone verify_manifest must NOT create the lease marker"
    );

    let report = restore_with_probe(&snapshot_dir, &data_dir, false, |_data_dir| Ok(()))
        .expect("a tagged export without a marker must restore as an unmanaged copy");

    // The copy walked the source tree: manifest, listed payload and listed
    // sidecar all landed in the restored target.
    assert_eq!(report.files_restored, 3, "manifest + payload + sidecar");
    assert_eq!(
        fs::read(data_dir.join(LISTED_PAYLOAD)).expect("read restored payload"),
        PAYLOAD_BYTES,
        "the restored payload must be the exported bytes"
    );
    assert_eq!(
        fs::read_to_string(data_dir.join(BACKUP_META_FILE_NAME)).expect("read restored sidecar"),
        OWNER_TAG_JSON,
        "the restored sidecar must be the exported owner tag"
    );

    // Readers never create the marker: the export parent must still hold
    // only the timestamped snapshot directory.
    assert!(
        !export_parent.join(SNAPSHOT_LEASE_FILE_NAME).exists(),
        "restore must NOT create the lease marker in the export parent"
    );
    let target_siblings: Vec<String> = fs::read_dir(target_parent.path())
        .expect("read target parent")
        .map(|entry| {
            entry
                .expect("dir entry")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .collect();
    assert!(
        target_siblings
            .iter()
            .all(|name| !name.contains("restore_tmp") && !name.contains("pre_restore_backup")),
        "a successful fresh-target restore leaves no scratch sibling, found: {target_siblings:?}"
    );
}

/// The same tagged export in a READ-ONLY parent (Unix export media) must
/// restore successfully: the reader takes no lease and never needs write
/// access to the export parent. Without the read-only-marker fix the
/// restore fails with an I/O refusal from a marker open/create before
/// reading anything.
#[cfg(unix)]
#[test]
fn tagged_export_in_read_only_parent_restores() {
    let export = build_v1_tagged_export();
    let export_parent = export.path();
    let _read_only = ReadOnlyDir::make_read_only(export_parent).expect("chmod export parent");
    let target_parent = TempDir::new().expect("target parent temp dir");
    let data_dir = target_parent.path().join("data_dir");

    let snapshot_dir = export_parent.join(TIMESTAMPED_NAME);
    let verify = verify_manifest(&snapshot_dir)
        .expect("standalone verify_manifest on a read-only tagged export");
    assert_eq!(
        verify.files_checked, 2,
        "manifest lists the payload and the sidecar"
    );
    assert!(
        !export_parent.join(SNAPSHOT_LEASE_FILE_NAME).exists(),
        "verification must not create the marker in the read-only parent"
    );

    let report = restore_with_probe(&snapshot_dir, &data_dir, false, |_data_dir| Ok(()))
        .expect("a tagged export in a read-only parent must restore lock-free");

    assert_eq!(report.files_restored, 3, "manifest + payload + sidecar");
    assert_eq!(
        fs::read(data_dir.join(LISTED_PAYLOAD)).expect("read restored payload"),
        PAYLOAD_BYTES,
        "the restored payload must be the exported bytes"
    );
    // The guard's Drop restored 0o755 before `export` drops; nothing was
    // ever written into the export parent — not even the marker.
    assert!(
        !export_parent.join(SNAPSHOT_LEASE_FILE_NAME).exists(),
        "restore must NOT create the lease marker in the read-only export parent"
    );
}

fn recursive_listing(root: &Path) -> Vec<(String, bool, u64)> {
    fn walk(root: &Path, current: &Path, out: &mut Vec<(String, bool, u64)>) {
        for entry in fs::read_dir(current).expect("list directory") {
            let entry = entry.expect("directory entry");
            let path = entry.path();
            let metadata = fs::symlink_metadata(&path).expect("entry metadata");
            out.push((
                path.strip_prefix(root)
                    .expect("relative path")
                    .to_string_lossy()
                    .into_owned(),
                metadata.is_dir(),
                metadata.len(),
            ));
            if metadata.is_dir() {
                walk(root, &path, out);
            }
        }
    }
    let mut listing = Vec::new();
    walk(root, root, &mut listing);
    listing.sort();
    listing
}

#[test]
fn public_restore_catalogue_presence_matrix_v1_and_v2_offline() {
    // Catches publishing corrupt, wrong-type, or unapproved catalogues and preserves the live target on refusal.
    for version in [1, 2] {
        let export = if version == 1 {
            build_v1_tagged_export()
        } else {
            build_v2_offline_export()
        };
        let snapshot = export.path().join(TIMESTAMPED_NAME);
        install_allowed_catalogue(&snapshot);
        refresh_manifest(&snapshot, version);
        let target_parent = TempDir::new().expect("valid target parent");
        let target = target_parent.path().join("live");
        fs::create_dir_all(&target).expect("create live target");
        fs::write(target.join("live.bin"), b"preserve me").expect("write live marker");
        restore_with_probe(&snapshot, &target, false, |_| Ok(()))
            .unwrap_or_else(|error| panic!("valid catalogue must publish for v{version}: {error}"));
        assert!(target.join("shamir_db_meta.redb").is_dir());

        for wrong_type in [false, true] {
            let export = if version == 1 {
                build_v1_tagged_export()
            } else {
                build_v2_offline_export()
            };
            let snapshot = export.path().join(TIMESTAMPED_NAME);
            if wrong_type {
                fs::write(snapshot.join("shamir_db_meta.redb"), b"not a directory")
                    .expect("write wrong-type catalogue");
            } else {
                fs::create_dir(snapshot.join("shamir_db_meta.redb"))
                    .expect("create invalid catalogue directory");
                fs::write(
                    snapshot.join("shamir_db_meta.redb/junk"),
                    b"not a Fjall fingerprint",
                )
                .expect("write invalid fingerprint");
                refresh_manifest(&snapshot, version);
            }
            let target_parent = TempDir::new().expect("refusal target parent");
            let target = target_parent.path().join("live");
            fs::create_dir_all(&target).expect("create live target");
            fs::write(target.join("live.bin"), b"preserve me").expect("write live marker");
            let before = recursive_listing(&target);
            let error = restore_with_probe(&snapshot, &target, false, |_| Ok(()))
                .expect_err("invalid catalogue must refuse");
            assert!(matches!(
                error,
                RestoreError::Catalogue(_)
                    | RestoreError::Consistency { .. }
                    | RestoreError::ManifestVerification(_)
            ));
            assert_eq!(
                recursive_listing(&target),
                before,
                "v{version} refusal changed target"
            );
        }
    }
}

#[test]
fn public_restore_rejects_legacy_reserved_catalogue_without_mutating_target_and_accepts_absent_catalogue(
) {
    // Catches removal of the pre-swap scan (publishes an unbootable home) and the old writing probe (persists last_started_at before refusal).
    let root = TempDir::new().expect("root");
    let home = root.path().join("home");
    fs::create_dir_all(&home).expect("home");
    crate::format_stamp::write_stamp(
        &home,
        &crate::format_stamp::FormatStamp::with_epoch(
            crate::format_stamp::CURRENT_FORMAT_EPOCH,
            "restore-catalogue-test",
            1_700_000_000_000,
        ),
    )
    .expect("write home stamp");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let store = runtime
        .block_on(shamir_db::shamir_db::system_store::SystemStore::init(
            shamir_db::shamir_db::system_store::SystemStoreConfig::Fjall(
                home.join("shamir_db_meta.redb"),
            ),
        ))
        .expect("initialize legacy catalogue");
    let mut record = shamir_types::types::common::new_map();
    record.insert(
        "name".to_string(),
        shamir_types::types::value::QueryValue::Str("users".to_string()),
    );
    runtime
        .block_on(store.save_database(
            "users",
            &shamir_types::types::value::QueryValue::Map(record),
            &shamir_db::access::ResourceMeta::default(),
        ))
        .expect("inject legacy reserved row");
    drop(store);
    drop(runtime);
    let snapshot = crate::sync::durability::backup::backup(&home, &root.path().join("snapshots"))
        .expect("backup legacy home")
        .dest_dir;
    let target = root.path().join("live");
    fs::create_dir_all(&target).expect("live target");
    fs::write(target.join("live.bin"), b"unchanged target").expect("live marker");
    let before = recursive_listing(&target);
    let error = restore_with_probe(&snapshot, &target, false, |_| Ok(()))
        .expect_err("legacy reserved catalogue must refuse before publication");
    assert!(matches!(
        error,
        RestoreError::Catalogue(shamir_db::DbError::Validation(_))
    ));
    assert_eq!(
        recursive_listing(&target),
        before,
        "refusal must preserve every target entry"
    );

    // Catches regressions that treat a catalogue-less generic backup as invalid rather than preserving prior restore behavior.
    let plain = root.path().join("plain");
    fs::create_dir_all(&plain).expect("plain source");
    crate::format_stamp::write_stamp(
        &plain,
        &crate::format_stamp::FormatStamp::with_epoch(
            crate::format_stamp::CURRENT_FORMAT_EPOCH,
            "restore-catalogue-test",
            1_700_000_000_000,
        ),
    )
    .expect("write plain stamp");
    fs::write(plain.join("payload.txt"), b"plain snapshot").expect("write payload");
    let plain_snapshot =
        crate::sync::durability::backup::backup(&plain, &root.path().join("plain-snapshots"))
            .expect("backup catalogue-less source")
            .dest_dir;
    let fresh = root.path().join("fresh");
    restore_with_probe(&plain_snapshot, &fresh, false, |_| Ok(()))
        .expect("catalogue-less snapshot publishes");
    assert_eq!(
        fs::read(fresh.join("payload.txt")).expect("restored payload"),
        b"plain snapshot"
    );
}

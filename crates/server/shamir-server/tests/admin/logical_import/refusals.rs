//! Typed incoherence/conflict refusals that pass verification but fail the
//! preflight, target and lock contract checks — each proves the typed
//! variant and that nothing is published and no lock or stage is left.

use shamir_server::logical_import::LogicalImportError;
use tokio_util::sync::CancellationToken;

use crate::logical_import::common::assert_no_leftovers;
use crate::logical_import::common::assert_target_untouched;
use crate::logical_import::common::build_fixture;
use crate::logical_import::common::export_args;
use crate::logical_import::common::export_now;
use crate::logical_import::common::handcrafted_bytes;
use crate::logical_import::common::handcrafted_bytes_for_database;
use crate::logical_import::common::import_args;
use crate::logical_import::common::import_args_as_db;
use crate::logical_import::common::import_now;
use crate::logical_import::common::runtime;
use crate::logical_import::common::write_bytes;

/// An artifact whose database owner names an identity that is not exported
/// fails the preflight coherence check; the target stays absent with no
/// lock or stage left behind.
#[test]
fn reserved_artifact_database_is_refused_before_stage_and_keeps_typed_error() {
    // Removing the preflight-name guard makes this artifact import proceed into the stage.
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_path_buf();
    let artifact = write_bytes(
        &root.join("reserved.mpack"),
        &handcrafted_bytes_for_database("users", None, None),
    );
    let target = root.join("target");
    let error = import_now(&import_args(&artifact, &target), CancellationToken::new()).unwrap_err();
    assert!(matches!(
        error,
        LogicalImportError::Engine(shamir_db::DbError::Validation(_))
    ));
    assert!(error.to_string().contains("reserved"));
    assert!(error.to_string().contains("users"));
    assert_no_leftovers(&root, &target);
}

#[test]
fn reserved_as_db_is_refused_before_lock_creation() {
    // Checking --as-db after lock creation leaves a stale lock file.
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_path_buf();
    let artifact = write_bytes(
        &root.join("app.mpack"),
        &handcrafted_bytes_for_database("app", None, None),
    );
    let target = root.join("target");
    let error = import_now(
        &import_args_as_db(&artifact, &target, "users"),
        CancellationToken::new(),
    )
    .unwrap_err();
    assert!(matches!(
        &error,
        LogicalImportError::Engine(shamir_db::DbError::Validation(_))
    ));
    assert!(error
        .to_string()
        .contains("database name 'users' is reserved by the server"));
    assert_no_leftovers(&root, &target);
}

#[test]
fn reserved_retry_name_does_not_mutate_existing_target() {
    // Reading the retry receipt before checking the effective name masks the typed refusal.
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_path_buf();
    let artifact = write_bytes(
        &root.join("app.mpack"),
        &handcrafted_bytes_for_database("app", None, None),
    );
    let target = root.join("target");
    import_now(&import_args(&artifact, &target), CancellationToken::new()).unwrap();
    let receipt = std::fs::read(target.join(".logical-import-receipt.json")).unwrap();
    let error = import_now(
        &import_args_as_db(&artifact, &target, "users"),
        CancellationToken::new(),
    )
    .unwrap_err();
    assert!(matches!(
        &error,
        LogicalImportError::Engine(shamir_db::DbError::Validation(_))
    ));
    assert!(error
        .to_string()
        .contains("database name 'users' is reserved by the server"));
    assert_eq!(
        std::fs::read(target.join(".logical-import-receipt.json")).unwrap(),
        receipt
    );
    assert_target_untouched(&root, &target);
}

#[test]
fn reserved_source_name_with_safe_override_imports() {
    // Checking the source name despite a safe --as-db rejects a valid routed import.
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_path_buf();
    let artifact = write_bytes(
        &root.join("users.mpack"),
        &handcrafted_bytes_for_database("users", None, None),
    );
    let target = root.join("target");
    let report = import_now(
        &import_args_as_db(&artifact, &target, "app2"),
        CancellationToken::new(),
    )
    .unwrap();
    assert_eq!(report.database, "app2");
    assert!(target.is_dir());
}

#[test]
fn unresolved_owner_is_refused_with_nothing_published() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_path_buf();
    let artifact = write_bytes(
        &root.join("ghost.mpack"),
        &handcrafted_bytes(Some("ghost"), None),
    );
    let target = root.join("target");
    let error = import_now(&import_args(&artifact, &target), CancellationToken::new()).unwrap_err();
    assert!(matches!(error, LogicalImportError::Preflight(_)));
    assert!(error.to_string().contains("not an exported identity"));
    assert_no_leftovers(&root, &target);
}

/// A record whose raw key is not the 16-byte record id is a typed preflight
/// refusal even though the frame verifies; nothing is published.
#[test]
fn raw_key_that_is_not_16_bytes_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_path_buf();
    let artifact = write_bytes(
        &root.join("short.mpack"),
        &handcrafted_bytes(None, Some(vec![1, 2, 3])),
    );
    let target = root.join("target");
    let error = import_now(&import_args(&artifact, &target), CancellationToken::new()).unwrap_err();
    assert!(matches!(error, LogicalImportError::Preflight(_)));
    assert!(error.to_string().contains("16-byte record id"));
    assert_no_leftovers(&root, &target);
}

/// An existing non-empty target directory is refused: no receipt, no
/// publish, target contents untouched, no lock or stage left behind. The
/// landed driver inspects the target only through the idempotent-retry
/// path, so this surfaces as the receipt refusal for a directory that was
/// never imported.
#[test]
fn non_empty_target_is_refused_without_touching_it() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_path_buf();
    let fixture = runtime().block_on(build_fixture(&root));
    drop(fixture.home);
    let artifact = root.join("dump.mpack");
    export_now(&root, &export_args("app", &artifact, false));
    let target = root.join("target");
    std::fs::create_dir_all(&target).unwrap();
    std::fs::write(target.join("keep.txt"), b"precious").unwrap();
    let error = import_now(&import_args(&artifact, &target), CancellationToken::new()).unwrap_err();
    assert!(matches!(error, LogicalImportError::Receipt(_)));
    assert_eq!(
        std::fs::read(target.join("keep.txt")).unwrap(),
        b"precious".to_vec(),
        "target contents untouched"
    );
    assert_target_untouched(&root, &target);
}

/// An existing `<target>.import.lock` refuses the import before the target
/// is even inspected; the pre-existing lock file is left exactly as found.
#[test]
fn existing_import_lock_refuses_the_attempt() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_path_buf();
    let fixture = runtime().block_on(build_fixture(&root));
    drop(fixture.home);
    let artifact = root.join("dump.mpack");
    export_now(&root, &export_args("app", &artifact, false));
    let target = root.join("target");
    let lock = root.join("target.import.lock");
    std::fs::write(&lock, b"held").unwrap();
    let error = import_now(&import_args(&artifact, &target), CancellationToken::new()).unwrap_err();
    assert!(matches!(error, LogicalImportError::Lock(_)));
    assert!(error.to_string().contains("import.lock"));
    assert_eq!(std::fs::read(&lock).unwrap(), b"held".to_vec());
    assert!(!target.exists());
    assert!(
        !std::fs::read_dir(&root).unwrap().any(|entry| entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with(".logical-import-")),
        "no stage without the lock"
    );
}

/// A complete target is accepted as an idempotent retry only when the
/// receipt matches; a target without a receipt (a foreign directory) is
/// refused instead of merged or overwritten.
#[test]
fn receipt_less_target_is_never_merged() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_path_buf();
    let fixture = runtime().block_on(build_fixture(&root));
    drop(fixture.home);
    let artifact = root.join("dump.mpack");
    export_now(&root, &export_args("app", &artifact, false));
    let target = root.join("target");
    import_now(&import_args(&artifact, &target), CancellationToken::new()).unwrap();
    // Remove the receipt: the next attempt must refuse, not re-verify.
    let receipt = target.join(".logical-import-receipt.json");
    if receipt.exists() {
        std::fs::remove_file(&receipt).unwrap();
    } else {
        panic!("published target should carry an import receipt");
    }
    let error = import_now(&import_args(&artifact, &target), CancellationToken::new()).unwrap_err();
    assert!(matches!(error, LogicalImportError::Receipt(_)));
    assert!(error.to_string().contains("without a"));
    assert!(target.join("shamir_db_meta.redb").exists(), "target intact");
    assert_target_untouched(&root, &target);
    // The removed receipt is the test's only mutation; restore semantics by
    // confirming the directory was NOT republished over.
    assert!(!receipt.exists());
}

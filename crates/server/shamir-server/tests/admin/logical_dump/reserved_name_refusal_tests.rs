use shamir_server::logical_dump;
use tokio_util::sync::CancellationToken;

use crate::logical_dump::fixture::{export_args, runtime};

#[test]
fn reserved_export_name_refused_before_writing_artifact() {
    // This catches moving the guard after MaintenanceOpen::open or replacing the typed error with a string.
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("not-a-directory");
    std::fs::write(&root, b"not a data directory").unwrap();
    let output = dir.path().join("reserved.dump");
    let args = export_args("__system__", &output, false);
    let error = runtime()
        .block_on(logical_dump::export(&root, &args, CancellationToken::new()))
        .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<shamir_db::DbError>(),
        Some(shamir_db::DbError::Validation(message))
            if message == "database name '__system__' is reserved by the server"
    ));
    assert!(!output.exists());
}

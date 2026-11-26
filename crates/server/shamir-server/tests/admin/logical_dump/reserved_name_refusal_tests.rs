use shamir_server::logical_dump;
use tokio_util::sync::CancellationToken;

use crate::logical_dump::fixture::{export_args, runtime};

#[test]
fn reserved_export_names_refused_before_open_without_over_refusing() {
    // Reserved names must fail typed validation before open; a safe name must reach the invalid-home open failure.
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("not-a-directory");
    std::fs::write(&root, b"not a data directory").unwrap();
    for name in ["__system__", "Users"] {
        let output = dir.path().join(format!("{name}.dump"));
        let args = export_args(name, &output, false);
        let error = runtime()
            .block_on(logical_dump::export(&root, &args, CancellationToken::new()))
            .unwrap_err();
        assert!(matches!(
            error.downcast_ref::<shamir_db::DbError>(),
            Some(shamir_db::DbError::Validation(message)) if message.contains(name)
        ));
        assert!(!output.exists());
    }
    let output = dir.path().join("app2.dump");
    let args = export_args("app2", &output, false);
    let error = runtime()
        .block_on(logical_dump::export(&root, &args, CancellationToken::new()))
        .unwrap_err();
    assert!(!matches!(
        error.downcast_ref::<shamir_db::DbError>(),
        Some(shamir_db::DbError::Validation(message)) if message.contains("reserved")
    ));
}

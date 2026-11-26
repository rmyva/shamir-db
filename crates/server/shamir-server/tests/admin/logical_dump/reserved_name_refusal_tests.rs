use std::path::Path;

use shamir_server::logical_dump;
use tokio_util::sync::CancellationToken;

use crate::logical_dump::fixture::{build_small_fixture, export_args, runtime};

#[test]
fn reserved_export_name_refused_before_writing_artifact() {
    // Removing the reserved-name check lets export open the fixture as a database target.
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_path_buf();
    let rt = runtime();
    let fixture = rt.block_on(build_small_fixture(&root));
    drop(fixture.home);
    let output = root.join("reserved.dump");
    let mut args = export_args("Users", &output, false);
    args.db = "Users".into();
    let error = rt
        .block_on(logical_dump::export(&root, &args, CancellationToken::new()))
        .unwrap_err();
    assert!(error.to_string().contains("reserved"));
    assert!(error.to_string().contains("Users"));
    assert!(!Path::new(&output).exists());
}

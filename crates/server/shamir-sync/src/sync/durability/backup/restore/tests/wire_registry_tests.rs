//! CI regression tests for `restore::wire_registry::validate_staged_wire_tables`.
//!
//! Mirrors the acceptance contract documented on that function: an absent
//! `wire_tables.mpack` and an empty one both pass; a valid MessagePack map
//! of `"db.repo"` string keys to arrays of table-name strings decodes; the
//! same map with non-string (u32) values refuses typed as
//! [`RestoreError::Consistency`] naming the pseudo-repository
//! `(wire_tables)`.

use std::time::Duration;

use super::super::wire_registry::validate_staged_wire_tables;
use crate::sync::durability::backup::restore::RestoreError;
use shamir_types::types::value::QueryValue;
use tempfile::TempDir;

const REAL_DATA_ROOT_ENTRIES: [&str; 20] = [
    "__system__",
    "shamir_db_meta.redb",
    "users",
    "server_meta",
    "cache",
    "counters",
    "audit",
    "audit_checkpoint",
    "api_tokens",
    "audit.log",
    "shamir_db_default_main.redb",
    "bootstrap_token.txt",
    "format.stamp",
    ".logical-import-receipt.json",
    ".shamir.lock",
    "wire_tables.mpack",
    "format.stamp.tmp",
    "shamir_db_meta.redb.shamirwal",
    "shamir_db_default_main.redb.shamirwal",
    ".shamir_restore_epoch",
];

#[test]
fn pinned_real_data_root_entries_are_refused_and_match_production() {
    // Removing a production name or adding a system store without pinning it would break this regression.
    assert_eq!(
        REAL_DATA_ROOT_ENTRIES.len(),
        shamir_db::RESERVED_DB_NAMES.len()
    );
    assert_eq!(
        REAL_DATA_ROOT_ENTRIES.as_slice(),
        &shamir_db::RESERVED_DB_NAMES,
        "removing a production name or adding a system store without updating the pin must fail",
    );
    for name in REAL_DATA_ROOT_ENTRIES {
        let repo = super::super::catalogue::CapturedRepo {
            db: "ordinary_db".into(),
            repo: "repo".into(),
            store_dir: format!("{name}/repo"),
            wal: (format!("wal-{name}/wal"), 0),
            v_star: 0,
        };
        assert!(
            super::super::catalogue::reject_reserved_store_route(&repo).is_err(),
            "{name}"
        );
        assert!(
            super::super::catalogue::reject_reserved_wal_dir(Some(name), &repo).is_err(),
            "{name}"
        );
    }
}

#[test]
fn reserved_store_components_match_shared_names_exactly() {
    // Removing a reserved store-route refusal while the independent pin still exists must fail this predicate test.
    for name in shamir_db::RESERVED_DB_NAMES {
        let repo = super::super::catalogue::CapturedRepo {
            db: "ordinary_db".into(),
            repo: "repo".into(),
            store_dir: format!("{name}/repo"),
            wal: (format!("wal-{name}/wal"), 0),
            v_star: 0,
        };
        assert!(super::super::catalogue::reject_reserved_store_route(&repo)
            .unwrap_err()
            .to_string()
            .contains(name));
    }
}

#[test]
fn store_route_accepts_ordinary_and_case_variant_components() {
    // An always-refuse implementation or case-folded literal route check must fail these positive controls.
    let repo = super::super::catalogue::CapturedRepo {
        db: "ordinary_db".into(),
        repo: "repo".into(),
        store_dir: "ordinary/repo".into(),
        wal: ("ordinary/wal".into(), 0),
        v_star: 0,
    };
    assert!(super::super::catalogue::reject_reserved_store_route(&repo).is_ok());
    let case_variant = super::super::catalogue::CapturedRepo {
        store_dir: "Users/repo".into(),
        ..repo
    };
    // Catches an always-refuse helper and verifies exact, case-sensitive route matching.
    assert!(super::super::catalogue::reject_reserved_store_route(&case_variant).is_ok());
}

#[test]
fn default_main_route_exception_is_exact() {
    let exact_store = super::super::catalogue::CapturedRepo {
        db: "default".into(),
        repo: "main".into(),
        store_dir: "shamir_db_default_main.redb".into(),
        wal: ("shamir_db_default_main.redb.shamirwal".into(), 0),
        v_star: 0,
    };
    assert!(super::super::catalogue::reject_reserved_store_route(&exact_store).is_ok());
    assert!(super::super::catalogue::reject_reserved_wal_dir(
        Some("shamir_db_default_main.redb.shamirwal"),
        &exact_store
    )
    .is_ok());

    // Catches the old first-component-only check, which admitted descendants of protected routes.
    for (store_dir, wal_route, wal_first) in [
        (
            "shamir_db_default_main.redb/nested",
            "ordinary/wal",
            "shamir_db_default_main.redb.shamirwal",
        ),
        (
            "ordinary/store",
            "shamir_db_default_main.redb.shamirwal/inner",
            "shamir_db_default_main.redb.shamirwal",
        ),
    ] {
        let repo = super::super::catalogue::CapturedRepo {
            store_dir: store_dir.into(),
            wal: (wal_route.into(), 0),
            ..exact_store.clone()
        };
        if store_dir.starts_with("shamir_db") {
            assert!(super::super::catalogue::reject_reserved_store_route(&repo).is_err());
        }
        if wal_route.starts_with("shamir_db") {
            assert!(
                super::super::catalogue::reject_reserved_wal_dir(Some(wal_first), &repo).is_err()
            );
        }
    }

    // Case-insensitive protected-shape detection refuses aliases that would collide on a case-insensitive volume.
    for (db, repo_name, store_dir, wal_route) in [
        (
            "default",
            "main",
            "SHAMIR_DB_DEFAULT_MAIN.REDB",
            "ordinary/wal",
        ),
        (
            "other",
            "else",
            "Shamir_Db_Default_Main.Redb",
            "ordinary/wal",
        ),
        (
            "default",
            "main",
            "ordinary/store",
            "Shamir_Db_Default_Main.Redb.shamirwal",
        ),
        (
            "other",
            "else",
            "ordinary/store",
            "Shamir_Db_Default_Main.Redb.shamirwal",
        ),
    ] {
        let repo = super::super::catalogue::CapturedRepo {
            db: db.into(),
            repo: repo_name.into(),
            store_dir: store_dir.into(),
            wal: (wal_route.into(), 0),
            v_star: 0,
        };
        if store_dir.eq_ignore_ascii_case("shamir_db_default_main.redb") {
            assert!(super::super::catalogue::reject_reserved_store_route(&repo).is_err());
        }
        if wal_route.starts_with("Shamir") {
            assert!(super::super::catalogue::reject_reserved_wal_dir(
                Some("Shamir_Db_Default_Main.Redb.shamirwal"),
                &repo
            )
            .is_err());
        }
    }

    // Catches widening the exception to any tuple on either exact protected route.
    let store_mismatches = [("default", "other"), ("other", "main")];
    for (db, repo_name) in store_mismatches {
        let repo = super::super::catalogue::CapturedRepo {
            db: db.into(),
            repo: repo_name.into(),
            store_dir: "shamir_db_default_main.redb".into(),
            wal: ("ordinary/wal".into(), 0),
            v_star: 0,
        };
        assert!(super::super::catalogue::reject_reserved_store_route(&repo).is_err());
    }
    let case_variant = super::super::catalogue::CapturedRepo {
        db: "default".into(),
        repo: "main".into(),
        store_dir: "Shamir_db_default_main.redb".into(),
        wal: ("ordinary/wal".into(), 0),
        v_star: 0,
    };
    assert!(super::super::catalogue::reject_reserved_store_route(&case_variant).is_err());

    let wal_mismatches = [
        ("default", "main", "shamir_db_default_main.redb"),
        ("default", "other", "shamir_db_default_main.redb.shamirwal"),
        ("other", "main", "shamir_db_default_main.redb.shamirwal"),
        ("default", "main", "Shamir_db_default_main.redb.shamirwal"),
    ];
    for (db, repo_name, wal_component) in wal_mismatches {
        let repo = super::super::catalogue::CapturedRepo {
            db: db.into(),
            repo: repo_name.into(),
            store_dir: "ordinary/store".into(),
            wal: (wal_component.into(), 0),
            v_star: 0,
        };
        assert!(
            super::super::catalogue::reject_reserved_wal_dir(Some(wal_component), &repo).is_err()
        );
    }
}

#[test]
fn wal_route_reserved_components_are_literal_and_case_sensitive() {
    // Removing literal reserved-route refusal fails below; the shortened reason assertion survives message edits.
    let repo = super::super::catalogue::CapturedRepo {
        db: "ordinary_db".into(),
        repo: "repo".into(),
        store_dir: "ordinary/store".into(),
        wal: ("users/wal".into(), 0),
        v_star: 0,
    };
    // A normalized comparison would wrongly reject the case variant used by server-produced routes.
    for name in shamir_db::RESERVED_DB_NAMES {
        assert!(
            super::super::catalogue::reject_reserved_wal_dir(Some(name), &repo)
                .unwrap_err()
                .to_string()
                .contains(name)
        );
    }
    let error = super::super::catalogue::reject_reserved_wal_dir(Some("users"), &repo).unwrap_err();
    assert!(matches!(error, RestoreError::Consistency { .. }));
    // Pin the error kind and useful identifiers, not a sentence that may be edited.
    if let RestoreError::Consistency { reason, .. } = error {
        assert!(reason.contains("wal_dir"));
        assert!(reason.contains("users"));
    }
    assert!(super::super::catalogue::reject_reserved_wal_dir(Some("Users"), &repo).is_ok());
    assert!(super::super::catalogue::reject_reserved_wal_dir(None, &repo).is_ok());
}

#[test]
fn nameless_catalogue_database_row_is_typed_corruption() {
    // Catches silently skipping malformed decoded rows, which could hide corrupted catalogue state.
    let mut row = shamir_types::types::common::new_map();
    row.insert("other".to_string(), QueryValue::Str("value".to_string()));
    let error =
        super::super::catalogue::validate_catalogue_db_name_records(&[QueryValue::Map(row)])
            .expect_err("nameless row is catalogue corruption");
    assert!(matches!(
        error,
        RestoreError::Catalogue(shamir_db::DbError::Validation(_))
    ));
}

#[tokio::test]
async fn staged_catalogue_name_scan_refuses_reserved_database_rows() {
    // Catches removal of the pre-publication refusal while bypassing create_db's reserved-name admission guard.
    let stage = TempDir::new().expect("temp dir");
    let store = shamir_db::shamir_db::system_store::SystemStore::init(
        shamir_db::shamir_db::system_store::SystemStoreConfig::Fjall(
            stage.path().join("shamir_db_meta.redb"),
        ),
    )
    .await
    .expect("initialize staged catalogue");
    let mut record = shamir_types::types::common::new_map();
    record.insert("name".to_string(), QueryValue::Str("users".to_string()));
    store
        .save_database(
            "users",
            &QueryValue::Map(record),
            &shamir_db::access::ResourceMeta::default(),
        )
        .await
        .expect("inject legacy reserved row");
    drop(store);
    let error = super::super::catalogue::validate_staged_db_names(stage.path())
        .await
        .expect_err("reserved catalogue row must refuse");
    assert!(matches!(
        &error,
        RestoreError::Catalogue(shamir_db::DbError::Validation(_))
    ));
    assert!(error
        .to_string()
        .contains("database name 'users' is reserved by the server"));
}

#[tokio::test]
async fn staged_catalogue_file_is_refused_as_wrong_type() {
    // Catches treating a present non-directory catalogue as absent after a copy walk ignores its type.
    let stage = TempDir::new().expect("temp dir");
    std::fs::write(stage.path().join("shamir_db_meta.redb"), b"not a directory")
        .expect("write wrong-type catalogue");
    let error = super::super::catalogue::validate_staged_db_names(stage.path())
        .await
        .expect_err("wrong-type catalogue must refuse");
    assert!(matches!(error, RestoreError::Consistency { .. }));
}

#[tokio::test]
async fn staged_catalogue_name_scan_accepts_internal_and_non_reserved_rows() {
    // Catches loss of the __system__ legacy-row exception or overbroad refusal of an ordinary database.
    let stage = TempDir::new().expect("temp dir");
    let store = shamir_db::shamir_db::system_store::SystemStore::init(
        shamir_db::shamir_db::system_store::SystemStoreConfig::Fjall(
            stage.path().join("shamir_db_meta.redb"),
        ),
    )
    .await
    .expect("initialize staged catalogue");
    for name in ["__system__", "users2"] {
        let mut record = shamir_types::types::common::new_map();
        record.insert("name".to_string(), QueryValue::Str(name.to_string()));
        store
            .save_database(
                name,
                &QueryValue::Map(record),
                &shamir_db::access::ResourceMeta::default(),
            )
            .await
            .expect("inject catalogue row");
    }
    let rows = store.load_databases().await.expect("load catalogue rows");
    assert!(rows
        .iter()
        .any(|row| row.get("name").and_then(QueryValue::as_str) == Some("__system__")));
    assert!(rows
        .iter()
        .any(|row| row.get("name").and_then(QueryValue::as_str) == Some("users2")));
    drop(store);
    assert!(super::super::catalogue::validate_catalogue_db_name_records(&rows).is_ok());
    assert!(
        super::super::catalogue::validate_staged_db_names(stage.path())
            .await
            .is_ok()
    );
}

/// Bounded so a wedged `tokio::fs::read` cannot hang CI.
const CALL_TIMEOUT: Duration = Duration::from_secs(10);

/// Encode `map` as MessagePack bytes the way the boot decoder would
/// accept a snapshot.
fn encode(map: &std::collections::BTreeMap<String, Vec<String>>) -> Vec<u8> {
    let mut buf = Vec::new();
    rmp_serde::encode::write(&mut buf, map).expect("encoding a valid map cannot fail");
    buf
}

#[tokio::test]
async fn absent_registry_is_accepted() {
    let stage = TempDir::new().expect("temp dir");
    let result = tokio::time::timeout(CALL_TIMEOUT, validate_staged_wire_tables(stage.path()))
        .await
        .expect("validation must not hang");
    assert!(
        result.is_ok(),
        "absent registry must be accepted: {result:?}"
    );
}

#[tokio::test]
async fn empty_registry_is_accepted() {
    let stage = TempDir::new().expect("temp dir");
    std::fs::write(stage.path().join("wire_tables.mpack"), b"").expect("write empty file");
    let result = tokio::time::timeout(CALL_TIMEOUT, validate_staged_wire_tables(stage.path()))
        .await
        .expect("validation must not hang");
    assert!(
        result.is_ok(),
        "empty registry must be accepted: {result:?}"
    );
}

#[tokio::test]
async fn valid_string_map_is_accepted() {
    let stage = TempDir::new().expect("temp dir");
    let mut map = std::collections::BTreeMap::new();
    map.insert(
        String::from("db.alpha"),
        vec![String::from("users"), String::from("sessions")],
    );
    map.insert(String::from("db.beta"), Vec::new());
    std::fs::write(stage.path().join("wire_tables.mpack"), encode(&map)).expect("write registry");
    let result = tokio::time::timeout(CALL_TIMEOUT, validate_staged_wire_tables(stage.path()))
        .await
        .expect("validation must not hang");
    assert!(result.is_ok(), "valid map must decode: {result:?}");
}

#[tokio::test]
async fn non_string_values_refuse_typed_consistency() {
    let stage = TempDir::new().expect("temp dir");
    // Same top-level shape, but the per-repo values are u32s instead of
    // arrays of strings — the boot decoder must reject this shape.
    let mut map = std::collections::BTreeMap::new();
    map.insert(String::from("db.alpha"), 7u32);
    let bytes = {
        let mut buf = Vec::new();
        rmp_serde::encode::write(&mut buf, &map).expect("encoding cannot fail");
        buf
    };
    std::fs::write(stage.path().join("wire_tables.mpack"), bytes).expect("write registry");
    let result = tokio::time::timeout(CALL_TIMEOUT, validate_staged_wire_tables(stage.path()))
        .await
        .expect("validation must not hang");
    match result {
        Err(RestoreError::Consistency { repo, reason: _ }) => {
            assert_eq!(repo, "(wire_tables)");
        }
        other => panic!("expected RestoreError::Consistency, got {other:?}"),
    }
}

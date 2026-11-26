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
use tempfile::TempDir;

#[test]
fn reserved_store_components_match_shared_names_exactly() {
    for name in shamir_db::RESERVED_DB_NAMES {
        let repo = super::super::catalogue::CapturedRepo {
            db: "ordinary_db".into(),
            repo: "repo".into(),
            store_dir: format!("{name}/repo"),
            wal: (format!("{name}/wal"), 0),
            v_star: 0,
        };
        assert!(super::super::catalogue::reject_reserved_store_dir(&repo)
            .unwrap_err()
            .to_string()
            .contains(name));
    }
}

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

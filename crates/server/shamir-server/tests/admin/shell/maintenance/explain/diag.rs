//! R5 PR5 shell maintenance diagnostics tests: the `diag wal` and
//! `diag durability` acceptance views (brief item 4) driven against the REAL
//! in-process server through the shared helpers of [`super::harness`].
//!
//! `diag wal` renders the shell's OWN copy of the status WAL projection
//! (`shell/commands/maintenance/diag.rs::project_wal`). The equivalent helper
//! in `shell/commands/state.rs` is private and widening an existing item is
//! forbidden, so the exact key set asserted here is the drift guard that
//! keeps the copy in step. `diag durability` is the `DescribeRepo`
//! projection with the fixed durability caveat.
//!
//! No test here asserts on timing: every assertion covers payload shape, the
//! stable command id and the wire op label only. A fixture failure reports
//! exit status and stream byte counts, never content.

use serde_json::Value;

use super::harness::{
    boot, overwrite_profile, run_shell, seed_catalogue, ShellRun, DB_NAME, REPO_NAME,
};
use super::state_data_common::{data, scram_profile};

/// The exact key set of the `wal` object in the shell's projection
/// (`shell/commands/maintenance/diag.rs::project_wal`). This list is the
/// drift guard for that copy: `state.rs`'s helper is private, and widening it
/// is forbidden, so the copy is pinned here instead.
const WAL_KEYS: [&str; 11] = [
    "backend_mem",
    "watermark_w",
    "recoverable_through",
    "pending_versions",
    "first_pending_version",
    "first_pending_age_ns",
    "sync_now_calls",
    "sync_rounds",
    "sync_coalesced",
    "sync_errors",
    "sync_duration_ns",
];

/// The launcher-created routing database, opened by every boot.
const BOOT_DB: &str = "default";

// ── Local shape helpers ──────────────────────────────────────────────

/// The single JSON Lines v1 record of one shell run.
fn record(run: &ShellRun) -> Value {
    let line = run
        .stdout
        .strip_suffix(b"\n")
        .expect("LF-terminated output");
    assert!(!line.contains(&b'\n'), "expected exactly one record");
    serde_json::from_slice(line).expect("invalid JSON record")
}

/// One leaf that must render as a NONEMPTY string.
fn nonempty_text(leaf: &Value) -> bool {
    leaf.as_str().is_some_and(|text| !text.is_empty())
}

/// One leaf that must render as a DECIMAL STRING (the shell's integer
/// rendering contract: a JSON string of ASCII digits, never a number).
fn decimal_text(leaf: &Value) -> bool {
    leaf.as_str()
        .is_some_and(|text| !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit()))
}

/// `true` when the `wal` object carries EXACTLY the eleven projection keys.
fn exact_wal_keys(wal: &serde_json::Map<String, Value>) -> bool {
    let mut observed = wal.keys().map(String::as_str).collect::<Vec<_>>();
    observed.sort_unstable();
    let mut expected = WAL_KEYS.to_vec();
    expected.sort_unstable();
    observed == expected
}

// ── Tests ────────────────────────────────────────────────────────────

/// `diag wal` is a VIEW of the observed status: exit 0, the stable command
/// id, the `Status` wire op, the fixed nonempty `source` and `caveat` notes,
/// and a nonempty `repositories` array. The boot-created `default` database
/// entry carries a `wal` object with exactly the eleven projection keys —
/// `backend_mem` a boolean, every other non-null leaf a decimal string.
#[test]
fn diag_wal_reports_the_status_wal_projection() {
    let fixture = boot("diag_wal_reports_the_status_wal_projection");
    let text = scram_profile(&fixture, "tls-tcp", &fixture.tls_addr.to_string());
    overwrite_profile(&fixture, &text);

    fixture.guard.mark("maintenance: diag wal");
    let run = run_shell(&fixture, &["--", "diag", "wal"]);
    let payload = data(&run, "diag wal");
    let envelope = record(&run);

    assert_eq!(
        envelope["command"].as_str(),
        Some("diag wal"),
        "diag wal lost its stable command id"
    );
    assert_eq!(
        envelope["op"].as_str(),
        Some("Status"),
        "diag wal must reuse the P1 Status op view"
    );
    assert!(
        nonempty_text(&payload["source"]),
        "diag wal `source` is not a nonempty string"
    );
    assert!(
        nonempty_text(&payload["caveat"]),
        "diag wal `caveat` is not a nonempty string"
    );
    let repositories = payload["repositories"]
        .as_array()
        .expect("diag wal `repositories` is not an array");
    assert!(
        !repositories.is_empty(),
        "diag wal reported no repositories"
    );

    // The boot-created database's repository: searched by identity, never
    // indexed, because the list is node-wide.
    let boot_repository = repositories
        .iter()
        .find(|row| row["db"].as_str() == Some(BOOT_DB))
        .expect("the boot-created database is absent from the WAL projection");
    // Drift guard: this is the shell's copy of the status projection
    // (`shell/commands/maintenance/diag.rs`); `state.rs`'s helper is private
    // and widening it is forbidden.
    let wal = boot_repository["wal"]
        .as_object()
        .expect("the boot repository `wal` block is not an object");
    assert!(
        exact_wal_keys(wal),
        "the `wal` block does not carry exactly the eleven projection keys"
    );
    for (key, leaf) in wal {
        if key == "backend_mem" {
            assert!(leaf.is_boolean(), "wal `backend_mem` is not a boolean");
        } else {
            assert!(
                leaf.is_null() || decimal_text(leaf),
                "wal `{key}` is neither null nor a decimal string"
            );
        }
    }
}

/// `diag durability REPO` is the `DescribeRepo` projection: exit 0, the
/// stable command id, the `DescribeRepo` wire op, the routing database label
/// exposed as `db`, the echoed `describe_repo`, an object `durability`, a
/// null-or-object `durability_override`, and a nonempty `caveat`.
#[test]
fn diag_durability_reports_the_describe_repo_projection() {
    let fixture = boot("diag_durability_reports_the_describe_repo_projection");
    seed_catalogue(&fixture);
    let text = scram_profile(&fixture, "tls-tcp", &fixture.tls_addr.to_string());
    overwrite_profile(&fixture, &text);

    fixture.guard.mark("maintenance: diag durability");
    let run = run_shell(&fixture, &["--", "diag", "durability", REPO_NAME]);
    let payload = data(&run, "diag durability");
    let envelope = record(&run);

    assert_eq!(
        envelope["command"].as_str(),
        Some("diag durability"),
        "diag durability lost its stable command id"
    );
    assert_eq!(
        envelope["op"].as_str(),
        Some("DescribeRepo"),
        "diag durability must reuse the DescribeRepo op view"
    );
    assert_eq!(
        payload["db"].as_str(),
        Some(DB_NAME),
        "diag durability lost the routing database label"
    );
    assert_eq!(
        payload["describe_repo"].as_str(),
        Some(REPO_NAME),
        "diag durability lost the requested repository name"
    );
    assert!(
        payload["durability"].is_object(),
        "diag durability `durability` is not an object"
    );
    assert!(
        payload["durability_override"].is_null() || payload["durability_override"].is_object(),
        "diag durability `durability_override` is neither null nor an object"
    );
    assert!(
        nonempty_text(&payload["caveat"]),
        "diag durability `caveat` is not a nonempty string"
    );
}

//! R5 PR5 shell maintenance tests: the online-copy trio (`backup start`,
//! `backup status`, `backup cancel`) driven against the REAL in-process
//! server through the shared helpers of [`super::harness`].
//!
//! These pin the online-copy command surface against the REAL server: the
//! typed status projection, the confirmation refusal before dispatch, the
//! server-side destination validation of an unmodified relative path, the
//! follower refusal, and the token refusal.

use std::path::Path;

use serde_json::Value;
use shamir_query_builder::token_admin;
use shamir_query_types::wire::token_admin::{ApiTokenChannels, ApiTokenMode};
use tempfile::TempDir;

use super::harness::{
    assert_exit, boot, boot_follower, overwrite_profile, profile_text, run_shell, seed_catalogue,
    seed_shell_user, ShellFixture, ShellRun, DB_NAME, SHELL_USER,
};
use super::secure_fixture::{boot_secure, provision_token, run_with_secret, write_secure_profile};
use super::state_data_common::scram_profile;

/// The launcher-created routing database. The follower fixture cannot be
/// seeded (a read-only node refuses the catalogue writes), so its profile
/// addresses the database every boot creates.
const DEFAULT_DB: &str = "default";

// ── Local shape helpers ──────────────────────────────────────────────

/// The single JSON Lines v1 record of one batch run: exactly one
/// LF-terminated line on stdout, wire version 1.
fn record(run: &ShellRun) -> Value {
    let line = run
        .stdout
        .strip_suffix(b"\n")
        .expect("LF-terminated output");
    assert!(!line.contains(&b'\n'), "expected exactly one record");
    let value = serde_json::from_slice::<Value>(&run.stdout).expect("invalid JSON record");
    assert_eq!(value["version"].as_u64(), Some(1), "wrong wire version");
    value
}

/// One batch run with the fixture's bootstrap secret scanned out of both
/// streams (the suite-wide leak discipline).
fn run_checked(fixture: &ShellFixture, args: &[&str]) -> ShellRun {
    let run = run_shell(fixture, args);
    assert!(
        !run.contains_canary(fixture.canary.as_str()),
        "the shell secret leaked"
    );
    run
}

/// One leaf that must render as a NONEMPTY string.
fn nonempty_text(leaf: &Value) -> bool {
    leaf.as_str().is_some_and(|text| !text.is_empty())
}

/// The DATA of one successful record: exit 0, the exact command id and op
/// label, `kind: result`, no error code, a data object and no batch
/// diagnostics.
fn result(fixture: &ShellFixture, args: &[&str], command: &str, op: &str) -> Value {
    let run = run_checked(fixture, args);
    assert_exit(&run, 0);
    let value = record(&run);
    assert_eq!(value["command"].as_str(), Some(command), "wrong command id");
    assert_eq!(
        value["kind"].as_str(),
        Some("result"),
        "not a result record"
    );
    assert_eq!(value["op"].as_str(), Some(op), "wrong op label");
    assert!(value["code"].is_null(), "a result record carries no code");
    assert!(
        value["data"].is_object(),
        "a result record carries no data object"
    );
    assert!(run.stderr.is_empty(), "unexpected batch diagnostics");
    value["data"].clone()
}

/// The whole ERROR record of one refused run: the exact exit, command id,
/// op label (`None` when the refusal happens before dispatch), code, and an
/// empty `data`.
fn refused_run(run: &ShellRun, command: &str, op: Option<&str>, code: &str, exit: i32) -> Value {
    assert_exit(run, exit);
    let value = record(run);
    assert_eq!(value["command"].as_str(), Some(command), "wrong command id");
    assert_eq!(value["kind"].as_str(), Some("error"), "not an error record");
    match op {
        Some(op) => assert_eq!(value["op"].as_str(), Some(op), "wrong op label"),
        None => assert!(
            value["op"].is_null(),
            "a pre-dispatch refusal carries no op"
        ),
    }
    assert_eq!(value["code"].as_str(), Some(code), "wrong refusal code");
    assert!(value["data"].is_null(), "an error record carries no data");
    value
}

/// [`refused_run`] for one command invoked against `fixture`.
fn refused(
    fixture: &ShellFixture,
    args: &[&str],
    command: &str,
    op: Option<&str>,
    code: &str,
    exit: i32,
) -> Value {
    let run = run_checked(fixture, args);
    refused_run(&run, command, op, code, exit)
}

/// Point the shell's profile at the bootstrap admin over TLS-TCP.
fn use_admin_profile(fixture: &ShellFixture) {
    let text = scram_profile(fixture, "tls-tcp", &fixture.tls_addr.to_string());
    overwrite_profile(fixture, &text);
}

/// Point a follower's profile at the bootstrap admin over TLS-TCP on the
/// launcher-created [`DEFAULT_DB`].
fn use_default_profile(fixture: &ShellFixture) {
    let text = profile_text(
        "tls-tcp",
        &fixture.tls_addr.to_string(),
        Some("localhost"),
        Some(DEFAULT_DB),
    )
    .replace("pin: <PIN>\n", &format!("pin: {}\n", fixture.pin_hex()));
    overwrite_profile(fixture, &text);
}

/// One absolute destination root inside a test-held [`TempDir`].
fn destination(dir: &TempDir) -> String {
    let path = dir.path().to_str().expect("the destination path is UTF-8");
    path.to_owned()
}

// ── Tests ────────────────────────────────────────────────────────────

#[test]
fn backup_status_scheduled_fixture_projects_the_legacy_key_set() {
    // (a) The EXACT frozen 16-field key list of the legacy `backup status`
    // projection, on a scheduled AND an unscheduled node; (b) the server's
    // optional `schedule` key must be ABSENT from the output; (c) the
    // schedule-independent fields must be EQUAL after normalising ONLY the
    // two per-fixture fields, each normalisation named at its site below.
    // Nothing here is compared byte-for-byte: payloads are parsed JSON.
    const LEGACY_KEYS: [&str; 16] = [
        "backup_id",
        "destination",
        "dest_dir",
        "started_at_unix_ns",
        "running",
        "cancel_requested",
        "stage",
        "failed",
        "failed_stage",
        "excluded_repos",
        "repos_in_snapshot",
        "repos_created_during",
        "repos",
        "progress_stage",
        "progress_started_at_unix_ns",
        "current_run_note",
    ];

    fn normalized_status(fixture: &ShellFixture, name: &str) -> serde_json::Map<String, Value> {
        let data = result(
            fixture,
            &["--", "backup", "status"],
            "backup status",
            "BackupStatus",
        );
        let mut object = data
            .as_object()
            .expect("backup status payload object")
            .clone();
        let keys: std::collections::BTreeSet<&str> = object.keys().map(String::as_str).collect();
        let expected: std::collections::BTreeSet<&str> = LEGACY_KEYS.into_iter().collect();
        assert_eq!(keys, expected, "{name}: legacy key set changed");
        assert!(
            !object.contains_key("schedule"),
            "{name}: the internal schedule key must not be projected"
        );
        // Normalisation 1: `destination` is the fixture's own data directory
        // (the registry seeds it from `data_dir`) — a per-fixture path.
        object.insert(
            "destination".into(),
            Value::String(String::from("<fixture-destination>")),
        );
        // Normalisation 2: `progress_started_at_unix_ns` is the wall-clock
        // timestamp `BackupProgress::new()` mints for the fixture's registry
        // record — per-fixture by construction.
        object.insert(
            "progress_started_at_unix_ns".into(),
            Value::String(String::from("<fixture-progress-start>")),
        );
        object
    }

    let scheduled = super::harness::boot_scheduled_backup(
        "backup_status_scheduled_fixture_projects_the_legacy_key_set",
    );
    seed_catalogue(&scheduled);
    use_admin_profile(&scheduled);
    let scheduled_status = normalized_status(&scheduled, "scheduled");

    let unscheduled = boot("backup_status_unscheduled_fixture_projects_the_legacy_key_set");
    seed_catalogue(&unscheduled);
    use_admin_profile(&unscheduled);
    let unscheduled_status = normalized_status(&unscheduled, "unscheduled");

    assert_eq!(
        scheduled_status, unscheduled_status,
        "schedule changed the projected status payload"
    );
}

/// A fresh fixture reports no running online copy: the typed status
/// projection answers `running: false` on a node that never started one.
/// Batch `start` is the ONLY export command that reaches the server on an
/// export-less node: batch `status`/`cancel` are refused by the dispatcher's
/// session-only gate before any dial (owned by
/// `export_online.rs::batch_status_cancel_are_session_only_refusals`), and
/// the interactive unavailable outcome of `status`/`cancel` is pinned at the
/// mapping layer (`shell/connect/tests/error_mapping.rs`).
#[test]
fn export_online_unconfigured_is_typed_without_path_or_secret() {
    let fixture = boot("export_online_unconfigured_is_typed_without_path_or_secret");
    seed_catalogue(&fixture);
    use_admin_profile(&fixture);
    let run = run_checked(
        &fixture,
        &["--", "export", "online", "start", DB_NAME, "--yes"],
    );
    refused_run(
        &run,
        "export online start",
        Some("StartExport"),
        "online_export_unavailable",
        5,
    );
    let output = String::from_utf8_lossy(&run.stdout);
    assert!(!output.contains('/'));
    assert!(!output.contains('\\'));
    assert!(!output.contains(fixture.canary.as_str()));
}

#[test]
fn backup_schedule_show_enabled_projects_schedule_schema() {
    let fixture = super::harness::boot_scheduled_backup(
        "backup_schedule_show_enabled_projects_schedule_schema",
    );
    seed_catalogue(&fixture);
    use_admin_profile(&fixture);
    let data = result(
        &fixture,
        &["--", "backup", "schedule", "show"],
        "backup schedule show",
        "BackupStatus",
    );
    assert_eq!(data["state"], "configured");
    assert_eq!(data["enabled"], true);
    assert_eq!(data["interval_seconds"], "3600");
    assert_eq!(data["window"], Value::Null);
    assert_eq!(data["keep_last"], "7");
    assert_eq!(data["run_on_start"], false);
    assert!(data["next_run"]
        .as_str()
        .is_some_and(|value| value.ends_with('Z') && value.contains('T')));
    assert_eq!(data["last_run"], Value::Null);
    let runs = data["runs"].as_object().expect("runs counters");
    assert_eq!(runs.len(), 8);
    for (name, value) in runs {
        let counter = value.as_str().expect("decimal-string counter");
        assert!(
            !counter.is_empty() && counter.bytes().all(|byte| byte.is_ascii_digit()),
            "runs.{name} is not decimal"
        );
    }
}

#[test]
fn backup_schedule_show_disabled_is_typed_success() {
    let fixture = boot("backup_schedule_show_disabled_is_typed_success");
    seed_catalogue(&fixture);
    use_admin_profile(&fixture);
    let data = result(
        &fixture,
        &["--", "backup", "schedule", "show"],
        "backup schedule show",
        "BackupStatus",
    );
    assert_eq!(
        data,
        serde_json::json!({"state": "schedule_not_configured"})
    );
}

#[test]
fn backup_status_reports_no_running_copy() {
    let fixture = boot("backup_status_reports_no_running_copy");
    seed_catalogue(&fixture);
    use_admin_profile(&fixture);

    let status = result(
        &fixture,
        &["--", "backup", "status"],
        "backup status",
        "BackupStatus",
    );
    assert_eq!(
        status["running"].as_bool(),
        Some(false),
        "a node that never started a copy reported a running one"
    );
}

/// An unconfirmed `backup start` is refused BEFORE any request: exit 6 with
/// the fixed `confirmation_required` code and no op label, and the node
/// still reports no running copy afterwards.
#[test]
fn backup_start_without_yes_is_refused_before_sending() {
    let destination_dir = TempDir::new().expect("destination tempdir");
    let fixture = boot("backup_start_without_yes_is_refused_before_sending");
    seed_catalogue(&fixture);
    use_admin_profile(&fixture);
    let to = destination(&destination_dir);

    refused(
        &fixture,
        &["--", "backup", "start", "--to", &to],
        "backup start",
        None,
        "confirmation_required",
        6,
    );
    let status = result(
        &fixture,
        &["--", "backup", "status"],
        "backup status",
        "BackupStatus",
    );
    assert_eq!(
        status["running"].as_bool(),
        Some(false),
        "an unconfirmed start must not leave a running copy"
    );
}

/// `backup start --yes` starts one copy: a nonempty backup id, the echoed
/// destination and its fixed note; a following status reports THAT run's
/// id (the registry keeps the last run's id, so equality still holds once
/// the copy has finished) and a decimal-string `started_at_unix_ns` that
/// is NOT the idle-node placeholder `"0"`. The test then cancels the copy
/// and waits, bounded, for it to reach a terminal state (no timing
/// assertion).
#[test]
fn backup_start_with_yes_starts_a_copy() {
    let destination_dir = TempDir::new().expect("destination tempdir");
    let fixture = boot("backup_start_with_yes_starts_a_copy");
    seed_catalogue(&fixture);
    use_admin_profile(&fixture);
    let to = destination(&destination_dir);

    let started = result(
        &fixture,
        &["--", "backup", "start", "--to", &to, "--yes"],
        "backup start",
        "StartBackup",
    );
    assert!(
        nonempty_text(&started["backup_id"]),
        "backup start returned no backup id"
    );
    let started_id = started["backup_id"]
        .as_str()
        .expect("backup start returned no backup id")
        .to_owned();
    assert!(
        started["destination"].is_string(),
        "backup start lost its destination"
    );
    assert!(
        nonempty_text(&started["destination_note"]),
        "backup start lost its destination note"
    );

    let status = result(
        &fixture,
        &["--", "backup", "status"],
        "backup status",
        "BackupStatus",
    );
    assert_eq!(
        status["backup_id"].as_str(),
        Some(started_id.as_str()),
        "backup status did not report the run the shell started"
    );
    let started_at = status["started_at_unix_ns"]
        .as_str()
        .expect("started_at_unix_ns is not a decimal string");
    assert!(
        !started_at.is_empty() && started_at.bytes().all(|byte| byte.is_ascii_digit()),
        "started_at_unix_ns is not a decimal string"
    );
    assert_ne!(
        status["started_at_unix_ns"].as_str(),
        Some("0"),
        "backup status reported the idle-node placeholder started_at_unix_ns"
    );

    let cancelled = result(
        &fixture,
        &["--", "backup", "cancel", "--yes"],
        "backup cancel",
        "CancelBackup",
    );
    assert!(
        cancelled["cancelled"].is_boolean(),
        "the cancel outcome is not a boolean"
    );

    // bounded wait for the run to reach a terminal state; not a latency assertion
    let mut polls = 0usize;
    let mut last_stdout = 0usize;
    let mut last_stderr = 0usize;
    let mut running = true;
    while running && polls < 300 {
        if polls > 0 {
            std::thread::sleep(std::time::Duration::from_millis(200));
        }
        polls += 1;
        let run = run_checked(&fixture, &["--", "backup", "status"]);
        last_stdout = run.stdout.len();
        last_stderr = run.stderr.len();
        assert_exit(&run, 0);
        let polled = record(&run);
        assert_eq!(
            polled["op"].as_str(),
            Some("BackupStatus"),
            "wrong op label"
        );
        running = polled["data"]["running"]
            .as_bool()
            .expect("backup status did not report a boolean `running`");
    }
    assert!(
        !running,
        "the copy did not reach a terminal state within {polls} status polls; \
         last status stdout {last_stdout} bytes, stderr {last_stderr} bytes",
    );
    fixture
        .guard
        .mark("bounded wait: the online copy reached a terminal state");
}

/// An unconfirmed `backup cancel` is refused locally: exit 6 with the fixed
/// `confirmation_required` code and no op label.
#[test]
fn backup_cancel_without_yes_is_refused() {
    let fixture = boot("backup_cancel_without_yes_is_refused");
    seed_catalogue(&fixture);
    use_admin_profile(&fixture);

    refused(
        &fixture,
        &["--", "backup", "cancel"],
        "backup cancel",
        None,
        "confirmation_required",
        6,
    );
}

/// `backup cancel --yes` answers the node's current run: the typed boolean
/// outcome and the fixed run-scoped note (`cancel` carries no id), and a
/// following status still reports the run the shell started — so the
/// `cancelled` leaf is not an idle-node `false`.
#[test]
fn backup_cancel_with_yes_answers_the_current_run() {
    let destination_dir = TempDir::new().expect("destination tempdir");
    let fixture = boot("backup_cancel_with_yes_answers_the_current_run");
    seed_catalogue(&fixture);
    use_admin_profile(&fixture);
    let to = destination(&destination_dir);

    let started = result(
        &fixture,
        &["--", "backup", "start", "--to", &to, "--yes"],
        "backup start",
        "StartBackup",
    );
    assert!(
        nonempty_text(&started["backup_id"]),
        "backup start returned no backup id"
    );
    let started_id = started["backup_id"]
        .as_str()
        .expect("backup start returned no backup id")
        .to_owned();
    let cancelled = result(
        &fixture,
        &["--", "backup", "cancel", "--yes"],
        "backup cancel",
        "CancelBackup",
    );
    assert!(
        cancelled["cancelled"].is_boolean(),
        "the cancel outcome is not a boolean"
    );
    assert!(
        nonempty_text(&cancelled["current_run_note"]),
        "cancel lost its current-run note"
    );
    let status = result(
        &fixture,
        &["--", "backup", "status"],
        "backup status",
        "BackupStatus",
    );
    assert_eq!(
        status["backup_id"].as_str(),
        Some(started_id.as_str()),
        "backup status does not report the run the shell started and cancelled"
    );
}

/// A RELATIVE destination is passed UNMODIFIED and refused by the server's
/// own policy: exit 5 with `backup_invalid_destination`, and the shell never
/// created or resolved the path.
#[test]
fn relative_destination_is_refused_by_the_server_unmodified() {
    let fixture = boot("relative_destination_is_refused_by_the_server_unmodified");
    seed_catalogue(&fixture);
    use_admin_profile(&fixture);

    refused(
        &fixture,
        &["--", "backup", "start", "--to", "relative-dest", "--yes"],
        "backup start",
        Some("StartBackup"),
        "backup_invalid_destination",
        5,
    );
    assert!(
        !Path::new("relative-dest").exists(),
        "the shell created or resolved the relative destination"
    );
}

/// A follower (read-only node) refuses the node-local start/cancel with the
/// stable `online_backup_follower_read_only` code; the read-only status view
/// still answers.
#[test]
fn follower_refuses_start_and_cancel() {
    let destination_dir = TempDir::new().expect("destination tempdir");
    let fixture = boot_follower("follower_refuses_start_and_cancel");
    use_default_profile(&fixture);
    let to = destination(&destination_dir);

    refused(
        &fixture,
        &["--", "backup", "start", "--to", &to, "--yes"],
        "backup start",
        Some("StartBackup"),
        "online_backup_follower_read_only",
        5,
    );
    refused(
        &fixture,
        &["--", "backup", "cancel", "--yes"],
        "backup cancel",
        Some("CancelBackup"),
        "online_backup_follower_read_only",
        5,
    );
    result(
        &fixture,
        &["--", "backup", "status"],
        "backup status",
        "BackupStatus",
    );
}

/// §3.3: the maintenance commands are SCRAM only. Every online-copy command
/// (and the WAL diagnostic) is refused in a token session with the fixed
/// `token_request_denied` code and exit 4 BEFORE any request: the empty
/// `data` and the absent op label are the pre-dispatch evidence (the
/// counting-relay proof lives in `state_data::token_subset`). This also pins
/// the gate ORDER: the token refusal precedes both the explained payload and
/// the unsupported refusal, and no request is sent.
#[test]
fn token_session_is_refused_with_no_request() {
    let destination_dir = TempDir::new().expect("destination tempdir");
    let secure = boot_secure("token_session_is_refused_with_no_request");
    let fixture = &secure.shell;
    seed_catalogue(fixture);
    seed_shell_user(fixture);
    let token = provision_token(
        fixture,
        token_admin::create_api_token("https")
            .for_user(SHELL_USER)
            .databases([DB_NAME])
            .mode(ApiTokenMode::ReadOnly)
            .channels(ApiTokenChannels::Both),
    );
    write_secure_profile(
        fixture,
        "https",
        secure.http_addr,
        DB_NAME,
        Some(&secure.ca_path),
    );
    let to = destination(&destination_dir);
    let start = ["--", "backup", "start", "--to", to.as_str(), "--yes"];

    for (args, command) in [
        (&["--", "backup", "status"][..], "backup status"),
        (&start[..], "backup start"),
        (&["--", "backup", "cancel", "--yes"][..], "backup cancel"),
        (&["--", "backup", "verify", "SNAPSHOT"][..], "backup verify"),
        (&["--", "backup", "restore"][..], "backup restore"),
        (&["--", "backup", "snapshot"][..], "backup snapshot"),
        (
            &["--", "backup", "schedule", "show"][..],
            "backup schedule show",
        ),
        (&["--", "diag", "wal"][..], "diag wal"),
        (&["--", "diag", "durability", "main"][..], "diag durability"),
        (&["--", "diag", "integrity"][..], "diag integrity"),
        (&["--", "version", "stamp"][..], "version stamp"),
        (&["--", "version", "migrate"][..], "version migrate"),
        (
            &[
                "--",
                "schema-migration",
                "status",
                "--database-id",
                "1111111111111111",
            ][..],
            "schema-migration status",
        ),
        (&["--", "export", "logical"][..], "export logical"),
        (&["--", "import", "logical"][..], "import logical"),
    ] {
        fixture.guard.mark(format!("token: {}", args.join(" ")));
        let run = run_with_secret(fixture, token.clone(), true, args);
        assert!(
            !run.contains_canary(token.as_str()),
            "the token leaked in a refusal"
        );
        refused_run(&run, command, None, "token_request_denied", 4);
    }
}

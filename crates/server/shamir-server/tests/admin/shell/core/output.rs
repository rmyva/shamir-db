//! Offline / output-contract acceptance for the administrative shell
//! (R5 PR1, design `docs/dev-artifacts/design/admin-shell.md` §§2, 5, 8)
//! against the REAL production binary: one LF-terminated JSON record per
//! local command; typed POST-parse errors carry the FULL golden record;
//! `--output-version` precedes any dial; no server config reads. The
//! scripted loop captures disable → write → flush → read → restore; the
//! direct prompt-seam tests feed the fake RAW bytes in every Enter shape
//! (CR, LF, CRLF), split with the PRODUCTION `is_hidden_enter` rule. The
//! shared bounded reader keeps interior CRs and enforces the cap. The
//! canary never reaches the profile home; no history file exists.
//!
//! Captured child bytes are never formatted into a panic message.

use std::cell::RefCell;
use std::io::{Cursor, Write};
use std::path::Path;
use std::rc::Rc;
use std::time::Duration;

use tempfile::TempDir;
use zeroize::Zeroizing;

use serde_json::Value as Json;

use super::harness;
use crate::shell::output::ShellError;
use crate::shell::terminal::secret::read_secret_with_writer;
use crate::shell::terminal::secret::HiddenLineSession;
use crate::shell::terminal::secret::SecretSource;
use crate::shell::terminal::secret::Terminal;

// ── Named constants ──────────────────────────────────────────────────

/// Hang-detector bound for direct binary runs (CI Windows runners are up
/// to 10x slower than a laptop — not a latency assertion).
const BINARY_BOUND: Duration = Duration::from_secs(120);

/// Exact §3.2 typed refusal for a secret on argv.
const ARGV_SECRET_CODE: &str = "secret_on_argv_refused";
const ARGV_SECRET_MESSAGE: &str =
    "secrets on the command line are refused; use --secret-stdin, --secret-fd, or --secret-env";

/// Exact §5 typed refusal for an unsupported `--output-version`.
const OUTPUT_VERSION_CODE: &str = "unsupported_output_version";
const OUTPUT_VERSION_MESSAGE: &str = "unsupported --output-version value; only 1 is supported";

/// Exact §5 typed refusal for interactive mode without a terminal.
const MISSING_TTY_CODE: &str = "interactive_mode_unavailable";

/// Exact §5 usage error.
const USAGE_CODE: &str = "usage";
const USAGE_MESSAGE: &str = "invalid command usage";

/// Exact typed codes of `ShellError::InvalidSecret` / `ShellError::Connection`
/// (`shell/output.rs` `ShellError::code()`; exit 6 and 2 respectively).
const INVALID_SECRET_CODE: &str = "invalid_secret";
const CONNECTION_CODE: &str = "connection_failed";

// ── Direct binary runner ─────────────────────────────────────────────

/// One direct run of the production binary via the harness's sanitized
/// env helper (`server_command`), bounded by [`BINARY_BOUND`]; stdin is
/// piped and closed at once: no probe reads stdin.
fn run_binary(cwd: &Path, env: Option<(&'static str, String)>, args: &[&str]) -> harness::ShellRun {
    let cwd = cwd.to_path_buf();
    let args: Vec<String> = args.iter().map(ToString::to_string).collect();
    let runtime = harness::TestRuntime::new();
    runtime.run(async move {
        let mut command = harness::server_command();
        command
            .args(&args)
            .current_dir(&cwd)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);
        if let Some((key, value)) = env {
            command.env(key, value);
        }
        let mut child = command.spawn().expect("spawn server binary child");
        // Offline probes never read stdin; closing the pipe at once is
        // both the EOF the contract expects and the no-secret discipline.
        drop(child.stdin.take());
        match tokio::time::timeout(BINARY_BOUND, child.wait_with_output()).await {
            // A pipe-capture failure is a typed local panic, never a
            // misread of a half-collected `Output`.
            Ok(Ok(output)) => harness::ShellRun {
                exit_code: output.status.code().unwrap_or(-1),
                stdout: output.stdout,
                stderr: output.stderr,
            },
            Ok(Err(_)) => panic!(
                "binary run failed while capturing its streams (io error); \
                 no exit code or stream bytes are available"
            ),
            Err(_) => panic!(
                "binary run exceeded its {BINARY_BOUND:?} hang bound and was killed \
                 (bound is a hang detector, not a latency assertion)"
            ),
        }
    })
}

#[cfg(test)]
#[path = "output_goldens.rs"]
mod output_goldens;

// ── Record / envelope assertion helpers ──────────────────────────────

/// Parse stdout as EXACTLY one LF-terminated JSON record (the §5 batch
/// contract); captured bytes never enter a failure message.
fn single_lf_record(label: &str, bytes: &[u8]) -> Json {
    assert!(
        bytes.last() == Some(&b'\n'),
        "{label}: stdout must end with a single LF terminator"
    );
    assert_eq!(
        bytes.iter().filter(|byte| **byte == b'\n').count(),
        1,
        "{label}: stdout must carry exactly one LF-terminated record"
    );
    match serde_json::from_slice(&bytes[..bytes.len() - 1]) {
        Ok(record) => record,
        Err(_) => panic!(
            "{label}: stdout is not one valid JSON record ({} bytes)",
            bytes.len()
        ),
    }
}

/// A success record: envelope + structured `data` object on stdout, and
/// stderr silent.
fn assert_local_result(
    label: &str,
    run: &harness::ShellRun,
    command: &str,
    op: Option<&str>,
) -> Json {
    assert_eq!(run.exit_code, 0, "{label}: local command must exit 0");
    let record = single_lf_record(label, &run.stdout);
    let expected = serde_json::json!({
        "version": 1,
        "command": command,
        "kind": "result",
        "op": op,
        "node": Json::Null,
    });
    let expected = expected
        .as_object()
        .expect("golden envelope constant is a JSON object");
    for (field, want) in expected {
        assert!(
            record.get(field) == Some(want),
            "{label}: envelope field {field} must match the v1 golden value"
        );
    }
    assert!(
        record["data"].is_object(),
        "{label}: data must be a structured object"
    );
    assert!(
        run.stderr.is_empty(),
        "{label}: stderr must stay silent for a local result ({} bytes)",
        run.stderr.len()
    );
    record
}

/// A typed POST-command-parse error record: FULL golden equality; the
/// record is never formatted into a failure message (golden values are
/// locally authored constants).
fn assert_typed_error(
    label: &str,
    run: &harness::ShellRun,
    exit_code: i32,
    command: &str,
    op: Option<&str>,
    code: &str,
    message: &str,
) {
    assert_eq!(
        run.exit_code, exit_code,
        "{label}: exit code per the §5 table"
    );
    let record = single_lf_record(label, &run.stdout);
    let expected = serde_json::json!({
        "version": 1,
        "command": command,
        "kind": "error",
        "op": op,
        "node": Json::Null,
        "data": Json::Null,
        "code": code,
        "message": message,
    });
    let expected = expected
        .as_object()
        .expect("golden typed-error constant is a JSON object");
    assert_eq!(
        record.as_object().map(|fields| fields.len()),
        Some(expected.len()),
        "{label}: error record must carry exactly the golden field set"
    );
    for (field, want) in expected {
        assert!(
            record.get(field) == Some(want),
            "{label}: error field {field} must match"
        );
    }
}

/// A PRE-command-parse refusal: fixed code/message on stderr ONLY — no
/// stdout record was earned. Captured bytes are checked by containment,
/// never formatted (a refusal could reflect the synthetic argv canary).
fn assert_pre_refusal(
    label: &str,
    run: &harness::ShellRun,
    exit_code: i32,
    code: &str,
    message: &str,
) {
    assert_eq!(
        run.exit_code, exit_code,
        "{label}: exit code per the §5 table"
    );
    assert!(
        run.stdout.is_empty(),
        "{label}: stdout must stay silent (no record was earned; {} bytes)",
        run.stdout.len()
    );
    let stderr = String::from_utf8_lossy(&run.stderr);
    assert!(
        stderr.contains(code) && stderr.contains(message),
        "{label}: stderr must carry the fixed refusal code and message"
    );
}

/// Canary discipline for a run: the secret must appear in NEITHER
/// captured stream (checked on raw bytes, never on lossy text).
fn assert_no_canary(run: &harness::ShellRun, canary: &str) {
    assert!(
        !run.contains_canary(canary),
        "canary secret leaked into a captured stream (checked on raw bytes)"
    );
}

/// Secret-leak scan: canary in NO profile-home file, no history file.
fn assert_no_canary_on_disk(fixture: &harness::ShellFixture) {
    let canary = fixture.canary.as_bytes();
    let mut stack = vec![fixture.profile_home.path().to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).expect("read profile home entry") {
            let path = entry.expect("profile home entry").path();
            if path.is_dir() {
                stack.push(path);
                continue;
            }
            assert!(
                !contains_bytes(
                    &std::fs::read(&path).expect("read profile-home file"),
                    canary
                ),
                "canary secret found in file {}",
                path.display()
            );
            assert!(
                !path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.to_lowercase().contains("history")),
                "history file must not exist: {}",
                path.display()
            );
        }
    }
}

fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
    !needle.is_empty()
        && needle.len() <= haystack.len()
        && haystack
            .windows(needle.len())
            .any(|window| window == needle)
}

// ── Tests ────────────────────────────────────────────────────────────

#[test]
fn profile_list_emits_one_v1_lf_record() {
    let fixture = harness::boot("shell_output_profile_list");
    harness::write_profile(&fixture);
    let run = harness::run_shell_ok(&fixture, &["--", "profile", "list"]);
    assert_local_result("profile list", &run, "profile list", None);
    assert_no_canary(&run, &fixture.canary);
    assert_no_canary_on_disk(&fixture);
}

#[test]
fn profile_show_emits_one_v1_lf_record() {
    let fixture = harness::boot("shell_output_profile_show");
    harness::write_profile(&fixture);
    let run = harness::run_shell_ok(&fixture, &["--", "profile", "show", harness::PROFILE_NAME]);
    assert_local_result("profile show", &run, "profile show", None);
    assert_no_canary(&run, &fixture.canary);
    assert_no_canary_on_disk(&fixture);
}

#[test]
fn argv_secret_refused_with_exact_typed_error() {
    let fixture = harness::boot("shell_output_argv_secret");
    harness::write_profile(&fixture);
    // A SYNTHETIC canary, never the fixture credential: the argv refusal
    // path is exercised without placing the real secret on a command line.
    let synthetic = Zeroizing::new(format!(
        "synthetic-argv-canary-{:x}@{}",
        std::process::id(),
        fixture.tls_addr.port()
    ));
    let run = harness::run_shell_refused(
        &fixture,
        &["--password", synthetic.as_str(), "--", "profile", "list"],
    );
    assert_pre_refusal(
        "argv secret",
        &run,
        6,
        ARGV_SECRET_CODE,
        ARGV_SECRET_MESSAGE,
    );
    assert_no_canary(&run, synthetic.as_str());
    assert_no_canary(&run, &fixture.canary);
    assert_no_canary_on_disk(&fixture);
}

#[test]
fn attached_argv_secret_flag_refused_with_same_typed_error() {
    let fixture = harness::boot("shell_output_argv_secret_attached");
    harness::write_profile(&fixture);
    let synthetic = Zeroizing::new(format!(
        "synthetic-argv-canary-attached-{:x}@",
        std::process::id()
    ));
    let attached = format!("--password={}", synthetic.as_str());
    let run = harness::run_shell_refused(&fixture, &[attached.as_str(), "--", "profile", "list"]);
    assert_pre_refusal(
        "attached argv secret",
        &run,
        6,
        ARGV_SECRET_CODE,
        ARGV_SECRET_MESSAGE,
    );
    assert_no_canary(&run, synthetic.as_str());
    assert_no_canary(&run, &fixture.canary);
    assert_no_canary_on_disk(&fixture);
}

#[test]
fn explicit_output_version_1_succeeds_on_live_ping() {
    let fixture = harness::boot("shell_output_version_1_live");
    // LIVE fixture profile (verified pin, real dial): `--output-version 1`
    // dials, authenticates and round-trips; the record carries `Ping`.
    harness::write_profile(&fixture);
    let run = harness::run_shell_ok(&fixture, &["--output-version", "1", "--", "ping"]);
    let record = assert_local_result("output-version-1 ping", &run, "ping", Some("Ping"));
    assert_eq!(
        record["data"],
        serde_json::json!({ "ok": true }),
        "ping data must be the fixed ok payload"
    );
    assert_no_canary(&run, &fixture.canary);
    assert_no_canary_on_disk(&fixture);
}

#[test]
fn unsupported_output_version_is_typed_error_after_command_parse() {
    let fixture = harness::boot("shell_output_version_rejected");
    // SAME live profile as the version-1 sibling (so the dial works):
    // `--output-version 2` is a POST-command-parse typed refusal — exactly
    // one error record on stdout (`op: null`), exit 1, stderr silent.
    harness::write_profile(&fixture);
    let run = harness::run_shell_refused(&fixture, &["--output-version", "2", "--", "ping"]);
    assert_typed_error(
        "output version",
        &run,
        1,
        "ping",
        None,
        OUTPUT_VERSION_CODE,
        OUTPUT_VERSION_MESSAGE,
    );
    assert!(
        run.stderr.is_empty(),
        "typed error goes to stdout only ({} bytes on stderr)",
        run.stderr.len()
    );
}
#[test]
fn missing_batch_command_is_usage_error() {
    let fixture = harness::boot("shell_output_usage");
    harness::write_profile(&fixture);
    let run = harness::run_shell_refused(&fixture, &["--"]);
    assert_pre_refusal("usage", &run, 1, USAGE_CODE, USAGE_MESSAGE);
}

#[test]
fn interactive_mode_refused_on_piped_streams_stderr_only() {
    let fixture = harness::boot("shell_output_interactive_refused");
    harness::write_profile(&fixture);
    // No `--` command: piped streams, fixed refusal on stderr only.
    let run = harness::run_shell_refused(&fixture, &[]);
    assert_eq!(run.exit_code, 1, "interactive refusal exits 1");
    assert!(
        run.stdout.is_empty(),
        "interactive refusal must not write a stdout record ({} bytes)",
        run.stdout.len()
    );
    let stderr = String::from_utf8_lossy(&run.stderr);
    assert!(
        stderr.contains(MISSING_TTY_CODE),
        "stderr must carry the stable interactive-refusal code"
    );
    assert_no_canary(&run, &fixture.canary);
}
#[test]
fn malformed_explicit_flag_is_fixed_usage_on_stderr() {
    let fixture = harness::boot("shell_output_malformed_flag");
    harness::write_profile(&fixture);
    // Malformed explicit flag: clap diagnostics could echo argv, so every
    // parse failure maps to the fixed usage refusal on stderr.
    let run = harness::run_shell_refused(&fixture, &["--output-version", "--"]);
    assert_pre_refusal("malformed flag", &run, 1, USAGE_CODE, USAGE_MESSAGE);
}

#[test]
fn shell_flags_after_separator_are_not_shell_flags() {
    let fixture = harness::boot("shell_output_flags_before_subcommand");
    harness::write_profile(&fixture);
    // Shell flags are only recognized BEFORE `--`; in legacy-subcommand
    // position it is an unknown token and a fixed usage refusal.
    let run = harness::run_shell_refused(
        &fixture,
        &["--", "--output-version", "1", "profile", "list"],
    );
    assert_pre_refusal("flag after separator", &run, 1, USAGE_CODE, USAGE_MESSAGE);
}

#[test]
fn typed_error_record_is_full_golden_including_command() {
    let fixture = harness::boot("shell_output_typed_error_golden");
    // A profile WITHOUT the routing `database` field: `db list` parses,
    // then the catalogue floor (checked before any dial or secret read)
    // produces the typed `usage` error — FULL golden record on stdout.
    let text = harness::profile_text(
        "tls-tcp",
        &format!("127.0.0.1:{}", fixture.tls_addr.port()),
        Some("localhost"),
        None,
    )
    .replacen("pin: <PIN>\n", &format!("pin: {}\n", fixture.pin_hex()), 1);
    harness::overwrite_profile(&fixture, &text);
    let run = harness::run_shell_refused(&fixture, &["--", "db", "list"]);
    assert_typed_error(
        "db list typed error",
        &run,
        1,
        "db list",
        Some("List"),
        USAGE_CODE,
        USAGE_MESSAGE,
    );
    assert_no_canary(&run, &fixture.canary);
    assert_no_canary_on_disk(&fixture);
}

#[test]
fn shell_runs_without_server_config_from_isolated_cwd() {
    let fixture = harness::boot("shell_output_no_config_isolated_cwd");
    harness::write_profile(&fixture);
    // Isolated empty cwd + profile home env: if the shell read any server
    // config it would fail. `profile list` is local — no secret read.
    let cwd = TempDir::new().expect("isolated cwd tempdir");
    let (key, value) = harness::profile_env(&fixture);
    let run = run_binary(
        cwd.path(),
        Some((key, value)),
        &[
            "shell",
            "--profile",
            harness::PROFILE_NAME,
            "--secret-stdin",
            "--",
            "profile",
            "list",
        ],
    );
    assert_local_result("isolated-cwd shell", &run, "profile list", None);
    assert_no_canary(&run, &fixture.canary);
}

#[test]
fn real_binary_shell_help_is_local_only_menu_without_prompt_or_dial() {
    // REAL binary `shell --help`, isolated empty cwd, env stripped
    // (`server_command`), stdin piped/closed: no profile exists, so no
    // dial can happen. A few STABLE menu lines asserted whitespace-exact
    // per line; no typed error and no secret prompt may appear.
    let cwd = TempDir::new().expect("help cwd tempdir");
    let run = run_binary(cwd.path(), None, &["shell", "--help"]);
    assert_eq!(run.exit_code, 0, "shell --help exits 0");
    assert!(
        run.stdout.is_empty(),
        "help must not write a stdout record ({} bytes)",
        run.stdout.len()
    );
    let stderr = String::from_utf8_lossy(&run.stderr);
    for line in [
        "commands:",
        "  ping",
        "  profile list | profile show NAME",
        "  quit | exit",
        "secret source flags: --secret-stdin | --secret-fd N (Unix only) | --secret-env NAME | --secret-prompt",
    ] {
        assert!(
            stderr.lines().any(|found| found == line),
            "stderr must carry the exact menu line {line:?}"
        );
    }
    assert!(
        !stderr.contains("password: "),
        "help must not prompt for a secret"
    );
}

#[test]
fn shell_refuses_explicit_config_flag_as_usage() {
    let fixture = harness::boot("shell_output_config_flag_refused");
    harness::write_profile(&fixture);
    // The shell never reads server config; an explicit `--config` (even a
    // nonexistent path) must be a fixed usage refusal on stderr.
    let run = harness::run_shell_refused(
        &fixture,
        &["--config", "no-such-server.ktav", "--", "profile", "list"],
    );
    assert_pre_refusal("config flag", &run, 1, USAGE_CODE, USAGE_MESSAGE);
}

/// Legacy config-path probe: exact exit code, local stderr marker.
fn expect_legacy_config_outcome(cwd: &Path, args: &[&str], exit: i32, marker: &str) {
    let run = run_binary(cwd, None, args);
    assert_eq!(
        run.exit_code, exit,
        "legacy subcommand exit code per the pre-existing boot path"
    );
    assert!(
        String::from_utf8_lossy(&run.stderr).contains(marker),
        "stderr must carry the local diagnostic marker {marker:?}"
    );
}

#[test]
fn run_subcommand_still_requires_and_validates_config() {
    let scratch = TempDir::new().expect("run-subcommand tempdir");
    let malformed = scratch.path().join("broken-server.ktav");
    std::fs::write(&malformed, "{{{{ not ktav at all\n").expect("write malformed config");
    let malformed_str = malformed.display().to_string();
    // Missing `--config`: clap usage exit 2. Malformed config, flag in
    // VALID CLI order: anyhow FAILURE exit 1 and the local `config parse`
    // marker, never a clap diagnostic.
    expect_legacy_config_outcome(scratch.path(), &["run"], 2, "--config");
    expect_legacy_config_outcome(
        scratch.path(),
        &["--config", malformed_str.as_str(), "run"],
        1,
        "config parse",
    );
}

#[test]
fn offline_subcommand_still_requires_and_validates_config() {
    let scratch = TempDir::new().expect("offline-subcommand tempdir");
    let malformed = scratch.path().join("broken-server.ktav");
    std::fs::write(&malformed, "{{{{ not ktav at all\n").expect("write malformed config");
    let malformed_str = malformed.display().to_string();
    // `format-migrate` (offline) keeps requiring `--config`: clap exit 2;
    // and validating it — flag in VALID CLI order: exit 1, local marker.
    expect_legacy_config_outcome(scratch.path(), &["format-migrate"], 2, "--config");
    expect_legacy_config_outcome(
        scratch.path(),
        &["--config", malformed_str.as_str(), "format-migrate"],
        1,
        "config parse",
    );
}

// ── Scripted terminal restore (in-process, real orchestration) ───────

// `tests/admin/main.rs` includes the REAL production shell package
// (`../../shell/mod.rs`) as `crate::shell`; the fake only records events —
// the ordering assertion proves the PRODUCTION code made the restore call.

/// Shared event log: the exact seam call order observed (ONE timeline
/// shared by the scripted stderr writer and the fake terminal).
#[derive(Clone, Default)]
struct ScriptLog {
    events: Rc<RefCell<Vec<&'static str>>>,
}

impl ScriptLog {
    fn push(&self, event: &'static str) {
        self.events.borrow_mut().push(event);
    }

    /// How many times a seam event was observed.
    fn count(&self, event: &str) -> usize {
        self.events
            .borrow()
            .iter()
            .filter(|item| **item == event)
            .count()
    }

    /// The named events must appear (among whatever else was recorded) in
    /// exactly this relative order.
    fn assert_subsequence(&self, label: &str, expected: &[&'static str]) {
        let events = self.events.borrow();
        let mut cursor = events.as_slice();
        for want in expected {
            match cursor.iter().position(|event| event == want) {
                Some(position) => cursor = &cursor[position + 1..],
                None => panic!(
                    "{label}: the scripted seam log does not contain the required \
                     relative order {expected:?} ({} events recorded)",
                    events.len()
                ),
            }
        }
    }
}

/// Scripted fake terminal: RAW outcomes per seam step plus the call
/// order on the SHARED log. Success `read` payloads end in a terminator.
struct FakeTerminal {
    read: Result<Zeroizing<Vec<u8>>, ShellError>,
    restore: Result<(), ShellError>,
    log: ScriptLog,
}

impl Terminal for FakeTerminal {
    fn begin_hidden_read(&self) -> Result<Box<dyn HiddenLineSession + '_>, ShellError> {
        self.log.push("disable");
        Ok(Box::new(FakeSession {
            read: self.read.clone(),
            restore: self.restore,
            log: self.log.clone(),
        }))
    }
}

struct FakeSession {
    read: Result<Zeroizing<Vec<u8>>, ShellError>,
    restore: Result<(), ShellError>,
    log: ScriptLog,
}

impl HiddenLineSession for FakeSession {
    fn read_line(&mut self) -> Result<Zeroizing<Vec<u8>>, ShellError> {
        self.log.push("read");
        let raw = match &self.read {
            Ok(bytes) => bytes.clone(),
            Err(error) => return Err(*error),
        };
        // The PRODUCTION Enter rule (`is_hidden_enter`): first CR/LF
        // ends the line, no optional-LF wait; EOF mid-line is refused.
        let end = raw
            .iter()
            .position(|byte| crate::shell::terminal::secret::is_hidden_enter(u16::from(*byte)))
            .ok_or(ShellError::InvalidSecret)?;
        Ok(Zeroizing::new(raw[..end].to_vec()))
    }

    fn restore(self: Box<Self>) -> Result<(), ShellError> {
        self.log.push("restore");
        self.restore
    }
}

/// Build a scripted fake terminal bound to the SHARED event log.
fn fake_terminal(
    log: ScriptLog,
    read: Result<Zeroizing<Vec<u8>>, ShellError>,
    restore: Result<(), ShellError>,
) -> FakeTerminal {
    FakeTerminal { read, restore, log }
}

/// One direct prompt attempt through the scripted seams, NO fixture boot.
fn direct_prompt(
    read: Result<Zeroizing<Vec<u8>>, ShellError>,
    restore: Result<(), ShellError>,
    fail_write: bool,
) -> (Result<Zeroizing<Vec<u8>>, ShellError>, ScriptLog, Vec<u8>) {
    let log = ScriptLog::default();
    let mut stderr = ScriptedStderr::new(log.clone(), fail_write);
    let terminal = fake_terminal(log.clone(), read, restore);
    let secret = read_secret_with_writer(SecretSource::Prompt, &terminal, &mut stderr);
    (secret, log, stderr.sink)
}

/// One prompt attempt: disable → write → flush → read → restore, once.
fn assert_single_prompt_attempt(log: &ScriptLog, label: &str) {
    log.assert_subsequence(
        label,
        &["disable", "prompt-write", "prompt-flush", "read", "restore"],
    );
    for event in ["disable", "prompt-write", "prompt-flush", "read", "restore"] {
        assert_eq!(log.count(event), 1, "{label}: {event} once");
    }
}

#[test]
fn prompt_success_disables_reads_then_restores() {
    // RAW scripted bytes in every Enter shape (CR, LF, CRLF, plus bytes
    // past the terminator), split with the PRODUCTION `is_hidden_enter`
    // rule — never pretrimmed, never blocking on an optional LF.
    let shapes: [&[u8]; 5] = [
        b"scripted-secret\r",
        b"scripted-secret\n",
        b"scripted-secret\r\n",
        &b"scripted-secret\r\ntrailing"[..],
        &b"scripted-secret\rmore"[..],
    ];
    for raw in shapes {
        let (secret, log, sink) = direct_prompt(Ok(Zeroizing::new(raw.to_vec())), Ok(()), false);
        assert_eq!(
            secret.expect("prompt read succeeds").as_slice(),
            b"scripted-secret",
            "the FIRST CR/LF ends the line; later bytes never read"
        );
        assert_single_prompt_attempt(&log, "prompt success");
        let prompts = String::from_utf8_lossy(&sink).matches("password: ").count();
        assert_eq!(prompts, 1, "password prompt written once");
    }
}

#[test]
fn prompt_failure_paths_restore_in_order_and_report() {
    // Failure paths, no boot: restore STILL happens on every path; a
    // failed restore outranks the read outcome (production precedence).
    let (secret, log, _) = direct_prompt(Err(ShellError::Interrupted), Ok(()), false);
    let error = secret.expect_err("an interrupted read is a typed refusal");
    assert_eq!(error, ShellError::Interrupted, "read error reported");
    assert_single_prompt_attempt(&log, "prompt interrupted");
    for (read, label) in [
        (
            Ok(Zeroizing::new(b"scripted-secret\n".to_vec())),
            "restore wins",
        ),
        (Err(ShellError::Interrupted), "restore wins over read error"),
    ] {
        let (secret, log, _) = direct_prompt(read, Err(ShellError::SecretTerminal), false);
        let error = secret.expect_err("a restore failure must fail the prompt");
        assert_eq!(error, ShellError::SecretTerminal, "{label}: restore wins");
        assert_single_prompt_attempt(&log, label);
    } // restore-error precedence loop

    // Prompt-write failure after disable: restore BEFORE reporting; no
    // read and no secret at the writer.
    let (secret, log, sink) =
        direct_prompt(Ok(Zeroizing::new(b"never-read\n".to_vec())), Ok(()), true);
    let error = secret.expect_err("a failed prompt write must fail the prompt");
    assert_eq!(
        error,
        ShellError::SecretTerminal,
        "restore wins after failed write"
    );
    log.assert_subsequence(
        "prompt write failure",
        &["disable", "prompt-write", "restore"],
    );
    assert_eq!(log.count("read"), 0, "no read after failed prompt write");
    assert!(
        !contains_bytes(&sink, b"never-read"),
        "secret never in stderr"
    );
}

#[test]
fn in_process_shell_run_help_is_reachable_and_exits_zero() {
    // Reachability root: `--help` needs no config, dial or secret.
    let exit = crate::shell::run(["--help".into()]);
    assert_eq!(exit, 0, "the fixed --help path exits 0");
}

// ── Scripted interactive REPL (coordinated `run_scripted` seam) ─────

// `run_scripted` drives the REAL interactive loop in-process: scripted
// stdin, captured stderr, the [`FakeTerminal`], an injected profile root.
/// A scripted stderr for [`crate::shell::parser::run::run_scripted`]: captures
/// every byte AND logs each write into the SHARED [`ScriptLog`] (exact
/// `password: ` prompt → `prompt-write`, its flush → `prompt-flush`,
/// REPL prompt → `repl-prompt`, else `stderr-write`).
struct ScriptedStderr {
    log: ScriptLog,
    sink: Vec<u8>,
    fail_write: bool,
    /// Set by a `prompt-write`, consumed by the next flush: flush events
    /// are scoped to the password prompt write only.
    armed: bool,
}

impl ScriptedStderr {
    fn new(log: ScriptLog, fail_write: bool) -> Self {
        Self {
            log,
            sink: Vec::new(),
            fail_write,
            armed: false,
        }
    }
}

impl Write for ScriptedStderr {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        // Logged BEFORE any failure return; failure confined to the prompt.
        let event = if buf == b"password: " {
            "prompt-write"
        } else if buf == b"> " {
            "repl-prompt"
        } else {
            "stderr-write"
        };
        self.log.push(event);
        if event == "prompt-write" {
            self.armed = true;
            if self.fail_write {
                return Err(std::io::Error::other("scripted prompt write failure"));
            }
        }
        self.sink.extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        if self.armed {
            self.armed = false;
            self.log.push("prompt-flush");
        }
        Ok(())
    }
}

/// One scripted REPL session: the fixture profile LEAF is written and
/// its PARENT is the injected profile root; SHARED timeline.
fn scripted_session(
    fixture: &harness::ShellFixture,
    lines: &str,
    read: Result<Zeroizing<Vec<u8>>, ShellError>,
    restore: Result<(), ShellError>,
) -> (u8, ScriptLog, Vec<u8>) {
    let leaf = harness::write_profile(fixture);
    let root = leaf
        .parent()
        .expect("profile leaf parent is the profiles root")
        .to_path_buf();
    let log = ScriptLog::default();
    let mut stderr = ScriptedStderr::new(log.clone(), false);
    let terminal = fake_terminal(log.clone(), read, restore);
    let mut input = Cursor::new(lines.as_bytes().to_vec());
    let exit = crate::shell::parser::run::run_scripted(
        ["--profile".into(), harness::PROFILE_NAME.into()],
        &mut input,
        &mut stderr,
        &terminal,
        Some(&root),
    );
    (exit, log, stderr.sink)
}

#[test]
fn scripted_prompt_success_orders_disable_write_read_restore() {
    // Scripted `ping` then `quit`: FULL cross-seam ordering, the REAL
    // fixture canary as the secret, the rendered `ok true` row proved.
    let fixture = harness::boot("shell_output_scripted_prompt_order");
    // The fake REQUIRES a terminator: raw canary bytes ending in LF,
    // exactly as a real terminal Enter delivers them.
    let mut canary_raw = fixture.canary.as_bytes().to_vec();
    canary_raw.push(b'\n');
    let (exit, log, sink) = scripted_session(
        &fixture,
        "ping\nquit\n",
        Ok(Zeroizing::new(canary_raw)),
        Ok(()),
    );
    assert_eq!(exit, 0, "the scripted session ends cleanly on quit");
    assert_single_prompt_attempt(&log, "prompt ordering");
    let sink = String::from_utf8_lossy(&sink);
    assert_eq!(
        sink.matches("password: ").count(),
        1,
        "password prompt written once"
    );
    assert!(
        sink.contains("ping") && sink.contains("true"),
        "the rendered ping result (ok true) must appear on the scripted stderr"
    );
    assert!(
        !contains_bytes(sink.as_bytes(), fixture.canary.as_bytes()),
        "the canary secret must never reach the captured stderr"
    );
}

#[test]
fn scripted_failed_explicit_connect_then_ping_has_no_retry_or_source_reread() {
    // Explicit `connect` fails with a non-interrupting read error; the
    // following `ping` hits the disconnected sentinel (typed
    // `connection_failed`) with NO second prompt attempt and no dial.
    let fixture = harness::boot("shell_output_scripted_failed_connect_ping");
    let (exit, log, sink) = scripted_session(
        &fixture,
        "connect\nping\nquit\n",
        Err(ShellError::InvalidSecret),
        Ok(()),
    );
    assert_eq!(exit, 0, "the scripted session ends cleanly on quit");
    assert_single_prompt_attempt(&log, "failed connect ordering");
    let sink = String::from_utf8_lossy(&sink);
    assert!(
        sink.contains(INVALID_SECRET_CODE),
        "read-error code reported"
    );
    assert!(
        sink.contains(CONNECTION_CODE),
        "the second command reports the typed disconnected-sentinel code"
    );
    assert!(!sink.contains("connected\""), "no second connect succeeds");
}

// ── Shared bounded reader: CR / LF terminator + cap discipline ───────

// The shared reader strips one CR paired with the LF terminator;
// interior and unterminated CRs are CONTENT. Bytes are injected through
// the byte-source seam (a closure over the payload slice), so the REAL
// reader loop is driven byte-for-byte on EVERY OS — no platform split.

/// Read one line through the REAL shared bounded reader.
fn reader_line(payload: &[u8]) -> Result<Zeroizing<Vec<u8>>, ShellError> {
    let mut bytes = payload.iter();
    crate::shell::terminal::secret::read_line_bounded(|| Ok(bytes.next().copied()))
}

#[test]
fn shared_reader_cr_terminator_and_cap_discipline() {
    // `SECRET_MAX_BYTES` in shell/terminal/secret.rs (pub(super): not
    // importable here — asserted by the cap cases below).
    const CAP: usize = 4096;
    let cases: &[(&[u8], &[u8])] = &[
        (b"a", b"a"),         // clean EOF, no terminator
        (b"a\n", b"a"),       // LF terminator
        (b"a\r\n", b"a"),     // CRLF: the paired CR is stripped
        (b"a\r\r\n", b"a\r"), // CRCRLF: exactly one interior CR is KEPT
        (b"a\r", b"a\r"),     // unterminated CR at EOF is content, retained
    ];
    for (payload, expected) in cases {
        let secret = reader_line(payload).expect("a capped, valid line must be accepted");
        assert_eq!(secret.as_slice(), *expected);
    }

    // Cap: exactly CAP content bytes + LF is accepted in full; one byte
    // more is refused `secret_too_long`, never truncated. The spare slot
    // is only for the CR of a CRLF terminator: CAP bytes + CRLF accepted.
    let cap_line = vec![b'a'; CAP];
    let secret = reader_line(&cap_line).expect("exactly-cap line is accepted");
    assert_eq!(secret.len(), CAP);
    let mut over = cap_line.clone();
    over.push(b'a');
    assert!(matches!(reader_line(&over), Err(ShellError::SecretTooLong)));
    let mut crlf = cap_line.clone();
    crlf.extend_from_slice(b"\r\n");
    let secret = reader_line(&crlf).expect("cap line with CRLF terminator is accepted");
    assert_eq!(secret.len(), CAP);
    // Exact-cap CRCRLF regression: CAP-1 content bytes + CRCRLF fills the
    // cap exactly — the paired CR pops, the SECRET CR stays as content.
    let mut crcrlf = vec![b'a'; CAP - 1];
    crcrlf.extend_from_slice(b"\r\r\n");
    let secret = reader_line(&crcrlf).expect("exact-cap CRCRLF line is accepted");
    assert_eq!(secret.len(), CAP, "the paired CR pops, the secret CR stays");
    assert_eq!(secret.last(), Some(&b'\r'), "trailing secret CR preserved");
    let mut bare_extra_cr = cap_line.clone();
    bare_extra_cr.extend_from_slice(b"\r\r");
    assert!(matches!(
        reader_line(&bare_extra_cr),
        Err(ShellError::SecretTooLong)
    ));

    // Empty input (immediate EOF) and non-UTF-8 content are typed refusals.
    assert!(matches!(reader_line(b""), Err(ShellError::InvalidSecret)));
    assert!(matches!(reader_line(b"\n"), Err(ShellError::InvalidSecret)));
    assert!(matches!(
        reader_line(b"\xff\n"),
        Err(ShellError::InvalidSecret)
    ));
}

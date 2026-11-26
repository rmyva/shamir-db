//! Online-export shell integration probes. The configured exporter lifecycle
//! is covered by the server's R3 acceptance; this file pins shell-local gates,
//! token policy, the privilege preflight and the fail-closed unavailable
//! response. Batch `status`/`cancel` NEVER reach the server (the dispatcher's
//! session-only gate, `shell/parser/dispatch.rs::jobs_outcome`, refuses them
//! before any dial), so no test here may expect a server outcome for them:
//! that refusal is owned by `batch_status_cancel_are_session_only_refusals`.

use std::time::Duration;

use serde_json::Value;
use shamir_query_builder::{batch::Batch, ddl, token_admin};
use shamir_query_types::wire::token_admin::{ApiTokenChannels, ApiTokenMode};
use zeroize::Zeroizing;

use super::harness::{
    boot, boot_export, overwrite_profile, run_shell, seed_catalogue, seed_shell_user, ShellFixture,
    DB_NAME, PROFILE_NAME, SHELL_USER,
};
use super::interactive::{assert_human_field, human_field_value, InteractiveChild};
use super::secure_fixture::{
    boot_secure, provision_token, run_token_shell, run_with_secret, write_secure_profile,
};
use super::state_data_common::{scram_profile, Relay, TOKEN_SUBSET};

/// Batch `start` waits on the SAME session for the terminal status and prints
/// ONE record; its process bound exceeds the shell's own 55-second wait.
#[test]
fn export_online_lifecycle_completes_with_opaque_receipt() {
    let fixture = boot_export("export_online_lifecycle_completes_with_opaque_receipt");
    seed_export_database(&fixture, EXPORT_LIFE_DB);
    let profile = scram_profile(&fixture, "tls-tcp", &fixture.tls_addr.to_string());
    overwrite_profile(&fixture, &profile);
    fixture
        .guard
        .mark("export lifecycle: batch start waits for terminal state");
    let run = super::harness::secure_fixture::run_secret_process_timeout(
        &fixture,
        Some(fixture.canary.clone()),
        false,
        None,
        &[],
        &["--", "export", "online", "start", EXPORT_LIFE_DB, "--yes"],
        std::time::Duration::from_secs(720),
    );
    assert_safe_output(&fixture, &run);
    assert_eq!(run.exit_code, 0);
    let record: Value = serde_json::from_slice(&run.stdout).expect("terminal export JSONL");
    assert_eq!(record["command"], "export online start");
    assert_eq!(record["kind"], "result");
    assert_eq!(
        record["data"]["phase"], "done",
        "terminal export record: {record}"
    );
    assert_opaque_label(
        record["data"]["export_id"].as_str().expect("export label"),
        "ex",
    );
    assert_opaque_label(
        record["data"]["artifact_name"]
            .as_str()
            .expect("artifact label"),
        "export",
    );
    for counter in ["sections", "items", "bytes"] {
        let value = record["data"][counter]
            .as_str()
            .expect("decimal counter string");
        assert!(!value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()));
    }
    fixture
        .guard
        .mark("export lifecycle: completed opaque artifact receipt");
}

/// Parse the one JSONL record and require a safe error envelope.
fn refusal(run: &super::harness::ShellRun, command: &str, code: &str, exit: i32) -> Value {
    assert_eq!(run.exit_code, exit);
    let line = run
        .stdout
        .strip_suffix(b"\n")
        .expect("one LF-terminated stdout record");
    assert!(!line.contains(&b'\n'));
    let record: Value = serde_json::from_slice(line).expect("valid JSONL record");
    assert_eq!(record["command"], command);
    assert_eq!(record["kind"], "error");
    assert_eq!(record["code"], code);
    assert_eq!(record["data"], Value::Null);
    assert_eq!(record["node"], Value::Null);
    assert!(run.exit_code == 0 || (1..=7).contains(&run.exit_code));
    record
}

/// ONE shared record scan for batch runs AND interactive child runs: every
/// stdout/stderr byte must be free of the fixture's canary, path separators
/// and all four private fixture roots.
fn assert_safe_streams(fixture: &ShellFixture, stdout: &[u8], stderr: &[u8]) {
    let output = format!(
        "{}{}",
        String::from_utf8_lossy(stdout),
        String::from_utf8_lossy(stderr)
    );
    assert!(!output.contains(fixture.canary.as_str()), "secret leaked");
    assert!(!output.contains('/'), "POSIX path separator leaked");
    assert!(!output.contains('\\'), "Windows path separator leaked");
    let fixture_path = super::harness::secure_fixture::data_dir(fixture)
        .display()
        .to_string();
    let profile_home = fixture.profile_home.path().display().to_string();
    // `data_dir` is the held fixture TempDir itself; `data/`, `export-root/`
    // and `scheduled-backups/` are its children (`boot_export`,
    // `boot_scheduled_backup`).
    let fixture_root = super::harness::secure_fixture::data_dir(fixture);
    let nested_data = fixture_root.join("data").display().to_string();
    let export_root = fixture_root.join("export-root").display().to_string();
    let scheduled_root = fixture_root.join("scheduled-backups").display().to_string();
    for private_root in [
        &fixture_path,
        &profile_home,
        &nested_data,
        &export_root,
        &scheduled_root,
    ] {
        assert!(
            !output.contains(private_root),
            "absolute fixture path leaked"
        );
    }
}

/// The PROFILE text is scanned for the canary and secret values ONLY. The
/// other scan rules do not apply BY CONSTRUCTION: a secure (HTTPS/token)
/// profile legitimately names the fixture CA file, which the harness copies
/// UNDER the fixture profile root (`secure_fixture.rs` copies the public
/// certificate to `profile_home/fixture-ca.pem`), so a separator/root scan of
/// the profile would reject the fixture itself, not a shell defect.
fn assert_safe_profile(fixture: &ShellFixture) {
    let profile_text = read_profile(fixture);
    assert!(
        !profile_text.contains(fixture.canary.as_str()),
        "profile contains canary"
    );
}

/// Batch-run scan: streams full rules + profile canary rule + guide examples.
fn assert_safe_output(fixture: &ShellFixture, run: &super::harness::ShellRun) {
    assert_safe_streams(fixture, &run.stdout, &run.stderr);
    assert_safe_profile(fixture);
    assert_export_guide_examples(fixture);
}

/// The receipt labels are server-generated opaque names
/// (`ex-{unix_millis}-{seq:06}` / `export-{unix_millis}-{seq:06}`):
/// pin the shape so a path-shaped peer value cannot masquerade as one.
fn assert_opaque_label(label: &str, prefix: &str) {
    let rest = label
        .strip_prefix(&format!("{prefix}-"))
        .expect("label prefix");
    let (timestamp, sequence) = rest.rsplit_once('-').expect("label sequence tail");
    assert!(
        !timestamp.is_empty()
            && timestamp.bytes().all(|byte| byte.is_ascii_digit())
            && sequence.len() == 6
            && sequence.bytes().all(|byte| byte.is_ascii_digit()),
        "label is not the server's opaque {prefix}-millis-seq shape"
    );
}

fn read_profile(fixture: &ShellFixture) -> String {
    let mut path = fixture.profile_home.path().to_path_buf();
    if cfg!(unix) {
        path.push("shamir-shell");
    } else {
        path.push("ShamirDB");
        path.push("shell");
    }
    path.push("profiles");
    path.push(format!("{PROFILE_NAME}.ktav"));
    std::fs::read_to_string(path).expect("read existing profile contents")
}

/// Check the command examples of BOTH export guide regions — the walkthrough
/// section and the separate client-chapter example: precisely one batch
/// `start`, no batch `status`/`cancel`, and no interactive command behind a
/// separate `shamir-server shell` process invocation.
fn assert_export_guide_examples(fixture: &ShellFixture) {
    let guide = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../../docs/guide-docs/guide/07-operations.md"),
    )
    .expect("read operations guide");
    assert!(!guide.contains(fixture.canary.as_str()));
    let profile_home = fixture.profile_home.path().display().to_string();
    let data_dir = super::harness::secure_fixture::data_dir(fixture)
        .display()
        .to_string();
    assert!(!guide.contains(&profile_home));
    assert!(!guide.contains(&data_dir));
    let walkthrough = guide
        .split_once("### 11. Онлайн-экспорт")
        .expect("export guide section")
        .1
        .split_once("### 12. Токен-сессии")
        .expect("end export guide section")
        .0;
    let client_block = guide
        .split_once("В интерактивном режиме сессия сохраняется между командами")
        .expect("client-chapter export paragraph")
        .1
        .split_once("|| Код | Когда")
        .expect("end of client-chapter export block")
        .0;
    for region in [walkthrough, client_block] {
        let mut fences = region.split("```");
        let mut code_blocks = Vec::new();
        while let (Some(_language), Some(code)) = (fences.next(), fences.next()) {
            code_blocks.push(code);
        }
        assert!(!code_blocks.is_empty(), "guide region has command examples");
        let batch_lines: Vec<&str> = code_blocks
            .iter()
            .flat_map(|block| block.lines())
            .filter(|line| line.trim_start().starts_with("shamir-server shell"))
            .collect();
        assert!(!batch_lines.is_empty());
        for line in &batch_lines {
            assert!(line.contains("export online start"));
            assert!(!line.contains("export online status"));
            assert!(!line.contains("export online cancel"));
        }
        for block in &code_blocks {
            assert!(!block.contains("<PIN>"));
            assert!(
                !block.contains('/') && !block.contains('\\'),
                "guide export examples must not carry path separators"
            );
            for line in block.lines() {
                assert!(
                    !line.trim_start().starts_with("export online ")
                        || !line.contains("shamir-server shell"),
                    "interactive command must not invoke a separate shell process"
                );
            }
        }
    }
}

/// OWNERSHIP, part (a): the SAME interactive session's `export online status`
/// returns the started job's record BEFORE and AFTER a second session's
/// status/cancel refusals, so a missing job can never fake the denial. The
/// registry is session-keyed, so the outsider refusal holds for a live AND a
/// terminal job alike — the test does not depend on this export staying live.
#[test]
fn interactive_export_session_ownership_is_enforced() {
    let fixture = boot_export("interactive_export_session_ownership_is_enforced");
    seed_catalogue(&fixture);
    let profile = scram_profile(&fixture, "tls-tcp", &fixture.tls_addr.to_string());
    overwrite_profile(&fixture, &profile);
    fixture
        .guard
        .mark("interactive ownership: drive owner and outsider");
    let owner_command = InteractiveChild::child_command(&fixture);
    let outsider_command = InteractiveChild::child_command(&fixture);
    let owner_secret = fixture.canary.clone();
    let outsider_secret = fixture.canary.clone();
    let owner_home = fixture.profile_home.path().to_path_buf();
    let outsider_home = owner_home.clone();
    let (owner_run, outsider_run) = fixture
        .rt
        .run_timeout(Duration::from_secs(720), async move {
            let mut owner = InteractiveChild::spawn(owner_command, owner_secret, owner_home).await;
            let connect = owner.command_until("connect", "connect", "connected").await;
            assert_human_field(&connect, "connected", "true");
            let start = owner
                .command_until(
                    &format!("export online start {DB_NAME} --yes"),
                    "export online start",
                    "export_id",
                )
                .await;
            let export_id = human_field_value(&start, "export_id").to_owned();
            assert!(!export_id.is_empty(), "interactive start returns a receipt");

            // LIVE-OWNER SUCCESS CONTROL, first pass: the same session sees
            // its own job before any outsider traffic.
            let owner_first = owner
                .command_until("export online status", "export online status", "export_id")
                .await;
            assert_eq!(human_field_value(&owner_first, "export_id"), export_id);

            let mut outsider =
                InteractiveChild::spawn(outsider_command, outsider_secret, outsider_home).await;
            let outsider_connect = outsider
                .command_until("connect", "connect", "connected")
                .await;
            assert_human_field(&outsider_connect, "connected", "true");
            let outsider_status = outsider
                .command_until("export online status", "error", "message")
                .await;
            assert_human_field(&outsider_status, "code", "online_export_not_found");
            assert_human_field(
                &outsider_status,
                "message",
                "no online export job is visible to this session",
            );
            let outsider_cancel = outsider
                .command_until("export online cancel --yes", "error", "message")
                .await;
            assert_human_field(&outsider_cancel, "code", "online_export_not_found");
            assert_human_field(
                &outsider_cancel,
                "message",
                "no online export job is visible to this session",
            );

            // LIVE-OWNER SUCCESS CONTROL, second pass: the owner still sees
            // its job after the refusals.
            let owner_again = owner
                .command_until("export online status", "export online status", "export_id")
                .await;
            assert_eq!(human_field_value(&owner_again, "export_id"), export_id);

            let owner_run = owner.finish().await;
            assert_eq!(owner_run.exit_code, 0);
            let outsider_run = outsider.finish().await;
            assert_eq!(outsider_run.exit_code, 0);
            (owner_run, outsider_run)
        });
    // The shared scan (canary, separators, every fixture root) on BOTH runs.
    for run in [&owner_run, &outsider_run] {
        assert_safe_streams(&fixture, &run.stdout, &run.stderr);
    }
}

/// CANCEL, part (b): the owner cancels its own job from the same interactive
/// session. PAIRING RULE (from the frozen registry contract,
/// `src/boot/config/diagnostics/logical/export_online/registry.rs:219-242`):
/// a receipt `cancelled = true` means the signal was delivered onto a LIVE
/// job, so the final phase MUST be `cancelled`; a receipt `false` means the
/// job had already finished or entered the noncancellable publication window,
/// so the final phase MUST be one of the other terminal outcomes
/// (`done` | `failed` | `cancelled`). The single post-cancel status read of
/// round 3 raced the worker (the receipt does not imply the unwind finished);
/// it is replaced by a BOUNDED loop over the SAME session: every poll is the
/// driver's own 180-second-bounded response wait, the loop is bounded by
/// 150 seconds of wall clock, and polls are paced by
/// [`STATUS_POLL_INTERVAL`]. A worker that never terminates fails the loop
/// bound; a correct run never does.
#[test]
fn interactive_export_cancel_reaches_an_enumerated_terminal_phase() {
    let fixture = boot_export("interactive_export_cancel_reaches_an_enumerated_terminal_phase");
    seed_catalogue(&fixture);
    let profile = scram_profile(&fixture, "tls-tcp", &fixture.tls_addr.to_string());
    overwrite_profile(&fixture, &profile);
    fixture
        .guard
        .mark("interactive cancel: drive owner session");
    let owner_command = InteractiveChild::child_command(&fixture);
    let owner_secret = fixture.canary.clone();
    let owner_home = fixture.profile_home.path().to_path_buf();
    let owner_run = fixture
        .rt
        .run_timeout(Duration::from_secs(720), async move {
            let mut owner = InteractiveChild::spawn(owner_command, owner_secret, owner_home).await;
            let connect = owner.command_until("connect", "connect", "connected").await;
            assert_human_field(&connect, "connected", "true");
            owner
                .command_until(
                    &format!("export online start {DB_NAME} --yes"),
                    "export online start",
                    "export_id",
                )
                .await;
            let cancelled = owner
                .command_until(
                    "export online cancel --yes",
                    "export online cancel",
                    "cancelled",
                )
                .await;
            let cancelled_value = human_field_value(&cancelled, "cancelled").to_owned();
            assert!(
                cancelled_value == "true" || cancelled_value == "false",
                "cancel receipt must be the typed boolean, got {cancelled_value}"
            );

            // The receipt only confirms the signal was delivered: the worker
            // may still report a live phase (`waiting_for_gate`, `exporting`,
            // `verifying`) while it unwinds, so every live phase keeps
            // polling. The outer timeout bounds the WHOLE loop, an in-flight
            // poll included.
            let final_phase = tokio::time::timeout(CANCEL_TERMINAL_BOUND, async {
                loop {
                    let terminal = owner
                        .command_until("export online status", "export online status", "phase")
                        .await;
                    let phase = human_field_value(&terminal, "phase").to_owned();
                    match phase.as_str() {
                        "cancelled" | "done" | "failed" => return phase,
                        "waiting_for_gate" | "exporting" | "verifying" => {
                            tokio::time::sleep(STATUS_POLL_INTERVAL).await;
                        }
                        other => panic!("unexpected export phase {other}"),
                    }
                }
            })
            .await
            .expect("the export worker never reached a terminal phase within the bound");
            // Signal delivered onto a LIVE job (`true`): the unwind must
            // land on `cancelled`. Already finished or in the publication
            // window (`false`): any terminal outcome is the contract (see
            // the pairing rule above).
            if cancelled_value.as_str() == "true" {
                assert_eq!(final_phase, "cancelled");
            }
            let owner_run = owner.finish().await;
            assert_eq!(owner_run.exit_code, 0);
            owner_run
        });
    assert_safe_streams(&fixture, &owner_run.stdout, &owner_run.stderr);
}

/// A small plain database for the lifecycle probe: the shared shell catalogue
/// carries content (an index with control characters in its name, a schema
/// table) that the exporter may refuse, and this probe must prove the success
/// path of a batch `start`.
const EXPORT_LIFE_DB: &str = "exportlifedb";

fn seed_export_database(fixture: &ShellFixture, db: &'static str) {
    const SERVER_ROUTING_DB: &str = "default";
    let addr = fixture.tls_addr;
    let pin = fixture.pin;
    let password = Zeroizing::new(fixture.canary.as_bytes().to_vec());
    fixture
        .rt
        .run_timeout(Duration::from_secs(120), async move {
            let client = super::harness::connect_tls_owned(addr, pin, password)
                .await
                .expect("export seed connect");
            let mut mk_db = Batch::new();
            mk_db.id("mk-export-db");
            mk_db.create_db("mk", ddl::create_db(db));
            client
                .execute(SERVER_ROUTING_DB, mk_db.build())
                .await
                .expect("create the export database");
            let mut mk_repo = Batch::new();
            mk_repo.id("mk-export-repo");
            mk_repo.create_repo(
                "mr",
                ddl::create_repo("main").engine(ddl::RepoEngine::Fjall),
            );
            client
                .execute(db, mk_repo.build())
                .await
                .expect("create the export repository");
            let mut mk_table = Batch::new();
            mk_table.id("mk-export-table");
            mk_table.create_table("tb", ddl::create_table("rows").repo("main"));
            client
                .execute(db, mk_table.build())
                .await
                .expect("create the export table");
            let mut rows = Batch::new();
            for index in 0..3 {
                rows.insert(
                    format!("ins-{index}"),
                    shamir_query_builder::write::insert("rows").row(shamir_query_builder::doc! {
                        "id" => format!("row-{index}"),
                        "marker" => "export-life",
                    }),
                );
            }
            client
                .execute(db, rows.build())
                .await
                .expect("insert the export rows");
            client.close().await;
        });
}

/// Wall-clock bound of the post-cancel terminal wait (a hang DETECTOR: a
/// correct small export terminates well inside it).
const CANCEL_TERMINAL_BOUND: std::time::Duration = std::time::Duration::from_secs(150);

/// Pacing between post-cancel status polls (never a tight loop).
const STATUS_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(50);

/// ROUTING-PRECEDENCE REGRESSION (round 4): the interactive `start` targets a
/// SECOND database while the profile's routing database is a different one;
/// the session must remember the started job's database, because the frozen
/// server resolves the job by session handle AND the envelope database
/// (`export_gate.rs` envelope-equality rule, read-only). Mutant: removing the
/// `job_database` recording in `shell/parser/dispatch.rs::jobs_outcome` (or
/// any precedence swap in `jobs.rs::status_envelope`) makes the later
/// `status`/`cancel` address the routing database and answer
/// `online_export_not_found` — exactly what these assertions reject.
#[test]
fn interactive_export_status_cancel_use_the_job_database() {
    let fixture = boot_export("interactive_export_status_cancel_use_the_job_database");
    seed_catalogue(&fixture);
    let profile = scram_profile(&fixture, "tls-tcp", &fixture.tls_addr.to_string());
    overwrite_profile(&fixture, &profile);
    // The second database: the routing database of the profile is `DB_NAME`
    // (`scram_profile` seeds `database: shelldb`), so the job is started on
    // a DIFFERENT database via the existing `ddl::create_db` builder (the
    // same one `harness.rs` seed uses).
    const JOB_DB: &str = "exportjobdb";
    const SERVER_ROUTING_DB: &str = "default";
    let addr = fixture.tls_addr;
    let pin = fixture.pin;
    let password = Zeroizing::new(fixture.canary.as_bytes().to_vec());
    fixture
        .rt
        .run_timeout(std::time::Duration::from_secs(120), async move {
            let client = super::harness::connect_tls_owned(addr, pin, password)
                .await
                .expect("routing seed connect");
            let mut batch = Batch::new();
            batch.create_db("mkjobdb", ddl::create_db(JOB_DB));
            client
                .execute(SERVER_ROUTING_DB, batch.build())
                .await
                .expect("create the second database");
            client.close().await;
        });
    fixture
        .guard
        .mark("interactive routing: start on the second database");
    let owner_command = InteractiveChild::child_command(&fixture);
    let owner_secret = fixture.canary.clone();
    let owner_home = fixture.profile_home.path().to_path_buf();
    let canary = fixture.canary.clone();
    let owner_run = fixture
        .rt
        .run_timeout(Duration::from_secs(720), async move {
            let mut owner = InteractiveChild::spawn(owner_command, owner_secret, owner_home).await;
            let connect = owner.command_until("connect", "connect", "connected").await;
            assert_human_field(&connect, "connected", "true");
            let start = owner
                .command_until(
                    &format!("export online start {JOB_DB} --yes"),
                    "export online start",
                    "export_id",
                )
                .await;
            let export_id = human_field_value(&start, "export_id").to_owned();
            assert!(!export_id.is_empty(), "interactive start returns a receipt");
            // WITH the recorded job database both commands resolve the job on
            // `JOB_DB`; WITHOUT it the envelope would be the routing database
            // and the server would answer `online_export_not_found`.
            let status = owner
                .command_until("export online status", "export online status", "export_id")
                .await;
            assert_eq!(human_field_value(&status, "export_id"), export_id);
            let cancelled = owner
                .command_until(
                    "export online cancel --yes",
                    "export online cancel",
                    "cancelled",
                )
                .await;
            let cancelled_value = human_field_value(&cancelled, "cancelled").to_owned();
            assert!(
                cancelled_value == "true" || cancelled_value == "false",
                "cancel receipt must be the typed boolean, got {cancelled_value}"
            );
            let deadline = std::time::Instant::now() + CANCEL_TERMINAL_BOUND;
            loop {
                assert!(
                    std::time::Instant::now() < deadline,
                    "the export worker never reached a terminal phase"
                );
                let terminal = owner
                    .command_until("export online status", "export online status", "phase")
                    .await;
                let phase = human_field_value(&terminal, "phase").to_owned();
                if matches!(phase.as_str(), "cancelled" | "done" | "failed") {
                    break;
                }
                tokio::time::sleep(STATUS_POLL_INTERVAL).await;
            }
            let run = owner.finish().await;
            assert_eq!(run.exit_code, 0);
            run
        });
    assert_safe_streams(&fixture, &owner_run.stdout, &owner_run.stderr);
    let streams = format!(
        "{}{}",
        String::from_utf8_lossy(&owner_run.stdout),
        String::from_utf8_lossy(&owner_run.stderr)
    );
    assert!(!streams.contains(canary.as_str()), "secret leaked");
    assert!(!streams.contains('/') && !streams.contains('\\'));
}

/// Reachable batch gates ONLY. The dispatcher refuses batch `status`/`cancel`
/// before confirmation and before any dial (`jobs_outcome`, session-only
/// gate), so neither a confirmation refusal nor the server's unconfigured
/// outcome is reachable for them — those two cases are NOT asserted here.
/// The typed unavailable outcome of interactive `status`/`cancel` is pinned
/// at the mapping layer (`shell/connect/tests/error_mapping.rs`); no cheap
/// interactive harness boots an export-less node, so it is recorded in the
/// step report as mapping-layer-only.
#[test]
fn export_online_local_gates_and_unavailable_are_typed() {
    let fixture = boot("export_online_local_gates_and_unavailable_are_typed");
    seed_catalogue(&fixture);
    let profile = scram_profile(&fixture, "tls-tcp", &fixture.tls_addr.to_string());
    overwrite_profile(&fixture, &profile);
    fixture
        .guard
        .mark("export local refusal: unconfirmed start");
    let run = run_shell(&fixture, &["--", "export", "online", "start", "shelldb"]);
    assert_safe_output(&fixture, &run);
    let record = refusal(&run, "export online start", "confirmation_required", 6);
    assert_eq!(record["op"], "StartExport");

    fixture.guard.mark("export unavailable: start");
    let run = run_shell(
        &fixture,
        &["--", "export", "online", "start", DB_NAME, "--yes"],
    );
    assert_safe_output(&fixture, &run);
    let record = refusal(&run, "export online start", "online_export_unavailable", 5);
    assert_eq!(record["op"], "StartExport");
}

#[test]
fn batch_status_cancel_are_session_only_refusals() {
    let fixture = boot("batch_status_cancel_are_session_only_refusals");
    let profile = scram_profile(&fixture, "tls-tcp", &fixture.tls_addr.to_string());
    for (args, command) in [
        (
            &["--", "export", "online", "status"][..],
            "export online status",
        ),
        (
            &["--", "export", "online", "cancel"][..],
            "export online cancel",
        ),
    ] {
        let relay = Relay::start(&fixture, fixture.tls_addr);
        overwrite_profile(
            &fixture,
            &profile.replace(&fixture.tls_addr.to_string(), &relay.addr.to_string()),
        );
        let run = run_shell(&fixture, args);
        assert_export_guide_examples(&fixture);
        assert_safe_output(&fixture, &run);
        let record = refusal(&run, command, "export_session_only", 1);
        assert_eq!(record["command"], command);
        assert_eq!(record["kind"], "error");
        assert_eq!(record["op"], Value::Null);
        assert_eq!(record["node"], Value::Null);
        assert_eq!(record["data"], Value::Null);
        let expected_message = "an export job belongs to the session that started it";
        assert_eq!(record["message"], expected_message);
        assert_eq!(
            record.as_object().expect("error envelope object").len(),
            8,
            "session-only refusal has the exact JSON v1 error envelope"
        );
        assert_eq!(
            relay.finish(&fixture),
            0,
            "session-only refusal must not dial"
        );
    }
}

/// SCRAM-only policy denies token sessions before dialing; a relay proves
/// every export command results in ZERO server requests.
#[test]
fn token_export_refusal_sends_zero_requests() {
    let secure = boot_secure("token_export_refusal_sends_zero_requests");
    let fixture = &secure.shell;
    seed_shell_user(fixture);
    let token = provision_token(
        fixture,
        token_admin::create_api_token("https")
            .for_user(SHELL_USER)
            .databases([DB_NAME])
            .mode(ApiTokenMode::ReadOnly)
            .channels(ApiTokenChannels::Both),
    );
    let relay = Relay::start(fixture, secure.http_addr);
    write_secure_profile(fixture, "https", relay.addr, DB_NAME, Some(&secure.ca_path));
    for args in [
        &["export", "online", "start", DB_NAME, "--yes"][..],
        &["export", "online", "status"][..],
        &["export", "online", "cancel", "--yes"][..],
    ] {
        fixture
            .guard
            .mark(format!("token export: {}", args.join(" ")));
        let run = run_token_shell(fixture, &token, args);
        assert_safe_output(fixture, &run);
        let command = match args[2] {
            "start" => "export online start",
            "status" => "export online status",
            _ => "export online cancel",
        };
        let record = refusal(&run, command, "token_request_denied", 4);
        assert_eq!(record["command"], command);
        assert_eq!(record["kind"], "error");
        assert_eq!(record["code"], "token_request_denied");
        assert_eq!(record["op"], Value::Null);
        assert_eq!(record["node"], Value::Null);
        assert_eq!(record["data"], Value::Null);
        assert_eq!(record["message"], TOKEN_SUBSET);
        assert_eq!(record.as_object().expect("token error envelope").len(), 8);
    }
    assert_eq!(relay.finish(fixture), 0, "token refusals must not dial");
}

/// The ordinary non-superuser is refused by the privilege preflight. Scope
/// denial beyond that preflight is owned by the server's R3 tests.
#[test]
fn export_start_non_superuser_is_refused_by_the_privilege_preflight() {
    let fixture = boot_export("export_start_non_superuser_is_refused_by_the_privilege_preflight");
    seed_catalogue(&fixture);
    let restricted_password = seed_shell_user(&fixture);
    let restricted = scram_profile(&fixture, "tls-tcp", &fixture.tls_addr.to_string())
        .replace("username: admin\n", &format!("username: {SHELL_USER}\n"));
    overwrite_profile(&fixture, &restricted);
    fixture
        .guard
        .mark("export start restricted privilege preflight");
    let run = run_with_secret(
        &fixture,
        restricted_password,
        false,
        &["--", "export", "online", "start", DB_NAME, "--yes"],
    );
    assert_safe_output(&fixture, &run);
    let record = refusal(&run, "export online start", "permission_denied", 4);
    assert_eq!(record["op"], "StartExport");
    assert_eq!(record["message"], "permission denied by the server");
}

/// The shell blocks an unconfirmed start locally before it can issue a request.
#[test]
fn export_online_confirmation_refusal_is_pre_dispatch() {
    let fixture = boot("export_online_confirmation_refusal_is_pre_dispatch");
    let profile = scram_profile(&fixture, "tls-tcp", &fixture.tls_addr.to_string());
    overwrite_profile(&fixture, &profile);
    fixture.guard.mark("export start blocked before request");
    let run = run_shell(&fixture, &["--", "export", "online", "start", "shelldb"]);
    assert_safe_output(&fixture, &run);
    let record = refusal(&run, "export online start", "confirmation_required", 6);
    assert_eq!(record["op"], "StartExport");
}

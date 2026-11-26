use super::*;
use std::collections::BTreeSet;
use std::path::Path;

#[test]
fn catalogue_command_ids_have_frozen_payload_field_sets() {
    let source = std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("shell/commands/catalogue.rs"),
    )
    .expect("read catalogue source");
    // Parse only the id() region, excluding the separate op() match.
    let id_region = source
        .split_once("fn id(&self)")
        .expect("catalogue id method")
        .1
        .split_once("fn op(&self)")
        .expect("separate id and op methods")
        .0;
    let ids: BTreeSet<String> = id_region
        .lines()
        .filter_map(|line| {
            line.split_once("=> \"")
                .and_then(|(_, tail)| tail.split_once('\"').map(|(s, _)| s.to_owned()))
        })
        .collect();
    let goldens: &[(&str, &[&str])] = &[
        ("db list", &["databases"]),
        ("repo list", &["repos"]),
        (
            "repo describe",
            &[
                "describe_repo",
                "db",
                "engine",
                "durability",
                "durability_override",
            ],
        ),
        ("table list", &["tables", "repo"]),
        ("index list", &["indexes", "table", "repo"]),
        ("function list", &["functions"]),
        (
            "table describe",
            &[
                "describe_table",
                "repo",
                "schema",
                "schema_version",
                "indexes",
                "validators",
                "retention",
                "buffer",
                "owner",
                "group",
                "mode",
            ],
        ),
        (
            "table schema",
            &["get_table_schema", "repo", "schema", "schema_version"],
        ),
    ];
    assert_eq!(
        ids,
        goldens.iter().map(|(id, _)| (*id).to_owned()).collect()
    );
    let fixture = harness::boot("catalogue_command_ids_have_frozen_payload_field_sets");
    harness::seed_catalogue(&fixture);
    let profile = harness::profile_text(
        "tls-tcp",
        &fixture.tls_addr.to_string(),
        Some("localhost"),
        Some(harness::DB_NAME),
    )
    .replace("pin: <PIN>\n", &format!("pin: {}\n", fixture.pin_hex()));
    assert!(
        !profile.contains("<PIN>"),
        "profile pin placeholder was not replaced"
    );
    harness::overwrite_profile(&fixture, &profile);
    let commands: &[(&[&str], &str)] = &[
        (&["db", "list"], "db list"),
        (&["repo", "list"], "repo list"),
        (&["repo", "describe", harness::REPO_NAME], "repo describe"),
        (&["table", "list", harness::REPO_NAME], "table list"),
        (
            &["index", "list", harness::REPO_NAME, harness::PLAIN_TABLE],
            "index list",
        ),
        (&["function", "list"], "function list"),
        (
            &[
                "table",
                "describe",
                harness::REPO_NAME,
                harness::PLAIN_TABLE,
            ],
            "table describe",
        ),
        (
            &["table", "schema", harness::REPO_NAME, harness::SCHEMA_TABLE],
            "table schema",
        ),
    ];
    for ((args, id), (_, fields)) in commands.iter().zip(goldens) {
        let mut argv = vec!["--"];
        argv.extend_from_slice(args);
        let run = harness::run_shell_ok(&fixture, &argv);
        let record = single_lf_record(id, &run.stdout);
        assert_payload_fields(&record, id, fields);
    }
}

#[test]
fn schedule_result_field_set_is_frozen() {
    let fixture = harness::boot_scheduled_backup("schedule_result_field_set_is_frozen");
    harness::seed_catalogue(&fixture);
    let profile = harness::profile_text(
        "tls-tcp",
        &fixture.tls_addr.to_string(),
        Some("localhost"),
        Some(harness::DB_NAME),
    )
    .replace("pin: <PIN>\n", &format!("pin: {}\n", fixture.pin_hex()));
    assert!(
        !profile.contains("<PIN>"),
        "profile pin placeholder was not replaced"
    );
    harness::overwrite_profile(&fixture, &profile);
    let run = harness::run_shell_ok(&fixture, &["--", "backup", "schedule", "show"]);
    let record = single_lf_record("backup schedule show", &run.stdout);
    assert_payload_fields(
        &record,
        "backup schedule show",
        &[
            "state",
            "enabled",
            "interval_seconds",
            "window",
            "keep_last",
            "run_on_start",
            "next_run",
            "last_run",
            "runs",
        ],
    );
}

fn assert_payload_fields(record: &Json, command: &str, fields: &[&str]) {
    assert_eq!(record["command"].as_str(), Some(command));
    let actual: BTreeSet<&str> = record["data"]
        .as_object()
        .expect("result payload object")
        .keys()
        .map(String::as_str)
        .collect();
    let expected: BTreeSet<&str> = fields.iter().copied().collect();
    assert_eq!(
        actual, expected,
        "{command} JSON v1 payload field set changed"
    );
}

#[test]
fn maintenance_new_command_result_field_sets_are_frozen() {
    let fixture = harness::boot_export("maintenance_new_command_result_field_sets_are_frozen");
    harness::seed_catalogue(&fixture);
    let profile = harness::profile_text(
        "tls-tcp",
        &fixture.tls_addr.to_string(),
        Some("localhost"),
        Some(harness::DB_NAME),
    )
    .replace("pin: <PIN>\n", &format!("pin: {}\n", fixture.pin_hex()));
    assert!(
        !profile.contains("<PIN>"),
        "profile pin placeholder was not replaced"
    );
    harness::overwrite_profile(&fixture, &profile);

    let start = harness::secure_fixture::run_secret_process_timeout(
        &fixture,
        Some(fixture.canary.clone()),
        false,
        None,
        &[],
        &["--", "export", "online", "start", harness::DB_NAME, "--yes"],
        std::time::Duration::from_secs(720),
    );
    assert_eq!(start.exit_code, 0);
    let start_record = single_lf_record("export online start", &start.stdout);
    assert_payload_fields(
        &start_record,
        "export online start",
        &[
            "export_id",
            "phase",
            "sections",
            "items",
            "bytes",
            "artifact_name",
            "failure",
        ],
    );

    for (command, args) in [
        (
            "export online status",
            &["--", "export", "online", "status"][..],
        ),
        (
            "export online cancel",
            &["--", "export", "online", "cancel", "--yes"][..],
        ),
    ] {
        let run = harness::secure_fixture::run_secret_process_timeout(
            &fixture,
            Some(fixture.canary.clone()),
            false,
            None,
            &[],
            args,
            std::time::Duration::from_secs(120),
        );
        assert_eq!(run.exit_code, 1);
        assert!(run.stderr.is_empty());
        let record = single_lf_record(command, &run.stdout);
        let expected_keys: BTreeSet<&str> = [
            "version", "command", "kind", "op", "node", "data", "code", "message",
        ]
        .into_iter()
        .collect();
        let actual_keys: BTreeSet<&str> = record
            .as_object()
            .expect("error envelope object")
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(actual_keys, expected_keys);
        assert_eq!(record["version"], 1);
        assert_eq!(record["command"], command);
        assert_eq!(record["kind"], "error");
        assert!(record["op"].is_null());
        assert!(record["node"].is_null());
        assert!(record["data"].is_null());
        assert_eq!(record["code"], "export_session_only");
        assert_eq!(
            record["message"],
            "an export job belongs to the session that started it"
        );
    }

    assert_export_guide_examples(&fixture);
}

fn assert_export_guide_examples(fixture: &harness::ShellFixture) {
    let guide = std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../../docs/guide-docs/guide/07-operations.md"),
    )
    .expect("read operations guide");
    assert!(!guide.contains(fixture.canary.as_str()));
    let profile_home = fixture.profile_home.path().display().to_string();
    let data_dir = harness::secure_fixture::data_dir(fixture)
        .display()
        .to_string();
    assert!(!guide.contains(&profile_home));
    assert!(!guide.contains(&data_dir));
    let section = guide
        .split_once("### 11. Онлайн-экспорт")
        .expect("export guide section")
        .1
        .split_once("### 12. Токен-сессии")
        .expect("end export guide section")
        .0;
    let mut fences = section.split("```");
    let mut code_blocks = Vec::new();
    while let (Some(_language), Some(code)) = (fences.next(), fences.next()) {
        code_blocks.push(code);
    }
    assert!(!code_blocks.is_empty(), "guide has command examples");
    for code in &code_blocks {
        assert!(!code.contains("export online status") || code.contains("интерактивной сессии"));
        assert!(!code.contains("export online cancel") || code.contains("интерактивной сессии"));
        assert!(!code.contains("<PIN>"));
        assert!(code.lines().all(|line| {
            !line.contains("shamir-server shell") || line.contains("export online start")
        }));
    }
    let batch_examples: Vec<&str> = code_blocks
        .iter()
        .flat_map(|code| code.lines())
        .filter(|line| line.trim_start().starts_with("shamir-server shell"))
        .collect();
    assert!(!batch_examples.is_empty());
    assert!(batch_examples
        .iter()
        .all(|line| line.contains("export online start")));
    assert!(batch_examples.iter().all(
        |line| !line.contains("export online status") && !line.contains("export online cancel")
    ));
    assert!(code_blocks
        .iter()
        .all(|code| !code.contains("/") && !code.contains('\\')));
}

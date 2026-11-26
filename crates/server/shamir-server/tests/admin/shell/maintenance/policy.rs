//! Pins discoverability: `shell --help` must advertise every command family
//! this release ships, including the explained-only ones, the working
//! `backup schedule show` and the online-export family (whose `status` and
//! `cancel` are interactive only). The help text is a fixed literal, never
//! argv — so this test spawns the real binary and only reads stderr, never
//! echoes it.

/// The literals the fixed help text must carry, one per command family.
const REQUIRED: [&str; 18] = [
    "backup start --to DIR",
    "backup status",
    "backup cancel",
    "backup verify SNAPSHOT",
    "backup restore",
    "backup snapshot",
    "backup schedule show",
    "version stamp",
    "version migrate",
    "schema-migration status --database-id ID",
    "export online start DATABASE [--repo R] [--table T] [--schema-only] [--include-artifacts] [--yes]",
    "export online status (interactive only)",
    "export online cancel [--yes] (interactive only)",
    "export logical",
    "import logical",
    "diag wal",
    "diag durability REPO",
    "diag integrity",
];

#[test]
fn help_lists_every_command_family() {
    let output = std::process::Command::new(super::harness::SERVER_BIN)
        .arg("shell")
        .arg("--help")
        .env_remove("SHAMIR_SHELL_PASSWORD")
        .env_remove("SHAMIR_SHELL_TOKEN")
        .env_remove("XDG_CONFIG_HOME")
        .env_remove("HOME")
        .env_remove("LOCALAPPDATA")
        .env_remove("USERPROFILE")
        .output()
        .expect("failed to spawn the server binary");
    assert!(
        output.status.code() == Some(0),
        "shell --help exited {:?}; stderr {} bytes",
        output.status.code(),
        output.stderr.len(),
    );
    assert!(
        output.stdout.is_empty(),
        "shell --help wrote {} stdout bytes",
        output.stdout.len(),
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    for literal in REQUIRED {
        assert!(
            stderr.contains(literal),
            "help text is missing {literal:?}; stderr {} bytes",
            output.stderr.len(),
        );
    }
    assert!(
        !stderr.contains("--force"),
        "help text unexpectedly advertises \"--force\"; stderr {} bytes",
        output.stderr.len(),
    );
}

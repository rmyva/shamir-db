//! Pin for the INLINE `Subcmd` variants behind the admin shell's explained
//! manuals (design `admin-shell.md` §4.10).
//!
//! `backup-verify`, `backup`, `restore` and `doctor` are declared INLINE in
//! `src/engine/server/bootstrap_mode.rs` (re-exported as
//! `shamir_server::server::{Cli, Subcmd}`) instead of as reusable `clap::Args`
//! structs, so the shell's integration test
//! (`tests/admin/shell/maintenance/explain/explain.rs`) — a separate crate —
//! does not parse them; it pins the printed lines. This module is a `#[cfg(test)]`
//! module of the LIBRARY target, selected by CI's `--lib` runs, so each
//! manual's tokens go through `Cli::try_parse_from` and the resulting
//! variant's FIELDS are matched and compared:
//!
//! ```text
//! shamir-server backup-verify SNAPSHOT
//! shamir-server --config CONFIG backup --to DIR
//! shamir-server --config CONFIG restore --from SNAPSHOT
//! shamir-server --config CONFIG doctor --db DB --repo REPO --table TABLE \
//!     --apply --pretty --json
//! ```
//!
//! What this kills that a text scan of `src/engine/server/bootstrap_mode.rs`
//! could not:
//!
//! * a renamed long flag — `#[arg(long = "database")] db: Option<String>`
//!   leaves the field declaration intact, so a declaration search passes
//!   while `--db` no longer parses;
//! * a comment-planted needle — `// formerly to: PathBuf` inside `Backup {`
//!   satisfies a substring search for `to: PathBuf` after the real field
//!   was renamed;
//! * a commented-out field (or variant) — the declaration text remains in
//!   the file, the parser loses the option.
//!
//! A text scan also FAILS on harmless edits (a trailing comment on
//! `Doctor {`, a reflowed declaration); the parser below cannot.

use std::path::PathBuf;

use clap::Parser;

use crate::server::{Cli, Subcmd};

/// `shamir-server backup-verify SNAPSHOT` — the positional snapshot path.
///
/// `Cli::config` is a REQUIRED top-level option, while the standalone
/// `backup-verify` form is dispatched by `run_main` before `Cli::parse` and
/// takes no config at all. `--config X` is supplied here only to satisfy the
/// top-level parser, exactly as the other three manuals advertise it; the
/// tokens this test pins are the subcommand and its `SNAPSHOT` positional.
#[test]
fn backup_verify_parses_the_manual_tokens() {
    let cli = Cli::try_parse_from([
        "shamir-server",
        "--config",
        "X",
        "backup-verify",
        "SNAPSHOT",
    ])
    .expect("`backup-verify SNAPSHOT` must parse with the server's real CLI");
    assert_eq!(cli.config, PathBuf::from("X"));
    match cli.command {
        Some(Subcmd::BackupVerify { snapshot }) => {
            assert_eq!(snapshot, PathBuf::from("SNAPSHOT"));
        }
        other => panic!("`backup-verify` did not parse to `Subcmd::BackupVerify`: {other:?}"),
    }
}

/// `shamir-server --config CONFIG backup --to DIR` — the `to` destination.
#[test]
fn backup_parses_the_manual_tokens() {
    let cli = Cli::try_parse_from(["shamir-server", "--config", "X", "backup", "--to", "DIR"])
        .expect("`backup --to DIR` must parse with the server's real CLI");
    assert_eq!(cli.config, PathBuf::from("X"));
    match cli.command {
        Some(Subcmd::Backup { to }) => {
            assert_eq!(to, PathBuf::from("DIR"));
        }
        other => panic!("`backup` did not parse to `Subcmd::Backup`: {other:?}"),
    }
}

/// `shamir-server --config CONFIG restore --from SNAPSHOT` — the `from`
/// source, with `--force` left off (the manual's safe offline form).
#[test]
fn restore_parses_the_manual_tokens() {
    let cli = Cli::try_parse_from(["shamir-server", "--config", "X", "restore", "--from", "DIR"])
        .expect("`restore --from DIR` must parse with the server's real CLI");
    assert_eq!(cli.config, PathBuf::from("X"));
    match cli.command {
        Some(Subcmd::Restore { from, force }) => {
            assert_eq!(from, PathBuf::from("DIR"));
            assert!(!force, "the manual's `restore` must not imply `--force`");
        }
        other => panic!("`restore` did not parse to `Subcmd::Restore`: {other:?}"),
    }
}

/// `shamir-server --config CONFIG doctor --db DB --repo REPO --table TABLE
/// --apply --pretty --json` — every filter and output flag the manual prints.
#[test]
fn doctor_parses_the_manual_tokens() {
    let cli = Cli::try_parse_from([
        "shamir-server",
        "--config",
        "X",
        "doctor",
        "--db",
        "DB",
        "--repo",
        "REPO",
        "--table",
        "TABLE",
        "--apply",
        "--pretty",
        "--json",
    ])
    .expect("`doctor` manual tokens must parse with the server's real CLI");
    assert_eq!(cli.config, PathBuf::from("X"));
    match cli.command {
        Some(Subcmd::Doctor {
            db,
            repo,
            table,
            apply,
            pretty,
            json,
        }) => {
            assert_eq!(db.as_deref(), Some("DB"));
            assert_eq!(repo.as_deref(), Some("REPO"));
            assert_eq!(table.as_deref(), Some("TABLE"));
            assert!(apply, "`--apply` must be the repairing form");
            assert!(pretty, "`--pretty` must be a flag");
            assert!(json, "`--json` must be a flag");
        }
        other => panic!("`doctor` did not parse to `Subcmd::Doctor`: {other:?}"),
    }
}

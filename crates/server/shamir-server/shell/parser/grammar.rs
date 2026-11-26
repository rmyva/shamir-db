//! The shell command grammar (design `docs/dev-artifacts/design/admin-shell.md`
//! §§4.1, §4.3): the clap option structs, the command enums and the stable
//! command-id projection. No I/O and no session state live here.

use clap::Parser as ClapParser;
use serde_json::Value as Json;

use super::state_args::{DataCommand, ReplicationCommand, VersionCommand};
use crate::shell::output::ShellError;

/// Batch or interactive; interactive defaults the no-echo prompt source.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Mode {
    Batch,
    Interactive,
}

/// One dispatched outcome: command id, wire op, result.
pub(super) type Outcome = (&'static str, Option<&'static str>, Result<Json, ShellError>);

#[derive(Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub(super) enum AuthKind {
    Scram,
    Token,
    Browser,
    HttpLogin,
    Resume,
}

impl AuthKind {
    pub(super) fn check(self) -> Result<(), ShellError> {
        match self {
            Self::Scram | Self::Token => Ok(()),
            Self::Browser | Self::HttpLogin => Err(ShellError::Deferred),
            Self::Resume => Err(ShellError::Resume),
        }
    }
}

/// Shell-level options, parsed only from the argv before the literal `--`.
#[derive(ClapParser)]
#[command(
    name = "shamir-server shell",
    disable_help_flag = true,
    disable_version_flag = true
)]
pub(super) struct ShellArgs {
    /// Connection profile name from the profile directory.
    #[arg(long, value_name = "NAME")]
    pub(super) profile: Option<String>,

    /// Authentication kind (SCRAM by default).
    #[arg(long, value_enum, default_value = "scram", value_name = "AUTH")]
    pub(super) auth: AuthKind,

    /// Session resume is refused outright.
    #[arg(long)]
    pub(super) resume: bool,

    /// Read the secret from standard input.
    #[arg(long)]
    pub(super) secret_stdin: bool,

    /// Read the secret from file descriptor N (Unix only).
    #[arg(long, value_name = "N")]
    pub(super) secret_fd: Option<u32>,

    /// Read the secret from the fixed, auth-matched environment variable.
    #[arg(long, value_name = "NAME")]
    pub(super) secret_env: Option<String>,

    /// Read the secret with a no-echo terminal prompt (explicit; batch mode
    /// must always name the source).
    #[arg(long)]
    pub(super) secret_prompt: bool,

    /// Quiet: suppress interactive diagnostics on stderr.
    #[arg(short = 'q', conflicts_with = "verbose")]
    pub(super) quiet: bool,

    /// Verbose: extra informational lines on stderr only.
    #[arg(short = 'v')]
    pub(super) verbose: bool,

    /// Disable color (PR1 emits no color).
    #[arg(long)]
    pub(super) no_color: bool,

    /// Local assertion of the output schema version; only 1 is accepted.
    #[arg(long, value_name = "VERSION")]
    pub(super) output_version: Option<u32>,
}

/// The single-command grammar wrapper (the command enum is a `Subcommand`).
#[derive(ClapParser)]
#[command(
    name = "shamir-server shell",
    disable_help_flag = true,
    disable_version_flag = true
)]
pub(super) struct CommandArgs {
    #[command(subcommand)]
    pub(super) command: Command,
}

/// The command grammar; no free-form tail, so extra argv tokens are usage refusals.
#[derive(clap::Subcommand)]
pub(super) enum Command {
    /// Establish or switch an authenticated session.
    Connect {
        name: Option<String>,
        #[arg(long, value_enum)]
        auth: Option<AuthKind>,
    },
    /// Round-trip the live connection.
    Ping,
    /// Local profile-directory commands (no secret is ever displayed).
    Profile {
        #[command(subcommand)]
        action: ProfileCommand,
    },
    Db {
        #[command(subcommand)]
        action: DbCommand,
    },
    Repo {
        #[command(subcommand)]
        action: RepoCommand,
    },
    Table {
        #[command(subcommand)]
        action: TableCommand,
    },
    Index {
        #[command(subcommand)]
        action: IndexCommand,
    },
    Function {
        #[command(subcommand)]
        action: FunctionCommand,
    },
    /// Server state: the P1 operator state snapshot (design §4.2).
    Status,
    /// `version show`: observed format epoch and stamp outcome (§4.2);
    /// the shell never reads the data directory.
    Version {
        #[command(subcommand)]
        action: VersionCommand,
    },
    /// Replication subscriptions (§4.2).
    Replication {
        #[command(subcommand)]
        action: ReplicationCommand,
    },
    /// Narrow data views (§4.4); the database is always explicit.
    Data {
        #[command(subcommand)]
        action: DataCommand,
    },
    /// Principal directory (rights §4.3).
    User {
        #[command(subcommand)]
        action: UserCommand,
    },
    /// Groups and their members (rights §4.3).
    Group {
        #[command(subcommand)]
        action: GroupCommand,
    },
    /// Access rights of one tree node (rights §4.3).
    Rights {
        #[command(subcommand)]
        action: RightsCommand,
    },
    /// Operator activity view and cooperative cancellation (activity §4.5).
    Activity {
        #[command(subcommand)]
        action: ActivityCli,
    },
    /// Online copy of the running server (design §4.7); SCRAM only.
    Backup {
        #[command(subcommand)]
        action: BackupCli,
    },
    /// Application-schema migration status of one database (design §4.8).
    SchemaMigration {
        #[command(subcommand)]
        action: SchemaMigrationCli,
    },
    /// Logical export manual (design §4.9); explained only, never executed.
    Export {
        #[command(subcommand)]
        action: ExportCli,
    },
    /// Logical import manual (design §4.9); explained only, never executed.
    Import {
        #[command(subcommand)]
        action: ImportCli,
    },
    /// Diagnostics (design §4.10).
    Diag {
        #[command(subcommand)]
        action: DiagCli,
    },
    /// `chmod PATH MODE`: change the mode bits of one resource (rights
    /// §4.3); SCRAM only and confirmation required.
    Chmod {
        path: String,
        mode: String,
        #[arg(long)]
        yes: bool,
    },
    /// `chown PATH OWNER`: change the owner of one resource (rights §4.3);
    /// SCRAM only and confirmation required.
    Chown {
        path: String,
        owner: String,
        #[arg(long)]
        yes: bool,
    },
    /// `chgrp PATH GROUP`: change the group of one resource (rights §4.3);
    /// SCRAM only and confirmation required.
    Chgrp {
        path: String,
        group: String,
        #[arg(long)]
        yes: bool,
    },
}

/// `activity` grammar: the shell-side command type is
/// [`crate::shell::commands::ActivityCommand`]; this is its argv shape.
#[derive(clap::Subcommand)]
pub(super) enum ActivityCli {
    /// List the live sessions.
    Sessions {
        #[arg(long)]
        all: bool,
        #[arg(long)]
        limit: Option<u32>,
    },
    /// List the in-flight request executions.
    Requests {
        #[arg(long)]
        all: bool,
        #[arg(long)]
        limit: Option<u32>,
    },
    /// Request cooperative cancellation of one request execution.
    Cancel {
        request: String,
        #[arg(long)]
        yes: bool,
    },
}

#[derive(clap::Subcommand)]
pub(super) enum BackupCli {
    /// Start an online copy into a server-side absolute destination root;
    /// the path is passed unmodified and only the server validates it.
    Start {
        #[arg(long, value_name = "DIR")]
        to: String,
        #[arg(long)]
        yes: bool,
    },
    /// Show the typed status of the current (or last) copy.
    Status,
    /// Cancel the current run on this node (no id).
    Cancel {
        #[arg(long)]
        yes: bool,
    },
    /// Explained only: verify one snapshot offline.
    Verify { snapshot: String },
    /// Explained only: restore one snapshot.
    Restore,
    /// Explained only: take a stopped-server snapshot.
    Snapshot,
    /// Backup schedule state.
    Schedule {
        #[command(subcommand)]
        action: ScheduleCli,
    },
}

#[derive(clap::Subcommand)]
pub(super) enum ScheduleCli {
    /// Show the configured schedule.
    Show,
}

#[derive(clap::Subcommand)]
pub(super) enum SchemaMigrationCli {
    /// One line of migration status for the database identity ID.
    Status {
        #[arg(long, value_name = "ID")]
        database_id: String,
    },
}

#[derive(clap::Subcommand)]
pub(super) enum ExportCli {
    /// Explained only: the server's offline logical-export command.
    Logical,
    /// Online logical export job (SCRAM only).
    Online {
        #[command(subcommand)]
        action: OnlineExportCli,
    },
}

#[derive(clap::Subcommand)]
pub(super) enum OnlineExportCli {
    Start {
        database: String,
        #[arg(long)]
        repo: Option<String>,
        #[arg(long)]
        table: Option<String>,
        #[arg(long)]
        schema_only: bool,
        #[arg(long)]
        include_artifacts: bool,
        #[arg(long)]
        yes: bool,
    },
    Status,
    Cancel {
        #[arg(long)]
        yes: bool,
    },
}

#[derive(clap::Subcommand)]
pub(super) enum ImportCli {
    /// Explained only: the server's offline logical-import command.
    Logical,
}

#[derive(clap::Subcommand)]
pub(super) enum DiagCli {
    /// Explained only: the offline integrity check.
    Integrity,
    /// WAL watermark / pending / commit telemetry.
    Wal,
    /// Effective-versus-stored durability of one repository.
    Durability { repo: String },
}

#[derive(clap::Subcommand)]
pub(super) enum UserCommand {
    /// List the principal directory rows.
    List,
}

#[derive(clap::Subcommand)]
pub(super) enum GroupCommand {
    /// Show the access tree, optionally restricted to one database.
    Tree {
        #[arg(long)]
        db: Option<String>,
    },
    /// Show the groups with their members.
    Members,
}

#[derive(clap::Subcommand)]
pub(super) enum RightsCommand {
    /// Show one access-tree node selected by `PATH`.
    Show { path: String },
}

#[derive(clap::Subcommand)]
pub(super) enum ProfileCommand {
    /// List the profile names in the profile directory.
    List,
    /// Show one profile; the pin is reported as present/absent only.
    Show { name: String },
}

#[derive(clap::Subcommand)]
pub(super) enum DbCommand {
    /// List databases.
    List,
}

#[derive(clap::Subcommand)]
pub(super) enum RepoCommand {
    /// List repositories.
    List,
    /// Describe repository durability (SCRAM only).
    Describe { repo: String },
}

#[derive(clap::Subcommand)]
pub(super) enum TableCommand {
    /// List tables of a repository.
    List { repo: String },
    /// Describe a table.
    Describe { repo: String, table: String },
    /// Show a table schema.
    Schema { repo: String, table: String },
}

#[derive(clap::Subcommand)]
pub(super) enum IndexCommand {
    /// List indexes of a table.
    List { repo: String, table: String },
}

#[derive(clap::Subcommand)]
pub(super) enum FunctionCommand {
    /// List functions, optionally restricted to a folder.
    List { folder: Option<String> },
}

/// Stable space-separated command id for the JSON v1 `command` field.
pub(super) fn command_id(command: &Command) -> &'static str {
    match command {
        Command::Connect { .. } => "connect",
        Command::Ping => "ping",
        Command::Profile { action } => match action {
            ProfileCommand::List => "profile list",
            ProfileCommand::Show { .. } => "profile show",
        },
        Command::Db {
            action: DbCommand::List,
        } => "db list",
        Command::Repo { action } => match action {
            RepoCommand::List => "repo list",
            RepoCommand::Describe { .. } => "repo describe",
        },
        Command::Table { action } => match action {
            TableCommand::List { .. } => "table list",
            TableCommand::Describe { .. } => "table describe",
            TableCommand::Schema { .. } => "table schema",
        },
        Command::Index {
            action: IndexCommand::List { .. },
        } => "index list",
        Command::Function {
            action: FunctionCommand::List { .. },
        } => "function list",
        Command::Status => "status",
        Command::Version { action } => match action {
            VersionCommand::Show => "version show",
            VersionCommand::Stamp => "version stamp",
            VersionCommand::Migrate => "version migrate",
        },
        Command::Replication {
            action: ReplicationCommand::Subscriptions,
        } => "replication subscriptions",
        Command::Data { action } => match action {
            DataCommand::Count { .. } => "data count",
            DataCommand::Head { .. } => "data head",
            DataCommand::Get { .. } => "data get",
        },
        Command::User {
            action: UserCommand::List,
        } => "user list",
        Command::Group { action } => match action {
            GroupCommand::Tree { .. } => "group tree",
            GroupCommand::Members => "group members",
        },
        Command::Rights {
            action: RightsCommand::Show { .. },
        } => "rights show",
        Command::Activity { action } => match action {
            ActivityCli::Sessions { .. } => "activity sessions",
            ActivityCli::Requests { .. } => "activity requests",
            ActivityCli::Cancel { .. } => "activity cancel",
        },
        Command::Backup { action } => match action {
            BackupCli::Start { .. } => "backup start",
            BackupCli::Status => "backup status",
            BackupCli::Cancel { .. } => "backup cancel",
            BackupCli::Verify { .. } => "backup verify",
            BackupCli::Restore => "backup restore",
            BackupCli::Snapshot => "backup snapshot",
            BackupCli::Schedule {
                action: ScheduleCli::Show,
            } => "backup schedule show",
        },
        Command::SchemaMigration {
            action: SchemaMigrationCli::Status { .. },
        } => "schema-migration status",
        Command::Export {
            action: ExportCli::Logical,
        } => "export logical",
        Command::Export {
            action: ExportCli::Online { action },
        } => match action {
            OnlineExportCli::Start { .. } => "export online start",
            OnlineExportCli::Status => "export online status",
            OnlineExportCli::Cancel { .. } => "export online cancel",
        },
        Command::Import {
            action: ImportCli::Logical,
        } => "import logical",
        Command::Diag { action } => match action {
            DiagCli::Integrity => "diag integrity",
            DiagCli::Wal => "diag wal",
            DiagCli::Durability { .. } => "diag durability",
        },
        Command::Chmod { .. } => "chmod",
        Command::Chown { .. } => "chown",
        Command::Chgrp { .. } => "chgrp",
    }
}

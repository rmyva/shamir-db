//! Boot bootstrap-mode policy variants.

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use zeroize::Zeroizing;

/// Bootstrap policy options exposed at boot time.
pub enum BootstrapMode {
    /// Use the supplied password verbatim.
    Password {
        username: String,
        password: Zeroizing<Vec<u8>>,
    },
    /// Generate a 32-byte random token (printed to logs + written to
    /// `data_dir/bootstrap_token.txt`, or the overridden `token_path`).
    /// Username defaults to `admin`. The token auto-deletes on first
    /// successful login or a 24h TTL, whichever comes first.
    RandomToken {
        /// Optional override of the default `admin` username.
        username: Option<String>,
        /// Optional override of the default `data_dir/bootstrap_token.txt`
        /// path — recommended to point at a tmpfs path (e.g.
        /// `/run/shamir/bootstrap_token.txt`) so the token is never swept
        /// into a `backup --to` snapshot of `data_dir`.
        token_path: Option<PathBuf>,
    },
    /// Skip bootstrap entirely; assume the directory already has a
    /// superuser. Used when the operator manages users out-of-band.
    Skip,
}

impl Default for BootstrapMode {
    fn default() -> Self {
        BootstrapMode::RandomToken {
            username: None,
            token_path: None,
        }
    }
}

#[derive(Parser)]
#[command(name = "shamir-server", version, about = "ShamirDB production server")]
pub struct Cli {
    /// Path to the `.ktav` config file.
    #[arg(short, long)]
    pub config: PathBuf,

    /// Optional username for the bootstrap superuser. Defaults to `admin`.
    #[arg(long, value_name = "NAME")]
    pub bootstrap_user: Option<String>,

    /// Optional password for the bootstrap superuser. Avoid argv: it may be
    /// exposed in process lists and shell history; prefer --bootstrap-password-stdin.
    /// If omitted, a random
    /// 32-byte token is generated and printed once at WARN level. The token
    /// is also written to `data_dir/bootstrap_token.txt` by default —
    /// override the output path with `--bootstrap-token-path`. The token
    /// auto-deletes on the first successful login, or after a 24h TTL
    /// (whichever comes first) — manual deletion is no longer the primary
    /// cleanup mechanism, though it remains a safe immediate step.
    #[arg(long, value_name = "PASSWORD")]
    pub bootstrap_password: Option<String>,

    /// Read one UTF-8 bootstrap password line from stdin (maximum 4096 bytes).
    #[arg(long, conflicts_with_all = ["bootstrap_password", "skip_bootstrap"])]
    pub bootstrap_password_stdin: bool,

    /// Override the output path for the random bootstrap token (only used
    /// when neither password option is supplied). Recommend a tmpfs path
    /// (e.g. `/run/shamir/bootstrap_token.txt`) so the token is never
    /// captured by a `backup --to` snapshot of `data_dir`.
    #[arg(long, value_name = "PATH")]
    pub bootstrap_token_path: Option<PathBuf>,

    /// Skip the bootstrap step entirely. Use only when the operator manages
    /// the user directory out-of-band.
    #[arg(long)]
    pub skip_bootstrap: bool,

    /// Optional subcommand. Without one (or with `run`) the server runs
    /// normally in the foreground; with `backup` it performs a one-shot
    /// snapshot and exits.
    #[command(subcommand)]
    pub command: Option<Subcmd>,
}

#[derive(Subcommand, Debug)]
pub enum Subcmd {
    /// Run in the foreground (default). Ctrl+C / SIGTERM → graceful
    /// shutdown. Omitting a subcommand is equivalent to `run`.
    Run {
        /// Internal: set by the Windows service ImagePath so the binary
        /// starts the SCM dispatcher instead of running in the foreground.
        #[arg(long, hide = true)]
        service: bool,
    },

    /// Export a stopped database's supported logical scope; fails on unsupported content.
    LogicalExport(crate::logical_dump::LogicalExportArgs),

    /// Import a verified logical dump into a NEW server data directory.
    LogicalImport(crate::logical_import::LogicalImportArgs),

    /// Export one stopped repository as a logical replication seed.
    ReplicationSeedExport {
        #[arg(long)]
        db: String,
        #[arg(long)]
        repo: String,
        #[arg(long)]
        out: PathBuf,
    },
    /// Verify a seed artifact without opening a repository.
    ReplicationSeedVerify {
        dir: PathBuf,
        #[arg(long)]
        manifest_digest: Option<String>,
    },
    /// Install into an isolated new repository; never activate replication.
    ReplicationSeedInstall {
        #[arg(long)]
        db: String,
        #[arg(long)]
        repo: String,
        #[arg(long)]
        artifact: PathBuf,
        #[arg(long)]
        manifest_digest: String,
        #[command(flatten)]
        subscription: crate::replication_seed::SeedSubscriptionArgs,
    },

    /// Snapshot `data_dir` (from --config) into `<to>/<UTC-timestamp>/`.
    /// **Server should be stopped first** for a fully consistent snapshot.
    /// Fjall is journal-based: a copy racing an in-flight append loses
    /// only the torn tail batch on next open (`TolerateCorruptTail`
    /// truncates to the last checksummed batch) — earlier committed
    /// batches are safe — but stop-and-copy is the strongest guarantee.
    /// A `manifest.json` (file list + sha256 + size per file) is written
    /// into the snapshot so `restore` can verify it before trusting it.
    Backup {
        /// Destination directory. Created if missing. The actual snapshot
        /// goes into `<to>/YYYYMMDD_HHMMSS/`.
        #[arg(long, value_name = "DIR")]
        to: PathBuf,
    },

    /// Restore a `backup --to` snapshot back into `data_dir` (from
    /// --config). **Offline only** — the server for this `data_dir` must
    /// be stopped first; a liveness probe (fjall's own exclusive advisory
    /// file lock on `data_dir/server_meta`) refuses the restore otherwise
    /// unless `--force` is passed. Verifies the snapshot's `manifest.json`
    /// BEFORE touching `data_dir`, then atomically swaps: the current
    /// `data_dir` (if any) is renamed to a `.pre_restore_backup_<timestamp>`
    /// sibling (preserved, not deleted — the explicit rollback path)
    /// before the restored snapshot takes its place. Finally invalidates
    /// every outstanding resumption ticket in the restored user directory
    /// so no session issued before the restore point can resume.
    Restore {
        /// Snapshot directory to restore from (e.g. `<to>/YYYYMMDD_HHMMSS`
        /// from a prior `backup --to`).
        #[arg(long, value_name = "DIR")]
        from: PathBuf,
        /// Skip the "is the server currently running" liveness probe. Use
        /// only when you are certain no server process holds `data_dir`
        /// (e.g. recovering from an unclean shutdown where the lock file
        /// itself is stale) — bypassing this check while a real server is
        /// running WILL corrupt the live database.
        #[arg(long)]
        force: bool,
    },

    /// Render the Shomer access-control tree (resources + functions +
    /// principals) and exit. Offline by default (opens `data_dir` from
    /// `--config`; the server must be stopped). With `--connect` it
    /// authenticates to a running server as an admin instead.
    AccessTree {
        /// Resource-depth cap: 0=root, 1=db, 2=store, 3=table. Default: full.
        #[arg(long, value_name = "N")]
        depth: Option<u32>,
        /// Restrict the resource tree to a single database.
        #[arg(long, value_name = "DB")]
        db: Option<String>,
        /// Emit the raw QueryValue dump instead of the rendered ASCII tree.
        #[arg(long)]
        pretty: bool,
        /// Online mode: connect to a running server at `host:port` and
        /// request the tree over TLS+SCRAM (requires admin credentials).
        #[arg(long, value_name = "ADDR")]
        connect: Option<String>,
        /// SNI hostname for TLS in online mode (matches the server cert).
        #[arg(long, value_name = "NAME", default_value = "localhost")]
        server_name: String,
        /// Username for online mode (must be an admin).
        #[arg(long, value_name = "NAME")]
        user: Option<String>,
        /// Password for online mode; falls back to `$SHAMIR_PASSWORD`.
        #[arg(long, value_name = "PASSWORD")]
        password: Option<String>,
    },

    /// Manage the OS service (install / uninstall / status).
    Service {
        #[command(subcommand)]
        action: crate::service::ServiceAction,
    },
    /// Manage API tokens in a stopped data directory.
    Token {
        #[command(subcommand)]
        action: crate::tokens::cli::TokenAction,
    },
    /// Offline format migration of a stopped server data directory.
    FormatMigrate(crate::migrate::FormatMigrateArgs),

    /// Interactive administration shell (design:
    /// `docs/dev-artifacts/design/admin-shell.md`). Dispatched before any
    /// config or logging setup; `--config` is not required for this
    /// subcommand. Must be the first argument: `shamir-server shell ...`.
    Shell,

    /// Verify one offline snapshot against its `manifest.json` (read-only,
    /// hashes every listed file). Dispatched before config load and without
    /// `--config`; must be the first argument. Never boots the server and
    /// never opens a data directory.
    BackupVerify {
        /// Snapshot directory (the one holding `manifest.json`).
        #[arg(value_name = "SNAPSHOT")]
        snapshot: PathBuf,
    },

    /// Verify and optionally repair table integrity. **Offline only** — the
    /// server for this `data_dir` must be stopped first (opens the data
    /// directory directly via `ShamirDb::init`). Scans all tables (or a
    /// filtered subset via `--db`/`--repo`/`--table`) and reports health
    /// diagnostics: Building/Failed indexes, counter consistency, index2
    /// registry consistency, and cross-family name collisions. With
    /// `--apply`, repairs unhealthy tables by dropping and rebuilding all
    /// indexes. Exits with non-zero if any table is unhealthy (useful for
    /// CI/health checks).
    Doctor {
        /// Restrict to a single database (default: all databases).
        #[arg(long, value_name = "DB")]
        db: Option<String>,
        /// Restrict to a single repository (default: all repositories).
        #[arg(long, value_name = "REPO")]
        repo: Option<String>,
        /// Restrict to a single table (default: all tables).
        #[arg(long, value_name = "TABLE")]
        table: Option<String>,
        /// Apply repairs to unhealthy tables (default: read-only verify).
        #[arg(long)]
        apply: bool,
        /// Emit pretty-printed JSON output (default: human-readable text).
        #[arg(long)]
        pretty: bool,
        /// Emit machine-readable JSON output (exclusive with `--pretty`).
        #[arg(long)]
        json: bool,
    },
}

//! ShamirDB production server binary.
//!
//! Boot orchestration lives in [`shamir_server::server::ServerLauncher`] —
//! `main` itself is intentionally tiny: parse CLI, install rustls crypto
//! provider, init tracing, hand the [`Config`] to the launcher, then wait
//! for SIGINT/SIGTERM.

// Native-Rust global allocator. Production-tuned `LargeCacheConfig`:
//   - 2 GiB per-shard budget — caps RSS while accommodating burst 1 MB
//     msgpack frames
//   - 512 MiB headroom — anti-thrash floor on steady-state QueryValues
//   - 500 ms decay interval + 25 % rate — release large buffers briskly
//     after queries complete
//   - Lazy mode — event-driven, no background thread (we already run a
//     full tokio runtime)
const ALLOCATOR_CONFIG: sefer_alloc::LargeCacheConfig = sefer_alloc::LargeCacheConfig::new()
    .budget_bytes(2 * 1024 * 1024 * 1024)
    .headroom_bytes(512 * 1024 * 1024)
    .decay_interval_ms(500)
    .decay_rate_percent(25)
    .mode(sefer_alloc::LargeCacheMode::Lazy);

#[global_allocator]
static GLOBAL: sefer_alloc::SeferAlloc = sefer_alloc::SeferAlloc::with_config(ALLOCATOR_CONFIG);

#[cfg(test)]
#[path = "../../shamir-server-security/src/security/service/tests/bootstrap_cli_tests.rs"]
mod bootstrap_cli_tests;
#[path = "../../shamir-server-security/src/security/service/bootstrap_stdin.rs"]
mod bootstrap_stdin;
#[path = "../shell/mod.rs"]
mod shell;

use std::path::PathBuf;

use clap::Parser;
use zeroize::Zeroizing;

use shamir_server::backup;
use shamir_server::backup::BackupError as Failure;
use shamir_server::config::Config;
use shamir_server::doctor;
use shamir_server::logging::sink_mode_for;
use shamir_server::server::{BootstrapMode, Cli, Subcmd};
use shamir_server::service::ServiceAction;

fn bootstrap_argv_warning_needed(argv_password: &Option<String>) -> bool {
    argv_password.is_some()
}

fn bootstrap_mode(
    skip: bool,
    argv_password: Option<String>,
    stdin_password: Option<Zeroizing<Vec<u8>>>,
    username: Option<String>,
    token_path: Option<PathBuf>,
) -> BootstrapMode {
    if skip {
        BootstrapMode::Skip
    } else if let Some(password) =
        stdin_password.or_else(|| argv_password.map(|pw| Zeroizing::new(pw.into_bytes())))
    {
        BootstrapMode::Password {
            username: username
                .unwrap_or_else(|| shamir_server::bootstrap::DEFAULT_BOOTSTRAP_NAME.to_string()),
            password,
        }
    } else {
        BootstrapMode::RandomToken {
            username,
            token_path,
        }
    }
}

fn main() -> std::process::ExitCode {
    match run_main() {
        Ok(code) => std::process::ExitCode::from(code),
        Err(error) => {
            eprintln!("{error:#}");
            std::process::ExitCode::FAILURE
        }
    }
}

fn run_main() -> anyhow::Result<u8> {
    // Early `shell` / `backup-verify` dispatch, immediately before
    // `Cli::parse()` (design §2, §4.7): neither needs server config, logging
    // or a data directory, so `--config` is not required. Match the first
    // argument EXACTLY: a later token goes through clap and is rejected below.
    // A flag token (one starting with `--`) is never accepted as the
    // `backup-verify` path: it is refused as a usage error instead.
    let mut raw_args = std::env::args_os();
    let _argv0 = raw_args.next();
    let first = raw_args.next();
    if first.as_deref() == Some(std::ffi::OsStr::new("shell")) {
        return Ok(shell::run(raw_args));
    }
    if first.as_deref() == Some(std::ffi::OsStr::new("backup-verify")) {
        let mut rest = raw_args;
        let Some(snapshot) = rest.next() else {
            eprintln!("{BACKUP_VERIFY_USAGE}");
            return Ok(1);
        };
        if snapshot.to_string_lossy().starts_with("--") {
            eprintln!("{BACKUP_VERIFY_USAGE}");
            return Ok(1);
        }
        if rest.next().is_some() {
            eprintln!("{BACKUP_VERIFY_USAGE}");
            return Ok(1);
        }
        return Ok(backup_verify(PathBuf::from(snapshot)));
    }

    let cli = Cli::parse();

    // A `shell` token anywhere but the first argument falls through to clap;
    // reject it before any configuration/logging/runtime work.
    if matches!(cli.command.as_ref(), Some(Subcmd::Shell)) {
        anyhow::bail!("`shell` must be the first argument: run `shamir-server shell ...`");
    }

    if matches!(cli.command.as_ref(), Some(Subcmd::BackupVerify { .. })) {
        anyhow::bail!(
            "`backup-verify` must be the first argument: run `shamir-server backup-verify SNAPSHOT`"
        );
    }

    if let Some(Subcmd::FormatMigrate(args)) = cli.command.as_ref() {
        let mut config = Config::from_file(&cli.config)?;
        config.validate()?;
        if let Some(ref log_file) = config.logging.file {
            let path = std::path::Path::new(log_file);
            if !path.is_absolute() {
                config.logging.file = Some(
                    shamir_server::service::absolute(path)?
                        .to_str()
                        .ok_or_else(|| anyhow::anyhow!("log file path is not valid UTF-8"))?
                        .to_owned(),
                );
            }
        }
        shamir_server::migrate::run(&config, args)?;
        return Ok(0);
    }

    // Windows SCM dispatcher: the SCM starts the process with `--service`
    // and expects a blocking `StartServiceCtrlDispatcher` call on the main
    // thread BEFORE any tokio runtime exists.
    #[cfg(windows)]
    if matches!(cli.command, Some(Subcmd::Run { service: true })) {
        return shamir_server::windows_service::run();
    }

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()?;
    let result = rt.block_on(run_async(cli));
    if result.is_err() {
        // A failed run can leave a detached blocking job (a stalled final
        // snapshot): dropping the runtime would wait for it without a bound and
        // the error would never be printed.
        rt.shutdown_timeout(std::time::Duration::from_secs(5));
    }
    result
}

/// Fixed usage line for the standalone `backup-verify` command (stderr; the
/// argv value is never echoed).
const BACKUP_VERIFY_USAGE: &str = "usage: shamir-server backup-verify SNAPSHOT";

/// Label column width of the `backup-verify` report.
const BACKUP_VERIFY_LABEL_WIDTH: usize = 16;

/// `backup-verify SNAPSHOT` (design `admin-shell.md` §4.7): the manual,
/// read-only offline snapshot check. Nothing boots, no data directory is
/// opened, no network is used and no config is read. It verifies
/// `manifest.json` and hashes every listed file, so it is NOT cheap; it runs
/// with the OS rights of the invoking user and modifies nothing. Exit codes:
/// 0 verified, 1 usage, 5 the snapshot did not verify.
fn backup_verify(snapshot: PathBuf) -> u8 {
    match shamir_server::backup::verify_manifest(&snapshot) {
        Ok(report) => {
            report_line("result", "verified");
            report_line("kind", backup_kind(report.kind));
            report_line("files_checked", &report.files_checked.to_string());
            report_line("total_bytes", &report.total_bytes.to_string());
            if let Some(epoch) = report.format_epoch {
                report_line("format_epoch", &epoch.to_string());
            }
            report_line(
                "note",
                "verification hashed every listed file; it is not a cheap check",
            );
            0
        }
        Err(error) => {
            report_line("result", "failed");
            report_line("error_class", backup_error_class(&error));
            report_line(
                "note",
                "the snapshot must not be restored or trusted; nothing was modified",
            );
            5
        }
    }
}

/// One `label  value` report line on stdout, with a fixed label column.
fn report_line(label: &str, value: &str) {
    println!(
        "{label:<width$}  {value}",
        width = BACKUP_VERIFY_LABEL_WIDTH
    );
}

/// The two manifest kinds the verifier distinguishes.
fn backup_kind(kind: shamir_server::backup::online::manifest::BackupKind) -> &'static str {
    match kind {
        shamir_server::backup::online::manifest::BackupKind::Offline => "offline",
        shamir_server::backup::online::manifest::BackupKind::Online => "online",
    }
}

/// Bounded corruption class of one verification failure. The library's
/// message text is never printed; a variant this build does not name reports
/// the general `corrupt` class.
fn backup_error_class(error: &shamir_server::backup::BackupError) -> &'static str {
    match error {
        Failure::Io(_) => "io",
        Failure::SourceMissing(_) => "source_missing",
        Failure::SourceNotDir(_) => "source_not_dir",
        Failure::ManifestMissing(_) => "manifest_missing",
        Failure::ManifestInvalid(_) => "manifest_invalid",
        Failure::ChecksumMismatch { .. } => "checksum_mismatch",
        Failure::UnmanifestedFile(_) => "unmanifested_file",
        Failure::UnsupportedManifestFormatVersion { .. } => "unsupported_manifest_format_version",
        Failure::UnsafeManifestPath(_) => "unsafe_manifest_path",
        Failure::ManagedSnapshotNotExported(_) => "managed_snapshot",
        Failure::CacheManifestPath(_) => "cache_manifest_path",
        Failure::DuplicateManifestEntry(_) => "duplicate_manifest_entry",
        Failure::WalSeqGap { .. } => "wal_sequence_gap",
        _ => "corrupt",
    }
}

async fn run_async(cli: Cli) -> anyhow::Result<u8> {
    // Install rustls crypto provider (required by tokio-rustls; second call
    // is a harmless no-op).
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    // Load + validate config so we can configure tracing from it.
    let config = Config::from_file(&cli.config)?;
    config.validate()?;

    // Non-blocking logging: log events flow through a lock-free MPSC
    // channel to a background writer thread so emitting never blocks a
    // worker. The guard must live until process exit for a clean flush.
    //
    // RM-6: if `logging.file` is a relative path, resolve it to absolute
    // *before* init so a service (which runs with a different cwd) writes
    // where the operator intended.
    let mut config = config;
    if let Some(ref rel) = config.logging.file {
        let p = std::path::Path::new(rel);
        if !p.is_absolute() {
            config.logging.file = Some(
                shamir_server::service::absolute(p)?
                    .to_str()
                    .ok_or_else(|| anyhow::anyhow!("log file path is not valid UTF-8"))?
                    .to_string(),
            );
        }
    }
    // Offline subcommands take the data-directory lock (or refuse it) and must
    // not create any file before their preconditions and their lock hold, so
    // they get no file sink. `Run` / `None` are the server path.
    //
    // Exhaustive on purpose — no `_` arm and no `matches!`: a new `Subcmd`
    // variant must state which sink it gets instead of silently inheriting
    // one (a compile error is the point).
    //
    // The `FormatMigrate`, `Shell` and `BackupVerify` arms below are
    // UNREACHABLE for this selection — all three return earlier in this
    // function — and are listed only to keep the match exhaustive.
    let offline = match cli.command {
        Some(
            Subcmd::Token { .. }
            | Subcmd::Restore { .. }
            | Subcmd::Backup { .. }
            | Subcmd::LogicalExport(_)
            | Subcmd::LogicalImport(_)
            | Subcmd::ReplicationSeedExport { .. }
            | Subcmd::ReplicationSeedInstall { .. }
            | Subcmd::ReplicationSeedVerify { .. }
            | Subcmd::AccessTree { .. }
            | Subcmd::Service { .. }
            | Subcmd::Doctor { .. }
            | Subcmd::FormatMigrate(_)
            | Subcmd::BackupVerify { .. }
            | Subcmd::Shell,
        ) => true,
        Some(Subcmd::Run { .. }) | None => false,
    };
    // The ONE boot path for both server entries: it refuses a sink that would
    // land in the audit-log namespace (nothing is opened for a refused sink),
    // then installs the subscriber. `SinkMode::StdoutOnly` — every offline
    // command — never validates, opens, creates or stat-s the path at all.
    let _log_guard = shamir_server::logging::boot_logging(
        &config.logging,
        &config.data_dir,
        sink_mode_for(offline),
    )?;

    // Subcommand dispatch — `backup` / `access-tree` exit without booting
    // the server.
    match cli.command {
        Some(Subcmd::LogicalExport(args)) => {
            let cancel = tokio_util::sync::CancellationToken::new();
            let operation =
                shamir_server::logical_dump::export(&config.data_dir, &args, cancel.clone());
            tokio::pin!(operation);
            let report = tokio::select! {
                result = &mut operation => result?,
                signal = tokio::signal::ctrl_c() => {
                    signal?;
                    cancel.cancel();
                    // Join, never detach offline recovery or lose its lock.
                    operation.await?
                }
            };
            println!(
                "logical-export verified: {} items, {} bytes",
                report.items, report.bytes
            );
            return Ok(0);
        }
        Some(Subcmd::LogicalImport(args)) => {
            let cancel = tokio_util::sync::CancellationToken::new();
            let operation = shamir_server::logical_import::import(args, cancel.clone());
            tokio::pin!(operation);
            let report = tokio::select! {
                result = &mut operation => result?,
                signal = tokio::signal::ctrl_c() => {
                    signal?;
                    cancel.cancel();
                    // Join, never detach the staged import or lose its lock.
                    operation.await?
                }
            };
            println!(
                "logical-import {} published: {} items, {} bytes{}",
                report.database,
                report.items,
                report.bytes,
                if report.published {
                    ""
                } else {
                    " (idempotent retry)"
                }
            );
            return Ok(0);
        }
        Some(Subcmd::ReplicationSeedExport { db, repo, out }) => {
            let digest =
                shamir_server::replication_seed::export(&config.data_dir, &db, &repo, &out).await?;
            println!("manifest sha256: {digest}");
            return Ok(0);
        }
        Some(Subcmd::ReplicationSeedInstall {
            db,
            repo,
            artifact,
            manifest_digest,
            subscription,
        }) => {
            let receipt = shamir_server::replication_seed::install_subscription(
                &config.data_dir,
                &db,
                &repo,
                &artifact,
                &manifest_digest,
                &subscription,
            )
            .await?;
            println!("installed manifest sha256: {}", receipt.artifact_digest);
            return Ok(0);
        }
        Some(Subcmd::ReplicationSeedVerify {
            dir,
            manifest_digest,
        }) => {
            let digest =
                tokio::task::spawn_blocking(move || shamir_server::replication_seed::verify(&dir))
                    .await??;
            if manifest_digest
                .as_ref()
                .is_some_and(|expected| expected != &digest)
            {
                anyhow::bail!("trusted manifest digest mismatch");
            }
            println!("manifest sha256: {digest}");
            return Ok(0);
        }
        Some(Subcmd::Backup { to }) => {
            let report = backup::backup(&config.data_dir, &to)?;
            println!(
                "backup ok: {} files, {} bytes → {}",
                report.files_copied,
                report.bytes_copied,
                report.dest_dir.display()
            );
            println!("format stamp: {}", report.format_stamp);
            return Ok(0);
        }
        Some(Subcmd::Restore { from, force }) => {
            let report = shamir_server::restore::restore(&from, &config.data_dir, force)?;
            println!(
                "restore ok: {} files, {} bytes restored → {}, {} user(s) had tickets invalidated",
                report.files_restored,
                report.bytes_restored,
                config.data_dir.display(),
                report.users_invalidated
            );
            if let Some(pre) = &report.pre_restore_backup {
                println!(
                    "pre-restore data preserved at {} — remove manually once satisfied",
                    pre.display()
                );
            }
            return Ok(0);
        }
        Some(Subcmd::AccessTree {
            depth,
            db,
            pretty,
            connect,
            server_name,
            user,
            password,
        }) => {
            let args = shamir_server::access_tree::AccessTreeArgs {
                depth,
                db,
                pretty,
                connect,
                server_name,
                user,
                password,
            };
            shamir_server::access_tree::run(&config, &args).await?;
            return Ok(0);
        }
        Some(Subcmd::Service { action }) => {
            match action {
                ServiceAction::Install { user } => {
                    shamir_server::service::install(&cli.config, user.as_deref())?;
                }
                ServiceAction::Uninstall => {
                    shamir_server::service::uninstall()?;
                }
                ServiceAction::Status => {
                    shamir_server::service::status()?;
                }
            }
            return Ok(0);
        }
        Some(Subcmd::Token { action }) => {
            shamir_server::tokens::cli::run(&config, action).await?;
            return Ok(0);
        }
        Some(Subcmd::Doctor {
            db,
            repo,
            table,
            apply,
            pretty,
            json,
        }) => {
            let args = doctor::DoctorArgs {
                db,
                repo,
                table,
                apply,
                pretty,
                json,
            };
            shamir_server::doctor::run(&config, &args).await?;
            return Ok(0);
        }
        Some(Subcmd::FormatMigrate(_)) => {
            anyhow::bail!("format-migrate dispatch must occur before logging initialization")
        }
        Some(Subcmd::Shell) => {
            anyhow::bail!("`shell` must be the first argument: run `shamir-server shell ...`")
        }
        Some(Subcmd::BackupVerify { .. }) => {
            anyhow::bail!(
                "`backup-verify` must be the first argument: run `shamir-server backup-verify SNAPSHOT`"
            )
        }
        Some(Subcmd::Run { .. }) | None => {}
    }

    tracing::info!(data_dir = ?config.data_dir, "shamir-server boot");

    // Construct bootstrap policy from CLI flags.
    let stdin_password = if cli.bootstrap_password_stdin {
        Some(
            tokio::task::spawn_blocking(|| bootstrap_stdin::read_password(std::io::stdin().lock()))
                .await??,
        )
    } else {
        None
    };
    let argv_password_used = bootstrap_argv_warning_needed(&cli.bootstrap_password);
    if argv_password_used {
        tracing::warn!("--bootstrap-password supplied via argv may be visible in process lists and shell history; prefer --bootstrap-password-stdin or the default random-token file");
    }
    let bootstrap = bootstrap_mode(
        cli.skip_bootstrap,
        cli.bootstrap_password,
        stdin_password,
        cli.bootstrap_user,
        cli.bootstrap_token_path,
    );

    // SIGHUP → live log-level reload.  Unix-only (no SIGHUP on Windows).
    // The task runs until the process exits; `_sighup_task` keeps the
    // `JoinHandle` alive so clippy's `let_underscore_future` is satisfied.
    #[cfg(unix)]
    let _sighup_task = {
        let config_path = cli.config.clone();
        tokio::spawn(async move {
            use tokio::signal::unix::{signal, SignalKind};
            let mut hup = match signal(SignalKind::hangup()) {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!("SIGHUP handler unavailable: {e}");
                    return;
                }
            };
            while hup.recv().await.is_some() {
                match shamir_server::config::Config::from_file(&config_path) {
                    Ok(c) => {
                        shamir_server::logging::reload(&c.logging);
                        tracing::info!(level = %c.logging.level, "SIGHUP: reloaded log level");
                    }
                    Err(e) => tracing::warn!("SIGHUP: config reload failed: {e}"),
                }
            }
        })
    };

    // The process exit code is this report's `exit_code()`; nothing forces the
    // exit. The runtime is dropped normally after `serve` returned — which is
    // why `serve` waits for the scheduled-backup worker and keeps the
    // single-instance data-directory lock until that worker finished.
    shamir_server::runtime::serve(
        config,
        bootstrap,
        shamir_server::runtime::foreground_shutdown(),
        shamir_server::runtime::notify_ready,
    )
    .await
    .map(|report| report.exit_code())
}

//! Per-command dispatch (design §§4.1, §5): the §3.3 token-subset policy,
//! session gating, and the outcome of each parsed command (connect, ping,
//! local profile commands, catalogue commands). No argv handling here.

use std::io::Write;

use serde_json::{json, Value as Json};

use super::grammar::{
    command_id, ActivityCli, AuthKind, BackupCli, Command, DbCommand, DiagCli, ExportCli,
    FunctionCommand, GroupCommand, ImportCli, IndexCommand, Mode, OnlineExportCli, Outcome,
    ProfileCommand, RepoCommand, RightsCommand, ScheduleCli, SchemaMigrationCli, ShellArgs,
    TableCommand, UserCommand,
};
use super::session::{
    check_secret_conflict, establish_profile, guarded, with_session, Ctx, Session,
};
use super::state_args::{DataCommand, ReplicationCommand, VersionCommand};
use crate::shell::commands::{
    AccessCommand, ActivityCommand, Catalogue, JobsCommand, MaintenanceCommand, StateCommand,
};
use crate::shell::connect;
use crate::shell::connect::{Profile, SessionClient};
use crate::shell::output::{self, ShellError};

/// Fixed interactive confirmation prompt for `activity cancel` and the
/// rights mutations (stderr only; never argv-derived).
const CONFIRM_PROMPT: &str = "type yes to confirm: ";

/// Closed §3.3 policy. Connect is lifecycle, not a remote data command.
fn allowed_with_token(command: &Command) -> bool {
    match command {
        Command::Connect { .. }
        | Command::Ping
        | Command::Db {
            action: DbCommand::List,
        }
        | Command::Repo {
            action: RepoCommand::List,
        }
        | Command::Table {
            action:
                TableCommand::List { .. } | TableCommand::Describe { .. } | TableCommand::Schema { .. },
        }
        | Command::Index {
            action: IndexCommand::List { .. },
        }
        | Command::Function {
            action: FunctionCommand::List { .. },
        }
        // §3.3: only the two narrow data views are in the read-only token
        // subset; `status`, `data get`, `replication subscriptions` and
        // `version show` are refused before any connect or request.
        | Command::Data {
            action: DataCommand::Count { .. } | DataCommand::Head { .. },
        } => true,
        Command::Profile { .. }
        | Command::Repo {
            action: RepoCommand::Describe { .. },
        }
        | Command::Status
        | Command::Version { .. }
        | Command::Replication { .. }
        | Command::Data {
            action: DataCommand::Get { .. },
        }
        // §3.3: the rights commands are SCRAM only.
        | Command::User { .. }
        | Command::Group { .. }
        | Command::Rights { .. }
        // §3.3: the rights mutations are SCRAM only.
        | Command::Chmod { .. }
        | Command::Chown { .. }
        | Command::Chgrp { .. }
        // §3.3: the activity commands are SCRAM only.
        | Command::Activity { .. }
        // §3.3: the maintenance commands are SCRAM only.
        | Command::Backup { .. }
        | Command::SchemaMigration { .. }
        | Command::Export { .. }
        | Command::Import { .. }
        | Command::Diag { .. } => false,
    }
}

/// Dispatches one parsed command; batch callers pass a fresh `None` session.
pub(super) fn dispatch(
    shell: &ShellArgs,
    mode: Mode,
    command: Command,
    session: &mut Option<Session>,
    runtime: &tokio::runtime::Runtime,
    ctx: &mut Ctx<'_>,
) -> Outcome {
    let id = command_id(&command);
    let auth = session.as_ref().map_or(shell.auth, |state| state.auth);
    let token = session
        .as_ref()
        .and_then(|state| state.live.as_ref())
        .map_or(auth == AuthKind::Token, |(_, client)| client.is_token());
    if token && !allowed_with_token(&command) {
        return (id, None, Err(ShellError::TokenRequestDenied));
    }
    if let (false, None, Err(err)) = (
        matches!(command, Command::Connect { .. }),
        session.as_ref(),
        auth.check(),
    ) {
        return (id, None, Err(err));
    }
    let which = match command {
        Command::Connect { name, auth } => {
            let auth = auth.unwrap_or(shell.auth);
            // Drop the old connection before any profile, prompt or dial.
            *session = Some(Session {
                auth,
                live: None,
                job_database: None,
            });
            let outcome = establish_profile(
                shell,
                mode,
                (auth, name.as_deref()),
                runtime,
                false,
                true,
                ctx,
            )
            .map(|(profile, client)| {
                *session = Some(Session {
                    auth,
                    live: Some((profile, client)),
                    job_database: None,
                });
                note(shell.verbose, "connected");
                json!({ "connected": true })
            });
            return (id, None, outcome);
        }
        Command::Ping => {
            let outcome = with_session(shell, mode, session, runtime, false, ctx)
                .and_then(|(_, client)| runtime.block_on(guarded(ping(client))));
            return (id, Some("Ping"), outcome.map(|()| json!({ "ok": true })));
        }
        Command::Profile { action } => return profile_outcome(shell, action, ctx),
        Command::Db {
            action: DbCommand::List,
        } => Catalogue::Databases,
        Command::Repo { action } => match action {
            RepoCommand::List => Catalogue::Repos,
            RepoCommand::Describe { repo } => Catalogue::DescribeRepo { repo },
        },
        Command::Table { action } => match action {
            TableCommand::List { repo } => Catalogue::Tables { repo },
            TableCommand::Describe { repo, table } => Catalogue::Describe { repo, table },
            TableCommand::Schema { repo, table } => Catalogue::Schema { repo, table },
        },
        Command::Index {
            action: IndexCommand::List { repo, table },
        } => Catalogue::Indexes { repo, table },
        Command::Function {
            action: FunctionCommand::List { folder },
        } => Catalogue::Functions { folder },
        Command::Status => {
            return state_outcome(shell, mode, session, runtime, ctx, id, StateCommand::Status)
        }
        Command::Version { action } => match action {
            VersionCommand::Show => {
                return state_outcome(
                    shell,
                    mode,
                    session,
                    runtime,
                    ctx,
                    id,
                    StateCommand::VersionShow,
                )
            }
            VersionCommand::Stamp => {
                return maintenance_outcome(
                    shell,
                    mode,
                    session,
                    runtime,
                    ctx,
                    id,
                    MaintenanceCommand::VersionStamp,
                )
            }
            VersionCommand::Migrate => {
                return maintenance_outcome(
                    shell,
                    mode,
                    session,
                    runtime,
                    ctx,
                    id,
                    MaintenanceCommand::VersionMigrate,
                )
            }
        },
        Command::Replication {
            action: ReplicationCommand::Subscriptions,
        } => {
            return state_outcome(
                shell,
                mode,
                session,
                runtime,
                ctx,
                id,
                StateCommand::ReplicationSubscriptions,
            )
        }
        Command::Data { action } => {
            return state_outcome(
                shell,
                mode,
                session,
                runtime,
                ctx,
                id,
                match action {
                    DataCommand::Count {
                        database,
                        repo,
                        table,
                    } => StateCommand::Count {
                        db: database,
                        repo,
                        table,
                    },
                    DataCommand::Head {
                        database,
                        repo,
                        table,
                        rows,
                    } => StateCommand::Head {
                        db: database,
                        repo,
                        table,
                        rows,
                    },
                    DataCommand::Get {
                        database,
                        repo,
                        table,
                        key,
                    } => StateCommand::Get {
                        db: database,
                        repo,
                        table,
                        key,
                    },
                },
            )
        }
        Command::User {
            action: UserCommand::List,
        } => {
            return access_outcome(
                shell,
                mode,
                session,
                runtime,
                ctx,
                AccessCommand::Users,
                None,
            )
        }
        Command::Group { action } => {
            return access_outcome(
                shell,
                mode,
                session,
                runtime,
                ctx,
                match action {
                    GroupCommand::Tree { db } => AccessCommand::GroupTree { db },
                    GroupCommand::Members => AccessCommand::GroupMembers,
                },
                None,
            )
        }
        Command::Rights {
            action: RightsCommand::Show { path },
        } => {
            return access_outcome(
                shell,
                mode,
                session,
                runtime,
                ctx,
                AccessCommand::RightsShow { path },
                None,
            )
        }
        Command::Chmod {
            path,
            mode: file_mode,
            yes,
        } => {
            return access_outcome(
                shell,
                mode,
                session,
                runtime,
                ctx,
                AccessCommand::Chmod {
                    path,
                    mode: file_mode,
                },
                Some(yes),
            )
        }
        Command::Chown { path, owner, yes } => {
            return access_outcome(
                shell,
                mode,
                session,
                runtime,
                ctx,
                AccessCommand::Chown { path, owner },
                Some(yes),
            )
        }
        Command::Chgrp { path, group, yes } => {
            return access_outcome(
                shell,
                mode,
                session,
                runtime,
                ctx,
                AccessCommand::Chgrp { path, group },
                Some(yes),
            )
        }
        Command::Activity { action } => {
            return activity_outcome(
                shell,
                mode,
                session,
                runtime,
                ctx,
                match action {
                    ActivityCli::Sessions { all, limit } => {
                        ActivityCommand::Sessions { all, limit }
                    }
                    ActivityCli::Requests { all, limit } => {
                        ActivityCommand::Requests { all, limit }
                    }
                    ActivityCli::Cancel { request, yes } => {
                        ActivityCommand::Cancel { request, yes }
                    }
                },
            )
        }
        Command::Backup { action } => {
            return maintenance_outcome(
                shell,
                mode,
                session,
                runtime,
                ctx,
                id,
                match action {
                    BackupCli::Start { to, yes } => MaintenanceCommand::BackupStart { to, yes },
                    BackupCli::Status => MaintenanceCommand::BackupStatus,
                    BackupCli::Cancel { yes } => MaintenanceCommand::BackupCancel { yes },
                    BackupCli::Verify { snapshot } => MaintenanceCommand::BackupVerify { snapshot },
                    BackupCli::Restore => MaintenanceCommand::BackupRestore,
                    BackupCli::Snapshot => MaintenanceCommand::BackupSnapshot,
                    BackupCli::Schedule {
                        action: ScheduleCli::Show,
                    } => {
                        return jobs_outcome(
                            shell,
                            mode,
                            session,
                            runtime,
                            ctx,
                            JobsCommand::ScheduleShow,
                        )
                    }
                },
            )
        }
        Command::SchemaMigration { action } => {
            return maintenance_outcome(
                shell,
                mode,
                session,
                runtime,
                ctx,
                id,
                match action {
                    SchemaMigrationCli::Status { database_id } => {
                        MaintenanceCommand::SchemaMigrationStatus { database_id }
                    }
                },
            )
        }
        Command::Export { action } => match action {
            ExportCli::Logical => {
                return maintenance_outcome(
                    shell,
                    mode,
                    session,
                    runtime,
                    ctx,
                    id,
                    MaintenanceCommand::ExportLogical,
                )
            }
            ExportCli::Online { action } => {
                let jobs = match action {
                    OnlineExportCli::Start {
                        database,
                        repo,
                        table,
                        schema_only,
                        include_artifacts,
                        yes,
                    } => JobsCommand::ExportStart {
                        database,
                        repo,
                        table,
                        schema_only,
                        include_artifacts,
                        yes,
                    },
                    OnlineExportCli::Status => JobsCommand::ExportStatus,
                    OnlineExportCli::Cancel { yes } => JobsCommand::ExportCancel { yes },
                };
                return jobs_outcome(shell, mode, session, runtime, ctx, jobs);
            }
        },
        Command::Import {
            action: ImportCli::Logical,
        } => {
            return maintenance_outcome(
                shell,
                mode,
                session,
                runtime,
                ctx,
                id,
                MaintenanceCommand::ImportLogical,
            )
        }
        Command::Diag { action } => {
            return maintenance_outcome(
                shell,
                mode,
                session,
                runtime,
                ctx,
                id,
                match action {
                    DiagCli::Integrity => MaintenanceCommand::DiagIntegrity,
                    DiagCli::Wal => MaintenanceCommand::DiagWal,
                    DiagCli::Durability { repo } => MaintenanceCommand::DiagDurability { repo },
                },
            )
        }
    };
    catalogue(shell, mode, session, runtime, ctx, which)
}

/// State and narrow-data commands (design §§4.2, §4.4), delegated to the
/// state worker (`shell/commands/state.rs`). The §3.3 token gate has already
/// run above; the worker's `validate` (the Base58 KEY check) runs HERE,
/// before any dial or secret read. Only `replication subscriptions` carries
/// the profile routing-database floor; `data *` name their database
/// explicitly and `status`/`version show` are server-wide.
fn state_outcome(
    shell: &ShellArgs,
    mode: Mode,
    session: &mut Option<Session>,
    runtime: &tokio::runtime::Runtime,
    ctx: &mut Ctx<'_>,
    id: &'static str,
    action: StateCommand,
) -> Outcome {
    if let Err(err) = action.validate() {
        return (id, None, Err(err));
    }
    let routing_db_required = action.requires_routing_db();
    let outcome = with_session(shell, mode, session, runtime, routing_db_required, ctx).and_then(
        |(profile, client)| {
            let routing_db = profile.database.as_deref();
            runtime.block_on(guarded(action.execute(client, routing_db)))
        },
    );
    (id, Some(action.op()), outcome)
}

/// Rights commands (design §4.3), delegated to the access worker
/// (`shell/commands/access.rs`). The §3.3 token gate has already run above;
/// the worker's `validate` (the selector parse, MODE and principal-token
/// checks) runs HERE, before any dial or secret read. For the three rights
/// mutations, the `--yes` answer in `yes` is confirmed HERE as well, so an
/// unconfirmed mutation returns BEFORE [`with_session`]: nothing is sent and
/// no request tag is computed. Every rights command carries the profile
/// routing-database floor, which must be present.
fn access_outcome(
    shell: &ShellArgs,
    mode: Mode,
    session: &mut Option<Session>,
    runtime: &tokio::runtime::Runtime,
    ctx: &mut Ctx<'_>,
    action: AccessCommand,
    yes: Option<bool>,
) -> Outcome {
    let id = action.id();
    if let Err(err) = action.validate() {
        return (id, None, Err(err));
    }
    if action.wants_confirm() {
        if let Err(err) = confirm_mutation(mode, yes.unwrap_or_default(), ctx) {
            return (id, None, Err(err));
        }
    }
    let routing_db_required = action.requires_routing_db();
    let outcome = with_session(shell, mode, session, runtime, routing_db_required, ctx).and_then(
        |(profile, client)| {
            let routing_db = profile.database.as_deref().ok_or(ShellError::Usage)?;
            runtime.block_on(guarded(action.execute(client, Some(routing_db))))
        },
    );
    (id, Some(action.op()), outcome)
}

/// Activity commands (design §4.5), delegated to the activity worker
/// (`shell/commands/activity.rs`). The §3.3 token gate has already run
/// above; the activity commands are SCRAM only. The worker's `validate`
/// (the `--limit` and request-id checks) runs HERE, before any dial, and so
/// does the `activity cancel` confirmation: a batch-mode refusal without
/// `--yes` is the typed `ConfirmationRequired` refusal and returns BEFORE
/// [`with_session`], so no request is sent at all.
fn activity_outcome(
    shell: &ShellArgs,
    mode: Mode,
    session: &mut Option<Session>,
    runtime: &tokio::runtime::Runtime,
    ctx: &mut Ctx<'_>,
    action: ActivityCommand,
) -> Outcome {
    let id = action.id();
    if let Err(err) = action.validate() {
        return (id, None, Err(err));
    }
    if action.wants_confirm() {
        if let Err(err) = confirm_cancel(mode, &action, ctx) {
            return (id, None, Err(err));
        }
    }
    let routing_db_required = action.requires_routing_db();
    let outcome = with_session(shell, mode, session, runtime, routing_db_required, ctx).and_then(
        |(profile, client)| {
            let routing_db = profile.database.as_deref().ok_or(ShellError::Usage)?;
            runtime.block_on(guarded(action.execute(client, routing_db)))
        },
    );
    (id, Some(action.op()), outcome)
}

/// The maintenance commands (design §§4.7–4.10), validated and confirmed
/// before any dial, with the explained or unsupported ones answered locally.
fn maintenance_outcome(
    shell: &ShellArgs,
    mode: Mode,
    session: &mut Option<Session>,
    runtime: &tokio::runtime::Runtime,
    ctx: &mut Ctx<'_>,
    id: &'static str,
    action: MaintenanceCommand,
) -> Outcome {
    if let Err(err) = action.validate() {
        return (id, None, Err(err));
    }
    if let Some(local) = action.local() {
        return (id, None, local);
    }
    if action.wants_confirm() {
        if let Err(err) = ctx.confirm(mode, action.yes(), CONFIRM_PROMPT) {
            return (id, None, Err(err));
        }
    }
    let routing_db_required = action.requires_routing_db();
    let outcome = with_session(shell, mode, session, runtime, routing_db_required, ctx).and_then(
        |(profile, client)| {
            let routing_db = profile.database.as_deref();
            if routing_db_required && routing_db.is_none() {
                return Err(ShellError::Usage);
            }
            runtime.block_on(guarded(action.execute(client, routing_db)))
        },
    );
    (id, action.op(), outcome)
}

fn jobs_outcome(
    shell: &ShellArgs,
    mode: Mode,
    session: &mut Option<Session>,
    runtime: &tokio::runtime::Runtime,
    ctx: &mut Ctx<'_>,
    action: JobsCommand,
) -> Outcome {
    let id = action.id();
    if mode == Mode::Batch
        && matches!(
            &action,
            JobsCommand::ExportStatus | JobsCommand::ExportCancel { .. }
        )
    {
        return (id, None, Err(ShellError::ExportSessionOnly));
    }
    if action.wants_confirm() {
        if let Err(err) = ctx.confirm(mode, action.yes(), CONFIRM_PROMPT) {
            return (id, Some(action.op()), Err(err));
        }
    }
    // Owned clone: `with_session` needs `&mut session` below.
    let job_database = session
        .as_ref()
        .and_then(|state| state.job_database.clone());
    let routing_db_required = action.requires_routing_db();
    let outcome = with_session(shell, mode, session, runtime, routing_db_required, ctx).and_then(
        |(profile, client)| {
            runtime.block_on(guarded(action.execute(
                client,
                job_database.as_deref(),
                profile.database.as_deref(),
                mode == Mode::Batch,
            )))
        },
    );
    // An interactive start RECORDS the database of the job this session
    // started; its later status/cancel address that database
    // (`jobs.rs::status_envelope`).
    if let (JobsCommand::ExportStart { database, .. }, Mode::Interactive, Ok(_)) =
        (&action, mode, &outcome)
    {
        if let Some(state) = session.as_mut() {
            state.job_database = Some(database.clone());
        }
    }
    (id, Some(action.op()), outcome)
}

/// Local confirmation for one destructive command. An explicit `--yes` is
/// accepted; batch mode without it is refused before any request, while
/// interactive mode requires the fixed `yes` prompt.
fn confirm_cancel(
    mode: Mode,
    action: &ActivityCommand,
    ctx: &mut Ctx<'_>,
) -> Result<(), ShellError> {
    ctx.confirm(
        mode,
        matches!(action, ActivityCommand::Cancel { yes: true, .. }),
        CONFIRM_PROMPT,
    )
}

/// Local confirmation for one rights mutation `chmod` / `chown` / `chgrp`:
/// the same shared [`Ctx::confirm`] and the same fixed prompt as
/// `activity cancel`, so an unconfirmed mutation is refused before any dial
/// and nothing is sent.
fn confirm_mutation(mode: Mode, yes: bool, ctx: &mut Ctx<'_>) -> Result<(), ShellError> {
    ctx.confirm(mode, yes, CONFIRM_PROMPT)
}

/// cancel-safe: NO — the request may already be sent; the caller drops the session.
async fn ping(client: &SessionClient) -> Result<(), ShellError> {
    client.ping().await.map_err(connect::request_error)
}

/// Catalogue command; the `database` floor is validated in [`with_session`].
fn catalogue(
    shell: &ShellArgs,
    mode: Mode,
    session: &mut Option<Session>,
    runtime: &tokio::runtime::Runtime,
    ctx: &mut Ctx<'_>,
    which: Catalogue,
) -> Outcome {
    let (id, op) = (which.id(), Some(which.op()));
    let outcome =
        with_session(shell, mode, session, runtime, true, ctx).and_then(|(profile, client)| {
            let database = profile.database.as_deref().ok_or(ShellError::Usage)?;
            runtime.block_on(guarded(async { which.execute(client, database).await }))
        });
    (id, op, outcome)
}

/// Local profile-directory commands; the pin is never displayed.
fn profile_outcome(shell: &ShellArgs, action: ProfileCommand, ctx: &Ctx<'_>) -> Outcome {
    // Local commands never read a secret, but conflicting secret-source
    // selectors are still a typed refusal.
    if let Err(err) = check_secret_conflict(shell) {
        return ("profile", None, Err(err));
    }
    match action {
        ProfileCommand::List => match connect::list() {
            Ok(names) => (
                "profile list",
                None,
                Ok(json!({
                    "profiles": names
                        .iter()
                        .map(|name| json!(output::sanitize(name)))
                        .collect::<Vec<Json>>()
                })),
            ),
            Err(err) => ("profile list", None, Err(err)),
        },
        ProfileCommand::Show { name } => {
            let loaded = ctx.load_profile(&name);
            match loaded {
                Ok(profile) => ("profile show", None, Ok(profile_record(&name, &profile))),
                Err(err) => ("profile show", None, Err(err)),
            }
        }
    }
}

/// Projects one profile to the output schema; the pin is a presence flag.
fn profile_record(name: &str, profile: &Profile) -> Json {
    json!({
        "name": output::sanitize(name),
        "address": output::sanitize(&profile.address),
        "transport": output::sanitize(&profile.transport),
        "username": profile.username.as_deref().map(output::sanitize),
        "database": profile.database.as_deref().map(output::sanitize),
        "pin_present": profile.pin.is_some(),
    })
}

/// One verbose informational line on stderr only (never secret material).
fn note(verbose: bool, text: &str) {
    if verbose {
        let mut stderr = std::io::stderr();
        let _ = writeln!(stderr, "{text}");
    }
}

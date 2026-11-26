//! Session establishment and the secret-source policy (design §§4.1, §6):
//! the sticky session state, the injected context behind [`run_scripted`],
//! profile validation, secret-source resolution, and the interruption /
//! request-budget wrappers around every await.

use std::future::Future;
use std::io::{Read, Write};
use std::path::Path;

use zeroize::Zeroizing;

use super::grammar::{AuthKind, Mode, ShellArgs};
use crate::shell::connect;
use crate::shell::connect::{Profile, SessionClient};
use crate::shell::output::ShellError;
use crate::shell::terminal::secret::Terminal;
use crate::shell::terminal::{self, SecretSource};

/// An attempted session without a live client stays dead until explicit connect.
pub(super) struct Session {
    pub(super) auth: AuthKind,
    pub(super) live: Option<(Profile, SessionClient)>,
    /// The database of the export job THIS session started (interactive
    /// `export online start`): the envelope database of its later
    /// `export online status` / `cancel` (`jobs.rs::status_envelope`).
    pub(super) job_database: Option<String>,
}

/// Injectable environment behind [`super::run_scripted`]: injected input,
/// stderr, terminal adapter and profile root (see the fields).
pub(super) struct Ctx<'a> {
    pub(super) input: &'a mut dyn Read,
    pub(super) stderr: &'a mut dyn Write,
    pub(super) terminal: &'a dyn Terminal,
    pub(super) profile_root: Option<&'a Path>,
}

impl Ctx<'_> {
    /// One fixed, locally authored text block to the context's stderr.
    pub(super) fn write_fixed(&mut self, text: &str) {
        let _ = self.stderr.write_all(text.as_bytes());
        let _ = self.stderr.flush();
    }

    /// Loads one profile through the injected root (or operator directory).
    pub(super) fn load_profile(&self, name: &str) -> Result<Profile, ShellError> {
        match self.profile_root {
            Some(root) => connect::load_from(root, name),
            None => connect::load(name),
        }
    }

    /// Local confirmation for one state-changing command. An explicit `yes`
    /// accepts; batch mode without it is the fixed typed
    /// [`ShellError::ConfirmationRequired`] refusal; interactive mode writes
    /// `prompt` and accepts only one bounded line whose trimmed text is
    /// exactly `yes` (anything else, including EOF, is the same refusal).
    pub(super) fn confirm(
        &mut self,
        mode: Mode,
        yes: bool,
        prompt: &str,
    ) -> Result<(), ShellError> {
        if yes {
            return Ok(());
        }
        match mode {
            Mode::Batch => Err(ShellError::ConfirmationRequired),
            Mode::Interactive => {
                let _ = self.stderr.write_all(prompt.as_bytes());
                let _ = self.stderr.flush();
                match super::input::read_line_bounded(&mut *self.input) {
                    super::input::ReadLine::Line(line) if line.trim() == "yes" => Ok(()),
                    _ => Err(ShellError::ConfirmationRequired),
                }
            }
        }
    }
}

/// Ensures a live session (implicit dial when none); the routing database
/// is validated before any dial or secret read; a failed implicit dial
/// sticks the disconnected sentinel.
pub(super) fn with_session<'a>(
    shell: &ShellArgs,
    mode: Mode,
    session: &'a mut Option<Session>,
    runtime: &tokio::runtime::Runtime,
    require_database: bool,
    ctx: &mut Ctx<'_>,
) -> Result<(&'a Profile, &'a SessionClient), ShellError> {
    if session.is_none() {
        let auth = shell.auth;
        *session = Some(Session {
            auth,
            live: None,
            job_database: None,
        });
        let live = establish_profile(
            shell,
            mode,
            (auth, None),
            runtime,
            require_database,
            false,
            ctx,
        )?;
        *session = Some(Session {
            auth,
            live: Some(live),
            job_database: None,
        });
    }
    let (profile, client) = session
        .as_ref()
        .and_then(|state| state.live.as_ref())
        .ok_or(ShellError::Connection)?;
    Ok((profile, client))
}

/// Validate the profile and transport before reading any secret.
pub(super) fn establish_profile(
    shell: &ShellArgs,
    mode: Mode,
    (auth, name): (AuthKind, Option<&str>),
    runtime: &tokio::runtime::Runtime,
    require_database: bool,
    explicit_auth: bool,
    ctx: &mut Ctx<'_>,
) -> Result<(Profile, SessionClient), ShellError> {
    auth.check()?;
    let name = name.or(shell.profile.as_deref()).ok_or(ShellError::Usage)?;
    let profile = ctx.load_profile(name)?;
    if require_database && profile.database.is_none() {
        return Err(ShellError::Usage);
    }
    let client = match auth {
        AuthKind::Scram => {
            let connection = connect::prepare(&profile)?;
            let secret = read_secret(shell, mode, auth, ctx)?;
            runtime.block_on(interruptible(async move {
                connection
                    .connect(secret)
                    .await
                    .map(SessionClient::Scram)
                    .map_err(connect::request_error)
            }))?
        }
        AuthKind::Token => {
            let connection = connect::prepare_token(&profile)?;
            let secret = read_secret(shell, mode, auth, ctx)?;
            runtime.block_on(interruptible(connection.connect(secret, explicit_auth)))?
        }
        AuthKind::Browser | AuthKind::HttpLogin => return Err(ShellError::Deferred),
        AuthKind::Resume => return Err(ShellError::Resume),
    };
    Ok((profile, client))
}

fn read_secret(
    shell: &ShellArgs,
    mode: Mode,
    auth: AuthKind,
    ctx: &mut Ctx<'_>,
) -> Result<Zeroizing<Vec<u8>>, ShellError> {
    let source = secret_source(shell, mode, auth)?;
    terminal::secret::read_secret_with_writer(source, ctx.terminal, ctx.stderr)
}

/// Resolves the secret source: no clap ArgGroup — multiple explicit
/// selectors are the typed [`ShellError::SecretSourceConflict`] refusal;
/// the hidden prompt is the default ONLY interactively; `--secret-fd` is
/// Unix-only and refused before any secret read or network activity.
fn secret_source(
    shell: &ShellArgs,
    mode: Mode,
    auth: AuthKind,
) -> Result<SecretSource, ShellError> {
    check_secret_conflict(shell)?;
    #[cfg(windows)]
    if shell.secret_fd.is_some() {
        return Err(ShellError::SecretFdWindows);
    }
    if let Some(fd) = shell.secret_fd {
        let fd = i32::try_from(fd).map_err(|_| ShellError::Usage)?;
        return Ok(SecretSource::Fd(fd));
    }
    if shell.secret_stdin {
        return Ok(SecretSource::Stdin);
    }
    if let Some(name) = shell.secret_env.as_deref() {
        return match (auth, name) {
            (AuthKind::Scram, "SHAMIR_SHELL_PASSWORD") => Ok(SecretSource::Env),
            (AuthKind::Token, "SHAMIR_SHELL_TOKEN") => Ok(SecretSource::EnvToken),
            _ => Err(ShellError::Usage),
        };
    }
    if shell.secret_prompt || mode == Mode::Interactive {
        Ok(SecretSource::Prompt)
    } else {
        Err(ShellError::InsecureSecret)
    }
}

/// Mutual-exclusion check for the secret-source selectors; also called by
/// the local `profile` commands (which never read a secret).
pub(super) fn check_secret_conflict(shell: &ShellArgs) -> Result<(), ShellError> {
    let explicit = usize::from(shell.secret_stdin)
        + usize::from(shell.secret_fd.is_some())
        + usize::from(shell.secret_env.is_some())
        + usize::from(shell.secret_prompt);
    if explicit > 1 {
        return Err(ShellError::SecretSourceConflict);
    }
    Ok(())
}

/// Connectors own the dial and handshake budgets; this only adds Ctrl+C.
/// cancel-safe: NO — the caller abandons the client on cancellation.
pub(super) async fn interruptible<T>(
    future: impl Future<Output = Result<T, ShellError>>,
) -> Result<T, ShellError> {
    tokio::select! {
        outcome = future => outcome,
        _ = tokio::signal::ctrl_c() => Err(ShellError::Interrupted),
    }
}

/// The handshake-phase budget (TLS + authentication) is owned entirely by
/// the connectors in [`crate::shell::connect::token`]; per-command requests use the
/// 60 s request deadline below. Expiry of that budget is a CONNECTION
/// failure: a locally elapsed budget proves nothing about dispatch, and §5
/// exit 7 (timeout after send / unknown outcome) is reserved for an actual
/// returned HTTP 408 or an elapsed per-command request deadline.
///
/// Apply the request deadline only after establishment. On expiry the caller
/// drops the session rather than retrying an operation that may have been sent.
pub(super) async fn guarded<T>(
    future: impl Future<Output = Result<T, ShellError>>,
) -> Result<T, ShellError> {
    tokio::time::timeout(connect::REQUEST_TIMEOUT, interruptible(future))
        .await
        .map_err(|_| ShellError::Timeout)?
}

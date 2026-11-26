/// The stage of the shutdown-owner wait that expired. Carried TYPED in
/// [`ShutdownReport::expired_owner_wait`] so callers can branch on the stage
/// without substring matching; [`Self::as_str`] is the exact operator text
/// used in the report entry (`shutdown owner wait expired (<as_str>: ...)`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ShutdownWaitStage {
    /// The scheduled backup worker's completion signal.
    ScheduledBackupJoin,
    /// The owned mutation workers.
    MutationWorkersJoin,
    /// The online-export drain handle.
    OnlineExportDrain,
    /// The release-tail machinery (reaper join or tail drain) not settling.
    ReleaseTailDrain,
}

impl ShutdownWaitStage {
    /// The stage label as it appears in the report entry text.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ScheduledBackupJoin => "scheduled backup worker join",
            Self::MutationWorkersJoin => "owned mutation workers join",
            Self::OnlineExportDrain => "online export drain",
            Self::ReleaseTailDrain => "release tail drain",
        }
    }
}

/// Repository-level clean-horizon outcomes plus shutdown-wide failures.
/// A pending repository is never mistaken for one whose checkpoint succeeded.
#[derive(Clone, Debug, Default)]
pub struct ShutdownReport {
    repositories: Vec<(String, Option<Result<(), String>>)>,
    errors: Vec<String>,
    /// The stage whose bounded owner wait expired, if any. Set ONLY together
    /// with the matching `shutdown owner wait expired (` error entry; while it
    /// is `Some`, the data-directory instance lock is deliberately leaked so
    /// the OS holds it until the process exits.
    expired_owner_wait: Option<ShutdownWaitStage>,
}

impl ShutdownReport {
    pub fn pending(repositories: Vec<String>) -> Self {
        Self {
            repositories: repositories.into_iter().map(|name| (name, None)).collect(),
            errors: Vec::new(),
            expired_owner_wait: None,
        }
    }

    /// Record that the bounded shutdown-owner wait for `stage` expired.
    /// Sets the typed stage AND pushes `error` verbatim — the entry's text
    /// (fixed prefix `shutdown owner wait expired (`) stays part of the
    /// operator contract. The report stays unclean, so the exit code remains
    /// the abnormal 70.
    pub fn set_expired_owner_wait(&mut self, stage: ShutdownWaitStage, error: impl Into<String>) {
        self.expired_owner_wait = Some(stage);
        self.errors.push(error.into());
    }

    /// The stage whose bounded owner wait expired, if any.
    pub fn expired_owner_wait(&self) -> Option<ShutdownWaitStage> {
        self.expired_owner_wait
    }

    pub fn record(&mut self, repository: &str, result: Result<(), String>) {
        if let Some((_, outcome)) = self
            .repositories
            .iter_mut()
            .find(|(name, _)| name == repository)
        {
            *outcome = Some(result);
        } else {
            self.repositories.push((repository.into(), Some(result)));
        }
    }

    pub fn add_error(&mut self, error: impl Into<String>) {
        self.errors.push(error.into());
    }

    pub fn errors(&self) -> &[String] {
        &self.errors
    }

    /// Borrows repository names and error text from this report.
    pub fn repository_failures(&self) -> Vec<(&str, &str)> {
        self.repositories
            .iter()
            .filter_map(|(name, outcome)| match outcome {
                Some(Ok(())) => None,
                Some(Err(error)) => Some((name.as_str(), error.as_str())),
                None => Some((name.as_str(), "shutdown did not complete")),
            })
            .collect()
    }

    pub fn is_clean(&self) -> bool {
        self.errors.is_empty()
            && self
                .repositories
                .iter()
                .all(|(_, outcome)| matches!(outcome, Some(Ok(()))))
    }

    /// Supervisor contract: 70 means shutdown did not complete cleanly.
    pub fn exit_code(&self) -> u8 {
        if self.is_clean() {
            0
        } else {
            70
        }
    }
}

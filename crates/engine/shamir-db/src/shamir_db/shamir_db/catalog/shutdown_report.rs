/// The stage of a bounded shutdown wait that expired. Carried TYPED in
/// [`ShutdownReport`] so callers can branch on the stage without substring
/// matching; [`Self::as_str`] is the exact operator label used in the derived
/// report entry (`shutdown owner wait expired (<as_str>: ...)`).
///
/// The four original variants keep their meaning; the later variants name the
/// stages the settlement decision also counts as NOT proof that the shutdown
/// work ended (outer deadline expiry, store-release verdict, generic
/// teardown quiescence, the HTTP listener drains, the meta-snapshot join).
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
    /// The embedding outer shutdown deadline expired and the bounded teardown
    /// future was abandoned.
    OuterShutdownDeadline,
    /// The store-release barrier did not return a clean `Released` verdict.
    StoreReleaseProbe,
    /// A teardown stage inside the bounded future (listeners, connections,
    /// replication, audit, observability) did not prove quiescence.
    TeardownQuiescence,
    /// The HTTP login-worker drain left workers detached.
    HttpLoginWorkerDrain,
    /// The HTTP login audit consumer did not settle within its bound.
    HttpAuditConsumerDrain,
    /// The periodic meta-snapshot task did not stop within its bound (its
    /// `server_meta` store handle may still be held).
    MetaSnapshotJoin,
}

impl ShutdownWaitStage {
    /// The stage label as it appears in the derived report entry text.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ScheduledBackupJoin => "scheduled backup worker join",
            Self::MutationWorkersJoin => "owned mutation workers join",
            Self::OnlineExportDrain => "online export drain",
            Self::ReleaseTailDrain => "release tail drain",
            Self::OuterShutdownDeadline => "outer shutdown deadline",
            Self::StoreReleaseProbe => "store release probe",
            Self::TeardownQuiescence => "teardown quiescence",
            Self::HttpLoginWorkerDrain => "http login worker drain",
            Self::HttpAuditConsumerDrain => "http login audit consumer drain",
            Self::MetaSnapshotJoin => "meta snapshot join",
        }
    }
}

/// One recorded expired bounded wait: the typed stage and the budget the
/// wait was given. The report keeps these IN ORDER of occurrence.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExpiredOwnerWait {
    pub stage: ShutdownWaitStage,
    pub budget: std::time::Duration,
}

/// Repository-level clean-horizon outcomes plus shutdown-wide failures.
/// A pending repository is never mistaken for one whose checkpoint succeeded.
#[derive(Clone, Debug, Default)]
pub struct ShutdownReport {
    repositories: Vec<(String, Option<Result<(), String>>)>,
    errors: Vec<String>,
    /// The bounded owner waits that expired, in order of occurrence. While
    /// this is non-empty, the data-directory instance lock is deliberately
    /// leaked so the OS holds it until the process exits.
    expired_owner_waits: Vec<ExpiredOwnerWait>,
}

impl ShutdownReport {
    pub fn pending(repositories: Vec<String>) -> Self {
        Self {
            repositories: repositories.into_iter().map(|name| (name, None)).collect(),
            errors: Vec::new(),
            expired_owner_waits: Vec::new(),
        }
    }

    /// Record that the bounded shutdown wait for `stage` expired after
    /// `budget`. The entry text is DERIVED here (fixed prefix
    /// `shutdown owner wait expired (` — part of the operator contract) and
    /// says that a wait EXPIRED; it never claims the stage's work is
    /// necessarily still running. The report stays unclean, so the exit code
    /// remains the abnormal 70, and the data-directory instance lock is kept
    /// until the process exits.
    pub fn record_expired_owner_wait(
        &mut self,
        stage: ShutdownWaitStage,
        budget: std::time::Duration,
    ) {
        self.expired_owner_waits
            .push(ExpiredOwnerWait { stage, budget });
        self.errors.push(format!(
            "shutdown owner wait expired ({}: waited {budget:?}): the bounded wait for that \
             stage EXPIRED before it could prove its work ended, so the data-directory \
             instance lock is KEPT until the process exits — check for a running backup or \
             export job, then restart the server",
            stage.as_str()
        ));
    }

    /// Every expired bounded wait, in order of occurrence.
    pub fn expired_owner_waits(&self) -> &[ExpiredOwnerWait] {
        &self.expired_owner_waits
    }

    /// The FIRST expired bounded wait, if any.
    pub fn first_expired_owner_wait(&self) -> Option<ExpiredOwnerWait> {
        self.expired_owner_waits.first().copied()
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
        self.expired_owner_waits.is_empty()
            && self.errors.is_empty()
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

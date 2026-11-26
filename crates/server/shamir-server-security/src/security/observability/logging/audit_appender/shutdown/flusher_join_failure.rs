//! Typed failure result for the audit appender background flusher join.

/// How the background flusher's join ended, when it did not end cleanly.
///
/// The two shapes must not be re-derived from message text: an inner job
/// failure was reported by a task that COMPLETED (its blocking work has
/// ended and the appender is latched fail-closed), while a lost task proves
/// nothing about its blocking work.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FlusherJoinFailure {
    /// The task joined and reported that its INNER blocking job failed (for
    /// example a panic inside the blocking flush): entries the job had
    /// already taken may be lost, the appender is latched fail-closed, and
    /// the bounded server teardown MAY still submit its final close.
    InnerBlockingJobFailed(String),
    /// The OUTER task did not join (it panicked or was cancelled): nothing
    /// proves its blocking work stopped, so the bounded server teardown
    /// skips its final close. (The standalone
    /// `FjallAuditAppender::shutdown` wrapper keeps its own final-flush
    /// attempt for either shape.)
    TaskLost(String),
}

impl FlusherJoinFailure {
    /// The failure text for the shutdown report.
    pub fn into_message(self) -> String {
        match self {
            Self::InnerBlockingJobFailed(message) | Self::TaskLost(message) => message,
        }
    }
}

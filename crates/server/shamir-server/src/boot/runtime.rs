//! Runtime modes — the reusable `serve` core and foreground shutdown signal.
//!
//! Every runtime mode (foreground, Windows service, Linux systemd) funnels
//! into [`serve`], which boots a [`ServerLauncher`] and waits for an
//! arbitrary shutdown future. Only the shutdown trigger differs between
//! modes; the boot + graceful-drain logic is identical.

#[cfg(test)]
use std::future::Future;
#[cfg(test)]
use std::pin::Pin;

use shamir_db::ShutdownReport;

use crate::config::Config;
use crate::server::{BootstrapMode, ServerHandle, ServerLauncher};

/// Maximum time to wait for in-flight connections to drain after receiving
/// a shutdown signal. If the deadline expires, we return an unclean report.
///
/// A single 1 s reserve is kept for late-stage WAITs. Each ends at the earlier
/// of stage start + cap and outer deadline minus reserve: 5 s / 500 ms for the
/// metadata join, 10 s / 250 ms for store release. These bound WAITs only;
/// synchronous work is not interrupted, and the detached metadata join may
/// continue after timeout. Report propagation retains the remaining reserve.
/// The scheduled-backup worker and data-directory lock remain owned outside
/// the bounded future and are awaited before `serve` returns. There is no forced
/// exit and nothing calls `std::process::exit`.
const SHUTDOWN_DEADLINE: std::time::Duration = std::time::Duration::from_secs(30);

/// Bound for dropping the tokio runtime itself on an ABNORMAL exit. When the
/// run failed (`Err`) or the shutdown report is abnormal (exit code 70), a
/// detached blocking job (a stalled final snapshot flush inside the fjall
/// store) could otherwise hold the runtime's normal destruction — and the
/// process — indefinitely after the bounded shutdown wait already returned.
/// The same 5 s bound the error path always had, now named and used by BOTH
/// `main.rs` paths and the Windows service path (see
/// [`exits_through_bounded_shutdown`]). The data-directory lock stays held on
/// that path (it is leaked by the owner guard), so the OS keeps it until the
/// process is gone.
pub const EXIT_RUNTIME_SHUTDOWN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// The process exit code for a run that PANICKED inside the polled drain
/// (unclean — the same abnormal-report semantics as a shutdown report that is
/// not clean). The Windows service maps the same panic to the same 70 as its
/// service exit status, so both runtime owners can never disagree.
pub const PANIC_EXIT_CODE: u8 = 70;

/// THE one decision for leaving through the BOUNDED runtime shutdown
/// (`shutdown_timeout(EXIT_RUNTIME_SHUTDOWN_TIMEOUT)`): the run failed at
/// boot (`boot_failed`), OR a shutdown report exists and is abnormal
/// (`report_abnormal = Some(!report.is_clean())`; `None` = no report — e.g.
/// a non-server subcommand — and never counts as abnormal). Used by BOTH
/// call sites (`main.rs` and the Windows service path) so they can never
/// disagree.
pub fn exits_through_bounded_shutdown(boot_failed: bool, report_abnormal: Option<bool>) -> bool {
    boot_failed || report_abnormal == Some(true)
}

/// The ONE source of the embedding shutdown deadline: `serve` and
/// `ServerHandle::shutdown` both read it, so the public wrapper can never
/// drift from the boot path.
pub(crate) fn drain_deadline() -> std::time::Duration {
    SHUTDOWN_DEADLINE
}

/// Boot the server, wait for `shutdown`, then drain gracefully.
///
/// This is the single entry point every runtime mode reuses. The caller
/// supplies the shutdown trigger — a `oneshot` receiver in tests,
/// [`foreground_shutdown`] in production, a SCM Stop event on Windows, etc.
///
/// After the shutdown signal fires, the drain phase is bounded by
/// [`SHUTDOWN_DEADLINE`]: the PUBLIC future `serve` resolves only after the
/// bounded teardown AND the post-deadline owner drain finish; on expiry the
/// bounded teardown future is dropped (that is the capped fallback — "always
/// awaited" is NOT true of the INNER timed teardown, only of the owner waits
/// `serve` performs afterwards). The owner (the scheduled-backup worker and
/// the data-directory instance lock) lives OUTSIDE the bounded future and is
/// still awaited before this function returns; an owner wait that itself
/// expires keeps the lock (leaked file) and the report names the stage typed.
/// There is no forced exit and nothing calls `std::process::exit`.
///
/// Order matters: the scheduled-backup worker and its in-flight run are
/// awaited BEFORE the repository flush (their original position). Only the
/// BOUND excludes them, not the order — a deadline that fires mid-teardown
/// leaves them to the owner, which awaits them after the deadline and releases
/// the data-directory lock only then. The worker wait is a completion signal,
/// not a join of the native runner thread.
pub async fn serve(
    config: Config,
    bootstrap: BootstrapMode,
    shutdown: impl std::future::Future<Output = ()>,
    on_ready: impl FnOnce(),
) -> anyhow::Result<ShutdownReport> {
    let launcher = ServerLauncher::new(config);
    let launcher = ServerLauncher {
        bootstrap,
        ..launcher
    };
    let handle = launcher
        .launch()
        .await
        .map_err(|e| anyhow::anyhow!("server boot failed: {e}"))?;

    tracing::info!(
        bound = ?handle.bound_addrs.iter().filter_map(|a| *a).collect::<Vec<_>>(),
        "shamir-server ready",
    );

    on_ready();

    shutdown.await;
    tracing::info!("shutting down");

    let report = drain_server(handle, drain_deadline()).await;
    crate::observability::record_shutdown(&report);
    Ok(report)
}

/// The embedding shutdown sequence: one bounded teardown, then the state that
/// must outlive that bound.
///
/// The shutdown future BORROWS the `ShutdownOwner` (`&mut`) instead of
/// owning it, which is what keeps both properties at once: on a normal
/// shutdown the scheduled worker and its in-flight run are awaited at their
/// original position, before the repository flush; on deadline expiry the
/// future is dropped with the owner intact, the report is marked unclean, and
/// the worker is still awaited here before the data-directory lock is released
/// by `ShutdownOwner::drain`.
///
/// The outer deadline is one absolute instant. Each late-stage WAIT ends at the
/// earlier of stage start + cap and outer deadline minus reserve (metadata join:
/// 5 s / 500 ms; store release: 10 s / 250 ms). These bound WAITs only;
/// synchronous work is not interrupted, and the detached metadata join may
/// continue after timeout. If one of the OWNER's own bounded waits expires
/// (see `shutdown_owner::record_expired_owner_wait`), the data-directory
/// lock is deliberately NOT released here: the owner guard leaks the file,
/// so the kernel holds the advisory lock until the process exits, and the
/// report names the expired stage typed (exit code 70, the abnormal-report
/// semantics).
pub(crate) async fn drain_server(
    handle: ServerHandle,
    deadline: std::time::Duration,
) -> ShutdownReport {
    let mut handle = handle;
    let mut report = handle.pending_shutdown_report();
    // Extracted BEFORE the deadline: expiry drops the teardown future, and a
    // running native backup thread plus the instance lock must survive that.
    // The future borrows the owner (`&mut`), it never takes it.
    let mut owner = handle.take_shutdown_owner();
    // ONE instant for both sides: the wait below ends exactly at it, and the
    // teardown's late stages (the meta-snapshot join, the store-release
    // barrier) spend only what is left of it, so a stage that expires at the
    // deadline still records its own verdict instead of being dropped with the
    // abandoned future.
    let deadline_at = tokio::time::Instant::now() + deadline;
    // THE settlement decision, shared with the fallback path: the outer
    // timeout is an UNSETTLED outcome (typed stage, lock kept by the owner
    // drain below); only a completed teardown with every inner settlement
    // point proven reaches the drain as `Settled`.
    let inner = match tokio::time::timeout_at(
        deadline_at,
        handle.shutdown_with_budget(&mut report, &mut owner, Some(deadline_at)),
    )
    .await
    {
        Ok(settlement) => settlement,
        Err(_) => {
            report.add_error(deadline_error(deadline));
            #[cfg(test)]
            crate::server::mark_fallback_drain_join();
            crate::server::record_expired_owner_wait(
                shamir_db::ShutdownWaitStage::OuterShutdownDeadline,
                deadline,
                &mut report,
            )
        }
    };
    // Outside the deadline: on expiry the teardown was abandoned mid-way, but
    // the worker is still awaited here and the lock is released only if every
    // settlement point — the outer timeout included — proved its work ended
    // and the report is clean.
    owner.drain(&mut report, inner).await;
    report
}

/// The one truthful deadline message, shared by both shutdown compositions.
///
/// Nothing forces an exit: the exit CODE reports the unclean stop. The message
/// text describes the production composition: `drain_server` awaits the
/// scheduled worker and releases the data-directory lock before the caller
/// returns. The test-only `finish_shutdown` does not by itself carry that
/// guarantee — its closure decides what is still awaited after the deadline.
fn deadline_error(deadline: std::time::Duration) -> String {
    format!(
        "graceful shutdown exceeded deadline {deadline:?}: the teardown stage still running \
         was abandoned; nothing forces an exit, and the scheduled backup worker and the \
         data-directory lock are still awaited before this call returns — unless an owner \
         wait itself expired (a `shutdown owner wait expired (` report entry), in which case \
         the lock is KEPT until the process exits"
    )
}

#[cfg(test)]
/// cancel-safe: NO — expiry abandons unfinished teardown, not completed outcomes.
pub(crate) async fn finish_shutdown(
    deadline: std::time::Duration,
    report: &mut ShutdownReport,
    shutdown: impl for<'a> FnOnce(
        &'a mut ShutdownReport,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>,
) -> ShutdownReport {
    if tokio::time::timeout(deadline, shutdown(report))
        .await
        .is_err()
    {
        report.add_error(deadline_error(deadline));
    }
    report.clone()
}

/// Notify the init system that the server is ready (post-bind).
///
/// On Linux under systemd (`Type=notify`), this sends `READY=1` via
/// `sd_notify`. Everywhere else (macOS, BSD, Windows, plain terminal) it
/// is a compile-time no-op.
pub fn notify_ready() {
    #[cfg(target_os = "linux")]
    {
        // Best-effort; no-op if $NOTIFY_SOCKET is unset (not under systemd).
        let _ = sd_notify::notify(&[sd_notify::NotifyState::Ready]);
    }
}

/// Resolves when the OS asks us to stop: Ctrl+C everywhere, plus SIGTERM on
/// unix (systemd / `kill` default).
pub async fn foreground_shutdown() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut term = signal(SignalKind::terminate()).expect("install SIGTERM handler");
        let ctrl_c = tokio::signal::ctrl_c();
        tokio::select! {
            _ = ctrl_c => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

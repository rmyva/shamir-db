//! Launch-rollback settlement: the data-directory lock is held by the SAME
//! leak-on-drop [`DataDirLock`] from the moment it is taken, and a failed
//! launch releases it ONLY when the rollback PROVES settlement — every
//! settlement point (the shared audit teardown, the detached-worker drains,
//! the meta-snapshot join, the ONE release-source stop, the cursor reaper,
//! and the store-release barrier over the stores this boot opened) returned
//! proof and the cleanup report is clean. An expiry or a non-clean probe
//! leaves the lock LEAKED (its `Drop` forgets the file, so the OS holds the
//! advisory lock until the process exits) and the ORIGINAL boot error is
//! returned unchanged. Reuses the graceful path's store-release barrier
//! (`release_stores_until`) instead of a sleep or a longer poll.

use std::sync::Arc;

use shamir_db::{ShutdownReport, ShutdownWaitStage};
use shamir_sync::durability::tx::tx_registry::{ReaperTask, TxRegistry};

use shamir_connect::server::audit_chain::{AuditChain, AuditChainWriter};
use shamir_connect::server::durable_counters::FjallConsumedCounters;
use shamir_connect::server::lockout::InMemoryLockoutStore;
use shamir_connect::server::rate_limit::InMemoryRateLimiter;

use super::rollback_cleanup::{
    join_cursor_reaper, join_meta_snapshot_settled, log_rollback_cleanup,
    stop_release_sources_settled, ROLLBACK_WORKER_DRAIN, STORE_RELEASE_TIMEOUT,
};
use crate::audit_appender::FjallAuditAppender;
use crate::boot::config::diagnostics::backup_online::OnlineBackupRegistry;
use crate::boot::config::diagnostics::logical::export_online::OnlineExport;
use crate::db_handler::{CancelRefusalAudit, MutationTasks, ShamirDbHandler};
use crate::scheduler::Scheduler;
use crate::server::server_handle::{
    report_store_release_outcome, DataDirLock, MetaSnapshotTask, Settlement,
};
use crate::server::server_launcher::audit_teardown::{teardown_audit, AuditTeardown};
use crate::server::store_release_barrier::{release_stores_until, ReleaseOutcome};
use crate::server_meta::ServerMetaStore;
use crate::user_directory::FjallUserDirectory;
use shamir_db::ShamirDb;

/// Everything the rollback must let go of BEFORE the store-release barrier:
/// each field holds (directly or transitively) a store this boot opened.
/// Dropping the last reference releases the store's directory lock, which is
/// what the barrier then PROVES.
pub(super) struct RollbackDrops {
    pub(super) handler: Arc<dyn shamir_connect::server::dispatch::RequestHandler>,
    pub(super) handler_concrete: Arc<ShamirDbHandler>,
    pub(super) shamir: Arc<ShamirDb>,
    pub(super) supervisor: Arc<crate::replication::SubscriptionSupervisor>,
    pub(super) token_service: Arc<crate::security::tokens::TokenService>,
    pub(super) audit_writer: Arc<AuditChainWriter>,
    pub(super) audit_appender: Arc<FjallAuditAppender>,
    pub(super) audit_chain: Arc<AuditChain>,
    pub(super) online_backup_registry: Arc<OnlineBackupRegistry>,
    pub(super) online_export: Option<Arc<OnlineExport>>,
    pub(super) meta: Arc<ServerMetaStore>,
    pub(super) user_dir: Arc<FjallUserDirectory>,
    pub(super) counters: Arc<FjallConsumedCounters>,
    pub(super) lockout: Arc<InMemoryLockoutStore>,
    pub(super) rate_limit: Arc<InMemoryRateLimiter>,
}

impl Drop for RollbackDrops {
    // The struct exists ONLY to release these references; the Drop impl
    // touches each field so the dead-code lint cannot flag them as unread.
    fn drop(&mut self) {
        let _ = (
            &self.handler,
            &self.handler_concrete,
            &self.shamir,
            &self.supervisor,
            &self.token_service,
            &self.audit_writer,
            &self.audit_appender,
            &self.audit_chain,
            &self.online_backup_registry,
            &self.online_export,
            &self.meta,
            &self.user_dir,
            &self.counters,
            &self.lockout,
            &self.rate_limit,
        );
    }
}

/// Everything the rollback owns for ONE failed launch.
pub(super) struct RollbackSettlement<'a> {
    pub(super) stage: &'static str,
    pub(super) data_dir: std::path::PathBuf,
    pub(super) cancel_refusal_audit: &'a Arc<CancelRefusalAudit>,
    pub(super) scheduler: Scheduler,
    pub(super) reaper: Option<ReaperTask>,
    pub(super) registry: &'a Arc<TxRegistry>,
    pub(super) meta_snapshot_task: Option<MetaSnapshotTask>,
    pub(super) cursor_reaper: Option<crate::cursor_registry::CursorReaperTask>,
    pub(super) owned_tracker: Arc<MutationTasks>,
    pub(super) drops: RollbackDrops,
    pub(super) lock: DataDirLock,
    pub(super) cleanup: &'a mut ShutdownReport,
}

/// The ONE settlement for a failed launch: run every teardown point, prove
/// (or fail to prove) that the stores this boot opened released their
/// directory locks, release-or-leak the instance lock accordingly, log the
/// cleanup, and leave the caller to return the ORIGINAL boot error.
///
/// Note: an already-running login SCRAM derivation (detached blocking job)
/// and the manual backup thread are known limits — see the operations guide
/// (a background-work registry is planned, task #2055).
pub(super) async fn settle_rollback(mut r: RollbackSettlement<'_>) {
    r.cancel_refusal_audit.close();
    let mut settlement = Settlement::Settled;
    // The shared audit teardown (the same one the graceful path uses).
    teardown_audit(
        AuditTeardown {
            cancel_refusal_audit: r.cancel_refusal_audit,
            appender: &r.drops.audit_appender,
            scheduler: r.scheduler,
            #[cfg(test)]
            drain_probe: None,
        },
        r.cleanup,
    )
    .await;
    // Bounded drain of the detached workers the failed boot owns. The
    // tracker is dropped BEFORE the barrier so nothing of it survives into
    // the store-release proof.
    let owned_tracker = std::mem::take(&mut r.owned_tracker);
    owned_tracker.close();
    if tokio::time::timeout(ROLLBACK_WORKER_DRAIN, owned_tracker.wait())
        .await
        .is_err()
    {
        tracing::error!("launch rollback: mutation workers still running after the drain bound");
        settlement = settlement.worst(Settlement::Unsettled(
            ShutdownWaitStage::MutationWorkersJoin,
        ));
    }
    if let Some(export) = r.drops.online_export.as_ref() {
        if !tokio::time::timeout(ROLLBACK_WORKER_DRAIN, export.wait_idle())
            .await
            .unwrap_or(false)
        {
            tracing::error!("launch rollback: the online export worker is still draining");
            settlement =
                settlement.worst(Settlement::Unsettled(ShutdownWaitStage::OnlineExportDrain));
        }
    }
    settlement = settlement
        .worst(join_meta_snapshot_settled(r.stage, r.meta_snapshot_task.take()).await)
        .worst(stop_release_sources_settled(r.stage, &mut r.reaper, r.registry, r.cleanup).await);
    drop(owned_tracker);
    join_cursor_reaper(r.cursor_reaper.take()).await;
    // Take ownership of the remaining pieces so the store holders can be
    // dropped BEFORE the barrier, and the lock decided on afterwards.
    let RollbackSettlement {
        stage,
        data_dir,
        drops,
        lock,
        cleanup,
        ..
    } = r;
    // Let go of every store this boot opened, THEN prove the release with the
    // SAME barrier the graceful shutdown uses.
    drop(drops);
    let deadline = tokio::time::Instant::now() + STORE_RELEASE_TIMEOUT;
    let outcome = release_stores_until(&data_dir, deadline).await;
    if outcome != ReleaseOutcome::Released {
        tracing::error!(
            ?outcome,
            stage,
            "launch rollback: the store-release barrier did not prove release"
        );
    }
    settlement = settlement.worst(report_store_release_outcome(
        outcome,
        STORE_RELEASE_TIMEOUT,
        cleanup,
    ));
    // PROOF required: release only when every point settled AND the cleanup
    // report is clean; otherwise the guard's Drop leaks the lock file on
    // purpose (the OS holds it until the process exits).
    if settlement == Settlement::Settled && cleanup.is_clean() {
        lock.release_clean();
    }
    log_rollback_cleanup(stage, cleanup);
}

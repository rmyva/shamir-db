use std::sync::Arc;
use tokio::task::JoinHandle;

use shamir_connect::server::audit_chain::AuditChainWriter;
use shamir_connect::server::session::Session;

use super::{spawn_detached_audit, AuditEventClass, CancelRefusalAudit, DetachedAuditJob};

/// Successful typed-read payload: scope names only.
#[derive(serde::Serialize)]
struct TypedReadAuditDetails<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    db: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    repo: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    table: Option<&'a str>,
}

/// Names-only payload for a refused read; no key, query, values or identity data.
#[derive(serde::Serialize)]
struct RefusedReadAuditDetails<'a> {
    db: &'a str,
    repo: &'a str,
    table: &'a str,
    outcome: &'a str,
}

/// Detached successful typed-read audit. This stays observability-class and is
/// unchanged in schema/admission from the existing GetRecord success event.
pub(in crate::engine::db_handler) fn enqueue_typed_read_audit(
    queue: &Arc<CancelRefusalAudit>,
    audit: &Arc<AuditChainWriter>,
    session: &Session,
    operation: &'static str,
    db: &str,
    repo_table: Option<(&str, &str)>,
) -> Option<JoinHandle<()>> {
    let db = (!db.is_empty()).then_some(db);
    let (repo, table) = match repo_table {
        Some((repo, table)) => (Some(repo), Some(table)),
        None => (None, None),
    };
    let details = match rmp_serde::to_vec_named(&TypedReadAuditDetails { db, repo, table }) {
        Ok(details) => details,
        Err(error) => {
            queue.record_drop(AuditEventClass::Observability);
            tracing::error!(%error, "audit append skipped: typed read encoding failed");
            return None;
        }
    };
    spawn_detached_audit(DetachedAuditJob {
        queue,
        audit,
        session,
        operation,
        details,
        class: AuditEventClass::Observability,
        dropped_message: "typed read audit job dropped before append completed",
        rejected_message: "audit append rejected typed read",
    })
}

/// Record one refused typed read under the existing bounded per-principal
/// live-job share (32 of 256); saturation drops/accounting follow queue policy.
pub(in crate::engine::db_handler) fn enqueue_refused_read_audit(
    queue: &Arc<CancelRefusalAudit>,
    audit: &Arc<AuditChainWriter>,
    session: &Session,
    operation: &'static str,
    scope: (&str, &str, &str),
    outcome: &str,
) -> Option<JoinHandle<()>> {
    let (db, repo, table) = scope;
    let details = match rmp_serde::to_vec_named(&RefusedReadAuditDetails {
        db,
        repo,
        table,
        outcome,
    }) {
        Ok(details) => details,
        Err(error) => {
            queue.record_drop(AuditEventClass::Security);
            tracing::error!(%error, "audit append skipped: refused read encoding failed");
            return None;
        }
    };
    spawn_detached_audit(DetachedAuditJob {
        queue,
        audit,
        session,
        operation,
        details,
        class: AuditEventClass::Security,
        dropped_message: "refused read audit job dropped before append completed",
        rejected_message: "audit append rejected refused read",
    })
}

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::query::batch::{BatchError, BatchTxPort};
use crate::tx::CommitError;
use shamir_query_types::batch::{TransactionInfo, VisibilityWait};
use shamir_tx::CancelHandle;

pub(crate) fn commit_implicit<'a, R: BatchTxPort + ?Sized>(
    repo: &'a R,
    tx: shamir_tx::TxContext,
    alias: &'a str,
    wait: VisibilityWait,
) -> Pin<Box<dyn Future<Output = Result<TransactionInfo, BatchError>> + Send + 'a>> {
    commit_implicit_with_cancel(repo, tx, alias, wait, None)
}

/// Preserve known publication, including durability warnings, as metadata.
/// The commit itself is never raced against an execution deadline.
///
/// Boxed on purpose: the commit pipeline future is large and this is awaited
/// from four write arms of the per-op runner, which recurses through nested
/// batches; inlining it would multiply the runner's stack footprint.
pub(crate) fn commit_implicit_with_cancel<'a, R: BatchTxPort + ?Sized>(
    repo: &'a R,
    tx: shamir_tx::TxContext,
    alias: &'a str,
    wait: VisibilityWait,
    cancel: Option<Arc<CancelHandle>>,
) -> Pin<Box<dyn Future<Output = Result<TransactionInfo, BatchError>> + Send + 'a>> {
    Box::pin(commit_implicit_inner(repo, tx, alias, wait, cancel))
}

/// Wire `code` for a failed implicit-batch commit, surfaced through
/// `BatchError::QueryError` and passed verbatim to `DbResponse::Error` by the
/// server's `error_code` mapper.
pub(crate) fn receipt_error_code(error: &CommitError) -> Option<&'static str> {
    match error {
        CommitError::MigrationFrozen { .. } => Some("table_migrating"),
        CommitError::UniqueViolation { .. } => Some("unique_violation"),
        CommitError::CasConflict { .. } => Some("version_conflict"),
        CommitError::PhantomConflict { .. }
        | CommitError::SsiConflict { .. }
        | CommitError::Wounded { .. } => Some("tx_conflict"),
        CommitError::WalRequiresReopen => Some("wal_requires_reopen"),
        // Attempted commit, outcome unknown — the client must reopen and
        // reconcile, never blind-retry.
        CommitError::WalOutcomeUnknown { .. } => Some("wal_requires_reopen"),
        // S5c-X: commit admission refused while the retained sealed WAL exceeds
        // `durability.wal_retention_max_bytes` under mode `refuse_writes`.
        // RETRYABLE: the refusal clears by itself once the drainer truncates
        // the WAL back under the cap; a retried commit then succeeds.
        CommitError::WalRetentionExceeded => Some("wal_retention_exceeded"),
        // R2a 1c: admission gate refused (closed past the bounded reopen cap,
        // or ACTIVE overflow). RETRYABLE: acquisition precedes `tx_gate` /
        // prelock, so nothing was mutated; a retried commit succeeds once the
        // capture barrier reopens the gate.
        CommitError::CommitAdmissionRefused => Some("commit_admission_refused"),
        CommitError::Storage(error) => error.code(),
        CommitError::Expired { .. } | CommitError::Cancelled => None,
        CommitError::WalDurability { .. } => Some("wal_durability"),
    }
}

async fn commit_implicit_inner<R: BatchTxPort + ?Sized>(
    repo: &R,
    tx: shamir_tx::TxContext,
    alias: &str,
    wait: VisibilityWait,
    cancel: Option<Arc<CancelHandle>>,
) -> Result<TransactionInfo, BatchError> {
    let tx_id = tx.tx_id.0;
    let snapshot = tx.snapshot_version;
    match repo.commit_implicit_batch(tx, wait, cancel).await {
        Ok(outcome) => Ok(TransactionInfo::committed(
            outcome.tx_id,
            outcome.snapshot_version,
            outcome.commit_version,
            outcome.materialized(),
        )
        .with_visibility(outcome.visibility)),
        Err(error @ CommitError::WalDurability { .. }) => {
            Ok(error.into_transaction_info(tx_id, snapshot))
        }
        Err(CommitError::WalOutcomeUnknown { commit_version }) => {
            Err(BatchError::WalOutcomeUnknown {
                alias: alias.into(),
                commit_version,
            })
        }
        Err(CommitError::Cancelled) => Err(BatchError::Cancelled),
        Err(error) => {
            let code = receipt_error_code(&error);
            Err(BatchError::QueryError {
                alias: alias.into(),
                message: error.to_string(),
                code: code.map(str::to_owned),
            })
        }
    }
}

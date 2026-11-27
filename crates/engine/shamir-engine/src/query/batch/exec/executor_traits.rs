//! Executor abstraction traits for the batch query engine.
//!
//! These traits are the thin contracts that decouple `QueryRunner`
//! (and the free `execute_batch*` functions) from the concrete
//! implementations that live in `shamir-db` / `shamir-server`.
//! The table-side [`TableResolver`] contract lives in
//! `table::support::table_resolver` and is re-exported here.

use crate::query::batch::{BatchError, BatchOp};
use shamir_query_types::batch::VisibilityWait;

use crate::query::read::QueryResult;
use shamir_engine_query::query::filter::read_budget::QueryMetrics;
use shamir_query_types::CallOp;
use shamir_storage::error::DbResult;
use shamir_types::access::Actor;
use shamir_types::types::common::TMap;

pub use crate::table::TableResolver;

/// Transaction lifecycle operations required by batch execution.
///
/// The facade implements this narrow port for the tx-layer `RepoCore` using
/// its explicit public batch transaction methods. Callers depend on this
/// contract rather than the coordinator's commit helper modules.
#[async_trait::async_trait]
pub trait BatchTxPort: Send + Sync {
    async fn commit_implicit_batch(
        &self,
        tx: shamir_tx::TxContext,
        wait: VisibilityWait,
        cancel: Option<std::sync::Arc<shamir_tx::CancelHandle>>,
    ) -> Result<crate::tx::TxOutcome, crate::tx::CommitError>;

    async fn commit_explicit_batch(
        &self,
        tx: shamir_tx::TxContext,
        wait: VisibilityWait,
        cancel: Option<std::sync::Arc<shamir_tx::CancelHandle>>,
    ) -> Result<crate::tx::TxOutcome, crate::tx::CommitError>;

    async fn begin_interactive_batch(
        &self,
        isolation: shamir_tx::IsolationLevel,
    ) -> shamir_storage::error::DbResult<(shamir_tx::TxContext, shamir_tx::SnapshotGuard)>;

    async fn commit_interactive_batch(
        &self,
        tx: shamir_tx::TxContext,
    ) -> Result<crate::tx::TxOutcome, crate::tx::CommitError>;

    async fn release_batch_locks(&self, tx: &shamir_tx::TxContext);
    async fn release_interactive_batch_locks(&self, tx: &shamir_tx::TxContext);
    async fn synced_flush(&self) -> DbResult<()>;
    async fn sync_wal(&self) -> DbResult<()>;
    fn default_visibility_wait(&self) -> VisibilityWait;
}

///
/// Resolves repositories to the tx-layer `RepoCore`, which implements
/// [`BatchTxPort`] in the facade through narrow public tx operations. Keeping
/// this return type preserves existing resolver implementations; callers use
/// the port contract for batch transaction lifecycle operations.
#[async_trait::async_trait]
pub trait BatchResolver: TableResolver {
    /// Resolve the tx coordinator for a repository. It implements [`BatchTxPort`].
    async fn resolve_repo(&self, repo_name: &str) -> DbResult<crate::tx::RepoCore>;

    /// Optional catalogue metadata probe used to preflight table reads.
    /// `None` means the resolver cannot answer without resolving the table.
    fn table_read_exists(
        &self,
        _db_name: &str,
        _repo_name: &str,
        _table_name: &str,
    ) -> Option<bool> {
        None
    }

    /// Optional per-repository telemetry sink for query dispatch. Implementors
    /// that do not own engine metrics may keep the default no-op behavior.
    fn query_metrics(&self, _repo_name: &str) -> Option<std::sync::Arc<dyn QueryMetrics>> {
        None
    }
}

/// Trait for executing admin (DDL) operations.
#[async_trait::async_trait]
pub trait AdminExecutor: Send + Sync {
    async fn execute_admin(&self, op: &BatchOp) -> Result<QueryResult, BatchError>;
}

/// Trait for invoking stored procedures / callable getter-functions.
///
/// The engine executor does not know how to run a WASM function — that
/// lives in `shamir-db` (`ShamirDb::invoke_function_in_db_as`). This
/// trait is the thin channel that lets `QueryRunner::run` dispatch a
/// `BatchOp::Call` to the facade without a circular dependency.
///
/// Injected alongside [`AdminExecutor`] (same pattern, same lifecycle).
#[async_trait::async_trait]
pub trait FunctionInvoker: Send + Sync {
    /// Invoke a stored procedure named by `op.call` with the given
    /// positional `op.params` (already resolved — `$query` refs
    /// expanded by the executor) under authority of `actor`, returning
    /// the function's `QueryValue` mapped into a `QueryResult.value`.
    async fn invoke_call(
        &self,
        op: &CallOp,
        actor: &Actor,
        resolved_refs: &TMap<String, QueryResult>,
    ) -> Result<QueryResult, BatchError>;
}

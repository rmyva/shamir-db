use futures_util::future::BoxFuture;
use shamir_tx::TxContext;
use std::sync::Arc;

/// Releases the Level-3 pessimistic record locks a parked transaction still holds when the
/// LAST reference to its context goes away outside the commit pipeline. Receives the taken
/// [`TxContext`]; the releaser ALREADY holds the repository resolved when the handle was parked, so
/// `RenameDb` / `RenameRepo` between begin and end cannot make it resolve nothing.
pub type TxAbortRelease = Arc<dyn Fn(TxContext) -> BoxFuture<'static, ()> + Send + Sync>;

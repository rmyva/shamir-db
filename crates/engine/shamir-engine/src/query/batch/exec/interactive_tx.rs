use std::time::Instant;

use crate::query::batch::batch_execute::{execute_plan_tx, filter_results};
use crate::query::batch::batch_validate::{
    validate_abort_on_skip_scope, validate_executable_guards, validate_tables,
};
use crate::query::batch::exec_scope::ExecScope;
use crate::query::batch::executor_traits::{AdminExecutor, BatchResolver, FunctionInvoker};
use crate::query::batch::Authorized;
#[cfg(test)]
use crate::query::batch::BatchRequest;
use crate::query::batch::BatchTxPort;
use crate::query::batch::{BatchError, BatchResponse};
#[cfg(test)]
use shamir_types::access::Actor;

// ===========================================================================
// Phase B — interactive (multi-call) transactions
//
// Phase A's `execute_transactional` opens a tx, runs ONE batch's plan, and
// commits — all inside a single call. Phase B factors that closed cycle into
// three reusable pieces so a `TxContext` can be parked server-side (the
// interactive-tx registry) and driven across multiple client round-trips:
//
//   * `open_interactive_tx`   — BEGIN: mint the tx + snapshot guard.
//   * `execute_in_open_tx`    — EXECUTE: run one batch's plan against the
//                               EXISTING tx, NO commit (the tx stays open).
//   * `commit_interactive_tx` — COMMIT: run the Phase-A commit pipeline.
//
// ROLLBACK needs no function — dropping the parked `TxContext` is the RAII
// rollback (no storage side effects). See
// `docs/dev-artifacts/roadmap/PHASE_B_INTERACTIVE_TX.md` §5. These are thin wrappers: no
// new execution logic. `execute_in_open_tx` mirrors `execute_batch`
// (plan → validate → run → filter) but routes through `execute_plan_tx`
// with the caller's `&mut TxContext` and returns `transaction: None` because
// the tx is still open — the commit outcome is produced later by
// `commit_interactive_tx`.
// ===========================================================================

/// BEGIN an interactive transaction: open the MVCC snapshot and mint the
/// `TxContext`.
///
/// The returned `SnapshotGuard` MUST be parked alongside the `TxContext` and
/// held until commit/rollback — dropping it early lets GC reclaim versions
/// the open tx still needs (see `RepoCore::begin_tx`).
pub async fn open_interactive_tx(
    repo: &impl BatchTxPort,
    iso: shamir_tx::IsolationLevel,
) -> shamir_storage::error::DbResult<(shamir_tx::TxContext, shamir_tx::SnapshotGuard)> {
    repo.begin_interactive_batch(iso).await
}

/// EXECUTE one batch inside an already-open interactive tx, WITHOUT
/// committing.
///
/// Mirrors `execute_batch` but threads the caller's existing `tx` through
/// `execute_plan_tx`; the returned `BatchResponse` always has
/// `transaction: None` (the tx remains open — the commit outcome is reported
/// later by [`commit_interactive_tx`]).
///
/// The single-repo guard is enforced here exactly as in `execute_batch`;
/// the caller additionally asserts that the batch targets the SAME repo the
/// handle is pinned to (the engine tx is committed against one repo).
///
/// Each call has a cooperative `max_execution_time_secs` budget, just like
/// `execute_batch` (30 seconds by default). This replaces the former
/// multi-roundtrip rationale for unbounded execution: a per-call bound is
/// safe because execution returns an ordinary `ExecutionTimedOut` error,
/// without cancelling a future or committing the transaction. The tx stays
/// open; the caller must roll it back/drop it on timeout, including any
/// writes staged by earlier calls. Existing safe execution checkpoints
/// enforce the bound; they do not preempt a stalled operation.
/// The transaction's lifetime across calls is a different budget, owned by
/// the caller, not this per-call deadline. Commit/WAL are not timed here.
///
/// Takes an [`Authorized`] token, not a raw `&BatchRequest` + `Actor` pair
/// (#1199) — see `execute_batch`'s doc and `authorized.rs`'s module doc.
pub async fn execute_in_open_tx(
    auth: Authorized<'_>,
    resolver: &dyn BatchResolver,
    admin: Option<&dyn AdminExecutor>,
    invoker: Option<&dyn FunctionInvoker>,
    tx: &mut shamir_tx::TxContext,
) -> Result<BatchResponse, BatchError> {
    let scope = auth.root_scope();
    execute_in_open_tx_with_scope(auth, resolver, admin, invoker, tx, &scope).await
}

// Share the entire entry implementation so tests can inject an already-expired
// deadline without sleeps or a global clock mock. Production always constructs
// it from policy-clamped request limits above; validation, planning and
// execution stay shared.
#[allow(clippy::too_many_arguments)]
pub(in crate::query::batch) async fn execute_in_open_tx_with_scope(
    auth: Authorized<'_>,
    resolver: &dyn BatchResolver,
    admin: Option<&dyn AdminExecutor>,
    invoker: Option<&dyn FunctionInvoker>,
    tx: &mut shamir_tx::TxContext,
    scope: &ExecScope,
) -> Result<BatchResponse, BatchError> {
    let repos = &auth.metadata().repos;
    validate_abort_on_skip_scope(&auth.request().queries)?;
    if repos.len() > 1 {
        let mut repos: Vec<String> = repos.iter().cloned().collect();
        repos.sort();
        return Err(BatchError::CrossRepoNotSupported { repos });
    }

    let (request, actor, db_name) = auth.into_parts();
    let actor = &actor;
    let start = Instant::now();
    let effective = request.limits.clamped_to(scope.limits());
    let mut plan = shamir_query_types::batch::BatchPlanner::plan(&request.queries, &effective)?;
    validate_tables(&request.queries, resolver, &effective, scope.limits()).await?;
    // #670: mirrors execute_batch_impl / run_nested_body_in_outer_tx — this
    // path previously skipped the filter-nesting-depth DoS guard, letting an
    // interactive-tx client submit arbitrarily deeply nested filters.
    validate_executable_guards(&request.queries)?;

    let all_results = execute_plan_tx(
        &mut plan,
        &request.queries,
        resolver,
        admin,
        invoker,
        actor,
        db_name,
        tx,
        scope,
    )
    .await?;

    let results = filter_results(all_results, request);
    let elapsed = start.elapsed();

    Ok(BatchResponse {
        id: request.id.clone(),
        results,
        execution_plan: std::mem::take(&mut plan.stages),
        edge_provenance: std::mem::take(&mut plan.edge_provenance),
        execution_time_us: elapsed.as_micros() as u64,
        // The tx is still open — there is no commit outcome yet.
        transaction: None,
        // Ambient interner deltas are attached server-side (shamir-db); empty here.
        interner_delta: Default::default(),
    })
}

/// Test-only convenience: calls [`execute_in_open_tx`] via
/// [`Authorized::unchecked`] (no access-control check), preserving the
/// PRE-#1199 call shape. See [`crate::query::batch::batch_execute::execute_batch_unchecked`]'s
/// doc for why this exists — same rationale, tx-aware variant.
#[cfg(test)]
pub(crate) async fn execute_in_open_tx_unchecked(
    request: &BatchRequest,
    resolver: &dyn BatchResolver,
    admin: Option<&dyn AdminExecutor>,
    invoker: Option<&dyn FunctionInvoker>,
    actor: &Actor,
    db_name: &str,
    tx: &mut shamir_tx::TxContext,
) -> Result<BatchResponse, BatchError> {
    let auth = Authorized::unchecked(request, actor.clone(), db_name);
    execute_in_open_tx(auth, resolver, admin, invoker, tx).await
}

/// COMMIT an interactive tx: run the full Phase-A commit pipeline.
///
/// The caller has already removed the `TxContext` from the registry (and
/// dropped any per-handle lock); the `SnapshotGuard` is dropped by the
/// caller only AFTER this returns — the snapshot must stay alive through
/// commit so SSI read-set validation and history reads remain correct.
pub async fn commit_interactive_tx(
    repo: &impl BatchTxPort,
    tx: shamir_tx::TxContext,
) -> Result<crate::tx::TxOutcome, crate::tx::CommitError> {
    repo.commit_interactive_batch(tx).await
}

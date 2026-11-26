use std::time::Instant;

use crate::query::batch::exec_scope::ExecScope;
use crate::query::batch::execution_deadline::warn_if_op_slow;
use crate::query::batch::executor_traits::{AdminExecutor, BatchResolver, FunctionInvoker};
use crate::query::batch::op_watchdog::register_op_watchdog;
use crate::query::batch::query_runner::{
    abort_reason, build_resolved_refs, resolve_skip, skip_or_abort, skipped_query_result,
    QueryRunner,
};
use crate::query::batch::result_size::ResultSize;
use crate::query::batch::BatchTxPort;
use crate::query::batch::{BatchError, BatchOp, BatchPlan, BatchRequest, QueryEntry};
use crate::query::read::QueryResult;
use shamir_query_types::batch::{
    IndexVisibility, IsolationLevel as WireIsolationLevel, ResultEncoding,
};
use shamir_tx::CommitVisibility;
use shamir_types::access::Actor;
use shamir_types::types::common::{new_map, new_map_wc, TMap};
use shamir_types::types::value::QueryValue;

/// tx-aware variant of `execute_plan_impl`.
///
/// Uses `QueryRunner` with `Some(&mut tx)` so each mutation routes
/// through `execute_*_tx`. Reads route through `TableManager::read_tx`
/// with a shared `&TxContext` (Vector I.1), so a Serializable batch's
/// SELECT populates the read-set and SSI write-skew detection is live
/// end-to-end through this wire path.
///
/// Uses the per-call deadline derived from the interactive request's limits.
/// Nested execution inherits it; no commit or transaction-lifetime bound is
/// introduced by this entry.
#[allow(clippy::too_many_arguments)]
pub(in crate::query::batch) async fn execute_plan_tx(
    plan: &mut BatchPlan,
    queries: &TMap<String, QueryEntry>,
    resolver: &dyn BatchResolver,
    admin: Option<&dyn AdminExecutor>,
    invoker: Option<&dyn FunctionInvoker>,
    actor: &Actor,
    db_name: &str,
    tx: &mut shamir_tx::TxContext,
    scope: &ExecScope,
) -> Result<TMap<String, QueryResult>, BatchError> {
    execute_plan_tx_impl(
        plan,
        queries,
        resolver,
        admin,
        invoker,
        actor,
        db_name,
        tx,
        0,
        &new_map(),
        ResultEncoding::Name,
        scope,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(in crate::query::batch) async fn execute_plan_tx_impl(
    plan: &mut BatchPlan,
    queries: &TMap<String, QueryEntry>,
    resolver: &dyn BatchResolver,
    admin: Option<&dyn AdminExecutor>,
    invoker: Option<&dyn FunctionInvoker>,
    actor: &Actor,
    db_name: &str,
    tx: &mut shamir_tx::TxContext,
    depth: usize,
    params: &TMap<String, QueryValue>,
    result_encoding: ResultEncoding,
    scope: &ExecScope,
) -> Result<TMap<String, QueryResult>, BatchError> {
    let mut all_results: TMap<String, QueryResult> = new_map_wc(queries.len());

    for stage in &plan.stages {
        for alias in stage {
            // #666 checkpoint: once per stage-alias, BEFORE dispatching the
            // op. The `Err` this raises flows out of `execute_plan_tx_impl`
            // into `execute_transactional_impl`'s existing `Err` arm — the
            // one that releases pessimistic locks and never calls
            // `commit_tx` — so timeout cleanup is byte-identical to any
            // other op failure's cleanup.
            scope.check()?;

            let entry = queries.get(alias).ok_or_else(|| BatchError::QueryError {
                alias: alias.clone(),
                message: "Query entry not found".to_string(),
                code: None,
            })?;

            let deps = plan.edge_provenance.get(alias);
            let resolved_refs = build_resolved_refs(&all_results, deps);

            // Epic03/B (#645): own `when` false, or cascaded from a
            // skipped DataFlow/Both dependency — do NOT execute the op.
            if skip_or_abort(
                entry,
                alias,
                resolve_skip(
                    entry,
                    &all_results,
                    deps,
                    &resolved_refs,
                    params,
                    resolver.scalar_resolver(),
                ),
            )? {
                let result = skipped_query_result();
                if scope.enforce_result_size() {
                    scope.charge_bytes(ResultSize::result(&result))?;
                }
                all_results.insert(alias.clone(), result);
                continue;
            }

            scope.charge_work(1)?;
            let mut runner = QueryRunner {
                resolver,
                admin,
                invoker,
                tx: Some(&mut *tx),
                actor: actor.clone(),
                db_name,
                depth,
                params,
                result_encoding,
                scope,
                pending_shape: None,
            };
            let op_started = Instant::now();
            let _guard = register_op_watchdog(alias);
            let result = runner.run(alias, entry, &resolved_refs).await?;
            warn_if_op_slow(alias, op_started.elapsed());
            if scope.enforce_result_size() {
                // Reads finish their own per-read meter in QueryRunner;
                // containers pay incrementally there as well. Their payloads
                // must not pay a second time at all_results insertion.
                if !matches!(
                    &entry.op,
                    BatchOp::Read(_) | BatchOp::Batch(_) | BatchOp::ForEach(_)
                ) {
                    scope.charge_bytes(ResultSize::result(&result))?;
                }
            }
            all_results.insert(alias.clone(), result);
        }
    }

    Ok(all_results)
}

/// Open a tx, execute the plan inside it, commit (or propagate abort).
///
/// Returns the per-query results AND the populated TransactionInfo
/// (committed or aborted with reason).
#[allow(clippy::too_many_arguments)]
pub(in crate::query::batch) async fn execute_transactional_impl(
    request: &BatchRequest,
    plan: &mut BatchPlan,
    resolver: &dyn BatchResolver,
    admin: Option<&dyn AdminExecutor>,
    invoker: Option<&dyn FunctionInvoker>,
    actor: &Actor,
    db_name: &str,
    depth: usize,
    params: &TMap<String, QueryValue>,
    result_encoding: ResultEncoding,
    scope: &ExecScope,
) -> Result<
    (
        TMap<String, QueryResult>,
        Option<shamir_query_types::batch::TransactionInfo>,
    ),
    BatchError,
> {
    // Determine target repo (cross-repo guard already enforced single).
    let repos = if depth == 0 {
        scope.root_metadata().map(|m| m.repos.clone())
    } else {
        None
    }
    .unwrap_or_else(|| shamir_query_types::batch::distinct_repos(&request.queries));
    let repo_name = repos.into_iter().next().unwrap_or_default();

    if repo_name.is_empty() {
        return Err(BatchError::QueryError {
            alias: String::new(),
            message: "transactional batch has no data ops to target a repo".into(),
            code: None,
        });
    }

    let repo = resolver
        .resolve_repo(&repo_name)
        .await
        .map_err(|e| BatchError::QueryError {
            alias: String::new(),
            message: format!("resolve_repo({}): {}", repo_name, e),
            code: None,
        })?;

    let visibility_wait = request
        .visibility_wait
        .unwrap_or_else(|| repo.default_visibility_wait());

    // Parse isolation (strict typed enum — exhaustive match, no fallback).
    let iso = match request.isolation {
        Some(WireIsolationLevel::Serializable) => shamir_tx::IsolationLevel::Serializable,
        Some(WireIsolationLevel::Pessimistic) => shamir_tx::IsolationLevel::Pessimistic,
        Some(WireIsolationLevel::Snapshot) | None => shamir_tx::IsolationLevel::Snapshot,
    };

    let (mut tx, _guard) =
        repo.begin_interactive_batch(iso)
            .await
            .map_err(|e| BatchError::QueryError {
                alias: String::new(),
                message: format!("begin_tx: {}", e),
                code: None,
            })?;
    tx.set_cancel_handle(scope.cancel_handle().cloned());
    // Thread the actor into the tx for commit-time provenance (R2).
    tx.set_actor(actor.clone());
    // Opt into async-index commit visibility when the wire client requests
    // it (v4 `index_visibility: "async"`; the old `durability: "async_index"`
    // string is gone). Absent or `"sync"` keeps CommitVisibility::Synchronous
    // so existing behaviour is byte-identical.
    if request.index_visibility == Some(IndexVisibility::Async) {
        tx.set_visibility(CommitVisibility::AsyncIndex);
    }
    let snapshot_version = tx.snapshot_version;
    let tx_id = tx.tx_id.0;

    // Execute plan in tx mode.
    let plan_result = execute_plan_tx_impl(
        plan,
        &request.queries,
        resolver,
        admin,
        invoker,
        actor,
        db_name,
        &mut tx,
        depth,
        params,
        result_encoding,
        scope,
    )
    .await;

    // #666 checkpoint: the LAST consultation of the deadline, immediately
    // BEFORE the commit decision. If the budget elapsed while the final
    // op was executing, the tx must NOT be committed — folding the check
    // into `plan_result` here routes the expired-budget case through the
    // very same `Err` arm below (pessimistic-lock release + RAII rollback,
    // `commit_tx` never called). Once `commit_tx` is entered, the deadline
    // is never consulted again: `commit_tx` always runs to completion,
    // uncancelled, and its outcome is always observed and reported
    // truthfully (its doc comment forbids racing it under
    // `tokio::select!`/`tokio::time::timeout`).
    let plan_result = plan_result.and_then(|results| {
        scope.check()?;
        scope.check_work(0)?;
        crate::query::batch::receipts::reserve(&results, &request.queries, depth, scope)?;
        Ok(results)
    });

    match plan_result {
        Err(plan_err) => {
            // Drop tx without commit = RAII rollback. Release any Level-3
            // pessimistic locks the tx held so blocked txs can proceed (the
            // locks live in the per-table MvccStore, not in TxContext, so
            // dropping TxContext alone does not free them). No-op for
            // Snapshot / Serializable (locked_keys is empty).
            repo.release_interactive_batch_locks(&tx).await;

            // #666: a deadline-exceeded abort keeps its identity all the
            // way to the caller — `ExecutionTimedOut` is a top-level batch
            // outcome (the server maps it to the `limits` error category),
            // not a per-query failure to be folded into an
            // `aborted`-TransactionInfo `Ok` response. The lock release +
            // RAII rollback above have already run at this point, so the
            // cleanup is identical either way.
            if matches!(
                plan_err,
                BatchError::ExecutionTimedOut { .. }
                    | BatchError::TooManyQueries { .. }
                    | BatchError::TooManyIterations { .. }
                    | BatchError::LimitExceeded { .. }
                    | BatchError::AsOfVisibilityTimeout { .. }
                    | BatchError::Cancelled
            ) {
                return Err(plan_err);
            }

            let mut info =
                shamir_query_types::batch::TransactionInfo::aborted(tx_id, abort_reason(&plan_err));
            // Keep the stable code of the failing operation alongside the
            // debug-text reason; `abort_reason` debug-formats the error and
            // must not remain the only record of the cause.
            if let Some(code) = plan_err.code() {
                info = info.with_abort_code(code);
            }
            Ok((new_map(), Some(info)))
        }
        Ok(results) => {
            let commit_started = Instant::now();
            let commit_result = repo
                .commit_explicit_batch(tx, visibility_wait, scope.cancel_handle().cloned())
                .await;
            QueryRunner::observe_commit(
                resolver,
                &repo_name,
                commit_started.elapsed(),
                commit_result.is_err(),
            );
            match commit_result {
                Ok(outcome) => {
                    // Thread the commit's materialization state to the
                    // client. `Complete` → materialized=true (projections
                    // applied inline); `Deferred` → materialized=false (the
                    // commit is durable via its WAL entry, but projection
                    // was deferred to crash-recovery). A `Deferred` outcome
                    // is still COMMITTED — see `MaterializationState`.
                    let info = shamir_query_types::batch::TransactionInfo::committed(
                        outcome.tx_id,
                        outcome.snapshot_version,
                        outcome.commit_version,
                        outcome.materialized(),
                    )
                    .with_visibility(outcome.visibility);
                    Ok((results, Some(info)))
                }
                Err(crate::tx::CommitError::Cancelled) => Err(BatchError::Cancelled),
                Err(commit_err) => {
                    // The commit outcome is returned as metadata — never
                    // swallowed here — because the CALLER decides the
                    // disposition:
                    //   * a top-level transactional batch returns this as
                    //     `Ok((results, Some(info)))` with `transaction`
                    //     metadata exactly as before (no behaviour change on
                    //     that path: an aborted or `indeterminate` outcome is
                    //     still a `Some(info)` on an `Ok` response);
                    //   * a `for_each` iteration (`arms_read.rs`) inspects
                    //     `info.status` and turns a non-committed outcome into
                    //     an iteration failure.
                    // `into_transaction_info` is what classifies the error
                    // (abort `reason` + its stable `abort_code`, or the
                    // `indeterminate` status for a possibly-written WAL frame);
                    // this arm must not narrow or re-interpret that.
                    let info = commit_err.into_transaction_info(tx_id, snapshot_version);
                    if info.is_committed() {
                        Ok((results, Some(info)))
                    } else {
                        Ok((new_map(), Some(info)))
                    }
                }
            }
        }
    }
}

use std::time::Instant;

use crate::query::batch::authorized::Authorized;
use crate::query::batch::batch_validate::{
    validate_abort_on_skip_scope, validate_executable_guards, validate_tables,
};
use crate::query::batch::exec_scope::ExecScope;
#[cfg(test)]
use crate::query::batch::execution_deadline::warn_if_op_slow;
use crate::query::batch::executor_traits::{AdminExecutor, BatchResolver, FunctionInvoker};
#[cfg(test)]
use crate::query::batch::op_watchdog::register_op_watchdog;
#[cfg(test)]
use crate::query::batch::query_runner::execute_single_impl;
use crate::query::batch::query_runner::QueryRunner;
#[cfg(test)]
use crate::query::batch::query_runner::{build_resolved_refs, resolve_skip, skipped_query_result};
#[cfg(test)]
use crate::query::batch::result_size::ResultSize;
use crate::query::batch::BatchTxPort;
#[cfg(test)]
use crate::query::batch::QueryEntry;
use crate::query::batch::{BatchError, BatchOp, BatchPlan, BatchRequest, BatchResponse};
use crate::query::read::QueryResult;
use shamir_collections::TFxSet;
use shamir_query_types::batch::RequestDurability;
#[cfg(test)]
use shamir_query_types::batch::ResultEncoding;
use shamir_types::access::Actor;
#[cfg(test)]
use shamir_types::types::common::new_map_wc;
use shamir_types::types::common::{new_map, TMap};
use shamir_types::types::value::QueryValue;

use super::execute_transactional_impl;

/// Execute a batch request against a table resolver.
///
/// 1. Plans the execution (topological sort into parallel stages)
/// 2. Executes each stage, passing results to dependent queries
/// 3. Filters results based on return_all / return_only
///
/// `actor` is threaded from the facade (R2) and carried into every
/// resource-touch point via [`QueryRunner`]. `db_name` provides the
/// database scope for `ResourcePath` construction.
///
/// **#666 (redesigned): wall-clock enforcement of
/// `BatchLimits.max_execution_time_secs` via COOPERATIVE deadline
/// checkpoints.** This SINGLE top-level entry point computes an
/// [`ExecScope`] once and threads it through the whole recursive
/// execution (`execute_batch_impl`, and transitively any nested
/// `Batch`/`ForEach` bodies — nested re-entrant calls inherit the outer
/// call's deadline, so the whole tree shares one budget). At each safe
/// checkpoint (before every stage-alias dispatch, before every `ForEach`
/// iteration, at nested-batch entry, and immediately BEFORE `commit_tx`)
/// an expired budget surfaces as an ordinary
/// `Err(BatchError::ExecutionTimedOut)` through the normal return path.
///
/// The original #666 mechanism (`tokio::time::timeout` wrapping the whole
/// future) was removed: it raced `commit_tx` — which is explicitly
/// non-cancel-safe at the API boundary (see its doc comment in
/// `tx/commit.rs`; a drop between WAL-begin and completion leaves the tx
/// durably committed while the client sees a timeout error) — and it
/// bypassed `execute_transactional_impl`'s `Err`-arm
/// `release_pessimistic_locks` cleanup by dropping the future mid-plan
/// (`TxContext` has no `Drop` impl for Level-3 locks), leaking pessimistic
/// locks permanently. With cooperative checkpoints nothing is ever
/// externally cancelled, so both hazards are structurally impossible. See
/// `execution_deadline.rs` for the full rationale.
///
/// `execute_in_open_tx` shares the same per-call root construction; the
/// interactive transaction lifetime remains a separate caller-owned budget.
///
/// A client-supplied `max_execution_time_secs: 0` is treated as the
/// smallest valid budget (1 second), NOT as "no timeout" — see
/// [`crate::query::batch::ExecutionDeadline::from_budget_secs`].
///
/// Takes an [`Authorized`] token, not a raw `&BatchRequest` + `Actor` pair
/// (#1199) — the token can only be minted by [`Authorized::authorize`],
/// which runs the real access-control check first. See `authorized.rs`'s
/// module doc for the full rationale.
pub async fn execute_batch(
    auth: Authorized<'_>,
    resolver: &dyn BatchResolver,
    admin: Option<&dyn AdminExecutor>,
    invoker: Option<&dyn FunctionInvoker>,
) -> Result<BatchResponse, BatchError> {
    let scope = auth.root_scope();
    let batch_sinks = QueryRunner::batch_sinks(resolver, &auth.metadata().repos);
    let (request, actor, db_name) = auth.into_parts();
    let started = Instant::now();
    let result = crate::query::batch::receipts::scope(execute_batch_impl(
        request,
        resolver,
        admin,
        invoker,
        actor,
        db_name,
        0,
        &new_map(),
        &scope,
    ))
    .await;
    QueryRunner::observe_batch(batch_sinks, started.elapsed());
    result
}

/// Test-only convenience: calls [`execute_batch`] via
/// [`Authorized::unchecked`] (no access-control check), preserving the
/// PRE-#1199 call shape (`&BatchRequest` + `Actor` + `db_name`) so the
/// large body of existing engine unit tests that exercise execution
/// mechanics — not authorization — didn't all need a structural rewrite.
/// Real (non-test) callers MUST go through `Authorized::authorize` with a
/// real [`crate::query::batch::AccessGate`] — this function does not
/// exist outside `#[cfg(test)]` builds.
#[cfg(test)]
pub(crate) async fn execute_batch_unchecked(
    request: &BatchRequest,
    resolver: &dyn BatchResolver,
    admin: Option<&dyn AdminExecutor>,
    invoker: Option<&dyn FunctionInvoker>,
    actor: Actor,
    db_name: &str,
) -> Result<BatchResponse, BatchError> {
    let auth = Authorized::unchecked(request, actor, db_name);
    execute_batch(auth, resolver, admin, invoker).await
}

/// Plan + validate a batch body's queries — `BatchPlanner::plan` plus the
/// `validate_tables`/`validate_filter_depth` preamble every
/// [`execute_batch_impl`] call used to run inline.
///
/// This is a pure function of `request.queries`/`request.limits`/
/// `request.transactional` — neither validation pass (nor the cross-repo
/// guard below) ever inspects a bound `$param` VALUE, only the query SHAPE
/// (table refs, filter ASTs, `$query`/`after` edges). That means a caller
/// that re-executes the SAME body shape many times with only the bound
/// values differing — the canonical case being `ForEach`'s per-iteration
/// recursion in `query_runner.rs` — can call this ONCE before the loop and
/// reuse the resulting [`BatchPlan`] across every iteration, instead of
/// paying a full re-plan + async table-resolution re-validation on every
/// single pass (see that module's `ForEach` handling for the loop that does
/// exactly this). The executable-value surface guard runs once over the full
/// nested tree before body planning/execution.
///
/// Split out of `execute_batch_impl` so the single-call path (`ForEach`'s
/// sibling `BatchOp::Batch`, and the top-level [`execute_batch`] entry)
/// keeps its exact prior behavior unchanged — it simply calls this once,
/// same as before.
pub(in crate::query::batch) async fn plan_and_validate_batch(
    request: &BatchRequest,
    resolver: &dyn BatchResolver,
    scope: &ExecScope,
) -> Result<BatchPlan, BatchError> {
    plan_and_validate_with_metadata(request, resolver, scope, None).await
}

async fn plan_and_validate_with_metadata(
    request: &BatchRequest,
    resolver: &dyn BatchResolver,
    scope: &ExecScope,
    metadata: Option<&shamir_query_types::batch::RequestMetadata>,
) -> Result<BatchPlan, BatchError> {
    // Validate receipt policy before planning or dispatch, including unchecked
    // test callers and recursively executed bodies.
    crate::query::batch::receipts::validate(request, resolver, metadata).await?;
    // Validate all executable filter/value/projection surfaces before planning
    // or running any operation, including nested batch/foreach bodies.
    validate_executable_guards(&request.queries)?;
    let effective = request.limits.clamped_to(scope.limits());
    for visit in shamir_query_types::batch::BatchTreeVisitor::new(&request.queries) {
        let nested_wait = match &visit.entry.op {
            BatchOp::Batch(sub) => sub.batch.visibility_wait,
            BatchOp::ForEach(fe) => fe.batch.visibility_wait,
            _ => None,
        };
        if request.transactional
            && matches!(
                nested_wait,
                Some(shamir_query_types::batch::VisibilityWait::Bounded { .. })
            )
        {
            return Err(BatchError::QueryError {
                alias: visit.alias.to_owned(),
                message: "bounded visibility is unsupported by nested batch bodies".into(),
                code: Some("validation".into()),
            });
        }
    }
    // 4.C: cross-repo guard for transactional batches.
    if request.transactional {
        let repos = metadata
            .map(|m| m.repos.clone())
            .unwrap_or_else(|| shamir_query_types::batch::distinct_repos(&request.queries));
        if repos.len() > 1 {
            let mut repos: Vec<String> = repos.into_iter().collect();
            repos.sort();
            return Err(BatchError::CrossRepoNotSupported { repos });
        }
    }

    // 1. Plan
    let plan = shamir_query_types::batch::BatchPlanner::plan(&request.queries, &effective)?;

    // 2. Validate: all referenced tables exist (skip admin ops)
    validate_tables(&request.queries, resolver, &effective, scope.limits()).await?;
    Ok(plan)
}

/// Test seam: the synced flush of requests run under this database name
/// reports an injected failure. An in-memory repo has no file WAL to fault, so
/// the post-commit flush boundary cannot be failed through the WAL seams.
#[cfg(test)]
pub(in crate::query::batch) static TEST_FAIL_SYNCED_FLUSH_DB: std::sync::OnceLock<String> =
    std::sync::OnceLock::new();

/// Durability step shared by [`execute_batch_impl`] and
/// [`execute_planned_batch_impl`]: if `request.durability == Some(RequestDurability::Synced)`,
/// flush every distinct repo the batch touched so the write survives an
/// immediate hard crash. No-op otherwise.
async fn flush_if_synced_durability(
    request: &BatchRequest,
    resolver: &dyn BatchResolver,
    db_name: &str,
    scope: &ExecScope,
    depth: usize,
) -> Result<(), BatchError> {
    if !matches!(request.durability, Some(RequestDurability::Synced)) {
        return Ok(());
    }
    #[cfg(test)]
    if TEST_FAIL_SYNCED_FLUSH_DB
        .get()
        .is_some_and(|name| name == db_name)
    {
        return Err(BatchError::QueryError {
            alias: String::new(),
            message: format!("synced flush {db_name}: injected failure"),
            code: None,
        });
    }
    let repos = if depth == 0 {
        scope.root_metadata().map(|m| m.repos.clone())
    } else {
        None
    }
    .unwrap_or_else(|| shamir_query_types::batch::distinct_repos(&request.queries));
    for repo_name in repos {
        let repo = resolver
            .resolve_repo(&repo_name)
            .await
            .map_err(|e| BatchError::QueryError {
                alias: String::new(),
                message: format!("resolve_repo({}): {}", repo_name, e),
                code: None,
            })?;
        BatchTxPort::synced_flush(&repo)
            .await
            .map_err(|e| BatchError::QueryError {
                alias: String::new(),
                message: format!("synced flush {}/{}: {}", db_name, repo_name, e),
                code: None,
            })?;
        // Belt-and-suspenders: also fsync the file WAL spine so the
        // committed entries reach level 3 (the WAL is the source of
        // truth; data-store flush above is derived). No-op for
        // in-memory repos.
        //
        // PR1: `sync_wal` demands a coalesced sync round that captures
        // coverage before fsync. Success covers that captured generation;
        // R advances only through covered predecessors. Successful sync
        // alone does not guarantee immediate replication serving across holes.
        BatchTxPort::sync_wal(&repo)
            .await
            .map_err(|e| BatchError::QueryError {
                alias: String::new(),
                message: format!("synced wal {}/{}: {}", db_name, repo_name, e),
                code: None,
            })?;
    }
    Ok(())
}

/// Internal entry point that carries the sub-batch recursion state.
///
/// - `depth` — current nesting level (0 at the public entry).
/// - `params` — injected `$param` bindings from the outer batch's
///   `BatchOp::Batch.bind` map, resolved to concrete `InnerValue`s.
/// - `scope` — the shared execution budget, computed once in
///   [`execute_batch`] and INHERITED unchanged by nested `Batch`/`ForEach`
///   bodies (a nested body's own `limits.max_execution_time_secs` is not
///   consulted — the outer call owns the budget, matching the original
///   #666 "one top-level budget covers the whole tree" semantics).
///
/// Called recursively by [`QueryRunner::run`] when it encounters a
/// `BatchOp::Batch` entry; callers above the public entry always pass
/// `depth = 0` and an empty `params` map.
///
/// Returns a boxed future because the function is mutually recursive
/// (QueryRunner::run → execute_batch_impl → execute_plan_impl →
/// execute_single_impl → QueryRunner::run) and Rust requires boxing
/// to give the async state machine a statically known size.
#[allow(clippy::too_many_arguments)]
pub(in crate::query::batch) fn execute_batch_impl<'a>(
    request: &'a BatchRequest,
    resolver: &'a dyn BatchResolver,
    admin: Option<&'a dyn AdminExecutor>,
    invoker: Option<&'a dyn FunctionInvoker>,
    actor: Actor,
    db_name: &'a str,
    depth: usize,
    params: &'a TMap<String, QueryValue>,
    scope: &'a ExecScope,
) -> std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<BatchResponse, BatchError>> + Send + 'a>,
> {
    Box::pin(async move {
        let start = Instant::now();

        // #666 checkpoint: nested-batch entry (each `BatchOp::Batch` /
        // `ForEach`-iteration recursion re-enters here on the non-tx path).
        // Free at the top-level call (the deadline was minted microseconds
        // ago); decisive for a recursion that starts after the budget
        // already elapsed.
        scope.check()?;

        let metadata = (depth == 0).then(|| scope.root_metadata()).flatten();
        if depth == 0 && !request.transactional {
            validate_abort_on_skip_scope(&request.queries)?;
        }
        let mut plan = plan_and_validate_with_metadata(request, resolver, scope, metadata).await?;

        // 3. Execute — branch on transactional.
        let (mut all_results, mut tx_info) = if request.transactional {
            execute_transactional_impl(
                request,
                &mut plan,
                resolver,
                admin,
                invoker,
                &actor,
                db_name,
                depth,
                params,
                request.result_encoding,
                scope,
            )
            .await?
        } else {
            let r = crate::query::batch::receipts::execute_plan(
                &mut plan,
                &request.queries,
                resolver,
                admin,
                invoker,
                &actor,
                db_name,
                depth,
                params,
                request.result_encoding,
                scope,
                request.visibility_wait,
            )
            .await?;
            (r, None)
        };

        // Non-transactional writes have no outcome receipt. Once a transactional
        // commit is known, retain its disposition and expose a durability error
        // in transaction metadata rather than replacing the receipt.
        if let Err(flush_error) =
            flush_if_synced_durability(request, resolver, db_name, scope, depth).await
        {
            if request.transactional {
                if let Some(info) = tx_info.as_mut() {
                    if info.error.is_none() {
                        info.error = Some(flush_error.to_string());
                    }
                } else {
                    return Err(flush_error);
                }
            } else {
                return Err(flush_error);
            }
        }

        if let Some(info) = tx_info.as_ref() {
            all_results =
                crate::query::batch::receipts::stamp(all_results, &request.queries, info, depth);
        }
        let results = crate::query::batch::receipts::filter(all_results, request);

        let elapsed = start.elapsed();

        Ok(BatchResponse {
            id: request.id.clone(),
            results,
            execution_plan: std::mem::take(&mut plan.stages),
            edge_provenance: std::mem::take(&mut plan.edge_provenance),
            execution_time_us: elapsed.as_micros() as u64,
            transaction: tx_info,
            // Ambient interner deltas are attached server-side (shamir-db's
            // execute_as post-processing); the engine layer leaves this empty.
            interner_delta: Default::default(),
        })
    }) // end Box::pin
}

/// Boxed future type for [`execute_planned_batch_impl`] — factored into its
/// own alias (mirroring `query_runner.rs`'s `NestedTxBodyFuture`) so the
/// return type doesn't trip clippy's `type_complexity` lint.
type PlannedBatchResultFuture<'a> = std::pin::Pin<
    Box<
        dyn std::future::Future<
                Output = Result<
                    (
                        TMap<String, QueryResult>,
                        Option<shamir_query_types::batch::TransactionInfo>,
                    ),
                    BatchError,
                >,
            > + Send
            + 'a,
    >,
>;

/// Execute an ALREADY-planned-and-validated batch body and return its
/// filtered per-alias results.
///
/// Companion to [`execute_batch_impl`] for a caller that computed `plan`
/// once via [`plan_and_validate_batch`] and wants to reuse it across many
/// executions of the byte-identical body shape — the only such caller
/// today is `ForEach`'s per-iteration recursion (non-tx arm) in
/// `query_runner.rs`, which now plans/validates the loop body ONCE before
/// the loop instead of once per iteration.
///
/// Returns the filtered results map plus the transaction outcome (`None` for
/// non-transactional bodies), not a full [`BatchResponse`] — the plan/timing/id
/// metadata `BatchResponse` otherwise carries is never consumed by a
/// `ForEach` iteration's caller (only `.results` is), but the outcome must be
/// returned so an ABORTED transactional body can fail its iteration instead
/// of looking like an empty success.
#[allow(clippy::too_many_arguments)]
pub(in crate::query::batch) fn execute_planned_batch_impl<'a>(
    request: &'a BatchRequest,
    plan: &'a mut BatchPlan,
    resolver: &'a dyn BatchResolver,
    admin: Option<&'a dyn AdminExecutor>,
    invoker: Option<&'a dyn FunctionInvoker>,
    actor: Actor,
    db_name: &'a str,
    depth: usize,
    params: &'a TMap<String, QueryValue>,
    scope: &'a ExecScope,
) -> PlannedBatchResultFuture<'a> {
    Box::pin(async move {
        // #666 checkpoint — mirrors `execute_batch_impl`'s pre-plan check.
        // The plan/validation itself already ran once, before the loop;
        // the deadline still gets a fresh look before this iteration's
        // real execution work.
        scope.check()?;

        // 3. Execute — branch on transactional.
        let (mut all_results, mut tx_info) = if request.transactional {
            execute_transactional_impl(
                request,
                plan,
                resolver,
                admin,
                invoker,
                &actor,
                db_name,
                depth,
                params,
                request.result_encoding,
                scope,
            )
            .await?
        } else {
            let r = crate::query::batch::receipts::execute_plan(
                plan,
                &request.queries,
                resolver,
                admin,
                invoker,
                &actor,
                db_name,
                depth,
                params,
                request.result_encoding,
                scope,
                request.visibility_wait,
            )
            .await?;
            (r, None)
        };

        // 3.5. Durability: same as `execute_batch_impl`. Once transactional
        // execution has produced a TransactionInfo, the commit disposition is
        // authoritative; a later sync failure must not replace it with Err and
        // make ForEach lose the attempted outcome. Preserve an existing outcome
        // error (notably wal_outcome_unknown / wal_durability); otherwise retain
        // the sync diagnostic in the optional error field. Non-transactional
        // batches have no transaction metadata, so their sync failure propagates.
        if let Err(flush_error) =
            flush_if_synced_durability(request, resolver, db_name, scope, depth).await
        {
            if request.transactional {
                if let Some(info) = tx_info.as_mut() {
                    if info.error.is_none() {
                        info.error = Some(flush_error.to_string());
                    }
                } else {
                    return Err(flush_error);
                }
            } else {
                return Err(flush_error);
            }
        }

        if let Some(info) = tx_info.as_ref() {
            all_results =
                crate::query::batch::receipts::stamp(all_results, &request.queries, info, depth);
        }
        let results = crate::query::batch::receipts::filter(all_results, request);
        Ok((results, tx_info))
    }) // end Box::pin
}

/// Execute a batch request with permission checks.
///
/// Same as [`execute_batch`] but runs `SessionPermissions::check_batch`
/// before planning/execution. Returns `BatchError::QueryError` if any
/// operation is denied.
///
/// **NOTE (architectural status):** This role-matrix RBAC +
/// `row_filter` RLS path is **test-only scaffolding**. The live
/// server access model is the **Shomer DAC** (owner/group/mode),
/// enforced via `ShamirDb::execute_as` -> `authorize_access` ->
/// `permits`. Groups replace roles; row-level security will be a
/// future Shomer `ResourcePath::Record`-level feature. This
/// function is retained for engine-level unit tests only.
#[cfg(test)]
pub async fn execute_batch_with_permissions(
    request: &BatchRequest,
    resolver: &dyn BatchResolver,
    admin: Option<&dyn AdminExecutor>,
    permissions: &crate::query::auth::SessionPermissions,
    db_name: &str,
) -> Result<BatchResponse, BatchError> {
    // 0. Permission check — fail fast before any work
    permissions
        .check_batch(&request.queries, db_name)
        .map_err(|(alias, action, resource)| BatchError::QueryError {
            alias,
            message: format!("Permission denied: {:?} on {:?}", action, resource),
            code: None,
        })?;

    // Stage B-1: enforce row-level security. `row_filter()` was computed but
    // never applied — AND each op's merged row_filter into its WHERE clause so a
    // row_filter grant actually restricts the rows a Read/Update/Delete touches.
    let mut request = request.clone();
    for (_alias, entry) in request.queries.iter_mut() {
        if let Some(rf) = permissions.row_filter_for_op(&entry.op, db_name) {
            apply_row_filter(&mut entry.op, rf);
        }
    }
    let auth = Authorized::unchecked(&request, Actor::System, db_name);
    execute_batch(auth, resolver, admin, None).await
}

/// AND a row-level-security filter into a data op's WHERE clause.
/// Read/Update/Delete are restricted; other ops are left unchanged
/// (Insert/Set row-match validation is a separate follow-up).
#[cfg(test)]
fn apply_row_filter(op: &mut crate::query::batch::BatchOp, rf: crate::query::filter::Filter) {
    match op {
        BatchOp::Read(q) => q.r#where = Some(and_combine(q.r#where.take(), rf)),
        BatchOp::Update(u) => u.where_clause = Some(and_combine(u.where_clause.take(), rf)),
        BatchOp::Delete(d) => {
            let existing = d.where_clause.clone();
            d.where_clause = crate::query::filter::Filter::And {
                filters: vec![existing, rf],
            };
        }
        _ => {}
    }
}

/// Combine an optional existing filter with a row filter via AND.
#[cfg(test)]
fn and_combine(
    existing: Option<crate::query::filter::Filter>,
    rf: crate::query::filter::Filter,
) -> crate::query::filter::Filter {
    match existing {
        Some(f) => crate::query::filter::Filter::And {
            filters: vec![f, rf],
        },
        None => rf,
    }
}

/// Execute a planned batch stage by stage.
///
/// For each stage, executes all queries sequentially within a stage.
/// Each query's FilterContext gets only the resolved_refs from its
/// DataFlow-provenance dependencies (`plan.edge_provenance`) — not all
/// accumulated results, and NOT `after`-only (`Explicit`) dependencies,
/// which are pure ordering and grant no data access.
///
/// **Note on parallelism.** Stages are a LOGICAL grouping of mutually
/// independent queries — the planner does not itself run anything, and
/// nothing here currently drives a stage's queries concurrently. Driving
/// them concurrently on a single task via `futures::future::try_join_all`
/// was tried and measured as a no-op on in-memory CPU-bound workloads —
/// there are no await suspension points inside the queries that would
/// yield to peers. Real parallelism needs `tokio::spawn`-per-query, which
/// in turn needs `Arc<dyn TableResolver>` / `Arc<dyn AdminExecutor>` (or a
/// scoped-spawn helper); kept out of scope for now and tracked as a
/// future opt. See
/// `docs/dev-artifacts/design/oql-01-stage-parallelism-adr.md` for the
/// decision record.
#[cfg(test)]
#[allow(clippy::too_many_arguments)]
pub(in crate::query::batch) async fn execute_plan_impl(
    plan: &mut BatchPlan,
    queries: &TMap<String, QueryEntry>,
    resolver: &dyn BatchResolver,
    admin: Option<&dyn AdminExecutor>,
    invoker: Option<&dyn FunctionInvoker>,
    actor: &Actor,
    db_name: &str,
    depth: usize,
    params: &TMap<String, QueryValue>,
    result_encoding: ResultEncoding,
    scope: &ExecScope,
) -> Result<TMap<String, QueryResult>, BatchError> {
    let mut all_results: TMap<String, QueryResult> = new_map_wc(queries.len());

    for stage in &plan.stages {
        for alias in stage {
            // #666 checkpoint: once per stage-alias, BEFORE dispatching the
            // op. An expired budget stops the batch from starting any
            // further unit of work; the error propagates through the normal
            // `?` return path like any other op failure.
            scope.check()?;

            let entry = queries.get(alias).ok_or_else(|| BatchError::QueryError {
                alias: alias.clone(),
                message: "Query entry not found".to_string(),
                code: None,
            })?;

            // Build resolved_refs with ONLY declared dependencies
            let deps = plan.edge_provenance.get(alias);
            let resolved_refs = build_resolved_refs(&all_results, deps);

            // Epic03/B (#645): own `when` false, or cascaded from a
            // skipped DataFlow/Both dependency — do NOT execute the op.
            if resolve_skip(
                entry,
                &all_results,
                deps,
                &resolved_refs,
                params,
                resolver.scalar_resolver(),
            ) {
                let result = skipped_query_result();
                if scope.enforce_result_size() {
                    scope.charge_bytes(ResultSize::result(&result))?;
                }
                all_results.insert(alias.clone(), result);
                continue;
            }

            scope.charge_work(1)?;
            let op_started = Instant::now();
            let _guard = register_op_watchdog(alias);
            let result = execute_single_impl(
                alias,
                entry,
                resolver,
                admin,
                invoker,
                &resolved_refs,
                actor,
                db_name,
                depth,
                params,
                result_encoding,
                scope,
            )
            .await?;
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

/// Filter results based on return_all / return_only / return_result flags.
pub(in crate::query::batch) fn filter_results(
    mut all_results: TMap<String, QueryResult>,
    request: &BatchRequest,
) -> TMap<String, QueryResult> {
    if let Some(ref only) = request.return_only {
        let keep: TFxSet<String> = only.iter().cloned().collect();
        all_results.retain(|alias, result| {
            crate::query::batch::receipts::has_receipt(result) || keep.contains(alias)
        });
        return all_results;
    }

    if !request.return_all {
        all_results.retain(|alias, result| {
            crate::query::batch::receipts::has_receipt(result)
                || request.queries.get(alias).is_some_and(|e| e.return_result)
        });
    }

    all_results
}

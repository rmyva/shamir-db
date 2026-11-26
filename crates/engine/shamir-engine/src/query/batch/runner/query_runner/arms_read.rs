use shamir_types::access::{trace_access, Action};

use super::runner::{DataArm, QueryRunner};

use shamir_types::types::common::{new_map, TMap};
use shamir_types::types::value::{InnerValue, QueryValue};

use super::helpers::{
    effective_max_iterations, is_typed_refusal, transactional_body_outcome_error,
};
use super::nested::run_nested_body_in_outer_tx;
use crate::query::batch::batch_execute::{
    execute_batch_impl, execute_planned_batch_impl, plan_and_validate_batch,
};
use crate::query::batch::result_size::ResultSize;
use crate::query::batch::{BatchError, BatchOp, QueryEntry};
use crate::query::filter::read_budget::ReadByteMeter;
use crate::query::filter::FilterContext;
use crate::query::read::QueryResult;

impl<'a> QueryRunner<'a> {
    pub(super) async fn run_sub_batch_arm(
        &mut self,
        alias: &str,
        entry: &QueryEntry,
        resolved_refs: &TMap<String, QueryResult>,
        _wait: Option<shamir_query_types::batch::VisibilityWait>,
    ) -> Result<QueryResult, BatchError> {
        let BatchOp::Batch(sub) = &entry.op else {
            unreachable!()
        };
        // Guard: transactional sub-batch inside an already-open tx
        // is not supported (two-phase commit across a shared TxContext
        // is not safe; per design the outer should be non-transactional
        // when it contains transactional sub-batches).
        if sub.batch.transactional && self.tx.is_some() {
            return Err(BatchError::query_coded(
                alias,
                "nested_tx_not_supported",
                "a transactional sub-batch cannot run inside an outer transaction",
            ));
        }

        // Resolve the `bind` map against the CURRENT scope's resolved_refs
        // and params. Each value is a FilterValue — resolve it to a
        // QueryValue using the same machinery as filter evaluation.
        // We use a dummy record (Null) because bind values may only
        // reference $query aliases or literals, not record fields.
        let dummy_record = InnerValue::Null;
        // We need an Interner for FilterContext, but bind values must only
        // be literals or $query refs (not FieldRefs). Use a scratch interner.
        let scratch = shamir_types::core::interner::Interner::new();
        let bind_ctx = FilterContext::new(&scratch, resolved_refs)
            .with_scalars(self.resolver.scalar_resolver())
            .with_params(self.params);
        let mut resolved_params: TMap<String, QueryValue> = new_map();
        for (key, fv) in &sub.bind {
            match fv {
                crate::query::filter::FilterValue::Param { name } => {
                    // $param in a bind value means look up from the
                    // current (outer) scope's params — propagation.
                    let v = self.params.get(name.as_str()).ok_or_else(|| {
                        BatchError::query_coded(
                            alias,
                            "unbound_param",
                            format!("$param '{}' is not bound in the current scope", name),
                        )
                    })?;
                    resolved_params.insert(key.clone(), v.clone());
                }
                other => {
                    let v = crate::query::filter::eval::resolve_filter_query(
                        other,
                        &dummy_record,
                        &bind_ctx,
                    )
                    .ok_or_else(|| BatchError::QueryError {
                        alias: alias.to_string(),
                        message: format!(
                            "bind key '{}': cannot resolve filter value {:?}",
                            key, fv
                        ),
                        code: None,
                    })?;
                    resolved_params.insert(key.clone(), v);
                }
            }
        }

        // Recurse into the sub-batch. When we're already inside an
        // open outer transaction (`self.tx.is_some()`), thread that
        // SAME `TxContext` through so the sub-batch's writes are
        // genuine participants in the outer tx — an error anywhere in
        // the sub-batch then aborts the WHOLE outer transaction via the
        // outer `execute_transactional_impl`'s existing RAII rollback
        // (#661). The `nested_tx_not_supported` guard above already
        // ensures `sub.batch.transactional == false` is the only
        // reachable case here. When there is no open outer tx
        // (`self.tx.is_none()`), keep the original `execute_batch_impl`
        // path byte-identical.
        // #666: a nested body's `ExecutionTimedOut` keeps its identity
        // (not wrapped into an alias-scoped `QueryError`) — the deadline
        // is ONE budget shared by the whole execution tree, so a
        // deadline exceeded inside a nested body IS the outer batch's
        // timeout. Preserving the variant lets
        // `execute_transactional_impl`'s `Err` arm re-raise it as the
        // top-level outcome, identical to a timeout detected at any
        // outer checkpoint.
        let wrap_sub_batch_err = |e: BatchError| match e {
            e if is_typed_refusal(&e) => e,
            limit_error @ (BatchError::ExecutionTimedOut { .. }
            | BatchError::TooManyQueries { .. }
            | BatchError::TooManyIterations { .. }
            | BatchError::LimitExceeded { .. }
            | BatchError::AsOfVisibilityTimeout { .. }
            | BatchError::Cancelled) => limit_error,
            e => BatchError::QueryError {
                alias: alias.to_string(),
                message: format!("sub-batch '{}' failed: {}", alias, e),
                code: e.code().map(str::to_owned),
            },
        };
        let inner_results = match self.tx.as_deref_mut() {
            Some(tx) => {
                let mut plan = plan_and_validate_batch(&sub.batch, self.resolver, self.scope)
                    .await
                    .map_err(wrap_sub_batch_err)?;
                run_nested_body_in_outer_tx(
                    &sub.batch,
                    &mut plan,
                    self.resolver,
                    self.admin,
                    self.invoker,
                    &self.actor,
                    self.db_name,
                    tx,
                    self.depth + 1,
                    &resolved_params,
                    self.result_encoding,
                    self.scope,
                )
                .await
                .map_err(wrap_sub_batch_err)?
            }
            None => {
                let response = execute_batch_impl(
                    &sub.batch,
                    self.resolver,
                    self.admin,
                    self.invoker,
                    self.actor.clone(),
                    self.db_name,
                    self.depth + 1,
                    &resolved_params,
                    self.scope,
                )
                .await
                .map_err(|e| match e {
                    e if is_typed_refusal(&e) => e,
                    limit_error @ (BatchError::ExecutionTimedOut { .. }
                    | BatchError::TooManyQueries { .. }
                    | BatchError::TooManyIterations { .. }
                    | BatchError::LimitExceeded { .. }
                    | BatchError::AsOfVisibilityTimeout { .. }
                    | BatchError::Cancelled) => limit_error,
                    e => BatchError::QueryError {
                        alias: alias.to_string(),
                        message: format!("sub-batch '{}' failed: {}", alias, e),
                        code: e.code().map(str::to_owned),
                    },
                })?;
                // An aborted/indeterminate transactional sub-batch must never
                // look like an empty success. This caller has no committed-prefix
                // knowledge, so an abort is always transaction_aborted with its
                // cause. A committed receipt (also one carrying a sync warning)
                // stays in the nested results, as before.
                if let Some(info) = response
                    .transaction
                    .as_ref()
                    .filter(|info| info.status != "committed")
                {
                    if let Some(e) = transactional_body_outcome_error(alias, info, None) {
                        return Err(e);
                    }
                }
                response.results
            }
        };

        // Wrap the inner results into a QueryResult for the outer
        // $query path resolution.
        //
        // `resolve_query_ref_value` in eval.rs checks `qr.value` first
        // (Call-result path). We store the inner results map as a QueryValue
        // Map in `value` so outer ops can access sub-aliases:
        //   $query @sub[0].records[0].id  — NOT supported (records empty)
        //   $query @sub.alias_name[0].id  — walks value.alias_name[0].id
        //
        // Direct conversion (F5): drive the existing `Serialize` impls of
        // `TMap<String, QueryResult>` / `QueryRecord` / `InsertedRecord`
        // into a `QueryValue` tree via `QueryValueSerializer` — same wire
        // shape the old msgpack round-trip produced, without the encode +
        // re-parse + re-allocate cost. See `query_value_serializer`.
        if self.scope.enforce_result_size() {
            self.scope.charge_bytes(
                ResultSize::result(&QueryResult::default())
                    .saturating_add(ResultSize::assembled(&inner_results)),
            )?;
        }
        let value =
            crate::query::batch::query_value_serializer::to_query_value(&inner_results).ok();
        Ok(QueryResult {
            records: Vec::new(),
            stats: None,
            pagination: None,
            value,
            explain: None,
            skipped: false,
            versions: None,
            corrupt_records: Vec::new(),
            op_id: None,
            ..QueryResult::default()
        })
    }

    pub(super) async fn run_for_each_arm(
        &mut self,
        alias: &str,
        entry: &QueryEntry,
        resolved_refs: &TMap<String, QueryResult>,
        _wait: Option<shamir_query_types::batch::VisibilityWait>,
    ) -> Result<QueryResult, BatchError> {
        let BatchOp::ForEach(_fe) = &entry.op else {
            unreachable!()
        };
        // reusing the exact recursive seam BatchOp::Batch uses above,
        // invoked in a loop instead of once. See
        // `docs/dev-artifacts/design/oql-04-loops-foreach-adr.md`.
        if let BatchOp::ForEach(fe) = &entry.op {
            // Guard: transactional loop body inside an already-open tx is
            // not supported — identical rationale to the sub-batch guard
            // above (two-phase commit across a shared TxContext is unsafe).
            if fe.batch.transactional && self.tx.is_some() {
                return Err(BatchError::query_coded(
                    alias,
                    "nested_tx_not_supported",
                    "a transactional for_each body cannot run inside an outer transaction",
                ));
            }

            // Resolve `over` against the CURRENT scope's resolved_refs and
            // params, exactly like `SubBatchOp.bind`'s resolution above:
            // a scratch Interner is safe here because `over` (like `bind`
            // values) may only reference $query aliases, $param, or
            // literals — never a record FieldRef (see ADR's bug #651
            // independence analysis).
            let dummy_record = InnerValue::Null;
            let scratch = shamir_types::core::interner::Interner::new();
            let over_ctx = FilterContext::new(&scratch, resolved_refs)
                .with_scalars(self.resolver.scalar_resolver())
                .with_params(self.params);

            // Column-ref form (`@alias[].field`) needs the "whole column"
            // resolver (`resolve_query_ref_column`), not the scalar
            // `resolve_filter_query` — a bare `resolve_filter_query` call on
            // a `[]`-prefixed QueryRef path does not walk every record, only
            // `resolve_query_ref_column` does (see Decision 1's discussion
            // of `@alias[].field`). Every other `over` shape (literal
            // array, `$param`, nested `$cond`/`$expr`) resolves through the
            // normal scalar path and is expected to yield a
            // `QueryValue::List`.
            let elements: Vec<QueryValue> = if let crate::query::filter::FilterValue::QueryRef {
                alias: over_alias,
                path,
            } = &fe.over
            {
                if crate::query::filter::is_column_query_ref(&fe.over) {
                    let key = over_alias.strip_prefix('@').unwrap_or(over_alias.as_str());
                    match resolved_refs.get(key) {
                        Some(qr) => {
                            crate::query::filter::resolve_query_ref_column(qr, path.as_deref())
                        }
                        None => Vec::new(),
                    }
                } else {
                    match crate::query::filter::eval::resolve_filter_query(
                        &fe.over,
                        &dummy_record,
                        &over_ctx,
                    ) {
                        Some(QueryValue::List(items)) => items,
                        Some(_) | None => {
                            return Err(BatchError::QueryError {
                                alias: alias.to_string(),
                                message: format!(
                                    "for_each '{}': 'over' did not resolve to a list",
                                    alias
                                ),
                                code: None,
                            });
                        }
                    }
                }
            } else {
                match crate::query::filter::eval::resolve_filter_query(
                    &fe.over,
                    &dummy_record,
                    &over_ctx,
                ) {
                    Some(QueryValue::List(items)) => items,
                    Some(_) | None => {
                        return Err(BatchError::QueryError {
                            alias: alias.to_string(),
                            message: format!(
                                "for_each '{}': 'over' did not resolve to a list",
                                alias
                            ),
                            code: None,
                        });
                    }
                }
            };

            // Runtime gate (ADR Decision 3): reject BEFORE iteration 0 if
            // the resolved length exceeds max_iterations — never a partial
            // run followed by a mid-loop abort.
            let body_limits = fe.batch.limits.clamped_to(self.scope.limits());
            let max_iterations = effective_max_iterations(&body_limits);
            if elements.len() > max_iterations {
                self.scope.check_cancelled()?;
                return Err(BatchError::TooManyIterations {
                    alias: alias.to_string(),
                    actual: elements.len(),
                    max: max_iterations,
                });
            }

            // Check the static body tree before iteration zero; do not charge
            // it here. Dynamic descendants are charged as they dispatch.
            // Keep visitor/counter temporaries out of the per-op await state.
            {
                let overflow = || BatchError::LimitExceeded {
                    kind: shamir_query_types::batch::LimitKind::Work,
                    actual: u64::MAX,
                    max: self.scope.limits().max_total_work,
                };
                let body_work = shamir_query_types::batch::BatchTreeVisitor::new(&fe.batch.queries)
                    .try_fold(0u64, |count, _| count.checked_add(1))
                    .ok_or_else(overflow)?;
                let extra = u64::try_from(elements.len())
                    .ok()
                    .and_then(|len| len.checked_mul(body_work))
                    .ok_or_else(overflow)?;
                self.scope.check_work(extra)?;
            }

            // Plan + validate the loop body ONCE, before iterating (P2 perf
            // fix — see `TASK_GROUPS.md` group 22). The body's SHAPE
            // (queries, table refs, filter ASTs) is byte-identical across
            // every iteration; only the bound `$param` value differs per
            // pass (injected below via `resolved_params`). Re-running
            // `BatchPlanner::plan` + `validate_tables` (async table
            // resolution) + `validate_filter_depth` on EVERY iteration was
            // pure redundant work whenever the body validates successfully
            // — an N-iteration loop used to pay N times the plan/validate
            // cost of ONE. Neither pass ever inspects a bound value, only
            // the query shape — see `plan_and_validate_batch`'s doc comment
            // for why hoisting it out of the loop is sound. `plan` is then
            // reused (via a fresh `&mut` reborrow) by every iteration below.
            self.scope.check()?;
            // KNOWN LIMIT (task #2007 5f): a typed empty-alias `server_busy` passes
            // through nested wrappers unchanged; the fixed error envelope cannot tell
            // "nothing published" from "partly published", so a mixed batch must not
            // be retried on this code.
            let wrap_for_each_body_err = |e: BatchError| match e {
                e if is_typed_refusal(&e) => e,
                limit_error @ (BatchError::ExecutionTimedOut { .. }
                | BatchError::TooManyQueries { .. }
                | BatchError::TooManyIterations { .. }
                | BatchError::LimitExceeded { .. }
                | BatchError::AsOfVisibilityTimeout { .. }
                | BatchError::Cancelled) => limit_error,
                e => BatchError::QueryError {
                    alias: alias.to_string(),
                    message: format!(
                        "for_each '{}': loop body failed to plan/validate: {}",
                        alias, e
                    ),
                    code: e.code().map(str::to_owned),
                },
            };
            let mut plan = plan_and_validate_batch(&fe.batch, self.resolver, self.scope)
                .await
                .map_err(wrap_for_each_body_err)?;

            // KNOWN LIMIT (task #2007 5g): with the engine result meter ON, a cap
            // below the ForEach container shell (`ResultSize::SHELL` + empty result)
            // rejects the container HERE, before iteration 0, as
            // `LimitExceeded(ResultBytes)` — not later as a whole-response
            // `result_too_large`.
            if self.scope.enforce_result_size() {
                self.scope.charge_bytes(
                    ResultSize::result(&QueryResult::default()).saturating_add(ResultSize::SHELL),
                )?;
            }
            let mut iterations: Vec<QueryValue> = Vec::with_capacity(elements.len());
            for element in elements {
                // #666 checkpoint: once per iteration, BEFORE recursing into
                // the loop body — the canonical "many cheap iterations
                // accumulating wall-clock time" DoS shape. An expired budget
                // propagates through the normal `?` return path below
                // (unwrapped — see the map_err pass-through), reaching the
                // outer tx's `Err`-arm cleanup like any other iteration
                // failure.
                self.scope.check()?;

                let mut resolved_params: TMap<String, QueryValue> = new_map();
                resolved_params.insert(fe.bind_row.clone(), element);

                // Recurse into the loop body — same seam as BatchOp::Batch,
                // invoked in a loop instead of once (ADR Decision 1).
                // Error semantics (ADR Decision 4): the `?` below propagates
                // the FIRST error immediately — both the tx case (abort the
                // whole containing batch) and the non-tx case (stop-at-
                // first, no partial results collected) fall out of this
                // same short-circuit, exactly like the existing sub-batch
                // recursion and the executor's stage loop.
                //
                // When we're already inside an open outer transaction
                // (`self.tx.is_some()`), thread that SAME `TxContext`
                // through every iteration — a re-borrow via
                // `as_deref_mut()` fresh on each loop pass, since a `&mut`
                // reborrow cannot be reused by value across iterations —
                // so all iterations' writes are genuine participants in
                // the outer tx and a failure on ANY iteration aborts the
                // WHOLE outer transaction via the outer
                // `execute_transactional_impl`'s existing RAII rollback
                // (#661), rather than each already-succeeded iteration
                // having committed independently and survived the abort.
                // The `nested_tx_not_supported` guard above already ensures
                // `fe.batch.transactional == false` is the only reachable
                // case here. When there is no open outer tx
                // (`self.tx.is_none()`), keep the original
                // `execute_batch_impl` path byte-identical.
                // #666: same `ExecutionTimedOut` identity-preserving
                // pass-through as the sub-batch recursion above — the
                // shared budget's timeout is never demoted to a per-alias
                // `QueryError`.
                let inner_results = match self.tx.as_deref_mut() {
                    Some(tx) => run_nested_body_in_outer_tx(
                        &fe.batch,
                        &mut plan,
                        self.resolver,
                        self.admin,
                        self.invoker,
                        &self.actor,
                        self.db_name,
                        tx,
                        self.depth + 1,
                        &resolved_params,
                        self.result_encoding,
                        self.scope,
                    )
                    .await
                    .map_err(|e| match e {
                        e if is_typed_refusal(&e) => e,
                        limit_error @ (BatchError::ExecutionTimedOut { .. }
                        | BatchError::TooManyQueries { .. }
                        | BatchError::TooManyIterations { .. }
                        | BatchError::LimitExceeded { .. }
                        | BatchError::AsOfVisibilityTimeout { .. }
                        | BatchError::Cancelled) => limit_error,
                        e => BatchError::QueryError {
                            alias: alias.to_string(),
                            message: format!("for_each '{}' iteration failed: {}", alias, e),
                            code: e.code().map(str::to_owned),
                        },
                    })?,
                    // A transactional body aborts by returning `Ok` with an
                    // `aborted` TransactionInfo and an empty result map. That
                    // abort committed nothing, so returning/appending the
                    // empty value would be a lie about what the body computed;
                    // fail the iteration with the abort's own stable code.
                    None => {
                        let (results, tx_info) = execute_planned_batch_impl(
                            &fe.batch,
                            &mut plan,
                            self.resolver,
                            self.admin,
                            self.invoker,
                            self.actor.clone(),
                            self.db_name,
                            self.depth + 1,
                            &resolved_params,
                            self.scope,
                        )
                        .await
                        .map_err(|e| match e {
                            e if is_typed_refusal(&e) => e,
                            limit_error @ (BatchError::ExecutionTimedOut { .. }
                            | BatchError::TooManyQueries { .. }
                            | BatchError::TooManyIterations { .. }
                            | BatchError::LimitExceeded { .. }
                            | BatchError::AsOfVisibilityTimeout { .. }
                            | BatchError::Cancelled) => limit_error,
                            e => BatchError::QueryError {
                                alias: alias.to_string(),
                                message: format!("for_each '{}' iteration failed: {}", alias, e),
                                code: e.code().map(str::to_owned),
                            },
                        })?;
                        if let Some(info) = tx_info {
                            if let Some(e) = transactional_body_outcome_error(
                                alias,
                                &info,
                                Some(iterations.len()),
                            ) {
                                return Err(e);
                            }
                        }
                        results
                    }
                };

                // Same per-iteration value shape a single sub-batch already
                // produces (direct conversion via `QueryValueSerializer`,
                // replacing the old msgpack round-trip — F5) — collected into
                // a List, one entry per iteration (ADR Decision 2).
                if self.scope.enforce_result_size() {
                    self.scope.charge_bytes(
                        ResultSize::ENTRY.saturating_add(ResultSize::assembled(&inner_results)),
                    )?;
                }
                let value =
                    crate::query::batch::query_value_serializer::to_query_value(&inner_results)
                        .ok()
                        .unwrap_or(QueryValue::Null);
                iterations.push(value);
            }

            return Ok(QueryResult::with_value(QueryValue::List(iterations)));
        }
        unreachable!("op was matched as ForEach above");
    }

    pub(super) async fn run_read_arm(
        &mut self,
        arm: DataArm<'_>,
    ) -> Result<QueryResult, BatchError> {
        let DataArm {
            alias,
            entry,
            table_ref,
            resource,
            table,
            ctx,
            ..
        } = arm;
        let BatchOp::Read(query) = &entry.op else {
            unreachable!()
        };
        // Observability trace only — see `trace_access`'s doc
        // comment. Real enforcement already ran in execute_as /
        // tx_execute_as before this runner was reached.
        trace_access(&self.actor, &resource, Action::Read).map_err(|e| BatchError::QueryError {
            alias: alias.to_string(),
            message: e.to_string(),
            code: None,
        })?;
        // D2 P1d-2b: TEMPORAL reads (AsOf / History) scan the durable
        // `history` version-log, which the cutover made the background
        // drainer fill (the ack-path now writes only the in-memory
        // overlay). The standard `Latest` read is overlay-aware (P1b
        // seams) and needs no drain, but a temporal read issued right
        // after a commit could race the async drainer and miss the
        // freshly-committed tail. Drain the repo first so temporal
        // reads are coherent regardless of drainer timing. `drain_all`
        // is idempotent / cheap when caught up; skipped for `Latest`.
        if !matches!(query.temporal, shamir_query_types::read::Temporal::Latest) {
            if let Ok(repo) = self.resolver.resolve_repo(&table_ref.repo).await {
                if let Err(e) = repo
                    .drainer()
                    .drain_all_with_cancel(
                        &repo,
                        self.scope.cancel_handle().map(std::sync::Arc::as_ref),
                    )
                    .await
                {
                    if crate::query::batch::exec_scope::is_request_cancelled(&e) {
                        return self.scope.check().and(Err(BatchError::Cancelled));
                    }
                    log::warn!("temporal read: drain_all {}: {e}", table_ref.repo);
                }
            }
        }
        // Vector I.1: in a transactional batch route the read through
        // `read_tx` with a SHARED `&TxContext` so the SELECT records
        // into the read-set (Serializable → SSI write-skew detection
        // goes live end-to-end). `as_deref()` reborrows the runner's
        // `&mut TxContext` as `&TxContext`; the read branch never also
        // holds the `&mut`, and queries within a stage run sequentially
        // (no read/write aliasing over the same tx). Non-tx batches
        // keep the original zero-overhead `read` path.
        //
        // S-read: thread result_encoding into the read path so that
        // Id-encoding requests return id-keyed IdBytes rows instead of
        // de-interning on the server.
        let metrics = self.resolver.query_metrics(&table_ref.repo);
        let meter =
            crate::query::batch::exec_scope::ScopeRowMeter::new(self.scope, metrics.as_deref());
        let ctx = ctx
            .with_row_byte_meter(meter.as_ref().map(|m| m as &dyn ReadByteMeter))
            .with_read_visibility_scope(self.scope);
        let result = match self.tx.as_deref() {
            Some(tx) => {
                table
                    .read_tx_with_encoding(query, &ctx, Some(tx), self.result_encoding)
                    .await
            }
            None => {
                table
                    .read_with_encoding(query, &ctx, self.result_encoding)
                    .await
            }
        }
        .map_err(|e| {
            if crate::query::batch::exec_scope::is_request_cancelled(&e) {
                return self.scope.check().err().unwrap_or(BatchError::Cancelled);
            }
            if let shamir_storage::error::DbError::Io(io) = &e {
                if let Some(error) = io.get_ref().and_then(|e| e.downcast_ref::<BatchError>()) {
                    return error.clone();
                }
            }
            if let shamir_storage::error::DbError::AsOfVisibilityTimeout {
                requested,
                visible_through,
                blocking_version,
            } = &e
            {
                // A call deadline still takes precedence for handle cleanup.
                if let Err(deadline) = self.scope.check() {
                    return deadline;
                }
                return BatchError::AsOfVisibilityTimeout {
                    alias: alias.into(),
                    requested: *requested,
                    visible_through: *visible_through,
                    blocking_version: *blocking_version,
                };
            }
            BatchError::QueryError {
                alias: alias.to_string(),
                message: e.to_string(),
                code: e.code().map(str::to_string),
            }
        })?;
        if let Some(meter) = &meter {
            meter.finish(&result)?;
            // Scan paths account actual stream batches. Candidate/index paths that
            // do not use those streams report their existing scanned-row statistic.
            if !meter.has_viewed_rows() {
                let index = result
                    .stats
                    .as_ref()
                    .and_then(|stats| stats.index_used.as_deref());
                let count_all = matches!(
                    query.select.items.as_slice(),
                    [shamir_engine_query::query::read::SelectItem::CountAll { .. }]
                );
                let viewed = match index {
                    // Counter/index count shortcuts inspect count/posting metadata,
                    // not table rows. MIN loads and examines exactly one source row.
                    Some("__record_counter__") => 0,
                    Some(name) if count_all && name.starts_with("idx_") => 0,
                    Some(name) if name.ends_with("_min") => 1,
                    _ => result
                        .stats
                        .as_ref()
                        .map_or(0, |stats| stats.records_scanned),
                };
                meter.viewed(viewed);
            }
            meter.finish_rows(result.records.len() as u64);
        }
        Ok(result)
    }
}

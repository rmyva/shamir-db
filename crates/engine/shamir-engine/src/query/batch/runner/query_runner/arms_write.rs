use shamir_types::access::{trace_access, Action};

use super::nested::write_value_error_to_batch_error;
use super::runner::{DataArm, QueryRunner};

use super::fk_isolation::{
    implicit_tx_isolation_for_fk_parent, require_footprint_if_fk_child, retry_on_tx_conflict,
    FkParentOpKind,
};
use crate::query::batch::param_subst::{contains_param_ref, resolve_write_value};
use crate::query::batch::receipts::write_result_to_query_result_with_encoding;
use crate::query::batch::BatchOp;
use crate::query::batch::{BatchError, BatchTxPort};
use crate::query::read::QueryResult;
impl<'a> QueryRunner<'a> {
    pub(super) async fn run_insert_arm(
        &mut self,
        arm: DataArm<'_>,
    ) -> Result<QueryResult, BatchError> {
        let DataArm {
            alias,
            entry,
            table_ref,
            resource,
            table,
            interner,
            ctx,
            wait,
        } = arm;
        let BatchOp::Insert(op) = &entry.op else {
            unreachable!()
        };
        // Observability trace only — see `trace_access`'s doc
        // comment. Real enforcement already ran in execute_as /
        // tx_execute_as before this runner was reached.
        trace_access(&self.actor, &resource, Action::Write).map_err(|e| {
            BatchError::QueryError {
                alias: alias.to_string(),
                message: e.to_string(),
                code: None,
            }
        })?;
        // #641: resolve reserved markers ($param/$query/$fn/$cond/
        // $expr) inside each inserted value row — but ONLY when a row
        // actually contains a marker node.
        //
        // The common bulk-insert path carries no markers: a cheap scan
        // (`contains_param_ref`, no allocation) lets us borrow `op`
        // directly — skipping the per-row clone-Vec AND the O(N) deep
        // structural `values == op.values` compare that the old code
        // paid on every insert to discover "nothing changed". Only when
        // some row references a marker do we build the resolved op
        // (and pay the clone), where resolution is genuinely needed.
        let subst_op;
        let needs_subst = op.values.iter().any(contains_param_ref);
        let op_ref: &shamir_query_types::write::InsertOp = if !needs_subst {
            op
        } else {
            let values: Vec<_> = op
                .values
                .iter()
                .map(|v| {
                    resolve_write_value(v, &ctx)
                        .map_err(|e| write_value_error_to_batch_error(alias, e))
                })
                .collect::<Result<_, _>>()?;
            subst_op = shamir_query_types::write::InsertOp {
                insert_into: op.insert_into.clone(),
                values,
                records_idmsgpack: Vec::new(),
                select: op.select.clone(),
            };
            &subst_op
        };
        let wr = match self.tx.as_deref_mut() {
            Some(tx) => {
                // F-28 Step 5 (S3-C): child-side hook — if this table
                // is an FK child, this tx's footprint must be
                // published regardless of isolation, so a concurrent
                // Serializable FK-parent-delete's phantom check has
                // something to conflict against.
                require_footprint_if_fk_child(self.resolver, table_ref, table.table_token(), tx)
                    .await;
                table
                    .execute_insert_tx(
                        op_ref,
                        tx,
                        entry.return_result,
                        Some(self.resolver),
                        &self.actor,
                    )
                    .await
                    .map_err(|e| {
                        if crate::query::batch::exec_scope::is_request_cancelled(&e) {
                            return BatchError::Cancelled;
                        }
                        BatchError::QueryError {
                            alias: alias.to_string(),
                            message: e.to_string(),
                            code: None,
                        }
                    })?
            }
            // Non-tx INSERT publishes one implicit commit and retains its receipt.
            None => {
                let repo = self
                    .resolver
                    .resolve_repo(&table_ref.repo)
                    .await
                    .map_err(|e| BatchError::QueryError {
                        alias: alias.to_string(),
                        message: format!("resolve_repo({}): {}", table_ref.repo, e),
                        code: None,
                    })?;
                let return_result = entry.return_result;
                // F-28 Step 1: straight-line begin/execute/commit —
                // no HRTB closure, so `self.resolver` can be borrowed
                // normally (this also fixes D2: the implicit path was
                // wrongly passing `None` instead of the resolver,
                // causing every FK-constrained autocommit insert to
                // be rejected regardless of whether the referenced
                // row existed).
                let (mut tx, _guard) =
                    crate::query::batch::begin_implicit_batch_tx(&repo, self.actor.clone(), alias)
                        .await?;
                // F-28 Step 5 (S3-C): child-side hook, implicit path —
                // see the identical comment on the explicit-tx arm
                // above.
                require_footprint_if_fk_child(
                    self.resolver,
                    table_ref,
                    table.table_token(),
                    &mut tx,
                )
                .await;
                let wr = table
                    .execute_insert_tx(
                        op_ref,
                        &mut tx,
                        return_result,
                        Some(self.resolver),
                        &self.actor,
                    )
                    .await
                    .map_err(|e| {
                        if crate::query::batch::exec_scope::is_request_cancelled(&e) {
                            return BatchError::Cancelled;
                        }
                        BatchError::QueryError {
                            alias: alias.to_string(),
                            message: e.to_string(),
                            code: None,
                        }
                    })?;
                let receipt = crate::query::batch::receipts::commit_implicit_with_cancel(
                    &repo,
                    tx,
                    alias,
                    wait.unwrap_or_else(|| repo.default_visibility_wait()),
                    self.scope.cancel_handle().cloned(),
                )
                .await?;
                return Ok(crate::query::batch::receipts::attach(
                    wr,
                    self.result_encoding,
                    interner,
                    receipt,
                ));
            }
        };
        Ok(write_result_to_query_result_with_encoding(
            wr,
            self.result_encoding,
            interner,
        ))
    }

    pub(super) async fn run_update_arm(
        &mut self,
        arm: DataArm<'_>,
    ) -> Result<QueryResult, BatchError> {
        let DataArm {
            alias,
            entry,
            table_ref,
            resource,
            table,
            interner,
            ctx,
            wait,
        } = arm;
        let BatchOp::Update(op) = &entry.op else {
            unreachable!()
        };
        // Observability trace only — see `trace_access`'s doc
        // comment. Real enforcement already ran in execute_as /
        // tx_execute_as before this runner was reached.
        trace_access(&self.actor, &resource, Action::Write).map_err(|e| {
            BatchError::QueryError {
                alias: alias.to_string(),
                message: e.to_string(),
                code: None,
            }
        })?;
        // #641: resolve reserved markers ($param/$query/$fn/$cond/
        // $expr) inside the `set` document. `resolve_write_value` has
        // a fast path (clone) when no marker nodes exist.
        let subst_set = resolve_write_value(&op.set, &ctx)
            .map_err(|e| write_value_error_to_batch_error(alias, e))?;
        let subst_op;
        let op_ref: &shamir_query_types::write::UpdateOp = if subst_set == op.set {
            op
        } else {
            subst_op = shamir_query_types::write::UpdateOp {
                update: op.update.clone(),
                where_clause: op.where_clause.clone(),
                set: subst_set,
                select: op.select.clone(),
                expected_version: op.expected_version,
            };
            &subst_op
        };

        // Phase ②.2b — ON UPDATE referential-action enforcement.
        //
        // F-28 Step 2: the fan-out plan is now built AFTER a `tx` is
        // available (explicit tx, or the implicit tx begun below),
        // and its row-level probes read via `list_stream_tx(Some(tx),
        // ..)` — read-your-own-writes against this transaction's own
        // staged writes. The plan still carries pre-resolved child
        // `TableManager` handles + the old→new value pairs; an empty
        // plan (the common case — update does not touch a referenced
        // parent field) is a fast no-op.
        //
        // TOCTOU caveat: see fk_on_update.rs docs — the in-tx RYOW gap
        // (D1) is closed. F-28 Step 5 closes the cross-transaction
        // race for the IMPLICIT path (see below); see this file's
        // module docs for the residual scope.
        let wr = match self.tx.as_deref_mut() {
            Some(tx) => {
                // F-28 Step 5 (S3-C): child-side hook — if this table
                // is itself an FK child of some OTHER table, this
                // tx's footprint must be published regardless of its
                // own isolation, so a concurrent Serializable
                // FK-parent-delete/update elsewhere has something to
                // conflict against.
                require_footprint_if_fk_child(self.resolver, table_ref, table.table_token(), tx)
                    .await;
                let fk_update_plan = crate::query::batch::fk_on_update::plan_fk_on_update(
                    self.resolver,
                    table_ref,
                    table,
                    op_ref,
                    &ctx,
                    alias,
                    tx,
                )
                .await?;
                // Apply child fan-out (cascade/setnull) inside the
                // explicit tx, BEFORE the parent update. Child rows
                // reference parent *values* (not row identity), so
                // re-keying them first keeps the tx consistent.
                crate::query::batch::fk_on_update::apply_fk_update_plan(fk_update_plan, tx, alias)
                    .await?;
                table
                    .execute_update_tx(op_ref, &ctx, tx, Some(self.resolver), &self.actor)
                    .await
                    .map_err(|e| {
                        if crate::query::batch::exec_scope::is_request_cancelled(&e) {
                            return BatchError::Cancelled;
                        }
                        BatchError::QueryError {
                            alias: alias.to_string(),
                            message: e.to_string(),
                            code: e.code().map(str::to_owned),
                        }
                    })?
            }
            // F4b-2: "everything is a transaction" — a non-tx update
            // routes through the implicit single-op BATCH transaction
            // (same pattern as INSERT in F4b-1).
            //
            // F-28 Step 1: straight-line begin/execute/commit — no
            // HRTB closure, so `self.resolver`/`ctx` can be borrowed
            // normally (this also fixes D2: the implicit path was
            // wrongly passing `None` instead of the resolver).
            //
            // F-28 Step 5 (S3-C): PARENT-side hook — an implicit
            // update against a table flagged as an FK parent with a
            // non-NoAction on_update action opens Serializable
            // instead of Snapshot, for the SAME reason as the DELETE
            // arm below: `plan_fk_on_update` runs the identical
            // `list_stream_tx(Some(tx), ..)` child-table scans
            // (RESTRICT/CASCADE/SET NULL) that DELETE's
            // `fk_restrict`/`fk_actions` run, reading child tables to
            // decide fan-out before the parent update commits. A
            // concurrent tx inserting a new child row referencing the
            // OLD parent value between this plan and commit exposes
            // the identical race (RESTRICT could wrongly allow the
            // rename past a reference that now dangles; CASCADE/SET
            // NULL could miss re-keying/nulling the new row). Same
            // bounded retry as DELETE.
            None => {
                let repo = self
                    .resolver
                    .resolve_repo(&table_ref.repo)
                    .await
                    .map_err(|e| BatchError::QueryError {
                        alias: alias.to_string(),
                        message: format!("resolve_repo({}): {}", table_ref.repo, e),
                        code: None,
                    })?;
                let isolation = implicit_tx_isolation_for_fk_parent(
                    self.resolver,
                    table_ref,
                    FkParentOpKind::Update,
                )
                .await;
                return retry_on_tx_conflict(|| async {
                    let (mut tx, _guard) =
                        crate::query::batch::begin_implicit_batch_tx_with_isolation(
                            &repo,
                            self.actor.clone(),
                            alias,
                            isolation,
                        )
                        .await?;
                    // F-28 Step 5: child-side hook, implicit path.
                    require_footprint_if_fk_child(
                        self.resolver,
                        table_ref,
                        table.table_token(),
                        &mut tx,
                    )
                    .await;
                    // F-28 Step 2: build the plan AFTER the implicit
                    // tx has begun, so its probes see this tx's own
                    // staged writes (read-your-own-writes).
                    let fk_update_plan = crate::query::batch::fk_on_update::plan_fk_on_update(
                        self.resolver,
                        table_ref,
                        table,
                        op_ref,
                        &ctx,
                        alias,
                        &tx,
                    )
                    .await?;
                    // Apply child fan-out inside the implicit tx
                    // BEFORE the parent update. The plan carries
                    // pre-resolved child handles (no resolver needed
                    // here).
                    crate::query::batch::fk_on_update::apply_fk_update_plan(
                        fk_update_plan,
                        &mut tx,
                        alias,
                    )
                    .await?;
                    let wr = table
                        .execute_update_tx(op_ref, &ctx, &mut tx, Some(self.resolver), &self.actor)
                        .await
                        .map_err(|e| {
                            if crate::query::batch::exec_scope::is_request_cancelled(&e) {
                                return BatchError::Cancelled;
                            }
                            BatchError::QueryError {
                                alias: alias.to_string(),
                                message: e.to_string(),
                                code: e.code().map(str::to_owned),
                            }
                        })?;
                    let receipt = crate::query::batch::receipts::commit_implicit_with_cancel(
                        &repo,
                        tx,
                        alias,
                        wait.unwrap_or_else(|| repo.default_visibility_wait()),
                        self.scope.cancel_handle().cloned(),
                    )
                    .await?;
                    Ok(crate::query::batch::receipts::attach(
                        wr,
                        self.result_encoding,
                        interner,
                        receipt,
                    ))
                })
                .await;
            }
        };
        Ok(write_result_to_query_result_with_encoding(
            wr,
            self.result_encoding,
            interner,
        ))
    }

    pub(super) async fn run_delete_arm(
        &mut self,
        arm: DataArm<'_>,
    ) -> Result<QueryResult, BatchError> {
        let DataArm {
            alias,
            entry,
            table_ref,
            resource,
            table,
            interner,
            ctx,
            wait,
        } = arm;
        let BatchOp::Delete(op) = &entry.op else {
            unreachable!()
        };
        // Observability trace only — see `trace_access`'s doc
        // comment. Real enforcement already ran in execute_as /
        // tx_execute_as before this runner was reached.
        trace_access(&self.actor, &resource, Action::Delete).map_err(|e| {
            BatchError::QueryError {
                alias: alias.to_string(),
                message: e.to_string(),
                code: None,
            }
        })?;

        // Phase D.1 — ON DELETE RESTRICT gate.
        // Phase D.2 — ON DELETE CASCADE + SET NULL.
        //
        // F-28 Step 2: both the RESTRICT gate and the cascade/setnull
        // plan are now built AFTER a `tx` is available (explicit tx,
        // or the implicit tx begun below), and their row-level probes
        // read via `list_stream_tx(Some(tx), ..)` — read-your-own-
        // writes against this transaction's own staged writes. The
        // cascade plan is still owned data (pre-resolved child
        // TableManager handles + parent values); the full cascade
        // tree (including grandchildren) is resolved at planning
        // time so no resolver is needed inside the apply step.
        //
        // TOCTOU caveat: see fk_restrict.rs / fk_actions.rs docs —
        // the in-tx RYOW gap (D1) is closed; the cross-transaction
        // race remains open (F-28 Step 3/4/5).
        let wr = match self.tx.as_deref_mut() {
            Some(tx) => {
                crate::query::batch::fk_restrict::check_fk_restrict(
                    self.resolver,
                    table_ref,
                    table,
                    &op.where_clause,
                    &ctx,
                    alias,
                    tx,
                )
                .await?;
                let cascade_plan = crate::query::batch::fk_actions::plan_cascade(
                    self.resolver,
                    table_ref,
                    table,
                    &op.where_clause,
                    &ctx,
                    alias,
                    tx,
                )
                .await?;
                // Apply cascade/setnull inside the explicit tx BEFORE
                // the parent delete (child rows reference parent values,
                // not parent row existence at mutation time; ordering
                // is cleaner this way).
                crate::query::batch::fk_actions::apply_cascade_plan(cascade_plan, tx, alias)
                    .await?;
                table
                    .execute_delete_tx(op, &ctx, tx, Some(self.resolver), &self.actor)
                    .await
                    .map_err(|e| {
                        if crate::query::batch::exec_scope::is_request_cancelled(&e) {
                            return BatchError::Cancelled;
                        }
                        BatchError::QueryError {
                            alias: alias.to_string(),
                            message: e.to_string(),
                            code: e.code().map(str::to_owned),
                        }
                    })?
            }
            // F4b-3: "everything is a transaction" — a non-tx delete
            // routes through the implicit single-op BATCH transaction
            // (same pattern as INSERT in F4b-1 and UPDATE in F4b-2).
            //
            // F-28 Step 1: straight-line begin/execute/commit — no
            // HRTB closure, so `self.resolver`/`ctx` can be borrowed
            // normally (this also fixes D2: the implicit path was
            // wrongly passing `None` instead of the resolver).
            //
            // F-28 Step 5 (S3-C): PARENT-side hook — an implicit
            // delete against a table flagged as an FK parent with a
            // non-NoAction on_delete action opens Serializable
            // instead of Snapshot, so the RESTRICT/CASCADE/SET NULL
            // child scans below record a real SSI predicate (closing
            // the cross-transaction race). Wrapped in a small bounded
            // retry (`retry_on_tx_conflict`) so an already-resolved
            // race doesn't surface as a client-visible error for what
            // looks like an ordinary single delete — see that
            // function's doc for the full rationale.
            None => {
                let repo = self
                    .resolver
                    .resolve_repo(&table_ref.repo)
                    .await
                    .map_err(|e| BatchError::QueryError {
                        alias: alias.to_string(),
                        message: format!("resolve_repo({}): {}", table_ref.repo, e),
                        code: None,
                    })?;
                let isolation = implicit_tx_isolation_for_fk_parent(
                    self.resolver,
                    table_ref,
                    FkParentOpKind::Delete,
                )
                .await;
                return retry_on_tx_conflict(|| async {
                    let (mut tx, _guard) =
                        crate::query::batch::begin_implicit_batch_tx_with_isolation(
                            &repo,
                            self.actor.clone(),
                            alias,
                            isolation,
                        )
                        .await?;
                    // F-28 Step 2: run the RESTRICT gate + build the
                    // cascade plan AFTER the implicit tx has begun, so
                    // their probes see this tx's own staged writes.
                    crate::query::batch::fk_restrict::check_fk_restrict(
                        self.resolver,
                        table_ref,
                        table,
                        &op.where_clause,
                        &ctx,
                        alias,
                        &tx,
                    )
                    .await?;
                    let cascade_plan = crate::query::batch::fk_actions::plan_cascade(
                        self.resolver,
                        table_ref,
                        table,
                        &op.where_clause,
                        &ctx,
                        alias,
                        &tx,
                    )
                    .await?;
                    // Apply cascade/setnull inside the implicit tx
                    // BEFORE the parent delete. The plan carries
                    // pre-resolved child handles (no resolver needed
                    // here).
                    crate::query::batch::fk_actions::apply_cascade_plan(
                        cascade_plan,
                        &mut tx,
                        alias,
                    )
                    .await?;
                    let wr = table
                        .execute_delete_tx(op, &ctx, &mut tx, Some(self.resolver), &self.actor)
                        .await
                        .map_err(|e| {
                            if crate::query::batch::exec_scope::is_request_cancelled(&e) {
                                return BatchError::Cancelled;
                            }
                            BatchError::QueryError {
                                alias: alias.to_string(),
                                message: e.to_string(),
                                code: e.code().map(str::to_owned),
                            }
                        })?;
                    let receipt = crate::query::batch::receipts::commit_implicit_with_cancel(
                        &repo,
                        tx,
                        alias,
                        wait.unwrap_or_else(|| repo.default_visibility_wait()),
                        self.scope.cancel_handle().cloned(),
                    )
                    .await?;
                    Ok(crate::query::batch::receipts::attach(
                        wr,
                        self.result_encoding,
                        interner,
                        receipt,
                    ))
                })
                .await;
            }
        };
        Ok(write_result_to_query_result_with_encoding(
            wr,
            self.result_encoding,
            interner,
        ))
    }

    pub(super) async fn run_set_arm(
        &mut self,
        arm: DataArm<'_>,
    ) -> Result<QueryResult, BatchError> {
        let DataArm {
            alias,
            entry,
            table_ref,
            resource,
            table,
            interner,
            ctx,
            wait,
        } = arm;
        let BatchOp::Set(op) = &entry.op else {
            unreachable!()
        };
        // Observability trace only — see `trace_access`'s doc
        // comment. Real enforcement already ran in execute_as /
        // tx_execute_as before this runner was reached.
        trace_access(&self.actor, &resource, Action::Write).map_err(|e| {
            BatchError::QueryError {
                alias: alias.to_string(),
                message: e.to_string(),
                code: None,
            }
        })?;
        // #641: resolve reserved markers ($param/$query/$fn/$cond/
        // $expr) inside the upsert `key` and `value`.
        // `resolve_write_value` has a fast path (clone) when no
        // marker nodes exist.
        let subst_key = resolve_write_value(&op.key, &ctx)
            .map_err(|e| write_value_error_to_batch_error(alias, e))?;
        let subst_value = resolve_write_value(&op.value, &ctx)
            .map_err(|e| write_value_error_to_batch_error(alias, e))?;
        let subst_op;
        let op_ref: &shamir_query_types::write::SetOp =
            if subst_key == op.key && subst_value == op.value {
                op
            } else {
                subst_op = shamir_query_types::write::SetOp {
                    set: op.set.clone(),
                    key: subst_key,
                    value: subst_value,
                };
                &subst_op
            };
        let wr = match self.tx.as_deref_mut() {
            Some(tx) => {
                // F-28 Step 5 (S3-C): child-side hook — see the
                // identical comment on the Insert/Update arms above.
                require_footprint_if_fk_child(self.resolver, table_ref, table.table_token(), tx)
                    .await;
                table
                    .execute_set_tx(op_ref, tx, Some(self.resolver), &self.actor)
                    .await
                    .map_err(|e| {
                        if crate::query::batch::exec_scope::is_request_cancelled(&e) {
                            return BatchError::Cancelled;
                        }
                        BatchError::QueryError {
                            alias: alias.to_string(),
                            message: e.to_string(),
                            code: None,
                        }
                    })?
            }
            // W3d-2: non-tx SET routes through the implicit single-op
            // batch transaction (same pattern as DELETE in F5a, INSERT
            // in F4b-1, UPDATE in F4b-2).
            //
            // F-28 Step 1: straight-line begin/execute/commit — no
            // HRTB closure, so `self.resolver` can be borrowed
            // normally (this also fixes D2: the implicit path was
            // wrongly passing `None` instead of the resolver).
            None => {
                let repo = self
                    .resolver
                    .resolve_repo(&table_ref.repo)
                    .await
                    .map_err(|e| BatchError::QueryError {
                        alias: alias.to_string(),
                        message: format!("resolve_repo({}): {}", table_ref.repo, e),
                        code: None,
                    })?;
                let (mut tx, _guard) =
                    crate::query::batch::begin_implicit_batch_tx(&repo, self.actor.clone(), alias)
                        .await?;
                // F-28 Step 5 (S3-C): child-side hook, implicit path.
                require_footprint_if_fk_child(
                    self.resolver,
                    table_ref,
                    table.table_token(),
                    &mut tx,
                )
                .await;
                let wr = table
                    .execute_set_tx(op_ref, &mut tx, Some(self.resolver), &self.actor)
                    .await
                    .map_err(|e| {
                        if crate::query::batch::exec_scope::is_request_cancelled(&e) {
                            return BatchError::Cancelled;
                        }
                        BatchError::QueryError {
                            alias: alias.to_string(),
                            message: e.to_string(),
                            code: None,
                        }
                    })?;
                let receipt = crate::query::batch::receipts::commit_implicit_with_cancel(
                    &repo,
                    tx,
                    alias,
                    wait.unwrap_or_else(|| repo.default_visibility_wait()),
                    self.scope.cancel_handle().cloned(),
                )
                .await?;
                return Ok(crate::query::batch::receipts::attach(
                    wr,
                    self.result_encoding,
                    interner,
                    receipt,
                ));
            }
        };
        Ok(write_result_to_query_result_with_encoding(
            wr,
            self.result_encoding,
            interner,
        ))
    }
}

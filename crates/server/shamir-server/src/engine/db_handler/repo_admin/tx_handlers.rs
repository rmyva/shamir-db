use shamir_connect::server::session::Session;
use shamir_db::query::batch::{BatchError, BatchOp, BatchRequest};
use shamir_query_types::batch::{BatchTreeVisitor, IsolationLevel, VisibilityWait};
use shamir_types::access::{Action, ResourcePath};
use std::sync::Arc;

use crate::tx_registry::{InteractiveTx, TxAbortRelease, TxRegistryError};

use super::activity::server_busy_response;
use super::admin::{check_destructive_hmacs, is_coarse_admin_gate_exempt};
use super::handler::{
    DbResponse, ParkedCommitCleanup, ShamirDbHandler, INTERACTIVE_TX_MAX_LIFETIME,
};

use crate::db_handler::config::NodeMode;

/// RV-SERVER-C73466A7: `execute`'s read-only replica gate applied to the
/// interactive path (same contract, same classifier). A `ReadOnly` node
/// still SERVES reads inside transactions — the card explicitly preserves
/// permitted reads (P0.md, "сохранении actor DAC и разрешённых reads") —
/// so a tx batch is rejected with the existing `read_only_replica` error
/// only when it contains a mutating operation, classified by the SAME
/// `BatchOp::is_write` visitor the non-transactional `execute` uses
/// (`handler.rs`). No-op for `ReadWrite`.
fn read_only_tx_write_gate(node_mode: NodeMode, batch: &BatchRequest) -> Option<DbResponse> {
    if node_mode != NodeMode::ReadOnly {
        return None;
    }
    for visit in BatchTreeVisitor::new(&batch.queries).filter(|visit| visit.body_depth().is_none())
    {
        let (alias, entry) = (visit.alias, visit.entry);
        if entry.op.is_write() {
            return Some(DbResponse::Error {
                code: "read_only_replica".into(),
                message: format!(
                    "query '{}' is a write; this node is a read-only replica",
                    alias
                ),
            });
        }
    }
    None
}

impl ShamirDbHandler {
    /// The releaser a newly parked [`InteractiveTx`] must carry: it is bound to
    /// the repository RESOLVED for this handle, so a `RenameDb`/`RenameRepo`
    /// (or a `DropDb`) between `TxBegin` and the end of the transaction cannot
    /// make the release resolve nothing. `None` (the repository vanished
    /// between `tx_begin_as` and this insert) logs instead of dropping the
    /// locks silently.
    pub fn tx_abort_release(&self, db_name: &str, repo_name: &str) -> TxAbortRelease {
        let repo = self
            .db
            .get_db(db_name)
            .and_then(|db| db.get_repo(repo_name));
        super::activity::tx_abort_release(repo)
    }

    /// Phase B BEGIN — open an interactive tx, park it in the registry bound
    /// to this session, reply with the minted handle + snapshot version.
    ///
    /// R4b-1 — the `tx_open` unit of work is registered in the in-flight
    /// activity table (RAII guard) after every cheap fast-reject gate and
    /// BEFORE the engine open wait (`db.tx_begin_as`), so a BEGIN stuck
    /// waiting on the engine stays visible as `Running`; `client_rid` is the
    /// OPTIONAL transport request id carried alongside the minted
    /// [`RequestExecId`].
    pub(super) async fn tx_begin_with_rid(
        &self,
        session: &Session,
        query_version: u32,
        db_name: &str,
        repo_name: &str,
        isolation: Option<IsolationLevel>,
        client_rid: Option<u32>,
    ) -> DbResponse {
        if let Err(e) = crate::version::check_query_lang(query_version) {
            return DbResponse::Error {
                code: "unsupported_query_version".into(),
                message: e.to_string(),
            };
        }
        // R4b-1 — in-flight registration, one insert + one RAII removal
        // (the guard owns the entry AND the per-session request slot).
        // Per-session cap (R4b-1 follow-up): a stamped session must
        // acquire a slot under `QueryLimitsCap::
        // max_in_flight_requests_per_session` HERE, before the engine
        // open wait; an exhausted session is rejected with the fixed
        // `server_busy` wire text (same code/message as the HTTP REST
        // executor and the serialized launcher overload frame) with no
        // side effect. Skipped ONLY for the stateless (handle-0)
        // fallback. The op-kind is the static discriminant name
        // `tx_open`; no query value ever enters the summary (privacy,
        // D3).
        // Visible in activity; interactive transaction rollback via operator
        // cancellation is deferred to R4b step 3.
        let _in_flight = match self.try_track_activity_request_with_cancellability(
            session, db_name, "tx_open", client_rid, false,
        ) {
            Ok(guard) => guard,
            Err(()) => return server_busy_response(),
        };

        // R3-2b — maintenance-gate enrollment for TxBegin. BEGIN can trigger
        // lazy recovery / fresh snapshot registration on the engine side, so
        // it enrolls unconditionally: admission is taken AFTER the version
        // check and the in-flight registration (a server-busy reject never
        // queues behind the gate) and BEFORE `db.tx_begin_as` touches any
        // transaction state; the guard is retained to the end of the call.
        let _maintenance = self.maintenance_admission().await;

        // RV-SERVER-C73466A7: TxBegin is NOT mode-gated — opening a
        // snapshot transaction is read-capable and a read-only replica must
        // keep serving reads inside interactive transactions. Writes are
        // refused per-op at TxExecute and at commit (below).
        let iso = isolation.unwrap_or(IsolationLevel::Snapshot);

        let actor = super::handler::session_actor(session);
        if let Err(e) = self
            .db
            .authorize_access(
                &actor,
                &ResourcePath::Database {
                    db: db_name.to_string(),
                },
                Action::Read,
            )
            .await
        {
            let err = BatchError::query_coded("", "access_denied", e.to_string());
            return DbResponse::Error {
                code: super::handler::error_code(&err).to_string(),
                message: err.to_string(),
            };
        }
        #[cfg(test)]
        self.begin_pause.pause().await;
        // `ShamirDb::tx_begin_as` still speaks the wire-string vocabulary;
        // `IsolationLevel::as_str` is exactly that vocabulary (v4 enum).
        let begin = self
            .db
            .tx_begin_as(actor, db_name, repo_name, iso.as_str())
            .await;
        let (tx, guard, repo) = match begin {
            Ok(pair) => pair,
            Err(e) => {
                return DbResponse::Error {
                    code: super::handler::error_code(&e).to_string(),
                    message: e.to_string(),
                }
            }
        };

        let handle = tx.tx_id.0;
        let snapshot_version = tx.snapshot_version;
        let it = InteractiveTx::new(
            tx,
            guard,
            session.session_id,
            session.user_id,
            db_name.to_string(),
            repo_name.to_string(),
            INTERACTIVE_TX_MAX_LIFETIME,
            super::activity::tx_abort_release(Some(repo.clone())),
        )
        .with_bound_repo(repo);
        // The release-tail owner is bound by the registry at registration
        // (`TxRegistry::register_activity`), never by the caller.
        match self.tx_registry.register_activity(
            handle,
            it,
            session.activity_handle(),
            Arc::from(session.username.as_str()),
        ) {
            // On reject the just-opened tx (`it`) drops = RAII rollback.
            Err(TxRegistryError::TxAlreadyOpen) => DbResponse::Error {
                code: "tx_already_open".into(),
                message: "session already has an open transaction".into(),
            },
            // R4b-1: tx ids are minted per repo; a collision with a tx from
            // another repo fails closed (the just-opened tx drops = rollback).
            Err(TxRegistryError::TxHandleCollision) => DbResponse::Error {
                code: "tx_handle_conflict".into(),
                message: "transaction handle already in use; retry (per-repo tx id collision)"
                    .into(),
            },
            Err(_) => DbResponse::Error {
                code: "tx_error".into(),
                message: "could not register transaction".into(),
            },
            Ok(_) => DbResponse::TxOpened {
                tx_handle: handle,
                snapshot_version,
                isolation: iso.as_str().to_string(),
            },
        }
    }

    /// Legacy test entry point: the pre-R4b-1 5-argument `tx_begin` (no
    /// transport request id). Test-only forwarding — identical semantics to
    /// `tx_begin_with_rid(..., None)`; the production caller (`handler.rs`)
    /// passes the transport `rid` via `tx_begin_with_rid`.
    #[cfg(test)]
    pub(super) async fn tx_begin(
        &self,
        session: &Session,
        query_version: u32,
        db_name: &str,
        repo_name: &str,
        isolation: Option<IsolationLevel>,
    ) -> DbResponse {
        self.tx_begin_with_rid(session, query_version, db_name, repo_name, isolation, None)
            .await
    }

    /// Phase B EXECUTE — run one batch inside an open interactive tx (no
    /// commit). Applies the same per-batch gates as [`Self::execute`]
    /// (version, limits cap, admin, destructive-HMAC), then threads the
    /// parked `TxContext` through the engine glue.
    ///
    /// After a maintenance-admitted batch is handed off, dropping the request
    /// stops waiting for the reply, not execution or registry cleanup.
    ///
    /// R4b-1 — the batch is registered in the in-flight activity table
    /// (RAII guard) after every cheap fast-reject gate and BEFORE the
    /// registry acquire and the ctx-lock wait, so an execute stuck waiting
    /// for the parked context stays visible as `Running`. The op-kind is
    /// the static discriminant summary of the batch (homogeneous batches
    /// keep their `activity_name`; heterogeneous ones are conservatively
    /// `write`), derived in the same pre-flight visitor pass that rejects
    /// `ListActivity`, `CancelRequest`, `CancelTx`, `CancelCursorAdmin`,
    /// `ListShapeStats` and `ResetShapeStats` —
    /// never from arguments (privacy, D3).
    pub(super) async fn tx_execute_with_rid(
        &self,
        session: &Session,
        query_version: u32,
        db_name: &str,
        tx_handle: u64,
        batch: BatchRequest,
        client_rid: Option<u32>,
    ) -> DbResponse {
        if let Err(e) = crate::version::check_query_lang(query_version) {
            return DbResponse::Error {
                code: "unsupported_query_version".into(),
                message: e.to_string(),
            };
        }

        // R3-2b — an online-export operation is NEVER valid inside an
        // interactive transaction. Reject EVERY form (standalone, mixed
        // with other ops, or nested inside a Batch/ForEach body at any
        // depth) HERE, before the read-only gate, before the in-flight
        // registration and before any admission lock or partial mutation —
        // and reject unauthorized callers before reporting the invalid form.
        if BatchTreeVisitor::new(&batch.queries).any(|visit| {
            matches!(
                visit.entry.op,
                shamir_query_types::batch::BatchOp::StartExport(_)
                    | shamir_query_types::batch::BatchOp::ExportStatus(_)
                    | shamir_query_types::batch::BatchOp::CancelExport(_)
            )
        }) {
            if let Some(refusal) = super::export_gate::scram_superuser_refusal(session) {
                return refusal;
            }
            return DbResponse::Error {
                code: "online_export_invalid_form".into(),
                message: "online export operations require a standalone, \
                          nontransactional, top-level single-operation batch"
                    .into(),
            };
        }

        if let Some(resp) = read_only_tx_write_gate(self.node_mode, &batch) {
            return resp;
        }

        // R4b-1 — a listing is never transactional. Reject EVERY form
        // (nested in a Batch/ForEach body, mixed with other ops, or
        // standalone) here, BEFORE admission and before the ctx lock is
        // taken, mirroring the `execute` pre-flight. The snapshot of an
        // interactive tx must never serve an operator view that is
        // explicitly nontransactional. The SAME walk derives the batch's
        // op-kind summary (static discriminant names only) AND detects a
        // nested visibility_wait (folded walk, R4b-1 follow-up);
        // precedence is preserved — an activity control op anywhere in the tree
        // is rejected DURING the walk, before the post-walk
        // visibility_wait check runs.
        let mut seen_op: Option<&'static str> = None;
        let mut uniform_ops = true;
        let mut nested_visibility_wait = false;
        for visit in BatchTreeVisitor::new(&batch.queries) {
            let control_op = match &visit.entry.op {
                BatchOp::ListActivity(_) => Some("ListActivity"),
                BatchOp::CancelRequest(_) => Some("CancelRequest"),
                BatchOp::CancelTx(_) => Some("CancelTx"),
                BatchOp::CancelCursorAdmin(_) => Some("CancelCursorAdmin"),
                // R4b step 4: a shape listing or wipe is never transactional
                // (node-local telemetry outside the snapshot), and a reset
                // must never be applied under an open transaction's view of
                // the repository.
                BatchOp::ListShapeStats(_) => Some("ListShapeStats"),
                BatchOp::ResetShapeStats(_) => Some("ResetShapeStats"),
                _ => None,
            };
            if let Some(op) = control_op {
                return DbResponse::Error {
                    code: "invalid_request".into(),
                    message: format!("{op} is not allowed inside a transactional batch"),
                };
            }
            let name = visit.entry.op.activity_name();
            match seen_op {
                None => seen_op = Some(name),
                Some(prev) if prev != name => uniform_ops = false,
                _ => {}
            }
            match &visit.entry.op {
                shamir_query_types::batch::BatchOp::Batch(sub) => {
                    nested_visibility_wait |= sub.batch.visibility_wait.is_some();
                }
                shamir_query_types::batch::BatchOp::ForEach(fe) => {
                    nested_visibility_wait |= fe.batch.visibility_wait.is_some();
                }
                // Only container operations carry nested BatchRequest options.
                _ => {}
            }
        }
        let tx_batch_op_kind = match (seen_op, uniform_ops) {
            (Some(name), true) => name,
            _ => "write",
        };
        if batch.visibility_wait.is_some() || nested_visibility_wait {
            return DbResponse::Error {
                code: "validation".into(),
                message: "visibility_wait is commit-only; use TxCommit".into(),
            };
        }

        // Admin / auth gate. An explicit 4-op allowlist (`List`,
        // `AccessTree`, `DescribeTable`, `GetTableSchema` — task #553, see
        // `is_coarse_admin_gate_exempt`) is exempted from this coarse
        // block; each still runs its own real per-table/per-path
        // authorization further down the stack.
        if !session.permissions.is_superuser {
            for visit in
                BatchTreeVisitor::new(&batch.queries).filter(|visit| visit.body_depth().is_none())
            {
                let (alias, entry) = (visit.alias, visit.entry);
                if entry.op.is_admin() && !is_coarse_admin_gate_exempt(&entry.op) {
                    return DbResponse::Error {
                        code: "permission_denied".into(),
                        message: format!("query '{}' requires superuser (admin/auth op)", alias),
                    };
                }
            }
        }

        // Destructive-op HMAC gate (same "did you mean it" guard as `execute`).
        if let Err((alias, code, message)) = check_destructive_hmacs(session, db_name, &batch) {
            return DbResponse::Error {
                code: code.into(),
                message: format!("query '{}': {}", alias, message),
            };
        }

        // Take admission after preflight and before acquiring the tx context.
        // Share the leaf classification with stateless Execute, not its
        // backup-control exemption: TxExecute previously enrolled CancelBackup.
        // Commit takes its own admission when staged writes land.
        let maintenance = if BatchTreeVisitor::new(&batch.queries)
            .any(|visit| super::execute::op_needs_maintenance_admission(&visit.entry.op))
        {
            self.maintenance_admission().await
        } else {
            None
        };

        // R4b-1 — in-flight registration, one insert + one RAII removal
        // (the guard owns the entry AND the per-session request slot),
        // after every cheap gate (visibility, admin/auth, HMAC) and
        // before the registry acquire / ctx-lock waits. The per-session
        // cap (see `tx_begin_with_rid`) rejects an exhausted session with
        // the fixed `server_busy` text before any execution.
        // Visible in activity; interactive transaction rollback via operator
        // cancellation is deferred to R4b step 3.
        let _in_flight = match self.try_track_activity_request_with_cancellability(
            session,
            db_name,
            tx_batch_op_kind,
            client_rid,
            false,
        ) {
            Ok(guard) => guard,
            Err(()) => return server_busy_response(),
        };

        // Identity-checked stale-failure removals below: `remove_owned`
        // refuses when the reaper already replaced the tx under the same
        // per-repo id.

        // Look up the handle and verify it belongs to this session.
        let (it, _lease) = match self
            .tx_registry
            .acquire_owned(tx_handle, &session.session_id)
        {
            Ok(acquired) => acquired,
            Err(TxRegistryError::TxNotFound) => {
                return DbResponse::Error {
                    code: "tx_not_found".into(),
                    message: format!("no open transaction for handle {}", tx_handle),
                }
            }
            Err(_) => {
                return DbResponse::Error {
                    code: "tx_forbidden".into(),
                    message: "transaction handle does not belong to this session".into(),
                }
            }
        };

        if it.remaining_lifetime().is_zero() {
            self.tx_registry.remove_owned(tx_handle, &it);
            drop(it);
            return DbResponse::Error {
                code: "tx_expired".into(),
                message: "interactive transaction exceeded its absolute lifetime".into(),
            };
        }
        // Preserve wire limits: the exact remaining lifetime is passed below,
        // after acquiring the context lock, so lock contention cannot renew it.

        // The handle is pinned to one (db, repo); every TxExecute must match.
        if it.db() != db_name {
            return DbResponse::Error {
                code: "tx_wrong_db".into(),
                message: format!("handle pinned to db '{}', got '{}'", it.db(), db_name),
            };
        }
        for r in shamir_query_types::batch::distinct_repos(&batch.queries) {
            if r != it.repo() {
                return DbResponse::Error {
                    code: "tx_wrong_repo".into(),
                    message: format!(
                        "TxExecute targets repo '{}' but the handle is pinned to '{}'",
                        r,
                        it.repo()
                    ),
                };
            }
        }

        if let Some(bound_repo) = it.bound_repo() {
            if let Some(reason) = super::tx_binding::tx_execute_binding_refusal(
                &self.db,
                db_name,
                it.repo(),
                bound_repo,
            )
            .await
            {
                self.tx_registry.remove_owned(tx_handle, &it);
                drop(it);
                return DbResponse::Error {
                    code: "tx_wrong_repo".into(),
                    message: reason,
                };
            }
        }

        let slot = if maintenance.is_some() {
            match self.try_owned_mutation_slot() {
                Ok(slot) => Some(slot),
                Err(()) => return server_busy_response(),
            }
        } else {
            None
        };
        let spawn_owned = slot.is_some();
        let actor = super::handler::session_actor(session);
        let repo_observer = self
            .audit
            .as_ref()
            .map(|audit| super::repo_audit::repo_audit_observer(audit.clone(), session));
        let db = Arc::clone(&self.db);
        let tx_registry = Arc::clone(&self.tx_registry);
        let policy = self.execution_policy();
        let max_tx_bytes = self.tx_limits.max_tx_bytes;
        let execute = async move {
            let (_slot, _maintenance, _in_flight, _lease) = (slot, maintenance, _in_flight, _lease);
            let exec = {
                let mut guard = it.ctx().lock().await;
                match guard.as_mut() {
                    Some(tx) => {
                        let r = db
                            .tx_execute_with_policy_and_budget_and_repo_observer(
                                actor,
                                it.db(),
                                &batch,
                                tx,
                                policy,
                                Some(it.remaining_lifetime()),
                                repo_observer,
                            )
                            .await;
                        // Measure staged size INSIDE the same lock as the engine call.
                        r.map(|resp| (resp, guard.as_ref().map_or(0, |tx| tx.staged_bytes())))
                    }
                    None => {
                        return DbResponse::Error {
                            code: "tx_not_found".into(),
                            message: format!("no open transaction for handle {}", tx_handle),
                        }
                    }
                }
            };
            match exec {
                Ok((response, staged_bytes)) => {
                    if staged_bytes > max_tx_bytes {
                        // Abort: drop the handle (RAII rollback of TxContext +
                        // SnapshotGuard via the Arc) and surface the cap to the
                        // client.
                        tx_registry.remove_owned(tx_handle, &it);
                        drop(it);
                        return DbResponse::Error {
                            code: "tx_too_large".into(),
                            message: format!(
                                "interactive transaction exceeded max_tx_bytes ({} > {})",
                                staged_bytes, max_tx_bytes
                            ),
                        };
                    }
                    DbResponse::TxBatch { response }
                }
                Err(e) => {
                    if matches!(
                        &e,
                        BatchError::ExecutionTimedOut { .. } | BatchError::LimitExceeded { .. }
                    ) {
                        tx_registry.remove_owned(tx_handle, &it);
                        drop(it);
                    }
                    super::cursor_handlers::error_response(&e)
                }
            }
        };

        if spawn_owned {
            // Admin can write directly, not just stage changes. The task owns
            // admission and the bounded slot through execution and cleanup.
            // fire-and-forget: detached by design if the reply waiter is dropped.
            match self.owned_mutation_tasks().spawn(execute) {
                Some(worker) => worker.await.unwrap_or_else(|_| DbResponse::Error {
                    code: "internal_error".into(),
                    message: "cannot complete database request".into(),
                }),
                // Shutdown closed worker admission; refuse instead of starting
                // work the flush cannot wait for.
                None => server_busy_response(),
            }
        } else {
            execute.await
        }
    }

    /// Phase B COMMIT — remove the handle from the registry, run the Phase-A
    /// commit pipeline on its `TxContext`, reply with the `TransactionInfo`.
    /// The owning `Arc` (and thus the `SnapshotGuard`) is held alive until
    /// commit returns, so the MVCC snapshot stays pinned through commit.
    pub(super) async fn tx_commit(
        &self,
        session: &Session,
        db_name: &str,
        tx_handle: u64,
    ) -> DbResponse {
        self.tx_commit_with_options(session, db_name, tx_handle, None)
            .await
    }

    pub(super) async fn tx_commit_with_options(
        &self,
        session: &Session,
        db_name: &str,
        tx_handle: u64,
        wait: Option<VisibilityWait>,
    ) -> DbResponse {
        // R3-2b — maintenance-gate enrollment for TxCommit. A commit can
        // land staged writes, and whether it did cannot be known without
        // taking the ctx lock, so the admission is taken UNCONDITIONALLY at
        // the top — before `get_owned`, `begin_commit` or any ctx lock —
        // and retained through the commit pipeline (the guard drops at the
        // end of this fn, after `tx_commit_as_with_options` returned). A
        // snapshot-only commit therefore also queues behind a freeze; the
        // wait is bounded by the export job's ceiling and the tx's own
        // absolute lifetime (INTERACTIVE_TX_MAX_LIFETIME) still bounds the
        // parked handle on the registry side.
        let _maintenance = self.maintenance_admission().await;

        let it = match self.tx_registry.get_owned(tx_handle, &session.session_id) {
            Ok(it) => it,
            Err(TxRegistryError::TxNotFound) => {
                return DbResponse::Error {
                    code: "tx_not_found".into(),
                    message: format!("no open transaction for handle {}", tx_handle),
                }
            }
            Err(_) => {
                return DbResponse::Error {
                    code: "tx_forbidden".into(),
                    message: "transaction handle does not belong to this session".into(),
                }
            }
        };
        if it.db() != db_name {
            return DbResponse::Error {
                code: "tx_wrong_db".into(),
                message: format!("handle pinned to db '{}', got '{}'", it.db(), db_name),
            };
        }

        // RV-SERVER-C73466A7: a read-only node must never land staged
        // writes. The commit is refused ONLY when the tx actually staged a
        // write; a read-only (snapshot) tx commits normally.
        let staged_bytes = {
            let guard = it.ctx().lock().await;
            guard.as_ref().map_or(0, |tx| tx.staged_bytes())
        };
        if self.node_mode == NodeMode::ReadOnly && staged_bytes > 0 {
            return DbResponse::Error {
                code: "read_only_replica".into(),
                message: format!(
                    "transaction has staged writes ({} bytes); this node is a read-only replica",
                    staged_bytes
                ),
            };
        }

        // Commit observation guard (R4b-1): the handle leaves `open` here —
        // no concurrent TxExecute, re-register or reap can find it, so the tx
        // is no longer executable — but the activity-mirror row stays visible
        // as `AwaitingCommit` until `commit_guard` drops, i.e. until the
        // commit pipeline returned (success or failure). The guard holds no
        // ctx lock and never makes the tx executable. `None` = raced the
        // reaper between `get_owned` and now.
        let _commit_guard = match self.tx_registry.begin_commit(tx_handle, &it) {
            Some(guard) => guard,
            None => {
                return DbResponse::Error {
                    code: "tx_not_found".into(),
                    message: format!("no open transaction for handle {}", tx_handle),
                }
            }
        };

        // Invalid commit policy consumes a correctly addressed owned handle too.
        // Dropping `it` releases the snapshot and rolls back staged writes.
        let wait = match self.transactions.resolve_commit_wait(wait) {
            Ok(wait) => wait,
            Err(e) => {
                drop(it);
                return DbResponse::Error {
                    code: "visibility_wait_invalid".into(),
                    message: e.to_string(),
                };
            }
        };
        let actor = super::handler::session_actor(session);
        let tx = it.ctx().lock().await.take();
        let mut cleanup = ParkedCommitCleanup::new(tx, Arc::clone(it.abort_release()));
        #[cfg(test)]
        self.pause_commit_after_take(tx_handle).await;
        if let Err(e) = self
            .db
            .authorize_access(
                &actor,
                &ResourcePath::Database {
                    db: db_name.to_string(),
                },
                Action::Write,
            )
            .await
        {
            let err = BatchError::query_coded("", "access_denied", e.to_string());
            cleanup.release_now().await;
            return DbResponse::Error {
                code: super::handler::error_code(&err).to_string(),
                message: err.to_string(),
            };
        }
        let release = Arc::clone(&cleanup.release);
        let commit = match cleanup.disarm() {
            Some(tx) => {
                drop(cleanup);
                self.db
                    .tx_commit_parked_as(
                        db_name,
                        it.repo(),
                        it.bound_repo(),
                        tx,
                        release,
                        Some(wait),
                    )
                    .await
            }
            None => Err(BatchError::QueryError {
                alias: String::new(),
                message: "transaction is already committed or rolled back".into(),
                code: None,
            }),
        };
        // `it` (holding the SnapshotGuard) drops here, AFTER commit returned —
        // the snapshot stayed pinned through commit.
        drop(it);
        match commit {
            Ok(transaction) => DbResponse::TxCommitted { transaction },
            Err(e) => DbResponse::Error {
                code: super::handler::error_code(&e).to_string(),
                message: e.to_string(),
            },
        }
    }

    /// Phase B ROLLBACK — remove the handle and drop the parked tx (RAII
    /// rollback, no storage side effects).
    pub(super) async fn tx_rollback(
        &self,
        session: &Session,
        db_name: &str,
        tx_handle: u64,
    ) -> DbResponse {
        let it = match self.tx_registry.get_owned(tx_handle, &session.session_id) {
            Ok(it) => it,
            Err(TxRegistryError::TxNotFound) => {
                return DbResponse::Error {
                    code: "tx_not_found".into(),
                    message: format!("no open transaction for handle {}", tx_handle),
                }
            }
            Err(_) => {
                return DbResponse::Error {
                    code: "tx_forbidden".into(),
                    message: "transaction handle does not belong to this session".into(),
                }
            }
        };
        if it.db() != db_name {
            return DbResponse::Error {
                code: "tx_wrong_db".into(),
                message: format!("handle pinned to db '{}', got '{}'", it.db(), db_name),
            };
        }
        // Identity-checked removal (R4b-1): if the reaper already took the
        // tx and another one was re-registered under the same per-repo id,
        // the fresh transaction survives (this rollback drops only OUR
        // parked tx). `None` = raced; the response is unchanged.
        self.tx_registry.remove_owned(tx_handle, &it);
        // Last ref drops here → InteractiveTx drops → TxContext drops (RAII
        // rollback) and the SnapshotGuard releases the GC pin.
        drop(it);
        DbResponse::TxRolledBack { tx_handle }
    }
}

#[cfg(test)]
impl ShamirDbHandler {
    /// Legacy test entry points: the pre-R4b-1 signatures without a
    /// transport request id. Test-only forwarding — identical semantics to
    /// the `*_with_rid(..., None)` forms; the production caller
    /// (`handler.rs`) passes the transport `rid` through the `_with_rid`
    /// methods.
    pub(super) async fn tx_execute(
        &self,
        session: &Session,
        query_version: u32,
        db_name: &str,
        tx_handle: u64,
        batch: BatchRequest,
    ) -> DbResponse {
        self.tx_execute_with_rid(session, query_version, db_name, tx_handle, batch, None)
            .await
    }
}

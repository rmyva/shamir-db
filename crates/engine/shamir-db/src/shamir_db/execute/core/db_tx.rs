//! `impl ShamirDb { tx_begin, tx_begin_as, tx_execute, tx_execute_as, tx_commit, tx_commit_as }`.
//!
//! Phase B — interactive (multi-call) transactions.
//!
//! These facade methods expose the engine's interactive-tx glue
//! (`open_interactive_tx` / `execute_in_open_tx` / `commit_interactive_tx`)
//! to the server, which owns the live-tx registry (it depends on `shamir-tx`
//! directly). The facade resolves the db/repo and builds the same
//! resolver + admin pair `execute` uses, then drives one lifecycle step. The
//! `TxContext` / `SnapshotGuard` flow back to the server registry via the
//! engine re-export (`crate::engine::tx::*`) — the same concrete `shamir-tx`
//! types the server names. See `docs/dev-artifacts/roadmap/PHASE_B_INTERACTIVE_TX.md` §5.

use std::sync::Arc;
use std::time::{Duration, Instant};

use shamir_query_types::batch::{ExecPolicy, ResyncRequiredDetail, VisibilityWait};

use crate::access::{Action, Actor, ResourcePath};
use crate::query::batch::{
    execute_in_open_tx, open_interactive_tx, BatchError, BatchRequest, BatchResponse,
    TransactionInfo,
};
use crate::shamir_db::execute::skip_read_quarantine_for_test;

use super::super::shamir_db::ShamirDb;
use super::admin_dispatch::ShamirAdminExecutor;
use super::db_execute::quarantined_read_hit;
use super::function_invoker::ShamirFunctionInvoker;
use super::table_resolver::DbTableResolver;

/// S5c-X PR8b: ordinary READS of a quarantined (`resync_required`) repo fail
/// closed for the interactive-tx family too. `tx_begin[_as]` refuses to pin a
/// snapshot view of a quarantined repo (fail closed at open); the
/// `tx_execute*` family re-checks per execute, so a tx opened BEFORE the
/// quarantine also fails closed. The negative-control seam mirrors the batch
/// gate in `repo_admin/db_execute.rs` exactly (test builds only).
/// Writes/COMMIT are NOT gated — reads are gated at begin/execute instead.
fn tx_read_quarantine_refusal(
    shamir: &ShamirDb,
    db_name: &str,
    alias: &str,
    repo_name: &str,
) -> Option<BatchError> {
    let skip_seam = skip_read_quarantine_for_test();
    if skip_seam {
        return None;
    }
    let reason = shamir.quarantined_read_reason(db_name, repo_name)?;
    Some(BatchError::ResyncRequired {
        alias: alias.to_string(),
        detail: Box::new(ResyncRequiredDetail {
            db: db_name.to_string(),
            repo: repo_name.to_string(),
            reason,
        }),
    })
}

/// Request-shaped twin of [`tx_read_quarantine_refusal`] for the
/// `tx_execute*` family — and the SINGLE shared implementation of the
/// request-shaped quarantine gate, also called by the batch path in
/// `repo_admin/db_execute.rs`. Mirrors the batch gate's whole-query-tree
/// walk so a read op targeting a quarantined repo is refused even inside a
/// pre-quarantine tx.
///
/// Deliberately SYNCHRONOUS (no `.await`) and fully self-contained (skip
/// seam + ONE snapshot load live here, not at the call sites): the async
/// callers keep only a one-line `if let Some(err) = ...` in their bodies,
/// so the tree-walk's locals (visitor, per-op bindings) never become fields
/// of the callers' generated `Future` state machines. Inlining the walk
/// into an async fn inflates that future, and under nested
/// `Batch`/`ForEach` recursion the future is held on the stack once per
/// recursion level — the Round-12 Windows stack overflow traced to exactly
/// that. Keep this helper sync and out of line: once inlined, its locals
/// still widen the caller's `poll` frame, which nests once per level.
#[inline(never)]
pub(crate) fn tx_read_quarantine_refusal_for_request(
    shamir: &ShamirDb,
    db_name: &str,
    request: &BatchRequest,
) -> Option<BatchError> {
    if skip_read_quarantine_for_test() {
        return None;
    }
    // ONE snapshot load per lookup; the whole-tree walk runs against the
    // loaded snapshot (never re-loads between ops).
    let snapshot = shamir.resync_quarantine.snapshot();
    quarantined_read_hit(db_name, request, &snapshot).map(|(alias, repo, reason)| {
        BatchError::ResyncRequired {
            alias,
            detail: Box::new(ResyncRequiredDetail {
                db: db_name.to_string(),
                repo,
                reason,
            }),
        }
    })
}

impl ShamirDb {
    /// BEGIN: open an interactive tx against `db_name`/`repo_name`. Returns
    /// the live `TxContext` + its `SnapshotGuard` for the caller (the server
    /// registry) to park between round-trips.
    pub async fn tx_begin(
        &self,
        db_name: &str,
        repo_name: &str,
        isolation: &str,
    ) -> Result<
        (
            crate::engine::tx::TxContext,
            crate::engine::tx::SnapshotGuard,
            crate::engine::repo::RepoInstance,
        ),
        BatchError,
    > {
        self.tx_begin_as(Actor::System, db_name, repo_name, isolation)
            .await
    }

    /// BEGIN with an explicit [`Actor`].
    pub async fn tx_begin_as(
        &self,
        actor: Actor,
        db_name: &str,
        repo_name: &str,
        isolation: &str,
    ) -> Result<
        (
            crate::engine::tx::TxContext,
            crate::engine::tx::SnapshotGuard,
            crate::engine::repo::RepoInstance,
        ),
        BatchError,
    > {
        self.authorize_access(
            &actor,
            &ResourcePath::Database {
                db: db_name.to_string(),
            },
            Action::Read,
        )
        .await
        .map_err(|e| BatchError::query_coded("", "access_denied", e.to_string()))?;
        if let Some(err) = tx_read_quarantine_refusal(self, db_name, "", repo_name) {
            return Err(err);
        }
        let db = self.get_db(db_name).ok_or_else(|| BatchError::QueryError {
            alias: String::new(),
            message: format!("Database '{}' not found", db_name),
            code: None,
        })?;
        let repo = db
            .get_repo(repo_name)
            .ok_or_else(|| BatchError::QueryError {
                alias: String::new(),
                message: format!("Repository '{}' not found", repo_name),
                code: None,
            })?;
        let iso = match isolation {
            "snapshot" => crate::engine::tx::IsolationLevel::Snapshot,
            "serializable" => crate::engine::tx::IsolationLevel::Serializable,
            "pessimistic" => crate::engine::tx::IsolationLevel::Pessimistic,
            other => {
                return Err(BatchError::query_coded(
                    "",
                    "invalid_request",
                    format!("unknown isolation '{other}'"),
                ));
            }
        };
        let (mut tx, guard) =
            open_interactive_tx(repo.core(), iso)
                .await
                .map_err(|e| BatchError::QueryError {
                    alias: String::new(),
                    message: format!("begin_tx: {}", e),
                    code: None,
                })?;
        tx.set_actor(actor);
        Ok((tx, guard, repo))
    }

    /// EXECUTE: run one batch inside an already-open interactive tx, WITHOUT
    /// committing. The `BatchResponse` carries `transaction: None` (the tx is
    /// still open). The single-repo guard is enforced inside the engine glue;
    /// the caller additionally asserts the batch targets the handle's repo.
    pub async fn tx_execute(
        &self,
        db_name: &str,
        request: &BatchRequest,
        tx: &mut crate::engine::tx::TxContext,
    ) -> Result<BatchResponse, BatchError> {
        self.tx_execute_as(Actor::System, db_name, request, tx)
            .await
    }

    /// EXECUTE with an explicit [`Actor`].
    ///
    /// Authorization (DB visibility + every op's `required_access`,
    /// recursively through nested `Batch`/`ForEach` bodies, deduplicated —
    /// mirrors the inline ACL cache this function used to keep by hand)
    /// now happens INSIDE the type-level seam (#1199): `self` is passed as
    /// the [`crate::query::batch::AccessGate`], and `execute_in_open_tx`
    /// structurally cannot run without the resulting [`Authorized`] token.
    /// See `Authorized::authorize`'s doc.
    ///
    /// S5c-X PR8b: reads inside the tx are gated here (and at
    /// `tx_begin[_as]`) — a quarantined repo is refused with
    /// `BatchError::ResyncRequired` even when the tx was opened before the
    /// quarantine; commit-time semantics stay ungated.
    pub async fn tx_execute_as(
        &self,
        actor: Actor,
        db_name: &str,
        request: &BatchRequest,
        tx: &mut crate::engine::tx::TxContext,
    ) -> Result<BatchResponse, BatchError> {
        self.tx_execute_with_policy(actor, db_name, request, tx, ExecPolicy::EMBEDDED)
            .await
    }

    /// Execute in an open transaction with a server-owned resource policy.
    pub async fn tx_execute_with_policy(
        &self,
        actor: Actor,
        db_name: &str,
        request: &BatchRequest,
        tx: &mut crate::engine::tx::TxContext,
        policy: ExecPolicy,
    ) -> Result<BatchResponse, BatchError> {
        self.tx_execute_with_policy_and_budget(actor, db_name, request, tx, policy, None)
            .await
    }

    /// Execute with an exact caller-owned cap, without changing request limits.
    #[allow(clippy::too_many_arguments)]
    pub async fn tx_execute_with_policy_and_budget(
        &self,
        actor: Actor,
        db_name: &str,
        request: &BatchRequest,
        tx: &mut crate::engine::tx::TxContext,
        policy: ExecPolicy,
        deadline_cap: Option<Duration>,
    ) -> Result<BatchResponse, BatchError> {
        self.tx_execute_with_policy_and_budget_and_repo_observer(
            actor,
            db_name,
            request,
            tx,
            policy,
            deadline_cap,
            None,
        )
        .await
    }

    /// Execute in an open transaction with a per-execution admin observer.
    #[allow(clippy::too_many_arguments)]
    pub async fn tx_execute_with_policy_and_budget_and_repo_observer(
        &self,
        actor: Actor,
        db_name: &str,
        request: &BatchRequest,
        tx: &mut crate::engine::tx::TxContext,
        policy: ExecPolicy,
        deadline_cap: Option<Duration>,
        repo_admin_observer: Option<Arc<dyn crate::RepoAdminObserver>>,
    ) -> Result<BatchResponse, BatchError> {
        // Authorization must consume, not renew, the caller's remaining budget.
        let started = Instant::now();
        let mut auth = self
            .authorize_batch(request, actor, db_name)
            .await?
            .with_policy(policy);
        if let Some(cap) = deadline_cap {
            auth = auth.with_deadline_cap(cap.saturating_sub(started.elapsed()));
        }

        // S5c-X PR8b: ordinary READS of a quarantined repo fail closed, even
        // for a tx opened BEFORE the quarantine. Same shape as the batch gate
        // in `repo_admin/db_execute.rs`. The test-only skip seam and the
        // `is_empty` fast path live INSIDE the shared sync helper, so only
        // this one-line call sits in the async state machine.
        // Authorization comes FIRST so unauthorized callers get
        // `access_denied` and never learn the repo's quarantine state.
        if let Some(err) = tx_read_quarantine_refusal_for_request(self, db_name, request) {
            return Err(err);
        }
        // R1c §4.3: same maintenance-fence gate as the non-tx batch entry
        // (`repo_admin/db_execute.rs`) — writes, DDL and stored-procedure
        // calls are refused while an application-schema migration holds the
        // database fence; reads pass. Same one-line sync-call shape.
        if let Some(err) =
            crate::shamir_db::execute::admin_db_schema_migration::maintenance_fence_refusal(
                self, db_name, request,
            )
        {
            return Err(err);
        }
        let db = self.get_db(db_name).ok_or_else(|| BatchError::QueryError {
            alias: String::new(),
            message: format!("Database '{}' not found", db_name),
            code: None,
        })?;
        let resolver = DbTableResolver {
            db,
            validators: self.validators().clone(),
        };
        let admin = ShamirAdminExecutor {
            shamir: self.clone(),
            db_name: db_name.to_string(),
            actor: auth.actor().clone(),
            repo_admin_observer,
        };
        let invoker = ShamirFunctionInvoker {
            shamir: self.clone(),
            db_name: db_name.to_string(),
        };
        execute_in_open_tx(auth, &resolver, Some(&admin), Some(&invoker), tx).await
    }

    /// COMMIT: run the Phase-A commit pipeline on a parked interactive tx and
    /// map the outcome to a wire [`TransactionInfo`] — `committed` (with the
    /// inherited `materialized` flag) on success, `aborted` with a reason on
    /// a commit-time conflict/violation. Mirrors the mapping the single-batch
    /// `execute_transactional` performs.
    ///
    /// Not gated by the read-quarantine (S5c-X PR8b): reads inside a tx are
    /// gated at `tx_begin`/`tx_execute` instead; commit/write semantics are
    /// out of read scope.
    pub async fn tx_commit(
        &self,
        db_name: &str,
        repo_name: &str,
        tx: crate::engine::tx::TxContext,
    ) -> Result<TransactionInfo, BatchError> {
        self.tx_commit_as(Actor::System, db_name, repo_name, tx)
            .await
    }

    /// COMMIT with an explicit [`Actor`].
    pub async fn tx_commit_as(
        &self,
        actor: Actor,
        db_name: &str,
        repo_name: &str,
        tx: crate::engine::tx::TxContext,
    ) -> Result<TransactionInfo, BatchError> {
        self.tx_commit_resolved(actor, db_name, repo_name, tx, None)
            .await
    }

    /// COMMIT with an explicit post-known-outcome visibility budget.
    pub async fn tx_commit_with_options(
        &self,
        db_name: &str,
        repo_name: &str,
        tx: crate::engine::tx::TxContext,
        wait: VisibilityWait,
    ) -> Result<TransactionInfo, BatchError> {
        self.tx_commit_as_with_options(Actor::System, db_name, repo_name, tx, wait)
            .await
    }

    /// COMMIT with an explicit actor and visibility policy.
    pub async fn tx_commit_as_with_options(
        &self,
        actor: Actor,
        db_name: &str,
        repo_name: &str,
        tx: crate::engine::tx::TxContext,
        wait: VisibilityWait,
    ) -> Result<TransactionInfo, BatchError> {
        self.tx_commit_resolved(actor, db_name, repo_name, tx, Some(wait))
            .await
    }

    async fn tx_commit_resolved(
        &self,
        actor: Actor,
        db_name: &str,
        repo_name: &str,
        tx: crate::engine::tx::TxContext,
        wait: Option<VisibilityWait>,
    ) -> Result<TransactionInfo, BatchError> {
        // Resolve the store BEFORE the authorization gate: the caller has
        // already taken the context out of the parked transaction, so an early
        // return must release this transaction's Level-3 record locks itself —
        // `InteractiveTx::drop` cannot (its context is gone). A database or
        // repository that no longer resolves has no reachable store left, so
        // there is nothing to release there.
        let db = self.get_db(db_name).ok_or_else(|| BatchError::QueryError {
            alias: String::new(),
            message: format!("Database '{}' not found", db_name),
            code: None,
        })?;
        let repo = db
            .get_repo(repo_name)
            .ok_or_else(|| BatchError::QueryError {
                alias: String::new(),
                message: format!("Repository '{}' not found", repo_name),
                code: None,
            })?;
        // `TxBegin` authorizes Read on the database node, `TxCommit` Write: a
        // session whose database ACL narrowed while the transaction was parked
        // lands here with staged record locks still held. Release them the way
        // the pipeline's own abort arms do (`acknowledgement/tail.rs`).
        if let Err(e) = self
            .authorize_access(
                &actor,
                &ResourcePath::Database {
                    db: db_name.to_string(),
                },
                Action::Write,
            )
            .await
        {
            crate::engine::tx::release_interactive_tx_locks(&tx, repo.core()).await;
            return Err(BatchError::query_coded("", "access_denied", e.to_string()));
        }
        let tx_id = tx.tx_id.0;
        let snapshot_version = tx.snapshot_version;
        let wait = wait.unwrap_or_else(|| repo.commit_visibility_wait());
        commit_on_repo(&repo, tx, tx_id, snapshot_version, wait).await
    }

    /// Commits a context taken out of a parked transaction. The repository is resolved BY NAME,
    /// exactly like the unbound facade: the caller authorized Database:Write for THIS name and the
    /// commit event carries this name. When `bound` (the repository resolved when the transaction
    /// was parked) is given, the name must still resolve to that SAME repository: a name that no
    /// longer resolves, or that now resolves to a different repository, is refused with a typed
    /// error and the record locks are released through `release`, which is bound to the parked
    /// repository (so a rename or a re-creation cannot strand them nor divert the commit).
    /// Authorization is deliberately not taken here so the parked-commit caller can hold it under
    /// its cleanup guard, keeping the release alive across cancellation.
    ///
    /// Known limit: after `disarm()` hands the context to the engine, cleanup is the pipeline's
    /// responsibility. Before `CommitDriver::new` (`entry.rs:756`), the pipeline awaits at
    /// `entry.rs:685-756`; dropped requests in those windows may strand Level-3 locks. See task #2020.
    pub async fn tx_commit_parked_as(
        &self,
        db_name: &str,
        repo_name: &str,
        bound: Option<&crate::engine::repo::RepoInstance>,
        tx: crate::engine::tx::TxContext,
        release: ParkedTxRelease,
        wait: Option<VisibilityWait>,
    ) -> Result<TransactionInfo, BatchError> {
        let Some(db) = self.get_db(db_name) else {
            release_parked_tx(tx, &release).await;
            return Err(BatchError::QueryError {
                alias: String::new(),
                message: format!("Database '{}' not found", db_name),
                code: None,
            });
        };
        let Some(repo) = db.get_repo(repo_name) else {
            release_parked_tx(tx, &release).await;
            return Err(BatchError::QueryError {
                alias: String::new(),
                message: format!("Repository '{}' not found", repo_name),
                code: None,
            });
        };
        if let Some(bound) = bound {
            let same = match (bound.tx_gate().await, repo.tx_gate().await) {
                (Ok(parked), Ok(current)) => std::sync::Arc::ptr_eq(&parked, &current),
                _ => false,
            };
            if !same {
                release_parked_tx(tx, &release).await;
                return Err(BatchError::QueryError {
                    alias: String::new(),
                    message: format!(
                        "Repository '{}' changed since the transaction began",
                        repo_name
                    ),
                    code: None,
                });
            }
        }
        let tx_id = tx.tx_id.0;
        let snapshot_version = tx.snapshot_version;
        let wait = wait.unwrap_or_else(|| repo.commit_visibility_wait());
        commit_on_repo(&repo, tx, tx_id, snapshot_version, wait).await
    }
}

/// The release hook of a parked interactive transaction, bound to the repository resolved at
/// parking time (the server's `TxAbortRelease` adapts directly). Invoked once when parked commit
/// ends outside the pipeline.
pub type ParkedTxRelease = std::sync::Arc<
    dyn Fn(crate::engine::tx::TxContext) -> futures::future::BoxFuture<'static, ()> + Send + Sync,
>;

async fn release_parked_tx(tx: crate::engine::tx::TxContext, release: &ParkedTxRelease) {
    if tx.locked_keys.is_empty() {
        return;
    }
    match tokio::spawn((release)(tx)).await {
        Ok(()) => (),
        Err(error) if error.is_panic() => log::error!("parked transaction lock release panicked"),
        Err(error) => log::warn!("parked transaction lock release task failed: {error}"),
    }
}

async fn commit_on_repo(
    repo: &crate::engine::repo::RepoInstance,
    tx: crate::engine::tx::TxContext,
    tx_id: u64,
    snapshot_version: u64,
    wait: VisibilityWait,
) -> Result<TransactionInfo, BatchError> {
    match repo.commit_tx_with_options(tx, wait).await {
        Ok(outcome) => Ok(TransactionInfo::committed(
            outcome.tx_id,
            outcome.snapshot_version,
            outcome.commit_version,
            outcome.materialized(),
        )
        .with_visibility(outcome.visibility)),
        Err(commit_err) => Ok(commit_err.into_transaction_info(tx_id, snapshot_version)),
    }
}

//! CancelRequest looks up ownership without taking a registry writer lock.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use shamir_connect::server::session::Session;
use shamir_query_types::admin::activity::{CancelOutcome, RequestExecId};
use shamir_query_types::wire::{CursorId, DbResponse};
use shamir_sync::durability::tx_cancel::TxParkedState;
use shamir_tx::{CancelState, TxContext};
use shamir_types::access::{Action, ResourcePath};

use super::requests::RequestEntry;

#[cfg(test)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::engine::db_handler) enum CancelPauseTarget {
    Transaction,
    Cursor,
}

impl ShamirDbHandler {
    #[cfg(test)]
    pub(in crate::engine::db_handler) fn with_cancel_authorized_pause(
        mut self,
        target: CancelPauseTarget,
        id: u64,
        reached: Arc<tokio::sync::Barrier>,
        resume: Arc<tokio::sync::Barrier>,
    ) -> Self {
        self.cancel_authorized_pause = Some((target, id, reached, resume));
        self
    }

    #[cfg(test)]
    pub(in crate::engine::db_handler) async fn pause_cancel_after_authorization(
        &self,
        target: CancelPauseTarget,
        id: u64,
    ) {
        if let Some((configured_target, configured_id, reached, resume)) =
            &self.cancel_authorized_pause
        {
            if *configured_target == target && *configured_id == id {
                reached.wait().await;
                resume.wait().await;
            }
        }
    }

    /// Test-only: install the CONSUMED-ONCE pause that parks the inline
    /// record-lock release (tail admission already closed) just after the
    /// drop-guard exists but BEFORE the release future's first poll, so a
    /// test can drop the cancel dispatch mid-guard and observe the guard's
    /// detached fallback. After the rendezvous the dispatch stays parked until
    /// the test aborts it. Shared across handler clones (`Arc`), per
    /// instance; `take`n on the one firing.
    #[cfg(test)]
    pub(in crate::engine::db_handler) fn with_cancel_release_pause(
        self,
        tx_handle: u64,
        reached: Arc<tokio::sync::Barrier>,
    ) -> Self {
        *self
            .cancel_counters
            .release_pause
            .lock()
            .expect("release pause mutex poisoned") = Some((tx_handle, reached));
        self
    }

    /// The pause itself: fires at most ONCE, for the configured handle only.
    #[cfg(test)]
    pub(in crate::engine::db_handler) async fn pause_inline_release_once(&self, tx_handle: u64) {
        // Take the barrier OUT of the guard and drop the guard BEFORE the
        // await: a `std::sync::MutexGuard` must never be held across `.await`.
        let reached = {
            let mut guard = self
                .cancel_counters
                .release_pause
                .lock()
                .expect("release pause mutex poisoned");
            guard.take().and_then(|(configured_handle, reached)| {
                (configured_handle == tx_handle).then_some(reached)
            })
        };
        if let Some(reached) = reached {
            reached.wait().await;
            // Stay parked after the rendezvous: the test aborts this dispatch
            // while the guard is alive and the release is not yet polled.
            std::future::pending::<()>().await;
        }
    }
}

use crate::engine::db_handler::handler::{session_actor, ShamirDbHandler};
use crate::engine::db_handler::repo_audit::{
    enqueue_cancel_refusal_audit, enqueue_operator_cancel_audit,
};
use crate::tx_registry::InteractiveTx;

pub(super) fn cancel_entry(entry: &RequestEntry) -> CancelOutcome {
    if !entry.cancellable {
        return match entry.cancel.state() {
            CancelState::Done | CancelState::Cancelled => CancelOutcome::AlreadyFinished,
            CancelState::Running | CancelState::Committing => CancelOutcome::NotCancellable,
        };
    }
    match entry.cancel.try_cancel() {
        Ok(()) => CancelOutcome::Cancelled,
        Err(CancelState::Committing) => CancelOutcome::AlreadyCommitting,
        Err(CancelState::Done | CancelState::Cancelled) => CancelOutcome::AlreadyFinished,
        Err(CancelState::Running) => unreachable!("running state cannot refuse cancellation"),
    }
}

/// Per-server cancellation outcome counters, one fixed slot per
/// `CancelOutcome`. Index order is shared with
/// `CANCEL_RESULT_LABELS` in the activity export
/// (`shamir_cancellations_total{result=...}`): 0 `cancelled`,
/// 1 `already_finished`, 2 `already_committing`, 3 `not_cancellable`.
pub(in crate::engine::db_handler) struct CancelCounters {
    results: [AtomicU64; 4],
    /// Test-only CONSUMED-ONCE pause: parks the inline record-lock release
    /// (`release_tx_locks`, tail admission already closed) just after its
    /// drop-guard exists but before the release future's first poll, so a
    /// test can drop the cancel dispatch mid-guard and observe the guard's
    /// detached fallback. Keyed by tx handle; shared across all handler
    /// clones through this `Arc` (per instance).
    #[cfg(test)]
    release_pause: std::sync::Mutex<Option<(u64, Arc<tokio::sync::Barrier>)>>,
}

impl CancelCounters {
    pub(in crate::engine::db_handler) fn new() -> Self {
        Self {
            results: std::array::from_fn(|_| AtomicU64::new(0)),
            #[cfg(test)]
            release_pause: std::sync::Mutex::new(None),
        }
    }

    pub(in crate::engine::db_handler) fn record(&self, outcome: CancelOutcome) {
        let index = match outcome {
            CancelOutcome::Cancelled => 0,
            CancelOutcome::AlreadyFinished => 1,
            CancelOutcome::AlreadyCommitting => 2,
            CancelOutcome::NotCancellable => 3,
        };
        self.results[index].fetch_add(1, Ordering::Relaxed);
    }

    pub(in crate::engine::db_handler) fn snapshot(&self) -> [u64; 4] {
        std::array::from_fn(|index| self.results[index].load(Ordering::Relaxed))
    }
}

impl ShamirDbHandler {
    /// Called on an authenticated control connection. Looking up the row
    /// reveals ownership internally, but no outcome is read or returned
    /// for another user's row before the ActivityView permission check.
    pub(in crate::engine::db_handler) async fn cancel_request(
        &self,
        session: &Session,
        target: RequestExecId,
    ) -> DbResponse {
        let Some(entry) = self.activity_requests.get(target.0) else {
            return self.cancel_reply(CancelOutcome::AlreadyFinished);
        };
        if entry.user_id != session.user_id
            && self
                .db
                .authorize_access(
                    &session_actor(session),
                    &ResourcePath::ActivityView,
                    Action::Manage,
                )
                .await
                .is_err()
        {
            return DbResponse::Error {
                code: "permission_denied".to_owned(),
                message:
                    "cancelling another user's request requires ActivityView manage permission"
                        .to_owned(),
            };
        }

        let outcome = cancel_entry(&entry);
        // Lookup has released its epoch guard. The state decision above does
        // not depend on audit I/O.
        //
        // The reply is built AFTER this synchronous enqueue, so its real cost
        // is the enqueue's cost: one per-principal counter update, one
        // `spawn_blocking` and one short push+prune of the queue's handle list.
        // None of that waits for the audit writer, its persistence gate or a
        // flush — a refusal that cannot be admitted is dropped and counted
        // instead of blocking the reply.
        if let Some(audit) = &self.audit {
            // The job is detached: the append runs on a blocking thread and
            // this reply never awaits it.
            drop(enqueue_cancel_refusal_audit(
                &self.cancel_refusal_audit,
                audit,
                session,
                target,
                &outcome,
            ));
        }
        self.cancel_reply(outcome)
    }

    /// The single construction point for a cancel reply, so every produced
    /// outcome is counted exactly once — no cancel path may build
    /// `DbResponse::CancelOutcome` directly.
    fn cancel_reply(&self, outcome: CancelOutcome) -> DbResponse {
        self.cancel_counters.record(outcome);
        DbResponse::CancelOutcome { outcome }
    }

    async fn may_manage_other_users_work(&self, session: &Session) -> bool {
        self.db
            .authorize_access(
                &session_actor(session),
                &ResourcePath::ActivityView,
                Action::Manage,
            )
            .await
            .is_ok()
    }

    /// `target` is only ever a fixed literal (`"transaction"` or `"cursor"`),
    /// so the message stays free of caller-supplied text.
    fn cancel_permission_denied(target: &'static str) -> DbResponse {
        DbResponse::Error {
            code: "permission_denied".to_owned(),
            message: format!(
                "cancelling another user's {target} requires ActivityView manage permission"
            ),
        }
    }

    /// Best-effort, detached audit for an operator-driven cancel: appends for
    /// every refusal outcome AND for every successful cross-user cancel —
    /// the same-user non-refusal case is the client's own normal path and is
    /// deliberately not audited.
    fn audit_operator_cancel(
        &self,
        session: &Session,
        operation: &'static str,
        target: u64,
        target_user_id: [u8; 16],
        outcome: &CancelOutcome,
    ) {
        let Some(audit) = &self.audit else { return };
        // Audited: both refusals, and a SUCCESSFUL cancel of another user's
        // work. `already_finished` is neither (nothing was refused or ended).
        let audited = match outcome {
            CancelOutcome::AlreadyCommitting | CancelOutcome::NotCancellable => true,
            CancelOutcome::Cancelled => target_user_id != session.user_id,
            CancelOutcome::AlreadyFinished => false,
        };
        if !audited {
            return;
        }
        // Detach by design: the reply must not wait for the writer.
        drop(enqueue_operator_cancel_audit(
            &self.cancel_refusal_audit,
            audit,
            session,
            operation,
            target,
            outcome,
        ));
    }

    /// Cancel an OPEN interactive transaction named by the activity-view
    /// handle. Own user (any of that user's sessions) needs authentication
    /// only; another user's transaction needs `ActivityView` + `Manage`. An
    /// unknown handle answers `already_finished` before the permission check.
    ///
    /// The decision is made ONCE, against the authoritative `open` map: the
    /// owner comes from the parked transaction, and the removal re-checks the
    /// handle's identity (`Arc::ptr_eq`) under the `open` entry's own shard
    /// lock, so a handle re-minted for ANOTHER transaction between the
    /// authorization and the removal answers `already_finished` and leaves
    /// the replacement alone. That shard lock is the only lock this cancel
    /// takes — no commit gate, no commit mutex — and no `.await` runs while
    /// it is held.
    ///
    /// A transaction with a request in flight is refused with
    /// `not_cancellable` (its context lock is only ever attempted, never
    /// awaited); a momentary hold by the commit path answers
    /// `already_committing` instead, since that handle is already reserved.
    /// An idle one has its Level-3 record locks released by a task the cancel
    /// call awaits (the locks are gone before the reply; the release survives
    /// the abort of the call, and a failed release is only logged while the
    /// call still awaits it), since dropping
    /// the `Arc` frees none of them.
    pub(in crate::engine::db_handler) async fn cancel_tx(
        &self,
        session: &Session,
        tx_handle: u64,
    ) -> DbResponse {
        let (target_user_id, target, committing_row) = match self.tx_registry.peek(tx_handle) {
            TxParkedState::Open(tx) => (*tx.owner_user_id(), Some(tx), None),
            TxParkedState::Committing(tx) => (*tx.owner_user_id(), None, Some(tx)),
            TxParkedState::Gone => return self.cancel_reply(CancelOutcome::AlreadyFinished),
        };
        if target_user_id != session.user_id && !self.may_manage_other_users_work(session).await {
            return Self::cancel_permission_denied("transaction");
        }
        if target_user_id != session.user_id {
            #[cfg(test)]
            self.pause_cancel_after_authorization(CancelPauseTarget::Transaction, tx_handle)
                .await;
        }
        let outcome = match (target, committing_row) {
            (Some(tx), _) => self.cancel_parked_tx(tx_handle, tx).await,
            (None, Some(tx)) => match self.tx_registry.peek(tx_handle) {
                TxParkedState::Committing(current) if Arc::ptr_eq(&tx, &current) => {
                    CancelOutcome::AlreadyCommitting
                }
                _ => CancelOutcome::AlreadyFinished,
            },
            (None, None) => CancelOutcome::AlreadyFinished,
        };
        self.audit_operator_cancel(session, "cancel_tx", tx_handle, target_user_id, &outcome);
        self.cancel_reply(outcome)
    }

    /// Remove `tx` (identity-checked) and release what it holds. The context
    /// lock is taken with a NON-BLOCKING attempt: a `TxExecute` holds it for
    /// the whole plan and the cancel never waits for that request.
    async fn cancel_parked_tx(&self, tx_handle: u64, tx: Arc<InteractiveTx>) -> CancelOutcome {
        let Ok(mut ctx) = tx.ctx().try_lock() else {
            // The lock is held by the owner's request (a `TxExecute` holds it
            // for the whole plan), or momentarily by the commit path reading
            // the context. Only the first case is a request the operator must
            // leave alone; a reserved handle is already committing. This is a
            // refusal-only re-read: no removal decision depends on it. A
            // transaction that is gone, or replaced by another one, is not a
            // request to leave alone: it answers `already_finished`.
            return match self.tx_registry.peek(tx_handle) {
                TxParkedState::Committing(current) if Arc::ptr_eq(&current, &tx) => {
                    CancelOutcome::AlreadyCommitting
                }
                TxParkedState::Committing(_) => CancelOutcome::AlreadyFinished,
                TxParkedState::Open(current) if Arc::ptr_eq(&current, &tx) => {
                    CancelOutcome::NotCancellable
                }
                TxParkedState::Open(_) | TxParkedState::Gone => CancelOutcome::AlreadyFinished,
            };
        };
        match self.tx_registry.remove_identity(tx_handle, &tx) {
            TxParkedState::Open(tx) => match ctx.take() {
                Some(parked) => {
                    drop(ctx);
                    self.release_tx_locks(tx_handle, parked, &tx).await;
                    CancelOutcome::Cancelled
                }
                None => CancelOutcome::AlreadyFinished,
            },
            TxParkedState::Committing(current) if Arc::ptr_eq(&current, &tx) => {
                CancelOutcome::AlreadyCommitting
            }
            TxParkedState::Committing(_) => CancelOutcome::AlreadyFinished,
            TxParkedState::Gone => CancelOutcome::AlreadyFinished,
        }
    }

    /// The operator-cancel path's own release of the Level-3 record locks the
    /// cancelled context still holds. The release runs as a release tail
    /// registered with the transaction registry's owner and this call AWAITS
    /// its completion, so on the ordinary path the locks are gone before the
    /// cancel reply returns; a failed or cancelled release task is logged
    /// while this call still awaits it (a repeated release of the same
    /// transaction is a harmless no-op because `MvccStore::release_locks`
    /// removes only holders whose `tx_version` matches).
    ///
    /// GUARANTEE (exact). Tracked ends: the awaited tail join on the
    /// ordinary path, and the inline await below on the closed-admission
    /// path. Detached-on-drop ends: (a) the guard's fallback — if THIS
    /// dispatch future is dropped while the inline await is pending, the
    /// drop-guard spawns the not-yet-polled release DETACHED (with a
    /// `warn!`; a `Drop` cannot await), the same recorded trade-off as the
    /// registry `Drop` path in `tx_abort_release.rs`; (b) the registry
    /// `Drop` release (here it cannot retry: the context was `take()`n out
    /// of the transaction, so `InteractiveTx::drop` sees `None`); (c) the
    /// `tails == None` branch, detached by design (a registry built without
    /// an owner). What can still be lost: a fallback or registry-`Drop`
    /// release that runs when NO tokio runtime is alive (its spawn fails
    /// after a `warn!`) — the locks then go with the process — and a
    /// detached tail (a)/(c) can overlap the shutdown flush, since nothing
    /// tracks it.
    async fn release_tx_locks(&self, tx_handle: u64, parked: TxContext, tx: &InteractiveTx) {
        let release = tx.abort_release();
        // The cancel dispatch can be dropped with its caller. Keep the lock
        // release alive independently; the handoff is atomic with shutdown
        // admission (`spawn_lossless` registers under the same lock as the
        // close latch and hands the future BACK on refusal) — a detached task
        // here would let a shutdown flush and close the stores while the
        // release is still writing.
        let release = Arc::clone(release);
        let tails = self.tx_registry.release_tails();
        let work = async move { release(parked).await };
        match tails {
            Some(tails) => match tails.spawn_lossless(work) {
                Ok(join) => {
                    if let Err(error) = join.await {
                        if !error.is_cancelled() {
                            tracing::warn!(tx_handle, %error, "cancel release task failed");
                        } else {
                            tracing::warn!(
                                tx_handle,
                                "cancel release task was cancelled before it finished; the \
                                 transaction context went with it, so the registry's Drop \
                                 cannot retry the record-lock release"
                            );
                        }
                    }
                }
                // Tail admission closed by shutdown: run the release inline
                // under this call rather than dropping it (a dropped release
                // leaves the record locks held until the process exits). The
                // guard completes the release even if THIS dispatch future
                // is dropped mid-await: a `Drop` cannot await, so the only
                // means left is a detached spawn (see the GUARANTEE above).
                Err(work) => {
                    let mut guard = InlineReleaseGuard {
                        work: Some(Box::pin(work)),
                        tx_handle,
                    };
                    #[cfg(test)]
                    self.pause_inline_release_once(tx_handle).await;
                    guard
                        .work
                        .as_mut()
                        .expect("the guard holds the inline release until it completes")
                        .as_mut()
                        .await;
                    guard.work = None;
                }
            },
            None => {
                // No registry owner (a registry built without one): keep the
                // historical detached behaviour rather than losing the lock
                // release entirely.
                tracing::warn!(
                    tx_handle,
                    "release tail untracked; the lock release runs detached"
                );
                tokio::spawn(work);
            }
        }
    }

    /// Cancel another session's result cursor by its id. Ownership comes from
    /// the cursor registry's activity mirror; `not_cancellable` NEVER applies
    /// here — the only outcomes are `cancelled` and `already_finished`. An
    /// unknown cursor, or one the registry holds without activity metadata
    /// (never produced by the wire `CreateCursor` path) and therefore cannot
    /// be attributed to a user, answers `already_finished` before any
    /// permission check.
    ///
    /// The snapshot scan is an operator-driven O(N) read (no lock any
    /// lifecycle writer takes); N is bounded by the per-session cursor caps.
    pub(in crate::engine::db_handler) async fn cancel_cursor_admin(
        &self,
        session: &Session,
        cursor_id: CursorId,
    ) -> DbResponse {
        let Some(row) = self
            .cursor_registry
            .activity_snapshot()
            .into_iter()
            .find(|row| row.cursor_id == cursor_id.0)
        else {
            return self.cancel_reply(CancelOutcome::AlreadyFinished);
        };
        let target_user_id = row.meta.user_id;
        if target_user_id != session.user_id && !self.may_manage_other_users_work(session).await {
            return Self::cancel_permission_denied("cursor");
        }
        if target_user_id != session.user_id {
            #[cfg(test)]
            self.pause_cancel_after_authorization(CancelPauseTarget::Cursor, cursor_id.0)
                .await;
        }
        // `remove` frees the session slot and detaches the pinned snapshot;
        // dropping the returned `Arc` is what releases that snapshot.
        let outcome = match self
            .cursor_registry
            .remove_activity_identity(cursor_id.0, &row.meta)
        {
            Some(cursor) => {
                drop(cursor);
                CancelOutcome::Cancelled
            }
            None => CancelOutcome::AlreadyFinished,
        };
        self.audit_operator_cancel(
            session,
            "cancel_cursor_admin",
            cursor_id.0,
            target_user_id,
            &outcome,
        );
        self.cancel_reply(outcome)
    }
}

/// Completes an inline record-lock release even when the cancel dispatch
/// that owns it is dropped mid-await. The guard keeps the release future
/// pinned INSIDE itself while it is awaited; if the dispatch is dropped
/// before completion, the guard's `Drop` takes the future back out and
/// spawns it DETACHED — a `Drop` cannot await, and dropping the future
/// would drop the taken `TxContext` with it, losing the record locks for
/// the process's whole remaining lifetime. If no runtime is alive at that
/// moment, the spawn is impossible and the loss is logged (`warn!`), never
/// panicking (same terminal case as `spawn_abort_release`). A completed
/// release clears the slot so the `Drop` does nothing.
struct InlineReleaseGuard<F: Future<Output = ()> + Send + 'static> {
    work: Option<Pin<Box<F>>>,
    tx_handle: u64,
}

impl<F: Future<Output = ()> + Send + 'static> Drop for InlineReleaseGuard<F> {
    fn drop(&mut self) {
        let Some(work) = self.work.take() else {
            return;
        };
        match tokio::runtime::Handle::try_current() {
            Ok(runtime) => {
                tracing::warn!(
                    self.tx_handle,
                    "cancel dispatch dropped mid-release; the record-lock release \
                     continues detached (untracked)"
                );
                runtime.spawn(work);
            }
            Err(_) => tracing::warn!(
                self.tx_handle,
                "cancel dispatch dropped outside a tokio runtime: its record locks \
                 go with the process"
            ),
        }
    }
}

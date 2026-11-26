//! `RequestHandler` bridge — exposes the **full ShamirDb Batch API** over the
//! authenticated wire.
//!
//! After SCRAM auth, every request comes in as `RequestEnvelope { session_id,
//! request_id, req: Vec<u8> }`. The transport calls
//! `dispatch_request_view(view, store, lookup_tickets_invalid, handler)`,
//! which runs the §7.5 validity check and then invokes
//! [`RequestHandler::handle`]. This module implements that trait against a
//! shared [`ShamirDb`] instance and routes the payload straight into
//! [`ShamirDb::execute`] — i.e. into the canonical, fully-featured query
//! entry point of the database.
//!
//! # Wire schema (msgpack, `rmp_serde::to_vec_named` / `from_slice`)
//!
//! - [`DbRequest::Ping`] — health check (zero DB cost; useful for keepalive).
//! - [`DbRequest::Execute { db, batch }`] — wraps a complete
//!   [`BatchRequest`] (see `shamir_db::query::batch`). The batch is the
//!   single point of entry for every database operation:
//!     - reads (with WHERE / SELECT projections+aggregations / GROUP BY /
//!       ORDER BY / pagination / `count_total`),
//!     - writes (Insert / Update / Set (upsert) / Delete),
//!     - admin DDL (CreateDb / DropDb / CreateRepo / DropRepo / CreateTable /
//!       DropTable / CreateIndex / DropIndex / List),
//!     - auth ops (CreateUser / DropUser / GrantRole / RevokeRole),
//!     - cross-query references via `{"$query": "@alias[].field"}`,
//!     - optional MVCC transactional semantics.
//!
//! [`DbResponse::Batch`] returns the **full** [`BatchResponse`] —
//! per-alias [`QueryResult`](shamir_db::query::read::QueryResult) with
//! `records: Vec<Value>`, [`QueryStats`](shamir_db::query::read::QueryStats),
//! [`PaginationInfo`](shamir_db::query::read::PaginationInfo), the
//! execution plan stages, total execution time, and transaction info. No
//! information is dropped or summarised by the bridge.
//!
//! # Permission gate (v1)
//!
//! The session-layer permission snapshot
//! ([`SessionPermissions`](shamir_connect::server::session::SessionPermissions))
//! currently tracks only `is_superuser` + `roles: Vec<String>`. This bridge
//! enforces a single coarse rule: **any [`BatchOp`] for which
//! [`BatchOp::is_admin`] returns true requires `is_superuser`**. Read/write
//! ops on data tables are accepted from any authenticated session.
//!
//! Fine-grained per-table RBAC (mapping role names → DB-side
//! [`SessionPermissions`](shamir_db::query::auth::SessionPermissions) +
//! [`execute_batch_with_permissions`]) is a follow-up item — the wire
//! schema does not need to change for it.
//!
//! # Error semantics
//!
//! `RequestHandler::handle` returns `Err(reason)` only for **protocol-level**
//! failures (msgpack decode, response encode). DB-layer failures
//! (admin denied, planner errors, query errors, lock timeouts) are returned
//! inside `Ok(bytes)` carrying a [`DbResponse::Error`] payload with a
//! coarse `kind` tag for clients to switch on without parsing prose.
//!
//! # Async model
//!
//! `RequestHandler::handle` is now async (returns a boxed future). The
//! `connection::request_loop` awaits it directly — no blocking-pool bridge
//! (`spawn_blocking` / `run_blocking`) is needed on the dispatch path.
//! Tokio worker threads are never parked; every `.await` inside the handler
//! yields the worker back to the scheduler. CPU-bound work (Argon2id key
//! derivation) is delegated to `tokio::task::spawn_blocking` so the worker
//! remains free during derivation.

use std::sync::Arc;
use std::time::Duration;

use super::mutation_tracker::MutationTasks;
use shamir_connect::server::audit_chain::AuditChainWriter;
use shamir_connect::server::conn_services::ConnectionServices;
use shamir_connect::server::dispatch::RequestHandler;
use shamir_connect::server::session::{Session, SessionStore};

use shamir_db::access::Actor;
use shamir_db::engine::db_instance::logical_export::MaintenanceGate;
use shamir_db::engine::table::writer_drain_barrier::WriterDrainGuard;
use shamir_db::query::batch::BatchError;
use shamir_db::ShamirDb;
use shamir_query_types::hmac as canon;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::boot::config::diagnostics::logical::export_online::OnlineExport;
use crate::byte_budget::ByteBudget;
use crate::security::tokens::TokenService;

// Wire DTOs are defined once in `shamir-query-types::wire` so that the
// SDK (`shamir-client`) and the server share the same definition. Re-
// exported here so existing `shamir_server::db_handler::DbRequest` import
// paths keep resolving.
pub use shamir_query_types::wire::{DbRequest, DbResponse};

use crate::cursor_registry::CursorRegistry;
use crate::tx_registry::TxRegistry;

use super::super::activity::ActivityRequests;
use super::super::activity::CancelCounters;
use super::super::admin::{
    change_password_challenge, change_password_verify, create_scram_user, set_replicator,
    set_superuser, AdminGlue,
};
use super::super::config::{
    CursorLimitsCap, NodeMode, QueryLimitsCap, SlowQueryConfig, TxLimitsCap,
};
use super::super::cursor_handlers::error_response;
use super::super::cursor_support::{authorize_cursor_read, resolve_repo};
use super::super::handlers::token_admin;
use super::super::operator_state::table_unavailable_response;
use super::super::repo_audit::CancelRefusalAudit;
use super::OperatorBootContext;

/// Absolute lifetime cap for an interactive (Phase B) transaction — bounds
/// how long any one open tx can pin MVCC GC, even if a client keeps it busy.
/// Mirrors the engine's `DEFAULT_MAX_TX_LIFETIME` (5 min); the commit path's
/// own `is_expired` check is the final backstop.
pub(in crate::engine::db_handler) const INTERACTIVE_TX_MAX_LIFETIME: Duration =
    Duration::from_secs(300);

// --------------------------------------------------------------------------
// Wire schema
// --------------------------------------------------------------------------
//
// `DbRequest` / `DbResponse` are defined in `shamir-query-types::wire`
// (re-exported above). Server-side validation of `query_version`
// against the hardcoded `SUPPORTED_QUERY_LANG_VERSIONS` list happens
// inside `RequestHandler::handle` below.

// --------------------------------------------------------------------------
// Handler
// --------------------------------------------------------------------------

/// Resolve the [`Actor`] for the current session.
///
/// Superuser sessions get `Actor::Admin(principal64(session.user_id))` —
/// bypasses the Shomer gate exactly like `Actor::System`, but attributes
/// ownership of admin-created resources to the real account instead of
/// collapsing to `owner = 0`. Regular sessions get
/// `Actor::User(principal64(session.user_id))`. `user_id` is the directory-
/// minted 16-byte id stamped on the session at login — NOT derived from the
/// username — so a dropped-and-recreated account gets a fresh id even if it
/// reuses the same name (closes the identity-inheritance-on-recreate bug,
/// design doc §1.1 finding 3).
pub(in crate::engine::db_handler) fn session_actor(session: &Session) -> Actor {
    let id = shamir_types::access::principal64(session.user_id);
    if session.permissions.is_superuser {
        Actor::Admin(id)
    } else {
        Actor::User(id)
    }
}

/// Bridge handler — routes wire requests through `RequestHandler::handle`.
/// # Permissions (v1)
///
/// Admin / auth batch ops AND [`DbRequest::CreateScramUser`] require
/// `session.permissions.is_superuser`. All other ops are accepted from any
/// authenticated session. A future patch will plumb per-role table-level
/// RBAC by mapping into
/// `shamir_db::query::auth::SessionPermissions` and using
/// `execute_batch_with_permissions`.
#[derive(Clone)]
pub struct ShamirDbHandler {
    pub(in crate::engine::db_handler) db: Arc<ShamirDb>,
    /// Per-connection audit writer, attached by the request boundary.
    pub(in crate::engine::db_handler) audit: Option<Arc<AuditChainWriter>>,
    pub(in crate::engine::db_handler) cancel_refusal_audit: Arc<CancelRefusalAudit>,
    pub(in crate::engine::db_handler) transactions: crate::config::TransactionsConfig,
    /// `None` means the handler was constructed without admin support;
    /// `CreateScramUser` requests will return `not_supported`.
    pub(in crate::engine::db_handler) admin: Option<AdminGlue>,
    /// Slow-query log threshold. Default disabled for unit tests; the
    /// boot path sets this from the operator's config.
    pub(in crate::engine::db_handler) slow_query: SlowQueryConfig,
    /// Test-only seam: pins the exact `elapsed_us` the slow-query decision
    /// sees, so a WARN assertion never depends on how long the real read
    /// happened to take. `None` in production (a `#[cfg(...)]` field — no
    /// runtime cost, no production surface).
    #[cfg(any(test, feature = "test-util"))]
    pub(in crate::engine::db_handler) slow_query_elapsed_override: Option<u64>,
    /// Server-side hard caps on per-batch resources.
    pub(in crate::engine::db_handler) query_limits: QueryLimitsCap,
    /// Server-side hard cap on per-interactive-tx staged bytes.
    pub(in crate::engine::db_handler) tx_limits: TxLimitsCap,
    /// RI-15 — global cap on the SUM of in-flight response bytes across
    /// every concurrently-executing batch/connection. Default
    /// [`ByteBudget::unbounded`] (matches pre-RI-15 behavior: only the
    /// per-batch `query_limits.max_result_size_bytes` cap applies).
    pub(in crate::engine::db_handler) byte_budget: ByteBudget,
    /// Read/write mode of this node. `ReadOnly` rejects client writes
    /// (they must go to the leader). Default `ReadWrite`; until R1 this
    /// is always `ReadWrite` so behaviour is unchanged.
    pub(in crate::engine::db_handler) node_mode: NodeMode,
    /// Leader epoch for VR-style fencing (REPLICATION §5.2). Every
    /// `ReplResponse` carries this value so followers can detect a
    /// regression (stale leader). Default `1`; persistence + bump-on-
    /// promote land in R3.
    pub(in crate::engine::db_handler) leader_epoch: u64,
    /// Phase B — registry of open interactive (multi-call) transactions,
    /// shared across all clones of the handler (`Arc`), so a `TxExecute` on
    /// one dispatch finds the tx a prior `TxBegin` parked.
    pub(in crate::engine::db_handler) tx_registry: Arc<TxRegistry>,
    /// FG-5b — registry of open server-side result cursors, shared across
    /// all clones of the handler (`Arc`), so a `FetchNext`/`CancelCursor`
    /// on one dispatch finds the cursor a prior `CreateCursor` parked.
    pub(in crate::engine::db_handler) cursor_registry: Arc<CursorRegistry>,
    /// FG-5b — server-side hard caps on result cursors (per-session count,
    /// idle-timeout eviction).
    pub(in crate::engine::db_handler) cursor_limits: CursorLimitsCap,
    /// FG-5b — monotonic source of fresh `CursorId` values. A plain
    /// `AtomicU64` (not the tx registry's `RepoTxGate::fresh_tx_id`,
    /// which is per-repo and async) since cursor ids are a
    /// server-handler-scoped concept, not an engine one.
    pub(in crate::engine::db_handler) next_cursor_id: Arc<std::sync::atomic::AtomicU64>,
    /// `SessionStore` access for ops that must kill OTHER live sessions of
    /// the calling user (currently only `ChangePasswordVerify`'s spec
    /// §12.5.3 session-kill step). `None` means the handler was built
    /// without it — the credential update in `ChangePasswordVerify` still
    /// lands, but the session-kill half is skipped (documented partial-fix
    /// fallback; see `ShamirDbHandler::with_session_store`).
    ///
    /// `SessionStore` itself lives at the connection/server layer
    /// (`server_launcher/launch.rs` creates it once and passes it into
    /// `dispatch_request_view` as its own parameter for the §7.5 validity
    /// check) — this field is an ADDITIONAL reference threaded in at
    /// construction time so this handler can reach it too, without
    /// restructuring `dispatch_request_view`'s signature or the
    /// `RequestHandler` trait.
    pub(in crate::engine::db_handler) session_store: Option<Arc<SessionStore>>,
    /// Shared server token service; absent in handlers constructed without token administration.
    pub(in crate::engine::db_handler) token_service: Option<Arc<TokenService>>,
    /// Whether users may create their own tokens. Listing and revocation remain allowed.
    pub(in crate::engine::db_handler) allow_self_service: bool,
    /// R4b-1 — in-flight request table backing the activity view's
    /// `Requests` rows and the `shamir_active_requests` gauge. Shared
    /// across all clones of the handler (`Arc`). One checked id mint
    /// (a CAS loop), one insert and one RAII removal (an identity peek, then
    /// the remove) per admitted batch — the entire hot-path cost. READERS are
    /// lock-free (`scc::HashIndex::iter` / `peek_with` under an epoch
    /// `Guard`); the WRITERS take brief internal bucket locks. The RAII
    /// guard additionally owns the per-session in-flight request slot
    /// (`Session::try_acquire_request_slot` — R4b-1 follow-up). See
    /// `super::activity::requests` for the bounding argument and the
    /// privacy rule.
    pub(in crate::engine::db_handler) activity_requests: Arc<ActivityRequests>,
    /// R3-2b — shared server-wide maintenance gate for the running server's
    /// logical-export freeze (design §0.3, §4.1). `None` (both
    /// constructors) means the handler runs unenrolled — every
    /// `wait_for_mutation` fast-paths; the boot path installs the
    /// process-wide instance via [`Self::with_maintenance_gate`] so that
    /// ALL clones of the handler share the SAME gate (`MaintenanceGate` is
    /// internally `Arc`-backed and `Clone`). Enrollment covers only this
    /// native/REST request boundary: `Execute` writes, `TxBegin`, mutating
    /// `TxExecute`, `TxCommit` and the top-level identity/token mutations.
    pub(in crate::engine::db_handler) maintenance: Option<MaintenanceGate>,
    // Bound owned mutation tasks across all connection clones, even when
    // the response byte budget is unbounded or reserves zero bytes.
    owned_mutation_slots: Arc<Semaphore>,
    owned_mutation_tasks: Arc<MutationTasks>,
    /// Counts requests decoded by the raw native request path only; token REST and WebSocket decode separately and are not counted. Production has no reader; cost is one Relaxed add per native request.
    pub(in crate::engine::db_handler) decoded_requests: Arc<std::sync::atomic::AtomicU64>,
    /// R4b-3 — per-server cancellation outcome counters (one fixed slot
    /// per `CancelOutcome`), backing the `shamir_cancellations_total`
    /// family. Shared across all clones of the handler (`Arc`).
    pub(in crate::engine::db_handler) cancel_counters: Arc<CancelCounters>,
    /// R4b-1 — monotonic source of fresh [`RequestExecId`] values, minted
    /// when a BATCH is admitted (same shape as `next_cursor_id`). The
    /// client's optional `rid` is carried alongside, never replaced.
    pub(in crate::engine::db_handler) next_request_exec_id: Arc<std::sync::atomic::AtomicU64>,
    /// R3-2b — the online logical-export job service (StartExport /
    /// ExportStatus / CancelExport). `None` (both constructors) means the
    /// export surface refuses with the stable
    /// `online_export_not_configured` code; the launcher installs the
    /// process-wide instance via [`Self::with_online_export`] after the
    /// admission audit has enrolled every mutation source in the shared
    /// gate. Shared across all clones of the handler (`Arc`).
    pub(in crate::engine::db_handler) online_export: Option<Arc<OnlineExport>>,
    /// Operator boot metadata captured by `ServerLauncher` and attached
    /// via [`Self::with_operator_boot`]. `None` for handlers built
    /// outside the server boot path (unit tests).
    pub(in crate::engine::db_handler) operator_boot: Option<OperatorBootContext>,
    /// Test-only seam: parks `reset_shape_stats` at the commit claim so a
    /// test can race a cancellation against it. `None` in production (a
    /// `#[cfg(test)]` field — no runtime cost, no production surface).
    #[cfg(test)]
    pub(in crate::engine::db_handler) shape_reset_pause:
        Option<std::sync::Arc<dyn Fn(bool) + Send + Sync>>,
    #[cfg(test)]
    pub(in crate::engine::db_handler) cancel_authorized_pause: Option<(
        super::super::activity::CancelPauseTarget,
        u64,
        Arc<tokio::sync::Barrier>,
        Arc<tokio::sync::Barrier>,
    )>,
    #[cfg(test)]
    pub(in crate::engine::db_handler) commit_handoff_pause:
        Arc<std::sync::Mutex<Option<super::test_seams::CommitPause>>>,
    #[cfg(test)]
    pub(in crate::engine::db_handler) begin_pause: super::test_seams::BeginPause,
}
impl ShamirDbHandler {
    pub(crate) fn cancel_refusal_audit(&self) -> Arc<CancelRefusalAudit> {
        Arc::clone(&self.cancel_refusal_audit)
    }

    pub fn with_audit_writer(mut self, audit: Arc<AuditChainWriter>) -> Self {
        self.audit = Some(audit);
        self
    }

    /// Test-only: install the shape-reset pause seam (see
    /// [`Self::shape_reset_pause_at`]).
    #[cfg(test)]
    pub(in crate::engine::db_handler) fn with_shape_reset_pause(
        mut self,
        pause: Option<std::sync::Arc<dyn Fn(bool) + Send + Sync>>,
    ) -> Self {
        self.shape_reset_pause = pause;
        self
    }

    /// Test-only seam used by `reset_shape_stats`: called with `false`
    /// immediately before the commit claim and with `true` immediately
    /// after a successful claim, both before the wipe.
    #[cfg(test)]
    pub(in crate::engine::db_handler) fn shape_reset_pause_at(&self, after_claim: bool) {
        if let Some(pause) = &self.shape_reset_pause {
            pause(after_claim);
        }
    }

    /// Construct a handler over a shared [`ShamirDb`] without admin support.
    /// Use [`Self::with_admin`] when SCRAM user creation should be possible.
    pub fn new(db: Arc<ShamirDb>) -> Self {
        Self {
            db,
            audit: None,
            cancel_refusal_audit: Arc::default(),
            transactions: crate::config::TransactionsConfig::default(),
            admin: None,
            slow_query: SlowQueryConfig::DISABLED,
            #[cfg(any(test, feature = "test-util"))]
            slow_query_elapsed_override: None,
            query_limits: QueryLimitsCap::UNLIMITED,
            tx_limits: TxLimitsCap::UNLIMITED,
            byte_budget: ByteBudget::unbounded(),
            node_mode: NodeMode::default(),
            leader_epoch: 1,
            tx_registry: Arc::new(TxRegistry::new()),
            cursor_registry: Arc::new(CursorRegistry::new()),
            cursor_limits: CursorLimitsCap::DEFAULT,
            next_cursor_id: Arc::new(std::sync::atomic::AtomicU64::new(1)),
            token_service: None,
            allow_self_service: true,
            session_store: None,
            activity_requests: Arc::new(ActivityRequests::new()),
            decoded_requests: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            cancel_counters: Arc::new(CancelCounters::new()),
            next_request_exec_id: Arc::new(std::sync::atomic::AtomicU64::new(1)),
            maintenance: None,
            owned_mutation_slots: Arc::new(Semaphore::new(
                shamir_tunables::instance_defaults::CONN_MAX_IN_FLIGHT,
            )),
            owned_mutation_tasks: Arc::new(MutationTasks::new()),
            online_export: None,
            operator_boot: None,
            #[cfg(test)]
            shape_reset_pause: None,
            #[cfg(test)]
            cancel_authorized_pause: None,
            #[cfg(test)]
            commit_handoff_pause: Arc::new(std::sync::Mutex::new(None)),
            #[cfg(test)]
            begin_pause: Default::default(),
        }
    }

    /// Construct a handler with admin (SCRAM user-creation) support.
    pub fn with_admin(db: Arc<ShamirDb>, admin: AdminGlue) -> Self {
        Self {
            db,
            audit: None,
            cancel_refusal_audit: Arc::default(),
            transactions: crate::config::TransactionsConfig::default(),
            admin: Some(admin),
            slow_query: SlowQueryConfig::DISABLED,
            #[cfg(any(test, feature = "test-util"))]
            slow_query_elapsed_override: None,
            query_limits: QueryLimitsCap::UNLIMITED,
            tx_limits: TxLimitsCap::UNLIMITED,
            byte_budget: ByteBudget::unbounded(),
            node_mode: NodeMode::default(),
            leader_epoch: 1,
            tx_registry: Arc::new(TxRegistry::new()),
            cursor_registry: Arc::new(CursorRegistry::new()),
            cursor_limits: CursorLimitsCap::DEFAULT,
            next_cursor_id: Arc::new(std::sync::atomic::AtomicU64::new(1)),
            token_service: None,
            allow_self_service: true,
            session_store: None,
            activity_requests: Arc::new(ActivityRequests::new()),
            decoded_requests: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            cancel_counters: Arc::new(CancelCounters::new()),
            next_request_exec_id: Arc::new(std::sync::atomic::AtomicU64::new(1)),
            maintenance: None,
            owned_mutation_slots: Arc::new(Semaphore::new(
                shamir_tunables::instance_defaults::CONN_MAX_IN_FLIGHT,
            )),
            owned_mutation_tasks: Arc::new(MutationTasks::new()),
            online_export: None,
            operator_boot: None,
            #[cfg(test)]
            shape_reset_pause: None,
            #[cfg(test)]
            cancel_authorized_pause: None,
            #[cfg(test)]
            commit_handoff_pause: Arc::new(std::sync::Mutex::new(None)),
            #[cfg(test)]
            begin_pause: Default::default(),
        }
    }

    /// R3-2b — install the online-export job service (launcher-only, after
    /// the admission audit). The service is shared `Arc`-backed, so every
    /// clone of the handler reaches the same registry and root.
    /// Set server-side hard caps on per-batch resources.
    pub fn with_query_limits(mut self, query_limits: QueryLimitsCap) -> Self {
        self.query_limits = query_limits;
        self
    }

    /// Set the per-interactive-tx staging byte cap.
    pub fn with_tx_limits(mut self, tx_limits: TxLimitsCap) -> Self {
        self.tx_limits = tx_limits;
        self
    }

    /// Set the server-side result-cursor caps (per-session count,
    /// idle-timeout). Default [`CursorLimitsCap::DEFAULT`].
    pub fn with_cursor_limits(mut self, cursor_limits: CursorLimitsCap) -> Self {
        self.cursor_limits = cursor_limits;
        self
    }

    /// Set the server-wide in-flight response-byte budget (RI-15).
    pub fn with_byte_budget(mut self, byte_budget: ByteBudget) -> Self {
        self.byte_budget = byte_budget;
        self
    }

    /// Set the node read/write mode. A `ReadOnly` node rejects client
    /// writes at the [`Self::execute`] gate (before they reach the engine).
    /// Default is `ReadWrite`.
    pub fn with_node_mode(mut self, mode: NodeMode) -> Self {
        self.node_mode = mode;
        self
    }

    /// Set the leader epoch for VR-style replication fencing (§5.2).
    /// Every `ReplResponse` carries this value; followers track the max
    /// epoch seen and reject a regression. Default `1`. Persistence and
    /// bump-on-promote are R3; for now this is a static field.
    pub fn with_leader_epoch(mut self, epoch: u64) -> Self {
        self.leader_epoch = epoch;
        self
    }

    /// Give this handler access to the server-wide [`SessionStore`] so
    /// `ChangePasswordVerify` can kill every OTHER live session of the
    /// calling user (spec §12.5.3) after a successful credential change.
    /// Without this, `SessionStore` — which is owned by the connection/
    /// server layer and passed to `dispatch_request_view` as its own
    /// parameter, not through the `RequestHandler` trait — is unreachable
    /// from inside `handle`. Default `None`: the credential update in
    /// `ChangePasswordVerify` still lands, but the session-kill step is
    /// skipped.
    pub fn with_session_store(mut self, session_store: Arc<SessionStore>) -> Self {
        self.session_store = Some(session_store);
        self
    }

    /// Reference to the underlying [`ShamirDb`] (for tests / admin glue).
    pub fn db(&self) -> &Arc<ShamirDb> {
        &self.db
    }

    /// Phase B Stage 6 — borrow the interactive-tx registry so the boot
    /// path can spawn the periodic reaper against it. The registry is
    /// already `Arc<TxRegistry>` internally; this returns a clone of that
    /// `Arc` so the reaper task and the handler share state.
    pub fn tx_registry(&self) -> Arc<TxRegistry> {
        Arc::clone(&self.tx_registry)
    }

    /// FG-5b — borrow the cursor registry so the boot path can spawn the
    /// periodic reaper against it. Mirrors [`Self::tx_registry`].
    pub fn cursor_registry(&self) -> Arc<CursorRegistry> {
        Arc::clone(&self.cursor_registry)
    }

    /// R3-2b — maintenance-gate admission for one request. `None` when no
    /// gate is installed (unenrolled handler) or the caller chose not to
    /// enroll this op; `Some(guard)` when the request was admitted: the
    /// guard MUST be retained until the request's last mutation has landed
    /// (RAII drop releases the writer-drain slot). While an online-export
    /// freeze is raised this awaits the drain and returns only after the
    /// freeze is released — reads stay available throughout, only
    /// mutations queue here. Cancel-safe: the future may be dropped while
    /// parked (the caller's request future is the only holder).
    pub(in crate::engine::db_handler) async fn maintenance_admission(
        &self,
    ) -> Option<WriterDrainGuard> {
        let gate = self.maintenance.as_ref()?;
        Some(gate.wait_for_mutation().await)
    }

    pub(crate) fn owned_mutation_tasks(&self) -> Arc<MutationTasks> {
        Arc::clone(&self.owned_mutation_tasks)
    }

    // Reject overload before maintenance admission or spawning any work.
    pub(in crate::engine::db_handler) fn try_owned_mutation_slot(
        &self,
    ) -> Result<OwnedSemaphorePermit, ()> {
        Arc::clone(&self.owned_mutation_slots)
            .try_acquire_owned()
            .map_err(|_| ())
    }

    /// FG-5b — mint a fresh, process-unique cursor id.
    pub(in crate::engine::db_handler) fn next_cursor_id(&self) -> u64 {
        self.next_cursor_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    /// R4b-1 — mint a fresh, CHECKED batch request-exec id (`None` at
    /// range exhaustion; see `export_activity::next_batch_exec_id`).
    pub(in crate::engine::db_handler) fn next_request_exec_id(&self) -> Option<u64> {
        super::export_activity::next_batch_exec_id(&self.next_request_exec_id)
    }
}

impl RequestHandler for ShamirDbHandler {
    fn handle<'a>(
        &'a self,
        session: &'a Session,
        req: &'a [u8],
        conn: &'a ConnectionServices,
    ) -> shamir_connect::server::dispatch::HandlerFuture<'a> {
        // R4b-1 — the raw native wire entry point has no transport request id and
        // forwards `None`; token REST and token WebSocket reach the SAME execution
        // through the already-decoded native handler (see `NativeDbHandler`).
        self.handle_with_request_id(session, req, conn, None)
    }

    fn handle_with_request_id<'a>(
        &'a self,
        session: &'a Session,
        req: &'a [u8],
        conn: &'a ConnectionServices,
        request_id: Option<u32>,
    ) -> shamir_connect::server::dispatch::HandlerFuture<'a> {
        Box::pin(async move {
            let request: DbRequest = match shamir_query_types::validate_request_alias_caps(req)
                .and_then(|()| rmp_serde::from_slice(req))
            {
                Ok(request) => request,
                Err(e) => {
                    let response = DbResponse::Error {
                        code: "invalid_request".into(),
                        message: e.to_string(),
                    };
                    return rmp_serde::to_vec_named(&response)
                        .map_err(|encode_err| format!("response encode: {}", encode_err));
                }
            };
            self.decoded_requests
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.handle_decoded_with_request_id(session, request, conn, request_id)
                .await
        })
    }
}

impl ShamirDbHandler {
    /// Handle an already decoded request through the native authorization and execution path.
    pub(crate) async fn handle_decoded(
        &self,
        session: &Session,
        request: DbRequest,
        conn: &ConnectionServices,
    ) -> Result<Vec<u8>, String> {
        // R4b-1 — compatibility path has no request id; rid callers use the variant below.
        self.handle_decoded_with_request_id(session, request, conn, None)
            .await
    }
    /// R4b-1 — rid-carrying variant of [`Self::handle_decoded`]. The
    /// client's OPTIONAL wire `rid` is carried alongside the minted
    /// [`RequestExecId`] into the in-flight activity entry; it never
    /// replaces it.
    pub(crate) async fn handle_decoded_with_request_id(
        &self,
        session: &Session,
        request: DbRequest,
        conn: &ConnectionServices,
        client_rid: Option<u32>,
    ) -> Result<Vec<u8>, String> {
        let top_level_canonical = match canon::canonical_db_request(&request) {
            Ok(bytes) => Some(bytes),
            Err(canon::CanonicalOpError::UnsupportedOperation) => None,
            Err(canon::CanonicalOpError::Encoding(_)) => {
                let response = DbResponse::Error {
                    code: "hmac_required".into(),
                    message: "request canonical encoding failed".into(),
                };
                return rmp_serde::to_vec_named(&response)
                    .map_err(|encode_err| format!("response encode: {}", encode_err));
            }
        };
        let response = match request {
            DbRequest::Ping => DbResponse::Pong,
            DbRequest::WaitForVisibility {
                query_version,
                db,
                repo,
                version,
                visibility_wait,
            } => {
                self.wait_for_visibility(
                    session,
                    query_version,
                    &db,
                    &repo,
                    version,
                    visibility_wait,
                )
                .await
            }
            DbRequest::Execute {
                query_version,
                db,
                batch,
            } => {
                self.execute_with_rid(session, query_version, &db, batch, conn, client_rid)
                    .await
            }
            // R3-2b — identity mutations are enrolled: admission is taken
            // around the handler call and retained through it, so an
            // online-export freeze drains pending user/superuser/
            // replicator changes before the freeze's logical instant.
            DbRequest::CreateScramUser {
                name,
                password,
                roles,
                hmac,
            } => {
                let _maintenance = self.maintenance_admission().await;
                create_scram_user(
                    self.admin.as_ref(),
                    session,
                    name,
                    password,
                    roles,
                    hmac,
                    top_level_canonical,
                )
                .await
            }
            DbRequest::SetSuperuser { user, on, hmac } => {
                let _maintenance = self.maintenance_admission().await;
                set_superuser(
                    self.admin.as_ref(),
                    session,
                    user,
                    on,
                    hmac,
                    top_level_canonical,
                )
                .await
            }
            DbRequest::SetReplicator { user, on, hmac } => {
                let _maintenance = self.maintenance_admission().await;
                set_replicator(
                    self.admin.as_ref(),
                    session,
                    user,
                    on,
                    hmac,
                    top_level_canonical,
                )
                .await
            }

            // --- Phase B: interactive (multi-call) transactions ---
            DbRequest::TxBegin {
                query_version,
                db,
                repo,
                isolation,
            } => {
                self.tx_begin_with_rid(session, query_version, &db, &repo, isolation, client_rid)
                    .await
            }
            DbRequest::TxExecute {
                query_version,
                db,
                tx_handle,
                batch,
            } => {
                self.tx_execute_with_rid(session, query_version, &db, tx_handle, batch, client_rid)
                    .await
            }
            DbRequest::TxCommit {
                query_version,
                visibility_wait,
                db,
                tx_handle,
            } => {
                if let Err(e) = crate::version::check_query_lang(query_version) {
                    DbResponse::Error {
                        code: "unsupported_query_version".into(),
                        message: e.to_string(),
                    }
                } else {
                    match visibility_wait {
                        None => self.tx_commit(session, &db, tx_handle).await,
                        Some(wait) => {
                            self.tx_commit_with_options(session, &db, tx_handle, Some(wait))
                                .await
                        }
                    }
                }
            }
            DbRequest::TxRollback { db, tx_handle } => {
                self.tx_rollback(session, &db, tx_handle).await
            }
            // REPLICATION §5 — privileged pull-API (R0-b).
            // Role + per-repo authorisation enforced inside `handle_repl`.
            DbRequest::Repl(repl_req) => {
                DbResponse::Repl(self.handle_repl(session, repl_req).await)
            }

            // changePassword (spec §12.5) — see `db_handler::admin`.
            DbRequest::ChangePasswordChallenge { client_nonce_cp } => {
                change_password_challenge(self.admin.as_ref(), session, client_nonce_cp).await
            }
            // R3-2b — `ChangePasswordVerify` writes the credential row and
            // (with a session store) kills other sessions: enrolled.
            DbRequest::ChangePasswordVerify(secrets) => {
                let _maintenance = self.maintenance_admission().await;
                change_password_verify(
                    self.admin.as_ref(),
                    self.session_store.as_ref(),
                    session,
                    secrets.client_proof_old,
                    secrets.new_salt,
                    secrets.new_stored_key,
                    secrets.new_server_key,
                )
                .await
            }

            // R3-2b — token administration: only the MUTATING ops
            // (`CreateApiToken`, `RevokeApiToken`, `RevokeUserTokens`) are
            // enrolled; `ListApiTokens` is a read and stays available
            // during a freeze.
            DbRequest::CreateApiToken { .. }
            | DbRequest::ListApiTokens { .. }
            | DbRequest::RevokeApiToken { .. }
            | DbRequest::RevokeUserTokens { .. } => {
                let _maintenance = if matches!(
                    &request,
                    DbRequest::CreateApiToken { .. }
                        | DbRequest::RevokeApiToken { .. }
                        | DbRequest::RevokeUserTokens { .. }
                ) {
                    self.maintenance_admission().await
                } else {
                    None
                };
                token_admin::handle(self, session, &request, top_level_canonical.as_deref()).await
            }

            // --- FG-5b: server-side result cursors ---
            // See `crate::cursor_registry` and `db_handler::cursor_handlers`
            // for the registry + snapshot-pinning + keyset-pagination
            // implementation this dispatches into.
            DbRequest::CreateCursor {
                query_version,
                db,
                query,
                page_size,
            } => {
                self.create_cursor(session, query_version, &db, query, page_size)
                    .await
            }
            DbRequest::FetchNext {
                cursor_id,
                page_size,
            } => self.fetch_next(session, cursor_id, page_size).await,
            DbRequest::CancelCursor { cursor_id } => self.cancel_cursor(session, cursor_id).await,
            DbRequest::GetDdlOpStatus {
                db,
                repo,
                table,
                op_id,
            } => {
                self.get_ddl_op_status(session, &db, &repo, &table, &op_id)
                    .await
            }
            // Admin-shell PR3, P2 — the typed exact-key point read. See
            // `repo_admin/operator_state.rs` for the gate ordering
            // (authorization before lookup), the quarantine probe, and the
            // result-size / RI-15 budget controls.
            DbRequest::GetRecord {
                db,
                repo,
                table,
                key,
            } => self.get_record(session, &db, &repo, &table, key).await,
            // Admin-shell PR3, P1 — the typed operator status snapshot.
            // See `repo_admin/operator_state.rs`: superuser gate, boot
            // capture (no request-time stamp or data-dir inspection; the
            // one catalogue read may touch disk), exported replication
            // snapshot.
            DbRequest::Status => self.operator_status(session).await,
        };

        // CR-B2: `ShamirDbHandler::execute` already serialized this
        // exact `response` value once (to shrink its upfront RI-15
        // reservation down to the actual size) and stashed those bytes
        // via `stash_serialized_response`. Reuse them here instead of
        // encoding a second time. Cursor page fetches stash too under a
        // finite budget or cap (a fetch that loses the terminal removal
        // discards it); the other arms (`Ping`, `ChangePassword*`, ...)
        // never stash, so they fall through to the fresh serialize.
        if let Some(bytes) = crate::byte_budget::take_stashed_serialized_response() {
            return Ok(bytes);
        }

        rmp_serde::to_vec_named(&response).map_err(|e| format!("encode_error: {}", e))
    }
}

/// Classification of a [`BatchError`] for the wire `code` tag.
///
/// When the error carries a structured `code` (DDL / admin errors set one
/// since §5 error-codes; the engine's implicit-commit receipt mapping sets
/// the commit-admission codes `wal_requires_reopen` /
/// `wal_retention_exceeded`), that code is returned verbatim. Unclassified
/// errors fall back to heuristic string-matching for backward compat.
pub(in crate::engine::db_handler) fn error_code(e: &BatchError) -> &str {
    match e {
        BatchError::LimitExceeded { kind, .. } => match kind {
            shamir_query_types::batch::LimitKind::PageSize
            | shamir_query_types::batch::LimitKind::Offset => "pagination_too_large",
            shamir_query_types::batch::LimitKind::Work
            | shamir_query_types::batch::LimitKind::TotalQueries => "work_budget_exceeded",
            shamir_query_types::batch::LimitKind::ResultBytes => "result_too_large",
            shamir_query_types::batch::LimitKind::Depth => "nesting_too_deep",
            shamir_query_types::batch::LimitKind::Iterations => "too_many_iterations",
        },
        BatchError::Cancelled => shamir_query_types::wire::error_code::CANCELLED,
        BatchError::TooManyIterations { .. } => "too_many_iterations",
        BatchError::TooManyQueries { .. }
        | BatchError::TooDeep { .. }
        | BatchError::ExecutionTimedOut { .. } => "limits",
        BatchError::CircularDependency { .. }
        | BatchError::UnknownAlias { .. }
        | BatchError::AfterPathIgnored { .. }
        | BatchError::InvalidWhenFilter { .. }
        | BatchError::InvalidCondCondition { .. } => "validation",
        BatchError::Timeout { .. } => "timeout",
        BatchError::LockTimeout { .. } => "lock_timeout",
        BatchError::QueryError {
            alias,
            message,
            code,
        } => {
            // Prefer structured code when present.
            if let Some(c) = code {
                return c.as_str();
            }
            // Legacy heuristic for untagged errors.
            if alias.is_empty() && message.contains("not found") {
                "unknown_db"
            } else if message.starts_with("access denied:") {
                "access_denied"
            } else {
                "query"
            }
        }
        BatchError::CrossRepoNotSupported { .. } => "tx_cross_repo_not_supported",
        BatchError::NestingTooDeep { .. } => "nesting_too_deep",
        // FG-5b: wire error codes for the cursor protocol.
        BatchError::CursorNotFound { .. } => "cursor_not_found",
        BatchError::CursorExpired { .. } => "cursor_expired",
        BatchError::CursorLimitExceeded { .. } => "cursor_limit_exceeded",
        // FG-5b: `CreateCursor` rejects AsOf/History queries outright (see
        // the variant's doc comment for the scope-cut rationale).
        BatchError::CursorTemporalNotSupported => "cursor_temporal_not_supported",
        // CR-B5: `CreateCursor` rejects `with_version = true` outright (see
        // the variant's doc comment — a cursor's AsOf-pinned reads hard-code
        // `versions: None`, so honoring the flag would silently drop it).
        BatchError::CursorWithVersionNotSupported => "cursor_with_version_not_supported",
        // CR-A3: `page_size == 0` (infinite-loop hazard) or `page_size` above
        // `max_cursor_page_size` (unbounded materialization hazard).
        BatchError::InvalidPageSize { .. } => "invalid_page_size",
        // CR-A5: a cursor page's serialized size exceeded
        // `max_result_size_bytes`; rejected outright, never truncated.
        BatchError::CursorPageTooLarge { .. } => "cursor_page_too_large",
        BatchError::AsOfVisibilityTimeout { .. } => "as_of_visibility_timeout",
        // RV-WAL-3B29232E: implicit single-op commit whose WAL outcome was
        // unknown after queueing — stable code; the attempted commit_version
        // rides in the typed error payload, no success claim.
        BatchError::WalOutcomeUnknown { .. } => "wal_requires_reopen",
        // S5c-X PR8b: read refused — replication destination in
        // `resync_required` quarantine; non-retryable until reseed.
        BatchError::ResyncRequired { .. } => "resync_required",
    }
}

impl ShamirDbHandler {
    /// Handle `DbRequest::GetDdlOpStatus` — poll a DDL operation's status by ID.
    ///
    /// #1064 (P0 SECURITY): this poll previously ran with NO authorization at
    /// all — any authenticated client that knew (or guessed, or found in a
    /// log/metric) an `op_id` could read another actor's DDL status,
    /// including `Failed.detail` internal error text, and use the
    /// found/not-found distinction as a db/repo/table existence oracle.
    /// Gated the same way `create_cursor` gates a cursor read
    /// (`authorize_cursor_read`: `Action::Read` on the `Database`, then on
    /// the `Table`) — reused directly rather than duplicated, since a DDL
    /// status poll is semantically "read this table's operational state."
    /// Authorizing BEFORE any table/repo resolution (using the raw name
    /// strings, exactly like `create_cursor` does) is what keeps an
    /// unauthorized caller's response indistinguishable from a genuine
    /// not-found — `self.db.get_ddl_op_status` (which does the actual
    /// resolution) is never even called unless authorization already
    /// succeeded. When it does run, a miss is classified with the SAME
    /// table-Read parity the gate itself uses: an absent TABLE answers
    /// `unknown_table` (the code `authorize_cursor_read`'s Table denial
    /// answers for a closed table), while a vanished parent keeps its own
    /// typed `unknown_db`/`unknown_repo`.
    async fn get_ddl_op_status(
        &self,
        session: &Session,
        db: &str,
        repo: &str,
        table: &str,
        op_id_str: &str,
    ) -> DbResponse {
        let actor = session_actor(session);
        if let Err(e) = authorize_cursor_read(&self.db, &actor, db, repo, table).await {
            if error_code(&e) == "unknown_table" {
                return table_unavailable_response();
            }
            return error_response(&e);
        }

        // Availability BEFORE the op_id is parsed: `get_ddl_op_status`
        // parses the id before resolving the table, so a malformed id would
        // otherwise answer `validation` for an absent table while a closed
        // one is refused by the Table ACL. Decide the parent/table gate here
        // instead: a vanished parent keeps its typed `unknown_db`/
        // `unknown_repo`, while an absent table and an existing-but-closed
        // table both answer the SAME fixed refusal.
        let repo_instance = match resolve_repo(&self.db, db, repo) {
            Ok(repo) => repo,
            Err(parent) => return error_response(&parent),
        };
        if !repo_instance.has_table(table) {
            return table_unavailable_response();
        }

        match self.db.get_ddl_op_status(db, repo, table, op_id_str).await {
            Ok(status) => DbResponse::DdlOpStatus { status },
            Err(e) => match e {
                // A table that vanished between the configured-table check
                // and the engine lookup is the same fixed table-unavailable
                // refusal. A vanished PARENT keeps its own typed code.
                shamir_db::DbError::NotFound(_) => match resolve_repo(&self.db, db, repo) {
                    Ok(_) => table_unavailable_response(),
                    Err(parent) => error_response(&parent),
                },
                other => {
                    let code = match &other {
                        shamir_db::DbError::Validation(_) => "validation",
                        _ => "query",
                    };
                    DbResponse::Error {
                        code: code.to_string(),
                        message: format!("Failed to read DDL op status: {}", other),
                    }
                }
            },
        }
    }
}

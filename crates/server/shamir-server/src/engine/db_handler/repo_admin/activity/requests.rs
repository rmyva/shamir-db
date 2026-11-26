//! In-flight request table for the activity view (R4b-1, operational-views
//! §2, decisions D5/D2 — D8 groundwork).
//!
//! One entry per ADMITTED batch, keyed by a handler-minted
//! [`RequestExecId`] (process-unique, from an `AtomicU64` — the same shape
//! as `next_cursor_id`). The client's optional `rid` is carried alongside
//! (`RequestEntry::client_rid`), never replaced by the minted id.
//!
//! # Lock-free readers
//!
//! Backed by `scc::HashIndex<u64, Arc<RequestEntry>, THasher>` (linked
//! scc 3.8.4): the paging reader uses
//! [`HashIndex::iter`]`(&scc::Guard)` and [`HashIndex::peek_with`] —
//! epoch-based, lock-free reader paths. The WRITERS (`insert_sync`,
//! `remove_sync`) do take brief internal bucket locks, but they are used
//! only on the registration/deregistration path (one checked id mint, one
//! insert and one identity-checked removal per batch, the whole hot-path
//! cost); readers never block on them —
//! `HashMap::iter_sync` / `DashMap::iter` were rejected because they take
//! locks a reader would contend on.
//!
//! # Bounding
//!
//! The table carries no cap of its own; per-SESSION depth is bounded by
//! the per-session in-flight request-slot cap (`QueryLimitsCap::
//! max_in_flight_requests_per_session` acquired through `Session::
//! try_acquire_request_slot`, R4b-1 follow-up): every tracked path
//! (batch execute, `TxBegin`, `TxExecute`) acquires the slot BEFORE
//! execution and the byte-budget wait, the RAII guard holds the permit
//! until drop, and a saturated session is rejected with the fixed
//! `server_busy` wire text. The cap is skipped ONLY for the stateless
//! token-REST fallback (`Session::activity_handle() == 0`). Aggregate
//! depth remains bounded by the existing admission rails — the shared
//! `ConnLimiter` (`security.connection.max_active_connections`,
//! `server_launcher/listeners.rs`), the per-connection in-flight semaphore
//! (`max_in_flight`, shamir-sync `request_loop.rs`), and the token-WS /
//! REST gates mirror it.
//!
//! # Cancellation state (D8)
//!
//! The executor and CancelRequest share one [`CancelHandle`]. Cancel can
//! only claim Running. Drop finishes if possible, then removes the entry
//! even if cancel won.
//!
//! # Privacy
//!
//! Entries carry ids, actor, db, the STATIC op-kind discriminant name and
//! timing only — never filter values, literals, row data, canonical query
//! bytes, the bearer token or its prefix (not even in `Debug` output, and
//! `RequestEntry` deliberately does not derive `Debug`).
//!
//! # Session-handle sentinel
//!
//! [`RequestEntry::session_handle`] carries
//! `Session::activity_handle()` as `Option<u64>`: `None` is the
//! connect-layer "un-stamped" sentinel (handle `0`, `UNSTAMPED_HANDLE`),
//! meaning the row came from a path with no minted handle (currently the
//! stateless token-REST fallback; token-WS sessions are separately stored
//! and minted — coordinated with the connect worker). `Some(h)` otherwise.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use scc::HashIndex;
use shamir_collections::THasher;
use shamir_connect::server::session::Session;
use shamir_connect::server::SessionRequestSlot;
use shamir_query_types::admin::activity::RequestExecId;
use shamir_tx::CancelHandle;

/// Fixed wire text of the overload rejection: the `server_busy` code
/// (and message) already produced by the HTTP REST executor and the
/// serialized launcher overload frame (`server_launcher/http/
/// launcher.rs`). No `BatchError` mapping — a plain fixed-text
/// `DbResponse::Error` on every saturated-session rejection.
pub(in crate::engine::db_handler) const SERVER_BUSY: &str = "server_busy";

/// The fixed-text `server_busy` response for a session whose per-session
/// in-flight request slots are exhausted (see the module bounding docs).
pub(in crate::engine::db_handler) fn server_busy_response() -> shamir_query_types::wire::DbResponse
{
    shamir_query_types::wire::DbResponse::Error {
        code: SERVER_BUSY.to_owned(),
        message: SERVER_BUSY.to_owned(),
    }
}

/// One in-flight batch row. Static discriminant names only
/// (`BatchOp::activity_name()`), never argument-derived.
pub(in crate::engine::db_handler) struct RequestEntry {
    /// Handler-minted process-unique id (the map key).
    pub id: RequestExecId,
    /// Minted session handle (`Session::activity_handle()`); `None` = the
    /// connect-layer un-stamped sentinel handle `0` (stateless token-REST
    /// fallback — see the module docs); `Some(h)` otherwise.
    pub session_handle: Option<u64>,
    /// Directory-minted 16-byte user id — the value `scope: Own` filtering
    /// matches on. Not a username, not a credential.
    pub user_id: [u8; 16],
    /// Display-only username. Not a credential.
    pub username: String,
    /// Database name the batch targets.
    pub db: String,
    /// STATIC op-kind name (`read` / `write` / `admin:<op>`), derived
    /// from the parsed discriminant only.
    pub op_kind: &'static str,
    /// MONOTONIC registration timestamp (ns since process start, from a
    /// single `Instant` epoch — never the wall clock, so `elapsed_ns()` is
    /// immune to clock steps). Atomic so the paging worker reads it
    /// lock-free; `elapsed_ns()` derives the age from it.
    pub started_at_ns: AtomicU64,
    /// State and wake primitive shared with the executor.
    pub cancel: Arc<CancelHandle>,
    /// Admission-time policy, immutable after publication.
    pub cancellable: bool,
    /// The client's OPTIONAL wire `rid`, carried alongside the minted id.
    pub client_rid: Option<u32>,
}

impl RequestEntry {
    /// Owned constructor for one admitted batch (used by
    /// [`ShamirDbHandler::try_track_activity_request`] and tests; keeps every
    /// call site under the clippy argument-count limit by combining the
    /// session metadata). The start stamp is the monotonic clock; the
    /// state word starts `Running`.
    pub(in crate::engine::db_handler) fn new(
        id: RequestExecId,
        session: &Session,
        db: String,
        op_kind: &'static str,
        client_rid: Option<u32>,
    ) -> Self {
        let handle = session.activity_handle();
        Self {
            id,
            session_handle: (handle != 0).then_some(handle),
            user_id: session.user_id,
            username: session.username.clone(),
            db,
            op_kind,
            started_at_ns: AtomicU64::new(monotonic_now_ns()),
            cancel: Arc::new(CancelHandle::default()),
            cancellable: true,
            client_rid,
        }
    }

    /// Nanoseconds since this batch was admitted (lock-free read of the
    /// atomic monotonic start stamp against the same clock).
    pub(in crate::engine::db_handler) fn elapsed_ns(&self) -> u64 {
        monotonic_now_ns().saturating_sub(self.started_at_ns.load(Ordering::Relaxed))
    }
}

/// Process-start epoch for the monotonic request clock: ns-since-start
/// stamps are computed against this single `Instant`, so `elapsed_ns()`
/// never depends on the wall clock (no clock-step artifacts).
static MONOTONIC_EPOCH: std::sync::LazyLock<std::time::Instant> =
    std::sync::LazyLock::new(std::time::Instant::now);

/// Monotonic now, in nanoseconds since process start, offset by one so a
/// stamp is never zero: the epoch is initialised by the first call, and a
/// coarse clock (Windows) can report zero elapsed for it.
pub(in crate::engine::db_handler) fn monotonic_now_ns() -> u64 {
    (MONOTONIC_EPOCH.elapsed().as_nanos() as u64).saturating_add(1)
}

/// In-flight table + gauge mirror. Clone-share via `Arc` (the handler
/// holds one; every handler clone shares it). READERS are lock-free
/// (epoch `Guard` iteration and `peek_with`); the WRITERS
/// (`insert_sync` / `remove_sync`) take brief internal bucket locks on
/// the registration/deregistration path only.
pub(in crate::engine::db_handler) struct ActivityRequests {
    index: HashIndex<u64, Arc<RequestEntry>, THasher>,
    /// Mirror of the live entry count, incremented BEFORE publication and
    /// decremented exactly once AFTER a successful removal, so it can
    /// never underflow and never counts an unobservable entry. Gauges read
    /// this mirror — never `len()` on the scc map.
    in_flight: AtomicUsize,
}

impl ActivityRequests {
    pub(in crate::engine::db_handler) fn new() -> Self {
        Self {
            index: HashIndex::default(),
            in_flight: AtomicUsize::new(0),
        }
    }

    /// Register one admitted batch that already HOLDS (or deliberately
    /// skips, `None` = stateless handle-0 fallback) its per-session
    /// request slot (production path via
    /// [`ShamirDbHandler::try_track_activity_request`]). The mirror is
    /// incremented BEFORE the entry becomes observable (`insert_sync`),
    /// so the gauge never under-reports and can never underflow on
    /// removal. Returns the RAII guard that owns the entry AND the slot
    /// for the batch's whole execution; see [`InFlightRequest`].
    pub(in crate::engine::db_handler) fn register_with_slot(
        self: &Arc<Self>,
        entry: RequestEntry,
        slot: Option<SessionRequestSlot>,
    ) -> InFlightRequest {
        let entry = Arc::new(entry);
        self.in_flight.fetch_add(1, Ordering::Relaxed);
        // Collision-safe: the mirror is incremented first, then rolled
        // back if the insert loses a key race (impossible with the
        // disjoint CHECKED mint in `export_activity`, but enforced here so
        // a failed insert can never leave the gauge counting a row that
        // does not exist).
        if self
            .index
            .insert_sync(entry.id.0, Arc::clone(&entry))
            .is_err()
        {
            self.in_flight.fetch_sub(1, Ordering::Relaxed);
        }
        InFlightRequest {
            table: Arc::clone(self),
            entry,
            _slot: slot,
        }
    }

    /// Test-only seam: register WITHOUT a session slot (delegates to
    /// [`Self::register_with_slot`] with `None`).
    #[cfg(test)]
    pub(in crate::engine::db_handler) fn register(
        self: &Arc<Self>,
        entry: RequestEntry,
    ) -> InFlightRequest {
        self.register_with_slot(entry, None)
    }

    /// Gauge/mirror read — the ONLY count source (no scc `len()`).
    pub(in crate::engine::db_handler) fn in_flight(&self) -> usize {
        self.in_flight.load(Ordering::Relaxed)
    }

    /// Lock-free point lookup by minted id for the paging worker
    /// (`peek_with` — an epoch-guard read; no lock a writer takes). The
    /// caller treats a vanished entry (request finished between the key
    /// pass and the lookup) as an ordinary torn-snapshot skip (D4).
    pub(in crate::engine::db_handler) fn get(&self, id: u64) -> Option<Arc<RequestEntry>> {
        self.index.peek_with(&id, |_, entry| Arc::clone(entry))
    }

    /// Lock-free scan for the paging worker (`list.rs`): every live entry
    /// is visited exactly once through the epoch `Guard`; no lock a
    /// writer takes is held, and a momentarily torn snapshot (entries
    /// appearing/disappearing mid-scan) is acceptable and documented in
    /// the design (§2). Order is unspecified — the caller sorts.
    pub(in crate::engine::db_handler) fn for_each<F: FnMut(&RequestEntry)>(&self, mut f: F) {
        let guard = scc::Guard::new();
        for (_, entry) in self.index.iter(&guard) {
            f(entry);
        }
    }
}

/// RAII registration for one admitted batch. Drop attempts to finish
/// without overwriting `Cancelled`, removes the row, and decrements the mirror on
/// every exit path, including panics and early returns, so a request can
/// never leak an entry.
pub(in crate::engine::db_handler) struct InFlightRequest {
    table: Arc<ActivityRequests>,
    entry: Arc<RequestEntry>,
    /// The per-session in-flight request slot (R4b-1 follow-up), held
    /// until drop so the permit is released on EVERY exit path (panic,
    /// early return) together with the entry. `None` only for the
    /// stateless (handle-0) fallback path, which skips the cap.
    _slot: Option<SessionRequestSlot>,
}

impl InFlightRequest {
    /// The minted id of the batch this guard tracks (the production read
    /// path for the id — the slow-query log and the `ListActivity`
    /// self-exclusion use this instead of touching the table).
    pub(in crate::engine::db_handler) fn id(&self) -> RequestExecId {
        self.entry.id
    }

    /// The OPTIONAL client `rid` carried alongside the minted id (the
    /// production read path for the slow-query log).
    pub(in crate::engine::db_handler) fn client_rid(&self) -> Option<u32> {
        self.entry.client_rid
    }

    /// Shared state and wake handle for propagation into the executor.
    pub(in crate::engine::db_handler) fn cancel_handle(&self) -> &Arc<CancelHandle> {
        &self.entry.cancel
    }

    /// Claim completion once before publishing a reply. If cancellation
    /// won, the caller must not return a normal result.
    pub(in crate::engine::db_handler) fn finish(&self) -> bool {
        self.cancel_handle().finish()
    }
}

impl Drop for InFlightRequest {
    fn drop(&mut self) {
        // Preserve the winner of the cancel/completion race.
        self.finish();
        // 2. Removal tied to ENTRY IDENTITY: only OUR row leaves, and only
        //    then the mirror decrements. A foreign row under the same key
        //    (impossible with the disjoint checked mint, enforced for
        //    defence in depth) is never removed — either guard could
        //    otherwise remove the other's row by key alone.
        let ours = self
            .table
            .index
            .peek_with(&self.entry.id.0, |_, v| Arc::ptr_eq(v, &self.entry))
            == Some(true);
        if ours && self.table.index.remove_sync(&self.entry.id.0) {
            // 3. Decrement the mirror once, only for a real removal — keeps
            //    the mirror in exact agreement with the live entry count.
            self.table.in_flight.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

impl crate::db_handler::ShamirDbHandler {
    /// R4b-1 — register one admitted unit of work in the in-flight
    /// activity table and return the RAII guard that owns the entry AND
    /// the per-session in-flight request slot (released on drop, every
    /// exit path). One checked id mint, ONE insert and ONE identity-checked
    /// removal per admission (the whole hot-path cost); the minted [`RequestExecId`] and the carried
    /// client `rid` are read off the returned guard
    /// ([`InFlightRequest::id`], [`InFlightRequest::client_rid`]). Shared
    /// by the batch execute path and the interactive-tx open/execute
    /// paths (the `tx_open` / batch op-kind rows), so every tracked path
    /// uses one registration primitive.
    ///
    /// Per-session cap (R4b-1 follow-up): BEFORE returning, a stamped
    /// session (`Session::activity_handle() != 0`) must acquire a slot
    /// under `QueryLimitsCap::max_in_flight_requests_per_session` via
    /// [`Session::try_acquire_request_slot`]; an exhausted session yields
    /// `Err(())` — the caller maps it to the fixed `server_busy` text
    /// ([`server_busy_response`]) BEFORE any execution or byte-budget
    /// wait, so a real write is refused with no side effect. The cap is
    /// skipped ONLY for the stateless handle-0 fallback.
    pub(in crate::engine::db_handler) fn try_track_activity_request(
        &self,
        session: &Session,
        db: &str,
        op_kind: &'static str,
        client_rid: Option<u32>,
    ) -> Result<InFlightRequest, ()> {
        self.try_track_activity_request_with_cancellability(session, db, op_kind, client_rid, true)
    }

    /// Register with an explicit policy before publishing the row (e.g.
    /// for interactive request work that has no cancellation checkpoints).
    pub(in crate::engine::db_handler) fn try_track_activity_request_with_cancellability(
        &self,
        session: &Session,
        db: &str,
        op_kind: &'static str,
        client_rid: Option<u32>,
        cancellable: bool,
    ) -> Result<InFlightRequest, ()> {
        let slot = if session.activity_handle() == 0 {
            // Stateless token-REST fallback: no minted handle, no slot.
            None
        } else {
            Some(
                session
                    .try_acquire_request_slot(self.query_limits.max_in_flight_requests_per_session)
                    .ok_or(())?,
            )
        };
        let exec_id = RequestExecId(self.next_request_exec_id().ok_or(())?);
        let mut entry = RequestEntry::new(exec_id, session, db.to_string(), op_kind, client_rid);
        entry.cancellable = cancellable;
        Ok(Arc::clone(&self.activity_requests).register_with_slot(entry, slot))
    }

    #[cfg(test)]
    pub(crate) fn track_committing_request_for_test(
        &self,
        session: &Session,
        db: &str,
    ) -> (RequestExecId, impl FnOnce() + use<>) {
        let request = self
            .try_track_activity_request(session, db, "write", None)
            .expect("register test write");
        assert!(request.cancel_handle().try_commit());
        (request.id(), move || drop(request))
    }
}

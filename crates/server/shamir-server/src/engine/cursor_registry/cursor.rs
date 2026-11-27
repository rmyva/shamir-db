//! FG-5b — server-side result cursor registry.
//!
//! Parks a bookmark-shaped, `Send + 'static` cursor state server-side
//! between `FetchNext` round-trips, keyed by an opaque `cursor_id` (see
//! `shamir_query_types::wire::CursorId`) and bound to the authenticated
//! session. Mirrors `crate::tx_registry` (`InteractiveTx` / `TxRegistry` /
//! `spawn_reaper_task`) almost exactly — see that module's doc comment for
//! the shared design rationale (dashmap over the server layer, the one
//! across-`.await` `tokio::sync::Mutex` per handle).
//!
//! # Design (see `docs/dev-artifacts/prompts/post-alpha/03-fg5b-engine-session-cursor.md`)
//!
//! A cursor does NOT hold a live `futures::Stream` across async calls — that
//! would fight `TableManager`'s `'a`-borrowed stream lifetimes for no
//! benefit. Instead cursor state is just a **bookmark**: the caller's
//! original `ReadQuery`, a resume bookmark (either a `Gt`/`Lt` boundary-
//! filter seek key when the query has an ORDER BY, or a row-count offset
//! otherwise — see `db_handler::cursor_handlers`'s module doc for why a
//! bare `Pagination::After` does not work here), whether a next page is
//! known to exist, and a pinned `shamir_tx::SnapshotGuard` (+ the version it
//! pinned) so every `FetchNext` reads a stable, snapshot-consistent view
//! for the cursor's whole lifetime — even though the stored query's own
//! `temporal` field says `Latest` (the only temporal mode a cursor
//! supports; see `shamir_query_types::batch::BatchError::CursorTemporalNotSupported`).
//!
//! **Difference from `TxRegistry`:** a session may open MANY cursors (up to
//! `CursorLimitsCap::max_cursors_per_session`), not just one. Tracking a
//! live per-session count needs O(1) cardinality, not `Vec<u64>::len()`
//! (banned per CLAUDE.md's O(x→0) pillar) — so `by_session` maps
//! `[u8; 32] → Arc<AtomicUsize>`, a live open-count per session that
//! `register` increments (rejecting over-cap) and `remove` decrements.

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use shamir_query_types::read::ReadQuery;
use shamir_tx::SnapshotGuard;
use shamir_types::types::record_id::RecordId;
use tokio::task::JoinHandle;

/// Default idle TTL for an open cursor before the reaper evicts it.
///
/// Deliberately longer than the interactive-tx idle TTL
/// (`crate::tx_registry::DEFAULT_INTERACTIVE_TX_IDLE_TTL`, 30 s): a cursor
/// is read-only and its `FetchNext` cadence is paced by how fast the CLIENT
/// consumes pages (e.g. streaming rows into a slow downstream sink), not by
/// a single round-trip like an interactive tx's next `TxExecute`. 60 s gives
/// a consuming client a generous window between pages before the server
/// reclaims the pinned MVCC snapshot.
pub const DEFAULT_CURSOR_IDLE_TTL: Duration = Duration::from_secs(60);

/// Default sweep cadence for the cursor reaper task. Comfortably below the
/// idle TTL so a cursor that idles past its TTL is reaped within a few
/// seconds of becoming reapable. Mirrors
/// `crate::tx_registry::DEFAULT_REAPER_INTERVAL`.
pub const DEFAULT_CURSOR_REAPER_INTERVAL: Duration = Duration::from_secs(5);

/// Handle for the periodic cursor reaper task. Stop signal is the shared
/// root `shutdown_token` (mirrors `crate::tx_registry::ReaperTask`).
pub struct CursorReaperTask {
    pub handle: JoinHandle<()>,
}

/// Errors surfaced when driving a cursor handle through the registry.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum CursorRegistryError {
    /// The session already has `limit` cursors open.
    #[error("cursor limit exceeded (max: {limit})")]
    CursorLimitExceeded { limit: u32 },
    /// No open cursor for this id (never opened, explicitly canceled, or
    /// reaped for idle timeout — see `CursorExpired` for the latter when
    /// the registry can distinguish it).
    #[error("cursor not found")]
    CursorNotFound,
    /// The cursor id WAS issued and has since been reaped for sitting idle
    /// past its TTL — distinguishable from `CursorNotFound` (a "you waited
    /// too long" signal vs. "that id was never valid") via a short-lived
    /// tombstone (see [`CursorRegistry`] doc comment on `reaped_tombstones`).
    #[error("cursor expired (idle-timeout eviction)")]
    CursorExpired,
    /// The cursor exists but belongs to a different session (cross-session
    /// theft attempt — even the same user on another connection is
    /// rejected). Mirrors `TxRegistryError::TxOwnershipMismatch`.
    #[error("cursor does not belong to this session")]
    CursorOwnershipMismatch,
    /// R4b-1: the id is ALREADY open — ids are minted monotonically by
    /// `next_cursor_id()` in production, so an occupied slot means a caller
    /// bug (or a test reusing a live id), and the registry now REJECTS it
    /// instead of silently overwriting (an overwrite would orphan the
    /// previous cursor's mirror row + count contribution). Normal id reuse
    /// AFTER a `remove`/reap is unaffected — the slot is vacant then.
    #[error("cursor id already open")]
    CursorIdAlreadyOpen,
}

/// CR-A4 (#764): which coordinate system a cursor's bookmark resumes from,
/// decided ONCE at `create_cursor` time (see [`Cursor::new`]) from whether
/// the caller's query has a simple single-column ORDER BY — never
/// re-derived OPPORTUNISTICALLY per `FetchNext` call. Pinning the mode up
/// front closes a latent hazard where a later page could otherwise flip
/// coordinate systems mid-scroll (e.g. if a projection quirk made one
/// page's `seek_key` extraction fail, silently falling back to the
/// row-count `offset` bookmark for that page only) — a flip like that
/// could duplicate or skip rows, since the two bookmark kinds are not
/// interchangeable positions in the same scan.
///
/// CR-D1 (#782, release blocker): there IS one deliberate, PERMANENT
/// exception — `db_handler::cursor_handlers::fetch_next` flips a
/// `Keyset`-mode cursor to `Offset` for the rest of its lifetime when the
/// keyset retry ceiling genuinely cannot make progress (an ORDER BY tie
/// run larger than `max_cursor_page_size`; see
/// `cursor_handlers::KeysetOutcome::StuckAtCeiling`). This is safe and
/// exactly-once-correct because `CursorState::offset` is maintained in
/// parallel on the Keyset branch the whole time (it already reflects the
/// true count of rows returned so far, independent of which mode produced
/// them) — see `fetch_next`'s dispatch match for the full argument. This
/// differs qualitatively from the hazard the paragraph above rules out: a
/// single, detected-failure-condition transition, never an
/// opportunistic/repeated per-page re-derivation.
/// F-53b Step 4 (#880): which pagination coordinate system a cursor's
/// bookmark resumes from — see [`PaginationMode`]'s existing variants above
/// for the CR-A4/CR-D1 discipline this extends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PaginationMode {
    /// Boundary-filter seek on the single ORDER BY column (`seek_key` +
    /// `tie_skip`), CR-A4's inclusive-boundary + skip-past-ties scheme.
    Keyset,
    /// Plain row-count `offset`, used when the query has no ORDER BY (or
    /// not a simple single-column one) — no field to build a boundary
    /// filter on.
    Offset,
    /// F-53b Step 4 (#880): sorted-index keyset seek via `Pagination::After`,
    /// reaching `TableManager::read_as_of`'s AsOf-aware seek arm
    /// (`read_as_of_keyset_seek`, F-53b Step 2/#878) directly instead of the
    /// `Keyset` mode's boundary-filter + full-rescan scheme. Decided ONCE at
    /// `create_cursor` time (same CR-A4 discipline) by
    /// `db_handler::cursor_handlers::probe_index_seek_eligible`:
    /// `query.where.is_none()` AND a single simple-field ORDER BY AND a
    /// sorted index covers it — a narrower, real slice of cursor shapes than
    /// `Keyset` (see `try_plan_keyset_seek`'s shared `where.is_some()` guard,
    /// `read_planner.rs:472-525`, which permanently excludes any caller
    /// `WHERE` from this path).
    ///
    /// Falls back PERMANENTLY to `Offset` (mirrors CR-D1's `StuckAtCeiling ->
    /// Offset` transition exactly, same `force_offset_mode` flag/commit-
    /// ordering) the first time a `FetchNext`'s `stats.index_used` does not
    /// end in `"_asof_keyset"` — the load-bearing safety net proven by the
    /// F-53b Step 3 spike (`docs/dev-artifacts/research/
    /// f53b-step3-cursor-pagination-after-spike.md`, §1.4): a bare
    /// mode-agnostic "always send `Pagination::After`" scheme would silently
    /// re-return the SAME page once the gate declines mid-lifetime (no
    /// boundary filter to fall back on, unlike `Keyset` mode), so this
    /// fallback is mandatory, not optional polish.
    IndexSeek,
}

/// Mutable per-cursor pagination state, guarded by the cursor's
/// `tokio::sync::Mutex` (a `FetchNext` mutates it across the async engine
/// read call — the same across-`.await` lock pattern `InteractiveTx.ctx`
/// uses for `TxExecute`).
pub struct CursorState {
    /// The caller's original query this cursor scans. `db_handler::
    /// cursor_handlers` clones it per `FetchNext` call and overwrites the
    /// clone's `pagination`/`where`/`temporal` fields to express the
    /// current bookmark — this stored copy's `from`/`select`/`where`/
    /// `order_by` are the caller's original request and never mutated.
    pub query: ReadQuery,
    /// CR-A4 (#764): the pagination coordinate system this cursor was
    /// pinned to at `create_cursor` time — see [`PaginationMode`]. Decided
    /// once, never re-derived per `FetchNext`.
    pub mode: PaginationMode,
    /// Current keyset seek key — the last row's (single-column) ORDER BY
    /// value, used to build an INCLUSIVE `field >= seek_key` (or `<=` for
    /// DESC) boundary filter on the next `FetchNext` (CR-A4: was `>`/`<`,
    /// which silently dropped tied rows straddling a page boundary — see
    /// `db_handler::cursor_handlers::boundary_filter`). Only set when
    /// `mode == PaginationMode::Keyset`; `None` before the first
    /// `FetchNext` (the initial page has nothing to seek past yet).
    pub seek_key: Option<shamir_types::types::value::QueryValue>,
    /// CR-A4 (#764): how many rows EXACTLY equal to `seek_key` have
    /// already been returned to the client (across all prior pages, for
    /// the CURRENT run of ties ending at `seek_key`). Since the cursor's
    /// read path (`TableManager::read_as_of`) never attaches a record's
    /// `_id` to a projected row (confirmed: `_id` is only ever attached on
    /// the WRITE-result path and the Latest-temporal sorted-index seek
    /// path — `read_as_of` -> `apply_select_value_bytes` discards the
    /// `RecordId` outright — see this task's brief,
    /// `docs/dev-artifacts/prompts/post-alpha/11-cr-a4-keyset-tie-breaker.md`),
    /// a real `RecordId`-based "skip past last_id" is not available on
    /// this path. `tie_skip` is the substitute: since `list_stream`'s
    /// enumeration order is stable and `apply_order_by_qv`'s sort is
    /// stable (`Vec::sort_by`, a documented std guarantee) across two
    /// `read_as_of` calls at the SAME pinned version with no concurrent
    /// write, the Nth row (by return order) among rows tied at
    /// `seek_key` is deterministic — so "skip the first `tie_skip` rows
    /// equal to `seek_key` in the next fetch" reproduces exactly the same
    /// effect "skip past last_id" would if `_id` were available. `0` when
    /// there is no active tie run to skip past (i.e. `seek_key` is set but
    /// no page has ended mid-tie yet — the boundary row is included
    /// zero times, so nothing needs skipping).
    pub tie_skip: u64,
    /// Row-count bookmark used when `mode == PaginationMode::Offset`.
    /// Without a caller-specified total order there is no field to build a
    /// `Gte`/`Lte` boundary filter on, so `FetchNext` instead resumes via
    /// `Pagination::LimitOffset { offset, limit }` against the pinned
    /// snapshot's stable (but engine-internal, insertion-order) full scan.
    pub offset: u64,
    /// Whether the cursor has already reported `has_more == false`.  Once
    /// set, a further `FetchNext` returns `CursorNotFound`/closes the
    /// cursor rather than re-running an exhausted scan.
    pub exhausted: bool,
    /// F-53b Step 4 (#880): the last row's `RecordId`, for `IndexSeek`
    /// mode's `Pagination::after_with_id` tie-breaker. `None` before the
    /// first `FetchNext` (nothing to seek past yet) and whenever `mode !=
    /// IndexSeek`. Reuses the EXISTING `seek_key: Option<QueryValue>` field
    /// above for the ORDER BY value half of the bookmark (no new field
    /// needed there) — only the id half is new, because `IndexSeek` is the
    /// ONLY mode whose read path (`read_as_of_keyset_seek`) attaches a real
    /// `RecordId` to every row; `Keyset` mode's generic AsOf full-scan
    /// projection never does (see `tie_skip`'s doc comment above), which is
    /// exactly why `Keyset` needs a counting substitute instead of a plain
    /// id.
    pub after_id: Option<RecordId>,
}

/// R4b-1 activity metadata, stamped onto a [`Cursor`] at construction (see
/// [`Cursor::with_activity`]) and mirrored into [`CursorRegistry::activity`]
/// (as an `Arc` — the reader clones Arcs, never Strings) at every registry
/// lifecycle linearization point.
///
/// PRIVACY (brief D3/D4): ids, actor, db, TABLE NAME, pinned snapshot
/// version and a monotonic creation baseline ONLY — never a bearer token,
/// its 8-byte prefix, query literals, filter values or row data. This
/// struct is the entire data surface of the cursor activity view.
/// `pub(crate)`: the projection stays inside the server engine — the paging
/// worker (`db_handler::repo_admin::activity::list`) reads it via
/// `activity_snapshot`.
#[derive(Clone, Debug)]
pub(crate) struct CursorActivity {
    /// Minted non-secret session handle (`Session::activity_handle`, D3) —
    /// never the 32-byte sid, and nothing derived from it.
    pub session_handle: u64,
    /// STABLE directory-minted user id (16 bytes) — survives a dropped-and-
    /// recreated account with a reused name, so a later `scope: Own` filter
    /// keys on identity, not a mutable username. Never a credential.
    pub user_id: [u8; 16],
    /// Owning user's username (post-NFC display name; safe by D3).
    pub username: String,
    /// Database the cursor reads.
    pub db: String,
    /// Table NAME the cursor scans — a name only, never a query literal.
    pub table: String,
    /// MVCC version the cursor pinned at creation.
    pub pinned_version: u64,
    /// Monotonic construction baseline (the same `Instant::now()` family as
    /// `Cursor::created_at`) — `elapsed` for a row is `created.elapsed()` at
    /// scan time. Deliberately NOT a unix wall-clock (monotonic cannot go
    /// backwards under NTP/leap adjustments).
    pub created: Instant,
}

/// One entry of [`CursorRegistry::activity_snapshot`]: the mirror's owned
/// metadata behind an `Arc` plus its cursor id. Cheap to collect (one Arc
/// clone per row, zero String clones); the paging worker sorts by
/// `cursor_id`, renders at most `limit` rows, and derives `elapsed` as
/// `meta.created.elapsed()` — this module deliberately does NOT pre-sort
/// or pre-render every row.
/// `pub(crate)`: engine-internal, same reasoning as [`CursorActivity`].
#[derive(Clone, Debug)]
pub(crate) struct CursorActivitySnapshot {
    pub cursor_id: u64,
    pub meta: Arc<CursorActivity>,
}

/// A live server-side result cursor parked between `FetchNext` round-trips.
pub struct Cursor {
    /// Mutable pagination bookmark. `tokio::sync::Mutex` because
    /// `FetchNext` holds the guard across the async `TableManager::read`
    /// call (the sanctioned across-await lock, mirroring `InteractiveTx::ctx`).
    state: tokio::sync::Mutex<CursorState>,
    /// Pins the MVCC snapshot this cursor reads at for its whole lifetime.
    /// Held only for its `Drop`; released when the owning `Arc` drops
    /// (cancel/reap).
    _snapshot: SnapshotGuard,
    /// The version `_snapshot` pinned — every `FetchNext` reads
    /// `Temporal::AsOf { at: At::Version(pinned_version) }` at this value,
    /// giving snapshot-consistent pagination across the cursor's lifetime
    /// even though the caller's query said `Temporal::Latest`.
    pinned_version: u64,
    /// Default page size (from `CreateCursor`); a `FetchNext` may request a
    /// different `page_size` per call, so this is only the fallback.
    default_page_size: u32,
    /// Owning session id — the theft guard.
    owner_sid: [u8; 32],
    /// Database the cursor is pinned to.
    db: String,
    /// Repo the cursor is pinned to.
    repo: String,
    /// Construction baseline for idle/deadline nanos (mirrors `InteractiveTx`).
    created_at: Instant,
    /// Nanos since `created_at` of the last activity bump.
    last_activity_nanos: AtomicU64,
    /// F-20 (#813): number of `FetchNext` calls currently in flight against
    /// this cursor. Incremented by [`CursorRegistry::get_owned_for_fetch`]
    /// WHILE the registry's per-entry read-guard is still held, decremented
    /// by [`FetchLease`]'s `Drop`. See that method's doc comment for the
    /// ordering argument this whole mechanism rests on.
    in_flight: AtomicU32,
    /// Set by the winning terminal transition before it waits for `state`;
    /// fetches check this flag after acquiring the state lock.
    terminal: AtomicBool,
    /// R4b-1: safe activity metadata (see [`CursorActivity`]) stamped by the
    /// `CreateCursor` handler via [`Cursor::with_activity`]. `None` on
    /// cursors built without it (existing constructor callers, most tests) —
    /// the registry mirrors it only when present, so existing call sites
    /// keep compiling and simply produce no activity row.
    activity: Option<CursorActivity>,
}

impl Cursor {
    /// Build a parked cursor. `mode` is decided ONCE by the caller (see
    /// `db_handler::cursor_handlers::create_cursor`, CR-A4 #764) from
    /// whether `query` has a simple single-column ORDER BY, and never
    /// re-derived afterward.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        query: ReadQuery,
        mode: PaginationMode,
        snapshot: SnapshotGuard,
        pinned_version: u64,
        default_page_size: u32,
        owner_sid: [u8; 32],
        db: String,
        repo: String,
    ) -> Self {
        Self {
            state: tokio::sync::Mutex::new(CursorState {
                query,
                mode,
                seek_key: None,
                tie_skip: 0,
                offset: 0,
                exhausted: false,
                after_id: None,
            }),
            _snapshot: snapshot,
            pinned_version,
            default_page_size,
            owner_sid,
            db,
            repo,
            created_at: Instant::now(),
            last_activity_nanos: AtomicU64::new(0),
            in_flight: AtomicU32::new(0),
            terminal: AtomicBool::new(false),
            activity: None,
        }
    }

    /// R4b-1: attach safe activity metadata at construction time (builder
    /// style, called only by `db_handler::cursor_handlers::create_cursor` —
    /// existing `Cursor::new` callers are untouched). Must be called BEFORE
    /// [`CursorRegistry::register`] so the mirror is populated inside
    /// register (see `register`'s doc comment for the exact lock scope:
    /// the `scc` projection write happens under `open`'s existing shard
    /// write lock plus `HashIndex`'s own internal bucket lock — no
    /// second mutation pass, but not literally lock-free either).
    #[must_use]
    pub(crate) fn with_activity(mut self, activity: CursorActivity) -> Self {
        self.activity = Some(activity);
        self
    }

    /// The attached activity metadata, if any (read by the registry's
    /// mirror-population step inside `register`).
    pub(crate) fn activity(&self) -> Option<&CursorActivity> {
        self.activity.as_ref()
    }

    /// The mutable pagination bookmark — lock it to run a `FetchNext`.
    pub fn state(&self) -> &tokio::sync::Mutex<CursorState> {
        &self.state
    }

    /// Whether a terminal close has won for this cursor.
    pub(crate) fn is_terminal(&self) -> bool {
        self.terminal.load(Ordering::Acquire)
    }

    /// Claim the one terminal-close transition for this cursor.
    pub(crate) fn begin_terminal_close(&self) -> bool {
        self.terminal
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// The MVCC version every `FetchNext` reads at.
    pub fn pinned_version(&self) -> u64 {
        self.pinned_version
    }

    /// Default page size from `CreateCursor`.
    pub fn default_page_size(&self) -> u32 {
        self.default_page_size
    }

    /// Owning session id.
    pub fn owner_sid(&self) -> &[u8; 32] {
        &self.owner_sid
    }

    /// Database the cursor is pinned to.
    pub fn db(&self) -> &str {
        &self.db
    }

    /// Repo the cursor is pinned to.
    pub fn repo(&self) -> &str {
        &self.repo
    }

    /// Mark activity (call on each successful `FetchNext`) to defer the
    /// idle timeout. Single atomic store — no lock taken.
    pub fn bump_activity(&self) {
        let elapsed = self.created_at.elapsed().as_nanos() as u64;
        self.last_activity_nanos.store(elapsed, Ordering::Release);
    }

    /// Whether this cursor has been idle longer than `idle_ttl` as of `now`.
    /// Unlike `InteractiveTx::is_expired` a cursor has no separate absolute
    /// deadline — its whole lifetime budget IS the idle TTL (a cursor being
    /// actively paged through, however long that takes, is legitimate use;
    /// only genuine abandonment should reap it).
    pub fn is_expired(&self, now: Instant, idle_ttl: Duration) -> bool {
        let elapsed_now = now.saturating_duration_since(self.created_at).as_nanos() as u64;
        let last = self.last_activity_nanos.load(Ordering::Acquire);
        elapsed_now.saturating_sub(last) >= idle_ttl.as_nanos() as u64
    }
}

/// F-20 (#813): RAII lease marking a `FetchNext` in flight against a
/// cursor. Returned by [`CursorRegistry::get_owned_for_fetch`] and held for
/// the fetch's full duration (from lookup to completion, across every early
/// `return` in `db_handler::cursor_handlers::fetch_next`) — decremented on
/// `Drop`, so the cursor is marked contended for exactly as long as the
/// fetch is genuinely in progress, no matter which exit path it takes. See
/// `get_owned_for_fetch`'s doc comment for the ordering property this whole
/// mechanism rests on.
pub struct FetchLease(Arc<Cursor>);

impl Drop for FetchLease {
    fn drop(&mut self) {
        self.0.in_flight.fetch_sub(1, Ordering::AcqRel);
    }
}

mod reaper;
mod registry;

pub use reaper::spawn_reaper_task;
pub use registry::CursorRegistry;

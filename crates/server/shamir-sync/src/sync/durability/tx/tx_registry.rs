//! Phase B — interactive (multi-call) transaction registry.
//!
//! Parks a live [`shamir_tx::TxContext`] + its [`shamir_tx::SnapshotGuard`]
//! server-side between client round-trips, keyed by an opaque `tx_handle`
//! (the engine-minted `TxId`) and bound to the authenticated session. See
//! `docs/dev-artifacts/roadmap/PHASE_B_INTERACTIVE_TX.md` §4.
//!
//! **Concurrency.** The server layer already builds on `dashmap` /
//! `parking_lot` (unlike the engine, whose hot paths mandate `scc`); the
//! registry follows suit with [`dashmap::DashMap`]. The per-handle
//! [`tokio::sync::Mutex`] is the one across-`.await` lock — a `TxExecute`
//! mutates the parked `TxContext` across the async plan run — and its
//! contention is bounded to a single client serially driving its own handle
//! (one tx per session, enforced here).
//!
//! **Ownership across COMMIT.** `TxContext` is not `Clone`. To hand it to the
//! Phase-A commit pipeline, COMMIT/ROLLBACK [`Option::take`]s it out of the
//! shared `Arc` through the per-handle mutex, leaving `None`; any later call
//! on a taken (closed) handle observes `None` and is rejected. The owning
//! `Arc<InteractiveTx>` — and thus the `SnapshotGuard` it holds — is kept
//! alive by the caller until commit returns, so the MVCC snapshot stays
//! pinned through commit (SSI validation + history reads need it), then drops.
//!
//! **Commit observation (R4b-1).** COMMIT no longer removes the handle at
//! the start. [`TxRegistry::begin_commit`] transitions the `open` entry from
//! [`Parked::Open`] to [`Parked::Committing`] under the entry's own map-shard
//! guard (same discipline as register/remove), so the identity stays
//! RESERVED in the authoritative registry while the commit pipeline runs:
//! no concurrent `TxExecute`, re-register or reap can acquire it (the
//! executable paths and the reaper skip `Committing`), yet the activity row
//! stays visible as [`TxActivityState::AwaitingCommit`] until the returned
//! [`TxCommitGuard`] drops — commit completed or failed. The guard performs
//! the single authoritative removal (open entry + projection row) under the
//! same map-shard guard, so a re-register of the same handle can never race
//! between the two removals.
//!
//! **Activity mirror (R4b-1).** [`TxRegistry`] additionally projects a
//! read-only metadata row per handle into an [`scc::HashIndex`] ([`TxActivity`]).
//! The HashIndex is a read projection only — rows are created, transitioned
//! and removed exclusively by the registry's own lifecycle methods (register,
//! begin-commit, remove/reap); it never admits or decides anything on its
//! own. Readers iterate/peek it lock-free under EBR (`scc::Guard`) and touch
//! no `tokio::sync::Mutex`: the metadata is `Arc`-owned and its mutable
//! parts are atomics, so a reader can never observe a parked `TxExecute` or
//! a commit-in-progress through a lock. A momentarily torn snapshot (a row
//! just added/removed mid-scan) is acceptable and documented by the activity
//! view design (`docs/dev-artifacts/design/operational-views.md` §2.4).
//!
//! **Handle collisions (R4b-1).** Transaction ids are minted PER REPO
//! (per-repo `RepoTxGate::fresh_tx_id`), but this registry is a single
//! server-level table keyed globally by that id. Two concurrent transactions
//! in different repos can therefore mint the SAME handle. `register` fails
//! closed with [`TxRegistryError::TxHandleCollision`] instead of silently
//! overwriting (which is what `DashMap::insert` used to do — corrupting the
//! one-tx-per-session bookkeeping and the first transaction's identity).
//! Known limitation: the existing wiring keys the registry by the raw
//! per-repo `TxId`; a caller that hits the collision gets an explicit error
//! (`tx_handle_conflict`) rather than corrupted state. Making ids globally
//! unique is an engine-layer change outside this step. The collision
//! decision is made against the REAL `open` map only (the entry is
//! `Occupied`), never against the read-only projection: the projection
//! (an [`scc::HashIndex`]) admits nothing and decides nothing.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::mapref::entry::{Entry, OccupiedEntry};
use dashmap::DashMap;
use scc::{Guard, HashIndex};
use shamir_collections::THasher;
use shamir_tx::{SnapshotGuard, TxContext};

pub use super::tx_release_hook::TxAbortRelease;

// Public paths stay on `tx_registry` (the module was split at the 1000-line
// cap); the reaper implementation lives in `super::reaper`.
pub use super::reaper::{spawn_reaper_task, ReaperTask};

use super::release_tails::ReleaseTails;
use super::tx_abort_release::spawn_abort_release;

/// Phase B Stage 6 — default idle TTL for an open interactive tx.
/// Matches `TRANSACTIONS.md` (30 s) and `PHASE_B_INTERACTIVE_TX.md` §6.4.
///
/// The absolute lifetime cap is a separate constant
/// (`INTERACTIVE_TX_MAX_LIFETIME` in `db_handler.rs`, 5 min, mirroring
/// `shamir_engine::DEFAULT_MAX_TX_LIFETIME`). Both are checked by
/// [`InteractiveTx::is_expired`] and so by [`TxRegistry::expired_handles`].
pub const DEFAULT_INTERACTIVE_TX_IDLE_TTL: Duration = Duration::from_secs(30);

/// Phase B Stage 6 — default sweep cadence for the reaper task.
/// Comfortably below the 30 s idle TTL so a tx that idles past its TTL is
/// reaped within a few seconds of becoming reapable.
pub const DEFAULT_REAPER_INTERVAL: Duration = Duration::from_secs(5);

/// Errors surfaced when driving a handle through the registry.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum TxRegistryError {
    /// The session already owns an open interactive tx (one-tx-per-session).
    #[error("session already has an open transaction")]
    TxAlreadyOpen,
    /// No open tx for this handle (never opened, or already committed /
    /// rolled back / reaped).
    #[error("transaction handle not found")]
    TxNotFound,
    /// The handle exists but belongs to a different session — a
    /// cross-session-theft attempt (even the same user on another
    /// connection is rejected).
    #[error("transaction handle does not belong to this session")]
    TxOwnershipMismatch,
    /// The handle is already present (open or mid-commit). Tx ids are
    /// minted per repo, so two repos can collide on the same id; the
    /// registry fails closed rather than overwrite the existing row.
    #[error("transaction handle already in use (cross-repo tx id collision)")]
    TxHandleCollision,
}

/// Activity-view state of one registry row (operational-views §2.1,
/// Transactions). The `u8` encodings are stable so a later step can CAS on
/// the word; only `Active → AwaitingCommit` is produced today. Values 3..=
/// are reserved; do not reuse.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TxActivityState {
    /// Open and executable (`register` … `begin_commit`).
    Active = 0,
    /// Commit in progress: no longer in `open`, not executable, kept visible
    /// until the commit pipeline returns (success or failure).
    AwaitingCommit = 1,
    /// Expired (absolute deadline or idle TTL past, per the registry's
    /// reaper clock) but not yet reaped — REAL expiry, derived on read from
    /// the shared timing; the row is still registered, the reaper will take
    /// it on its next tick.
    ExpiredPendingReap = 2,
}

impl TxActivityState {
    fn from_u8(raw: u8) -> Option<Self> {
        match raw {
            0 => Some(Self::Active),
            1 => Some(Self::AwaitingCommit),
            2 => Some(Self::ExpiredPendingReap),
            _ => None,
        }
    }
}

/// Shared per-tx timing + lease state (R4b-1). One small `Arc` shared by the
/// parked [`InteractiveTx`] AND its projection row [`TxActivity`], so idle,
/// elapsed and the absolute deadline are computed from ONE monotonic
/// baseline and ONE activity clock — the mirror never duplicates a timestamp
/// that could drift from the reaper's view. Holds no `TxContext`, no
/// `SnapshotGuard`: a projection row never retains the snapshot past
/// teardown — when the tx drops, the mirror row only keeps these atomics
/// alive until the guard that removes it runs.
pub(crate) struct TxTiming {
    /// Construction baseline. All time-since-creation is computed against
    /// this `Instant` so `last_activity` and `deadline` are stored as `u64`
    /// nanos (cheap atomic load/store on the bump path).
    created_at: Instant,
    /// Nanos since `created_at` of the last activity bump.
    last_activity_nanos: AtomicU64,
    /// Interactive-lease count (idle-reap suspension).
    in_flight: AtomicUsize,
    /// Nanos since `created_at` for the absolute deadline. Set once and
    /// never mutated.
    deadline_nanos: u64,
}

impl TxTiming {
    fn new(deadline_nanos: u64) -> Self {
        Self {
            created_at: tokio::time::Instant::now().into_std(),
            last_activity_nanos: AtomicU64::new(0),
            in_flight: AtomicUsize::new(0),
            deadline_nanos,
        }
    }

    /// Nanos since `created_at` (monotonic — no wall clock, no regression).
    fn elapsed_ns(&self) -> u64 {
        tokio::time::Instant::now()
            .into_std()
            .saturating_duration_since(self.created_at)
            .as_nanos() as u64
    }

    /// Idle nanos: elapsed minus the last activity bump.
    fn idle_ns(&self) -> u64 {
        self.elapsed_ns()
            .saturating_sub(self.last_activity_nanos.load(Ordering::Acquire))
    }

    /// Mark activity without allowing concurrent refreshes to regress time.
    fn bump_activity(&self) {
        self.last_activity_nanos
            .fetch_max(self.elapsed_ns(), Ordering::Release);
    }

    /// Whether the tx is past its absolute deadline OR has been idle longer
    /// than `idle_ttl` as of `now`. The sweep reaps any tx that returns true.
    fn is_expired(&self, now: Instant, idle_ttl: Duration) -> bool {
        let elapsed_now = now.saturating_duration_since(self.created_at).as_nanos() as u64;
        if elapsed_now >= self.deadline_nanos {
            return true;
        }
        if self.in_flight.load(Ordering::Acquire) != 0 {
            return false;
        }
        let last = self.last_activity_nanos.load(Ordering::Acquire);
        elapsed_now.saturating_sub(last) >= idle_ttl.as_nanos() as u64
    }
}

/// Lock-free, credential-free metadata row for one interactive transaction.
///
/// `Arc`-shared from the registry's mirror ([`HashIndex`]) only: no
/// `TxContext`, no session id, no snapshot guard, no mutex — the reader path
/// touches only strings, ids and atomics, so it can never block or be
/// blocked by `TxExecute` (which parks on the ctx mutex) or by a commit
/// holding that mutex across the pipeline.
///
/// Privacy: rows carry ids, actor, db, timing and state only. The 32-byte
/// session bearer token and its 8-byte prefix NEVER appear here (D3); the
/// session is identified by the non-secret minted `session_handle`
/// (`Session::activity_handle()`).
pub struct TxActivity {
    /// The engine-minted tx handle (registry key).
    handle: u64,
    /// Minted non-secret session handle (`Session::activity_handle()`); 0
    /// means "not stamped" (rows built without session context).
    session_handle: u64,
    /// Owning user id (actor identity, not a credential; stable for the
    /// `scope: Own` filter for the row's whole life).
    user_id: [u8; 16],
    /// Owning username.
    username: Arc<str>,
    /// Database the tx is pinned to (scope for the activity view).
    db: Arc<str>,
    /// Lifecycle word; written only by the registry's lifecycle methods
    /// (`Release` published, `Acquire` read) so the reader never needs a
    /// lock. `Active` is refined to `ExpiredPendingReap` on read (see
    /// [`Self::state`]).
    state: std::sync::atomic::AtomicU8,
    /// Shared timing/lease state (single source for idle/elapsed/deadline).
    timing: Arc<TxTiming>,
    /// The reaper's idle TTL at construction time — the clock that decides
    /// `ExpiredPendingReap` on read. The registry stores one TTL and the
    /// reaper is launched with the same constant.
    idle_ttl: Duration,
}

impl TxActivity {
    /// Registry-internal constructor: rows are created only by the registry's
    /// own lifecycle methods (no independent admission).
    pub(crate) fn new(
        handle: u64,
        session_handle: u64,
        user_id: [u8; 16],
        username: Arc<str>,
        db: Arc<str>,
        timing: Arc<TxTiming>,
        idle_ttl: Duration,
    ) -> Self {
        Self {
            handle,
            session_handle,
            user_id,
            username,
            db,
            state: std::sync::atomic::AtomicU8::new(TxActivityState::Active as u8),
            timing,
            idle_ttl,
        }
    }

    /// The engine-minted tx handle.
    pub fn handle(&self) -> u64 {
        self.handle
    }

    /// The minted session handle (`Session::activity_handle()`); 0 = not
    /// stamped (rows built without session context).
    pub fn session_handle(&self) -> u64 {
        self.session_handle
    }

    /// Owning user id.
    pub fn user_id(&self) -> &[u8; 16] {
        &self.user_id
    }

    /// Owning username.
    pub fn username(&self) -> &str {
        &self.username
    }

    /// Database the tx is pinned to.
    pub fn db(&self) -> &str {
        &self.db
    }

    /// Current lifecycle state.
    ///
    /// `Active` is refined on read: a still-registered row whose absolute
    /// deadline or idle TTL (the reaper's clock) has passed reports
    /// [`TxActivityState::ExpiredPendingReap`] — real expiry, not a
    /// placeholder. Only [`TxRegistry::begin_commit`] writes the word
    /// itself; the reaper removes the row instead of transitioning it.
    pub fn state(&self) -> TxActivityState {
        match TxActivityState::from_u8(self.state.load(Ordering::Acquire)) {
            // The word is only ever written with the encodings above.
            None => unreachable!("tx activity state word holds a defined encoding"),
            Some(state @ TxActivityState::AwaitingCommit) => state,
            Some(TxActivityState::Active) => {
                if self
                    .timing
                    .is_expired(tokio::time::Instant::now().into_std(), self.idle_ttl)
                {
                    TxActivityState::ExpiredPendingReap
                } else {
                    TxActivityState::Active
                }
            }
            // Not produced by a write today; report as-is if a later step
            // starts producing it.
            Some(state) => state,
        }
    }

    /// Nanos since `created_at` (monotonic baseline — no wall clock, no
    /// regression). Same clock as [`InteractiveTx::remaining_lifetime`].
    pub fn elapsed_ns(&self) -> u64 {
        self.timing.elapsed_ns()
    }

    /// Idle nanos: time since `created_at` minus the last activity bump —
    /// exactly the clock the reaper consults, SHARED with the parked tx (no
    /// duplicate timestamp that could drift).
    pub fn idle_ns(&self) -> u64 {
        self.timing.idle_ns()
    }

    /// Transition `Active → AwaitingCommit`. Only `begin_commit` calls this,
    /// after the handle is gone from `open` — there is no competing writer.
    fn mark_awaiting_commit(&self) {
        self.state
            .store(TxActivityState::AwaitingCommit as u8, Ordering::Release);
    }
}

/// A live interactive transaction parked between client round-trips.
pub struct InteractiveTx {
    /// The live overlay, wrapped so COMMIT/ROLLBACK can `take()` ownership.
    /// `tokio::sync::Mutex` because `TxExecute` holds the guard across the
    /// async plan run (the sanctioned across-await lock).
    ctx: tokio::sync::Mutex<Option<TxContext>>,
    /// Releases the record locks this context may still hold when the last
    /// reference to it goes away outside the commit pipeline — see [`TxAbortRelease`].
    abort_release: TxAbortRelease,
    /// Pins the MVCC snapshot for GC. Held only for its `Drop`; released when
    /// the owning `Arc` drops (after commit/rollback/timeout removal).
    _snapshot: SnapshotGuard,
    /// Owning session id — the abort-on-disconnect key and theft guard.
    owner_sid: [u8; 32],
    /// Owning user id (informational / defence-in-depth).
    owner_user_id: [u8; 16],
    /// Database the handle is pinned to — every `TxExecute` must match.
    db: String,
    /// Repo the handle is pinned to — the engine tx commits against one repo.
    repo: String,
    /// Repository resolved at parking; commits target it, not a later name match.
    /// None in test constructors (by-name fallback).
    pub(super) bound_repo: Option<shamir_db::engine::repo::RepoInstance>,
    pub(super) release_tails: Option<Arc<ReleaseTails>>,

    timing: Arc<TxTiming>,
}

/// Keeps idle reaping suspended until the operation exits, including cancellation.
#[must_use]
pub struct TxLease {
    tx: Arc<InteractiveTx>,
}

impl Drop for TxLease {
    fn drop(&mut self) {
        self.tx.bump_activity();
        // Publish the refreshed timestamp before the last lease becomes idle.
        self.tx.timing.in_flight.fetch_sub(1, Ordering::AcqRel);
    }
}

impl InteractiveTx {
    /// Build a parked interactive tx. `max_lifetime` sets the absolute
    /// deadline (mirror Phase A's `DEFAULT_MAX_TX_LIFETIME`).
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        ctx: TxContext,
        snapshot: SnapshotGuard,
        owner_sid: [u8; 32],
        owner_user_id: [u8; 16],
        db: String,
        repo: String,
        max_lifetime: Duration,
        abort_release: TxAbortRelease,
    ) -> Self {
        Self {
            ctx: tokio::sync::Mutex::new(Some(ctx)),
            abort_release,
            _snapshot: snapshot,
            owner_sid,
            owner_user_id,
            db,
            repo,
            bound_repo: None,
            release_tails: None,
            timing: Arc::new(TxTiming::new(max_lifetime.as_nanos() as u64)),
        }
    }

    /// The shared timing/lease state (registry-internal: cloned into the
    /// projection row at registration).
    pub(crate) fn timing(&self) -> Arc<TxTiming> {
        Arc::clone(&self.timing)
    }

    /// The parked overlay. Lock it to run a `TxExecute` (`Some` → run; `None`
    /// → the handle was already committed/rolled back) or to `take()` the
    /// `TxContext` for COMMIT/ROLLBACK.
    pub fn ctx(&self) -> &tokio::sync::Mutex<Option<TxContext>> {
        &self.ctx
    }

    /// Owning session id — compare against the caller's `&Session` before
    /// touching the tx (theft guard).
    pub fn owner_sid(&self) -> &[u8; 32] {
        &self.owner_sid
    }

    /// Owning user id.
    pub fn owner_user_id(&self) -> &[u8; 16] {
        &self.owner_user_id
    }

    /// Database the handle is pinned to.
    pub fn db(&self) -> &str {
        &self.db
    }

    /// Repo the handle is pinned to (every `TxExecute` must target it).
    pub fn repo(&self) -> &str {
        &self.repo
    }

    /// The repo the parked commit must target, if bound at parking.
    pub fn bound_repo(&self) -> Option<&shamir_db::engine::repo::RepoInstance> {
        self.bound_repo.as_ref()
    }

    /// Bind the repo resolved at parking time (production begin path).
    #[must_use]
    pub fn with_bound_repo(mut self, repo: shamir_db::engine::repo::RepoInstance) -> Self {
        self.bound_repo = Some(repo);
        self
    }

    /// Remaining absolute lifetime on the registry's monotonic clock.
    /// Activity refreshes do not extend it; an expired budget saturates to zero.
    pub fn remaining_lifetime(&self) -> Duration {
        let elapsed = u128::from(self.timing.elapsed_ns());
        let remaining = u128::from(self.timing.deadline_nanos).saturating_sub(elapsed);
        Duration::from_nanos(
            u64::try_from(remaining).expect("remaining lifetime cannot exceed deadline_nanos"),
        )
    }

    /// Mark activity without allowing concurrent refreshes to regress time.
    pub fn bump_activity(&self) {
        self.timing.bump_activity();
    }

    /// Whether this tx is past its absolute deadline OR has been idle longer
    /// than `idle_ttl` as of `now`. The sweep reaps any tx that returns true.
    pub fn is_expired(&self, now: Instant, idle_ttl: Duration) -> bool {
        self.timing.is_expired(now, idle_ttl)
    }

    /// The releaser this transaction carries, used by the synchronous operator-cancel path.
    pub fn abort_release(&self) -> &TxAbortRelease {
        &self.abort_release
    }
}

impl Drop for InteractiveTx {
    /// The single release point for a transaction that ended outside the commit
    /// pipeline — rollback, the reaper, session teardown, an execution-limit
    /// abort, or a request that loses its race with them. The context is parked
    /// here only when no commit path took it; `get_mut` needs no lock because
    /// this is the last reference, so nobody holds the guard.
    fn drop(&mut self) {
        if let Some(ctx) = self.ctx.get_mut().take() {
            let tails = self.release_tails.as_deref();
            spawn_abort_release(&self.abort_release, tails, ctx, &self.db, &self.repo);
        }
    }
}

/// `Committing` reserves the key against re-registration and executable paths;
/// only [`TxCommitGuard::drop`] removes it under the shard guard.
pub(super) enum Parked {
    Open(Arc<InteractiveTx>),
    Committing(Arc<InteractiveTx>),
}

/// The server-resident table of open interactive transactions.
///
/// `open` maps `tx_handle → Parked` (an [`Parked::Open`] parked
/// transaction, or [`Parked::Committing`] — the identity reserved mid-commit
/// under the same map guard); `by_session` enforces the
/// one-tx-per-session invariant (`session_id → tx_handle`). Both are
/// `dashmap` (lock-free-enough for the server layer, no `RwLock` poisoning
/// surfaced to callers). `activity` is the R4b-1 read projection
/// ([`TxActivity`]) into a lock-free `scc::HashIndex` (EBR) — it is written
/// ONLY by the lifecycle methods below, never admits a row on its own, and
/// is the source the activity view reads.
///
/// **Admission ordering (race discipline).** `register` holds the `open`
/// vacant-entry shard guard while it inserts the projection row, inserts the
/// `open` entry and takes the session slot, and bumps BOTH gauge mirrors
/// BEFORE the guard is released — so a mirror count can never lag a visible
/// row and a concurrent removal can never observe the increment-before-row
/// half-state (no `fetch_sub` on a zeroed mirror). A duplicate handle is
/// rejected inside the guard against the REAL `open` map (`Occupied` —
/// `Committing` counts), never against the projection; the projection is
/// write-only admission output and read-only telemetry input.
/// Every removal path (remove, remove_expired, the commit guard) holds the
/// `open` ENTRY guard for its whole body — `open` removal and projection-row
/// drop happen under one shard guard — so a racing re-register of the SAME
/// id cannot slip between the two removals and have the stale projection
/// drop delete its fresh row. No admission decision reads the projection.
#[derive(Default)]
pub struct TxRegistry {
    pub(super) open: DashMap<u64, Parked, THasher>,
    by_session: DashMap<[u8; 32], u64, THasher>,
    activity: HashIndex<u64, Arc<TxActivity>, THasher>,
    /// Gauge mirror for `open` rows — `shamir_active_transactions` reads
    /// THIS, never `DashMap::len`. Maintained on EVERY mutation path:
    /// register, remove, remove_expired (reap) and begin_commit.
    open_mirror: AtomicUsize,
    /// Gauge mirror for projection rows (includes `AwaitingCommit`).
    activity_mirror: AtomicUsize,
    pub(super) release_tails: Option<Arc<ReleaseTails>>,
}

impl TxRegistry {
    /// Register a freshly-opened interactive tx under `handle` (no session
    /// context: the projection row carries an unstamped session handle and
    /// an empty username). See [`Self::register_activity`] for the
    /// session-attributed form the handler uses.
    ///
    /// Enforces one-tx-per-session: if the owning session already has an open
    /// tx, returns [`TxRegistryError::TxAlreadyOpen`] and the passed `it` is
    /// dropped (RAII rollback of the just-opened, unused tx). Returns the
    /// shared `Arc` on success.
    pub fn register(
        &self,
        handle: u64,
        it: InteractiveTx,
    ) -> Result<Arc<InteractiveTx>, TxRegistryError> {
        self.register_activity(handle, it, 0, Arc::from(""))
    }

    /// Register with full activity attribution: `session_handle` is the
    /// minted non-secret handle (`Session::activity_handle()`) and
    /// `username` the owning actor's name. Both land in the projection row
    /// verbatim; the secret session id never does.
    ///
    /// Duplicate handles are REJECTED, not replaced: tx ids are minted per
    /// repo, so two repos can collide on one id; the registry fails closed
    /// ([`TxRegistryError::TxHandleCollision`]) rather than silently
    /// corrupting the first transaction's row.
    pub fn register_activity(
        &self,
        handle: u64,
        it: InteractiveTx,
        session_handle: u64,
        username: Arc<str>,
    ) -> Result<Arc<InteractiveTx>, TxRegistryError> {
        let owner_sid = it.owner_sid;
        let db: Arc<str> = Arc::from(it.db());
        let user_id = it.owner_user_id;
        let timing = it.timing();
        // The `open` vacant guard below excludes reaper/removal for this
        // handle for the whole admission sequence.
        match self.open.entry(handle) {
            // The REAL `open` map is the only admission authority: any
            // occupied entry — an open tx or a `Committing` reservation —
            // rejects the duplicate. The read-only projection is never
            // consulted (it cannot admit or decide).
            Entry::Occupied(_) => Err(TxRegistryError::TxHandleCollision),
            Entry::Vacant(slot) => {
                match self.by_session.entry(owner_sid) {
                    // Session already drives a tx — reject (the new `it`
                    // drops here).
                    Entry::Occupied(_) => Err(TxRegistryError::TxAlreadyOpen),
                    Entry::Vacant(session_slot) => {
                        // Bind the registry's release-tail owner HERE, at
                        // registration (no caller-managed binding): every
                        // parked transaction carries the owner its `Drop`
                        // release tails drain through shutdown.
                        let mut it = it;
                        it.release_tails = self.release_tails.clone();
                        let activity = TxActivity::new(
                            handle,
                            session_handle,
                            user_id,
                            username,
                            db,
                            timing,
                            DEFAULT_INTERACTIVE_TX_IDLE_TTL,
                        );
                        // Count + publish while the `open` vacant guard is
                        // held, so no removal runs between and no reader sees
                        // an uncounted row.
                        if self
                            .activity
                            .insert_sync(handle, Arc::new(activity))
                            .is_err()
                        {
                            return Err(TxRegistryError::TxHandleCollision);
                        }
                        let arc = Arc::new(it);
                        self.open_mirror.fetch_add(1, Ordering::AcqRel);
                        self.activity_mirror.fetch_add(1, Ordering::AcqRel);
                        slot.insert(Parked::Open(Arc::clone(&arc)));
                        session_slot.insert(handle);
                        Ok(arc)
                    }
                }
            }
        }
    }

    /// Look up a handle, verifying it belongs to `sid`. Clones the `Arc` out
    /// (the `DashMap` ref is dropped before returning — never held across the
    /// caller's subsequent `.await`). A `Committing` reservation (identity
    /// held by the commit pipeline) is not executable: `TxNotFound`.
    pub fn get_owned(
        &self,
        handle: u64,
        sid: &[u8; 32],
    ) -> Result<Arc<InteractiveTx>, TxRegistryError> {
        let arc = match self.open.get(&handle) {
            Some(entry) => match &*entry {
                Parked::Open(arc) => Arc::clone(arc),
                // Committing (or absent): not executable either way.
                Parked::Committing(_) => return Err(TxRegistryError::TxNotFound),
            },
            None => return Err(TxRegistryError::TxNotFound),
        };
        if &arc.owner_sid != sid {
            return Err(TxRegistryError::TxOwnershipMismatch);
        }
        Ok(arc)
    }

    /// Acquire idle protection while the map reference still excludes removal.
    /// No map guard escapes; a `Committing` reservation is not acquirable.
    pub fn acquire_owned(
        &self,
        handle: u64,
        sid: &[u8; 32],
    ) -> Result<(Arc<InteractiveTx>, TxLease), TxRegistryError> {
        let entry = self.open.get(&handle).ok_or(TxRegistryError::TxNotFound)?;
        let Parked::Open(tx) = entry.value() else {
            return Err(TxRegistryError::TxNotFound);
        };
        if &tx.owner_sid != sid {
            return Err(TxRegistryError::TxOwnershipMismatch);
        }
        // Increment the lease while the map guard excludes removal.
        tx.timing
            .in_flight
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                count.checked_add(1)
            })
            .expect("interactive transaction lease count overflow");
        tx.bump_activity();
        let tx = Arc::clone(tx);
        let lease = TxLease {
            tx: Arc::clone(&tx),
        };
        Ok((tx, lease))
    }

    /// Remove a handle (ROLLBACK / reap). Also frees the session slot so
    /// the session can open a new tx. Returns the `Arc` so the caller can
    /// `take()` the `TxContext` and keep the `SnapshotGuard` alive until it
    /// is done with it. A `Committing` reservation is left alone (the
    /// [`TxCommitGuard`] owns its teardown) and yields `None`.
    ///
    /// The whole removal runs under the `open` entry's shard guard, so a
    /// racing re-register of the SAME handle cannot slip between the two
    /// table mutations.
    pub fn remove(&self, handle: u64) -> Option<Arc<InteractiveTx>> {
        match self.open.entry(handle) {
            Entry::Occupied(slot) if matches!(slot.get(), Parked::Open(_)) => {
                Some(self.teardown_open(slot, handle))
            }
            _ => None,
        }
    }

    /// Identity-checked removal for the handler's stale-failure / rollback
    /// paths: remove `handle` ONLY if it still parks the very
    /// [`Arc<InteractiveTx>`] the caller verified via [`Self::get_owned`] /
    /// [`Self::acquire_owned`] (`Arc::ptr_eq`, the same discipline as the
    /// reaper). If the reaper already removed that tx and a new one was
    /// re-registered under the same per-repo id, this refuses (`None`) and
    /// the fresh transaction survives.
    pub fn remove_owned(
        &self,
        handle: u64,
        expected: &Arc<InteractiveTx>,
    ) -> Option<Arc<InteractiveTx>> {
        match self.open.entry(handle) {
            Entry::Occupied(slot) => {
                if matches!(slot.get(), Parked::Open(arc) if Arc::ptr_eq(arc, expected)) {
                    return Some(self.teardown_open(slot, handle));
                }
                None
            }
            Entry::Vacant(_) => None,
        }
    }

    /// The single removal teardown: open entry, open-mirror decrement,
    /// session slot and projection row, ALL while the `open` entry's shard
    /// guard (`slot`) is still held — a racing re-register of the same
    /// handle cannot slip between the two table mutations. Callers must
    /// have verified the variant (and, where required, identity/expiry).
    pub(super) fn teardown_open(
        &self,
        slot: OccupiedEntry<'_, u64, Parked>,
        handle: u64,
    ) -> Arc<InteractiveTx> {
        let Parked::Open(arc) = slot.get() else {
            unreachable!("teardown_open is only called on Parked::Open entries")
        };
        let arc = Arc::clone(arc);
        self.open_mirror.fetch_sub(1, Ordering::AcqRel);
        // Free the one-tx-per-session slot (guard against a stale pointer).
        self.by_session
            .remove_if(&arc.owner_sid, |_, h| *h == handle);
        if self.activity.remove_sync(&handle) {
            self.activity_mirror.fetch_sub(1, Ordering::AcqRel);
        }
        // Remove the `open` row LAST, still under the entry guard.
        slot.remove_entry();
        arc
    }

    /// Remove the transaction owned by `sid` during session teardown.
    ///
    /// The session-index guard is dropped before removing the handle. The
    /// caller drops the returned `Arc` to trigger RAII rollback.
    pub fn remove_for_session(&self, sid: &[u8; 32]) -> Option<Arc<InteractiveTx>> {
        let handle = *self.by_session.get(sid)?.value();
        match self.open.entry(handle) {
            Entry::Occupied(slot) if matches!(slot.get(), Parked::Open(arc) if &arc.owner_sid == sid) => {
                Some(self.teardown_open(slot, handle))
            }
            _ => None,
        }
    }

    /// Handles whose tx is past its absolute deadline or idle past `idle_ttl`
    /// as of `now`. The background sweep removes each (drop = RAII abort).
    /// `Committing` reservations are skipped — the commit pipeline owns them.
    pub fn expired_handles(&self, now: Instant, idle_ttl: Duration) -> Vec<u64> {
        self.open
            .iter()
            .filter_map(|e| match e.value() {
                Parked::Open(tx) if tx.is_expired(now, idle_ttl) => Some(*e.key()),
                _ => None,
            })
            .collect()
    }

    /// Recheck a sweep candidate under the same shard lock that excludes
    /// acquisition, revalidating identity (`Arc::ptr_eq`) and expiry before
    /// removing. `Committing` reservations and replaced handles yield `None`.
    pub fn remove_expired(
        &self,
        handle: u64,
        expected: &Arc<InteractiveTx>,
        idle_ttl: Duration,
    ) -> Option<Arc<InteractiveTx>> {
        match self.open.entry(handle) {
            Entry::Occupied(slot) => {
                if let Parked::Open(tx) = slot.get() {
                    if Arc::ptr_eq(tx, expected)
                        && tx.is_expired(tokio::time::Instant::now().into_std(), idle_ttl)
                    {
                        return Some(self.teardown_open(slot, handle));
                    }
                }
                None
            }
            Entry::Vacant(_) => None,
        }
    }

    /// Remove EVERY parked transaction (shutdown teardown). `Committing`
    /// reservations are left alone — the commit pipeline owns them. Callers
    /// drop each returned `Arc` to trigger the RAII release, so a shutdown
    /// that empties the registry BEFORE closing release-tail admission gets
    /// every lingering transaction's `Drop` release REGISTERED (trackable,
    /// drainable) instead of refused by a closed owner.
    pub fn remove_all(&self) -> Vec<Arc<InteractiveTx>> {
        let handles: Vec<u64> = self
            .open
            .iter()
            .filter_map(|entry| match entry.value() {
                Parked::Open(_) => Some(*entry.key()),
                _ => None,
            })
            .collect();
        handles.into_iter().filter_map(|h| self.remove(h)).collect()
    }

    /// Reap expired entries, revalidating identity and expiry at removal.
    pub fn reap_expired(&self, idle_ttl: Duration) -> usize {
        let now = tokio::time::Instant::now().into_std();
        let candidates: Vec<_> = self
            .open
            .iter()
            .filter_map(|entry| match entry.value() {
                Parked::Open(tx) if tx.is_expired(now, idle_ttl) => {
                    Some((*entry.key(), Arc::clone(tx)))
                }
                // Committing reservations belong to the commit pipeline.
                _ => None,
            })
            .collect();
        candidates
            .into_iter()
            .filter(|(handle, tx)| self.remove_expired(*handle, tx, idle_ttl).is_some())
            .count()
    }

    /// Begin a COMMIT for `handle`, where `expected` is the `Arc` the caller
    /// obtained via [`Self::get_owned`] (identity-rechecked under the entry
    /// guard, the same discipline as the reaper).
    ///
    /// `Committing` reserves the identity: executable paths and reaping skip it.
    /// Activity remains visible as AwaitingCommit until the guard drops. The guard
    /// removes the entry and projection together under its shard guard; `None`
    /// means the row was reaped. It survives cancellation of the commit future.
    pub fn begin_commit(
        self: &Arc<Self>,
        handle: u64,
        expected: &Arc<InteractiveTx>,
    ) -> Option<TxCommitGuard> {
        match self.open.entry(handle) {
            Entry::Occupied(mut slot) => {
                if !matches!(slot.get(), Parked::Open(arc) if Arc::ptr_eq(arc, expected)) {
                    return None;
                }
                *slot.get_mut() = Parked::Committing(Arc::clone(expected));
                self.open_mirror.fetch_sub(1, Ordering::AcqRel);
                self.by_session
                    .remove_if(&expected.owner_sid, |_, h| *h == handle);
                let _ = self
                    .activity
                    .peek_with(&handle, |_, meta| meta.mark_awaiting_commit());
                Some(TxCommitGuard {
                    registry: Arc::clone(self),
                    handle,
                    identity: Arc::clone(expected),
                })
            }
            Entry::Vacant(_) => None,
        }
    }

    /// The projection row for `handle`, if present. Clone of the shared
    /// `Arc` only — no lock, no string copy.
    pub fn activity_row(&self, handle: u64) -> Option<Arc<TxActivity>> {
        let mut row = None;
        let _ = self
            .activity
            .peek_with(&handle, |_, meta| row = Some(Arc::clone(meta)));
        row
    }

    /// Callback-based scan over all projection rows under one EBR guard.
    /// The callback returns `false` to stop the scan. Rows are visited in
    /// arbitrary shard order; each row is an `&TxActivity` valid only for the
    /// call. This is the paging worker's access path: it can sort KEYS (u64)
    /// and render lazily, cloning nothing but the `Arc`s it actually emits.
    pub fn for_each_activity(&self, mut visit: impl FnMut(&TxActivity) -> bool) {
        let guard = Guard::new();
        for (_, meta) in self.activity.iter(&guard) {
            if !visit(meta) {
                break;
            }
        }
    }

    /// Key+`Arc` tuple snapshot for the paging worker: u64 keys and shared
    /// `Arc` clones ONLY (no string copies, no sorting here — the worker
    /// sorts keys and renders lazily). Lock-free under one EBR guard;
    /// momentarily torn across concurrent mutations by design (§2.4).
    pub fn activity_rows(&self) -> Vec<(u64, Arc<TxActivity>)> {
        let guard = Guard::new();
        self.activity
            .iter(&guard)
            .map(|(key, row)| (*key, Arc::clone(row)))
            .collect()
    }

    /// Rows visible to the activity view, INCLUDING `AwaitingCommit` commit
    /// rows (which are no longer in `open`). Always ≥ [`Self::len`]; the
    /// difference is the number of commits currently in flight. Reads the
    /// mirror, never `HashIndex::len`.
    pub fn activity_count(&self) -> usize {
        self.activity_mirror.load(Ordering::Acquire)
    }

    /// Number of open interactive transactions (committing rows NOT
    /// included — they left `open` at `begin_commit`). Reads the mirror,
    /// never `DashMap::len`.
    pub fn len(&self) -> usize {
        self.open_mirror.load(Ordering::Acquire)
    }

    /// Whether no interactive transactions are open.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Completes the commit observation: dropping the guard removes the
/// `Committing` `open` reservation AND the projection row — both under the
/// The guard owns teardown under the entry shard guard; it survives a
/// cancelled commit future and cannot tear down a fresh reservation.
pub struct TxCommitGuard {
    registry: Arc<TxRegistry>,
    handle: u64,
    identity: Arc<InteractiveTx>,
}

impl Drop for TxCommitGuard {
    fn drop(&mut self) {
        if let Entry::Occupied(slot) = self.registry.open.entry(self.handle) {
            if matches!(slot.get(), Parked::Committing(tx) if Arc::ptr_eq(tx, &self.identity)) {
                if self.registry.activity.remove_sync(&self.handle) {
                    self.registry.activity_mirror.fetch_sub(1, Ordering::AcqRel);
                }
                // Remove the `open` reservation LAST, still under the entry
                // guard.
                slot.remove_entry();
            }
        }
    }
}

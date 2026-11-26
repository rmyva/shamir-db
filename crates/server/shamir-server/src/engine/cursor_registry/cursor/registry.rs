use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use scc::{Guard, HashIndex};
use shamir_collections::THasher;

use super::{Cursor, CursorActivity, CursorActivitySnapshot, CursorRegistryError, FetchLease};

#[cfg(test)]
type RemovePause = (u64, Arc<tokio::sync::Barrier>, Arc<tokio::sync::Barrier>);

/// The server-resident table of open result cursors.
///
/// `open` maps `cursor_id → Cursor`; `by_session` tracks a LIVE per-session
/// open-cursor COUNT (`Arc<AtomicUsize>`), not a `Vec<u64>` — an O(1)
/// cardinality check on the hot `register`/`remove` path, per CLAUDE.md's
/// O(x→0) pillar (a `.len()` on a per-session `Vec` would be banned only if
/// scc were in play here, but the intent — never materialise-then-count —
/// applies equally to `dashmap`).
///
/// `reaped_tombstones` is a short-lived marker set: `sweep_and_reap`
/// inserts the id here (with a bounded self-expiry sweep alongside the
/// regular cursor sweep) so a `FetchNext` against a JUST-reaped id can
/// report `CursorExpired` instead of the less-specific `CursorNotFound` —
/// distinguishing "you waited too long" from "that id was never valid".
/// Explicit `CancelCursor` does NOT tombstone (canceling is a deliberate,
/// successful close — not an error condition a later fetch should complain
/// about differently).
#[derive(Default)]
pub struct CursorRegistry {
    open: DashMap<u64, Arc<Cursor>, THasher>,
    by_session: DashMap<[u8; 32], Arc<AtomicUsize>, THasher>,
    reaped_tombstones: DashMap<u64, Instant, THasher>,
    /// R4b-1: non-authoritative lock-free-reader projection of safe cursor
    /// metadata keyed by cursor id (see [`CursorActivity`]). Written ONLY at
    /// the existing lifecycle linearization points (`register`, `remove`,
    /// `sweep_and_reap`), so no ordering is added or changed for
    /// `get_owned_for_fetch`/`reap`; read ONLY via `HashIndex::iter` under
    /// an epoch `Guard`, which takes no lock a writer takes (unlike a
    /// `DashMap::iter`, which holds shard read locks — the reason scc's
    /// `HashIndex` was chosen per the brief).
    activity: HashIndex<u64, Arc<CursorActivity>, THasher>,
    /// R4b-1: O(1) count mirror next to `open`. `len()`/`is_empty` read
    /// THIS, never `DashMap::len()` — the gauge feed per the brief (mirrors,
    /// no scan).
    count: AtomicUsize,
    #[cfg(test)]
    remove_pause: std::sync::Mutex<Option<RemovePause>>,
}

/// How long a reaped-for-idle cursor id stays tombstoned (so a `FetchNext`
/// racing the reaper still gets `CursorExpired` rather than
/// `CursorNotFound`). Generous relative to the reaper's own sweep interval
/// so a client's in-flight `FetchNext` at the moment of eviction reliably
/// observes the more specific error.
const TOMBSTONE_TTL: Duration = Duration::from_secs(300);

impl CursorRegistry {
    #[cfg(test)]
    pub(crate) fn with_remove_pause(
        &self,
        id: u64,
        reached: Arc<tokio::sync::Barrier>,
        resume: Arc<tokio::sync::Barrier>,
    ) {
        *self.remove_pause.lock().unwrap() = Some((id, reached, resume));
    }

    #[cfg(test)]
    pub(crate) async fn pause_before_remove(&self, id: u64) {
        let pause = {
            let mut configured = self.remove_pause.lock().unwrap();
            if configured
                .as_ref()
                .is_some_and(|(configured_id, _, _)| *configured_id == id)
            {
                configured.take()
            } else {
                None
            }
        };
        if let Some((_, reached, resume)) = pause {
            reached.wait().await;
            resume.wait().await;
        }
    }

    pub fn new() -> Self {
        Self::default()
    }

    /// Register a freshly-opened cursor under `cursor_id`, enforcing the
    /// per-session cap.
    ///
    /// On success increments the session's live open-count. On rejection
    /// the just-built `cursor` (and the `SnapshotGuard`/table handle it
    /// holds) is dropped — RAII release of the just-opened, unused
    /// resource — mirroring `TxRegistry::register`'s `TxAlreadyOpen` path.
    ///
    /// Writer locks taken by this method (in order):
    /// 1. `by_session`'s shard write lock (`dashmap::entry`) for the
    ///    session's counter, held across the cap CAS loop and dropped
    ///    before the insert below (see the inline comment for the
    ///    `free_session_slot` ordering argument).
    /// 2. `open`'s shard write lock (`dashmap::entry` vacant guard) for
    ///    `cursor_id`, held while the activity projection is written via
    ///    `HashIndex::insert_sync` — that call takes AND holds `HashIndex`'s
    ///    own internal bucket lock for the key — and the count mirror is
    ///    bumped. No NEW lock class is introduced (the projection write is
    ///    nested inside a lock `insert` already took), but it is a second,
    ///    real lock acquisition, not a lock-free append.
    pub fn register(
        &self,
        cursor_id: u64,
        owner_sid: [u8; 32],
        cursor: Cursor,
        max_per_session: u32,
    ) -> Result<Arc<Cursor>, CursorRegistryError> {
        let slot = self
            .by_session
            .entry(owner_sid)
            .or_insert_with(|| Arc::new(AtomicUsize::new(0)));
        let counter = Arc::clone(slot.value());

        // #1073 (F-1): the CAS loop runs WHILE still holding `slot` — the
        // shard write lock `dashmap`'s `entry()` took for `owner_sid`. This
        // is the SAME shard write lock `free_session_slot`'s `remove_if`
        // takes (see that method's doc comment for the `dashmap` internals
        // this rests on), so the increment below is now atomic relative to
        // a concurrent `remove()` of the last cursor on this session:
        // either `free_session_slot` runs entirely before this `entry()`
        // call (this loop then correctly starts a fresh `AtomicUsize(0)`
        // via `or_insert_with`), or entirely after `slot` is dropped below
        // (it observes the post-increment, non-zero count and does not
        // remove the entry). There is no window where this loop can bump
        // an `Arc<AtomicUsize>` that `free_session_slot` has already (or is
        // about to) orphan or `fetch_sub` past zero — closing both failure
        // scenarios in this task's description: cap overrun via two
        // independently-created counters, and a permanent lock-out via
        // `fetch_sub` wrapping a zeroed/removed counter to `usize::MAX`.
        //
        // Holding the shard lock across the loop body is safe: nothing
        // else in this method touches `by_session` again before `slot`
        // drops, so there is no reentrancy risk, and the loop itself does
        // not block on anything external — with the lock held, no other
        // thread can mutate this SAME counter concurrently, so in practice
        // the `compare_exchange` below succeeds on its first attempt.
        loop {
            let cur = counter.load(Ordering::Acquire);
            if cur >= max_per_session as usize {
                // `cursor` (the just-built Cursor + SnapshotGuard) drops
                // here — RAII release, same pattern as TxAlreadyOpen.
                drop(slot);
                return Err(CursorRegistryError::CursorLimitExceeded {
                    limit: max_per_session,
                });
            }
            if counter
                .compare_exchange(cur, cur + 1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                break;
            }
        }
        drop(slot);

        let arc = Arc::new(cursor);
        // R4b-1: populate the activity mirror and the count mirror BEFORE
        // the authoritative entry becomes visible, UNDER `open`'s shard
        // write lock (the `entry()` vacant guard). `remove`/`sweep_and_reap`
        // tear the mirror + count down under the SAME shard lock (see their
        // doc comments), so the pair (publish, mirror+count) is atomic per
        // shard: a teardown for this id can only observe the entry strictly
        // after its mirror row and count contribution exist — no ghost
        // mirror row for an already-removed cursor, no mirror-count
        // underflow. No lock class is added: this is the lock `insert`
        // already took, held marginally longer (two atomic ops + one
        // `HashIndex::insert_sync`, none of which block on external I/O).
        match self.open.entry(cursor_id) {
            dashmap::mapref::entry::Entry::Vacant(vacant) => {
                if let Some(a) = arc.activity() {
                    // `insert_sync` REJECTS a duplicate key (Err) instead of
                    // overwriting — defense in depth alongside the `Vacant`
                    // match above: an id that somehow already carries a
                    // mirror row belongs to a cursor this call must not
                    // clobber (see `CursorIdAlreadyOpen`).
                    if self
                        .activity
                        .insert_sync(cursor_id, Arc::new(a.clone()))
                        .is_err()
                    {
                        self.free_session_slot(&arc.owner_sid);
                        return Err(CursorRegistryError::CursorIdAlreadyOpen);
                    }
                }
                self.count.fetch_add(1, Ordering::AcqRel);
                // Clear any stale tombstone for a reused id (ids are u64-
                // minted monotonically by the caller in production; tests
                // may reuse small ids, so keep this registry-side invariant
                // tight regardless).
                self.reaped_tombstones.remove(&cursor_id);
                vacant.insert(Arc::clone(&arc));
            }
            // Deliberate behavior change (R4b-1): the previous
            // `self.open.insert(...)` silently OVERWROTE an occupied id,
            // orphaning the old cursor's mirror row, count contribution and
            // session slot. Ids are minted monotonically in production, so
            // an Occupied slot is a caller bug — reject it closed. Normal
            // id reuse AFTER `remove`/reap takes the Vacant arm unchanged.
            dashmap::mapref::entry::Entry::Occupied(_) => {
                self.free_session_slot(&arc.owner_sid);
                return Err(CursorRegistryError::CursorIdAlreadyOpen);
            }
        }
        Ok(arc)
    }

    /// Look up a cursor, verifying it belongs to `sid`.
    ///
    /// Returns [`CursorRegistryError::CursorExpired`] when `cursor_id` was
    /// reaped for idle-timeout within the tombstone window,
    /// [`CursorRegistryError::CursorNotFound`] when it was never issued (or
    /// its tombstone has aged out), and
    /// [`CursorRegistryError::CursorOwnershipMismatch`] on a cross-session
    /// lookup.
    pub fn get_owned(
        &self,
        cursor_id: u64,
        sid: &[u8; 32],
    ) -> Result<Arc<Cursor>, CursorRegistryError> {
        let arc = match self.open.get(&cursor_id) {
            Some(r) => Arc::clone(r.value()),
            None => {
                return Err(if self.reaped_tombstones.contains_key(&cursor_id) {
                    CursorRegistryError::CursorExpired
                } else {
                    CursorRegistryError::CursorNotFound
                });
            }
        };
        if &arc.owner_sid != sid {
            return Err(CursorRegistryError::CursorOwnershipMismatch);
        }
        Ok(arc)
    }

    /// F-20 (#813): like [`Self::get_owned`], but additionally marks the
    /// cursor "in flight" for the lifetime of the returned [`FetchLease`] —
    /// used ONLY by `fetch_next`, never by `cancel_cursor` or any other
    /// caller (`get_owned` itself is untouched; existing callers/tests keep
    /// its current `Result<Arc<Cursor>, CursorRegistryError>` shape).
    ///
    /// The critical property: `in_flight` is incremented WHILE the shard
    /// read-guard returned by `self.open.get(...)` is still alive — before
    /// it is dropped, and therefore strictly before this call returns. This
    /// closes a gap a plain re-check-`try_lock`-at-removal would NOT close:
    /// even re-checking `try_lock()` at the exact moment of removal leaves a
    /// window between "`get_owned` returns the `Arc<Cursor>`" and "the
    /// caller actually calls `cursor.state().lock().await`" during which
    /// `try_lock()` would still succeed (nothing is locked yet) — a reaper
    /// sweep landing in that exact gap would remove the cursor even though a
    /// fetch is genuinely about to use it. Incrementing `in_flight` inside
    /// the same critical section as the registry lookup has no such gap:
    /// any reaper removal attempt for this key must run either strictly
    /// before this lookup starts (observing this entry gone, or observing it
    /// present and racing the increment safely under the same shard lock —
    /// see `sweep_and_reap`'s doc comment for the other side of this
    /// argument) or strictly after this lookup's read-guard is dropped — by
    /// which point the increment (which happened before that drop) is
    /// already visible to `sweep_and_reap`'s `Acquire` load. There is no
    /// interleaving where a removal's predicate can observe `in_flight == 0`
    /// while a lookup that already returned `Ok` is still "between get and
    /// lock".
    pub fn get_owned_for_fetch(
        &self,
        cursor_id: u64,
        sid: &[u8; 32],
    ) -> Result<(Arc<Cursor>, FetchLease), CursorRegistryError> {
        let arc = match self.open.get(&cursor_id) {
            Some(r) => {
                // Increment WHILE the shard read-guard `r` is still alive —
                // this is the ordering property the whole design rests on.
                r.value().in_flight.fetch_add(1, Ordering::AcqRel);
                Arc::clone(r.value())
            }
            None => {
                return Err(if self.reaped_tombstones.contains_key(&cursor_id) {
                    CursorRegistryError::CursorExpired
                } else {
                    CursorRegistryError::CursorNotFound
                });
            }
        };
        if &arc.owner_sid != sid {
            // Undo the increment — no lease is returned on this error path,
            // so nothing will ever call `FetchLease::drop` to balance it.
            arc.in_flight.fetch_sub(1, Ordering::AcqRel);
            return Err(CursorRegistryError::CursorOwnershipMismatch);
        }
        let lease = FetchLease(Arc::clone(&arc));
        Ok((arc, lease))
    }

    /// Remove a cursor (explicit `CancelCursor`, or the scan-exhausted
    /// auto-close on the last `FetchNext` page). Frees the session's slot.
    /// Does NOT tombstone — a deliberate close is not an error condition a
    /// later fetch should be told "expired" about.
    pub fn remove(&self, cursor_id: u64) -> Option<Arc<Cursor>> {
        self.remove_matching(cursor_id, None, None)
    }

    /// Remove only the selected cursor instance, protecting a reused id.
    pub(crate) fn remove_identity(
        &self,
        cursor_id: u64,
        expected: &Arc<Cursor>,
    ) -> Option<Arc<Cursor>> {
        self.remove_matching(cursor_id, Some(expected), None)
    }

    /// Remove only the cursor whose activity row was used for the caller's
    /// authorization decision. The open-entry shard guard serializes this
    /// identity check with removal/re-registration of a reused id.
    pub(in crate::engine) fn remove_activity_identity(
        &self,
        cursor_id: u64,
        expected: &Arc<CursorActivity>,
    ) -> Option<Arc<Cursor>> {
        self.remove_matching(cursor_id, None, Some(expected))
    }

    fn remove_matching(
        &self,
        cursor_id: u64,
        expected_cursor: Option<&Arc<Cursor>>,
        expected_activity: Option<&Arc<CursorActivity>>,
    ) -> Option<Arc<Cursor>> {
        // `remove_if`'s predicate runs under the shard write lock also taken
        // by `register`, making mirror removal and count decrement atomic
        // with publication. Identity checks under that lock leave replacements
        // untouched when an id has been reused.
        let mut removed: Option<Arc<Cursor>> = None;
        self.open.remove_if(&cursor_id, |_, cursor| {
            let matches_cursor =
                expected_cursor.is_none_or(|expected| Arc::ptr_eq(cursor, expected));
            let matches_activity = expected_activity.is_none_or(|expected| {
                self.activity
                    .peek_with(&cursor_id, |_, current| Arc::ptr_eq(current, expected))
                    .unwrap_or(false)
            });
            let matches = matches_cursor && matches_activity;
            if matches {
                self.activity.remove_sync(&cursor_id);
                self.count.fetch_sub(1, Ordering::AcqRel);
                removed = Some(Arc::clone(cursor));
            }
            matches
        });
        let cursor = removed?;
        self.free_session_slot(&cursor.owner_sid);
        Some(cursor)
    }

    /// Remove all cursors owned by a disconnecting session. The O(N) scan is
    /// deliberate and confined to teardown, keeping hot registration O(1).
    pub(crate) fn remove_all_for_session(&self, sid: &[u8; 32]) -> Vec<Arc<Cursor>> {
        self.remove_all_for_session_selected(sid, |_, _| {})
    }

    #[cfg(test)]
    pub(crate) fn remove_all_for_session_with(
        &self,
        sid: &[u8; 32],
        after_selection: impl FnMut(u64, &Arc<Cursor>),
    ) -> Vec<Arc<Cursor>> {
        self.remove_all_for_session_selected(sid, after_selection)
    }

    fn remove_all_for_session_selected(
        &self,
        sid: &[u8; 32],
        mut after_selection: impl FnMut(u64, &Arc<Cursor>),
    ) -> Vec<Arc<Cursor>> {
        let ids: Vec<u64> = self
            .open
            .iter()
            .filter(|entry| entry.value().owner_sid() == sid)
            .map(|entry| *entry.key())
            .collect();
        ids.into_iter()
            .filter_map(|id| {
                let cursor = self.open.get(&id).map(|entry| Arc::clone(entry.value()))?;
                if cursor.owner_sid() != sid {
                    return None;
                }
                after_selection(id, &cursor);
                self.remove_identity(id, &cursor)
            })
            .collect()
    }

    /// Decrement the session's live open-cursor count and, if it just
    /// reached zero, remove the `by_session` entry entirely — otherwise a
    /// long-lived server leaks one `[u8; 32] -> Arc<AtomicUsize>` entry per
    /// connection that ever opened a cursor (CR-B8 / #774, R-3).
    ///
    /// The decrement and the "now zero, so remove" check are combined into
    /// ONE atomic step via `DashMap::remove_if`, whose predicate runs while
    /// `dashmap` holds the shard's write lock for `owner_sid` (verified
    /// against `dashmap` 6.1.0's `_remove_if`, which calls
    /// `_yield_write_shard` before invoking the predicate and only releases
    /// it after — see `dashmap::HashMap::_entry`, used by `register`'s
    /// `.entry(owner_sid)`, which takes the SAME shard write lock). So this
    /// decrement-then-maybe-remove is atomic relative to a concurrent
    /// `register()`: either `register`'s `entry()` call happens strictly
    /// before this `remove_if` (it sees the pre-decrement count, adds to
    /// the same `Arc`, and this `remove_if`'s predicate then observes a
    /// NON-zero result and does not remove), or strictly after (this
    /// `remove_if` already removed the entry, `register`'s `entry()` finds
    /// nothing and creates a fresh `AtomicUsize(0)` via `or_insert_with`,
    /// correctly starting over for a session that just hit zero). Either
    /// interleaving preserves correct cap accounting — a session can never
    /// exceed `max_per_session` split across two independently-created
    /// counters.
    ///
    /// #1073 (F-1): this atomicity argument only holds because `register`
    /// holds the shard write lock (`slot`, the `entry()` guard) across its
    /// ENTIRE CAS loop, not just the `entry()` call itself — an earlier
    /// version dropped `slot` immediately after cloning the counter `Arc`,
    /// so the increment below could interleave with this `remove_if`
    /// after all: the entry could be removed (or removed then recreated)
    /// between `register`'s `entry()` returning and its CAS actually
    /// running, orphaning a counter or `fetch_sub`-ing an already-zeroed
    /// one to `usize::MAX`. See `register`'s CAS-loop comment for the
    /// current (fixed) locking discipline this doc comment's claim
    /// depends on.
    fn free_session_slot(&self, owner_sid: &[u8; 32]) {
        self.by_session.remove_if(owner_sid, |_, counter| {
            // Pre-decrement value == 1 means the NEW value is 0.
            counter.fetch_sub(1, Ordering::AcqRel) == 1
        });
    }

    /// Sweep idle-expired, uncontended cursors in ONE atomic-per-entry pass
    /// and remove them, leaving a tombstone for each so a racing `FetchNext`
    /// gets `CursorExpired` instead of `CursorNotFound`. Returns the number
    /// reaped.
    ///
    /// F-9 (#799) originally gated this on `state().try_lock()` succeeding:
    /// `fetch_next` (`db_handler::cursor_handlers`) holds
    /// `cursor.state().lock().await` for the ENTIRE duration of a
    /// `FetchNext` (including the actual keyset/offset page scan), only
    /// dropping it right before `bump_activity()` at the very end.
    /// `bump_activity` is the ONLY thing that moves `last_activity_nanos`,
    /// and it fires once, at the END of a successful `FetchNext` — so a
    /// `FetchNext` whose own execution time (large `page_size`, expensive
    /// scan, slow keyset boundary search) exceeds `idle_ttl` looked exactly
    /// as idle to `is_expired` as a genuinely abandoned cursor, even though
    /// it was being actively used the whole time. `try_lock()` told the two
    /// apart, but F-9 only collected a list (this same method, formerly
    /// named `expired_ids`) — the actual removal happened in the reaper's
    /// sweep loop via a SEPARATE, later call to `remove_for_idle_reap`,
    /// leaving a single-reaper-tick-sized gap in which a new `FetchNext`
    /// could look up the cursor and be about to lock `state()` (so
    /// `try_lock()` would still have succeeded) when the removal pass
    /// reached it anyway.
    ///
    /// F-20 (#813) closes that residual with TWO changes, combined: (1) an
    /// `in_flight` counter (see [`Cursor`]/[`FetchLease`]) incremented by
    /// [`Self::get_owned_for_fetch`] WHILE the registry's read-guard for
    /// that entry is still held — covering the full lookup-to-completion
    /// window, a strict superset of `try_lock`'s lock-to-unlock window, with
    /// no gap between "looked up" and "about to lock"; and (2) folding the
    /// expiry check, the in-flight check, and the actual removal into ONE
    /// atomic step per entry via `DashMap::retain`, whose predicate runs
    /// while that shard's write lock is held (same guarantee
    /// `free_session_slot` already documents and relies on for
    /// `remove_if` — see its doc comment for the citation of *why* this
    /// `dashmap` 6.1.0 ordering guarantee holds). `try_lock()` on
    /// `state()` is no longer used as a reap-gate: `in_flight` covers a
    /// strict superset of the window it covered, so keeping both would be
    /// two overlapping, redundant mechanisms — a single, understood one
    /// is preferred (see `get_owned_for_fetch`'s doc comment for the
    /// ordering argument in full).
    pub fn sweep_and_reap(&self, now: Instant, idle_ttl: Duration) -> usize {
        let mut reaped_owners: Vec<[u8; 32]> = Vec::new();
        let mut reaped_ids: Vec<u64> = Vec::new();
        self.open.retain(|id, cursor| {
            let expired =
                cursor.is_expired(now, idle_ttl) && cursor.in_flight.load(Ordering::Acquire) == 0;
            if expired {
                reaped_owners.push(cursor.owner_sid);
                reaped_ids.push(*id);
                // R4b-1: mirror teardown + count decrement INSIDE the
                // retain predicate — i.e. under the same per-shard write
                // lock that removes the authoritative entry — so a
                // concurrent `register` (vacant guard) is fully serialized
                // with this removal for every key on this shard: the
                // reaper never deletes a mirror row a register has not
                // finished contributing, and never misses one it has.
                self.activity.remove_sync(id);
                self.count.fetch_sub(1, Ordering::AcqRel);
            }
            !expired // keep everything that is NOT being reaped
        });
        for owner in &reaped_owners {
            self.free_session_slot(owner);
        }
        let tombstoned_at = Instant::now();
        for id in &reaped_ids {
            self.reaped_tombstones.insert(*id, tombstoned_at);
        }
        reaped_ids.len()
    }

    /// Drop tombstones older than [`TOMBSTONE_TTL`] — called by the reaper
    /// sweep alongside `sweep_and_reap` so the tombstone set does not grow
    /// unbounded.
    pub fn sweep_tombstones(&self, now: Instant) {
        self.reaped_tombstones
            .retain(|_, inserted_at| now.saturating_duration_since(*inserted_at) < TOMBSTONE_TTL);
    }

    /// Number of open cursors — O(1), reads the R4b-1 [`AtomicUsize`]
    /// mirror (`count`), never `DashMap::len()`. This is what the activity
    /// gauges read.
    pub fn len(&self) -> usize {
        self.count.load(Ordering::Acquire)
    }

    /// Whether no cursors are open — mirror-backed like [`Self::len`].
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// R4b-1 reader: every open cursor's safe activity metadata as
    /// `(cursor_id, Arc<CursorActivity>)` pairs (see
    /// [`CursorActivitySnapshot`]). INTENTIONALLY cheap and unsorted:
    /// one `Arc` clone per row, zero `String` clones, NO global sort — the
    /// paging worker sorts by `cursor_id`, renders at
    /// most `limit` rows and derives `elapsed` as `meta.created.elapsed()`.
    ///
    /// Lock-free: iterates the scc `HashIndex` mirror under a fresh epoch
    /// `Guard` — the reader takes NO lock that any lifecycle writer
    /// (`register` / `remove` / `sweep_and_reap`) takes, unlike a
    /// `DashMap::iter` which would hold shard read locks. A momentarily
    /// torn snapshot (a cursor registered/removed mid-scan) is acceptable
    /// and documented per the brief's D4. No bearer material, query
    /// literal or row data can appear in a row by construction — the
    /// mirror's value type carries only the safe fields.
    /// `pub(crate)`: engine-internal — read by the paging worker
    /// (`db_handler::repo_admin::activity::list`).
    pub(crate) fn activity_snapshot(&self) -> Vec<CursorActivitySnapshot> {
        let guard = Guard::new();
        self.activity
            .iter(&guard)
            .map(|(cursor_id, meta)| CursorActivitySnapshot {
                cursor_id: *cursor_id,
                meta: Arc::clone(meta),
            })
            .collect()
    }

    /// Live open-cursor count for a session (0 if the session has never
    /// opened one). O(1) — reads the `AtomicUsize` mirror, never counts.
    pub fn open_count_for_session(&self, sid: &[u8; 32]) -> usize {
        self.by_session
            .get(sid)
            .map(|c| c.value().load(Ordering::Acquire))
            .unwrap_or(0)
    }

    /// Number of `by_session` entries currently tracked (test/diagnostic
    /// probe only — asserts CR-B8's leak fix: a session's entry must be
    /// removed once its live count reaches zero, not merely zeroed out).
    /// NOT a hot-path method: `DashMap::len()` is fine here (unlike `scc`'s
    /// banned O(N) `len()`, `dashmap`'s isn't on `clippy.toml`'s
    /// `disallowed-methods` list), but production code should never need
    /// this cardinality — only `open_count_for_session`'s O(1) per-session
    /// lookup.
    #[cfg(test)]
    pub(crate) fn by_session_len(&self) -> usize {
        self.by_session.len()
    }
}

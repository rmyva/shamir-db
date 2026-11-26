//! S5c-X PR5b part 2 — the channel-full journal repair drainer.
//!
//! When a changefeed event hits a full journal channel, `shamir-tx` records
//! its `commit_version` in `RepairPending` (no gap, no drop). This module is
//! the engine-side drainer that lands those deferred events on later drain
//! passes:
//!
//! 1. Derive the event again from the surviving WAL entry (the WAL segment is
//!    held retained by the `settle_and_truncate` repair-pending clamp for
//!    exactly this purpose).
//! 2. Probe the journal store first: an already-present equal event retires
//!    the version WITHOUT a repair credit (idempotence — the event was never
//!    repaired by us); a DIFFERING event is a loud `repair_mismatch` and is
//!    never overwritten and never declared a gap (the journal HAS an event).
//! 3. Otherwise `journal_enqueue_tracked` the derived event: admitted →
//!    `on_admitted`; Full → leave pending for the next pass; Closed → the
//!    gap was already marked and the version retired in-band.
//!
//! Cost model: the fast path is one atomic depth load per drainer pass. The
//! full retained-WAL scan (`replay()`) runs only when the pending set is
//! non-empty — the accepted rare-path cost, since a full channel is by
//! definition an exceptional backpressure event.

use std::sync::Arc;

use shamir_collections::TFxMap;
use shamir_storage::error::{DbError, DbResult};

use crate::commit::core::RepoCore;

/// Land every deferred repair-pending journal event whose WAL evidence
/// survived. Returns the number of versions repaired or retired this pass.
///
/// Called from `Drainer::settle_and_truncate` — i.e. on every drain pass
/// where truncation matters — BEFORE the truncation ceiling is computed, so
/// an admission immediately lifts its clamp.
pub(crate) async fn drain_repair_pending(repo: &RepoCore) -> DbResult<usize> {
    // Fast path: one atomic load per pass (runs on EVERY drain pass).
    let feed = repo.changefeed().await?;
    if feed.feed.repair_pending_depth() == 0 {
        return Ok(0);
    }
    let mut pending = feed.feed.repair_pending().snapshot();
    if pending.is_empty() {
        return Ok(0);
    }
    pending.sort_unstable();

    // Rare path (pending non-empty): full retained-WAL scan. See module doc.
    let wal = repo.wal_group().await?;
    let mut by_version: TFxMap<u64, Arc<shamir_wal::WalEntryV2>> = TFxMap::default();
    for entry in wal.replay().await? {
        by_version.insert(entry.commit_version, Arc::new(entry));
    }

    let mut retired = 0usize;
    for version in pending {
        let Some(entry) = by_version.get(&version).cloned() else {
            // WAL evidence gone (retention crossed it — should be impossible
            // while the clamp holds, so this is a loud invariant breach).
            log::warn!(
                "repair-pending drain: version {version} wal entry no longer retained; \
                 declaring gap"
            );
            feed.feed.mark_gap(version);
            feed.feed.repair_pending().on_unrepairable(version);
            retired += 1;
            continue;
        };
        let Some(event) = shamir_tx::event_from_wal_entry(&entry) else {
            // Legacy / unknown entry format: the event is underivable from
            // the retained evidence — the minimal-gap declaration.
            log::warn!(
                "repair-pending drain: version {version} wal entry is legacy \
                 (event underivable); declaring gap"
            );
            feed.feed.mark_gap(version);
            feed.feed.repair_pending().on_unrepairable(version);
            retired += 1;
            continue;
        };

        // Idempotence probe: mirror repair_event's store lookup before any
        // enqueue, so a re-run never duplicates an already-present event.
        let key = shamir_tx::changefeed::version_key(version);
        let rows = feed
            .store
            .range_from(key, 1)
            .await
            .map_err(|e| DbError::Storage(e.to_string()))?;
        if let Some(bytes) = rows.first() {
            let found =
                rmp_serde::from_slice::<shamir_tx::ChangelogEvent>(bytes).map_err(|error| {
                    DbError::Codec(format!("repair-pending {version} probe decode: {error}"))
                })?;
            if found.commit_version == version {
                if found == event {
                    log::debug!(
                        "repair-pending drain: version {version} already present in the \
                         journal; retiring without a repair credit"
                    );
                } else {
                    // NEVER overwrite, never mark a gap: the journal HAS an
                    // event at this version — only the derivation disagrees.
                    feed.feed.bump_repair_mismatch();
                    log::error!(
                        "repair-pending drain: version {version} journal event DIFFERS from \
                         the WAL-derived event; retiring pending without overwrite"
                    );
                }
                feed.feed.repair_pending().on_unrepairable(version);
                retired += 1;
                continue;
            }
        }

        let event = Arc::new(event);
        if feed.feed.journal_enqueue_tracked(&event) {
            feed.feed.repair_pending().on_admitted(version);
            retired += 1;
        } else if feed.feed.first_gap_version() == version {
            // Closed writer: the tracked enqueue already marked the gap and
            // retired the version in-band (`on_writer_closed`); count it.
            retired += 1;
        }
        // Full: still no room — LEAVE pending for the next pass (no
        // counters); the enqueue already woke `repair_notify`.
    }
    Ok(retired)
}

/// Project a WAL entry's `interner_delta` (`Vec<(scope, name, id)>`) into the
/// single max-id shape the A5 gate consumes (`Option<u64>`). Mirrors
/// `materialize`'s `interner_delta_max_id` capture, sourced here from the
/// durable WAL entry rather than the in-memory `TxContext`.
///
/// Stage I: the interner is per-REPO, so every triple's `id` shares one
/// id-namespace — we just take the max across the whole delta. The first
/// `u64` (the scope constant) is ignored.
///
/// (Moved verbatim out of the over-ratchet `drainer.rs`; its only callers
/// live there.)
pub(crate) fn entry_interner_max_id(entry: &shamir_wal::WalEntryV2) -> Option<u64> {
    entry
        .interner_delta
        .iter()
        .map(|(_scope, _name, id)| *id)
        .max()
}

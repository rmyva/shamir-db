use std::collections::BTreeSet;
use std::sync::Arc;

use crate::apply_replicated::follower_durability::in_flight_range;
use crate::clean_horizon::recovery_floor::reserve_recovery_floor;
use crate::commit::core::RepoCore;
use crate::meta::repl::{classify_follower_bookmark, FollowerBookmarkState};
use shamir_storage::error::{DbError, DbResult};
use shamir_storage::key_bytes::KeyBytes;
use shamir_tx::changefeed::cold::{
    delete_event, events_in_range, flush_journal, put_event_if_missing,
};
use shamir_tx::changefeed::{decode_gap_marker, gap_marker_key};
use shamir_wal::WalEntryV2;

use super::classification::{classify, Evidence};

const JOURNAL_BATCH_SIZE: usize = 256;

fn missing_intervals(known: &BTreeSet<u64>, after: u64, through: u64) -> Vec<(u64, u64)> {
    if through <= after {
        return Vec::new();
    }
    let Some(mut next_missing) = after.checked_add(1) else {
        return Vec::new();
    };
    let mut gaps = Vec::new();
    for version in known.range(next_missing..=through).copied() {
        if version > next_missing {
            gaps.push((next_missing, version - 1));
        }
        let Some(next) = version.checked_add(1) else {
            return gaps;
        };
        next_missing = next;
    }
    if next_missing <= through {
        gaps.push((next_missing, through));
    }
    gaps
}

pub(crate) async fn reconcile_cold_start(
    repo: &RepoCore,
    entries: &[WalEntryV2],
    permit: &crate::repo::RepoWritePermit,
) -> DbResult<()> {
    reconcile_with_hook(repo, entries, permit, &mut |_| Ok(())).await
}

pub(super) async fn reconcile_with_hook(
    repo: &RepoCore,
    entries: &[WalEntryV2],
    permit: &crate::repo::RepoWritePermit,
    hook: &mut (dyn FnMut(&'static str) -> DbResult<()> + Send),
) -> DbResult<()> {
    // PR6b-3: classify the durable follower-role state BEFORE every
    // early-return branch. A legacy/malformed bookmark record refuses the
    // OPEN (state-table row "typed refusal at open"), including across the
    // non-persistent-WAL and `h <= j` branches below.
    let info = repo.tx_info_store().await?;
    let follower_state = classify_follower_bookmark(&info).await?;
    let follower_provenance = matches!(follower_state, FollowerBookmarkState::Native(_));
    if follower_provenance {
        // Durable role state — a valid v3 record, bookmark zero and
        // fence-less included — is the ONLY thing that identifies a
        // follower; enable native visibility before any read below.
        repo.tx_gate().await?.enable_receipt_native_visibility();
    }
    if !repo.has_persistent_wal() {
        return repo.certify_legacy_cold_start().await;
    }

    let gate = repo.tx_gate().await?;
    let h = gate.version_allocation_high_water_mark();
    repo.flush_all_history().await?;

    let store = repo.changelog_store_for_cold_reconcile().await?;
    let previous_gap = match store.get(KeyBytes::from(gap_marker_key())).await {
        Ok(bytes) => Some(decode_gap_marker(&bytes).map_err(DbError::Storage)?),
        Err(DbError::NotFound(_)) => None,
        Err(error) => return Err(error),
    };
    let adapter = crate::repo::changelog_store::StoreChangelog::new(Arc::clone(&store));
    let j = adapter.load_clean_horizon().await?;

    let feed = repo.changefeed().await?.feed;
    if let Some((first, through)) = previous_gap {
        feed.record_recovery_gap(first, through).await?;
    }
    // PR6b-2: complete an outstanding replication receipt BEFORE the `h <= j`
    // early return and the PR6c classification, so the receipt's versions are
    // certified through the bookmark's clean horizon instead of becoming a
    // recovery GAP.
    super::receipt_recovery::complete_outstanding_receipt(repo, &info, permit).await?;
    if h <= j {
        if let Some(coverage) = repo.wal_group().await?.coverage_observer() {
            coverage.certify_exclusive_open(h);
        }
        return Ok(());
    }

    // PR6c: a follower record with a known journal horizon certifies every
    // event through it; only the in-flight batch stays uncertain.
    let certified_through =
        super::follower::follower_certified_through(j, follower_provenance, &info).await?;
    let recovery_floor = reserve_recovery_floor(repo, &info, h.max(j)).await?;
    if recovery_floor > h {
        return Err(DbError::Storage(format!(
            "recovery floor {recovery_floor} exceeds allocation horizon {h}"
        )));
    }

    let mut wal_versions = BTreeSet::new();
    let mut legacy_versions = BTreeSet::new();
    let mut eventless_versions = BTreeSet::new();
    for entry in entries
        .iter()
        .filter(|entry| entry.commit_version > j && entry.commit_version <= h)
    {
        wal_versions.insert(entry.commit_version);
        match classify(entry) {
            Evidence::Derived(event) => {
                hook("pre_put")?;
                put_event_if_missing(store.as_ref(), &event).await?;
                hook("after_put")?;
                hook("mid_reconcile")?;
            }
            Evidence::LegacyUnknown => {
                legacy_versions.insert(entry.commit_version);
            }
            Evidence::Eventless => {
                eventless_versions.insert(entry.commit_version);
            }
        }
    }

    let existing_events = events_in_range(store.as_ref(), j, h, JOURNAL_BATCH_SIZE).await?;
    let existing_versions: BTreeSet<u64> = existing_events
        .iter()
        .map(|event| event.commit_version)
        .collect();
    // Eventless WAL entries intentionally add no gap. If such an entry has a
    // journal record, the persisted WAL and journal disagree, so refuse open.
    if existing_versions
        .iter()
        .any(|version| eventless_versions.contains(version))
    {
        return Err(DbError::Storage(
            "eventless WAL entry has journal event".into(),
        ));
    }

    let no_wal_versions: BTreeSet<u64> = existing_versions
        .difference(&wal_versions)
        .copied()
        .collect();
    if !follower_provenance {
        for version in &no_wal_versions {
            hook("pre_delete")?;
            delete_event(store.as_ref(), *version).await?;
            hook("after_delete")?;
        }
    }

    // Missing intervals cannot be separated into aborted allocations versus
    // lost served commits with persisted state, so they remain conservative
    // for a plain repository. A follower with a known record horizon already
    // certified everything through it, so only its in-flight batch stays an
    // uncertain GAP.
    if follower_provenance {
        if let Some(range) = in_flight_range(Some(certified_through), h) {
            feed.record_recovery_gap(*range.start(), *range.end())
                .await?;
        }
    } else {
        for (first, through) in missing_intervals(&wal_versions, j, h) {
            feed.record_recovery_gap(first, through).await?;
        }
    }
    for version in legacy_versions {
        feed.record_recovery_gap(version, version).await?;
    }

    hook("pre_flush")?;
    flush_journal(store.as_ref()).await?;
    hook("after_flush")?;
    hook("before_j")?;
    feed.record_clean_horizon(h).await?;
    hook("after_j")?;

    if let Some(coverage) = repo.wal_group().await?.coverage_observer() {
        coverage.certify_exclusive_open(h);
    }
    Ok(())
}

use std::sync::Arc;

use crate::commit::core::RepoCore;
use shamir_storage::error::{DbError, DbResult};
use shamir_wal::WalEntryV2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Repaired,
    /// The journal channel was FULL: the event was deferred into the
    /// repair-pending set (no gap, no failure). The drainer's repair-pending
    /// drain lands it on a later pass; the orphan itself stays unresolved.
    EnqueuePending,
    Gap(GapReason),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GapReason {
    UnknownWAL,
    EnqueueFailed,
    WriterGap,
}

impl std::fmt::Display for GapReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownWAL => f.write_str("unknown_or_eventless_wal"),
            Self::EnqueueFailed => f.write_str("journal_writer_unavailable"),
            Self::WriterGap => f.write_str("journal_enqueue_or_persist_failed"),
        }
    }
}

fn is_closed_writer(error: &DbError) -> bool {
    matches!(error, DbError::Storage(message) if message == "changefeed writer closed")
}

/// WAL-derived event repair. A checkpoint failure is retryable unless the
/// existing changefeed boundary reports its exact closed-send sentinel. The
/// sentinel is stringly typed because the existing production API does not expose a closed-writer type; backend errors are never parsed.
pub async fn repair_event(repo: &RepoCore, entry: &WalEntryV2) -> DbResult<Outcome> {
    let version = entry.commit_version;
    let Some(event) = shamir_tx::event_from_wal_entry(entry) else {
        return Ok(Outcome::Gap(GapReason::UnknownWAL));
    };
    let handle = repo.changefeed().await?;
    let feed = &handle.feed;
    let safe_horizon = feed.clean_horizon();
    if safe_horizon >= version {
        return Err(DbError::Internal(format!(
            "orphan {version} is already covered by clean horizon {safe_horizon}"
        )));
    }

    // A failed preflight must not probe by enqueueing: an event may already
    // be pending and an enqueue can duplicate it. Only the API's exact closed
    // sender sentinel authorizes conservative GAP settlement.
    if let Err(error) = feed.record_clean_horizon(safe_horizon).await {
        if is_closed_writer(&error) {
            log::warn!("orphan {version}: journal writer closed before repair");
            return Ok(Outcome::Gap(GapReason::EnqueueFailed));
        }
        return Err(error);
    }

    let key = shamir_tx::changefeed::version_key(version);
    let rows = handle
        .store
        .range_from(key.clone(), 1)
        .await
        .map_err(|e| DbError::Storage(e.to_string()))?;
    if let Some(bytes) = rows.first() {
        let found = rmp_serde::from_slice::<shamir_tx::ChangelogEvent>(bytes)
            .map_err(|error| DbError::Codec(format!("orphan {version} event decode: {error}")))?;
        if found.commit_version == version {
            if found == event {
                return Ok(Outcome::Repaired);
            }
            return Err(DbError::Internal(format!(
                "orphan {version} existing journal event differs from WAL"
            )));
        }
    }

    let event = Arc::new(event);
    if !feed.journal_enqueue_tracked(&event) {
        // Closed: the tracked enqueue already marked the exact gap (and
        // retired its repair-pending record) — settle as before. Full: the
        // event was deferred into the repair-pending set (PR5b — no gap, no
        // counters); the repair-pending drain lands it on a later pass.
        if feed.first_gap_version() == version {
            return Ok(Outcome::Gap(GapReason::EnqueueFailed));
        }
        return Ok(Outcome::EnqueuePending);
    }
    if let Err(error) = feed.record_clean_horizon(safe_horizon).await {
        if is_closed_writer(&error) {
            return Ok(Outcome::Gap(GapReason::EnqueueFailed));
        }
        return Err(error);
    }

    let rows = handle
        .store
        .range_from(key, 1)
        .await
        .map_err(|e| DbError::Storage(e.to_string()))?;
    let Some(bytes) = rows.first() else {
        return Ok(Outcome::Gap(GapReason::WriterGap));
    };
    let found = rmp_serde::from_slice::<shamir_tx::ChangelogEvent>(bytes).map_err(|error| {
        DbError::Codec(format!("orphan {version} persisted event decode: {error}"))
    })?;
    if found.commit_version != version {
        return Ok(Outcome::Gap(GapReason::WriterGap));
    }
    if found == *event {
        Ok(Outcome::Repaired)
    } else {
        Err(DbError::Internal(format!(
            "orphan {version} persisted journal event differs from WAL"
        )))
    }
}

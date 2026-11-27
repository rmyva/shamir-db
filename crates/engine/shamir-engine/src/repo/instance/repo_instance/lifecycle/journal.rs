use super::super::ChangefeedHandle;
#[cfg(test)]
use crate::tx::clean_horizon::checkpoint::test_seam;
use shamir_storage::error::{DbError, DbResult};
use std::sync::Arc;

use super::super::RepoInstance;

impl RepoInstance {
    /// Read the unchanged journal, attaching name bindings only for Pull.
    /// The tuple retains the journal's gap metadata; translation errors propagate.
    ///
    /// Replication-only R clamp; ordinary changefeeds retain completion W.
    pub async fn read_changelog_for_replication(
        &self,
        from_version: u64,
        limit: usize,
    ) -> DbResult<(Vec<crate::tx::ReplicatedEvent>, Option<u64>)> {
        let journal = self
            .read_changelog_replication_page(from_version, limit)
            .await?
            .read;
        let manager = self.repo_interner().await?;
        let interner = manager.get().await?;
        let mut events = Vec::with_capacity(journal.events.len());
        for event in journal.events {
            let mut ids = shamir_collections::TFxMap::default();
            for change in &event.changes {
                if change.op == shamir_tx::ChangeOp::Put {
                    let bytes = change
                        .value
                        .as_ref()
                        .ok_or_else(|| DbError::Codec("replication Put has no value".into()))?;
                    let value = shamir_types::types::value::InnerValue::from_bytes(bytes)
                        .map_err(|error| DbError::Codec(error.to_string()))?;
                    shamir_tx::collect_referenced_ids(&value, &mut ids);
                }
            }
            let mut field_bindings = Vec::with_capacity(ids.len());
            for id in ids.keys() {
                let name = interner
                    .get_str(&shamir_types::core::interner::InternerKey::new(*id))
                    .ok_or_else(|| {
                        DbError::Codec(format!("replication field id {id} has no name"))
                    })?;
                field_bindings.push((*id, name.to_string()));
            }
            field_bindings.sort_unstable_by_key(|(id, _)| *id);
            events.push(crate::tx::ReplicatedEvent {
                event,
                field_bindings,
            });
        }
        Ok((events, journal.gap_at))
    }
    pub async fn certified_gap_interval(&self) -> DbResult<Option<(u64, u64)>> {
        Ok(self.changefeed().await?.feed.certified_gap_interval())
    }

    // ============================================================================
    // Changefeed (Phase 3b): live broadcast + durable journal
    // ============================================================================

    /// cancel-safe: yes — `OnceCell::get_or_try_init`; the returned handle
    /// is an in-memory `Arc` clone. Lazily builds the changefeed once per
    /// repo (see the core method for the init contract).
    pub(crate) async fn changefeed(&self) -> DbResult<ChangefeedHandle> {
        #[cfg(test)]
        test_seam::record(&self.core, "before_feed_init");
        self.core.changefeed().await
    }

    #[cfg(test)]
    /// Resolve before entering the synchronous publish block. Failure is
    /// logged, but does not mark a gap until this version actually publishes.
    pub(crate) async fn prepare_journal_event(
        &self,
        event: Option<shamir_tx::ChangelogEvent>,
    ) -> Option<(
        Arc<shamir_tx::ChangelogEvent>,
        Option<Arc<shamir_tx::RepoChangefeed>>,
    )> {
        self.core.prepare_journal_event(event).await
    }

    #[cfg(test)]
    /// Synchronous publish-time fallback; minimum is shared across repo clones.
    pub(crate) fn record_pending_changefeed_gap(&self, version: u64) {
        self.core.record_pending_changefeed_gap(version);
    }

    #[cfg(test)]
    /// Live only: never re-enqueue an event already handled before publish.
    pub(crate) fn broadcast_changefeed_event(&self, event: Option<Arc<shamir_tx::ChangelogEvent>>) {
        self.core.broadcast_changefeed_event(event);
    }

    /// Subscribe to this repo's live changefeed.
    ///
    /// Returns a `broadcast::Receiver` that yields every `ChangelogEvent`
    /// emitted after the call. A subscriber that falls behind the bounded
    /// ring receives `RecvError::Lagged` and should re-sync the missed
    /// window via [`read_changelog_from`](Self::read_changelog_from).
    pub async fn subscribe_changelog(
        &self,
    ) -> DbResult<tokio::sync::broadcast::Receiver<Arc<shamir_tx::ChangelogEvent>>> {
        Ok(self.changefeed().await?.feed.subscribe())
    }

    /// Resumable pull: read up to `limit` durable journal events with
    /// `commit_version >= from_version`, ascending.
    ///
    /// Returns a [`shamir_tx::JournalRead`] which carries both the events and
    /// a `gap_at` field. When `gap_at` is `Some(v)` the journal has a known
    /// gap at version `v` (a prior overflow dropped that event) — callers
    /// should treat the feed as non-contiguous from `from_version`.
    /// The prefix is bounded by the contiguous completion watermark, not the
    /// potentially out-of-order last committed version. A stalled writer may
    /// return an older (possibly empty) prefix; confirmation logs/counts its
    /// own timeout, so this layer does not log a second warning.
    /// cancel-safe: yes — gate/feed initialization can be retried; confirmation
    /// only advances a monotonic prefix, and range reads mutate no data.
    pub async fn read_changelog_from(
        &self,
        from_version: u64,
        limit: usize,
    ) -> DbResult<shamir_tx::JournalRead> {
        Ok(self
            .read_changelog_bounded_impl(from_version, limit, false, None)
            .await?
            .read)
    }

    /// Read a page and expose the actual confirmed bound, sampled before
    /// feed initialization. Store range failures are errors, not empty pages;
    /// corruption and known holes retain the journal's `gap_at` semantics.
    /// cancel-safe: yes — initialization is retryable, confirmation advances
    /// only a monotonic prefix, and the single range read mutates no records.
    pub async fn read_changelog_bounded(
        &self,
        from_version: u64,
        limit: usize,
    ) -> DbResult<crate::repo::BoundedJournalRead> {
        self.read_changelog_bounded_impl(from_version, limit, true, None)
            .await
    }

    pub(in crate::repo) async fn read_changelog_bounded_impl(
        &self,
        from_version: u64,
        limit: usize,
        preserve_errors: bool,
        // PR1: replication-only extra clamp (`R`). `None` everywhere else —
        // changefeed subscribers keep the completion-watermark bound.
        r_clamp: Option<u64>,
    ) -> DbResult<crate::repo::BoundedJournalRead> {
        let sampled_w = self.tx_gate().await?.completion().watermark();
        let h = self.changefeed().await?;
        self.core
            .durability_handle()
            .read_changelog_bounded_from(
                h,
                sampled_w,
                from_version,
                limit,
                preserve_errors,
                r_clamp,
            )
            .await
    }

    /// Park until the completion prefix reaches `target_version`, including
    /// aborted/empty versions, or report that this gate requires reopening.
    /// This is completion progress, not journal persistence confirmation.
    /// cancel-safe: yes — retryable gate initialization followed by the
    /// gate's cancellation-safe notification wait; no added polling or state.
    pub async fn await_journal_progress(
        &self,
        target_version: u64,
    ) -> DbResult<shamir_tx::repo_tx_gate::VisibleOutcome> {
        Ok(self.tx_gate().await?.await_visible(target_version).await)
    }

    /// Persisted clean horizon J of the replication journal: every version
    /// through J is covered by the runtime clean checkpoint: journal events or persisted gaps.
    /// cancel-safe: yes — retryable lazy journal initialization, then an atomic read.
    pub async fn clean_horizon(&self) -> DbResult<u64> {
        Ok(self.changefeed().await?.feed.clean_horizon())
    }

    /// Stop the journal writer so the next clean-horizon checkpoint fails.
    /// Failure-injection seam for shutdown-reporting tests only.
    #[doc(hidden)]
    pub async fn stop_journal_writer_for_test(&self) -> DbResult<()> {
        self.changefeed().await?.feed.shutdown_and_wait().await
    }

    /// Install an isolated journal fixture before lazy initialization.
    #[cfg(test)]
    pub(crate) fn install_journal_for_test(&self, handle: ChangefeedHandle) {
        assert!(self.core.durability_handle().changefeed.set(handle).is_ok());
    }

    /// Test-only read-side observation of writer persistence/confirmation.
    #[cfg(test)]
    pub(crate) async fn journal_feed_for_test(&self) -> DbResult<Arc<shamir_tx::RepoChangefeed>> {
        Ok(self.changefeed().await?.feed)
    }
}

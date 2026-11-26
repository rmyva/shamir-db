use std::sync::Arc;

use shamir_tx::{ChangelogEvent, RepoChangefeed, VersionGuard};

use crate::commit::core::RepoCore;

#[cfg(test)]
fn maybe_crash_before_publish(
    prepared: Option<&(Arc<ChangelogEvent>, Option<Arc<RepoChangefeed>>)>,
) {
    if !std::env::var("SHAMIR_TEST_CRASH_AFTER").is_ok_and(|phase| phase == "pre_publish") {
        return;
    }
    if let (Ok(path), Some((event, _))) = (
        std::env::var("SHAMIR_COLD_RECONCILE_EXPECTED_EVENTS"),
        prepared,
    ) {
        let mut file = std::fs::File::create(path).expect("create pre-publish oracle");
        rmp_serde::encode::write(&mut file, event.as_ref()).expect("encode pre-publish event");
        std::io::Write::flush(&mut file).expect("flush pre-publish oracle");
        file.sync_all().expect("sync pre-publish oracle");
    }
    if let Ok(marker) = std::env::var("SHAMIR_COLD_RECONCILE_PHASE_MARKER") {
        let mut file = std::fs::File::create(marker).expect("create pre-publish marker");
        std::io::Write::write_all(&mut file, b"pre_publish").expect("write pre-publish marker");
        file.sync_all().expect("sync pre-publish marker");
    }
    std::process::abort();
}

/// Publish only at the point of no return, after WAL success and the last
/// crash/failure exit. No event is ever enqueued for a version that never
/// publishes: preparation alone has no journal/gap side effect, and this
/// synchronous enqueue-or-gap + commit pair has no await or fallible step
/// between them. Cancellation cannot split the pair. Closed marks a gap in
/// `journal_enqueue`; Full defers the event into the repair-pending set
/// (`repair_pending`) and the drainer's `drain_repair_pending` lands it on a
/// later pass. Failed initialization records its gap here, not at
/// preparation time. The returned Arc is the SAME allocation used by the
/// channel and later live broadcast (one allocation, as in the old emit).
#[inline]
pub fn publish_journaled(
    repo: &RepoCore,
    guard: VersionGuard,
    prepared: Option<(Arc<ChangelogEvent>, Option<Arc<RepoChangefeed>>)>,
) -> Option<Arc<ChangelogEvent>> {
    #[cfg(test)]
    maybe_crash_before_publish(prepared.as_ref());
    let event = prepared.map(|(event, feed)| {
        match feed {
            Some(feed) => feed.journal_enqueue(&event),
            None => repo.record_pending_changefeed_gap(event.commit_version),
        }
        event
    });
    guard.commit();
    event
}

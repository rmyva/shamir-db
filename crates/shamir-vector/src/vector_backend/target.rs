//! Compaction double-write target (RV-INDEX-9F62C8DF).
//!
//! A target receives the mirror of every mutation while a compaction
//! generation is armed. Any mirror failure (or same-RID ordering ambiguity)
//! permanently fails the target: a failed target is NEVER published — the
//! old primary stays authoritative and compaction is retried later.

use crate::adapter::VectorAdapter;
use shamir_types::types::common::THasher;
use shamir_types::types::record_id::RecordId;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

pub(crate) struct CompactionTarget {
    pub(crate) adapter: Arc<dyn VectorAdapter>,
    failed: AtomicBool,
    reason: OnceLock<String>,
    /// Nonblocking per-generation RID claims (same-RID overlap detection).
    /// Sync accessors only — probed/released on runtime worker threads
    /// (same #589 hazard class as `compaction_deleted_rids`).
    rid_claims: scc::HashMap<RecordId, (), THasher>,
    #[cfg(test)]
    fail_next_mirror: AtomicBool,
}

impl CompactionTarget {
    pub(crate) fn new(adapter: Arc<dyn VectorAdapter>) -> Arc<Self> {
        Arc::new(Self {
            adapter,
            failed: AtomicBool::new(false),
            reason: OnceLock::new(),
            rid_claims: scc::HashMap::with_hasher(THasher::default()),
            #[cfg(test)]
            fail_next_mirror: AtomicBool::new(false),
        })
    }

    pub(crate) fn failed(&self) -> bool {
        self.failed.load(Ordering::Acquire)
    }

    pub(crate) fn fail_reason(&self) -> Option<&str> {
        self.reason.get().map(String::as_str)
    }

    /// Permanently invalidate the generation. Happens-before the final
    /// lease-count release (callers fail BEFORE dropping the lease), so the
    /// coordinator's post-drain check sees it (Release store / Acquire load).
    pub(crate) fn fail(&self, reason: String) {
        let _ = self.reason.set(reason);
        self.failed.store(true, Ordering::Release);
    }

    /// Claim a RID for the duration of one lease. `false` = already claimed.
    pub(crate) fn claim(&self, rid: RecordId) -> bool {
        self.rid_claims.insert_sync(rid, ()).is_ok()
    }

    pub(crate) fn release(&self, rid: RecordId) {
        let _ = self.rid_claims.remove_sync(&rid);
    }

    /// Mirror an upsert. Never fails the user's write: an error only marks
    /// the generation failed.
    pub(crate) async fn mirror_upsert(&self, rid: RecordId, v: &[f32]) {
        if !self.mirror_ready() {
            return;
        }
        if let Err(e) = self.adapter.upsert(rid, v).await {
            self.fail(format!("mirror upsert failed for rid {rid:?}: {e}"));
        }
    }

    /// Mirror a delete (same contract as [`Self::mirror_upsert`]).
    pub(crate) async fn mirror_delete(&self, rid: RecordId) {
        if !self.mirror_ready() {
            return;
        }
        if let Err(e) = self.adapter.delete(rid).await {
            self.fail(format!("mirror delete failed for rid {rid:?}: {e}"));
        }
    }

    /// Mirror a committed batch (same contract as [`Self::mirror_upsert`]).
    pub(crate) async fn mirror_committed(&self, vecs: &[(RecordId, Vec<f32>)]) {
        if !self.mirror_ready() {
            return;
        }
        if let Err(e) = self.adapter.apply_committed_vectors(vecs).await {
            self.fail(format!("mirror committed batch failed: {e}"));
        }
    }

    /// `false` → skip the mirror (generation already failed, or a test
    /// injected the failure).
    fn mirror_ready(&self) -> bool {
        if self.failed() {
            return false;
        }
        #[cfg(test)]
        if self.fail_next_mirror.swap(false, Ordering::AcqRel) {
            self.fail("injected mirror failure (fail_next_mirror)".into());
            return false;
        }
        true
    }

    /// Test-only: make the NEXT mirror operation fail (and fail the target).
    #[cfg(test)]
    pub(crate) fn set_fail_next_mirror(&self) {
        self.fail_next_mirror.store(true, Ordering::Release);
    }
}

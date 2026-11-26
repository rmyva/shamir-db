use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use shamir_storage::error::DbResult;
use tokio::sync::Notify;

use super::mem_frames::MemFrames;
use crate::segment::frame_reader::MAX_FRAME_BYTES;
use crate::segment_set::SegmentSet;
use crate::wal_entry_v2::WalEntryV2;
use crate::wal_segment::WalSegment;

// ---- Test-only FILE-sink sync pause (B3 coalescing test) ----
// Mirrors MemSink's pause handshake for the File arm so an engine-level test
// can park a coalesced sync_now round mid-fsync deterministically. Compiled
// unconditionally (like `arm_fail_next_sync_for_test`) because the consumer
// is a CROSS-CRATE test (shamir-engine), where this crate's `cfg(test)` is
// off; the flag is never armed outside tests, so production cost is one
// relaxed load per fsync.
static FILE_FAIL_NEXT_SYNC: AtomicBool = AtomicBool::new(false);
// Test-only seal-fsync fault (RV-WAL-3B29232E) lives on the individual
// SegmentSet/WAL instance; it is not shared between repositories.
static FILE_PAUSE_NEXT_SYNC: AtomicBool = AtomicBool::new(false);
static FILE_SYNC_PAUSED: Notify = Notify::const_new();
static FILE_SYNC_RESUME: Notify = Notify::const_new();

async fn file_sync_pause_point() {
    if FILE_PAUSE_NEXT_SYNC.swap(false, Ordering::AcqRel) {
        FILE_SYNC_PAUSED.notify_one();
        FILE_SYNC_RESUME.notified().await;
    }
}

impl WalSink {
    /// Test-only: fail this file-backed sink's next SEAL fsync inside
    /// `SegmentSet::seal_and_rotate`. The fault belongs to this repository's
    /// WAL instance and is consumed once after frames are written.
    #[doc(hidden)]
    pub fn arm_fail_next_seal_sync_for_test(&self) {
        match self {
            Self::File(segments) => segments.arm_fail_next_seal_sync_for_test(),
            Self::Mem(_) => panic!("seal fsync failure is File-only"),
        }
    }

    /// Test-only: take the pending seal-fsync fault flag (one-shot).
    pub(crate) fn take_fail_next_seal_sync_for_test(flag: &AtomicBool) -> bool {
        flag.swap(false, Ordering::AcqRel)
    }

    /// Test-only: park the next File-sink `sync()` until
    /// [`Self::resume_file_sync`]. Cross-crate test seam (see the statics
    /// above for why this is not `cfg(test)`).
    #[doc(hidden)]
    pub fn arm_pause_next_file_sync() {
        FILE_PAUSE_NEXT_SYNC.store(true, Ordering::Release);
    }

    /// Test-only: wait until a File-sink `sync()` is parked.
    #[doc(hidden)]
    pub async fn wait_file_sync_paused() {
        FILE_SYNC_PAUSED.notified().await;
    }

    /// Test-only: release the parked File-sink `sync()`.
    #[doc(hidden)]
    pub fn resume_file_sync() {
        FILE_SYNC_RESUME.notify_one();
    }
}

/// In-RAM WAL sink — mirrors [`WalSegment`]'s interface using immutable batches. Used by in-memory repos so the
/// group-commit write path is identical to disk repos (one code path),
/// while giving genuine in-process replay parity: the buffer lives in the
/// same RAM as the repo and survives a *simulated* same-process "crash"
/// exactly as `InMemoryStore` does.
pub struct MemSink {
    // O(1) head snapshot and batch publication; corpus work runs on blocking workers.
    frames: Arc<MemFrames>,
    next_seq: AtomicU64,
    /// Highest `commit_version` ever appended (monotonic `fetch_max`).
    /// Mirrors [`WalSegment::max_committed`] so the in-RAM sink carries the
    /// same watermark for future frame-level GC (F6 truncation parity, I7).
    max_committed: AtomicU64,
    /// Test-only fault-injection knob (audit §1.6 / task #531). When armed,
    /// the NEXT `append_batch` fails BEFORE any frame is pushed — a genuine
    /// failure of the real write path (not a simulated rollback), so the
    /// group-commit leader's circuit-breaker fires and `append_many` returns
    /// `Err` while ZERO of the batch's frames survive to a later replay. This
    /// exercises the all-or-nothing atomicity claim `append_many`'s doc makes.
    /// Cross-crate test seam remains in production Mem sinks (the `File` hot
    /// path is untouched — it stays an enum, no dyn dispatch).
    fail_next_append: AtomicBool,
    // No crate test-util feature: cross-crate sync-fault seam remains in production Mem sinks.
    fail_next_sync: AtomicBool,
    #[cfg(test)]
    pause_next_append: AtomicBool,
    #[cfg(test)]
    append_paused: Notify,
    #[cfg(test)]
    append_resume: Notify,
    #[cfg(test)]
    pause_next_sync: AtomicBool,
    #[cfg(test)]
    sync_paused: Notify,
    #[cfg(test)]
    sync_resume: Notify,
}

impl MemSink {
    fn new() -> Self {
        Self {
            frames: Arc::new(MemFrames::default()),
            next_seq: AtomicU64::new(0),
            max_committed: AtomicU64::new(0),
            fail_next_append: AtomicBool::new(false),
            fail_next_sync: AtomicBool::new(false),
            #[cfg(test)]
            pause_next_append: AtomicBool::new(false),
            #[cfg(test)]
            append_paused: Notify::new(),
            #[cfg(test)]
            append_resume: Notify::new(),
            #[cfg(test)]
            pause_next_sync: AtomicBool::new(false),
            #[cfg(test)]
            sync_paused: Notify::new(),
            #[cfg(test)]
            sync_resume: Notify::new(),
        }
    }

    /// Highest `commit_version` ever appended to this in-RAM sink.
    pub fn max_committed(&self) -> u64 {
        self.max_committed.load(Ordering::Acquire)
    }

    /// Test-only: arm the next `append_batch` to fail. The failing call
    /// pushes NO frames (returns before publication), so the batch
    /// leaves zero trace — mirroring a `File` sink quarantining a segment on
    /// a partial write. One-shot: the flag clears itself on the failing call.
    /// Cross-crate test seam remains in production Mem sinks.
    #[doc(hidden)]
    pub fn arm_fail_next_append(&self) {
        self.fail_next_append.store(true, Ordering::Release);
    }

    /// Test-only: direct access to the frames mutex (RV-WAL-B8E9884C
    /// regression: `has_truncatable` must answer without taking this lock).
    #[doc(hidden)]
    #[cfg(test)]
    pub(crate) fn with_frames_lock_for_test(&self, f: impl FnOnce()) {
        self.frames.with_lock(f);
    }
}

impl Default for MemSink {
    fn default() -> Self {
        Self::new()
    }
}

/// WAL storage sink — file-backed (disk repos) or in-RAM (in-memory repos).
/// Enum, not trait: no dyn dispatch on the hot path.
pub enum WalSink {
    /// Real append-only directory of numbered segments (F6b). write() =
    /// level 2, sync_all = level 3. Truncation deletes whole sealed
    /// segments once their data is durable in history.
    File(SegmentSet),
    /// In-RAM immutable-batch segment. append = publish, sync = no-op (no fsync in
    /// RAM), replay = decode every stored payload.
    Mem(MemSink),
}

impl WalSink {
    /// Construct a fresh in-memory sink.
    pub fn mem() -> Self {
        Self::Mem(MemSink::default())
    }

    /// Test-only (task #531 / RV-WAL-3B29232E): arm the next `append_batch`
    /// on a `Mem` sink to fail. Panics on a `File` sink (fault injection is a
    /// `Mem`-only knob — production durability runs on `File` and its design
    /// is untouched). Cross-crate test seam, hence not `cfg(test)`.
    #[doc(hidden)]
    pub fn arm_fail_next_append_for_test(&self) {
        match self {
            Self::Mem(m) => m.arm_fail_next_append(),
            Self::File(_) => panic!("arm_fail_next_append is Mem-only"),
        }
    }

    // Single-consumer test handshakes: notify_one retains a permit when the
    // test observes the pause after the sink has signalled it. No sleeps.
    #[cfg(test)]
    fn mem_for_test(&self) -> &MemSink {
        match self {
            Self::Mem(mem) => mem,
            Self::File(_) => panic!("pause/fault injection is Mem-only"),
        }
    }

    #[cfg(test)]
    pub(crate) fn pause_mem_replay(
        &self,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        std::sync::mpsc::Sender<()>,
    ) {
        self.mem_for_test().frames.pause_replay()
    }

    #[cfg(test)]
    pub(crate) fn pause_mem_truncate(
        &self,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        std::sync::mpsc::Sender<()>,
    ) {
        self.mem_for_test().frames.pause_truncate()
    }

    #[cfg(test)]
    pub(crate) fn arm_pause_next_append(&self) {
        self.mem_for_test()
            .pause_next_append
            .store(true, Ordering::Release);
    }

    #[cfg(test)]
    pub(crate) async fn wait_append_paused(&self) {
        self.mem_for_test().append_paused.notified().await;
    }

    #[cfg(test)]
    pub(crate) fn resume_append(&self) {
        self.mem_for_test().append_resume.notify_one();
    }

    #[cfg(test)]
    pub(crate) fn arm_pause_next_sync(&self) {
        self.mem_for_test()
            .pause_next_sync
            .store(true, Ordering::Release);
    }

    #[cfg(test)]
    pub(crate) async fn wait_sync_paused(&self) {
        self.mem_for_test().sync_paused.notified().await;
    }

    #[cfg(test)]
    pub(crate) fn resume_sync(&self) {
        self.mem_for_test().sync_resume.notify_one();
    }

    #[doc(hidden)]
    pub fn arm_fail_next_sync(&self) {
        match self {
            Self::Mem(mem) => mem.fail_next_sync.store(true, Ordering::Release),
            Self::File(_) => FILE_FAIL_NEXT_SYNC.store(true, Ordering::Release),
        }
    }

    pub async fn append_batch(&self, payloads: Vec<Vec<u8>>, max_version: u64) -> DbResult<u64> {
        match self {
            Self::File(seg) => seg.append_batch(payloads, max_version).await,
            Self::Mem(m) => {
                #[cfg(test)]
                if m.pause_next_append.swap(false, Ordering::AcqRel) {
                    m.append_paused.notify_one();
                    m.append_resume.notified().await;
                }
                // Test-only fault injection (task #531): fail the whole batch
                // BEFORE touching `next_seq` / `frames`, so no partial state
                // survives. A genuine failure of the real Mem write path —
                // the leader sees `is_ok() == false`, trips the circuit
                // breaker, and every caller gets `Err`. Nothing is pushed, so
                // a subsequent replay sees ZERO of the batch's entries.
                // Cross-crate test seam remains in production Mem sinks.
                if m.fail_next_append.swap(false, Ordering::AcqRel) {
                    return Err(shamir_storage::error::DbError::Storage(
                        "MemSink injected append_batch failure (task #531)".into(),
                    ));
                }
                if payloads
                    .iter()
                    .any(|payload| payload.len() > MAX_FRAME_BYTES)
                {
                    return Err(shamir_storage::error::DbError::Storage(format!(
                        "WalSegment payload exceeds {MAX_FRAME_BYTES}-byte frame limit"
                    )));
                }
                m.max_committed.fetch_max(max_version, Ordering::AcqRel);
                if payloads.is_empty() {
                    return Ok(m.next_seq.load(Ordering::Acquire));
                }
                let n = payloads.len() as u64;
                let last_seq = m.next_seq.fetch_add(n, Ordering::AcqRel) + n - 1;
                let tagged: Vec<_> = payloads
                    .into_iter()
                    .map(|p| {
                        let version = WalEntryV2::decode(&p)
                            .map(|entry| entry.commit_version)
                            .unwrap_or(0);
                        m.max_committed.fetch_max(version, Ordering::AcqRel);
                        let tag = if version == 0 {
                            0
                        } else {
                            max_version.max(version)
                        };
                        (tag, p)
                    })
                    .collect();
                m.frames.append(tagged);
                Ok(last_seq)
            }
        }
    }

    /// Worker-only exact snapshot; pin the captured active file for its fsync.
    pub(crate) async fn sync_snapshot(
        &self,
    ) -> DbResult<(Vec<WalEntryV2>, Option<Arc<WalSegment>>)> {
        match self {
            Self::File(segments) => {
                let (entries, active) = segments.sync_snapshot().await?;
                Ok((entries, Some(active)))
            }
            Self::Mem(_) => Ok((self.replay().await?, None)),
        }
    }

    pub(crate) async fn sync_captured(&self, active: Option<&WalSegment>) -> DbResult<()> {
        if let Some(active) = active {
            file_sync_pause_point().await;
            if FILE_FAIL_NEXT_SYNC.swap(false, Ordering::AcqRel) {
                return Err(shamir_storage::error::DbError::Storage(
                    "File sink injected sync failure".into(),
                ));
            }
            match self {
                Self::File(segments) => segments.sync_active(active).await,
                Self::Mem(_) => Err(shamir_storage::error::DbError::Internal(
                    "captured file requires File sink".into(),
                )),
            }
        } else {
            self.sync().await
        }
    }

    pub async fn sync(&self) -> DbResult<()> {
        match self {
            Self::File(seg) => {
                file_sync_pause_point().await;
                if FILE_FAIL_NEXT_SYNC.swap(false, Ordering::AcqRel) {
                    return Err(shamir_storage::error::DbError::Storage(
                        "File sink injected sync failure".into(),
                    ));
                }
                seg.sync().await
            }
            Self::Mem(_m) => {
                #[cfg(test)]
                {
                    if _m.pause_next_sync.swap(false, Ordering::AcqRel) {
                        _m.sync_paused.notify_one();
                        _m.sync_resume.notified().await;
                    }
                }
                if _m.fail_next_sync.swap(false, Ordering::AcqRel) {
                    return Err(shamir_storage::error::DbError::Storage(
                        "MemSink injected sync failure".into(),
                    ));
                }
                Ok(())
            }
        }
    }

    /// Highest `commit_version` ever appended through this sink (monotonic).
    /// Drives F6 segment-level / frame-level truncation.
    pub fn max_committed(&self) -> u64 {
        match self {
            Self::File(seg) => seg.max_committed(),
            Self::Mem(m) => m.max_committed(),
        }
    }

    pub async fn replay(&self) -> DbResult<Vec<WalEntryV2>> {
        match self {
            Self::File(seg) => seg.replay().await,
            Self::Mem(m) => m.frames.replay().await,
        }
    }

    /// Truncate every record fully durable in history — i.e. whose
    /// `commit_version` is in `(0, durable]`. Returns the count reclaimed
    /// (deleted sealed segments for `File`, dropped frames for `Mem`).
    ///
    /// `File` delegates to [`SegmentSet::truncate_below`] (deletes whole
    /// sealed segments; the active segment and any pin are untouched).
    /// `Mem` drops non-pinned frames whose batch `max_version` is in `(0, durable]`,
    /// keeping any pinned (`v == 0`) frame and any frame above the durable
    /// watermark (I7). fsync-gate (I2) is the caller's responsibility — it
    /// flushes history before invoking this.
    pub async fn truncate_below(&self, durable: u64) -> DbResult<usize> {
        match self {
            Self::File(segset) => segset.truncate_below(durable).await,
            Self::Mem(m) => m.frames.truncate(durable).await,
        }
    }

    /// Cheap probe: is there anything truncatable at `durable`? Gates the
    /// (relatively expensive) history-flush + truncate in the drainer so it
    /// fires only on a segment/frame boundary, never per-commit (I2).
    ///
    /// `File`: any unpinned sealed segment with `0 < max_version <= durable`.
    /// `Mem`: any frame with `0 < v <= durable`.
    pub fn has_truncatable(&self, durable: u64) -> bool {
        match self {
            Self::File(segset) => segset.has_truncatable(durable),
            Self::Mem(m) => m.frames.eligible(durable),
        }
    }

    pub async fn sealed_retained_bytes(&self) -> DbResult<u64> {
        match self {
            Self::File(segset) => segset.sealed_retained_bytes().await,
            Self::Mem(_) => Ok(0),
        }
    }

    // ---- Sync scrape accessors for the §2.4 WAL gauges (never on the
    // append path). `Mem` reports 0: `shamir_wal_backend_mem` already tells
    // the operator the backend, and the in-RAM sink has no on-disk size.

    /// Retained segment FILES (sealed + active).
    pub(crate) fn segment_count(&self) -> u64 {
        match self {
            Self::File(segset) => segset.segment_count(),
            Self::Mem(_) => 0,
        }
    }

    /// Approximate on-disk WAL size (active + sealed).
    pub(crate) fn wal_size_bytes(&self) -> u64 {
        match self {
            Self::File(segset) => segset.approx_size_bytes(),
            Self::Mem(_) => 0,
        }
    }

    /// Cumulative WAL payload bytes appended.
    pub(crate) fn bytes_written_total(&self) -> u64 {
        match self {
            Self::File(segset) => segset.bytes_written_total(),
            Self::Mem(_) => 0,
        }
    }

    /// Distance from `durable_version` to the oldest reclaimable-but-retained
    /// sealed segment boundary (0 when nothing is truncatable).
    pub(crate) fn truncation_lag_versions(&self, durable_version: u64) -> u64 {
        match self {
            Self::File(segset) => segset.truncation_lag_versions(durable_version),
            Self::Mem(_) => 0,
        }
    }
}

use std::sync::atomic::Ordering;
use std::sync::Arc;

use shamir_collections::TFxMap;
use shamir_wal::{WalEntryV2, WalOpV2};

use super::Drainer;

impl Drainer {
    /// Record that the commit path applied `(commit_version, table_token)`'s
    /// index postings inline, so [`drain_step`](Self::drain_step) skips that
    /// entry's `IndexPut` / `IndexDel` ops for the table. Must be called
    /// before the version is published (the drain only sees published ones).
    pub(crate) fn mark_index_applied(&self, commit_version: u64, table_token: u64) {
        // offer precedes inline materialization, even when the window is full.
        // Clone keys out before touching the second map: never nest bucket guards.
        let keys = {
            let applied = self
                .index_applied
                .entry_sync((commit_version, table_token))
                .and_modify(|state| state.0 = true)
                .or_insert((true, Vec::new()));
            // A batch containing only BumpFtsStats has no WAL posting keys,
            // but must still preserve the existing inline-applied membership.
            applied.get().1.clone()
        };
        for key in keys {
            self.record_posting_applied(commit_version, table_token, key);
        }
    }

    pub(super) fn record_posting_applied(&self, version: u64, table: u64, key: bytes::Bytes) {
        self.posting_applied
            .entry_sync((table, key))
            .and_modify(|highest| *highest = (*highest).max(version))
            .or_insert(version);
    }

    pub(super) fn posting_superseded(&self, version: u64, table: u64, key: &bytes::Bytes) -> bool {
        self.posting_applied
            .read_sync(&(table, key.clone()), |_, highest| *highest > version)
            .unwrap_or(false)
    }

    #[cfg(test)]
    pub(crate) fn posting_version_for_test(&self, table: u64, key: &bytes::Bytes) -> Option<u64> {
        self.posting_applied
            .read_sync(&(table, key.clone()), |_, version| *version)
    }

    pub(super) fn index_applied_inline(&self, commit_version: u64, table_token: u64) -> bool {
        self.index_applied
            .read_sync(&(commit_version, table_token), |_, state| state.0)
            .unwrap_or(false)
    }

    pub(super) fn prune_index_applied(&self, durable: u64) {
        // The bucket-locked predicates preserve racing newer inline updates.
        // No unresolved orphan can lie in the contiguous durable prefix.
        self.index_applied
            .retain_sync(|(version, _), _| *version > durable);
        self.posting_applied
            .retain_sync(|_, highest| *highest > durable);
    }

    pub(super) fn clear_index_applied(&self, entry: &WalEntryV2) {
        for op in &entry.ops {
            if let WalOpV2::IndexPut {
                table_id_interned, ..
            }
            | WalOpV2::IndexDel {
                table_id_interned, ..
            } = op
            {
                self.index_applied
                    .remove_sync(&(entry.commit_version, *table_id_interned));
            }
        }
    }

    fn remember_index_keys(&self, entry: &WalEntryV2) {
        let mut keys: TFxMap<u64, Vec<bytes::Bytes>> = TFxMap::default();
        for op in &entry.ops {
            if let WalOpV2::IndexPut {
                table_id_interned,
                key,
                ..
            }
            | WalOpV2::IndexDel {
                table_id_interned,
                key,
                ..
            } = op
            {
                keys.entry(*table_id_interned)
                    .or_default()
                    .push(key.clone());
            }
        }
        for (table, keys) in keys {
            // Reseeding must not erase an already-applied inline marker.
            let _ = self
                .index_applied
                .insert_sync((entry.commit_version, table), (false, keys));
        }
    }

    pub(crate) fn mark_counter_applied(&self, version: u64, table: u64) {
        let _ = self.counter_applied.insert_sync((version, table), ());
    }

    pub(super) fn counter_applied(&self, version: u64, table: u64) -> bool {
        self.counter_applied.contains_sync(&(version, table))
    }
    /// Wake the drain loop. Called by the producer (commit path) in P1d-2b
    /// after publishing a version; a no-op safety net otherwise.
    pub fn wake(&self) {
        self.notify.notify_one();
    }

    /// Total versions drained across the drainer's lifetime.
    pub fn drained_total(&self) -> u64 {
        self.drained_total.load(Ordering::Relaxed)
    }

    /// Offer a WAL entry to the drain window (called by the commit path
    /// AFTER wal.begin_grouped returns durable, Op #2 Stage 2).
    /// Insert is idempotent at the commit_version key. Lock-free.
    ///
    /// PT 1: backpressure check uses `window_depth.load(Relaxed)` —
    /// O(1) atomic — instead of `scc::TreeIndex::len()` which iterates
    /// the whole tree per call (re-introducing the O(N) cliff Op #2
    /// killed). The depth is approximate (racy +∆ ≤ concurrent committer
    /// count) and the cap is soft, so this is safe.
    pub fn offer(&self, entry: Arc<WalEntryV2>) {
        self.remember_index_keys(&entry);
        let hw = self.window_high_watermark.load(Ordering::Relaxed);
        if self.window_depth.load(Ordering::Relaxed) >= hw {
            // Backpressure: drop, recovery is via drain_step's gap-reseed.
            self.offer_dropped_total.fetch_add(1, Ordering::Relaxed);
            return;
        }
        if self.window.insert_sync(entry.commit_version, entry).is_ok() {
            self.window_depth.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Seed the window from a cold-start `wal.recover()` result.
    /// Called ONCE during Drainer::spawn (or RepoInstance::drainer init).
    pub fn seed_from_recover(&self, entries: Vec<WalEntryV2>) {
        for e in entries {
            self.remember_index_keys(&e);
            let v = e.commit_version;
            if self.window.insert_sync(v, Arc::new(e)).is_ok() {
                self.window_depth.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Test-only window length probe (replaces public `len` to keep API tight).
    #[cfg(test)]
    #[allow(clippy::disallowed_methods)] // O(N) ack: test-only accessor; depth mirror is window_depth()
    pub fn window_len(&self) -> usize {
        self.window.len()
    }

    /// Test-only: collect window keys in ascending order.
    #[cfg(test)]
    pub fn window_keys(&self) -> Vec<u64> {
        let guard = scc::Guard::new();
        self.window.iter(&guard).map(|(k, _)| *k).collect()
    }

    /// Test-only: retrieve a window entry by commit_version.
    #[cfg(test)]
    pub fn window_entry(&self, version: u64) -> Option<Arc<WalEntryV2>> {
        self.window.peek_with(&version, |_, v| Arc::clone(v))
    }

    /// Total entries dropped by `offer` due to backpressure (telemetry).
    pub fn offer_dropped_total(&self) -> u64 {
        self.offer_dropped_total.load(Ordering::Relaxed)
    }

    /// Set the window high-watermark (soft cap for `offer` backpressure).
    pub fn set_window_high_watermark(&self, hw: usize) {
        self.window_high_watermark.store(hw, Ordering::Relaxed);
    }

    /// Test-only: read the current window high-watermark.
    #[cfg(test)]
    pub fn window_high_watermark(&self) -> usize {
        self.window_high_watermark.load(Ordering::Relaxed)
    }

    /// Test-only: remove a window entry by commit_version (for gap simulation).
    #[cfg(test)]
    pub(crate) fn window_remove_for_test(&self, version: u64) {
        if self.window.remove_sync(&version) {
            self.window_depth.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Test-only: read the approximate window depth (atomic mirror).
    #[cfg(test)]
    pub fn window_depth(&self) -> usize {
        self.window_depth.load(Ordering::Relaxed)
    }

    /// Test-only: hold the same replay lock used by drain_step.
    #[cfg(test)]
    pub(crate) async fn replay_lock_for_test(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.replaying.lock().await
    }
}

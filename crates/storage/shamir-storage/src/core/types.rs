use crate::error::{DbError, DbResult};
use crate::key_bytes::KeyBytes;
use async_trait::async_trait;
use bytes::Bytes;
use futures::{stream::Stream, StreamExt};
use std::pin::Pin;
use std::sync::Arc;

use crate::storage_membuffer::Slot;

pub type RecordKey = KeyBytes;

pub(crate) type RecordStream =
    Pin<Box<dyn Stream<Item = Result<Vec<(RecordKey, Bytes)>, DbError>> + Send>>;

/// Audit finding 2.3 (task #530) — merge a sorted dirty-overlay snapshot on
/// top of a sorted `inner` record stream, WITHOUT draining the overlay to
/// disk first.
///
/// This is a classic 2-way sorted merge (mirrors
/// `MvccStore::current_stream`'s overlay-vs-history merge): both the `overlay`
/// snapshot and the `inner` stream are key-sorted (ascending when
/// `reverse == false`, descending when `reverse == true` — the overlay is
/// pre-sorted by the caller to match), so a single linear pass yields a merged
/// key-ordered stream. Semantics:
///
/// * An overlay entry ALWAYS wins over `inner` for the same key (it is the
///   newer, not-yet-flushed value). `Slot::Live` → emit the overlay value;
///   `Slot::Tombstone` → EXCLUDE the key (even if `inner` still holds stale
///   data for it).
/// * Overlay-only keys (present in the dirty buffer but not yet in `inner`)
///   are emitted in their sorted position during the merge, and any that sort
///   after the last `inner` key are flushed in the overlay-only tail phase.
/// * Ordering across the merged output is preserved, so range / prefix scans
///   keep their ascending / descending guarantee.
///
/// `inner` MUST yield keys in the requested order (ascending, or descending
/// for reverse) — every `Store` scan upholds this per the trait contract.
pub(crate) fn merge_overlay_stream(
    overlay: Vec<(RecordKey, Slot)>,
    inner: RecordStream,
    batch_size: usize,
    reverse: bool,
) -> RecordStream {
    Box::pin(async_stream::stream! {
        let batch_cap = batch_size.max(1);
        let mut ov = overlay.into_iter().peekable();
        let mut out: Vec<(RecordKey, Bytes)> = Vec::with_capacity(batch_cap);

        // `ord(a, b)` is the "a should come before b in output order" test.
        // Ascending: a < b. Descending: a > b.
        let before = |a: &RecordKey, b: &RecordKey| -> bool {
            if reverse {
                a > b
            } else {
                a < b
            }
        };

        futures::pin_mut!(inner);
        while let Some(batch) = futures::StreamExt::next(&mut inner).await {
            let batch = match batch {
                Ok(b) => b,
                Err(e) => {
                    yield Err(e);
                    return;
                }
            };
            for (ik, iv) in batch {
                // Emit every overlay entry that sorts strictly before the
                // current inner key (overlay-only keys interleaved in order).
                while let Some((ok, _)) = ov.peek() {
                    if before(ok, &ik) {
                        let (ok, oslot) = ov.next().unwrap();
                        if let Slot::Live(v) = oslot {
                            out.push((ok, v));
                            if out.len() >= batch_cap {
                                yield Ok(std::mem::take(&mut out));
                                out = Vec::with_capacity(batch_cap);
                            }
                        }
                        // Tombstone-only overlay key: nothing to emit.
                    } else {
                        break;
                    }
                }
                // Overlay entry for the EXACT inner key wins (newer write).
                if let Some((ok, _)) = ov.peek() {
                    if *ok == ik {
                        let (ok, oslot) = ov.next().unwrap();
                        match oslot {
                            Slot::Live(v) => {
                                out.push((ok, v));
                                if out.len() >= batch_cap {
                                    yield Ok(std::mem::take(&mut out));
                                    out = Vec::with_capacity(batch_cap);
                                }
                            }
                            // Tombstone masks the stale inner value → skip.
                            Slot::Tombstone => {}
                        }
                        continue;
                    }
                }
                // No overlay entry for this key → inner value stands.
                out.push((ik, iv));
                if out.len() >= batch_cap {
                    yield Ok(std::mem::take(&mut out));
                    out = Vec::with_capacity(batch_cap);
                }
            }
        }

        // Overlay-only tail: any remaining overlay entries (keys `inner` never
        // yielded). Live entries are emitted; tombstones are dropped.
        for (ok, oslot) in ov {
            if let Slot::Live(v) = oslot {
                out.push((ok, v));
                if out.len() >= batch_cap {
                    yield Ok(std::mem::take(&mut out));
                    out = Vec::with_capacity(batch_cap);
                }
            }
        }

        if !out.is_empty() {
            yield Ok(out);
        }
    })
}

/// A single mixed-op for `Store::transact`. Represents either a
/// `set` or a `remove` against a `RecordKey`.
///
/// Used by the transactional engine layer to bundle heterogeneous
/// writes into one atomic batch that backends with native write
/// transactions can commit as a single unit.
#[derive(Debug, Clone)]
pub enum KvOp {
    Set(RecordKey, Bytes),
    Remove(RecordKey),
}

/// An asynchronous, key-value store trait that operates on raw bytes.
///
/// This trait provides a low-level storage abstraction. It is the responsibility
/// of the caller to handle serialization and deserialization of the values.
/// The key is fixed to `RecordKey`.
#[async_trait]
pub trait Store: Send + Sync {
    /// Inserts a new record with a generated `RecordKey`.
    async fn insert(&self, value: Bytes) -> DbResult<RecordKey>;

    /// Returns `true` if created, `false` if updated. The value is exact for
    /// serialized mutation paths of a wrapper; direct writes through another
    /// handle can race a backend probe unless that backend provides atomic
    /// compare-and-set semantics. Fjall's probe and write are not one atomic op.
    async fn set(&self, key: RecordKey, value: Bytes) -> DbResult<bool>;

    /// Retrieves a record's raw bytes by its `RecordKey`.
    async fn get(&self, key: RecordKey) -> DbResult<Bytes>;

    /// Vectored read — fetch many records in one logical call.
    ///
    /// Returns a vector parallel to `keys`: `Some(bytes)` for hits,
    /// `None` for keys that are not present (a missing key is NOT
    /// a `DbError::NotFound` — the caller decides per-key). Other
    /// storage-level errors fail the whole batch.
    ///
    /// Default impl loops over `self.get`, mapping NotFound to None.
    /// Disk backends override with a single transactional read to
    /// collapse N×`spawn_blocking` + N transaction setups into one.
    async fn get_many(&self, keys: Vec<RecordKey>) -> DbResult<Vec<Option<Bytes>>> {
        let mut out = Vec::with_capacity(keys.len());
        for k in keys {
            match self.get(k).await {
                Ok(b) => out.push(Some(b)),
                Err(DbError::NotFound(_)) => out.push(None),
                Err(e) => return Err(e),
            }
        }
        Ok(out)
    }

    /// Removes a record by its `RecordKey`; returns whether it existed.
    /// Exactness is subject to the same concurrent-backend limitation as
    /// [`Self::set`].
    async fn remove(&self, key: RecordKey) -> DbResult<bool>;

    /// Like [`Self::set`] but for callers that don't need the "was created"
    /// flag — skips the extra existence-check lookup some backends (Fjall)
    /// would otherwise need to compute it (task #613). Default impl just
    /// discards `set`'s flag; override where the backend can genuinely skip
    /// the check.
    async fn set_no_flag(&self, key: RecordKey, value: Bytes) -> DbResult<()> {
        self.set(key, value).await.map(|_| ())
    }

    /// Like [`Self::remove`] but for callers that don't need the "existed"
    /// flag (task #613). Default impl just discards `remove`'s flag.
    async fn remove_no_flag(&self, key: RecordKey) -> DbResult<()> {
        self.remove(key).await.map(|_| ())
    }

    /// Force the backend to make all writes-so-far durable.
    ///
    /// **Durability contract.** The basic `insert` / `set` / `remove`
    /// operations are *eventually* durable — backends are free to
    /// buffer writes in a WAL / page cache / background flusher and
    /// only fsync periodically. Callers that need a strict
    /// commit-boundary (an RPC reply, a transaction end, an explicit
    /// `FLUSH` request from a user) call `flush()` and await it.
    ///
    /// Default impl is a no-op for backends that have no buffered
    /// state (in-memory) or that already fsync per write at a lower
    /// layer.
    ///
    /// Buffered backends and wrappers MUST override.
    async fn flush(&self) -> DbResult<()> {
        Ok(())
    }

    /// Drain this store's buffered state into its durable backing WITHOUT
    /// forcing a backend-level durability barrier.
    ///
    /// Contract: after `flush_drain()` returns Ok, every value written
    /// through this store is in the durable backend's write path (journal /
    /// WAL / memtable), exactly as after `flush()` — the ONLY thing it may
    /// skip is the backend-wide fsync. Callers that drain many stores and
    /// then fsync the backend ONCE (see `Repo::persist_once`) use this to
    /// avoid N identical full-database fsyncs per shutdown pass.
    ///
    /// Default impl = `self.flush().await`: for backends where `flush()` IS
    /// the drain (in-memory no-op, backends that fsync per write), the two
    /// are equivalent, so non-participating backends keep today's behaviour
    /// unchanged. Buffered wrappers (MemBuffer, Cached) and Fjall override.
    async fn flush_drain(&self) -> DbResult<()> {
        self.flush().await
    }

    /// Apply a hot-reload of MemBuffer config. Default no-op
    /// (raw backends don't have a buffer layer). `MemBufferStore`
    /// overrides this to update its atomic config fields; wrappers
    /// like `CachedStore` should override to propagate to inner.
    ///
    /// Lives on the `Store` trait so the engine can call it on
    /// `Arc<dyn Store>` without downcasting — most backends ignore
    /// the call, the MemBuffer in the stack picks it up.
    async fn apply_buffer_config(
        &self,
        _config: &crate::storage_membuffer::MemBufferConfig,
    ) -> DbResult<()> {
        Ok(())
    }

    /// Return the unwrapped underlying backend, bypassing any wrapper
    /// layers (MemBuffer, Cached).
    ///
    /// Default: `None` — this Store is already raw. Wrappers override
    /// and return `Some(inner.clone())`.
    ///
    /// Used by the upcoming MvccStore at construction time to obtain
    /// a write path that bypasses write-back caches: tx commits go
    /// straight to the durable backend because durability IS the commit
    /// point.
    ///
    /// Note: returns `Option<Arc<dyn Store>>` rather than always-Some
    /// so non-wrapper backends can answer cheaply (a single None) — the
    /// caller iterates [`fully_unwrap_store`] only when it needs the
    /// chain-unwrapped raw store.
    async fn raw_backend(&self) -> Option<Arc<dyn Store>> {
        None
    }

    /// Insert many records in one logical batch — for backends that
    /// expose a transactional write API, this collapses N×fsync into
    /// one. Backends without such an override use the default loop.
    ///
    /// **Atomicity.** When a backend overrides this with a
    /// transactional impl, the batch is all-or-nothing. The default
    /// loop impl is NOT atomic — it inherits per-element semantics.
    /// Callers that need atomicity should not rely on the default.
    ///
    /// Returns one `RecordKey` per input value, in input order.
    async fn insert_many(&self, values: Vec<Bytes>) -> DbResult<Vec<RecordKey>> {
        let mut out = Vec::with_capacity(values.len());
        for v in values {
            out.push(self.insert(v).await?);
        }
        Ok(out)
    }

    /// Upsert many records in one logical batch. Same atomicity story
    /// as `insert_many`. Returns one `bool` per input pair, in input
    /// order — `true` if the record was created, `false` if it was
    /// updated.
    async fn set_many(&self, items: Vec<(RecordKey, Bytes)>) -> DbResult<Vec<bool>> {
        let mut out = Vec::with_capacity(items.len());
        for (k, v) in items {
            out.push(self.set(k, v).await?);
        }
        Ok(out)
    }

    /// Remove many records in one logical batch. Same atomicity story
    /// as `insert_many`. Returns one `bool` per input key, in input
    /// order — `true` if the record existed and was removed.
    async fn remove_many(&self, keys: Vec<RecordKey>) -> DbResult<Vec<bool>> {
        let mut out = Vec::with_capacity(keys.len());
        for k in keys {
            out.push(self.remove(k).await?);
        }
        Ok(out)
    }

    /// Apply a mixed-op batch (`Set` / `Remove`) sequentially. The
    /// DEFAULT implementation below applies each op one at a time and
    /// provides **NO cross-op atomicity** — a crash mid-batch, or a
    /// concurrent read landing between two ops, can observe a
    /// partially-applied state. Empty `ops` is a no-op.
    ///
    /// Backends may override this to use a native batch. Whether an
    /// override delivers whole-batch visibility atomicity is opt-in and
    /// queryable via [`Self::supports_atomic_transact`]. FjallStore and
    /// MirroredStore report `false`; partial state may be observable to
    /// a concurrent reader mid-batch. Callers whose correctness depends
    /// on whole-batch visibility atomicity should check the flag.
    ///
    /// Used by the transactional engine layer to bundle a tx commit into
    /// one apply (data + index postings + version-log + counter updates).
    ///
    /// # Production callers and their non-atomic tolerance
    ///
    /// F-85 (#913): NO production caller gates on the flag today — every
    /// one tolerates the `false` case by construction, so the flag serves
    /// as honest, queryable capability metadata rather than a gate any
    /// production path enforces. Current callers, grouped by HOW they
    /// tolerate a non-atomic batch:
    ///
    /// - **Self-healing settle / re-scan** (idempotent re-run converges):
    ///   `SortedIndexManager::rekey_postings` (#962) re-scans the old-id
    ///   prefix and re-applies until a pass finds nothing to move;
    ///   `base_index::IndexManager::apply_ops`, and
    ///   `write_ops::apply_index_ops` / `apply_index_ops_at_commit`,
    ///   write last-write-wins postings that are rebuildable from the
    ///   primary record set and idempotent under WAL replay.
    /// - **MVCC version-log** (watermark-gated visibility + WAL replay):
    ///   `MvccStore::set_versioned` / batched-set / `delete_versioned`
    ///   (`shamir-tx::mvcc_store`) and `write_committed_to_history` /
    ///   `write_committed_batch_to_history` (`mvcc_history`) land the
    ///   `(data + ts)` entries in one `transact`, but the reader-visible
    ///   floor is advanced (`guard.commit()` / `publish_committed_max`)
    ///   ONLY after the call returns `Ok`, so a concurrent reader never
    ///   observes a partial batch; crash-safety is the WAL's job (these
    ///   paths are documented `cancel-safe: NO`, recovered by replay).
    /// - **Non-MVCC / unattached tables** (WAL replay + redelivery):
    ///   `commit_phases` and `apply_replicated` write directly to `base`
    ///   only on the `None` branch (tables without a version-log);
    ///   recovery replays / re-delivers.
    /// - **Vector snapshot publish / generation flip** (pre-written
    ///   chunks + CRC'd manifest): `shamir-index::vector::snapshot`
    ///   writes new chunks BEFORE the single publish/flip `transact`, and
    ///   the manifest is a CRC'd `MetaEnvelope`, so `restore_on_open`
    ///   ignores a partially-published manifest and a non-atomic flip at
    ///   worst leaves stale old-gen chunks (ignored — the manifest points
    ///   unambiguously at the new gen).
    ///
    /// F-77 (#904): the prior contract text ("when a backend overrides
    /// `transact`, partial state is never observable") was an overpromise
    /// that `MirroredStore` silently violated for its ephemeral subset;
    /// the flag makes the capability machine-checkable.
    async fn transact(&self, ops: Vec<KvOp>) -> DbResult<()> {
        for op in ops {
            match op {
                KvOp::Set(k, v) => {
                    let _ = self.set(k, v).await?;
                }
                KvOp::Remove(k) => {
                    let _ = self.remove(k).await?;
                }
            }
        }
        Ok(())
    }

    /// Whether this backend's [`Self::transact`] delivers whole-batch
    /// VISIBILITY atomicity — i.e. a concurrent reader observes EITHER
    /// the full pre-batch state OR the full post-batch state, never a
    /// partially-applied batch.
    ///
    /// Default `false`: most backends either inherit the default
    /// per-op `transact` loop (no cross-op atomicity) or — like
    /// `MirroredStore` — override `transact` but still apply at least
    /// one subset per-key to a lock-free primary with no multi-key
    /// publish primitive. FjallStore also reports `false`: its raw reads
    /// can observe a partially applied native batch. Wrappers report their
    /// own capability; consult the implementation before assuming it.
    ///
    /// **Callers whose correctness DEPENDS on whole-batch visibility
    /// atomicity should check this flag** before assuming every
    /// `transact` override delivers it — see [`Self::transact`]'s
    /// atomicity contract. F-77 (#904): the prior contract text ("when
    /// a backend overrides `transact`, partial state is never
    /// observable") was an overpromise that `MirroredStore` silently
    /// violated for its ephemeral subset; this flag makes the
    /// capability machine-checkable. F-85 (#913): an audit found ZERO
    /// production callers read this flag — every current `transact`
    /// caller tolerates the non-atomic case by construction (settle /
    /// re-scan, watermark-gated visibility + WAL replay, redelivery, or
    /// pre-written-chunk snapshot publish — see [`Self::transact`]'s
    /// caller list), so the flag serves as honest, queryable capability
    /// metadata rather than a gate any production path enforces.
    fn supports_atomic_transact(&self) -> bool {
        false
    }

    /// Whether this backend supports a repository-wide atomic batch
    /// ([`Repo::begin_atomic_batch`]) writing through the RAW backend by
    /// store name — such a batch bypasses any wrapper cache, so wrapper
    /// backends (`CachedStore`, `MemBufferStore`, `MirroredStore`) keep the
    /// default `false` = unsupported and persistent-follower activation can
    /// reject such stacks. Raw fjall / in-memory backends report `true`.
    fn supports_atomic_batch(&self) -> bool {
        false
    }

    /// Returns an async stream that yields batches of records.
    /// Like PHP generators but with batching - yields Vec of size batch_size.
    /// Batches are fetched as the stream is polled; prefetching is not guaranteed.
    ///
    /// # Ordering guarantee
    ///
    /// Keys within and across batches are yielded in ascending
    /// lexicographic byte order — the SAME guarantee documented on
    /// [`Store::scan_prefix_stream`], every implementor MUST uphold it.
    /// Callers rely on this for correctness, not just performance —
    /// `storage_membuffer.rs`'s merge-overlay scans (task #530) do a
    /// linear 2-way sorted merge of the dirty overlay against this
    /// stream and would silently resurrect a tombstoned or stale key if
    /// an implementor ever yielded out of order.
    ///
    /// # Arguments
    /// * `batch_size` - Number of records per batch
    ///
    /// # Returns
    /// Stream that yields DbResult<Vec<(RecordKey, Bytes)>> - each batch has exactly batch_size items
    /// (except possibly the last batch which may be smaller)
    /// `batch_size == 0` yields an empty stream.
    fn iter_stream(&self, batch_size: usize) -> RecordStream;

    /// Returns an async stream that yields batches of records with keys starting with the given prefix.
    ///
    /// Like `iter_stream()` but filtered by prefix. More efficient than loading all results into memory.
    /// A zero batch size yields an empty stream.
    ///
    /// # Ordering guarantee
    ///
    /// Keys within and across batches are yielded in ascending
    /// lexicographic byte order, and each batch resumes strictly past
    /// the previous batch's last key (`Bound::Excluded`) — every
    /// implementor MUST uphold this (see `storage_in_memory.rs`,
    /// `storage_fjall.rs`, `storage_cached.rs`, `storage_membuffer.rs`
    /// for the reference pattern). Callers rely on this for
    /// correctness, not just performance — e.g.
    /// `IndexManager::lookup_by_index`'s posting-list cache
    /// (`Arc<[RecordId]>`, audit 3.2 / task #499) depends on the scan
    /// already being sorted and duplicate-free instead of re-sorting
    /// via an intermediate `BTreeSet`.
    ///
    /// # Arguments
    /// * `prefix` - The prefix to search for
    /// * `batch_size` - Number of records per batch; zero yields an empty stream
    ///
    /// # Returns
    /// Stream that yields batches of matching records
    fn scan_prefix_stream(&self, prefix: Bytes, batch_size: usize) -> RecordStream;

    /// Returns an async stream of records whose keys fall in
    /// `[start_inclusive ..= end_inclusive]` (lexicographic byte
    /// order). `None` on either side = unbounded.
    ///
    /// The in-memory backend overrides this with a native range scan.
    /// The default implementation filters `iter_stream`, scanning until the
    /// upper bound is passed (or the source ends). Disk backends SHOULD
    /// override this with a native seek into the LSM tree — `FjallStore`
    /// does.
    ///
    /// # Arguments
    /// * `start_inclusive` — lower bound. `None` = scan from the start.
    /// * `end_inclusive` — upper bound. `None` = scan to the end.
    /// * `batch_size` — number of records per yielded batch; zero yields an empty stream.
    fn iter_range_stream(
        &self,
        start_inclusive: Option<Bytes>,
        end_inclusive: Option<Bytes>,
        batch_size: usize,
    ) -> RecordStream {
        if batch_size == 0 {
            return Box::pin(futures::stream::empty());
        }
        // Default impl: full scan + in-stream filter. Correct for
        // any backend that has `iter_stream`. Native impls below
        // make it O(log N + K) by seeking into the B-tree.
        let inner = self.iter_stream(batch_size);
        Box::pin(default_range_filter(inner, start_inclusive, end_inclusive))
    }

    /// Same as `iter_range_stream`, but walks the range in
    /// REVERSE byte order — high → low. Used by reverse-iter
    /// sorted-index reads: `lookup_last_k`, `lookup_max`, and
    /// `ORDER BY field DESC LIMIT K`.
    ///
    /// `batch_size == 0` yields an empty stream.
    /// Default impl: collect the forward-range stream and reverse
    /// it in memory. Correct for any backend, but O(N) memory.
    /// Fjall and in-memory backends override with native reverse scans.
    fn iter_range_stream_reverse(
        &self,
        start_inclusive: Option<Bytes>,
        end_inclusive: Option<Bytes>,
        batch_size: usize,
    ) -> RecordStream {
        if batch_size == 0 {
            return Box::pin(futures::stream::empty());
        }
        let inner = self.iter_range_stream(start_inclusive, end_inclusive, batch_size);
        Box::pin(default_reverse(inner, batch_size))
    }
}

/// Default reverse wrapper: drains the inner stream into memory,
/// reverses, and re-emits in `batch_size` chunks. Memory ≈ N items
/// — fine for in-memory backend (small datasets, no I/O latency
/// pressure); disk backends override with a native reverse cursor.
fn default_reverse(
    mut inner: RecordStream,
    batch_size: usize,
) -> impl Stream<Item = Result<Vec<(RecordKey, Bytes)>, DbError>> + Send {
    async_stream::stream! {
        let mut all: Vec<(RecordKey, Bytes)> = Vec::new();
        while let Some(batch) = inner.next().await {
            let batch = batch?;
            all.extend(batch);
        }
        all.reverse();
        while !all.is_empty() {
            let take = batch_size.min(all.len());
            // `drain(0..take)` after a reverse drains the rear of the
            // pre-reversed vec, but we want high→low order from the
            // reversed front. So drain prefix.
            let batch: Vec<_> = all.drain(..take).collect();
            yield Ok(batch);
        }
    }
}

/// Default range-filter wrapper around an existing iter stream.
///
/// Filters each yielded batch element-by-element against the
/// `[start..=end]` window and yields filtered batches as soon as we
/// have any (preserving the stream's batching semantics).
fn default_range_filter(
    mut inner: RecordStream,
    start: Option<Bytes>,
    end: Option<Bytes>,
) -> impl Stream<Item = Result<Vec<(RecordKey, Bytes)>, DbError>> + Send {
    async_stream::stream! {
        while let Some(batch) = inner.next().await {
            let batch = batch?;
            let mut filtered = Vec::new();
            let mut past_end = false;
            for (key, value) in batch {
                if end.as_ref().is_some_and(|e| key.as_ref() > e.as_ref()) {
                    past_end = true;
                    break;
                }
                if start.as_ref().is_none_or(|s| key.as_ref() >= s.as_ref()) {
                    filtered.push((key, value));
                }
            }
            if !filtered.is_empty() {
                yield Ok(filtered);
            }
            if past_end {
                break;
            }
        }
    }
}

/// Walk the `raw_backend` chain until we hit a backend that isn't
/// a wrapper. Useful when an unwrapper (MvccStore, GcWorker) needs
/// a write path that no buffer / cache layer intercepts.
///
/// Stops at the first `None` — usually one or two hops.
pub async fn fully_unwrap_store(store: &Arc<dyn Store>) -> Arc<dyn Store> {
    let mut cur = Arc::clone(store);
    while let Some(inner) = cur.raw_backend().await {
        cur = inner;
    }
    cur
}

/// A trait for a repository that can manage multiple `Store` instances.
#[async_trait]
pub trait Repo: Send + Sync {
    /// Retrieves a store by name. Creates it if it doesn't exist.
    async fn store_get<S>(&self, name: S) -> DbResult<Arc<dyn Store>>
    where
        S: AsRef<str> + Send;

    /// Deletes an entire store by name.
    async fn store_delete<S: AsRef<str> + Send>(&self, name: S) -> DbResult<bool>;

    /// Lists all stores in the repository.
    async fn stores_list(&self) -> DbResult<Vec<String>>;

    /// Open an existing store without creating one. The default fails closed;
    /// repositories that can prove existence without an open-or-create race
    /// should override it.
    async fn store_get_existing(&self, name: &str) -> DbResult<Option<Arc<dyn Store>>> {
        let _ = name;
        Err(DbError::Storage(
            "repository cannot safely distinguish lookup from creation".into(),
        ))
    }

    /// Copy every key/value from store `from` into store `to`.
    ///
    /// Used by `RENAME TABLE`: a table's physical data lives in three
    /// name-keyed stores (`__data__<t>`, `__info__<t>`, `__history__<t>`)
    /// and the storage layer has no native store-rename primitive, so
    /// rename is implemented as copy-then-orphan (the old store is left
    /// in place — same disposition as `DROP TABLE`, which intentionally
    /// orphans `__data__` since the catalogue is the source of truth).
    ///
    /// Default impl streams the source in batches of 256 and calls
    /// `set_many` on the destination. This is a merge/upsert: matching keys
    /// are overwritten and unrelated destination keys remain. It is not a
    /// snapshot and does not roll back on error or cancellation; completed
    /// batches, and possibly part of the current batch, may remain applied.
    /// Backends with a native copy/rename primitive may override.
    async fn copy_store(&self, from: &str, to: &str) -> DbResult<()> {
        const BATCH: usize = 256;
        let src = self.store_get(from).await?;
        let dst = self.store_get(to).await?;
        let mut stream = src.iter_stream(BATCH);
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            if batch.is_empty() {
                continue;
            }
            dst.set_many(batch).await?;
        }
        Ok(())
    }

    /// Force ONE backend-wide durability barrier for every store of this
    /// repository: after it returns Ok, all data drained by `flush_drain()`
    /// (or written directly) is as durable as a per-store `flush()` loop.
    ///
    /// Default: `Ok(())` — backends whose `Store::flush()` already persists
    /// per store (or that have nothing to persist, in-memory) need no second
    /// barrier. Backends with ONE database-wide journal (fjall) override and
    /// fsync once; their per-store `flush()` MUST then not be called in the
    /// same pass (the caller pairs `flush_drain` with this).
    async fn persist_once(&self) -> DbResult<()> {
        let _ = self;
        Ok(())
    }

    /// Whether this repository implements [`Self::begin_atomic_batch`] with
    /// real all-or-nothing semantics. Default: false.
    fn supports_atomic_batch(&self) -> bool {
        false
    }

    /// Begin a repository-wide atomic batch over named stores. Default:
    /// unsupported (typed `DbError::AtomicBatchUnsupported`). Persistent
    /// backends that cannot honour wrapper-cache coherence must report
    /// `supports_atomic_batch() == false` so callers can reject before
    /// allocating anything.
    async fn begin_atomic_batch(&self) -> DbResult<Box<dyn AtomicBatch>> {
        let _ = self;
        Err(DbError::AtomicBatchUnsupported(
            "backend does not implement repository-wide atomic batches".into(),
        ))
    }
}

/// The outcome of an [`AtomicBatch::commit`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AtomicCommitOutcome {
    /// The batch was applied and is durable per the backend's policy; on
    /// reopen the repository observes the FULL pre- or post-batch state.
    Committed,

    /// The commit path failed at a point where the batch MAY have been
    /// applied. The outcome is unknowable from the caller's side: the caller
    /// MUST reconcile by reading back — never blindly re-apply, never assume
    /// absence.
    Uncertain {
        /// Human-readable detail of where the commit path failed.
        detail: String,
    },
}

/// A pending repository-wide atomic batch over SEVERAL named stores of one
/// repository. Ops are staged in memory and applied in ONE atomic commit
/// (`commit`). Stores are addressed by NAME (the `Repo::store_get` key).
#[async_trait]
pub trait AtomicBatch: Send {
    /// Stage a set. Fails (before any state change) on an invalid store name.
    fn set(&mut self, store: &str, key: RecordKey, value: Bytes) -> DbResult<()>;

    /// Stage a delete.
    fn delete(&mut self, store: &str, key: RecordKey) -> DbResult<()>;

    /// Commit atomically. See `AtomicCommitOutcome` for the error/uncertain
    /// contract: a plain `Err` means NOTHING was applied; `Ok(Uncertain{..})`
    /// means the outcome is unknowable and the caller must reconcile.
    async fn commit(self: Box<Self>) -> DbResult<AtomicCommitOutcome>;
}

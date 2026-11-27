//! `FjallStore` — one keyspace behind the `Store` trait.
use super::persist::persist_sync_all;
use super::worker::{submit, WriteJob, WriteWorker};
use crate::error::{DbError, DbResult};
use crate::types::{KvOp, RecordKey, Store};
use async_stream::stream;
use async_trait::async_trait;
use bytes::Bytes;
use fjall::{Database, Keyspace};
use futures::stream::Stream;
use std::io;
use std::ops::Bound;
use std::pin::Pin;
use std::sync::Arc;
use tokio::task;

pub struct FjallStore {
    pub(super) keyspace: Keyspace,
    /// Kept alongside the keyspace so `Store::flush()` can call
    /// `Database::persist(PersistMode::SyncAll)` — fjall journals
    /// to a per-database WAL, not per-keyspace, so durability is
    /// the database's concern.
    pub(super) db: Arc<Database>,
    /// Dedicated write thread (task #536), spawned LAZILY on first
    /// `insert`/`transact` submission — not eagerly in `store_get`. Point-
    /// writes with NO embedded read — `insert`, `transact` — are submitted
    /// here and executed one at a time in channel admission order. Concurrent
    /// invocation order and ordering across handles or routes are not guaranteed.
    /// Await preceding operations when ordering matters. `set`/`remove`
    /// stay on `spawn_blocking` (their embedded `contains_key` read measured
    /// a net regression when routed through the worker); reads stay on
    /// `spawn_blocking` too. See the worker's module comment for the
    /// measured reasoning behind the split.
    ///
    /// Lazy spawn matters: `store_get` is called per-store, including for
    /// stores that never issue `insert`/`transact` (e.g. the engine's
    /// `__tx__` marker store, fetched fresh on every transaction commit via
    /// `RepoInstance::tx_info_store` — only `set`/`get`/`flush` traffic).
    /// Eagerly spawning a worker thread in `store_get` would spin up and
    /// then immediately tear down an OS thread on every commit for zero
    /// work — a real regression on the commit hot path an `@fl` review
    /// caught (the bench couldn't see it: it constructs each store once).
    /// `OnceLock` makes the spawn a one-time cost per store instance, paid
    /// only by callers that actually submit a job.
    pub(super) worker: std::sync::OnceLock<Result<WriteWorker, Arc<io::Error>>>,
    #[cfg(test)]
    pub(super) spawn_worker: Option<Box<WorkerSpawn>>,
}

#[cfg(test)]
type WorkerSpawn = dyn Fn(Keyspace, Arc<Database>) -> io::Result<WriteWorker> + Send + Sync;

#[derive(Debug)]
struct WorkerStartError(Arc<io::Error>);

impl std::fmt::Display for WorkerStartError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl std::error::Error for WorkerStartError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}

impl FjallStore {
    /// cfg(test): flush the active memtable into persisted segments so tests
    /// can exercise the persisted-table read path of `iter_range_stream`.
    /// Bounded fjall API call: `Keyspace::rotate_memtable_and_wait`
    /// (fjall 3.1.13 `src/keyspace/mod.rs:713`, `#[doc(hidden)]`) seals the
    /// memtable, enqueues the flush task, and waits until sealed memtables
    /// reach zero. Blocking — call from a blocking context only.
    #[cfg(test)]
    pub(crate) fn persist_memtables_for_test(&self) {
        self.keyspace
            .rotate_memtable_and_wait()
            .expect("memtable rotation + flush must succeed");
    }

    /// Lazily spawn (on first call) or return the already-spawned write
    /// worker for this store. See the `worker` field's doc comment for why
    /// this must be lazy rather than eager.
    pub(super) fn worker(&self) -> DbResult<&WriteWorker> {
        let result = self.worker.get_or_init(|| {
            #[cfg(test)]
            if let Some(spawn) = &self.spawn_worker {
                return spawn(self.keyspace.clone(), self.db.clone()).map_err(Arc::new);
            }
            WriteWorker::spawn(self.keyspace.clone(), self.db.clone()).map_err(Arc::new)
        });
        result.as_ref().map_err(|error| {
            DbError::Io(io::Error::new(
                error.kind(),
                WorkerStartError(Arc::clone(error)),
            ))
        })
    }
}

#[async_trait]
impl Store for FjallStore {
    async fn insert(&self, value: Bytes) -> DbResult<RecordKey> {
        // No collision probe is made; see exec_insert for the probabilistic
        // uniqueness and overwrite contract.
        submit(self.worker()?.sender(), |reply| WriteJob::Insert {
            value,
            reply,
        })
        .await
    }

    async fn set(&self, key: RecordKey, value: Bytes) -> DbResult<bool> {
        let keyspace = self.keyspace.clone();
        task::spawn_blocking(move || -> DbResult<bool> {
            // §1.2 (audit 2026-07-06-perf-radical-o-notation): the
            // `contains_key` here doubles the LSM point-lookup cost of every
            // `set`. The flag is needed by the `Store` trait contract
            // (`set` returns `bool` = "was created") and several callers
            // consume it (engine's `delete_returning_version`, CachedStore's
            // size tracking, the storage fjall tests). fjall 3.x
            // `Keyspace::insert` returns `Result<(), Error>` — no prior-value
            // — so `existed` CANNOT be derived from the write op itself; the
            // separate lookup is the only way to honor the contract here.
            //
            // The engine layer already does its own existence check via
            // `self.get(id).await.ok()` before calling through (see
            // `table_manager_crud.rs::delete_returning_version`), so the
            // storage-side flag is technically redundant for the engine — but
            // removing the trait's `bool` return would be a cross-workspace
            // API change, out of scope for this surgical perf fix. A flag-free
            // fast-path variant on `Store` is the proper follow-up.
            //
            // §B13 (acknowledged TOCTOU): the engine never issues two
            // concurrent `set` calls for the same `RecordKey` on a single
            // table (writes are serialised through `TableManager` dispatch),
            // so the `existed` flag stays consistent with the actual write
            // under normal use. Concurrent calls from outside the engine
            // (e.g. tooling) would race; documented here so the contract is
            // explicit.
            //
            // NOT routed through the write worker (task #536): benched under
            // 8×-cache-size cold data + 32-way fan-out
            // (`benches/storage_fjall_write_worker.rs`) at ~1.45× SLOWER
            // (1,124,693 -> 1,630,982 ns/op) — the embedded `contains_key`
            // read, previously overlapped across the `spawn_blocking` pool,
            // gets serialized onto one thread when routed through the
            // worker, reproducing the read-parallelism hazard
            // `docs/dev-artifacts/design/fjall-worker-loop-524-findings.md` warned about.
            // `insert`/`transact` have no such embedded read and DO route
            // through the worker (a measured ~1.7× win) — see the worker's
            // module comment.
            let existed = keyspace
                .contains_key(&key[..])
                .map_err(|e| DbError::Storage(e.to_string()))?;

            keyspace
                .insert(&key[..], &*value)
                .map_err(|e| DbError::Storage(e.to_string()))?;

            Ok(!existed)
        })
        .await
        .map_err(|e| DbError::Internal(e.to_string()))?
    }

    /// Fast-path `set` for callers that don't need the "was created" flag
    /// (task #613) — skips the `contains_key` lookup `set` embeds to
    /// derive that flag, halving the LSM point-lookup cost for this op.
    async fn set_no_flag(&self, key: RecordKey, value: Bytes) -> DbResult<()> {
        let keyspace = self.keyspace.clone();
        task::spawn_blocking(move || -> DbResult<()> {
            keyspace
                .insert(&key[..], &*value)
                .map_err(|e| DbError::Storage(e.to_string()))
        })
        .await
        .map_err(|e| DbError::Internal(e.to_string()))?
    }

    async fn get(&self, key: RecordKey) -> DbResult<Bytes> {
        let keyspace = self.keyspace.clone();
        task::spawn_blocking(move || -> DbResult<Bytes> {
            match keyspace
                .get(&key[..])
                .map_err(|e| DbError::Storage(e.to_string()))?
            {
                // §1.1 (audit 2026-07-06-perf-radical-o-notation): with the
                // fjall `bytes_1` feature on, `Slice` IS a `bytes::Bytes`
                // under the hood and `From<Slice> for Bytes` is a true
                // zero-copy move (just unwraps the inner `Bytes` — both are
                // refcounted byte buffers). The previous `copy_from_slice`
                // did a full memcpy + alloc per point-read.
                Some(slice) => Ok(Bytes::from(slice)),
                None => Err(DbError::missing_key(&key)),
            }
        })
        .await
        .map_err(|e| DbError::Internal(e.to_string()))?
    }

    /// Reverse range scan using fjall's `keyspace.range(...)` which
    /// implements `DoubleEndedIterator` — so `.rev()` walks the
    /// LSM tree backwards natively, no in-memory collect.
    /// Replaces the default `collect-forward + reverse` impl.
    fn iter_range_stream_reverse(
        &self,
        start_inclusive: Option<Bytes>,
        end_inclusive: Option<Bytes>,
        batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<Vec<(RecordKey, Bytes)>, DbError>> + Send>> {
        let keyspace = self.keyspace.clone();
        let start_bytes = start_inclusive.map(|b| b.to_vec());
        let end_bytes = end_inclusive.map(|b| b.to_vec());

        Box::pin(stream! {
            if batch_size == 0 { return; }
            // Cursor walks downward; upper bound shrinks each batch.
            let mut cursor: Option<Vec<u8>> = None;

            loop {
                let keyspace_clone = keyspace.clone();
                let cur = cursor.clone();
                let lower_init = start_bytes.clone();
                let upper_init = end_bytes.clone();

                let batch: DbResult<Vec<(RecordKey, Bytes)>> = task::spawn_blocking(move || {
                    let lower: Bound<Vec<u8>> = match &lower_init {
                        Some(s) => Bound::Included(s.clone()),
                        None => Bound::Unbounded,
                    };
                    let upper: Bound<Vec<u8>> = match (&cur, &upper_init) {
                        (Some(c), _) => Bound::Excluded(c.clone()),
                        (None, Some(e)) => Bound::Included(e.clone()),
                        (None, None) => Bound::Unbounded,
                    };

                    let mut items = Vec::new();
                    for guard in keyspace_clone.range((lower, upper)).rev().take(batch_size) {
                        let (key, val) = guard
                            .into_inner()
                            .map_err(|e| DbError::Storage(e.to_string()))?;
                        // §1.1: zero-copy conversion (see `get`).
                        items.push((RecordKey::from(Bytes::from(key)), Bytes::from(val)));
                    }
                    Ok(items)
                })
                .await
                .map_err(|e| DbError::Internal(e.to_string()))?;

                let batch = batch?;
                if batch.is_empty() {
                    break;
                }
                cursor = batch.last().map(|(k, _)| k.to_vec());
                yield Ok(batch);
            }
        })
    }

    /// Forward range scan using fjall's `keyspace.range(...)` — seeks
    /// directly to the lower bound in the LSM tree. Each BATCH call costs
    /// O(log N + K); the cursor re-opens `range` with a `Bound::Excluded`
    /// upper cursor per batch, so a whole K-key stream costs O(B·(log N + K))
    /// over B batches (K keys total). Replaces the default `iter_stream` +
    /// filter impl, which was O(total keyspace) per call. Mirrors
    /// `iter_range_stream_reverse`, walking upward: the cursor
    /// advances past each batch's last key via `Bound::Excluded`.
    fn iter_range_stream(
        &self,
        start_inclusive: Option<Bytes>,
        end_inclusive: Option<Bytes>,
        batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<Vec<(RecordKey, Bytes)>, DbError>> + Send>> {
        let keyspace = self.keyspace.clone();
        let start_bytes = start_inclusive.map(|b| b.to_vec());
        let end_bytes = end_inclusive.map(|b| b.to_vec());

        Box::pin(stream! {
            if batch_size == 0 { return; }
            // An inverted range is empty (as under the default filter impl);
            // never hand it to the LSM iterator.
            if let (Some(s), Some(e)) = (&start_bytes, &end_bytes) {
                if s > e { return; }
            }
            // Cursor walks upward; lower bound advances each batch.
            let mut cursor: Option<Vec<u8>> = None;

            loop {
                let keyspace_clone = keyspace.clone();
                let cur = cursor.clone();
                let lower_init = start_bytes.clone();
                let upper_init = end_bytes.clone();

                let batch: DbResult<Vec<(RecordKey, Bytes)>> = task::spawn_blocking(move || {
                    let lower: Bound<Vec<u8>> = match (&cur, &lower_init) {
                        (Some(c), _) => Bound::Excluded(c.clone()),
                        (None, Some(s)) => Bound::Included(s.clone()),
                        (None, None) => Bound::Unbounded,
                    };
                    let upper: Bound<Vec<u8>> = match &upper_init {
                        Some(e) => Bound::Included(e.clone()),
                        None => Bound::Unbounded,
                    };

                    let mut items = Vec::new();
                    for guard in keyspace_clone.range((lower, upper)).take(batch_size) {
                        let (key, val) = guard
                            .into_inner()
                            .map_err(|e| DbError::Storage(e.to_string()))?;
                        // §1.1: zero-copy conversion (see `get`).
                        items.push((RecordKey::from(Bytes::from(key)), Bytes::from(val)));
                    }
                    Ok(items)
                })
                .await
                .map_err(|e| DbError::Internal(e.to_string()))?;

                let batch = batch?;
                if batch.is_empty() {
                    break;
                }
                cursor = batch.last().map(|(k, _)| k.to_vec());
                yield Ok(batch);
            }
        })
    }

    /// Submit a mixed batch via fjall `OwnedWriteBatch`.
    ///
    /// Raw concurrent reads can observe a partially applied batch; see
    /// [`Store::supports_atomic_transact`].
    async fn transact(&self, ops: Vec<KvOp>) -> DbResult<()> {
        if ops.is_empty() {
            return Ok(());
        }
        // This worker orders jobs by channel admission, not against writes
        // through other routes or handles.
        submit(self.worker()?.sender(), |reply| WriteJob::Transact {
            ops,
            reply,
        })
        .await
    }

    /// `false` — RV-STORAGE-CC8D9EB5: pinned fjall 3.1.13 (Cargo.lock)
    /// `WriteBatch::commit` applies batch items to memtables individually
    /// and only then publishes the snapshot watermark, while this store's
    /// raw reads (`Keyspace::get`/`range`/`prefix`) read at `SeqNo::MAX`
    /// unclamped by the watermark — a reader can observe a
    /// partially-applied batch, so the advertised atomic-visibility
    /// guarantee is withdrawn until reads are publication-bounded.
    fn supports_atomic_transact(&self) -> bool {
        false
    }

    /// `true` — raw fjall backend: the repo-wide atomic batch writes
    /// through `Database::batch()` directly (see `exec_atomic_commit`).
    fn supports_atomic_batch(&self) -> bool {
        true
    }

    /// Force the WAL to fsync-on-disk. fjall buffers individual
    /// writes in the journal; `persist(SyncAll)` fsyncs the journal
    /// + writes any pending metadata. Reachable through
    /// `Arc<dyn Store>` — without this override callers hitting
    /// the default no-op would silently get "eventually durable"
    /// even after an explicit `flush()`.
    async fn flush(&self) -> DbResult<()> {
        let db = self.db.clone();
        task::spawn_blocking(move || -> DbResult<()> {
            persist_sync_all(&db).map_err(|e| DbError::Storage(format!("Fjall persist: {}", e)))?;
            Ok(())
        })
        .await
        .map_err(|e| DbError::Internal(e.to_string()))?
    }

    /// fjall journals every write into the per-Database journal at write
    /// time; there is NO per-store buffered state to drain, and durability
    /// is a DATABASE-level act (`persist`). A drain is therefore a no-op —
    /// the fsync happens once via `FjallRepo::persist_once`.
    async fn flush_drain(&self) -> DbResult<()> {
        Ok(())
    }

    async fn get_many(&self, keys: Vec<RecordKey>) -> DbResult<Vec<Option<Bytes>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let keyspace = self.keyspace.clone();
        task::spawn_blocking(move || -> DbResult<Vec<Option<Bytes>>> {
            let mut out = Vec::with_capacity(keys.len());
            for k in keys {
                match keyspace
                    .get(&k[..])
                    .map_err(|e| DbError::Storage(e.to_string()))?
                {
                    // §1.1: zero-copy conversion (see `get`).
                    Some(slice) => out.push(Some(Bytes::from(slice))),
                    None => out.push(None),
                }
            }
            Ok(out)
        })
        .await
        .map_err(|e| DbError::Internal(e.to_string()))?
    }

    async fn remove(&self, key: RecordKey) -> DbResult<bool> {
        let keyspace = self.keyspace.clone();
        task::spawn_blocking(move || -> DbResult<bool> {
            // §1.2 (audit 2026-07-06-perf-radical-o-notation): same shape as
            // `set` — the `contains_key` doubles the LSM point-lookup cost.
            // The flag is required by the `Store` trait contract (`remove`
            // returns `bool` = "existed and was removed") and consumed by
            // callers (engine's `delete_returning_version`, the storage
            // fjall tests). fjall 3.x `Keyspace::remove` returns
            // `Result<(), Error>` — no prior-value — so `existed` cannot be
            // derived from the tombstone write itself. See `set`'s comment
            // for the full rationale on why the trait-surface fast-path
            // variant is left as a follow-up, and why this is NOT routed
            // through the write worker (task #536) — the embedded
            // `contains_key` read measured a net regression there, same as
            // `set`.
            let existed = keyspace
                .contains_key(&key[..])
                .map_err(|e| DbError::Storage(e.to_string()))?;

            if existed {
                keyspace
                    .remove(&key[..])
                    .map_err(|e| DbError::Storage(e.to_string()))?;
            }

            Ok(existed)
        })
        .await
        .map_err(|e| DbError::Internal(e.to_string()))?
    }

    /// Fast-path `remove` for callers that don't need the "existed" flag
    /// (task #613) — skips the `contains_key` lookup `remove` embeds to
    /// derive that flag, halving the LSM point-lookup cost for this op.
    async fn remove_no_flag(&self, key: RecordKey) -> DbResult<()> {
        let keyspace = self.keyspace.clone();
        task::spawn_blocking(move || -> DbResult<()> {
            keyspace
                .remove(&key[..])
                .map_err(|e| DbError::Storage(e.to_string()))
        })
        .await
        .map_err(|e| DbError::Internal(e.to_string()))?
    }

    fn iter_stream(
        &self,
        batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<Vec<(RecordKey, Bytes)>, DbError>> + Send>> {
        let keyspace = self.keyspace.clone();

        Box::pin(stream! {
            if batch_size == 0 { return; }
            let mut last_key: Option<Vec<u8>> = None;

            loop {
                let keyspace_clone = keyspace.clone();
                let start_key = last_key;

                let batch: DbResult<(Vec<_>, Option<Vec<u8>>)> = task::spawn_blocking(move || {
                    let lower: Bound<Vec<u8>> = match &start_key {
                        Some(c) => Bound::Excluded(c.clone()),
                        None => Bound::Unbounded,
                    };
                    let upper: Bound<Vec<u8>> = Bound::Unbounded;

                    let mut items = Vec::with_capacity(256);
                    let mut last_batch_key: Option<Vec<u8>> = None;

                    for guard in keyspace_clone.range((lower, upper)).take(batch_size) {
                        let (key, value_slice) = guard
                            .into_inner()
                            .map_err(|e| DbError::Storage(e.to_string()))?;

                        last_batch_key = Some(key.to_vec());
                        // §1.1: zero-copy conversion for both key and value
                        // (see `get`).
                        items.push((RecordKey::from(Bytes::from(key)), Bytes::from(value_slice)));
                    }

                    Ok((items, last_batch_key))
                })
                .await
                .map_err(|e| DbError::Internal(e.to_string()))?;

                let (batch, next_key) = batch?;

                if batch.is_empty() {
                    break;
                }

                last_key = next_key;
                yield Ok(batch);
            }
        })
    }

    /// Prefix scan via `keyspace.range` — O(log N + M) per batch.
    ///
    /// Replaces the old O(N) full-iter + linear cursor re-seek that scanned
    /// every key on every batch call. Fjall's `range` seeks directly to the
    /// first matching key using the LSM-tree index, then `take_while` stops
    /// at the first key that no longer starts with the prefix. Subsequent
    /// batches use `Bound::Excluded(last_key)` to resume at exactly the right
    /// position — same pattern as `iter_stream`.
    fn scan_prefix_stream(
        &self,
        prefix: Bytes,
        batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<Vec<(RecordKey, Bytes)>, DbError>> + Send>> {
        let keyspace = self.keyspace.clone();

        Box::pin(stream! {
            if batch_size == 0 { return; }
            let mut last_key: Option<Vec<u8>> = None;
            let prefix_vec = prefix.to_vec();

            loop {
                let keyspace_clone = keyspace.clone();
                let cur_last = last_key;
                let pfx = prefix_vec.clone();

                let batch: DbResult<(Vec<_>, Option<Vec<u8>>)> = task::spawn_blocking(move || {

                    // Seek directly to the prefix boundary (or just past the cursor).
                    let lower: Bound<Vec<u8>> = match cur_last {
                        Some(ref c) => Bound::Excluded(c.clone()),
                        None => Bound::Included(pfx.clone()),
                    };
                    let upper: Bound<Vec<u8>> = Bound::Unbounded;

                    let mut items = Vec::with_capacity(256);
                    let mut last_batch_key: Option<Vec<u8>> = None;

                    // Range-seek + prefix boundary: take up to batch_size entries
                    // that still start with `pfx`. The `take(batch_size)` bounds
                    // the per-batch cost; the explicit prefix check terminates the
                    // scan once we've passed the prefix range (fjall yields lex order).
                    'batch: for guard in keyspace_clone.range((lower, upper)).take(batch_size) {
                        let (key, value_slice) = guard
                            .into_inner()
                            .map_err(|e| DbError::Storage(e.to_string()))?;

                        if !key.starts_with(&pfx) {
                            // We've exited the prefix range — stop this batch.
                            // The outer loop will also break because next batch
                            // would start at last_batch_key (already past prefix).
                            // We use a sentinel: push nothing, end loop.
                            break 'batch;
                        }

                        last_batch_key = Some(key.to_vec());
                        // §1.1: zero-copy conversion for both key and value
                        // (see `get`).
                        items.push((RecordKey::from(Bytes::from(key)), Bytes::from(value_slice)));
                    }

                    Ok((items, last_batch_key))
                })
                .await
                .map_err(|e| DbError::Internal(e.to_string()))?;

                let (batch, next_key) = batch?;

                if batch.is_empty() {
                    break;
                }

                last_key = next_key;
                yield Ok(batch);
            }
        })
    }
}

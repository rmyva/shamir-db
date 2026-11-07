//! Brute-force exact KNN adapter.
//!
//! O(n) per query — correct baseline. Lock-free read / single-writer
//! write: writes go through a bounded mpsc, a spawned task drains the
//! channel and applies ops to its owned working state, then publishes
//! a fresh `Arc<BruteSnap>` via `ArcSwap`. Readers grab the current
//! snapshot with one atomic load — no locks.
//!
//! Snapshots share immutable bounded chunks; the writer owns the lookup index
//! and copies only chunks modified since the previous publication.
//!
//! Search: top-k is computed with a bounded `BinaryHeap` of size k
//! (O(N log k)) instead of a full sort over all N entries (O(N log N)).

use crate::adapter::{SearchOpts, VectorAdapter, VectorError};
use crate::simd::{dot_product, l2_squared};
use arc_swap::ArcSwap;
use async_trait::async_trait;
use shamir_collections::TFxMap;
use shamir_index_core::kind::VectorMetric;
use shamir_types::types::record_id::RecordId;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// Maximum allowed top-k value. Mirrors `HnswAdapter::MAX_TOPK`.
const MAX_TOPK: u32 = 10_000;

const CHUNK_LEN: usize = 128;

#[derive(Clone)]
struct Row {
    rid: RecordId,
    vector: Vec<f32>,
    norm: f32,
}

#[derive(Clone, Default)]
struct BruteSnap {
    chunks: Vec<Arc<Vec<Row>>>,
    len: usize,
}

#[derive(Default)]
struct BruteWork {
    snap: BruteSnap,
    index: TFxMap<RecordId, usize>,
}

#[cfg(test)]
#[path = "../tests/quant/brute_force_internal_tests.rs"]
mod internal_tests;
#[cfg(test)]
#[path = "../tests/backend/brute_force_internal_tests.rs"]
mod shutdown_tests;

enum ShutdownFailure {
    Poisoned,
    Join(tokio::task::JoinError),
}

enum WriteOp {
    Upsert(RecordId, Vec<f32>),
    Delete(RecordId),
}

pub struct BruteForceAdapter {
    dim: u32,
    metric: VectorMetric,
    write_tx: mpsc::Sender<WriteOp>,
    snapshot: Arc<ArcSwap<BruteSnap>>,
    // Teardown-only slot: acquired once by consuming shutdown, never across await;
    // no search/write path acquires it, so contention is not expected.
    join: std::sync::Mutex<Option<JoinHandle<()>>>,
    #[cfg(test)]
    worker_hook: Option<Arc<dyn Fn() + Send + Sync>>,
}

/// Default bounded-channel capacity (mirrors `IndexActor`'s default).
/// Bounded so a runaway producer can't OOM the server; large enough
/// to absorb bulk-import bursts and let the writer task coalesce
/// many ops into a single publish.
const DEFAULT_CHANNEL_CAPACITY: usize = 1024;

impl BruteForceAdapter {
    pub fn new(dim: u32, metric: VectorMetric) -> Self {
        let snapshot = Arc::new(ArcSwap::from(Arc::new(BruteSnap::default())));
        let (write_tx, mut rx) = mpsc::channel::<WriteOp>(DEFAULT_CHANNEL_CAPACITY);
        let snap_for_task = snapshot.clone();

        let join = tokio::spawn(async move {
            // Working state owned by this task — never shared, so we
            // mutate in place and publish a fresh `Arc<BruteSnap>`
            // once per drained batch (NOT once per op).
            let mut work = BruteWork::default();
            while let Some(first) = rx.recv().await {
                apply_op(&mut work, first);
                // Drain the queued burst before publishing shared chunks.
                while let Ok(op) = rx.try_recv() {
                    apply_op(&mut work, op);
                }
                snap_for_task.store(Arc::new(work.snap.clone()));
            }
        });

        Self {
            dim,
            metric,
            write_tx,
            snapshot,
            join: std::sync::Mutex::new(Some(join)),
            #[cfg(test)]
            worker_hook: None,
        }
    }

    /// Distance with the stored vector's norm provided by the caller
    /// (precomputed at insert time). Only the query-side norm is
    /// computed per search, and only for `Cosine`.
    #[inline]
    fn distance_with_stored_norm(
        metric: VectorMetric,
        query: &[f32],
        query_norm: f32,
        stored: &[f32],
        stored_norm: f32,
    ) -> f32 {
        match metric {
            VectorMetric::L2 => l2_squared(query, stored).sqrt(),
            VectorMetric::Cosine => {
                if query_norm < 1e-9 || stored_norm < 1e-9 {
                    return 1.0;
                }
                let dot = dot_product(query, stored);
                1.0 - dot / (query_norm * stored_norm)
            }
            VectorMetric::Dot => -dot_product(query, stored),
        }
    }

    pub async fn shutdown(self) {
        self.shutdown_with_report(|event| match event {
            ShutdownFailure::Poisoned => log::error!("brute-force shutdown: join lock poisoned"),
            ShutdownFailure::Join(error) => log::error!("brute-force writer task failed: {error}"),
        })
        .await;
    }

    async fn shutdown_with_report(self, mut report: impl FnMut(ShutdownFailure)) {
        drop(self.write_tx);
        let (join, poisoned) = match self.join.lock() {
            Ok(mut slot) => (slot.take(), false),
            Err(err) => (err.into_inner().take(), true),
        };
        if poisoned {
            report(ShutdownFailure::Poisoned);
        }
        if let Some(j) = join {
            if let Err(error) = j.await {
                report(ShutdownFailure::Join(error));
            }
        }
    }
}

#[inline]
fn norm_of(v: &[f32]) -> f32 {
    dot_product(v, v).sqrt()
}

fn apply_op(work: &mut BruteWork, op: WriteOp) {
    match op {
        WriteOp::Upsert(rid, vector) => {
            let row = Row {
                rid,
                norm: norm_of(&vector),
                vector,
            };
            if let Some(&pos) = work.index.get(&rid) {
                Arc::make_mut(&mut work.snap.chunks[pos / CHUNK_LEN])[pos % CHUNK_LEN] = row;
            } else {
                let pos = work.snap.len;
                if pos.is_multiple_of(CHUNK_LEN) {
                    work.snap
                        .chunks
                        .push(Arc::new(Vec::with_capacity(CHUNK_LEN)));
                }
                Arc::make_mut(&mut work.snap.chunks[pos / CHUNK_LEN]).push(row);
                work.snap.len += 1;
                work.index.insert(rid, pos);
            }
        }
        WriteOp::Delete(rid) => {
            if let Some(pos) = work.index.remove(&rid) {
                let last = work.snap.len - 1;
                let last_chunk = last / CHUNK_LEN;
                let moved = Arc::make_mut(&mut work.snap.chunks[last_chunk])
                    .pop()
                    .expect("nonempty final brute-force chunk");
                if pos != last {
                    work.index.insert(moved.rid, pos);
                    Arc::make_mut(&mut work.snap.chunks[pos / CHUNK_LEN])[pos % CHUNK_LEN] = moved;
                }
                work.snap.len -= 1;
                if work.snap.chunks[last_chunk].is_empty() {
                    work.snap.chunks.pop();
                }
            }
        }
    }
}

/// Bounded-heap top-K helper. We keep the K WORST so far at the top
/// (`Reverse`-style max-heap by distance); a new candidate with
/// smaller distance pops the worst and pushes itself, so the heap
/// always holds the current K best. Final drain + sort is O(K log K).
fn push_topk(heap: &mut BinaryHeap<HeapEntry>, k: usize, rid: RecordId, dist: f32) {
    if k == 0 {
        return;
    }
    if heap.len() < k {
        heap.push(HeapEntry { rid, dist });
    } else if let Some(top) = heap.peek() {
        // BinaryHeap is a max-heap on `Ord`; HeapEntry orders by dist
        // ascending semantics inverted below (so peek = worst). Swap
        // if the candidate beats the current worst.
        if dist < top.dist {
            heap.pop();
            heap.push(HeapEntry { rid, dist });
        }
    }
}

/// Max-heap entry: larger `dist` is "greater" so `peek()` returns the
/// current worst candidate in the top-K window.
#[derive(Debug)]
struct HeapEntry {
    rid: RecordId,
    dist: f32,
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.dist == other.dist
    }
}
impl Eq for HeapEntry {}
impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Treat NaN as Equal (matches the old `partial_cmp(...).unwrap_or(Equal)`).
        self.dist
            .partial_cmp(&other.dist)
            .unwrap_or(Ordering::Equal)
    }
}

#[async_trait]
impl VectorAdapter for BruteForceAdapter {
    async fn upsert(&self, rid: RecordId, vec: &[f32]) -> Result<(), VectorError> {
        if vec.len() as u32 != self.dim {
            return Err(VectorError::DimMismatch {
                expected: self.dim,
                got: vec.len() as u32,
            });
        }
        self.write_tx
            .send(WriteOp::Upsert(rid, vec.to_vec()))
            .await
            .map_err(|_| VectorError::Internal("actor stopped".into()))?;
        // Yield to let the actor make progress — not guaranteed but
        // helps single-threaded tests observe the write soon. Kept
        // for behavioural parity with the previous impl.
        tokio::task::yield_now().await;
        Ok(())
    }

    async fn delete(&self, rid: RecordId) -> Result<(), VectorError> {
        self.write_tx
            .send(WriteOp::Delete(rid))
            .await
            .map_err(|_| VectorError::Internal("actor stopped".into()))?;
        tokio::task::yield_now().await;
        Ok(())
    }

    async fn search(
        &self,
        query: &[f32],
        k: u32,
        _opts: SearchOpts,
        staged: Option<&[(RecordId, Vec<f32>)]>,
    ) -> Result<Vec<(RecordId, f32)>, VectorError> {
        // BruteForce is EXACT KNN — there is no `ef` width knob (the
        // traversal visits every vector). `opts.ef_search` is accepted for
        // API uniformity but is a no-op (ef_search is inherently
        // approximate-search-only). `opts.oversample` is consumed at the
        // ENGINE level (P3 / V3.1): the engine requests `k′ = k × oversample`
        // candidates, so BruteForce sees a widened `k` and returns that many.
        if query.len() as u32 != self.dim {
            return Err(VectorError::DimMismatch {
                expected: self.dim,
                got: query.len() as u32,
            });
        }
        let k = if k == 0 {
            return Ok(vec![]);
        } else {
            k.min(MAX_TOPK)
        };
        let snap = self.snapshot.load_full();
        let metric = self.metric;
        let dim = self.dim;
        let query = query.to_vec();
        let staged = staged.map(<[(RecordId, Vec<f32>)]>::to_vec);
        #[cfg(test)]
        let worker_hook = self.worker_hook.clone();
        tokio::task::spawn_blocking(move || {
            #[cfg(test)]
            if let Some(hook) = worker_hook {
                hook();
            }
            exact_search(&snap, metric, dim, &query, k as usize, staged.as_deref())
        })
        .await
        .map_err(|error| VectorError::Internal(format!("brute-force search worker: {error}")))?
    }

    fn dim(&self) -> u32 {
        self.dim
    }

    fn len(&self) -> usize {
        self.snapshot.load().len
    }
}

fn exact_search(
    snap: &BruteSnap,
    metric: VectorMetric,
    dim: u32,
    query: &[f32],
    k_usize: usize,
    staged: Option<&[(RecordId, Vec<f32>)]>,
) -> Result<Vec<(RecordId, f32)>, VectorError> {
    let query_norm = match metric {
        VectorMetric::Cosine => norm_of(query),
        _ => 0.0,
    };
    let mut heap: BinaryHeap<HeapEntry> = BinaryHeap::with_capacity(k_usize.saturating_add(1));
    for row in snap.chunks.iter().flat_map(|chunk| chunk.iter()) {
        let d = BruteForceAdapter::distance_with_stored_norm(
            metric,
            query,
            query_norm,
            &row.vector,
            row.norm,
        );
        push_topk(&mut heap, k_usize, row.rid, d);
    }
    // Preserve the existing independent staged-candidate semantics.
    if let Some(staged) = staged {
        for (rid, vec) in staged {
            if vec.len() != dim as usize {
                return Err(VectorError::DimMismatch {
                    expected: dim,
                    got: u32::try_from(vec.len()).unwrap_or(u32::MAX),
                });
            }
            let stored_norm = match metric {
                VectorMetric::Cosine => norm_of(vec),
                _ => 0.0,
            };
            let d = BruteForceAdapter::distance_with_stored_norm(
                metric,
                query,
                query_norm,
                vec,
                stored_norm,
            );
            push_topk(&mut heap, k_usize, *rid, d);
        }
    }
    let mut out: Vec<(RecordId, f32)> = heap.into_iter().map(|e| (e.rid, e.dist)).collect();
    out.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));
    Ok(out)
}

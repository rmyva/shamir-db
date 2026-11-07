//! P0 stage L3b — promote-order turnstile, backend-level regression tests.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use shamir_storage::core::error::DbResult;
use shamir_storage::core::types::{RecordKey, Store};
use shamir_storage::storage_in_memory::InMemoryStore;
use shamir_types::core::interner::Interner;
use tokio::sync::oneshot;

use shamir_index_core::backend::IndexBackend;

use super::prune::delta_prune_tests::{lcg_vec, make_backend, rid, seed_base_and_open, top1, DIM};

/// Deadlines are deadlock guards, never scheduling or performance oracles.
async fn within<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(30), future)
        .await
        .expect("promote-order test stalled — a turn was leaked")
}

/// (e) a dropped/cancelled ticket releases the turn: a ticket that never
/// promotes (its commit future was cancelled before Phase 5d) must not wedge
/// the turnstile — a later ticket still acquires the turn and promotes.
///
/// Revert analysis: revert the Drop-based release (the ticket `Drop` no
/// longer inserts into `turn_finished` / advances `promote_turn`) and the
/// cancelled chunk's index is never skipped — `within(t2.wait_turn())`
/// times out and the test fails (on pre-L3b code no ticket exists at all,
/// so the leak cannot even be expressed).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rv_index_l3b_promote_order_cancelled_ticket_releases_turn() {
    let i = Interner::new();
    let info_store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let id = 40;
    let backend = Arc::new(make_backend(&i, id));
    seed_base_and_open(&backend, &info_store, id).await;

    // Chunk idx0 gets a ticket whose commit future is "cancelled": drop it
    // without ever promoting.
    let x = (rid(100), lcg_vec(DIM as usize, 100));
    let idx0 = backend
        .append_vector_delta(&info_store, std::slice::from_ref(&x), &[])
        .await
        .unwrap()
        .expect("a non-empty promote must reserve a chunk");
    let erased: Arc<dyn IndexBackend> = backend.clone();
    let cancelled = shamir_index_core::promote_ticket(Arc::clone(&erased), idx0);
    drop(cancelled);

    // A later transaction appends the next chunk and promotes: it must NOT
    // wait behind the cancelled ticket's index.
    let idx1 = backend
        .append_vector_delta(&info_store, std::slice::from_ref(&x), &[])
        .await
        .unwrap()
        .expect("a non-empty promote must reserve a chunk");
    assert_eq!(idx1, idx0 + 1, "chunk indices are contiguous");
    let ticket = shamir_index_core::promote_ticket(erased, idx1);
    within(ticket.wait_turn()).await;
    ticket
        .promote(std::slice::from_ref(&x), &[])
        .await
        .expect("the promoted batch must apply cleanly inside the turn");
    drop(ticket);
    backend.mark_delta_applied(idx1);

    // The promotion actually reached the live graph.
    assert_eq!(top1(&backend, 100).await, rid(100));
}

/// Test Store shim (RV finding 1): delegates everything to an inner
/// `InMemoryStore` except `set`, which first signals `reached_tx` (so the
/// test KNOWS the append is parked inside the store write) and then parks on
/// `release` until the test lets it through. No sleeps anywhere.
struct PendingSetStore {
    inner: InMemoryStore,
    reached_tx: Mutex<Option<oneshot::Sender<()>>>,
    release: Arc<tokio::sync::Notify>,
}

impl PendingSetStore {
    fn new() -> Self {
        Self {
            inner: InMemoryStore::new(),
            reached_tx: Mutex::new(None),
            release: Arc::new(tokio::sync::Notify::new()),
        }
    }
}

#[async_trait::async_trait]
impl Store for PendingSetStore {
    async fn insert(&self, value: bytes::Bytes) -> DbResult<RecordKey> {
        self.inner.insert(value).await
    }
    async fn set(&self, key: RecordKey, value: bytes::Bytes) -> DbResult<bool> {
        // Park ONLY when armed (test traffic): taking a sender means the test
        // wants this call held. Unarmed seed/delta traffic passes straight through.
        let armed = self.reached_tx.lock().unwrap().take();
        if let Some(tx) = armed {
            let _ = tx.send(());
            self.release.notified().await;
        }
        self.inner.set(key, value).await
    }
    async fn get(&self, key: RecordKey) -> DbResult<bytes::Bytes> {
        self.inner.get(key).await
    }
    async fn remove(&self, key: RecordKey) -> DbResult<bool> {
        self.inner.remove(key).await
    }
    fn iter_stream(
        &self,
        batch_cap: usize,
    ) -> std::pin::Pin<
        Box<dyn futures::Stream<Item = DbResult<Vec<(RecordKey, bytes::Bytes)>>> + Send>,
    > {
        self.inner.iter_stream(batch_cap)
    }
    fn scan_prefix_stream(
        &self,
        prefix: bytes::Bytes,
        batch_cap: usize,
    ) -> std::pin::Pin<
        Box<dyn futures::Stream<Item = DbResult<Vec<(RecordKey, bytes::Bytes)>>> + Send>,
    > {
        self.inner.scan_prefix_stream(prefix, batch_cap)
    }
}

/// (f) a delta-append future CANCELLED mid-store-write releases the turn
/// (RV finding 1): the reservation guard must not let a dropped append
/// wedge the turnstile — the reserved index is skipped (never marked
/// applied) and a later chunk acquires the turn and promotes.
///
/// Chunk indices are NOT hardcoded (round-2 finding 4): `start =
/// applied_prefix()` right after seeding IS the next reserved index. The
/// cancelled append reserved `start` (released-but-never-acked); the
/// surviving append reserves `start + 1`.
///
/// Revert analysis: delete the `DeltaReservationGuard` (the append-local
/// Drop guard in `vector_backend/maintenance.rs`, disarmed in
/// `append_vector_delta`). The cancelled append's index is then never
/// released into `turn_finished`, so the next chunk's `wait_turn` never
/// observes its turn and `within` times out — the test fails.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rv_index_l3b_promote_order_cancelled_append_releases_turn() {
    let i = Interner::new();
    let shim = Arc::new(PendingSetStore::new());
    let info_store: Arc<dyn Store> = shim.clone();
    let id = 41;
    let backend = Arc::new(make_backend(&i, id));
    within(seed_base_and_open(&backend, &info_store, id)).await;
    // The contiguous applied prefix after seeding == next_delta_idx == the
    // index the FIRST append will reserve (never hardcoded — finding 4).
    let start = backend.applied_prefix();

    let x = (rid(200), lcg_vec(DIM as usize, 200));
    // Arm the shim: the append will park INSIDE `Store::set`, deterministically.
    let (reached_tx, reached_rx) = oneshot::channel();
    *shim.reached_tx.lock().unwrap() = Some(reached_tx);
    let erased: Arc<dyn IndexBackend> = backend.clone();
    let task = tokio::spawn({
        let erased = Arc::clone(&erased);
        let info_store = Arc::clone(&info_store);
        let x = x.clone();
        async move {
            IndexBackend::append_vector_delta(
                erased.as_ref(),
                &info_store,
                std::slice::from_ref(&x),
                &[],
            )
            .await
        }
    });
    // Deterministic park: the append future is now suspended in `set`.
    let _ = within(reached_rx).await;
    // Cancel the append: `JoinHandle::drop` only detaches, so `abort()` is
    // required to actually drop the future at its suspension point inside
    // `set`, running the append-local Drop guard (never marked applied).
    task.abort();

    // A later transaction appends the next chunk with a normal store flow
    // (shim disarmed: `reached_tx` was consumed) and must NOT wait forever.
    let idx1 = within(IndexBackend::append_vector_delta(
        backend.as_ref(),
        &info_store,
        std::slice::from_ref(&x),
        &[],
    ))
    .await
    .unwrap()
    .expect("a non-empty promote must reserve a chunk");
    assert_eq!(
        idx1,
        start + 1,
        "the cancelled append reserved `start` (released, never acked); the \
         surviving append must take the NEXT index"
    );
    let ticket = shamir_index_core::promote_ticket(erased, idx1);
    within(ticket.wait_turn()).await;
    ticket
        .promote(std::slice::from_ref(&x), &[])
        .await
        .expect("the promoted batch must apply cleanly inside the turn");
    ticket.release_turn();
    // The surviving chunk is acked, but the CANCELLED chunk (`start`) was
    // released WITHOUT ever being acknowledged — it stays replayable and
    // blocks the contiguous applied prefix at `start` (not start+2).
    backend.mark_delta_applied(idx1);
    assert_eq!(
        backend.applied_prefix(),
        start,
        "the cancelled append's chunk must stay UNACKNOWLEDGED — released but \
         never acked, so the contiguous prefix cannot pass it"
    );

    // The promotion actually reached the live graph.
    assert_eq!(top1(&backend, 200).await, rid(200));
}

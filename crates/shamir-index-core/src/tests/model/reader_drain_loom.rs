use loom::sync::atomic::{fence, AtomicBool, AtomicUsize, Ordering};
use loom::sync::Arc;
use loom::thread;

/// Minimal model of the two-atomics that matter: the `in_flight` drain counter
/// and the intent `dropping` flag. `reader_read` models the reader's read of
/// the posting store landing — it is the thing the drain exists to fence
/// against the physical sweep.
///
/// `read_at_drop_return` is a snapshot of `reader_read` taken at the exact
/// instant the drainer's spin loop observes `in_flight == 0` and returns —
/// i.e. the moment `wait_for_drain()` returns in the real code. Asserting
/// against THIS field (rather than against `reader_read` after the caller
/// later joins the reader thread) is what makes the invariant meaningful:
/// `join()` always blocks until the reader thread has fully finished and
/// stored `reader_read`, so a post-`join` read of `reader_read` is true by
/// construction and proves nothing about the drain protocol. Sampling at
/// drain-return time is the only point at which the contract can actually
/// be violated.
struct Model {
    in_flight: AtomicUsize,
    dropping: AtomicBool,
    reader_read: AtomicBool,
    /// Snapshot of `reader_read` captured inside `run_drop` at the
    /// instant its spin loop exits (i.e. when `wait_for_drain()` returns).
    read_at_drop_return: AtomicBool,
}

impl Model {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            in_flight: AtomicUsize::new(0),
            dropping: AtomicBool::new(false),
            reader_read: AtomicBool::new(false),
            read_at_drop_return: AtomicBool::new(false),
        })
    }
}

/// Reader thread: `enter` (SeqCst fetch_add) → read `dropping` (SeqCst) →
/// if `false`, model the read → drop the guard (SeqCst fetch_sub). Returns
/// whether it took the fast path.
fn run_reader(m: Arc<Model>) -> bool {
    m.in_flight.fetch_add(1, Ordering::SeqCst);
    // Modeling-only fence: compensates for loom 0.7's no-op SeqCst-access
    // enforcement — see this module's top-level doc. No counterpart in the
    // real code.
    fence(Ordering::SeqCst);
    let fast = !m.dropping.load(Ordering::SeqCst);
    if fast {
        m.reader_read.store(true, Ordering::SeqCst);
    }
    m.in_flight.fetch_sub(1, Ordering::SeqCst);
    fast
}

/// Drop thread: raise `dropping` (SeqCst store) → drain (spin on `in_flight`
/// until 0, SeqCst) → take the post-drain snapshot.
///
/// The snapshot MUST be taken HERE — at the instant the spin loop exits and
/// `wait_for_drain()` returns — not by the caller after `join()`ing the reader.
/// A post-`join` read of `reader_read` is trivially true whenever the reader
/// took the fast path (the thread has fully returned), so it cannot witness
/// the interleaving hole it is meant to detect.
fn run_drop(m: &Model) {
    m.dropping.store(true, Ordering::SeqCst);
    // Modeling-only fence: compensates for loom 0.7's no-op SeqCst-access
    // enforcement — see this module's top-level doc. No counterpart in the
    // real code.
    fence(Ordering::SeqCst);
    while m.in_flight.load(Ordering::SeqCst) != 0 {
        thread::yield_now();
    }
    // wait_for_drain() has returned — every in-flight reader is out of the
    // drain set. Snapshot reader_read RIGHT HERE, at the instant of return,
    // so the assertion samples the state the drainer actually observed at
    // return time rather than the strictly-later post-join state.
    m.read_at_drop_return
        .store(m.reader_read.load(Ordering::SeqCst), Ordering::SeqCst);
}

#[test]
fn drop_wait_returns_only_after_in_flight_reader_completes() {
    loom::model(|| {
        let m = Model::new();
        let m_r = Arc::clone(&m);
        let reader = thread::spawn(move || run_reader(m_r));
        run_drop(&m);
        let took_fast = reader.join().unwrap();

        // THE INVARIANT (interleaving contract): once the drainer's
        // wait_for_drain returns (in_flight hit 0), a fast-path reader that
        // entered the drain set has completed its modeled read — the guard
        // decrement, which is what drove in_flight back to 0, is sequenced
        // AFTER the read.
        //
        // Assert against `read_at_drop_return` (sampled inside `run_drop`
        // at the instant its spin loop exits), NOT against a fresh read of
        // `reader_read` here. Reading `reader_read` after `join()` is
        // tautological: `join()` blocks until the reader thread has fully
        // returned, and `run_reader` stores `reader_read` before returning,
        // so the value is always true — the assertion would pass even if
        // `wait_for_drain()` were broken to a no-op. The snapshot at
        // drain-return time is the only observation point that can actually
        // witness a violation.
        if took_fast {
            assert!(
                m.read_at_drop_return.load(Ordering::SeqCst),
                "wait_for_drain returned before the in-flight reader's read \
                 landed — an interleaving hole in the drain contract"
            );
        }
    });
}

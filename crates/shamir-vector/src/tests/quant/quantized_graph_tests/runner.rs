//! Tests for the V5.2 (#411) quantized HNSW adapter — shared test
//! infrastructure: hang-diagnostic graph-test runner (#1232/#1263),
//! deterministic RNG helpers, and runtime-shutdown regression tests.
//!
//! These tests exercise the [`HnswAdapter`] with
//! `quantization == Some(Sq8)`; the topical test groups live in
//! `fit_recall`, `vr8_memory`, and `fastpath_gate`.

use shamir_test_diag::FlakeGuard;
use shamir_types::types::record_id::RecordId;
use std::future::Future;
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::sync::mpsc;
use std::time::Duration;

// ----- hang diagnostics (#1232) --------------------------------------------
//
// Two of these tests were once killed by the anonymous 600 s nextest timeout
// (once on ubuntu, once on macOS) with no output. A phase marker on stderr
// names the stuck phase in the CI log; the deadline turns a stall of an
// otherwise live runtime into a named failure. If the runtime thread itself
// is blocked synchronously the deadline cannot fire, and the last marker is
// the evidence.

pub(in crate::tests) fn phase(test: &str, name: &str) {
    eprintln!("SQ8-PHASE {test}: {name}");
}

async fn within_deadline<F: Future>(test: &str, fut: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(120), fut)
        .await
        .unwrap_or_else(|_| {
            panic!("{test}: no progress for 120 s (the last SQ8-PHASE marker names the phase)")
        })
}

// cancel-safe: NO for graph bodies — upsert awaits may leave running blocking
// inserts or detached tasks. Deadline cancellation fails the test; the runner
// bounds shutdown rather than pretending cancellation stops those inserts.
// #1263: ordinary runtime Drop waits indefinitely for running spawn_blocking
// work. Catch the body panic while retaining runtime ownership, bound shutdown
// on both exits, then propagate the original assertion/deadline payload.
// Requested flavor change: these were current_thread tests; use one async
// worker (not the machine's core count) on the manual multi-thread runtime.
pub(in crate::tests) fn run_graph_test<F: Future>(test: &str, body: F) -> F::Output {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("build graph test runtime");
    // Test state is not reused after an unwind; only runtime shutdown follows.
    let result = catch_unwind(AssertUnwindSafe(|| {
        runtime.block_on(within_deadline(test, body))
    }));
    runtime.shutdown_timeout(Duration::from_secs(5));
    match result {
        Ok(output) => output,
        Err(payload) => resume_unwind(payload),
    }
}

// ----- runtime shutdown regressions (#1263) -------------------------------

// The outer synchronous test owns release, so the blocking closure cannot
// finish until run_graph_test returns/unwinds. Ordinary runtime Drop would
// deadlock here until the runner kills the test. Release also runs if any
// assertion panics, and the completion channel ensures cleanup is observed.
struct BlockingReleaseGuard {
    release: Option<mpsc::Sender<()>>,
    finished: mpsc::Receiver<()>,
}

impl Drop for BlockingReleaseGuard {
    fn drop(&mut self) {
        drop(self.release.take());
        // Disconnection also handles setup failing before the closure starts.
        let _ = self.finished.recv();
    }
}

fn gated_blocking_task() -> (BlockingReleaseGuard, impl Future<Output = ()>) {
    let (release, wait_for_release) = mpsc::channel();
    let (finished, wait_for_finish) = mpsc::channel();
    let guard = BlockingReleaseGuard {
        release: Some(release),
        finished: wait_for_finish,
    };
    let start = async move {
        let (started, wait_for_start) = tokio::sync::oneshot::channel();
        // fire-and-forget: detached by design to reproduce pending blocking
        // work at shutdown; the outer guard releases and observes completion.
        drop(tokio::task::spawn_blocking(move || {
            // A cancelled body no longer needs the start acknowledgement.
            let _ = started.send(());
            let _ = wait_for_release.recv();
            let _ = finished.send(());
        }));
        wait_for_start.await.expect("blocking closure started");
    };
    (guard, start)
}

#[test]
fn graph_runner_success_with_pending_blocking_task() {
    let (guard, started) = gated_blocking_task();
    let output = run_graph_test("graph_runner_success_with_pending_blocking_task", async {
        started.await;
        42
    });
    assert_eq!(output, 42, "runner must return the body's output");
    assert!(matches!(
        guard.finished.try_recv(),
        Err(mpsc::TryRecvError::Empty),
    ));
    drop(guard);
}

#[test]
fn graph_runner_preserves_panic_with_pending_blocking_task() {
    let (guard, started) = gated_blocking_task();
    let result = catch_unwind(AssertUnwindSafe(|| {
        run_graph_test(
            "graph_runner_preserves_panic_with_pending_blocking_task",
            async {
                started.await;
                panic!("original graph body assertion");
            },
        );
    }));
    let payload = result.expect_err("runner must propagate the body panic");
    assert_eq!(
        payload.downcast_ref::<&str>(),
        Some(&"original graph body assertion"),
    );
    assert!(matches!(
        guard.finished.try_recv(),
        Err(mpsc::TryRecvError::Empty),
    ));
    drop(guard);
}

#[test]
fn graph_runner_preserves_deadline_with_pending_blocking_task() {
    let (guard, started) = gated_blocking_task();
    let result = catch_unwind(AssertUnwindSafe(|| {
        run_graph_test(
            "graph_runner_preserves_deadline_with_pending_blocking_task",
            async {
                started.await;
                // After the handshake the closure is definitely running. Pending
                // never resolves: ZERO deterministically expires without sleeps,
                // elapsed-time assertions, or a current-thread virtual-time feature.
                tokio::time::timeout(Duration::ZERO, std::future::pending::<()>())
                    .await
                    .unwrap_or_else(|_| {
                        panic!(
                            "graph_runner_preserves_deadline_with_pending_blocking_task: \
                                blocking phase deadline expired"
                        )
                    });
            },
        );
    }));
    let payload = result.expect_err("runner must propagate the actual deadline panic");
    assert_eq!(
        payload.downcast_ref::<&str>(),
        Some(
            &"graph_runner_preserves_deadline_with_pending_blocking_task: \
               blocking phase deadline expired"
        ),
    );
    assert!(matches!(
        guard.finished.try_recv(),
        Err(mpsc::TryRecvError::Empty),
    ));
    drop(guard);
}

// ----- deterministic RNG (historical high-32 fixture mapping) --------------

struct Lcg {
    state: u64,
}
impl Lcg {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }
    #[inline]
    fn next_u64(&mut self) -> u64 {
        self.state = self
            .state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.state
    }
    #[inline]
    fn next_f32(&mut self) -> f32 {
        let high = (self.next_u64() >> 32) as u32;
        (high as f32) / (1u64 << 32) as f32
    }
    #[inline]
    fn next_range(&mut self, lo: f32, hi: f32) -> f32 {
        lo + (hi - lo) * self.next_f32()
    }
    fn next_gaussian(&mut self) -> f32 {
        loop {
            let u1 = self.next_f32() * 2.0 - 1.0;
            let u2 = self.next_f32() * 2.0 - 1.0;
            let s = u1 * u1 + u2 * u2;
            if s > 0.0 && s < 1.0 {
                let mul = ((-2.0 * s.ln()) / s).sqrt();
                return u1 * mul;
            }
        }
    }
}

pub(in crate::tests) fn clustered(
    n: usize,
    dim: usize,
    k: usize,
    sigma: f32,
    seed: u64,
) -> Vec<Vec<f32>> {
    assert!(k > 0);
    let mut rng = Lcg::new(seed);
    let centroids: Vec<Vec<f32>> = (0..k)
        .map(|_| (0..dim).map(|_| rng.next_range(-1.0, 1.0)).collect())
        .collect();
    (0..n)
        .map(|i| {
            let c = &centroids[i % k];
            (0..dim)
                .map(|j| c[j] + sigma * rng.next_gaussian())
                .collect()
        })
        .collect()
}

pub(in crate::tests) fn rid(n: u64) -> RecordId {
    let bytes = (n as u128).to_be_bytes();
    RecordId(bytes)
}

/// #433 — seeded single-vector generator for the fast-path convergence
/// regression tests (deterministic, one vector per seed).
pub(in crate::tests) fn random_vec(dim: usize, seed: u64) -> Vec<f32> {
    let mut rng = Lcg::new(seed.wrapping_add(0x9E37_79B9_7F4A_7C15));
    (0..dim).map(|_| rng.next_range(-1.0, 1.0)).collect()
}

pub(in crate::tests) fn recall_at_k(truth: &[RecordId], cand: &[RecordId]) -> f32 {
    let hits = cand
        .iter()
        .filter(|c| truth.iter().any(|t| t == *c))
        .count();
    hits as f32 / truth.len().max(1) as f32
}

/// Shared failure-only recall diagnostics for the statistical recall tests
/// (design: docs/dev-artifacts/design/flake-failure-logs.md): notes the
/// dataset size, k, ef and the measured recall against its floor so a
/// statistical miss is reproducible from the failure log alone. Call right
/// before the recall assertion.
pub(in crate::tests) fn note_recall_stats(
    guard: &FlakeGuard,
    dataset_n: usize,
    k: usize,
    ef_search: u32,
    recall: f64,
    floor: f64,
) {
    guard.note("dataset size", dataset_n.to_string());
    guard.note("k", k.to_string());
    guard.note("ef_search", ef_search.to_string());
    guard.note("measured recall", format!("{recall:.4}"));
    guard.note("recall floor", format!("{floor:.4}"));
}

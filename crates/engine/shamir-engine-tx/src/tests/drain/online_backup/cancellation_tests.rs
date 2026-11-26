//! Cancellation of the online-backup barriers, two PROVEN kinds,
//! deliberately distinguished:
//!
//! * COOPERATIVE (R2a follow-up 3): one parameterized test drives
//!   [`RepoInstance::seal_point_controlled`] on a dedicated OS thread via
//!   `Handle::block_on` of the TEST runtime's handle (one current-thread
//!   runtime; its home thread is the test, which keeps driving timers,
//!   I/O and tasks through async awaits) and refuses it at EVERY named
//!   seam (`after_close`, `after_quiesce`, `after_drain`, `after_pause`,
//!   `after_recheck`, `after_flush`) via an EXTERNAL `AtomicBool` the
//!   caller-supplied `cancelled` closure reads — the typed
//!   [`OnlineBackupError::Cancelled`], never a dropped future. The
//!   barrier suspends deterministically INSIDE the synchronous step hook
//!   (per-thread state; readiness via a tokio oneshot, bounded release
//!   via a `sync_channel(1)` `recv_timeout` on the barrier thread). A
//!   transaction pre-staged BEFORE the barrier is driven as the SAME
//!   pinned `commit_tx` future: polled only after the hook fired (gate
//!   already closed, so it parks on the refused gate with NO lease —
//!   asserted `Pending`, twice), then awaited to completion AFTER the
//!   refusal — the SAME writer commits exactly baseline + 1. WAL
//!   snapshots while paused assert `max == Some(committed)` and no frame
//!   above the baseline; a caller truncation pin acquired BEFORE seeding
//!   holds its floor through all six refusals.
//! * UNWIND (brief item 8, test 4): the older tests DROP the barrier
//!   future exactly at a named step via a deterministic panic inside the
//!   `set_online_backup_step_hook` seam — the panic payload recovered
//!   from the `JoinHandle` proves the future was mid-flight AT that step
//!   (no vacuous abort, no cross-test arm: those barriers run on the
//!   test's own runtime thread). This is the RAII-unwind guarantee,
//!   distinct from the cooperative refusal; both must leave the same
//!   fail-closed state. After EVERY drop: the gate is open with zero
//!   leases, the pause is released, and fresh capture AND seal barriers
//!   succeed.
//!
//! One plain `fn(&'static str)` hook serves BOTH modes (the slot is a
//! process-global `OnceLock`, first installer wins — every test here
//! installs the same fn); per-run behavior lives in per-thread state.
//! No sleeps — the hook fire / handoff IS the synchronization; every
//! await and blocking recv is bounded by a generous harness budget that
//! turns a regression into a fast, loud failure, never into a pass.

use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use shamir_test_diag::FlakeGuard;
use shamir_tx::IsolationLevel;
use shamir_types::types::value::InnerValue;

use crate::repo::instance::repo_instance::OnlineBackupError;
use crate::repo::RepoInstance;
use crate::table::TableConfig;
use crate::tx::tests::drain::drainer_tests::support::make_repo;

/// Harness cap around each bounded await / blocking recv; the
/// synchronization itself is the hook fire, the oneshot handoff or the
/// abort join.
const CANCEL_BUDGET: Duration = Duration::from_secs(30);

/// Every step the controlled seal fires, in barrier order (the production
/// worker added `after_flush`; capture fires only the first two).
const CONTROL_STEPS: [&str; 6] = [
    "after_close",
    "after_quiesce",
    "after_drain",
    "after_pause",
    "after_recheck",
    "after_flush",
];

/// Marker inside the unwind panic payload.
const SENTINEL: &str = "step-hook unwind sentinel";

/// Per-thread hook state. The unwind tests run their barrier on the test's
/// own runtime thread and arm the panic mode there; the cooperative test
/// runs the barrier on a dedicated OS thread that configures the block
/// mode before `Handle::block_on`.
#[derive(Default)]
struct HookState {
    /// Unwind mode, one-shot: the step whose next fire panics.
    armed: Option<&'static str>,
    /// Every step this thread's barriers fired.
    recorded: Vec<&'static str>,
    /// The single seam at which the hook suspends the barrier.
    block_at: Option<&'static str>,
    /// Bound for the hook's blocking release wait.
    block_budget: Duration,
    /// Barrier -> test: which seam the hook suspended at (oneshot).
    readiness: Option<tokio::sync::oneshot::Sender<&'static str>>,
    /// Test -> barrier: releases the suspended hook (capacity 1, so the
    /// test-side send never blocks while the hook is parked on recv).
    release: Option<std::sync::mpsc::Receiver<()>>,
}

thread_local! {
    static STEP_HOOK_STATE: RefCell<HookState> = RefCell::new(HookState::default());
}

/// The ONE hook fn every test installs (the slot is a process-global
/// `OnceLock`; identical fn pointers make first-installer-wins harmless).
fn barrier_step_hook(step: &'static str) {
    STEP_HOOK_STATE.with(|state| {
        let mut state = state.borrow_mut();
        state.recorded.push(step);
        if state.armed == Some(step) {
            state.armed = None;
            panic!("{SENTINEL}: {step}");
        }
        if state.block_at == Some(step) {
            if let Some(tx) = state.readiness.take() {
                let _ = tx.send(step);
            }
            if let Some(rx) = state.release.as_ref() {
                let _ = rx.recv_timeout(state.block_budget);
            }
        }
    });
}

fn arm_step(step: &'static str) {
    STEP_HOOK_STATE.with(|state| {
        let mut state = state.borrow_mut();
        assert!(
            state.armed.is_none(),
            "a step is already armed on this thread"
        );
        state.armed = Some(step);
    });
}

/// Outcome of one controlled barrier run on the dedicated OS thread.
struct BarrierOutcome {
    result: Result<u64, OnlineBackupError>,
    recorded: Vec<&'static str>,
}

/// One live controlled barrier run: the dedicated thread's join handle,
/// the readiness / result oneshots, the bounded release channel and the
/// external cancel flag the `cancelled` closure reads.
struct SealRun {
    handle: std::thread::JoinHandle<()>,
    ready: tokio::sync::oneshot::Receiver<&'static str>,
    results: tokio::sync::oneshot::Receiver<BarrierOutcome>,
    release: std::sync::mpsc::SyncSender<()>,
    cancel_flag: Arc<AtomicBool>,
}

/// Spawn the controlled seal for ONE seam on a dedicated OS thread,
/// driving the future on the TEST runtime's handle (`Handle::block_on` —
/// one current-thread runtime; its home thread is the test, still driving
/// timers/IO/tasks). The thread configures its own (fresh) per-thread
/// hook state first, installs the shared hook fn, then runs
/// `seal_point_controlled` and hands the outcome back over the oneshot.
fn spawn_controlled_seal(
    repo: &RepoInstance,
    step: &'static str,
    runtime: tokio::runtime::Handle,
) -> SealRun {
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (results_tx, results_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
    let cancel_flag = Arc::new(AtomicBool::new(false));
    let owned = repo.clone();
    let flag = Arc::clone(&cancel_flag);
    let handle = std::thread::Builder::new()
        .name("controlled-seal-barrier".into())
        .stack_size(16 * 1024 * 1024)
        .spawn(move || {
            STEP_HOOK_STATE.with(|state| {
                let mut state = state.borrow_mut();
                *state = HookState {
                    block_at: Some(step),
                    block_budget: CANCEL_BUDGET,
                    readiness: Some(ready_tx),
                    release: Some(release_rx),
                    ..HookState::default()
                };
            });
            RepoInstance::set_online_backup_step_hook(barrier_step_hook);
            let result = runtime.block_on(Box::pin(async move {
                owned
                    .seal_point_controlled(None, move || flag.load(Ordering::Acquire))
                    .await
                    .map(|hold| hold.position())
            }));
            let recorded = STEP_HOOK_STATE.with(|state| state.borrow().recorded.clone());
            let _ = results_tx.send(BarrierOutcome { result, recorded });
        })
        .expect("barrier thread spawn");
    SealRun {
        handle,
        ready: ready_rx,
        results: results_rx,
        release: release_tx,
        cancel_flag,
    }
}

/// The recorded steps must be EXACTLY the control-step prefix up to and
/// including `upto` — nothing skipped, nothing beyond (the refusal fired
/// at that boundary and nowhere else).
fn assert_recorded_prefix(recorded: &[&'static str], upto: &'static str) {
    let end = CONTROL_STEPS
        .iter()
        .position(|candidate| *candidate == upto)
        .expect("unknown control step")
        + 1;
    assert_eq!(
        recorded,
        &CONTROL_STEPS[..end],
        "the barrier must fire exactly the steps up to {upto}"
    );
}

async fn seed_commit(repo: &RepoInstance, value: &str) {
    let table = repo.get_table("t").await.unwrap();
    let (mut tx, _guard) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    table
        .insert_tx(&InnerValue::Str(value.into()), Some(&mut tx))
        .await
        .unwrap();
    repo.commit_tx(tx).await.unwrap();
}

async fn retained_versions(repo: &RepoInstance) -> Vec<u64> {
    let wal = repo.repo_wal().await.unwrap();
    let mut versions: Vec<u64> = wal
        .recover()
        .await
        .unwrap()
        .iter()
        .map(|entry| entry.commit_version)
        .collect();
    versions.sort_unstable();
    versions
}

/// Fail-closed invariants after the refusals, then fresh capture AND seal
/// barriers succeed (no leaked close owner, no leaked hold).
async fn assert_fail_closed(repo: &RepoInstance) {
    let admission = repo.core().commit_admission();
    assert!(
        !admission.is_closed(),
        "gate must reopen after the refusals"
    );
    assert_eq!(admission.active_count(), 0, "no leaked admission lease");
    assert!(
        !repo.drainer().pause_held(),
        "pause released after the refusals"
    );

    let position = tokio::time::timeout(CANCEL_BUDGET, repo.capture_point())
        .await
        .expect("fresh capture completes after the refusals")
        .expect("fresh capture succeeds after the refusals");
    assert_eq!(position, repo.tx_gate().await.unwrap().last_committed());
    let hold = tokio::time::timeout(CANCEL_BUDGET, repo.seal_point())
        .await
        .expect("fresh seal completes after the refusals")
        .expect("fresh seal succeeds after the refusals");
    drop(hold);
    assert!(!repo.drainer().pause_held());
}

/// ONE parameterized test, all six named seams: at each seam the barrier
/// suspends INSIDE the synchronous hook with the gate closed; the
/// pre-staged transaction's SAME pinned `commit_tx` future is polled
/// there (Pending, twice — parked on the refused gate, no lease), the WAL
/// snapshot stays exactly at the committed baseline, the external
/// `AtomicBool` is signalled, the hook is released, the seal refuses with
/// the typed `Cancelled`, and the SAME future is then awaited to commit
/// exactly baseline + 1. A caller pin acquired BEFORE seeding holds its
/// floor through every refusal.
#[tokio::test]
async fn controlled_seal_refuses_cooperatively_at_every_named_step() {
    // Failure-only flake diagnostics: the timeline proves which phase hung
    // (hook suspension, parked writer poll, or the refusal handoff) if this
    // ever wedges on CI; the report prints ONLY on failure. The watchdog
    // fires below the runner's slow-kill so the dump lands first.
    let diag = FlakeGuard::with_watchdog(
        "controlled_seal_refuses_cooperatively_at_every_named_step",
        Duration::from_secs(120),
    );
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    // Caller pin BEFORE seeding: floor 0 stays live through every refusal.
    let pin = repo.acquire_truncation_pin(0).await.unwrap();
    seed_commit(&repo, "cancellation-seed").await;
    let mut committed = repo.current_commit_version().await.unwrap();
    let baseline = retained_versions(&repo).await;
    assert_eq!(baseline.last(), Some(&committed), "WAL at the baseline");
    let runtime = tokio::runtime::Handle::current();

    for (index, step) in CONTROL_STEPS.into_iter().enumerate() {
        let expect_pause = matches!(step, "after_pause" | "after_recheck" | "after_flush");
        // Pre-stage the writer's transaction BEFORE the barrier; the SAME
        // pinned commit future is only POLLED after the hook fired.
        let table = repo.get_table("t").await.unwrap();
        let (mut tx, guard) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
        table
            .insert_tx(
                &InnerValue::Str(format!("cooperative-{index}")),
                Some(&mut tx),
            )
            .await
            .unwrap();
        let writer_repo = repo.clone();
        let run = spawn_controlled_seal(&repo, step, runtime.clone());
        diag.mark(format!("barrier spawned for {step}"));

        // Deterministic suspend at the seam, gate closed.
        let at = tokio::time::timeout(CANCEL_BUDGET, run.ready)
            .await
            .expect("the barrier never reached the blocked seam")
            .expect("readiness handoff");
        diag.mark(format!("barrier suspended at {at}"));
        assert_eq!(at, step, "the barrier suspended at an unexpected seam");
        // ONLY NOW poll the pre-staged commit future: against the already
        // closed gate it deterministically parks on `acquire_leader`
        // holding NO lease (no pre-close lease race). Two polls prove it
        // stays pending — the commit cannot slip past the closed gate.
        let mut writer = Box::pin(async move {
            let _snapshot_guard = guard;
            writer_repo
                .commit_tx(tx)
                .await
                .map(|outcome| outcome.commit_version)
        });
        assert!(
            futures::poll!(writer.as_mut()).is_pending(),
            "the pre-staged commit must park on the closed gate at {step}"
        );
        assert!(
            futures::poll!(writer.as_mut()).is_pending(),
            "the pre-staged commit must still be parked at {step}"
        );
        let admission = repo.core().commit_admission();
        assert!(admission.is_closed(), "the gate is closed at {step}");
        assert_eq!(admission.active_count(), 0, "no lease at {step}");
        assert_eq!(
            repo.drainer().pause_held(),
            expect_pause,
            "drain pause state at {step}"
        );
        // Anti-vacuity while paused: exactly the committed baseline, and
        // NO frame above it (the parked writer has not published).
        let suspended = retained_versions(&repo).await;
        assert_eq!(
            suspended.last(),
            Some(&committed),
            "WAL top must be the committed baseline at {step}"
        );
        assert!(
            suspended.iter().all(|version| version <= &committed),
            "no WAL frame above the baseline while paused at {step}, \
             got {suspended:?}"
        );

        // Signal the external cancel flag, then release the synchronous
        // hook: the boundary check fires the typed refusal.
        run.cancel_flag.store(true, Ordering::Release);
        run.release.send(()).expect("release handoff");
        diag.note("cancel flagged", step);
        let outcome = tokio::time::timeout(CANCEL_BUDGET, run.results)
            .await
            .expect("the barrier never finished after its release")
            .expect("barrier outcome handoff");
        run.handle.join().expect("barrier thread");
        assert!(
            matches!(outcome.result, Err(OnlineBackupError::Cancelled)),
            "step {step} must refuse cooperatively, got {:?}",
            outcome.result
        );
        assert_recorded_prefix(&outcome.recorded, step);

        // Await the SAME pinned future: the same writer commits exactly
        // baseline + 1 after the refusal reopened the gate.
        let version = tokio::time::timeout(CANCEL_BUDGET, writer.as_mut())
            .await
            .expect("the parked writer never finished its commit")
            .expect("parked writer committed");
        assert_eq!(version, committed + 1, "the same writer commits at {step}");
        diag.mark(format!(
            "writer committed {version} after refusal at {step}"
        ));
        committed = version;
    }

    // While the pin is still held (floor 0), every recovered version —
    // seeded baseline and each post-refusal writer commit — must still be
    // retained. Deletion/truncation assertions are covered by the dedicated
    // pin-release tests; a background drainer may legitimately settle the
    // final writer before we drop our pin, so only assert the full prefix.
    let held = retained_versions(&repo).await;
    let expected: Vec<u64> = (1..=committed).collect();
    assert_eq!(
        held, expected,
        "floor 0 must retain the complete seeded+writer prefix"
    );
    drop(pin);
    assert_fail_closed(&repo).await;
}

async fn fixture_with_committed_data() -> RepoInstance {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    seed_commit(&repo, "cancellation-seed").await;
    repo
}

// ---------------------------------------------------------------------------
// Unwind cancellation (RAII drop mid-flight) — the older guarantee
// ---------------------------------------------------------------------------

/// Drive the barrier to the armed step (a deterministic unwind there),
/// then assert the fail-closed invariants AND that fresh barriers
/// succeed. The panic payload proves the future was dropped mid-flight at
/// exactly the armed step.
async fn cancel_barrier_at(repo: &RepoInstance, step: &'static str, seal: bool) {
    RepoInstance::set_online_backup_step_hook(barrier_step_hook);
    arm_step(step);
    let owned = repo.clone();
    let task = if seal {
        tokio::spawn(Box::pin(async move {
            owned.seal_point().await.map(|hold| hold.position())
        }))
    } else {
        tokio::spawn(Box::pin(async move { owned.capture_point().await }))
    };
    let outcome = tokio::time::timeout(CANCEL_BUDGET, task)
        .await
        .expect("the barrier never reached the armed step")
        .expect_err("the barrier must unwind at the armed step, not complete");
    assert!(
        outcome.is_panic(),
        "the barrier completed before the armed step fired"
    );
    let payload = outcome.into_panic();
    let expected = format!("{SENTINEL}: {step}");
    let message = payload
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| payload.downcast_ref::<&'static str>().copied());
    assert_eq!(
        message,
        Some(expected.as_str()),
        "the unwind must have fired exactly the armed step"
    );

    let admission = repo.core().commit_admission();
    assert!(!admission.is_closed(), "gate must reopen after the drop");
    assert_eq!(admission.active_count(), 0, "no leaked admission lease");
    assert!(
        !repo.drainer().pause_held(),
        "pause released after the drop"
    );

    // No leaked close owner / hold: both barriers complete cleanly.
    let position = tokio::time::timeout(CANCEL_BUDGET, repo.capture_point())
        .await
        .expect("fresh capture completes after cancellation")
        .expect("fresh capture succeeds after cancellation");
    assert_eq!(position, repo.tx_gate().await.unwrap().last_committed());
    let hold = tokio::time::timeout(CANCEL_BUDGET, repo.seal_point())
        .await
        .expect("fresh seal completes after cancellation")
        .expect("fresh seal succeeds after cancellation");
    drop(hold);
    assert!(!repo.drainer().pause_held());
}

#[tokio::test]
async fn capture_cancelled_after_close_leaves_no_owner() {
    let repo = fixture_with_committed_data().await;
    cancel_barrier_at(&repo, "after_close", false).await;
}

#[tokio::test]
async fn capture_cancelled_after_quiesce_leaves_no_owner() {
    let repo = fixture_with_committed_data().await;
    cancel_barrier_at(&repo, "after_quiesce", false).await;
}

#[tokio::test]
async fn seal_cancelled_after_drain_leaves_no_owner_or_pause() {
    let repo = fixture_with_committed_data().await;
    cancel_barrier_at(&repo, "after_drain", true).await;
}

#[tokio::test]
async fn seal_cancelled_after_pause_releases_the_pause() {
    let repo = fixture_with_committed_data().await;
    cancel_barrier_at(&repo, "after_pause", true).await;
}

#[tokio::test]
async fn seal_cancelled_after_recheck_releases_the_pause() {
    let repo = fixture_with_committed_data().await;
    cancel_barrier_at(&repo, "after_recheck", true).await;
}

/// The hold itself lives in a task: while the hold is alive BOTH gates
/// are held (admission closed, drain paused); aborting the owner task
/// unwinds the hold — pause released FIRST, gate reopened LAST — and a
/// fresh writer commits and both fresh barriers succeed.
#[tokio::test]
async fn hold_owning_task_abort_releases_both_gates() {
    let repo = fixture_with_committed_data().await;
    let (got_hold_tx, got_hold_rx) = tokio::sync::oneshot::channel();
    let owned = repo.clone();
    let task = tokio::spawn(Box::pin(async move {
        let hold = owned.seal_point().await.expect("seal in the owner task");
        let _ = got_hold_tx.send(());
        std::future::pending::<()>().await;
        drop(hold);
    }));
    tokio::time::timeout(CANCEL_BUDGET, got_hold_rx)
        .await
        .expect("the owner task never obtained the hold")
        .expect("hold handoff");

    // While the hold lives inside the task, the hold keeps BOTH gates.
    let admission = repo.core().commit_admission();
    assert!(admission.is_closed(), "the hold keeps the gate closed");
    assert!(
        repo.drainer().pause_held(),
        "the hold keeps the drain paused"
    );

    // Bounded abort: the unwind drops the hold (pause FIRST, gate LAST).
    task.abort();
    let outcome = tokio::time::timeout(CANCEL_BUDGET, task)
        .await
        .expect("the abort join hung")
        .expect_err("abort join");
    assert!(outcome.is_cancelled(), "the abort must cancel the task");
    assert!(!repo.drainer().pause_held(), "unwind released the pause");
    assert!(!admission.is_closed(), "unwind reopened the gate");

    // Fresh writer succeeds once both gates are open.
    let table = repo.get_table("t").await.unwrap();
    let (mut tx, _guard) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let id = table
        .insert_tx(&InnerValue::Str("post-abort".into()), Some(&mut tx))
        .await
        .unwrap();
    repo.commit_tx(tx).await.unwrap();
    let got = table.get(id).await.unwrap();
    assert!(
        matches!(got, InnerValue::Str(ref s) if s == "post-abort"),
        "fresh writer data readable after the abort, got {got:?}"
    );

    // Fresh barriers succeed: no leaked close owner, no leaked hold.
    let position = tokio::time::timeout(CANCEL_BUDGET, repo.capture_point())
        .await
        .expect("fresh capture completes after the abort")
        .expect("fresh capture succeeds after the abort");
    assert_eq!(position, repo.tx_gate().await.unwrap().last_committed());
    let hold = tokio::time::timeout(CANCEL_BUDGET, repo.seal_point())
        .await
        .expect("fresh seal completes after the abort")
        .expect("fresh seal succeeds after the abort");
    drop(hold);
    assert!(!repo.drainer().pause_held());
}

/// Sanity anchor: the hook really fired every named step across the
/// seal barrier run on this thread — including the production worker's
/// `after_flush` — so the armed handoffs of both modes were never
/// silently short-circuited by barrier completion before the armed step.
#[tokio::test]
async fn step_hook_fires_every_named_barrier_step() {
    RepoInstance::set_online_backup_step_hook(barrier_step_hook);
    let repo = fixture_with_committed_data().await;
    let hold = repo.seal_point().await.expect("seal");
    drop(hold);

    let recorded = STEP_HOOK_STATE.with(|state| state.borrow().recorded.clone());
    for expected in CONTROL_STEPS {
        assert!(
            recorded.contains(&expected),
            "step {expected} never fired; recorded {recorded:?}"
        );
    }
}

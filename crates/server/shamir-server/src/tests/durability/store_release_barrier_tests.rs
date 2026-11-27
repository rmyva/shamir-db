use std::fs::{File, OpenOptions};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

#[cfg(unix)]
use crate::server::open_lock_is_discovery_gap;
use crate::server::{
    bounded_probe_for_test, install_re_probe_gate, release_stores, wait_stores_released,
    ReleaseOutcome, UnverifiedReason, RE_PROBE_SUBMITS,
};
use tokio::time::Instant;

/// A fake fjall store root: `lock` + `version` marker files.
fn make_store(root: &Path, name: &str) -> PathBuf {
    let dir = root.join(name);
    std::fs::create_dir_all(&dir).expect("store dir");
    std::fs::write(dir.join("version"), b"3").expect("version marker");
    std::fs::write(dir.join("lock"), b"").expect("lock file");
    dir
}

fn hold_lock(dir: &Path) -> File {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(dir.join("lock"))
        .expect("open lock");
    file.try_lock().expect("take the store lock");
    file
}

#[tokio::test]
async fn no_stores_means_released() {
    let temp = tempfile::tempdir().expect("tempdir");
    let res = wait_stores_released(temp.path(), Duration::from_secs(5)).await;
    assert!(res.is_ok());
}

#[tokio::test]
async fn unlocked_stores_are_released_at_once() {
    let temp = tempfile::tempdir().expect("tempdir");
    make_store(temp.path(), "a.redb");
    make_store(&temp.path().join("nested"), "b");
    let res = wait_stores_released(temp.path(), Duration::from_secs(5)).await;
    assert!(res.is_ok());
}

#[tokio::test]
async fn waits_until_the_last_holder_drops_the_lock() {
    let temp = tempfile::tempdir().expect("tempdir");
    let store = make_store(temp.path(), "held.redb");
    let holder = hold_lock(&store);

    let released = Arc::new(AtomicBool::new(false));
    let release = {
        let released = Arc::clone(&released);
        async move {
            tokio::time::sleep(Duration::from_millis(80)).await;
            released.store(true, Ordering::SeqCst);
            drop(holder);
        }
    };
    let (res, ()) = tokio::join!(
        wait_stores_released(temp.path(), Duration::from_secs(30)),
        release
    );

    assert!(res.is_ok());
    assert!(
        released.load(Ordering::SeqCst),
        "the barrier must not return before the holder dropped the lock"
    );
}

#[tokio::test]
async fn reports_the_stores_still_locked_after_the_timeout() {
    let temp = tempfile::tempdir().expect("tempdir");
    let store = make_store(temp.path(), "held.redb");
    let _holder = hold_lock(&store);

    let res = wait_stores_released(temp.path(), Duration::from_millis(100)).await;

    assert_eq!(res, Err(vec![store]));
}

#[tokio::test]
async fn directories_without_both_markers_are_not_stores() {
    let temp = tempfile::tempdir().expect("tempdir");
    let dir = temp.path().join("not-a-store");
    std::fs::create_dir_all(&dir).expect("dir");
    std::fs::write(dir.join("lock"), b"").expect("lock only");
    let _holder = hold_lock(&dir);

    let res = wait_stores_released(temp.path(), Duration::from_millis(100)).await;

    assert!(res.is_ok());
}

// ---------------------------------------------------------------------------
// Discovery must never mistake an unreadable store for a released one.
// ---------------------------------------------------------------------------

/// A fjall store can be reached through a directory SYMLINK: the launcher opens
/// `data_dir/users` by pathname, and a deployment can make that a link to the
/// real directory. Discovery must FOLLOW the link, probe the TARGET, and report
/// the LOGICAL path.
///
/// Mutant: the former `entry.file_type().is_dir()` walk (always false for a
/// link) never descends into the link, finds no store at all, and answers
/// `Released` while the lock is held — the first assertion then sees `Ok(())`
/// instead of `Err(vec![data_dir/users])`; a barrier that cannot probe through
/// the link at all fails the second half.
#[cfg(unix)]
#[tokio::test]
async fn a_store_reached_through_a_directory_symlink_is_probed() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().join("data");
    std::fs::create_dir_all(&data_dir).expect("data dir");
    // The store itself lives outside the data dir; only the link is inside.
    let target = temp.path().join("users-target");
    std::fs::create_dir_all(&target).expect("store dir");
    std::fs::write(target.join("version"), b"3").expect("version marker");
    std::fs::write(target.join("lock"), b"").expect("lock file");
    let logical = data_dir.join("users");
    std::os::unix::fs::symlink(&target, &logical).expect("directory symlink");

    let holder = hold_lock(&target);
    assert_eq!(
        wait_stores_released(&data_dir, Duration::from_millis(100)).await,
        Err(vec![logical.clone()]),
        "a store held through a directory symlink must be reported under its logical name"
    );

    holder.unlock().expect("release the store lock");
    assert!(
        wait_stores_released(&data_dir, Duration::from_secs(5))
            .await
            .is_ok(),
        "the very same store is Released once its lock is free (mutant: a walk that \
         cannot reach the store through the link in either direction)"
    );
}

/// A dangling `version` marker is undecidable, not evidence that this is not a
/// store. Discovery must fail closed rather than report an empty store set.
///
/// Mutant: marker metadata errors collapsed into `false` allow the complete
/// walk to finish empty and incorrectly return `Released`.
#[cfg(unix)]
#[tokio::test]
async fn a_dangling_version_marker_is_a_discovery_gap() {
    let temp = tempfile::tempdir().expect("tempdir");
    let store = temp.path().join("users");
    std::fs::create_dir_all(&store).expect("store directory");
    std::os::unix::fs::symlink(store.join("missing-version-target"), store.join("version"))
        .expect("dangling version marker symlink");
    std::fs::write(store.join("lock"), b"").expect("lock marker");

    let outcome = release_stores(temp.path(), Duration::from_millis(100)).await;

    assert!(
        matches!(outcome, ReleaseOutcome::Unverified(_)),
        "an undecidable version marker is a discovery gap, never Released: {outcome:?}"
    );
}

/// A dangling `lock` marker exists but cannot be opened, so it is a discovery
/// gap rather than an absent marker or an unlocked store.
///
/// Mutant: classifying with `symlink_metadata` alone calls this complete, then
/// the probe follows the dangling link and treats `NotFound` as Unlocked; the
/// barrier incorrectly reports `Released`.
#[cfg(unix)]
#[tokio::test]
async fn a_dangling_lock_marker_is_a_discovery_gap() {
    let temp = tempfile::tempdir().expect("tempdir");
    let store = temp.path().join("users");
    std::fs::create_dir_all(&store).expect("store directory");
    std::fs::write(store.join("version"), b"3").expect("version marker");
    std::os::unix::fs::symlink(store.join("missing-lock-target"), store.join("lock"))
        .expect("dangling lock marker symlink");

    let outcome = release_stores(temp.path(), Duration::from_millis(100)).await;

    assert!(
        matches!(outcome, ReleaseOutcome::Unverified(_)),
        "a dangling lock marker is specifically a discovery gap, never Released: {outcome:?}"
    );
}

/// Opening the `lock` marker of a DISCOVERED store is a discovery gap when the
/// open fails with `NotFound`: the marker exists but cannot be followed (link
/// metadata says so), or it vanished after discovery. On Unix an unlinked lock
/// file can still be held through an open descriptor, so absence proves
/// nothing. (Other open errors, such as `PermissionDenied`, are a probe error.)
///
/// Mutant: mapping the open-time `NotFound` to Unlocked (the former result for
/// a vanished marker) makes the second assertion fail; mapping it for an
/// existing dangling marker makes the first one fail.
#[cfg(unix)]
#[test]
fn opening_a_dangling_lock_marker_is_a_discovery_gap() {
    let temp = tempfile::tempdir().expect("tempdir");
    let store = temp.path().join("users");
    std::fs::create_dir_all(&store).expect("store directory");
    std::os::unix::fs::symlink(store.join("missing-lock-target"), store.join("lock"))
        .expect("dangling lock marker symlink");

    assert!(
        open_lock_is_discovery_gap(&store),
        "an existing but unopenable lock marker must be a discovery gap"
    );
    let vanished = temp.path().join("vanished-marker-store");
    std::fs::create_dir_all(&vanished).expect("store whose lock marker vanished");
    assert!(
        open_lock_is_discovery_gap(&vanished),
        "a lock marker that vanished after discovery is UNKNOWN, never unlocked"
    );
}

/// Current-thread runtime with a PAUSED clock and ONE blocking worker: virtual
/// time moves only through `advance`, so the completion stamp and the deadline
/// are compared without any real-time race.
fn paused_runtime_with_one_blocking_worker() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .max_blocking_threads(1)
        .build()
        .expect("test runtime")
}

/// Waits on the runtime thread (so no timer can fire meanwhile) until every
/// blocking job queued before this call has finished: a sentinel queued behind
/// them signals only after they ran.
fn drain_blocking_worker() {
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    tokio::task::spawn_blocking(move || {
        let _ = done_tx.send(());
    });
    done_rx
        .recv_timeout(Duration::from_secs(60))
        .expect("the sentinel runs after the queued jobs on the one blocking worker");
}

/// A probe whose blocking job finishes AFTER the absolute deadline is not an
/// in-budget observation, even when its result is already waiting at the next
/// poll: `timeout_at` polls the receiver before its timer, so without the
/// completion stamp the late result would win and a late empty held set would
/// certify a release.
///
/// The job signals that it entered and is then held on a channel; the test waits
/// for that signal, advances the virtual clock past the deadline, and only then
/// releases and drains the job: the result and the expired timer are both ready
/// at the poll that consumes the result (the probe is not polled during the
/// sentinel wait).
///
/// Mutants: sending the bare value (no completion stamp) returns `Ok`; a stamp
/// taken before the probe call on the worker is ordered before the clock jump by
/// the entry signal, so it classifies the late job in time and returns `Ok`.
#[test]
fn a_probe_finishing_after_the_deadline_is_not_an_observation() {
    paused_runtime_with_one_blocking_worker().block_on(async {
        let deadline = Instant::now() + Duration::from_millis(100);
        let (entered_tx, entered_rx) = std::sync::mpsc::channel::<()>();
        let (gate_tx, gate_rx) = std::sync::mpsc::channel::<()>();
        let mut probe = Box::pin(bounded_probe_for_test(deadline, move || {
            let _ = entered_tx.send(());
            // Parks until the sender is dropped (an `Err` also returns).
            let _ = gate_rx.recv();
            "late empty held set"
        }));
        // The first poll queues the job and waits on its receiver; the job cannot
        // finish before it is released, so the poll is pending.
        let first = std::future::poll_fn(|cx| Poll::Ready(probe.as_mut().poll(cx))).await;
        assert!(first.is_pending(), "the probe must wait for its job");
        entered_rx
            .recv_timeout(Duration::from_secs(60))
            .expect("the probe job must start before the clock moves");

        tokio::time::advance(Duration::from_millis(500)).await;
        drop(gate_tx);
        drain_blocking_worker();

        let outcome = probe.await;

        assert_eq!(
            outcome,
            Err(UnverifiedReason::PoolStarved),
            "a result completed after the deadline must not be accepted as an observation"
        );
    });
}

/// The complement: a probe that finished BEFORE the deadline keeps its result
/// even when the poll that consumes it comes after the deadline.
///
/// The job is held on a channel until the first poll returned pending, then
/// released and drained while the virtual clock is still before the deadline;
/// only then the clock passes the deadline and the probe is polled again.
///
/// Mutant: a stamp taken on the receiving side classifies by the time of the
/// consuming poll and rejects this result as late.
#[test]
fn a_probe_finishing_before_the_deadline_is_an_observation_even_when_polled_late() {
    paused_runtime_with_one_blocking_worker().block_on(async {
        let deadline = Instant::now() + Duration::from_millis(100);
        let (gate_tx, gate_rx) = std::sync::mpsc::channel::<()>();
        let mut probe = Box::pin(bounded_probe_for_test(deadline, move || {
            // Parks until the sender is dropped (an `Err` also returns).
            let _ = gate_rx.recv();
            "in-budget held set"
        }));
        let first = std::future::poll_fn(|cx| Poll::Ready(probe.as_mut().poll(cx))).await;
        assert!(first.is_pending(), "the probe must wait for its job");

        // The job runs and sends while the virtual clock is still before the deadline.
        drop(gate_tx);
        drain_blocking_worker();
        tokio::time::advance(Duration::from_millis(500)).await;

        let outcome = probe.await;

        assert_eq!(
            outcome,
            Ok("in-budget held set"),
            "a result completed in budget must survive a late poll"
        );
    });
}

/// A store whose `lock` entry cannot be opened is UNKNOWN state.
///
/// Mutant: the former `is_released` returned `true` for EVERY lock-file open
/// error, and the former discovery predicate (`lock.is_file()`) did not even
/// classify this directory as a store — both report `Released` for a store
/// whose lock was never observed, and the assertion then sees `Released`.
#[tokio::test]
async fn an_unopenable_lock_file_is_unverified_never_released() {
    let temp = tempfile::tempdir().expect("tempdir");
    let store = temp.path().join("users");
    std::fs::create_dir_all(store.join("lock")).expect("a `lock` DIRECTORY");
    std::fs::write(store.join("version"), b"3").expect("version marker");

    let outcome = release_stores(temp.path(), Duration::from_millis(100)).await;

    assert!(
        matches!(outcome, ReleaseOutcome::Unverified(_)),
        "an unopenable lock file leaves the store state UNKNOWN, got {outcome:?}"
    );
}

/// An unreadable directory may hide a store root, so the walk has not finished.
///
/// Mutant: the former `let Ok(entries) = read_dir(..) else { continue }` skipped
/// the directory, the walk "completed" with an EMPTY set, and the barrier
/// reported `Released` although a store held inside was never seen.
#[cfg(unix)]
#[tokio::test]
async fn an_unreadable_directory_is_a_gap_not_a_release() {
    let temp = tempfile::tempdir().expect("tempdir");
    let unreadable = temp.path().join("unreadable");
    std::fs::create_dir_all(&unreadable).expect("dir");
    std::fs::set_permissions(&unreadable, std::fs::Permissions::from_mode(0o000))
        .expect("mode 000");

    // A privileged runner can read a 0o000 directory, so the gap cannot be
    // reproduced there (the depth-cap test covers the gap path regardless).
    if std::fs::read_dir(&unreadable).is_ok() {
        let _ = std::fs::set_permissions(&unreadable, std::fs::Permissions::from_mode(0o700));
        eprintln!("skipping: this runner can read a 0o000 directory (privileged)");
        return;
    }

    let outcome = release_stores(temp.path(), Duration::from_millis(200)).await;
    // Restore unconditionally, before asserting: a failing claim must never
    // leave an unreadable directory behind for the TempDir cleanup.
    let _ = std::fs::set_permissions(&unreadable, std::fs::Permissions::from_mode(0o700));

    assert!(
        matches!(outcome, ReleaseOutcome::Unverified(_)),
        "an unreadable directory hides part of the store set: UNKNOWN, never Released — got \
         {outcome:?}"
    );
}

// ---------------------------------------------------------------------------
// Observed-lock semantics: only a COMPLETED probe may speak.
// ---------------------------------------------------------------------------

/// A COMPLETE walk that finds no store is a release; a walk cut short is not.
///
/// Mutant: the former depth stop (`if depth >= MAX_DEPTH { continue }`) silently
/// truncated the walk, so an empty result meant "released" although everything
/// below the cap was never looked at — the second half then sees `Released`
/// instead of `Unverified`.
#[tokio::test]
async fn an_empty_discovery_is_released_and_a_truncated_one_is_unverified() {
    let temp = tempfile::tempdir().expect("tempdir");
    assert!(
        matches!(
            release_stores(temp.path(), Duration::from_secs(5)).await,
            ReleaseOutcome::Released
        ),
        "a root with no store, walked completely, has nothing holding a lock: Released"
    );

    // Directories below the walk's depth cap: whether a store root sits down
    // there is unknowable, so the empty result must not be read as a release.
    let mut deep = temp.path().to_path_buf();
    for name in ["a", "b", "c", "d", "e", "f", "g", "h"] {
        deep.push(name);
    }
    std::fs::create_dir_all(&deep).expect("directories below the depth cap");

    assert!(
        matches!(
            release_stores(temp.path(), Duration::from_millis(500)).await,
            ReleaseOutcome::Unverified(_)
        ),
        "a walk that stopped at the depth cap cannot claim the stores released"
    );
}

/// A probe that never runs must not be read as a release.
///
/// Mutant: a barrier whose "no observation yet" is treated as an empty (⇒
/// released) store set, or which invents a `StillLocked` verdict for a store it
/// never observed: the only store here is UNLOCKED, so a probe that could run
/// would answer `Released`, and every other verdict is wrong.
#[test]
fn a_starved_first_probe_is_unverified() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .expect("test runtime");
    let temp = tempfile::tempdir().expect("test root");
    make_store(temp.path(), "counters");

    // Occupy the ONLY blocking worker for the whole budget, released by a
    // channel rendezvous (never a sleep).
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (gate_tx, gate_rx) = std::sync::mpsc::channel::<()>();
    let occupied = runtime.spawn_blocking(move || {
        let _ = started_tx.send(());
        let _ = gate_rx.recv();
    });
    started_rx
        .recv_timeout(Duration::from_secs(30))
        .expect("the pool worker started the occupying job");

    let outcome = runtime.block_on(release_stores(temp.path(), Duration::from_millis(150)));
    let _ = gate_tx.send(());
    runtime.block_on(occupied).expect("the occupier joined");

    assert!(
        matches!(outcome, ReleaseOutcome::Unverified(_)),
        "a first probe that never ran leaves the store state UNKNOWN, got {outcome:?}"
    );
}

/// The verdict comes from a RE-PROBE that was really SUBMITTED, and the last
/// held observation survives its starvation.
///
/// Order: the barrier itself signals, at the re-probe seam, that a first
/// observation COMPLETED (the seam sits only after the `held.is_empty()` and
/// deadline checks); the second occupier parks BEFORE the seam is released, so
/// the re-probe can never run. The process-global submission counter must read
/// exactly 1 after the verdict: a barrier that answers `StillLocked` from the
/// first observation — discovery-seeded, or a fallback returned without ever
/// submitting the re-probe — leaves it at 0 or never reaches the seam at all.
///
/// ONE absolute deadline (150 s, computed once) bounds every wait in the test,
/// including the worker starts and the joins; the barrier budget (10 s) is the
/// time the starved re-probe waits before its verdict, so it stays far below
/// that deadline and far above the channel handshakes.
///
/// Mutants killed: the re-probe skipped (a fallback returned right after the
/// seam — the counter stays 0); the re-probe reordered before the first
/// observation (the barrier finishes before the seam signals — panic branch);
/// a lost observation (`Unverified` — fails the match at the end); a verdict
/// answered from the discovery instead of the completed observation (finishes
/// before arrival — panic branch).
#[test]
fn a_starved_re_probe_keeps_the_last_held_observation() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .expect("test runtime");
    let temp = tempfile::tempdir().expect("test root");
    let store = make_store(temp.path(), "counters");
    let holder = hold_lock(&store);
    let deadline = std::time::Instant::now() + Duration::from_secs(150);

    let outcome = runtime.block_on(async move {
        let deadline = tokio::time::Instant::from_std(deadline);
        // 1. Occupy the ONLY blocking worker; the barrier's first probe queues
        //    behind it. Released by channel, never a sleep.
        let (first_started_tx, first_started_rx) = tokio::sync::oneshot::channel();
        let (first_release_tx, first_release_rx) = std::sync::mpsc::channel::<()>();
        let first = tokio::task::spawn_blocking(move || {
            let _ = first_started_tx.send(());
            let _ = first_release_rx.recv();
        });
        tokio::time::timeout_at(deadline, first_started_rx)
            .await
            .expect("the first occupier started within the test deadline")
            .expect("the first occupier signalled");

        // 2. Install the re-probe gate, then poll the barrier's first future
        //    turn directly: it synchronously submits its first probe before
        //    returning Pending.
        let mut gate = install_re_probe_gate();
        let root = temp.path().to_path_buf();
        // The verdict of the starved re-probe arrives only when this budget
        // runs out, so it must be far below the test deadline; the first
        // observation (a lock probe once the first occupier is released) needs
        // a tiny fraction of it.
        let mut barrier = Box::pin(release_stores(&root, Duration::from_secs(10)));
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(
            std::future::Future::poll(barrier.as_mut(), &mut context).is_pending(),
            "the first probe is queued behind the first occupier"
        );

        // 3. Free the worker: the queued first probe runs, observes the held
        //    store, and the loop reaches the seam.
        drop(first_release_tx);

        // 4. Drive the barrier until the seam's arrival signal. A verdict
        //    arriving first means the re-probe path was skipped or reordered.
        let arrived = tokio::time::timeout_at(deadline, async {
            tokio::select! {
                biased;
                signal = gate.arrived.recv() => signal,
                outcome = &mut barrier => panic!(
                    "the barrier finished before the re-probe seam: the re-probe was \
                     skipped or reordered (mutant): {outcome:?}"
                ),
            }
        })
        .await
        .expect("the re-probe seam arrived within the test deadline");
        assert!(
            arrived.is_some(),
            "the seam must signal the test before parking"
        );

        // 5. Park a second occupier on the worker: the re-probe, submitted
        //    only AFTER this, can never run inside the budget.
        let (second_started_tx, second_started_rx) = tokio::sync::oneshot::channel();
        let (second_release_tx, second_release_rx) = std::sync::mpsc::channel::<()>();
        let second = tokio::task::spawn_blocking(move || {
            let _ = second_started_tx.send(());
            let _ = second_release_rx.recv();
        });
        tokio::time::timeout_at(deadline, second_started_rx)
            .await
            .expect("the second occupier started within the test deadline")
            .expect("the second occupier signalled");

        // 6. Release the seam: the re-probe is submitted behind the parked
        //    second occupier and starves.
        tokio::time::timeout_at(deadline, gate.release.send(()))
            .await
            .expect("the re-probe gate released within the test deadline")
            .expect("the re-probe seam receiver remains alive");

        let outcome = tokio::time::timeout_at(deadline, barrier.as_mut())
            .await
            .expect("the barrier verdict arrived within the test deadline");
        drop(second_release_tx);
        tokio::time::timeout_at(deadline, first)
            .await
            .expect("the first occupier joined within the test deadline")
            .expect("the first occupier joined");
        tokio::time::timeout_at(deadline, second)
            .await
            .expect("the second occupier joined within the test deadline")
            .expect("the second occupier joined");
        outcome
    });

    drop(holder);
    // The fallback answer must come FROM a submitted re-probe: a barrier that
    // returns `StillLocked` without ever submitting one fails here.
    assert_eq!(
        RE_PROBE_SUBMITS.load(std::sync::atomic::Ordering::Relaxed),
        1,
        "exactly one re-probe must have been submitted before the verdict"
    );
    match outcome {
        ReleaseOutcome::StillLocked(still) => assert_eq!(
            still,
            vec![store],
            "the barrier must keep naming the store it observed held when the re-probe \
             cannot run"
        ),
        other => panic!(
            "a held store whose re-probe was starved must stay StillLocked (a discovery \
             is not an observation, and a lost observation is not a release): got {other:?}"
        ),
    }
}

/// A REAL re-probe on a FREE pool answers differently from the first
/// observation: the lock is released while the barrier parks at the seam, so
/// the re-probe must answer `Released` where the first observation saw held.
///
/// Mutants killed: the re-probe skipped or answered from the first observation
/// (`StillLocked` from the stale held set instead of `Released`); the re-probe
/// reordered before the first observation (the barrier then answers from a
/// free FIRST probe and finishes before the seam signals — panic branch); the
/// first observation forged empty (`Released` before arrival — panic branch).
#[test]
fn the_re_probe_sees_the_lock_free_after_the_first_observation() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime");
    let temp = tempfile::tempdir().expect("test root");
    make_store(temp.path(), "counters");
    let holder = hold_lock(&temp.path().join("counters"));
    let deadline = std::time::Instant::now() + Duration::from_secs(150);

    let outcome = runtime.block_on(async move {
        let deadline = tokio::time::Instant::from_std(deadline);
        let mut gate = install_re_probe_gate();
        let root = temp.path().to_path_buf();
        let probe_budget = deadline.saturating_duration_since(tokio::time::Instant::now());
        let mut barrier = Box::pin(release_stores(&root, probe_budget));
        // The pool is FREE: the first probe runs at once and the loop reaches
        // the seam with the completed held observation.
        let arrived = tokio::time::timeout_at(deadline, async {
            tokio::select! {
                biased;
                signal = gate.arrived.recv() => signal,
                outcome = &mut barrier => panic!(
                    "the barrier finished before the re-probe seam (mutant: the first \
                     observation was forged empty or the re-probe ran early): {outcome:?}"
                ),
            }
        })
        .await
        .expect("the re-probe seam arrived within the test deadline");
        assert!(
            arrived.is_some(),
            "the seam must signal the test before parking"
        );

        // Change the world the re-probe will see: the lock is released while
        // the barrier is parked at the seam.
        drop(holder);

        // Release the seam: the re-probe runs on the free pool and must see
        // the lock FREE — the answer must change.
        tokio::time::timeout_at(deadline, gate.release.send(()))
            .await
            .expect("the re-probe gate released within the test deadline")
            .expect("the re-probe seam receiver remains alive");
        tokio::time::timeout_at(deadline, barrier.as_mut())
            .await
            .expect("the barrier verdict arrived within the test deadline")
    });

    assert_eq!(
        RE_PROBE_SUBMITS.load(std::sync::atomic::Ordering::Relaxed),
        1,
        "exactly one re-probe must have been submitted before the verdict"
    );
    assert!(
        matches!(outcome, ReleaseOutcome::Released),
        "a re-probe on a free pool must answer Released, not repeat the first held \
         observation: {outcome:?}"
    );
}

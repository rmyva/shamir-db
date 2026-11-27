//! Two DISTINCT registries in one process must report INDEPENDENT schedule
//! sections (item 3): the section used to be served from a process-global
//! cell, so a schedule-less server reported its neighbour's schedule and
//! stopping one server erased the other's section.

use std::path::Path;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serial_test::serial;
use shamir_db::engine::repo::{BoxRepoFactory, RepoConfig};
use shamir_db::engine::table::TableConfig;
use shamir_db::{OnlineBackupAdminPort, ShamirDb, SystemStoreConfig};
use shamir_private_dir::create_private_dir;
use shamir_types::time::local_minute_of_day;
use tokio_util::sync::CancellationToken;

use crate::boot::config::diagnostics::backup_online::metrics::backup_metrics_snapshot;
use crate::boot::config::diagnostics::backup_online::metrics::ScheduleInfo;
use crate::boot::config::diagnostics::backup_online::metrics::ScheduledLastRun;
use crate::boot::config::diagnostics::backup_online::registry::arm_scheduled_retention_pause;
use crate::boot::config::diagnostics::backup_online::schedule::runner::spawn_for_test;
use crate::boot::config::diagnostics::backup_online::OnlineBackupRegistry;
use crate::config::{BackupRetentionConfig, BackupScheduleConfig, ResolvedSchedule};
use crate::sync::durability::backup::online::progress::BackupStage;
use shamir_types::types::value::QueryValue;

const WAIT: Duration = Duration::from_secs(120);
const OWNER: &str = "two-registry-sections-test";

fn remaining(deadline: Instant) -> Duration {
    deadline.saturating_duration_since(Instant::now()).min(WAIT)
}

/// One small engine + registry pair on its own data dir (fjall locks are per
/// directory, so the two registries never share storage).
async fn registry_fixture(root: &Path) -> (Arc<ShamirDb>, Arc<OnlineBackupRegistry>) {
    let db = ShamirDb::init(SystemStoreConfig::Fjall(root.join("system")))
        .await
        .expect("init engine");
    db.create_db("app").await.expect("create app");
    db.add_repo(
        "app",
        RepoConfig::new("main", BoxRepoFactory::fjall_raw(root.join("repos/main")))
            .add_table(TableConfig::new("rows")),
    )
    .await
    .expect("add repo");
    let db = Arc::new(db);
    let registry = Arc::new(OnlineBackupRegistry::new(
        Arc::clone(&db),
        root.to_path_buf(),
        tokio::runtime::Handle::current(),
    ));
    (db, registry)
}

/// The BackupStatus reply's `schedule` section through the real admin
/// adapter; `None` when the reply carries no section at all.
async fn schedule_interval(port: &OnlineBackupRegistry) -> Option<i64> {
    let value = OnlineBackupAdminPort::status(port)
        .await
        .expect("bounded admin status");
    let QueryValue::Map(reply) = value else {
        panic!("status reply map expected")
    };
    let Some(QueryValue::Map(schedule)) = reply.get("schedule") else {
        return None;
    };
    let Some(QueryValue::Int(interval)) = schedule.get("interval_seconds") else {
        panic!("schedule section without interval_seconds: {schedule:?}")
    };
    Some(*interval)
}

fn schedule_for(destination: &Path, interval: Duration) -> ResolvedSchedule {
    ResolvedSchedule {
        destination: destination.to_path_buf(),
        interval,
        window: None,
        run_on_start: false,
        retention: BackupRetentionConfig::default(),
    }
}

/// (a) While registry A runs a scheduler, schedule-less registry B reports NO
/// schedule section — never A's. (b) Stopping A's scheduler leaves B's own
/// section intact with B's configuration. The install happens synchronously
/// inside `spawn_for_test`, so both reads are immediately deterministic; the
/// only waits are bounded join handshakes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_registries_report_independent_schedule_sections() {
    let a_root = tempfile::tempdir().expect("tempdir A");
    let b_root = tempfile::tempdir().expect("tempdir B");
    let deadline = Instant::now() + WAIT;
    let (_db_a, a) = tokio::time::timeout(WAIT, registry_fixture(a_root.path()))
        .await
        .expect("bounded fixture A");
    let (_db_b, b) = tokio::time::timeout(
        deadline.saturating_duration_since(Instant::now()),
        registry_fixture(b_root.path()),
    )
    .await
    .expect("bounded fixture B");

    // Fresh registry: no section before any scheduler exists.
    assert_eq!(
        schedule_interval(&b).await,
        None,
        "a schedule-less registry must report no schedule section"
    );

    let shutdown_a = CancellationToken::new();
    let task_a = spawn_for_test(
        schedule_for(
            &a_root.path().join("unused-backup"),
            Duration::from_secs(3600),
        ),
        Arc::clone(&a),
        OWNER.into(),
        shutdown_a.clone(),
        Duration::from_secs(3600),
        |_| {},
        || {},
    );
    // B stays schedule-less while A's scheduler is live.
    assert_eq!(
        schedule_interval(&a).await,
        Some(3600),
        "the scheduled registry reports its own schedule"
    );
    assert_eq!(
        schedule_interval(&b).await,
        None,
        "a schedule-less registry must NOT inherit its neighbour's schedule"
    );

    // B now runs its OWN scheduler with a distinct interval.
    let shutdown_b = CancellationToken::new();
    let task_b = spawn_for_test(
        schedule_for(
            &b_root.path().join("unused-backup"),
            Duration::from_secs(7200),
        ),
        Arc::clone(&b),
        OWNER.into(),
        shutdown_b.clone(),
        Duration::from_secs(7200),
        |_| {},
        || {},
    );
    assert_eq!(
        schedule_interval(&b).await,
        Some(7200),
        "each registry reports its own schedule, not the shared neighbour's"
    );

    // Stopping A's scheduler clears only A's section; B's stays intact.
    shutdown_a.cancel();
    tokio::time::timeout(deadline.saturating_duration_since(Instant::now()), task_a)
        .await
        .expect("bounded A exit handshake")
        .expect("A scheduler joins");
    assert_eq!(
        schedule_interval(&a).await,
        None,
        "the stopped registry's schedule section is gone"
    );
    assert_eq!(
        schedule_interval(&b).await,
        Some(7200),
        "stopping one registry's schedule must leave the other's section intact"
    );
    shutdown_b.cancel();
    tokio::time::timeout(deadline.saturating_duration_since(Instant::now()), task_b)
        .await
        .expect("bounded B exit handshake")
        .expect("B scheduler joins");
    assert_eq!(schedule_interval(&b).await, None);
}

/// One bounded condition-variable gate `(set, wake)`.
type Gate = (Mutex<bool>, Condvar);

fn wait_for_gate(gate: &Gate, what: &str) {
    let deadline = Instant::now() + WAIT;
    let mut opened = gate.0.lock().expect("gate lock");
    while !*opened {
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(!remaining.is_zero(), "{what} must happen within the bound");
        let (next, _) = gate.1.wait_timeout(opened, remaining).expect("gate wait");
        opened = next;
    }
}

fn open_gate(gate: &Gate) {
    *gate.0.lock().expect("gate lock") = true;
    gate.1.notify_all();
}

/// A window label that provably contains the CURRENT local minute (one minute
/// of slack on each side, wrapping at midnight), so the real
/// `local_minute_of_day` gate admits the run deterministically.
fn window_label_around_now() -> String {
    let minute = local_minute_of_day();
    let fmt = |m: u32| format!("{:02}:{:02}", m / 60, m % 60);
    format!(
        "{}-{}",
        fmt((minute + 1439) % 1440),
        fmt((minute + 2) % 1440)
    )
}

fn unix_nanos_now() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the wall clock is after the epoch")
        .as_nanos()
}

/// Bounded wait for the schedule cell's last-run record. NO real signal
/// reaches the test here: this test drives the runner through `spawn_for_test`,
/// which owns and drains the run's `finished` oneshot internally (the runner
/// awaits it in its post-loop drain), and the only test-visible callback
/// (`on_terminal`) fires at the COPY outcome — BEFORE retention and the final
/// publication. With no signal, the bounded poll stays; the item-4b test
/// below proves the same ordering through the real `finished` signal.
async fn await_last_run(registry: &OnlineBackupRegistry, deadline: Instant) -> ScheduledLastRun {
    loop {
        if let Some(run) = registry
            .schedule_cell()
            .snapshot()
            .expect("schedule installed")
            .last_run
        {
            return run;
        }
        assert!(
            Instant::now() < deadline,
            "the last-run publication must happen within the bound"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// (item 4a) A REAL scheduled SUCCESS driven by the actual scheduler, through
/// a daily window and the real clock: the last-run record publishes
/// `completed` / `done` with NO failure `reason`, the next-run cell carries
/// the REAL next due time (boot + one full wall-clock interval, the aligned
/// cadence), and a stale generation's next-run write is rejected (the
/// per-registry generation fence, item 3).
///
/// Mutants caught: publishing a failure `reason` on success; publishing a
/// synthetic or boot-relative next-run stamp instead of the aligned
/// wall-clock deadline; and a generation-blind next-run write (`update(None,
/// …)`), which would let the retired scheduler overwrite the live schedule.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scheduled_success_publishes_done_real_next_run_and_rejects_stale_generation() {
    let root = tempfile::tempdir().expect("data root");
    let dest = tempfile::tempdir().expect("destination root");
    let to = dest.path().join("private");
    create_private_dir(&to).expect("private destination");
    let deadline = Instant::now() + WAIT;
    let (_db, registry) = tokio::time::timeout(WAIT, registry_fixture(root.path()))
        .await
        .expect("bounded fixture");

    let interval_secs = 3_600u64;
    let resolve = |run_on_start, window: String| {
        BackupScheduleConfig {
            destination: to.clone(),
            interval: format!("{interval_secs}s"),
            window: Some(window),
            run_on_start,
            retention: BackupRetentionConfig::default(),
        }
        .resolve(root.path())
        .expect("resolve schedule")
    };
    let boot_unix_ns = unix_nanos_now();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let shutdown = CancellationToken::new();
    let task = spawn_for_test(
        resolve(true, window_label_around_now()),
        Arc::clone(&registry),
        OWNER.into(),
        shutdown.clone(),
        Duration::from_secs(interval_secs),
        move |ok| {
            let _ = tx.send(ok);
        },
        || {},
    );
    let completed = tokio::time::timeout(remaining(deadline), rx.recv())
        .await
        .expect("bounded terminal signal")
        .expect("terminal channel");
    assert!(
        completed,
        "the scheduled run's copy outcome must be a success"
    );

    // The final publication (retention + last-run record) comes after the
    // terminal handshake.
    let last = await_last_run(&registry, deadline).await;
    assert_eq!(
        last.result, "completed",
        "a success is never failed/cancelled"
    );
    assert_eq!(last.stage, "done", "the typed outcome stage of a success");
    assert_eq!(last.reason, None, "a success carries NO failure reason");
    let status = registry.status();
    assert!(!status.running, "the run is fully over");
    assert_eq!(status.stage, BackupStage::Done);
    assert_eq!(status.last_error, None, "no failure error on success");
    assert_eq!(last.started_at_unix_ns, status.started_at_unix_ns);

    // The REAL next due time: the run started at boot (`run_on_start`), so
    // the aligned cadence puts the next tick one full interval after boot.
    let next = registry
        .schedule_cell()
        .snapshot()
        .expect("schedule installed")
        .next_run_unix_ns
        .expect("the live scheduler published its next due time");
    let expected = boot_unix_ns + u128::from(interval_secs) * 1_000_000_000;
    let slack = 5_000_000_000u128;
    assert!(
        next + slack > expected && next < expected + slack,
        "next_run must be the real wall-clock deadline boot+{interval_secs}s \
         (expected ~{expected}, got {next})"
    );
    assert_eq!(
        registry
            .schedule_cell()
            .snapshot()
            .unwrap()
            .info
            .interval_secs,
        interval_secs
    );

    // Stale generation rejected: install a NEW generation (fresh registry →
    // gen 1 was the run's scheduler, gen 2 is this one), wait for its own
    // next-run publication, then replay gen 1's write.
    let shutdown2 = CancellationToken::new();
    let task2 = spawn_for_test(
        resolve(false, window_label_around_now()),
        Arc::clone(&registry),
        OWNER.into(),
        shutdown2.clone(),
        Duration::from_secs(interval_secs),
        |_| {},
        || {},
    );
    let cell = registry.schedule_cell();
    // NO signal exists for this wait: the next-run cell is published by the
    // runner's `publish_next` closure, whose only test-visible counterpart is
    // the cell itself (the run's signals stay inside the runner), so a bounded
    // poll is the only observer.
    loop {
        if cell
            .snapshot()
            .expect("schedule installed")
            .next_run_unix_ns
            .is_some()
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "generation 2's next-run publication within bound"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let live = cell
        .snapshot()
        .expect("schedule installed")
        .next_run_unix_ns;
    cell.note_next_run_unix_ns(1, 123_456);
    assert_eq!(
        cell.snapshot()
            .expect("schedule installed")
            .next_run_unix_ns,
        live,
        "a stale generation's next-run write must be rejected"
    );
    cell.note_next_run_unix_ns(2, live.unwrap().saturating_sub(1));
    assert_ne!(
        cell.snapshot()
            .expect("schedule installed")
            .next_run_unix_ns,
        live,
        "the live generation's write applies"
    );

    shutdown.cancel();
    shutdown2.cancel();
    tokio::time::timeout(remaining(deadline), task)
        .await
        .expect("bounded scheduler 1 exit")
        .expect("scheduler 1 joins");
    tokio::time::timeout(remaining(deadline), task2)
        .await
        .expect("bounded scheduler 2 exit")
        .expect("scheduler 2 joins");
    assert_eq!(schedule_interval(&registry).await, None);
}

/// (item 4b) The COPY outcome publishes BEFORE retention and the final record
/// AFTER it: the real runner is PARKED at the start of its retention pass by
/// the per-run one-shot thread-local pause seam (armed from the per-run
/// post-run callback — consumed once, never global), and the copy outcome is
/// already delivered and the record still `running` while parked. Releasing
/// the park completes retention and the final publication and resolves the
/// run's LATE `finished` signal.
///
/// Mutants caught: moving the copy publication after retention (the outcome
/// await cannot complete while parked); publishing `running = false` (or the
/// last-run record) before retention (the `running` assertion below fails).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn copy_outcome_publishes_before_retention_and_final_record_after_it() {
    let root = tempfile::tempdir().expect("data root");
    let dest = tempfile::tempdir().expect("destination root");
    let to = dest.path().join("private");
    create_private_dir(&to).expect("private destination");
    // The scheduled preflight (destination.rs `scheduled_preflight`) requires
    // `to.canonicalize() == to`; canonicalize exactly as the other
    // destination tests do (schedule/tests/registry.rs:202 et al.).
    let to = to.canonicalize().expect("canonical destination");
    let deadline = Instant::now() + WAIT;
    let (_db, registry) = tokio::time::timeout(WAIT, registry_fixture(root.path()))
        .await
        .expect("bounded fixture");

    // The scheduler task normally installs the registry's schedule cell
    // (`spawn_with_observer` calls `install_schedule_info` before spawning);
    // this test drives `start_scheduled` DIRECTLY, so it installs the cell
    // itself, mirroring the runner's install for a window-less schedule.
    // Fresh registry, so the run's final publication (which lands through
    // `note_schedule_last_run`) finds the cell installed. Gen 1 asserted:
    // the runner passes this generation as the owner of next-run writes
    // (this test writes no next-run, so holding it is enough).
    let _generation = registry
        .schedule_cell()
        .install_schedule_info(ScheduleInfo {
            interval_secs: 3_600,
            window: None,
            keep_last: BackupRetentionConfig::default().keep_last,
            run_on_start: false,
        });
    assert_eq!(_generation, 1, "the fresh registry's first generation");

    let parked_gate = Arc::new((Mutex::new(false), Condvar::new()));
    let release_gate = Arc::new((Mutex::new(false), Condvar::new()));
    let parked_at = Arc::clone(&parked_gate);
    let release_after = Arc::clone(&release_gate);
    registry.arm_post_run_callback_for_shutdown_test(move |_published: &Path| {
        // Runs ON THE RUNNER THREAD, after the copy and before retention:
        // arm the one-shot pause there (the pause seam is thread-local).
        let parked = Arc::clone(&parked_at);
        let release = Arc::clone(&release_after);
        let pause = arm_scheduled_retention_pause(move || {
            open_gate(&parked);
            wait_for_gate(&release, "the test's release of the parked retention");
        });
        // The pause fires in THIS run's retention pass; the zero-sized guard
        // would only clear an unfired install.
        std::mem::forget(pause);
        None
    });
    let mut signals = registry
        .start_scheduled(to, OWNER.to_owned(), BackupRetentionConfig::default())
        .expect("admit scheduled run");
    wait_for_gate(&parked_gate, "the retention park");

    // BEFORE retention: the copy outcome is published while the runner is
    // parked inside the retention pass.
    let completed = tokio::time::timeout(remaining(deadline), signals.outcome)
        .await
        .expect("the copy outcome is published before retention")
        .expect("the runner reports its copy outcome");
    assert!(completed, "the copy itself succeeded");
    assert!(
        registry.status().running,
        "the final publication has NOT happened while retention is parked"
    );
    // (c1) While parked BEFORE the final publication, the schedule cell's
    // last-run record is still ABSENT (it publishes only at the final
    // publication) ...
    assert!(
        registry
            .schedule_cell()
            .snapshot()
            .expect("schedule installed")
            .last_run
            .is_none(),
        "no last-run record may exist before the final publication"
    );
    // (c2) ... and the run's LATE `finished` signal is STILL PENDING. A
    // zero-timeout poll on the receiver proves pendingness WITHOUT consuming
    // it: `timeout` losing the race leaves `&mut signals.finished` untouched
    // (oneshot receives are cancel-safe), so the real await below still
    // succeeds. If `finished` were already sent, this poll would return Ok.
    assert!(
        tokio::time::timeout(Duration::ZERO, &mut signals.finished)
            .await
            .is_err(),
        "the late finished signal must still be pending while retention is parked"
    );

    // AFTER retention: release the park; retention and the final publication
    // complete and resolve the late `finished` signal.
    open_gate(&release_gate);
    tokio::time::timeout(remaining(deadline), &mut signals.finished)
        .await
        .expect("the finished signal resolves within the bound")
        .expect(
            "the runner must SEND `finished`: a Closed receiver means the \
             runner panicked before its post-drain",
        );
    let status = registry.status();
    assert!(!status.running, "the final publication releases admission");
    assert_eq!(status.stage, BackupStage::Done);
    assert_eq!(status.last_error, None);
    // (c3) After release, the last-run record is PRESENT with the expected
    // values — proving the record only ever appears after retention.
    let last = registry
        .schedule_cell()
        .snapshot()
        .expect("schedule installed")
        .last_run
        .expect("the final publication must install the last-run record");
    assert_eq!(last.result, "completed");
    assert_eq!(last.stage, "done");
    assert_eq!(last.reason, None);
    assert_eq!(last.started_at_unix_ns, status.started_at_unix_ns);
}

/// (item 4c) Deterministic scheduler-task abort BEFORE its first poll: on the
/// current-thread runtime the spawned task is polled only when the test
/// awaits, so aborting before the first await provably drops the never-polled
/// future — and its exit guard still clears the section.
///
/// Mutant caught: a guard that clears only from the task BODY (never runs
/// here) — the section would survive the abort.
#[tokio::test]
async fn abort_before_first_poll_clears_the_schedule_section() {
    let root = tempfile::tempdir().expect("data root");
    let (_db, registry) = registry_fixture(root.path()).await;
    let shutdown = CancellationToken::new();
    let task = spawn_for_test(
        ResolvedSchedule {
            destination: root.path().join("unused-backup"),
            interval: Duration::from_secs(3600),
            window: None,
            run_on_start: false,
            retention: BackupRetentionConfig::default(),
        },
        Arc::clone(&registry),
        OWNER.into(),
        shutdown.clone(),
        Duration::from_secs(3600),
        |_| {},
        || {},
    );
    // The install is SYNCHRONOUS inside `spawn_for_test`: the section must be
    // present BEFORE the abort, proving the abort (not a failed install)
    // clears it below.
    assert_eq!(
        schedule_interval(&registry).await,
        Some(3600),
        "the section is installed immediately after spawn, before the abort"
    );
    // No await between spawn and abort: the task cannot have been polled.
    task.abort();
    let joined = tokio::time::timeout(Duration::from_secs(120), task)
        .await
        .expect("bounded abort join handshake")
        .expect_err("abort must cancel the task");
    assert!(joined.is_cancelled());
    assert_eq!(
        schedule_interval(&registry).await,
        None,
        "the never-polled future's exit guard must still clear the section"
    );
}

/// (item 4c) Deterministic abort AFTER the first poll: the terminal callback
/// fires only once the runner loop has polled past its first tick (the run
/// was admitted and failed fast on an invalid destination), proving the body
/// ran; aborting afterwards still clears the section through the guard.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn abort_after_first_poll_clears_the_schedule_section() {
    let root = tempfile::tempdir().expect("data root");
    let deadline = Instant::now() + WAIT;
    let (_db, registry) = tokio::time::timeout(WAIT, registry_fixture(root.path()))
        .await
        .expect("bounded fixture");
    let shutdown = CancellationToken::new();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let task = spawn_for_test(
        ResolvedSchedule {
            // Never created: the scheduled preflight fails fast.
            destination: root.path().join("missing-private"),
            interval: Duration::from_secs(3600),
            window: None,
            run_on_start: true,
            retention: BackupRetentionConfig::default(),
        },
        Arc::clone(&registry),
        OWNER.into(),
        shutdown.clone(),
        Duration::from_secs(3600),
        move |ok| {
            let _ = tx.send(ok);
        },
        || {},
    );
    let ok = tokio::time::timeout(remaining(deadline), rx.recv())
        .await
        .expect("bounded terminal signal")
        .expect("terminal channel");
    assert!(!ok, "the first poll admitted the run and it failed fast");
    assert!(
        schedule_interval(&registry).await.is_some(),
        "the section is installed while the scheduler is live"
    );
    task.abort();
    let joined = tokio::time::timeout(remaining(deadline), task)
        .await
        .expect("abort join handshake");
    assert!(joined
        .expect_err("abort must cancel the task")
        .is_cancelled());
    assert_eq!(
        schedule_interval(&registry).await,
        None,
        "aborting a polled scheduler still clears the section"
    );
}

/// A window label that provably EXCLUDES the current local minute: a
/// one-minute window ten hours ahead, so neither now nor any nearby minute is
/// admitted even if the wall clock steps a minute during the test.
fn window_label_excluding_now() -> String {
    let minute = local_minute_of_day();
    let fmt = |m: u32| format!("{:02}:{:02}", m / 60, m % 60);
    format!(
        "{}-{}",
        fmt((minute + 600) % 1440),
        fmt((minute + 601) % 1440)
    )
}

/// (item 4a NEGATIVE CONTROL) A scheduler whose daily window EXCLUDES the
/// current local minute must NOT admit a run: driven under tokio's PAUSED
/// clock, two full scheduled ticks fire within virtual hours while the REAL
/// wall-clock minute stays outside the window, and both ticks are recorded as
/// `scheduled_skipped_window` with ZERO `scheduled_started` — the gate
/// discriminates (the success test above proves the inclusive side on the
/// real clock; this test proves the exclusive side).
///
/// Why a paused clock instead of a ≥120 s real-clock observation: under
/// `start_paused` the runner's real `sleep_until` deadlines fire as soon as
/// virtual time advances, so TWO genuine gate evaluations (boot tick + first
/// aligned tick) are observed deterministically in milliseconds — a strictly
/// stronger observation than one 120 s real-clock window that contains zero
/// scheduled ticks, without a two-minute wall wait per run.
///
/// Mutants caught: removing the window gate (the boot tick would ADMIT a run
/// instead of skipping it); inverting `window.contains`; skipping without
/// accounting (`SkippedWindow` not noted).
#[tokio::test(start_paused = true)]
#[serial]
async fn window_excluding_now_skips_both_ticks_and_never_admits_a_run() {
    let root = tempfile::tempdir().expect("data root");
    let dest = tempfile::tempdir().expect("destination root");
    let to = dest.path().join("private");
    create_private_dir(&to).expect("private destination");
    let (_db, registry) = registry_fixture(root.path()).await;

    // The run counters are process-global metrics: capture the baseline
    // BEFORE the scheduler exists.
    let baseline = backup_metrics_snapshot();
    let interval_secs = 3_600u64;
    let resolve = |run_on_start| {
        BackupScheduleConfig {
            destination: to.clone(),
            interval: format!("{interval_secs}s"),
            window: Some(window_label_excluding_now()),
            run_on_start,
            retention: BackupRetentionConfig::default(),
        }
        .resolve(root.path())
        .expect("resolve schedule")
    };
    let shutdown = CancellationToken::new();
    let task = spawn_for_test(
        resolve(true),
        Arc::clone(&registry),
        OWNER.into(),
        shutdown.clone(),
        Duration::from_secs(interval_secs),
        |_| {},
        || {},
    );

    // Two full scheduled ticks across virtual hours: the boot tick and the
    // first aligned tick (boot + one interval) both land OUTSIDE the window.
    tokio::time::sleep(Duration::from_secs(2 * interval_secs + 60)).await;

    let after = backup_metrics_snapshot();
    assert_eq!(
        after.scheduled_runs_total[0] - baseline.scheduled_runs_total[0],
        0,
        "an excluded window must NEVER admit a scheduled run"
    );
    assert!(
        after.scheduled_runs_total[2] - baseline.scheduled_runs_total[2] >= 2,
        "both ticks must be gated out as skipped-window \
         (baseline {}, after {})",
        baseline.scheduled_runs_total[2],
        after.scheduled_runs_total[2]
    );
    assert!(
        registry
            .schedule_cell()
            .snapshot()
            .expect("schedule installed")
            .last_run
            .is_none(),
        "no run happened, so no last-run record exists"
    );
    assert!(!registry.status().running);

    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(120), task)
        .await
        .expect("bounded scheduler exit")
        .expect("scheduler joins");
}

/// (finding 4e) The runner's next-run publish path must keep u128 ALL THE WAY
/// into the schedule cell: an interval above ~530 years makes the real next
/// due (wall-clock now + interval) EXCEED u64::MAX nanoseconds, so the old
/// closure's `.min(u64::MAX) as u64` published u64::MAX instead of the true
/// stamp. The huge value is published at the runner's FIRST POLL
/// (`run_on_start = false` publishes `boot + interval` before any tick), so
/// no run is admitted and no timer elapses. The interval alone (~550 years =
/// 550*365*86_400 s = 1.73448e19 ns) stays just under u64::MAX
/// (1.8446744e19 ns); `Instant::checked_add` of such an interval is expected
/// to succeed on the CI platforms (fitting in u64 nanoseconds is not a proof
/// for every platform). The published stamp is `boot_unix_ns + interval_ns`,
/// which exceeds u64::MAX as soon as the epoch is OLDER than
/// u64::MAX − interval_ns = 1.1019441e18 ns ≈ 34.9 years — the case since
/// late 2004 — and the epoch only ages, so the crossing holds permanently
/// (today the epoch sits ~1.79e18 ns in, giving ~1.91e19 ns).
///
/// Mutant: "clamping at the cell instead of the adapter" — restoring the
/// pre-store clamp in `runner.rs` makes the cell carry `u64::MAX`, and the
/// `> u64::MAX` assertion below fails. (A cell-level write of a huge u128
/// does NOT kill this mutant — this test goes through
/// `spawn_for_test`'s real publish closure.)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn huge_interval_next_run_survives_u128_to_the_cell_unclamped() {
    // ~550 years: nanos = 550*365*86_400 s = 1.73448e19 ns, just under
    // u64::MAX (1.8446744e19), so `Instant::checked_add` is expected to
    // succeed; the epoch stamp added on top (~1.79e18 ns) pushes the
    // PUBLISHED value beyond u64::MAX.
    let interval_secs = 550u64 * 365 * 86_400;
    let interval = Duration::from_secs(interval_secs);
    let interval_nanos = interval.as_nanos();
    assert!(
        interval_nanos <= u128::from(u64::MAX),
        "the interval must stay within the representable Instant range"
    );
    let root = tempfile::tempdir().expect("data root");
    let deadline = Instant::now() + WAIT;
    let (_db, registry) = tokio::time::timeout(WAIT, registry_fixture(root.path()))
        .await
        .expect("bounded fixture");
    let before = unix_nanos_now();
    let shutdown = CancellationToken::new();
    let task = spawn_for_test(
        ResolvedSchedule {
            destination: root.path().join("unused-backup"),
            interval,
            window: None,
            run_on_start: false,
            retention: BackupRetentionConfig::default(),
        },
        Arc::clone(&registry),
        OWNER.into(),
        shutdown.clone(),
        interval,
        |_| {},
        || {},
    );

    // The publication happens at the runner's first poll. NO signal exists
    // for it (the publish closure's only observable is the cell itself), so
    // a bounded poll is the only observer.
    let cell = registry.schedule_cell();
    let next = loop {
        if let Some(next) = cell
            .snapshot()
            .expect("schedule installed")
            .next_run_unix_ns
        {
            break next;
        }
        assert!(
            Instant::now() < deadline,
            "the next-run publication must happen within the bound"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };

    // THE clamp killer: the real stamp is beyond u64, so any pre-store clamp
    // publishes u64::MAX and this assertion fails.
    assert!(
        next > u128::from(u64::MAX),
        "the cell must carry the UNCLAMPED u128 next due (got {next}, \
         u64::MAX = {})",
        u64::MAX
    );
    let after = unix_nanos_now();
    assert!(
        next >= before + interval_nanos && next <= after + interval_nanos,
        "the stamp must be the REAL wall-clock boot+interval \
         (expected in [{}, {}], got {next})",
        before + interval_nanos,
        after + interval_nanos
    );

    shutdown.cancel();
    tokio::time::timeout(deadline.saturating_duration_since(Instant::now()), task)
        .await
        .expect("bounded scheduler exit")
        .expect("scheduler joins");
    assert_eq!(
        schedule_interval(&registry).await,
        None,
        "the exit guard clears the section"
    );
}

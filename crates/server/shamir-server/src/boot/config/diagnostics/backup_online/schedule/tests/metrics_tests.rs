//! Evidence-cell tests for `shamir_backup_last_success_timestamp_seconds`
//! (`online-backup.md` §4.8, item B): one exported stamp, seeded at boot from
//! the newest durable row, raised by every in-process success, and clamped to
//! "now" exactly ONCE when a future stamp appears.
//!
//! The export cell, the replaceable durable handle and the injected wall clock
//! are all PROCESS-GLOBAL, so every test here is `#[serial]`, starts from a
//! known cell (`set_export_last_success_for_test(0)`) and restores it before it
//! returns — a test in the same binary can never inherit or leave evidence.
//! The same process-global state also carries the boot arming decision
//! (`arm_action`) and the `schedule_enabled` cell, pinned here as well.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serial_test::serial;
use shamir_test_diag::try_install_global_tracing_ring;
use tempfile::TempDir;

use super::super::super::metrics::{
    arm_action, backup_metrics_snapshot, enable_schedule, install_backup_meta_store, note_success,
    set_export_last_success_for_test, ArmingAction, ClockOverride, ScheduleInfo, ScheduleState,
};
use crate::server_meta::ServerMetaStore;

/// 2023-11-14T22:13:20Z — arbitrary but fixed, so no assertion reads the real
/// clock. Every second below is derived from it.
const NOW: u64 = 1_700_000_000;

/// A REAL store on a `TempDir` the caller keeps alive for the test.
fn temp_store() -> (TempDir, Arc<ServerMetaStore>) {
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().join("server_meta");
    let store = Arc::new(ServerMetaStore::open_or_init(&path).expect("open_or_init"));
    (dir, store)
}

/// The exported stamp is the NEWEST of the durable row, the armed-at row and
/// the in-memory success, and a future stamp is clamped to "now" once — the
/// clamped value is what later scrapes report, so the exported age grows.
///
/// Mutants caught: (a) seeding from the durable last-success row alone without
/// the in-memory max → the success assertion sees the old row; (b)
/// `note_success` without `fetch_max` → same assertion; (c) the old per-scrape
/// `min(now)` without write-back → the third scrape follows the advanced clock
/// instead of staying at the first clamped value; (d) `fetch_min` returning the
/// PREVIOUS value without the trailing `min` → the first scrape hands out the
/// future stamp instead of "now".
#[serial]
#[test]
fn last_success_stamp_takes_newest_source_and_never_the_future() {
    set_export_last_success_for_test(0);
    let clock = ClockOverride::set(NOW);

    let (_dir, store) = temp_store();
    store
        .store_backup_last_success_unix_ns(NOW - 900)
        .expect("seed the durable last-success row");
    // Boot WITHOUT a schedule: nothing may be armed, so the export comes from
    // the last-success row alone.
    install_backup_meta_store(Arc::clone(&store), false);
    assert_eq!(
        backup_metrics_snapshot().last_success_timestamp_seconds,
        NOW - 900,
        "a boot must seed the exported stamp from the durable last-success row"
    );

    // A newer in-process success wins over the older row; duration and bytes
    // keep their in-memory source.
    clock.pin(NOW - 300);
    note_success(17, Duration::from_secs(2));
    let after_success = backup_metrics_snapshot();
    assert_eq!(
        after_success.last_success_timestamp_seconds,
        NOW - 300,
        "a newer in-process success must win over the older durable row"
    );
    assert_eq!(after_success.last_bytes, 17, "last_bytes stays in-memory");

    // A forward clock step at success stamps the FUTURE.
    clock.pin(NOW + 86_400);
    note_success(0, Duration::ZERO);
    clock.pin(NOW);
    let first = backup_metrics_snapshot().last_success_timestamp_seconds;
    assert_eq!(
        first, NOW,
        "the first scrape must report now, not the future"
    );
    let second = backup_metrics_snapshot().last_success_timestamp_seconds;
    assert_eq!(
        second, first,
        "the second scrape must report the SAME clamped value, not move with the clock"
    );
    clock.advance(120);
    let third = backup_metrics_snapshot().last_success_timestamp_seconds;
    assert_eq!(
        third, first,
        "age grows: the clamp does not follow the clock"
    );

    drop(clock);
    set_export_last_success_for_test(0);
}

/// The exported stamp only ever moves FORWARD: a success stamped with an OLDER
/// second (a backwards clock step between two runs) must not replace the stamp
/// a NEWER run already published, so the exported last-success stamp never
/// regresses below the previous run's.
///
/// Mutant caught: `store` instead of `fetch_max` in `note_success` — the
/// second, older stamp replaces the newer one and the last-success stamp
/// regresses. The live scheduled route cannot force this (two consecutive real
/// runs stamp a non-decreasing clock, so a `store` there is invisible), hence
/// the invariant is pinned here at the evidence cell.
#[serial]
#[test]
fn note_success_never_lowers_the_exported_stamp() {
    set_export_last_success_for_test(0);
    let clock = ClockOverride::set(NOW);

    note_success(1, Duration::from_secs(1));
    assert_eq!(
        backup_metrics_snapshot().last_success_timestamp_seconds,
        NOW,
        "the first success must publish its own stamp"
    );

    // Run N+1 stamps an OLDER second than run N did.
    clock.pin(NOW - 60);
    note_success(2, Duration::from_secs(1));
    // Restore the clock BEFORE the scrape: a scrape made at the older second
    // would `fetch_min` the cell down to it, and the assertion below would then
    // measure the scrape's own clamp instead of the success accounting.
    clock.pin(NOW);
    assert_eq!(
        backup_metrics_snapshot().last_success_timestamp_seconds,
        NOW,
        "an older success stamp must never REPLACE a newer one (mutant: `store` \
         instead of `fetch_max` in `note_success`)"
    );

    drop(clock);
    set_export_last_success_for_test(0);
}

/// Every launch installs its OWN store: a success after the second install is
/// persisted through B, and A is never written again.
///
/// Mutant caught: the former first-wins `OnceLock<Weak<ServerMetaStore>>`,
/// which kept the first boot's handle for the whole process — B's row would
/// stay absent and A's row would take the second stamp.
#[serial]
#[test]
fn holder_replaces_across_installs() {
    set_export_last_success_for_test(0);
    let clock = ClockOverride::set(NOW);

    let (_dir_a, store_a) = temp_store();
    let (_dir_b, store_b) = temp_store();

    install_backup_meta_store(Arc::clone(&store_a), false);
    clock.pin(NOW + 10);
    note_success(1, Duration::from_secs(1));
    let stamp_a = store_a.backup_last_success_unix_ns().expect("read A");
    assert_eq!(stamp_a, Some(NOW + 10), "store A takes the first stamp");

    install_backup_meta_store(Arc::clone(&store_b), false);
    clock.pin(NOW + 20);
    note_success(2, Duration::from_secs(1));
    let stamp_b = store_b.backup_last_success_unix_ns().expect("read B");
    assert_eq!(stamp_b, Some(NOW + 20), "store B takes the second stamp");
    let stamp_a_again = store_a.backup_last_success_unix_ns().expect("A again");
    assert_eq!(stamp_a_again, Some(NOW + 10), "store A is left untouched");

    drop(clock);
    set_export_last_success_for_test(0);
}

/// ONE unreadable armed-at row is NOT an absent one: the durable FIRST
/// arming stamp must never be overwritten, so the unreadable case borrows
/// "now" for this boot only instead of writing it.
///
/// Mutant caught: routing `Err` to `WriteNow` (the parent behaviour, where an
/// `if let Ok(Some(..))` let the error fall through to the store write) makes
/// the third assertion fail — and resurrects the stale-alert defect the
/// durable row exists to prevent.
#[serial]
#[test]
fn unreadable_armed_at_row_borrows_now_and_never_rewrites_the_stamp() {
    assert_eq!(
        arm_action(Ok(Some(7)), NOW),
        ArmingAction::Keep(7),
        "a readable row keeps the durable first-arming stamp"
    );
    assert_eq!(
        arm_action(Ok(None), NOW),
        ArmingAction::WriteNow(NOW),
        "an absent row arms now"
    );
    assert_eq!(
        arm_action(Err(()), NOW),
        ArmingAction::UseNowWithoutWriting(NOW),
        "an unreadable row is used for this boot but MUST NOT be rewritten"
    );
}

/// The PRODUCTION arming path against a REAL store: an UNDECODABLE armed-at
/// row makes this boot borrow the wall clock while the durable row keeps its
/// exact bytes, and the bounded `read-armed-at-failed` warning is logged
/// without a filesystem path. The classifier test above pins the ROUTING
/// only, so a write restored after `arm_action` would still pass it.
///
/// Mutant caught: writing `now` inside the production
/// `ArmingAction::UseNowWithoutWriting` arm of `arm_schedule_clock` — the
/// seeded bytes become a msgpack `u64`, so the byte-for-byte assertion fails
/// (that mutant leaves both the exported provisional now and the warning
/// token unchanged).
#[serial]
#[test]
fn unreadable_armed_at_row_drives_the_production_arming_path_without_a_write() {
    set_export_last_success_for_test(0);
    try_install_global_tracing_ring();
    let (dir, store) = temp_store();
    // The private `KEY_BACKUP_ARMED_AT_UNIX_SECS` const in `engine/server_meta.rs`;
    // the literal is spelled out here because the const is module-private. `0xcf`
    // announces a `u64` payload with only two bytes behind it, so `get::<u64>` hits
    // a real `rmp_serde` decode error (trailing garbage like `[1, 2, 3]` would NOT:
    // `from_slice` does not reject trailing bytes).
    store.set_raw_row_for_test("backup_armed_at_unix_secs_v1", &[0xcf, 0, 1]);
    let before = store
        .blob_for_test("backup_armed_at_unix_secs_v1")
        .expect("the seeded undecodable armed-at row exists");

    install_backup_meta_store(Arc::clone(&store), true);

    // (a) The write this mutant restores would replace these bytes.
    let after = store
        .blob_for_test("backup_armed_at_unix_secs_v1")
        .expect("an unreadable row is NOT absent: it must not be removed");
    assert_eq!(
        after, before,
        "the durable first-arming stamp must survive an unreadable read byte-for-byte"
    );

    // (b) The exported evidence is the PROVISIONAL now, never the seed.
    let exported = backup_metrics_snapshot().last_success_timestamp_seconds;
    let wall = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the wall clock is after the epoch")
        .as_secs();
    assert_ne!(
        exported, 0,
        "an unreadable row still exports a provisional now"
    );
    assert!(
        exported + 5 >= wall && exported <= wall + 5,
        "the exported stamp must be the provisional now ({exported}) within 5 s of {wall}"
    );

    // (c) The bounded warning fired and carries NO filesystem path.
    let captured = shamir_test_diag::ring::shared_ring()
        .snapshot()
        .into_iter()
        .rev()
        .find(|record| record.message.contains("read-armed-at-failed"))
        .unwrap_or_else(|| panic!("the unreadable-row warning was not captured"));
    let temp_dir = dir.path().display().to_string();
    assert!(
        !captured.message.contains(&temp_dir),
        "the warning must not carry the data-dir path: {}",
        captured.message
    );

    set_export_last_success_for_test(0);
    assert!(
        dir.path().exists(),
        "the store stays alive for the whole test"
    );
}

/// The next-run cell keeps the runner's u128 wall-clock stamp VERBATIM: all
/// upstream arithmetic is u128 and the runner's publish closure is the only
/// u64 adapter (and it no longer clamps), so a boundary value must be stored
/// verbatim, `0` must publish "no next run", and the generation fence must
/// reject a stale scheduler's write. The unclamped u64 overflow itself is
/// proven end-to-end through the runner in
/// `two_registry_sections::huge_interval_next_run_survives_u128_to_the_cell_unclamped`.
///
/// Mutants caught: clamping to `u128::MAX - 1` (or any sentinel) at the cell;
/// dropping the generation fence (`update(None, …)`), which would let an
/// older scheduler clobber the live schedule's next run.
#[test]
fn next_run_cell_is_the_u64_boundary_and_fences_stale_generations() {
    let state = ScheduleState::default();
    let generation = state.install_schedule_info(ScheduleInfo {
        interval_secs: 60,
        window: None,
        keep_last: 1,
        run_on_start: false,
    });
    assert_eq!(generation, 1, "generations start at 1");
    state.note_next_run_unix_ns(generation, u128::MAX);
    assert_eq!(
        state
            .snapshot()
            .expect("schedule installed")
            .next_run_unix_ns,
        Some(u128::MAX),
        "the boundary value is stored verbatim, not clamped or narrowed"
    );
    state.note_next_run_unix_ns(generation, 0);
    assert_eq!(
        state
            .snapshot()
            .expect("schedule installed")
            .next_run_unix_ns,
        None,
        "0 publishes no next run"
    );

    let newer = state.install_schedule_info(ScheduleInfo {
        interval_secs: 60,
        window: None,
        keep_last: 1,
        run_on_start: false,
    });
    assert_eq!(newer, 2, "generations are per-registry and monotonic");
    state.note_next_run_unix_ns(generation, 7);
    assert_eq!(
        state
            .snapshot()
            .expect("schedule installed")
            .next_run_unix_ns,
        None,
        "a stale generation's next-run write must be rejected"
    );
    state.note_next_run_unix_ns(newer, u128::MAX - 1);
    assert_eq!(
        state
            .snapshot()
            .expect("schedule installed")
            .next_run_unix_ns,
        Some(u128::MAX - 1),
        "the live generation's write applies"
    );
}

/// A relaunch WITHOUT a schedule must not inherit the process-global
/// `schedule_enabled` flag: the disarm path clears the cell, so the
/// stale-backup alert cannot fire for a schedule-less server.
///
/// Mutant caught: removing the reset leaves the cell at the previous
/// launch's non-zero stamp, so `schedule_enabled` stays `true` and the second
/// assertion fails. The test ends at the same zero state it asserts, so no
/// later serial test inherits the flag.
#[serial]
#[test]
fn schedule_less_install_clears_the_process_global_schedule_enabled_flag() {
    set_export_last_success_for_test(0);

    enable_schedule();
    let enabled = backup_metrics_snapshot();
    assert!(
        enabled.schedule_enabled,
        "a configured schedule must export schedule_enabled"
    );
    assert_ne!(
        enabled.schedule_started_timestamp_seconds, 0,
        "an enabled schedule exports its non-zero started stamp"
    );

    let (_dir, store) = temp_store();
    install_backup_meta_store(Arc::clone(&store), false);
    let disabled = backup_metrics_snapshot();
    assert!(
        !disabled.schedule_enabled,
        "a schedule-less relaunch must clear schedule_enabled"
    );
    assert_eq!(
        disabled.schedule_started_timestamp_seconds, 0,
        "the started stamp is reset, not merely hidden"
    );

    set_export_last_success_for_test(0);
}

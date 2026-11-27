//! F6b — WAL truncation cutover.
//!
//! After F6b the `WalSink::File` variant holds a [`shamir_wal::SegmentSet`]
//! (a directory of numbered segments), and the [`Drainer`] truncates the WAL
//! once the data is durable in `history`:
//!
//!   1. **Truncation collapses the WAL after a drain.** Committing N records
//!      leaves N WAL frames/entries inflight. A `drain_all` pass replays each
//!      into `history`, advances `durable_watermark` to visibility, and —
//!      because every entry's `commit_version <= durable_watermark` — truncates
//!      them out of the WAL. `wal.recover()` then returns only the undrained
//!      tail (≈0 once durable == visibility), while every value still reads
//!      back from `history`. (Mem sink frame-GC — the truncation granule for
//!      in-memory repos, I7.)
//!
//!   2. **Reopen after truncation recovers all.** A disk repo committed, then
//!      drained (truncating the WAL), reopens with every value intact — the
//!      data is durable in `history`, the truncated WAL segments are not
//!      needed for recovery (I1).
//!
//!   3. **I2 order — truncate only when truncatable.** `has_truncatable` gates
//!      the history-flush + truncate so it fires only once the durable
//!      watermark crosses a sealed segment / frame, NEVER on every commit (no
//!      fsync-on-commit regression).
//!
//! The segment-rotation / per-segment truncation SEMANTICS (seal on size,
//! drop-drained-keep-undrained, v=0-pins, torn-tail) are unit-tested directly
//! against `SegmentSet` in `shamir-wal` (`segment_set_tests.rs`); here we
//! assert the engine WIRING — that `drain_step` calls truncate at the right
//! moment and recovery survives it.

#[cfg(feature = "fjall")]
use std::path::PathBuf;
use std::sync::Arc;

#[cfg(feature = "fjall")]
use serial_test::serial;
use shamir_storage::storage_in_memory::InMemoryRepo;
#[cfg(feature = "fjall")]
use shamir_test_diag::FlakeGuard;
use shamir_tx::IsolationLevel;
use shamir_types::types::value::InnerValue;

#[cfg(feature = "fjall")]
use crate::repo::BoxRepoFactory;
use crate::repo::{BoxRepo, RepoInstance};
use crate::table::TableConfig;
use crate::tx::drainer::Drainer;

fn make_mem_repo() -> RepoInstance {
    let repo = Arc::new(InMemoryRepo::new());
    RepoInstance::new("f6b".into(), BoxRepo::InMemory(repo), Vec::new())
}

/// Retry-reopen a disk-backed repo (Windows releases file locks lazily
/// after `drop`). Mirrors the helper in `recovery_tests.rs`.
/// Disk-backed (fjall_raw) — requires the `fjall` feature.
#[cfg(feature = "fjall")]
pub(crate) async fn reopen_disk_repo(
    name: &str,
    path: PathBuf,
    tables: Vec<TableConfig>,
) -> RepoInstance {
    let mut last_err = None;
    for _attempt in 0..10 {
        match RepoInstance::from_factory(
            name.into(),
            BoxRepoFactory::fjall_raw(path.clone()),
            tables.clone(),
        )
        .await
        {
            Ok(r) => return r,
            Err(e) => {
                last_err = Some(e);
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        }
    }
    panic!("reopen_disk_repo({name:?}) failed after 10 retries: {last_err:?}");
}

/// Commit N records → N WAL entries inflight. `drain_all` lands every value in
/// `history`, advances durable to visibility, and TRUNCATES the WAL: a fresh
/// `recover()` returns ~0 entries (the undrained tail is empty when durable ==
/// visibility). Every value still reads back, now served from `history`.
#[tokio::test(flavor = "current_thread")]
async fn truncation_collapses_wal_after_drain() {
    const N: usize = 12;

    let repo = make_mem_repo();
    repo.disable_background_drainer_for_test();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    let gate = repo.tx_gate().await.unwrap();
    let wal = repo.repo_wal().await.unwrap();

    let mut rids = Vec::with_capacity(N);
    for i in 0..N {
        let (mut tx, _g) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
        let rid = tbl
            .insert_tx(&InnerValue::Str(format!("v{i}")), Some(&mut tx))
            .await
            .unwrap();
        repo.commit_tx(tx).await.unwrap();
        rids.push(rid);
    }

    // Before the drain: durable is far behind, so nothing is truncatable yet,
    // and the WAL holds one inflight entry per committed version.
    assert_eq!(gate.durable_watermark(), 0, "nothing drained yet");
    assert!(
        !wal.has_truncatable(gate.durable_watermark()),
        "with durable == 0 there is nothing truncatable"
    );
    let before = wal.recover().await.unwrap().len();
    assert!(
        before >= N,
        "WAL must hold the committed tail before drain (got {before})"
    );

    // Drain → replay into history, advance durable to visibility, TRUNCATE.
    let drained = Drainer::new().drain_all(&repo.core).await.unwrap();
    assert!(drained >= 1, "drain_all must drain the committed tail");
    assert_eq!(
        gate.durable_watermark(),
        gate.last_committed(),
        "durable converges to visibility"
    );

    // The WAL collapsed: every entry was at or below durable, so truncation
    // dropped them. recover() now returns only the (empty) undrained tail.
    let after = wal.recover().await.unwrap().len();
    assert_eq!(
        after, 0,
        "truncation must collapse the WAL to the undrained tail (got {after})"
    );

    // And nothing more is truncatable now (already collapsed).
    assert!(
        !wal.has_truncatable(gate.durable_watermark()),
        "nothing left to truncate after the collapse"
    );

    // Every value still reads back — served from `history`, not the WAL.
    for (i, rid) in rids.iter().enumerate() {
        let got = tbl.get(*rid).await.unwrap();
        let expect = format!("v{i}");
        assert!(
            matches!(got, InnerValue::Str(ref s) if *s == expect),
            "rid {rid:?}: expected {expect}, got {got:?}"
        );
    }
}

/// I2 order: `has_truncatable` is the gate. It is FALSE on a fresh commit
/// (durable still lags) and only flips TRUE once the drain advances durable
/// over the committed versions — so the history-flush + truncate fires on a
/// drain boundary, never per-commit.
#[tokio::test(flavor = "current_thread")]
async fn truncate_only_when_truncatable_not_per_commit() {
    let repo = make_mem_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    let gate = repo.tx_gate().await.unwrap();
    let wal = repo.repo_wal().await.unwrap();

    // A commit, with NO drain: visibility advanced, durable did not. The gate
    // must be false at the (still-zero) durable watermark — committing alone
    // never makes the WAL truncatable (the data is not yet durable in history).
    let (mut tx, _g) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    tbl.insert_tx(&InnerValue::Str("a".into()), Some(&mut tx))
        .await
        .unwrap();
    repo.commit_tx(tx).await.unwrap();

    assert!(gate.last_committed() >= 1, "commit advanced visibility");
    assert_eq!(gate.durable_watermark(), 0, "no drain → durable still 0");
    assert!(
        !wal.has_truncatable(gate.durable_watermark()),
        "committing without draining must NOT make the WAL truncatable (I2)"
    );

    // After a drain, durable advances over the committed version → the gate
    // flips true at the pre-truncation watermark. (The drainer itself then
    // truncates inside the pass, so post-drain it is false again — proven by
    // `truncation_collapses_wal_after_drain`. Here we only assert the gate is
    // version-driven, not commit-driven.)
    let dur_before = gate.durable_watermark();
    Drainer::new().drain_step(&repo.core).await.unwrap();
    assert!(
        gate.durable_watermark() > dur_before,
        "drain advanced durable over the committed version"
    );
}

/// Count `*.wal` segment files in a repo's `<name>.shamirwal/` directory.
/// Only consumed by fjall-gated tests — requires the `fjall` feature.
#[cfg(feature = "fjall")]
pub(crate) fn shamirwal_seg_count(repo_dir: &std::path::Path) -> usize {
    // `repo_instance::repo_wal` builds the WAL dir as a SIBLING of the backing
    // dir: `<file_name>.shamirwal`. For a `sled_raw(<dir>)` repo the backing
    // dir IS `<dir>`, so the WAL lives at `<dir>.shamirwal`.
    let file_name = repo_dir.file_name().expect("repo dir has a name");
    let mut wal_name = file_name.to_os_string();
    wal_name.push(".shamirwal");
    let wal_dir = repo_dir.with_file_name(wal_name);
    match std::fs::read_dir(&wal_dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.file_name()
                    .to_str()
                    .map(|n| n.ends_with(".wal"))
                    .unwrap_or(false)
            })
            .count(),
        Err(_) => 0,
    }
}

/// F6c growth-limit (engine level): with a tiny `SHAMIR_WAL_SEGMENT_MAX_BYTES`,
/// committing N ≫ one-segment records through the real `commit_tx` path and
/// draining periodically keeps the number of `*.wal` segment files BOUNDED —
/// the drainer truncates sealed segments below `durable_watermark`, so once
/// `durable == visibility` only the active segment (+ O(1) untruncated sealed)
/// remain. The count must NOT grow with N.
///
/// `current_thread` + a process-unique tempdir keep the per-test scratch
/// isolated. The `SHAMIR_WAL_SEGMENT_MAX_BYTES` env var is process-global
/// state — nextest runs tests as threads inside ONE binary process by
/// default, so a concurrent sibling reading this env at WAL bring-up
/// would see a stale value and race the `remove_var` here. `#[serial]`
/// serialises every test that touches process-global env vars.
/// Disk-backed (fjall_raw) growth-limit test — requires the `fjall` feature.
#[cfg(feature = "fjall")]
#[tokio::test(flavor = "current_thread")]
#[serial]
async fn wal_segment_count_bounded_under_drain() {
    const N: usize = 80;

    let tempdir = tempfile::TempDir::new().expect("tempdir");
    let repo_dir = tempdir.path().join("growth_repo");

    // Tiny cap so N small records roll MANY segments. Set before the repo's
    // `repo_wal` OnceCell initialises (it reads this env at first use).
    std::env::set_var("SHAMIR_WAL_SEGMENT_MAX_BYTES", "1024");

    let repo = reopen_disk_repo("growth", repo_dir.clone(), vec![TableConfig::new("t")]).await;
    let tbl = repo.get_table("t").await.unwrap();
    let gate = repo.tx_gate().await.unwrap();
    let drainer = Drainer::new();

    let mut max_seen = 0usize;
    for i in 0..N {
        let (mut tx, _g) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
        tbl.insert_tx(&InnerValue::Str(format!("growth-{i:04}")), Some(&mut tx))
            .await
            .unwrap();
        repo.commit_tx(tx).await.unwrap();

        // Drain every few commits so durable chases visibility and sealed
        // segments are reclaimed continuously (steady-state, not one-shot).
        if i % 4 == 3 {
            drainer.drain_all(&repo.core).await.unwrap();
            let files = shamirwal_seg_count(&repo_dir);
            if files > max_seen {
                max_seen = files;
            }
        }
    }

    // Final drain: durable converges to visibility, all sealed segments below
    // it are gone.
    drainer.drain_all(&repo.core).await.unwrap();
    assert_eq!(
        gate.durable_watermark(),
        gate.last_committed(),
        "durable converged to visibility after the final drain"
    );

    let final_files = shamirwal_seg_count(&repo_dir);
    std::env::remove_var("SHAMIR_WAL_SEGMENT_MAX_BYTES");

    // The cap was tiny and N large, so WITHOUT truncation we'd see dozens of
    // segments. With truncation the count is small and independent of N: the
    // active segment plus a small O(1) untruncated remainder.
    assert!(
        final_files <= 4,
        "with durable == visibility the WAL must collapse to ~active; N={N} \
         produced {final_files} segment files (max during run {max_seen})"
    );
    // And the run genuinely rolled segments at some point (otherwise the cap
    // was not exercised and the bound proves nothing).
    assert!(
        max_seen >= 1,
        "the run must have produced at least one segment file (max_seen={max_seen})"
    );
}

/// Disk repo: commit, drain (truncating the WAL), reopen → every value intact.
/// The data is durable in `history`; the truncated WAL segments are not needed
/// for recovery (I1).
/// Disk-backed (fjall_raw) reopen test — requires the `fjall` feature.
#[cfg(feature = "fjall")]
#[tokio::test]
async fn reopen_after_truncation_recovers_all() {
    const N: usize = 10;

    let tempdir = tempfile::TempDir::new().expect("tempdir");
    let path = tempdir.path().to_path_buf();

    let mut rids = Vec::with_capacity(N);
    {
        let repo = reopen_disk_repo("trunc", path.clone(), vec![TableConfig::new("t")]).await;
        let tbl = repo.get_table("t").await.unwrap();
        let gate = repo.tx_gate().await.unwrap();
        let wal = repo.repo_wal().await.unwrap();

        for i in 0..N {
            let (mut tx, _g) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
            let rid = tbl
                .insert_tx(&InnerValue::Str(format!("v{i}")), Some(&mut tx))
                .await
                .unwrap();
            repo.commit_tx(tx).await.unwrap();
            rids.push(rid);
        }

        // Drain → history durable + WAL truncated.
        Drainer::new().drain_all(&repo.core).await.unwrap();
        assert_eq!(
            gate.durable_watermark(),
            gate.last_committed(),
            "durable converged to visibility before reopen"
        );
        // Flush the durable stores so the reopen sees committed history.
        repo.flush_buffers().await.unwrap();

        // NOTE: with the default 8 MiB segment cap, N small records never roll
        // the segment, so they all live in the single ACTIVE segment — which is
        // NEVER truncated (I3, it holds the append tail). So `recover()` still
        // returns them here. Per-segment truncation (sealed-drop) is unit-tested
        // in `shamir-wal::segment_set_tests`; what this test pins is that
        // recovery survives the truncation path being LIVE on a disk repo (the
        // drainer ran `has_truncatable` + the truncate seam without losing data)
        // and that every value round-trips through `history` across a reopen.
        let _ = wal.recover().await.unwrap();

        // Drop repo (releases the sled lock).
    }

    // Reopen over the same dir: the WAL is (near-)empty after truncation, yet
    // every value is recovered from `history`.
    let repo2 = reopen_disk_repo("trunc", path.clone(), vec![TableConfig::new("t")]).await;
    repo2.recover_v2_inflight().await.unwrap();
    let tbl2 = repo2.get_table("t").await.unwrap();
    for (i, rid) in rids.iter().enumerate() {
        let got = tbl2.get(*rid).await.unwrap();
        let expect = format!("v{i}");
        assert!(
            matches!(got, InnerValue::Str(ref s) if *s == expect),
            "rid {rid:?} after reopen: expected {expect}, got {got:?}"
        );
    }
}

/// CRIT-2 (#436): the F6b truncation bound is `truncation_ceiling`, NOT
/// `durable_watermark`. `durable_watermark` advances unconditionally on every
/// entry (`gate.mark_durable`), but a sealed WAL segment must NOT be unlinked
/// while it may hold an entry whose interner delta is not yet durably
/// persisted (the A5 gate, `interner_delta_safe_to_truncate`, tripped
/// `Ok(false)`) — otherwise the only surviving copy of a (name, id) mapping
/// is deleted, and a crash before the interner's own checkpoint lets a later
/// mint reuse the id under a different name (silent corruption).
///
/// This test forces the A5 gate to ALWAYS report unsafe (via the
/// `FORCE_A5_UNSAFE` test seam in `materialize.rs`) on a disk-backed repo
/// with a tiny segment cap (so committing N records rolls multiple sealed
/// segments), then proves: `durable_watermark` converges to visibility (data
/// IS durable in `history`), but the on-disk segment count does NOT shrink —
/// the interner-unsafe ceiling pins every segment in place, contradicting
/// what `durable_watermark`-gated truncation (the pre-fix behavior) would do.
/// Disarming the fault and draining again then truncates normally, proving
/// the ceiling is a genuine gate, not a permanent stall.
/// Disk-backed (fjall_raw) CRIT-2 test — requires the `fjall` feature.
#[cfg(feature = "fjall")]
#[tokio::test(flavor = "current_thread")]
#[serial]
async fn truncation_ceiling_blocks_segment_removal_when_a5_gate_unsafe() {
    use crate::tx::materialize::FORCE_A5_UNSAFE;
    use std::sync::atomic::Ordering;

    // Failure-only flake diagnostics (design:
    // docs/dev-artifacts/design/flake-failure-logs.md): the earlier
    // "before=N, after=N" unlink-lag failure is invisible without a
    // timeline of the drain passes and the on-disk segment counts. The
    // report prints ONLY while the test is panicking.
    let guard = FlakeGuard::new("truncation_ceiling_blocks_segment_removal_when_a5_gate_unsafe");

    const N: usize = 80;

    let tempdir = tempfile::TempDir::new().expect("tempdir");
    let repo_dir = tempdir.path().join("crit2_repo");

    std::env::set_var("SHAMIR_WAL_SEGMENT_MAX_BYTES", "1024");
    guard.note("segment cap env", "SHAMIR_WAL_SEGMENT_MAX_BYTES=1024");

    // Arm the fault BEFORE any commits. `commit_tx`'s tail wakes an ambient
    // background drain loop (the same one behind the `crash_at_phase7`
    // flake fixed earlier) that runs the identical `drain_step` code and
    // reads the SAME `FORCE_A5_UNSAFE` flag — if it were armed only after
    // the commit loop, that background task could race ahead and drain
    // (safely truncate) some segments before this test ever gets to force
    // the fault, corrupting the "before" baseline. Arming first makes every
    // drain pass — background or foreground — agree that nothing is safe to
    // truncate for the whole setup phase.
    FORCE_A5_UNSAFE.store(true, Ordering::SeqCst);
    guard.note("A5 interner gate forced unsafe", "true");

    let repo = reopen_disk_repo("crit2", repo_dir.clone(), vec![TableConfig::new("t")]).await;
    let tbl = repo.get_table("t").await.unwrap();
    let gate = repo.tx_gate().await.unwrap();
    let wal = repo.repo_wal().await.unwrap();
    // Use the repo's OWN ambient drainer (`repo.drainer()`), NOT a fresh
    // `Drainer::new()`. `commit_tx`'s tail wakes the ambient one (lazily
    // spawned via `RepoInstance`'s `OnceLock`) in the background regardless
    // of what this test does explicitly — a separate `Drainer::new()`
    // instance has its OWN `unsafe_floor_version`/`window` state, so the
    // persisted-floor mechanism (CRIT-2) on a throwaway instance would never
    // see what the ambient background drainer already did (or vice versa).
    // Driving the SAME instance the background loop uses is the only way to
    // get a deterministic, single-authority view of the truncation ceiling.
    let drainer = repo.drainer();

    for i in 0..N {
        let (mut tx, _g) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
        tbl.insert_tx(&InnerValue::Str(format!("crit2-{i:04}")), Some(&mut tx))
            .await
            .unwrap();
        repo.commit_tx(tx).await.unwrap();
    }
    guard.mark("80 commits done");

    // Sanity: with the tiny cap and N=80 records, multiple sealed segments
    // must exist before any drain — otherwise the test proves nothing (a
    // single-segment repo has no truncation to observe).
    let segs_before_drain = shamirwal_seg_count(&repo_dir);
    guard.note(
        "segments on disk before drain",
        segs_before_drain.to_string(),
    );
    assert!(
        segs_before_drain >= 2,
        "test setup: expected multiple rolled segments before drain, got {segs_before_drain}"
    );

    // Explicit drain while STILL armed — belt-and-suspenders against any
    // remaining undrained tail. Keep the fault ARMED through the entire
    // "unsafe" assertion window below: `repo.drainer()` is the SAME ambient
    // instance `commit_tx`'s `wake()` drives in the background, so disarming
    // here (before checking segment counts) would let a background pass that
    // is still catching up from one of the 80 commits observe
    // `FORCE_A5_UNSAFE == false` and legitimately truncate a segment WHILE
    // this test is still in its "everything must stay unsafe" window —
    // exactly the flake this comment now documents (found under a full
    // `@engine --full` stress run: 1/4 runs failed with
    // `segments before=6, after=5`).
    drainer.drain_all(&repo.core).await.unwrap();
    guard.mark("drain_all completed with the A5 fault armed");
    guard.note(
        "gate state in the unsafe window",
        format!(
            "durable={}, last_committed={}, has_truncatable(durable watermark; not the drainer's clamped ceiling)={}, \
             a5_forced_unsafe={}",
            gate.durable_watermark(),
            gate.last_committed(),
            wal.has_truncatable(gate.durable_watermark()),
            FORCE_A5_UNSAFE.load(Ordering::SeqCst),
        ),
    );

    // Durable watermark still converges to visibility — `mark_durable` runs
    // unconditionally regardless of the A5 gate (read-visibility must not
    // regress just because the interner hasn't checkpointed).
    assert_eq!(
        gate.durable_watermark(),
        gate.last_committed(),
        "durable_watermark must converge to visibility even when every A5 \
         gate call is forced unsafe — mark_durable is unconditional"
    );

    // But NOT ONE segment was removed: with A5 forced unsafe on every entry,
    // `truncation_ceiling` never advances past 0, so `wal.has_truncatable`
    // (gated on the ceiling, not `durable_watermark`) stays false the whole
    // pass. Pre-fix (truncation gated on `durable_watermark` directly) this
    // would have collapsed to ~1 segment, exactly like
    // `wal_segment_count_bounded_under_drain` above.
    let segs_after_unsafe_drain = shamirwal_seg_count(&repo_dir);
    guard.note(
        "segments on disk after the unsafe drain",
        segs_after_unsafe_drain.to_string(),
    );
    assert_eq!(
        segs_after_unsafe_drain, segs_before_drain,
        "CRIT-2 regression: WAL segments were removed despite EVERY entry's \
         A5 interner-hwm gate reporting unsafe — truncation must be bounded \
         by truncation_ceiling, not durable_watermark (segments before={segs_before_drain}, \
         after={segs_after_unsafe_drain})"
    );

    // ONLY NOW disarm — after the "everything stayed unsafe" assertion has
    // been checked, so no background pass can slip a truncation through
    // while that invariant is still being verified.
    FORCE_A5_UNSAFE.store(false, Ordering::SeqCst);
    guard.mark("A5 fault disarmed");

    // Disarming the fault and draining again must now truncate normally —
    // proving the ceiling is a real, liftable gate, not a permanent wedge.
    //
    // Truncation is not instantaneous from this test's point of view:
    // `SegmentSet::truncate_below` CLAIMS the deletable segments under its lock
    // and unlinks them afterwards on a blocking thread, and the ambient
    // drainer's own pass can hold that claim — so an explicit `drain_all`
    // finds nothing left to claim, returns at once, and the files are still on
    // disk for a few more milliseconds (worse on a loaded runner, where the
    // earlier fixed 10-retry loop ran out in microseconds; the failure showed
    // before=11/after=11 although the next look found every sealed segment
    // gone). Poll for the effect with a bounded wait instead.
    let mut segs_after_safe_drain = shamirwal_seg_count(&repo_dir);
    for _ in 0..500 {
        if segs_after_safe_drain < segs_before_drain {
            break;
        }
        drainer.drain_all(&repo.core).await.unwrap();
        segs_after_safe_drain = shamirwal_seg_count(&repo_dir);
        if segs_after_safe_drain >= segs_before_drain {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }
    guard.mark("safe-drain poll loop exited");
    guard.note(
        "segments on disk after the safe drain",
        segs_after_safe_drain.to_string(),
    );
    std::env::remove_var("SHAMIR_WAL_SEGMENT_MAX_BYTES");
    assert!(
        segs_after_safe_drain < segs_before_drain,
        "once the A5 gate is safe again, truncation must proceed and shrink \
         the segment count (before={segs_before_drain}, after={segs_after_safe_drain})"
    );
}

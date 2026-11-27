//! CRIT-A / CRIT-B regression tests: V2 crash recovery must (a) be
//! reachable on the open path and (b) restore the MVCC version floor so
//! `assign_next_version()` never re-issues a commit_version a recovered
//! inflight entry already consumed.
//!
//! These exercise `RepoInstance::recover_v2_inflight` — the exact entry
//! point wired into the `shamir-db` bootstrap (`ShamirDb::init` /
//! `ShamirDb::add_repo`) — over a disk-backed (tempdir) repo so a
//! "restart" (drop + reopen over the same path) observes the same
//! persisted state a real process restart would, including the file WAL
//! segment. (F5e: the single WAL write path uses a per-instance `Mem` sink
//! for in-memory repos, so an injected inflight entry only genuinely
//! survives a restart on the file segment.)

use std::path::PathBuf;

use shamir_types::types::record_id::RecordId;
use shamir_types::types::value::InnerValue;
use shamir_wal::{WalDurability, WalEntryV2, WalOpV2};

use crate::meta::recovery_marker::{load_last_committed, save_last_committed};
use crate::repo::{repo_token, BoxRepoFactory, RepoInstance};
use crate::table::table_manager::table_token_for;
use crate::table::TableConfig;

fn rid(n: u8) -> RecordId {
    let mut a = [0u8; 16];
    a[15] = n;
    RecordId(a)
}

/// Open (or reopen) a disk-backed repo at `path`, retrying on
/// Windows where the backend releases its file lock lazily after `drop`.
async fn open_disk(name: &str, path: PathBuf, tables: Vec<TableConfig>) -> RepoInstance {
    let mut last_err = None;
    for _attempt in 0..10 {
        match RepoInstance::from_factory_for_recovery_test(
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
    panic!("open_disk({name:?}) failed after 10 retries: {last_err:?}");
}

/// Append a durable inflight V2 `Put` entry (NO data_store update)
/// carrying `commit_version` to the file WAL, exactly as a crash between
/// commit Phase 4 and Phase 7 leaves behind. Uses a throwaway
/// `RepoInstance` over `path` and never constructs its gate, then drops it
/// — modelling the in-memory side of a restart. The entry lives in the
/// persisted file segment and survives.
async fn seed_inflight_put(
    path: &std::path::Path,
    table: &str,
    record: RecordId,
    body: bytes::Bytes,
    commit_version: u64,
) {
    let seed = open_disk("r", path.to_path_buf(), vec![TableConfig::new(table)]).await;
    let wal = seed.repo_wal().await.unwrap();
    let entry = WalEntryV2::new(
        wal.fresh_txn_id(),
        repo_token(seed.name()),
        vec![WalOpV2::Put {
            table_id_interned: table_token_for(table),
            rid: record,
            body,
        }],
    )
    .with_commit_version(commit_version);
    // Synced so the entry hits the segment file before the drop below.
    wal.begin_grouped(&entry, WalDurability::Synced)
        .await
        .unwrap();
    drop(seed);
}

/// CRIT-A: recovery (the function the bootstrap open path invokes) replays
/// an inflight entry left by a crash, AND CRIT-B: it advances the gate's
/// version floor past the replayed commit_version and persists the
/// recovered floor durably.
#[tokio::test]
async fn recovery_wired_into_open_replays_inflight() {
    let tempdir = tempfile::TempDir::new().expect("tempdir");
    let path = tempdir.path().to_path_buf();
    let record = rid(42);
    let body = InnerValue::Str("recovered".into()).to_bytes().unwrap();

    // Fresh repo (no marker) + a single inflight tx at commit_version 10.
    seed_inflight_put(&path, "t", record, body, 10).await;

    // === SIMULATED RESTART: fresh RepoInstance over the same storage ===
    let repo = open_disk("r", path.clone(), vec![TableConfig::new("t")]).await;

    // Sanity: the inflight entry survived the "restart".
    let wal = repo.repo_wal().await.unwrap();
    assert_eq!(wal.recover().await.unwrap().len(), 1);

    // This is exactly what `ShamirDb::init` / `add_repo` now call.
    let recovered = repo.recover_v2_inflight().await.unwrap();
    assert_eq!(recovered, 1, "the inflight tx must be replayed");

    // Data is applied to the data store.
    let tbl = repo.get_table("t").await.unwrap();
    let read_back = tbl.get(record).await.unwrap();
    assert!(
        matches!(read_back, InnerValue::Str(ref s) if s == "recovered"),
        "expected recovered value, got {read_back:?}"
    );

    // CRIT-B: the next assigned version must be strictly greater than the
    // replayed commit_version (10) — not reuse 8/9/10.
    let gate = repo.tx_gate().await.unwrap();
    let next = gate.assign_next_version();
    assert!(
        next > 10,
        "assign_next_version must exceed replayed commit_version 10, got {next}"
    );

    // The persisted marker must reflect the recovered max so the *next*
    // restart seeds the floor correctly.
    let info = repo.tx_info_store().await.unwrap();
    let marker = load_last_committed(&info).await.unwrap();
    assert!(
        marker >= Some(10),
        "persisted last_committed marker must be >= recovered max 10, got {marker:?}"
    );
}

/// CRIT-B core: a stale persisted marker (7) plus an inflight entry whose
/// commit_version (10) outran the marker must yield `assign_next_version()
/// == 11` after recovery — NOT 8 (which would re-use the 8/9/10 version
/// space the crashed tx already consumed → monotonicity violation,
/// snapshot reads returning wrong data).
#[tokio::test]
async fn recovery_advances_gate_past_replayed_commit_version() {
    let tempdir = tempfile::TempDir::new().expect("tempdir");
    let path = tempdir.path().to_path_buf();

    // Persist a stale marker = 7 directly (a clean commit that landed
    // before the crashed one), WITHOUT constructing any gate.
    {
        let seed = open_disk("r", path.clone(), vec![TableConfig::new("t")]).await;
        let info = seed.tx_info_store().await.unwrap();
        save_last_committed(&info, 7).await.unwrap();
    }

    // Inflight tx at commit_version = 10 (marker never advanced to it —
    // crash before Phase 6.5).
    let record = rid(7);
    let body = InnerValue::Str("v10".into()).to_bytes().unwrap();
    seed_inflight_put(&path, "t", record, body, 10).await;

    // === SIMULATED RESTART ===
    let repo = open_disk("r", path.clone(), vec![TableConfig::new("t")]).await;

    let recovered = repo.recover_v2_inflight().await.unwrap();
    assert_eq!(recovered, 1);

    // The decisive assertion: 11, not 8.
    let gate = repo.tx_gate().await.unwrap();
    assert_eq!(
        gate.assign_next_version(),
        11,
        "gate must resume above the inflight commit_version (10), not from \
         the stale marker (7)"
    );

    // Marker re-persisted to the recovered floor (10) so a *second*
    // restart still seeds the gate at 10 rather than rewinding to 7.
    let info = repo.tx_info_store().await.unwrap();
    assert_eq!(load_last_committed(&info).await.unwrap(), Some(10));
}

/// P1d: recovery rebuilds the completion-prefix. After replaying 3 durable
/// WAL entries (V=1,2,3), the completion tracker watermark must equal 3 and
/// last_committed must mirror it.
#[tokio::test]
async fn recovery_rebuilds_completion_prefix() {
    let tempdir = tempfile::TempDir::new().expect("tempdir");
    let path = tempdir.path().to_path_buf();

    // Seed 3 inflight entries at commit_versions 1, 2, 3.
    for v in 1..=3u64 {
        let record = rid(v as u8);
        let body = InnerValue::Str(format!("v{v}")).to_bytes().unwrap();
        seed_inflight_put(&path, "t", record, body, v).await;
    }

    // === SIMULATED RESTART ===
    let repo = open_disk("r", path.clone(), vec![TableConfig::new("t")]).await;

    let recovered = repo.recover_v2_inflight().await.unwrap();
    assert_eq!(recovered, 3);

    let gate = repo.tx_gate().await.unwrap();
    // P1d: watermark must advance to 3 (all versions ≤ 3 are Materialized).
    assert_eq!(
        gate.completion().watermark(),
        3,
        "completion watermark must equal highest recovered commit_version"
    );
    // last_committed must mirror the watermark.
    assert_eq!(
        gate.last_committed(),
        3,
        "last_committed must equal the completion watermark after recovery"
    );
}

/// Defence-in-depth for the marker re-persist: after a first recovery
/// persists the floor, a SECOND fresh `RepoInstance` over the same storage
/// must still seed its gate above the recovered commit_version. (F5e: the
/// file WAL has no per-entry truncation until F6, so the inflight entry is
/// re-replayed on the second restart too; recovery is idempotent and the
/// floor is also durably persisted via the marker.)
#[tokio::test]
async fn recovered_floor_survives_a_second_restart() {
    let tempdir = tempfile::TempDir::new().expect("tempdir");
    let path = tempdir.path().to_path_buf();
    let record = rid(9);
    let body = InnerValue::Str("v25".into()).to_bytes().unwrap();
    seed_inflight_put(&path, "t", record, body, 25).await;

    // First restart: recover (replays the entry, persists the floor).
    {
        let repo1 = open_disk("r", path.clone(), vec![TableConfig::new("t")]).await;
        assert_eq!(repo1.recover_v2_inflight().await.unwrap(), 1);
        drop(repo1);
    }

    // Second restart: the gate floor must be above the recovered
    // commit_version — sourced from the persisted marker and/or the
    // replayed segment entry.
    let repo2 = open_disk("r", path.clone(), vec![TableConfig::new("t")]).await;
    repo2.recover_v2_inflight().await.unwrap();

    let gate = repo2.tx_gate().await.unwrap();
    assert!(
        gate.assign_next_version() > 25,
        "the recovered floor (25) must survive a second restart"
    );
}

/// Audit §1.7 regression: `persist_markers` must write the MONOTONIC max
/// (`gate.last_committed()`), not the raw `commit_version` of the calling
/// tx. Without this, parallel Phase 6.5 writers can regress the marker:
/// committer A (v=10) writes 10, then committer B (v=9) writes 9, and a
/// crash leaves the marker at 9 — the gate seeds below a truncated segment
/// and `assign_next_version` re-issues 10.
///
/// This test simulates the race: advance the gate's `last_committed` to 10
/// (a parallel committer that already published), then call `persist_markers`
/// with `commit_version = 9` (the "slower" committer). The persisted marker
/// must be >= 10, NOT regressed to 9.
#[tokio::test]
async fn persist_markers_writes_monotonic_max_not_raw_version() {
    let tempdir = tempfile::TempDir::new().expect("tempdir");
    let path = tempdir.path().to_path_buf();
    let repo = open_disk("r", path, vec![TableConfig::new("t")]).await;
    let gate = repo.tx_gate().await.unwrap();

    // Simulate a parallel committer that published v=10 (advancing
    // last_committed to 10 via the monotonic max).
    gate.publish_committed_max(10);
    assert_eq!(gate.last_committed(), 10);

    // Now the "current" committer (v=9) calls persist_markers. Before the
    // fix this wrote 9, regressing the marker below the already-published 10.
    crate::tx::commit_phases::persist_markers(&repo.core, gate.as_ref(), 9)
        .await
        .unwrap();

    // The marker must reflect the monotonic max (10), not the raw 9.
    let info = repo.tx_info_store().await.unwrap();
    let marker = load_last_committed(&info).await.unwrap();
    assert!(
        marker >= Some(10),
        "persist_markers must write the monotonic max (>= 10), not regress to the \
         raw commit_version (9); got {marker:?}"
    );
}

/// Audit §1.7 residual (found in @sh review of task #494): the in-memory
/// `gate.last_committed()` value is a monotonic max, but WITHOUT
/// serialization on the disk write itself, two concurrent `persist_markers`
/// calls could still land in the WRONG order on disk — e.g. committer A
/// (reads gate max = 10, about to write) races committer B, which ALSO
/// reads the gate (still 10 at that instant) but wins the disk-write race
/// and persists first; if a THIRD write for a smaller value somehow landed
/// after (a plain `Store::set` has no ordering guarantee relative to other
/// concurrent `Store::set` calls to the same key), the marker could regress
/// even though every caller's `gate.last_committed()` read was correct.
///
/// This test proves `persist_markers` refuses to regress the ON-DISK value
/// even when a HIGHER value is already persisted than what this call's own
/// candidate would be — i.e. it does a genuine read-current/write-if-greater
/// under `marker_write_mutex`, not a blind write of `gate.last_committed()`.
#[tokio::test]
async fn persist_markers_never_regresses_the_on_disk_marker() {
    let tempdir = tempfile::TempDir::new().expect("tempdir");
    let path = tempdir.path().to_path_buf();
    let repo = open_disk("r", path, vec![TableConfig::new("t")]).await;
    let gate = repo.tx_gate().await.unwrap();
    let info = repo.tx_info_store().await.unwrap();

    // Simulate a "faster" concurrent writer that already persisted 20
    // directly (bypassing `persist_markers`, standing in for a genuinely
    // concurrent call that raced ahead on the disk write).
    save_last_committed(&info, 20).await.unwrap();

    // The gate's own monotonic max is only 10 here (this committer hasn't
    // observed the other writer's in-memory publish, only sees its own
    // commit_version=9 candidate folded into gate.last_committed()=10).
    gate.publish_committed_max(10);
    assert_eq!(gate.last_committed(), 10);

    // persist_markers must NOT overwrite the already-higher on-disk value
    // (20) with the lower in-memory candidate (10) — that would be exactly
    // the disk-write-ordering regression this fix closes.
    crate::tx::commit_phases::persist_markers(&repo.core, gate.as_ref(), 9)
        .await
        .unwrap();

    let marker = load_last_committed(&info).await.unwrap();
    assert_eq!(
        marker,
        Some(20),
        "persist_markers must never regress an already-higher on-disk marker; \
         got {marker:?} (expected the pre-existing 20 to survive)"
    );
}

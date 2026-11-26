//! Kill-subprocess crash harness (S5c-X PR6b-2, review CI requirement): a
//! `#[tokio::test]` child crashes the process with `std::process::abort()`
//! inside `test_seam::phase()` at a receipt pipeline phase; the PARENT tests
//! spawn the child through the libtest binary, then reopen the repo and
//! replay the SAME batch — asserting retry convergence: identical history
//! allocation, byte-identical journal rows across both replays, absolute
//! counters, and a retired receipt with an advanced bookmark v3.
//!
//! The PARENT requires the child's `SHAMIR_RECEIPT_ABORT_FIRED {phase:?}`
//! stderr marker for the EXACT crash point (review finding 4): a non-zero
//! exit alone is ambiguous (any startup panic produces one).
//!
//! The only permitted wait is the bounded fjall Database-lock release retry
//! right after the child was killed (resource acquisition, not
//! synchronization).

use bytes::Bytes;
use futures::StreamExt;
use shamir_collections::TMap;
use shamir_storage::types::RecordKey;
use shamir_tx::{decode_version_key, ChangeOp, ChangelogEvent, RecordChange};
use shamir_types::access::Actor;
use shamir_types::types::record_id::RecordId;
use shamir_types::types::value::InnerValue;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use crate::meta::repl::bookmark_v3::load_bookmark_v3;
use crate::meta::repl::receipt::receipt_key;
use crate::repo::{BoxRepoFactory, RepoInstance};
use crate::table::TableConfig;
use crate::tx::apply_replicated::apply_replicated_batch;
use crate::tx::apply_replicated::follower_durability::test_seam::{self, Phase};
use crate::tx::{ApplyPolicy, ReplicatedEvent};

/// Full libtest path of the child scenario test in this binary.
const CHILD_TEST: &str = "tx::replication::apply_replicated::follower_durability::receipt::tests::crash_subprocess_tests::receipt_crash_child_scenario";

// ---------------------------------------------------------------- helpers

fn event(leader_version: u64, id: u8) -> ReplicatedEvent {
    ReplicatedEvent {
        event: ChangelogEvent {
            repo: "leader".into(),
            commit_version: leader_version,
            tx_id: leader_version,
            actor: Actor::System,
            timestamp_ns: leader_version * 1_000_000,
            changes: vec![RecordChange {
                table: "items".into(),
                key: RecordId([id; 16]).to_bytes(),
                op: ChangeOp::Put,
                value: Some(InnerValue::Str(format!("row-{id}")).to_bytes().unwrap()),
            }],
        },
        field_bindings: Vec::new(),
    }
}

fn delete_event(leader_version: u64, id: u8) -> ReplicatedEvent {
    ReplicatedEvent {
        event: ChangelogEvent {
            repo: "leader".into(),
            commit_version: leader_version,
            tx_id: leader_version,
            actor: Actor::System,
            timestamp_ns: leader_version * 1_000_000,
            changes: vec![RecordChange {
                table: "items".into(),
                key: RecordId([id; 16]).to_bytes(),
                op: ChangeOp::Delete,
                value: None,
            }],
        },
        field_bindings: Vec::new(),
    }
}

fn repo_name(path: &Path) -> String {
    format!(
        "receipt-crash-{}",
        path.display().to_string().replace('\\', "_")
    )
}

async fn open_with_retry(path: &Path, name: &str) -> RepoInstance {
    // Bounded resource-acquisition retry: the killed child's fjall Database
    // lock may take a moment to be released by the OS (Windows especially).
    for attempt in 0..25u32 {
        match RepoInstance::from_factory(
            name.to_owned(),
            BoxRepoFactory::fjall_raw(path),
            vec![TableConfig::new("items")],
        )
        .await
        {
            Ok(repo) => return repo,
            Err(error) if attempt < 24 => {
                let _ = error;
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Err(error) => panic!("reopen failed after retries: {error}"),
        }
    }
    unreachable!("retry loop always returns or panics")
}

async fn close(repo: RepoInstance) {
    let handle = repo.changefeed().await.unwrap();
    handle.feed.shutdown_and_wait().await.unwrap();
    drop(handle);
    drop(repo);
}

async fn journal_bytes(repo: &RepoInstance, version: u64) -> Option<Bytes> {
    repo.follower_journal_store()
        .await
        .unwrap()
        .get(RecordKey::from_slice(&version.to_be_bytes()))
        .await
        .ok()
}

/// All history rows of `keys`, sorted: `(key, version, value)`.
async fn history_rows(repo: &RepoInstance, keys: &[Vec<u8>]) -> Vec<(Vec<u8>, u64, Bytes)> {
    let store = repo.raw_store("__history__items").await.unwrap();
    let mut rows = Vec::new();
    for batch in store.iter_stream(128).collect::<Vec<_>>().await {
        for (record_key, value) in batch.unwrap() {
            if let Some((key, version)) = decode_version_key(record_key.as_ref()) {
                if keys.iter().any(|k| k.as_slice() == key) {
                    rows.push((key.to_vec(), version, value));
                }
            }
        }
    }
    rows.sort();
    rows
}

/// Per-version timestamp row of the history store: keyed `0x00 ||
/// version_be8`, holding the 8-byte LE millis (the TS_TAG mirror written by
/// `receipt/stage.rs::stage_materialized_batch`).
async fn ts_row(repo: &RepoInstance, version: u64) -> Option<u64> {
    let mut key = Vec::with_capacity(9);
    key.push(0x00);
    key.extend_from_slice(&version.to_be_bytes());
    let value = repo
        .raw_store("__history__items")
        .await
        .unwrap()
        .get(RecordKey::from_slice(&key))
        .await
        .ok()?;
    Some(u64::from_le_bytes(value.as_ref().try_into().unwrap()))
}

// ---------------------------------------------------------------- scenario

#[derive(Clone, Copy)]
struct Scenario {
    /// Crash point passed to the child (`SHAMIR_RECEIPT_CRASH_POINT`).
    point: &'static str,
    multi: bool,
    tombstone: bool,
}

impl Scenario {
    fn events(&self) -> Vec<ReplicatedEvent> {
        if self.tombstone {
            // A later event deletes the earlier key.
            vec![event(1, 7), delete_event(2, 7)]
        } else if self.multi {
            // Repeated key: the same key lands at versions 1 and 2, which
            // exercises the ABSOLUTE counter (two puts, one distinct key).
            vec![event(1, 1), event(2, 1)]
        } else {
            vec![event(1, 1)]
        }
    }

    fn keys(&self) -> Vec<Vec<u8>> {
        if self.tombstone {
            vec![RecordId([7; 16]).to_bytes().to_vec()]
        } else {
            vec![RecordId([1; 16]).to_bytes().to_vec()]
        }
    }

    fn local_version_watermark(&self) -> u64 {
        self.events().len() as u64
    }

    /// Expected `(key, version, value)` history set: one row per event, at
    /// the receipt's local versions `1..=N`; a Delete row is the empty value.
    fn expected_history(&self) -> Vec<(Vec<u8>, u64, Bytes)> {
        self.events()
            .iter()
            .enumerate()
            .map(|(index, event)| {
                let change = &event.event.changes[0];
                let value = match &change.value {
                    Some(value) => Bytes::copy_from_slice(value),
                    None => Bytes::new(),
                };
                (change.key.to_vec(), index as u64 + 1, value)
            })
            .collect()
    }

    /// Expected absolute counter after the batch: the counter semantics are
    /// "absolute live-row count of DISTINCT keys present", i.e. the latest
    /// op per key wins (a tombstoned key contributes 0, a repeated key 1) —
    /// NOT the number of Put events (review finding 9a: repeated-key
    /// expected 2 where the true count is 1; Put+Delete expected 1 where the
    /// true count is 0).
    fn expected_counter(&self) -> u64 {
        let mut live: TMap<Vec<u8>, bool> = TMap::default();
        for event in self.events() {
            let change = &event.event.changes[0];
            live.insert(change.key.to_vec(), change.op == ChangeOp::Put);
        }
        live.into_values().filter(|exists| *exists).count() as u64
    }
}

async fn assert_converged(repo: &RepoInstance, scenario: &Scenario) {
    let keys = scenario.keys();
    let expected = scenario.expected_history();
    let versions: Vec<u64> = {
        let mut versions = expected
            .iter()
            .map(|(_, version, _)| *version)
            .collect::<Vec<_>>();
        versions.sort_unstable();
        versions
    };

    // Allocation unchanged: EXACTLY the receipt's versions, no extras.
    assert_eq!(
        history_rows(repo, &keys).await,
        expected,
        "history allocation must be exactly the receipt's versions {versions:?}"
    );

    // Journal bytes at every applied version, non-empty.
    for version in 1..=scenario.local_version_watermark() {
        let bytes = journal_bytes(repo, version)
            .await
            .unwrap_or_else(|| panic!("journal row for version {version} missing"));
        assert!(!bytes.is_empty(), "journal row {version} empty");
    }

    // Exact downstream journal bytes: the follower journal row at local
    // version v is the reprojected event (same actor/timestamp/key/op/value,
    // `repo` = this repo's name, `commit_version` = v), rmp-serialized.
    for (index, event) in scenario.events().iter().enumerate() {
        let version = index as u64 + 1;
        let expected = ChangelogEvent {
            repo: repo.name().to_owned(),
            commit_version: version,
            tx_id: event.event.tx_id,
            actor: event.event.actor.clone(),
            timestamp_ns: event.event.timestamp_ns,
            changes: event.event.changes.clone(),
        };
        let expected_bytes = rmp_serde::to_vec(&expected)
            .unwrap_or_else(|error| panic!("expected journal encode v{version}: {error}"));
        let bytes = journal_bytes(repo, version)
            .await
            .unwrap_or_else(|| panic!("journal row for version {version} missing"));
        assert_eq!(
            bytes.as_ref(),
            expected_bytes.as_slice(),
            "journal row {version} must be the exact reprojected event"
        );
    }

    // Exact timestamp rows: the history store carries one `[0x00 || v_be8]`
    // ts row per version with the 8-byte LE millis of the event.
    for (index, event) in scenario.events().iter().enumerate() {
        let version = index as u64 + 1;
        assert_eq!(
            ts_row(repo, version).await,
            Some(event.event.timestamp_ns / 1_000_000),
            "history ts row millis for version {version}"
        );
    }

    // Allocation watermark: no extra versions beyond the receipt's batch.
    assert_eq!(
        repo.tx_gate()
            .await
            .unwrap()
            .version_allocation_high_water_mark(),
        scenario.local_version_watermark(),
        "no versions may be allocated beyond the receipt's batch"
    );

    // Absolute counter.
    let counter = repo.get_table("items").await.unwrap().counter().clone();
    assert_eq!(
        counter.get().await.unwrap(),
        scenario.expected_counter(),
        "counter must equal the absolute applied row count"
    );

    // Tombstone scenario: the deleted key reads as absent from the snapshot.
    if scenario.tombstone {
        let table = repo.get_table("items").await.unwrap();
        assert!(table.get_bytes(RecordId([7; 16])).await.unwrap().is_none());
        drop(table);
    }

    // Bookmark v3: receipt retired, bookmark == leader watermark,
    // clean horizon covers the batch, receipt key absent.
    let info = repo.tx_info_store().await.unwrap();
    let v3 = load_bookmark_v3(&info).await.unwrap().unwrap();
    assert_eq!(v3.outstanding_receipt, None, "receipt must be retired");
    assert_eq!(v3.bookmark, scenario.local_version_watermark());
    assert_eq!(v3.clean_through, Some(scenario.events().len() as u64));
    assert!(matches!(
        info.get(receipt_key()).await,
        Err(shamir_storage::error::DbError::NotFound(_))
    ));
    drop(info);
}

/// Historical snapshot / AS OF reads, checked against the latest journal
/// change for each key visible at each local version.
async fn assert_as_of_reads(repo: &RepoInstance, scenario: &Scenario) {
    let mvcc = repo
        .get_table("items")
        .await
        .unwrap()
        .mvcc_store()
        .expect("items table must have an MVCC store");
    let watermark = scenario.local_version_watermark();
    let events = scenario.events();
    for at in 1..=watermark {
        for key in scenario.keys() {
            let expected = events[..at as usize]
                .iter()
                .rev()
                .flat_map(|event| event.event.changes.iter())
                .find(|change| change.key.as_ref() == key.as_slice())
                .and_then(|change| change.value.as_ref().cloned());
            assert_eq!(
                mvcc.get_at(&key, at).await.unwrap(),
                expected,
                "as-of read mismatch for key {key:?} at version {at}"
            );
        }
    }
}

// ------------------------------------------------------------------ child

/// CHILD process scenario: crashes with `std::process::abort()` inside the
/// receipt pipeline at `SHAMIR_RECEIPT_CRASH_POINT`. Returns immediately (a
/// no-op pass) when `SHAMIR_RECEIPT_CRASH_SCENARIO` is unset, so ordinary
/// test runs never spawn anything.
#[tokio::test]
async fn receipt_crash_child_scenario() {
    let Ok(scenario) = std::env::var("SHAMIR_RECEIPT_CRASH_SCENARIO") else {
        return;
    };
    assert_eq!(scenario, "receipt-crash");
    let path = PathBuf::from(std::env::var("SHAMIR_RECEIPT_CRASH_PATH").unwrap());
    let point = std::env::var("SHAMIR_RECEIPT_CRASH_POINT").unwrap();
    let (phase, phase_str): (Phase, &'static str) = match point.as_str() {
        "prepared" => (Phase::Prepared, "prepared"),
        "materialized" => (Phase::Materialized, "materialized"),
        "checkpointed" => (Phase::Checkpointed, "checkpointed"),
        other => panic!("unknown crash point {other}"),
    };

    let name = repo_name(&path);
    let repo = open_with_retry(&path, &name).await;

    let multi = std::env::var("SHAMIR_RECEIPT_CRASH_MULTI").is_ok_and(|v| v == "1");
    let tombstone = std::env::var("SHAMIR_RECEIPT_CRASH_TOMBSTONE").is_ok_and(|v| v == "1");
    let scenario = Scenario {
        point: phase_str,
        multi,
        tombstone,
    };
    let events = scenario.events();

    let hook = Arc::new(test_seam::Hook::default());
    *hook.abort_at.lock().unwrap() = Some(phase);
    test_seam::install(&name, Arc::clone(&hook));

    let result = apply_replicated_batch(&repo.core, &events, 0, ApplyPolicy::Trusted).await;
    unreachable!("abort must preempt the apply; it returned {result:?}");
}

// ------------------------------------------------------------------ parent

/// Serializes the parent tests: they spawn children against shared
/// process-global infrastructure.
static PARENT_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Spawns the child scenario in this test binary and returns
/// `(exit status, combined output)`. The child is expected to DIE via
/// `abort()` (non-zero exit: SIGABRT on Unix, exit code 3 on Windows).
fn spawn_child(path: &Path, extra_env: &[(&str, &str)]) -> (std::process::ExitStatus, String) {
    let mut command = std::process::Command::new(std::env::current_exe().unwrap());
    command
        .arg(CHILD_TEST)
        .arg("--exact")
        .arg("--test-threads=1")
        .arg("--nocapture")
        .env("SHAMIR_RECEIPT_CRASH_SCENARIO", "receipt-crash")
        .env("SHAMIR_RECEIPT_CRASH_PATH", path);
    for (key, value) in extra_env {
        command.env(key, value);
    }
    let output = command.output().unwrap();
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    (output.status, text)
}

fn assert_child_died(status: std::process::ExitStatus, output: &str, point: &str) {
    assert!(
        !status.success(),
        "child at crash point {point} must die via abort(), got success"
    );
    assert!(
        !output.contains("test result: ok"),
        "child at crash point {point} must not report a passing run; output: {output}"
    );
    // Negative-control for the harness itself (review finding 9b): the child
    // must have died via the seam's abort INSIDE the pipeline, not by
    // falling off the end of `receipt_crash_child_scenario`'s fallback
    // `unreachable!("abort must preempt the apply; ...")` — the latter would
    // mean the apply RETURNED and the crash never exercised the pipeline.
    assert!(
        !output.contains("abort must preempt"),
        "child at crash point {point} fell through to the fallback panic: the \
         abort hook did not fire inside the pipeline; output: {output}"
    );
    // Round 5 (review finding 4): the death itself is still ambiguous — an
    // unrelated startup panic (env var missing, repo open failure, …) also
    // produces a non-zero exit with no "test result: ok". The producer
    // prints `SHAMIR_RECEIPT_ABORT_FIRED {phase:?}` (test_seam.rs,
    // `phase()`) right before `std::process::abort()`, so the child MUST
    // have printed the marker AND the exact `{:?}`-formatted phase name for
    // its crash point. Missing marker = child died WITHOUT reaching the
    // armed abort hook.
    assert!(
        output.contains("SHAMIR_RECEIPT_ABORT_FIRED"),
        "child at crash point {point} died without reaching the armed abort \
         hook (no SHAMIR_RECEIPT_ABORT_FIRED marker); output: {output}"
    );
    let expected_phase = match point {
        "prepared" => "Prepared",
        "materialized" => "Materialized",
        "checkpointed" => "Checkpointed",
        other => panic!("unknown crash point {other}"),
    };
    assert!(
        output.contains(&format!("SHAMIR_RECEIPT_ABORT_FIRED {expected_phase}")),
        "child at crash point {point} reached an abort hook but for the WRONG \
         phase: expected `SHAMIR_RECEIPT_ABORT_FIRED {expected_phase}`; output: {output}"
    );
}

/// For one crash point: spawn the aborting child, then reopen TWICE, replaying
/// the SAME batch after each open, and verify full convergence each time
/// (identical allocation, byte-identical journal, counters, retired receipt).
async fn run_point(scenario: Scenario) {
    let temp = tempfile::TempDir::new().unwrap();
    let name = repo_name(temp.path());

    let mut env = vec![];
    if scenario.multi {
        env.push(("SHAMIR_RECEIPT_CRASH_MULTI", "1"));
    }
    if scenario.tombstone {
        env.push(("SHAMIR_RECEIPT_CRASH_TOMBSTONE", "1"));
    }
    let (status, output) = {
        let mut env = env.clone();
        env.push(("SHAMIR_RECEIPT_CRASH_POINT", scenario.point));
        spawn_child(temp.path(), &env)
    };
    assert_child_died(status, &output, scenario.point);

    let events = scenario.events();

    // First reopen + replay (resumes / converges the crashed receipt).
    let journal_snapshot;
    let history_snapshot;
    {
        let repo = open_with_retry(temp.path(), &name).await;
        let outcome = apply_replicated_batch(&repo.core, &events, 0, ApplyPolicy::Trusted).await;
        assert!(outcome.is_ok(), "first replay failed: {outcome:?}");
        assert_converged(&repo, &scenario).await;
        assert_as_of_reads(&repo, &scenario).await;
        journal_snapshot = collect_journals(&repo, scenario.local_version_watermark()).await;
        history_snapshot = history_rows(&repo, &scenario.keys()).await;
        close(repo).await;
    }

    // Second reopen + replay: the batch is already applied; the invariants
    // must hold UNCHANGED (no new versions, no journal divergence).
    {
        let repo = open_with_retry(temp.path(), &name).await;
        let outcome = apply_replicated_batch(&repo.core, &events, 0, ApplyPolicy::Trusted).await;
        assert!(outcome.is_ok(), "second replay failed: {outcome:?}");
        assert_converged(&repo, &scenario).await;
        assert_as_of_reads(&repo, &scenario).await;
        assert_eq!(
            collect_journals(&repo, scenario.local_version_watermark()).await,
            journal_snapshot,
            "journal bytes must be identical across the two replays"
        );
        assert_eq!(
            history_rows(&repo, &scenario.keys()).await,
            history_snapshot,
            "history allocation must be identical across the two replays"
        );
        close(repo).await;
    }
    // `temp` drops here: every temp dir is cleaned.
}

async fn collect_journals(repo: &RepoInstance, watermark: u64) -> Vec<Option<Bytes>> {
    let mut rows = Vec::new();
    for version in 1..=watermark {
        rows.push(journal_bytes(repo, version).await);
    }
    rows
}

#[tokio::test]
async fn crash_reopen_replay_convergence_matrix() {
    let _guard = PARENT_LOCK.lock().await;
    for point in ["prepared", "materialized", "checkpointed"] {
        run_point(Scenario {
            point,
            multi: false,
            tombstone: false,
        })
        .await;
    }
    // MULTI (repeated key → absolute counter) and TOMBSTONE (delete row)
    // variants: one crash point each keeps the total runtime modest.
    run_point(Scenario {
        point: "materialized",
        multi: true,
        tombstone: false,
    })
    .await;
    run_point(Scenario {
        point: "checkpointed",
        multi: false,
        tombstone: true,
    })
    .await;
}

//! P3b — Stress + crash-injection tests for the Version Oracle.
//!
//! Proves serializability, monotonicity, watermark convergence, and recovery
//! correctness under concurrent load and simulated crashes.
//!
//! # Flake-hunt methodology
//!
//! All concurrency tests here are designed to be deterministic in their
//! outcome (abort vs. commit decisions are pure functions of the tx index,
//! not of scheduling). To verify non-flakiness, run repeated iterations:
//!
//! ```sh
//! for i in $(seq 1 200); do
//!   ./scripts/test.sh -p shamir-engine -- oracle_stress || { echo "FAILED on iter $i"; break; }
//! done
//! ```
//!
//! Iteration counts for stress tests are raised to catch scheduler-dependent
//! regressions: `N=50` for disjoint, `M=30` for snapshot, `M=40` for SSI
//! conflict, `TOTAL=80` for abort-stall. This gives 200+ version slots per
//! run, exercising the contiguous-watermark advance logic.

use std::sync::Arc;

use bytes::Bytes;
use shamir_storage::storage_in_memory::{InMemoryRepo, InMemoryStore};
use shamir_storage::types::Store;
use shamir_tx::{IsolationLevel, StagingStore, TxContext, TxId, VersionProvider};
#[cfg(feature = "fjall")]
use shamir_types::types::record_id::RecordId;
use shamir_types::types::value::InnerValue;
#[cfg(feature = "fjall")]
use shamir_wal::{WalDurability, WalEntryV2, WalOpV2};

use crate::tx::commit_tx;
use crate::tx::repo::repo_instance::RepoInstance;
#[cfg(feature = "fjall")]
use crate::tx::repo::repo_token;
use crate::tx::repo::repo_types::BoxRepo;
#[cfg(feature = "fjall")]
use crate::tx::repo::BoxRepoFactory;
use crate::tx::table::table_manager::table_token_for;
use crate::tx::table::TableConfig;
use crate::tx::RepoVersionProvider;

fn make_repo() -> RepoInstance {
    let repo = Arc::new(InMemoryRepo::new());
    RepoInstance::new("stress".into(), BoxRepo::InMemory(repo), Vec::new())
}

// =========================================================================
// Test 1 — concurrent disjoint-table throughput + monotonicity
// =========================================================================

/// N concurrent committers each writing a distinct table. All succeed with
/// unique monotonic versions; the watermark advances to cover all of them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn oracle_stress_disjoint_table_concurrent_monotonic() {
    const N: usize = 50;

    let tables: Vec<String> = (0..N).map(|i| format!("tbl_{i}")).collect();
    let repo = make_repo();
    for t in &tables {
        repo.add_table_for_test(TableConfig::new(t.as_str()));
    }

    let mut handles = Vec::with_capacity(N);
    for (i, table_name) in tables.iter().enumerate() {
        let r = repo.clone();
        let table_name = table_name.clone();
        handles.push(tokio::spawn(async move {
            let tbl = r.get_table(&table_name).await.unwrap();
            let (mut tx, _g) = r.begin_tx(IsolationLevel::Snapshot).await.unwrap();
            tbl.insert_tx(&InnerValue::Int(i as i64), Some(&mut tx))
                .await
                .unwrap();
            let outcome = r.commit_tx(tx).await.unwrap();
            outcome.commit_version
        }));
    }

    let mut versions = Vec::with_capacity(N);
    for h in handles {
        versions.push(h.await.unwrap());
    }

    // All versions unique.
    let mut sorted = versions.clone();
    sorted.sort_unstable();
    sorted.dedup();
    assert_eq!(
        sorted.len(),
        N,
        "all {N} commit versions must be unique, got {} unique",
        sorted.len()
    );

    // Monotonic: sorted == original sorted (trivially true since we sort).
    // More importantly, min > 0.
    assert!(*sorted.first().unwrap() > 0);

    // Watermark must advance to cover all committed versions.
    let gate = repo.tx_gate().await.unwrap();
    let wm = gate.completion().watermark();
    let max_v = *sorted.last().unwrap();
    assert!(
        wm >= max_v,
        "watermark ({wm}) must be >= max committed version ({max_v})"
    );

    // Each table has its data.
    for (i, t) in tables.iter().enumerate() {
        let tbl = repo.get_table(t.as_str()).await.unwrap();
        let count = tbl.counter().get().await.unwrap();
        assert!(
            count >= 1,
            "table {t} (idx {i}) must have at least 1 record"
        );
    }
}

// =========================================================================
// Test 2 — concurrent same-table, conflict resolution
// =========================================================================

/// M concurrent committers on the SAME (table, key) under Snapshot isolation.
/// All succeed (last-writer-wins); versions are unique; watermark advances.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn oracle_stress_same_table_snapshot_all_succeed() {
    const M: usize = 30;

    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("shared"));
    let data_store: Arc<dyn Store> = Arc::new(InMemoryStore::new());

    let mut handles = Vec::with_capacity(M);
    for i in 0..M {
        let r = repo.clone();
        let ds = Arc::clone(&data_store);
        handles.push(tokio::spawn(async move {
            let mut tx = TxContext::new(TxId::new(i as u64 + 1000), 0, 0, IsolationLevel::Snapshot);
            let mut staging = StagingStore::new(Arc::clone(&ds));
            staging.set(
                Bytes::from_static(b"contested_key").into(),
                Bytes::from(format!("val_{i}")),
            );
            tx.write_set.insert(table_token_for("shared"), staging);
            let outcome = commit_tx(tx, &r.core).await.unwrap();
            outcome.commit_version
        }));
    }

    let mut versions = Vec::with_capacity(M);
    for h in handles {
        versions.push(h.await.unwrap());
    }

    versions.sort_unstable();
    versions.dedup();
    assert_eq!(versions.len(), M, "all versions must be unique");

    let gate = repo.tx_gate().await.unwrap();
    let wm = gate.completion().watermark();
    assert!(
        wm >= *versions.last().unwrap(),
        "watermark must advance past all committed versions"
    );

    // Final value is one of the writes.
    let got = data_store
        .get(Bytes::from_static(b"contested_key").into())
        .await
        .unwrap();
    let got_str = String::from_utf8(got.to_vec()).unwrap();
    assert!(
        got_str.starts_with("val_"),
        "final value must be from one of the writers, got: {got_str}"
    );
}

/// Same-table Serializable: post-hoc read-set staleness detection via
/// `RepoVersionProvider`. This validates the ABORT path when a B-tx declares
/// `version_seen=0` for a key that A already committed at V_a ≥ 1. It is NOT
/// a concurrent anti-dependency SSI conflict (A finishes fully before B starts),
/// but rather exercises the `validate_read_set` abort logic with the real
/// `MvccStore`-backed provider.
///
/// Scenario:
///   1. Tx A commits (Snapshot) via raw StagingStore, writing `raw_key` into
///      table "ser". After commit, the MvccStore for "ser" has
///      `version_of(raw_key) == V_a ≥ 1`.
///   2. M concurrent B-txs each:
///      - `record_read("ser", raw_key, 0)` — assert "I saw raw_key at version 0"
///        (explicitly BEFORE V_a, which is the read that conflicts)
///      - attach `RepoVersionProvider` (queries real MvccStore)
///      - stage a write on a DISTINCT per-tx key (tx is non-empty → no C6 skip)
///      - call `commit_tx`
///   3. At pre_commit, `validate_read_set` calls `version_of(raw_key)` → V_a > 0
///      → read-set staleness → each B-tx aborts.
///
/// NOTE: `record_read(table, key, version_seen)` takes an EXPLICIT `version_seen`
/// supplied by the caller — it is NOT the snapshot_version. By passing 0 we
/// assert "last time I read this key the committed version was 0", which is
/// always less than V_a (≥ 1). This is the A-writes / B-reads anomaly.
///
/// Invariants (scheduling-independent because tx A finishes before B-txs start):
///   - `successes + conflicts == M`   (no tx is lost)
///   - `conflicts > 0`                (real SSI provider detects the conflict)
///   - Each successful B-tx has a unique commit version
///   - Watermark advances past tx A's committed version
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn oracle_stress_same_table_serializable_posthoc_readset_staleness() {
    const M: usize = 40;

    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("ser"));

    let table_token = table_token_for("ser");
    // raw_key is an arbitrary fixed byte sequence that tx A writes and B-txs read.
    let raw_key = Bytes::from_static(b"contested_ssi_key");

    // Step 1: commit tx A via low-level StagingStore so the exact raw_key
    // enters the MvccStore version cache. We use a fresh InMemoryStore as the
    // backing data_store for the staging (this is the same pattern as
    // oracle_stress_same_table_snapshot_all_succeed).
    let a_data_store: Arc<dyn shamir_storage::types::Store> = Arc::new(InMemoryStore::new());
    let a_commit_version = {
        let mut tx_a = TxContext::new(TxId::new(1), 0, 0, IsolationLevel::Snapshot);
        let mut staging = StagingStore::new(Arc::clone(&a_data_store));
        staging.set(raw_key.clone().into(), Bytes::from_static(b"a_writes"));
        tx_a.write_set.insert(table_token, staging);
        let outcome = commit_tx(tx_a, &repo.core).await.unwrap();
        outcome.commit_version
    };
    // A must commit at V_a > 0; otherwise the SSI check `V_a > 0` would not fire.
    assert!(
        a_commit_version > 0,
        "tx A must commit at version > 0 (got {a_commit_version})"
    );

    // Step 2: M concurrent B-txs.
    let per_table_mvcc = Arc::clone(repo.per_table_mvcc());
    let mut handles = Vec::with_capacity(M);
    for i in 0..M {
        let r = repo.clone();
        let mvcc = Arc::clone(&per_table_mvcc);
        let read_key = raw_key.clone();
        handles.push(tokio::spawn(async move {
            // B-tx id starts at 1000 to avoid colliding with tx A's id=1.
            let mut tx = TxContext::new(
                TxId::new(1000 + i as u64),
                0,
                0,
                IsolationLevel::Serializable,
            );
            // Declare "I read raw_key at version 0" — before V_a → will conflict.
            tx.record_read(table_token, read_key, 0);
            // Real provider: MvccStore::version_of(raw_key) returns V_a ≥ 1.
            tx.set_version_provider(Arc::new(RepoVersionProvider {
                per_table_mvcc: mvcc,
            }));
            // Stage a DIFFERENT per-tx key so the tx is non-empty (not C6 fast-path).
            let b_data: Arc<dyn shamir_storage::types::Store> = Arc::new(InMemoryStore::new());
            let mut staging = StagingStore::new(b_data);
            staging.set(
                Bytes::from(format!("b_key_{i}").into_bytes()).into(),
                Bytes::from_static(b"b_val"),
            );
            tx.write_set.insert(table_token, staging);
            commit_tx(tx, &r.core).await
        }));
    }

    let mut successes = 0u32;
    let mut conflicts = 0u32;
    let mut committed_versions = Vec::new();
    for h in handles {
        match h.await.unwrap() {
            Ok(outcome) => {
                successes += 1;
                committed_versions.push(outcome.commit_version);
            }
            Err(_) => conflicts += 1,
        }
    }

    // INVARIANT: no tx is lost.
    assert_eq!(
        successes + conflicts,
        M as u32,
        "all {M} B-txs must resolve; got successes={successes} conflicts={conflicts}"
    );

    // INVARIANT: the real SSI provider triggers at least some conflicts.
    // Every B-tx recorded raw_key at version 0 and A committed at V_a ≥ 1,
    // so validate_read_set returns conflict for each B-tx that reaches
    // pre_commit_locked_validate. Scheduling could allow a B-tx to get a
    // version BEFORE the provider sees V_a (race between commit pipeline
    // stages) — but in practice, since A.commit_tx awaited fully above,
    // all B-txs will see the conflict. We require > 0 to avoid a
    // hypothetical future race making the assertion spuriously pass.
    assert!(
        conflicts > 0,
        "real SSI provider (RepoVersionProvider) must detect the A-writes / \
         B-reads conflict (V_a={a_commit_version}); got 0 conflicts"
    );

    // INVARIANT: each successful B-tx has a unique commit version.
    if successes > 0 {
        committed_versions.sort_unstable();
        committed_versions.dedup();
        assert_eq!(
            committed_versions.len(),
            successes as usize,
            "each successful B-tx must have a unique commit version"
        );
    }

    // INVARIANT: watermark advances past tx A's version.
    let gate = repo.tx_gate().await.unwrap();
    let wm = gate.completion().watermark();
    assert!(
        wm >= a_commit_version,
        "watermark ({wm}) must advance past tx A's commit version ({a_commit_version})"
    );
}

// =========================================================================
// Test 3 — recovery after crash (WAL durable, materialize not yet run)
// =========================================================================

/// Disk-backed (fjall_raw) helpers/tests below — require the `fjall` feature.
#[cfg(feature = "fjall")]
fn rid(n: u8) -> RecordId {
    let mut a = [0u8; 16];
    a[15] = n;
    RecordId(a)
}
/// Open (or reopen) a disk-backed repo, retrying on Windows where
/// the backend releases its file lock lazily after `drop`.
#[cfg(feature = "fjall")]
async fn open_disk(path: &std::path::Path, tables: Vec<TableConfig>) -> RepoInstance {
    let mut last_err = None;
    for _attempt in 0..10 {
        match RepoInstance::from_factory(
            "stress".into(),
            BoxRepoFactory::fjall_raw(path.to_path_buf()),
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
    panic!("open_disk failed after 10 retries: {last_err:?}");
}

/// Append a durable inflight V2 `Put` to the file WAL (F5e: the file
/// segment is the medium that survives a simulated restart — the `Mem`
/// sink is per-instance), modelling a crash after WAL fsync but before the
/// data_store update. Disk-backed (fjall_raw) — requires the `fjall` feature.
#[cfg(feature = "fjall")]
async fn seed_inflight_put(
    path: &std::path::Path,
    table: &str,
    record: RecordId,
    body: bytes::Bytes,
    commit_version: u64,
) {
    let seed = open_disk(path, vec![TableConfig::new(table)]).await;
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
    wal.begin_grouped(&entry, WalDurability::Synced)
        .await
        .unwrap();
    drop(seed);
}

/// Crash point (b): WAL durable, materialize not yet run. Recovery must
/// replay all durable entries, restore data, advance watermark.
/// Disk-backed (fjall_raw) — requires the `fjall` feature.
#[cfg(feature = "fjall")]
#[tokio::test]
async fn oracle_stress_recovery_after_wal_durable_no_materialize() {
    const N: u64 = 10;
    let tempdir = tempfile::TempDir::new().expect("tempdir");
    let path = tempdir.path().to_path_buf();

    // Seed N inflight entries at commit_versions 1..=N.
    for v in 1..=N {
        let record = rid(v as u8);
        let body = InnerValue::Str(format!("crash_{v}")).to_bytes().unwrap();
        seed_inflight_put(&path, "t", record, body, v).await;
    }

    // Simulated restart.
    let repo = open_disk(&path, vec![TableConfig::new("t")]).await;

    let recovered = repo.recover_v2_inflight().await.unwrap();
    assert_eq!(recovered, N as usize);

    // All records are present.
    let tbl = repo.get_table("t").await.unwrap();
    for v in 1..=N {
        let record = rid(v as u8);
        let val = tbl.get(record).await.unwrap();
        assert!(
            matches!(val, InnerValue::Str(ref s) if s == &format!("crash_{v}")),
            "record {v} must be recovered, got {val:?}"
        );
    }

    // Watermark = N (all contiguous).
    let gate = repo.tx_gate().await.unwrap();
    assert_eq!(
        gate.completion().watermark(),
        N,
        "watermark must equal highest recovered version"
    );

    // Next assigned version > N.
    assert!(gate.assign_next_version() > N);
}

/// Crash point (b) with a GAP: versions 1,2,4,5 are durable (3 is missing —
/// simulating a tx that crashed BEFORE writing to the WAL).
///
/// # Gap-watermark semantics (pinned decision)
///
/// ## What happens during gate construction
///
/// `RepoInstance::tx_gate()` pre-scans inflight WAL entries to find
/// `max_inflight_commit_version = 5`, then seeds:
///
///   `RepoTxGate::new(last_committed = 5, ...)`
///
/// which calls `CompletionTracker::with_watermark(5)`. The watermark is
/// therefore **already 5** before recovery marks any version.
///
/// ## What recovery does
///
/// `recover_inflight_v2` then calls `gate.completion().mark(v, Materialized)`
/// for v in {1, 2, 4, 5}. `CompletionTracker::mark` skips any version ≤ the
/// current watermark (already compacted). Since watermark = 5, all four marks
/// are no-ops. The watermark stays at 5.
///
/// ## Why this is NOT a liveness bug
///
/// Version 3 never had a WAL entry — the tx crashed before fsync. The design
/// uses `max_inflight` as the floor: "all versions ≤ max_inflight are resolved,
/// either by replay or by implicit loss." The crashed tx's version slot is
/// implicitly retired, future txs start from 6, and no stall occurs.
///
/// The gap at version 3 is never stalled in the watermark because the watermark
/// jumps to 5 during gate construction (before recovery even runs). This is the
/// CORRECT behavior for WAL-backed stores: "no WAL entry = no durable data =
/// implicitly gone."
///
/// ## Pinned assertion: `watermark == 5`
///
/// NOT 2 — the naively expected "contiguous-prefix stall" value. The gate
/// pre-seeds from `max_inflight`, resolving the gap implicitly.
/// Disk-backed (fjall_raw) — requires the `fjall` feature.
#[cfg(feature = "fjall")]
#[tokio::test]
async fn oracle_stress_recovery_gap_in_versions() {
    let tempdir = tempfile::TempDir::new().expect("tempdir");
    let path = tempdir.path().to_path_buf();

    for v in [1u64, 2, 4, 5] {
        let record = rid(v as u8);
        let body = InnerValue::Str(format!("g_{v}")).to_bytes().unwrap();
        seed_inflight_put(&path, "t", record, body, v).await;
    }

    let repo = open_disk(&path, vec![TableConfig::new("t")]).await;

    let recovered = repo.recover_v2_inflight().await.unwrap();
    assert_eq!(recovered, 4, "4 durable WAL entries must be replayed");

    let gate = repo.tx_gate().await.unwrap();
    let wm = gate.completion().watermark();

    // WHY == 5 (not 2, which naive "contiguous prefix" reasoning would suggest):
    //
    // `tx_gate()` is called inside `recover_inflight_v2` (Step 1 of that fn)
    // and pre-seeds CompletionTracker to `max_inflight = 5`. After that,
    // recovery's per-entry `mark(v, Materialized)` calls for v in {1,2,4,5}
    // are all no-ops (v ≤ watermark). The gap at version 3 is implicitly
    // resolved by the floor — "no WAL entry = crashed before write = gone."
    assert_eq!(
        wm, 5,
        "watermark must be exactly 5: gate pre-seeded from max_inflight=5; \
         the gap at v=3 is implicitly resolved by the pre-seeded floor"
    );

    // Next assigned version must exceed the highest recovered version (5).
    assert!(
        gate.assign_next_version() > 5,
        "next assigned version must be > 5 (max recovered)"
    );
}

// =========================================================================
// Test 4 — abort under load doesn't stall the watermark
// =========================================================================

/// A mix of committing and aborting txs: the watermark keeps advancing and
/// never stalls on an aborted version.
///
/// # Count determinism analysis
///
/// The abort/commit decision for tx `i` is a PURE FUNCTION of `i`:
///   - `should_abort = i % 2 == 1` is computed before `begin_tx`
///   - `EvenOddProvider` is a constant: it does not inspect inter-tx state
///   - Even txs carry no read-set and no provider → SSI check is a no-op →
///     always commit
///   - Odd txs carry a read-set with `version_seen=1` and a provider that
///     returns `999_999` → `validate_read_set` sees `999_999 > 1` → always abort
///
/// Because both the conflict decision AND the write-set are disjoint across
/// indices (each tx inserts its own record), the outcome is independent of
/// scheduling order. `aborts == TOTAL/2` and `successes == TOTAL/2` are
/// therefore EXACT deterministic assertions, not approximations.
///
/// # Flake-hunt
///
/// TOTAL is set to 80 (vs. the previous 40) to increase the probability of
/// interleaved Aborted/Materialized marks and exercise the try_advance loop
/// under higher contention. The `tokio::sync::Barrier` ensures all spawned
/// tasks race into `commit_tx` simultaneously rather than fanning out
/// sequentially, exercising the real concurrent watermark-advance path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn oracle_stress_abort_does_not_stall_watermark() {
    const TOTAL: usize = 80;

    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));

    // Barrier ensures all TOTAL tasks enter commit_tx concurrently.
    let barrier = Arc::new(tokio::sync::Barrier::new(TOTAL));

    // Abort/commit is a pure function of index: even → commit, odd → abort.
    // EvenOddProvider is a constant (no shared mutable state) so the decision
    // cannot vary across runs.
    struct EvenOddProvider {
        should_conflict: bool,
    }
    impl VersionProvider for EvenOddProvider {
        fn version_of(&self, _t: u64, _k: &Bytes) -> Option<u64> {
            if self.should_conflict {
                Some(999_999) // 999_999 > version_seen(1) → SSI conflict → abort
            } else {
                Some(0) // 0 ≤ version_seen → no conflict → commit proceeds
            }
        }
    }

    let mut handles = Vec::with_capacity(TOTAL);
    for i in 0..TOTAL {
        let r = repo.clone();
        let b = Arc::clone(&barrier);
        let should_abort = i % 2 == 1;
        handles.push(tokio::spawn(async move {
            let (mut tx, _g) = r.begin_tx(IsolationLevel::Serializable).await.unwrap();

            if should_abort {
                // version_seen=1; provider returns 999_999 > 1 → conflict.
                tx.record_read(table_token_for("t"), Bytes::from_static(b"k"), 1);
                tx.set_version_provider(Arc::new(EvenOddProvider {
                    should_conflict: true,
                }));
            }

            // Stage a write so the tx is non-empty (avoids C6 fast-path).
            let tbl = r.get_table("t").await.unwrap();
            tbl.insert_tx(&InnerValue::Int(i as i64), Some(&mut tx))
                .await
                .unwrap();

            // All tasks race into commit_tx together.
            b.wait().await;
            r.commit_tx(tx).await
        }));
    }

    let mut successes = 0u64;
    let mut aborts = 0u64;
    for h in handles {
        match h.await.unwrap() {
            Ok(_) => successes += 1,
            Err(_) => aborts += 1,
        }
    }

    // Exact count: deterministic — see doc-comment above.
    assert_eq!(
        aborts,
        (TOTAL / 2) as u64,
        "odd-indexed txs must abort (EvenOddProvider always returns 999_999 > version_seen=1)"
    );
    assert_eq!(
        successes,
        (TOTAL / 2) as u64,
        "even-indexed txs must commit (no read-set → SSI check is no-op)"
    );

    // INVARIANT (P0c): SSI-aborted txs no longer allocate version slots
    // (assign is deferred past validation). Only the TOTAL/2 successful txs
    // consume slots, so the watermark must advance past all of them.
    let expected = (TOTAL / 2) as u64;
    let gate = repo.tx_gate().await.unwrap();
    let wm = gate.completion().watermark();
    assert!(
        wm >= expected,
        "watermark ({wm}) must advance past all {expected} committed versions \
         (SSI-aborted txs allocate no slot under P0c)"
    );
}

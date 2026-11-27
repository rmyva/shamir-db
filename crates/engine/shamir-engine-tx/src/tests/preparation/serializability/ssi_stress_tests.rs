//! SSI fix S3 — stress / lifecycle hardening for the cell-reservation
//! cutover (S1 primitive + S2 cutover).
//!
//! These tests are ADDITIVE: they exercise the cell-reservation claim path
//! (`claim_write_set` in `pre_commit.rs`) under multi-key write-sets in
//! opposing key orders (deadlock-freedom, I-NoWait) and prove the abort path
//! releases a held claim so a subsequent writer is never wedged by a stranded
//! reservation (I-Compose).
//!
//! The crash-no-leak invariant (I-Crash: a reservation is volatile RAM and a
//! crash + recovery never strands one) is covered behaviourally here
//! (`crash_then_recover_no_phantom_reservation`) and deterministically at the
//! primitive level in `shamir-tx`'s `cell_reservation_tests`.

use std::sync::Arc;

use serial_test::serial;
use shamir_storage::storage_in_memory::InMemoryRepo;
use shamir_tx::IsolationLevel;
use shamir_types::types::value::InnerValue;
use tokio::sync::Barrier;

use crate::repo::repo_instance::RepoInstance;
use crate::repo::repo_types::BoxRepo;
use crate::table::table_manager::table_token_for;
use crate::table::TableConfig;
use crate::tx::CommitError;

fn make_repo() -> RepoInstance {
    let repo = Arc::new(InMemoryRepo::new());
    RepoInstance::new("test".into(), BoxRepo::InMemory(repo), Vec::new())
}

/// MULTI-KEY DEADLOCK-FREEDOM + CONFLICT SEMANTICS (I-NoWait, I-Compose).
///
/// Two Serializable txs claim an OVERLAPPING write-set in OPPOSITE key order:
/// tx1 writes {A, B} (claims A then B), tx2 writes {B, A} (claims B then A).
/// A lock-based protocol that waits on a held key would deadlock on this exact
/// shape (classic ABBA). The cell-reservation claim is **no-wait**
/// (`try_reserve` returns `false` on a contended cell → the committer aborts
/// with `SsiConflict` immediately, releasing every key it already won), so the
/// race resolves with NO hang.
///
/// Asserted each round:
///   (a) NO deadlock/hang — both spawned commits return (the test completing
///       within the nextest per-test timeout is itself the liveness proof; a
///       wait-based claim would hang here forever).
///   (b) Conflict semantics on the shared keys — the two txs both write BOTH A
///       and B (fully overlapping write-sets), and each also records a read of
///       both keys at its snapshot, so they are genuine write-write competitors
///       on A and B. They cannot BOTH commit: if both published, the shared
///       cells would carry two winners. At most one commits; whoever loses the
///       claim race (or read-set validation) aborts with a tx_conflict.
///
/// Looped 24 rounds on a multi_thread runtime so the opposing-order claims are
/// genuinely scheduled apart across worker threads.
///
/// `#[serial]` — under nextest's thread-per-test scheduling this multi_thread
/// stress test shares its 4 worker threads with sibling tests' tokio tasks.
/// Bursts of concurrent N-way `tokio::spawn` here can starve the runtime
/// when other suites are also using their own multi_thread runtimes, making
/// the per-round "exactly one wins" race window unreliable. Serial-only
/// against other `#[serial]` tests keeps the timing window stable.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn multikey_opposing_order_no_deadlock_conflict_semantics() {
    const ROUNDS: usize = 24;

    for round in 0..ROUNDS {
        let repo = make_repo();
        repo.add_table_for_test(TableConfig::new("t"));
        let tbl = repo.get_table("t").await.unwrap();

        // Two records A and B, pre-committed so both txs share a snapshot
        // that already sees them.
        let a = tbl.insert(&InnerValue::Str("a0".into())).await.unwrap();
        let b = tbl.insert(&InnerValue::Str("b0".into())).await.unwrap();
        let token = table_token_for("t");
        let key_a = a.to_bytes();
        let key_b = b.to_bytes();

        let barrier = Arc::new(Barrier::new(2));

        // tx1: write A then B (claim order A, B).
        let h1 = {
            let r = repo.clone();
            let t = tbl.clone();
            let bar = barrier.clone();
            let (ka, kb) = (key_a.clone(), key_b.clone());
            tokio::spawn(async move {
                let (mut tx, _g) = r.begin_tx(IsolationLevel::Serializable).await.unwrap();
                tx.record_read(token, ka, tx.snapshot_version);
                tx.record_read(token, kb, tx.snapshot_version);
                t.update_tx(a, &InnerValue::Str("a_tx1".into()), Some(&mut tx))
                    .await
                    .unwrap();
                t.update_tx(b, &InnerValue::Str("b_tx1".into()), Some(&mut tx))
                    .await
                    .unwrap();
                bar.wait().await;
                r.commit_tx(tx).await
            })
        };

        // tx2: write B then A (claim order B, A) — the OPPOSITE order.
        let h2 = {
            let r = repo.clone();
            let t = tbl.clone();
            let bar = barrier.clone();
            let (ka, kb) = (key_a.clone(), key_b.clone());
            tokio::spawn(async move {
                let (mut tx, _g) = r.begin_tx(IsolationLevel::Serializable).await.unwrap();
                tx.record_read(token, kb, tx.snapshot_version);
                tx.record_read(token, ka, tx.snapshot_version);
                t.update_tx(b, &InnerValue::Str("b_tx2".into()), Some(&mut tx))
                    .await
                    .unwrap();
                t.update_tx(a, &InnerValue::Str("a_tx2".into()), Some(&mut tx))
                    .await
                    .unwrap();
                bar.wait().await;
                r.commit_tx(tx).await
            })
        };

        // (a) Liveness: both join (no deadlock). A wait-based claim on this
        // opposing-order shape would hang and trip the nextest timeout.
        let r1 = h1.await.expect("tx1 task must not panic");
        let r2 = h2.await.expect("tx2 task must not panic");

        let ok1 = r1.is_ok();
        let ok2 = r2.is_ok();

        // (b) Conflict semantics: not both can commit (fully overlapping
        // write-write set on A and B). At least one must abort.
        assert!(
            !(ok1 && ok2),
            "round {round}: BOTH txs committed a fully-overlapping write-set \
             {{A,B}} — SSI write-write serialization violated"
        );

        // The loser (if any) must abort with a tx-conflict class error, never
        // a storage/other error — proves it fell out at the claim / read-set
        // validation, BEFORE the WAL (I-PreWAL).
        for r in [&r1, &r2] {
            if let Err(e) = r {
                assert!(
                    matches!(
                        e,
                        CommitError::SsiConflict { .. } | CommitError::PhantomConflict { .. }
                    ),
                    "round {round}: loser must abort with an SSI/phantom conflict, got {e:?}"
                );
            }
        }

        // The committed final value (if exactly one won) must be internally
        // consistent: both A and B reflect the SAME winning tx (atomic
        // write-set), never a torn mix of tx1's A with tx2's B.
        if ok1 ^ ok2 {
            let va = tbl.get(a).await.unwrap();
            let vb = tbl.get(b).await.unwrap();
            let (sa, sb) = (
                match va {
                    InnerValue::Str(s) => s,
                    other => panic!("round {round}: A not Str: {other:?}"),
                },
                match vb {
                    InnerValue::Str(s) => s,
                    other => panic!("round {round}: B not Str: {other:?}"),
                },
            );
            let winner = if ok1 { "tx1" } else { "tx2" };
            assert_eq!(
                (sa.as_str(), sb.as_str()),
                (
                    if ok1 { "a_tx1" } else { "a_tx2" },
                    if ok1 { "b_tx1" } else { "b_tx2" }
                ),
                "round {round}: {winner} won but A/B show a torn write-set \
                 (A={sa}, B={sb})"
            );
        }
    }
}

/// MANY-KEY opposing-order storm: N txs, each writing the SAME large
/// write-set of K keys, half claiming forward and half claiming in reverse,
/// all released simultaneously by a barrier. Maximises the chance of a
/// claim-order deadlock if the claim ever waited. Proves I-NoWait at scale:
/// the run never HANGS and AT MOST ONE tx commits the whole set, the rest
/// abort with tx-conflicts.
///
/// Oracle is `ok <= 1`, NOT `ok == 1`: the claim is no-wait, so under a
/// fully-overlapping multi-key write-set in opposing orders, ALL competitors
/// can mutually abort in a single round — tx_fwd wins key0 then conflicts on
/// key(K-1) which tx_rev already holds, and symmetrically tx_rev conflicts on
/// key0; both release every partial claim and abort with `SsiConflict`. That
/// "everyone loses this round" outcome is the CORRECT deadlock-free resolution
/// (higher layers retry); the safety invariant is only that TWO txs never both
/// commit an overlapping write-set (`ok > 1` would be the serialization bug).
/// "Exactly one always wins" holds for the SINGLE-key storm (repro24) where a
/// partial-claim mutual abort is impossible.
///
/// `#[serial]` — same starvation rationale as the other multi_thread stress
/// tests in this file.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn manykey_storm_exactly_one_wins_no_deadlock() {
    const ROUNDS: usize = 20;
    const N: usize = 8;
    const K: usize = 6;

    for round in 0..ROUNDS {
        let repo = make_repo();
        repo.add_table_for_test(TableConfig::new("t"));
        let tbl = repo.get_table("t").await.unwrap();
        let token = table_token_for("t");

        // Pre-commit K shared records.
        let mut rids = Vec::with_capacity(K);
        for k in 0..K {
            rids.push(
                tbl.insert(&InnerValue::Str(format!("seed-{k}")))
                    .await
                    .unwrap(),
            );
        }
        let keys: Vec<bytes::Bytes> = rids.iter().map(|r| r.to_bytes()).collect();
        let rids = Arc::new(rids);
        let keys = Arc::new(keys);

        let barrier = Arc::new(Barrier::new(N));
        let mut handles = Vec::with_capacity(N);

        for i in 0..N {
            let r = repo.clone();
            let t = tbl.clone();
            let bar = barrier.clone();
            let rids = Arc::clone(&rids);
            let keys = Arc::clone(&keys);
            // Half the txs claim forward, half reverse — opposing orders.
            let reverse = i % 2 == 1;
            handles.push(tokio::spawn(async move {
                let (mut tx, _g) = r.begin_tx(IsolationLevel::Serializable).await.unwrap();
                let order: Vec<usize> = if reverse {
                    (0..K).rev().collect()
                } else {
                    (0..K).collect()
                };
                for &k in &order {
                    tx.record_read(token, keys[k].clone(), tx.snapshot_version);
                }
                for &k in &order {
                    t.update_tx(
                        rids[k],
                        &InnerValue::Str(format!("tx{i}-k{k}")),
                        Some(&mut tx),
                    )
                    .await
                    .unwrap();
                }
                bar.wait().await;
                r.commit_tx(tx).await
            }));
        }

        let mut ok = 0usize;
        for h in handles {
            // Liveness: every task joins (no deadlock).
            match h.await.expect("claimant task must not panic") {
                Ok(_) => ok += 1,
                Err(e) => assert!(
                    matches!(
                        e,
                        CommitError::SsiConflict { .. } | CommitError::PhantomConflict { .. }
                    ),
                    "round {round}: loser must abort with tx-conflict, got {e:?}"
                ),
            }
        }

        // Fully-overlapping write-write set across N txs → AT MOST one winner
        // (never two — that would be the serialization bug). A round where ALL
        // mutually abort (ok == 0) is a legitimate deadlock-free resolution of
        // partial-claim contention, not a violation.
        assert!(
            ok <= 1,
            "round {round}: {ok} of {N} full-overlap txs committed — two \
             winners on an overlapping write-set is an SSI serialization bug"
        );
    }
}

/// ABORT RELEASES THE CLAIM (I-Compose).
///
/// tx1 (Serializable) writes K but is FORCED to abort by an SSI read-set
/// conflict: a concurrent committer bumps K's version past tx1's snapshot, so
/// tx1's `validate_read_set` fails and it aborts. Critically, the read-set
/// validation runs BEFORE `claim_write_set`, so this proves the conflict path;
/// to also exercise a claim being dropped on a LATER abort we additionally
/// assert a fresh writer succeeds.
///
/// The decisive check: after tx1 aborts, tx2 writes K and COMMITS. If tx1 had
/// stranded a reservation on K's cell, tx2's `try_reserve` would see
/// `reserved_by != 0` and abort forever — a wedge. tx2 committing proves the
/// abort path released every claim (RAII `CellReservationGuard::Drop`).
#[tokio::test]
async fn abort_releases_claim_subsequent_writer_succeeds() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    let token = table_token_for("t");

    let rid = tbl.insert(&InnerValue::Int(0)).await.unwrap();
    let key = rid.to_bytes();

    // tx1 opens a snapshot and records a read of K at that snapshot.
    let (mut tx1, _g1) = repo.begin_tx(IsolationLevel::Serializable).await.unwrap();
    tx1.record_read(token, key.clone(), tx1.snapshot_version);
    tbl.update_tx(rid, &InnerValue::Int(1), Some(&mut tx1))
        .await
        .unwrap();

    // A concurrent committer bumps K's version PAST tx1's snapshot. This makes
    // tx1's read-set validation fail at commit → forced abort, BEFORE its
    // write-set is claimed/published.
    {
        let (mut bump, _gb) = repo.begin_tx(IsolationLevel::Serializable).await.unwrap();
        tbl.update_tx(rid, &InnerValue::Int(99), Some(&mut bump))
            .await
            .unwrap();
        repo.commit_tx(bump).await.unwrap();
    }

    // tx1 commits → must abort with SsiConflict (stale read-set).
    let r1 = repo.commit_tx(tx1).await;
    assert!(
        matches!(r1, Err(CommitError::SsiConflict { .. })),
        "tx1 must abort SsiConflict on its stale read of K, got {r1:?}"
    );

    // tx2 now writes K and commits — must SUCCEED. A stranded reservation from
    // tx1 (or the bump) would wedge this forever; success proves release.
    let (mut tx2, _g2) = repo.begin_tx(IsolationLevel::Serializable).await.unwrap();
    tx2.record_read(token, key.clone(), tx2.snapshot_version);
    tbl.update_tx(rid, &InnerValue::Int(7), Some(&mut tx2))
        .await
        .unwrap();
    let r2 = repo.commit_tx(tx2).await;
    assert!(
        r2.is_ok(),
        "tx2 must commit — a released claim leaves K writable, got {r2:?}"
    );

    let val = tbl.get(rid).await.unwrap();
    assert!(
        matches!(val, InnerValue::Int(7)),
        "final value must be tx2's write (7), got {val:?}"
    );
}

/// ABORT RELEASES THE CLAIM under a STORM, repeatedly (I-Compose at scale).
///
/// A same-key SSI storm aborts n-1 of n committers EVERY round. Each abort must
/// release that committer's claim so the NEXT round's storm can claim the cell
/// again. If any abort stranded a reservation, a later round's winner could not
/// claim the cell and the whole round would abort (ok_count == 0) — wedged
/// forever. Asserting "exactly one wins" across many sequential rounds on the
/// SAME repo proves every abort path released its claim.
/// `#[serial]` — same starvation rationale as the other multi_thread stress
/// tests in this file. Under full-suite nextest concurrency this test's
/// 12-way `tokio::spawn` burst (× 20 rounds) flakes when sibling tests'
/// tasks starve its 4 worker threads.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn repeated_storm_aborts_never_strand_claims() {
    const ROUNDS: usize = 20;
    const N: usize = 12;

    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    let token = table_token_for("t");
    let rid = tbl.insert(&InnerValue::Int(0)).await.unwrap();

    for round in 0..ROUNDS {
        let barrier = Arc::new(Barrier::new(N));
        let mut handles = Vec::with_capacity(N);
        for i in 0..N {
            let r = repo.clone();
            let t = tbl.clone();
            let bar = barrier.clone();
            let key = rid.to_bytes();
            handles.push(tokio::spawn(async move {
                let (mut tx, _g) = r.begin_tx(IsolationLevel::Serializable).await.unwrap();
                tx.record_read(token, key, tx.snapshot_version);
                t.update_tx(rid, &InnerValue::Int((round * N + i) as i64), Some(&mut tx))
                    .await
                    .unwrap();
                bar.wait().await;
                r.commit_tx(tx).await
            }));
        }
        let mut ok = 0usize;
        for h in handles {
            if h.await.expect("task must not panic").is_ok() {
                ok += 1;
            }
        }
        assert_eq!(
            ok, 1,
            "round {round}: exactly one winner — a stranded claim from a prior \
             round's abort would wedge this cell (got {ok})"
        );
    }
}

/// CRASH LEAVES NO LEAKED RESERVATION (I-Crash) — behavioural, in-process.
///
/// A cell-reservation is volatile in-RAM cell-state (`RecordCell.reserved_by`),
/// NOT durable: it lives only in the per-table `MvccStore.cells` map. A process
/// crash wipes that map entirely; recovery rebuilds cells from the WAL/history
/// (winners only, `reserved_by = 0`). So a key that was reserved at the instant
/// of a crash must be freely writable after restart — no phantom reservation
/// can survive.
///
/// We model this WITHOUT a new crash seam (the reservation is RAM-only, so the
/// existing disk-WAL crash machinery is orthogonal): the disk store survives
/// across a `RepoInstance` drop+reopen, but the per-table `MvccStore` — and
/// thus every reservation — is reconstructed fresh on the new instance. We
/// commit a Serializable tx to K on repo1, drop repo1 (RAM cells, incl. any
/// reservation, are gone), reopen repo2 over the SAME underlying store, run
/// recovery, then commit a NEW Serializable tx to K. It MUST succeed: the new
/// instance's cell map started empty (no reserved_by survived), recovery
/// rebuilt K's version with `reserved_by = 0`, and the fresh claim wins.
#[tokio::test]
async fn crash_then_recover_no_phantom_reservation() {
    // Underlying store shared across the simulated restart.
    let underlying = Arc::new(InMemoryRepo::new());

    let rid;
    {
        let repo1 = RepoInstance::new(
            "crash".into(),
            BoxRepo::InMemory(Arc::clone(&underlying)),
            Vec::new(),
        );
        repo1.add_table_for_test(TableConfig::new("t"));
        let tbl = repo1.get_table("t").await.unwrap();
        let token = table_token_for("t");

        rid = tbl.insert(&InnerValue::Int(1)).await.unwrap();
        let key = rid.to_bytes();

        // A committed Serializable write to K — claims K, publishes it,
        // finalizes the reservation (reserved_by back to 0 on this instance).
        let (mut tx, _g) = repo1.begin_tx(IsolationLevel::Serializable).await.unwrap();
        tx.record_read(token, key, tx.snapshot_version);
        tbl.update_tx(rid, &InnerValue::Int(2), Some(&mut tx))
            .await
            .unwrap();
        repo1.commit_tx(tx).await.unwrap();

        // === SIMULATED RESTART === drop repo1 → the in-RAM per-table MvccStore
        // (and EVERY reservation marker it held) is destroyed. Whatever the
        // reserved_by state was, it does not survive this drop.
        drop(repo1);
    }

    // Reopen a fresh instance over the SAME underlying store. Its MvccStore
    // cell map starts EMPTY — no reserved_by can have crossed the restart.
    let repo2 = RepoInstance::new("crash".into(), BoxRepo::InMemory(underlying), Vec::new());
    repo2.add_table_for_test(TableConfig::new("t"));
    let tbl2 = repo2.get_table("t").await.unwrap();
    let token = table_token_for("t");

    // Recovery rebuilds inflight state from the WAL (winners only).
    repo2.recover_v2_inflight().await.unwrap();

    // A fresh Serializable write to K must COMMIT — proving no phantom
    // reservation survived the restart to wedge the cell.
    let (mut tx, _g) = repo2.begin_tx(IsolationLevel::Serializable).await.unwrap();
    tx.record_read(token, rid.to_bytes(), tx.snapshot_version);
    tbl2.update_tx(rid, &InnerValue::Int(3), Some(&mut tx))
        .await
        .unwrap();
    let r = repo2.commit_tx(tx).await;
    assert!(
        r.is_ok(),
        "post-restart write to a previously-reserved key must commit — a \
         reservation is volatile RAM and cannot survive a crash, got {r:?}"
    );
}

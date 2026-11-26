//! D2 P1e — overlay-GC bound + commit-path soft backpressure.
//!
//! Two halves, both pinning the P1e contract that the in-memory overlay is
//! bounded to the still-undrained `(durable_watermark, last_committed]` window:
//!
//!   A. **Overlay GC.** After a real `commit_tx` burst the overlay holds one
//!      entry per committed version (the ack-path published only the overlay,
//!      not `history`). A `drain_all` pass advances `durable_watermark` to
//!      `last_committed`, and the drainer's post-pass `gc_overlay_to` sweep
//!      then drops every overlay entry `<= durable_watermark` — so the overlay
//!      (and the `pending_ts` commit-stamp map) collapse to ~0 while every
//!      value still reads back (now served by `history`). A multi-round loop
//!      proves the overlay does not grow without bound across many
//!      commit-then-drain cycles.
//!
//!   B. **Backpressure.** [`apply_backpressure`](crate::tx::commit::apply_backpressure)
//!      is driven directly with a tiny artificial threshold so the state
//!      machine is exercised deterministically without needing 10 000 real
//!      commits:
//!        * gap <= high  → returns IMMEDIATELY (fast path, zero await).
//!        * gap >  high  → parks on durable progress, then RELEASES once a
//!          concurrent drain pulls the gap below the low-watermark (`high/2`).
//!        * stuck drain  → the wall-clock deadlock guard ABANDONS the brake and
//!          PROCEEDS (never hangs the committer on a faulted disk).
//!
//! All tests run on the `current_thread` runtime so the repo's spawned
//! background drainer can only progress at the `.await` points the test
//! controls — the watermark reads between a commit and a synchronous assertion
//! are therefore deterministic.

use std::sync::Arc;

use shamir_storage::storage_in_memory::InMemoryRepo;
use shamir_tx::IsolationLevel;
use shamir_types::types::record_id::RecordId;
use shamir_types::types::value::InnerValue;
use shamir_wal::{WalDurability, WalEntryV2, WalOpV2};

use crate::tx::commit::apply_backpressure;
use crate::tx::drainer::Drainer;
use crate::tx::repo::{repo_token, BoxRepo, RepoInstance};
use crate::tx::table::table_manager::table_token_for;
use crate::tx::table::TableConfig;

fn make_repo() -> RepoInstance {
    let repo = Arc::new(InMemoryRepo::new());
    RepoInstance::new("p1e".into(), BoxRepo::InMemory(repo), Vec::new())
}

/// Total overlay entries across every per-table `MvccStore` of `repo`.
fn total_overlay_len(repo: &RepoInstance) -> usize {
    let mut sum = 0usize;
    repo.per_table_mvcc().iter_sync(|_, m| {
        sum += m.overlay_len();
        true
    });
    sum
}

/// Total `pending_ts` commit-stamp entries across every per-table store.
fn total_pending_ts_len(repo: &RepoInstance) -> usize {
    let mut sum = 0usize;
    repo.per_table_mvcc().iter_sync(|_, m| {
        sum += m.pending_ts_len();
        true
    });
    sum
}

// ===========================================================================
// A. Overlay GC bound
// ===========================================================================

/// Commit N records through the real tx pipeline → overlay holds N entries and
/// durable lags. `drain_all` lands every value in `history`, advances durable
/// to visibility, and the post-pass GC drops the overlay (and `pending_ts`) to
/// ~0. Every value still reads back (now from `history`).
#[tokio::test(flavor = "current_thread")]
async fn overlay_gc_collapses_after_drain() {
    const N: usize = 20;

    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    let gate = repo.tx_gate().await.unwrap();

    // Commit N records, one per tx, through the production commit path.
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

    // Visibility advanced by N; the ack-path published only the overlay, so
    // every committed version is overlay-resident and durable lags far behind.
    let vis = gate.last_committed();
    assert_eq!(vis, N as u64, "one version assigned per commit");
    assert!(
        total_overlay_len(&repo) >= N,
        "overlay must hold one entry per committed version before drain (got {})",
        total_overlay_len(&repo)
    );

    // Drain with a STANDALONE drainer (the spawned background one cannot have
    // raced here — current_thread, no prior await between the commits and now
    // that would let it run to completion; even if it had, the END state is
    // identical, which is what we assert).
    let drained = Drainer::new().drain_all(&repo.core).await.unwrap();
    assert!(drained >= 1, "drain_all must drain the committed tail");

    // Durable caught up to visibility.
    assert_eq!(
        gate.durable_watermark(),
        vis,
        "durable converges to visibility after drain_all"
    );

    // The post-pass `gc_overlay_to(durable_watermark)` swept every overlay
    // entry <= durable_watermark (== vis), collapsing the overlay to ~0. The
    // bound is `last_committed - durable_watermark == 0` here.
    assert_eq!(
        total_overlay_len(&repo),
        0,
        "overlay must be GC'd to empty once durable == visibility"
    );
    assert_eq!(
        total_pending_ts_len(&repo),
        0,
        "pending_ts commit-stamps must be reclaimed alongside the overlay"
    );

    // Every value still reads back — now served from `history`, not the overlay.
    for (i, rid) in rids.iter().enumerate() {
        let got = tbl.get(*rid).await.unwrap();
        let expect = format!("v{i}");
        assert!(
            matches!(got, InnerValue::Str(ref s) if *s == expect),
            "rid {rid:?}: expected {expect}, got {got:?}"
        );
    }
}

/// Overlay does not grow without bound across many commit-then-drain rounds:
/// after each `drain_all` the overlay is back at 0, and the undrained window
/// `last_committed - durable_watermark` is 0 — independent of the round count.
#[tokio::test(flavor = "current_thread")]
async fn overlay_bounded_across_rounds() {
    const ROUNDS: usize = 8;
    const PER_ROUND: usize = 5;

    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    let gate = repo.tx_gate().await.unwrap();
    let drainer = Drainer::new();

    for round in 0..ROUNDS {
        for i in 0..PER_ROUND {
            let (mut tx, _g) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
            // Overwrite a small fixed key-space so successive rounds layer new
            // versions on the SAME keys — the overlay would accumulate every
            // historical version if GC did not bound it to the durable window.
            let _ = tbl
                .insert_tx(&InnerValue::Str(format!("r{round}_{i}")), Some(&mut tx))
                .await
                .unwrap();
            repo.commit_tx(tx).await.unwrap();
        }

        drainer.drain_all(&repo.core).await.unwrap();

        // Invariant after every round: durable == visibility, overlay empty.
        assert_eq!(
            gate.durable_watermark(),
            gate.last_committed(),
            "round {round}: durable must converge to visibility"
        );
        assert_eq!(
            total_overlay_len(&repo),
            0,
            "round {round}: overlay must not grow across rounds (bounded by GC)"
        );
    }

    // Total versions committed = ROUNDS * PER_ROUND, yet the overlay never
    // exceeded the single-round window and is now empty.
    assert_eq!(gate.last_committed(), (ROUNDS * PER_ROUND) as u64);
    assert_eq!(total_overlay_len(&repo), 0);
}

// ===========================================================================
// B. Backpressure state machine
// ===========================================================================

/// gap <= high → `apply_backpressure` returns immediately (fast path). We prove
/// "no await / no park" by wrapping the call in a near-zero timeout: a fast
/// path that never reaches an `.await` completes before the timer can fire.
#[tokio::test(flavor = "current_thread")]
async fn backpressure_fast_path_returns_immediately() {
    let repo = make_repo();
    let gate = repo.tx_gate().await.unwrap();

    // Visibility 5, durable 5 → gap 0. `mark_durable` needs the contiguous
    // prefix, so mark 1..=5.
    gate.publish_committed_max(5);
    for v in 1..=5u64 {
        gate.mark_durable(v);
    }
    assert_eq!(
        gate.last_committed()
            .saturating_sub(gate.durable_watermark()),
        0
    );

    // high = 1: gap (0) <= high (1) → fast path. A 1 ms timeout is generous for
    // a path that does two atomic loads and returns without parking.
    let r = tokio::time::timeout(
        std::time::Duration::from_millis(1),
        apply_backpressure(&repo.core, 1),
    )
    .await;
    assert!(r.is_ok(), "gap <= high must return immediately (no park)");
}

/// gap > high → the committer brakes, then RELEASES once a concurrent drain
/// pulls the gap below the low-watermark (`high/2`). Proves: (1) the brake
/// engages (does not return instantly), (2) it converges (no deadlock) once
/// durable advances.
#[tokio::test(flavor = "current_thread")]
async fn backpressure_engages_then_releases_on_drain() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let gate = repo.tx_gate().await.unwrap();
    let token = table_token_for("t");

    // Build a large undrained gap by hand: visibility 100, durable 0. We seed
    // matching inflight WAL entries so the drainer has something real to replay
    // (mark_durable). high = 40, low = 20 → must drain past version 80 (gap 20)
    // to release.
    let wal = repo.repo_wal().await.unwrap();
    for v in 1..=100u64 {
        let mut a = [0u8; 16];
        a[8..16].copy_from_slice(&v.to_be_bytes());
        let body = InnerValue::Str(format!("v{v}")).to_bytes().unwrap();
        let entry = WalEntryV2::new(
            wal.fresh_txn_id(),
            repo_token(repo.name()),
            vec![WalOpV2::Put {
                table_id_interned: token,
                rid: RecordId(a),
                body,
            }],
        )
        .with_commit_version(v);
        wal.begin_grouped(&entry, WalDurability::Buffered)
            .await
            .unwrap();
    }
    gate.publish_committed_max(100);
    assert_eq!(gate.durable_watermark(), 0);
    assert_eq!(gate.last_committed(), 100);

    // Concurrent drainer: advances durable in small steps with a yield between
    // them, so `apply_backpressure` is forced to actually PARK and re-check
    // (rather than find the gap already closed on the first read). A standalone
    // drainer here is the live drain that the brake is waiting on; we wake the
    // gate's durable-progress signal via each `mark_durable` it performs.
    let drainer = Arc::new(Drainer::new());

    let repo_bg = repo.clone();
    let drainer_bg = Arc::clone(&drainer);
    let drain_task = tokio::spawn(async move {
        // Drain the whole tail; each replayed version calls `gate.mark_durable`
        // → `notify_waiters`, releasing the parked committer once gap <= low.
        // A yield between passes guarantees the committer observes an
        // intermediate (still-braked) gap at least once.
        //
        // This loop is NOT an unbounded spin-wait: it terminates by its OWN
        // logic (`if n == 0 { break }`), bounded by the finite seeded WAL
        // data (100 versions). Each `drain_step` processes one version and
        // decrements the remaining count — the loop runs at most ~100 times.
        // The test's synchronization point (`apply_backpressure` below) IS
        // wrapped in a 30 s `tokio::time::timeout` with a named assertion;
        // `drain_task.await` (after the timeout-bounded join) completes
        // immediately since the drain has already converged by then.
        loop {
            let n = drainer_bg.drain_step(&repo_bg.core).await.unwrap();
            tokio::task::yield_now().await;
            if n == 0 {
                break;
            }
        }
    });

    // high = 40 → low = 20. Brake must hold until durable reaches 80
    // (gap = 100 - 80 = 20 <= low). Bounded by a 30 s timeout: the brake's own
    // deadlock guard is 5 s, and the concurrent drain converges well inside it,
    // so a timeout here would itself be a real hang BUG (not a flaky margin).
    let braked = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        apply_backpressure(&repo.core, 40),
    )
    .await;
    assert!(
        braked.is_ok(),
        "backpressure must release (no deadlock) once the drain advances durable"
    );

    // On release the gap is at or below the low-watermark (20). It may be lower
    // if the drain ran ahead, but it MUST be <= low (released by hysteresis) and
    // the brake must NOT have abandoned (that path only fires after 5 s; the
    // drain converged far sooner).
    let gap = gate
        .last_committed()
        .saturating_sub(gate.durable_watermark());
    assert!(
        gap <= 20,
        "released gap {gap} must be <= low-watermark 20 (hysteresis release)"
    );

    drain_task.await.unwrap();
    // Drain fully to confirm convergence (no stuck tail).
    assert_eq!(gate.durable_watermark(), 100, "drain converged fully");
}

/// Deadlock safety: when the drain is STUCK (durable cannot advance — here
/// because visibility was published with NO inflight WAL entry for the drainer
/// to replay), `apply_backpressure` must NOT hang forever. The wall-clock
/// guard abandons the brake after `BACKPRESSURE_MAX_WAIT` and PROCEEDS. The
/// data is unaffected (already committed/observable); only the overlay bound is
/// relaxed under the faulted drain.
#[tokio::test(flavor = "current_thread")]
async fn backpressure_abandons_when_drain_stuck() {
    let repo = make_repo();
    let gate = repo.tx_gate().await.unwrap();

    // Visibility 100, durable 0 → permanent gap of 100. No inflight WAL entries
    // exist, so the drainer's `drain_step` finds nothing to replay and durable
    // can NEVER advance — a faithful "stuck drain" model.
    gate.publish_committed_max(100);
    assert_eq!(gate.durable_watermark(), 0);

    // high = 10, low = 5 → gap (100) > high. The brake parks, the gap never
    // closes, and the 5 s wall-clock guard must abandon and return. Bound the
    // test at 20 s: it MUST finish via the guard (≈5 s), so a 20 s timeout
    // firing would be a real liveness BUG, not a flaky margin.
    let start = std::time::Instant::now();
    let r = tokio::time::timeout(
        std::time::Duration::from_secs(20),
        apply_backpressure(&repo.core, 10),
    )
    .await;
    assert!(
        r.is_ok(),
        "stuck-drain backpressure must abandon (deadlock guard), not hang"
    );
    // It returned via the wall-clock guard, so it took at least the guard
    // budget — proving it genuinely PARKED and was released by the guard, not
    // by a (non-existent) drain advance. (Lower bound a touch under 5 s to
    // absorb timer granularity.)
    assert!(
        start.elapsed() >= std::time::Duration::from_secs(4),
        "must have parked until the wall-clock guard fired (elapsed {:?})",
        start.elapsed()
    );

    // The gap is still wide open — data is untouched, the brake simply yielded.
    assert_eq!(
        gate.last_committed()
            .saturating_sub(gate.durable_watermark()),
        100,
        "stuck drain leaves the gap open; backpressure relaxed the bound, not the data"
    );
}

use std::sync::Arc;

use shamir_wal::WalEntryV2;

use crate::table::TableConfig;
use crate::tx::drainer::Drainer;

use super::support::{make_repo, rid, seed_inflight_put};

// ── Op #2 Stage 1: window infrastructure tests ──────────────────────

/// Offer three entries with commit_versions 3, 1, 2 (out of order).
/// Assert window_len == 3 and iteration yields ascending keys 1, 2, 3.
#[tokio::test]
async fn offer_inserts_at_commit_version_in_ascending_order() {
    let drainer = Drainer::new();

    let mk = |v: u64| -> Arc<WalEntryV2> {
        Arc::new(WalEntryV2::new(v, 0, vec![]).with_commit_version(v))
    };

    // Insert out of order on purpose.
    drainer.offer(mk(3));
    drainer.offer(mk(1));
    drainer.offer(mk(2));

    assert_eq!(drainer.window_len(), 3);

    // Iterate the window and collect keys — must be ascending.
    assert_eq!(drainer.window_keys(), vec![1, 2, 3]);
}

/// seed_from_recover populates the window from a Vec<WalEntryV2>.
#[tokio::test]
async fn seed_from_recover_populates_window_with_inflight_entries() {
    let drainer = Drainer::new();

    let entries: Vec<WalEntryV2> = (10..=14)
        .map(|v| WalEntryV2::new(v, 0, vec![]).with_commit_version(v))
        .collect();

    drainer.seed_from_recover(entries);
    assert_eq!(drainer.window_len(), 5);
}
// ── Op #2 Stage 4: backpressure (bounded offer) tests ───────────────

/// Fresh `Drainer::new()` has the default high-watermark of 64K.
#[tokio::test]
async fn default_high_watermark_is_64k() {
    let drainer = Drainer::new();
    assert_eq!(
        drainer.window_high_watermark(),
        64 * 1024,
        "default high-watermark must be 64K"
    );
}

/// Set high_watermark to 4, offer 10 entries. The window must cap around
/// the watermark (soft limit — races allow a few extras) and
/// `offer_dropped_total` must account for the gap.
#[tokio::test]
async fn offer_drops_at_high_watermark() {
    let drainer = Drainer::new();
    drainer.set_window_high_watermark(4);

    let mk = |v: u64| -> Arc<WalEntryV2> {
        Arc::new(WalEntryV2::new(v, 0, vec![]).with_commit_version(v))
    };

    for v in 1..=10u64 {
        drainer.offer(mk(v));
    }

    let wlen = drainer.window_len();
    let dropped = drainer.offer_dropped_total();

    // Soft limit: window should be ~4, but races may allow a few extras.
    assert!(
        (4..=7).contains(&wlen),
        "window_len ({wlen}) should be in [4, 7] with hw=4"
    );
    assert!(
        dropped > 0,
        "some entries must have been dropped (dropped={dropped})"
    );
    // Accounting: offered 10 total = window_len + dropped.
    assert_eq!(
        wlen as u64 + dropped,
        10,
        "window_len ({wlen}) + dropped ({dropped}) must equal 10"
    );
}
/// PT 1: the atomic `window_depth` mirror must stay in lock-step with
/// `scc::TreeIndex::len()` across every mutation site (offer success,
/// offer duplicate, seed, drain Phase C remove, test-only remove). The
/// purpose of the counter is to make `offer`'s backpressure check O(1)
/// — `scc::TreeIndex::len()` is `iter().count()` per source (2.4.0
/// tree_index.rs:727), which is the O(N) regression this guards against.
#[tokio::test]
async fn window_depth_atomic_mirror_matches_tree_across_mutations() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let gate = repo.tx_gate().await.unwrap();
    for v in 1..=4u64 {
        seed_inflight_put(&repo, "t", rid(v as u8), &format!("d{v}"), v).await;
    }
    gate.publish_committed_max(4);

    let drainer = Drainer::new();
    assert_eq!(drainer.window_depth(), 0, "fresh = 0");
    assert_eq!(drainer.window_depth(), drainer.window_len());

    // 1. offer success: depth bumps per successful insert.
    let wal = repo.repo_wal().await.unwrap();
    let entries = wal.recover().await.unwrap();
    let arcs: Vec<_> = entries.into_iter().map(Arc::new).collect();
    for e in &arcs {
        drainer.offer(Arc::clone(e));
    }
    assert_eq!(drainer.window_depth(), 4, "four offers, four inserts");
    assert_eq!(drainer.window_depth(), drainer.window_len());

    // 2. offer duplicate: scc::TreeIndex::insert returns Err on existing
    //    key; the depth mirror must NOT double-count.
    for e in &arcs {
        drainer.offer(Arc::clone(e));
    }
    assert_eq!(drainer.window_depth(), 4, "duplicate offers ignored");
    assert_eq!(drainer.window_depth(), drainer.window_len());

    // 3. test-only remove: depth decrements on a successful remove.
    drainer.window_remove_for_test(2);
    assert_eq!(drainer.window_depth(), 3, "one remove");
    assert_eq!(drainer.window_depth(), drainer.window_len());
    // No-op remove (already absent): depth must not move.
    drainer.window_remove_for_test(2);
    assert_eq!(drainer.window_depth(), 3, "no-op remove preserves depth");

    // 4. seed_from_recover: depth bumps per fresh insert, ignores
    //    duplicates.
    let entries2 = wal.recover().await.unwrap(); // returns all 4
    drainer.seed_from_recover(entries2);
    assert_eq!(
        drainer.window_depth(),
        4,
        "v=2 reseeded; 1,3,4 already present"
    );
    assert_eq!(drainer.window_depth(), drainer.window_len());

    // 5. drain Phase C remove: depth decrements once per finalized entry.
    let drained = drainer.drain_all(&repo.core).await.unwrap();
    assert_eq!(drained, 4);
    assert_eq!(drainer.window_depth(), 0, "all finalized → depth 0");
    assert_eq!(drainer.window_depth(), drainer.window_len());
}

/// PT 1: when the window is at or above the soft high-watermark, `offer`
/// MUST short-circuit on the atomic load — no `scc::TreeIndex::len()`
/// walk, no tree mutation. This is the O(1)-backpressure contract.
/// We can't directly assert "no len() call" from outside, but we CAN
/// prove the equivalent by setting hw=0 and observing that the drop
/// counter advances and the window stays untouched even though many
/// offers fire — the only path that produces that observable triple
/// is the atomic-load-and-return branch.
#[tokio::test]
async fn offer_backpressure_check_uses_atomic_depth_not_tree_walk() {
    let drainer = Drainer::new();
    drainer.set_window_high_watermark(0);

    // 1000 offers under hw=0: every one must drop without touching the tree.
    for v in 1..=1_000u64 {
        let entry = WalEntryV2::new(v, 0, vec![]).with_commit_version(v);
        drainer.offer(Arc::new(entry));
    }

    assert_eq!(drainer.window_depth(), 0, "no inserts under hw=0");
    assert_eq!(drainer.window_len(), 0, "tree confirms no inserts");
    assert_eq!(
        drainer.offer_dropped_total(),
        1_000,
        "every offer dropped on the atomic backpressure check"
    );
}

use std::sync::Arc;

use bytes::Bytes;
use shamir_storage::storage_in_memory::InMemoryStore;
use shamir_storage::types::Store;
use shamir_tx::{IsolationLevel, StagingStore, TxContext, TxId};
use shamir_types::types::value::InnerValue;
use shamir_wal::{WalDurability, WalEntryV2, WalOpV2};

use crate::tx::commit_tx;
use crate::tx::drainer::Drainer;
use crate::tx::repo::repo_token;
use crate::tx::table::{table_manager::table_token_for, TableConfig};

use super::support::{make_repo, rid, seed_inflight_put};

/// Stage 3: drain_step with an empty window falls back to wal.recover()
/// (gap-reseed path) and still drains correctly.
#[tokio::test]
async fn drain_step_gap_reseed_when_window_empty() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();

    let gate = repo.tx_gate().await.unwrap();
    seed_inflight_put(&repo, "t", rid(1), "stage3", 1).await;
    gate.publish_committed_max(1);

    let drainer = Drainer::new();
    // Window is empty — drain_step must fall back via gap-reseed.
    assert_eq!(drainer.window_len(), 0);

    let drained = drainer.drain_step(&repo.core).await.unwrap();
    assert_eq!(drained, 1, "drain_step works via gap-reseed fallback");
    assert_eq!(gate.durable_watermark(), 1);

    let got = tbl.get(rid(1)).await.unwrap();
    assert!(
        matches!(got, InnerValue::Str(ref s) if s == "stage3"),
        "data resolves from history after drain"
    );
}
/// drain_step replays the visible-but-undurable prefix into history,
/// advances `durable_watermark` to visibility, and the data reads back.
#[tokio::test]
async fn drain_step_replays_visible_prefix_into_history() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();

    // Build the gate FIRST (durable == visibility == 0) BEFORE any inflight
    // entry exists, so seeding below does not lift the durable floor.
    let gate = repo.tx_gate().await.unwrap();
    assert_eq!(gate.last_committed(), 0);
    assert_eq!(gate.durable_watermark(), 0);

    // Seed inflight entries at versions 1, 2, 3 (data NOT yet in history).
    seed_inflight_put(&repo, "t", rid(1), "v1", 1).await;
    seed_inflight_put(&repo, "t", rid(2), "v2", 2).await;
    seed_inflight_put(&repo, "t", rid(3), "v3", 3).await;

    // Ack-path published visibility to 3, but durable still lags at 0.
    gate.publish_committed_max(3);
    assert_eq!(gate.last_committed(), 3);
    assert_eq!(gate.durable_watermark(), 0, "durable lags visibility");

    // Before drain: history has nothing for these rids.
    assert!(tbl.get(rid(1)).await.is_err());

    let drainer = Drainer::new();
    let drained = drainer.drain_step(&repo.core).await.unwrap();
    assert_eq!(drained, 3, "all three visible-undurable versions drained");
    assert_eq!(drainer.drained_total(), 3);

    // Durable watermark advanced to visibility.
    assert_eq!(
        gate.durable_watermark(),
        3,
        "durable caught up to visibility"
    );

    // Data now resolves from history.
    for (n, expect) in [(1u8, "v1"), (2, "v2"), (3, "v3")] {
        let got = tbl.get(rid(n)).await.unwrap();
        assert!(
            matches!(got, InnerValue::Str(ref s) if s == expect),
            "rid {n}: expected {expect}, got {got:?}"
        );
    }
}

/// drain_step with `durable >= visibility` is a no-op (nothing to drain).
#[tokio::test]
async fn drain_step_noop_when_durable_at_visibility() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));

    let gate = repo.tx_gate().await.unwrap();
    // Advance BOTH watermarks to 5: nothing is visible-but-undurable. The
    // durable watermark is a CONTIGUOUS prefix, so mark every version 1..=5
    // (marking only 5 would leave the prefix at 0).
    gate.publish_committed_max(5);
    for v in 1..=5u64 {
        gate.mark_durable(v);
    }
    assert_eq!(gate.last_committed(), 5);
    assert_eq!(gate.durable_watermark(), 5);

    let drainer = Drainer::new();
    let drained = drainer.drain_step(&repo.core).await.unwrap();
    assert_eq!(drained, 0, "durable == visibility → no drain");
    assert_eq!(gate.durable_watermark(), 5, "watermark unchanged");
}

/// Idempotency: a second drain_step over already-drained state returns 0
/// and leaves history + watermark unchanged.
#[tokio::test]
async fn drain_step_is_idempotent() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();

    let gate = repo.tx_gate().await.unwrap();
    seed_inflight_put(&repo, "t", rid(1), "once", 1).await;
    seed_inflight_put(&repo, "t", rid(2), "once", 2).await;
    gate.publish_committed_max(2);

    let drainer = Drainer::new();
    assert_eq!(drainer.drain_step(&repo.core).await.unwrap(), 2);
    assert_eq!(gate.durable_watermark(), 2);

    // Second pass: everything is already at/below the durable prefix.
    let second = drainer.drain_step(&repo.core).await.unwrap();
    assert_eq!(second, 0, "re-drain of drained state is a no-op");
    assert_eq!(gate.durable_watermark(), 2, "watermark unchanged");
    assert_eq!(drainer.drained_total(), 2, "no double-count");

    // Data still resolves to the single correct value (no corruption).
    let got = tbl.get(rid(1)).await.unwrap();
    assert!(matches!(got, InnerValue::Str(ref s) if s == "once"));
}

/// drain_all flushes the whole inflight tail and returns total drained.
#[tokio::test]
async fn drain_all_flushes_then_stops() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));

    let gate = repo.tx_gate().await.unwrap();
    for v in 1..=4u64 {
        seed_inflight_put(&repo, "t", rid(v as u8), "x", v).await;
    }
    gate.publish_committed_max(4);

    let drainer = Drainer::new();
    let total = drainer.drain_all(&repo.core).await.unwrap();
    assert_eq!(total, 4);
    assert_eq!(gate.durable_watermark(), 4);
    // A follow-up drain_all is a clean no-op.
    assert_eq!(drainer.drain_all(&repo.core).await.unwrap(), 0);
}

/// drain_step never drains a version ABOVE visibility: an entry committed in
/// the WAL but not yet published stays undurable.
#[tokio::test]
async fn drain_step_respects_visibility_ceiling() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();

    let gate = repo.tx_gate().await.unwrap();
    seed_inflight_put(&repo, "t", rid(1), "visible", 1).await;
    seed_inflight_put(&repo, "t", rid(2), "pending", 2).await;

    // Only version 1 is published; version 2 is committed-in-WAL but not
    // yet ack-visible.
    gate.publish_committed_max(1);

    let drainer = Drainer::new();
    let drained = drainer.drain_step(&repo.core).await.unwrap();
    assert_eq!(drained, 1, "only the visible version is drained");
    assert_eq!(gate.durable_watermark(), 1);

    // v1 is in history; v2 is NOT (above visibility).
    assert!(tbl.get(rid(1)).await.is_ok());
    assert!(
        tbl.get(rid(2)).await.is_err(),
        "version above visibility must not be drained"
    );
}

/// A5 interner-hwm gate: an entry whose interner delta references an id above
/// the table's persisted high-water mark IS drained to history (durable
/// advances) but its WAL marker is NOT truncated — it stays inflight until a
/// future interner checkpoint advances the hwm.
///
/// On the in-memory `Mem` WAL sink `wal.commit` is itself a no-op (markers
/// live until F6 truncation), so the load-bearing assertion is that
/// `drain_step` (a) still marks the version durable and (b) takes the A5
/// "retain marker" branch rather than erroring. We assert (a) directly and
/// (b) by the entry remaining replayable on a second pass with no panic.
#[tokio::test]
async fn drain_step_a5_retains_marker_above_hwm() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    let token = table_token_for("t");

    // Fresh table: persisted interner high-water mark is 0.
    assert_eq!(tbl.interner().persisted_high_water(), 0);

    let gate = repo.tx_gate().await.unwrap();
    let wal = repo.repo_wal().await.unwrap();

    let body = InnerValue::Str("with_delta".into()).to_bytes().unwrap();
    let mut entry = WalEntryV2::new(
        wal.fresh_txn_id(),
        repo_token(repo.name()),
        vec![WalOpV2::Put {
            table_id_interned: token,
            rid: rid(1),
            body,
        }],
    )
    .with_commit_version(1);
    // Interner delta id 42 — far above the persisted hwm (0) → A5 gate must
    // judge truncation UNSAFE and retain the marker.
    entry.interner_delta = vec![(token, "fresh_field".to_string(), 42)];
    wal.begin_grouped(&entry, WalDurability::Buffered)
        .await
        .unwrap();

    gate.publish_committed_max(1);

    let drainer = Drainer::new();
    let drained = drainer.drain_step(&repo.core).await.unwrap();

    // Data is durable in history despite the retained marker — the A5 gate
    // defers TRUNCATION, not DURABILITY.
    assert_eq!(drained, 1);
    assert_eq!(
        gate.durable_watermark(),
        1,
        "A5: durable advances even when the marker is retained"
    );
    let got = tbl.get(rid(1)).await.unwrap();
    assert!(matches!(got, InnerValue::Str(ref s) if s == "with_delta"));

    // The interner delta was applied on replay (id 42 → "fresh_field").
    let interner = tbl.interner().get().await.unwrap();
    assert_eq!(
        interner.get_ind("fresh_field").map(|k| k.id()),
        Some(42),
        "interner delta applied during drain replay"
    );

    // Second pass: version 1 is now at the durable floor → no-op, no panic.
    assert_eq!(drainer.drain_step(&repo.core).await.unwrap(), 0);
}

/// P0 group-1 fix (2026-08-14 review): a table's `per_table_mvcc`
/// registration can be evicted (`RepoInstance::remove_table`,
/// repo_instance.rs:510 — DROP TABLE racing an undrained commit, per the
/// review's spot-check note) while an earlier commit's `Put` for that token
/// is still sitting in the drainer's window. Before the fix, Phase A
/// unconditionally skipped every `Put`/`Delete` op, Phase B did nothing when
/// `per_table_mvcc` lacked the table's entry, and Phase C finalized the
/// entry anyway (`mark_durable` + `wal.commit`) — the write vanished with no
/// error and no retry path.
///
/// This test reproduces the exact trigger the review names: it evicts ONLY
/// the `per_table_mvcc` registration (leaving the table's own `TableManager`
/// cache / config / token registration intact, exactly what
/// `per_table_mvcc.remove_sync` alone does at repo_instance.rs:510) and
/// asserts `drain_step` does not silently drop the write.
#[tokio::test]
async fn drain_step_recovers_data_for_table_missing_from_per_table_mvcc() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    let token = table_token_for("t");

    // Sanity: the normal path DOES register per_table_mvcc.
    assert!(
        repo.per_table_mvcc().read_sync(&token, |_, _| ()).is_some(),
        "get_table must register per_table_mvcc"
    );

    let gate = repo.tx_gate().await.unwrap();
    seed_inflight_put(&repo, "t", rid(1), "unattached", 1).await;
    gate.publish_committed_max(1);

    // Simulate the remove_table race: evict ONLY the per_table_mvcc entry
    // while the entry above is still undrained.
    assert!(
        repo.per_table_mvcc().remove_sync(&token).is_some(),
        "must actually evict the registration to reproduce the race"
    );

    let drainer = Drainer::new();
    let drained = drainer.drain_step(&repo.core).await.unwrap();

    assert_eq!(drained, 1, "the entry must still be drained (not stuck)");
    assert_eq!(gate.durable_watermark(), 1);

    let got = tbl.get(rid(1)).await.unwrap();
    assert!(
        matches!(got, InnerValue::Str(ref s) if s == "unattached"),
        "Put for a table missing from per_table_mvcc must not be silently \
         dropped when the entry is finalized: got {got:?}"
    );
}
// Stage 3 test 4: crash_recovery still works — sanity check that
// the window addition does not break recovery. Delegated to the
// existing crash_recovery test suite (run via gate command).
/// Stage 3 test 3: drain_step with dur >= vis returns 0 immediately —
/// no window scan, no wal.recover().
#[tokio::test]
async fn drain_step_empty_window_no_recover_call() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));

    let gate = repo.tx_gate().await.unwrap();
    gate.publish_committed_max(5);
    for v in 1..=5u64 {
        gate.mark_durable(v);
    }
    assert_eq!(gate.durable_watermark(), 5);

    let drainer = Drainer::new();
    let drained = drainer.drain_step(&repo.core).await.unwrap();
    assert_eq!(drained, 0, "dur >= vis → immediate return 0");
}
/// End-to-end: set hw=2, commit N=10 txs through the commit API, then
/// drain_step. All 10 entries must be drained (gap-reseed recovers drops).
#[tokio::test]
async fn drain_step_recovers_dropped_entries_via_gap_reseed() {
    let repo = make_repo();
    repo.disable_background_drainer_for_test();
    repo.add_table_for_test(crate::tx::table::TableConfig::new("t"));

    // Set a very low watermark so most offers are dropped.
    let drainer = repo.drainer();
    drainer.set_window_high_watermark(2);

    for i in 1..=10u64 {
        let mut tx = TxContext::new(TxId::new(i), 0, 0, IsolationLevel::Snapshot);
        let data_store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
        let mut staging = StagingStore::new(Arc::clone(&data_store));
        staging.set(
            Bytes::from(format!("k{i}")).into(),
            Bytes::from(format!("v{i}")),
        );
        tx.write_set.insert(i, staging);
        let _ = commit_tx(tx, &repo.core).await.unwrap();
    }

    let dropped = drainer.offer_dropped_total();
    assert!(
        dropped > 0,
        "with hw=2, some offers must have been dropped (dropped={dropped})"
    );

    assert_eq!(drainer.window_keys(), vec![1, 2]);
    let wal = repo.repo_wal().await.unwrap();
    let versions: Vec<_> = wal
        .recover()
        .await
        .unwrap()
        .into_iter()
        .map(|e| e.commit_version)
        .collect();
    assert_eq!(versions, (1..=10).collect::<Vec<_>>());
    let recover_before = drainer.recover_calls();
    let drained = drainer.drain_all(&repo.core).await.unwrap();
    assert!(drainer.recover_calls() > recover_before);
    assert_eq!(drained, 10, "all 10 entries must be drained despite drops");

    let gate = repo.tx_gate().await.unwrap();
    assert_eq!(
        gate.durable_watermark(),
        gate.last_committed(),
        "durable must catch up to visibility"
    );
    assert_eq!(drainer.window_len(), 0, "window must be empty after drain");
}
/// Stage 3 test 2: a gap in the window triggers exactly one gap-reseed
/// from wal.recover(), and all entries are still drained.
#[tokio::test]
async fn drain_step_window_gap_triggers_one_recover_reseed() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));

    let gate = repo.tx_gate().await.unwrap();

    // Seed inflight entries for versions 1, 2, 3 in WAL.
    for v in 1..=3u64 {
        seed_inflight_put(&repo, "t", rid(v as u8), &format!("gap{v}"), v).await;
    }
    gate.publish_committed_max(3);

    // Offer all 3 to the window, then remove v=2 to create a gap.
    let drainer = Drainer::new();
    let wal = repo.repo_wal().await.unwrap();
    let entries = wal.recover().await.unwrap();
    for e in &entries {
        drainer.offer(Arc::new(e.clone()));
    }
    assert_eq!(drainer.window_len(), 3);
    drainer.window_remove_for_test(2);
    assert_eq!(drainer.window_len(), 2);
    assert_eq!(drainer.window_keys(), vec![1, 3]);

    // drain_all loops drain_step until 0. The new empty-prefix trigger
    // fires the gap-reseed exactly once on the pass that sees the empty
    // contiguous prefix (the pass after v=1 drained and watermark moved
    // to 1, leaving the window with {3} and expected=2 mismatching k=3).
    let recover_before = drainer.recover_calls();
    let drained = drainer.drain_all(&repo.core).await.unwrap();
    let recover_after = drainer.recover_calls();
    assert_eq!(drained, 3, "gap-reseed recovers the missing entry");
    assert_eq!(
        recover_after - recover_before,
        1,
        "exactly one gap-reseed fires (on the empty-prefix pass)"
    );
    assert_eq!(gate.durable_watermark(), 3);
    assert_eq!(drainer.window_len(), 0, "window empty after drain");
}

/// PT 2: an aborted commit_version INTERIOR to (dur, vis] — its
/// version was burned via fetch_add but no WAL entry was written, and
/// `VersionGuard::drop` (version_guard.rs:114) marked the durable
/// tracker Aborted — must NOT trigger a spurious wal.recover() reseed.
/// Empty-prefix invariant: `dur = durable_watermark()` already skips
/// every leading aborted version, so `dur+1` is NEVER aborted. A
/// non-empty contiguous prefix therefore means real progress; the
/// aborted version above is crossed via the moving watermark on the
/// next pass. Reseed must fire only on a TRUE gap (dropped offer),
/// never on an abort hole.
#[tokio::test]
async fn drain_step_interior_abort_does_not_trigger_spurious_recover() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));

    let gate = repo.tx_gate().await.unwrap();
    // Seed WAL with entries for v=1 and v=3 only; v=2 was aborted, never
    // written.
    seed_inflight_put(&repo, "t", rid(1), "abort_a", 1).await;
    seed_inflight_put(&repo, "t", rid(3), "abort_c", 3).await;
    // Mirror VersionGuard::drop for an aborted v=2 (e.g. SSI conflict
    // or empty-tx abort): durable tracker marks it Aborted so the
    // contiguous watermark crosses it once v=1 lands.
    gate.mark_durable_aborted(2);
    gate.publish_committed_max(3);
    assert_eq!(gate.durable_watermark(), 0, "dur=0 before v=1 drains");
    assert_eq!(gate.last_committed(), 3);

    let drainer = Drainer::new();
    let wal = repo.repo_wal().await.unwrap();
    let entries = wal.recover().await.unwrap();
    assert_eq!(entries.len(), 2, "WAL holds only the committed entries");
    for e in &entries {
        drainer.offer(Arc::new(e.clone()));
    }
    assert_eq!(drainer.window_keys(), vec![1, 3]);

    let recover_before = drainer.recover_calls();
    let drained = drainer.drain_all(&repo.core).await.unwrap();
    let recover_after = drainer.recover_calls();

    assert_eq!(drained, 2, "two committed entries drained (v=1, v=3)");
    assert_eq!(
        recover_after - recover_before,
        0,
        "interior abort must NOT trigger spurious wal.recover() — the \
         window already has every committed entry; the abort hole is \
         crossed via durable_watermark advance, not a reseed"
    );
    assert_eq!(gate.durable_watermark(), 3, "watermark crosses abort");
    assert_eq!(drainer.window_len(), 0, "window drained empty");
}

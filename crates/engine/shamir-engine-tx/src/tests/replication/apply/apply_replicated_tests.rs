//! Tests for the follower apply entry [`crate::tx::apply_replicated_batch`]
//! (receipt-only protocol, PR6b-3), exercised one event per batch.
//!
//! Each test constructs an in-memory follower repo with an `items` table,
//! builds a [`ChangelogEvent`] via the REAL `shamir_tx::project_event` path
//! (so the byte shape is identical to what a leader emits), and asserts the
//! apply contract: Put/Delete convergence, idempotency by leader-watermark,
//! watermark ordering, and downstream changefeed re-emission.

use std::sync::Arc;

use bytes::Bytes;
use shamir_storage::error::{DbError, DbResult};
use shamir_storage::storage_in_memory::InMemoryRepo;
use shamir_storage::storage_in_memory::InMemoryStore;
use shamir_storage::types::{Repo, Store};
use shamir_tx::{IsolationLevel, StagingStore, TxContext, TxId};
use shamir_types::access::Actor;
use shamir_types::types::record_id::RecordId;
use shamir_types::types::value::InnerValue;

use crate::repo::{BoxRepo, RepoInstance};
use crate::table::TableConfig;
use crate::tx::apply_replicated::ApplyRejectReason;
use crate::tx::{apply_replicated_batch, ApplyOutcome, ApplyPolicy, ReplicatedEvent};

/// Apply ONE event through the public batch entry (the receipt pipeline).
async fn apply_one(
    repo: &RepoInstance,
    event: &shamir_tx::ChangelogEvent,
    field_bindings: &[(u64, String)],
    bookmark: u64,
    policy: ApplyPolicy,
) -> DbResult<ApplyOutcome> {
    apply_replicated_batch(
        &repo.core,
        &[ReplicatedEvent {
            event: event.clone(),
            field_bindings: field_bindings.to_vec(),
        }],
        bookmark,
        policy,
    )
    .await
}

/// Build an in-memory follower repo with one configured table `items`.
fn follower_repo() -> RepoInstance {
    let repo = Arc::new(InMemoryRepo::new());
    RepoInstance::new(
        "follower".into(),
        BoxRepo::InMemory(repo),
        vec![TableConfig::new("items")],
    )
}

/// Fixed rid for tests — last byte varies so each rid is distinct.
fn rid(n: u8) -> RecordId {
    let mut a = [0u8; 16];
    a[15] = n;
    RecordId(a)
}

fn items_token() -> u64 {
    crate::table::table_manager::table_token_for("items")
}

/// Build a [`ChangelogEvent`] that projects a Put on `rid` of `value`,
/// using the REAL `shamir_tx::project_event` path so the byte shape matches
/// what a leader emits. The staging `TxContext` is built via
/// `ensure_table_staging` so the human-readable table name is recorded in
/// `table_tokens` (otherwise `project_event` falls back to `token:{n}` and
/// `apply_replicated_batch`'s `table_token_for` would resolve the wrong token).
fn put_event(value: &str, record: RecordId, leader_version: u64) -> shamir_tx::ChangelogEvent {
    let mut tx = TxContext::new(TxId::new(1), 0, 0, IsolationLevel::Snapshot);
    tx.set_actor(Actor::User(42));
    let data_store: Arc<dyn Store> =
        Arc::new(shamir_storage::storage_in_memory::InMemoryStore::new());
    let body = InnerValue::Str(value.into()).to_bytes().unwrap();
    tx.ensure_table_staging(items_token(), "items", data_store)
        .set(record.to_bytes().into(), body);
    shamir_tx::project_event(&tx, "leader", leader_version).unwrap()
}

/// Build a [`ChangelogEvent`] that projects a Delete on `rid`, using the
/// REAL `shamir_tx::project_event` path. See `put_event`.
fn delete_event(record: RecordId, leader_version: u64) -> shamir_tx::ChangelogEvent {
    let mut tx = TxContext::new(TxId::new(2), 0, 0, IsolationLevel::Snapshot);
    tx.set_actor(Actor::User(42));
    let data_store: Arc<dyn Store> =
        Arc::new(shamir_storage::storage_in_memory::InMemoryStore::new());
    tx.ensure_table_staging(items_token(), "items", data_store)
        .remove(record.to_bytes().into());
    shamir_tx::project_event(&tx, "leader", leader_version).unwrap()
}

// ── Test 1: Put convergence ─────────────────────────────────────────

/// Apply a ChangelogEvent with a Put on `items` → the record reads back on
/// the follower with the same bytes.
#[tokio::test]
async fn put_convergence() {
    let follower = follower_repo();
    // Force the follower to instantiate its `items` TableManager + MvccStore.
    let follower_tbl = follower.get_table("items").await.unwrap();

    let event = put_event("hello", rid(1), 1);

    let outcome = apply_one(&follower, &event, &[], 0, ApplyPolicy::Trusted)
        .await
        .unwrap();
    let local_v = match outcome {
        ApplyOutcome::Applied { local_version } => local_version,
        other => panic!("expected Applied, got {other:?}"),
    };
    assert_eq!(local_v, 1, "first local version is 1");

    // Read back on the follower — same bytes as the leader staged.
    let got = follower_tbl.get(rid(1)).await.unwrap();
    assert!(
        matches!(got, InnerValue::Str(ref s) if s == "hello"),
        "Put should converge: got {got:?}"
    );
}

// ── Test 2: Delete convergence ──────────────────────────────────────

/// Apply a Put then a Delete → the record is absent on the follower.
#[tokio::test]
async fn delete_convergence() {
    let follower = follower_repo();
    let follower_tbl = follower.get_table("items").await.unwrap();

    let put_ev = put_event("tmp", rid(7), 1);
    let del_ev = delete_event(rid(7), 2);

    let _ = apply_one(&follower, &put_ev, &[], 0, ApplyPolicy::Trusted)
        .await
        .unwrap();
    // Confirm Put landed first.
    let got = follower_tbl.get(rid(7)).await.unwrap();
    assert!(matches!(got, InnerValue::Str(ref s) if s == "tmp"));

    // Apply Delete — watermark is 1 (Put's leader version).
    let outcome = apply_one(&follower, &del_ev, &[], 1, ApplyPolicy::Trusted)
        .await
        .unwrap();
    assert!(matches!(outcome, ApplyOutcome::Applied { .. }));

    // Record is now absent.
    let err = follower_tbl.get(rid(7)).await.unwrap_err();
    assert!(
        matches!(err, shamir_storage::error::DbError::NotFound(_)),
        "Delete should remove the record: got {err:?}"
    );
}

// ── Test 3: Idempotency ─────────────────────────────────────────────

/// Apply one event twice with watermark = event.commit_version after the
/// first → the second is a skip (no-op) and state is unchanged.
#[tokio::test]
async fn idempotent_reapply_is_skip() {
    let follower = follower_repo();
    let follower_tbl = follower.get_table("items").await.unwrap();

    let event = put_event("v3", rid(3), 5);

    // First apply at watermark 0 → Applied.
    let first = apply_one(&follower, &event, &[], 0, ApplyPolicy::Trusted)
        .await
        .unwrap();
    assert!(matches!(first, ApplyOutcome::Applied { .. }));

    // Capture the follower's gate floor after the first apply.
    let gate = follower.tx_gate().await.unwrap();
    let floor_after_first = gate.last_committed();

    // Second apply with watermark = event.commit_version (5) → Skipped, no
    // version consumed, no state change.
    let second = apply_one(&follower, &event, &[], 5, ApplyPolicy::Trusted)
        .await
        .unwrap();
    assert_eq!(second, ApplyOutcome::Skipped);

    // No new version allocated.
    assert_eq!(
        gate.last_committed(),
        floor_after_first,
        "skip must NOT advance the gate floor"
    );

    // State unchanged — same bytes.
    let got = follower_tbl.get(rid(3)).await.unwrap();
    assert!(matches!(got, InnerValue::Str(ref s) if s == "v3"));
}

// ── Test 4: watermark ordering ──────────────────────────────────────

/// v=5 with watermark=3 applies; v=5 with watermark=5 skips.
#[tokio::test]
async fn watermark_ordering() {
    let event = put_event("ord", rid(9), 5);

    // watermark=3 < 5 → Applied.
    {
        let follower = follower_repo();
        let follower_tbl = follower.get_table("items").await.unwrap();
        let outcome = apply_one(&follower, &event, &[], 3, ApplyPolicy::Trusted)
            .await
            .unwrap();
        assert!(matches!(outcome, ApplyOutcome::Applied { .. }));
        let got = follower_tbl.get(rid(9)).await.unwrap();
        assert!(matches!(got, InnerValue::Str(ref s) if s == "ord"));
    }

    // watermark=5 (== 5) → Skipped, in isolation on a fresh follower.
    {
        let follower = follower_repo();
        let _ = follower.get_table("items").await.unwrap();
        let outcome = apply_one(&follower, &event, &[], 5, ApplyPolicy::Trusted)
            .await
            .unwrap();
        assert_eq!(outcome, ApplyOutcome::Skipped);
    }

    // watermark=4 (< 5) → Applied on a fresh follower.
    {
        let follower = follower_repo();
        let follower_tbl = follower.get_table("items").await.unwrap();
        let outcome = apply_one(&follower, &event, &[], 4, ApplyPolicy::Trusted)
            .await
            .unwrap();
        assert!(matches!(outcome, ApplyOutcome::Applied { .. }));
        let got = follower_tbl.get(rid(9)).await.unwrap();
        assert!(matches!(got, InnerValue::Str(ref s) if s == "ord"));
    }
}

// ── Test 5: finalize-tail — downstream changefeed re-emit ───────────

/// After apply_replicated_batch, the follower's OWN changefeed carries a
/// re-emitted event at the follower-local version (downstream chain
/// replication works).
#[tokio::test]
async fn downstream_changefeed_reemit() {
    let follower = follower_repo();
    let _ = follower.get_table("items").await.unwrap();

    // Subscribe to the follower's changefeed BEFORE applying so the live
    // broadcast track fires (the journal-only fallback path skips the
    // broadcast when subscriber_count == 0).
    let mut rx = follower.subscribe_changelog().await.unwrap();

    let event = put_event("chain", rid(11), 7);

    let outcome = apply_one(&follower, &event, &[], 0, ApplyPolicy::Trusted)
        .await
        .unwrap();
    let local_v = match outcome {
        ApplyOutcome::Applied { local_version } => local_version,
        other => panic!("expected Applied, got {other:?}"),
    };
    assert_eq!(local_v, 1, "first local version is 1");

    // The follower should have re-emitted the event on its own changefeed.
    let rebroadcast = rx
        .recv()
        .await
        .expect("follower changefeed re-emitted the event");
    assert_eq!(
        rebroadcast.commit_version, local_v,
        "downstream event keyed on follower-local version, not leader version"
    );
    assert_eq!(
        rebroadcast.repo, "follower",
        "downstream repo name is the follower's"
    );
    assert_eq!(
        rebroadcast.changes.len(),
        event.changes.len(),
        "downstream carries the same record changes"
    );
    // The leader version 7 is NOT the downstream key.
    assert_ne!(rebroadcast.commit_version, event.commit_version);
}

// ── Test 6: apply before any read is MVCC-visible (MVCC-attach ordering) ──

/// Regression for the MVCC-attach ordering risk surfaced by R1-d: a
/// follower that applies a replicated event BEFORE ever serving a read had
/// no per-table `MvccStore` attached, so `apply_replicated_batch` took the
/// base-store fallback and the write was invisible to a later MVCC read.
/// `apply_replicated_batch` now forces the attach (via `get_table`) before
/// writing, so the record is visible even when apply is the very first
/// touch of the table — NO prior `get_table` here on purpose.
#[tokio::test]
async fn apply_before_any_read_is_mvcc_visible() {
    let follower = follower_repo();

    // Apply FIRST — the follower has never read `items`, so its MvccStore
    // is not yet attached at the top of the apply entry.
    let event = put_event("fresh", rid(3), 1);
    let outcome = apply_one(&follower, &event, &[], 0, ApplyPolicy::Trusted)
        .await
        .unwrap();
    assert!(
        matches!(outcome, ApplyOutcome::Applied { .. }),
        "apply on a fresh follower should succeed"
    );

    // Now read back through the MVCC path — the value must be visible
    // (before the fix this returned NotFound because the write went to the
    // base store while the read routes through `history`).
    let follower_tbl = follower.get_table("items").await.unwrap();
    let got = follower_tbl.get(rid(3)).await.unwrap();
    assert!(
        matches!(got, InnerValue::Str(ref s) if s == "fresh"),
        "apply-before-read must be MVCC-visible: got {got:?}"
    );
}

// ── A12 regression — completion-tracker terminal marking ───────────
//
// The legacy per-event executor used to allocate its local version via the BARE
// `assign_next_version()` (which only bumps the version_counter atomic) and
// never marked that version in the gate's `completion` tracker on the success
// path — only `mark_durable_aborted` ran on the failure path, and that touches
// the SEPARATE `durable_completion` tracker, not the visibility one. A version
// allocated but never marked in `completion` leaves a permanent hole: the
// tracker's `try_advance` stops at the first version with no terminal state,
// so a later local commit at M > N cannot push the contiguous watermark past
// N. The fix routes both paths through a VersionGuard: success materializes,
// pure failures abort, and SNAP-6 partial visibility requires reopen without
// settling either tracker. RAII also protects cancellation at awaited history.

/// Reproduce the stuck-watermark scenario: apply_replicated_batch succeeds (version
/// N allocated), then a LOCAL tx commits at M > N. The completion tracker's
/// contiguous watermark MUST advance through both N and M — before the fix it
/// wedged at N-1 forever because N was never terminally marked in
/// `completion`.
#[tokio::test]
async fn a12_success_path_completion_watermark_advances_past_replicated_version() {
    let follower = follower_repo();
    // Force-attach the `items` MvccStore BEFORE applying so the write hits the
    // MVCC path (matches the production follower that has served at least one
    // read).
    let _ = follower.get_table("items").await.unwrap();
    let gate = follower.tx_gate().await.unwrap();
    let completion_before = gate.completion().watermark();
    assert_eq!(completion_before, 0, "fresh gate: watermark at 0");

    // Step 1 — apply_replicated_batch allocates local_version = 1 (N) and (post-fix)
    // marks it `Materialized` in `completion` via the VersionGuard's `commit()`.
    let event = put_event("a12-ok", rid(21), 1);
    let outcome = apply_one(&follower, &event, &[], 0, ApplyPolicy::Trusted)
        .await
        .unwrap();
    let n = match outcome {
        ApplyOutcome::Applied { local_version } => local_version,
        other => panic!("expected Applied, got {other:?}"),
    };
    assert_eq!(n, 1, "first allocated version is 1 (N)");

    // The completion tracker MUST now reflect N as terminally marked. Before
    // the fix the bare `assign_next_version` left NO entry for N here, so the
    // watermark stayed at 0 (N was neither Pending, Materialized, nor Aborted
    // in the tracker — `try_advance` breaks on a missing entry).
    assert_eq!(
        gate.completion().watermark(),
        n,
        "completion watermark must advance to N after a successful apply_replicated_batch"
    );

    // Step 2 — a LOCAL tx commit at M = N + 1 through the normal commit path.
    // Its `VersionGuard::commit()` marks M `Materialized`; the tracker's
    // `try_advance` then needs BOTH N and N+1 terminally marked to advance
    // past N+1. Before the fix N was unmarked, so the watermark was stuck at
    // N-1 = 0 even though M was correctly marked.
    let mut local_tx = TxContext::new(TxId::new(99), 0, 0, IsolationLevel::Snapshot);
    let data_store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let mut staging = StagingStore::new(Arc::clone(&data_store));
    staging.set(
        Bytes::from_static(b"local-k").into(),
        Bytes::from_static(b"local-v"),
    );
    // Use a DIFFERENT table token than `items` so this commit is independent
    // of the replicated table's state. Any non-zero token works for an
    // InMemory commit (see durable_watermark_tests::staged_tx).
    local_tx.write_set.insert(7777u64, staging);

    let outcome = crate::tx::commit_tx(local_tx, &follower.core)
        .await
        .unwrap();
    let m = outcome.commit_version;
    assert!(m > n, "local commit M ({m}) must be > N ({n})");
    assert!(outcome.materialized(), "InMemory local commit is Complete");

    // The HARD assertion: completion watermark advances to M. Before the fix
    // this was `0` (stuck at N-1's gap), proving the clog.
    assert_eq!(
        gate.completion().watermark(),
        m,
        "completion watermark must advance through M after both N (replicated) \
         and M (local) are terminally marked — was stuck at N-1 before the fix"
    );
    // Visibility floor tracks the watermark (both run through the same
    // `last_committed_version` atomic via `advance_last_committed`).
    assert_eq!(gate.last_committed(), m);
}

/// Regression: the A12 fix MUST NOT change the apply path's externally
/// observable success-path contract — the record still lands, the downstream
/// changefeed still re-emits at the follower-local version, and the durable
/// watermark still tracks the visibility watermark (inline-materialize
/// invariant `durable_watermark() <= last_committed()` preserved).
#[tokio::test]
async fn a12_regression_success_path_preserves_data_changefeed_and_durable() {
    let follower = follower_repo();
    let _ = follower.get_table("items").await.unwrap();
    let gate = follower.tx_gate().await.unwrap();
    let mut rx = follower.subscribe_changelog().await.unwrap();

    let event = put_event("regress", rid(41), 3);
    let outcome = apply_one(&follower, &event, &[], 0, ApplyPolicy::Trusted)
        .await
        .unwrap();
    let n = match outcome {
        ApplyOutcome::Applied { local_version } => local_version,
        other => panic!("expected Applied, got {other:?}"),
    };
    assert_eq!(n, 1);

    // Data converged.
    let follower_tbl = follower.get_table("items").await.unwrap();
    let got = follower_tbl.get(rid(41)).await.unwrap();
    assert!(
        matches!(got, InnerValue::Str(ref s) if s == "regress"),
        "regression: data must still converge: got {got:?}"
    );

    // Downstream changefeed re-emitted at the follower-local version.
    let rebroadcast = rx.recv().await.expect("downstream event re-emitted");
    assert_eq!(rebroadcast.commit_version, n);
    assert_eq!(rebroadcast.repo, "follower");

    // Durable watermark tracks visibility (the fix keeps `mark_durable(n)`
    // alongside the new `guard.commit()` — both are needed: the guard covers
    // the visibility tracker, `mark_durable` covers the durable tracker +
    // `durable_progress.notify_waiters()`).
    assert!(
        gate.durable_watermark() <= gate.last_committed(),
        "inline-materialize invariant: durable <= visibility"
    );
    assert_eq!(gate.durable_watermark(), n);
    assert_eq!(gate.last_committed(), n);
}

// ── Group 16, Defect 2 — non-NotFound attach failure must not be swallowed ──

/// Build a follower repo with `items` configured but its `__info__items`
/// store pre-corrupted at the exact key `IndexManager::new` reads FIRST on
/// table open (`RecordId::system("indexes")`). `IndexInfo::decode_bytes` on
/// garbage bytes there fails with a genuine `DbError::Codec` — a real,
/// non-`NotFound` attach failure reachable through `get_table` /
/// `create_table_context` with no `BoxRepo`/`Repo` fault-injection rewrite
/// needed (`BoxRepo` is a closed enum; corrupting on-disk-shaped content
/// through the public `Store` API is the established, reusable seam — see
/// `IndexManager::new`'s F-83 corruption policy in
/// `shamir-index/src/base_index/index_manager.rs`).
///
/// Returns the follower AND the raw backing `InMemoryRepo` so the test can
/// inspect `__data__items` directly afterward — `items` can never be opened
/// through the normal `TableManager` path given the corruption, so a raw
/// store read is the only way to prove no write landed.
async fn follower_repo_with_corrupt_items_info() -> (RepoInstance, Arc<InMemoryRepo>) {
    let mem = Arc::new(InMemoryRepo::new());
    let info_store = mem.store_get("__info__items").await.unwrap();
    info_store
        .set(
            RecordId::system("indexes").to_bytes().into(),
            Bytes::from_static(b"not valid index metadata"),
        )
        .await
        .unwrap();
    let follower = RepoInstance::new(
        "follower".into(),
        BoxRepo::InMemory(Arc::clone(&mem)),
        vec![TableConfig::new("items")],
    );
    (follower, mem)
}

/// Regression: `apply_replicated_batch` forces the per-table MVCC attach via
/// `repo.get_table(&table_name)` and, before the fix, discarded EVERY error
/// from that call (`let _ = repo.get_table(&table_name).await;`) — not just
/// the expected "table not configured yet" `NotFound` case.
///
/// The corruption this test injects fails `TableManager::create` (via
/// `IndexManager::new`) AFTER `create_table_context` has already registered
/// the freshly-built `MvccStore` in `per_table_mvcc` (see
/// `repo_instance.rs::create_table_context`'s ordering — the `insert_sync`
/// attach happens before the `TableManager::create` call that fails here).
/// So pre-fix, discarding the `get_table` error did NOT fall through to the
/// `base.transact` branch as the general case would — it found that
/// ORPHANED `MvccStore` entry via `per_table_mvcc().read_sync` and wrote the
/// replicated record straight into it via `apply_committed_ops`, then ran
/// the full success tail (`mark_durable` / `persist_markers` / downstream
/// changefeed re-emit) and returned `Ok(ApplyOutcome::Applied)` — even
/// though `items` can never actually be opened for reads again. Since the
/// caller (`follower_loop`) only advances the durable replication bookmark
/// after `Ok(ApplyOutcome::Applied)`, it would advance the bookmark past an
/// event whose data is now stranded in an unreachable store, and no retry
/// could ever repair the divergence.
///
/// Post-fix: the non-`NotFound` error propagates immediately — before the
/// `per_table_mvcc` lookup is even reached — so neither the orphaned-mvcc
/// write nor the finalize tail (`version_guard.commit()`, `mark_durable`,
/// `persist_markers`, downstream changefeed re-emit) ever runs.
///
/// Attach errors now occur in preflight, before any version is allocated.
#[tokio::test]
async fn get_table_attach_failure_is_not_swallowed() {
    let (follower, mem) = follower_repo_with_corrupt_items_info().await;
    let gate = follower.tx_gate().await.unwrap();
    let allocated = gate.version_allocation_high_water_mark();
    // Subscribe BEFORE the failing apply so a (wrongly) re-emitted
    // downstream event would be observable via `try_recv()` afterward.
    let mut rx = follower.subscribe_changelog().await.unwrap();

    let event = put_event("should-not-land", rid(50), 1);

    let err = apply_one(&follower, &event, &[], 0, ApplyPolicy::Trusted)
        .await
        .expect_err("a genuine non-NotFound attach failure must propagate, not be swallowed");
    assert!(
        !matches!(err, DbError::NotFound(_)),
        "expected a genuine attach failure (Codec/Internal), got NotFound: {err:?}"
    );

    assert_eq!(gate.version_allocation_high_water_mark(), allocated);
    // The finalize tail must NOT have run: no downstream changefeed
    // re-emit. `try_recv` is deterministic here (no other task can have
    // produced on this broadcast channel by the time `apply_replicated_batch`
    // has already returned).
    assert!(
        matches!(
            rx.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        ),
        "the finalize tail (downstream changefeed re-emit) must not run when \
         the forced attach genuinely failed"
    );

    // Orthogonal confirmation: the direct `base.transact` fallback (the
    // OTHER branch a swallowed error could have taken, in the general case
    // where no orphaned `per_table_mvcc` entry exists) must not have run
    // either — `items` can never open through the normal path given the
    // corruption, so read the raw `__data__items` store directly.
    let raw_data_store = mem.store_get("__data__items").await.unwrap();
    let raw_get = raw_data_store.get(rid(50).to_bytes().into()).await;
    assert!(
        matches!(raw_get, Err(DbError::NotFound(_))),
        "no write should have landed via the base-store fallback: {raw_get:?}"
    );
}

/// #1199 — `ApplyPolicy::ValidatePayload` rejects a `Put` whose `value`
/// bytes decode as neither `RecordView` nor `InnerValue`, and consumes NO
/// version doing so. Translation also requires decoding under `Trusted`.
#[tokio::test]
async fn validate_payload_rejects_undecodable_put_and_consumes_no_version() {
    let follower = follower_repo();
    let _ = follower.get_table("items").await.unwrap();
    let gate = follower.tx_gate().await.unwrap();
    assert_eq!(gate.completion().watermark(), 0);

    // 0xC1 is msgpack's permanently reserved/never-used marker byte — both
    // `RecordView::new` (top-level map-header read) and `InnerValue::from_bytes`
    // (full `rmp_serde` decode) error on it deterministically.
    let garbage_event = shamir_tx::ChangelogEvent {
        repo: "leader".into(),
        commit_version: 1,
        tx_id: 1,
        actor: Actor::User(42),
        timestamp_ns: 0,
        changes: vec![shamir_tx::RecordChange {
            table: "items".into(),
            key: rid(50).to_bytes(),
            op: shamir_tx::ChangeOp::Put,
            value: Some(vec![0xC1u8].into()),
        }],
    };

    let err = apply_one(
        &follower,
        &garbage_event,
        &[],
        0,
        ApplyPolicy::ValidatePayload,
    )
    .await;
    assert_eq!(
        err.unwrap(),
        ApplyOutcome::Rejected {
            reason: ApplyRejectReason::UndecodablePayload,
        }
    );
    assert_eq!(
        gate.completion().watermark(),
        0,
        "a ValidatePayload rejection must consume NO version — the decode \
         check runs before assign_next_version_guarded"
    );

    // Trusted skips optional validation, not decoding required for translation.
    let outcome = apply_one(&follower, &garbage_event, &[], 0, ApplyPolicy::Trusted)
        .await
        .unwrap();
    assert_eq!(
        outcome,
        ApplyOutcome::Rejected {
            reason: ApplyRejectReason::UndecodablePayload,
        }
    );
}

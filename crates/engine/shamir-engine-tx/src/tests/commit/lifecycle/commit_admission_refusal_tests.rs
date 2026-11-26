//! R2a 1c wiring: the commit admission gate refuses a leader commit BEFORE
//! any side effect. The refused leader is OVERLAY-ONLY — its single effect
//! would be the interner overlay merge in `pre_commit_prelock` — so the
//! observable-state proof reads exactly the things the gate must protect:
//! the base interner snapshot, the repo version clock, and the WAL position.
//!
//! Negative control: freezing tables alone does NOT stop an overlay-only
//! leader (it touches no table token, so the migration freeze never sees
//! it) — the admission gate is what stops it.
//!
//! Deterministic handshakes only; no sleeps as correctness evidence. The
//! refusal test's real-time wait is the commit entry's own
//! `LEADER_REOPEN_WAIT_CAP` (2s under `cfg(test)`, 15s in production),
//! bounded by a generous timeout backstop.

use std::sync::Arc;
use std::time::Duration;

use shamir_storage::storage_in_memory::InMemoryRepo;
use shamir_tx::{IsolationLevel, TxContext, TxId};
use shamir_types::access::Actor;

use crate::tx::commit::pipeline::admission::tests::close_helpers::close;
use crate::tx::repo::{BoxRepo, RepoInstance};
use crate::tx::table::TableConfig;
use crate::tx::CommitError;

/// Backstop around the commit entry's `LEADER_REOPEN_WAIT_CAP` (2s under
/// `cfg(test)`): ten times the cap, so a loaded runner cannot trip it.
const REFUSAL_BUDGET: Duration = Duration::from_secs(20);

async fn fixture(name: &str) -> RepoInstance {
    let repo = RepoInstance::new(
        name.into(),
        BoxRepo::InMemory(Arc::new(InMemoryRepo::new())),
        vec![TableConfig::new("t")],
    );
    // Resolve WAL/gate first-touch I/O before anything is spawned.
    repo.repo_wal().await.unwrap();
    repo.tx_gate().await.unwrap();
    repo
}

/// A transaction whose ONLY effect is an interner overlay merge: the overlay
/// is populated, there is NO write set, NO index write set — the design's
/// "overlay-only commit skips every table token and enters prelock, merging
/// the overlay into the base interner".
fn overlay_only_tx() -> TxContext {
    let mut tx = TxContext::new(TxId::new(1), 0, 0, IsolationLevel::Snapshot);
    tx.set_actor(Actor::User(42));
    tx.interner_overlay
        .insert_sync("admission_probe".to_string(), shamir_tx::OVERLAY_ID_BASE)
        .expect("overlay insert ok");
    tx
}

async fn interner_snapshot(repo: &RepoInstance) -> (usize, u64, u64) {
    let tbl = repo.get_table("t").await.unwrap();
    let interner = tbl.interner().get().await.unwrap();
    (
        interner.len(),
        interner.generation(),
        interner.max_published_id(),
    )
}

/// Closed gate: an overlay-only leader is refused (typed, after the entry's
/// reopen-wait cap) BEFORE the overlay merge — base interner, repo version
/// and WAL position are all bit-for-bit unchanged.
#[tokio::test]
async fn close_refuses_overlay_only_leader() {
    let repo = fixture("admission_refusal").await;
    let gate = repo.tx_gate().await.unwrap();
    let before_interner = interner_snapshot(&repo).await;
    let before_version = gate.version_allocation_high_water_mark();
    let wal = repo.repo_wal().await.unwrap();
    let before_wal = wal.recover().await.unwrap().len();

    let admission = Arc::clone(repo.core().commit_admission());
    let owner = close(&admission).await.expect("close");
    assert!(admission.is_closed());

    let owned = repo.clone();
    let commit = tokio::spawn(async move { owned.commit_tx(overlay_only_tx()).await });
    let outcome = tokio::time::timeout(REFUSAL_BUDGET, commit)
        .await
        .expect("overlay-only leader did not resolve at the reopen-wait cap")
        .expect("commit task join");
    assert!(matches!(outcome, Err(CommitError::CommitAdmissionRefused)));

    // Observable state: NOTHING advanced. The refusal happened before the
    // prelock overlay merge, before any version allocation, before WAL.
    assert_eq!(interner_snapshot(&repo).await, before_interner);
    assert_eq!(gate.version_allocation_high_water_mark(), before_version);
    assert_eq!(wal.recover().await.unwrap().len(), before_wal);
    assert_eq!(admission.active_count(), 0);

    drop(owner);
    assert!(!admission.is_closed());
}

/// Negative control: with the gate OPEN but the table frozen for migration,
/// the SAME overlay-only commit proceeds and its overlay merge lands in the
/// base interner — freezing tables alone does not stop it; the gate does.
#[tokio::test]
async fn table_freeze_alone_does_not_stop_overlay_only_leader() {
    let repo = fixture("admission_freeze").await;
    let tbl = repo.get_table("t").await.unwrap();
    let freeze = tbl.begin_migration_freeze_impl().await.unwrap();
    assert!(tbl.needs_write_barrier());

    let outcome = repo.commit_tx(overlay_only_tx()).await;
    assert!(
        outcome.is_ok(),
        "overlay-only leader must proceed past a table freeze: {outcome:?}"
    );

    // The commit genuinely went THROUGH prelock: the overlay name is now in
    // the base interner (the side effect the closed gate must prevent).
    let interner = tbl.interner().get().await.unwrap();
    assert!(interner.get_ind("admission_probe").is_some());

    drop(freeze);
    assert!(!tbl.needs_write_barrier());
}

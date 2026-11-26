//! R2a 1c wiring: the follower apply is a SEPARATE commit entry admitting
//! through the SAME per-repo gate as the leader — refuse at once with the
//! typed conflict while the gate is closed (no side effect), succeed
//! normally after the close owner drops.
//!
//! Setup mirrors `apply_replicated_tests`: an in-memory follower repo with
//! one configured table, and a `ChangelogEvent` built through the REAL
//! `shamir_tx::project_event` path.

use std::sync::Arc;

use shamir_storage::error::{DbError, DbResult};
use shamir_storage::storage_in_memory::{InMemoryRepo, InMemoryStore};
use shamir_storage::types::Store;
use shamir_tx::{IsolationLevel, TxContext, TxId};
use shamir_types::access::Actor;
use shamir_types::types::record_id::RecordId;
use shamir_types::types::value::InnerValue;

use crate::repo::{BoxRepo, RepoInstance};
use crate::table::table_manager::table_token_for;
use crate::table::TableConfig;
use crate::tx::commit::pipeline::admission::tests::close_helpers::close;
use crate::tx::{apply_replicated_batch, ApplyOutcome, ApplyPolicy, ReplicatedEvent};

fn follower_repo() -> RepoInstance {
    let repo = Arc::new(InMemoryRepo::new());
    RepoInstance::new(
        "follower".into(),
        BoxRepo::InMemory(repo),
        vec![TableConfig::new("items")],
    )
}

fn rid(n: u8) -> RecordId {
    let mut a = [0u8; 16];
    a[15] = n;
    RecordId(a)
}

fn items_token() -> u64 {
    table_token_for("items")
}

fn put_event(value: &str, record: RecordId, leader_version: u64) -> shamir_tx::ChangelogEvent {
    let mut tx = TxContext::new(TxId::new(1), 0, 0, IsolationLevel::Snapshot);
    tx.set_actor(Actor::User(42));
    let data_store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let body = InnerValue::Str(value.into()).to_bytes().unwrap();
    tx.ensure_table_staging(items_token(), "items", data_store)
        .set(record.to_bytes().into(), body);
    shamir_tx::project_event(&tx, "leader", leader_version).unwrap()
}

async fn apply_one(
    repo: &RepoInstance,
    event: &shamir_tx::ChangelogEvent,
    bookmark: u64,
) -> DbResult<ApplyOutcome> {
    apply_replicated_batch(
        &repo.core,
        &[ReplicatedEvent {
            event: event.clone(),
            field_bindings: Vec::new(),
        }],
        bookmark,
        ApplyPolicy::Trusted,
    )
    .await
}

#[tokio::test]
async fn follower_apply_respects_gate() {
    let follower = follower_repo();
    // Instantiate the items TableManager + MvccStore before the gate dance.
    let tbl = follower.get_table("items").await.unwrap();
    let event = put_event("gated", rid(1), 1);

    let admission = Arc::clone(follower.core().commit_admission());
    let owner = close(&admission).await.expect("close");
    assert!(admission.is_closed());

    // Closed gate: IMMEDIATE typed refusal, no wait, no side effect.
    let err = apply_one(&follower, &event, 0).await.unwrap_err();
    match err {
        DbError::Conflict(msg) => {
            assert!(
                msg.contains("commit admission closed"),
                "unexpected conflict message: {msg}"
            );
        }
        other => panic!("expected DbError::Conflict, got {other:?}"),
    }
    // The lease never existed: the refused call left the count untouched.
    assert_eq!(admission.active_count(), 0);

    // After the close owner drops (reopen) the SAME apply succeeds normally.
    drop(owner);
    assert!(!admission.is_closed());
    let outcome = apply_one(&follower, &event, 0).await.unwrap();
    assert!(matches!(
        outcome,
        ApplyOutcome::Applied { local_version: 1 }
    ));
    assert_eq!(
        tbl.get(rid(1)).await.unwrap(),
        InnerValue::Str("gated".into())
    );
    assert_eq!(admission.active_count(), 0);
}

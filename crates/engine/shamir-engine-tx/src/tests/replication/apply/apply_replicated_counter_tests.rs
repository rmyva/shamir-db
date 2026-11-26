//! S4: current-state counter deltas and idempotent replayed delivery
use std::sync::Arc;

use shamir_query_builder::{select::count_all, Query};
use shamir_storage::storage_in_memory::InMemoryRepo;

use shamir_tx::{ChangeOp, ChangelogEvent, RecordChange};
use shamir_types::access::Actor;
use shamir_types::types::common::new_map;
use shamir_types::types::record_id::RecordId;
use shamir_types::types::value::InnerValue;

use crate::tx::query::filter::eval_context::FilterContext;
use crate::tx::repo::{BoxRepo, RepoInstance};
use crate::tx::table::TableConfig;
use crate::tx::{apply_replicated_batch, ApplyOutcome, ApplyPolicy, ReplicatedEvent};

fn follower(tables: &[&str]) -> RepoInstance {
    RepoInstance::new(
        "follower".into(),
        BoxRepo::InMemory(Arc::new(InMemoryRepo::new())),
        tables.iter().map(|name| TableConfig::new(*name)).collect(),
    )
}

fn change(table: &str, id: u8, op: ChangeOp) -> RecordChange {
    RecordChange {
        table: table.into(),
        key: RecordId([id; 16]).to_bytes(),
        op,
        value: (op == ChangeOp::Put).then(|| InnerValue::Str("row".into()).to_bytes().unwrap()),
    }
}

fn event(version: u64, changes: Vec<RecordChange>) -> ChangelogEvent {
    ChangelogEvent {
        repo: "leader".into(),
        commit_version: version,
        tx_id: version,
        actor: Actor::User(42),
        timestamp_ns: 0,
        changes,
    }
}

async fn apply(repo: &RepoInstance, event: &ChangelogEvent) -> ApplyOutcome {
    // Never advance the caller watermark: the receipt pipeline consults the
    // DURABLE bookmark as well, so a replay of an already-checkpointed
    // leader version is a skip.
    apply_replicated_batch(
        &repo.core,
        &[ReplicatedEvent {
            event: event.clone(),
            field_bindings: Vec::new(),
        }],
        0,
        ApplyPolicy::Trusted,
    )
    .await
    .unwrap()
}

async fn count(repo: &RepoInstance, table: &str, expected: u64) {
    assert_eq!(
        repo.get_table(table)
            .await
            .unwrap()
            .counter()
            .get()
            .await
            .unwrap(),
        expected,
    );
}

// Red before S4: data is present but the follower's counter remains zero.
#[tokio::test]
async fn three_inserts_one_delete_count_is_two() {
    let repo = follower(&["items"]);
    apply(
        &repo,
        &event(
            1,
            (1..=3)
                .map(|id| change("items", id, ChangeOp::Put))
                .collect(),
        ),
    )
    .await;
    count(&repo, "items", 3).await;
    apply(&repo, &event(2, vec![change("items", 2, ChangeOp::Delete)])).await;
    count(&repo, "items", 2).await;
    let table = repo.get_table("items").await.unwrap();
    assert!(table.get_bytes(RecordId([1; 16])).await.unwrap().is_some());
    assert_eq!(table.get_bytes(RecordId([2; 16])).await.unwrap(), None);
    assert!(table.get_bytes(RecordId([3; 16])).await.unwrap().is_some());
    let interner = table.interner().get().await.unwrap();
    let refs = new_map();
    let ctx = FilterContext::new(interner, &refs);
    let query = Query::with_repo("follower", "items")
        .select([count_all("n")])
        .build();
    let result = table.read(&query, &ctx).await.unwrap();
    assert_eq!(result.records.len(), 1);
    assert_eq!(result.records[0].get_value_u64("n"), Some(2));
}

// Red before S4 at the first count; naive footprint counting fails at replay.
#[tokio::test]
async fn replayed_delivery_does_not_double_count() {
    let repo = follower(&["items"]);
    let put = event(1, vec![change("items", 1, ChangeOp::Put)]);
    let first = apply(&repo, &put).await;
    count(&repo, "items", 1).await;
    // The receipt pipeline checkpoints the durable bookmark per batch: the
    // replay of the same leader version is a skip — no version consumed,
    // count unchanged.
    assert_eq!(first, ApplyOutcome::Applied { local_version: 1 });
    assert_eq!(apply(&repo, &put).await, ApplyOutcome::Skipped);
    count(&repo, "items", 1).await;
    let delete = event(2, vec![change("items", 1, ChangeOp::Delete)]);
    apply(&repo, &delete).await;
    count(&repo, "items", 0).await;
    apply(&repo, &delete).await;
    count(&repo, "items", 0).await;
}

// Each final operation is compared ONCE with its pre-image. Includes both
// absent and existing keys and an update (Put of an existing row).
#[tokio::test]
async fn repeated_ops_per_key_use_only_the_last_operation() {
    let repo = follower(&["items"]);
    apply(
        &repo,
        &event(
            1,
            vec![
                change("items", 1, ChangeOp::Put),
                change("items", 2, ChangeOp::Put),
            ],
        ),
    )
    .await;
    let mixed = event(
        2,
        vec![
            change("items", 1, ChangeOp::Delete),
            change("items", 1, ChangeOp::Put),
            change("items", 2, ChangeOp::Put),
            change("items", 2, ChangeOp::Delete),
            change("items", 3, ChangeOp::Put),
            change("items", 3, ChangeOp::Delete),
            change("items", 4, ChangeOp::Delete),
            change("items", 4, ChangeOp::Put),
            change("items", 4, ChangeOp::Put),
            change("items", 5, ChangeOp::Delete),
        ],
    );
    apply(&repo, &mixed).await;
    count(&repo, "items", 2).await;
    let table = repo.get_table("items").await.unwrap();
    for id in 1..=5 {
        assert_eq!(
            table.get_bytes(RecordId([id; 16])).await.unwrap().is_some(),
            id == 1 || id == 4,
        );
    }
    apply(&repo, &mixed).await;
    count(&repo, "items", 2).await;
}

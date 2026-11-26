use std::num::NonZeroU32;
use std::sync::Arc;

use shamir_query_builder::ddl::{create_index, Metric, Quantization, Tokenizer};
use shamir_query_types::batch::BatchOp;

use shamir_storage::error::DbError;
use shamir_storage::storage_in_memory::InMemoryRepo;
use shamir_tx::{ChangeOp, ChangelogEvent, RecordChange};
use shamir_types::access::Actor;
use shamir_types::types::record_id::RecordId;
use shamir_types::types::value::InnerValue;

use crate::tx::apply_replicated::ApplyRejectReason;
use crate::tx::repo::{BoxRepo, RepoInstance};
use crate::tx::table::table_manager::table_token_for;
use crate::tx::table::TableConfig;
use crate::tx::{apply_replicated_batch, ApplyOutcome, ApplyPolicy, ReplicatedEvent};

fn event(tables: &[&str]) -> ChangelogEvent {
    ChangelogEvent {
        repo: "leader".into(),
        commit_version: 1,
        tx_id: 1,
        actor: Actor::User(42),
        timestamp_ns: 0,
        changes: tables
            .iter()
            .map(|table| RecordChange {
                table: (*table).into(),
                key: RecordId([1; 16]).to_bytes(),
                op: ChangeOp::Put,
                value: Some(InnerValue::Str("row".into()).to_bytes().unwrap()),
            })
            .collect(),
    }
}

fn follower() -> RepoInstance {
    RepoInstance::new(
        "follower".into(),
        BoxRepo::InMemory(Arc::new(InMemoryRepo::new())),
        vec![TableConfig::new("items")],
    )
}

async fn assert_refused(repo: &RepoInstance, event: &ChangelogEvent, reason: ApplyRejectReason) {
    let gate = repo.tx_gate().await.unwrap();
    let allocated = gate.version_allocation_high_water_mark();
    let committed = gate.last_committed();
    let durable = gate.durable_watermark();
    let mut downstream = repo.subscribe_changelog().await.unwrap();
    let outcome = apply_replicated_batch(
        &repo.core,
        &[ReplicatedEvent {
            event: event.clone(),
            field_bindings: Vec::new(),
        }],
        0,
        ApplyPolicy::Trusted,
    )
    .await
    .unwrap();
    assert_eq!(outcome, ApplyOutcome::Rejected { reason });
    assert_eq!(gate.version_allocation_high_water_mark(), allocated);
    assert_eq!(gate.last_committed(), committed);
    assert_eq!(gate.durable_watermark(), durable);
    assert!(matches!(
        downstream.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));
    let table = repo.get_table("items").await.unwrap();
    assert_eq!(table.counter().get().await.unwrap(), 0);
    assert!(matches!(
        table.get(RecordId([1; 16])).await,
        Err(DbError::NotFound(_))
    ));
}

#[tokio::test]
async fn unknown_table_is_rejected_without_version_or_downstream_event() {
    assert_refused(
        &follower(),
        &event(&["missing"]),
        ApplyRejectReason::UnknownTable,
    )
    .await;
}

#[tokio::test]
async fn mixed_known_and_unknown_tables_write_nothing() {
    assert_refused(
        &follower(),
        &event(&["items", "missing"]),
        ApplyRejectReason::UnknownTable,
    )
    .await;
}

#[tokio::test]
async fn hash_index_is_incompatible_with_raw_replication() {
    let repo = follower();
    repo.create_index("items", "by_name", &["name"])
        .await
        .unwrap();
    assert_refused(
        &repo,
        &event(&["items"]),
        ApplyRejectReason::IndexedTable {
            table: "items".into(),
            index: "by_name".into(),
        },
    )
    .await;
}

#[tokio::test]
async fn every_index_family_is_named_and_rejects_the_entire_event() {
    for family in ["hash", "unique", "sorted", "fts", "functional", "vector"] {
        let repo = RepoInstance::new(
            "follower".into(),
            BoxRepo::InMemory(Arc::new(InMemoryRepo::new())),
            vec![TableConfig::new("items"), TableConfig::new("indexed")],
        );
        let table = repo.get_table("indexed").await.unwrap();
        match family {
            "hash" => table.create_index("by_name", &["name"]).await.unwrap(),
            "unique" => table
                .create_unique_index("by_name", &["name"])
                .await
                .unwrap(),
            "sorted" => table
                .create_sorted_index("by_name", &["name"])
                .await
                .unwrap(),
            _ => {
                let builder = create_index("by_name", "indexed");
                let op = match family {
                    "fts" => builder
                        .fts(vec!["name".into()], Tokenizer::Whitespace)
                        .unwrap(),
                    "functional" => builder.functional(vec!["name".into()], "lower").unwrap(),
                    "vector" => builder
                        .vector(
                            vec!["name".into()],
                            NonZeroU32::new(2).unwrap(),
                            Metric::Cosine,
                            Quantization::Off,
                        )
                        .unwrap(),
                    _ => unreachable!("fixture family"),
                };
                let BatchOp::CreateIndex(op) = op else {
                    panic!("index builder")
                };
                table.create_index_v2(&op).await.unwrap();
            }
        }
        // Red before S4: rejection lacked both names. The no-write portion
        // already passed; it also catches accidental index-maintenance bypass.
        assert_refused(
            &repo,
            &event(&["items", "indexed"]),
            ApplyRejectReason::IndexedTable {
                table: "indexed".into(),
                index: "by_name".into(),
            },
        )
        .await;
        assert_eq!(table.get_bytes(RecordId([1; 16])).await.unwrap(), None);
        assert_eq!(table.counter().get().await.unwrap(), 0);
    }
}

#[tokio::test]
async fn missing_put_value_is_rejected_before_allocation() {
    let mut event = event(&["items"]);
    event.changes[0].value = None;
    assert_refused(&follower(), &event, ApplyRejectReason::MissingValue).await;
}

#[tokio::test]
async fn missing_mvcc_store_is_rejected_without_base_write() {
    let repo = follower();
    let table = repo.get_table("items").await.unwrap();
    assert!(repo
        .per_table_mvcc()
        .remove_sync(&table_token_for("items"))
        .is_some());
    assert_refused(&repo, &event(&["items"]), ApplyRejectReason::NoMvccStore).await;
    assert!(matches!(
        table
            .data_store()
            .get(RecordId([1; 16]).to_bytes().into())
            .await,
        Err(DbError::NotFound(_))
    ));
}

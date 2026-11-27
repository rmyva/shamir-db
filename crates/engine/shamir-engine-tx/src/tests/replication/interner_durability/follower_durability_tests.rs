//! FOLLOWER-DURABILITY: adversarial explicit durability boundaries that
//! survive the receipt-only retirement — allocation/certification window
//! arithmetic, recovery-marker corruption refusals, and marker-floor
//! preservation — plus the receipt-only apply refusing on a corrupt marker.

use std::sync::Arc;

use bytes::Bytes;
use shamir_storage::storage_in_memory::InMemoryRepo;
use shamir_storage::types::RecordKey;
use shamir_tx::{ChangeOp, ChangelogEvent, RecordChange};
use shamir_types::access::Actor;
use shamir_types::types::record_id::RecordId;
use shamir_types::types::value::InnerValue;

use crate::meta::{MetaEnvelope, MetaKey};
use crate::repo::{BoxRepo, RepoInstance};
use crate::table::TableConfig;
use crate::tx::apply_replicated::follower_durability::in_flight_range;
use crate::tx::{apply_replicated_batch, ApplyPolicy, ReplicatedEvent};

fn repo() -> RepoInstance {
    RepoInstance::new(
        "follower".into(),
        BoxRepo::InMemory(Arc::new(InMemoryRepo::new())),
        vec![TableConfig::new("items")],
    )
}

fn events() -> Vec<ReplicatedEvent> {
    (1..=2u8)
        .map(|id| ReplicatedEvent {
            event: ChangelogEvent {
                repo: "leader".into(),
                commit_version: u64::from(id),
                tx_id: u64::from(id),
                actor: Actor::System,
                timestamp_ns: 0,
                changes: vec![RecordChange {
                    table: "items".into(),
                    key: RecordId([id; 16]).to_bytes(),
                    op: ChangeOp::Put,
                    value: Some(InnerValue::Str("row".into()).to_bytes().unwrap()),
                }],
            },
            field_bindings: Vec::new(),
        })
        .collect()
}

fn marker_key() -> RecordKey {
    RecordKey::from_slice(MetaKey::LastCommittedVersion.as_record_id().as_bytes())
}

#[test]
fn crash_phase_table_has_precise_certification_windows() {
    let cases = [
        (None, 4, Some(1..=4)),
        (Some(0), 4, Some(1..=4)),
        (Some(4), 4, None),
        (Some(2), 4, Some(3..=4)),
    ];
    for (horizon, end, expected) in cases {
        assert_eq!(in_flight_range(horizon, end), expected);
    }
}

// A corrupt LastCommittedVersion marker fails the receipt pipeline's durable
// floor reads BEFORE any allocation or first-touch write: nothing lands.
#[tokio::test]
async fn corrupt_marker_fails_the_receipt_apply_without_writes() {
    let repo = repo();
    repo.get_table("items").await.unwrap();
    repo.tx_info_store()
        .await
        .unwrap()
        .set(marker_key(), Bytes::from_static(b"corrupt"))
        .await
        .unwrap();
    apply_replicated_batch(&repo.core, &events(), 0, ApplyPolicy::Trusted)
        .await
        .expect_err("corrupt recovery marker must fail the apply");
    assert_eq!(repo.replication_bookmark().await.unwrap(), 0);
    assert_eq!(
        repo.get_table("items")
            .await
            .unwrap()
            .get_bytes(RecordId([1; 16]))
            .await
            .unwrap(),
        None,
    );
}

// Red before fix: corrupt marker decoded as absent and gate seeded at zero.
#[tokio::test]
async fn corrupt_marker_refuses_gate_initialization() {
    let repo = repo();
    repo.tx_info_store()
        .await
        .unwrap()
        .set(marker_key(), Bytes::from_static(b"corrupt"))
        .await
        .unwrap();
    assert!(repo.tx_gate().await.is_err());
}

// Covers the other gate seed: no silent fallback to tx id 1.
#[tokio::test]
async fn corrupt_tx_id_refuses_gate_initialization() {
    let repo = repo();
    let info = repo.tx_info_store().await.unwrap();
    info.set(
        marker_key(),
        Bytes::from(MetaEnvelope::new(8u64).encode().unwrap()),
    )
    .await
    .unwrap();
    info.set(
        RecordKey::from_slice(MetaKey::NextTxId.as_record_id().as_bytes()),
        Bytes::from_static(b"corrupt"),
    )
    .await
    .unwrap();
    assert!(repo.tx_gate().await.is_err());
}

// Behavioral counterfactual: a delayed normal commit tail must not overwrite
// the reserved floor or a larger transaction-id snapshot with its lower view.
#[tokio::test]
async fn normal_commit_tail_preserves_reserved_floor_and_transaction_id() {
    let repo = repo();
    let gate = repo.tx_gate().await.unwrap();
    let info = repo.tx_info_store().await.unwrap();
    crate::meta::recovery_marker::save_last_committed_flushed(&info, 12)
        .await
        .unwrap();
    crate::meta::recovery_marker::save_next_tx_id_snapshot(&info, 99)
        .await
        .unwrap();
    info.flush().await.unwrap();
    crate::tx::commit_phases::persist_markers(&repo.core, &gate, 0)
        .await
        .unwrap();
    assert_eq!(
        crate::meta::recovery_marker::load_last_committed(&info)
            .await
            .unwrap(),
        Some(12),
    );
    assert_eq!(
        crate::meta::recovery_marker::load_next_tx_id_snapshot(&info)
            .await
            .unwrap(),
        Some(99),
    );
}

// Exercise public factory open, not merely lazy gate construction.
#[cfg(feature = "fjall")]
#[test]
fn corrupt_marker_refuses_factory_open() {
    let directory = tempfile::TempDir::new().unwrap();
    {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let seed = RepoInstance::from_factory(
                "follower".into(),
                crate::repo::BoxRepoFactory::fjall_raw(directory.path()),
                vec![TableConfig::new("items")],
            )
            .await
            .unwrap();
            let info = seed.tx_info_store().await.unwrap();
            info.set(marker_key(), Bytes::from_static(b"corrupt"))
                .await
                .unwrap();
            info.flush().await.unwrap();
        });
    }
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let result = RepoInstance::from_factory(
            "follower".into(),
            crate::repo::BoxRepoFactory::fjall_raw(directory.path()),
            vec![TableConfig::new("items")],
        )
        .await;
        assert!(result.is_err(), "factory must propagate marker corruption");
    });
}

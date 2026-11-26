use std::sync::Arc;

use shamir_collections::{TFxMap, TMap, TSet};
use shamir_storage::storage_in_memory::InMemoryRepo;
use shamir_storage::types::Repo;
use shamir_tx::{collect_referenced_ids, remap_value, ChangeOp, ChangelogEvent, RecordChange};
use shamir_types::access::Actor;
use shamir_types::codecs::interned::inner_value_to_query_value;
use shamir_types::core::interner::InternerKey;
use shamir_types::types::record_id::RecordId;
use shamir_types::types::value::{InnerValue, QueryValue};

use crate::tx::apply_replicated::ApplyRejectReason;
use crate::tx::repo::{BoxRepo, RepoInstance};
use crate::tx::table::TableConfig;
use crate::tx::{apply_replicated_batch, ApplyOutcome, ApplyPolicy, ReplicatedEvent};

/// Apply ONE event through the public batch entry (the receipt pipeline).
async fn apply_batch(
    repo: &RepoInstance,
    event: &ChangelogEvent,
    field_bindings: &[(u64, String)],
    bookmark: u64,
    policy: ApplyPolicy,
) -> ApplyOutcome {
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
    .unwrap()
}

fn repo(name: &str) -> RepoInstance {
    RepoInstance::new(
        name.into(),
        BoxRepo::InMemory(Arc::new(InMemoryRepo::new())),
        vec![TableConfig::new("items")],
    )
}

fn map(id: u64, value: InnerValue) -> InnerValue {
    let mut fields = TMap::default();
    fields.insert(InternerKey::new(id), value);
    InnerValue::Map(fields)
}

fn event(ids: &[u64]) -> ChangelogEvent {
    ChangelogEvent {
        repo: "leader".into(),
        commit_version: 7,
        tx_id: 1,
        actor: Actor::User(42),
        timestamp_ns: 0,
        changes: ids
            .iter()
            .enumerate()
            .map(|(n, id)| RecordChange {
                table: "items".into(),
                key: RecordId([n as u8 + 1; 16]).to_bytes(),
                op: ChangeOp::Put,
                value: Some(map(*id, InnerValue::Int(20)).to_bytes().unwrap()),
            })
            .collect(),
    }
}

/// A later missing binding must reject the whole event before even first-touch.
/// Moving preflight into the write loop would leak the first row/name/version.
#[tokio::test]
async fn later_put_missing_binding_is_atomic_before_first_touch() {
    for policy in [ApplyPolicy::Trusted, ApplyPolicy::ValidatePayload] {
        let follower = repo("follower");
        let table = follower.get_table("items").await.unwrap();
        let gate = follower.tx_gate().await.unwrap();
        let manager = follower.repo_interner().await.unwrap();
        let interner = manager.get().await.unwrap();
        let mut rx = follower.subscribe_changelog().await.unwrap();
        let event = event(&[101, 202]);
        assert_eq!(
            apply_batch(&follower, &event, &[(101, "first_touch".into())], 0, policy).await,
            ApplyOutcome::Rejected {
                reason: ApplyRejectReason::MissingFieldBinding,
            },
        );
        assert_eq!(gate.version_allocation_high_water_mark(), 0);
        assert_eq!(gate.last_committed(), 0);
        assert_eq!(gate.durable_watermark(), 0);
        assert!(interner.get_ind("first_touch").is_none());
        assert_eq!(manager.persisted_high_water(), 0);
        for change in &event.changes {
            let id = RecordId::try_from_bytes(&change.key).unwrap();
            assert_eq!(table.get_bytes(id).await.unwrap(), None);
        }
        assert!(matches!(
            rx.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        ));
        assert!(follower
            .read_changelog_from(0, 100)
            .await
            .unwrap()
            .events
            .is_empty());
    }
}

/// Re-emitting original leader bytes breaks decoding here and at the next hop.
#[tokio::test]
async fn downstream_uses_follower_ids_and_chain_consumer_decodes() {
    let follower = repo("follower");
    let manager = follower.repo_interner().await.unwrap();
    let interner = manager.get().await.unwrap();
    interner.touch_ind("follower_only").unwrap();
    let mut rx = follower.subscribe_changelog().await.unwrap();
    let source = event(&[101]);
    assert_eq!(
        apply_batch(
            &follower,
            &source,
            &[(101, "v".into())],
            0,
            ApplyPolicy::Trusted
        )
        .await,
        ApplyOutcome::Applied { local_version: 1 },
    );
    let downstream = rx.try_recv().expect("synchronous re-emission");
    assert_eq!(downstream.repo, "follower");
    assert_eq!(downstream.commit_version, 1);
    let local_id = interner.get_ind("v").unwrap().id();
    assert_ne!(local_id, 101);
    let body = InnerValue::from_bytes(downstream.changes[0].value.as_ref().unwrap()).unwrap();
    let mut ids = TFxMap::default();
    collect_referenced_ids(&body, &mut ids);
    assert_eq!(ids, TFxMap::from_iter([(local_id, ())]));
    let decoded = inner_value_to_query_value(&body, interner).unwrap();
    assert_eq!(decoded.get("v"), Some(&QueryValue::Int(20)));
    assert_eq!(decoded.get("follower_only"), None);

    let consumer = repo("consumer");
    let consumer_manager = consumer.repo_interner().await.unwrap();
    let consumer_interner = consumer_manager.get().await.unwrap();
    for name in ["consumer_only", "another", "third"] {
        consumer_interner.touch_ind(name).unwrap();
    }
    assert_eq!(
        apply_batch(
            &consumer,
            &downstream,
            &[(local_id, "v".into())],
            0,
            ApplyPolicy::Trusted
        )
        .await,
        ApplyOutcome::Applied { local_version: 1 },
    );
    let table = consumer.get_table("items").await.unwrap();
    let id = RecordId::try_from_bytes(&source.changes[0].key).unwrap();
    let row = table.get(id).await.unwrap();
    assert_eq!(
        inner_value_to_query_value(&row, consumer_interner).unwrap(),
        decoded,
    );
}

/// Trusted skips optional validation, never mandatory translation decoding.
#[tokio::test]
async fn trusted_malformed_bytes_reject_without_names_data_or_version() {
    let follower = repo("follower");
    let mut source = event(&[101]);
    source.changes[0].value = Some(bytes::Bytes::from_static(&[0xc1]));
    let mut rx = follower.subscribe_changelog().await.unwrap();
    assert_eq!(
        apply_batch(
            &follower,
            &source,
            &[(101, "untouched".into())],
            0,
            ApplyPolicy::Trusted
        )
        .await,
        ApplyOutcome::Rejected {
            reason: ApplyRejectReason::UndecodablePayload,
        },
    );
    let gate = follower.tx_gate().await.unwrap();
    assert_eq!(gate.version_allocation_high_water_mark(), 0);
    let manager = follower.repo_interner().await.unwrap();
    assert!(manager.get().await.unwrap().get_ind("untouched").is_none());
    let table = follower.get_table("items").await.unwrap();
    assert_eq!(table.get_bytes(RecordId([1; 16])).await.unwrap(), None);
    assert!(matches!(
        rx.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));
}

fn wire_field<'a>(value: &'a rmpv::Value, name: &str) -> &'a rmpv::Value {
    value
        .as_map()
        .unwrap()
        .iter()
        .find(|(key, _)| key.as_str() == Some(name))
        .map(|(_, value)| value)
        .unwrap()
}

/// Binary keys must survive Pull decoding for both Puts and Deletes.
/// Wire compatibility defaults do not authorize foreign ids on an old envelope.
#[tokio::test]
async fn wire_roundtrip_and_default_bindings_fail_closed() {
    let mut source = event(&[101]);
    source.changes.push(RecordChange {
        table: "items".into(),
        key: RecordId([0xff; 16]).to_bytes(),
        op: ChangeOp::Delete,
        value: None,
    });
    let envelope = ReplicatedEvent {
        event: source,
        field_bindings: vec![(101, "v".into())],
    };
    // Inspect actual MessagePack, not a sequence-key stand-in for the wire.
    let bytes = rmp_serde::to_vec_named(&envelope).unwrap();
    let wire = rmpv::decode::read_value(&mut std::io::Cursor::new(&bytes)).unwrap();
    assert_eq!(wire.as_map().unwrap().len(), 7);
    assert!(wire
        .as_map()
        .unwrap()
        .iter()
        .all(|(key, _)| key.as_str() != Some("event")));
    assert_eq!(wire_field(&wire, "repo").as_str(), Some("leader"));
    let changes = wire_field(&wire, "changes").as_array().unwrap();
    assert_eq!(changes.len(), 2);
    for (change, expected) in changes.iter().zip(&envelope.event.changes) {
        assert_eq!(expected.key.len(), 16);
        assert_eq!(
            wire_field(change, "key"),
            &rmpv::Value::Binary(expected.key.to_vec()),
        );
    }
    assert!(wire_field(&changes[0], "value").is_array());
    assert_eq!(wire_field(&changes[1], "value"), &rmpv::Value::Nil);
    assert_eq!(
        wire_field(&wire, "field_bindings")
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        rmp_serde::from_slice::<ReplicatedEvent>(&bytes).unwrap(),
        envelope,
    );
    assert_eq!(
        rmp_serde::from_read::<_, ReplicatedEvent>(std::io::Cursor::new(&bytes)).unwrap(),
        envelope,
    );
    let legacy = rmp_serde::to_vec_named(&envelope.event).unwrap();
    let decoded: ReplicatedEvent = rmp_serde::from_slice(&legacy).unwrap();
    assert_eq!(decoded.event, envelope.event);
    assert!(decoded.field_bindings.is_empty());
    let default_bytes = rmp_serde::to_vec_named(&decoded).unwrap();
    let default_wire = rmpv::decode::read_value(&mut std::io::Cursor::new(&default_bytes)).unwrap();
    assert!(wire_field(&default_wire, "field_bindings")
        .as_array()
        .unwrap()
        .is_empty());
    assert_eq!(
        rmp_serde::from_slice::<ReplicatedEvent>(&default_bytes).unwrap(),
        decoded,
    );
    let follower = repo("follower");
    assert_eq!(
        apply_batch(
            &follower,
            &decoded.event,
            &decoded.field_bindings,
            0,
            ApplyPolicy::Trusted
        )
        .await,
        ApplyOutcome::Rejected {
            reason: ApplyRejectReason::MissingFieldBinding,
        },
    );
    assert_eq!(
        follower
            .tx_gate()
            .await
            .unwrap()
            .version_allocation_high_water_mark(),
        0,
    );
}

/// Missing bindings are valid when neither scalar Puts nor Deletes reference ids.
#[tokio::test]
async fn legacy_scalar_put_and_delete_allow_default_empty_bindings() {
    for policy in [ApplyPolicy::Trusted, ApplyPolicy::ValidatePayload] {
        let follower = repo("follower");
        let mut source = event(&[101]);
        source.changes[0].value = Some(InnerValue::Int(20).to_bytes().unwrap());
        let legacy = rmp_serde::to_vec_named(&source).unwrap();
        let put: ReplicatedEvent = rmp_serde::from_slice(&legacy).unwrap();
        assert_eq!(put.event, source);
        assert!(put.field_bindings.is_empty());
        assert_eq!(
            apply_batch(&follower, &put.event, &put.field_bindings, 0, policy).await,
            ApplyOutcome::Applied { local_version: 1 },
        );
        let table = follower.get_table("items").await.unwrap();
        let id = RecordId::try_from_bytes(&source.changes[0].key).unwrap();
        assert_eq!(table.get_bytes(id).await.unwrap(), source.changes[0].value);

        source.commit_version += 1;
        source.changes[0].op = ChangeOp::Delete;
        source.changes[0].value = None;
        let legacy = rmp_serde::to_vec_named(&source).unwrap();
        let delete: ReplicatedEvent = rmp_serde::from_slice(&legacy).unwrap();
        assert_eq!(delete.event, source);
        assert!(delete.field_bindings.is_empty());
        assert_eq!(
            apply_batch(
                &follower,
                &delete.event,
                &delete.field_bindings,
                put.event.commit_version,
                policy
            )
            .await,
            ApplyOutcome::Applied { local_version: 2 },
        );
        assert_eq!(table.get_bytes(id).await.unwrap(), None);
    }
}

/// Silently omitting an unresolved id would create an unusable Pull envelope.
#[tokio::test]
async fn leader_unresolved_referenced_id_returns_error() {
    let storage = Arc::new(InMemoryRepo::new());
    let leader = RepoInstance::new(
        "leader".into(),
        BoxRepo::InMemory(Arc::clone(&storage)),
        vec![TableConfig::new("items")],
    );
    let mut source = event(&[987654]);
    source.commit_version = 1;
    // Seed the exact journal-writer encoding synchronously: emit only enqueues,
    // and shutdown/flush_hint do not acknowledge completion of the writer.
    let journal = storage.store_get("__changelog__").await.unwrap();
    journal
        .set(
            shamir_tx::changefeed::version_key(source.commit_version).into(),
            rmp_serde::to_vec(&source).unwrap().into(),
        )
        .await
        .unwrap();
    // Journal reads are bounded by the completion watermark: settle the
    // version the seeded journal row stands for.
    leader.tx_gate().await.unwrap().completion().mark(
        source.commit_version,
        shamir_tx::CompletionState::Materialized,
    );
    assert_eq!(
        leader.read_changelog_from(0, 100).await.unwrap().events,
        vec![source.clone()],
    );
    let error = leader
        .read_changelog_for_replication(0, 100)
        .await
        .unwrap_err();
    assert!(matches!(error, shamir_storage::error::DbError::Codec(_)));
    assert!(error.to_string().contains("987654"), "{error}");
    assert_eq!(
        leader.read_changelog_from(0, 100).await.unwrap().events,
        vec![source],
    );
}

/// Both walks must recurse through Set/List/Map, rebuilding hashed Set entries.
/// Omitting Set traversal leaves foreign ids; mutating entries in place breaks contains/remove.
#[test]
fn nested_sets_collect_remap_and_rebuild_hash_membership() {
    let leaf = map(30, InnerValue::Int(7));
    let mut inner_set = TSet::default();
    inner_set.insert(leaf);
    let element = map(20, InnerValue::List(vec![InnerValue::Set(inner_set)]));
    let mut outer_set = TSet::default();
    outer_set.insert(element);
    outer_set.insert(InnerValue::Str("unchanged".into()));
    let mut value = map(10, InnerValue::Set(outer_set));
    let mut ids = TFxMap::default();
    collect_referenced_ids(&value, &mut ids);
    assert_eq!(ids, TFxMap::from_iter([(10, ()), (20, ()), (30, ())]));
    remap_value(
        &mut value,
        &TFxMap::from_iter([(10, 110), (20, 120), (30, 130)]),
    );
    ids.clear();
    collect_referenced_ids(&value, &mut ids);
    assert_eq!(ids, TFxMap::from_iter([(110, ()), (120, ()), (130, ())]));
    let mut expected_inner = TSet::default();
    let expected_leaf = map(130, InnerValue::Int(7));
    expected_inner.insert(expected_leaf.clone());
    let expected = map(120, InnerValue::List(vec![InnerValue::Set(expected_inner)]));
    let InnerValue::Map(fields) = &mut value else {
        panic!("map")
    };
    let InnerValue::Set(set) = fields.get_mut(&InternerKey::new(110)).unwrap() else {
        panic!("set")
    };
    assert!(set.contains(&expected));
    assert!(
        !set.insert(expected.clone()),
        "duplicate membership must work after rehash",
    );
    let InnerValue::Map(nested) = set.get(&expected).unwrap() else {
        panic!("nested map")
    };
    let InnerValue::List(list) = nested.get(&InternerKey::new(120)).unwrap() else {
        panic!("list")
    };
    let InnerValue::Set(inner) = &list[0] else {
        panic!("inner set")
    };
    assert!(inner.contains(&expected_leaf));
    assert!(set.shift_remove(&expected));
    assert!(!set.contains(&expected));
    assert!(set.contains(&InnerValue::Str("unchanged".into())));
}

use crate::meta::repl_bookmark::{load_clean_horizon, load_replication_bookmark};
use crate::repo::{BoxRepo, RepoInstance};
use crate::table::TableConfig;
use crate::tx::apply_replicated::apply_replicated_batch;
use crate::tx::{ApplyOutcome, ApplyPolicy, ReplicatedEvent};
use bytes::Bytes;
use shamir_storage::storage_in_memory::InMemoryRepo;
use shamir_storage::types::RecordKey;
use shamir_tx::{ChangeOp, ChangelogEvent, RecordChange};
use shamir_types::access::Actor;
use shamir_types::types::record_id::RecordId;
use shamir_types::types::value::InnerValue;
use std::sync::Arc;

fn replicated(version: u64, id: u8) -> ReplicatedEvent {
    ReplicatedEvent {
        event: ChangelogEvent {
            repo: "leader".into(),
            commit_version: version,
            tx_id: version,
            actor: Actor::System,
            timestamp_ns: 0,
            changes: vec![RecordChange {
                table: "items".into(),
                key: RecordId([id; 16]).to_bytes(),
                op: ChangeOp::Put,
                value: Some(InnerValue::Str(format!("row-{id}")).to_bytes().unwrap()),
            }],
        },
        field_bindings: Vec::new(),
    }
}

fn repo(name: &str) -> RepoInstance {
    RepoInstance::new(
        name.into(),
        BoxRepo::InMemory(Arc::new(InMemoryRepo::new())),
        vec![TableConfig::new("items")],
    )
}

#[tokio::test]
async fn persisted_gap_allows_batch_and_keeps_clean_horizon_unchanged() {
    let name = "follower-gap-barrier";
    let repo = repo(name);
    assert_eq!(
        apply_replicated_batch(&repo.core, &[replicated(1, 1)], 0, ApplyPolicy::Trusted)
            .await
            .unwrap(),
        ApplyOutcome::Applied { local_version: 1 }
    );
    let feed = repo.changefeed().await.unwrap();
    {
        let info = repo.tx_info_store().await.unwrap();
        assert_eq!(load_replication_bookmark(&info).await.unwrap(), 1);
        assert_eq!(load_clean_horizon(&info).await.unwrap(), Some(1));
    }
    feed.feed.mark_gap(2);
    assert_eq!(feed.feed.first_gap_version(), 2);
    assert_eq!(
        apply_replicated_batch(&repo.core, &[replicated(2, 2)], 1, ApplyPolicy::Trusted)
            .await
            .unwrap(),
        ApplyOutcome::Applied { local_version: 2 }
    );
    {
        let info = repo.tx_info_store().await.unwrap();
        assert_eq!(load_replication_bookmark(&info).await.unwrap(), 2);
        assert_eq!(load_clean_horizon(&info).await.unwrap(), Some(1));
    }
    assert_eq!(
        apply_replicated_batch(&repo.core, &[replicated(3, 3)], 2, ApplyPolicy::Trusted)
            .await
            .unwrap(),
        ApplyOutcome::Applied { local_version: 3 }
    );
    let info = repo.tx_info_store().await.unwrap();
    assert_eq!(load_replication_bookmark(&info).await.unwrap(), 3);
    assert_eq!(load_clean_horizon(&info).await.unwrap(), Some(1));
    assert_eq!(feed.feed.certified_gap_interval(), Some((2, 2)));
    let journal = repo.follower_journal_store().await.unwrap();
    let mut expected = replicated(3, 3).event;
    expected.repo = name.into();
    assert_eq!(
        journal
            .get(RecordKey::from_slice(&3u64.to_be_bytes()))
            .await
            .unwrap(),
        Bytes::from(rmp_serde::to_vec(&expected).unwrap())
    );
    feed.feed.shutdown_and_wait().await.unwrap();
    drop(feed);
    drop(repo);
}

#[tokio::test]
async fn pending_init_gap_advances_bookmark_without_certifying_horizon() {
    let name = "follower-gap-init-failure";
    let repo = repo(name);
    repo.record_pending_changefeed_gap(1);
    assert_eq!(
        apply_replicated_batch(&repo.core, &[replicated(1, 1)], 0, ApplyPolicy::Trusted)
            .await
            .unwrap(),
        ApplyOutcome::Applied { local_version: 1 }
    );
    let feed = repo.changefeed().await.unwrap();
    assert_eq!(feed.feed.first_gap_version(), 1);
    {
        let info = repo.tx_info_store().await.unwrap();
        assert_eq!(load_replication_bookmark(&info).await.unwrap(), 1);
        assert_eq!(load_clean_horizon(&info).await.unwrap(), None);
    }
    assert_eq!(
        apply_replicated_batch(&repo.core, &[replicated(2, 2)], 1, ApplyPolicy::Trusted)
            .await
            .unwrap(),
        ApplyOutcome::Applied { local_version: 2 }
    );
    let info = repo.tx_info_store().await.unwrap();
    assert_eq!(load_replication_bookmark(&info).await.unwrap(), 2);
    assert_eq!(load_clean_horizon(&info).await.unwrap(), None);
    assert_eq!(feed.feed.certified_gap_interval(), Some((1, 1)));
    feed.feed.shutdown_and_wait().await.unwrap();
    drop(feed);
    drop(repo);
}

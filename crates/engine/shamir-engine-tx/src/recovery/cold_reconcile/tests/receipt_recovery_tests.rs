//! Cold-start recovery of an outstanding replication receipt (PR6b-2): a
//! repo that crashed after the durable Prepared phase completes the receipt
//! during the next open's cold reconcile — the versions are certified
//! through the bookmark's clean horizon, so no recovery GAP is declared over
//! them. FAILS on pre-PR6b-2-cold code: the receipt stayed outstanding
//! (fail-stopped open) and/or the batch's versions were declared a GAP.

use std::path::Path;
use std::sync::Arc;

use shamir_storage::types::RecordKey;
use shamir_tx::{decode_version_key, encode_version_key, ChangeOp, ChangelogEvent, RecordChange};
use shamir_types::access::Actor;
use shamir_types::types::record_id::RecordId;
use shamir_types::types::value::InnerValue;

use crate::meta::repl::bookmark_v3::load_bookmark_v3;
use crate::meta::repl::receipt::receipt_key;
use crate::repo::RepoInstance;
use crate::tx::apply_replicated::follower_durability::receipt::stage;
use crate::tx::apply_replicated::follower_durability::test_seam::{self, Phase};

use super::helpers::open_with_retry;
use crate::tx::apply_replicated::{apply_replicated_batch, ApplyPolicy};
use crate::tx::ReplicatedEvent;

fn event(leader_version: u64, id: u8) -> ReplicatedEvent {
    ReplicatedEvent {
        event: ChangelogEvent {
            repo: "leader".into(),
            commit_version: leader_version,
            tx_id: leader_version,
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

async fn close(repo: RepoInstance) {
    let handle = repo.changefeed().await.unwrap();
    handle.feed.shutdown_and_wait().await.unwrap();
    drop(handle);
    drop(repo);
}

fn repo_name(tag: &str, path: &Path) -> String {
    format!("{tag}-{}", path.display().to_string().replace('\\', "_"))
}

async fn assert_completed_state(repo: &RepoInstance) {
    let info = repo.tx_info_store().await.unwrap();
    // Receipt retired, bookmark v3 advanced with the receipt's clean horizon.
    assert!(matches!(
        info.get(receipt_key()).await,
        Err(shamir_storage::error::DbError::NotFound(_))
    ));
    let v3 = load_bookmark_v3(&info).await.unwrap().unwrap();
    assert_eq!(v3.outstanding_receipt, None);
    assert_eq!(v3.bookmark, 1, "bookmark == upstream_end");
    assert_eq!(v3.clean_through, Some(1));
    drop(info);

    // Journal + history rows landed.
    let journal_bytes = repo
        .follower_journal_store()
        .await
        .unwrap()
        .get(RecordKey::from_slice(&1u64.to_be_bytes()))
        .await
        .expect("journal row at local version 1");
    assert!(!journal_bytes.is_empty());
    let history = repo.raw_store("__history__items").await.unwrap();
    let key = RecordId([1; 16]).to_bytes();
    let row = history
        .get(RecordKey::from_slice(&encode_version_key(key.as_ref(), 1)))
        .await
        .expect("history row at local version 1");
    assert!(!row.is_empty());
    assert_eq!(
        decode_version_key(&encode_version_key(key.as_ref(), 1)),
        Some((key.as_ref(), 1))
    );

    // No recovery GAP covers the completed receipt's versions.
    let feed = repo.changefeed().await.unwrap().feed;
    assert_eq!(
        feed.certified_gap_interval(),
        None,
        "the completed receipt's version must not be declared a gap"
    );
    drop(feed);
}

// Crash after the durable Prepared phase, close, reopen: the cold reconcile
// completes the outstanding receipt (history + journal + counters + bookmark
// advance + retirement) BEFORE classification, so no gap is declared and a
// second reopen observes the identical settled state.
#[tokio::test(flavor = "current_thread")]
async fn cold_reopen_completes_outstanding_receipt() {
    let temp = tempfile::TempDir::new().unwrap();
    let name = repo_name("receipt-cold", temp.path());
    let repo = open_with_retry(temp.path(), &name).await;

    // Drive the batch through the repository-owned worker until the sticky
    // `fail_after = Phase::Prepared` flag exhausts the 3 retries: the worker
    // parks the chunk guards, returns Err, goes IDLE, and leaves the durable
    // Prepared receipt outstanding — the same durable state a crash leaves.
    let hook = Arc::new(test_seam::Hook::default());
    *hook.fail_after.lock().unwrap() = Some(Phase::Prepared);
    hook.fail_after_sticky
        .store(true, std::sync::atomic::Ordering::Release);
    test_seam::install(&name, Arc::clone(&hook));
    let events = [event(1, 1)];
    let error = apply_replicated_batch(&repo.core, &events, 0, ApplyPolicy::Trusted)
        .await
        .unwrap_err();
    assert!(
        matches!(error, shamir_storage::error::DbError::Storage(_)),
        "expected the injected Storage failure, got {error:?}"
    );
    test_seam::remove(&name);
    // Graceful close: the worker shutdown QUARANTINES the parked guards
    // (sets requires_reopen on the in-RAM gate and drops them — after a
    // reopen the Aborted marks are acceptable), so the cold reopen starts
    // from a fresh, clean gate and completes the receipt during reconcile.
    // The parked receipt is a DURABLE PAUSE: shutdown must report it (not
    // `Completed`) and fail the gate for the next open.
    let shutdown = repo.shutdown_clean(std::time::Duration::from_secs(5)).await;
    assert!(
        matches!(&shutdown, Err(shamir_storage::error::DbError::Storage(message)) if message.contains("durable pause")),
        "shutdown must report the durable pause, got {shutdown:?}"
    );
    assert!(repo.tx_gate().await.unwrap().requires_reopen());
    close(repo).await;

    for _ in 0..2 {
        let reopened = open_with_retry(temp.path(), &name).await;
        assert_completed_state(&reopened).await;
        close(reopened).await;
    }
}

// A durable Materialized receipt is the pre-checkpoint crash state: open must
// finish only the checkpoint tail and must not repeat the materialization batch.
#[tokio::test(flavor = "current_thread")]
async fn cold_reopen_materialized_receipt_does_not_recommit() {
    let temp = tempfile::TempDir::new().unwrap();
    let name = repo_name("receipt-cold-materialized", temp.path());
    let repo = open_with_retry(temp.path(), &name).await;

    let hook = Arc::new(test_seam::Hook::default());
    *hook.fail_after.lock().unwrap() = Some(Phase::Materialized);
    hook.fail_after_sticky
        .store(true, std::sync::atomic::Ordering::Release);
    test_seam::install(&name, Arc::clone(&hook));
    let events = [event(1, 1)];
    let error = apply_replicated_batch(&repo.core, &events, 0, ApplyPolicy::Trusted)
        .await
        .unwrap_err();
    assert!(
        matches!(error, shamir_storage::error::DbError::Storage(_)),
        "expected injected Storage failure, got {error:?}"
    );
    test_seam::remove(&name);
    let shutdown = repo.shutdown_clean(std::time::Duration::from_secs(5)).await;
    assert!(
        matches!(&shutdown, Err(shamir_storage::error::DbError::Storage(message)) if message.contains("durable pause")),
        "shutdown must report the durable pause, got {shutdown:?}"
    );
    close(repo).await;

    let commits_before_reopen = stage::materialization_commit_count(&name);
    assert!(
        commits_before_reopen >= 1,
        "setup must have observed the initial materialization commit"
    );
    let reopened = open_with_retry(temp.path(), &name).await;
    assert_eq!(
        stage::materialization_commit_count(&name),
        commits_before_reopen,
        "cold recovery of a Materialized receipt must not commit materialization"
    );
    assert_completed_state(&reopened).await;
    close(reopened).await;
}

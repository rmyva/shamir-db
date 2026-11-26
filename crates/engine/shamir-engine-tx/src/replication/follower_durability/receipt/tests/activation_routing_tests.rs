//! Activation/admission tests (PR6b-3): the receipt protocol is the ONLY
//! follower apply protocol. Legacy v1/v2/u64 bookmark records (encoded by
//! the test via raw `MetaEnvelope` tuples, INCLUDING bookmark zero) refuse
//! `apply_replicated_batch` with `LegacyBookmarkFormat`; a floor marker
//! without a bookmark refuses with `LegacyFollowerState` while a LEADER with
//! the same floor still opens; a valid v3 record (any bookmark, incl. 0)
//! applies through the receipt path; refusals hold across reopen;
//! `admit_follower` verdicts: Fresh → Fresh, Native → Native, conflicting
//! seed-install state → refusal, floor → refusal.

use std::sync::Arc;

use bytes::Bytes;
use shamir_storage::error::{DbError, FollowerRefusalReason};
use shamir_storage::storage_in_memory::{InMemoryRepo, InMemoryStore};
use shamir_storage::types::{RecordKey, Store};
use shamir_tx::{ChangeOp, ChangelogEvent, RecordChange};
use shamir_types::access::Actor;
use shamir_types::types::record_id::RecordId;
use shamir_types::types::value::InnerValue;

use crate::apply_replicated::follower_durability::receipt::resume::activation::admission_decision;
use crate::apply_replicated::follower_durability::receipt::resume::activation::admit_follower;
use crate::apply_replicated::follower_durability::receipt::resume::activation::FollowerAdmission;
use crate::meta::recovery_marker::{load_last_committed, save_last_committed_flushed};
use crate::meta::repl::bookmark_v3::{
    classify_follower_bookmark, save_bookmark_v3, BookmarkV3, FollowerBookmarkState,
};
use crate::meta::MetaEnvelope;
use crate::repo::seed_install::{SeedInstallState, SeedReceipt};
use crate::repo::{BoxRepo, RepoInstance};
use crate::table::TableConfig;
use crate::{apply_replicated_batch, ApplyPolicy, ReplicatedEvent};

fn repo() -> RepoInstance {
    RepoInstance::new(
        "follower-activation".into(),
        BoxRepo::InMemory(Arc::new(InMemoryRepo::new())),
        vec![TableConfig::new("items")],
    )
}

fn info() -> Arc<dyn Store> {
    Arc::new(InMemoryStore::new())
}

fn bookmark_key() -> RecordKey {
    RecordKey::from_slice(
        crate::meta::MetaKey::ReplicationBookmark
            .as_record_id()
            .as_bytes(),
    )
}

fn seed_receipt() -> SeedReceipt {
    SeedReceipt {
        artifact_digest: "a".repeat(64),
        source_db: "source".into(),
        source_repo: "items".into(),
        source_identity: crate::meta::repo_identity::RepoIdentity {
            lineage_id: [7; 16],
            history_epoch: 1,
        },
        schema_digest: "b".repeat(64),
        source_version: 4,
        local_baseline: 1,
    }
}

async fn write_seed_install_state(info: &Arc<dyn Store>, state: &SeedInstallState) {
    let bytes = MetaEnvelope::new(state.clone()).encode().unwrap();
    info.set(
        RecordKey::from_slice(b"__meta__/seed_install/receipt"),
        Bytes::from(bytes),
    )
    .await
    .unwrap();
}

fn event(leader_version: u64) -> ReplicatedEvent {
    ReplicatedEvent {
        event: ChangelogEvent {
            repo: "leader".into(),
            commit_version: leader_version,
            tx_id: leader_version,
            actor: Actor::System,
            timestamp_ns: 0,
            changes: vec![RecordChange {
                table: "items".into(),
                key: RecordId([1; 16]).to_bytes(),
                op: ChangeOp::Put,
                value: Some(InnerValue::Str("row".into()).to_bytes().unwrap()),
            }],
        },
        field_bindings: Vec::new(),
    }
}

/// The tests own the legacy fixture bytes — no production legacy writer
/// exists anymore (mirrors `meta::repl::tests::bookmark_refusal_tests`).
fn legacy_bookmark_bytes() -> Vec<Vec<u8>> {
    let fence = crate::meta::repl_bookmark::FenceRecord {
        bookmark: 10,
        lineage_id: [5; 16],
        history_epoch: 3,
        observed_identity: None,
    };
    vec![
        // Fixed-width u64 records, including bookmark zero.
        MetaEnvelope::new(10u64).encode().unwrap(),
        MetaEnvelope::new(0u64).encode().unwrap(),
        // v1: bookmark bound to its upstream fence.
        MetaEnvelope::new((1u16, fence)).encode().unwrap(),
        // v2: fence + independent clean horizon, including bookmark zero.
        MetaEnvelope::new((2u16, (Some(fence), 10u64, Some(8u64))))
            .encode()
            .unwrap(),
        MetaEnvelope::new((
            2u16,
            (
                None::<crate::meta::repl_bookmark::FenceRecord>,
                0u64,
                None::<u64>,
            ),
        ))
        .encode()
        .unwrap(),
    ]
}

async fn assert_legacy_bookmark_refusal(repo: &RepoInstance) {
    let error = apply_replicated_batch(&repo.core, &[event(1)], 0, ApplyPolicy::Trusted)
        .await
        .expect_err("a legacy bookmark record must refuse the apply");
    assert!(
        matches!(
            &error,
            DbError::FollowerStateRefused {
                reason: FollowerRefusalReason::LegacyBookmarkFormat
            }
        ),
        "{error:?}"
    );
    // Nothing was adopted or written: still refused, still no v3.
    let error = apply_replicated_batch(&repo.core, &[event(1)], 0, ApplyPolicy::Trusted)
        .await
        .expect_err("the refusal must hold on a repeated attempt");
    assert!(matches!(
        error,
        DbError::FollowerStateRefused {
            reason: FollowerRefusalReason::LegacyBookmarkFormat
        }
    ));
    assert!(matches!(
        repo.replication_bookmark().await.unwrap_err(),
        DbError::FollowerStateRefused {
            reason: FollowerRefusalReason::LegacyBookmarkFormat
        }
    ));
    // get_table hits the initialized-table cache (warmed before seeding), so
    // it does not initialize the gate's legacy refusal — the data check works.
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

/// Acceptance tests 1 + 2: every legacy record shape — u64/v1/v2, including
/// bookmark zero — refuses the apply repeatedly; no data, no version, no v3.
#[tokio::test]
async fn legacy_bookmark_records_refuse_the_apply_repeatedly() {
    for bytes in legacy_bookmark_bytes() {
        let repo = repo();
        let info = repo.tx_info_store().await.unwrap();
        repo.get_table("items").await.unwrap();
        info.set(bookmark_key(), Bytes::from(bytes.clone()))
            .await
            .unwrap();
        assert_legacy_bookmark_refusal(&repo).await;
    }
}

/// Acceptance test 2: floor without bookmark refuses follower apply/apply
/// attempts repeatedly; no allocation, no history write, no v3 mint.
#[tokio::test]
async fn floor_without_bookmark_refuses_follower_apply_repeatedly() {
    let repo = repo();
    let info = repo.tx_info_store().await.unwrap();
    repo.get_table("items").await.unwrap();
    save_last_committed_flushed(&info, 6).await.unwrap();
    for _ in 0..2 {
        let error = apply_replicated_batch(&repo.core, &[event(1)], 0, ApplyPolicy::Trusted)
            .await
            .expect_err("floor-without-bookmark must refuse the apply");
        assert!(matches!(
            error,
            DbError::FollowerStateRefused {
                reason: FollowerRefusalReason::LegacyFollowerState
            }
        ));
    }
    assert_eq!(repo.replication_bookmark().await.unwrap(), 0);
    assert_eq!(
        load_last_committed(&info).await.unwrap(),
        Some(6),
        "the floor is untouched"
    );
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

/// Acceptance test 3: a LEADER with the same recovery marker still opens and
/// preserves its allocation floor.
#[tokio::test]
async fn leader_with_floor_marker_still_opens_and_preserves_floor() {
    let repo = repo();
    let info = repo.tx_info_store().await.unwrap();
    save_last_committed_flushed(&info, 9).await.unwrap();
    let gate = repo.tx_gate().await.unwrap();
    assert_eq!(gate.version_allocation_high_water_mark(), 9);
    assert_eq!(repo.replication_bookmark().await.unwrap(), 0);
}

/// A valid v3 record applies through the receipt path — including bookmark
/// zero and no outstanding receipt (durable follower provenance).
#[tokio::test]
async fn valid_v3_record_including_zero_bookmark_applies() {
    for bookmark in [0u64, 10] {
        let repo = repo();
        let info = repo.tx_info_store().await.unwrap();
        repo.get_table("items").await.unwrap();
        save_bookmark_v3(
            &info,
            &BookmarkV3 {
                fence: None,
                bookmark,
                clean_through: None,
                outstanding_receipt: None,
            },
        )
        .await
        .unwrap();
        // Offer an event ABOVE the seeded bookmark: `apply_fresh` computes
        // the watermark as max(caller bookmark, durable bookmark) and
        // `prepare_event` skips events <= watermark, so event(bookmark + 1)
        // is the first version that survives and applies.
        let outcome =
            apply_replicated_batch(&repo.core, &[event(bookmark + 1)], 0, ApplyPolicy::Trusted)
                .await
                .unwrap();
        assert_eq!(outcome, crate::ApplyOutcome::Applied { local_version: 1 });
    }
}

/// A v3 record with a nonzero bookmark and real data applies; the durable
/// bookmark advances through receipt checkpointing.
#[tokio::test]
async fn v3_native_follower_applies_and_checkpoints_the_bookmark() {
    let repo = repo();
    let info = repo.tx_info_store().await.unwrap();
    repo.get_table("items").await.unwrap();
    save_bookmark_v3(
        &info,
        &BookmarkV3 {
            fence: None,
            bookmark: 10,
            clean_through: Some(1),
            outstanding_receipt: None,
        },
    )
    .await
    .unwrap();
    let outcome = apply_replicated_batch(&repo.core, &[event(11)], 0, ApplyPolicy::Trusted)
        .await
        .unwrap();
    assert_eq!(outcome, crate::ApplyOutcome::Applied { local_version: 1 });
    assert_eq!(repo.replication_bookmark().await.unwrap(), 11);
}

#[cfg(feature = "fjall")]
async fn open_with_retry(path: &std::path::Path, name: &str) -> RepoInstance {
    // Match the crash harness: fjall's OS-level Database lock can linger after close.
    for attempt in 0..25u32 {
        match RepoInstance::from_factory(
            name.to_owned(),
            crate::repo::BoxRepoFactory::fjall_raw(path),
            vec![TableConfig::new("items")],
        )
        .await
        {
            Ok(repo) => return repo,
            Err(error) if attempt < 24 => {
                let _ = error;
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
            Err(error) => panic!("reopen failed after retries: {error}"),
        }
    }
    unreachable!("retry loop always returns or panics")
}

#[cfg(feature = "fjall")]
async fn shutdown(repo: RepoInstance) {
    let handle = repo.changefeed().await.unwrap();
    handle.feed.shutdown_and_wait().await.unwrap();
    drop(handle);
    drop(repo);
}

/// The refusal also holds across reopen on a durable backend.
#[cfg(feature = "fjall")]
#[tokio::test]
async fn legacy_floor_refusal_holds_across_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().to_path_buf();
    let name = "activation-restart";
    let first = open_with_retry(&path, name).await;
    let first_info = first.raw_store("__tx__").await.unwrap();
    save_last_committed_flushed(&first_info, 2).await.unwrap();
    drop(first_info);
    shutdown(first).await;

    let reopened = open_with_retry(&path, name).await;
    // The reopened store handle must stay alive for the assertion scope.
    let _reopened_info = reopened.raw_store("__tx__").await.unwrap();
    let error = apply_replicated_batch(&reopened.core, &[event(1)], 0, ApplyPolicy::Trusted)
        .await
        .expect_err("the refusal must survive reopen");
    assert!(matches!(
        error,
        DbError::FollowerStateRefused {
            reason: FollowerRefusalReason::LegacyFollowerState
        }
    ));
    shutdown(reopened).await;
}

// ── admit_follower units ─────────────────────────────────────────────

#[tokio::test]
async fn admit_follower_fresh_and_native_verdicts() {
    // Fresh: no bookmark, no floor.
    let repo = repo();
    let info = info();
    assert!(matches!(
        admit_follower(&repo.core, &info).await.unwrap(),
        FollowerAdmission::Fresh
    ));
    // Native: a valid v3 record exists.
    save_bookmark_v3(
        &info,
        &BookmarkV3 {
            fence: None,
            bookmark: 0,
            clean_through: None,
            outstanding_receipt: None,
        },
    )
    .await
    .unwrap();
    assert!(matches!(
        admit_follower(&repo.core, &info).await.unwrap(),
        FollowerAdmission::Native
    ));
}

#[tokio::test]
async fn admit_follower_refuses_conflicting_seed_install_state() {
    let repo = repo();
    let info = info();
    write_seed_install_state(&info, &SeedInstallState::Installing(seed_receipt())).await;
    let error = admit_follower(&repo.core, &info)
        .await
        .expect_err("conflicting seed-install state must refuse");
    assert!(matches!(
        error,
        DbError::FollowerStateRefused {
            reason: FollowerRefusalReason::LegacyFollowerState
        }
    ));
    // A Complete receipt conflicts just the same — no bookmark exists.
    write_seed_install_state(&info, &SeedInstallState::Complete(seed_receipt())).await;
    let error = admit_follower(&repo.core, &info).await.unwrap_err();
    assert!(matches!(
        error,
        DbError::FollowerStateRefused {
            reason: FollowerRefusalReason::LegacyFollowerState
        }
    ));
}

#[tokio::test]
async fn admit_follower_refuses_floor_without_bookmark() {
    let repo = repo();
    let info = info();
    save_last_committed_flushed(&info, 3).await.unwrap();
    let error = admit_follower(&repo.core, &info).await.unwrap_err();
    assert!(matches!(
        error,
        DbError::FollowerStateRefused {
            reason: FollowerRefusalReason::LegacyFollowerState
        }
    ));
    // The classification agrees.
    assert!(matches!(
        classify_follower_bookmark(&info).await.unwrap(),
        crate::meta::repl::bookmark_v3::FollowerBookmarkState::FloorWithoutBookmark { floor: 3 }
    ));
}

/// Acceptance test 8: refusal order — the durable-state refusal comes first
/// (`FloorWithoutBookmark` refuses with `FollowerStateRefused` regardless of
/// backend capability; classification is read-only), then the capability
/// gate refuses every verdict that would allow follower work
/// (`Native`/`Fresh` on an unsupported backend). The false-capability
/// backend is exercised through the pure `admission_decision` function
/// because `BoxRepo` is a crate-internal enum that cannot be constructed
/// around a refusing store from tests.
#[tokio::test]
async fn admission_decision_refuses_unsupported_backends_before_any_state() {
    let native = BookmarkV3 {
        fence: None,
        bookmark: 0,
        clean_through: None,
        outstanding_receipt: None,
    };
    // Unsupported backend: refused regardless of the classified state.
    assert!(matches!(
        admission_decision(false, FollowerBookmarkState::Native(native.clone())).unwrap_err(),
        DbError::AtomicBatchUnsupported(_)
    ));
    assert!(matches!(
        admission_decision(false, FollowerBookmarkState::Fresh).unwrap_err(),
        DbError::AtomicBatchUnsupported(_)
    ));
    assert!(matches!(
        admission_decision(
            false,
            FollowerBookmarkState::FloorWithoutBookmark { floor: 7 }
        )
        .unwrap_err(),
        DbError::FollowerStateRefused {
            reason: FollowerRefusalReason::LegacyFollowerState
        }
    ));
    assert!(matches!(
        admission_decision(
            true,
            FollowerBookmarkState::FloorWithoutBookmark { floor: 7 }
        )
        .unwrap_err(),
        DbError::FollowerStateRefused {
            reason: FollowerRefusalReason::LegacyFollowerState
        }
    ));
    // Supported backend: state-driven verdicts.
    assert!(matches!(
        admission_decision(true, FollowerBookmarkState::Native(native)).unwrap(),
        FollowerAdmission::Native
    ));
    assert!(matches!(
        admission_decision(true, FollowerBookmarkState::Fresh).unwrap(),
        FollowerAdmission::Fresh
    ));
}

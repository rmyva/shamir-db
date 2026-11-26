//! DDL interlock tests (S5c-X PR6b-2 item 5, REWRITTEN for PR6b-2c): the old
//! file tested the REMOVED `ddl_admission_lock`/`DdlGuard`/`ddl_hook`
//! mechanism. The shipped design has deliberately NO second admission
//! mechanism: the follower receipt pipeline holds the per-repository
//! WRITE-DOMAIN permit (`RepoWriteDomain`) for its whole run, and every DDL
//! (`rename_table_stores` and friends) admits through the SAME domain — a
//! blocked DDL is therefore provably blocked on the domain HELD BY THE
//! PIPELINE.
//!
//! Deterministic: no sleeps, no yield budgets. The parked pipeline is
//! released via the `test_seam` Notify handshake; the contended DDL is
//! observed through the `#[cfg(test)]` contended-admission Notify, which
//! fires when a write-domain admit is polled while the domain mutex is
//! HELD.

use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use shamir_storage::error::DbError;
use shamir_tx::{ChangeOp, ChangelogEvent, RecordChange};
use shamir_types::access::Actor;
use shamir_types::types::record_id::RecordId;
use shamir_types::types::value::InnerValue;

use crate::repo::RepoWriteError;
use crate::repo::{BoxRepoFactory, RepoInstance};
use crate::table::TableConfig;
use crate::tx::apply_replicated::follower_durability::test_seam::{self, Phase};
use crate::tx::apply_replicated::{apply_replicated_batch, ApplyOutcome, ApplyPolicy};
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

async fn open(path: &Path, name: &str, tables: &[&str]) -> RepoInstance {
    RepoInstance::from_factory(
        name.into(),
        BoxRepoFactory::fjall_raw(path),
        tables.iter().map(|t| TableConfig::new(*t)).collect(),
    )
    .await
    .unwrap()
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

// While the pipeline is parked at Phase::Prepared it HOLDS the write-domain
// permit; a concurrently spawned DDL (`rename_table_stores`) must not
// complete until the pipeline is released. The contended-admission Notify
// fires exactly when the DDL's `admit()` is POLLED while the domain mutex is
// held — a deterministic proof that the DDL is blocked on the domain held BY
// THE PIPELINE (no second mutex exists to be blocking it). After the release
// the DDL completes and a re-apply of the same batch is a deterministic
// Skipped no-op (the durable bookmark classifies it as applied).
//
// The DDL renames "plain", a table the follower batch never touches, so the
// post-release rename cannot interfere with the batch's "items" stores.
#[tokio::test]
async fn parked_apply_blocks_ddl_via_the_write_domain() {
    let temp = tempfile::TempDir::new().unwrap();
    let name = repo_name("ddl-interlock", temp.path());
    let repo = open(temp.path(), &name, &["items", "plain"]).await;

    let hook = Arc::new(test_seam::Hook::default());
    *hook.park_at.lock().unwrap() = Some(Phase::Prepared);
    test_seam::install(&name, Arc::clone(&hook));
    let events = [event(1, 1)];
    let mut apply = Box::pin(apply_replicated_batch(
        &repo.core,
        &events,
        0,
        ApplyPolicy::Trusted,
    ));
    // Register the `reached` waker BEFORE polling the caller
    // (notify_waiters wakes only already-registered waiters).
    let reached = hook.reached.notified();
    tokio::pin!(reached);
    reached.as_mut().enable();
    tokio::select! {
        _ = &mut reached => {}
        result = &mut apply => {
            panic!("apply completed before Phase::Prepared: {result:?}")
        }
    }
    // Dropping the caller future drops only the JoinHandle await; the
    // detached worker stays parked INSIDE the pipeline, holding the
    // write-domain permit.
    drop(apply);

    // Install the contended-admission probe and register its waiter BEFORE
    // spawning the DDL: the notification cannot be missed.
    let contended = Arc::new(tokio::sync::Notify::new());
    repo.write_domain()
        .install_contended_admission_for_test(Arc::clone(&contended));
    let contended_fired = contended.notified();
    tokio::pin!(contended_fired);
    contended_fired.as_mut().enable();

    let ddl_repo = repo.clone();
    let ddl = tokio::spawn(async move {
        ddl_repo
            .rename_table_stores("plain", "plain2")
            .await
            .unwrap()
    });
    // Fires exactly when the DDL's admit is polled against the HELD domain
    // mutex — i.e. the DDL reached the admission point and is BLOCKED.
    contended_fired.await;
    // Sound WITHOUT any spin: the parked pipeline still holds the permit,
    // so the DDL cannot have been granted admission (its completion would
    // have resolved this task).
    assert!(
        !ddl.is_finished(),
        "DDL completed while the parked pipeline held the write domain"
    );

    // Release the parked pipeline: the worker completes the whole batch
    // (materialize → checkpoint → publish) under its permit; only then is
    // the blocked rename granted admission and proceeds.
    hook.resume.notify_waiters();
    test_seam::remove(&name);
    // Reap the caller: the detached worker completes the dropped job on its
    // own; wait for the pipeline to report idle before the re-apply.
    repo.receipt_worker_wait_idle().await;
    ddl.await.unwrap();

    // The receipt pipeline ran to completion on the DETACHED
    // repository-owned worker; the re-apply below queues behind the write
    // domain, so by the time it runs the receipt is retired and the durable
    // v3 bookmark is 1 — a deterministic Skipped no-op.
    assert_eq!(
        apply_replicated_batch(&repo.core, &[event(1, 1)], 0, ApplyPolicy::Trusted)
            .await
            .unwrap(),
        ApplyOutcome::Skipped
    );
    let gate = repo.tx_gate().await.unwrap();
    assert!(!gate.requires_reopen());

    close(repo).await;
}

// PR6b-2c paused reservation: a 3-strike park after the durable Prepared
// commit places a PAUSED RESERVATION on the write domain — ordinary
// `admit()` is rejected with `RepoWriteError::Reserved`, while the follow-up
// receipt apply resumes THROUGH the reservation (`admit_with_reservation`)
// and clears it on successful publication.
#[tokio::test]
async fn paused_reservation_rejects_ordinary_admission_resume_uses_it() {
    let temp = tempfile::TempDir::new().unwrap();
    let name = repo_name("ddl-reservation", temp.path());
    let repo = open(temp.path(), &name, &["items"]).await;

    // STICKY fail_after = Phase::Prepared: all 3 Storage retries fail, the
    // worker parks the guards and places the paused reservation.
    let hook = Arc::new(test_seam::Hook::default());
    *hook.fail_after.lock().unwrap() = Some(Phase::Prepared);
    hook.fail_after_sticky.store(true, Ordering::Release);
    test_seam::install(&name, Arc::clone(&hook));
    let error = apply_replicated_batch(&repo.core, &[event(1, 1)], 0, ApplyPolicy::Trusted)
        .await
        .unwrap_err();
    test_seam::remove(&name);
    assert!(
        matches!(error, DbError::Storage(_)),
        "expected the injected Storage failure, got {error:?}"
    );
    assert!(repo.write_domain().has_outstanding_reservation());
    assert_eq!(repo.receipt_worker_parked_guards().await, Some(1));

    // Ordinary admission is deterministically rejected: the domain mutex is
    // FREE (the parked pipeline released its permit when it returned the
    // error), so the spawned admit resolves — to `Reserved`.
    let domain = repo.write_domain().clone();
    let admitted = tokio::spawn(async move { domain.admit().await });
    assert!(
        matches!(
            admitted.await.unwrap(),
            Err(RepoWriteError::Reserved { .. })
        ),
        "ordinary admission must be rejected while the paused reservation is \
         outstanding"
    );

    // The follow-up apply resumes THROUGH the reservation: same batch,
    // identical allocation, and the reservation clears with the receipt.
    hook.fail_after.lock().unwrap().take();
    assert_eq!(
        apply_replicated_batch(&repo.core, &[event(1, 1)], 0, ApplyPolicy::Trusted)
            .await
            .unwrap(),
        ApplyOutcome::Applied { local_version: 1 }
    );
    assert!(!repo.write_domain().has_outstanding_reservation());
    assert_eq!(repo.receipt_worker_parked_guards().await, Some(0));
    // With the reservation cleared, ordinary admission works again.
    let domain = repo.write_domain().clone();
    let _permit = domain.admit().await.unwrap();

    close(repo).await;
}

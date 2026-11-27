//! Step 4: read back / validate journal coverage, then atomically
//! checkpoint bookmark+J and retire the receipt.

use std::sync::Arc;
use std::time::Duration;

#[cfg(any(test, feature = "test-util"))]
use shamir_collections::TFxMap;
#[cfg(any(test, feature = "test-util"))]
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
#[cfg(any(test, feature = "test-util"))]
use std::sync::{Mutex, OnceLock};
#[cfg(any(test, feature = "test-util"))]
use tokio::sync::Notify;

use bytes::Bytes;
use shamir_storage::error::{DbError, DbResult};
use shamir_storage::types::{AtomicBatch, AtomicCommitOutcome, RecordKey, Store};
use shamir_tx::{version_key, RepoTxGate};

use crate::commit::core::RepoCore;
use crate::meta::repl::bookmark::FenceRecord;
use crate::meta::repl::bookmark_v3::{encode_bookmark_v3, load_bookmark_v3, BookmarkV3};
use crate::meta::repl::receipt::receipt_key;
use crate::meta::repl::ReplicationReceipt;
use crate::repo::changelog_store::write_domain::RepoWritePermit;

use super::{publish, stage};

const TX_STORE: &str = "__tx__";

/// Test-only seam: after a `Committed` checkpoint outcome, pretend it was
/// `Uncertain` so the read-back reconcile path runs in tests (mirrors
/// stage.rs's `SIMULATE_UNCERTAIN`).
#[cfg(any(test, feature = "test-util"))]
static SIMULATE_CHECKPOINT_UNCERTAIN: AtomicBool = AtomicBool::new(false);

#[cfg(any(test, feature = "test-util"))]
fn fail_final_flushes() -> &'static Mutex<TFxMap<String, bool>> {
    static FAIL_FINAL_FLUSH: OnceLock<Mutex<TFxMap<String, bool>>> = OnceLock::new();
    FAIL_FINAL_FLUSH.get_or_init(|| Mutex::new(TFxMap::default()))
}

#[cfg(any(test, feature = "test-util"))]
#[doc(hidden)]
pub fn set_fail_final_flush(repo_name: &str, fail: bool) {
    let mut failures = fail_final_flushes()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if fail {
        failures.insert(repo_name.to_owned(), true);
    } else {
        failures.remove(repo_name);
    }
}

#[cfg(any(test, feature = "test-util"))]
fn fail_final_flush(repo_name: &str) -> bool {
    fail_final_flushes()
        .lock()
        .unwrap()
        .get(repo_name)
        .copied()
        .unwrap_or(false)
}

#[cfg(any(test, feature = "test-util"))]
#[doc(hidden)]
pub fn set_simulate_checkpoint_uncertain(on: bool) {
    SIMULATE_CHECKPOINT_UNCERTAIN.store(on, Ordering::Release);
}

/// NEGATIVE-CONTROL hook (review finding 9d): when armed, the Uncertain
/// branch treats the outcome as settled WITHOUT the read-back or the retry —
/// the protection is removed while the branch is still entered, so
/// `checkpoint_reconciles()` still increments. Combined with
/// `set_uncertain_without_commit`, a test can prove the reconciliation
/// tests discriminate real read-back protection and not merely branch entry:
/// with this seam ON the settled-state assertions (bookmark advanced, clean
/// horizon, receipt retired) provably FAIL.
#[cfg(any(test, feature = "test-util"))]
static DISABLE_UNCERTAIN_RECONCILE: AtomicBool = AtomicBool::new(false);

#[cfg(any(test, feature = "test-util"))]
#[doc(hidden)]
pub fn disable_uncertain_reconcile(on: bool) {
    DISABLE_UNCERTAIN_RECONCILE.store(on, Ordering::Release);
}

/// Companion negative-control hook: when armed, `checkpoint()` reports
/// `Uncertain` WITHOUT staging or committing anything, so the durable state
/// is genuinely unsettled (bookmark not advanced, receipt outstanding) —
/// exactly the situation the read-back/retry protection exists to repair.
#[cfg(any(test, feature = "test-util"))]
static UNCERTAIN_WITHOUT_COMMIT: AtomicBool = AtomicBool::new(false);

#[cfg(any(test, feature = "test-util"))]
#[doc(hidden)]
pub fn set_uncertain_without_commit(on: bool) {
    UNCERTAIN_WITHOUT_COMMIT.store(on, Ordering::Release);
}

/// Test-only observable seam: counts how many times the checkpoint's
/// read-back/reconcile branch actually ran (one increment per Uncertain
/// outcome). Lets tests prove the reconcile path executed instead of merely
/// inferring it from the settled state.
#[cfg(any(test, feature = "test-util"))]
static CHECKPOINT_RECONCILES: AtomicUsize = AtomicUsize::new(0);

#[cfg(any(test, feature = "test-util"))]
#[doc(hidden)]
pub fn checkpoint_reconciles() -> usize {
    CHECKPOINT_RECONCILES.load(Ordering::Acquire)
}

/// Test-only serialization: the seams below are process-global, so the
/// tests that touch them (or the reconcile counter) take this lock to stay
/// deterministic under cargo's parallel test threads. Async on purpose:
/// tests hold the guard across `.await` points for their whole body, so a
/// poisoning-`Mutex` guard would trip `clippy::await_holding_lock` — this
/// is the workspace-sanctioned `tokio::sync::Mutex` case.
#[cfg(any(test, feature = "test-util"))]
static CHECKPOINT_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[cfg(any(test, feature = "test-util"))]
#[doc(hidden)]
pub async fn checkpoint_test_lock() -> tokio::sync::MutexGuard<'static, ()> {
    CHECKPOINT_TEST_LOCK.lock().await
}

/// Commit-park gate for cancellation tests: parks `checkpoint()` AFTER the
/// WAL group resolution and BEFORE staging the batch, so a test can drop
/// the CALLER future (and its reply receiver) while the repository-owned
/// lifecycle worker is mid-checkpoint — and then watch the detached worker
/// complete.
#[cfg(any(test, feature = "test-util"))]
static COMMIT_PARK_ARMED: AtomicBool = AtomicBool::new(false);
#[cfg(any(test, feature = "test-util"))]
static COMMIT_PARK_REACHED: Notify = Notify::const_new();
#[cfg(any(test, feature = "test-util"))]
static COMMIT_PARK_RELEASE: Notify = Notify::const_new();

#[cfg(any(test, feature = "test-util"))]
#[doc(hidden)]
pub fn arm_commit_park(on: bool) {
    COMMIT_PARK_ARMED.store(on, Ordering::Release);
}

/// Same gate, INSIDE the Uncertain reconcile branch (immediately before the
/// read-back): parks the reconcile path so a test can drop the caller
/// future before the reconcile completes.
#[cfg(any(test, feature = "test-util"))]
static RECONCILE_PARK_ARMED: AtomicBool = AtomicBool::new(false);
#[cfg(any(test, feature = "test-util"))]
static RECONCILE_PARK_REACHED: Notify = Notify::const_new();
#[cfg(any(test, feature = "test-util"))]
static RECONCILE_PARK_RELEASE: Notify = Notify::const_new();

#[cfg(any(test, feature = "test-util"))]
#[doc(hidden)]
pub fn arm_reconcile_park(on: bool) {
    RECONCILE_PARK_ARMED.store(on, Ordering::Release);
}

#[cfg(any(test, feature = "test-util"))]
#[doc(hidden)]
pub async fn wait_reconcile_park_reached() {
    let notified = RECONCILE_PARK_REACHED.notified();
    tokio::pin!(notified);
    notified.as_mut().enable();
    notified.await;
}

#[cfg(any(test, feature = "test-util"))]
#[doc(hidden)]
pub fn release_reconcile_park() {
    RECONCILE_PARK_RELEASE.notify_waiters();
}

/// The journal confirmation budget for the durable tail.
const RECEIPT_CONFIRM_BUDGET: Duration = Duration::from_secs(1);

/// Every receipt event must already be in the follower journal, byte-exact.
/// The journal flush afterwards is THE durability boundary for the journal
/// rows — it happens BEFORE any bookmark advance.
pub(crate) async fn verify_journal(repo: &RepoCore, receipt: &ReplicationReceipt) -> DbResult<()> {
    let store = repo.follower_journal_store().await?;
    for (index, mapping) in receipt.mapping.iter().enumerate() {
        let expected = receipt.downstream_events.get(index).ok_or_else(|| {
            DbError::RepoRequiresReopen(
                "receipt downstream events are shorter than its mapping".into(),
            )
        })?;
        match store
            .get(RecordKey::from_slice(&version_key(mapping.local_version)))
            .await
        {
            Ok(actual) if actual.as_ref() == expected.as_slice() => {}
            Ok(_) | Err(DbError::NotFound(_)) => {
                return Err(DbError::Storage(format!(
                    "follower receipt journal coverage missing/mismatched at local version {}",
                    mapping.local_version
                )));
            }
            Err(error) => return Err(error),
        }
    }
    store.flush().await
}

/// Confirm the journal feed through `local_end` so the clean horizon J may
/// advance over the batch.
pub(crate) async fn confirm_journal(repo: &RepoCore, local_end: u64) -> DbResult<()> {
    let handle = repo.changefeed().await?;
    let confirmed = handle
        .feed
        .confirm_through(local_end, RECEIPT_CONFIRM_BUDGET)
        .await;
    if confirmed < local_end {
        return Err(DbError::Storage(format!(
            "follower receipt journal confirmation stopped at {confirmed}, required {local_end}"
        )));
    }
    Ok(())
}

fn local_end(receipt: &ReplicationReceipt) -> u64 {
    receipt
        .mapping
        .iter()
        .map(|mapping| mapping.local_version)
        .max()
        .unwrap_or(0)
}

/// The documented gap invariant (docs/guide-docs/follower-journal-durability.md):
/// a certified GAP does not reject otherwise valid follower batches — the
/// upstream bookmark advances while the clean-history horizon J remains at
/// the last gap-free version. Cap the candidate horizon at the journal
/// feed's first open gap; a J already certified above the gap never
/// regresses.
pub(crate) async fn certified_clean_through(
    repo: &RepoCore,
    old: &BookmarkV3,
    end: u64,
) -> DbResult<Option<u64>> {
    let candidate = old.clean_through.map_or(end, |j| j.max(end));
    let first_gap = repo.changefeed().await?.feed.first_gap_version();
    let capped = if first_gap == 0 {
        candidate
    } else {
        candidate.min(first_gap - 1)
    };
    Ok(old
        .clean_through
        .map_or_else(|| (capped > 0).then_some(capped), |j| Some(j.max(capped))))
}

async fn stage_checkpoint_batch(
    repo: &RepoCore,
    permit: &RepoWritePermit,
    receipt: &ReplicationReceipt,
    old: &BookmarkV3,
) -> DbResult<Box<dyn AtomicBatch>> {
    let end = local_end(receipt);
    let bookmark = old.bookmark.max(receipt.upstream_end);
    let clean_through = certified_clean_through(repo, old, end).await?;
    // Keep the v3 decode invariant `fence.bookmark == bookmark`: the legacy
    // writers sync the embedded fence's bookmark on every advance, and the
    // fence decoder rejects the record otherwise.
    let fence = old.fence.map(|fence| FenceRecord { bookmark, ..fence });
    let bookmark = BookmarkV3 {
        fence,
        bookmark,
        clean_through,
        outstanding_receipt: None,
    };
    let mut batch = repo.begin_repo_atomic_batch(permit).await?;
    batch.set(
        TX_STORE,
        stage::bookmark_record_key(),
        Bytes::from(encode_bookmark_v3(&bookmark)?),
    )?;
    // The receipt retires ATOMICALLY with the bookmark advance: the
    // `Checkpointed` phase is therefore never durably observable — a crash
    // leaves either Prepared/Materialized + old bookmark, or retired receipt
    // + advanced bookmark. That is intentional.
    batch.delete(TX_STORE, receipt_key())?;
    Ok(batch)
}

/// Stage and commit the checkpoint batch. Test builds may INJECT an
/// `Uncertain` outcome WITHOUT committing anything
/// (`UNCERTAIN_WITHOUT_COMMIT`) so the reconcile branch below runs against
/// genuinely unsettled durable state; the production build runs the plain
/// staging path — zero behavior change when the seam is unarmed.
#[cfg(any(test, feature = "test-util"))]
async fn staged_checkpoint_outcome(
    repo: &RepoCore,
    permit: &RepoWritePermit,
    receipt: &ReplicationReceipt,
    old: &BookmarkV3,
) -> DbResult<AtomicCommitOutcome> {
    if UNCERTAIN_WITHOUT_COMMIT.load(Ordering::Acquire) {
        return Ok(AtomicCommitOutcome::Uncertain {
            detail: "injected uncertain WITHOUT commit (negative control)".into(),
        });
    }
    stage_checkpoint_batch(repo, permit, receipt, old)
        .await?
        .commit()
        .await
}

#[cfg(not(any(test, feature = "test-util")))]
async fn staged_checkpoint_outcome(
    repo: &RepoCore,
    permit: &RepoWritePermit,
    receipt: &ReplicationReceipt,
    old: &BookmarkV3,
) -> DbResult<AtomicCommitOutcome> {
    stage_checkpoint_batch(repo, permit, receipt, old)
        .await?
        .commit()
        .await
}

async fn reconcile_uncertain(
    repo: &RepoCore,
    permit: &RepoWritePermit,
    info: &Arc<dyn Store>,
    receipt: &ReplicationReceipt,
    old: &BookmarkV3,
    detail: String,
) -> DbResult<()> {
    // Reconcile by reading back: advanced bookmark AND retired receipt.
    let settled = match load_bookmark_v3(info).await {
        Ok(Some(bookmark)) => {
            bookmark.bookmark >= receipt.upstream_end && bookmark.outstanding_receipt.is_none()
        }
        _ => false,
    };
    if !settled {
        match stage_checkpoint_batch(repo, permit, receipt, old)
            .await?
            .commit()
            .await?
        {
            AtomicCommitOutcome::Committed => {}
            AtomicCommitOutcome::Uncertain { detail } => {
                return Err(DbError::RepoRequiresReopen(format!(
                    "receipt checkpoint commit uncertain twice: {detail}"
                )));
            }
        }
    } else {
        log::warn!(
            "receipt checkpoint commit was uncertain but the read-back shows \
             the bookmark advanced (repo {}): {detail}",
            repo.name()
        );
    }
    Ok(())
}

pub(crate) struct PublicationCompletion<'a, F> {
    pub(crate) lease: Option<&'a super::resume::worker::ReceiptLease>,
    pub(crate) on_published: F,
}

/// Step 4b: advance the bookmark, J, and retire the receipt in ONE atomic
/// batch, then publish to RAM in the SAME poll (the synchronous
/// `publish::apply_publication` runs directly after the commit's poll
/// resolves, with no `.await` in between), and flush the info store
/// afterwards.
pub(crate) async fn checkpoint(
    repo: &RepoCore,
    permit: &RepoWritePermit,
    info: &Arc<dyn Store>,
    receipt: &ReplicationReceipt,
    old: &BookmarkV3,
    gate_publication: (&Arc<RepoTxGate>, &publish::Publication),
    completion: PublicationCompletion<'_, impl FnOnce()>,
) -> DbResult<()> {
    let (gate, publication) = gate_publication;
    // Resolve the WAL group BEFORE the commit so nothing awaits between the
    // commit's poll resolving and the RAM publication (see the cancellation
    // proof on `publish::apply_publication`).
    let group = repo.wal_group().await.ok();
    #[cfg(any(test, feature = "test-util"))]
    if COMMIT_PARK_ARMED.load(Ordering::Acquire) {
        COMMIT_PARK_REACHED.notify_waiters();
        COMMIT_PARK_RELEASE.notified().await;
    }
    let outcome = staged_checkpoint_outcome(repo, permit, receipt, old).await?;
    #[cfg(any(test, feature = "test-util"))]
    let outcome = if outcome == AtomicCommitOutcome::Committed
        && SIMULATE_CHECKPOINT_UNCERTAIN.load(Ordering::Acquire)
    {
        AtomicCommitOutcome::Uncertain {
            detail: "injected checkpoint".into(),
        }
    } else {
        outcome
    };
    if let AtomicCommitOutcome::Uncertain { detail } = outcome {
        // The read-back/reconcile branch RAN — observable in tests via
        // `checkpoint_reconciles()`.
        #[cfg(any(test, feature = "test-util"))]
        CHECKPOINT_RECONCILES.fetch_add(1, Ordering::AcqRel);
        #[cfg(any(test, feature = "test-util"))]
        if RECONCILE_PARK_ARMED.load(Ordering::Acquire) {
            RECONCILE_PARK_REACHED.notify_waiters();
            RECONCILE_PARK_RELEASE.notified().await;
        }
        // PRODUCTION always reconciles: only the negative-control
        // short-circuit is test-only. The `#[cfg(any(test, feature = "test-util"))]` attribute applies to
        // the inner `if` STATEMENT alone; the non-test build runs
        // `reconcile_uncertain` unconditionally.
        #[cfg(any(test, feature = "test-util"))]
        if DISABLE_UNCERTAIN_RECONCILE.load(Ordering::Acquire) {
            // Negative control: protection removed. Treat the outcome as
            // settled without the read-back or the retry — the branch was
            // entered (counter incremented above) but the durable state is
            // left unreconciled, which the tests assert provably.
            log::warn!(
                "receipt checkpoint commit uncertain and reconcile DISABLED \
                 (negative control) on repo {}: {detail}",
                repo.name()
            );
        } else {
            reconcile_uncertain(repo, permit, info, receipt, old, detail).await?;
        }
        #[cfg(not(any(test, feature = "test-util")))]
        reconcile_uncertain(repo, permit, info, receipt, old, detail).await?;
    }
    if let Some(lease) = completion.lease {
        lease
            .publish(|| publish::apply_publication(gate, group.as_ref(), publication))
            .map_err(|_| {
                DbError::RepoRequiresReopen(
                    "receipt publication rejected after lifecycle termination".into(),
                )
            })?;
    } else {
        // Cold open owns the repository's write-domain permit and runs before
        // exposure; no concurrent lifecycle worker can publish against this gate.
        publish::apply_publication(gate, group.as_ref(), publication);
    }
    (completion.on_published)();
    // Phase::Checkpointed fires only AFTER the publication has completed —
    // a test that drops the apply future at this phase can never observe
    // the commit-without-publication window.
    #[cfg(any(test, feature = "test-util"))]
    super::super::test_seam::phase(repo.name(), super::super::test_seam::Phase::Checkpointed).await;
    log::info!(
        "follower receipt: Checkpointed upstream {}..={} on repo {}",
        receipt.upstream_start,
        receipt.upstream_end,
        repo.name()
    );
    #[cfg(any(test, feature = "test-util"))]
    if fail_final_flush(repo.name()) {
        return Err(DbError::Storage("injected final flush failure".into()));
    }
    info.flush().await
}

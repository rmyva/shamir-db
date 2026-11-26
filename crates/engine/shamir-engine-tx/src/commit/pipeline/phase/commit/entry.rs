use std::sync::Arc;

use shamir_query_types::batch::TransactionInfo;
use shamir_storage::error::DbError;
use shamir_tx::{CancelHandle, CancelState, TxContext};
use shamir_wal::WalOpV2;

use super::legacy_and_lockfree::{
    commit_tx_inner_legacy_async, commit_tx_lockfree, release_pessimistic_locks,
};
use crate::ack::commit_tx_with_options;
use crate::commit::core::RepoCore;
use crate::commit::pipeline::admission::gate::LEADER_REOPEN_WAIT_CAP;
use crate::pre_commit::{pre_commit_prelock, PreLockResult};
use crate::tx_outcome::{MaterializationState, TxOutcome};

const DEFAULT_MAX_TX_LIFETIME: std::time::Duration = std::time::Duration::from_secs(300); // 5 min

/// Test-only override for [`DEFAULT_MAX_TX_LIFETIME`].
///
/// `Instant::now() - Duration::from_secs(N)` (the natural way to simulate an
/// old tx) panics on overflow if the process/host has been up for less than
/// `N` — observed on a freshly-booted CI VM. Rather than have expiry tests
/// backdate `TxContext::started_at` by a large, machine-uptime-dependent
/// amount, they shrink the effective max lifetime here (to a few
/// milliseconds) and use a short real `tokio::time::sleep` instead — fully
/// deterministic regardless of host uptime. `nextest` runs each test in its
/// own process, so this global cannot leak across test files.
#[cfg(any(test, feature = "test-util"))]
pub static TEST_MAX_TX_LIFETIME_OVERRIDE: std::sync::OnceLock<std::time::Duration> =
    std::sync::OnceLock::new();

fn effective_max_tx_lifetime() -> std::time::Duration {
    #[cfg(test)]
    if let Some(d) = TEST_MAX_TX_LIFETIME_OVERRIDE.get() {
        return *d;
    }
    DEFAULT_MAX_TX_LIFETIME
}

/// Test-only pause/resume handshake installed via
/// [`TEST_POST_VALIDATE_PRE_PUBLISH_HOOK`]. `reached` lets the harness thread
/// detect (via polling, no sleeps) that the commit under test has actually
/// parked at the seam before the harness fires the concurrent writer;
/// `resume` is the release signal the harness fires once the concurrent
/// writer has fully committed.
///
/// `armed` closes a real hang this seam originally had: EVERY transaction
/// that reaches `commit_tx_lockfree`'s hook call site parks here, not just
/// the "parent" tx a test intends to pause — including the concurrent
/// writer the harness itself drives to commit AFTER releasing the parent
/// (once the writer acquires `commit_lock` in turn, it reaches this SAME
/// program point). A `Notify::notify_one()` wakes at most one parked waiter,
/// so a harness that calls it exactly once (to release the parent) leaves
/// the writer's later arrival parked forever with no second wakeup ever
/// coming — a genuine deadlock, not a slow test. `armed` makes the pause
/// one-shot: the first caller to reach the seam consumes it (CAS
/// true→false) and actually parks; every subsequent caller (the writer,
/// or any other concurrent committer) sees `armed == false` and passes
/// through immediately, unpaused. A test that wants exactly ONE tx to pause
/// here gets that for free without having to know in advance how many
/// OTHER committers will transit the same seam before the test finishes.
#[cfg(any(test, feature = "test-util"))]
#[derive(Debug)]
pub struct PostValidatePrePublishHook {
    pub reached: std::sync::atomic::AtomicUsize,
    pub resume: tokio::sync::Notify,
    pub armed: std::sync::atomic::AtomicBool,
}

/// Test-only pause seam: fires in `commit_tx_lockfree` immediately AFTER
/// `pre_commit_locked_validate` returns success (Phase 2-bis's
/// `predicate_conflicts_batch` — the RI barrier's own commit-time re-check —
/// has already run and passed) and BEFORE Phase 4 (`wal.begin_grouped`, the
/// actual commit/publish point).
///
/// F-46 (#857): the existing race-test seam
/// (`fk_ri_barrier_tests.rs`'s `RaceInjectingResolver`, injecting at a
/// `resolve_repo()` call ordinal) fires the concurrent writer BEFORE the
/// parent even reaches `commit_interactive_tx` — it proves a
/// writer-already-fully-committed-before-parent-validates race, not a
/// writer-committing-DURING-the-parent's-own-commit-lock-window race. This
/// hook fires deterministically inside that window instead, so a test can
/// drive a concurrent FK-child writer to completion strictly between the
/// parent's barrier check and the parent's own publish — the exact gap the
/// 2026-07-28 readonly review's P0-1 finding describes.
///
/// Shape mirrors `TEST_MAX_TX_LIFETIME_OVERRIDE` above (a `#[cfg(test)]`
/// `OnceLock`, zero cost when unset) combined with
/// `fk_reverse_cache_race_tests.rs`'s counter-poll + `Notify` pause/resume
/// handshake: a test installs an `Arc<PostValidatePrePublishHook>` here, then
/// the seam bumps `reached` and parks on `resume.notified()` at exactly this
/// program point. The harness thread busy-polls `reached` (no sleeps) until
/// it sees the bump, fires the concurrent writer to full commit, THEN calls
/// `resume.notify_one()`, guaranteeing (by construction of the wait) that the
/// writer's publish strictly happens-before this tx resumes toward its own
/// publish — the precise interleaving the review's counter-example requires.
///
/// `nextest` runs each test in its own process (see
/// `TEST_MAX_TX_LIFETIME_OVERRIDE`'s doc), so this global cannot leak across
/// test files. Uninstalled (`None`, the default for every other test in the
/// suite) this is a single `OnceLock::get()` read with no lock, no
/// allocation, no await point taken.
#[cfg(any(test, feature = "test-util"))]
pub static TEST_POST_VALIDATE_PRE_PUBLISH_HOOK: std::sync::OnceLock<
    std::sync::Arc<PostValidatePrePublishHook>,
> = std::sync::OnceLock::new();

/// Parks on `TEST_POST_VALIDATE_PRE_PUBLISH_HOOK` if a test installed one;
/// a true no-op otherwise. See that static's doc for the exact program point
/// and rationale.
pub(super) async fn fire_post_validate_pre_publish_test_hook() {
    #[cfg(test)]
    if let Some(hook) = TEST_POST_VALIDATE_PRE_PUBLISH_HOOK.get() {
        hook.reached
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        // One-shot: only the FIRST tx to reach this seam actually parks (see
        // `PostValidatePrePublishHook::armed`'s doc for why this is load-
        // bearing, not cosmetic). `compare_exchange` CAS true→false so
        // exactly one racer wins even if two committers reached this line
        // concurrently; every later arrival (including the harness's own
        // concurrent writer, once IT reaches this same program point after
        // acquiring `commit_lock` in turn) sees `false` and returns at once.
        let should_pause = hook
            .armed
            .compare_exchange(
                true,
                false,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
            )
            .is_ok();
        if should_pause {
            hook.resume.notified().await;
        }
    }
}

#[cfg(any(test, feature = "test-util"))]
pub struct PostWalPrePublishHook {
    pub repo_name: String,
    pub pre_wal: std::sync::atomic::AtomicBool,
    pub after_counter: std::sync::atomic::AtomicBool,
    pub fail_orphan_markers: std::sync::atomic::AtomicBool,
    pub panic_after_resume: std::sync::atomic::AtomicBool,
    pub panic_resume: tokio::sync::Notify,
    pub reached: tokio::sync::Notify,
    pub resume: tokio::sync::Notify,
    pub armed: std::sync::atomic::AtomicBool,
}

#[cfg(any(test, feature = "test-util"))]
pub static TEST_POST_WAL_PRE_PUBLISH_HOOK: std::sync::OnceLock<
    std::sync::Arc<PostWalPrePublishHook>,
> = std::sync::OnceLock::new();

pub(in crate::tx) async fn fire_post_wal_pre_publish_test_hook(
    repo: &RepoCore,
    after_counter: bool,
) {
    #[cfg(any(test, feature = "test-util"))]
    if TEST_POST_WAL_PRE_PUBLISH_HOOK.get().is_some_and(|hook| {
        hook.after_counter
            .load(std::sync::atomic::Ordering::Acquire)
            != after_counter
    }) {
        return;
    }
    let _ = after_counter;
    fire_cancel_test_hook(repo, false).await;
}

pub(super) async fn fire_cancel_test_hook(repo: &RepoCore, pre_wal: bool) {
    #[cfg(any(test, feature = "test-util"))]
    if let Some(hook) = TEST_POST_WAL_PRE_PUBLISH_HOOK.get() {
        if hook.repo_name == repo.name()
            && hook.pre_wal.load(std::sync::atomic::Ordering::Acquire) == pre_wal
            && hook.armed.swap(false, std::sync::atomic::Ordering::AcqRel)
        {
            // Capture the one-shot panic mode before announcing the seam. A
            // separate wake targets the orphan while B1 keeps another ACTIVE
            // committer parked on the ordinary resume queue.
            let panic_after_resume = hook
                .panic_after_resume
                .swap(false, std::sync::atomic::Ordering::AcqRel);
            hook.reached.notify_one();
            if panic_after_resume {
                hook.panic_resume.notified().await;
                panic!("injected commit-tail panic");
            }
            hook.resume.notified().await;
        }
    }
    let _ = (repo, pre_wal);
}

/// Test-only one-shot pause for AsyncIndex's independently spawned tail.
#[cfg(any(test, feature = "test-util"))]
#[derive(Debug)]
pub struct AsyncIndexTailHook {
    pub repo_name: String,
    pub reached: tokio::sync::Notify,
    pub resume: tokio::sync::Notify,
    pub armed: std::sync::atomic::AtomicBool,
}

#[cfg(any(test, feature = "test-util"))]
pub static TEST_ASYNC_INDEX_TAIL_HOOK: std::sync::OnceLock<std::sync::Arc<AsyncIndexTailHook>> =
    std::sync::OnceLock::new();

pub(super) async fn fire_async_index_tail_test_hook(repo: &RepoCore) {
    #[cfg(test)]
    if let Some(hook) = TEST_ASYNC_INDEX_TAIL_HOOK.get() {
        if hook.repo_name == repo.name()
            && hook.armed.swap(false, std::sync::atomic::Ordering::AcqRel)
        {
            hook.reached.notify_one();
            hook.resume.notified().await;
        }
    }
    let _ = repo;
}

/// Test-only crash-injection seam for the real crash-recovery harness
/// (`crates/engine/shamir-engine/tests/crash_recovery.rs`, Vector II.1).
///
/// At each labelled point in the commit pipeline the harness can force a
/// HARD process death to prove atomicity around the Phase-4 commit point:
/// a crash *before* Phase 4 leaves nothing durable (clean abort); a crash
/// *at/after* Phase 4 but before Phase 7 leaves an inflight WAL marker
/// that recovery replays idempotently (all-or-nothing materialization).
///
/// Why `std::process::abort()` and not `panic!`:
///   `panic!` UNWINDS the stack — it runs every `Drop` impl on the way
///   out, which would flush `MemBuffer`/`Cached` write-backs, drop
///   storage handles cleanly, and let `RAII` guards release. That is a
///   *graceful* shutdown, the exact opposite of a crash. `process::abort`
///   raises `SIGABRT` immediately: no unwind, no `Drop`, no flush — the
///   closest in-process analog to `kill -9`. The on-disk state left
///   behind is therefore a genuine torn-mid-commit image, which is what
///   the recovery contract must survive.
///
/// Durability of the on-disk image (the `repo` argument):
///   The crash is real, but for the recovery contract to be *meaningful*
///   the WAL entry written in Phase 4 (and the Phase-5 row writes for the
///   later seams) must already be on disk when the process dies. Several
///   backends — redb's `Store::set` among them — use a deferred-fsync
///   write mode where rows become crash-durable only on an explicit
///   `flush()` (or, in production, on the MemBuffer flush tick). To model
///   a backend whose WAL/data writes are *synchronously* durable (the
///   durability mode the recovery guarantee assumes), every seam AT or
///   AFTER the commit point flushes the repo's shared store before
///   aborting. Because all of a repo's stores (WAL `__tx__`, per-table
///   `__data__`/`__info__`) share one backend handle (e.g. one redb
///   `Database`), a single `flush()` makes the entire pending image
///   durable. This flush is NOT a graceful shutdown: it is `fsync`, not
///   `Drop` — no buffers are drained through destructors, no guards
///   release, no in-memory tx state is reconciled. The `pre_commit` seam
///   deliberately does NOT flush: it fires BEFORE the WAL entry exists,
///   so the disk must show nothing tx-related (clean abort).
///
/// Gating (zero cost in production):
///   The whole hook is `#[cfg(debug_assertions)]`. In a normal
///   `--release` build `debug_assertions` is off, so `maybe_crash`
///   compiles to the empty `#[inline(always)]` twin below — dead code,
///   no env read, no branch, the `repo`/`phase` args unused. Tests build
///   in debug, where the hook is live but still a NO-OP unless
///   `SHAMIR_TEST_CRASH_AFTER` is set to a matching phase label. With no
///   env var present every existing test pays at most one `env::var` miss
///   per seam and never crashes or flushes.
///
/// Phase labels: `pre_commit`, `phase4`, `phase5a`, `phase5c`,
/// `phase5d_delta` (#426 VR-4 — after the pre-publish delta-chunk append,
/// before publish; proves the durable delta survives a crash and
/// `restore_on_open` replays it), `phase5d_delta_async` (the AsyncIndex
/// twin), `phase6`, `phase6_5`, `phase7`. D4 drain seam (fired from the drainer's per-entry
/// loop, not the ack-path): `drain_replay` — AFTER `replay_v2_entry` made the
/// entry durable in `history` but BEFORE `mark_durable` advanced the durable
/// watermark, so the entry is still inflight and recovery re-replays it
/// idempotently. F6c truncation seams (fired from the drainer, not the
/// ack-path): `pre_truncate` (before history-flush + segment unlink) and
/// `post_truncate` (after a successful `truncate_below`). A third truncation
/// seam, `wal_mid_delete`, lives inside `shamir-wal`'s
/// `SegmentSet::truncate_below` (it cannot reach this engine hook) and aborts
/// between two segment unlinks.
#[cfg(debug_assertions)]
pub(in crate::tx) async fn maybe_crash(phase: &str, repo: &RepoCore) {
    let Ok(target) = std::env::var("SHAMIR_TEST_CRASH_AFTER") else {
        return;
    };
    if target != phase {
        return;
    }
    // AT/AFTER the commit point: make the pending on-disk image durable
    // (fsync, NOT a graceful drain) so the killed process leaves a real
    // torn-mid-commit state the reopened repo must recover. `pre_commit`
    // skips this — nothing tx-related should be durable yet.
    if phase != "pre_commit" {
        eprintln!("maybe_crash: flushing before abort at {phase}");
        if let Ok(store) = repo.tx_info_store().await {
            let _ = store.flush().await;
        }
    }
    // HARD crash: no unwind, no Drop, no in-memory reconciliation.
    eprintln!("maybe_crash: abort at {phase}");
    std::process::abort();
}

/// Release twin: the crash seam does not exist outside debug builds.
#[cfg(not(debug_assertions))]
#[inline(always)]
pub(in crate::tx) async fn maybe_crash(_phase: &str, _repo: &RepoCore) {}

#[derive(Debug, thiserror::Error)]
pub enum TxError {
    #[error("request cancelled before commit")]
    Cancelled,
    #[error("WAL outcome indeterminate; repository must be reopened")]
    WalRequiresReopen,
    /// Ticket came back Unknown/Indeterminate AFTER the frame was queued; the
    /// attempt may have survived — never settle as aborted. WAL replay after
    /// reopen decides.
    #[error(
        "WAL outcome indeterminate (commit_version {commit_version}); repository must be reopened"
    )]
    WalOutcomeUnknown { commit_version: u64 },
    /// PR2 S5c-X: the retention gate is refusing writes — the retained
    /// sealed WAL exceeded `wal_retention_max_bytes` under `RefuseWrites`.
    /// Admission retries succeed once the drainer's declare-gap/truncate
    /// pass brings retention back under the cap.
    #[error("WAL retention limit exceeded; writes are refused until the journal catches up")]
    WalRetentionExceeded,
    /// R2a 1c: the commit admission gate refused this leader — either CLOSED
    /// past the bounded reopen-wait cap (`LEADER_REOPEN_WAIT_CAP`) or the
    /// bounded ACTIVE count overflowed. Retryable: the acquisition happens
    /// BEFORE `tx_gate` / `pre_commit_prelock`, so no side effect exists —
    /// the base interner is unchanged, no version was assigned, no WAL was
    /// appended. Succeeds once the capture barrier reopens the gate.
    #[error("commit admission refused; retry after the capture barrier reopens")]
    CommitAdmissionRefused,
    #[error("commit {commit_version} published but WAL durability failed")]
    WalDurability {
        commit_version: u64,
        materialization: MaterializationState,
        visibility: shamir_query_types::batch::VisibilityState,
    },
    #[error("table '{table}' is frozen for migration")]
    MigrationFrozen { table: String },
    #[error("storage: {0}")]
    Storage(#[from] DbError),
    #[error("ssi conflict on key {key:?}")]
    SsiConflict { key: bytes::Bytes },
    #[error("unique constraint violated on key {key:?}")]
    UniqueViolation { key: bytes::Bytes },
    #[error("tx expired: elapsed {elapsed:?} > max {max:?}")]
    Expired {
        elapsed: std::time::Duration,
        max: std::time::Duration,
    },
    /// Phase C. A concurrent committer wrote a key matching one of
    /// this tx's recorded predicate dependencies (a phantom). Surfaced
    /// to the wire path as `"tx_conflict"` so existing client retry
    /// covers it.
    #[error("phantom conflict on predicate {dep}")]
    PhantomConflict { dep: String },
    /// Level-3 wound-wait abort: an older (higher-priority) tx wounded
    /// this one during `lock_key`, or this tx observed its own wound flag
    /// at commit entry. Surfaced to the wire path as `"tx_conflict"` so
    /// existing client retry covers it.
    #[error("tx {} wounded (wound-wait abort)", tx_version)]
    Wounded { tx_version: u64 },
    /// FG-7: a CAS (`expected_version`) check failed at commit time — a
    /// concurrent committer changed the key's version between the
    /// immediate `check_expected_version` staging-time check and this tx's
    /// commit. Surfaced to the wire path as `"version_conflict"` (NOT
    /// `"tx_conflict"`) so client retry logic keyed on `version_conflict`
    /// (the immediate-check error code) handles BOTH failure timings
    /// identically — see `OPTIMISTIC_CONCURRENCY.md`.
    #[error("cas conflict on key {key:?}: expected version {expected} but found {found}")]
    CasConflict {
        key: bytes::Bytes,
        expected: u64,
        found: u64,
    },
}

impl TxError {
    /// Map commit errors without turning a published version into an abort.
    ///
    /// Every abort classification that already has a stable wire word keeps
    /// that word in `reason` UNCHANGED *and* publishes it through
    /// [`TransactionInfo::with_abort_code`], so a caller that must choose a
    /// disposition (the `for_each` arm, which fails an iteration with this
    /// code) never has to parse debug text. No code is invented here:
    /// classifications with no stable word (`Storage` without a
    /// [`DbError::code`], `Expired`) still leave `abort_code` absent.
    pub fn into_transaction_info(
        self,
        tx_id: u64,
        snapshot_version: u64,
    ) -> shamir_query_types::batch::TransactionInfo {
        let (reason, abort_code): (String, Option<&'static str>) = match self {
            Self::WalDurability {
                commit_version,
                materialization,
                visibility,
            } => {
                return TransactionInfo::committed_with_error(
                    tx_id,
                    snapshot_version,
                    commit_version,
                    materialization == MaterializationState::Complete,
                    "wal_durability",
                )
                .with_visibility(visibility);
            }
            Self::MigrationFrozen { .. } => {
                ("table_migrating".to_string(), Some("table_migrating"))
            }
            Self::SsiConflict { .. } | Self::PhantomConflict { .. } | Self::Wounded { .. } => {
                ("tx_conflict".to_string(), Some("tx_conflict"))
            }
            Self::UniqueViolation { .. } => {
                ("unique_violation".to_string(), Some("unique_violation"))
            }
            // FG-7: commit-time CAS failures retain the staging-time code.
            Self::CasConflict { .. } => ("version_conflict".to_string(), Some("version_conflict")),
            Self::WalRequiresReopen => (
                "wal_requires_reopen".to_string(),
                Some("wal_requires_reopen"),
            ),
            Self::WalOutcomeUnknown { commit_version } => {
                return TransactionInfo::indeterminate(tx_id, snapshot_version, commit_version);
            }
            Self::WalRetentionExceeded => (
                "wal_retention_exceeded".to_string(),
                Some("wal_retention_exceeded"),
            ),
            Self::CommitAdmissionRefused => (
                "commit_admission_refused".to_string(),
                Some("commit_admission_refused"),
            ),
            // Batch callers return cancellation on the error path instead.
            Self::Cancelled => ("cancelled".to_string(), Some("cancelled")),
            // Preserve a storage error's stable classification in both fields:
            // `reason` remains byte-for-byte the existing diagnostic/code, while
            // `abort_code` is populated only when DbError exposes a code.
            Self::Storage(e) => (
                e.code()
                    .map(str::to_owned)
                    .unwrap_or_else(|| format!("storage: {}", e)),
                e.code(),
            ),
            Self::Expired { elapsed, max } => (
                format!("tx expired: elapsed {:?} > max {:?}", elapsed, max),
                None,
            ),
        };
        let info = TransactionInfo::aborted(tx_id, reason);
        match abort_code {
            Some(code) => info.with_abort_code(code),
            None => info,
        }
    }
}

impl From<TxError> for DbError {
    fn from(error: TxError) -> Self {
        match error {
            error @ (TxError::WalRequiresReopen
            | TxError::WalOutcomeUnknown { .. }
            | TxError::WalRetentionExceeded
            | TxError::CommitAdmissionRefused
            | TxError::WalDurability { .. }) => DbError::Storage(error.to_string()),
            TxError::Storage(error) => error,
            TxError::MigrationFrozen { table } => DbError::MigrationFrozen { table },
            error @ (TxError::SsiConflict { .. }
            | TxError::PhantomConflict { .. }
            | TxError::Wounded { .. }) => DbError::Conflict(error.to_string()),
            error @ TxError::UniqueViolation { .. } => DbError::DuplicateKey(error.to_string()),
            error @ TxError::CasConflict { .. } => DbError::VersionConflict(error.to_string()),
            // DbError has no expiry category; retain the diagnostic without
            // misclassifying expiry as a retryable transaction conflict.
            error @ (TxError::Expired { .. } | TxError::Cancelled) => {
                DbError::Internal(error.to_string())
            }
        }
    }
}

mod durability_completion;
pub(in crate::tx) use durability_completion::commit_wal_durability;
pub use durability_completion::wal_completion;
#[cfg(any(test, feature = "test-util"))]
#[doc(hidden)]
pub use durability_completion::TEST_SYNCED_WAL_REPO;

/// cancel-safe: yes — read-only over `&TxContext`. The only `.await`
/// is `interner_overlay.scan_async` which is itself cancel-safe (the
/// borrowed map is untouched on drop); the rest is in-memory iteration.
/// No external state mutation happens here.
///
/// Build WalOpV2 ops from a TxContext for inclusion in the V2 WAL entry.
///
/// Emitted ops in order:
/// - CounterDelta per table.
/// - InternerOverlayMerge (if overlay non-empty).
/// - IndexPut / IndexDel from index_write_set (table_id_interned from
///   per-op table_token stamped at write time; idx_id=0 placeholder).
/// - Put / Delete from write_set snapshot (carry table_id_interned
///   so recovery can resolve target data_store).
/// - BumpFtsStats is in-memory only and not serialised.
pub async fn wal_ops_from_tx(tx: &TxContext) -> Vec<WalOpV2> {
    let mut ops =
        Vec::with_capacity(tx.counter_deltas.len() + tx.index_write_set.len() + tx.write_set.len());

    for (table_id, delta) in &tx.counter_deltas {
        ops.push(WalOpV2::CounterDelta {
            table_id_interned: *table_id,
            delta: *delta,
        });
    }

    // L10(c): skip the async scan when the overlay is empty. `is_empty()`
    // on `scc::HashMap` is NOT an atomic length read (scc exposes no atomic
    // length — `len()` is banned by `clippy.toml`'s `disallowed-methods`
    // precisely because it is O(N)); `is_empty` walks the bucket array
    // until it finds the first entry or exhausts it. That bucket-walk is
    // sync and short-circuits on the first non-empty bucket, so it stays
    // cheap on the hot path and avoids a needless `.await` point. Race
    // with a concurrent `insert` is benign: the overlay is tx-scoped and
    // only the owning tx mutates it, so the emptiness check is stable.
    if !tx.interner_overlay.is_empty() {
        // O(N) ack: per-tx interner delta sizing, bounded by this tx's touches.
        #[allow(clippy::disallowed_methods)]
        let mut entries: Vec<(u64, String)> = Vec::with_capacity(tx.interner_overlay.len());
        tx.interner_overlay
            .iter_async(|k, v| {
                entries.push((*v, k.clone()));
                true
            })
            .await;
        if !entries.is_empty() {
            ops.push(WalOpV2::InternerOverlayMerge { entries });
        }
    }

    for (table_token, op) in &tx.index_write_set {
        match op {
            shamir_tx::IndexWriteOp::SetPosting { key, value, .. } => {
                ops.push(WalOpV2::IndexPut {
                    table_id_interned: *table_token,
                    idx_id: 0,
                    key: key.clone(),
                    value: value.clone(),
                });
            }
            shamir_tx::IndexWriteOp::RemovePosting { key, .. } => {
                ops.push(WalOpV2::IndexDel {
                    table_id_interned: *table_token,
                    idx_id: 0,
                    key: key.clone(),
                });
            }
            shamir_tx::IndexWriteOp::BumpFtsStats { .. } => {}
        }
    }

    // Phase 4 data ops: snapshot each per-table StagingStore (no
    // consume — drain happens in Phase 5). This makes the WAL entry
    // self-contained: recovery can replay tx data writes without
    // needing the (still-staged) StagingStore around.
    for (table_id, staging) in &tx.write_set {
        for kv_op in staging.iter_ops() {
            match kv_op {
                shamir_storage::types::KvOp::Set(k, v) => {
                    if let Some(rid) = shamir_types::types::record_id::RecordId::try_from_bytes(&k)
                    {
                        ops.push(WalOpV2::Put {
                            table_id_interned: *table_id,
                            rid,
                            body: v,
                        });
                    }
                }
                shamir_storage::types::KvOp::Remove(k) => {
                    if let Some(rid) = shamir_types::types::record_id::RecordId::try_from_bytes(&k)
                    {
                        ops.push(WalOpV2::Delete {
                            table_id_interned: *table_id,
                            rid,
                        });
                    }
                }
            }
        }
    }

    ops
}

/// cancel-safe: partial — cancellation during prelock (including unique-lock
/// waits) still aborts. AFTER prelock, cancellation only stops waiting: the
/// exact suspended commit future runs to completion on the captured runtime,
/// even before WAL acceptance. Do not blindly retry an unobserved outcome.
/// Inline panics propagate without restarting the future; detached panics
/// are logged and existing VersionGuard orphan recovery remains the fallback.
/// Shutdown must stop admission and quiesce commits before stopping Tokio.
/// AsyncIndex retains its separately spawned materialization tail, counted
/// through completion by the same quiescence barrier.
///
/// Thin metrics-only wrapper around `commit_tx_inner`: it records the
/// `on_tx_aborted_storage` metric on a PRE-commit storage abort and is
/// otherwise transparent.
///
/// Metric semantics shift (Vector I.3 / MED-A): post-Phase-4 storage
/// failures no longer surface as `Err(TxError::Storage)` aborts — they
/// become `Ok(TxOutcome { materialization: Deferred, .. })`. So every
/// `Err(TxError::Storage)` reaching this wrapper is by construction a
/// PRE-commit abort, and `on_tx_aborted_storage` correctly counts only
/// those. Deferred materialization is observable three ways:
/// `TxOutcome::materialization`, the `log::warn!` emitted in `materialize`,
/// and the `TxMetrics::txs_materialization_deferred` counter
/// (`on_tx_materialization_deferred`, fired in `commit_tx_inner` when
/// `materialize` returns `Deferred`).
///
/// There is NOTHING to do for HNSW staging on a pre-commit abort —
/// staged vectors live inside the `TxContext` (`staged_vectors`) and
/// vanish when the tx is dropped on the `Err` return below. That is the
/// RAII win: no per-tx buffer outlives the `TxContext`, so no broadcast
/// drain is needed (HIGH-6).
pub async fn commit_tx(tx: TxContext, repo: &RepoCore) -> Result<TxOutcome, TxError> {
    commit_tx_with_options(tx, repo, repo.commit_visibility_wait()).await
}

/// Orchestrates the commit pipeline around the WAL commit point.
///
/// The boundary is explicit: `pre_commit` runs Phases 1–4 and may
/// return a real abort `Err` (nothing durable yet). A *successful*
/// `pre_commit` means Phase 4 `wal.begin` landed — that durable WAL
/// entry IS the commit. From there [`crate::materialize::materialize`] runs Phases 5–7
/// best-effort and NEVER aborts: a projection failure is logged and
/// deferred to recovery, the version is still published, and the tx is
/// reported COMMITTED (with `MaterializationState::Deferred`).
///
/// C6 empty-tx fast-path: when `pre_commit` returns `Ok(None)` (the tx
/// staged nothing durable, after SSI validation passed) this function
/// short-circuits — no version assigned, no WAL write, no publish — and
/// returns `Ok(TxOutcome { commit_version: snapshot_version, materialization:
/// Complete, .. })`.
pub(in crate::tx) async fn commit_tx_inner(
    mut tx: TxContext,
    repo: &RepoCore,
    cancel: Option<Arc<CancelHandle>>,
) -> Result<TxOutcome, TxError> {
    // Level-3 wound-wait: a tx wounded by an older (higher-priority) tx must
    // abort even if it finished all its statements. The check runs at commit
    // entry so a wounded tx can never commit. No-op for Snapshot / Serializable
    // (their wounded flag stays false — they never take locks).
    if let Err(tx_version) = tx.ensure_not_wounded() {
        release_pessimistic_locks(&tx, repo).await;
        return Err(TxError::Wounded { tx_version });
    }

    let max_tx_lifetime = effective_max_tx_lifetime();
    if tx.is_expired(max_tx_lifetime) {
        release_pessimistic_locks(&tx, repo).await;
        repo.tx_metrics().on_tx_aborted_expired();
        return Err(TxError::Expired {
            elapsed: tx.elapsed(),
            max: max_tx_lifetime,
        });
    }

    // R2a 1c: commit admission gate (design `online-backup.md` section 2).
    // Acquired after the wound/expiry early exits and BEFORE `tx_gate` and
    // `pre_commit_prelock` (which would merge the overlay into the base
    // interner), so an overlay-only leader blocked here has touched NOTHING.
    // A closed gate waits for reopen bounded by `LEADER_REOPEN_WAIT_CAP`,
    // then returns a typed retryable error with no side effect. The lease
    // moves INTO the `CommitDriver` continuation below — through the
    // suspended-future transfer on drop, the AsyncIndex tail handoff and the
    // spawned panic cleanup — and is released only after publication and
    // every mutation-capable tail / error cleanup. Every early-return error
    // path below simply drops it (RAII), releasing the count.
    let admission_lease = match repo
        .commit_admission()
        .acquire_leader(LEADER_REOPEN_WAIT_CAP)
        .await
    {
        Ok(lease) => lease,
        Err(_) => {
            release_pessimistic_locks(&tx, repo).await;
            return Err(TxError::CommitAdmissionRefused);
        }
    };

    // Group 15 (2026-08-14 review, SUMMARY.md 6.2): both resolutions can
    // hit REAL I/O on first touch (`repo_wal()` does `create_dir_all` +
    // `SegmentSet::open`) — mirror every other early-exit path in this
    // function and release Level-3 locks before propagating the error, so
    // a Pessimistic tx never leaks `locked_keys` when its commit happens
    // to be the repo's first WAL touch.
    let gate = match repo.tx_gate().await {
        Ok(g) => g,
        Err(e) => {
            release_pessimistic_locks(&tx, repo).await;
            return Err(e.into());
        }
    };
    if gate.requires_reopen() {
        release_pessimistic_locks(&tx, repo).await;
        return Err(TxError::WalRequiresReopen);
    }
    // PR2 S5c-X: retention refusal — the ENTIRE hot-path cost is this one
    // atomic load (plus the pre-existing lock release on the early exit).
    if repo.wal_retention_refuse_writes() {
        release_pessimistic_locks(&tx, repo).await;
        return Err(TxError::WalRetentionExceeded);
    }
    let wal = match repo.repo_wal().await {
        Ok(w) => w,
        Err(e) => {
            release_pessimistic_locks(&tx, repo).await;
            return Err(e.into());
        }
    };

    if wal.requires_reopen() {
        gate.set_requires_reopen();
        release_pessimistic_locks(&tx, repo).await;
        return Err(TxError::WalRequiresReopen);
    }

    // === PRE-LOCK SECTION (concurrent across committers) ===
    //
    // Phase 1 (interner overlay merge + remap) and Phase 2.5 + 2.6
    // (uwl_guards acquisition + unique re-validation) run OUTSIDE
    // `commit_lock`. Phase 1 is CAS-safe on DashMap (concurrent merges
    // converge). Phase 2.5/2.6 serialize on per-table uwl_guards in
    // sorted token order — no global serialization needed. Two committers
    // touching disjoint unique tables proceed fully in parallel here.
    let PreLockResult {
        uwl_guards,
        drain_guards,
    } = match pre_commit_prelock(&mut tx, repo, cancel.as_deref()).await {
        Ok(r) => r,
        Err(e) => {
            release_pessimistic_locks(&tx, repo).await;
            return Err(e);
        }
    };

    // Own the foreground clone, tx and prelock guards before either downstream
    // body takes commit_lock / the serializable guard. No spawn or yield on
    // the happy path; backpressure remains in the caller-only wrapper.
    crate::CommitDriver::new(tx, repo, move |tx, repo| async move {
        let _tail = repo.commit_tail().enter();
        // Implicit commits run serially within a request; later writes inherit
        // the first commit's decision. Never reset the state from a driver tail.
        // Both interactive commit routes arrive without an explicit handle.
        // Use the request handle already owned by the context, if any. The
        // driver owns the context even when the caller stops waiting.
        match cancel.as_deref().or_else(|| tx.cancel_handle()) {
            Some(cancel) if cancel.state() != CancelState::Committing && !cancel.try_commit() => {
                release_pessimistic_locks(&tx, &repo).await;
                return Err(if cancel.is_cancelled() {
                    TxError::Cancelled
                } else {
                    DbError::Internal("commit requires an active request".into()).into()
                });
            }
            _ => {}
        }
        // AsyncIndex txs bypass group-commit: their post-commit tail is spawned
        // as a background task, which is incompatible with the leader processing
        // another tx's materialization. They always wait for the lock.
        if tx.visibility == shamir_tx::CommitVisibility::AsyncIndex {
            commit_tx_inner_legacy_async(
                tx,
                uwl_guards,
                drain_guards,
                &repo,
                gate.as_ref(),
                wal.as_ref(),
                admission_lease,
            )
            .await
        } else {
            // === P2c: LOCKFREE COMMIT PATH ===
            //
            // Disjoint-table commits run fully in parallel — no global
            // `commit_mutex`. Correctness relies on per-table uwl_guards:
            // same-table committers serialize at uwl acquisition (pre-lock);
            // disjoint-table committers never conflict in SSI predicates.
            //
            // Sequence (all lock-free or per-table-locked):
            //   1. validate (atomic version + lock-free TreeIndex scan)
            //   2. WAL begin (unique key per txn_id — concurrent-safe)
            //   3. record_commit_writes (lock-free TreeIndex insert)
            //   4. materialize (gated by uwl_guards, not commit_mutex)
            //
            // Group-commit WAL batching is sacrificed for parallelism; the
            // WAL backend's internal batching (if any) still amortizes fsync.
            // R2a 1c: the admission lease lives until the lock-free body —
            // including every mutation-capable tail — has completed inline.
            let _admission_lease = admission_lease;
            commit_tx_lockfree(
                tx,
                uwl_guards,
                drain_guards,
                &repo,
                gate.as_ref(),
                wal.as_ref(),
            )
            .await
        }
    })
    .await
}

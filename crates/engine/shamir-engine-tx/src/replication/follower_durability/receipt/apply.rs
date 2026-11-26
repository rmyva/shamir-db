//! Orchestration (S5c-X PR6b-2): route one pull batch through the receipt
//! pipeline — resume an outstanding receipt if any, otherwise chunk the
//! prepared events and run Prepared → Materialized → adopt-counters →
//! checkpoint (bookmark commit + RAM publication in one poll) per chunk.
//!
//! Ordering (PR6b-3): admission (`admit_follower`, durable role validation,
//! typed refusals) → adoption (a `Fresh` verdict durably writes the v3
//! baseline) → visibility (`enable_receipt_native_visibility`, derived from
//! durable role state — Native record or the baseline just written), and
//! only then any version allocation, table resolution or receipt staging.
//!
//! Cancellation: the pipeline runs on the REPOSITORY-OWNED lifecycle worker
//! (`resume::worker`, ONE lifecycle spawn per repo — the merged
//! `ForegroundLifecycle` foundation). The caller submits a bounded job and
//! awaits only the oneshot reply; dropping the caller future drops that
//! await only — the worker retains the job (with its own repo clone) and
//! runs it to completion or to a durable pause point. Bounded: `submit` is
//! `try_send` into the lifecycle's capacity-bounded queue; the write-domain
//! permit inside the pipeline serializes applies (one at a time). Errors
//! while fresh chunk guards are held PARK those guards in the lease
//! (finding 2); retries re-load the durable receipt so the recorded
//! counters are never recomputed from the live cache after Materialized
//! committed (finding 3).

use std::sync::Arc;

use shamir_collections::TFxMap;
use shamir_storage::error::{DbError, DbResult};
use shamir_storage::types::Store;
use shamir_tx::RepoTxGate;

use super::super::super::{prepare_event, ApplyOutcome, ApplyPolicy, PreparedEvent};
#[cfg(test)]
use super::super::test_seam::{self, Phase};
use super::resume::activation::{admit_follower, FollowerAdmission};
use super::resume::worker::ReceiptLease;
use super::{checkpoint, publish, resume, stage};
use crate::commit::core::RepoCore;
use crate::meta::repl::bookmark::FenceRecord;
use crate::meta::repl::bookmark_v3::{load_bookmark_v3, save_bookmark_v3, BookmarkV3};
use crate::meta::repl::{ReceiptBinding, ReceiptPhase, ReplicationReceipt, MAX_RECEIPT_EVENTS};
use crate::meta::repl_bookmark::load_replication_bookmark;
use crate::repo::changelog_store::write_domain::{RepoWritePermit, RepoWriteReservation};
use crate::ReplicatedEvent;

pub(crate) async fn apply_batch_receipt(
    repo: &RepoCore,
    info: Arc<dyn Store>,
    events: Vec<ReplicatedEvent>,
    bookmark: u64,
    policy: ApplyPolicy,
) -> DbResult<ApplyOutcome> {
    // Repository-owned bounded lifecycle worker (review finding 4, round 2):
    // the entry lazily installs the worker on the FIRST submission, then
    // submits and awaits only the oneshot reply — a cancelled caller drops
    // the receiver, and the worker KEEPS RUNNING the job with its own repo
    // clone. No orphan `tokio::spawn` outside the lifecycle's
    // `install_worker`.
    repo.submit_receipt_job(info, events, bookmark, policy)
        .await
}

/// Scope-owned paused-reservation guard: a reservation taken from the lease
/// must survive the WHOLE resume. Dropping it early clears the write-domain
/// slot and lets ordinary writes/DDL interleave before the receipt retires.
/// Restored on every durable-pause exit (Drop); disarmed by [`Self::release`]
/// once the receipt has retired and published.
struct PausedReservation<'a> {
    lease: &'a ReceiptLease,
    reservation: Option<RepoWriteReservation>,
}

impl PausedReservation<'_> {
    /// The receipt retired and published: the pause is over — do not restore.
    fn release(&mut self) {
        self.reservation = None;
    }
}

impl Drop for PausedReservation<'_> {
    fn drop(&mut self) {
        if let Some(reservation) = self.reservation.take() {
            self.lease.place_pause_reservation(reservation);
        }
    }
}

pub(crate) async fn run_receipt_pipeline(
    repo: &RepoCore,
    info: Arc<dyn Store>,
    events: Vec<ReplicatedEvent>,
    bookmark: u64,
    policy: ApplyPolicy,
    lease: &ReceiptLease,
) -> DbResult<ApplyOutcome> {
    let gate = repo.tx_gate().await?;
    // Repository write-admission domain for the receipt path (PR6b-2c): the
    // DOMAIN permit excludes DDL and every other repository mutation for the
    // pipeline's whole run — there is deliberately NO second admission
    // mechanism here. Lock order (per #183): the permit FIRST, the marker
    // write mutex second. When the lease carries a paused reservation (the
    // previous failure parked one), admission goes through
    // `admit_with_reservation`; the reservation is taken OUT of the lease's
    // mutex BEFORE the await and restored on failure, so no std-sync guard
    // is ever held across `.await` and the future stays `Send`.
    // Table managers self-admit through the SAME write domain on first
    // init (`TableManager::create_with_domain` → `admit()`); the pipeline
    // holds the domain permit for its whole run, so every table below is
    // resolved ADMITTED (`get_table_admitted`) — never re-entering
    // `admit()` under the held permit. Unconfigured tables keep the legacy
    // `Rejected` handling inside `prepare_event`.
    let (permit, mut pause) = match lease.take_reservation() {
        Some(reservation) => {
            match repo
                .write_domain()
                .admit_with_reservation(&reservation)
                .await
            {
                Ok(permit) => (
                    permit,
                    PausedReservation {
                        lease,
                        reservation: Some(reservation),
                    },
                ),
                Err(error) => {
                    // Admission refused: the pause stays exactly as it was.
                    lease.place_pause_reservation(reservation);
                    return Err(admission_error(repo, error));
                }
            }
        }
        None => {
            let permit = repo
                .write_domain()
                .admit()
                .await
                .map_err(|error| admission_error(repo, error))?;
            (
                permit,
                PausedReservation {
                    lease,
                    reservation: None,
                },
            )
        }
    };
    let _marker = repo.marker_write_mutex().lock().await;
    // PR6b-3 ordering: ADMISSION (durable role validation; typed refusals
    // propagate) → ADOPTION (a fresh repo durably writes the v3 baseline —
    // bookmark 0, no fence, no J, no receipt — BEFORE any version is
    // allocated, any table resolved, or any receipt staged) → VISIBILITY
    // (enabled below, derived from durable role state, never
    // "opportunistically" by the first receipt).
    match admit_follower(repo, &info).await? {
        FollowerAdmission::Native => {}
        FollowerAdmission::Fresh => {
            save_bookmark_v3(
                &info,
                &BookmarkV3 {
                    fence: None,
                    bookmark: 0,
                    clean_through: None,
                    outstanding_receipt: None,
                },
            )
            .await?;
            log::info!(
                "follower admission: adopted fresh repo {} with a v3 baseline \
                 (bookmark 0, no fence, no clean horizon, no receipt)",
                repo.name()
            );
        }
    }
    // Receipt activation is the provenance boundary for native visibility.
    // Enable before any receipt materialization can allocate versions.
    gate.enable_receipt_native_visibility();
    if gate.requires_reopen() {
        return Err(DbError::RepoRequiresReopen(repo.name().to_string()));
    }
    if let Some(outstanding) = resume::lookup(repo, &info).await? {
        // Full identity check (finding 1c) BEFORE any resume work and
        // before loading bookmark v3: fence lineage/epoch, offered
        // coverage, bindings, and byte-identical re-translation.
        resume::identity::verify_identity(repo, &info, &outstanding.receipt, &events, &permit)
            .await?;
        let tail = resume::tail_events(&outstanding.receipt, &events);
        let v3 = load_bookmark_v3(&info).await?.ok_or_else(|| {
            DbError::RepoRequiresReopen("replication bookmark vanished during resume".into())
        })?;
        let old = BookmarkV3 {
            fence: v3.fence,
            bookmark: outstanding.receipt.predecessor_bookmark,
            clean_through: v3.clean_through,
            outstanding_receipt: None,
        };
        // Converge both resume phases on the SAME publication/durability row
        // building — but NOT on re-commit (reviewer P1 #6): the Materialized
        // phase is ALREADY durable — its rows, counters and receipt bytes
        // were committed by the phase that wrote it (mirroring the inline
        // fast path in `run_chunk_tail`), so re-running
        // `commit_materialized_work` would be a pure rewrite of durable
        // state. Control flow:
        //   Prepared    -> materialize (compute counters vs live pre-state,
        //                  nothing published yet) -> commit Materialized
        //                  batch -> publication -> durable_tail
        //   Materialized-> publication built STRAIGHT from the STORED
        //                  receipt (decoded downstream events + recorded
        //                  absolute counters) -> durable_tail
        let (receipt, publication) = match outstanding.receipt.phase {
            ReceiptPhase::Prepared => {
                let work =
                    stage::materialize_from_prepared_receipt(repo, &permit, &outstanding.receipt)
                        .await?;
                let (receipt, _counters) =
                    stage::commit_materialized_work(repo, &permit, &info, &work, &old).await?;
                let publication =
                    publish::publication_from_receipt(repo, &permit, &receipt).await?;
                (receipt, publication)
            }
            ReceiptPhase::Materialized => {
                let publication =
                    publish::publication_from_receipt(repo, &permit, &outstanding.receipt).await?;
                (outstanding.receipt.clone(), publication)
            }
            ReceiptPhase::Checkpointed => {
                return Err(DbError::RepoRequiresReopen(
                    "bookmark references a Checkpointed receipt".into(),
                ));
            }
        };
        let floor = publication.floor;
        let new_bookmark = durable_tail(
            repo,
            &permit,
            &info,
            (&gate, lease),
            (&receipt, &old),
            publication,
            Some(&mut pause),
        )
        .await?;
        lease.retire_parked();
        // `durable_tail` disarms the scope guard synchronously at publication,
        // before checkpoint's fallible final flush.
        if !tail.is_empty() {
            let tail: Vec<ReplicatedEvent> = tail.into_iter().cloned().collect();
            let tail_outcome = apply_fresh(
                repo,
                &permit,
                &info,
                &gate,
                &tail,
                new_bookmark,
                policy,
                lease,
            )
            .await?;
            if let ApplyOutcome::Applied { .. } = tail_outcome {
                return Ok(tail_outcome);
            }
        }
        return Ok(ApplyOutcome::Applied {
            local_version: floor,
        });
    }
    apply_fresh(
        repo, &permit, &info, &gate, &events, bookmark, policy, lease,
    )
    .await
}

/// Map a write-domain admission rejection to the repo's `Internal` error
/// style (see repo_instance.rs's mapping).
fn admission_error(
    repo: &RepoCore,
    error: crate::repo::changelog_store::write_domain::RepoWriteError,
) -> DbError {
    DbError::Internal(format!(
        "receipt pipeline admission rejected for repo {}: {error}",
        repo.name()
    ))
}

/// The fresh path: translate the whole batch (rejections stay pre-write, like
/// the legacy batch path), then chunk the prepared events greedily under the
/// receipt's combined structural bounds and run the pipeline per chunk.
#[allow(clippy::too_many_arguments)]
async fn apply_fresh(
    repo: &RepoCore,
    permit: &RepoWritePermit,
    info: &Arc<dyn Store>,
    gate: &Arc<RepoTxGate>,
    events: &[ReplicatedEvent],
    bookmark: u64,
    policy: ApplyPolicy,
    lease: &ReceiptLease,
) -> DbResult<ApplyOutcome> {
    // Design step 1: the DURABLE bookmark is consulted BEFORE any version
    // allocation, so a replayed pull carrying a stale caller watermark can
    // never allocate replacement versions over versions the durable state
    // already owns. (The resume path already consults the receipt before
    // anything else.) The caller-supplied `bookmark` is kept for
    // compatibility, but the durable floor always applies.
    let durable_bookmark = load_replication_bookmark(info).await?;
    let watermark = bookmark.max(durable_bookmark);
    let mut prepared: Vec<PreparedEvent> = Vec::with_capacity(events.len());
    let mut origins: Vec<usize> = Vec::new();
    for (index, event) in events.iter().enumerate() {
        match prepare_event(
            repo,
            &event.event,
            &event.field_bindings,
            watermark,
            policy,
            Some(permit),
        )
        .await?
        {
            Ok(event) => {
                prepared.push(event);
                origins.push(index);
            }
            Err(ApplyOutcome::Skipped) => {}
            Err(rejected) => return Ok(rejected),
        }
    }
    if prepared.is_empty() {
        return Ok(ApplyOutcome::Skipped);
    }
    // Preparation awaited I/O; re-check admission before allocation.
    if gate.requires_reopen() {
        return Err(DbError::RepoRequiresReopen(repo.name().to_string()));
    }
    let mut old = match load_bookmark_v3(info).await? {
        Some(bookmark) => BookmarkV3 {
            outstanding_receipt: None,
            ..bookmark
        },
        // After strict loading and mandatory admission/adoption, `None` is a
        // contradiction: the v3 baseline exists or the pipeline wrote it.
        None => {
            return Err(DbError::RepoRequiresReopen(
                "replication bookmark v3 baseline vanished".into(),
            ))
        }
    };

    let mut start = 0usize;
    while start < prepared.len() {
        // Greedy chunking under the COMBINED structural bound: each event
        // costs 3 (mapping + downstream event + timestamp) plus TWICE its
        // op count — the Materialized receipt adds one counter entry per
        // distinct table (≤ ops), and the bound is checked again by
        // `encode_receipt` at that phase.
        let mut combined = 0usize;
        let mut end = start;
        while end < prepared.len() {
            let cost = 3 + 2 * prepared[end]
                .tables
                .iter()
                .map(|table| table.ops.len())
                .sum::<usize>();
            if combined + cost > MAX_RECEIPT_EVENTS && end > start {
                break;
            }
            combined += cost;
            end += 1;
        }
        let mut guards = Vec::with_capacity(end - start);
        for event in &mut prepared[start..end] {
            let mut guard = gate.assign_next_version_guarded();
            // Receipt materialization writes follower-visible rows before local
            // publication. A panic in that window must reopen, never Aborted-mark
            // the allocated version and advance the floors beneath those rows.
            guard.visibility_started_without_wal();
            event.version = Some(guard.version());
            guards.push(guard);
        }
        // Guard ownership across errors (finding 2): from allocation until
        // publication the worker OWNS these guards — on ANY failure they are
        // PARKED in the lease (never dropped), because a dropped guard
        // marks Aborted and advances the watermarks past still-unpublished
        // versions while the repo keeps serving. Storage failures retry the
        // idempotent pipeline inline (the durable receipt is the redo
        // authority); the 3rd failure or any NON-retryable error parks and
        // returns. Parked guards retire on the next successful publication,
        // or at worker shutdown under quarantine (requires_reopen).
        let mut attempt = 0usize;
        let (new_bookmark, chunk_local_end) = loop {
            match try_chunk(
                repo,
                permit,
                info,
                gate,
                events,
                &origins[start..end],
                &prepared[start..end],
                &old,
                lease,
            )
            .await
            {
                Ok(done) => break done,
                Err(error) => {
                    attempt += 1;
                    if !matches!(error, DbError::Storage(_)) || attempt >= 3 {
                        let parked: Vec<_> = guards.drain(0..end - start).collect();
                        lease.park(parked);
                        // Paused reservation (PR6b-2c): only when a receipt
                        // is OUTSTANDING — the parked guards alone do not
                        // justify rejecting ordinary admission repo-wide. A
                        // failure with NO outstanding receipt (before the
                        // Prepared commit) places NO reservation.
                        if matches!(resume::lookup(repo, info).await, Ok(Some(_))) {
                            if let Some(reservation) = repo.write_domain().try_place_reservation() {
                                lease.place_pause_reservation(reservation);
                            }
                        }
                        return Err(error);
                    }
                    log::warn!(
                        "follower receipt chunk failed (attempt {attempt}/3), \
                         retrying: {error}"
                    );
                }
            }
        };
        // The chunk published and is durable. Retire parked prior guards,
        // then commit this chunk's original allocation guards (not drop them
        // armed after publication's separate reowned guards committed).
        lease.retire_parked();
        // Publication owns the reowned guards; explicitly discharge the
        // original allocation guards rather than dropping them armed.
        for guard in guards.drain(0..end - start) {
            guard.commit();
        }
        old = BookmarkV3 {
            fence: old.fence.map(|fence| FenceRecord {
                bookmark: new_bookmark,
                ..fence
            }),
            bookmark: new_bookmark,
            clean_through: checkpoint::certified_clean_through(repo, &old, chunk_local_end).await?,
            outstanding_receipt: None,
        };
        start = end;
    }
    Ok(ApplyOutcome::Applied {
        local_version: gate.version_allocation_high_water_mark(),
    })
}

/// One retryable attempt of one fresh chunk: build the plan, durably commit
/// the Prepared receipt (idempotent — same receipt bytes — so a fallible
/// flush after the Prepared commit retries instead of dropping the chunk's
/// guards), then the post-Prepared tail.
#[allow(clippy::too_many_arguments)]
async fn try_chunk(
    repo: &RepoCore,
    permit: &RepoWritePermit,
    info: &Arc<dyn Store>,
    gate: &Arc<RepoTxGate>,
    events: &[ReplicatedEvent],
    origins: &[usize],
    prepared: &[PreparedEvent],
    old: &BookmarkV3,
    lease: &ReceiptLease,
) -> DbResult<(u64, u64)> {
    let bindings = collect_bindings(events, origins);
    let leader_start = prepared[0].event.commit_version;
    let leader_end = prepared[prepared.len() - 1].event.commit_version;
    let mut plan = stage::build_plan(
        repo,
        prepared,
        bindings,
        old.bookmark,
        leader_start,
        leader_end,
    )
    .await?;
    // Phase-aware retry (finding 3): once this chunk's Materialized receipt
    // is durable, rewriting the Prepared-phase bytes over it would ERASE the
    // recorded absolute counters and force the retry to recompute them from
    // the live cache (where the batch's own history rows read back as
    // pre-existing) — skip the rewrite and let `run_chunk_tail` replay the
    // recorded Materialized receipt. Matching is by CONTENT (`same_batch`),
    // not by `receipt_id`: the id is a hash of phase+counters (see
    // `stage::set_counters`), so a freshly rebuilt Prepared plan can never
    // share it with the durable Materialized receipt even for the identical
    // batch.
    // Hoisted so the phase check and the post-checkpoint reconciliation
    // below share ONE durable-receipt read.
    let stored = resume::lookup(repo, info).await?;
    let already_materialized = matches!(
        stored.as_ref(),
        Some(outstanding)
            if outstanding.receipt.phase == ReceiptPhase::Materialized
                && same_batch(&outstanding.receipt, &plan.receipt),
    );
    if !already_materialized {
        if stored.is_none() {
            // A failure AFTER checkpoint/publication (e.g. the fallible info
            // flush at the end of `checkpoint`) lands here: the receipt is
            // retired and the durable bookmark already covers this chunk.
            // Re-staging Prepared would REWIND the durable bookmark —
            // reconcile from the advanced bookmark and treat the chunk as
            // done instead.
            let chunk_local_end = prepared[prepared.len() - 1]
                .version
                .ok_or_else(|| DbError::Storage("prepared event lost its local version".into()))?;
            if let Some(v3) = load_bookmark_v3(info).await? {
                // Completion evidence is the checkpoint Uncertain read-back
                // (checkpoint.rs:328-333): durable bookmark advanced AND
                // receipt retired — written in ONE atomic batch
                // (checkpoint.rs:263-280), so observing both means the chunk
                // is committed and published. `clean_through` (J) is
                // gap-capped (checkpoint.rs:233-248) and does NOT evidence
                // completion: with an open gap below this chunk, J stays at
                // the gap while the bookmark + retired receipt + watermarks
                // prove the chunk is done.
                if v3.outstanding_receipt.is_none()
                    && v3.bookmark >= leader_end
                    && gate.last_committed() >= chunk_local_end
                    && gate.durable_watermark() >= chunk_local_end
                {
                    return Ok((v3.bookmark, chunk_local_end));
                }
            }
        }
        stage::commit_prepared(repo, permit, info, &plan, old).await?;
        #[cfg(test)]
        fail_after(repo.name(), Phase::Prepared)?;
    }
    run_chunk_tail(repo, permit, info, gate, &mut plan, old, lease).await
}

/// Post-Prepared tail of one fresh chunk: the idempotent materialization,
/// the durable Materialized batch, the publication build, and the
/// durability tail. Returns `(new_bookmark, chunk_local_end)`.
async fn run_chunk_tail(
    repo: &RepoCore,
    permit: &RepoWritePermit,
    info: &Arc<dyn Store>,
    gate: &Arc<RepoTxGate>,
    plan: &mut stage::BatchPlan,
    old: &BookmarkV3,
    lease: &ReceiptLease,
) -> DbResult<(u64, u64)> {
    // Phase-aware, counter-idempotent retry (finding 3): on EACH attempt,
    // load the durable outstanding receipt FIRST. If this chunk's
    // Materialized receipt is already durable (content-matched via
    // `same_batch`), the recorded absolute counters are replayed — never
    // recomputed from the live cache/MVCC, where the batch's own history
    // rows read back as pre-existing and the absolute count would be
    // overwritten with a wrong value — and the durable batch is NOT
    // re-staged: the pipeline goes straight to the checkpoint tail. Only
    // when no Materialized receipt is durable yet is `materialize_from_plan`
    // used (counters computed once against the true pre-state).
    let stored = resume::lookup(repo, info).await?;
    if matches!(
        stored.as_ref(),
        Some(outstanding)
            if outstanding.receipt.phase == ReceiptPhase::Materialized
                && same_batch(&outstanding.receipt, &plan.receipt)
    ) {
        // Materialized -> straight to the checkpoint (round 4 / finding 1a):
        // the durable matching receipt IS the Materialized bytes —
        // re-staging/committing the batch would be a pure rewrite AND the
        // publication is built STRAIGHT from the STORED receipt
        // (`publication_from_receipt`: decoded downstream events + the
        // receipt's own recorded absolute counters + ops segmentation), NOT
        // from the freshly rebuilt plan — the stored receipt is the single
        // durable authority for what gets published. Then go directly to
        // `durable_tail` (journal verify/repair + confirm + checkpoint all
        // happen inside it, so every idempotency property is kept).
        #[expect(
            clippy::expect_used,
            reason = "guard: matches! above proves stored is Some and its receipt matches the plan"
        )]
        let stored = &stored.expect("matched above").receipt;
        // Test seam on the FAST path too (reviewer P1 #2): without this
        // check a sticky `fail_after = Phase::Materialized` flag could never
        // bite after the first Materialized commit — the retry takes this
        // fast path straight to `durable_tail` and succeeds, so a test
        // expecting the pipeline to PARK on the sticky flag would observe a
        // successful retry instead. Attempt trace with the flag sticky:
        //   attempt 1: no stored receipt -> materialize+commit -> seam fires
        //              (Storage "injected") -> retry
        //   attempt 2: fast path -> seam fires again -> retry
        //   attempt 3: fast path -> seam fires -> attempt >= 3 -> PARK.
        #[cfg(test)]
        fail_after(repo.name(), Phase::Materialized)?;
        let publication = publish::publication_from_receipt(repo, permit, stored).await?;
        let chunk_local_end = stored
            .mapping
            .iter()
            .map(|mapping| mapping.local_version)
            .max()
            .unwrap_or(0);
        let new_bookmark = durable_tail(
            repo,
            permit,
            info,
            (gate, lease),
            (stored, old),
            publication,
            None,
        )
        .await?;
        return Ok((new_bookmark, chunk_local_end));
    }
    let work = stage::materialize_from_plan(repo, permit, plan).await?;
    let (receipt, counters) =
        stage::commit_materialized_work(repo, permit, info, &work, old).await?;
    #[cfg(test)]
    fail_after(repo.name(), Phase::Materialized)?;
    let publication = publish::publication_from_plan(repo, permit, plan, &counters).await?;
    let chunk_local_end = receipt
        .mapping
        .iter()
        .map(|mapping| mapping.local_version)
        .max()
        .unwrap_or(0);
    let new_bookmark = durable_tail(
        repo,
        permit,
        info,
        (gate, lease),
        (&receipt, old),
        publication,
        None,
    )
    .await?;
    Ok((new_bookmark, chunk_local_end))
}

/// Shared post-materialization durability tail: verify journal coverage
/// (repairing through the WRITER, never direct writes, when a Materialized
/// receipt's rows are missing), confirm the feed, adopt the absolute
/// counters, checkpoint bookmark+J WITH the RAM publication in the same
/// poll, then broadcast.
async fn durable_tail(
    repo: &RepoCore,
    permit: &RepoWritePermit,
    info: &Arc<dyn Store>,
    gate_and_lease: (&Arc<RepoTxGate>, &ReceiptLease),
    receipt_and_old: (&ReplicationReceipt, &BookmarkV3),
    publication: publish::Publication,
    pause: Option<&mut PausedReservation<'_>>,
) -> DbResult<u64> {
    let (receipt, old) = receipt_and_old;
    let (gate, lease) = gate_and_lease;
    if let Err(first) = checkpoint::verify_journal(repo, receipt).await {
        log::warn!(
            "follower receipt journal coverage incomplete ({first}); repairing \
             via install_durable_events"
        );
        let handle = repo.changefeed().await?;
        let records = stage::journal_records(receipt)?;
        handle
            .feed
            .install_durable_events(records)
            .await
            .map_err(|error| DbError::Storage(format!("receipt journal repair: {error}")))?;
        checkpoint::verify_journal(repo, receipt).await?;
    }
    let local_end = receipt
        .mapping
        .iter()
        .map(|mapping| mapping.local_version)
        .max()
        .unwrap_or(0);
    checkpoint::confirm_journal(repo, local_end).await?;
    publish::adopt_counters(repo, &publication).await?;
    checkpoint::checkpoint(
        repo,
        permit,
        info,
        receipt,
        old,
        (gate, &publication),
        checkpoint::PublicationCompletion {
            lease: Some(lease),
            on_published: || {
                if let Some(pause) = pause {
                    lease.clear_reservation();
                    pause.release();
                    lease.retire_parked();
                }
            },
        },
    )
    .await?;
    let floor = publication.floor;
    for event in &publication.events {
        repo.broadcast_changefeed_event(Some(Arc::clone(&event.downstream)));
    }
    log::info!(
        "follower receipt: applied batch through local version {floor} \
         (upstream {}..={})",
        receipt.upstream_start,
        receipt.upstream_end
    );
    Ok(old.bookmark.max(receipt.upstream_end))
}

/// Phase-invariant CONTENT match between the durable receipt and the freshly
/// rebuilt plan (finding 1b — full identity, not just the range): the exact
/// local allocations (`mapping`), the upstream batch boundary
/// (`upstream_start`/`upstream_end`), the rollback point
/// (`predecessor_bookmark`), the fence identity (`lineage_id`,
/// `upstream_epoch`), the field-name `bindings`, and the downstream event
/// BYTES. All of these are fixed at Prepared time by `build_plan` (which
/// reads fence lineage/epoch and serializes the downstream bytes) and never
/// change when the receipt is rewritten to Materialized, so the predicate
/// matches the identical batch in either phase and rejects any different
/// batch. `receipt_id` itself is NOT usable across phases: it hashes the
/// whole receipt including phase AND counters (see `stage::set_counters`),
/// and the fresh plan is always rebuilt as Prepared with empty counters, so
/// the freshly computed id can never equal the durable Materialized id even
/// for the identical batch.
fn same_batch(stored: &ReplicationReceipt, plan_receipt: &ReplicationReceipt) -> bool {
    stored.mapping == plan_receipt.mapping
        && stored.upstream_start == plan_receipt.upstream_start
        && stored.upstream_end == plan_receipt.upstream_end
        && stored.predecessor_bookmark == plan_receipt.predecessor_bookmark
        && stored.lineage_id == plan_receipt.lineage_id
        && stored.upstream_epoch == plan_receipt.upstream_epoch
        && stored.bindings == plan_receipt.bindings
        && stored.downstream_events == plan_receipt.downstream_events
}

/// Inject the test seam's one-shot (or sticky) post-phase failure: returns
/// `DbError::Storage("injected")` — or `DbError::RepoRequiresReopen("injected")`
/// when the reopen-kind flag is set — exactly when the flag fires for `phase`.
#[cfg(test)]
fn fail_after(repo_name: &str, phase: Phase) -> DbResult<()> {
    if test_seam::take_fail_after(repo_name, phase) {
        if test_seam::fail_after_reopen(repo_name) {
            Err(DbError::RepoRequiresReopen("injected".into()))
        } else {
            Err(DbError::Storage("injected".into()))
        }
    } else {
        Ok(())
    }
}

/// Deduplicated `(interned_id, name)` bindings of the chunk's original
/// events, in first-seen order. Conflicts were already rejected by
/// `prepare_event`.
fn collect_bindings(events: &[ReplicatedEvent], origins: &[usize]) -> Vec<ReceiptBinding> {
    let mut seen = TFxMap::<u64, ()>::default();
    let mut bindings = Vec::new();
    for &origin in origins {
        for (id, name) in &events[origin].field_bindings {
            if seen.insert(*id, ()).is_none() {
                bindings.push(ReceiptBinding {
                    interned_id: *id,
                    name: name.clone(),
                });
            }
        }
    }
    bindings
}

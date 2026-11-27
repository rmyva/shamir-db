use std::hash::{BuildHasher, Hash, Hasher};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use arc_swap::ArcSwapOption;
use shamir_collections::TFxMap;
use shamir_connect::common::push_envelope::{PushEnvelope, PushKind};
use shamir_connect::server::conn_services::PushSink;
use shamir_db::access::{Action, Actor, ResourcePath};
use shamir_db::engine::table::InternerReadView;
use shamir_db::shamir_db::ChangelogDeliveryGate;
use shamir_db::types::value::QueryValue;
use shamir_db::ShamirDb;
use shamir_query_builder::batch::Batch;
use shamir_query_builder::query::Query;
use shamir_query_types::batch::BatchLimits;
use shamir_query_types::filter::Filter;
use shamir_query_types::subscribe::deliver_mode::DeliverMode;
use shamir_query_types::subscribe::event_mask::EventMask;
use shamir_query_types::subscribe::source::SubscriptionSource;
use shamir_tx::ChangeOp;
use tracing::Instrument;

use crate::subscriptions::db_journal_source::DbJournalSource;
use crate::subscriptions::decode_cache::{cache_evict_up_to, cache_get, cache_insert, CachedBytes};
use crate::subscriptions::deliver_cache::{
    deliver_cache_evict_up_to, deliver_cache_get, deliver_cache_insert,
};
use crate::subscriptions::filter_eval::{bytes_to_arc, CompiledFilterSlot};
use crate::subscriptions::journal_drain::JournalDrain;
use crate::subscriptions::journal_source::wait_for_closed;
use crate::subscriptions::push::{make_deliver_data, push_required_gated, try_push_event_gated};
use crate::subscriptions::registry::SubscriptionRegistry;
use crate::subscriptions::target_match::{
    any_target_interested_indexed, build_target_index, matches_any_indexed,
};
#[cfg(test)]
use crate::subscriptions::tests::bridge::bridge_snapshot_cut_tests;

/// RAII drop-guard that removes a subscription's registry slot on EVERY
/// bridge-task exit path — natural stream completion, an early error return,
/// the CRIT-5 "all sources denied" abort, or external task cancellation via
/// `JoinHandle::abort()` (tokio unwinds through in-scope locals at the next
/// await point, firing this `Drop`).
///
/// Without this the `active` counter and `subs` map entry would leak whenever
/// `bridge_task` finishes on its own (finding 2b / former #513): `remove()` /
/// `close_all()` were the ONLY decrement sites, so a bridge that exited
/// without an explicit client `Unsubscribe` permanently consumed a slot.
///
/// Idempotency: in the explicit-`Unsubscribe` path `registry.remove(id)` is
/// already called by the handler *before* the abort lands, so this guard's
/// later `remove` is a no-op — `scc::HashMap::remove` on an already-gone key
/// returns `None`, and [`SubscriptionRegistry::remove`] only decrements
/// `active` when the map entry was actually present, so the counter is never
/// double-decremented.
struct SubscriptionSlotGuard {
    registry: Arc<SubscriptionRegistry>,
    sub_id: u64,
}

impl Drop for SubscriptionSlotGuard {
    fn drop(&mut self) {
        self.registry.remove(self.sub_id);
    }
}

/// Convert cached raw bytes + interner to a `QueryValue` for delivery.
///
/// Delegates to the read view's engine-side decoder (zero-copy `RecordView`
/// lens first; full `InnerValue` decode fallback for bare-scalar / non-map
/// records).
fn bytes_to_query_value(bytes: &[u8], interner: &InternerReadView) -> Option<QueryValue> {
    match interner.record_bytes_to_query_value(bytes) {
        Ok(value) => value,
        Err(error) => {
            tracing::warn!(%error, "subscription delivery: failed to de-intern record bytes");
            None
        }
    }
}

/// Bridge task: subscribes to the changefeed broadcast for the relevant
/// repos, filters events by table name and event mask, and pushes
/// `PushEnvelope` frames through the connection's `PushSink`.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn bridge_task(
    sub_id: u64,
    db: Arc<ShamirDb>,
    db_name: String,
    sources: Vec<SubscriptionSource>,
    deliver: DeliverMode,
    actor: Actor,
    push: Arc<dyn PushSink>,
    from_version: Option<u64>,
    initial: bool,
    // Operator-configured query limits (finding 2b-ii). Reactive Batch/Call
    // deliveries run under the subscribing actor's identity and MUST be
    // bounded by these limits, not `BatchLimits::default()`.
    query_limits: BatchLimits,
    // Per-connection registry (finding 2b / former #513): the guard below uses
    // it to release this subscription's slot when the bridge exits on its own.
    registry: Arc<SubscriptionRegistry>,
) {
    // Release this subscription's registry slot on EVERY exit path. Bound
    // before any `.await` so cancellation-unwind fires its `Drop` too.
    let slot_guard = SubscriptionSlotGuard { registry, sub_id };

    let started = std::time::Instant::now();
    let activated = AtomicBool::new(false);
    let seq = AtomicU64::new(0);
    // Unique per ShamirDb instance -- prevents deliver-cache pollution across
    // distinct in-memory databases in tests.
    let db_id = Arc::as_ptr(&db) as u64;
    // Decode/deliver cache discriminator: combines the instance pointer AND
    // the database NAME. The instance ptr alone is identical for every
    // database hosted by one ShamirDb (the production / e2e shape: one server,
    // many `createDb` databases), so a key without the db name lets two
    // databases that share a repo name ("main") and overlapping low
    // commit-versions collide in the GLOBAL cache — returning another db's
    // record bytes for filter evaluation. Hashing in `db_name` isolates them.
    let db_disc: u64 = {
        let mut h = shamir_collections::THasher::default().build_hasher();
        db_id.hash(&mut h);
        db_name.hash(&mut h);
        h.finish()
    };

    let run = async {
        // Authorization is rechecked at activation in case ACLs changed
        // after Authorized preflight. Any unavailable source fails the whole
        // activation; never filter it out and continue with a partial stream.
        for source in &sources {
            let member = db
                .get_db(&db_name)
                .is_some_and(|repo_db| repo_db.has_table(&source.table.repo, &source.table.table));
            let path = ResourcePath::Table {
                db: db_name.clone(),
                store: source.table.repo.clone(),
                table: source.table.table.clone(),
            };
            if !member
                || db
                    .authorize_access(&actor, &path, Action::Read)
                    .await
                    .is_err()
            {
                tracing::warn!(
                    sub_id,
                    repo = %source.table.repo,
                    table = %source.table.table,
                    "subscription activation failed: source is no longer readable",
                );
                return;
            }
        }

        let targets: Vec<(String, String, EventMask, Option<Filter>)> = sources
            .iter()
            .map(|s| {
                (
                    s.table.repo.clone(),
                    s.table.table.clone(),
                    s.events.clone(),
                    s.filter.clone(),
                )
            })
            .collect();

        // Gather unique repos.
        let repos: Vec<String> = {
            let mut r: Vec<String> = targets.iter().map(|(repo, _, _, _)| repo.clone()).collect();
            r.sort();
            r.dedup();
            r
        };

        // Subscribe to each repo's changefeed FIRST -- before journal backfill --
        // so we don't miss events between journal read and live subscription.
        let mut receivers = Vec::with_capacity(repos.len());
        let mut delivery_gates: Vec<ChangelogDeliveryGate> = Vec::with_capacity(repos.len());
        let mut states = Vec::with_capacity(repos.len());
        for repo in &repos {
            match db.subscribe_changelog(&db_name, repo).await {
                Ok(Some(rx)) => {
                    delivery_gates.push(rx.delivery_gate());
                    receivers.push(rx);
                    // Attach before sampling W; never use a post-snapshot maximum.
                    let Some(w) = db.changelog_visible_floor(&db_name, repo).await else {
                        tracing::warn!(sub_id, repo, "activation cut unavailable");
                        return;
                    };
                    // Journal versions start at 1: resume 0 includes all history,
                    // just like resume 1; saturating subtraction preserves both.
                    let cursor = from_version.map_or(w, |fv| fv.saturating_sub(1));
                    states.push(JournalDrain::new(cursor));
                    tracing::info!(
                        sub_id,
                        repo = %repo,
                        activation_floor = w,
                        cursor,
                        from_version = ?from_version,
                        attach_us = started.elapsed().as_micros() as u64,
                        "changefeed attached; commits above the cursor are delivered live",
                    );
                }
                // S5c-X PR8b: a quarantined (`resync_required`) repo is
                // refused (`DbError::ResyncRequired`) — the bridge must NOT
                // attach a live event stream (nor run the initial snapshot)
                // against a repo whose reads are quarantined.
                Ok(None) => {
                    tracing::warn!(sub_id, repo = %repo, "changefeed not available");
                    return;
                }
                Err(error) => {
                    tracing::warn!(sub_id, repo = %repo, %error, "changefeed refused");
                    return;
                }
            }
        }

        // The changefeed receivers are attached and the activation cut is sampled: from here
        // every commit is delivered live. Release the activation barrier the `Subscribe`
        // handler is waiting on, so the client's ack never precedes the attach.
        tracing::info!(
            sub_id,
            ready_us = started.elapsed().as_micros() as u64,
            "bridge live; releasing the activation barrier",
        );
        slot_guard.registry.signal_ready(sub_id);
        activated.store(true, Ordering::Release);

        let terminal_gates = delivery_gates.clone();
        let bridge_work = async {
            // repo name -> index (built once; repos are fixed at subscribe time).
            let repo_idx: TFxMap<String, usize> = repos
                .iter()
                .enumerate()
                .map(|(i, r)| (r.clone(), i))
                .collect();
            // O(1) target index: (repo_idx, table) -> target indices.
            // Built once at subscribe time; replaces the two O(T) linear scans.
            let target_index = build_target_index(&targets, &repo_idx);

            // Per-target compiled-filter cache slots (parallel to `targets`).
            // `Some(slot)` for targets with a filter (lazily populated on first
            // event, recompiled when the interner grows); `None` for targets
            // without a filter. This avoids re-running `compile_filter` (which
            // includes `Regex::new`, `TSet` construction, `String` clones) on
            // every event × every subscriber.
            let filter_slots: Vec<Option<CompiledFilterSlot>> = targets
                .iter()
                .map(|(_, _, _, filter)| filter.as_ref().map(|_| ArcSwapOption::empty()))
                .collect();
            // The activation W remains the live-only floor; initial snapshots
            // replace it with their pinned transaction cut before live delivery.
            let source = DbJournalSource {
                db: &db,
                name: &db_name,
            };
            let deliver_event = |ri: usize, event: shamir_tx::ChangelogEvent| {
                let repo = &repos[ri];
                let gate = &delivery_gates[ri];
                let targets = &targets;
                let target_index = &target_index;
                let filter_slots = &filter_slots;
                let deliver = &deliver;
                let db = &db;
                let db_name = &db_name;
                let actor = &actor;
                let query_limits = &query_limits;
                let push = &push;
                let seq = &seq;
                async move {
                    tracing::debug!(
                        repo = %repo,
                        commit_version = event.commit_version,
                        changes = event.changes.len(),
                        "delivering journal event",
                    );
                    // Any page may have been read before quarantine. A closed gate
                    // aborts before examining or accepting page contents.
                    if gate.closed_now() {
                        return false;
                    }
                    for (change_idx, change) in event.changes.iter().enumerate() {
                        // O(1) pre-check via target index: skip the async
                        // interner lookup entirely when no target could match.
                        if !any_target_interested_indexed(
                            targets,
                            target_index,
                            ri,
                            &change.table,
                            &change.op,
                        ) {
                            tracing::trace!(
                                repo = %repo,
                                commit_version = event.commit_version,
                                table = ?change.table,
                                "change skipped: no interested target",
                            );
                            continue;
                        }
                        // Cache raw bytes + interner per event across all
                        // bridge tasks: the cache deduplicates the interner
                        // lookup so that N subscribers pay O(1).
                        // No InnerValue decode on the filter path -- the
                        // RecordView lens reads fields zero-copy.
                        let decoded_arc: CachedBytes = match (&change.op, change.value.as_deref()) {
                            (ChangeOp::Put, Some(bytes)) => {
                                if let Some(cached) =
                                    cache_get(db_disc, repo, event.commit_version, change_idx)
                                {
                                    cached
                                } else {
                                    let entry = db
                                        .get_table_interner_read_view(db_name, repo, &change.table)
                                        .await
                                        .map(|interner| (bytes_to_arc(bytes), interner));
                                    cache_insert(
                                        db_disc,
                                        repo,
                                        event.commit_version,
                                        change_idx,
                                        entry,
                                    )
                                }
                            }
                            _ => {
                                static NONE_ARC: std::sync::OnceLock<CachedBytes> =
                                    std::sync::OnceLock::new();
                                Arc::clone(NONE_ARC.get_or_init(|| Arc::new(None)))
                            }
                        };
                        let bytes_ref = (*decoded_arc).as_ref();
                        if !matches_any_indexed(
                            targets,
                            filter_slots,
                            target_index,
                            ri,
                            &change.table,
                            &change.op,
                            bytes_ref,
                        ) {
                            tracing::debug!(
                                repo = %repo,
                                commit_version = event.commit_version,
                                table = ?change.table,
                                "change skipped: filtered out by the subscription",
                            );
                            continue;
                        }
                        // Lazy decode of the interner-encoded value into a
                        // QueryValue. Only `DeliverMode::Records` reads it
                        // (via `make_event_data`); Keys/Batch/Call never do.
                        // We therefore defer the de-intern until we know we're
                        // on a Records deliver-cache MISS -- on cache hits and
                        // in every other mode the decode is skipped entirely.
                        let deliver_mode_disc = match deliver {
                            DeliverMode::Records => Some(0u8),
                            DeliverMode::Keys => Some(1u8),
                            _ => None,
                        };
                        // For Records/Keys the payload is shared via Arc --
                        // we borrow it for serialization (zero-copy fan-out).
                        // For Batch/Call the payload is built per-subscriber.
                        let cached_arc;
                        let owned_buf;
                        let data_ref: &[u8] = if let Some(mode) = deliver_mode_disc {
                            if let Some(arc) = deliver_cache_get(
                                db_disc,
                                repo,
                                event.commit_version,
                                change_idx,
                                mode,
                            ) {
                                cached_arc = arc;
                                &cached_arc
                            } else {
                                // Cache MISS in the Records/Keys shared branch.
                                // Only Records actually consumes value_qv
                                // (`make_event_data`); Keys/Batch/Call ignore
                                // it, so gate the de-intern on Records (mode 0)
                                // to skip it entirely in Keys mode.
                                let value_qv: Option<QueryValue> = if mode == 0 {
                                    match bytes_ref {
                                        Some((bytes, cell)) => bytes_to_query_value(bytes, cell),
                                        None => None,
                                    }
                                } else {
                                    None
                                };
                                let built = make_deliver_data(
                                    deliver,
                                    db,
                                    db_name,
                                    actor,
                                    change,
                                    value_qv.as_ref(),
                                    event.commit_version,
                                    query_limits,
                                )
                                .await;
                                cached_arc = deliver_cache_insert(
                                    db_disc,
                                    repo,
                                    event.commit_version,
                                    change_idx,
                                    mode,
                                    built,
                                );
                                &cached_arc
                            }
                        } else {
                            // Batch/Call: `make_deliver_data` ignores the
                            // value_qv arg, so pass None and skip the decode.
                            owned_buf = make_deliver_data(
                                deliver,
                                db,
                                db_name,
                                actor,
                                change,
                                None,
                                event.commit_version,
                                query_limits,
                            )
                            .await;
                            &owned_buf
                        };
                        if !try_push_event_gated(gate, push, sub_id, seq, data_ref).await {
                            tracing::warn!(
                                repo = %repo,
                                commit_version = event.commit_version,
                                "event push failed; closing the subscription",
                            );
                            return false;
                        }
                        tracing::debug!(
                            repo = %repo,
                            commit_version = event.commit_version,
                            table = ?change.table,
                            seq = seq.load(Ordering::Relaxed),
                            bytes = data_ref.len(),
                            "event pushed",
                        );
                    }
                    // Eviction is performance-only: slower subscribers rebuild misses
                    // from their own journal pages; cursors need not agree.
                    if event.commit_version > 1 {
                        cache_evict_up_to(event.commit_version - 1);
                        deliver_cache_evict_up_to(event.commit_version - 1);
                    }
                    true
                }
            };
            let emit_gap = |at| {
                let push = &push;
                let seq = &seq;
                async move {
                    let envelope = PushEnvelope {
                        push: PushKind::Gap,
                        sub: sub_id,
                        seq: seq.load(Ordering::Relaxed),
                        data: None,
                        gap_at: Some(at),
                    };
                    if let Ok(frame) = rmp_serde::to_vec_named(&envelope) {
                        let _ = push.try_push(frame);
                    }
                }
            };
            if from_version.is_some() {
                if let Err(error) = JournalDrain::run(
                    &source,
                    &repos,
                    &mut receivers,
                    &mut states,
                    true,
                    deliver_event,
                    emit_gap,
                )
                .await
                {
                    tracing::warn!(sub_id, %error, "journal resume terminated");
                    return;
                }
            }

            // Initial snapshot: read existing records for subscribed tables.
            if initial {
                #[cfg(test)]
                bridge_snapshot_cut_tests::pause(0, 0).await;
                for (ri, repo) in repos.iter().enumerate() {
                    let (mut tx, guard, _) = match db
                        .tx_begin_as(actor.clone(), &db_name, repo, "snapshot")
                        .await
                    {
                        Ok(open) => open,
                        Err(error) => {
                            tracing::warn!(sub_id, %error, "initial snapshot begin failed");
                            return;
                        }
                    };
                    let cut = tx.snapshot_version;
                    #[cfg(test)]
                    bridge_snapshot_cut_tests::pause(1, cut).await;
                    #[cfg(test)]
                    let mut first_target_of_repo = true;
                    for (target_repo, target_table, _mask, filter) in targets.iter() {
                        if target_repo != repo {
                            continue;
                        }
                        let mut query =
                            Query::with_repo(target_repo.as_str(), target_table.as_str()).build();
                        if filter.is_some() {
                            query.r#where = filter.clone();
                        }
                        let mut batch = Batch::new();
                        batch.query("_snapshot", query);
                        batch.return_all();
                        // Match the existing server-owned reactive-read limits policy.
                        // Each target read has its own cooperative execution budget.
                        batch.limits(query_limits.clone());
                        let find_req = match batch.try_build() {
                            Ok(req) => req,
                            Err(error) => {
                                tracing::warn!(sub_id, %error, "snapshot batch build failed");
                                return;
                            }
                        };

                        match db
                            .tx_execute_as(actor.clone(), &db_name, &find_req, &mut tx)
                            .await
                        {
                            Ok(response) => {
                                #[cfg(test)]
                                bridge_snapshot_cut_tests::pause(4, cut).await;
                                for qr in response.results.values() {
                                    for record in &qr.records {
                                        let record_val = record.as_value();
                                        let key_qv: QueryValue = record_val
                                            .get("_id")
                                            .cloned()
                                            .unwrap_or(QueryValue::Null);
                                        let value_qv: QueryValue = record_val.into_owned();
                                        #[derive(serde::Serialize)]
                                        struct SnapshotEvent<'x> {
                                            table: &'x str,
                                            op: &'x str,
                                            key: &'x QueryValue,
                                            value: &'x QueryValue,
                                            commit_version: u64,
                                        }
                                        let obj = SnapshotEvent {
                                            table: target_table,
                                            op: "put",
                                            key: &key_qv,
                                            value: &value_qv,
                                            commit_version: 0,
                                        };
                                        let data = match rmp_serde::to_vec_named(&obj) {
                                            Ok(data) => data,
                                            Err(error) => {
                                                tracing::warn!(
                                                    sub_id,
                                                    %error,
                                                    "snapshot serialization failed",
                                                );
                                                return;
                                            }
                                        };
                                        if delivery_gates[ri].closed_now() {
                                            return;
                                        }
                                        if !try_push_event_gated(
                                            &delivery_gates[ri],
                                            &push,
                                            sub_id,
                                            &seq,
                                            &data,
                                        )
                                        .await
                                        {
                                            return;
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::warn!(
                                    sub_id,
                                    table = %target_table,
                                    "initial snapshot failed: {e}",
                                );
                                return;
                            }
                        }
                        #[cfg(test)]
                        if std::mem::take(&mut first_target_of_repo) {
                            bridge_snapshot_cut_tests::pause(2, cut).await;
                        }
                    }
                    // Read-only rollback is synchronous: TxContext drops staged
                    // state without I/O; SnapshotGuard removes the GC pin on every
                    // exit, including query/push failure and task cancellation.
                    drop(tx);
                    drop(guard);
                    states[ri].cursor = states[ri].cursor.max(cut);
                }

                let s = seq.load(Ordering::Relaxed);
                let ready = PushEnvelope {
                    push: PushKind::Ready,
                    sub: sub_id,
                    seq: s,
                    data: None,
                    gap_at: None,
                };
                let frame = match rmp_serde::to_vec_named(&ready) {
                    Ok(frame) => frame,
                    Err(error) => {
                        tracing::warn!(sub_id, %error, "control serialization failed");
                        return;
                    }
                };
                if let Some(gate) = delivery_gates.first() {
                    if !push_required_gated(gate, &push, sub_id, &seq, frame).await {
                        return;
                    }
                }
            }

            // Initial drain is mandatory even without a future broadcast hint.
            for state in &mut states {
                state.request_drain();
            }
            if let Err(error) = JournalDrain::run(
                &source,
                &repos,
                &mut receivers,
                &mut states,
                false,
                deliver_event,
                emit_gap,
            )
            .await
            {
                tracing::warn!(sub_id, %error, "journal subscription terminated");
            }
        };
        let first_closed = wait_for_closed(&terminal_gates);
        tokio::select! {
            biased;
            _ = first_closed => (),
            () = bridge_work => (),
        }
    };

    let span = tracing::info_span!("sub_bridge", sub_id);
    run.instrument(span.clone()).await;
    span.in_scope(|| {
        tracing::info!(
            delivered_frames = seq.load(Ordering::Relaxed),
            lifetime_ms = started.elapsed().as_millis() as u64,
            "bridge exited",
        );
    });

    // A bridge that exits before signalling readiness failed activation. It has
    // no client-visible grant, so emitting Closed would leak a frame for an
    // unknown subscription. Once activated, self-exits remain observable.
    if activated.load(Ordering::Acquire) {
        let s = seq.load(Ordering::Relaxed);
        let closed = PushEnvelope {
            push: PushKind::Closed,
            sub: sub_id,
            seq: s,
            data: None,
            gap_at: None,
        };
        if let Ok(frame) = rmp_serde::to_vec_named(&closed) {
            let _ = push.try_push(frame);
        }
    }
}

//! Offset (row-count) bookmark fetching plus the `FetchNext` /
//! `CancelCursor` wire handlers.

use shamir_connect::server::session::Session;
use shamir_db::engine::query::filter::eval_context::FilterContext;
use shamir_db::engine::table::TableManager;
use shamir_db::query::batch::BatchError;
use shamir_db::query::read::{At, Pagination, QueryResult, ReadQuery, Temporal};
use shamir_query_types::wire::CursorId;
use shamir_types::types::common::{new_map, TMap};

use crate::byte_budget::stash_serialized_response;
use crate::cursor_registry::{CursorRegistryError, PaginationMode};

use super::budget::{
    enforce_page_budget, error_response, reserve_page_budget_upfront, wrap_engine_err,
    wrap_table_lookup_err,
};
use super::keyset::{
    build_filter_context, fetch_keyset_page, order_by_field_value, row_record_id, KeysetOutcome,
};
use crate::db_handler::cursor_support::{authorize_cursor_read, resolve_repo};
use crate::db_handler::handler::{error_code, session_actor, DbResponse, ShamirDbHandler};
use crate::db_handler::operator_state::table_unavailable_response;
use crate::db_handler::visibility_handlers::quarantine_cursor_error;

/// Outcome of [`fetch_offset_page`]: the up-to-`effective_page_size` rows
/// for this call, whether more remain, and the advanced row-count bookmark.
pub(super) struct OffsetPage {
    result: QueryResult,
    has_more: bool,
    new_offset: u64,
}

/// Row-count `Pagination::LimitOffset` fetch used by both a genuinely
/// `PaginationMode::Offset` cursor's every `FetchNext` AND, since CR-D1
/// (#782), a `Keyset`-mode cursor that just detected
/// [`KeysetOutcome::StuckAtCeiling`] and is falling back to this bookmark
/// for the REST of its lifetime (see `fetch_next`'s dispatch match for the
/// exactly-once correctness argument — `offset` was already being
/// maintained in parallel on the Keyset branch, tracking the TRUE
/// cumulative row count regardless of which mode was active).
///
/// Same peek-ahead-by-one-row trick as `create_cursor`'s first page: fetch
/// `effective_page_size + 1` internally so the true end-of-data can be told
/// apart from an exact multiple of `effective_page_size`. Widen to `u64`
/// before the `+1`; `Pagination::LimitOffset::limit` is `Option<u64>`, so
/// this cannot overflow.
pub(super) async fn fetch_offset_page(
    table: &TableManager,
    ctx: &FilterContext<'_>,
    base_query: &ReadQuery,
    offset: u64,
    effective_page_size: u32,
    pinned_version: u64,
) -> Result<OffsetPage, BatchError> {
    let internal_limit = (effective_page_size as u64).saturating_add(1);
    let mut next_query = base_query.clone();
    next_query.pagination = Pagination::LimitOffset {
        limit: Some(internal_limit),
        offset,
    };
    next_query.temporal = Temporal::AsOf {
        at: At::Version(pinned_version),
    };
    let mut fetched = table
        .read_with_encoding(&next_query, ctx, Default::default())
        .await
        .map_err(wrap_engine_err)?;

    // The peek row (if present) proves at least one more record remains
    // beyond `effective_page_size` — trim it off before it's handed to the
    // client or counted toward the returned stats, and advance the offset
    // bookmark by the RETURNED count only (advancing by the peek-inflated
    // fetched count would skip a row on the next page).
    let has_more = fetched.records.len() as u64 > effective_page_size as u64;
    if has_more {
        fetched.records.truncate(effective_page_size as usize);
        if let Some(stats) = fetched.stats.as_mut() {
            stats.records_returned = fetched.records.len() as u64;
        }
    }
    let new_offset = offset + fetched.records.len() as u64;
    Ok(OffsetPage {
        result: fetched,
        has_more,
        new_offset,
    })
}

impl ShamirDbHandler {
    pub(in crate::engine::db_handler) async fn remove_cursor_identity(
        &self,
        id: u64,
        cursor: &std::sync::Arc<crate::cursor_registry::Cursor>,
    ) {
        #[cfg(test)]
        self.cursor_registry.pause_before_remove(id).await;
        self.cursor_registry.remove_identity(id, cursor);
    }

    /// FG-5b FETCH_NEXT — look up the cursor, re-run the pinned read at the
    /// current bookmark, advance the bookmark, reply with the page.
    ///
    /// CR-B3 (#769): `page_size` is `Some(n)` for an explicit per-call
    /// override (unchanged, client-controlled backpressure) or `None` to
    /// fall back to the cursor's stored `CreateCursor`-time default
    /// (`Cursor::default_page_size`). The `Some(n)` case is still validated
    /// BEFORE the registry lookup (CR-A3's existing property — avoids a
    /// wasted registry hit and avoids ever reaching the `has_more`
    /// infinite-loop computation for a malformed request). The `None` case
    /// has nothing to validate yet at this point — the stored default is
    /// only known AFTER the registry lookup succeeds — so validation for
    /// that path is deferred to just after the lookup, below.
    ///
    /// CR-C1 (P-6): this re-resolves `db -> repo -> table`
    /// (`resolve_repo`/`repo.get_table`) and rebuilds the `FilterContext`/
    /// interner handle on EVERY call, rather than caching a `TableManager`
    /// handle on `Cursor`/`CursorState` across the cursor's lifetime. This
    /// is a deliberate, re-verified tradeoff, not an oversight:
    /// `RepoInstance::get_table` performs its "does this table still exist"
    /// check freshly on each call (see its doc comment: the existence check
    /// happens INSIDE the per-table `OnceCell` init closure, serialized
    /// against a concurrent `remove_table`) — so a `DropTable` that commits
    /// WHILE a cursor is open mid-scroll is observed on the very next
    /// `FetchNext` as a clean typed `unknown_table`
    /// (`wrap_table_lookup_err` — the SAME code an absent table answers, so
    /// the refusal is not an existence oracle), instead
    /// of silently continuing to serve reads against a conceptually-dropped
    /// table. A CACHED `TableManager` handle would paper over exactly that:
    /// the handle itself has no lifecycle hook that revalidates the table
    /// still exists in the current catalog, so a cursor holding one would
    /// keep succeeding indefinitely after the table was dropped, until
    /// something else (e.g. the underlying storage being torn down) finally
    /// broke it in a much less legible way. The per-page cost of
    /// re-resolution (a `DashMap`/`OnceCell` hit plus one interner fetch) is
    /// accepted in exchange for this correctness property; see
    /// `mid_scroll_drop_table_produces_clean_error_not_stale_reads` in
    /// `tests/cursor_handler_tests.rs` for the behavior this decision
    /// preserves.
    pub(in crate::engine::db_handler) async fn fetch_next(
        &self,
        session: &Session,
        cursor_id: CursorId,
        page_size: Option<u32>,
    ) -> DbResponse {
        let max_page_size = self.cursor_limits.max_cursor_page_size;
        if let Some(requested) = page_size {
            // CR-A3: validate an explicit page_size BEFORE the registry
            // lookup — it doesn't need the cursor, and this avoids a wasted
            // registry hit (and, critically, avoids ever running the
            // has_more == 0 >= 0 → true infinite-loop computation below) for
            // a malformed request. A bad page_size on one FetchNext call
            // must not corrupt or close the cursor — it isn't looked up at
            // all here, so it stays untouched.
            if requested == 0 || requested > max_page_size {
                return error_response(&BatchError::InvalidPageSize {
                    page_size: requested,
                    max: max_page_size,
                });
            }
        }

        // F-20 (#813): `get_owned_for_fetch`, not `get_owned` — marks the
        // cursor "in flight" for the reaper's `sweep_and_reap` for as long as
        // `_fetch_lease` stays in scope (the rest of this function's body,
        // across every early `return` below), closing the residual gap
        // documented on `CursorRegistry::sweep_and_reap` between "cursor
        // looked up" and "state() lock actually acquired".
        let (cursor, _fetch_lease) = match self
            .cursor_registry
            .get_owned_for_fetch(cursor_id.0, &session.session_id)
        {
            Ok(pair) => pair,
            Err(e) => return cursor_registry_error_response(cursor_id, e),
        };

        // CR-B3: resolve the effective page size now that the cursor is
        // known — `None` falls back to the stored `CreateCursor`-time
        // default. Validate the resolved value again (defense-in-depth):
        // the default was already validated once at `CreateCursor` time via
        // CR-A3, so this should always pass in practice, but a stored value
        // is never silently trusted. A failure here is a normal
        // `InvalidPageSize` rejection — same shape as the `Some(n)` case
        // above — and must NOT mutate/close the cursor (the "a bad
        // page_size on one call must not corrupt the cursor" invariant
        // CR-A3 already established): the lookup above only cloned an
        // `Arc<Cursor>`, it did not touch registry/cursor state, so simply
        // returning here leaves everything untouched.
        let effective_page_size = page_size.unwrap_or_else(|| cursor.default_page_size());
        if effective_page_size == 0 || effective_page_size > max_page_size {
            return error_response(&BatchError::InvalidPageSize {
                page_size: effective_page_size,
                max: max_page_size,
            });
        }

        let mut state = cursor.state().lock().await;
        if state.exhausted {
            drop(state);
            return DbResponse::Error {
                code: "cursor_not_found".into(),
                message: format!(
                    "cursor {} is already exhausted (no more pages) and has been closed",
                    cursor_id
                ),
            };
        }

        let table_name = state.query.from.table.clone();

        // N-9 (CR-D5, #786): authorize BEFORE resolving the repo, matching
        // `create_cursor`'s order (`authorize_cursor_read` then
        // `resolve_repo`) — this used to be the other way around here, a
        // gratuitous asymmetry between the two handlers with no behavior
        // difference (the session already owns the cursor by the time
        // `fetch_next` runs, so `resolve_repo`'s error path leaked no
        // additional information either order), but nothing stops a future
        // change from making the ORDER matter, so keeping the two handlers
        // consistent is worth the pure reshuffle.
        //
        // W-6 (#790, Wave D review) + read-refusal parity: a db dropped
        // between `CreateCursor` and this `FetchNext` still surfaces through
        // the TABLE check (not `unknown_db`) for a non-admin actor, but that
        // table-level refusal is now the SAME typed `unknown_table` every
        // read path answers for an absent/forbidden table — NOT
        // `access_denied`, which would tell the caller the table exists.
        // Verified mechanism (NOT resource_meta's fail-closed path —
        // `ShamirDb::remove_db` only deletes the db's own catalogue row,
        // which `resource_meta` treats as a plain not-found default-open,
        // not a storage error): the TARGET TABLE's own real, still-
        // persisted ACL (untouched by `remove_db`) denies a non-owner
        // directly, under the NEW order, before `resolve_repo` ever gets a
        // chance to run and return `unknown_db` for the now-missing db.
        // Either way this is a deliberate, accepted consequence of
        // authorizing before resolving, documented here since it was not
        // explicitly called out when the reorder landed (CR-D5/N-9) — see
        // `fetch_next_after_db_dropped_mid_scroll_surfaces_unknown_table_not_unknown_db`.
        //
        // Re-authorize on every FetchNext, not just at CreateCursor time: a
        // pinned snapshot only bounds WHAT DATA a cursor can see, not
        // whether the actor SHOULD still be allowed to see it — a
        // permission revoked mid-scroll (between CreateCursor and a later
        // FetchNext) must close the read here, same class of gap as the
        // CreateCursor check above. Cheap: no I/O beyond the existing
        // `resource_meta` catalog reads every other authorize call already
        // does.
        let actor = session_actor(session);
        if let Err(e) =
            authorize_cursor_read(&self.db, &actor, cursor.db(), cursor.repo(), &table_name).await
        {
            self.cursor_registry.remove_identity(cursor_id.0, &cursor);
            drop(state);
            if error_code(&e) == "unknown_table" {
                return table_unavailable_response();
            }
            return error_response(&e);
        }

        // Availability BEFORE any gate that can differ: resolve the parents
        // (typed `unknown_db`/`unknown_repo`) and require the table to still
        // be configured. An ABSENT table therefore answers the SAME fixed
        // refusal a CLOSED table answered at the ACL above, in the same
        // availability phase and before the quarantine probe can distinguish
        // them.
        let repo = match resolve_repo(&self.db, cursor.db(), cursor.repo()) {
            Ok(repo) => repo,
            Err(parent) => return error_response(&parent),
        };
        let Some(authorized_config) = repo.table_config(&table_name) else {
            self.cursor_registry.remove_identity(cursor_id.0, &cursor);
            drop(state);
            return table_unavailable_response();
        };
        let Some(authorized_registration) = repo.table_registration(&table_name) else {
            self.cursor_registry.remove_identity(cursor_id.0, &cursor);
            drop(state);
            return table_unavailable_response();
        };
        if !repo.has_table(&table_name) {
            self.cursor_registry.remove_identity(cursor_id.0, &cursor);
            drop(state);
            return table_unavailable_response();
        }

        // S5c-X PR8b: a cursor created BEFORE its repo was quarantined must
        // fail closed on the next fetch — the quarantine is re-probed on
        // every page. This is a read-refusal, so the closest existing
        // convention is the access-denied path above: the cursor is removed
        // (closed) before returning the error.
        if let Some(e) = quarantine_cursor_error(&self.db, cursor.db(), cursor.repo()) {
            self.cursor_registry.remove_identity(cursor_id.0, &cursor);
            drop(state);
            return error_response(&e);
        }

        // F-1 (#792): `ShamirDb::get_table`, not `RepoInstance::get_table` —
        // see `create_cursor`'s matching call site for why (registry
        // injection). `state.mode` is pinned once at `create_cursor` and
        // never re-derived here, so this fix isn't exercised by the schema
        // gate itself on THIS path today, but keeping both handlers
        // consistent avoids reintroducing the same silent-`None`-registry
        // gap for any future `fetch_next` logic that needs it.
        let table = match self
            .db
            .get_table(cursor.db(), cursor.repo(), &table_name)
            .await
        {
            Ok(t) => t,
            // A table that vanished between the configured-table check and
            // this lookup is a terminal table miss: answer the SAME fixed
            // refusal and retire the cursor (like the revoked-Read path
            // above). A vanished PARENT keeps its typed `unknown_db`/
            // `unknown_repo` and is not a table miss.
            Err(e) => {
                let wrapped = wrap_table_lookup_err(&self.db, cursor.db(), cursor.repo(), e);
                if error_code(&wrapped) == "unknown_table" {
                    self.cursor_registry.remove_identity(cursor_id.0, &cursor);
                    drop(state);
                    return table_unavailable_response();
                }
                return error_response(&wrapped);
            }
        };

        // Test-only pause after acquisition, before the identity re-check.
        #[cfg(test)]
        self.fetch_identity_pause().await;
        let repo = match resolve_repo(&self.db, cursor.db(), cursor.repo()) {
            Ok(repo) => repo,
            Err(_) => {
                self.cursor_registry.remove_identity(cursor_id.0, &cursor);
                drop(state);
                return table_unavailable_response();
            }
        };
        let Some(current_config) = repo.table_config(&table_name) else {
            self.cursor_registry.remove_identity(cursor_id.0, &cursor);
            drop(state);
            return table_unavailable_response();
        };
        let Some(current_registration) = repo.table_registration(&table_name) else {
            self.cursor_registry.remove_identity(cursor_id.0, &cursor);
            drop(state);
            return table_unavailable_response();
        };
        if current_config.store_id != authorized_config.store_id
            || !authorized_registration.same_registration(&current_registration)
        {
            self.cursor_registry.remove_identity(cursor_id.0, &cursor);
            drop(state);
            return error_response(&BatchError::query_coded(
                "",
                "access_denied",
                "the authorized table registration changed during the request",
            ));
        }

        let empty_refs: TMap<String, shamir_db::query::read::QueryResult> = new_map();
        let ctx = match build_filter_context(&table, &empty_refs).await {
            Ok(c) => c,
            Err(e) => return error_response(&e),
        };

        let base_query = state.query.clone();

        // CR-A4 (#764): `state.mode` was pinned once at `create_cursor`
        // time and never re-derived here — see `PaginationMode`'s doc
        // comment for why flip-flopping bookmark kinds mid-scroll is
        // unsafe. `Keyset` additionally requires a `seek_key` to seek
        // from; a `Keyset`-mode cursor with `seek_key == None` (e.g. the
        // ORDER BY field was absent from a page's projection, OR — W-2/W-3
        // (#789) — the last row's ORDER BY value was a `Dec`/`Big` value
        // with no `FilterValue` equivalent, or a `Bin`/`List` value that
        // converts fine but has no `compare_values` arm to ever match
        // against, see `safe_seek_key`) falls back to the row-count
        // bookmark for THIS call only, matching the pre-CR-A4 "can't build
        // a seek from this page" safety net.
        //
        // CR-D1 (#782, release blocker): there is now a SECOND, narrower
        // reason a Keyset-mode cursor ends up on the offset bookmark for a
        // call: `fetch_keyset_page` returning
        // `KeysetOutcome::StuckAtCeiling` (a tie run at the ORDER BY
        // boundary genuinely exceeds `max_cursor_page_size` — see that
        // type's doc comment for the exact livelock this avoids). Unlike
        // CR-A4's per-page "no seek_key, fall back for THIS call only"
        // case above, this fallback is PERMANENT for the cursor's REST OF
        // LIFETIME: `state.mode` itself flips to `PaginationMode::Offset`
        // below, so every FUTURE `FetchNext` goes straight through the
        // offset branch without re-detecting the stuck condition. This is
        // safe specifically because `state.offset` has been maintained in
        // parallel on the Keyset branch all along (see `new_offset`
        // below, both here and in the ordinary Keyset arm) — it already
        // holds the exact count of rows returned to the client so far
        // regardless of which mode produced them, so resuming a plain
        // `LimitOffset { offset: state.offset, .. }` scan from here
        // continues at exactly the right position: every row at or before
        // `state.offset` in the pinned snapshot's stable full-scan order
        // was already handed out (this call or an earlier one), and
        // nothing after it has been. This does NOT contradict CR-A4's "a
        // cursor's mode never flip-flops mid-scroll" invariant, which is
        // about never flipping OPPORTUNISTICALLY/repeatedly per page — this
        // is a ONE-TIME, detected-failure-condition transition, driven by a
        // bookmark that was correct under either coordinate system the
        // whole time.
        //
        // CR-B2: reserve an upfront, pessimistic slice of the RI-15 budget
        // BEFORE running the pinned-version read below (any arm) — bounds
        // execution-time memory for this page, not just write-path
        // residency. `None` when there's no natural upfront estimate
        // (unbounded budget or inactive per-page cap); `enforce_page_budget`
        // falls back to its post-hoc-only acquire in that case.
        let budget_guard = reserve_page_budget_upfront(self).await;

        let (
            page,
            new_seek_key,
            new_tie_skip,
            has_more,
            new_offset,
            force_offset_mode,
            new_after_id,
        );
        match (state.mode, state.seek_key.clone()) {
            (PaginationMode::IndexSeek, seek_key_opt) => {
                // F-53b Step 4 (#880): reach `TableManager::read_as_of`'s
                // AsOf-aware sorted-index seek arm (`read_as_of_keyset_seek`,
                // F-53b Step 2/#878) directly via `Pagination::after_with_id`
                // — no boundary filter, no `where` at all (the eligibility
                // probe already proved the caller's original query has none;
                // see `probe_index_seek_eligible`'s doc comment).
                //
                // CR-B4-style peek: request ONE more row than
                // `effective_page_size` so the true end-of-data can be told
                // apart from an exact multiple of `effective_page_size` —
                // `read_as_of_keyset_seek`/`read_keyset_seek` both fetch
                // EXACTLY `limit` rows (bounded only by data availability),
                // they do not bake in a peek row themselves, so this call
                // site must apply the same +1 convention every other page
                // helper in this file (`fetch_keyset_page`,
                // `fetch_offset_page`) already uses.
                let internal_limit = (effective_page_size as u64).saturating_add(1);
                let mut q = base_query.clone();
                q.pagination = Pagination::after_with_id(
                    seek_key_opt.map(|v| vec![v]).unwrap_or_default(),
                    Some(internal_limit),
                    state.after_id,
                );
                q.temporal = Temporal::AsOf {
                    at: At::Version(cursor.pinned_version()),
                };
                let result = match table.read_with_encoding(&q, &ctx, Default::default()).await {
                    Ok(r) => r,
                    Err(e) => {
                        drop(state);
                        return error_response(&wrap_engine_err(e));
                    }
                };

                let took_seek_arm = result
                    .stats
                    .as_ref()
                    .and_then(|s| s.index_used.as_deref())
                    .is_some_and(|l| l.ends_with("_asof_keyset"));

                if took_seek_arm {
                    // Normal path: peek-row trim, advance the bookmark from
                    // the (now client-visible-sized) tail, and keep
                    // `state.offset` moving in parallel — the CR-D1
                    // precondition the fallback arm below depends on (every
                    // `FetchNext`, on ANY mode, keeps `offset` reflecting the
                    // true cumulative row count so far).
                    let mut result = result;
                    let returned_more = result.records.len() as u64 > effective_page_size as u64;
                    if returned_more {
                        result.records.truncate(effective_page_size as usize);
                        if let Some(stats) = result.stats.as_mut() {
                            stats.records_returned = result.records.len() as u64;
                        }
                    }
                    new_offset = state.offset + result.records.len() as u64;
                    new_after_id = if returned_more {
                        result.records.last().and_then(row_record_id)
                    } else {
                        None
                    };
                    new_seek_key = if returned_more {
                        result
                            .records
                            .last()
                            .and_then(|r| order_by_field_value(&base_query, r))
                    } else {
                        None
                    };
                    page = result;
                    has_more = returned_more;
                    new_tie_skip = 0;
                    force_offset_mode = false;
                } else {
                    // MANDATORY one-time fallback (design memo §1.4 / §2.3):
                    // mirrors CR-D1's `StuckAtCeiling -> Offset` handling
                    // exactly — re-run THIS SAME call via
                    // `fetch_offset_page(state.offset, ..)` so the caller
                    // still gets a CORRECT page back (not a silent repeat of
                    // an earlier page — see the design memo §1.4 for the
                    // concretely-proven hazard a naive always-`After` scheme
                    // would hit here), and defer the `state.mode` commit to
                    // `Offset` until AFTER this page clears the budget gate
                    // below (same ordering CR-D1 already documents).
                    let offset_outcome = match fetch_offset_page(
                        &table,
                        &ctx,
                        &base_query,
                        state.offset,
                        effective_page_size,
                        cursor.pinned_version(),
                    )
                    .await
                    {
                        Ok(o) => o,
                        Err(e) => {
                            drop(state);
                            return error_response(&e);
                        }
                    };
                    new_offset = offset_outcome.new_offset;
                    page = offset_outcome.result;
                    has_more = offset_outcome.has_more;
                    new_seek_key = None;
                    new_tie_skip = 0;
                    new_after_id = None;
                    force_offset_mode = true;
                }
            }
            (PaginationMode::Keyset, Some(seek_key)) => {
                let outcome = match fetch_keyset_page(
                    &table,
                    &ctx,
                    &base_query,
                    &seek_key,
                    state.tie_skip,
                    effective_page_size,
                    cursor.pinned_version(),
                    max_page_size,
                )
                .await
                {
                    Ok(o) => o,
                    Err(e) => {
                        drop(state);
                        return error_response(&e);
                    }
                };
                match outcome {
                    KeysetOutcome::Page(outcome) => {
                        new_offset = state.offset + outcome.result.records.len() as u64;
                        page = outcome.result;
                        new_seek_key = outcome.next_seek_key;
                        new_tie_skip = outcome.next_tie_skip;
                        has_more = outcome.has_more;
                        force_offset_mode = false;
                        new_after_id = None;
                    }
                    KeysetOutcome::StuckAtCeiling => {
                        // CR-D1: permanently switch coordinate systems and
                        // re-run THIS SAME call via the offset bookmark —
                        // the caller must still get a real page back, not
                        // an error or an empty response, for the call that
                        // TRIGGERED the fallback.
                        let offset_outcome = match fetch_offset_page(
                            &table,
                            &ctx,
                            &base_query,
                            state.offset,
                            effective_page_size,
                            cursor.pinned_version(),
                        )
                        .await
                        {
                            Ok(o) => o,
                            Err(e) => {
                                drop(state);
                                return error_response(&e);
                            }
                        };
                        new_offset = offset_outcome.new_offset;
                        page = offset_outcome.result;
                        has_more = offset_outcome.has_more;
                        new_seek_key = None;
                        new_tie_skip = 0;
                        force_offset_mode = true;
                        new_after_id = None;
                    }
                }
            }
            _ => {
                let outcome = match fetch_offset_page(
                    &table,
                    &ctx,
                    &base_query,
                    state.offset,
                    effective_page_size,
                    cursor.pinned_version(),
                )
                .await
                {
                    Ok(o) => o,
                    Err(e) => {
                        drop(state);
                        return error_response(&e);
                    }
                };
                new_offset = outcome.new_offset;
                page = outcome.result;
                has_more = outcome.has_more;
                new_seek_key = None;
                new_tie_skip = 0;
                force_offset_mode = false;
                new_after_id = None;
            }
        }

        // CR-D4 (#785, N-4): build the FULL `DbResponse` now — `cursor_id`
        // (function parameter) and `has_more` are both already known at this
        // point. The only later branch that replaces it is the lost
        // terminal-removal race below (it answers `cursor_not_found` and
        // discards any stashed page), so this value is the final response
        // once it clears the budget gate below and wins that removal.
        let response = DbResponse::CursorPage {
            cursor_id,
            page,
            has_more,
        };

        // CR-A5 (+ CR-B2, CR-D4): gate the FULL response BEFORE mutating the
        // cursor's bookmark state (seek_key/offset/exhausted/mode) or
        // removing it from the registry on exhaustion — a rejected page must
        // leave the cursor exactly as it was before this call, so the client
        // can retry `FetchNext` (e.g. with a smaller `page_size`) against an
        // untouched bookmark instead of one that silently advanced past
        // records it never received. This also protects CR-D1's mode flip
        // below: a page that gets rejected here must NOT have already
        // committed the cursor to `Offset` mode — the client retrying with a
        // smaller `page_size` should still see the same Keyset-mode stuck
        // condition (and get another chance at a real fallback page), not
        // an already-flipped cursor whose triggering page was thrown away.
        // `budget_guard` (reserved upfront, before the read above) is
        // shrunk to the actual size here rather than acquired fresh.
        let bytes = match enforce_page_budget(self, &response, budget_guard).await {
            Ok(bytes) => bytes,
            Err(e) => {
                drop(state);
                return error_response(&e);
            }
        };
        // The ONLY early return after the stash below is the lost
        // terminal-removal race, which discards the stash; this budget
        // rejection runs BEFORE the stash. Safe to stash now: that lost
        // race is the only way `response` is replaced from here on.
        if let Some(bytes) = bytes {
            stash_serialized_response(bytes);
        }

        state.seek_key = new_seek_key;
        state.tie_skip = new_tie_skip;
        state.offset = new_offset;
        state.exhausted = !has_more;
        state.after_id = new_after_id;
        // CR-D1 (#782): commit the permanent keyset->offset mode switch
        // only now that the page has cleared the budget gate — see the
        // dispatch match above for the exactly-once correctness argument.
        // F-53b Step 4 (#880): the SAME flag/ordering also carries the
        // IndexSeek -> Offset fallback (see the new dispatch arm above) —
        // no second flag, no different commit-ordering rule.
        if force_offset_mode {
            state.mode = PaginationMode::Offset;
        }
        drop(state);
        cursor.bump_activity();

        if !has_more {
            #[cfg(test)]
            self.cursor_registry.pause_before_remove(cursor_id.0).await;
            // The terminal response is safe to expose only if this fetch
            // wins removal of the exact cursor it just exhausted. A concurrent
            // close/session cleanup may remove it after `exhausted` is
            // published; in that case do not return the already-built page
            // after the cursor has been closed.
            if self
                .cursor_registry
                .remove_identity(cursor_id.0, &cursor)
                .is_none()
            {
                // The page bytes were stashed for the wire path above; a lost removal
                // race must not send them — `RequestHandler::handle` prefers stashed
                // bytes over the returned error response.
                drop(crate::byte_budget::take_stashed_serialized_response());
                return error_response(&BatchError::CursorNotFound { cursor_id });
            }
        }

        // The SAME value already serialized above — not rebuilt, so it can
        // never accidentally diverge from what was stashed.
        // NON-final pages have no removal gate by design (accepted linearization):
        // a fetch that already holds its registry lease linearizes BEFORE a
        // concurrent close/cleanup, and its page is data consistent with the
        // pinned pre-close snapshot; the identity predicates protect id REUSE,
        // not in-flight reads, and CancelCursor is idempotent `CursorClosed`.
        response
    }

    /// FG-5b CANCEL — idempotent close. Canceling an unknown/already-closed
    /// cursor is NOT an error (CURSORS.md) — reply `CursorClosed` either way.
    pub(in crate::engine::db_handler) async fn cancel_cursor(
        &self,
        session: &Session,
        cursor_id: CursorId,
    ) -> DbResponse {
        // Only remove when it's actually ours; a cross-session cancel
        // attempt is silently treated the same as "already closed" (no
        // information leak about another session's cursor existing).
        if let Ok(cursor) = self
            .cursor_registry
            .get_owned(cursor_id.0, &session.session_id)
        {
            self.remove_cursor_identity(cursor_id.0, &cursor).await;
        }
        DbResponse::CursorClosed { cursor_id }
    }
}

pub(super) fn cursor_registry_error_response(
    cursor_id: CursorId,
    e: CursorRegistryError,
) -> DbResponse {
    match e {
        CursorRegistryError::CursorExpired => {
            error_response(&BatchError::CursorExpired { cursor_id })
        }
        CursorRegistryError::CursorNotFound | CursorRegistryError::CursorOwnershipMismatch => {
            error_response(&BatchError::CursorNotFound { cursor_id })
        }
        CursorRegistryError::CursorLimitExceeded { limit } => {
            error_response(&BatchError::CursorLimitExceeded { limit })
        }
        // R4b-1: the id is already held by a live cursor. Stable,
        // machine-readable code; the message carries no private data
        // (no filter values, credentials or token material).
        CursorRegistryError::CursorIdAlreadyOpen => DbResponse::Error {
            code: "cursor_id_conflict".into(),
            message: "cursor id already open".into(),
        },
    }
}

//! `CreateCursor` wire handler.

use shamir_connect::server::session::Session;
use shamir_db::query::batch::BatchError;
use shamir_db::query::read::{At, Pagination, ReadQuery, Temporal};
use shamir_query_types::wire::CursorId;
use shamir_types::types::common::{new_map, TMap};

use crate::byte_budget::stash_serialized_response;
use crate::cursor_registry::{Cursor, CursorActivity, CursorRegistryError, PaginationMode};

use super::budget::{
    enforce_page_budget, error_response, reserve_page_budget_upfront, wrap_engine_err,
    wrap_table_lookup_err,
};
use super::keyset::{
    build_filter_context, order_by_column_contains_null, order_by_column_is_schema_typed_scalar,
    order_by_field_value, pagination_mode_for_query, probe_index_seek_eligible, row_record_id,
    safe_seek_key, same_boundary_value,
};
use crate::db_handler::cursor_support::{authorize_cursor_read, resolve_repo};
use crate::db_handler::handler::{session_actor, DbResponse, ShamirDbHandler};
use crate::db_handler::visibility_handlers::quarantine_cursor_error;
use crate::engine::db_handler::repo_audit::enqueue_refused_read_audit;

impl ShamirDbHandler {
    /// FG-5b CREATE — resolve the query's table, open an MVCC snapshot
    /// pinned for the cursor's whole lifetime, run the first page, and
    /// park the cursor in the registry bound to this session.
    pub(in crate::engine::db_handler) async fn create_cursor(
        &self,
        session: &Session,
        query_version: u32,
        db_name: &str,
        query: ReadQuery,
        page_size: u32,
    ) -> DbResponse {
        let repo = query.from.repo.clone();
        let table = query.from.table.clone();
        let response = self
            .create_cursor_inner(session, query_version, db_name, query, page_size)
            .await;
        if let (Some(audit), DbResponse::Error { code, .. }) = (&self.audit, &response) {
            drop(enqueue_refused_read_audit(
                &self.cancel_refusal_audit,
                audit,
                session,
                "create_cursor",
                (db_name, &repo, &table),
                code,
            ));
        }
        response
    }

    async fn create_cursor_inner(
        &self,
        session: &Session,
        query_version: u32,
        db_name: &str,
        query: ReadQuery,
        page_size: u32,
    ) -> DbResponse {
        if let Err(e) = crate::version::check_query_lang(query_version) {
            return DbResponse::Error {
                code: "unsupported_query_version".into(),
                message: e.to_string(),
            };
        }

        let repo_name = query.from.repo.clone();
        let table_name = query.from.table.clone();

        let actor = session_actor(session);
        if let Err(e) = self
            .db
            .authorize_access(
                &actor,
                &shamir_db::access::ResourcePath::Database {
                    db: db_name.to_string(),
                },
                shamir_db::access::Action::Read,
            )
            .await
        {
            return error_response(&BatchError::query_coded("", "access_denied", e.to_string()));
        }

        let repo = match resolve_repo(&self.db, db_name, &repo_name) {
            Ok(r) => r,
            Err(e) => return error_response(&e),
        };
        // Capture the table incarnation before name-based authorization.
        // Missing/unpinnable registrations share the fixed unavailable refusal.
        let Some(authorized_config) = repo.table_config(&table_name) else {
            return error_response(&crate::db_handler::operator_state::table_unavailable_error());
        };
        let Some(authorized_registration) = repo.table_registration(&table_name) else {
            return error_response(&crate::db_handler::operator_state::table_unavailable_error());
        };
        if let Err(e) =
            authorize_cursor_read(&self.db, &actor, db_name, &repo_name, &table_name).await
        {
            return error_response(&e);
        }

        // CR-A3: reject page_size == 0 and over-cap requests only after the
        // target table has passed the availability and Read-ACL gates.
        let max_page_size = self.cursor_limits.max_cursor_page_size;
        if page_size == 0 || page_size > max_page_size {
            return error_response(&BatchError::InvalidPageSize {
                page_size,
                max: max_page_size,
            });
        }
        if !matches!(query.temporal, Temporal::Latest) {
            return error_response(&BatchError::CursorTemporalNotSupported);
        }
        if query.with_version {
            return error_response(&BatchError::CursorWithVersionNotSupported);
        }

        // S5c-X PR8b: quarantine is checked after target-dependent request
        // validation, but before opening a snapshot or resolving a manager.
        if let Some(e) = quarantine_cursor_error(&self.db, db_name, &repo_name) {
            return error_response(&e);
        }

        // F-1 (#792): `ShamirDb::get_table` (NOT `RepoInstance::get_table`)
        // is used here specifically because it injects the global
        // `ValidatorRegistry` (`table.set_validator_registry(...)`) into the
        // returned `TableManager` clone — `RepoInstance::get_table` alone
        // does not (`validator_registry` is a plain `Option<Arc<..>>` field,
        // not shared via `ArcSwap`/a cell, so it only affects whichever
        // clone it's set on). Without this, `order_by_column_is_schema_typed_
        // scalar`'s `table.validator_registry_read_view()` would always see `None`
        // regardless of what's bound via `bind_validator`, silently
        // defeating the new schema-typed-scalar gate for every cursor. Reads
        // don't otherwise need the registry (validators are a write-path
        // concept), so this is a registry-injection fix, not a behavior
        // change to what a cursor's read can see.
        let table = match self.db.get_table(db_name, &repo_name, &table_name).await {
            Ok(t) => t,
            // An absent table answers `unknown_table`, the SAME code the
            // table-Read denial above answers for an existing closed table
            // (`authorize_cursor_read`); parents keep `unknown_db`/`unknown_repo`.
            Err(e) => {
                return error_response(&wrap_table_lookup_err(&self.db, db_name, &repo_name, e));
            }
        };
        // Test-only pause after acquisition, before the identity re-check.
        #[cfg(test)]
        self.create_cursor_identity_pause().await;
        let repo = match resolve_repo(&self.db, db_name, &repo_name) {
            Ok(repo) => repo,
            Err(_) => {
                return error_response(&crate::db_handler::operator_state::table_unavailable_error())
            }
        };
        let Some(current_config) = repo.table_config(&table_name) else {
            return error_response(&crate::db_handler::operator_state::table_unavailable_error());
        };
        let Some(current_registration) = repo.table_registration(&table_name) else {
            return error_response(&crate::db_handler::operator_state::table_unavailable_error());
        };
        if current_config.store_id != authorized_config.store_id
            || !authorized_registration.same_registration(&current_registration)
        {
            return error_response(&BatchError::query_coded(
                "",
                "access_denied",
                "the authorized table registration changed during the request",
            ));
        }
        let gate = match repo.tx_gate().await {
            Ok(g) => g,
            Err(e) => return error_response(&wrap_engine_err(e)),
        };
        let guard = gate.open_snapshot().await;
        let pinned_version = guard.version();

        // N-7 (CR-D5, #786): best-effort PERFORMANCE optimization, not a
        // correctness requirement — confirmed by reading `read_temporal.rs`'s
        // `read_as_of` (this cursor's whole read path, since every
        // `create_cursor`/`fetch_next` read is `Temporal::AsOf`) and the
        // `MvccStore` overlay-merge machinery it calls into:
        //
        // - `read_as_of`'s enumeration source
        //   (`list_stream_with_tombstones` -> `MvccStore::
        //   current_stream_with_tombstones` -> `current_stream_impl`)
        //   explicitly merges the in-memory overlay
        //   (`self.overlay.snapshot_le(floor)`) with durable `history` at
        //   stream-open time — a key that exists ONLY in the overlay (never
        //   drained) is enumerated exactly as if it were already durable.
        // - Its per-id point read (`MvccStore::get_at_many`/`get_at`) probes
        //   the overlay FIRST, per key, before ever touching `history` — an
        //   overlay-only `(key, version)` pair resolves correctly with no
        //   drain required.
        //
        // So an `AsOf` read at the pinned version is ALREADY coherent
        // whether or not this drain has run — this call flushes the repo's
        // in-memory overlay into durable history + reclaims WAL segments
        // once, up front, purely so every `FetchNext` for this cursor's
        // lifetime reads through the (cheaper) durable-history path instead
        // of paying the overlay-merge cost on every call. If it fails, reads
        // remain correct (via the overlay-aware path above) but keep paying
        // that cost for the cursor's lifetime instead — logged, not fatal.
        if let Err(e) = repo.drainer().drain_all(repo.core()).await {
            tracing::warn!(?e, db = db_name, repo = %repo_name, "create_cursor: drain_all failed");
        }

        let empty_refs: TMap<String, shamir_db::query::read::QueryResult> = new_map();
        let ctx = match build_filter_context(&table, &empty_refs).await {
            Ok(c) => c,
            Err(e) => return error_response(&e),
        };

        // W-4 (#790, Wave D review): reserve the RI-15 upfront budget slice
        // BEFORE the CR-D2 null probe below, not just before the first-page
        // read — the probe's own `read_with_encoding` call is the SAME cost
        // class as the first-page read (full pinned-snapshot scan; see
        // `order_by_column_contains_null`'s doc comment), so it was
        // previously running with zero admission-gate protection: a burst of
        // concurrent `CreateCursor` calls against a keyset-eligible,
        // null-containing query could all run the probe simultaneously,
        // unthrottled by the very budget mechanism the first-page read right
        // after it is subject to. Widening the single upfront reservation to
        // cover BOTH reads (rather than reserving twice) is safe:
        // `enforce_page_budget`'s later `shrink_to`/`grow_unchecked` step
        // only cares about the FINAL measured size of `response`, never
        // about when the reservation started, so moving the acquire earlier
        // changes nothing about the eventual shrink/grow math.
        let budget_guard = reserve_page_budget_upfront(self).await;

        // CR-D2 (#783, release blocker): a query's ORDER BY SHAPE alone
        // (`pagination_mode_for_query`) cannot tell whether the column's
        // DATA is actually safe for the keyset boundary-filter scheme — a
        // `Null`/missing value in that column makes the `field >= seek_key`
        // boundary permanently (and silently) exclude that row past page 1
        // (see `order_by_column_contains_null`'s doc comment for the full
        // mechanism). When the query is otherwise keyset-eligible, probe
        // the SAME pinned snapshot ONCE for a null/missing value in that
        // column before running the first page at all — if found, the
        // WHOLE cursor falls back to `PaginationMode::Offset` from
        // creation, closing the null/missing case unconditionally.
        //
        // F-53b Step 4 (#880): try `IndexSeek` eligibility FIRST — when
        // eligible, the `order_by_column_is_schema_typed_scalar` +
        // `order_by_column_contains_null` probes are skipped entirely (the
        // seek arm's own per-candidate MVCC classifier already handles the
        // data-shape edge cases they exist for). Only when
        // `probe_index_seek_eligible` returns `None` does the existing
        // chain run, completely unchanged.
        let mode = match probe_index_seek_eligible(&table, &query).await {
            Err(e) => return error_response(&wrap_engine_err(e)),
            Ok(Some(_)) => PaginationMode::IndexSeek,
            Ok(None) => {
                // F-1 (#792, release blocker) + F-17 (#810): gate keyset
                // eligibility on the ORDER BY column's SCHEMA-enforced type.
                // `keyset_safe` is only stamped when the schema was bound while
                // the table was empty, so a schema declared onto an
                // already-populated column falls back to `Offset`. When the gate
                // DOES pass, the null probe below still runs — this gate proves
                // "no second non-null type is possible", not "never null".
                let mut mode = pagination_mode_for_query(&query);
                if mode == PaginationMode::Keyset {
                    // `pagination_mode_for_query` only returns `Keyset` when
                    // `query.order_by` is `Some` with exactly one single-segment
                    // field — both already validated, so this is infallible here.
                    let order_by_field = &query
                        .order_by
                        .as_ref()
                        .expect("pagination_mode_for_query returned Keyset without an order_by")
                        .items[0]
                        .field;
                    if !order_by_column_is_schema_typed_scalar(&table, order_by_field) {
                        mode = PaginationMode::Offset;
                    }
                }
                if mode == PaginationMode::Keyset {
                    match order_by_column_contains_null(&table, &ctx, &query, pinned_version).await
                    {
                        Ok(true) => mode = PaginationMode::Offset,
                        Ok(false) => {}
                        Err(e) => return error_response(&e),
                    }
                }
                mode
            }
        };

        // CR-B4: fetch one extra "peek" row beyond the client-visible
        // `page_size` so the true end-of-data can be told apart from "the
        // result set happens to be an exact multiple of page_size" — without
        // the peek, `fetched == page_size` is ambiguous, and the OLD
        // `fetched >= page_size` heuristic reported `has_more: true` on the
        // genuine last page, causing one spurious empty round-trip (the
        // `DbResponse::CursorPage::has_more` doc comment promises `true`
        // means "a further FetchNext will return AT LEAST ONE more record" —
        // widen to `u64` before the `+1` since `page_size` is validated only
        // against `max_cursor_page_size` as a `u32`, not against the wider
        // internal fetch limit; `Pagination::LimitOffset::limit` is already
        // `Option<u64>`, so this cannot overflow.
        let internal_limit = (page_size as u64).saturating_add(1);

        // F-53b Step 4 (#880): `IndexSeek` mode's first page is ATTEMPTED via
        // `Pagination::after_with_id(vec![], ..)` — an empty seek tuple,
        // since there is no prior bookmark yet at `create_cursor` time.
        // `TableManager::try_plan_keyset_seek` (`read_planner.rs:493-497`)
        // hard-requires `key.len() == 1`, so this key ALWAYS declines the
        // seek arm on page 1 specifically — there is no type-agnostic
        // sentinel value provably "before/after every real value" for an
        // arbitrary sorted-index column in BOTH ASC and DESC direction (see
        // the design memo: `QueryValue::Null` is a universal ASC-minimum
        // sentinel via `sort_codec`'s tag ordering, but no universal
        // DESC-maximum exists — `Bytes` is the highest tag, but no `Bytes`
        // VALUE sorts after every other `Bytes` value). Building one would
        // need a new engine-side primitive, explicitly out of scope here
        // (cursor-wiring only, consuming the existing engine API).
        //
        // This first page therefore ALWAYS falls through to `read_as_of`'s
        // full-scan tail (verified below via `stats.index_used`) and is
        // rebuilt via `PaginationMode::Offset`'s `Pagination::LimitOffset`
        // path instead — the SAME fallback mechanism `fetch_next` uses (step
        // 6), just applied at creation time. Critically, this does NOT
        // downgrade `mode` itself: `IndexSeek` stays pinned for the cursor's
        // ongoing lifetime, because `fetch_next`'s FIRST real call seeds
        // `Pagination::After` from THIS page's LAST ROW — a real,
        // correctly-typed value (not a synthetic sentinel) — which DOES
        // reach the seek arm from page 2 onward. Only a genuine MID-LIFETIME
        // gate decline (a `fetch_next` that previously succeeded, then
        // stops — see that function's `IndexSeek` dispatch arm) commits the
        // permanent `IndexSeek -> Offset` transition; a page-1 decline here
        // is expected/structural, not a failure signal.
        let mut first_query = query.clone();
        first_query.pagination = if mode == PaginationMode::IndexSeek {
            Pagination::after_with_id(vec![], Some(internal_limit), None)
        } else {
            Pagination::LimitOffset {
                limit: Some(internal_limit),
                offset: 0,
            }
        };
        first_query.temporal = Temporal::AsOf {
            at: At::Version(pinned_version),
        };

        let mut page = match table
            .read_with_encoding(&first_query, &ctx, Default::default())
            .await
        {
            Ok(p) => p,
            Err(e) => return error_response(&wrap_engine_err(e)),
        };

        if mode == PaginationMode::IndexSeek {
            let took_seek_arm = page
                .stats
                .as_ref()
                .and_then(|s| s.index_used.as_deref())
                .is_some_and(|l| l.ends_with("_asof_keyset"));
            if !took_seek_arm {
                first_query.pagination = Pagination::LimitOffset {
                    limit: Some(internal_limit),
                    offset: 0,
                };
                page = match table
                    .read_with_encoding(&first_query, &ctx, Default::default())
                    .await
                {
                    Ok(p) => p,
                    Err(e) => return error_response(&wrap_engine_err(e)),
                };
            }
        }

        // CR-B4: the peek row (if present) proves there's at least one more
        // record beyond `page_size` — trim it off BEFORE the page goes out
        // over the wire or is measured for the byte budget, so both the
        // client-visible payload and the budget accounting reflect only the
        // `page_size` rows actually returned.
        let has_more = page.records.len() as u64 > page_size as u64;
        if has_more {
            page.records.truncate(page_size as usize);
            if let Some(stats) = page.stats.as_mut() {
                stats.records_returned = page.records.len() as u64;
            }
        }

        let (seek_key, tie_skip) = if has_more && mode == PaginationMode::Keyset {
            match page
                .records
                .last()
                .and_then(|r| order_by_field_value(&query, r))
            {
                Some(last_value) => {
                    // CR-A4: how many rows in THIS page already share the
                    // last row's ORDER BY value — that many must be
                    // skipped from the front of the next (inclusive)
                    // refetch so they aren't handed to the client twice.
                    // Tie count is against the RAW extracted value (see
                    // `bookmark_from_tail`'s matching comment); W-2/W-3
                    // (#789): the value actually stored as the bookmark
                    // must first clear `safe_seek_key`, else this cursor
                    // falls back to the row-count offset bookmark for this
                    // call, same as the pre-existing "no seek_key" case.
                    let tie_skip = page
                        .records
                        .iter()
                        .filter(|r| {
                            order_by_field_value(&query, r)
                                .is_some_and(|v| same_boundary_value(&v, &last_value))
                        })
                        .count() as u64;
                    (safe_seek_key(last_value), tie_skip)
                }
                None => (None, 0),
            }
        } else {
            (None, 0)
        };

        // F-53b Step 4 (#880): `IndexSeek` mode's bookmark half — the last
        // row's ORDER BY value (reusing `seek_key`, same as `Keyset`) plus
        // its REAL `RecordId` (`after_id`, new). Simpler than `Keyset`'s
        // `tie_skip` counting scheme: no tie-counting needed, since
        // `read_as_of_keyset_seek` always attaches a real id to every row
        // (unlike `Keyset` mode's generic AsOf full-scan projection, which
        // never does — see `tie_skip`'s doc comment).
        let after_id = if has_more && mode == PaginationMode::IndexSeek {
            page.records.last().and_then(row_record_id)
        } else {
            None
        };
        let seek_key = if mode == PaginationMode::IndexSeek {
            if has_more {
                page.records
                    .last()
                    .and_then(|r| order_by_field_value(&query, r))
            } else {
                None
            }
        } else {
            seek_key
        };
        let offset = page.records.len() as u64;

        // CR-D4 (#785, N-4): mint `cursor_id` BEFORE the budget/size check
        // (instead of after, as it was pre-CR-D4) so the FULL `DbResponse`
        // value (below) already exists at the point `enforce_page_budget`
        // needs to measure it — mirroring `execute()`'s real behavior of
        // serializing the whole wire envelope, not just the inner `page`.
        // Minting slightly earlier and never using it (on the too-large or
        // `register()`-failure paths below) simply "burns" an id — harmless,
        // `next_cursor_id()` is a plain counter bump with no other side
        // effect, the same tradeoff RI-15's own upfront reserve already
        // accepts elsewhere (reserved-then-released on a rejected path).
        let cursor_id = self.next_cursor_id();
        let response = DbResponse::CursorPage {
            cursor_id: CursorId(cursor_id),
            page,
            has_more,
        };

        // CR-A5 (+ CR-B2, CR-D4): gate the FULL response against the
        // per-page byte-size cap and the RI-15 global byte budget BEFORE
        // deciding whether to register the cursor — a rejected page must
        // not mint a registered cursor (there is nothing to `FetchNext`
        // against; the client never receives this page's bytes at all).
        // `budget_guard` (reserved upfront, before the read above) is
        // shrunk to the actual size here rather than acquired fresh.
        //
        // The returned `bytes` are deliberately NOT stashed yet (see
        // `enforce_page_budget`'s doc comment) — the `has_more` branch below
        // still has to clear `cursor_registry.register`, which can itself
        // fail and produce a DIFFERENT response; stashing here would leak
        // stale `CursorPage` bytes into that failure's wire response.
        let bytes = match enforce_page_budget(self, &response, budget_guard).await {
            Ok(bytes) => bytes,
            Err(e) => return error_response(&e),
        };

        if !has_more {
            // The entire result fit on the first page — no `FetchNext` will
            // ever be issued (both the Rust and TS SDKs stop iterating as
            // soon as `has_more == false`). Registering it anyway would park
            // a live `SnapshotGuard` MVCC pin and a per-session registry
            // slot for no reason until the idle-timeout reaper eventually
            // reclaims it. Returning here instead lets `response` (built
            // above) go out while `guard` (never wrapped into a `Cursor` for
            // this branch) drops immediately via RAII, and the per-session
            // cursor cap is never touched by an already-exhausted cursor.
            // The minted `cursor_id` is handed to the client unregistered: a
            // later `FetchNext`/`CancelCursor` against it falls through to
            // the existing not-found / idempotent-close paths, which is the
            // accurate answer for an id that never existed in the registry.
            //
            // This `response` IS the final return value — safe to stash now.
            if let Some(bytes) = bytes {
                stash_serialized_response(bytes);
            }
            return response;
        }

        let cursor = Cursor::new(
            query,
            mode,
            guard,
            pinned_version,
            page_size,
            session.session_id,
            db_name.to_string(),
            repo_name,
        )
        // R4b-1: stamp safe activity metadata (minted session handle,
        // username, db, TABLE NAME, pinned version, creation unix-ns) at
        // construction so `CursorRegistry::register` mirrors it inside its
        // existing linearization point. Safe fields only — no bearer token,
        // prefix, query literal or row data ever enters `CursorActivity`.
        .with_activity(CursorActivity {
            session_handle: session.activity_handle(),
            user_id: session.user_id,
            username: session.username.clone(),
            db: db_name.to_string(),
            table: table_name,
            pinned_version,
            created: std::time::Instant::now(),
        });
        {
            let mut state = cursor.state().lock().await;
            state.seek_key = seek_key;
            state.tie_skip = tie_skip;
            state.offset = offset;
            state.exhausted = !has_more;
            state.after_id = after_id;
        }

        match self.cursor_registry.register(
            cursor_id,
            session.session_id,
            cursor,
            self.cursor_limits.max_cursors_per_session as u32,
        ) {
            Ok(_) => {
                // `response` is now confirmed final — safe to stash.
                if let Some(bytes) = bytes {
                    stash_serialized_response(bytes);
                }
                response
            }
            Err(CursorRegistryError::CursorLimitExceeded { limit }) => {
                // `bytes` (the stale `CursorPage` encoding) is discarded here
                // — deliberately NOT stashed. Stashing it would serve the
                // client the abandoned `CursorPage` instead of this actual
                // `CursorLimitExceeded` error (the exact wire-corruption bug
                // this task's "subtlety" section warns about). This rare
                // failure path takes the pre-existing fresh-encode hit in
                // `RequestHandler::handle`, same as any other error response.
                error_response(&BatchError::CursorLimitExceeded { limit })
            }
            Err(_) => DbResponse::Error {
                code: "cursor_error".into(),
                message: "could not register cursor".into(),
            },
        }
    }
}

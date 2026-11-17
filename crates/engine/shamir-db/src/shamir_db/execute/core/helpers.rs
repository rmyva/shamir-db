//! Shared helper functions used across admin executor sub-modules.

use crate::query::batch::BatchError;
use crate::query::read::DdlOpState;
use crate::query::read::{QueryRecord, QueryResult, QueryStats};
use crate::query::FilterValue;
use crate::types::common::TMap;
use crate::types::value::QueryValue;
use shamir_engine::index2::kind::IndexKind;
use shamir_types::types::record_id::RecordId;

/// Kind label and uniqueness of an index2-registry index (DESCRIBE / LIST).
pub(super) fn index2_kind(kind: &IndexKind) -> (QueryValue, bool) {
    let (label, unique) = match kind {
        IndexKind::Btree { unique } => ("btree", *unique),
        IndexKind::Fts { .. } => ("fulltext", false),
        IndexKind::Functional(_) => ("functional", false),
        IndexKind::Vector(_) => ("vector", false),
    };
    (QueryValue::Str(label.to_owned()), unique)
}

/// Construct a `QueryResult` for a successful admin operation.
pub(super) fn admin_result(data: QueryValue) -> QueryResult {
    QueryResult {
        receipt: None,
        records: vec![QueryRecord::Direct(data)],
        stats: Some(QueryStats {
            index_used: None,
            records_scanned: 0,
            records_returned: 1,
            execution_time_us: 0,
        }),
        pagination: None,
        value: None,
        explain: None,
        skipped: false,
        versions: None,
        corrupt_records: Vec::new(),
        op_id: None,
        ddl_status: None,
    }
}

/// Construct a `QueryResult` for a successful DDL operation with an operation ID.
///
/// This is the central place for stamping `op_id` into DDL results (RFC §3.2).
/// The `op_id` is minted at dispatch time for recoverable DDL ops and passed
/// through the DDL handler chain; this variant of `admin_result` attaches it
/// to the `QueryResult` that gets returned to the client.
///
/// The `ddl_status` field is populated ONLY for the inline-succeeded common case
/// (i.e., `Succeeded`). For crash-recovered operations, the client MUST poll
/// via `DbRequest::GetDdlOpStatus` to see `SucceededViaCrashRecovery`.
pub(super) fn admin_result_with_op_id(
    data: QueryValue,
    op_id: RecordId,
) -> Result<QueryResult, BatchError> {
    admin_result_with_op_id_at(data, op_id, std::time::SystemTime::now())
}

pub(super) fn admin_result_with_op_id_at(
    data: QueryValue,
    op_id: RecordId,
    now: std::time::SystemTime,
) -> Result<QueryResult, BatchError> {
    let completed_at = system_time_to_unix_millis(now).ok();
    Ok(QueryResult {
        receipt: None,
        records: vec![QueryRecord::Direct(data)],
        stats: Some(QueryStats {
            index_used: None,
            records_scanned: 0,
            records_returned: 1,
            execution_time_us: 0,
        }),
        pagination: None,
        value: None,
        explain: None,
        skipped: false,
        versions: None,
        corrupt_records: Vec::new(),
        op_id: Some(op_id),
        ddl_status: completed_at.map(|completed_at| DdlOpState::Succeeded { completed_at }),
    })
}

/// Convert a clock reading to the DDL status timestamp, rejecting pre-epoch values.
pub(super) fn system_time_to_unix_millis(time: std::time::SystemTime) -> Result<u64, BatchError> {
    time.duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .map_err(|error| BatchError::QueryError {
            alias: String::new(),
            message: format!("system clock precedes UNIX_EPOCH: {error}"),
            code: Some("internal_error".to_string()),
        })
}

/// Serialize any `serde::Serialize` value to `QueryValue` via msgpack round-trip.
///
/// Used at call sites where a typed struct (e.g. `BufferConfigDto`,
/// `ChangelogEvent`, `ResourceRef`) must be embedded in an admin response
/// `QueryValue` but has no direct `→ QueryValue` conversion. The msgpack
/// codec is byte-identical to the wire, so this is lossless.
pub(super) fn to_qv<T: serde::Serialize>(v: &T) -> Result<QueryValue, BatchError> {
    let bytes = rmp_serde::to_vec_named(v).map_err(|error| BatchError::QueryError {
        alias: String::new(),
        message: format!("failed to serialize admin response value: {error}"),
        code: Some("internal_error".to_string()),
    })?;
    rmp_serde::from_slice::<QueryValue>(&bytes).map_err(|error| BatchError::QueryError {
        alias: String::new(),
        message: format!("failed to decode admin response value: {error}"),
        code: Some("internal_error".to_string()),
    })
}

/// Map a typed [`crate::DbError`] onto a coded [`BatchError`] so the admin
/// dispatch surface keeps its machine-readable `code` (S5c-X PR8b-2: the
/// subscription-binding guards surface typed catalogue/validation errors).
pub(super) fn db_error_to_batch(error: crate::DbError) -> BatchError {
    let code = error.code().map(str::to_string);
    BatchError::QueryError {
        alias: String::new(),
        message: error.to_string(),
        code,
    }
}

/// Round-5 (B4): dispatch conversion for the binding-guard refusals. The
/// two typed guard variants map to their DEDICATED wire codes with the
/// builder's complete detail text; everything else falls back to the
/// generic [`db_error_to_batch`]. Every caller of
/// `ensure_destinations_unbound` / `ensure_drop_allowed` on the
/// admin-dispatch surface routes through this so the codes survive
/// (`replication_destination_bound` / `replication_destination_quarantined`).
pub(super) fn repl_guard_error_to_batch(error: crate::DbError) -> BatchError {
    match error {
        crate::DbError::ReplicationDestinationBound { detail } => {
            BatchError::query_coded("", "replication_destination_bound", detail)
        }
        crate::DbError::ReplicationDestinationQuarantined { detail } => {
            BatchError::query_coded("", "replication_destination_quarantined", detail)
        }
        other => db_error_to_batch(other),
    }
}

/// Rejects path-traversal characters in database and repository names.
///
/// Only `[A-Za-z0-9_-]` is allowed — no `/`, `\`, `:`, `.`, or any
/// non-ASCII byte. Empty strings are also rejected.
pub(super) fn validate_name_component(s: &str, label: &str) -> Result<(), BatchError> {
    if s.is_empty() {
        return Err(BatchError::QueryError {
            alias: String::new(),
            message: format!("{} must not be empty", label),
            code: None,
        });
    }
    if s == "." || s == ".." {
        return Err(BatchError::QueryError {
            alias: String::new(),
            message: format!("{} must not be '.' or '..'", label),
            code: None,
        });
    }
    for ch in s.chars() {
        if !ch.is_ascii_alphanumeric() && ch != '_' && ch != '-' {
            return Err(BatchError::QueryError {
                alias: String::new(),
                message: format!(
                    "{} contains disallowed character '{}': \
                     only [A-Za-z0-9_-] are permitted",
                    label, ch
                ),
                code: None,
            });
        }
    }
    Ok(())
}

/// Map the wire DTO into the storage struct without dragging the
/// storage crate's serde-compatible-by-coincidence layout into
/// the API contract — the two types are intentionally distinct.
pub(super) fn storage_from_dto(
    dto: &crate::query::admin::BufferConfigDto,
) -> crate::storage::storage_membuffer::MemBufferConfig {
    crate::storage::storage_membuffer::MemBufferConfig {
        max_bytes: dto.max_bytes,
        max_entries: dto.max_entries,
        ttl_ms: dto.ttl_ms,
        flush_interval_ms: dto.flush_interval_ms,
        flush_batch_size: dto.flush_batch_size,
    }
}

pub(super) fn dto_from_storage(
    cfg: &crate::storage::storage_membuffer::MemBufferConfig,
) -> crate::query::admin::BufferConfigDto {
    crate::query::admin::BufferConfigDto {
        max_bytes: cfg.max_bytes,
        max_entries: cfg.max_entries,
        ttl_ms: cfg.ttl_ms,
        flush_interval_ms: cfg.flush_interval_ms,
        flush_batch_size: cfg.flush_batch_size,
    }
}

/// Apply only the fields the patch actually set; leave the rest
/// alone. Double-option semantics for `ttl_ms`: `Some(None)` ↔
/// "clear TTL"; `Some(Some(v))` ↔ "set TTL"; `None` ↔ "untouched".
pub(super) fn apply_patch(
    cfg: &mut crate::storage::storage_membuffer::MemBufferConfig,
    patch: &crate::query::admin::BufferConfigPatch,
) {
    if let Some(v) = patch.max_bytes {
        cfg.max_bytes = v;
    }
    if let Some(v) = patch.max_entries {
        cfg.max_entries = v;
    }
    if let Some(v) = patch.ttl_ms {
        cfg.ttl_ms = v;
    }
    if let Some(v) = patch.flush_interval_ms {
        cfg.flush_interval_ms = v;
    }
    if let Some(v) = patch.flush_batch_size {
        cfg.flush_batch_size = v;
    }
}

/// T3: look up a table's `MvccStore` and apply a retention policy.
///
/// `per_table_mvcc` is lazily populated on first `get_table()`, so we
/// force table instantiation first, then resolve the store by token.
/// If the table has no MvccStore entry (shouldn't happen for a real
/// table), the policy is skipped gracefully — the next instantiation
/// will pick up CurrentOnly (the `MvccStore::new` default).
pub(super) async fn apply_table_retention(
    shamir: &super::super::shamir_db::ShamirDb,
    db_name: &str,
    repo: &str,
    table: &str,
    policy: crate::engine::repo::MvccRetention,
) -> Result<(), BatchError> {
    let err = |msg: String| BatchError::QueryError {
        alias: String::new(),
        message: msg,
        code: None,
    };
    let db = shamir
        .get_db(db_name)
        .ok_or_else(|| err(format!("Database '{}' not found", db_name)))?;
    let repo_instance = db
        .get_repo(repo)
        .ok_or_else(|| err(format!("Repository '{}' not found", repo)))?;
    let permit = repo_instance
        .write_domain()
        .admit()
        .await
        .map_err(|e| err(e.to_string()))?;
    let _table = repo_instance
        .get_table_admitted(table, &permit)
        .await
        .map_err(|e| err(e.to_string()))?;
    let token = crate::engine::table::table_token_for(table);
    if let Some(mvcc) = repo_instance.per_table_mvcc().get_sync(&token) {
        mvcc.set_retention(policy).map_err(err)?;
    }
    Ok(())
}

/// T4-purge: resolve the live `MvccStore` for `(db, repo, table)`.
///
/// Mirrors [`apply_table_retention`]'s lookup but RETURNS the store
/// (needed so the caller can read its clock and run the purge). Forces
/// table instantiation first (`per_table_mvcc` is lazily populated on
/// first `get_table`). Errors clearly on unknown db / repo / table or
/// a table with no MvccStore entry.
pub(super) async fn resolve_table_mvcc(
    shamir: &super::super::shamir_db::ShamirDb,
    db_name: &str,
    repo: &str,
    table: &str,
) -> Result<std::sync::Arc<shamir_tx::MvccStore>, BatchError> {
    let err = |msg: String| BatchError::QueryError {
        alias: String::new(),
        message: msg,
        code: None,
    };
    let db = shamir
        .get_db(db_name)
        .ok_or_else(|| err(format!("Database '{}' not found", db_name)))?;
    // Force the lazy MvccStore entry for this table.
    let _ = db
        .get_table(repo, table)
        .await
        .map_err(|e| err(e.to_string()))?;
    let repo_instance = db
        .get_repo(repo)
        .ok_or_else(|| err(format!("Repository '{}' not found", repo)))?;
    let token = crate::engine::table::table_token_for(table);
    repo_instance
        .per_table_mvcc()
        .get_sync(&token)
        .map(|entry| std::sync::Arc::clone(&entry))
        .ok_or_else(|| {
            err(format!(
                "Table '{}.{}' has no MvccStore (History/Purge require an MVCC-backed table)",
                repo, table
            ))
        })
}

/// Convert a `FilterValue` literal to a `QueryValue`.
///
/// Literals (Null / Bool / Int / Float / String / Binary / Array) are mapped
/// directly. `$query` / `QueryRef` variants are resolved against
/// `resolved_refs` — the same value-first / records-second rules as the
/// filter evaluator (Phase 2). Other dynamic variants (`$ref`, `$fn`,
/// `$expr`, `$cond`) collapse to `Null` here — **not** because they're
/// meaningless (they now evaluate correctly in `resolve_filter_query`,
/// #635) but because this is a genuinely separate, record-free resolver
/// for `Call` op positional params: there is no `RecordRef`/`Interner`/
/// `FilterContext` in scope here to compile a `$cond`'s `Filter` condition
/// or resolve a `$ref` field path against. Wiring those variants through
/// would require plumbing a record + interner into every `Call`-param call
/// site — out of scope for #635; tracked separately if Call params ever
/// need `$cond`/`$expr`/`$fn`/`$ref` support.
pub(super) fn filter_value_to_query_value(
    fv: &FilterValue,
    resolved_refs: &TMap<String, QueryResult>,
) -> QueryValue {
    match fv {
        FilterValue::Null => QueryValue::Null,
        FilterValue::Bool(b) => QueryValue::Bool(*b),
        FilterValue::Int(i) => QueryValue::Int(*i),
        FilterValue::Float(f) => QueryValue::F64(*f),
        FilterValue::String(s) => QueryValue::Str(s.clone()),
        FilterValue::Binary(b) => QueryValue::Bin(b.clone()),
        FilterValue::Array(arr) => QueryValue::List(
            arr.iter()
                .map(|v| filter_value_to_query_value(v, resolved_refs))
                .collect(),
        ),
        FilterValue::QueryRef { alias, path } => {
            let key = alias.strip_prefix('@').unwrap_or(alias.as_str());
            let Some(qr) = resolved_refs.get(key) else {
                return QueryValue::Null;
            };
            // Same value-first / records-second rule as the filter evaluator:
            // a Call result lives in `value`; a Read result lives in `records`.
            if let Some(value) = &qr.value {
                navigate_query_value(value, path.as_deref())
                    .cloned()
                    .unwrap_or(QueryValue::Null)
            } else if path.is_none() {
                // No path + Read result: synthesize from the records array.
                let arr: Vec<QueryValue> = qr
                    .records
                    .iter()
                    .map(|r| r.as_value().into_owned())
                    .collect();
                QueryValue::List(arr)
            } else {
                // Indexed/field path into records. Only the `[n]` form is
                // meaningful without a record context; walk to the record
                // then navigate.
                let path = path.as_deref().unwrap_or("");
                if let Some(rest) = path.strip_prefix('[') {
                    if let Some(end) = rest.find(']') {
                        if let Ok(idx) = rest[..end].parse::<usize>() {
                            if let Some(record) = qr.records.get(idx) {
                                let record_qv = record.as_value();
                                let after = &rest[end + 1..];
                                if let Some(field_path) = after.strip_prefix('.') {
                                    return record_qv
                                        .get(field_path)
                                        .cloned()
                                        .unwrap_or(QueryValue::Null);
                                }
                                return record_qv.into_owned();
                            }
                        }
                    }
                }
                QueryValue::Null
            }
        }
        // $ref / $fn / $expr / $cond / $param — not meaningful as positional
        // params here (no record context to resolve against): collapse to
        // Null.  Logged at warn-level so a dynamic marker silently reaching a
        // function's positional arg is discoverable rather than invisible.
        fv @ (FilterValue::FieldRef { .. }
        | FilterValue::FnCall { .. }
        | FilterValue::Expr { .. }
        | FilterValue::Cond { .. }
        | FilterValue::Param { .. }) => {
            log::warn!(
                "Call param resolver: {:?} marker is not supported here \
                 (no record context for $ref/$fn/$expr/$cond) — collapsing to \
                 Null",
                fv
            );
            QueryValue::Null
        }
    }
}

/// Walk a path like `.field`, `[0]`, `[0].name` through a `QueryValue`.
/// Returns `None` on any miss / unsupported syntax.
/// `None` path returns the root value itself.
fn navigate_query_value<'a>(mut cur: &'a QueryValue, path: Option<&str>) -> Option<&'a QueryValue> {
    let Some(path) = path else {
        return Some(cur);
    };
    let mut rest = path;
    while !rest.is_empty() {
        if let Some(after_dot) = rest.strip_prefix('.') {
            let end = after_dot.find(['.', '[']).unwrap_or(after_dot.len());
            let field = &after_dot[..end];
            cur = match cur {
                QueryValue::Map(m) => m.get(field)?,
                _ => return None,
            };
            rest = &after_dot[end..];
        } else if rest.starts_with('[') {
            let bracket_end = rest.find(']')?;
            let idx: usize = rest[1..bracket_end].parse().ok()?;
            cur = match cur {
                QueryValue::List(l) => l.get(idx)?,
                _ => return None,
            };
            rest = &rest[bracket_end + 1..];
        } else {
            return None;
        }
    }
    Some(cur)
}

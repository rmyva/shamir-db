//! Catalogue commands (design §4.3, R5a): `db list`, `repo list`,
//! `repo describe`, `table list`, `index list`, `function list`,
//! `table describe`, `table schema`.
//!
//! Mapping evidence (all OBSERVED on this branch):
//! - `db list`      → `ListOp::Databases`    — handler
//!   `crates/engine/shamir-db/src/shamir_db/execute/admin_meta/admin_list.rs:33-38`
//!   (`List(Root)`; response key `databases`, `List` of `Str`).
//! - `repo list`    → `ListOp::Repos`        — `admin_list.rs:39-55`
//!   (`List(Database)`; key `repos`, `List` of `Str`).
//! - `repo describe` → `DescribeRepo` — `admin_repo_durability.rs:182-278`
//!   (superuser + `Read(repo)`; token refused before dispatch by the parser).
//! - `table list`   → `ListOp::Tables`       — `admin_list.rs:57-75`
//!   (`List(Store)`; keys `tables` (`List` of `Str`), `repo` (`Str`)).
//! - `index list`   → `ListOp::Indexes`      — `admin_list.rs:113-177`
//!   (`List(Table)`; keys `indexes` (`List` of objects with `name`/`unique`
//!   and optional `kind`), `table`, `repo`).
//! - `function list` → `ListOp::Functions`   — `admin_list.rs:179-211`
//!   (`List(FunctionNamespace)`; key `functions`, `List` of objects with
//!   `name`/`kind`).
//! - `table describe` → `DescribeTable`      — `admin_describe.rs:37-246`
//!   (`Read(table)`; gate-exempt per `handlers/admin.rs:616-629`; keys
//!   `describe_table`, `repo`, `schema`, `schema_version`, `indexes`,
//!   `validators`, `retention`/`buffer` (object or null), `owner`, `group`,
//!   `mode`; `owner` is the u64 principal id as an `Int` bit pattern, the
//!   `System` owner is the only special text, `group` may be null).
//! - `table schema` → `GetTableSchema`       —
//!   `crates/engine/shamir-db/src/shamir_db/execute/admin_ddl/admin_schema.rs:932-988`
//!   (`Read(table)`; keys `get_table_schema`, `repo`, `schema` (`List` of
//!   objects), `schema_version`).
//!
//! Builders only (no direct `BatchOp` construction); the wire batch goes
//! through `SessionClient::execute(db, batch)`, the request built by
//! `Batch::try_build` (client-side validation). Table-scoped builders
//! get an explicit `.repo(...)` to avoid silently targeting the default `main`.
//!
//! Errors: server failures route through
//! `crate::shell::connect::request_error`, which maps only the typed
//! allowlisted codes (access/permission → Denied, everything else →
//! Server). Local structural problems are never forged into
//! `ClientError`: a builder-validation failure is `Usage`, and a response
//! shape that does not match the observed contract above is `Server` —
//! projected through typed allowlisting so a malicious peer cannot smuggle
//! unexpected fields or shapes into the output. Authorized text is sanitized.

use serde_json::Value;

use super::super::output::{value, ShellError};
use crate::shell::connect::{request_error, SessionClient};
use shamir_client::BatchResponse;
use shamir_collections::TMap;
use shamir_query_builder::batch::Batch;
use shamir_query_builder::ddl::{
    describe_repo, describe_table, get_table_schema, list_databases, list_functions, list_indexes,
    list_repos, list_tables,
};
use shamir_query_types::batch::BatchOp;
use shamir_query_types::read::QueryRecord;
use shamir_types::types::value::QueryValue;

/// R5a catalogue commands.
#[derive(Debug, Clone)]
pub(in crate::shell) enum Catalogue {
    Databases,
    Repos,
    DescribeRepo { repo: String },
    Tables { repo: String },
    Indexes { repo: String, table: String },
    Functions { folder: Option<String> },
    Describe { repo: String, table: String },
    Schema { repo: String, table: String },
}

impl Catalogue {
    /// Stable space-separated command id (JSON v1 `command` field, design §5).
    pub(in crate::shell) fn id(&self) -> &'static str {
        match self {
            Catalogue::Databases => "db list",
            Catalogue::Repos => "repo list",
            Catalogue::DescribeRepo { .. } => "repo describe",
            Catalogue::Tables { .. } => "table list",
            Catalogue::Indexes { .. } => "index list",
            Catalogue::Functions { .. } => "function list",
            Catalogue::Describe { .. } => "table describe",
            Catalogue::Schema { .. } => "table schema",
        }
    }

    /// Wire operation label (JSON v1 `op` field, design §5).
    pub(in crate::shell) fn op(&self) -> &'static str {
        match self {
            Catalogue::Databases
            | Catalogue::Repos
            | Catalogue::Tables { .. }
            | Catalogue::Indexes { .. }
            | Catalogue::Functions { .. } => "List",
            Catalogue::DescribeRepo { .. } => "DescribeRepo",
            Catalogue::Describe { .. } => "DescribeTable",
            Catalogue::Schema { .. } => "GetTableSchema",
        }
    }

    /// Execute against the routing database `db` (the batch permission
    /// floor, design §4 preamble). Authorization happens exclusively on the
    /// normal remote route; this code sends no privilege material.
    /// cancel-safe: NO — a sent batch may still execute; do not retry on timeout.
    pub(in crate::shell) async fn execute(
        &self,
        client: &SessionClient,
        db: &str,
    ) -> Result<Value, ShellError> {
        let op = match self {
            Catalogue::Databases => list_databases(),
            Catalogue::Repos => list_repos(),
            Catalogue::DescribeRepo { repo } => describe_repo(repo.clone()).repo(db).build(),
            Catalogue::Tables { repo } => list_tables().repo(repo.clone()).build(),
            Catalogue::Indexes { repo, table } => {
                list_indexes(table.clone()).repo(repo.clone()).build()
            }
            Catalogue::Functions { folder } => {
                let b = list_functions();
                match folder {
                    Some(f) => b.folder(f.clone()).build(),
                    None => b.build(),
                }
            }
            Catalogue::Describe { repo, table } => {
                describe_table(table.clone()).repo(repo.clone()).build()
            }
            Catalogue::Schema { repo, table } => {
                get_table_schema(table.clone()).repo(repo.clone()).build()
            }
        };
        self.run(client, db, op).await
    }

    /// Send one single-op batch and project the single direct result.
    async fn run(
        &self,
        client: &SessionClient,
        db: &str,
        op: BatchOp,
    ) -> Result<Value, ShellError> {
        let mut batch = Batch::new();
        batch.insert(RESULT_ALIAS, op);
        // Client-side validation failure is a local usage problem, not a
        // server outcome — never forged into a `ClientError`.
        let request = batch.try_build().map_err(|_| ShellError::Usage)?;
        let response = client.execute(db, request).await.map_err(request_error)?;
        let projected = self.extract(response, db)?;
        client.check_catalogue_output(&projected)?;
        Ok(projected)
    }

    /// Validate the response shape and project the allowlisted fields.
    fn extract(&self, mut response: BatchResponse, db: &str) -> Result<Value, ShellError> {
        let result = response
            .results
            .get_mut(RESULT_ALIAS)
            .ok_or(ShellError::Server)?;
        if result.skipped {
            return Err(ShellError::Server);
        }
        let records = std::mem::take(&mut result.records);
        // Exactly one direct record carrying the payload map; anything else
        // is a protocol mismatch, reported as `Server` without echoing any
        // peer-supplied detail.
        let qv = {
            let mut iter = records.into_iter();
            match iter.next() {
                Some(QueryRecord::Direct(qv)) if iter.next().is_none() => qv,
                _ => return Err(ShellError::Server),
            }
        };
        self.project(qv, db)
    }

    /// Project the documented fields and check their observed server shapes.
    /// Repo durability maps are allowlisted separately; table description
    /// projects principal IDs from their wire bit patterns.
    fn project(&self, qv: QueryValue, db: &str) -> Result<Value, ShellError> {
        let mut fields = match qv {
            QueryValue::Map(fields) => fields,
            _ => return Err(ShellError::Server),
        };
        match self {
            Catalogue::Databases => {
                keep(&mut fields, &["databases"])?;
                require_str_list(&fields, "databases")?;
            }
            Catalogue::Repos => {
                keep(&mut fields, &["repos"])?;
                require_str_list(&fields, "repos")?;
            }
            Catalogue::DescribeRepo { repo } => {
                let budgets = project_repo(&mut fields, repo, db)?;
                let mut json = value(&QueryValue::Map(fields))?;
                write_budget_numbers(&mut json, budgets);
                return Ok(json);
            }
            Catalogue::Tables { .. } => {
                keep(&mut fields, &["tables", "repo"])?;
                require_str_list(&fields, "tables")?;
                require_str(&fields, "repo")?;
            }
            Catalogue::Indexes { .. } => {
                keep(&mut fields, &["indexes", "table", "repo"])?;
                check_index_list(&mut fields, "indexes")?;
                require_str(&fields, "table")?;
                require_str(&fields, "repo")?;
            }
            Catalogue::Functions { .. } => {
                keep(&mut fields, &["functions"])?;
                check_function_list(&mut fields, "functions")?;
            }
            Catalogue::Describe { .. } => {
                keep(
                    &mut fields,
                    &[
                        "describe_table",
                        "repo",
                        "schema",
                        "schema_version",
                        "indexes",
                        "validators",
                        "retention",
                        "buffer",
                        "owner",
                        "group",
                        "mode",
                    ],
                )?;
                require_str(&fields, "describe_table")?;
                require_str(&fields, "repo")?;
                check_map_list(&fields, "schema")?;
                require_int(&fields, "schema_version")?;
                check_index_list(&mut fields, "indexes")?;
                check_validator_list(&mut fields, "validators")?;
                check_map_or_null(&fields, "retention")?;
                check_map_or_null(&fields, "buffer")?;
                principal(&mut fields, "owner", false)?;
                principal(&mut fields, "group", true)?;
                require_int(&fields, "mode")?;
            }
            Catalogue::Schema { .. } => {
                keep(
                    &mut fields,
                    &["get_table_schema", "repo", "schema", "schema_version"],
                )?;
                require_str(&fields, "get_table_schema")?;
                require_str(&fields, "repo")?;
                check_map_list(&fields, "schema")?;
                require_int(&fields, "schema_version")?;
            }
        }
        value(&QueryValue::Map(fields))
    }
}

/// `DescribeRepo` calls the routing database `repo` in its response. Check
/// both echoed labels against the request and expose that label as `db`.
/// This view is for durability: omit the server-local path and the repository
/// replication identity (`lineage_id` and `history_epoch`).
fn project_repo(
    fields: &mut TMap<String, QueryValue>,
    repo: &str,
    db: &str,
) -> Result<(Option<u64>, Option<u64>), ShellError> {
    keep(
        fields,
        &[
            "describe_repo",
            "repo",
            "engine",
            "durability",
            "durability_override",
        ],
    )?;
    if fields.get("describe_repo").and_then(QueryValue::as_str) != Some(repo)
        || fields.get("repo").and_then(QueryValue::as_str) != Some(db)
        || !matches!(
            fields.get("engine").and_then(QueryValue::as_str),
            Some("in_memory" | "fjall" | "fjall_raw" | "hybrid")
        )
    {
        return Err(ShellError::Server);
    }
    let durability_budget = check_durability(fields, "durability", true)?;
    let override_budget = check_durability(fields, "durability_override", false)?;
    let db_label = fields.shift_remove("repo").ok_or(ShellError::Server)?;
    fields.insert("db".into(), db_label);
    Ok((durability_budget, override_budget))
}

/// `effective_durability_record` and `durability_to_record` share the same
/// serializer. The configured override may be null or sparse. The effective
/// policy requires the three enum leaves; a zero budget is omitted upstream.
fn check_durability(
    fields: &mut TMap<String, QueryValue>,
    key: &str,
    effective: bool,
) -> Result<Option<u64>, ShellError> {
    let policy = match fields.get_mut(key) {
        Some(QueryValue::Map(policy)) => policy,
        Some(QueryValue::Null) if !effective => return Ok(None),
        _ => return Err(ShellError::Server),
    };
    const MODES: [(&str, &[&str]); 3] = [
        ("wal_commit", &["buffered", "synced"]),
        (
            "on_wal_retention_exceeded",
            &["refuse_writes", "declare_gap"],
        ),
        ("replication_serve", &["durable", "completed_reseed"]),
    ];
    const BUDGET: &str = "wal_retention_max_bytes";
    policy.retain(|key, _| key == BUDGET || MODES.iter().any(|(name, _)| key == *name));
    for (name, allowed) in MODES {
        match policy.get(name) {
            Some(QueryValue::Str(mode)) if allowed.contains(&mode.as_str()) => {}
            None if !effective => {}
            _ => return Err(ShellError::Server),
        }
    }
    match policy.get(BUDGET) {
        None => Ok(None),
        // Big budgets arrive as a decimal string (wire MessagePack of
        // `Big`, decoded as `Str`); ASCII digits only, checked — zero and
        // overflowing text keep the typed error. The single parsed result
        // is emitted as a JSON number by write_budget_numbers.
        Some(QueryValue::Int(n)) if *n > 0 => Ok(Some(*n as u64)),
        Some(QueryValue::Big(n)) => match n.to_string().parse::<u64>() {
            Ok(parsed) if parsed > 0 => Ok(Some(parsed)),
            _ => Err(ShellError::Server),
        },
        Some(QueryValue::Str(text)) if is_ascii_decimal(text) => {
            // Digits-only input can only fail here on overflow.
            let parsed: u64 = text.parse().map_err(|_| ShellError::Server)?;
            if parsed == 0 {
                Err(ShellError::Server)
            } else {
                Ok(Some(parsed))
            }
        }
        Some(_) => Err(ShellError::Server),
    }
}

/// Non-empty ASCII digits only (`u64::from_str` would admit `+`).
fn is_ascii_decimal(text: &str) -> bool {
    !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit())
}

/// Write ONLY the parsed `wal_retention_max_bytes` budgets — the only u64
/// fields of the description — as JSON numbers over the generic rendering.
fn write_budget_numbers(json: &mut Value, budgets: (Option<u64>, Option<u64>)) {
    let Some(obj) = json.as_object_mut() else {
        return;
    };
    for (key, parsed) in [
        ("durability", budgets.0),
        ("durability_override", budgets.1),
    ] {
        if let Some(parsed) = parsed {
            if let Some(policy) = obj.get_mut(key).and_then(Value::as_object_mut) {
                policy.insert("wal_retention_max_bytes".into(), Value::from(parsed));
            }
        }
    }
}

/// Response alias for the single-op batch.
const RESULT_ALIAS: &str = "result";

/// Keep only `keys` in the payload map; a missing allowlisted key is a
/// shape violation (the observed server always emits every key).
fn keep(fields: &mut TMap<String, QueryValue>, keys: &[&str]) -> Result<(), ShellError> {
    fields.retain(|k, _| keys.contains(&k.as_str()));
    for &key in keys {
        if !fields.contains_key(key) {
            return Err(ShellError::Server);
        }
    }
    Ok(())
}

fn require_str(fields: &TMap<String, QueryValue>, key: &str) -> Result<(), ShellError> {
    match fields.get(key) {
        Some(QueryValue::Str(_)) => Ok(()),
        _ => Err(ShellError::Server),
    }
}

fn require_int(fields: &TMap<String, QueryValue>, key: &str) -> Result<(), ShellError> {
    match fields.get(key) {
        Some(QueryValue::Int(_)) => Ok(()),
        _ => Err(ShellError::Server),
    }
}

fn require_str_list(fields: &TMap<String, QueryValue>, key: &str) -> Result<(), ShellError> {
    match fields.get(key) {
        Some(QueryValue::List(items)) => {
            if items.iter().all(|i| matches!(i, QueryValue::Str(_))) {
                Ok(())
            } else {
                Err(ShellError::Server)
            }
        }
        _ => Err(ShellError::Server),
    }
}

/// `List` whose elements are all objects.
fn check_map_list(fields: &TMap<String, QueryValue>, key: &str) -> Result<(), ShellError> {
    match fields.get(key) {
        Some(QueryValue::List(items)) => {
            if items.iter().all(|i| matches!(i, QueryValue::Map(_))) {
                Ok(())
            } else {
                Err(ShellError::Server)
            }
        }
        _ => Err(ShellError::Server),
    }
}

/// Object or null (retention/buffer are null when unset).
fn check_map_or_null(fields: &TMap<String, QueryValue>, key: &str) -> Result<(), ShellError> {
    match fields.get(key) {
        Some(QueryValue::Map(_)) | Some(QueryValue::Null) => Ok(()),
        _ => Err(ShellError::Server),
    }
}

/// Index entries in place: only `name`/`unique`/`kind` survive; `name`
/// (`Str`) and `unique` (`Bool`) are always present, `kind` (`Str`) is
/// optional (`admin_list.rs`/`admin_describe.rs` emit plain and unique
/// indexes without it).
fn check_index_list(fields: &mut TMap<String, QueryValue>, key: &str) -> Result<(), ShellError> {
    let items = match fields.get_mut(key) {
        Some(QueryValue::List(items)) => items,
        _ => return Err(ShellError::Server),
    };
    for item in items.iter_mut() {
        let entry = match item {
            QueryValue::Map(entry) => entry,
            _ => return Err(ShellError::Server),
        };
        entry.retain(|k, _| matches!(k.as_str(), "name" | "unique" | "kind"));
        require_str(entry, "name")?;
        match entry.get("unique") {
            Some(QueryValue::Bool(_)) => {}
            _ => return Err(ShellError::Server),
        }
        match entry.get("kind") {
            None | Some(QueryValue::Str(_)) => {}
            Some(_) => return Err(ShellError::Server),
        }
    }
    Ok(())
}

/// Function entries in place: `name` and `kind`, both `Str`.
fn check_function_list(fields: &mut TMap<String, QueryValue>, key: &str) -> Result<(), ShellError> {
    let items = match fields.get_mut(key) {
        Some(QueryValue::List(items)) => items,
        _ => return Err(ShellError::Server),
    };
    for item in items.iter_mut() {
        let entry = match item {
            QueryValue::Map(entry) => entry,
            _ => return Err(ShellError::Server),
        };
        entry.retain(|k, _| matches!(k.as_str(), "name" | "kind"));
        require_str(entry, "name")?;
        require_str(entry, "kind")?;
    }
    Ok(())
}

/// Validator bindings in place: `validator_id` (`Str`) and `priority`
/// (`Int`) (`admin_describe.rs:137-147`).
fn check_validator_list(
    fields: &mut TMap<String, QueryValue>,
    key: &str,
) -> Result<(), ShellError> {
    let items = match fields.get_mut(key) {
        Some(QueryValue::List(items)) => items,
        _ => return Err(ShellError::Server),
    };
    for item in items.iter_mut() {
        let entry = match item {
            QueryValue::Map(entry) => entry,
            _ => return Err(ShellError::Server),
        };
        entry.retain(|k, _| matches!(k.as_str(), "validator_id" | "priority"));
        require_str(entry, "validator_id")?;
        require_int(entry, "priority")?;
    }
    Ok(())
}

/// Principal id projection in place (`admin_describe.rs:199-213`): a real
/// owner/group is the u64 principal id carried as the `Int` bit pattern,
/// rendered as its unsigned decimal string; the `System` owner is the only
/// special text and passes through unchanged; `group` may be null
/// (unknown). Any other shape is a violation.
fn principal(
    fields: &mut TMap<String, QueryValue>,
    key: &str,
    allow_null: bool,
) -> Result<(), ShellError> {
    let entry = fields.get_mut(key).ok_or(ShellError::Server)?;
    match entry {
        QueryValue::Int(id) => {
            let rendered = (*id as u64).to_string();
            *entry = QueryValue::Str(rendered);
        }
        QueryValue::Str(text) if text == "System" => {}
        QueryValue::Null if allow_null => {}
        _ => return Err(ShellError::Server),
    }
    Ok(())
}

#[cfg(test)]
#[path = "../tests/commands/catalogue_budget_tests.rs"]
mod catalogue_budget_tests;

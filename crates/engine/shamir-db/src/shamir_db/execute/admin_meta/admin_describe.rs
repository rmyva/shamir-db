//! Admin handler: DescribeTable.

use crate::access::{Action, ResourcePath};
use crate::query::admin::DescribeTableOp;
use crate::query::batch::BatchError;
use crate::query::read::QueryResult;
use crate::shamir_db::shamir_db::schema_management::{SCHEMA_FIELD, SCHEMA_VERSION_FIELD};
use crate::types::value::QueryValue;
use shamir_types::core::interner::InternerKey;
use shamir_types::mpack;

use super::admin_dispatch::ShamirAdminExecutor;
use super::helpers::{admin_result, dto_from_storage, index2_kind, to_qv};
use super::schema_codec::{dto_list_from_catalogue, serialise_rules_flat};

fn err_code(code: &str, msg: impl Into<String>) -> BatchError {
    BatchError::QueryError {
        alias: String::new(),
        message: msg.into(),
        code: Some(code.to_string()),
    }
}

fn err_access(e: shamir_types::access::AccessError) -> BatchError {
    err_code("access_denied", e.to_string())
}

fn err(msg: impl Into<String>) -> BatchError {
    BatchError::QueryError {
        alias: String::new(),
        message: msg.into(),
        code: None,
    }
}

impl ShamirAdminExecutor {
    pub(super) async fn handle_describe_table(
        &self,
        op: &DescribeTableOp,
    ) -> Result<QueryResult, BatchError> {
        let table = &op.describe_table;
        let repo = &op.repo;
        let db = &self.db_name;

        // Authz: Action::Read on the table (introspection).
        self.shamir
            .authorize_access(
                &self.actor,
                &ResourcePath::table(db.clone(), repo.clone(), table.clone()),
                Action::Read,
            )
            .await
            .map_err(err_access)?;

        // ── 1. Schema ────────────────────────────────────────────────
        let rec = self
            .shamir
            .system_store()
            .load_table_record(db, repo, table)
            .await
            .map_err(|e| err_code("internal_error", e.to_string()))?
            .ok_or_else(|| err_code("not_found", format!("table '{db}/{repo}/{table}'")))?;

        let interner_mgr = self
            .shamir
            .resolve_repo_interner(db, repo)
            .await
            .map_err(|e| err_code("internal_error", e.to_string()))?;
        let interner = interner_mgr
            .get()
            .await
            .map_err(|e| err_code("internal_error", e.to_string()))?;

        let schema_dto = match rec.get(SCHEMA_FIELD) {
            Some(qv) if !matches!(qv, QueryValue::Null) => {
                dto_list_from_catalogue(qv, interner).map_err(|e| err_code("bad_schema", e))?
            }
            _ => Vec::new(),
        };
        let schema_version = rec
            .get(SCHEMA_VERSION_FIELD)
            .and_then(|v| v.as_i64())
            .unwrap_or(0);
        let schema_qv = serialise_rules_flat(&schema_dto);

        // ── 2. Indexes ───────────────────────────────────────────────
        let db_inst = self
            .shamir
            .get_db(db)
            .ok_or_else(|| err(format!("Database '{}' not found", db)))?;
        let tm = db_inst
            .get_table(repo, table)
            .await
            .map_err(|e| err(e.to_string()))?;

        let mut indexes: Vec<QueryValue> = Vec::new();
        for def in tm.index_definitions() {
            let name = interner
                .get_str(&InternerKey::new(def.name_interned))
                .map(|arc| arc.to_string())
                .unwrap_or_else(|| def.name_interned.to_string());
            indexes.push(mpack!({"name": @(QueryValue::Str(name)), "unique": false}));
        }
        for def in tm.unique_index_definitions() {
            let name = interner
                .get_str(&InternerKey::new(def.name_interned))
                .map(|arc| arc.to_string())
                .unwrap_or_else(|| def.name_interned.to_string());
            indexes.push(mpack!({"name": @(QueryValue::Str(name)), "unique": true}));
        }
        for def in tm.sorted_index_definitions() {
            let name = interner
                .get_str(&InternerKey::new(def.name_interned))
                .map(|arc| arc.to_string())
                .unwrap_or_else(|| def.name_interned.to_string());
            indexes.push(mpack!({
                "name": @(QueryValue::Str(name)),
                "unique": false,
                "kind": @(QueryValue::Str("sorted".to_owned())),
            }));
        }
        for desc in tm.index2_descriptors().await {
            let (kind, unique) = index2_kind(&desc.kind);
            indexes.push(mpack!({
                "name": @(QueryValue::Str(desc.name)),
                "unique": @(QueryValue::Bool(unique)),
                "kind": @(kind),
            }));
        }

        // ── 3. Validators ────────────────────────────────────────────
        let bindings = self
            .shamir
            .list_validator_bindings(db, repo, table)
            .await
            .map_err(|e| err(e.to_string()))?;
        let validators_qv: Vec<QueryValue> = bindings
            .iter()
            .map(|b| {
                mpack!({
                    "validator_id": @(QueryValue::Str(b.validator_id.to_string())),
                    "priority": @(QueryValue::Int(b.priority as i64)),
                })
            })
            .collect();

        // ── 4. Retention ─────────────────────────────────────────────
        let retention_qv = {
            let repo_instance = db_inst.get_repo(repo);
            let token = crate::engine::table::table_token_for(table);
            match repo_instance.and_then(|r| {
                r.per_table_mvcc()
                    .get_sync(&token)
                    .map(|arc| std::sync::Arc::clone(&arc))
            }) {
                Some(mvcc) => {
                    let r = mvcc.retention();
                    // DESCRIBE response: admin-introspection reference-form,
                    // documented exception from builder-only rule.
                    let mut m = shamir_collections::new_map();
                    match r.max_age_secs {
                        Some(v) => m.insert("max_age_secs".to_string(), QueryValue::Int(v as i64)),
                        None => m.insert("max_age_secs".to_string(), QueryValue::Null),
                    };
                    match r.max_count {
                        Some(v) => m.insert("max_count".to_string(), QueryValue::Int(v as i64)),
                        None => m.insert("max_count".to_string(), QueryValue::Null),
                    };
                    match r.min_count {
                        Some(v) => m.insert("min_count".to_string(), QueryValue::Int(v as i64)),
                        None => m.insert("min_count".to_string(), QueryValue::Null),
                    };
                    QueryValue::Map(m)
                }
                None => QueryValue::Null,
            }
        };

        // ── 5. Buffer config ─────────────────────────────────────────
        let buffer_qv = match tm
            .get_buffer_config()
            .await
            .map_err(|e| err(e.to_string()))?
        {
            Some(c) => to_qv(&dto_from_storage(&c))?,
            None => QueryValue::Null,
        };

        // ── 6. Access meta (owner / mode / group) ────────────────────
        let meta = self
            .shamir
            .resource_meta(&ResourcePath::table(
                db.clone(),
                repo.clone(),
                table.clone(),
            ))
            .await
            .map_err(|e| err(e.to_string()))?;
        let owner_qv = match &meta.owner {
            crate::access::Actor::System => QueryValue::Str("System".to_string()),
            // `Admin` and `User` both carry a real principal64 owner id
            // (`Admin` is real ownership, not the anonymous System id); they
            // render identically here. In practice `meta.owner` comes from
            // `from_record`→`from_owner_id`, which never produces `Admin`
            // (admin-ness is a live session property, never persisted) — the
            // arm exists only for exhaustiveness.
            crate::access::Actor::Admin(id) | crate::access::Actor::User(id) => {
                QueryValue::Int(*id as i64)
            }
        };
        let group_qv = match meta.group {
            Some(g) => QueryValue::Int(g as i64),
            None => QueryValue::Null,
        };
        let mode_qv = QueryValue::Int(meta.mode as i64);

        // ── Compose final response ───────────────────────────────────
        Ok(admin_result(mpack!({
            "describe_table": @(QueryValue::Str(table.clone())),
            "repo": @(QueryValue::Str(repo.clone())),
            "schema": @schema_qv,
            "schema_version": @QueryValue::Int(schema_version),
            "indexes": @(QueryValue::List(indexes)),
            "validators": @(QueryValue::List(validators_qv)),
            "retention": @retention_qv,
            "buffer": @buffer_qv,
            "owner": @owner_qv,
            "group": @group_qv,
            "mode": @mode_qv,
        })))
    }
}

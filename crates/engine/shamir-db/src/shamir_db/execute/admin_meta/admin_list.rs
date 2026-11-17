//! Admin handler: List.

use crate::access::{Action, ResourcePath};
use crate::query::admin::ListOp;
use crate::query::batch::BatchError;
use crate::query::read::QueryResult;
use crate::types::value::QueryValue;
use shamir_types::mpack;

use super::admin_dispatch::ShamirAdminExecutor;
use super::helpers::{admin_result, index2_kind};

impl ShamirAdminExecutor {
    pub(super) async fn handle_list(&self, list_op: &ListOp) -> Result<QueryResult, BatchError> {
        let err = |msg: String| BatchError::QueryError {
            alias: String::new(),
            message: msg,
            code: None,
        };
        let err_code = |code: &str, msg: String| BatchError::QueryError {
            alias: String::new(),
            message: msg,
            code: Some(code.to_string()),
        };
        let err_access =
            |e: shamir_types::access::AccessError| err_code("access_denied", e.to_string());

        match list_op {
            ListOp::Databases => {
                self.shamir
                    .authorize_access(&self.actor, &ResourcePath::Root, Action::List)
                    .await
                    .map_err(err_access)?;
                let dbs = self.shamir.list_dbs();
                Ok(admin_result(mpack!({
                    "databases": @(QueryValue::List(dbs.into_iter().map(QueryValue::Str).collect())),
                })))
            }
            ListOp::Repos => {
                self.shamir
                    .authorize_access(
                        &self.actor,
                        &ResourcePath::database(self.db_name.clone()),
                        Action::List,
                    )
                    .await
                    .map_err(err_access)?;
                let db = self
                    .shamir
                    .get_db(&self.db_name)
                    .ok_or_else(|| err(format!("Database '{}' not found", self.db_name)))?;
                let repos = db.list_repos();
                Ok(admin_result(mpack!({
                    "repos": @(QueryValue::List(repos.into_iter().map(QueryValue::Str).collect())),
                })))
            }
            ListOp::Tables { repo } => {
                self.shamir
                    .authorize_access(
                        &self.actor,
                        &ResourcePath::store(self.db_name.clone(), repo.clone()),
                        Action::List,
                    )
                    .await
                    .map_err(err_access)?;
                let db = self
                    .shamir
                    .get_db(&self.db_name)
                    .ok_or_else(|| err(format!("Database '{}' not found", self.db_name)))?;
                let tables = db.list_tables(repo).map_err(|e| err(e.to_string()))?;
                Ok(admin_result(mpack!({
                    "tables": @(QueryValue::List(tables.into_iter().map(QueryValue::Str).collect())),
                    "repo": @(QueryValue::Str(repo.clone())),
                })))
            }
            ListOp::Users => {
                self.shamir
                    .authorize_access(&self.actor, &ResourcePath::Root, Action::Manage)
                    .await
                    .map_err(err_access)?;
                // Task #559: list users from the injected PrincipalResolver
                // (the real durable directory) instead of Store B's
                // `users_table()`. With no resolver installed there is no
                // live principal source, so this degrades to a typed
                // `not_supported` (mirroring how the user-admin handlers
                // behave without a port) rather than silently returning
                // stale Store-B rows.
                let Some(resolver) = self.shamir.principal_resolver() else {
                    return Err(err_code(
                        "not_supported",
                        "user directory is not configured on this server".to_string(),
                    ));
                };
                let users: Vec<QueryValue> = resolver
                    .list()
                    .into_iter()
                    .map(|p| {
                        mpack!({
                            "name": @(QueryValue::Str(p.name)),
                            "principal64": @(QueryValue::Int(p.principal64 as i64)),
                            "superuser": @(QueryValue::Bool(p.superuser)),
                            "database": @(match p.database {
                                Some(d) => QueryValue::Str(d),
                                None => QueryValue::Null,
                            }),
                        })
                    })
                    .collect();
                Ok(admin_result(mpack!({
                    "users": @(QueryValue::List(users)),
                })))
            }
            ListOp::Indexes { table, repo } => {
                self.shamir
                    .authorize_access(
                        &self.actor,
                        &ResourcePath::table(self.db_name.clone(), repo.clone(), table.clone()),
                        Action::List,
                    )
                    .await
                    .map_err(err_access)?;
                let db = self
                    .shamir
                    .get_db(&self.db_name)
                    .ok_or_else(|| err(format!("Database '{}' not found", self.db_name)))?;
                let tm = db
                    .get_table(repo, table)
                    .await
                    .map_err(|e| err(e.to_string()))?;
                // S5c-X F1 (Round 6): index-name resolution through the engine's
                // read-only interner view — no &Interner escape.
                let names = tm
                    .info_store_read_view()
                    .interner_read_view()
                    .await
                    .map_err(|e| err(e.to_string()))?;

                let mut indexes: Vec<QueryValue> = Vec::new();
                for def in tm.index_definitions() {
                    let name = names
                        .get_str(&crate::core::interner::InternerKey::new(def.name_interned))
                        .map(|arc| arc.to_string())
                        .unwrap_or_else(|| def.name_interned.to_string());
                    indexes.push(mpack!({"name": @(QueryValue::Str(name)), "unique": false}));
                }
                for def in tm.unique_index_definitions() {
                    let name = names
                        .get_str(&crate::core::interner::InternerKey::new(def.name_interned))
                        .map(|arc| arc.to_string())
                        .unwrap_or_else(|| def.name_interned.to_string());
                    indexes.push(mpack!({"name": @(QueryValue::Str(name)), "unique": true}));
                }
                for def in tm.sorted_index_definitions() {
                    let name = names
                        .get_str(&crate::core::interner::InternerKey::new(def.name_interned))
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

                Ok(admin_result(mpack!({
                    "indexes": @(QueryValue::List(indexes)),
                    "table": @(QueryValue::Str(table.clone())),
                    "repo": @(QueryValue::Str(repo.clone())),
                })))
            }
            ListOp::Functions { folder } => {
                self.shamir
                    .authorize_access(&self.actor, &ResourcePath::FunctionNamespace, Action::List)
                    .await
                    .map_err(err_access)?;
                let mut entries = self
                    .shamir
                    .list_functions_with_kind()
                    .await
                    .map_err(|e| err(e.to_string()))?;
                if let Some(prefix) = folder {
                    let prefix_slash = if prefix.ends_with('/') {
                        prefix.clone()
                    } else {
                        format!("{}/", prefix)
                    };
                    entries.retain(|(n, _)| n.starts_with(&prefix_slash));
                }
                let items: Vec<QueryValue> = entries
                    .into_iter()
                    .map(|(name, kind)| {
                        mpack!({
                            "name": @(QueryValue::Str(name)),
                            "kind": @(QueryValue::Str(kind.as_str().to_string())),
                        })
                    })
                    .collect();
                Ok(admin_result(mpack!({
                    "functions": @(QueryValue::List(items)),
                })))
            }
            ListOp::Validators => {
                self.shamir
                    .authorize_access(&self.actor, &ResourcePath::FunctionNamespace, Action::List)
                    .await
                    .map_err(err_access)?;
                let entries = self
                    .shamir
                    .list_validators_with_kind()
                    .await
                    .map_err(|e| err(e.to_string()))?;
                let items: Vec<QueryValue> = entries
                    .iter()
                    .map(|(id, name, kind)| {
                        let bound = self.shamir.validators().bound_tables(id);
                        mpack!({
                            "id": @(QueryValue::Str(id.to_string())),
                            "name": @(QueryValue::Str(name.clone())),
                            "kind": @(QueryValue::Str(kind.as_str().to_string())),
                            "bound_in": @(QueryValue::List(bound.into_iter().map(QueryValue::Str).collect())),
                        })
                    })
                    .collect();
                Ok(admin_result(mpack!({
                    "validators": @(QueryValue::List(items)),
                })))
            }
            ListOp::FunctionFolders { parent } => {
                self.shamir
                    .authorize_access(&self.actor, &ResourcePath::FunctionNamespace, Action::List)
                    .await
                    .map_err(err_access)?;
                let mut folders = self
                    .shamir
                    .list_function_folders()
                    .await
                    .map_err(|e| err(e.to_string()))?;
                if let Some(prefix) = parent {
                    let prefix_slash = if prefix.ends_with('/') {
                        prefix.clone()
                    } else {
                        format!("{}/", prefix)
                    };
                    folders.retain(|f| f.starts_with(&prefix_slash));
                }
                Ok(admin_result(mpack!({
                    "function_folders": @(QueryValue::List(folders.into_iter().map(QueryValue::Str).collect())),
                })))
            }
        }
    }
}

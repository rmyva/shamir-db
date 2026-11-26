//! Rights commands (design `admin-shell.md` §4.3, §9 PR4): `user list`,
//! `group tree`, `group members`, `rights show`, and the rights mutations
//! `chmod`, `chown` and `chgrp`.
//!
//! Every command runs through the existing batch path — no wire change:
//! - `user list` sends the existing `List`/`Users` admin op and projects the
//!   strictly allowlisted principal directory rows.
//! - `group tree`, `group members` and `rights show` send the existing
//!   `AccessTree` admin op. `group tree` forwards its optional database
//!   filter; the other two always request the unfiltered tree.
//! - `rights show` selects one node LOCALLY from the authorized tree. The
//!   path argument is never sent as a `ResourceRef` and no second request is
//!   made.
//! - `chmod`, `chown` and `chgrp` reuse the same path selector, resolve an
//!   owner or group NAME locally from the same `user list` / `access_tree`
//!   batch the read commands send, and then call the typed
//!   `Client::chmod` / `chown` / `chgrp` signing methods. The server decides
//!   authorization; the shell sends no privilege material.
//! - The selector itself lives in `selector.rs`: `root`/`/` is the synthetic
//!   root, `db:NAME` is always the database `NAME` (this is how a database
//!   named `root` is addressed), `function:NAME` is a stored function whose
//!   name may contain `/`, and a bare path is one to three resource
//!   components.
//!
//! Errors: server failures route through `crate::shell::connect::request_error`
//! (allowlisted stable codes only, never a verbatim server string). A local
//! selector refusal is `ShellError::Usage`; a selector that names no node is
//! `ShellError::NotFound`; a response whose shape does not match the engine's
//! own record is `ShellError::Server`. Every authorized string is sanitized
//! through `output::value` / `output::sanitize`; peer-supplied text is never
//! echoed raw.

use serde_json::{json, Value};
use shamir_query_builder::batch::Batch;
use shamir_query_builder::ddl::{access_tree, list_users};
use shamir_query_types::batch::BatchResponse;
use shamir_query_types::read::{QueryRecord, QueryResult};
use shamir_types::types::value::QueryValue;

use self::mutate::Mutation;
use super::super::output::{self, value, ShellError};
use super::selector::{normalized_path, parse_selector, Selector, RESOURCE_KINDS};
use crate::shell::connect::{request_error, SessionClient};

#[path = "mutate.rs"]
mod mutate;

/// Response alias of the `user list` batch entry.
const USER_ALIAS: &str = "users";

/// Response alias of the `AccessTree` batch entry.
const TREE_ALIAS: &str = "tree";

/// Deepest resource node level the shell projects (the root is level 1).
const MAX_NODE_LEVELS: usize = 8;

/// Most nodes the shell projects across one resource tree. The cap applies to
/// the whole-tree renderings only (`group tree`, `group members`); `rights
/// show` projects ONE selected node and is therefore unaffected by a large
/// catalogue.
const MAX_NODES: usize = 2000;

/// R5 step 4 rights commands.
#[derive(Debug, Clone)]
pub(in crate::shell) enum AccessCommand {
    /// `user list` — the principal directory rows.
    Users,
    /// `group tree [--db DB]` — the access tree, optionally one database.
    GroupTree { db: Option<String> },
    /// `group members` — the groups with their members, unfiltered tree.
    GroupMembers,
    /// `rights show PATH` — one node selected from the unfiltered tree.
    RightsShow { path: String },
    /// `chmod PATH MODE` — change the mode of one securable resource.
    Chmod { path: String, mode: String },
    /// `chown PATH OWNER` — change the owner of one securable resource.
    Chown { path: String, owner: String },
    /// `chgrp PATH GROUP` — change the group of one securable resource.
    Chgrp { path: String, group: String },
}

impl AccessCommand {
    /// Stable space-separated command id (JSON v1 `command` field, §5).
    pub(in crate::shell) fn id(&self) -> &'static str {
        match self {
            AccessCommand::Users => "user list",
            AccessCommand::GroupTree { .. } => "group tree",
            AccessCommand::GroupMembers => "group members",
            AccessCommand::RightsShow { .. } => "rights show",
            AccessCommand::Chmod { .. } => "chmod",
            AccessCommand::Chown { .. } => "chown",
            AccessCommand::Chgrp { .. } => "chgrp",
        }
    }

    /// Wire operation label (JSON v1 `op` field, §5).
    pub(in crate::shell) fn op(&self) -> &'static str {
        match self {
            AccessCommand::Users => "List",
            AccessCommand::GroupTree { .. }
            | AccessCommand::GroupMembers
            | AccessCommand::RightsShow { .. } => "AccessTree",
            AccessCommand::Chmod { .. } => "Chmod",
            AccessCommand::Chown { .. } => "Chown",
            AccessCommand::Chgrp { .. } => "Chgrp",
        }
    }

    /// Only the three rights mutations change server-side state, so only
    /// they need a confirmation.
    pub(in crate::shell) fn wants_confirm(&self) -> bool {
        matches!(
            self,
            AccessCommand::Chmod { .. } | AccessCommand::Chown { .. } | AccessCommand::Chgrp { .. }
        )
    }

    /// Every command here runs through the batch path, so all of them carry
    /// the profile's routing database floor.
    pub(in crate::shell) fn requires_routing_db(&self) -> bool {
        true
    }

    /// Pre-connection validation: the `rights show` selector and the
    /// mutation path selector are parsed HERE, before any dial or secret
    /// read, as are MODE and the owner / group token. A malformed selector
    /// is the fixed typed `Usage` refusal; the resource root is
    /// `RootMutation`; a bad MODE is `InvalidMode`.
    pub(in crate::shell) fn validate(&self) -> Result<(), ShellError> {
        match self {
            AccessCommand::Users
            | AccessCommand::GroupTree { .. }
            | AccessCommand::GroupMembers => Ok(()),
            AccessCommand::RightsShow { path } => parse_selector(path).map(|_| ()),
            AccessCommand::Chmod { path, mode } => {
                mutate::validate(&Mutation::Chmod { path, mode })
            }
            AccessCommand::Chown { path, owner } => {
                mutate::validate(&Mutation::Chown { path, owner })
            }
            AccessCommand::Chgrp { path, group } => {
                mutate::validate(&Mutation::Chgrp { path, group })
            }
        }
    }

    /// Execute the command on the routing database `routing_db`.
    /// Authorization happens exclusively on the normal remote route; this
    /// code sends no privilege material.
    /// cancel-safe: NO — a sent batch may still execute; do not retry on
    /// timeout.
    pub(in crate::shell) async fn execute(
        &self,
        client: &SessionClient,
        routing_db: Option<&str>,
    ) -> Result<Value, ShellError> {
        let started = tokio::time::Instant::now();
        let db = routing_db.ok_or(ShellError::Usage)?;
        // Every arm yields a `Result` instead of propagating with `?`: the
        // deadline must be consulted AFTER the projection and the output
        // check, so an elapsed budget wins over either refusal (the
        // `data head` rule, `state.rs::head_tail`).
        let payload: Result<Value, ShellError> = match self {
            AccessCommand::Users => {
                async {
                    let result = run_users(client, db).await?;
                    project_users(&result)
                }
                .await
            }
            AccessCommand::GroupTree { db: filter } => {
                async {
                    let result = run_tree(client, db, filter.as_deref()).await?;
                    let qv = tree_value(single_direct_value(&result)?)?;
                    project_tree(qv, filter.as_deref())
                }
                .await
            }
            AccessCommand::GroupMembers => {
                async {
                    let result = run_tree(client, db, None).await?;
                    let qv = tree_value(single_direct_value(&result)?)?;
                    project_members(qv)
                }
                .await
            }
            AccessCommand::RightsShow { path } => {
                async {
                    let selector = parse_selector(path)?;
                    let result = run_tree(client, db, None).await?;
                    let qv = tree_value(single_direct_value(&result)?)?;
                    project_rights(qv, &selector)
                }
                .await
            }
            AccessCommand::Chmod { path, mode } => {
                async { mutate::execute(client, db, &Mutation::Chmod { path, mode }).await }.await
            }
            AccessCommand::Chown { path, owner } => {
                async { mutate::execute(client, db, &Mutation::Chown { path, owner }).await }.await
            }
            AccessCommand::Chgrp { path, group } => {
                async { mutate::execute(client, db, &Mutation::Chgrp { path, group }).await }.await
            }
        };
        output::finish_command(started, payload, |payload| {
            client.check_catalogue_output(payload)
        })
    }
}

/// Send the one-op `user list` batch on `db`.
async fn run_users(client: &SessionClient, db: &str) -> Result<QueryResult, ShellError> {
    let mut batch = Batch::new();
    batch.op(USER_ALIAS, list_users());
    // Client-side validation failure is a local usage problem.
    let request = batch.try_build().map_err(|_| ShellError::Usage)?;
    let response = client.execute(db, request).await.map_err(request_error)?;
    take_result(response, USER_ALIAS)
}

/// Send the one-op `AccessTree` batch on `db`, restricted to `filter` when
/// it is `Some`.
async fn run_tree(
    client: &SessionClient,
    db: &str,
    filter: Option<&str>,
) -> Result<QueryResult, ShellError> {
    let mut op = access_tree();
    if let Some(database) = filter {
        op = op.db(database.to_string());
    }
    let mut batch = Batch::new();
    batch.op(TREE_ALIAS, op);
    let request = batch.try_build().map_err(|_| ShellError::Usage)?;
    let response = client.execute(db, request).await.map_err(request_error)?;
    take_result(response, TREE_ALIAS)
}

/// Extract the one-alias batch result as an owned, unprojected
/// [`QueryResult`]. A missing or skipped alias is a protocol mismatch,
/// reported as `Server` without echoing peer-supplied detail.
fn take_result(response: BatchResponse, alias: &str) -> Result<QueryResult, ShellError> {
    let mut results = response.results;
    let result = results.get_mut(alias).ok_or(ShellError::Server)?;
    if result.skipped {
        return Err(ShellError::Server);
    }
    Ok(std::mem::take(result))
}

/// The single direct record of a one-alias batch, as a typed `QueryValue`.
fn single_direct_value(result: &QueryResult) -> Result<&QueryValue, ShellError> {
    let [record] = result.records.as_slice() else {
        return Err(ShellError::Server);
    };
    match record {
        QueryRecord::Direct(qv) => Ok(qv),
        _ => Err(ShellError::Server),
    }
}

/// Project one non-negative id / counter leaf (`Int` at or above zero) as a
/// decimal string.
fn decimal(item: &QueryValue) -> Result<String, ShellError> {
    match item {
        QueryValue::Int(number) if *number >= 0 => Ok(number.to_string()),
        _ => Err(ShellError::Server),
    }
}

/// Project one required string leaf.
fn text(item: &QueryValue) -> Result<Value, ShellError> {
    match item {
        QueryValue::Str(_) => value(item).map_err(|_| ShellError::Server),
        _ => Err(ShellError::Server),
    }
}

/// Project one optional string leaf (`Null` or a sanitized string).
fn optional_text(item: &QueryValue) -> Result<Value, ShellError> {
    match item {
        QueryValue::Null => Ok(Value::Null),
        QueryValue::Str(_) => value(item).map_err(|_| ShellError::Server),
        _ => Err(ShellError::Server),
    }
}

/// Project one optional non-negative id leaf (`Null` or a decimal string).
fn optional_decimal(item: &QueryValue) -> Result<Value, ShellError> {
    match item {
        QueryValue::Null => Ok(Value::Null),
        _ => decimal(item).map(Value::String),
    }
}

/// STRICT projection of `user list`: exactly one direct map with exactly the
/// `users` list; each entry must be a map with exactly `name`, `principal64`,
/// `superuser` and `database`, rendered as
/// `{"name":<sanitized>,"principal64":<decimal>,"superuser":<bool>,"database":<sanitized|null>}`.
fn project_users(result: &QueryResult) -> Result<Value, ShellError> {
    let qv = single_direct_value(result)?;
    let QueryValue::Map(fields) = qv else {
        return Err(ShellError::Server);
    };
    if fields.len() != 1 {
        return Err(ShellError::Server);
    }
    let QueryValue::List(entries) = fields.get("users").ok_or(ShellError::Server)? else {
        return Err(ShellError::Server);
    };
    let mut users = Vec::with_capacity(entries.len());
    for entry in entries {
        let QueryValue::Map(entry_fields) = entry else {
            return Err(ShellError::Server);
        };
        if entry_fields.len() != 4 {
            return Err(ShellError::Server);
        }
        let name = text(entry_fields.get("name").ok_or(ShellError::Server)?)?;
        let principal64 = decimal(entry_fields.get("principal64").ok_or(ShellError::Server)?)?;
        let superuser = match entry_fields.get("superuser").ok_or(ShellError::Server)? {
            QueryValue::Bool(flag) => *flag,
            _ => return Err(ShellError::Server),
        };
        let database = optional_text(entry_fields.get("database").ok_or(ShellError::Server)?)?;
        users.push(json!({
            "name": name,
            "principal64": principal64,
            "superuser": superuser,
            "database": database,
        }));
    }
    Ok(json!({ "users": users }))
}

/// One field of a projected resource node.
fn field<'a>(node: &'a QueryValue, key: &str) -> Result<&'a QueryValue, ShellError> {
    match node {
        QueryValue::Map(fields) => fields.get(key).ok_or(ShellError::Server),
        _ => Err(ShellError::Server),
    }
}

/// Project the LEAVES of one access-tree node, without its `children`: every
/// resource node carries `name`, `kind`, `owner`, `owner_name`, `group`,
/// `group_name`, `mode` and `setuid`, and a function node also carries its
/// `builtin` flag (the engine sends it; dropping it would hide the difference
/// between a built-in and a stored function). Any other key is a shape
/// mismatch and is refused, never echoed.
fn node_fields(node: &QueryValue) -> Result<serde_json::Map<String, Value>, ShellError> {
    let QueryValue::Map(fields) = node else {
        return Err(ShellError::Server);
    };
    for key in fields.keys() {
        match key.as_str() {
            "name" | "kind" | "owner" | "owner_name" | "group" | "group_name" | "mode"
            | "setuid" | "children" | "builtin" => {}
            _ => return Err(ShellError::Server),
        }
    }
    // The node's own `children` must be a list when present, also for a node
    // projected WITHOUT its descendants (`rights show`): a malformed field is
    // a shape mismatch, never silently ignored.
    if let Some(children) = fields.get("children") {
        if !matches!(children, QueryValue::List(_)) {
            return Err(ShellError::Server);
        }
    }
    let name = text(field(node, "name")?)?;
    let kind = text(field(node, "kind")?)?;
    let owner = decimal(field(node, "owner")?)?;
    let owner_name = optional_text(field(node, "owner_name")?)?;
    let group = optional_decimal(field(node, "group")?)?;
    let group_name = optional_text(field(node, "group_name")?)?;
    let mode = match field(node, "mode")? {
        QueryValue::Int(number) if *number >= 0 => format!("{number:04o}"),
        _ => return Err(ShellError::Server),
    };
    let setuid = match field(node, "setuid")? {
        QueryValue::Bool(flag) => *flag,
        _ => return Err(ShellError::Server),
    };
    let mut projected = serde_json::Map::new();
    projected.insert(String::from("name"), name);
    projected.insert(String::from("kind"), kind);
    projected.insert(String::from("owner"), Value::String(owner));
    projected.insert(String::from("owner_name"), owner_name);
    projected.insert(String::from("group"), group);
    projected.insert(String::from("group_name"), group_name);
    projected.insert(String::from("mode"), Value::String(mode));
    projected.insert(String::from("setuid"), Value::Bool(setuid));
    if let Some(item) = fields.get("builtin") {
        let QueryValue::Bool(builtin) = item else {
            return Err(ShellError::Server);
        };
        projected.insert(String::from("builtin"), Value::Bool(*builtin));
    }
    Ok(projected)
}

/// Project one access-tree node (and, recursively, its children). `level` is
/// the node's depth with the root at 1; deeper than [`MAX_NODE_LEVELS`] or
/// more than [`MAX_NODES`] nodes in total is the typed
/// [`ShellError::TreeTooLarge`] refusal, whose fixed message names the limit
/// and the `--db` remedy. `children` may be absent (function nodes omit it)
/// and is then projected as an empty list. Any other key is a shape mismatch
/// and is refused, never echoed.
fn project_node(node: &QueryValue, level: usize, nodes: &mut usize) -> Result<Value, ShellError> {
    if level > MAX_NODE_LEVELS {
        return Err(ShellError::TreeTooLarge);
    }
    *nodes = nodes.checked_add(1).ok_or(ShellError::TreeTooLarge)?;
    if *nodes > MAX_NODES {
        return Err(ShellError::TreeTooLarge);
    }
    let mut projected = node_fields(node)?;
    let mut children = Vec::new();
    for child in raw_children(node)? {
        children.push(project_node(child, level + 1, nodes)?);
    }
    projected.insert(String::from("children"), Value::Array(children));
    Ok(Value::Object(projected))
}

/// The tree map of an `AccessTree` result. The engine wraps the tree under
/// the single `access_tree` key of the direct record; a direct tree map is
/// also accepted. Anything else is the typed `Server` refusal.
fn tree_value(qv: &QueryValue) -> Result<&QueryValue, ShellError> {
    if let QueryValue::Map(fields) = qv {
        if fields.len() == 1 {
            if let Some(wrapped) = fields.get("access_tree") {
                return match wrapped {
                    QueryValue::Map(_) => Ok(wrapped),
                    _ => Err(ShellError::Server),
                };
            }
        }
    }
    Ok(qv)
}

/// The `resources`, `functions` and `principals` members of a tree map.
fn tree_parts(qv: &QueryValue) -> Result<(&QueryValue, &QueryValue, &QueryValue), ShellError> {
    let QueryValue::Map(fields) = qv else {
        return Err(ShellError::Server);
    };
    Ok((
        fields.get("resources").ok_or(ShellError::Server)?,
        fields.get("functions").ok_or(ShellError::Server)?,
        fields.get("principals").ok_or(ShellError::Server)?,
    ))
}

/// Project every entry of a list as a resource node, sharing one node budget.
fn project_node_list(items: &QueryValue, nodes: &mut usize) -> Result<Value, ShellError> {
    let QueryValue::List(entries) = items else {
        return Err(ShellError::Server);
    };
    let mut projected = Vec::with_capacity(entries.len());
    for entry in entries {
        projected.push(project_node(entry, 1, nodes)?);
    }
    Ok(Value::Array(projected))
}

/// STRICT projection of one principal entry (`{"id","name"}`).
fn project_principal(entry: &QueryValue) -> Result<Value, ShellError> {
    let QueryValue::Map(fields) = entry else {
        return Err(ShellError::Server);
    };
    if fields.len() != 2 {
        return Err(ShellError::Server);
    }
    Ok(json!({
        "id": decimal(fields.get("id").ok_or(ShellError::Server)?)?,
        "name": text(fields.get("name").ok_or(ShellError::Server)?)?,
    }))
}

/// STRICT projection of one group entry with its members
/// (`{"id","name","members":[{"id","name"}]}`); a member name may be `Null`.
fn project_group(entry: &QueryValue) -> Result<Value, ShellError> {
    let QueryValue::Map(fields) = entry else {
        return Err(ShellError::Server);
    };
    if fields.len() != 3 {
        return Err(ShellError::Server);
    }
    let QueryValue::List(members) = fields.get("members").ok_or(ShellError::Server)? else {
        return Err(ShellError::Server);
    };
    let mut projected = Vec::with_capacity(members.len());
    for member in members {
        let QueryValue::Map(member_fields) = member else {
            return Err(ShellError::Server);
        };
        if member_fields.len() != 2 {
            return Err(ShellError::Server);
        }
        projected.push(json!({
            "id": decimal(member_fields.get("id").ok_or(ShellError::Server)?)?,
            "name": optional_text(member_fields.get("name").ok_or(ShellError::Server)?)?,
        }));
    }
    Ok(json!({
        "id": decimal(fields.get("id").ok_or(ShellError::Server)?)?,
        "name": text(fields.get("name").ok_or(ShellError::Server)?)?,
        "members": projected,
    }))
}

/// The two principal lists of a tree map, shape-checked as lists.
fn principal_lists(qv: &QueryValue) -> Result<(&QueryValue, &QueryValue), ShellError> {
    let (_, _, principals) = tree_parts(qv)?;
    let QueryValue::Map(fields) = principals else {
        return Err(ShellError::Server);
    };
    if fields.len() != 2 {
        return Err(ShellError::Server);
    }
    Ok((
        fields.get("users").ok_or(ShellError::Server)?,
        fields.get("groups").ok_or(ShellError::Server)?,
    ))
}

/// Project every entry of a list through `project`.
fn project_list(
    items: &QueryValue,
    project: impl Fn(&QueryValue) -> Result<Value, ShellError>,
) -> Result<Value, ShellError> {
    let QueryValue::List(entries) = items else {
        return Err(ShellError::Server);
    };
    let mut projected = Vec::with_capacity(entries.len());
    for entry in entries {
        projected.push(project(entry)?);
    }
    Ok(Value::Array(projected))
}

/// Project the full `group tree` result: `database_filter`, the resource
/// root, the function nodes and both principal lists.
fn project_tree(qv: &QueryValue, filter: Option<&str>) -> Result<Value, ShellError> {
    let (resources, functions, _) = tree_parts(qv)?;
    let (users, groups) = principal_lists(qv)?;
    let mut nodes = 0usize;
    let root = project_node(resources, 1, &mut nodes)?;
    let functions = project_node_list(functions, &mut nodes)?;
    let users = project_list(users, project_principal)?;
    let groups = project_list(groups, project_group)?;
    Ok(json!({
        "database_filter": filter.map(output::sanitize),
        "resources": root,
        "functions": functions,
        "principals": {
            "users": users,
            "groups": groups,
        },
    }))
}

/// Project the `group members` result: ONLY the group list.
fn project_members(qv: &QueryValue) -> Result<Value, ShellError> {
    let (_, groups) = principal_lists(qv)?;
    Ok(json!({ "groups": project_list(groups, project_group)? }))
}

/// The `name` leaf of one raw tree node, as a string.
fn raw_name(node: &QueryValue) -> Option<&str> {
    match node {
        QueryValue::Map(fields) => match fields.get("name") {
            Some(QueryValue::Str(text)) => Some(text),
            _ => None,
        },
        _ => None,
    }
}

/// The `kind` leaf of one raw tree node, as a string.
fn raw_kind(node: &QueryValue) -> Option<&str> {
    match node {
        QueryValue::Map(fields) => match fields.get("kind") {
            Some(QueryValue::Str(text)) => Some(text),
            _ => None,
        },
        _ => None,
    }
}

/// The children of one raw tree node: absent → no children; a present
/// `children` that is not a list is a shape mismatch (`Server`), never a
/// silently empty subtree.
fn raw_children(node: &QueryValue) -> Result<&[QueryValue], ShellError> {
    match node {
        QueryValue::Map(fields) => match fields.get("children") {
            Some(QueryValue::List(items)) => Ok(items.as_slice()),
            Some(_) => Err(ShellError::Server),
            None => Ok(&[]),
        },
        _ => Ok(&[]),
    }
}

/// Select the node named by `selector` from the authorized tree. A selector
/// that matches no node is `ShellError::NotFound`.
fn select_node<'a>(qv: &'a QueryValue, selector: &Selector) -> Result<&'a QueryValue, ShellError> {
    let (resources, functions, _) = tree_parts(qv)?;
    match selector {
        Selector::Root => match resources {
            QueryValue::Map(_) => Ok(resources),
            _ => Err(ShellError::Server),
        },
        Selector::Database(name) => {
            if !matches!(resources, QueryValue::Map(_)) {
                return Err(ShellError::Server);
            }
            raw_children(resources)?
                .iter()
                .find(|child| {
                    raw_kind(child) == Some(RESOURCE_KINDS[0])
                        && raw_name(child) == Some(name.as_str())
                })
                .ok_or(ShellError::NotFound)
        }
        Selector::Function(name) => {
            let QueryValue::List(entries) = functions else {
                return Err(ShellError::Server);
            };
            entries
                .iter()
                .find(|entry| raw_name(entry) == Some(name.as_str()))
                .ok_or(ShellError::NotFound)
        }
        Selector::Resource(segments) => {
            let mut current = match resources {
                QueryValue::Map(_) => resources,
                _ => return Err(ShellError::Server),
            };
            for (depth, segment) in segments.iter().enumerate() {
                let kind = RESOURCE_KINDS[depth];
                current = raw_children(current)?
                    .iter()
                    .find(|child| {
                        raw_kind(child) == Some(kind) && raw_name(child) == Some(segment.as_str())
                    })
                    .ok_or(ShellError::NotFound)?;
            }
            Ok(current)
        }
    }
}

/// Project the `rights show` result: the SELECTED node's own leaves (its
/// descendants are never needed, so they are never projected, never counted
/// and never able to trip the whole-tree cap) plus the sanitized normalized
/// path. Mutant: projecting the selected node through [`project_node`], which
/// walks the subtree and refuses a healthy large catalogue.
fn project_rights(qv: &QueryValue, selector: &Selector) -> Result<Value, ShellError> {
    let node = select_node(qv, selector)?;
    let mut out = serde_json::Map::new();
    out.insert(
        String::from("path"),
        Value::String(output::sanitize(&normalized_path(selector))),
    );
    out.extend(node_fields(node)?);
    Ok(Value::Object(out))
}

#[cfg(test)]
#[path = "../../tests/commands/access/access_deadline_tests.rs"]
mod access_deadline_tests;

#[cfg(test)]
#[path = "../../tests/commands/access/access_projection_tests.rs"]
mod access_projection_tests;

//! The three rights mutations — `chmod`, `chown` and `chgrp` — as ONE
//! coherent part, split out of `access.rs`.
//!
//! Each mutation is described by a [`Mutation`]; [`validate`] runs the
//! pre-connection checks (selector, resource root, MODE, owner / group token)
//! and [`execute`] sends the typed `Client::chmod` / `chown` / `chgrp` call.
//! Everything below the two entry points is private to this module: the
//! selector helpers, the token limits and the principal resolvers are reached
//! ONLY through them.
//!
//! Behaviour is unchanged from `access.rs`: a malformed selector is the fixed
//! typed `Usage` refusal, the resource root is `RootMutation`, a bad MODE is
//! `InvalidMode`, an unacceptable owner / group token is `Usage`, an unknown
//! name is `UnknownPrincipal`, and a response whose shape does not match the
//! engine's own record is `Server`. Every path, owner and group string is
//! sanitized; the server decides authorization and the shell sends no
//! privilege material.

use serde_json::{json, Value};
use shamir_query_types::admin::ResourceRef;
use shamir_query_types::read::QueryResult;
use shamir_types::types::value::QueryValue;

use super::super::super::output::{self, ShellError};
use super::super::selector::{normalized_path, parse_selector, Selector};
use super::{
    field, principal_lists, raw_name, run_tree, run_users, single_direct_value, tree_value,
};
use crate::shell::connect::SessionClient;

/// Longest accepted owner / group NAME token, in bytes.
const MAX_PRINCIPAL_BYTES: usize = 256;

/// One pending rights mutation, borrowed from the parsed command.
pub(super) enum Mutation<'a> {
    /// `chmod PATH MODE` — change the mode of one securable resource.
    Chmod { path: &'a str, mode: &'a str },
    /// `chown PATH OWNER` — change the owner of one securable resource.
    Chown { path: &'a str, owner: &'a str },
    /// `chgrp PATH GROUP` — change the group of one securable resource.
    Chgrp { path: &'a str, group: &'a str },
}

/// Pre-connection validation of one mutation: the path selector and the
/// mutation resource are resolved HERE, before any dial or secret read, as
/// are MODE and the owner / group token.
pub(super) fn validate(mutation: &Mutation<'_>) -> Result<(), ShellError> {
    match mutation {
        Mutation::Chmod { path, mode } => {
            let selector = parse_selector(path)?;
            mutation_resource(&selector)?;
            parse_mode(mode).map(|_| ())
        }
        Mutation::Chown { path, owner } => {
            let selector = parse_selector(path)?;
            mutation_resource(&selector)?;
            valid_principal(owner)
                .then_some(())
                .ok_or(ShellError::Usage)
        }
        Mutation::Chgrp { path, group } => {
            let selector = parse_selector(path)?;
            mutation_resource(&selector)?;
            valid_principal(group)
                .then_some(())
                .ok_or(ShellError::Usage)
        }
    }
}

/// Execute one mutation on the routing database `db`. Each arm yields a
/// `Result` instead of propagating with `?`, so the caller's deadline policy
/// is unchanged.
pub(super) async fn execute(
    client: &SessionClient,
    db: &str,
    mutation: &Mutation<'_>,
) -> Result<Value, ShellError> {
    match mutation {
        Mutation::Chmod { path, mode } => {
            let selector = parse_selector(path)?;
            let resource = mutation_resource(&selector)?;
            let mode = parse_mode(mode)?;
            client.chmod(db, resource, mode).await?;
            Ok(json!({
                "path": output::sanitize(&normalized_path(&selector)),
                "mode": format!("{mode:04o}"),
            }))
        }
        Mutation::Chown { path, owner } => {
            let selector = parse_selector(path)?;
            let resource = mutation_resource(&selector)?;
            let users = run_users(client, db).await?;
            let owner_id = user_id(&users, owner)?;
            client.chown(db, resource, owner_id).await?;
            Ok(json!({
                "path": output::sanitize(&normalized_path(&selector)),
                "owner": output::sanitize(owner),
                "owner_id": owner_id.to_string(),
            }))
        }
        Mutation::Chgrp { path, group } => {
            let selector = parse_selector(path)?;
            let resource = mutation_resource(&selector)?;
            let result = run_tree(client, db, None).await?;
            let qv = tree_value(single_direct_value(&result)?)?;
            let group_id = group_id(qv, group)?;
            client.chgrp(db, resource, Some(group_id)).await?;
            Ok(json!({
                "path": output::sanitize(&normalized_path(&selector)),
                "group": output::sanitize(group),
                "group_id": group_id.to_string(),
            }))
        }
    }
}

/// The `ResourceRef` addressed by an already-parsed mutation selector. The
/// root is the fixed `RootMutation` refusal; a function is `Function`; one,
/// two or three segments are `Database`, `Store` and `Table`. Any other
/// selector shape is the fixed `Usage` refusal.
fn mutation_resource(selector: &Selector) -> Result<ResourceRef, ShellError> {
    match selector {
        Selector::Root => Err(ShellError::RootMutation),
        Selector::Database(database) => Ok(ResourceRef::Database {
            database: database.clone(),
        }),
        Selector::Function(function) => Ok(ResourceRef::Function {
            function: function.clone(),
        }),
        Selector::Resource(segments) => match segments.as_slice() {
            [database] => Ok(ResourceRef::Database {
                database: database.clone(),
            }),
            [database, store] => Ok(ResourceRef::Store {
                store: [database.clone(), store.clone()],
            }),
            [database, store, table] => Ok(ResourceRef::Table {
                table: [database.clone(), store.clone(), table.clone()],
            }),
            _ => Err(ShellError::Usage),
        },
    }
}

/// Parse one `chmod` MODE strictly: one to four ASCII octal digits, value at
/// most `0o7777`. Anything else is the fixed `InvalidMode` refusal.
fn parse_mode(text: &str) -> Result<u16, ShellError> {
    let digits = text.as_bytes();
    if digits.is_empty() || digits.len() > 4 {
        return Err(ShellError::InvalidMode);
    }
    if !digits.iter().all(|byte| (b'0'..=b'7').contains(byte)) {
        return Err(ShellError::InvalidMode);
    }
    let mode = u16::from_str_radix(text, 8).map_err(|_| ShellError::InvalidMode)?;
    if mode > 0o7777 {
        return Err(ShellError::InvalidMode);
    }
    Ok(mode)
}

/// Whether one owner / group NAME token is acceptable: non-empty, at most
/// [`MAX_PRINCIPAL_BYTES`] bytes, and free of control bytes.
fn valid_principal(text: &str) -> bool {
    !text.is_empty()
        && text.len() <= MAX_PRINCIPAL_BYTES
        && !text.bytes().any(|byte| byte < 0x20 || byte == 0x7f)
}

/// Resolve one exact principal NAME to its numeric `principal64` id from the
/// `user list` result. An unknown name is `UnknownPrincipal`; a malformed
/// result is the typed `Server` refusal.
fn user_id(result: &QueryResult, name: &str) -> Result<u64, ShellError> {
    let QueryValue::Map(fields) = single_direct_value(result)? else {
        return Err(ShellError::Server);
    };
    if fields.len() != 1 {
        return Err(ShellError::Server);
    }
    let QueryValue::List(entries) = fields.get("users").ok_or(ShellError::Server)? else {
        return Err(ShellError::Server);
    };
    for entry in entries {
        if raw_name(entry) != Some(name) {
            continue;
        }
        return match field(entry, "principal64")? {
            QueryValue::Int(id) if *id >= 0 => u64::try_from(*id).map_err(|_| ShellError::Server),
            _ => Err(ShellError::Server),
        };
    }
    Err(ShellError::UnknownPrincipal)
}

/// Resolve one exact group NAME to its numeric id from `principals.groups`
/// of an access-tree result. An unknown name is `UnknownPrincipal`; a
/// malformed result is the typed `Server` refusal.
fn group_id(qv: &QueryValue, name: &str) -> Result<u64, ShellError> {
    let (_, groups) = principal_lists(qv)?;
    let QueryValue::List(entries) = groups else {
        return Err(ShellError::Server);
    };
    for entry in entries {
        if raw_name(entry) != Some(name) {
            continue;
        }
        return match field(entry, "id")? {
            QueryValue::Int(id) if *id >= 0 => u64::try_from(*id).map_err(|_| ShellError::Server),
            _ => Err(ShellError::Server),
        };
    }
    Err(ShellError::UnknownPrincipal)
}

#[cfg(test)]
#[path = "../../tests/commands/access/access_mutation_tests.rs"]
mod access_mutation_tests;

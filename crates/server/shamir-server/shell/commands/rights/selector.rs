//! The rights path selector used by `rights show` and the three rights
//! mutations (`design admin-shell.md` §4.3). It is parsed LOCALLY before any
//! dial or secret read, and the path is never sent as a `ResourceRef`: the
//! access tree is fetched and the addressed node is selected on the client.
//!
//! Accepted forms:
//! - `root` or `/` — the synthetic resource root (never mutable);
//! - `db:NAME` — ALWAYS the database `NAME`, which is how a database that is
//!   itself called `root` is addressed (the bare `root` keeps meaning the
//!   synthetic root);
//! - `function:NAME` — a stored function by exact name. `NAME` may contain
//!   `/` because a stored function name is a folder path (`math/abs` is ONE
//!   function, not a folder entity);
//! - `DATABASE[/STORE[/TABLE]]` — one to three resource components.
//!
//! The name rules are MIRRORS of engine rules the shell cannot call without
//! a new dependency, so each one names its source:
//! - database and store components mirror `validate_name_component`
//!   (`crates/engine/shamir-db/src/shamir_db/execute/core/helpers.rs`), which
//!   the engine applies to database/repository names;
//! - a stored FUNCTION name is NOT validated by the engine at all (it is
//!   persisted verbatim,
//!   `crates/engine/shamir-db/src/shamir_db/shamir_db/db/function/management.rs`):
//!   the shell refuses only what it cannot address (empty, control bytes,
//!   past a generous length bound);
//! - the table component mirrors the loosest PHYSICAL store-name length rule
//!   (`crates/engine/shamir-engine-table/src/support/model/names.rs`,
//!   `validate_table_store_name`): it refuses only names no table can have.
//!
//! The server stays the authority for everything the shell does not mirror.

use super::super::output::ShellError;

/// The resource kinds of the three supported resource-path depths.
pub(in crate::shell) const RESOURCE_KINDS: [&str; 3] = ["database", "store", "table"];

/// Longest accepted stored FUNCTION name, in bytes.
///
/// The engine imposes NO length cap and no charset rule on a function name: it
/// persists the registered name as given (`create_function_with_opts_as`,
/// `crates/engine/shamir-db/src/shamir_db/shamir_db/db/function/management.rs`),
/// with no folder or segment check on the name itself. 1024 bytes is therefore
/// a shell-side sanity bound for a path-like name, not a mirrored engine rule.
pub(in crate::shell) const MAX_FUNCTION_NAME_BYTES: usize = 1024;

/// Longest accepted TABLE component, in bytes: the LOOSEST bound that still
/// refuses only impossible names.
///
/// Derived from the physical store names the engine generates for a table
/// (`crates/engine/shamir-engine-table/src/support/model/names.rs`,
/// `table_store_names`, checked by `validate_table_store_name` against fjall's
/// 255-byte store limit): `255 - "__history__" (11)` = **244**, the case of a
/// legacy table whose store name carries no `--{id}` suffix. A table created
/// today also carries the suffix (two bytes of `--` plus the encoded id), so
/// the engine's real maximum is lower and depends on the id length: the
/// server stays the authority for names between the guaranteed-fit length and
/// this bound. A longer component can never exist, so the shell refuses it
/// instead of signing a request for an impossible target.
///
/// This is a LOCAL shell-side FEASIBILITY gate on a Fjall physical-store
/// limit, NOT parity with an engine refusal: a 245-byte component is refused
/// here as [`ShellError::Usage`], while the engine would answer with its own
/// typed validation error (the shell cannot know a name between the
/// guaranteed-fit length and 244).
///
/// An ID-less embedded in-memory table is not subject to the fjall store-name
/// check at all (`crates/storage/shamir-storage/src/stores/storage_in_memory.rs`,
/// memory backend), so this ceiling is conservative for it too.
///
/// [`MAX_FUNCTION_NAME_BYTES`] (1024) is deliberately independent of this
/// rule and stays: the engine persists function names verbatim with no
/// physical-name rule (`create_function_with_opts_as`); do not merge the two.
pub(in crate::shell) const MAX_TABLE_NAME_BYTES: usize = 244;

/// The target a `rights show` / mutation path selector resolved to.
pub(in crate::shell) enum Selector {
    /// The resource root (`root` or `/`).
    Root,
    /// The database `NAME` of the explicit `db:NAME` form.
    Database(String),
    /// One entry of the `functions` list, selected by exact name.
    Function(String),
    /// One `database` / `store` / `table` path of one, two or three segments.
    Resource(Vec<String>),
}

/// Whether one DATABASE or STORE name component passes the engine's own name
/// rule. MIRRORS `validate_name_component`
/// (`crates/engine/shamir-db/src/shamir_db/execute/core/helpers.rs`), which is
/// `pub(super)` inside `shamir-db` and therefore not reachable from this
/// crate: only `[A-Za-z0-9_-]`, never empty, never `.`/`..`. That rule has NO
/// length cap, so neither does this mirror — a length ceiling here would hide
/// databases the engine accepts.
fn valid_name_component(text: &str) -> bool {
    !text.is_empty()
        && text != "."
        && text != ".."
        && text
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
}

/// Whether one TABLE component can exist as a physical store name
/// ([`MAX_TABLE_NAME_BYTES`]).
fn valid_table_component(text: &str) -> bool {
    !text.is_empty() && text.len() <= MAX_TABLE_NAME_BYTES
}

/// Parse a `function:NAME` payload. The engine stores a function name verbatim
/// (a path-like `math/abs` is ONE name; `:`, `.` and empty path segments are
/// all legal), so the shell refuses only what it cannot represent: an empty
/// name, a control byte, or a name past [`MAX_FUNCTION_NAME_BYTES`].
fn parse_function_name(name: &str) -> Result<(), ShellError> {
    if name.is_empty()
        || name.len() > MAX_FUNCTION_NAME_BYTES
        || name.bytes().any(|byte| byte < 0x20 || byte == 0x7f)
    {
        return Err(ShellError::Usage);
    }
    Ok(())
}

/// Parse the `rights show` / mutation path selector. `root` and `/` name the
/// resource root; `db:NAME` names a database explicitly; `function:NAME` names
/// a stored function by exact name; otherwise the path is split on `/` into
/// exactly one, two or three non-empty segments selecting a `database`,
/// `store` or `table` node. A component the mirrored engine rule rejects — or
/// a segment count the selector does not support — is `ShellError::Usage`
/// before any request.
pub(in crate::shell) fn parse_selector(path: &str) -> Result<Selector, ShellError> {
    if path == "root" || path == "/" {
        return Ok(Selector::Root);
    }
    if let Some(name) = path.strip_prefix("db:") {
        if !valid_name_component(name) {
            return Err(ShellError::Usage);
        }
        return Ok(Selector::Database(name.to_string()));
    }
    if let Some(name) = path.strip_prefix("function:") {
        parse_function_name(name)?;
        return Ok(Selector::Function(name.to_string()));
    }
    let segments: Vec<&str> = path.split('/').collect();
    if segments.is_empty() || segments.len() > RESOURCE_KINDS.len() {
        return Err(ShellError::Usage);
    }
    for (depth, segment) in segments.iter().enumerate() {
        let accepted = match depth {
            // The database and store components must satisfy the engine's own
            // name rule before they can become a wire target.
            0 | 1 => valid_name_component(segment),
            // The table component must fit the physical store name the engine
            // generates; the server stays the authority for its content.
            _ => valid_table_component(segment),
        };
        if !accepted {
            return Err(ShellError::Usage);
        }
    }
    Ok(Selector::Resource(
        segments.into_iter().map(str::to_string).collect(),
    ))
}

/// The normalized `path` output of a selector: the form the operator used for
/// the two explicit prefixes, and the slash-joined components otherwise.
pub(in crate::shell) fn normalized_path(selector: &Selector) -> String {
    match selector {
        Selector::Root => "/".to_string(),
        Selector::Database(name) => format!("db:{name}"),
        Selector::Function(name) => format!("function:{name}"),
        Selector::Resource(segments) => segments.join("/"),
    }
}

#[cfg(test)]
#[path = "../../tests/commands/selector_tests.rs"]
mod selector_tests;

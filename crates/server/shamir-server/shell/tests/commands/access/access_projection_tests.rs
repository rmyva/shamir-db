//! Projection seams of the rights commands: `rights show` must inspect ONE
//! node (a healthy large catalogue must not trip the whole-tree cap), and the
//! whole-tree commands must keep it.
//!
//! The typed mutation target is covered by `access_mutation_tests.rs`, which
//! is declared from the `mutate` module that owns it.

use shamir_types::types::common::new_map;
use shamir_types::types::value::QueryValue;

use super::{project_rights, project_tree, Selector};
use crate::shell::output::ShellError;

/// One raw access-tree node in the engine's own shape.
fn node(name: &str, kind: &str, children: Vec<QueryValue>) -> QueryValue {
    let mut map = new_map();
    map.insert(String::from("name"), QueryValue::Str(name.to_string()));
    map.insert(String::from("kind"), QueryValue::Str(kind.to_string()));
    map.insert(String::from("owner"), QueryValue::Int(1));
    map.insert(
        String::from("owner_name"),
        QueryValue::Str(String::from("alice")),
    );
    map.insert(String::from("group"), QueryValue::Null);
    map.insert(String::from("group_name"), QueryValue::Null);
    map.insert(String::from("mode"), QueryValue::Int(0o750));
    map.insert(String::from("setuid"), QueryValue::Bool(false));
    map.insert(String::from("children"), QueryValue::List(children));
    QueryValue::Map(map)
}

/// One function node without `children`, optionally carrying `builtin`.
fn function_node(name: &str, builtin: Option<bool>) -> QueryValue {
    let mut map = new_map();
    map.insert(String::from("name"), QueryValue::Str(name.to_string()));
    map.insert(
        String::from("kind"),
        QueryValue::Str(String::from("function")),
    );
    map.insert(String::from("owner"), QueryValue::Int(1));
    map.insert(String::from("owner_name"), QueryValue::Null);
    map.insert(String::from("group"), QueryValue::Null);
    map.insert(String::from("group_name"), QueryValue::Null);
    map.insert(String::from("mode"), QueryValue::Int(0o700));
    map.insert(String::from("setuid"), QueryValue::Bool(false));
    if let Some(flag) = builtin {
        map.insert(String::from("builtin"), QueryValue::Bool(flag));
    }
    QueryValue::Map(map)
}

/// A tree map with `root` as the resource root, the given functions and empty
/// principal lists.
fn tree(root: QueryValue, functions: Vec<QueryValue>) -> QueryValue {
    let mut principals = new_map();
    principals.insert(String::from("users"), QueryValue::List(Vec::new()));
    principals.insert(String::from("groups"), QueryValue::List(Vec::new()));
    let mut map = new_map();
    map.insert(String::from("resources"), root);
    map.insert(String::from("functions"), QueryValue::List(functions));
    map.insert(String::from("principals"), QueryValue::Map(principals));
    QueryValue::Map(map)
}

/// `2001` tables under one store: a healthy catalogue whose node count is over
/// the shell's whole-tree budget.
fn wide_store() -> QueryValue {
    let tables: Vec<QueryValue> = (0..2001)
        .map(|index| node(&format!("t{index}"), "table", Vec::new()))
        .collect();
    node("store1", "store", tables)
}

/// `rights show` of the root projects EXACTLY the root even when the subtree is
/// over the whole-tree cap. Mutant: projecting the selected node and its
/// descendants before selection (`project_node`), which refuses a healthy
/// 2001-table catalogue.
#[test]
fn rights_show_projects_only_the_selected_node() {
    let root = node(
        "root",
        "root",
        vec![node("db1", "database", vec![wide_store()])],
    );
    let value = tree(root, Vec::new());
    let projected = project_rights(&value, &Selector::Root).expect("the root projects");
    assert_eq!(projected["path"], "/");
    assert_eq!(projected["name"], "root");
    assert_eq!(projected["kind"], "root");
    assert!(
        projected.get("children").is_none(),
        "rights show never projects descendants"
    );
}

/// The same guarantee for a SELECTED node whose own subtree is over the cap.
#[test]
fn rights_show_of_a_wide_node_projects_that_node_only() {
    let root = node(
        "root",
        "root",
        vec![node("db1", "database", vec![wide_store()])],
    );
    let value = tree(root, Vec::new());
    let selector = Selector::Resource(vec![String::from("db1")]);
    let projected = project_rights(&value, &selector).expect("db1 projects");
    assert_eq!(projected["path"], "db1");
    assert_eq!(projected["name"], "db1");
    assert_eq!(projected["kind"], "database");
    // The selected node's leaves are all present.
    assert_eq!(projected["owner"], "1");
    assert_eq!(projected["owner_name"], "alice");
    assert_eq!(projected["mode"], "0750");
    assert_eq!(projected["setuid"], false);
}

/// `group tree` keeps the whole-tree cap and reports it as the TYPED refusal
/// naming the limit and the `--db` remedy instead of a generic server error.
/// Mutant: returning `ShellError::Server` for the cap.
#[test]
fn group_tree_over_the_cap_is_the_typed_refusal() {
    let root = node(
        "root",
        "root",
        vec![node("db1", "database", vec![wide_store()])],
    );
    let value = tree(root, Vec::new());
    let refusal = project_tree(&value, None).expect_err("the whole tree is over the cap");
    assert!(
        matches!(refusal, ShellError::TreeTooLarge),
        "the cap is its own typed refusal"
    );
    assert_eq!(refusal.code(), "tree_too_large");
    assert_eq!(refusal.exit(), 5);
    assert!(
        refusal.message().contains("--db"),
        "the fixed message names the remedy"
    );
}

/// A present `children` that is not a list is a shape mismatch in the whole
/// tree projection, never a silently empty subtree. Mutant: treating any
/// non-list `children` as "no children".
#[test]
fn a_non_list_children_field_is_a_shape_mismatch() {
    let mut malformed = node("db1", "database", Vec::new());
    if let QueryValue::Map(fields) = &mut malformed {
        fields.insert(
            String::from("children"),
            QueryValue::Str(String::from("not a list")),
        );
    }
    let value = tree(node("root", "root", vec![malformed]), Vec::new());
    let refusal = project_tree(&value, None).expect_err("a malformed subtree must be refused");
    assert!(
        matches!(refusal, ShellError::Server),
        "a malformed `children` is a server shape mismatch"
    );
}

/// A function node carries the `builtin` flag the engine sends: `group tree`
/// and `rights show` must be able to tell a built-in from a stored function.
/// Mutant: verifying the Boolean and then discarding it.
#[test]
fn function_nodes_keep_the_builtin_flag() {
    let root = node("root", "root", Vec::new());
    let functions = vec![
        function_node("now", Some(true)),
        function_node("math/abs", Some(false)),
    ];
    let projected = project_tree(&tree(root, functions), None).expect("the tree projects");
    assert_eq!(projected["functions"][0]["builtin"], true);
    assert_eq!(projected["functions"][1]["builtin"], false);
    assert_eq!(projected["functions"][1]["name"], "math/abs");
}

/// A non-Boolean `builtin` is a shape mismatch, never a coerced value.
#[test]
fn a_non_boolean_builtin_is_a_shape_mismatch() {
    let mut map = new_map();
    map.insert(String::from("name"), QueryValue::Str(String::from("now")));
    map.insert(
        String::from("kind"),
        QueryValue::Str(String::from("function")),
    );
    map.insert(String::from("owner"), QueryValue::Int(1));
    map.insert(String::from("owner_name"), QueryValue::Null);
    map.insert(String::from("group"), QueryValue::Null);
    map.insert(String::from("group_name"), QueryValue::Null);
    map.insert(String::from("mode"), QueryValue::Int(0o700));
    map.insert(String::from("setuid"), QueryValue::Bool(false));
    map.insert(
        String::from("builtin"),
        QueryValue::Str(String::from("yes")),
    );
    let value = tree(node("root", "root", Vec::new()), vec![QueryValue::Map(map)]);
    assert!(matches!(
        project_tree(&value, None),
        Err(ShellError::Server)
    ));
}

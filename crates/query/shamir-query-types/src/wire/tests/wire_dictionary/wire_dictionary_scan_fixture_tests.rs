//! Real-graph fixture and traversal unit tests for the wire-dictionary
//! pinning test (WIRE-DICT-1). Split out of
//! `wire_dictionary_scan_tests.rs` (file size cap): these tests prove the
//! completeness machinery non-vacuous against an ACTUAL scan of a minimal
//! source fixture through the REAL `collect_items`, plus unit tests for
//! `type_graph_edges` and the hand-built-graph traversal tests.

use std::collections::BTreeMap;
use std::path::Path;

use super::wire_dictionary_scan_source::{collect_items, ScanEntry};
use super::wire_dictionary_scan_tests::{
    check_completeness, check_snapshot_sanity, reachable, type_graph_edges, Corpus, SnapField,
    Snapshot, SnapshotType, FIXED_ROOTS, PRIMITIVES,
};

// ---------------------------------------------------------------------------

const FIXTURE_SRC: &str = "mod m { \
    pub struct DbRequest { a: A } \
    pub struct DbResponse { } \
    pub struct A { b: Vec<B>, pair: (B, u32) } \
    pub struct B { x: u32 } \
}";

fn fixture_scan() -> (BTreeMap<String, ScanEntry>, BTreeMap<String, String>) {
    let file = syn::parse_file(FIXTURE_SRC).expect("fixture parses");
    let mut scan = BTreeMap::new();
    let mut aliases = BTreeMap::new();
    let mut errs = Vec::new();
    collect_items(
        &file.items,
        &mut scan,
        &mut aliases,
        Path::new("fixture.rs"),
        &mut errs,
    );
    assert!(errs.is_empty(), "fixture scan errors: {errs:?}");
    (scan, aliases)
}

fn fixture_snap_type(kind: &str, fields: Vec<SnapField>) -> SnapshotType {
    SnapshotType {
        crate_name: "shamir-query-types".to_string(),
        path: String::new(),
        kind: kind.to_string(),
        impl_kind: "derived".to_string(),
        verdict: "named".to_string(),
        reason: String::new(),
        serde: Vec::new(),
        fields,
        variants: Vec::new(),
    }
}

fn fixture_snap_field(name: &str, ty: &str) -> SnapField {
    SnapField {
        name: name.to_string(),
        wire: name.to_string(),
        ty: ty.to_string(),
        serde: Vec::new(),
    }
}

fn fixture_roots() -> Vec<String> {
    FIXED_ROOTS.iter().map(|r| r.to_string()).collect()
}

/// (i) B missing from the snapshot types while still reachable — via the
/// `Vec<B>` field AND the `(B, u32)` tuple field — must FAIL completeness.
#[test]
fn fixture_missing_reachable_type_fails_completeness() {
    let (scan, aliases) = fixture_scan();
    let mut types = BTreeMap::new();
    types.insert("DbRequest".to_string(), fixture_snap_type("struct", vec![]));
    types.insert(
        "DbResponse".to_string(),
        fixture_snap_type("struct", vec![]),
    );
    // A is present but B is NOT — B is still reachable through Vec<B> and
    // (B, u32), so the check must fail naming B.
    types.insert(
        "A".to_string(),
        fixture_snap_type(
            "struct",
            vec![
                fixture_snap_field("b", "Vec<B>"),
                fixture_snap_field("pair", "(B, u32)"),
            ],
        ),
    );
    let c = Corpus {
        scan,
        aliases,
        snapshot: Snapshot {
            schema_version: 1,
            roots: fixture_roots(),
            types,
        },
    };
    check_snapshot_sanity(&c).unwrap_or_else(|e| panic!("sanity should pass: {e}"));
    let err = check_completeness(&c).expect_err("missing reachable B must fail completeness");
    assert!(
        err.contains("B") && err.contains("missing"),
        "failure must name the missing type B, got: {err}"
    );
}

/// (ii) with B present, the same REAL checks PASS.
#[test]
fn fixture_complete_graph_passes_real_checks() {
    let (scan, aliases) = fixture_scan();
    let mut types = BTreeMap::new();
    types.insert("DbRequest".to_string(), fixture_snap_type("struct", vec![]));
    types.insert(
        "DbResponse".to_string(),
        fixture_snap_type("struct", vec![]),
    );
    types.insert(
        "A".to_string(),
        fixture_snap_type(
            "struct",
            vec![
                fixture_snap_field("b", "Vec<B>"),
                fixture_snap_field("pair", "(B, u32)"),
            ],
        ),
    );
    types.insert("B".to_string(), fixture_snap_type("struct", vec![]));
    let c = Corpus {
        scan,
        aliases,
        snapshot: Snapshot {
            schema_version: 1,
            roots: fixture_roots(),
            types,
        },
    };
    check_snapshot_sanity(&c).unwrap_or_else(|e| panic!("{e}"));
    check_completeness(&c).unwrap_or_else(|e| panic!("{e}"));
}

/// (iv) an empty-roots snapshot must FAIL sanity (completeness would still
/// run from the FIXED roots — it is never driven by the snapshot's list).
#[test]
fn fixture_empty_roots_fail_sanity() {
    let (scan, aliases) = fixture_scan();
    let types: BTreeMap<String, SnapshotType> = BTreeMap::new();
    let c = Corpus {
        scan,
        aliases,
        snapshot: Snapshot {
            schema_version: 1,
            roots: Vec::new(),
            types,
        },
    };
    let err = check_snapshot_sanity(&c).expect_err("empty roots must fail sanity");
    assert!(err.contains("roots"), "failure must name roots, got: {err}");
}

/// (iii) the edge extractor resolves BOTH fixture edge kinds: generic
/// arguments (Vec<B>) and tuple elements ((B, u32)).
#[test]
fn type_graph_edges_resolve_generic_and_tuple_elements() {
    assert_eq!(
        type_graph_edges("Vec<B>").unwrap(),
        vec!["B".to_string()],
        "Vec<B> must resolve its element"
    );
    assert_eq!(
        type_graph_edges("(B, u32)").unwrap(),
        vec!["B".to_string(), "u32".to_string()],
        "tuples must resolve EVERY element"
    );
    assert_eq!(
        type_graph_edges("Vec<(B, u32)>").unwrap(),
        vec!["B".to_string(), "u32".to_string()],
        "tuple elements nested inside generics must resolve"
    );
    assert_eq!(
        type_graph_edges("TMap<A, String>").unwrap(),
        vec!["A".to_string(), "String".to_string()],
        "ALL generic arguments must resolve (not just the last)"
    );
    assert_eq!(
        type_graph_edges("Option<Filter>").unwrap(),
        vec!["Filter".to_string()],
    );
    assert_eq!(
        type_graph_edges("[u8; 16]").unwrap(),
        vec!["u8".to_string()]
    );
    assert_eq!(
        type_graph_edges("ByteBuf").unwrap(),
        vec!["ByteBuf".to_string()]
    );
    // unsupported / malformed type strings FAIL, they are never skipped
    let err = type_graph_edges("#unsupported-type#").expect_err("unsupported must fail");
    assert!(err.contains("#unsupported-type#"), "got: {err}");
    assert!(
        type_graph_edges("Foo<").is_err(),
        "unbalanced generics must fail"
    );
    assert!(type_graph_edges("").is_err(), "empty type must fail");
}

// ---------------------------------------------------------------------------
// Unit tests for the completeness traversal itself (hand-built graphs)
// ---------------------------------------------------------------------------

#[test]
fn completeness_traversal_fails_on_a_missing_type() {
    // A → B → Missing: the closure MUST surface "Missing", so the
    // production completeness check would fail on this graph.
    let mut defs = BTreeMap::new();
    defs.insert(
        "A".to_string(),
        vec![("field b".to_string(), "B".to_string())],
    );
    defs.insert(
        "B".to_string(),
        vec![("field m".to_string(), "Missing".to_string())],
    );
    let reached = reachable(&["A"], &defs).expect("no malformed types");
    assert!(
        reached.contains("Missing"),
        "traversal must surface the undefined type `Missing`, got {reached:?}"
    );
    // the traversal itself is non-vacuous: it visits the intermediate edge
    assert!(
        reached.contains("B") && reached.contains("A"),
        "traversal must visit the roots and their members, got {reached:?}"
    );
}

#[test]
fn completeness_traversal_surfaces_unsupported_type_strings_with_context() {
    let mut defs = BTreeMap::new();
    defs.insert(
        "A".to_string(),
        vec![("field bad".to_string(), "#unsupported-type#".to_string())],
    );
    let err = reachable(&["A"], &defs).expect_err("unsupported type must fail");
    assert!(
        err.contains("A") && err.contains("field bad") && err.contains("#unsupported-type#"),
        "error must name the type and field, got: {err}"
    );
}

#[test]
fn completeness_traversal_on_a_fully_covered_graph_yields_only_defined_names() {
    // A → [B, Vec<B>], B → (A, u32) — every reached name is defined; the
    // closure must terminate on a self-referential graph too.
    let mut defs = BTreeMap::new();
    defs.insert(
        "A".to_string(),
        vec![
            ("field v".to_string(), "Vec<B>".to_string()),
            ("field b".to_string(), "B".to_string()),
        ],
    );
    defs.insert(
        "B".to_string(),
        vec![("field t".to_string(), "(A, u32)".to_string())],
    );
    let reached = reachable(&["A"], &defs).expect("no malformed types");
    for name in &reached {
        assert!(
            defs.contains_key(name) || PRIMITIVES.contains(&name.as_str()),
            "fully-covered graph must not reach undefined type `{name}`, got {reached:?}"
        );
    }
    assert!(
        reached.contains("B"),
        "traversal must resolve Vec<B> to B, got {reached:?}"
    );
    assert!(
        reached.contains("u32"),
        "tuple element u32 must be reached, got {reached:?}"
    );
}

/// The reachable name set from the fixture scan must contain B through BOTH
/// edge kinds (generic + tuple) — guards the fixture against silently
/// losing an edge kind.
#[test]
fn fixture_reachability_covers_both_edge_kinds() {
    let (scan, aliases) = fixture_scan();
    let mut defs: BTreeMap<String, Vec<(String, String)>> = BTreeMap::new();
    for (name, entry) in &scan {
        defs.insert(name.clone(), entry.member_types());
    }
    for (alias, target) in &aliases {
        defs.insert(alias.clone(), vec![("alias".to_string(), target.clone())]);
    }
    let reached = reachable(FIXED_ROOTS, &defs).expect("no malformed fixture types");
    for expected in ["DbRequest", "DbResponse", "A", "B", "u32"] {
        assert!(
            reached.contains(expected),
            "fixture reachability must cover `{expected}`, got {reached:?}"
        );
    }
}

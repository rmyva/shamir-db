//! Real-graph fixture and traversal unit tests for the wire-dictionary
//! pinning test (WIRE-DICT-1). Split out of
//! `wire_dictionary_scan_tests.rs` (file size cap): these tests prove the
//! truncation/append rule (CHECK 1) and the completeness machinery (CHECK
//! 2) non-vacuous against an ACTUAL scan of a minimal source fixture
//! through the REAL `collect_items` + REAL checks, plus unit tests for
//! `type_graph_edges` and the hand-built-graph traversal tests.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use super::wire_dictionary_scan_source::{collect_items, ScanEntry};
use super::wire_dictionary_scan_tests::{
    check_completeness, check_one_type, check_snapshot_sanity, reachable, Corpus, SnapField,
    Snapshot, SnapshotType, FIXED_ROOTS, PRIMITIVES,
};

type FixtureScan = (
    BTreeMap<String, ScanEntry>,
    BTreeMap<String, String>,
    BTreeMap<String, BTreeSet<std::path::PathBuf>>,
);

const FIXTURE_SRC: &str = "mod m { \
    pub struct DbRequest { a: A } \
    pub struct DbResponse { } \
    pub struct A { b: Vec<B>, pair: (B, u32) } \
    pub struct B { x: u32 } \
}";

/// The SAME graph with the MIDDLE field `b` removed from A (source shortened
/// by one member at position 0, `pair` shifted to #0).
const FIXTURE_SRC_MIDDLE_REMOVED: &str = "mod m { \
    pub struct DbRequest { a: A } \
    pub struct DbResponse { } \
    pub struct A { pair: (B, u32) } \
    pub struct B { x: u32 } \
}";

fn fixture_scan() -> FixtureScan {
    let file = syn::parse_file(FIXTURE_SRC).expect("fixture parses");
    let mut scan = BTreeMap::new();
    let mut aliases = BTreeMap::new();
    let mut defining: BTreeMap<String, BTreeSet<std::path::PathBuf>> = BTreeMap::new();
    let mut errs = Vec::new();
    collect_items(
        &file.items,
        &mut scan,
        &mut aliases,
        &mut defining,
        Path::new("fixture.rs"),
        &mut errs,
    );
    assert!(errs.is_empty(), "fixture scan errors: {errs:?}");
    (scan, aliases, defining)
}

fn fixture_snap_type(kind: &str, fields: Vec<SnapField>) -> SnapshotType {
    SnapshotType {
        crate_name: "shamir-query-types".to_string(),
        path: Some("fixture.rs".to_string()),
        kind: kind.to_string(),
        impl_kind: "derived".to_string(),
        verdict: "named".to_string(),
        reason: String::new(),
        unreachable_reason: String::new(),
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

/// CHECK 1 fixture harness: real scan of the given source, a snapshot for A
/// that pins the given field list, then the REAL `check_one_type`
/// collector. Returns all collected mismatch strings (empty = passes).
fn check_fixture_a_src(src: &str, snap_fields: Vec<SnapField>) -> Vec<String> {
    let file = syn::parse_file(src).expect("fixture parses");
    let mut scan = BTreeMap::new();
    let mut aliases = BTreeMap::new();
    let mut defining = BTreeMap::new();
    let mut errs = Vec::new();
    collect_items(
        &file.items,
        &mut scan,
        &mut aliases,
        &mut defining,
        Path::new("fixture.rs"),
        &mut errs,
    );
    assert!(errs.is_empty(), "fixture scan errors: {errs:?}");
    let rec = scan.get("A").expect("fixture defines A");
    let snap = fixture_snap_type("struct", snap_fields);
    let mut mism = Vec::new();
    check_one_type("A", &snap, rec, &mut mism);
    mism
}

fn check_fixture_a(snap_fields: Vec<SnapField>) -> Vec<String> {
    check_fixture_a_src(FIXTURE_SRC, snap_fields)
}

/// (i) B missing from the snapshot types while still reachable — via the
/// `Vec<B>` field AND the `(B, u32)` tuple field — must FAIL completeness.
#[test]
fn fixture_missing_reachable_type_fails_completeness() {
    let (scan, aliases, defining) = fixture_scan();
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
        defining,
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
    let (scan, aliases, defining) = fixture_scan();
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
        defining,
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
    let (scan, aliases, defining) = fixture_scan();
    let types: BTreeMap<String, SnapshotType> = BTreeMap::new();
    let c = Corpus {
        scan,
        aliases,
        defining,
        snapshot: Snapshot {
            schema_version: 1,
            roots: Vec::new(),
            types,
        },
    };
    let err = check_snapshot_sanity(&c).expect_err("empty roots must fail sanity");
    assert!(err.contains("roots"), "failure must name roots, got: {err}");
}

// ---------------------------------------------------------------------------
// CHECK 1 length-rule fixtures: pure APPEND tolerated; removal / truncation
// / reorder all fail. All through the REAL `collect_items` scan + REAL
// `check_one_type` mismatch collector.
// ---------------------------------------------------------------------------

/// APPEND: the source (b, pair) may carry MORE trailing members than the
/// snapshot pins — the only tolerated length difference.
#[test]
fn fixture_appended_trailing_field_passes() {
    // Snapshot pins only the first source field; `pair` is a trailing
    // source APPEND.
    let mism = check_fixture_a(vec![fixture_snap_field("b", "Vec<B>")]);
    assert!(
        mism.is_empty(),
        "a trailing source APPEND must be tolerated, got: {mism:?}"
    );
}

/// REMOVAL: the snapshot expects a member the source no longer declares —
/// a source SHORTER than the snapshot must fail with the removal message
/// ("protocol version change required"), never be tolerated at the tail.
#[test]
fn fixture_source_shortened_fails() {
    // Snapshot expects (b, pair); the source has both — instead SHORTEN the
    // snapshot side of the comparison by pinning a third field the source
    // does not have, i.e. the snapshot is longer than the source list.
    let mut fields = vec![
        fixture_snap_field("b", "Vec<B>"),
        fixture_snap_field("pair", "(B, u32)"),
    ];
    fields.push(fixture_snap_field("extra", "u64")); // source has no #2
    let mism = check_fixture_a(fields);
    assert!(
        !mism.is_empty(),
        "a snapshot longer than the source (removal) must fail"
    );
    assert!(
        mism.iter()
            .any(|m| m.contains("protocol version change required")),
        "removal failure must demand a protocol version change, got: {mism:?}"
    );
    assert!(
        mism.iter().any(|m| m.contains("field list length")),
        "failure must be the field-list-length guard, got: {mism:?}"
    );
}

/// MIDDLE REMOVAL: the source no longer declares `b` at position 0, so it
/// is SHORTER than the snapshot (b, pair) — the removal guard must fire AND
/// no positional compare may silently re-align `pair` to snapshot #0.
#[test]
fn fixture_middle_removal_fails() {
    let mism = check_fixture_a_src(
        FIXTURE_SRC_MIDDLE_REMOVED,
        vec![
            fixture_snap_field("b", "Vec<B>"),
            fixture_snap_field("pair", "(B, u32)"),
        ],
    );
    assert!(
        !mism.is_empty(),
        "a middle removal (source shortened) must fail"
    );
    assert!(
        mism.iter()
            .any(|m| m.contains("protocol version change required")),
        "removal failure must demand a protocol version change, got: {mism:?}"
    );
}

/// REORDER: same members, different order — the positional compare fails
/// on position 0 and reports ALL positional mismatches (collector, not
/// early-return).
#[test]
fn fixture_reorder_fails() {
    let mism = check_fixture_a(vec![
        fixture_snap_field("pair", "(B, u32)"),
        fixture_snap_field("b", "Vec<B>"),
    ]);
    assert!(
        mism.len() >= 2,
        "reorder must report at least the two swapped positions (all mismatches), got: {mism:?}"
    );
    assert!(
        mism.iter().any(|m| m.contains("field #0")) && mism.iter().any(|m| m.contains("field #1")),
        "reorder failure must name BOTH swapped positions, got: {mism:?}"
    );
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
    let (scan, aliases, _defining) = fixture_scan();
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

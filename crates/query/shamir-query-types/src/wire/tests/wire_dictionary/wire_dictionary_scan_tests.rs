//! Wire-dictionary pinning test (WIRE-DICT-1, design
//! `docs/dev-artifacts/design/wire-keyword-dictionary.md` §3.2).
//!
//! The protocol dictionary IS the declaration order of every wire struct
//! field and enum variant. This test rebuilds that table straight from the
//! crate source with a `syn` scanner and compares it against the committed
//! snapshot `docs/guide-docs/client-server-protocol-spec/wire-dictionary.json`:
//!
//! - CHECK 1 (order pin): every derived snapshot type for this crate must
//!   still exist with the same struct/enum kind (classified by SYNTAX:
//!   named-field struct / newtype / tuple / unit), the same field/variant
//!   names, the same EFFECTIVE WIRE NAMES (rename / rename_all applied), the
//!   same member types as written in source, and the same full set of
//!   wire-shape-affecting serde attributes at the same positions. Anything
//!   else (reorder, rename, retag, adapter swap, removal) fails with
//!   "protocol version change required". Source entries AFTER the snapshot's
//!   last entry are allowed (the append-only rule) and do not fail; a
//!   snapshot LONGER than the source is a truncation and fails.
//! - CHECK 2 (completeness): real reachability, computed in the SOURCE.
//!   Starting from the snapshot roots ("DbRequest", "DbResponse") the scan
//!   map is closed over member types (tuples and arrays resolve each
//!   element, crate type aliases expand) and every reached crate-defined
//!   type must be a snapshot key or a documented exclusion. The closure
//!   deliberately does NOT recurse into `QueryValue`: its hand-written impl
//!   is self-describing (externally tagged, no field keys), so its members
//!   emit no wire keys and end the traversal there.
//! - CHECK 3 (hand-written impls): snapshot entries with `impl ==
//!   "hand_written"` are skipped by CHECK 1's positional comparison — their
//!   fields come from a manual serde impl, not from a derive, so the
//!   declaration order says nothing about the wire. Such types must still
//!   EXIST in the scan and CHECK 2 still applies. NOTE: a hand-written impl
//!   can drift silently in field order because nothing here observes it —
//!   the per-type indexed/named round-trip decode tests in
//!   `wire_dictionary_decode_tests.rs` (and WIRE-DICT-2's adapter tests)
//!   are its guard.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use serde::Deserialize;

use super::wire_dictionary_scan_source::{
    collect_items, collect_rs_files, Member, ScanEntry, SerdeFlags,
};

// ---------------------------------------------------------------------------
// Typed snapshot model (schema_version 1)
// ---------------------------------------------------------------------------

#[derive(Deserialize, Debug)]
struct Snapshot {
    #[allow(dead_code)]
    schema_version: u32,
    roots: Vec<String>,
    types: BTreeMap<String, SnapshotType>,
}

#[derive(Deserialize, Debug)]
struct SnapshotType {
    #[serde(rename = "crate")]
    crate_name: String,
    #[allow(dead_code)]
    path: String,
    kind: String,
    #[serde(rename = "impl")]
    impl_kind: String,
    #[allow(dead_code)]
    verdict: String,
    #[serde(default)]
    #[allow(dead_code)]
    reason: String,
    #[serde(default)]
    serde: Vec<String>,
    #[serde(default)]
    fields: Vec<SnapField>,
    #[serde(default)]
    variants: Vec<SnapVariant>,
}

#[derive(Deserialize, Debug)]
struct SnapField {
    name: String,
    /// effective wire name pinned by the scanner's rename/rename_all rules
    #[serde(default)]
    wire: String,
    #[serde(rename = "type", default)]
    ty: String,
    #[serde(default)]
    serde: Vec<String>,
}

#[derive(Deserialize, Debug)]
struct SnapVariant {
    name: String,
    #[serde(default)]
    wire: String,
    shape: String,
    #[serde(default)]
    serde: Vec<String>,
    #[serde(default)]
    fields: Vec<SnapField>,
}

/// Attribute keys the scanner pins. Everything else appearing in a snapshot
/// `serde` array (e.g. `from = "FieldRuleWire"`, hand-written prose notes) is
/// not wire-shape-affecting and is normalized away on BOTH sides.
const PINNED_VALUE_ATTRS_IN_ORDER: &[&str] = &[
    // longest prefixes first: `rename_all` before `rename`
    "rename_all",
    "rename",
    "alias",
    "default",
    "tag",
    "content",
    "skip_serializing_if",
    "with",
    "serialize_with",
    "deserialize_with",
];

const PINNED_FLAG_ATTRS: &[&str] = &[
    "flatten",
    "skip_serializing",
    "skip_deserializing",
    "deny_unknown_fields",
    "untagged",
    "transparent",
    // bare `default` (no `= path`) is a flag-style entry in the snapshot
    "default",
];

/// Normalize one snapshot `serde` array entry into its canonical form.
/// Returns `None` for attributes the scanner does not pin.
fn normalize_snap_attr(raw: &str) -> Option<String> {
    let s = raw.trim();
    for key in PINNED_VALUE_ATTRS_IN_ORDER {
        if let Some(rest) = s.strip_prefix(key) {
            let rest = rest.trim_start();
            if let Some(v) = rest.strip_prefix('=') {
                let value = v.trim().trim_matches('"');
                return Some(format!("{key} = \"{value}\""));
            }
        }
    }
    for key in PINNED_FLAG_ATTRS {
        if s == *key {
            return Some(key.to_string());
        }
    }
    None
}

fn normalized_snap_set(serde: &[String]) -> BTreeSet<String> {
    serde
        .iter()
        .filter_map(|s| normalize_snap_attr(s))
        .collect()
}

fn sets_diff(expected: &BTreeSet<String>, actual: &BTreeSet<String>) -> String {
    let missing: Vec<&String> = expected.difference(actual).collect();
    let extra: Vec<&String> = actual.difference(expected).collect();
    let mut out = String::new();
    if !missing.is_empty() {
        out.push_str(&format!(" snapshot missing {missing:?};"));
    }
    if !extra.is_empty() {
        out.push_str(&format!(" snapshot has extra {extra:?};"));
    }
    out
}

// ---------------------------------------------------------------------------
// Shared corpus (parsed once, shared by the checks)
// ---------------------------------------------------------------------------

struct Corpus {
    scan: BTreeMap<String, ScanEntry>,
    /// crate type alias name → target type string (`Item::Type`)
    aliases: BTreeMap<String, String>,
    snapshot: Snapshot,
}

fn snapshot_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(3)
        .expect("crate sits three levels below the repo root")
        .join("docs/guide-docs/client-server-protocol-spec/wire-dictionary.json")
}

fn corpus() -> &'static Corpus {
    static CORPUS: OnceLock<Corpus> = OnceLock::new();
    CORPUS.get_or_init(|| {
        let src_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut files = Vec::new();
        collect_rs_files(&src_root, &mut files);
        assert!(
            files.len() > 50,
            "scanner found only {} source files under {} — walk is broken",
            files.len(),
            src_root.display()
        );
        let mut scan = BTreeMap::new();
        let mut aliases = BTreeMap::new();
        for file in files {
            let text = fs::read_to_string(&file)
                .unwrap_or_else(|e| panic!("cannot read {}: {e}", file.display()));
            let ast = syn::parse_file(&text)
                .unwrap_or_else(|e| panic!("cannot parse {}: {e}", file.display()));
            collect_items(&ast.items, &mut scan, &mut aliases);
        }
        assert!(
            scan.len() > 100,
            "scanner found only {} type definitions under {} — walk is broken",
            scan.len(),
            src_root.display()
        );
        let text = fs::read_to_string(snapshot_path()).expect("snapshot JSON is readable");
        let snapshot: Snapshot = serde_json::from_str(&text).expect("snapshot JSON parses");
        Corpus {
            scan,
            aliases,
            snapshot,
        }
    })
}

fn check_snapshot_sanity(c: &Corpus) {
    for root in ["DbRequest", "DbResponse"] {
        assert!(
            c.snapshot.types.contains_key(root),
            "snapshot is missing the required wire root `{root}` — \
             wire-dictionary.json is truncated or corrupted — \
             protocol version change required"
        );
    }
    // Truncation guard. NOTE: we deliberately do NOT assert
    // `snapshot.types.len() >= scan.len()`: the scanner sees EVERY type
    // defined in the crate, including non-wire helpers and cfg(test)
    // fixture types that the snapshot intentionally omits, so the raw
    // inequality would false-positive. The real truncation guard is the
    // reverse direction, enforced by CHECK 1 + CHECK 3: every snapshot
    // type must still exist in the scan, so deleting a snapshot entry
    // (or shrinking the snapshot) always fails.
    for (name, snap) in &c.snapshot.types {
        if snap.crate_name != "shamir-query-types" {
            continue;
        }
        assert!(
            c.scan.contains_key(name) || c.aliases.contains_key(name),
            "snapshot type {name} is neither scanned nor a known crate alias — \
             snapshot and scanner disagree on what this crate defines — \
             protocol version change required"
        );
    }
}

/// Strip `Option<>` / `Vec<>` / `Box<>` / `HashMap<K, V>` / `TMap<K, V>` /
/// generic parameters / whitespace / crate paths; take the last path
/// segment. Returns `None` for fixed arrays `[u8; 16]` and tuples `(A, B)`
/// (the reachability closure resolves their ELEMENTS instead).
fn base_type_name(ty: &str) -> Option<String> {
    let mut t = ty.trim();
    if t.starts_with('[') || t.starts_with('(') {
        return None;
    }
    loop {
        t = t.trim();
        if t.ends_with('>') {
            let open = t.find('<')?;
            let inner = &t[open + 1..t.len() - 1];
            if inner.trim_start().starts_with('(') {
                return None; // Vec<(u64, String)>-style tuple element
            }
            // for maps take the VALUE type (the last top-level segment)
            let mut depth = 0usize;
            let mut last_start = 0usize;
            for (i, b) in inner.as_bytes().iter().enumerate() {
                match b {
                    b'<' => depth += 1,
                    b'>' => depth -= 1,
                    b',' if depth == 0 => last_start = i + 1,
                    _ => {}
                }
            }
            t = inner[last_start..].trim();
        } else {
            break;
        }
    }
    let seg = t.rsplit("::").next().unwrap_or(t).trim();
    let name = seg.split('<').next().unwrap_or(seg).trim();
    if name.is_empty() {
        None
    } else {
        Some(name.to_string())
    }
}

/// Split a member type string into the type strings whose base names
/// should be resolved: tuples `(A, B)` yield each element, arrays
/// `[T; N]` yield `T`, anything else yields itself.
fn ty_elements(ty: &str) -> Vec<String> {
    let t = ty.trim();
    if t.starts_with('(') && t.ends_with(')') {
        return split_top_level(&t[1..t.len() - 1]);
    }
    if t.starts_with('[') {
        if let Some(semi) = t.find(';') {
            return vec![t[1..semi].trim().to_string()];
        }
    }
    vec![t.to_string()]
}

/// Split on commas at bracket depth 0 only.
fn split_top_level(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut depth = 0usize;
    let mut start = 0usize;
    for (i, b) in s.as_bytes().iter().enumerate() {
        match b {
            b'<' | b'(' | b'[' => depth += 1,
            b'>' | b')' | b']' => depth = depth.saturating_sub(1),
            b',' if depth == 0 => {
                out.push(s[start..i].trim().to_string());
                start = i + 1;
            }
            _ => {}
        }
    }
    out.push(s[start..].trim().to_string());
    out
}

const PRIMITIVES: &[&str] = &[
    "u8", "u16", "u32", "u64", "u128", "i8", "i16", "i32", "i64", "i128", "bool", "String", "char",
    "f32", "f64", "usize", "isize",
];

/// Names that can never carry wire keys, deliberately excluded from
/// CHECK 2. Each entry carries its justification.
const EXCLUDED_TYPE_NAMES: &[&str] = &[
    // std::num::NonZeroU64 behind `with = "nonzero_u64_wire"`: a bare
    // self-describing integer on the wire, no keys.
    "NonZeroU64",
    // `pub type FieldPath = Vec<String>` (read/core/field_path.rs): a plain
    // array on the wire, no keys. (The closure expands the alias, but the
    // alias name itself is reached and stays excluded here.)
    "FieldPath",
    // shamir-types internals behind the hand-written QueryValue impl; both
    // travel as strings on the wire ("serialized as string" notes). Not
    // reached while the closure stops at QueryValue, kept excluded in case
    // a future wire DTO references them directly.
    "Decimal",
    "BigInt",
    // Generic parameter of `Value<Key>` inside the QueryValue snapshot
    // entry — a type parameter, not a wire type.
    "Key",
];

/// Pure reachability closure over `defs` (type name → member type strings;
/// crate type aliases map to a single-element target list). Returns every
/// name transitively reachable from `roots`, including the roots.
///
/// Termination: a `visited` set guards the worklist.
///
/// `QueryValue` is a TERMINAL node: its hand-written serde impl is
/// externally-tagged and self-describing (no field keys on the wire), so
/// its members are deliberately kept out of the closure.
fn reachable(roots: &[&str], defs: &BTreeMap<String, Vec<String>>) -> BTreeSet<String> {
    let mut visited: BTreeSet<String> = BTreeSet::new();
    let mut worklist: Vec<String> = roots.iter().map(|r| r.to_string()).collect();
    while let Some(name) = worklist.pop() {
        if !visited.insert(name.clone()) {
            continue;
        }
        // QueryValue's impl is self-describing — stop there (see doc comment).
        if name == "QueryValue" {
            continue;
        }
        if let Some(members) = defs.get(&name) {
            for member in members {
                for element in ty_elements(member) {
                    if let Some(base) = base_type_name(&element) {
                        if !visited.contains(&base) {
                            worklist.push(base);
                        }
                    }
                }
            }
        }
    }
    visited
}

/// CHECK 2 — real completeness from the roots, computed in the SOURCE:
/// every crate-defined type reachable from DbRequest/DbResponse must be a
/// snapshot key or a documented exclusion. A name that a scan of this
/// crate finds as a struct/enum but that has no snapshot entry is a NEW
/// wire type and fails loudly.
fn check_completeness(c: &Corpus) {
    let mut defs: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (name, entry) in &c.scan {
        defs.insert(name.clone(), entry.member_types());
    }
    // crate type aliases expand to their target's members
    for (alias, target) in &c.aliases {
        defs.insert(alias.clone(), vec![target.clone()]);
    }
    let roots: Vec<&str> = c.snapshot.roots.iter().map(|r| r.as_str()).collect();
    let reached = reachable(&roots, &defs);
    for name in reached {
        if c.snapshot.types.contains_key(&name)
            || PRIMITIVES.contains(&name.as_str())
            || EXCLUDED_TYPE_NAMES.contains(&name.as_str())
        {
            continue;
        }
        panic!(
            "new wire type {name} reachable from the snapshot roots {:?} but missing from \
             wire-dictionary.json — add it or exclude it deliberately{}",
            c.snapshot.roots,
            if c.scan.contains_key(&name) {
                " (found in this crate by the source scanner)"
            } else {
                " (external crate type)"
            }
        );
    }
}

/// CHECK 1 name comparison is rename-aware: a snapshot field name matches
/// either the Rust identifier or any of its `rename`/`alias` values.
fn name_matches(source: &str, snapshot: &str, flags: &SerdeFlags) -> bool {
    source == snapshot || flags.has_rename(snapshot)
}

fn fail(type_name: &str, position: &str, expected: &str, actual: &str) -> ! {
    panic!(
        "wire type {type_name} at {position}: expected {expected}, actual {actual} — \
         protocol version change required"
    );
}

/// CHECK 1 — order pin for one derived type.
fn check_one_type(name: &str, snap: &SnapshotType, rec: &ScanEntry) {
    if rec.kind != snap.kind {
        fail(
            name,
            "type kind",
            &format!("kind {}", snap.kind),
            &format!("kind {}", rec.kind),
        );
    }
    // container-level serde attributes: normalized set equality
    let expected = rec.container.canonical();
    let actual = normalized_snap_set(&snap.serde);
    if expected != actual {
        fail(
            name,
            "container serde attributes",
            &format!("exactly {expected:?}"),
            &format!("exactly {actual:?} ({})", sets_diff(&expected, &actual)),
        );
    }
    if snap.kind == "enum" {
        check_variants(name, snap, rec);
    } else {
        // struct / newtype / tuple / unit — all compare their field lists
        check_fields(name, "field list", &snap.fields, &rec.fields);
    }
}

fn check_fields(type_name: &str, position: &str, snap_fields: &[SnapField], src_fields: &[Member]) {
    let snapshot_len = snap_fields.len();
    if snapshot_len > src_fields.len() {
        fail(
            type_name,
            &format!("{position} field list length"),
            &format!(
                "at least {snapshot_len} fields (if fields were APPENDED in source, the \
                 snapshot must be regenerated into the snapshot deliberately)"
            ),
            &format!("only {} fields", src_fields.len()),
        );
    }
    for (i, snap_field) in snap_fields.iter().enumerate() {
        let src = &src_fields[i];
        let position = &format!("{position} field #{i}");
        if !name_matches(&src.name, &snap_field.name, &src.flags) {
            fail(
                type_name,
                position,
                &format!("field named `{}`", snap_field.name),
                &format!("field named `{}`", src.name),
            );
        }
        // effective wire name: scanner-computed vs pinned
        if src.wire != snap_field.wire {
            fail(
                type_name,
                position,
                &format!("wire name `{}`", snap_field.wire),
                &format!(
                    "wire name `{}` (rename/rename_all computed from source)",
                    src.wire
                ),
            );
        }
        // member type as written in source
        if src.ty != snap_field.ty {
            fail(
                type_name,
                position,
                &format!("type `{}`", snap_field.ty),
                &format!("type `{}`", src.ty),
            );
        }
        // full wire-shape attribute set: normalized set equality
        let expected = src.flags.canonical();
        let actual = normalized_snap_set(&snap_field.serde);
        if expected != actual {
            fail(
                type_name,
                &format!("{position} (`{}`) serde attributes", snap_field.name),
                &format!("exactly {expected:?}"),
                &format!("exactly {actual:?} ({})", sets_diff(&expected, &actual)),
            );
        }
    }
    // source entries after the snapshot's last field: append-only, allowed
}

fn check_variants(type_name: &str, snap: &SnapshotType, rec: &ScanEntry) {
    let snapshot_len = snap.variants.len();
    if snapshot_len > rec.variants.len() {
        fail(
            type_name,
            "variant list length",
            &format!(
                "at least {snapshot_len} variants (if variants were APPENDED in source, the \
                 snapshot must be regenerated into the snapshot deliberately)"
            ),
            &format!("only {} variants", rec.variants.len()),
        );
    }
    for (i, snap_variant) in snap.variants.iter().enumerate() {
        let src = &rec.variants[i];
        let position = &format!("variant #{i}");
        if !name_matches(&src.name, &snap_variant.name, &src.flags) {
            fail(
                type_name,
                position,
                &format!("variant named `{}`", snap_variant.name),
                &format!("variant named `{}`", src.name),
            );
        }
        if src.wire != snap_variant.wire {
            fail(
                type_name,
                &format!("{position} (`{}`) wire name", snap_variant.name),
                &format!("wire name `{}`", snap_variant.wire),
                &format!(
                    "wire name `{}` (rename/rename_all computed from source)",
                    src.wire
                ),
            );
        }
        if src.shape != snap_variant.shape {
            fail(
                type_name,
                &format!("{position} (`{}`) shape", snap_variant.name),
                &format!("shape {}", snap_variant.shape),
                &format!("shape {}", src.shape),
            );
        }
        let expected = src.flags.canonical();
        let actual = normalized_snap_set(&snap_variant.serde);
        if expected != actual {
            fail(
                type_name,
                &format!("{position} (`{}`) serde attributes", snap_variant.name),
                &format!("exactly {expected:?}"),
                &format!("exactly {actual:?} ({})", sets_diff(&expected, &actual)),
            );
        }
        // variant payload fields, same positional + wire-name + attr rules
        check_fields(
            type_name,
            &format!("variant #{} (`{}`)", i, snap_variant.name),
            &snap_variant.fields,
            &src.fields,
        );
    }
}

/// CHECK 3 — hand-written impls: existence only for the positional check
/// (their wire shape comes from the manual impl), plus CHECK 2 above.
fn check_hand_written_exist(c: &Corpus) {
    for (name, snap) in &c.snapshot.types {
        if snap.crate_name != "shamir-query-types" || snap.impl_kind != "hand_written" {
            continue;
        }
        if !c.scan.contains_key(name) {
            panic!(
                "hand-written wire type {name} is in the snapshot but no longer defined \
                 in the crate — protocol version change required"
            );
        }
    }
}

#[test]
fn snapshot_order_pins_declaration_order_of_every_derived_wire_type() {
    let c = corpus();
    check_snapshot_sanity(c);
    for (name, snap) in &c.snapshot.types {
        if snap.crate_name != "shamir-query-types" {
            continue;
        }
        if snap.impl_kind == "hand_written" {
            continue; // CHECK 3 covers existence; positional comparison skipped
        }
        let rec = c.scan.get(name).unwrap_or_else(|| {
            panic!(
                "wire type {name} is in the snapshot but no longer defined in the crate — \
                 protocol version change required"
            )
        });
        check_one_type(name, snap, rec);
    }
}

#[test]
fn snapshot_is_complete_no_silent_new_wire_type() {
    let c = corpus();
    check_snapshot_sanity(c);
    check_completeness(c);
}

#[test]
fn snapshot_hand_written_types_still_exist_in_the_crate() {
    let c = corpus();
    check_snapshot_sanity(c);
    check_hand_written_exist(c);
}

// ---------------------------------------------------------------------------
// Unit tests for the completeness traversal itself
// ---------------------------------------------------------------------------

#[test]
fn completeness_traversal_fails_on_a_missing_type() {
    // A → B → Missing: the closure MUST surface "Missing", so the
    // production completeness check would fail on this graph.
    let mut defs = BTreeMap::new();
    defs.insert("A".to_string(), vec!["B".to_string()]);
    defs.insert("B".to_string(), vec!["Missing".to_string()]);
    let reached = reachable(&["A"], &defs);
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
fn completeness_traversal_on_a_fully_covered_graph_yields_only_defined_names() {
    // A → [B, Vec<B>], B → () — every reached name is defined; the closure
    // must terminate on a self-referential graph too.
    let mut defs = BTreeMap::new();
    defs.insert("A".to_string(), vec!["Vec<B>".to_string(), "B".to_string()]);
    defs.insert("B".to_string(), vec!["(A, u32)".to_string()]);
    let reached = reachable(&["A"], &defs);
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
}

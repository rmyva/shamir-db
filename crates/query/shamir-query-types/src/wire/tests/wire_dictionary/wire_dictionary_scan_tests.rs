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
//!   Starting from the FIXED roots ["DbRequest", "DbResponse"] (never the
//!   snapshot's own roots list — a corrupted list cannot shrink the
//!   traversal) the scan map is closed over member types via
//!   `type_graph_edges` (every generic argument, tuple element, array/slice
//!   element at any depth resolves; crate type aliases expand) and every
//!   reached crate-defined type must be a snapshot key or a documented
//!   exclusion. A reachable type missing from the snapshot fails REGARDLESS
//!   of impl kind — derived OR hand_written — so deleting a snapshot entry
//!   for a still-reachable type always fails here.
//! - CHECK 3 (hand-written impls): snapshot entries with `impl ==
//!   "hand_written"` are skipped by CHECK 1's positional comparison — their
//!   fields come from a manual serde impl, not from a derive, so the
//!   declaration order says nothing about the wire. Such types must still
//!   EXIST in the scan and CHECK 2 still applies. NOTE: a hand-written impl
//!   can drift silently in field order because nothing here observes it —
//!   the per-type indexed/named round-trip decode tests in
//!   `wire_dictionary_decode_tests.rs` (and WIRE-DICT-2's adapter tests)
//!   are its guard.
//!
//! External-crate snapshot entries (ByteBuf, TMap, TSet, RecordId,
//! SecretString, QueryValue, Actor) are LISTED but their internal shape is
//! NOT machine-verified: the scanner sees only this crate's source.

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
pub(super) struct Snapshot {
    #[allow(dead_code)]
    pub(super) schema_version: u32,
    pub(super) roots: Vec<String>,
    pub(super) types: BTreeMap<String, SnapshotType>,
}

#[derive(Deserialize, Debug)]
pub(super) struct SnapshotType {
    #[serde(rename = "crate")]
    pub(super) crate_name: String,
    /// OPTIONAL: EXTERNAL-crate entries have no source path in this crate by
    /// definition — the model reflects that instead of inventing paths.
    #[serde(default)]
    #[allow(dead_code)]
    pub(super) path: String,
    pub(super) kind: String,
    #[serde(rename = "impl")]
    pub(super) impl_kind: String,
    #[allow(dead_code)]
    pub(super) verdict: String,
    #[serde(default)]
    #[allow(dead_code)]
    pub(super) reason: String,
    #[serde(default)]
    pub(super) serde: Vec<String>,
    #[serde(default)]
    pub(super) fields: Vec<SnapField>,
    #[serde(default)]
    pub(super) variants: Vec<SnapVariant>,
}

#[derive(Deserialize, Debug)]
pub(super) struct SnapField {
    pub(super) name: String,
    /// effective wire name pinned by the scanner's rename/rename_all rules
    #[serde(default)]
    pub(super) wire: String,
    #[serde(rename = "type", default)]
    pub(super) ty: String,
    #[serde(default)]
    pub(super) serde: Vec<String>,
}

#[derive(Deserialize, Debug)]
pub(super) struct SnapVariant {
    pub(super) name: String,
    #[serde(default)]
    pub(super) wire: String,
    pub(super) shape: String,
    #[serde(default)]
    pub(super) serde: Vec<String>,
    #[serde(default)]
    pub(super) fields: Vec<SnapField>,
}

/// Fixed completeness roots (CHECK 2): the traversal ALWAYS starts here,
/// never from `snapshot.roots`, so a corrupted/trimmed roots list cannot
/// make completeness vacuous. `check_snapshot_sanity` separately asserts
/// the snapshot's own roots list is exactly this set.
pub(super) const FIXED_ROOTS: &[&str] = &["DbRequest", "DbResponse"];

/// Attribute keys the scanner pins (value form `key = "value"`). Order
/// matters: LONGEST / more specific prefixes first, so
/// `rename_serialize` / `rename_deserialize` / `rename_all_fields` /
/// `rename_all` are never mangled by the plain `rename` prefix.
const PINNED_VALUE_ATTRS_IN_ORDER: &[&str] = &[
    "rename_serialize",
    "rename_deserialize",
    "rename_all_fields",
    "rename_all",
    "rename",
    "alias",
    "default",
    "tag",
    "content",
    "from",
    "try_from",
    "into",
    "remote",
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
    "skip",
    "other",
    // bare `default` (no `= path`) is a flag-style entry in the snapshot
    "default",
];

/// Normalize one snapshot `serde` array entry into its canonical form(s).
/// Returns an EMPTY vec for attributes the scanner does not pin (prose
/// notes, `bound`, `crate`, ...). The directional serde form
/// `rename(serialize = "a", deserialize = "b")` normalizes to the two
/// canonical entries `rename_serialize = "a"` and
/// `rename_deserialize = "b"`, mirroring the scanner's flags exactly.
fn normalize_snap_attr(raw: &str) -> Vec<String> {
    let s = raw.trim();
    if s.starts_with("rename(") && s.ends_with(')') {
        let inner = &s["rename(".len()..s.len() - 1];
        let mut out = Vec::new();
        for part in inner.split(',') {
            let part = part.trim();
            let Some((k, v)) = part.split_once('=') else {
                return Vec::new(); // malformed: normalize away, CHECK 1 fails loudly
            };
            let value = v.trim().trim_matches('"');
            match k.trim() {
                "serialize" => out.push(format!("rename_serialize = \"{value}\"")),
                "deserialize" => out.push(format!("rename_deserialize = \"{value}\"")),
                _ => return Vec::new(),
            }
        }
        if out.is_empty() {
            return Vec::new();
        }
        return out;
    }
    for key in PINNED_VALUE_ATTRS_IN_ORDER {
        if let Some(rest) = s.strip_prefix(key) {
            let rest = rest.trim_start();
            if let Some(v) = rest.strip_prefix('=') {
                let value = v.trim().trim_matches('"');
                return vec![format!("{key} = \"{value}\"")];
            }
        }
    }
    for key in PINNED_FLAG_ATTRS {
        if s == *key {
            return vec![key.to_string()];
        }
    }
    Vec::new()
}

fn normalized_snap_set(serde: &[String]) -> BTreeSet<String> {
    serde.iter().flat_map(|s| normalize_snap_attr(s)).collect()
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

pub(super) struct Corpus {
    pub(super) scan: BTreeMap<String, ScanEntry>,
    /// crate type alias name → target type string (`Item::Type`)
    pub(super) aliases: BTreeMap<String, String>,
    pub(super) snapshot: Snapshot,
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
        collect_rs_files(&src_root, &mut files)
            .unwrap_or_else(|e| panic!("source walk failed: {e}"));
        assert!(
            files.len() > 50,
            "scanner found only {} source files under {} — walk is broken",
            files.len(),
            src_root.display()
        );
        let mut scan = BTreeMap::new();
        let mut aliases = BTreeMap::new();
        // serde attribute parse errors are COLLECTED during the walk and
        // must be empty — each message already names its file and item.
        let mut errs = Vec::new();
        for file in files {
            let text = fs::read_to_string(&file)
                .unwrap_or_else(|e| panic!("cannot read {}: {e}", file.display()));
            let ast = syn::parse_file(&text)
                .unwrap_or_else(|e| panic!("cannot parse {}: {e}", file.display()));
            collect_items(&ast.items, &mut scan, &mut aliases, &file, &mut errs);
        }
        assert!(
            errs.is_empty(),
            "malformed serde attributes found during scan:\n{}",
            errs.join("\n")
        );
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

// ---------------------------------------------------------------------------
// Type-graph edge extraction (CHECK 2)
// ---------------------------------------------------------------------------

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

pub(super) const PRIMITIVES: &[&str] = &[
    "u8", "u16", "u32", "u64", "u128", "i8", "i16", "i32", "i64", "i128", "bool", "String", "char",
    "f32", "f64", "usize", "isize",
];

/// Extract EVERY named type argument / tuple element reachable inside a
/// member type string, recursively:
///
/// - `Vec<(A, Missing)>` → `[A, Missing]` (tuple elements at any depth)
/// - `TMap<A, String>` → `[A, String]` (ALL generic arguments, not just the
///   last); `Option<T>` / `Box<T>` / `HashMap<K, V>` likewise resolve every
///   argument. Generic CONTAINER names themselves are not returned (they
///   are std/external wrappers, never wire keys); only a non-generic path
///   yields itself, so `ByteBuf` → `[ByteBuf]`.
/// - slices/arrays `[T]` / `[T; N]` → `[T]`'s element
/// - lifetimes and const generic arguments carry no wire types and are
///   dropped
///
/// An UNSUPPORTED (`#unsupported-type#`) or MALFORMED type string is an
/// `Err` naming the type — check_completeness fails loudly with the field
/// context, it never silently skips.
pub(super) fn type_graph_edges(ty: &str) -> Result<Vec<String>, String> {
    let t = ty.trim();
    if t.is_empty() {
        return Err("empty type string".to_string());
    }
    if t.contains("#unsupported") {
        return Err(format!("unsupported type string `{t}`"));
    }
    if let Some(rest) = t.strip_prefix('&') {
        let rest = rest.trim_start();
        let rest = rest.strip_prefix("mut ").unwrap_or(rest).trim_start();
        return type_graph_edges(rest);
    }
    if let Some(rest) = t.strip_prefix('[') {
        // slice `[T]` or array `[T; N]`
        let inner = match rest.find(';') {
            Some(semi) => &rest[..semi],
            None => match rest.strip_suffix(']') {
                Some(x) => x,
                None => return Err(format!("malformed array/slice type `{t}`")),
            },
        };
        return type_graph_edges(inner.trim());
    }
    if t.starts_with('(') && t.ends_with(')') {
        let mut out = Vec::new();
        for el in split_top_level(&t[1..t.len() - 1]) {
            out.extend(type_graph_edges(&el)?);
        }
        return Ok(out);
    }
    // split trailing generic arguments off the base path
    match t.find('<') {
        Some(open) => {
            if !t.ends_with('>') {
                return Err(format!("malformed generic type `{t}` (unbalanced <>)"));
            }
            let mut out = Vec::new();
            for arg in split_top_level(&t[open + 1..t.len() - 1]) {
                // strip associated-type bindings `Item = T`
                let arg = match arg.split_once('=') {
                    Some((lhs, rhs)) if !lhs.contains('<') => rhs.trim(),
                    _ => arg.as_str(),
                };
                // lifetimes and const generic args carry no wire types
                if arg.starts_with('\'') || arg.chars().next().is_some_and(|c| c.is_ascii_digit()) {
                    continue;
                }
                out.extend(type_graph_edges(arg)?);
            }
            Ok(out)
        }
        None => {
            let name = t.rsplit("::").next().unwrap_or(t).trim();
            if name.is_empty() {
                Err(format!("malformed type `{t}` (no name)"))
            } else {
                Ok(vec![name.to_string()])
            }
        }
    }
}

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

/// Pure reachability closure over `defs` (type name → (field label, member
/// type string) pairs; crate type aliases map to a single target). Returns
/// every name transitively reachable from `roots`, including the roots, or
/// an `Err` naming the type AND field of any unsupported/malformed member
/// type string.
///
/// Termination: a `visited` set guards the worklist.
///
/// `QueryValue` is a TERMINAL node: its hand-written serde impl is
/// self-describing (raw scalars, sequences and maps — no per-field keys),
/// so its members are deliberately kept out of the closure.
pub(super) fn reachable(
    roots: &[&str],
    defs: &BTreeMap<String, Vec<(String, String)>>,
) -> Result<BTreeSet<String>, String> {
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
            for (label, member) in members {
                for edge in
                    type_graph_edges(member).map_err(|e| format!("in {name} ({label}): {e}"))?
                {
                    if !visited.contains(&edge) {
                        worklist.push(edge);
                    }
                }
            }
        }
    }
    Ok(visited)
}

// ---------------------------------------------------------------------------
// Checks (Result-returning; thin panicking wrappers live in the #[test]s)
// ---------------------------------------------------------------------------

/// CHECK 0 — snapshot sanity: the roots list must be non-empty and must be
/// EXACTLY the fixed wire roots, and every in-crate snapshot type must
/// exist in the scan.
pub(super) fn check_snapshot_sanity(c: &Corpus) -> Result<(), String> {
    if c.snapshot.roots.is_empty() {
        return Err(
            "snapshot roots list is empty — wire-dictionary.json is corrupted — \
             protocol version change required"
                .to_string(),
        );
    }
    let root_set: BTreeSet<&str> = c.snapshot.roots.iter().map(|s| s.as_str()).collect();
    let required: BTreeSet<&str> = FIXED_ROOTS.iter().copied().collect();
    if root_set != required {
        return Err(format!(
            "snapshot roots must be exactly {required:?}, got {root_set:?} — \
             wire-dictionary.json is corrupted — protocol version change required"
        ));
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
        if !c.scan.contains_key(name) && !c.aliases.contains_key(name) {
            return Err(format!(
                "snapshot type {name} is neither scanned nor a known crate alias — \
                 snapshot and scanner disagree on what this crate defines — \
                 protocol version change required"
            ));
        }
    }
    Ok(())
}

/// CHECK 2 — real completeness from the FIXED roots, computed in the
/// SOURCE: every crate-defined type reachable from DbRequest/DbResponse
/// must be a snapshot key or a documented exclusion. A name that a scan of
/// this crate finds as a struct/enum but that has no snapshot entry is a
/// NEW wire type and fails loudly. This ALSO covers a DELETED snapshot
/// entry whose type is still reachable: the check fails regardless of the
/// entry's impl kind (derived or hand_written).
pub(super) fn check_completeness(c: &Corpus) -> Result<(), String> {
    let mut defs: BTreeMap<String, Vec<(String, String)>> = BTreeMap::new();
    for (name, entry) in &c.scan {
        let mut members = entry.member_types();
        // `#[serde(from = "T")]` conversion targets are part of the type's
        // wire surface: serde deserializes T, so T's members are pinned too
        // (e.g. FieldRuleDto deserializes FieldRuleWire).
        if let Some(target) = &entry.container.from {
            members.push(("from target".to_string(), target.clone()));
        }
        defs.insert(name.clone(), members);
    }
    // crate type aliases expand to their target's members
    for (alias, target) in &c.aliases {
        defs.insert(
            alias.clone(),
            vec![("type alias target".to_string(), target.clone())],
        );
    }
    let reached =
        reachable(FIXED_ROOTS, &defs).map_err(|e| format!("completeness traversal failed: {e}"))?;
    for name in reached {
        if c.snapshot.types.contains_key(&name)
            || PRIMITIVES.contains(&name.as_str())
            || EXCLUDED_TYPE_NAMES.contains(&name.as_str())
        {
            continue;
        }
        return Err(format!(
            "new wire type {name} reachable from the fixed roots {FIXED_ROOTS:?} but missing \
             from wire-dictionary.json — add it or exclude it deliberately{}",
            if c.scan.contains_key(&name) {
                " (found in this crate by the source scanner)".to_string()
            } else {
                // external-crate types are LISTED in the snapshot but their
                // internal shape is NOT machine-verified by this scan
                " (external crate type — LISTED in the snapshot but its internal \
                 shape is NOT machine-verified)"
                    .to_string()
            }
        ));
    }
    Ok(())
}

/// CHECK 1 name comparison is rename-aware: a snapshot field name matches
/// either the Rust identifier or any of its `rename`/`alias` values.
fn name_matches(source: &str, snapshot: &str, flags: &SerdeFlags) -> bool {
    source == snapshot || flags.has_rename(snapshot)
}

fn fail(type_name: &str, position: &str, expected: &str, actual: &str) -> Result<(), String> {
    Err(format!(
        "wire type {type_name} at {position}: expected {expected}, actual {actual} — \
         protocol version change required"
    ))
}

/// CHECK 1 — order pin for one derived type.
fn check_one_type(name: &str, snap: &SnapshotType, rec: &ScanEntry) -> Result<(), String> {
    if rec.kind != snap.kind {
        return fail(
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
        return fail(
            name,
            "container serde attributes",
            &format!("exactly {expected:?}"),
            &format!("exactly {actual:?} ({})", sets_diff(&expected, &actual)),
        );
    }
    if snap.kind == "enum" {
        check_variants(name, snap, rec)
    } else {
        // struct / newtype / tuple / unit — all compare their field lists
        check_fields(name, "field list", &snap.fields, &rec.fields)
    }
}

fn check_fields(
    type_name: &str,
    position: &str,
    snap_fields: &[SnapField],
    src_fields: &[Member],
) -> Result<(), String> {
    let snapshot_len = snap_fields.len();
    if snapshot_len > src_fields.len() {
        return fail(
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
            return fail(
                type_name,
                position,
                &format!("field named `{}`", snap_field.name),
                &format!("field named `{}`", src.name),
            );
        }
        // effective wire name: scanner-computed vs pinned
        if src.wire != snap_field.wire {
            return fail(
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
            return fail(
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
            return fail(
                type_name,
                &format!("{position} (`{}`) serde attributes", snap_field.name),
                &format!("exactly {expected:?}"),
                &format!("exactly {actual:?} ({})", sets_diff(&expected, &actual)),
            );
        }
    }
    // source entries after the snapshot's last field: append-only, allowed
    Ok(())
}

fn check_variants(type_name: &str, snap: &SnapshotType, rec: &ScanEntry) -> Result<(), String> {
    let snapshot_len = snap.variants.len();
    if snapshot_len > rec.variants.len() {
        return fail(
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
            return fail(
                type_name,
                position,
                &format!("variant named `{}`", snap_variant.name),
                &format!("variant named `{}`", src.name),
            );
        }
        if src.wire != snap_variant.wire {
            return fail(
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
            return fail(
                type_name,
                &format!("{position} (`{}`) shape", snap_variant.name),
                &format!("shape {}", snap_variant.shape),
                &format!("shape {}", src.shape),
            );
        }
        let expected = src.flags.canonical();
        let actual = normalized_snap_set(&snap_variant.serde);
        if expected != actual {
            return fail(
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
        )?;
    }
    Ok(())
}

/// CHECK 3 — hand-written impls: existence only for the positional check
/// (their wire shape comes from the manual impl), plus CHECK 2 above.
fn check_hand_written_exist(c: &Corpus) -> Result<(), String> {
    for (name, snap) in &c.snapshot.types {
        if snap.crate_name != "shamir-query-types" || snap.impl_kind != "hand_written" {
            continue;
        }
        if !c.scan.contains_key(name) {
            return Err(format!(
                "hand-written wire type {name} is in the snapshot but no longer defined \
                 in the crate — protocol version change required"
            ));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// The tests themselves
// ---------------------------------------------------------------------------

#[test]
fn snapshot_order_pins_declaration_order_of_every_derived_wire_type() {
    let c = corpus();
    check_snapshot_sanity(c).unwrap_or_else(|e| panic!("{e}"));
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
        check_one_type(name, snap, rec).unwrap_or_else(|e| panic!("{e}"));
    }
}

#[test]
fn snapshot_is_complete_no_silent_new_wire_type() {
    let c = corpus();
    check_snapshot_sanity(c).unwrap_or_else(|e| panic!("{e}"));
    check_completeness(c).unwrap_or_else(|e| panic!("{e}"));
}

#[test]
fn snapshot_hand_written_types_still_exist_in_the_crate() {
    let c = corpus();
    check_snapshot_sanity(c).unwrap_or_else(|e| panic!("{e}"));
    check_hand_written_exist(c).unwrap_or_else(|e| panic!("{e}"));
}

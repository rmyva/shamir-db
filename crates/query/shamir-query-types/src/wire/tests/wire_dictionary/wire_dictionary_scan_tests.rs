//! Wire-dictionary pinning test (WIRE-DICT-1, design
//! `docs/dev-artifacts/design/wire-keyword-dictionary.md` §3.2).
//!
//! The protocol dictionary IS the declaration order of every wire struct
//! field and enum variant. This test rebuilds that table straight from the
//! crate source with a `syn` scanner and compares it against the committed
//! snapshot `docs/guide-docs/client-server-protocol-spec/wire-dictionary.json`:
//!
//! - CHECK 1 (order pin): every derived snapshot type for this crate must
//!   still exist with the same field/variant names and the same critical
//!   serde flags at the same positions. Anything else (reorder, rename,
//!   removal, flag change) fails with "protocol version change required".
//!   Source entries AFTER the snapshot's last entry are allowed (the
//!   append-only rule) and do not fail.
//! - CHECK 2 (completeness): every type name appearing in a snapshot
//!   field/variant `type` string must be a snapshot key, a primitive, or a
//!   documented exclusion. A new wire DTO reachable from the snapshot but
//!   missing from it fails, so a future new wire type cannot pass silently.
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
use syn::Item;

// ---------------------------------------------------------------------------
// Normalized serde attribute model (no raw strings cross the comparisons)
// ---------------------------------------------------------------------------

/// Normalized serde flags that matter for the wire dictionary.
#[derive(Default, Debug, Clone)]
struct SerdeFlags {
    default: bool,
    flatten: bool,
    skip_serializing: bool,
    skip_deserializing: bool,
    deny_unknown_fields: bool,
    untagged: bool,
    transparent: bool,
    /// container `tag = "..."` value
    tag: Option<String>,
    /// container `content = "..."` value
    content: Option<String>,
    /// every `rename = "..."` / `alias = "..."` value (rename-aware matching)
    renames: Vec<String>,
}

impl SerdeFlags {
    fn has_rename(&self, name: &str) -> bool {
        self.renames.iter().any(|rename| rename == name)
    }

    fn from_attrs(attrs: &[syn::Attribute]) -> SerdeFlags {
        let mut flags = SerdeFlags::default();
        for attr in attrs {
            if !attr.path().is_ident("serde") {
                continue;
            }
            let _ = attr.parse_nested_meta(|meta| {
                let name = meta
                    .path
                    .get_ident()
                    .map(|i| i.to_string())
                    .unwrap_or_default();
                match name.as_str() {
                    "default" => {
                        // `default` with or without `= path`
                        if meta.input.peek(syn::Token![=]) {
                            let _: syn::Expr = meta.value()?.parse()?;
                        }
                        flags.default = true;
                    }
                    "flatten" => flags.flatten = true,
                    "skip_serializing" => flags.skip_serializing = true,
                    "skip_deserializing" => flags.skip_deserializing = true,
                    "deny_unknown_fields" => flags.deny_unknown_fields = true,
                    "untagged" => flags.untagged = true,
                    "transparent" => flags.transparent = true,
                    "tag" => {
                        let lit: syn::LitStr = meta.value()?.parse()?;
                        flags.tag = Some(lit.value());
                    }
                    "content" => {
                        let lit: syn::LitStr = meta.value()?.parse()?;
                        flags.content = Some(lit.value());
                    }
                    "rename" | "alias" => {
                        let lit: syn::LitStr = meta.value()?.parse()?;
                        flags.renames.push(lit.value());
                    }
                    // rename_all / with / deserialize_with / serialize_with /
                    // skip_serializing_if / serde(with = ..) / from / try_from /
                    // into / bounded explicit paths: normalized away — they are
                    // index-safe and not pinned by this test.
                    _ => {
                        if meta.input.peek(syn::Token![=]) {
                            let _: syn::Expr = meta.value()?.parse()?;
                        }
                    }
                }
                Ok(())
            });
        }
        flags
    }
}

/// One struct field (or one unnamed field, name = its index as string).
#[derive(Debug, Clone)]
struct Member {
    name: String,
    flags: SerdeFlags,
}

/// One enum variant, with its shape and (for struct/tuple shapes) fields.
#[derive(Debug, Clone)]
struct VariantRec {
    name: String,
    shape: &'static str, // "unit" | "newtype" | "tuple" | "struct"
    flags: SerdeFlags,
    fields: Vec<Member>,
}

/// One scanned struct or enum, keyed by its base type name (before `<`).
#[derive(Debug, Clone)]
struct ScanEntry {
    kind: &'static str, // "struct" | "enum"
    container: SerdeFlags,
    fields: Vec<Member>,
    variants: Vec<VariantRec>,
}

fn fields_to_members(fields: &syn::Fields) -> Vec<Member> {
    match fields {
        syn::Fields::Named(named) => named
            .named
            .iter()
            .map(|f| Member {
                name: f.ident.as_ref().unwrap().to_string(),
                flags: SerdeFlags::from_attrs(&f.attrs),
            })
            .collect(),
        syn::Fields::Unnamed(unnamed) => unnamed
            .unnamed
            .iter()
            .enumerate()
            .map(|(i, f)| Member {
                name: i.to_string(),
                flags: SerdeFlags::from_attrs(&f.attrs),
            })
            .collect(),
        syn::Fields::Unit => Vec::new(),
    }
}

fn variant_shape(fields: &syn::Fields) -> &'static str {
    match fields {
        syn::Fields::Unit => "unit",
        syn::Fields::Unnamed(u) if u.unnamed.len() == 1 => "newtype",
        syn::Fields::Unnamed(_) => "tuple",
        syn::Fields::Named(_) => "struct",
    }
}

fn collect_items(items: &[Item], out: &mut BTreeMap<String, ScanEntry>) {
    for item in items {
        match item {
            Item::Struct(s) => {
                let members = fields_to_members(&s.fields);
                out.entry(s.ident.to_string()).or_insert(ScanEntry {
                    kind: "struct",
                    container: SerdeFlags::from_attrs(&s.attrs),
                    fields: members,
                    variants: Vec::new(),
                });
            }
            Item::Enum(e) => {
                let variants = e
                    .variants
                    .iter()
                    .map(|v| VariantRec {
                        name: v.ident.to_string(),
                        shape: variant_shape(&v.fields),
                        flags: SerdeFlags::from_attrs(&v.attrs),
                        fields: fields_to_members(&v.fields),
                    })
                    .collect();
                out.entry(e.ident.to_string()).or_insert(ScanEntry {
                    kind: "enum",
                    container: SerdeFlags::from_attrs(&e.attrs),
                    fields: Vec::new(),
                    variants,
                });
            }
            // Inline modules: recurse so no type definition is missed.
            Item::Mod(m) => {
                if let Some((_, inner)) = &m.content {
                    collect_items(inner, out);
                }
            }
            _ => {}
        }
    }
}

/// Recursively collect every `*.rs` file under `root`, skipping any
/// directory named `tests` (test modules do not define wire types).
fn collect_rs_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if path.file_name().and_then(|n| n.to_str()) == Some("tests") {
                continue;
            }
            collect_rs_files(&path, out);
        } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
            out.push(path);
        }
    }
}

// ---------------------------------------------------------------------------
// Typed snapshot model (schema_version 1)
// ---------------------------------------------------------------------------

#[derive(Deserialize, Debug)]
struct Snapshot {
    #[allow(dead_code)]
    schema_version: u32,
    #[allow(dead_code)]
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
    #[serde(rename = "type", default)]
    #[allow(dead_code)]
    ty: String,
    #[serde(default)]
    serde: Vec<String>,
}

#[derive(Deserialize, Debug)]
struct SnapVariant {
    name: String,
    shape: String,
    #[serde(default)]
    serde: Vec<String>,
    #[serde(default)]
    fields: Vec<SnapField>,
}

impl SnapshotType {
    /// The container `tag = "..."` value recorded in the snapshot, if any.
    fn tag(&self) -> Option<String> {
        self.serde.iter().find_map(|s| {
            s.strip_prefix("tag = ")
                .map(|v| v.trim_matches('"').to_string())
        })
    }

    fn has_untagged(&self) -> bool {
        self.serde.iter().any(|s| s == "untagged")
    }
}

impl SnapField {
    /// Critical, index-affecting flags recorded for this field.
    /// (`default`, `skip_serializing_if`, `rename`, `with`,
    /// `deserialize_with` are deliberately NOT pinned: they do not shift
    /// the u64 field index mapping.)
    fn critical_flags(&self) -> (bool, bool, bool) {
        (
            self.serde.iter().any(|s| s == "flatten"),
            self.serde.iter().any(|s| s == "skip_serializing"),
            self.serde.iter().any(|s| s == "skip_deserializing"),
        )
    }
}

impl SnapVariant {
    fn critical_flags(&self) -> (bool, bool, bool) {
        (
            self.serde.iter().any(|s| s == "flatten"),
            self.serde.iter().any(|s| s == "skip_serializing"),
            self.serde.iter().any(|s| s == "skip_deserializing"),
        )
    }
}

// ---------------------------------------------------------------------------
// Shared corpus (parsed once, shared by the three checks)
// ---------------------------------------------------------------------------

struct Corpus {
    scan: BTreeMap<String, ScanEntry>,
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
        for file in files {
            let text = fs::read_to_string(&file)
                .unwrap_or_else(|e| panic!("cannot read {}: {e}", file.display()));
            let ast = syn::parse_file(&text)
                .unwrap_or_else(|e| panic!("cannot parse {}: {e}", file.display()));
            collect_items(&ast.items, &mut scan);
        }
        let text = fs::read_to_string(snapshot_path()).expect("snapshot JSON is readable");
        let snapshot: Snapshot = serde_json::from_str(&text).expect("snapshot JSON parses");
        Corpus { scan, snapshot }
    })
}

/// Strip `Option<>` / `Vec<>` / `Box<>` / `HashMap<K, V>` / `TMap<K, V>` /
/// generic parameters / whitespace / crate paths; take the last path
/// segment. Returns `None` for fixed arrays `[u8; 16]` (self-describing,
/// keyless) and tuples `(A, B)` (also keyless).
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
    // array on the wire, no keys.
    "FieldPath",
    // shamir-types internals behind the hand-written QueryValue impl; both
    // travel as strings on the wire ("serialized as string" notes).
    "Decimal",
    "BigInt",
    // Generic parameter of `Value<Key>` inside the QueryValue snapshot
    // entry — a type parameter, not a wire type.
    "Key",
];

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
    // critical container flags: tag / untagged
    if rec.container.tag != snap.tag() {
        fail(
            name,
            "container tag",
            &format!("tag = {:?}", snap.tag()),
            &format!("tag = {:?}", rec.container.tag),
        );
    }
    if rec.container.untagged != snap.has_untagged() {
        fail(
            name,
            "container untagged",
            &format!("untagged = {}", snap.has_untagged()),
            &format!("untagged = {}", rec.container.untagged),
        );
    }
    match rec.kind {
        "struct" => {
            let snapshot_len = snap.fields.len();
            if snapshot_len > rec.fields.len() {
                fail(
                    name,
                    "field list length",
                    &format!("at least {snapshot_len} fields"),
                    &format!("only {} fields", rec.fields.len()),
                );
            }
            for (i, snap_field) in snap.fields.iter().enumerate() {
                let src = &rec.fields[i];
                if !name_matches(&src.name, &snap_field.name, &src.flags) {
                    fail(
                        name,
                        &format!("field #{i}"),
                        &format!("field named `{}`", snap_field.name),
                        &format!("field named `{}`", src.name),
                    );
                }
                let (s_flat, s_skip_ser, s_skip_de) = snap_field.critical_flags();
                if (s_flat, s_skip_ser, s_skip_de)
                    != (
                        src.flags.flatten,
                        src.flags.skip_serializing,
                        src.flags.skip_deserializing,
                    )
                {
                    fail(
                        name,
                        &format!("field #{i} (`{}`) critical serde flags", snap_field.name),
                        &format!(
                            "(flatten={s_flat}, skip_serializing={s_skip_ser}, skip_deserializing={s_skip_de})"
                        ),
                        &format!(
                            "(flatten={}, skip_serializing={}, skip_deserializing={})",
                            src.flags.flatten, src.flags.skip_serializing, src.flags.skip_deserializing
                        ),
                    );
                }
            }
            // source entries after the snapshot's last field: append-only, allowed
        }
        "enum" => {
            let snapshot_len = snap.variants.len();
            if snapshot_len > rec.variants.len() {
                fail(
                    name,
                    "variant list length",
                    &format!("at least {snapshot_len} variants"),
                    &format!("only {} variants", rec.variants.len()),
                );
            }
            for (i, snap_variant) in snap.variants.iter().enumerate() {
                let src = &rec.variants[i];
                if !name_matches(&src.name, &snap_variant.name, &src.flags) {
                    fail(
                        name,
                        &format!("variant #{i}"),
                        &format!("variant named `{}`", snap_variant.name),
                        &format!("variant named `{}`", src.name),
                    );
                }
                if src.shape != snap_variant.shape {
                    fail(
                        name,
                        &format!("variant #{} (`{}`) shape", i, snap_variant.name),
                        &format!("shape {}", snap_variant.shape),
                        &format!("shape {}", src.shape),
                    );
                }
                let (s_flat, s_skip_ser, s_skip_de) = snap_variant.critical_flags();
                if (s_flat, s_skip_ser, s_skip_de)
                    != (
                        src.flags.flatten,
                        src.flags.skip_serializing,
                        src.flags.skip_deserializing,
                    )
                {
                    fail(
                        name,
                        &format!("variant #{} (`{}`) critical serde flags", i, snap_variant.name),
                        &format!(
                            "(flatten={s_flat}, skip_serializing={s_skip_ser}, skip_deserializing={s_skip_de})"
                        ),
                        &format!(
                            "(flatten={}, skip_serializing={}, skip_deserializing={})",
                            src.flags.flatten, src.flags.skip_serializing, src.flags.skip_deserializing
                        ),
                    );
                }
                // variant payload fields, same positional + rename-aware rules
                let fields_len = snap_variant.fields.len();
                if fields_len > src.fields.len() {
                    fail(
                        name,
                        &format!("variant #{} (`{}`) field list length", i, snap_variant.name),
                        &format!("at least {fields_len} fields"),
                        &format!("only {} fields", src.fields.len()),
                    );
                }
                for (j, snap_field) in snap_variant.fields.iter().enumerate() {
                    let src_field = &src.fields[j];
                    if !name_matches(&src_field.name, &snap_field.name, &src_field.flags) {
                        fail(
                            name,
                            &format!("variant #{} (`{}`) field #{j}", i, snap_variant.name),
                            &format!("field named `{}`", snap_field.name),
                            &format!("field named `{}`", src_field.name),
                        );
                    }
                    let (f_flat, f_skip_ser, f_skip_de) = snap_field.critical_flags();
                    if (f_flat, f_skip_ser, f_skip_de)
                        != (
                            src_field.flags.flatten,
                            src_field.flags.skip_serializing,
                            src_field.flags.skip_deserializing,
                        )
                    {
                        fail(
                            name,
                            &format!(
                                "variant #{} (`{}`) field #{j} (`{}`) critical serde flags",
                                i, snap_variant.name, snap_field.name
                            ),
                            &format!(
                                "(flatten={f_flat}, skip_serializing={f_skip_ser}, skip_deserializing={f_skip_de})"
                            ),
                            &format!(
                                "(flatten={}, skip_serializing={}, skip_deserializing={})",
                                src_field.flags.flatten,
                                src_field.flags.skip_serializing,
                                src_field.flags.skip_deserializing
                            ),
                        );
                    }
                }
            }
        }
        other => panic!("snapshot kind `{other}` for {name} is not struct/enum"),
    }
}

/// CHECK 2 — every type name reachable from the snapshot must be pinned
/// (a snapshot key), primitive, or deliberately excluded. A name that a
/// scan of this crate finds as a struct/enum but that has no snapshot
/// entry is a NEW wire type and fails loudly.
fn check_completeness(c: &Corpus) {
    let mut reachable = BTreeSet::new();
    for snap in c.snapshot.types.values() {
        for field in &snap.fields {
            if let Some(n) = base_type_name(&field.ty) {
                reachable.insert(n);
            }
        }
        for variant in &snap.variants {
            for field in &variant.fields {
                if let Some(n) = base_type_name(&field.ty) {
                    reachable.insert(n);
                }
            }
        }
    }
    for name in reachable {
        if c.snapshot.types.contains_key(&name)
            || PRIMITIVES.contains(&name.as_str())
            || EXCLUDED_TYPE_NAMES.contains(&name.as_str())
        {
            continue;
        }
        panic!(
            "new wire type {name} reachable from the snapshot but missing from \
             wire-dictionary.json — add it or exclude it deliberately{}",
            if c.scan.contains_key(&name) {
                " (found in this crate by the source scanner)"
            } else {
                " (external crate type)"
            }
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
    check_completeness(c);
}

#[test]
fn snapshot_hand_written_types_still_exist_in_the_crate() {
    let c = corpus();
    check_hand_written_exist(c);
}

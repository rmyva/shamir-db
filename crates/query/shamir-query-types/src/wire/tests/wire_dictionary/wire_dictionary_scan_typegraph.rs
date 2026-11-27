//! syn::Type-based completeness walk for the wire-dictionary pinning test
//! (WIRE-DICT-1, CHECK 2). Split out of `wire_dictionary_scan_tests.rs`
//! (file size cap): the transparent-wrapper allowlist, the per-head edge
//! rules, ambiguous-name disambiguation and the `reachable_syn` closure.

use std::collections::BTreeSet;
use std::path::PathBuf;

use syn::ext::IdentExt;

use super::wire_dictionary_scan_tests::{Corpus, SnapshotType, PRIMITIVES};

/// Transparent-wrapper / container heads the completeness walk DOES NOT
/// treat as wire names: it recurses into ALL their type arguments instead.
/// (`&T`, slices `[T]`, arrays `[T; N]` and tuples `(A, B, ...)` are handled
/// structurally by the syn walker; `()` contributes nothing and must NOT
/// fail.)
const WRAPPER_HEADS: &[&str] = &["Box", "Arc", "Rc", "Option", "Vec", "VecDeque"];

/// Map/set heads: the head name IS an edge (the container has its own
/// snapshot entry, e.g. TMap/TSet) AND all type arguments are followed.
const MAPSET_HEADS: &[&str] = &[
    "TMap", "TSet", "HashMap", "BTreeMap", "HashSet", "BTreeSet", "IndexMap", "IndexSet",
];

/// syn::Type-based completeness walk (replaces the string-based
/// `type_graph_edges` for CHECK 2; the string version is retained for the
/// fixture unit tests in `wire_dictionary_scan_fixture_tests.rs`).
///
/// - transparent wrappers (above): recurse into every argument, head not
///   emitted
/// - map/set heads: emit the head as an edge AND recurse into every
///   argument
/// - any OTHER generic head (e.g. `NewDto<String>`, `Alias<String>`): the
///   head is ALWAYS an edge, and must resolve to a snapshot entry, scan
///   entry, alias or primitive — otherwise Err naming the head
/// - lifetimes / const generic args carry no wire types and are dropped
/// - unsupported forms (BareFn, Macro, ImplTrait, Infer, Ptr, Never,
///   TraitObject, QSelf) FAIL CLOSED
pub(super) fn syn_type_edges(
    ty: &syn::Type,
    allowed_heads: &BTreeSet<String>,
) -> Result<Vec<String>, String> {
    match ty {
        syn::Type::Reference(r) => syn_type_edges(&r.elem, allowed_heads),
        syn::Type::Slice(s) => syn_type_edges(&s.elem, allowed_heads),
        syn::Type::Array(a) => syn_type_edges(&a.elem, allowed_heads),
        syn::Type::Paren(p) => syn_type_edges(&p.elem, allowed_heads),
        syn::Type::Group(g) => syn_type_edges(&g.elem, allowed_heads),
        syn::Type::Tuple(t) => {
            // `()` contributes nothing and must NOT fail
            let mut out = Vec::new();
            for el in &t.elems {
                out.extend(syn_type_edges(el, allowed_heads)?);
            }
            Ok(out)
        }
        syn::Type::Path(p) if p.qself.is_none() => path_edges(&p.path, allowed_heads),
        other => Err(format!(
            "unsupported type `{}` (bare Fn / macro / impl Trait / raw pointer / \
             trait object / qself are not wire types)",
            super::wire_dictionary_scan_source::ty_to_string(other)
        )),
    }
}

fn path_edges(path: &syn::Path, allowed_heads: &BTreeSet<String>) -> Result<Vec<String>, String> {
    let last = path
        .segments
        .last()
        .ok_or_else(|| "malformed empty path type".to_string())?;
    let head = last.ident.unraw().to_string();
    match &last.arguments {
        syn::PathArguments::None => Ok(vec![head]),
        syn::PathArguments::AngleBracketed(args) => {
            let mut out = Vec::new();
            let follow_all =
                WRAPPER_HEADS.contains(&head.as_str()) || MAPSET_HEADS.contains(&head.as_str());
            if MAPSET_HEADS.contains(&head.as_str()) {
                out.push(head.clone());
            } else if !follow_all {
                // generic head: ALWAYS an edge, and must resolve
                if !allowed_heads.contains(&head) {
                    return Err(format!(
                        "generic type head `{head}` resolves to no snapshot entry, crate scan \
                         entry, alias or primitive"
                    ));
                }
                out.push(head.clone());
            }
            for arg in &args.args {
                match arg {
                    syn::GenericArgument::Type(t) => {
                        out.extend(syn_type_edges(t, allowed_heads)?);
                    }
                    // lifetimes and const generics carry no wire types
                    syn::GenericArgument::Lifetime(_) | syn::GenericArgument::Const(_) => {}
                    _ => return Err(format!("unsupported generic argument in `{head}<...>`")),
                }
            }
            Ok(out)
        }
        syn::PathArguments::Parenthesized(_) => Err(format!(
            "unsupported Fn-style type `{head}(...)` — not a wire type"
        )),
    }
}

/// Names any generic head may resolve to without failing.
fn allowed_heads(c: &Corpus) -> BTreeSet<String> {
    let mut out: BTreeSet<String> = PRIMITIVES.iter().map(|s| s.to_string()).collect();
    out.extend(c.scan.keys().cloned());
    out.extend(c.aliases.keys().cloned());
    out.extend(c.snapshot.types.keys().cloned());
    out.extend(EXCLUDED_TYPE_NAMES.iter().map(|s| s.to_string()));
    out
}

/// Does the snapshot entry for `name` pin one of `paths`? (ambiguous-name
/// disambiguation: the snapshot `path` is repo-relative, e.g.
/// `src/ops/...`, the scanner's defining paths are absolute.)
fn snapshot_pins_path(t: &SnapshotType, paths: &BTreeSet<PathBuf>) -> bool {
    match &t.path {
        Some(p) => paths
            .iter()
            .any(|def| def.to_string_lossy().ends_with(p.as_str())),
        None => false,
    }
}

/// Pure reachability closure over the corpus with the syn::Type walker.
/// Returns every name transitively reachable from `roots`, including the
/// roots, or an Err naming the type AND field of any unsupported/malformed
/// member type.
///
/// Termination: a `visited` set guards the worklist. `QueryValue` is a
/// TERMINAL node (hand-written self-describing impl — see its doc).
pub(super) fn reachable_syn(c: &Corpus, roots: &[&str]) -> Result<BTreeSet<String>, String> {
    let allowed = allowed_heads(c);
    let mut visited: BTreeSet<String> = BTreeSet::new();
    let mut worklist: Vec<String> = roots.iter().map(|r| r.to_string()).collect();
    while let Some(name) = worklist.pop() {
        if !visited.insert(name.clone()) {
            continue;
        }
        if name == "QueryValue" {
            continue;
        }
        // AMBIGUOUS name: two files define it. Resolution fails unless the
        // snapshot entry pins the qualifying path.
        if let Some(paths) = c.defining.get(&name) {
            if paths.len() > 1 {
                let pinned = c
                    .snapshot
                    .types
                    .get(&name)
                    .map(|t| snapshot_pins_path(t, paths))
                    .unwrap_or(false);
                if !pinned {
                    let listed: Vec<String> =
                        paths.iter().map(|p| p.display().to_string()).collect();
                    return Err(format!(
                        "ambiguous type name `{name}` is defined by multiple files {listed:?} — \
                         the snapshot entry must pin the qualifying `path`"
                    ));
                }
            }
        }
        let push_edges_ty = |label: &str, ty: &syn::Type| -> Result<Vec<String>, String> {
            syn_type_edges(ty, &allowed)
                .map(|edges| {
                    edges
                        .into_iter()
                        .filter(|edge| !visited.contains(edge))
                        .collect()
                })
                .map_err(|e| format!("in {name} ({label}): {e}"))
        };
        let push_edges = |label: &str, ty_src: &str| -> Result<Vec<String>, String> {
            // Parse on demand into a LOCAL: the corpus (in a `static
            // OnceLock`) must stay Send+Sync, and syn types are not.
            let ty: syn::Type = syn::parse_str(ty_src)
                .map_err(|e| format!("in {name} ({label}): cannot parse `{ty_src}`: {e}"))?;
            push_edges_ty(label, &ty)
        };
        if let Some(entry) = c.scan.get(&name) {
            for f in &entry.fields {
                push_edges(&format!("field {}", f.name), &f.ty)
                    .map(|edges| worklist.extend(edges))?;
            }
            for v in &entry.variants {
                for f in &v.fields {
                    push_edges(&format!("variant {}::{}", v.name, f.name), &f.ty)
                        .map(|edges| worklist.extend(edges))?;
                }
            }
            // `#[serde(from = "T")]` conversion targets are part of the
            // wire surface: serde deserializes T, so T is pinned too
            // (e.g. FieldRuleDto deserializes FieldRuleWire).
            if let Some(target) = &entry.container.from {
                let ty: syn::Type = syn::parse_str(target).map_err(|e| {
                    format!("in {name} (from target): cannot parse `{target}`: {e}")
                })?;
                push_edges_ty("from target", &ty).map(|edges| worklist.extend(edges))?;
            }
        }
        // crate type aliases expand to their target's members
        if let Some(target) = c.aliases.get(&name) {
            let ty: syn::Type = syn::parse_str(target).map_err(|e| {
                format!("in {name} (type alias target): cannot parse `{target}`: {e}")
            })?;
            push_edges_ty("type alias target", &ty).map(|edges| worklist.extend(edges))?;
        }
    }
    Ok(visited)
}

/// Names that can never carry wire keys, deliberately excluded from
/// CHECK 2. Each entry carries its justification.
pub(super) const EXCLUDED_TYPE_NAMES: &[&str] = &[
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

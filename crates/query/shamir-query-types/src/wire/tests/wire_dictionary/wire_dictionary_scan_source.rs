//! Source-scanning half of the wire-dictionary pinning test (WIRE-DICT-1,
//! design `docs/dev-artifacts/design/wire-keyword-dictionary.md` §3.2).
//!
//! Walks the crate source with `syn` and produces the normalized scan
//! entries (struct/enum kind, members, effective wire names, serde
//! attribute flags) that `wire_dictionary_scan_tests.rs` pins against the
//! committed snapshot. Pure scanning helpers only — no snapshot model, no
//! corpus, no checks.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use syn::ext::IdentExt;
use syn::Item;

// ---------------------------------------------------------------------------
// Normalized serde attribute model (no raw strings cross the comparisons)
// ---------------------------------------------------------------------------

/// Normalized serde attributes that change wire shape or index mapping.
/// Strings are kept wherever a path/value matters (`default = "f"`,
/// `skip_serializing_if = "Option::is_none"`, `with = "serde_bytes"`, ...).
#[derive(Default, Debug, Clone)]
pub(crate) struct SerdeFlags {
    /// `#[serde(default)]` = Some(""), `#[serde(default = "path")]` = Some(path)
    pub(crate) default: Option<String>,
    pub(crate) flatten: bool,
    pub(crate) skip_serializing: bool,
    pub(crate) skip_deserializing: bool,
    pub(crate) deny_unknown_fields: bool,
    pub(crate) untagged: bool,
    pub(crate) transparent: bool,
    /// container `tag = "..."`
    pub(crate) tag: Option<String>,
    /// container `content = "..."`
    pub(crate) content: Option<String>,
    /// container/variant `rename_all = "..."`
    pub(crate) rename_all: Option<String>,
    /// `skip_serializing_if = "..."`
    pub(crate) skip_serializing_if: Option<String>,
    /// `with = "..."`
    pub(crate) with: Option<String>,
    /// `serialize_with = "..."`
    pub(crate) serialize_with: Option<String>,
    /// `deserialize_with = "..."`
    pub(crate) deserialize_with: Option<String>,
    /// every `rename = "..."` value
    pub(crate) renames: Vec<String>,
    /// every `alias = "..."` value
    pub(crate) aliases: Vec<String>,
    /// `from = "T"` conversion target (serde deserializes T, converts)
    pub(crate) from: Option<String>,
    pub(crate) try_from: Option<String>,
    pub(crate) into: Option<String>,
    pub(crate) remote: Option<String>,
    /// container `rename_all_fields = "..."` (enum variant FIELD names)
    pub(crate) rename_all_fields: Option<String>,
    /// `#[serde(skip)]` = skip_serializing + skip_deserializing; recorded as
    /// the `skip` attribute string itself so the JSON can pin it exactly.
    pub(crate) skip: bool,
    /// variant-level `#[serde(other)]`
    pub(crate) other: bool,
    /// `rename(serialize = "a")` values
    pub(crate) renames_serialize: Vec<String>,
    /// `rename(deserialize = "b")` values — the DESERIALIZE value is the
    /// identifier serde's field/variant visitor matches on, so it is the
    /// effective wire name when no plain `rename` is present.
    pub(crate) renames_deserialize: Vec<String>,
}

impl SerdeFlags {
    pub(crate) fn has_rename(&self, name: &str) -> bool {
        self.renames.iter().any(|r| r == name) || self.aliases.iter().any(|a| a == name)
    }

    /// The first `rename = "..."` value, if any (the effective wire name).
    /// A directional `rename(serialize = "a", deserialize = "b")` yields
    /// its DESERIALIZE value: that is the identifier the serde visitor
    /// matches on while decoding, which is what this dictionary pins.
    pub(crate) fn rename(&self) -> Option<&str> {
        self.renames
            .first()
            .or_else(|| self.renames_deserialize.first())
            .map(|s| s.as_str())
    }

    /// The canonical attribute strings this scanner pins, as a set. This is
    /// the single source of truth for what CHECK 1 compares.
    pub(crate) fn canonical(&self) -> BTreeSet<String> {
        let mut set = BTreeSet::new();
        if let Some(d) = &self.default {
            if d.is_empty() {
                set.insert("default".to_string());
            } else {
                set.insert(format!("default = \"{d}\""));
            }
        }
        for (present, name) in [
            (self.flatten, "flatten"),
            (self.skip_serializing, "skip_serializing"),
            (self.skip_deserializing, "skip_deserializing"),
            (self.deny_unknown_fields, "deny_unknown_fields"),
            (self.untagged, "untagged"),
            (self.transparent, "transparent"),
        ] {
            if present {
                set.insert(name.to_string());
            }
        }
        for (value, key) in [
            (&self.tag, "tag"),
            (&self.content, "content"),
            (&self.rename_all, "rename_all"),
            (&self.rename_all_fields, "rename_all_fields"),
            (&self.from, "from"),
            (&self.try_from, "try_from"),
            (&self.into, "into"),
            (&self.remote, "remote"),
            (&self.skip_serializing_if, "skip_serializing_if"),
            (&self.with, "with"),
            (&self.serialize_with, "serialize_with"),
            (&self.deserialize_with, "deserialize_with"),
        ] {
            if let Some(v) = value {
                set.insert(format!("{key} = \"{v}\""));
            }
        }
        for r in &self.renames {
            set.insert(format!("rename = \"{r}\""));
        }
        for a in &self.aliases {
            set.insert(format!("alias = \"{a}\""));
        }
        for (present, name) in [(self.skip, "skip"), (self.other, "other")] {
            if present {
                set.insert(name.to_string());
            }
        }
        for r in &self.renames_serialize {
            set.insert(format!("rename_serialize = \"{r}\""));
        }
        for r in &self.renames_deserialize {
            set.insert(format!("rename_deserialize = \"{r}\""));
        }
        set
    }

    /// Parse `#[serde(...)]` attributes into flags. Attribute FILTER is
    /// airtight: only `#[serde(...)]` attrs are read (`attr.path().is_ident("serde")`),
    /// so Rust-native `#[default]` attrs never contaminate the flags.
    ///
    /// `parse_nested_meta` ERRORS ARE COLLECTED in `errs`, never discarded:
    /// a malformed serde attribute must fail the scan (the caller prefixes
    /// the current file path and item name). Unknown serde keys are
    /// consumed (value literal, if any) but not pinned.
    pub(crate) fn from_attrs(attrs: &[syn::Attribute], errs: &mut Vec<String>) -> SerdeFlags {
        let mut flags = SerdeFlags::default();
        for attr in attrs {
            if !attr.path().is_ident("serde") {
                continue;
            }
            if let Err(e) = attr.parse_nested_meta(|meta| {
                let name = meta
                    .path
                    .get_ident()
                    .map(|i| i.to_string())
                    .unwrap_or_default();
                // serde attribute values are string literals (`default = "f"`)
                let value_lit = || -> syn::Result<String> {
                    let lit: syn::LitStr = meta.value()?.parse()?;
                    Ok(lit.value())
                };
                let result = match name.as_str() {
                    "default" => {
                        if meta.input.peek(syn::Token![=]) {
                            flags.default = Some(value_lit()?);
                        } else {
                            flags.default = Some(String::new());
                        }
                        Ok(())
                    }
                    "flatten" => {
                        flags.flatten = true;
                        Ok(())
                    }
                    "skip_serializing" => {
                        flags.skip_serializing = true;
                        Ok(())
                    }
                    "skip_deserializing" => {
                        flags.skip_deserializing = true;
                        Ok(())
                    }
                    "deny_unknown_fields" => {
                        flags.deny_unknown_fields = true;
                        Ok(())
                    }
                    "untagged" => {
                        flags.untagged = true;
                        Ok(())
                    }
                    "transparent" => {
                        flags.transparent = true;
                        Ok(())
                    }
                    "tag" => {
                        flags.tag = Some(value_lit()?);
                        Ok(())
                    }
                    "content" => {
                        flags.content = Some(value_lit()?);
                        Ok(())
                    }
                    "rename_all" => {
                        flags.rename_all = Some(value_lit()?);
                        Ok(())
                    }
                    "skip_serializing_if" => {
                        flags.skip_serializing_if = Some(value_lit()?);
                        Ok(())
                    }
                    "with" => {
                        flags.with = Some(value_lit()?);
                        Ok(())
                    }
                    "serialize_with" => {
                        flags.serialize_with = Some(value_lit()?);
                        Ok(())
                    }
                    "deserialize_with" => {
                        flags.deserialize_with = Some(value_lit()?);
                        Ok(())
                    }
                    "skip" => {
                        flags.skip = true;
                        Ok(())
                    }
                    "other" => {
                        flags.other = true;
                        Ok(())
                    }
                    "rename_all_fields" => {
                        flags.rename_all_fields = Some(value_lit()?);
                        Ok(())
                    }
                    "from" => {
                        flags.from = Some(value_lit()?);
                        Ok(())
                    }
                    "try_from" => {
                        flags.try_from = Some(value_lit()?);
                        Ok(())
                    }
                    "into" => {
                        flags.into = Some(value_lit()?);
                        Ok(())
                    }
                    "remote" => {
                        flags.remote = Some(value_lit()?);
                        Ok(())
                    }
                    "rename" => {
                        // directional form: rename(serialize = "a", deserialize = "b")
                        if meta.input.peek(syn::token::Paren) {
                            meta.parse_nested_meta(|m| {
                                let key = m
                                    .path
                                    .get_ident()
                                    .map(|i| i.to_string())
                                    .unwrap_or_default();
                                let v: syn::LitStr = m.value()?.parse()?;
                                match key.as_str() {
                                    "serialize" => {
                                        flags.renames_serialize.push(v.value());
                                        Ok(())
                                    }
                                    "deserialize" => {
                                        flags.renames_deserialize.push(v.value());
                                        Ok(())
                                    }
                                    _ => Err(
                                        m.error(format!("unsupported nested rename key `{key}`"))
                                    ),
                                }
                            })?;
                            Ok(())
                        } else {
                            flags.renames.push(value_lit()?);
                            Ok(())
                        }
                    }
                    "alias" => {
                        flags.aliases.push(value_lit()?);
                        Ok(())
                    }
                    // bound / crate: pinned-irrelevant, consumed and ignored.
                    _ => {
                        if meta.input.peek(syn::Token![=]) {
                            let _: syn::Lit = meta.value()?.parse()?;
                        }
                        Ok(())
                    }
                };
                result
            }) {
                errs.push(e.to_string());
            }
        }
        flags
    }
}

/// One struct field (or one unnamed field, name = its index as string).
#[derive(Debug, Clone)]
pub(crate) struct Member {
    /// Rust identifier as written (`r#where` normalizes to `where`),
    /// or the index for unnamed fields.
    pub(crate) name: String,
    /// EFFECTIVE WIRE NAME: `rename` if present, else the identifier under
    /// the container's `rename_all` rule.
    pub(crate) wire: String,
    /// The member type as written in source (whitespace-normalized only).
    pub(crate) ty: String,
    pub(crate) flags: SerdeFlags,
}

/// One enum variant, with its shape and (for struct/tuple shapes) fields.
#[derive(Debug, Clone)]
pub(crate) struct VariantRec {
    pub(crate) name: String,
    /// effective wire name (rename / container rename_all)
    pub(crate) wire: String,
    pub(crate) shape: &'static str, // "unit" | "newtype" | "tuple" | "struct"
    pub(crate) flags: SerdeFlags,
    pub(crate) fields: Vec<Member>,
}

/// One scanned struct or enum, keyed by its base type name (before `<`).
/// `kind` is classified from SYNTAX for structs:
/// named fields → "struct", exactly one unnamed field → "newtype",
/// several unnamed fields → "tuple", no fields → "unit".
#[derive(Debug, Clone)]
pub(crate) struct ScanEntry {
    pub(crate) kind: &'static str, // "struct" | "enum" | "newtype" | "tuple" | "unit"
    pub(crate) container: SerdeFlags,
    pub(crate) fields: Vec<Member>,
    pub(crate) variants: Vec<VariantRec>,
}

impl ScanEntry {
    /// Member type strings with a FIELD LABEL ("field name" or
    /// "Variant::field"), used as the reachability-graph edges so a
    /// malformed/unsupported type string can be reported naming the field.
    pub(crate) fn member_types(&self) -> Vec<(String, String)> {
        let mut out = Vec::new();
        for f in &self.fields {
            out.push((format!("field {}", f.name), f.ty.clone()));
        }
        for v in &self.variants {
            for f in &v.fields {
                out.push((format!("variant {}::{}", v.name, f.name), f.ty.clone()));
            }
        }
        out
    }
}

/// serde's RenameRule applied the way serde_derive does it: a `_` is
/// inserted before every uppercase character at a non-zero index (snake /
/// kebab / screaming), plus the plain case rewrites. An unknown rule value
/// keeps the identifier as-is, which then mismatches loudly in CHECK 1.
pub(crate) fn apply_rename_rule(name: &str, rule: Option<&str>) -> String {
    let mut out = String::with_capacity(name.len() + 4);
    match rule {
        None => name.to_string(),
        Some("lowercase") => name.to_lowercase(),
        Some("UPPERCASE") => name.to_uppercase(),
        Some("camelCase") => {
            let mut c = name.chars();
            match c.next() {
                Some(f) => f.to_lowercase().collect::<String>() + c.as_str(),
                None => String::new(),
            }
        }
        Some("PascalCase") => {
            let mut c = name.chars();
            match c.next() {
                Some(f) => f.to_uppercase().collect::<String>() + c.as_str(),
                None => String::new(),
            }
        }
        Some(rule @ ("snake_case" | "kebab-case" | "SCREAMING_SNAKE_CASE")) => {
            let sep = if rule == "kebab-case" { '-' } else { '_' };
            for (i, ch) in name.chars().enumerate() {
                if ch.is_uppercase() && i != 0 {
                    out.push(sep);
                }
                if rule == "SCREAMING_SNAKE_CASE" {
                    out.push(ch.to_ascii_uppercase());
                } else {
                    out.push(ch.to_ascii_lowercase());
                }
            }
            out
        }
        // unknown rename_all value: keep the identifier; CHECK 1 mismatches loudly
        Some(_) => name.to_string(),
    }
}

pub(crate) fn effective_wire(
    ident: &str,
    rename: Option<&str>,
    rename_all: Option<&str>,
) -> String {
    match rename {
        Some(r) => r.to_string(),
        None => apply_rename_rule(ident, rename_all),
    }
}

pub(crate) fn fields_to_members(
    fields: &syn::Fields,
    rename_all: Option<&str>,
    errs: &mut Vec<String>,
) -> Vec<Member> {
    match fields {
        syn::Fields::Named(named) => named
            .named
            .iter()
            .map(|f| {
                // unraw: `r#where` / `r#type` travel as `where` / `type`
                let name = f.ident.as_ref().unwrap().unraw().to_string();
                let flags = SerdeFlags::from_attrs(&f.attrs, errs);
                let wire = effective_wire(&name, flags.rename(), rename_all);
                Member {
                    name,
                    wire,
                    ty: ty_to_string(&f.ty),
                    flags,
                }
            })
            .collect(),
        syn::Fields::Unnamed(unnamed) => unnamed
            .unnamed
            .iter()
            .enumerate()
            .map(|(i, f)| {
                let flags = SerdeFlags::from_attrs(&f.attrs, errs);
                Member {
                    name: i.to_string(),
                    wire: i.to_string(),
                    ty: ty_to_string(&f.ty),
                    flags,
                }
            })
            .collect(),
        syn::Fields::Unit => Vec::new(),
    }
}

/// Structural kind, shared by structs and enum variants: named fields →
/// "struct", exactly one unnamed field → "newtype", several unnamed →
/// "tuple", unit → "unit".
pub(crate) fn shape_of_fields(fields: &syn::Fields) -> &'static str {
    match fields {
        syn::Fields::Unit => "unit",
        syn::Fields::Unnamed(u) if u.unnamed.len() == 1 => "newtype",
        syn::Fields::Unnamed(_) => "tuple",
        syn::Fields::Named(_) => "struct",
    }
}

pub(crate) fn collect_items(
    items: &[Item],
    out: &mut BTreeMap<String, ScanEntry>,
    aliases: &mut BTreeMap<String, String>,
    file: &Path,
    errs: &mut Vec<String>,
) {
    /// Prefix raw `from_attrs` errors with the file path and item name so
    /// the failure names its source location (errors are never discarded).
    fn contextualize(errs: &mut Vec<String>, base: usize, file: &Path, item: &str) {
        let raw: Vec<String> = errs.drain(base..).collect();
        for e in raw {
            errs.push(format!("{}: {item}: {e}", file.display()));
        }
    }
    for item in items {
        match item {
            Item::Struct(s) => {
                let name = s.ident.unraw().to_string();
                let base = errs.len();
                let container = SerdeFlags::from_attrs(&s.attrs, errs);
                // serde_derive ast.rs:96-99 — struct fields take the
                // container's rename_all rule (rename_all_fields is
                // enum-only in serde: it is REJECTED on structs).
                let members = fields_to_members(&s.fields, container.rename_all.as_deref(), errs);
                contextualize(errs, base, file, &format!("struct {name}"));
                let kind = shape_of_fields(&s.fields);
                out.entry(name).or_insert(ScanEntry {
                    kind,
                    container,
                    fields: members,
                    variants: Vec::new(),
                });
            }
            Item::Enum(e) => {
                let name = e.ident.unraw().to_string();
                let base = errs.len();
                let container = SerdeFlags::from_attrs(&e.attrs, errs);
                let rule = container.rename_all.as_deref();
                let variants = e
                    .variants
                    .iter()
                    .map(|v| {
                        let flags = SerdeFlags::from_attrs(&v.attrs, errs);
                        // serde_derive ast.rs:92-96 — a variant's FIELD
                        // names use the VARIANT-level rename_all if
                        // present, else the container's rename_all_fields
                        // (NOT the container rename_all, which governs
                        // VARIANT NAMES only).
                        let field_rule = flags
                            .rename_all
                            .as_deref()
                            .or(container.rename_all_fields.as_deref());
                        let vname = v.ident.unraw().to_string();
                        VariantRec {
                            name: vname.clone(),
                            wire: effective_wire(&vname, flags.rename(), rule),
                            shape: shape_of_fields(&v.fields),
                            fields: fields_to_members(&v.fields, field_rule, errs),
                            flags,
                        }
                    })
                    .collect();
                contextualize(errs, base, file, &format!("enum {name}"));
                out.entry(name).or_insert(ScanEntry {
                    kind: "enum",
                    container,
                    fields: Vec::new(),
                    variants,
                });
            }
            // Crate type aliases: recorded so the completeness closure can
            // expand them (e.g. `pub type FieldPath = Vec<String>`).
            Item::Type(t) => {
                aliases
                    .entry(t.ident.unraw().to_string())
                    .or_insert_with(|| ty_to_string(&t.ty));
            }
            // Inline modules: recurse so no type definition is missed.
            Item::Mod(m) => {
                if let Some((_, inner)) = &m.content {
                    collect_items(inner, out, aliases, file, errs);
                }
            }
            _ => {}
        }
    }
}

/// Manual `syn::Type` formatter (syn types do not implement Display and
/// `quote` is not a dev-dependency). Paths are kept as written; only
/// insignificant whitespace is normalized. Unknown variants yield a marker
/// string — never a panic.
pub(crate) fn ty_to_string(ty: &syn::Type) -> String {
    match ty {
        syn::Type::Array(a) => format!("[{}; {}]", ty_to_string(&a.elem), expr_to_string(&a.len)),
        syn::Type::Slice(s) => format!("[{}]", ty_to_string(&s.elem)),
        syn::Type::Reference(r) => {
            let lifetime = r
                .lifetime
                .as_ref()
                .map(|l| l.ident.to_string() + " ")
                .unwrap_or_default();
            let mutable = if r.mutability.is_some() { "mut " } else { "" };
            format!("&{lifetime}{mutable}{}", ty_to_string(&r.elem))
        }
        syn::Type::Ptr(p) => {
            let kind = if p.const_token.is_some() {
                "const "
            } else {
                "mut "
            };
            format!("*{kind}{}", ty_to_string(&p.elem))
        }
        syn::Type::Never(_) => "!".to_string(),
        syn::Type::Tuple(t) => {
            let elems: Vec<String> = t.elems.iter().map(ty_to_string).collect();
            if elems.len() == 1 {
                format!("({},)", elems[0])
            } else {
                format!("({})", elems.join(", "))
            }
        }
        syn::Type::Paren(p) => format!("({})", ty_to_string(&p.elem)),
        syn::Type::Group(g) => ty_to_string(&g.elem),
        syn::Type::Path(p) => path_to_string(&p.qself, &p.path),
        syn::Type::TraitObject(t) => {
            let bounds: Vec<String> = t.bounds.iter().map(bound_to_string).collect();
            bounds.join(" + ")
        }
        syn::Type::BareFn(_)
        | syn::Type::ImplTrait(_)
        | syn::Type::Macro(_)
        | syn::Type::Infer(_) => "#unsupported-type#".to_string(),
        _ => "#unsupported-type#".to_string(),
    }
}

pub(crate) fn bound_to_string(b: &syn::TypeParamBound) -> String {
    match b {
        syn::TypeParamBound::Trait(t) => path_to_string(&None, &t.path),
        syn::TypeParamBound::Lifetime(l) => l.to_string(),
        _ => "#unsupported-bound#".to_string(),
    }
}

pub(crate) fn path_to_string(qself: &Option<syn::QSelf>, path: &syn::Path) -> String {
    if qself.is_some() {
        return "#unsupported-qself#".to_string();
    }
    let mut out = if path.leading_colon.is_some() {
        "::".to_string()
    } else {
        String::new()
    };
    for (i, seg) in path.segments.iter().enumerate() {
        if i > 0 {
            out.push_str("::");
        }
        out.push_str(&seg.ident.to_string());
        match &seg.arguments {
            syn::PathArguments::None => {}
            syn::PathArguments::AngleBracketed(a) => {
                let args: Vec<String> = a
                    .args
                    .iter()
                    .map(|arg| match arg {
                        syn::GenericArgument::Type(t) => ty_to_string(t),
                        syn::GenericArgument::Lifetime(l) => l.to_string(),
                        syn::GenericArgument::Const(c) => expr_to_string(c),
                        _ => "#unsupported-generic-arg#".to_string(),
                    })
                    .collect();
                out.push_str(&format!("<{}>", args.join(", ")));
            }
            syn::PathArguments::Parenthesized(_) => {
                out.push_str("#unsupported-parenthesized-args#");
            }
        }
    }
    out
}

pub(crate) fn expr_to_string(expr: &syn::Expr) -> String {
    match expr {
        syn::Expr::Lit(l) => match &l.lit {
            syn::Lit::Int(i) => i.to_string(),
            syn::Lit::Str(s) => s.value(),
            syn::Lit::Bool(b) => b.value.to_string(),
            _ => "#unsupported-lit#".to_string(),
        },
        syn::Expr::Path(p) => path_to_string(&p.qself, &p.path),
        _ => "#unsupported-expr#".to_string(),
    }
}

/// Recursively collect every `*.rs` file under `root`, skipping any
/// directory named `tests` (test modules do not define wire types).
/// Directory-read errors PROPAGATE with the offending path — nothing is
/// skipped silently except `tests` dirs.
pub(crate) fn collect_rs_files(dir: &Path, out: &mut Vec<PathBuf>) -> Result<(), String> {
    let entries =
        fs::read_dir(dir).map_err(|e| format!("cannot read directory {}: {e}", dir.display()))?;
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if path.file_name().and_then(|n| n.to_str()) == Some("tests") {
                continue;
            }
            collect_rs_files(&path, out)?;
        } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
            out.push(path);
        }
    }
    Ok(())
}

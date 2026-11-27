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
/// Where a `#[serde(...)]` attribute sits. serde accepts different keys at
/// different positions, and the dictionary model additionally treats
/// variant-level `default` as MALFORMED: it is not a real serde attribute
/// (the Rust-native `#[default]` derive attr is a different thing entirely
/// and is never read by this scanner).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum SerdePos {
    /// struct/enum container
    Container,
    /// struct field, variant payload field, or unnamed field
    Field,
    /// enum variant
    Variant,
}

/// Serde keys that CANNOT affect the wire, deliberately consumed and
/// ignored (each with its one-line reason). ANY OTHER serde key that is
/// neither pinned by `SerdeFlags` nor listed here FAILS the scan.
pub(super) const HARMLESS_SERDE_KEYS: &[&str] = &[
    // `bound = "..."` / `bounds(..)`: adds trait bounds on generated impls;
    // pure compile-time, no wire effect.
    "bound",
    // `crate = "..."`: the path serde_derive uses to refer to the serde
    // crate; codegen plumbing only.
    "crate",
    // `no_default`: deserializer fallback plumbing (use the visitor's
    // missing-field error instead of Field::default); no wire name/shape.
    "no_default",
];

/// Serde keys the scanner PINS (they change wire names or wire shape).
pub(super) const PINNED_SERDE_KEYS: &[&str] = &[
    "default",
    "flatten",
    "skip_serializing",
    "skip_deserializing",
    "deny_unknown_fields",
    "untagged",
    "transparent",
    "tag",
    "content",
    "rename_all",
    "rename_all_fields",
    "skip_serializing_if",
    "with",
    "serialize_with",
    "deserialize_with",
    "skip",
    "other",
    "rename",
    "alias",
    "from",
    "try_from",
    "into",
    "remote",
];

/// Shape-changing serde attributes the dictionary model cannot represent.
const SHAPE_CHANGING_KEYS: &[&str] = &["field_identifier", "variant_identifier"];

#[derive(Default, Debug, Clone)]
pub(super) struct SerdeFlags {
    /// `#[serde(default)]` = Some(""), `#[serde(default = "path")]` = Some(path)
    pub(super) default: Option<String>,
    pub(super) flatten: bool,
    pub(super) skip_serializing: bool,
    pub(super) skip_deserializing: bool,
    pub(super) deny_unknown_fields: bool,
    pub(super) untagged: bool,
    pub(super) transparent: bool,
    /// container `tag = "..."`
    pub(super) tag: Option<String>,
    /// container `content = "..."`
    pub(super) content: Option<String>,
    /// container/variant `rename_all = "..."`
    pub(super) rename_all: Option<String>,
    /// `skip_serializing_if = "..."`
    pub(super) skip_serializing_if: Option<String>,
    /// `with = "..."`
    pub(super) with: Option<String>,
    /// `serialize_with = "..."`
    pub(super) serialize_with: Option<String>,
    /// `deserialize_with = "..."`
    pub(super) deserialize_with: Option<String>,
    /// every `rename = "..."` value
    pub(super) renames: Vec<String>,
    /// every `alias = "..."` value
    pub(super) aliases: Vec<String>,
    /// `from = "T"` conversion target (serde deserializes T, converts)
    pub(super) from: Option<String>,
    pub(super) try_from: Option<String>,
    pub(super) into: Option<String>,
    pub(super) remote: Option<String>,
    /// container `rename_all_fields = "..."` (enum variant FIELD names)
    pub(super) rename_all_fields: Option<String>,
    /// `#[serde(skip)]` = skip_serializing + skip_deserializing; recorded as
    /// the `skip` attribute string itself so the JSON can pin it exactly.
    pub(super) skip: bool,
    /// variant-level `#[serde(other)]`
    pub(super) other: bool,
    /// `rename(serialize = "a")` values
    pub(super) renames_serialize: Vec<String>,
    /// `rename(deserialize = "b")` values — the DESERIALIZE value is the
    /// identifier serde's field/variant visitor matches on, so it is the
    /// effective wire name when no plain `rename` is present.
    pub(super) renames_deserialize: Vec<String>,
    /// directional `rename_all(serialize = "...")` (container/variant level)
    pub(super) rename_all_serialize: Option<String>,
    /// directional `rename_all(deserialize = "...")` — the effective rule
    /// for wire names when no plain `rename_all` is present.
    pub(super) rename_all_deserialize: Option<String>,
    /// directional `rename_all_fields(serialize = "...")` (container level)
    pub(super) rename_all_fields_serialize: Option<String>,
    /// directional `rename_all_fields(deserialize = "...")`
    pub(super) rename_all_fields_deserialize: Option<String>,
    /// The literal cfg predicate text when this member is cfg-gated AND
    /// carries serde (or a cfg_attr expanding to serde). Pinned by the
    /// snapshot as `cfg = "<predicate>"`; a gated member without that pin
    /// fails the comparison.
    pub(super) cfg: Option<String>,
}
impl SerdeFlags {
    /// The rule governing DESERIALIZE wire names: the directional
    /// deserialize value wins, else the plain rule.
    pub(super) fn rule_deserialize(&self) -> Option<&str> {
        self.rename_all_deserialize
            .as_deref()
            .or(self.rename_all.as_deref())
    }

    pub(super) fn has_rename(&self, name: &str) -> bool {
        self.renames.iter().any(|r| r == name) || self.aliases.iter().any(|a| a == name)
    }

    /// The effective DESERIALIZE wire name: plain/directional rename,
    /// preferring the deserialize value of a directional rename.
    pub(super) fn rename(&self) -> Option<&str> {
        self.renames
            .first()
            .or_else(|| self.renames_deserialize.first())
            .map(|s| s.as_str())
    }

    /// The canonical attribute strings this scanner pins, as a set. This is
    /// the single source of truth for what CHECK 1 compares.
    pub(super) fn canonical(&self) -> BTreeSet<String> {
        let mut set = BTreeSet::new();
        if let Some(pred) = &self.cfg {
            set.insert(format!("cfg = \"{pred}\""));
        }
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
            (&self.rename_all_serialize, "rename_all_serialize"),
            (&self.rename_all_deserialize, "rename_all_deserialize"),
            (&self.rename_all_fields, "rename_all_fields"),
            (
                &self.rename_all_fields_serialize,
                "rename_all_fields_serialize",
            ),
            (
                &self.rename_all_fields_deserialize,
                "rename_all_fields_deserialize",
            ),
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

    /// Parse `#[serde(...)]` attributes into flags at a given position.
    /// Attribute FILTER is airtight: only `#[serde(...)]` attrs are read
    /// (`attr.path().is_ident("serde")`), so Rust-native `#[default]` attrs
    /// never contaminate the flags.
    ///
    /// ERRORS ARE COLLECTED in `errs`, never discarded: a malformed serde
    /// attribute (including variant-level `default`, shape-changing
    /// identifier attrs, and unclassified keys) must fail the scan. Values
    /// of directional forms are parsed with syn nested metas — quoted
    /// commas inside a name are never split by string cutting.
    pub(super) fn from_attrs(
        attrs: &[syn::Attribute],
        pos: SerdePos,
        errs: &mut Vec<String>,
    ) -> SerdeFlags {
        let mut flags = SerdeFlags {
            cfg: cfg_gate(attrs, errs),
            ..SerdeFlags::default()
        };
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
                // Directional nested form: `key(serialize = "a",
                // deserialize = "b")`. syn's parse_nested_meta handles the
                // quoting; no string splitting of names ever happens.
                let nested_directional = |flags: &mut SerdeFlags,
                                          sink_ser: &mut dyn FnMut(&mut SerdeFlags, String),
                                          sink_de: &mut dyn FnMut(&mut SerdeFlags, String)|
                 -> syn::Result<()> {
                    meta.parse_nested_meta(|m| {
                        let key = m
                            .path
                            .get_ident()
                            .map(|i| i.to_string())
                            .unwrap_or_default();
                        let v: syn::LitStr = m.value()?.parse()?;
                        match key.as_str() {
                            "serialize" => {
                                sink_ser(flags, v.value());
                                Ok(())
                            }
                            "deserialize" => {
                                sink_de(flags, v.value());
                                Ok(())
                            }
                            _ => Err(
                                m.error(format!("unsupported nested key `{key}` in `{name}(...)`"))
                            ),
                        }
                    })
                };
                let result = match name.as_str() {
                    "default" => {
                        if pos == SerdePos::Variant {
                            // NOT a real serde attribute: serde_derive never
                            // accepts `default` on a variant (the Rust-native
                            // `#[default]` derive attr is a different thing).
                            Err(meta.error(
                                "variant-level serde `default` is not a serde attribute — \
                                 remove it from the source or the snapshot",
                            ))
                        } else if meta.input.peek(syn::Token![=]) {
                            flags.default = Some(value_lit()?);
                            Ok(())
                        } else {
                            flags.default = Some(String::new());
                            Ok(())
                        }
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
                        if meta.input.peek(syn::token::Paren) {
                            nested_directional(
                                &mut flags,
                                &mut |f, v| f.rename_all_serialize = Some(v),
                                &mut |f, v| f.rename_all_deserialize = Some(v),
                            )
                        } else {
                            flags.rename_all = Some(value_lit()?);
                            Ok(())
                        }
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
                        if meta.input.peek(syn::token::Paren) {
                            nested_directional(
                                &mut flags,
                                &mut |f, v| f.rename_all_fields_serialize = Some(v),
                                &mut |f, v| f.rename_all_fields_deserialize = Some(v),
                            )
                        } else {
                            flags.rename_all_fields = Some(value_lit()?);
                            Ok(())
                        }
                    }
                    "rename" => {
                        // directional form: rename(serialize = "a", deserialize = "b")
                        if meta.input.peek(syn::token::Paren) {
                            nested_directional(
                                &mut flags,
                                &mut |f, v| f.renames_serialize.push(v),
                                &mut |f, v| f.renames_deserialize.push(v),
                            )
                        } else {
                            flags.renames.push(value_lit()?);
                            Ok(())
                        }
                    }
                    "alias" => {
                        flags.aliases.push(value_lit()?);
                        Ok(())
                    }
                    // shape-changing identifier attributes: they rewrite the
                    // DESERIALIZE shape itself (field/variant identifiers are
                    // matched by index), which the dictionary model cannot
                    // represent — fail explicitly, never silently ignore.
                    k if SHAPE_CHANGING_KEYS.contains(&k) => Err(meta.error(format!(
                        "`{k}` is a shape-changing serde attribute not supported by the \
                         dictionary model"
                    ))),
                    // pinned, recorded (conversion targets / mirror path)
                    "from" | "try_from" | "into" | "remote" => {
                        let v = value_lit()?;
                        match name.as_str() {
                            "from" => flags.from = Some(v),
                            "try_from" => flags.try_from = Some(v),
                            "into" => flags.into = Some(v),
                            _ => flags.remote = Some(v),
                        }
                        Ok(())
                    }
                    k if HARMLESS_SERDE_KEYS.contains(&k) => {
                        // pinned-irrelevant: consume an attached value, if any
                        if meta.input.peek(syn::Token![=]) {
                            let _: syn::Lit = meta.value()?.parse()?;
                        }
                        Ok(())
                    }
                    // UNCLASSIFIED serde key: fail the scan naming the key so
                    // a serde version upgrade cannot change the wire silently.
                    k => Err(meta.error(format!(
                        "unclassified serde attribute `{k}` — not pinned (pinned set: \
                         {PINNED_SERDE_KEYS:?}) and not on the harmless allowlist \
                         {HARMLESS_SERDE_KEYS:?}"
                    ))),
                };
                result
            }) {
                errs.push(e.to_string());
            }
        }
        flags
    }
}

/// cfg / cfg_attr gating (ITEM 3): a member that is cfg-gated AND carries
/// serde — or carries a cfg_attr expanding to a serde callee — must have
/// its predicate PINNED by the snapshot (`cfg = "<predicate>"`); a gated
/// member without that pin fails the comparison. cfg_attr with a non-serde
/// callee and no serde attr on the member is harmless.
pub(super) fn cfg_gate(attrs: &[syn::Attribute], errs: &mut Vec<String>) -> Option<String> {
    let has_serde = attrs.iter().any(|a| a.path().is_ident("serde"));
    let mut gated = None;
    for attr in attrs {
        let path = attr.path();
        if path.is_ident("cfg") {
            // `#[cfg(PRED)]` with a serde attr on the same member: the
            // member's presence depends on build config, so the snapshot
            // must pin the predicate text.
            if has_serde {
                gated = Some(
                    attr.meta
                        .require_list()
                        .map(|l| l.tokens.to_string())
                        .unwrap_or_else(|e| {
                            errs.push(format!("malformed cfg attribute: {e}"));
                            String::new()
                        }),
                );
            }
        } else if path.is_ident("cfg_attr") {
            // `#[cfg_attr(PRED, serde(...))]`: gated if a callee is `serde`;
            // non-serde callees are harmless. Predicate = tokens up to the
            // first top-level comma (token-text matching only — cfg
            // predicates are plain paths/feature names).
            if let Ok(list) = attr.meta.require_list() {
                let text = list.tokens.to_string();
                let serde_callee = text
                    .split_once(',')
                    .map(|(_, rest)| {
                        rest.split(',').any(|callee| {
                            callee
                                .trim()
                                .trim_start_matches('(')
                                .trim_end_matches(')')
                                .split_whitespace()
                                .next()
                                .map(|p| {
                                    p.rsplit("::").next().unwrap_or(p) == "serde"
                                        && !p.contains('!')
                                })
                                .unwrap_or(false)
                        })
                    })
                    .unwrap_or(false);
                if serde_callee {
                    let pred = text.split_once(',').map(|(p, _)| p.trim().to_string());
                    gated = pred;
                }
            }
        }
    }
    gated
}

/// One struct field (or one unnamed field, name = its index as string).
#[derive(Debug, Clone)]
pub(super) struct Member {
    /// Rust identifier as written (`r#where` normalizes to `where`),
    /// or the index for unnamed fields.
    pub(super) name: String,
    /// EFFECTIVE WIRE NAME: `rename` if present, else the identifier under
    /// the container's `rename_all` rule.
    pub(super) wire: String,
    /// Member type as written; STRING not syn::Type (corpus must stay Send+Sync).
    pub(super) ty: String,
    pub(super) flags: SerdeFlags,
}

/// One enum variant, with its shape and (for struct/tuple shapes) fields.
#[derive(Debug, Clone)]
pub(super) struct VariantRec {
    pub(super) name: String,
    /// effective wire name (rename / container rename_all)
    pub(super) wire: String,
    pub(super) shape: &'static str, // "unit" | "newtype" | "tuple" | "struct"
    pub(super) flags: SerdeFlags,
    pub(super) fields: Vec<Member>,
}

/// One scanned struct or enum, keyed by its base type name (before `<`).
/// `kind` is classified from SYNTAX for structs:
/// named fields → "struct", exactly one unnamed field → "newtype",
/// several unnamed fields → "tuple", no fields → "unit".
#[derive(Debug, Clone)]
pub(super) struct ScanEntry {
    pub(super) kind: &'static str, // "struct" | "enum" | "newtype" | "tuple" | "unit"
    pub(super) container: SerdeFlags,
    pub(super) fields: Vec<Member>,
    pub(super) variants: Vec<VariantRec>,
}

impl ScanEntry {
    /// Member type strings with a FIELD LABEL ("field name" or
    /// "Variant::field"), used as the reachability-graph edges so a
    /// malformed/unsupported type string can be reported naming the field.
    pub(super) fn member_types(&self) -> Vec<(String, String)> {
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

/// serde's `RenameRule`, implemented EXACTLY as serde_derive-1.0.229
/// `src/internals/case.rs:56-108` does it. FIELD and VARIANT application
/// differ (fields keep `_` separators for Lower/Snake, variants split on
/// uppercase boundaries) — serde applies `apply_to_field` to struct fields
/// and `apply_to_variant` to variant names, and so do we.
///
/// Every spelling serde supports is modeled:
/// `lowercase`, `UPPERCASE`, `PascalCase`, `camelCase`, `snake_case`,
/// `SCREAMING_SNAKE_CASE`, `kebab-case`, `SCREAMING-KEBAB-CASE`.
const RENAME_RULES: &[&str] = &[
    "lowercase",
    "UPPERCASE",
    "PascalCase",
    "camelCase",
    "snake_case",
    "SCREAMING_SNAKE_CASE",
    "kebab-case",
    "SCREAMING-KEBAB-CASE",
];

/// Validate a `rename_all` value against serde's exact rule table; an
/// unknown value is a scan ERROR (serde_derive rejects it at compile time,
/// so it can never legitimately reach the wire).
pub(super) fn check_rename_rule(rule: Option<&str>, errs: &mut Vec<String>) {
    if let Some(r) = rule {
        if !RENAME_RULES.contains(&r) {
            errs.push(format!(
                "unknown rename_all rule `{r}` — serde supports {RENAME_RULES:?}"
            ));
        }
    }
}

/// case.rs `RenameRule::apply_to_field`.
pub(super) fn apply_to_field(field: &str, rule: &str) -> String {
    match rule {
        // None | LowerCase | SnakeCase → unchanged
        "lowercase" | "snake_case" => field.to_string(),
        "UPPERCASE" => field.to_ascii_uppercase(),
        "SCREAMING_SNAKE_CASE" => field.to_ascii_uppercase(),
        "PascalCase" => {
            let mut pascal = String::new();
            let mut capitalize = true;
            for ch in field.chars() {
                if ch == '_' {
                    capitalize = true;
                } else if capitalize {
                    pascal.push(ch.to_ascii_uppercase());
                    capitalize = false;
                } else {
                    pascal.push(ch);
                }
            }
            pascal
        }
        "camelCase" => {
            let pascal = apply_to_field(field, "PascalCase");
            let mut c = pascal.chars();
            match c.next() {
                Some(f) => f.to_lowercase().collect::<String>() + c.as_str(),
                None => String::new(),
            }
        }
        "kebab-case" => field.replace('_', "-"),
        // ScreamingSnakeCase.apply_to_field(f).replace('_', "-")
        "SCREAMING-KEBAB-CASE" => field.to_ascii_uppercase().replace('_', "-"),
        _ => field.to_string(), // unknown: check_rename_rule already errors
    }
}

/// case.rs `RenameRule::apply_to_variant`.
pub(super) fn apply_to_variant(variant: &str, rule: &str) -> String {
    match rule {
        // None | PascalCase → unchanged
        "PascalCase" => variant.to_string(),
        "lowercase" => variant.to_ascii_lowercase(),
        "UPPERCASE" => variant.to_ascii_uppercase(),
        "camelCase" => {
            let mut c = variant.chars();
            match c.next() {
                Some(f) => f.to_lowercase().collect::<String>() + c.as_str(),
                None => String::new(),
            }
        }
        "snake_case" => {
            let mut snake = String::new();
            for (i, ch) in variant.char_indices() {
                if i > 0 && ch.is_uppercase() {
                    snake.push('_');
                }
                snake.push(ch.to_ascii_lowercase());
            }
            snake
        }
        "SCREAMING_SNAKE_CASE" => apply_to_variant(variant, "snake_case").to_ascii_uppercase(),
        "kebab-case" => apply_to_variant(variant, "snake_case").replace('_', "-"),
        "SCREAMING-KEBAB-CASE" => {
            apply_to_variant(variant, "SCREAMING_SNAKE_CASE").replace('_', "-")
        }
        _ => variant.to_string(), // unknown: check_rename_rule already errors
    }
}

/// Wire name for a FIELD: `rename` (or the deserialize value of a
/// directional rename) wins, else the FIELD rename rule.
pub(super) fn field_wire(name: &str, rename: Option<&str>, rule: Option<&str>) -> String {
    match rename {
        Some(r) => r.to_string(),
        None => match rule {
            Some(r) => apply_to_field(name, r),
            None => name.to_string(),
        },
    }
}

/// Wire name for a VARIANT: same precedence, VARIANT rename rule.
pub(super) fn variant_wire(name: &str, rename: Option<&str>, rule: Option<&str>) -> String {
    match rename {
        Some(r) => r.to_string(),
        None => match rule {
            Some(r) => apply_to_variant(name, r),
            None => name.to_string(),
        },
    }
}

pub(super) fn fields_to_members(
    fields: &syn::Fields,
    rename_all: Option<&str>,
    errs: &mut Vec<String>,
) -> Vec<Member> {
    check_rename_rule(rename_all, errs);
    match fields {
        syn::Fields::Named(named) => named
            .named
            .iter()
            .map(|f| {
                // unraw: `r#where` / `r#type` travel as `where` / `type`
                let name = f.ident.as_ref().unwrap().unraw().to_string();
                let flags = SerdeFlags::from_attrs(&f.attrs, SerdePos::Field, errs);
                let wire = field_wire(&name, flags.rename(), rename_all);
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
                let flags = SerdeFlags::from_attrs(&f.attrs, SerdePos::Field, errs);
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
pub(super) fn shape_of_fields(fields: &syn::Fields) -> &'static str {
    match fields {
        syn::Fields::Unit => "unit",
        syn::Fields::Unnamed(u) if u.unnamed.len() == 1 => "newtype",
        syn::Fields::Unnamed(_) => "tuple",
        syn::Fields::Named(_) => "struct",
    }
}

pub(super) fn collect_items(
    items: &[Item],
    out: &mut BTreeMap<String, ScanEntry>,
    aliases: &mut BTreeMap<String, String>,
    // Defining file per type name, for ambiguity detection (the same base
    // name defined by two different files/paths makes that name AMBIGUOUS).
    defining: &mut BTreeMap<String, BTreeSet<PathBuf>>,
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
                let container = SerdeFlags::from_attrs(&s.attrs, SerdePos::Container, errs);
                // serde_derive ast.rs:96-99 — struct fields take the
                // container's rename_all rule (deserialize direction wins);
                // rename_all_fields is enum-only in serde (REJECTED on
                // structs).
                check_rename_rule(container.rule_deserialize(), errs);
                let members = fields_to_members(&s.fields, container.rule_deserialize(), errs);
                contextualize(errs, base, file, &format!("struct {name}"));
                let kind = shape_of_fields(&s.fields);
                defining
                    .entry(name.clone())
                    .or_default()
                    .insert(file.to_path_buf());
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
                let container = SerdeFlags::from_attrs(&e.attrs, SerdePos::Container, errs);
                check_rename_rule(container.rule_deserialize(), errs);
                // VARIANT names use the container rename_all (deserialize
                // direction wins)
                let rule = container.rule_deserialize();
                let variants = e
                    .variants
                    .iter()
                    .map(|v| {
                        let flags = SerdeFlags::from_attrs(&v.attrs, SerdePos::Variant, errs);
                        check_rename_rule(flags.rule_deserialize(), errs);
                        // serde_derive ast.rs:92-96 — a variant's FIELD
                        // names use the VARIANT-level rename_all (deserialize
                        // direction wins) if present, else the container's
                        // rename_all_fields (deserialize direction wins; NOT
                        // the container rename_all, which governs VARIANT
                        // NAMES only).
                        let field_rule = flags
                            .rule_deserialize()
                            .or(container.rename_all_fields_deserialize.as_deref())
                            .or(container.rename_all_fields.as_deref());
                        let vname = v.ident.unraw().to_string();
                        VariantRec {
                            name: vname.clone(),
                            wire: variant_wire(&vname, flags.rename(), rule),
                            shape: shape_of_fields(&v.fields),
                            fields: fields_to_members(&v.fields, field_rule, errs),
                            flags,
                        }
                    })
                    .collect();
                contextualize(errs, base, file, &format!("enum {name}"));
                defining
                    .entry(name.clone())
                    .or_default()
                    .insert(file.to_path_buf());
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
                    collect_items(inner, out, aliases, defining, file, errs);
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
pub(super) fn ty_to_string(ty: &syn::Type) -> String {
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

pub(super) fn bound_to_string(b: &syn::TypeParamBound) -> String {
    match b {
        syn::TypeParamBound::Trait(t) => path_to_string(&None, &t.path),
        syn::TypeParamBound::Lifetime(l) => l.to_string(),
        _ => "#unsupported-bound#".to_string(),
    }
}

pub(super) fn path_to_string(qself: &Option<syn::QSelf>, path: &syn::Path) -> String {
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

pub(super) fn expr_to_string(expr: &syn::Expr) -> String {
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
/// directory named `tests` (test modules do not define wire types), and
/// sort the result so the scan is deterministic regardless of read_dir
/// order (ambiguity detection depends on stable ordering). Directory-read
/// and per-entry errors propagate; `file_type()` failures are errors too,
/// never silent misclassification of a file as a directory.
pub(super) fn collect_rs_files(dir: &Path, out: &mut Vec<PathBuf>) -> Result<(), String> {
    let entries =
        fs::read_dir(dir).map_err(|e| format!("cannot read directory {}: {e}", dir.display()))?;
    for entry in entries {
        let entry = entry.map_err(|e| format!("cannot read entry in {}: {e}", dir.display()))?;
        let path = entry.path();
        let is_dir = entry
            .file_type()
            .map_err(|e| format!("cannot stat {}: {e}", path.display()))?
            .is_dir();
        if is_dir {
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

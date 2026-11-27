//! Structured BatchOp pinning test (WIRE-DICT-1, CHECK on the snapshot's
//! `ops` array). BatchOp is hand-written serde (`impl = "hand_written"`),
//! so CHECK 1 skips its variants — but the snapshot carries a SEPARATE
//! `ops` array (`{"variant","operation","keys","payload"}` per entry) that
//! documents the discriminator-key dispatch table implemented by
//! `define_op_kinds!` in `src/ops/batch/ops/op_kind.rs`. This file pins
//! that array against BOTH ground truths:
//!
//! 1. the REAL source scan (`corpus()`): the array's `variant` + `payload`
//!    sequence must equal BatchOp's declaration order exactly;
//! 2. the `define_op_kinds!` macro invocation, parsed with `syn`:
//!    each array entry's `keys` must equal the macro's discriminator-key
//!    literal for the same variant (see the extraction rule at
//!    `macro_op_key_pairs` — the compare is keyed by VARIANT NAME, not
//!    positional, because the macro's arm order is a match-arm order, not
//!    the enum declaration order the array pins).
//!
//! NOTE on `operation`: the array's `operation` string (the activity label,
//! e.g. `"read"` / `"admin:create_db"`) is defined in batch_op.rs's
//! hand-written serializer, NOT in `define_op_kinds!` — the macro carries
//! only (variant, discriminator key). It is therefore only sanity-checked
//! here (non-empty), not macro-verified; the decode-side tests cover the
//! actual dispatch behaviour.

use std::fs;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use std::collections::BTreeMap;

use super::wire_dictionary_scan_tests::corpus;

// ---------------------------------------------------------------------------
// The snapshot's ops array, loaded typed
// ---------------------------------------------------------------------------

#[derive(Deserialize, Debug)]
struct SnapshotFile {
    types: SnapshotTypes,
}

#[derive(Deserialize, Debug)]
struct SnapshotTypes {
    #[serde(rename = "BatchOp")]
    batch_op: BatchOpJson,
}

#[derive(Deserialize, Debug)]
struct BatchOpJson {
    /// `{"variant", "operation", "keys", "payload"}` entries, in BatchOp's
    /// declaration order.
    ops: Vec<BatchOpEntry>,
}

#[derive(Deserialize, Debug, Clone)]
struct BatchOpEntry {
    variant: String,
    operation: String,
    keys: Vec<String>,
    payload: String,
}

fn snapshot_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(3)
        .expect("crate sits three levels below the repo root")
        .join("docs/guide-docs/client-server-protocol-spec/wire-dictionary.json")
}

fn load_ops() -> Vec<BatchOpEntry> {
    let text = fs::read_to_string(snapshot_path())
        .unwrap_or_else(|e| panic!("cannot read snapshot {}: {e}", snapshot_path().display()));
    let parsed: SnapshotFile = serde_json::from_str(&text).unwrap_or_else(|e| {
        panic!(
            "cannot parse the BatchOp ops array of {}: {e}",
            snapshot_path().display()
        )
    });
    parsed.types.batch_op.ops
}

// ---------------------------------------------------------------------------
// Ground truth 1: the scan corpus
// ---------------------------------------------------------------------------

/// BatchOp's variants in DECLARATION order as (variant, payload type string)
/// pairs, straight from the REAL scan corpus (the scanner's type formatter —
/// the same strings CHECK 1 compares elsewhere).
fn scan_batch_op_ops() -> Vec<(String, String)> {
    let c = corpus();
    let rec = c
        .scan
        .get("BatchOp")
        .expect("BatchOp must be defined in the crate source");
    rec.variants
        .iter()
        .map(|v| {
            assert_eq!(
                v.fields.len(),
                1,
                "BatchOp variants are newtype-shaped; variant {} is not",
                v.name
            );
            (v.name.clone(), v.fields[0].ty.clone())
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Ground truth 2: the define_op_kinds! macro invocation
// ---------------------------------------------------------------------------

/// EXTRACTION RULE (documented per the task): parse
/// `src/ops/batch/ops/op_kind.rs` with `syn::parse_file`, find the
/// `Item::Macro` whose macro path is `define_op_kinds`, and read its
/// invocation TokenStream. The macro's grammar is a comma-separated
/// repetition of `IDENT => "LITERAL"` arms (`$($variant:ident =>
/// $key:literal),* $(,)?`), so the token stream is exactly the sequence
/// `Ident, Punct('='), Punct('>'), Literal(string), [Punct(',')]` per arm.
/// Extraction: render the stream to text (token rendering inserts single
/// spaces between tokens and never alters string-literal contents), split
/// on `=>`, then split each arm on the trailing comma; the first word of
/// each arm is the variant ident, the first quoted literal is the
/// discriminator key. Any arm that does not match this shape fails loudly
/// (the macro grammar changed — update this extractor, never guess).
/// Returns the (variant, key) pairs in MACRO ARM order.
fn macro_op_key_pairs() -> Vec<(String, String)> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/ops/batch/ops/op_kind.rs");
    let text =
        fs::read_to_string(&path).unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()));
    let ast =
        syn::parse_file(&text).unwrap_or_else(|e| panic!("cannot parse {}: {e}", path.display()));
    let mut invocations = ast.items.iter().filter_map(|item| match item {
        syn::Item::Macro(m) => {
            if m.mac.path.is_ident("define_op_kinds") {
                Some(&m.mac)
            } else {
                None
            }
        }
        _ => None,
    });
    let mac = invocations
        .next()
        .expect("op_kind.rs must invoke define_op_kinds! exactly once at item level");
    assert!(
        invocations.next().is_none(),
        "define_op_kinds! is invoked more than once in op_kind.rs"
    );
    let rendered = mac.tokens.to_string();
    let mut pairs = Vec::new();
    for arm in rendered.split(',') {
        let arm = arm.trim();
        if arm.is_empty() {
            continue; // trailing comma
        }
        let (ident, lit) = arm.split_once("=>").unwrap_or_else(|| {
            panic!("define_op_kinds! arm `{arm}` is not `IDENT => \"literal\"` — macro grammar changed")
        });
        let ident = ident.trim();
        let lit = lit.trim();
        assert!(
            lit.starts_with('"') && lit.ends_with('"') && lit.len() >= 2,
            "define_op_kinds! arm `{arm}`: right side is not a string literal"
        );
        assert!(
            !ident.contains(' ') && !ident.contains('"'),
            "define_op_kinds! arm `{arm}`: left side is not a bare ident"
        );
        pairs.push((ident.to_string(), lit[1..lit.len() - 1].to_string()));
    }
    pairs
}

// ---------------------------------------------------------------------------
// The compare (Result-returning so the mutation self-test can drive it)
// ---------------------------------------------------------------------------

/// Full structured compare of the snapshot's ops array against both ground
/// truths. `Err` names the entry + "protocol version change required".
fn compare_batch_op_ops(
    scan_ops: &[(String, String)],
    json_ops: &[BatchOpEntry],
    macro_pairs: &[(String, String)],
) -> Result<(), String> {
    // (a) variant + payload sequence == scan declaration order, POSITIONALLY.
    if json_ops.len() != scan_ops.len() {
        return Err(format!(
            "wire type BatchOp ops array length: expected {} (declaration order), actual {} — \
             protocol version change required",
            scan_ops.len(),
            json_ops.len()
        ));
    }
    for (i, ((scan_variant, scan_payload), entry)) in scan_ops.iter().zip(json_ops).enumerate() {
        if &entry.variant != scan_variant || entry.payload != *scan_payload {
            return Err(format!(
                "wire type BatchOp ops entry #{i}: expected (variant `{scan_variant}`, payload \
                 `{scan_payload}`), actual (variant `{}`, payload `{}`) — \
                 protocol version change required",
                entry.variant, entry.payload
            ));
        }
        if entry.operation.is_empty() {
            return Err(format!(
                "wire type BatchOp ops entry #{i} (`{scan_variant}`): `operation` label is \
                 empty — protocol version change required"
            ));
        }
    }
    // (b) discriminator keys == define_op_kinds! arms, KEYED BY VARIANT
    // (macro arm order is a match-arm order, not the declaration order the
    // array pins — see the module doc).
    let macro_by_variant: BTreeMap<&str, &str> = macro_pairs
        .iter()
        .map(|(v, k)| (v.as_str(), k.as_str()))
        .collect();
    if macro_pairs.len() != macro_by_variant.len() {
        return Err(format!(
            "wire type BatchOp: define_op_kinds! declares {} arms but only {} distinct \
             variants — duplicate match arms — protocol version change required",
            macro_pairs.len(),
            macro_by_variant.len()
        ));
    }
    for entry in json_ops {
        let expected = macro_by_variant
            .get(entry.variant.as_str())
            .ok_or_else(|| {
                format!(
                    "wire type BatchOp ops entry (`{}`): no define_op_kinds! arm declares this \
                 variant — protocol version change required",
                    entry.variant
                )
            })?;
        if entry.keys.len() != 1 || &entry.keys[0] != expected {
            return Err(format!(
                "wire type BatchOp ops entry (`{}`) discriminator keys: expected \
                 [\"{expected}\"] from define_op_kinds!, actual {:?} — \
                 protocol version change required",
                entry.variant, entry.keys
            ));
        }
    }
    // every macro arm must be represented in the array too (bijective)
    for (variant, _key) in macro_pairs {
        if !json_ops.iter().any(|e| &e.variant == variant) {
            return Err(format!(
                "wire type BatchOp: define_op_kinds! arm `{variant}` has no ops-array entry — \
                 protocol version change required"
            ));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// (a)+(b): the array's variant+payload sequence equals the scan's
/// declaration order exactly, and every entry's discriminator keys equal
/// the define_op_kinds! arm for its variant.
#[test]
fn batchop_ops_array_matches_scan_order_and_macro_keys() {
    let scan_ops = scan_batch_op_ops();
    let json_ops = load_ops();
    let macro_pairs = macro_op_key_pairs();
    compare_batch_op_ops(&scan_ops, &json_ops, &macro_pairs).unwrap_or_else(|e| panic!("{e}"));
}

/// (c) non-vacuity: the compare has real work to do (93 ops today) and a
/// single in-memory mutation of one `keys` value flips it to Err.
#[test]
fn batchop_compare_is_non_vacuous() {
    let scan_ops = scan_batch_op_ops();
    let json_ops = load_ops();

    // length sanity: the array is pinned to the real variant count and is
    // far too large to pass by accident
    assert_eq!(
        json_ops.len(),
        scan_ops.len(),
        "ops array length must equal the scanned BatchOp variant count"
    );
    assert!(
        json_ops.len() > 50,
        "BatchOp has {} ops — implausibly small; the corpus or scan is broken",
        json_ops.len()
    );

    // mutation self-test: flip ONE entry's keys value in memory → Err
    let macro_pairs = macro_op_key_pairs();
    let mut mutated = json_ops.clone();
    let flip = mutated[0].keys[0].clone();
    mutated[0].keys[0] = format!("{flip}-mutated");
    let err = compare_batch_op_ops(&scan_ops, &mutated, &macro_pairs)
        .expect_err("a mutated discriminator key must fail the compare");
    assert!(
        err.contains("protocol version change required") && err.contains(&mutated[0].variant),
        "mutation failure must name the entry, got: {err}"
    );

    // also: dropping an entry entirely must fail
    let mut truncated = json_ops.clone();
    truncated.pop();
    assert!(
        compare_batch_op_ops(&scan_ops, &truncated, &macro_pairs).is_err(),
        "a shortened ops array must fail the compare"
    );
}

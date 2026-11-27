//! Source-scan boundary gate for the engine-family raw component handles.
//!
//! The `TableManager` accessors `info_store()`, `data_store()`, and
//! `interner()` are `#[doc(hidden)] pub`, reserved for engine-family callers
//! (shamir-engine-repo, shamir-engine-table, shamir-engine,
//! shamir-engine-query, shamir-engine-validator). This test replaces the
//! former compile-fail doctest gate (doctests cannot run under nextest): it
//! walks every `.rs` file in the workspace `crates/` tree and FAILS if any
//! file belonging to a crate OUTSIDE the engine family contains the call
//! substrings `.data_store(`, `.info_store(`, or `.interner(`.
//!
//! Note: `shamir-client-ts` contains no `.rs` files, and the `vendor/hnsw_rs`
//! checkout lives outside `crates/`, so the walk below naturally excludes it.

use std::fs;
use std::path::{Path, PathBuf};

/// Raw-handle call substrings that must never appear outside the engine family.
const FORBIDDEN_SUBSTRINGS: &[&str] = &[".data_store(", ".info_store(", ".interner("];

/// Crate directory names allowed to use the raw handles, with the reason.
const ENGINE_FAMILY: &[&str] = &[
    "shamir-engine-repo",
    "shamir-engine-table",
    "shamir-engine-tx",
    "shamir-engine",
    "shamir-engine-query",
    "shamir-engine-validator",
];

/// Workspace-relative path suffixes exempt from the scan, each with a
/// one-line reason naming the actual receiver type. Kept empty for now:
/// every hit found during the initial audit lives inside the engine family,
/// and no unrelated type shares those method names outside it.
const ALLOWLIST: &[(&str, &str)] = &[];

fn crates_root() -> PathBuf {
    // <workspace>/crates/engine/shamir-engine-table -> <workspace>/crates
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../..")
        .join("crates")
}

fn is_hidden_or_skipped(dir_name: &str) -> bool {
    dir_name == "target" || dir_name == "vendor"
}

fn collect_rs_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    let mut entries: Vec<_> = entries.filter_map(|e| e.ok()).collect();
    entries.sort_by_key(|e| e.file_name());
    for entry in entries {
        let path = entry.path();
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if path.is_dir() {
            if !is_hidden_or_skipped(&name) {
                collect_rs_files(&path, out);
            }
        } else if name.ends_with(".rs") {
            out.push(path);
        }
    }
}

#[test]
fn no_raw_handle_calls_outside_engine_family() {
    let root = crates_root();
    assert!(
        root.is_dir(),
        "workspace crates/ root not found at {}",
        root.display()
    );

    let mut files = Vec::new();
    collect_rs_files(&root, &mut files);
    assert!(
        files.len() > 100,
        "scan found implausibly few .rs files ({}); walk is broken",
        files.len()
    );

    let mut violations = Vec::new();
    for file in &files {
        // Owning crate: crates/<group>/<crate>/..., so the second component
        // of the root-relative path. Never skip silently: an unresolvable
        // path is a broken walk, not a pass.
        let rel = file
            .strip_prefix(&root)
            .unwrap_or_else(|_| panic!("{} is not under {}", file.display(), root.display()));
        let crate_dir = rel
            .components()
            .nth(1)
            .map(|c| c.as_os_str().to_string_lossy().into_owned())
            .unwrap_or_else(|| panic!("{}: no crate directory under its group", rel.display()));
        if ENGINE_FAMILY.contains(&crate_dir.as_str()) {
            continue;
        }

        if ALLOWLIST
            .iter()
            .any(|(suffix, _)| rel == Path::new(suffix) || rel.to_string_lossy().ends_with(suffix))
        {
            continue;
        }

        let Ok(content) = fs::read_to_string(file) else {
            violations.push(format!("{}: unreadable file", rel.display()));
            continue;
        };
        for needle in FORBIDDEN_SUBSTRINGS {
            if content.contains(needle) {
                violations.push(format!(
                    "{}: contains `{}` — raw TableManager handles (info_store/data_store/interner) \
                     are reserved for the engine family (shamir-engine-repo, shamir-engine-table, \
                     shamir-engine-tx, shamir-engine, shamir-engine-query, shamir-engine-validator)",
                    rel.display(),
                    needle
                ));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "engine-family raw-handle boundary violated:\n{}",
        violations.join("\n")
    );
}

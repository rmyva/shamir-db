//! The rights path selector, proven locally (design `admin-shell.md` §4.3):
//! every accepted form resolves to the right target, and every mirrored
//! engine rule is pinned at its exact boundary. The paths here never reach
//! the wire — the selector decides what the shell may address.

use super::{
    normalized_path, parse_selector, Selector, MAX_FUNCTION_NAME_BYTES, MAX_TABLE_NAME_BYTES,
};
use crate::shell::output::ShellError;

/// The `db:root` form selects the DATABASE `root`: a bare `root` keeps its
/// meaning (the synthetic root), so without this form a database that is
/// legally named `root` is unreachable. Mutant: resolving `db:root` as Root.
#[test]
fn explicit_db_form_addresses_a_database_named_root() {
    let selector = parse_selector("db:root").expect("db:root is a database selector");
    assert!(
        matches!(&selector, Selector::Database(name) if name == "root"),
        "db:root must be the database"
    );
    assert_eq!(normalized_path(&selector), "db:root");
    // The bare word keeps meaning the synthetic root.
    assert!(matches!(
        parse_selector("root").expect("root parses"),
        Selector::Root
    ));
    assert!(matches!(
        parse_selector("/").expect("/ parses"),
        Selector::Root
    ));
    // One database component is still a plain database path.
    assert!(matches!(
        parse_selector("root/root").expect("two components parse"),
        Selector::Resource(segments) if segments == ["root", "root"]
    ));
}

/// The `db:` form takes exactly ONE component that satisfies the engine's
/// database rule.
#[test]
fn db_form_rejects_everything_else() {
    for path in ["db:", "db:a/b", "db:a.b", "db:with space", "db:root/"] {
        assert!(
            matches!(parse_selector(path), Err(ShellError::Usage)),
            "path `{path}` must be a local usage refusal"
        );
    }
}

/// A stored function name may contain `/`: `math/abs` is ONE function reached
/// through the folder `math`, not a folder entity. Mutant: rejecting `/` in a
/// `function:NAME` payload, which makes every slash-named function
/// unaddressable.
#[test]
fn slash_named_function_is_addressable() {
    let selector = parse_selector("function:math/abs").expect("a slash-named function parses");
    assert!(
        matches!(&selector, Selector::Function(name) if name == "math/abs"),
        "the registered name is kept verbatim"
    );
    assert_eq!(normalized_path(&selector), "function:math/abs");
    // A deeper folder path, and a plain one-segment function, both parse.
    assert!(matches!(
        parse_selector("function:reports/daily/total").expect("three segments parse"),
        Selector::Function(name) if name == "reports/daily/total"
    ));
    assert!(matches!(
        parse_selector("function:tax_total").expect("one segment parses"),
        Selector::Function(name) if name == "tax_total"
    ));
}

/// The engine stores a function name verbatim and validates nothing about it,
/// so the shell refuses only what it cannot address: an empty name or a
/// control byte. Dots, colons, doubled and leading or trailing slashes are all
/// legal registered names and must stay addressable. Mutant: validating the
/// folder segments or the leaf with the component charset.
#[test]
fn function_names_are_addressable_as_the_engine_stores_them() {
    for path in ["function:", "function:ab\u{1}c", "function:ab\u{7f}"] {
        assert!(
            matches!(parse_selector(path), Err(ShellError::Usage)),
            "path `{path}` must be a local usage refusal"
        );
    }
    for name in [
        "math.v2/abs",
        "math//abs",
        "math/abs:v2",
        "/abs",
        "math/",
        "math/my.func",
        "ma..th/abs",
    ] {
        let selector = parse_selector(&format!("function:{name}"))
            .unwrap_or_else(|_| panic!("`{name}` is a legal stored function name"));
        assert!(
            matches!(&selector, Selector::Function(stored) if stored == name),
            "the registered name is kept verbatim"
        );
    }
}

/// The function-name length cap is exactly [`MAX_FUNCTION_NAME_BYTES`]: the
/// engine caps nothing, so the shell's bound is a sanity bound and one byte
/// over it is a local refusal; a 256-byte name must still parse.
#[test]
fn function_name_length_boundary() {
    let name = |len: usize| format!("function:{}", "a".repeat(len));
    assert!(parse_selector(&name(256)).is_ok());
    assert!(parse_selector(&name(MAX_FUNCTION_NAME_BYTES)).is_ok());
    assert!(
        matches!(
            parse_selector(&name(MAX_FUNCTION_NAME_BYTES + 1)),
            Err(ShellError::Usage)
        ),
        "one byte over the function-name cap is a local refusal"
    );
}

/// The TABLE component is bounded by the loosest physical store name the
/// engine can hold (`MAX_TABLE_NAME_BYTES`, 255 minus the `__history__`
/// prefix): a longer component can never exist, so the shell refuses it
/// instead of signing a request for an impossible target, while every length
/// the engine might accept (a 221-byte table with a short id suffix) is still
/// sent. Mutant: the generic 256-byte ceiling, or a conservative worst-case
/// bound that hides creatable tables.
#[test]
fn table_component_boundary_is_the_physical_name_rule() {
    assert_eq!(MAX_TABLE_NAME_BYTES, 244);
    let path = |len: usize| format!("db1/store1/{}", "t".repeat(len));
    assert!(
        matches!(
            parse_selector(&path(221)).expect("a 221-byte table the engine may accept parses"),
            Selector::Resource(_)
        ),
        "a name longer than the worst-case id suffix allows is still sent"
    );
    assert!(matches!(
        parse_selector(&path(MAX_TABLE_NAME_BYTES)).expect("the exact maximum parses"),
        Selector::Resource(_)
    ));
    assert!(
        matches!(
            parse_selector(&path(MAX_TABLE_NAME_BYTES + 1)),
            Err(ShellError::Usage)
        ),
        "one byte over the physical-name maximum is refused locally"
    );
    // The DATABASE component has no such ceiling: the engine's rule carries
    // no length cap, so a long-but-legal name must not be hidden either.
    let long_db = "d".repeat(MAX_TABLE_NAME_BYTES + 30);
    assert!(
        matches!(
            parse_selector(&long_db).expect("a long database name parses"),
            Selector::Resource(segments) if segments.len() == 1
        ),
        "the database component mirrors the engine rule, which has no length cap"
    );
}

/// A database or store component outside the engine's `[A-Za-z0-9_-]` rule is
/// a local refusal, and so is a fourth component. Mutant: sending the
/// selector through and letting the server decide.
#[test]
fn component_rules_and_depth_are_enforced_locally() {
    for path in ["a.b", "..", ".", "db1//store1", "db1/store1/t/extra", ""] {
        assert!(
            matches!(parse_selector(path), Err(ShellError::Usage)),
            "path `{path}` must be a local usage refusal"
        );
    }
    assert!(matches!(
        parse_selector("db1/store1").expect("two components parse"),
        Selector::Resource(segments) if segments == ["db1", "store1"]
    ));
}

/// The normalized path output names the form the operator used, so a mutation
/// result never claims a different target than the one addressed.
#[test]
fn normalized_path_matches_the_addressed_form() {
    assert_eq!(normalized_path(&Selector::Root), "/");
    assert_eq!(
        normalized_path(&Selector::Database("billing".to_string())),
        "db:billing"
    );
    assert_eq!(
        normalized_path(&Selector::Function("math/abs".to_string())),
        "function:math/abs"
    );
    assert_eq!(
        normalized_path(&Selector::Resource(vec![
            "billing".to_string(),
            "main".to_string(),
            "invoices".to_string()
        ])),
        "billing/main/invoices"
    );
}

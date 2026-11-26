//! Unit test for the peer-text sanitization of the `schema-migration
//! status` projection — the "malicious peer / reflection" guard.
//!
//! A unit test and not a live probe: the shell harness boots a REAL
//! server, so there is no seam to inject a hostile reply into a running
//! session. The strict allowlist projection is the only place peer text
//! can be echoed to the operator, so it is tested directly with a canary
//! carrying an ANSI escape (ESC), a bidi override (U+202E) and a line
//! separator (U+2028).

use serde_json::Value;
use shamir_collections::new_map;
use shamir_types::types::value::QueryValue;

/// Canary carrying an ANSI escape, a bidi override and a line separator.
const HOSTILE: &str = "canary-7f3a\u{1b}[31m\u{202e}\u{2028}tail";

/// A `schema-migration status` reply the `super::project` allowlist
/// accepts, with `HOSTILE` injected into every peer-supplied text field.
fn status_value() -> QueryValue {
    let mut target = new_map();
    target.insert(
        String::from("database_identity"),
        QueryValue::Str(HOSTILE.to_owned()),
    );

    let mut in_flight = new_map();
    in_flight.insert(
        String::from("migration_id"),
        QueryValue::Str(HOSTILE.to_owned()),
    );
    in_flight.insert(String::from("ordinal"), QueryValue::Int(1));
    in_flight.insert(String::from("fence_generation"), QueryValue::Int(1));
    in_flight.insert(String::from("status"), QueryValue::Str(HOSTILE.to_owned()));

    let mut applied_entry = new_map();
    applied_entry.insert(String::from("id"), QueryValue::Str(HOSTILE.to_owned()));
    applied_entry.insert(String::from("ordinal"), QueryValue::Int(1));
    applied_entry.insert(String::from("checksum"), QueryValue::Bin(vec![0xAB; 32]));

    let mut fields = new_map();
    fields.insert(String::from("protocol_version"), QueryValue::Int(1));
    fields.insert(String::from("target"), QueryValue::Map(target));
    fields.insert(
        String::from("last_applied_id"),
        QueryValue::Str(HOSTILE.to_owned()),
    );
    fields.insert(String::from("last_applied_ordinal"), QueryValue::Int(1));
    fields.insert(
        String::from("last_applied_checksum"),
        QueryValue::Bin(vec![0xAB; 32]),
    );
    fields.insert(
        String::from("checksum_validation"),
        QueryValue::Str(String::from("valid")),
    );
    fields.insert(String::from("fence_generation"), QueryValue::Int(1));
    fields.insert(String::from("in_flight"), QueryValue::Map(in_flight));
    fields.insert(
        String::from("applied"),
        QueryValue::List(vec![QueryValue::Map(applied_entry)]),
    );
    QueryValue::Map(fields)
}

/// Every string leaf of a projected JSON value, in document order.
fn string_leaves(value: &Value, out: &mut Vec<String>) {
    match value {
        Value::String(text) => out.push(text.clone()),
        Value::Array(items) => {
            for item in items {
                string_leaves(item, out);
            }
        }
        Value::Object(fields) => {
            for field in fields.values() {
                string_leaves(field, out);
            }
        }
        _ => {}
    }
}

/// `schema-migration status` never reflects a hostile server value, and
/// its top-level shape stays exactly the eleven allowlisted keys.
#[test]
fn schema_migration_status_never_reflects_a_hostile_server_value() {
    let payload =
        super::project(&status_value()).expect("the hostile payload passes the allowlist");
    let serialized = serde_json::to_string(&payload).expect("the projection serializes");
    for character in ['\u{1b}', '\u{202e}', '\u{2028}'] {
        assert!(
            !serialized.contains(character),
            "serialized projection leaked the raw character {character:?}"
        );
    }

    let mut leaves = Vec::new();
    string_leaves(&payload, &mut leaves);
    assert!(
        leaves.iter().all(|leaf| leaf != HOSTILE),
        "a projected string reflected the hostile input verbatim"
    );

    let mut keys: Vec<&str> = payload
        .as_object()
        .expect("the projection returns an object")
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort_unstable();
    assert_eq!(
        keys,
        vec![
            "applied",
            "applied_count",
            "checksum_validation",
            "database_identity",
            "error",
            "fence_generation",
            "in_flight",
            "last_applied_checksum",
            "last_applied_id",
            "last_applied_ordinal",
            "protocol_version",
        ],
        "the projection must emit exactly the eleven allowlisted keys"
    );
}

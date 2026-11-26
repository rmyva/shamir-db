//! Unit regression for the `repo describe` WAL u64 budget mapping
//! (R5-2b item 2): the wire serializes u64 values above `i64::MAX` as
//! `Big`, which MessagePack-carries as a decimal STRING decoded by the
//! client as `QueryValue::Str`. `check_durability` must accept that
//! checked decimal spelling (ASCII digits only, zero/overflow refused),
//! and `write_budget_numbers` must emit the single parsed result as a
//! JSON number — the only u64 fields of the description, in `durability`
//! and `durability_override` (`lineage_id`/`history_epoch` are omitted by
//! `project_repo`; every other numeric in the catalogue views is i64).

use shamir_collections::{new_map, TMap};
use shamir_types::types::value::QueryValue;

use crate::shell::commands::catalogue::{check_durability, Catalogue};
use crate::shell::output::ShellError;

/// Minimal effective policy: the three enum leaves plus the budget.
fn effective_policy(budget: QueryValue) -> QueryValue {
    let mut policy = new_map();
    policy.insert("wal_commit".into(), QueryValue::Str("synced".into()));
    policy.insert(
        "on_wal_retention_exceeded".into(),
        QueryValue::Str("refuse_writes".into()),
    );
    policy.insert(
        "replication_serve".into(),
        QueryValue::Str("durable".into()),
    );
    policy.insert("wal_retention_max_bytes".into(), budget);
    QueryValue::Map(policy)
}

/// Sparse stored override: only the budget (the other leaves are absent).
fn sparse_override(budget: QueryValue) -> QueryValue {
    let mut policy = new_map();
    policy.insert("wal_retention_max_bytes".into(), budget);
    QueryValue::Map(policy)
}

fn fields_for(key: &str, budget: QueryValue) -> TMap<String, QueryValue> {
    let mut fields = new_map();
    fields.insert(key.into(), effective_policy(budget));
    fields
}

/// A full `repo describe` payload: every allowlisted key of `project_repo`.
fn describe_repo_fields(durability: QueryValue, override_: QueryValue) -> TMap<String, QueryValue> {
    let mut fields = new_map();
    fields.insert("describe_repo".into(), QueryValue::Str("r".into()));
    fields.insert("repo".into(), QueryValue::Str("db".into()));
    fields.insert("engine".into(), QueryValue::Str("fjall".into()));
    fields.insert("durability".into(), durability);
    fields.insert("durability_override".into(), override_);
    fields
}

fn project_describe(durability: QueryValue, override_: QueryValue) -> serde_json::Value {
    Catalogue::DescribeRepo { repo: "r".into() }
        .project(
            QueryValue::Map(describe_repo_fields(durability, override_)),
            "db",
        )
        .expect("valid payload projects")
}

/// The canonical `Int` spelling keeps the generic rendering.
#[test]
fn canonical_int_budget_passes_through() {
    let mut fields = fields_for("durability", QueryValue::Int(4096));
    assert_eq!(
        check_durability(&mut fields, "durability", true),
        Ok(Some(4096))
    );
    let json = project_describe(
        effective_policy(QueryValue::Int(4096)),
        sparse_override(QueryValue::Int(4096)),
    );
    assert_eq!(
        json["durability"]["wal_retention_max_bytes"].as_u64(),
        Some(4096)
    );
}

/// The canonical `Big` spelling (u64::MAX) maps to the same JSON number.
#[test]
fn canonical_big_budget_maps_to_json_number() {
    let json = project_describe(
        effective_policy(QueryValue::from(u64::MAX)),
        sparse_override(QueryValue::Int(4096)),
    );
    assert_eq!(
        json["durability"]["wal_retention_max_bytes"].as_u64(),
        Some(u64::MAX)
    );
}

/// REGRESSION: the wire spelling for u64::MAX is a decimal STRING; the
/// projection must emit it as a JSON number (`as_u64() == Some(MAX)`),
/// not echo the string.
#[test]
fn decimal_string_budget_at_u64_max_emits_json_number() {
    let json = project_describe(
        effective_policy(QueryValue::Str(u64::MAX.to_string())),
        sparse_override(QueryValue::Int(4096)),
    );
    assert_eq!(
        json["durability"]["wal_retention_max_bytes"].as_u64(),
        Some(u64::MAX)
    );
}

/// The same mapping applies to the stored sparse `durability_override` map.
#[test]
fn override_budget_string_emits_json_number() {
    let json = project_describe(
        effective_policy(QueryValue::Int(4096)),
        sparse_override(QueryValue::Str("18446744073709551615".into())),
    );
    assert_eq!(
        json["durability_override"]["wal_retention_max_bytes"].as_u64(),
        Some(u64::MAX)
    );
}

/// Malformed, signed, zero and overflowing strings keep the typed error.
#[test]
fn malformed_budget_strings_are_refused() {
    for text in [
        "",
        "12x3",
        " 4096",
        "4096 ",
        "+4096",
        "-1",
        "0",
        "00",
        "18446744073709551616",
    ] {
        let mut fields = fields_for("durability", QueryValue::Str(text.into()));
        assert_eq!(
            check_durability(&mut fields, "durability", true),
            Err(ShellError::Server),
            "text {text:?} must be refused"
        );
    }
}

/// Non-string junk stays refused, and a policy missing an enum leaf is
/// still a violation even with a valid budget.
#[test]
fn non_string_budget_and_incomplete_policy_are_refused() {
    let mut fields = fields_for("durability", QueryValue::Bool(true));
    assert_eq!(
        check_durability(&mut fields, "durability", true),
        Err(ShellError::Server)
    );
    let mut fields = new_map();
    fields.insert(
        "durability".into(),
        sparse_override(QueryValue::Str("4096".into())),
    );
    assert_eq!(
        check_durability(&mut fields, "durability", true),
        Err(ShellError::Server)
    );
}

/// A sparse override (budget only) is valid on the non-effective map.
#[test]
fn sparse_override_without_budget_is_accepted() {
    let mut fields = fields_for("durability_override", QueryValue::Null);
    if let Some(QueryValue::Map(policy)) = fields.get_mut("durability_override") {
        policy.shift_remove("wal_retention_max_bytes");
    }
    assert_eq!(
        check_durability(&mut fields, "durability_override", false),
        Ok(None)
    );
}

/// The write touches ONLY the budget leaves: other fields keep the
/// generic decimal-string rendering.
#[test]
fn write_leaves_other_fields_as_strings() {
    let json = project_describe(
        effective_policy(QueryValue::Int(4096)),
        sparse_override(QueryValue::Int(4096)),
    );
    assert_eq!(json["describe_repo"], "r");
    assert!(json["engine"].is_string());
    assert_eq!(json["db"], "db");
    assert_eq!(
        json["durability_override"]["wal_retention_max_bytes"].as_u64(),
        Some(4096)
    );
}

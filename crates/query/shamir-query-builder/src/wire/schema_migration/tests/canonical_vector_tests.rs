//! Pins schema-migration canonical bytes and checksums to the shared
//! cross-client vector file; every case is rebuilt from the typed DDL
//! builders only. Drift is rejected three ways: the operation variant
//! sequence, every structured language-neutral arg (exact DTO field values,
//! including exact source bytes), and the pinned bytes plus checksum.

use crate::ddl;
use crate::wire::schema_migration::{SchemaMigration, SchemaMigrationSet};
use shamir_query_types::admin::types::schema_migration::canonical_body_checksum;
use shamir_query_types::admin::types::schema_migration::{
    SchemaMigrationBody, SchemaMigrationFunctionDependencies, SchemaMigrationOp,
};
use shamir_query_types::admin::Retention;
use shamir_query_types::batch::BatchOp;

const FIXTURE: &str =
    include_str!("../../../../tests/fixtures/schema_migration_canonical_vectors.json");

fn fixture_cases() -> Vec<serde_json::Value> {
    let value: serde_json::Value = serde_json::from_str(FIXTURE).unwrap();
    value["cases"].as_array().unwrap().clone()
}

fn string_field(case: &serde_json::Value, key: &str) -> String {
    case[key].as_str().unwrap().to_owned()
}

/// Rebuild the typed bodies of one fixture case from the DDL builders.
fn bodies_for_case(case: &serde_json::Value) -> Vec<SchemaMigrationBody> {
    match string_field(case, "name").as_str() {
        "all-nineteen-operation-kinds" => vec![all_nineteen_kinds(case)],
        "function-dependency-ordering-is-canonical" => vec![dependency_ordering(case)],
        "optional-fields-change-canonical-bytes" => vec![optional_fields(case)],
        "operation-order-and-ordinal-pin-set" => operation_order_set(),
        other => panic!("fixture case has no typed reconstruction: {other}"),
    }
}

fn metadata_matches(body: &SchemaMigrationBody, migration: &serde_json::Value) {
    assert_eq!(body.metadata.id, migration["id"].as_str().unwrap());
    assert_eq!(
        body.metadata.ordinal,
        migration["ordinal"].as_u64().unwrap()
    );
    assert_eq!(
        body.metadata.display_name,
        migration["display_name"].as_str().unwrap()
    );
    let described: Vec<&str> = migration["operations"]
        .as_array()
        .unwrap()
        .iter()
        .map(|op| op["variant"].as_str().unwrap())
        .collect();
    let built: Vec<String> = body.operations.iter().map(serde_variant).collect();
    assert_eq!(
        built, described,
        "fixture operation descriptions drifted from the typed reconstruction"
    );
}

fn serde_variant(operation: &SchemaMigrationOp) -> String {
    serde_json::to_value(operation).unwrap()["operation"]
        .as_str()
        .unwrap()
        .to_owned()
}

fn operation_payload(operation: &SchemaMigrationOp) -> serde_json::Value {
    serde_json::to_value(operation).unwrap()["payload"].clone()
}

/// Rule bounds compare by IEEE bits so negative zero stays distinct.
fn assert_rule_bound(rule: &serde_json::Value, key: &str, expected: &serde_json::Value) {
    let actual = rule
        .get(key)
        .unwrap_or_else(|| panic!("rule constraint {key} is missing from the built operation"))
        .as_f64()
        .unwrap_or_else(|| panic!("rule constraint {key} is not an f64"));
    let expected = expected
        .as_f64()
        .unwrap_or_else(|| panic!("fixture rule constraint {key} must be an f64"));
    assert_eq!(
        actual.to_bits(),
        expected.to_bits(),
        "rule constraint {key} drifted (bit-exact comparison, including negative zero)"
    );
}

/// True when the DTO omits the field at its default (skipped null, false or
/// empty list), so an absent payload key is acceptable.
fn is_default_marker(expected: &serde_json::Value) -> bool {
    expected.is_null()
        || expected == &serde_json::Value::Bool(false)
        || expected.as_array().is_some_and(Vec::is_empty)
}

/// Every structured arg must equal the built DTO field; fields at their
/// DTO default may be omitted from the wire payload.
fn assert_args_match(operation: &SchemaMigrationOp, args: &serde_json::Value) {
    let payload = operation_payload(operation);
    let Some(args) = args.as_object() else {
        panic!("operation args must be a JSON object");
    };
    for (key, expected) in args {
        let actual = match key.as_str() {
            "rule_path" => payload["rule"].get("path"),
            "rule_type" => payload["rule"].get("type"),
            "rule_min" | "rule_max" => {
                let bound = key.strip_prefix("rule_").unwrap_or_default();
                assert_rule_bound(&payload["rule"], bound, expected);
                continue;
            }
            "check_source" => payload["check_function"].get("source"),
            plain => payload.get(plain),
        };
        let drifted = match actual {
            None => !is_default_marker(expected),
            Some(actual) => actual != expected,
        };
        assert!(
            !drifted,
            "fixture arg {key} drifted from the built operation: expected {expected}, got {actual:?}"
        );
    }
}

/// Function dependency rows and their name lists are pinned too; the
/// canonical encoding sorts them, so comparison is order-insensitive.
fn dependencies_match(body: &SchemaMigrationBody, migration: &serde_json::Value) {
    let described = migration["function_dependencies"]
        .as_array()
        .expect("fixture migration must describe its function dependencies");
    assert_eq!(
        body.function_dependencies.len(),
        described.len(),
        "fixture function dependency rows drifted from the typed reconstruction"
    );
    for (dependency, expected) in body.function_dependencies.iter().zip(described) {
        assert_eq!(
            dependency.function,
            expected["function"].as_str().unwrap(),
            "fixture dependency function name drifted"
        );
        assert!(
            expected
                .get("description")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|text| !text.is_empty()),
            "fixture dependency row for {} must describe its planning role",
            dependency.function
        );
        let mut built = dependency.depends_on.clone();
        built.sort();
        let mut expected_names: Vec<String> = expected["depends_on"]
            .as_array()
            .unwrap()
            .iter()
            .map(|name| name.as_str().unwrap().to_owned())
            .collect();
        expected_names.sort();
        assert_eq!(
            built, expected_names,
            "fixture dependency names drifted for {}",
            dependency.function
        );
    }
}

fn assert_migration_matches(case: &serde_json::Value, set: &SchemaMigrationSet) {
    let migrations = case["migrations"].as_array().unwrap();
    assert_eq!(
        set.bodies().len(),
        migrations.len(),
        "case {} did not pin every migration it describes",
        string_field(case, "name")
    );
    let bytes = set.canonical_bytes().unwrap();
    let checksums = set.checksums().unwrap();
    for (index, migration) in migrations.iter().enumerate() {
        let body = &set.bodies()[index];
        metadata_matches(body, migration);
        dependencies_match(body, migration);
        let described_ops = migration["operations"].as_array().unwrap();
        for (operation, described) in body.operations.iter().zip(described_ops) {
            let args = described.get("args").unwrap_or_else(|| {
                panic!(
                    "operation {} of case {} carries no structured args",
                    described["variant"].as_str().unwrap(),
                    string_field(case, "name")
                )
            });
            assert_args_match(operation, args);
        }
        assert_eq!(
            hex(&bytes[index]),
            migration["expected_canonical_bytes_hex"].as_str().unwrap(),
            "canonical bytes drifted for case {}",
            string_field(case, "name")
        );
        assert_eq!(
            hex(&checksums[index]),
            migration["expected_checksum_hex"].as_str().unwrap(),
            "checksum drifted for case {}",
            string_field(case, "name")
        );
    }
}

fn all_nineteen_kinds(case: &serde_json::Value) -> SchemaMigrationBody {
    let operations = vec![
        ddl::create_table("t").build(),
        ddl::drop_table("t")
            .hmac("tag")
            .if_exists()
            .build()
            .unwrap(),
        ddl::rename_table("t", "u").build(),
        ddl::create_index("ix", "t")
            .fts(vec!["a".to_owned()], ddl::Tokenizer::Whitespace)
            .unwrap(),
        ddl::drop_index("ix", "t")
            .hmac("tag")
            .if_exists()
            .build()
            .unwrap(),
        ddl::rename_index("t", "ix", "iy").build(),
        ddl::set_table_schema("t").build(),
        ddl::add_schema_rule("t")
            .rule(ddl::field(["a"]).f64_type().min_f64(1.0).max_f64(-0.0))
            .build()
            .unwrap(),
        ddl::remove_schema_rule("t", ["a"]).build(),
        ddl::create_function("f")
            .source("fn f() {}")
            .build()
            .unwrap(),
        ddl::drop_function("f")
            .hmac("tag")
            .if_exists()
            .build()
            .unwrap(),
        ddl::rename_function("f", "g"),
        ddl::check_function().source("fn f() {}").build().unwrap(),
        ddl::alter_function("f", "never"),
        ddl::create_validator("v")
            .source("fn validate() {}")
            .build()
            .unwrap(),
        ddl::drop_validator("v")
            .hmac("tag")
            .if_exists()
            .build()
            .unwrap(),
        ddl::rename_validator("v", "w"),
        ddl::bind_validator("v", "t")
            .db("db")
            .ops([ddl::WriteOp::Insert, ddl::WriteOp::Update])
            .build(),
        ddl::unbind_validator("v", "t").db("db").build(),
    ];
    build_single(case, Vec::new(), operations)
}

fn dependency_ordering(case: &serde_json::Value) -> SchemaMigrationBody {
    let dependencies = vec![
        SchemaMigrationFunctionDependencies {
            function: "zeta_fn".to_owned(),
            depends_on: vec!["b_dep".to_owned(), "a_dep".to_owned()],
        },
        SchemaMigrationFunctionDependencies {
            function: "alpha_fn".to_owned(),
            depends_on: vec!["solo".to_owned()],
        },
    ];
    let operations = vec![
        ddl::create_function("zeta_fn")
            .source("fn zeta_fn() {}")
            .build()
            .unwrap(),
        ddl::create_validator("alpha_v")
            .source("fn alpha_v() {}")
            .build()
            .unwrap(),
    ];
    build_single(case, dependencies, operations)
}

fn optional_fields(case: &serde_json::Value) -> SchemaMigrationBody {
    let operations = vec![
        ddl::create_table("opts_t")
            .if_not_exists()
            .retention(Retention {
                max_age_secs: Some(3600),
                max_count: None,
                min_count: Some(10),
            })
            .build(),
        ddl::create_function("opts_f")
            .source("fn opts_f() {}")
            .replace()
            .visibility("public")
            .optimize("now")
            .hmac("tag")
            .build()
            .unwrap(),
        ddl::set_table_schema("opts_t").expected_version(7).build(),
        ddl::bind_validator("opts_v", "opts_t")
            .db("db")
            .ops([ddl::WriteOp::Delete, ddl::WriteOp::Upsert])
            .priority(2000)
            .build(),
        ddl::drop_table("opts_t").hmac("tag").build().unwrap(),
    ];
    build_single(case, Vec::new(), operations)
}

fn operation_order_set() -> Vec<SchemaMigrationBody> {
    let first = SchemaMigration::new("order-a", 1, "order first")
        .op(ddl::rename_table("oa", "ob").build())
        .unwrap()
        .op(ddl::rename_function("oa_f", "oa_g"))
        .unwrap()
        .op(ddl::unbind_validator("oa_v", "oa_t").db("db").build())
        .unwrap()
        .build()
        .unwrap();
    let second = SchemaMigration::new("order-b", 2, "order second")
        .op(ddl::unbind_validator("ob_v", "ob_t").db("db").build())
        .unwrap()
        .op(ddl::rename_function("ob_f", "ob_g"))
        .unwrap()
        .op(ddl::rename_table("ob", "oc").build())
        .unwrap()
        .build()
        .unwrap();
    vec![first, second]
}

fn build_single(
    case: &serde_json::Value,
    dependencies: Vec<SchemaMigrationFunctionDependencies>,
    operations: Vec<BatchOp>,
) -> SchemaMigrationBody {
    let migration = &case["migrations"][0];
    let mut builder = SchemaMigration::new(
        migration["id"].as_str().unwrap(),
        migration["ordinal"].as_u64().unwrap(),
        migration["display_name"].as_str().unwrap(),
    )
    .dependencies(dependencies);
    for operation in operations {
        builder = builder.op(operation).unwrap();
    }
    builder.build().unwrap()
}

#[test]
fn rust_migration_body_canonical_vectors_match_file() {
    let cases = fixture_cases();
    assert_eq!(cases.len(), 4, "the shared vector file holds four cases");
    for case in &cases {
        let set = SchemaMigrationSet::new(bodies_for_case(case)).unwrap();
        assert_migration_matches(case, &set);
    }
    // Order-isolation control: identical metadata and identical operations
    // with two adjacent operations swapped must produce different checksums,
    // proving the pinned set case distinguishes order and not just identity.
    order_swap_control();
}

/// Identical metadata, identical operation set, two authored orders.
fn order_swap_control() {
    let build = |validator_first: bool| -> SchemaMigrationBody {
        let builder = SchemaMigration::new("order-swap", 1, "same metadata");
        let builder = if validator_first {
            builder.op(ddl::rename_validator("sw_v", "sw_w")).unwrap()
        } else {
            builder
                .op(ddl::rename_table("sw_t", "sw_u").build())
                .unwrap()
        };
        let builder = if validator_first {
            builder
                .op(ddl::rename_table("sw_t", "sw_u").build())
                .unwrap()
        } else {
            builder.op(ddl::rename_validator("sw_v", "sw_w")).unwrap()
        };
        builder
            .op(ddl::rename_function("sw_f", "sw_g"))
            .unwrap()
            .build()
            .unwrap()
    };
    let straight = canonical_body_checksum(&build(false)).unwrap();
    let swapped = canonical_body_checksum(&build(true)).unwrap();
    assert_ne!(
        straight, swapped,
        "swapping two operations with identical metadata must change the checksum"
    );
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

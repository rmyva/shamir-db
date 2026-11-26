//! Parent UPDATE/DELETE issued through the PUBLIC `ShamirDb` API must still
//! succeed while the FK CHILD table's rename is in doubt: the write path's
//! read-only walks (reverse-FK cache rebuild) resolve the fenced child name
//! through `DbTableResolver::resolve_unfenced`, so the fence must not break
//! ordinary writes on the parent.

use crate::access::{Actor, ResourceMeta};
use crate::shamir_db::shamir_db::catalog::validate_catalogue_db_names;
use crate::shamir_db::shamir_db::is_reserved_db_name;
use crate::shamir_db::system_store::DatabaseIdentity;
use crate::shamir_db::ShamirDb;
use shamir_query_builder::batch::Batch;
use shamir_query_builder::ddl;
use shamir_query_builder::filter;
use shamir_query_builder::write;
use shamir_query_builder::write::doc;
use shamir_query_builder::write::UpdateReturnMode;
use shamir_types::mpack;
use shamir_types::types::common::new_map;
use shamir_types::types::record_id::RecordId;
use shamir_types::types::value::QueryValue;

#[tokio::test]
async fn parent_update_and_delete_succeed_while_child_rename_in_doubt() {
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        let db = ShamirDb::init_memory().await.unwrap();
        db.create_db("fk-walk").await.unwrap();

        // Real repo with a real FK: employees.dept_id -> departments.dept_id
        // (NoAction, so the parent ops themselves are allowed to proceed).
        let mut b = Batch::new();
        b.id(1);
        b.create_repo(
            "cr",
            ddl::create_repo("main").tables(["departments", "employees"]),
        );
        db.execute(
            "fk-walk",
            &b.to_request_via_msgpack().expect("batch request encoding"),
        )
        .await
        .unwrap();

        let fk_id = RecordId::system("fk_walk_child_schema");
        let rules = vec![shamir_engine::validator::schema::FieldRule {
            path: vec!["dept_id".to_string()],
            ty: shamir_engine::validator::schema::type_tag::TypeTag::Int,
            constraints: shamir_engine::validator::schema::constraints::Constraints {
                foreign_key: Some(
                    shamir_engine::validator::schema::ForeignKeyRef::with_on_delete(
                        "departments",
                        "dept_id",
                        shamir_query_types::admin::FkAction::NoAction,
                    ),
                ),
                required: true,
                ..Default::default()
            },
            keyset_safe: false,
        }];
        db.compile_table_schema("fk-walk", "main", "employees", fk_id, rules)
            .await
            .expect("child FK schema activation must succeed");

        // Seed one parent row + one referencing child row via the public API.
        let mut b = Batch::new();
        b.id(2);
        b.insert(
            "ins_dept",
            write::insert("departments").row(mpack!({ "dept_id": 100, "name": "Engineering" })),
        );
        db.execute(
            "fk-walk",
            &b.to_request_via_msgpack().expect("batch request encoding"),
        )
        .await
        .unwrap();

        let mut b = Batch::new();
        b.id(3);
        b.insert(
            "ins_emp",
            write::insert("employees").row(mpack!({ "name": "Alice", "dept_id": 100 })),
        );
        db.execute(
            "fk-walk",
            &b.to_request_via_msgpack().expect("batch request encoding"),
        )
        .await
        .unwrap();

        // Force the CHILD rename into the in-doubt (fenced) state: the direct
        // engine-level `rename_table_stores_with` + `InDoubt` outcome (same
        // pattern as the engine walk tests; `rename_table_as` itself refuses
        // schema-bearing tables, and the FK child carries a schema).
        let repo = db.get_db("fk-walk").unwrap().get_repo("main").unwrap();
        assert!(matches!(
            repo.rename_table_stores_with("employees", "employees2", |_| async {
                crate::engine::repo::RenamePersistOutcome::InDoubt(crate::DbError::Storage(
                    "uncertain".into(),
                ))
            })
            .await,
            Err(crate::DbError::Conflict(message)) if message.starts_with("rename in doubt:")
        ));
        assert!(repo.has_rename_in_doubt());
        assert!(matches!(
            repo.get_table("employees").await,
            Err(crate::DbError::Conflict(message)) if message.starts_with("rename in doubt:")
        ));

        // The setup inserts warmed the reverse-FK cache; drop it so the parent
        // operations below must rebuild it through `resolve_unfenced`.
        repo.fk_reverse_cache().invalidate();

        // Parent UPDATE via the public execute path: its reverse-FK walk must
        // resolve the fenced child through `resolve_unfenced` and succeed.
        let mut b = Batch::new();
        b.id(4);
        b.try_update(
            "upd_dept",
            write::update("departments")
                .where_(filter::eq("dept_id", 100))
                .set(doc().set("name", "Platform").unwrap())
                .returning(UpdateReturnMode::All),
        )
        .unwrap();
        let resp = db
            .execute(
                "fk-walk",
                &b.to_request_via_msgpack().expect("batch request encoding"),
            )
            .await
            .expect("parent update must succeed while the child is fenced");
        assert_eq!(resp.results["upd_dept"].records.len(), 1);

        // Parent DELETE likewise: the read-only FK discovery walk must not
        // trip over the fence.
        repo.fk_reverse_cache().invalidate();
        let mut b = Batch::new();
        b.id(5);
        b.try_delete(
            "del_dept",
            write::delete("departments")
                .where_(filter::eq("dept_id", 100))
                .returning(),
        )
        .unwrap();
        let resp = db
            .execute(
                "fk-walk",
                &b.to_request_via_msgpack().expect("batch request encoding"),
            )
            .await
            .expect("parent delete must succeed while the child is fenced");
        assert_eq!(resp.results["del_dept"].records.len(), 1);
    })
    .await
    .expect("fk walk test exceeded 30 seconds");
}
#[test]
fn reserved_database_names_apply_only_documented_normalization() {
    for name in [
        "users",
        "Users",
        "USERS",
        "users.",
        "users ",
        "USERS.",
        "shamir_db_meta.redb",
        "__SYSTEM__",
        "__system__",
        "Users.",
        "server_meta",
        "cache",
        "counters",
        "audit",
    ] {
        assert!(is_reserved_db_name(name), "{name:?} should be reserved");
    }
    for name in [
        "users2",
        "my_users",
        "audit_log",
        "users_dir",
        "cache_server",
        "server_meta2",
        "USERS~1",
        "user",
        "",
        "users .x",
    ] {
        assert!(!is_reserved_db_name(name), "{name:?} should be accepted");
    }
}

#[test]
fn catalogue_validation_fails_closed_except_for_system_database() {
    let record = |name: &str| {
        let mut fields = new_map();
        fields.insert("name".into(), QueryValue::Str(name.into()));
        QueryValue::Map(fields)
    };
    for name in ["users", "users ", "Users"] {
        let error = validate_catalogue_db_names(&[record(name)]).unwrap_err();
        assert!(error.to_string().contains(name));
        assert!(error.to_string().contains("previous release"));
    }
    assert!(validate_catalogue_db_names(&[record("__system__")]).is_ok());
    assert!(validate_catalogue_db_names(&[record("users2")]).is_ok());
    assert!(validate_catalogue_db_names(&[]).is_ok());
}

#[tokio::test]
async fn rename_refuses_reserved_destination() {
    let db = ShamirDb::init_memory().await.unwrap();
    db.create_db("ordinary").await.unwrap();
    let error = db
        .rename_db("ordinary", "users")
        .await
        .expect_err("reserved destination must be refused");
    assert!(matches!(error, crate::DbError::Validation(_)));
    assert!(error.to_string().contains("reserved"));
}
#[tokio::test]
async fn rename_refuses_normalized_reserved_destination_spellings() {
    let db = ShamirDb::init_memory().await.unwrap();
    db.create_db("ordinary").await.unwrap();
    // Exact `__system__`-only checks would accept the normalized reserved spellings.
    for name in ["Users", "USERS", "users.", "users ", "audit"] {
        let error = db
            .rename_db("ordinary", name)
            .await
            .expect_err("reserved destination must be refused");
        assert!(matches!(error, crate::DbError::Validation(_)));
        assert!(error.to_string().contains("reserved"));
    }
}

#[tokio::test]
async fn create_accepts_non_reserved_names() {
    let db = ShamirDb::init_memory().await.unwrap();
    // Prefix/substring matching would incorrectly reject these valid names.
    for name in ["users2", "my_users", "audit_log"] {
        db.create_db(name).await.unwrap();
        assert!(db.has_db(name));
    }
}

#[tokio::test]
async fn rename_from_reserved_name_still_works() {
    let db = ShamirDb::init_memory().await.unwrap();
    let identity = DatabaseIdentity::new();
    let mut fields = new_map();
    fields.insert("name".into(), QueryValue::Str("users".into()));
    fields.insert("identity".into(), QueryValue::Str(identity.to_string()));
    let record = QueryValue::Map(fields);
    db.system_store()
        .save_database(
            "users",
            &record,
            &ResourceMeta::owned_enforced(Actor::System),
        )
        .await
        .unwrap();
    db.dbs.insert(
        "users".to_string(),
        crate::engine::db_instance::db_instance::DbInstance::new(),
    );
    db.database_identities.insert("users".to_string(), identity);

    db.rename_db("users", "users_renamed").await.unwrap();

    assert!(db.has_db("users_renamed"));
    assert!(!db.has_db("users"));
}

#[tokio::test]
async fn create_refuses_reserved_names_with_validation_error() {
    let db = ShamirDb::init_memory().await.unwrap();
    for name in ["users", "Users", "USERS", "users."] {
        let result = db.create_db_as_checked(name, Actor::System).await;
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("reserved name {name:?} must be refused"),
        };
        assert!(matches!(error, crate::DbError::Validation(_)));
        assert!(error.to_string().contains("reserved"));
    }
}

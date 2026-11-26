//! Tests for admin target-existence validation.
//!
//! Originally task #543, which scoped this validation down to nothing on the
//! user/owner side (owner/group/member ids were treated as free-standing
//! numeric identifiers that need NOT correspond to a persisted record —
//! enforcing existence broke 14+ pre-existing tests across `shamir-db` and
//! `shamir-server`). Task #561 re-enables it, now coherent because task #559
//! landed a real `PrincipalResolver` trait (`ports.rs`) backed by the durable
//! `FjallUserDirectory`, injected into `ShamirDb` via
//! `.with_principal_resolver(...)`.
//!
//! What THIS module now proves is actually enforced (task #561):
//!
//! - **`chown` owner-id check — resolver-GATED.** When a `PrincipalResolver`
//!   is installed, `chown` to a non-`OWNER_SYSTEM` owner id that does NOT
//!   resolve via the resolver is rejected with `invalid_owner`; when no
//!   resolver is installed the check is skipped (permissive fallback, so
//!   core ACL ops still work in embedded/no-directory deployments and the
//!   many tests that build a bare `ShamirDb` without a port). The
//!   `OWNER_SYSTEM` lockout guard (a non-System actor cannot hand a resource
//!   to System) is separate, unconditional, and unchanged.
//! - **`add_group_member` member-id check — resolver-GATED.** Same shape:
//!   with a resolver installed, adding a user id that doesn't resolve is
//!   rejected with `invalid_owner`; without one it's allowed. The GROUP side
//!   of this op is validated unconditionally via `group_id_exists` (see
//!   `add_group_member_nonexistent_group_id_is_rejected`).
//! - **`chgrp` group-id check — UNCONDITIONAL.** Groups have ALWAYS been
//!   directly, id-keyed checkable via `group_id_exists` (a point lookup on
//!   `load_group`, not a scan), so this check never depended on the
//!   identity-model work that blocked the user-side checks. `chgrp(path,
//!   Some(gid))` to a gid with no persisted group is rejected with
//!   `invalid_owner`; `chgrp(path, None)` (clearing the group) is never
//!   checked.

use shamir_query_builder::batch::Batch;
use shamir_query_builder::ddl;

use crate::engine::repo::repo_types::BoxRepoFactory;
use crate::engine::repo::RepoConfig;
use crate::engine::table::TableConfig;
use crate::query::batch::{BatchError, BatchRequest, BatchResponse};
use crate::shamir_db::{PrincipalInfo, PrincipalResolver, ShamirDb};
use shamir_types::access::{
    principal64_from_username, Actor, ResourceMeta, ResourcePath, OWNER_SYSTEM,
};
use shamir_types::types::value::QueryValue;
use std::sync::Arc;

/// In-memory `ShamirDb` with `testdb` / `main` / `items` and NO
/// `PrincipalResolver` installed. Uses `ShamirDb::add_repo` (not
/// `DbInstance::add_repo`) so the table catalogue record is persisted to the
/// system store — `resource_meta` / `set_resource_meta` for a `Table` path
/// require the catalogue record to exist (see `access_tree_tests.rs`'s
/// `setup` for the same pattern). The absent resolver is the permissive /
/// degraded deployment shape: §1/§3's user-target checks are no-ops here.
async fn setup() -> ShamirDb {
    let shamir = ShamirDb::init_memory().await.unwrap();
    shamir.create_db("testdb").await.expect("create_db");
    let repo_config =
        RepoConfig::new("main", BoxRepoFactory::in_memory()).add_table(TableConfig::new("items"));
    shamir.add_repo("testdb", repo_config).await.unwrap();
    shamir
}

/// Same as [`setup`] but with a [`MockResolver`] installed, so §1/§3's
/// user-target existence checks are ACTIVE. Mirrors the mock-resolver
/// pattern from `access_tree_tests.rs`'s `MockAliceResolver` / task #559.
async fn setup_with_resolver() -> ShamirDb {
    let shamir = ShamirDb::init_memory()
        .await
        .unwrap()
        .with_principal_resolver(Arc::new(MockResolver));
    shamir.create_db("testdb").await.expect("create_db");
    let repo_config =
        RepoConfig::new("main", BoxRepoFactory::in_memory()).add_table(TableConfig::new("items"));
    shamir.add_repo("testdb", repo_config).await.unwrap();
    shamir
}

/// Test-only mock resolver: resolves exactly the two fixture principals
/// ("alice", "bob") by their `principal64_from_username` projection key, and
/// `None` for everything else. Mirrors `access_tree_tests.rs`'s
/// `MockAliceResolver` (task #559's established mock pattern for THIS crate);
/// extended to two users so a "resolves" vs "doesn't resolve" contrast is
/// possible within one test.
struct MockResolver;

impl PrincipalResolver for MockResolver {
    fn resolve(&self, p64: u64) -> Option<PrincipalInfo> {
        known_principals()
            .into_iter()
            .find(|p| p.principal64 == p64)
    }
    fn list(&self) -> Vec<PrincipalInfo> {
        known_principals()
    }
}

/// The principal set [`MockResolver`] admits. Deterministic, derived from the
/// same `principal64_from_username` projection the real directory uses.
fn known_principals() -> Vec<PrincipalInfo> {
    ["alice", "bob"]
        .iter()
        .map(|name| PrincipalInfo {
            principal64: principal64_from_username(name),
            name: name.to_string(),
            user_id: [0u8; 16],
            database: None,
            superuser: false,
        })
        .collect()
}

/// Derive a deterministic `principal64` for a test fixture user name. With no
/// resolver installed the codebase convention treats numeric ids as
/// free-standing identifiers; with a resolver installed the id must resolve —
/// [`MockResolver`] resolves the names this helper produces.
async fn seed_user(_shamir: &ShamirDb, name: &str) -> u64 {
    principal64_from_username(name)
}

// ============================================================================
// chown — resolver-gated owner existence check (§1) + OWNER_SYSTEM guard
// ============================================================================

/// **Degrade (no resolver):** `chown` to a user id with no persisted user
/// record still succeeds — without a `PrincipalResolver` installed §1's check
/// is a no-op (permissive fallback preserved). Regression guard: a future
/// change that accidentally makes the check unconditional would break this.
#[tokio::test]
async fn chown_to_nonexistent_user_succeeds_without_resolver() {
    let shamir = setup().await;

    let mut b = Batch::new();
    b.id(1);
    b.chown(
        "co",
        ddl::chown(ddl::res::table("testdb", "main", "items"), 999_999)
            .hmac("test")
            .build()
            .expect("build op"),
    );
    let req = b.to_request_via_msgpack().expect("batch request encoding");

    shamir
        .execute("testdb", &req)
        .await
        .expect("chown to a numeric id with no resolver installed must still succeed");

    let meta = shamir
        .resource_meta(&ResourcePath::table("testdb", "main", "items"))
        .await
        .unwrap();
    assert_eq!(meta.owner, Actor::User(999_999));
}

/// **Negative (resolver installed):** `chown` to an owner id that does NOT
/// resolve via the installed resolver is rejected with `invalid_owner`
/// (task #561 §1).
#[tokio::test]
async fn chown_to_nonexistent_user_rejected_with_resolver() {
    let shamir = setup_with_resolver().await;

    let mut b = Batch::new();
    b.id(1);
    b.chown(
        "co",
        ddl::chown(ddl::res::table("testdb", "main", "items"), 999_999)
            .hmac("test")
            .build()
            .expect("build op"),
    );
    let req = b.to_request_via_msgpack().expect("batch request encoding");

    let err = shamir.execute("testdb", &req).await.expect_err(
        "chown to an unresolvable owner id must be rejected when a resolver is installed",
    );
    assert_eq!(err.code(), Some("invalid_owner"));
}

/// **Positive (resolver installed):** `chown` to an owner id that DOES resolve
/// via the installed resolver succeeds and the meta reflects the new owner.
#[tokio::test]
async fn chown_to_existing_user_succeeds_with_resolver() {
    let shamir = setup_with_resolver().await;
    let alice = seed_user(&shamir, "alice").await;

    let mut b = Batch::new();
    b.id(1);
    b.chown(
        "co",
        ddl::chown(ddl::res::table("testdb", "main", "items"), alice)
            .hmac("test")
            .build()
            .expect("build op"),
    );
    let req = b.to_request_via_msgpack().expect("batch request encoding");

    shamir
        .execute("testdb", &req)
        .await
        .expect("chown to a resolvable owner id must succeed");

    let meta = shamir
        .resource_meta(&ResourcePath::table("testdb", "main", "items"))
        .await
        .unwrap();
    assert_eq!(meta.owner, Actor::User(alice));
}

/// `chown` to `OWNER_SYSTEM` (`0`) by a non-System actor must be rejected
/// — a one-way footgun/DoS lockout (only `Actor::System` could manage the
/// resource afterwards). Unconditional; orthogonal to §1's resolver check.
#[tokio::test]
async fn chown_to_system_by_non_system_actor_is_rejected() {
    let shamir = setup().await;
    let alice = seed_user(&shamir, "alice").await;

    // Ancestors (database, store) default to enforced/System-owned — open
    // them so alice's ownership of the *table* is the sole gate under
    // test (mirrors the pattern in facade_gateway_acl_tests.rs).
    let open = ResourceMeta::open();
    shamir
        .set_resource_meta(&ResourcePath::database("testdb"), &open)
        .await
        .unwrap();
    shamir
        .set_resource_meta(&ResourcePath::store("testdb", "main"), &open)
        .await
        .unwrap();

    // Make alice the owner + grant her Manage so she can reach the
    // chown handler at all.
    let meta = ResourceMeta {
        owner: Actor::User(alice),
        group: None,
        mode: 0o700,
    };
    shamir
        .set_resource_meta(&ResourcePath::table("testdb", "main", "items"), &meta)
        .await
        .unwrap();

    let mut b = Batch::new();
    b.id(1);
    b.chown(
        "co",
        ddl::chown(ddl::res::table("testdb", "main", "items"), OWNER_SYSTEM)
            .hmac("test")
            .build()
            .expect("build op"),
    );
    let req = b.to_request_via_msgpack().expect("batch request encoding");

    let err = shamir
        .execute_as(Actor::User(alice), "testdb", &req)
        .await
        .expect_err("chown to OWNER_SYSTEM by a non-System actor must be rejected");
    assert_eq!(err.code(), Some("invalid_owner"));

    let meta = shamir
        .resource_meta(&ResourcePath::table("testdb", "main", "items"))
        .await
        .unwrap();
    assert_eq!(
        meta.owner,
        Actor::User(alice),
        "resource must still be owned by alice, not System"
    );
}

/// `chown` to `OWNER_SYSTEM` by `Actor::System` itself must still succeed
/// — this is the legitimate path (System reassigning a resource to
/// itself / an admin root session making a resource System-owned).
#[tokio::test]
async fn chown_to_system_by_system_actor_succeeds() {
    let shamir = setup().await;
    let alice = seed_user(&shamir, "alice").await;

    let meta = ResourceMeta {
        owner: Actor::User(alice),
        group: None,
        mode: 0o700,
    };
    shamir
        .set_resource_meta(&ResourcePath::table("testdb", "main", "items"), &meta)
        .await
        .unwrap();

    let mut b = Batch::new();
    b.id(1);
    b.chown(
        "co",
        ddl::chown(ddl::res::table("testdb", "main", "items"), OWNER_SYSTEM)
            .hmac("test")
            .build()
            .expect("build op"),
    );
    let req = b.to_request_via_msgpack().expect("batch request encoding");

    // `execute` = `execute_as(Actor::System, ...)`.
    shamir
        .execute("testdb", &req)
        .await
        .expect("System actor chowning a resource to System must succeed");

    let meta = shamir
        .resource_meta(&ResourcePath::table("testdb", "main", "items"))
        .await
        .unwrap();
    assert_eq!(meta.owner, Actor::System);
}

/// `chown` to `OWNER_SYSTEM` by `Actor::Admin` (a real superuser wire
/// session — `session_actor` maps every live superuser session to
/// `Actor::Admin(principal64(..))`, never to bare `Actor::System`, task
/// #555) must ALSO succeed — regression test for a gap found during #555's
/// adversarial review: the lockout guard originally checked
/// `self.actor != Actor::System` only, which made this legitimate path
/// unconditionally rejected for every real admin session once
/// `Actor::Admin` existed.
#[tokio::test]
async fn chown_to_system_by_admin_actor_succeeds() {
    let shamir = setup().await;
    let alice = seed_user(&shamir, "alice").await;

    let meta = ResourceMeta {
        owner: Actor::User(alice),
        group: None,
        mode: 0o700,
    };
    shamir
        .set_resource_meta(&ResourcePath::table("testdb", "main", "items"), &meta)
        .await
        .unwrap();

    let mut b = Batch::new();
    b.id(1);
    b.chown(
        "co",
        ddl::chown(ddl::res::table("testdb", "main", "items"), OWNER_SYSTEM)
            .hmac("test")
            .build()
            .expect("build op"),
    );
    let req = b.to_request_via_msgpack().expect("batch request encoding");

    shamir
        .execute_as(Actor::Admin(999), "testdb", &req)
        .await
        .expect("Admin actor chowning a resource to System must succeed");

    let meta = shamir
        .resource_meta(&ResourcePath::table("testdb", "main", "items"))
        .await
        .unwrap();
    assert_eq!(meta.owner, Actor::System);
}

// ============================================================================
// chgrp — UNCONDITIONAL group existence check (§2)
// ============================================================================

/// **Negative (unconditional):** `chgrp` to a group id with no persisted
/// group record is rejected with `invalid_owner` (task #561 §2). This check
/// needs no resolver — groups are id-keyed and directly checkable via
/// `group_id_exists`.
#[tokio::test]
async fn chgrp_to_nonexistent_group_is_rejected() {
    let shamir = setup().await;

    let mut b = Batch::new();
    b.id(1);
    b.chgrp(
        "cg",
        ddl::chgrp(ddl::res::table("testdb", "main", "items"), Some(999_999))
            .hmac("test")
            .build()
            .expect("build op"),
    );
    let req = b.to_request_via_msgpack().expect("batch request encoding");

    let err = shamir
        .execute("testdb", &req)
        .await
        .expect_err("chgrp to a nonexistent group id must be rejected");
    assert_eq!(err.code(), Some("invalid_owner"));

    // No phantom group record must have been fabricated at the dangling id.
    let group = shamir.system_store().load_group(999_999).await.unwrap();
    assert!(
        group.is_none(),
        "a phantom group record must not have been created for the dangling id"
    );
}

/// `chgrp` to `None` (clearing the group) needs no validation and must
/// always succeed, even with no groups created at all.
#[tokio::test]
async fn chgrp_to_none_clears_unconditionally() {
    let shamir = setup().await;
    let gid = shamir.create_group("devs").await.unwrap();

    // First set a real group so there is something to clear.
    let meta = ResourceMeta {
        owner: Actor::System,
        group: Some(gid),
        mode: 0o755,
    };
    shamir
        .set_resource_meta(&ResourcePath::table("testdb", "main", "items"), &meta)
        .await
        .unwrap();

    let mut b = Batch::new();
    b.id(1);
    b.chgrp(
        "cg",
        ddl::chgrp(ddl::res::table("testdb", "main", "items"), None)
            .hmac("test")
            .build()
            .expect("build op"),
    );
    let req = b.to_request_via_msgpack().expect("batch request encoding");

    shamir
        .execute("testdb", &req)
        .await
        .expect("chgrp to None (clear) must always succeed");

    let meta = shamir
        .resource_meta(&ResourcePath::table("testdb", "main", "items"))
        .await
        .unwrap();
    assert_eq!(meta.group, None);
}

/// **Positive (unconditional):** `chgrp` to a real, existing group id
/// succeeds and the meta reflects the new group.
#[tokio::test]
async fn chgrp_to_existing_group_succeeds() {
    let shamir = setup().await;
    let gid = shamir.create_group("devs").await.unwrap();

    let mut b = Batch::new();
    b.id(1);
    b.chgrp(
        "cg",
        ddl::chgrp(ddl::res::table("testdb", "main", "items"), Some(gid))
            .hmac("test")
            .build()
            .expect("build op"),
    );
    let req = b.to_request_via_msgpack().expect("batch request encoding");

    shamir
        .execute("testdb", &req)
        .await
        .expect("chgrp to an existing group must succeed");

    let meta = shamir
        .resource_meta(&ResourcePath::table("testdb", "main", "items"))
        .await
        .unwrap();
    assert_eq!(meta.group, Some(gid));
}

// ============================================================================
// add_group_member — resolver-gated member existence check (§3) +
// unconditional group-id check
// ============================================================================

/// `add_group_member` against a `GroupRef::Id` that resolves to no
/// persisted group must be rejected (unconditional group-side check).
/// `resolve_group_id` only validates existence for `GroupRef::Name` (a scan);
/// the `Id` variant passes the id straight through with no check, so without
/// `group_id_exists` here `system_store::add_group_member` would silently
/// fabricate a phantom group record for the dangling id.
#[tokio::test]
async fn add_group_member_nonexistent_group_id_is_rejected() {
    let shamir = setup().await;
    let alice = seed_user(&shamir, "alice").await;

    let mut b = Batch::new();
    b.id(1);
    b.add_group_member(
        "agm",
        ddl::add_group_member(ddl::GroupRef::Id { id: 999_999 }, alice)
            .hmac("test")
            .build()
            .expect("build op"),
    );
    let req = b.to_request_via_msgpack().expect("batch request encoding");

    let err = shamir
        .execute("testdb", &req)
        .await
        .expect_err("add_group_member against a nonexistent group id must be rejected");
    assert_eq!(err.code(), Some("invalid_owner"));

    // No phantom group record must have been fabricated at the dangling id.
    let group = shamir.system_store().load_group(999_999).await.unwrap();
    assert!(
        group.is_none(),
        "a phantom group record must not have been created for the dangling id"
    );
}

/// **Degrade (no resolver):** `add_group_member` with a numeric user id that
/// has no persisted user record still succeeds when no resolver is installed
/// — §3's check is a no-op here (permissive fallback preserved). The GROUP
/// side is still validated (see the test above). Regression guard against an
/// accidentally-unconditional member check.
#[tokio::test]
async fn add_group_member_nonexistent_user_succeeds_without_resolver() {
    let shamir = setup().await;
    let gid = shamir.create_group("devs").await.unwrap();

    let mut b = Batch::new();
    b.id(1);
    b.add_group_member(
        "agm",
        ddl::add_group_member(ddl::GroupRef::Id { id: gid }, 999_999)
            .hmac("test")
            .build()
            .expect("build op"),
    );
    let req = b.to_request_via_msgpack().expect("batch request encoding");

    shamir
        .execute("testdb", &req)
        .await
        .expect("add_group_member with no resolver installed must still succeed");

    let members = shamir.group_members(gid).await.unwrap();
    assert!(members.contains(&999_999));
}

/// **Negative (resolver installed):** `add_group_member` with a user id that
/// does NOT resolve via the installed resolver is rejected with
/// `invalid_owner` (task #561 §3).
#[tokio::test]
async fn add_group_member_nonexistent_user_rejected_with_resolver() {
    let shamir = setup_with_resolver().await;
    let gid = shamir.create_group("devs").await.unwrap();

    let mut b = Batch::new();
    b.id(1);
    b.add_group_member(
        "agm",
        ddl::add_group_member(ddl::GroupRef::Id { id: gid }, 999_999)
            .hmac("test")
            .build()
            .expect("build op"),
    );
    let req = b.to_request_via_msgpack().expect("batch request encoding");

    let err = shamir
        .execute("testdb", &req)
        .await
        .expect_err(
            "add_group_member with an unresolvable user id must be rejected when a resolver is installed",
        );
    assert_eq!(err.code(), Some("invalid_owner"));

    // Membership must not have been recorded.
    let members = shamir.group_members(gid).await.unwrap();
    assert!(
        !members.contains(&999_999),
        "the unresolvable member id must not have been added"
    );
}

/// **Positive (resolver installed):** `add_group_member` with a user id that
/// DOES resolve via the installed resolver succeeds.
#[tokio::test]
async fn add_group_member_existing_user_succeeds_with_resolver() {
    let shamir = setup_with_resolver().await;
    let gid = shamir.create_group("devs").await.unwrap();
    let alice = seed_user(&shamir, "alice").await;

    let mut b = Batch::new();
    b.id(1);
    b.add_group_member(
        "agm",
        ddl::add_group_member(ddl::GroupRef::Id { id: gid }, alice)
            .hmac("test")
            .build()
            .expect("build op"),
    );
    let req = b.to_request_via_msgpack().expect("batch request encoding");

    shamir
        .execute("testdb", &req)
        .await
        .expect("add_group_member with a resolvable user id must succeed");

    let members = shamir.group_members(gid).await.unwrap();
    assert!(members.contains(&alice));
}

// ============================================================================
// remove_group_member (decision: harmless no-op, left unvalidated)
// ============================================================================

/// `remove_group_member` for a user id that was never a member (whether
/// or not it resolves to a real user) is a harmless, idempotent no-op —
/// it must succeed rather than error, since nothing dangling is ever
/// written.
#[tokio::test]
async fn remove_group_member_nonexistent_user_is_harmless_noop() {
    let shamir = setup().await;
    let gid = shamir.create_group("devs").await.unwrap();

    let mut b = Batch::new();
    b.id(1);
    b.remove_group_member(
        "rgm",
        ddl::remove_group_member(ddl::GroupRef::Id { id: gid }, 999_999)
            .hmac("test")
            .build()
            .expect("build op"),
    );
    let req = b.to_request_via_msgpack().expect("batch request encoding");

    shamir
        .execute("testdb", &req)
        .await
        .expect("remove_group_member for a non-member id must be a harmless no-op");

    let members = shamir.group_members(gid).await.unwrap();
    assert!(!members.contains(&999_999));
}

// ============================================================================
// access_tree — merged from access_tree_tests.rs (layout slot consolidation;
// both modules exercise the access-control admin surface).
// ============================================================================

/// Test-only mock resolver: returns a single PrincipalInfo for "alice",
/// mirroring the pre-#559 Store-B-backed principals listing. Task #559
/// changed `access_tree`'s principals loop to read from the resolver.
struct MockAliceResolver;

impl PrincipalResolver for MockAliceResolver {
    fn resolve(&self, p64: u64) -> Option<PrincipalInfo> {
        let alice = principal64_from_username("alice");
        if p64 == alice {
            Some(PrincipalInfo {
                principal64: alice,
                name: "alice".to_string(),
                user_id: [0u8; 16],
                database: None,
                superuser: false,
            })
        } else {
            None
        }
    }
    fn list(&self) -> Vec<PrincipalInfo> {
        vec![PrincipalInfo {
            principal64: principal64_from_username("alice"),
            name: "alice".to_string(),
            user_id: [0u8; 16],
            database: None,
            superuser: false,
        }]
    }
}

/// Find a child node by its `name`, panicking if absent. Children order
/// is non-deterministic (dashmap iteration), so always look up by name.
fn child<'a>(node: &'a QueryValue, name: &str) -> &'a QueryValue {
    node["children"]
        .as_array()
        .expect("children array")
        .iter()
        .find(|c| c["name"].as_str() == Some(name))
        .unwrap_or_else(|| panic!("child '{name}' not found"))
}

#[tokio::test]
async fn access_tree_structure_meta_and_principals() {
    let shamir = ShamirDb::init_memory()
        .await
        .unwrap()
        .with_principal_resolver(Arc::new(MockAliceResolver));
    shamir.create_db("testdb").await.expect("create_db");
    let config =
        RepoConfig::new("data", BoxRepoFactory::in_memory()).add_table(TableConfig::new("users"));
    shamir.add_repo("testdb", config).await.unwrap();

    let gid = shamir.create_group("devs").await.unwrap();
    let alice = principal64_from_username("alice");
    shamir.add_group_member(gid, alice).await.unwrap();

    let meta = ResourceMeta {
        owner: Actor::User(alice),
        group: Some(gid),
        mode: 0o750,
    };
    shamir
        .set_resource_meta(&ResourcePath::table("testdb", "data", "users"), &meta)
        .await
        .unwrap();

    let tree = shamir.access_tree(None, None).await.unwrap();

    let root = &tree["resources"];
    assert_eq!(root["kind"].as_str(), Some("root"));
    let db = child(root, "testdb");
    assert_eq!(db["kind"].as_str(), Some("database"));
    let store = child(db, "data");
    assert_eq!(store["kind"].as_str(), Some("store"));
    let table = child(store, "users");
    assert_eq!(table["kind"].as_str(), Some("table"));
    assert_eq!(table["mode"].as_u64(), Some(0o750));
    assert_eq!(table["owner"].as_u64(), Some(alice));
    assert_eq!(table["owner_name"].as_str(), Some("alice"));
    assert_eq!(table["group"].as_u64(), Some(gid));
    assert_eq!(table["group_name"].as_str(), Some("devs"));

    let users = tree["principals"]["users"].as_array().unwrap();
    assert!(
        users.iter().any(|u| u["name"].as_str() == Some("alice")),
        "alice should appear in principals.users"
    );
    let groups = tree["principals"]["groups"].as_array().unwrap();
    let devs = groups
        .iter()
        .find(|g| g["name"].as_str() == Some("devs"))
        .expect("devs group present");
    assert_eq!(devs["id"].as_u64(), Some(gid));
    let members = devs["members"].as_array().unwrap();
    assert!(
        members
            .iter()
            .any(|m| m["id"].as_u64() == Some(alice) && m["name"].as_str() == Some("alice")),
        "alice should be a resolved member of devs"
    );
}

#[tokio::test]
async fn access_tree_depth_caps_the_hierarchy() {
    let shamir = ShamirDb::init_memory().await.unwrap();
    shamir.create_db("testdb").await.expect("create_db");
    let config =
        RepoConfig::new("data", BoxRepoFactory::in_memory()).add_table(TableConfig::new("t"));
    shamir.add_repo("testdb", config).await.unwrap();

    let t0 = shamir.access_tree(Some(0), None).await.unwrap();
    assert!(t0["resources"]["children"].as_array().unwrap().is_empty());

    let t1 = shamir.access_tree(Some(1), None).await.unwrap();
    let db = child(&t1["resources"], "testdb");
    assert!(db["children"].as_array().unwrap().is_empty());

    let t2 = shamir.access_tree(Some(2), None).await.unwrap();
    let store = child(child(&t2["resources"], "testdb"), "data");
    assert!(store["children"].as_array().unwrap().is_empty());

    let tf = shamir.access_tree(None, None).await.unwrap();
    let table = child(child(child(&tf["resources"], "testdb"), "data"), "t");
    assert_eq!(table["kind"].as_str(), Some("table"));
}

#[tokio::test]
async fn access_tree_dispatch_admin_gate_denies_non_admin() {
    let shamir = ShamirDb::init_memory().await.unwrap();
    shamir.create_db("testdb").await.expect("create_db");
    let config =
        RepoConfig::new("data", BoxRepoFactory::in_memory()).add_table(TableConfig::new("t"));
    shamir.add_repo("testdb", config).await.unwrap();

    let mut b = Batch::new();
    b.id(1);
    b.access_tree("tree", ddl::access_tree());
    let req: BatchRequest = b.to_request_via_msgpack().expect("batch request encoding");

    let tree_present = |res: &Result<BatchResponse, BatchError>| -> bool {
        matches!(res, Ok(r) if r
            .results
            .get("tree")
            .and_then(|q| q.records.first())
            .and_then(|rec| rec.get_value("access_tree"))
            .and_then(|t| t.get("resources"))
            .is_some())
    };

    let sys = shamir.execute_as(Actor::System, "testdb", &req).await;
    assert!(tree_present(&sys), "System must receive the access tree");

    let bob = principal64_from_username("bob");
    let user = shamir.execute_as(Actor::User(bob), "testdb", &req).await;
    assert!(
        !tree_present(&user),
        "a non-admin user must be denied the access tree"
    );
}

#[tokio::test]
async fn access_tree_db_filter_scopes_to_one_database() {
    let shamir = ShamirDb::init_memory().await.unwrap();
    shamir.create_db("alpha").await.expect("create_db");
    shamir.create_db("beta").await.expect("create_db");
    let cfg_a =
        RepoConfig::new("data", BoxRepoFactory::in_memory()).add_table(TableConfig::new("t"));
    shamir.add_repo("alpha", cfg_a).await.unwrap();

    let tree = shamir.access_tree(None, Some("alpha")).await.unwrap();
    let dbs = tree["resources"]["children"].as_array().unwrap();
    assert_eq!(dbs.len(), 1, "filter must yield exactly one database");
    assert_eq!(dbs[0]["name"].as_str(), Some("alpha"));
}

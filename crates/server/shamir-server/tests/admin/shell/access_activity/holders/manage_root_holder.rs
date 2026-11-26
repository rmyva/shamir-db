//! POSITIVE witness of the real `Manage(Root)` rule: OWNERSHIP, not the
//! superuser flag.
//!
//! `Manage` is ownership-only — `permits` compares the actor's principal64
//! with the persisted owner and never looks at the mode bits
//! (`crates/foundation/shamir-types/src/entries/access/policy.rs:43-49`).
//! The ordinary "no rights at all" denial is already covered by
//! `rights_gaps.rs:267`, and that probe can stop at the DATABASE floor
//! (`crates/engine/shamir-engine/src/query/batch/control/authorized.rs:102-116`),
//! so it says nothing about `Manage(Root)`. This file pins the rule itself:
//! a non-bootstrap OWNER of Root is allowed, a non-owner on the same server
//! is refused, and both probes pass the database floor first.
//!
//! Root is not addressable over the wire — `ResourceRef`
//! (`crates/query/shamir-query-types/src/admin/access.rs:30`) has no `Root`
//! variant — so the principals are minted and Root OWNERSHIP is written
//! OFFLINE before launch (`secure_fixture.rs::seed_probe_principals` +
//! `seed_root_acl`, `mode` 0755 so non-owner `Execute` traversal works).
//! Right after boot the admin grants BOTH probes `Read` on the routing
//! database through the typed ACL builder, so no batch can stop at the
//! database floor and the only remaining gate in front of `user list`
//! (`crates/engine/shamir-db/src/shamir_db/execute/admin_meta/admin_list.rs:78`)
//! is `Manage(Root)`.
//!
//! Mutants this test is written to kill:
//! * a superuser-only `Manage` gate → the OWNER is refused (step 1 fails);
//! * an ownership-ignoring `Manage` gate (always allow) → the NON-OWNER is
//!   accepted (step 3 fails).

use serde_json::Value;
use shamir_query_types::admin::ResourceRef;

use super::super::harness::{overwrite_profile, seed_catalogue, ShellFixture, DB_NAME, SHELL_USER};
use super::super::secure_fixture::{boot_secure_root_owner, run_with_secret, set_acl_mode};
use super::super::state_data_common::{data, refusal, scram_profile};

/// The second probe: a non-superuser that does NOT own Root.
const NON_OWNER: &str = "shelloutsider";

/// Point the shell's profile at a named principal over TLS-TCP.
fn use_profile(fixture: &ShellFixture, user: &str) {
    let text = scram_profile(fixture, "tls-tcp", &fixture.tls_addr.to_string())
        .replace("username: admin\n", &format!("username: {user}\n"));
    overwrite_profile(fixture, &text);
}

#[test]
fn root_owner_without_superuser_manages_root_and_non_owner_is_denied() {
    let (secure, owner_secret, other_secret) = boot_secure_root_owner(
        "root_owner_without_superuser_manages_root_and_non_owner_is_denied",
        SHELL_USER,
        NON_OWNER,
    );
    let fixture = &secure.shell;
    seed_catalogue(fixture);
    // The routing database is the batch permission floor for BOTH probes.
    // Without this grant an ordinary principal stops there, long before
    // `Manage(Root)` is consulted — so even the OWNER could prove nothing.
    set_acl_mode(
        fixture,
        ResourceRef::Database {
            database: DB_NAME.into(),
        },
        0o755,
    );

    // 1) The OWNER: a plain, non-superuser principal, no roles, no flag.
    use_profile(fixture, SHELL_USER);
    fixture.guard.mark("owner: user list under Manage(Root)");
    let owner_run = run_with_secret(fixture, owner_secret.clone(), false, &["user", "list"]);
    assert!(
        !owner_run.contains_canary(owner_secret.as_str())
            && !owner_run.contains_canary(other_secret.as_str()),
        "secret leaked in the owner run"
    );
    let directory = data(&owner_run, "user");
    let entries = directory["users"].as_array().expect("users array");
    let owner = entries
        .iter()
        .find(|entry| entry["name"] == SHELL_USER)
        .expect("the Root owner must read the principal directory");
    assert_eq!(
        owner["superuser"],
        Value::Bool(false),
        "the owner must hold Manage(Root) WITHOUT the superuser flag"
    );

    // 2) The NON-OWNER's database floor is OPEN on the same server: this
    // batch passes the same Read-on-database gate and only needs `List` on
    // Root, which the 0755 mode grants to others. So the refusal below can
    // only be the Root `Manage` rule.
    use_profile(fixture, NON_OWNER);
    fixture
        .guard
        .mark("non-owner: db list proves the database floor is open");
    let floor = run_with_secret(fixture, other_secret.clone(), false, &["db", "list"]);
    assert!(
        !floor.contains_canary(owner_secret.as_str())
            && !floor.contains_canary(other_secret.as_str()),
        "secret leaked in the non-owner floor probe"
    );
    let databases = data(&floor, "db");
    assert!(
        databases["databases"]
            .as_array()
            .is_some_and(|names| names.iter().any(|name| name == DB_NAME)),
        "the non-owner must pass the database floor before the Root gate"
    );

    // 3) Same server, same routing database, only the ownership differs.
    fixture
        .guard
        .mark("non-owner: user list refused by Manage(Root)");
    let denied = run_with_secret(fixture, other_secret.clone(), false, &["user", "list"]);
    assert!(
        !denied.contains_canary(owner_secret.as_str())
            && !denied.contains_canary(other_secret.as_str()),
        "secret leaked in the non-owner denial"
    );
    refusal(&denied, "user", "permission_denied", 4);
}

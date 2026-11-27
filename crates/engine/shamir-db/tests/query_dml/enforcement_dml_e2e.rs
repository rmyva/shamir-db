use shamir_db::engine::repo::repo_types::BoxRepoFactory;
use shamir_db::engine::repo::RepoConfig;
use shamir_db::engine::table::TableConfig;
use shamir_db::ShamirDb;
use shamir_query_builder::batch::Batch;
use shamir_query_builder::q;
use shamir_query_builder::val::lit;
use shamir_types::access::{Actor, ResourceMeta, ResourcePath};

// ---------------------------------------------------------------------------
// Helper: build a Batch, encode to msgpack, decode back to BatchRequest.
// This proves the wire round-trip is lossless for every query in the file.
// ---------------------------------------------------------------------------

/// Helper: create an in-memory ShamirDb with `testdb` / `main` / `items`.
///
/// G.4c: new objects default to enforced (0o700, System). These tests
/// exercise table-level DML enforcement, so the db + store ancestors are
/// opened here to keep traversal-Execute from masking the table-level
/// behaviour under test.
async fn setup() -> ShamirDb {
    let shamir = ShamirDb::init_memory().await.unwrap();
    shamir.create_db("testdb").await.expect("create_db");
    let config =
        RepoConfig::new("main", BoxRepoFactory::in_memory()).add_table(TableConfig::new("items"));
    shamir.add_repo("testdb", config).await.unwrap();
    let open = ResourceMeta::open();
    shamir
        .set_resource_meta(&ResourcePath::database("testdb"), &open)
        .await
        .unwrap();
    shamir
        .set_resource_meta(&ResourcePath::store("testdb", "main"), &open)
        .await
        .unwrap();
    shamir
}

/// Insert a record via System actor (always allowed).
async fn seed_record(shamir: &ShamirDb) {
    let mut b = Batch::new();
    b.id("seed");
    b.insert(
        "ins",
        q!(insert into items values {
            "name" => "widget",
            "price" => 42
        }),
    );
    let req = b.to_request_via_msgpack().expect("batch request encoding");
    shamir.execute("testdb", &req).await.unwrap();
}

// ============================================================================
// Default mode: enforced (0o700) denies strangers; explicit open allows all
// ============================================================================

#[tokio::test]
async fn default_mode_allows_all_users() {
    let shamir = setup().await;
    seed_record(&shamir).await;

    // G.4c: the table was created with enforced default (owner=System,
    // 0o700). A stranger is now DENIED by default (the create-default path).
    let mut b = Batch::new();
    b.id("r");
    b.query("r", q!(from items));
    let read_req = b.to_request_via_msgpack().expect("batch request encoding");

    let denied = shamir
        .execute_as(Actor::User(99), "testdb", &read_req)
        .await;
    assert!(
        denied.is_err(),
        "enforced default (0o700) should deny a non-owner stranger"
    );

    // Explicit chmod to OPEN (0o777) — the open path still allows everyone.
    shamir
        .set_resource_meta(
            &ResourcePath::table("testdb", "main", "items"),
            &ResourceMeta::open(),
        )
        .await
        .unwrap();

    let resp = shamir
        .execute_as(Actor::User(99), "testdb", &read_req)
        .await;
    assert!(
        resp.is_ok(),
        "after explicit chmod 0o777, any user should be able to read"
    );

    // Insert
    let mut b = Batch::new();
    b.id("i");
    b.insert(
        "i",
        q!(insert into items values {
            "name" => "gadget",
            "price" => 7
        }),
    );
    let ins_req = b.to_request_via_msgpack().expect("batch request encoding");

    let resp = shamir.execute_as(Actor::User(99), "testdb", &ins_req).await;
    assert!(
        resp.is_ok(),
        "after explicit chmod 0o777, any user should be able to insert"
    );
}

// ============================================================================
// Restricted table (0o750, owner=User(1)): owner allowed, stranger denied
// ============================================================================

#[tokio::test]
async fn restricted_table_owner_allowed_stranger_denied_read() {
    let shamir = setup().await;
    seed_record(&shamir).await;

    // Restrict the table: owner=User(1), mode=0o750
    let meta = ResourceMeta {
        owner: Actor::User(1),
        group: None,
        mode: 0o750,
    };
    shamir
        .set_resource_meta(&ResourcePath::table("testdb", "main", "items"), &meta)
        .await
        .unwrap();

    let mut b = Batch::new();
    b.id("r");
    b.query("r", q!(from items));
    let read_req = b.to_request_via_msgpack().expect("batch request encoding");

    // Owner User(1) reads successfully
    let resp = shamir.execute_as(Actor::User(1), "testdb", &read_req).await;
    assert!(resp.is_ok(), "owner should be able to read");

    // Stranger User(99) is denied
    let err = shamir
        .execute_as(Actor::User(99), "testdb", &read_req)
        .await
        .expect_err("stranger should be denied read on 0o750 table");
    assert_unknown_table(Err::<(), _>(err), "User(99) read on restricted table");
}

#[tokio::test]
async fn restricted_table_stranger_denied_insert() {
    let shamir = setup().await;

    let meta = ResourceMeta {
        owner: Actor::User(1),
        group: None,
        mode: 0o750,
    };
    shamir
        .set_resource_meta(&ResourcePath::table("testdb", "main", "items"), &meta)
        .await
        .unwrap();

    let mut b = Batch::new();
    b.id("i");
    b.insert(
        "i",
        q!(insert into items values {
            "name" => "gadget",
            "price" => 7
        }),
    );
    let ins_req = b.to_request_via_msgpack().expect("batch request encoding");

    // Owner can insert (0o750 → owner has rwx)
    let resp = shamir.execute_as(Actor::User(1), "testdb", &ins_req).await;
    assert!(resp.is_ok(), "owner should be able to insert");

    // Stranger denied
    let err = shamir
        .execute_as(Actor::User(99), "testdb", &ins_req)
        .await
        .expect_err("stranger should be denied insert on 0o750 table");
    let msg = format!("{err:?}");
    assert!(
        msg.contains("denied") || msg.contains("Denied"),
        "error should mention denial, got: {msg}"
    );
}

#[tokio::test]
async fn restricted_table_stranger_denied_delete() {
    let shamir = setup().await;
    seed_record(&shamir).await;

    let meta = ResourceMeta {
        owner: Actor::User(1),
        group: None,
        mode: 0o750,
    };
    shamir
        .set_resource_meta(&ResourcePath::table("testdb", "main", "items"), &meta)
        .await
        .unwrap();

    let mut b = Batch::new();
    b.id("d");
    b.delete("d", q!(delete from items where name == "widget").unwrap());
    let del_req = b.to_request_via_msgpack().expect("batch request encoding");

    // Stranger denied
    let err = shamir
        .execute_as(Actor::User(99), "testdb", &del_req)
        .await
        .expect_err("stranger should be denied delete on 0o750 table");
    let msg = format!("{err:?}");
    assert!(
        msg.contains("denied") || msg.contains("Denied"),
        "error should mention denial, got: {msg}"
    );
}

// ============================================================================
// System actor always bypasses
// ============================================================================

#[tokio::test]
async fn system_bypasses_restricted_table() {
    let shamir = setup().await;
    seed_record(&shamir).await;

    // Lock down to User(1) only
    let meta = ResourceMeta {
        owner: Actor::User(1),
        group: None,
        mode: 0o700,
    };
    shamir
        .set_resource_meta(&ResourcePath::table("testdb", "main", "items"), &meta)
        .await
        .unwrap();

    let mut b = Batch::new();
    b.id("r");
    b.query("r", q!(from items));
    let read_req = b.to_request_via_msgpack().expect("batch request encoding");

    // System always passes
    let resp = shamir.execute_as(Actor::System, "testdb", &read_req).await;
    assert!(resp.is_ok(), "System must always bypass ACLs");

    // Also via the convenience `execute` (which is execute_as(System))
    let resp = shamir.execute("testdb", &read_req).await;
    assert!(resp.is_ok(), "execute() delegates to System, must pass");
}

// ============================================================================
// Interactive tx path: tx_execute_as enforces table DML too
// ============================================================================

#[tokio::test]
async fn tx_execute_as_enforces_table_acl() {
    let shamir = setup().await;
    seed_record(&shamir).await;

    // Restrict table to User(1)
    let meta = ResourceMeta {
        owner: Actor::User(1),
        group: None,
        mode: 0o700,
    };
    shamir
        .set_resource_meta(&ResourcePath::table("testdb", "main", "items"), &meta)
        .await
        .unwrap();

    let mut b = Batch::new();
    b.id("txr");
    b.query("r", q!(from items));
    let read_req = b.to_request_via_msgpack().expect("batch request encoding");

    // Open an interactive tx as User(1) (owner) — should succeed
    let (mut tx_ok, _guard_ok, _repo) = shamir
        .tx_begin_as(Actor::User(1), "testdb", "main", "snapshot")
        .await
        .unwrap();

    let resp = shamir
        .tx_execute_as(Actor::User(1), "testdb", &read_req, &mut tx_ok)
        .await;
    assert!(
        resp.is_ok(),
        "owner should be able to read inside interactive tx"
    );

    // Open an interactive tx as User(99) (stranger) — tx_begin may succeed
    // (database is open), but tx_execute_as with a read on restricted table
    // should fail.
    let (mut tx_bad, _guard_bad, _repo) = shamir
        .tx_begin_as(Actor::User(99), "testdb", "main", "snapshot")
        .await
        .unwrap();

    let err = shamir
        .tx_execute_as(Actor::User(99), "testdb", &read_req, &mut tx_bad)
        .await
        .expect_err("stranger should be denied read in interactive tx on restricted table");
    assert_unknown_table(
        Err::<(), _>(err),
        "stranger read in interactive tx on restricted table",
    );
}

// ============================================================================
// CRITICAL — `ForEach`/`Batch` per-table ACL bypass (security-fix brief
// docs/dev-artifacts/prompts/security-fix/01-foreach-batch-acl-bypass.md).
//
// `BatchOp::required_access` returns `None` for `BatchOp::Batch`/
// `BatchOp::ForEach` (no `table_ref()`), so a flat, one-level authorization
// pre-check loop over `request.queries.values()` never inspects what a
// nested `Batch`/`ForEach` body actually touches. An actor with permission
// on SOME tables but explicitly NOT on a forbidden table could read/write
// that forbidden table by simply wrapping the op in a top-level `ForEach`
// (even a trivial single-iteration `over`) or a plain `Batch` sub-batch.
// `collect_required_access` (mirroring `distinct_repos`'s #660 recursive
// walk) closes this by recursing into nested bodies at any depth.
//
// `items` stays open (any actor may read/insert/delete it) while `secrets`
// is locked to `Actor::User(1)` only (mode 0o700) — `Actor::User(2)` has
// permission on `items` but explicitly NOT on `secrets`, the exact shape
// the brief describes.
// ============================================================================

/// Extend `setup()` with a second table, `secrets`, restricted to
/// `Actor::User(1)` only (owner-rwx, mode 0o700) — the FORBIDDEN table for
/// `Actor::User(2)` in every bypass test below. `items` is explicitly opened
/// here too (`setup()` only opens the db/store ancestors, not the table
/// itself — G.4c defaults new tables to an enforced owner-only mode) so
/// `Actor::User(2)` genuinely has permission on it — the ALLOWED table for
/// the positive/regression tests.
async fn setup_with_secrets() -> ShamirDb {
    let shamir = setup().await;
    shamir
        .set_resource_meta(
            &ResourcePath::table("testdb", "main", "items"),
            &ResourceMeta::open(),
        )
        .await
        .unwrap();
    let config = RepoConfig::new("vault", BoxRepoFactory::in_memory())
        .add_table(TableConfig::new("secrets"));
    shamir.add_repo("testdb", config).await.unwrap();
    shamir
        .set_resource_meta(
            &ResourcePath::store("testdb", "vault"),
            &ResourceMeta::open(),
        )
        .await
        .unwrap();
    let meta = ResourceMeta {
        owner: Actor::User(1),
        group: None,
        mode: 0o700,
    };
    shamir
        .set_resource_meta(&ResourcePath::table("testdb", "vault", "secrets"), &meta)
        .await
        .unwrap();
    shamir
}

fn assert_unknown_table<T: std::fmt::Debug>(result: Result<T, impl std::fmt::Debug>, ctx: &str) {
    let err = match result {
        Ok(v) => panic!("{ctx}: expected unknown_table, got Ok({v:?})"),
        Err(e) => e,
    };
    let msg = format!("{err:?}");
    assert!(
        msg.contains("unknown_table") && msg.contains("table is unavailable"),
        "{ctx}: expected the canonical table refusal, got: {msg}"
    );
}

fn assert_access_denied<T: std::fmt::Debug>(result: Result<T, impl std::fmt::Debug>, ctx: &str) {
    let err = match result {
        Ok(v) => panic!("{ctx}: expected access_denied, got Ok({v:?})"),
        Err(e) => e,
    };
    let msg = format!("{err:?}");
    assert!(
        msg.contains("denied") || msg.contains("Denied"),
        "{ctx}: expected an access_denied error, got: {msg}"
    );
}

// ---------------------------------------------------------------------------
// 1. execute_as — top-level ForEach body touching the forbidden table must
//    be denied (previously: silently succeeded).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn execute_as_denies_forbidden_table_inside_top_level_for_each() {
    let shamir = setup_with_secrets().await;

    let mut inner = Batch::new();
    inner.query("r", q!(from vault.secrets));
    let inner_req = inner.build();

    let mut outer = Batch::new();
    outer.id("bypass_foreach");
    outer.for_each("loop", vec![lit(1000_i64), lit(2000_i64)], "row", inner_req);
    let req = outer
        .to_request_via_msgpack()
        .expect("batch request encoding");

    let result = shamir.execute_as(Actor::User(2), "testdb", &req).await;
    assert_unknown_table(
        result,
        "User(2) reading forbidden `secrets` via top-level ForEach must be refused",
    );
}

#[tokio::test]
async fn execute_as_denies_forbidden_table_insert_inside_top_level_for_each() {
    let shamir = setup_with_secrets().await;

    let mut inner = Batch::new();
    inner.insert(
        "i",
        q!(insert into vault.secrets values {
            "value" => "leak"
        }),
    );
    let inner_req = inner.build();

    let mut outer = Batch::new();
    outer.id("bypass_foreach_insert");
    outer.for_each("loop", vec![lit(1000_i64), lit(2000_i64)], "row", inner_req);
    let req = outer
        .to_request_via_msgpack()
        .expect("batch request encoding");

    let result = shamir.execute_as(Actor::User(2), "testdb", &req).await;
    assert_access_denied(
        result,
        "User(2) inserting into forbidden `secrets` via top-level ForEach must be denied",
    );
}

// ---------------------------------------------------------------------------
// 2. execute_as — same shape via plain Batch (non-transactional sub-batch).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn execute_as_denies_forbidden_table_inside_top_level_batch() {
    let shamir = setup_with_secrets().await;

    let mut inner = Batch::new();
    inner.query("r", q!(from vault.secrets));
    let inner_req = inner.build();

    let mut outer = Batch::new();
    outer.id("bypass_batch");
    outer.sub_batch_no_bind("sub", inner_req);
    let req = outer
        .to_request_via_msgpack()
        .expect("batch request encoding");

    let result = shamir.execute_as(Actor::User(2), "testdb", &req).await;
    assert_unknown_table(
        result,
        "User(2) reading forbidden `secrets` via top-level Batch must be refused",
    );
}

// ---------------------------------------------------------------------------
// 3. execute_as — nested arbitrarily: Batch -> ForEach -> Batch, innermost
//    body touches the forbidden table. Proves recursion isn't one level deep.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn execute_as_denies_forbidden_table_nested_batch_foreach_batch() {
    let shamir = setup_with_secrets().await;

    let mut innermost = Batch::new();
    innermost.query("r", q!(from vault.secrets));
    let innermost_req = innermost.build();

    let mut middle = Batch::new();
    middle.sub_batch_no_bind("innermost", innermost_req);
    let middle_req = middle.build();

    let mut outer_inner = Batch::new();
    outer_inner.for_each(
        "loop",
        vec![lit(1000_i64), lit(2000_i64)],
        "row",
        middle_req,
    );
    let outer_inner_req = outer_inner.build();

    let mut outer = Batch::new();
    outer.id("bypass_nested");
    outer.sub_batch_no_bind("outer_sub", outer_inner_req);
    let req = outer
        .to_request_via_msgpack()
        .expect("batch request encoding");

    let result = shamir.execute_as(Actor::User(2), "testdb", &req).await;
    assert_unknown_table(
        result,
        "User(2) reading forbidden `secrets` via Batch->ForEach->Batch must be refused",
    );
}

// ---------------------------------------------------------------------------
// 4. Positive/regression — the SAME nested shapes touching the ALLOWED
//    table (`items`) must still succeed. Don't just prove "now everything
//    is denied" — prove legitimate nested usage still works (#661 depends
//    on nested Batch/ForEach being usable).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn execute_as_allows_permitted_table_inside_top_level_for_each() {
    let shamir = setup_with_secrets().await;
    seed_record(&shamir).await;

    let mut inner = Batch::new();
    inner.query("r", q!(from items));
    let inner_req = inner.build();

    let mut outer = Batch::new();
    outer.id("legit_foreach");
    outer.for_each("loop", vec![lit(1000_i64), lit(2000_i64)], "row", inner_req);
    let req = outer
        .to_request_via_msgpack()
        .expect("batch request encoding");

    let result = shamir.execute_as(Actor::User(2), "testdb", &req).await;
    assert!(
        result.is_ok(),
        "User(2) reading permitted `items` via top-level ForEach must still succeed: {result:?}"
    );
}

#[tokio::test]
async fn execute_as_allows_permitted_table_inside_top_level_batch() {
    let shamir = setup_with_secrets().await;
    seed_record(&shamir).await;

    let mut inner = Batch::new();
    inner.query("r", q!(from items));
    let inner_req = inner.build();

    let mut outer = Batch::new();
    outer.id("legit_batch");
    outer.sub_batch_no_bind("sub", inner_req);
    let req = outer
        .to_request_via_msgpack()
        .expect("batch request encoding");

    let result = shamir.execute_as(Actor::User(2), "testdb", &req).await;
    assert!(
        result.is_ok(),
        "User(2) reading permitted `items` via top-level Batch must still succeed: {result:?}"
    );
}

#[tokio::test]
async fn execute_as_allows_permitted_table_nested_batch_foreach_batch() {
    let shamir = setup_with_secrets().await;
    seed_record(&shamir).await;

    let mut innermost = Batch::new();
    innermost.query("r", q!(from items));
    let innermost_req = innermost.build();

    let mut middle = Batch::new();
    middle.sub_batch_no_bind("innermost", innermost_req);
    let middle_req = middle.build();

    let mut outer_inner = Batch::new();
    outer_inner.for_each(
        "loop",
        vec![lit(1000_i64), lit(2000_i64)],
        "row",
        middle_req,
    );
    let outer_inner_req = outer_inner.build();

    let mut outer = Batch::new();
    outer.id("legit_nested");
    outer.sub_batch_no_bind("outer_sub", outer_inner_req);
    let req = outer
        .to_request_via_msgpack()
        .expect("batch request encoding");

    let result = shamir.execute_as(Actor::User(2), "testdb", &req).await;
    assert!(
        result.is_ok(),
        "User(2) reading permitted `items` via nested Batch->ForEach->Batch must still succeed: {result:?}"
    );
}

// ---------------------------------------------------------------------------
// 5. Both entry points — mirror tests 1-4 for tx_execute_as (transactional/
//    interactive). The bug and the fix are symmetric across both call
//    sites — the tx path additionally exercises the ACL inline cache.
//
// tx_execute_as enforces the SINGLE-repo constraint (the tx is opened
// against one repo/handle), so these tx tests scope the nested bodies to
// tables WITHIN the tx's own repo — `vault` (for the forbidden-table cases)
// or `main` (for the permitted-table cases) — matching how a real
// interactive tx is used.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tx_execute_as_denies_forbidden_table_inside_top_level_for_each() {
    let shamir = setup_with_secrets().await;

    let mut inner = Batch::new();
    inner.query("r", q!(from vault.secrets));
    let inner_req = inner.build();

    let mut outer = Batch::new();
    outer.id("tx_bypass_foreach");
    outer.for_each("loop", vec![lit(1000_i64), lit(2000_i64)], "row", inner_req);
    let req = outer
        .to_request_via_msgpack()
        .expect("batch request encoding");

    // User(2) has no rights on `vault/secrets` (owner-only User(1)), but the
    // `vault` STORE itself is open (setup_with_secrets), so tx_begin_as
    // succeeds — the forbidden table is the ForEach body's inner op, which
    // is exactly what this bug lets bypass a flat one-level pre-check.
    let (mut tx, _guard, _repo) = shamir
        .tx_begin_as(Actor::User(2), "testdb", "vault", "snapshot")
        .await
        .unwrap();

    let result = shamir
        .tx_execute_as(Actor::User(2), "testdb", &req, &mut tx)
        .await;
    assert_unknown_table(
        result,
        "User(2) reading forbidden `secrets` via top-level ForEach in tx_execute_as must be refused",
    );
}

#[tokio::test]
async fn tx_execute_as_denies_forbidden_table_inside_top_level_batch() {
    let shamir = setup_with_secrets().await;

    let mut inner = Batch::new();
    inner.query("r", q!(from vault.secrets));
    let inner_req = inner.build();

    let mut outer = Batch::new();
    outer.id("tx_bypass_batch");
    outer.sub_batch_no_bind("sub", inner_req);
    let req = outer
        .to_request_via_msgpack()
        .expect("batch request encoding");

    let (mut tx, _guard, _repo) = shamir
        .tx_begin_as(Actor::User(2), "testdb", "vault", "snapshot")
        .await
        .unwrap();

    let result = shamir
        .tx_execute_as(Actor::User(2), "testdb", &req, &mut tx)
        .await;
    assert_unknown_table(
        result,
        "User(2) reading forbidden `secrets` via top-level Batch in tx_execute_as must be refused",
    );
}

#[tokio::test]
async fn tx_execute_as_denies_forbidden_table_nested_batch_foreach_batch() {
    let shamir = setup_with_secrets().await;

    let mut innermost = Batch::new();
    innermost.query("r", q!(from vault.secrets));
    let innermost_req = innermost.build();

    let mut middle = Batch::new();
    middle.sub_batch_no_bind("innermost", innermost_req);
    let middle_req = middle.build();

    let mut outer_inner = Batch::new();
    outer_inner.for_each(
        "loop",
        vec![lit(1000_i64), lit(2000_i64)],
        "row",
        middle_req,
    );
    let outer_inner_req = outer_inner.build();

    let mut outer = Batch::new();
    outer.id("tx_bypass_nested");
    outer.sub_batch_no_bind("outer_sub", outer_inner_req);
    let req = outer
        .to_request_via_msgpack()
        .expect("batch request encoding");

    let (mut tx, _guard, _repo) = shamir
        .tx_begin_as(Actor::User(2), "testdb", "vault", "snapshot")
        .await
        .unwrap();

    let result = shamir
        .tx_execute_as(Actor::User(2), "testdb", &req, &mut tx)
        .await;
    assert_unknown_table(
        result,
        "User(2) reading forbidden `secrets` via nested Batch->ForEach->Batch in tx_execute_as must be refused",
    );
}

#[tokio::test]
async fn tx_execute_as_allows_permitted_table_inside_top_level_for_each() {
    let shamir = setup_with_secrets().await;
    seed_record(&shamir).await;

    let mut inner = Batch::new();
    inner.query("r", q!(from items));
    let inner_req = inner.build();

    let mut outer = Batch::new();
    outer.id("tx_legit_foreach");
    outer.for_each("loop", vec![lit(1000_i64), lit(2000_i64)], "row", inner_req);
    let req = outer
        .to_request_via_msgpack()
        .expect("batch request encoding");

    let (mut tx, _guard, _repo) = shamir
        .tx_begin_as(Actor::User(2), "testdb", "main", "snapshot")
        .await
        .unwrap();

    let result = shamir
        .tx_execute_as(Actor::User(2), "testdb", &req, &mut tx)
        .await;
    assert!(
        result.is_ok(),
        "User(2) reading permitted `items` via top-level ForEach in tx_execute_as must still succeed: {result:?}"
    );
}

#[tokio::test]
async fn tx_execute_as_allows_permitted_table_inside_top_level_batch() {
    let shamir = setup_with_secrets().await;
    seed_record(&shamir).await;

    let mut inner = Batch::new();
    inner.query("r", q!(from items));
    let inner_req = inner.build();

    let mut outer = Batch::new();
    outer.id("tx_legit_batch");
    outer.sub_batch_no_bind("sub", inner_req);
    let req = outer
        .to_request_via_msgpack()
        .expect("batch request encoding");

    let (mut tx, _guard, _repo) = shamir
        .tx_begin_as(Actor::User(2), "testdb", "main", "snapshot")
        .await
        .unwrap();

    let result = shamir
        .tx_execute_as(Actor::User(2), "testdb", &req, &mut tx)
        .await;
    assert!(
        result.is_ok(),
        "User(2) reading permitted `items` via top-level Batch in tx_execute_as must still succeed: {result:?}"
    );
}

#[tokio::test]
async fn tx_execute_as_allows_permitted_table_nested_batch_foreach_batch() {
    let shamir = setup_with_secrets().await;
    seed_record(&shamir).await;

    let mut innermost = Batch::new();
    innermost.query("r", q!(from items));
    let innermost_req = innermost.build();

    let mut middle = Batch::new();
    middle.sub_batch_no_bind("innermost", innermost_req);
    let middle_req = middle.build();

    let mut outer_inner = Batch::new();
    outer_inner.for_each(
        "loop",
        vec![lit(1000_i64), lit(2000_i64)],
        "row",
        middle_req,
    );
    let outer_inner_req = outer_inner.build();

    let mut outer = Batch::new();
    outer.id("tx_legit_nested");
    outer.sub_batch_no_bind("outer_sub", outer_inner_req);
    let req = outer
        .to_request_via_msgpack()
        .expect("batch request encoding");

    let (mut tx, _guard, _repo) = shamir
        .tx_begin_as(Actor::User(2), "testdb", "main", "snapshot")
        .await
        .unwrap();

    let result = shamir
        .tx_execute_as(Actor::User(2), "testdb", &req, &mut tx)
        .await;
    assert!(
        result.is_ok(),
        "User(2) reading permitted `items` via nested Batch->ForEach->Batch in tx_execute_as must still succeed: {result:?}"
    );
}

// ---------------------------------------------------------------------------
// 6. Actor::System unaffected — the admin bypass must still work through
//    nested Batch/ForEach exactly as before (System should never hit a
//    denial), for both execute_as and tx_execute_as.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn execute_as_system_bypasses_nested_batch_foreach_on_forbidden_table() {
    let shamir = setup_with_secrets().await;

    let mut innermost = Batch::new();
    innermost.query("r", q!(from vault.secrets));
    let innermost_req = innermost.build();

    let mut middle = Batch::new();
    middle.sub_batch_no_bind("innermost", innermost_req);
    let middle_req = middle.build();

    let mut outer_inner = Batch::new();
    outer_inner.for_each(
        "loop",
        vec![lit(1000_i64), lit(2000_i64)],
        "row",
        middle_req,
    );
    let outer_inner_req = outer_inner.build();

    let mut outer = Batch::new();
    outer.id("system_nested");
    outer.sub_batch_no_bind("outer_sub", outer_inner_req);
    let req = outer
        .to_request_via_msgpack()
        .expect("batch request encoding");

    let result = shamir.execute_as(Actor::System, "testdb", &req).await;
    assert!(
        result.is_ok(),
        "Actor::System must bypass nested Batch/ForEach ACL checks: {result:?}"
    );
}

#[tokio::test]
async fn tx_execute_as_system_bypasses_nested_batch_foreach_on_forbidden_table() {
    let shamir = setup_with_secrets().await;

    let mut innermost = Batch::new();
    innermost.query("r", q!(from vault.secrets));
    let innermost_req = innermost.build();

    let mut middle = Batch::new();
    middle.sub_batch_no_bind("innermost", innermost_req);
    let middle_req = middle.build();

    let mut outer_inner = Batch::new();
    outer_inner.for_each(
        "loop",
        vec![lit(1000_i64), lit(2000_i64)],
        "row",
        middle_req,
    );
    let outer_inner_req = outer_inner.build();

    let mut outer = Batch::new();
    outer.id("tx_system_nested");
    outer.sub_batch_no_bind("outer_sub", outer_inner_req);
    let req = outer
        .to_request_via_msgpack()
        .expect("batch request encoding");

    let (mut tx, _guard, _repo) = shamir
        .tx_begin_as(Actor::System, "testdb", "vault", "snapshot")
        .await
        .unwrap();

    let result = shamir
        .tx_execute_as(Actor::System, "testdb", &req, &mut tx)
        .await;
    assert!(
        result.is_ok(),
        "Actor::System must bypass nested Batch/ForEach ACL checks in tx_execute_as: {result:?}"
    );
}
